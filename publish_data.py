"""Zip the processed data/ folder and publish it on the `data-latest` GitHub Release.

The snapshot is split into ~120 MB zips (`nse-data-01.zip`, ...) instead of one big file:
uploads of the whole 430 MB snapshot are aborted mid-transfer by the network, while parts of
this size go through. Each part is a normal zip holding whole files, so extracting all of them
in the repo root rebuilds `data/` - no joining step. Assets are replaced in place (a same-named
asset is deleted first), so nothing accumulates on the release.

    python publish_data.py                  # zip + upload
    python publish_data.py --no-upload      # build the parts only (--out DIR to keep them)

Restore on another machine (from the repo root):

    gh release download data-latest -p "nse-data-*.zip"
    for %f in (nse-data-*.zip) do tar -xf %f      # cmd;  PowerShell: dir nse-data-*.zip | % { tar -xf $_ }

Uploading uses `curl -T` (streaming) rather than `gh release upload`: gh managed ~1 Mbit/s on
this connection, curl streams at line speed.
"""
from __future__ import annotations

import argparse
import json
import subprocess
import tempfile
import zipfile
from pathlib import Path

from config import DATA_DIR, LOG_DIR, MANUAL_OVERRIDES_FILE, META_FILE, RAW_DIR, ROOT_DIR

TAG = "data-latest"
ASSET_PREFIX = "nse-data-"
PART_MAX_BYTES = 120 * 1024 * 1024
REPO = "ancap97/NSEDATA4ME"
UPLOAD_RETRIES = 5
EXCLUDE_DIRS = (RAW_DIR, LOG_DIR)
EXCLUDE_SUFFIXES = (".tmp", ".bak", ".zip")
EXCLUDE_FILES = (MANUAL_OVERRIDES_FILE,)  # tracked in git; extracting a snapshot must not overwrite it


def data_files() -> list[Path]:
    out = []
    for p in sorted(DATA_DIR.rglob("*")):
        if not p.is_file() or p.suffix in EXCLUDE_SUFFIXES or p in EXCLUDE_FILES:
            continue
        if any(p.is_relative_to(d) for d in EXCLUDE_DIRS):
            continue
        out.append(p)
    return out


def split_into_parts(files: list[Path], max_bytes: int = PART_MAX_BYTES) -> list[list[Path]]:
    """Group whole files into parts of at most max_bytes (a single larger file gets its own part)."""
    parts: list[list[Path]] = [[]]
    size = 0
    for p in files:
        n = p.stat().st_size
        if parts[-1] and size + n > max_bytes:
            parts.append([])
            size = 0
        parts[-1].append(p)
        size += n
    return [p for p in parts if p]


def build_parts(dest_dir: Path) -> list[Path]:
    parts = split_into_parts(data_files())
    built = []
    for i, group in enumerate(parts, 1):
        dest = dest_dir / f"{ASSET_PREFIX}{i:02d}.zip"
        with zipfile.ZipFile(dest, "w") as zf:
            for p in group:
                # parquet is already zstd-compressed; deflating it again only costs time
                method = zipfile.ZIP_STORED if p.suffix == ".parquet" else zipfile.ZIP_DEFLATED
                zf.write(p, p.relative_to(ROOT_DIR).as_posix(), compress_type=method)
        print(f"  {dest.name}: {len(group)} files, {dest.stat().st_size / 1e6:.0f} MB")
        built.append(dest)
    return built


def gh(*args: str, check: bool = True) -> str:
    r = subprocess.run(["gh", *args], cwd=ROOT_DIR, check=check, capture_output=True, text=True)
    return r.stdout.strip()


def release() -> tuple[int, dict[str, dict]]:
    """(release id, {asset name: asset}) for the data-latest release."""
    data = json.loads(gh("api", f"repos/{REPO}/releases/tags/{TAG}"))
    return data["id"], {a["name"]: a for a in data["assets"]}


def delete_asset(asset: dict) -> None:
    gh("api", "--method", "DELETE", f"repos/{REPO}/releases/assets/{asset['id']}")


def upload_part(release_id: int, token: str, path: Path) -> None:
    url = f"https://uploads.github.com/repos/{REPO}/releases/{release_id}/assets?name={path.name}"
    # -T streams from disk (gh's own upload crawls on this connection) and --retry covers the
    # mid-transfer aborts; the auth header goes in via stdin so the token stays out of argv
    cmd = [
        "curl", "-sS", "--fail-with-body", "--retry", str(UPLOAD_RETRIES), "--retry-all-errors",
        "-X", "POST", "-T", str(path), "-H", "Content-Type: application/zip", "-K", "-", url,
    ]
    r = subprocess.run(cmd, cwd=ROOT_DIR, input=f'header = "Authorization: Bearer {token}"\n',
                       capture_output=True, text=True)
    if r.returncode != 0:
        raise SystemExit(f"upload of {path.name} failed (curl {r.returncode}): {r.stderr.strip() or r.stdout.strip()}")


def verify(built: list[Path]) -> None:
    _, remote = release()
    for p in built:
        a = remote.get(p.name)
        if not a or a["size"] != p.stat().st_size or a["state"] != "uploaded":
            raise SystemExit(f"{p.name}: upload did not land correctly ({a})")
    for name, asset in remote.items():
        if name.startswith(ASSET_PREFIX) and name not in {p.name for p in built}:
            delete_asset(asset)
            print(f"  removed stale asset {name}")


def publish(built: list[Path], last_synced: str) -> None:
    notes = (
        f"Processed NSE EOD data synced through {last_synced}, split into {len(built)} zips.\n\n"
        "Restore from the repo root:\n```\ngh release download data-latest -p \"nse-data-*.zip\"\n"
        "dir nse-data-*.zip | % { tar -xf $_ }\n```"
    )
    title = f"NSE data (through {last_synced})"
    if subprocess.run(["gh", "release", "view", TAG], cwd=ROOT_DIR, capture_output=True).returncode != 0:
        gh("release", "create", TAG, "--title", title, "--notes", notes)
    else:
        gh("release", "edit", TAG, "--title", title, "--notes", notes)

    token = gh("auth", "token")
    release_id, existing = release()
    for i, p in enumerate(built, 1):
        print(f"uploading {p.name} ({i}/{len(built)}, {p.stat().st_size / 1e6:.0f} MB)...", flush=True)
        if p.name in existing:  # a same-named asset blocks the upload, so replace it
            delete_asset(existing[p.name])
        upload_part(release_id, token, p)
    verify(built)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--out", type=Path, help="directory to keep the built zips in (default: temp, deleted)")
    ap.add_argument("--no-upload", action="store_true", help="only build the parts")
    args = ap.parse_args()

    meta = json.loads(META_FILE.read_text(encoding="utf-8"))
    if meta.get("in_progress"):
        raise SystemExit(f"sync is mid-commit for {meta['in_progress']['date']}; finish or resume sync.py first")
    last_synced = meta["last_synced"]

    with tempfile.TemporaryDirectory() as tmp:
        dest_dir = args.out or Path(tmp)
        dest_dir.mkdir(parents=True, exist_ok=True)
        print(f"zipping data/ (synced through {last_synced})")
        built = build_parts(dest_dir)
        if not args.no_upload:
            publish(built, last_synced)
            print(f"published {len(built)} parts to release {TAG} (data through {last_synced})")


if __name__ == "__main__":
    main()
