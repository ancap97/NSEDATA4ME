"""Zip the processed data/ folder and publish it as the `data-latest` GitHub Release asset.

The asset is replaced in place (gh release upload --clobber), so old snapshots do not pile up
and nothing is added to the git history. Requires the GitHub CLI (`gh`), logged in.

    python publish_data.py                                  # zip + upload
    python publish_data.py --no-upload --out nse-data.zip   # zip only

Restore on another machine (from the repo root):

    gh release download data-latest -p nse-data.zip
    tar -xf nse-data.zip
"""
from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import tempfile
import zipfile
from pathlib import Path

from config import DATA_DIR, LOG_DIR, MANUAL_OVERRIDES_FILE, META_FILE, RAW_DIR, ROOT_DIR

TAG = "data-latest"
ASSET = "nse-data.zip"
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


def build_zip(dest: Path) -> int:
    files = data_files()
    with zipfile.ZipFile(dest, "w") as zf:
        for p in files:
            # parquet is already zstd-compressed; deflating it again only costs time
            method = zipfile.ZIP_STORED if p.suffix == ".parquet" else zipfile.ZIP_DEFLATED
            zf.write(p, p.relative_to(ROOT_DIR).as_posix(), compress_type=method)
    return len(files)


def gh(*args: str, check: bool = True) -> subprocess.CompletedProcess:
    return subprocess.run(["gh", *args], cwd=ROOT_DIR, check=check, capture_output=True, text=True)


def upload(zip_path: Path, last_synced: str) -> None:
    notes = (
        f"Processed NSE EOD data synced through {last_synced}.\n\n"
        f"Restore from the repo root:\n```\ngh release download {TAG} -p {ASSET}\ntar -xf {ASSET}\n```"
    )
    title = f"NSE data (through {last_synced})"
    if gh("release", "view", TAG, check=False).returncode != 0:
        gh("release", "create", TAG, "--title", title, "--notes", notes)
    else:
        gh("release", "edit", TAG, "--title", title, "--notes", notes)
    gh("release", "upload", TAG, str(zip_path), "--clobber")  # asset name = file name


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--out", type=Path, help="write the zip here and keep it (default: temp file, deleted after upload)")
    ap.add_argument("--no-upload", action="store_true", help="only build the zip")
    args = ap.parse_args()

    meta = json.loads(META_FILE.read_text(encoding="utf-8"))
    if meta.get("in_progress"):
        raise SystemExit(f"sync is mid-commit for {meta['in_progress']['date']}; finish or resume sync.py first")
    last_synced = meta["last_synced"]

    with tempfile.TemporaryDirectory() as tmp:
        dest = Path(tmp) / ASSET
        n = build_zip(dest)
        print(f"zipped {n} files ({dest.stat().st_size / 1e6:.0f} MB)")
        if not args.no_upload:
            upload(dest, last_synced)
            print(f"uploaded {ASSET} to release {TAG} (data through {last_synced})")
        if args.out:
            shutil.move(dest, args.out)
            print(f"zip kept at {args.out}")


if __name__ == "__main__":
    main()
