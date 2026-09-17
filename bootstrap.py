"""First-run helper: fetch the data snapshot from the `data` branch.

`main` holds code only, so a fresh clone has no `data/` folder. `sync.py` calls
`ensure_data()` on startup; it pulls the latest snapshot (~430 MB) once and from then on sync
just appends new days to it. Nothing here runs when the data is already present.
"""
from __future__ import annotations

import subprocess
import tarfile
import tempfile
from pathlib import Path

from config import DATA_DIR, META_FILE, ROOT_DIR, STORE_DIR

BRANCH = "data"
REMOTE = "origin"
BRANCH_ZIP_URL = "https://github.com/ancap97/NSEDATA4ME/archive/refs/heads/data.zip"

MANUAL_STEPS = f"""No data/ folder and no git remote to fetch it from.

Download {BRANCH_ZIP_URL}
and move the `data` folder out of the extracted `NSEDATA4ME-data` folder into
{ROOT_DIR}, so that {DATA_DIR / 'store'} exists. Then run sync.py again."""


def data_present() -> bool:
    return META_FILE.exists() and any(STORE_DIR.glob("*.parquet"))


def _git(*args: str) -> str:
    r = subprocess.run(["git", *args], cwd=ROOT_DIR, check=True, capture_output=True, text=True)
    return r.stdout.strip()


def _has_remote() -> bool:
    try:
        return bool((ROOT_DIR / ".git").exists() and _git("remote"))
    except (subprocess.CalledProcessError, FileNotFoundError):
        return False


def fetch_snapshot() -> None:
    """Extract the tip of the data branch into the repo root."""
    print(f"no data/ found - fetching the snapshot from the {BRANCH} branch (~430 MB, one time)...", flush=True)
    _git("fetch", "--depth", "1", REMOTE, BRANCH)
    with tempfile.TemporaryDirectory() as tmp:
        tar = Path(tmp) / "snapshot.tar"
        _git("archive", "-o", str(tar), "FETCH_HEAD")
        with tarfile.open(tar) as tf:
            # extraction filter keeps the archive from writing outside the repo (Python 3.12+)
            try:
                tf.extractall(ROOT_DIR, filter="data")
            except TypeError:  # older Python: no filter argument
                tf.extractall(ROOT_DIR)
    print(f"snapshot restored to {DATA_DIR}", flush=True)


def ensure_data() -> None:
    """Make sure data/ exists before a sync; fetch it once if this is a fresh clone."""
    if data_present():
        return
    if not _has_remote():
        raise SystemExit(MANUAL_STEPS)
    fetch_snapshot()
    if not data_present():
        raise SystemExit(f"snapshot fetch did not produce {META_FILE}.\n\n{MANUAL_STEPS}")


if __name__ == "__main__":
    ensure_data()
    print("data present" if data_present() else "data missing")
