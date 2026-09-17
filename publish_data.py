"""Put the current data/ folder on the orphan `data` branch, ready to force-push.

`main` holds code only, so its history stays small. The processed data lives on a separate
branch that is *replaced* (not added to) on every publish: each run writes a fresh, parentless
commit, so the repo never accumulates old copies of the Parquet files.

    python publish_data.py            # build the commit, print the push command
    python publish_data.py --push     # build it and force-push to origin

Nothing in your working tree or on `main` is touched: the commit is assembled through a
temporary git index, so no checkout or stash is involved.

Getting the data on another machine (see README):

    git fetch origin data --depth 1
    git archive -o data.tar FETCH_HEAD && tar -xf data.tar && del data.tar
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import tempfile
from datetime import date
from pathlib import Path

from config import META_FILE, ROOT_DIR

BRANCH = "data"
REMOTE = "origin"
# data/raw is 3 GB of re-downloadable NSE reports and data/logs is machine-local
EXCLUDES = [":(exclude)data/raw", ":(exclude)data/logs", ":(exclude)data/*.tmp",
            ":(exclude)data/*.bak", ":(exclude)data/store/*.tmp"]


def git(*args: str, index: Path | None = None) -> str:
    env = {**os.environ, "GIT_INDEX_FILE": str(index)} if index else None
    r = subprocess.run(["git", *args], cwd=ROOT_DIR, env=env, check=True, capture_output=True, text=True)
    return r.stdout.strip()


def build_commit(index: Path, last_synced: str) -> str:
    """Write data/ into a fresh parentless commit on refs/heads/data. Returns its sha."""
    git("read-tree", "--empty", index=index)
    git("add", "--force", "--", "data", *EXCLUDES, index=index)
    tree = git("write-tree", index=index)
    message = f"NSE EOD data through {last_synced} (snapshot taken {date.today().isoformat()})"
    sha = git("commit-tree", tree, "-m", message)  # no -p: every publish replaces the branch
    git("update-ref", f"refs/heads/{BRANCH}", sha)
    return sha


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--push", action="store_true", help=f"force-push {BRANCH} to {REMOTE} afterwards")
    args = ap.parse_args()

    meta = json.loads(META_FILE.read_text(encoding="utf-8"))
    if meta.get("in_progress"):
        raise SystemExit(f"sync is mid-commit for {meta['in_progress']['date']}; finish or resume sync.py first")
    last_synced = meta["last_synced"]

    with tempfile.TemporaryDirectory() as tmp:
        sha = build_commit(Path(tmp) / "index", last_synced)

    files = git("ls-tree", "-r", "--name-only", sha).count("\n") + 1
    print(f"{BRANCH} branch now at {sha[:9]}: {files} files, data through {last_synced}")
    if args.push:
        print(f"pushing to {REMOTE}/{BRANCH} (this replaces the branch)...", flush=True)
        subprocess.run(["git", "push", "--force", REMOTE, BRANCH], cwd=ROOT_DIR, check=True)
        print("pushed")
    else:
        print(f"to publish it:  git push --force {REMOTE} {BRANCH}")


if __name__ == "__main__":
    main()
