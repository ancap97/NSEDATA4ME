"""Golden-output snapshots of the public read API against a real data/ snapshot.

The `data` branch snapshot is too large for the repo, so only digests are committed
(tests/golden/golden_hashes.json). Point the script at a checkout of the data branch:

    git fetch --depth 1 origin data:refs/remotes/origin/data
    git worktree add --detach ../nse-data origin/data
    python -m tests.golden.golden check  ../nse-data/data      # exit 1 on any difference
    python -m tests.golden.golden record ../nse-data/data      # (re)write the digests

or run it through pytest:  NSE_GOLDEN_DATA=../nse-data/data pytest tests/test_golden.py

Every call is read-only; the snapshot directory is never written to by the loader paths used here.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
import time
from collections.abc import Callable
from pathlib import Path

import pandas as pd
import pytest

from tests._repoint import repoint

HASH_FILE = Path(__file__).with_name("golden_hashes.json")

# current, renamed (old names), delisted, split/bonus/manual-override and SME securities
SYMBOLS = [
    "RELIANCE", "TCS", "INFOSYSTCH", "ITC", "IRCTC", "HDFCBANK", "HDFC", "SUNTV", "ZOMATO",
    "IIFLWAM", "LTIM", "TATAMOTORS", "DHFL", "JETAIRWAYS", "SATYAMCOMP", "BAJFINANCE",
    "GULFPETRO", "KMSUGAR", "MOM30IETF", "AAKAAR",
]


def digest(obj) -> dict:
    """Content digest of a DataFrame / Series / list / str, sensitive to values, dtypes,
    column order, index and row order."""
    h = hashlib.sha256()
    if isinstance(obj, pd.Series):
        obj = obj.to_frame()
    if isinstance(obj, pd.DataFrame):
        info = {
            "shape": list(obj.shape),
            "columns_head": [str(c) for c in list(obj.columns)[:20]],
            "dtypes": sorted({str(t) for t in obj.dtypes}),
        }
        h.update(repr([str(c) for c in obj.columns]).encode())
        h.update(repr([str(t) for t in obj.dtypes]).encode())
        h.update(repr((obj.index.names, obj.columns.name, str(obj.index.dtype))).encode())
        if len(obj) and obj.shape[1]:
            h.update(pd.util.hash_pandas_object(obj, index=True).to_numpy().tobytes())
        elif len(obj):
            h.update(pd.util.hash_pandas_object(obj.index).to_numpy().tobytes())
        info["sha256"] = h.hexdigest()
        return info
    h.update(json.dumps(obj, sort_keys=True, default=str).encode())
    return {"sha256": h.hexdigest(), "len": len(obj) if hasattr(obj, "__len__") else None}


def _adj_factor_recompute() -> pd.DataFrame:
    """adj_factor recomputed from the actions table for every security, vs the stored column."""
    import adjuster
    from storage import Store

    store = Store()
    acts = adjuster.load_actions()
    by_key = {k: g for k, g in acts[acts["key"].notna()].groupby("key")}
    rows = []
    for key in store.list_keys():
        df = store.read(key, columns=["date", "adj_factor"])
        new = adjuster.compute_adj_factor(df["date"], by_key.get(key))
        mism = int((abs(new - df["adj_factor"].to_numpy()) > 1e-9 * abs(new)).sum())
        rows.append((key, len(df), hashlib.sha1(new.tobytes()).hexdigest(), mism))
    return pd.DataFrame(rows, columns=["key", "rows", "factor_sha1", "mismatches"])


def calls() -> list[tuple[str, Callable]]:
    import breadth
    import loader
    from storage import Store
    from symbol_master import EntityResolver

    S = SYMBOLS
    return [
        ("list_symbols", lambda: loader.list_symbols()),
        ("list_symbols_active_sme", lambda: loader.list_symbols(include_delisted=False, include_sme=True)),
        ("resolve_symbols", lambda: loader.resolve_symbols(S, include_sme=True)),
        ("load_data_syms_2024", lambda: loader.load_data(S, start_date="2024-01-01")),
        ("load_data_syms_raw_full", lambda: loader.load_data(S, adjusted=False)),
        ("load_data_syms_cols_window", lambda: loader.load_data(
            S, start_date="2010-01-01", end_date="2015-12-31", columns=["date", "symbol", "close", "volume", "adj_factor"])),
        ("load_data_syms_none_start", lambda: loader.load_data(["ITC", "INFY"], start_date=None, end_date="2006-01-31")),
        ("load_symbol_infosystch", lambda: loader.load_symbol("INFOSYSTCH")),
        ("load_symbol_zomato_raw", lambda: loader.load_symbol("ZOMATO", adjusted=False, start_date="2025-01-01")),
        ("load_wide_close_syms_2024", lambda: loader.load_wide("close", symbols=S, start_date="2024-01-01")),
        ("load_wide_close_all_2024", lambda: loader.load_wide("close", start_date="2024-01-01")),
        ("load_wide_volume_active_2025", lambda: loader.load_wide("volume", start_date="2025-01-01", include_delisted=False)),
        ("load_wide_close_raw_sme_2026", lambda: loader.load_wide("close", adjusted=False, start_date="2026-06-01", include_sme=True)),
        ("load_data_all_2026_sme", lambda: loader.load_data(start_date="2026-01-01", include_sme=True)),
        ("load_data_all_cols_2025", lambda: loader.load_data(start_date="2025-01-01", columns=["date", "symbol", "close", "deliv_qty"])),
        ("load_index_nifty50", lambda: loader.load_index("Nifty 50")),
        ("load_index_bank_2020", lambda: loader.load_index("Nifty Bank", start_date="2020-01-01", end_date="2024-12-31")),
        ("load_index_old_name", lambda: loader.load_index("S&P CNX Nifty")),
        ("list_indices", lambda: loader.list_indices()),
        ("load_breadth", lambda: loader.load_breadth()),
        ("load_breadth_window", lambda: loader.load_breadth(start_date="2020-01-01", end_date="2020-12-31")),
        ("load_actions", lambda: loader.load_actions()),
        ("load_actions_irctc_adj", lambda: loader.load_actions("IRCTC", adjusting_only=True)),
        ("load_actions_infosystch", lambda: loader.load_actions("INFOSYSTCH")),
        ("store_last_dates", lambda: pd.Series(Store().last_dates()).sort_index()),
        ("master_frame", lambda: EntityResolver.load().master_frame()),
        ("history_json", lambda: EntityResolver.load().history_json()),
        ("adj_factor_recompute", _adj_factor_recompute),
        ("compute_breadth", lambda: breadth.compute_breadth(Store())),
    ]


def snapshot(data_dir: Path, only: list[str] | None = None) -> tuple[dict, dict]:
    mp = pytest.MonkeyPatch()
    try:
        repoint(Path(data_dir), mp)
        out, timings = {}, {}
        for name, fn in calls():
            if only and name not in only:
                continue
            t0 = time.perf_counter()
            res = fn()
            timings[name] = round(time.perf_counter() - t0, 3)
            out[name] = digest(res)
        return out, timings
    finally:
        mp.undo()


def compare(expected: dict, actual: dict) -> list[str]:
    diffs = []
    for name, exp in expected.items():
        act = actual.get(name)
        if act is None:
            continue
        if act != exp:
            diffs.append(f"{name}: expected {exp} got {act}")
    return diffs


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("mode", choices=("record", "check", "time"))
    ap.add_argument("data_dir")
    ap.add_argument("--only", nargs="*")
    args = ap.parse_args(argv)
    actual, timings = snapshot(Path(args.data_dir), args.only)
    for k, v in timings.items():
        print(f"{v:8.3f}s  {k}")
    print(f"{sum(timings.values()):8.3f}s  TOTAL")
    if args.mode == "record":
        if args.only and HASH_FILE.exists():  # partial re-record keeps the other entries
            actual = {**json.loads(HASH_FILE.read_text()), **actual}
        HASH_FILE.write_text(json.dumps(actual, indent=1, sort_keys=True) + "\n")
        print(f"wrote {HASH_FILE}")
        return 0
    if args.mode == "check":
        diffs = compare(json.loads(HASH_FILE.read_text()), actual)
        for d in diffs:
            print("MISMATCH", d)
        print("golden: OK" if not diffs else f"golden: {len(diffs)} mismatch(es)")
        return 1 if diffs else 0
    return 0


if __name__ == "__main__":
    sys.exit(main())
