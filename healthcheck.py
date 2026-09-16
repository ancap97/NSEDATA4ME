"""One-screen health report for the database. Run it any time; exit code 0 = healthy.

    python healthcheck.py          # human readable
    python healthcheck.py --json   # machine readable (for scripts / an LLM)

It answers: is the data current, did the last sync succeed, is anything waiting
for a human (adjustment warnings, unconfirmed actions, identity reviews,
unexplained price gaps), and do meta.json, the store and breadth agree.
Exit code 1 when something needs attention (details in the output).
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime
from pathlib import Path

import pandas as pd

from config import (
    ACTIONS_DIR,
    ADJ_WARNINGS_FILE,
    BREADTH_FILE,
    DATA_DIR,
    FAILED_DATES_FILE,
    LOG_DIR,
    STORE_DIR,
    TZ_IN,
)
from pipeline import candidate_dates, eod_published_cutoff
from storage import Store, load_meta

STATUS_FILE = LOG_DIR / "last_sync_status.json"
IDENTITY_REVIEW_FILE = LOG_DIR / "identity_review.csv"
IMPLIED_FILE = LOG_DIR / "implied_actions.csv"
UNCONFIRMED_FILE = ACTIONS_DIR / "unconfirmed_actions.csv"

# more than this many trading days behind = stale (a long weekend + one failed run is fine)
STALE_TRADING_DAYS = 3


def _csv_rows(p: Path) -> int:
    if not p.exists() or p.stat().st_size == 0:
        return 0
    try:
        return len(pd.read_csv(p))
    except Exception:
        return -1


def collect() -> dict:
    out: dict = {"problems": [], "notes": []}
    try:
        meta = load_meta()
    except RuntimeError as e:
        out["problems"].append(str(e))
        return out

    now = datetime.now(TZ_IN)
    last = meta.get("last_synced")
    out["last_synced"] = last
    if not last:
        out["problems"].append("database not built (meta.last_synced empty) - restore data/ from a backup")
        return out

    # freshness: how many probable trading days are missing right now
    cutoff = eod_published_cutoff(now)
    behind = candidate_dates(date.fromisoformat(last), cutoff, meta.get("holidays") or {}, meta.get("special_sessions") or [])
    out["trading_days_behind"] = len(behind)
    out["expected_through"] = cutoff.isoformat()
    if len(behind) > STALE_TRADING_DAYS:
        out["problems"].append(f"data is {len(behind)} trading days behind ({last} -> {cutoff}); run `python sync.py`")
    elif behind:
        out["notes"].append(f"{len(behind)} trading day(s) not yet synced: {[d.isoformat() for d in behind]}")

    if meta.get("in_progress"):
        out["problems"].append(f"a sync was interrupted while writing {meta['in_progress'].get('date')}; the next `python sync.py` rolls it back automatically")

    # last run outcome
    if STATUS_FILE.exists():
        try:
            st = json.loads(STATUS_FILE.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            st = {"ok": False, "error": "status file unreadable"}
        out["last_sync_run"] = {"ok": st.get("ok"), "finished_at": st.get("finished_at"), "error": st.get("error")}
        if not st.get("ok"):
            out["problems"].append(f"last sync FAILED at {st.get('finished_at')}: {st.get('error')} (see data/logs/sync.log)")
        summ = st.get("summary") or {}
        if summ.get("index_parse_failures"):
            out["problems"].append(f"index report could not be parsed for {summ['index_parse_failures']} - fix parsers.parse_indices, then sync.py --redo-date")
    else:
        out["last_sync_run"] = None
        out["notes"].append("no sync has run since this status file was introduced")

    # store / breadth agreement
    store = Store()
    n_files = sum(1 for _ in STORE_DIR.glob("*.parquet"))
    out["securities"] = n_files
    out["symbol_count_meta"] = meta.get("symbol_count")
    rel = store.read("RELIANCE", columns=["date"])
    if rel.empty:
        out["problems"].append("RELIANCE missing from the store - the store looks damaged")
    else:
        store_last = rel["date"].max().date().isoformat()
        out["store_last_date"] = store_last
        if store_last != last:
            out["problems"].append(f"store last date {store_last} != meta.last_synced {last}")
    if BREADTH_FILE.exists():
        b_last = meta.get("breadth_last_date")
        out["breadth_last_date"] = b_last
        if b_last != last:
            out["problems"].append(f"breadth last date {b_last} != last synced {last}; run `python breadth.py`")
    else:
        out["problems"].append("breadth file missing; run `python breadth.py`")
    if not meta.get("adjustments_applied"):
        out["problems"].append("adjustments not applied (meta.adjustments_applied false) - restore data/ from a backup")

    # corporate actions
    out["actions_fetched_through"] = meta.get("actions_fetched_through")
    out["pending_delivery"] = meta.get("pending_delivery") or []
    if len(out["pending_delivery"]) > 3:
        out["notes"].append(f"{len(out['pending_delivery'])} days still waiting for a delivery report")

    # things waiting for a human
    review = {
        "adjustment_warnings": _csv_rows(ADJ_WARNINGS_FILE),
        "unconfirmed_actions": _csv_rows(UNCONFIRMED_FILE),
        "identity_review": _csv_rows(IDENTITY_REVIEW_FILE),
    }
    if IMPLIED_FILE.exists() and IMPLIED_FILE.stat().st_size > 0:
        try:
            imp = pd.read_csv(IMPLIED_FILE)
            review["implied_unexplained"] = int((imp["status"] == "unexplained").sum()) if len(imp) else 0
        except Exception:
            review["implied_unexplained"] = -1
    out["review_queue"] = review
    if review["adjustment_warnings"]:
        out["problems"].append(f"{review['adjustment_warnings']} adjustment warning(s) in data/logs/adjustment_warnings.csv - an applied split/bonus does not match the price move")
    if review["identity_review"]:
        out["notes"].append(f"{review['identity_review']} identity decision(s) to confirm in data/logs/identity_review.csv")
    if review["unconfirmed_actions"]:
        out["notes"].append(f"{review['unconfirmed_actions']} announced action(s) dropped as unconfirmed by prices (data/actions/unconfirmed_actions.csv)")

    # recent download failures
    if FAILED_DATES_FILE.exists() and FAILED_DATES_FILE.stat().st_size > 0:
        try:
            f = pd.read_csv(FAILED_DATES_FILE)
            recent = f[pd.to_datetime(f["logged_at"], utc=True, errors="coerce") >= pd.Timestamp.now(tz="UTC") - pd.Timedelta(days=7)]
            out["download_failures_7d"] = len(recent)
            if len(recent):
                out["notes"].append(f"{len(recent)} download failure(s) in the last 7 days (data/logs/failed_dates.csv)")
        except Exception:
            pass

    out["ok"] = not out["problems"]
    return out


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args(argv)
    info = collect()
    if args.json:
        print(json.dumps(info, indent=1, default=str))
    else:
        print(f"NSE EOD database at {DATA_DIR}")
        print(f"  last synced      : {info.get('last_synced')}  (expected through {info.get('expected_through')}, "
              f"{info.get('trading_days_behind', '?')} trading day(s) behind)")
        run = info.get("last_sync_run")
        print(f"  last sync run    : {'OK' if run and run['ok'] else 'FAILED' if run else 'unknown'}"
              f"{'  ' + run['finished_at'] if run else ''}")
        print(f"  securities       : {info.get('securities')}  store last {info.get('store_last_date')}  breadth last {info.get('breadth_last_date')}")
        print(f"  actions fetched  : through {info.get('actions_fetched_through')}")
        print(f"  review queue     : {info.get('review_queue')}")
        for n in info["notes"]:
            print(f"  note    - {n}")
        for p in info["problems"]:
            print(f"  PROBLEM - {p}")
        print("  STATUS: " + ("HEALTHY" if info.get("ok") else "NEEDS ATTENTION"))
    return 0 if info.get("ok") else 1


if __name__ == "__main__":
    sys.exit(main())
