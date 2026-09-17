"""Incremental daily sync - the entrypoint to run every evening / weekly.

    python sync.py            # fetch everything since meta.last_synced
    python sync.py --force    # also try today's date even before 18:00 IST
    python sync.py --no-breadth

Flow
  1. recover from an interrupted run (drop partially written date)
  2. refresh NSE holiday list for the current year
  3. retry delivery reports that were unavailable earlier (backfill)
  4. download bhavcopy (+ delivery, indices, PR) for each candidate trading day
  5. ingest day by day, advancing meta.last_synced after each commit
  6. fetch new corporate actions (NSE API + PR zips), recompute adj_factor for
     affected securities, verify ex-date continuity
  7. rebuild market breadth, symbol master, log Nifty PE alert

Schedule (Windows Task Scheduler, runs 19:00 IST Mon-Fri):
  schtasks /Create /SC WEEKLY /D MON,TUE,WED,THU,FRI /ST 19:00 /TN "NSE EOD Sync" ^
    /TR "\"C:\\path\\to\\.venv\\Scripts\\python.exe\" \"C:\\path\\to\\sync.py\""
Linux/macOS cron:  0 19 * * 1-5  cd /path/to/project && .venv/bin/python sync.py
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import date, datetime, timedelta
from typing import Callable, Dict, List, Optional

import pandas as pd

from adjuster import (
    ADJUSTING_TYPES,
    api_rows_to_frame,
    apply_adjustments,
    build_actions_table,
    load_actions,
    load_pr_actions,
    save_actions,
    save_warnings,
)
from bootstrap import ensure_data
from config import ADJ_WARNINGS_FILE, LOG_DIR, SYNC_LOG_FILE, TZ_IN, ensure_dirs
from pipeline import (
    backfill_delivery,
    candidate_dates,
    check_pe_alert,
    eod_published_cutoff,
    has_delivery,
    holiday_key,
    ingest_dates,
    load_indices_day,
)
from scraper import NSEClient, raw_path
from storage import IndexStore, Store, load_meta, save_meta
from symbol_master import EntityResolver

logger = logging.getLogger("sync")

ProgressFn = Callable[[str, int, int, str], None]
# Machine-readable outcome of the last run (read by healthcheck.py)
SYNC_STATUS_FILE = LOG_DIR / "last_sync_status.json"


def _print_progress(stage: str, i: int, n: int, msg: str) -> None:
    pct = f"{i / n * 100:5.1f}%" if n else "     "
    print(f"[{stage:8s}] {pct} {i}/{n} {msg}", flush=True)


# ------------------------------------------------------------------ helpers


def recover_interrupted(meta: dict, store: Store, istore: IndexStore) -> None:
    ip = meta.get("in_progress")
    if not ip:
        return
    d = date.fromisoformat(ip["date"])
    keys = ip.get("keys") or store.list_keys()
    logger.warning("recovering from interrupted sync of %s: removing partial rows from %d files", d, len(keys))
    store.remove_dates(keys, [d])
    istore.remove_dates([d])
    meta["in_progress"] = None
    save_meta(meta)


def refresh_holidays(client: NSEClient, meta: dict, year: int) -> None:
    if meta.get("holiday_year") == year and meta.get("holidays"):
        return
    try:
        hol = client.holidays()
        if hol:
            meta["holidays"] = hol
            meta["holiday_year"] = year
            save_meta(meta)
            logger.info("holiday list refreshed: %d holidays for %d", len(hol), year)
    except Exception as e:
        logger.warning("could not refresh holiday list: %s", e)


def refresh_special_sessions(client: NSEClient, meta: dict) -> None:
    """Pick up Saturday / special live-trading sessions from NSE circulars (CMTR)."""
    last = meta.get("special_sessions_checked")
    start = date.fromisoformat(last) if last else date.today() - timedelta(days=30)
    try:
        rows = client.circulars("CMTR", start, date.today() + timedelta(days=30))
    except Exception as e:
        logger.info("circulars check skipped: %s", e)
        return
    from dateutil import parser as dparser

    sessions = set(meta.get("special_sessions") or [])
    for c in rows:
        sub = str(c.get("sub", "")).lower()
        if "live trading session" in sub or ("special" in sub and "trading session" in sub):
            try:
                d = dparser.parse(sub, fuzzy=True).date()
            except (ValueError, OverflowError):
                continue
            if d.isoformat() not in sessions:
                sessions.add(d.isoformat())
                logger.warning("special trading session detected: %s (%s)", d, c.get("sub"))
    meta["special_sessions"] = sorted(sessions)
    meta["special_sessions_checked"] = date.today().isoformat()
    save_meta(meta)


def sync_actions(client: NSEClient, meta: dict, resolver: EntityResolver, new_pr_dates: List[date], store: Store) -> pd.DataFrame:
    """Fetch new corporate actions and merge them into the actions table."""
    existing = load_actions()
    today = datetime.now(TZ_IN).date()
    through = meta.get("actions_fetched_through")
    start = (date.fromisoformat(through) - timedelta(days=7)) if through else (today - timedelta(days=60))
    end = today + timedelta(days=45)

    api_frames = []
    windows = []
    w0 = start
    while w0 <= end:
        w1 = min(w0 + timedelta(days=29), end)
        windows.append((w0, w1))
        w0 = w1 + timedelta(days=1)
    for segment in ("equities", "sme", "mf"):
        for w0, w1 in windows:
            try:
                rows = client.actions(segment, w0, w1)
                api_frames.append(api_rows_to_frame(rows))
                logger.info("NSE actions API %s: %d rows (%s..%s)", segment, len(rows), w0, w1)
            except Exception as e:
                logger.warning("actions API %s %s..%s failed: %s", segment, w0, w1, e)
    api_frames = [f for f in api_frames if len(f)]
    api = pd.concat(api_frames, ignore_index=True) if api_frames else None

    pr = None
    if new_pr_dates:
        zips = [raw_path("pr", d) for d in new_pr_dates if raw_path("pr", d).exists()]
        if zips:
            pr = load_pr_actions(zips)
            logger.info("PR zips: %d action rows from %d files", len(pr), len(zips))

    table = build_actions_table(resolver, [f for f in (api, pr) if f is not None], existing=existing, store=store)
    save_actions(table)
    if api is not None and len(api):
        meta["actions_fetched_through"] = today.isoformat()
    meta["last_actions_sync"] = today.isoformat()
    save_meta(meta)
    return table


def changed_action_keys(old: pd.DataFrame, new: pd.DataFrame) -> set:
    def sig(df: pd.DataFrame) -> set:
        adj = df[df["type"].isin(ADJUSTING_TYPES) & df["key"].notna()]
        return set(zip(adj["key"], adj["ex_date"].astype(str), adj["factor"].round(8)))

    a, b = sig(old), sig(new)
    return {k for k, _, _ in a.symmetric_difference(b)}


# ------------------------------------------------------------------ main flow


def prepare_redo(client: NSEClient, store: Store, istore: IndexStore, meta: dict, d: date) -> None:
    """Force re-download of every report for one already-synced day and drop that
    day's rows so it is ingested again (used after a bad/corrupt raw file or a parser fix)."""
    client.missing.remove("bhav", d)
    p = client.download_report("bhav", d, force=True)
    if p is None:
        raise SystemExit(f"NSE has no bhavcopy for {d}; nothing to redo")
    for kind in ("indices", "pr"):
        try:
            client.download_report(kind, d, force=True)
        except Exception as e:
            logger.warning("%s %s: %s", kind, d, e)
    try:
        client.download_delivery(d, force=True)
    except Exception as e:
        logger.warning("delivery %s: %s", d, e)
    n = store.remove_dates(store.list_keys(), [d])
    istore.remove_dates([d])
    iso = d.isoformat()
    if iso in (meta.get("pending_delivery") or []):
        meta["pending_delivery"].remove(iso)
    meta.get("holidays", {}).pop(holiday_key(d), None)
    save_meta(meta)
    logger.warning("redo %s: reports re-downloaded, rows removed from %d security files; re-ingesting", d, n)


def run_sync(
    force: bool = False,
    do_breadth: bool = True,
    progress: ProgressFn = _print_progress,
    redo: Optional[date] = None,
) -> Dict:
    ensure_dirs()
    meta = load_meta()
    if not meta.get("last_synced"):
        raise SystemExit(
            "database not found under data/ - run `python sync.py` (it fetches the snapshot "
            "from the data branch) or restore it from a backup"
        )

    store, istore = Store(), IndexStore()
    resolver = EntityResolver.load()
    client = NSEClient()
    summary: Dict = {"ingested": [], "missing": [], "pending": [], "index_parse_failures": [], "warnings": 0, "renames": 0}

    try:
        recover_interrupted(meta, store, istore)

        now = datetime.now(TZ_IN)
        refresh_holidays(client, meta, now.year)
        refresh_special_sessions(client, meta)

        # 1. delivery backfill for dates ingested without a delivery report
        pending = list(meta.get("pending_delivery") or [])
        got: List[date] = []
        for iso in pending:
            d = date.fromisoformat(iso)
            try:
                p = client.download_delivery(d)
            except Exception as e:
                logger.info("delivery %s still unavailable: %s", d, e)
                p = None
            if p is not None:
                got.append(d)
                meta["pending_delivery"].remove(iso)
            elif (now.date() - d).days > 10:
                logger.warning("delivery report for %s never published - giving up", d)
                meta["pending_delivery"].remove(iso)
        if got:
            n = backfill_delivery(store, got, progress)
            logger.info("delivery backfilled for %s (%d securities)", [d.isoformat() for d in got], n)
        save_meta(meta)

        # 2. candidate dates
        last = date.fromisoformat(meta["last_synced"])
        if redo is not None:
            if redo > last:
                raise SystemExit(f"--redo-date {redo} is after the last synced date {last}; run a normal sync instead")
            prepare_redo(client, store, istore, meta, redo)
            available: List[date] = [redo]
        else:
            end = now.date() if force else eod_published_cutoff(now)
            holidays = meta.get("holidays") or {}
            special = meta.get("special_sessions") or []
            candidates = candidate_dates(last, end, holidays, special)
            if not candidates:
                logger.info("up to date (last synced %s). Reports for %s expected after 18:00 IST.", last, end + timedelta(days=1))
                return summary

            # 3. downloads
            available = []
            n = len(candidates)
            for i, d in enumerate(candidates, 1):
                p = client.download_report("bhav", d)  # raises on network failure -> abort, never skip a day
                if p is None:
                    progress("download", i, n, f"{d} bhavcopy not available")
                    continue
                available.append(d)
                for kind in ("indices", "pr"):
                    try:
                        client.download_report(kind, d)
                    except Exception as e:
                        logger.warning("%s %s: %s", kind, d, e)
                try:
                    client.download_delivery(d)
                except Exception as e:
                    logger.warning("delivery %s: %s", d, e)
                progress("download", i, n, f"{d} ok")

            latest = max(available) if available else None
            for d in candidates:
                if d in available:
                    continue
                if latest and d < latest:
                    client.missing.add("bhav", d)
                    meta.setdefault("holidays", {})[holiday_key(d)] = "inferred (no bhavcopy published)"
                    summary["missing"].append(d.isoformat())
                    logger.info("%s: no bhavcopy but later dates exist -> treated as market holiday", d)
                else:
                    summary["pending"].append(d.isoformat())
            if not available:
                logger.info("no new reports published yet (last synced %s)", last)
                save_meta(meta)
                return summary

        # 4. ingest day by day
        def before_commit(batch: List[date], keys: List[str]) -> None:
            meta["in_progress"] = {"date": batch[-1].isoformat(), "keys": keys}
            save_meta(meta)

        def on_committed(d: date, keys: List[str]) -> None:
            meta["in_progress"] = None
            meta["last_synced"] = max(meta["last_synced"], d.isoformat())
            if not has_delivery(d):
                meta.setdefault("pending_delivery", []).append(d.isoformat())
            save_meta(meta)
            # persist identity state with the rows it describes, so a crash between two
            # days cannot leave the resolver behind the store
            resolver.save()
            summary["ingested"].append(d.isoformat())
            idx = load_indices_day(d)
            if idx is None and raw_path("indices", d).exists():
                summary["index_parse_failures"].append(d.isoformat())
                logger.error(
                    "%s: index report exists but could not be parsed - index rows for this day are missing. "
                    "Fix parse_indices in parsers.py, then run `python sync.py --redo-date %s`", d, d,
                )
            check_pe_alert(d, idx)
            logger.info("%s ingested (%d securities)", d, len(keys))

        stats = ingest_dates(
            available, resolver, store, istore, batch_by="day", progress=progress,
            before_commit=before_commit, on_day_committed=on_committed,
        )
        summary["renames"] = stats["renames"]
        resolver.save_outputs(as_of=available[-1])
        meta["symbol_count"] = len(resolver.entities)

        # 5. corporate actions + adjustments
        old_actions = load_actions()
        new_actions = sync_actions(client, meta, resolver, available, store)
        keys = changed_action_keys(old_actions, new_actions)
        first_new = pd.Timestamp(available[0])
        future = new_actions[new_actions["type"].isin(ADJUSTING_TYPES) & (new_actions["ex_date"] >= first_new)]
        keys |= set(future["key"].dropna())
        if keys:
            rewritten, warn = apply_adjustments(store, new_actions, keys=sorted(keys), progress=lambda i, n_, k: progress("adjust", i, n_, k))
            logger.info("adjustment factors recomputed for %d securities (%d rewritten)", len(keys), rewritten)
            if len(warn):
                prev = pd.read_csv(ADJ_WARNINGS_FILE) if ADJ_WARNINGS_FILE.exists() else pd.DataFrame()
                allw = pd.concat([prev, warn], ignore_index=True).drop_duplicates(subset=["key", "ex_date", "type"], keep="last")
                save_warnings(allw)
                for w in warn.itertuples():
                    logger.warning("ADJUSTMENT CHECK %s %s %s factor=%s ratio=%s: %s", w.key, w.ex_date, w.type, w.factor, w.ratio, w.issue)
            summary["warnings"] = len(warn)

        # 6. breadth
        if do_breadth:
            from breadth import rebuild_breadth

            df = rebuild_breadth(store, progress=lambda i, n_, k: progress("breadth", i, n_, k))
            meta["breadth_last_date"] = df["date"].max().date().isoformat() if len(df) else None

        meta["last_sync_run"] = datetime.now(TZ_IN).isoformat(timespec="seconds")
        save_meta(meta)
        logger.info("sync complete: %s", {k: (v if not isinstance(v, list) else len(v)) for k, v in summary.items()})
        return summary
    finally:
        client.close()


def write_status(ok: bool, summary: Optional[Dict] = None, error: Optional[str] = None) -> None:
    """Record the outcome of this run in logs/last_sync_status.json (atomic)."""
    import json
    import os

    try:
        last_synced = load_meta().get("last_synced")
    except Exception as e:  # meta unreadable is itself the error being reported
        last_synced = None
        error = error or str(e)
    payload = {
        "ok": ok,
        "finished_at": datetime.now(TZ_IN).isoformat(timespec="seconds"),
        "last_synced": last_synced,
        "summary": summary,
        "error": error,
    }
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    tmp = SYNC_STATUS_FILE.with_suffix(".tmp")
    tmp.write_text(json.dumps(payload, indent=1, default=str), encoding="utf-8")
    os.replace(tmp, SYNC_STATUS_FILE)


def main(argv: Optional[List[str]] = None) -> None:
    ap = argparse.ArgumentParser(description="Incremental NSE EOD sync")
    ap.add_argument("--force", action="store_true", help="attempt today's reports even before 18:00 IST")
    ap.add_argument("--no-breadth", action="store_true", help="skip market breadth rebuild")
    ap.add_argument("--no-fetch", action="store_true",
                    help="do not fetch the data snapshot from the data branch when data/ is missing")
    ap.add_argument(
        "--redo-date", metavar="YYYY-MM-DD", default=None,
        help="re-download every report for one already-synced day and ingest it again "
             "(after a corrupt raw file, a parser fix, or an NSE correction)",
    )
    args = ap.parse_args(argv)
    redo = date.fromisoformat(args.redo_date) if args.redo_date else None

    if not args.no_fetch:
        ensure_data()  # fresh clone: pull the snapshot from the data branch, then sync on top

    LOG_DIR.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        handlers=[logging.StreamHandler(sys.stdout), logging.FileHandler(SYNC_LOG_FILE, encoding="utf-8")],
    )
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    try:
        summary = run_sync(force=args.force, do_breadth=not args.no_breadth, redo=redo)
    except BaseException as e:  # SystemExit / KeyboardInterrupt are failures too for the status file
        logger.exception("sync failed")
        write_status(False, error=f"{type(e).__name__}: {e}")
        sys.exit(1)
    write_status(True, summary=summary)


if __name__ == "__main__":
    main()
