"""Shared ingestion logic used by sync.py (daily).

A "batch" is a list of trade dates whose parsed rows are committed to the
store together. sync uses one batch per day (crash-safe with
meta['in_progress'] bookkeeping).
"""

from __future__ import annotations

import logging
from datetime import date, datetime, timedelta
from typing import Callable, Dict, List, Optional, Tuple

import pandas as pd

from config import (
    PE_ALERT_HIGH,
    PE_ALERT_LOW,
    PE_ALERTS_FILE,
    SEC_FULL_START_DATE,
    START_DATE,
    TZ_IN,
)
from parsers import merge_delivery, parse_bhav, parse_delivery_full, parse_indices, parse_mto
from scraper import raw_path
from storage import IndexStore, Store
from symbol_master import EntityResolver

logger = logging.getLogger("pipeline")

ProgressFn = Callable[[str, int, int, str], None]


def _noop(stage: str, i: int, n: int, msg: str) -> None:
    pass


# ------------------------------------------------------------------ calendar helpers


def available_trade_dates(start: date = START_DATE, end: Optional[date] = None) -> List[date]:
    """Dates for which a raw bhavcopy exists on disk (the authoritative
    historical trading calendar)."""
    from parsers import date_from_bhav_filename
    from config import RAW_DIR

    end = end or date.today()
    out = []
    for year_dir in sorted((RAW_DIR / "bhav").glob("*")):
        if not year_dir.is_dir():
            continue
        for p in year_dir.iterdir():
            d = date_from_bhav_filename(p.name)
            if d and start <= d <= end and p.stat().st_size > 0:
                out.append(d)
    return sorted(set(out))


def holiday_key(d: date) -> str:
    return d.strftime("%d-%b-%Y")


def is_candidate_trading_day(d: date, holidays: Dict[str, str], special_sessions: List[str]) -> bool:
    if d.isoformat() in special_sessions:
        return True
    if d.weekday() >= 5:
        return False
    desc = holidays.get(holiday_key(d))
    if desc and "laxmi pujan" in desc.lower():  # Muhurat trading day: market open
        return True
    return desc is None


def candidate_dates(last_synced: date, end: date, holidays: Dict[str, str], special_sessions: List[str]) -> List[date]:
    """Dates after last_synced up to and including end that may have traded."""
    out = []
    d = last_synced + timedelta(days=1)
    while d <= end:
        if is_candidate_trading_day(d, holidays, special_sessions):
            out.append(d)
        d += timedelta(days=1)
    return out


def eod_published_cutoff(now: Optional[datetime] = None) -> date:
    """Latest date whose EOD reports can be expected to exist right now."""
    from config import EOD_PUBLISH_HOUR

    now = now or datetime.now(TZ_IN)
    if now.hour < EOD_PUBLISH_HOUR:
        return now.date() - timedelta(days=1)
    return now.date()


# ------------------------------------------------------------------ parsing one day


def load_day(d: date) -> Optional[pd.DataFrame]:
    """Parse bhavcopy (+ delivery) for a date from raw files. None if no bhavcopy."""
    bhav = raw_path("bhav", d)
    if not bhav.exists() or bhav.stat().st_size == 0:
        return None
    try:
        df = parse_bhav(bhav, d)
    except Exception as e:
        logger.error("failed to parse %s: %s", bhav.name, e)
        raise
    if df.empty:
        logger.warning("%s: bhavcopy has no equity rows", d)
        return None

    dlv = None
    full = raw_path("delivery", d)
    mto = raw_path("mto", d)
    try:
        if d >= SEC_FULL_START_DATE and full.exists() and full.stat().st_size > 0:
            dlv = parse_delivery_full(full)
        elif mto.exists() and mto.stat().st_size > 0:
            dlv = parse_mto(mto)
        elif full.exists() and full.stat().st_size > 0:
            dlv = parse_delivery_full(full)
    except Exception as e:
        logger.warning("%s: delivery parse failed (%s) - continuing without delivery", d, e)
        dlv = None
    return merge_delivery(df, dlv)


def has_delivery(d: date) -> bool:
    for kind in ("delivery", "mto"):
        p = raw_path(kind, d)
        if p.exists() and p.stat().st_size > 0:
            return True
    return False


def load_indices_day(d: date) -> Optional[pd.DataFrame]:
    p = raw_path("indices", d)
    if not p.exists() or p.stat().st_size == 0:
        return None
    try:
        return parse_indices(p, d)
    except Exception as e:
        logger.warning("%s: indices parse failed: %s", d, e)
        return None


# ------------------------------------------------------------------ committing batches


def _alias_map(renames: List[Tuple[str, str]]) -> Dict[str, str]:
    alias: Dict[str, str] = {}
    for old, new in renames:
        for k, v in list(alias.items()):
            if v == old:
                alias[k] = new
        alias[old] = new
    return alias


def commit_batch(store: Store, frames: List[pd.DataFrame], renames: List[Tuple[str, str]]) -> List[str]:
    """Apply file renames, then append rows grouped by final key. Returns keys touched."""
    for old, new in renames:
        store.rename(old, new)
    if not frames:
        return []
    alias = _alias_map(renames)
    big = pd.concat(frames, ignore_index=True)
    if alias:
        big["key"] = big["key"].map(lambda k: alias.get(k, k))
    touched = []
    for key, grp in big.groupby("key", sort=False):
        store.append(key, grp.drop(columns="key"))
        touched.append(key)
    return touched


def ingest_dates(
    dates: List[date],
    resolver: EntityResolver,
    store: Store,
    index_store: IndexStore,
    batch_by: str = "year",
    progress: ProgressFn = _noop,
    on_day_committed: Optional[Callable[[date, List[str]], None]] = None,
    before_commit: Optional[Callable[[List[date], List[str]], None]] = None,
) -> Dict[str, int]:
    """Parse and store the given trade dates in chronological order."""
    stats = {"days": 0, "rows": 0, "renames": 0}
    n = len(dates)
    batches: List[List[date]] = []
    if batch_by == "year":
        cur: List[date] = []
        for d in dates:
            if cur and d.year != cur[-1].year:
                batches.append(cur)
                cur = []
            cur.append(d)
        if cur:
            batches.append(cur)
    else:
        batches = [[d] for d in dates]

    done = 0
    for batch in batches:
        frames: List[pd.DataFrame] = []
        renames: List[Tuple[str, str]] = []
        index_frames: List[pd.DataFrame] = []
        for d in batch:
            df = load_day(d)
            done += 1
            if df is None:
                continue
            keys, r = resolver.resolve_day(df, d)
            df = df.assign(key=keys.to_numpy())
            frames.append(df)
            renames.extend(r)
            idx = load_indices_day(d)
            if idx is not None:
                index_frames.append(idx)
            stats["days"] += 1
            stats["rows"] += len(df)
            if done % 25 == 0 or done == n:
                progress("ingest", done, n, f"{d} parsed")

        keys_in_batch = sorted({k for f in frames for k in f["key"].unique()}) if frames else []
        if before_commit:
            before_commit(batch, keys_in_batch)
        progress("commit", done, n, f"writing {len(keys_in_batch)} securities for {batch[0]}..{batch[-1]}")
        touched = commit_batch(store, frames, renames)
        stats["renames"] += len(renames)
        if index_frames:
            index_store.append_day(pd.concat(index_frames, ignore_index=True))
        if on_day_committed:
            on_day_committed(batch[-1], touched)
        del frames, index_frames
    return stats


# ------------------------------------------------------------------ delivery backfill


def backfill_delivery(store: Store, dates: List[date], progress: ProgressFn = _noop) -> int:
    """Fill deliv_qty/deliv_pct/trades for dates already in the store.

    All requested days are parsed first, then every security file is
    rewritten at most once (one pass over the store)."""
    frames = []
    for d in dates:
        df = load_day(d)
        if df is None or df["deliv_qty"].isna().all():
            continue
        frames.append(df[["date", "symbol", "deliv_qty", "deliv_pct", "trades"]])
    if not frames:
        return 0
    dlv = pd.concat(frames, ignore_index=True).set_index(["date", "symbol"])
    stamps = set(dlv.index.get_level_values(0).unique())
    keys = store.list_keys()
    updated = 0
    for i, key in enumerate(keys, 1):
        s = store.read(key)
        m = s["date"].isin(stamps)
        if not m.any():
            continue
        idx = pd.MultiIndex.from_arrays([s.loc[m, "date"], s.loc[m, "symbol"]])
        hit = idx.isin(dlv.index)
        if not hit.any():
            continue
        rows = dlv.loc[idx[hit]]
        target = s.index[m][hit]
        s.loc[target, "deliv_qty"] = rows["deliv_qty"].to_numpy()
        s.loc[target, "deliv_pct"] = rows["deliv_pct"].to_numpy()
        need_tr = s.loc[target, "trades"].isna().to_numpy() & rows["trades"].notna().to_numpy()
        if need_tr.any():
            s.loc[target[need_tr], "trades"] = rows["trades"].to_numpy()[need_tr]
        store.write(key, s)
        updated += 1
        if i % 500 == 0:
            progress("backfill", i, len(keys), key)
    return updated


# ------------------------------------------------------------------ PE alerts


def check_pe_alert(d: date, index_day: Optional[pd.DataFrame]) -> Optional[str]:
    if index_day is None or index_day.empty:
        return None
    row = index_day[index_day["index"].str.lower() == "nifty 50"]
    if row.empty or pd.isna(row["pe"].iloc[0]):
        return None
    pe = float(row["pe"].iloc[0])
    msg = None
    if pe <= PE_ALERT_LOW:
        msg = f"ALERT: Nifty 50 PE {pe:.2f} <= {PE_ALERT_LOW} (cheap)"
    elif pe >= PE_ALERT_HIGH:
        msg = f"ALERT: Nifty 50 PE {pe:.2f} >= {PE_ALERT_HIGH} (expensive)"
    if msg:
        PE_ALERTS_FILE.parent.mkdir(parents=True, exist_ok=True)
        new = not PE_ALERTS_FILE.exists()
        with PE_ALERTS_FILE.open("a", encoding="utf-8") as f:
            if new:
                f.write("date,pe,message\n")
            f.write(f"{d.isoformat()},{pe:.2f},{msg}\n")
        logger.warning(msg)
    else:
        logger.info("Nifty 50 PE %.2f", pe)
    return msg
