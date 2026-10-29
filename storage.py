"""Parquet storage: one file per security, one per index, plus meta.json.

Security files hold UNADJUSTED prices exactly as reported by NSE plus an
`adj_factor` column (product of all split/bonus factors with ex-date after the
row). Adjusted price = price / adj_factor, adjusted volume = volume * adj_factor.
Storing raw + factor means new corporate actions only rewrite one column and
the raw record is never mutated - the history is fully auditable and
reproducible.

All writes are atomic (temp file + os.replace) so a crash mid-sync cannot
leave a truncated Parquet file behind.
"""

from __future__ import annotations

import copy
import json
import multiprocessing
import os
import re
import shutil
from concurrent.futures import ProcessPoolExecutor
from datetime import date, datetime
from pathlib import Path
from typing import Callable, Dict, Iterable, Iterator, List, Optional, Sequence, Tuple

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.dataset as pads

from config import INDEX_DIR, META_FILE, STORE_COLUMNS, STORE_DIR, ensure_dirs

_INDEX_NAME_RE = re.compile(r"[^a-z0-9]+")

# Store columns whose pandas conversion is the same for every file, so one scan over many
# files gives what reading them one by one does. String columns are excluded: an all-null
# isin is stored as Arrow `null` and read back as object, not str.
SCAN_COLUMNS = ("date", "open", "high", "low", "close", "prev_close", "volume", "turnover", "trades",
                "deliv_qty", "deliv_pct", "adj_factor")


def _scan_signature(schema: pa.Schema, columns: Sequence[str]) -> Optional[tuple]:
    """Arrow type + pandas metadata of `columns` in one file (None if any is missing)."""
    try:
        meta = {c["name"]: (c["pandas_type"], c["numpy_type"])
                for c in json.loads(schema.metadata[b"pandas"])["columns"]}
    except (KeyError, TypeError, ValueError):
        return None
    sig = []
    for c in columns:
        i = schema.get_field_index(c)
        if i < 0 or c not in meta:
            return None
        sig.append((c, str(schema.field(i).type), meta[c]))
    return tuple(sig)


class _JsonEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, (datetime, date)):
            return o.isoformat()
        if isinstance(o, pd.Timestamp):
            return o.isoformat()
        return super().default(o)


def _atomic_parquet(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    df.to_parquet(tmp, index=False, engine="pyarrow", compression="zstd")
    os.replace(tmp, path)


def normalise_store_frame(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    if "adj_factor" not in df:
        df["adj_factor"] = 1.0
    for col in STORE_COLUMNS:
        if col not in df:
            df[col] = None
    df = df.loc[:, STORE_COLUMNS]
    df["date"] = pd.to_datetime(df["date"]).astype("datetime64[ns]")
    for col in ("open", "high", "low", "close", "prev_close", "turnover", "deliv_pct", "adj_factor"):
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("float64")
    for col in ("volume", "trades", "deliv_qty"):
        df[col] = pd.to_numeric(df[col], errors="coerce").round().astype("Int64")
    for col in ("symbol", "series", "isin"):
        df[col] = df[col].astype("object").where(df[col].notna(), None)
    df = df.sort_values("date", kind="stable").drop_duplicates(subset="date", keep="last")
    return df.reset_index(drop=True)


# Rewriting thousands of security files (one day's appends, --redo-date) is per-file pandas
# overhead that threads cannot overlap, so large batches run in worker processes ("spawn",
# which is also what Windows uses). Each file is written by exactly one worker, results are
# consumed in key order. "spawn" re-imports the caller's __main__ in every worker, which only
# works for scripts guarded by `if __name__ == "__main__"`; so this is on for the sync.py
# command (allow_parallel_rewrites) and otherwise only when NSE_SYNC_WORKERS asks for it.
# NSE_SYNC_WORKERS=1 keeps everything in-process.
PARALLEL_MIN_FILES = 200
_PARALLEL_ALLOWED = False


def allow_parallel_rewrites() -> None:
    global _PARALLEL_ALLOWED
    _PARALLEL_ALLOWED = True


def parallel_workers() -> int:
    env = os.environ.get("NSE_SYNC_WORKERS", "").strip()
    if env:
        return max(1, int(env))
    if not _PARALLEL_ALLOWED:
        return 1
    return max(1, min(4, (os.cpu_count() or 2) - 1))


def _append_job(directory: str, key: str, rows: pd.DataFrame) -> str:
    Store(Path(directory)).append(key, rows)
    return key


def _remove_dates_job(directory: str, key: str, stamps: frozenset) -> bool:
    return Store(Path(directory)).remove_dates([key], stamps) == 1


def run_parallel(fn: Callable, jobs: Sequence[tuple], workers: Optional[int] = None) -> Iterator:
    """Yield fn(*job) for each job, in order, from worker processes. Falls back to running
    in-process when a pool cannot be started (restricted environments)."""
    workers = parallel_workers() if workers is None else workers
    try:
        pool = ProcessPoolExecutor(min(workers, len(jobs)), mp_context=multiprocessing.get_context("spawn"))
    except (OSError, NotImplementedError, ValueError, ImportError):
        pool = None
    if pool is None:
        for job in jobs:
            yield fn(*job)
        return
    with pool:
        yield from pool.map(fn, *zip(*jobs), chunksize=8)


class Store:
    """Per-security Parquet files under data/store/<key lower>.parquet."""

    def __init__(self, directory: Path = STORE_DIR):
        self.dir = directory
        self.dir.mkdir(parents=True, exist_ok=True)

    def path(self, key: str) -> Path:
        return self.dir / f"{key.lower()}.parquet"

    def exists(self, key: str) -> bool:
        return self.path(key).exists()

    def list_keys(self) -> List[str]:
        return sorted(p.stem.upper() for p in self.dir.glob("*.parquet"))

    def read(self, key: str, columns: Optional[List[str]] = None) -> pd.DataFrame:
        p = self.path(key)
        if not p.exists():
            return pd.DataFrame(columns=columns or STORE_COLUMNS)
        return pd.read_parquet(p, columns=columns, engine="pyarrow")

    def scan(
        self,
        keys: Sequence[str],
        columns: Sequence[str],
        start: Optional[pd.Timestamp] = None,
        end: Optional[pd.Timestamp] = None,
    ) -> Optional[Tuple[pd.DataFrame, np.ndarray]]:
        """Read `columns` of many securities in one multi-threaded pyarrow scan.

        Returns (frame, row_owner): exactly the rows and dtypes that reading each existing key
        with read(key, columns), keeping start <= date <= end and concatenating in `keys` order
        gives, plus for each row the position of its key in `keys`. Returns None whenever that
        equivalence cannot be guaranteed (string or unknown columns, duplicate keys, tz-aware
        or NaT bounds, files whose column types or pandas metadata differ, out-of-order
        batches); callers then read file by file."""
        if not columns or "date" not in columns or not set(columns) <= set(SCAN_COLUMNS):
            return None
        bounds = [b for b in (start, end) if b is not None]
        if any(pd.isna(b) or b.tzinfo is not None for b in bounds):
            return None
        paths = [self.path(k) for k in keys]
        if len(set(paths)) != len(paths):
            return None
        present = [i for i, p in enumerate(paths) if p.exists()]
        if not present:
            return pd.DataFrame(), np.zeros(0, dtype=np.int64)

        dset = pads.dataset([str(paths[i]) for i in present], format="parquet")
        frags = list(dset.get_fragments())
        if len(frags) != len(present) or any(Path(f.path) != paths[i] for f, i in zip(frags, present)):
            return None
        ref = _scan_signature(frags[0].physical_schema, columns)
        if ref is None or any(_scan_signature(f.physical_schema, columns) != ref for f in frags[1:]):
            return None

        flt = None
        if start is not None:
            flt = pads.field("date") >= pa.scalar(start.to_datetime64())
        if end is not None:
            e = pads.field("date") <= pa.scalar(end.to_datetime64())
            flt = e if flt is None else flt & e
        scanner = dset.scanner(columns=list(columns), filter=flt)
        pos = {f.path: i for f, i in zip(frags, present)}
        batches, owners, last = [], [], -1
        for tagged in scanner.scan_batches():
            rb = tagged.record_batch
            if not rb.num_rows:
                continue
            i = pos[tagged.fragment.path]
            if i < last:
                return None
            last = i
            batches.append(rb)
            owners.append(np.full(rb.num_rows, i, dtype=np.int64))
        frame = pa.Table.from_batches(batches, schema=scanner.projected_schema).to_pandas()
        return frame, (np.concatenate(owners) if owners else np.zeros(0, dtype=np.int64))

    def write(self, key: str, df: pd.DataFrame) -> None:
        _atomic_parquet(normalise_store_frame(df), self.path(key))

    def append(self, key: str, rows: pd.DataFrame) -> None:
        if self.exists(key):
            existing = self.read(key)
            rows = normalise_store_frame(rows)
            combined = pd.concat([existing, rows], ignore_index=True)
        else:
            combined = rows
        self.write(key, combined)

    def rename(self, old: str, new: str) -> None:
        src, dst = self.path(old), self.path(new)
        if not src.exists():
            return
        if dst.exists():
            merged = pd.concat([self.read(new), self.read(old)], ignore_index=True)
            self.write(new, merged)
            src.unlink()
        else:
            os.replace(src, dst)

    def remove_dates(self, keys: Iterable[str], dates: Iterable[date]) -> int:
        stamps = {pd.Timestamp(d) for d in dates}
        keys = list(keys)
        workers = parallel_workers()
        if workers > 1 and len(keys) >= PARALLEL_MIN_FILES:
            jobs = [(str(self.dir), k, frozenset(stamps)) for k in keys]
            return sum(run_parallel(_remove_dates_job, jobs, workers))
        touched = 0
        for key in keys:
            if not self.exists(key):
                continue
            df = self.read(key)
            mask = df["date"].isin(stamps)
            if mask.any():
                self.write(key, df[~mask])
                touched += 1
        return touched

    def last_dates(self) -> Dict[str, pd.Timestamp]:
        out = {}
        for key in self.list_keys():
            d = pd.read_parquet(self.path(key), columns=["date"])["date"]
            if len(d):
                out[key] = d.max()
        return out


# NSE renamed its indices in Nov 2015 (S&P CNX Nifty -> CNX Nifty -> Nifty 50).
# Files are kept under the historical names; reads merge them by canonical name.
_INDEX_SPECIAL = {
    "nifty": "nifty 50",
    "nifty junior": "nifty next 50",
    "midcap": "nifty midcap 100",
    "smallcap": "nifty smallcap 100",
    "finance": "nifty financial services",
    "service sector": "nifty services sector",
    "alpha index": "nifty alpha 50",
    "high beta": "nifty high beta 50",
    "low volatility": "nifty low volatility 50",
    "dividend opportunities": "nifty dividend opportunities 50",
    "nifty dividend": "nifty dividend opportunities 50",
    "defty": "nifty 50 usd",
    "100 equal weight": "nifty100 equal weight",
}


def canonical_index_name(name: str) -> str:
    n = re.sub(r"\s+", " ", name.strip()).lower()
    for prefix in ("s&p cnx ", "cnx "):
        if n.startswith(prefix):
            rest = n[len(prefix):]
            return _INDEX_SPECIAL.get(rest, f"nifty {rest}")
    return n


class IndexStore:
    """Per-index Parquet files under data/indices/<slug>.parquet."""

    COLUMNS = ["date", "index", "open", "high", "low", "close", "volume", "turnover_cr", "pe", "pb", "div_yield"]

    def __init__(self, directory: Path = INDEX_DIR):
        self.dir = directory
        self.dir.mkdir(parents=True, exist_ok=True)
        self._names: Optional[Dict[str, str]] = None  # slug -> stored index name
        self._last_dates: Optional[Dict[str, str]] = None  # slug -> str(last date), "" if unreadable

    @staticmethod
    def slug(name: str) -> str:
        return _INDEX_NAME_RE.sub("_", name.strip().lower()).strip("_")

    def path(self, name: str) -> Path:
        return self.dir / f"{self.slug(name)}.parquet"

    def _stored_names(self, with_dates: bool = False) -> Dict[str, str]:
        if self._names is None:
            # with_dates (list_names): one read per file for the stored name and the last date
            self._names, self._last_dates = {}, ({} if with_dates else None)
            for p in sorted(self.dir.glob("*.parquet")):
                both = self._read_name_and_last_date(p) if with_dates else None
                if both is not None:
                    self._names[p.stem], self._last_dates[p.stem] = both
                    continue
                try:  # name only (or the two columns judged separately, as before)
                    nm = pd.read_parquet(p, columns=["index"])["index"]
                    self._names[p.stem] = nm.iloc[-1] if len(nm) else p.stem
                except Exception:
                    self._names[p.stem] = p.stem
        return self._names

    @staticmethod
    def _read_name_and_last_date(p: Path) -> Optional[Tuple[str, str]]:
        try:
            df = pd.read_parquet(p, columns=["index", "date"])
            return (df["index"].iloc[-1] if len(df) else p.stem), str(df["date"].max())
        except Exception:
            return None

    def _last_date(self, slug: str) -> str:
        if self._last_dates is not None and slug in self._last_dates:
            return self._last_dates[slug]
        try:
            return str(pd.read_parquet(self.dir / f"{slug}.parquet", columns=["date"])["date"].max())
        except Exception:
            return ""

    def list_names(self, canonical: bool = True) -> List[str]:
        names = list(self._stored_names(with_dates=canonical).values())
        if not canonical:
            return names
        # latest stored name per canonical group (Nifty 50 rather than CNX Nifty)
        best: Dict[str, Tuple[str, str]] = {}
        for slug, nm in self._stored_names(with_dates=True).items():
            key = canonical_index_name(nm)
            last = self._last_date(slug)
            if key not in best or last > best[key][0]:
                best[key] = (last, nm)
        return sorted(v[1] for v in best.values())

    def read(self, name: str) -> pd.DataFrame:
        key = canonical_index_name(name)
        frames = []
        for slug, nm in self._stored_names().items():
            if canonical_index_name(nm) == key:
                frames.append(pd.read_parquet(self.dir / f"{slug}.parquet", engine="pyarrow"))
        if not frames:
            p = self.path(name)
            if not p.exists():
                return pd.DataFrame(columns=self.COLUMNS)
            return pd.read_parquet(p, engine="pyarrow")
        df = pd.concat(frames, ignore_index=True)
        df = df.sort_values(["date", "index"], kind="stable").drop_duplicates(subset="date", keep="last")
        return df.reset_index(drop=True)

    def write(self, name: str, df: pd.DataFrame) -> None:
        df = df.loc[:, self.COLUMNS].copy()
        df["date"] = pd.to_datetime(df["date"]).astype("datetime64[ns]")
        df = df.sort_values("date", kind="stable").drop_duplicates(subset="date", keep="last").reset_index(drop=True)
        _atomic_parquet(df, self.path(name))
        self._last_dates = None

    def append(self, name: str, rows: pd.DataFrame) -> None:
        if self.path(name).exists():
            rows = pd.concat([self.read(name), rows], ignore_index=True)
        self.write(name, rows)

    def append_day(self, day: pd.DataFrame) -> None:
        for name, grp in day.groupby("index", sort=False):
            self.append(name, grp)
        self._names = None
        self._last_dates = None

    def remove_dates(self, dates: Iterable[date]) -> None:
        stamps = {pd.Timestamp(d) for d in dates}
        self._last_dates = None
        for p in self.dir.glob("*.parquet"):
            df = pd.read_parquet(p)
            mask = df["date"].isin(stamps)
            if mask.any():
                _atomic_parquet(df[~mask].reset_index(drop=True), p)


# --------------------------------------------------------------------------- #
# meta.json
# --------------------------------------------------------------------------- #

DEFAULT_META = {
    "schema_version": 1,
    "last_synced": None,  # last trade date fully ingested (ISO)
    "last_actions_sync": None,  # last date corporate actions were fetched through
    "actions_fetched_through": None,
    "breadth_last_date": None,
    "adjustments_applied": False,
    "adjustments_applied_at": None,
    "bootstrap": {},  # step -> completed flag
    "holidays": {},  # {'DD-Mon-YYYY': description}
    "holiday_year": None,
    "special_sessions": [],  # ISO dates of Saturday/Muhurat sessions
    "in_progress": None,  # {'date': ISO, 'keys': [...]} while a day is being written
    "pending_delivery": [],  # dates whose delivery report was unavailable
    "symbol_count": 0,
}


def load_meta(path: Path = META_FILE) -> dict:
    """Read meta.json. A corrupt file raises instead of silently resetting the
    sync state (which would make sync believe the database was never built).
    Restore from meta.json.bak (written before every save) if that happens."""
    ensure_dirs()
    meta = copy.deepcopy(DEFAULT_META)  # the defaults hold dicts/lists that callers mutate
    if path.exists():
        try:
            meta.update(json.loads(path.read_text(encoding="utf-8")))
        except json.JSONDecodeError as e:
            raise RuntimeError(
                f"{path} is not valid JSON ({e}). Restore it from {path.with_suffix('.json.bak')} "
                "or fix it by hand; do not delete it."
            ) from e
    return meta


def save_meta(meta: dict, path: Path = META_FILE) -> None:
    ensure_dirs()
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(meta, indent=1, cls=_JsonEncoder), encoding="utf-8")
    if path.exists():
        # copy, not move: meta.json must exist at every instant (bootstrap treats a missing
        # meta.json as a fresh clone and would re-extract the snapshot over the store)
        bak = path.with_suffix(".json.bak")
        try:
            shutil.copy2(path, bak)
        except OSError:
            pass
    os.replace(tmp, path)
