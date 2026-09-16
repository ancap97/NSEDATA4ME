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

import json
import os
import re
from datetime import date, datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import pandas as pd

from config import INDEX_DIR, META_FILE, STORE_COLUMNS, STORE_DIR, ensure_dirs

_INDEX_NAME_RE = re.compile(r"[^a-z0-9]+")


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

    @staticmethod
    def slug(name: str) -> str:
        return _INDEX_NAME_RE.sub("_", name.strip().lower()).strip("_")

    def path(self, name: str) -> Path:
        return self.dir / f"{self.slug(name)}.parquet"

    def _stored_names(self) -> Dict[str, str]:
        if self._names is None:
            self._names = {}
            for p in sorted(self.dir.glob("*.parquet")):
                try:
                    nm = pd.read_parquet(p, columns=["index"])["index"]
                    self._names[p.stem] = nm.iloc[-1] if len(nm) else p.stem
                except Exception:
                    self._names[p.stem] = p.stem
        return self._names

    def list_names(self, canonical: bool = True) -> List[str]:
        names = list(self._stored_names().values())
        if not canonical:
            return names
        # latest stored name per canonical group (Nifty 50 rather than CNX Nifty)
        best: Dict[str, Tuple[str, str]] = {}
        for slug, nm in self._stored_names().items():
            key = canonical_index_name(nm)
            try:
                last = str(pd.read_parquet(self.dir / f"{slug}.parquet", columns=["date"])["date"].max())
            except Exception:
                last = ""
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

    def append(self, name: str, rows: pd.DataFrame) -> None:
        if self.path(name).exists():
            rows = pd.concat([self.read(name), rows], ignore_index=True)
        self.write(name, rows)

    def append_day(self, day: pd.DataFrame) -> None:
        for name, grp in day.groupby("index", sort=False):
            self.append(name, grp)
        self._names = None

    def remove_dates(self, dates: Iterable[date]) -> None:
        stamps = {pd.Timestamp(d) for d in dates}
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
    meta = dict(DEFAULT_META)
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
        bak = path.with_suffix(".json.bak")
        try:
            os.replace(path, bak)
        except OSError:
            pass
    os.replace(tmp, path)
