"""Security identity: maps daily (symbol, series, ISIN) rows to stable entities.

Identity rules (mirror eod2's isin.csv + SymbolTracker logic):
  * An entity's storage key is its *current* trading symbol (SME issues get a
    `_SME` suffix). When NSE renames a symbol the entity keeps its history and
    the key/file is renamed.
  * A rename is detected when an already-known ISIN appears under a new
    symbol. A face-value change re-issues the ISIN under the same symbol, so
    a new ISIN on a known symbol is attached to the existing entity.
  * Pre-2011-06-22 bhavcopies have no ISIN; rows are keyed by symbol and are
    joined to the ISIN era by symbol continuity.
  * An SME issue migrating to the main board (SM/ST -> EQ) keeps its history
    (`X_SME` -> `X`).

The full symbol/ISIN history per entity is persisted in
data/isin_symbol_map.json (same shape as eod2's file) and the flat master
table in data/symbol_master.parquet.

Derived from BennyThadikaran/eod2 (GPL-3).
"""

from __future__ import annotations

import json
import logging
from dataclasses import asdict, dataclass, field
from datetime import date
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd

from config import DATA_DIR, SME_SERIES, SYMBOL_HISTORY_FILE, SYMBOL_MASTER_FILE

logger = logging.getLogger("symbol_master")

STATE_FILE = DATA_DIR / "symbol_state.json"
IDENTITY_REVIEW_FILE = DATA_DIR / "logs" / "identity_review.csv"
IDENTITY_REVIEW_GAP_DAYS = 365


def flag_identity(d: str, key: str, message: str) -> None:
    """Append an identity decision that deserves a human look to logs/identity_review.csv."""
    logger.warning("IDENTITY REVIEW %s %s: %s", d, key, message)
    try:
        IDENTITY_REVIEW_FILE.parent.mkdir(parents=True, exist_ok=True)
        new = not IDENTITY_REVIEW_FILE.exists()
        with IDENTITY_REVIEW_FILE.open("a", encoding="utf-8") as f:
            if new:
                f.write("date,key,message\n")
            f.write(f"{d},{key},\"{message}\"\n")
    except OSError as e:  # never let bookkeeping break ingestion
        logger.error("could not write %s: %s", IDENTITY_REVIEW_FILE, e)


@dataclass
class Period:
    value: str
    from_date: str
    to_date: str


@dataclass
class Entity:
    key: str
    symbol: str
    series: str
    isin: Optional[str]
    first_date: str
    last_date: str
    symbols: List[Period] = field(default_factory=list)
    isins: List[Period] = field(default_factory=list)
    notes: List[str] = field(default_factory=list)

    @property
    def is_sme(self) -> bool:
        return self.key.endswith("_SME")


def make_key(symbol: str, series: str) -> str:
    return f"{symbol}_SME" if series in SME_SERIES else symbol


class EntityResolver:
    """Stateful resolver, fed with daily bhavcopy rows in chronological order."""

    def __init__(self) -> None:
        self.entities: Dict[str, Entity] = {}
        self.isin2key: Dict[str, str] = {}
        self.pair2key: Dict[str, str] = {}

    # -- persistence ---------------------------------------------------------
    def save(self, path: Path = STATE_FILE) -> None:
        payload = {
            "entities": {k: asdict(e) for k, e in self.entities.items()},
            "isin2key": self.isin2key,
            "pair2key": self.pair2key,
        }
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(".tmp")
        tmp.write_text(json.dumps(payload))
        tmp.replace(path)

    @classmethod
    def load(cls, path: Path = STATE_FILE) -> "EntityResolver":
        r = cls()
        if not path.exists():
            return r
        payload = json.loads(path.read_text())
        for k, e in payload["entities"].items():
            r.entities[k] = Entity(
                key=e["key"],
                symbol=e["symbol"],
                series=e["series"],
                isin=e.get("isin"),
                first_date=e["first_date"],
                last_date=e["last_date"],
                symbols=[Period(**p) for p in e.get("symbols", [])],
                isins=[Period(**p) for p in e.get("isins", [])],
                notes=list(e.get("notes", [])),
            )
        r.isin2key = dict(payload["isin2key"])
        r.pair2key = dict(payload["pair2key"])
        return r

    # -- core ----------------------------------------------------------------
    @staticmethod
    def _pair(symbol: str, series: str, isin: Optional[str]) -> str:
        sme = "S" if series in SME_SERIES else "M"
        return f"{symbol}|{sme}|{isin or ''}"

    def _new_entity(self, key: str, symbol: str, series: str, isin: Optional[str], d: str) -> Entity:
        e = Entity(key=key, symbol=symbol, series=series, isin=isin, first_date=d, last_date=d)
        e.symbols.append(Period(symbol, d, d))
        if isin:
            e.isins.append(Period(isin, d, d))
            self.isin2key[isin] = key
        self.entities[key] = e
        return e

    def _rename(self, old_key: str, new_key: str, new_symbol: str, series: str, d: str, reason: str) -> str:
        """Rename entity old_key -> new_key. Returns the key actually used."""
        if new_key in self.entities and new_key != old_key:
            other = self.entities[new_key]
            logger.warning(
                "symbol collision on %s: %s wants key %s already used by entity "
                "(isin %s, last %s); keeping separate under %s",
                d, old_key, new_key, other.isin, other.last_date, old_key,
            )
            self.entities[old_key].notes.append(f"{d}: NSE symbol now {new_symbol} but key {new_key} taken")
            flag_identity(
                d, old_key,
                f"NSE now trades this ISIN as {new_symbol} but key {new_key} belongs to another entity "
                f"(isin {other.isin}, last traded {other.last_date}); history kept under {old_key}",
            )
            return old_key

        e = self.entities.pop(old_key)
        e.key = new_key
        e.symbol = new_symbol
        e.series = series
        if e.symbols and e.symbols[-1].value != new_symbol:
            e.symbols.append(Period(new_symbol, d, d))
        e.notes.append(f"{d}: {reason} {old_key} -> {new_key}")
        self.entities[new_key] = e
        for isin, k in list(self.isin2key.items()):
            if k == old_key:
                self.isin2key[isin] = new_key
        for p, k in list(self.pair2key.items()):
            if k == old_key:
                self.pair2key[p] = new_key
        logger.info("rename %s -> %s (%s)", old_key, new_key, reason)
        return new_key

    def resolve_day(self, day: pd.DataFrame, trade_date: date) -> Tuple[pd.Series, List[Tuple[str, str]]]:
        """Return (keys aligned with `day`, list of (old_key, new_key) renames)."""
        d = trade_date.isoformat()
        renames: List[Tuple[str, str]] = []

        pairs = day["symbol"] + "|" + day["series"].isin(SME_SERIES).map({True: "S", False: "M"}) + "|" + day["isin"].fillna("").astype(str)
        keys = pairs.map(self.pair2key).astype("object")

        unknown = keys.isna()
        if unknown.any():
            for idx, symbol, series, isin, pair in zip(
                day.index[unknown],
                day.loc[unknown, "symbol"],
                day.loc[unknown, "series"],
                day.loc[unknown, "isin"],
                pairs[unknown],
            ):
                isin = isin if isinstance(isin, str) and isin else None
                base_key = make_key(symbol, series)

                if isin and isin in self.isin2key:
                    key = self.isin2key[isin]
                    e = self.entities[key]
                    if key != base_key:
                        reason = "sme->main" if e.is_sme and base_key == symbol else "symbol change"
                        old = key
                        key = self._rename(key, base_key, symbol, series, d, reason)
                        if key != old:
                            renames.append((old, key))
                elif base_key in self.entities:
                    e = self.entities[base_key]
                    key = base_key
                    if isin and e.isin != isin:
                        if e.isin:
                            e.notes.append(f"{d}: ISIN {e.isin} -> {isin}")
                            gap = (trade_date - date.fromisoformat(e.last_date)).days
                            if gap > IDENTITY_REVIEW_GAP_DAYS:
                                # a new ISIN under a symbol that has not traded for a long time is
                                # usually a relisting of the same company, but NSE does reuse symbols.
                                # Keep one history (the common case) and flag it for a human check.
                                flag_identity(
                                    d, key,
                                    f"new ISIN {isin} after {gap} days without trades (old ISIN {e.isin}); "
                                    "possible symbol reuse by a different company - verify and split the "
                                    "history if so",
                                )
                        e.isin = isin
                        e.isins.append(Period(isin, d, d))
                        self.isin2key[isin] = key
                else:
                    key = base_key
                    self._new_entity(key, symbol, series, isin, d)

                self.pair2key[pair] = key
                keys[idx] = key

        # update last_date / series for all entities seen today
        for key, series in zip(keys, day["series"]):
            e = self.entities[key]
            e.last_date = d
            e.series = series
            if e.symbols:
                e.symbols[-1].to_date = d
            if e.isins:
                e.isins[-1].to_date = d
        return keys, renames

    # -- outputs -------------------------------------------------------------
    def master_frame(self, as_of: Optional[date] = None, inactive_after_days: int = 20) -> pd.DataFrame:
        rows = []
        latest = as_of.isoformat() if as_of else max((e.last_date for e in self.entities.values()), default="")
        cutoff = (pd.Timestamp(latest) - pd.Timedelta(days=inactive_after_days)) if latest else None
        for e in self.entities.values():
            rows.append(
                {
                    "key": e.key,
                    "symbol": e.symbol,
                    "series": e.series,
                    "isin": e.isin,
                    "is_sme": e.is_sme,
                    "first_date": e.first_date,
                    "last_date": e.last_date,
                    "status": "active" if cutoff is not None and pd.Timestamp(e.last_date) >= cutoff else "inactive",
                    "previous_symbols": ",".join(p.value for p in e.symbols[:-1]),
                    "previous_isins": ",".join(p.value for p in e.isins[:-1]),
                    "notes": " | ".join(e.notes),
                }
            )
        df = pd.DataFrame(rows)
        if df.empty:
            return df
        df["first_date"] = pd.to_datetime(df["first_date"])
        df["last_date"] = pd.to_datetime(df["last_date"])
        return df.sort_values("key").reset_index(drop=True)

    def history_json(self) -> str:
        sym2isin = {}
        isin2hist: Dict[str, list] = {}
        for e in self.entities.values():
            for p in e.symbols:
                if e.isin:
                    sym2isin[p.value] = e.isin
            isin_key = e.isin or f"NOISIN:{e.key}"
            isin2hist[isin_key] = [
                {"symbol": p.value, "from_date": p.from_date, "to_date": p.to_date, "action": None} for p in e.symbols
            ]
        return json.dumps({"sym2isin": sym2isin, "isin2hist": isin2hist}, indent=1)

    def save_outputs(self, as_of: Optional[date] = None) -> pd.DataFrame:
        master = self.master_frame(as_of)
        SYMBOL_MASTER_FILE.parent.mkdir(parents=True, exist_ok=True)
        master.to_parquet(SYMBOL_MASTER_FILE, index=False)
        SYMBOL_HISTORY_FILE.write_text(self.history_json())
        self.save()
        return master

    # -- lookups -------------------------------------------------------------
    def key_for_symbol(self, symbol: str, on: Optional[date] = None, sme: bool = False) -> Optional[str]:
        """Resolve any historical or current symbol to an entity key."""
        symbol = symbol.upper().strip()
        direct = make_key(symbol, "SM" if sme else "EQ")
        if direct in self.entities and on is None:
            return direct
        # (prefer matching board, then latest period) - an SME issue that later
        # migrated to the main board is one entity whose key lost the _SME suffix
        candidates = []
        for e in self.entities.values():
            for p in e.symbols:
                if p.value == symbol:
                    if on is None or (p.from_date <= on.isoformat() <= p.to_date):
                        candidates.append((e.is_sme == sme, p.to_date, e.key))
        if not candidates:
            for k in (direct, make_key(symbol, "EQ" if sme else "SM")):
                if k in self.entities:
                    return k
            return None
        candidates.sort()
        return candidates[-1][2]

    def key_for_isin(self, isin: str) -> Optional[str]:
        return self.isin2key.get(isin)


def load_master() -> pd.DataFrame:
    if not SYMBOL_MASTER_FILE.exists():
        return pd.DataFrame()
    return pd.read_parquet(SYMBOL_MASTER_FILE)
