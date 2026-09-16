"""Public read API for backtests and notebooks.

    from loader import load_data, load_breadth, load_index, load_actions

    df = load_data(["RELIANCE", "TCS"], start_date="2010-01-01")       # adjusted
    df = load_data(adjusted=False)                                     # raw NSE prices, all symbols
    df = load_data(include_delisted=True)                              # survivorship-bias-free universe

Returned columns (long format, one row per symbol-date):
    date, symbol, key, isin, series, open, high, low, close, prev_close,
    volume, turnover, trades, deliv_qty, deliv_pct, adj_factor
When adjusted=True prices are divided by adj_factor and share quantities
(volume, deliv_qty) multiplied by it; `symbol` is the *current* symbol of the
security so a renamed company has a single continuous history.
"""

from __future__ import annotations

from datetime import date
from typing import Iterable, List, Optional, Union

import pandas as pd

from adjuster import load_actions as _load_actions
from breadth import load_breadth as _load_breadth
from storage import IndexStore, Store
from symbol_master import EntityResolver, load_master

DateLike = Union[str, date, pd.Timestamp, None]

PRICE_COLS = ("open", "high", "low", "close", "prev_close")
QTY_COLS = ("volume", "deliv_qty")


def _ts(d: DateLike) -> Optional[pd.Timestamp]:
    return None if d is None else pd.Timestamp(d)


def list_symbols(include_delisted: bool = True, include_sme: bool = False) -> pd.DataFrame:
    m = load_master()
    if m.empty:
        return m
    if not include_sme:
        m = m[~m["is_sme"]]
    if not include_delisted:
        m = m[m["status"] == "active"]
    return m.reset_index(drop=True)


def resolve_symbols(symbols: Iterable[str], include_sme: bool = False) -> List[str]:
    """Map current or historical symbols to store keys."""
    resolver = EntityResolver.load()
    keys = []
    for s in symbols:
        s = s.upper().strip()
        k = resolver.key_for_symbol(s, sme=False)
        if k is None and include_sme:
            k = resolver.key_for_symbol(s, sme=True)
        if k is None:
            raise KeyError(f"unknown symbol: {s}")
        keys.append(k)
    return keys


def load_data(
    symbols: Optional[Iterable[str]] = None,
    start_date: DateLike = "2005-01-01",
    end_date: DateLike = None,
    include_delisted: bool = True,
    include_sme: bool = False,
    adjusted: bool = True,
    columns: Optional[List[str]] = None,
) -> pd.DataFrame:
    store = Store()
    master = load_master()

    if symbols is not None:
        keys = resolve_symbols(symbols, include_sme=True)
    else:
        if master.empty:
            keys = store.list_keys()
        else:
            m = master
            if not include_sme:
                m = m[~m["is_sme"]]
            if not include_delisted:
                m = m[m["status"] == "active"]
            keys = m["key"].tolist()

    start, end = _ts(start_date), _ts(end_date)
    current_symbol = dict(zip(master["key"], master["symbol"])) if not master.empty else {}
    frames = []
    for key in keys:
        df = store.read(key)
        if df.empty:
            continue
        if start is not None:
            df = df[df["date"] >= start]
        if end is not None:
            df = df[df["date"] <= end]
        if df.empty:
            continue
        df = df.copy()
        df["key"] = key
        df["symbol"] = current_symbol.get(key, key)
        frames.append(df)

    if not frames:
        return pd.DataFrame()
    out = pd.concat(frames, ignore_index=True)
    if adjusted:
        af = out["adj_factor"]
        for c in PRICE_COLS:
            out[c] = out[c] / af
        for c in QTY_COLS:
            out[c] = (out[c].astype("Float64") * af).round().astype("Int64")
    out = out.sort_values(["symbol", "date"]).reset_index(drop=True)
    cols = ["date", "symbol", "key", "isin", "series", *PRICE_COLS, "volume", "turnover", "trades", "deliv_qty", "deliv_pct", "adj_factor"]
    out = out.loc[:, cols]
    if columns:
        out = out.loc[:, [c for c in columns if c in out.columns]]
    return out


def load_symbol(symbol: str, adjusted: bool = True, **kw) -> pd.DataFrame:
    """Single symbol, indexed by date."""
    df = load_data([symbol], adjusted=adjusted, **kw)
    return df.set_index("date") if not df.empty else df


def load_wide(field: str = "close", adjusted: bool = True, **kw) -> pd.DataFrame:
    """Date x symbol matrix of one field.

    Columns are current symbols; if two securities ever share a current symbol
    (NSE reuses symbols of delisted companies) those columns use the store key
    instead so the pivot never fails."""
    df = load_data(adjusted=adjusted, columns=["date", "symbol", "key", field], **kw)
    if df.empty:
        return df
    sym_keys = df.groupby("symbol")["key"].nunique()
    clash = set(sym_keys[sym_keys > 1].index)
    col = df["symbol"].where(~df["symbol"].isin(clash), df["key"])
    return df.assign(_col=col).pivot(index="date", columns="_col", values=field).rename_axis(columns="symbol")


def load_breadth(start_date: DateLike = None, end_date: DateLike = None) -> pd.DataFrame:
    df = _load_breadth()
    if df.empty:
        return df
    if start_date is not None:
        df = df[df["date"] >= _ts(start_date)]
    if end_date is not None:
        df = df[df["date"] <= _ts(end_date)]
    return df.reset_index(drop=True)


def load_index(name: str = "Nifty 50", start_date: DateLike = None, end_date: DateLike = None) -> pd.DataFrame:
    df = IndexStore().read(name)
    if df.empty:
        return df
    if start_date is not None:
        df = df[df["date"] >= _ts(start_date)]
    if end_date is not None:
        df = df[df["date"] <= _ts(end_date)]
    return df.reset_index(drop=True)


def list_indices() -> List[str]:
    return IndexStore().list_names()


def load_actions(symbol: Optional[str] = None, adjusting_only: bool = False) -> pd.DataFrame:
    df = _load_actions()
    if symbol is not None:
        key = resolve_symbols([symbol], include_sme=True)[0]
        df = df[df["key"] == key]
    if adjusting_only:
        df = df[df["type"].isin(("SPLIT", "BONUS", "CONSOLIDATION"))]
    return df.sort_values("ex_date").reset_index(drop=True)
