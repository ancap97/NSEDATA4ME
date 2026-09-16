"""Market breadth over the full (survivorship-bias-free) NSE main-board universe.

Recomputed from scratch on every sync from adjusted closes, so it is always
consistent with the price store (no incremental EMA state to corrupt).

Per trading day (universe = main-board stocks that traded that day):
  advances / declines / unchanged, adv_dec_ratio
  ad_line             cumulative sum of (adv - dec) / total       (eod2 definition)
  pct_above_50dma / pct_above_200dma
  new_highs / new_lows  vs. trailing 252-day high/low (excluding today)
  net_new_highs_cum   cumulative (new_highs - new_lows)
  net_adv_ratio       (adv - dec) / total * 100
  mcclellan_osc       EMA19 - EMA39 of net_adv_ratio (ratio-adjusted McClellan)
"""

from __future__ import annotations

import logging
from typing import Callable, List, Optional

import numpy as np
import pandas as pd

from config import BREADTH_FILE
from storage import Store
from symbol_master import load_master

logger = logging.getLogger("breadth")

BREADTH_COLUMNS = [
    "date",
    "universe",
    "advances",
    "declines",
    "unchanged",
    "adv_dec_ratio",
    "net_adv_ratio",
    "ad_line",
    "pct_above_50dma",
    "pct_above_200dma",
    "new_highs",
    "new_lows",
    "net_new_highs",
    "net_new_highs_cum",
    "mcclellan_osc",
]


def _load_wide(store: Store, keys: List[str], progress: Optional[Callable] = None):
    highs, lows, closes = {}, {}, {}
    n = len(keys)
    for i, key in enumerate(keys, 1):
        df = store.read(key, columns=["date", "high", "low", "close", "adj_factor"])
        if df.empty:
            continue
        df = df.drop_duplicates("date").set_index("date")
        af = df["adj_factor"].to_numpy()
        highs[key] = (df["high"] / af).astype("float32")
        lows[key] = (df["low"] / af).astype("float32")
        closes[key] = (df["close"] / af).astype("float32")
        if progress and (i % 500 == 0 or i == n):
            progress(i, n, key)
    high = pd.DataFrame(highs).sort_index()
    low = pd.DataFrame(lows).sort_index()
    close = pd.DataFrame(closes).sort_index()
    return high, low, close


def compute_breadth(store: Store, progress: Optional[Callable] = None) -> pd.DataFrame:
    master = load_master()
    if master.empty:
        keys = [k for k in store.list_keys() if not k.endswith("_SME")]
    else:
        keys = master.loc[~master["is_sme"], "key"].tolist()

    high, low, close = _load_wide(store, keys, progress)
    if close.empty:
        return pd.DataFrame(columns=BREADTH_COLUMNS)

    traded = close.notna()
    close_ff = close.ffill()
    prev = close_ff.shift(1)

    adv = ((close > prev) & traded).sum(axis=1)
    dec = ((close < prev) & traded).sum(axis=1)
    counted = (traded & prev.notna()).sum(axis=1)
    unch = counted - adv - dec

    ma50 = close_ff.rolling(50, min_periods=50).mean()
    ok50 = traded & ma50.notna()
    pct50 = ((close > ma50) & ok50).sum(axis=1) / ok50.sum(axis=1).replace(0, np.nan) * 100
    del ma50, ok50

    ma200 = close_ff.rolling(200, min_periods=200).mean()
    ok200 = traded & ma200.notna()
    pct200 = ((close > ma200) & ok200).sum(axis=1) / ok200.sum(axis=1).replace(0, np.nan) * 100
    del ma200, ok200, close_ff

    hi252 = high.rolling(252, min_periods=200).max().shift(1)
    new_high = ((high > hi252) & traded).sum(axis=1)
    del hi252, high
    lo252 = low.rolling(252, min_periods=200).min().shift(1)
    new_low = ((low < lo252) & traded).sum(axis=1)
    del lo252, low

    total = counted.replace(0, np.nan)
    net_adv_ratio = (adv - dec) / total * 100
    ad_line = ((adv - dec) / total).fillna(0).cumsum()
    fast = net_adv_ratio.ewm(span=19, adjust=False, min_periods=19).mean()
    slow = net_adv_ratio.ewm(span=39, adjust=False, min_periods=39).mean()

    out = pd.DataFrame(
        {
            "date": close.index,
            "universe": traded.sum(axis=1).to_numpy(),
            "advances": adv.to_numpy(),
            "declines": dec.to_numpy(),
            "unchanged": unch.to_numpy(),
            "adv_dec_ratio": (adv / dec.replace(0, np.nan)).round(4).to_numpy(),
            "net_adv_ratio": net_adv_ratio.round(4).to_numpy(),
            "ad_line": ad_line.round(6).to_numpy(),
            "pct_above_50dma": pct50.round(2).to_numpy(),
            "pct_above_200dma": pct200.round(2).to_numpy(),
            "new_highs": new_high.to_numpy(),
            "new_lows": new_low.to_numpy(),
            "net_new_highs": (new_high - new_low).to_numpy(),
            "net_new_highs_cum": (new_high - new_low).cumsum().to_numpy(),
            "mcclellan_osc": (fast - slow).round(4).to_numpy(),
        }
    )
    out = out[out["universe"] > 0].reset_index(drop=True)
    return out


def save_breadth(df: pd.DataFrame) -> None:
    BREADTH_FILE.parent.mkdir(parents=True, exist_ok=True)
    tmp = BREADTH_FILE.with_suffix(".tmp")
    df.to_parquet(tmp, index=False)
    tmp.replace(BREADTH_FILE)


def load_breadth() -> pd.DataFrame:
    if not BREADTH_FILE.exists():
        return pd.DataFrame(columns=BREADTH_COLUMNS)
    return pd.read_parquet(BREADTH_FILE)


def rebuild_breadth(store: Optional[Store] = None, progress: Optional[Callable] = None) -> pd.DataFrame:
    store = store or Store()
    df = compute_breadth(store, progress)
    save_breadth(df)
    logger.info("breadth rebuilt: %d days, last %s", len(df), df["date"].max() if len(df) else None)
    return df


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    rebuild_breadth()
