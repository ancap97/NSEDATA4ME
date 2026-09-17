"""Parsers that normalise every NSE report format into one schema.

Every equity parser returns a DataFrame with columns:
    date, symbol, series, isin, open, high, low, close, prev_close,
    volume, turnover, trades, deliv_qty, deliv_pct
`isin`, `trades`, `deliv_qty`, `deliv_pct` may be NaN when the source report
does not carry them (pre-2011 bhavcopies, missing delivery report).

Formats handled
  * old equity bhavcopy   cm{DD}{MON}{YYYY}bhav.csv            (1994 .. 2024-07-05)
  * UDiFF bhavcopy        BhavCopy_NSE_CM_0_0_0_{YYYYMMDD}_F_0000.csv (2024-07-08 ..)
  * delivery (full)       sec_bhavdata_full_{ddmmyyyy}.csv     (2019 ..)
  * delivery (MTO)        MTO_{ddmmyyyy}.DAT                    (2003 ..)
  * indices               ind_close_all_{ddmmyyyy}.csv
  * PR bhavcopy zip       PR{ddmmyy}.zip -> bc{ddmmyyyy}.csv    corporate actions
"""

from __future__ import annotations

import csv
import re
import zipfile
from datetime import date, datetime
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

from config import SERIES_PRIORITY, VALID_SERIES

RIGHTS_ENTITLEMENT_RE = re.compile(r"-RE\d*$")

EQUITY_COLUMNS = [
    "date",
    "symbol",
    "series",
    "isin",
    "open",
    "high",
    "low",
    "close",
    "prev_close",
    "volume",
    "turnover",
    "trades",
    "deliv_qty",
    "deliv_pct",
]


def _clean_columns(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [str(c).strip() for c in df.columns]
    return df.loc[:, [c for c in df.columns if c and not c.startswith("Unnamed")]]


def _finalise(df: pd.DataFrame) -> pd.DataFrame:
    """Filter to equity series, drop rights entitlements, dedupe by
    (symbol) keeping the highest priority series, enforce dtypes."""
    df = df[df["series"].isin(VALID_SERIES)]
    df = df[~df["symbol"].str.contains(RIGHTS_ENTITLEMENT_RE, na=False)]

    df = df.assign(_rank=df["series"].map(SERIES_PRIORITY))
    df = df.sort_values(["symbol", "_rank"], kind="stable")
    df = df.drop_duplicates(subset=["symbol"], keep="first").drop(columns="_rank")

    for col in ("open", "high", "low", "close", "prev_close", "turnover", "deliv_pct"):
        if col not in df:
            df[col] = np.nan
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("float64")

    for col in ("volume", "trades", "deliv_qty"):
        if col not in df:
            df[col] = np.nan
        df[col] = pd.to_numeric(df[col], errors="coerce").round().astype("Int64")

    if "isin" not in df:
        df["isin"] = None
    df["isin"] = df["isin"].where(df["isin"].notna() & (df["isin"].astype(str).str.strip() != ""), None)
    df["isin"] = df["isin"].astype("object")
    df["symbol"] = df["symbol"].astype(str).str.strip()
    df["series"] = df["series"].astype(str).str.strip()

    df = df[df["close"].notna() & (df["close"] > 0)]
    return df.loc[:, EQUITY_COLUMNS].reset_index(drop=True)


# --------------------------------------------------------------------------- #
# Equity bhavcopy
# --------------------------------------------------------------------------- #


def parse_bhav_old(path: Path, trade_date: date) -> pd.DataFrame:
    df = _clean_columns(pd.read_csv(path, dtype=str))
    df["SYMBOL"] = df["SYMBOL"].str.strip()
    df["SERIES"] = df["SERIES"].str.strip()

    out = pd.DataFrame(
        {
            "date": pd.Timestamp(trade_date),
            "symbol": df["SYMBOL"],
            "series": df["SERIES"],
            "isin": df["ISIN"].str.strip() if "ISIN" in df else None,
            "open": df["OPEN"],
            "high": df["HIGH"],
            "low": df["LOW"],
            "close": df["CLOSE"],
            "prev_close": df["PREVCLOSE"],
            "volume": df["TOTTRDQTY"],
            "turnover": df["TOTTRDVAL"],
            "trades": df["TOTALTRADES"] if "TOTALTRADES" in df else np.nan,
        }
    )
    return _finalise(out)


def parse_bhav_udiff(path: Path, trade_date: date) -> pd.DataFrame:
    df = _clean_columns(pd.read_csv(path, dtype=str))
    df = df[df["Sgmt"].str.strip() == "CM"]

    out = pd.DataFrame(
        {
            "date": pd.Timestamp(trade_date),
            "symbol": df["TckrSymb"].str.strip(),
            "series": df["SctySrs"].str.strip(),
            "isin": df["ISIN"].str.strip(),
            "open": df["OpnPric"],
            "high": df["HghPric"],
            "low": df["LwPric"],
            "close": df["ClsPric"],
            "prev_close": df["PrvsClsgPric"],
            "volume": df["TtlTradgVol"],
            "turnover": df["TtlTrfVal"],
            "trades": df["TtlNbOfTxsExctd"],
        }
    )
    return _finalise(out)


def parse_bhav(path: Path, trade_date: date) -> pd.DataFrame:
    if path.name.startswith("BhavCopy_NSE_CM"):
        return parse_bhav_udiff(path, trade_date)
    return parse_bhav_old(path, trade_date)


# --------------------------------------------------------------------------- #
# Delivery reports -> DataFrame[symbol, series, trades?, deliv_qty, deliv_pct]
# --------------------------------------------------------------------------- #


def parse_delivery_full(path: Path) -> pd.DataFrame:
    df = _clean_columns(pd.read_csv(path, dtype=str))
    out = pd.DataFrame(
        {
            "symbol": df["SYMBOL"].str.strip(),
            "series": df["SERIES"].str.strip(),
            "volume": pd.to_numeric(df["TTL_TRD_QNTY"].str.strip(), errors="coerce"),
            "trades": pd.to_numeric(df["NO_OF_TRADES"].str.strip(), errors="coerce"),
            "deliv_qty": pd.to_numeric(df["DELIV_QTY"].str.strip(), errors="coerce"),
            "deliv_pct": pd.to_numeric(df["DELIV_PER"].str.strip(), errors="coerce"),
        }
    )
    return _dedupe_delivery(out)


def parse_mto(path: Path) -> pd.DataFrame:
    rows = []
    with path.open("r", encoding="utf-8", errors="replace") as f:
        for line in csv.reader(f):
            if len(line) >= 7 and line[0].strip() == "20":
                rows.append(line[2:7])

    out = pd.DataFrame(rows, columns=["symbol", "series", "volume", "deliv_qty", "deliv_pct"])
    out["symbol"] = out["symbol"].str.strip()
    out["series"] = out["series"].str.strip()
    for col in ("volume", "deliv_qty", "deliv_pct"):
        out[col] = pd.to_numeric(out[col].str.strip(), errors="coerce")
    out["trades"] = np.nan
    return _dedupe_delivery(out)


def _dedupe_delivery(df: pd.DataFrame) -> pd.DataFrame:
    df = df[df["series"].isin(VALID_SERIES)]
    df = df.assign(_rank=df["series"].map(SERIES_PRIORITY))
    df = df.sort_values(["symbol", "_rank"], kind="stable")
    df = df.drop_duplicates(subset=["symbol"], keep="first").drop(columns="_rank")
    return df.reset_index(drop=True)


def merge_delivery(bhav: pd.DataFrame, dlv: Optional[pd.DataFrame]) -> pd.DataFrame:
    """Attach delivery quantities to the bhavcopy rows (by symbol).

    BE/BZ (trade-for-trade) series are 100% delivery, so volume is used when
    the delivery report does not list them. Trade counts from the delivery
    report fill in gaps where the bhavcopy has none.
    """
    if dlv is None or dlv.empty:
        return bhav

    dlv = dlv.set_index("symbol")
    idx = bhav["symbol"]
    have = idx.isin(dlv.index)

    dq = pd.Series(pd.NA, index=bhav.index, dtype="Float64")
    dp = pd.Series(np.nan, index=bhav.index, dtype="float64")
    tr = pd.Series(pd.NA, index=bhav.index, dtype="Float64")

    dq[have] = dlv.loc[idx[have], "deliv_qty"].to_numpy()
    dp[have] = dlv.loc[idx[have], "deliv_pct"].to_numpy()
    tr[have] = dlv.loc[idx[have], "trades"].to_numpy()

    bhav = bhav.copy()
    t2t = bhav["series"].isin(("BE", "BZ"))
    dq[t2t] = bhav.loc[t2t, "volume"].astype("Float64")
    dp[t2t] = 100.0

    bhav["deliv_qty"] = dq.round().astype("Int64")
    bhav["deliv_pct"] = dp
    bhav["trades"] = bhav["trades"].astype("Float64").fillna(tr).round().astype("Int64")
    return bhav


# --------------------------------------------------------------------------- #
# Indices
# --------------------------------------------------------------------------- #

INDEX_COLUMNS = ["date", "index", "open", "high", "low", "close", "volume", "turnover_cr", "pe", "pb", "div_yield"]


def parse_indices(path: Path, trade_date: date) -> pd.DataFrame:
    df = _clean_columns(pd.read_csv(path, dtype=str, encoding="utf-8", encoding_errors="replace"))
    out = pd.DataFrame(
        {
            "date": pd.Timestamp(trade_date),
            "index": df["Index Name"].str.strip(),
            "open": df["Open Index Value"],
            "high": df["High Index Value"],
            "low": df["Low Index Value"],
            "close": df["Closing Index Value"],
            "volume": df["Volume"],
            "turnover_cr": df["Turnover (Rs. Cr.)"],
            "pe": df["P/E"],
            "pb": df["P/B"],
            "div_yield": df["Div Yield"],
        }
    )
    for col in INDEX_COLUMNS[2:]:
        out[col] = pd.to_numeric(out[col].astype(str).str.strip(), errors="coerce")
    out = out[out["close"].notna()]
    return out.reset_index(drop=True)


# --------------------------------------------------------------------------- #
# PR bhavcopy zip -> corporate action rows
# --------------------------------------------------------------------------- #

PR_ACTION_COLUMNS = ["symbol", "series", "ex_date", "rec_date", "purpose"]


def _parse_dates_multi(ser: pd.Series) -> pd.Series:
    ser = ser.astype(str).str.strip()
    out = pd.Series(pd.NaT, index=ser.index, dtype="datetime64[ns]")
    for fmt in ("%d/%m/%Y", "%d-%m-%Y", "%Y-%m-%d", "%d-%b-%Y"):
        missing = out.isna()
        if not missing.any():
            break
        parsed = pd.to_datetime(ser[missing], format=fmt, errors="coerce")
        out[missing] = parsed
    return out


_DATE_OR_BLANK = re.compile(r"^\s*(\d{1,2}[/-]\d{1,2}[/-]\d{2,4}|\d{4}-\d{2}-\d{2}|\d{1,2}-[A-Za-z]{3}-\d{4})?\s*$")


def _read_pr_bc_lines(raw: str) -> pd.DataFrame:
    """The bc file is comma separated but unquoted: SECURITY names and PURPOSE
    text may contain commas. Re-align such rows using the date columns."""
    lines = [ln for ln in raw.splitlines() if ln.strip()]
    if not lines:
        return pd.DataFrame()
    header = [h.strip() for h in lines[0].split(",")]
    n = len(header)
    try:
        i_rec, i_ex, i_purpose = header.index("RECORD_DT"), header.index("EX_DT"), header.index("PURPOSE")
    except ValueError:
        return pd.DataFrame()
    rows = []
    for ln in lines[1:]:
        f = ln.split(",")
        if len(f) == n:
            rows.append(f)
            continue
        if len(f) < n:
            continue
        extra = len(f) - n
        if _DATE_OR_BLANK.match(f[i_rec]) and _DATE_OR_BLANK.match(f[i_ex]):
            # extras belong to PURPOSE (last column)
            rows.append(f[:i_purpose] + [",".join(f[i_purpose:])])
        else:
            # extras belong to SECURITY (column 2)
            rows.append(f[:2] + [",".join(f[2 : 3 + extra])] + f[3 + extra :])
    return pd.DataFrame(rows, columns=header)


def parse_pr_actions(zip_path: Path) -> pd.DataFrame:
    """Extract the corporate action (bc*) file from a PR zip."""
    try:
        zf = zipfile.ZipFile(zip_path)
    except zipfile.BadZipFile:
        return pd.DataFrame(columns=PR_ACTION_COLUMNS)

    with zf:
        name = next((n for n in zf.namelist() if n.lower().startswith("bc") and n.lower().endswith(".csv")), None)
        if name is None or zf.getinfo(name).file_size == 0:
            return pd.DataFrame(columns=PR_ACTION_COLUMNS)
        raw = zf.read(name).decode("utf-8", errors="replace")

    df = _read_pr_bc_lines(raw)
    needed = {"SYMBOL", "SERIES", "RECORD_DT", "EX_DT", "PURPOSE"}
    if df.empty or not needed.issubset(df.columns):
        return pd.DataFrame(columns=PR_ACTION_COLUMNS)

    df = df[df["SERIES"].str.strip().isin(VALID_SERIES)]
    out = pd.DataFrame(
        {
            "symbol": df["SYMBOL"].str.strip(),
            "series": df["SERIES"].str.strip(),
            "ex_date": _parse_dates_multi(df["EX_DT"]),
            "rec_date": _parse_dates_multi(df["RECORD_DT"]),
            "purpose": df["PURPOSE"].astype(str).str.strip(),
        }
    )
    out = out[out["ex_date"].notna()]
    return out.reset_index(drop=True)


def bhav_filename(d: date) -> str:
    from config import UDIFF_START_DATE

    if d >= UDIFF_START_DATE:
        return f"BhavCopy_NSE_CM_0_0_0_{d:%Y%m%d}_F_0000.csv"
    return f"cm{d.strftime('%d%b%Y').upper()}bhav.csv"


def delivery_filename(d: date) -> str:
    return f"sec_bhavdata_full_{d:%d%m%Y}.csv"


def mto_filename(d: date) -> str:
    return f"MTO_{d:%d%m%Y}.DAT"


def indices_filename(d: date) -> str:
    return f"ind_close_all_{d:%d%m%Y}.csv"


def pr_filename(d: date) -> str:
    return f"PR{d:%d%m%y}.zip"
