"""Synthetic NSE raw reports and a small database built through the real ingestion code.

The market (weekdays 2024-01-01 .. 2024-02-09, old bhavcopy format + sec_bhavdata_full):

  AAA      EQ  every day; FV split 10 -> 5 ex 2024-01-08 (close halves, volume doubles)
  BBB      EQ  every day; bonus 1:1 ex 2024-02-01
  OLDNAME  EQ  renamed to NEWNAME (same ISIN) from 2024-01-15; bonus 1:2 ex 2024-01-04,
               announced under the old symbol
  DEAD     EQ  trades 2024-01-01 .. 2024-01-03 only (delisted)
  SMEX     SM  every day -> key SMEX_SME
  DUP      SM  2024-01-01 .. 2024-01-03 (key DUP_SME), then an unrelated main-board DUP with
               another ISIN from 2024-01-29 (key DUP) -> two entities share the symbol
  MIG      SM  until 2024-01-19, then EQ with the same ISIN -> one entity, key MIG
Indices: "S&P CNX Nifty" until 2024-01-12, "Nifty 50" afterwards, "Nifty Bank" every day.
"""

from __future__ import annotations

import zipfile
from collections.abc import Iterable
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

from config import UDIFF_START_DATE

START, END = date(2024, 1, 1), date(2024, 2, 9)


def weekdays(start: date = START, end: date = END) -> list[date]:
    out, d = [], start
    while d <= end:
        if d.weekday() < 5:
            out.append(d)
        d += timedelta(days=1)
    return out


DAYS = weekdays()
SPLIT_EX = date(2024, 1, 8)
BONUS_EX = date(2024, 2, 1)
OLD_BONUS_EX = date(2024, 1, 4)
RENAME_ON = date(2024, 1, 15)
MIGRATE_ON = date(2024, 1, 22)


def raw_path(kind: str, d: date) -> Path:
    import scraper  # resolved at call time so a repointed RAW_DIR is used

    return scraper.raw_path(kind, d)


# ------------------------------------------------------------------ report writers


def write_bhav(d: date, rows: Iterable[dict]) -> Path:
    """rows: symbol, series, isin, open, high, low, close, prev_close, volume, turnover, trades."""
    p = raw_path("bhav", d)
    p.parent.mkdir(parents=True, exist_ok=True)
    rows = list(rows)
    if d >= UDIFF_START_DATE:
        hdr = ("TradDt,BizDt,Sgmt,Src,FinInstrmTp,FinInstrmId,ISIN,TckrSymb,SctySrs,XpryDt,FininstrmActlXpryDt,"
               "StrkPric,OptnTp,FinInstrmNm,OpnPric,HghPric,LwPric,ClsPric,LastPric,PrvsClsgPric,UndrlygPric,"
               "SttlmPric,OpnIntrst,ChngInOpnIntrst,TtlTradgVol,TtlTrfVal,TtlNbOfTxsExctd,SsnId,NewBrdLotQty,"
               "Rmks,Rsvd1,Rsvd2,Rsvd3,Rsvd4")
        lines = [hdr] + [
            f"{d},{d},CM,NSE,STK,1,{r['isin']},{r['symbol']},{r['series']},,,,,{r['symbol']} LTD,{r['open']},"
            f"{r['high']},{r['low']},{r['close']},{r['close']},{r['prev_close']},,{r['close']},,,{r['volume']},"
            f"{r['turnover']},{r['trades']},F1,1,,,,,"
            for r in rows
        ]
    else:
        stamp = d.strftime("%d-%b-%Y").upper()
        lines = ["SYMBOL,SERIES,OPEN,HIGH,LOW,CLOSE,LAST,PREVCLOSE,TOTTRDQTY,TOTTRDVAL,TIMESTAMP,TOTALTRADES,ISIN,"] + [
            f"{r['symbol']},{r['series']},{r['open']},{r['high']},{r['low']},{r['close']},{r['close']},"
            f"{r['prev_close']},{r['volume']},{r['turnover']},{stamp},{r['trades']},{r['isin']},"
            for r in rows
        ]
    p.write_text("\n".join(lines) + "\n")
    return p


def write_sec_full(d: date, rows: Iterable[dict]) -> Path:
    p = raw_path("delivery", d)
    p.parent.mkdir(parents=True, exist_ok=True)
    lines = [("SYMBOL, SERIES, DATE1, PREV_CLOSE, OPEN_PRICE, HIGH_PRICE, LOW_PRICE, LAST_PRICE, CLOSE_PRICE, "
              "AVG_PRICE, TTL_TRD_QNTY, TURNOVER_LACS, NO_OF_TRADES, DELIV_QTY, DELIV_PER")]
    for r in rows:
        dq = int(r["volume"] * 0.4)
        lines.append(f"{r['symbol']}, {r['series']}, {d:%d-%b-%Y}, {r['prev_close']}, {r['open']}, {r['high']}, "
                     f"{r['low']}, {r['close']}, {r['close']}, {r['close']}, {r['volume']}, 1.0, {r['trades']}, "
                     f"{dq}, 40.00")
    p.write_text("\n".join(lines) + "\n")
    return p


def write_indices(d: date, rows: Iterable[dict]) -> Path:
    p = raw_path("indices", d)
    p.parent.mkdir(parents=True, exist_ok=True)
    lines = [("Index Name,Index Date,Open Index Value,High Index Value,Low Index Value,Closing Index Value,"
              "Points Change,Change(%),Volume,Turnover (Rs. Cr.),P/E,P/B,Div Yield")]
    for r in rows:
        c = r["close"]
        lines.append(f"{r['index']},{d:%d-%m-%Y},{c - 1},{c + 5},{c - 5},{c},0,0,1000,10.5,{r.get('pe', 22.5)},3.1,1.2")
    p.write_text("\n".join(lines) + "\n")
    return p


def write_pr_zip(d: date, bc_rows: Iterable[str]) -> Path:
    p = raw_path("pr", d)
    p.parent.mkdir(parents=True, exist_ok=True)
    body = "SERIES,SYMBOL,SECURITY,RECORD_DT,BC_STRT_DT,BC_END_DT,EX_DT,ND_STRT_DT,ND_END_DT,PURPOSE\n" + "\n".join(bc_rows)
    with zipfile.ZipFile(p, "w") as zf:
        zf.writestr(f"bc{d:%d%m%Y}.csv", body)
    return p


# ------------------------------------------------------------------ the synthetic market


def _px(symbol: str, i: int, d: date) -> float:
    base = {"AAA": 100.0, "BBB": 200.0, "OLDNAME": 300.0, "DEAD": 10.0, "SMEX": 50.0, "DUP": 30.0, "MIG": 40.0}[symbol]
    c = base + i  # gentle uptrend, one rupee a day
    if symbol == "AAA" and d >= SPLIT_EX:
        c /= 2
    if symbol == "BBB" and d >= BONUS_EX:
        c /= 2
    if symbol == "OLDNAME" and d >= OLD_BONUS_EX:
        c /= 1.5
    return round(c, 2)


def _vol(symbol: str, d: date) -> int:
    v = 1000
    if symbol == "AAA" and d >= SPLIT_EX:
        v *= 2
    if symbol == "BBB" and d >= BONUS_EX:
        v *= 2
    return v


def day_rows(d: date) -> list[dict]:
    i = DAYS.index(d) if d in DAYS else len(DAYS)
    secs = [("AAA", "EQ", "INE000A01011"), ("BBB", "EQ", "INE000B01011"), ("SMEX", "SM", "INE000D01011")]
    secs.append(("OLDNAME" if d < RENAME_ON else "NEWNAME", "EQ", "INE000C01011"))
    if d <= date(2024, 1, 3):
        secs += [("DEAD", "EQ", "INE000E01011"), ("DUP", "SM", "INE000F01011")]
    if d >= date(2024, 1, 29):
        secs.append(("DUP", "EQ", "INE000G01011"))
    secs.append(("MIG", "SM" if d < MIGRATE_ON else "EQ", "INE000H01011"))
    rows = []
    for sym, series, isin in secs:
        price_sym = "OLDNAME" if sym == "NEWNAME" else sym
        c = _px(price_sym, i, d)
        prev = _px(price_sym, i - 1, DAYS[i - 1]) if i > 0 else c
        vol = _vol(price_sym, d)
        rows.append({"symbol": sym, "series": series, "isin": isin, "open": c, "high": round(c * 1.01, 2),
                     "low": round(c * 0.99, 2), "close": c, "prev_close": prev, "volume": vol,
                     "turnover": round(vol * c, 2), "trades": 10 + i})
    return rows


def index_rows(d: date) -> list[dict]:
    i = DAYS.index(d) if d in DAYS else len(DAYS)
    nifty = "S&P CNX Nifty" if d <= date(2024, 1, 12) else "Nifty 50"
    return [{"index": nifty, "close": 21000.0 + 10 * i, "pe": 22.0}, {"index": "Nifty Bank", "close": 47000.0 + i}]


def write_day(d: date, delivery: bool = True, indices: bool = True) -> None:
    rows = day_rows(d)
    write_bhav(d, rows)
    if delivery:
        write_sec_full(d, rows)
    if indices:
        write_indices(d, index_rows(d))


API_ACTIONS = [
    {"symbol": "AAA", "series": "EQ", "subject": "Face Value Split (Sub-Division) - From Rs 10/- Per Share To Rs 5/- Per Share",
     "exDate": SPLIT_EX.strftime("%d-%b-%Y"), "recDate": SPLIT_EX.strftime("%d-%b-%Y"), "isin": "INE000A01011"},
    {"symbol": "BBB", "series": "EQ", "subject": "Bonus 1:1", "exDate": BONUS_EX.strftime("%d-%b-%Y"), "recDate": "-"},
    {"symbol": "OLDNAME", "series": "EQ", "subject": "Bonus 1:2", "exDate": OLD_BONUS_EX.strftime("%d-%b-%Y"), "recDate": "-"},
    {"symbol": "BBB", "series": "EQ", "subject": "Dividend - Rs 5 Per Share", "exDate": "15-Jan-2024", "recDate": "15-Jan-2024"},
]


def build_db(days: list[date] | None = None) -> dict:
    """Write raw reports and run the real ingest -> actions -> adjust -> master -> breadth path."""
    import adjuster
    import breadth
    import pipeline
    import storage
    from storage import IndexStore, Store
    from symbol_master import EntityResolver

    days = days or DAYS
    for d in days:
        write_day(d)
    store, istore, resolver = Store(), IndexStore(), EntityResolver()
    stats = pipeline.ingest_dates(days, resolver, store, istore, batch_by="day")
    resolver.save_outputs(as_of=days[-1])
    api = adjuster.api_rows_to_frame(API_ACTIONS)
    table = adjuster.build_actions_table(resolver, [api], existing=None, store=store)
    adjuster.save_actions(table)
    adjuster.apply_adjustments(store, table)
    breadth.rebuild_breadth(store)
    meta = storage.load_meta()
    meta.update(last_synced=days[-1].isoformat(), adjustments_applied=True, symbol_count=len(resolver.entities),
                breadth_last_date=days[-1].isoformat())
    storage.save_meta(meta)
    return {"stats": stats, "resolver": resolver, "actions": table}


def raw_close(key: str) -> pd.Series:
    from storage import Store

    df = Store().read(key)
    return df.set_index("date")["close"]
