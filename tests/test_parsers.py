from __future__ import annotations

import zipfile
from datetime import date

import numpy as np
import pandas as pd
import pytest

import parsers
from parsers import (
    EQUITY_COLUMNS,
    bhav_filename,
    merge_delivery,
    parse_bhav,
    parse_delivery_full,
    parse_indices,
    parse_mto,
    parse_pr_actions,
)


def _row(df, symbol):
    r = df[df["symbol"] == symbol]
    assert len(r) == 1, f"{symbol}: {len(r)} rows"
    return r.iloc[0]


# ------------------------------------------------------------------ old bhavcopy


def test_old_bhav_pre_isin_era(fixtures):
    df = parse_bhav(fixtures / "cm03JAN2005bhav.csv", date(2005, 1, 3))
    assert list(df.columns) == EQUITY_COLUMNS
    # GS series dropped, zero close dropped, ABB deduped to EQ (higher priority than BE)
    assert sorted(df["symbol"]) == ["20MICRONS", "ABB"]
    abb = _row(df, "ABB")
    assert abb["series"] == "EQ" and abb["close"] == 1005.5 and abb["prev_close"] == 995
    assert abb["volume"] == 2000 and abb["turnover"] == 2011000.0
    # no ISIN / trade count before 2011
    assert df["isin"].isna().all() and df["trades"].isna().all()
    assert (df["date"] == pd.Timestamp("2005-01-03")).all()
    assert str(df["volume"].dtype) == "Int64" and str(df["trades"].dtype) == "Int64"
    assert df["close"].dtype == np.float64 and df["deliv_pct"].dtype == np.float64


def test_old_bhav_with_isin(fixtures):
    df = parse_bhav(fixtures / "cm01AUG2011bhav.csv", date(2011, 8, 1))
    # N1 (debt) series dropped; rights entitlement (-RE) dropped; BE and SM kept
    assert sorted(df["symbol"]) == ["INFY", "SMALLCO", "SMEONE", "TATAMTRDVR"]
    infy = _row(df, "INFY")
    assert infy["isin"] == "INE009A01021" and infy["trades"] == 45000
    assert infy["close"] == 2740.55 and infy["turnover"] == 4100000000.25
    assert _row(df, "TATAMTRDVR")["series"] == "EQ"
    assert _row(df, "SMEONE")["series"] == "SM"


def test_udiff_bhav(fixtures):
    p = fixtures / "BhavCopy_NSE_CM_0_0_0_20240708_F_0000.csv"
    df = parse_bhav(p, date(2024, 7, 8))
    assert list(df.columns) == EQUITY_COLUMNS
    # FO segment row and GB series dropped
    assert sorted(df["symbol"]) == ["INFY", "RELIANCE", "SMALLCO", "SMEONE"]
    rel = _row(df, "RELIANCE")
    assert rel["isin"] == "INE002A01018" and rel["series"] == "EQ"
    assert (rel["open"], rel["high"], rel["low"], rel["close"], rel["prev_close"]) == (3200, 3215.5, 3180, 3195.25, 3190.1)
    assert rel["volume"] == 5000000 and rel["trades"] == 250000 and rel["turnover"] == 15976250000.5
    assert df["deliv_qty"].isna().all()


@pytest.mark.parametrize(
    "d, name",
    [
        (date(2024, 7, 5), "cm05JUL2024bhav.csv"),
        (date(2024, 7, 8), "BhavCopy_NSE_CM_0_0_0_20240708_F_0000.csv"),
        (date(2005, 1, 3), "cm03JAN2005bhav.csv"),
    ],
)
def test_bhav_filename_switches_on_udiff_date(d, name):
    assert bhav_filename(d) == name


def test_other_filenames():
    d = date(2019, 3, 4)
    assert parsers.delivery_filename(d) == "sec_bhavdata_full_04032019.csv"
    assert parsers.mto_filename(d) == "MTO_04032019.DAT"
    assert parsers.indices_filename(d) == "ind_close_all_04032019.csv"
    assert parsers.pr_filename(d) == "PR040319.zip"


# ------------------------------------------------------------------ delivery


def test_sec_bhavdata_full(fixtures):
    dlv = parse_delivery_full(fixtures / "sec_bhavdata_full_08072024.csv")
    assert sorted(dlv["symbol"]) == ["INFY", "RELIANCE", "SMALLCO", "SMEONE"]  # ETF series dropped
    rel = _row(dlv, "RELIANCE")
    assert rel["deliv_qty"] == 2500000 and rel["deliv_pct"] == 50.0 and rel["trades"] == 250000
    sme = _row(dlv, "SMEONE")
    assert np.isnan(sme["deliv_qty"]) and np.isnan(sme["deliv_pct"])  # "-" in the report


def test_mto(fixtures):
    dlv = parse_mto(fixtures / "MTO_03012005.DAT")
    assert sorted(dlv["symbol"]) == ["20MICRONS", "ABB"]
    abb = _row(dlv, "ABB")
    assert abb["series"] == "EQ" and abb["deliv_qty"] == 1500 and abb["deliv_pct"] == 75.0
    assert dlv["trades"].isna().all()


def test_merge_delivery_udiff_plus_full(fixtures):
    bhav = parse_bhav(fixtures / "BhavCopy_NSE_CM_0_0_0_20240708_F_0000.csv", date(2024, 7, 8))
    dlv = parse_delivery_full(fixtures / "sec_bhavdata_full_08072024.csv")
    out = merge_delivery(bhav, dlv)
    assert list(out.columns) == EQUITY_COLUMNS
    assert _row(out, "RELIANCE")["deliv_qty"] == 2500000 and _row(out, "RELIANCE")["deliv_pct"] == 50.0
    assert _row(out, "INFY")["deliv_qty"] == 4200000
    # BZ (trade-for-trade) is 100% delivery: volume is used whatever the report says
    sz = _row(out, "SMALLCO")
    assert sz["deliv_qty"] == 1000 and sz["deliv_pct"] == 100.0
    assert pd.isna(_row(out, "SMEONE")["deliv_qty"])
    assert str(out["deliv_qty"].dtype) == "Int64" and str(out["trades"].dtype) == "Int64"


def test_merge_delivery_fills_trades_and_handles_missing(fixtures):
    bhav = parse_bhav(fixtures / "cm03JAN2005bhav.csv", date(2005, 1, 3))
    assert merge_delivery(bhav, None) is bhav
    dlv = parse_mto(fixtures / "MTO_03012005.DAT").assign(trades=[7, np.nan])
    out = merge_delivery(bhav, dlv)
    m = _row(out, "20MICRONS")
    assert m["deliv_qty"] == 6000 and m["deliv_pct"] == 48.6 and m["trades"] == 7
    assert pd.isna(_row(out, "ABB")["trades"])


# ------------------------------------------------------------------ indices


def test_indices(fixtures):
    df = parse_indices(fixtures / "ind_close_all_08072024.csv", date(2024, 7, 8))
    assert list(df.columns) == parsers.INDEX_COLUMNS
    assert list(df["index"]) == ["Nifty 50", "Nifty Bank", "India VIX"]  # no close -> dropped
    n = df.iloc[0]
    assert n["close"] == 24320.55 and n["pe"] == 23.03 and n["pb"] == 4.12 and n["div_yield"] == 1.18
    assert n["volume"] == 270000000 and n["turnover_cr"] == 27000.5
    vix = df.iloc[2]
    assert np.isnan(vix["volume"]) and np.isnan(vix["pe"])
    assert (df["date"] == pd.Timestamp("2024-07-08")).all()


# ------------------------------------------------------------------ PR corporate actions


def _pr_zip(tmp_path, bc_text: str, name="PR080724.zip", member="bc08072024.csv"):
    p = tmp_path / name
    with zipfile.ZipFile(p, "w") as zf:
        zf.writestr(member, bc_text)
        zf.writestr("Pd08072024.csv", "unrelated")
    return p


def test_pr_actions_realigns_commas(tmp_path, fixtures):
    p = _pr_zip(tmp_path, (fixtures / "bc08072024.csv").read_text())
    df = parse_pr_actions(p)
    assert list(df.columns) == parsers.PR_ACTION_COLUMNS
    by = df.set_index("symbol")
    # N1 series dropped, row without EX_DT dropped
    assert sorted(by.index) == ["ABC", "INFY", "SMEONE", "XYZ"]
    assert by.loc["ABC", "purpose"] == "BONUS 1:1"  # comma in SECURITY
    assert by.loc["ABC", "ex_date"] == pd.Timestamp("2024-07-10")
    assert by.loc["XYZ", "purpose"] == "AGM/DIV RS 2, SPL DIV RS 1"  # comma in PURPOSE
    assert by.loc["INFY", "rec_date"] == pd.Timestamp("2024-07-09")
    assert by.loc["SMEONE", "series"] == "SM"


def test_pr_actions_bad_or_empty_zip(tmp_path):
    bad = tmp_path / "PR010124.zip"
    bad.write_bytes(b"<html>not a zip</html>")
    assert parse_pr_actions(bad).empty
    empty = _pr_zip(tmp_path, "", name="PR020124.zip", member="bc02012024.csv")
    assert parse_pr_actions(empty).empty


def test_parse_dates_multi_formats():
    s = pd.Series(["08/07/2024", "08-07-2024", "2024-07-08", "08-Jul-2024", "", "garbage"])
    out = parsers._parse_dates_multi(s)
    assert list(out[:4]) == [pd.Timestamp("2024-07-08")] * 4
    assert out[4:].isna().all()
