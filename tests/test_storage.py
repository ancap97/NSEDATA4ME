from __future__ import annotations

import json
from datetime import date

import numpy as np
import pandas as pd
import pytest

from config import STORE_COLUMNS


def _rows(dates, close=100.0, symbol="ABC"):
    return pd.DataFrame(
        {
            "date": pd.to_datetime(dates),
            "symbol": symbol,
            "series": "EQ",
            "isin": "INE000X01011",
            "open": close, "high": close, "low": close, "close": close, "prev_close": close,
            "volume": 10, "turnover": 10 * close, "trades": 1, "deliv_qty": 5, "deliv_pct": 50.0,
        }
    )


def test_normalise_store_frame_schema():
    from storage import normalise_store_frame

    df = _rows(["2024-01-02", "2024-01-01", "2024-01-02"]).drop(columns=["trades", "isin"])
    df.loc[2, "close"] = 101.0
    out = normalise_store_frame(df)
    assert list(out.columns) == STORE_COLUMNS
    assert list(out["date"]) == [pd.Timestamp("2024-01-01"), pd.Timestamp("2024-01-02")]
    assert out["close"].iloc[1] == 101.0  # duplicate date: last row wins
    assert (out["adj_factor"] == 1.0).all()
    assert str(out["trades"].dtype) == "Int64" and out["trades"].isna().all()
    assert out["isin"].isna().all()


def test_store_append_rename_remove(data_dir):
    from storage import Store

    s = Store()
    s.append("ABC", _rows(["2024-01-01", "2024-01-02"]))
    s.append("ABC", _rows(["2024-01-02", "2024-01-03"], close=105.0))
    df = s.read("ABC")
    assert list(df["close"]) == [100.0, 105.0, 105.0]  # re-appended day replaced
    assert s.list_keys() == ["ABC"] and not list(s.dir.glob("*.tmp"))
    assert list(s.read("ABC", columns=["date"]).columns) == ["date"]
    assert s.read("NOPE").empty and list(s.read("NOPE").columns) == STORE_COLUMNS

    s.rename("ABC", "XYZ")
    assert s.list_keys() == ["XYZ"] and len(s.read("XYZ")) == 3
    s.rename("MISSING", "XYZ")  # no-op
    s.append("OLD", _rows(["2024-01-04"]))
    s.rename("OLD", "XYZ")  # merge into an existing file
    assert s.list_keys() == ["XYZ"] and len(s.read("XYZ")) == 4

    assert s.remove_dates(["XYZ", "MISSING"], [date(2024, 1, 2)]) == 1
    assert pd.Timestamp("2024-01-02") not in set(s.read("XYZ")["date"])
    assert s.remove_dates(["XYZ"], [date(2030, 1, 1)]) == 0
    assert s.last_dates() == {"XYZ": pd.Timestamp("2024-01-04")}


def test_store_key_case_and_path(data_dir):
    from storage import Store

    s = Store()
    assert s.path("M&M").name == "m&m.parquet" and s.path("ABC_SME").name == "abc_sme.parquet"


def test_index_store_canonical_names(data_dir):
    from storage import IndexStore, canonical_index_name

    assert canonical_index_name("S&P CNX Nifty") == "nifty 50"
    assert canonical_index_name("CNX  Nifty Junior") == "nifty next 50"
    assert canonical_index_name("CNX IT") == "nifty it"
    assert canonical_index_name("Nifty 50") == "nifty 50"
    ist = IndexStore()
    cols = ["date", "index", "open", "high", "low", "close", "volume", "turnover_cr", "pe", "pb", "div_yield"]
    mk = lambda d, n, c: pd.DataFrame([[pd.Timestamp(d), n, c, c, c, c, 1, 1.0, 20.0, 3.0, 1.0]], columns=cols)
    ist.append_day(mk("2015-11-05", "CNX Nifty", 1.0))
    ist.append_day(pd.concat([mk("2015-11-09", "Nifty 50", 2.0), mk("2015-11-09", "Nifty Bank", 5.0)]))
    df = ist.read("nifty 50")
    assert list(df["close"]) == [1.0, 2.0]
    assert ist.list_names() == ["Nifty 50", "Nifty Bank"]
    assert sorted(ist.list_names(canonical=False)) == ["CNX Nifty", "Nifty 50", "Nifty Bank"]
    ist.remove_dates([date(2015, 11, 9)])
    assert list(IndexStore().read("Nifty 50")["close"]) == [1.0]


def test_meta_roundtrip_backup_and_corruption(data_dir):
    import storage

    m = storage.load_meta()
    assert m["last_synced"] is None and m["schema_version"] == 1
    m["last_synced"] = "2024-01-02"
    m["when"] = pd.Timestamp("2024-01-02 18:30")
    storage.save_meta(m)
    m["last_synced"] = "2024-01-03"
    storage.save_meta(m)
    assert storage.load_meta()["last_synced"] == "2024-01-03"
    bak = json.loads((data_dir / "meta.json.bak").read_text())
    assert bak["last_synced"] == "2024-01-02" and bak["when"] == "2024-01-02T18:30:00"
    assert not (data_dir / "meta.tmp").exists()
    (data_dir / "meta.json").write_text("{not json")
    with pytest.raises(RuntimeError, match="not valid JSON"):
        storage.load_meta()


def test_atomic_parquet_leaves_no_tmp(data_dir, tmp_path):
    from storage import _atomic_parquet

    p = tmp_path / "x.parquet"
    _atomic_parquet(pd.DataFrame({"a": [1]}), p)
    assert p.exists() and not (tmp_path / "x.parquet.tmp").exists()
    assert pd.read_parquet(p)["a"].tolist() == [1]


def test_store_roundtrip_dtypes(data_dir):
    from storage import Store

    s = Store()
    s.write("ABC", _rows(["2024-01-01"]).assign(isin=None, trades=pd.NA))
    df = s.read("ABC")
    assert str(df["trades"].dtype) == "Int64" and df["trades"].isna().all()
    assert df["isin"].isna().all()
    assert df["adj_factor"].dtype == np.float64



def test_load_meta_defaults_are_not_shared(data_dir):
    """Mutating one loaded meta must not leak into DEFAULT_META / the next load_meta()."""
    import storage

    (data_dir / "meta.json").write_text(json.dumps({"last_synced": "2024-01-02"}))
    m = storage.load_meta()
    m["pending_delivery"].append("2024-01-02")
    m["holidays"]["26-Jan-2024"] = "Republic Day"
    m["special_sessions"].append("2024-01-20")
    fresh = storage.load_meta()
    assert fresh["pending_delivery"] == [] and fresh["holidays"] == {} and fresh["special_sessions"] == []
    assert storage.DEFAULT_META["pending_delivery"] == [] and storage.DEFAULT_META["holidays"] == {}
