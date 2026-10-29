"""Public read API against a small database built through the real ingest/adjust code."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from tests import factory
from tests._repoint import repoint

EXPECTED_COLUMNS = ["date", "symbol", "key", "isin", "series", "open", "high", "low", "close", "prev_close",
                    "volume", "turnover", "trades", "deliv_qty", "deliv_pct", "adj_factor"]


@pytest.fixture(scope="module")
def db(tmp_path_factory):
    with pytest.MonkeyPatch.context() as mp:
        d = repoint(tmp_path_factory.mktemp("db") / "data", mp)
        import config

        config.ensure_dirs()
        info = factory.build_db()
        yield {"dir": d, **info}


@pytest.fixture
def loader(db):
    import loader as _loader

    return _loader


def test_columns_and_dtypes(loader):
    df = loader.load_data(["AAA"])
    assert list(df.columns) == EXPECTED_COLUMNS
    assert str(df["date"].dtype) == "datetime64[ns]"
    for c in ("open", "high", "low", "close", "prev_close", "turnover", "deliv_pct", "adj_factor"):
        assert df[c].dtype == np.float64, c
    for c in ("volume", "trades", "deliv_qty"):
        assert str(df[c].dtype) == "Int64", c
    assert df["date"].is_monotonic_increasing and len(df) == len(factory.DAYS)


def test_adjusted_vs_unadjusted_split(loader):
    raw = loader.load_data(["AAA"], adjusted=False).set_index("date")
    adj = loader.load_data(["AAA"]).set_index("date")
    before = raw.index < pd.Timestamp(factory.SPLIT_EX)
    # unadjusted = NSE prices exactly as published
    assert raw.loc[before, "close"].iloc[0] == 100.0 and raw.loc[~before, "close"].iloc[0] == 52.5
    assert (raw["adj_factor"][before] == 2.0).all() and (raw["adj_factor"][~before] == 1.0).all()
    # adjusted: prices / factor, quantities * factor, turnover and trades untouched
    for c in ("open", "high", "low", "close", "prev_close"):
        assert np.allclose(adj[c], raw[c] / raw["adj_factor"]), c
    assert (adj["volume"] == (raw["volume"] * raw["adj_factor"]).astype("Int64")).all()
    assert (adj["deliv_qty"] == (raw["deliv_qty"].astype("Float64") * raw["adj_factor"]).round().astype("Int64")).all()
    assert adj["turnover"].equals(raw["turnover"]) and adj["trades"].equals(raw["trades"])
    # continuity across the split: adjusted close keeps rising by ~0.5 a day
    assert adj["close"].diff().dropna().between(0.4, 0.6).all()


def test_old_symbol_resolves_to_current(loader):
    old = loader.load_symbol("OLDNAME")
    new = loader.load_symbol("NEWNAME")
    pd.testing.assert_frame_equal(old, new)
    assert (old["symbol"] == "NEWNAME").all() and (old["key"] == "NEWNAME").all()
    assert old.index.name == "date" and len(old) == len(factory.DAYS)
    # bonus 1:2 announced under the old name is applied to the whole history
    assert old["adj_factor"][old.index < pd.Timestamp(factory.OLD_BONUS_EX)].eq(1.5).all()
    assert loader.resolve_symbols(["oldname", " NEWNAME "]) == ["NEWNAME", "NEWNAME"]
    with pytest.raises(KeyError):
        loader.resolve_symbols(["NOSUCH"])


def test_universe_filters(loader):
    keys = lambda **kw: sorted(loader.load_data(columns=["key"], **kw)["key"].unique())
    assert keys() == ["AAA", "BBB", "DEAD", "DUP", "MIG", "NEWNAME"]
    assert keys(include_delisted=False) == ["AAA", "BBB", "DUP", "MIG", "NEWNAME"]
    assert keys(include_sme=True) == ["AAA", "BBB", "DEAD", "DUP", "DUP_SME", "MIG", "NEWNAME", "SMEX_SME"]
    assert keys(include_sme=True, include_delisted=False) == ["AAA", "BBB", "DUP", "MIG", "NEWNAME", "SMEX_SME"]
    # SME -> main board migration keeps one history under the main-board key
    mig = loader.load_symbol("MIG", adjusted=False)
    assert len(mig) == len(factory.DAYS) and set(mig["series"]) == {"SM", "EQ"}


def test_list_symbols(loader):
    allm = loader.list_symbols()
    assert not allm["is_sme"].any() and "DEAD" in set(allm["key"])
    act = loader.list_symbols(include_delisted=False, include_sme=True)
    assert set(act["status"]) == {"active"} and "SMEX_SME" in set(act["key"]) and "DEAD" not in set(act["key"])
    row = allm.set_index("key").loc["NEWNAME"]
    assert row["previous_symbols"] == "OLDNAME" and row["isin"] == "INE000C01011"


def test_date_filters_and_columns(loader):
    df = loader.load_data(["BBB", "AAA"], start_date="2024-01-08", end_date=pd.Timestamp("2024-01-12"))
    assert df["date"].min() == pd.Timestamp("2024-01-08") and df["date"].max() == pd.Timestamp("2024-01-12")
    assert list(df["symbol"].unique()) == ["AAA", "BBB"]  # sorted by symbol, then date
    assert len(df) == 10
    cols = loader.load_data(["AAA"], columns=["close", "nonexistent", "date"])
    assert list(cols.columns) == ["close", "date"]
    full = loader.load_data(["AAA"])
    assert cols["close"].equals(full["close"])
    assert loader.load_data(["AAA"], start_date="2030-01-01").empty
    assert len(loader.load_data(["AAA"], start_date=None)) == len(factory.DAYS)


def test_delisted_kept_with_history(loader):
    dead = loader.load_symbol("DEAD")
    assert list(dead.index) == [pd.Timestamp(d) for d in factory.DAYS[:3]]


def test_load_wide(loader):
    px = loader.load_wide("close", start_date="2024-01-02")
    assert px.columns.name == "symbol" and px.index.name == "date"
    assert list(px.columns) == ["AAA", "BBB", "DEAD", "DUP", "MIG", "NEWNAME"]
    assert px.index.min() == pd.Timestamp("2024-01-02")
    long = loader.load_data(["AAA"], start_date="2024-01-02").set_index("date")["close"]
    assert np.array_equal(px["AAA"].to_numpy(), long.to_numpy())
    # DUP is used by two entities (SME issue and an unrelated main-board company): store keys
    both = loader.load_wide("close", include_sme=True)
    assert {"DUP", "DUP_SME", "SMEX"} <= set(both.columns)
    raw = loader.load_wide("volume", adjusted=False, symbols=["AAA"])
    assert raw["AAA"].iloc[0] == 1000


def test_load_index_merges_renamed_index(loader):
    n = loader.load_index("Nifty 50")
    assert len(n) == len(factory.DAYS) and n["date"].is_monotonic_increasing
    assert set(n["index"]) == {"S&P CNX Nifty", "Nifty 50"}
    pd.testing.assert_frame_equal(loader.load_index("S&P CNX Nifty"), n)
    w = loader.load_index("nifty bank", start_date="2024-01-10", end_date="2024-01-12")
    assert list(w["date"]) == [pd.Timestamp(f"2024-01-1{i}") for i in (0, 1, 2)]
    assert loader.load_index("No Such Index").empty
    assert loader.list_indices() == ["Nifty 50", "Nifty Bank"]


def test_load_breadth(loader):
    b = loader.load_breadth()
    assert len(b) == len(factory.DAYS) and list(b.columns[:3]) == ["date", "universe", "advances"]
    w = loader.load_breadth(start_date="2024-02-01", end_date="2024-02-05")
    assert list(w["date"]) == [pd.Timestamp(x) for x in ("2024-02-01", "2024-02-02", "2024-02-05")]
    # adjusted closes all rise every day in the synthetic market: no declines after day 1
    assert (b["declines"].iloc[1:] == 0).all()


def test_load_actions(loader):
    a = loader.load_actions()
    assert a["ex_date"].is_monotonic_increasing
    adj = loader.load_actions("OLDNAME", adjusting_only=True)
    assert list(adj["type"]) == ["BONUS"] and adj["key"].iloc[0] == "NEWNAME" and adj["factor"].iloc[0] == 1.5
    bbb = loader.load_actions("BBB")
    assert list(bbb["type"]) == ["DIVIDEND", "BONUS"]
    assert list(loader.load_actions("BBB", adjusting_only=True)["type"]) == ["BONUS"]


@pytest.mark.parametrize("adjusted", [True, False])
@pytest.mark.parametrize(
    "columns",
    [["close"], ["date", "symbol", "close", "volume"], ["volume", "deliv_qty", "turnover"], ["key"],
     ["isin", "series", "trades"], ["adj_factor", "prev_close"], ["nonexistent"], ["symbol", "key", "date"]],
)
def test_columns_arg_equals_full_load_then_select(loader, columns, adjusted):
    """Reading only the needed store columns must give exactly the full result's columns."""
    kw = {"start_date": "2024-01-03", "include_sme": True, "adjusted": adjusted}
    full = loader.load_data(**kw)
    got = loader.load_data(columns=columns, **kw)
    pd.testing.assert_frame_equal(got, full.loc[:, [c for c in columns if c in full.columns]])
    sym = loader.load_data(["OLDNAME", "AAA"], columns=columns, **kw)
    sym_full = loader.load_data(["OLDNAME", "AAA"], **kw)
    pd.testing.assert_frame_equal(sym, sym_full.loc[:, [c for c in columns if c in sym_full.columns]])


@pytest.mark.parametrize(
    "kw",
    [
        {"columns": ["date", "symbol", "close"]},
        {"columns": ["close", "volume", "deliv_qty", "trades"], "adjusted": False},
        {"columns": ["open", "high", "low", "close", "prev_close", "volume", "turnover", "trades", "deliv_qty",
                     "deliv_pct", "adj_factor"], "include_sme": True},
        {"columns": ["close"], "start_date": "2024-01-08", "end_date": "2024-01-31"},
        {"columns": ["close"], "start_date": None, "include_delisted": False},
        {"columns": ["close"], "start_date": "2030-01-01"},
        {"columns": ["close", "isin"]},  # string column: scan declines, file-by-file path
        {"symbols": ["AAA", "OLDNAME", "NEWNAME"], "columns": ["close"]},  # duplicate key: declines
    ],
)
def test_scan_path_matches_file_by_file(loader, monkeypatch, kw):
    monkeypatch.setattr(loader, "SCAN_MIN_KEYS", 10**9)
    slow = loader.load_data(**kw)
    monkeypatch.setattr(loader, "SCAN_MIN_KEYS", 1)
    fast = loader.load_data(**kw)
    pd.testing.assert_frame_equal(fast, slow)
    if "symbols" in kw:  # aliases of one security: load_wide raises on the duplicates either way
        return
    wide_kw = {k: v for k, v in kw.items() if k != "columns"}
    monkeypatch.setattr(loader, "SCAN_MIN_KEYS", 10**9)
    w_slow = loader.load_wide("close", **wide_kw)
    monkeypatch.setattr(loader, "SCAN_MIN_KEYS", 1)
    pd.testing.assert_frame_equal(loader.load_wide("close", **wide_kw), w_slow)


def test_store_scan_guards(db):
    from storage import Store

    s = Store()
    keys = ["AAA", "BBB", "NOFILE", "DEAD"]
    frame, owner = s.scan(keys, ["date", "close"])
    assert sorted(set(owner)) == [0, 1, 3]  # missing file skipped, positions refer to `keys`
    per_file = pd.concat([s.read(k, columns=["date", "close"]) for k in ("AAA", "BBB", "DEAD")], ignore_index=True)
    pd.testing.assert_frame_equal(frame, per_file)
    assert s.scan(keys, ["date", "isin"]) is None
    assert s.scan(keys, ["close"]) is None  # date is needed for the bounds
    assert s.scan(["AAA", "AAA"], ["date", "close"]) is None
    assert s.scan(keys, ["date", "close"], start=pd.Timestamp("2024-01-02", tz="Asia/Kolkata")) is None
    assert s.scan(keys, ["date", "close"], start=pd.NaT) is None
    empty, own = s.scan(["NOFILE"], ["date", "close"])
    assert empty.empty and len(own) == 0
    f, o = s.scan(keys, ["date", "close"], start=pd.Timestamp("2030-01-01"))
    assert f.empty and len(o) == 0


def test_store_scan_declines_mixed_file_schemas(data_dir):
    from storage import Store

    s = Store()
    base = pd.DataFrame({"date": pd.to_datetime(["2024-01-01"]).astype("datetime64[ns]"), "close": [1.0],
                         "volume": [10]})
    s.write("A", base)
    s.write("B", base)
    assert s.scan(["A", "B"], ["date", "volume"]) is not None
    # a file written outside Store.write with another type for volume
    base.assign(volume=[10.0]).to_parquet(s.path("B"), index=False)
    assert s.scan(["A", "B"], ["date", "volume"]) is None
    assert s.scan(["A", "B"], ["date", "close"]) is not None
    base.assign(date=base["date"].astype("datetime64[us]")).to_parquet(s.path("B"), index=False)
    assert s.scan(["A", "B"], ["date", "close"]) is None  # date unit differs
