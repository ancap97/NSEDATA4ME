from __future__ import annotations

from datetime import date

import numpy as np
import pandas as pd
import pytest

import adjuster
from adjuster import classify, compute_adj_factor

# ------------------------------------------------------------------ purpose classification


@pytest.mark.parametrize(
    "purpose, expected",
    [
        ("Face Value Split (Sub-Division) - From Rs 10/- Per Share To Rs 2/- Per Share", [("SPLIT", 5.0, None)]),
        ("Face Value Split (Sub-Division) - From Rs 2/- Per Share To Re 1/- Per Share", [("SPLIT", 2.0, None)]),
        ("Bonus 1:1", [("BONUS", 2.0, None)]),
        ("Bonus 4:1", [("BONUS", 5.0, None)]),
        ("Bonus 2:5", [("BONUS", 1.4, None)]),
        ("Consolidation Of Shares From Rs 1 Per Share To Rs 10 Per Share", [("CONSOLIDATION", 0.1, None)]),
        ("Spl-Rs10 To Rs2", [("SPLIT", 5.0, None)]),  # pre-2011 abbreviation
        ("Bon 1:2", [("BONUS", 1.5, None)]),
        ("Bonus Debentures 1:1", []),  # debenture bonus is not an equity bonus
        ("Rights 1:5 @ Premium Rs 100", [("RIGHTS", None, None)]),
        ("Demerger", [("SCHEME", None, None)]),
        ("Buy Back", [("BUYBACK", None, None)]),
        ("FV SPLIT RS 10 TO [TRUNCATED]", [("SPLIT_UNPARSED", None, None)]),
        ("", []),
    ],
)
def test_classify(purpose, expected):
    got = classify(purpose)
    assert [(t, None if f is None else round(f, 9), a) for t, f, a in got] == expected


def test_classify_dividends():
    assert classify("Interim Dividend - Rs 7.50 Per Share") == [("DIVIDEND", None, 7.5)]
    # "Spl" next to a dividend is a special dividend, not a split
    t = classify("Agm/Div-Rs 5/Spl Div-Rs 7.5")
    assert [x[0] for x in t] == ["DIVIDEND"] and t[0][2] == 12.5


def test_normalise_pr_purpose_and_split_combined():
    assert adjuster.normalise_pr_purpose("AGM/DIV-RS.5") == "AGM/ DIVIDEND - RS 5"
    s = adjuster.normalise_pr_purpose("BON 1:1/FVSPLT RS10-RS5")
    assert "BONUS" in s and "FV SPLIT" in s
    assert adjuster.split_combined_purpose("BONUS 1:1 / FV SPLIT RS 10 TO RS 2") == ["BONUS 1:1", "FV SPLIT RS 10 TO RS 2"]
    assert adjuster.split_combined_purpose("BONUS 1:1") == ["BONUS 1:1"]


# ------------------------------------------------------------------ adj_factor maths (hand computed)


def _acts(*rows):
    return pd.DataFrame(rows, columns=["ex_date", "type", "factor"]).assign(ex_date=lambda d: pd.to_datetime(d["ex_date"]))


DATES = pd.Series(pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-03", "2024-01-04", "2024-01-05"]))


def test_factor_split_then_bonus():
    # split 10->2 (x5) ex 01-03, bonus 1:1 (x2) ex 01-05
    acts = _acts(("2024-01-05", "BONUS", 2.0), ("2024-01-03", "SPLIT", 5.0))  # unsorted on purpose
    assert compute_adj_factor(DATES, acts).tolist() == [10.0, 10.0, 2.0, 2.0, 1.0]


def test_factor_applies_strictly_before_ex_date():
    acts = _acts(("2024-01-03", "SPLIT", 2.0))
    f = compute_adj_factor(DATES, acts)
    assert f.tolist() == [2.0, 2.0, 1.0, 1.0, 1.0]  # the ex-date row itself is post-action


def test_factor_consolidation():
    acts = _acts(("2024-01-02", "CONSOLIDATION", 0.1))  # 10 shares of Re 1 -> 1 share of Rs 10
    f = compute_adj_factor(DATES, acts)
    assert f.tolist() == [0.1, 1.0, 1.0, 1.0, 1.0]
    raw_close = np.array([10.0, 100.0, 101.0, 102.0, 103.0])
    assert np.allclose(raw_close / f, [100.0, 100.0, 101.0, 102.0, 103.0])


def test_factor_overlapping_actions_same_day_multiply():
    # bonus 1:1 and FV split 10->5 with the same ex-date are two actions: x2 x2
    acts = _acts(("2024-01-04", "BONUS", 2.0), ("2024-01-04", "SPLIT", 2.0), ("2024-01-02", "BONUS", 1.5))
    assert compute_adj_factor(DATES, acts).tolist() == [6.0, 4.0, 4.0, 1.0, 1.0]


def test_factor_ignores_non_adjusting_and_bad_rows():
    acts = _acts(
        ("2024-01-03", "DIVIDEND", np.nan), ("2024-01-03", "SCHEME", np.nan),  # demerger: not applied
        ("2024-01-03", "RIGHTS", 1.2), ("2024-01-03", "SPLIT_UNPARSED", np.nan),
        ("2024-01-03", "SPLIT", np.nan), ("2024-01-03", "BONUS", 0.0),
    )
    assert compute_adj_factor(DATES, acts).tolist() == [1.0] * 5
    assert compute_adj_factor(DATES, None).tolist() == [1.0] * 5
    assert compute_adj_factor(DATES, acts.iloc[0:0]).tolist() == [1.0] * 5


def test_factor_actions_outside_history():
    acts = _acts(("2023-06-01", "SPLIT", 5.0), ("2025-01-01", "BONUS", 2.0))  # before / after all rows
    assert compute_adj_factor(DATES, acts).tolist() == [2.0] * 5


# ------------------------------------------------------------------ table build, manual overrides


def _resolver_with(symbols):
    from symbol_master import EntityResolver

    r = EntityResolver()
    day = pd.DataFrame({"symbol": symbols, "series": "EQ", "isin": [f"INE{i:03d}X01011" for i in range(len(symbols))]})
    r.resolve_day(day, date(2024, 1, 1))
    return r


def _api(rows):
    return pd.DataFrame(rows, columns=["symbol", "series", "ex_date", "rec_date", "purpose", "source"]).assign(
        ex_date=lambda d: pd.to_datetime(d["ex_date"]), rec_date=pd.NaT
    )


def test_build_actions_table_priority_and_manual_overrides(data_dir):
    import config

    config.MANUAL_OVERRIDES_FILE.write_text(
        "symbol,ex_date,type,factor,action,note\n"
        "AAA,2024-03-01,SPLIT,5.0,add,verified by hand\n"
        "BBB,2024-02-01,BONUS,2.0,ignore,never happened\n"
        "CCC,2024-04-01,BONUS,1.5,add,manual beats api\n"
    )
    r = _resolver_with(["AAA", "BBB", "CCC"])
    api = _api([
        ("AAA", "EQ", "2024-02-15", None, "Bonus 1:1", "api"),
        ("BBB", "EQ", "2024-02-01", None, "Bonus 1:1", "api"),
        ("CCC", "EQ", "2024-04-01", None, "Bonus 1:1", "api"),
    ])
    pr = _api([("AAA", "EQ", "2024-02-15", None, "BONUS 1:1", "pr"), ("ZZZ", "EQ", "2024-02-15", None, "BONUS 1:1", "pr")])
    t = adjuster.build_actions_table(r, [api, pr])
    assert list(t.columns) == adjuster.ACTION_COLUMNS + ["orig_ex_date"]
    by = {(k, str(e.date()), ty): row for (k, e, ty), row in zip(zip(t["key"], t["ex_date"], t["type"]), t.itertuples())}
    assert by[("AAA", "2024-02-15", "BONUS")].source == "api"  # api wins over pr
    assert by[("AAA", "2024-03-01", "SPLIT")].source == "manual" and by[("AAA", "2024-03-01", "SPLIT")].factor == 5.0
    assert ("BBB", "2024-02-01", "BONUS") not in by  # ignored
    ccc = by[("CCC", "2024-04-01", "BONUS")]
    assert ccc.source == "manual" and ccc.factor == 1.5
    zzz = t[t["symbol"] == "ZZZ"]
    assert len(zzz) == 1 and zzz["key"].isna().all()  # unknown symbol kept, unkeyed


def test_conflicting_factors_reported(data_dir):
    r = _resolver_with(["AAA"])
    api = _api([("AAA", "EQ", "2024-02-15", None, "Bonus 1:1", "api")])
    pr = _api([("AAA", "EQ", "2024-02-15", None, "BONUS 1:2", "pr")])
    t = adjuster.build_actions_table(r, [api, pr])
    assert t["factor"].tolist() == [2.0]
    import config

    assert (config.ACTIONS_DIR / "actions_conflicts.csv").exists()


def test_near_duplicates_dropped():
    df = pd.DataFrame(
        {
            "_ident": ["A", "A", "A"], "type": ["SPLIT", "BONUS", "BONUS"], "_prio": [1, 4, 1],
            "ex_date": pd.to_datetime(["2024-01-10", "2024-01-20", "2024-01-10"]), "factor": [2.0, 2.0, 2.0],
        }
    )
    out = adjuster._drop_near_duplicates(df)
    # same-day split + bonus both stay, the pr copy 10 days later is dropped
    assert sorted(out.index) == [0, 2]


def test_rekey_renamed():
    from symbol_master import EntityResolver

    r = EntityResolver()
    r.resolve_day(pd.DataFrame({"symbol": ["OLD"], "series": ["EQ"], "isin": ["INE1"]}), date(2024, 1, 1))
    r.resolve_day(pd.DataFrame({"symbol": ["NEW"], "series": ["EQ"], "isin": ["INE1"]}), date(2024, 1, 2))
    keys = pd.Series(["OLD", "NEW", None, "UNKNOWN"], dtype=object)
    out = adjuster.rekey_renamed(keys, r)
    assert out[[0, 1, 3]].tolist() == ["NEW", "NEW", "UNKNOWN"] and pd.isna(out[2])


def test_api_rows_to_frame():
    df = adjuster.api_rows_to_frame([
        {"symbol": "A ", "series": "EQ", "subject": "Bonus 1:1", "exDate": "05-Sep-2026", "recDate": "-"},
        {"symbol": "B", "subject": "Dividend", "exDate": "-"},
        {"symbol": "C", "subject": "x", "exDate": "2026-09-05"},  # unparsable -> skipped
    ])
    assert df["symbol"].tolist() == ["A"] and df["ex_date"].iloc[0] == pd.Timestamp("2026-09-05")
    assert pd.isna(df["rec_date"].iloc[0]) and df["source"].iloc[0] == "api"


def test_load_manual_overrides_format(data_dir):
    import config

    assert adjuster.load_manual_overrides().empty
    config.MANUAL_OVERRIDES_FILE.write_text(
        "Symbol , ex_date,type,factor,action,note\nabc ,2024-01-02,split,2,,n\nxyz,not-a-date,BONUS,2,add,n\n"
    )
    m = adjuster.load_manual_overrides()
    assert m["symbol"].tolist() == ["ABC"] and m["type"].tolist() == ["SPLIT"] and m["action"].tolist() == ["add"]
    assert m["factor"].tolist() == [2.0]


def test_repo_manual_overrides_file_parses():
    """The tracked data/actions/manual_overrides.csv keeps its documented format."""
    import config

    df = pd.read_csv(config.MANUAL_OVERRIDES_FILE, dtype=str)
    assert list(df.columns) == ["symbol", "ex_date", "type", "factor", "action", "note"]
    assert set(df["action"]) <= {"add", "ignore"}
    assert pd.to_datetime(df["ex_date"], errors="coerce").notna().all()


# ------------------------------------------------------------------ prices as arbiter


def _store_with(data_dir, key, dates, closes):
    from storage import Store

    s = Store()
    s.write(key, pd.DataFrame({"date": pd.to_datetime(dates), "symbol": key, "series": "EQ", "isin": "I",
                               "open": closes, "high": closes, "low": closes, "close": closes, "prev_close": closes,
                               "volume": 1, "turnover": 1.0}))
    return s


def test_reconcile_moves_ex_date_to_price_drop(data_dir):
    dates = pd.bdate_range("2024-01-01", periods=10)
    closes = [100, 101, 102, 103, 51.5, 52, 52.5, 53, 53.5, 54]  # halves on day 5 (2024-01-05)
    s = _store_with(data_dir, "AAA", dates, closes)
    acts = pd.DataFrame({"key": ["AAA"], "symbol": ["AAA"], "ex_date": [pd.Timestamp("2024-01-09")], "rec_date": pd.NaT,
                         "type": ["SPLIT"], "factor": [2.0], "amount": np.nan, "purpose": ["split"], "source": ["api"],
                         "hash": ["h"]})
    out = adjuster.reconcile_ex_dates(acts, s)
    assert out["ex_date"].iloc[0] == pd.Timestamp("2024-01-05")
    assert out["orig_ex_date"].iloc[0] == pd.Timestamp("2024-01-09")
    assert "by price check" in out["purpose"].iloc[0]


def test_fill_unparsed_from_prices(data_dir):
    dates = pd.bdate_range("2024-01-01", periods=6)
    s = _store_with(data_dir, "AAA", dates, [100, 100, 100, 20.2, 20.1, 20.3])  # 10 -> 2 split = x5
    acts = pd.DataFrame({"key": ["AAA"], "ex_date": [dates[3]], "type": ["SPLIT_UNPARSED"], "factor": [np.nan],
                         "purpose": ["FV SPLIT [TRUNCATED]"], "hash": ["h"]})
    out = adjuster.fill_unparsed_from_prices(acts, s)
    assert out["type"].iloc[0] == "SPLIT" and out["factor"].iloc[0] == 5.0


def test_apply_adjustments_and_verify(data_dir):
    dates = pd.bdate_range("2024-01-01", periods=6)
    s = _store_with(data_dir, "AAA", dates, [100, 100, 100, 50, 50, 50])
    good = pd.DataFrame({"key": ["AAA"], "ex_date": [dates[3]], "type": ["SPLIT"], "factor": [2.0], "source": ["api"],
                         "purpose": ["split"]})
    n, warn = adjuster.apply_adjustments(s, good)
    assert n == 1 and warn.empty
    assert s.read("AAA")["adj_factor"].tolist() == [2.0, 2.0, 2.0, 1.0, 1.0, 1.0]
    n2, _ = adjuster.apply_adjustments(s, good)
    assert n2 == 0  # unchanged factors are not rewritten
    wrong = good.assign(factor=5.0)
    _, warn = adjuster.apply_adjustments(s, wrong, keys=["AAA", "MISSING"])
    assert len(warn) == 1 and warn["issue"].iloc[0] == "adjusted close jump across ex-date"
