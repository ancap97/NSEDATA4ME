from __future__ import annotations

from datetime import date

import pandas as pd
import pytest

from symbol_master import EntityResolver, make_key


def day(*rows):
    return pd.DataFrame(rows, columns=["symbol", "series", "isin"])


def test_make_key():
    assert make_key("ABC", "EQ") == "ABC" and make_key("ABC", "SM") == "ABC_SME" and make_key("ABC", "ST") == "ABC_SME"


def test_rename_by_isin_and_history():
    r = EntityResolver()
    keys, ren = r.resolve_day(day(("INFOSYSTCH", "EQ", "INE009A01021")), date(2011, 6, 1))
    assert list(keys) == ["INFOSYSTCH"] and ren == []
    keys, ren = r.resolve_day(day(("INFY", "EQ", "INE009A01021")), date(2011, 6, 2))
    assert list(keys) == ["INFY"] and ren == [("INFOSYSTCH", "INFY")]
    e = r.entities["INFY"]
    assert [p.value for p in e.symbols] == ["INFOSYSTCH", "INFY"] and e.first_date == "2011-06-01"
    assert r.key_for_symbol("INFOSYSTCH") == "INFY"
    assert r.key_for_symbol("infosystch", on=date(2011, 6, 1)) == "INFY"
    assert r.key_for_isin("INE009A01021") == "INFY"
    assert r.key_for_symbol("NOPE") is None


def test_pre_isin_rows_join_isin_era_by_symbol():
    r = EntityResolver()
    r.resolve_day(day(("ABC", "EQ", None)), date(2010, 1, 4))
    keys, _ = r.resolve_day(day(("ABC", "EQ", "INE000A01011")), date(2011, 6, 22))
    assert list(keys) == ["ABC"] and len(r.entities) == 1 and r.entities["ABC"].isin == "INE000A01011"


def test_face_value_change_new_isin_same_entity():
    r = EntityResolver()
    r.resolve_day(day(("ABC", "EQ", "INE000A01011")), date(2020, 1, 1))
    r.resolve_day(day(("ABC", "EQ", "INE000A01029")), date(2020, 1, 2))
    e = r.entities["ABC"]
    assert [p.value for p in e.isins] == ["INE000A01011", "INE000A01029"]
    assert r.master_frame().loc[0, "previous_isins"] == "INE000A01011"


def test_sme_migration_keeps_history():
    r = EntityResolver()
    r.resolve_day(day(("XYZ", "SM", "INE111A01011")), date(2023, 1, 2))
    keys, ren = r.resolve_day(day(("XYZ", "EQ", "INE111A01011")), date(2023, 6, 1))
    assert ren == [("XYZ_SME", "XYZ")] and list(keys) == ["XYZ"] and not r.entities["XYZ"].is_sme
    assert "sme->main" in r.entities["XYZ"].notes[-1]


def test_rename_collision_keeps_separate_entities(data_dir):
    r = EntityResolver()
    r.resolve_day(day(("OLD", "EQ", "INE1"), ("TAKEN", "EQ", "INE2")), date(2020, 1, 1))
    keys, ren = r.resolve_day(day(("TAKEN", "EQ", "INE1")), date(2020, 1, 2))  # INE1 now trades as TAKEN
    assert ren == [] and list(keys) == ["OLD"]
    assert set(r.entities) == {"OLD", "TAKEN"}
    import symbol_master

    assert symbol_master.IDENTITY_REVIEW_FILE.exists()


def test_long_gap_new_isin_is_flagged(data_dir):
    import symbol_master

    r = EntityResolver()
    r.resolve_day(day(("ABC", "EQ", "INE1")), date(2010, 1, 1))
    r.resolve_day(day(("ABC", "EQ", "INE2")), date(2012, 1, 1))
    assert "possible symbol reuse" in symbol_master.IDENTITY_REVIEW_FILE.read_text()


def test_master_frame_status_and_persistence(data_dir):
    import config
    import symbol_master

    r = EntityResolver()
    r.resolve_day(day(("A", "EQ", "I1"), ("B", "EQ", "I2"), ("C", "SM", "I3")), date(2024, 1, 1))
    r.resolve_day(day(("A", "EQ", "I1"), ("C", "SM", "I3")), date(2024, 3, 1))
    m = r.save_outputs(as_of=date(2024, 3, 1)).set_index("key")
    assert m.loc["A", "status"] == "active" and m.loc["B", "status"] == "inactive" and bool(m.loc["C_SME", "is_sme"])
    assert list(m.columns) == ["symbol", "series", "isin", "is_sme", "first_date", "last_date", "status",
                               "previous_symbols", "previous_isins", "notes"]
    pd.testing.assert_frame_equal(symbol_master.load_master().set_index("key"), m)
    r2 = EntityResolver.load()
    assert r2.isin2key == r.isin2key and r2.pair2key == r.pair2key
    assert r2.entities["A"].last_date == "2024-03-01"
    assert config.SYMBOL_HISTORY_FILE.exists()


def test_resolving_an_older_day_again_keeps_latest_state():
    """--redo-date re-resolves a past day: last_date, series and symbol periods must not move back."""
    r = EntityResolver()
    r.resolve_day(day(("OLD", "EQ", "INE1"), ("B", "EQ", "INE2")), date(2024, 1, 1))
    r.resolve_day(day(("NEW", "BE", "INE1"), ("B", "EQ", "INE2")), date(2024, 1, 3))  # rename + series change
    keys, ren = r.resolve_day(day(("OLD", "EQ", "INE1"), ("B", "EQ", "INE2")), date(2024, 1, 1))
    assert list(keys) == ["NEW", "B"] and ren == []
    e = r.entities["NEW"]
    assert (e.last_date, e.series, e.symbol) == ("2024-01-03", "BE", "NEW")
    assert [(p.value, p.from_date, p.to_date) for p in e.symbols] == [
        ("OLD", "2024-01-01", "2024-01-01"), ("NEW", "2024-01-03", "2024-01-03")]
    assert [(p.from_date, p.to_date) for p in e.isins] == [("2024-01-01", "2024-01-03")]
    assert r.entities["B"].last_date == "2024-01-03"


def test_master_write_failure_keeps_previous_files(data_dir, monkeypatch):
    """A crash while writing symbol_master.parquet / isin_symbol_map.json must leave the previous
    files readable (every loader call reads the master)."""
    from pathlib import Path

    import config
    import symbol_master

    r = EntityResolver()
    r.resolve_day(day(("A", "EQ", "I1")), date(2024, 1, 1))
    r.save_outputs()
    before = symbol_master.load_master()
    history = config.SYMBOL_HISTORY_FILE.read_text()
    r.resolve_day(day(("B", "EQ", "I2")), date(2024, 1, 2))

    real_parquet, real_text = pd.DataFrame.to_parquet, Path.write_text

    def torn_parquet(self, path, *a, **kw):
        Path(path).write_bytes(b"PAR1 torn")
        raise OSError("killed mid-write")

    monkeypatch.setattr(pd.DataFrame, "to_parquet", torn_parquet)
    with pytest.raises(OSError):
        r.save_outputs()
    monkeypatch.setattr(pd.DataFrame, "to_parquet", real_parquet)
    pd.testing.assert_frame_equal(symbol_master.load_master(), before)

    def torn_text(self, data, *a, **kw):
        real_text(self, data[:10], *a, **kw)
        raise OSError("killed mid-write")

    monkeypatch.setattr(Path, "write_text", torn_text)
    with pytest.raises(OSError):
        r.save_outputs()
    monkeypatch.setattr(Path, "write_text", real_text)
    assert config.SYMBOL_HISTORY_FILE.read_text() == history
