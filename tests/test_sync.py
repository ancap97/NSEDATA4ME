"""sync.py flow against a fake NSE: candidate dates, holidays, cutoff, --force, --redo-date."""

from __future__ import annotations

import json
from datetime import date, datetime

import pandas as pd
import pytest

import pipeline
import scraper
import sync
from tests import factory
from tests.fakes import FakeNSE, frozen_datetime, ist, publish

D = factory.DAYS  # weekdays 2024-01-01 .. 2024-02-09
BASE = 20  # the database starts synced through D[19] = 2024-01-26 (Fri)


# ------------------------------------------------------------------ calendar helpers


def test_candidate_dates_weekends_holidays_special_sessions():
    hol = {"26-Jan-2024": "Republic Day", "01-Nov-2024": "Diwali Laxmi Pujan*"}
    got = pipeline.candidate_dates(date(2024, 1, 24), date(2024, 1, 31), hol, ["2024-01-27"])
    assert got == [date(2024, 1, 25), date(2024, 1, 27), date(2024, 1, 29), date(2024, 1, 30), date(2024, 1, 31)]
    # Muhurat session on a weekday holiday is a trading day
    assert pipeline.is_candidate_trading_day(date(2024, 11, 1), hol, [])
    assert pipeline.candidate_dates(date(2024, 1, 31), date(2024, 1, 31), hol, []) == []


def test_muhurat_session_on_a_weekend_is_a_candidate():
    # NSE's 2026 list: "08-Nov-2026 Diwali Laxmi Pujan*" is a Sunday with a Muhurat session
    # (the store has earlier weekend ones, e.g. 2023-11-12 and 2016-10-30)
    hol = {"08-Nov-2026": "Diwali Laxmi Pujan*", "10-Nov-2026": "Diwali-Balipratipada"}
    assert pipeline.is_candidate_trading_day(date(2026, 11, 8), hol, [])
    got = pipeline.candidate_dates(date(2026, 11, 6), date(2026, 11, 11), hol, [])
    assert got == [date(2026, 11, 8), date(2026, 11, 9), date(2026, 11, 11)]
    assert not pipeline.is_candidate_trading_day(date(2026, 11, 7), hol, [])  # plain Saturday


@pytest.mark.parametrize(
    "now, expected",
    [
        (ist(2024, 1, 29, 17, 59), date(2024, 1, 28)),
        (ist(2024, 1, 29, 18, 0), date(2024, 1, 29)),
        (ist(2024, 1, 29, 23, 59), date(2024, 1, 29)),
        (ist(2024, 1, 30, 0, 30), date(2024, 1, 29)),
    ],
)
def test_eod_published_cutoff_ist(now, expected):
    from datetime import timezone

    from config import TZ_IN

    assert pipeline.eod_published_cutoff(now) == expected
    # the same instant seen from UTC gives the same answer once converted to IST
    assert pipeline.eod_published_cutoff(now.astimezone(timezone.utc).astimezone(TZ_IN)) == expected


# ------------------------------------------------------------------ end-to-end sync


@pytest.fixture
def nse(data_dir, monkeypatch):
    """A database synced through D[BASE-1] plus an empty fake NSE; returns a runner."""
    factory.build_db(D[:BASE])
    FakeNSE.server = {}
    FakeNSE.requested = []
    FakeNSE.api_rows = factory.API_ACTIONS
    FakeNSE.holiday_list = {}
    monkeypatch.setattr(sync, "NSEClient", FakeNSE)

    def run(now: datetime, **kw):
        clock = frozen_datetime(now)
        monkeypatch.setattr(sync, "datetime", clock)
        monkeypatch.setattr(scraper, "datetime", clock)
        return sync.run_sync(progress=lambda *a: None, **kw)

    return run


def meta():
    import storage

    return storage.load_meta()


def store_dates(key):
    from storage import Store

    return list(Store().read(key)["date"].dt.date)


def test_sync_ingests_new_days_infers_holiday_and_leaves_today_pending(nse):
    # 2024-01-30 is never published (closure), 2024-02-05 (today) is not out yet
    for d in (date(2024, 1, 29), date(2024, 1, 31), date(2024, 2, 1), date(2024, 2, 2)):
        publish(d)
    s = nse(ist(2024, 2, 5, 19, 0))
    assert s["ingested"] == ["2024-01-29", "2024-01-31", "2024-02-01", "2024-02-02"]
    assert s["missing"] == ["2024-01-30"] and s["pending"] == ["2024-02-05"]
    m = meta()
    assert m["last_synced"] == "2024-02-02" and m["in_progress"] is None
    assert m["holidays"]["30-Jan-2024"].startswith("inferred")
    assert m["breadth_last_date"] == "2024-02-02"
    assert scraper.MissingDates().is_missing("bhav", date(2024, 1, 30))  # 6 days old: recorded
    assert not scraper.MissingDates().is_missing("bhav", date(2024, 2, 5))  # today: retried next run
    assert store_dates("AAA")[-4:] == [date(2024, 1, 29), date(2024, 1, 31), date(2024, 2, 1), date(2024, 2, 2)]
    # the bonus ex 2024-02-01 is applied to the rows ingested in this run as well
    import loader

    bbb = loader.load_symbol("BBB", adjusted=False)
    assert bbb.loc[: "2024-01-31", "adj_factor"].eq(2.0).all() and bbb.loc["2024-02-01":, "adj_factor"].eq(1.0).all()
    # a new main-board DUP appeared on 2024-01-29 next to the delisted SME DUP
    assert "DUP" in set(loader.list_symbols()["key"])

    # next run: nothing new published -> nothing requested again for the inferred holiday
    FakeNSE.requested = []
    s2 = nse(ist(2024, 2, 5, 20, 0))
    assert s2["ingested"] == [] and s2["pending"] == ["2024-02-05"]
    assert not any("30012024" in u or "30JAN2024" in u for u in FakeNSE.requested)


def test_before_cutoff_today_is_not_attempted_unless_forced(nse):
    publish(date(2024, 1, 29))
    s = nse(ist(2024, 1, 29, 17, 30))
    assert s["ingested"] == [] and meta()["last_synced"] == "2024-01-26"
    assert FakeNSE.requested == []
    s = nse(ist(2024, 1, 29, 17, 30), force=True)
    assert s["ingested"] == ["2024-01-29"] and meta()["last_synced"] == "2024-01-29"


def test_holiday_from_nse_list_is_skipped(nse):
    FakeNSE.holiday_list = {"29-Jan-2024": "Test holiday"}
    publish(date(2024, 1, 30))
    s = nse(ist(2024, 1, 30, 19, 0))
    assert s["ingested"] == ["2024-01-30"] and s["missing"] == []
    assert not any("29012024" in u or "29JAN2024" in u for u in FakeNSE.requested)


def test_delivery_backfilled_on_a_later_run(nse):
    d = date(2024, 1, 29)
    publish(d, delivery=False)
    nse(ist(2024, 1, 29, 19, 0))
    assert meta()["pending_delivery"] == ["2024-01-29"]
    from storage import Store

    assert pd.isna(Store().read("AAA").set_index("date").loc["2024-01-29", "deliv_qty"])
    publish(d)  # delivery report appears later
    nse(ist(2024, 1, 30, 10, 0))
    assert meta()["pending_delivery"] == []
    assert Store().read("AAA").set_index("date").loc["2024-01-29", "deliv_qty"] == 800


def test_redo_date_reingests_one_day(nse):
    redo = D[10]  # 2024-01-15
    def bump(rows):
        return [dict(r, close=r["close"] + 1) if r["symbol"] == "AAA" else r for r in rows]

    from storage import Store

    before = Store().read("AAA").set_index("date")["close"]
    publish(redo, mutate=bump)
    s = nse(ist(2024, 1, 29, 10, 0), redo=redo)
    assert s["ingested"] == [redo.isoformat()]
    after = Store().read("AAA").set_index("date")["close"]
    assert after[pd.Timestamp(redo)] == before[pd.Timestamp(redo)] + 1
    assert after.drop(pd.Timestamp(redo)).equals(before.drop(pd.Timestamp(redo)))
    assert meta()["last_synced"] == "2024-01-26"
    with pytest.raises(SystemExit, match="after the last synced date"):
        nse(ist(2024, 1, 29, 10, 0), redo=date(2024, 1, 29))


def test_redo_date_keeps_symbol_master_current(nse):
    import loader

    cols = ["last_date", "status", "series", "previous_symbols"]
    before = loader.list_symbols(include_sme=True).set_index("key")[cols]
    publish(D[10])
    nse(ist(2024, 1, 29, 10, 0), redo=D[10])
    after = loader.list_symbols(include_sme=True).set_index("key")[cols]
    pd.testing.assert_frame_equal(after, before)


def test_redo_date_without_bhavcopy_exits(nse):
    with pytest.raises(SystemExit, match="no bhavcopy"):
        nse(ist(2024, 1, 29, 10, 0), redo=D[5])


def test_interrupted_day_is_rolled_back(nse):
    import storage
    from storage import Store

    partial = factory.day_rows(D[BASE])[0]  # AAA row of 2024-01-29 written before a crash
    Store().append("AAA", pd.DataFrame([{**partial, "date": pd.Timestamp(D[BASE])}]))
    m = meta()
    m["in_progress"] = {"date": D[BASE].isoformat(), "keys": ["AAA"]}
    storage.save_meta(m)
    s = nse(ist(2024, 1, 29, 19, 0))  # nothing published: day stays pending
    assert s["pending"] == ["2024-01-29"]
    assert D[BASE] not in store_dates("AAA") and meta()["in_progress"] is None


def test_resolver_saved_before_day_is_marked_committed(nse, monkeypatch):
    """A crash right after meta.json records a day as committed must not leave the identity
    state (symbol_state.json) a day behind the store."""
    publish(date(2024, 1, 29))
    real = sync.save_meta

    def crash_after_commit(m, *a, **kw):
        real(m, *a, **kw)
        if m.get("in_progress") is None and m.get("last_synced") == "2024-01-29":
            raise KeyboardInterrupt  # process killed here

    monkeypatch.setattr(sync, "save_meta", crash_after_commit)
    with pytest.raises(KeyboardInterrupt):
        nse(ist(2024, 1, 29, 19, 0))
    from symbol_master import EntityResolver

    assert meta()["last_synced"] == "2024-01-29"
    r = EntityResolver.load()
    assert r.entities["AAA"].last_date == "2024-01-29" and "DUP" in r.entities


def test_database_missing_exits(data_dir, monkeypatch):
    monkeypatch.setattr(sync, "NSEClient", FakeNSE)
    with pytest.raises(SystemExit, match="database not found"):
        sync.run_sync(progress=lambda *a: None)


# ------------------------------------------------------------------ CLI


@pytest.fixture
def keep_parallel_flag(monkeypatch):
    import storage

    monkeypatch.setattr(storage, "_PARALLEL_ALLOWED", False)  # restored after the test
    return storage


def test_main_exit_codes_and_status_file(data_dir, monkeypatch, keep_parallel_flag):
    calls = {}

    def ok(**kw):
        calls.update(kw)
        return {"ingested": ["2024-01-29"]}

    monkeypatch.setattr(sync, "run_sync", ok)
    monkeypatch.setattr(sync, "ensure_data", lambda: pytest.fail("--no-fetch must skip the snapshot fetch"))
    sync.main(["--no-fetch", "--force", "--no-breadth", "--redo-date", "2024-01-15"])
    assert calls["force"] is True and calls["do_breadth"] is False and calls["redo"] == date(2024, 1, 15)
    st = json.loads(sync.SYNC_STATUS_FILE.read_text())
    assert st["ok"] is True and st["summary"] == {"ingested": ["2024-01-29"]}
    assert keep_parallel_flag._PARALLEL_ALLOWED  # the command may rewrite files in worker processes

    def boom(**kw):
        raise ConnectionError("NSE down")

    monkeypatch.setattr(sync, "run_sync", boom)
    with pytest.raises(SystemExit) as e:
        sync.main(["--no-fetch"])
    assert e.value.code == 1
    st = json.loads(sync.SYNC_STATUS_FILE.read_text())
    assert st["ok"] is False and "NSE down" in st["error"]


def test_main_fetches_snapshot_by_default(data_dir, monkeypatch, keep_parallel_flag):
    seen = []
    monkeypatch.setattr(sync, "ensure_data", lambda: seen.append(True))
    monkeypatch.setattr(sync, "run_sync", lambda **kw: {})
    sync.main([])
    assert seen == [True]


def test_changed_action_keys():
    a = pd.DataFrame({"key": ["A", "B"], "type": ["SPLIT", "BONUS"], "ex_date": pd.to_datetime(["2024-01-01"] * 2),
                      "factor": [2.0, 2.0]})
    b = a.assign(factor=[2.0, 1.5])
    assert sync.changed_action_keys(a, b) == {"B"}
    assert sync.changed_action_keys(a, a) == set()
