from __future__ import annotations

import json

import pytest

import healthcheck
from tests import factory
from tests.fakes import frozen_datetime, ist


@pytest.fixture
def healthy_db(data_dir, monkeypatch):
    factory.build_db()  # synced through 2024-02-09 (Fri)
    from storage import Store

    Store().write("RELIANCE", Store().read("AAA"))  # healthcheck probes RELIANCE for the store's last date
    healthcheck.STATUS_FILE.write_text(json.dumps({"ok": True, "finished_at": "2024-02-09T19:30:00+05:30"}))
    monkeypatch.setattr(healthcheck, "datetime", frozen_datetime(ist(2024, 2, 10, 12, 0)))
    return data_dir


def test_healthy_exit_0(healthy_db, capsys):
    assert healthcheck.main([]) == 0
    assert "STATUS: HEALTHY" in capsys.readouterr().out
    assert healthcheck.main(["--json"]) == 0
    info = json.loads(capsys.readouterr().out)
    assert info["ok"] is True and info["trading_days_behind"] == 0 and info["last_synced"] == "2024-02-09"


def test_stale_and_failed_run_exit_1(healthy_db, monkeypatch, capsys):
    monkeypatch.setattr(healthcheck, "datetime", frozen_datetime(ist(2024, 2, 16, 19, 0)))
    healthcheck.STATUS_FILE.write_text(json.dumps({"ok": False, "finished_at": "x", "error": "boom"}))
    assert healthcheck.main([]) == 1
    out = capsys.readouterr().out
    assert "5 trading days behind" in out and "last sync FAILED" in out and "NEEDS ATTENTION" in out


def test_no_database(data_dir):
    info = healthcheck.collect()
    assert not info.get("ok") and "database not built" in info["problems"][0]
