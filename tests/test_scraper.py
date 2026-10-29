"""NSEClient with a scripted fake session: no network, no real sleeping."""

from __future__ import annotations

import json
from datetime import date, datetime, timedelta

import pytest
import requests

import scraper
from config import TZ_IN
from tests.fakes import FakeResponse, FakeSession, zip_bytes


@pytest.fixture
def sleeps(monkeypatch):
    calls = []
    monkeypatch.setattr(scraper.time, "sleep", lambda s: calls.append(s))
    monkeypatch.setattr(scraper.random, "uniform", lambda a, b: 0.0)
    return calls


@pytest.fixture
def client(data_dir, sleeps):
    c = scraper.NSEClient(min_interval=0.0)
    c._cookies_at = float("inf")  # cookies "fresh": no bootstrap request unless forced
    return c


def script(c, *items):
    c.session = FakeSession(items)
    return c.session


OLD_DAY = date(2024, 1, 2)


# ------------------------------------------------------------------ urls / paths


def test_report_urls_switch_formats():
    assert scraper.report_url("bhav", date(2024, 7, 5)).endswith("/historical/EQUITIES/2024/JUL/cm05JUL2024bhav.csv.zip")
    assert scraper.report_url("bhav", date(2024, 7, 8)).endswith("/cm/BhavCopy_NSE_CM_0_0_0_20240708_F_0000.csv.zip")
    assert scraper.report_url("delivery", OLD_DAY).endswith("sec_bhavdata_full_02012024.csv")
    assert scraper.report_url("mto", OLD_DAY).endswith("MTO_02012024.DAT")
    assert scraper.report_url("pr", OLD_DAY).endswith("PR020124.zip")
    with pytest.raises(ValueError):
        scraper.report_url("nope", OLD_DAY)


def test_raw_path_layout(data_dir):
    p = scraper.raw_path("bhav", date(2024, 7, 8))
    assert p == data_dir / "raw" / "bhav" / "2024" / "BhavCopy_NSE_CM_0_0_0_20240708_F_0000.csv"


# ------------------------------------------------------------------ retries


def test_get_retries_then_succeeds(client, sleeps):
    ok = FakeResponse(200, b"x")
    s = script(client, requests.ConnectionError("reset"), FakeResponse(503), ok)
    assert client._get("https://x/a") is ok
    assert len(s.calls) == 3
    assert 2.0 in sleeps and 4 in sleeps  # backoff after the exception and after the 503


def test_get_403_refreshes_cookies(client, sleeps):
    ok = FakeResponse(200, b"x")
    s = script(client, FakeResponse(403), FakeResponse(200), ok)  # 2nd response is the cookie bootstrap
    assert client._get("https://x/a") is ok
    assert s.calls[1].endswith("/option-chain")


def test_get_gives_up_after_max_retries(client):
    s = script(client, *[FakeResponse(500) for _ in range(scraper.MAX_RETRIES)])
    with pytest.raises(ConnectionError, match="giving up"):
        client._get("https://x/a")
    assert len(s.calls) == scraper.MAX_RETRIES


def test_get_404_and_other_statuses(client):
    script(client, FakeResponse(404))
    with pytest.raises(scraper.ReportUnavailable):
        client._get("https://x/a")
    script(client, FakeResponse(418))
    with pytest.raises(ConnectionError, match="HTTP 418"):
        client._get("https://x/a")


def test_throttle_waits_min_interval(data_dir, sleeps, monkeypatch):
    c = scraper.NSEClient(min_interval=1.2)
    t = iter([100.0, 100.0, 100.5, 101.7])
    monkeypatch.setattr(scraper.time, "monotonic", lambda: next(t))
    c._last_request = 0.0
    c._throttle()  # long since the last request: no wait
    c._throttle()  # 0.5 s after the previous one: wait the remaining 0.7 s
    assert sleeps == [pytest.approx(0.7)]


# ------------------------------------------------------------------ download_report


def test_download_zip_extracts_csv(client):
    script(client, FakeResponse(200, zip_bytes("cm02JAN2024bhav.csv", "SYMBOL\nX\n"), "application/zip"))
    p = client.download_report("bhav", OLD_DAY)
    assert p == scraper.raw_path("bhav", OLD_DAY) and p.read_text() == "SYMBOL\nX\n"
    assert not list(p.parent.glob("*.part"))
    assert client.stats["downloaded"] == 1
    # already on disk: no request
    s = script(client)
    assert client.download_report("bhav", OLD_DAY) == p and s.calls == []
    assert client.stats["skipped"] == 1


def test_download_pr_zip_kept_as_zip(client):
    body = zip_bytes("bc02012024.csv", "SERIES\n")
    script(client, FakeResponse(200, body, "application/zip"))
    p = client.download_report("pr", OLD_DAY)
    assert p.suffix == ".zip" and p.read_bytes() == body


def test_download_plain_file(client):
    script(client, FakeResponse(200, b"Index Name\n"))
    p = client.download_report("indices", OLD_DAY)
    assert p.read_bytes() == b"Index Name\n"


def test_old_404_recorded_as_missing_and_not_requested_again(client):
    script(client, FakeResponse(404))
    assert client.download_report("bhav", OLD_DAY) is None
    assert client.missing.is_missing("bhav", OLD_DAY)
    stored = json.loads(scraper.MISSING_DATES_FILE.read_text())
    assert stored["bhav"] == [OLD_DAY.isoformat()]
    s = script(client)
    assert client.download_report("bhav", OLD_DAY) is None and s.calls == []
    # force ignores the missing record
    script(client, FakeResponse(200, b"data"))
    assert client.download_report("indices", OLD_DAY, force=True) is not None


def test_recent_404_is_pending_not_missing(client):
    recent = datetime.now(TZ_IN).date() - timedelta(days=1)
    script(client, FakeResponse(404))
    assert client.download_report("bhav", recent) is None
    assert not client.missing.is_missing("bhav", recent)
    script(client, FakeResponse(200, b"<html>", "text/html; charset=utf-8"))  # NSE error page
    assert client.download_report("indices", recent) is None
    assert not client.missing.is_missing("indices", recent)


def test_network_failure_logged_and_raised(client):
    script(client, *[requests.Timeout("t") for _ in range(scraper.MAX_RETRIES)])
    with pytest.raises(ConnectionError):
        client.download_report("bhav", OLD_DAY)
    assert "bhav" in scraper.FAILED_DATES_FILE.read_text()
    assert not scraper.raw_path("bhav", OLD_DAY).exists()
    assert client.stats["failed"] == 1


def test_empty_download_raises(client):
    script(client, FakeResponse(200, b""))
    with pytest.raises(ConnectionError, match="empty download"):
        client.download_report("indices", OLD_DAY)
    assert not scraper.raw_path("indices", OLD_DAY).exists()


def test_download_delivery_fallbacks(client):
    d = date(2024, 1, 2)
    script(client, FakeResponse(404), FakeResponse(200, b"20,1,X,EQ,1,1,100\n"))
    p = client.download_delivery(d)
    assert p == scraper.raw_path("mto", d)
    pre = date(2018, 12, 31)  # before sec_bhavdata_full exists: MTO only
    s = script(client, FakeResponse(200, b"20,1,X,EQ,1,1,100\n"))
    assert client.download_delivery(pre) == scraper.raw_path("mto", pre)
    assert len(s.calls) == 1 and "MTO_" in s.calls[0]


# ------------------------------------------------------------------ API helpers


def test_api_endpoints(client):
    script(client, FakeResponse(200, js={"CM": [{"tradingDate": "26-Jan-2026", "description": "Republic Day"}]}))
    assert client.holidays() == {"26-Jan-2026": "Republic Day"}
    script(client, FakeResponse(200, js={"data": [{"symbol": "A"}]}))
    assert client.actions("equities", OLD_DAY, OLD_DAY) == [{"symbol": "A"}]
    script(client, FakeResponse(200, js=[{"symbol": "B"}]))
    assert client.actions("equities", OLD_DAY, OLD_DAY, symbol="B") == [{"symbol": "B"}]
    script(client, FakeResponse(200, js={"data": [{"sub": "x"}]}))
    assert client.circulars("CMTR", OLD_DAY, OLD_DAY) == [{"sub": "x"}]
    script(client, FakeResponse(200))
    with pytest.raises(ConnectionError, match="non-JSON"):
        client.api("holiday-master")


def test_missing_dates_tolerates_corrupt_file(data_dir):
    scraper.MISSING_DATES_FILE.parent.mkdir(parents=True, exist_ok=True)
    scraper.MISSING_DATES_FILE.write_text("{broken")
    m = scraper.MissingDates()
    assert not m.is_missing("bhav", OLD_DAY)
    m.add("bhav", OLD_DAY)
    m.remove("bhav", OLD_DAY)
    assert json.loads(scraper.MISSING_DATES_FILE.read_text())["bhav"] == []


def test_failed_extraction_keeps_previous_file(client):
    """A forced re-download (--redo-date) that delivers a corrupt zip must not truncate the
    good bhavcopy already on disk, and must not leave a partial file that later runs trust."""
    import zipfile

    p = scraper.raw_path("bhav", OLD_DAY)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text("good")
    body = bytearray(zip_bytes("cm02JAN2024bhav.csv", "SYMBOL\n" + "X\n" * 100))
    i = body.index(b"X\nX\n")
    body[i] = ord("Y")  # corrupt the stored data -> CRC mismatch on extraction
    script(client, FakeResponse(200, bytes(body), "application/zip"))
    with pytest.raises(zipfile.BadZipFile):
        client.download_report("bhav", OLD_DAY, force=True)
    assert p.read_text() == "good"
    assert [x.name for x in p.parent.iterdir()] == [p.name]


def test_missing_dates_write_is_atomic(data_dir, monkeypatch):
    from pathlib import Path

    m = scraper.MissingDates()
    m.add("bhav", OLD_DAY)
    real = Path.write_text

    def torn(self, data, *a, **kw):
        real(self, data[:5], *a, **kw)
        raise OSError("killed mid-write")

    monkeypatch.setattr(Path, "write_text", torn)
    with pytest.raises(OSError):
        m.add("bhav", date(2024, 1, 3))
    monkeypatch.setattr(Path, "write_text", real)
    assert json.loads(scraper.MISSING_DATES_FILE.read_text())["bhav"] == [OLD_DAY.isoformat()]


def test_retried_responses_are_closed(client):
    """Streamed responses that are retried must be closed so the connection returns to the pool."""
    bad = [FakeResponse(503), FakeResponse(429), FakeResponse(403)]
    script(client, bad[0], bad[1], bad[2], FakeResponse(200), FakeResponse(200, b"x"))  # 4th: cookie refresh
    client._get("https://x/a")
    assert [r.closed for r in bad] == [True, True, True]


def test_give_up_message_names_last_status(client):
    script(client, *[FakeResponse(403) if i % 2 == 0 else FakeResponse(200) for i in range(2 * scraper.MAX_RETRIES)])
    with pytest.raises(ConnectionError, match="HTTP 403"):
        client._get("https://x/a")
