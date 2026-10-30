"""Test doubles for NSE: scripted HTTP responses, an in-memory report server, a frozen clock."""

from __future__ import annotations

import io
import zipfile
from datetime import date, datetime
from typing import ClassVar

import scraper
from config import TZ_IN


class FakeResponse:
    def __init__(self, status=200, body=b"", ctype="text/csv", js=None):
        self.status_code = status
        self.headers = {"content-type": ctype}
        self._body = body
        self._json = js
        self.closed = False

    def iter_content(self, chunk_size=1):
        for i in range(0, len(self._body), chunk_size):
            yield self._body[i : i + chunk_size]

    def json(self):
        if self._json is None:
            raise ValueError("no json")
        return self._json

    def close(self):
        self.closed = True


class FakeSession:
    """Returns the scripted responses in order; an Exception instance is raised instead."""

    def __init__(self, script):
        self.script = list(script)
        self.calls: list[str] = []
        self.headers: dict[str, str] = {}

    def get(self, url, params=None, timeout=None, stream=False):
        self.calls.append(url)
        item = self.script.pop(0)
        if isinstance(item, Exception):
            raise item
        return item

    def close(self):
        pass


def zip_bytes(name: str, data) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr(name, data)
    return buf.getvalue()


class FakeNSE(scraper.NSEClient):
    """The real NSEClient download/missing-date logic on top of an in-memory NSE.

    `server` maps report URLs to bytes (anything absent is a 404); `api_rows` is returned by
    the corporate-actions endpoint for the equities segment."""

    server: ClassVar[dict[str, bytes]] = {}
    api_rows: ClassVar[list[dict]] = []
    holiday_list: ClassVar[dict[str, str]] = {}
    requested: ClassVar[list[str]] = []

    def __init__(self, *a, **kw):
        super().__init__(min_interval=0.0)

    def _get(self, url, params=None, stream=False):
        FakeNSE.requested.append(url)
        if url not in self.server:
            raise scraper.ReportUnavailable(url)
        return FakeResponse(200, self.server[url], "application/zip" if url.endswith(".zip") else "text/csv")

    def api(self, endpoint, params=None):
        if endpoint == "holiday-master":
            return {"CM": [{"tradingDate": k, "description": v} for k, v in self.holiday_list.items()]}
        if endpoint == "circulars":
            return {"data": []}
        if endpoint == "corporates-corporateActions":
            return {"data": self.api_rows if params["index"] == "equities" else []}
        raise AssertionError(endpoint)


def publish(d: date, delivery: bool = True, indices: bool = True, mutate=None) -> None:
    """Put one synthetic trading day on the fake NSE server (raw files are not left on disk)."""
    from tests import factory

    kinds = ("bhav", "delivery", "indices")
    # the factory writes into data/raw; keep whatever a sync already downloaded there
    saved = {k: scraper.raw_path(k, d).read_bytes() for k in kinds if scraper.raw_path(k, d).exists()}
    for k in saved:
        scraper.raw_path(k, d).unlink()
    rows = factory.day_rows(d)
    if mutate:
        rows = mutate(rows)
    factory.write_bhav(d, rows)
    if delivery:
        factory.write_sec_full(d, rows)
    if indices:
        factory.write_indices(d, factory.index_rows(d))
    for kind in kinds:
        p = scraper.raw_path(kind, d)
        if p.exists():
            body = p.read_bytes()
            p.unlink()
            url = scraper.report_url(kind, d)
            FakeNSE.server[url] = zip_bytes(p.name, body) if url.endswith(".zip") else body
        if kind in saved:
            p.write_bytes(saved[kind])


def ist(*args) -> datetime:
    """An IST wall-clock datetime."""
    return datetime(*args, tzinfo=TZ_IN)


def frozen_datetime(at: datetime):
    """A datetime subclass whose now() returns `at` (an IST wall-clock time)."""
    at = at.replace(tzinfo=TZ_IN) if at.tzinfo is None else at

    class Frozen(datetime):
        @classmethod
        def now(cls, tz=None):
            return at.astimezone(tz) if tz else at.replace(tzinfo=None)

    return Frozen
