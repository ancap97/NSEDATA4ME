"""Shared fixtures. The whole suite runs offline against a temporary data directory."""

from __future__ import annotations

from pathlib import Path

import pytest
import requests

from tests._repoint import repoint

FIXTURES = Path(__file__).with_name("fixtures")


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    """Any real HTTP request is a test bug."""

    def refuse(self, request, *a, **kw):
        raise RuntimeError(f"network access in tests: {request.url}")

    monkeypatch.setattr(requests.adapters.HTTPAdapter, "send", refuse)


@pytest.fixture
def data_dir(tmp_path, monkeypatch):
    """A fresh, empty stand-in for data/ with every project path pointed at it."""
    d = repoint(tmp_path / "data", monkeypatch)
    import config

    config.ensure_dirs()
    return d


@pytest.fixture
def fixtures() -> Path:
    return FIXTURES
