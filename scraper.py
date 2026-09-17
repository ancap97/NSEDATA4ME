"""NSE downloader: rate-limited, retrying, resume-safe.

Cookie handling and headers follow BennyThadikaran/NseIndiaApi (GPL-3).
Raw reports are stored immutably under data/raw/<kind>/<year>/ using NSE's
own file names, so a re-run only fetches what is not on disk.

Missing reports are classified:
  * recent date (within RECENT_DAYS_RETRY) -> "pending", retried next run
  * older date                              -> "missing", recorded in
    logs/missing_dates.json and never requested again (holiday or NSE gap)
All network/parse failures are appended to logs/failed_dates.csv.
"""

from __future__ import annotations

import csv
import json
import logging
import random
import time
import zipfile
from datetime import date, datetime
from pathlib import Path
from typing import Dict, List, Optional

import requests

from config import (
    FAILED_DATES_FILE,
    LOG_DIR,
    MAX_RETRIES,
    MISSING_DATES_FILE,
    NSE_API,
    NSE_ARCHIVES,
    NSE_HOME,
    RAW_DIR,
    RECENT_DAYS_RETRY,
    REQUEST_JITTER,
    REQUEST_MIN_INTERVAL,
    REQUEST_TIMEOUT,
    SEC_FULL_START_DATE,
    TZ_IN,
    UDIFF_START_DATE,
    ensure_dirs,
)
from parsers import (
    bhav_filename,
    delivery_filename,
    indices_filename,
    mto_filename,
    pr_filename,
)

logger = logging.getLogger("scraper")

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "*/*",
    "Accept-Language": "en-US,en;q=0.9",
    # no "br": requests cannot decode brotli without an extra package
    "Accept-Encoding": "gzip, deflate",
    "Referer": "https://www.nseindia.com/get-quotes/equity?symbol=HDFCBANK",
    "Connection": "keep-alive",
}

KINDS = ("bhav", "delivery", "mto", "indices", "pr")


class ReportUnavailable(Exception):
    """NSE returned 404 / HTML page: report does not exist (yet)."""


def raw_path(kind: str, d: date) -> Path:
    names = {
        "bhav": bhav_filename,
        "delivery": delivery_filename,
        "mto": mto_filename,
        "indices": indices_filename,
        "pr": pr_filename,
    }
    return RAW_DIR / kind / str(d.year) / names[kind](d)


def report_url(kind: str, d: date) -> str:
    if kind == "bhav":
        if d >= UDIFF_START_DATE:
            return f"{NSE_ARCHIVES}/content/cm/BhavCopy_NSE_CM_0_0_0_{d:%Y%m%d}_F_0000.csv.zip"
        ds = d.strftime("%d%b%Y").upper()
        return f"{NSE_ARCHIVES}/content/historical/EQUITIES/{d.year}/{ds[2:5]}/cm{ds}bhav.csv.zip"
    if kind == "delivery":
        return f"{NSE_ARCHIVES}/products/content/sec_bhavdata_full_{d:%d%m%Y}.csv"
    if kind == "mto":
        return f"{NSE_ARCHIVES}/archives/equities/mto/MTO_{d:%d%m%Y}.DAT"
    if kind == "indices":
        return f"{NSE_ARCHIVES}/content/indices/ind_close_all_{d:%d%m%Y}.csv"
    if kind == "pr":
        return f"{NSE_ARCHIVES}/archives/equities/bhavcopy/pr/PR{d:%d%m%y}.zip"
    raise ValueError(kind)


class MissingDates:
    """Persistent record of report dates NSE does not have (holidays/gaps)."""

    def __init__(self, path: Path = MISSING_DATES_FILE):
        self.path = path
        self.data: Dict[str, List[str]] = {k: [] for k in KINDS}
        if path.exists():
            try:
                self.data.update(json.loads(path.read_text()))
            except json.JSONDecodeError:
                pass
        self._sets = {k: set(v) for k, v in self.data.items()}

    def is_missing(self, kind: str, d: date) -> bool:
        return d.isoformat() in self._sets.setdefault(kind, set())

    def add(self, kind: str, d: date) -> None:
        key = d.isoformat()
        if key not in self._sets.setdefault(kind, set()):
            self._sets[kind].add(key)
            self.data.setdefault(kind, []).append(key)
            self.save()

    def remove(self, kind: str, d: date) -> None:
        key = d.isoformat()
        if key in self._sets.get(kind, set()):
            self._sets[kind].discard(key)
            self.data[kind] = sorted(self._sets[kind])
            self.save()

    def save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(json.dumps({k: sorted(v) for k, v in self._sets.items()}, indent=1))


def log_failed_date(kind: str, d: date, reason: str) -> None:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    new = not FAILED_DATES_FILE.exists()
    with FAILED_DATES_FILE.open("a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        if new:
            w.writerow(["logged_at", "kind", "date", "reason"])
        w.writerow([datetime.now(TZ_IN).isoformat(timespec="seconds"), kind, d.isoformat(), reason])


class NSEClient:
    """requests.Session wrapper with cookie bootstrap, throttling and retries."""

    def __init__(self, min_interval: float = REQUEST_MIN_INTERVAL, timeout: int = REQUEST_TIMEOUT):
        ensure_dirs()
        self.session = requests.Session()
        self.session.headers.update(HEADERS)
        self.min_interval = min_interval
        self.timeout = timeout
        self._last_request = 0.0
        self._cookies_at = 0.0
        self.missing = MissingDates()
        self.stats = {"downloaded": 0, "skipped": 0, "unavailable": 0, "failed": 0}

    # -- low level ---------------------------------------------------------
    def _throttle(self) -> None:
        wait = self.min_interval + random.uniform(0, REQUEST_JITTER) - (time.monotonic() - self._last_request)
        if wait > 0:
            time.sleep(wait)
        self._last_request = time.monotonic()

    def bootstrap_cookies(self, force: bool = False) -> None:
        if not force and time.monotonic() - self._cookies_at < 20 * 60:
            return
        self._throttle()
        try:
            self.session.get(f"{NSE_HOME}/option-chain", timeout=self.timeout)
        except requests.RequestException as e:
            logger.warning("cookie bootstrap failed: %s", e)
        self._cookies_at = time.monotonic()

    def _get(self, url: str, params: Optional[dict] = None, stream: bool = False) -> requests.Response:
        last_exc: Optional[Exception] = None
        for attempt in range(MAX_RETRIES):
            self._throttle()
            try:
                r = self.session.get(url, params=params, timeout=self.timeout, stream=stream)
            except requests.RequestException as e:
                last_exc = e
                backoff = min(2 ** (attempt + 1), 30) + random.uniform(0, 1)
                logger.info("request error %s (attempt %d) - retry in %.0fs", e, attempt + 1, backoff)
                time.sleep(backoff)
                continue

            if r.status_code == 404:
                raise ReportUnavailable(url)
            if r.status_code in (401, 403):
                logger.info("HTTP %s - refreshing cookies", r.status_code)
                self.bootstrap_cookies(force=True)
                time.sleep(2 ** attempt)
                continue
            if r.status_code >= 500 or r.status_code == 429:
                backoff = min(2 ** (attempt + 1), 60)
                logger.info("HTTP %s - retry in %ds", r.status_code, backoff)
                time.sleep(backoff)
                continue
            if r.status_code != 200:
                raise ConnectionError(f"HTTP {r.status_code} for {url}")
            return r
        raise ConnectionError(f"giving up on {url}: {last_exc}")

    # -- JSON API ------------------------------------------------------------
    def api(self, endpoint: str, params: Optional[dict] = None):
        self.bootstrap_cookies()
        r = self._get(f"{NSE_API}/{endpoint}", params=params)
        try:
            return r.json()
        except ValueError as e:
            raise ConnectionError(f"non-JSON response from {endpoint}") from e

    def holidays(self) -> Dict[str, str]:
        """Trading holidays for the capital market segment, {'DD-Mon-YYYY': desc}."""
        data = self.api("holiday-master", {"type": "trading"})
        out = {}
        for row in data.get("CM", []):
            out[row["tradingDate"]] = row.get("description", "")
        return out

    def actions(self, segment: str, from_date: date, to_date: date, symbol: Optional[str] = None) -> List[dict]:
        params = {
            "index": segment,
            "from_date": from_date.strftime("%d-%m-%Y"),
            "to_date": to_date.strftime("%d-%m-%Y"),
        }
        if symbol:
            params["symbol"] = symbol
        data = self.api("corporates-corporateActions", params)
        if isinstance(data, dict):
            data = data.get("data", [])
        return data or []

    def circulars(self, dept_code: str, from_date: date, to_date: date) -> List[dict]:
        data = self.api(
            "circulars",
            {"from_date": from_date.strftime("%d-%m-%Y"), "to_date": to_date.strftime("%d-%m-%Y"), "dept": dept_code},
        )
        return data.get("data", []) if isinstance(data, dict) else []

    # -- report downloads ----------------------------------------------------
    def download_report(self, kind: str, d: date, force: bool = False) -> Optional[Path]:
        """Download one report to data/raw. Returns path, or None if unavailable.

        Raises ConnectionError after retries are exhausted (caller decides).
        """
        dest = raw_path(kind, d)
        if dest.exists() and dest.stat().st_size > 0 and not force:
            self.stats["skipped"] += 1
            return dest
        if not force and self.missing.is_missing(kind, d):
            self.stats["unavailable"] += 1
            return None

        url = report_url(kind, d)
        dest.parent.mkdir(parents=True, exist_ok=True)
        tmp = dest.with_suffix(dest.suffix + ".part")
        try:
            r = self._get(url, stream=True)
            ctype = r.headers.get("content-type", "")
            if "text/html" in ctype:
                raise ReportUnavailable(url)
            with tmp.open("wb") as f:
                for chunk in r.iter_content(chunk_size=1 << 20):
                    f.write(chunk)
            if url.endswith(".zip"):
                self._unzip_single(tmp, dest)
                tmp.unlink(missing_ok=True)
            else:
                tmp.replace(dest)
        except ReportUnavailable:
            tmp.unlink(missing_ok=True)
            self.stats["unavailable"] += 1
            age = (datetime.now(TZ_IN).date() - d).days
            if age > RECENT_DAYS_RETRY:
                self.missing.add(kind, d)
            logger.info("%s %s: not available on NSE", kind, d)
            return None
        except Exception as e:
            tmp.unlink(missing_ok=True)
            self.stats["failed"] += 1
            log_failed_date(kind, d, repr(e))
            raise

        if dest.stat().st_size == 0:
            dest.unlink()
            log_failed_date(kind, d, "empty file")
            raise ConnectionError(f"empty download for {kind} {d}")

        self.stats["downloaded"] += 1
        logger.info("%s %s: downloaded", kind, d)
        return dest

    @staticmethod
    def _unzip_single(zip_tmp: Path, dest: Path) -> None:
        if dest.suffix == ".zip":
            zip_tmp.replace(dest)
            return
        with zipfile.ZipFile(zip_tmp) as zf:
            names = [n for n in zf.namelist() if not n.endswith("/")]
            if not names:
                raise ReportUnavailable(str(zip_tmp))
            member = next((n for n in names if n.lower().endswith(".csv")), names[0])
            with zf.open(member) as src, dest.open("wb") as out:
                out.write(src.read())

    def download_delivery(self, d: date, force: bool = False) -> Optional[Path]:
        """Delivery data: sec_bhavdata_full from 2019, MTO before (fallback both)."""
        if d >= SEC_FULL_START_DATE:
            p = self.download_report("delivery", d, force)
            if p is not None:
                return p
        return self.download_report("mto", d, force)

    def close(self) -> None:
        self.session.close()
