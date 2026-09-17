"""Central configuration and path layout for the NSE EOD database.

Layout (all under DATA_DIR):
  raw/            immutable downloaded NSE reports, one folder per kind and year
  store/          one Parquet file per security (UNADJUSTED prices + adj_factor)
  indices/        one Parquet file per NSE index (from ind_close_all reports)
  actions/        corporate actions table (Parquet + CSV) and manual overrides
  breadth/        market breadth time series
  logs/           sync log, failed dates, adjustment warnings
  meta.json       sync state, per-symbol last dates, holidays
"""

from __future__ import annotations

from datetime import date
from pathlib import Path
from zoneinfo import ZoneInfo

ROOT_DIR = Path(__file__).resolve().parent
DATA_DIR = ROOT_DIR / "data"
RAW_DIR = DATA_DIR / "raw"
STORE_DIR = DATA_DIR / "store"
INDEX_DIR = DATA_DIR / "indices"
ACTIONS_DIR = DATA_DIR / "actions"
BREADTH_DIR = DATA_DIR / "breadth"
LOG_DIR = DATA_DIR / "logs"

META_FILE = DATA_DIR / "meta.json"
SYMBOL_MASTER_FILE = DATA_DIR / "symbol_master.parquet"
SYMBOL_HISTORY_FILE = DATA_DIR / "isin_symbol_map.json"
ACTIONS_FILE = ACTIONS_DIR / "actions.parquet"
ACTIONS_CSV = ACTIONS_DIR / "actions.csv"
MANUAL_OVERRIDES_FILE = ACTIONS_DIR / "manual_overrides.csv"
BREADTH_FILE = BREADTH_DIR / "breadth.parquet"
FAILED_DATES_FILE = LOG_DIR / "failed_dates.csv"
MISSING_DATES_FILE = LOG_DIR / "missing_dates.json"
ADJ_WARNINGS_FILE = LOG_DIR / "adjustment_warnings.csv"
PE_ALERTS_FILE = LOG_DIR / "pe_alerts.csv"
SYNC_LOG_FILE = LOG_DIR / "sync.log"


# NSE switched equity bhavcopy to the CM-UDiFF format on this date
UDIFF_START_DATE = date(2024, 7, 8)
# sec_bhavdata_full delivery report is only available from here; MTO before that
SEC_FULL_START_DATE = date(2019, 1, 1)

TZ_IN = ZoneInfo("Asia/Kolkata")

# Equity series kept. https://www.nseindia.com/market-data/legend-of-series
MAIN_BOARD_SERIES = ("EQ", "BE", "BZ")
SME_SERIES = ("SM", "ST")
VALID_SERIES = MAIN_BOARD_SERIES + SME_SERIES
SERIES_PRIORITY = {"EQ": 1, "BE": 2, "BZ": 3, "SM": 4, "ST": 5}

# Scraper behaviour
REQUEST_MIN_INTERVAL = 1.2  # seconds between requests (plus jitter)
REQUEST_JITTER = 0.6
REQUEST_TIMEOUT = 30
MAX_RETRIES = 4
# Reports for the last N days that return 404 are treated as "not yet published"
# and retried on the next sync, older ones are recorded as missing/holiday.
RECENT_DAYS_RETRY = 5
# NSE publishes EOD reports in the evening; before this hour (IST) today's
# date is not attempted.
EOD_PUBLISH_HOUR = 18

# Adjustment sanity band: adjusted close on ex-date / adjusted close on previous
# day should be within this band, otherwise the adjustment is flagged.
ADJ_CHECK_LOW = 0.67
ADJ_CHECK_HIGH = 1.5

# Nifty PE alert thresholds
PE_ALERT_LOW = 20.0
PE_ALERT_HIGH = 25.0

NSE_HOME = "https://www.nseindia.com"
NSE_API = "https://www.nseindia.com/api"
NSE_ARCHIVES = "https://nsearchives.nseindia.com"

STORE_COLUMNS = [
    "date",
    "symbol",
    "series",
    "isin",
    "open",
    "high",
    "low",
    "close",
    "prev_close",
    "volume",
    "turnover",
    "trades",
    "deliv_qty",
    "deliv_pct",
    "adj_factor",
]


def ensure_dirs() -> None:
    for d in (DATA_DIR, RAW_DIR, STORE_DIR, INDEX_DIR, ACTIONS_DIR, BREADTH_DIR, LOG_DIR):
        d.mkdir(parents=True, exist_ok=True)
