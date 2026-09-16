# NSE EOD Database

Survivorship-bias-free NSE equity history (2005 → today): daily OHLCV + delivery,
corporate-action adjusted, one Parquet file per security. Logic derived from
[eod2](https://github.com/BennyThadikaran/eod2) (GPL-3, see LICENSE).

## Use it in a backtest

```python
import sys
sys.path.insert(0, r"path\to\this\repo")

from loader import load_data, load_wide, load_symbol, load_index, load_breadth, load_actions, list_symbols, list_indices

px   = load_wide("close", start_date="2015-01-01")          # date x symbol, adjusted, incl. delisted
vol  = load_wide("volume", start_date="2015-01-01")
df   = load_data(["RELIANCE", "TCS"], start_date="2010-01-01")   # long format
rel  = load_symbol("RELIANCE")                               # one security, date index
raw  = load_data(["ITC"], adjusted=False)                    # NSE prices as published
nifty = load_index("Nifty 50")                               # OHLC + P/E, P/B, div yield
acts = load_actions("IRCTC", adjusting_only=True)
```

To skip the `sys.path` line permanently, drop a `.pth` file into the other venv:
`"path\to\this\repo" | Out-File -Encoding ascii .venv\Lib\site-packages\nse.pth`

Read path needs only `pandas pyarrow numpy`.

**Columns:** `date, symbol, key, isin, series, open, high, low, close, prev_close,
volume, turnover, trades, deliv_qty, deliv_pct, adj_factor`

`load_data` args: `symbols=None, start_date="2005-01-01", end_date=None,
include_delisted=True, include_sme=False, adjusted=True, columns=None`

Notes
* Adjusted = `price / adj_factor`, `qty * adj_factor` (splits, bonuses, consolidations, demergers). Dividends and rights are **not** applied.
* `symbol` is the current symbol; old names resolve (`load_symbol("INFOSYSTCH")`).
* Use `close.shift(1)` for returns, not `prev_close` (NSE's reference price).
* `trades` exists from 2011-06-22; delivery from 2005.
* Loading all ~symbols reads thousands of files — pass `start_date`/`columns` and cache the result (e.g. `px.to_parquet(...)`) in your own project.

## Update

```powershell
.venv\Scripts\python sync.py                          # after 18:00 IST; fetches every day since last sync
.venv\Scripts\python sync.py --force                  # try today before 18:00
.venv\Scripts\python sync.py --redo-date 2026-09-04   # re-download + re-ingest one day
.venv\Scripts\python healthcheck.py                   # status; exit 1 = needs attention
```

Manual corporate-action fixes: add rows to `data/actions/manual_overrides.csv`, then sync.

## Layout

```
loader.py        read API (import this)
sync.py          update entrypoint        healthcheck.py  status check
config.py scraper.py parsers.py symbol_master.py storage.py adjuster.py breadth.py pipeline.py   (sync internals)
data/store/      one Parquet per security (unadjusted + adj_factor)
data/indices/    NSE indices             data/breadth/  market breadth
data/actions/    corporate actions       data/raw/      downloaded NSE reports (sync needs these)
data/meta.json   sync state              data/logs/     sync log + status
```

`data/raw/` and `data/logs/` are not in the repo (too large / machine-local). Sync does not need
the old raw files: it continues from `last_synced` in `data/meta.json` and downloads new days.
The full-rebuild tooling (bootstrap, validation, dashboard, tests) has been removed — back up `data/`.
