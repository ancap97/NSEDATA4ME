# NSE EOD Database

A local, survivorship-bias-free end-of-day database of NSE (National Stock Exchange of India)
equities, built for backtesting.

* **Daily OHLCV + delivery data from 2005 to today**, one Parquet file per security
* **Includes delisted stocks**, so backtests are not biased towards survivors
* **Corporate-action adjusted** (splits, bonuses, consolidations, demergers) via a stored
  `adj_factor`; unadjusted NSE prices are kept too
* **Symbol changes resolved** by ISIN: old names still load (`INFOSYSTCH` → `INFY`)
* **NSE indices** (OHLC, P/E, P/B, dividend yield) and **market breadth**
* **Incremental sync**: one command downloads the NSE bhavcopy, delivery, index and
  corporate-action reports for every day since the last run

## Credits and inspiration

This project is inspired by and derived from
**[eod2](https://github.com/BennyThadikaran/eod2) by Benny Thadikaran**, which is licensed
under the GNU GPL v3. The approach to downloading NSE reports, adjusting for corporate
actions and analysing delivery follows eod2.

Also based on Benny Thadikaran's work (both GPL v3):

* [NseIndiaApi](https://github.com/BennyThadikaran/NseIndiaApi): NSE request headers and cookie handling
* [eod2_utils](https://github.com/BennyThadikaran/eod2_utils): historical corporate-action seed data

Many thanks to Benny Thadikaran for making these tools open source.

## License

Because it is derived from GPL v3 code, this project is also licensed under the
**GNU General Public License v3.0**. See [LICENSE](LICENSE).

Market data is published by NSE and remains subject to
[NSE's terms of use](https://www.nseindia.com/terms-of-use).

## Setup

```powershell
git clone https://github.com/ancap97/NSEDATA4ME.git
cd NSEDATA4ME
python -m venv .venv
.venv\Scripts\pip install -r requirements.txt
.venv\Scripts\python sync.py
```

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

**Using more than one computer:** run `git pull` before syncing, and commit + `git push`
after. Don't sync on two machines without pulling in between — the Parquet files can't be merged.

## Layout

```
loader.py        read API (import this)
sync.py          update entrypoint        healthcheck.py  status check
config.py scraper.py parsers.py symbol_master.py storage.py adjuster.py breadth.py pipeline.py   (sync internals)
data/store/      one Parquet per security (unadjusted + adj_factor)
data/indices/    NSE indices             data/breadth/  market breadth
data/actions/    corporate actions       data/raw/      downloaded NSE reports (not in repo)
data/meta.json   sync state              data/logs/     sync log + status (not in repo)
```

`data/raw/` and `data/logs/` are not in the repo (too large / machine-local). Sync does not need
the old raw files: it continues from `last_synced` in `data/meta.json` and downloads new days.
On a fresh clone, `healthcheck.py` reports "needs attention" until the first sync.
The full-rebuild tooling (bootstrap, validation, dashboard, tests) has been removed — back up `data/`.
