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

## Setup

```powershell
git clone https://github.com/ancap97/NSEDATA4ME.git
cd NSEDATA4ME
python -m venv .venv
.venv\Scripts\pip install -r requirements.txt
.venv\Scripts\python sync.py
```

That's it. On the first run `sync.py` downloads the data snapshot (~430 MB, once) and then brings
it up to today; afterwards the same command just adds the new days. Optionally check the result
with `.venv\Scripts\python healthcheck.py` (exit code 0 = healthy).

**How the data is shipped:** `main` holds code only; the ~430 MB of Parquet lives on the separate
**`data` branch**, refreshed every week or two. Sync starts from that snapshot and appends every
trading day since it was taken, so you always end up current even if the branch is weeks old.
Each day takes a few minutes to ingest, so a month-old snapshot means a longer first run.

If you downloaded the repo as a zip instead of cloning it (no git remote to fetch from), get
<https://github.com/ancap97/NSEDATA4ME/archive/refs/heads/data.zip> too and move the `data`
folder out of the extracted `NSEDATA4ME-data\` folder into the repo, so you end up with
`NSEDATA4ME\data\store\...`, then run `sync.py`.

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
* A demerger's factor is the parent's previous close / its ex-date open (the price NSE's special
  pre-open session discovers), net of any split or bonus on the same day. Gaps under 2 % are not
  applied (indistinguishable from an ordinary move); for an exact ratio add a `DEMERGER` row to
  `manual_overrides.csv` with `factor` = price before / price after.
* `symbol` is the current symbol; old names resolve (`load_symbol("INFOSYSTCH")`). An old and a
  current name of one security in the same call load it once.
* Use `close.shift(1)` for returns, not `prev_close` (NSE's reference price).
* `trades` exists from 2011-06-22; delivery from 2005.
* Loading every symbol reads ~4,500 files — pass `start_date`/`columns` and cache the result (e.g. `px.to_parquet(...)`) in your own project.

## Update

```powershell
.venv\Scripts\python sync.py                          # after 18:00 IST; fetches every day since last sync
.venv\Scripts\python sync.py --force                  # try today before 18:00
.venv\Scripts\python sync.py --redo-date 2026-09-04   # re-download + re-ingest one day
.venv\Scripts\python healthcheck.py                   # status; exit 1 = needs attention
.venv\Scripts\python publish_data.py --push           # refresh the data branch (maintainer)
```

Manual corporate-action fixes: add rows to `data/actions/manual_overrides.csv` (tracked on `main`), then sync.
`type` is `SPLIT`, `BONUS`, `CONSOLIDATION` or `DEMERGER`; `action` is `add` or `ignore`.

`sync.py` rewrites the security files of large batches (a day's ~3,400 appends, `--redo-date`) in
a few worker processes; set `$env:NSE_SYNC_WORKERS = 1` to keep it in one process.

**Why data is not on `main`:** every sync rewrites thousands of compressed Parquet files, so each
data commit would add ~400 MB of history that git can never reuse. `publish_data.py` writes a
fresh parentless commit on the `data` branch and force-pushes it, replacing the previous snapshot
instead of stacking on it, so the repo stays about one snapshot in size.

**Using more than one computer:** take the snapshot before syncing, and refresh the branch after.
Don't sync on two machines in between — the Parquet files can't be merged.

## Running tests

```powershell
.venv\Scripts\pip install pytest
.venv\Scripts\python -m pytest -q            # offline: fixtures + a small synthetic database, ~2 min
```

The suite never touches `data/` or the network (every data path is redirected to a temporary
folder and HTTP is blocked). Optionally check the read API against a real snapshot: the digests in
`tests/golden/golden_hashes.json` were recorded from `data` branch commit `fcb3714` (data through
2026-10-26, pandas 3.0 / pyarrow 26) and must match exactly.

```powershell
git fetch --depth 1 origin data:refs/remotes/origin/data
git worktree add --detach ..\nse-data origin/data
.venv\Scripts\python -m tests.golden.golden check ..\nse-data\data    # exit 1 on any difference
# or through pytest:  $env:NSE_GOLDEN_DATA = "..\nse-data\data"; .venv\Scripts\python -m pytest tests\test_golden.py
```

After refreshing the `data` branch, re-record with `python -m tests.golden.golden record <dir>`.

## Layout

```
loader.py        read API (import this)
sync.py          update entrypoint        healthcheck.py   status check
bootstrap.py     first-run snapshot fetch  publish_data.py  refresh the data branch (maintainer)
config.py scraper.py parsers.py symbol_master.py storage.py adjuster.py breadth.py pipeline.py   (sync internals)
data/store/      one Parquet per security (unadjusted + adj_factor)
data/indices/    NSE indices             data/breadth/  market breadth
data/actions/    corporate actions       data/raw/      downloaded NSE reports
data/meta.json   sync state              data/logs/     sync log + status
tests/           offline test suite      docs/REVIEW.md code review notes
```

On `main` only `data/actions/manual_overrides.csv` is tracked. The processed data (`store`,
`indices`, `actions`, `breadth`, `meta.json`, symbol maps) comes from the `data` branch;
`data/raw/` and `data/logs/` are machine-local and never published. Sync does not need the old
raw files: it continues from `last_synced` in `data/meta.json` and downloads new days.
There is no tooling to rebuild the database from scratch (it was removed): the `data` branch
snapshot is the starting point, so keep a backup of `data/` if you care about your local copy.

## Credits and license

Derived from [eod2](https://github.com/BennyThadikaran/eod2) and related projects by
Benny Thadikaran (GPL v3), so this project is also licensed under the
**GNU General Public License v3.0**. See [LICENSE](LICENSE).

Market data is published by NSE and remains subject to
[NSE's terms of use](https://www.nseindia.com/static/nse-terms-of-use).
