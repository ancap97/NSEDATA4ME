# Code review: NSEDATA4ME

Scope: every `.py` file and the README on `main` at `edb16d4`, checked against the `data` branch
snapshot `fcb3714` (data through 2026-10-26, 4,583 securities, 232 index files).
Line numbers refer to `edb16d4`.

## 1. Architecture and data flow

```
                     NSE archives / API (scraper.NSEClient: throttle, retries, cookie refresh)
                                         |
          raw/<kind>/<year>/<NSE file name>   (immutable; .part temp file; MissingDates for 404s)
                                         |
 parsers.py  bhav (old <=2024-07-05 | UDiFF >=2024-07-08) + delivery (sec_bhavdata_full >=2019 | MTO)
             -> one normalised frame per day (EQUITY_COLUMNS), series filter + per-symbol dedupe
                                         |
 symbol_master.EntityResolver.resolve_day   (symbol|board|ISIN pair -> entity key; renames by ISIN)
                                         |
 pipeline.ingest_dates -> commit_batch      (Store.rename for renames, Store.append per key)
                                         |
 storage.Store  data/store/<key>.parquet    UNADJUSTED prices + adj_factor (atomic tmp+replace)
 storage.IndexStore data/indices/<slug>.parquet  (old index names merged by canonical name)
 storage meta.json                          (last_synced, in_progress, holidays, pending_delivery..)
                                         |
 adjuster.build_actions_table   API + PR bc files + existing table + manual_overrides.csv
          -> classify purpose -> resolve keys -> dedupe by source priority -> fill *_UNPARSED
             from prices -> reconcile ex-dates with prices -> drop near-duplicates
 adjuster.apply_adjustments     adj_factor(row) = prod(factor of actions with ex_date > row.date)
                                         |
 breadth.rebuild_breadth        full recompute from adjusted closes of all main-board keys
                                         |
 loader.py  (read API)  load_data / load_wide / load_symbol / load_index / load_breadth / load_actions
```

`sync.run_sync` order: recover `in_progress` -> holidays + special sessions -> delivery backfill ->
candidate dates (weekday, not a holiday, or special session; up to the 18:00 IST cutoff) ->
download -> ingest one day per batch (`in_progress` set before, cleared after each day) ->
symbol master -> corporate actions + adj_factor for affected keys -> breadth -> meta.

Exit codes: `sync.py` 0 on success, 1 on any exception (status in `logs/last_sync_status.json`);
`healthcheck.py` 0 healthy, 1 needs attention.

## 2. Suspected bugs

Severity: H = data loss / wrong data likely, M = wrong data in a plausible situation, L = edge case
or cosmetic. Status column is filled in as the work proceeds (see section 6).

| # | Where | What and why | Sev |
|---|-------|--------------|-----|
| 1 | `storage.py:271` | `load_meta` starts from `dict(DEFAULT_META)`, a shallow copy, so the mutable defaults (`holidays`, `bootstrap`, `special_sessions`, `pending_delivery`) are shared. A meta without `pending_delivery` + `meta["pending_delivery"].append(...)` (sync does exactly this via `.remove`/`.append`) mutates `DEFAULT_META` for the rest of the process; every later `load_meta()` sees the leaked values. Reproduced. | M |
| 2 | `storage.py:283-293` | `save_meta` moves `meta.json` to `.bak` *before* moving the new file in. A crash between the two `os.replace` calls leaves no `meta.json`; `bootstrap.data_present()` then returns False and the next `sync.py` re-extracts the (older) data-branch snapshot over the local store. | M |
| 3 | `symbol_master.py:231-239`, `sync.py:366` | `resolve_day` unconditionally sets `last_date`, `series`, `symbols[-1].to_date`, `isins[-1].to_date` to the day being resolved. `--redo-date` re-resolves an *old* day, which moves `last_date` backwards for every security that traded that day (and `to_date` of a renamed security's current symbol to before its `from_date`). `save_outputs(as_of=redo_date)` then computes `status` relative to the redo date. Effects: `symbol_master.parquet` `last_date`/`status` wrong, `key_for_symbol(on=...)` misses, `include_delisted=False` universe wrong until a later sync touches each entity (never, for securities that stopped trading since). | M |
| 4 | `scraper.py:300-311` | `_unzip_single` writes the extracted CSV straight into the final path. A crash/kill mid-write leaves a truncated non-empty bhavcopy that later runs treat as present (size > 0), so a truncated day can be ingested silently. With `force=True` (`--redo-date`) a corrupt download truncates the previously good file before the error is noticed. Every other download path is tmp + replace. | M |
| 5 | `pipeline.py:44-52` | `is_candidate_trading_day` checks the weekend before the "Diwali Laxmi Pujan" (Muhurat) rule, so a Muhurat session on a Saturday/Sunday is never attempted unless `special_sessions` happens to list it. The store has weekend Muhurat days (2016-10-30, 2023-11-12, both Sundays) that came from the removed bulk build; the current NSE list has `08-Nov-2026: Diwali Laxmi Pujan*`, a Sunday. | M |
| 6 | `sync.py:343-349` | `on_committed` saves `meta.json` (day committed) before `resolver.save()`. A crash between them leaves the resolver one day behind the store, the opposite of what the comment intends (new entities from that day are recreated later with the wrong `first_date`). Saving the resolver first is idempotent on recovery. | L |
| 7 | `symbol_master.py:283-288`, `scraper.py:134-136` | `symbol_master.parquet`, `isin_symbol_map.json` and `logs/missing_dates.json` are written in place. A crash mid-write corrupts the symbol master, which makes every `loader` call fail (`load_master`). | L |
| 8 | `scraper.py:180-208` | With `stream=True`, responses for 401/403/429/5xx are never closed before retrying (connection not returned to the pool), and after exhausting retries on 403 the error reads `giving up on <url>: None` because `last_exc` only tracks exceptions. | L |
| 9 | `symbol_master.py:185-239` | If one day lists the same ISIN under two symbols, the second row renames the entity after the first row's key was assigned; the final loop then hits `KeyError` on the old key and the sync aborts. Not seen in NSE data (per-day dedupe is by symbol, not ISIN). | L |
| 10 | `loader.py:78` | `load_data(["INFY", "INFOSYSTCH"])` resolves both to `INFY` and returns every row twice; `load_wide(symbols=[...])` with such aliases raises "Index contains duplicate entries". Changing it changes output -> section 5. | L |
| 11 | `README.md:9`, `adjuster.py:62` | README says prices are adjusted for demergers; `ADJUSTING_TYPES` is SPLIT/BONUS/CONSOLIDATION only and demergers are stored as `SCHEME` with no factor (e.g. TMPV/TATAMOTORS 2025-10-14: 660.75 -> 395.45 with `adj_factor` 1.0; RELIANCE 2023-07-20, ITC 2025-01-06). A manual override with `type=DEMERGER` would be silently ignored. -> section 5. | M |
| 12 | `parsers.py:190-219` | `merge_delivery` docstring says BE/BZ use volume as delivery "when the delivery report does not list them"; the code always overwrites with volume / 100 %. -> section 5 (doc vs behaviour). | L |
| 13 | `storage.py:99-108` | `Store.rename` onto an existing file concatenates `[new, old]` and keeps the *old* row on duplicate dates. Only reachable when a file exists for a key that is not an entity. | L |

Checked and found correct (kept here so they are not re-investigated): `compute_adj_factor`
(strictly-after semantics, cumulative product; recomputing it for all 4,583 securities reproduces
every stored `adj_factor` exactly), split/bonus/consolidation factor maths (`old_fv/new_fv`,
`1 + a/b`, consolidation < 1), recovery of an interrupted day (append de-duplicates by date, so a
re-ingest replaces partial rows even for renamed keys), the 404 "not yet published" window,
UDiFF/old filename switch, `SEC_FULL_START_DATE` preference order, the `B023` ruff finding in
`pipeline.py:217` (the lambda is only called inside the same iteration).

## 3. Risky spots (no change proposed unless listed above)

* **18:00 IST cutoff** (`pipeline.eod_published_cutoff`): correct as long as `now` is IST-aware
  (it is everywhere in the code). `--force` uses today's IST date. `sync.refresh_special_sessions`
  uses `date.today()` (machine-local date) for its window: harmless (30-day overlap).
* **Holidays**: only the current year's list is kept (`refresh_holidays` replaces it on Jan 1), so
  previous-year holidays are re-tried after New Year; a 404 with a later published day is then
  recorded as an inferred holiday. `healthcheck` can over-count "days behind" by the number of
  such holidays around New Year.
* **Special sessions** are found by fuzzy-parsing circular subjects (`sync.py:144`): a subject
  with other numbers can yield a wrong date (it only adds a candidate; a 404 makes it a holiday).
* **UDiFF switch 2024-07-08**: filename and URL both switch on `UDIFF_START_DATE`; parser dispatch
  is by file name. Re-downloading a pre-switch day always uses the old format, as it should.
* **SEC_FULL_START_DATE 2019-01-01**: `download_delivery` falls back to MTO when sec_full is
  missing; `load_day` prefers sec_full from 2019 and MTO before. A 404 older than 5 days is recorded
  in `missing_dates.json` permanently.
* **ISIN / symbol renames**: rename detection is ISIN based; pre-2011 rows have no ISIN and are
  joined by symbol continuity. Symbol reuse after >365 days is flagged to `identity_review.csv`
  but merged. `key_for_symbol` falls back to the latest period when a symbol was used twice.
* **adj_factor maths**: factors multiply across overlapping actions (same-day bonus + split are
  two rows and both apply, e.g. BAJFINANCE 2016-09-08 = 2 x 5). Near-duplicates within 35 days
  with the same factor are dropped (one action recorded by two sources with different dates).
  `reconcile_ex_dates` moves ex-dates and can *replace* a factor from the price move; these are
  logged in the purpose text. Dividends/rights/demergers are not applied (see #11).
* **New rows get `adj_factor = 1.0`** on append; correctness relies on step 5 recomputing every key
  with an adjusting action with `ex_date >= first new day`. If the actions step fails after
  ingest, the next sync still covers those keys (future actions stay >= its first day).
* **Retries / rate limiting**: 1.2 s + 0-0.6 s jitter between requests, 4 attempts, cookie refresh
  on 401/403, exponential backoff on 429/5xx. A bhavcopy network failure aborts the whole run on
  purpose ("never skip a day").
* **Atomicity**: store, index, actions parquet, breadth, resolver state and status file use
  tmp + replace. Exceptions are #2, #4 and #7. The actions CSV, `unconfirmed_actions.csv` and
  `adjustment_warnings.csv` are rewritten in place but are reports, not inputs to the store.
* **Windows**: `zoneinfo` needs the `tzdata` package on Windows; it arrives as a pandas dependency.
  All paths go through `pathlib`; `os.replace` overwrites on Windows.

## 4. Optimisation candidates

* `loader.load_data` reads all 15 columns of every file and filters dates in pandas; `load_wide`
  needs 3. Reading only the needed columns roughly halves the per-file cost (5.3 ms -> 2.8 ms
  measured). Files are independent, so reads can run in a thread pool (pyarrow releases the GIL).
* `loader.resolve_symbols` parses the 2 MB `symbol_state.json` on every call; `load_data` also
  reads `symbol_master.parquet` again.
* `IndexStore._stored_names` + `list_names` read every index file twice (232 files).
* `breadth._load_wide` (sync step 6, ~37 s) reads ~4,000 files serially.
* `Store.remove_dates` (used by `--redo-date` and recovery) reads every file in full; files whose
  date range cannot contain the day can be skipped from Parquet statistics.
* `pipeline.commit_batch`: one read-modify-write per security per day (the "few minutes per
  day" of sync); independent per key.

## 5. Needs owner decision

(Filled in during the work; see the end of this document.)

## 6. Work log

(Filled in during the work.)
