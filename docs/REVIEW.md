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
or cosmetic. Each fix has a test that failed before it (section 6).

| # | Where | What and why | Sev | Status |
|---|-------|--------------|-----|--------|
| 1 | `storage.py:271` | `load_meta` starts from `dict(DEFAULT_META)`, a shallow copy, so the mutable defaults (`holidays`, `bootstrap`, `special_sessions`, `pending_delivery`) are shared. A meta without `pending_delivery` + `meta["pending_delivery"].append(...)` (sync does exactly this via `.remove`/`.append`) mutates `DEFAULT_META` for the rest of the process; every later `load_meta()` sees the leaked values. Reproduced. | M | fixed `dd15bfa` |
| 2 | `storage.py:283-293` | `save_meta` moves `meta.json` to `.bak` *before* moving the new file in. A crash between the two `os.replace` calls leaves no `meta.json`; `bootstrap.data_present()` then returns False and the next `sync.py` re-extracts the (older) data-branch snapshot over the local store. | M | fixed `04fffff` |
| 3 | `symbol_master.py:231-239`, `sync.py:366` | `resolve_day` unconditionally sets `last_date`, `series`, `symbols[-1].to_date`, `isins[-1].to_date` to the day being resolved. `--redo-date` re-resolves an *old* day, which moves `last_date` backwards for every security that traded that day (and `to_date` of a renamed security's current symbol to before its `from_date`). `save_outputs(as_of=redo_date)` then computes `status` relative to the redo date. Effects: `symbol_master.parquet` `last_date`/`status` wrong, `key_for_symbol(on=...)` misses, `include_delisted=False` universe wrong until a later sync touches each entity (never, for securities that stopped trading since). | M | fixed `a6e9c14` |
| 4 | `scraper.py:300-311` | `_unzip_single` writes the extracted CSV straight into the final path. A crash/kill mid-write leaves a truncated non-empty bhavcopy that later runs treat as present (size > 0), so a truncated day can be ingested silently. With `force=True` (`--redo-date`) a corrupt download truncates the previously good file before the error is noticed. Every other download path is tmp + replace. | M | fixed `1574a73` |
| 5 | `pipeline.py:44-52` | `is_candidate_trading_day` checks the weekend before the "Diwali Laxmi Pujan" (Muhurat) rule, so a Muhurat session on a Saturday/Sunday is never attempted unless `special_sessions` happens to list it. The store has weekend Muhurat days (2016-10-30, 2023-11-12, both Sundays) that came from the removed bulk build; the current NSE list has `08-Nov-2026: Diwali Laxmi Pujan*`, a Sunday. | M | fixed `0ca8de9` |
| 6 | `sync.py:343-349` | `on_committed` saves `meta.json` (day committed) before `resolver.save()`. A crash between them leaves the resolver one day behind the store, the opposite of what the comment intends (new entities from that day are recreated later with the wrong `first_date`). Saving the resolver first is idempotent on recovery. | L | fixed `4bd2b5e` |
| 7 | `symbol_master.py:283-288`, `scraper.py:134-136` | `symbol_master.parquet`, `isin_symbol_map.json` and `logs/missing_dates.json` are written in place. A crash mid-write corrupts the symbol master, which makes every `loader` call fail (`load_master`). | L | fixed `9f7aa99` |
| 8 | `scraper.py:180-208` | With `stream=True`, responses for 401/403/429/5xx are never closed before retrying (connection not returned to the pool), and after exhausting retries on 403 the error reads `giving up on <url>: None` because `last_exc` only tracks exceptions. | L | fixed `58d2bc0` |
| 9 | `symbol_master.py:185-239` | If one day lists the same ISIN under two symbols, the second row renames the entity after the first row's key was assigned; the final loop then hits `KeyError` on the old key and the sync aborts. Not seen in NSE data (per-day dedupe is by symbol, not ISIN). | L | owner decision (5.4) |
| 10 | `loader.py:78` | `load_data(["INFY", "INFOSYSTCH"])` resolves both to `INFY` and returns every row twice; `load_wide(symbols=[...])` with such aliases raises "Index contains duplicate entries". Changing it changes output -> section 5. | L | owner decision (5.1) |
| 11 | `README.md:9`, `adjuster.py:62` | README says prices are adjusted for demergers; `ADJUSTING_TYPES` is SPLIT/BONUS/CONSOLIDATION only and demergers are stored as `SCHEME` with no factor (e.g. TMPV/TATAMOTORS 2025-10-14: 660.75 -> 395.45 with `adj_factor` 1.0; RELIANCE 2023-07-20, ITC 2025-01-06). A manual override with `type=DEMERGER` would be silently ignored. -> section 5. | M | fixed `78d9207` (owner decision taken, see 6) |
| 12 | `parsers.py:190-219` | `merge_delivery` docstring says BE/BZ use volume as delivery "when the delivery report does not list them"; the code always overwrites with volume / 100 %. -> section 5 (doc vs behaviour). | L | owner decision (5.2) |
| 13 | `storage.py:99-108` | `Store.rename` onto an existing file concatenates `[new, old]` and keeps the *old* row on duplicate dates. Only reachable when a file exists for a key that is not an entity. | L | owner decision (5.3) |

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
  tmp + replace. Exceptions were #2, #4 and #7 (now fixed). The actions CSV, `unconfirmed_actions.csv` and
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

Not changed, because fixing them changes existing outputs or needs a design choice.

1. **Aliased symbols in one call (#10).** `load_data(["INFY", "INFOSYSTCH"])` returns every INFY
   row twice and `load_wide(symbols=[...])` with such a pair raises "Index contains duplicate
   entries". De-duplicating the resolved keys (keeping order) is a one-line change in
   `load_data`, but it changes the output of those calls.
2. **`merge_delivery` doc vs behaviour (#12).** BE/BZ rows always get `deliv_qty = volume`,
   `deliv_pct = 100` even when the delivery report lists them; the docstring says only when they
   are missing. Either the docstring or the behaviour should change (changing behaviour rewrites
   history on the next `--redo-date` only).
3. **`Store.rename` onto an existing file (#13)** keeps the old file's row when both have the same
   date. Only reachable when a file exists for a key that is not an entity; which side should
   win is a policy choice.
4. **Same ISIN under two symbols on one day (#9)** would abort the sync with `KeyError`. Not seen
   in NSE data; a fix has to decide which symbol becomes the entity's key that day.
5. **Index files duplicate their predecessors.** `IndexStore.append` reads the merged canonical
   history, so `nifty_50.parquet` also holds the S&P CNX Nifty / CNX Nifty rows (3,618 rows, 911
   of them copies). Reads de-duplicate, so outputs are right; cleaning it up rewrites files.
6. **Lint configuration.** There is no ruff config in the repo, so `ruff check .` uses each
   installed version's defaults (0.16/0.17 here enable pyupgrade, blind-except, datetime-tz rules).
   Suggest committing a `[tool.ruff]` section (e.g. `E4,E7,E9,F,B,I`) and running ruff + pytest in
   a GitHub Action.
7. **Holidays around New Year.** Only the current year's NSE list is kept, so `healthcheck` can
   count last December's holidays as "days behind" for a few days in January (sync itself recovers
   by inferring them).

## 6. Work log

### Safety net (Step 2)

* `tests/` (164 tests, offline, ~100 s): fixture files in NSE's real formats (old and UDiFF
  bhavcopy, sec_bhavdata_full, MTO, ind_close_all, PR bc file), a synthetic market built through
  the real ingest/adjust/breadth code (split, bonus announced under an old symbol, rename,
  delisting, SME issue, SME->main migration, shared symbol, renamed index), a fake NSE server for
  `run_sync` (holidays, 18:00 IST cutoff, `--force`, `--redo-date`, recovery, delivery backfill,
  CLI exit codes) and a scripted HTTP session for the scraper (retries, 404 window, throttling).
  `tests/_repoint.py` redirects every data path to a temp dir without touching production code;
  any real HTTP request fails the test.
* Golden outputs: `tests/golden/golden.py` digests 29 calls (load_wide/load_data/load_symbol for
  20 current, renamed, delisted and SME symbols and for the whole universe, indices incl. the old
  index name, breadth, actions, symbol master, adj_factor recomputed for all 4,583 securities, a
  full `compute_breadth`) on `data` commit `fcb3714`. Recomputing adj_factor from the actions table
  reproduces every stored value. Every commit after the suite was checked against these digests;
  all match.
* Ruff baseline (`ruff check .`, ruff 0.16.9 defaults, original code): 194 findings, of which the
  classic core set (E4/E7/E9/F) is 2 (unused imports).

### Bugs fixed (Step 3) -- each with a test that failed first

| Commit | Fix | Test |
|--------|-----|------|
| `dd15bfa` | #1 deep-copy DEFAULT_META in load_meta | `test_storage.py::test_load_meta_defaults_are_not_shared` |
| `04fffff` | #2 save_meta copies the backup, meta.json never absent | `test_storage.py::test_save_meta_never_leaves_meta_json_missing` |
| `a6e9c14` | #3 resolve_day never moves an entity back in time; master as of last_synced | `test_symbol_master.py::test_resolving_an_older_day_again_keeps_latest_state`, `test_sync.py::test_redo_date_keeps_symbol_master_current` |
| `1574a73` | #4 atomic zip extraction | `test_scraper.py::test_failed_extraction_keeps_previous_file` |
| `0ca8de9` | #5 weekend Muhurat session is a candidate day | `test_sync.py::test_muhurat_session_on_a_weekend_is_a_candidate` |
| `4bd2b5e` | #6 resolver saved before the day is marked committed | `test_sync.py::test_resolver_saved_before_day_is_marked_committed` |
| `9f7aa99` | #7 atomic symbol master / history map / missing dates | `test_symbol_master.py::test_master_write_failure_keeps_previous_files`, `test_scraper.py::test_missing_dates_write_is_atomic` |
| `58d2bc0` | #8 close failed streamed responses, name the last error | `test_scraper.py::test_retried_responses_are_closed`, `::test_give_up_message_names_last_status` |

None of these changes any golden output. #5 changes which dates sync *attempts* (a weekend
"Laxmi Pujan" day, next one 08-Nov-2026); if NSE publishes nothing it is recorded as a holiday.

### Optimisations (Step 4) -- golden outputs identical

Measured in this container (4 vCPU, local SSD) on the `fcb3714` snapshot. Before = code after the
bug fixes; same machine, same session.

| Call | Before | After | Change |
|------|-------:|------:|--------|
| `load_wide("close", start_date="2024-01-01")` (all main board, incl. delisted) | 30.0 s | 3.8 s | `101d12a` column projection, `fec17bc` one pyarrow scan |
| `load_wide("volume", start_date="2025-01-01", include_delisted=False)` | 23.0 s | 2.7 s | same |
| `load_wide("close", adjusted=False, start_date="2026-06-01", include_sme=True)` | 33.0 s | 3.1 s | same |
| `load_data(start_date="2025-01-01", columns=[date, symbol, close, deliv_qty])` | 26.2 s | 2.8 s | same |
| `load_data(start_date="2026-01-01", include_sme=True)` (all 15 columns: file by file) | 30.8 s | 25.7 s | key/symbol set once after concat |
| `load_symbol("INFOSYSTCH")`, repeated | 78 ms | 32 ms | `a7dcb04` resolver cache |
| `load_actions("IRCTC")`, repeated | 55 ms | 12 ms | same |
| `list_indices()` | 1.54 s | 0.82 s | `903556d` one read per index file |
| `compute_breadth` (end of every sync) | 40.8 s | 12.7 s | `f7c06ac` scan + numpy fill |
| ingest one day, 3,412 securities (`ingest_dates`) | 115.6 s | 41.6 s | `15c5420` worker processes |
| `Store.remove_dates` over all files (`--redo-date`) | 87.9 s | 30.5 s | same |

Notes
* Threads were measured first and were slower for both reads and appends (per-file cost is
  pandas Python overhead under the GIL), so the loader uses one pyarrow dataset scan (C++
  threads) and sync uses `spawn` worker processes. The scan is used only when it provably gives
  the file-by-file result (numeric/date columns, identical Arrow types and pandas metadata in
  every file, no duplicate keys, tz-naive bounds, ordered batches) and otherwise falls back.
  The worker pool is used only by the `sync.py` command or with `NSE_SYNC_WORKERS`, because
  `spawn` re-imports the caller's `__main__`; `NSE_SYNC_WORKERS=1` disables it.
* Day re-ingest benchmark: the last day (2026-10-26) was removed from a copy and re-ingested from
  bhavcopy/delivery files rebuilt from the stored rows; the re-ingested rows equal the originals
  and all 4,583 files written by the worker pool are byte-identical to the serial run.
* Not changed: `Store.last_dates` (unused), `apply_adjustments` (touches few keys per sync),
  `load_data` with all columns (string columns vary in type across files: 400 files store an
  all-null `isin` as Arrow `null`, which pandas reads as object, so they must be read one by one).

### Demerger adjustment (owner decision taken: #11 fixed in `78d9207`)

* Demergers are a new adjusting type `DEMERGER`. Factor = the parent's previous close / its ex-date
  open (the price NSE's special pre-open session discovers; NSE does not adjust `prev_close` for
  demergers), divided by any split/bonus/consolidation applied on the same trading day. Applied
  only between 1.02x and 25x and with trading within 7 days of the recorded date; otherwise left
  without a factor (not applied, not reported as a warning). A manual override with
  `type=DEMERGER` wins, for exact cost-of-acquisition ratios.
* Volumes are multiplied by the factor like for every other adjusting action, as the README's model
  states (turnover stays invariant). A demerger does not change the share count, so read adjusted
  volume around a demerger with that in mind.
* Existing tables typed demergers `SCHEME`; rebuilding the table re-types them, so the next sync that
  ingests a day (or a `--redo-date`) migrates the data and rewrites the affected securities.
* Simulated on a copy of `fcb3714`: exactly the 120 demerger rows are re-typed, 93 get a factor
  (e.g. TMPV 2025-10-14 1.6519, RELIANCE 2023-07-20 1.1015, ITC 2025-01-06 1.0571, SIEMENS
  2025-04-07 2.0115), 27 stay unapplied (gap < 2 %, already absorbed by a same-day split such as
  SUVEN and KESORAMIND, or no trading near the date). Exactly the 88 securities with an applied
  demerger are rewritten; no other file changes; no security gets two demergers within 35 days.
  TMPV's adjusted close now goes 400.00 -> 395.45 (-1.1 %) on the ex-date instead of -40 %.
* Deliberate golden differences after migration (14 of 29 entries): every adjusted/`adj_factor`
  output that includes a demerged security before its ex-date (load_data/load_wide entries, e.g.
  29 of the columns of `load_wide("close", start_date="2024-01-01")`, all demerged), the actions
  table, the recomputed factors and breadth (5,214 of 5,407 days, because the A/D line and new-high
  counts are cumulative). Raw prices, indices and the symbol master are unchanged. On the
  unmigrated snapshot all 29 digests still match; re-record them after publishing a migrated
  snapshot.
* Unchanged existing warning: KMSUGAR's manual 2010 split (stock suspended across the ex-date)
  fails the continuity check with the snapshot's own table too; it is reported again when the
  migration re-verifies KMSUGAR (demerger 2026-10-01).

### Live check (Step 5)

`python sync.py --redo-date 2026-10-23` on a copy of the snapshot (code + data in a scratch folder):
exit 0 in 1 min 57 s. NSE served all four reports from nsearchives and the corporate-actions API
answered (a bare `curl` to www.nseindia.com gets HTTP 403 from AkamaiGHost; the client's cookie
bootstrap was accepted). Afterwards 4,582 of 4,583 store files, the symbol master, breadth and all
232 index files are identical to the snapshot; the one difference is CONCORDBIO, whose
`adj_factor` became 2.0 for all history because NSE has since announced a bonus 1:1 ex
2026-11-04 (20 new action rows, none removed). `healthcheck.py` on the snapshot and on the synced
copy: HEALTHY, exit 0 (3 trading days not yet synced, as expected for a 2026-10-26 snapshot).

### Final state (Step 6)

* `python -m pytest -q`: 163 passed, 1 skipped (the golden test without `NSE_GOLDEN_DATA`).
* Golden check on `fcb3714`: all 29 digests match.
* `ruff check .` (0.16.9): core E4/E7/E9/F 2 -> 0; tests clean. Total on the modules 194 -> 208:
  +6 UP006 / +9 UP045 annotation-style (new code follows the modules' `typing.Optional`/`List`
  style), +1 BLE001 (`IndexStore` keeps the old broad `except Exception` semantics), -2 F401.
* Public API, CLI flags/exit codes, on-disk layout, Parquet schemas, meta.json and
  manual_overrides.csv formats unchanged (manual_overrides.csv now also accepts `type=DEMERGER`).
  Deliberate output change: demerger adjustment (above). New, optional: env var `NSE_SYNC_WORKERS`,
  `Store.scan`, `storage.allow_parallel_rewrites`.
