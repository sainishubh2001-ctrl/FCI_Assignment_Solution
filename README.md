# Idempotent Ingestion & Safe Replay

**Assignment chosen:** *Idempotent Ingestion & Safe Replay* (Senior Data Engineer take-home; it is the only assignment in the brief).

A Python pipeline (pandas for the data work, SQLite for state) that loads a bank's daily transaction drops into one canonical dataset. It quarantines malformed rows with reason codes, collapses duplicates and whole-file re-deliveries without ever discarding a legitimate repeat, merges a late correction file into the business day it belongs to, and proves all of it with a reconciliation summary. Running it twice, or re-feeding any file, changes nothing.

**Start here:** [`output/reconciliation_summary.md`](output/reconciliation_summary.md) (the numbers), then [`output/`](output) (the datasets).

## Results on the provided files

| | |
|---|---|
| Rows read | **555** (187 + 365 + 3) |
| Rows loaded (canonical) | **361** (183 for 2026-08-01, 178 for 2026-08-02) |
| Rows quarantined | **6**: 3 distinct bad records, each delivered twice (`MISSING_REF_NO`, `BOTH_DEBIT_AND_CREDIT`, `NO_DEBIT_OR_CREDIT`) |
| Duplicates removed | **188**: 8 repeated inside a file + 180 rows of the day-1 re-delivery |
| Backfill net effect | **+3 rows** into 2026-08-01 (180 → 183): credits +97,000.00, debits +18,500.00, net +78,500.00 |
| Identity | 555 = 361 + 6 + 188, and 7 independent control checks pass |

## Quick start

Requires Python 3.10+ (developed on 3.13, also verified on 3.12; pandas 2.2 through 3.1).

```powershell
# Windows PowerShell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements-dev.txt
python run_pipeline.py          # reads sample_data/assignment-1-pipeline, writes output/
pytest                          # 182 tests
```

```bash
# macOS / Linux
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements-dev.txt
python run_pipeline.py
pytest
```

Run it a second time: every file reports `SKIPPED` and `git status` shows `output/` untouched.

| Option | Default | Purpose |
|---|---|---|
| `--config` | `config/bank_a.yaml` | source definition: columns, formats, dedup key, rules |
| `--input` | `sample_data/assignment-1-pipeline` | folder with the delivered files |
| `--output` | `output` | published datasets and reconciliation summary |
| `--state` | `state` | SQLite ledger + canonical store (git-ignored, rebuildable) |
| `--reset` | off | delete the state first and rebuild everything from the input folder |

Exit codes: `0` success, `1` a file failed or a reconciliation control check failed, `2` bad config or input folder. After `pip install -e .` it is also available as `txn-ingest` and `python -m txn_pipeline`.

## What the data actually contains

I profiled the raw files before designing anything (`NOTES.txt` says they are as delivered, so nothing was hand-cleaned and they are tracked byte-for-byte).

1. **The re-delivery is disguised.** `batch_2026-08-02.csv` is all of day 1 again (187 rows, same order) plus 178 new day-2 rows. But **no re-sent row is byte-identical to its original**: the date goes from `01-08-2026` to `1/08/2026` on all 187, and 78 times lose zero-padding (`9:00:00`). Consequences: dedup on raw text keeps 180 phantom rows, a strict `%d-%m-%Y` parse rejects all 365 rows of batch 2, and pandas' default `to_datetime` silently reads `1/08/2026` as 8 January.
2. **Amounts are written with inconsistent precision** (`5000`, `21400.6`, `21400.65`), so they are compared as exact decimals, never as text or floats.
3. **4 exact duplicates inside batch 1** (lines 182-185), re-sent again inside batch 2.
4. **3 planted bad rows** (lines 186-188, re-sent in batch 2): a blank `ref_no`; a `SUSPENSE ENTRY` with debit = credit = 1,200; a POS row with neither. There are no impossible calendar dates in the files, so the brief's "bad dates" case is latent. It is implemented and tested (`INVALID_DATE`, e.g. `31-02-2026`).
5. **Legitimate repeats exist.** Two Zomato orders on the same day on the same account have different times, amounts and references. A coarse key would silently destroy 45 real transactions.
6. **`account_type` drifts** (25 of 40 accounts show more than one value) and **`balance`** is a running snapshot in *file* order, not time order. Neither belongs in a key.
7. **The late file** holds 3 rows, all dated 2026-08-01, with evening times (21:10-23:41) and new references (`REF900001-3`): pure additions.

## How it works

```
 delivered file
      │  SHA-256 already in the ledger? ── yes ──► SKIPPED (no-op)
      ▼ no
 extract     csv module, raw line + line number kept; ragged lines become MALFORMED_ROW
      ▼
 normalise   day-first dates, H:MM:SS times, exact 2-dp Decimal, trimmed + upper-cased ids
      ▼
 validate    MISSING_<FIELD> · INVALID_DATE/TIME/AMOUNT/BALANCE · BOTH_DEBIT_AND_CREDIT · NO_DEBIT_OR_CREDIT
      │                                                   └─► quarantine (reason, file, line, raw text)
      ▼
 dedup       txn_id = hash(source + key fields); repeats in the file / already stored ─► duplicates_removed
      ▼
 merge       ONE SQLite transaction: insert new rows (UNIQUE txn_id), tag REGULAR / BACKFILL,
      │      measure the backfill impact, row-accounting self-check (rollback on any mismatch)
      ▼
 publish     canonical_transactions.csv · quarantine.csv · duplicates_removed.csv · reconciliation_summary.{json,md}
```

### Why pandas (and SQLite)

The brief allows pandas or PySpark. I chose **pandas**: the data (555 rows here, and realistically 10⁵-10⁷ per daily extract) fits comfortably in memory on one machine; Spark would add JVM start-up and cluster overhead for no benefit; and a reviewer can run this with a plain `pip install`. The logic (normalise, validate, anti-join on a deterministic key, merge) maps one-to-one onto Spark and a Delta/Iceberg `MERGE` if volumes grow. Money is `Decimal` end to end, never float.

**SQLite** (standard library) is the state store because it gives the two properties idempotency needs: a `UNIQUE` key enforced by the storage engine, and atomic multi-table transactions. The published CSVs are exports of that state, so everything stays inspectable.

### Deduplication key

```
account_number + txn_date + txn_time + ref_no + direction (debit/credit) + amount      (after normalisation)
```

Hashed with the source name into a stable `txn_id`. Rows with the same key are the same transaction; any difference in reference, time or amount makes a different one.

| Candidate key | Distinct rows on the valid data (correct = 361) |
|---|---|
| Raw-text hash, no normalisation | 541: re-delivery **not collapsed** (+180) |
| `(account, date, description)` | 316: **45 legitimate transactions destroyed** |
| `ref_no` only | 361 here, but every blank or reused reference would collapse into one row |
| **Composite key above, after normalisation** | **361**, and safe for blank or non-unique references |

**Deliberately excluded:** `balance` (a running snapshot that is restated when a backfill lands), `description` (free text) and `account_type` (drifts between deliveries). A restated copy of the same transaction therefore collapses instead of becoming a phantom new row.

**Trade-off:** if a bank *amends* a transaction (same `ref_no`, different amount), the amended row has a different key and is loaded as a separate row. The pipeline does not guess which version is right; it reports "same `ref_no` on different transactions" under data-quality warnings so a human can decide (currently 0). Real amendment handling needs versioned rows; see "With more time".

### Quarantine: nothing is dropped, nothing crashes

| Reason code | Rule |
|---|---|
| `MALFORMED_ROW` | wrong number of fields. The file is read with the `csv` module because `pandas.read_csv` would either raise on such a line or, with `on_bad_lines='skip'`, silently drop it |
| `MISSING_REF_NO`, `MISSING_ACCOUNT_NUMBER` | blank value in a field listed under `validation.required` |
| `INVALID_DATE` / `INVALID_TIME` | unparseable, not a real calendar date, or outside the year range |
| `INVALID_AMOUNT` / `INVALID_BALANCE` | not a plain decimal, negative debit/credit, or more than 2 decimal places (never rounded silently) |
| `BOTH_DEBIT_AND_CREDIT` / `NO_DEBIT_OR_CREDIT` | a side counts as populated only if it carries a non-zero amount, so a feed that writes `0.00` for the unused side is not misread |

`quarantine.csv` keeps the file, line number, raw values, raw text, the primary `reason_code`, every failing rule in `all_reasons` and a format-independent `record_fingerprint` (so the same bad record re-sent later can be recognised: 6 rows = 3 distinct records). Quarantine is an audit log, not a work queue: a corrected row arrives in a later delivery and loads normally.

### Idempotency

Four layers, so no single one has to be trusted:

1. **Deterministic outputs.** No timestamps or randomness in anything published; stable ordering; LF line endings. Files are only rewritten when their content changes.
2. **Row level.** The key is a `UNIQUE` primary key and new rows are found with an anti-join, so a replay cannot create a duplicate even if every other safeguard failed.
3. **File level.** A SHA-256 ledger of file bytes makes re-feeding an identical file a logged no-op, whatever it is called.
4. **Atomic per file.** One SQLite transaction covers canonical rows, quarantine, duplicates and the ledger entry, and ends with a row-accounting check (`read = loaded + quarantined + duplicates`). A crash or mismatch rolls back everything; a rerun then completes cleanly.

| Scenario | Outcome (all covered by tests) |
|---|---|
| Run twice | every file `SKIPPED`; output files byte-identical and not even rewritten |
| Same file re-fed under another name | `SKIPPED` (same bytes); nothing changes |
| Same rows, different bytes (like batch 2's day 1) | processed as a new delivery; every row recognised as already loaded; canonical unchanged; listed in `duplicates_removed.csv` |
| Crash in the middle of a file | rolled back; no partial state; rerun succeeds |
| State deleted (`--reset`) | rebuilt from the input folder; outputs byte-identical |
| Files arrive in a different order | same canonical content; only the lineage columns (`batch_file`, `source_line`) reflect which delivery came first |

### Backfill

A new row whose `txn_date` is at least `late_after_days` (config, default 1) older than the batch date in its file name is a **backfill**: tagged `load_type=BACKFILL` and merged into **its own business-date partition**, never appended as a "day 4". It goes through exactly the same validation and dedup as every other row, so a correction that repeats already-loaded rows skips them instead of duplicating them.

The impact is measured on the table itself, before and after the insert, and asserted to equal the rows added (otherwise the file's transaction rolls back). For the sample: day 1 goes from 180 rows (debits 3,302,528.75; credits 1,915,422.42) to 183 rows (3,321,028.75; 2,012,422.42); by account, `AC100000002` −18,500.00, `AC100000005` +15,000.00, `AC100000009` +82,000.00; no later day is touched. The report also shows that batch 2's 184 late-dated rows (the re-delivery) had a net effect of zero.

### Reconciliation

`reconciliation_summary.json` (machine-readable) and `.md` (human) are rebuilt from the ledger and contain no timestamps. They report rows read, loaded, quarantined by reason, duplicates removed (in-file vs re-delivered), per-file figures, late-dated rows, the backfill before/after, the canonical dataset by business date, data-quality warnings (`account_type` drift, shared `ref_no`) and **control checks** that cross-check the ledger against the rows physically stored: per-file row accounting, ledger counters vs rows in each table, canonical count, control totals of debits/credits, backfill partition arithmetic, unique ids, positive amounts. Any failure makes the CLI exit non-zero (tests tamper with the store to prove it).

### Configuration: onboarding another bank

Everything that varies between banks is in YAML ([`config/bank_a.yaml`](config/bank_a.yaml)): file pattern, delimiter, encoding, column mapping, date/time formats, decimal and thousands separators, required fields, the **dedup key**, the file-name date pattern and the lateness threshold. [`config/bank_b_example.yaml`](config/bank_b_example.yaml) onboards a deliberately different bank (semicolons, other headers, ISO dates, `1,250.00` amounts, no time column, a different key and file naming) with **no code change**:

```bash
python run_pipeline.py --config config/bank_b_example.yaml --input sample_data/bank_b_example --output output_bank_b --state state_bank_b
```

Two sources can share one state database without colliding (`txn_id` is source-scoped), and identical file bytes are refused under a second source rather than silently skipped.

## Tests

`pytest` runs 182 tests: normalisation, validation rules, dedup (re-delivery collapse, legitimate repeats, restated attributes, blank references), idempotency (reruns, renamed re-feeds, rebuild equals incremental, out-of-order arrival, crash rollback), backfill (merge, already-loaded rows, thresholds, integrity assertion), reconciliation (including tamper detection), the CLI, a second bank, and a golden test on the three delivered files. Outputs are byte-identical on pandas 2.2.3 / 2.3.3 / 3.1.1.

To show the suite really would catch a regression, `python tools/mutation_check.py` breaks the dedup and backfill logic 11 ways in a scratch copy (key drops `txn_time`, key adds `balance` or `account_type`, coarse key, `ref_no`-only key, slash dates rejected, time/amount not normalised, store check disabled, file ledger ignored, lateness never detected) and confirms the suite fails each time: 11/11 caught.

## Assumptions (where the brief was ambiguous)

- **`1/08/2026` is day-first** (1 August): batch 2 is named 2026-08-02 and its `1/08/2026` rows are an exact copy of batch 1's `01-08-2026` rows. In this config slash dates are only ever read day-first.
- **A blank `ref_no` is malformed** (`MISSING_REF_NO`), not loaded. It is the reference that ties a row to the bank's records, so a row without it cannot be traced, matched or safely re-deduplicated. Quarantine is not deletion: it is fully recorded and recoverable. To load such rows instead, remove `ref_no` from `validation.required`; distinct blank-reference rows still stay distinct because the key includes the other fields (covered by a test).
- **File-name date = batch date.** It drives processing order and lateness. A file without one is never treated as a backfill.
- **Times are the bank's local time**, with no timezone conversion; no currency is given, so amounts are plain decimals.
- **Negative debit/credit is invalid** (no reversal semantics assumed); `balance` is informational (validated as a number, not used for identity).
- **A changed file with the same name is a new delivery**; the SHA-256 ledger, not the name, decides what has been loaded.
- **Quarantined records are recorded per delivery**, so a bad row re-sent in a later file appears again with its own lineage.

## Trade-offs and known limits

- Amended transactions (same reference, new amount) are flagged, not merged (see above).
- `balance` is the source's snapshot. After a backfill the balances on the later rows of that account are not recomputed; a downstream consumer that needs running balances should derive them.
- Validation parses value by value in Python (clear and unit-testable, with cached date/time parsing). Fine to millions of rows; beyond that I would move it to vectorised columns or Spark.
- Whole files are read into memory and SQLite has a single writer: right for one machine, not for a distributed load.
- The same transaction first seen in a different order keeps the same content but a different `batch_file` / `source_line` lineage.

## With more time

- Real amendment handling: versioned rows (SCD2) with an explicit supersede rule and an audit trail.
- Optional balance-continuity checks per account as a data-quality signal.
- Run on Spark with a Delta/Iceberg `MERGE` keyed on `txn_id`, partitioned by `txn_date`, so a backfill rewrites only the affected partitions.
- Orchestration with file-arrival triggers, quarantine-rate alerting and SLAs, schema-contract checks, and CI running the suite plus the mutation check.
- Property-based tests (e.g. Hypothesis) for "any order and any replay of deliveries gives the same canonical set".

## Repository layout

```
run_pipeline.py                 entry point
config/bank_a.yaml              source definition for the sample feed
config/bank_b_example.yaml      a second bank, config only
src/txn_pipeline/               config · extract · normalize · validate · dedup · store (SQLite) · pipeline · reconcile · export · cli
tests/                          182 tests (unit, scenario, golden end-to-end)
tools/mutation_check.py         proves the tests catch dedup/backfill regressions
sample_data/                    the three delivered files + NOTES.txt (byte-for-byte), and the Bank B example
output/                         published deliverables (regenerated by run_pipeline.py)
state/                          SQLite ledger and canonical store (git-ignored)
```

| Output file | Contents |
|---|---|
| `canonical_transactions.csv` | `txn_id, source, account_number, txn_date, txn_time, description, ref_no, direction, amount, balance, account_type, load_type, batch_file, source_line` |
| `quarantine.csv` | `source, batch_file, source_line, reason_code, all_reasons, reason_detail`, the raw values as received, `raw_line`, `record_fingerprint` |
| `duplicates_removed.csv` | every collapsed row with `WITHIN_FILE` / `ALREADY_LOADED` and the canonical row it duplicates (`kept_batch_file`, `kept_source_line`) |
| `reconciliation_summary.json` / `.md` | the reconciliation, machine- and human-readable |
