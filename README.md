# Idempotent ingestion and safe replay

Assignment 1 of the Senior Data Engineer take-home. A small Python pipeline (pandas, with SQLite for state) that loads a bank's daily transaction files into one clean dataset. Bad rows go to a quarantine file with a reason. Duplicates and the accidental re-delivery are collapsed without losing real repeat transactions. The late correction file is merged into the day it belongs to, and a reconciliation summary shows what happened to every row. Running it again, or feeding it a file twice, changes nothing.

## Results on the sample files

- 555 rows read, 361 loaded (183 for 2026-08-01, 178 for 2026-08-02).
- 6 quarantined: the same three bad rows (blank `ref_no`, debit and credit both filled, neither filled) in both batch 1 and batch 2.
- 188 duplicates removed: 8 repeated inside a file, and 180 because batch 2 re-sends all of day 1.
- Backfill: 3 rows added to 2026-08-01 (180 to 183 rows), credits +97,000.00, debits +18,500.00, net +78,500.00.

The full breakdown is in [`output/reconciliation_summary.md`](output/reconciliation_summary.md).

## Running it

Needs Python 3.10 or newer. Developed on 3.13 and also run on 3.12, with pandas from 2.2 to 3.1.

```
python -m venv .venv
.venv\Scripts\activate            # macOS/Linux: source .venv/bin/activate
pip install -r requirements-dev.txt
python run_pipeline.py
pytest
```

`python run_pipeline.py` reads `sample_data/assignment-1-pipeline`, keeps its state in `state/` and writes to `output/`. The options are `--config`, `--input`, `--output`, `--state` and `--reset` (throw the state away and rebuild from the input folder). Run it a second time and every file is reported as skipped. The exit code is 1 if a file can't be read or a reconciliation check fails, and 2 for a bad config or input folder.

## What is in the data

- Batch 2 contains all of day 1 again, but no re-sent row is identical to its original: the date goes from `01-08-2026` to `1/08/2026` and some times lose their leading zero. Comparing raw text finds none of them (it would leave 180 extra rows), a strict `%d-%m-%Y` parse rejects all of batch 2, and pandas' default `to_datetime` reads `1/08/2026` as 8 January.
- Amounts have inconsistent precision (`5000`, `21400.6`, `21400.65`).
- Batch 1 has four exact duplicate rows (lines 182-185), repeated in batch 2.
- Batch 1 ends with three bad rows (lines 186-188), also repeated in batch 2. There are no impossible dates in the files, so `INVALID_DATE` is implemented and tested but never fires on this data.
- Genuine repeats exist, such as two Zomato orders on one day for one account. A key of (account, date, description) would throw away 45 real transactions.
- `account_type` differs between rows of the same account (25 of 40 accounts), and `balance` is a running figure that follows file order, not time order.
- The late file has three rows, all dated 2026-08-01, with new references.

## Design

### Deduplication key

Account, date, time, `ref_no`, direction (debit or credit) and amount, compared after dates, times and amounts are normalised. Two rows with the same key are the same transaction. A different reference, time or amount makes it a different one, which is what keeps genuine repeats.

| Key | Rows kept (the right answer is 361) |
|---|---|
| raw text, no normalising | 541, the re-delivery is not collapsed |
| account + date + description | 316, 45 genuine transactions lost |
| `ref_no` only | 361 here, but every blank or reused reference would merge into one row |
| the key above | 361 |

`balance`, `description` and `account_type` are left out on purpose. Balance is a running figure that changes when a backfill lands, description is free text, and account type changes between files for the same account, so a restated copy of a transaction should still count as the same one.

### Quarantine

Each rejected row keeps its file, line number, reason and the line as received. Reasons: `MALFORMED_ROW` (wrong number of fields), `MISSING_REF_NO`, `MISSING_ACCOUNT_NUMBER`, `INVALID_DATE`, `INVALID_TIME`, `INVALID_AMOUNT` (not a plain number, negative, or more than two decimals), `INVALID_BALANCE`, `BOTH_DEBIT_AND_CREDIT` and `NO_DEBIT_OR_CREDIT`. A side counts as filled only if its amount is not zero. Files are read with Python's `csv` module because `pandas.read_csv` raises on a line with too many fields or, with `on_bad_lines="skip"`, quietly drops it.

### Idempotency

Three things work together:

1. Every file is recorded by the SHA-256 of its bytes, so the same file fed again, under any name, is skipped.
2. A row's id is a hash of its key and the store has a unique constraint on it, so a replay cannot insert a duplicate even when the file check doesn't apply. This is what handles batch 2, whose bytes differ from batch 1.
3. Each file is loaded in one SQLite transaction that also checks every row is accounted for (loaded, quarantined or duplicate), so a crash leaves nothing half-loaded.

The outputs have no timestamps and a fixed order, so a rerun, or a rebuild with `--reset`, produces byte-identical files.

### Backfill

A new row whose date is at least one day older than the batch date in its file name is flagged `BACKFILL` and stored under its own business date, so it sits inside day 1 instead of being added as day 4. It goes through the same validation and dedup as every other row, so rows the correction repeats are skipped. The report shows the date before and after (rows, debits, credits) and the effect per account.

### Reconciliation

`reconciliation_summary.json` and `.md` give rows read, loaded and quarantined (by reason), duplicates (inside a file and re-sent), per-file numbers, the backfill effect, the dataset by day and a couple of warnings. Two checks compare the ledger with the stored rows (row counts per file, and debit/credit totals); if either fails the run exits with code 1. `duplicates_removed.csv` lists every collapsed row and the row it duplicates.

### Config

Everything specific to a bank is in YAML: file pattern, delimiter, column names, date and amount formats, required fields, the dedup key and how the batch date is read from the file name. `config/bank_b_example.yaml` loads a differently shaped bank (semicolons, other headers, ISO dates, `1,250.00` amounts, no time column, a different key) with no code change:

```
python run_pipeline.py --config config/bank_b_example.yaml --input sample_data/bank_b_example --output output_bank_b --state state_bank_b
```

Use a separate `--state` for each source.

### Why pandas

The brief allows pandas or PySpark. The data here is hundreds of rows, and even a real daily extract (1e5 to 1e7 rows) fits in memory on one machine, so Spark would add start-up and cluster overhead for nothing. The logic (normalise, validate, anti-join on a key, merge) maps straight onto Spark and a Delta `MERGE` if volumes grow. Money is `Decimal`, never float. SQLite holds the state because it gives a unique constraint and an atomic transaction per file with nothing extra to install.

## Assumptions

- `1/08/2026` is 1 August, day first: batch 2 is named 2026-08-02 and those rows are copies of batch 1's `01-08-2026` rows.
- A blank `ref_no` is a bad row, because the reference is what ties a row to the bank's records. To load such rows anyway, remove `ref_no` from `validation.required`.
- The date in the file name is the batch date. A file name without one is never treated as a backfill.
- Times are the bank's local time. No currency is given, so amounts are plain decimals.
- A negative debit or credit is invalid. `balance` is only checked to be a number.
- A changed file with the same name is a new delivery: the SHA-256 decides what has been loaded, not the name.
- A bad row sent again in a later file is quarantined again for that file.

## Limits, and what I would do next

- If a bank amends a transaction (same `ref_no`, new amount) it gets a different key, so both rows load. The report lists such pairs but nothing merges them. Proper handling needs versioned rows.
- `balance` is the bank's snapshot and is not recomputed after a backfill.
- Validation works value by value in Python, which is fine for millions of rows. Beyond that I would vectorise it or move to Spark with a Delta `MERGE` on `txn_id`, partitioned by date. Whole files are read into memory and SQLite has one writer, so this is a single-machine design.
- Next steps: per-account balance continuity checks, alerting on the quarantine rate, scheduling on file arrival, and a CI job that runs the tests.

## Layout

```
run_pipeline.py        entry point
config/                bank_a.yaml, bank_b_example.yaml
src/txn_pipeline/      extract, normalize, validate, dedup, store, pipeline, report, cli
tests/                 117 tests, including one set on the three delivered files
sample_data/           the delivered files (byte for byte) and the Bank B example
output/                canonical_transactions.csv, quarantine.csv, duplicates_removed.csv,
                       reconciliation_summary.json and .md
```
