# Reconciliation summary: bank_a

Built from the ingestion ledger. It carries no run timestamps, so re-running the pipeline never changes it.
Dedup key: `account_number, txn_date, txn_time, ref_no, direction, amount`

## 1. Headline

| Metric | Value |
|---|---|
| Files loaded | 3 |
| Rows read | 555 |
| Rows loaded into the canonical dataset | 361 |
| Rows quarantined | 6 (3 distinct bad records) |
| Duplicates removed | 188 (8 repeated inside a file, 180 re-delivered rows that were already loaded) |
| Backfill net effect | `batch_2026-08-04_late_correction.csv`: +3 rows into 2026-08-01 (credits +97,000.00, debits +18,500.00, net +78,500.00) |

**555 read = 361 loaded + 6 quarantined + 188 duplicates removed**

## 2. Per file

| File | Batch date | Read | Loaded | Quarantined | Duplicates (in file / already loaded) | Loaded as backfill |
|---|---|---|---|---|---|---|
| batch_2026-08-01.csv | 2026-08-01 | 187 | 180 | 3 | 4 (4 / 0) | 0 |
| batch_2026-08-02.csv | 2026-08-02 | 365 | 178 | 3 | 184 (4 / 180) | 0 |
| batch_2026-08-04_late_correction.csv | 2026-08-04 | 3 | 3 | 0 | 0 (0 / 0) | 3 |

## 3. Quarantine by reason

| Reason code | Rows | Distinct records |
|---|---|---|
| BOTH_DEBIT_AND_CREDIT | 2 | 1 |
| MISSING_REF_NO | 2 | 1 |
| NO_DEBIT_OR_CREDIT | 2 | 1 |

A bad record that is re-sent in a later file is quarantined again for that delivery, so rows can exceed distinct records. Every quarantined row keeps its file, line number and raw text in `quarantine.csv`.

## 4. Late-dated rows and backfill

Rows whose business date is older than the file's batch date:

| File | Late-dated valid rows | New (backfilled) | Already loaded | Repeated inside file |
|---|---|---|---|---|
| batch_2026-08-02.csv | 184 | 0 | 180 | 4 |
| batch_2026-08-04_late_correction.csv | 3 | 3 | 0 | 0 |

### Backfill from `batch_2026-08-04_late_correction.csv` (batch date 2026-08-04)

Business date 2026-08-01 (3 day(s) late):

|  | Rows | Debits | Credits | Net |
|---|---|---|---|---|
| Before | 180 | 3,302,528.75 | 1,915,422.42 | -1,387,106.33 |
| Backfilled | +3 | +18,500.00 | +97,000.00 | +78,500.00 |
| After | 183 | 3,321,028.75 | 2,012,422.42 | -1,308,606.33 |

By account:

| Account | Rows | Debits | Credits | Net |
|---|---|---|---|---|
| AC100000002 | 1 | 18,500.00 | 0.00 | -18,500.00 |
| AC100000005 | 1 | 0.00 | 15,000.00 | +15,000.00 |
| AC100000009 | 1 | 0.00 | 82,000.00 | +82,000.00 |

Of 3 valid late-dated row(s) in this file, 3 were new and were merged into the business date above; 0 were already loaded and skipped instead of being duplicated, and 0 repeated another row of the same file.

## 5. Canonical dataset by business date

| Date | Rows | Regular | Backfill | Debits | Credits | Net |
|---|---|---|---|---|---|---|
| 2026-08-01 | 183 | 180 | 3 | 3,321,028.75 | 2,012,422.42 | -1,308,606.33 |
| 2026-08-02 | 178 | 178 | 0 | 3,390,381.64 | 2,099,315.74 | -1,291,065.90 |

## 6. Data-quality warnings (informational, nothing is rejected for these)

- **account_type drift:** 25 of 40 accounts carry more than one account_type across rows (account_type is excluded from the dedup key; values are kept as delivered).
- **Same ref_no on different transactions:** 0 (both rows are loaded because their keys differ; review for a possible amendment).

## 7. Control checks

| Check | Result | Detail |
|---|---|---|
| Every file: rows read = loaded + quarantined + duplicates removed | PASS | 3 file(s) balance |
| Ledger counters match the rows physically stored for each file | PASS | loaded, quarantined and duplicate rows all present |
| Canonical row count = rows loaded across all files | PASS | 361 rows |
| Control totals: canonical debits and credits = totals loaded per file | PASS | debits 6711410.39, credits 4111738.16 |
| Backfilled rows landed in their own business-date partitions | PASS | 3 backfilled row(s) |
| No transaction id appears twice in the canonical dataset | PASS | unique |
| Every canonical amount is positive and has a direction | PASS | all positive |

All control checks passed: **yes**
