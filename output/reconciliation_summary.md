# Reconciliation summary: bank_a

Built from the ingestion ledger. There are no run timestamps in it, so re-running the pipeline never changes this file.
Dedup key: `account_number, txn_date, txn_time, ref_no, direction, amount`.

|  |  |
|---|---|
| Files loaded | 3 |
| Rows read | 555 |
| Rows loaded | 361 |
| Rows quarantined | 6 |
| Duplicates removed | 188 (8 repeated inside a file, 180 re-sent rows that were already loaded) |
| Backfill | batch_2026-08-04_late_correction.csv: +3 row(s) into 2026-08-01 (credits +97,000.00, debits +18,500.00, net +78,500.00) |

555 read = 361 loaded + 6 quarantined + 188 duplicates removed

## By file

| File | Batch date | Read | Loaded | Quarantined | Duplicates (in file / already loaded) | Backfilled |
|---|---|---|---|---|---|---|
| batch_2026-08-01.csv | 2026-08-01 | 187 | 180 | 3 | 4 (4 / 0) | 0 |
| batch_2026-08-02.csv | 2026-08-02 | 365 | 178 | 3 | 184 (4 / 180) | 0 |
| batch_2026-08-04_late_correction.csv | 2026-08-04 | 3 | 3 | 0 | 0 (0 / 0) | 3 |

## Quarantined, by reason

| Reason | Rows |
|---|---|
| BOTH_DEBIT_AND_CREDIT | 2 |
| MISSING_REF_NO | 2 |
| NO_DEBIT_OR_CREDIT | 2 |

A bad row that is sent again in a later file is quarantined again for that file, so these counts include repeats.

## Backfill from batch_2026-08-04_late_correction.csv

Business date 2026-08-01, 3 day(s) before the file's batch date:

|  | Rows | Debits | Credits | Net |
|---|---|---|---|---|
| Before | 180 | 3,302,528.75 | 1,915,422.42 | -1,387,106.33 |
| Added | +3 | +18,500.00 | +97,000.00 | +78,500.00 |
| After | 183 | 3,321,028.75 | 2,012,422.42 | -1,308,606.33 |

By account:

| Account | Rows | Debits | Credits | Net |
|---|---|---|---|---|
| AC100000002 | 1 | 18,500.00 | 0.00 | -18,500.00 |
| AC100000005 | 1 | 0.00 | 15,000.00 | 15,000.00 |
| AC100000009 | 1 | 0.00 | 82,000.00 | 82,000.00 |

## Canonical dataset by business date

| Date | Rows | Regular | Backfill | Debits | Credits | Net |
|---|---|---|---|---|---|---|
| 2026-08-01 | 183 | 180 | 3 | 3,321,028.75 | 2,012,422.42 | -1,308,606.33 |
| 2026-08-02 | 178 | 178 | 0 | 3,390,381.64 | 2,099,315.74 | -1,291,065.90 |

## Warnings

- account_type changes between rows for 25 of 40 accounts, so it is not part of the dedup key.
- 0 account/ref_no pair(s) appear on more than one stored transaction.

## Checks

| Check | Result | Detail |
|---|---|---|
| rows read = loaded + quarantined + duplicates, and those rows are stored, for every file | pass | 3 file(s) balance |
| debit and credit totals of each file's stored rows match the totals recorded when it was loaded | pass | match |
