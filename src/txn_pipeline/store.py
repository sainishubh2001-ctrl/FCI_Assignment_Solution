from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from decimal import Decimal
from pathlib import Path
from typing import Any, Iterator, Sequence

import pandas as pd

SCHEMA = """
CREATE TABLE IF NOT EXISTS batches (
    batch_sha256       TEXT PRIMARY KEY,
    source             TEXT NOT NULL,
    batch_file         TEXT NOT NULL,
    batch_date         TEXT,
    seq                INTEGER NOT NULL,
    rows_read          INTEGER NOT NULL,
    rows_loaded        INTEGER NOT NULL,
    rows_quarantined   INTEGER NOT NULL,
    dup_within_file    INTEGER NOT NULL,
    dup_already_loaded INTEGER NOT NULL,
    blank_lines        INTEGER NOT NULL,
    stats_json         TEXT NOT NULL,
    loaded_at          TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS transactions (
    txn_id         TEXT PRIMARY KEY,
    source         TEXT NOT NULL,
    account_number TEXT NOT NULL,
    txn_date       TEXT NOT NULL,
    txn_time       TEXT,
    description    TEXT,
    ref_no         TEXT,
    direction      TEXT NOT NULL CHECK (direction IN ('DEBIT', 'CREDIT')),
    amount         TEXT NOT NULL,
    balance        TEXT,
    account_type   TEXT,
    batch_sha256   TEXT NOT NULL REFERENCES batches (batch_sha256),
    batch_file     TEXT NOT NULL,
    source_line    INTEGER NOT NULL,
    load_type      TEXT NOT NULL CHECK (load_type IN ('REGULAR', 'BACKFILL'))
);
CREATE INDEX IF NOT EXISTS ix_transactions_date ON transactions (source, txn_date);

CREATE TABLE IF NOT EXISTS quarantine (
    source             TEXT NOT NULL,
    batch_sha256       TEXT NOT NULL REFERENCES batches (batch_sha256),
    batch_file         TEXT NOT NULL,
    source_line        INTEGER NOT NULL,
    reason_code        TEXT NOT NULL,
    all_reasons        TEXT NOT NULL,
    reason_detail      TEXT NOT NULL,
    account_number     TEXT,
    txn_date           TEXT,
    txn_time           TEXT,
    description        TEXT,
    ref_no             TEXT,
    debit              TEXT,
    credit             TEXT,
    balance            TEXT,
    account_type       TEXT,
    raw_line           TEXT NOT NULL,
    record_fingerprint TEXT NOT NULL,
    PRIMARY KEY (batch_sha256, source_line)
);

CREATE TABLE IF NOT EXISTS duplicates (
    source         TEXT NOT NULL,
    batch_sha256   TEXT NOT NULL REFERENCES batches (batch_sha256),
    source_line    INTEGER NOT NULL,
    duplicate_kind TEXT NOT NULL CHECK (duplicate_kind IN ('WITHIN_FILE', 'ALREADY_LOADED')),
    txn_id         TEXT NOT NULL REFERENCES transactions (txn_id),
    PRIMARY KEY (batch_sha256, source_line)
);
"""

TRANSACTION_COLUMNS = [
    "txn_id",
    "source",
    "account_number",
    "txn_date",
    "txn_time",
    "description",
    "ref_no",
    "direction",
    "amount",
    "balance",
    "account_type",
    "batch_sha256",
    "batch_file",
    "source_line",
    "load_type",
]
QUARANTINE_COLUMNS = [
    "source",
    "batch_sha256",
    "batch_file",
    "source_line",
    "reason_code",
    "all_reasons",
    "reason_detail",
    "account_number",
    "txn_date",
    "txn_time",
    "description",
    "ref_no",
    "debit",
    "credit",
    "balance",
    "account_type",
    "raw_line",
    "record_fingerprint",
]
DUPLICATE_COLUMNS = ["source", "batch_sha256", "source_line", "duplicate_kind", "txn_id"]
BATCH_COLUMNS = [
    "batch_sha256",
    "source",
    "batch_file",
    "batch_date",
    "seq",
    "rows_read",
    "rows_loaded",
    "rows_quarantined",
    "dup_within_file",
    "dup_already_loaded",
    "blank_lines",
    "stats_json",
    "loaded_at",
]

_CHUNK = 500


class Store:
    """SQLite ledger + canonical store. One file load is one transaction: all of it or none of it."""

    def __init__(self, path: Path | str):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(self.path, isolation_level=None)
        self.conn.execute("PRAGMA foreign_keys = ON")
        self.conn.executescript(SCHEMA)

    def __enter__(self) -> Store:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def close(self) -> None:
        self.conn.close()

    @contextmanager
    def transaction(self) -> Iterator[None]:
        self.conn.execute("BEGIN IMMEDIATE")
        self.conn.execute("PRAGMA defer_foreign_keys = ON")
        try:
            yield
        except BaseException:
            self.conn.execute("ROLLBACK")
            raise
        else:
            self.conn.execute("COMMIT")

    def batch_file(self, sha256: str) -> str | None:
        row = self.conn.execute("SELECT batch_file FROM batches WHERE batch_sha256 = ?", (sha256,)).fetchone()
        return None if row is None else row[0]

    def next_seq(self) -> int:
        return int(self.conn.execute("SELECT COALESCE(MAX(seq), 0) + 1 FROM batches").fetchone()[0])

    def existing_ids(self, ids: Sequence[str]) -> set[str]:
        ids = list(ids)
        found: set[str] = set()
        for start in range(0, len(ids), _CHUNK):
            chunk = ids[start : start + _CHUNK]
            marks = ",".join("?" * len(chunk))
            query = f"SELECT txn_id FROM transactions WHERE txn_id IN ({marks})"
            found.update(row[0] for row in self.conn.execute(query, chunk))
        return found

    def partition_totals(self, source: str, txn_dates: Sequence[str]) -> dict[str, dict[str, Any]]:
        """Row count and exact debit/credit totals per business date."""
        totals = {d: {"rows": 0, "debit": Decimal(0), "credit": Decimal(0)} for d in txn_dates}
        for start in range(0, len(txn_dates), _CHUNK):
            chunk = list(txn_dates[start : start + _CHUNK])
            marks = ",".join("?" * len(chunk))
            query = f"SELECT txn_date, direction, amount FROM transactions WHERE source = ? AND txn_date IN ({marks})"
            for txn_date, direction, amount in self.conn.execute(query, [source, *chunk]):
                bucket = totals[txn_date]
                bucket["rows"] += 1
                bucket["debit" if direction == "DEBIT" else "credit"] += Decimal(amount)
        return totals

    def batch_table_counts(self, source: str) -> dict[str, dict[str, int]]:
        """Rows physically present per batch in each table, independent of the ledger's own counters."""
        counts: dict[str, dict[str, int]] = {}
        for table, label in (("transactions", "loaded"), ("quarantine", "quarantined"), ("duplicates", "duplicates")):
            query = f"SELECT batch_sha256, COUNT(*) FROM {table} WHERE source = ? GROUP BY batch_sha256"
            for sha, number in self.conn.execute(query, (source,)):
                counts.setdefault(sha, {"loaded": 0, "quarantined": 0, "duplicates": 0})[label] = number
        return counts

    def insert_transactions(self, frame: pd.DataFrame) -> None:
        self._insert("transactions", TRANSACTION_COLUMNS, frame)

    def insert_quarantine(self, frame: pd.DataFrame) -> None:
        self._insert("quarantine", QUARANTINE_COLUMNS, frame)

    def insert_duplicates(self, frame: pd.DataFrame) -> None:
        self._insert("duplicates", DUPLICATE_COLUMNS, frame)

    def insert_batch(self, record: dict[str, Any]) -> None:
        marks = ",".join("?" * len(BATCH_COLUMNS))
        values = [json.dumps(record[c], sort_keys=True) if c == "stats_json" else record[c] for c in BATCH_COLUMNS]
        self.conn.execute(f"INSERT INTO batches ({','.join(BATCH_COLUMNS)}) VALUES ({marks})", values)

    def canonical(self, source: str) -> pd.DataFrame:
        return self._query(
            """
            SELECT txn_id, source, account_number, txn_date, txn_time, description, ref_no, direction,
                   amount, balance, account_type, load_type, batch_file, source_line
            FROM transactions WHERE source = ?
            ORDER BY txn_date, COALESCE(txn_time, ''), account_number, COALESCE(ref_no, ''), txn_id
            """,
            (source,),
        )

    def quarantined(self, source: str) -> pd.DataFrame:
        return self._query(
            """
            SELECT q.source, q.batch_file, q.source_line, q.reason_code, q.all_reasons, q.reason_detail,
                   q.account_number, q.txn_date, q.txn_time, q.description, q.ref_no, q.debit, q.credit,
                   q.balance, q.account_type, q.raw_line, q.record_fingerprint
            FROM quarantine q JOIN batches b ON b.batch_sha256 = q.batch_sha256
            WHERE q.source = ?
            ORDER BY COALESCE(b.batch_date, '9999-12-31'), q.batch_file, q.source_line
            """,
            (source,),
        )

    def duplicates_removed(self, source: str) -> pd.DataFrame:
        return self._query(
            """
            SELECT d.source, b.batch_file, d.source_line, d.duplicate_kind, d.txn_id,
                   t.batch_file AS kept_batch_file, t.source_line AS kept_source_line,
                   t.account_number, t.txn_date, t.txn_time, t.ref_no, t.direction, t.amount
            FROM duplicates d
            JOIN batches b ON b.batch_sha256 = d.batch_sha256
            JOIN transactions t ON t.txn_id = d.txn_id
            WHERE d.source = ?
            ORDER BY COALESCE(b.batch_date, '9999-12-31'), b.batch_file, d.source_line
            """,
            (source,),
        )

    def batches(self, source: str) -> list[dict[str, Any]]:
        cursor = self.conn.execute(
            f"""
            SELECT {','.join(BATCH_COLUMNS)} FROM batches WHERE source = ?
            ORDER BY COALESCE(batch_date, '9999-12-31'), batch_file, batch_sha256
            """,
            (source,),
        )
        names = [col[0] for col in cursor.description]
        out = []
        for row in cursor:
            record = dict(zip(names, row))
            record["stats"] = json.loads(record.pop("stats_json"))
            out.append(record)
        return out

    def _insert(self, table: str, columns: list[str], frame: pd.DataFrame) -> None:
        if frame.empty:
            return
        marks = ",".join("?" * len(columns))
        self.conn.executemany(
            f"INSERT INTO {table} ({','.join(columns)}) VALUES ({marks})", _rows(frame, columns)
        )

    def _query(self, sql: str, params: tuple = ()) -> pd.DataFrame:
        return pd.read_sql_query(sql, self.conn, params=params)


def _rows(frame: pd.DataFrame, columns: list[str]) -> list[tuple]:
    return [tuple(_native(v) for v in row) for row in frame[columns].itertuples(index=False, name=None)]


def _native(value: Any) -> Any:
    if value is None or value is pd.NA:
        return None
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, float) and value != value:
        return None
    if hasattr(value, "item"):
        return value.item()
    return value
