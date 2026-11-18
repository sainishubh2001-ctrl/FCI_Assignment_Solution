from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from decimal import Decimal
from pathlib import Path
from typing import Any, Iterator, Sequence

import pandas as pd

SCHEMA = """
CREATE TABLE IF NOT EXISTS batches (
    batch_sha256       TEXT PRIMARY KEY,
    batch_file         TEXT NOT NULL,
    batch_date         TEXT,
    seq                INTEGER NOT NULL,
    rows_read          INTEGER NOT NULL,
    rows_loaded        INTEGER NOT NULL,
    rows_quarantined   INTEGER NOT NULL,
    dup_within_file    INTEGER NOT NULL,
    dup_already_loaded INTEGER NOT NULL,
    debit_total        TEXT NOT NULL,
    credit_total       TEXT NOT NULL,
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
    batch_sha256   TEXT NOT NULL REFERENCES batches,
    source_line    INTEGER NOT NULL,
    load_type      TEXT NOT NULL CHECK (load_type IN ('REGULAR', 'BACKFILL'))
);

CREATE TABLE IF NOT EXISTS quarantine (
    batch_sha256  TEXT NOT NULL REFERENCES batches,
    source_line   INTEGER NOT NULL,
    reason_code   TEXT NOT NULL,
    reason_detail TEXT NOT NULL,
    raw_line      TEXT NOT NULL,
    PRIMARY KEY (batch_sha256, source_line)
);

CREATE TABLE IF NOT EXISTS duplicates (
    batch_sha256   TEXT NOT NULL REFERENCES batches,
    source_line    INTEGER NOT NULL,
    duplicate_kind TEXT NOT NULL CHECK (duplicate_kind IN ('WITHIN_FILE', 'ALREADY_LOADED')),
    txn_id         TEXT NOT NULL REFERENCES transactions,
    PRIMARY KEY (batch_sha256, source_line)
);
"""

_CHUNK = 500


class Store:
    """SQLite ledger and canonical store. One file load is one transaction."""

    def __init__(self, path: Path | str):
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(path, isolation_level=None)
        self.conn.execute("PRAGMA foreign_keys = ON")
        self.conn.executescript(SCHEMA)

    def __enter__(self) -> Store:
        return self

    def __exit__(self, *exc: object) -> None:
        self.conn.close()

    @contextmanager
    def transaction(self) -> Iterator[None]:
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            yield
        except BaseException:
            self.conn.execute("ROLLBACK")
            raise
        self.conn.execute("COMMIT")

    def batch_file(self, sha256: str) -> str | None:
        row = self.conn.execute("SELECT batch_file FROM batches WHERE batch_sha256 = ?", (sha256,)).fetchone()
        return row[0] if row else None

    def next_seq(self) -> int:
        return self.conn.execute("SELECT COALESCE(MAX(seq), 0) + 1 FROM batches").fetchone()[0]

    def existing_ids(self, ids: Sequence[str]) -> set[str]:
        found: set[str] = set()
        for i in range(0, len(ids), _CHUNK):
            chunk = list(ids[i : i + _CHUNK])
            marks = ",".join("?" * len(chunk))
            found.update(r[0] for r in self.conn.execute(f"SELECT txn_id FROM transactions WHERE txn_id IN ({marks})", chunk))
        return found

    def insert_batch(self, **fields: Any) -> None:
        marks = ",".join("?" * len(fields))
        self.conn.execute(f"INSERT INTO batches ({','.join(fields)}) VALUES ({marks})", list(fields.values()))

    def insert(self, table: str, frame: pd.DataFrame) -> None:
        """Insert every row of `frame`; its column names must match the table's."""
        if frame.empty:
            return
        marks = ",".join("?" * len(frame.columns))
        rows = [tuple(_native(v) for v in row) for row in frame.itertuples(index=False, name=None)]
        self.conn.executemany(f"INSERT INTO {table} ({','.join(frame.columns)}) VALUES ({marks})", rows)

    def batches(self) -> pd.DataFrame:
        return self._frame("SELECT * FROM batches ORDER BY COALESCE(batch_date, '9999-12-31'), batch_file, batch_sha256")

    def transactions(self) -> pd.DataFrame:
        return self._frame(
            """
            SELECT t.txn_id, t.source, t.account_number, t.txn_date, t.txn_time, t.description, t.ref_no,
                   t.direction, t.amount, t.balance, t.account_type, t.load_type,
                   b.batch_file, t.source_line, t.batch_sha256, b.seq
            FROM transactions t JOIN batches b USING (batch_sha256)
            ORDER BY t.txn_date, COALESCE(t.txn_time, ''), t.account_number, COALESCE(t.ref_no, ''), t.txn_id
            """
        )

    def quarantine(self) -> pd.DataFrame:
        return self._frame(
            """
            SELECT q.batch_sha256, b.batch_file, q.source_line, q.reason_code, q.reason_detail, q.raw_line
            FROM quarantine q JOIN batches b USING (batch_sha256)
            ORDER BY COALESCE(b.batch_date, '9999-12-31'), b.batch_file, q.source_line
            """
        )

    def duplicates(self) -> pd.DataFrame:
        return self._frame(
            """
            SELECT d.batch_sha256, b.batch_file, d.source_line, d.duplicate_kind, d.txn_id,
                   kb.batch_file AS kept_batch_file, t.source_line AS kept_source_line,
                   t.account_number, t.txn_date, t.txn_time, t.ref_no, t.direction, t.amount
            FROM duplicates d
            JOIN batches b ON b.batch_sha256 = d.batch_sha256
            JOIN transactions t ON t.txn_id = d.txn_id
            JOIN batches kb ON kb.batch_sha256 = t.batch_sha256
            ORDER BY COALESCE(b.batch_date, '9999-12-31'), b.batch_file, d.source_line
            """
        )

    def _frame(self, sql: str) -> pd.DataFrame:
        return pd.read_sql_query(sql, self.conn)


def _native(value: Any) -> Any:
    if value is None or value is pd.NA or (isinstance(value, float) and value != value):
        return None
    if isinstance(value, Decimal):
        return format(value, "f")
    return value.item() if hasattr(value, "item") else value
