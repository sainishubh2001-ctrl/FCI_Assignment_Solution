from __future__ import annotations

from pathlib import Path

HEADER = "account_number,txn_date,txn_time,description,ref_no,debit,credit,balance,account_type"


def make_row(
    account: str = "AC1",
    date: str = "01-08-2026",
    time: str = "10:00:00",
    desc: str = "POS PURCHASE",
    ref: str = "REF1",
    debit: str = "100.00",
    credit: str = "",
    balance: str = "900.00",
    typ: str = "SAVINGS",
) -> str:
    return ",".join([account, date, time, desc, ref, debit, credit, balance, typ])


def write_batch(directory: Path, name: str, rows: list[str], eol: str = "\r\n") -> Path:
    path = Path(directory) / name
    path.write_bytes((eol.join([HEADER, *rows]) + eol).encode("utf-8"))
    return path


def raw_rec(**overrides: str) -> dict:
    rec = {
        "account_number": "AC1",
        "txn_date": "01-08-2026",
        "txn_time": "10:00:00",
        "description": "POS PURCHASE",
        "ref_no": "REF1",
        "debit": "100.00",
        "credit": "",
        "balance": "900.00",
        "account_type": "SAVINGS",
        "source_line": 2,
        "raw_line": "raw",
    }
    rec.update(overrides)
    return rec
