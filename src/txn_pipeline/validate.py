from __future__ import annotations

from typing import Any

import pandas as pd

from .config import SourceConfig
from .extract import RawFile
from .normalize import clean_id, clean_text, parse_date, parse_decimal, parse_time

INVALID_DATE = "INVALID_DATE"
INVALID_TIME = "INVALID_TIME"
INVALID_AMOUNT = "INVALID_AMOUNT"
INVALID_BALANCE = "INVALID_BALANCE"
BOTH_DEBIT_AND_CREDIT = "BOTH_DEBIT_AND_CREDIT"
NO_DEBIT_OR_CREDIT = "NO_DEBIT_OR_CREDIT"
MALFORMED_ROW = "MALFORMED_ROW"

VALID_COLUMNS = [
    "account_number", "txn_date", "txn_time", "description", "ref_no",
    "direction", "amount", "balance", "account_type", "source_line",
]
REJECT_COLUMNS = ["source_line", "reason_code", "reason_detail", "raw_line"]

Issues = list[tuple[str, str]]


def check_row(row: dict[str, Any], cfg: SourceConfig) -> tuple[dict[str, Any], Issues]:
    """Normalise one row; also returns every rule it breaks, most important first."""
    issues: Issues = []

    def parse(code: str, field: str, func, *args):
        try:
            return func(row[field], *args)
        except ValueError as exc:
            issues.append((code, f"{field} {exc}"))

    clean = {f: clean_id(row[f]) for f in ("account_number", "ref_no", "account_type")}
    clean["description"] = clean_text(row["description"])
    for field in cfg.required_fields:
        if clean[field] is None:
            issues.append((f"MISSING_{field.upper()}", f"{field} is blank"))

    day = parse(INVALID_DATE, "txn_date", parse_date, cfg.date_formats)
    clean["txn_date"] = day.isoformat() if day else None
    clock = parse(INVALID_TIME, "txn_time", parse_time, cfg.time_formats) if cfg.columns["txn_time"] else None
    clean["txn_time"] = clock.strftime("%H:%M:%S") if clock else None

    money = (cfg.decimal_places, cfg.decimal_separator, cfg.thousands_separator)
    debit = parse(INVALID_AMOUNT, "debit", parse_decimal, *money)
    credit = parse(INVALID_AMOUNT, "credit", parse_decimal, *money)
    for side, value in (("debit", debit), ("credit", credit)):
        if value is not None and value < 0:
            issues.append((INVALID_AMOUNT, f"{side} {value} is negative"))
    amounts_ok = all(code != INVALID_AMOUNT for code, _ in issues)

    clean["balance"] = parse(INVALID_BALANCE, "balance", parse_decimal, *money) if cfg.columns["balance"] else None

    # a side counts as populated only if it carries a non-zero amount, so a feed that
    # writes 0.00 for the unused side isn't read as "both populated"
    if amounts_ok:
        if debit and credit:
            issues.append((BOTH_DEBIT_AND_CREDIT, f"debit={debit} and credit={credit} are both populated"))
        elif not (debit or credit):
            issues.append((NO_DEBIT_OR_CREDIT, "neither debit nor credit has an amount"))
        else:
            clean["direction"], clean["amount"] = ("DEBIT", debit) if debit else ("CREDIT", credit)
    return clean, issues


def validate_rows(raw: RawFile, cfg: SourceConfig) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Split a file into valid rows and rejected rows (malformed lines included)."""
    valid, rejected = [], []
    for row in raw.rows.to_dict("records"):
        clean, issues = check_row(row, cfg)
        if issues:
            detail = " | ".join(f"{code}: {text}" for code, text in issues)
            rejected.append(_reject(row["source_line"], issues[0][0], detail, row["raw_line"]))
        else:
            valid.append({**clean, "source_line": row["source_line"]})
    for rec in raw.ragged:
        detail = f"{MALFORMED_ROW}: expected {raw.header_width} fields, found {len(rec.fields)}"
        rejected.append(_reject(rec.line, MALFORMED_ROW, detail, rec.raw))
    rejected.sort(key=lambda r: r["source_line"])
    return _frame(valid, VALID_COLUMNS), _frame(rejected, REJECT_COLUMNS)


def _reject(line: int, code: str, detail: str, raw_line: str) -> dict[str, Any]:
    return {"source_line": line, "reason_code": code, "reason_detail": detail, "raw_line": raw_line}


def _frame(rows: list[dict[str, Any]], columns: list[str]) -> pd.DataFrame:
    frame = pd.DataFrame(rows, columns=columns, dtype=object)
    frame["source_line"] = frame["source_line"].astype("int64")
    return frame
