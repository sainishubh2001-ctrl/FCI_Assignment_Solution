from __future__ import annotations

import hashlib
from decimal import Decimal
from typing import Any

import pandas as pd

from .config import INPUT_FIELDS, SourceConfig
from .extract import RawRecord
from .normalize import clean_id, clean_text, fmt_amount, parse_date, parse_decimal, parse_time

MALFORMED_ROW = "MALFORMED_ROW"
INVALID_DATE = "INVALID_DATE"
INVALID_TIME = "INVALID_TIME"
INVALID_AMOUNT = "INVALID_AMOUNT"
INVALID_BALANCE = "INVALID_BALANCE"
BOTH_DEBIT_AND_CREDIT = "BOTH_DEBIT_AND_CREDIT"
NO_DEBIT_OR_CREDIT = "NO_DEBIT_OR_CREDIT"

VALID_COLUMNS = [
    "account_number",
    "txn_date",
    "txn_time",
    "description",
    "ref_no",
    "direction",
    "amount",
    "balance",
    "account_type",
    "source_line",
]
REJECT_COLUMNS = [
    "source_line",
    "reason_code",
    "all_reasons",
    "reason_detail",
    *INPUT_FIELDS,
    "raw_line",
    "record_fingerprint",
]

Issue = tuple[str, str]


def check_row(rec: dict[str, Any], cfg: SourceConfig) -> tuple[dict[str, Any], list[Issue]]:
    """Normalise one source row and list every rule it breaks, highest priority first."""
    out: dict[str, Any] = {
        "account_number": clean_id(rec.get("account_number")),
        "ref_no": clean_id(rec.get("ref_no")),
        "account_type": clean_id(rec.get("account_type")),
        "description": clean_text(rec.get("description")),
        "txn_time": None,
        "balance": None,
    }
    issues: list[Issue] = []

    for field in cfg.required_fields:
        if out[field] is None:
            issues.append((f"MISSING_{field.upper()}", f"{field} is blank"))

    try:
        out["txn_date"] = parse_date(
            rec.get("txn_date"), cfg.date_formats, cfg.min_year, cfg.max_year
        ).isoformat()
    except ValueError as exc:
        out["txn_date"] = None
        issues.append((INVALID_DATE, f"txn_date: {exc}"))

    if cfg.columns["txn_time"] is not None:
        try:
            out["txn_time"] = parse_time(rec.get("txn_time"), cfg.time_formats).strftime("%H:%M:%S")
        except ValueError as exc:
            issues.append((INVALID_TIME, f"txn_time: {exc}"))

    amount_errors: list[str] = []
    for side in ("debit", "credit"):
        try:
            value = parse_decimal(
                rec.get(side), cfg.decimal_places, cfg.decimal_separator, cfg.thousands_separator
            )
        except ValueError as exc:
            amount_errors.append(f"{side}: {exc}")
            value = None
        else:
            if value is not None and value < 0:
                amount_errors.append(f"{side}: {fmt_amount(value)} is negative")
        out[side] = value
    if amount_errors:
        issues.append((INVALID_AMOUNT, "; ".join(amount_errors)))

    if cfg.columns["balance"] is not None:
        try:
            out["balance"] = parse_decimal(
                rec.get("balance"), cfg.decimal_places, cfg.decimal_separator, cfg.thousands_separator
            )
        except ValueError as exc:
            issues.append((INVALID_BALANCE, f"balance: {exc}"))

    if not amount_errors:
        # A side only counts as populated when it carries a non-zero amount, so a feed that writes
        # 0.00 for the unused side is not mistaken for "both populated".
        has_debit = out["debit"] is not None and out["debit"] != 0
        has_credit = out["credit"] is not None and out["credit"] != 0
        if has_debit and has_credit:
            issues.append(
                (
                    BOTH_DEBIT_AND_CREDIT,
                    f"debit={fmt_amount(out['debit'])} and credit={fmt_amount(out['credit'])} are both populated",
                )
            )
        elif not has_debit and not has_credit:
            issues.append((NO_DEBIT_OR_CREDIT, "neither debit nor credit carries an amount"))
        elif has_debit:
            out["direction"], out["amount"] = "DEBIT", out["debit"]
        else:
            out["direction"], out["amount"] = "CREDIT", out["credit"]

    return out, issues


def fingerprint(rec: dict[str, Any], normalised: dict[str, Any]) -> str:
    """Identity of a bad record that ignores formatting, so a re-delivered bad row matches its first sighting."""
    parts = []
    for field in INPUT_FIELDS:
        value = normalised.get(field)
        if value is None:
            value = clean_text(rec.get(field)) or ""
        parts.append(fmt_amount(value) if isinstance(value, Decimal) else str(value))
    return hashlib.sha256("\x1f".join(parts).encode("utf-8")).hexdigest()[:32]


def validate_rows(raw: pd.DataFrame, cfg: SourceConfig) -> tuple[pd.DataFrame, pd.DataFrame]:
    valid_rows: list[dict[str, Any]] = []
    reject_rows: list[dict[str, Any]] = []

    for rec in raw.to_dict("records"):
        normalised, issues = check_row(rec, cfg)
        if issues:
            reject_rows.append(
                {
                    "source_line": rec["source_line"],
                    "reason_code": issues[0][0],
                    "all_reasons": ";".join(code for code, _ in issues),
                    "reason_detail": " | ".join(f"{code}: {detail}" for code, detail in issues),
                    **{field: rec.get(field) for field in INPUT_FIELDS},
                    "raw_line": rec["raw_line"],
                    "record_fingerprint": fingerprint(rec, normalised),
                }
            )
        else:
            valid_rows.append({**{col: normalised.get(col) for col in VALID_COLUMNS}, "source_line": rec["source_line"]})

    return _frame(valid_rows, VALID_COLUMNS), _frame(reject_rows, REJECT_COLUMNS)


def malformed_rejects(records: list[RawRecord], header_width: int) -> pd.DataFrame:
    rows = []
    for rec in records:
        normalised_line = " ".join(rec.raw_line.split())
        rows.append(
            {
                "source_line": rec.line_no,
                "reason_code": MALFORMED_ROW,
                "all_reasons": MALFORMED_ROW,
                "reason_detail": f"{MALFORMED_ROW}: expected {header_width} fields, found {len(rec.fields)}",
                **{field: None for field in INPUT_FIELDS},
                "raw_line": rec.raw_line,
                "record_fingerprint": hashlib.sha256(normalised_line.encode("utf-8")).hexdigest()[:32],
            }
        )
    return _frame(rows, REJECT_COLUMNS)


def _frame(rows: list[dict[str, Any]], columns: list[str]) -> pd.DataFrame:
    frame = pd.DataFrame(rows, columns=columns, dtype=object)
    frame["source_line"] = frame["source_line"].astype("int64")
    return frame
