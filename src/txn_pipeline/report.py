from __future__ import annotations

import json
import os
from datetime import date
from decimal import Decimal
from functools import partial
from pathlib import Path
from typing import Any, Callable

import pandas as pd

from .config import SourceConfig
from .store import Store

ZERO = Decimal(0)

CANONICAL_COLUMNS = [
    "txn_id", "source", "account_number", "txn_date", "txn_time", "description", "ref_no",
    "direction", "amount", "balance", "account_type", "load_type", "batch_file", "source_line",
]
QUARANTINE_COLUMNS = ["batch_file", "source_line", "reason_code", "reason_detail", "raw_line"]
DUPLICATE_COLUMNS = [
    "batch_file", "source_line", "duplicate_kind", "txn_id", "kept_batch_file", "kept_source_line",
    "account_number", "txn_date", "txn_time", "ref_no", "direction", "amount",
]

View = Callable[[int, Decimal, Decimal], dict[str, Any]]


def sums(frame: pd.DataFrame) -> tuple[int, Decimal, Decimal]:
    """Row count, total debits and total credits of a frame of transactions."""
    debit = sum((Decimal(a) for a in frame.loc[frame["direction"] == "DEBIT", "amount"]), ZERO)
    credit = sum((Decimal(a) for a in frame.loc[frame["direction"] == "CREDIT", "amount"]), ZERO)
    return len(frame), debit, credit


def publish(store: Store, cfg: SourceConfig, output_dir: Path) -> dict[str, Any]:
    batches, txns, quarantine, duplicates = store.batches(), store.transactions(), store.quarantine(), store.duplicates()
    report = build_report(cfg, batches, txns, quarantine, duplicates)
    documents = {
        "canonical_transactions.csv": _csv(txns[CANONICAL_COLUMNS]),
        "quarantine.csv": _csv(quarantine[QUARANTINE_COLUMNS]),
        "duplicates_removed.csv": _csv(duplicates[DUPLICATE_COLUMNS]),
        "reconciliation_summary.json": json.dumps(report, indent=2) + "\n",
        "reconciliation_summary.md": render_markdown(report),
    }
    output_dir.mkdir(parents=True, exist_ok=True)
    for name, text in documents.items():
        temp = output_dir / f"{name}.tmp"
        temp.write_bytes(text.encode("utf-8"))
        os.replace(temp, output_dir / name)
    return report


def build_report(
    cfg: SourceConfig, batches: pd.DataFrame, txns: pd.DataFrame, quarantine: pd.DataFrame, duplicates: pd.DataFrame
) -> dict[str, Any]:
    view = partial(_view, Decimal(1).scaleb(-cfg.decimal_places))
    by_reason = quarantine["reason_code"].value_counts().sort_index()
    checks = _checks(batches, txns, quarantine, duplicates)
    return {
        "source": cfg.name,
        "dedup_key": list(cfg.dedup_key),
        "totals": _totals(batches),
        "files": _files(batches, txns, quarantine),
        "quarantine_by_reason": [{"reason_code": code, "rows": int(n)} for code, n in by_reason.items()],
        "backfill": _backfills(batches, txns, view),
        "canonical_by_date": _by_date(txns, view),
        "warnings": _warnings(txns),
        "checks": checks,
        "all_checks_passed": all(c["passed"] for c in checks),
    }


def _view(places: Decimal, rows: int, debit: Decimal, credit: Decimal) -> dict[str, Any]:
    def money(value: Decimal) -> str:
        return format(value.quantize(places), "f")

    return {"rows": rows, "debit_total": money(debit), "credit_total": money(credit), "net": money(credit - debit)}


def _text(value: Any) -> str | None:
    return value if isinstance(value, str) else None  # pandas turns a missing string into NaN


def _totals(batches: pd.DataFrame) -> dict[str, int]:
    within, already = int(batches["dup_within_file"].sum()), int(batches["dup_already_loaded"].sum())
    return {
        "files_loaded": len(batches),
        "rows_read": int(batches["rows_read"].sum()),
        "rows_loaded": int(batches["rows_loaded"].sum()),
        "rows_quarantined": int(batches["rows_quarantined"].sum()),
        "duplicates_removed": within + already,
        "duplicates_within_file": within,
        "duplicates_already_loaded": already,
    }


def _files(batches: pd.DataFrame, txns: pd.DataFrame, quarantine: pd.DataFrame) -> list[dict[str, Any]]:
    files = []
    for b in batches.itertuples():
        reasons = quarantine.loc[quarantine["batch_sha256"] == b.batch_sha256, "reason_code"].value_counts().sort_index()
        backfilled = (txns["batch_sha256"] == b.batch_sha256) & (txns["load_type"] == "BACKFILL")
        files.append(
            {
                "file": b.batch_file,
                "sha256": b.batch_sha256,
                "batch_date": _text(b.batch_date),
                "rows_read": int(b.rows_read),
                "rows_loaded": int(b.rows_loaded),
                "rows_quarantined": int(b.rows_quarantined),
                "quarantined_by_reason": {code: int(n) for code, n in reasons.items()},
                "duplicates_within_file": int(b.dup_within_file),
                "duplicates_already_loaded": int(b.dup_already_loaded),
                "rows_loaded_as_backfill": int(backfilled.sum()),
            }
        )
    return files


def _backfills(batches: pd.DataFrame, txns: pd.DataFrame, view: View) -> list[dict[str, Any]]:
    """What each late file did to the business dates it landed in, read off the stored rows.

    "Before" is what the date held when the file arrived (rows from earlier batches only).
    """
    out = []
    for b in batches.itertuples():
        late = txns[(txns["batch_sha256"] == b.batch_sha256) & (txns["load_type"] == "BACKFILL")]
        if late.empty:
            continue
        partitions = []
        for day, added in late.groupby("txn_date"):
            before = sums(txns[(txns["txn_date"] == day) & (txns["seq"] < b.seq)])
            plus = sums(added)
            partitions.append(
                {
                    "txn_date": day,
                    "days_late": (date.fromisoformat(b.batch_date) - date.fromisoformat(day)).days,
                    "before": view(*before),
                    "added": view(*plus),
                    "after": view(*(x + y for x, y in zip(before, plus))),
                }
            )
        out.append(
            {
                "file": b.batch_file,
                "batch_date": b.batch_date,
                **view(*sums(late)),
                "partitions": partitions,
                "by_account": [{"account_number": acct, **view(*sums(g))} for acct, g in late.groupby("account_number")],
            }
        )
    return out


def _by_date(txns: pd.DataFrame, view: View) -> list[dict[str, Any]]:
    return [
        {
            "txn_date": day,
            **view(*sums(g)),
            "regular_rows": int((g["load_type"] == "REGULAR").sum()),
            "backfill_rows": int((g["load_type"] == "BACKFILL").sum()),
        }
        for day, g in txns.groupby("txn_date")
    ]


def _checks(batches: pd.DataFrame, txns: pd.DataFrame, quarantine: pd.DataFrame, duplicates: pd.DataFrame) -> list[dict]:
    expected = batches.set_index("batch_sha256")
    stored = (
        pd.DataFrame(
            {
                "loaded": txns.groupby("batch_sha256").size(),
                "quarantined": quarantine.groupby("batch_sha256").size(),
                "duplicates": duplicates.groupby("batch_sha256").size(),
            }
        )
        .reindex(expected.index)
        .fillna(0)
        .astype(int)
    )
    dups = expected["dup_within_file"] + expected["dup_already_loaded"]
    balanced = expected["rows_read"] == expected["rows_loaded"] + expected["rows_quarantined"] + dups
    present = (
        (stored["loaded"] == expected["rows_loaded"])
        & (stored["quarantined"] == expected["rows_quarantined"])
        & (stored["duplicates"] == dups)
    )
    unbalanced = expected.loc[~(balanced & present), "batch_file"].tolist()

    per_batch = {sha: sums(g) for sha, g in txns.groupby("batch_sha256")}
    drifted = [
        b.batch_file
        for b in batches.itertuples()
        if per_batch.get(b.batch_sha256, (0, ZERO, ZERO))[1:] != (Decimal(b.debit_total), Decimal(b.credit_total))
    ]
    return [
        {
            "name": "rows read = loaded + quarantined + duplicates, and those rows are stored, for every file",
            "passed": not unbalanced,
            "detail": f"fails for {unbalanced}" if unbalanced else f"{len(batches)} file(s) balance",
        },
        {
            "name": "debit and credit totals of each file's stored rows match the totals recorded when it was loaded",
            "passed": not drifted,
            "detail": f"differs for {drifted}" if drifted else "match",
        },
    ]


def _warnings(txns: pd.DataFrame) -> dict[str, Any]:
    types: dict[str, set[str]] = {}
    for account, kind in zip(txns["account_number"], txns["account_type"]):
        if pd.notna(kind):
            types.setdefault(account, set()).add(kind)
    drift = {a: sorted(kinds) for a, kinds in sorted(types.items()) if len(kinds) > 1}

    refs = txns.dropna(subset=["ref_no"]).groupby(["account_number", "ref_no"]).size()
    shared = [{"account_number": a, "ref_no": r, "transactions": int(n)} for (a, r), n in refs[refs > 1].items()]
    return {
        "account_type_drift": {"accounts": len(drift), "of": len(types), "examples": dict(list(drift.items())[:5])},
        "same_ref_no_different_transactions": shared,
    }


def _csv(frame: pd.DataFrame) -> str:
    return frame.to_csv(index=False, lineterminator="\n")


def render_markdown(r: dict[str, Any]) -> str:
    t = r["totals"]
    lines = [
        f"# Reconciliation summary: {r['source']}",
        "",
        "Built from the ingestion ledger. There are no run timestamps in it, so re-running the pipeline never changes this file.",
        f"Dedup key: `{', '.join(r['dedup_key'])}`.",
        "",
        _table(
            ["", ""],
            [
                ["Files loaded", t["files_loaded"]],
                ["Rows read", t["rows_read"]],
                ["Rows loaded", t["rows_loaded"]],
                ["Rows quarantined", t["rows_quarantined"]],
                [
                    "Duplicates removed",
                    f"{t['duplicates_removed']} ({t['duplicates_within_file']} repeated inside a file, "
                    f"{t['duplicates_already_loaded']} re-sent rows that were already loaded)",
                ],
                ["Backfill", _backfill_headline(r["backfill"])],
            ],
        ),
        "",
        f"{t['rows_read']} read = {t['rows_loaded']} loaded + {t['rows_quarantined']} quarantined + {t['duplicates_removed']} duplicates removed",
        "",
        "## By file",
        "",
        _table(
            ["File", "Batch date", "Read", "Loaded", "Quarantined", "Duplicates (in file / already loaded)", "Backfilled"],
            [
                [
                    f["file"], f["batch_date"] or "-", f["rows_read"], f["rows_loaded"], f["rows_quarantined"],
                    f"{f['duplicates_within_file'] + f['duplicates_already_loaded']} "
                    f"({f['duplicates_within_file']} / {f['duplicates_already_loaded']})",
                    f["rows_loaded_as_backfill"],
                ]
                for f in r["files"]
            ],
        ),
        "",
        "## Quarantined, by reason",
        "",
        _table(["Reason", "Rows"], [[q["reason_code"], q["rows"]] for q in r["quarantine_by_reason"]] or [["none", 0]]),
        "",
        "A bad row that is sent again in a later file is quarantined again for that file, so these counts include repeats.",
        "",
    ]

    for b in r["backfill"]:
        lines += [f"## Backfill from {b['file']}", ""]
        for p in b["partitions"]:
            lines += [
                f"Business date {p['txn_date']}, {p['days_late']} day(s) before the file's batch date:",
                "",
                _table(
                    ["", "Rows", "Debits", "Credits", "Net"],
                    [
                        [label, *_cells(p[key], signed=key == "added")]
                        for label, key in (("Before", "before"), ("Added", "added"), ("After", "after"))
                    ],
                ),
                "",
            ]
        lines += [
            "By account:",
            "",
            _table(["Account", "Rows", "Debits", "Credits", "Net"], [[a["account_number"], *_cells(a)] for a in b["by_account"]]),
            "",
        ]

    drift = r["warnings"]["account_type_drift"]
    lines += [
        "## Canonical dataset by business date",
        "",
        _table(
            ["Date", "Rows", "Regular", "Backfill", "Debits", "Credits", "Net"],
            [[d["txn_date"], d["rows"], d["regular_rows"], d["backfill_rows"], *_cells(d)[1:]] for d in r["canonical_by_date"]],
        ),
        "",
        "## Warnings",
        "",
        f"- account_type changes between rows for {drift['accounts']} of {drift['of']} accounts, so it is not part of the dedup key.",
        f"- {len(r['warnings']['same_ref_no_different_transactions'])} account/ref_no pair(s) appear on more than one stored transaction.",
        "",
        "## Checks",
        "",
        _table(["Check", "Result", "Detail"], [[c["name"], "pass" if c["passed"] else "FAIL", c["detail"]] for c in r["checks"]]),
        "",
    ]
    return "\n".join(lines)


def _backfill_headline(backfill: list[dict[str, Any]]) -> str:
    if not backfill:
        return "none"
    return "; ".join(
        f"{b['file']}: +{b['rows']} row(s) into {', '.join(p['txn_date'] for p in b['partitions'])} "
        f"(credits {_m(b['credit_total'], True)}, debits {_m(b['debit_total'], True)}, net {_m(b['net'], True)})"
        for b in backfill
    )


def _cells(view: dict[str, Any], signed: bool = False) -> list[Any]:
    rows = f"+{view['rows']}" if signed else view["rows"]
    return [rows, _m(view["debit_total"], signed), _m(view["credit_total"], signed), _m(view["net"], signed)]


def _m(value: str, signed: bool = False) -> str:
    return format(Decimal(value), "+,f" if signed else ",f")


def _table(headers: list[str], rows: list[list[Any]]) -> str:
    lines = ["| " + " | ".join(headers) + " |", "|" + "---|" * len(headers)]
    lines += ["| " + " | ".join(str(cell) for cell in row) + " |" for row in rows]
    return "\n".join(lines)
