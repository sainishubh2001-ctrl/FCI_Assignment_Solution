from __future__ import annotations

from collections import Counter, defaultdict
from decimal import Decimal
from typing import Any

import pandas as pd

from .config import SourceConfig
from .store import Store

ZERO = Decimal(0)


def build_report(store: Store, cfg: SourceConfig) -> dict[str, Any]:
    batches = store.batches(cfg.name)
    canonical = store.canonical(cfg.name)
    quarantine = store.quarantined(cfg.name)
    table_counts = store.batch_table_counts(cfg.name)

    def money(value: Decimal) -> str:
        return format(value.quantize(Decimal(1).scaleb(-cfg.decimal_places)), "f")

    files = [_file_row(b) for b in batches]
    backfills = [_backfill_row(b) for b in batches if b["stats"].get("backfill")]
    checks = _control_checks(batches, canonical, table_counts, money)

    return {
        "source": cfg.name,
        "dedup_key": list(cfg.dedup_key),
        "totals": _totals(batches, quarantine),
        "files": files,
        "quarantine_by_reason": _quarantine_by_reason(quarantine),
        "backfill": backfills,
        "canonical": {"rows": len(canonical), "by_txn_date": _by_txn_date(canonical, money)},
        "data_quality_warnings": _warnings(canonical),
        "control_checks": checks,
        "all_checks_passed": all(c["passed"] for c in checks),
    }


def _file_row(batch: dict[str, Any]) -> dict[str, Any]:
    stats = batch["stats"]
    return {
        "file": batch["batch_file"],
        "sha256": batch["batch_sha256"],
        "batch_date": batch["batch_date"],
        "rows_read": batch["rows_read"],
        "rows_loaded": batch["rows_loaded"],
        "rows_quarantined": batch["rows_quarantined"],
        "quarantined_by_reason": stats["quarantined_by_reason"],
        "duplicates_within_file": batch["dup_within_file"],
        "duplicates_already_loaded": batch["dup_already_loaded"],
        "rows_loaded_as_backfill": (stats.get("backfill") or {}).get("rows", 0),
        "late_dated_rows": stats["late_dated"],
    }


def _backfill_row(batch: dict[str, Any]) -> dict[str, Any]:
    return {
        "file": batch["batch_file"],
        "batch_date": batch["batch_date"],
        "late_dated_rows": batch["stats"]["late_dated"],
        **batch["stats"]["backfill"],
    }


def _totals(batches: list[dict[str, Any]], quarantine: pd.DataFrame) -> dict[str, Any]:
    within = sum(b["dup_within_file"] for b in batches)
    already = sum(b["dup_already_loaded"] for b in batches)
    return {
        "files_loaded": len(batches),
        "rows_read": sum(b["rows_read"] for b in batches),
        "rows_loaded": sum(b["rows_loaded"] for b in batches),
        "rows_quarantined": sum(b["rows_quarantined"] for b in batches),
        "distinct_quarantined_records": int(quarantine["record_fingerprint"].nunique()),
        "duplicates_removed": within + already,
        "duplicates_within_file": within,
        "duplicates_already_loaded": already,
    }


def _quarantine_by_reason(quarantine: pd.DataFrame) -> list[dict[str, Any]]:
    rows = Counter(quarantine["reason_code"])
    distinct: dict[str, set] = defaultdict(set)
    for code, fingerprint in zip(quarantine["reason_code"], quarantine["record_fingerprint"]):
        distinct[code].add(fingerprint)
    return [{"reason_code": code, "rows": rows[code], "distinct_records": len(distinct[code])} for code in sorted(rows)]


def _by_txn_date(canonical: pd.DataFrame, money) -> list[dict[str, Any]]:
    out = []
    for txn_date, group in canonical.groupby("txn_date", sort=True):
        debit = sum((Decimal(a) for a, d in zip(group["amount"], group["direction"]) if d == "DEBIT"), ZERO)
        credit = sum((Decimal(a) for a, d in zip(group["amount"], group["direction"]) if d == "CREDIT"), ZERO)
        out.append(
            {
                "txn_date": txn_date,
                "rows": len(group),
                "regular_rows": int((group["load_type"] == "REGULAR").sum()),
                "backfill_rows": int((group["load_type"] == "BACKFILL").sum()),
                "debit_total": money(debit),
                "credit_total": money(credit),
                "net": money(credit - debit),
            }
        )
    return out


def _warnings(canonical: pd.DataFrame) -> dict[str, Any]:
    types: dict[str, set] = defaultdict(set)
    for account, account_type in zip(canonical["account_number"], canonical["account_type"]):
        if pd.notna(account_type):
            types[account].add(account_type)
    drift = [{"account_number": a, "account_types": sorted(t)} for a, t in sorted(types.items()) if len(t) > 1]

    refs = canonical[canonical["ref_no"].notna()].groupby(["account_number", "ref_no"]).size()
    shared = [
        {"account_number": account, "ref_no": ref, "transactions": int(n)}
        for (account, ref), n in refs[refs > 1].items()
    ]
    return {
        "account_type_drift": {
            "accounts_with_multiple_types": len(drift),
            "accounts_total": int(canonical["account_number"].nunique()),
            "accounts": drift,
            "action": "account_type is excluded from the dedup key; values are kept as delivered",
        },
        "same_ref_no_on_different_transactions": {
            "count": len(shared),
            "transactions": shared,
            "action": "both rows are loaded because their keys differ; review for a possible amendment",
        },
    }


def _control_checks(batches, canonical: pd.DataFrame, table_counts, money) -> list[dict[str, Any]]:
    def check(name: str, failures: list[str], ok_detail: str) -> dict[str, Any]:
        return {"name": name, "passed": not failures, "detail": "; ".join(failures) if failures else ok_detail}

    accounting = [
        b["batch_file"]
        for b in batches
        if b["rows_read"] != b["rows_loaded"] + b["rows_quarantined"] + b["dup_within_file"] + b["dup_already_loaded"]
    ]

    physical = []
    for b in batches:
        found = table_counts.get(b["batch_sha256"], {"loaded": 0, "quarantined": 0, "duplicates": 0})
        expected = (b["rows_loaded"], b["rows_quarantined"], b["dup_within_file"] + b["dup_already_loaded"])
        if (found["loaded"], found["quarantined"], found["duplicates"]) != expected:
            physical.append(b["batch_file"])

    loaded = sum(b["rows_loaded"] for b in batches)
    stored_debit = sum((Decimal(a) for a, d in zip(canonical["amount"], canonical["direction"]) if d == "DEBIT"), ZERO)
    stored_credit = sum((Decimal(a) for a, d in zip(canonical["amount"], canonical["direction"]) if d == "CREDIT"), ZERO)
    ledger_debit = sum((Decimal(b["stats"]["loaded_debit_total"]) for b in batches), ZERO)
    ledger_credit = sum((Decimal(b["stats"]["loaded_credit_total"]) for b in batches), ZERO)

    backfill_rows = sum((b["stats"].get("backfill") or {}).get("rows", 0) for b in batches)
    partition_rows = sum(p["delta"]["rows"] for b in batches for p in (b["stats"].get("backfill") or {}).get("partitions", []))
    stored_backfill = int((canonical["load_type"] == "BACKFILL").sum())

    bad_amounts = [a for a in canonical["amount"] if Decimal(a) <= 0]

    return [
        check("Every file: rows read = loaded + quarantined + duplicates removed", accounting, f"{len(batches)} file(s) balance"),
        check("Ledger counters match the rows physically stored for each file", physical, "loaded, quarantined and duplicate rows all present"),
        check(
            "Canonical row count = rows loaded across all files",
            [] if len(canonical) == loaded else [f"canonical has {len(canonical)}, ledger says {loaded}"],
            f"{len(canonical)} rows",
        ),
        check(
            "Control totals: canonical debits and credits = totals loaded per file",
            [] if (stored_debit, stored_credit) == (ledger_debit, ledger_credit)
            else [f"canonical {money(stored_debit)}/{money(stored_credit)} vs ledger {money(ledger_debit)}/{money(ledger_credit)}"],
            f"debits {money(stored_debit)}, credits {money(stored_credit)}",
        ),
        check(
            "Backfilled rows landed in their own business-date partitions",
            [] if backfill_rows == partition_rows == stored_backfill
            else [f"ledger {backfill_rows}, partition deltas {partition_rows}, stored {stored_backfill}"],
            f"{stored_backfill} backfilled row(s)",
        ),
        check("No transaction id appears twice in the canonical dataset", [] if canonical["txn_id"].is_unique else ["duplicate txn_id found"], "unique"),
        check("Every canonical amount is positive and has a direction", [str(len(bad_amounts)) + " non-positive amount(s)"] if bad_amounts else [], "all positive"),
    ]


def render_markdown(report: dict[str, Any]) -> str:
    t = report["totals"]
    lines = [
        f"# Reconciliation summary: {report['source']}",
        "",
        "Built from the ingestion ledger. It carries no run timestamps, so re-running the pipeline never changes it.",
        f"Dedup key: `{', '.join(report['dedup_key'])}`",
        "",
        "## 1. Headline",
        "",
        _table(
            ["Metric", "Value"],
            [
                ["Files loaded", t["files_loaded"]],
                ["Rows read", t["rows_read"]],
                ["Rows loaded into the canonical dataset", t["rows_loaded"]],
                ["Rows quarantined", f"{t['rows_quarantined']} ({t['distinct_quarantined_records']} distinct bad records)"],
                [
                    "Duplicates removed",
                    f"{t['duplicates_removed']} ({t['duplicates_within_file']} repeated inside a file, "
                    f"{t['duplicates_already_loaded']} re-delivered rows that were already loaded)",
                ],
                ["Backfill net effect", _backfill_headline(report["backfill"])],
            ],
        ),
        "",
        f"**{t['rows_read']} read = {t['rows_loaded']} loaded + {t['rows_quarantined']} quarantined + "
        f"{t['duplicates_removed']} duplicates removed**",
        "",
        "## 2. Per file",
        "",
        _table(
            ["File", "Batch date", "Read", "Loaded", "Quarantined", "Duplicates (in file / already loaded)", "Loaded as backfill"],
            [
                [
                    f["file"],
                    f["batch_date"] or "-",
                    f["rows_read"],
                    f["rows_loaded"],
                    f["rows_quarantined"],
                    f"{f['duplicates_within_file'] + f['duplicates_already_loaded']} ({f['duplicates_within_file']} / {f['duplicates_already_loaded']})",
                    f["rows_loaded_as_backfill"],
                ]
                for f in report["files"]
            ],
        ),
        "",
        "## 3. Quarantine by reason",
        "",
        _table(
            ["Reason code", "Rows", "Distinct records"],
            [[q["reason_code"], q["rows"], q["distinct_records"]] for q in report["quarantine_by_reason"]] or [["none", 0, 0]],
        ),
        "",
        "A bad record that is re-sent in a later file is quarantined again for that delivery, so rows can exceed distinct records. "
        "Every quarantined row keeps its file, line number and raw text in `quarantine.csv`.",
        "",
        "## 4. Late-dated rows and backfill",
        "",
    ]

    late_files = [f for f in report["files"] if f["late_dated_rows"]["rows"]]
    if late_files:
        lines += [
            "Rows whose business date is older than the file's batch date:",
            "",
            _table(
                ["File", "Late-dated valid rows", "New (backfilled)", "Already loaded", "Repeated inside file"],
                [
                    [f["file"], d["rows"], d["new"], d["already_loaded"], d["within_file_repeats"]]
                    for f in late_files
                    for d in [f["late_dated_rows"]]
                ],
            ),
            "",
        ]
    else:
        lines += ["No late-dated rows were delivered.", ""]

    for b in report["backfill"]:
        lines += [f"### Backfill from `{b['file']}` (batch date {b['batch_date']})", ""]
        for p in b["partitions"]:
            lines += [
                f"Business date {p['txn_date']} ({p['days_late']} day(s) late):",
                "",
                _table(
                    ["", "Rows", "Debits", "Credits", "Net"],
                    [
                        ["Before", p["before"]["rows"], _m(p["before"]["debit_total"]), _m(p["before"]["credit_total"]), _m(p["before"]["net"])],
                        ["Backfilled", f"+{p['delta']['rows']}", _m(p["delta"]["debit_total"], True), _m(p["delta"]["credit_total"], True), _m(p["delta"]["net"], True)],
                        ["After", p["after"]["rows"], _m(p["after"]["debit_total"]), _m(p["after"]["credit_total"]), _m(p["after"]["net"])],
                    ],
                ),
                "",
            ]
        late = b["late_dated_rows"]
        lines += [
            "By account:",
            "",
            _table(
                ["Account", "Rows", "Debits", "Credits", "Net"],
                [[a["account_number"], a["rows"], _m(a["debit_total"]), _m(a["credit_total"]), _m(a["net"], True)] for a in b["by_account"]],
            ),
            "",
            f"Of {late['rows']} valid late-dated row(s) in this file, {late['new']} were new and were merged into the business date "
            f"above; {late['already_loaded']} were already loaded and skipped instead of being duplicated, and "
            f"{late['within_file_repeats']} repeated another row of the same file.",
            "",
        ]

    lines += [
        "## 5. Canonical dataset by business date",
        "",
        _table(
            ["Date", "Rows", "Regular", "Backfill", "Debits", "Credits", "Net"],
            [
                [d["txn_date"], d["rows"], d["regular_rows"], d["backfill_rows"], _m(d["debit_total"]), _m(d["credit_total"]), _m(d["net"], True)]
                for d in report["canonical"]["by_txn_date"]
            ],
        ),
        "",
        "## 6. Data-quality warnings (informational, nothing is rejected for these)",
        "",
    ]
    drift = report["data_quality_warnings"]["account_type_drift"]
    shared = report["data_quality_warnings"]["same_ref_no_on_different_transactions"]
    lines += [
        f"- **account_type drift:** {drift['accounts_with_multiple_types']} of {drift['accounts_total']} accounts carry more than one "
        f"account_type across rows ({drift['action']}).",
        f"- **Same ref_no on different transactions:** {shared['count']} ({shared['action']}).",
        "",
        "## 7. Control checks",
        "",
        _table(
            ["Check", "Result", "Detail"],
            [[c["name"], "PASS" if c["passed"] else "FAIL", c["detail"]] for c in report["control_checks"]],
        ),
        "",
        f"All control checks passed: **{'yes' if report['all_checks_passed'] else 'NO'}**",
        "",
    ]
    return "\n".join(lines)


def _backfill_headline(backfills: list[dict[str, Any]]) -> str:
    if not backfills:
        return "none"
    parts = []
    for b in backfills:
        dates = ", ".join(p["txn_date"] for p in b["partitions"])
        parts.append(
            f"`{b['file']}`: +{b['rows']} rows into {dates} (credits {_m(b['credit_total'], True)}, "
            f"debits {_m(b['debit_total'], True)}, net {_m(b['net'], True)})"
        )
    return "; ".join(parts)


def _m(value: str, signed: bool = False) -> str:
    return format(Decimal(value), "+,f" if signed else ",f")


def _table(headers: list[str], rows: list[list[Any]]) -> str:
    out = ["| " + " | ".join(headers) + " |", "|" + "|".join("---" for _ in headers) + "|"]
    out += ["| " + " | ".join(str(cell) for cell in row) + " |" for row in rows]
    return "\n".join(out)
