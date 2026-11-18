from __future__ import annotations

import argparse
import logging
import os
import sys
from decimal import Decimal
from pathlib import Path

from .config import ConfigError, load_config
from .pipeline import run


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="txn-ingest",
        description="Idempotent bank-transaction ingestion: normalise, quarantine, dedup, backfill, reconcile.",
    )
    parser.add_argument("--config", default="config/bank_a.yaml", help="source configuration (default: %(default)s)")
    parser.add_argument("--input", default="sample_data/assignment-1-pipeline", help="folder with the delivered files (default: %(default)s)")
    parser.add_argument("--output", default="output", help="folder for the published datasets (default: %(default)s)")
    parser.add_argument("--state", default="state", help="folder for the SQLite ledger/canonical store (default: %(default)s)")
    parser.add_argument("--reset", action="store_true", help="delete the stored state first and rebuild everything from the input folder")
    return parser


def configure_logging() -> None:
    """Attach a stdout handler to the package logger only, leaving the root logger to the host process."""
    logger = logging.getLogger("txn_pipeline")
    for handler in [h for h in logger.handlers if getattr(h, "_txn_cli", False)]:
        logger.removeHandler(handler)
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter("%(message)s"))
    handler._txn_cli = True  # type: ignore[attr-defined]
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    logger.propagate = False


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    configure_logging()

    try:
        cfg = load_config(args.config)
    except ConfigError as exc:
        print(f"configuration error: {exc}", file=sys.stderr)
        return 2
    if not Path(args.input).is_dir():
        print(f"input folder not found: {args.input}", file=sys.stderr)
        return 2

    result = run(cfg, Path(args.input), Path(args.output), Path(args.state), reset=args.reset)
    for line in summary_lines(result.report, Path(args.output)):
        print(line)
    for outcome in result.failed:
        print(f"FAILED {outcome.name}: {outcome.message}", file=sys.stderr)
    if not result.report["all_checks_passed"]:
        failed = [c["name"] for c in result.report["control_checks"] if not c["passed"]]
        print(f"RECONCILIATION FAILED: {failed}", file=sys.stderr)
    return 0 if result.ok else 1


def summary_lines(report: dict, output: Path) -> list[str]:
    t = report["totals"]
    lines = [
        "",
        f"Reconciliation: {t['rows_read']} read = {t['rows_loaded']} loaded + {t['rows_quarantined']} quarantined "
        f"+ {t['duplicates_removed']} duplicates removed "
        f"({'all control checks passed' if report['all_checks_passed'] else 'CONTROL CHECK FAILURE'})",
    ]
    for b in report["backfill"]:
        dates = ", ".join(p["txn_date"] for p in b["partitions"])
        lines.append(
            f"Backfill: {b['file']} added {b['rows']} rows to {dates} "
            f"(credits {format(Decimal(b['credit_total']), '+,f')}, debits {format(Decimal(b['debit_total']), '+,f')})"
        )
    lines.append(f"Outputs in {output}{os.sep}: canonical_transactions.csv, quarantine.csv, duplicates_removed.csv, reconciliation_summary.json/.md")
    return lines


if __name__ == "__main__":
    raise SystemExit(main())
