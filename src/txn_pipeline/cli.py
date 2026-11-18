from __future__ import annotations

import argparse
import logging
import sys
from decimal import Decimal
from pathlib import Path

from .config import ConfigError, load_config
from .pipeline import run


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="txn-ingest", description="Load bank transaction files into a clean, deduplicated dataset.")
    parser.add_argument("--config", default="config/bank_a.yaml", help="source definition (default: %(default)s)")
    parser.add_argument("--input", default="sample_data/assignment-1-pipeline", help="folder with the delivered files (default: %(default)s)")
    parser.add_argument("--output", default="output", help="where the datasets and reconciliation go (default: %(default)s)")
    parser.add_argument("--state", default="state", help="where the SQLite state lives (default: %(default)s)")
    parser.add_argument("--reset", action="store_true", help="drop the stored state and rebuild from the input folder")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    # own handler on the package logger so the host process's logging setup doesn't swallow the output
    log = logging.getLogger("txn_pipeline")
    log.handlers = [logging.StreamHandler(sys.stdout)]
    log.setLevel(logging.INFO)
    log.propagate = False

    try:
        cfg = load_config(args.config)
    except ConfigError as exc:
        print(f"configuration error: {exc}", file=sys.stderr)
        return 2
    if not Path(args.input).is_dir():
        print(f"input folder not found: {args.input}", file=sys.stderr)
        return 2

    result = run(cfg, Path(args.input), Path(args.output), Path(args.state), reset=args.reset)
    print(summary(result.report))
    for outcome in result.failed:
        print(f"FAILED {outcome.name}: {outcome.message}", file=sys.stderr)
    for check in result.report["checks"]:
        if not check["passed"]:
            print(f"CHECK FAILED: {check['name']} ({check['detail']})", file=sys.stderr)
    return 0 if result.ok else 1


def summary(report: dict) -> str:
    t = report["totals"]
    lines = [
        "",
        f"{t['rows_read']} read = {t['rows_loaded']} loaded + {t['rows_quarantined']} quarantined + {t['duplicates_removed']} duplicates removed",
    ]
    for b in report["backfill"]:
        days = ", ".join(p["txn_date"] for p in b["partitions"])
        lines.append(
            f"backfill: {b['file']} added {b['rows']} row(s) to {days} "
            f"(credits {Decimal(b['credit_total']):+,.2f}, debits {Decimal(b['debit_total']):+,.2f})"
        )
    return "\n".join(lines)


if __name__ == "__main__":
    raise SystemExit(main())
