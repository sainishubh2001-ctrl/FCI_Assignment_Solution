from __future__ import annotations

import argparse
import logging
import sys
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
    for outcome in result.failed:
        print(f"FAILED {outcome.name}: {outcome.message}", file=sys.stderr)
    return 1 if result.failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
