from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from .config import SourceConfig
from .dedup import add_txn_ids, split_batch
from .extract import FileFormatError, discover_files, read_raw_file, sha256_of
from .report import publish, sums
from .store import Store
from .validate import validate_rows

log = logging.getLogger("txn_pipeline")

LOADED, SKIPPED, FAILED = "LOADED", "SKIPPED", "FAILED"


@dataclass
class FileOutcome:
    name: str
    status: str
    message: str = ""
    counts: dict[str, int] = field(default_factory=dict)

    def describe(self) -> str:
        if self.status != LOADED:
            return f"{self.status} ({self.message})"
        c = self.counts
        return (
            f"read={c['rows_read']} loaded={c['rows_loaded']} quarantined={c['rows_quarantined']} "
            f"duplicates={c['dup_within_file'] + c['dup_already_loaded']} "
            f"(in file {c['dup_within_file']}, already loaded {c['dup_already_loaded']})"
        )


@dataclass
class RunResult:
    outcomes: list[FileOutcome]
    report: dict[str, Any]

    @property
    def failed(self) -> list[FileOutcome]:
        return [o for o in self.outcomes if o.status == FAILED]

    @property
    def ok(self) -> bool:
        return not self.failed and self.report["all_checks_passed"]


def run(cfg: SourceConfig, input_dir: Path, output_dir: Path, state_dir: Path, reset: bool = False) -> RunResult:
    db = Path(state_dir) / "pipeline.db"
    if reset:
        db.unlink(missing_ok=True)
    files = discover_files(Path(input_dir), cfg)
    with Store(db) as store:
        outcomes = []
        for n, path in enumerate(files, start=1):
            outcomes.append(process_file(path, cfg, store))
            log.info("[%d/%d] %s: %s", n, len(files), path.name, outcomes[-1].describe())
        report = publish(store, cfg, Path(output_dir))
    return RunResult(outcomes, report)


def process_file(path: Path, cfg: SourceConfig, store: Store) -> FileOutcome:
    """Load one file in a single transaction: either all of it is stored, or none of it."""
    data = path.read_bytes()
    sha = sha256_of(data)
    try:
        with store.transaction():
            earlier = store.batch_file(sha)
            if earlier:
                return FileOutcome(path.name, SKIPPED, f"same content as {earlier}")
            return FileOutcome(path.name, LOADED, counts=_load(path.name, sha, data, cfg, store))
    except FileFormatError as exc:
        return FileOutcome(path.name, FAILED, str(exc))


def _load(name: str, sha: str, data: bytes, cfg: SourceConfig, store: Store) -> dict[str, int]:
    raw = read_raw_file(name, data, cfg)
    valid, rejected = validate_rows(raw, cfg)
    valid = add_txn_ids(valid, cfg)
    new, repeats, stored = split_batch(valid, store.existing_ids(valid["txn_id"].tolist()))
    new = new.assign(
        source=cfg.name,
        batch_sha256=sha,
        load_type=_load_types(new["txn_date"], raw.batch_date, cfg.late_after_days),
    )

    counts = {
        "rows_read": raw.rows_read,
        "rows_loaded": len(new),
        "rows_quarantined": len(rejected),
        "dup_within_file": len(repeats),
        "dup_already_loaded": len(stored),
    }
    if raw.rows_read != len(new) + len(rejected) + len(repeats) + len(stored):
        raise RuntimeError(f"{name}: rows read do not add up: {counts}")

    _, debit, credit = sums(new)
    store.insert_batch(
        batch_sha256=sha,
        batch_file=name,
        batch_date=raw.batch_date.isoformat() if raw.batch_date else None,
        seq=store.next_seq(),
        **counts,
        debit_total=format(debit, "f"),
        credit_total=format(credit, "f"),
        loaded_at=datetime.now(timezone.utc).isoformat(timespec="seconds"),
    )
    store.insert("transactions", new)
    store.insert("quarantine", rejected.assign(batch_sha256=sha))
    store.insert("duplicates", _duplicate_rows(sha, repeats, stored))
    return counts


def _load_types(txn_dates: pd.Series, batch_date: date | None, late_after_days: int) -> list[str]:
    """BACKFILL when the business date is at least `late_after_days` older than the file's batch date."""
    if batch_date is None:
        return ["REGULAR"] * len(txn_dates)
    return [
        "BACKFILL" if (batch_date - date.fromisoformat(day)).days >= late_after_days else "REGULAR"
        for day in txn_dates
    ]


def _duplicate_rows(sha: str, repeats: pd.DataFrame, stored: pd.DataFrame) -> pd.DataFrame:
    rows = [(sha, line, "WITHIN_FILE", txn) for line, txn in zip(repeats["source_line"], repeats["txn_id"])]
    rows += [(sha, line, "ALREADY_LOADED", txn) for line, txn in zip(stored["source_line"], stored["txn_id"])]
    return pd.DataFrame(rows, columns=["batch_sha256", "source_line", "duplicate_kind", "txn_id"], dtype=object)
