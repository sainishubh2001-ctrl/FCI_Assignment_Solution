from __future__ import annotations

import logging
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import pandas as pd

from .config import SourceConfig
from .dedup import add_txn_ids, split_batch
from .export import write_dataset_outputs
from .extract import FileFormatError, discover_files, read_raw_file, sha256_of
from .store import DUPLICATE_COLUMNS, Store
from .validate import malformed_rejects, validate_rows

log = logging.getLogger("txn_pipeline")

LOADED, SKIPPED, FAILED = "LOADED", "SKIPPED", "FAILED"


@dataclass
class FileOutcome:
    name: str
    sha256: str
    status: str
    message: str = ""
    counts: dict[str, int] = field(default_factory=dict)


@dataclass
class RunResult:
    outcomes: list[FileOutcome]
    outputs: dict[str, bool]

    @property
    def failed(self) -> list[FileOutcome]:
        return [o for o in self.outcomes if o.status == FAILED]


def run(cfg: SourceConfig, input_dir: Path, output_dir: Path, state_dir: Path, reset: bool = False) -> RunResult:
    state_path = Path(state_dir) / "pipeline.db"
    if reset and state_path.exists():
        state_path.unlink()

    files = discover_files(Path(input_dir), cfg)
    with Store(state_path) as store:
        outcomes = []
        for position, path in enumerate(files, start=1):
            outcome = process_file(path, cfg, store)
            outcomes.append(outcome)
            log.info("[%d/%d] %s: %s", position, len(files), outcome.name, _describe(outcome))
        outputs = write_dataset_outputs(store, cfg, Path(output_dir))
    return RunResult(outcomes, outputs)


def process_file(path: Path, cfg: SourceConfig, store: Store) -> FileOutcome:
    data = path.read_bytes()
    sha = sha256_of(data)
    earlier_name = store.batch_file(sha)
    if earlier_name is not None:
        return FileOutcome(path.name, sha, SKIPPED, f"identical content already loaded as {earlier_name}")

    try:
        raw = read_raw_file(path.name, data, cfg)
    except FileFormatError as exc:
        return FileOutcome(path.name, sha, FAILED, str(exc))

    valid, rejected = validate_rows(raw.complete, cfg)
    rejected = _stack(rejected, malformed_rejects(raw.malformed, raw.header_width))
    valid = add_txn_ids(valid, cfg)

    with store.transaction():
        if store.batch_file(sha) is not None:
            return FileOutcome(path.name, sha, SKIPPED, "identical content already loaded")

        existing = store.existing_ids(valid["txn_id"].unique().tolist())
        new, within_file, already_loaded = split_batch(valid, existing)

        loaded_rows = new.assign(source=cfg.name, batch_sha256=sha, batch_file=path.name, load_type="REGULAR")
        quarantine_rows = rejected.assign(source=cfg.name, batch_sha256=sha, batch_file=path.name)
        duplicate_rows = pd.DataFrame(
            [(cfg.name, sha, line, "WITHIN_FILE", tid) for line, tid in zip(within_file["source_line"], within_file["txn_id"])]
            + [(cfg.name, sha, line, "ALREADY_LOADED", tid) for line, tid in zip(already_loaded["source_line"], already_loaded["txn_id"])],
            columns=DUPLICATE_COLUMNS,
            dtype=object,
        )

        counts = {
            "rows_read": raw.rows_read,
            "rows_loaded": len(new),
            "rows_quarantined": len(rejected),
            "dup_within_file": len(within_file),
            "dup_already_loaded": len(already_loaded),
        }
        accounted = counts["rows_loaded"] + counts["rows_quarantined"] + counts["dup_within_file"] + counts["dup_already_loaded"]
        if accounted != counts["rows_read"]:
            raise RuntimeError(f"{path.name}: {counts['rows_read']} rows read but {accounted} accounted for")

        debit, credit = _direction_totals(new)
        store.insert_transactions(loaded_rows)
        store.insert_quarantine(quarantine_rows)
        store.insert_duplicates(duplicate_rows)
        store.insert_batch(
            {
                "batch_sha256": sha,
                "source": cfg.name,
                "batch_file": path.name,
                "batch_date": raw.batch_date.isoformat() if raw.batch_date else None,
                "seq": store.next_seq(),
                **counts,
                "blank_lines": raw.blank_lines,
                "stats_json": {
                    "quarantined_by_reason": dict(sorted(Counter(rejected["reason_code"]).items())),
                    "loaded_debit_total": _money(debit, cfg),
                    "loaded_credit_total": _money(credit, cfg),
                },
                "loaded_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            }
        )
    return FileOutcome(path.name, sha, LOADED, counts=counts)


def _direction_totals(frame: pd.DataFrame) -> tuple[Decimal, Decimal]:
    debit = sum((a for a, d in zip(frame["amount"], frame["direction"]) if d == "DEBIT"), Decimal(0))
    credit = sum((a for a, d in zip(frame["amount"], frame["direction"]) if d == "CREDIT"), Decimal(0))
    return debit, credit


def _money(value: Decimal, cfg: SourceConfig) -> str:
    return format(value.quantize(Decimal(1).scaleb(-cfg.decimal_places)), "f")


def _stack(first: pd.DataFrame, second: pd.DataFrame) -> pd.DataFrame:
    if second.empty:
        return first
    if first.empty:
        return second
    return pd.concat([first, second], ignore_index=True)


def _describe(outcome: FileOutcome) -> str:
    if outcome.status != LOADED:
        return f"{outcome.status} ({outcome.message})"
    c = outcome.counts
    duplicates = c["dup_within_file"] + c["dup_already_loaded"]
    return (
        f"read={c['rows_read']} loaded={c['rows_loaded']} quarantined={c['rows_quarantined']} "
        f"duplicates={duplicates} (within file {c['dup_within_file']}, already loaded {c['dup_already_loaded']})"
    )
