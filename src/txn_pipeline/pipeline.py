from __future__ import annotations

import logging
from collections import Counter
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

import pandas as pd

from .config import SourceConfig
from .dedup import add_txn_ids, split_batch
from .export import write_outputs
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
    report: dict[str, Any]

    @property
    def failed(self) -> list[FileOutcome]:
        return [o for o in self.outcomes if o.status == FAILED]

    @property
    def ok(self) -> bool:
        return not self.failed and self.report["all_checks_passed"]


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
        report, outputs = write_outputs(store, cfg, Path(output_dir))
    return RunResult(outcomes, outputs, report)


def process_file(path: Path, cfg: SourceConfig, store: Store) -> FileOutcome:
    data = path.read_bytes()
    sha = sha256_of(data)
    earlier = _already_loaded(store, sha, path.name, cfg)
    if earlier is not None:
        return earlier

    try:
        raw = read_raw_file(path.name, data, cfg)
    except FileFormatError as exc:
        return FileOutcome(path.name, sha, FAILED, str(exc))

    valid, rejected = validate_rows(raw.complete, cfg)
    rejected = _stack(rejected, malformed_rejects(raw.malformed, raw.header_width))
    valid = add_txn_ids(valid, cfg)

    with store.transaction():
        earlier = _already_loaded(store, sha, path.name, cfg)
        if earlier is not None:
            return earlier

        existing = store.existing_ids(valid["txn_id"].unique().tolist())
        new, within_file, already_loaded = split_batch(valid, existing)

        is_late = late_mask(new, raw.batch_date, cfg.late_after_days)
        loaded_rows = new.assign(
            source=cfg.name,
            batch_sha256=sha,
            batch_file=path.name,
            load_type=["BACKFILL" if late else "REGULAR" for late in is_late],
        )
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

        backfill_rows = loaded_rows[is_late]
        backfill_dates = sorted(backfill_rows["txn_date"].unique())
        before = store.partition_totals(cfg.name, backfill_dates)
        store.insert_transactions(loaded_rows)
        after = store.partition_totals(cfg.name, backfill_dates)
        store.insert_quarantine(quarantine_rows)
        store.insert_duplicates(duplicate_rows)

        debit, credit = _direction_totals(new)
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
                    "late_dated": _late_stats(valid, new, within_file, already_loaded, raw.batch_date, cfg),
                    "backfill": _backfill_stats(backfill_rows, before, after, raw.batch_date, cfg) if backfill_dates else None,
                },
                "loaded_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            }
        )
    return FileOutcome(path.name, sha, LOADED, counts={**counts, "rows_backfill": len(backfill_rows)})


def _already_loaded(store: Store, sha: str, name: str, cfg: SourceConfig) -> FileOutcome | None:
    owner = store.batch_owner(sha)
    if owner is None:
        return None
    source, earlier_name = owner
    if source != cfg.name:
        message = f"identical bytes were already loaded for source '{source}' as {earlier_name}; refusing to load them as '{cfg.name}'"
        return FileOutcome(name, sha, FAILED, message)
    return FileOutcome(name, sha, SKIPPED, f"identical content already loaded as {earlier_name}")


def late_mask(frame: pd.DataFrame, batch_date: date | None, threshold_days: int) -> pd.Series:
    """True for rows whose business date is at least `threshold_days` older than the file's batch date."""
    if batch_date is None or frame.empty:
        return pd.Series(False, index=frame.index, dtype=bool)
    flags = [(batch_date - date.fromisoformat(d)).days >= threshold_days for d in frame["txn_date"]]
    return pd.Series(flags, index=frame.index, dtype=bool)


def _late_stats(valid, new, within_file, already_loaded, batch_date, cfg) -> dict[str, int]:
    def late(frame: pd.DataFrame) -> int:
        return int(late_mask(frame, batch_date, cfg.late_after_days).sum())

    return {
        "rows": late(valid),
        "new": late(new),
        "already_loaded": late(already_loaded),
        "within_file_repeats": late(within_file),
    }


def _backfill_stats(rows: pd.DataFrame, before: dict, after: dict, batch_date: date, cfg: SourceConfig) -> dict[str, Any]:
    """Impact of late rows on the business dates they belong to, measured on the table itself."""
    partitions = []
    for txn_date in sorted(before):
        added = rows[rows["txn_date"] == txn_date]
        added_debit, added_credit = _direction_totals(added)
        delta = {
            "rows": after[txn_date]["rows"] - before[txn_date]["rows"],
            "debit": after[txn_date]["debit"] - before[txn_date]["debit"],
            "credit": after[txn_date]["credit"] - before[txn_date]["credit"],
        }
        if (delta["rows"], delta["debit"], delta["credit"]) != (len(added), added_debit, added_credit):
            raise RuntimeError(f"backfill into {txn_date} changed the partition by {delta}, expected {len(added)} rows")
        partitions.append(
            {
                "txn_date": txn_date,
                "days_late": (batch_date - date.fromisoformat(txn_date)).days,
                "before": _snapshot(before[txn_date], cfg),
                "after": _snapshot(after[txn_date], cfg),
                "delta": _snapshot(delta, cfg),
            }
        )

    by_account = []
    for account, group in rows.groupby("account_number", sort=True):
        debit, credit = _direction_totals(group)
        by_account.append({"account_number": account, **_snapshot({"rows": len(group), "debit": debit, "credit": credit}, cfg)})

    debit, credit = _direction_totals(rows)
    return {
        **_snapshot({"rows": len(rows), "debit": debit, "credit": credit}, cfg),
        "partitions": partitions,
        "by_account": by_account,
    }


def _snapshot(bucket: dict[str, Any], cfg: SourceConfig) -> dict[str, Any]:
    return {
        "rows": bucket["rows"],
        "debit_total": _money(bucket["debit"], cfg),
        "credit_total": _money(bucket["credit"], cfg),
        "net": _money(bucket["credit"] - bucket["debit"], cfg),
    }


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
    backfill = f" (backfill {c['rows_backfill']})" if c["rows_backfill"] else ""
    return (
        f"read={c['rows_read']} loaded={c['rows_loaded']}{backfill} quarantined={c['rows_quarantined']} "
        f"duplicates={duplicates} (within file {c['dup_within_file']}, already loaded {c['dup_already_loaded']})"
    )
