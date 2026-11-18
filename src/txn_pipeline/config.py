from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import yaml

INPUT_FIELDS = (
    "account_number", "txn_date", "txn_time", "description", "ref_no",
    "debit", "credit", "balance", "account_type",
)
TEXT_FIELDS = ("account_number", "ref_no", "description", "account_type")
KEY_FIELDS = (
    "account_number", "txn_date", "txn_time", "description", "ref_no",
    "balance", "account_type", "direction", "amount",
)
MUST_MAP = ("account_number", "txn_date", "debit", "credit")


class ConfigError(ValueError):
    pass


@dataclass(frozen=True)
class SourceConfig:
    name: str
    file_glob: str
    encoding: str
    delimiter: str
    batch_date_regex: str | None
    batch_date_format: str
    columns: dict[str, str | None]
    date_formats: tuple[str, ...]
    time_formats: tuple[str, ...]
    decimal_places: int
    decimal_separator: str
    thousands_separator: str | None
    required_fields: tuple[str, ...]
    dedup_key: tuple[str, ...]
    late_after_days: int


def load_config(path: str | Path) -> SourceConfig:
    try:
        raw = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
    except (OSError, yaml.YAMLError) as exc:
        raise ConfigError(f"cannot read config {path}: {exc}") from exc
    return parse_config(raw)


def parse_config(raw: dict) -> SourceConfig:
    try:
        source, formats = raw["source"], raw["formats"]
        unknown = set(raw["columns"]) - set(INPUT_FIELDS)
        if unknown:
            raise ConfigError(f"columns: unknown field(s) {sorted(unknown)}")
        cfg = SourceConfig(
            name=source["name"],
            file_glob=source.get("file_glob", "*.csv"),
            encoding=source.get("encoding", "utf-8"),
            delimiter=source.get("delimiter", ","),
            batch_date_regex=source.get("batch_date_regex"),
            batch_date_format=source.get("batch_date_format", "%Y-%m-%d"),
            columns={f: raw["columns"].get(f) or None for f in INPUT_FIELDS},
            date_formats=tuple(formats["date"]),
            time_formats=tuple(formats.get("time", ("%H:%M:%S", "%H:%M"))),
            decimal_places=formats.get("decimal_places", 2),
            decimal_separator=formats.get("decimal_separator", "."),
            thousands_separator=formats.get("thousands_separator"),
            required_fields=tuple(raw.get("validation", {}).get("required", ("account_number",))),
            dedup_key=tuple(raw["dedup"]["key"]),
            late_after_days=raw.get("backfill", {}).get("late_after_days", 1),
        )
    except (KeyError, TypeError, AttributeError) as exc:
        raise ConfigError(f"missing or malformed setting: {exc}") from exc

    unmapped = [f for f in MUST_MAP if not cfg.columns[f]]
    if unmapped:
        raise ConfigError(f"columns: {unmapped} must be mapped to a source header")
    unknown = [f for f in cfg.dedup_key if f not in KEY_FIELDS]
    unknown += [f for f in cfg.required_fields if f not in TEXT_FIELDS]
    if unknown:
        raise ConfigError(f"dedup.key / validation.required: unknown or unsupported field(s) {unknown}")
    unmapped = {f for f in (*cfg.dedup_key, *cfg.required_fields) if f in INPUT_FIELDS and not cfg.columns[f]}
    if unmapped:
        raise ConfigError(f"used in dedup.key / validation.required but not mapped in columns: {sorted(unmapped)}")
    return cfg
