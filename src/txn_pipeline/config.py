from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

INPUT_FIELDS = (
    "account_number",
    "txn_date",
    "txn_time",
    "description",
    "ref_no",
    "debit",
    "credit",
    "balance",
    "account_type",
)
MANDATORY_COLUMNS = ("account_number", "txn_date", "debit", "credit")
KEY_FIELDS = (
    "account_number",
    "txn_date",
    "txn_time",
    "description",
    "ref_no",
    "direction",
    "amount",
    "balance",
    "account_type",
)
REQUIRABLE_FIELDS = ("account_number", "ref_no", "description", "account_type")


class ConfigError(ValueError):
    """The source configuration is invalid."""


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
    min_year: int
    max_year: int
    required_fields: tuple[str, ...]
    dedup_key: tuple[str, ...]
    late_after_days: int

    @property
    def decode_encoding(self) -> str:
        return "utf-8-sig" if self.encoding.lower().replace("_", "-") == "utf-8" else self.encoding


def load_config(path: str | Path) -> SourceConfig:
    path = Path(path)
    try:
        raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    except (OSError, yaml.YAMLError) as exc:
        raise ConfigError(f"cannot read config {path}: {exc}") from exc
    return parse_config(raw)


def parse_config(raw: Any) -> SourceConfig:
    if not isinstance(raw, dict):
        raise ConfigError("config must be a mapping with source/columns/formats/dedup sections")

    source = _section(raw, "source")
    formats = _section(raw, "formats")
    validation = _section(raw, "validation")
    dedup = _section(raw, "dedup")
    backfill = _section(raw, "backfill")

    name = source.get("name")
    if not name or not isinstance(name, str):
        raise ConfigError("source.name is required")

    delimiter = source.get("delimiter", ",")
    if not isinstance(delimiter, str) or len(delimiter) != 1:
        raise ConfigError("source.delimiter must be a single character")

    batch_date_regex = source.get("batch_date_regex")
    if batch_date_regex is not None:
        try:
            if re.compile(batch_date_regex).groups != 1:
                raise ConfigError("source.batch_date_regex must contain exactly one capture group")
        except re.error as exc:
            raise ConfigError(f"source.batch_date_regex is not a valid regex: {exc}") from exc

    columns = _parse_columns(_section(raw, "columns"))

    date_formats = _string_list(formats.get("date"), "formats.date", required=True)
    time_formats = _string_list(formats.get("time", ["%H:%M:%S", "%H:%M"]), "formats.time")
    decimal_places = formats.get("decimal_places", 2)
    if not isinstance(decimal_places, int) or not 0 <= decimal_places <= 6:
        raise ConfigError("formats.decimal_places must be an integer between 0 and 6")

    required = tuple(_string_list(validation.get("required", ["account_number"]), "validation.required"))
    for field in required:
        if field not in REQUIRABLE_FIELDS:
            raise ConfigError(f"validation.required: '{field}' must be one of {list(REQUIRABLE_FIELDS)}")
        if columns[field] is None:
            raise ConfigError(f"validation.required: '{field}' is required but not mapped in columns")

    key = tuple(_string_list(dedup.get("key"), "dedup.key", required=True))
    for field in key:
        if field not in KEY_FIELDS:
            raise ConfigError(f"dedup.key: unknown field '{field}', expected one of {list(KEY_FIELDS)}")
        if field in INPUT_FIELDS and columns[field] is None:
            raise ConfigError(f"dedup.key: '{field}' is not mapped in columns")

    late_after_days = backfill.get("late_after_days", 1)
    if not isinstance(late_after_days, int) or late_after_days < 1:
        raise ConfigError("backfill.late_after_days must be an integer >= 1")

    return SourceConfig(
        name=name,
        file_glob=source.get("file_glob", "*.csv"),
        encoding=source.get("encoding", "utf-8"),
        delimiter=delimiter,
        batch_date_regex=batch_date_regex,
        batch_date_format=source.get("batch_date_format", "%Y-%m-%d"),
        columns=columns,
        date_formats=tuple(date_formats),
        time_formats=tuple(time_formats),
        decimal_places=decimal_places,
        decimal_separator=formats.get("decimal_separator", "."),
        thousands_separator=formats.get("thousands_separator"),
        min_year=int(formats.get("min_year", 2000)),
        max_year=int(formats.get("max_year", 2100)),
        required_fields=required,
        dedup_key=key,
        late_after_days=late_after_days,
    )


def _section(raw: dict, name: str) -> dict:
    section = raw.get(name)
    if section is None:
        return {}
    if not isinstance(section, dict):
        raise ConfigError(f"'{name}' must be a mapping")
    return section


def _parse_columns(section: dict) -> dict[str, str | None]:
    unknown = set(section) - set(INPUT_FIELDS)
    if unknown:
        raise ConfigError(f"columns: unknown canonical field(s) {sorted(unknown)}; expected {list(INPUT_FIELDS)}")
    columns = {field: (str(section[field]).strip() if section.get(field) else None) for field in INPUT_FIELDS}
    unmapped = [field for field in MANDATORY_COLUMNS if columns[field] is None]
    if unmapped:
        raise ConfigError(f"columns: {unmapped} must be mapped to a source header")
    return columns


def _string_list(value: Any, label: str, required: bool = False) -> list[str]:
    if value is None:
        if required:
            raise ConfigError(f"{label} is required")
        return []
    if not isinstance(value, list) or not value or not all(isinstance(v, str) for v in value):
        raise ConfigError(f"{label} must be a non-empty list of strings")
    return value
