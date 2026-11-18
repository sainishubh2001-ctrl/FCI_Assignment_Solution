from __future__ import annotations

import csv
import hashlib
import io
import re
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path

import pandas as pd

from .config import INPUT_FIELDS, SourceConfig


class FileFormatError(Exception):
    """The file as a whole cannot be processed: bad encoding, no header, missing columns, unreadable CSV."""


@dataclass(frozen=True)
class RawRecord:
    line_no: int
    fields: list[str]
    raw_line: str


@dataclass
class RawFile:
    name: str
    sha256: str
    batch_date: date | None
    complete: pd.DataFrame
    malformed: list[RawRecord]
    header_width: int
    rows_read: int
    blank_lines: int


def sha256_of(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def parse_batch_date(name: str, cfg: SourceConfig) -> date | None:
    if not cfg.batch_date_regex:
        return None
    match = re.search(cfg.batch_date_regex, name)
    if not match:
        return None
    try:
        return datetime.strptime(match.group(1), cfg.batch_date_format).date()
    except ValueError:
        return None


def discover_files(input_dir: Path, cfg: SourceConfig) -> list[Path]:
    """Matching files in arrival order: batch date from the file name, then name."""
    files = [p for p in Path(input_dir).glob(cfg.file_glob) if p.is_file()]
    return sorted(files, key=lambda p: (parse_batch_date(p.name, cfg) or date.max, p.name))


def read_raw_file(name: str, data: bytes, cfg: SourceConfig) -> RawFile:
    try:
        text = data.decode(cfg.decode_encoding)
    except UnicodeDecodeError as exc:
        raise FileFormatError(f"{name}: not valid {cfg.encoding}: {exc}") from exc

    lines = list(io.StringIO(text, newline=""))
    reader = csv.reader(lines, delimiter=cfg.delimiter)
    header: list[str] | None = None
    records: list[RawRecord] = []
    blank_lines = 0
    consumed = 0
    try:
        for fields in reader:
            first_line, consumed = consumed + 1, reader.line_num
            if not fields:
                blank_lines += 1
                continue
            if header is None:
                header = [cell.strip() for cell in fields]
                continue
            raw_line = "".join(lines[first_line - 1 : consumed]).rstrip("\r\n")
            records.append(RawRecord(first_line, fields, raw_line))
    except csv.Error as exc:
        raise FileFormatError(f"{name}: unreadable CSV near line {consumed + 1}: {exc}") from exc
    if header is None:
        raise FileFormatError(f"{name}: file has no header row")

    position = {cell.lower(): idx for idx, cell in enumerate(header)}
    missing = [src for src in cfg.columns.values() if src and src.lower() not in position]
    if missing:
        raise FileFormatError(f"{name}: missing expected column(s) {missing}; found header {header}")

    expected = len(header)
    complete = [rec for rec in records if len(rec.fields) == expected]
    malformed = [rec for rec in records if len(rec.fields) != expected]

    columns = {
        field: [rec.fields[position[src.lower()]] if src else None for rec in complete]
        for field, src in cfg.columns.items()
    }
    frame = pd.DataFrame(columns, columns=list(INPUT_FIELDS), dtype=object)
    frame["source_line"] = pd.Series([rec.line_no for rec in complete], dtype="int64")
    frame["raw_line"] = pd.Series([rec.raw_line for rec in complete], dtype=object)

    return RawFile(
        name=name,
        sha256=sha256_of(data),
        batch_date=parse_batch_date(name, cfg),
        complete=frame,
        malformed=malformed,
        header_width=expected,
        rows_read=len(records),
        blank_lines=blank_lines,
    )
