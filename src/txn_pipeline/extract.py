from __future__ import annotations

import csv
import hashlib
import io
import re
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import NamedTuple

import pandas as pd

from .config import INPUT_FIELDS, SourceConfig


class FileFormatError(Exception):
    """The file as a whole can't be read: bad encoding, no header, or an expected column is missing."""


class Record(NamedTuple):
    line: int
    fields: list[str]
    raw: str


@dataclass
class RawFile:
    batch_date: date | None
    rows: pd.DataFrame  # rows with the right number of fields
    ragged: list[Record]  # rows with the wrong number of fields
    header_width: int

    @property
    def rows_read(self) -> int:
        return len(self.rows) + len(self.ragged)


def sha256_of(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def batch_date_of(name: str, cfg: SourceConfig) -> date | None:
    match = re.search(cfg.batch_date_regex, name) if cfg.batch_date_regex else None
    if not match:
        return None
    try:
        return datetime.strptime(match.group(1), cfg.batch_date_format).date()
    except ValueError:
        return None


def discover_files(folder: Path, cfg: SourceConfig) -> list[Path]:
    """Matching files in arrival order: batch date from the name, then the name itself."""
    files = [p for p in Path(folder).glob(cfg.file_glob) if p.is_file()]
    return sorted(files, key=lambda p: (batch_date_of(p.name, cfg) or date.max, p.name))


def read_raw_file(name: str, data: bytes, cfg: SourceConfig) -> RawFile:
    # csv module rather than pandas.read_csv: a ragged line must be kept, not raise and not be skipped
    encoding = "utf-8-sig" if cfg.encoding.lower() in ("utf-8", "utf8") else cfg.encoding
    try:
        text = data.decode(encoding)
    except UnicodeDecodeError as exc:
        raise FileFormatError(f"{name}: not valid {cfg.encoding}: {exc}") from exc

    lines = list(io.StringIO(text, newline=""))
    reader = csv.reader(lines, delimiter=cfg.delimiter)
    header, records, last = None, [], 0
    try:
        for fields in reader:
            first, last = last + 1, reader.line_num
            if not fields:  # blank line
                continue
            if header is None:
                header = [cell.strip().lower() for cell in fields]
            else:
                records.append(Record(first, fields, "".join(lines[first - 1 : last]).rstrip("\r\n")))
    except csv.Error as exc:
        raise FileFormatError(f"{name}: unreadable CSV near line {last + 1}: {exc}") from exc
    if header is None:
        raise FileFormatError(f"{name}: no header row")

    position = {cell: i for i, cell in enumerate(header)}
    missing = [col for col in cfg.columns.values() if col and col.lower() not in position]
    if missing:
        raise FileFormatError(f"{name}: missing column(s) {missing}")

    good = [r for r in records if len(r.fields) == len(header)]
    ragged = [r for r in records if len(r.fields) != len(header)]
    columns = {
        field: [r.fields[position[col.lower()]] if col else None for r in good]
        for field, col in cfg.columns.items()
    }
    rows = pd.DataFrame(columns, columns=list(INPUT_FIELDS), dtype=object)
    rows["source_line"] = pd.Series([r.line for r in good], dtype="int64")
    rows["raw_line"] = pd.Series([r.raw for r in good], dtype=object)
    return RawFile(batch_date_of(name, cfg), rows, ragged, len(header))
