from __future__ import annotations

import hashlib
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from txn_pipeline.config import SourceConfig
from txn_pipeline.pipeline import RunResult, run

HEADER = "account_number,txn_date,txn_time,description,ref_no,debit,credit,balance,account_type"


@dataclass
class Workspace:
    """A throwaway input/output/state folder trio plus helpers to drive and inspect the pipeline."""

    root: Path
    cfg: SourceConfig

    def __post_init__(self) -> None:
        self.input.mkdir(parents=True, exist_ok=True)

    @property
    def input(self) -> Path:
        return self.root / "in"

    @property
    def output(self) -> Path:
        return self.root / "out"

    @property
    def state(self) -> Path:
        return self.root / "state"

    def add(self, name: str, rows: list[str], eol: str = "\r\n") -> Path:
        return write_batch(self.input, name, rows, eol)

    def run(self, reset: bool = False, cfg: SourceConfig | None = None) -> RunResult:
        return run(cfg or self.cfg, self.input, self.output, self.state, reset=reset)

    def table(self, name: str) -> pd.DataFrame:
        return pd.read_csv(self.output / name, dtype=str, keep_default_na=False)

    def output_hashes(self) -> dict[str, str]:
        return {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(self.output.iterdir()) if p.is_file()}



def make_row(
    account: str = "AC1",
    date: str = "01-08-2026",
    time: str = "10:00:00",
    desc: str = "POS PURCHASE",
    ref: str = "REF1",
    debit: str = "100.00",
    credit: str = "",
    balance: str = "900.00",
    typ: str = "SAVINGS",
) -> str:
    return ",".join([account, date, time, desc, ref, debit, credit, balance, typ])


def write_batch(directory: Path, name: str, rows: list[str], eol: str = "\r\n") -> Path:
    path = Path(directory) / name
    path.write_bytes((eol.join([HEADER, *rows]) + eol).encode("utf-8"))
    return path


def raw_rec(**overrides: str) -> dict:
    rec = {
        "account_number": "AC1",
        "txn_date": "01-08-2026",
        "txn_time": "10:00:00",
        "description": "POS PURCHASE",
        "ref_no": "REF1",
        "debit": "100.00",
        "credit": "",
        "balance": "900.00",
        "account_type": "SAVINGS",
        "source_line": 2,
        "raw_line": "raw",
    }
    rec.update(overrides)
    return rec
