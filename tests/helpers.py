from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from txn_pipeline.config import SourceConfig
from txn_pipeline.pipeline import RunResult, run

HEADER = "account_number,txn_date,txn_time,description,ref_no,debit,credit,balance,account_type"


def make_row(account="AC1", date="01-08-2026", time="10:00:00", desc="POS", ref="R1",
             debit="100.00", credit="", balance="900.00", typ="SAVINGS") -> str:
    return ",".join([account, date, time, desc, ref, debit, credit, balance, typ])


@dataclass
class Workspace:
    """Throwaway input/output/state folders plus shortcuts to run and inspect the pipeline."""

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
        path = self.input / name
        path.write_bytes((eol.join([HEADER, *rows]) + eol).encode("utf-8"))
        return path

    def run(self, reset: bool = False, cfg: SourceConfig | None = None) -> RunResult:
        return run(cfg or self.cfg, self.input, self.output, self.state, reset=reset)

    def table(self, name: str = "canonical_transactions.csv") -> pd.DataFrame:
        return pd.read_csv(self.output / name, dtype=str, keep_default_na=False)

    def report(self) -> dict:
        return json.loads((self.output / "reconciliation_summary.json").read_text(encoding="utf-8"))

    def hashes(self) -> dict[str, str]:
        return {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(self.output.iterdir())}
