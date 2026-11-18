from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

import pandas as pd

from .config import SourceConfig
from .reconcile import build_report, render_markdown
from .store import Store


def csv_text(frame: pd.DataFrame) -> str:
    return frame.to_csv(index=False, lineterminator="\n")


def write_if_changed(path: Path, text: str) -> bool:
    """Atomic write that leaves the file (and its timestamp) alone when the content is identical."""
    data = text.encode("utf-8")
    if path.exists() and path.read_bytes() == data:
        return False
    temp = path.with_name(path.name + ".tmp")
    temp.write_bytes(data)
    os.replace(temp, path)
    return True


def write_outputs(store: Store, cfg: SourceConfig, output_dir: Path) -> tuple[dict[str, Any], dict[str, bool]]:
    """Publish the datasets and the reconciliation summary; returns the report and which files changed."""
    output_dir.mkdir(parents=True, exist_ok=True)
    report = build_report(store, cfg)
    documents = {
        "canonical_transactions.csv": csv_text(store.canonical(cfg.name)),
        "quarantine.csv": csv_text(store.quarantined(cfg.name)),
        "duplicates_removed.csv": csv_text(store.duplicates_removed(cfg.name)),
        "reconciliation_summary.json": json.dumps(report, indent=2, ensure_ascii=False) + "\n",
        "reconciliation_summary.md": render_markdown(report),
    }
    return report, {name: write_if_changed(output_dir / name, text) for name, text in documents.items()}
