from __future__ import annotations

import os
from pathlib import Path

import pandas as pd

from .config import SourceConfig
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


def write_dataset_outputs(store: Store, cfg: SourceConfig, output_dir: Path) -> dict[str, bool]:
    output_dir.mkdir(parents=True, exist_ok=True)
    datasets = {
        "canonical_transactions.csv": store.canonical(cfg.name),
        "quarantine.csv": store.quarantined(cfg.name),
        "duplicates_removed.csv": store.duplicates_removed(cfg.name),
    }
    return {name: write_if_changed(output_dir / name, csv_text(frame)) for name, frame in datasets.items()}
