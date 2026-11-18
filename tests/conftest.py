from __future__ import annotations

from pathlib import Path

import pytest

from txn_pipeline.config import SourceConfig, load_config

REPO = Path(__file__).resolve().parents[1]
SAMPLE_DIR = REPO / "sample_data" / "assignment-1-pipeline"
BANK_A_CONFIG = REPO / "config" / "bank_a.yaml"


@pytest.fixture(scope="session")
def cfg() -> SourceConfig:
    return load_config(BANK_A_CONFIG)
