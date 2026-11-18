from __future__ import annotations

from pathlib import Path

import pytest
from helpers import Workspace

from txn_pipeline.config import SourceConfig, load_config

REPO = Path(__file__).resolve().parents[1]
SAMPLE_DIR = REPO / "sample_data" / "assignment-1-pipeline"
BANK_A_CONFIG = REPO / "config" / "bank_a.yaml"
BANK_B_CONFIG = REPO / "config" / "bank_b_example.yaml"
BANK_B_SAMPLE_DIR = REPO / "sample_data" / "bank_b_example"


@pytest.fixture(scope="session")
def cfg() -> SourceConfig:
    return load_config(BANK_A_CONFIG)


@pytest.fixture(scope="session")
def cfg_b() -> SourceConfig:
    return load_config(BANK_B_CONFIG)


@pytest.fixture
def ws(tmp_path, cfg) -> Workspace:
    return Workspace(tmp_path, cfg)
