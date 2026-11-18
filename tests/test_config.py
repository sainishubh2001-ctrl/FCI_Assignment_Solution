from __future__ import annotations

import copy

import pytest
import yaml
from conftest import BANK_A_CONFIG

from txn_pipeline.config import ConfigError, load_config, parse_config


@pytest.fixture
def raw():
    return yaml.safe_load(BANK_A_CONFIG.read_text(encoding="utf-8"))


def test_the_sample_config_describes_bank_a(cfg):
    assert cfg.name == "bank_a"
    assert cfg.dedup_key == ("account_number", "txn_date", "txn_time", "ref_no", "direction", "amount")
    assert cfg.required_fields == ("account_number", "ref_no")


@pytest.mark.parametrize(
    "mutate, message",
    [
        (lambda c: c["source"].pop("name"), "missing or malformed"),
        (lambda c: c["formats"].pop("date"), "missing or malformed"),
        (lambda c: c["dedup"].pop("key"), "missing or malformed"),
        (lambda c: c["columns"].update(debit=None), "must be mapped"),
        (lambda c: c["columns"].update(amount="x"), "unknown field"),
        (lambda c: c["dedup"].update(key=["account_number", "bogus"]), "unknown or unsupported"),
        (lambda c: c["validation"].update(required=["balance"]), "unknown or unsupported"),
        (lambda c: c["columns"].update(txn_time=None), "not mapped"),
    ],
)
def test_a_bad_config_is_rejected_with_a_clear_message(raw, mutate, message):
    broken = copy.deepcopy(raw)
    mutate(broken)
    with pytest.raises(ConfigError, match=message):
        parse_config(broken)


def test_an_unreadable_config_file_is_a_config_error(tmp_path):
    with pytest.raises(ConfigError, match="cannot read config"):
        load_config(tmp_path / "missing.yaml")
