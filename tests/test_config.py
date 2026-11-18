from __future__ import annotations

import copy

import pytest
import yaml
from conftest import BANK_A_CONFIG

from txn_pipeline.config import ConfigError, load_config, parse_config


@pytest.fixture
def raw_config():
    return yaml.safe_load(BANK_A_CONFIG.read_text(encoding="utf-8"))


def mutated(raw_config, path, value):
    cfg = copy.deepcopy(raw_config)
    node = cfg
    for part in path[:-1]:
        node = node[part]
    if value is KeyError:
        del node[path[-1]]
    else:
        node[path[-1]] = value
    return cfg


def test_sample_config_loads_with_expected_policy(cfg):
    assert cfg.name == "bank_a"
    assert cfg.dedup_key == ("account_number", "txn_date", "txn_time", "ref_no", "direction", "amount")
    assert cfg.required_fields == ("account_number", "ref_no")
    assert cfg.late_after_days == 1


@pytest.mark.parametrize(
    "path,value,message",
    [
        (("source", "name"), KeyError, "source.name"),
        (("source", "delimiter"), ";;", "single character"),
        (("source", "batch_date_regex"), "(", "not a valid regex"),
        (("source", "batch_date_regex"), r"batch_\d+", "exactly one capture group"),
        (("columns", "debit"), None, "must be mapped"),
        (("columns", "amount"), "x", "unknown canonical field"),
        (("formats", "date"), KeyError, "formats.date"),
        (("formats", "decimal_places"), 9, "between 0 and 6"),
        (("validation", "required"), ["balance"], "must be one of"),
        (("dedup", "key"), KeyError, "dedup.key"),
        (("dedup", "key"), ["account_number", "bogus"], "unknown field"),
        (("backfill", "late_after_days"), 0, ">= 1"),
    ],
)
def test_invalid_configs_are_rejected_with_a_clear_message(raw_config, path, value, message):
    with pytest.raises(ConfigError, match=message):
        parse_config(mutated(raw_config, path, value))


def test_key_field_that_the_source_does_not_supply_is_rejected(raw_config):
    broken = mutated(raw_config, ("columns", "txn_time"), None)
    with pytest.raises(ConfigError, match="'txn_time' is not mapped"):
        parse_config(broken)


def test_unreadable_config_file_is_a_config_error(tmp_path):
    with pytest.raises(ConfigError, match="cannot read config"):
        load_config(tmp_path / "missing.yaml")
