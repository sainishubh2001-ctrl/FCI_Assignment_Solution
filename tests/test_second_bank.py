"""Onboarding a second bank with a different format and key is a config change, not a code change."""

from __future__ import annotations

import json

import pytest
from conftest import BANK_B_SAMPLE_DIR, SAMPLE_DIR
from helpers import Workspace, make_row

from txn_pipeline.pipeline import FAILED, LOADED, SKIPPED, run

DAY1, LATE = "bankb_20260801.csv", "bankb_20260804_late.csv"


@pytest.fixture
def bank_b(tmp_path, cfg_b):
    ws = Workspace(tmp_path, cfg_b)
    for path in BANK_B_SAMPLE_DIR.glob("bankb_*.csv"):
        (ws.input / path.name).write_bytes(path.read_bytes())
    result = ws.run()
    report = json.loads((ws.output / "reconciliation_summary.json").read_text(encoding="utf-8"))
    return ws, result, report


def test_bank_b_totals_reconcile(bank_b):
    _, result, report = bank_b
    assert [o.status for o in result.outcomes] == [LOADED, LOADED]
    assert report["totals"] == {
        "files_loaded": 2,
        "rows_read": 9,
        "rows_loaded": 5,
        "rows_quarantined": 2,
        "distinct_quarantined_records": 2,
        "duplicates_removed": 2,
        "duplicates_within_file": 1,
        "duplicates_already_loaded": 1,
    }
    assert report["all_checks_passed"] is True
    assert report["dedup_key"] == ["account_number", "txn_date", "ref_no", "direction", "amount"]


def test_bank_b_values_are_normalised_into_the_same_canonical_shape(bank_b):
    ws, _, _ = bank_b
    table = ws.table("canonical_transactions.csv")
    atm = table[table["ref_no"] == "UTR0001"].iloc[0]
    assert (atm["account_number"], atm["amount"], atm["balance"], atm["account_type"], atm["direction"]) == (
        "5001", "1250.00", "98750.00", "CURRENT", "DEBIT",
    )
    assert (table["txn_time"] == "").all()  # this bank supplies no time of day


def test_bank_b_legitimate_repeat_with_a_different_reference_is_kept(bank_b):
    ws, _, _ = bank_b
    table = ws.table("canonical_transactions.csv")
    assert sorted(table[table["account_number"] == "5001"]["ref_no"]) == ["UTR0001", "UTR0002", "UTR0007"]


def test_bank_b_rejects_are_quarantined_with_reasons(bank_b):
    ws, _, _ = bank_b
    quarantine = ws.table("quarantine.csv")
    assert quarantine[["source_line", "reason_code"]].values.tolist() == [
        ["6", "BOTH_DEBIT_AND_CREDIT"],
        ["7", "INVALID_DATE"],
    ]


def test_bank_b_backfill_and_same_day_rows_are_told_apart(bank_b):
    ws, _, report = bank_b
    table = ws.table("canonical_transactions.csv")
    assert dict(zip(table["ref_no"], table["load_type"]))["UTR0006"] == "BACKFILL"
    assert dict(zip(table["ref_no"], table["load_type"]))["UTR0007"] == "REGULAR"
    (backfill,) = report["backfill"]
    (partition,) = backfill["partitions"]
    assert (partition["before"]["rows"], partition["after"]["rows"], backfill["credit_total"]) == (3, 4, "2500.50")
    assert backfill["late_dated_rows"] == {"rows": 2, "new": 1, "already_loaded": 1, "within_file_repeats": 0}


def test_bank_b_is_idempotent(bank_b):
    ws, _, _ = bank_b
    before = ws.output_hashes()
    assert [o.status for o in ws.run().outcomes] == [SKIPPED, SKIPPED]
    ws.run(reset=True)
    assert ws.output_hashes() == before


def test_two_banks_share_one_state_database_without_colliding(tmp_path, cfg, cfg_b):
    state = tmp_path / "state"
    a_in, b_in = tmp_path / "a_in", tmp_path / "b_in"
    a_in.mkdir()
    b_in.mkdir()
    # the same account, date, reference and amount arrive from both banks
    (a_in / "batch_2026-08-01.csv").write_bytes(
        ("account_number,txn_date,txn_time,description,ref_no,debit,credit,balance,account_type\r\n"
         + make_row(account="5001", ref="UTR0001", debit="1250.00") + "\r\n").encode()
    )
    (b_in / "bankb_20260801.csv").write_bytes((BANK_B_SAMPLE_DIR / DAY1).read_bytes())

    run(cfg, a_in, tmp_path / "out_a", state)
    run(cfg_b, b_in, tmp_path / "out_b", state)

    import pandas as pd

    a = pd.read_csv(tmp_path / "out_a" / "canonical_transactions.csv", dtype=str)
    b = pd.read_csv(tmp_path / "out_b" / "canonical_transactions.csv", dtype=str)
    assert (set(a["source"]), len(a)) == ({"bank_a"}, 1)
    assert (set(b["source"]), len(b)) == ({"bank_b"}, 3)
    assert not set(a["txn_id"]) & set(b["txn_id"])


def test_identical_bytes_are_never_loaded_under_a_second_source(tmp_path, cfg, cfg_b):
    state = tmp_path / "state"
    b_in, a_in = tmp_path / "b_in", tmp_path / "a_in"
    b_in.mkdir()
    a_in.mkdir()
    payload = (BANK_B_SAMPLE_DIR / DAY1).read_bytes()
    (b_in / "bankb_20260801.csv").write_bytes(payload)
    (a_in / "batch_2026-08-01.csv").write_bytes(payload)

    run(cfg_b, b_in, tmp_path / "out_b", state)
    result = run(cfg, a_in, tmp_path / "out_a", state)

    assert result.outcomes[0].status == FAILED
    assert "already loaded for source 'bank_b'" in result.outcomes[0].message


def test_both_sample_sets_are_self_consistent_with_their_own_configs(cfg, cfg_b):
    assert list(SAMPLE_DIR.glob(cfg.file_glob)) and list(BANK_B_SAMPLE_DIR.glob(cfg_b.file_glob))
    assert not list(SAMPLE_DIR.glob(cfg_b.file_glob)) and not list(BANK_B_SAMPLE_DIR.glob(cfg.file_glob))
