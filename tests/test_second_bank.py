"""A second bank with a different layout and key loads through the same code, with only a config file."""

from __future__ import annotations

import pytest
from conftest import BANK_B_DIR
from helpers import Workspace

from txn_pipeline.pipeline import LOADED, SKIPPED


@pytest.fixture
def bank_b(tmp_path, cfg_b):
    ws = Workspace(tmp_path, cfg_b)
    for path in BANK_B_DIR.glob("bankb_*.csv"):
        (ws.input / path.name).write_bytes(path.read_bytes())
    return ws, ws.run()


def test_the_totals_reconcile(bank_b):
    ws, result = bank_b
    assert [o.status for o in result.outcomes] == [LOADED, LOADED]
    assert ws.report()["totals"] == {
        "files_loaded": 2,
        "rows_read": 9,
        "rows_loaded": 5,
        "rows_quarantined": 2,
        "duplicates_removed": 2,
        "duplicates_within_file": 1,
        "duplicates_already_loaded": 1,
    }
    assert ws.report()["all_checks_passed"]


def test_values_end_up_in_the_same_canonical_shape(bank_b):
    ws, _ = bank_b
    atm = ws.table().set_index("ref_no").loc["UTR0001"]
    assert (atm["account_number"], atm["amount"], atm["balance"], atm["account_type"], atm["direction"]) == (
        "5001", "1250.00", "98750.00", "CURRENT", "DEBIT",
    )
    assert (ws.table()["txn_time"] == "").all()  # this bank sends no time of day


def test_two_identical_looking_withdrawals_with_different_references_both_survive(bank_b):
    ws, _ = bank_b
    table = ws.table()
    assert sorted(table[table["account_number"] == "5001"]["ref_no"]) == ["UTR0001", "UTR0002", "UTR0007"]


def test_bad_rows_are_quarantined_with_their_reason(bank_b):
    ws, _ = bank_b
    assert ws.table("quarantine.csv")[["source_line", "reason_code"]].values.tolist() == [
        ["6", "BOTH_DEBIT_AND_CREDIT"],
        ["7", "INVALID_DATE"],
    ]


def test_the_late_file_mixes_a_backfill_with_a_same_day_row(bank_b):
    ws, _ = bank_b
    loaded = dict(zip(ws.table()["ref_no"], ws.table()["load_type"]))
    assert (loaded["UTR0006"], loaded["UTR0007"]) == ("BACKFILL", "REGULAR")
    (backfill,) = ws.report()["backfill"]
    assert (backfill["rows"], backfill["credit_total"]) == (1, "2500.50")


def test_a_rerun_changes_nothing(bank_b):
    ws, _ = bank_b
    before = ws.hashes()
    assert [o.status for o in ws.run().outcomes] == [SKIPPED, SKIPPED]
    ws.run(reset=True)
    assert ws.hashes() == before
