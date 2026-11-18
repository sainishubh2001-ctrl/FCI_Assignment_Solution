from __future__ import annotations

from conftest import BANK_A_CONFIG
from helpers import make_row

from txn_pipeline.cli import main
from txn_pipeline.store import Store


def seed(ws):
    ws.add("batch_2026-08-01.csv", [
        make_row(ref="R1", debit="100"),
        make_row(ref="R2", debit="", credit="40"),
        make_row(ref="R2", debit="", credit="40"),
        make_row(ref="BAD", debit="5", credit="5"),
        make_row(ref="", debit="9"),
    ])
    ws.add("batch_2026-08-02.csv", [
        make_row(ref="R1", date="1/08/2026"),
        make_row(ref="BAD", date="1/08/2026", debit="5", credit="5"),
        make_row(ref="R3", date="2/08/2026", debit="60"),
    ])


def cli_args(ws):
    return ["--config", str(BANK_A_CONFIG), "--input", str(ws.input), "--output", str(ws.output), "--state", str(ws.state)]


def test_the_totals_add_up(ws):
    seed(ws)
    ws.run()

    assert ws.report()["totals"] == {
        "files_loaded": 2,
        "rows_read": 8,
        "rows_loaded": 3,
        "rows_quarantined": 3,
        "duplicates_removed": 2,
        "duplicates_within_file": 1,
        "duplicates_already_loaded": 1,
    }


def test_each_file_reports_its_own_numbers(ws):
    seed(ws)
    ws.run()

    first, second = ws.report()["files"]
    assert [first[k] for k in ("rows_read", "rows_loaded", "rows_quarantined", "duplicates_within_file")] == [5, 2, 2, 1]
    assert [second[k] for k in ("rows_read", "rows_loaded", "rows_quarantined", "duplicates_already_loaded")] == [3, 1, 1, 1]


def test_quarantined_rows_are_counted_by_reason(ws):
    seed(ws)
    ws.run()
    assert ws.report()["quarantine_by_reason"] == [
        {"reason_code": "BOTH_DEBIT_AND_CREDIT", "rows": 2},
        {"reason_code": "MISSING_REF_NO", "rows": 1},
    ]


def test_amounts_are_summed_exactly(ws):
    ws.add("batch_2026-08-01.csv", [make_row(ref="A", debit="0.10"), make_row(ref="B", debit="0.20"), make_row(ref="C", debit="", credit="0.30")])
    ws.run()
    (day,) = ws.report()["canonical_by_date"]
    assert (day["debit_total"], day["credit_total"], day["net"]) == ("0.30", "0.30", "0.00")


def test_account_type_drift_is_reported_but_nothing_is_rejected_for_it(ws):
    ws.add("batch_2026-08-01.csv", [make_row(ref="R1", typ="SAVINGS"), make_row(ref="R2", typ="CURRENT"), make_row(ref="R3", account="AC2")])
    ws.run()

    assert ws.report()["warnings"]["account_type_drift"] == {"accounts": 1, "of": 2, "examples": {"AC1": ["CURRENT", "SAVINGS"]}}
    assert len(ws.table()) == 3


def test_a_row_that_goes_missing_after_loading_is_caught_and_fails_the_run(ws, capsys):
    seed(ws)
    assert main(cli_args(ws)) == 0
    with Store(ws.state / "pipeline.db") as store:
        store.conn.execute("DELETE FROM transactions WHERE ref_no = 'R3'")
    capsys.readouterr()

    assert main(cli_args(ws)) == 1
    assert "CHECK FAILED" in capsys.readouterr().err
    assert ws.report()["all_checks_passed"] is False


def test_an_amount_changed_after_loading_is_caught_by_the_control_totals(ws):
    seed(ws)
    ws.run()
    with Store(ws.state / "pipeline.db") as store:
        store.conn.execute("UPDATE transactions SET amount = '999.00' WHERE ref_no = 'R1'")

    report = ws.run().report
    assert [c["passed"] for c in report["checks"]] == [True, False]
