from __future__ import annotations

import json
from decimal import Decimal

from conftest import BANK_A_CONFIG
from helpers import make_row

from txn_pipeline.cli import main
from txn_pipeline.reconcile import build_report, render_markdown
from txn_pipeline.store import Store


def report(ws):
    return json.loads((ws.output / "reconciliation_summary.json").read_text(encoding="utf-8"))


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


class TestTotals:
    def test_rows_read_equal_loaded_plus_quarantined_plus_duplicates(self, ws):
        seed(ws)
        ws.run()
        t = report(ws)["totals"]

        assert t["rows_read"] == t["rows_loaded"] + t["rows_quarantined"] + t["duplicates_removed"]
        assert t["duplicates_within_file"] + t["duplicates_already_loaded"] == t["duplicates_removed"]

    def test_exact_figures(self, ws):
        seed(ws)
        ws.run()
        t = report(ws)["totals"]
        assert t == {
            "files_loaded": 2,
            "rows_read": 8,
            "rows_loaded": 3,
            "rows_quarantined": 3,
            "distinct_quarantined_records": 2,
            "duplicates_removed": 2,
            "duplicates_within_file": 1,
            "duplicates_already_loaded": 1,
        }

    def test_per_file_figures(self, ws):
        seed(ws)
        ws.run()
        rows = {f["file"]: f for f in report(ws)["files"]}
        first, second = rows["batch_2026-08-01.csv"], rows["batch_2026-08-02.csv"]
        assert (first["rows_read"], first["rows_loaded"], first["rows_quarantined"], first["duplicates_within_file"]) == (5, 2, 2, 1)
        assert (second["rows_read"], second["rows_loaded"], second["rows_quarantined"], second["duplicates_already_loaded"]) == (3, 1, 1, 1)

    def test_quarantine_by_reason_counts_rows_and_distinct_records(self, ws):
        seed(ws)
        ws.run()
        by_reason = {q["reason_code"]: q for q in report(ws)["quarantine_by_reason"]}
        assert by_reason["BOTH_DEBIT_AND_CREDIT"]["rows"] == 2
        assert by_reason["BOTH_DEBIT_AND_CREDIT"]["distinct_records"] == 1
        assert by_reason["MISSING_REF_NO"] == {"reason_code": "MISSING_REF_NO", "rows": 1, "distinct_records": 1}

    def test_canonical_by_date_uses_exact_decimals(self, ws):
        ws.add("batch_2026-08-01.csv", [make_row(ref="A", debit="0.10"), make_row(ref="B", debit="0.20"), make_row(ref="C", debit="", credit="0.30")])
        ws.run()
        day = report(ws)["canonical"]["by_txn_date"][0]
        assert (day["debit_total"], day["credit_total"], day["net"]) == ("0.30", "0.30", "0.00")


class TestControlChecks:
    def test_all_checks_pass_on_a_normal_run(self, ws):
        seed(ws)
        ws.run()
        assert report(ws)["all_checks_passed"] is True

    def test_a_missing_canonical_row_is_detected(self, ws, cfg):
        seed(ws)
        ws.run()
        with Store(ws.state / "pipeline.db") as store:
            store.conn.execute("PRAGMA foreign_keys = OFF")
            store.conn.execute("DELETE FROM transactions WHERE ref_no = 'R1'")
            tampered = build_report(store, cfg)

        failed = {c["name"] for c in tampered["control_checks"] if not c["passed"]}
        assert tampered["all_checks_passed"] is False
        assert "Canonical row count = rows loaded across all files" in failed
        assert any("Ledger counters" in name for name in failed)

    def test_an_altered_amount_is_detected_by_the_control_totals(self, ws, cfg):
        seed(ws)
        ws.run()
        with Store(ws.state / "pipeline.db") as store:
            store.conn.execute("UPDATE transactions SET amount = '999.00' WHERE ref_no = 'R1'")
            tampered = build_report(store, cfg)
        assert any("Control totals" in c["name"] and not c["passed"] for c in tampered["control_checks"])

    def test_a_failed_reconciliation_makes_the_cli_exit_nonzero(self, ws, capsys):
        seed(ws)
        args = ["--config", str(BANK_A_CONFIG), "--input", str(ws.input), "--output", str(ws.output), "--state", str(ws.state)]
        assert main(args) == 0
        with Store(ws.state / "pipeline.db") as store:
            store.conn.execute("PRAGMA foreign_keys = OFF")
            store.conn.execute("DELETE FROM transactions WHERE ref_no = 'R3'")
        capsys.readouterr()

        assert main(args) == 1
        assert "RECONCILIATION FAILED" in capsys.readouterr().err


class TestWarnings:
    def test_account_type_drift_is_reported_but_nothing_is_rejected(self, ws):
        ws.add("batch_2026-08-01.csv", [make_row(ref="R1", typ="SAVINGS"), make_row(ref="R2", typ="CURRENT"), make_row(ref="R3", account="AC2", typ="SALARY")])
        ws.run()
        drift = report(ws)["data_quality_warnings"]["account_type_drift"]
        assert drift["accounts_with_multiple_types"] == 1
        assert drift["accounts"] == [{"account_number": "AC1", "account_types": ["CURRENT", "SAVINGS"]}]
        assert len(ws.table("canonical_transactions.csv")) == 3

    def test_same_ref_on_different_transactions_is_flagged(self, ws):
        ws.add("batch_2026-08-01.csv", [make_row(ref="R1", debit="100"), make_row(ref="R1", debit="250")])
        ws.run()
        shared = report(ws)["data_quality_warnings"]["same_ref_no_on_different_transactions"]
        assert shared["count"] == 1
        assert shared["transactions"] == [{"account_number": "AC1", "ref_no": "R1", "transactions": 2}]


class TestRendering:
    def test_json_has_no_run_timestamps_and_markdown_states_the_identity(self, ws, cfg):
        seed(ws)
        ws.run()
        text = (ws.output / "reconciliation_summary.json").read_text(encoding="utf-8")
        assert "loaded_at" not in text
        markdown = (ws.output / "reconciliation_summary.md").read_text(encoding="utf-8")
        assert "**8 read = 3 loaded + 3 quarantined + 2 duplicates removed**" in markdown

    def test_money_in_markdown_is_grouped_and_signed(self, ws, cfg):
        ws.add("batch_2026-08-01.csv", [make_row(ref="R1", debit="", credit="1234567.89")])
        ws.run()
        with Store(ws.state / "pipeline.db") as store:
            markdown = render_markdown(build_report(store, cfg))
        assert "1,234,567.89" in markdown

    def test_json_amounts_are_strings_not_floats(self, ws):
        ws.add("batch_2026-08-01.csv", [make_row(ref="R1", debit="0.10")])
        ws.run()
        day = report(ws)["canonical"]["by_txn_date"][0]
        assert isinstance(day["debit_total"], str) and Decimal(day["debit_total"]) == Decimal("0.10")
