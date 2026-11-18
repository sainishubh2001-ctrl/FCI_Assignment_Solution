from __future__ import annotations

from helpers import HEADER, make_row

from txn_pipeline.pipeline import LOADED, SKIPPED


def test_header_only_file_is_a_recorded_empty_delivery(ws):
    (ws.input / "batch_2026-08-01.csv").write_text(HEADER + "\r\n")
    first = ws.run()
    assert first.outcomes[0].status == LOADED
    assert first.outcomes[0].counts["rows_read"] == 0
    assert len(ws.table("canonical_transactions.csv")) == 0
    assert ws.run().outcomes[0].status == SKIPPED


def test_a_file_where_every_row_is_bad_loads_nothing_and_quarantines_everything(ws):
    ws.add("batch_2026-08-01.csv", [
        make_row(ref="R1", debit="", credit=""),
        make_row(ref="R2", date="31-02-2026"),
        make_row(ref="", debit="5"),
    ])
    result = ws.run()

    counts = result.outcomes[0].counts
    assert (counts["rows_read"], counts["rows_loaded"], counts["rows_quarantined"]) == (3, 0, 3)
    assert len(ws.table("canonical_transactions.csv")) == 0
    assert ws.table("quarantine.csv")["reason_code"].tolist() == ["NO_DEBIT_OR_CREDIT", "INVALID_DATE", "MISSING_REF_NO"]


def test_ragged_lines_flow_through_the_pipeline_into_quarantine(ws):
    ws.add("batch_2026-08-01.csv", [make_row(ref="R1"), make_row(ref="R2") + ",EXTRA", "AC1,01-08-2026", make_row(ref="R3")])
    result = ws.run()

    counts = result.outcomes[0].counts
    assert (counts["rows_read"], counts["rows_loaded"], counts["rows_quarantined"]) == (4, 2, 2)
    quarantine = ws.table("quarantine.csv")
    assert quarantine["reason_code"].tolist() == ["MALFORMED_ROW", "MALFORMED_ROW"]
    assert quarantine["source_line"].tolist() == ["3", "4"]
    assert quarantine["raw_line"].tolist()[1] == "AC1,01-08-2026"


def test_a_real_calendar_error_is_quarantined_with_its_reason(ws):
    ws.add("batch_2026-08-01.csv", [make_row(ref="R1", date="31-02-2026"), make_row(ref="R2", date="01-08-2026")])
    ws.run()
    quarantine = ws.table("quarantine.csv")
    assert quarantine["reason_code"].tolist() == ["INVALID_DATE"]
    assert "31-02-2026" in quarantine["reason_detail"].iloc[0]


def test_surrounding_whitespace_and_case_do_not_defeat_dedup(ws):
    ws.add("batch_2026-08-01.csv", [make_row(account="ac1", ref=" ref1 "), make_row(account="AC1", ref="REF1")])
    ws.run()
    assert len(ws.table("canonical_transactions.csv")) == 1
