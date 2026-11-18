"""Golden tests on the three files exactly as delivered (sample_data/assignment-1-pipeline)."""

from __future__ import annotations

import csv
import json

import pytest
from conftest import SAMPLE_DIR

from txn_pipeline.pipeline import SKIPPED

DAY1, DAY2, LATE = "batch_2026-08-01.csv", "batch_2026-08-02.csv", "batch_2026-08-04_late_correction.csv"


@pytest.fixture
def loaded(ws):
    for path in SAMPLE_DIR.glob("batch_*.csv"):
        (ws.input / path.name).write_bytes(path.read_bytes())
    result = ws.run()
    report = json.loads((ws.output / "reconciliation_summary.json").read_text(encoding="utf-8"))
    return ws, result, report


def test_headline_numbers(loaded):
    _, _, report = loaded
    assert report["totals"] == {
        "files_loaded": 3,
        "rows_read": 555,
        "rows_loaded": 361,
        "rows_quarantined": 6,
        "distinct_quarantined_records": 3,
        "duplicates_removed": 188,
        "duplicates_within_file": 8,
        "duplicates_already_loaded": 180,
    }
    assert report["all_checks_passed"] is True


def test_per_file_numbers(loaded):
    _, _, report = loaded
    files = {f["file"]: f for f in report["files"]}
    assert [files[n][k] for n in (DAY1, DAY2, LATE) for k in ("rows_read", "rows_loaded", "rows_quarantined")] == [
        187, 180, 3,
        365, 178, 3,
        3, 3, 0,
    ]
    assert (files[DAY1]["duplicates_within_file"], files[DAY1]["duplicates_already_loaded"]) == (4, 0)
    assert (files[DAY2]["duplicates_within_file"], files[DAY2]["duplicates_already_loaded"]) == (4, 180)


def test_the_full_day_one_redelivery_adds_nothing_new(loaded):
    _, _, report = loaded
    day2 = next(f for f in report["files"] if f["file"] == DAY2)
    assert day2["late_dated_rows"] == {"rows": 184, "new": 0, "already_loaded": 180, "within_file_repeats": 4}


def test_each_quarantine_reason_is_caught_with_its_source_line(loaded):
    ws, _, _ = loaded
    quarantine = ws.table("quarantine.csv")
    found = quarantine[["batch_file", "source_line", "reason_code"]].values.tolist()
    assert found == [
        [DAY1, "186", "MISSING_REF_NO"],
        [DAY1, "187", "BOTH_DEBIT_AND_CREDIT"],
        [DAY1, "188", "NO_DEBIT_OR_CREDIT"],
        [DAY2, "186", "MISSING_REF_NO"],
        [DAY2, "187", "BOTH_DEBIT_AND_CREDIT"],
        [DAY2, "188", "NO_DEBIT_OR_CREDIT"],
    ]
    assert quarantine["record_fingerprint"].nunique() == 3
    assert (quarantine["raw_line"] != "").all()


def test_canonical_dataset_shape(loaded):
    ws, _, report = loaded
    table = ws.table("canonical_transactions.csv")
    assert len(table) == 361
    assert table["txn_id"].is_unique
    assert table["ref_no"].is_unique  # ref_no is unique per transaction in this feed
    assert {d["txn_date"]: d["rows"] for d in report["canonical"]["by_txn_date"]} == {"2026-08-01": 183, "2026-08-02": 178}
    assert (table["txn_time"].str.len() == 8).all()  # every time is zero-padded HH:MM:SS
    assert table["amount"].str.fullmatch(r"\d+\.\d{2}").all()


def test_legitimate_same_day_repeats_survive(loaded):
    ws, _, _ = loaded
    table = ws.table("canonical_transactions.csv")
    zomato = table[(table["account_number"] == "AC100000001") & (table["description"] == "UPI-ZOMATO-ORDER") & (table["txn_date"] == "2026-08-01")]
    assert sorted(zomato["ref_no"]) == ["REF770285", "REF914981"]


def test_the_four_planted_duplicates_collapse_to_one_row_each(loaded):
    ws, _, _ = loaded
    table = ws.table("canonical_transactions.csv")
    for ref in ("REF973627", "REF814134", "REF233004", "REF531432"):
        assert (table["ref_no"] == ref).sum() == 1


def test_backfill_effect(loaded):
    ws, _, report = loaded
    (backfill,) = report["backfill"]
    assert backfill["file"] == LATE
    assert (backfill["rows"], backfill["debit_total"], backfill["credit_total"], backfill["net"]) == (3, "18500.00", "97000.00", "78500.00")
    (partition,) = backfill["partitions"]
    assert (partition["txn_date"], partition["days_late"]) == ("2026-08-01", 3)
    assert (partition["before"]["rows"], partition["after"]["rows"]) == (180, 183)
    assert {a["account_number"]: a["net"] for a in backfill["by_account"]} == {
        "AC100000002": "-18500.00",
        "AC100000005": "15000.00",
        "AC100000009": "82000.00",
    }
    table = ws.table("canonical_transactions.csv")
    assert sorted(table[table["load_type"] == "BACKFILL"]["ref_no"]) == ["REF900001", "REF900002", "REF900003"]


def test_account_type_drift_is_surfaced(loaded):
    _, _, report = loaded
    drift = report["data_quality_warnings"]["account_type_drift"]
    assert (drift["accounts_with_multiple_types"], drift["accounts_total"]) == (25, 40)


def test_replaying_every_file_in_every_way_changes_nothing(loaded):
    ws, _, _ = loaded
    reference = ws.output_hashes()

    assert all(o.status == SKIPPED for o in ws.run().outcomes)
    for name in (DAY1, DAY2, LATE):
        (ws.input / f"replay of {name}").write_bytes((ws.input / name).read_bytes())
    assert all(o.status == SKIPPED for o in ws.run().outcomes)
    ws.run(reset=True)

    assert ws.output_hashes() == reference


def test_published_csvs_are_plain_utf8_with_lf_line_endings(loaded):
    ws, _, _ = loaded
    for name in ("canonical_transactions.csv", "quarantine.csv", "duplicates_removed.csv"):
        data = (ws.output / name).read_bytes()
        assert b"\r" not in data and data.endswith(b"\n")
        with open(ws.output / name, newline="", encoding="utf-8") as handle:
            widths = {len(row) for row in csv.reader(handle)}
        assert len(widths) == 1
