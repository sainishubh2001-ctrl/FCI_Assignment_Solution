"""Golden tests on the three files exactly as delivered."""

from __future__ import annotations

import pytest
from conftest import SAMPLE_DIR

from txn_pipeline.pipeline import SKIPPED

DAY1, DAY2, LATE = "batch_2026-08-01.csv", "batch_2026-08-02.csv", "batch_2026-08-04_late_correction.csv"


@pytest.fixture
def loaded(ws):
    for path in SAMPLE_DIR.glob("batch_*.csv"):
        (ws.input / path.name).write_bytes(path.read_bytes())
    ws.run()
    return ws


def test_headline_numbers(loaded):
    report = loaded.report()
    assert report["totals"] == {
        "files_loaded": 3,
        "rows_read": 555,
        "rows_loaded": 361,
        "rows_quarantined": 6,
        "duplicates_removed": 188,
        "duplicates_within_file": 8,
        "duplicates_already_loaded": 180,
    }
    assert report["all_checks_passed"]


def test_numbers_per_file(loaded):
    files = {f["file"]: f for f in loaded.report()["files"]}
    assert [(files[n]["rows_read"], files[n]["rows_loaded"], files[n]["rows_quarantined"]) for n in (DAY1, DAY2, LATE)] == [
        (187, 180, 3), (365, 178, 3), (3, 3, 0),
    ]
    assert (files[DAY2]["duplicates_within_file"], files[DAY2]["duplicates_already_loaded"]) == (4, 180)


def test_every_bad_row_is_quarantined_with_its_reason_and_line(loaded):
    quarantine = loaded.table("quarantine.csv")
    assert quarantine[["batch_file", "source_line", "reason_code"]].values.tolist() == [
        [DAY1, "186", "MISSING_REF_NO"],
        [DAY1, "187", "BOTH_DEBIT_AND_CREDIT"],
        [DAY1, "188", "NO_DEBIT_OR_CREDIT"],
        [DAY2, "186", "MISSING_REF_NO"],
        [DAY2, "187", "BOTH_DEBIT_AND_CREDIT"],
        [DAY2, "188", "NO_DEBIT_OR_CREDIT"],
    ]


def test_canonical_dataset_shape(loaded):
    table = loaded.table()
    assert len(table) == 361 and table["txn_id"].is_unique and table["ref_no"].is_unique
    assert table["txn_date"].value_counts().to_dict() == {"2026-08-01": 183, "2026-08-02": 178}
    assert (table["txn_time"].str.len() == 8).all()  # all times zero-padded
    assert table["amount"].str.fullmatch(r"\d+\.\d{2}").all()


def test_legitimate_same_day_repeats_survive(loaded):
    table = loaded.table()
    zomato = table[(table["account_number"] == "AC100000001") & (table["description"] == "UPI-ZOMATO-ORDER") & (table["txn_date"] == "2026-08-01")]
    assert sorted(zomato["ref_no"]) == ["REF770285", "REF914981"]


def test_the_four_planted_duplicates_collapse_to_one_row_each(loaded):
    table = loaded.table()
    for ref in ("REF973627", "REF814134", "REF233004", "REF531432"):
        assert (table["ref_no"] == ref).sum() == 1


def test_the_late_file_is_a_backfill_into_day_one(loaded):
    (backfill,) = loaded.report()["backfill"]
    assert backfill["file"] == LATE
    assert (backfill["rows"], backfill["debit_total"], backfill["credit_total"], backfill["net"]) == (3, "18500.00", "97000.00", "78500.00")
    (day,) = backfill["partitions"]
    assert (day["txn_date"], day["days_late"], day["before"]["rows"], day["after"]["rows"]) == ("2026-08-01", 3, 180, 183)
    assert {a["account_number"]: a["net"] for a in backfill["by_account"]} == {
        "AC100000002": "-18500.00", "AC100000005": "15000.00", "AC100000009": "82000.00",
    }
    table = loaded.table()
    assert sorted(table[table["load_type"] == "BACKFILL"]["ref_no"]) == ["REF900001", "REF900002", "REF900003"]


def test_account_type_drift_is_reported(loaded):
    drift = loaded.report()["warnings"]["account_type_drift"]
    assert (drift["accounts"], drift["of"]) == (25, 40)


def test_every_way_of_replaying_the_files_leaves_the_output_unchanged(loaded):
    reference = loaded.hashes()

    assert all(o.status == SKIPPED for o in loaded.run().outcomes)
    for name in (DAY1, DAY2, LATE):
        (loaded.input / f"replay of {name}").write_bytes((loaded.input / name).read_bytes())
    assert all(o.status == SKIPPED for o in loaded.run().outcomes)
    loaded.run(reset=True)

    assert loaded.hashes() == reference
