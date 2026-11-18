from __future__ import annotations

import dataclasses

import pandas as pd
import pytest
from helpers import make_row

from txn_pipeline.dedup import make_txn_id, split_batch


def test_a_redelivery_in_another_format_collapses_to_nothing_new(ws):
    ws.add("batch_2026-08-01.csv", [
        make_row(ref="R1", time="09:05:00", debit="21400.6"),
        make_row(ref="R2", time="14:30:00", debit="", credit="5000"),
    ])
    # the same two transactions, re-sent with the day-2 feed's quirks: other date style, unpadded hour, other precision
    ws.add("batch_2026-08-02.csv", [
        make_row(ref="R1", date="1/08/2026", time="9:05:00", debit="21400.60"),
        make_row(ref="R2", date="1/08/2026", time="14:30:00", debit="", credit="5000.00"),
    ])
    result = ws.run()

    assert result.outcomes[1].counts["dup_already_loaded"] == 2
    assert len(ws.table()) == 2


def test_repeats_inside_a_file_collapse_and_are_listed(ws):
    ws.add("batch_2026-08-01.csv", [make_row(ref="R1"), make_row(ref="R2"), make_row(ref="R1")])
    ws.run()

    assert len(ws.table()) == 2
    audit = ws.table("duplicates_removed.csv")
    assert audit[["source_line", "duplicate_kind", "kept_source_line"]].values.tolist() == [["4", "WITHIN_FILE", "2"]]


def test_a_redelivered_row_points_at_the_row_that_was_kept(ws):
    ws.add("batch_2026-08-01.csv", [make_row(ref="R1")])
    ws.add("batch_2026-08-02.csv", [make_row(ref="R1", date="1/08/2026")])
    ws.run()

    audit = ws.table("duplicates_removed.csv")
    assert audit[["batch_file", "duplicate_kind", "kept_batch_file"]].values.tolist() == [
        ["batch_2026-08-02.csv", "ALREADY_LOADED", "batch_2026-08-01.csv"]
    ]


@pytest.mark.parametrize(
    "changes",
    [{"ref": "R2"}, {"time": "10:00:01"}, {"debit": "100.01"}, {"debit": "", "credit": "100"}],
    ids=["other ref", "other time", "other amount", "other direction"],
)
def test_a_legitimate_repeat_is_never_collapsed(ws, changes):
    ws.add("batch_2026-08-01.csv", [make_row(), make_row(**changes)])
    ws.run()
    assert len(ws.table()) == 2


def test_same_day_repeats_of_account_and_description_are_all_kept(ws):
    ws.add("batch_2026-08-01.csv", [
        make_row(desc="UPI-ZOMATO-ORDER", ref="R1", time="03:54:00", debit="41046.33"),
        make_row(desc="UPI-ZOMATO-ORDER", ref="R2", time="09:19:00", debit="58057.08"),
        make_row(desc="UPI-ZOMATO-ORDER", ref="R3", time="21:17:00", debit="17026.76"),
    ])
    ws.run()
    assert len(ws.table()) == 3


def test_a_restated_balance_description_or_account_type_is_the_same_transaction(ws):
    ws.add("batch_2026-08-01.csv", [make_row(balance="900.00", desc="POS", typ="SAVINGS")])
    ws.add("batch_2026-08-02.csv", [make_row(balance="750.25", desc="POS (RESTATED)", typ="CURRENT")])
    result = ws.run()

    assert result.outcomes[1].counts["dup_already_loaded"] == 1
    assert len(ws.table()) == 1


def test_the_same_ref_with_different_content_is_loaded_and_flagged(ws):
    ws.add("batch_2026-08-01.csv", [make_row(debit="100"), make_row(debit="250")])
    ws.run()

    assert sorted(ws.table()["amount"]) == ["100.00", "250.00"]
    assert ws.report()["warnings"]["same_ref_no_different_transactions"] == [
        {"account_number": "AC1", "ref_no": "R1", "transactions": 2}
    ]


def test_rows_with_a_blank_ref_are_quarantined_never_merged_with_each_other(ws):
    ws.add("batch_2026-08-01.csv", [make_row(ref="", debit="10"), make_row(ref="", debit="20"), make_row(ref="R1")])
    ws.run()

    assert ws.table()["ref_no"].tolist() == ["R1"]
    assert ws.table("quarantine.csv")["reason_code"].tolist() == ["MISSING_REF_NO", "MISSING_REF_NO"]


def test_ref_no_can_be_made_optional_and_distinct_blank_ref_rows_stay_distinct(ws, cfg):
    optional = dataclasses.replace(cfg, required_fields=("account_number",))
    ws.add("batch_2026-08-01.csv", [make_row(ref="", debit="10"), make_row(ref="", debit="20")])
    ws.add("batch_2026-08-02.csv", [make_row(ref="", debit="10", date="1/08/2026")])
    ws.run(cfg=optional)

    assert sorted(ws.table()["amount"]) == ["10.00", "20.00"]
    assert len(ws.table("quarantine.csv")) == 0 and len(ws.table("duplicates_removed.csv")) == 1


def test_the_dedup_key_comes_from_config(ws, cfg):
    ws.add("batch_2026-08-01.csv", [make_row(time="10:00:00"), make_row(time="11:00:00")])
    ws.run(cfg=dataclasses.replace(cfg, dedup_key=("account_number", "ref_no")))
    assert len(ws.table()) == 1


def test_txn_id_is_stable_and_scoped_to_the_source():
    key = ["AC1", "2026-08-01", "10:00:00", "R1", "DEBIT", "100.00"]
    assert make_txn_id("bank_a", key) == make_txn_id("bank_a", key) != make_txn_id("bank_b", key)
    assert make_txn_id("s", ["ab", "c"]) != make_txn_id("s", ["a", "bc"])


def test_split_batch_separates_new_in_file_repeats_and_already_stored():
    frame = pd.DataFrame({"txn_id": ["a", "b", "a", "c"], "source_line": [2, 3, 4, 5]})
    new, repeats, stored = split_batch(frame, {"c"})
    assert (new["txn_id"].tolist(), repeats["source_line"].tolist(), stored["txn_id"].tolist()) == (["a", "b"], [4], ["c"])
