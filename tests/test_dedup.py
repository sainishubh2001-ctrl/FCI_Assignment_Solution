from __future__ import annotations

import dataclasses

import pandas as pd
from helpers import make_row

from txn_pipeline.dedup import make_txn_id, split_batch


class TestRedeliveryCollapse:
    def test_redelivery_in_a_different_format_collapses_to_nothing_new(self, ws):
        ws.add("batch_2026-08-01.csv", [
            make_row(ref="R1", time="09:05:00", debit="21400.6"),
            make_row(ref="R2", time="14:30:00", debit="", credit="5000"),
        ])
        # same two transactions, re-sent with the day-2 feed's formatting quirks
        ws.add("batch_2026-08-02.csv", [
            make_row(ref="R1", date="1/08/2026", time="9:05:00", debit="21400.60"),
            make_row(ref="R2", date="1/08/2026", time="14:30:00", debit="", credit="5000.00"),
        ])
        result = ws.run()

        second = result.outcomes[1].counts
        assert (second["rows_loaded"], second["dup_already_loaded"]) == (0, 2)
        assert len(ws.table("canonical_transactions.csv")) == 2

    def test_a_raw_text_dedup_would_have_missed_it(self, ws):
        row_a = make_row(ref="R1", time="09:05:00")
        row_b = make_row(ref="R1", date="1/08/2026", time="9:05:00")
        assert row_a != row_b  # the bytes differ, only normalisation reveals they are the same transaction

    def test_in_file_duplicates_collapse_and_are_audited(self, ws):
        ws.add("batch_2026-08-01.csv", [make_row(ref="R1"), make_row(ref="R2"), make_row(ref="R1")])
        ws.run()

        assert len(ws.table("canonical_transactions.csv")) == 2
        audit = ws.table("duplicates_removed.csv")
        assert audit[["source_line", "duplicate_kind", "kept_source_line"]].values.tolist() == [["4", "WITHIN_FILE", "2"]]

    def test_redelivered_duplicates_point_back_to_the_row_that_was_kept(self, ws):
        ws.add("batch_2026-08-01.csv", [make_row(ref="R1")])
        ws.add("batch_2026-08-02.csv", [make_row(ref="R1", date="1/08/2026")])
        ws.run()

        audit = ws.table("duplicates_removed.csv")
        assert audit[["batch_file", "duplicate_kind", "kept_batch_file"]].values.tolist() == [
            ["batch_2026-08-02.csv", "ALREADY_LOADED", "batch_2026-08-01.csv"]
        ]


class TestLegitimateRepeatsSurvive:
    def test_same_everything_but_a_different_ref_is_a_different_transaction(self, ws):
        ws.add("batch_2026-08-01.csv", [make_row(ref="R1"), make_row(ref="R2")])
        ws.run()
        assert len(ws.table("canonical_transactions.csv")) == 2

    def test_same_ref_but_a_different_time_is_a_different_transaction(self, ws):
        ws.add("batch_2026-08-01.csv", [make_row(ref="R1", time="10:00:00"), make_row(ref="R1", time="10:00:01")])
        ws.run()
        assert len(ws.table("canonical_transactions.csv")) == 2

    def test_same_ref_and_time_but_a_different_amount_is_a_different_transaction(self, ws):
        ws.add("batch_2026-08-01.csv", [make_row(ref="R1", debit="100"), make_row(ref="R1", debit="100.01")])
        ws.run()
        assert len(ws.table("canonical_transactions.csv")) == 2

    def test_same_amount_in_the_opposite_direction_is_a_different_transaction(self, ws):
        ws.add("batch_2026-08-01.csv", [make_row(ref="R1", debit="100", credit=""), make_row(ref="R1", debit="", credit="100")])
        ws.run()
        assert sorted(ws.table("canonical_transactions.csv")["direction"]) == ["CREDIT", "DEBIT"]

    def test_coarse_account_date_description_repeats_are_all_kept(self, ws):
        rows = [
            make_row(desc="UPI-ZOMATO-ORDER", ref="R1", time="03:54:00", debit="41046.33"),
            make_row(desc="UPI-ZOMATO-ORDER", ref="R2", time="09:19:00", debit="58057.08"),
            make_row(desc="UPI-ZOMATO-ORDER", ref="R3", time="21:17:00", debit="17026.76"),
        ]
        ws.add("batch_2026-08-01.csv", rows)
        ws.run()
        assert len(ws.table("canonical_transactions.csv")) == 3

    def test_restated_balance_description_or_account_type_is_still_the_same_transaction(self, ws):
        ws.add("batch_2026-08-01.csv", [make_row(ref="R1", balance="900.00", desc="POS", typ="SAVINGS")])
        ws.add("batch_2026-08-02.csv", [make_row(ref="R1", balance="750.25", desc="POS PURCHASE (RESTATED)", typ="CURRENT")])
        result = ws.run()

        assert result.outcomes[1].counts["dup_already_loaded"] == 1
        assert len(ws.table("canonical_transactions.csv")) == 1

    def test_same_ref_with_different_content_is_loaded_not_silently_merged(self, ws):
        ws.add("batch_2026-08-01.csv", [make_row(ref="R1", debit="100"), make_row(ref="R1", debit="250")])
        ws.run()
        assert sorted(ws.table("canonical_transactions.csv")["amount"]) == ["100.00", "250.00"]


class TestRowsWithoutAReference:
    def test_blank_ref_rows_are_each_quarantined_never_merged_with_one_another(self, ws):
        ws.add("batch_2026-08-01.csv", [make_row(ref="", debit="10"), make_row(ref="", debit="20"), make_row(ref="R1")])
        ws.run()

        assert ws.table("canonical_transactions.csv")["ref_no"].tolist() == ["R1"]
        quarantine = ws.table("quarantine.csv")
        assert quarantine["reason_code"].tolist() == ["MISSING_REF_NO", "MISSING_REF_NO"]
        assert len(ws.table("duplicates_removed.csv")) == 0

    def test_ref_no_can_be_made_optional_by_config_without_merging_distinct_rows(self, ws, cfg):
        optional = dataclasses.replace(cfg, required_fields=("account_number",))
        ws.add("batch_2026-08-01.csv", [make_row(ref="", debit="10"), make_row(ref="", debit="20")])
        ws.add("batch_2026-08-02.csv", [make_row(ref="", debit="10", date="1/08/2026")])
        ws.run(cfg=optional)

        assert sorted(ws.table("canonical_transactions.csv")["amount"]) == ["10.00", "20.00"]
        assert len(ws.table("quarantine.csv")) == 0
        assert len(ws.table("duplicates_removed.csv")) == 1


class TestKeyIsConfigurable:
    def test_a_different_key_changes_what_counts_as_a_duplicate(self, ws, cfg):
        ws.add("batch_2026-08-01.csv", [make_row(ref="R1", time="10:00:00"), make_row(ref="R1", time="11:00:00")])
        loose = dataclasses.replace(cfg, dedup_key=("account_number", "ref_no"))
        ws.run(cfg=loose)
        assert len(ws.table("canonical_transactions.csv")) == 1


class TestKeyHelpers:
    def test_txn_id_is_deterministic_and_source_scoped(self):
        parts = ["AC1", "2026-08-01", "10:00:00", "R1", "DEBIT", "100.00"]
        assert make_txn_id("bank_a", parts) == make_txn_id("bank_a", parts)
        assert make_txn_id("bank_a", parts) != make_txn_id("bank_b", parts)

    def test_txn_id_does_not_confuse_field_boundaries(self):
        assert make_txn_id("s", ["ab", "c"]) != make_txn_id("s", ["a", "bc"])

    def test_split_batch_separates_new_in_file_repeats_and_already_loaded(self):
        frame = pd.DataFrame({"txn_id": ["a", "b", "a", "c"], "source_line": [2, 3, 4, 5]})
        new, within, loaded = split_batch(frame, already_loaded_ids={"c"})
        assert new["txn_id"].tolist() == ["a", "b"]
        assert within["source_line"].tolist() == [4]
        assert loaded["txn_id"].tolist() == ["c"]
