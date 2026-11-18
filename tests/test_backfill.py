from __future__ import annotations

import dataclasses
import json

import pytest
from helpers import Workspace, make_row

from txn_pipeline.store import Store


def day_one_and_late_file(ws):
    ws.add("batch_2026-08-01.csv", [
        make_row(ref="R1", time="09:00:00", debit="100"),
        make_row(ref="R2", time="22:00:00", debit="", credit="50"),
    ])
    ws.add("batch_2026-08-02.csv", [make_row(ref="R3", date="2/08/2026", time="10:00:00", debit="30")])
    ws.add("batch_2026-08-04_late_correction.csv", [
        make_row(ref="L1", time="21:10:00", debit="18500"),
        make_row(ref="L2", account="AC2", time="23:41:00", debit="", credit="15000"),
    ])


def report(ws):
    return json.loads((ws.output / "reconciliation_summary.json").read_text(encoding="utf-8"))


class TestBackfillMerge:
    def test_late_rows_are_merged_into_their_own_business_date_and_flagged(self, ws):
        day_one_and_late_file(ws)
        ws.run()

        table = ws.table("canonical_transactions.csv")
        assert set(table["txn_date"]) == {"2026-08-01", "2026-08-02"}  # nothing is filed under 2026-08-04
        day_one = table[table["txn_date"] == "2026-08-01"]
        assert sorted(day_one["ref_no"]) == ["L1", "L2", "R1", "R2"]
        assert dict(zip(day_one["ref_no"], day_one["load_type"])) == {
            "R1": "REGULAR", "R2": "REGULAR", "L1": "BACKFILL", "L2": "BACKFILL",
        }

    def test_backfilled_rows_sit_inside_the_day_in_time_order(self, ws):
        day_one_and_late_file(ws)
        ws.run()
        day_one = ws.table("canonical_transactions.csv").query("txn_date == '2026-08-01'")
        assert day_one["txn_time"].tolist() == sorted(day_one["txn_time"].tolist())
        assert day_one["ref_no"].tolist() == ["R1", "L1", "R2", "L2"]

    def test_net_effect_is_measured_before_and_after(self, ws):
        day_one_and_late_file(ws)
        ws.run()

        backfill = report(ws)["backfill"]
        assert len(backfill) == 1
        partition = backfill[0]["partitions"][0]
        assert partition["txn_date"] == "2026-08-01" and partition["days_late"] == 3
        assert partition["before"] == {"rows": 2, "debit_total": "100.00", "credit_total": "50.00", "net": "-50.00"}
        assert partition["delta"] == {"rows": 2, "debit_total": "18500.00", "credit_total": "15000.00", "net": "-3500.00"}
        assert partition["after"] == {"rows": 4, "debit_total": "18600.00", "credit_total": "15050.00", "net": "-3550.00"}
        assert [a["account_number"] for a in backfill[0]["by_account"]] == ["AC1", "AC2"]

    def test_other_business_dates_are_untouched_by_the_backfill(self, ws):
        day_one_and_late_file(ws)
        ws.run()
        by_date = {d["txn_date"]: d for d in report(ws)["canonical"]["by_txn_date"]}
        assert by_date["2026-08-02"]["rows"] == 1 and by_date["2026-08-02"]["backfill_rows"] == 0
        assert by_date["2026-08-01"]["backfill_rows"] == 2

    def test_rows_already_loaded_are_skipped_not_duplicated(self, ws):
        ws.add("batch_2026-08-01.csv", [make_row(ref="R1", debit="100"), make_row(ref="R2", debit="", credit="50")])
        ws.add("batch_2026-08-04_late_correction.csv", [
            make_row(ref="R1", date="1/08/2026", time="10:00:00", debit="100.00"),  # already loaded, other format
            make_row(ref="L1", time="23:00:00", debit="7"),
        ])
        result = ws.run()

        counts = result.outcomes[1].counts
        assert (counts["rows_loaded"], counts["dup_already_loaded"], counts["rows_backfill"]) == (1, 1, 1)
        assert len(ws.table("canonical_transactions.csv")) == 3
        late = report(ws)["backfill"][0]["late_dated_rows"]
        assert late == {"rows": 2, "new": 1, "already_loaded": 1, "within_file_repeats": 0}

    def test_a_correction_file_with_only_known_rows_changes_nothing(self, ws):
        ws.add("batch_2026-08-01.csv", [make_row(ref="R1"), make_row(ref="R2")])
        ws.run()
        before = ws.table("canonical_transactions.csv")
        ws.add("batch_2026-08-04_late_correction.csv", [make_row(ref="R1", date="1/08/2026"), make_row(ref="R2")])
        ws.run()

        assert ws.table("canonical_transactions.csv").equals(before)
        assert report(ws)["backfill"] == []

    def test_repeats_inside_the_late_file_are_collapsed(self, ws):
        ws.add("batch_2026-08-04_late_correction.csv", [make_row(ref="L1"), make_row(ref="L1")])
        ws.run()
        assert len(ws.table("canonical_transactions.csv")) == 1
        assert report(ws)["backfill"][0]["late_dated_rows"]["within_file_repeats"] == 1


class TestClassification:
    def test_same_day_delivery_is_regular(self, ws):
        ws.add("batch_2026-08-01.csv", [make_row(ref="R1")])
        ws.run()
        assert set(ws.table("canonical_transactions.csv")["load_type"]) == {"REGULAR"}
        assert report(ws)["backfill"] == []

    def test_lateness_threshold_is_configurable(self, ws, cfg):
        ws.add("batch_2026-08-04_late_correction.csv", [make_row(ref="L1")])
        ws.run(cfg=dataclasses.replace(cfg, late_after_days=5))
        assert set(ws.table("canonical_transactions.csv")["load_type"]) == {"REGULAR"}

        ws.run(cfg=cfg, reset=True)
        assert set(ws.table("canonical_transactions.csv")["load_type"]) == {"BACKFILL"}

    def test_file_without_a_batch_date_is_never_called_a_backfill(self, ws):
        ws.add("batch_misc.csv", [make_row(ref="R1")])
        ws.run()
        assert set(ws.table("canonical_transactions.csv")["load_type"]) == {"REGULAR"}

    def test_arrival_order_does_not_change_which_rows_are_backfill(self, ws, tmp_path_factory, cfg):
        day_one_and_late_file(ws)
        ws.run()

        other = Workspace(tmp_path_factory.mktemp("reordered"), cfg)
        other.add("batch_2026-08-04_late_correction.csv", [
            make_row(ref="L1", time="21:10:00", debit="18500"),
            make_row(ref="L2", account="AC2", time="23:41:00", debit="", credit="15000"),
        ])
        other.run()
        other.add("batch_2026-08-01.csv", [make_row(ref="R1", time="09:00:00", debit="100"), make_row(ref="R2", time="22:00:00", debit="", credit="50")])
        other.add("batch_2026-08-02.csv", [make_row(ref="R3", date="2/08/2026", time="10:00:00", debit="30")])
        other.run()

        columns = ["txn_id", "txn_date", "txn_time", "direction", "amount", "load_type"]
        assert ws.table("canonical_transactions.csv")[columns].equals(other.table("canonical_transactions.csv")[columns])


class TestBackfillIdempotency:
    def test_rerunning_and_refeeding_the_late_file_changes_nothing(self, ws):
        day_one_and_late_file(ws)
        ws.run()
        before = ws.output_hashes()

        ws.run()
        (ws.input / "batch_2026-08-04_late_correction (resent).csv").write_bytes(
            (ws.input / "batch_2026-08-04_late_correction.csv").read_bytes()
        )
        ws.run()

        assert ws.output_hashes() == before


class TestBackfillIntegrity:
    def test_a_partition_that_does_not_move_by_exactly_the_added_rows_aborts_the_load(self, ws, monkeypatch):
        day_one_and_late_file(ws)
        real = Store.partition_totals
        calls = {"n": 0}

        def lying(self, source, dates):
            calls["n"] += 1
            totals = real(self, source, dates)
            if calls["n"] % 2 == 0:  # the "after" reading
                for bucket in totals.values():
                    bucket["rows"] += 1
            return totals

        monkeypatch.setattr(Store, "partition_totals", lying)
        with pytest.raises(RuntimeError, match="changed the partition"):
            ws.run()
        with Store(ws.state / "pipeline.db") as store:
            loaded_files = [r[0] for r in store.conn.execute("SELECT batch_file FROM batches ORDER BY seq")]
        assert "batch_2026-08-04_late_correction.csv" not in loaded_files
