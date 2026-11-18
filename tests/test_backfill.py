from __future__ import annotations

import dataclasses

from helpers import make_row


def three_days(ws):
    ws.add("batch_2026-08-01.csv", [
        make_row(ref="R1", time="09:00:00", debit="100"),
        make_row(ref="R2", time="22:00:00", debit="", credit="50"),
    ])
    ws.add("batch_2026-08-02.csv", [make_row(ref="R3", date="2/08/2026", debit="30")])
    ws.add("batch_2026-08-04_late_correction.csv", [
        make_row(ref="L1", time="21:10:00", debit="18500"),
        make_row(ref="L2", account="AC2", time="23:41:00", debit="", credit="15000"),
    ])


def test_late_rows_go_into_their_own_business_date_and_are_flagged(ws):
    three_days(ws)
    ws.run()

    table = ws.table()
    assert set(table["txn_date"]) == {"2026-08-01", "2026-08-02"}  # nothing filed under the 4th
    day_one = table[table["txn_date"] == "2026-08-01"]
    assert day_one["ref_no"].tolist() == ["R1", "L1", "R2", "L2"]  # in time order, not appended
    assert dict(zip(day_one["ref_no"], day_one["load_type"])) == {
        "R1": "REGULAR", "R2": "REGULAR", "L1": "BACKFILL", "L2": "BACKFILL",
    }


def test_the_net_effect_is_before_added_after(ws):
    three_days(ws)
    ws.run()

    (backfill,) = ws.report()["backfill"]
    (day,) = backfill["partitions"]
    assert (day["txn_date"], day["days_late"]) == ("2026-08-01", 3)
    assert day["before"] == {"rows": 2, "debit_total": "100.00", "credit_total": "50.00", "net": "-50.00"}
    assert day["added"] == {"rows": 2, "debit_total": "18500.00", "credit_total": "15000.00", "net": "-3500.00"}
    assert day["after"] == {"rows": 4, "debit_total": "18600.00", "credit_total": "15050.00", "net": "-3550.00"}
    assert [a["account_number"] for a in backfill["by_account"]] == ["AC1", "AC2"]


def test_other_business_dates_are_untouched(ws):
    three_days(ws)
    ws.run()
    by_date = {d["txn_date"]: d for d in ws.report()["canonical_by_date"]}
    assert (by_date["2026-08-02"]["rows"], by_date["2026-08-02"]["backfill_rows"]) == (1, 0)


def test_rows_the_correction_repeats_are_skipped_not_duplicated(ws):
    ws.add("batch_2026-08-01.csv", [make_row(ref="R1", debit="100"), make_row(ref="R2", debit="", credit="50")])
    ws.add("batch_2026-08-04_late_correction.csv", [
        make_row(ref="R1", date="1/08/2026", debit="100.00"),
        make_row(ref="L1", time="23:00:00", debit="7"),
    ])
    result = ws.run()

    counts = result.outcomes[1].counts
    assert (counts["rows_loaded"], counts["dup_already_loaded"]) == (1, 1)
    assert len(ws.table()) == 3
    assert ws.report()["backfill"][0]["rows"] == 1


def test_a_correction_made_only_of_known_rows_changes_nothing(ws):
    ws.add("batch_2026-08-01.csv", [make_row(ref="R1"), make_row(ref="R2")])
    ws.run()
    before = ws.table()
    ws.add("batch_2026-08-04_late_correction.csv", [make_row(ref="R1", date="1/08/2026"), make_row(ref="R2")])
    ws.run()

    assert ws.table().equals(before)
    assert ws.report()["backfill"] == []


def test_same_day_delivery_is_not_a_backfill(ws):
    ws.add("batch_2026-08-01.csv", [make_row(ref="R1")])
    ws.run()
    assert set(ws.table()["load_type"]) == {"REGULAR"}


def test_the_lateness_threshold_comes_from_config(ws, cfg):
    ws.add("batch_2026-08-04_late_correction.csv", [make_row(ref="L1")])
    ws.run(cfg=dataclasses.replace(cfg, late_after_days=5))
    assert set(ws.table()["load_type"]) == {"REGULAR"}

    ws.run(reset=True)
    assert set(ws.table()["load_type"]) == {"BACKFILL"}


def test_a_file_name_without_a_date_is_never_treated_as_a_backfill(ws):
    ws.add("batch_misc.csv", [make_row(ref="R1")])
    ws.run()
    assert set(ws.table()["load_type"]) == {"REGULAR"}
