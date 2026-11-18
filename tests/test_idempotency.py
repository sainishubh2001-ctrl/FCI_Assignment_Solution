from __future__ import annotations

import pytest
from helpers import Workspace, make_row

from txn_pipeline import pipeline
from txn_pipeline.pipeline import FAILED, LOADED, SKIPPED
from txn_pipeline.store import Store


def day_one(ws):
    ws.add("batch_2026-08-01.csv", [
        make_row(ref="R1"),
        make_row(ref="R2", debit="", credit="50"),
        make_row(ref="BAD", debit="1", credit="1"),
    ])


def day_two(ws):
    ws.add("batch_2026-08-02.csv", [make_row(ref="R1", date="1/08/2026"), make_row(ref="R3", date="2/08/2026")])


def late_file(ws):
    ws.add("batch_2026-08-04_late_correction.csv", [make_row(ref="R9", time="23:50:00", debit="", credit="75")])


def statuses(result):
    return [o.status for o in result.outcomes]


def test_running_twice_changes_nothing(ws):
    day_one(ws), day_two(ws), late_file(ws)
    assert statuses(ws.run()) == [LOADED] * 3
    before = ws.hashes()

    assert statuses(ws.run()) == [SKIPPED] * 3
    assert ws.hashes() == before


def test_refeeding_a_loaded_file_under_another_name_is_a_noop(ws):
    day_one(ws)
    ws.run()
    before = ws.hashes()
    (ws.input / "batch_2026-08-01 - copy.csv").write_bytes((ws.input / "batch_2026-08-01.csv").read_bytes())

    result = ws.run()

    assert [o.status for o in result.outcomes] == [SKIPPED, SKIPPED]
    assert ws.hashes() == before


def test_the_same_rows_with_different_bytes_add_nothing(ws):
    day_one(ws)
    ws.run()
    before = ws.table()
    ws.add("batch_2026-08-05_resend.csv", [make_row(ref="R1"), make_row(ref="R2", debit="", credit="50")], eol="\n")

    ws.run()

    assert ws.table().equals(before)


def test_loading_a_file_at_a_time_gives_the_same_result_as_rebuilding_from_scratch(ws):
    day_one(ws)
    ws.run()
    day_two(ws)
    ws.run()
    late_file(ws)
    ws.run()
    incremental = ws.hashes()

    ws.run(reset=True)

    assert ws.hashes() == incremental


def test_canonical_content_does_not_depend_on_arrival_order(ws, tmp_path_factory, cfg):
    day_one(ws), day_two(ws), late_file(ws)
    ws.run()

    reordered = Workspace(tmp_path_factory.mktemp("reordered"), cfg)
    late_file(reordered)
    reordered.run()
    day_two(reordered), day_one(reordered)
    reordered.run()

    columns = ["txn_id", "txn_date", "txn_time", "direction", "amount", "load_type"]
    assert ws.table()[columns].equals(reordered.table()[columns])


def test_a_crash_midway_leaves_nothing_behind_and_a_retry_succeeds(ws, monkeypatch):
    day_one(ws), day_two(ws)
    real = Store.insert_batch

    def crash(self, **fields):
        raise RuntimeError("crash")

    monkeypatch.setattr(Store, "insert_batch", crash)
    with pytest.raises(RuntimeError, match="crash"):
        ws.run()

    with Store(ws.state / "pipeline.db") as store:
        rows = [store.conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0] for t in ("batches", "transactions", "quarantine", "duplicates")]
    assert rows == [0, 0, 0, 0]

    monkeypatch.setattr(Store, "insert_batch", real)
    assert statuses(ws.run()) == [LOADED, LOADED]


def test_a_load_that_does_not_account_for_every_row_is_rolled_back(ws, monkeypatch):
    day_one(ws)
    monkeypatch.setattr(pipeline, "split_batch", lambda valid, stored: (valid.iloc[:0],) * 3)

    with pytest.raises(RuntimeError, match="do not add up"):
        ws.run()

    with Store(ws.state / "pipeline.db") as store:
        assert store.conn.execute("SELECT COUNT(*) FROM batches").fetchone()[0] == 0


def test_an_unreadable_file_is_reported_and_picked_up_once_fixed(ws):
    day_one(ws)
    (ws.input / "batch_2026-08-02.csv").write_text("account_number,txn_date\r\nAC1,02-08-2026\r\n")

    first = ws.run()
    assert statuses(first) == [LOADED, FAILED] and "missing column" in first.outcomes[1].message
    assert len(ws.table()) == 2

    day_two(ws)
    assert statuses(ws.run()) == [SKIPPED, LOADED]
    assert len(ws.table()) == 3
