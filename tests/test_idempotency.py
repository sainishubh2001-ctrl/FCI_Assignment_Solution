from __future__ import annotations

import sqlite3

import pytest
from conftest import SAMPLE_DIR
from helpers import make_row

from txn_pipeline.pipeline import FAILED, LOADED, SKIPPED
from txn_pipeline.store import Store


def seed_three_days(ws):
    ws.add("batch_2026-08-01.csv", [make_row(ref="R1"), make_row(ref="R2", debit="", credit="50"), make_row(ref="BAD", debit="1", credit="1")])
    ws.add("batch_2026-08-02.csv", [make_row(ref="R1", date="1/08/2026"), make_row(ref="R3", date="2/08/2026")])
    ws.add("batch_2026-08-04_late_correction.csv", [make_row(ref="R9", time="23:50:00", debit="", credit="75")])


def statuses(result):
    return [o.status for o in result.outcomes]


class TestRerun:
    def test_running_twice_changes_nothing(self, ws):
        seed_three_days(ws)
        first = ws.run()
        before = ws.output_hashes()
        second = ws.run()

        assert statuses(first) == [LOADED] * 3
        assert statuses(second) == [SKIPPED] * 3
        assert ws.output_hashes() == before

    def test_second_run_rewrites_no_output_file(self, ws):
        seed_three_days(ws)
        ws.run()
        assert not any(ws.run().outputs.values())

    def test_refeeding_the_same_file_under_a_new_name_is_a_noop(self, ws):
        seed_three_days(ws)
        ws.run()
        before = ws.output_hashes()
        (ws.input / "batch_2026-08-01 - copy.csv").write_bytes((ws.input / "batch_2026-08-01.csv").read_bytes())

        result = ws.run()

        copy = next(o for o in result.outcomes if "copy" in o.name)
        assert copy.status == SKIPPED and "batch_2026-08-01.csv" in copy.message
        assert ws.output_hashes() == before

    def test_refeeding_the_same_rows_with_different_bytes_adds_no_canonical_rows(self, ws):
        seed_three_days(ws)
        ws.run()
        canonical_before = ws.table("canonical_transactions.csv")
        ws.add("batch_2026-08-05_resend.csv", [make_row(ref="R1"), make_row(ref="R2", debit="", credit="50")], eol="\n")

        ws.run()

        assert ws.table("canonical_transactions.csv").equals(canonical_before)

    def test_incremental_arrival_equals_one_shot_rebuild(self, ws):
        ws.add("batch_2026-08-01.csv", [make_row(ref="R1"), make_row(ref="BAD", debit="1", credit="1")])
        ws.run()
        ws.add("batch_2026-08-02.csv", [make_row(ref="R1", date="1/08/2026"), make_row(ref="R3", date="2/08/2026")])
        ws.run()
        ws.add("batch_2026-08-04_late_correction.csv", [make_row(ref="R9", time="23:50:00", debit="", credit="75")])
        ws.run()
        incremental = ws.output_hashes()

        ws.run(reset=True)

        assert ws.output_hashes() == incremental

    def test_sample_files_rerun_is_byte_identical(self, ws):
        for path in SAMPLE_DIR.glob("batch_*.csv"):
            (ws.input / path.name).write_bytes(path.read_bytes())
        ws.run()
        before = ws.output_hashes()
        ws.run()
        ws.run(reset=True)
        assert ws.output_hashes() == before


class TestOrderIndependence:
    def test_canonical_content_does_not_depend_on_arrival_order(self, ws, tmp_path_factory, cfg):
        from helpers import Workspace

        seed_three_days(ws)
        ws.run()

        other = Workspace(tmp_path_factory.mktemp("other"), cfg)
        # the late file and day 2 land before day 1
        other.add("batch_2026-08-04_late_correction.csv", [make_row(ref="R9", time="23:50:00", debit="", credit="75")])
        other.run()
        other.add("batch_2026-08-02.csv", [make_row(ref="R1", date="1/08/2026"), make_row(ref="R3", date="2/08/2026")])
        other.run()
        other.add("batch_2026-08-01.csv", [make_row(ref="R1"), make_row(ref="R2", debit="", credit="50"), make_row(ref="BAD", debit="1", credit="1")])
        other.run()

        key = ["txn_id", "direction", "amount", "txn_date", "txn_time"]
        assert ws.table("canonical_transactions.csv")[key].equals(other.table("canonical_transactions.csv")[key])


class TestAtomicity:
    def test_crash_mid_load_leaves_no_partial_state_and_a_retry_succeeds(self, ws, monkeypatch):
        seed_three_days(ws)
        original = Store.insert_batch

        def explode(self, record):
            raise RuntimeError("simulated crash after rows were written")

        monkeypatch.setattr(Store, "insert_batch", explode)
        with pytest.raises(RuntimeError, match="simulated crash"):
            ws.run()

        with Store(ws.state / "pipeline.db") as store:
            counts = [store.conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0] for t in ("batches", "transactions", "quarantine", "duplicates")]
        assert counts == [0, 0, 0, 0]

        monkeypatch.setattr(Store, "insert_batch", original)
        assert statuses(ws.run()) == [LOADED] * 3

        reference = ws.output_hashes()
        ws.run(reset=True)
        assert ws.output_hashes() == reference

    def test_the_store_itself_refuses_a_duplicate_key(self, ws, tmp_path):
        seed_three_days(ws)
        ws.run()
        with Store(ws.state / "pipeline.db") as store:
            frame = store.canonical("bank_a").head(1).assign(batch_sha256=lambda d: store.conn.execute("SELECT batch_sha256 FROM batches LIMIT 1").fetchone()[0])
            with pytest.raises(sqlite3.IntegrityError):
                store.insert_transactions(frame)

    def test_a_row_accounting_mismatch_aborts_the_load(self, ws, monkeypatch):
        seed_three_days(ws)
        import txn_pipeline.pipeline as pipeline

        monkeypatch.setattr(pipeline, "split_batch", lambda valid, existing: (valid.iloc[0:0], valid.iloc[0:0], valid.iloc[0:0]))
        with pytest.raises(RuntimeError, match="accounted for"):
            ws.run()
        with Store(ws.state / "pipeline.db") as store:
            assert store.conn.execute("SELECT COUNT(*) FROM batches").fetchone()[0] == 0


class TestFileFailures:
    def test_a_bad_file_is_reported_without_blocking_good_ones_and_is_retried_after_a_fix(self, ws):
        ws.add("batch_2026-08-01.csv", [make_row(ref="R1")])
        (ws.input / "batch_2026-08-02.csv").write_text("account_number,txn_date\r\nAC1,02-08-2026\r\n")

        first = ws.run()
        assert statuses(first) == [LOADED, FAILED]
        assert "missing expected column" in first.outcomes[1].message
        assert len(ws.table("canonical_transactions.csv")) == 1

        ws.add("batch_2026-08-02.csv", [make_row(ref="R2", date="2/08/2026")])
        second = ws.run()
        assert statuses(second) == [SKIPPED, LOADED]
        assert len(ws.table("canonical_transactions.csv")) == 2
