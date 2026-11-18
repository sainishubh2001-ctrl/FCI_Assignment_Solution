from __future__ import annotations

from conftest import BANK_A_CONFIG
from helpers import make_row

from txn_pipeline.cli import main


def args(ws, *extra):
    return ["--config", str(BANK_A_CONFIG), "--input", str(ws.input), "--output", str(ws.output), "--state", str(ws.state), *extra]


def test_successful_run_exits_zero_and_logs_each_file(ws, capsys):
    ws.add("batch_2026-08-01.csv", [make_row(ref="R1")])
    assert main(args(ws)) == 0
    assert "batch_2026-08-01.csv: read=1 loaded=1" in capsys.readouterr().out


def test_rerun_exits_zero_and_reports_skips(ws, capsys):
    ws.add("batch_2026-08-01.csv", [make_row(ref="R1")])
    main(args(ws))
    capsys.readouterr()
    assert main(args(ws)) == 0
    assert "SKIPPED" in capsys.readouterr().out


def test_unreadable_config_exits_two(ws, capsys):
    code = main(["--config", str(ws.root / "nope.yaml"), "--input", str(ws.input), "--output", str(ws.output), "--state", str(ws.state)])
    assert code == 2
    assert "configuration error" in capsys.readouterr().err


def test_missing_input_folder_exits_two(ws, capsys):
    code = main(["--config", str(BANK_A_CONFIG), "--input", str(ws.root / "nowhere"), "--output", str(ws.output), "--state", str(ws.state)])
    assert code == 2
    assert "input folder not found" in capsys.readouterr().err


def test_a_failed_file_exits_one_but_still_publishes_the_rest(ws, capsys):
    ws.add("batch_2026-08-01.csv", [make_row(ref="R1")])
    (ws.input / "batch_2026-08-02.csv").write_text("not,the,right,header\r\n1,2,3,4\r\n")
    assert main(args(ws)) == 1
    assert "FAILED batch_2026-08-02.csv" in capsys.readouterr().err
    assert len(ws.table("canonical_transactions.csv")) == 1


def test_reset_flag_rebuilds_from_the_input_folder(ws):
    ws.add("batch_2026-08-01.csv", [make_row(ref="R1")])
    main(args(ws))
    before = ws.output_hashes()
    assert main(args(ws, "--reset")) == 0
    assert ws.output_hashes() == before
