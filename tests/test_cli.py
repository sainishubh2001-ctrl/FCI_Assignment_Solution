from __future__ import annotations

from conftest import BANK_A_CONFIG
from helpers import make_row

from txn_pipeline.cli import main


def cli_args(ws, *extra):
    return ["--config", str(BANK_A_CONFIG), "--input", str(ws.input), "--output", str(ws.output), "--state", str(ws.state), *extra]


def test_a_normal_run_exits_zero_and_prints_each_file_and_the_totals(ws, capsys):
    ws.add("batch_2026-08-01.csv", [make_row()])
    assert main(cli_args(ws)) == 0
    out = capsys.readouterr().out
    assert "batch_2026-08-01.csv: read=1 loaded=1" in out and "1 read = 1 loaded + 0 quarantined + 0 duplicates removed" in out


def test_a_second_run_reports_skips(ws, capsys):
    ws.add("batch_2026-08-01.csv", [make_row()])
    main(cli_args(ws))
    capsys.readouterr()
    assert main(cli_args(ws)) == 0
    assert "SKIPPED" in capsys.readouterr().out


def test_a_bad_config_or_missing_input_folder_exits_two(ws, capsys):
    assert main(["--config", str(ws.root / "nope.yaml"), "--input", str(ws.input)]) == 2
    assert "configuration error" in capsys.readouterr().err
    assert main(["--config", str(BANK_A_CONFIG), "--input", str(ws.root / "nowhere")]) == 2
    assert "input folder not found" in capsys.readouterr().err


def test_a_file_that_cannot_be_read_exits_one_but_the_rest_is_still_published(ws, capsys):
    ws.add("batch_2026-08-01.csv", [make_row()])
    (ws.input / "batch_2026-08-02.csv").write_text("not,the,right,header\r\n1,2,3,4\r\n")
    assert main(cli_args(ws)) == 1
    assert "FAILED batch_2026-08-02.csv" in capsys.readouterr().err
    assert len(ws.table()) == 1


def test_reset_rebuilds_the_same_output(ws):
    ws.add("batch_2026-08-01.csv", [make_row()])
    main(cli_args(ws))
    before = ws.hashes()
    assert main(cli_args(ws, "--reset")) == 0
    assert ws.hashes() == before
