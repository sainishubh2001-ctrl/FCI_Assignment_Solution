from __future__ import annotations

import pytest
from helpers import HEADER, make_row

from txn_pipeline.extract import FileFormatError, read_raw_file
from txn_pipeline.validate import check_row, validate_rows


def fields(**overrides):
    row = {
        "account_number": "AC1", "txn_date": "01-08-2026", "txn_time": "10:00:00", "description": "POS",
        "ref_no": "REF1", "debit": "100.00", "credit": "", "balance": "900.00", "account_type": "SAVINGS",
    }
    return {**row, **overrides}


def codes(issues):
    return [code for code, _ in issues]


def test_a_clean_row_comes_back_normalised(cfg):
    clean, issues = check_row(fields(txn_date="1/08/2026", txn_time="9:05:00", debit="21400.6"), cfg)
    assert issues == []
    assert (clean["txn_date"], clean["txn_time"]) == ("2026-08-01", "09:05:00")
    assert (clean["direction"], str(clean["amount"])) == ("DEBIT", "21400.60")


@pytest.mark.parametrize(
    "overrides, expected",
    [
        ({"debit": "1200", "credit": "1200"}, ["BOTH_DEBIT_AND_CREDIT"]),
        ({"debit": "", "credit": ""}, ["NO_DEBIT_OR_CREDIT"]),
        ({"debit": "0", "credit": "0.00"}, ["NO_DEBIT_OR_CREDIT"]),
        ({"txn_date": "31-02-2026"}, ["INVALID_DATE"]),
        ({"txn_date": ""}, ["INVALID_DATE"]),
        ({"txn_time": "25:00:00"}, ["INVALID_TIME"]),
        ({"debit": "abc"}, ["INVALID_AMOUNT"]),
        ({"debit": "-5"}, ["INVALID_AMOUNT"]),
        ({"debit": "10.005"}, ["INVALID_AMOUNT"]),
        ({"balance": "lots"}, ["INVALID_BALANCE"]),
        ({"ref_no": "  "}, ["MISSING_REF_NO"]),
        ({"account_number": ""}, ["MISSING_ACCOUNT_NUMBER"]),
    ],
)
def test_each_rule_gets_its_own_reason_code(cfg, overrides, expected):
    assert codes(check_row(fields(**overrides), cfg)[1]) == expected


def test_a_zero_on_the_unused_side_is_not_both_populated(cfg):
    clean, issues = check_row(fields(debit="0.00", credit="500"), cfg)
    assert issues == [] and clean["direction"] == "CREDIT"


def test_blank_balance_is_allowed(cfg):
    clean, issues = check_row(fields(balance=""), cfg)
    assert issues == [] and clean["balance"] is None


def test_every_broken_rule_is_reported_most_important_first(cfg):
    _, issues = check_row(fields(ref_no="", txn_date="31-02-2026", debit="1", credit="2"), cfg)
    assert codes(issues) == ["MISSING_REF_NO", "INVALID_DATE", "BOTH_DEBIT_AND_CREDIT"]


def test_rows_are_split_and_keep_their_line_number_and_raw_text(cfg):
    rows = [
        make_row(ref="R1"),
        make_row(ref="R2", debit="5", credit="5"),
        make_row(ref="R3", date="99-99-2026"),
        make_row(ref="R4", debit="", credit="7.5"),
    ]
    raw = read_raw_file("batch_2026-08-01.csv", (HEADER + "\r\n" + "\r\n".join(rows) + "\r\n").encode(), cfg)
    valid, rejected = validate_rows(raw, cfg)

    assert valid[["ref_no", "source_line"]].values.tolist() == [["R1", 2], ["R4", 5]]
    assert rejected[["source_line", "reason_code"]].values.tolist() == [[3, "BOTH_DEBIT_AND_CREDIT"], [4, "INVALID_DATE"]]
    assert rejected["raw_line"].tolist() == rows[1:3]


def test_ragged_lines_are_quarantined_not_dropped_and_do_not_stop_the_file(cfg):
    lines = [make_row(ref="R1"), make_row(ref="R2") + ",EXTRA", "AC1,01-08-2026", make_row(ref="R3")]
    raw = read_raw_file("batch_2026-08-01.csv", (HEADER + "\r\n" + "\r\n".join(lines) + "\r\n").encode(), cfg)
    valid, rejected = validate_rows(raw, cfg)

    assert raw.rows_read == 4 and valid["ref_no"].tolist() == ["R1", "R3"]
    assert rejected[["source_line", "reason_code"]].values.tolist() == [[3, "MALFORMED_ROW"], [4, "MALFORMED_ROW"]]
    assert "expected 9 fields, found 10" in rejected["reason_detail"].iloc[0]


def test_bom_lf_endings_blank_lines_and_header_case_are_tolerated(cfg):
    data = ("\ufeff" + HEADER.upper() + "\n" + make_row() + "\n\n\n").encode("utf-8")
    assert read_raw_file("batch_2026-08-01.csv", data, cfg).rows_read == 1


def test_a_row_of_only_commas_is_a_bad_row_not_a_blank_line(cfg):
    raw = read_raw_file("batch_2026-08-01.csv", (HEADER + "\r\n,,,,,,,,\r\n").encode(), cfg)
    valid, rejected = validate_rows(raw, cfg)
    assert raw.rows_read == 1 and valid.empty and len(rejected) == 1


@pytest.mark.parametrize(
    "data, message",
    [
        (b"account_number,txn_date\r\nAC1,01-08-2026\r\n", "missing column"),
        ((HEADER + "\r\n").encode() + make_row(desc="CAF\xc9").encode("latin-1"), "not valid"),
        (b"", "no header"),
    ],
)
def test_a_file_that_cannot_be_read_as_a_whole_fails_loudly(cfg, data, message):
    with pytest.raises(FileFormatError, match=message):
        read_raw_file("batch_2026-08-01.csv", data, cfg)
