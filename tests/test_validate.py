from __future__ import annotations

import pytest
from helpers import HEADER, make_row, raw_rec

from txn_pipeline.extract import FileFormatError, discover_files, read_raw_file
from txn_pipeline.validate import (
    BOTH_DEBIT_AND_CREDIT,
    INVALID_AMOUNT,
    INVALID_BALANCE,
    INVALID_DATE,
    INVALID_TIME,
    MALFORMED_ROW,
    NO_DEBIT_OR_CREDIT,
    check_row,
    fingerprint,
    malformed_rejects,
    validate_rows,
)


def codes(issues):
    return [code for code, _ in issues]


class TestCheckRow:
    def test_clean_debit_row_is_normalised(self, cfg):
        out, issues = check_row(raw_rec(txn_date="1/08/2026", txn_time="9:05:00", debit="21400.6"), cfg)
        assert issues == []
        assert (out["txn_date"], out["txn_time"]) == ("2026-08-01", "09:05:00")
        assert (out["direction"], str(out["amount"])) == ("DEBIT", "21400.60")

    def test_clean_credit_row(self, cfg):
        out, issues = check_row(raw_rec(debit="", credit="5000"), cfg)
        assert issues == []
        assert (out["direction"], str(out["amount"])) == ("CREDIT", "5000.00")

    def test_both_debit_and_credit_is_quarantined(self, cfg):
        _, issues = check_row(raw_rec(debit="1200", credit="1200"), cfg)
        assert codes(issues) == [BOTH_DEBIT_AND_CREDIT]

    def test_neither_debit_nor_credit_is_quarantined(self, cfg):
        _, issues = check_row(raw_rec(debit="", credit=""), cfg)
        assert codes(issues) == [NO_DEBIT_OR_CREDIT]

    def test_zero_on_the_unused_side_is_not_both_populated(self, cfg):
        out, issues = check_row(raw_rec(debit="0.00", credit="500"), cfg)
        assert issues == []
        assert out["direction"] == "CREDIT"

    def test_zero_on_both_sides_is_neither(self, cfg):
        _, issues = check_row(raw_rec(debit="0", credit="0.00"), cfg)
        assert codes(issues) == [NO_DEBIT_OR_CREDIT]

    @pytest.mark.parametrize("bad", ["31-02-2026", "15-13-2026", "N/A", "", "2026-08-01"])
    def test_bad_dates_are_quarantined(self, cfg, bad):
        _, issues = check_row(raw_rec(txn_date=bad), cfg)
        assert codes(issues) == [INVALID_DATE]

    @pytest.mark.parametrize("bad", ["25:00:00", "noon", ""])
    def test_bad_times_are_quarantined(self, cfg, bad):
        _, issues = check_row(raw_rec(txn_time=bad), cfg)
        assert codes(issues) == [INVALID_TIME]

    @pytest.mark.parametrize("field,bad", [("debit", "abc"), ("credit", "1e5"), ("debit", "-5"), ("debit", "10.005")])
    def test_bad_amounts_are_quarantined(self, cfg, field, bad):
        overrides = {"debit": "", "credit": "", field: bad}
        _, issues = check_row(raw_rec(**overrides), cfg)
        assert codes(issues) == [INVALID_AMOUNT]

    def test_bad_balance_is_quarantined(self, cfg):
        _, issues = check_row(raw_rec(balance="lots"), cfg)
        assert codes(issues) == [INVALID_BALANCE]

    def test_blank_balance_is_allowed(self, cfg):
        out, issues = check_row(raw_rec(balance=""), cfg)
        assert issues == [] and out["balance"] is None

    def test_missing_ref_no_is_quarantined(self, cfg):
        _, issues = check_row(raw_rec(ref_no="  "), cfg)
        assert codes(issues) == ["MISSING_REF_NO"]

    def test_missing_account_is_quarantined(self, cfg):
        _, issues = check_row(raw_rec(account_number=""), cfg)
        assert codes(issues) == ["MISSING_ACCOUNT_NUMBER"]

    def test_all_failing_rules_are_reported_in_priority_order(self, cfg):
        _, issues = check_row(raw_rec(ref_no="", txn_date="31-02-2026", debit="1", credit="2"), cfg)
        assert codes(issues) == ["MISSING_REF_NO", INVALID_DATE, BOTH_DEBIT_AND_CREDIT]


class TestFingerprint:
    def test_same_bad_record_in_different_formats_has_one_fingerprint(self, cfg):
        a = raw_rec(txn_date="01-08-2026", txn_time="09:00:00", debit="", credit="", balance="22000")
        b = raw_rec(txn_date="1/08/2026", txn_time="9:00:00", debit="", credit="", balance="22000.00")
        fa, fb = (fingerprint(r, check_row(r, cfg)[0]) for r in (a, b))
        assert fa == fb

    def test_different_bad_records_differ(self, cfg):
        a, b = raw_rec(debit="", credit=""), raw_rec(debit="", credit="", ref_no="REF2")
        assert fingerprint(a, check_row(a, cfg)[0]) != fingerprint(b, check_row(b, cfg)[0])


class TestValidateRows:
    def test_rows_are_split_into_valid_and_rejected_with_lineage(self, cfg, tmp_path):
        rows = [
            make_row(ref="R1"),
            make_row(ref="R2", debit="5", credit="5"),
            make_row(ref="R3", date="99-99-2026"),
            make_row(ref="R4", debit="", credit="7.5"),
        ]
        data = (HEADER + "\r\n" + "\r\n".join(rows) + "\r\n").encode()
        raw = read_raw_file("batch_2026-08-01.csv", data, cfg)
        valid, rejected = validate_rows(raw.complete, cfg)
        assert valid["ref_no"].tolist() == ["R1", "R4"]
        assert valid["source_line"].tolist() == [2, 5]
        assert rejected["reason_code"].tolist() == [BOTH_DEBIT_AND_CREDIT, INVALID_DATE]
        assert rejected["source_line"].tolist() == [3, 4]
        assert rejected["raw_line"].iloc[0] == rows[1]

    def test_empty_input_yields_empty_frames(self, cfg):
        raw = read_raw_file("batch_2026-08-01.csv", (HEADER + "\r\n").encode(), cfg)
        valid, rejected = validate_rows(raw.complete, cfg)
        assert valid.empty and rejected.empty


class TestExtract:
    def test_ragged_rows_are_quarantined_not_crashed_or_skipped(self, cfg):
        good = make_row(ref="R1")
        too_many = good + ",EXTRA"
        too_few = "AC1,01-08-2026,10:00:00"
        data = (HEADER + "\r\n" + "\r\n".join([good, too_many, too_few, make_row(ref="R2")]) + "\r\n").encode()
        raw = read_raw_file("batch_2026-08-01.csv", data, cfg)
        assert raw.rows_read == 4
        assert len(raw.complete) == 2
        rejects = malformed_rejects(raw.malformed, raw.header_width)
        assert rejects["reason_code"].tolist() == [MALFORMED_ROW, MALFORMED_ROW]
        assert rejects["source_line"].tolist() == [3, 4]
        assert "expected 9 fields, found 10" in rejects["reason_detail"].iloc[0]
        assert rejects["raw_line"].iloc[1] == too_few

    def test_bom_lf_endings_and_header_case_are_tolerated(self, cfg):
        header = HEADER.upper()
        data = ("\ufeff" + header + "\n" + make_row() + "\n").encode("utf-8")
        raw = read_raw_file("batch_2026-08-01.csv", data, cfg)
        assert raw.rows_read == 1 and len(raw.complete) == 1

    def test_blank_lines_are_counted_not_treated_as_rows(self, cfg):
        data = (HEADER + "\r\n" + make_row() + "\r\n\r\n\r\n").encode()
        raw = read_raw_file("batch_2026-08-01.csv", data, cfg)
        assert raw.rows_read == 1 and raw.blank_lines == 2

    def test_all_empty_fields_row_is_a_row_not_a_blank_line(self, cfg):
        data = (HEADER + "\r\n" + ",,,,,,,,\r\n").encode()
        raw = read_raw_file("batch_2026-08-01.csv", data, cfg)
        valid, rejected = validate_rows(raw.complete, cfg)
        assert raw.rows_read == 1 and valid.empty and len(rejected) == 1

    def test_missing_expected_column_fails_the_file_loudly(self, cfg):
        data = b"account_number,txn_date\r\nAC1,01-08-2026\r\n"
        with pytest.raises(FileFormatError, match="missing expected column"):
            read_raw_file("batch_2026-08-01.csv", data, cfg)

    def test_wrong_encoding_fails_the_file_loudly(self, cfg):
        data = (HEADER + "\r\n").encode() + make_row(desc="CAF\xc9").encode("latin-1") + b"\r\n"
        with pytest.raises(FileFormatError, match="not valid"):
            read_raw_file("batch_2026-08-01.csv", data, cfg)

    def test_empty_file_fails_the_file_loudly(self, cfg):
        with pytest.raises(FileFormatError, match="no header"):
            read_raw_file("batch_2026-08-01.csv", b"", cfg)

    def test_quoted_field_with_embedded_comma_is_one_field(self, cfg):
        line = 'AC1,01-08-2026,10:00:00,"SHOP, CITY",REF1,100.00,,900.00,SAVINGS'
        raw = read_raw_file("batch_2026-08-01.csv", (HEADER + "\r\n" + line + "\r\n").encode(), cfg)
        assert raw.complete["description"].iloc[0] == "SHOP, CITY"


class TestDiscovery:
    def test_files_are_ordered_by_batch_date_then_name(self, cfg, tmp_path):
        for name in ["batch_2026-08-04_late_correction.csv", "batch_2026-08-01.csv", "batch_2026-08-02.csv", "other.csv"]:
            (tmp_path / name).write_text(HEADER)
        names = [p.name for p in discover_files(tmp_path, cfg)]
        assert names == ["batch_2026-08-01.csv", "batch_2026-08-02.csv", "batch_2026-08-04_late_correction.csv"]
