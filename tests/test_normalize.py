from __future__ import annotations

from datetime import date, time
from decimal import Decimal

import pytest

from txn_pipeline.normalize import (
    clean_id,
    clean_text,
    fmt_amount,
    parse_date,
    parse_decimal,
    parse_time,
)

DATE_FORMATS = ("%d-%m-%Y", "%d/%m/%Y")
TIME_FORMATS = ("%H:%M:%S", "%H:%M")


class TestParseDate:
    def test_both_feed_formats_resolve_to_the_same_day(self):
        assert parse_date("01-08-2026", DATE_FORMATS, 2000, 2100) == date(2026, 8, 1)
        assert parse_date("1/08/2026", DATE_FORMATS, 2000, 2100) == date(2026, 8, 1)

    def test_slash_dates_are_day_first_never_month_first(self):
        assert parse_date("2/08/2026", DATE_FORMATS, 2000, 2100) == date(2026, 8, 2)

    @pytest.mark.parametrize(
        "bad", ["31-02-2026", "00-08-2026", "15-13-2026", "N/A", "2026-08-01", "", "   ", None, "01-08-0026"]
    )
    def test_invalid_dates_raise(self, bad):
        with pytest.raises(ValueError):
            parse_date(bad, DATE_FORMATS, 2000, 2100)

    def test_list_and_tuple_formats_are_both_accepted(self):
        assert parse_date("01-08-2026", list(DATE_FORMATS), 2000, 2100) == date(2026, 8, 1)


class TestParseTime:
    @pytest.mark.parametrize("text", ["09:05:00", "9:05:00"])
    def test_padded_and_unpadded_hours_are_equal(self, text):
        assert parse_time(text, TIME_FORMATS) == time(9, 5, 0)

    def test_minute_precision_is_accepted(self):
        assert parse_time("23:41", TIME_FORMATS) == time(23, 41, 0)

    @pytest.mark.parametrize("bad", ["25:00:00", "10:61:00", "noon", "", None])
    def test_invalid_times_raise(self, bad):
        with pytest.raises(ValueError):
            parse_time(bad, TIME_FORMATS)


class TestParseDecimal:
    @pytest.mark.parametrize("text", ["5000", "5000.0", "5000.00", " 5000 ", "+5000"])
    def test_precision_variants_normalise_to_the_same_value(self, text):
        assert fmt_amount(parse_decimal(text)) == "5000.00"

    def test_short_fraction_is_padded(self):
        assert fmt_amount(parse_decimal("21400.6")) == "21400.60"

    def test_blank_is_none_not_zero(self):
        assert parse_decimal("") is None
        assert parse_decimal("   ") is None
        assert parse_decimal(None) is None

    @pytest.mark.parametrize("bad", ["abc", "1e5", "1_000", "--5", "1.2.3", "NaN", "Infinity", "١٢٣", "$100"])
    def test_non_plain_numbers_are_rejected(self, bad):
        with pytest.raises(ValueError):
            parse_decimal(bad)

    def test_excess_precision_is_rejected_not_rounded(self):
        with pytest.raises(ValueError, match="decimal places"):
            parse_decimal("10.005")

    def test_trailing_zeros_beyond_precision_are_harmless(self):
        assert fmt_amount(parse_decimal("10.500")) == "10.50"

    def test_negative_values_parse(self):
        assert parse_decimal("-19215.07") == Decimal("-19215.07")

    def test_thousands_separator(self):
        assert fmt_amount(parse_decimal("1,250.00", thousands_separator=",")) == "1250.00"

    def test_european_separators(self):
        assert fmt_amount(parse_decimal("1.250,50", decimal_separator=",", thousands_separator=".")) == "1250.50"

    def test_money_is_exact_decimal_not_float(self):
        total = sum(parse_decimal(v) for v in ["0.10", "0.20"])
        assert total == Decimal("0.30")


class TestCleaners:
    def test_clean_id_trims_and_uppercases(self):
        assert clean_id("  ref 123 ") == "REF 123"
        assert clean_id("ac100000001") == "AC100000001"

    def test_blank_becomes_none(self):
        assert clean_id("   ") is None
        assert clean_text("") is None

    def test_internal_whitespace_collapses(self):
        assert clean_text("UPI-ZOMATO   ORDER") == "UPI-ZOMATO ORDER"
