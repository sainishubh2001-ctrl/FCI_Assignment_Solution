from __future__ import annotations

from datetime import date, time
from decimal import Decimal

import pytest

from txn_pipeline.normalize import clean_id, parse_date, parse_decimal, parse_time

DATES = ("%d-%m-%Y", "%d/%m/%Y")
TIMES = ("%H:%M:%S", "%H:%M")


def test_both_date_styles_give_the_same_day():
    assert parse_date("01-08-2026", DATES) == parse_date("1/08/2026", DATES) == date(2026, 8, 1)


def test_slash_dates_are_day_first():
    assert parse_date("2/08/2026", DATES) == date(2026, 8, 2)


@pytest.mark.parametrize("bad", ["31-02-2026", "15-13-2026", "N/A", "2026-08-01", "", None])
def test_bad_dates_raise(bad):
    with pytest.raises(ValueError):
        parse_date(bad, DATES)


def test_unpadded_hour_is_the_same_time():
    assert parse_time("9:05:00", TIMES) == parse_time("09:05:00", TIMES) == time(9, 5)


@pytest.mark.parametrize("bad", ["25:00:00", "noon", ""])
def test_bad_times_raise(bad):
    with pytest.raises(ValueError):
        parse_time(bad, TIMES)


@pytest.mark.parametrize("text", ["5000", "5000.0", "5000.00", " 5000 "])
def test_amount_precision_does_not_matter(text):
    assert str(parse_decimal(text)) == "5000.00"


def test_blank_amount_is_none_not_zero():
    assert parse_decimal("  ") is None


@pytest.mark.parametrize("bad", ["abc", "1e5", "1_000", "--5", "NaN", "$100"])
def test_anything_but_a_plain_number_raises(bad):
    with pytest.raises(ValueError):
        parse_decimal(bad)


def test_extra_precision_is_an_error_not_a_rounding():
    with pytest.raises(ValueError, match="decimal places"):
        parse_decimal("10.005")


def test_separators_are_configurable():
    assert parse_decimal("1,250.50", thousands_separator=",") == Decimal("1250.50")
    assert parse_decimal("1.250,50", decimal_separator=",", thousands_separator=".") == Decimal("1250.50")


def test_ids_are_trimmed_and_upper_cased():
    assert clean_id("  ac1 ") == "AC1"
    assert clean_id("  ") is None
