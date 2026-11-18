from __future__ import annotations

import re
from datetime import date, datetime, time
from functools import lru_cache
from decimal import Decimal, InvalidOperation
from typing import Sequence

_NUMBER = re.compile(r"[+-]?([0-9]+(\.[0-9]*)?|\.[0-9]+)")


def clean_text(value: str | None) -> str | None:
    if value is None:
        return None
    text = " ".join(str(value).split())
    return text or None


def clean_id(value: str | None) -> str | None:
    text = clean_text(value)
    return text.upper() if text else None


def parse_date(value: str | None, formats: Sequence[str], min_year: int, max_year: int) -> date:
    text = clean_text(value)
    if text is None:
        raise ValueError("date is blank")
    return _parse_date(text, tuple(formats), min_year, max_year)


def parse_time(value: str | None, formats: Sequence[str]) -> time:
    text = clean_text(value)
    if text is None:
        raise ValueError("time is blank")
    return _parse_time(text, tuple(formats))


@lru_cache(maxsize=8192)
def _parse_date(text: str, formats: tuple[str, ...], min_year: int, max_year: int) -> date:
    for fmt in formats:
        try:
            parsed = datetime.strptime(text, fmt).date()
        except ValueError:
            continue
        if not min_year <= parsed.year <= max_year:
            raise ValueError(f"{text!r} has year {parsed.year}, outside {min_year}-{max_year}")
        return parsed
    raise ValueError(f"{text!r} is not a real date in any of {list(formats)}")


@lru_cache(maxsize=8192)
def _parse_time(text: str, formats: tuple[str, ...]) -> time:
    for fmt in formats:
        try:
            return datetime.strptime(text, fmt).time()
        except ValueError:
            continue
    raise ValueError(f"{text!r} does not match any of {list(formats)}")


def parse_decimal(
    value: str | None,
    places: int = 2,
    decimal_separator: str = ".",
    thousands_separator: str | None = None,
) -> Decimal | None:
    """Exact decimal from text; blank -> None. Never goes through float and never rounds."""
    text = clean_text(value)
    if text is None:
        return None
    if thousands_separator:
        text = text.replace(thousands_separator, "")
    if decimal_separator != ".":
        text = text.replace(decimal_separator, ".")
    if not _NUMBER.fullmatch(text):
        raise ValueError(f"{value!r} is not a plain decimal number")
    number = Decimal(text)
    quantum = Decimal(1).scaleb(-places)
    try:
        rounded = number.quantize(quantum)
    except InvalidOperation as exc:
        raise ValueError(f"{value!r} is too large") from exc
    if rounded != number:
        raise ValueError(f"{value!r} has more than {places} decimal places")
    return rounded


def fmt_amount(value: Decimal | None) -> str | None:
    return None if value is None else format(value, "f")
