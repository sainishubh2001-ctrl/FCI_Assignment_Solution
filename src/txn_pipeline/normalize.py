from __future__ import annotations

import re
from datetime import date, datetime, time
from decimal import Decimal, InvalidOperation
from functools import lru_cache
from typing import Sequence

# plain digits only: Decimal() would also accept "1e5", "1_000" and non-ASCII digits
_NUMBER = re.compile(r"[+-]?([0-9]+(\.[0-9]*)?|\.[0-9]+)")


def clean_text(value: str | None) -> str | None:
    text = " ".join(str(value).split()) if value is not None else ""
    return text or None


def clean_id(value: str | None) -> str | None:
    text = clean_text(value)
    return text.upper() if text else None


@lru_cache(maxsize=100_000)
def _parse_datetime(text: str, formats: tuple[str, ...]) -> datetime:
    for fmt in formats:
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            pass
    raise ValueError(f"{text!r} is not a valid value for {list(formats)}")


def _required(value: str | None) -> str:
    text = clean_text(value)
    if text is None:
        raise ValueError("is blank")
    return text


def parse_date(value: str | None, formats: Sequence[str]) -> date:
    return _parse_datetime(_required(value), tuple(formats)).date()


def parse_time(value: str | None, formats: Sequence[str]) -> time:
    return _parse_datetime(_required(value), tuple(formats)).time()


def parse_decimal(
    value: str | None, places: int = 2, decimal_separator: str = ".", thousands_separator: str | None = None
) -> Decimal | None:
    """Exact decimal, never via float. Blank gives None; extra precision is an error, not a rounding."""
    text = clean_text(value)
    if text is None:
        return None
    if thousands_separator:
        text = text.replace(thousands_separator, "")
    text = text.replace(decimal_separator, ".")
    if not _NUMBER.fullmatch(text):
        raise ValueError(f"{value!r} is not a plain number")
    number = Decimal(text)
    try:
        exact = number.quantize(Decimal(1).scaleb(-places))
    except InvalidOperation:
        raise ValueError(f"{value!r} is too large") from None
    if exact != number:
        raise ValueError(f"{value!r} has more than {places} decimal places")
    return exact
