from __future__ import annotations

import hashlib
from decimal import Decimal
from typing import Sequence

import pandas as pd

from .config import SourceConfig
from .normalize import fmt_amount


def make_txn_id(source: str, parts: Sequence[object]) -> str:
    """Stable surrogate key: the same business key always yields the same id, in any run or order."""
    text = "\x1f".join([source, *(_key_part(part) for part in parts)])
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:32]


def add_txn_ids(valid: pd.DataFrame, cfg: SourceConfig) -> pd.DataFrame:
    keyed = valid.copy()
    key_columns = list(cfg.dedup_key)
    keyed["txn_id"] = pd.Series(
        [make_txn_id(cfg.name, parts) for parts in valid[key_columns].itertuples(index=False, name=None)],
        index=valid.index,
        dtype=object,
    )
    return keyed


def split_batch(
    valid: pd.DataFrame, already_loaded_ids: set[str]
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Split validated rows into (new, repeats inside this file, rows already in the store).

    The first occurrence of a key in the file is the candidate; later occurrences are in-file repeats.
    A candidate whose key is already stored is a re-delivery of an earlier load.
    """
    repeat_in_file = valid.duplicated("txn_id", keep="first")
    candidates = valid[~repeat_in_file]
    known = candidates["txn_id"].isin(already_loaded_ids)
    return candidates[~known], valid[repeat_in_file], candidates[known]


def _key_part(value: object) -> str:
    if value is None or (isinstance(value, float) and value != value):
        return ""
    if isinstance(value, Decimal):
        return fmt_amount(value) or ""
    return str(value)
