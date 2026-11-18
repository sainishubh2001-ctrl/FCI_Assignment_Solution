from __future__ import annotations

import hashlib
from typing import Sequence

import pandas as pd

from .config import SourceConfig


def make_txn_id(source: str, parts: Sequence[object]) -> str:
    """Stable id for a business key: same key, same id, in any run and any order."""
    text = "\x1f".join([source, *("" if p is None or p != p else str(p) for p in parts)])
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:32]


def add_txn_ids(valid: pd.DataFrame, cfg: SourceConfig) -> pd.DataFrame:
    keys = valid[list(cfg.dedup_key)].itertuples(index=False, name=None)
    ids = pd.Series([make_txn_id(cfg.name, key) for key in keys], index=valid.index, dtype=object)
    return valid.assign(txn_id=ids)


def split_batch(valid: pd.DataFrame, stored_ids: set[str]) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Returns (new rows, repeats inside this file, rows already in the store).

    The first occurrence of a key in the file is the candidate; a candidate whose key is
    already stored is a re-delivery of an earlier load.
    """
    repeat = valid.duplicated("txn_id", keep="first")
    candidates = valid[~repeat]
    stored = candidates["txn_id"].isin(stored_ids)
    return candidates[~stored], valid[repeat], candidates[stored]
