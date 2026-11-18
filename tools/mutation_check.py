"""Break the dedup and backfill logic on purpose and confirm the test suite notices.

Every mutation is applied to a scratch copy of the repository and the suite is run against it;
each mutation must make at least one test fail. Run from the repository root:

    python tools/mutation_check.py
"""

from __future__ import annotations

import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
KEY = "key: [account_number, txn_date, txn_time, ref_no, direction, amount]"
BANK_A = "config/bank_a.yaml"

MUTATIONS = [
    ("key drops txn_time: same-ref repeats collapse", BANK_A, KEY, "key: [account_number, txn_date, ref_no, direction, amount]"),
    ("key adds balance: restated copies become new rows", BANK_A, KEY, KEY[:-1] + ", balance]"),
    ("key adds account_type: type drift defeats dedup", BANK_A, KEY, KEY[:-1] + ", account_type]"),
    ("key is (account, date, description): legit repeats lost", BANK_A, KEY, "key: [account_number, txn_date, description]"),
    ("key is ref_no only", BANK_A, KEY, "key: [ref_no]"),
    ("slash-style dates rejected: re-delivery not recognised", BANK_A, 'date: ["%d-%m-%Y", "%d/%m/%Y"]', 'date: ["%d-%m-%Y"]'),
    (
        "time padding not normalised",
        "src/txn_pipeline/validate.py",
        'out["txn_time"] = parse_time(rec.get("txn_time"), cfg.time_formats).strftime("%H:%M:%S")',
        'out["txn_time"] = str(rec.get("txn_time"))',
    ),
    (
        "amount precision not normalised",
        "src/txn_pipeline/validate.py",
        'out["direction"], out["amount"] = "DEBIT", out["debit"]',
        'out["direction"], out["amount"] = "DEBIT", rec.get("debit")',
    ),
    (
        "re-delivery not checked against the store",
        "src/txn_pipeline/dedup.py",
        'known = candidates["txn_id"].isin(already_loaded_ids)',
        'known = candidates["txn_id"].isin(set())',
    ),
    (
        "file-level SHA-256 ledger ignored",
        "src/txn_pipeline/pipeline.py",
        "    owner = store.batch_owner(sha)\n    if owner is None:",
        "    owner = None\n    if owner is None:",
    ),
    (
        "lateness never detected: nothing is a backfill",
        "src/txn_pipeline/pipeline.py",
        'flags = [(batch_date - date.fromisoformat(d)).days >= threshold_days for d in frame["txn_date"]]',
        'flags = [False for d in frame["txn_date"]]',
    ),
]


def run_mutation(rel_path: str, old: str, new: str) -> tuple[str, str]:
    scratch = Path(tempfile.mkdtemp(prefix="mutation_"))
    try:
        for part in ("src", "tests", "config", "sample_data"):
            shutil.copytree(REPO / part, scratch / part, ignore=shutil.ignore_patterns("__pycache__"))
        shutil.copy(REPO / "pyproject.toml", scratch / "pyproject.toml")
        target = scratch / rel_path
        text = target.read_text(encoding="utf-8")
        if old not in text:
            return "NOT APPLIED", f"pattern not found in {rel_path}"
        target.write_text(text.replace(old, new, 1), encoding="utf-8")
        proc = subprocess.run(
            [sys.executable, "-m", "pytest", "-q", "-x", "-p", "no:cacheprovider", "tests",
             "--deselect", "tests/test_config.py::test_sample_config_loads_with_expected_policy"],
            cwd=scratch, capture_output=True, text=True,
        )
        if proc.returncode == 0:
            return "SURVIVED", "the whole suite still passes"
        first_failure = next((line for line in proc.stdout.splitlines() if line.startswith("FAILED")), "")
        return "caught", first_failure.removeprefix("FAILED tests/").split(" - ")[0]
    finally:
        shutil.rmtree(scratch, ignore_errors=True)


def main() -> int:
    bad = 0
    for name, rel_path, old, new in MUTATIONS:
        verdict, detail = run_mutation(rel_path, old, new)
        bad += verdict != "caught"
        print(f"{verdict:12s} {name:58s} {detail}")
    print(f"\n{len(MUTATIONS) - bad}/{len(MUTATIONS)} mutations caught")
    return 1 if bad else 0


if __name__ == "__main__":
    raise SystemExit(main())
