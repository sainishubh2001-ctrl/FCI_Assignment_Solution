"""Entry point: python run_pipeline.py [--config ... --input ... --output ... --state ... --reset]"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent / "src"))

from txn_pipeline.cli import main  # noqa: E402

if __name__ == "__main__":
    raise SystemExit(main())
