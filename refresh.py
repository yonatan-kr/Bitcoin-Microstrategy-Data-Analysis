"""
refresh.py — incremental pipeline refresh
  1. Appends new BTC candles from Binance since last load
  2. Refreshes MSTR daily + hourly from yfinance
  3. Runs all dbt models

Usage:
    python refresh.py
"""

import subprocess
import sys
from pathlib import Path

ROOT    = Path(__file__).parent
DBT_DIR = ROOT / "transform" / "btc_dbt"
PYTHON  = sys.executable  # always use the same interpreter that launched this script
DBT     = str(Path(sys.executable).parent / "dbt")


def run(cmd: list, cwd: Path = ROOT) -> None:
    print(f"\n>>> {' '.join(cmd)}")
    result = subprocess.run(cmd, cwd=cwd)
    if result.returncode != 0:
        print(f"ERROR: command failed with exit code {result.returncode}")
        sys.exit(result.returncode)


if __name__ == "__main__":
    print("=" * 60)
    print("  BTC / MSTR Pipeline Refresh")
    print("=" * 60)

    # 1. Incremental BTC load
    run([PYTHON, "ingestion/btc_to_bigquery.py"])

    # 2. MSTR refresh (always full reload from yfinance)
    run([PYTHON, "ingestion/mstr_to_bigquery.py"])

    # 3. dbt run — rebuild all models
    run([DBT, "run"], cwd=DBT_DIR)

    print("\n All done.")
