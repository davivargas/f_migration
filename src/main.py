from __future__ import annotations

import argparse
from pathlib import Path
import argparse
import json

from .loader import load_all
from .validator import validate
from .anomalies import detect_amount_outliers
from .report import build_summary, format_summary


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Next-Day Migration Validator: validate and summarize accounting exports."
    )
    parser.add_argument("--data", type=str, default="data", help="Path to folder containing CSV files.")
    parser.add_argument("--z", type=float, default=3.5, help="Modified z-score threshold for amount anomalies.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    data_dir = Path(args.data)

    loaded = load_all(data_dir)

    issues = validate(loaded.accounts, loaded.transactions, loaded.vendors)
    anomaly = detect_amount_outliers(loaded.transactions, z_threshold=args.z)

    vendors_count = None if loaded.vendors is None else int(loaded.vendors.shape[0])
    summary = build_summary(
        accounts_count=int(loaded.accounts.shape[0]),
        transactions_count=int(loaded.transactions.shape[0]),
        vendors_count=vendors_count,
        issues=issues,
        anomaly=anomaly
    )

    print(format_summary(summary))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
