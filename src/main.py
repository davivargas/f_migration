from pathlib import Path
import argparse
import json

from src.adapters.simple_csv import SimpleCsvAdapter
from src.adapters.kaggle_financial_accounting import KaggleFinancialAccountingAdapter
from src.validator import validate
from src.anomalies import detect_amount_outliers
from src.report import build_summary, format_summary, to_json_dict, RiskLevel
from src.stress import apply_stress


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Next-Day Migration Validator"
    )

    parser.add_argument(
        "--format",
        default="simple_csv",
        choices=["simple_csv", "kaggle_financial_accounting"],
        help="Input data format"
    )

    parser.add_argument(
        "--input",
        default="data",
        help="Folder (simple_csv) or CSV file path (kaggle_financial_accounting)"
    )

    parser.add_argument(
        "--currency",
        default="USD",
        help="Default currency for datasets without currency information"
    )

    parser.add_argument(
        "--z",
        type=float,
        default=3.5,
        help="Z-score threshold for anomaly detection"
    )

    parser.add_argument(
    "--stress-test",
    action="store_true",
    help="Inject controlled data issues to test validation logic"
    )

    parser.add_argument(
    "--json",
    default="",
    help="Write JSON report to this path (e.g. out/report.json)."
    )

    return parser.parse_args()


def main() -> int:
    args = parse_args()
    input_path = Path(args.input)

    if args.format == "simple_csv":
        adapter = SimpleCsvAdapter()
    else:
        adapter = KaggleFinancialAccountingAdapter(
            default_currency=args.currency
        )

    loaded = adapter.load(input_path)

    if args.stress_test:
        loaded = loaded.__class__(
            accounts=loaded.accounts,
            transactions=apply_stress(loaded.transactions, loaded.accounts),
            vendors=loaded.vendors
        )


    issues = validate(
        loaded.accounts,
        loaded.transactions,
        loaded.vendors
    )

    anomaly = detect_amount_outliers(
        loaded.transactions,
        z_threshold=args.z
    )

    vendors_count = (
        None if loaded.vendors is None else int(loaded.vendors.shape[0])
    )

    summary = build_summary(
        accounts_count=int(loaded.accounts.shape[0]),
        transactions_count=int(loaded.transactions.shape[0]),
        vendors_count=vendors_count,
        issues=issues,
        anomaly=anomaly
    )

    print(format_summary(summary))
    if args.json:
        out_path = Path(args.json)
        out_path.parent.mkdir(parents=True, exist_ok=True)

        with out_path.open("w", encoding="utf-8") as f:
            json.dump(to_json_dict(summary), f, indent=2, ensure_ascii=False)

    if summary.risk == RiskLevel.LOW:
        return 0
    if summary.risk == RiskLevel.MEDIUM:
        return 2
    return 5


if __name__ == "__main__":
    raise SystemExit(main())
