from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
import pytest

from src.validator import validate
from src.anomalies import top_n_amount_outliers
from src.report import build_summary, to_json_dict, RiskLevel

from src.adapters.kaggle_financial_accounting import KaggleFinancialAccountingAdapter
from src.adapters.gov_canada_gl import GovCanadaGLAdapter


# -----------------------------
# Helpers
# -----------------------------

def _project_root() -> Path:
    here = Path(__file__).resolve()
    return next(p for p in here.parents if (p / "src").exists())


def _p(rel: str) -> Path:
    p = _project_root() / rel
    if not p.exists():
        pytest.skip(f"Missing file: {p}")
    return p


def _issue_map(issues) -> Dict[str, int]:
    """
    Map issue.message -> issue.count for exact comparisons.
    """
    return {str(i.message): int(i.count) for i in issues}


def _find_issue_by_substring(issues, needle: str):
    n = needle.lower()
    for i in issues:
        if n in str(i.message).lower():
            return i
    return None


def _run_eval(accounts: pd.DataFrame,
              transactions: pd.DataFrame,
              vendors: Optional[pd.DataFrame],
              anomaly=None):
    """
    Produce the same kind of output your CLI produces: issues + summary json.
    This validates the *evaluation*, not just internal functions.
    """
    issues = validate(accounts, transactions, vendors)
    vendors_count = None if vendors is None else int(vendors.shape[0])

    summary = build_summary(
        accounts_count=int(accounts.shape[0]),
        transactions_count=int(transactions.shape[0]),
        vendors_count=vendors_count,
        issues=issues,
        anomaly=anomaly,
    )
    return issues, summary


def _top2_truth(transactions: pd.DataFrame) -> List[Dict[str, object]]:
    """
    Independently compute the true top-2 outliers by absolute amount.
    """
    tx = transactions.copy()
    tx["amount_num"] = pd.to_numeric(tx["amount"], errors="coerce")
    tx = tx[tx["amount_num"].notna()]
    tx["abs_amount"] = tx["amount_num"].abs()
    out = tx.sort_values("abs_amount", ascending=False).head(2)
    return out[["transaction_id", "amount"]].to_dict(orient="records")


# -----------------------------
# Mock datasets (deterministic)
# -----------------------------

@pytest.fixture
def mock_broken():
    accounts = pd.DataFrame([
        {"account_id": 10, "account_name": "Cash", "type": "asset", "currency": "USD"},
        {"account_id": 11, "account_name": "Revenue", "type": "revenue", "currency": "USD"},
    ])

    today = date.today()
    future = today + timedelta(days=7)

    # Engineered to produce exact known findings
    transactions = pd.DataFrame([
        {"transaction_id": "dup", "account_id": 10, "amount": 100.0, "currency": "USD", "date": str(today)},
        {"transaction_id": "dup", "account_id": 10, "amount": 120.0, "currency": "USD", "date": str(today)},  # duplicate id (1)
        {"transaction_id": "missing_acc", "account_id": 999, "amount": 10.0, "currency": "USD", "date": str(today)},  # missing FK (1)
        {"transaction_id": "future", "account_id": 10, "amount": 10.0, "currency": "USD", "date": str(future)},  # future date (1)
        {"transaction_id": "zero", "account_id": 10, "amount": 0.0, "currency": "USD", "date": str(today)},  # zero (1)
        {"transaction_id": "cur_mismatch", "account_id": 11, "amount": 1.0, "currency": "EUR", "date": str(today)},  # mismatch (1)
    ])

    return accounts, transactions, None


@pytest.fixture
def mock_outliers():
    accounts = pd.DataFrame([
        {"account_id": 1, "account_name": "Cash", "type": "asset", "currency": "USD"},
    ])

    today = str(date.today())
    amounts = [10.0] * 100 + [12.0] * 100 + [9.0] * 100
    amounts += [5_000_000.0, -9_000_000.0, 15_000_000.0, -20_000_000.0]

    tx = pd.DataFrame([
        {"transaction_id": f"t{i}", "account_id": 1, "amount": float(a), "currency": "USD", "date": today}
        for i, a in enumerate(amounts, start=1)
    ])

    return accounts, tx, None


def test_mock_broken_exact_evaluation(mock_broken):
    accounts, tx, vendors = mock_broken
    issues, summary = _run_eval(accounts, tx, vendors)

    # Instead of vague “issue exists”, assert correctness of evaluation.
    # We match by substrings because your Issue.message is human text.
    dup = _find_issue_by_substring(issues, "duplicate")
    missing_fk = _find_issue_by_substring(issues, "missing accounts")
    future = _find_issue_by_substring(issues, "future")
    zero = _find_issue_by_substring(issues, "zero-amount")
    cur = _find_issue_by_substring(issues, "currency mismatch")

    assert dup is not None and int(dup.count) == 1
    assert missing_fk is not None and int(missing_fk.count) == 1
    assert future is not None and int(future.count) == 1
    assert zero is not None and int(zero.count) == 1
    assert cur is not None and int(cur.count) == 1

    # Risk should not be LOW for this dataset
    assert summary.risk in (RiskLevel.MEDIUM, RiskLevel.HIGH)


def test_mock_top_outliers_examples_are_true(mock_outliers):
    accounts, tx, vendors = mock_outliers

    anomaly = top_n_amount_outliers(tx, n=50)
    assert anomaly is not None and anomaly.count == 50

    # Validate the *examples* match the true top-2 values by abs amount.
    truth = _top2_truth(tx)
    got = anomaly.examples[:2]

    assert got[0]["transaction_id"] == truth[0]["transaction_id"]
    assert float(got[0]["amount"]) == float(truth[0]["amount"])
    assert got[1]["transaction_id"] == truth[1]["transaction_id"]
    assert float(got[1]["amount"]) == float(truth[1]["amount"])


# -----------------------------
# Real datasets (evaluation validity)
# -----------------------------

@pytest.fixture(scope="session")
def kaggle_loaded():
    path = _p("data/real_datasets/financial_accounting.csv")
    adapter = KaggleFinancialAccountingAdapter(default_currency="USD")
    return adapter.load(path)


@pytest.fixture(scope="session")
def gov_loaded():
    path = _p("data/real_datasets/receiver_general_accounting_transactions_canada.csv")
    adapter = GovCanadaGLAdapter(currency="CAD")
    loaded = adapter.load(path)

    # Collect meta without mutating frozen LoadedData
    cleaning = getattr(adapter, "last_cleaning_stats", None)
    fallback = int(getattr(adapter, "last_fallback_ids_used", 0))

    return loaded, cleaning, fallback


def test_real_gov_top_outliers_examples_match_truth(gov_loaded):
    loaded, cleaning, fallback = gov_loaded

    # Evaluate with top-outliers like your CLI
    anomaly = top_n_amount_outliers(loaded.transactions, n=50)
    assert anomaly is not None and anomaly.count == 50

    truth = _top2_truth(loaded.transactions)
    got = anomaly.examples[:2]

    assert got[0]["transaction_id"] == truth[0]["transaction_id"]
    assert float(got[0]["amount"]) == float(truth[0]["amount"])
    assert got[1]["transaction_id"] == truth[1]["transaction_id"]
    assert float(got[1]["amount"]) == float(truth[1]["amount"])

    # Also sanity: fallback IDs used should equal number of gov_row_*
    gov_row_count = int(
        loaded.transactions["transaction_id"].astype(str).str.startswith("gov_row_").sum()
    )
    assert gov_row_count == fallback


def test_real_kaggle_has_no_missing_fk(kaggle_loaded):
    tx = kaggle_loaded.transactions
    ac = kaggle_loaded.accounts

    # Independent FK check: every tx.account_id must exist in accounts.account_id
    tx_ids = set(pd.to_numeric(tx["account_id"], errors="coerce").dropna().astype(int).tolist())
    ac_ids = set(pd.to_numeric(ac["account_id"], errors="coerce").dropna().astype(int).tolist())

    missing = tx_ids.difference(ac_ids)
    assert len(missing) == 0


# -----------------------------
# Golden regression tests (optional but best)
# -----------------------------

def test_real_gov_matches_golden_json_if_present(gov_loaded):
    """
    If you save a golden expected output, this becomes a strict evaluation contract test.
    """
    golden_path = _project_root() / "tests" / "expected" / "gov_report.json"
    if not golden_path.exists():
        pytest.skip("Golden file not found: tests/expected/gov_report.json")

    loaded, cleaning, fallback = gov_loaded
    anomaly = top_n_amount_outliers(loaded.transactions, n=50)
    issues, summary = _run_eval(loaded.accounts, loaded.transactions, loaded.vendors, anomaly=anomaly)

    cleaning_dict = None
    if cleaning is not None:
        cleaning_dict = cleaning.__dict__.copy()
        cleaning_dict["fallback_transaction_ids_used"] = fallback

    current = to_json_dict(summary, cleaning_dict)
    expected = json.loads(golden_path.read_text(encoding="utf-8"))

    assert current == expected
