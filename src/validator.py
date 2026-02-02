from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import List, Dict, Any

import pandas as pd


@dataclass(frozen=True)
class Issue:
    """Single validation finding with a small sample of concrete examples."""
    category: str
    message: str
    count: int
    examples: List[Dict[str, Any]]


def _require_columns(df: pd.DataFrame, required: List[str], label: str) -> List[Issue]:
    """Validate that df contains required columns; returns a schema Issue if not."""
    missing = [c for c in required if c not in df.columns]
    if not missing:
        return []
    return [Issue(
        category="schema",
        message=f"{label} missing required columns: {', '.join(missing)}",
        count=len(missing),
        examples=[]
    )]


def _duplicate_ids(df: pd.DataFrame, id_col: str, label: str) -> List[Issue]:
    """Detect duplicate identifier values in id_col and return unique duplicates count."""
    if id_col not in df.columns:
        return []
    dup_mask = df[id_col].duplicated(keep=False)
    dup = df.loc[dup_mask, [id_col]].dropna()
    if dup.empty:
        return []
    examples = dup.drop_duplicates().head(5).to_dict(orient="records")
    return [Issue(
        category="integrity",
        message=f"{label} has duplicate {id_col} values",
        count=int(dup[id_col].nunique()),
        examples=examples
    )]


def _missing_account_refs(transactions: pd.DataFrame, accounts: pd.DataFrame) -> List[Issue]:
    """Flag transactions whose account_id does not exist in accounts."""
    if "account_id" not in transactions.columns or "account_id" not in accounts.columns:
        return []
    acc_ids = set(accounts["account_id"].astype(str).dropna().tolist())
    tx_acc = transactions["account_id"].astype(str)
    missing_mask = ~tx_acc.isin(acc_ids)
    missing = transactions.loc[missing_mask, ["transaction_id", "account_id"]].dropna()
    if missing.empty:
        return []
    examples = missing.head(5).to_dict(orient="records")
    return [Issue(
        category="integrity",
        message="Transactions reference missing accounts",
        count=int(missing.shape[0]),
        examples=examples
    )]


def _currency_mismatches(transactions: pd.DataFrame, accounts: pd.DataFrame) -> List[Issue]:
    """Flag transactions whose currency differs from the referenced account currency."""
    needed_tx = {"account_id", "currency", "transaction_id"}
    needed_ac = {"account_id", "currency"}
    if not needed_tx.issubset(transactions.columns) or not needed_ac.issubset(accounts.columns):
        return []

    tx = transactions.copy()
    ac = accounts.copy()
    tx["account_id"] = tx["account_id"].astype(str)
    ac["account_id"] = ac["account_id"].astype(str)

    merged = tx.merge(
        ac[["account_id", "currency"]],
        on="account_id",
        how="left",
        suffixes=("_tx", "_ac")
    )
    mism = merged[
        (merged["currency_tx"].notna())
        & (merged["currency_ac"].notna())
        & (merged["currency_tx"] != merged["currency_ac"])
    ]
    if mism.empty:
        return []
    examples = mism[["transaction_id", "account_id", "currency_tx", "currency_ac"]].head(5).to_dict(orient="records")
    return [Issue(
        category="sanity",
        message="Currency mismatch between transaction and account",
        count=int(mism.shape[0]),
        examples=examples
    )]


def _future_dated_transactions(transactions: pd.DataFrame) -> List[Issue]:
    """Flag transactions with a parsed date later than today."""
    if "date" not in transactions.columns:
        return []
    tx = transactions.copy()
    tx["date_parsed"] = pd.to_datetime(tx["date"], errors="coerce").dt.date
    today = date.today()
    future = tx[(tx["date_parsed"].notna()) & (tx["date_parsed"] > today)]
    if future.empty:
        return []
    examples = future[["transaction_id", "date"]].head(5).to_dict(orient="records")
    return [Issue(
        category="sanity",
        message="Transactions dated in the future",
        count=int(future.shape[0]),
        examples=examples
    )]


def _zero_amounts(transactions: pd.DataFrame) -> List[Issue]:
    """Flag transactions where amount parses to numeric zero."""
    if "amount" not in transactions.columns:
        return []
    tx = transactions.copy()
    tx["amount_num"] = pd.to_numeric(tx["amount"], errors="coerce")
    zero = tx[tx["amount_num"] == 0]
    if zero.empty:
        return []
    examples = zero[["transaction_id", "amount"]].head(5).to_dict(orient="records")
    return [Issue(
        category="sanity",
        message="Zero-amount transactions",
        count=int(zero.shape[0]),
        examples=examples
    )]


def validate(accounts: pd.DataFrame, transactions: pd.DataFrame, vendors: pd.DataFrame | None = None) -> List[Issue]:
    """
    Run all validations and return a flat list of Issues.

    The function is intentionally format-agnostic; adapters are responsible for
    mapping raw inputs into the expected canonical columns.
    """
    issues: List[Issue] = []

    issues.extend(_require_columns(accounts, ["account_id", "account_name", "type", "currency"], "accounts"))
    issues.extend(_require_columns(transactions, ["transaction_id", "account_id", "amount", "currency", "date"], "transactions"))

    issues.extend(_duplicate_ids(accounts, "account_id", "accounts"))
    issues.extend(_duplicate_ids(transactions, "transaction_id", "transactions"))

    issues.extend(_missing_account_refs(transactions, accounts))
    issues.extend(_currency_mismatches(transactions, accounts))
    issues.extend(_future_dated_transactions(transactions))
    issues.extend(_zero_amounts(transactions))
    issues.extend(_missing_transaction_ids(transactions))

    return issues


def _missing_transaction_ids(transactions: pd.DataFrame) -> List[Issue]:
    """Flag missing/blank transaction_id values (including stringified nulls)."""
    if "transaction_id" not in transactions.columns:
        return []
    txid = transactions["transaction_id"].astype(str)
    missing = txid.str.strip().isin(["", "nan", "None"])
    if not missing.any():
        return []
    examples = transactions.loc[missing, ["transaction_id"]].head(5).to_dict(orient="records")
    return [Issue(
        category="schema",
        message="Missing or invalid transaction_id values",
        count=int(missing.sum()),
        examples=examples
    )]
