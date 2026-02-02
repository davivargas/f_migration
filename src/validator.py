from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import List, Dict, Any

import pandas as pd


@dataclass(frozen=True)
class Issue:
    category: str
    message: str
    count: int
    examples: List[Dict[str, Any]]


def _require_columns(df: pd.DataFrame, required: List[str], label: str) -> List[Issue]:
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
    needed_tx = {"account_id", "currency", "transaction_id"}
    needed_ac = {"account_id", "currency"}
    if not needed_tx.issubset(transactions.columns) or not needed_ac.issubset(accounts.columns):
        return []

    tx = transactions.copy()
    ac = accounts.copy()
    tx["account_id"] = tx["account_id"].astype(str)
    ac["account_id"] = ac["account_id"].astype(str)

    merged = tx.merge(ac[["account_id", "currency"]], on="account_id", how="left", suffixes=("_tx", "_ac"))
    mism = merged[(merged["currency_tx"].notna()) & (merged["currency_ac"].notna()) & (merged["currency_tx"] != merged["currency_ac"])]
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
    issues: List[Issue] = []

    issues.extend(_require_columns(accounts, ["account_id", "account_name", "type", "currency"], "accounts"))
    issues.extend(_require_columns(transactions, ["transaction_id", "account_id", "amount", "currency", "date"], "transactions"))

    issues.extend(_duplicate_ids(accounts, "account_id", "accounts"))
    issues.extend(_duplicate_ids(transactions, "transaction_id", "transactions"))

    issues.extend(_missing_account_refs(transactions, accounts))
    issues.extend(_currency_mismatches(transactions, accounts))
    issues.extend(_future_dated_transactions(transactions))
    issues.extend(_zero_amounts(transactions))

    return issues
