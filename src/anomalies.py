from __future__ import annotations

from dataclasses import dataclass
from typing import List, Dict, Any

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class AnomalyResult:
    """Container for anomaly detection results."""
    message: str
    count: int
    examples: List[Dict[str, Any]]


def detect_amount_outliers(
    transactions: pd.DataFrame,
    z_threshold: float = 3.5
) -> AnomalyResult | None:
    """
    Detect statistical outliers in transaction amounts using the modified Z-score
    (MAD-based), which is more robust than mean/std for heavy-tailed data.
    """
    if "amount" not in transactions.columns or "transaction_id" not in transactions.columns:
        return None

    tx = transactions.copy()
    tx["amount_num"] = pd.to_numeric(tx["amount"], errors="coerce")
    values = tx["amount_num"].dropna().values

    # Too few points to compute a meaningful distribution
    if values.size < 8:
        return None

    median = float(np.median(values))
    mad = float(np.median(np.abs(values - median)))

    # No variability -> no meaningful outliers
    if mad == 0:
        return None

    modified_z = 0.6745 * (tx["amount_num"] - median) / mad
    outliers = tx[modified_z.abs() > z_threshold]

    if outliers.empty:
        return None

    # Keep a small sample for human inspection
    examples = (
        outliers[["transaction_id", "amount"]]
        .head(5)
        .to_dict(orient="records")
    )

    return AnomalyResult(
        message=f"Anomalous transaction amounts (modified z-score > {z_threshold})",
        count=int(outliers.shape[0]),
        examples=examples
    )


def top_n_amount_outliers(
    transactions: pd.DataFrame,
    n: int = 50
) -> AnomalyResult | None:
    """
    Deterministically surface the N largest absolute transaction amounts.
    Intended for manual review rather than statistical inference.
    """
    if "amount" not in transactions.columns or "transaction_id" not in transactions.columns:
        return None

    tx = transactions.copy()
    tx["amount_num"] = pd.to_numeric(tx["amount"], errors="coerce")
    tx = tx[tx["amount_num"].notna()]

    if tx.empty:
        return None

    tx["abs_amount"] = tx["amount_num"].abs()
    out = tx.sort_values("abs_amount", ascending=False).head(n)

    if out.empty:
        return None

    # Only show a few examples in the report, even if N is large
    examples = (
        out[["transaction_id", "amount"]]
        .head(5)
        .to_dict(orient="records")
    )

    return AnomalyResult(
        message=f"Top {min(n, out.shape[0])} largest absolute transaction amounts",
        count=int(out.shape[0]),
        examples=examples
    )
