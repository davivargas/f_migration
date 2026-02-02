from __future__ import annotations

from dataclasses import dataclass
from typing import List, Dict, Any

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class AnomalyResult:
    message: str
    count: int
    examples: List[Dict[str, Any]]


def detect_amount_outliers(transactions: pd.DataFrame, z_threshold: float = 3.5) -> AnomalyResult | None:
    if "amount" not in transactions.columns or "transaction_id" not in transactions.columns:
        return None

    tx = transactions.copy()
    tx["amount_num"] = pd.to_numeric(tx["amount"], errors="coerce")
    values = tx["amount_num"].dropna().values

    if values.size < 8:
        return None  # too small to be meaningful

    median = float(np.median(values))
    mad = float(np.median(np.abs(values - median)))

    if mad == 0:
        return None

    modified_z = 0.6745 * (tx["amount_num"] - median) / mad
    outliers = tx[modified_z.abs() > z_threshold]

    if outliers.empty:
        return None

    examples = outliers[["transaction_id", "amount"]].head(5).to_dict(orient="records")
    return AnomalyResult(
        message=f"Anomalous transaction amounts (modified z-score > {z_threshold})",
        count=int(outliers.shape[0]),
        examples=examples
    )
