from __future__ import annotations

import random
import pandas as pd
from datetime import date


def apply_stress(transactions: pd.DataFrame, accounts: pd.DataFrame) -> pd.DataFrame:
    """
    Inject controlled data issues into transactions to stress-test validation logic.

    This is intentionally deterministic (fixed seed) so test results are repeatable.
    """
    tx = transactions.copy()
    n = len(tx)

    rng = random.Random(42)

    def sample_idx(pct: float) -> list[int]:
        """Return a reproducible random sample of row indices."""
        return rng.sample(range(n), int(n * pct))

    # Invalid account references
    for i in sample_idx(0.005):
        tx.at[i, "account_id"] = 999999

    # Currency mismatches
    for i in sample_idx(0.005):
        tx.at[i, "currency"] = "EUR"

    # Future-dated transactions
    for i in sample_idx(0.002):
        tx.at[i, "date"] = date(2035, 1, 1).isoformat()

    # Zero-amount transactions
    for i in sample_idx(0.001):
        tx.at[i, "amount"] = 0.0

    # Extreme values to trigger anomaly detection
    for i in rng.sample(range(n), min(10, n)):
        tx.at[i, "amount"] = 9.99e7

    return tx
