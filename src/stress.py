from __future__ import annotations

import random
import pandas as pd
from datetime import date


def apply_stress(transactions: pd.DataFrame, accounts: pd.DataFrame) -> pd.DataFrame:
    tx = transactions.copy()
    n = len(tx)

    rng = random.Random(42)

    def sample_idx(pct: float) -> list[int]:
        return rng.sample(range(n), int(n * pct))

    for i in sample_idx(0.005):
        tx.at[i, "account_id"] = 999999

    for i in sample_idx(0.005):
        tx.at[i, "currency"] = "EUR"

    for i in sample_idx(0.002):
        tx.at[i, "date"] = date(2035, 1, 1).isoformat()

    for i in sample_idx(0.001):
        tx.at[i, "amount"] = 0.0

    for i in rng.sample(range(n), min(10, n)):
        tx.at[i, "amount"] = 9.99e7

    return tx
