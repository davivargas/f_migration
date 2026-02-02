from __future__ import annotations

from pathlib import Path
from typing import Optional
from adapters.base import LoadedData

import pandas as pd

def load_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Missing file: {path}")
    df = pd.read_csv(path)
    if df.empty:
        raise ValueError(f"Empty file: {path}")
    return df


def load_all(data_dir: Path) -> LoadedData:
    accounts_path = data_dir / "accounts.csv"
    transactions_path = data_dir / "transactions.csv"
    vendors_path = data_dir / "vendors.csv"

    accounts_df = load_csv(accounts_path)
    transactions_df = load_csv(transactions_path)

    vendors_df: Optional[pd.DataFrame]
    if vendors_path.exists():
        vendors_df = load_csv(vendors_path)
    else:
        vendors_df = None

    return LoadedData(accounts=accounts_df, transactions=transactions_df, vendors=vendors_df)
