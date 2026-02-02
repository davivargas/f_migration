from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional

import pandas as pd

from .base import LoadedData


@dataclass(frozen=True)
class KaggleCleanStats:
    rows_in: int
    rows_out: int
    dropped_bad_date: int
    dropped_bad_amount: int


class KaggleFinancialAccountingAdapter:

    def __init__(self,
                 default_currency: str = "USD",
                 drop_bad_rows: bool = True) -> None:
        self._default_currency = default_currency
        self._drop_bad_rows = drop_bad_rows

    def load(self, input_path: Path) -> LoadedData:
        raw = pd.read_csv(input_path)
        cleaned, _stats = self._clean(raw)

        accounts, account_id_map = self._build_accounts(cleaned)
        transactions = self._build_transactions(cleaned, account_id_map)
        vendors = self._build_vendors(cleaned)

        return LoadedData(accounts=accounts, transactions=transactions, vendors=vendors)

    def _clean(self, df: pd.DataFrame) -> tuple[pd.DataFrame, KaggleCleanStats]:
        rows_in = int(df.shape[0])

        df = df.copy()
        df.columns = [c.strip() for c in df.columns]

        required = {
            "Date", "Account", "Debit", "Category", "Transaction_Type",
            "Description", "Customer_Vendor"
        }
        missing = required.difference(df.columns)
        if missing:
            raise ValueError(f"Missing required columns: {sorted(missing)}")

        for col in ["Account", "Description", "Category", "Transaction_Type",
                    "Customer_Vendor", "Payment_Method"]:
            if col in df.columns:
                df[col] = df[col].astype(str).str.strip()

        parsed = pd.to_datetime(df["Date"], errors="coerce")
        bad_date = int(parsed.isna().sum())
        df["date_iso"] = parsed.dt.date.astype("string")

        debit = pd.to_numeric(df["Debit"], errors="coerce")
        bad_amount = int(debit.isna().sum())
        df["debit_num"] = debit

        dropped_bad_date = 0
        dropped_bad_amount = 0
        if self._drop_bad_rows:
            before = df.shape[0]
            df = df[df["date_iso"].notna()]
            dropped_bad_date = int(before - df.shape[0])

            before = df.shape[0]
            df = df[df["debit_num"].notna()]
            dropped_bad_amount = int(before - df.shape[0])

        stats = KaggleCleanStats(
            rows_in=rows_in,
            rows_out=int(df.shape[0]),
            dropped_bad_date=dropped_bad_date,
            dropped_bad_amount=dropped_bad_amount,
        )
        return df, stats

    def _build_accounts(self, df: pd.DataFrame) -> tuple[pd.DataFrame, Dict[str, int]]:
        unique_accounts = sorted(df["Account"].dropna().unique().tolist())
        account_id_map: Dict[str, int] = {name: 1000 + i for i, name in enumerate(unique_accounts, start=1)}

        def map_type(cat: str) -> str:
            c = str(cat).strip().lower()
            if c in {"asset", "liability", "revenue", "expense", "equity"}:
                return c
            if "asset" in c:
                return "asset"
            if "liabil" in c:
                return "liability"
            if "reven" in c or "income" in c:
                return "revenue"
            if "expens" in c or "cost" in c:
                return "expense"
            return "unknown"

        cat_mode = (
            df.groupby("Account")["Category"]
              .agg(lambda s: s.value_counts().index[0] if not s.empty else "unknown")
        )

        accounts = pd.DataFrame({
            "account_id": [account_id_map[a] for a in unique_accounts],
            "account_name": unique_accounts,
            "type": [map_type(cat_mode.get(a, "unknown")) for a in unique_accounts],
            "currency": [self._default_currency for _ in unique_accounts],
        })

        return accounts, account_id_map

    def _build_transactions(self, df: pd.DataFrame, account_id_map: Dict[str, int]) -> pd.DataFrame:
        tx = pd.DataFrame()

        tx["transaction_id"] = [f"kfa_{i:06d}" for i in range(1, df.shape[0] + 1)]
        tx["account_id"] = df["Account"].map(account_id_map).astype("Int64")

        def signed_amount(row) -> float:
            t = str(row["Transaction_Type"]).lower()
            val = float(row["debit_num"])
            if t in {"purchase", "expense"}:
                return -val
            return val

        tx["amount"] = df.apply(signed_amount, axis=1).round(2)
        tx["currency"] = self._default_currency
        tx["date"] = df["date_iso"].astype(str)

        desc = df["Description"].fillna("").astype(str)
        if "Reference" in df.columns:
            tx["description"] = desc + " | ref=" + df["Reference"].astype(str)
        else:
            tx["description"] = desc

        return tx

    def _build_vendors(self, df: pd.DataFrame) -> Optional[pd.DataFrame]:
        if "Customer_Vendor" not in df.columns:
            return None

        v = df["Customer_Vendor"].dropna().astype(str).str.strip()
        v = v[v != ""]
        if v.empty:
            return None

        unique_v = sorted(v.unique().tolist())
        vendors = pd.DataFrame({
            "vendor_id": [f"v{i:05d}" for i in range(1, len(unique_v) + 1)],
            "name": unique_v,
            "country": "" 
        })
        return vendors
