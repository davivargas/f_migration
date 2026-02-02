from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional, Tuple

import pandas as pd

from .base import LoadedData


@dataclass(frozen=True)
class GovCleanStats:
    rows_in: int
    rows_out: int
    bad_date: int
    bad_amount: int
    bad_cd_code: int
    missing_dept_or_gl: int


class GovCanadaGLAdapter:

    _COL_VOUCHER = "Journal-Voucher-Identifier-Identificateur-de-la-pièce-de-journal"
    _COL_ITEM = "Journal-Voucher-Item-Identifier-Identificateur-de-l'item-de-la-pièce-de-journal"
    _COL_DATE = "Accounting-Effective-Date-Date-d'entrée-en-vigueur-comptable"
    _COL_DEPT = "DepartmentNumber-Numéro-de-Ministère"
    _COL_GL = "General-Ledger-Account-Code-Code-du-compte-du-grand-livre-général"
    _COL_CD = "Credit/Debit-Code-Code-Crédit/Débit"
    _COL_AMOUNT = "Journal-Voucher-Item-Amount-Montant-de-l'item-de-la-pièce-de-journal"
    _COL_CTRL_NUM = "Accounting-Control-Number-Numéro-contrôle-comptable"
    _COL_FY = "Fiscal-Year-Année-Fiscale"
    _COL_FM = "Fiscal-Month-Mois-Fiscal"

    def __init__(self,
                 currency: str = "CAD",
                 drop_bad_rows: bool = True,
                 bucket_missing_accounts: bool = True) -> None:
        self._currency = currency
        self._drop_bad_rows = drop_bad_rows
        self._bucket_missing_accounts = bucket_missing_accounts

    def load(self, input_path: Path) -> LoadedData:
        raw = pd.read_csv(input_path)
        cleaned, _stats = self._clean(raw)

        accounts, account_id_map = self._build_accounts(cleaned)
        transactions = self._build_transactions(cleaned, account_id_map)

        return LoadedData(
            accounts=accounts,
            transactions=transactions,
            vendors=None
        )

    def _require_columns(self, df: pd.DataFrame) -> None:
        required = {
            self._COL_VOUCHER,
            self._COL_ITEM,
            self._COL_DATE,
            self._COL_DEPT,
            self._COL_GL,
            self._COL_CD,
            self._COL_AMOUNT
        }
        missing = required.difference(df.columns)
        if missing:
            raise ValueError(f"Missing required columns: {sorted(missing)}")

    def _clean(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, GovCleanStats]:
        rows_in = int(df.shape[0])

        df = df.copy()
        df.columns = [c.strip() for c in df.columns]
        self._require_columns(df)

        for col in [self._COL_VOUCHER, self._COL_ITEM, self._COL_DEPT, self._COL_GL, self._COL_CD]:
            df[col] = df[col].astype("string").str.strip()

        missing_dept_or_gl = int(
            df[self._COL_DEPT].isna().sum()
            + (df[self._COL_DEPT] == "").sum()
            + df[self._COL_GL].isna().sum()
            + (df[self._COL_GL] == "").sum()
        )

        if self._bucket_missing_accounts:
            df[self._COL_DEPT] = df[self._COL_DEPT].fillna("UNKNOWN").replace("", "UNKNOWN")
            df[self._COL_GL] = df[self._COL_GL].fillna("UNKNOWN").replace("", "UNKNOWN")
        else:
            df = df[df[self._COL_DEPT].notna() & df[self._COL_GL].notna()]
            df = df[(df[self._COL_DEPT] != "") & (df[self._COL_GL] != "")]

        parsed = pd.to_datetime(df[self._COL_DATE], errors="coerce")
        bad_date = int(parsed.isna().sum())
        df["date_iso"] = parsed.dt.date.astype("string")

        cd = df[self._COL_CD].astype("string").str.upper().str.strip()
        cd = cd.replace({
            "CR": "C",
            "CREDIT": "C",
            "CRED": "C",
            "CRED.": "C",
            "DR": "D",
            "DEBIT": "D",
            "DEB": "D",
            "DEB.": "D",
        })

        df["cd_norm"] = cd

        valid_cd_mask = df["cd_norm"].isin(["C", "D"])
        bad_cd_code = int((~valid_cd_mask).sum())

        amt_str = df[self._COL_AMOUNT].astype("string").str.strip()

        amt_str = amt_str.str.replace(" ", "", regex=False)

        both_mask = amt_str.str.contains(",", na=False) & amt_str.str.contains(r"\.", na=False)
        amt_str.loc[both_mask] = amt_str.loc[both_mask].str.replace(",", "", regex=False)

        comma_only_mask = amt_str.str.contains(",", na=False) & ~amt_str.str.contains(r"\.", na=False)
        amt_str.loc[comma_only_mask] = amt_str.loc[comma_only_mask].str.replace(",", ".", regex=False)

        amt_str = amt_str.str.replace(",", "", regex=False)

        amt_num = pd.to_numeric(amt_str, errors="coerce")
        bad_amount = int(amt_num.isna().sum())

        amount = pd.Series(pd.NA, index=df.index, dtype="Float64")
        amount[valid_cd_mask & (df["cd_norm"] == "C")] = amt_num[valid_cd_mask & (df["cd_norm"] == "C")]
        amount[valid_cd_mask & (df["cd_norm"] == "D")] = -amt_num[valid_cd_mask & (df["cd_norm"] == "D")]

        df["amount"] = amount

        if self._drop_bad_rows:
            df = df[df["date_iso"].notna()]
            df = df[df["amount"].notna()]

        rows_out = int(df.shape[0])

        stats = GovCleanStats(
            rows_in=rows_in,
            rows_out=rows_out,
            bad_date=bad_date,
            bad_amount=bad_amount,
            bad_cd_code=bad_cd_code,
            missing_dept_or_gl=missing_dept_or_gl
        )

        return df, stats

    def _build_accounts(self, df: pd.DataFrame) -> tuple[pd.DataFrame, Dict[str, int]]:
        df = df.copy()

        df["account_key"] = df[self._COL_DEPT] + "-" + df[self._COL_GL]

        unique_accounts = sorted(df["account_key"].dropna().unique().tolist())

        account_id_map: Dict[str, int] = {
            k: 1000 + i for i, k in enumerate(unique_accounts, start=1)
        }

        accounts = pd.DataFrame({
            "account_id": [account_id_map[k] for k in unique_accounts],
            "account_name": [f"Dept/GL {k}" for k in unique_accounts],
            "type": ["unknown"] * len(unique_accounts),
            "currency": [self._currency] * len(unique_accounts),
        })

        return accounts, account_id_map

    def _build_transactions(self, df: pd.DataFrame, account_id_map: Dict[str, int]) -> pd.DataFrame:
        df = df.copy()
        df["account_key"] = df[self._COL_DEPT] + "-" + df[self._COL_GL]

        tx = pd.DataFrame()

        voucher = df[self._COL_VOUCHER].astype("string").fillna("").str.strip()
        item = df[self._COL_ITEM].astype("string").fillna("").str.strip()

        base_id = "gov_" + voucher + "_" + item
        missing_id = (voucher == "") | (item == "")

        # IMPORTANT: assign only to the subset
        base_id.loc[missing_id] = "gov_row_" + df.index[missing_id].astype(str)

        tx["transaction_id"] = base_id
        tx["account_id"] = df["account_key"].map(account_id_map).astype("Int64")
        tx["amount"] = df["amount"].astype(float).round(2)
        tx["currency"] = self._currency
        tx["date"] = df["date_iso"].astype(str)

        # Clean description (no "nan")
        desc = "JV " + voucher + " | Item " + item

        if self._COL_CTRL_NUM in df.columns:
            ctrl = df[self._COL_CTRL_NUM].astype("string").fillna("").str.strip()
            desc = desc + " | Ctrl " + ctrl

        if self._COL_FY in df.columns:
            fy = df[self._COL_FY].astype("string").fillna("").str.strip()
            desc = desc + " | FY " + fy

        if self._COL_FM in df.columns:
            fm = df[self._COL_FM].astype("string").fillna("").str.strip()
            desc = desc + " | FM " + fm

        tx["description"] = desc
        return tx
