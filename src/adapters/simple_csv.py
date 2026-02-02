from __future__ import annotations

from pathlib import Path

from .base import LoadedData
from ..loader import load_all


class SimpleCsvAdapter:
    """Adapter for the project's synthetic/simple CSV folder format."""
    def load(self, input_path: Path) -> LoadedData:
        # input_path points to a folder containing the canonical CSVs (e.g., accounts.csv, transactions.csv, vendors.csv).
        return load_all(input_path)
