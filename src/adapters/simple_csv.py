from __future__ import annotations

from pathlib import Path

from .base import LoadedData
from ..loader import load_all


class SimpleCsvAdapter:
    def load(self, input_path: Path) -> LoadedData:
        # input_path is a folder: ./data
        return load_all(input_path)
