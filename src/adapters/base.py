from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Protocol

import pandas as pd


@dataclass(frozen=True)
class LoadedData:
    """
    Immutable container holding data after it has been loaded and normalized
    into the canonical internal schema.
    """
    accounts: pd.DataFrame
    transactions: pd.DataFrame
    vendors: Optional[pd.DataFrame]


class Adapter(Protocol):
    """
    Interface for dataset-specific loaders that map raw input formats
    into the canonical LoadedData structure.
    """

    def load(self, input_path: Path) -> LoadedData:
        """
        Load a dataset from input_path and return canonical tables
        ready for validation.
        """
        ...
