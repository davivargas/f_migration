from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Protocol

import pandas as pd


@dataclass(frozen=True)
class LoadedData:
    accounts: pd.DataFrame
    transactions: pd.DataFrame
    vendors: Optional[pd.DataFrame]


class Adapter(Protocol):
    def load(self, input_path: Path) -> LoadedData:
        ...
