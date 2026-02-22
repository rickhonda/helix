from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Dict, Protocol
import pandas as pd

TS_BIN = "ts_bin"
CHANNEL = "channel"
SERIES_KEY = "series_key"
METRIC = "metric"
VALUE = "value"
SCORE = "score"

@dataclass(frozen=True)
class RunContext:
    run_id: str = "run"
    cache_dir: str = ".helix_cache"

class ScoreOperator(Protocol):
    op_name: str
    def apply(self, series_df: pd.DataFrame, spec: Dict[str, Any]) -> pd.DataFrame: ...
