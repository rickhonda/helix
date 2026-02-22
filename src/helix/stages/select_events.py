from __future__ import annotations
from typing import Any, Dict
import pandas as pd
from helix.core.types import RunContext

def select_events(events_df: pd.DataFrame, select_spec: Dict[str, Any], time_col: str, ctx: RunContext) -> pd.DataFrame:
    df = events_df.copy()
    df[time_col] = pd.to_datetime(df[time_col], utc=True, errors="coerce")
    df = df.dropna(subset=[time_col])
    for f in select_spec.get("filters", []):
        col = f["col"]; op = f.get("op","eq"); val = f.get("value")
        if op == "eq":
            df = df[df[col] == val]
        elif op == "in":
            df = df[df[col].isin(val)]
        elif op == "neq":
            df = df[df[col] != val]
        else:
            raise ValueError(f"Unsupported select filter op '{op}'")
    return df
