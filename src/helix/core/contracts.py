from __future__ import annotations
import pandas as pd
from .types import TS_BIN, CHANNEL, SERIES_KEY, METRIC, VALUE, SCORE

def validate_event_df(df: pd.DataFrame, time_col: str) -> None:
    if time_col not in df.columns:
        raise ValueError(f"Missing time column '{time_col}'. Columns={list(df.columns)}")

def validate_series_df(df: pd.DataFrame) -> None:
    required = [TS_BIN, CHANNEL, SERIES_KEY, METRIC, VALUE]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Series df missing columns {missing}. Columns={list(df.columns)}")

def validate_scored_df(df: pd.DataFrame) -> None:
    validate_series_df(df)
    if SCORE not in df.columns:
        raise ValueError(f"Scored df missing '{SCORE}'. Columns={list(df.columns)}")
