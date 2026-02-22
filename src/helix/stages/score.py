from __future__ import annotations
from typing import Any, Dict, List
import numpy as np
import pandas as pd
from helix.core.types import RunContext, SCORE
from helix.operators.score.registry import get_score_operator

def score_series(series_df: pd.DataFrame, scoring_specs: List[Dict[str, Any]], ctx: RunContext) -> pd.DataFrame:
    out = series_df.copy()
    if SCORE not in out.columns:
        out[SCORE] = np.nan
    if out is None or len(out) == 0:
        return out
    for spec in scoring_specs:
        op = get_score_operator(spec["op"])
        out = op.apply(out, spec)
        if out is None:
            out = series_df.copy()
        if SCORE not in out.columns:
            out[SCORE] = np.nan
    return out
