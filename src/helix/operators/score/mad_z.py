from __future__ import annotations
from typing import Any, Dict
import numpy as np
import pandas as pd
from helix.core.types import TS_BIN, CHANNEL, SERIES_KEY, METRIC, VALUE, SCORE
from helix.operators.score.base import ScoreOperator

class MadZOperator(ScoreOperator):
    op_name = "mad_z"

    def apply(self, series_df: pd.DataFrame, spec: Dict[str, Any]) -> pd.DataFrame:
        if series_df is None or len(series_df) == 0:
            out = series_df.copy()
            if SCORE not in out.columns:
                out[SCORE] = np.nan
            return out

        eps = float(spec.get("eps", 1e-9))
        out = series_df.copy().sort_values([CHANNEL, SERIES_KEY, METRIC, TS_BIN])

        def _mad(g: pd.DataFrame) -> pd.DataFrame:
            x = g[VALUE].astype(float).to_numpy()
            med = np.nanmedian(x)
            mad = np.nanmedian(np.abs(x - med))
            denom = (mad * 1.4826) + eps
            g[SCORE] = (x - med) / denom
            return g

        return out.groupby([CHANNEL, SERIES_KEY, METRIC], group_keys=False).apply(_mad)
