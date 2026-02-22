from __future__ import annotations
from typing import Any, Dict
import numpy as np
import pandas as pd
from helix.core.types import TS_BIN, CHANNEL, SERIES_KEY, METRIC, VALUE, SCORE
from helix.operators.score.base import ScoreOperator

class EwmaResidualZOperator(ScoreOperator):
    op_name = "ewma_residual_z"

    def apply(self, series_df: pd.DataFrame, spec: Dict[str, Any]) -> pd.DataFrame:
        if series_df is None or len(series_df) == 0:
            out = series_df.copy()
            if "baseline" not in out.columns:
                out["baseline"] = np.nan
            if "residual" not in out.columns:
                out["residual"] = np.nan
            if SCORE not in out.columns:
                out[SCORE] = np.nan
            return out

        alpha = float(spec.get("alpha", 0.05))
        warmup = int(spec.get("warmup", 0))
        eps = float(spec.get("eps", 1e-9))

        out = series_df.copy().sort_values([CHANNEL, SERIES_KEY, METRIC, TS_BIN])

        def _ewma(g: pd.DataFrame) -> pd.DataFrame:
            x = g[VALUE].astype(float).to_numpy()
            baseline = np.full_like(x, np.nan, dtype=float)
            prev = np.nan
            for i, xi in enumerate(x):
                if np.isnan(xi):
                    baseline[i] = prev
                    continue
                prev = xi if np.isnan(prev) else (1 - alpha) * prev + alpha * xi
                baseline[i] = prev

            resid = x - baseline
            rr = resid[warmup:] if warmup < len(resid) else resid
            med = np.nanmedian(rr) if len(rr) else np.nanmedian(resid)
            mad = np.nanmedian(np.abs(rr - med)) if len(rr) else np.nanmedian(np.abs(resid - med))
            denom = (mad * 1.4826) + eps
            z = (resid - med) / denom

            g["baseline"] = baseline
            g["residual"] = resid
            g[SCORE] = z
            return g

        return out.groupby([CHANNEL, SERIES_KEY, METRIC], group_keys=False).apply(_ewma)
