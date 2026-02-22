from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Dict, List, Tuple
import pandas as pd
import numpy as np
from helix.core.types import TS_BIN, CHANNEL, SERIES_KEY, METRIC, SCORE

@dataclass
class DetectionResult:
    detection: Dict[str, Any]
    summary: pd.DataFrame
    surface_slice: pd.DataFrame

def _find_runs(mask: np.ndarray) -> List[Tuple[int,int]]:
    runs = []
    start = None
    for i, v in enumerate(mask):
        if v and start is None:
            start = i
        if (not v) and start is not None:
            runs.append((start, i-1))
            start = None
    if start is not None:
        runs.append((start, len(mask)-1))
    return runs

def extract_detection_from_surface(
    surface: pd.DataFrame,
    threshold: float = 3.0,
    min_bins: int = 5,
    persistence: float = 0.6,
    prefer_smallest_window: bool = True,
) -> DetectionResult:
    df = surface.copy()

    required = [TS_BIN, "window", CHANNEL, SERIES_KEY, METRIC, SCORE]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(
            "Surface is missing required columns for detection extraction: "
            + ", ".join(missing)
            + f"\nGot columns: {list(df.columns)}"
            + "\nFix: delete .helix_cache and rerun `helix surface`."
        )

    df[TS_BIN] = pd.to_datetime(df[TS_BIN], utc=True, errors="coerce")
    df = df.dropna(subset=[TS_BIN, SCORE])
    if len(df) == 0:
        raise ValueError("Surface has no rows after cleaning (no ts_bin/score).")

    ch = df[CHANNEL].iloc[0]
    m = df[METRIC].iloc[0]

    best_key = None
    best_p = -1.0
    best_pivot = None

    for key, g in df.groupby(SERIES_KEY):
        piv = g.pivot_table(index=TS_BIN, columns="window", values=SCORE, aggfunc="mean").sort_index()
        if piv.shape[1] < 2:
            continue
        frac = (piv >= threshold).mean(axis=1)
        mx = float(frac.max()) if len(frac) else 0.0
        if mx > best_p:
            best_p = mx
            best_key = key
            best_pivot = piv

    if best_key is None or best_pivot is None:
        best_key = df[SERIES_KEY].iloc[0]
        best_pivot = df[df[SERIES_KEY] == best_key].pivot_table(index=TS_BIN, columns="window", values=SCORE, aggfunc="mean").sort_index()

    piv = best_pivot
    frac = (piv >= threshold).mean(axis=1)
    mask = (frac >= persistence).to_numpy()
    runs = _find_runs(mask)
    runs = [r for r in runs if (r[1]-r[0]+1) >= min_bins]

    times = piv.index.to_numpy()
    ranges = []
    for a,b in runs:
        ranges.append({"start": str(times[a]), "end": str(times[b]), "bins": int(b-a+1), "max_persistence": float(frac.iloc[a:b+1].max())})

    # choose best window by avg score within ranges (or overall)
    if ranges:
        idx = np.zeros(len(piv.index), dtype=bool)
        for a,b in runs:
            idx[a:b+1] = True
        slice_df = piv[idx]
    else:
        slice_df = piv

    window_scores = {}
    for w in piv.columns:
        s = slice_df[w].astype(float)
        window_scores[w] = float(np.nanmean(s.values)) if len(s) else float("nan")

    def _window_seconds(w: str) -> float:
        w = str(w).strip().lower()
        if w.endswith("ms"):
            return float(w[:-2]) / 1000.0
        if w.endswith("s"):
            return float(w[:-1])
        if w.endswith("min"):
            return float(w[:-3]) * 60.0
        if w.endswith("h"):
            return float(w[:-1]) * 3600.0
        return float("inf")

    best_window = None
    best_val = -1e18
    for w, val in window_scores.items():
        if np.isnan(val):
            continue
        if val > best_val:
            best_val = val
            best_window = w
        elif val == best_val and best_window is not None and prefer_smallest_window:
            if _window_seconds(w) < _window_seconds(best_window):
                best_window = w

    if best_window is None:
        best_window = str(piv.columns[0])

    detection = {
        "channel": ch,
        "metric": m,
        "series_key": best_key,
        "window": best_window,
        "hop": best_window,
        "threshold": float(threshold),
        "min_bins": int(min_bins),
        "persistence": float(persistence),
        "time_ranges": ranges,
        "notes": "Generated from surface persistence across windows.",
    }

    summary = pd.DataFrame([{
        "series_key": best_key,
        "best_window": best_window,
        "threshold": threshold,
        "min_bins": min_bins,
        "persistence": persistence,
        "num_ranges": len(ranges),
        "max_persistence": float(frac.max()),
    }])

    surface_slice = df[(df[SERIES_KEY]==best_key) & (df["window"]==best_window)].sort_values(TS_BIN)
    return DetectionResult(detection=detection, summary=summary, surface_slice=surface_slice)
