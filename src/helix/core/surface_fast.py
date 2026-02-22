from __future__ import annotations
from typing import Any, Dict, List
import pandas as pd
from helix.core.types import RunContext, TS_BIN, CHANNEL, SERIES_KEY, METRIC, SCORE
from helix.core.cache import make_key, load_df, save_df
from helix.stages.select_events import select_events
from helix.stages.aggregate import aggregate_minbin, coarsen_from_minbin
from helix.stages.score import score_series

def build_surface(events_df: pd.DataFrame, spec: Dict[str, Any], ctx: RunContext | None = None) -> pd.DataFrame:
    ctx = ctx or RunContext(cache_dir=spec.get("cache_dir", ".helix_cache"))
    time_col = spec.get("time_col", "ts")

    surf = spec.get("surface", {})
    windows: List[str] = list(surf.get("windows", []))
    if not windows:
        raise ValueError("spec.surface.windows must be a non-empty list.")
    hop_min = surf.get("hop_min")
    if not hop_min:
        raise ValueError("spec.surface.hop_min is required for fast surface (e.g., '30s').")

    channel_filter = surf.get("channel")
    metric_filter = surf.get("metric")

    sel_key = make_key("selected", spec, extra={"n": int(len(events_df))})
    selected = load_df(ctx.cache_dir, sel_key)
    if selected is None:
        selected = select_events(events_df, spec.get("select", {}), time_col=time_col, ctx=ctx)
        save_df(ctx.cache_dir, sel_key, selected)

    base_channels = spec.get("base_channels", [])

    min_key = make_key("minbin", spec, extra={"sel": sel_key, "hop_min": hop_min})
    minbin = load_df(ctx.cache_dir, min_key)
    if minbin is None:
        minbin = aggregate_minbin(selected, base_channels, time_col=time_col, hop_min=hop_min, ctx=ctx)
        save_df(ctx.cache_dir, min_key, minbin)

    parts = []
    for w in windows:
        win_key = make_key("surface_win", spec, extra={"min": min_key, "window": w})
        cached = load_df(ctx.cache_dir, win_key)
        if cached is None:
            coarse = coarsen_from_minbin(minbin, window=w)
            scored = score_series(coarse, spec.get("scoring", []), ctx=ctx).copy()
            scored["window"] = w

            # enforce schema
            if CHANNEL not in scored.columns:
                scored[CHANNEL] = channel_filter if channel_filter is not None else "unknown"
            if SERIES_KEY not in scored.columns:
                scored[SERIES_KEY] = "global"
            if METRIC not in scored.columns:
                scored[METRIC] = metric_filter if metric_filter is not None else "value"
            if SCORE not in scored.columns:
                scored[SCORE] = pd.NA
            if TS_BIN not in scored.columns:
                raise ValueError(f"Scored output missing '{TS_BIN}'. Columns={list(scored.columns)}")

            if channel_filter is not None:
                scored = scored[scored[CHANNEL] == channel_filter]
            if metric_filter is not None:
                scored = scored[scored[METRIC] == metric_filter]

            scored = scored[[TS_BIN, "window", CHANNEL, SERIES_KEY, METRIC, SCORE]]
            save_df(ctx.cache_dir, win_key, scored)
            cached = scored

        parts.append(cached)

    out = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame(columns=[TS_BIN, "window", CHANNEL, SERIES_KEY, METRIC, SCORE])
    out[TS_BIN] = pd.to_datetime(out[TS_BIN], utc=True, errors="coerce")
    return out.sort_values(["window", CHANNEL, SERIES_KEY, METRIC, TS_BIN]).reset_index(drop=True)
