from __future__ import annotations
from typing import Any, Dict
import pandas as pd
from helix.core.types import RunContext
from helix.core.contracts import validate_event_df, validate_series_df, validate_scored_df
from helix.core.cache import make_key, load_df, save_df
from helix.stages.select_events import select_events
from helix.stages.aggregate import aggregate_base_series
from helix.stages.score import score_series

def run_pipeline(events_df: pd.DataFrame, spec: Dict[str, Any], ctx: RunContext | None = None) -> Dict[str, pd.DataFrame]:
    ctx = ctx or RunContext(cache_dir=spec.get("cache_dir", ".helix_cache"))
    time_col = spec.get("time_col", "ts")
    validate_event_df(events_df, time_col=time_col)

    sel_key = make_key("selected", spec, extra={"n": int(len(events_df))})
    selected = load_df(ctx.cache_dir, sel_key)
    if selected is None:
        selected = select_events(events_df, spec.get("select", {}), time_col=time_col, ctx=ctx)
        save_df(ctx.cache_dir, sel_key, selected)

    base_key = make_key("base", spec, extra={"sel": sel_key})
    base = load_df(ctx.cache_dir, base_key)
    if base is None:
        base = aggregate_base_series(selected, spec.get("base_channels", []), time_col=time_col, ctx=ctx)
        save_df(ctx.cache_dir, base_key, base)
    validate_series_df(base)

    scored_key = make_key("scored", spec, extra={"base": base_key})
    scored = load_df(ctx.cache_dir, scored_key)
    if scored is None:
        scored = score_series(base, spec.get("scoring", []), ctx=ctx)
        save_df(ctx.cache_dir, scored_key, scored)
    validate_scored_df(scored)

    return {"selected": selected, "base": base, "scored": scored}
