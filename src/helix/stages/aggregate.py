from __future__ import annotations
from typing import Any, Dict, List
import pandas as pd
import numpy as np
from helix.core.types import RunContext, TS_BIN, CHANNEL, SERIES_KEY, METRIC, VALUE

_SUPPORTED_AGGS = {"count", "sum", "mean", "min", "max"}

def _series_key(df: pd.DataFrame, group_by: list[str]) -> pd.Series:
    if not group_by:
        return pd.Series(["global"] * len(df), index=df.index)
    return df[group_by].astype(str).agg("|".join, axis=1)

def _apply_channel_filters(df: pd.DataFrame, filters: list[dict[str, Any]]) -> pd.DataFrame:
    out = df
    for f in filters:
        col = f["col"]; op = f.get("op","eq"); val = f.get("value")
        if op == "eq":
            out = out[out[col] == val]
        elif op == "in":
            out = out[out[col].isin(val)]
        else:
            raise ValueError(f"Unsupported channel filter op '{op}'")
    return out

def aggregate_base_series(events_df: pd.DataFrame, base_channels: List[Dict[str, Any]], time_col: str, ctx: RunContext) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    df = events_df.copy()
    df[time_col] = pd.to_datetime(df[time_col], utc=True, errors="coerce")
    df = df.dropna(subset=[time_col])

    for ch in base_channels:
        name = ch["name"]
        window = ch.get("window", "5min")
        hop = ch.get("hop")
        bin_freq = hop or window

        group_by = ch.get("group_by", [])
        measure = ch.get("measure")
        aggs = ch.get("aggs", ["count"])
        for a in aggs:
            if a not in _SUPPORTED_AGGS:
                raise ValueError(f"Unsupported agg '{a}' for channel '{name}'. Supported={sorted(_SUPPORTED_AGGS)}")

        tmp = _apply_channel_filters(df, ch.get("filters", []))
        tmp[TS_BIN] = tmp[time_col].dt.floor(bin_freq)
        tmp[SERIES_KEY] = _series_key(tmp, group_by)

        if measure is None:
            g = tmp.groupby([TS_BIN, SERIES_KEY], dropna=False).size().rename("count").reset_index()
            if "count" in aggs:
                for _, r in g.iterrows():
                    rows.append({TS_BIN:r[TS_BIN], CHANNEL:name, SERIES_KEY:r[SERIES_KEY], METRIC:"count", VALUE:float(r["count"])})
        else:
            if measure not in tmp.columns:
                raise ValueError(f"Measure '{measure}' not found for channel '{name}'. Columns={list(tmp.columns)}")
            series = tmp.groupby([TS_BIN, SERIES_KEY], dropna=False)[measure]
            out = series.agg(aggs).reset_index()
            for a in aggs:
                for _, r in out.iterrows():
                    rows.append({TS_BIN:r[TS_BIN], CHANNEL:name, SERIES_KEY:r[SERIES_KEY], METRIC:a, VALUE: float(r[a]) if pd.notna(r[a]) else np.nan})

    if not rows:
        return pd.DataFrame(columns=[TS_BIN, CHANNEL, SERIES_KEY, METRIC, VALUE])
    res = pd.DataFrame(rows)
    res[TS_BIN] = pd.to_datetime(res[TS_BIN], utc=True)
    return res.sort_values([CHANNEL, SERIES_KEY, METRIC, TS_BIN]).reset_index(drop=True)

def aggregate_minbin(events_df: pd.DataFrame, base_channels: List[Dict[str, Any]], time_col: str, hop_min: str, ctx: RunContext) -> pd.DataFrame:
    """Compute min-hop bins once. For mean, emits sum+count+mean."""
    rows: list[dict[str, Any]] = []
    df = events_df.copy()
    df[time_col] = pd.to_datetime(df[time_col], utc=True, errors="coerce")
    df = df.dropna(subset=[time_col])

    for ch in base_channels:
        name = ch["name"]
        group_by = ch.get("group_by", [])
        measure = ch.get("measure")
        aggs = set(ch.get("aggs", ["count"]))
        if not aggs.issubset(_SUPPORTED_AGGS):
            bad = sorted(list(aggs - _SUPPORTED_AGGS))
            raise ValueError(f"Unsupported aggs {bad} for channel '{name}'.")
        tmp = _apply_channel_filters(df, ch.get("filters", []))
        tmp[TS_BIN] = tmp[time_col].dt.floor(hop_min)
        tmp[SERIES_KEY] = _series_key(tmp, group_by)

        if measure is None:
            g = tmp.groupby([TS_BIN, SERIES_KEY], dropna=False).size().rename("count").reset_index()
            for _, r in g.iterrows():
                rows.append({TS_BIN:r[TS_BIN], CHANNEL:name, SERIES_KEY:r[SERIES_KEY], METRIC:"count", VALUE:float(r["count"])})
        else:
            if measure not in tmp.columns:
                raise ValueError(f"Measure '{measure}' not found for channel '{name}'. Columns={list(tmp.columns)}")
            s = tmp.groupby([TS_BIN, SERIES_KEY], dropna=False)[measure]
            need_mean = "mean" in aggs
            need_sum = "sum" in aggs or need_mean
            need_count = "count" in aggs or need_mean

            if need_sum:
                out_sum = s.sum().rename("sum").reset_index()
                for _, r in out_sum.iterrows():
                    rows.append({TS_BIN:r[TS_BIN], CHANNEL:name, SERIES_KEY:r[SERIES_KEY], METRIC:"sum", VALUE:float(r["sum"])})
            if need_count:
                out_cnt = s.count().rename("count").reset_index()
                for _, r in out_cnt.iterrows():
                    rows.append({TS_BIN:r[TS_BIN], CHANNEL:name, SERIES_KEY:r[SERIES_KEY], METRIC:"count", VALUE:float(r["count"])})
            if "mean" in aggs:
                out_mean = s.mean().rename("mean").reset_index()
                for _, r in out_mean.iterrows():
                    rows.append({TS_BIN:r[TS_BIN], CHANNEL:name, SERIES_KEY:r[SERIES_KEY], METRIC:"mean", VALUE:float(r["mean"])})
            if "min" in aggs:
                out_min = s.min().rename("min").reset_index()
                for _, r in out_min.iterrows():
                    rows.append({TS_BIN:r[TS_BIN], CHANNEL:name, SERIES_KEY:r[SERIES_KEY], METRIC:"min", VALUE:float(r["min"])})
            if "max" in aggs:
                out_max = s.max().rename("max").reset_index()
                for _, r in out_max.iterrows():
                    rows.append({TS_BIN:r[TS_BIN], CHANNEL:name, SERIES_KEY:r[SERIES_KEY], METRIC:"max", VALUE:float(r["max"])})

    if not rows:
        return pd.DataFrame(columns=[TS_BIN, CHANNEL, SERIES_KEY, METRIC, VALUE])
    res = pd.DataFrame(rows)
    res[TS_BIN] = pd.to_datetime(res[TS_BIN], utc=True)
    return res.sort_values([CHANNEL, SERIES_KEY, METRIC, TS_BIN]).reset_index(drop=True)

def coarsen_from_minbin(minbin_series: pd.DataFrame, window: str) -> pd.DataFrame:
    """Coarsen min-hop series to window bins (hop==window) for sum/count/mean."""
    df = minbin_series.copy()
    df[TS_BIN] = pd.to_datetime(df[TS_BIN], utc=True)
    df["ts_coarse"] = df[TS_BIN].dt.floor(window)

    parts = []

    for metric in ["sum", "count"]:
        d = df[df[METRIC] == metric]
        if len(d):
            g = d.groupby(["ts_coarse", CHANNEL, SERIES_KEY, METRIC], dropna=False)[VALUE].sum().reset_index()
            g = g.rename(columns={"ts_coarse": TS_BIN})
            parts.append(g[[TS_BIN, CHANNEL, SERIES_KEY, METRIC, VALUE]])

    sum_df = None
    cnt_df = None
    for p in parts:
        if (p[METRIC] == "sum").any():
            sum_df = p[p[METRIC] == "sum"].copy()
        if (p[METRIC] == "count").any():
            cnt_df = p[p[METRIC] == "count"].copy()

    if sum_df is not None and cnt_df is not None:
        m = sum_df.merge(cnt_df, on=[TS_BIN, CHANNEL, SERIES_KEY], how="outer", suffixes=("_sum","_cnt"))
        # value_sum/value_cnt columns exist
        v_sum = m[f"{VALUE}_sum"]
        v_cnt = m[f"{VALUE}_cnt"]
        mean_out = m[[TS_BIN, CHANNEL, SERIES_KEY]].copy()
        mean_out[METRIC] = "mean"
        mean_out[VALUE] = (v_sum / v_cnt).astype(float)
        parts.append(mean_out[[TS_BIN, CHANNEL, SERIES_KEY, METRIC, VALUE]])
    else:
        d = df[df[METRIC] == "mean"]
        if len(d):
            g = d.groupby(["ts_coarse", CHANNEL, SERIES_KEY], dropna=False)[VALUE].mean().reset_index()
            g = g.rename(columns={"ts_coarse": TS_BIN})
            g[METRIC] = "mean"
            parts.append(g[[TS_BIN, CHANNEL, SERIES_KEY, METRIC, VALUE]])

    out = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame(columns=[TS_BIN, CHANNEL, SERIES_KEY, METRIC, VALUE])
    out[TS_BIN] = pd.to_datetime(out[TS_BIN], utc=True)
    return out.sort_values([CHANNEL, SERIES_KEY, METRIC, TS_BIN]).reset_index(drop=True)
