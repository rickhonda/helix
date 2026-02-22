from __future__ import annotations
import os
import sys
import yaml
import click

from helix.specs.load import load_spec
from helix.specs.validate import validate_spec
from helix.io.read import read_events_csv
from helix.io.write import ensure_dir, write_csv
from helix.core.pipeline import run_pipeline
from helix.core.surface_fast import build_surface
from helix.core.detect import extract_detection_from_surface
from helix.core.types import RunContext

def _diag():
    import helix
    click.echo("=== HELIX DIAGNOSTICS ===", err=True)
    click.echo(f"sys.executable: {sys.executable}", err=True)
    click.echo(f"sys.version:    {sys.version.splitlines()[0]}", err=True)
    click.echo(f"VIRTUAL_ENV:    {os.environ.get('VIRTUAL_ENV')}", err=True)
    click.echo(f"helix.__file__: {helix.__file__}", err=True)
    click.echo("=========================", err=True)

@click.group()
def cli():
    """Helix v0.5 demo CLI."""
    pass

@cli.command()
def doctor():
    _diag()
    click.echo("doctor: ok")

@cli.command()
@click.option("--spec", "spec_path", required=True, type=click.Path(exists=True, dir_okay=False))
@click.option("--events", "events_path", required=True, type=click.Path(exists=True, dir_okay=False))
@click.option("--out", "out_dir", default="outputs", type=click.Path(file_okay=False))
@click.option("--diag", is_flag=True)
def run(spec_path: str, events_path: str, out_dir: str, diag: bool):
    if diag or os.environ.get("HELIX_DIAG","").strip().lower() in {"1","true","yes","on"}:
        _diag()
    spec = load_spec(spec_path)
    validate_spec(spec)
    events = read_events_csv(events_path)
    ctx = RunContext(cache_dir=spec.get("cache_dir", ".helix_cache"))
    out = run_pipeline(events, spec, ctx=ctx)
    outp = ensure_dir(out_dir)
    write_csv(out["base"], outp / "base.csv")
    write_csv(out["scored"], outp / "scored.csv")
    click.echo(f"Wrote: {outp/'base.csv'}")
    click.echo(f"Wrote: {outp/'scored.csv'}")

@cli.command()
@click.option("--spec", "spec_path", required=True, type=click.Path(exists=True, dir_okay=False))
@click.option("--events", "events_path", required=True, type=click.Path(exists=True, dir_okay=False))
@click.option("--out", "out_dir", default="outputs_surface", type=click.Path(file_okay=False))
def surface(spec_path: str, events_path: str, out_dir: str):
    spec = load_spec(spec_path)
    validate_spec(spec)
    events = read_events_csv(events_path)
    ctx = RunContext(cache_dir=spec.get("cache_dir", ".helix_cache"))
    surf = build_surface(events, spec, ctx=ctx)
    outp = ensure_dir(out_dir)
    write_csv(surf, outp / "surface.csv")
    click.echo(f"Wrote: {outp/'surface.csv'}")

@cli.command()
@click.option("--spec", "spec_path", required=True, type=click.Path(exists=True, dir_okay=False))
@click.option("--events", "events_path", required=True, type=click.Path(exists=True, dir_okay=False))
@click.option("--out", "out_dir", default="detections", type=click.Path(file_okay=False))
@click.option("--threshold", default=3.0, type=float)
@click.option("--min-bins", default=5, type=int)
@click.option("--persistence", default=0.6, type=float)
def detect(spec_path: str, events_path: str, out_dir: str, threshold: float, min_bins: int, persistence: float):
    spec = load_spec(spec_path)
    validate_spec(spec)
    events = read_events_csv(events_path)
    ctx = RunContext(cache_dir=spec.get("cache_dir", ".helix_cache"))
    surf = build_surface(events, spec, ctx=ctx)
    res = extract_detection_from_surface(surf, threshold=threshold, min_bins=min_bins, persistence=persistence)
    outp = ensure_dir(out_dir)
    with open(outp / "detection.yaml", "w", encoding="utf-8") as f:
        yaml.safe_dump(res.detection, f, sort_keys=False)
    write_csv(res.summary, outp / "summary.csv")
    write_csv(res.surface_slice, outp / "surface_slice.csv")
    click.echo(f"Wrote: {outp/'detection.yaml'}")
    click.echo(f"Wrote: {outp/'summary.csv'}")
    click.echo(f"Wrote: {outp/'surface_slice.csv'}")

@cli.command()
@click.option("--csv", "csv_path", required=True, type=click.Path(exists=True, dir_okay=False))
@click.option("--out", "out_path", required=True, type=click.Path(dir_okay=False))
@click.option("--series-key", default=None, help="Which series_key to plot (default: first one in the CSV).")
@click.option("--channel", default=None, help="Optional channel filter.")
@click.option("--metric", default=None, help="Optional metric filter.")
@click.option("--dpi", default=200, type=int)
@click.option("--max-ticks", default=7, type=int, help="Max number of x-axis tick labels.")
def heatmap(csv_path: str, out_path: str, series_key: str | None, channel: str | None, metric: str | None, dpi: int, max_ticks: int):
    """Render a window-vs-time heatmap from a surface CSV."""
    import numpy as np
    import pandas as pd
    import matplotlib.pyplot as plt

    def window_to_seconds(w: str) -> float:
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

    df = pd.read_csv(csv_path)
    required = {"ts_bin", "window", "score"}
    missing = required - set(df.columns)
    if missing:
        raise click.ClickException(f"Surface CSV missing columns: {sorted(missing)}. Got: {list(df.columns)}")

    if channel is not None and "channel" in df.columns:
        df = df[df["channel"] == channel]
    if metric is not None and "metric" in df.columns:
        df = df[df["metric"] == metric]

    if "series_key" in df.columns:
        if series_key is None:
            series_key = str(df["series_key"].dropna().unique()[0])
        df = df[df["series_key"] == series_key]
    else:
        series_key = series_key or "global"

    df["ts_bin"] = pd.to_datetime(df["ts_bin"], utc=True, errors="coerce")
    df = df.dropna(subset=["ts_bin", "score"])
    if df.empty:
        raise click.ClickException("No rows left after filtering/cleanup. Check --series-key/--channel/--metric.")

    windows_sorted = sorted(df["window"].dropna().unique(), key=window_to_seconds)
    pivot = (
        df.pivot_table(index="window", columns="ts_bin", values="score", aggfunc="mean")
          .reindex(windows_sorted)
    )

    plt.figure(figsize=(18, 6))
    # smallest window closest to x-axis
    plt.imshow(pivot.values, aspect="auto", origin="lower")
    plt.yticks(range(len(pivot.index)), pivot.index)

    cols = list(pivot.columns)
    n = len(cols)
    if n > 0:
        idx = np.linspace(0, n - 1, num=min(max_ticks, n), dtype=int)
        labels = [pd.to_datetime(cols[i]).strftime("%Y-%m-%d") for i in idx]
        plt.xticks(idx, labels)

    plt.colorbar(label="Score")
    plt.title(f"Surface Heatmap (series_key={series_key})")
    plt.xlabel("date (UTC)")
    plt.ylabel("window (small → large)")
    plt.tight_layout()
    plt.savefig(out_path, dpi=dpi)
    click.echo(f"Wrote: {out_path}")
