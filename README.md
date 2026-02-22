# Helix v0.5 Demo — Min-hop Surface Reuse + Detect + Heatmap

This is a **drop-in** repo zip intended to be a stable baseline.

Included:
- `helix run` — select + aggregate + score
- `helix surface` — **min-hop reuse** (fast window sweep) for `count/sum/mean`
- `helix detect` — extract a detection artifact from the surface
- `helix heatmap` — render a surface heatmap PNG with:
  - windows sorted by duration
  - **smallest window closest to x-axis**
  - **few x ticks showing dates only**

Cache directory: `.helix_cache/` (safe to delete).

## Install (uv)
```bash
uv venv --python 3.12
uv pip install -e . --python .venv/bin/python3
```

## Demo: numeric (Intermagnet-like)
```bash
rm -rf .helix_cache outputs_surface detections
uv run helix surface --spec examples/specs/demo_numeric.yaml --events examples/data/numeric.csv --out outputs_surface
uv run helix detect  --spec examples/specs/demo_numeric.yaml --events examples/data/numeric.csv --out detections
uv run helix heatmap --csv outputs_surface/surface.csv --out heatmap.png
```

## Surface schema
`surface.csv` columns are stable:
`ts_bin,window,channel,series_key,metric,score`
