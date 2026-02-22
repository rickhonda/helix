import pandas as pd
from helix.specs.load import load_spec
from helix.core.surface_fast import build_surface
from helix.core.detect import extract_detection_from_surface

def test_surface_and_detect():
    spec = load_spec("examples/specs/demo_numeric.yaml")
    events = pd.read_csv("examples/data/numeric.csv")
    surf = build_surface(events, spec)
    assert len(surf) > 0
    # ensure stable schema
    for col in ["ts_bin","window","channel","series_key","metric","score"]:
        assert col in surf.columns
    res = extract_detection_from_surface(surf, threshold=3.0, min_bins=3, persistence=0.5)
    assert "window" in res.detection
