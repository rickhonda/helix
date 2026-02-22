from __future__ import annotations
import hashlib, json
from pathlib import Path
from typing import Any, Dict, Optional
import pandas as pd

def _stable_json(obj: Any) -> str:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), default=str)

def make_key(prefix: str, spec: Dict[str, Any], extra: Dict[str, Any] | None = None) -> str:
    payload = {"spec": spec, "extra": extra or {}}
    h = hashlib.sha256((_stable_json(payload)).encode("utf-8")).hexdigest()[:24]
    return f"{prefix}_{h}"

def cache_path(cache_dir: str, key: str, ext: str = "parquet") -> Path:
    d = Path(cache_dir)
    d.mkdir(parents=True, exist_ok=True)
    return d / f"{key}.{ext}"

def load_df(cache_dir: str, key: str) -> Optional[pd.DataFrame]:
    p = cache_path(cache_dir, key, "parquet")
    if p.exists():
        return pd.read_parquet(p)
    return None

def save_df(cache_dir: str, key: str, df: pd.DataFrame) -> None:
    p = cache_path(cache_dir, key, "parquet")
    df.to_parquet(p, index=False)
