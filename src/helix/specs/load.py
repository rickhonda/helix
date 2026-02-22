from __future__ import annotations
from typing import Any, Dict
import yaml

def load_spec(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)
    if not isinstance(data, dict):
        raise ValueError("Spec root must be a mapping/dict.")
    return data
