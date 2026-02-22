from __future__ import annotations
from typing import Any, Dict

def validate_spec(spec: Dict[str, Any]) -> None:
    if "base_channels" not in spec or not isinstance(spec["base_channels"], list):
        raise ValueError("Spec must include 'base_channels' as a list.")
    if "scoring" not in spec or not isinstance(spec["scoring"], list) or not spec["scoring"]:
        raise ValueError("Spec must include non-empty 'scoring' list.")
