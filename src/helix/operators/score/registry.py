from helix.operators.score.mad_z import MadZOperator
from helix.operators.score.ewma_residual_z import EwmaResidualZOperator

_REGISTRY = {"mad_z": MadZOperator(), "ewma_residual_z": EwmaResidualZOperator()}

def get_score_operator(name: str):
    if name not in _REGISTRY:
        raise ValueError(f"Unknown score operator '{name}'. Available={sorted(_REGISTRY)}")
    return _REGISTRY[name]
