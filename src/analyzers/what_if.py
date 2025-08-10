from __future__ import annotations

import numpy as np
import pandas as pd
from typing import Dict, Any


def simulate_counterfactuals(model, X: pd.DataFrame, scenarios: Dict[str, Dict[str, Any]]) -> Dict[str, float]:
    """Apply simple counterfactual changes to features and return predicted deltas.
    scenarios: name -> {feature: value or callable(current_series)->series}
    Returns: scenario -> delta_mean (predicted_sales - baseline)
    """
    preds_base = model.predict(X)
    out: Dict[str, float] = {}
    for name, changes in scenarios.items():
        X_cf = X.copy()
        for feat, val in changes.items():
            if callable(val):
                X_cf[feat] = val(X_cf[feat])
            else:
                X_cf[feat] = val
        preds_cf = model.predict(X_cf)
        out[name] = float(np.mean(preds_cf - preds_base))
    return out