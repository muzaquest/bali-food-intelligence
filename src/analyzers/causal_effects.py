from __future__ import annotations

import numpy as np
import pandas as pd
from dataclasses import dataclass
from typing import Dict, List, Optional

try:
    import statsmodels.api as sm
    import statsmodels.formula.api as smf
    HAS_SM = True
except Exception:
    HAS_SM = False


@dataclass
class CausalEffect:
    factor: str
    effect_pct: float
    ci_low: float
    ci_high: float
    p_value: Optional[float]
    samples: int


def _percent_effect(coef: float, baseline: float) -> float:
    if baseline == 0:
        return 0.0
    return 100.0 * (coef / max(1e-9, baseline))


def estimate_causal_effects(df: pd.DataFrame) -> List[CausalEffect]:
    """Estimate causal effects for key factors with simple controls.
    Controls: lags, day-of-week, month fixed effects.
    Factors: heavy rain, holiday, store closed/busy.
    """
    data = df.copy()
    if "date" not in data.columns or "total_sales" not in data.columns:
        return []
    data["date"] = pd.to_datetime(data["date"])  # ensure datetime

    # Feature engineering
    for lag in [1, 7, 14]:
        data[f"sales_lag_{lag}"] = data["total_sales"].shift(lag)
    data["dow"] = data["date"].dt.dayofweek
    data["month"] = data["date"].dt.month

    # Treatments
    if "rain" not in data.columns:
        data["rain"] = 0.0
    data["heavy_rain"] = (data["rain"] >= 10.0).astype(int)
    data["is_holiday"] = data.get("is_holiday", 0)
    data["store_is_closed"] = data.get("store_is_closed", 0)
    data["store_is_busy"] = data.get("store_is_busy", 0)

    data = data.dropna()
    if len(data) < 90:
        # Not enough data
        return []

    effects: List[CausalEffect] = []

    if HAS_SM:
        formula = (
            "total_sales ~ heavy_rain + is_holiday + store_is_closed + store_is_busy + "
            "sales_lag_1 + sales_lag_7 + sales_lag_14 + C(dow) + C(month)"
        )
        model = smf.ols(formula, data=data).fit(cov_type="HAC", cov_kwds={"maxlags":7})
        baseline = data["total_sales"].mean()
        for factor in ["heavy_rain", "is_holiday", "store_is_closed", "store_is_busy"]:
            coef = model.params.get(factor, 0.0)
            se = model.bse.get(factor, np.nan)
            pval = model.pvalues.get(factor, np.nan)
            # 95% CI
            ci_low = coef - 1.96 * se if np.isfinite(se) else np.nan
            ci_high = coef + 1.96 * se if np.isfinite(se) else np.nan
            effects.append(
                CausalEffect(
                    factor=factor,
                    effect_pct=_percent_effect(coef, baseline),
                    ci_low=_percent_effect(ci_low, baseline) if np.isfinite(ci_low) else np.nan,
                    ci_high=_percent_effect(ci_high, baseline) if np.isfinite(ci_high) else np.nan,
                    p_value=float(pval) if np.isfinite(pval) else None,
                    samples=len(data),
                )
            )
        return effects

    # Fallback: bootstrap difference-in-means
    rng = np.random.default_rng(42)
    baseline = data["total_sales"].mean()
    for factor, condition in {
        "heavy_rain": data["heavy_rain"] == 1,
        "is_holiday": data["is_holiday"] == 1,
        "store_is_closed": data["store_is_closed"] == 1,
        "store_is_busy": data["store_is_busy"] == 1,
    }.items():
        treated = data.loc[condition, "total_sales"].to_numpy()
        control = data.loc[~condition, "total_sales"].to_numpy()
        if len(treated) < 20 or len(control) < 20:
            continue
        diffs = []
        for _ in range(1000):
            t = rng.choice(treated, size=len(treated), replace=True)
            c = rng.choice(control, size=len(control), replace=True)
            diffs.append((t.mean() - c.mean()) / max(1e-9, baseline) * 100.0)
        ci_low, effect, ci_high = np.percentile(diffs, [2.5, 50.0, 97.5]).tolist()
        effects.append(CausalEffect(factor=factor, effect_pct=effect, ci_low=ci_low, ci_high=ci_high, p_value=None, samples=len(data)))
    return effects