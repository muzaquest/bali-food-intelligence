from __future__ import annotations

import numpy as np
import pandas as pd
from dataclasses import dataclass
from typing import Dict, List, Tuple, Optional

from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_percentage_error, mean_absolute_error, r2_score


@dataclass
class BacktestMetrics:
    mape: float
    mae: float
    r2: float
    folds: int
    observations: int
    feature_importance: Optional[pd.DataFrame]


def _prepare_features(df: pd.DataFrame, target_col: str = "total_sales") -> Tuple[pd.DataFrame, pd.Series]:
    data = df.copy()
    data = data.sort_values("date")
    # Basic lags
    for lag in [1, 7, 14]:
        data[f"sales_lag_{lag}"] = data[target_col].shift(lag)
    # Calendar
    data["dow"] = pd.to_datetime(data["date"]).dt.dayofweek
    dow_dummies = pd.get_dummies(data["dow"], prefix="dow", drop_first=True)
    data = pd.concat([data.drop(columns=["dow"]), dow_dummies], axis=1)
    # Weather proxies if present
    for col in ["rain", "temperature", "is_holiday", "is_weekend", "marketing_spend", "store_is_closed", "store_is_busy"]:
        if col not in data.columns:
            data[col] = 0
    # Drop rows with NA from lagging
    data = data.dropna()
    y = data[target_col]
    X = data.drop(columns=[target_col])
    # Drop non-numeric columns (keep engineered dummies)
    X = X.select_dtypes(include=[np.number])
    return X, y


def time_series_backtest(df: pd.DataFrame, n_splits: int = 5) -> BacktestMetrics:
    """Rolling-origin backtest for daily sales forecasting.
    Expects df with at least ['date','total_sales'] and optional engineered features.
    """
    if len(df) < 100:
        n_splits = max(3, min(4, len(df) // 20))
    df = df.sort_values("date").reset_index(drop=True)

    # Prepare baseline features
    X_all, y_all = _prepare_features(df)

    fold_sizes = np.linspace(0.6, 0.9, n_splits)  # growing windows
    y_true_all: List[float] = []
    y_pred_all: List[float] = []
    importances: List[pd.Series] = []

    for frac in fold_sizes:
        split_idx = int(len(X_all) * frac)
        if split_idx <= 30 or split_idx >= len(X_all) - 7:
            continue
        X_train, y_train = X_all.iloc[:split_idx], y_all.iloc[:split_idx]
        X_test, y_test = X_all.iloc[split_idx:], y_all.iloc[split_idx:]

        model = RandomForestRegressor(n_estimators=300, random_state=42, n_jobs=-1)
        model.fit(X_train, y_train)
        y_pred = model.predict(X_test)

        y_true_all.extend(y_test.tolist())
        y_pred_all.extend(y_pred.tolist())

        # Collect feature importances
        importances.append(pd.Series(model.feature_importances_, index=X_train.columns))

    if not y_true_all:
        return BacktestMetrics(mape=np.nan, mae=np.nan, r2=np.nan, folds=0, observations=0, feature_importance=None)

    mape = float(mean_absolute_percentage_error(y_true_all, y_pred_all))
    mae = float(mean_absolute_error(y_true_all, y_pred_all))
    r2 = float(r2_score(y_true_all, y_pred_all))

    fi_df = None
    if importances:
        fi_df = pd.concat(importances, axis=1).mean(axis=1).sort_values(ascending=False).rename("importance").to_frame()

    return BacktestMetrics(mape=mape, mae=mae, r2=r2, folds=len(importances), observations=len(y_true_all), feature_importance=fi_df)