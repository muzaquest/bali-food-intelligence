from __future__ import annotations

import os
import json
from dataclasses import dataclass
from typing import Tuple, Dict, Any

import numpy as np
import pandas as pd
import sqlite3

from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, mean_absolute_percentage_error, r2_score
from sklearn.model_selection import train_test_split
import joblib


@dataclass
class TrainResult:
    model_path: str
    feature_names_path: str
    metrics_path: str
    metrics: Dict[str, Any]


def _time_to_minutes(value: Any) -> float:
    if value is None or value == '' or value == '00:00:00':
        return 0.0
    try:
        s = str(value)
        if ':' in s:
            parts = s.split(':')
            h = int(parts[0])
            m = int(parts[1])
            sec = int(parts[2]) if len(parts) > 2 else 0
            return h * 60 + m + sec / 60.0
        # numeric string
        v = float(s)
        # some DBs may store minutes already
        return v
    except Exception:
        return 0.0


def _build_dates_frame(conn: sqlite3.Connection) -> pd.DataFrame:
    q = """
    WITH all_rows AS (
        SELECT restaurant_id, stat_date FROM grab_stats
        UNION
        SELECT restaurant_id, stat_date FROM gojek_stats
    )
    SELECT restaurant_id, stat_date as date
    FROM all_rows
    ORDER BY restaurant_id, date
    """
    return pd.read_sql_query(q, conn)


def build_global_dataset(db_path: str = "database.sqlite") -> pd.DataFrame:
    with sqlite3.connect(db_path) as conn:
        dates = _build_dates_frame(conn)
        # Map restaurant_id -> name (for potential future per-location weather)
        rest_map = pd.read_sql_query("SELECT id as restaurant_id, name FROM restaurants", conn)
        # Grab
        gq = """
        SELECT restaurant_id,
               stat_date as date,
               COALESCE(sales,0) as grab_sales,
               COALESCE(orders,0) as grab_orders,
               COALESCE(rating, NULL) as rating_grab,
               COALESCE(offline_rate,0) as grab_offline_rate,
               COALESCE(driver_waiting_time,0) as grab_driver_waiting,
               COALESCE(ads_spend,0) as grab_ads_spend,
               COALESCE(ads_sales,0) as grab_ads_sales,
               COALESCE(impressions,0) as grab_impressions
        FROM grab_stats
        """
        grab = pd.read_sql_query(gq, conn)

        # Gojek
        jq = """
        SELECT restaurant_id,
               stat_date as date,
               COALESCE(sales,0) as gojek_sales,
               COALESCE(orders,0) as gojek_orders,
               COALESCE(rating,NULL) as rating_gojek,
               COALESCE(close_time,'00:00:00') as gojek_close_time,
               COALESCE(preparation_time,'00:00:00') as gojek_preparation_time,
               COALESCE(delivery_time,'00:00:00') as gojek_delivery_time,
               COALESCE(driver_waiting,0) as gojek_driver_waiting,
               COALESCE(ads_spend,0) as gojek_ads_spend,
               COALESCE(ads_sales,0) as gojek_ads_sales,
               0 as gojek_impressions
        FROM gojek_stats
        """
        gojek = pd.read_sql_query(jq, conn)

    # Merge to dates
    df = dates.merge(grab, on=["restaurant_id", "date"], how="left")
    df = df.merge(gojek, on=["restaurant_id", "date"], how="left")
    df = df.merge(rest_map, on="restaurant_id", how="left")
    for col in [
        "grab_sales","grab_orders","grab_offline_rate","grab_driver_waiting",
        "grab_ads_spend","grab_ads_sales","grab_impressions",
        "gojek_sales","gojek_orders","gojek_driver_waiting",
        "gojek_ads_spend","gojek_ads_sales","gojek_impressions"
    ]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

    # Ratings: prefer available
    df["rating_grab"] = df["rating_grab"].fillna(0)
    df["rating_gojek"] = df["rating_gojek"].fillna(0)

    # Convert times
    df["gojek_close_minutes"] = df["gojek_close_time"].apply(_time_to_minutes)
    df["gojek_preparation_minutes"] = df["gojek_preparation_time"].apply(_time_to_minutes)
    df["gojek_delivery_minutes"] = df["gojek_delivery_time"].apply(_time_to_minutes)

    # Target and simple aggregations
    df["total_sales"] = df["grab_sales"].astype(float) + df["gojek_sales"].astype(float)
    df["total_orders"] = df["grab_orders"].astype(float) + df["gojek_orders"].astype(float)

    # Marketing features
    df["roas_grab"] = df["grab_ads_sales"] / df["grab_ads_spend"].replace(0, np.nan)
    df["roas_gojek"] = df["gojek_ads_sales"] / df["gojek_ads_spend"].replace(0, np.nan)
    df["roas_grab"] = df["roas_grab"].fillna(0)
    df["roas_gojek"] = df["roas_gojek"].fillna(0)
    df["ads_spend_total"] = df["grab_ads_spend"] + df["gojek_ads_spend"]
    df["impressions_total"] = df["grab_impressions"] + df["gojek_impressions"]

    # Calendar features
    dts = pd.to_datetime(df["date"])
    df["day_of_week"] = dts.dt.dayofweek
    df["is_weekend"] = (df["day_of_week"] >= 5).astype(int)
    df["month"] = dts.dt.month

    # External features: try cached weather and holidays
    df["precipitation"] = 0.0
    df["temperature"] = 27.0
    # Weather cache (optional): expects {"YYYY-MM-DD": {"precipitation": float, "temperature": float}}
    try:
        with open(os.path.join("data", "mega_weather_analysis.json"), "r", encoding="utf-8") as f:
            weather_cache = json.load(f)
        if isinstance(weather_cache, dict):
            df["precipitation"] = df["date"].map(lambda d: float(weather_cache.get(str(d), {}).get("precipitation", 0.0)))
            df["temperature"] = df["date"].map(lambda d: float(weather_cache.get(str(d), {}).get("temperature", 27.0)))
    except Exception:
        pass
    # Per-restaurant weather cache override
    try:
        cache_dir = os.path.join('data', 'weather_cache')
        if os.path.isdir(cache_dir):
            # Build map: rid -> {date_str: {precipitation, temperature}}
            rid_to_weather = {}
            for fn in os.listdir(cache_dir):
                if not fn.startswith('daily_') or not fn.endswith('.json'):
                    continue
                rid = int(fn.replace('daily_', '').replace('.json', ''))
                with open(os.path.join(cache_dir, fn), 'r', encoding='utf-8') as f:
                    rid_to_weather[rid] = json.load(f)
            if rid_to_weather:
                # Apply per-row lookup overriding generic values
                def wx_prec(row):
                    wc = rid_to_weather.get(int(row['restaurant_id']))
                    if not wc:
                        return row['precipitation']
                    v = wc.get(str(row['date']), {}).get('precipitation')
                    return float(v) if v is not None else row['precipitation']
                def wx_temp(row):
                    wc = rid_to_weather.get(int(row['restaurant_id']))
                    if not wc:
                        return row['temperature']
                    v = wc.get(str(row['date']), {}).get('temperature')
                    return float(v) if v is not None else row['temperature']
                df['precipitation'] = df.apply(wx_prec, axis=1)
                df['temperature'] = df.apply(wx_temp, axis=1)
    except Exception:
        pass
    # Holidays (164 types): mark binary if date appears in comprehensive file
    df["is_holiday"] = 0
    try:
        # Prefer enhanced holidays if exists
        path = os.path.join("data", "enhanced_holidays.json")
        if not os.path.exists(path):
            path = os.path.join("data", "comprehensive_holiday_analysis.json")
        with open(path, "r", encoding="utf-8") as f:
            hol = json.load(f)
        # Try both direct date->info and nested under 'results'
        if isinstance(hol, dict):
            holiday_map = hol.get("results", hol)
            if isinstance(holiday_map, dict):
                holiday_dates = set(map(str, holiday_map.keys()))
                df["is_holiday"] = df["date"].astype(str).isin(holiday_dates).astype(int)
    except Exception:
        pass

    # Tourism index (0..1)
    try:
        from src.utils.tourism_loader import load_tourism_index
        tourism_map = load_tourism_index()
        df["tourism_index"] = df["date"].astype(str).map(lambda d: float(tourism_map.get(str(d), 0.0)))
    except Exception:
        df["tourism_index"] = 0.0

    # Trends per restaurant
    df = df.sort_values(["restaurant_id", "date"]).reset_index(drop=True)
    df["sales_7day_avg"] = (
        df.groupby("restaurant_id")["total_sales"].rolling(7, min_periods=1).mean().shift(1).reset_index(level=0, drop=True)
    )
    df["sales_30day_avg"] = (
        df.groupby("restaurant_id")["total_sales"].rolling(30, min_periods=1).mean().shift(1).reset_index(level=0, drop=True)
    )
    df["sales_gradient_7"] = (
        df.groupby("restaurant_id")["total_sales"].diff(7)
    )

    # Drop initial NaNs due to lags
    df = df.dropna(subset=["sales_7day_avg", "sales_30day_avg"]).reset_index(drop=True)

    return df


def train_global_model(db_path: str = "database.sqlite", models_dir: str = "models") -> TrainResult:
    os.makedirs(models_dir, exist_ok=True)
    df = build_global_dataset(db_path)
    if df.empty or len(df) < 200:
        raise RuntimeError("Недостаточно данных для глобального обучения")

    # Features covering 17 факторов
    feature_cols = [
        # Операционные
        "grab_offline_rate", "gojek_close_minutes",
        "grab_driver_waiting", "gojek_driver_waiting",
        "gojek_preparation_minutes", "gojek_delivery_minutes",
        # Внешние
        "precipitation", "temperature", "is_holiday", "day_of_week", "is_weekend",
        # Маркетинг
        "roas_grab", "roas_gojek", "ads_spend_total", "impressions_total",
        # Качество
        "rating_grab", "rating_gojek",
        # Тренды/туризм
        "sales_7day_avg", "sales_30day_avg", "sales_gradient_7", "tourism_index",
    ]

    # One-hot for day_of_week (already numeric; keep as numeric category)
    X = df[feature_cols].copy()
    y = df["total_sales"].astype(float)

    # Train/test split preserving time: last 14 days as test per restaurant
    # Simplified: use chronological split on full data
    split_idx = int(len(df) * 0.9)
    X_train, X_test = X.iloc[:split_idx], X.iloc[split_idx:]
    y_train, y_test = y.iloc[:split_idx], y.iloc[split_idx:]

    model = RandomForestRegressor(
        n_estimators=600,
        max_depth=None,
        random_state=42,
        n_jobs=-1,
        min_samples_leaf=2,
    )
    model.fit(X_train, y_train)

    y_pred = model.predict(X_test)
    # Robust metrics: avoid division by zero, clip tails
    import numpy as np
    y_true = y_test.copy()
    y_hat = y_pred.copy()
    mask = y_true > 0
    if mask.sum() > 0:
        ape = np.abs((y_hat[mask] - y_true[mask]) / y_true[mask])
        ape = np.clip(ape, 0, 10)  # cap extreme at 1000%
        mape = float(ape.mean())
    else:
        mape = float('nan')
    metrics = {
        "mape": mape,
        "mae": float(mean_absolute_error(y_test, y_pred)),
        "r2": float(r2_score(y_test, y_pred)),
        "n_train": int(len(y_train)),
        "n_test": int(len(y_test)),
    }

    model_path = os.path.join(models_dir, "global_sales_rf.joblib")
    features_path = os.path.join(models_dir, "global_sales_rf_features.json")
    metrics_path = os.path.join(models_dir, "global_sales_rf_metrics.json")

    joblib.dump(model, model_path)
    with open(features_path, "w", encoding="utf-8") as f:
        json.dump({"features": feature_cols}, f, ensure_ascii=False, indent=2)
    with open(metrics_path, "w", encoding="utf-8") as f:
        json.dump(metrics, f, ensure_ascii=False, indent=2)

    return TrainResult(model_path=model_path, feature_names_path=features_path, metrics_path=metrics_path, metrics=metrics)