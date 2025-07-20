#!/usr/bin/env python3
"""
🔧 ИСПРАВЛЕННЫЙ FEATURE ENGINEERING
Создаёт только разрешённые признаки согласно DATA_FIELDS_USED.md
"""

import pandas as pd
import numpy as np
from typing import List, Dict, Optional, Tuple
import logging

logger = logging.getLogger(__name__)

# 📊 РАЗРЕШЁННЫЕ ЛАГИ (только исторические данные)
ALLOWED_LAGS = [1, 3, 7, 14]  # дни
ALLOWED_ROLLING_WINDOWS = [3, 7, 14, 30]  # дни
ALLOWED_DIFF_WINDOWS = [1, 7]  # дни
ALLOWED_TREND_WINDOWS = [7, 14]  # дни

# ⚠️ ПОЛЯ ДЛЯ СОЗДАНИЯ ЛАГОВ (исключаем целевую переменную и технические)
LAG_FEATURES = [
    'orders', 'avg_order_value', 'delivery_time', 'cancelled_orders',
    'rating', 'new_customers_count', 'marketing_spend', 'promotion_usage'
]

# 📊 ПОЛЯ ДЛЯ СКОЛЬЗЯЩИХ СРЕДНИХ
ROLLING_FEATURES = [
    'orders', 'avg_order_value', 'delivery_time', 'rating', 'marketing_spend'
]

# 🔗 РАЗРЕШЁННЫЕ ВЗАИМОДЕЙСТВИЯ
INTERACTION_PAIRS = [
    ('temperature_celsius', 'is_weekend'),
    ('is_rainy', 'is_holiday'),
    ('marketing_spend', 'is_weekend'),
    ('is_hot', 'delivery_time'),
    ('humidity_percent', 'orders')
]

def validate_no_future_leakage(df: pd.DataFrame, feature_cols: List[str]) -> None:
    """Проверяет отсутствие утечки данных из будущего"""
    
    logger.info("🔍 Проверка на утечку данных из будущего...")
    
    # 1. Проверяем названия колонок
    forbidden_patterns = ['lag_minus', 'future', 'next_', 'tomorrow']
    
    for pattern in forbidden_patterns:
        forbidden_cols = [col for col in feature_cols if pattern in col.lower()]
        if forbidden_cols:
            raise ValueError(f"❌ Найдены признаки из будущего: {forbidden_cols}")
    
    # 2. Проверяем что лаги положительные
    lag_cols = [col for col in feature_cols if '_lag_' in col]
    for col in lag_cols:
        try:
            lag_value = int(col.split('_lag_')[-1])
            if lag_value <= 0:
                raise ValueError(f"❌ Найден неположительный лаг: {col}")
        except:
            continue  # Если не можем распарсить - пропускаем
    
    logger.info("✅ Утечки данных из будущего не найдено")

def create_lag_features(df: pd.DataFrame) -> pd.DataFrame:
    """Создаёт лаговые признаки (только исторические)"""
    
    logger.info("📈 Создание лаговых признаков...")
    
    df_with_lags = df.copy()
    
    # Сортируем по ресторану, платформе и дате для корректных лагов
    df_with_lags = df_with_lags.sort_values(['restaurant_name', 'platform', 'date'])
    
    created_features = 0
    
    for feature in LAG_FEATURES:
        if feature not in df_with_lags.columns:
            logger.warning(f"⚠️ Признак {feature} не найден, пропускаем")
            continue
            
        for lag in ALLOWED_LAGS:
            lag_col_name = f"{feature}_lag_{lag}"
            
            # Создаём лаг внутри каждой группы (ресторан + платформа)
            df_with_lags[lag_col_name] = df_with_lags.groupby(['restaurant_name', 'platform'])[feature].shift(lag)
            created_features += 1
    
    logger.info(f"✅ Создано {created_features} лаговых признаков")
    return df_with_lags

def create_rolling_features(df: pd.DataFrame) -> pd.DataFrame:
    """Создаёт скользящие средние"""
    
    logger.info("🎯 Создание скользящих средних...")
    
    df_with_rolling = df.copy()
    created_features = 0
    
    for feature in ROLLING_FEATURES:
        if feature not in df_with_rolling.columns:
            continue
            
        for window in ALLOWED_ROLLING_WINDOWS:
            # Скользящее среднее
            rolling_mean_col = f"{feature}_rolling_mean_{window}"
            df_with_rolling[rolling_mean_col] = df_with_rolling.groupby(['restaurant_name', 'platform'])[feature].rolling(
                window=window, min_periods=1).mean().reset_index(level=[0,1], drop=True)
            
            # Скользящее стандартное отклонение
            rolling_std_col = f"{feature}_rolling_std_{window}"
            df_with_rolling[rolling_std_col] = df_with_rolling.groupby(['restaurant_name', 'platform'])[feature].rolling(
                window=window, min_periods=1).std().reset_index(level=[0,1], drop=True)
            
            created_features += 2
    
    logger.info(f"✅ Создано {created_features} rolling признаков")
    return df_with_rolling

def create_diff_features(df: pd.DataFrame) -> pd.DataFrame:
    """Создаёт разностные признаки"""
    
    logger.info("📊 Создание разностных признаков...")
    
    df_with_diffs = df.copy()
    created_features = 0
    
    # Только для ключевых метрик
    diff_features = ['orders', 'avg_order_value', 'rating', 'marketing_spend']
    
    for feature in diff_features:
        if feature not in df_with_diffs.columns:
            continue
            
        for window in ALLOWED_DIFF_WINDOWS:
            diff_col_name = f"{feature}_diff_{window}"
            
            # Разность с предыдущим значением
            lag_values = df_with_diffs.groupby(['restaurant_name', 'platform'])[feature].shift(window)
            df_with_diffs[diff_col_name] = df_with_diffs[feature] - lag_values
            created_features += 1
    
    logger.info(f"✅ Создано {created_features} разностных признаков")
    return df_with_diffs

def create_trend_features(df: pd.DataFrame) -> pd.DataFrame:
    """Создаёт трендовые признаки"""
    
    logger.info("📉 Создание трендовых признаков...")
    
    df_with_trends = df.copy()
    created_features = 0
    
    trend_features = ['orders', 'rating', 'delivery_time']
    
    for feature in trend_features:
        if feature not in df_with_trends.columns:
            continue
            
        for window in ALLOWED_TREND_WINDOWS:
            trend_col_name = f"{feature}_trend_{window}"
            
            # Простой тренд как разность между текущим значением и средним за период
            rolling_mean = df_with_trends.groupby(['restaurant_name', 'platform'])[feature].rolling(
                window=window, min_periods=1).mean().reset_index(level=[0,1], drop=True)
            
            df_with_trends[trend_col_name] = df_with_trends[feature] - rolling_mean
            created_features += 1
    
    logger.info(f"✅ Создано {created_features} трендовых признаков")
    return df_with_trends

def create_interaction_features(df: pd.DataFrame) -> pd.DataFrame:
    """Создаёт взаимодействующие признаки"""
    
    logger.info("🔗 Создание взаимодействующих признаков...")
    
    df_with_interactions = df.copy()
    created_features = 0
    
    for feature1, feature2 in INTERACTION_PAIRS:
        if feature1 in df_with_interactions.columns and feature2 in df_with_interactions.columns:
            interaction_name = f"{feature1}_{feature2}_interaction"
            
            # Простое произведение для взаимодействия
            df_with_interactions[interaction_name] = df_with_interactions[feature1] * df_with_interactions[feature2]
            created_features += 1
    
    logger.info(f"✅ Создано {created_features} взаимодействующих признаков")
    return df_with_interactions

def create_aggregate_features(df: pd.DataFrame) -> pd.DataFrame:
    """Создаёт агрегированные признаки"""
    
    logger.info("📊 Создание агрегированных признаков...")
    
    df_with_aggs = df.copy()
    created_features = 0
    
    # Ратио признаки
    if 'cancelled_orders' in df_with_aggs.columns and 'orders' in df_with_aggs.columns:
        df_with_aggs['cancel_rate'] = df_with_aggs['cancelled_orders'] / (df_with_aggs['orders'] + 1e-6)
        created_features += 1
    
    if 'new_customers_count' in df_with_aggs.columns and 'total_customers_count' in df_with_aggs.columns:
        df_with_aggs['new_customer_rate'] = df_with_aggs['new_customers_count'] / (df_with_aggs['total_customers_count'] + 1e-6)
        created_features += 1
    
    if 'marketing_spend' in df_with_aggs.columns and 'orders' in df_with_aggs.columns:
        df_with_aggs['marketing_efficiency'] = df_with_aggs['orders'] / (df_with_aggs['marketing_spend'] + 1e-6)
        created_features += 1
    
    # Комбинированные метрики
    if 'peak_hour_orders' in df_with_aggs.columns and 'off_peak_orders' in df_with_aggs.columns:
        df_with_aggs['peak_to_offpeak_ratio'] = df_with_aggs['peak_hour_orders'] / (df_with_aggs['off_peak_orders'] + 1e-6)
        created_features += 1
    
    logger.info(f"✅ Создано {created_features} агрегированных признаков")
    return df_with_aggs

def prepare_features_fixed(df: pd.DataFrame) -> pd.DataFrame:
    """
    Основная функция для создания признаков с контролем качества
    
    Args:
        df: Исходный DataFrame
        
    Returns:
        DataFrame с дополнительными признаками
    """
    
    logger.info("🔧 Начало создания признаков...")
    logger.info(f"📊 Исходных признаков: {len(df.columns)}")
    
    if df.empty:
        logger.warning("⚠️ Пустой DataFrame, возвращаем как есть")
        return df
    
    try:
        # 1. Создаём лаговые признаки
        df_featured = create_lag_features(df)
        
        # 2. Создаём скользящие средние  
        df_featured = create_rolling_features(df_featured)
        
        # 3. Создаём разностные признаки
        df_featured = create_diff_features(df_featured)
        
        # 4. Создаём трендовые признаки
        df_featured = create_trend_features(df_featured)
        
        # 5. Создаём взаимодействующие признаки
        df_featured = create_interaction_features(df_featured)
        
        # 6. Создаём агрегированные признаки
        df_featured = create_aggregate_features(df_featured)
        
        # 7. Валидируем результат
        feature_cols = [col for col in df_featured.columns if col not in ['date', 'restaurant_name', 'total_sales']]
        validate_no_future_leakage(df_featured, feature_cols)
        
        logger.info(f"✅ Итого признаков: {len(df_featured.columns)} (+{len(df_featured.columns) - len(df)})")
        logger.info("🔧 Создание признаков завершено успешно")
        
        return df_featured
        
    except Exception as e:
        logger.error(f"❌ Ошибка создания признаков: {e}")
        raise

def clean_features(df: pd.DataFrame) -> pd.DataFrame:
    """Очищает признаки от NaN и выбросов"""
    
    logger.info("🧹 Очистка признаков...")
    
    df_clean = df.copy()
    
    # Заполняем NaN нулями для новых признаков
    numeric_cols = df_clean.select_dtypes(include=[np.number]).columns
    df_clean[numeric_cols] = df_clean[numeric_cols].fillna(0)
    
    # Удаляем бесконечные значения
    df_clean = df_clean.replace([np.inf, -np.inf], 0)
    
    # Ограничиваем выбросы (по 99-му перцентилю)
    for col in numeric_cols:
        if col not in ['date', 'restaurant_name']:
            p99 = df_clean[col].quantile(0.99)
            p01 = df_clean[col].quantile(0.01)
            df_clean[col] = df_clean[col].clip(lower=p01, upper=p99)
    
    logger.info("✅ Очистка признаков завершена")
    return df_clean

def prepare_for_model(df: pd.DataFrame, target_col: str = 'total_sales') -> Tuple[pd.DataFrame, List[str]]:
    """
    Подготавливает данные для модели машинного обучения
    
    Args:
        df: DataFrame с признаками
        target_col: Название целевой переменной
        
    Returns:
        Tuple из (подготовленный DataFrame, список признаков)
    """
    
    logger.info("🤖 Подготовка данных для модели...")
    
    # Исключаем нецифровые и служебные колонки
    exclude_cols = {
        'date', 'restaurant_name', target_col,
        'weather_condition',  # категориальная
        'platform'  # будем обработать отдельно
    }
    
    # Энкодим платформу
    df_model = df.copy()
    if 'platform' in df_model.columns:
        df_model['platform_grab'] = (df_model['platform'] == 'grab').astype(int)
        df_model['platform_gojek'] = (df_model['platform'] == 'gojek').astype(int)
    
    # Выбираем только числовые признаки
    feature_cols = [col for col in df_model.columns 
                   if col not in exclude_cols and df_model[col].dtype in ['int64', 'float64']]
    
    logger.info(f"✅ Подготовлено {len(feature_cols)} признаков для модели")
    
    return df_model, feature_cols

if __name__ == "__main__":
    # Тестирование feature engineering
    import logging
    logging.basicConfig(level=logging.INFO)
    
    print("🧪 ТЕСТ ИСПРАВЛЕННОГО FEATURE ENGINEERING")
    print("=" * 50)
    
    # Создаём тестовые данные
    np.random.seed(42)
    dates = pd.date_range('2025-04-01', '2025-06-30', freq='D')
    
    test_data = []
    for restaurant in ['Ika Canggu', 'Prana']:
        for platform in ['grab', 'gojek']:
            for date in dates:
                test_data.append({
                    'date': date,
                    'restaurant_name': restaurant,
                    'platform': platform,
                    'total_sales': np.random.uniform(500, 1500),
                    'orders': np.random.randint(10, 50),
                    'avg_order_value': np.random.uniform(20, 60),
                    'delivery_time': np.random.uniform(20, 40),
                    'rating': np.random.uniform(3.5, 5.0),
                    'marketing_spend': np.random.uniform(0, 200),
                    'is_weekend': date.weekday() >= 5,
                    'temperature_celsius': np.random.uniform(25, 35)
                })
    
    test_df = pd.DataFrame(test_data)
    
    try:
        # Тест создания признаков
        featured_df = prepare_features_fixed(test_df)
        print(f"✅ Тест пройден: {len(test_df.columns)} → {len(featured_df.columns)} признаков")
        
        # Тест подготовки для модели
        model_df, features = prepare_for_model(featured_df)
        print(f"✅ Подготовка для модели: {len(features)} числовых признаков")
        
    except Exception as e:
        print(f"❌ Тест провален: {e}")
    
    print("🏁 Тестирование завершено")