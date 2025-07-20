#!/usr/bin/env python3
"""
Улучшенный feature engineering с использованием ВСЕХ полей базы данных
+ погода + календарь + расширенные метрики
"""

import pandas as pd
import numpy as np
import logging
from typing import List, Dict, Tuple
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

def create_weather_features(df: pd.DataFrame) -> pd.DataFrame:
    """Создание features на основе погодных данных"""
    try:
        # Простые weather features
        if 'temperature_celsius' in df.columns:
            df['temp_category'] = pd.cut(df['temperature_celsius'], 
                                       bins=[-np.inf, 25, 30, 35, np.inf],
                                       labels=['cool', 'comfortable', 'warm', 'hot'])
            df['temp_category'] = df['temp_category'].astype(str)
        
        # Дискомфорт индекс
        if 'temperature_celsius' in df.columns and 'humidity_percent' in df.columns:
            df['discomfort_index'] = df['temperature_celsius'] + (df['humidity_percent'] / 100) * 10
            df['is_uncomfortable'] = (df['discomfort_index'] > 35).astype(int)
        
        # Влияние дождя на доставку
        if 'precipitation_mm' in df.columns:
            df['rain_category'] = pd.cut(df['precipitation_mm'],
                                       bins=[-np.inf, 0.1, 5, 15, np.inf],
                                       labels=['no_rain', 'light', 'moderate', 'heavy'])
            df['rain_category'] = df['rain_category'].astype(str)
            df['rain_impact_score'] = np.minimum(df['precipitation_mm'] / 10, 2.0)  # 0-2 scale
        
        # Комбинированные weather conditions
        weather_conditions = []
        for _, row in df.iterrows():
            conditions = []
            if row.get('is_rainy', False):
                conditions.append('rainy')
            if row.get('is_hot', False):
                conditions.append('hot')
            if row.get('is_humid', False):
                conditions.append('humid')
            if row.get('is_windy', False):
                conditions.append('windy')
            
            if not conditions:
                conditions.append('clear')
            
            weather_conditions.append('_'.join(sorted(conditions)))
        
        df['weather_combination'] = weather_conditions
        
        logger.info("✅ Создано weather features")
        return df
        
    except Exception as e:
        logger.error(f"Ошибка при создании weather features: {e}")
        return df

def create_calendar_features(df: pd.DataFrame) -> pd.DataFrame:
    """Создание features на основе календарных данных"""
    try:
        # Циклические features для времени
        if 'day_of_week' in df.columns:
            df['day_of_week_sin'] = np.sin(2 * np.pi * df['day_of_week'] / 7)
            df['day_of_week_cos'] = np.cos(2 * np.pi * df['day_of_week'] / 7)
        
        if 'month' in df.columns:
            df['month_sin'] = np.sin(2 * np.pi * df['month'] / 12)
            df['month_cos'] = np.cos(2 * np.pi * df['month'] / 12)
        
        if 'day' in df.columns:
            df['day_sin'] = np.sin(2 * np.pi * df['day'] / 31)
            df['day_cos'] = np.cos(2 * np.pi * df['day'] / 31)
        
        # Специальные периоды
        special_periods = []
        for _, row in df.iterrows():
            periods = []
            if row.get('is_weekend', False):
                periods.append('weekend')
            if row.get('is_holiday', False):
                periods.append('holiday')
            if row.get('is_tourist_high_season', False):
                periods.append('tourist_season')
            if row.get('is_pay_day', False):
                periods.append('payday')
            if row.get('is_month_end', False):
                periods.append('month_end')
            
            if not periods:
                periods.append('regular')
            
            special_periods.append('_'.join(sorted(periods)))
        
        df['special_period_combination'] = special_periods
        
        # Расстояние до ближайшего праздника
        if 'is_holiday' in df.columns:
            df['days_to_holiday'] = 0
            df['days_from_holiday'] = 0
            
            for restaurant in df['restaurant_name'].unique():
                mask = df['restaurant_name'] == restaurant
                restaurant_df = df[mask].copy().sort_values('date')
                
                holiday_dates = restaurant_df[restaurant_df['is_holiday']]['date'].values
                
                if len(holiday_dates) > 0:
                    for idx, date in enumerate(restaurant_df['date'].values):
                        # Дни до ближайшего праздника
                        future_holidays = holiday_dates[holiday_dates > date]
                        if len(future_holidays) > 0:
                            days_to = (pd.to_datetime(future_holidays[0]) - pd.to_datetime(date)).days
                        else:
                            days_to = 365  # Если нет будущих праздников
                        
                        # Дни с последнего праздника
                        past_holidays = holiday_dates[holiday_dates <= date]
                        if len(past_holidays) > 0:
                            days_from = (pd.to_datetime(date) - pd.to_datetime(past_holidays[-1])).days
                        else:
                            days_from = 365  # Если нет прошлых праздников
                        
                        restaurant_idx = restaurant_df.index[idx]
                        df.loc[restaurant_idx, 'days_to_holiday'] = min(days_to, 30)  # Cap at 30
                        df.loc[restaurant_idx, 'days_from_holiday'] = min(days_from, 30)  # Cap at 30
        
        logger.info("✅ Создано calendar features")
        return df
        
    except Exception as e:
        logger.error(f"Ошибка при создании calendar features: {e}")
        return df

def create_customer_analytics_features(df: pd.DataFrame) -> pd.DataFrame:
    """Создание features на основе клиентской аналитики"""
    try:
        # Базовые коэффициенты удержания и лояльности
        if 'new_customers_count' in df.columns and 'returning_customers_count' in df.columns:
            df['total_customers_interaction'] = df['new_customers_count'] + df['returning_customers_count']
            df['customer_retention_rate'] = df['returning_customers_count'] / (df['total_customers_interaction'] + 1e-8)
            df['new_customer_ratio'] = df['new_customers_count'] / (df['total_customers_interaction'] + 1e-8)
            
            # Клиентская стабильность
            df['customer_stability_score'] = df['customer_retention_rate'] * (1 - abs(df['new_customer_ratio'] - 0.3))
        
        # Доходность по типам клиентов
        revenue_columns = ['revenue_new_customers', 'revenue_returning_customers', 'revenue_reactivated_customers']
        available_revenue_cols = [col for col in revenue_columns if col in df.columns]
        
        if available_revenue_cols:
            df['total_customer_revenue'] = df[available_revenue_cols].sum(axis=1)
            
            for col in available_revenue_cols:
                ratio_name = col.replace('revenue_', '') + '_revenue_ratio'
                df[ratio_name] = df[col] / (df['total_customer_revenue'] + 1e-8)
        
        # Average revenue per customer type
        if 'revenue_new_customers' in df.columns and 'new_customers_count' in df.columns:
            df['avg_revenue_per_new_customer'] = df['revenue_new_customers'] / (df['new_customers_count'] + 1e-8)
        
        if 'revenue_returning_customers' in df.columns and 'returning_customers_count' in df.columns:
            df['avg_revenue_per_returning_customer'] = df['revenue_returning_customers'] / (df['returning_customers_count'] + 1e-8)
        
        # Customer lifetime value indicators
        if 'avg_revenue_per_returning_customer' in df.columns and 'customer_retention_rate' in df.columns:
            df['customer_lifetime_indicator'] = df['avg_revenue_per_returning_customer'] * df['customer_retention_rate']
        
        logger.info("✅ Создано customer analytics features")
        return df
        
    except Exception as e:
        logger.error(f"Ошибка при создании customer analytics features: {e}")
        return df

def create_operational_efficiency_features(df: pd.DataFrame) -> pd.DataFrame:
    """Создание features на основе операционной эффективности"""
    try:
        # Процесс выполнения заказа (для gojek данных)
        time_fields = ['accepting_time_minutes', 'preparation_time_minutes', 'delivery_time_minutes']
        available_time_fields = [field for field in time_fields if field in df.columns]
        
        if available_time_fields:
            df['total_fulfillment_time'] = df[available_time_fields].sum(axis=1)
            
            # Эффективность по каждому этапу
            for field in available_time_fields:
                efficiency_name = field.replace('_minutes', '_efficiency')
                df[efficiency_name] = df['orders'] / (df[field] + 1e-8)
            
            # Доли времени
            for field in available_time_fields:
                ratio_name = field.replace('_minutes', '_time_ratio')
                df[ratio_name] = df[field] / (df['total_fulfillment_time'] + 1e-8)
        
        # Показатели принятия и выполнения заказов
        if 'accepted_orders_count' in df.columns and 'incoming_orders_count' in df.columns:
            df['order_acceptance_rate'] = df['accepted_orders_count'] / (df['incoming_orders_count'] + 1e-8)
            df['order_rejection_rate'] = 1 - df['order_acceptance_rate']
        
        if 'lost_orders_count' in df.columns and 'incoming_orders_count' in df.columns:
            df['order_loss_rate'] = df['lost_orders_count'] / (df['incoming_orders_count'] + 1e-8)
        
        # Реализация заказов
        if 'order_realization_rate' in df.columns:
            df['unrealized_order_rate'] = 1 - (df['order_realization_rate'] / 100)
        
        # Операционные проблемы
        operational_issues = ['stockout_incidents', 'busy_periods_count', 'store_closed_periods', 
                             'acceptance_timeout_count', 'driver_waiting_incidents']
        available_issues = [col for col in operational_issues if col in df.columns]
        
        if available_issues:
            df['total_operational_issues'] = df[available_issues].sum(axis=1)
            df['operational_stability_score'] = 1 / (1 + df['total_operational_issues'])
            
            # Доли разных типов проблем
            for issue in available_issues:
                ratio_name = issue.replace('_count', '').replace('_incidents', '') + '_issue_ratio'
                df[ratio_name] = df[issue] / (df['total_operational_issues'] + 1e-8)
        
        # Capacity utilization
        if 'marked_ready_count' in df.columns and 'accepted_orders_count' in df.columns:
            df['preparation_completion_rate'] = df['marked_ready_count'] / (df['accepted_orders_count'] + 1e-8)
        
        logger.info("✅ Создано operational efficiency features")
        return df
        
    except Exception as e:
        logger.error(f"Ошибка при создании operational efficiency features: {e}")
        return df

def create_quality_features(df: pd.DataFrame) -> pd.DataFrame:
    """Создание features на основе детализации качества"""
    try:
        # Детализация рейтингов
        rating_columns = ['rating_1_star', 'rating_2_star', 'rating_3_star', 'rating_4_star', 'rating_5_star']
        available_rating_cols = [col for col in rating_columns if col in df.columns]
        
        if available_rating_cols:
            df['total_detailed_ratings'] = df[available_rating_cols].sum(axis=1)
            
            # Вычисляем точный рейтинг на основе распределения
            if len(available_rating_cols) == 5:
                df['calculated_detailed_rating'] = (
                    df['rating_5_star'] * 5 + 
                    df['rating_4_star'] * 4 + 
                    df['rating_3_star'] * 3 + 
                    df['rating_2_star'] * 2 + 
                    df['rating_1_star'] * 1
                ) / (df['total_detailed_ratings'] + 1e-8)
            
            # Процентные доли каждого рейтинга
            for col in available_rating_cols:
                pct_name = col + '_pct'
                df[pct_name] = df[col] / (df['total_detailed_ratings'] + 1e-8)
            
            # Группировка рейтингов
            if 'rating_1_star' in df.columns and 'rating_2_star' in df.columns:
                df['negative_ratings_count'] = df['rating_1_star'] + df['rating_2_star']
                df['negative_rating_ratio'] = df['negative_ratings_count'] / (df['total_detailed_ratings'] + 1e-8)
            
            if 'rating_4_star' in df.columns and 'rating_5_star' in df.columns:
                df['positive_ratings_count'] = df['rating_4_star'] + df['rating_5_star']
                df['positive_rating_ratio'] = df['positive_ratings_count'] / (df['total_detailed_ratings'] + 1e-8)
            
            if 'rating_3_star' in df.columns:
                df['neutral_rating_ratio'] = df['rating_3_star'] / (df['total_detailed_ratings'] + 1e-8)
            
            # Quality consistency
            if len(available_rating_cols) >= 3:
                # Стандартное отклонение рейтингов как мера консистентности
                rating_values = []
                for _, row in df.iterrows():
                    ratings_dist = []
                    for i, col in enumerate(available_rating_cols, 1):
                        ratings_dist.extend([i] * int(row[col]))
                    
                    if ratings_dist:
                        rating_values.append(np.std(ratings_dist))
                    else:
                        rating_values.append(0)
                
                df['rating_consistency_score'] = 1 / (1 + np.array(rating_values))
        
        logger.info("✅ Создано quality features")
        return df
        
    except Exception as e:
        logger.error(f"Ошибка при создании quality features: {e}")
        return df

def create_marketing_features(df: pd.DataFrame) -> pd.DataFrame:
    """Создание features на основе маркетинговых данных"""
    try:
        # Basic ROAS and ad effectiveness
        if 'ads_sales' in df.columns and 'ads_spend' in df.columns:
            df['roas'] = df['ads_sales'] / (df['ads_spend'] + 1e-8)
            df['ads_on'] = (df['ads_spend'] > 0).astype(int)
            df['ad_investment_intensity'] = df['ads_spend'] / (df['total_sales'] + 1e-8)
        
        # Impression and conversion metrics
        if 'ad_impressions' in df.columns:
            df['cost_per_impression'] = df['ads_spend'] / (df['ad_impressions'] + 1e-8)
            df['impression_to_order_rate'] = df['orders'] / (df['ad_impressions'] + 1e-8)
            df['impression_to_sales_rate'] = df['total_sales'] / (df['ad_impressions'] + 1e-8)
        
        # Click-through metrics
        if 'ad_click_through_rate' in df.columns and 'ad_impressions' in df.columns:
            df['estimated_clicks'] = df['ad_impressions'] * df['ad_click_through_rate'] / 100
            df['cost_per_click'] = df['ads_spend'] / (df['estimated_clicks'] + 1e-8)
            df['click_to_order_rate'] = df['orders'] / (df['estimated_clicks'] + 1e-8)
        
        # Funnel metrics
        funnel_columns = ['ad_impressions', 'unique_impressions', 'menu_visits', 'add_to_carts', 'conversion_reach']
        available_funnel_cols = [col for col in funnel_columns if col in df.columns]
        
        if len(available_funnel_cols) >= 2:
            for i in range(len(available_funnel_cols) - 1):
                current_step = available_funnel_cols[i]
                next_step = available_funnel_cols[i + 1]
                
                conversion_rate_name = f'{current_step}_to_{next_step}_rate'
                df[conversion_rate_name] = df[next_step] / (df[current_step] + 1e-8)
        
        # Marketing efficiency score
        marketing_metrics = ['roas', 'impression_to_order_rate', 'cost_per_click']
        available_marketing = [col for col in marketing_metrics if col in df.columns]
        
        if available_marketing:
            # Нормализуем метрики и создаем общий score
            normalized_metrics = []
            for metric in available_marketing:
                normalized = (df[metric] - df[metric].min()) / (df[metric].max() - df[metric].min() + 1e-8)
                normalized_metrics.append(normalized)
            
            df['marketing_efficiency_score'] = np.mean(normalized_metrics, axis=0)
        
        logger.info("✅ Создано marketing features")
        return df
        
    except Exception as e:
        logger.error(f"Ошибка при создании marketing features: {e}")
        return df

def create_lag_features(df: pd.DataFrame, lag_periods: List[int] = [1, 3, 7]) -> pd.DataFrame:
    """Создание lag features для временных рядов"""
    try:
        # Группируем по ресторанам для создания lag features
        lag_columns = ['total_sales', 'orders', 'rating', 'customer_retention_rate', 
                      'operational_stability_score', 'marketing_efficiency_score']
        available_lag_cols = [col for col in lag_columns if col in df.columns]
        
        if not available_lag_cols:
            return df
        
        # Сортируем по дате
        df = df.sort_values(['restaurant_name', 'date'])
        
        for restaurant in df['restaurant_name'].unique():
            mask = df['restaurant_name'] == restaurant
            restaurant_data = df[mask].copy()
            
            for col in available_lag_cols:
                for lag in lag_periods:
                    lag_col_name = f'{col}_lag_{lag}'
                    
                    # Создаем lag feature
                    lagged_values = restaurant_data[col].shift(lag)
                    
                    # Записываем обратно в основной DataFrame
                    df.loc[mask, lag_col_name] = lagged_values.values
                    
                    # Создаем difference feature (текущее значение - lag)
                    if lag == 1:  # Только для lag 1, чтобы не создавать слишком много features
                        diff_col_name = f'{col}_diff_{lag}'
                        diff_values = restaurant_data[col] - lagged_values
                        df.loc[mask, diff_col_name] = diff_values.values
        
        # Заполняем NaN значения для lag features
        lag_feature_columns = [col for col in df.columns if '_lag_' in col or '_diff_' in col]
        df[lag_feature_columns] = df[lag_feature_columns].fillna(0)
        
        logger.info(f"✅ Создано {len(lag_feature_columns)} lag features")
        return df
        
    except Exception as e:
        logger.error(f"Ошибка при создании lag features: {e}")
        return df

def create_interaction_features(df: pd.DataFrame) -> pd.DataFrame:
    """Создание interaction features между различными метриками"""
    try:
        # Weather + Business interactions
        if 'is_rainy' in df.columns and 'total_sales' in df.columns:
            df['sales_rain_interaction'] = df['total_sales'] * df['is_rainy']
        
        if 'is_weekend' in df.columns and 'orders' in df.columns:
            df['orders_weekend_interaction'] = df['orders'] * df['is_weekend']
        
        # Marketing + Weather interactions
        if 'ads_spend' in df.columns and 'is_rainy' in df.columns:
            df['ads_weather_interaction'] = df['ads_spend'] * (1 + df['is_rainy'])
        
        # Quality + Operations interactions
        if 'rating' in df.columns and 'operational_stability_score' in df.columns:
            df['quality_operations_interaction'] = df['rating'] * df['operational_stability_score']
        
        # Customer + Marketing interactions
        if 'customer_retention_rate' in df.columns and 'marketing_efficiency_score' in df.columns:
            df['customer_marketing_interaction'] = df['customer_retention_rate'] * df['marketing_efficiency_score']
        
        # Holiday + Business interactions
        if 'is_holiday' in df.columns and 'total_sales' in df.columns:
            df['sales_holiday_interaction'] = df['total_sales'] * df['is_holiday']
        
        # Platform + Performance interactions
        if 'platform' in df.columns:
            platform_encoded = pd.get_dummies(df['platform'], prefix='platform')
            df = pd.concat([df, platform_encoded], axis=1)
            
            if 'total_sales' in df.columns and 'platform_grab' in df.columns:
                df['sales_grab_interaction'] = df['total_sales'] * df['platform_grab']
        
        logger.info("✅ Создано interaction features")
        return df
        
    except Exception as e:
        logger.error(f"Ошибка при создании interaction features: {e}")
        return df

def create_rolling_features(df: pd.DataFrame, windows: List[int] = [3, 7, 14]) -> pd.DataFrame:
    """Создание rolling (скользящих) features"""
    try:
        # Сортируем по дате
        df = df.sort_values(['restaurant_name', 'date'])
        
        # Колонки для rolling features
        rolling_columns = ['total_sales', 'orders', 'rating', 'roas', 'customer_retention_rate']
        available_rolling_cols = [col for col in rolling_columns if col in df.columns]
        
        if not available_rolling_cols:
            return df
        
        for restaurant in df['restaurant_name'].unique():
            mask = df['restaurant_name'] == restaurant
            restaurant_data = df[mask].copy()
            
            for col in available_rolling_cols:
                for window in windows:
                    # Rolling mean
                    rolling_mean_name = f'{col}_rolling_mean_{window}'
                    rolling_mean = restaurant_data[col].rolling(window=window, min_periods=1).mean()
                    df.loc[mask, rolling_mean_name] = rolling_mean.values
                    
                    # Rolling std (for variability)
                    rolling_std_name = f'{col}_rolling_std_{window}'
                    rolling_std = restaurant_data[col].rolling(window=window, min_periods=1).std().fillna(0)
                    df.loc[mask, rolling_std_name] = rolling_std.values
                    
                    # Rolling trend (difference from rolling mean)
                    trend_name = f'{col}_trend_{window}'
                    trend = restaurant_data[col] - rolling_mean
                    df.loc[mask, trend_name] = trend.values
        
        logger.info(f"✅ Создано rolling features для {len(windows)} окон")
        return df
        
    except Exception as e:
        logger.error(f"Ошибка при создании rolling features: {e}")
        return df

def prepare_features_enhanced(df: pd.DataFrame) -> pd.DataFrame:
    """ГЛАВНАЯ ФУНКЦИЯ: Создание всех расширенных features"""
    try:
        logger.info("🚀 НАЧИНАЕМ СОЗДАНИЕ РАСШИРЕННЫХ FEATURES")
        
        # 1. Weather features
        df = create_weather_features(df)
        
        # 2. Calendar features
        df = create_calendar_features(df)
        
        # 3. Customer analytics features
        df = create_customer_analytics_features(df)
        
        # 4. Operational efficiency features
        df = create_operational_efficiency_features(df)
        
        # 5. Quality features
        df = create_quality_features(df)
        
        # 6. Marketing features
        df = create_marketing_features(df)
        
        # 7. Lag features (временные сдвиги)
        df = create_lag_features(df)
        
        # 8. Rolling features (скользящие окна)
        df = create_rolling_features(df)
        
        # 9. Interaction features
        df = create_interaction_features(df)
        
        # 10. Финальная обработка
        df = df.fillna(0)
        df = df.replace([np.inf, -np.inf], 0)
        
        # Логирование итогов
        total_features = len(df.columns)
        new_features = [col for col in df.columns if any(x in col for x in ['_rate', '_ratio', '_efficiency', '_pct', '_score', '_lag_', '_rolling_', '_trend_', '_interaction'])]
        
        logger.info(f"🎉 СОЗДАНИЕ FEATURES ЗАВЕРШЕНО:")
        logger.info(f"📊 Общее количество полей: {total_features}")
        logger.info(f"🌟 Новых вычисляемых features: {len(new_features)}")
        logger.info(f"📈 Записей обработано: {len(df)}")
        
        return df
        
    except Exception as e:
        logger.error(f"Ошибка при создании расширенных features: {e}")
        return df

# Функция для обратной совместимости
def prepare_features(df: pd.DataFrame) -> pd.DataFrame:
    """Обертка для обратной совместимости"""
    return prepare_features_enhanced(df)

if __name__ == "__main__":
    # Тест создания features
    print("🧪 ТЕСТИРОВАНИЕ РАСШИРЕННОГО FEATURE ENGINEERING")
    
    # Создаем тестовые данные
    test_df = pd.DataFrame({
        'date': pd.date_range('2024-01-01', periods=10),
        'restaurant_name': ['Test Restaurant'] * 10,
        'total_sales': np.random.randint(1000, 5000, 10),
        'orders': np.random.randint(20, 100, 10),
        'rating': np.random.uniform(3.5, 5.0, 10),
        'temperature_celsius': np.random.uniform(25, 35, 10),
        'is_rainy': np.random.choice([0, 1], 10),
        'is_weekend': np.random.choice([0, 1], 10)
    })
    
    print(f"📊 Исходные данные: {len(test_df.columns)} колонок")
    
    enhanced_df = prepare_features_enhanced(test_df)
    
    print(f"🌟 После обработки: {len(enhanced_df.columns)} колонок")
    print(f"🔧 Добавлено: {len(enhanced_df.columns) - len(test_df.columns)} новых features")