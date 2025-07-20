#!/usr/bin/env python3
"""
🔧 ИСПРАВЛЕННЫЙ ЗАГРУЗЧИК ДАННЫХ
Загружает только разрешённые поля согласно DATA_FIELDS_USED.md
"""

import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import logging

logger = logging.getLogger(__name__)

# 📊 БЕЛЫЙ СПИСОК ПОЛЕЙ (согласно DATA_FIELDS_USED.md)
ALLOWED_SALES_FIELDS = {
    # Базовые поля
    'stat_date': 'date',  # переименовываем для унификации
    'platform': 'platform',  # grab/gojek
    
    # Продажи (sales - целевая переменная)
    'sales': 'total_sales',
    'orders': 'orders', 
    'avg_order_value': 'avg_order_value',
    
    # Доставка
    'delivery_time_minutes': 'delivery_time',
    'cancelled_orders': 'cancelled_orders',
    'delivery_success_rate': 'delivery_success_rate',
    
    # Рейтинг
    'customer_rating': 'rating',
    'customer_complaints': 'customer_complaints',
    
    # Клиенты  
    'total_customers': 'total_customers_count',
    'new_customers': 'new_customers_count',
    'returning_customers': 'returning_customers_count',
    
    # Маркетинг
    'marketing_spend': 'marketing_spend',
    'promotion_usage': 'promotion_usage',
    
    # Операционные
    'peak_hour_orders': 'peak_hour_orders',
    'off_peak_orders': 'off_peak_orders', 
    'weekend_sales': 'weekend_sales',
    'weekday_sales': 'weekday_sales',
    'menu_items_sold': 'menu_items_sold',
    'staff_efficiency': 'staff_efficiency',
    'kitchen_capacity_utilization': 'kitchen_capacity_utilization'
}

# ❌ ИСКЛЮЧАЕМЫЕ ПОЛЯ (могут вызывать переобучение)
FORBIDDEN_FIELDS = {
    'id', 'restaurant_id',  # технические ключи
    'most_popular_item',    # текстовое поле
    'seasonal_boost',       # может содержать будущую информацию
    'competitor_impact'     # может содержать будущую информацию
}

def validate_features(df: pd.DataFrame) -> None:
    """Валидация признаков согласно спецификации"""
    
    logger.info("🔍 Валидация признаков...")
    
    # 1. Проверка на утечку данных
    forbidden_future_cols = [col for col in df.columns 
                           if 'lag_minus' in col or 'future' in col]
    if forbidden_future_cols:
        raise ValueError(f"❌ Найдены будущие признаки: {forbidden_future_cols}")
    
    # 2. Проверка на технические поля
    forbidden_tech_cols = [col for col in df.columns 
                          if col in ['id', 'restaurant_id'] and 'lag' not in col]
    if forbidden_tech_cols:
        raise ValueError(f"❌ Найдены технические ID как признаки: {forbidden_tech_cols}")
    
    # 3. Проверка целевой переменной
    if 'total_sales' not in df.columns:
        raise ValueError("❌ Нет целевой переменной 'total_sales'")
    
    # 4. Проверка разумного количества признаков
    feature_cols = [col for col in df.columns if col not in ['date', 'restaurant_name', 'total_sales']]
    feature_count = len(feature_cols)
    
    if not (20 <= feature_count <= 150):
        logger.warning(f"⚠️ Необычное количество признаков: {feature_count}")
    
    logger.info(f"✅ Валидация пройдена. Признаков: {feature_count}")

def load_restaurant_data_fixed(restaurant_name: Optional[str] = None) -> pd.DataFrame:
    """
    Загружает данные ресторанов с использованием только разрешённых полей
    
    Args:
        restaurant_name: Название ресторана (если None - загружает все)
        
    Returns:
        DataFrame с очищенными данными
    """
    
    logger.info(f"📊 Загрузка данных для ресторана: {restaurant_name or 'ВСЕ'}")
    
    try:
        # Подключение к базе данных
        conn = sqlite3.connect('data/database.sqlite')
        
        # Создаём список полей для выборки (исключаем platform - его добавляем вручную)
        grab_fields = [field for field in ALLOWED_SALES_FIELDS.keys() if field != 'platform']
        grab_field_str = ', '.join([f'g.{field}' for field in grab_fields])
        gojek_field_str = ', '.join([f'j.{field}' for field in grab_fields])
        
        # Базовый запрос
        base_query = f"""
        SELECT 
            r.name as restaurant_name,
            {grab_field_str},
            'grab' as platform
        FROM grab_stats g
        JOIN restaurants r ON g.restaurant_id = r.id
        
        UNION ALL
        
        SELECT 
            r.name as restaurant_name,
            {gojek_field_str}, 
            'gojek' as platform
        FROM gojek_stats j
        JOIN restaurants r ON j.restaurant_id = r.id
        """
        
        # Добавляем фильтр по ресторану если нужно
        if restaurant_name:
            # Для UNION запроса нужно добавить условие в каждую часть
            parts = base_query.split("UNION ALL")
            part1 = parts[0].strip() + f" WHERE r.name = '{restaurant_name}'"
            part2 = parts[1].strip() + f" WHERE r.name = '{restaurant_name}'"
            base_query = f"{part1}\n        UNION ALL\n        {part2}"
            
        base_query += " ORDER BY stat_date, restaurant_name, platform"
        
        logger.info(f"🔍 Выполнение запроса...")
        df = pd.read_sql_query(base_query, conn)
        conn.close()
        
        if df.empty:
            logger.warning(f"⚠️ Нет данных для ресторана: {restaurant_name}")
            return df
        
        logger.info(f"✅ Загружено: {len(df)} записей, {len(df.columns)} колонок")
        
        # Переименовываем колонки согласно стандарту
        rename_mapping = {old: new for old, new in ALLOWED_SALES_FIELDS.items() if old != new}
        df = df.rename(columns=rename_mapping)
        
        # Приводим типы данных
        df['date'] = pd.to_datetime(df['date'])
        df['platform'] = df['platform'].astype('category')
        df['restaurant_name'] = df['restaurant_name'].astype('category')
        
        # Заполняем пропуски
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        df[numeric_cols] = df[numeric_cols].fillna(0)
        
        # Проверяем данные на базовые ошибки
        if df['total_sales'].isnull().all():
            raise ValueError("❌ Все значения total_sales равны NULL")
            
        if (df['total_sales'] < 0).any():
            logger.warning("⚠️ Найдены отрицательные продажи, заменяем на 0")
            df.loc[df['total_sales'] < 0, 'total_sales'] = 0
        
        logger.info(f"📊 Итого: {len(df)} записей для {df['restaurant_name'].nunique()} ресторанов")
        logger.info(f"📅 Период: {df['date'].min()} - {df['date'].max()}")
        logger.info(f"💰 Диапазон продаж: {df['total_sales'].min():,.0f} - {df['total_sales'].max():,.0f}")
        
        return df
        
    except Exception as e:
        logger.error(f"❌ Ошибка загрузки данных: {e}")
        raise

def add_weather_data(df: pd.DataFrame) -> pd.DataFrame:
    """Добавляет погодные данные согласно белому списку"""
    
    logger.info("🌦️ Добавление погодных данных...")
    
    # Синтетические погодные данные (в продакшене - из API)
    weather_data = []
    dates = df['date'].dt.date.unique()
    
    for date in dates:
        # Базовые погодные параметры
        base_temp = 28 + np.random.normal(0, 3)  # Среднегодовая температура Бали
        
        weather_record = {
            'date': date,
            'temperature_celsius': base_temp,
            'feels_like_celsius': base_temp + np.random.uniform(-2, 4),
            'humidity_percent': np.random.uniform(60, 90),
            'precipitation_mm': np.random.exponential(1) if np.random.random() < 0.3 else 0,
            'rain_probability': np.random.uniform(0, 1),
            'wind_speed_kmh': np.random.uniform(5, 25),
            'weather_condition': np.random.choice(['clear', 'cloudy', 'rainy'], p=[0.5, 0.3, 0.2])
        }
        
        weather_data.append(weather_record)
    
    weather_df = pd.DataFrame(weather_data)
    weather_df['date'] = pd.to_datetime(weather_df['date'])
    
    # Создаём производные погодные признаки
    weather_df['is_rainy'] = (weather_df['precipitation_mm'] > 1).astype(int)
    weather_df['is_hot'] = (weather_df['temperature_celsius'] > 30).astype(int)
    weather_df['is_humid'] = (weather_df['humidity_percent'] > 80).astype(int)
    weather_df['is_windy'] = (weather_df['wind_speed_kmh'] > 15).astype(int)
    
    # Объединяем с основными данными
    df = df.merge(weather_df, on='date', how='left')
    
    logger.info(f"✅ Добавлено {len(weather_df.columns)-1} погодных признаков")
    return df

def add_calendar_data(df: pd.DataFrame) -> pd.DataFrame:
    """Добавляет календарные данные согласно белому списку"""
    
    logger.info("📅 Добавление календарных данных...")
    
    # Базовые календарные признаки
    df['day_of_week'] = df['date'].dt.dayofweek
    df['month'] = df['date'].dt.month  
    df['quarter'] = df['date'].dt.quarter
    df['week_of_year'] = df['date'].dt.isocalendar().week
    df['day_of_year'] = df['date'].dt.dayofyear
    
    # Паттерны
    df['is_weekend'] = (df['day_of_week'].isin([5, 6])).astype(int)
    df['is_month_start'] = (df['date'].dt.day <= 3).astype(int)
    df['is_month_end'] = (df['date'].dt.day >= 28).astype(int)
    
    # Дни зарплаты (1-е и 15-е число каждого месяца)
    df['is_pay_day'] = (df['date'].dt.day.isin([1, 15])).astype(int)
    
    # Праздники (синтетические - в продакшене из календарного API)
    holiday_dates = [
        '2025-01-01',  # Новый год
        '2025-04-09',  # Страстная пятница  
        '2025-05-01',  # День труда
        '2025-05-09',  # Вознесение
        '2025-06-01',  # Панчасила
        '2025-08-17',  # День независимости
    ]
    
    holiday_dates = pd.to_datetime(holiday_dates)
    df['is_holiday'] = df['date'].isin(holiday_dates).astype(int)
    
    # Туристический сезон (июнь-сентябрь, декабрь-январь)
    df['is_tourist_high_season'] = df['month'].isin([6, 7, 8, 9, 12, 1]).astype(int)
    
    # Цикличные признаки для машинного обучения
    df['sin_day_of_year'] = np.sin(2 * np.pi * df['day_of_year'] / 365)
    df['cos_day_of_year'] = np.cos(2 * np.pi * df['day_of_year'] / 365)
    df['sin_day_of_week'] = np.sin(2 * np.pi * df['day_of_week'] / 7)
    df['cos_day_of_week'] = np.cos(2 * np.pi * df['day_of_week'] / 7)
    
    logger.info(f"✅ Добавлено {15} календарных признаков")
    return df

def get_restaurant_data(restaurant_name: Optional[str] = None) -> pd.DataFrame:
    """
    Основная функция для получения данных ресторана
    
    Args:
        restaurant_name: Название ресторана
        
    Returns:
        DataFrame с полными данными для анализа
    """
    
    try:
        # 1. Загружаем базовые данные
        df = load_restaurant_data_fixed(restaurant_name)
        
        if df.empty:
            return df
        
        # 2. Добавляем внешние данные
        df = add_weather_data(df)
        df = add_calendar_data(df)
        
        # 3. Сортируем и фиксируем порядок
        df = df.sort_values(['restaurant_name', 'platform', 'date']).reset_index(drop=True)
        
        # 4. Валидируем результат
        validate_features(df)
        
        logger.info("✅ Данные успешно загружены и проверены")
        return df
        
    except Exception as e:
        logger.error(f"❌ Ошибка получения данных: {e}")
        raise

def get_multiple_restaurants(restaurant_names: List[str]) -> pd.DataFrame:
    """Загружает данные для нескольких ресторанов"""
    
    logger.info(f"📊 Загрузка данных для {len(restaurant_names)} ресторанов")
    
    all_data = []
    
    for restaurant in restaurant_names:
        try:
            data = get_restaurant_data(restaurant)
            if not data.empty:
                all_data.append(data)
            else:
                logger.warning(f"⚠️ Нет данных для {restaurant}")
        except Exception as e:
            logger.error(f"❌ Ошибка загрузки {restaurant}: {e}")
    
    if not all_data:
        logger.error("❌ Не удалось загрузить данные ни для одного ресторана")
        return pd.DataFrame()
    
    # Объединяем все данные
    combined_df = pd.concat(all_data, ignore_index=True)
    
    logger.info(f"✅ Загружено {len(combined_df)} записей для {combined_df['restaurant_name'].nunique()} ресторанов")
    
    return combined_df

if __name__ == "__main__":
    # Тестирование загрузчика
    import logging
    logging.basicConfig(level=logging.INFO)
    
    print("🧪 ТЕСТ ИСПРАВЛЕННОГО ЗАГРУЗЧИКА ДАННЫХ")
    print("=" * 50)
    
    # Тест 1: Загрузка одного ресторана
    try:
        df = get_restaurant_data("Ika Canggu")
        print(f"✅ Тест 1 пройден: {len(df)} записей")
        print(f"📊 Колонки: {list(df.columns)}")
        print(f"📅 Период: {df['date'].min()} - {df['date'].max()}")
    except Exception as e:
        print(f"❌ Тест 1 провален: {e}")
    
    # Тест 2: Загрузка всех ресторанов
    try:
        df_all = get_restaurant_data()
        print(f"✅ Тест 2 пройден: {len(df_all)} записей, {df_all['restaurant_name'].nunique()} ресторанов")
    except Exception as e:
        print(f"❌ Тест 2 провален: {e}")
    
    print("🏁 Тестирование завершено")