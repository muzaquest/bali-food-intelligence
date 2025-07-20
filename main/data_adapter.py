#!/usr/bin/env python3
"""
🔧 АДАПТЕР ДАННЫХ
Преобразует данные из реальной базы данных в формат, ожидаемый системой аналитики
"""

import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import logging

logger = logging.getLogger(__name__)

def load_restaurant_data(restaurant_name: str = None, start_date: str = None, end_date: str = None) -> pd.DataFrame:
    """
    Загружает данные ресторанов из реальной базы данных (grab_stats + gojek_stats)
    """
    try:
        # Подключение к базе данных в корне
        conn = sqlite3.connect('database.sqlite')
        
        # Получаем данные Grab
        grab_query = '''
        SELECT g.*, r.name as restaurant_name, 'grab' as platform
        FROM grab_stats g
        JOIN restaurants r ON g.restaurant_id = r.id
        '''
        
        # Получаем данные Gojek  
        gojek_query = '''
        SELECT g.*, r.name as restaurant_name, 'gojek' as platform
        FROM gojek_stats g
        JOIN restaurants r ON g.restaurant_id = r.id
        '''
        
        conditions = []
        params_grab = []
        params_gojek = []
        
        # Фильтр по ресторану
        if restaurant_name:
            conditions.append("r.name = ?")
            params_grab.append(restaurant_name)
            params_gojek.append(restaurant_name)
        
        # Фильтр по датам
        if start_date:
            conditions.append("g.stat_date >= ?")
            params_grab.append(start_date)
            params_gojek.append(start_date)
            
        if end_date:
            conditions.append("g.stat_date <= ?")
            params_grab.append(end_date)
            params_gojek.append(end_date)
        
        if conditions:
            condition_str = " WHERE " + " AND ".join(conditions)
            grab_query += condition_str
            gojek_query += condition_str
        
        grab_query += " ORDER BY g.stat_date, g.restaurant_id"
        gojek_query += " ORDER BY g.stat_date, g.restaurant_id"
        
        # Выполняем запросы
        grab_df = pd.read_sql_query(grab_query, conn, params=params_grab)
        gojek_df = pd.read_sql_query(gojek_query, conn, params=params_gojek)
        
        conn.close()
        
        if grab_df.empty and gojek_df.empty:
            logger.warning("Данные не найдены для указанных параметров")
            return pd.DataFrame()
        
        # Объединяем данные
        combined_df = pd.concat([grab_df, gojek_df], ignore_index=True)
        
        # Преобразуем в ожидаемый формат
        df_transformed = transform_real_data_format(combined_df)
        
        logger.info(f"Загружено {len(df_transformed)} записей для {df_transformed['restaurant_name'].nunique()} ресторанов")
        
        return df_transformed
        
    except Exception as e:
        logger.error(f"Ошибка при загрузке данных: {e}")
        return pd.DataFrame()

def transform_real_data_format(df: pd.DataFrame) -> pd.DataFrame:
    """
    Преобразует реальные данные в ожидаемый формат системы
    """
    if df.empty:
        return df
    
    # Переименовываем поля для соответствия ожидаемому формату
    column_mapping = {
        'stat_date': 'date',
        'sales': 'total_sales',
        'orders': 'orders',
        'rating': 'rating',
        'ads_sales': 'marketing_sales',
        'ads_spend': 'marketing_spend',
        'ads_orders': 'marketing_orders',
        'offline_rate': 'offline_rate',
        'cancelation_rate': 'cancel_rate'
    }
    
    # Применяем переименование
    df_transformed = df.copy()
    for old_col, new_col in column_mapping.items():
        if old_col in df_transformed.columns:
            df_transformed[new_col] = df_transformed[old_col]
    
    # Добавляем недостающие поля
    df_transformed = add_calculated_fields_real(df_transformed)
    
    # Преобразуем типы данных
    df_transformed['date'] = pd.to_datetime(df_transformed['date'])
    
    # Убираем исходные поля, которые больше не нужны
    columns_to_keep = [
        'date', 'restaurant_name', 'platform', 'total_sales', 'orders', 'rating',
        'marketing_spend', 'marketing_sales', 'marketing_orders', 'cancel_rate',
        'avg_order_value', 'delivery_time', 'cancelled_orders', 'ads_on', 'roas',
        'weather_condition', 'temperature_celsius', 'humidity_percent', 'precipitation_mm',
        'is_rainy', 'is_hot', 'is_windy', 'is_weekend', 'is_holiday', 
        'is_tourist_high_season', 'is_pay_day', 'day_of_week', 'month', 'year',
        'month_end', 'month_start', 'delivery_success_rate', 'revenue_per_order',
        'sales_per_hour', 'total_customers_count', 'new_customers_count', 
        'returning_customers_count', 'peak_hour_orders', 'off_peak_orders',
        'weekend_sales', 'weekday_sales', 'menu_items_sold', 'promotion_usage',
        'customer_complaints', 'location', 'cuisine_type'
    ]
    
    # Оставляем только нужные колонки
    available_columns = [col for col in columns_to_keep if col in df_transformed.columns]
    df_transformed = df_transformed[available_columns]
    
    return df_transformed

def add_calculated_fields_real(df: pd.DataFrame) -> pd.DataFrame:
    """
    Добавляет вычисляемые поля для реальных данных
    """
    # Основные вычисляемые поля
    df['avg_order_value'] = df['total_sales'] / df['orders'].replace(0, 1)
    df['cancelled_orders'] = (df['orders'] * df.get('cancel_rate', 0.05) / 100).astype(int)
    
    # Маркетинг
    df['ads_on'] = (df.get('marketing_spend', 0) > 0).astype(int)
    df['roas'] = df.get('marketing_sales', 0) / df.get('marketing_spend', 1).replace(0, 1)
    
    # Время доставки (оценочное на основе платформы)
    np.random.seed(42)  # Для воспроизводимости
    base_delivery_time = np.where(df['platform'] == 'grab', 35, 30)
    df['delivery_time'] = base_delivery_time + np.random.normal(0, 5, len(df))
    df['delivery_time'] = np.clip(df['delivery_time'], 15, 60).astype(int)
    
    # Погодные условия (симулируем)
    weather_conditions = ['Sunny', 'Partly Cloudy', 'Cloudy', 'Rainy', 'Stormy']
    weather_probs = [0.4, 0.25, 0.2, 0.1, 0.05]  # Вероятности для Бали
    
    df['weather_condition'] = np.random.choice(weather_conditions, len(df), p=weather_probs)
    df['temperature_celsius'] = np.random.normal(28, 3, len(df))
    df['humidity_percent'] = np.random.normal(75, 10, len(df))
    df['precipitation_mm'] = np.where(
        df['weather_condition'].isin(['Rainy', 'Stormy']),
        np.random.exponential(5, len(df)),
        0
    )
    
    # Погодные флаги
    df['is_rainy'] = (df['weather_condition'] == 'Rainy').astype(int)
    df['is_hot'] = (df['temperature_celsius'] > 30).astype(int)
    df['is_windy'] = 0  # Нет данных о ветре
    
    # Календарные данные
    df['date_temp'] = pd.to_datetime(df['date'])
    df['is_weekend'] = (df['date_temp'].dt.dayofweek >= 5).astype(int)
    df['is_holiday'] = 0  # Можно добавить логику для праздников
    df['is_tourist_high_season'] = df['date_temp'].dt.month.isin([7, 8, 12, 1]).astype(int)
    df['is_pay_day'] = df['date_temp'].dt.day.isin([25, 26, 27, 28, 29, 30, 31, 1, 2]).astype(int)
    df['day_of_week'] = df['date_temp'].dt.dayofweek
    df['month'] = df['date_temp'].dt.month
    df['year'] = df['date_temp'].dt.year
    df['month_end'] = (df['date_temp'].dt.day >= 28).astype(int)
    df['month_start'] = (df['date_temp'].dt.day <= 3).astype(int)
    
    # Дополнительные метрики
    df['delivery_success_rate'] = 1 - (df.get('cancel_rate', 5) / 100)
    df['revenue_per_order'] = df['avg_order_value']
    df['sales_per_hour'] = df['total_sales'] / 24
    
    # Customer метрики (оценочные)
    df['total_customers_count'] = (df['orders'] * 0.8).astype(int)
    df['new_customers_count'] = (df['orders'] * 0.2).astype(int)
    df['returning_customers_count'] = df['total_customers_count'] - df['new_customers_count']
    
    # Пиковые часы (оценочные)
    df['peak_hour_orders'] = (df['orders'] * 0.6).astype(int)
    df['off_peak_orders'] = df['orders'] - df['peak_hour_orders']
    
    # Продажи по дням
    weekend_multiplier = df['is_weekend'].apply(lambda x: 1.2 if x else 0.8)
    df['weekend_sales'] = df['total_sales'] * weekend_multiplier
    df['weekday_sales'] = df['total_sales'] * (2 - weekend_multiplier)
    
    # Меню
    df['menu_items_sold'] = (df['orders'] * np.random.uniform(1.2, 2.5, len(df))).astype(int)
    
    # Промо
    df['promotion_usage'] = np.random.uniform(0.1, 0.4, len(df))
    
    # Жалобы (оценочные)
    df['customer_complaints'] = (df['orders'] * (5 - df['rating']) / 20).astype(int)
    
    # Добавляем информацию о ресторане (можно расширить)
    df['location'] = 'Bali'
    df['cuisine_type'] = 'Various'
    
    # Убираем временную колонку
    if 'date_temp' in df.columns:
        df = df.drop('date_temp', axis=1)
    
    return df

def get_available_restaurants() -> List[Dict]:
    """
    Возвращает список доступных ресторанов из реальной базы данных
    """
    try:
        conn = sqlite3.connect('database.sqlite')
        
        query = '''
        SELECT r.name,
               COUNT(DISTINCT COALESCE(g.stat_date, gj.stat_date)) as days_data,
               MIN(COALESCE(g.stat_date, gj.stat_date)) as first_date,
               MAX(COALESCE(g.stat_date, gj.stat_date)) as last_date,
               COUNT(g.id) as grab_records,
               COUNT(gj.id) as gojek_records
        FROM restaurants r
        LEFT JOIN grab_stats g ON r.id = g.restaurant_id
        LEFT JOIN gojek_stats gj ON r.id = gj.restaurant_id
        WHERE g.id IS NOT NULL OR gj.id IS NOT NULL
        GROUP BY r.id, r.name
        ORDER BY r.name
        '''
        
        df = pd.read_sql_query(query, conn)
        conn.close()
        
        return df.to_dict('records')
        
    except Exception as e:
        logger.error(f"Ошибка при получении списка ресторанов: {e}")
        return []

def validate_data_structure() -> bool:
    """
    Проверяет структуру реальной базы данных
    """
    try:
        conn = sqlite3.connect('database.sqlite')
        
        # Проверяем наличие таблиц
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [row[0] for row in cursor.fetchall()]
        
        required_tables = ['restaurants', 'grab_stats', 'gojek_stats']
        missing_tables = [t for t in required_tables if t not in tables]
        
        if missing_tables:
            logger.error(f"Отсутствуют таблицы: {missing_tables}")
            return False
        
        # Проверяем данные
        cursor.execute("SELECT COUNT(*) FROM grab_stats")
        grab_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM gojek_stats")
        gojek_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM restaurants")
        restaurant_count = cursor.fetchone()[0]
        
        conn.close()
        
        logger.info(f"✅ Структура данных корректна: {restaurant_count} ресторанов, {grab_count} записей Grab, {gojek_count} записей Gojek")
        return True
        
    except Exception as e:
        logger.error(f"Ошибка при проверке структуры данных: {e}")
        return False