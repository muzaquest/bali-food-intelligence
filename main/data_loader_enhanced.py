#!/usr/bin/env python3
"""
Улучшенный загрузчик данных с использованием ВСЕХ полей базы данных
+ интеграция с погодой и календарем
"""

import sqlite3
import pandas as pd
import numpy as np
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import requests
import json
try:
    from config import DATABASE_PATH
except ImportError:
    DATABASE_PATH = "data/database.sqlite"

logger = logging.getLogger(__name__)

# Mapping полей для стандартизации названий
GRAB_FIELD_MAPPING = {
    # Основные данные
    'stat_date': 'date',
    'sales': 'total_sales',
    'orders': 'orders',
    'customer_rating': 'rating',
    
    # Реклама и маркетинг
    'ads_sales': 'ads_sales',
    'ads_orders': 'ads_orders',
    'ads_spend': 'ads_spend',
    'ads_ctr': 'ad_click_through_rate',
    'impressions': 'ad_impressions',
    'unique_impressions_reach': 'unique_impressions',
    'unique_menu_visits': 'menu_visits',
    'unique_add_to_carts': 'add_to_carts',
    'unique_conversion_reach': 'conversion_reach',
    
    # Операционные метрики
    'cancelation_rate': 'cancel_rate',
    'offline_rate': 'offline_rate',
    'cancelled_orders': 'cancelled_orders_count',
    'store_is_closed': 'store_closed_periods',
    'store_is_busy': 'busy_periods_count',
    'store_is_closing_soon': 'closing_soon_periods',
    'out_of_stock': 'stockout_incidents',
    
    # Клиентская аналитика
    'new_customers': 'new_customers_count',
    'repeated_customers': 'returning_customers_count',
    'reactivated_customers': 'reactivated_customers_count',
    'total_customers': 'total_customers_count',
    'earned_new_customers': 'revenue_new_customers',
    'earned_repeated_customers': 'revenue_returning_customers',
    'earned_reactivated_customers': 'revenue_reactivated_customers',
    
    # Доп. данные
    'payouts': 'payouts',
    'restaurant_id': 'restaurant_id'
}

GOJEK_FIELD_MAPPING = {
    # Основные данные
    'stat_date': 'date',
    'sales': 'total_sales',
    'orders': 'orders',
    'customer_rating': 'rating',
    
    # Временные метрики
    'accepting_time': 'accepting_time_minutes',
    'preparation_time': 'preparation_time_minutes', 
    'delivery_time': 'delivery_time_minutes',
    
    # Реклама
    'ads_sales': 'ads_sales',
    'ads_orders': 'ads_orders',
    'ads_spend': 'ads_spend',
    
    # Заказы и операции
    'lost_orders': 'lost_orders_count',
    'accepted_orders': 'accepted_orders_count',
    'incoming_orders': 'incoming_orders_count',
    'cancelled_orders': 'cancelled_orders_count',
    'realized_orders_percentage': 'order_realization_rate',
    'acceptance_timeout': 'acceptance_timeout_count',
    'marked_ready': 'marked_ready_count',
    
    # Детализация рейтингов
    'one_star_ratings': 'rating_1_star',
    'two_star_ratings': 'rating_2_star',
    'three_star_ratings': 'rating_3_star',
    'four_star_ratings': 'rating_4_star',
    'five_star_ratings': 'rating_5_star',
    
    # Статусы и проблемы
    'store_is_busy': 'busy_periods_count',
    'store_is_closed': 'store_closed_periods',
    'out_of_stock': 'stockout_incidents',
    'close_time': 'manual_close_periods',
    'driver_waiting': 'driver_waiting_incidents',
    'potential_lost': 'potential_lost_orders',
    
    # Клиенты
    'new_client': 'new_customers_count',
    'active_client': 'active_customers_count',
    'returned_client': 'returning_customers_count',
    
    # Доп. данные
    'payouts': 'payouts',
    'restaurant_id': 'restaurant_id'
}

def get_table_columns(db_path: str, table_name: str) -> List[str]:
    """Получить список всех колонок в таблице"""
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute(f"PRAGMA table_info({table_name})")
        columns = [row[1] for row in cursor.fetchall() if row[1] not in ['id', 'created_at']]
        conn.close()
        return columns
    except Exception as e:
        logger.error(f"Ошибка при получении колонок {table_name}: {e}")
        return []

def convert_time_to_minutes(time_value) -> float:
    """Конвертировать TIME в минуты"""
    if pd.isna(time_value) or time_value is None:
        return 0.0
    
    if isinstance(time_value, str):
        try:
            # Формат: HH:MM:SS или MM:SS
            parts = time_value.split(':')
            if len(parts) == 3:  # HH:MM:SS
                hours, minutes, seconds = map(int, parts)
                return hours * 60 + minutes + seconds / 60
            elif len(parts) == 2:  # MM:SS
                minutes, seconds = map(int, parts)
                return minutes + seconds / 60
        except:
            return 0.0
    
    return float(time_value) if time_value else 0.0

def load_grab_stats_enhanced(db_path: str) -> pd.DataFrame:
    """Загрузка ВСЕХ доступных полей из grab_stats"""
    try:
        conn = sqlite3.connect(db_path)
        
        # Получить все доступные колонки
        available_columns = get_table_columns(db_path, 'grab_stats')
        logger.info(f"Найдено {len(available_columns)} колонок в grab_stats")
        
        # Загружаем названия ресторанов
        restaurants_query = "SELECT id, name FROM restaurants ORDER BY name"
        restaurants_df = pd.read_sql_query(restaurants_query, conn)
        
        # Формируем динамический запрос для всех доступных полей
        select_fields = []
        for col in available_columns:
            if col in GRAB_FIELD_MAPPING:
                select_fields.append(f"g.{col}")
        
        query = f"""
        SELECT {', '.join(select_fields)}
        FROM grab_stats g
        WHERE g.sales > 0 AND g.restaurant_id IS NOT NULL
        ORDER BY g.restaurant_id, g.stat_date
        """
        
        df = pd.read_sql_query(query, conn)
        logger.info(f"Загружено {len(df)} записей из grab_stats с {len(df.columns)} полями")
        
        # Применяем mapping полей
        df = df.rename(columns=GRAB_FIELD_MAPPING)
        
        # Объединяем с названиями ресторанов
        df = df.merge(restaurants_df, left_on='restaurant_id', right_on='id', how='left')
        df['restaurant_name'] = df['name']
        df = df.drop(['restaurant_id', 'id', 'name'], axis=1)
        
        # Конвертируем дату
        df['date'] = pd.to_datetime(df['date'])
        
        # Обрабатываем числовые поля
        numeric_columns = [col for col in df.columns if col not in ['date', 'restaurant_name']]
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        
        # Добавляем платформу
        df['platform'] = 'grab'
        
        conn.close()
        
        logger.info(f"Успешно загружены расширенные данные grab_stats: {len(df)} записей")
        return df
        
    except Exception as e:
        logger.error(f"Ошибка при загрузке расширенных grab_stats: {e}")
        return pd.DataFrame()

def load_gojek_stats_enhanced(db_path: str) -> pd.DataFrame:
    """Загрузка ВСЕХ доступных полей из gojek_stats"""
    try:
        conn = sqlite3.connect(db_path)
        
        # Проверяем существование таблицы
        tables = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='gojek_stats'").fetchall()
        if not tables:
            logger.info("Таблица gojek_stats не найдена")
            conn.close()
            return pd.DataFrame()
        
        # Получить все доступные колонки
        available_columns = get_table_columns(db_path, 'gojek_stats')
        logger.info(f"Найдено {len(available_columns)} колонок в gojek_stats")
        
        # Загружаем названия ресторанов
        restaurants_query = "SELECT id, name FROM restaurants ORDER BY name"
        restaurants_df = pd.read_sql_query(restaurants_query, conn)
        
        # Формируем динамический запрос
        select_fields = []
        for col in available_columns:
            if col in GOJEK_FIELD_MAPPING:
                select_fields.append(f"g.{col}")
        
        query = f"""
        SELECT {', '.join(select_fields)}
        FROM gojek_stats g
        WHERE g.sales > 0 AND g.restaurant_id IS NOT NULL
        ORDER BY g.restaurant_id, g.stat_date
        """
        
        df = pd.read_sql_query(query, conn)
        logger.info(f"Загружено {len(df)} записей из gojek_stats с {len(df.columns)} полями")
        
        # Применяем mapping полей
        df = df.rename(columns=GOJEK_FIELD_MAPPING)
        
        # Объединяем с названиями ресторанов
        df = df.merge(restaurants_df, left_on='restaurant_id', right_on='id', how='left')
        df['restaurant_name'] = df['name']
        df = df.drop(['restaurant_id', 'id', 'name'], axis=1)
        
        # Конвертируем дату
        df['date'] = pd.to_datetime(df['date'])
        
        # Конвертируем временные поля в минуты
        time_fields = ['accepting_time_minutes', 'preparation_time_minutes', 'delivery_time_minutes']
        for field in time_fields:
            if field in df.columns:
                df[field] = df[field].apply(convert_time_to_minutes)
        
        # Обрабатываем числовые поля
        numeric_columns = [col for col in df.columns if col not in ['date', 'restaurant_name']]
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        
        # Добавляем платформу
        df['platform'] = 'gojek'
        
        conn.close()
        
        logger.info(f"Успешно загружены расширенные данные gojek_stats: {len(df)} записей")
        return df
        
    except Exception as e:
        logger.error(f"Ошибка при загрузке расширенных gojek_stats: {e}")
        return pd.DataFrame()

def get_weather_data(start_date: str, end_date: str, location: str = "Canggu,Bali") -> pd.DataFrame:
    """Получить данные о погоде (или создать синтетические)"""
    try:
        # Создаем синтетические данные о погоде для демонстрации
        # В реальной системе здесь был бы API вызов
        
        date_range = pd.date_range(start=start_date, end=end_date, freq='D')
        
        # Генерируем реалистичные данные о погоде для Бали
        np.random.seed(42)  # Для воспроизводимости
        
        weather_data = []
        for date in date_range:
            # Сезонность: сухой сезон (май-октябрь) vs дождливый (ноябрь-апрель)
            month = date.month
            is_dry_season = month in [5, 6, 7, 8, 9, 10]
            
            # Базовые параметры в зависимости от сезона
            if is_dry_season:
                base_temp = 29
                rain_chance = 0.2
                humidity_base = 70
            else:
                base_temp = 27
                rain_chance = 0.6
                humidity_base = 80
            
            # Генерируем данные с вариацией
            temperature = base_temp + np.random.normal(0, 2)
            humidity = humidity_base + np.random.normal(0, 10)
            precipitation = 0 if np.random.random() > rain_chance else np.random.exponential(5)
            wind_speed = np.random.exponential(10)
            
            # Условия погоды
            if precipitation > 10:
                condition = 'rainy'
            elif humidity > 85:
                condition = 'humid'
            elif temperature > 32:
                condition = 'hot'
            elif wind_speed > 15:
                condition = 'windy'
            else:
                condition = 'clear'
            
            weather_data.append({
                'date': date,
                'temperature_celsius': round(temperature, 1),
                'humidity_percent': round(max(30, min(100, humidity)), 1),
                'precipitation_mm': round(precipitation, 1),
                'wind_speed_kmh': round(wind_speed, 1),
                'weather_condition': condition,
                'is_rainy': precipitation > 1,
                'is_hot': temperature > 32,
                'is_humid': humidity > 85,
                'is_windy': wind_speed > 15
            })
        
        weather_df = pd.DataFrame(weather_data)
        logger.info(f"Создано {len(weather_df)} записей погодных данных")
        return weather_df
        
    except Exception as e:
        logger.error(f"Ошибка при создании погодных данных: {e}")
        return pd.DataFrame()

def get_calendar_data(start_date: str, end_date: str) -> pd.DataFrame:
    """Получить календарные данные и праздники"""
    try:
        date_range = pd.date_range(start=start_date, end=end_date, freq='D')
        
        # Праздники в Индонезии и Бали (основные)
        indonesian_holidays = {
            '2024-01-01': 'New Year',
            '2024-02-08': 'Lunar New Year',
            '2024-02-12': 'Lunar New Year Holiday',
            '2024-03-11': 'Nyepi (Balinese New Year)',
            '2024-03-29': 'Good Friday', 
            '2024-05-01': 'Labor Day',
            '2024-05-09': 'Buddha Day',
            '2024-05-23': 'Ascension Day',
            '2024-06-01': 'Pancasila Day',
            '2024-06-17': 'Eid al-Adha',
            '2024-07-07': 'Islamic New Year',
            '2024-08-17': 'Independence Day',
            '2024-09-16': 'Prophet Muhammad Birthday',
            '2024-12-25': 'Christmas',
            '2025-01-01': 'New Year',
            '2025-01-29': 'Lunar New Year',
            '2025-03-29': 'Nyepi (Balinese New Year)',
            '2025-04-18': 'Good Friday',
            '2025-05-01': 'Labor Day',
            '2025-05-29': 'Buddha Day',
            '2025-06-01': 'Pancasila Day',
            '2025-06-05': 'Eid al-Adha',
            '2025-06-26': 'Islamic New Year',
            '2025-08-17': 'Independence Day',
            '2025-09-05': 'Prophet Muhammad Birthday',
            '2025-12-25': 'Christmas'
        }
        
        calendar_data = []
        for date in date_range:
            date_str = date.strftime('%Y-%m-%d')
            
            # Основная информация
            day_of_week = date.dayofweek  # 0=Monday, 6=Sunday
            is_weekend = day_of_week >= 5
            is_holiday = date_str in indonesian_holidays
            holiday_name = indonesian_holidays.get(date_str, '')
            
            # Дополнительная категоризация
            month = date.month
            day = date.day
            
            # Сезоны в Бали
            is_dry_season = month in [5, 6, 7, 8, 9, 10]
            is_tourist_high_season = month in [7, 8, 12, 1]  # Летние каникулы + Новый год
            
            # Специальные периоды
            is_month_start = day <= 3
            is_month_end = day >= 28
            is_pay_day = day in [1, 15]  # Дни зарплаты
            
            # Культурные особенности Бали
            is_full_moon = (date.day - 1) % 29 == 14  # Приблизительно
            is_galungan_period = False  # Упрощено, в реальности сложнее
            
            # День недели категории
            if day_of_week == 0:
                day_category = 'monday'
            elif day_of_week == 4:
                day_category = 'friday'  
            elif day_of_week == 5:
                day_category = 'saturday'
            elif day_of_week == 6:
                day_category = 'sunday'
            else:
                day_category = 'weekday'
            
            calendar_data.append({
                'date': date,
                'day_of_week': day_of_week,
                'day_name': date.strftime('%A'),
                'month': month,
                'month_name': date.strftime('%B'),
                'day': day,
                'is_weekend': is_weekend,
                'is_holiday': is_holiday,
                'holiday_name': holiday_name,
                'is_dry_season': is_dry_season,
                'is_tourist_high_season': is_tourist_high_season,
                'is_month_start': is_month_start,
                'is_month_end': is_month_end,
                'is_pay_day': is_pay_day,
                'is_full_moon': is_full_moon,
                'day_category': day_category,
                'week_of_year': date.isocalendar()[1],
                'quarter': (month - 1) // 3 + 1
            })
        
        calendar_df = pd.DataFrame(calendar_data)
        logger.info(f"Создано {len(calendar_df)} записей календарных данных")
        return calendar_df
        
    except Exception as e:
        logger.error(f"Ошибка при создании календарных данных: {e}")
        return pd.DataFrame()

def create_enhanced_features(df: pd.DataFrame) -> pd.DataFrame:
    """Создание расширенных features из всех доступных полей"""
    try:
        # КЛИЕНТСКАЯ АНАЛИТИКА
        if 'new_customers_count' in df.columns and 'returning_customers_count' in df.columns:
            df['total_customers_calc'] = df['new_customers_count'] + df['returning_customers_count']
            df['customer_retention_rate'] = df['returning_customers_count'] / (df['total_customers_calc'] + 1e-8)
            df['new_customer_ratio'] = df['new_customers_count'] / (df['total_customers_calc'] + 1e-8)
        
        # КАЧЕСТВО ОБСЛУЖИВАНИЯ (детализация рейтингов)
        rating_columns = ['rating_1_star', 'rating_2_star', 'rating_3_star', 'rating_4_star', 'rating_5_star']
        if all(col in df.columns for col in rating_columns):
            df['total_ratings'] = df[rating_columns].sum(axis=1)
            
            # Вычисляем точный рейтинг на основе распределения
            df['calculated_rating'] = (
                df['rating_5_star'] * 5 + 
                df['rating_4_star'] * 4 + 
                df['rating_3_star'] * 3 + 
                df['rating_2_star'] * 2 + 
                df['rating_1_star'] * 1
            ) / (df['total_ratings'] + 1e-8)
            
            # Доли каждого рейтинга
            for i in range(1, 6):
                df[f'rating_{i}_star_pct'] = df[f'rating_{i}_star'] / (df['total_ratings'] + 1e-8)
            
            # Негативные отзывы
            df['negative_rating_ratio'] = (df['rating_1_star'] + df['rating_2_star']) / (df['total_ratings'] + 1e-8)
            df['positive_rating_ratio'] = (df['rating_4_star'] + df['rating_5_star']) / (df['total_ratings'] + 1e-8)
        
        # ОПЕРАЦИОННАЯ ЭФФЕКТИВНОСТЬ
        if 'accepted_orders_count' in df.columns and 'incoming_orders_count' in df.columns:
            df['order_acceptance_rate'] = df['accepted_orders_count'] / (df['incoming_orders_count'] + 1e-8)
        
        if 'cancelled_orders_count' in df.columns:
            df['order_completion_rate'] = (df['orders'] - df['cancelled_orders_count']) / (df['orders'] + 1e-8)
        
        # МАРКЕТИНГОВАЯ ЭФФЕКТИВНОСТЬ
        if 'ad_impressions' in df.columns and 'ads_spend' in df.columns:
            df['cost_per_impression'] = df['ads_spend'] / (df['ad_impressions'] + 1e-8)
            df['impression_to_order_rate'] = df['orders'] / (df['ad_impressions'] + 1e-8)
        
        if 'ad_click_through_rate' in df.columns and 'ad_impressions' in df.columns:
            df['estimated_clicks'] = df['ad_impressions'] * df['ad_click_through_rate'] / 100
            df['cost_per_click'] = df['ads_spend'] / (df['estimated_clicks'] + 1e-8)
        
        # ВРЕМЕННАЯ ЭФФЕКТИВНОСТЬ (для gojek)
        time_fields = ['accepting_time_minutes', 'preparation_time_minutes', 'delivery_time_minutes']
        if all(field in df.columns for field in time_fields):
            df['total_fulfillment_time'] = df[time_fields].sum(axis=1)
            df['preparation_efficiency'] = df['orders'] / (df['preparation_time_minutes'] + 1e-8)
            df['delivery_efficiency'] = df['orders'] / (df['delivery_time_minutes'] + 1e-8)
        
        # ФИНАНСОВЫЕ МЕТРИКИ
        if 'total_sales' in df.columns and 'orders' in df.columns:
            df['avg_order_value'] = df['total_sales'] / (df['orders'] + 1e-8)
        
        if 'ads_sales' in df.columns and 'ads_spend' in df.columns:
            df['roas'] = df['ads_sales'] / (df['ads_spend'] + 1e-8)
            df['ads_on'] = (df['ads_spend'] > 0).astype(int)
        
        # ПРОБЛЕМНЫЕ МЕТРИКИ
        operational_issues = ['stockout_incidents', 'busy_periods_count', 'store_closed_periods']
        available_issues = [col for col in operational_issues if col in df.columns]
        if available_issues:
            df['total_operational_issues'] = df[available_issues].sum(axis=1)
            df['operational_stability_score'] = 1 / (1 + df['total_operational_issues'])
        
        logger.info(f"Создано {len([col for col in df.columns if '_rate' in col or '_ratio' in col or '_efficiency' in col])} новых вычисляемых метрик")
        return df
        
    except Exception as e:
        logger.error(f"Ошибка при создании расширенных features: {e}")
        return df

def load_data_enhanced(db_path: str = None) -> pd.DataFrame:
    """Загрузка ВСЕХ данных с максимальной детализацией"""
    if db_path is None:
        db_path = DATABASE_PATH
    
    logger.info(f"🚀 ЗАГРУЗКА МАКСИМАЛЬНО ДЕТАЛИЗИРОВАННЫХ ДАННЫХ из {db_path}")
    
    try:
        # Загружаем расширенные данные из всех источников
        grab_df = load_grab_stats_enhanced(db_path)
        gojek_df = load_gojek_stats_enhanced(db_path)
        
        if grab_df.empty and gojek_df.empty:
            logger.error("Не удалось загрузить данные ни из одного источника")
            return pd.DataFrame()
        
        # Объединяем данные
        all_dfs = []
        if not grab_df.empty:
            all_dfs.append(grab_df)
        if not gojek_df.empty:
            all_dfs.append(gojek_df)
        
        # Приводим к общему формату
        combined_df = pd.concat(all_dfs, ignore_index=True, sort=False)
        
        # Определяем диапазон дат
        start_date = combined_df['date'].min().strftime('%Y-%m-%d')
        end_date = combined_df['date'].max().strftime('%Y-%m-%d')
        
        logger.info(f"📅 Диапазон данных: {start_date} - {end_date}")
        
        # Добавляем погодные данные
        weather_df = get_weather_data(start_date, end_date)
        if not weather_df.empty:
            combined_df = combined_df.merge(weather_df, on='date', how='left')
            logger.info("✅ Добавлены погодные данные")
        
        # Добавляем календарные данные
        calendar_df = get_calendar_data(start_date, end_date)
        if not calendar_df.empty:
            combined_df = combined_df.merge(calendar_df, on='date', how='left')
            logger.info("✅ Добавлены календарные данные")
        
        # Создаем расширенные features
        combined_df = create_enhanced_features(combined_df)
        logger.info("✅ Созданы расширенные вычисляемые метрики")
        
        # Финальная обработка
        combined_df = combined_df.fillna(0)
        combined_df = combined_df.replace([np.inf, -np.inf], 0)
        
        logger.info(f"🎉 ЗАГРУЗКА ЗАВЕРШЕНА: {len(combined_df)} записей с {len(combined_df.columns)} полями")
        logger.info(f"📊 Уникальных ресторанов: {combined_df['restaurant_name'].nunique()}")
        logger.info(f"📈 Платформы: {combined_df['platform'].value_counts().to_dict()}")
        
        return combined_df
        
    except Exception as e:
        logger.error(f"Ошибка при загрузке расширенных данных: {e}")
        return pd.DataFrame()

# Обратная совместимость - функции-обертки
def load_data_for_training(db_path: str = None) -> pd.DataFrame:
    """Обертка для обратной совместимости"""
    return load_data_enhanced(db_path)

def get_restaurant_data(restaurant_name: str, db_path: str = None) -> Optional[pd.DataFrame]:
    """Получить данные конкретного ресторана с максимальной детализацией"""
    df = load_data_enhanced(db_path)
    if df.empty:
        return None
    
    restaurant_data = df[df['restaurant_name'].str.contains(restaurant_name, case=False, na=False)]
    if restaurant_data.empty:
        logger.warning(f"Ресторан '{restaurant_name}' не найден")
        return None
    
    return restaurant_data

def get_restaurants_list(db_path: str = None) -> List[str]:
    """Получить список всех ресторанов"""
    df = load_data_enhanced(db_path)
    if df.empty:
        return []
    
    return sorted(df['restaurant_name'].unique().tolist())

if __name__ == "__main__":
    # Тест расширенной загрузки данных
    print("🧪 ТЕСТИРОВАНИЕ РАСШИРЕННОЙ ЗАГРУЗКИ ДАННЫХ")
    df = load_data_enhanced()
    print(f"📊 Загружено: {len(df)} записей с {len(df.columns)} полями")
    print(f"🏪 Рестораны: {df['restaurant_name'].nunique()}")
    print(f"📅 Период: {df['date'].min()} - {df['date'].max()}")
    print(f"🌟 Новые метрики: {[col for col in df.columns if any(x in col for x in ['_rate', '_ratio', '_efficiency', '_pct'])]}")