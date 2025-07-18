"""
Модуль для загрузки и обработки данных
"""
import pandas as pd
import numpy as np
import sqlite3
import logging
from datetime import datetime, timedelta
import os
from config import DATABASE_PATH, MAIN_DATABASE_PATH

logger = logging.getLogger(__name__)

def load_grab_stats(db_path):
    """Загрузка статистики Grab"""
    try:
        conn = sqlite3.connect(db_path)
        logger.info(f"Подключение к базе {db_path} установлено")
        
        # Проверяем наличие таблицы
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='grab_stats'")
        if not cursor.fetchone():
            logger.warning("Таблица grab_stats не найдена")
            return pd.DataFrame()
        
        # Сначала получаем список ресторанов
        restaurants_query = """
        SELECT id, name FROM restaurants ORDER BY name
        """
        restaurants_df = pd.read_sql_query(restaurants_query, conn)
        
        # Загружаем статистику с правильными колонками
        query = """
        SELECT 
            g.restaurant_id,
            g.date,
            g.sales as total_sales,
            g.orders,
            g.avg_order_value,
            g.ads_spend as ads_sales,
            g.ads_enabled as ads_on,
            g.rating,
            g.delivery_time,
            CASE WHEN g.ads_spend > 0 THEN g.sales / g.ads_spend ELSE 0 END as roas,
            1 as position,
            0.05 as cancel_rate
        FROM grab_stats g
        ORDER BY g.restaurant_id, g.date
        """
        
        df = pd.read_sql_query(query, conn)
        
        # Добавляем названия ресторанов
        df = df.merge(restaurants_df, left_on='restaurant_id', right_on='id', how='left')
        df['restaurant_name'] = df['name']
        
        # Удаляем лишние колонки
        df = df.drop(['restaurant_id', 'id', 'name'], axis=1)
        
        logger.info(f"Загружено {len(df)} записей из grab_stats")
        
        conn.close()
        return df
        
    except Exception as e:
        logger.error(f"Ошибка загрузки grab_stats: {e}")
        return pd.DataFrame()

def load_gojek_stats(db_path):
    """Загрузка статистики Gojek"""
    try:
        conn = sqlite3.connect(db_path)
        
        # Проверяем наличие таблицы
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='gojek_stats'")
        if not cursor.fetchone():
            logger.info("Таблица gojek_stats не найдена, возвращаю пустой DataFrame")
            conn.close()
            return pd.DataFrame()
        
        # Если таблица есть, загружаем данные
        query = """
        SELECT 
            restaurant_name,
            date,
            sales as total_sales,
            orders,
            rating,
            cancel_rate,
            ads_on,
            ads_sales,
            position,
            roas
        FROM gojek_stats
        ORDER BY restaurant_name, date
        """
        
        df = pd.read_sql_query(query, conn)
        logger.info(f"Загружено {len(df)} записей из gojek_stats")
        
        conn.close()
        return df
        
    except Exception as e:
        logger.error(f"Ошибка загрузки gojek_stats: {e}")
        return pd.DataFrame()

def load_restaurants_list(db_path):
    """Загрузка списка ресторанов"""
    try:
        conn = sqlite3.connect(db_path)
        
        query = """
        SELECT 
            id,
            name,
            region as location
        FROM restaurants
        ORDER BY name
        """
        
        df = pd.read_sql_query(query, conn)
        logger.info(f"Загружено {len(df)} ресторанов")
        
        conn.close()
        return df
        
    except Exception as e:
        logger.error(f"Ошибка загрузки списка ресторанов: {e}")
        return pd.DataFrame()

def create_synthetic_weather_data(df):
    """Создание синтетических данных о погоде"""
    logger.info("Создание синтетических погодных данных...")
    
    # Получаем уникальные даты
    dates = pd.to_datetime(df['date']).dt.date.unique()
    
    weather_data = []
    np.random.seed(42)  # Для воспроизводимости
    
    for date in dates:
        # Сезонные колебания температуры
        day_of_year = pd.to_datetime(date).timetuple().tm_yday
        base_temp = 26 + 3 * np.sin(2 * np.pi * day_of_year / 365)
        
        weather_data.append({
            'date': date,
            'temp_c': base_temp + np.random.normal(0, 2),
            'rain_mm': np.random.exponential(2) if np.random.random() < 0.3 else 0,
            'humidity': np.random.normal(75, 10)
        })
    
    weather_df = pd.DataFrame(weather_data)
    logger.info(f"Создано {len(weather_df)} синтетических погодных записей")
    
    return weather_df

def create_synthetic_holiday_data(df):
    """Создание синтетических данных о праздниках"""
    logger.info("Создание синтетических данных о праздниках...")
    
    # Получаем уникальные даты
    dates = pd.to_datetime(df['date']).dt.date.unique()
    
    # Определяем праздники (примерные даты)
    holidays = [
        '2023-01-01', '2023-03-22', '2023-04-22', '2023-05-01',
        '2023-06-01', '2023-08-17', '2023-12-25', '2024-01-01',
        '2024-03-11', '2024-04-10', '2024-05-01', '2024-06-01',
        '2024-08-17', '2024-12-25'
    ]
    
    # Добавляем случайные праздники
    np.random.seed(42)
    random_holidays = np.random.choice(dates, size=10, replace=False)
    all_holidays = set(holidays + [str(d) for d in random_holidays])
    
    holiday_data = []
    for date in dates:
        is_holiday = str(date) in all_holidays
        holiday_data.append({
            'date': date,
            'is_holiday': is_holiday
        })
    
    holiday_df = pd.DataFrame(holiday_data)
    logger.info(f"Создано {len(holiday_df[holiday_df['is_holiday']])} праздничных дней")
    
    return holiday_df

def combine_data_sources(grab_df, gojek_df, weather_df, holiday_df):
    """Объединение всех источников данных"""
    logger.info("Объединение источников данных...")
    
    # Объединяем grab и gojek данные
    if not gojek_df.empty:
        stats_df = pd.concat([grab_df, gojek_df], ignore_index=True)
    else:
        stats_df = grab_df.copy()
    
    # Убеждаемся, что date в правильном формате
    stats_df['date'] = pd.to_datetime(stats_df['date'])
    weather_df['date'] = pd.to_datetime(weather_df['date'])
    holiday_df['date'] = pd.to_datetime(holiday_df['date'])
    
    # Объединяем с погодными данными
    combined_df = stats_df.merge(weather_df, on='date', how='left')
    
    # Объединяем с данными о праздниках
    combined_df = combined_df.merge(holiday_df, on='date', how='left')
    
    # Заполняем пропущенные значения
    numeric_columns = ['total_sales', 'orders', 'rating', 'cancel_rate', 'ads_on', 
                      'ads_sales', 'position', 'roas', 'temp_c', 'rain_mm', 'humidity']
    
    for col in numeric_columns:
        if col in combined_df.columns:
            combined_df[col] = combined_df[col].fillna(0)
    
    # Заполняем булевы значения
    if 'is_holiday' in combined_df.columns:
        combined_df['is_holiday'] = combined_df['is_holiday'].fillna(False)
    
    # Преобразуем ads_on в числовой формат
    if 'ads_on' in combined_df.columns:
        combined_df['ads_on'] = combined_df['ads_on'].astype(float)
    
    logger.info(f"Объединенный датасет содержит {len(combined_df)} записей")
    return combined_df

def load_data_for_training(db_path=None, start_date=None, end_date=None):
    """Загрузка данных для обучения модели"""
    if db_path is None:
        db_path = DATABASE_PATH
    
    logger.info(f"Загрузка данных для обучения из {db_path}")
    
    try:
        conn = sqlite3.connect(db_path)
        logger.info(f"Подключение к базе {db_path} установлено")
        
        # Загружаем статистику
        grab_df = load_grab_stats(db_path)
        gojek_df = load_gojek_stats(db_path)
        
        if grab_df.empty and gojek_df.empty:
            logger.error("Нет данных для загрузки")
            return None
        
        # Загружаем список ресторанов
        restaurants_df = load_restaurants_list(db_path)
        logger.info(f"Загружено {len(restaurants_df)} ресторанов")
        
        # Создаем синтетические данные
        weather_df = create_synthetic_weather_data(grab_df if not grab_df.empty else gojek_df)
        holiday_df = create_synthetic_holiday_data(grab_df if not grab_df.empty else gojek_df)
        
        # Объединяем все данные
        combined_df = combine_data_sources(grab_df, gojek_df, weather_df, holiday_df)
        
        # Фильтруем по датам если указано
        if start_date:
            combined_df = combined_df[combined_df['date'] >= start_date]
        if end_date:
            combined_df = combined_df[combined_df['date'] <= end_date]
        
        # Проверяем качество данных
        from utils import check_data_quality
        check_data_quality(combined_df)
        
        logger.info("Соединение с базой закрыто")
        conn.close()
        
        return combined_df
        
    except Exception as e:
        logger.error(f"Ошибка загрузки данных: {e}")
        return None

def get_restaurant_data(restaurant_name, db_path=None):
    """Получение данных для конкретного ресторана"""
    if db_path is None:
        db_path = DATABASE_PATH
    
    logger.info(f"Загрузка данных для ресторана '{restaurant_name}'")
    
    # Загружаем все данные
    df = load_data_for_training(db_path)
    
    if df is None:
        logger.error("Не удалось загрузить данные")
        return None
    
    # Фильтруем по ресторану
    restaurant_df = df[df['restaurant_name'] == restaurant_name].copy()
    
    if restaurant_df.empty:
        logger.error(f"Нет данных для ресторана '{restaurant_name}'")
        return None
    
    logger.info(f"Загружено {len(restaurant_df)} записей для ресторана '{restaurant_name}'")
    return restaurant_df

def get_restaurants_list(db_path=None):
    """Получение списка всех ресторанов"""
    if db_path is None:
        db_path = DATABASE_PATH
    
    logger.info("Получение списка ресторанов")
    
    # Загружаем все данные
    df = load_data_for_training(db_path)
    
    if df is None:
        logger.error("Не удалось загрузить данные")
        return []
    
    # Получаем уникальные рестораны с количеством записей
    restaurants = df.groupby('restaurant_name').size().reset_index(name='records_count')
    restaurants = restaurants.sort_values('restaurant_name')
    
    return restaurants.to_dict('records')