"""
Модуль для загрузки и обработки данных из SQLite базы данных
"""
import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging
from typing import Optional, Tuple, List
from config import DATABASE_PATH

logger = logging.getLogger(__name__)

def load_grab_stats(db_path: str) -> pd.DataFrame:
    """Загрузка данных из grab_stats с правильными названиями колонок"""
    try:
        conn = sqlite3.connect(db_path)
        
        # Загружаем названия ресторанов
        restaurants_query = "SELECT id, name FROM restaurants ORDER BY name"
        restaurants_df = pd.read_sql_query(restaurants_query, conn)
        
        # Загружаем статистику с правильными названиями колонок
        query = """
        SELECT 
            g.restaurant_id,
            g.stat_date as date,
            g.sales as total_sales,
            g.orders,
            g.sales / NULLIF(g.orders, 0) as avg_order_value,
            g.ads_sales,
            CASE WHEN g.ads_spend > 0 THEN 1 ELSE 0 END as ads_on,
            g.rating,
            30 as delivery_time,
            CASE WHEN g.ads_spend > 0 THEN g.ads_sales / g.ads_spend ELSE 0 END as roas,
            1 as position,
            COALESCE(g.cancelation_rate, 0.05) as cancel_rate
        FROM grab_stats g
        WHERE g.sales > 0
        ORDER BY g.restaurant_id, g.stat_date
        """
        
        df = pd.read_sql_query(query, conn)
        
        # Объединяем с названиями ресторанов
        df = df.merge(restaurants_df, left_on='restaurant_id', right_on='id', how='left')
        df['restaurant_name'] = df['name']
        df = df.drop(['restaurant_id', 'id', 'name'], axis=1)
        
        # Конвертируем дату
        df['date'] = pd.to_datetime(df['date'])
        
        # Убеждаемся, что числовые колонки имеют правильный тип
        numeric_columns = ['total_sales', 'ads_sales', 'rating', 'roas', 'position', 'cancel_rate', 'ads_on']
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        
        conn.close()
        
        logger.info(f"Загружено {len(df)} записей из grab_stats")
        logger.info(f"Уникальных ресторанов: {df['restaurant_name'].nunique()}")
        
        return df
        
    except Exception as e:
        logger.error(f"Ошибка загрузки grab_stats: {e}")
        return pd.DataFrame()

def load_gojek_stats(db_path: str) -> pd.DataFrame:
    """Загрузка данных из gojek_stats (если есть)"""
    try:
        conn = sqlite3.connect(db_path)
        
        # Проверяем, есть ли таблица gojek_stats
        tables = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='gojek_stats'").fetchall()
        if not tables:
            logger.info("Таблица gojek_stats не найдена, возвращаю пустой DataFrame")
            conn.close()
            return pd.DataFrame()
        
        # Загружаем названия ресторанов
        restaurants_query = "SELECT id, name FROM restaurants ORDER BY name"
        restaurants_df = pd.read_sql_query(restaurants_query, conn)
        
        # Загружаем данные из gojek_stats (структура может отличаться)
        query = """
        SELECT 
            g.restaurant_id,
            g.stat_date as date,
            g.sales as total_sales,
            g.orders,
            g.sales / NULLIF(g.orders, 0) as avg_order_value,
            COALESCE(g.ads_sales, 0) as ads_sales,
            CASE WHEN COALESCE(g.ads_spend, 0) > 0 THEN 1 ELSE 0 END as ads_on,
            COALESCE(g.rating, 4.5) as rating,
            30 as delivery_time,
            CASE WHEN COALESCE(g.ads_spend, 0) > 0 THEN COALESCE(g.ads_sales, 0) / g.ads_spend ELSE 0 END as roas,
            1 as position,
            0.05 as cancel_rate
        FROM gojek_stats g
        WHERE g.sales > 0
        ORDER BY g.restaurant_id, g.stat_date
        """
        
        df = pd.read_sql_query(query, conn)
        
        # Объединяем с названиями ресторанов
        df = df.merge(restaurants_df, left_on='restaurant_id', right_on='id', how='left')
        df['restaurant_name'] = df['name']
        df = df.drop(['restaurant_id', 'id', 'name'], axis=1)
        
        # Конвертируем дату
        df['date'] = pd.to_datetime(df['date'])
        
        # Убеждаемся, что числовые колонки имеют правильный тип
        numeric_columns = ['total_sales', 'ads_sales', 'rating', 'roas', 'position', 'cancel_rate', 'ads_on']
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        
        conn.close()
        
        logger.info(f"Загружено {len(df)} записей из gojek_stats")
        
        return df
        
    except Exception as e:
        logger.error(f"Ошибка загрузки gojek_stats: {e}")
        return pd.DataFrame()

def load_restaurants_list(db_path: str) -> pd.DataFrame:
    """Загрузка списка ресторанов"""
    try:
        conn = sqlite3.connect(db_path)
        
        query = "SELECT id, name, 'Indonesia' as location FROM restaurants ORDER BY name"
        df = pd.read_sql_query(query, conn)
        
        conn.close()
        
        logger.info(f"Загружено {len(df)} ресторанов")
        
        return df
        
    except Exception as e:
        logger.error(f"Ошибка загрузки списка ресторанов: {e}")
        return pd.DataFrame()

def create_synthetic_weather_data(start_date: str = "2024-01-01", 
                                end_date: str = "2024-12-31") -> pd.DataFrame:
    """Создание синтетических данных о погоде"""
    try:
        logger.info("Создание синтетических погодных данных...")
        
        start = pd.to_datetime(start_date)
        end = pd.to_datetime(end_date)
        
        dates = pd.date_range(start=start, end=end, freq='D')
        
        # Создаем реалистичные данные о погоде для Индонезии
        np.random.seed(42)
        
        weather_data = []
        for date in dates:
            # Температура: 24-34°C с сезонными колебаниями
            base_temp = 28 + 3 * np.sin(2 * np.pi * date.dayofyear / 365)
            temp_c = base_temp + np.random.normal(0, 2)
            temp_c = np.clip(temp_c, 24, 34)
            
            # Дождь: больше в сезон дождей (октябрь-март)
            if date.month in [10, 11, 12, 1, 2, 3]:
                rain_probability = 0.4
            else:
                rain_probability = 0.1
            
            if np.random.random() < rain_probability:
                rain_mm = np.random.exponential(10)
                rain_mm = np.clip(rain_mm, 0, 50)
            else:
                rain_mm = 0
            
            weather_data.append({
                'date': date,
                'temp_c': round(temp_c, 1),
                'rain_mm': round(rain_mm, 1)
            })
        
        df = pd.DataFrame(weather_data)
        
        logger.info(f"Создано {len(df)} синтетических погодных записей")
        
        return df
        
    except Exception as e:
        logger.error(f"Ошибка создания погодных данных: {e}")
        return pd.DataFrame()

def create_synthetic_holiday_data(start_date: str = "2024-01-01", 
                                end_date: str = "2024-12-31") -> pd.DataFrame:
    """Создание синтетических данных о праздниках"""
    try:
        logger.info("Создание синтетических данных о праздниках...")
        
        start = pd.to_datetime(start_date)
        end = pd.to_datetime(end_date)
        
        dates = pd.date_range(start=start, end=end, freq='D')
        
        # Основные праздники Индонезии
        holidays = [
            ('01-01', 'New Year'),
            ('02-10', 'Chinese New Year'),
            ('03-11', 'Nyepi'),
            ('04-10', 'Good Friday'),
            ('05-01', 'Labor Day'),
            ('05-09', 'Ascension Day'),
            ('06-01', 'Pancasila Day'),
            ('08-17', 'Independence Day'),
            ('12-25', 'Christmas'),
            ('12-26', 'Boxing Day')
        ]
        
        holiday_data = []
        for date in dates:
            is_holiday = False
            holiday_name = None
            
            # Проверяем фиксированные праздники
            date_str = date.strftime('%m-%d')
            for holiday_date, name in holidays:
                if date_str == holiday_date:
                    is_holiday = True
                    holiday_name = name
                    break
            
            # Добавляем религиозные праздники (примерные даты)
            if date.month == 4 and date.day in [22, 23]:  # Eid al-Fitr
                is_holiday = True
                holiday_name = 'Eid al-Fitr'
            elif date.month == 6 and date.day in [28, 29]:  # Eid al-Adha
                is_holiday = True
                holiday_name = 'Eid al-Adha'
            
            holiday_data.append({
                'date': date,
                'is_holiday': is_holiday,
                'holiday_name': holiday_name
            })
        
        df = pd.DataFrame(holiday_data)
        
        logger.info(f"Создано {len(df[df['is_holiday']])} праздничных дней")
        
        return df
        
    except Exception as e:
        logger.error(f"Ошибка создания данных о праздниках: {e}")
        return pd.DataFrame()

def combine_data_sources(grab_df: pd.DataFrame, gojek_df: pd.DataFrame, 
                        weather_df: pd.DataFrame, holiday_df: pd.DataFrame) -> pd.DataFrame:
    """Объединение всех источников данных"""
    try:
        logger.info("Объединение источников данных...")
        
        # Объединяем данные из разных платформ
        if not gojek_df.empty:
            stats_df = pd.concat([grab_df, gojek_df], ignore_index=True)
        else:
            stats_df = grab_df.copy()
        
        # Убеждаемся, что даты в правильном формате
        stats_df['date'] = pd.to_datetime(stats_df['date'])
        weather_df['date'] = pd.to_datetime(weather_df['date'])
        holiday_df['date'] = pd.to_datetime(holiday_df['date'])
        
        # Объединяем с погодными данными
        combined_df = stats_df.merge(weather_df, on='date', how='left')
        
        # Объединяем с праздничными данными
        combined_df = combined_df.merge(holiday_df, on='date', how='left')
        
        # Заполняем пропущенные значения
        combined_df['temp_c'] = combined_df['temp_c'].fillna(28.0)
        combined_df['rain_mm'] = combined_df['rain_mm'].fillna(0.0)
        combined_df['is_holiday'] = combined_df['is_holiday'].fillna(False)
        
        # Добавляем день недели
        combined_df['day_of_week'] = combined_df['date'].dt.dayofweek
        
        # Убеждаемся, что ads_on имеет правильный тип
        if 'ads_on' in combined_df.columns:
            combined_df['ads_on'] = combined_df['ads_on'].astype(float)
        
        logger.info(f"Объединенный датасет содержит {len(combined_df)} записей")
        
        return combined_df
        
    except Exception as e:
        logger.error(f"Ошибка объединения данных: {e}")
        return pd.DataFrame()

def load_data_for_training(db_path: str = None) -> pd.DataFrame:
    """Загрузка и подготовка данных для обучения модели"""
    if db_path is None:
        db_path = DATABASE_PATH
    
    logger.info(f"Загрузка данных для обучения из {db_path}")
    
    try:
        # Загружаем данные из разных источников
        grab_df = load_grab_stats(db_path)
        gojek_df = load_gojek_stats(db_path)
        
        if grab_df.empty:
            logger.error("Не удалось загрузить данные из grab_stats")
            return pd.DataFrame()
        
        # Определяем диапазон дат из данных
        all_dates = grab_df['date'].tolist()
        if not gojek_df.empty:
            all_dates.extend(gojek_df['date'].tolist())
        
        if not all_dates:
            logger.error("Нет данных для определения диапазона дат")
            return pd.DataFrame()
        
        start_date = min(all_dates).strftime('%Y-%m-%d')
        end_date = max(all_dates).strftime('%Y-%m-%d')
        
        # Создаем синтетические данные
        weather_df = create_synthetic_weather_data(start_date, end_date)
        holiday_df = create_synthetic_holiday_data(start_date, end_date)
        
        # Объединяем все данные
        combined_df = combine_data_sources(grab_df, gojek_df, weather_df, holiday_df)
        
        return combined_df
        
    except Exception as e:
        logger.error(f"Ошибка загрузки данных для обучения: {e}")
        return pd.DataFrame()

def get_restaurant_data(restaurant_name: str, db_path: str = None) -> Optional[pd.DataFrame]:
    """Получение данных конкретного ресторана"""
    if db_path is None:
        db_path = DATABASE_PATH
    
    logger.info(f"Загрузка данных для ресторана '{restaurant_name}'")
    
    try:
        # Загружаем все данные
        df = load_data_for_training(db_path)
        
        if df.empty:
            logger.error("Не удалось загрузить данные")
            return None
        
        # Фильтруем по ресторану
        restaurant_df = df[df['restaurant_name'] == restaurant_name].copy()
        
        if restaurant_df.empty:
            logger.error(f"Ресторан '{restaurant_name}' не найден")
            return None
        
        logger.info(f"Загружено {len(restaurant_df)} записей для ресторана '{restaurant_name}'")
        
        return restaurant_df
        
    except Exception as e:
        logger.error(f"Ошибка получения данных ресторана: {e}")
        return None

def get_restaurants_list(db_path: str = None) -> List[str]:
    """Получение списка всех ресторанов"""
    if db_path is None:
        db_path = DATABASE_PATH
    
    logger.info("Получение списка ресторанов")
    
    try:
        # Загружаем все данные
        df = load_data_for_training(db_path)
        
        if df.empty:
            logger.error("Не удалось загрузить данные")
            return []
        
        # Получаем уникальные рестораны с количеством записей
        restaurant_counts = df['restaurant_name'].value_counts()
        
        restaurants = []
        for restaurant, count in restaurant_counts.items():
            restaurants.append(f"{restaurant} ({count} записей)")
        
        return restaurants
        
    except Exception as e:
        logger.error(f"Ошибка получения списка ресторанов: {e}")
        return []