"""
Модуль для загрузки и обработки данных из SQLite базы
"""
import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging
from config import DATABASE_PATH
from utils import check_data_quality, create_sample_data

logger = logging.getLogger(__name__)

class DataLoader:
    """Класс для загрузки данных из базы deliverybooster.db"""
    
    def __init__(self, db_path=DATABASE_PATH):
        self.db_path = db_path
        self.connection = None
    
    def connect(self):
        """Подключение к базе данных"""
        try:
            self.connection = sqlite3.connect(self.db_path)
            logger.info(f"Подключение к базе {self.db_path} установлено")
            return True
        except sqlite3.Error as e:
            logger.error(f"Ошибка подключения к базе: {e}")
            return False
    
    def disconnect(self):
        """Отключение от базы данных"""
        if self.connection:
            self.connection.close()
            logger.info("Соединение с базой закрыто")
    
    def get_table_info(self, table_name):
        """Получение информации о структуре таблицы"""
        if not self.connection:
            logger.error("Нет подключения к базе данных")
            return None
        
        try:
            query = f"PRAGMA table_info({table_name})"
            df = pd.read_sql_query(query, self.connection)
            return df
        except Exception as e:
            logger.error(f"Ошибка получения информации о таблице {table_name}: {e}")
            return None
    
    def load_grab_stats(self, start_date=None, end_date=None):
        """Загрузка данных из таблицы grab_stats"""
        if not self.connection:
            logger.error("Нет подключения к базе данных")
            return None
        
        query = """
        SELECT 
            restaurant_id,
            date,
            total_sales,
            ads_sales,
            rating,
            roas,
            position,
            cancel_rate,
            CASE WHEN ads_sales > 0 THEN 1 ELSE 0 END as ads_on
        FROM grab_stats
        """
        
        if start_date and end_date:
            query += f" WHERE date BETWEEN '{start_date}' AND '{end_date}'"
        
        query += " ORDER BY restaurant_id, date"
        
        try:
            df = pd.read_sql_query(query, self.connection)
            logger.info(f"Загружено {len(df)} записей из grab_stats")
            return df
        except Exception as e:
            logger.error(f"Ошибка загрузки grab_stats: {e}")
            return None
    
    def load_gojek_stats(self, start_date=None, end_date=None):
        """Загрузка данных из таблицы gojek_stats"""
        if not self.connection:
            logger.error("Нет подключения к базе данных")
            return None
        
        query = """
        SELECT 
            restaurant_id,
            date,
            total_sales,
            ads_sales,
            rating,
            roas,
            position,
            cancel_rate,
            CASE WHEN ads_sales > 0 THEN 1 ELSE 0 END as ads_on
        FROM gojek_stats
        """
        
        if start_date and end_date:
            query += f" WHERE date BETWEEN '{start_date}' AND '{end_date}'"
        
        query += " ORDER BY restaurant_id, date"
        
        try:
            df = pd.read_sql_query(query, self.connection)
            logger.info(f"Загружено {len(df)} записей из gojek_stats")
            return df
        except Exception as e:
            logger.error(f"Ошибка загрузки gojek_stats: {e}")
            return None
    
    def load_restaurants(self):
        """Загрузка информации о ресторанах"""
        if not self.connection:
            logger.error("Нет подключения к базе данных")
            return None
        
        query = """
        SELECT 
            id as restaurant_id,
            name as restaurant_name,
            location,
            cuisine_type
        FROM restaurants
        """
        
        try:
            df = pd.read_sql_query(query, self.connection)
            logger.info(f"Загружено {len(df)} ресторанов")
            return df
        except Exception as e:
            logger.error(f"Ошибка загрузки restaurants: {e}")
            return None
    
    def load_weather(self, start_date=None, end_date=None):
        """Загрузка погодных данных"""
        if not self.connection:
            logger.error("Нет подключения к базе данных")
            return None
        
        query = """
        SELECT 
            date,
            location,
            rain_mm,
            temp_c
        FROM weather
        """
        
        if start_date and end_date:
            query += f" WHERE date BETWEEN '{start_date}' AND '{end_date}'"
        
        query += " ORDER BY date, location"
        
        try:
            df = pd.read_sql_query(query, self.connection)
            logger.info(f"Загружено {len(df)} погодных записей")
            return df
        except Exception as e:
            logger.error(f"Ошибка загрузки weather: {e}")
            return None
    
    def load_holidays(self, start_date=None, end_date=None):
        """Загрузка данных о праздниках"""
        if not self.connection:
            logger.error("Нет подключения к базе данных")
            return None
        
        query = """
        SELECT 
            date,
            holiday_type,
            name as holiday_name
        FROM holidays
        """
        
        if start_date and end_date:
            query += f" WHERE date BETWEEN '{start_date}' AND '{end_date}'"
        
        query += " ORDER BY date"
        
        try:
            df = pd.read_sql_query(query, self.connection)
            logger.info(f"Загружено {len(df)} праздничных дней")
            return df
        except Exception as e:
            logger.error(f"Ошибка загрузки holidays: {e}")
            return None
    
    def load_combined_data(self, start_date=None, end_date=None, platform='both'):
        """Загрузка объединенных данных со всех таблиц"""
        if not self.connect():
            logger.error("Не удалось подключиться к базе данных")
            # Создаем тестовые данные для демонстрации
            logger.info("Создаю тестовые данные для демонстрации")
            return create_sample_data()
        
        try:
            # Загрузка основных данных
            if platform == 'grab':
                stats_df = self.load_grab_stats(start_date, end_date)
            elif platform == 'gojek':
                stats_df = self.load_gojek_stats(start_date, end_date)
            else:  # both
                grab_df = self.load_grab_stats(start_date, end_date)
                gojek_df = self.load_gojek_stats(start_date, end_date)
                if grab_df is not None and gojek_df is not None:
                    grab_df['platform'] = 'grab'
                    gojek_df['platform'] = 'gojek'
                    stats_df = pd.concat([grab_df, gojek_df], ignore_index=True)
                else:
                    stats_df = grab_df if grab_df is not None else gojek_df
            
            if stats_df is None:
                logger.error("Не удалось загрузить данные статистики")
                return None
            
            # Загрузка дополнительных данных
            restaurants_df = self.load_restaurants()
            weather_df = self.load_weather(start_date, end_date)
            holidays_df = self.load_holidays(start_date, end_date)
            
            # Объединение данных
            # Добавляем информацию о ресторанах
            if restaurants_df is not None:
                combined_df = stats_df.merge(restaurants_df, on='restaurant_id', how='left')
            else:
                combined_df = stats_df.copy()
                combined_df['restaurant_name'] = 'Unknown'
                combined_df['location'] = 'Unknown'
            
            # Добавляем погодные данные
            if weather_df is not None:
                combined_df = combined_df.merge(weather_df, 
                                              on=['date', 'location'], 
                                              how='left')
            else:
                combined_df['rain_mm'] = 0
                combined_df['temp_c'] = 25
            
            # Добавляем информацию о праздниках
            if holidays_df is not None:
                holidays_df['is_holiday'] = 1
                combined_df = combined_df.merge(holidays_df[['date', 'is_holiday']], 
                                              on='date', how='left')
                combined_df['is_holiday'] = combined_df['is_holiday'].fillna(0)
            else:
                combined_df['is_holiday'] = 0
            
            # Добавляем день недели
            combined_df['date'] = pd.to_datetime(combined_df['date'])
            combined_df['day_of_week'] = combined_df['date'].dt.dayofweek
            combined_df['date'] = combined_df['date'].dt.strftime('%Y-%m-%d')
            
            # Заполняем пропущенные значения
            combined_df = combined_df.fillna({
                'rain_mm': 0,
                'temp_c': 25,
                'is_holiday': 0,
                'rating': 4.0,
                'roas': 0,
                'position': 10,
                'cancel_rate': 0.1
            })
            
            logger.info(f"Объединенный датасет содержит {len(combined_df)} записей")
            
            # Проверка качества данных
            check_data_quality(combined_df)
            
            return combined_df
            
        except Exception as e:
            logger.error(f"Ошибка при загрузке объединенных данных: {e}")
            return None
        finally:
            self.disconnect()

def load_data_for_training(start_date=None, end_date=None, platform='both'):
    """Функция для загрузки данных для обучения модели"""
    loader = DataLoader()
    return loader.load_combined_data(start_date, end_date, platform)

def get_restaurant_data(restaurant_name, start_date=None, end_date=None):
    """Получение данных для конкретного ресторана"""
    loader = DataLoader()
    combined_df = loader.load_combined_data(start_date, end_date)
    
    if combined_df is None:
        return None
    
    restaurant_df = combined_df[combined_df['restaurant_name'] == restaurant_name].copy()
    
    if restaurant_df.empty:
        logger.warning(f"Данные для ресторана '{restaurant_name}' не найдены")
        return None
    
    # Сортируем по дате
    restaurant_df = restaurant_df.sort_values('date')
    
    logger.info(f"Загружено {len(restaurant_df)} записей для ресторана '{restaurant_name}'")
    return restaurant_df