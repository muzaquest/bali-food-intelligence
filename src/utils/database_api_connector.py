#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
🌐 API КОННЕКТОР К ОБНОВЛЯЕМОЙ БАЗЕ ДАННЫХ
==========================================
Модуль для работы с внешней БД через REST API вместо локального файла
"""

import requests
import sqlite3
import pandas as pd
import json
import os
import time
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Any
import hashlib

class DatabaseAPIConnector:
    """
    Коннектор для работы с обновляемой БД через API
    Кеширует данные локально для производительности
    """
    
    def __init__(self, api_base_url: str, api_key: str = None, cache_ttl: int = 300):
        """
        Инициализация API коннектора
        
        Args:
            api_base_url: Базовый URL API (например: https://api.muzaquest.com/v1)
            api_key: API ключ для аутентификации
            cache_ttl: Время жизни кеша в секундах (по умолчанию 5 минут)
        """
        self.api_base_url = api_base_url.rstrip('/')
        self.api_key = api_key
        self.cache_ttl = cache_ttl
        self.cache_dir = '.cache'
        self.session = requests.Session()
        
        # Настраиваем заголовки
        if api_key:
            self.session.headers.update({
                'Authorization': f'Bearer {api_key}',
                'Content-Type': 'application/json'
            })
        
        # Создаем папку для кеша
        os.makedirs(self.cache_dir, exist_ok=True)
        
        print(f"🌐 API коннектор инициализирован: {api_base_url}")
    
    def _get_cache_path(self, cache_key: str) -> str:
        """Генерирует путь к файлу кеша"""
        # Создаем безопасное имя файла из ключа
        safe_key = hashlib.md5(cache_key.encode()).hexdigest()
        return os.path.join(self.cache_dir, f"{safe_key}.json")
    
    def _is_cache_valid(self, cache_path: str) -> bool:
        """Проверяет актуальность кеша"""
        if not os.path.exists(cache_path):
            return False
        
        cache_time = os.path.getmtime(cache_path)
        return (time.time() - cache_time) < self.cache_ttl
    
    def _save_cache(self, cache_key: str, data: Any):
        """Сохраняет данные в кеш"""
        try:
            cache_path = self._get_cache_path(cache_key)
            with open(cache_path, 'w', encoding='utf-8') as f:
                json.dump({
                    'timestamp': time.time(),
                    'data': data
                }, f, ensure_ascii=False, default=str)
        except Exception as e:
            print(f"⚠️ Ошибка сохранения кеша: {e}")
    
    def _load_cache(self, cache_key: str) -> Optional[Any]:
        """Загружает данные из кеша"""
        try:
            cache_path = self._get_cache_path(cache_key)
            
            if not self._is_cache_valid(cache_path):
                return None
            
            with open(cache_path, 'r', encoding='utf-8') as f:
                cache_data = json.load(f)
                return cache_data['data']
        except Exception as e:
            print(f"⚠️ Ошибка загрузки кеша: {e}")
            return None
    
    def _api_request(self, endpoint: str, params: dict = None) -> Optional[Dict]:
        """Выполняет запрос к API"""
        try:
            url = f"{self.api_base_url}{endpoint}"
            
            print(f"🌐 API запрос: {endpoint}")
            
            response = self.session.get(url, params=params, timeout=30)
            response.raise_for_status()
            
            return response.json()
            
        except requests.exceptions.RequestException as e:
            print(f"❌ Ошибка API запроса {endpoint}: {e}")
            return None
        except json.JSONDecodeError as e:
            print(f"❌ Ошибка парсинга JSON {endpoint}: {e}")
            return None
    
    def get_restaurants(self) -> List[Dict]:
        """Получает список всех ресторанов"""
        cache_key = "restaurants_list"
        
        # Проверяем кеш
        cached_data = self._load_cache(cache_key)
        if cached_data:
            print(f"📋 Рестораны загружены из кеша ({len(cached_data)} шт.)")
            return cached_data
        
        # Запрашиваем через API
        data = self._api_request('/restaurants')
        if data and 'restaurants' in data:
            restaurants = data['restaurants']
            self._save_cache(cache_key, restaurants)
            print(f"📋 Рестораны загружены через API ({len(restaurants)} шт.)")
            return restaurants
        
        print("❌ Не удалось загрузить список ресторанов")
        return []
    
    def get_restaurant_stats(self, restaurant_name: str, start_date: str = None, end_date: str = None, platform: str = 'all') -> List[Dict]:
        """
        Получает статистику ресторана
        
        Args:
            restaurant_name: Название ресторана
            start_date: Дата начала в формате YYYY-MM-DD
            end_date: Дата окончания в формате YYYY-MM-DD  
            platform: Платформа ('grab', 'gojek', 'all')
        """
        # Создаем ключ кеша
        cache_key = f"stats_{restaurant_name}_{start_date}_{end_date}_{platform}"
        
        # Проверяем кеш
        cached_data = self._load_cache(cache_key)
        if cached_data:
            print(f"📊 Статистика {restaurant_name} загружена из кеша")
            return cached_data
        
        # Параметры запроса
        params = {
            'restaurant': restaurant_name,
            'platform': platform
        }
        
        if start_date:
            params['start_date'] = start_date
        if end_date:
            params['end_date'] = end_date
        
        # Запрашиваем через API
        data = self._api_request('/restaurant-stats', params)
        if data and 'stats' in data:
            stats = data['stats']
            self._save_cache(cache_key, stats)
            print(f"📊 Статистика {restaurant_name} загружена через API ({len(stats)} записей)")
            return stats
        
        print(f"❌ Не удалось загрузить статистику для {restaurant_name}")
        return []
    
    def get_market_overview(self, start_date: str = None, end_date: str = None) -> Dict:
        """Получает обзорные данные по рынку"""
        cache_key = f"market_overview_{start_date}_{end_date}"
        
        # Проверяем кеш
        cached_data = self._load_cache(cache_key)
        if cached_data:
            print("🌍 Обзор рынка загружен из кеша")
            return cached_data
        
        # Параметры запроса
        params = {}
        if start_date:
            params['start_date'] = start_date
        if end_date:
            params['end_date'] = end_date
        
        # Запрашиваем через API
        data = self._api_request('/market-overview', params)
        if data:
            self._save_cache(cache_key, data)
            print("🌍 Обзор рынка загружен через API")
            return data
        
        print("❌ Не удалось загрузить обзор рынка")
        return {}
    
    def get_restaurant_location(self, restaurant_name: str) -> Optional[Dict]:
        """Получает координаты ресторана"""
        cache_key = f"location_{restaurant_name}"
        
        # Проверяем кеш
        cached_data = self._load_cache(cache_key)
        if cached_data:
            return cached_data
        
        # Запрашиваем через API
        data = self._api_request(f'/restaurant-location', {'restaurant': restaurant_name})
        if data and 'location' in data:
            location = data['location']
            self._save_cache(cache_key, location)
            return location
        
        # Возвращаем координаты центра Бали по умолчанию
        default_location = {
            'latitude': -8.4095,
            'longitude': 115.1889,
            'location': 'Denpasar',
            'area': 'Denpasar',
            'zone': 'Central'
        }
        return default_location
    
    def create_local_database(self, output_file: str = 'database.sqlite'):
        """
        Создает локальную SQLite базу из API данных
        Используется как fallback или для оффлайн работы
        """
        print(f"🔄 Создаем локальную БД из API данных...")
        
        try:
            # Создаем соединение с новой БД
            conn = sqlite3.connect(output_file)
            cursor = conn.cursor()
            
            # Создаем таблицы
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS restaurants (
                    id INTEGER PRIMARY KEY,
                    name TEXT UNIQUE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS grab_stats (
                    id INTEGER PRIMARY KEY,
                    restaurant_id INTEGER,
                    stat_date DATE,
                    sales REAL,
                    orders INTEGER,
                    rating REAL,
                    -- ... все остальные поля
                    FOREIGN KEY (restaurant_id) REFERENCES restaurants (id)
                )
            ''')
            
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS gojek_stats (
                    id INTEGER PRIMARY KEY,
                    restaurant_id INTEGER,
                    stat_date DATE,
                    sales REAL,
                    orders INTEGER,
                    rating REAL,
                    -- ... все остальные поля
                    FOREIGN KEY (restaurant_id) REFERENCES restaurants (id)
                )
            ''')
            
            # Загружаем рестораны
            restaurants = self.get_restaurants()
            for restaurant in restaurants:
                cursor.execute(
                    'INSERT OR REPLACE INTO restaurants (name) VALUES (?)',
                    (restaurant['name'],)
                )
            
            # Загружаем статистику для каждого ресторана
            for restaurant in restaurants:
                restaurant_name = restaurant['name']
                
                # Получаем статистику Grab
                grab_stats = self.get_restaurant_stats(restaurant_name, platform='grab')
                for stat in grab_stats:
                    # Вставляем данные (упрощенный пример)
                    cursor.execute('''
                        INSERT OR REPLACE INTO grab_stats 
                        (restaurant_id, stat_date, sales, orders, rating)
                        VALUES (
                            (SELECT id FROM restaurants WHERE name = ?),
                            ?, ?, ?, ?
                        )
                    ''', (
                        restaurant_name,
                        stat.get('date'),
                        stat.get('sales', 0),
                        stat.get('orders', 0),
                        stat.get('rating', 0)
                    ))
                
                # Получаем статистику Gojek
                gojek_stats = self.get_restaurant_stats(restaurant_name, platform='gojek')
                for stat in gojek_stats:
                    cursor.execute('''
                        INSERT OR REPLACE INTO gojek_stats 
                        (restaurant_id, stat_date, sales, orders, rating)
                        VALUES (
                            (SELECT id FROM restaurants WHERE name = ?),
                            ?, ?, ?, ?
                        )
                    ''', (
                        restaurant_name,
                        stat.get('date'),
                        stat.get('sales', 0),
                        stat.get('orders', 0),
                        stat.get('rating', 0)
                    ))
            
            conn.commit()
            conn.close()
            
            print(f"✅ Локальная БД создана: {output_file}")
            return True
            
        except Exception as e:
            print(f"❌ Ошибка создания локальной БД: {e}")
            return False
    
    def test_connection(self) -> bool:
        """Тестирует подключение к API"""
        print("🔍 Тестируем подключение к API...")
        
        try:
            # Тестовый запрос
            data = self._api_request('/health')
            
            if data:
                print("✅ API подключение работает")
                if 'version' in data:
                    print(f"📋 Версия API: {data['version']}")
                if 'restaurants_count' in data:
                    print(f"📊 Ресторанов в базе: {data['restaurants_count']}")
                return True
            else:
                print("❌ API не отвечает")
                return False
                
        except Exception as e:
            print(f"❌ Ошибка тестирования API: {e}")
            return False
    
    def clear_cache(self):
        """Очищает весь кеш"""
        try:
            import shutil
            if os.path.exists(self.cache_dir):
                shutil.rmtree(self.cache_dir)
                os.makedirs(self.cache_dir, exist_ok=True)
                print("🧹 Кеш очищен")
        except Exception as e:
            print(f"⚠️ Ошибка очистки кеша: {e}")


# Адаптер для интеграции с существующим кодом
class DatabaseAdapter:
    """
    Адаптер, который позволяет использовать API как обычную SQLite БД
    Прозрачно работает с существующим кодом
    """
    
    def __init__(self, api_connector: DatabaseAPIConnector):
        self.api = api_connector
        self.local_db_path = 'temp_database.sqlite'
    
    def execute_query(self, query: str, params: tuple = None) -> pd.DataFrame:
        """
        Выполняет SQL-подобный запрос через API
        Автоматически переводит SQL в API вызовы
        """
        try:
            # Парсим простые запросы
            query_lower = query.lower().strip()
            
            if 'select' in query_lower and 'restaurants' in query_lower:
                # Запрос ресторанов
                restaurants = self.api.get_restaurants()
                return pd.DataFrame(restaurants)
            
            elif 'grab_stats' in query_lower or 'gojek_stats' in query_lower:
                # Запрос статистики - создаем временную БД
                if self.api.create_local_database(self.local_db_path):
                    conn = sqlite3.connect(self.local_db_path)
                    result = pd.read_sql_query(query, conn, params=params)
                    conn.close()
                    return result
            
            # Для сложных запросов создаем полную локальную БД
            if self.api.create_local_database(self.local_db_path):
                conn = sqlite3.connect(self.local_db_path)
                result = pd.read_sql_query(query, conn, params=params)
                conn.close()
                return result
            
            return pd.DataFrame()
            
        except Exception as e:
            print(f"❌ Ошибка выполнения запроса: {e}")
            return pd.DataFrame()


# Функция для настройки API коннектора
def setup_api_database(api_url: str, api_key: str = None) -> DatabaseAPIConnector:
    """
    Настраивает API коннектор для работы с обновляемой БД
    
    Args:
        api_url: URL API базы данных
        api_key: API ключ (если требуется)
    
    Returns:
        Настроенный коннектор
    """
    connector = DatabaseAPIConnector(api_url, api_key)
    
    # Тестируем подключение
    if connector.test_connection():
        print("🎉 API база данных настроена и готова к работе!")
        return connector
    else:
        print("❌ Не удалось подключиться к API базе данных")
        return None


if __name__ == "__main__":
    # Пример использования
    print("🌐 ТЕСТ API КОННЕКТОРА")
    print("=" * 40)
    
    # Настройка (замените на реальные значения)
    API_URL = "https://api.muzaquest.com/v1"
    API_KEY = "your-api-key-here"
    
    # Создаем коннектор
    api_db = setup_api_database(API_URL, API_KEY)
    
    if api_db:
        # Тестируем функции
        restaurants = api_db.get_restaurants()
        print(f"📋 Найдено ресторанов: {len(restaurants)}")
        
        if restaurants:
            # Тестируем статистику первого ресторана
            first_restaurant = restaurants[0]['name']
            stats = api_db.get_restaurant_stats(first_restaurant)
            print(f"📊 Статистика {first_restaurant}: {len(stats)} записей")
        
        # Тестируем адаптер
        adapter = DatabaseAdapter(api_db)
        df = adapter.execute_query("SELECT name FROM restaurants LIMIT 5")
        print(f"🔍 Тест адаптера: {len(df)} строк")