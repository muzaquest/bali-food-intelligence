#!/usr/bin/env python3
"""
Client Database API Integration для подключения к базе данных клиента
"""

import sqlite3
import requests
import json
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Union
import logging

class ClientDatabaseAPI:
    """Сервис для работы с базой данных клиента"""
    
    def __init__(self, connection_config: Dict):
        """
        Инициализация подключения к базе данных
        
        connection_config может содержать:
        - type: 'sqlite', 'mysql', 'postgresql', 'api'
        - host, port, database, username, password для SQL баз
        - api_url, api_key для REST API
        - file_path для SQLite
        """
        self.config = connection_config
        self.connection_type = connection_config.get('type', 'sqlite')
        self.connection = None
        
        # Настройка логирования
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
        
        # Инициализация подключения
        self._initialize_connection()
    
    def _initialize_connection(self):
        """Инициализирует подключение к базе данных"""
        try:
            if self.connection_type == 'sqlite':
                self.connection = sqlite3.connect(
                    self.config.get('file_path', 'client_data.db'),
                    check_same_thread=False
                )
                self.connection.row_factory = sqlite3.Row
                
            elif self.connection_type == 'mysql':
                import mysql.connector
                self.connection = mysql.connector.connect(
                    host=self.config['host'],
                    port=self.config.get('port', 3306),
                    database=self.config['database'],
                    user=self.config['username'],
                    password=self.config['password']
                )
                
            elif self.connection_type == 'postgresql':
                import psycopg2
                self.connection = psycopg2.connect(
                    host=self.config['host'],
                    port=self.config.get('port', 5432),
                    database=self.config['database'],
                    user=self.config['username'],
                    password=self.config['password']
                )
                
            elif self.connection_type == 'api':
                # Для REST API подключения
                self.api_url = self.config['api_url']
                self.api_key = self.config.get('api_key')
                self.headers = {
                    'Authorization': f'Bearer {self.api_key}' if self.api_key else '',
                    'Content-Type': 'application/json'
                }
                
            self.logger.info(f"Подключение к базе данных {self.connection_type} успешно")
            
        except Exception as e:
            self.logger.error(f"Ошибка подключения к базе данных: {e}")
            raise
    
    def get_daily_stats(self, restaurant_id: int, date: str) -> Dict:
        """Получает статистику ресторана за день"""
        try:
            if self.connection_type == 'api':
                return self._get_daily_stats_api(restaurant_id, date)
            else:
                return self._get_daily_stats_sql(restaurant_id, date)
                
        except Exception as e:
            self.logger.error(f"Ошибка получения данных за день: {e}")
            return self._get_default_daily_stats()
    
    def get_historical_data(self, restaurant_id: int, days: int = 30) -> List[Dict]:
        """Получает исторические данные ресторана"""
        try:
            if self.connection_type == 'api':
                return self._get_historical_data_api(restaurant_id, days)
            else:
                return self._get_historical_data_sql(restaurant_id, days)
                
        except Exception as e:
            self.logger.error(f"Ошибка получения исторических данных: {e}")
            return []
    
    def get_restaurant_info(self, restaurant_id: int) -> Dict:
        """Получает информацию о ресторане"""
        try:
            if self.connection_type == 'api':
                return self._get_restaurant_info_api(restaurant_id)
            else:
                return self._get_restaurant_info_sql(restaurant_id)
                
        except Exception as e:
            self.logger.error(f"Ошибка получения информации о ресторане: {e}")
            return {'id': restaurant_id, 'name': 'Unknown', 'region': 'Unknown'}
    
    def get_all_restaurants(self) -> List[Dict]:
        """Получает список всех ресторанов"""
        try:
            if self.connection_type == 'api':
                return self._get_all_restaurants_api()
            else:
                return self._get_all_restaurants_sql()
                
        except Exception as e:
            self.logger.error(f"Ошибка получения списка ресторанов: {e}")
            return []
    
    def get_combined_platform_data(self, restaurant_id: int, date: str) -> Dict:
        """Получает объединенные данные Grab и Gojek"""
        try:
            if self.connection_type == 'api':
                return self._get_combined_data_api(restaurant_id, date)
            else:
                return self._get_combined_data_sql(restaurant_id, date)
                
        except Exception as e:
            self.logger.error(f"Ошибка получения объединенных данных: {e}")
            return self._get_default_combined_data()
    
    def update_restaurant_data(self, restaurant_id: int, data: Dict) -> bool:
        """Обновляет данные ресторана"""
        try:
            if self.connection_type == 'api':
                return self._update_data_api(restaurant_id, data)
            else:
                return self._update_data_sql(restaurant_id, data)
                
        except Exception as e:
            self.logger.error(f"Ошибка обновления данных: {e}")
            return False
    
    # SQL методы
    def _get_daily_stats_sql(self, restaurant_id: int, date: str) -> Dict:
        """Получает дневную статистику через SQL"""
        query = """
        SELECT 
            r.name,
            r.region,
            COALESCE(g.sales, 0) + COALESCE(gr.sales, 0) as total_sales,
            COALESCE(g.orders, 0) + COALESCE(gr.orders, 0) as total_orders,
            COALESCE(g.ads_spend, 0) + COALESCE(gr.ads_spend, 0) as total_ads_spend,
            COALESCE(g.rating, gr.rating, 0) as rating,
            COALESCE(g.cancellation_rate, gr.cancellation_rate, 0) as cancellation_rate,
            COALESCE(g.delivery_time, gr.delivery_time, 0) as delivery_time
        FROM restaurants r
        LEFT JOIN gojek_stats g ON r.id = g.restaurant_id AND g.date = ?
        LEFT JOIN grab_stats gr ON r.id = gr.restaurant_id AND gr.date = ?
        WHERE r.id = ?
        """
        
        cursor = self.connection.cursor()
        cursor.execute(query, (date, date, restaurant_id))
        row = cursor.fetchone()
        
        if row:
            return {
                'restaurant_name': row['name'],
                'region': row['region'],
                'sales': row['total_sales'],
                'orders': row['total_orders'],
                'ads_spend': row['total_ads_spend'],
                'rating': row['rating'],
                'cancellation_rate': row['cancellation_rate'],
                'delivery_time': row['delivery_time'],
                'avg_order_value': row['total_sales'] / row['total_orders'] if row['total_orders'] > 0 else 0,
                'ads_enabled': row['total_ads_spend'] > 0,
                'date': date
            }
        
        return self._get_default_daily_stats()
    
    def _get_historical_data_sql(self, restaurant_id: int, days: int) -> List[Dict]:
        """Получает исторические данные через SQL"""
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=days)
        
        query = """
        SELECT 
            date,
            COALESCE(g.sales, 0) + COALESCE(gr.sales, 0) as total_sales,
            COALESCE(g.orders, 0) + COALESCE(gr.orders, 0) as total_orders,
            COALESCE(g.ads_spend, 0) + COALESCE(gr.ads_spend, 0) as total_ads_spend,
            COALESCE(g.rating, gr.rating, 0) as rating,
            COALESCE(g.cancellation_rate, gr.cancellation_rate, 0) as cancellation_rate
        FROM (
            SELECT DISTINCT date FROM gojek_stats 
            WHERE restaurant_id = ? AND date BETWEEN ? AND ?
            UNION
            SELECT DISTINCT date FROM grab_stats 
            WHERE restaurant_id = ? AND date BETWEEN ? AND ?
        ) dates
        LEFT JOIN gojek_stats g ON dates.date = g.date AND g.restaurant_id = ?
        LEFT JOIN grab_stats gr ON dates.date = gr.date AND gr.restaurant_id = ?
        ORDER BY date DESC
        """
        
        cursor = self.connection.cursor()
        cursor.execute(query, (
            restaurant_id, start_date, end_date,
            restaurant_id, start_date, end_date,
            restaurant_id, restaurant_id
        ))
        
        results = []
        for row in cursor.fetchall():
            results.append({
                'date': row['date'],
                'sales': row['total_sales'],
                'orders': row['total_orders'],
                'ads_spend': row['total_ads_spend'],
                'rating': row['rating'],
                'cancellation_rate': row['cancellation_rate'],
                'avg_order_value': row['total_sales'] / row['total_orders'] if row['total_orders'] > 0 else 0
            })
        
        return results
    
    def _get_restaurant_info_sql(self, restaurant_id: int) -> Dict:
        """Получает информацию о ресторане через SQL"""
        query = "SELECT * FROM restaurants WHERE id = ?"
        cursor = self.connection.cursor()
        cursor.execute(query, (restaurant_id,))
        row = cursor.fetchone()
        
        if row:
            return {
                'id': row['id'],
                'name': row['name'],
                'region': row['region'],
                'token': row.get('token', ''),
                'created_at': row.get('created_at', '')
            }
        
        return {'id': restaurant_id, 'name': 'Unknown', 'region': 'Unknown'}
    
    def _get_all_restaurants_sql(self) -> List[Dict]:
        """Получает все рестораны через SQL"""
        query = "SELECT * FROM restaurants ORDER BY name"
        cursor = self.connection.cursor()
        cursor.execute(query)
        
        restaurants = []
        for row in cursor.fetchall():
            restaurants.append({
                'id': row['id'],
                'name': row['name'],
                'region': row['region'],
                'token': row.get('token', ''),
                'created_at': row.get('created_at', '')
            })
        
        return restaurants
    
    def _get_combined_data_sql(self, restaurant_id: int, date: str) -> Dict:
        """Получает объединенные данные платформ через SQL"""
        query = """
        SELECT 
            'gojek' as platform,
            sales, orders, ads_spend, rating, cancellation_rate, delivery_time
        FROM gojek_stats 
        WHERE restaurant_id = ? AND date = ?
        UNION ALL
        SELECT 
            'grab' as platform,
            sales, orders, ads_spend, rating, cancellation_rate, delivery_time
        FROM grab_stats 
        WHERE restaurant_id = ? AND date = ?
        """
        
        cursor = self.connection.cursor()
        cursor.execute(query, (restaurant_id, date, restaurant_id, date))
        
        platforms = {'gojek': {}, 'grab': {}}
        total_sales = 0
        total_orders = 0
        
        for row in cursor.fetchall():
            platform = row['platform']
            platforms[platform] = {
                'sales': row['sales'],
                'orders': row['orders'],
                'ads_spend': row['ads_spend'],
                'rating': row['rating'],
                'cancellation_rate': row['cancellation_rate'],
                'delivery_time': row['delivery_time']
            }
            total_sales += row['sales']
            total_orders += row['orders']
        
        return {
            'date': date,
            'restaurant_id': restaurant_id,
            'platforms': platforms,
            'totals': {
                'sales': total_sales,
                'orders': total_orders,
                'avg_order_value': total_sales / total_orders if total_orders > 0 else 0
            }
        }
    
    def _update_data_sql(self, restaurant_id: int, data: Dict) -> bool:
        """Обновляет данные через SQL"""
        try:
            # Пример обновления данных ресторана
            query = """
            UPDATE restaurants 
            SET name = ?, region = ?, token = ? 
            WHERE id = ?
            """
            
            cursor = self.connection.cursor()
            cursor.execute(query, (
                data.get('name'),
                data.get('region'),
                data.get('token'),
                restaurant_id
            ))
            
            self.connection.commit()
            return True
            
        except Exception as e:
            self.logger.error(f"Ошибка обновления SQL: {e}")
            return False
    
    # API методы
    def _get_daily_stats_api(self, restaurant_id: int, date: str) -> Dict:
        """Получает дневную статистику через API"""
        url = f"{self.api_url}/restaurants/{restaurant_id}/stats/daily"
        params = {'date': date}
        
        response = requests.get(url, headers=self.headers, params=params)
        response.raise_for_status()
        
        data = response.json()
        return {
            'restaurant_name': data.get('restaurant_name', 'Unknown'),
            'region': data.get('region', 'Unknown'),
            'sales': data.get('total_sales', 0),
            'orders': data.get('total_orders', 0),
            'ads_spend': data.get('ads_spend', 0),
            'rating': data.get('rating', 0),
            'cancellation_rate': data.get('cancellation_rate', 0),
            'delivery_time': data.get('delivery_time', 0),
            'avg_order_value': data.get('avg_order_value', 0),
            'ads_enabled': data.get('ads_enabled', False),
            'date': date
        }
    
    def _get_historical_data_api(self, restaurant_id: int, days: int) -> List[Dict]:
        """Получает исторические данные через API"""
        url = f"{self.api_url}/restaurants/{restaurant_id}/stats/historical"
        params = {'days': days}
        
        response = requests.get(url, headers=self.headers, params=params)
        response.raise_for_status()
        
        return response.json().get('data', [])
    
    def _get_restaurant_info_api(self, restaurant_id: int) -> Dict:
        """Получает информацию о ресторане через API"""
        url = f"{self.api_url}/restaurants/{restaurant_id}"
        
        response = requests.get(url, headers=self.headers)
        response.raise_for_status()
        
        return response.json()
    
    def _get_all_restaurants_api(self) -> List[Dict]:
        """Получает все рестораны через API"""
        url = f"{self.api_url}/restaurants"
        
        response = requests.get(url, headers=self.headers)
        response.raise_for_status()
        
        return response.json().get('restaurants', [])
    
    def _get_combined_data_api(self, restaurant_id: int, date: str) -> Dict:
        """Получает объединенные данные через API"""
        url = f"{self.api_url}/restaurants/{restaurant_id}/stats/combined"
        params = {'date': date}
        
        response = requests.get(url, headers=self.headers, params=params)
        response.raise_for_status()
        
        return response.json()
    
    def _update_data_api(self, restaurant_id: int, data: Dict) -> bool:
        """Обновляет данные через API"""
        url = f"{self.api_url}/restaurants/{restaurant_id}"
        
        response = requests.put(url, headers=self.headers, json=data)
        return response.status_code == 200
    
    # Вспомогательные методы
    def _get_default_daily_stats(self) -> Dict:
        """Возвращает статистику по умолчанию"""
        return {
            'restaurant_name': 'Unknown',
            'region': 'Unknown',
            'sales': 0,
            'orders': 0,
            'ads_spend': 0,
            'rating': 0,
            'cancellation_rate': 0,
            'delivery_time': 0,
            'avg_order_value': 0,
            'ads_enabled': False,
            'date': datetime.now().strftime('%Y-%m-%d')
        }
    
    def _get_default_combined_data(self) -> Dict:
        """Возвращает объединенные данные по умолчанию"""
        return {
            'date': datetime.now().strftime('%Y-%m-%d'),
            'restaurant_id': 0,
            'platforms': {
                'gojek': {'sales': 0, 'orders': 0, 'ads_spend': 0, 'rating': 0},
                'grab': {'sales': 0, 'orders': 0, 'ads_spend': 0, 'rating': 0}
            },
            'totals': {'sales': 0, 'orders': 0, 'avg_order_value': 0}
        }
    
    def test_connection(self) -> bool:
        """Тестирует подключение к базе данных"""
        try:
            if self.connection_type == 'api':
                response = requests.get(f"{self.api_url}/health", headers=self.headers)
                return response.status_code == 200
            else:
                cursor = self.connection.cursor()
                cursor.execute("SELECT 1")
                return True
                
        except Exception as e:
            self.logger.error(f"Ошибка тестирования подключения: {e}")
            return False
    
    def get_data_quality_report(self, restaurant_id: int, days: int = 30) -> Dict:
        """Генерирует отчет о качестве данных"""
        try:
            historical_data = self.get_historical_data(restaurant_id, days)
            
            total_days = len(historical_data)
            missing_sales = sum(1 for d in historical_data if d['sales'] == 0)
            missing_orders = sum(1 for d in historical_data if d['orders'] == 0)
            
            return {
                'restaurant_id': restaurant_id,
                'period_days': days,
                'total_records': total_days,
                'data_completeness': (total_days - missing_sales) / total_days if total_days > 0 else 0,
                'missing_sales_days': missing_sales,
                'missing_orders_days': missing_orders,
                'avg_daily_sales': sum(d['sales'] for d in historical_data) / total_days if total_days > 0 else 0,
                'data_quality_score': self._calculate_quality_score(historical_data)
            }
            
        except Exception as e:
            self.logger.error(f"Ошибка генерации отчета качества: {e}")
            return {'error': str(e)}
    
    def _calculate_quality_score(self, data: List[Dict]) -> float:
        """Рассчитывает оценку качества данных (0-1)"""
        if not data:
            return 0.0
        
        score = 0.0
        total_checks = 0
        
        for record in data:
            # Проверка наличия продаж
            if record['sales'] > 0:
                score += 0.3
            
            # Проверка наличия заказов
            if record['orders'] > 0:
                score += 0.3
            
            # Проверка разумности среднего чека
            if record['avg_order_value'] > 10000:  # Минимальный чек 10k IDR
                score += 0.2
            
            # Проверка рейтинга
            if record['rating'] > 0:
                score += 0.2
            
            total_checks += 1
        
        return score / total_checks if total_checks > 0 else 0.0
    
    def close_connection(self):
        """Закрывает подключение к базе данных"""
        if self.connection and self.connection_type != 'api':
            self.connection.close()
            self.logger.info("Подключение к базе данных закрыто")

# Пример использования
def main():
    # Конфигурация для SQLite
    sqlite_config = {
        'type': 'sqlite',
        'file_path': 'client_data.db'
    }
    
    # Конфигурация для API
    api_config = {
        'type': 'api',
        'api_url': 'https://api.client.com/v1',
        'api_key': 'your_api_key_here'
    }
    
    # Создаем подключение
    db_api = ClientDatabaseAPI(sqlite_config)
    
    # Тестируем подключение
    if db_api.test_connection():
        print("✅ Подключение к базе данных успешно")
        
        # Получаем данные за день
        daily_stats = db_api.get_daily_stats(1, '2024-01-15')
        print(f"Продажи за день: {daily_stats['sales']:,.0f} IDR")
        
        # Получаем отчет о качестве данных
        quality_report = db_api.get_data_quality_report(1, 30)
        print(f"Качество данных: {quality_report['data_quality_score']:.2f}")
        
    else:
        print("❌ Ошибка подключения к базе данных")
    
    # Закрываем подключение
    db_api.close_connection()

if __name__ == "__main__":
    main()