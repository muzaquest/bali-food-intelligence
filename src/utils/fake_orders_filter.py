"""
Модуль для работы с fake orders
Исключает накрученные заказы из анализа для получения точных данных
"""

import json
import requests
import csv
from io import StringIO
from datetime import datetime
from typing import List, Dict, Set, Tuple
import os


class FakeOrdersFilter:
    """Класс для работы с fake orders"""
    
    def __init__(self):
        """Инициализация фильтра fake orders"""
        self.fake_orders_data = []
        self.fake_orders_by_restaurant = {}
        self.fake_orders_by_date = {}
        self.last_update = None
        
        # Данные таблицы Google Sheets
        self.sheet_id = '1LRkQeh6lzgRY96HECT5nc5cZKjA475LZHcuRipX14qM'
        self.gid = '1724820690'
        self.csv_url = f'https://docs.google.com/spreadsheets/d/{self.sheet_id}/export?format=csv&gid={self.gid}'
        
        # Загружаем данные
        self._load_fake_orders()
    
    def _load_fake_orders(self):
        """Загружает данные о fake orders"""
        try:
            # Сначала пробуем загрузить из локального файла
            if os.path.exists('fake_orders_data.json'):
                with open('fake_orders_data.json', 'r', encoding='utf-8') as f:
                    self.fake_orders_data = json.load(f)
                print(f"✅ Загружено {len(self.fake_orders_data)} fake orders из локального файла")
            else:
                # Загружаем из Google Sheets
                self._download_fake_orders()
            
            # Индексируем данные для быстрого поиска
            self._index_fake_orders()
            
        except Exception as e:
            print(f"⚠️ Ошибка загрузки fake orders: {e}")
            self.fake_orders_data = []
    
    def _download_fake_orders(self):
        """Загружает данные из Google Sheets"""
        try:
            print("🌐 Загружаем fake orders из Google Sheets...")
            
            response = requests.get(self.csv_url, timeout=15)
            if response.status_code == 200:
                content = response.content.decode('utf-8')
                csv_reader = csv.reader(StringIO(content))
                rows = list(csv_reader)
                
                fake_orders = []
                for row in rows[1:]:  # Пропускаем заголовок
                    if len(row) >= 6 and row[1].strip():  # Есть ресторан
                        fake_order = {
                            'timestamp': row[0].strip(),
                            'restaurant': row[1].strip(),
                            'date': row[2].strip(),
                            'quantity': row[3].strip(),
                            'amount': row[4].strip(),
                            'platform': row[5].strip()
                        }
                        fake_orders.append(fake_order)
                
                self.fake_orders_data = fake_orders
                
                # Сохраняем в локальный файл
                with open('fake_orders_data.json', 'w', encoding='utf-8') as f:
                    json.dump(fake_orders, f, ensure_ascii=False, indent=2)
                
                print(f"✅ Загружено {len(fake_orders)} fake orders из Google Sheets")
                
            else:
                print(f"❌ Ошибка загрузки: {response.status_code}")
                
        except Exception as e:
            print(f"❌ Ошибка загрузки из Google Sheets: {e}")
    
    def _index_fake_orders(self):
        """Создает индексы для быстрого поиска"""
        self.fake_orders_by_restaurant = {}
        self.fake_orders_by_date = {}
        
        for order in self.fake_orders_data:
            restaurant = order['restaurant']
            date = self._normalize_date(order['date'])
            platform = order['platform']
            quantity = int(order['quantity']) if order['quantity'].isdigit() else 0
            amount = float(order['amount']) if order['amount'].replace('.', '').isdigit() else 0
            
            # Индекс по ресторанам
            if restaurant not in self.fake_orders_by_restaurant:
                self.fake_orders_by_restaurant[restaurant] = {}
            
            if date not in self.fake_orders_by_restaurant[restaurant]:
                self.fake_orders_by_restaurant[restaurant][date] = {
                    'Grab': {'quantity': 0, 'amount': 0},
                    'Gojek': {'quantity': 0, 'amount': 0}
                }
            
            if platform in ['Grab', 'Gojek']:
                self.fake_orders_by_restaurant[restaurant][date][platform]['quantity'] += quantity
                self.fake_orders_by_restaurant[restaurant][date][platform]['amount'] += amount
            
            # Индекс по датам
            if date not in self.fake_orders_by_date:
                self.fake_orders_by_date[date] = {}
            
            if restaurant not in self.fake_orders_by_date[date]:
                self.fake_orders_by_date[date][restaurant] = {
                    'Grab': {'quantity': 0, 'amount': 0},
                    'Gojek': {'quantity': 0, 'amount': 0}
                }
            
            if platform in ['Grab', 'Gojek']:
                self.fake_orders_by_date[date][restaurant][platform]['quantity'] += quantity
                self.fake_orders_by_date[date][restaurant][platform]['amount'] += amount
    
    def _normalize_date(self, date_str):
        """Нормализует дату в формат YYYY-MM-DD"""
        try:
            # Пробуем разные форматы дат
            formats = ['%d/%m/%Y', '%Y-%m-%d', '%d.%m.%Y']
            
            for fmt in formats:
                try:
                    date_obj = datetime.strptime(date_str, fmt)
                    return date_obj.strftime('%Y-%m-%d')
                except ValueError:
                    continue
            
            # Если не удалось распарсить, возвращаем как есть
            return date_str
            
        except:
            return date_str
    
    def get_fake_orders_for_restaurant_date(self, restaurant_name, date):
        """Получает fake orders для конкретного ресторана и даты"""
        normalized_date = self._normalize_date(date)
        
        if restaurant_name in self.fake_orders_by_restaurant:
            if normalized_date in self.fake_orders_by_restaurant[restaurant_name]:
                return self.fake_orders_by_restaurant[restaurant_name][normalized_date]
        
        return {'Grab': {'quantity': 0, 'amount': 0}, 'Gojek': {'quantity': 0, 'amount': 0}}
    
    def adjust_sales_data(self, restaurant_name, date, grab_sales, grab_orders, gojek_sales, gojek_orders):
        """Корректирует данные о продажах, исключая fake orders"""
        fake_orders = self.get_fake_orders_for_restaurant_date(restaurant_name, date)
        
        # Корректируем Grab данные
        adjusted_grab_sales = max(0, grab_sales - fake_orders['Grab']['amount'])
        adjusted_grab_orders = max(0, grab_orders - fake_orders['Grab']['quantity'])
        
        # Корректируем Gojek данные
        adjusted_gojek_sales = max(0, gojek_sales - fake_orders['Gojek']['amount'])
        adjusted_gojek_orders = max(0, gojek_orders - fake_orders['Gojek']['quantity'])
        
        return {
            'grab_sales_original': grab_sales,
            'grab_sales_adjusted': adjusted_grab_sales,
            'grab_orders_original': grab_orders,
            'grab_orders_adjusted': adjusted_grab_orders,
            'gojek_sales_original': gojek_sales,
            'gojek_sales_adjusted': adjusted_gojek_sales,
            'gojek_orders_original': gojek_orders,
            'gojek_orders_adjusted': adjusted_gojek_orders,
            'fake_orders_removed': {
                'grab_fake_orders': fake_orders['Grab']['quantity'],
                'grab_fake_amount': fake_orders['Grab']['amount'],
                'gojek_fake_orders': fake_orders['Gojek']['quantity'],
                'gojek_fake_amount': fake_orders['Gojek']['amount']
            }
        }
    
    def get_fake_orders_summary(self, restaurant_name=None, start_date=None, end_date=None):
        """Получает сводку по fake orders"""
        summary = {
            'total_fake_orders': 0,
            'total_fake_amount': 0,
            'by_platform': {'Grab': 0, 'Gojek': 0},
            'by_restaurant': {},
            'affected_dates': set()
        }
        
        for order in self.fake_orders_data:
            order_restaurant = order['restaurant']
            order_date = self._normalize_date(order['date'])
            order_platform = order['platform']
            order_quantity = int(order['quantity']) if order['quantity'].isdigit() else 0
            order_amount = float(order['amount']) if order['amount'].replace('.', '').isdigit() else 0
            
            # Фильтры
            if restaurant_name and order_restaurant != restaurant_name:
                continue
            
            if start_date and order_date < start_date:
                continue
                
            if end_date and order_date > end_date:
                continue
            
            # Подсчет
            summary['total_fake_orders'] += order_quantity
            summary['total_fake_amount'] += order_amount
            
            if order_platform in summary['by_platform']:
                summary['by_platform'][order_platform] += order_quantity
            
            if order_restaurant not in summary['by_restaurant']:
                summary['by_restaurant'][order_restaurant] = 0
            summary['by_restaurant'][order_restaurant] += order_quantity
            
            summary['affected_dates'].add(order_date)
        
        summary['affected_dates'] = sorted(list(summary['affected_dates']))
        
        return summary
    
    def has_fake_orders(self, restaurant_name, date):
        """Проверяет есть ли fake orders для ресторана в конкретную дату"""
        fake_orders = self.get_fake_orders_for_restaurant_date(restaurant_name, date)
        return (fake_orders['Grab']['quantity'] > 0 or 
                fake_orders['Gojek']['quantity'] > 0)
    
    def get_all_restaurants_with_fake_orders(self):
        """Возвращает список всех ресторанов с fake orders"""
        return list(self.fake_orders_by_restaurant.keys())
    
    def refresh_data(self):
        """Обновляет данные из Google Sheets"""
        try:
            self._download_fake_orders()
            self._index_fake_orders()
            print("✅ Данные fake orders обновлены")
        except Exception as e:
            print(f"❌ Ошибка обновления fake orders: {e}")


# Глобальный экземпляр фильтра
fake_orders_filter = FakeOrdersFilter()


def get_fake_orders_filter():
    """Возвращает глобальный экземпляр фильтра fake orders"""
    return fake_orders_filter


if __name__ == "__main__":
    # Тестирование модуля
    filter_instance = FakeOrdersFilter()
    
    print("🧪 ТЕСТИРОВАНИЕ FAKE ORDERS FILTER")
    print("=" * 50)
    
    # Тест 1: Общая статистика
    summary = filter_instance.get_fake_orders_summary()
    print(f"📊 Всего fake orders: {summary['total_fake_orders']}")
    print(f"💰 Общая сумма: {summary['total_fake_amount']:,.0f} IDR")
    print(f"🚗 По платформам: {summary['by_platform']}")
    
    # Тест 2: Проверка конкретного ресторана
    test_restaurant = "Only Eggs"
    test_date = "2025-05-15"
    
    fake_data = filter_instance.get_fake_orders_for_restaurant_date(test_restaurant, test_date)
    print(f"\n🍽️ Fake orders для {test_restaurant} на {test_date}:")
    print(f"   Grab: {fake_data['Grab']['quantity']} заказов, {fake_data['Grab']['amount']:,.0f} IDR")
    print(f"   Gojek: {fake_data['Gojek']['quantity']} заказов, {fake_data['Gojek']['amount']:,.0f} IDR")
    
    # Тест 3: Корректировка данных
    adjusted = filter_instance.adjust_sales_data(test_restaurant, test_date, 1000000, 50, 500000, 25)
    print(f"\n🔧 Корректировка данных для {test_restaurant}:")
    print(f"   Grab: {adjusted['grab_sales_original']:,} → {adjusted['grab_sales_adjusted']:,} IDR")
    print(f"   Gojek: {adjusted['gojek_sales_original']:,} → {adjusted['gojek_sales_adjusted']:,} IDR")