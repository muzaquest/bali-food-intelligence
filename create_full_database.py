#!/usr/bin/env python3
"""
🏗️ СОЗДАНИЕ ПОЛНОЦЕННОЙ БАЗЫ ДАННЫХ
Генерируем 2 года реалистичных данных с сезонностью, трендами и аномалиями
"""

import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import math

def create_realistic_restaurant_data():
    """Создает реалистичную базу данных с 2 годами истории"""
    
    print("🏗️ СОЗДАНИЕ ПОЛНОЦЕННОЙ БАЗЫ ДАННЫХ")
    print("=" * 50)
    
    # Параметры
    start_date = datetime(2023, 1, 1)  # 2 года назад
    end_date = datetime(2024, 12, 31)  
    dates = pd.date_range(start_date, end_date, freq='D')
    
    # Реальные рестораны Бали с характеристиками
    restaurants = [
        {
            'id': 1, 'name': 'Ika Canggu', 'location': 'Canggu', 'cuisine_type': 'Seafood',
            'base_sales': 8500, 'volatility': 0.25, 'growth_rate': 0.15,
            'seasonal_strength': 0.3, 'weekend_boost': 0.4, 'rating_base': 4.2
        },
        {
            'id': 2, 'name': 'Prana Restaurant', 'location': 'Seminyak', 'cuisine_type': 'Fine Dining',
            'base_sales': 15000, 'volatility': 0.15, 'growth_rate': 0.25,
            'seasonal_strength': 0.5, 'weekend_boost': 0.6, 'rating_base': 4.6
        },
        {
            'id': 3, 'name': 'Pinkman Coffee', 'location': 'Ubud', 'cuisine_type': 'Cafe',
            'base_sales': 6000, 'volatility': 0.20, 'growth_rate': 0.30,
            'seasonal_strength': 0.2, 'weekend_boost': 0.3, 'rating_base': 4.4
        },
        {
            'id': 4, 'name': 'Ika Kerobokan', 'location': 'Kerobokan', 'cuisine_type': 'Seafood',
            'base_sales': 7000, 'volatility': 0.30, 'growth_rate': 0.10,
            'seasonal_strength': 0.25, 'weekend_boost': 0.35, 'rating_base': 4.1
        },
        {
            'id': 5, 'name': 'Warung Made', 'location': 'Sanur', 'cuisine_type': 'Local',
            'base_sales': 4500, 'volatility': 0.35, 'growth_rate': 0.05,
            'seasonal_strength': 0.15, 'weekend_boost': 0.25, 'rating_base': 4.3
        }
    ]
    
    # Создаем базу данных
    conn = sqlite3.connect('data/database.sqlite')
    cursor = conn.cursor()
    
    # Очищаем старые таблицы
    cursor.execute('DROP TABLE IF EXISTS restaurants')
    cursor.execute('DROP TABLE IF EXISTS grab_stats')
    cursor.execute('DROP TABLE IF EXISTS gojek_stats')
    
    # Создаем таблицу ресторанов
    cursor.execute('''
        CREATE TABLE restaurants (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            location TEXT NOT NULL,
            cuisine_type TEXT NOT NULL,
            rating REAL NOT NULL,
            latitude REAL,
            longitude REAL
        )
    ''')
    
    # Создаем таблицы статистики
    for platform in ['grab', 'gojek']:
        cursor.execute(f'''
            CREATE TABLE {platform}_stats (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                restaurant_id INTEGER,
                stat_date DATE,
                sales REAL,
                orders INTEGER,
                avg_order_value REAL,
                delivery_time_minutes REAL,
                customer_rating REAL,
                total_customers INTEGER,
                new_customers INTEGER,
                returning_customers INTEGER,
                cancelled_orders INTEGER,
                peak_hour_orders INTEGER,
                off_peak_orders INTEGER,
                weekend_sales REAL,
                weekday_sales REAL,
                promotion_usage INTEGER,
                customer_complaints INTEGER,
                delivery_success_rate REAL,
                menu_items_sold INTEGER,
                marketing_spend REAL,
                staff_efficiency REAL,
                kitchen_capacity_utilization REAL,
                FOREIGN KEY (restaurant_id) REFERENCES restaurants (id)
            )
        ''')
    
    # Заполняем рестораны
    for restaurant in restaurants:
        cursor.execute('''
            INSERT INTO restaurants (id, name, location, cuisine_type, rating, latitude, longitude)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (
            restaurant['id'], restaurant['name'], restaurant['location'], 
            restaurant['cuisine_type'], restaurant['rating_base'],
            -8.6 + random.uniform(-0.3, 0.3),  # Координаты Бали
            115.2 + random.uniform(-0.3, 0.3)
        ))
    
    print(f"✅ Создано {len(restaurants)} ресторанов")
    
    # Генерируем исторические данные
    total_records = 0
    
    for restaurant in restaurants:
        print(f"📊 Генерация данных для {restaurant['name']}...")
        
        for platform in ['grab', 'gojek']:
            platform_modifier = 0.7 if platform == 'gojek' else 1.0
            
            for i, date in enumerate(dates):
                # Базовые факторы
                days_from_start = i
                year_progress = (date - start_date).days / 365.25
                
                # Рост со временем
                growth_factor = 1 + restaurant['growth_rate'] * year_progress
                
                # Сезонность (туристический сезон: июн-сен, дек-янв)
                month = date.month
                is_high_season = month in [6, 7, 8, 9, 12, 1]
                seasonal_factor = 1 + (restaurant['seasonal_strength'] if is_high_season else -restaurant['seasonal_strength'] * 0.3)
                
                # Недельные паттерны
                is_weekend = date.weekday() >= 5
                weekend_factor = 1 + (restaurant['weekend_boost'] if is_weekend else -0.1)
                
                # Особые события и аномалии
                special_events = generate_special_events(date, restaurant['name'])
                event_factor = special_events['sales_multiplier']
                
                # Базовые продажи
                base_sales = restaurant['base_sales'] * platform_modifier
                daily_sales = base_sales * growth_factor * seasonal_factor * weekend_factor * event_factor
                
                # Добавляем шум
                noise = np.random.normal(1, restaurant['volatility'])
                daily_sales = max(100, daily_sales * noise)
                
                # Производные метрики
                orders = max(5, int(daily_sales / (25 + random.uniform(-5, 10))))
                avg_order_value = daily_sales / orders if orders > 0 else 0
                
                delivery_time = 25 + random.uniform(-5, 15) + (5 if is_weekend else 0)
                rating = restaurant['rating_base'] + random.uniform(-0.3, 0.3)
                rating = max(3.0, min(5.0, rating))
                
                total_customers = max(1, int(orders * random.uniform(0.8, 1.2)))
                new_customers = max(0, int(total_customers * random.uniform(0.1, 0.4)))
                returning_customers = total_customers - new_customers
                
                cancelled_orders = max(0, int(orders * random.uniform(0.02, 0.08)))
                peak_orders = int(orders * random.uniform(0.6, 0.8))
                off_peak_orders = orders - peak_orders
                
                # Маркетинг (реалистичные кампании)
                marketing_spend = generate_marketing_spend(date, restaurant['name'], daily_sales)
                
                # Вставляем запись
                cursor.execute(f'''
                    INSERT INTO {platform}_stats (
                        restaurant_id, stat_date, sales, orders, avg_order_value,
                        delivery_time_minutes, customer_rating, total_customers,
                        new_customers, returning_customers, cancelled_orders,
                        peak_hour_orders, off_peak_orders, weekend_sales, weekday_sales,
                        promotion_usage, customer_complaints, delivery_success_rate,
                        menu_items_sold, marketing_spend, staff_efficiency,
                        kitchen_capacity_utilization
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    restaurant['id'], date.strftime('%Y-%m-%d'), 
                    round(daily_sales, 2), orders, round(avg_order_value, 2),
                    round(delivery_time, 1), round(rating, 2), total_customers,
                    new_customers, returning_customers, cancelled_orders,
                    peak_orders, off_peak_orders,
                    round(daily_sales if is_weekend else 0, 2),
                    round(0 if is_weekend else daily_sales, 2),
                    random.randint(0, 3), random.randint(0, 2),
                    round(random.uniform(0.92, 0.98), 3),
                    random.randint(15, 45), round(marketing_spend, 2),
                    round(random.uniform(0.7, 0.95), 2),
                    round(random.uniform(0.6, 0.9), 2)
                ))
                
                total_records += 1
    
    conn.commit()
    conn.close()
    
    print(f"✅ База данных создана: {total_records} записей за 2 года")
    print(f"📅 Период: {start_date.strftime('%Y-%m-%d')} - {end_date.strftime('%Y-%m-%d')}")
    print(f"🏪 Рестораны: {len(restaurants)}")

def generate_special_events(date, restaurant_name):
    """Генерирует особые события и аномалии"""
    
    events = {
        'sales_multiplier': 1.0,
        'event_name': None
    }
    
    # Праздники и события Бали
    month, day = date.month, date.day
    
    # Крупные праздники
    holidays = {
        (1, 1): ('New Year', 1.8),
        (3, 22): ('Nyepi (Balinese New Year)', 0.1),  # Все закрыто
        (8, 17): ('Independence Day', 1.4),
        (12, 25): ('Christmas', 1.6),
        (12, 31): ('New Year Eve', 2.0)
    }
    
    if (month, day) in holidays:
        events['event_name'] = holidays[(month, day)][0]
        events['sales_multiplier'] = holidays[(month, day)][1]
        return events
    
    # Сезонные события
    if month in [7, 8]:  # Пик туристического сезона
        if random.random() < 0.05:  # 5% вероятность особого события
            events['event_name'] = 'Tourist Peak Event'
            events['sales_multiplier'] = random.uniform(1.5, 2.2)
    
    # Негативные события (редко)
    if random.random() < 0.01:  # 1% вероятность
        negative_events = [
            ('Bad Weather', random.uniform(0.3, 0.7)),
            ('Power Outage', random.uniform(0.1, 0.4)),
            ('Supply Issues', random.uniform(0.5, 0.8))
        ]
        event = random.choice(negative_events)
        events['event_name'] = event[0]
        events['sales_multiplier'] = event[1]
    
    # Положительные события для конкретных ресторанов
    if restaurant_name == 'Prana Restaurant' and random.random() < 0.02:
        events['event_name'] = 'Celebrity Visit'
        events['sales_multiplier'] = random.uniform(2.0, 3.5)
    elif restaurant_name == 'Ika Canggu' and random.random() < 0.015:
        events['event_name'] = 'Surfing Competition'
        events['sales_multiplier'] = random.uniform(1.8, 2.5)
    
    return events

def generate_marketing_spend(date, restaurant_name, daily_sales):
    """Генерирует реалистичные расходы на маркетинг"""
    
    # Базовые расходы в % от продаж
    base_rate = {
        'Prana Restaurant': 0.15,
        'Ika Canggu': 0.08,
        'Pinkman Coffee': 0.12,
        'Ika Kerobokan': 0.06,
        'Warung Made': 0.04
    }.get(restaurant_name, 0.08)
    
    # Увеличиваем маркетинг в туристический сезон
    month = date.month
    is_high_season = month in [6, 7, 8, 9, 12, 1]
    seasonal_multiplier = 1.5 if is_high_season else 1.0
    
    # Иногда нет рекламы
    if random.random() < 0.3:  # 30% дней без рекламы
        return 0
    
    # Случайные кампании
    campaign_multiplier = 1.0
    if random.random() < 0.05:  # 5% дней - большая кампания
        campaign_multiplier = random.uniform(3.0, 8.0)
    elif random.random() < 0.15:  # 15% дней - средняя кампания
        campaign_multiplier = random.uniform(1.5, 3.0)
    
    marketing = daily_sales * base_rate * seasonal_multiplier * campaign_multiplier
    return max(0, marketing + random.uniform(-marketing*0.3, marketing*0.3))

if __name__ == "__main__":
    create_realistic_restaurant_data()
    
    # Проверим что получилось
    print("\n🔍 ПРОВЕРКА СОЗДАННОЙ БАЗЫ:")
    
    conn = sqlite3.connect('data/database.sqlite')
    cursor = conn.cursor()
    
    cursor.execute('SELECT COUNT(*) FROM grab_stats')
    grab_count = cursor.fetchone()[0]
    
    cursor.execute('SELECT COUNT(*) FROM gojek_stats') 
    gojek_count = cursor.fetchone()[0]
    
    cursor.execute('SELECT MIN(stat_date), MAX(stat_date) FROM grab_stats')
    date_range = cursor.fetchone()
    
    cursor.execute('SELECT name FROM restaurants')
    restaurants = [row[0] for row in cursor.fetchall()]
    
    cursor.execute('SELECT AVG(sales), MIN(sales), MAX(sales) FROM grab_stats')
    sales_stats = cursor.fetchone()
    
    conn.close()
    
    print(f"📊 Grab записей: {grab_count:,}")
    print(f"📊 Gojek записей: {gojek_count:,}")  
    print(f"📅 Диапазон дат: {date_range[0]} - {date_range[1]}")
    print(f"🏪 Рестораны: {', '.join(restaurants)}")
    print(f"💰 Продажи: avg={sales_stats[0]:,.0f}, min={sales_stats[1]:,.0f}, max={sales_stats[2]:,.0f}")
    
    print("\n🎉 ГОТОВО! База данных с полной историей создана")