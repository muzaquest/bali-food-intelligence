#!/usr/bin/env python3
"""
Создание тестовых CSV файлов для демонстрации системы
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random

def create_test_restaurants():
    """Создает тестовую таблицу ресторанов"""
    restaurants = [
        {"id": 1, "name": "Warung Bali Asli", "region": "Seminyak", "grab_restaurant_id": "grab_001", "gojek_restaurant_id": "gojek_001"},
        {"id": 2, "name": "Ubud Organic Cafe", "region": "Ubud", "grab_restaurant_id": "grab_002", "gojek_restaurant_id": "gojek_002"},
        {"id": 3, "name": "Canggu Surf Cafe", "region": "Canggu", "grab_restaurant_id": "grab_003", "gojek_restaurant_id": "gojek_003"},
        {"id": 4, "name": "Denpasar Local", "region": "Denpasar", "grab_restaurant_id": "grab_004", "gojek_restaurant_id": "gojek_004"},
        {"id": 5, "name": "Sanur Beach Resto", "region": "Sanur", "grab_restaurant_id": "grab_005", "gojek_restaurant_id": "gojek_005"},
        {"id": 6, "name": "Nusa Dua Fine Dining", "region": "Seminyak", "grab_restaurant_id": "grab_006", "gojek_restaurant_id": "gojek_006"},
        {"id": 7, "name": "Jimbaran Seafood", "region": "Canggu", "grab_restaurant_id": "grab_007", "gojek_restaurant_id": "gojek_007"},
        {"id": 8, "name": "Kuta Night Market", "region": "Denpasar", "grab_restaurant_id": "grab_008", "gojek_restaurant_id": "gojek_008"},
    ]
    
    df = pd.DataFrame(restaurants)
    df['connected_date'] = '2022-01-01'
    df['cuisine_type'] = ['Indonesian', 'Healthy', 'Western', 'Local', 'Seafood', 'Fine Dining', 'Seafood', 'Street Food']
    df['rating'] = np.random.uniform(3.8, 4.8, len(df)).round(1)
    df['avg_delivery_time'] = np.random.randint(20, 45, len(df))
    
    return df

def create_test_grab_stats():
    """Создает тестовую статистику Grab"""
    restaurants = list(range(1, 9))
    
    # Генерируем данные за 2 года
    start_date = datetime(2022, 1, 1)
    end_date = datetime(2024, 1, 1)
    
    data = []
    current_date = start_date
    
    while current_date < end_date:
        for restaurant_id in restaurants:
            # Базовые продажи зависят от региона и ресторана
            base_sales = {
                1: 3500000,  # Seminyak - высокие продажи
                2: 2800000,  # Ubud - средние
                3: 3200000,  # Canggu - высокие
                4: 2200000,  # Denpasar - низкие
                5: 2600000,  # Sanur - средние
                6: 4200000,  # Seminyak Fine Dining - очень высокие
                7: 3800000,  # Jimbaran Seafood - высокие
                8: 1800000,  # Kuta Street Food - низкие
            }[restaurant_id]
            
            # Сезонные колебания
            month = current_date.month
            seasonal_multiplier = 1.0
            if month in [6, 7, 8]:  # Высокий сезон
                seasonal_multiplier = 1.3
            elif month in [1, 2, 11, 12]:  # Средний сезон
                seasonal_multiplier = 1.1
            else:  # Низкий сезон
                seasonal_multiplier = 0.8
            
            # Дневные колебания
            day_of_week = current_date.weekday()
            day_multiplier = 1.0
            if day_of_week in [4, 5, 6]:  # Пятница, суббота, воскресенье
                day_multiplier = 1.2
            elif day_of_week in [0, 1]:  # Понедельник, вторник
                day_multiplier = 0.8
            
            # Случайные колебания
            random_multiplier = np.random.uniform(0.7, 1.4)
            
            # Итоговые продажи
            daily_sales = base_sales * seasonal_multiplier * day_multiplier * random_multiplier
            daily_orders = int(daily_sales / np.random.uniform(45000, 65000))  # Средний чек 45-65k
            
            # Данные о рекламе
            ads_enabled = np.random.choice([True, False], p=[0.7, 0.3])
            ads_spend = np.random.uniform(50000, 200000) if ads_enabled else 0
            
            # Рейтинги и отмены
            rating = np.random.uniform(3.5, 4.9)
            cancellation_rate = np.random.uniform(0.02, 0.15)
            
            data.append({
                'restaurant_id': restaurant_id,
                'date': current_date.strftime('%Y-%m-%d'),
                'total_sales': round(daily_sales),
                'total_orders': daily_orders,
                'ads_spend': round(ads_spend),
                'ads_enabled': ads_enabled,
                'avg_rating': round(rating, 1),
                'cancellation_rate': round(cancellation_rate, 3),
                'avg_preparation_time': np.random.randint(15, 35),
                'avg_order_value': round(daily_sales / max(daily_orders, 1)),
                'delivery_time': np.random.randint(20, 45)
            })
        
        current_date += timedelta(days=1)
    
    return pd.DataFrame(data)

def create_test_gojek_stats():
    """Создает тестовую статистику Gojek (обычно меньше чем Grab)"""
    restaurants = list(range(1, 9))
    
    # Генерируем данные за 2 года
    start_date = datetime(2022, 1, 1)
    end_date = datetime(2024, 1, 1)
    
    data = []
    current_date = start_date
    
    while current_date < end_date:
        for restaurant_id in restaurants:
            # Gojek обычно имеет меньшие объемы чем Grab
            base_sales = {
                1: 2100000,  # 60% от Grab
                2: 1680000,
                3: 1920000,
                4: 1320000,
                5: 1560000,
                6: 2520000,
                7: 2280000,
                8: 1080000,
            }[restaurant_id]
            
            # Те же сезонные факторы
            month = current_date.month
            seasonal_multiplier = 1.0
            if month in [6, 7, 8]:
                seasonal_multiplier = 1.3
            elif month in [1, 2, 11, 12]:
                seasonal_multiplier = 1.1
            else:
                seasonal_multiplier = 0.8
            
            day_of_week = current_date.weekday()
            day_multiplier = 1.0
            if day_of_week in [4, 5, 6]:
                day_multiplier = 1.2
            elif day_of_week in [0, 1]:
                day_multiplier = 0.8
            
            random_multiplier = np.random.uniform(0.7, 1.4)
            
            daily_sales = base_sales * seasonal_multiplier * day_multiplier * random_multiplier
            daily_orders = int(daily_sales / np.random.uniform(40000, 60000))
            
            ads_enabled = np.random.choice([True, False], p=[0.6, 0.4])  # Меньше рекламы в Gojek
            ads_spend = np.random.uniform(30000, 150000) if ads_enabled else 0
            
            rating = np.random.uniform(3.4, 4.8)
            cancellation_rate = np.random.uniform(0.03, 0.18)  # Чуть выше отмены
            
            data.append({
                'restaurant_id': restaurant_id,
                'date': current_date.strftime('%Y-%m-%d'),
                'total_sales': round(daily_sales),
                'total_orders': daily_orders,
                'ads_spend': round(ads_spend),
                'ads_enabled': ads_enabled,
                'avg_rating': round(rating, 1),
                'cancellation_rate': round(cancellation_rate, 3),
                'avg_preparation_time': np.random.randint(18, 40),
                'avg_order_value': round(daily_sales / max(daily_orders, 1)),
                'delivery_time': np.random.randint(25, 50)
            })
        
        current_date += timedelta(days=1)
    
    return pd.DataFrame(data)

def main():
    print("🏗️ СОЗДАНИЕ ТЕСТОВЫХ CSV ФАЙЛОВ")
    print("=" * 50)
    
    # Создаем тестовые данные
    print("📊 Создание данных ресторанов...")
    restaurants_df = create_test_restaurants()
    restaurants_df.to_csv('restaurants.csv', index=False)
    print(f"✅ restaurants.csv создан: {len(restaurants_df)} записей")
    
    print("\n📊 Создание статистики Grab (это займет немного времени)...")
    grab_df = create_test_grab_stats()
    grab_df.to_csv('grab_stats.csv', index=False)
    print(f"✅ grab_stats.csv создан: {len(grab_df)} записей")
    
    print("\n📊 Создание статистики Gojek...")
    gojek_df = create_test_gojek_stats()
    gojek_df.to_csv('gojek_stats.csv', index=False)
    print(f"✅ gojek_stats.csv создан: {len(gojek_df)} записей")
    
    print("\n📈 СТАТИСТИКА СОЗДАННЫХ ДАННЫХ:")
    print(f"🏪 Ресторанов: {len(restaurants_df)}")
    print(f"📅 Период: 2022-01-01 - 2024-01-01 (2 года)")
    print(f"📊 Grab записей: {len(grab_df):,}")
    print(f"📊 Gojek записей: {len(gojek_df):,}")
    print(f"💰 Общие продажи (Grab): {grab_df['total_sales'].sum():,.0f} IDR")
    print(f"💰 Общие продажи (Gojek): {gojek_df['total_sales'].sum():,.0f} IDR")
    
    print("\n🎯 СЛЕДУЮЩИЕ ШАГИ:")
    print("1. Запустите: python3 analyze_client_csv.py")
    print("2. После анализа: python3 main.py train")
    print("3. Тестируйте: python3 main.py analyze --restaurant_id 1 --date 2023-06-15")
    
    print("\n📝 ПРИМЕЧАНИЕ:")
    print("Это тестовые данные для демонстрации системы.")
    print("Замените файлы на реальные CSV данные клиента для работы с настоящими данными.")

if __name__ == "__main__":
    main()