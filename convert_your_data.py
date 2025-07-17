#!/usr/bin/env python3
"""
Скрипт для конвертации ваших данных в формат системы
"""

import pandas as pd
import sqlite3
from datetime import datetime
import os

def convert_csv_to_database(csv_file_path, db_path='./data/sales_data.db'):
    """
    Конвертирует CSV файл в SQLite базу данных
    """
    # Читаем CSV
    try:
        df = pd.read_csv(csv_file_path)
        print(f"✅ Загружен CSV файл: {len(df)} строк")
    except Exception as e:
        print(f"❌ Ошибка чтения CSV: {e}")
        return False
    
    # Проверяем обязательные колонки
    required_columns = ['date', 'restaurant_name', 'region', 'sales', 'orders']
    missing_columns = [col for col in required_columns if col not in df.columns]
    
    if missing_columns:
        print(f"❌ Отсутствуют обязательные колонки: {missing_columns}")
        print(f"Доступные колонки: {list(df.columns)}")
        return False
    
    # Создаем директорию для базы данных
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    
    # Подключаемся к базе данных
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Создаем таблицы
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS restaurants (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE,
            region TEXT,
            cuisine_type TEXT,
            rating REAL,
            avg_delivery_time INTEGER,
            commission_rate REAL
        )
    ''')
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS grab_stats (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            restaurant_id INTEGER,
            date DATE,
            sales REAL,
            orders INTEGER,
            avg_order_value REAL,
            ads_spend REAL,
            ads_enabled BOOLEAN,
            rating REAL,
            delivery_time INTEGER,
            FOREIGN KEY (restaurant_id) REFERENCES restaurants(id)
        )
    ''')
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS gojek_stats (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            restaurant_id INTEGER,
            date DATE,
            sales REAL,
            orders INTEGER,
            avg_order_value REAL,
            ads_spend REAL,
            ads_enabled BOOLEAN,
            rating REAL,
            delivery_time INTEGER,
            FOREIGN KEY (restaurant_id) REFERENCES restaurants(id)
        )
    ''')
    
    # Заполняем таблицу ресторанов
    restaurants = df[['restaurant_name', 'region']].drop_duplicates()
    
    for _, row in restaurants.iterrows():
        cursor.execute('''
            INSERT OR IGNORE INTO restaurants (name, region, rating, avg_delivery_time, commission_rate)
            VALUES (?, ?, ?, ?, ?)
        ''', (row['restaurant_name'], row['region'], 4.0, 30, 0.25))
    
    # Получаем ID ресторанов
    restaurant_ids = {}
    cursor.execute('SELECT id, name FROM restaurants')
    for rest_id, name in cursor.fetchall():
        restaurant_ids[name] = rest_id
    
    # Заполняем статистику (используем grab_stats как основную таблицу)
    for _, row in df.iterrows():
        restaurant_id = restaurant_ids[row['restaurant_name']]
        
        # Вычисляем недостающие поля
        avg_order_value = row.get('avg_order_value', row['sales'] / row['orders'] if row['orders'] > 0 else 0)
        ads_enabled = row.get('ads_enabled', True)
        ads_spend = row.get('ads_spend', 0)
        rating = row.get('rating', 4.0)
        delivery_time = row.get('delivery_time', 30)
        
        cursor.execute('''
            INSERT INTO grab_stats 
            (restaurant_id, date, sales, orders, avg_order_value, ads_spend, ads_enabled, rating, delivery_time)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (restaurant_id, row['date'], row['sales'], row['orders'], 
              avg_order_value, ads_spend, ads_enabled, rating, delivery_time))
    
    conn.commit()
    conn.close()
    
    print(f"✅ База данных создана: {db_path}")
    print(f"✅ Загружено ресторанов: {len(restaurants)}")
    print(f"✅ Загружено записей продаж: {len(df)}")
    
    return True

def validate_data_format(csv_file_path):
    """
    Проверяет формат данных
    """
    try:
        df = pd.read_csv(csv_file_path)
        print(f"📊 Анализ файла: {csv_file_path}")
        print(f"📊 Количество строк: {len(df)}")
        print(f"📊 Колонки: {list(df.columns)}")
        
        # Проверяем обязательные колонки
        required = ['date', 'restaurant_name', 'region', 'sales', 'orders']
        optional = ['ads_enabled', 'rating', 'avg_order_value', 'delivery_time', 'ads_spend']
        
        print("\n✅ Обязательные колонки:")
        for col in required:
            status = "✅" if col in df.columns else "❌"
            print(f"  {status} {col}")
        
        print("\n🔧 Опциональные колонки:")
        for col in optional:
            status = "✅" if col in df.columns else "➖"
            print(f"  {status} {col}")
        
        # Проверяем регионы
        if 'region' in df.columns:
            regions = df['region'].unique()
            print(f"\n🗺️  Регионы: {list(regions)}")
            
            valid_regions = ['Seminyak', 'Ubud', 'Canggu', 'Denpasar', 'Sanur']
            invalid_regions = [r for r in regions if r not in valid_regions]
            if invalid_regions:
                print(f"⚠️  Неизвестные регионы: {invalid_regions}")
                print(f"✅ Допустимые регионы: {valid_regions}")
        
        # Проверяем даты
        if 'date' in df.columns:
            date_range = f"{df['date'].min()} - {df['date'].max()}"
            print(f"📅 Период данных: {date_range}")
        
        # Проверяем продажи
        if 'sales' in df.columns:
            avg_sales = df['sales'].mean()
            print(f"💰 Средние продажи: {avg_sales:,.0f} IDR")
        
        return True
        
    except Exception as e:
        print(f"❌ Ошибка анализа файла: {e}")
        return False

def main():
    print("🚀 Конвертер данных для системы анализа продаж")
    print("=" * 50)
    
    # Проверяем наличие файла шаблона
    if os.path.exists('data_template.csv'):
        print("📋 Пример формата данных в файле: data_template.csv")
        validate_data_format('data_template.csv')
        print("\n" + "=" * 50)
    
    # Запрашиваем путь к файлу пользователя
    csv_file = input("📁 Укажите путь к вашему CSV файлу: ").strip()
    
    if not os.path.exists(csv_file):
        print(f"❌ Файл не найден: {csv_file}")
        return
    
    # Анализируем данные
    print("\n🔍 Анализ ваших данных:")
    if not validate_data_format(csv_file):
        return
    
    # Конвертируем
    print("\n🔄 Конвертация в базу данных...")
    if convert_csv_to_database(csv_file):
        print("\n🎉 Готово! Теперь можете запустить:")
        print("   python main.py train")
        print("   python main.py analyze --restaurant_id 1 --date 2024-01-15")
    else:
        print("\n❌ Ошибка конвертации")

if __name__ == "__main__":
    main()