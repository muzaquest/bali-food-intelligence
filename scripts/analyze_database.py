#!/usr/bin/env python3
"""
📊 Анализатор базы данных ресторанов
Предоставляет детальную статистику по базе данных
"""

import sqlite3
import pandas as pd
import json
from datetime import datetime, timedelta

def analyze_database_structure():
    """Анализирует структуру базы данных"""
    try:
        conn = sqlite3.connect('../database.sqlite')
        cursor = conn.cursor()
        
        print("🏗️ СТРУКТУРА БАЗЫ ДАННЫХ")
        print("=" * 50)
        
        # Получаем список таблиц
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = cursor.fetchall()
        
        for table_name in tables:
            table = table_name[0]
            print(f"\n📋 Таблица: {table}")
            
            # Получаем структуру таблицы
            cursor.execute(f"PRAGMA table_info({table})")
            columns = cursor.fetchall()
            
            for col in columns:
                print(f"  - {col[1]} ({col[2]})")
            
            # Считаем записи
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
            print(f"  📊 Записей: {count}")
        
        conn.close()
        return True
        
    except Exception as e:
        print(f"❌ Ошибка анализа структуры: {e}")
        return False

def analyze_restaurant_data():
    """Анализирует данные ресторанов"""
    try:
        conn = sqlite3.connect('../database.sqlite')
        
        # Основная статистика
        df = pd.read_sql_query("SELECT * FROM restaurants", conn)
        
        print("\n🍽️ СТАТИСТИКА РЕСТОРАНОВ")
        print("=" * 50)
        print(f"Всего ресторанов: {len(df)}")
        
        # Анализ по городам
        if 'city' in df.columns:
            city_stats = df['city'].value_counts()
            print(f"\n🏙️ Распределение по городам:")
            for city, count in city_stats.head(10).items():
                print(f"  - {city}: {count}")
        
        # Анализ по платформам
        if 'platform' in df.columns:
            platform_stats = df['platform'].value_counts()
            print(f"\n📱 Распределение по платформам:")
            for platform, count in platform_stats.items():
                print(f"  - {platform}: {count}")
        
        # Анализ рейтингов
        if 'rating' in df.columns:
            print(f"\n⭐ Статистика рейтингов:")
            print(f"  - Средний рейтинг: {df['rating'].mean():.2f}")
            print(f"  - Медианный рейтинг: {df['rating'].median():.2f}")
            print(f"  - Мин/Макс: {df['rating'].min():.1f} / {df['rating'].max():.1f}")
        
        conn.close()
        return True
        
    except Exception as e:
        print(f"❌ Ошибка анализа ресторанов: {e}")
        return False

def analyze_sales_data():
    """Анализирует данные продаж"""
    try:
        conn = sqlite3.connect('../database.sqlite')
        
        # Проверяем наличие таблицы продаж
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE '%sales%'")
        sales_tables = cursor.fetchall()
        
        if not sales_tables:
            print("\n💰 ДАННЫЕ ПРОДАЖ: Не найдены")
            conn.close()
            return True
        
        print("\n💰 СТАТИСТИКА ПРОДАЖ")
        print("=" * 50)
        
        for table_name in sales_tables:
            table = table_name[0]
            df = pd.read_sql_query(f"SELECT * FROM {table}", conn)
            
            print(f"\n📊 Таблица: {table}")
            print(f"  - Записей: {len(df)}")
            
            if 'sales' in df.columns:
                print(f"  - Общие продажи: ${df['sales'].sum():,.2f}")
                print(f"  - Средние продажи: ${df['sales'].mean():,.2f}")
            
            if 'orders' in df.columns:
                print(f"  - Общее количество заказов: {df['orders'].sum():,}")
                print(f"  - Среднее количество заказов: {df['orders'].mean():.1f}")
        
        conn.close()
        return True
        
    except Exception as e:
        print(f"❌ Ошибка анализа продаж: {e}")
        return False

def generate_data_quality_report():
    """Генерирует отчет о качестве данных"""
    try:
        conn = sqlite3.connect('../database.sqlite')
        
        print("\n🔍 ОТЧЕТ О КАЧЕСТВЕ ДАННЫХ")
        print("=" * 50)
        
        # Проверяем основную таблицу
        df = pd.read_sql_query("SELECT * FROM restaurants", conn)
        
        print(f"📊 Общая статистика:")
        print(f"  - Всего записей: {len(df)}")
        print(f"  - Столбцов: {len(df.columns)}")
        
        # Проверяем пропущенные значения
        missing_data = df.isnull().sum()
        if missing_data.sum() > 0:
            print(f"\n⚠️ Пропущенные данные:")
            for col, missing in missing_data.items():
                if missing > 0:
                    percentage = (missing / len(df)) * 100
                    print(f"  - {col}: {missing} ({percentage:.1f}%)")
        else:
            print(f"\n✅ Пропущенных данных не найдено")
        
        # Проверяем дубликаты
        if 'name' in df.columns:
            duplicates = df['name'].duplicated().sum()
            print(f"\n🔄 Дубликаты по названию: {duplicates}")
        
        conn.close()
        return True
        
    except Exception as e:
        print(f"❌ Ошибка анализа качества: {e}")
        return False

def export_summary():
    """Экспортирует сводку в JSON"""
    try:
        conn = sqlite3.connect('../database.sqlite')
        
        # Собираем статистику
        summary = {
            "timestamp": datetime.now().isoformat(),
            "database_info": {},
            "data_quality": {},
            "statistics": {}
        }
        
        # Основная информация
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = cursor.fetchall()
        
        summary["database_info"]["tables"] = len(tables)
        summary["database_info"]["table_names"] = [t[0] for t in tables]
        
        # Статистика по ресторанам
        df = pd.read_sql_query("SELECT * FROM restaurants", conn)
        summary["statistics"]["total_restaurants"] = len(df)
        
        if 'city' in df.columns:
            summary["statistics"]["cities"] = df['city'].nunique()
        
        if 'platform' in df.columns:
            summary["statistics"]["platforms"] = df['platform'].value_counts().to_dict()
        
        # Сохраняем в файл
        with open('../data/database_analysis.json', 'w', encoding='utf-8') as f:
            json.dump(summary, f, ensure_ascii=False, indent=2)
        
        print(f"\n💾 Сводка экспортирована в data/database_analysis.json")
        
        conn.close()
        return True
        
    except Exception as e:
        print(f"❌ Ошибка экспорта: {e}")
        return False

def main():
    """Основная функция анализа"""
    print("🔍 АНАЛИЗАТОР БАЗЫ ДАННЫХ РЕСТОРАНОВ")
    print("=" * 60)
    
    # Проверяем наличие базы данных
    import os
    if not os.path.exists('../database.sqlite'):
        print("❌ База данных не найдена!")
        print("💡 Запустите: python setup_database.py")
        return
    
    # Выполняем анализы
    analyze_database_structure()
    analyze_restaurant_data()
    analyze_sales_data()
    generate_data_quality_report()
    export_summary()
    
    print(f"\n🎉 Анализ завершен!")

if __name__ == "__main__":
    main()