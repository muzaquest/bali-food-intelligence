#!/usr/bin/env python3
"""
Проверка совместимости базы данных клиента
"""

import sqlite3
import sys
import pandas as pd
from datetime import datetime, timedelta

def check_database_compatibility(db_path: str):
    """Проверяет совместимость базы данных клиента"""
    
    print(f"🔍 ПРОВЕРКА БАЗЫ ДАННЫХ: {db_path}")
    print("=" * 60)
    
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Проверка существования таблиц
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [row[0] for row in cursor.fetchall()]
        
        print("📋 НАЙДЕННЫЕ ТАБЛИЦЫ:")
        for table in tables:
            print(f"  ✅ {table}")
        
        required_tables = ['restaurants', 'grab_stats', 'gojek_stats']
        missing_tables = [t for t in required_tables if t not in tables]
        
        if missing_tables:
            print(f"\n❌ ОТСУТСТВУЮТ ОБЯЗАТЕЛЬНЫЕ ТАБЛИЦЫ: {missing_tables}")
            return False
        
        print("\n✅ Все обязательные таблицы найдены!")
        
        # Проверка структуры таблиц
        print("\n📊 ПРОВЕРКА СТРУКТУРЫ ТАБЛИЦ:")
        
        # Проверка restaurants
        cursor.execute("PRAGMA table_info(restaurants)")
        restaurant_columns = [row[1] for row in cursor.fetchall()]
        required_restaurant_cols = ['id', 'name']
        
        print(f"\n  📍 restaurants ({len(restaurant_columns)} колонок):")
        for col in restaurant_columns[:5]:  # Показываем первые 5
            print(f"    • {col}")
        if len(restaurant_columns) > 5:
            print(f"    ... и еще {len(restaurant_columns) - 5}")
        
        missing_rest_cols = [c for c in required_restaurant_cols if c not in restaurant_columns]
        if missing_rest_cols:
            print(f"    ❌ Отсутствуют: {missing_rest_cols}")
            return False
        
        # Проверка grab_stats
        cursor.execute("PRAGMA table_info(grab_stats)")
        grab_columns = [row[1] for row in cursor.fetchall()]
        required_grab_cols = ['stat_date', 'sales', 'orders', 'restaurant_id']
        
        print(f"\n  📊 grab_stats ({len(grab_columns)} колонок):")
        for col in grab_columns[:5]:
            print(f"    • {col}")
        if len(grab_columns) > 5:
            print(f"    ... и еще {len(grab_columns) - 5}")
        
        missing_grab_cols = [c for c in required_grab_cols if c not in grab_columns]
        if missing_grab_cols:
            print(f"    ❌ Отсутствуют: {missing_grab_cols}")
            return False
        
        # Проверка gojek_stats
        cursor.execute("PRAGMA table_info(gojek_stats)")
        gojek_columns = [row[1] for row in cursor.fetchall()]
        required_gojek_cols = ['stat_date', 'sales', 'orders', 'restaurant_id']
        
        print(f"\n  📊 gojek_stats ({len(gojek_columns)} колонок):")
        for col in gojek_columns[:5]:
            print(f"    • {col}")
        if len(gojek_columns) > 5:
            print(f"    ... и еще {len(gojek_columns) - 5}")
        
        missing_gojek_cols = [c for c in required_gojek_cols if c not in gojek_columns]
        if missing_gojek_cols:
            print(f"    ❌ Отсутствуют: {missing_gojek_cols}")
            return False
        
        print("\n✅ Структура таблиц совместима!")
        
        # Проверка данных
        print("\n📈 ПРОВЕРКА ДАННЫХ:")
        
        # Количество ресторанов
        cursor.execute("SELECT COUNT(*) FROM restaurants")
        restaurant_count = cursor.fetchone()[0]
        print(f"  🏪 Ресторанов: {restaurant_count}")
        
        # Диапазон дат
        cursor.execute("SELECT MIN(stat_date), MAX(stat_date) FROM grab_stats")
        grab_dates = cursor.fetchone()
        if grab_dates[0]:
            print(f"  📅 Grab данные: {grab_dates[0]} - {grab_dates[1]}")
        
        cursor.execute("SELECT MIN(stat_date), MAX(stat_date) FROM gojek_stats")
        gojek_dates = cursor.fetchone()
        if gojek_dates[0]:
            print(f"  📅 Gojek данные: {gojek_dates[0]} - {gojek_dates[1]}")
        
        # Общее количество записей
        cursor.execute("SELECT COUNT(*) FROM grab_stats")
        grab_count = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM gojek_stats")
        gojek_count = cursor.fetchone()[0]
        
        print(f"  📊 Всего записей: {grab_count + gojek_count}")
        print(f"    • Grab: {grab_count}")
        print(f"    • Gojek: {gojek_count}")
        
        # Проверка YoY возможности
        if grab_dates[0] or gojek_dates[0]:
            earliest_date = min([d for d in [grab_dates[0], gojek_dates[0]] if d])
            latest_date = max([d for d in [grab_dates[1], gojek_dates[1]] if d])
            
            earliest = datetime.strptime(earliest_date, '%Y-%m-%d')
            latest = datetime.strptime(latest_date, '%Y-%m-%d')
            
            data_span = (latest - earliest).days
            
            print(f"\n📊 АНАЛИЗ ВОЗМОЖНОСТЕЙ:")
            print(f"  📅 Период данных: {data_span} дней")
            
            if data_span >= 365:
                print("  ✅ YoY сравнения доступны")
            else:
                print("  ⚠️ YoY сравнения недоступны (нужен минимум год данных)")
            
            if restaurant_count >= 3:
                print("  ✅ Конкурентный анализ информативен")
            else:
                print("  ⚠️ Конкурентный анализ ограничен (мало ресторанов)")
        
        print(f"\n🎉 БАЗА ДАННЫХ ГОТОВА К ИСПОЛЬЗОВАНИЮ!")
        print(f"\n🚀 КОМАНДЫ ДЛЯ ЗАПУСКА:")
        print(f"export DATABASE_PATH='{db_path}'")
        print(f"python3 main.py list")
        print(f"python3 main.py market --start-date '{grab_dates[0] or gojek_dates[0]}' --end-date '{grab_dates[1] or gojek_dates[1]}'")
        
        return True
        
    except Exception as e:
        print(f"❌ ОШИБКА: {e}")
        return False
    finally:
        if 'conn' in locals():
            conn.close()

def main():
    if len(sys.argv) != 2:
        print("Использование: python3 check_database_compatibility.py /path/to/client.db")
        sys.exit(1)
    
    db_path = sys.argv[1]
    
    if check_database_compatibility(db_path):
        print("\n✅ База данных полностью совместима!")
        sys.exit(0)
    else:
        print("\n❌ База данных требует доработки!")
        sys.exit(1)

if __name__ == "__main__":
    main()