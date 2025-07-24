#!/usr/bin/env python3
"""
🍽️ СТАРАЯ ВЕРСИЯ ПРОГРАММЫ ДЛЯ REPLIT
Упрощенная версия системы аналитики ресторанов для Replit
"""

import sqlite3
import pandas as pd
import json
from datetime import datetime

def connect_database():
    """Подключение к базе данных"""
    try:
        conn = sqlite3.connect('database.sqlite')
        print("✅ Подключение к базе данных успешно")
        return conn
    except Exception as e:
        print(f"❌ Ошибка подключения к БД: {e}")
        return None

def get_restaurants(conn):
    """Получение списка ресторанов"""
    try:
        query = "SELECT DISTINCT name, city FROM restaurants LIMIT 10"
        df = pd.read_sql_query(query, conn)
        return df
    except Exception as e:
        print(f"❌ Ошибка получения ресторанов: {e}")
        return None

def analyze_restaurant(conn, restaurant_name):
    """Простой анализ ресторана"""
    try:
        query = f"""
        SELECT * FROM restaurants 
        WHERE name = '{restaurant_name}'
        LIMIT 1
        """
        df = pd.read_sql_query(query, conn)
        
        if df.empty:
            return "Ресторан не найден"
        
        restaurant = df.iloc[0]
        
        report = f"""
🍽️ АНАЛИЗ РЕСТОРАНА: {restaurant_name}
{'=' * 50}

📍 Локация: {restaurant.get('city', 'Не указано')}
⭐ Рейтинг: {restaurant.get('rating', 'Не указано')}
📱 Платформа: {restaurant.get('platform', 'Не указано')}

📊 БАЗОВАЯ СТАТИСТИКА:
- Продажи: ${restaurant.get('sales', 0):,.2f}
- Заказы: {restaurant.get('orders', 0):,}
- Средний чек: ${restaurant.get('avg_order_value', 0):.2f}

🎯 РЕКОМЕНДАЦИИ:
- Продолжить мониторинг показателей
- Сравнить с конкурентами в регионе
- Оптимизировать популярные позиции меню
        """
        
        return report
        
    except Exception as e:
        return f"❌ Ошибка анализа: {e}"

def main():
    """Основная функция"""
    print("🚀 СИСТЕМА АНАЛИТИКИ РЕСТОРАНОВ (REPLIT)")
    print("=" * 60)
    
    # Подключаемся к базе данных
    conn = connect_database()
    if not conn:
        return
    
    # Получаем список ресторанов
    restaurants = get_restaurants(conn)
    if restaurants is None:
        return
    
    print("\n📋 ДОСТУПНЫЕ РЕСТОРАНЫ:")
    for idx, row in restaurants.iterrows():
        print(f"{idx + 1}. {row['name']} ({row['city']})")
    
    # Выбор ресторана
    try:
        choice = input("\n🔢 Выберите номер ресторана: ")
        idx = int(choice) - 1
        
        if 0 <= idx < len(restaurants):
            restaurant_name = restaurants.iloc[idx]['name']
            
            # Анализ
            print("\n🔍 Выполняю анализ...")
            report = analyze_restaurant(conn, restaurant_name)
            print(report)
            
        else:
            print("❌ Неверный номер ресторана")
            
    except ValueError:
        print("❌ Введите корректный номер")
    except KeyboardInterrupt:
        print("\n👋 До свидания!")
    finally:
        conn.close()

if __name__ == "__main__":
    main()