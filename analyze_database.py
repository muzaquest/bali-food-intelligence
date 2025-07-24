#!/usr/bin/env python3
import sqlite3
import json
import statistics
from datetime import datetime, timedelta
from collections import defaultdict
import math

def analyze_database():
    """Анализ базы данных для расчета реальных коэффициентов"""
    
    print("🔍 АНАЛИЗ БАЗЫ ДАННЫХ ЗА 2.5 ГОДА")
    print("=" * 50)
    
    # Подключение к базе данных
    conn = sqlite3.connect('database.sqlite')
    cursor = conn.cursor()
    
    # 1. Анализ структуры базы данных
    print("\n📊 СТРУКТУРА БАЗЫ ДАННЫХ:")
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = cursor.fetchall()
    
    for table in tables:
        table_name = table[0]
        print(f"\n🗂️  Таблица: {table_name}")
        
        # Получаем схему таблицы
        cursor.execute(f"PRAGMA table_info({table_name})")
        columns = cursor.fetchall()
        
        for col in columns:
            print(f"   - {col[1]} ({col[2]})")
        
        # Получаем количество записей
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        count = cursor.fetchone()[0]
        print(f"   📈 Записей: {count:,}")
    
    # 2. Анализ основной таблицы grab_stats
    print("\n" + "=" * 50)
    print("📈 АНАЛИЗ ДАННЫХ GRAB_STATS")
    print("=" * 50)
    
    # Получаем диапазон дат
    cursor.execute("""
        SELECT MIN(stat_date) as min_date, MAX(stat_date) as max_date, COUNT(*) as total_records
        FROM grab_stats
    """)
    date_info = cursor.fetchone()
    print(f"📅 Период данных: {date_info[0]} - {date_info[1]}")
    print(f"📊 Общее количество записей: {date_info[2]:,}")
    
    # Получаем список ресторанов
    cursor.execute("""
        SELECT DISTINCT r.name, COUNT(*) as records_count
        FROM grab_stats gs 
        JOIN restaurants r ON gs.restaurant_id = r.id 
        GROUP BY gs.restaurant_id, r.name
        ORDER BY r.name
    """)
    restaurants = cursor.fetchall()
    print(f"🏪 Количество ресторанов: {len(restaurants)}")
    print("   Рестораны:")
    for name, count in restaurants[:10]:
        print(f"     - {name}: {count:,} записей")
    if len(restaurants) > 10:
        print(f"     ... и еще {len(restaurants) - 10} ресторанов")
    
    # 3. Анализ доступных полей для расчета коэффициентов
    print("\n🔬 АНАЛИЗ ДОСТУПНЫХ ДАННЫХ ДЛЯ КОЭФФИЦИЕНТОВ:")
    
    # Проверяем наличие ключевых полей
    cursor.execute("PRAGMA table_info(grab_stats)")
    columns = [col[1] for col in cursor.fetchall()]
    
    key_fields = {
        'ads_spend': 'Расходы на рекламу',
        'sales': 'Общие продажи',
        'rating': 'Рейтинг ресторана',
        'ads_ctr': 'CTR рекламы',
        'unique_conversion_reach': 'Конверсия',
        'ads_orders': 'Заказы от рекламы',
        'store_is_closed': 'Статус закрытия',
        'stat_date': 'Дата',
        'orders': 'Общие заказы',
        'ads_sales': 'Продажи от рекламы',
        'impressions': 'Показы рекламы'
    }
    
    available_fields = []
    for field, description in key_fields.items():
        if field in columns:
            available_fields.append(field)
            print(f"✅ {field} - {description}")
        else:
            print(f"❌ {field} - {description} (НЕТ ДАННЫХ)")
    
    # 4. Расчет реальных коэффициентов
    print("\n" + "=" * 50)
    print("🧮 РАСЧЕТ РЕАЛЬНЫХ КОЭФФИЦИЕНТОВ")
    print("=" * 50)
    
    coefficients = {}
    
    # А) МАРКЕТИНГОВЫЙ КОЭФФИЦИЕНТ
    if 'ads_spend' in available_fields and 'sales' in available_fields:
        print("\n📈 АНАЛИЗ ВЛИЯНИЯ РЕКЛАМЫ:")
        
        cursor.execute("""
            SELECT 
                restaurant_id,
                stat_date,
                ads_spend,
                sales,
                LAG(ads_spend) OVER (PARTITION BY restaurant_id ORDER BY stat_date) as prev_ads_spend,
                LAG(sales) OVER (PARTITION BY restaurant_id ORDER BY stat_date) as prev_sales
            FROM grab_stats 
            WHERE ads_spend IS NOT NULL AND sales IS NOT NULL
            ORDER BY restaurant_id, stat_date
        """)
        
        marketing_data = cursor.fetchall()
        marketing_changes = []
        sales_changes = []
        
        for row in marketing_data:
            if row[4] is not None and row[5] is not None and row[4] > 0 and row[5] > 0:
                marketing_change = (row[2] - row[4]) / row[4]  # Относительное изменение маркетинга
                sales_change = (row[3] - row[5]) / row[5]      # Относительное изменение продаж
                
                # Фильтруем экстремальные значения
                if abs(marketing_change) < 2 and abs(sales_change) < 2:
                    marketing_changes.append(marketing_change)
                    sales_changes.append(sales_change)
        
        if len(marketing_changes) > 50:  # Достаточно данных для анализа
            # Простая корреляция
            correlation = calculate_correlation(marketing_changes, sales_changes)
            coefficients['marketing'] = correlation
            
            print(f"   📊 Проанализировано изменений: {len(marketing_changes):,}")
            print(f"   🎯 Реальный коэффициент маркетинга: {correlation:.3f}")
            print(f"   📋 Было эмпирически: 0.500")
            print(f"   🔄 Изменение: {((correlation - 0.5) / 0.5 * 100):+.1f}%")
        else:
            print("   ❌ Недостаточно данных для анализа маркетинга")
    
    # Б) КОЭФФИЦИЕНТ РЕЙТИНГА
    if 'rating' in available_fields and 'sales' in available_fields:
        print("\n⭐ АНАЛИЗ ВЛИЯНИЯ РЕЙТИНГА:")
        
        cursor.execute("""
            SELECT 
                restaurant_id,
                stat_date,
                rating,
                sales,
                LAG(rating) OVER (PARTITION BY restaurant_id ORDER BY stat_date) as prev_rating,
                LAG(sales) OVER (PARTITION BY restaurant_id ORDER BY stat_date) as prev_sales
            FROM grab_stats 
            WHERE rating IS NOT NULL AND sales IS NOT NULL
            ORDER BY restaurant_id, stat_date
        """)
        
        rating_data = cursor.fetchall()
        rating_changes = []
        sales_changes = []
        
        for row in rating_data:
            if row[4] is not None and row[5] is not None and row[5] > 0:
                rating_change = row[2] - row[4]               # Абсолютное изменение рейтинга
                sales_change = (row[3] - row[5]) / row[5]     # Относительное изменение продаж
                
                # Учитываем только значимые изменения рейтинга
                if abs(rating_change) >= 0.01 and abs(sales_change) < 2:
                    rating_changes.append(rating_change)
                    sales_changes.append(sales_change)
        
        if len(rating_changes) > 30:
            # Рассчитываем влияние изменения рейтинга на 0.1 пункта
            correlation = calculate_correlation(rating_changes, sales_changes)
            rating_coefficient = correlation * 10  # Переводим в влияние на 0.1 пункта
            coefficients['rating'] = rating_coefficient
            
            print(f"   📊 Проанализировано изменений: {len(rating_changes):,}")
            print(f"   🎯 Реальный коэффициент рейтинга: {rating_coefficient:.3f} (за 0.1★)")
            print(f"   📋 Было эмпирически: 0.080")
            print(f"   🔄 Изменение: {((rating_coefficient - 0.08) / 0.08 * 100):+.1f}%")
        else:
            print("   ❌ Недостаточно данных для анализа рейтинга")
    
    # В) АНАЛИЗ ДНЕЙ НЕДЕЛИ
    print("\n📅 АНАЛИЗ ВЛИЯНИЯ ДНЕЙ НЕДЕЛИ:")
    
    cursor.execute("""
        SELECT 
            CASE CAST(strftime('%w', stat_date) AS INTEGER)
                WHEN 0 THEN 'Воскресенье'
                WHEN 1 THEN 'Понедельник'
                WHEN 2 THEN 'Вторник'
                WHEN 3 THEN 'Среда'
                WHEN 4 THEN 'Четверг'
                WHEN 5 THEN 'Пятница'
                WHEN 6 THEN 'Суббота'
            END as day_name,
            AVG(sales) as avg_sales,
            COUNT(*) as count_days
        FROM grab_stats 
        WHERE sales IS NOT NULL
        GROUP BY strftime('%w', stat_date)
        ORDER BY strftime('%w', stat_date)
    """)
    
    weekday_data = cursor.fetchall()
    if weekday_data:
        # Рассчитываем средние продажи по дням недели
        total_avg = sum(row[1] for row in weekday_data) / len(weekday_data)
        
        coefficients['weekdays'] = {}
        for day_name, avg_sales, count in weekday_data:
            impact = (avg_sales - total_avg) / total_avg
            coefficients['weekdays'][day_name] = impact
            print(f"   {day_name}: {impact:+.1%} (среднее: {avg_sales:.0f}, дней: {count})")
    
    # Г) АНАЛИЗ ЗАКРЫТИЙ РЕСТОРАНОВ
    if 'store_is_closed' in available_fields:
        print("\n🚫 АНАЛИЗ ВЛИЯНИЯ ЗАКРЫТИЙ:")
        
        cursor.execute("""
            SELECT 
                AVG(CASE WHEN store_is_closed = 0 THEN sales END) as open_sales,
                AVG(CASE WHEN store_is_closed = 1 THEN sales END) as closed_sales,
                COUNT(CASE WHEN store_is_closed = 0 THEN 1 END) as open_days,
                COUNT(CASE WHEN store_is_closed = 1 THEN 1 END) as closed_days
            FROM grab_stats 
            WHERE sales IS NOT NULL
        """)
        
        closure_data = cursor.fetchone()
        if closure_data[0] and closure_data[1]:
            closure_impact = (closure_data[1] - closure_data[0]) / closure_data[0]
            coefficients['closure'] = closure_impact
            
            print(f"   📊 Открытые дни: {closure_data[2]:,} (среднее: {closure_data[0]:.0f})")
            print(f"   📊 Закрытые дни: {closure_data[3]:,} (среднее: {closure_data[1]:.0f})")
            print(f"   🎯 Реальное влияние закрытия: {closure_impact:.1%}")
            print(f"   📋 Было эмпирически: -80%")
    
    # 5. Сохранение результатов
    print("\n" + "=" * 50)
    print("💾 СОХРАНЕНИЕ РЕАЛЬНЫХ КОЭФФИЦИЕНТОВ")
    print("=" * 50)
    
    # Сохраняем коэффициенты в JSON файл
    with open('real_coefficients.json', 'w', encoding='utf-8') as f:
        json.dump(coefficients, f, indent=2, ensure_ascii=False)
    
    print("✅ Коэффициенты сохранены в real_coefficients.json")
    
    # Выводим сводку
    print("\n📋 СВОДКА РЕАЛЬНЫХ КОЭФФИЦИЕНТОВ:")
    for key, value in coefficients.items():
        if isinstance(value, dict):
            print(f"   {key}:")
            for subkey, subvalue in value.items():
                print(f"     {subkey}: {subvalue:.3f}")
        else:
            print(f"   {key}: {value:.3f}")
    
    conn.close()
    return coefficients

def calculate_correlation(x_values, y_values):
    """Простая функция расчета корреляции"""
    if len(x_values) != len(y_values) or len(x_values) < 2:
        return 0
    
    n = len(x_values)
    sum_x = sum(x_values)
    sum_y = sum(y_values)
    sum_xy = sum(x * y for x, y in zip(x_values, y_values))
    sum_x2 = sum(x * x for x in x_values)
    sum_y2 = sum(y * y for y in y_values)
    
    denominator = math.sqrt((n * sum_x2 - sum_x * sum_x) * (n * sum_y2 - sum_y * sum_y))
    
    if denominator == 0:
        return 0
    
    correlation = (n * sum_xy - sum_x * sum_y) / denominator
    return correlation

if __name__ == "__main__":
    try:
        coefficients = analyze_database()
        print("\n🎉 АНАЛИЗ ЗАВЕРШЕН УСПЕШНО!")
        
    except Exception as e:
        print(f"\n❌ ОШИБКА: {e}")
        import traceback
        traceback.print_exc()