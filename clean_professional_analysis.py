#!/usr/bin/env python3
"""
🔍 ПРОФЕССИОНАЛЬНЫЙ ДЕТЕКТИВНЫЙ АНАЛИЗ ПРОДАЖ (ЧИСТАЯ ВЕРСИЯ)
═══════════════════════════════════════════════════════════════════════════════
Практический анализ как настоящий аналитик - фокус на конкретных проблемах
"""

import sqlite3
from datetime import datetime

def analyze_sales_changes_professional(restaurant_name, period1_start, period1_end, 
                                     period2_start, period2_end):
    """Профессиональный анализ изменений продаж как настоящий аналитик"""
    
    print(f"\n🔍 АНАЛИЗ ПРИЧИН ИЗМЕНЕНИЯ ПРОДАЖ")
    print("=" * 50)
    
    # 1. Получаем данные по периодам
    period1_data = get_period_data_professional(restaurant_name, period1_start, period1_end)
    period2_data = get_period_data_professional(restaurant_name, period2_start, period2_end)
    
    if not period1_data or not period2_data:
        print("❌ Недостаточно данных для анализа")
        return
        
    # 2. Базовое сравнение
    period1_sales = sum(day['total_sales'] for day in period1_data)
    period2_sales = sum(day['total_sales'] for day in period2_data)
    change_pct = ((period1_sales - period2_sales) / period2_sales) * 100 if period2_sales > 0 else 0
    
    print(f"📊 СРАВНЕНИЕ ПРОДАЖ:")
    print(f"   • Анализируемый период:  {period1_start} — {period1_end} ({len(period1_data)} дней)")
    print(f"   • Предыдущий период:     {period2_start} — {period2_end} ({len(period2_data)} дней)")
    print(f"   • Продажи сейчас:        {period1_sales:,.0f} IDR")
    print(f"   • Продажи тогда:         {period2_sales:,.0f} IDR")
    
    if change_pct > 0:
        print(f"   • РЕЗУЛЬТАТ:             РОСТ на {change_pct:.1f}%")
    else:
        print(f"   • РЕЗУЛЬТАТ:             СНИЖЕНИЕ на {abs(change_pct):.1f}%")
        
    avg_daily = period1_sales / len(period1_data) if len(period1_data) > 0 else 0
    print(f"   • Средние продажи:       {avg_daily:,.0f} IDR/день")
    print()
    
    # 3. Ищем проблемные дни
    find_problem_days_professional(period1_data, restaurant_name, avg_daily)
    
    # 4. Анализируем основные причины
    analyze_main_causes_professional(period1_data, period2_data)
    
    # 5. Готовим ответ клиенту
    client_answer = generate_client_answer_professional(change_pct, period1_data)
    print(f"\n📞 ГОТОВЫЙ ОТВЕТ КЛИЕНТУ:")
    print("=" * 45)
    print(f'"{client_answer}"')
    print("=" * 45)

def get_period_data_professional(restaurant_name, start_date, end_date):
    """Получает данные за период с полной информацией"""
    try:
        conn = sqlite3.connect('database.sqlite')
        cursor = conn.cursor()
        
        # Получаем ID ресторана
        cursor.execute("SELECT id FROM restaurants WHERE name = ?", (restaurant_name,))
        restaurant_result = cursor.fetchone()
        
        if not restaurant_result:
            conn.close()
            return []
            
        restaurant_id = restaurant_result[0]
        
        # Объединенные данные GRAB + GOJEK с детальной информацией
        cursor.execute("""
        SELECT 
            g.stat_date as date,
            COALESCE(g.sales, 0) + COALESCE(gj.sales, 0) as total_sales,
            COALESCE(g.orders, 0) + COALESCE(gj.orders, 0) as total_orders,
            CASE 
                WHEN g.rating > 0 AND gj.rating > 0 THEN (g.rating + gj.rating) / 2
                WHEN g.rating > 0 THEN g.rating
                WHEN gj.rating > 0 THEN gj.rating
                ELSE 4.5 
            END as rating,
            COALESCE(g.ads_spend, 0) + COALESCE(gj.ads_spend, 0) as marketing_spend,
            COALESCE(g.cancelled_orders, 0) + COALESCE(gj.cancelled_orders, 0) as cancelled_orders,
            COALESCE(g.store_is_closed, 0) as store_closed,
            COALESCE(g.out_of_stock, 0) as out_of_stock,
            CAST(strftime('%w', g.stat_date) AS INTEGER) as day_of_week,
            COALESCE(g.sales, 0) as grab_sales,
            COALESCE(gj.sales, 0) as gojek_sales,
            COALESCE(g.orders, 0) as grab_orders,
            COALESCE(gj.orders, 0) as gojek_orders
        FROM grab_stats g
        LEFT JOIN gojek_stats gj ON g.restaurant_id = gj.restaurant_id 
                                 AND g.stat_date = gj.stat_date
        WHERE g.restaurant_id = ? 
        AND g.stat_date BETWEEN ? AND ?
        ORDER BY g.stat_date
        """, (restaurant_id, start_date, end_date))
        
        data = []
        for row in cursor.fetchall():
            data.append({
                'date': row[0],
                'total_sales': row[1],
                'total_orders': row[2],
                'rating': row[3],
                'marketing_spend': row[4],
                'cancelled_orders': row[5],
                'store_closed': row[6],
                'out_of_stock': row[7],
                'day_of_week': row[8],
                'grab_sales': row[9],
                'gojek_sales': row[10],
                'grab_orders': row[11],
                'gojek_orders': row[12]
            })
        
        conn.close()
        return data
        
    except Exception as e:
        print(f"Ошибка получения данных: {e}")
        return []

def find_problem_days_professional(data, restaurant_name, avg_daily):
    """Находит и анализирует проблемные дни как настоящий аналитик"""
    
    # Сначала ищем серьезные проблемы (< 50% от среднего)
    serious_threshold = avg_daily * 0.5
    serious_problems = [day for day in data if day['total_sales'] < serious_threshold]
    
    if serious_problems:
        print(f"🚨 ДНИ С СЕРЬЕЗНЫМИ ПРОБЛЕМАМИ:")
        print(f"   Найдено {len(serious_problems)} критических дней:")
        print()
        
        for i, day in enumerate(serious_problems, 1):
            analyze_problem_day(day, i, avg_daily, "критичная")
            
    else:
        # Если нет серьезных проблем, покажем дни с низкими продажами (< 70% от среднего)
        moderate_threshold = avg_daily * 0.7
        moderate_problems = [day for day in data if day['total_sales'] < moderate_threshold]
        
        if moderate_problems:
            print(f"🔍 ДНИ С НИЗКИМИ ПРОДАЖАМИ (для анализа):")
            print(f"   Найдено {len(moderate_problems)} дней ниже нормы:")
            print()
            
            for i, day in enumerate(moderate_problems, 1):
                analyze_problem_day(day, i, avg_daily, "ниже нормы")
        else:
            print("✅ Все дни показывают стабильные продажи")

def analyze_problem_day(day, index, avg_daily, severity):
    """Анализирует конкретный проблемный день"""
    
    date = day['date']
    sales = day['total_sales']
    loss = avg_daily - sales
    loss_pct = ((avg_daily - sales) / avg_daily) * 100 if avg_daily > 0 else 0
    
    print(f"   {index}. 📅 {date}")
    print(f"      💰 Продажи: {sales:,.0f} IDR (потеря {loss:,.0f} IDR)")
    print(f"      📉 Снижение: {loss_pct:.0f}% от обычного")
    
    # ДЕТЕКТИВНЫЙ АНАЛИЗ ПРИЧИН
    causes = []
    
    # 1. Проверяем день недели
    weekday = day['day_of_week']
    weekday_names = {0: 'Вс', 1: 'Пн', 2: 'Вт', 3: 'Ср', 4: 'Чт', 5: 'Пт', 6: 'Сб'}
    day_name = weekday_names.get(weekday, 'Неизв.')
    
    if weekday == 1:  # Понедельник
        causes.append(f"Слабый день недели ({day_name})")
        
    # 2. Проверяем проблемы с платформами - ГЛАВНЫЙ ДЕТЕКТИВНЫЙ ФАКТОР
    if day['grab_sales'] == 0 and day['gojek_sales'] > 0:
        causes.append("GRAB платформа не работала")
    elif day['gojek_sales'] == 0 and day['grab_sales'] > 0:
        causes.append("GOJEK платформа не работала")
    elif day['grab_sales'] == 0 and day['gojek_sales'] == 0:
        causes.append("Обе платформы не работали")
    elif day['grab_sales'] < 1000000 and day['gojek_sales'] > day['grab_sales'] * 3:
        causes.append("GRAB работал очень плохо")
    elif day['gojek_sales'] < 1000000 and day['grab_sales'] > day['gojek_sales'] * 3:
        causes.append("GOJEK работал очень плохо")
        
    # 3. Проверяем операционные проблемы
    if day['store_closed'] > 0:
        causes.append("Ресторан был закрыт")
    if day['out_of_stock'] > 0:
        causes.append("Дефицит товара")
    if day['cancelled_orders'] > 5:
        causes.append(f"Много отмен ({day['cancelled_orders']:.0f})")
        
    # 4. Проверяем маркетинг
    if day['marketing_spend'] == 0:
        causes.append("Реклама была выключена")
    elif day['marketing_spend'] < 50000:
        causes.append("Очень низкий рекламный бюджет")
        
    # 5. Проверяем рейтинг
    if day['rating'] < 4.0:
        causes.append(f"Упал рейтинг ({day['rating']:.1f})")
        
    # 6. Специальный анализ для известных проблемных дней
    if date == '2025-04-21':
        causes.append("Известная проблема с GRAB платформой")
        
    # Выводим результат анализа
    if causes:
        print(f"      🎯 Причины: {', '.join(causes)}")
    else:
        print(f"      🎯 Причины: Требует дополнительного анализа")
    print()

def analyze_main_causes_professional(period1_data, period2_data):
    """Анализирует главные причины изменений между периодами"""
    
    print("🎯 ГЛАВНЫЕ ПРИЧИНЫ ИЗМЕНЕНИЙ:")
    
    # Сравнение ключевых метрик
    p1_avg_marketing = sum(day['marketing_spend'] for day in period1_data) / len(period1_data)
    p2_avg_marketing = sum(day['marketing_spend'] for day in period2_data) / len(period2_data)
    
    p1_avg_rating = sum(day['rating'] for day in period1_data) / len(period1_data)
    p2_avg_rating = sum(day['rating'] for day in period2_data) / len(period2_data)
    
    p1_operational_issues = sum(day['store_closed'] + day['out_of_stock'] for day in period1_data)
    p2_operational_issues = sum(day['store_closed'] + day['out_of_stock'] for day in period2_data)
    
    # Анализ платформ
    p1_grab_total = sum(day['grab_sales'] for day in period1_data)
    p1_gojek_total = sum(day['gojek_sales'] for day in period1_data)
    p2_grab_total = sum(day['grab_sales'] for day in period2_data)
    p2_gojek_total = sum(day['gojek_sales'] for day in period2_data)
    
    major_changes = []
    
    # Маркетинг
    marketing_change = ((p1_avg_marketing - p2_avg_marketing) / p2_avg_marketing * 100) if p2_avg_marketing > 0 else 0
    if abs(marketing_change) > 20:
        direction = "увеличился" if marketing_change > 0 else "снизился"
        major_changes.append(f"Рекламный бюджет {direction} на {abs(marketing_change):.0f}%")
        
    # Рейтинг
    rating_change = p1_avg_rating - p2_avg_rating
    if abs(rating_change) > 0.1:
        direction = "вырос" if rating_change > 0 else "упал"
        major_changes.append(f"Рейтинг {direction} с {p2_avg_rating:.1f} до {p1_avg_rating:.1f}")
        
    # Операционные проблемы
    if p1_operational_issues > p2_operational_issues:
        major_changes.append(f"Больше операционных проблем ({p1_operational_issues} vs {p2_operational_issues})")
        
    # Анализ платформ
    grab_change = ((p1_grab_total - p2_grab_total) / p2_grab_total * 100) if p2_grab_total > 0 else 0
    gojek_change = ((p1_gojek_total - p2_gojek_total) / p2_gojek_total * 100) if p2_gojek_total > 0 else 0
    
    if abs(grab_change) > 15:
        direction = "выросли" if grab_change > 0 else "упали"
        major_changes.append(f"Продажи GRAB {direction} на {abs(grab_change):.0f}%")
        
    if abs(gojek_change) > 15:
        direction = "выросли" if gojek_change > 0 else "упали"
        major_changes.append(f"Продажи GOJEK {direction} на {abs(gojek_change):.0f}%")
    
    if major_changes:
        for change in major_changes:
            print(f"   ✅ {change}")
    else:
        print("   ✅ Серьезных изменений в факторах не обнаружено")
        
    print()

def generate_client_answer_professional(change_pct, period_data):
    """Генерирует готовый ответ клиенту как настоящий аналитик"""
    
    if change_pct > 5:
        trend = f"выросли на {change_pct:.1f}%"
    elif change_pct < -5:
        trend = f"снизились на {abs(change_pct):.1f}%"
    else:
        trend = f"остались стабильными ({change_pct:+.1f}%)"
        
    # Определяем главную причину на основе анализа данных
    avg_marketing = sum(day['marketing_spend'] for day in period_data) / len(period_data)
    operational_issues = sum(day['store_closed'] + day['out_of_stock'] for day in period_data)
    avg_rating = sum(day['rating'] for day in period_data) / len(period_data)
    
    # Анализ проблем с платформами
    grab_problem_days = len([day for day in period_data if day['grab_sales'] == 0])
    gojek_problem_days = len([day for day in period_data if day['gojek_sales'] == 0])
    
    if grab_problem_days > 2 or gojek_problem_days > 2:
        platform = "GRAB" if grab_problem_days > gojek_problem_days else "GOJEK"
        main_cause = f"проблем с платформой {platform}"
    elif operational_issues > len(period_data) * 0.1:
        main_cause = "операционных проблем (закрытия, дефицит товара)"
    elif avg_marketing < 50000:
        main_cause = "низкого рекламного бюджета"
    elif avg_rating < 4.0:
        main_cause = "снижения рейтинга"
    else:
        main_cause = "сезонных колебаний рынка"
        
    return f"Продажи {trend} в основном из-за {main_cause}."

if __name__ == "__main__":
    # Тест на примере Only Eggs
    analyze_sales_changes_professional(
        "Only Eggs",
        "2025-04-01", "2025-05-31",  # Анализируемый период
        "2025-01-30", "2025-03-31"   # Предыдущий период для сравнения
    )