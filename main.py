#!/usr/bin/env python3
"""
🎯 ПОЛНЫЙ CLI ДЛЯ MUZAQUEST ANALYTICS - ИСПОЛЬЗУЕТ ВСЕ ПАРАМЕТРЫ
Полное использование всех 30+ полей из grab_stats и gojek_stats
"""

import argparse
import sys
import sqlite3
import os
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

try:
    import pandas as pd
    import numpy as np
except ImportError:
    print("❌ Требуется установка pandas и numpy: pip install pandas numpy")
    sys.exit(1)

def get_restaurant_data_full(restaurant_name, start_date, end_date, db_path="database.sqlite"):
    """Получает ВСЕ доступные данные ресторана из grab_stats и gojek_stats"""
    conn = sqlite3.connect(db_path)
    
    # Получаем ID ресторана
    restaurant_query = "SELECT id FROM restaurants WHERE name = ?"
    restaurant_result = pd.read_sql_query(restaurant_query, conn, params=(restaurant_name,))
    
    if len(restaurant_result) == 0:
        conn.close()
        print(f"❌ Ресторан '{restaurant_name}' не найден")
        return pd.DataFrame()
    
    restaurant_id = restaurant_result.iloc[0]['id']
    
    # РАСШИРЕННЫЙ запрос для Grab (ВСЕ поля)
    grab_query = """
    SELECT 
        stat_date as date,
        'grab' as platform,
        sales as total_sales,
        orders,
        rating,
        COALESCE(ads_spend, 0) as marketing_spend,
        COALESCE(ads_sales, 0) as marketing_sales,
        COALESCE(ads_orders, 0) as marketing_orders,
        CASE WHEN ads_spend > 0 THEN 1 ELSE 0 END as ads_on,
        COALESCE(cancelation_rate, 0) as cancel_rate,
        COALESCE(offline_rate, 0) as offline_rate,
        COALESCE(cancelled_orders, 0) as cancelled_orders,
        COALESCE(store_is_closed, 0) as store_is_closed,
        COALESCE(store_is_busy, 0) as store_is_busy,
        COALESCE(store_is_closing_soon, 0) as store_is_closing_soon,
        COALESCE(out_of_stock, 0) as out_of_stock,
        COALESCE(ads_ctr, 0) as ads_ctr,
        COALESCE(impressions, 0) as impressions,
        COALESCE(unique_impressions_reach, 0) as unique_impressions_reach,
        COALESCE(unique_menu_visits, 0) as unique_menu_visits,
        COALESCE(unique_add_to_carts, 0) as unique_add_to_carts,
        COALESCE(unique_conversion_reach, 0) as unique_conversion_reach,
        COALESCE(new_customers, 0) as new_customers,
        COALESCE(earned_new_customers, 0) as earned_new_customers,
        COALESCE(repeated_customers, 0) as repeated_customers,
        COALESCE(earned_repeated_customers, 0) as earned_repeated_customers,
        COALESCE(reactivated_customers, 0) as reactivated_customers,
        COALESCE(earned_reactivated_customers, 0) as earned_reactivated_customers,
        COALESCE(total_customers, 0) as total_customers,
        COALESCE(payouts, 0) as payouts,
        NULL as accepting_time,
        NULL as preparation_time,
        NULL as delivery_time,
        NULL as lost_orders,
        NULL as realized_orders_percentage,
        NULL as one_star_ratings,
        NULL as two_star_ratings,
        NULL as three_star_ratings,
        NULL as four_star_ratings,
        NULL as five_star_ratings
    FROM grab_stats 
    WHERE restaurant_id = ? AND stat_date BETWEEN ? AND ?
    ORDER BY stat_date
    """
    
    # РАСШИРЕННЫЙ запрос для Gojek (ВСЕ поля)
    gojek_query = """
    SELECT 
        stat_date as date,
        'gojek' as platform,
        sales as total_sales,
        orders,
        rating,
        COALESCE(ads_spend, 0) as marketing_spend,
        COALESCE(ads_sales, 0) as marketing_sales,
        COALESCE(ads_orders, 0) as marketing_orders,
        CASE WHEN ads_spend > 0 THEN 1 ELSE 0 END as ads_on,
        0 as cancel_rate,
        0 as offline_rate,
        COALESCE(cancelled_orders, 0) as cancelled_orders,
        COALESCE(store_is_closed, 0) as store_is_closed,
        COALESCE(store_is_busy, 0) as store_is_busy,
        0 as store_is_closing_soon,
        COALESCE(out_of_stock, 0) as out_of_stock,
        0 as ads_ctr,
        0 as impressions,
        0 as unique_impressions_reach,
        0 as unique_menu_visits,
        0 as unique_add_to_carts,
        0 as unique_conversion_reach,
        COALESCE(new_client, 0) as new_customers,
        0 as earned_new_customers,
        COALESCE(active_client, 0) as repeated_customers,
        0 as earned_repeated_customers,
        COALESCE(returned_client, 0) as reactivated_customers,
        0 as earned_reactivated_customers,
        COALESCE(new_client + active_client + returned_client, 0) as total_customers,
        COALESCE(payouts, 0) as payouts,
        accepting_time,
        preparation_time,
        delivery_time,
        COALESCE(lost_orders, 0) as lost_orders,
        COALESCE(realized_orders_percentage, 0) as realized_orders_percentage,
        COALESCE(one_star_ratings, 0) as one_star_ratings,
        COALESCE(two_star_ratings, 0) as two_star_ratings,
        COALESCE(three_star_ratings, 0) as three_star_ratings,
        COALESCE(four_star_ratings, 0) as four_star_ratings,
        COALESCE(five_star_ratings, 0) as five_star_ratings
    FROM gojek_stats 
    WHERE restaurant_id = ? AND stat_date BETWEEN ? AND ?
    ORDER BY stat_date
    """
    
    # Используем прямую подстановку
    grab_query_formatted = grab_query.replace('?', f'{restaurant_id}', 1).replace('?', f"'{start_date}'", 1).replace('?', f"'{end_date}'", 1)
    gojek_query_formatted = gojek_query.replace('?', f'{restaurant_id}', 1).replace('?', f"'{start_date}'", 1).replace('?', f"'{end_date}'", 1)
    
    grab_data = pd.read_sql_query(grab_query_formatted, conn)
    gojek_data = pd.read_sql_query(gojek_query_formatted, conn)
    
    # Объединяем данные
    all_data = pd.concat([grab_data, gojek_data], ignore_index=True)
    
    # Агрегируем по дням с учетом ВСЕХ полей
    if not all_data.empty:
        data = all_data.groupby('date').agg({
            'total_sales': 'sum',
            'orders': 'sum',
            'rating': 'mean',
            'marketing_spend': 'sum',
            'marketing_sales': 'sum',
            'marketing_orders': 'sum',
            'ads_on': 'max',
            'cancel_rate': 'mean',
            'offline_rate': 'mean',
            'cancelled_orders': 'sum',
            'store_is_closed': 'max',
            'store_is_busy': 'max',
            'store_is_closing_soon': 'max',
            'out_of_stock': 'max',
            'ads_ctr': 'mean',
            'impressions': 'sum',
            'unique_impressions_reach': 'sum',
            'unique_menu_visits': 'sum',
            'unique_add_to_carts': 'sum',
            'unique_conversion_reach': 'sum',
            'new_customers': 'sum',
            'earned_new_customers': 'sum',
            'repeated_customers': 'sum',
            'earned_repeated_customers': 'sum',
            'reactivated_customers': 'sum',
            'earned_reactivated_customers': 'sum',
            'total_customers': 'sum',
            'payouts': 'sum',
            'lost_orders': 'sum',
            'realized_orders_percentage': 'mean',
            'one_star_ratings': 'sum',
            'two_star_ratings': 'sum',
            'three_star_ratings': 'sum',
            'four_star_ratings': 'sum',
            'five_star_ratings': 'sum'
        }).reset_index()
        
        # Добавляем дополнительные вычисляемые поля
        data['is_weekend'] = pd.to_datetime(data['date']).dt.dayofweek.isin([5, 6]).astype(int)
        data['is_holiday'] = data['date'].isin([
            '2025-04-10', '2025-04-14', '2025-05-07', '2025-05-12', 
            '2025-05-29', '2025-06-01', '2025-06-16', '2025-06-17'
        ]).astype(int)
        data['weekday'] = pd.to_datetime(data['date']).dt.day_name()
        data['month'] = pd.to_datetime(data['date']).dt.month
        data['avg_order_value'] = data['total_sales'] / data['orders'].replace(0, 1)
        data['roas'] = data['marketing_sales'] / data['marketing_spend'].replace(0, 1)
        
        # Новые KPI на основе дополнительных полей
        data['conversion_rate'] = data['unique_conversion_reach'] / data['unique_impressions_reach'].replace(0, 1) * 100
        data['add_to_cart_rate'] = data['unique_add_to_carts'] / data['unique_menu_visits'].replace(0, 1) * 100
        data['customer_retention_rate'] = data['repeated_customers'] / data['total_customers'].replace(0, 1) * 100
        data['order_cancellation_rate'] = data['cancelled_orders'] / (data['orders'] + data['cancelled_orders']).replace(0, 1) * 100
        data['customer_satisfaction_score'] = (data['five_star_ratings'] * 5 + data['four_star_ratings'] * 4 + 
                                              data['three_star_ratings'] * 3 + data['two_star_ratings'] * 2 + 
                                              data['one_star_ratings'] * 1) / (data['one_star_ratings'] + 
                                              data['two_star_ratings'] + data['three_star_ratings'] + 
                                              data['four_star_ratings'] + data['five_star_ratings']).replace(0, 1)
        
        # Операционные проблемы
        data['operational_issues'] = (data['store_is_closed'] + data['store_is_busy'] + 
                                    data['store_is_closing_soon'] + data['out_of_stock'])
        
    else:
        data = pd.DataFrame()
    
    conn.close()
    return data

def analyze_restaurant(restaurant_name, start_date=None, end_date=None):
    """ПОЛНЫЙ анализ ресторана с использованием ВСЕХ доступных параметров"""
    print(f"\n🔬 ПОЛНЫЙ АНАЛИЗ ВСЕХ ПАРАМЕТРОВ: {restaurant_name.upper()}")
    print("=" * 80)
    print("🚀 Используем ВСЕ 30+ параметров из grab_stats и gojek_stats!")
    print()
    
    # Устанавливаем период по умолчанию
    if not start_date or not end_date:
        start_date = "2025-04-01"
        end_date = "2025-06-22"
    
    print(f"📅 Период анализа: {start_date} → {end_date}")
    print()
    
    # Получаем данные
    data = get_restaurant_data_full(restaurant_name, start_date, end_date)
    
    if data.empty:
        print("❌ Нет данных для анализа")
        return
    
    # 1. Базовая аналитика
    print("📊 1. БАЗОВАЯ АНАЛИТИКА")
    print("-" * 40)
    
    total_sales = data['total_sales'].sum()
    total_orders = data['orders'].sum()
    avg_rating = data['rating'].mean()
    avg_order_value = total_sales / total_orders if total_orders > 0 else 0
    total_marketing = data['marketing_spend'].sum()
    avg_roas = data['marketing_sales'].sum() / total_marketing if total_marketing > 0 else 0
    
    print(f"💰 Общие продажи: {total_sales:,.0f} IDR")
    print(f"📦 Общие заказы: {total_orders:,.0f}")
    print(f"📊 Средний чек: {avg_order_value:,.0f} IDR")
    print(f"⭐ Средний рейтинг: {avg_rating:.2f}/5.0")
    print(f"💸 Затраты на маркетинг: {total_marketing:,.0f} IDR")
    print(f"🎯 ROAS: {avg_roas:.2f}x")
    print(f"📅 Дней данных: {len(data)}")
    print()
    
    # 2. НОВЫЙ! Анализ клиентской базы
    print("👥 2. АНАЛИЗ КЛИЕНТСКОЙ БАЗЫ")
    print("-" * 40)
    
    total_customers = data['total_customers'].sum()
    new_customers = data['new_customers'].sum()
    repeated_customers = data['repeated_customers'].sum()
    reactivated_customers = data['reactivated_customers'].sum()
    
    if total_customers > 0:
        new_customer_rate = (new_customers / total_customers) * 100
        retention_rate = (repeated_customers / total_customers) * 100
        reactivation_rate = (reactivated_customers / total_customers) * 100
        
        print(f"👥 Общее количество клиентов: {total_customers:,.0f}")
        print(f"🆕 Новые клиенты: {new_customers:,.0f} ({new_customer_rate:.1f}%)")
        print(f"🔄 Повторные клиенты: {repeated_customers:,.0f} ({retention_rate:.1f}%)")
        print(f"📲 Реактивированные: {reactivated_customers:,.0f} ({reactivation_rate:.1f}%)")
        
        # Доходность по типам клиентов
        if data['earned_new_customers'].sum() > 0:
            print(f"💰 Доход от новых: {data['earned_new_customers'].sum():,.0f} IDR")
            print(f"💰 Доход от повторных: {data['earned_repeated_customers'].sum():,.0f} IDR")
            print(f"💰 Доход от реактивированных: {data['earned_reactivated_customers'].sum():,.0f} IDR")
    
    print()
    
    # 3. НОВЫЙ! Анализ маркетинговой воронки
    print("📈 3. АНАЛИЗ МАРКЕТИНГОВОЙ ВОРОНКИ")
    print("-" * 40)
    
    total_impressions = data['impressions'].sum()
    total_menu_visits = data['unique_menu_visits'].sum()
    total_add_to_carts = data['unique_add_to_carts'].sum()
    total_conversions = data['unique_conversion_reach'].sum()
    
    if total_impressions > 0:
        ctr = (total_menu_visits / total_impressions) * 100
        add_to_cart_rate = (total_add_to_carts / total_menu_visits) * 100 if total_menu_visits > 0 else 0
        conversion_rate = (total_conversions / total_menu_visits) * 100 if total_menu_visits > 0 else 0
        
        print(f"👁️ Показы рекламы: {total_impressions:,.0f}")
        print(f"🔗 Посещения меню: {total_menu_visits:,.0f} (CTR: {ctr:.2f}%)")
        print(f"🛒 Добавления в корзину: {total_add_to_carts:,.0f} (Rate: {add_to_cart_rate:.2f}%)")
        print(f"✅ Конверсии: {total_conversions:,.0f} (Rate: {conversion_rate:.2f}%)")
        print(f"📊 Средний CTR рекламы: {data['ads_ctr'].mean():.2f}%")
    
    print()
    
    # 4. НОВЫЙ! Анализ операционных проблем
    print("⚠️ 4. АНАЛИЗ ОПЕРАЦИОННЫХ ПРОБЛЕМ")
    print("-" * 40)
    
    closed_days = data['store_is_closed'].sum()
    busy_days = data['store_is_busy'].sum()
    closing_soon_days = data['store_is_closing_soon'].sum()
    out_of_stock_days = data['out_of_stock'].sum()
    avg_cancellation_rate = data['order_cancellation_rate'].mean()
    
    total_operational_issues = data['operational_issues'].sum()
    
    print(f"🏪 Дней когда магазин был закрыт: {closed_days}")
    print(f"🔥 Дней когда магазин был занят: {busy_days}")
    print(f"⏰ Дней 'скоро закрытие': {closing_soon_days}")
    print(f"📦 Дней с отсутствием товара: {out_of_stock_days}")
    print(f"❌ Средний процент отмен заказов: {avg_cancellation_rate:.1f}%")
    print(f"⚠️ Общие операционные проблемы: {total_operational_issues} случаев")
    
    if total_operational_issues > len(data) * 0.1:
        print("🚨 ВНИМАНИЕ: Высокий уровень операционных проблем!")
    
    print()
    
    # 5. НОВЫЙ! Детальный анализ качества обслуживания
    print("⭐ 5. АНАЛИЗ КАЧЕСТВА ОБСЛУЖИВАНИЯ")
    print("-" * 40)
    
    total_ratings = (data['one_star_ratings'].sum() + data['two_star_ratings'].sum() + 
                    data['three_star_ratings'].sum() + data['four_star_ratings'].sum() + 
                    data['five_star_ratings'].sum())
    
    if total_ratings > 0:
        five_star_rate = (data['five_star_ratings'].sum() / total_ratings) * 100
        four_star_rate = (data['four_star_ratings'].sum() / total_ratings) * 100
        three_star_rate = (data['three_star_ratings'].sum() / total_ratings) * 100
        two_star_rate = (data['two_star_ratings'].sum() / total_ratings) * 100
        one_star_rate = (data['one_star_ratings'].sum() / total_ratings) * 100
        
        print(f"⭐⭐⭐⭐⭐ 5 звезд: {data['five_star_ratings'].sum():,.0f} ({five_star_rate:.1f}%)")
        print(f"⭐⭐⭐⭐ 4 звезды: {data['four_star_ratings'].sum():,.0f} ({four_star_rate:.1f}%)")
        print(f"⭐⭐⭐ 3 звезды: {data['three_star_ratings'].sum():,.0f} ({three_star_rate:.1f}%)")
        print(f"⭐⭐ 2 звезды: {data['two_star_ratings'].sum():,.0f} ({two_star_rate:.1f}%)")
        print(f"⭐ 1 звезда: {data['one_star_ratings'].sum():,.0f} ({one_star_rate:.1f}%)")
        
        satisfaction_score = data['customer_satisfaction_score'].mean()
        print(f"📊 Общий индекс удовлетворенности: {satisfaction_score:.2f}/5.0")
        
        if one_star_rate > 10:
            print("🚨 КРИТИЧНО: Высокий процент 1-звездочных отзывов!")
    
    print()
    
    # 6. НОВЫЙ! Анализ времени обслуживания (Gojek)
    print("⏱️ 6. АНАЛИЗ ВРЕМЕНИ ОБСЛУЖИВАНИЯ")
    print("-" * 40)
    
    if data['realized_orders_percentage'].mean() > 0:
        avg_realization = data['realized_orders_percentage'].mean()
        lost_orders = data['lost_orders'].sum()
        print(f"✅ Процент выполненных заказов: {avg_realization:.1f}%")
        print(f"❌ Потерянные заказы: {lost_orders:,.0f}")
        
        if avg_realization < 90:
            print("🚨 КРИТИЧНО: Низкий процент выполнения заказов!")
    
    print()
    
    # 7. Рекомендации на основе ВСЕХ данных
    print("💡 7. УМНЫЕ РЕКОМЕНДАЦИИ НА ОСНОВЕ ВСЕХ ДАННЫХ")
    print("-" * 40)
    
    recommendations = []
    
    # Анализ клиентской базы
    if total_customers > 0:
        new_customer_rate = (new_customers / total_customers) * 100
        if new_customer_rate < 30:
            recommendations.append("👥 Увеличить привлечение новых клиентов (сейчас {:.1f}%)".format(new_customer_rate))
        
        retention_rate = (repeated_customers / total_customers) * 100
        if retention_rate < 40:
            recommendations.append("🔄 Улучшить удержание клиентов (сейчас {:.1f}%)".format(retention_rate))
    
    # Анализ маркетинговой воронки
    if total_impressions > 0:
        ctr = (total_menu_visits / total_impressions) * 100
        if ctr < 2:
            recommendations.append("📈 Улучшить CTR рекламы (сейчас {:.2f}%)".format(ctr))
        
        if total_menu_visits > 0:
            conversion_rate = (total_conversions / total_menu_visits) * 100
            if conversion_rate < 10:
                recommendations.append("✅ Оптимизировать конверсию (сейчас {:.2f}%)".format(conversion_rate))
    
    # Операционные проблемы
    if total_operational_issues > len(data) * 0.1:
        recommendations.append("⚠️ СРОЧНО: Решить операционные проблемы (слишком много)")
    
    # Качество обслуживания
    if total_ratings > 0:
        one_star_rate = (data['one_star_ratings'].sum() / total_ratings) * 100
        if one_star_rate > 5:
            recommendations.append("⭐ КРИТИЧНО: Снизить количество 1-звездочных отзывов ({:.1f}%)".format(one_star_rate))
    
    # Время обслуживания
    if data['realized_orders_percentage'].mean() > 0:
        avg_realization = data['realized_orders_percentage'].mean()
        if avg_realization < 90:
            recommendations.append("⏱️ Улучшить процент выполнения заказов ({:.1f}%)".format(avg_realization))
    
    print("Рекомендации:")
    if recommendations:
        for i, rec in enumerate(recommendations, 1):
            print(f"  {i}. {rec}")
    else:
        print("  ✅ Все показатели в норме!")
    
    print()
    
    # Сохраняем расширенный отчет
    try:
        os.makedirs('reports', exist_ok=True)
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"reports/full_analysis_{restaurant_name.replace(' ', '_')}_{timestamp}.txt"
        
        with open(filename, 'w', encoding='utf-8') as f:
            f.write(f"ПОЛНЫЙ АНАЛИЗ ВСЕХ ПАРАМЕТРОВ: {restaurant_name.upper()}\n")
            f.write("=" * 80 + "\n")
            f.write(f"Период: {start_date} → {end_date}\n")
            f.write(f"Создан: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            f.write("ИСПОЛЬЗОВАНЫ ВСЕ 30+ ПАРАМЕТРОВ ИЗ БАЗЫ ДАННЫХ\n\n")
            
            f.write("ОСНОВНЫЕ МЕТРИКИ:\n")
            f.write(f"Общие продажи: {total_sales:,.0f} IDR\n")
            f.write(f"Общие заказы: {total_orders:,.0f}\n")
            f.write(f"Общие клиенты: {total_customers:,.0f}\n")
            f.write(f"Новые клиенты: {new_customers:,.0f}\n")
            f.write(f"Операционные проблемы: {total_operational_issues}\n")
        
        print(f"💾 Полный отчет сохранен: {filename}")
        
    except Exception as e:
        print(f"❌ Ошибка сохранения отчета: {e}")

def list_restaurants():
    """Показывает список доступных ресторанов"""
    print("🏪 ДОСТУПНЫЕ РЕСТОРАНЫ MUZAQUEST")
    print("=" * 60)
    
    try:
        conn = sqlite3.connect("database.sqlite")
        
        # Получаем рестораны с их статистикой
        query = """
        SELECT r.id, r.name,
               COUNT(DISTINCT g.stat_date) as grab_days,
               COUNT(DISTINCT gj.stat_date) as gojek_days,
               MIN(COALESCE(g.stat_date, gj.stat_date)) as first_date,
               MAX(COALESCE(g.stat_date, gj.stat_date)) as last_date,
               SUM(COALESCE(g.sales, 0) + COALESCE(gj.sales, 0)) as total_sales
        FROM restaurants r
        LEFT JOIN grab_stats g ON r.id = g.restaurant_id
        LEFT JOIN gojek_stats gj ON r.id = gj.restaurant_id
        GROUP BY r.id, r.name
        HAVING (grab_days > 0 OR gojek_days > 0)
        ORDER BY total_sales DESC, r.name
        """
        
        df = pd.read_sql_query(query, conn)
        
        for i, row in df.iterrows():
            total_days = max(row['grab_days'] or 0, row['gojek_days'] or 0)
            
            print(f"{i+1:2d}. 🍽️ {row['name']}")
            print(f"    📊 Данных: {total_days} дней ({row['first_date']} → {row['last_date']})")
            print(f"    📈 Grab: {row['grab_days'] or 0} дней | Gojek: {row['gojek_days'] or 0} дней")
            
            if row['total_sales']:
                print(f"    💰 Общие продажи: {row['total_sales']:,.0f} IDR")
            
            print()
        
        conn.close()
        
    except Exception as e:
        print(f"❌ Ошибка при получении списка ресторанов: {e}")

def analyze_market(start_date=None, end_date=None):
    """Анализ всего рынка"""
    print("\n🌍 АНАЛИЗ ВСЕГО РЫНКА MUZAQUEST")
    print("=" * 80)
    
    # Устанавливаем период по умолчанию
    if not start_date or not end_date:
        start_date = "2025-04-01"
        end_date = "2025-06-22"
    
    print(f"📅 Период анализа: {start_date} → {end_date}")
    print()
    
    try:
        conn = sqlite3.connect("database.sqlite")
        
        # Общая статистика рынка
        query = """
        WITH market_data AS (
            SELECT r.name,
                   SUM(COALESCE(g.sales, 0) + COALESCE(gj.sales, 0)) as total_sales,
                   SUM(COALESCE(g.orders, 0) + COALESCE(gj.orders, 0)) as total_orders
            FROM restaurants r
            LEFT JOIN grab_stats g ON r.id = g.restaurant_id 
                AND g.stat_date BETWEEN ? AND ?
            LEFT JOIN gojek_stats gj ON r.id = gj.restaurant_id 
                AND gj.stat_date BETWEEN ? AND ?
            GROUP BY r.name
            HAVING total_sales > 0
        )
        SELECT 
            COUNT(*) as active_restaurants,
            SUM(total_sales) as market_sales,
            SUM(total_orders) as market_orders,
            AVG(total_sales) as avg_restaurant_sales
        FROM market_data
        """
        
        market_stats = pd.read_sql_query(query, conn, params=(start_date, end_date, start_date, end_date))
        
        print("📊 ОБЗОР РЫНКА")
        print("-" * 40)
        if not market_stats.empty:
            stats = market_stats.iloc[0]
            print(f"🏪 Активных ресторанов: {stats['active_restaurants']}")
            print(f"💰 Общие продажи рынка: {stats['market_sales']:,.0f} IDR")
            print(f"📦 Общие заказы рынка: {stats['market_orders']:,.0f}")
            print(f"📊 Средние продажи на ресторан: {stats['avg_restaurant_sales']:,.0f} IDR")
        
        # Лидеры рынка
        leaders_query = """
        SELECT r.name,
               SUM(COALESCE(g.sales, 0) + COALESCE(gj.sales, 0)) as total_sales,
               SUM(COALESCE(g.orders, 0) + COALESCE(gj.orders, 0)) as total_orders,
               AVG(COALESCE(g.rating, gj.rating)) as avg_rating
        FROM restaurants r
        LEFT JOIN grab_stats g ON r.id = g.restaurant_id 
            AND g.stat_date BETWEEN ? AND ?
        LEFT JOIN gojek_stats gj ON r.id = gj.restaurant_id 
            AND gj.stat_date BETWEEN ? AND ?
        GROUP BY r.name
        HAVING total_sales > 0
        ORDER BY total_sales DESC
        LIMIT 10
        """
        
        leaders = pd.read_sql_query(leaders_query, conn, params=(start_date, end_date, start_date, end_date))
        
        print(f"\n🏆 ЛИДЕРЫ РЫНКА")
        print("-" * 40)
        print("ТОП-10 по продажам:")
        for i, row in leaders.iterrows():
            avg_order_value = row['total_sales'] / row['total_orders'] if row['total_orders'] > 0 else 0
            print(f"  {i+1:2d}. {row['name']:<25} {row['total_sales']:>12,.0f} IDR")
            print(f"      📦 {row['total_orders']:,} заказов | 💰 {avg_order_value:,.0f} IDR/заказ | ⭐ {row['avg_rating']:.2f}")
        
        conn.close()
        
    except Exception as e:
        print(f"❌ Ошибка при анализе рынка: {e}")

def main():
    """Главная функция CLI"""
    
    print("""
🎯 MUZAQUEST ANALYTICS - ПОЛНЫЙ АНАЛИЗ ВСЕХ ПАРАМЕТРОВ
═══════════════════════════════════════════════════════════════════════════════
🚀 Используем ВСЕ 30+ полей из grab_stats и gojek_stats!
═══════════════════════════════════════════════════════════════════════════════
""")
    
    parser = argparse.ArgumentParser(
        description="Muzaquest Analytics - Полный анализ всех параметров",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
ПРИМЕРЫ ИСПОЛЬЗОВАНИЯ:
  
  📋 Список ресторанов:
    python main.py list
  
  🔬 Полный анализ ресторана (ВСЕ 30+ параметров):
    python main.py analyze "Ika Canggu"
    python main.py analyze "Ika Canggu" --start 2025-04-01 --end 2025-06-22
  
  🌍 Анализ всего рынка:
    python main.py market
    python main.py market --start 2025-04-01 --end 2025-06-22

НОВЫЕ ВОЗМОЖНОСТИ:
  👥 Анализ клиентской базы (новые/повторные/реактивированные)
  📈 Маркетинговая воронка (показы → клики → конверсии)
  ⚠️ Операционные проблемы (закрыт/занят/нет товара)
  ⭐ Детальные рейтинги (1-5 звезд)
  ⏱️ Анализ времени обслуживания
  💡 Умные рекомендации на основе всех данных
        """
    )
    
    parser.add_argument('command', 
                       choices=['list', 'analyze', 'market'],
                       help='Команда для выполнения')
    
    parser.add_argument('restaurant', nargs='?', 
                       help='Название ресторана для анализа')
    
    parser.add_argument('--start', 
                       help='Дата начала периода (YYYY-MM-DD)')
    
    parser.add_argument('--end', 
                       help='Дата окончания периода (YYYY-MM-DD)')
    
    args = parser.parse_args()
    
    # Проверяем наличие базы данных
    if not os.path.exists('database.sqlite'):
        print("❌ База данных 'database.sqlite' не найдена!")
        print("   Убедитесь, что файл database.sqlite находится в корневой папке")
        sys.exit(1)
    
    try:
        if args.command == 'list':
            list_restaurants()
            
        elif args.command == 'analyze':
            if not args.restaurant:
                print("❌ Укажите название ресторана для анализа")
                print("   Используйте: python main.py analyze \"Название ресторана\"")
                sys.exit(1)
            
            analyze_restaurant(args.restaurant, args.start, args.end)
            
        elif args.command == 'market':
            analyze_market(args.start, args.end)
    
    except KeyboardInterrupt:
        print("\n\n🛑 Анализ прерван пользователем")
        sys.exit(0)
    
    except Exception as e:
        print(f"\n❌ Критическая ошибка: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()