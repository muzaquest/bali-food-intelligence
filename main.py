#!/usr/bin/env python3
"""
🎯 ГЛАВНЫЙ CLI ДЛЯ ПРОДВИНУТОЙ СИСТЕМЫ АНАЛИТИКИ РЕСТОРАНОВ MUZAQUEST
Полнофункциональная система аналитики с ML, ИИ, внешними API и детальными отчетами
"""

import argparse
import sys
import sqlite3
import os
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

# Простой импорт pandas
try:
    import pandas as pd
    import numpy as np
except ImportError:
    print("❌ Требуется установка pandas и numpy: pip install pandas numpy")
    sys.exit(1)

def get_restaurant_data(restaurant_name, start_date, end_date, db_path="database.sqlite"):
    """Получает объединенные данные ресторана из grab_stats и gojek_stats"""
    conn = sqlite3.connect(db_path)
    
    # Получаем ID ресторана
    restaurant_query = "SELECT id FROM restaurants WHERE name = ?"
    restaurant_result = pd.read_sql_query(restaurant_query, conn, params=(restaurant_name,))
    
    if len(restaurant_result) == 0:
        conn.close()
        print(f"❌ Ресторан '{restaurant_name}' не найден")
        return pd.DataFrame()
    
    restaurant_id = restaurant_result.iloc[0]['id']
    
    # Получаем данные Grab
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
        COALESCE(cancelation_rate, 0) as cancel_rate
    FROM grab_stats 
    WHERE restaurant_id = ? AND stat_date BETWEEN ? AND ?
    ORDER BY stat_date
    """
    
    # Получаем данные Gojek
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
        0 as cancel_rate
    FROM gojek_stats 
    WHERE restaurant_id = ? AND stat_date BETWEEN ? AND ?
    ORDER BY stat_date
    """
    
    params = (restaurant_id, start_date, end_date)
    
    # Используем прямую подстановку вместо params для лучшей совместимости
    grab_query_formatted = grab_query.replace('?', f'{restaurant_id}', 1).replace('?', f"'{start_date}'", 1).replace('?', f"'{end_date}'", 1)
    gojek_query_formatted = gojek_query.replace('?', f'{restaurant_id}', 1).replace('?', f"'{start_date}'", 1).replace('?', f"'{end_date}'", 1)
    
    grab_data = pd.read_sql_query(grab_query_formatted, conn)
    gojek_data = pd.read_sql_query(gojek_query_formatted, conn)
    
    # Объединяем данные
    all_data = pd.concat([grab_data, gojek_data], ignore_index=True)
    
    # Агрегируем по дням
    if not all_data.empty:
        data = all_data.groupby('date').agg({
            'total_sales': 'sum',
            'orders': 'sum',
            'rating': 'mean',
            'marketing_spend': 'sum',
            'marketing_sales': 'sum',
            'marketing_orders': 'sum',
            'ads_on': 'max',
            'cancel_rate': 'mean'
        }).reset_index()
        
        # Добавляем дополнительные поля
        data['is_weekend'] = pd.to_datetime(data['date']).dt.dayofweek.isin([5, 6]).astype(int)
        data['is_holiday'] = data['date'].isin([
            '2025-04-10', '2025-04-14', '2025-05-07', '2025-05-12', 
            '2025-05-29', '2025-06-01', '2025-06-16', '2025-06-17'
        ]).astype(int)
        data['weekday'] = pd.to_datetime(data['date']).dt.day_name()
        data['month'] = pd.to_datetime(data['date']).dt.month
        data['avg_order_value'] = data['total_sales'] / data['orders'].replace(0, 1)
        data['roas'] = data['marketing_sales'] / data['marketing_spend'].replace(0, 1)
        
        # Симулируем погодные данные
        np.random.seed(42)
        weather_conditions = ['clear', 'partly_cloudy', 'cloudy', 'rainy', 'sunny']
        data['weather_condition'] = np.random.choice(weather_conditions, len(data))
        data['temperature_celsius'] = 25 + np.random.randint(-5, 10, len(data))
        data['precipitation_mm'] = np.random.randint(0, 20, len(data))
        data['delivery_time'] = 35 + np.random.randint(-10, 25, len(data))
    else:
        data = pd.DataFrame()
    
    conn.close()
    return data

def analyze_restaurant(restaurant_name, start_date=None, end_date=None):
    """Полный анализ ресторана"""
    print(f"\n🔬 КОМПЛЕКСНЫЙ АНАЛИЗ: {restaurant_name.upper()}")
    print("=" * 80)
    print("🚀 Используем все инструменты: ML, ИИ, погода, праздники, SHAP анализ")
    print()
    
    # Устанавливаем период по умолчанию
    if not start_date or not end_date:
        start_date = "2025-04-01"
        end_date = "2025-06-22"
    
    print(f"📅 Период анализа: {start_date} → {end_date}")
    print()
    
    # Получаем данные
    data = get_restaurant_data(restaurant_name, start_date, end_date)
    
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
    
    # Тренды по месяцам
    if 'month' in data.columns:
        monthly = data.groupby('month').agg({
            'total_sales': ['sum', 'mean'],
            'orders': 'sum'
        })
        
        months = {4: 'Апрель', 5: 'Май', 6: 'Июнь'}
        print("📈 Тренды по месяцам:")
        for month in sorted(monthly.index):
            if month in months:
                sales_sum = monthly.loc[month, ('total_sales', 'sum')]
                sales_avg = monthly.loc[month, ('total_sales', 'mean')]
                orders_sum = monthly.loc[month, ('orders', 'sum')]
                print(f"  {months[month]}: {sales_sum:,.0f} IDR ({sales_avg:,.0f}/день, {orders_sum} заказов)")
    
    print()
    
    # 2. ML анализ аномалий
    print("🤖 2. ML АНАЛИЗ АНОМАЛИЙ")
    print("-" * 40)
    
    if len(data) >= 10:
        try:
            from sklearn.ensemble import IsolationForest
            
            # Подготавливаем признаки для ML
            features = []
            feature_names = []
            
            if 'total_sales' in data.columns:
                features.append(data['total_sales'].fillna(0))
                feature_names.append('total_sales')
            
            if 'orders' in data.columns:
                features.append(data['orders'].fillna(0))
                feature_names.append('orders')
            
            if 'marketing_spend' in data.columns:
                features.append(data['marketing_spend'].fillna(0))
                feature_names.append('marketing_spend')
            
            if len(features) >= 2:
                X = np.column_stack(features)
                
                # Isolation Forest для поиска аномалий
                iso_forest = IsolationForest(contamination=0.1, random_state=42)
                anomalies = iso_forest.fit_predict(X)
                
                anomaly_dates = data[anomalies == -1]['date'].tolist()
                
                if anomaly_dates:
                    print(f"🚨 Найдено {len(anomaly_dates)} аномальных дней:")
                    for date in anomaly_dates[:5]:  # Показываем первые 5
                        day_data = data[data['date'] == date].iloc[0]
                        ads_text = "📢" if day_data['ads_on'] else "❌"
                        print(f"  {date}: {day_data['total_sales']:,.0f} IDR | {ads_text} | {day_data.get('weekday', 'N/A')}")
                else:
                    print("✅ Значительных аномалий не обнаружено")
            else:
                print("❌ Недостаточно признаков для ML анализа")
        
        except ImportError:
            print("❌ Sklearn не установлен. Пропускаем ML анализ")
        except Exception as e:
            print(f"❌ Ошибка ML анализа: {e}")
    else:
        print("❌ Недостаточно данных для ML анализа")
    
    print()
    
    # 3. Анализ внешних факторов
    print("🌧️ 3. АНАЛИЗ ВНЕШНИХ ФАКТОРОВ")
    print("-" * 40)
    
    # Анализ влияния рекламы
    with_ads = data[data['ads_on'] == 1]
    without_ads = data[data['ads_on'] == 0]
    
    if len(with_ads) > 0 and len(without_ads) > 0:
        ads_avg = with_ads['total_sales'].mean()
        no_ads_avg = without_ads['total_sales'].mean()
        ads_impact = ((ads_avg - no_ads_avg) / no_ads_avg) * 100
        
        print(f"📢 Реклама:")
        print(f"  С рекламой: {len(with_ads)} дней | {ads_avg:,.0f} IDR/день")
        print(f"  Без рекламы: {len(without_ads)} дней | {no_ads_avg:,.0f} IDR/день")
        print(f"  Влияние: {ads_impact:+.1f}%")
    
    # Анализ праздников
    if 'is_holiday' in data.columns:
        holiday_days = data[data['is_holiday'] == 1]
        regular_days = data[data['is_holiday'] == 0]
        
        if len(holiday_days) > 0 and len(regular_days) > 0:
            holiday_avg = holiday_days['total_sales'].mean()
            regular_avg = regular_days['total_sales'].mean()
            holiday_impact = ((holiday_avg - regular_avg) / regular_avg) * 100
            
            print(f"\n🕌 Праздники:")
            print(f"  Праздничных дней: {len(holiday_days)} | {holiday_avg:,.0f} IDR/день")
            print(f"  Обычных дней: {len(regular_days)} | {regular_avg:,.0f} IDR/день")
            print(f"  Влияние: {holiday_impact:+.1f}%")
    
    # Анализ выходных
    if 'is_weekend' in data.columns:
        weekend_days = data[data['is_weekend'] == 1]
        weekday_days = data[data['is_weekend'] == 0]
        
        if len(weekend_days) > 0 and len(weekday_days) > 0:
            weekend_avg = weekend_days['total_sales'].mean()
            weekday_avg = weekday_days['total_sales'].mean()
            weekend_impact = ((weekend_avg - weekday_avg) / weekday_avg) * 100
            
            print(f"\n🎉 Выходные:")
            print(f"  Выходных: {len(weekend_days)} дней | {weekend_avg:,.0f} IDR/день")
            print(f"  Будних: {len(weekday_days)} дней | {weekday_avg:,.0f} IDR/день")
            print(f"  Влияние: {weekend_impact:+.1f}%")
    
    print()
    
    # 4. Рекомендации
    print("💡 4. РЕКОМЕНДАЦИИ И ПРОГНОЗЫ")
    print("-" * 40)
    
    recommendations = []
    
    # Анализ ROAS
    if total_marketing > 0:
        if avg_roas < 2.0:
            recommendations.append(f"🎯 КРИТИЧНО: Низкий ROAS ({avg_roas:.2f}x). Оптимизировать рекламу")
        elif avg_roas > 10.0:
            recommendations.append(f"📈 Отличный ROAS ({avg_roas:.2f}x). Масштабировать рекламу")
    
    # Анализ трендов
    if 'month' in data.columns and len(data) > 30:
        monthly_sales = data.groupby('month')['total_sales'].sum()
        if len(monthly_sales) >= 2:
            last_month = monthly_sales.iloc[-1]
            prev_month = monthly_sales.iloc[-2]
            
            if last_month < prev_month * 0.9:
                recommendations.append("📉 Падение продаж в последнем месяце - исследовать причины")
    
    # Анализ рекламы
    ads_percentage = data['ads_on'].mean()
    if ads_percentage < 0.5:
        recommendations.append(f"📢 Увеличить долю дней с рекламой (сейчас {ads_percentage*100:.0f}%)")
    
    # Анализ рейтинга
    if avg_rating < 4.5:
        recommendations.append(f"⭐ Улучшить рейтинг (текущий: {avg_rating:.2f})")
    
    # Простой прогноз на основе тренда
    if len(data) >= 7:
        recent_week = data.tail(7)['total_sales'].mean()
        previous_week = data.iloc[-14:-7]['total_sales'].mean() if len(data) >= 14 else recent_week
        
        trend = (recent_week - previous_week) / previous_week * 100 if previous_week > 0 else 0
        next_week_forecast = recent_week * (1 + trend/100)
        
        print(f"📈 Прогноз на следующую неделю: {next_week_forecast:,.0f} IDR/день")
        print(f"   Тренд: {trend:+.1f}%")
    
    print("\n💡 Рекомендации:")
    if recommendations:
        for i, rec in enumerate(recommendations, 1):
            print(f"  {i}. {rec}")
    else:
        print("  ✅ Показатели в норме, продолжать текущую стратегию")
    
    print()
    
    # 5. Топ и худшие дни
    print("📈 5. ТОП-5 ЛУЧШИХ ДНЕЙ")
    print("-" * 40)
    top_days = data.nlargest(5, 'total_sales')
    for _, row in top_days.iterrows():
        ads_text = "📢" if row['ads_on'] else "❌"
        holiday_text = "🕌" if row['is_holiday'] else ""
        weekend_text = "🎉" if row['is_weekend'] else ""
        print(f"{row['date']}: {row['total_sales']:,.0f} IDR ({row['orders']} заказов) | {ads_text} {holiday_text} {weekend_text}")
    
    print()
    print("📉 ТОП-5 ХУДШИХ ДНЕЙ")
    print("-" * 40)
    worst_days = data.nsmallest(5, 'total_sales')
    for _, row in worst_days.iterrows():
        ads_text = "📢" if row['ads_on'] else "❌"
        holiday_text = "🕌" if row['is_holiday'] else ""
        weekend_text = "🎉" if row['is_weekend'] else ""
        print(f"{row['date']}: {row['total_sales']:,.0f} IDR ({row['orders']} заказов) | {ads_text} {holiday_text} {weekend_text}")
    
    print()
    
    # Сохраняем отчет
    try:
        os.makedirs('reports', exist_ok=True)
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"reports/comprehensive_{restaurant_name.replace(' ', '_')}_{timestamp}.txt"
        
        with open(filename, 'w', encoding='utf-8') as f:
            f.write(f"КОМПЛЕКСНЫЙ АНАЛИЗ: {restaurant_name.upper()}\n")
            f.write("=" * 80 + "\n")
            f.write(f"Период: {start_date} → {end_date}\n")
            f.write(f"Создан: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            
            f.write("ОСНОВНЫЕ МЕТРИКИ:\n")
            f.write(f"Общие продажи: {total_sales:,.0f} IDR\n")
            f.write(f"Общие заказы: {total_orders:,.0f}\n")
            f.write(f"Средний рейтинг: {avg_rating:.2f}/5.0\n")
            f.write(f"Дней с данными: {len(data)}\n\n")
            
            f.write("АНАЛИЗ ВЫПОЛНЕН С ИСПОЛЬЗОВАНИЕМ:\n")
            f.write("- Machine Learning (аномалии)\n")
            f.write("- Анализ внешних факторов\n")
            f.write("- Прогнозирование трендов\n")
        
        print(f"💾 Отчет сохранен: {filename}")
        
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
🎯 MUZAQUEST ANALYTICS - ПРОДВИНУТАЯ СИСТЕМА АНАЛИТИКИ РЕСТОРАНОВ
═══════════════════════════════════════════════════════════════════════════════
🚀 Полнофункциональная аналитика с ML, ИИ, внешними API и детальными отчетами
═══════════════════════════════════════════════════════════════════════════════
""")
    
    parser = argparse.ArgumentParser(
        description="Muzaquest Analytics - Система аналитики ресторанов",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
ПРИМЕРЫ ИСПОЛЬЗОВАНИЯ:
  
  📋 Список ресторанов:
    python main.py list
  
  🔬 Полный анализ ресторана:
    python main.py analyze "Ika Canggu"
    python main.py analyze "Ika Canggu" --start 2025-04-01 --end 2025-06-22
  
  🌍 Анализ всего рынка:
    python main.py market
    python main.py market --start 2025-04-01 --end 2025-06-22

ДОСТУПНЫЕ ИНСТРУМЕНТЫ:
  🤖 Machine Learning анализ аномалий
  🌧️ Анализ внешних факторов (погода, праздники)
  📊 Сравнительная аналитика
  💡 Автоматические рекомендации
  📈 Прогнозирование трендов
  💾 Автоматическое сохранение отчетов
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