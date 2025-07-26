import sqlite3
import pandas as pd
import json
from datetime import datetime, timedelta
import numpy as np

class RealHolidayAnalyzer:
    """
    Анализ РЕАЛЬНОГО влияния праздников на продажи
    Основан только на данных из database.sqlite
    """
    
    def __init__(self):
        self.db_path = 'database.sqlite'
        
    def get_holiday_list(self):
        """Получает список реальных балийских праздников за анализируемый период"""
        # Основываемся на периоде данных в базе
        conn = sqlite3.connect(self.db_path)
        
        # Получаем диапазон дат из базы
        date_range_query = """
            SELECT MIN(stat_date) as min_date, MAX(stat_date) as max_date 
            FROM grab_stats
            UNION ALL
            SELECT MIN(stat_date) as min_date, MAX(stat_date) as max_date 
            FROM gojek_stats
        """
        date_ranges = pd.read_sql_query(date_range_query, conn)
        
        min_date = date_ranges['min_date'].min()
        max_date = date_ranges['max_date'].max()
        
        conn.close()
        
        print(f"📅 Анализ праздников за период: {min_date} → {max_date}")
        
        # РЕАЛЬНЫЕ балийские праздники за этот период
        # Основано на официальном календаре Бали 2025
        holidays = {
            # Общенациональные праздники Индонезии
            '2025-01-01': {'name': 'New Year Day', 'type': 'national', 'category': 'Новый год'},
            '2025-01-29': {'name': 'Chinese New Year', 'type': 'national', 'category': 'Китайский НГ'},
            '2025-03-29': {'name': 'Nyepi (Day of Silence)', 'type': 'balinese', 'category': 'День тишины'},
            '2025-03-31': {'name': 'Eid al-Fitr', 'type': 'national', 'category': 'Ураза-байрам'},
            '2025-04-01': {'name': 'Eid al-Fitr Holiday', 'type': 'national', 'category': 'Ураза-байрам'},
            '2025-04-18': {'name': 'Good Friday', 'type': 'national', 'category': 'Страстная пятница'},
            '2025-05-01': {'name': 'Labor Day', 'type': 'national', 'category': 'День труда'},
            '2025-05-12': {'name': 'Vesak Day', 'type': 'national', 'category': 'День Будды'},
            '2025-05-29': {'name': 'Ascension Day', 'type': 'national', 'category': 'Вознесение'},
            '2025-06-01': {'name': 'Pancasila Day', 'type': 'national', 'category': 'День Панчасила'},
            '2025-06-06': {'name': 'Eid al-Adha', 'type': 'national', 'category': 'Курбан-байрам'},
            '2025-08-17': {'name': 'Independence Day', 'type': 'national', 'category': 'День независимости'},
            
            # Специфически балийские праздники
            '2025-04-16': {'name': 'Galungan', 'type': 'balinese', 'category': 'Galungan'},
            '2025-04-26': {'name': 'Kuningan', 'type': 'balinese', 'category': 'Kuningan'},
            
            # Полнолуния (Purnama) - религиозные дни
            '2025-01-13': {'name': 'Purnama (Full Moon)', 'type': 'balinese', 'category': 'Полнолуние'},
            '2025-02-12': {'name': 'Purnama (Full Moon)', 'type': 'balinese', 'category': 'Полнолуние'},
            '2025-03-14': {'name': 'Purnama (Full Moon)', 'type': 'balinese', 'category': 'Полнолуние'},
            '2025-04-13': {'name': 'Purnama (Full Moon)', 'type': 'balinese', 'category': 'Полнолуние'},
            '2025-05-12': {'name': 'Purnama (Full Moon)', 'type': 'balinese', 'category': 'Полнолуние'},
            '2025-06-11': {'name': 'Purnama (Full Moon)', 'type': 'balinese', 'category': 'Полнолуние'},
            
            # Новолуния (Tilem) - дни очищения
            '2025-01-29': {'name': 'Tilem (New Moon)', 'type': 'balinese', 'category': 'Новолуние'},
            '2025-02-28': {'name': 'Tilem (New Moon)', 'type': 'balinese', 'category': 'Новолуние'},
            '2025-03-29': {'name': 'Tilem (New Moon)', 'type': 'balinese', 'category': 'Новолуние'},
            '2025-04-27': {'name': 'Tilem (New Moon)', 'type': 'balinese', 'category': 'Новолуние'},
            '2025-05-27': {'name': 'Tilem (New Moon)', 'type': 'balinese', 'category': 'Новолуние'},
            '2025-06-25': {'name': 'Tilem (New Moon)', 'type': 'balinese', 'category': 'Новолуние'},
        }
        
        # Фильтруем только праздники в диапазоне данных
        filtered_holidays = {}
        for date, info in holidays.items():
            if min_date <= date <= max_date:
                filtered_holidays[date] = info
        
        print(f"🎉 Найдено праздников в периоде: {len(filtered_holidays)}")
        
        return filtered_holidays, min_date, max_date
    
    def analyze_holiday_impact(self):
        """Анализирует реальное влияние каждого праздника на продажи"""
        print("🔍 АНАЛИЗ РЕАЛЬНОГО ВЛИЯНИЯ ПРАЗДНИКОВ")
        print("=" * 50)
        
        holidays, min_date, max_date = self.get_holiday_list()
        
        if not holidays:
            print("❌ Нет праздников в анализируемом периоде")
            return
        
        conn = sqlite3.connect(self.db_path)
        
        # Получаем все данные по продажам с названиями ресторанов
        query = """
            SELECT 
                g.stat_date as date,
                r.name as restaurant_name,
                g.sales as grab_sales,
                0 as gojek_sales
            FROM grab_stats g
            JOIN restaurants r ON g.restaurant_id = r.id
            WHERE g.stat_date BETWEEN ? AND ?
            UNION ALL
            SELECT 
                gj.stat_date as date,
                r.name as restaurant_name,
                0 as grab_sales,
                gj.sales as gojek_sales
            FROM gojek_stats gj
            JOIN restaurants r ON gj.restaurant_id = r.id
            WHERE gj.stat_date BETWEEN ? AND ?
        """
        
        sales_data = pd.read_sql_query(query, conn, params=[min_date, max_date, min_date, max_date])
        conn.close()
        
        # Группируем по дате и ресторану, суммируем продажи
        sales_data['grab_sales'] = pd.to_numeric(sales_data['grab_sales'], errors='coerce').fillna(0)
        sales_data['gojek_sales'] = pd.to_numeric(sales_data['gojek_sales'], errors='coerce').fillna(0)
        sales_data['total_sales'] = sales_data['grab_sales'] + sales_data['gojek_sales']
        daily_sales = sales_data.groupby(['date', 'restaurant_name'])['total_sales'].sum().reset_index()
        
        # Агрегируем по дням
        market_daily = daily_sales.groupby('date')['total_sales'].sum().reset_index()
        
        print(f"📊 Данных по дням: {len(market_daily)}")
        print(f"📊 Всего ресторанов: {daily_sales['restaurant_name'].nunique()}")
        
        # Анализируем каждый праздник
        results = {}
        
        # Базовая средняя продажа (без праздников)
        holiday_dates = list(holidays.keys())
        regular_days = market_daily[~market_daily['date'].isin(holiday_dates)]
        baseline_avg = regular_days['total_sales'].mean() if len(regular_days) > 0 else 0
        
        print(f"\n📈 Базовая средняя продажа (обычные дни): {baseline_avg:,.0f} IDR")
        
        categories_impact = {}
        
        for holiday_date, holiday_info in holidays.items():
            # Продажи в праздничный день
            holiday_sales = market_daily[market_daily['date'] == holiday_date]['total_sales']
            
            if len(holiday_sales) > 0:
                holiday_total = holiday_sales.iloc[0]
                impact_percent = ((holiday_total - baseline_avg) / baseline_avg * 100) if baseline_avg > 0 else 0
                
                results[holiday_date] = {
                    'name': holiday_info['name'],
                    'type': holiday_info['type'],
                    'category': holiday_info['category'],
                    'sales': holiday_total,
                    'impact_percent': impact_percent
                }
                
                # Группируем по категориям
                category = holiday_info['category']
                if category not in categories_impact:
                    categories_impact[category] = []
                categories_impact[category].append(impact_percent)
                
                # Эмодзи для визуализации
                if impact_percent > 10:
                    emoji = "🔥"
                elif impact_percent > 0:
                    emoji = "📈"
                elif impact_percent > -10:
                    emoji = "📉"
                else:
                    emoji = "💥"
                
                print(f"  {emoji} {holiday_date}: {holiday_info['name']}")
                print(f"     Продажи: {holiday_total:,.0f} IDR ({impact_percent:+.1f}%)")
                print(f"     Тип: {holiday_info['type']} | Категория: {holiday_info['category']}")
                print()
            else:
                print(f"  ❌ {holiday_date}: {holiday_info['name']} - нет данных")
                results[holiday_date] = {
                    'name': holiday_info['name'],
                    'type': holiday_info['type'],
                    'category': holiday_info['category'],
                    'sales': 0,
                    'impact_percent': 0
                }
        
        # Анализ по категориям
        print("\n🎯 АНАЛИЗ ПО КАТЕГОРИЯМ ПРАЗДНИКОВ:")
        print("=" * 40)
        
        for category, impacts in categories_impact.items():
            if impacts:
                avg_impact = np.mean(impacts)
                count = len(impacts)
                
                if avg_impact > 5:
                    trend = "📈 ПОЛОЖИТЕЛЬНОЕ"
                elif avg_impact < -5:
                    trend = "📉 НЕГАТИВНОЕ"
                else:
                    trend = "➡️ НЕЙТРАЛЬНОЕ"
                
                print(f"🏷️ {category}:")
                print(f"   {trend} влияние: {avg_impact:+.1f}% (среднее)")
                print(f"   📊 Количество дней: {count}")
                print(f"   📈 Диапазон: {min(impacts):+.1f}% → {max(impacts):+.1f}%")
                print()
        
        # Сравнение типов праздников
        print("\n🇮🇩 СРАВНЕНИЕ: НАЦИОНАЛЬНЫЕ vs БАЛИЙСКИЕ")
        print("=" * 45)
        
        national_impacts = [r['impact_percent'] for r in results.values() if r['type'] == 'national' and r['impact_percent'] != 0]
        balinese_impacts = [r['impact_percent'] for r in results.values() if r['type'] == 'balinese' and r['impact_percent'] != 0]
        
        if national_impacts:
            national_avg = np.mean(national_impacts)
            print(f"🇮🇩 Национальные праздники: {national_avg:+.1f}% (среднее влияние)")
            print(f"   📊 Количество: {len(national_impacts)} дней")
        
        if balinese_impacts:
            balinese_avg = np.mean(balinese_impacts)
            print(f"🏝️ Балийские праздники: {balinese_avg:+.1f}% (среднее влияние)")
            print(f"   📊 Количество: {len(balinese_impacts)} дней")
        
        # Топ/худшие праздники
        print("\n🏆 ТОП-5 ЛУЧШИХ ПРАЗДНИКОВ ДЛЯ ПРОДАЖ:")
        print("=" * 35)
        
        sorted_results = sorted(results.items(), key=lambda x: x[1]['impact_percent'], reverse=True)
        
        for i, (date, data) in enumerate(sorted_results[:5], 1):
            if data['impact_percent'] != 0:
                print(f"{i}. {data['name']} ({date})")
                print(f"   💰 Продажи: {data['sales']:,.0f} IDR ({data['impact_percent']:+.1f}%)")
        
        print("\n💥 ТОП-5 ХУДШИХ ПРАЗДНИКОВ ДЛЯ ПРОДАЖ:")
        print("=" * 35)
        
        for i, (date, data) in enumerate(sorted_results[-5:], 1):
            if data['impact_percent'] != 0:
                print(f"{i}. {data['name']} ({date})")
                print(f"   💸 Продажи: {data['sales']:,.0f} IDR ({data['impact_percent']:+.1f}%)")
        
        # Сохраняем результаты
        output_file = 'data/real_holiday_impact_analysis.json'
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump({
                'analysis_period': {'start': min_date, 'end': max_date},
                'baseline_average': baseline_avg,
                'total_holidays': len(holidays),
                'holidays_with_data': len([r for r in results.values() if r['impact_percent'] != 0]),
                'results': results,
                'category_averages': {cat: np.mean(impacts) for cat, impacts in categories_impact.items()},
                'type_averages': {
                    'national': np.mean(national_impacts) if national_impacts else 0,
                    'balinese': np.mean(balinese_impacts) if balinese_impacts else 0
                }
            }, f, ensure_ascii=False, indent=2)
        
        print(f"\n💾 Результаты сохранены в: {output_file}")
        
        return results
    
    def check_nyepi_specifically(self):
        """Специальная проверка Nyepi - самого важного балийского праздника"""
        print("\n🔍 СПЕЦИАЛЬНАЯ ПРОВЕРКА: NYEPI (ДЕНЬ ТИШИНЫ)")
        print("=" * 50)
        
        conn = sqlite3.connect(self.db_path)
        
        # Ищем данные по Nyepi (29 марта 2025)
        nyepi_date = '2025-03-29'
        
        # Продажи в день Nyepi
        nyepi_query = """
            SELECT 
                g.stat_date,
                r.name as restaurant_name,
                SUM(g.sales + COALESCE(gj.sales, 0)) as total_sales
            FROM grab_stats g
            JOIN restaurants r ON g.restaurant_id = r.id
            LEFT JOIN gojek_stats gj ON gj.restaurant_id = r.id AND gj.stat_date = g.stat_date
            WHERE g.stat_date = ?
            GROUP BY g.stat_date, r.name
        """
        
        nyepi_data = pd.read_sql_query(nyepi_query, conn, params=[nyepi_date])
        
        # Средние продажи за неделю до и после Nyepi
        before_after_query = """
            SELECT 
                g.stat_date,
                SUM(g.sales + COALESCE(gj.sales, 0)) as daily_total
            FROM grab_stats g
            JOIN restaurants r ON g.restaurant_id = r.id
            LEFT JOIN gojek_stats gj ON gj.restaurant_id = r.id AND gj.stat_date = g.stat_date
            WHERE g.stat_date BETWEEN ? AND ? AND g.stat_date != ?
            GROUP BY g.stat_date
        """
        
        week_before = '2025-03-22'
        week_after = '2025-04-05'
        
        normal_data = pd.read_sql_query(before_after_query, conn, 
                                       params=[week_before, week_after, nyepi_date])
        
        conn.close()
        
        if len(nyepi_data) > 0:
            nyepi_total = nyepi_data['total_sales'].sum()
            print(f"📊 Продажи в Nyepi ({nyepi_date}): {nyepi_total:,.0f} IDR")
            print(f"📊 Ресторанов с данными: {len(nyepi_data)}")
            
            if len(normal_data) > 0:
                normal_avg = normal_data['daily_total'].mean()
                impact = ((nyepi_total - normal_avg) / normal_avg * 100) if normal_avg > 0 else 0
                
                print(f"📊 Средние продажи в обычные дни: {normal_avg:,.0f} IDR")
                print(f"📊 Реальное влияние Nyepi: {impact:+.1f}%")
                
                if impact < -30:
                    print("✅ ЛОГИЧНО: Сильное падение (все закрыто)")
                elif impact < -10:
                    print("⚠️ УМЕРЕННОЕ: Частичное влияние")
                elif impact > 0:
                    print("❓ СТРАННО: Рост в день тишины")
                else:
                    print("➡️ НЕЙТРАЛЬНО: Минимальное влияние")
                
                return impact
        else:
            print("❌ Нет данных по Nyepi в базе")
            
        return None

def main():
    """Запуск анализа реального влияния праздников"""
    analyzer = RealHolidayAnalyzer()
    
    print("🎉 АНАЛИЗ РЕАЛЬНОГО ВЛИЯНИЯ ПРАЗДНИКОВ НА ПРОДАЖИ")
    print("=" * 60)
    print("🎯 ЦЕЛЬ: Получить честные данные из database.sqlite")
    print("❌ БЕЗ ЭМПИРИЧЕСКИХ ПРЕДПОЛОЖЕНИЙ!")
    print()
    
    # Основной анализ
    results = analyzer.analyze_holiday_impact()
    
    # Специальная проверка Nyepi
    nyepi_impact = analyzer.check_nyepi_specifically()
    
    print("\n🏁 ФИНАЛЬНЫЕ ВЫВОДЫ:")
    print("=" * 25)
    print("✅ Анализ основан на реальных данных из database.sqlite")
    print("✅ Учтены только праздники с фактическими продажами")
    print("✅ Исключены эмпирические предположения")
    print()
    print("🎯 Результаты можно использовать в AI помощнике!")

if __name__ == "__main__":
    main()