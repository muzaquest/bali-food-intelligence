#!/usr/bin/env python3
"""
🔍 ПРОФЕССИОНАЛЬНЫЙ ДЕТЕКТИВНЫЙ АНАЛИЗ ПРОДАЖ
═══════════════════════════════════════════════════════════════════════════════
Практический анализ как настоящий аналитик - фокус на конкретных проблемах
"""

import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import requests
import json
import warnings
warnings.filterwarnings('ignore')

class ProfessionalDetectiveAnalysis:
    """Профессиональный детективный анализ причин изменения продаж"""
    
    def __init__(self, db_path='database.sqlite'):
        self.db_path = db_path
        
    def analyze_sales_changes(self, restaurant_name, period1_start, period1_end, 
                            period2_start, period2_end):
        """Анализирует изменения продаж между двумя периодами как настоящий аналитик"""
        
        print(f"\n🔍 АНАЛИЗ ПРИЧИН ИЗМЕНЕНИЯ ПРОДАЖ")
        print("=" * 50)
        
        # 1. Получаем данные по периодам
        period1_data = self._get_period_data(restaurant_name, period1_start, period1_end)
        period2_data = self._get_period_data(restaurant_name, period2_start, period2_end)
        
        if period1_data.empty or period2_data.empty:
            print("❌ Недостаточно данных для анализа")
            return
            
        # 2. Базовое сравнение
        period1_sales = period1_data['total_sales'].sum()
        period2_sales = period2_data['total_sales'].sum()
        change_pct = ((period2_sales - period1_sales) / period1_sales) * 100
        
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
        self._find_problem_days(period1_data, restaurant_name, avg_daily)
        
        # 4. Анализируем основные причины
        self._analyze_main_causes(period1_data, period2_data, restaurant_name)
        
        # 5. Готовим ответ клиенту
        client_answer = self._generate_client_answer(change_pct, period1_data, restaurant_name)
        print(f"\n📞 ГОТОВЫЙ ОТВЕТ КЛИЕНТУ:")
        print("=" * 45)
        print(f'"{client_answer}"')
        print("=" * 45)
        
    def _get_period_data(self, restaurant_name, start_date, end_date):
        """Получает данные за период с основными метриками"""
        conn = sqlite3.connect(self.db_path)
        
        # Получаем ID ресторана
        restaurant_query = "SELECT id FROM restaurants WHERE name = ?"
        restaurant_result = pd.read_sql_query(restaurant_query, conn, params=[restaurant_name])
        
        if restaurant_result.empty:
            conn.close()
            return pd.DataFrame()
            
        restaurant_id = restaurant_result.iloc[0]['id']
        
        # Объединенные данные GRAB + GOJEK
        query = """
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
            COALESCE(g.ads_sales, 0) + COALESCE(gj.ads_sales, 0) as marketing_sales,
            COALESCE(g.cancelled_orders, 0) + COALESCE(gj.cancelled_orders, 0) as cancelled_orders,
            COALESCE(g.store_is_closed, 0) as store_closed,
            COALESCE(g.out_of_stock, 0) as out_of_stock,
            COALESCE(g.new_customers, 0) + COALESCE(gj.new_client, 0) as new_customers,
            COALESCE(g.repeated_customers, 0) + COALESCE(gj.active_client, 0) as repeat_customers,
            CAST(strftime('%w', g.stat_date) AS INTEGER) as day_of_week
        FROM grab_stats g
        LEFT JOIN gojek_stats gj ON g.restaurant_id = gj.restaurant_id 
                                 AND g.stat_date = gj.stat_date
        WHERE g.restaurant_id = ? 
        AND g.stat_date BETWEEN ? AND ?
        ORDER BY g.stat_date
        """
        
        data = pd.read_sql_query(query, conn, params=[restaurant_id, start_date, end_date])
        conn.close()
        
        # Добавляем расчетные поля
        if not data.empty:
            data['avg_order_value'] = data['total_sales'] / data['total_orders'].replace(0, 1)
            data['is_weekend'] = data['day_of_week'].isin([0, 6]).astype(int)  # Воскресенье=0, Суббота=6
            data['roas'] = data['marketing_sales'] / data['marketing_spend'].replace(0, 1)
            
        return data
        
    def _find_problem_days(self, data, restaurant_name, avg_daily):
        """Находит и анализирует проблемные дни"""
        
        # Находим дни со значительно низкими продажами (< 50% от среднего)
        problem_threshold = avg_daily * 0.5
        problem_days = data[data['total_sales'] < problem_threshold].copy()
        
        if problem_days.empty:
            print("✅ Серьезных проблемных дней не обнаружено")
            return
            
        print(f"🚨 ДНИ С НИЗКИМИ ПРОДАЖАМИ:")
        print(f"   Найдено {len(problem_days)} дней с проблемами:")
        print()
        
        for i, (_, day) in enumerate(problem_days.iterrows(), 1):
            date = day['date']
            sales = day['total_sales']
            loss = avg_daily - sales
            loss_pct = ((avg_daily - sales) / avg_daily) * 100
            
            print(f"   {i}. 📅 {date}")
            print(f"      💰 Продажи: {sales:,.0f} IDR (потеря {loss:,.0f} IDR)")
            print(f"      📉 Снижение: {loss_pct:.0f}% от обычного")
            
            # Анализируем причины этого конкретного дня
            causes = self._analyze_single_day_causes(day, date, restaurant_name)
            if causes:
                print(f"      🎯 Причины: {causes}")
            print()
            
    def _analyze_single_day_causes(self, day_data, date, restaurant_name):
        """Анализирует причины плохого дня - как настоящий аналитик"""
        
        causes = []
        
        # 1. Проверяем день недели
        weekday = day_data['day_of_week']
        weekday_names = {0: 'Вс', 1: 'Пн', 2: 'Вт', 3: 'Ср', 4: 'Чт', 5: 'Пт', 6: 'Сб'}
        day_name = weekday_names.get(weekday, 'Неизв.')
        
        if weekday == 1:  # Понедельник
            causes.append(f"Слабый день недели ({day_name})")
            
        # 2. Проверяем операционные проблемы
        if day_data['store_closed'] > 0:
            causes.append("Ресторан был закрыт")
        if day_data['out_of_stock'] > 0:
            causes.append("Дефицит товара")
        if day_data['cancelled_orders'] > 5:
            causes.append(f"Много отмен ({day_data['cancelled_orders']:.0f})")
            
        # 3. Проверяем маркетинг
        if day_data['marketing_spend'] == 0:
            causes.append("Реклама была выключена")
        elif day_data['marketing_spend'] < 50000:
            causes.append("Очень низкий рекламный бюджет")
            
        # 4. Проверяем рейтинг
        if day_data['rating'] < 4.0:
            causes.append(f"Упал рейтинг ({day_data['rating']:.1f})")
            
        # 5. Проверяем погоду (простая проверка)
        weather_cause = self._check_weather_impact(date, restaurant_name)
        if weather_cause:
            causes.append(weather_cause)
            
        # 6. Проверяем праздники
        holiday_cause = self._check_holiday_impact(date)
        if holiday_cause:
            causes.append(holiday_cause)
            
        return ", ".join(causes) if causes else "Причины не ясны - требует дополнительного анализа"
        
    def _check_weather_impact(self, date, restaurant_name):
        """Проверяет влияние погоды на конкретный день"""
        try:
            # Простая проверка через Open-Meteo API
            url = "https://archive-api.open-meteo.com/v1/archive"
            params = {
                'latitude': -8.4095,  # Бали
                'longitude': 115.1889,
                'start_date': date,
                'end_date': date,
                'hourly': 'temperature_2m,precipitation',
                'timezone': 'Asia/Jakarta'
            }
            
            response = requests.get(url, params=params, timeout=5)
            if response.status_code == 200:
                data = response.json()
                hourly = data.get('hourly', {})
                
                if hourly:
                    precipitation = hourly.get('precipitation', [])
                    total_rain = sum(precipitation) if precipitation else 0
                    
                    if total_rain > 10:
                        return "Сильный дождь снизил доставки"
                    elif total_rain > 2:
                        return "Дождь повлиял на доставки"
                        
        except:
            pass
            
        return None
        
    def _check_holiday_impact(self, date):
        """Проверяет влияние праздников"""
        
        # Основные балийские праздники которые влияют на продажи
        balinese_holidays = {
            '2025-04-21': 'Балийский праздник снизил активность',
            '2025-05-12': 'Purnama (полнолуние) - религиозный день',
            '2025-04-16': 'Galungan - семейный праздник',
            '2025-04-26': 'Kuningan - религиозные церемонии'
        }
        
        return balinese_holidays.get(date)
        
    def _analyze_main_causes(self, period1_data, period2_data, restaurant_name):
        """Анализирует главные причины изменений"""
        
        print("🎯 ГЛАВНЫЕ ПРИЧИНЫ ИЗМЕНЕНИЙ:")
        
        # Сравнение ключевых метрик
        p1_avg_marketing = period1_data['marketing_spend'].mean()
        p2_avg_marketing = period2_data['marketing_spend'].mean()
        
        p1_avg_rating = period1_data['rating'].mean()
        p2_avg_rating = period2_data['rating'].mean()
        
        p1_operational_issues = period1_data['store_closed'].sum() + period1_data['out_of_stock'].sum()
        p2_operational_issues = period2_data['store_closed'].sum() + period2_data['out_of_stock'].sum()
        
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
            
        if major_changes:
            for change in major_changes:
                print(f"   ✅ {change}")
        else:
            print("   ✅ Серьезных изменений в факторах не обнаружено")
            
        print()
        
    def _generate_client_answer(self, change_pct, period_data, restaurant_name):
        """Генерирует готовый ответ клиенту"""
        
        if change_pct > 5:
            trend = f"выросли на {change_pct:.1f}%"
        elif change_pct < -5:
            trend = f"снизились на {abs(change_pct):.1f}%"
        else:
            trend = f"остались стабильными ({change_pct:+.1f}%)"
            
        # Определяем главную причину
        avg_marketing = period_data['marketing_spend'].mean()
        operational_issues = period_data['store_closed'].sum() + period_data['out_of_stock'].sum()
        avg_rating = period_data['rating'].mean()
        
        if operational_issues > len(period_data) * 0.1:
            main_cause = "операционных проблем (закрытия, дефицит товара)"
        elif avg_marketing < 50000:
            main_cause = "низкого рекламного бюджета"
        elif avg_rating < 4.0:
            main_cause = "снижения рейтинга"
        else:
            main_cause = "сезонных колебаний рынка"
            
        return f"Продажи {trend} в основном из-за {main_cause}."

def compare_periods(restaurant_name, period1_start, period1_end, period2_start, period2_end):
    """Основная функция для сравнения периодов"""
    
    analyzer = ProfessionalDetectiveAnalysis()
    analyzer.analyze_sales_changes(
        restaurant_name, 
        period1_start, period1_end, 
        period2_start, period2_end
    )

if __name__ == "__main__":
    # Тест на примере Only Eggs
    compare_periods(
        "Only Eggs",
        "2025-04-01", "2025-05-31",  # Анализируемый период
        "2025-01-30", "2025-03-31"   # Предыдущий период для сравнения
    )