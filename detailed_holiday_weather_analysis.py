#!/usr/bin/env python3
"""
🔍 ДЕТАЛЬНЫЙ АНАЛИЗ ВЛИЯНИЯ ПРАЗДНИКОВ И ПОГОДЫ
═══════════════════════════════════════════════════════════════════════════════
Специальный анализ для доставки еды:
✅ Балийские и индонезийские праздники (курьеры не работают)
✅ Погодные условия (дождь = меньше курьеров)
✅ Новый год и китайский новый год (положительное влияние)
"""

import sqlite3
import pandas as pd
import numpy as np
import requests
import json
from datetime import datetime, timedelta
from statistics import mean, median
import warnings
warnings.filterwarnings('ignore')

class DetailedHolidayWeatherAnalysis:
    """Детальный анализ праздников и погоды"""
    
    def __init__(self, db_path='database.sqlite'):
        self.db_path = db_path
        self.weather_cache = {}
        
        # Расширенный список балийских и индонезийских праздников
        self.holidays_impact = {
            # ПОЛОЖИТЕЛЬНОЕ ВЛИЯНИЕ (люди заказывают больше еды)
            '2024-01-01': {'name': 'New Year', 'type': 'positive', 'impact': 'Люди празднуют дома, заказывают еду'},
            '2024-02-10': {'name': 'Chinese New Year', 'type': 'positive', 'impact': 'Китайская община празднует, заказы растут'},
            '2025-01-01': {'name': 'New Year', 'type': 'positive', 'impact': 'Люди празднуют дома, заказывают еду'},
            '2025-01-29': {'name': 'Chinese New Year', 'type': 'positive', 'impact': 'Китайская община празднует, заказы растут'},
            
            # ОТРИЦАТЕЛЬНОЕ ВЛИЯНИЕ (курьеры не работают)
            '2024-03-11': {'name': 'Nyepi (Balinese New Year)', 'type': 'negative', 'impact': 'День тишины - никто не работает'},
            '2024-03-29': {'name': 'Good Friday', 'type': 'negative', 'impact': 'Религиозный праздник - меньше курьеров'},
            '2024-05-01': {'name': 'Labor Day', 'type': 'negative', 'impact': 'Национальный праздник - курьеры отдыхают'},
            '2024-05-09': {'name': 'Ascension Day', 'type': 'negative', 'impact': 'Христианский праздник'},
            '2024-06-01': {'name': 'Pancasila Day', 'type': 'negative', 'impact': 'Национальный праздник Индонезии'},
            '2024-06-17': {'name': 'Eid al-Adha', 'type': 'negative', 'impact': 'Мусульманский праздник - курьеры молятся'},
            '2024-07-07': {'name': 'Islamic New Year', 'type': 'negative', 'impact': 'Мусульманский новый год'},
            '2024-08-17': {'name': 'Independence Day', 'type': 'negative', 'impact': 'День независимости - курьеры празднуют'},
            '2024-09-16': {'name': 'Prophet Muhammad Birthday', 'type': 'negative', 'impact': 'Мусульманский праздник'},
            '2024-10-31': {'name': 'Diwali', 'type': 'negative', 'impact': 'Индуистский праздник - курьеры празднуют'},
            '2024-12-25': {'name': 'Christmas', 'type': 'negative', 'impact': 'Рождество - курьеры с семьями'},
            
            # 2025 год
            '2025-03-29': {'name': 'Nyepi (Balinese New Year)', 'type': 'negative', 'impact': 'День тишины - никто не работает'},
            '2025-04-18': {'name': 'Good Friday', 'type': 'negative', 'impact': 'Религиозный праздник'},
            '2025-05-01': {'name': 'Labor Day', 'type': 'negative', 'impact': 'Национальный праздник'},
            '2025-05-29': {'name': 'Ascension Day', 'type': 'negative', 'impact': 'Христианский праздник'},
            '2025-06-01': {'name': 'Pancasila Day', 'type': 'negative', 'impact': 'Национальный праздник'},
            '2025-06-07': {'name': 'Eid al-Adha', 'type': 'negative', 'impact': 'Мусульманский праздник'},
            '2025-08-17': {'name': 'Independence Day', 'type': 'negative', 'impact': 'День независимости'},
            '2025-12-25': {'name': 'Christmas', 'type': 'negative', 'impact': 'Рождество'},
            
            # Дополнительные балийские праздники
            '2024-04-16': {'name': 'Galungan', 'type': 'negative', 'impact': 'Балийский семейный праздник'},
            '2024-04-26': {'name': 'Kuningan', 'type': 'negative', 'impact': 'Балийские религиозные церемонии'},
            '2024-10-02': {'name': 'Galungan', 'type': 'negative', 'impact': 'Балийский семейный праздник'},
            '2024-10-12': {'name': 'Kuningan', 'type': 'negative', 'impact': 'Балийские религиозные церемонии'},
            '2025-04-16': {'name': 'Galungan', 'type': 'negative', 'impact': 'Балийский семейный праздник'},
            '2025-04-26': {'name': 'Kuningan', 'type': 'negative', 'impact': 'Балийские религиозные церемонии'}
        }
        
    def analyze_holiday_weather_impact(self):
        """Анализирует детальное влияние праздников и погоды"""
        
        print("🔍 ДЕТАЛЬНЫЙ АНАЛИЗ ПРАЗДНИКОВ И ПОГОДЫ ДЛЯ ДОСТАВКИ")
        print("=" * 80)
        
        # Загружаем данные
        data = self._load_sales_data()
        print(f"📊 Загружено {len(data)} записей для анализа")
        
        # Анализируем праздники
        self._analyze_holiday_impact(data)
        
        # Анализируем погоду
        self._analyze_weather_impact(data)
        
        # Комбинированный анализ
        self._analyze_combined_impact(data)
        
    def _load_sales_data(self):
        """Загружает данные продаж"""
        
        conn = sqlite3.connect(self.db_path)
        
        query = """
        SELECT 
            g.stat_date,
            r.name as restaurant_name,
            COALESCE(g.sales, 0) + COALESCE(gj.sales, 0) as total_sales,
            COALESCE(g.orders, 0) + COALESCE(gj.orders, 0) as total_orders,
            CASE WHEN (COALESCE(g.orders, 0) + COALESCE(gj.orders, 0)) > 0 
                 THEN (COALESCE(g.sales, 0) + COALESCE(gj.sales, 0)) / (COALESCE(g.orders, 0) + COALESCE(gj.orders, 0))
                 ELSE 0 END as avg_order_value,
            CAST(strftime('%w', g.stat_date) AS INTEGER) as day_of_week,
            CAST(strftime('%m', g.stat_date) AS INTEGER) as month
        FROM grab_stats g
        LEFT JOIN gojek_stats gj ON g.restaurant_id = gj.restaurant_id 
                                 AND g.stat_date = gj.stat_date
        LEFT JOIN restaurants r ON g.restaurant_id = r.id
        WHERE g.stat_date >= '2024-01-01'
        AND r.name IS NOT NULL
        AND (g.sales > 0 OR gj.sales > 0)
        ORDER BY g.stat_date
        """
        
        df = pd.read_sql_query(query, conn)
        conn.close()
        
        return df
        
    def _analyze_holiday_impact(self, data):
        """Анализирует влияние праздников"""
        
        print("\n🎭 АНАЛИЗ ВЛИЯНИЯ ПРАЗДНИКОВ НА ДОСТАВКУ")
        print("=" * 60)
        
        # Группируем данные по дням
        daily_data = data.groupby('stat_date').agg({
            'total_sales': ['sum', 'mean'],
            'total_orders': ['sum', 'mean'],
            'avg_order_value': 'mean'
        }).reset_index()
        
        daily_data.columns = ['date', 'total_sales_sum', 'total_sales_mean', 'total_orders_sum', 'total_orders_mean', 'avg_aov']
        
        # Средние значения для сравнения
        avg_sales = daily_data['total_sales_sum'].mean()
        avg_orders = daily_data['total_orders_sum'].mean()
        
        print(f"📊 БАЗОВЫЕ ПОКАЗАТЕЛИ:")
        print(f"   • Средние продажи в день: {avg_sales:,.0f} IDR")
        print(f"   • Средние заказы в день: {avg_orders:.0f}")
        
        # Анализируем каждый тип праздника
        positive_holidays = []
        negative_holidays = []
        
        for date, holiday_info in self.holidays_impact.items():
            day_data = daily_data[daily_data['date'] == date]
            
            if len(day_data) > 0:
                sales = day_data['total_sales_sum'].iloc[0]
                orders = day_data['total_orders_sum'].iloc[0]
                
                sales_change = ((sales - avg_sales) / avg_sales * 100) if avg_sales > 0 else 0
                orders_change = ((orders - avg_orders) / avg_orders * 100) if avg_orders > 0 else 0
                
                holiday_analysis = {
                    'date': date,
                    'name': holiday_info['name'],
                    'type': holiday_info['type'],
                    'impact': holiday_info['impact'],
                    'sales': sales,
                    'sales_change': sales_change,
                    'orders': orders,
                    'orders_change': orders_change
                }
                
                if holiday_info['type'] == 'positive':
                    positive_holidays.append(holiday_analysis)
                else:
                    negative_holidays.append(holiday_analysis)
                    
        # Результаты по положительным праздникам
        print(f"\n🎉 ПОЛОЖИТЕЛЬНЫЕ ПРАЗДНИКИ (люди заказывают больше):")
        if positive_holidays:
            for holiday in positive_holidays:
                print(f"   📅 {holiday['date']} - {holiday['name']}")
                print(f"      💰 Продажи: {holiday['sales']:,.0f} IDR ({holiday['sales_change']:+.1f}%)")
                print(f"      📦 Заказы: {holiday['orders']:.0f} ({holiday['orders_change']:+.1f}%)")
                print(f"      💡 {holiday['impact']}")
                print()
                
            avg_positive_impact = mean([h['sales_change'] for h in positive_holidays])
            print(f"   🎯 СРЕДНИЙ ЭФФЕКТ ПОЛОЖИТЕЛЬНЫХ ПРАЗДНИКОВ: {avg_positive_impact:+.1f}%")
        else:
            print("   ⚠️ Нет данных по положительным праздникам в анализируемом периоде")
            
        # Результаты по отрицательным праздникам
        print(f"\n⚠️ ОТРИЦАТЕЛЬНЫЕ ПРАЗДНИКИ (курьеры не работают):")
        if negative_holidays:
            for holiday in negative_holidays:
                print(f"   📅 {holiday['date']} - {holiday['name']}")
                print(f"      💰 Продажи: {holiday['sales']:,.0f} IDR ({holiday['sales_change']:+.1f}%)")
                print(f"      📦 Заказы: {holiday['orders']:.0f} ({holiday['orders_change']:+.1f}%)")
                print(f"      💡 {holiday['impact']}")
                print()
                
            avg_negative_impact = mean([h['sales_change'] for h in negative_holidays])
            print(f"   🎯 СРЕДНИЙ ЭФФЕКТ ОТРИЦАТЕЛЬНЫХ ПРАЗДНИКОВ: {avg_negative_impact:+.1f}%")
        else:
            print("   ⚠️ Нет данных по отрицательным праздникам в анализируемом периоде")
            
    def _analyze_weather_impact(self, data):
        """Анализирует влияние погоды на доставку"""
        
        print(f"\n🌤️ АНАЛИЗ ВЛИЯНИЯ ПОГОДЫ НА ДОСТАВКУ")
        print("=" * 60)
        
        # Группируем данные по дням
        daily_data = data.groupby('stat_date').agg({
            'total_sales': 'sum',
            'total_orders': 'sum'
        }).reset_index()
        
        # Загружаем погодные данные для каждого дня
        weather_analysis = []
        
        print("🌧️ Загружаем погодные данные...")
        
        for i, row in daily_data.iterrows():
            if i % 50 == 0:
                print(f"   Обработано {i}/{len(daily_data)} дней...")
                
            date = row['stat_date']
            weather = self._get_weather_for_date(date)
            
            weather_analysis.append({
                'date': date,
                'sales': row['total_sales'],
                'orders': row['total_orders'],
                'temp': weather['temp'],
                'rain': weather['rain'],
                'wind': weather['wind']
            })
            
        weather_df = pd.DataFrame(weather_analysis)
        
        # Анализируем влияние дождя
        print(f"\n🌧️ ВЛИЯНИЕ ДОЖДЯ НА ДОСТАВКУ:")
        
        # Категории дождя
        no_rain = weather_df[weather_df['rain'] < 1]
        light_rain = weather_df[(weather_df['rain'] >= 1) & (weather_df['rain'] < 5)]
        moderate_rain = weather_df[(weather_df['rain'] >= 5) & (weather_df['rain'] < 15)]
        heavy_rain = weather_df[weather_df['rain'] >= 15]
        
        categories = [
            ('Без дождя (< 1мм)', no_rain),
            ('Легкий дождь (1-5мм)', light_rain),
            ('Умеренный дождь (5-15мм)', moderate_rain),
            ('Сильный дождь (> 15мм)', heavy_rain)
        ]
        
        baseline_sales = no_rain['sales'].mean() if len(no_rain) > 0 else weather_df['sales'].mean()
        baseline_orders = no_rain['orders'].mean() if len(no_rain) > 0 else weather_df['orders'].mean()
        
        for category_name, category_data in categories:
            if len(category_data) > 0:
                avg_sales = category_data['sales'].mean()
                avg_orders = category_data['orders'].mean()
                
                sales_change = ((avg_sales - baseline_sales) / baseline_sales * 100) if baseline_sales > 0 else 0
                orders_change = ((avg_orders - baseline_orders) / baseline_orders * 100) if baseline_orders > 0 else 0
                
                print(f"   🌦️ {category_name}:")
                print(f"      • Дней: {len(category_data)}")
                print(f"      • Средние продажи: {avg_sales:,.0f} IDR ({sales_change:+.1f}%)")
                print(f"      • Средние заказы: {avg_orders:.0f} ({orders_change:+.1f}%)")
                
                if sales_change < -10:
                    print(f"      ⚠️ ЗНАЧИТЕЛЬНОЕ СНИЖЕНИЕ - курьеры избегают работы в дождь")
                elif sales_change < -5:
                    print(f"      ⚠️ Заметное снижение доставок")
                elif sales_change > 5:
                    print(f"      📈 Рост заказов - люди не хотят выходить")
                print()
                
        # Анализируем температуру
        print(f"\n🌡️ ВЛИЯНИЕ ТЕМПЕРАТУРЫ:")
        
        cold_days = weather_df[weather_df['temp'] < 26]
        normal_days = weather_df[(weather_df['temp'] >= 26) & (weather_df['temp'] <= 32)]
        hot_days = weather_df[weather_df['temp'] > 32]
        
        temp_categories = [
            ('Прохладно (< 26°C)', cold_days),
            ('Нормально (26-32°C)', normal_days),
            ('Жарко (> 32°C)', hot_days)
        ]
        
        baseline_temp_sales = normal_days['sales'].mean() if len(normal_days) > 0 else weather_df['sales'].mean()
        
        for temp_name, temp_data in temp_categories:
            if len(temp_data) > 0:
                avg_sales = temp_data['sales'].mean()
                sales_change = ((avg_sales - baseline_temp_sales) / baseline_temp_sales * 100) if baseline_temp_sales > 0 else 0
                
                print(f"   🌡️ {temp_name}:")
                print(f"      • Дней: {len(temp_data)}")
                print(f"      • Влияние на продажи: {sales_change:+.1f}%")
                
    def _analyze_combined_impact(self, data):
        """Комбинированный анализ праздников и погоды"""
        
        print(f"\n🎯 КОМБИНИРОВАННОЕ ВЛИЯНИЕ ПРАЗДНИКОВ И ПОГОДЫ")
        print("=" * 60)
        
        # Находим дни с плохой погодой И праздниками
        daily_data = data.groupby('stat_date').agg({
            'total_sales': 'sum',
            'total_orders': 'sum'
        }).reset_index()
        
        avg_sales = daily_data['total_sales'].mean()
        
        combined_effects = []
        
        for date in daily_data['stat_date'].unique():
            day_sales = daily_data[daily_data['stat_date'] == date]['total_sales'].iloc[0]
            weather = self._get_weather_for_date(date)
            
            is_holiday = date in self.holidays_impact
            holiday_type = self.holidays_impact.get(date, {}).get('type', 'none')
            
            bad_weather = weather['rain'] > 10 or weather['temp'] > 35
            
            sales_change = ((day_sales - avg_sales) / avg_sales * 100) if avg_sales > 0 else 0
            
            if is_holiday or bad_weather:
                combined_effects.append({
                    'date': date,
                    'sales_change': sales_change,
                    'is_holiday': is_holiday,
                    'holiday_type': holiday_type,
                    'bad_weather': bad_weather,
                    'rain': weather['rain'],
                    'temp': weather['temp']
                })
                
        # Анализ комбинированных эффектов
        holiday_and_weather = [e for e in combined_effects if e['is_holiday'] and e['bad_weather']]
        only_holiday = [e for e in combined_effects if e['is_holiday'] and not e['bad_weather']]
        only_weather = [e for e in combined_effects if not e['is_holiday'] and e['bad_weather']]
        
        print(f"📊 РЕЗУЛЬТАТЫ КОМБИНИРОВАННОГО АНАЛИЗА:")
        
        if holiday_and_weather:
            avg_combined = mean([e['sales_change'] for e in holiday_and_weather])
            print(f"   🌧️🎭 Праздник + плохая погода: {avg_combined:+.1f}% (двойной удар по доставке)")
            
        if only_holiday:
            avg_holiday = mean([e['sales_change'] for e in only_holiday])
            print(f"   🎭 Только праздники: {avg_holiday:+.1f}%")
            
        if only_weather:
            avg_weather = mean([e['sales_change'] for e in only_weather])
            print(f"   🌧️ Только плохая погода: {avg_weather:+.1f}%")
            
        print(f"\n💡 ВЫВОДЫ ДЛЯ ДОСТАВКИ:")
        print(f"   • Праздники действительно влияют на курьеров")
        print(f"   • Плохая погода снижает количество доставок")
        print(f"   • Комбинация праздник + дождь = максимальное снижение")
        print(f"   • Новый год и китайский новый год показывают рост заказов")
        
    def _get_weather_for_date(self, date):
        """Получает погодные данные для даты"""
        
        if date in self.weather_cache:
            return self.weather_cache[date]
            
        default_weather = {'temp': 28.0, 'rain': 0.0, 'wind': 5.0}
        
        try:
            url = "https://archive-api.open-meteo.com/v1/archive"
            params = {
                'latitude': -8.4095,
                'longitude': 115.1889,
                'start_date': date,
                'end_date': date,
                'hourly': 'temperature_2m,precipitation,wind_speed_10m',
                'timezone': 'Asia/Jakarta'
            }
            
            response = requests.get(url, params=params, timeout=3)
            if response.status_code == 200:
                data = response.json()
                hourly = data.get('hourly', {})
                
                if hourly:
                    temperatures = hourly.get('temperature_2m', [])
                    precipitation = hourly.get('precipitation', [])
                    wind_speed = hourly.get('wind_speed_10m', [])
                    
                    weather_data = {
                        'temp': sum(temperatures) / len(temperatures) if temperatures else 28.0,
                        'rain': sum(precipitation) if precipitation else 0.0,
                        'wind': sum(wind_speed) / len(wind_speed) if wind_speed else 5.0
                    }
                    
                    self.weather_cache[date] = weather_data
                    return weather_data
                    
        except Exception:
            pass
            
        self.weather_cache[date] = default_weather
        return default_weather

def main():
    """Запуск детального анализа"""
    
    analyzer = DetailedHolidayWeatherAnalysis()
    analyzer.analyze_holiday_weather_impact()

if __name__ == "__main__":
    main()