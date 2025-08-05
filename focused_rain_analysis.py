#!/usr/bin/env python3
"""
🌧️ ФОКУСИРОВАННЫЙ АНАЛИЗ ВЛИЯНИЯ ДОЖДЯ
═══════════════════════════════════════════════════════════════════════════════
✅ Выборочный анализ по ключевым дням и ресторанам
✅ Проверка влияния сильного дождя на продажи
✅ Быстрая проверка гипотезы клиента
"""

import sqlite3
import pandas as pd
import requests
import json
from datetime import datetime
from statistics import mean
import warnings
warnings.filterwarnings('ignore')

class FocusedRainAnalysis:
    """Фокусированный анализ дождя"""
    
    def __init__(self, db_path='database.sqlite'):
        self.db_path = db_path
        self.weather_cache = {}
        
    def analyze_rain_impact_focused(self):
        """Фокусированный анализ влияния дождя"""
        
        print("🌧️ ФОКУСИРОВАННЫЙ АНАЛИЗ ВЛИЯНИЯ ДОЖДЯ НА ДОСТАВКУ")
        print("=" * 80)
        
        # 1. Загружаем данные по топ ресторанам
        top_restaurants = self._get_top_restaurants()
        print(f"🏪 Анализируем {len(top_restaurants)} топ-ресторанов")
        
        # 2. Загружаем данные продаж за последние месяцы
        sales_data = self._load_recent_sales_data(top_restaurants)
        print(f"📊 Загружено {len(sales_data)} записей продаж")
        
        # 3. Анализируем погоду выборочно
        weather_sales_data = self._analyze_weather_sample(sales_data)
        
        # 4. Проверяем влияние дождя
        self._check_rain_impact(weather_sales_data)
        
        # 5. Ищем конкретные примеры
        self._find_specific_examples(weather_sales_data)
        
    def _get_top_restaurants(self):
        """Получает топ рестораны по продажам"""
        
        conn = sqlite3.connect(self.db_path)
        
        query = """
        SELECT 
            r.id,
            r.name,
            SUM(COALESCE(g.sales, 0) + COALESCE(gj.sales, 0)) as total_sales
        FROM restaurants r
        LEFT JOIN grab_stats g ON r.id = g.restaurant_id
        LEFT JOIN gojek_stats gj ON r.id = gj.restaurant_id AND g.stat_date = gj.stat_date
        WHERE g.stat_date >= '2024-01-01'
        GROUP BY r.id, r.name
        HAVING total_sales > 0
        ORDER BY total_sales DESC
        LIMIT 15
        """
        
        df = pd.read_sql_query(query, conn)
        conn.close()
        
        return df
        
    def _load_recent_sales_data(self, top_restaurants):
        """Загружает данные продаж за последние месяцы"""
        
        conn = sqlite3.connect(self.db_path)
        
        restaurant_ids = tuple(top_restaurants['id'].tolist())
        
        query = f"""
        SELECT 
            g.stat_date,
            r.name as restaurant_name,
            COALESCE(g.sales, 0) + COALESCE(gj.sales, 0) as total_sales,
            COALESCE(g.orders, 0) + COALESCE(gj.orders, 0) as total_orders,
            COALESCE(g.sales, 0) as grab_sales,
            COALESCE(gj.sales, 0) as gojek_sales
        FROM grab_stats g
        LEFT JOIN gojek_stats gj ON g.restaurant_id = gj.restaurant_id 
                                 AND g.stat_date = gj.stat_date
        LEFT JOIN restaurants r ON g.restaurant_id = r.id
        WHERE g.restaurant_id IN {restaurant_ids}
        AND g.stat_date >= '2024-03-01'
        AND (g.sales > 0 OR gj.sales > 0)
        ORDER BY g.stat_date DESC, r.name
        """
        
        df = pd.read_sql_query(query, conn)
        conn.close()
        
        return df
        
    def _analyze_weather_sample(self, sales_data):
        """Анализирует погоду для выборки данных"""
        
        print(f"\n🌤️ АНАЛИЗ ПОГОДЫ ДЛЯ ВЫБОРКИ")
        print("-" * 50)
        
        # Берем каждый 5-й день для анализа (чтобы ускорить)
        unique_dates = sorted(sales_data['stat_date'].unique())
        sample_dates = unique_dates[::5]  # каждый 5-й день
        
        print(f"🗓️ Анализируем {len(sample_dates)} дат из {len(unique_dates)}")
        
        weather_sales_data = []
        
        for i, date in enumerate(sample_dates):
            if i % 10 == 0:
                print(f"   Обработано {i}/{len(sample_dates)} дат...")
                
            # Получаем погоду для центра Бали
            weather = self._get_weather_for_date(date, -8.4095, 115.1889)
            
            # Получаем данные продаж для этой даты
            day_sales = sales_data[sales_data['stat_date'] == date]
            
            if len(day_sales) > 0:
                total_sales = day_sales['total_sales'].sum()
                total_orders = day_sales['total_orders'].sum()
                restaurant_count = len(day_sales)
                
                weather_sales_data.append({
                    'date': date,
                    'total_sales': total_sales,
                    'total_orders': total_orders,
                    'restaurant_count': restaurant_count,
                    'avg_sales_per_restaurant': total_sales / restaurant_count if restaurant_count > 0 else 0,
                    'temperature': weather['temp'],
                    'precipitation': weather['rain'],
                    'wind_speed': weather['wind'],
                    'is_heavy_rain': weather['rain'] > 15,
                    'is_extreme_rain': weather['rain'] > 30
                })
                
        return pd.DataFrame(weather_sales_data)
        
    def _check_rain_impact(self, data):
        """Проверяет влияние дождя на продажи"""
        
        print(f"\n🌧️ ПРОВЕРКА ВЛИЯНИЯ ДОЖДЯ")
        print("-" * 50)
        
        if len(data) == 0:
            print("❌ Нет данных для анализа")
            return
            
        # Категоризируем дни по количеству дождя
        no_rain = data[data['precipitation'] < 1]
        light_rain = data[(data['precipitation'] >= 1) & (data['precipitation'] < 10)]
        moderate_rain = data[(data['precipitation'] >= 10) & (data['precipitation'] < 20)]
        heavy_rain = data[data['precipitation'] >= 20]
        
        categories = [
            ('Без дождя (< 1мм)', no_rain),
            ('Легкий дождь (1-10мм)', light_rain),
            ('Умеренный дождь (10-20мм)', moderate_rain),
            ('Сильный дождь (≥ 20мм)', heavy_rain)
        ]
        
        print(f"📊 АНАЛИЗ ПО КАТЕГОРИЯМ ДОЖДЯ:")
        
        baseline_sales = no_rain['avg_sales_per_restaurant'].mean() if len(no_rain) > 0 else data['avg_sales_per_restaurant'].mean()
        
        for category_name, category_data in categories:
            if len(category_data) > 0:
                avg_sales = category_data['avg_sales_per_restaurant'].mean()
                avg_orders = category_data['total_orders'].mean()
                days_count = len(category_data)
                
                sales_change = ((avg_sales - baseline_sales) / baseline_sales * 100) if baseline_sales > 0 else 0
                
                print(f"   🌦️ {category_name}:")
                print(f"      • Дней в выборке: {days_count}")
                print(f"      • Средние продажи на ресторан: {avg_sales:,.0f} IDR")
                print(f"      • Изменение от базовой линии: {sales_change:+.1f}%")
                
                if sales_change < -15:
                    print(f"      ⚠️ ЗНАЧИТЕЛЬНОЕ СНИЖЕНИЕ - подтверждает слова клиента!")
                elif sales_change > 15:
                    print(f"      📈 РОСТ - люди не выходят, заказывают больше")
                else:
                    print(f"      ➡️ Умеренное влияние")
                print()
                
        # Специальный анализ экстремального дождя
        extreme_rain = data[data['precipitation'] > 30]
        if len(extreme_rain) > 0:
            print(f"⛈️ ЭКСТРЕМАЛЬНЫЙ ДОЖДЬ (> 30мм):")
            print(f"   • Найдено {len(extreme_rain)} дней")
            
            extreme_avg_sales = extreme_rain['avg_sales_per_restaurant'].mean()
            extreme_change = ((extreme_avg_sales - baseline_sales) / baseline_sales * 100) if baseline_sales > 0 else 0
            
            print(f"   • Средние продажи: {extreme_avg_sales:,.0f} IDR")
            print(f"   • Изменение: {extreme_change:+.1f}%")
            
            if extreme_change < -30:
                print(f"   ⚠️ КРИТИЧЕСКОЕ СНИЖЕНИЕ - курьеры боятся ездить!")
                
    def _find_specific_examples(self, data):
        """Находит конкретные примеры дней с сильным дождем"""
        
        print(f"\n🔍 КОНКРЕТНЫЕ ПРИМЕРЫ ДНЕЙ С СИЛЬНЫМ ДОЖДЕМ")
        print("-" * 50)
        
        # Находим дни с самым сильным дождем
        heavy_rain_days = data[data['precipitation'] > 15].sort_values('precipitation', ascending=False)
        
        if len(heavy_rain_days) == 0:
            print("❌ В выборке не найдено дней с сильным дождем > 15мм")
            
            # Показываем дни с максимальным дождем из имеющихся
            max_rain_days = data.nlargest(5, 'precipitation')
            print(f"🌧️ ТОП-5 САМЫХ ДОЖДЛИВЫХ ДНЕЙ В ВЫБОРКЕ:")
            
            for _, day in max_rain_days.iterrows():
                print(f"   📅 {day['date']}")
                print(f"      🌧️ Дождь: {day['precipitation']:.1f}мм")
                print(f"      💰 Средние продажи на ресторан: {day['avg_sales_per_restaurant']:,.0f} IDR")
                print(f"      📦 Общие заказы: {day['total_orders']:.0f}")
                print()
        else:
            print(f"🌧️ НАЙДЕНО {len(heavy_rain_days)} ДНЕЙ С СИЛЬНЫМ ДОЖДЕМ:")
            
            for _, day in heavy_rain_days.head(10).iterrows():
                print(f"   📅 {day['date']}")
                print(f"      🌧️ Дождь: {day['precipitation']:.1f}мм")
                print(f"      🌡️ Температура: {day['temperature']:.1f}°C")
                print(f"      💨 Ветер: {day['wind_speed']:.1f} м/с")
                print(f"      💰 Средние продажи на ресторан: {day['avg_sales_per_restaurant']:,.0f} IDR")
                print(f"      📦 Общие заказы: {day['total_orders']:.0f}")
                
                # Сравниваем с базовой линией
                baseline = data[data['precipitation'] < 5]['avg_sales_per_restaurant'].mean()
                if baseline > 0:
                    change = ((day['avg_sales_per_restaurant'] - baseline) / baseline * 100)
                    print(f"      📊 Изменение от обычного дня: {change:+.1f}%")
                print()
                
        # Проверяем корреляцию
        if len(data) > 10:
            correlation = data['precipitation'].corr(data['avg_sales_per_restaurant'])
            print(f"📈 КОРРЕЛЯЦИЯ ДОЖДЬ-ПРОДАЖИ: {correlation:.3f}")
            
            if correlation < -0.3:
                print(f"   ⚠️ СИЛЬНАЯ ОТРИЦАТЕЛЬНАЯ КОРРЕЛЯЦИЯ - клиент прав!")
            elif correlation > 0.3:
                print(f"   📈 ПОЛОЖИТЕЛЬНАЯ КОРРЕЛЯЦИЯ - люди заказывают больше в дождь")
            else:
                print(f"   ➡️ Слабая корреляция")
                
    def _get_weather_for_date(self, date, lat, lon):
        """Получает погоду для даты и координат"""
        
        cache_key = f"{lat}_{lon}_{date}"
        
        if cache_key in self.weather_cache:
            return self.weather_cache[cache_key]
            
        default_weather = {'temp': 28.0, 'rain': 0.0, 'wind': 5.0}
        
        try:
            url = "https://archive-api.open-meteo.com/v1/archive"
            params = {
                'latitude': lat,
                'longitude': lon,
                'start_date': date,
                'end_date': date,
                'hourly': 'temperature_2m,precipitation,wind_speed_10m',
                'timezone': 'Asia/Jakarta'
            }
            
            response = requests.get(url, params=params, timeout=5)
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
                    
                    self.weather_cache[cache_key] = weather_data
                    return weather_data
                    
        except Exception:
            pass
            
        self.weather_cache[cache_key] = default_weather
        return default_weather

def main():
    """Запуск фокусированного анализа"""
    
    analyzer = FocusedRainAnalysis()
    analyzer.analyze_rain_impact_focused()

if __name__ == "__main__":
    main()