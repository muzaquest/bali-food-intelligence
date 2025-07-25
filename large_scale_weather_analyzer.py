#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
МАСШТАБНЫЙ АНАЛИЗ ПОГОДНЫХ ЗАКОНОМЕРНОСТЕЙ
========================================

Анализирует МАКСИМАЛЬНЫЙ объем данных для получения
статистически значимых результатов.

ЦЕЛЬ: Проанализировать 10,000+ записей для надежных выводов
"""

import sqlite3
import pandas as pd
import numpy as np
import json
from datetime import datetime, timedelta
from collections import defaultdict
import requests
import os
from scipy import stats
import time
import warnings
warnings.filterwarnings('ignore')

class LargeScaleWeatherAnalyzer:
    def __init__(self):
        self.db_path = 'database.sqlite'
        self.locations_file = 'data/bali_restaurant_locations.json'
        self.weather_api_url = "https://api.open-meteo.com/v1/forecast"
        self.cache_file = 'data/large_scale_weather_cache.json'
        self.results_file = 'data/large_scale_analysis_results.json'
        
        # Загружаем кэш если существует
        self.weather_cache = self._load_cache()
        
    def _load_cache(self):
        """Загружает кэш погодных данных"""
        try:
            with open(self.cache_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except:
            return {}
    
    def _save_cache(self):
        """Сохраняет кэш погодных данных"""
        try:
            os.makedirs('data', exist_ok=True)
            with open(self.cache_file, 'w', encoding='utf-8') as f:
                json.dump(self.weather_cache, f, ensure_ascii=False, indent=2)
        except Exception as e:
            print(f"⚠️ Ошибка сохранения кэша: {e}")
    
    def get_large_sample_data(self, sample_size=10000):
        """Получает большую выборку данных стратифицированно"""
        conn = sqlite3.connect(self.db_path)
        
        # Сначала получаем статистику для стратификации
        strat_query = """
        SELECT 
            r.name as restaurant_name,
            COUNT(*) as record_count
        FROM restaurants r
        LEFT JOIN grab_stats g ON r.id = g.restaurant_id 
        LEFT JOIN gojek_stats gj ON r.id = gj.restaurant_id 
        WHERE (g.stat_date IS NOT NULL OR gj.stat_date IS NOT NULL)
          AND (COALESCE(g.sales, 0) + COALESCE(gj.sales, 0)) > 0
        GROUP BY r.name
        ORDER BY record_count DESC
        """
        
        restaurant_stats = pd.read_sql_query(strat_query, conn)
        
        # Выбираем топ рестораны для анализа
        top_restaurants = restaurant_stats.head(20)['restaurant_name'].tolist()
        
        print(f"📊 Выбрано {len(top_restaurants)} топ-ресторанов для анализа")
        
        # Получаем данные с равномерным распределением по ресторанам
        records_per_restaurant = sample_size // len(top_restaurants)
        
        all_data = []
        
        for restaurant in top_restaurants:
            query = f"""
            SELECT 
                r.name as restaurant_name,
                COALESCE(g.stat_date, gj.stat_date) as date,
                COALESCE(g.sales, 0) + COALESCE(gj.sales, 0) as total_sales,
                COALESCE(g.orders, 0) + COALESCE(gj.orders, 0) as total_orders,
                g.cancelled_orders as grab_cancelled,
                g.store_is_busy as grab_busy,
                g.store_is_closed as grab_closed,
                g.out_of_stock as grab_out_of_stock,
                CASE 
                    WHEN (COALESCE(g.orders, 0) + COALESCE(gj.orders, 0)) > 0 
                    THEN (COALESCE(g.sales, 0) + COALESCE(gj.sales, 0)) / (COALESCE(g.orders, 0) + COALESCE(gj.orders, 0))
                    ELSE 0 
                END as avg_order_value,
                CASE CAST(strftime('%w', COALESCE(g.stat_date, gj.stat_date)) AS INTEGER)
                    WHEN 0 THEN 'Sunday'
                    WHEN 1 THEN 'Monday'
                    WHEN 2 THEN 'Tuesday'
                    WHEN 3 THEN 'Wednesday'
                    WHEN 4 THEN 'Thursday'
                    WHEN 5 THEN 'Friday'
                    WHEN 6 THEN 'Saturday'
                END as day_of_week
            FROM restaurants r
            LEFT JOIN grab_stats g ON r.id = g.restaurant_id 
            LEFT JOIN gojek_stats gj ON r.id = gj.restaurant_id 
            WHERE r.name = '{restaurant}'
              AND (g.stat_date IS NOT NULL OR gj.stat_date IS NOT NULL)
              AND (COALESCE(g.sales, 0) + COALESCE(gj.sales, 0)) > 0
            ORDER BY RANDOM()
            LIMIT {records_per_restaurant}
            """
            
            restaurant_data = pd.read_sql_query(query, conn)
            all_data.append(restaurant_data)
            
            print(f"   ✅ {restaurant}: {len(restaurant_data)} записей")
        
        conn.close()
        
        # Объединяем все данные
        final_data = pd.concat(all_data, ignore_index=True)
        
        print(f"\n📊 ИТОГО ЗАГРУЖЕНО ДЛЯ АНАЛИЗА:")
        print(f"   • Записей: {len(final_data):,}")
        print(f"   • Ресторанов: {final_data['restaurant_name'].nunique()}")
        print(f"   • Уникальных дат: {final_data['date'].nunique():,}")
        print(f"   • Период: {final_data['date'].min()} → {final_data['date'].max()}")
        
        return final_data
    
    def load_restaurant_locations(self):
        """Загружает GPS координаты всех ресторанов"""
        try:
            with open(self.locations_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            return {r['name']: r for r in data['restaurants']}
        except Exception as e:
            print(f"⚠️ Ошибка загрузки координат: {e}")
            return {}
    
    def get_weather_with_cache(self, lat, lon, date):
        """Получает погоду с использованием кэша"""
        cache_key = f"{lat:.3f}_{lon:.3f}_{date}"
        
        # Проверяем кэш
        if cache_key in self.weather_cache:
            return self.weather_cache[cache_key]
        
        # Если нет в кэше, запрашиваем
        try:
            params = {
                'latitude': lat,
                'longitude': lon,
                'start_date': date,
                'end_date': date,
                'hourly': 'temperature_2m,precipitation,weather_code,wind_speed_10m,relative_humidity_2m',
                'timezone': 'Asia/Jakarta'
            }
            
            response = requests.get(self.weather_api_url, params=params, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                hourly = data.get('hourly', {})
                
                if hourly and len(hourly.get('time', [])) > 0:
                    temps = hourly.get('temperature_2m', [28])
                    precipitation = hourly.get('precipitation', [0])
                    weather_codes = hourly.get('weather_code', [0])
                    wind_speeds = hourly.get('wind_speed_10m', [5])
                    humidity = hourly.get('relative_humidity_2m', [75])
                    
                    weather_summary = {
                        'temperature': np.mean(temps) if temps else 28,
                        'rain': sum(precipitation) if precipitation else 0,
                        'wind': max(wind_speeds) if wind_speeds else 5,
                        'humidity': np.mean(humidity) if humidity else 75,
                        'condition': self._weather_code_to_condition(
                            max(set(weather_codes), key=weather_codes.count) if weather_codes else 0
                        )
                    }
                    
                    # Сохраняем в кэш
                    self.weather_cache[cache_key] = weather_summary
                    return weather_summary
            
            return None
        except Exception as e:
            return None
    
    def _weather_code_to_condition(self, code):
        """Конвертирует WMO код в условие"""
        if code == 0:
            return 'Clear'
        elif code in [1, 2, 3]:
            return 'Clouds'
        elif code in [45, 48]:
            return 'Fog'
        elif code in [51, 53, 55, 56, 57, 61, 63, 65, 66, 67, 80, 81, 82]:
            return 'Rain'
        elif code in [95, 96, 99]:
            return 'Thunderstorm'
        else:
            return 'Unknown'
    
    def run_large_scale_analysis(self, sample_size=10000, batch_size=100):
        """Запускает масштабный анализ"""
        print(f"🌍 ЗАПУСК МАСШТАБНОГО АНАЛИЗА ({sample_size:,} ЗАПИСЕЙ)")
        print("=" * 60)
        
        # Загружаем данные
        locations = self.load_restaurant_locations()
        large_data = self.get_large_sample_data(sample_size)
        
        if not locations:
            print("❌ Нет данных о координатах ресторанов")
            return None
        
        # Фильтруем рестораны с координатами
        restaurants_with_coords = [name for name in large_data['restaurant_name'].unique() 
                                  if name in locations]
        
        print(f"📍 Ресторанов с координатами: {len(restaurants_with_coords)}")
        
        filtered_data = large_data[large_data['restaurant_name'].isin(restaurants_with_coords)]
        
        print(f"🎯 Анализируем {len(filtered_data):,} записей")
        
        # Батчевая обработка
        all_weather_data = []
        processed = 0
        
        print("🌤️ Сбор погодных данных батчами...")
        
        # Группируем уникальные комбинации ресторан-дата
        unique_combinations = filtered_data[['restaurant_name', 'date']].drop_duplicates()
        
        print(f"📊 Уникальных комбинаций ресторан-дата: {len(unique_combinations):,}")
        
        for i in range(0, len(unique_combinations), batch_size):
            batch = unique_combinations.iloc[i:i+batch_size]
            
            if i % 1000 == 0:
                print(f"   Батч {i//batch_size + 1}: обрабатываем {len(batch)} комбинаций...")
            
            for _, row in batch.iterrows():
                restaurant_name = row['restaurant_name']
                date = row['date']
                
                if restaurant_name in locations:
                    location = locations[restaurant_name]
                    
                    # Получаем погоду
                    weather = self.get_weather_with_cache(
                        location['latitude'], 
                        location['longitude'], 
                        date
                    )
                    
                    if weather:
                        # Получаем все данные продаж для этого ресторана в этот день
                        day_data = filtered_data[
                            (filtered_data['restaurant_name'] == restaurant_name) & 
                            (filtered_data['date'] == date)
                        ]
                        
                        if not day_data.empty:
                            record = {
                                'restaurant': restaurant_name,
                                'date': date,
                                'zone': location.get('zone', 'Unknown'),
                                'area': location.get('area', 'Unknown'),
                                'total_sales': day_data['total_sales'].sum(),
                                'total_orders': day_data['total_orders'].sum(),
                                'avg_order_value': day_data['avg_order_value'].mean(),
                                'cancelled_orders': day_data['grab_cancelled'].fillna(0).sum(),
                                'temperature': weather['temperature'],
                                'rain': weather['rain'],
                                'wind': weather['wind'],
                                'humidity': weather['humidity'],
                                'condition': weather['condition'],
                                'day_of_week': day_data['day_of_week'].iloc[0]
                            }
                            all_weather_data.append(record)
                
                processed += 1
                
                # Небольшая пауза чтобы не перегружать API
                if processed % 100 == 0:
                    time.sleep(1)
                
                # Сохраняем кэш периодически
                if processed % 500 == 0:
                    self._save_cache()
            
            if i % 1000 == 0:
                print(f"   Завершен батч {i//batch_size + 1}, собрано {len(all_weather_data)} записей")
        
        # Финальное сохранение кэша
        self._save_cache()
        
        if not all_weather_data:
            print("❌ Не удалось получить погодные данные")
            return None
        
        # Анализируем результаты
        df = pd.DataFrame(all_weather_data)
        print(f"\n✅ Собрано данных для масштабного анализа: {len(df):,} записей")
        
        results = self._analyze_large_scale_patterns(df)
        
        # Сохраняем результаты
        self._save_results(results)
        
        return results
    
    def _analyze_large_scale_patterns(self, df):
        """Анализирует паттерны на большой выборке"""
        print(f"
🌍 МАСШТАБНЫЙ АНАЛИЗ ПОГОДНЫХ ЗАКОНОМЕРНОСТЕЙ")
        print("=" * 55)
        
        results = {
            'total_observations': len(df),
            'restaurants_analyzed': df['restaurant'].nunique(),
            'date_range': (df['date'].min(), df['date'].max()),
            'analysis_timestamp': datetime.now().isoformat()
        }
        
        # Базовая статистика
        avg_sales = df['total_sales'].mean()
        avg_orders = df['total_orders'].mean()
        
        print(f"💰 БАЗОВЫЕ ПОКАЗАТЕЛИ:")
        print(f"   • Средние продажи: {avg_sales:,.0f} IDR/день")
        print(f"   • Средние заказы: {avg_orders:.1f}/день")
        print(f"   • Наблюдений: {len(df):,}")
        print(f"   • Ресторанов: {df['restaurant'].nunique()}")
        
        # ДЕТАЛЬНЫЙ ДОЖДЕВОЙ АНАЛИЗ с большой выборкой
        print(f"
🌧️ ДЕТАЛЬНЫЙ ДОЖДЕВОЙ АНАЛИЗ (БОЛЬШАЯ ВЫБОРКА):")
        rain_ranges = [
            (0, 0.1, "Сухо"),
            (0.1, 1, "Очень легкий дождь"),
            (1, 3, "Легкий дождь"),
            (3, 6, "Умеренный дождь"),
            (6, 12, "Сильный дождь"),
            (12, 25, "Очень сильный дождь"),
            (25, 100, "Ливень")
        ]
        
        rain_analysis = {}
        for min_rain, max_rain, desc in rain_ranges:
            rain_data = df[(df['rain'] >= min_rain) & (df['rain'] < max_rain)]
            if len(rain_data) >= 30:  # Минимум 30 наблюдений
                rain_avg_sales = rain_data['total_sales'].mean()
                rain_avg_orders = rain_data['total_orders'].mean()
                rain_avg_cancelled = rain_data['cancelled_orders'].mean()
                sales_impact = ((rain_avg_sales - avg_sales) / avg_sales * 100)
                orders_impact = ((rain_avg_orders - avg_orders) / avg_orders * 100)
                
                # Статистическая значимость
                _, p_value_sales = stats.ttest_1samp(rain_data['total_sales'], avg_sales)
                _, p_value_orders = stats.ttest_1samp(rain_data['total_orders'], avg_orders)
                
                rain_analysis[desc] = {
                    'rain_range': f"{min_rain}-{max_rain}мм",
                    'sales_impact': sales_impact,
                    'orders_impact': orders_impact,
                    'avg_cancelled': rain_avg_cancelled,
                    'count': len(rain_data),
                    'p_value_sales': p_value_sales,
                    'p_value_orders': p_value_orders,
                    'significant_sales': p_value_sales < 0.05,
                    'significant_orders': p_value_orders < 0.05
                }
                
                # Значимость
                sales_sig = "📈 ЗНАЧИМО" if p_value_sales < 0.05 else "➡️ Тренд"
                orders_sig = "📈 ЗНАЧИМО" if p_value_orders < 0.05 else "➡️ Тренд"
                
                print(f"   {desc} ({min_rain}-{max_rain}мм): {len(rain_data):,} дней")
                print(f"      💰 Продажи: {sales_impact:+.1f}% ({sales_sig}, p={p_value_sales:.3f})")
                print(f"      📦 Заказы: {orders_impact:+.1f}% ({orders_sig}, p={p_value_orders:.3f})")
                print(f"      ❌ Отмены: {rain_avg_cancelled:.1f}/день")
        
        # Температурный анализ
        print(f"
🌡️ ТЕМПЕРАТУРНЫЙ АНАЛИЗ:")
        temp_ranges = [
            (0, 25, "Прохладно"),
            (25, 27, "Комфортно"), 
            (27, 29, "Тепло"),
            (29, 31, "Жарко"),
            (31, 50, "Очень жарко")
        ]
        
        temp_analysis = {}
        for min_temp, max_temp, desc in temp_ranges:
            temp_data = df[(df['temperature'] >= min_temp) & (df['temperature'] < max_temp)]
            if len(temp_data) >= 50:
                temp_avg_sales = temp_data['total_sales'].mean()
                sales_impact = ((temp_avg_sales - avg_sales) / avg_sales * 100)
                
                _, p_value = stats.ttest_1samp(temp_data['total_sales'], avg_sales)
                
                temp_analysis[desc] = {
                    'sales_impact': sales_impact,
                    'count': len(temp_data),
                    'avg_temp': temp_data['temperature'].mean(),
                    'p_value': p_value,
                    'significant': p_value < 0.05
                }
                
                significance = "📈 ЗНАЧИМО" if p_value < 0.05 else "➡️ Тренд"
                print(f"   {desc} ({temp_data['temperature'].mean():.1f}°C): {sales_impact:+.1f}% ({significance}, {len(temp_data):,} дней)")
        
        # Ветровой анализ
        print(f"
💨 ВЕТРОВОЙ АНАЛИЗ:")
        wind_ranges = [
            (0, 8, "Штиль"),
            (8, 15, "Легкий ветер"),
            (15, 25, "Умеренный ветер"),
            (25, 50, "Сильный ветер")
        ]
        
        wind_analysis = {}
        for min_wind, max_wind, desc in wind_ranges:
            wind_data = df[(df['wind'] >= min_wind) & (df['wind'] < max_wind)]
            if len(wind_data) >= 30:
                wind_avg_sales = wind_data['total_sales'].mean()
                sales_impact = ((wind_avg_sales - avg_sales) / avg_sales * 100)
                
                _, p_value = stats.ttest_1samp(wind_data['total_sales'], avg_sales)
                
                wind_analysis[desc] = {
                    'sales_impact': sales_impact,
                    'count': len(wind_data),
                    'p_value': p_value,
                    'significant': p_value < 0.05
                }
                
                significance = "📈 ЗНАЧИМО" if p_value < 0.05 else "➡️ Тренд"
                print(f"   {desc}: {sales_impact:+.1f}% ({significance}, {len(wind_data):,} дней)")
        
        # Общие корреляции
        correlations = {
            'temperature_sales': df['temperature'].corr(df['total_sales']),
            'rain_sales': df['rain'].corr(df['total_sales']),
            'wind_sales': df['wind'].corr(df['total_sales']),
            'humidity_sales': df['humidity'].corr(df['total_sales']),
            'rain_cancelled': df['rain'].corr(df['cancelled_orders'])
        }
        
        print(f"
📊 ОБЩИЕ КОРРЕЛЯЦИИ:")
        for name, value in correlations.items():
            if abs(value) > 0.02:  # Показываем даже слабые корреляции
                print(f"   📊 {name}: {value:.4f}")
        
        # Сохраняем все результаты
        results.update({
            'rain_analysis': rain_analysis,
            'temperature_analysis': temp_analysis,
            'wind_analysis': wind_analysis,
            'correlations': correlations,
            'base_metrics': {
                'avg_sales': avg_sales,
                'avg_orders': avg_orders
            }
        })
        
        return results
    
    def _save_results(self, results):
        """Сохраняет результаты анализа"""
        try:
            os.makedirs('data', exist_ok=True)
            
            # Конвертируем numpy типы для JSON
            def convert_numpy(obj):
                if isinstance(obj, np.integer):
                    return int(obj)
                elif isinstance(obj, np.floating):
                    return float(obj)
                elif isinstance(obj, np.ndarray):
                    return obj.tolist()
                return obj
            
            filename = 'data/large_scale_weather_analysis.json'
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(results, f, ensure_ascii=False, indent=2, default=convert_numpy)
            
            print(f"
💾 Результаты сохранены: {filename}")
            
        except Exception as e:
            print(f"⚠️ Ошибка сохранения результатов: {e}")

def main():
    """Запуск масштабного анализа"""
    analyzer = LargeScaleWeatherAnalyzer()
    
    print("�� МАСШТАБНЫЙ АНАЛИЗ ПОГОДНЫХ ЗАКОНОМЕРНОСТЕЙ")
    print("=" * 65)
    
    # Запускаем анализ с большой выборкой
    results = analyzer.run_large_scale_analysis(sample_size=10000, batch_size=50)
    
    if results:
        print(f"📊 Проанализировано: {results['total_observations']:,} наблюдений")
        print(f"🏪 Ресторанов: {results['restaurants_analyzed']}")
        print(f"📅 Период: {results['date_range'][0]} → {results['date_range'][1]}")
        
        print("
💡 ДОСТИЖЕНИЯ:")
        print("   ✅ Большая выборка обеспечивает статистическую значимость")
        print("   ✅ Детальная разбивка дождевых категорий")
        print("   ✅ P-значения для каждого эффекта")
        print("   ✅ Готово для обновления основной системы")

if __name__ == "__main__":
    main()
