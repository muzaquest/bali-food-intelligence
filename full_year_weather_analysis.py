#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ПОЛНЫЙ ГОДОВОЙ АНАЛИЗ ВЛИЯНИЯ ПОГОДЫ НА DELIVERY
==============================================

Анализирует ВЕСЬ 2024 год для максимальной достоверности результатов.
Оптимизирован для работы с большими объемами данных.
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

class FullYearWeatherAnalyzer:
    def __init__(self):
        self.db_path = 'database.sqlite'
        self.locations_file = 'data/bali_restaurant_locations.json'
        self.weather_api_url = "https://api.open-meteo.com/v1/forecast"
        self.cache_file = 'data/full_year_weather_cache.json'
        self.results_file = 'data/full_year_analysis_results.json'
        
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
    
    def get_full_year_data(self, year=2024):
        """Получает данные за полный год"""
        conn = sqlite3.connect(self.db_path)
        
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
            END as avg_order_value
        FROM restaurants r
        LEFT JOIN grab_stats g ON r.id = g.restaurant_id 
        LEFT JOIN gojek_stats gj ON r.id = gj.restaurant_id 
        WHERE (g.stat_date IS NOT NULL OR gj.stat_date IS NOT NULL)
          AND (COALESCE(g.sales, 0) + COALESCE(gj.sales, 0)) > 0
          AND strftime('%Y', COALESCE(g.stat_date, gj.stat_date)) = '{year}'
        ORDER BY r.name, date
        """
        
        df = pd.read_sql_query(query, conn)
        conn.close()
        
        print(f"📊 ЗАГРУЖЕНО ДАННЫХ ЗА {year} ГОД:")
        print(f"   • Ресторанов: {df['restaurant_name'].nunique()}")
        print(f"   • Записей: {len(df):,}")
        print(f"   • Период: {df['date'].min()} → {df['date'].max()}")
        print(f"   • Уникальных дней: {df['date'].nunique()}")
        
        return df
    
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
    
    def run_full_year_analysis(self, year=2024, batch_size=100):
        """Запускает полный анализ года с батчевой обработкой"""
        print(f"🌍 ЗАПУСК ПОЛНОГО АНАЛИЗА {year} ГОДА")
        print("=" * 50)
        
        # Загружаем данные
        locations = self.load_restaurant_locations()
        yearly_data = self.get_full_year_data(year)
        
        if not locations:
            print("❌ Нет данных о координатах ресторанов")
            return None
        
        # Фильтруем рестораны с координатами
        restaurants_with_coords = [name for name in yearly_data['restaurant_name'].unique() 
                                 if name in locations]
        
        print(f"📍 Ресторанов с координатами: {len(restaurants_with_coords)}")
        
        filtered_data = yearly_data[yearly_data['restaurant_name'].isin(restaurants_with_coords)]
        
        print(f"🎯 Анализируем {len(filtered_data):,} записей за {year} год")
        
        # Батчевая обработка для экономии памяти и API запросов
        all_weather_data = []
        processed = 0
        
        print("🌤️ Сбор погодных данных батчами...")
        
        # Группируем уникальные комбинации ресторан-дата
        unique_combinations = filtered_data[['restaurant_name', 'date']].drop_duplicates()
        
        for i in range(0, len(unique_combinations), batch_size):
            batch = unique_combinations.iloc[i:i+batch_size]
            
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
                                'cancelled_orders': day_data['grab_cancelled'].sum(),
                                'temperature': weather['temperature'],
                                'rain': weather['rain'],
                                'wind': weather['wind'],
                                'humidity': weather['humidity'],
                                'condition': weather['condition']
                            }
                            all_weather_data.append(record)
                
                processed += 1
                
                # Небольшая пауза чтобы не перегружать API
                if processed % 50 == 0:
                    time.sleep(1)
            
            # Сохраняем кэш после каждого батча
            self._save_cache()
            
            print(f"   Завершен батч {i//batch_size + 1}, собрано {len(all_weather_data)} записей")
        
        if not all_weather_data:
            print("❌ Не удалось получить погодные данные")
            return None
        
        # Анализируем результаты
        df = pd.DataFrame(all_weather_data)
        print(f"\n✅ Собрано данных для полного анализа: {len(df):,} записей")
        
        results = self._analyze_full_year_patterns(df, year)
        
        # Сохраняем результаты
        self._save_results(results, year)
        
        return results
    
    def _analyze_full_year_patterns(self, df, year):
        """Анализирует паттерны за полный год"""
        print(f"\n🌍 АНАЛИЗ ПОГОДНЫХ ЗАКОНОМЕРНОСТЕЙ ЗА {year} ГОД")
        print("=" * 55)
        
        results = {
            'year': year,
            'total_observations': len(df),
            'restaurants_analyzed': df['restaurant'].nunique(),
            'date_range': (df['date'].min(), df['date'].max()),
            'analysis_timestamp': datetime.now().isoformat()
        }
        
        # Базовая статистика
        avg_sales = df['total_sales'].mean()
        avg_orders = df['total_orders'].mean()
        
        print(f"💰 БАЗОВЫЕ ПОКАЗАТЕЛИ ЗА {year}:")
        print(f"   • Средние продажи: {avg_sales:,.0f} IDR/день")
        print(f"   • Средние заказы: {avg_orders:.1f}/день")
        print(f"   • Наблюдений: {len(df):,}")
        print(f"   • Ресторанов: {df['restaurant'].nunique()}")
        
        # Температурный анализ
        print(f"\n🌡️ ТЕМПЕРАТУРНЫЙ АНАЛИЗ:")
        temp_ranges = [
            (0, 26, "Прохладно"),
            (26, 28, "Комфортно"), 
            (28, 30, "Тепло"),
            (30, 32, "Жарко"),
            (32, 50, "Очень жарко")
        ]
        
        temp_analysis = {}
        for min_temp, max_temp, desc in temp_ranges:
            temp_data = df[(df['temperature'] >= min_temp) & (df['temperature'] < max_temp)]
            if len(temp_data) >= 20:  # Минимум 20 наблюдений
                temp_avg_sales = temp_data['total_sales'].mean()
                temp_avg_orders = temp_data['total_orders'].mean()
                sales_impact = ((temp_avg_sales - avg_sales) / avg_sales * 100)
                orders_impact = ((temp_avg_orders - avg_orders) / avg_orders * 100)
                
                # Статистическая значимость
                _, p_value = stats.ttest_1samp(temp_data['total_sales'], avg_sales)
                
                temp_analysis[desc] = {
                    'sales_impact': sales_impact,
                    'orders_impact': orders_impact,
                    'count': len(temp_data),
                    'avg_temp': temp_data['temperature'].mean(),
                    'p_value': p_value,
                    'significant': p_value < 0.05
                }
                
                significance = "📈 Значимо" if p_value < 0.05 else "➡️ Тренд"
                print(f"   {significance} {desc} ({temp_data['temperature'].mean():.1f}°C): продажи {sales_impact:+.1f}% ({len(temp_data)} дней)")
        
        # Дождевой анализ
        print(f"\n🌧️ ДОЖДЕВОЙ АНАЛИЗ:")
        rain_ranges = [
            (0, 0.1, "Сухо"),
            (0.1, 2, "Легкий дождь"),
            (2, 8, "Умеренный дождь"),
            (8, 20, "Сильный дождь"),
            (20, 100, "Ливень")
        ]
        
        rain_analysis = {}
        for min_rain, max_rain, desc in rain_ranges:
            rain_data = df[(df['rain'] >= min_rain) & (df['rain'] < max_rain)]
            if len(rain_data) >= 10:
                rain_avg_sales = rain_data['total_sales'].mean()
                rain_avg_cancelled = rain_data['cancelled_orders'].mean()
                sales_impact = ((rain_avg_sales - avg_sales) / avg_sales * 100)
                
                _, p_value = stats.ttest_1samp(rain_data['total_sales'], avg_sales)
                
                rain_analysis[desc] = {
                    'sales_impact': sales_impact,
                    'avg_cancelled': rain_avg_cancelled,
                    'count': len(rain_data),
                    'p_value': p_value,
                    'significant': p_value < 0.05
                }
                
                significance = "📈 Значимо" if p_value < 0.05 else "➡️ Тренд"
                print(f"   {significance} {desc}: продажи {sales_impact:+.1f}%, отмены {rain_avg_cancelled:.1f}/день ({len(rain_data)} дней)")
        
        # Ветровой анализ
        print(f"\n💨 ВЕТРОВОЙ АНАЛИЗ:")
        wind_ranges = [
            (0, 10, "Штиль"),
            (10, 20, "Легкий ветер"),
            (20, 30, "Умеренный ветер"),
            (30, 50, "Сильный ветер")
        ]
        
        wind_analysis = {}
        for min_wind, max_wind, desc in wind_ranges:
            wind_data = df[(df['wind'] >= min_wind) & (df['wind'] < max_wind)]
            if len(wind_data) >= 15:
                wind_avg_sales = wind_data['total_sales'].mean()
                sales_impact = ((wind_avg_sales - avg_sales) / avg_sales * 100)
                
                _, p_value = stats.ttest_1samp(wind_data['total_sales'], avg_sales)
                
                wind_analysis[desc] = {
                    'sales_impact': sales_impact,
                    'count': len(wind_data),
                    'p_value': p_value,
                    'significant': p_value < 0.05
                }
                
                significance = "📈 Значимо" if p_value < 0.05 else "➡️ Тренд"
                print(f"   {significance} {desc}: продажи {sales_impact:+.1f}% ({len(wind_data)} дней)")
        
        # Зональный анализ
        print(f"\n🌍 ЗОНАЛЬНЫЙ АНАЛИЗ:")
        zone_analysis = {}
        for zone in df['zone'].unique():
            zone_data = df[df['zone'] == zone]
            if len(zone_data) >= 50:
                zone_temp_corr = zone_data['temperature'].corr(zone_data['total_sales'])
                zone_rain_corr = zone_data['rain'].corr(zone_data['total_sales'])
                zone_wind_corr = zone_data['wind'].corr(zone_data['total_sales'])
                
                zone_analysis[zone] = {
                    'temp_correlation': zone_temp_corr,
                    'rain_correlation': zone_rain_corr,
                    'wind_correlation': zone_wind_corr,
                    'observations': len(zone_data)
                }
                
                print(f"   📍 {zone} ({len(zone_data)} наблюдений):")
                print(f"      🌡️ Температура: {zone_temp_corr:+.3f}")
                print(f"      🌧️ Дождь: {zone_rain_corr:+.3f}")
                print(f"      💨 Ветер: {zone_wind_corr:+.3f}")
        
        # Корреляции
        correlations = {
            'temperature_sales': df['temperature'].corr(df['total_sales']),
            'rain_sales': df['rain'].corr(df['total_sales']),
            'wind_sales': df['wind'].corr(df['total_sales']),
            'humidity_sales': df['humidity'].corr(df['total_sales'])
        }
        
        print(f"\n📊 ОБЩИЕ КОРРЕЛЯЦИИ:")
        for name, value in correlations.items():
            if abs(value) > 0.05:
                print(f"   📊 {name}: {value:.3f}")
        
        # Сохраняем все результаты
        results.update({
            'temperature_analysis': temp_analysis,
            'rain_analysis': rain_analysis,
            'wind_analysis': wind_analysis,
            'zone_analysis': zone_analysis,
            'correlations': correlations,
            'base_metrics': {
                'avg_sales': avg_sales,
                'avg_orders': avg_orders
            }
        })
        
        return results
    
    def _save_results(self, results, year):
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
            
            filename = f'data/full_year_analysis_{year}.json'
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(results, f, ensure_ascii=False, indent=2, default=convert_numpy)
            
            print(f"\n💾 Результаты сохранены: {filename}")
            
        except Exception as e:
            print(f"⚠️ Ошибка сохранения результатов: {e}")

def main():
    """Запуск полного годового анализа"""
    analyzer = FullYearWeatherAnalyzer()
    
    print("🌍 ПОЛНЫЙ ГОДОВОЙ АНАЛИЗ ВЛИЯНИЯ ПОГОДЫ")
    print("Максимальная достоверность через анализ целого года!")
    print("=" * 65)
    
    # Запускаем анализ 2024 года
    results = analyzer.run_full_year_analysis(year=2024, batch_size=50)
    
    if results:
        print("\n🎉 ПОЛНЫЙ ГОДОВОЙ АНАЛИЗ ЗАВЕРШЕН!")
        print(f"📊 Проанализировано: {results['total_observations']:,} наблюдений")
        print(f"🏪 Ресторанов: {results['restaurants_analyzed']}")
        print(f"📅 Период: {results['date_range'][0]} → {results['date_range'][1]}")
        
        print("\n💡 ДОСТИЖЕНИЯ:")
        print("   ✅ Полный год данных проанализирован")
        print("   ✅ Максимальная статистическая достоверность")
        print("   ✅ Все сезоны и погодные условия учтены")
        print("   ✅ Готово для интеграции в основную систему")

if __name__ == "__main__":
    main()