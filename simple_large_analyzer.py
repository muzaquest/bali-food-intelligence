#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ПРОСТОЙ МАСШТАБНЫЙ АНАЛИЗ ПОГОДНЫХ ДАННЫХ
========================================
"""

import sqlite3
import pandas as pd
import numpy as np
import json
from datetime import datetime
import requests
import os
from scipy import stats
import time

class SimpleLargeAnalyzer:
    def __init__(self):
        self.db_path = 'database.sqlite'
        self.locations_file = 'data/bali_restaurant_locations.json'
        self.weather_api_url = "https://api.open-meteo.com/v1/forecast"
        
    def load_restaurant_locations(self):
        """Загружает GPS координаты всех ресторанов"""
        try:
            with open(self.locations_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            return {r['name']: r for r in data['restaurants']}
        except Exception as e:
            print(f"⚠️ Ошибка загрузки координат: {e}")
            return {}
    
    def get_sample_data(self, sample_size=5000):
        """Получает выборку данных для анализа"""
        conn = sqlite3.connect(self.db_path)
        
        query = f"""
        SELECT 
            r.name as restaurant_name,
            COALESCE(g.stat_date, gj.stat_date) as date,
            COALESCE(g.sales, 0) + COALESCE(gj.sales, 0) as total_sales,
            COALESCE(g.orders, 0) + COALESCE(gj.orders, 0) as total_orders,
            g.cancelled_orders as grab_cancelled
        FROM restaurants r
        LEFT JOIN grab_stats g ON r.id = g.restaurant_id 
        LEFT JOIN gojek_stats gj ON r.id = gj.restaurant_id 
        WHERE (g.stat_date IS NOT NULL OR gj.stat_date IS NOT NULL)
          AND (COALESCE(g.sales, 0) + COALESCE(gj.sales, 0)) > 0
        ORDER BY RANDOM()
        LIMIT {sample_size}
        """
        
        df = pd.read_sql_query(query, conn)
        conn.close()
        
        print(f"📊 Загружено данных: {len(df):,} записей")
        print(f"   • Ресторанов: {df['restaurant_name'].nunique()}")
        print(f"   • Период: {df['date'].min()} → {df['date'].max()}")
        
        return df
    
    def get_weather(self, lat, lon, date):
        """Получает погодные данные"""
        try:
            params = {
                'latitude': lat,
                'longitude': lon,
                'start_date': date,
                'end_date': date,
                'hourly': 'temperature_2m,precipitation,wind_speed_10m',
                'timezone': 'Asia/Jakarta'
            }
            
            response = requests.get(self.weather_api_url, params=params, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                hourly = data.get('hourly', {})
                
                if hourly and len(hourly.get('time', [])) > 0:
                    temps = hourly.get('temperature_2m', [28])
                    precipitation = hourly.get('precipitation', [0])
                    wind_speeds = hourly.get('wind_speed_10m', [5])
                    
                    return {
                        'temperature': np.mean(temps) if temps else 28,
                        'rain': sum(precipitation) if precipitation else 0,
                        'wind': max(wind_speeds) if wind_speeds else 5
                    }
            
            return None
        except Exception as e:
            return None
    
    def run_analysis(self):
        """Запускает анализ"""
        print("🌍 ЗАПУСК ПРОСТОГО МАСШТАБНОГО АНАЛИЗА")
        print("=" * 45)
        
        # Загружаем данные
        locations = self.load_restaurant_locations()
        sales_data = self.get_sample_data(3000)  # Берем 3000 записей
        
        if not locations:
            print("❌ Нет координат ресторанов")
            return
        
        # Фильтруем рестораны с координатами
        restaurants_with_coords = [name for name in sales_data['restaurant_name'].unique() 
                                  if name in locations]
        
        print(f"📍 Ресторанов с координатами: {len(restaurants_with_coords)}")
        
        filtered_data = sales_data[sales_data['restaurant_name'].isin(restaurants_with_coords)]
        
        # Собираем погодные данные
        weather_data = []
        processed = 0
        
        print("🌤️ Сбор погодных данных...")
        
        # Берем уникальные комбинации ресторан-дата
        unique_combos = filtered_data[['restaurant_name', 'date']].drop_duplicates()
        
        # Ограничиваем до 500 запросов для экономии времени
        sample_combos = unique_combos.sample(min(500, len(unique_combos)), random_state=42)
        
        for _, row in sample_combos.iterrows():
            restaurant_name = row['restaurant_name']
            date = row['date']
            
            if restaurant_name in locations:
                location = locations[restaurant_name]
                
                weather = self.get_weather(
                    location['latitude'], 
                    location['longitude'], 
                    date
                )
                
                if weather:
                    # Получаем продажи для этого дня
                    day_sales = filtered_data[
                        (filtered_data['restaurant_name'] == restaurant_name) & 
                        (filtered_data['date'] == date)
                    ]
                    
                    if not day_sales.empty:
                        record = {
                            'restaurant': restaurant_name,
                            'date': date,
                            'total_sales': day_sales['total_sales'].sum(),
                            'total_orders': day_sales['total_orders'].sum(),
                            'cancelled_orders': day_sales['grab_cancelled'].fillna(0).sum(),
                            'temperature': weather['temperature'],
                            'rain': weather['rain'],
                            'wind': weather['wind']
                        }
                        weather_data.append(record)
            
            processed += 1
            if processed % 50 == 0:
                print(f"   Обработано: {processed}/{len(sample_combos)}")
                time.sleep(1)  # Пауза для API
        
        if not weather_data:
            print("❌ Не удалось получить погодные данные")
            return
        
        # Анализируем результаты
        df = pd.DataFrame(weather_data)
        print(f"\n✅ Собрано для анализа: {len(df):,} записей")
        
        self.analyze_patterns(df)
    
    def analyze_patterns(self, df):
        """Анализирует погодные паттерны"""
        print("\n🌍 АНАЛИЗ ПОГОДНЫХ ЗАКОНОМЕРНОСТЕЙ")
        print("=" * 40)
        
        avg_sales = df['total_sales'].mean()
        avg_orders = df['total_orders'].mean()
        
        print(f"💰 Базовые показатели:")
        print(f"   • Средние продажи: {avg_sales:,.0f} IDR/день")
        print(f"   • Средние заказы: {avg_orders:.1f}/день")
        print(f"   • Наблюдений: {len(df):,}")
        
        # ДЕТАЛЬНЫЙ ДОЖДЕВОЙ АНАЛИЗ
        print(f"\n🌧️ ДЕТАЛЬНЫЙ ДОЖДЕВОЙ АНАЛИЗ:")
        
        rain_ranges = [
            (0, 0.1, "Сухо"),
            (0.1, 2, "Легкий дождь"),
            (2, 8, "Умеренный дождь"),
            (8, 20, "Сильный дождь"),
            (20, 100, "Ливень")
        ]
        
        for min_rain, max_rain, desc in rain_ranges:
            rain_data = df[(df['rain'] >= min_rain) & (df['rain'] < max_rain)]
            
            if len(rain_data) >= 10:  # Минимум 10 наблюдений
                rain_avg_sales = rain_data['total_sales'].mean()
                rain_avg_orders = rain_data['total_orders'].mean()
                rain_avg_cancelled = rain_data['cancelled_orders'].mean()
                
                sales_impact = ((rain_avg_sales - avg_sales) / avg_sales * 100)
                orders_impact = ((rain_avg_orders - avg_orders) / avg_orders * 100)
                
                # Статистическая значимость
                try:
                    _, p_value = stats.ttest_1samp(rain_data['total_sales'], avg_sales)
                    significant = "📈 ЗНАЧИМО" if p_value < 0.05 else "➡️ Тренд"
                except:
                    significant = "📊 Данные"
                
                print(f"   {desc} ({min_rain}-{max_rain}мм): {len(rain_data):,} дней")
                print(f"      💰 Продажи: {sales_impact:+.1f}% ({significant})")
                print(f"      📦 Заказы: {orders_impact:+.1f}%")
                print(f"      ❌ Отмены: {rain_avg_cancelled:.1f}/день")
        
        # Температурный анализ
        print(f"\n🌡️ ТЕМПЕРАТУРНЫЙ АНАЛИЗ:")
        
        temp_ranges = [
            (0, 26, "Прохладно"),
            (26, 28, "Комфортно"), 
            (28, 30, "Тепло"),
            (30, 32, "Жарко"),
            (32, 50, "Очень жарко")
        ]
        
        for min_temp, max_temp, desc in temp_ranges:
            temp_data = df[(df['temperature'] >= min_temp) & (df['temperature'] < max_temp)]
            
            if len(temp_data) >= 10:
                temp_avg_sales = temp_data['total_sales'].mean()
                sales_impact = ((temp_avg_sales - avg_sales) / avg_sales * 100)
                
                try:
                    _, p_value = stats.ttest_1samp(temp_data['total_sales'], avg_sales)
                    significant = "📈 ЗНАЧИМО" if p_value < 0.05 else "➡️ Тренд"
                except:
                    significant = "📊 Данные"
                
                print(f"   {desc} ({temp_data['temperature'].mean():.1f}°C): {sales_impact:+.1f}% ({significant}, {len(temp_data):,} дней)")
        
        # Ветровой анализ
        print(f"\n💨 ВЕТРОВОЙ АНАЛИЗ:")
        
        wind_ranges = [
            (0, 10, "Штиль"),
            (10, 20, "Легкий ветер"),
            (20, 30, "Умеренный ветер"),
            (30, 50, "Сильный ветер")
        ]
        
        for min_wind, max_wind, desc in wind_ranges:
            wind_data = df[(df['wind'] >= min_wind) & (df['wind'] < max_wind)]
            
            if len(wind_data) >= 5:
                wind_avg_sales = wind_data['total_sales'].mean()
                sales_impact = ((wind_avg_sales - avg_sales) / avg_sales * 100)
                
                try:
                    _, p_value = stats.ttest_1samp(wind_data['total_sales'], avg_sales)
                    significant = "📈 ЗНАЧИМО" if p_value < 0.05 else "➡️ Тренд"
                except:
                    significant = "📊 Данные"
                
                print(f"   {desc}: {sales_impact:+.1f}% ({significant}, {len(wind_data):,} дней)")
        
        # Корреляции
        print(f"\n📊 КОРРЕЛЯЦИИ:")
        correlations = {
            'temperature_sales': df['temperature'].corr(df['total_sales']),
            'rain_sales': df['rain'].corr(df['total_sales']),
            'wind_sales': df['wind'].corr(df['total_sales']),
            'rain_cancelled': df['rain'].corr(df['cancelled_orders'])
        }
        
        for name, value in correlations.items():
            if abs(value) > 0.05:
                print(f"   📊 {name}: {value:.3f}")
        
        # Сохраняем результаты
        try:
            os.makedirs('data', exist_ok=True)
            results = {
                'total_observations': len(df),
                'avg_sales': avg_sales,
                'avg_orders': avg_orders,
                'correlations': correlations,
                'analysis_timestamp': datetime.now().isoformat()
            }
            
            with open('data/simple_large_analysis.json', 'w', encoding='utf-8') as f:
                json.dump(results, f, ensure_ascii=False, indent=2, default=str)
            
            print(f"\n💾 Результаты сохранены: data/simple_large_analysis.json")
        except Exception as e:
            print(f"⚠️ Ошибка сохранения: {e}")

def main():
    analyzer = SimpleLargeAnalyzer()
    analyzer.run_analysis()

if __name__ == "__main__":
    main()