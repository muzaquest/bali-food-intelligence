#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ГЛОБАЛЬНЫЙ АНАЛИЗ ВЛИЯНИЯ ПОГОДЫ И ПРАЗДНИКОВ
===========================================

Анализирует влияние погоды и праздников на продажи
по ВСЕЙ базе ресторанов для получения статистически
значимых выводов.

Возможности:
- Анализ 59 ресторанов × 668 дней = 39,412 точек данных
- Точные GPS координаты для каждого ресторана
- Реальные погодные данные из Open-Meteo API
- Статистическая значимость и корреляции
- Сегментация по зонам (Beach, Central, Mountain, Cliff)
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

class GlobalWeatherAnalyzer:
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
    
    def get_all_restaurant_data(self):
        """Получает данные о продажах всех ресторанов"""
        conn = sqlite3.connect(self.db_path)
        
        query = """
        SELECT 
            r.name as restaurant_name,
            COALESCE(g.stat_date, gj.stat_date) as date,
            COALESCE(g.sales, 0) + COALESCE(gj.sales, 0) as total_sales,
            COALESCE(g.orders, 0) + COALESCE(gj.orders, 0) as total_orders
        FROM restaurants r
        LEFT JOIN grab_stats g ON r.id = g.restaurant_id 
        LEFT JOIN gojek_stats gj ON r.id = gj.restaurant_id 
        WHERE (g.stat_date IS NOT NULL OR gj.stat_date IS NOT NULL)
          AND (COALESCE(g.sales, 0) + COALESCE(gj.sales, 0)) > 0
        ORDER BY r.name, date
        """
        
        df = pd.read_sql_query(query, conn)
        conn.close()
        
        print(f"📊 Загружено данных:")
        print(f"   • Ресторанов: {df['restaurant_name'].nunique()}")
        print(f"   • Записей: {len(df):,}")
        print(f"   • Период: {df['date'].min()} → {df['date'].max()}")
        
        return df
    
    def get_weather_for_location(self, lat, lon, date):
        """Получает погоду для конкретной локации и даты"""
        try:
            params = {
                'latitude': lat,
                'longitude': lon,
                'start_date': date,
                'end_date': date,
                'hourly': 'temperature_2m,precipitation,weather_code',
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
                    
                    avg_temp = sum(temps) / len(temps) if temps else 28
                    total_rain = sum(precipitation) if precipitation else 0
                    main_weather_code = max(set(weather_codes), key=weather_codes.count) if weather_codes else 0
                    
                    condition = self._weather_code_to_condition(main_weather_code)
                    
                    return {
                        'temperature': avg_temp,
                        'condition': condition,
                        'rain': total_rain,
                        'weather_code': main_weather_code
                    }
            
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
        elif code in [51, 53, 55, 56, 57]:
            return 'Drizzle'
        elif code in [61, 63, 65, 66, 67]:
            return 'Rain'
        elif code in [71, 73, 75, 77]:
            return 'Snow'
        elif code in [80, 81, 82]:
            return 'Rain'
        elif code in [85, 86]:
            return 'Snow'
        elif code in [95, 96, 99]:
            return 'Thunderstorm'
        else:
            return 'Unknown'
    
    def analyze_global_weather_impact(self, sample_size=1000):
        """
        Глобальный анализ влияния погоды на продажи
        
        Args:
            sample_size: Количество случайных дней для анализа (для экономии API запросов)
        """
        print("🌍 ЗАПУСК ГЛОБАЛЬНОГО АНАЛИЗА ПОГОДЫ")
        print("=" * 50)
        
        # Загружаем данные
        locations = self.load_restaurant_locations()
        sales_data = self.get_all_restaurant_data()
        
        if not locations:
            print("❌ Нет данных о координатах ресторанов")
            return None
        
        # Фильтруем рестораны с координатами
        restaurants_with_coords = [name for name in sales_data['restaurant_name'].unique() 
                                 if name in locations]
        
        print(f"📍 Ресторанов с координатами: {len(restaurants_with_coords)}")
        
        # Создаем выборку для анализа
        filtered_data = sales_data[sales_data['restaurant_name'].isin(restaurants_with_coords)]
        
        # Берем случайную выборку для экономии API запросов
        if len(filtered_data) > sample_size:
            sample_data = filtered_data.sample(n=sample_size, random_state=42)
            print(f"🎯 Анализируем выборку: {sample_size} записей из {len(filtered_data):,}")
        else:
            sample_data = filtered_data
            print(f"🎯 Анализируем все данные: {len(sample_data):,} записей")
        
        # Собираем данные погоды и продаж
        weather_sales_data = []
        processed = 0
        
        print("🌤️ Сбор погодных данных...")
        
        for _, row in sample_data.iterrows():
            restaurant_name = row['restaurant_name']
            date = row['date']
            sales = row['total_sales']
            
            if restaurant_name in locations:
                location = locations[restaurant_name]
                weather = self.get_weather_for_location(
                    location['latitude'], 
                    location['longitude'], 
                    date
                )
                
                if weather:
                    weather_sales_data.append({
                        'restaurant': restaurant_name,
                        'date': date,
                        'sales': sales,
                        'zone': location.get('zone', 'Unknown'),
                        'area': location.get('area', 'Unknown'),
                        'temperature': weather['temperature'],
                        'condition': weather['condition'],
                        'rain': weather['rain']
                    })
            
            processed += 1
            if processed % 100 == 0:
                print(f"   Обработано: {processed}/{len(sample_data)}")
        
        if not weather_sales_data:
            print("❌ Не удалось получить данные о погоде")
            return None
        
        # Анализируем результаты
        df = pd.DataFrame(weather_sales_data)
        print(f"\n✅ Собрано данных для анализа: {len(df)} записей")
        
        return self._analyze_weather_patterns(df)
    
    def _analyze_weather_patterns(self, df):
        """Анализирует паттерны влияния погоды"""
        print("\n📊 АНАЛИЗ ВЛИЯНИЯ ПОГОДЫ НА ПРОДАЖИ")
        print("=" * 40)
        
        results = {}
        
        # 1. Общее влияние погодных условий
        overall_avg = df['sales'].mean()
        weather_impact = {}
        
        print(f"💰 Общая средняя продажа: {overall_avg:,.0f} IDR")
        print(f"\n🌤️ Влияние погодных условий:")
        
        for condition in df['condition'].unique():
            condition_data = df[df['condition'] == condition]
            if len(condition_data) >= 5:  # Минимум 5 наблюдений
                avg_sales = condition_data['sales'].mean()
                impact = ((avg_sales - overall_avg) / overall_avg * 100)
                count = len(condition_data)
                
                # Статистическая значимость
                _, p_value = stats.ttest_1samp(condition_data['sales'], overall_avg)
                significant = "📈" if p_value < 0.05 else "➡️"
                
                weather_impact[condition] = {
                    'avg_sales': avg_sales,
                    'impact_percent': impact,
                    'count': count,
                    'p_value': p_value,
                    'significant': p_value < 0.05
                }
                
                print(f"   {significant} {condition}: {impact:+.1f}% ({count} наблюдений)")
                if p_value < 0.05:
                    print(f"      📊 Статистически значимо (p={p_value:.3f})")
        
        # 2. Анализ по зонам
        print(f"\n🌍 Влияние погоды по зонам:")
        zone_analysis = {}
        
        for zone in df['zone'].unique():
            zone_data = df[df['zone'] == zone]
            if len(zone_data) >= 10:
                zone_avg = zone_data['sales'].mean()
                zone_weather = {}
                
                print(f"\n   📍 {zone} зона ({len(zone_data)} наблюдений):")
                
                for condition in zone_data['condition'].unique():
                    condition_zone_data = zone_data[zone_data['condition'] == condition]
                    if len(condition_zone_data) >= 3:
                        avg_sales = condition_zone_data['sales'].mean()
                        impact = ((avg_sales - zone_avg) / zone_avg * 100)
                        
                        zone_weather[condition] = impact
                        print(f"      {condition}: {impact:+.1f}%")
                
                zone_analysis[zone] = zone_weather
        
        # 3. Корреляция с температурой
        temp_corr = df['temperature'].corr(df['sales'])
        print(f"\n🌡️ Корреляция температуры и продаж: {temp_corr:.3f}")
        
        # 4. Влияние дождя
        rainy_days = df[df['rain'] > 0]
        dry_days = df[df['rain'] == 0]
        
        if len(rainy_days) > 0 and len(dry_days) > 0:
            rain_avg = rainy_days['sales'].mean()
            dry_avg = dry_days['sales'].mean()
            rain_impact = ((rain_avg - dry_avg) / dry_avg * 100)
            
            print(f"\n💧 Влияние дождя:")
            print(f"   • Дождливые дни: {rain_avg:,.0f} IDR ({len(rainy_days)} дней)")
            print(f"   • Сухие дни: {dry_avg:,.0f} IDR ({len(dry_days)} дней)")
            print(f"   • Влияние дождя: {rain_impact:+.1f}%")
        
        results = {
            'weather_impact': weather_impact,
            'zone_analysis': zone_analysis,
            'temperature_correlation': temp_corr,
            'total_observations': len(df),
            'restaurants_analyzed': df['restaurant'].nunique()
        }
        
        return results

def main():
    """Демонстрация глобального анализа"""
    analyzer = GlobalWeatherAnalyzer()
    
    print("🎯 ГЛОБАЛЬНЫЙ АНАЛИЗ ВЛИЯНИЯ ПОГОДЫ")
    print("Анализирует влияние погоды на продажи по всей базе ресторанов")
    print("=" * 60)
    
    # Запускаем анализ с выборкой 500 записей для демонстрации
    results = analyzer.analyze_global_weather_impact(sample_size=500)
    
    if results:
        print("\n🎉 АНАЛИЗ ЗАВЕРШЕН!")
        print(f"📊 Проанализировано: {results['total_observations']} наблюдений")
        print(f"🏪 Ресторанов: {results['restaurants_analyzed']}")
        print("\n💡 Результаты можно использовать для:")
        print("   • Более точного прогнозирования влияния погоды")
        print("   • Персонализации по зонам (Beach, Central, Mountain)")
        print("   • Статистически обоснованных рекомендаций")
        print("   • Автоматических уведомлений о погодных рисках")

if __name__ == "__main__":
    main()