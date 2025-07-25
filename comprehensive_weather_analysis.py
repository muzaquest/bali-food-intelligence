#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ПОЛНЫЙ АНАЛИЗ ВСЕХ ПОГОДНЫХ ЗАКОНОМЕРНОСТЕЙ ДЛЯ DELIVERY
======================================================

Анализирует ВСЕ погодные факторы и их влияние на delivery:
🌡️ ТЕМПЕРАТУРА: жара/холод → поведение клиентов
☀️ СОЛНЦЕ: ясная погода → активность заказов
🌧️ ДОЖДЬ: влияние на курьеров и клиентов
💨 ВЕТЕР: безопасность байкеров
🌫️ ТУМАН: видимость и доставка
⛈️ ГРОЗЫ: экстремальные условия
💧 ВЛАЖНОСТЬ: комфорт и активность
🌅 ВРЕМЯ СУТОК: пики заказов vs погода

ЦЕЛЬ: Выявить ВСЕ скрытые закономерности!
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
import warnings
warnings.filterwarnings('ignore')

class ComprehensiveWeatherAnalyzer:
    def __init__(self):
        self.db_path = 'database.sqlite'
        self.locations_file = 'data/bali_restaurant_locations.json'
        self.weather_api_url = "https://api.open-meteo.com/v1/forecast"
        self.cache_file = 'data/weather_analysis_cache.json'
        
    def load_restaurant_locations(self):
        """Загружает GPS координаты всех ресторанов"""
        try:
            with open(self.locations_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            return {r['name']: r for r in data['restaurants']}
        except Exception as e:
            print(f"⚠️ Ошибка загрузки координат: {e}")
            return {}
    
    def get_comprehensive_delivery_data(self):
        """Получает максимально полные данные для анализа"""
        conn = sqlite3.connect(self.db_path)
        
        query = """
        SELECT 
            r.name as restaurant_name,
            COALESCE(g.stat_date, gj.stat_date) as date,
            
            -- Основные метрики
            COALESCE(g.sales, 0) + COALESCE(gj.sales, 0) as total_sales,
            COALESCE(g.orders, 0) + COALESCE(gj.orders, 0) as total_orders,
            
            -- Grab детали
            g.sales as grab_sales,
            g.orders as grab_orders,
            g.cancelled_orders as grab_cancelled,
            g.store_is_busy as grab_busy,
            g.store_is_closed as grab_closed,
            g.out_of_stock as grab_out_of_stock,
            
            -- Gojek детали
            gj.sales as gojek_sales,
            gj.orders as gojek_orders,
            gj.accepting_time as gojek_accepting_time,
            gj.preparation_time as gojek_prep_time,
            
            -- Рейтинги
            COALESCE(g.rating, gj.rating) as rating,
            
            -- Средний чек
            CASE 
                WHEN (COALESCE(g.orders, 0) + COALESCE(gj.orders, 0)) > 0 
                THEN (COALESCE(g.sales, 0) + COALESCE(gj.sales, 0)) / (COALESCE(g.orders, 0) + COALESCE(gj.orders, 0))
                ELSE 0 
            END as avg_order_value,
            
            -- День недели для анализа паттернов
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
        WHERE (g.stat_date IS NOT NULL OR gj.stat_date IS NOT NULL)
          AND (COALESCE(g.sales, 0) + COALESCE(gj.sales, 0)) > 0
        ORDER BY r.name, date
        """
        
        df = pd.read_sql_query(query, conn)
        conn.close()
        
        print(f"📊 ЗАГРУЖЕНО ДАННЫХ ДЛЯ ПОЛНОГО АНАЛИЗА:")
        print(f"   • Ресторанов: {df['restaurant_name'].nunique()}")
        print(f"   • Записей: {len(df):,}")
        print(f"   • Период: {df['date'].min()} → {df['date'].max()}")
        print(f"   • Дней недели: {df['day_of_week'].value_counts().to_dict()}")
        
        return df
    
    def get_comprehensive_weather(self, lat, lon, date):
        """Получает ПОЛНУЮ погодную информацию"""
        try:
            params = {
                'latitude': lat,
                'longitude': lon,
                'start_date': date,
                'end_date': date,
                'hourly': [
                    'temperature_2m', 'precipitation', 'weather_code',
                    'wind_speed_10m', 'wind_direction_10m', 'relative_humidity_2m',
                    'cloud_cover', 'visibility', 'pressure_msl',
                    'apparent_temperature', 'uv_index'
                ],
                'timezone': 'Asia/Jakarta'
            }
            
            response = requests.get(self.weather_api_url, params=params, timeout=15)
            
            if response.status_code == 200:
                data = response.json()
                hourly = data.get('hourly', {})
                
                if hourly and len(hourly.get('time', [])) > 0:
                    # Извлекаем все метрики
                    temps = hourly.get('temperature_2m', [28])
                    apparent_temps = hourly.get('apparent_temperature', [28])
                    precipitation = hourly.get('precipitation', [0])
                    weather_codes = hourly.get('weather_code', [0])
                    wind_speeds = hourly.get('wind_speed_10m', [5])
                    wind_directions = hourly.get('wind_direction_10m', [0])
                    humidity = hourly.get('relative_humidity_2m', [75])
                    cloud_cover = hourly.get('cloud_cover', [50])
                    visibility = hourly.get('visibility', [10000])
                    pressure = hourly.get('pressure_msl', [1013])
                    uv_index = hourly.get('uv_index', [5])
                    
                    # Рассчитываем агрегированные метрики
                    weather_summary = {
                        # Температурные характеристики
                        'avg_temp': np.mean(temps) if temps else 28,
                        'max_temp': max(temps) if temps else 28,
                        'min_temp': min(temps) if temps else 28,
                        'temp_range': (max(temps) - min(temps)) if temps else 0,
                        'feels_like': np.mean(apparent_temps) if apparent_temps else 28,
                        
                        # Осадки и влажность
                        'total_rain': sum(precipitation) if precipitation else 0,
                        'max_rain_hour': max(precipitation) if precipitation else 0,
                        'rain_hours': sum(1 for p in precipitation if p > 0.1),
                        'avg_humidity': np.mean(humidity) if humidity else 75,
                        
                        # Ветер
                        'avg_wind': np.mean(wind_speeds) if wind_speeds else 5,
                        'max_wind': max(wind_speeds) if wind_speeds else 5,
                        'wind_direction': np.mean(wind_directions) if wind_directions else 0,
                        
                        # Облачность и видимость
                        'avg_cloud_cover': np.mean(cloud_cover) if cloud_cover else 50,
                        'min_visibility': min(visibility) if visibility else 10000,
                        'avg_pressure': np.mean(pressure) if pressure else 1013,
                        'max_uv': max(uv_index) if uv_index else 5,
                        
                        # Погодные условия
                        'weather_code': max(set(weather_codes), key=weather_codes.count) if weather_codes else 0,
                        'condition': self._weather_code_to_condition(max(set(weather_codes), key=weather_codes.count) if weather_codes else 0)
                    }
                    
                    # Добавляем индексы для анализа (с проверкой)
                    try:
                        weather_summary.update({
                            'heat_index': self._calculate_heat_index(weather_summary),
                            'comfort_index': self._calculate_comfort_index(weather_summary),
                            'courier_safety': self._calculate_courier_safety(weather_summary),
                            'customer_mood': self._calculate_customer_mood(weather_summary)
                        })
                    except Exception as e:
                        # Если ошибка в расчете индексов, используем значения по умолчанию
                        weather_summary.update({
                            'heat_index': 3,
                            'comfort_index': 3,
                            'courier_safety': 7,
                            'customer_mood': 5
                        })
                    
                    return weather_summary
            
            return None
                
        except Exception as e:
            print(f"⚠️ Ошибка получения погоды: {e}")
            return None
    
    def _weather_code_to_condition(self, code):
        """Детальная классификация погодных условий"""
        conditions = {
            0: 'Clear_Sky',
            1: 'Mainly_Clear', 2: 'Partly_Cloudy', 3: 'Overcast',
            45: 'Fog', 48: 'Depositing_Rime_Fog',
            51: 'Light_Drizzle', 53: 'Moderate_Drizzle', 55: 'Dense_Drizzle',
            56: 'Light_Freezing_Drizzle', 57: 'Dense_Freezing_Drizzle',
            61: 'Slight_Rain', 63: 'Moderate_Rain', 65: 'Heavy_Rain',
            66: 'Light_Freezing_Rain', 67: 'Heavy_Freezing_Rain',
            71: 'Slight_Snow', 73: 'Moderate_Snow', 75: 'Heavy_Snow',
            77: 'Snow_Grains',
            80: 'Slight_Rain_Showers', 81: 'Moderate_Rain_Showers', 82: 'Violent_Rain_Showers',
            85: 'Slight_Snow_Showers', 86: 'Heavy_Snow_Showers',
            95: 'Thunderstorm', 96: 'Thunderstorm_Light_Hail', 99: 'Thunderstorm_Heavy_Hail'
        }
        return conditions.get(code, 'Unknown')
    
    def _calculate_heat_index(self, weather):
        """Индекс жары (влияние на желание заказывать)"""
        temp = weather.get('feels_like', weather.get('avg_temp', 28))
        
        if temp < 26:
            return 1  # Прохладно - возможно больше выходят
        elif temp < 30:
            return 2  # Комфортно
        elif temp < 34:
            return 3  # Тепло - начинают больше заказывать?
        elif temp < 38:
            return 4  # Жарко - определенно заказывают больше
        else:
            return 5  # Экстремально жарко
    
    def _calculate_comfort_index(self, weather):
        """Общий индекс комфорта"""
        score = 5  # Базовый комфорт
        
        # Температура
        temp = weather.get('avg_temp', 28)
        if temp < 22 or temp > 35:
            score -= 2
        elif temp < 24 or temp > 32:
            score -= 1
            
        # Влажность
        humidity = weather.get('avg_humidity', 75)
        if humidity > 85:
            score -= 1
        elif humidity > 90:
            score -= 2
            
        # Дождь
        rain = weather.get('total_rain', 0)
        if rain > 10:
            score -= 2
        elif rain > 2:
            score -= 1
            
        # Ветер
        wind = weather.get('max_wind', 5)
        if wind > 25:
            score -= 1
            
        return max(1, score)
    
    def _calculate_courier_safety(self, weather):
        """Безопасность для курьеров (1-10)"""
        safety = 10
        
        # Дождь - основная опасность
        rain = weather.get('total_rain', 0)
        if rain > 15:
            safety -= 4
        elif rain > 5:
            safety -= 2
        elif rain > 1:
            safety -= 1
            
        # Ветер
        wind = weather.get('max_wind', 5)
        if wind > 30:
            safety -= 3
        elif wind > 20:
            safety -= 2
        elif wind > 15:
            safety -= 1
            
        # Видимость
        visibility = weather.get('min_visibility', 10000)
        if visibility < 1000:
            safety -= 2
        elif visibility < 5000:
            safety -= 1
            
        # Грозы
        condition = weather.get('condition', 'Clear_Sky')
        if 'Thunderstorm' in condition:
            safety -= 3
            
        return max(1, safety)
    
    def _calculate_customer_mood(self, weather):
        """Настроение клиентов (влияние на заказы)"""
        mood = 5  # Нейтральное
        
        # Хорошая погода = больше активности
        condition = weather.get('condition', 'Clear_Sky')
        if condition == 'Clear_Sky':
            mood += 2
        elif condition == 'Mainly_Clear':
            mood += 1
            
        # Жара = больше заказов домой
        heat_index = weather.get('heat_index', 3)
        if heat_index >= 4:
            mood += 1  # Жарко = заказывают больше
            
        # Дождь = сидят дома
        rain = weather.get('total_rain', 0)
        if rain > 5:
            mood += 1  # Дождь = больше заказов
        elif rain > 15:
            mood += 2  # Сильный дождь = точно дома
            
        # Плохая видимость
        visibility = weather.get('min_visibility', 10000)
        if visibility < 5000:
            mood -= 1
            
        return max(1, min(10, mood))
    
    def run_comprehensive_analysis(self, sample_size=3000):
        """
        ПОЛНЫЙ АНАЛИЗ ВСЕХ ПОГОДНЫХ ЗАКОНОМЕРНОСТЕЙ
        """
        print("🌍 ЗАПУСК ПОЛНОГО АНАЛИЗА ПОГОДНЫХ ЗАКОНОМЕРНОСТЕЙ")
        print("=" * 60)
        
        # Загружаем данные
        locations = self.load_restaurant_locations()
        delivery_data = self.get_comprehensive_delivery_data()
        
        if not locations:
            print("❌ Нет данных о координатах ресторанов")
            return None
        
        # Фильтруем рестораны с координатами
        restaurants_with_coords = [name for name in delivery_data['restaurant_name'].unique() 
                                 if name in locations]
        
        print(f"📍 Ресторанов с координатами: {len(restaurants_with_coords)}")
        
        # Создаем выборку
        filtered_data = delivery_data[delivery_data['restaurant_name'].isin(restaurants_with_coords)]
        
        if len(filtered_data) > sample_size:
            sample_data = filtered_data.sample(n=sample_size, random_state=42)
            print(f"🎯 Анализируем выборку: {sample_size:,} записей из {len(filtered_data):,}")
        else:
            sample_data = filtered_data
            print(f"🎯 Анализируем все данные: {len(sample_data):,} записей")
        
        # Собираем данные
        comprehensive_data = []
        processed = 0
        
        print("🌤️ Сбор ПОЛНЫХ погодных данных...")
        
        for _, row in sample_data.iterrows():
            restaurant_name = row['restaurant_name']
            date = row['date']
            
            if restaurant_name in locations:
                location = locations[restaurant_name]
                weather = self.get_comprehensive_weather(
                    location['latitude'], 
                    location['longitude'], 
                    date
                )
                
                if weather:
                    record = {
                        # Основная информация
                        'restaurant': restaurant_name,
                        'date': date,
                        'zone': location.get('zone', 'Unknown'),
                        'area': location.get('area', 'Unknown'),
                        'day_of_week': row['day_of_week'],
                        
                        # Бизнес-метрики
                        'total_sales': row['total_sales'],
                        'total_orders': row['total_orders'],
                        'avg_order_value': row['avg_order_value'],
                        'grab_sales': row['grab_sales'] or 0,
                        'gojek_sales': row['gojek_sales'] or 0,
                        'cancelled_orders': row['grab_cancelled'] or 0,
                        'rating': row['rating'] or 0,
                        
                        # Операционные проблемы
                        'busy_day': row['grab_busy'] or 0,
                        'closed_day': row['grab_closed'] or 0,
                        'out_of_stock': row['grab_out_of_stock'] or 0,
                    }
                    
                    # Добавляем все погодные данные
                    record.update(weather)
                    comprehensive_data.append(record)
            
            processed += 1
            if processed % 500 == 0:
                print(f"   Обработано: {processed:,}/{len(sample_data):,}")
        
        if not comprehensive_data:
            print("❌ Не удалось получить погодные данные")
            return None
        
        # Анализируем результаты
        df = pd.DataFrame(comprehensive_data)
        print(f"\n✅ Собрано данных для ПОЛНОГО анализа: {len(df):,} записей")
        
        return self._analyze_all_weather_patterns(df)
    
    def _analyze_all_weather_patterns(self, df):
        """Анализирует ВСЕ погодные закономерности"""
        print("\n🌍 АНАЛИЗ ВСЕХ ПОГОДНЫХ ЗАКОНОМЕРНОСТЕЙ")
        print("=" * 50)
        
        results = {}
        
        # 1. БАЗОВАЯ СТАТИСТИКА
        print(f"💰 БАЗОВЫЕ ПОКАЗАТЕЛИ:")
        avg_sales = df['total_sales'].mean()
        avg_orders = df['total_orders'].mean()
        avg_aov = df['avg_order_value'].mean()
        
        print(f"   • Средние продажи: {avg_sales:,.0f} IDR/день")
        print(f"   • Средние заказы: {avg_orders:.1f}/день")
        print(f"   • Средний чек: {avg_aov:,.0f} IDR")
        
        # 2. ТЕМПЕРАТУРНЫЙ АНАЛИЗ
        print(f"\n🌡️ ВЛИЯНИЕ ТЕМПЕРАТУРЫ:")
        temp_analysis = {}
        
        temp_ranges = [
            (0, 24, "Прохладно"),
            (24, 28, "Комфортно"), 
            (28, 32, "Тепло"),
            (32, 36, "Жарко"),
            (36, 50, "Очень жарко")
        ]
        
        for min_temp, max_temp, desc in temp_ranges:
            temp_data = df[(df['avg_temp'] >= min_temp) & (df['avg_temp'] < max_temp)]
            if len(temp_data) >= 10:
                temp_avg_sales = temp_data['total_sales'].mean()
                temp_avg_orders = temp_data['total_orders'].mean()
                sales_impact = ((temp_avg_sales - avg_sales) / avg_sales * 100)
                orders_impact = ((temp_avg_orders - avg_orders) / avg_orders * 100)
                
                temp_analysis[desc] = {
                    'sales_impact': sales_impact,
                    'orders_impact': orders_impact,
                    'count': len(temp_data),
                    'avg_temp': temp_data['avg_temp'].mean()
                }
                
                print(f"   🌡️ {desc} ({temp_data['avg_temp'].mean():.1f}°C): продажи {sales_impact:+.1f}%, заказы {orders_impact:+.1f}% ({len(temp_data)} дней)")
        
        # 3. АНАЛИЗ ДОЖДЯ (ДЕТАЛЬНЫЙ)
        print(f"\n🌧️ ДЕТАЛЬНЫЙ АНАЛИЗ ДОЖДЯ:")
        rain_analysis = {}
        
        rain_ranges = [
            (0, 0.1, "Сухо"),
            (0.1, 2, "Легкий дождь"),
            (2, 8, "Умеренный дождь"),
            (8, 20, "Сильный дождь"),
            (20, 100, "Ливень")
        ]
        
        for min_rain, max_rain, desc in rain_ranges:
            rain_data = df[(df['total_rain'] >= min_rain) & (df['total_rain'] < max_rain)]
            if len(rain_data) >= 5:
                rain_avg_sales = rain_data['total_sales'].mean()
                rain_avg_cancelled = rain_data['cancelled_orders'].mean()
                sales_impact = ((rain_avg_sales - avg_sales) / avg_sales * 100)
                
                rain_analysis[desc] = {
                    'sales_impact': sales_impact,
                    'avg_cancelled': rain_avg_cancelled,
                    'count': len(rain_data)
                }
                
                print(f"   🌧️ {desc}: продажи {sales_impact:+.1f}%, отмены {rain_avg_cancelled:.1f}/день ({len(rain_data)} дней)")
        
        # 4. АНАЛИЗ ВЕТРА
        print(f"\n💨 ВЛИЯНИЕ ВЕТРА:")
        wind_analysis = {}
        
        wind_ranges = [
            (0, 10, "Штиль"),
            (10, 20, "Легкий ветер"),
            (20, 30, "Умеренный ветер"),
            (30, 50, "Сильный ветер")
        ]
        
        for min_wind, max_wind, desc in wind_ranges:
            wind_data = df[(df['max_wind'] >= min_wind) & (df['max_wind'] < max_wind)]
            if len(wind_data) >= 10:
                wind_avg_sales = wind_data['total_sales'].mean()
                sales_impact = ((wind_avg_sales - avg_sales) / avg_sales * 100)
                
                wind_analysis[desc] = {
                    'sales_impact': sales_impact,
                    'count': len(wind_data)
                }
                
                print(f"   💨 {desc}: продажи {sales_impact:+.1f}% ({len(wind_data)} дней)")
        
        # 5. АНАЛИЗ КОМФОРТА
        print(f"\n😊 ИНДЕКС КОМФОРТА VS ПРОДАЖИ:")
        comfort_analysis = {}
        
        for comfort_level in range(1, 6):
            comfort_data = df[df['comfort_index'] == comfort_level]
            if len(comfort_data) >= 5:
                comfort_avg_sales = comfort_data['total_sales'].mean()
                sales_impact = ((comfort_avg_sales - avg_sales) / avg_sales * 100)
                
                comfort_analysis[comfort_level] = {
                    'sales_impact': sales_impact,
                    'count': len(comfort_data)
                }
                
                comfort_desc = ["", "Очень неком.", "Неком.", "Норм.", "Комф.", "Отлично"][comfort_level]
                print(f"   😊 {comfort_level}/5 ({comfort_desc}): продажи {sales_impact:+.1f}% ({len(comfort_data)} дней)")
        
        # 6. АНАЛИЗ НАСТРОЕНИЯ КЛИЕНТОВ
        print(f"\n🎭 НАСТРОЕНИЕ КЛИЕНТОВ VS ЗАКАЗЫ:")
        mood_analysis = {}
        
        for mood_level in range(1, 11):
            mood_data = df[df['customer_mood'] == mood_level]
            if len(mood_data) >= 3:
                mood_avg_orders = mood_data['total_orders'].mean()
                orders_impact = ((mood_avg_orders - avg_orders) / avg_orders * 100)
                
                mood_analysis[mood_level] = {
                    'orders_impact': orders_impact,
                    'count': len(mood_data)
                }
                
                if len(mood_data) >= 5:  # Показываем только значимые
                    print(f"   🎭 Настроение {mood_level}/10: заказы {orders_impact:+.1f}% ({len(mood_data)} дней)")
        
        # 7. АНАЛИЗ ПО ЗОНАМ
        print(f"\n🌍 ПОГОДНЫЕ РАЗЛИЧИЯ ПО ЗОНАМ:")
        zone_analysis = {}
        
        for zone in df['zone'].unique():
            zone_data = df[df['zone'] == zone]
            if len(zone_data) >= 30:
                print(f"\n   📍 {zone} ЗОНА ({len(zone_data)} наблюдений):")
                
                # Температурная чувствительность зоны
                zone_temp_corr = zone_data['avg_temp'].corr(zone_data['total_sales'])
                zone_rain_corr = zone_data['total_rain'].corr(zone_data['total_sales'])
                zone_wind_corr = zone_data['max_wind'].corr(zone_data['total_sales'])
                
                print(f"      🌡️ Корреляция температуры: {zone_temp_corr:.3f}")
                print(f"      🌧️ Корреляция дождя: {zone_rain_corr:.3f}")
                print(f"      💨 Корреляция ветра: {zone_wind_corr:.3f}")
                
                zone_analysis[zone] = {
                    'temp_correlation': zone_temp_corr,
                    'rain_correlation': zone_rain_corr,
                    'wind_correlation': zone_wind_corr,
                    'observations': len(zone_data)
                }
        
        # 8. ДЕНЬ НЕДЕЛИ + ПОГОДА
        print(f"\n📅 ПОГОДА VS ДЕНЬ НЕДЕЛИ:")
        weekday_weather = {}
        
        for day in ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']:
            day_data = df[df['day_of_week'] == day]
            if len(day_data) >= 20:
                # Влияние дождя в этот день недели
                day_rainy = day_data[day_data['total_rain'] > 2]
                day_dry = day_data[day_data['total_rain'] <= 2]
                
                if len(day_rainy) >= 5 and len(day_dry) >= 5:
                    rain_impact = ((day_rainy['total_sales'].mean() - day_dry['total_sales'].mean()) / 
                                 day_dry['total_sales'].mean() * 100)
                    
                    weekday_weather[day] = rain_impact
                    print(f"   📅 {day}: дождь влияет на {rain_impact:+.1f}%")
        
        # 9. КОРРЕЛЯЦИОННЫЙ АНАЛИЗ
        print(f"\n📊 КОРРЕЛЯЦИИ (ВСЕ ФАКТОРЫ):")
        correlations = {
            'temperature_sales': df['avg_temp'].corr(df['total_sales']),
            'temperature_orders': df['avg_temp'].corr(df['total_orders']),
            'rain_sales': df['total_rain'].corr(df['total_sales']),
            'rain_cancelled': df['total_rain'].corr(df['cancelled_orders']),
            'wind_sales': df['max_wind'].corr(df['total_sales']),
            'humidity_sales': df['avg_humidity'].corr(df['total_sales']),
            'comfort_sales': df['comfort_index'].corr(df['total_sales']),
            'mood_orders': df['customer_mood'].corr(df['total_orders']),
            'heat_index_sales': df['heat_index'].corr(df['total_sales']),
            'courier_safety_sales': df['courier_safety'].corr(df['total_sales'])
        }
        
        for corr_name, corr_value in correlations.items():
            if abs(corr_value) > 0.05:  # Показываем все заметные корреляции
                print(f"   📊 {corr_name}: {corr_value:.3f}")
        
        # 10. ТОП НАХОДКИ
        print(f"\n🔍 ТОП ПОГОДНЫЕ НАХОДКИ:")
        
        # Находим самые сильные эффекты
        findings = []
        
        # Проверяем температурные эффекты
        for temp_desc, temp_data in temp_analysis.items():
            if abs(temp_data['sales_impact']) > 10:
                findings.append(f"🌡️ {temp_desc}: {temp_data['sales_impact']:+.1f}% продаж")
        
        # Проверяем дождевые эффекты  
        for rain_desc, rain_data in rain_analysis.items():
            if abs(rain_data['sales_impact']) > 15:
                findings.append(f"🌧️ {rain_desc}: {rain_data['sales_impact']:+.1f}% продаж")
        
        # Проверяем ветровые эффекты
        for wind_desc, wind_data in wind_analysis.items():
            if abs(wind_data['sales_impact']) > 8:
                findings.append(f"💨 {wind_desc}: {wind_data['sales_impact']:+.1f}% продаж")
        
        for i, finding in enumerate(findings[:5], 1):
            print(f"   {i}. {finding}")
        
        if not findings:
            print("   💡 Все эффекты умеренные - стабильный бизнес!")
        
        # Сохраняем результаты
        results = {
            'temperature_analysis': temp_analysis,
            'rain_analysis': rain_analysis,
            'wind_analysis': wind_analysis,
            'comfort_analysis': comfort_analysis,
            'mood_analysis': mood_analysis,
            'zone_analysis': zone_analysis,
            'weekday_weather': weekday_weather,
            'correlations': correlations,
            'top_findings': findings,
            'total_observations': len(df),
            'restaurants_analyzed': df['restaurant'].nunique(),
            'date_range': (df['date'].min(), df['date'].max())
        }
        
        return results

def main():
    """Запуск полного анализа"""
    analyzer = ComprehensiveWeatherAnalyzer()
    
    print("🌍 ПОЛНЫЙ АНАЛИЗ ВСЕХ ПОГОДНЫХ ЗАКОНОМЕРНОСТЕЙ")
    print("Выявляем ВСЕ скрытые паттерны влияния погоды на delivery!")
    print("=" * 65)
    
    # Запускаем полный анализ
    results = analyzer.run_comprehensive_analysis(sample_size=2000)
    
    if results:
        print("\n🎉 ПОЛНЫЙ АНАЛИЗ ЗАВЕРШЕН!")
        print(f"📊 Проанализировано: {results['total_observations']:,} наблюдений")
        print(f"🏪 Ресторанов: {results['restaurants_analyzed']}")
        print(f"📅 Период: {results['date_range'][0]} → {results['date_range'][1]}")
        
        print(f"\n🔍 НАЙДЕНО ЗАКОНОМЕРНОСТЕЙ: {len(results['top_findings'])}")
        
        print("\n💡 ТЕПЕРЬ МЫ ЗНАЕМ:")
        print("   🌡️ Как температура влияет на желание заказывать")
        print("   🌧️ Детальное влияние всех типов дождя")
        print("   💨 Как ветер влияет на курьеров и продажи")
        print("   😊 Связь комфорта погоды и активности клиентов")
        print("   🌍 Различия по зонам Бали")
        print("   📅 Как день недели модифицирует погодные эффекты")
        
        # Сохраняем результаты
        try:
            os.makedirs('data', exist_ok=True)
            with open('data/comprehensive_weather_analysis.json', 'w', encoding='utf-8') as f:
                # Конвертируем numpy типы для JSON
                def convert_numpy(obj):
                    if isinstance(obj, np.integer):
                        return int(obj)
                    elif isinstance(obj, np.floating):
                        return float(obj)
                    elif isinstance(obj, np.ndarray):
                        return obj.tolist()
                    return obj
                
                import json
                json.dump(results, f, ensure_ascii=False, indent=2, default=convert_numpy)
            print(f"\n💾 Результаты сохранены: data/comprehensive_weather_analysis.json")
        except Exception as e:
            print(f"⚠️ Ошибка сохранения: {e}")

if __name__ == "__main__":
    main()