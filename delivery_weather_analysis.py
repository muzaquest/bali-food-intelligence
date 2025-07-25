#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ГЛОБАЛЬНЫЙ АНАЛИЗ ВЛИЯНИЯ ПОГОДЫ НА DELIVERY-БИЗНЕС
=================================================

Специализированный анализ для delivery-ресторанов с учетом:
- Влияния погоды на работу курьеров (байкеры)
- Проблем с доставкой в дождь
- Особенностей Grab/Gojek платформ
- Операционных сложностей (busy_days, cancelled_orders)

Ключевые гипотезы для проверки:
1. Дождь снижает продажи из-за недоступности курьеров
2. Сильный дождь = больше отмененных заказов
3. Разные зоны по-разному страдают от погоды
4. Праздники влияют на доступность курьеров
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

class DeliveryWeatherAnalyzer:
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
    
    def get_delivery_data_with_operations(self):
        """Получает данные с операционными метриками для delivery-анализа"""
        conn = sqlite3.connect(self.db_path)
        
        # Расширенный запрос с операционными данными
        query = """
        SELECT 
            r.name as restaurant_name,
            COALESCE(g.stat_date, gj.stat_date) as date,
            COALESCE(g.sales, 0) + COALESCE(gj.sales, 0) as total_sales,
            COALESCE(g.orders, 0) + COALESCE(gj.orders, 0) as total_orders,
            
            -- Grab операционные данные
            g.cancelled_orders as grab_cancelled,
            g.store_is_busy as grab_busy,
            g.store_is_closed as grab_closed,
            g.out_of_stock as grab_out_of_stock,
            
            -- Gojek операционные данные  
            gj.accepting_time as gojek_accepting_time,
            gj.preparation_time as gojek_prep_time,
            
            -- Средний чек для анализа поведения клиентов
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
        ORDER BY r.name, date
        """
        
        df = pd.read_sql_query(query, conn)
        conn.close()
        
        print(f"📊 Загружено delivery-данных:")
        print(f"   • Ресторанов: {df['restaurant_name'].nunique()}")
        print(f"   • Записей: {len(df):,}")
        print(f"   • Период: {df['date'].min()} → {df['date'].max()}")
        
        # Добавляем операционные флаги
        df['has_operational_issues'] = (
            (df['grab_cancelled'].fillna(0) > 0) |
            (df['grab_busy'].fillna(0) > 0) |
            (df['grab_closed'].fillna(0) > 0) |
            (df['grab_out_of_stock'].fillna(0) > 0)
        )
        
        print(f"   • Дней с операционными проблемами: {df['has_operational_issues'].sum():,}")
        
        return df
    
    def get_weather_for_location(self, lat, lon, date):
        """Получает погоду с акцентом на условия для курьеров"""
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
                    
                    avg_temp = sum(temps) / len(temps) if temps else 28
                    total_rain = sum(precipitation) if precipitation else 0
                    max_wind = max(wind_speeds) if wind_speeds else 5
                    avg_humidity = sum(humidity) / len(humidity) if humidity else 75
                    
                    # Определяем условия для курьеров
                    main_weather_code = max(set(weather_codes), key=weather_codes.count) if weather_codes else 0
                    condition = self._weather_code_to_condition(main_weather_code)
                    
                    # Оценка сложности для курьеров (0-10)
                    courier_difficulty = self._calculate_courier_difficulty(
                        condition, total_rain, max_wind, avg_temp, avg_humidity
                    )
                    
                    return {
                        'temperature': avg_temp,
                        'condition': condition,
                        'rain': total_rain,
                        'wind_speed': max_wind,
                        'humidity': avg_humidity,
                        'courier_difficulty': courier_difficulty,
                        'weather_code': main_weather_code
                    }
            
            return None
                
        except Exception as e:
            return None
    
    def _calculate_courier_difficulty(self, condition, rain, wind, temp, humidity):
        """Рассчитывает сложность работы для курьеров (0-10 баллов)"""
        difficulty = 0
        
        # Дождь - основной фактор
        if rain > 10:  # Сильный дождь
            difficulty += 4
        elif rain > 5:  # Умеренный дождь
            difficulty += 3
        elif rain > 1:  # Легкий дождь
            difficulty += 2
        elif rain > 0:  # Морось
            difficulty += 1
            
        # Ветер
        if wind > 25:  # Сильный ветер
            difficulty += 2
        elif wind > 15:  # Умеренный ветер
            difficulty += 1
            
        # Температура
        if temp > 35:  # Очень жарко
            difficulty += 1
        elif temp < 20:  # Холодно для тропиков
            difficulty += 1
            
        # Влажность + жара
        if humidity > 85 and temp > 30:
            difficulty += 1
            
        # Тип погоды
        if condition == 'Thunderstorm':
            difficulty += 2
        elif condition == 'Rain':
            difficulty += 1
            
        return min(difficulty, 10)  # Максимум 10 баллов
    
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
    
    def analyze_delivery_weather_impact(self, sample_size=2000):
        """
        Анализ влияния погоды на delivery-бизнес
        
        Фокус на:
        - Доступность курьеров в разную погоду
        - Операционные проблемы vs погода
        - Отмененные заказы vs дождь
        """
        print("🚴‍♂️ ЗАПУСК DELIVERY-АНАЛИЗА ВЛИЯНИЯ ПОГОДЫ")
        print("=" * 55)
        
        # Загружаем данные
        locations = self.load_restaurant_locations()
        delivery_data = self.get_delivery_data_with_operations()
        
        if not locations:
            print("❌ Нет данных о координатах ресторанов")
            return None
        
        # Фильтруем рестораны с координатами
        restaurants_with_coords = [name for name in delivery_data['restaurant_name'].unique() 
                                 if name in locations]
        
        print(f"📍 Delivery-ресторанов с координатами: {len(restaurants_with_coords)}")
        
        # Создаем выборку для анализа
        filtered_data = delivery_data[delivery_data['restaurant_name'].isin(restaurants_with_coords)]
        
        # Берем выборку для анализа
        if len(filtered_data) > sample_size:
            sample_data = filtered_data.sample(n=sample_size, random_state=42)
            print(f"🎯 Анализируем выборку: {sample_size} записей из {len(filtered_data):,}")
        else:
            sample_data = filtered_data
            print(f"🎯 Анализируем все данные: {len(sample_data):,} записей")
        
        # Собираем данные погоды и delivery-метрик
        delivery_weather_data = []
        processed = 0
        
        print("🌤️ Сбор погодных данных для delivery-анализа...")
        
        for _, row in sample_data.iterrows():
            restaurant_name = row['restaurant_name']
            date = row['date']
            
            if restaurant_name in locations:
                location = locations[restaurant_name]
                weather = self.get_weather_for_location(
                    location['latitude'], 
                    location['longitude'], 
                    date
                )
                
                if weather:
                    delivery_weather_data.append({
                        'restaurant': restaurant_name,
                        'date': date,
                        'zone': location.get('zone', 'Unknown'),
                        'area': location.get('area', 'Unknown'),
                        
                        # Основные метрики
                        'sales': row['total_sales'],
                        'orders': row['total_orders'],
                        'avg_order_value': row['avg_order_value'],
                        
                        # Операционные проблемы
                        'cancelled_orders': row['grab_cancelled'] or 0,
                        'busy_day': row['grab_busy'] or 0,
                        'closed_day': row['grab_closed'] or 0,
                        'out_of_stock': row['grab_out_of_stock'] or 0,
                        'has_issues': row['has_operational_issues'],
                        
                        # Погодные условия
                        'temperature': weather['temperature'],
                        'condition': weather['condition'],
                        'rain': weather['rain'],
                        'wind_speed': weather['wind_speed'],
                        'courier_difficulty': weather['courier_difficulty']
                    })
            
            processed += 1
            if processed % 200 == 0:
                print(f"   Обработано: {processed}/{len(sample_data)}")
        
        if not delivery_weather_data:
            print("❌ Не удалось получить данные о погоде")
            return None
        
        # Анализируем результаты
        df = pd.DataFrame(delivery_weather_data)
        print(f"\n✅ Собрано данных для delivery-анализа: {len(df)} записей")
        
        return self._analyze_delivery_patterns(df)
    
    def _analyze_delivery_patterns(self, df):
        """Анализирует паттерны влияния погоды на delivery"""
        print("\n🚴‍♂️ АНАЛИЗ ВЛИЯНИЯ ПОГОДЫ НА DELIVERY-БИЗНЕС")
        print("=" * 50)
        
        results = {}
        
        # 1. Основной анализ продаж vs погода
        overall_avg_sales = df['sales'].mean()
        overall_avg_orders = df['orders'].mean()
        
        print(f"💰 Средние показатели delivery:")
        print(f"   • Продажи: {overall_avg_sales:,.0f} IDR/день")
        print(f"   • Заказы: {overall_avg_orders:.1f} заказов/день")
        
        # 2. Анализ по сложности для курьеров
        print(f"\n🚴‍♂️ Влияние сложности условий для курьеров:")
        
        courier_impact = {}
        for difficulty_level in [0, 1, 2, 3, 4, 5]:
            level_data = df[(df['courier_difficulty'] >= difficulty_level) & 
                          (df['courier_difficulty'] < difficulty_level + 1)]
            
            if len(level_data) >= 5:
                avg_sales = level_data['sales'].mean()
                avg_orders = level_data['orders'].mean()
                sales_impact = ((avg_sales - overall_avg_sales) / overall_avg_sales * 100)
                orders_impact = ((avg_orders - overall_avg_orders) / overall_avg_orders * 100)
                
                courier_impact[difficulty_level] = {
                    'sales_impact': sales_impact,
                    'orders_impact': orders_impact,
                    'count': len(level_data)
                }
                
                difficulty_desc = ["Отличные", "Хорошие", "Нормальные", "Сложные", "Тяжелые", "Экстремальные"][difficulty_level]
                print(f"   {difficulty_level}⭐ {difficulty_desc} условия: продажи {sales_impact:+.1f}%, заказы {orders_impact:+.1f}% ({len(level_data)} дней)")
        
        # 3. Анализ дождя и отмененных заказов
        print(f"\n🌧️ Дождь vs отмененные заказы:")
        
        rainy_days = df[df['rain'] > 1]  # Дни с дождем >1мм
        dry_days = df[df['rain'] <= 1]
        
        if len(rainy_days) > 0 and len(dry_days) > 0:
            rain_cancelled = rainy_days['cancelled_orders'].mean()
            dry_cancelled = dry_days['cancelled_orders'].mean()
            
            rain_sales = rainy_days['sales'].mean()
            dry_sales = dry_days['sales'].mean()
            
            cancelled_increase = ((rain_cancelled - dry_cancelled) / dry_cancelled * 100) if dry_cancelled > 0 else 0
            sales_decrease = ((rain_sales - dry_sales) / dry_sales * 100) if dry_sales > 0 else 0
            
            print(f"   • Дождливые дни: {rain_cancelled:.1f} отмен/день, продажи {rain_sales:,.0f} IDR")
            print(f"   • Сухие дни: {dry_cancelled:.1f} отмен/день, продажи {dry_sales:,.0f} IDR")
            print(f"   • Дождь увеличивает отмены на {cancelled_increase:+.1f}%")
            print(f"   • Дождь снижает продажи на {abs(sales_decrease):.1f}%")
        
        # 4. Анализ по зонам (delivery-специфика)
        print(f"\n🌍 Влияние погоды по зонам (delivery-аспект):")
        zone_analysis = {}
        
        for zone in df['zone'].unique():
            zone_data = df[df['zone'] == zone]
            if len(zone_data) >= 20:
                zone_avg_sales = zone_data['sales'].mean()
                zone_avg_cancelled = zone_data['cancelled_orders'].mean()
                
                print(f"\n   📍 {zone} зона ({len(zone_data)} наблюдений):")
                print(f"      💰 Средние продажи: {zone_avg_sales:,.0f} IDR")
                print(f"      ❌ Средние отмены: {zone_avg_cancelled:.1f}/день")
                
                # Анализ дождя в зоне
                zone_rainy = zone_data[zone_data['rain'] > 1]
                zone_dry = zone_data[zone_data['rain'] <= 1]
                
                if len(zone_rainy) > 5 and len(zone_dry) > 5:
                    rain_impact_sales = ((zone_rainy['sales'].mean() - zone_dry['sales'].mean()) / 
                                       zone_dry['sales'].mean() * 100)
                    rain_impact_cancelled = ((zone_rainy['cancelled_orders'].mean() - zone_dry['cancelled_orders'].mean()) / 
                                           zone_dry['cancelled_orders'].mean() * 100) if zone_dry['cancelled_orders'].mean() > 0 else 0
                    
                    print(f"      🌧️ Влияние дождя: продажи {rain_impact_sales:+.1f}%, отмены {rain_impact_cancelled:+.1f}%")
                    
                    zone_analysis[zone] = {
                        'rain_impact_sales': rain_impact_sales,
                        'rain_impact_cancelled': rain_impact_cancelled,
                        'avg_sales': zone_avg_sales,
                        'avg_cancelled': zone_avg_cancelled
                    }
        
        # 5. Корреляции для delivery
        print(f"\n📊 Корреляции (delivery-специфика):")
        
        correlations = {}
        correlations['rain_vs_sales'] = df['rain'].corr(df['sales'])
        correlations['rain_vs_cancelled'] = df['rain'].corr(df['cancelled_orders'])
        correlations['courier_difficulty_vs_sales'] = df['courier_difficulty'].corr(df['sales'])
        correlations['wind_vs_orders'] = df['wind_speed'].corr(df['orders'])
        
        for corr_name, corr_value in correlations.items():
            if abs(corr_value) > 0.1:  # Показываем только значимые
                print(f"   • {corr_name}: {corr_value:.3f}")
        
        # 6. Рекомендации для delivery
        print(f"\n💡 ВЫВОДЫ ДЛЯ DELIVERY-БИЗНЕСА:")
        
        if courier_impact.get(3, {}).get('sales_impact', 0) < -10:
            print("   🚨 КРИТИЧНО: Сложные условия для курьеров снижают продажи >10%")
        
        # Проверяем наличие переменной sales_decrease
        if len(rainy_days) > 0 and len(dry_days) > 0:
            rain_sales = rainy_days['sales'].mean()
            dry_sales = dry_days['sales'].mean()
            sales_decrease = ((rain_sales - dry_sales) / dry_sales * 100) if dry_sales > 0 else 0
            
            if sales_decrease < -15:
                print("   🌧️ ВАЖНО: Дождь критично влияет на доступность курьеров")
            
        print("   🎯 Рекомендации:")
        print("     • Мониторить прогноз погоды для планирования")
        print("     • Разработать стратегию для дождливых дней")
        print("     • Рассмотреть бонусы курьерам в плохую погоду")
        print("     • Предупреждать клиентов о возможных задержках")
        
        results = {
            'courier_difficulty_impact': courier_impact,
            'zone_analysis': zone_analysis,
            'correlations': correlations,
            'total_observations': len(df),
            'restaurants_analyzed': df['restaurant'].nunique(),
            'rainy_days_analyzed': len(rainy_days),
            'avg_cancelled_orders': df['cancelled_orders'].mean()
        }
        
        return results

def main():
    """Демонстрация delivery-анализа"""
    analyzer = DeliveryWeatherAnalyzer()
    
    print("🚴‍♂️ ГЛОБАЛЬНЫЙ АНАЛИЗ ВЛИЯНИЯ ПОГОДЫ НА DELIVERY")
    print("Анализирует влияние погоды на курьеров и доставку")
    print("=" * 60)
    
    # Запускаем анализ с выборкой для демонстрации
    results = analyzer.analyze_delivery_weather_impact(sample_size=1000)
    
    if results:
        print("\n🎉 DELIVERY-АНАЛИЗ ЗАВЕРШЕН!")
        print(f"📊 Проанализировано: {results['total_observations']} наблюдений")
        print(f"🏪 Ресторанов: {results['restaurants_analyzed']}")
        print(f"🌧️ Дождливых дней: {results['rainy_days_analyzed']}")
        print(f"❌ Средние отмены: {results['avg_cancelled_orders']:.1f}/день")
        
        print("\n💡 Теперь мы можем:")
        print("   • Точно предсказывать влияние дождя на курьеров")
        print("   • Планировать операции по прогнозу погоды")
        print("   • Объяснять клиентам реальные причины падений")
        print("   • Разрабатывать стратегии для плохой погоды")

if __name__ == "__main__":
    main()