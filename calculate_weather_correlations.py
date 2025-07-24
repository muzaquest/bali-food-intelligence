#!/usr/bin/env python3
"""
🌤️ РАСЧЕТ РЕАЛЬНЫХ ПОГОДНЫХ КОРРЕЛЯЦИЙ ИЗ OPEN-METEO
Заменяет эмпирические коэффициенты на научно обоснованные данные
"""

import requests
import sqlite3
import json
from datetime import datetime, timedelta
import pandas as pd
import numpy as np

def get_weather_data_range(start_date, end_date, lat=-8.4095, lon=115.1889):
    """Получает погодные данные за период из Open-Meteo"""
    
    print(f"📊 Получение погодных данных: {start_date} → {end_date}")
    
    try:
        # Open-Meteo Historical Weather API
        params = {
            'latitude': lat,
            'longitude': lon,
            'start_date': start_date,
            'end_date': end_date,
            'daily': 'temperature_2m_mean,precipitation_sum,weather_code',
            'timezone': 'Asia/Jakarta'
        }
        
        response = requests.get('https://archive-api.open-meteo.com/v1/archive', 
                              params=params, timeout=30)
        
        if response.status_code == 200:
            data = response.json()
            daily = data.get('daily', {})
            
            weather_data = []
            times = daily.get('time', [])
            temps = daily.get('temperature_2m_mean', [])
            precipitation = daily.get('precipitation_sum', [])
            weather_codes = daily.get('weather_code', [])
            
            for i, date in enumerate(times):
                condition = weather_code_to_condition(weather_codes[i] if i < len(weather_codes) else 0)
                
                weather_data.append({
                    'date': date,
                    'temperature': temps[i] if i < len(temps) else 28,
                    'precipitation': precipitation[i] if i < len(precipitation) else 0,
                    'condition': condition
                })
            
            print(f"✅ Получено {len(weather_data)} дней погодных данных")
            return weather_data
            
    except Exception as e:
        print(f"❌ Ошибка получения погодных данных: {e}")
        return []

def weather_code_to_condition(code):
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
    elif code in [71, 73, 75, 77, 85, 86]:
        return 'Snow'
    elif code in [80, 81, 82]:
        return 'Rain'
    elif code in [95, 96, 99]:
        return 'Thunderstorm'
    else:
        return 'Clear'

def get_sales_data():
    """Получает данные продаж из базы данных"""
    
    print("📊 Загрузка данных продаж из базы данных...")
    
    try:
        conn = sqlite3.connect('database.sqlite')
        
        # Объединяем данные Grab и Gojek
        query = """
        SELECT 
            stat_date as date,
            sales,
            'grab' as source
        FROM grab_stats 
        WHERE sales > 0
        
        UNION ALL
        
        SELECT 
            stat_date as date,
            sales,
            'gojek' as source  
        FROM gojek_stats
        WHERE sales > 0
        
        ORDER BY date
        """
        
        df = pd.read_sql_query(query, conn)
        conn.close()
        
        # Группируем по дням и суммируем продажи
        daily_sales = df.groupby('date')['sales'].sum().reset_index()
        
        print(f"✅ Загружено {len(daily_sales)} дней продаж")
        return daily_sales.to_dict('records')
        
    except Exception as e:
        print(f"❌ Ошибка загрузки данных продаж: {e}")
        return []

def calculate_weather_correlations():
    """Рассчитывает корреляции погода ↔ продажи"""
    
    print("\n🧮 РАСЧЕТ РЕАЛЬНЫХ ПОГОДНЫХ КОРРЕЛЯЦИЙ")
    print("=" * 60)
    
    # Получаем данные продаж
    sales_data = get_sales_data()
    if not sales_data:
        print("❌ Нет данных продаж")
        return
    
    # Определяем период данных
    dates = [item['date'] for item in sales_data]
    start_date = min(dates)
    end_date = max(dates)
    
    print(f"📅 Период анализа: {start_date} → {end_date}")
    
    # Получаем погодные данные за тот же период
    weather_data = get_weather_data_range(start_date, end_date)
    if not weather_data:
        print("❌ Нет погодных данных")
        return
    
    # Объединяем данные
    print("\n🔗 Объединение данных продаж и погоды...")
    
    # Создаем словарь продаж по датам
    sales_dict = {item['date']: item['sales'] for item in sales_data}
    
    combined_data = []
    for weather in weather_data:
        date = weather['date']
        if date in sales_dict:
            combined_data.append({
                'date': date,
                'sales': sales_dict[date],
                'temperature': weather['temperature'],
                'precipitation': weather['precipitation'],
                'condition': weather['condition']
            })
    
    print(f"✅ Объединено {len(combined_data)} дней данных")
    
    if len(combined_data) < 30:
        print("❌ Недостаточно данных для корреляционного анализа")
        return
    
    # Анализ корреляций по погодным условиям
    print("\n📊 АНАЛИЗ ВЛИЯНИЯ ПОГОДНЫХ УСЛОВИЙ:")
    
    df = pd.DataFrame(combined_data)
    
    # Средние продажи по условиям
    condition_stats = df.groupby('condition')['sales'].agg(['mean', 'count']).reset_index()
    overall_mean = df['sales'].mean()
    
    weather_coefficients = {}
    
    for _, row in condition_stats.iterrows():
        condition = row['condition']
        mean_sales = row['mean']
        count = row['count']
        
        if count >= 10:  # Минимум 10 дней для статистической значимости
            impact = (mean_sales - overall_mean) / overall_mean
            weather_coefficients[condition] = impact
            
            print(f"   {condition:12}: {impact:+6.1%} (дней: {count:3d}, продажи: {mean_sales:,.0f})")
    
    # Анализ влияния осадков
    print("\n🌧️ АНАЛИЗ ВЛИЯНИЯ ОСАДКОВ:")
    
    rainy_days = df[df['precipitation'] > 1]  # Дни с дождем >1мм
    clear_days = df[df['precipitation'] == 0]  # Ясные дни
    
    if len(rainy_days) >= 10 and len(clear_days) >= 10:
        rainy_mean = rainy_days['sales'].mean()
        clear_mean = clear_days['sales'].mean()
        rain_impact = (rainy_mean - clear_mean) / clear_mean
        
        weather_coefficients['Rain_vs_Clear'] = rain_impact
        
        print(f"   Дождливые дни: {rainy_mean:,.0f} IDR (дней: {len(rainy_days)})")
        print(f"   Ясные дни:     {clear_mean:,.0f} IDR (дней: {len(clear_days)})")
        print(f"   Влияние дождя: {rain_impact:+.1%}")
    
    # Анализ температурных корреляций
    print("\n🌡️ АНАЛИЗ ТЕМПЕРАТУРНЫХ КОРРЕЛЯЦИЙ:")
    
    temp_corr = df['temperature'].corr(df['sales'])
    print(f"   Корреляция температура ↔ продажи: {temp_corr:.3f}")
    
    # Сохранение результатов
    print("\n💾 СОХРАНЕНИЕ РЕЗУЛЬТАТОВ:")
    
    results = {
        'weather_coefficients': weather_coefficients,
        'temperature_correlation': temp_corr,
        'analysis_period': {
            'start_date': start_date,
            'end_date': end_date,
            'total_days': len(combined_data)
        },
        'data_sources': {
            'weather': 'Open-Meteo (ERA5/ECMWF)',
            'sales': 'Grab + Gojek stats',
            'calculated_at': datetime.now().isoformat()
        }
    }
    
    # Сохраняем в JSON
    with open('weather_correlations.json', 'w', encoding='utf-8') as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    
    print("✅ Результаты сохранены в weather_correlations.json")
    
    # Обновляем real_coefficients.json
    try:
        with open('real_coefficients.json', 'r', encoding='utf-8') as f:
            coefficients = json.load(f)
    except:
        coefficients = {}
    
    coefficients['weather'] = weather_coefficients
    coefficients['temperature_correlation'] = temp_corr
    
    with open('real_coefficients.json', 'w', encoding='utf-8') as f:
        json.dump(coefficients, f, indent=2, ensure_ascii=False)
    
    print("✅ Погодные коэффициенты добавлены в real_coefficients.json")
    
    print("\n🎉 АНАЛИЗ ЗАВЕРШЕН!")
    print(f"📊 Проанализировано {len(combined_data)} дней")
    print(f"🌤️ Найдено {len(weather_coefficients)} погодных факторов")
    print("🔬 Все коэффициенты основаны на реальных данных Open-Meteo")

if __name__ == "__main__":
    calculate_weather_correlations()