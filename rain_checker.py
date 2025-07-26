
import sqlite3
import pandas as pd
import numpy as np
import json
import requests

def load_mega_data():
    """Воссоздает данные МЕГА анализа"""
    
    # Загружаем координаты
    with open('data/bali_restaurant_locations.json', 'r', encoding='utf-8') as f:
        locations_data = json.load(f)
    locations = {r['name']: r for r in locations_data['restaurants']}
    
    # Загружаем данные продаж
    conn = sqlite3.connect('database.sqlite')
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
    ORDER BY RANDOM()
    LIMIT 6000
    """
    
    df = pd.read_sql_query(query, conn)
    conn.close()
    
    return df, locations

def get_weather_for_date(lat, lon, date):
    """Получает погоду для конкретной даты"""
    try:
        url = "https://api.open-meteo.com/v1/forecast"
        params = {
            'latitude': lat,
            'longitude': lon,
            'start_date': date,
            'end_date': date,
            'hourly': 'temperature_2m,precipitation,wind_speed_10m',
            'timezone': 'Asia/Jakarta'
        }
        
        response = requests.get(url, params=params, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            hourly = data.get('hourly', {})
            
            if hourly and len(hourly.get('time', [])) > 0:
                precipitation = hourly.get('precipitation', [0])
                return sum(precipitation) if precipitation else 0
        
        return None
    except:
        return None

def analyze_heavy_rain_days():
    """Детально анализирует дни с сильными дождями"""
    
    sales_data, locations = load_mega_data()
    
    print('🔍 ДЕТАЛЬНАЯ ПРОВЕРКА ДНЕЙ С СИЛЬНЫМИ ДОЖДЯМИ')
    print('=' * 50)
    
    # Фильтруем рестораны с координатами
    restaurants_with_coords = [name for name in sales_data['restaurant_name'].unique() 
                              if name in locations]
    
    filtered_data = sales_data[sales_data['restaurant_name'].isin(restaurants_with_coords)]
    unique_combos = filtered_data[['restaurant_name', 'date']].drop_duplicates()
    
    # Берем выборку для проверки
    sample_combos = unique_combos.sample(min(300, len(unique_combos)), random_state=42)
    
    heavy_rain_data = []
    moderate_rain_data = []
    light_rain_data = []
    dry_data = []
    
    print(f'📊 Проверяем {len(sample_combos)} комбинаций...')
    
    for i, (_, row) in enumerate(sample_combos.iterrows()):
        restaurant_name = row['restaurant_name']
        date = row['date']
        
        if restaurant_name in locations:
            location = locations[restaurant_name]
            
            rain = get_weather_for_date(
                location['latitude'], 
                location['longitude'], 
                date
            )
            
            if rain is not None:
                # Получаем продажи
                day_sales = filtered_data[
                    (filtered_data['restaurant_name'] == restaurant_name) & 
                    (filtered_data['date'] == date)
                ]
                
                if not day_sales.empty:
                    total_sales = day_sales['total_sales'].sum()
                    
                    record = {
                        'restaurant': restaurant_name,
                        'date': date,
                        'rain': rain,
                        'sales': total_sales
                    }
                    
                    # Классифицируем по дождю
                    if rain >= 8:  # Сильный дождь
                        heavy_rain_data.append(record)
                    elif rain >= 2:  # Умеренный дождь
                        moderate_rain_data.append(record)
                    elif rain >= 0.1:  # Легкий дождь
                        light_rain_data.append(record)
                    else:  # Сухо
                        dry_data.append(record)
        
        if (i + 1) % 50 == 0:
            print(f'   Обработано: {i + 1}/{len(sample_combos)}')
            import time
            time.sleep(1)
    
    print(f'')
    print(f'📊 РЕЗУЛЬТАТЫ ПРОВЕРКИ:')
    print(f'   🌧️ Сильный дождь (≥8мм): {len(heavy_rain_data)} дней')
    print(f'   🌧️ Умеренный дождь (2-8мм): {len(moderate_rain_data)} дней')
    print(f'   🌧️ Легкий дождь (0.1-2мм): {len(light_rain_data)} дней')
    print(f'   ☀️ Сухо (<0.1мм): {len(dry_data)} дней')
    
    # Анализируем продажи
    if heavy_rain_data:
        print(f'')
        print(f'🔍 ДЕТАЛЬНЫЙ АНАЛИЗ СИЛЬНЫХ ДОЖДЕЙ:')
        print('=' * 40)
        
        # Сортируем по количеству дождя
        heavy_rain_data.sort(key=lambda x: x['rain'], reverse=True)
        
        total_sales = sum(record['sales'] for record in heavy_rain_data)
        avg_sales = total_sales / len(heavy_rain_data)
        
        print(f'📊 Средние продажи в сильный дождь: {avg_sales:,.0f} IDR')
        
        print(f'')
        print(f'📋 ТОП-10 САМЫХ ДОЖДЛИВЫХ ДНЕЙ:')
        for i, record in enumerate(heavy_rain_data[:10], 1):
            print(f'   {i:2d}. {record["date"]} | {record["restaurant"]:25} | {record["rain"]:5.1f}мм | {record["sales"]:8,.0f} IDR')
        
        # Сравниваем с другими категориями
        if dry_data:
            dry_avg = sum(r['sales'] for r in dry_data) / len(dry_data)
            impact = ((avg_sales - dry_avg) / dry_avg * 100)
            print(f'')
            print(f'📊 СРАВНЕНИЕ С СУХИМИ ДНЯМИ:')
            print(f'   ☀️ Средние продажи в сухие дни: {dry_avg:,.0f} IDR')
            print(f'   📈 Влияние сильного дождя: {impact:+.1f}%')
            
            if impact > 5:
                print(f'   ✅ Подтверждение: сильный дождь действительно увеличивает продажи')
            elif impact < -5:
                print(f'   ❌ Опровержение: сильный дождь снижает продажи')
            else:
                print(f'   ➡️ Нейтральный эффект: сильный дождь почти не влияет')
    else:
        print(f'❌ Не найдено дней с сильным дождем в выборке')

if __name__ == "__main__":
    analyze_heavy_rain_days()
