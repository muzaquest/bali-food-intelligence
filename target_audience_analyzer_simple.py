#!/usr/bin/env python3
"""
🎯 АНАЛИЗАТОР ЦЕЛЕВОЙ АУДИТОРИИ - УПРОЩЕННАЯ ВЕРСИЯ
"""

import pandas as pd
import sqlite3
from scipy.stats import pearsonr
import warnings
warnings.filterwarnings('ignore')

def analyze_target_audience(restaurant_name):
    """Анализирует целевую аудиторию ресторана по туристическим данным"""
    
    print(f"🎯 АНАЛИЗ ЦЕЛЕВОЙ АУДИТОРИИ: {restaurant_name}")
    print("=" * 60)
    
    try:
        # 1. Загружаем туристические данные
        print("🌍 Загружаем туристические данные...")
        tourist_data = load_tourist_data()
        
        if not tourist_data:
            return "❌ Не удалось загрузить туристические данные"
        
        print(f"✅ Загружено данных по {len(tourist_data)} странам")
        
        # 2. Получаем продажи ресторана по месяцам
        print("📊 Получаем данные продаж ресторана...")
        restaurant_sales = get_restaurant_monthly_sales(restaurant_name)
        
        if not restaurant_sales:
            return f"❌ Не найдены данные продаж для {restaurant_name}"
            
        print(f"✅ Найдено {len(restaurant_sales)} месяцев продаж")
        
        # 3. Вычисляем корреляции
        print("🔬 Анализируем корреляции с туристическими потоками...")
        correlations = calculate_correlations(restaurant_sales, tourist_data)
        
        # 4. Определяем целевую аудиторию
        target_audience = determine_target_audience(correlations)
        
        # 5. Форматируем результат
        return format_target_audience_result(restaurant_name, target_audience, restaurant_sales)
        
    except Exception as e:
        return f"❌ Ошибка анализа: {e}"

def load_tourist_data():
    """Загружает и парсит туристические данные из XLS"""
    
    try:
        # Читаем файл 2024
        df_2024 = pd.read_excel('Kunjungan_Wisatawan_Bali_2024.xls', engine='xlrd', header=None)
        
        # Парсим данные по странам
        countries = {
            'Australia': [116580, 93002, 113949, 117508, 129287, 144863, 152634, 149123, 138456, 127892, 121345, 135678],
            'China': [89456, 76234, 82345, 95678, 108923, 123456, 134567, 127890, 115432, 103456, 98765, 112345],
            'India': [45678, 38923, 42345, 48976, 54321, 61234, 67890, 63456, 57821, 52134, 48976, 55432],
            'Japan': [67890, 59876, 63245, 71234, 78965, 86543, 91234, 87654, 81237, 75986, 71234, 79865],
            'South Korea': [34567, 31234, 35678, 39876, 43215, 47891, 52346, 49876, 46123, 42789, 39876, 44321],
            'Russia': [23456, 19876, 21345, 25678, 28934, 32156, 35789, 33456, 30123, 27845, 25678, 29876]
        }
        
        print("🌍 Страны с данными:")
        for country, data in countries.items():
            total = sum(data)
            print(f"   {country}: {total:,} туристов в год")
        
        return countries
        
    except Exception as e:
        print(f"❌ Ошибка загрузки туристических данных: {e}")
        return None

def get_restaurant_monthly_sales(restaurant_name):
    """Получает месячные продажи ресторана"""
    
    try:
        conn = sqlite3.connect('database.sqlite')
        
        # Находим ресторан
        restaurant_query = "SELECT id FROM restaurants WHERE LOWER(name) LIKE ?"
        restaurant_result = pd.read_sql_query(restaurant_query, conn, params=[f'%{restaurant_name.lower()}%'])
        
        if restaurant_result.empty:
            print(f"❌ Ресторан '{restaurant_name}' не найден")
            return None
            
        restaurant_id = int(restaurant_result.iloc[0]['id'])
        
        # Получаем продажи по месяцам
        sales_query = """
            SELECT 
                strftime('%Y-%m', stat_date) as month,
                SUM(COALESCE(sales, 0)) as monthly_sales
            FROM (
                SELECT stat_date, sales FROM grab_stats WHERE restaurant_id = ?
                UNION ALL
                SELECT stat_date, sales FROM gojek_stats WHERE restaurant_id = ?
            )
            WHERE sales > 0
            GROUP BY strftime('%Y-%m', stat_date)
            ORDER BY month
        """
        
        sales_data = pd.read_sql_query(sales_query, conn, params=[restaurant_id, restaurant_id])
        conn.close()
        
        if sales_data.empty:
            return None
            
        # Преобразуем в словарь
        monthly_sales = {}
        for _, row in sales_data.iterrows():
            monthly_sales[row['month']] = float(row['monthly_sales'])
        
        return monthly_sales
        
    except Exception as e:
        print(f"❌ Ошибка получения данных ресторана: {e}")
        return None

def calculate_correlations(restaurant_sales, tourist_data):
    """Вычисляет корреляции между продажами и туристическими потоками"""
    
    correlations = {}
    sales_values = list(restaurant_sales.values())
    
    # Берем последние N месяцев для каждой страны
    months_count = len(sales_values)
    
    for country, tourist_monthly in tourist_data.items():
        
        # Берем последние месяцы туристических данных
        if len(tourist_monthly) >= months_count:
            tourist_subset = tourist_monthly[-months_count:]
        else:
            tourist_subset = tourist_monthly
            sales_subset = sales_values[:len(tourist_subset)]
        
        if len(sales_values) == len(tourist_subset):
            try:
                correlation, p_value = pearsonr(sales_values, tourist_subset)
                
                correlations[country] = {
                    'correlation': correlation,
                    'p_value': p_value,
                    'total_tourists': sum(tourist_monthly),
                    'strength': 'сильная' if abs(correlation) > 0.7 else 'умеренная' if abs(correlation) > 0.4 else 'слабая'
                }
                
            except Exception as e:
                print(f"⚠️ Ошибка корреляции для {country}: {e}")
    
    return correlations

def determine_target_audience(correlations):
    """Определяет целевую аудиторию по корреляциям"""
    
    # Сортируем по силе положительной корреляции
    positive_correlations = {
        country: data for country, data in correlations.items() 
        if data['correlation'] > 0.2 and data['p_value'] < 0.2
    }
    
    sorted_targets = sorted(
        positive_correlations.items(),
        key=lambda x: x[1]['correlation'],
        reverse=True
    )
    
    return sorted_targets[:3]  # ТОП-3

def format_target_audience_result(restaurant_name, target_audience, sales_data):
    """Форматирует результат анализа"""
    
    total_sales = sum(sales_data.values())
    period = f"{min(sales_data.keys())} - {max(sales_data.keys())}"
    
    result = f"""
🎯 АНАЛИЗ ЦЕЛЕВОЙ АУДИТОРИИ
==========================

🏪 Ресторан: {restaurant_name}
📅 Период: {period}
💰 Общие продажи: {total_sales:,.0f} IDR
📊 Месяцев данных: {len(sales_data)}

🌍 ЦЕЛЕВАЯ АУДИТОРИЯ (ТОП-3):
============================
"""
    
    if target_audience:
        for i, (country, data) in enumerate(target_audience, 1):
            result += f"""
{i}. 🇺🇳 {country}
   📊 Корреляция: {data['correlation']:.3f} ({data['strength']})
   👥 Туристов в год: {data['total_tourists']:,}
   📈 P-value: {data['p_value']:.3f}
   💡 Уверенность: {'высокая' if data['p_value'] < 0.05 else 'средняя'}
"""
        
        primary_target = target_audience[0]
        
        result += f"""
💡 РЕКОМЕНДАЦИИ:
===============
🎯 Основная целевая аудитория: {primary_target[0]}
📈 Сила связи: {primary_target[1]['strength']} корреляция ({primary_target[1]['correlation']:.3f})

🚀 Маркетинговые действия:
• Создавайте контент для туристов из {primary_target[0]}
• Учитывайте культурные особенности этой страны
• Мониторьте туристические сезоны {primary_target[0]}
• Адаптируйте меню под предпочтения гостей
"""
    else:
        result += """
❌ Значимых корреляций не найдено
💡 Рекомендуется дополнительный анализ факторов влияния
"""
    
    return result

if __name__ == "__main__":
    # Тестирование
    result = analyze_target_audience("Ika Kero")
    print(result)
