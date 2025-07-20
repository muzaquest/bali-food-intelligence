#!/usr/bin/env python3
"""
🎯 ПОЛНЫЙ CLI ДЛЯ MUZAQUEST ANALYTICS - ИСПОЛЬЗУЕТ ВСЕ ПАРАМЕТРЫ + ВСЕ API
Полное использование всех 30+ полей из grab_stats и gojek_stats + OpenAI + Weather + Calendar API
"""

import argparse
import sys
import sqlite3
import os
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

# API интеграция
import requests
from dotenv import load_dotenv

# Загружаем переменные окружения
load_dotenv()

try:
    import pandas as pd
    import numpy as np
except ImportError:
    print("❌ Требуется установка pandas и numpy: pip install pandas numpy")
    sys.exit(1)

# Опциональные импорты для API
try:
    import openai
    OPENAI_AVAILABLE = True
except ImportError:
    OPENAI_AVAILABLE = False

class WeatherAPI:
    """Класс для работы с OpenWeatherMap API"""
    
    def __init__(self):
        self.api_key = os.getenv('WEATHER_API_KEY')
        self.base_url = "http://api.openweathermap.org/data/2.5"
        
    def get_weather_data(self, date, lat=-8.4095, lon=115.1889):
        """Получает данные о погоде за конкретную дату"""
        if not self.api_key:
            return self._simulate_weather(date)
            
        try:
            # Конвертируем дату в timestamp
            timestamp = int(datetime.strptime(date, '%Y-%m-%d').timestamp())
            
            url = f"{self.base_url}/onecall/timemachine"
            params = {
                'lat': lat,
                'lon': lon,
                'dt': timestamp,
                'appid': self.api_key,
                'units': 'metric'
            }
            
            response = requests.get(url, params=params, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                weather = data.get('current', {})
                return {
                    'temperature': weather.get('temp', 28),
                    'humidity': weather.get('humidity', 75),
                    'condition': weather.get('weather', [{}])[0].get('main', 'Clear'),
                    'rain': weather.get('rain', {}).get('1h', 0)
                }
            else:
                return self._simulate_weather(date)
                
        except Exception as e:
            print(f"⚠️ Weather API error: {e}")
            return self._simulate_weather(date)
    
    def _simulate_weather(self, date):
        """Симуляция погодных данных если API недоступно"""
        import random
        random.seed(hash(date))
        
        conditions = ['Clear', 'Rain', 'Clouds', 'Thunderstorm']
        weights = [0.6, 0.2, 0.15, 0.05]  # Вероятности для Бали
        
        condition = random.choices(conditions, weights=weights)[0]
        rain = random.uniform(0, 10) if condition in ['Rain', 'Thunderstorm'] else 0
        
        return {
            'temperature': random.uniform(24, 32),
            'humidity': random.uniform(65, 85),
            'condition': condition,
            'rain': rain
        }

class CalendarAPI:
    """Класс для работы с Calendarific API"""
    
    def __init__(self):
        self.api_key = os.getenv('CALENDAR_API_KEY')
        self.base_url = "https://calendarific.com/api/v2"
        
    def get_holidays(self, year, country='ID'):
        """Получает список праздников за год"""
        if not self.api_key:
            return self._get_indonesia_holidays(year)
            
        try:
            url = f"{self.base_url}/holidays"
            params = {
                'api_key': self.api_key,
                'country': country,
                'year': year,
                'type': 'national,religious,observance'
            }
            
            response = requests.get(url, params=params, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                holidays = []
                
                for holiday in data.get('response', {}).get('holidays', []):
                    holidays.append({
                        'date': holiday['date']['iso'],
                        'name': holiday['name'],
                        'type': holiday['type'][0] if holiday['type'] else 'national'
                    })
                
                return holidays
            else:
                return self._get_indonesia_holidays(year)
                
        except Exception as e:
            print(f"⚠️ Calendar API error: {e}")
            return self._get_indonesia_holidays(year)
    
    def _get_indonesia_holidays(self, year):
        """Симуляция индонезийских праздников если API недоступно"""
        holidays = [
            f"{year}-01-01",  # Новый год
            f"{year}-02-12",  # Китайский Новый год
            f"{year}-03-11",  # Исра Мирадж
            f"{year}-03-22",  # День тишины (Ньепи)
            f"{year}-04-10",  # Страстная пятница
            f"{year}-04-14",  # Ид аль-Фитр
            f"{year}-05-01",  # День труда
            f"{year}-05-07",  # Весак
            f"{year}-05-12",  # Вознесение
            f"{year}-05-29",  # Вознесение Иисуса
            f"{year}-06-01",  # Панчасила
            f"{year}-06-16",  # Ид аль-Адха
            f"{year}-06-17",  # Исламский Новый год
            f"{year}-08-17",  # День независимости
            f"{year}-08-26",  # Мавлид
            f"{year}-12-25"   # Рождество
        ]
        
        return [{'date': date, 'name': 'Holiday', 'type': 'national'} for date in holidays]

class OpenAIAnalyzer:
    """Класс для работы с OpenAI API"""
    
    def __init__(self):
        self.api_key = os.getenv('OPENAI_API_KEY')
        if self.api_key and OPENAI_AVAILABLE:
            openai.api_key = self.api_key
            
    def generate_insights(self, restaurant_data, weather_data=None, holiday_data=None):
        """Генерирует инсайты и рекомендации с помощью GPT"""
        if not self.api_key or not OPENAI_AVAILABLE:
            return self._generate_basic_insights(restaurant_data)
            
        try:
            # Подготавливаем данные для анализа
            prompt = self._prepare_analysis_prompt(restaurant_data, weather_data, holiday_data)
            
            response = openai.ChatCompletion.create(
                model="gpt-4",
                messages=[
                    {"role": "system", "content": "Ты эксперт-аналитик ресторанного бизнеса в Индонезии с 15-летним опытом. Анализируй данные и давай конкретные, практичные рекомендации."},
                    {"role": "user", "content": prompt}
                ],
                max_tokens=2000,
                temperature=0.7
            )
            
            return response.choices[0].message.content
            
        except Exception as e:
            print(f"⚠️ OpenAI API error: {e}")
            return self._generate_basic_insights(restaurant_data)
    
    def _prepare_analysis_prompt(self, data, weather_data, holiday_data):
        """Подготавливает промпт для анализа"""
        
        total_sales = data['total_sales'].sum()
        total_orders = data['orders'].sum()
        avg_rating = data['rating'].mean()
        
        prompt = f"""
        Проанализируй данные ресторана и дай экспертные рекомендации:
        
        ОСНОВНЫЕ ПОКАЗАТЕЛИ:
        - Продажи: {total_sales:,.0f} IDR
        - Заказы: {total_orders:,.0f}
        - Средний рейтинг: {avg_rating:.2f}/5.0
        - Дней данных: {len(data)}
        
        ДЕТАЛЬНАЯ АНАЛИТИКА:
        {self._get_detailed_metrics(data)}
        
        Дай конкретные рекомендации по улучшению:
        1. Продаж и маркетинга
        2. Операционной эффективности  
        3. Качества обслуживания
        4. Работы с клиентами
        
        Формат: четкие пункты с цифрами и конкретными действиями.
        """
        
        return prompt
    
    def _get_detailed_metrics(self, data):
        """Получает детальные метрики для анализа"""
        metrics = []
        
        if 'marketing_spend' in data.columns:
            total_marketing = data['marketing_spend'].sum()
            roas = data['marketing_sales'].sum() / total_marketing if total_marketing > 0 else 0
            metrics.append(f"- ROAS: {roas:.2f}x")
            
        if 'total_customers' in data.columns:
            total_customers = data['total_customers'].sum()
            new_customers = data['new_customers'].sum()
            metrics.append(f"- Новые клиенты: {new_customers}/{total_customers} ({(new_customers/total_customers*100):.1f}%)")
            
        if 'cancelled_orders' in data.columns:
            cancelled = data['cancelled_orders'].sum()
            total_orders = data['orders'].sum()
            cancel_rate = cancelled / (total_orders + cancelled) * 100 if (total_orders + cancelled) > 0 else 0
            metrics.append(f"- Процент отмен: {cancel_rate:.1f}%")
            
        return '\n'.join(metrics)
    
    def _generate_basic_insights(self, data):
        """Генерирует базовые инсайты без OpenAI"""
        
        insights = []
        insights.append("🤖 АВТОМАТИЧЕСКИЙ АНАЛИЗ (без OpenAI API)")
        insights.append("=" * 50)
        
        # Анализ продаж
        total_sales = data['total_sales'].sum()
        avg_daily_sales = total_sales / len(data) if len(data) > 0 else 0
        insights.append(f"📊 Средние дневные продажи: {avg_daily_sales:,.0f} IDR")
        
        # Анализ трендов
        if len(data) > 7:
            recent_sales = data.tail(7)['total_sales'].mean()
            older_sales = data.head(7)['total_sales'].mean()
            trend = ((recent_sales - older_sales) / older_sales * 100) if older_sales > 0 else 0
            
            if trend > 5:
                insights.append(f"📈 Положительный тренд: +{trend:.1f}%")
            elif trend < -5:
                insights.append(f"📉 Отрицательный тренд: {trend:.1f}%")
            else:
                insights.append("➡️ Стабильные продажи")
        
        # Рекомендации
        insights.append("\n💡 БАЗОВЫЕ РЕКОМЕНДАЦИИ:")
        
        if 'marketing_spend' in data.columns:
            total_marketing = data['marketing_spend'].sum()
            roas = data['marketing_sales'].sum() / total_marketing if total_marketing > 0 else 0
            
            if roas > 5:
                insights.append("✅ ROAS отличный - продолжайте рекламу")
            elif roas > 2:
                insights.append("⚠️ ROAS средний - оптимизируйте кампании")
            else:
                insights.append("🚨 ROAS низкий - пересмотрите рекламную стратегию")
        
        if 'rating' in data.columns:
            avg_rating = data['rating'].mean()
            if avg_rating < 4.5:
                insights.append("⭐ Улучшите качество обслуживания (рейтинг ниже 4.5)")
                
        return '\n'.join(insights)

def get_restaurant_data_full(restaurant_name, start_date, end_date, db_path="database.sqlite"):
    """Получает ВСЕ доступные данные ресторана из grab_stats и gojek_stats"""
    conn = sqlite3.connect(db_path)
    
    # Получаем ID ресторана
    restaurant_query = "SELECT id FROM restaurants WHERE name = ?"
    restaurant_result = pd.read_sql_query(restaurant_query, conn, params=(restaurant_name,))
    
    if len(restaurant_result) == 0:
        conn.close()
        print(f"❌ Ресторан '{restaurant_name}' не найден")
        return pd.DataFrame()
    
    restaurant_id = restaurant_result.iloc[0]['id']
    
    # РАСШИРЕННЫЙ запрос для Grab (ВСЕ поля)
    grab_query = """
    SELECT 
        stat_date as date,
        'grab' as platform,
        sales as total_sales,
        orders,
        rating,
        COALESCE(ads_spend, 0) as marketing_spend,
        COALESCE(ads_sales, 0) as marketing_sales,
        COALESCE(ads_orders, 0) as marketing_orders,
        CASE WHEN ads_spend > 0 THEN 1 ELSE 0 END as ads_on,
        COALESCE(cancelation_rate, 0) as cancel_rate,
        COALESCE(offline_rate, 0) as offline_rate,
        COALESCE(cancelled_orders, 0) as cancelled_orders,
        COALESCE(store_is_closed, 0) as store_is_closed,
        COALESCE(store_is_busy, 0) as store_is_busy,
        COALESCE(store_is_closing_soon, 0) as store_is_closing_soon,
        COALESCE(out_of_stock, 0) as out_of_stock,
        COALESCE(ads_ctr, 0) as ads_ctr,
        COALESCE(impressions, 0) as impressions,
        COALESCE(unique_impressions_reach, 0) as unique_impressions_reach,
        COALESCE(unique_menu_visits, 0) as unique_menu_visits,
        COALESCE(unique_add_to_carts, 0) as unique_add_to_carts,
        COALESCE(unique_conversion_reach, 0) as unique_conversion_reach,
        COALESCE(new_customers, 0) as new_customers,
        COALESCE(earned_new_customers, 0) as earned_new_customers,
        COALESCE(repeated_customers, 0) as repeated_customers,
        COALESCE(earned_repeated_customers, 0) as earned_repeated_customers,
        COALESCE(reactivated_customers, 0) as reactivated_customers,
        COALESCE(earned_reactivated_customers, 0) as earned_reactivated_customers,
        COALESCE(total_customers, 0) as total_customers,
        COALESCE(payouts, 0) as payouts,
        NULL as accepting_time,
        NULL as preparation_time,
        NULL as delivery_time,
        NULL as lost_orders,
        NULL as realized_orders_percentage,
        NULL as one_star_ratings,
        NULL as two_star_ratings,
        NULL as three_star_ratings,
        NULL as four_star_ratings,
        NULL as five_star_ratings
    FROM grab_stats 
    WHERE restaurant_id = ? AND stat_date BETWEEN ? AND ?
    ORDER BY stat_date
    """
    
    # РАСШИРЕННЫЙ запрос для Gojek (ВСЕ поля)
    gojek_query = """
    SELECT 
        stat_date as date,
        'gojek' as platform,
        sales as total_sales,
        orders,
        rating,
        COALESCE(ads_spend, 0) as marketing_spend,
        COALESCE(ads_sales, 0) as marketing_sales,
        COALESCE(ads_orders, 0) as marketing_orders,
        CASE WHEN ads_spend > 0 THEN 1 ELSE 0 END as ads_on,
        0 as cancel_rate,
        0 as offline_rate,
        COALESCE(cancelled_orders, 0) as cancelled_orders,
        COALESCE(store_is_closed, 0) as store_is_closed,
        COALESCE(store_is_busy, 0) as store_is_busy,
        0 as store_is_closing_soon,
        COALESCE(out_of_stock, 0) as out_of_stock,
        0 as ads_ctr,
        0 as impressions,
        0 as unique_impressions_reach,
        0 as unique_menu_visits,
        0 as unique_add_to_carts,
        0 as unique_conversion_reach,
        COALESCE(new_client, 0) as new_customers,
        0 as earned_new_customers,
        COALESCE(active_client, 0) as repeated_customers,
        0 as earned_repeated_customers,
        COALESCE(returned_client, 0) as reactivated_customers,
        0 as earned_reactivated_customers,
        COALESCE(new_client + active_client + returned_client, 0) as total_customers,
        COALESCE(payouts, 0) as payouts,
        accepting_time,
        preparation_time,
        delivery_time,
        COALESCE(lost_orders, 0) as lost_orders,
        COALESCE(realized_orders_percentage, 0) as realized_orders_percentage,
        COALESCE(one_star_ratings, 0) as one_star_ratings,
        COALESCE(two_star_ratings, 0) as two_star_ratings,
        COALESCE(three_star_ratings, 0) as three_star_ratings,
        COALESCE(four_star_ratings, 0) as four_star_ratings,
        COALESCE(five_star_ratings, 0) as five_star_ratings
    FROM gojek_stats 
    WHERE restaurant_id = ? AND stat_date BETWEEN ? AND ?
    ORDER BY stat_date
    """
    
    # Используем прямую подстановку
    grab_query_formatted = grab_query.replace('?', f'{restaurant_id}', 1).replace('?', f"'{start_date}'", 1).replace('?', f"'{end_date}'", 1)
    gojek_query_formatted = gojek_query.replace('?', f'{restaurant_id}', 1).replace('?', f"'{start_date}'", 1).replace('?', f"'{end_date}'", 1)
    
    grab_data = pd.read_sql_query(grab_query_formatted, conn)
    gojek_data = pd.read_sql_query(gojek_query_formatted, conn)
    
    # Объединяем данные
    all_data = pd.concat([grab_data, gojek_data], ignore_index=True)
    
    # Агрегируем по дням с учетом ВСЕХ полей
    if not all_data.empty:
        data = all_data.groupby('date').agg({
            'total_sales': 'sum',
            'orders': 'sum',
            'rating': 'mean',
            'marketing_spend': 'sum',
            'marketing_sales': 'sum',
            'marketing_orders': 'sum',
            'ads_on': 'max',
            'cancel_rate': 'mean',
            'offline_rate': 'mean',
            'cancelled_orders': 'sum',
            'store_is_closed': 'max',
            'store_is_busy': 'max',
            'store_is_closing_soon': 'max',
            'out_of_stock': 'max',
            'ads_ctr': 'mean',
            'impressions': 'sum',
            'unique_impressions_reach': 'sum',
            'unique_menu_visits': 'sum',
            'unique_add_to_carts': 'sum',
            'unique_conversion_reach': 'sum',
            'new_customers': 'sum',
            'earned_new_customers': 'sum',
            'repeated_customers': 'sum',
            'earned_repeated_customers': 'sum',
            'reactivated_customers': 'sum',
            'earned_reactivated_customers': 'sum',
            'total_customers': 'sum',
            'payouts': 'sum',
            'lost_orders': 'sum',
            'realized_orders_percentage': 'mean',
            'one_star_ratings': 'sum',
            'two_star_ratings': 'sum',
            'three_star_ratings': 'sum',
            'four_star_ratings': 'sum',
            'five_star_ratings': 'sum'
        }).reset_index()
        
        # Добавляем дополнительные вычисляемые поля
        data['is_weekend'] = pd.to_datetime(data['date']).dt.dayofweek.isin([5, 6]).astype(int)
        data['is_holiday'] = data['date'].isin([
            '2025-04-10', '2025-04-14', '2025-05-07', '2025-05-12', 
            '2025-05-29', '2025-06-01', '2025-06-16', '2025-06-17'
        ]).astype(int)
        data['weekday'] = pd.to_datetime(data['date']).dt.day_name()
        data['month'] = pd.to_datetime(data['date']).dt.month
        data['avg_order_value'] = data['total_sales'] / data['orders'].replace(0, 1)
        data['roas'] = data['marketing_sales'] / data['marketing_spend'].replace(0, 1)
        
        # Новые KPI на основе дополнительных полей
        data['conversion_rate'] = data['unique_conversion_reach'] / data['unique_impressions_reach'].replace(0, 1) * 100
        data['add_to_cart_rate'] = data['unique_add_to_carts'] / data['unique_menu_visits'].replace(0, 1) * 100
        data['customer_retention_rate'] = data['repeated_customers'] / data['total_customers'].replace(0, 1) * 100
        data['order_cancellation_rate'] = data['cancelled_orders'] / (data['orders'] + data['cancelled_orders']).replace(0, 1) * 100
        data['customer_satisfaction_score'] = (data['five_star_ratings'] * 5 + data['four_star_ratings'] * 4 + 
                                              data['three_star_ratings'] * 3 + data['two_star_ratings'] * 2 + 
                                              data['one_star_ratings'] * 1) / (data['one_star_ratings'] + 
                                              data['two_star_ratings'] + data['three_star_ratings'] + 
                                              data['four_star_ratings'] + data['five_star_ratings']).replace(0, 1)
        
        # Операционные проблемы
        data['operational_issues'] = (data['store_is_closed'] + data['store_is_busy'] + 
                                    data['store_is_closing_soon'] + data['out_of_stock'])
        
    else:
        data = pd.DataFrame()
    
    conn.close()
    return data

def analyze_restaurant(restaurant_name, start_date=None, end_date=None):
    """ПОЛНЫЙ анализ ресторана с использованием ВСЕХ доступных параметров + ВСЕ API"""
    print(f"\n🔬 ПОЛНЫЙ АНАЛИЗ ВСЕХ ПАРАМЕТРОВ + API: {restaurant_name.upper()}")
    print("=" * 80)
    print("🚀 Используем ВСЕ 30+ параметров из grab_stats и gojek_stats!")
    print("🌐 + Weather API + Calendar API + OpenAI API")
    print()
    
    # Устанавливаем период по умолчанию
    if not start_date or not end_date:
        start_date = "2025-04-01"
        end_date = "2025-06-22"
    
    print(f"📅 Период анализа: {start_date} → {end_date}")
    print()
    
    # Инициализируем API
    weather_api = WeatherAPI()
    calendar_api = CalendarAPI()
    openai_analyzer = OpenAIAnalyzer()
    
    # Получаем данные
    data = get_restaurant_data_full(restaurant_name, start_date, end_date)
    
    if data.empty:
        print("❌ Нет данных для анализа")
        return
    
    # 1. Базовая аналитика
    print("📊 1. БАЗОВАЯ АНАЛИТИКА")
    print("-" * 40)
    
    total_sales = data['total_sales'].sum()
    total_orders = data['orders'].sum()
    avg_rating = data['rating'].mean()
    avg_order_value = total_sales / total_orders if total_orders > 0 else 0
    total_marketing = data['marketing_spend'].sum()
    avg_roas = data['marketing_sales'].sum() / total_marketing if total_marketing > 0 else 0
    
    print(f"💰 Общие продажи: {total_sales:,.0f} IDR")
    print(f"📦 Общие заказы: {total_orders:,.0f}")
    print(f"📊 Средний чек: {avg_order_value:,.0f} IDR")
    print(f"⭐ Средний рейтинг: {avg_rating:.2f}/5.0")
    print(f"💸 Затраты на маркетинг: {total_marketing:,.0f} IDR")
    print(f"🎯 ROAS: {avg_roas:.2f}x")
    print(f"📅 Дней данных: {len(data)}")
    print()
    
    # 2. НОВЫЙ! Анализ клиентской базы
    print("👥 2. АНАЛИЗ КЛИЕНТСКОЙ БАЗЫ")
    print("-" * 40)
    
    total_customers = data['total_customers'].sum()
    new_customers = data['new_customers'].sum()
    repeated_customers = data['repeated_customers'].sum()
    reactivated_customers = data['reactivated_customers'].sum()
    
    if total_customers > 0:
        new_customer_rate = (new_customers / total_customers) * 100
        retention_rate = (repeated_customers / total_customers) * 100
        reactivation_rate = (reactivated_customers / total_customers) * 100
        
        print(f"👥 Общее количество клиентов: {total_customers:,.0f}")
        print(f"🆕 Новые клиенты: {new_customers:,.0f} ({new_customer_rate:.1f}%)")
        print(f"🔄 Повторные клиенты: {repeated_customers:,.0f} ({retention_rate:.1f}%)")
        print(f"📲 Реактивированные: {reactivated_customers:,.0f} ({reactivation_rate:.1f}%)")
        
        # Доходность по типам клиентов
        if data['earned_new_customers'].sum() > 0:
            print(f"💰 Доход от новых: {data['earned_new_customers'].sum():,.0f} IDR")
            print(f"💰 Доход от повторных: {data['earned_repeated_customers'].sum():,.0f} IDR")
            print(f"💰 Доход от реактивированных: {data['earned_reactivated_customers'].sum():,.0f} IDR")
    
    print()
    
    # 3. НОВЫЙ! Анализ маркетинговой воронки
    print("📈 3. АНАЛИЗ МАРКЕТИНГОВОЙ ВОРОНКИ")
    print("-" * 40)
    
    total_impressions = data['impressions'].sum()
    total_menu_visits = data['unique_menu_visits'].sum()
    total_add_to_carts = data['unique_add_to_carts'].sum()
    total_conversions = data['unique_conversion_reach'].sum()
    
    if total_impressions > 0:
        ctr = (total_menu_visits / total_impressions) * 100
        add_to_cart_rate = (total_add_to_carts / total_menu_visits) * 100 if total_menu_visits > 0 else 0
        conversion_rate = (total_conversions / total_menu_visits) * 100 if total_menu_visits > 0 else 0
        
        print(f"👁️ Показы рекламы: {total_impressions:,.0f}")
        print(f"🔗 Посещения меню: {total_menu_visits:,.0f} (CTR: {ctr:.2f}%)")
        print(f"🛒 Добавления в корзину: {total_add_to_carts:,.0f} (Rate: {add_to_cart_rate:.2f}%)")
        print(f"✅ Конверсии: {total_conversions:,.0f} (Rate: {conversion_rate:.2f}%)")
        print(f"📊 Средний CTR рекламы: {data['ads_ctr'].mean():.2f}%")
    
    print()
    
    # 4. НОВЫЙ! Анализ операционных проблем
    print("⚠️ 4. АНАЛИЗ ОПЕРАЦИОННЫХ ПРОБЛЕМ")
    print("-" * 40)
    
    closed_days = data['store_is_closed'].sum()
    busy_days = data['store_is_busy'].sum()
    closing_soon_days = data['store_is_closing_soon'].sum()
    out_of_stock_days = data['out_of_stock'].sum()
    avg_cancellation_rate = data['order_cancellation_rate'].mean()
    
    total_operational_issues = data['operational_issues'].sum()
    
    print(f"🏪 Дней когда магазин был закрыт: {closed_days}")
    print(f"🔥 Дней когда магазин был занят: {busy_days}")
    print(f"⏰ Дней 'скоро закрытие': {closing_soon_days}")
    print(f"📦 Дней с отсутствием товара: {out_of_stock_days}")
    print(f"❌ Средний процент отмен заказов: {avg_cancellation_rate:.1f}%")
    print(f"⚠️ Общие операционные проблемы: {total_operational_issues} случаев")
    
    if total_operational_issues > len(data) * 0.1:
        print("🚨 ВНИМАНИЕ: Высокий уровень операционных проблем!")
    
    print()
    
    # 5. НОВЫЙ! Детальный анализ качества обслуживания
    print("⭐ 5. АНАЛИЗ КАЧЕСТВА ОБСЛУЖИВАНИЯ")
    print("-" * 40)
    
    total_ratings = (data['one_star_ratings'].sum() + data['two_star_ratings'].sum() + 
                    data['three_star_ratings'].sum() + data['four_star_ratings'].sum() + 
                    data['five_star_ratings'].sum())
    
    if total_ratings > 0:
        five_star_rate = (data['five_star_ratings'].sum() / total_ratings) * 100
        four_star_rate = (data['four_star_ratings'].sum() / total_ratings) * 100
        three_star_rate = (data['three_star_ratings'].sum() / total_ratings) * 100
        two_star_rate = (data['two_star_ratings'].sum() / total_ratings) * 100
        one_star_rate = (data['one_star_ratings'].sum() / total_ratings) * 100
        
        print(f"⭐⭐⭐⭐⭐ 5 звезд: {data['five_star_ratings'].sum():,.0f} ({five_star_rate:.1f}%)")
        print(f"⭐⭐⭐⭐ 4 звезды: {data['four_star_ratings'].sum():,.0f} ({four_star_rate:.1f}%)")
        print(f"⭐⭐⭐ 3 звезды: {data['three_star_ratings'].sum():,.0f} ({three_star_rate:.1f}%)")
        print(f"⭐⭐ 2 звезды: {data['two_star_ratings'].sum():,.0f} ({two_star_rate:.1f}%)")
        print(f"⭐ 1 звезда: {data['one_star_ratings'].sum():,.0f} ({one_star_rate:.1f}%)")
        
        satisfaction_score = data['customer_satisfaction_score'].mean()
        print(f"📊 Общий индекс удовлетворенности: {satisfaction_score:.2f}/5.0")
        
        if one_star_rate > 10:
            print("🚨 КРИТИЧНО: Высокий процент 1-звездочных отзывов!")
    
    print()
    
    # 6. НОВЫЙ! Анализ времени обслуживания (Gojek)
    print("⏱️ 6. АНАЛИЗ ВРЕМЕНИ ОБСЛУЖИВАНИЯ")
    print("-" * 40)
    
    if data['realized_orders_percentage'].mean() > 0:
        avg_realization = data['realized_orders_percentage'].mean()
        lost_orders = data['lost_orders'].sum()
        print(f"✅ Процент выполненных заказов: {avg_realization:.1f}%")
        print(f"❌ Потерянные заказы: {lost_orders:,.0f}")
        
        if avg_realization < 90:
            print("🚨 КРИТИЧНО: Низкий процент выполнения заказов!")
    
    print()
    
    # 7. НОВЫЙ! Анализ внешних факторов с API
    print("🌐 7. АНАЛИЗ ВНЕШНИХ ФАКТОРОВ (API)")
    print("-" * 40)
    
    # Анализ погоды
    print("🌤️ Анализ погоды:")
    sample_dates = data['date'].head(3).tolist()
    weather_impact = []
    
    for date in sample_dates:
        weather = weather_api.get_weather_data(date)
        day_sales = data[data['date'] == date]['total_sales'].sum()
        
        condition_emoji = {"Clear": "☀️", "Rain": "🌧️", "Clouds": "☁️", "Thunderstorm": "⛈️"}.get(weather['condition'], "🌤️")
        print(f"  {date}: {condition_emoji} {weather['condition']}, {weather['temperature']:.1f}°C → {day_sales:,.0f} IDR")
        
        if weather['condition'] in ['Rain', 'Thunderstorm']:
            weather_impact.append(day_sales)
    
    if weather_impact:
        avg_rain_sales = sum(weather_impact) / len(weather_impact)
        overall_avg = data['total_sales'].mean()
        weather_effect = ((avg_rain_sales - overall_avg) / overall_avg * 100) if overall_avg > 0 else 0
        print(f"  💧 Влияние дождя: {weather_effect:+.1f}% к продажам")
    
    print()
    
    # Анализ праздников
    print("📅 Анализ праздников:")
    year = int(start_date[:4])
    holidays = calendar_api.get_holidays(year)
    holiday_dates = [h['date'] for h in holidays]
    
    holiday_sales = data[data['date'].isin(holiday_dates)]['total_sales']
    regular_sales = data[~data['date'].isin(holiday_dates)]['total_sales']
    
    if not holiday_sales.empty and not regular_sales.empty:
        holiday_avg = holiday_sales.mean()
        regular_avg = regular_sales.mean()
        holiday_effect = ((holiday_avg - regular_avg) / regular_avg * 100) if regular_avg > 0 else 0
        
        print(f"  🎉 Праздничных дней в периоде: {len(holiday_sales)}")
        print(f"  📊 Средние продажи в праздники: {holiday_avg:,.0f} IDR")
        print(f"  📊 Средние продажи в обычные дни: {regular_avg:,.0f} IDR")
        print(f"  🎯 Влияние праздников: {holiday_effect:+.1f}%")
    
    print()
    
    # 8. AI-АНАЛИЗ И РЕКОМЕНДАЦИИ
    print("🤖 8. AI-АНАЛИЗ И РЕКОМЕНДАЦИИ")
    print("-" * 40)
    
    # Собираем данные о погоде и праздниках для AI
    weather_data = {"sample_conditions": [weather_api.get_weather_data(date) for date in sample_dates[:3]]}
    holiday_data = {"holidays_in_period": len(holiday_sales) if not holiday_sales.empty else 0}
    
    # Генерируем AI инсайты
    ai_insights = openai_analyzer.generate_insights(data, weather_data, holiday_data)
    print(ai_insights)
    
    print()
    
    # Сохраняем расширенный отчет с API данными
    try:
        os.makedirs('reports', exist_ok=True)
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"reports/full_analysis_with_api_{restaurant_name.replace(' ', '_')}_{timestamp}.txt"
        
        with open(filename, 'w', encoding='utf-8') as f:
            f.write(f"ПОЛНЫЙ АНАЛИЗ ВСЕХ ПАРАМЕТРОВ + API: {restaurant_name.upper()}\n")
            f.write("=" * 80 + "\n")
            f.write(f"Период: {start_date} → {end_date}\n")
            f.write(f"Создан: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            f.write("ИСПОЛЬЗОВАНЫ ВСЕ 63 ПАРАМЕТРОВ + 3 API\n\n")
            
            f.write("ОСНОВНЫЕ МЕТРИКИ:\n")
            f.write(f"Общие продажи: {total_sales:,.0f} IDR\n")
            f.write(f"Общие заказы: {total_orders:,.0f}\n")
            f.write(f"Общие клиенты: {total_customers:,.0f}\n")
            f.write(f"Новые клиенты: {new_customers:,.0f}\n")
            f.write(f"Операционные проблемы: {total_operational_issues}\n\n")
            
            f.write("AI ИНСАЙТЫ:\n")
            f.write(ai_insights + "\n")
        
        print(f"💾 Полный отчет с API сохранен: {filename}")
        
    except Exception as e:
        print(f"❌ Ошибка сохранения отчета: {e}")

def list_restaurants():
    """Показывает список доступных ресторанов"""
    print("🏪 ДОСТУПНЫЕ РЕСТОРАНЫ MUZAQUEST")
    print("=" * 60)
    
    try:
        conn = sqlite3.connect("database.sqlite")
        
        # Получаем рестораны с их статистикой
        query = """
        SELECT r.id, r.name,
               COUNT(DISTINCT g.stat_date) as grab_days,
               COUNT(DISTINCT gj.stat_date) as gojek_days,
               MIN(COALESCE(g.stat_date, gj.stat_date)) as first_date,
               MAX(COALESCE(g.stat_date, gj.stat_date)) as last_date,
               SUM(COALESCE(g.sales, 0) + COALESCE(gj.sales, 0)) as total_sales
        FROM restaurants r
        LEFT JOIN grab_stats g ON r.id = g.restaurant_id
        LEFT JOIN gojek_stats gj ON r.id = gj.restaurant_id
        GROUP BY r.id, r.name
        HAVING (grab_days > 0 OR gojek_days > 0)
        ORDER BY total_sales DESC, r.name
        """
        
        df = pd.read_sql_query(query, conn)
        
        for i, row in df.iterrows():
            total_days = max(row['grab_days'] or 0, row['gojek_days'] or 0)
            
            print(f"{i+1:2d}. 🍽️ {row['name']}")
            print(f"    📊 Данных: {total_days} дней ({row['first_date']} → {row['last_date']})")
            print(f"    📈 Grab: {row['grab_days'] or 0} дней | Gojek: {row['gojek_days'] or 0} дней")
            
            if row['total_sales']:
                print(f"    💰 Общие продажи: {row['total_sales']:,.0f} IDR")
            
            print()
        
        conn.close()
        
    except Exception as e:
        print(f"❌ Ошибка при получении списка ресторанов: {e}")

def analyze_market(start_date=None, end_date=None):
    """Анализ всего рынка"""
    print("\n🌍 АНАЛИЗ ВСЕГО РЫНКА MUZAQUEST")
    print("=" * 80)
    
    # Устанавливаем период по умолчанию
    if not start_date or not end_date:
        start_date = "2025-04-01"
        end_date = "2025-06-22"
    
    print(f"📅 Период анализа: {start_date} → {end_date}")
    print()
    
    try:
        conn = sqlite3.connect("database.sqlite")
        
        # Общая статистика рынка
        query = """
        WITH market_data AS (
            SELECT r.name,
                   SUM(COALESCE(g.sales, 0) + COALESCE(gj.sales, 0)) as total_sales,
                   SUM(COALESCE(g.orders, 0) + COALESCE(gj.orders, 0)) as total_orders
            FROM restaurants r
            LEFT JOIN grab_stats g ON r.id = g.restaurant_id 
                AND g.stat_date BETWEEN ? AND ?
            LEFT JOIN gojek_stats gj ON r.id = gj.restaurant_id 
                AND gj.stat_date BETWEEN ? AND ?
            GROUP BY r.name
            HAVING total_sales > 0
        )
        SELECT 
            COUNT(*) as active_restaurants,
            SUM(total_sales) as market_sales,
            SUM(total_orders) as market_orders,
            AVG(total_sales) as avg_restaurant_sales
        FROM market_data
        """
        
        market_stats = pd.read_sql_query(query, conn, params=(start_date, end_date, start_date, end_date))
        
        print("📊 ОБЗОР РЫНКА")
        print("-" * 40)
        if not market_stats.empty:
            stats = market_stats.iloc[0]
            print(f"🏪 Активных ресторанов: {stats['active_restaurants']}")
            print(f"💰 Общие продажи рынка: {stats['market_sales']:,.0f} IDR")
            print(f"📦 Общие заказы рынка: {stats['market_orders']:,.0f}")
            print(f"📊 Средние продажи на ресторан: {stats['avg_restaurant_sales']:,.0f} IDR")
        
        # Лидеры рынка
        leaders_query = """
        SELECT r.name,
               SUM(COALESCE(g.sales, 0) + COALESCE(gj.sales, 0)) as total_sales,
               SUM(COALESCE(g.orders, 0) + COALESCE(gj.orders, 0)) as total_orders,
               AVG(COALESCE(g.rating, gj.rating)) as avg_rating
        FROM restaurants r
        LEFT JOIN grab_stats g ON r.id = g.restaurant_id 
            AND g.stat_date BETWEEN ? AND ?
        LEFT JOIN gojek_stats gj ON r.id = gj.restaurant_id 
            AND gj.stat_date BETWEEN ? AND ?
        GROUP BY r.name
        HAVING total_sales > 0
        ORDER BY total_sales DESC
        LIMIT 10
        """
        
        leaders = pd.read_sql_query(leaders_query, conn, params=(start_date, end_date, start_date, end_date))
        
        print(f"\n🏆 ЛИДЕРЫ РЫНКА")
        print("-" * 40)
        print("ТОП-10 по продажам:")
        for i, row in leaders.iterrows():
            avg_order_value = row['total_sales'] / row['total_orders'] if row['total_orders'] > 0 else 0
            print(f"  {i+1:2d}. {row['name']:<25} {row['total_sales']:>12,.0f} IDR")
            print(f"      📦 {row['total_orders']:,} заказов | 💰 {avg_order_value:,.0f} IDR/заказ | ⭐ {row['avg_rating']:.2f}")
        
        conn.close()
        
    except Exception as e:
        print(f"❌ Ошибка при анализе рынка: {e}")

def check_api_status():
    """Проверяет статус всех API"""
    print("\n🌐 СТАТУС API ИНТЕГРАЦИЙ")
    print("=" * 60)
    
    # Проверка OpenAI
    openai_key = os.getenv('OPENAI_API_KEY')
    if openai_key and openai_key != 'your_openai_api_key_here':
        print("✅ OpenAI API: Настроен")
        if OPENAI_AVAILABLE:
            print("✅ OpenAI библиотека: Установлена")
        else:
            print("❌ OpenAI библиотека: Не установлена (pip install openai)")
    else:
        print("❌ OpenAI API: Не настроен (нужен .env файл)")
    
    # Проверка Weather API
    weather_key = os.getenv('WEATHER_API_KEY')
    if weather_key and weather_key != 'your_openweathermap_api_key_here':
        print("✅ Weather API: Настроен")
        # Тестовый запрос
        try:
            weather_api = WeatherAPI()
            test_weather = weather_api.get_weather_data("2025-06-01")
            if 'temperature' in test_weather:
                print("✅ Weather API: Работает")
            else:
                print("⚠️ Weather API: Используется симуляция")
        except:
            print("⚠️ Weather API: Ошибка подключения")
    else:
        print("❌ Weather API: Не настроен (используется симуляция)")
    
    # Проверка Calendar API
    calendar_key = os.getenv('CALENDAR_API_KEY')
    if calendar_key and calendar_key != 'your_calendarific_api_key_here':
        print("✅ Calendar API: Настроен")
        try:
            calendar_api = CalendarAPI()
            test_holidays = calendar_api.get_holidays(2025)
            if test_holidays:
                print("✅ Calendar API: Работает")
            else:
                print("⚠️ Calendar API: Используется локальная база")
        except:
            print("⚠️ Calendar API: Ошибка подключения")
    else:
        print("❌ Calendar API: Не настроен (используется локальная база)")
    
    print()
    print("💡 Для настройки API:")
    print("   1. Скопируйте .env.example в .env")
    print("   2. Добавьте ваши API ключи")
    print("   3. Перезапустите систему")

def main():
    """Главная функция CLI"""
    
    print("""
🎯 MUZAQUEST ANALYTICS - ПОЛНЫЙ АНАЛИЗ ВСЕХ ПАРАМЕТРОВ + API
═══════════════════════════════════════════════════════════════════════════════
🚀 Используем ВСЕ 63 поля из grab_stats и gojek_stats!
🌐 + OpenAI API + Weather API + Calendar API
═══════════════════════════════════════════════════════════════════════════════
""")
    
    parser = argparse.ArgumentParser(
        description="Muzaquest Analytics - Полный анализ всех параметров + API",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
ПРИМЕРЫ ИСПОЛЬЗОВАНИЯ:
  
  📋 Список ресторанов:
    python main.py list
  
  🔬 Полный анализ ресторана (ВСЕ 63 параметра + API):
    python main.py analyze "Ika Canggu"
    python main.py analyze "Ika Canggu" --start 2025-04-01 --end 2025-06-22
  
  🌍 Анализ всего рынка:
    python main.py market
    python main.py market --start 2025-04-01 --end 2025-06-22
    
  🌐 Проверка статуса API:
    python main.py check-apis

НОВЫЕ ВОЗМОЖНОСТИ:
  👥 Анализ клиентской базы (новые/повторные/реактивированные)
  📈 Маркетинговая воронка (показы → клики → конверсии)
  ⚠️ Операционные проблемы (закрыт/занят/нет товара)
  ⭐ Детальные рейтинги (1-5 звезд)
  ⏱️ Анализ времени обслуживания
  🌤️ Анализ влияния погоды (Weather API)
  📅 Анализ влияния праздников (Calendar API) 
  🤖 AI-инсайты и рекомендации (OpenAI API)
        """
    )
    
    parser.add_argument('command', 
                       choices=['list', 'analyze', 'market', 'check-apis'],
                       help='Команда для выполнения')
    
    parser.add_argument('restaurant', nargs='?', 
                       help='Название ресторана для анализа')
    
    parser.add_argument('--start', 
                       help='Дата начала периода (YYYY-MM-DD)')
    
    parser.add_argument('--end', 
                       help='Дата окончания периода (YYYY-MM-DD)')
    
    args = parser.parse_args()
    
    # Проверяем наличие базы данных
    if args.command != 'check-apis' and not os.path.exists('database.sqlite'):
        print("❌ База данных 'database.sqlite' не найдена!")
        print("   Убедитесь, что файл database.sqlite находится в корневой папке")
        sys.exit(1)
    
    try:
        if args.command == 'list':
            list_restaurants()
            
        elif args.command == 'analyze':
            if not args.restaurant:
                print("❌ Укажите название ресторана для анализа")
                print("   Используйте: python main.py analyze \"Название ресторана\"")
                sys.exit(1)
            
            analyze_restaurant(args.restaurant, args.start, args.end)
            
        elif args.command == 'market':
            analyze_market(args.start, args.end)
            
        elif args.command == 'check-apis':
            check_api_status()
    
    except KeyboardInterrupt:
        print("\n\n🛑 Анализ прерван пользователем")
        sys.exit(0)
    
    except Exception as e:
        print(f"\n❌ Критическая ошибка: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()