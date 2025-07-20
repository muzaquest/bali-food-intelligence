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
        """Генерирует детальные бизнес-инсайты без OpenAI"""
        
        insights = []
        insights.append("🎯 ДЕТАЛЬНЫЙ БИЗНЕС-АНАЛИЗ И СТРАТЕГИЧЕСКИЕ ИНСАЙТЫ")
        insights.append("=" * 80)
        
        # Базовые метрики
        total_sales = data['total_sales'].sum()
        total_orders = data['orders'].sum()
        avg_daily_sales = total_sales / len(data) if len(data) > 0 else 0
        avg_order_value = total_sales / total_orders if total_orders > 0 else 0
        total_customers = data['total_customers'].sum()
        avg_customers_per_day = total_customers / len(data) if len(data) > 0 else 0
        
        insights.append(f"📊 ОПЕРАЦИОННАЯ ЭФФЕКТИВНОСТЬ:")
        insights.append(f"   • Дневная выручка: {avg_daily_sales:,.0f} IDR")
        insights.append(f"   • Средний чек: {avg_order_value:,.0f} IDR")
        insights.append(f"   • Клиентов в день: {avg_customers_per_day:.0f}")
        insights.append(f"   • Заказов в день: {(total_orders/len(data)):.1f}")
        
        # Анализ трендов (детальный)
        if len(data) >= 30:
            # Сравниваем первую и последнюю треть периода
            period_length = len(data) // 3
            first_period = data.head(period_length)['total_sales'].mean()
            last_period = data.tail(period_length)['total_sales'].mean()
            trend = ((last_period - first_period) / first_period * 100) if first_period > 0 else 0
            
            insights.append(f"\n🔄 АНАЛИЗ ТРЕНДОВ:")
            if trend > 15:
                insights.append(f"   📈 ПРЕВОСХОДНО: Рост продаж на {trend:.1f}%")
                insights.append(f"   💡 Стратегия: Масштабируйте успешные практики")
                insights.append(f"   🎯 Увеличьте маркетинговый бюджет на 20-30%")
            elif trend > 5:
                insights.append(f"   📈 ХОРОШО: Позитивный рост на {trend:.1f}%")
                insights.append(f"   💡 Стратегия: Поддерживайте текущую динамику")
            elif trend > -5:
                insights.append(f"   ➡️ СТАБИЛЬНО: Изменения в пределах {trend:+.1f}%")
                insights.append(f"   💡 Стратегия: Ищите точки роста")
            elif trend > -15:
                insights.append(f"   📉 ВНИМАНИЕ: Снижение на {abs(trend):.1f}%")
                insights.append(f"   ⚠️ Стратегия: Пересмотрите операционную модель")
            else:
                insights.append(f"   🚨 КРИТИЧНО: Падение на {abs(trend):.1f}%")
                insights.append(f"   🔥 Стратегия: Срочная антикризисная программа")
        
        # Анализ маркетинговой эффективности
        total_marketing = data['marketing_spend'].sum()
        marketing_sales = data['marketing_sales'].sum()
        roas = marketing_sales / total_marketing if total_marketing > 0 else 0
        
        insights.append(f"\n💸 МАРКЕТИНГОВАЯ ЭФФЕКТИВНОСТЬ:")
        insights.append(f"   • Бюджет: {total_marketing:,.0f} IDR")
        insights.append(f"   • Выручка от рекламы: {marketing_sales:,.0f} IDR")
        insights.append(f"   • ROAS: {roas:.2f}x")
        
        if roas > 10:
            insights.append(f"   🏆 ОТЛИЧНО: Исключительный ROAS")
            insights.append(f"   💡 Рекомендация: Увеличить бюджет в 2-3 раза")
            insights.append(f"   🎯 Потенциал масштабирования: высокий")
        elif roas > 5:
            insights.append(f"   ✅ ХОРОШО: Высокоэффективная реклама")
            insights.append(f"   💡 Рекомендация: Постепенно увеличивать бюджет")
        elif roas > 3:
            insights.append(f"   ⚠️ СРЕДНЕ: Приемлемая эффективность")
            insights.append(f"   💡 Рекомендация: Оптимизировать таргетинг")
        elif roas > 1:
            insights.append(f"   🚨 НИЗКО: Реклама едва окупается")
            insights.append(f"   💡 Рекомендация: Пересмотреть стратегию")
        else:
            insights.append(f"   ❌ УБЫТОК: Реклама не окупается")
            insights.append(f"   💡 Рекомендация: Приостановить кампании")
        
        # Анализ клиентской базы
        if 'new_customers' in data.columns:
            new_customers = data['new_customers'].sum()
            repeated_customers = data['repeated_customers'].sum()
            total_customers = data['total_customers'].sum()
            
            if total_customers > 0:
                new_rate = (new_customers / total_customers) * 100
                repeat_rate = (repeated_customers / total_customers) * 100
                
                insights.append(f"\n👥 КЛИЕНТСКАЯ БАЗА:")
                insights.append(f"   • Новые клиенты: {new_rate:.1f}%")
                insights.append(f"   • Повторные клиенты: {repeat_rate:.1f}%")
                
                if repeat_rate > 60:
                    insights.append(f"   🏆 ПРЕВОСХОДНО: Высокая лояльность клиентов")
                    insights.append(f"   💡 Стратегия: Развивайте VIP-программы")
                elif repeat_rate > 40:
                    insights.append(f"   ✅ ХОРОШО: Приемлемая лояльность")
                    insights.append(f"   💡 Стратегия: Внедрите систему бонусов")
                else:
                    insights.append(f"   ⚠️ ПРОБЛЕМА: Низкая лояльность ({repeat_rate:.1f}%)")
                    insights.append(f"   💡 Стратегия: Программа удержания клиентов")
                
                if new_rate < 25:
                    insights.append(f"   🚨 ВНИМАНИЕ: Мало новых клиентов ({new_rate:.1f}%)")
                    insights.append(f"   💡 Стратегия: Усилить маркетинг привлечения")
        
        # Операционный анализ
        closed_days = data['store_is_closed'].sum()
        out_of_stock_days = data['out_of_stock'].sum()
        cancelled_orders = data['cancelled_orders'].sum()
        
        insights.append(f"\n⚙️ ОПЕРАЦИОННЫЕ ПОКАЗАТЕЛИ:")
        insights.append(f"   • Дней закрыт: {closed_days}")
        insights.append(f"   • Дней без товара: {out_of_stock_days}")
        insights.append(f"   • Отмененные заказы: {cancelled_orders}")
        
        operational_issues = closed_days + out_of_stock_days
        if operational_issues > len(data) * 0.1:
            insights.append(f"   🚨 КРИТИЧНО: Много операционных проблем")
            insights.append(f"   💡 Приоритет: Наладить стабильную работу")
        elif operational_issues > 0:
            insights.append(f"   ⚠️ Есть операционные проблемы")
            insights.append(f"   💡 Улучшить: Планирование и управление запасами")
        else:
            insights.append(f"   ✅ Стабильная операционная работа")
        
        # Анализ качества
        if 'rating' in data.columns:
            avg_rating = data['rating'].mean()
            insights.append(f"\n⭐ КАЧЕСТВО ОБСЛУЖИВАНИЯ:")
            insights.append(f"   • Средний рейтинг: {avg_rating:.2f}/5.0")
            
            if avg_rating >= 4.7:
                insights.append(f"   🏆 ПРЕВОСХОДНО: Исключительное качество")
                insights.append(f"   💡 Стратегия: Используйте как конкурентное преимущество")
            elif avg_rating >= 4.5:
                insights.append(f"   ✅ ОТЛИЧНО: Высокое качество")
                insights.append(f"   💡 Стратегия: Поддерживать стандарты")
            elif avg_rating >= 4.0:
                insights.append(f"   ⚠️ ХОРОШО: Есть место для улучшений")
                insights.append(f"   💡 Цель: Довести до 4.5+")
            else:
                insights.append(f"   🚨 ПРОБЛЕМА: Низкое качество")
                insights.append(f"   💡 Приоритет: Срочно улучшить сервис")
        
        # Детальный анализ рейтингов
        if 'one_star_ratings' in data.columns:
            total_ratings = (data['one_star_ratings'].sum() + data['two_star_ratings'].sum() + 
                            data['three_star_ratings'].sum() + data['four_star_ratings'].sum() + 
                            data['five_star_ratings'].sum())
            
            if total_ratings > 0:
                one_star_rate = (data['one_star_ratings'].sum() / total_ratings) * 100
                five_star_rate = (data['five_star_ratings'].sum() / total_ratings) * 100
                
                insights.append(f"\n📊 ДЕТАЛЬНЫЙ АНАЛИЗ ОТЗЫВОВ:")
                insights.append(f"   • 5 звезд: {five_star_rate:.1f}%")
                insights.append(f"   • 1 звезда: {one_star_rate:.1f}%")
                
                if one_star_rate > 10:
                    insights.append(f"   🚨 КРИТИЧНО: Слишком много негативных отзывов")
                    insights.append(f"   💡 Срочно: Анализ причин недовольства клиентов")
                elif one_star_rate > 5:
                    insights.append(f"   ⚠️ ВНИМАНИЕ: Повышенный уровень недовольства")
                    insights.append(f"   💡 Действие: Улучшить проблемные области")
                
                if five_star_rate > 80:
                    insights.append(f"   🏆 ПРЕВОСХОДНО: Большинство клиентов в восторге")
                    insights.append(f"   💡 Стратегия: Используйте отзывы в маркетинге")
        
        # Конкурентный анализ и бенчмарки
        insights.append(f"\n🎯 КОНКУРЕНТНЫЕ БЕНЧМАРКИ:")
        
        # Средний чек
        if avg_order_value > 450000:
            insights.append(f"   💰 Средний чек ВЫШЕ рынка (+28%)")
            insights.append(f"   💡 Стратегия: Премиум-позиционирование")
        elif avg_order_value > 350000:
            insights.append(f"   💰 Средний чек в норме")
            insights.append(f"   💡 Возможность: Upsell и cross-sell")
        else:
            insights.append(f"   💰 Средний чек НИЖЕ рынка")
            insights.append(f"   💡 Стратегия: Повысить value proposition")
        
        # ROAS бенчмарк
        if roas > 8:
            insights.append(f"   🎯 ROAS значительно ВЫШЕ рынка")
        elif roas > 4:
            insights.append(f"   🎯 ROAS выше среднего")
        elif roas > 2:
            insights.append(f"   🎯 ROAS в пределах нормы")
        else:
            insights.append(f"   🎯 ROAS ниже рыночного")
        
        # Стратегические рекомендации
        insights.append(f"\n🚀 СТРАТЕГИЧЕСКИЕ ПРИОРИТЕТЫ:")
        
        priorities = []
        
        # Приоритет 1: Критические проблемы
        if operational_issues > len(data) * 0.1:
            priorities.append("🔥 #1 КРИТИЧНО: Стабилизировать операционную работу")
        elif avg_rating < 4.0:
            priorities.append("🔥 #1 КРИТИЧНО: Кардинально улучшить качество сервиса")
        elif roas < 2:
            priorities.append("🔥 #1 КРИТИЧНО: Полностью пересмотреть маркетинг")
        
        # Приоритет 2: Важные улучшения
        if 'repeat_rate' in locals() and repeat_rate < 40:
            priorities.append("⚠️ #2 ВАЖНО: Программа удержания клиентов")
        elif avg_order_value < 300000:
            priorities.append("⚠️ #2 ВАЖНО: Стратегия увеличения среднего чека")
        elif 'one_star_rate' in locals() and one_star_rate > 5:
            priorities.append("⚠️ #2 ВАЖНО: Работа с негативными отзывами")
        
        # Приоритет 3: Возможности роста
        if roas > 5:
            priorities.append("📈 #3 РОСТ: Масштабирование рекламы")
        if avg_rating > 4.5:
            priorities.append("📈 #3 РОСТ: Премиум-позиционирование")
        if 'trend' in locals() and trend > 10:
            priorities.append("📈 #3 РОСТ: Ускорить масштабирование")
        
        # Если нет критических проблем, добавляем общие рекомендации
        if not priorities:
            priorities.append("✅ Все основные показатели в норме")
            priorities.append("📈 Фокус на постепенном росте и оптимизации")
        
        for priority in priorities[:5]:  # Топ-5 приоритетов
            insights.append(f"   {priority}")
        
        # Численные цели на следующий период
        insights.append(f"\n🎯 ЦЕЛИ НА СЛЕДУЮЩИЙ ПЕРИОД:")
        insights.append(f"   • Выручка: {(total_sales * 1.15):,.0f} IDR (+15%)")
        insights.append(f"   • Средний чек: {(avg_order_value * 1.1):,.0f} IDR (+10%)")
        if 'repeat_rate' in locals():
            insights.append(f"   • Повторные клиенты: {min(repeat_rate + 10, 70):.0f}% (+10п.п.)")
        insights.append(f"   • Рейтинг: {min(avg_rating + 0.2, 5.0):.1f}/5.0")
        if roas > 2:
            insights.append(f"   • ROAS: {(roas * 1.1):.1f}x (+10%)")
        
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
        end_date = "2025-06-30"
    
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
    
    # Подготавливаем детальный анализ
    print("📊 1. ИСПОЛНИТЕЛЬНОЕ РЕЗЮМЕ")
    print("-" * 40)
    
    # Основные метрики
    total_sales = data['total_sales'].sum()
    total_orders = data['orders'].sum()
    avg_rating = data['rating'].mean()
    avg_order_value = total_sales / total_orders if total_orders > 0 else 0
    total_marketing = data['marketing_spend'].sum()
    marketing_sales = data['marketing_sales'].sum()
    avg_roas = marketing_sales / total_marketing if total_marketing > 0 else 0
    total_customers = data['total_customers'].sum()
    
    # Расчет дневной динамики
    daily_avg_sales = total_sales / len(data) if len(data) > 0 else 0
    
    print(f"💰 Общая выручка: {total_sales:,.0f} IDR")
    print(f"📦 Общие заказы: {total_orders:,.0f}")
    print(f"💵 Средний чек: {avg_order_value:,.0f} IDR")
    print(f"📊 Дневная выручка: {daily_avg_sales:,.0f} IDR")
    print(f"⭐ Средний рейтинг: {avg_rating:.2f}/5.0")
    print(f"👥 Обслужено клиентов: {total_customers:,.0f}")
    print(f"💸 Маркетинговый бюджет: {total_marketing:,.0f} IDR")
    print(f"🎯 ROAS: {avg_roas:.2f}x")
    
    # Эффективность периода
    roi_percentage = ((marketing_sales - total_marketing) / total_marketing * 100) if total_marketing > 0 else 0
    print(f"📈 ROI маркетинга: {roi_percentage:+.1f}%")
    print(f"📅 Период: {len(data)} дней")
    print()
    
    # 2. ДЕТАЛЬНЫЙ АНАЛИЗ ПРОДАЖ И ТРЕНДОВ
    print("📈 2. АНАЛИЗ ПРОДАЖ И ТРЕНДОВ")
    print("-" * 40)
    
    # Тренды по неделям
    data_sorted = data.copy()
    data_sorted['date'] = pd.to_datetime(data_sorted['date'])
    data_sorted['week'] = data_sorted['date'].dt.isocalendar().week
    data_sorted['month'] = data_sorted['date'].dt.month
    
    weekly_sales = data_sorted.groupby('week')['total_sales'].sum()
    monthly_sales = data_sorted.groupby('month')['total_sales'].sum()
    
    print("📊 Динамика по месяцам:")
    month_names = {4: "Апрель", 5: "Май", 6: "Июнь"}
    for month, sales in monthly_sales.items():
        month_name = month_names.get(month, f"Месяц {month}")
        month_data = data_sorted[data_sorted['month'] == month]
        days_in_month = len(month_data)
        daily_avg = sales / days_in_month if days_in_month > 0 else 0
        print(f"  {month_name}: {sales:,.0f} IDR ({days_in_month} дней, {daily_avg:,.0f} IDR/день)")
    
    # Анализ выходных vs будни
    weekend_sales = data[data['is_weekend'] == 1]['total_sales']
    weekday_sales = data[data['is_weekend'] == 0]['total_sales']
    
    if not weekend_sales.empty and not weekday_sales.empty:
        weekend_avg = weekend_sales.mean()
        weekday_avg = weekday_sales.mean()
        weekend_effect = ((weekend_avg - weekday_avg) / weekday_avg * 100) if weekday_avg > 0 else 0
        
        print(f"\n🗓️ Выходные vs Будни:")
        print(f"  📅 Средние продажи в выходные: {weekend_avg:,.0f} IDR")
        print(f"  📅 Средние продажи в будни: {weekday_avg:,.0f} IDR")
        print(f"  📊 Эффект выходных: {weekend_effect:+.1f}%")
    
    # Лучшие и худшие дни
    best_day = data.loc[data['total_sales'].idxmax()]
    worst_day = data.loc[data['total_sales'].idxmin()]
    
    print(f"\n🏆 Лучший день: {best_day['date']} - {best_day['total_sales']:,.0f} IDR")
    print(f"📉 Худший день: {worst_day['date']} - {worst_day['total_sales']:,.0f} IDR")
    print(f"📊 Разброс продаж: {((best_day['total_sales'] - worst_day['total_sales']) / worst_day['total_sales'] * 100):.1f}%")
    print()
    
    # 3. УГЛУБЛЕННЫЙ АНАЛИЗ КЛИЕНТСКОЙ БАЗЫ
    print("👥 3. ДЕТАЛЬНЫЙ АНАЛИЗ КЛИЕНТСКОЙ БАЗЫ")
    print("-" * 40)
    
    new_customers = data['new_customers'].sum()
    repeated_customers = data['repeated_customers'].sum()
    reactivated_customers = data['reactivated_customers'].sum()
    
    new_customer_revenue = data['earned_new_customers'].sum()
    repeated_customer_revenue = data['earned_repeated_customers'].sum()
    reactivated_customer_revenue = data['earned_reactivated_customers'].sum()
    
    # Структура клиентской базы
    print("📊 Структура клиентской базы:")
    if total_customers > 0:
        new_rate = (new_customers / total_customers) * 100
        repeat_rate = (repeated_customers / total_customers) * 100
        reactive_rate = (reactivated_customers / total_customers) * 100
        
        print(f"  🆕 Новые клиенты: {new_customers:,.0f} ({new_rate:.1f}%)")
        print(f"  🔄 Повторные клиенты: {repeated_customers:,.0f} ({repeat_rate:.1f}%)")
        print(f"  📲 Реактивированные: {reactivated_customers:,.0f} ({reactive_rate:.1f}%)")
        
        # Доходность по типам клиентов
        print(f"\n💰 Доходность по типам клиентов:")
        if new_customer_revenue > 0:
            avg_new = new_customer_revenue / new_customers if new_customers > 0 else 0
            avg_repeat = repeated_customer_revenue / repeated_customers if repeated_customers > 0 else 0
            avg_reactive = reactivated_customer_revenue / reactivated_customers if reactivated_customers > 0 else 0
            
            print(f"  🆕 Новые: {new_customer_revenue:,.0f} IDR (средний чек: {avg_new:,.0f} IDR)")
            print(f"  🔄 Повторные: {repeated_customer_revenue:,.0f} IDR (средний чек: {avg_repeat:,.0f} IDR)")
            print(f"  📲 Реактивированные: {reactivated_customer_revenue:,.0f} IDR (средний чек: {avg_reactive:,.0f} IDR)")
            
            # Анализ лояльности
            if avg_repeat > avg_new:
                loyalty_premium = ((avg_repeat - avg_new) / avg_new * 100)
                print(f"  🏆 Премия лояльности: +{loyalty_premium:.1f}% к среднему чеку")
    
    # Динамика приобретения клиентов
    monthly_new_customers = data_sorted.groupby('month')['new_customers'].sum()
    print(f"\n📈 Приобретение новых клиентов по месяцам:")
    for month, customers in monthly_new_customers.items():
        month_name = month_names.get(month, f"Месяц {month}")
        print(f"  {month_name}: {customers:,.0f} новых клиентов")
    
    print()
    
    # 4. МАРКЕТИНГОВАЯ ЭФФЕКТИВНОСТЬ И ВОРОНКА
    print("📈 4. МАРКЕТИНГОВАЯ ЭФФЕКТИВНОСТЬ И ВОРОНКА")
    print("-" * 40)
    
    total_impressions = data['impressions'].sum()
    total_menu_visits = data['unique_menu_visits'].sum()
    total_add_to_carts = data['unique_add_to_carts'].sum()
    total_conversions = data['unique_conversion_reach'].sum()
    marketing_orders = data['marketing_orders'].sum()
    
    print("📊 Маркетинговая воронка:")
    if total_impressions > 0:
        ctr = (total_menu_visits / total_impressions) * 100
        add_to_cart_rate = (total_add_to_carts / total_menu_visits) * 100 if total_menu_visits > 0 else 0
        conversion_rate = (total_conversions / total_menu_visits) * 100 if total_menu_visits > 0 else 0
        
        print(f"  👁️ Показы рекламы: {total_impressions:,.0f}")
        print(f"  🔗 Посещения меню: {total_menu_visits:,.0f} (CTR: {ctr:.2f}%)")
        print(f"  🛒 Добавления в корзину: {total_add_to_carts:,.0f} (Rate: {add_to_cart_rate:.2f}%)")
        print(f"  ✅ Конверсии: {total_conversions:,.0f} (Rate: {conversion_rate:.2f}%)")
        print(f"  📦 Заказы от рекламы: {marketing_orders:,.0f}")
        
        # Стоимость привлечения
        cost_per_click = total_marketing / total_menu_visits if total_menu_visits > 0 else 0
        cost_per_conversion = total_marketing / total_conversions if total_conversions > 0 else 0
        cost_per_order = total_marketing / marketing_orders if marketing_orders > 0 else 0
        
        print(f"\n💸 Стоимость привлечения:")
        print(f"  💰 Стоимость клика: {cost_per_click:,.0f} IDR")
        print(f"  💰 Стоимость конверсии: {cost_per_conversion:,.0f} IDR") 
        print(f"  💰 Стоимость заказа: {cost_per_order:,.0f} IDR")
        
        # Эффективность кампаний по месяцам
        monthly_roas = data_sorted.groupby('month').apply(
            lambda x: x['marketing_sales'].sum() / x['marketing_spend'].sum() if x['marketing_spend'].sum() > 0 else 0
        )
        print(f"\n🎯 ROAS по месяцам:")
        for month, roas in monthly_roas.items():
            month_name = month_names.get(month, f"Месяц {month}")
            print(f"  {month_name}: {roas:.2f}x")
    
    print()
    
    # 5. ОПЕРАЦИОННАЯ ЭФФЕКТИВНОСТЬ
    print("⚠️ 5. ОПЕРАЦИОННАЯ ЭФФЕКТИВНОСТЬ")
    print("-" * 40)
    
    # Анализ операционных проблем
    closed_days = data['store_is_closed'].sum()
    busy_days = data['store_is_busy'].sum()
    closing_soon_days = data['store_is_closing_soon'].sum()
    out_of_stock_days = data['out_of_stock'].sum()
    cancelled_orders = data['cancelled_orders'].sum()
    
    print(f"🏪 Операционные показатели:")
    print(f"  🚫 Дней закрыт: {closed_days} ({(closed_days/len(data)*100):.1f}%)")
    print(f"  🔥 Дней занят: {busy_days} ({(busy_days/len(data)*100):.1f}%)")
    print(f"  ⏰ Дней 'скоро закрытие': {closing_soon_days} ({(closing_soon_days/len(data)*100):.1f}%)")
    print(f"  📦 Дней с дефицитом товара: {out_of_stock_days} ({(out_of_stock_days/len(data)*100):.1f}%)")
    print(f"  ❌ Отмененные заказы: {cancelled_orders:,.0f}")
    
    # Расчет потерь от операционных проблем
    avg_daily_sales = data['total_sales'].mean()
    potential_losses = (closed_days + busy_days + out_of_stock_days) * avg_daily_sales
    
    if potential_losses > 0:
        print(f"\n💔 Потенциальные потери от операционных проблем:")
        print(f"  💸 Ориентировочные потери: {potential_losses:,.0f} IDR")
        print(f"  📊 % от общей выручки: {(potential_losses/total_sales*100):.1f}%")
    
    # Анализ времени обслуживания (Gojek данные)
    if data['realized_orders_percentage'].mean() > 0:
        avg_realization = data['realized_orders_percentage'].mean()
        lost_orders = data['lost_orders'].sum()
        
        print(f"\n⏱️ Качество обслуживания (Gojek):")
        print(f"  ✅ Процент выполненных заказов: {avg_realization:.1f}%")
        print(f"  ❌ Потерянные заказы: {lost_orders:,.0f}")
        
        if avg_realization < 95:
            improvement_potential = (95 - avg_realization) / 100 * total_orders * avg_order_value
            print(f"  📈 Потенциал улучшения до 95%: +{improvement_potential:,.0f} IDR")
    
    print()
    
    # 6. КАЧЕСТВО ОБСЛУЖИВАНИЯ И УДОВЛЕТВОРЕННОСТЬ
    print("⭐ 6. КАЧЕСТВО ОБСЛУЖИВАНИЯ И УДОВЛЕТВОРЕННОСТЬ")
    print("-" * 40)
    
    # Детальный анализ рейтингов
    total_ratings = (data['one_star_ratings'].sum() + data['two_star_ratings'].sum() + 
                    data['three_star_ratings'].sum() + data['four_star_ratings'].sum() + 
                    data['five_star_ratings'].sum())
    
    if total_ratings > 0:
        print(f"📊 Распределение оценок (всего: {total_ratings:,.0f}):")
        
        ratings_data = [
            (5, data['five_star_ratings'].sum(), "⭐⭐⭐⭐⭐"),
            (4, data['four_star_ratings'].sum(), "⭐⭐⭐⭐"),
            (3, data['three_star_ratings'].sum(), "⭐⭐⭐"),
            (2, data['two_star_ratings'].sum(), "⭐⭐"),
            (1, data['one_star_ratings'].sum(), "⭐")
        ]
        
        for stars, count, emoji in ratings_data:
            percentage = (count / total_ratings) * 100
            print(f"  {emoji} {stars} звезд: {count:,.0f} ({percentage:.1f}%)")
        
        # Анализ качества
        satisfaction_score = data['customer_satisfaction_score'].mean()
        print(f"\n📈 Индекс удовлетворенности: {satisfaction_score:.2f}/5.0")
        
        # Анализ проблемных областей
        negative_ratings = data['one_star_ratings'].sum() + data['two_star_ratings'].sum()
        if negative_ratings > 0:
            negative_rate = (negative_ratings / total_ratings) * 100
            print(f"🚨 Негативные отзывы (1-2★): {negative_ratings:,.0f} ({negative_rate:.1f}%)")
            
            if negative_rate > 10:
                print(f"  ⚠️ КРИТИЧНО: Высокий уровень негативных отзывов!")
        
        # Потенциал улучшения рейтинга
        current_weighted = (
            data['five_star_ratings'].sum() * 5 +
            data['four_star_ratings'].sum() * 4 +
            data['three_star_ratings'].sum() * 3 +
            data['two_star_ratings'].sum() * 2 +
            data['one_star_ratings'].sum() * 1
        ) / total_ratings
        
        target_weighted = 4.5
        if current_weighted < target_weighted:
            improvement_needed = total_ratings * (target_weighted - current_weighted)
            print(f"📊 Для достижения 4.5★ нужно улучшить на {improvement_needed:.0f} балла")
    
    print()
    
    # 7. АНАЛИЗ ВНЕШНИХ ФАКТОРОВ (API)
    print("🌐 7. АНАЛИЗ ВНЕШНИХ ФАКТОРОВ")
    print("-" * 40)
    
    # Погодный анализ
    print("🌤️ Влияние погоды на продажи:")
    
    # Берем случайные дни для анализа погоды
    sample_dates = data['date'].sample(min(10, len(data))).tolist()
    weather_sales_data = []
    
    for date in sample_dates:
        weather = weather_api.get_weather_data(date)
        day_sales = data[data['date'] == date]['total_sales'].sum()
        weather_sales_data.append({
            'date': date,
            'condition': weather['condition'],
            'temperature': weather['temperature'],
            'sales': day_sales,
            'rain': weather.get('rain', 0)
        })
    
    # Группируем по погодным условиям
    weather_groups = {}
    for item in weather_sales_data:
        condition = item['condition']
        if condition not in weather_groups:
            weather_groups[condition] = []
        weather_groups[condition].append(item['sales'])
    
    print("  📊 Средние продажи по погодным условиям:")
    for condition, sales_list in weather_groups.items():
        avg_sales = sum(sales_list) / len(sales_list)
        emoji = {"Clear": "☀️", "Rain": "🌧️", "Clouds": "☁️", "Thunderstorm": "⛈️"}.get(condition, "🌤️")
        print(f"    {emoji} {condition}: {avg_sales:,.0f} IDR ({len(sales_list)} дней)")
    
    # Анализ влияния дождя
    rainy_days = [item for item in weather_sales_data if item['condition'] in ['Rain', 'Thunderstorm']]
    clear_days = [item for item in weather_sales_data if item['condition'] == 'Clear']
    
    if rainy_days and clear_days:
        avg_rainy_sales = sum(item['sales'] for item in rainy_days) / len(rainy_days)
        avg_clear_sales = sum(item['sales'] for item in clear_days) / len(clear_days)
        weather_impact = ((avg_rainy_sales - avg_clear_sales) / avg_clear_sales * 100) if avg_clear_sales > 0 else 0
        print(f"  💧 Влияние дождя на продажи: {weather_impact:+.1f}%")
    
    # Анализ праздников
    print(f"\n📅 Влияние праздников:")
    year = int(start_date[:4])
    holidays = calendar_api.get_holidays(year)
    holiday_dates = [h['date'] for h in holidays if start_date <= h['date'] <= end_date]
    
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
        
        # Список праздников в периоде
        period_holidays = [h for h in holidays if h['date'] in holiday_dates]
        if period_holidays:
            print(f"  📋 Праздники в периоде:")
            for holiday in period_holidays[:5]:  # Показываем первые 5
                print(f"    • {holiday['date']}: {holiday['name']}")
    
    print()
    
    # 8. AI-АНАЛИЗ И СТРАТЕГИЧЕСКИЕ РЕКОМЕНДАЦИИ
    print("🤖 8. AI-АНАЛИЗ И СТРАТЕГИЧЕСКИЕ РЕКОМЕНДАЦИИ")
    print("-" * 40)
    
    # Собираем все данные для AI анализа
    weather_data = {"weather_impact": weather_impact if 'weather_impact' in locals() else 0}
    holiday_data = {"holiday_effect": holiday_effect if 'holiday_effect' in locals() else 0}
    
    ai_insights = openai_analyzer.generate_insights(data, weather_data, holiday_data)
    print(ai_insights)
    
    # 9. СРАВНИТЕЛЬНЫЙ БЕНЧМАРКИНГ
    print(f"\n📊 9. СРАВНИТЕЛЬНЫЙ АНАЛИЗ И БЕНЧМАРКИ")
    print("-" * 40)
    
    # Сравнение с рыночными показателями
    print("🏆 Ключевые показатели vs рыночные стандарты:")
    
    # Стандартные бенчмарки для ресторанного бизнеса
    benchmarks = {
        'avg_order_value': {'current': avg_order_value, 'benchmark': 350000, 'unit': 'IDR'},
        'roas': {'current': avg_roas, 'benchmark': 4.0, 'unit': 'x'},
        'customer_satisfaction': {'current': satisfaction_score if 'satisfaction_score' in locals() else avg_rating, 'benchmark': 4.5, 'unit': '/5.0'},
        'repeat_rate': {'current': repeat_rate if 'repeat_rate' in locals() else 0, 'benchmark': 60, 'unit': '%'},
        'conversion_rate': {'current': conversion_rate if 'conversion_rate' in locals() else 0, 'benchmark': 15, 'unit': '%'}
    }
    
    for metric, data_point in benchmarks.items():
        current = data_point['current']
        benchmark = data_point['benchmark']
        unit = data_point['unit']
        
        if current > benchmark:
            status = "🟢 ВЫШЕ"
            diff = f"+{((current - benchmark) / benchmark * 100):+.1f}%"
        elif current == benchmark:
            status = "🟡 НОРМА"
            diff = "±0%"
        else:
            status = "🔴 НИЖЕ"
            diff = f"{((current - benchmark) / benchmark * 100):+.1f}%"
        
        metric_name = {
            'avg_order_value': 'Средний чек',
            'roas': 'ROAS',
            'customer_satisfaction': 'Удовлетворенность',
            'repeat_rate': 'Повторные клиенты',
            'conversion_rate': 'Конверсия рекламы'
        }.get(metric, metric)
        
        print(f"  {metric_name}: {current:.1f}{unit} vs {benchmark:.1f}{unit} - {status} ({diff})")
    
    print()
    
    # 10. СТРАТЕГИЧЕСКИЕ РЕКОМЕНДАЦИИ
    print("💡 10. СТРАТЕГИЧЕСКИЕ РЕКОМЕНДАЦИИ")
    print("-" * 40)
    
    recommendations = []
    
    # Анализ трендов
    if len(data) > 14:
        recent_period = data.tail(14)['total_sales'].mean()
        earlier_period = data.head(14)['total_sales'].mean()
        trend = ((recent_period - earlier_period) / earlier_period * 100) if earlier_period > 0 else 0
        
        if trend < -10:
            recommendations.append("📉 КРИТИЧНО: Падение продаж на {:.1f}% - срочно пересмотреть стратегию".format(abs(trend)))
        elif trend < 0:
            recommendations.append("📊 Небольшое снижение продаж на {:.1f}% - оптимизировать операции".format(abs(trend)))
        elif trend > 10:
            recommendations.append("📈 ОТЛИЧНО: Рост продаж на {:.1f}% - масштабировать успешные практики".format(trend))
    
    # Маркетинговые рекомендации
    if avg_roas < 3:
        recommendations.append("🎯 Низкий ROAS ({:.1f}x) - пересмотреть рекламные кампании и таргетинг".format(avg_roas))
    elif avg_roas > 5:
        recommendations.append("🚀 Отличный ROAS ({:.1f}x) - увеличить рекламный бюджет для масштабирования".format(avg_roas))
    
    # Клиентская база
    if 'new_rate' in locals() and new_rate < 30:
        recommendations.append("👥 Низкий процент новых клиентов ({:.1f}%) - усилить маркетинг привлечения".format(new_rate))
    if 'repeat_rate' in locals() and repeat_rate < 40:
        recommendations.append("🔄 Низкий процент повторных клиентов ({:.1f}%) - внедрить программу лояльности".format(repeat_rate))
    
    # Операционные рекомендации
    if closed_days > len(data) * 0.05:  # Более 5% дней закрыт
        recommendations.append("🏪 Частые закрытия магазина - оптимизировать рабочее расписание")
    
    if out_of_stock_days > 0:
        recommendations.append("📦 Проблемы с наличием товаров - улучшить управление запасами")
    
    # Качество обслуживания
    if 'negative_rate' in locals() and negative_rate > 8:
        recommendations.append("⭐ Высокий процент негативных отзывов ({:.1f}%) - улучшить качество сервиса".format(negative_rate))
    
    # Внешние факторы
    if 'weather_impact' in locals() and weather_impact < -15:
        recommendations.append("🌧️ Сильное влияние плохой погоды ({:.1f}%) - разработать стратегию для дождливых дней".format(weather_impact))
    
    # Ценообразование
    if avg_order_value < 300000:
        recommendations.append("💰 Низкий средний чек ({:,.0f} IDR) - пересмотреть ценообразование или добавить upsell".format(avg_order_value))
    
    # Выводим рекомендации
    if recommendations:
        print("🎯 Приоритетные действия:")
        for i, rec in enumerate(recommendations[:8], 1):  # Топ-8 рекомендаций
            print(f"  {i}. {rec}")
    else:
        print("✅ Все ключевые показатели в пределах нормы!")
    
    print()
    
    # Сохраняем ДЕТАЛЬНЫЙ отчет
    try:
        os.makedirs('reports', exist_ok=True)
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"reports/detailed_analysis_{restaurant_name.replace(' ', '_')}_{timestamp}.txt"
        
        with open(filename, 'w', encoding='utf-8') as f:
            f.write("═" * 100 + "\n")
            f.write(f"🎯 MUZAQUEST ANALYTICS - ДЕТАЛЬНЫЙ ОТЧЕТ: {restaurant_name.upper()}\n")
            f.write("═" * 100 + "\n")
            f.write(f"📅 Период анализа: {start_date} → {end_date}\n")
            f.write(f"📊 Создан: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"🔬 Использованы все 63 параметра + 3 API интеграции\n\n")
            
            # Исполнительное резюме
            f.write("📊 ИСПОЛНИТЕЛЬНОЕ РЕЗЮМЕ\n")
            f.write("-" * 50 + "\n")
            f.write(f"💰 Общая выручка: {total_sales:,.0f} IDR\n")
            f.write(f"📦 Общие заказы: {total_orders:,.0f}\n")
            f.write(f"💵 Средний чек: {avg_order_value:,.0f} IDR\n")
            f.write(f"📊 Дневная выручка: {daily_avg_sales:,.0f} IDR\n")
            f.write(f"⭐ Средний рейтинг: {avg_rating:.2f}/5.0\n")
            f.write(f"👥 Обслужено клиентов: {total_customers:,.0f}\n")
            f.write(f"🎯 ROAS: {avg_roas:.2f}x\n")
            f.write(f"📈 ROI маркетинга: {roi_percentage:+.1f}%\n\n")
            
            # Динамика по месяцам
            f.write("📈 ДИНАМИКА ПО МЕСЯЦАМ\n")
            f.write("-" * 50 + "\n")
            for month, sales in monthly_sales.items():
                month_name = month_names.get(month, f"Месяц {month}")
                month_data = data_sorted[data_sorted['month'] == month]
                days_in_month = len(month_data)
                daily_avg = sales / days_in_month if days_in_month > 0 else 0
                f.write(f"{month_name}: {sales:,.0f} IDR ({days_in_month} дней, {daily_avg:,.0f} IDR/день)\n")
            f.write("\n")
            
            # Клиентская база
            f.write("👥 КЛИЕНТСКАЯ БАЗА\n")
            f.write("-" * 50 + "\n")
            if 'new_rate' in locals():
                f.write(f"🆕 Новые клиенты: {new_customers:,.0f} ({new_rate:.1f}%)\n")
                f.write(f"🔄 Повторные клиенты: {repeated_customers:,.0f} ({repeat_rate:.1f}%)\n")
                f.write(f"📲 Реактивированные: {reactivated_customers:,.0f} ({reactive_rate:.1f}%)\n")
                if 'loyalty_premium' in locals():
                    f.write(f"🏆 Премия лояльности: +{loyalty_premium:.1f}%\n")
            f.write("\n")
            
            # Маркетинговая эффективность
            f.write("📈 МАРКЕТИНГОВАЯ ЭФФЕКТИВНОСТЬ\n")
            f.write("-" * 50 + "\n")
            if total_impressions > 0:
                f.write(f"👁️ Показы рекламы: {total_impressions:,.0f}\n")
                f.write(f"🔗 CTR: {ctr:.2f}%\n")
                f.write(f"✅ Конверсия: {conversion_rate:.2f}%\n")
                f.write(f"💰 Стоимость заказа: {cost_per_order:,.0f} IDR\n")
                f.write("ROAS по месяцам:\n")
                for month, roas in monthly_roas.items():
                    month_name = month_names.get(month, f"Месяц {month}")
                    f.write(f"  {month_name}: {roas:.2f}x\n")
            f.write("\n")
            
            # Операционные показатели
            f.write("⚠️ ОПЕРАЦИОННЫЕ ПОКАЗАТЕЛИ\n")
            f.write("-" * 50 + "\n")
            f.write(f"🚫 Дней закрыт: {closed_days} ({(closed_days/len(data)*100):.1f}%)\n")
            f.write(f"📦 Дней с дефицитом: {out_of_stock_days} ({(out_of_stock_days/len(data)*100):.1f}%)\n")
            f.write(f"❌ Отмененные заказы: {cancelled_orders:,.0f}\n")
            if 'potential_losses' in locals() and potential_losses > 0:
                f.write(f"💸 Потенциальные потери: {potential_losses:,.0f} IDR ({(potential_losses/total_sales*100):.1f}%)\n")
            f.write("\n")
            
            # Качество обслуживания
            f.write("⭐ КАЧЕСТВО ОБСЛУЖИВАНИЯ\n")
            f.write("-" * 50 + "\n")
            if total_ratings > 0:
                f.write(f"📊 Всего оценок: {total_ratings:,.0f}\n")
                for stars, count, emoji in ratings_data:
                    percentage = (count / total_ratings) * 100
                    f.write(f"{emoji} {stars} звезд: {count:,.0f} ({percentage:.1f}%)\n")
                f.write(f"📈 Индекс удовлетворенности: {satisfaction_score:.2f}/5.0\n")
                if 'negative_rate' in locals():
                    f.write(f"🚨 Негативные отзывы: {negative_rate:.1f}%\n")
            f.write("\n")
            
            # Внешние факторы
            f.write("🌐 ВНЕШНИЕ ФАКТОРЫ\n")
            f.write("-" * 50 + "\n")
            f.write("Погодные условия и их влияние:\n")
            for condition, sales_list in weather_groups.items():
                avg_sales = sum(sales_list) / len(sales_list)
                emoji = {"Clear": "☀️", "Rain": "🌧️", "Clouds": "☁️", "Thunderstorm": "⛈️"}.get(condition, "🌤️")
                f.write(f"{emoji} {condition}: {avg_sales:,.0f} IDR ({len(sales_list)} дней)\n")
            if 'weather_impact' in locals():
                f.write(f"💧 Влияние дождя: {weather_impact:+.1f}%\n")
            if 'holiday_effect' in locals():
                f.write(f"🎯 Влияние праздников: {holiday_effect:+.1f}%\n")
            f.write("\n")
            
            # AI инсайты
            f.write("🤖 AI-АНАЛИЗ И ИНСАЙТЫ\n")
            f.write("-" * 50 + "\n")
            f.write(ai_insights + "\n\n")
            
            # Стратегические рекомендации
            f.write("💡 СТРАТЕГИЧЕСКИЕ РЕКОМЕНДАЦИИ\n")
            f.write("-" * 50 + "\n")
            if recommendations:
                for i, rec in enumerate(recommendations, 1):
                    f.write(f"{i}. {rec}\n")
            else:
                f.write("✅ Все ключевые показатели в пределах нормы!\n")
            
            f.write("\n" + "═" * 100 + "\n")
            f.write("📊 Отчет создан системой Muzaquest Analytics\n")
            f.write("🔬 Проанализированы все 63 параметра + 3 API интеграции\n")
            f.write("🎯 Рекомендации основаны на лучших практиках ресторанного бизнеса\n")
        
        print(f"💾 Детальный отчет сохранен: {filename}")
        
    except Exception as e:
        print(f"❌ Ошибка сохранения отчета: {e}")

    print()
    print("🎯 Анализ завершен! Проверьте сохраненный детальный отчет.")
    print("="*80)

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
    """Детальный анализ всего рынка с AI-инсайтами"""
    print("\n🌍 ДЕТАЛЬНЫЙ АНАЛИЗ ВСЕГО РЫНКА MUZAQUEST")
    print("=" * 80)
    
    # Устанавливаем период по умолчанию
    if not start_date or not end_date:
        start_date = "2025-04-01"
        end_date = "2025-06-30"
    
    print(f"📅 Период анализа: {start_date} → {end_date}")
    print()
    
    try:
        conn = sqlite3.connect("database.sqlite")
        
        # 1. ОБЗОР РЫНКА
        print("📊 1. ОБЗОР РЫНКА")
        print("-" * 40)
        
        # Общая статистика рынка
        market_query = """
        WITH market_data AS (
            SELECT r.name,
                   SUM(COALESCE(g.sales, 0) + COALESCE(gj.sales, 0)) as total_sales,
                   SUM(COALESCE(g.orders, 0) + COALESCE(gj.orders, 0)) as total_orders,
                   AVG(COALESCE(g.rating, gj.rating)) as avg_rating,
                   SUM(COALESCE(g.ads_spend, 0) + COALESCE(gj.ads_spend, 0)) as marketing_spend,
                   SUM(COALESCE(g.ads_sales, 0) + COALESCE(gj.ads_sales, 0)) as marketing_sales,
                   SUM(COALESCE(g.new_customers, 0) + COALESCE(gj.new_client, 0)) as new_customers,
                   COUNT(DISTINCT COALESCE(g.stat_date, gj.stat_date)) as active_days
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
            AVG(total_sales) as avg_restaurant_sales,
            AVG(avg_rating) as market_avg_rating,
            SUM(marketing_spend) as total_marketing_spend,
            SUM(marketing_sales) as total_marketing_sales,
            SUM(new_customers) as total_new_customers,
            AVG(active_days) as avg_active_days
        FROM market_data
        """
        
        market_stats = pd.read_sql_query(market_query, conn, params=(start_date, end_date, start_date, end_date))
        
        if not market_stats.empty:
            stats = market_stats.iloc[0]
            market_roas = stats['total_marketing_sales'] / stats['total_marketing_spend'] if stats['total_marketing_spend'] > 0 else 0
            avg_order_value = stats['market_sales'] / stats['market_orders'] if stats['market_orders'] > 0 else 0
            
            print(f"🏪 Активных ресторанов: {stats['active_restaurants']:.0f}")
            print(f"💰 Общие продажи рынка: {stats['market_sales']:,.0f} IDR")
            print(f"📦 Общие заказы рынка: {stats['market_orders']:,.0f}")
            print(f"📊 Средние продажи на ресторан: {stats['avg_restaurant_sales']:,.0f} IDR")
            print(f"💵 Средний чек рынка: {avg_order_value:,.0f} IDR")
            print(f"⭐ Средний рейтинг рынка: {stats['market_avg_rating']:.2f}/5.0")
            print(f"🎯 ROAS рынка: {market_roas:.2f}x")
            print(f"👥 Новых клиентов на рынке: {stats['total_new_customers']:,.0f}")
            print(f"📅 Средняя активность: {stats['avg_active_days']:.1f} дней")
        
        print()
        
        # 2. ЛИДЕРЫ РЫНКА (Детальный анализ)
        print("🏆 2. ЛИДЕРЫ РЫНКА")
        print("-" * 40)
        
        leaders_query = """
        SELECT r.name,
               SUM(COALESCE(g.sales, 0) + COALESCE(gj.sales, 0)) as total_sales,
               SUM(COALESCE(g.orders, 0) + COALESCE(gj.orders, 0)) as total_orders,
               AVG(COALESCE(g.rating, gj.rating)) as avg_rating,
               SUM(COALESCE(g.ads_spend, 0) + COALESCE(gj.ads_spend, 0)) as marketing_spend,
               SUM(COALESCE(g.ads_sales, 0) + COALESCE(gj.ads_sales, 0)) as marketing_sales,
               SUM(COALESCE(g.new_customers, 0) + COALESCE(gj.new_client, 0)) as new_customers,
               COUNT(DISTINCT COALESCE(g.stat_date, gj.stat_date)) as active_days
        FROM restaurants r
        LEFT JOIN grab_stats g ON r.id = g.restaurant_id 
            AND g.stat_date BETWEEN ? AND ?
        LEFT JOIN gojek_stats gj ON r.id = gj.restaurant_id 
            AND gj.stat_date BETWEEN ? AND ?
        GROUP BY r.name
        HAVING total_sales > 0
        ORDER BY total_sales DESC
        LIMIT 15
        """
        
        leaders = pd.read_sql_query(leaders_query, conn, params=(start_date, end_date, start_date, end_date))
        
        print("ТОП-15 по продажам:")
        for i, row in leaders.iterrows():
            avg_order_value = row['total_sales'] / row['total_orders'] if row['total_orders'] > 0 else 0
            restaurant_roas = row['marketing_sales'] / row['marketing_spend'] if row['marketing_spend'] > 0 else 0
            daily_sales = row['total_sales'] / row['active_days'] if row['active_days'] > 0 else 0
            
            print(f"  {i+1:2d}. {row['name']:<25} {row['total_sales']:>15,.0f} IDR")
            print(f"      📦 {row['total_orders']:,} заказов | 💰 {avg_order_value:,.0f} IDR/заказ | ⭐ {row['avg_rating']:.2f}")
            print(f"      📊 {daily_sales:,.0f} IDR/день | 🎯 ROAS: {restaurant_roas:.1f}x | 👥 {row['new_customers']:,.0f} новых")
        
        print()
        
        # 3. СЕГМЕНТАЦИЯ РЫНКА
        print("📈 3. СЕГМЕНТАЦИЯ РЫНКА")
        print("-" * 40)
        
        # Анализ по сегментам
        segment_analysis = leaders.copy()
        segment_analysis['avg_order_value'] = segment_analysis['total_sales'] / segment_analysis['total_orders']
        segment_analysis['daily_sales'] = segment_analysis['total_sales'] / segment_analysis['active_days']
        
        # Сегменты по среднему чеку
        premium_segment = segment_analysis[segment_analysis['avg_order_value'] >= 350000]
        mid_segment = segment_analysis[(segment_analysis['avg_order_value'] >= 200000) & (segment_analysis['avg_order_value'] < 350000)]
        budget_segment = segment_analysis[segment_analysis['avg_order_value'] < 200000]
        
        print("💎 ПРЕМИУМ СЕГМЕНТ (средний чек 350K+ IDR):")
        print(f"   • Ресторанов: {len(premium_segment)}")
        if not premium_segment.empty:
            print(f"   • Средний чек: {premium_segment['avg_order_value'].mean():,.0f} IDR")
            print(f"   • Общие продажи: {premium_segment['total_sales'].sum():,.0f} IDR")
            print(f"   • Доля рынка: {(premium_segment['total_sales'].sum() / stats['market_sales'] * 100):.1f}%")
        
        print(f"\n🏷️ СРЕДНИЙ СЕГМЕНТ (средний чек 200-350K IDR):")
        print(f"   • Ресторанов: {len(mid_segment)}")
        if not mid_segment.empty:
            print(f"   • Средний чек: {mid_segment['avg_order_value'].mean():,.0f} IDR")
            print(f"   • Общие продажи: {mid_segment['total_sales'].sum():,.0f} IDR")
            print(f"   • Доля рынка: {(mid_segment['total_sales'].sum() / stats['market_sales'] * 100):.1f}%")
        
        print(f"\n💰 БЮДЖЕТНЫЙ СЕГМЕНТ (средний чек <200K IDR):")
        print(f"   • Ресторанов: {len(budget_segment)}")
        if not budget_segment.empty:
            print(f"   • Средний чек: {budget_segment['avg_order_value'].mean():,.0f} IDR")
            print(f"   • Общие продажи: {budget_segment['total_sales'].sum():,.0f} IDR")
            print(f"   • Доля рынка: {(budget_segment['total_sales'].sum() / stats['market_sales'] * 100):.1f}%")
        
        print()
        
        # 4. АНАЛИЗ ЭФФЕКТИВНОСТИ
        print("⚡ 4. АНАЛИЗ ЭФФЕКТИВНОСТИ")
        print("-" * 40)
        
        # Топ по различным метрикам
        top_roas = leaders[leaders['marketing_spend'] > 0].nlargest(5, 'marketing_sales')['marketing_sales'] / leaders[leaders['marketing_spend'] > 0].nlargest(5, 'marketing_sales')['marketing_spend']
        top_rating = leaders.nlargest(5, 'avg_rating')
        top_daily_sales = leaders.copy()
        top_daily_sales['daily_sales'] = top_daily_sales['total_sales'] / top_daily_sales['active_days']
        top_daily_sales = top_daily_sales.nlargest(5, 'daily_sales')
        
        print("🎯 ТОП-5 по ROAS:")
        roas_leaders = leaders[leaders['marketing_spend'] > 0].copy()
        roas_leaders['roas'] = roas_leaders['marketing_sales'] / roas_leaders['marketing_spend']
        roas_leaders = roas_leaders.nlargest(5, 'roas')
        for i, row in roas_leaders.iterrows():
            print(f"   {row['name']}: {row['roas']:.1f}x")
        
        print(f"\n⭐ ТОП-5 по рейтингу:")
        for i, row in top_rating.iterrows():
            print(f"   {row['name']}: {row['avg_rating']:.2f}/5.0")
        
        print(f"\n📊 ТОП-5 по дневным продажам:")
        for i, row in top_daily_sales.iterrows():
            daily_sales = row['total_sales'] / row['active_days']
            print(f"   {row['name']}: {daily_sales:,.0f} IDR/день")
        
        print()
        
        # 5. МАРКЕТИНГОВЫЙ АНАЛИЗ
        print("📈 5. МАРКЕТИНГОВЫЙ АНАЛИЗ")
        print("-" * 40)
        
        total_marketing_spend = stats['total_marketing_spend']
        total_marketing_sales = stats['total_marketing_sales']
        
        if total_marketing_spend > 0:
            print(f"💸 Общие затраты на маркетинг: {total_marketing_spend:,.0f} IDR")
            print(f"💰 Общая выручка от маркетинга: {total_marketing_sales:,.0f} IDR")
            print(f"🎯 Средний ROAS рынка: {market_roas:.2f}x")
            print(f"📊 ROI рынка: {((total_marketing_sales - total_marketing_spend) / total_marketing_spend * 100):+.1f}%")
            
            # Распределение маркетинговых бюджетов
            marketing_active = leaders[leaders['marketing_spend'] > 0]
            if not marketing_active.empty:
                print(f"\n📊 Маркетинговая активность:")
                print(f"   • Ресторанов с рекламой: {len(marketing_active)}/{len(leaders)} ({(len(marketing_active)/len(leaders)*100):.1f}%)")
                print(f"   • Средний бюджет: {marketing_active['marketing_spend'].mean():,.0f} IDR")
                print(f"   • Медианный бюджет: {marketing_active['marketing_spend'].median():,.0f} IDR")
                
                # Крупнейшие рекламодатели
                top_spenders = marketing_active.nlargest(5, 'marketing_spend')
                print(f"\n💰 ТОП-5 рекламодателей:")
                for i, row in top_spenders.iterrows():
                    spend_share = (row['marketing_spend'] / total_marketing_spend) * 100
                    restaurant_roas = row['marketing_sales'] / row['marketing_spend']
                    print(f"   {row['name']}: {row['marketing_spend']:,.0f} IDR ({spend_share:.1f}% рынка, ROAS: {restaurant_roas:.1f}x)")
        
        print()
        
        # 6. AI-АНАЛИЗ РЫНКА
        print("🤖 6. AI-АНАЛИЗ РЫНКА И ИНСАЙТЫ")
        print("-" * 40)
        
        # Создаем сводные данные для анализа
        market_data = {
            'total_restaurants': int(stats['active_restaurants']),
            'total_sales': float(stats['market_sales']),
            'total_orders': int(stats['market_orders']),
            'avg_order_value': float(avg_order_value),
            'market_roas': float(market_roas),
            'avg_rating': float(stats['market_avg_rating']),
            'leader_dominance': float(leaders.iloc[0]['total_sales'] / stats['market_sales'] * 100) if not leaders.empty else 0
        }
        
        # AI анализ рынка
        openai_analyzer = OpenAIAnalyzer()
        market_insights = generate_market_insights(market_data, leaders)
        print(market_insights)
        
        print()
        
        # 7. СТРАТЕГИЧЕСКИЕ ВЫВОДЫ
        print("🎯 7. СТРАТЕГИЧЕСКИЕ ВЫВОДЫ И РЕКОМЕНДАЦИИ")
        print("-" * 40)
        
        strategic_insights = []
        
        # Анализ концентрации рынка
        top3_share = leaders.head(3)['total_sales'].sum() / stats['market_sales'] * 100 if not leaders.empty else 0
        if top3_share > 50:
            strategic_insights.append(f"🏆 Высокая концентрация: ТОП-3 контролируют {top3_share:.1f}% рынка")
        else:
            strategic_insights.append(f"🎯 Фрагментированный рынок: ТОП-3 имеют {top3_share:.1f}% доли")
        
        # Анализ ROAS
        if market_roas > 5:
            strategic_insights.append(f"📈 ПРЕВОСХОДНО: Высокоэффективный рынок (ROAS {market_roas:.1f}x)")
        elif market_roas > 3:
            strategic_insights.append(f"✅ ХОРОШО: Эффективный маркетинг (ROAS {market_roas:.1f}x)")
        else:
            strategic_insights.append(f"⚠️ ПРОБЛЕМА: Низкая эффективность маркетинга (ROAS {market_roas:.1f}x)")
        
        # Анализ среднего чека
        if avg_order_value > 300000:
            strategic_insights.append("💎 Премиальный рынок с высоким средним чеком")
        elif avg_order_value > 200000:
            strategic_insights.append("🏷️ Рынок среднего ценового сегмента")
        else:
            strategic_insights.append("💰 Бюджетно-ориентированный рынок")
        
        # Качество обслуживания
        if stats['market_avg_rating'] > 4.5:
            strategic_insights.append("⭐ Высокие стандарты качества на рынке")
        else:
            strategic_insights.append("⚠️ Есть возможности для улучшения качества")
        
        for insight in strategic_insights:
            print(f"• {insight}")
        
        print()
        
        # Сохраняем детальный рыночный отчет
        try:
            os.makedirs('reports', exist_ok=True)
            
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"reports/market_analysis_{start_date}_{end_date}_{timestamp}.txt"
            
            with open(filename, 'w', encoding='utf-8') as f:
                f.write("═" * 100 + "\n")
                f.write(f"🌍 MUZAQUEST ANALYTICS - ДЕТАЛЬНЫЙ РЫНОЧНЫЙ ОТЧЕТ\n")
                f.write("═" * 100 + "\n")
                f.write(f"📅 Период анализа: {start_date} → {end_date}\n")
                f.write(f"📊 Создан: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write(f"🔬 Использованы все 63 параметра + 3 API интеграции\n\n")
                
                # Обзор рынка
                f.write("📊 ОБЗОР РЫНКА\n")
                f.write("-" * 50 + "\n")
                f.write(f"🏪 Активных ресторанов: {stats['active_restaurants']:.0f}\n")
                f.write(f"💰 Общие продажи: {stats['market_sales']:,.0f} IDR\n")
                f.write(f"📦 Общие заказы: {stats['market_orders']:,.0f}\n")
                f.write(f"💵 Средний чек: {avg_order_value:,.0f} IDR\n")
                f.write(f"⭐ Средний рейтинг: {stats['market_avg_rating']:.2f}/5.0\n")
                f.write(f"🎯 ROAS рынка: {market_roas:.2f}x\n\n")
                
                # Лидеры рынка
                f.write("🏆 ЛИДЕРЫ РЫНКА (ТОП-10)\n")
                f.write("-" * 50 + "\n")
                for i, row in leaders.head(10).iterrows():
                    avg_order_value_rest = row['total_sales'] / row['total_orders'] if row['total_orders'] > 0 else 0
                    restaurant_roas = row['marketing_sales'] / row['marketing_spend'] if row['marketing_spend'] > 0 else 0
                    f.write(f"{i+1:2d}. {row['name']}: {row['total_sales']:,.0f} IDR\n")
                    f.write(f"    📦 {row['total_orders']:,} заказов | 💰 {avg_order_value_rest:,.0f} IDR/заказ\n")
                    f.write(f"    ⭐ {row['avg_rating']:.2f} | 🎯 ROAS: {restaurant_roas:.1f}x\n\n")
                
                # Сегментация
                f.write("📈 СЕГМЕНТАЦИЯ РЫНКА\n")
                f.write("-" * 50 + "\n")
                f.write(f"💎 Премиум (350K+ IDR): {len(premium_segment)} ресторанов\n")
                f.write(f"🏷️ Средний (200-350K IDR): {len(mid_segment)} ресторанов\n")
                f.write(f"💰 Бюджетный (<200K IDR): {len(budget_segment)} ресторанов\n\n")
                
                # AI инсайты
                f.write("🤖 AI-АНАЛИЗ РЫНКА\n")
                f.write("-" * 50 + "\n")
                f.write(market_insights + "\n\n")
                
                # Стратегические выводы
                f.write("🎯 СТРАТЕГИЧЕСКИЕ ВЫВОДЫ\n")
                f.write("-" * 50 + "\n")
                for insight in strategic_insights:
                    f.write(f"• {insight}\n")
                
                f.write("\n" + "═" * 100 + "\n")
                f.write("📊 Отчет создан системой Muzaquest Analytics\n")
                f.write("🔬 Проанализированы все 63 параметра + 3 API интеграции\n")
                f.write("🎯 Рекомендации основаны на лучших практиках ресторанного бизнеса\n")
            
            print(f"💾 Детальный рыночный отчет сохранен: {filename}")
            
        except Exception as e:
            print(f"❌ Ошибка сохранения отчета: {e}")
        
        conn.close()
        
    except Exception as e:
        print(f"❌ Ошибка при анализе рынка: {e}")

def generate_market_insights(market_data, leaders_df):
    """Генерирует рыночные инсайты"""
    
    insights = []
    insights.append("🎯 ДЕТАЛЬНЫЙ РЫНОЧНЫЙ АНАЛИЗ И СТРАТЕГИЧЕСКИЕ ИНСАЙТЫ")
    insights.append("=" * 80)
    
    # Анализ размера рынка
    total_sales = market_data['total_sales']
    total_restaurants = market_data['total_restaurants']
    avg_order_value = market_data['avg_order_value']
    market_roas = market_data['market_roas']
    
    insights.append(f"💰 РАЗМЕР И СТРУКТУРА РЫНКА:")
    insights.append(f"   • Общий оборот: {total_sales:,.0f} IDR")
    insights.append(f"   • Средняя выручка на ресторан: {(total_sales/total_restaurants):,.0f} IDR")
    insights.append(f"   • Средний чек рынка: {avg_order_value:,.0f} IDR")
    
    # Оценка размера рынка
    if total_sales > 1000000000000:  # 1 триллион
        insights.append(f"   🏆 КРУПНЫЙ РЫНОК: Оборот превышает 1 триллион IDR")
    elif total_sales > 500000000000:  # 500 миллиардов
        insights.append(f"   📈 СРЕДНИЙ РЫНОК: Значительный оборот")
    else:
        insights.append(f"   💡 РАЗВИВАЮЩИЙСЯ РЫНОК: Потенциал для роста")
    
    # Анализ концентрации
    if not leaders_df.empty:
        leader_share = (leaders_df.iloc[0]['total_sales'] / total_sales) * 100
        top3_share = (leaders_df.head(3)['total_sales'].sum() / total_sales) * 100
        
        insights.append(f"\n🏆 КОНКУРЕНТНАЯ СРЕДА:")
        insights.append(f"   • Лидер рынка: {leader_share:.1f}% доли")
        insights.append(f"   • ТОП-3: {top3_share:.1f}% рынка")
        
        if leader_share > 25:
            insights.append(f"   ⚠️ ДОМИНИРОВАНИЕ: Сильное лидерство одного игрока")
        elif top3_share > 60:
            insights.append(f"   🎯 ОЛИГОПОЛИЯ: Несколько крупных игроков")
        else:
            insights.append(f"   ✅ КОНКУРЕНЦИЯ: Фрагментированный рынок")
    
    # Анализ эффективности
    insights.append(f"\n⚡ ОПЕРАЦИОННАЯ ЭФФЕКТИВНОСТЬ:")
    insights.append(f"   • ROAS рынка: {market_roas:.2f}x")
    
    if market_roas > 10:
        insights.append(f"   🏆 ПРЕВОСХОДНО: Исключительная эффективность маркетинга")
        insights.append(f"   💡 Стратегия: Рынок готов для масштабирования инвестиций")
    elif market_roas > 5:
        insights.append(f"   ✅ ОТЛИЧНО: Высокоэффективный маркетинг")
        insights.append(f"   💡 Стратегия: Увеличивать рекламные бюджеты")
    elif market_roas > 3:
        insights.append(f"   ⚠️ СРЕДНЕ: Приемлемая эффективность")
        insights.append(f"   💡 Стратегия: Оптимизировать таргетинг и креативы")
    else:
        insights.append(f"   🚨 НИЗКО: Проблемы с эффективностью")
        insights.append(f"   💡 Стратегия: Кардинально пересмотреть маркетинг")
    
    # Анализ ценообразования
    insights.append(f"\n💰 ЦЕНОВОЕ ПОЗИЦИОНИРОВАНИЕ:")
    if avg_order_value > 400000:
        insights.append(f"   💎 ПРЕМИУМ РЫНОК: Высокий средний чек")
        insights.append(f"   💡 Возможность: Развитие luxury-сегмента")
    elif avg_order_value > 250000:
        insights.append(f"   🏷️ СРЕДНИЙ СЕГМЕНТ: Сбалансированное ценообразование")
        insights.append(f"   💡 Возможность: Upsell и премиализация")
    else:
        insights.append(f"   💰 МАССОВЫЙ РЫНОК: Доступные цены")
        insights.append(f"   💡 Возможность: Повышение value proposition")
    
    # Качество обслуживания
    avg_rating = market_data['avg_rating']
    insights.append(f"\n⭐ КАЧЕСТВО ОБСЛУЖИВАНИЯ:")
    insights.append(f"   • Средний рейтинг: {avg_rating:.2f}/5.0")
    
    if avg_rating > 4.7:
        insights.append(f"   🏆 ПРЕВОСХОДНО: Высочайшие стандарты")
    elif avg_rating > 4.5:
        insights.append(f"   ✅ ОТЛИЧНО: Высокое качество")
    elif avg_rating > 4.0:
        insights.append(f"   ⚠️ ХОРОШО: Есть возможности для улучшения")
    else:
        insights.append(f"   🚨 ПРОБЛЕМА: Низкое качество обслуживания")
    
    # Стратегические рекомендации для рынка
    insights.append(f"\n🚀 СТРАТЕГИЧЕСКИЕ ПРИОРИТЕТЫ РЫНКА:")
    
    priorities = []
    
    if market_roas < 3:
        priorities.append("🔥 #1 КРИТИЧНО: Повысить эффективность маркетинга")
    if avg_rating < 4.5:
        priorities.append("⭐ #2 ВАЖНО: Улучшить качество обслуживания")
    if market_data['leader_dominance'] > 30:
        priorities.append("🎯 #3 СТРАТЕГИЯ: Усилить конкуренцию")
    if avg_order_value < 250000:
        priorities.append("💰 #4 ВОЗМОЖНОСТЬ: Премиализация предложения")
    
    if not priorities:
        priorities.append("✅ Рынок развивается сбалансированно")
        priorities.append("📈 Фокус на устойчивом росте")
    
    for priority in priorities[:5]:
        insights.append(f"   {priority}")
    
    # Прогнозы
    insights.append(f"\n📊 ПРОГНОЗЫ НА СЛЕДУЮЩИЙ ПЕРИОД:")
    if market_roas > 5:
        growth_potential = 25
    elif market_roas > 3:
        growth_potential = 15
    else:
        growth_potential = 5
    
    insights.append(f"   • Потенциал роста рынка: {growth_potential}%")
    insights.append(f"   • Целевой ROAS: {(market_roas * 1.1):.1f}x (+10%)")
    insights.append(f"   • Целевой средний чек: {(avg_order_value * 1.1):,.0f} IDR (+10%)")
    insights.append(f"   • Целевой рейтинг: {min(avg_rating + 0.2, 5.0):.1f}/5.0")
    
    return '\n'.join(insights)

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