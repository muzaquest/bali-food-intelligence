#!/usr/bin/env python3
"""
🎯 ПРОДАКШН АНАЛИЗАТОР ПРОДАЖ ДЛЯ REPLIT ИНТЕГРАЦИИ
═══════════════════════════════════════════════════════════════════════════════
Полнофункциональный детективный анализ с интеграцией в main.py

✅ ВКЛЮЧАЕТ ВСЕ НОВЫЕ ФУНКЦИИ:
- Анализ временных факторов с отклонениями от среднемесячных
- ROAS и эффективность рекламы
- Driver Waiting Time, Preparation Time, Delivery Time
- Close Time (выключение программы)
- Погода и праздники
- Операционные факторы
- Профессиональные отчеты
"""

import sqlite3
import pandas as pd
import json
import requests
from datetime import datetime, timedelta
import warnings
import sys
import os
warnings.filterwarnings('ignore')

# Добавляем путь к utils для импорта fake_orders_filter
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))
try:
    from src.utils.fake_orders_filter import get_fake_orders_filter
    FAKE_ORDERS_AVAILABLE = True
except ImportError:
    print("⚠️ Fake orders filter недоступен")
    FAKE_ORDERS_AVAILABLE = False

class ProductionSalesAnalyzer:
    """Продакшн анализатор для детективного анализа продаж"""
    
    def __init__(self):
        self.holidays_data = self._load_holidays()
        self.locations_data = self._load_locations()
        
        # Проверяем доступность ML
        self.ml_available = self._check_ml_availability()
        
        # Инициализируем fake orders filter
        if FAKE_ORDERS_AVAILABLE:
            self.fake_orders_filter = get_fake_orders_filter()
            print(f"✅ Fake orders filter загружен: {len(self.fake_orders_filter.fake_orders_data)} записей")
        else:
            self.fake_orders_filter = None
        
    def _check_ml_availability(self):
        """Проверяет доступность ML библиотек"""
        try:
            import sklearn
            import shap
            return True
        except ImportError:
            return False
        
    def _load_holidays(self):
        """Загружаем данные о праздниках"""
        try:
            with open('data/comprehensive_holiday_analysis.json', 'r', encoding='utf-8') as f:
                data = json.load(f)
            return data.get('results', {})
        except Exception as e:
            print(f"⚠️ Не удалось загрузить праздники: {e}")
            return {}
    
    def _load_locations(self):
        """Загружаем данные о локациях ресторанов"""
        try:
            with open('data/bali_restaurant_locations.json', 'r', encoding='utf-8') as f:
                data = json.load(f)
                # Преобразуем в словарь name -> location
                locations = {}
                if 'restaurants' in data:
                    for restaurant in data['restaurants']:
                        locations[restaurant['name']] = {
                            'latitude': restaurant['latitude'],
                            'longitude': restaurant['longitude'],
                            'location': restaurant['location']
                        }
                return locations
        except Exception as e:
            print(f"⚠️ Не удалось загрузить локации: {e}")
            return {}
    
    def analyze_restaurant_performance(self, restaurant_name, start_date, end_date, use_ml=True):
        """
        Главная функция анализа - совместимая с main.py
        Возвращает список строк для вывода
        
        Args:
            restaurant_name: Название ресторана
            start_date: Начальная дата
            end_date: Конечная дата  
            use_ml: Использовать ML интеграцию если доступна
        """
        
        # Если ML доступен и запрошен - используем интегрированный анализ
        if use_ml and self.ml_available:
            try:
                from .integrated_ml_detective import IntegratedMLDetective
                print("🤖 Используем ML-интегрированный анализ...")
                
                ml_detective = IntegratedMLDetective()
                return ml_detective.analyze_with_ml_explanations(
                    restaurant_name, start_date, end_date
                )
            except Exception as e:
                print(f"⚠️ ML анализ недоступен: {e}")
                print("📊 Переходим к стандартному детективному анализу...")
        
        # Стандартный детективный анализ
        return self._standard_detective_analysis(restaurant_name, start_date, end_date)
    
    def _standard_detective_analysis(self, restaurant_name, start_date, end_date):
        """Стандартный детективный анализ без ML"""
        try:
            # Определяем проблемные дни
            bad_days = self._find_bad_days(restaurant_name, start_date, end_date)
            
            if not bad_days:
                return [
                    f"✅ Отличные новости! У {restaurant_name} нет критических падений продаж",
                    f"📊 Период анализа: {start_date} - {end_date}",
                    f"🎯 Все дни показывают стабильную работу"
                ]
            
            results = []
            results.append(f"🔍 ДЕТЕКТИВНЫЙ АНАЛИЗ: {restaurant_name}")
            results.append(f"📅 Период: {start_date} - {end_date}")
            results.append(f"🚨 Найдено {len(bad_days)} проблемных дней")
            results.append("")
            
            # Анализируем каждый проблемный день
            for i, bad_day_info in enumerate(bad_days[:5], 1):  # Топ-5 худших дней
                date = bad_day_info[0]
                problem_percent = bad_day_info[1]
                problem_type = bad_day_info[2] if len(bad_day_info) > 2 else 'relative_drop'
                
                day_analysis = self._analyze_specific_day(restaurant_name, date)
                
                results.append(f"📉 ПРОБЛЕМНЫЙ ДЕНЬ #{i}: {date}")
                
                if problem_type == 'absolute_low':
                    results.append(f"   📉 Критически низкие продажи: {problem_percent:.1f}% ниже медианы")
                else:
                    results.append(f"   💔 Падение продаж: {problem_percent:.1f}%")
                results.append("")
                
                # Добавляем детальный анализ
                for line in day_analysis:
                    results.append(f"   {line}")
                results.append("")
            
            # Общие рекомендации
            results.extend(self._generate_general_recommendations(bad_days))
            
            return results
            
        except Exception as e:
            return [
                f"❌ Ошибка анализа {restaurant_name}: {e}",
                "🔧 Используется fallback анализ..."
            ]
    
    def _find_bad_days(self, restaurant_name, start_date, end_date):
        """Находит дни с критическим падением продаж"""
        with sqlite3.connect('database.sqlite') as conn:
            # Сначала найдем ID ресторана
            restaurant_query = f"SELECT id FROM restaurants WHERE name = '{restaurant_name}'"
            restaurant_df = pd.read_sql_query(restaurant_query, conn)
            
            if restaurant_df.empty:
                return []
                
            restaurant_id = restaurant_df.iloc[0]['id']
            
            # Получаем все даты с продажами из обеих таблиц
            query = f"""
            WITH all_dates AS (
                SELECT stat_date FROM grab_stats 
                WHERE restaurant_id = {restaurant_id} 
                AND stat_date BETWEEN '{start_date}' AND '{end_date}'
                UNION 
                SELECT stat_date FROM gojek_stats 
                WHERE restaurant_id = {restaurant_id}
                AND stat_date BETWEEN '{start_date}' AND '{end_date}'
            ),
            combined_sales AS (
                SELECT 
                    ad.stat_date,
                    COALESCE(g.sales, 0) + COALESCE(gj.sales, 0) as total_sales
                FROM all_dates ad
                LEFT JOIN grab_stats g ON ad.stat_date = g.stat_date AND g.restaurant_id = {restaurant_id}
                LEFT JOIN gojek_stats gj ON ad.stat_date = gj.stat_date AND gj.restaurant_id = {restaurant_id}
            )
            SELECT * FROM combined_sales 
            ORDER BY stat_date
            """
            
            df = pd.read_sql_query(query, conn)
        
        if len(df) < 7:  # Недостаточно данных
            return []
        
        # Рассчитываем скользящее среднее
        df['sales_7day_avg'] = df['total_sales'].rolling(window=7, center=True).mean()
        
        # Находим дни с падением больше 20%
        bad_days = []
        for _, row in df.iterrows():
            if pd.isna(row['sales_7day_avg']) or row['sales_7day_avg'] == 0:
                continue
                
            drop_percent = ((row['sales_7day_avg'] - row['total_sales']) / row['sales_7day_avg']) * 100
            if drop_percent >= 20:  # Падение больше 20%
                bad_days.append((row['stat_date'], drop_percent, 'relative_drop'))
        
        # Добавляем дни с критически низкими абсолютными продажами
        median_sales = df['total_sales'].median()
        low_threshold = median_sales * 0.7  # 70% от медианы
        
        for _, row in df.iterrows():
            if row['total_sales'] < low_threshold:
                # Проверяем что этот день еще не добавлен
                if not any(day[0] == row['stat_date'] for day in bad_days):
                    below_median_percent = ((median_sales - row['total_sales']) / median_sales) * 100
                    bad_days.append((row['stat_date'], below_median_percent, 'absolute_low'))
        
        # Сортируем по величине проблемы
        bad_days.sort(key=lambda x: x[1], reverse=True)
        return bad_days
    
    def _analyze_specific_day(self, restaurant_name, target_date):
        """Детальный анализ конкретного дня"""
        # Получаем данные дня
        day_data = self._get_day_data(restaurant_name, target_date)
        if not day_data:
            return ["❌ Нет данных за этот день"]
        
        # Получаем среднемесячные показатели
        monthly_averages = self._get_monthly_averages(restaurant_name, target_date)
        
        # Получаем погоду
        weather_data = self._get_weather_data(restaurant_name, target_date)
        
        # Проверяем праздники
        holiday_info = self.holidays_data.get(target_date)
        
        results = []
        
        # Основные показатели
        results.append(f"💰 Продажи: {day_data['total_sales']:,.0f} IDR ({day_data['total_orders']} заказов)")
        results.append(f"🟢 Grab: {day_data['grab_sales']:,.0f} IDR ({day_data['grab_orders']} заказов)")
        results.append(f"🟠 Gojek: {day_data['gojek_sales']:,.0f} IDR ({day_data['gojek_orders']} заказов)")
        
        # Анализ факторов
        factors = []
        impact_score = 0
        critical_issues = []
        
        # Fake orders уже исключены в исполнительном резюме, здесь не показываем
        
        # 1. Выключение программы
        if day_data.get('gojek_close_time', '00:00:00') != '00:00:00':
            outage_seconds = self._parse_time_string(day_data['gojek_close_time'])
            if outage_seconds >= 18000:  # > 5 часов
                factors.append(f"🚨 КРИТИЧНО: Gojek выключен {self._format_duration(outage_seconds)}")
                impact_score += 50
                critical_issues.append("Критическое выключение Gojek")
            elif outage_seconds >= 3600:  # > 1 часа
                factors.append(f"⚠️ Gojek выключен {self._format_duration(outage_seconds)}")
                impact_score += 30
        
        if day_data.get('grab_offline_rate', 0) > 0:
            offline_rate = day_data['grab_offline_rate']
            if offline_rate >= 300:  # > 5 часов
                factors.append(f"🚨 КРИТИЧНО: Grab offline {offline_rate:.1f}%")
                impact_score += 50
                critical_issues.append("Критическое выключение Grab")
            elif offline_rate >= 60:  # > 1 часа
                factors.append(f"⚠️ Grab offline {offline_rate:.1f}%")
                impact_score += 30
        
        # 2. Временные показатели с отклонениями
        time_impact = self._analyze_time_factors(day_data, monthly_averages, factors, critical_issues)
        impact_score += time_impact
        
        # 3. Реклама и ROAS
        ads_impact = self._analyze_advertising(day_data, factors, critical_issues)
        impact_score += ads_impact
        
        # 4. Погода
        if weather_data:
            if weather_data['precipitation'] > 10:
                factors.append(f"🌧️ Сильный дождь ({weather_data['precipitation']:.1f}мм)")
                impact_score += 25
            elif weather_data['precipitation'] > 5:
                factors.append(f"🌦️ Умеренный дождь ({weather_data['precipitation']:.1f}мм)")
                impact_score += 15
            elif weather_data['precipitation'] > 0:
                factors.append(f"🌤️ Легкий дождь ({weather_data['precipitation']:.1f}мм)")
                impact_score += 5
        
        # 5. Праздники
        if holiday_info:
            factors.append(f"🎉 {holiday_info.get('name', 'Праздник')}")
            impact_score += 25
        
        # 6. Рейтинги
        gojek_rating = day_data.get('gojek_rating', 0)
        grab_rating = day_data.get('grab_rating', 0)
        
        if gojek_rating > 0 and gojek_rating < 4.5:
            factors.append(f"⭐ Низкий рейтинг Gojek: {gojek_rating}")
            impact_score += 20
        elif gojek_rating > 0 and gojek_rating < 4.7:
            factors.append(f"⭐ Средний рейтинг Gojek: {gojek_rating}")
            impact_score += 10
            
        if grab_rating > 0 and grab_rating < 4.5:
            factors.append(f"⭐ Низкий рейтинг Grab: {grab_rating}")
            impact_score += 20
        elif grab_rating > 0 and grab_rating < 4.7:
            factors.append(f"⭐ Средний рейтинг Grab: {grab_rating}")
            impact_score += 10
        
        # 7. День недели
        weekday = pd.to_datetime(target_date).strftime('%A')
        if weekday in ['Sunday', 'Monday']:
            factors.append(f"📅 Слабый день недели ({weekday})")
            impact_score += 5
        
        # Выводим факторы
        if factors:
            results.append("")
            results.append("🔍 ФАКТОРЫ ВЛИЯНИЯ:")
            for i, factor in enumerate(factors[:5], 1):  # Топ-5 факторов
                results.append(f"   {i}. {factor}")
        
        # Критические проблемы
        if critical_issues:
            results.append("")
            results.append("🚨 КРИТИЧЕСКИЕ ПРОБЛЕМЫ:")
            for issue in critical_issues:
                results.append(f"   • {issue}")
        
        # Оценка влияния
        results.append("")
        if impact_score >= 70:
            results.append("📊 ОЦЕНКА: 🔴 КРИТИЧЕСКОЕ негативное влияние")
        elif impact_score >= 40:
            results.append("📊 ОЦЕНКА: 🟡 ВЫСОКОЕ негативное влияние") 
        elif impact_score >= 20:
            results.append("📊 ОЦЕНКА: 🟠 СРЕДНЕЕ негативное влияние")
        else:
            results.append("📊 ОЦЕНКА: 🟢 НИЗКОЕ негативное влияние")
        
        return results
    
    def _apply_fake_orders_filter(self, restaurant_name, date, day_data):
        """Применяет фильтр fake orders к данным"""
        if not self.fake_orders_filter or not day_data:
            return day_data, None
        
        # Получаем оригинальные данные
        grab_sales = day_data.get('grab_sales', 0)
        grab_orders = day_data.get('grab_orders', 0)
        gojek_sales = day_data.get('gojek_sales', 0)
        gojek_orders = day_data.get('gojek_orders', 0)
        
        # Применяем корректировку
        adjustment = self.fake_orders_filter.adjust_sales_data(
            restaurant_name, date, grab_sales, grab_orders, gojek_sales, gojek_orders
        )
        
        # Обновляем данные
        day_data['grab_sales'] = adjustment['grab_sales_adjusted']
        day_data['grab_orders'] = adjustment['grab_orders_adjusted']
        day_data['gojek_sales'] = adjustment['gojek_sales_adjusted']
        day_data['gojek_orders'] = adjustment['gojek_orders_adjusted']
        day_data['total_sales'] = day_data['grab_sales'] + day_data['gojek_sales']
        day_data['total_orders'] = day_data['grab_orders'] + day_data['gojek_orders']
        
        # Сохраняем информацию о корректировке
        fake_info = adjustment['fake_orders_removed']
        if (fake_info['grab_fake_orders'] > 0 or fake_info['gojek_fake_orders'] > 0):
            return day_data, fake_info
        
        return day_data, None

    def _get_day_data(self, restaurant_name, target_date):
        """Получает данные за конкретный день"""
        # Сначала найдем ID ресторана
        with sqlite3.connect('database.sqlite') as conn:
            restaurant_query = f"SELECT id FROM restaurants WHERE name = '{restaurant_name}'"
            restaurant_df = pd.read_sql_query(restaurant_query, conn)
            
            if restaurant_df.empty:
                return None
                
            restaurant_id = restaurant_df.iloc[0]['id']
            
            # Получаем данные Grab
            grab_query = f"""
            SELECT 
                stat_date,
                COALESCE(sales, 0) as grab_sales,
                COALESCE(orders, 0) as grab_orders,
                COALESCE(offline_rate, 0) as grab_offline_rate,
                COALESCE(driver_waiting_time, 0) / 60.0 as grab_driver_waiting_min,
                COALESCE(ads_spend, 0) as grab_ads_spend,
                COALESCE(ads_sales, 0) as grab_ads_sales,
                COALESCE(rating, 0) as grab_rating
            FROM grab_stats
            WHERE restaurant_id = {restaurant_id} AND stat_date = '{target_date}'
            """
            grab_df = pd.read_sql_query(grab_query, conn)
            
            # Получаем данные Gojek
            gojek_query = f"""
            SELECT 
                stat_date,
                COALESCE(sales, 0) as gojek_sales,
                COALESCE(orders, 0) as gojek_orders,
                COALESCE(close_time, '00:00:00') as gojek_close_time,
                COALESCE(preparation_time, '00:00:00') as gojek_preparation_time,
                COALESCE(delivery_time, '00:00:00') as gojek_delivery_time,
                COALESCE(driver_waiting, 0) as gojek_driver_waiting_min,
                COALESCE(ads_spend, 0) as gojek_ads_spend,
                COALESCE(ads_sales, 0) as gojek_ads_sales,
                COALESCE(rating, 0) as gojek_rating
            FROM gojek_stats
            WHERE restaurant_id = {restaurant_id} AND stat_date = '{target_date}'
            """
            gojek_df = pd.read_sql_query(gojek_query, conn)
            
            # Если нет данных ни в одной из таблиц
            if grab_df.empty and gojek_df.empty:
                return None
                
            # Объединяем данные
            result = {
                'stat_date': target_date,
                'grab_sales': grab_df.iloc[0]['grab_sales'] if not grab_df.empty else 0,
                'gojek_sales': gojek_df.iloc[0]['gojek_sales'] if not gojek_df.empty else 0,
                'grab_orders': grab_df.iloc[0]['grab_orders'] if not grab_df.empty else 0,
                'gojek_orders': gojek_df.iloc[0]['gojek_orders'] if not gojek_df.empty else 0,
                'grab_offline_rate': grab_df.iloc[0]['grab_offline_rate'] if not grab_df.empty else 0,
                'gojek_close_time': gojek_df.iloc[0]['gojek_close_time'] if not gojek_df.empty else '00:00:00',
                'gojek_preparation_time': gojek_df.iloc[0]['gojek_preparation_time'] if not gojek_df.empty else '00:00:00',
                'gojek_delivery_time': gojek_df.iloc[0]['gojek_delivery_time'] if not gojek_df.empty else '00:00:00',
                'gojek_driver_waiting_min': gojek_df.iloc[0]['gojek_driver_waiting_min'] if not gojek_df.empty else 0,
                'grab_driver_waiting_min': grab_df.iloc[0]['grab_driver_waiting_min'] if not grab_df.empty else 0,
                'grab_ads_spend': grab_df.iloc[0]['grab_ads_spend'] if not grab_df.empty else 0,
                'grab_ads_sales': grab_df.iloc[0]['grab_ads_sales'] if not grab_df.empty else 0,
                'gojek_ads_spend': gojek_df.iloc[0]['gojek_ads_spend'] if not gojek_df.empty else 0,
                'gojek_ads_sales': gojek_df.iloc[0]['gojek_ads_sales'] if not gojek_df.empty else 0,
                'grab_rating': grab_df.iloc[0]['grab_rating'] if not grab_df.empty else 0,
                'gojek_rating': gojek_df.iloc[0]['gojek_rating'] if not gojek_df.empty else 0
            }
            
            result['total_sales'] = result['grab_sales'] + result['gojek_sales']
            result['total_orders'] = result['grab_orders'] + result['gojek_orders']
            
            # Применяем фильтр fake orders
            result, fake_info = self._apply_fake_orders_filter(restaurant_name, target_date, result)
            
            # Добавляем информацию о fake orders если есть
            if fake_info:
                result['fake_orders_detected'] = fake_info
            
            return result
    
    def _get_monthly_averages(self, restaurant_name, target_date):
        """Получает среднемесячные временные показатели"""
        target_month = target_date[:7]  # YYYY-MM
        
        with sqlite3.connect('database.sqlite') as conn:
            # Сначала найдем ID ресторана
            restaurant_query = f"SELECT id FROM restaurants WHERE name = '{restaurant_name}'"
            restaurant_df = pd.read_sql_query(restaurant_query, conn)
            
            if restaurant_df.empty:
                return {'avg_prep_time': 0, 'avg_delivery_time': 0, 'avg_gojek_waiting': 0, 'avg_grab_waiting': 0}
                
            restaurant_id = restaurant_df.iloc[0]['id']
            
            query = f"""
            WITH all_dates AS (
                SELECT stat_date FROM grab_stats 
                WHERE restaurant_id = {restaurant_id} AND stat_date LIKE '{target_month}%'
                UNION 
                SELECT stat_date FROM gojek_stats 
                WHERE restaurant_id = {restaurant_id} AND stat_date LIKE '{target_month}%'
            )
            SELECT 
                AVG(CASE WHEN gj.preparation_time IS NOT NULL AND gj.preparation_time != '00:00:00' 
                    THEN (CAST(substr(gj.preparation_time, 1, 2) AS INTEGER) * 60 + 
                          CAST(substr(gj.preparation_time, 4, 2) AS INTEGER) + 
                          CAST(substr(gj.preparation_time, 7, 2) AS INTEGER) / 60.0) 
                    ELSE NULL END) as avg_prep_time,
                AVG(CASE WHEN gj.delivery_time IS NOT NULL AND gj.delivery_time != '00:00:00' 
                    THEN (CAST(substr(gj.delivery_time, 1, 2) AS INTEGER) * 60 + 
                          CAST(substr(gj.delivery_time, 4, 2) AS INTEGER) + 
                          CAST(substr(gj.delivery_time, 7, 2) AS INTEGER) / 60.0) 
                    ELSE NULL END) as avg_delivery_time,
                AVG(CASE WHEN gj.driver_waiting > 0 THEN gj.driver_waiting ELSE NULL END) as avg_gojek_waiting,
                AVG(CASE WHEN g.driver_waiting_time > 0 THEN g.driver_waiting_time / 60.0 ELSE NULL END) as avg_grab_waiting
            FROM all_dates ad
            LEFT JOIN grab_stats g ON ad.stat_date = g.stat_date AND g.restaurant_id = {restaurant_id}
            LEFT JOIN gojek_stats gj ON ad.stat_date = gj.stat_date AND gj.restaurant_id = {restaurant_id}
            """
            
            df = pd.read_sql_query(query, conn)
            
        if len(df) > 0:
            return {
                'avg_prep_time': df['avg_prep_time'].iloc[0] or 0,
                'avg_delivery_time': df['avg_delivery_time'].iloc[0] or 0,
                'avg_gojek_waiting': df['avg_gojek_waiting'].iloc[0] or 0,
                'avg_grab_waiting': df['avg_grab_waiting'].iloc[0] or 0
            }
        return {'avg_prep_time': 0, 'avg_delivery_time': 0, 'avg_gojek_waiting': 0, 'avg_grab_waiting': 0}
    
    def _get_weather_data(self, restaurant_name, date):
        """Получает данные о погоде"""
        location = self.locations_data.get(restaurant_name)
        if not location:
            return None
            
        try:
            url = f"https://archive-api.open-meteo.com/v1/era5"
            params = {
                'latitude': location['latitude'],
                'longitude': location['longitude'],
                'start_date': date,
                'end_date': date,
                'daily': ['precipitation_sum', 'temperature_2m_mean'],
                'timezone': 'Asia/Jakarta'
            }
            
            response = requests.get(url, params=params, timeout=10)
            if response.status_code == 200:
                data = response.json()
                if 'daily' in data:
                    return {
                        'precipitation': data['daily']['precipitation_sum'][0] or 0,
                        'temperature': data['daily']['temperature_2m_mean'][0] or 27
                    }
        except:
            pass
        
        return None
    
    def _analyze_time_factors(self, day_data, monthly_averages, factors, critical_issues):
        """Анализирует временные факторы с отклонениями"""
        impact_score = 0
        
        # Preparation Time
        prep_time_str = day_data.get('gojek_preparation_time', '00:00:00')
        if prep_time_str and prep_time_str != '00:00:00':
            prep_minutes = self._parse_time_to_minutes(prep_time_str)
            avg_prep = monthly_averages['avg_prep_time']
            
            if avg_prep > 0:
                prep_deviation = ((prep_minutes - avg_prep) / avg_prep) * 100
                if prep_deviation >= 50:
                    factors.append(f"🚨 КРИТИЧНО: Gojek Preparation {prep_minutes:.1f}мин (+{prep_deviation:.0f}%)")
                    critical_issues.append("Критическое время готовки Gojek")
                    impact_score += 30
                elif prep_deviation >= 30:
                    factors.append(f"⚠️ Gojek Preparation {prep_minutes:.1f}мин (+{prep_deviation:.0f}% выше)")
                    impact_score += 15
        
        # Delivery Time
        delivery_time_str = day_data.get('gojek_delivery_time', '00:00:00')
        if delivery_time_str and delivery_time_str != '00:00:00':
            delivery_minutes = self._parse_time_to_minutes(delivery_time_str)
            avg_delivery = monthly_averages['avg_delivery_time']
            
            if avg_delivery > 0:
                delivery_deviation = ((delivery_minutes - avg_delivery) / avg_delivery) * 100
                if delivery_deviation >= 50:
                    factors.append(f"🚨 КРИТИЧНО: Gojek Delivery {delivery_minutes:.1f}мин (+{delivery_deviation:.0f}%)")
                    critical_issues.append("Критическое время доставки Gojek")
                    impact_score += 30
                elif delivery_deviation >= 30:
                    factors.append(f"⚠️ Gojek Delivery {delivery_minutes:.1f}мин (+{delivery_deviation:.0f}% выше)")
                    impact_score += 15
        
        # Driver Waiting Time
        gojek_waiting = day_data.get('gojek_driver_waiting_min', 0)
        if gojek_waiting > 0:
            avg_gojek_waiting = monthly_averages['avg_gojek_waiting']
            if avg_gojek_waiting > 0:
                waiting_deviation = ((gojek_waiting - avg_gojek_waiting) / avg_gojek_waiting) * 100
                if waiting_deviation >= 50:
                    factors.append(f"🚨 КРИТИЧНО: Gojek Driver Waiting {gojek_waiting}мин (+{waiting_deviation:.0f}%)")
                    critical_issues.append("Критическое время ожидания Gojek")
                    impact_score += 30
                elif waiting_deviation >= 30:
                    factors.append(f"⚠️ Gojek Driver Waiting {gojek_waiting}мин (+{waiting_deviation:.0f}% выше)")
                    impact_score += 15
                    
        return impact_score
    
    def _analyze_advertising(self, day_data, factors, critical_issues):
        """Анализирует рекламные показатели"""
        grab_ads_spend = day_data.get('grab_ads_spend', 0)
        grab_ads_sales = day_data.get('grab_ads_sales', 0)
        gojek_ads_spend = day_data.get('gojek_ads_spend', 0)
        gojek_ads_sales = day_data.get('gojek_ads_sales', 0)
        
        ads_working = False
        impact_score = 0
        
        if grab_ads_spend > 0:
            grab_roas = grab_ads_sales / grab_ads_spend
            ads_working = True
            if grab_roas >= 10:
                factors.append(f"✅ Grab ROAS отличный: {grab_roas:.1f}")
            elif grab_roas >= 3:
                factors.append(f"🟢 Grab ROAS хороший: {grab_roas:.1f}")
            elif grab_roas < 1:
                factors.append(f"🚨 Grab ROAS критичный: {grab_roas:.1f}")
                critical_issues.append("Критически низкий ROAS Grab")
                impact_score += 40
                
        if gojek_ads_spend > 0:
            gojek_roas = gojek_ads_sales / gojek_ads_spend
            ads_working = True
            if gojek_roas >= 10:
                factors.append(f"✅ Gojek ROAS отличный: {gojek_roas:.1f}")
            elif gojek_roas >= 3:
                factors.append(f"🟢 Gojek ROAS хороший: {gojek_roas:.1f}")
            elif gojek_roas < 1:
                factors.append(f"🚨 Gojek ROAS критичный: {gojek_roas:.1f}")
                critical_issues.append("Критически низкий ROAS Gojek")
                impact_score += 40
        
        if not ads_working:
            factors.append("❌ Реклама не работала")
            impact_score += 20
            
        return impact_score
    
    def _generate_general_recommendations(self, bad_days):
        """Генерирует общие рекомендации"""
        results = []
        results.append("💡 ОБЩИЕ РЕКОМЕНДАЦИИ:")
        results.append("")
        
        # Анализируем паттерны
        weekend_issues = sum(1 for bad_day_info in bad_days if pd.to_datetime(bad_day_info[0]).strftime('%A') in ['Sunday', 'Monday'])
        
        if weekend_issues > len(bad_days) * 0.5:
            results.append("📅 Проблема с выходными днями:")
            results.append("   • Усилить маркетинг по воскресеньям")
            results.append("   • Запустить специальные промо на понедельник")
        
        results.append("🔧 Операционные меры:")
        results.append("   • Настроить мониторинг доступности платформ")
        results.append("   • Контролировать время ожидания водителей")
        results.append("   • Оптимизировать время готовки и доставки")
        
        results.append("📊 Аналитические меры:")
        results.append("   • Ежедневный мониторинг ROAS")
        results.append("   • Анализ погодных условий для планирования")
        results.append("   • Отслеживание праздников и их влияния")
        
        return results
    
    def _parse_time_string(self, time_str):
        """Парсит строку времени в секунды"""
        if not time_str or time_str == '00:00:00':
            return 0
        try:
            parts = time_str.split(':')
            if len(parts) >= 3:
                return int(parts[0]) * 3600 + int(parts[1]) * 60 + int(parts[2])
        except:
            pass
        return 0
    
    def _parse_time_to_minutes(self, time_str):
        """Парсит строку времени в минуты"""
        if not time_str or time_str == '00:00:00':
            return 0
        try:
            parts = time_str.split(':')
            if len(parts) >= 3:
                return int(parts[0]) * 60 + int(parts[1]) + int(parts[2]) / 60.0
        except:
            pass
        return 0
    
    def _format_duration(self, seconds):
        """Форматирует секунды в читаемый вид"""
        if seconds < 3600:
            minutes = seconds // 60
            return f"{minutes}м"
        else:
            hours = seconds // 3600
            minutes = (seconds % 3600) // 60
            if minutes > 0:
                return f"{hours}ч {minutes}м"
            else:
                return f"{hours}ч"

    def get_period_statistics_with_corrections(self, restaurant_name, start_date, end_date):
        """
        Получает статистику за период с полными корректировками:
        Итоговые данные = Исходные - Отмененные - Потерянные - Фейковые
        """
        try:
            with sqlite3.connect('database.sqlite') as conn:
                # Получаем ID ресторана
                restaurant_query = f"SELECT id FROM restaurants WHERE name = '{restaurant_name}'"
                restaurant_df = pd.read_sql_query(restaurant_query, conn)
                if restaurant_df.empty:
                    return None
                
                restaurant_id = restaurant_df.iloc[0]['id']
                
                # Получаем исходные данные Grab
                grab_query = f"""
                SELECT 
                    SUM(COALESCE(sales, 0)) as original_sales,
                    SUM(COALESCE(orders, 0)) as original_orders,
                    SUM(COALESCE(cancelled_orders, 0)) as cancelled_orders,
                    SUM(COALESCE(ads_spend, 0)) as ads_spend,
                    SUM(COALESCE(ads_sales, 0)) as ads_sales,
                    SUM(COALESCE(payouts, 0)) as payouts,
                    SUM(COALESCE(new_customers, 0)) as new_customers,
                    SUM(COALESCE(repeated_customers, 0)) as repeated_customers,
                    SUM(COALESCE(reactivated_customers, 0)) as reactivated_customers
                FROM grab_stats
                WHERE restaurant_id = {restaurant_id}
                AND stat_date BETWEEN '{start_date}' AND '{end_date}'
                """
                grab_df = pd.read_sql_query(grab_query, conn)
                
                # Получаем исходные данные Gojek
                gojek_query = f"""
                SELECT 
                    SUM(COALESCE(sales, 0)) as original_sales,
                    SUM(COALESCE(orders, 0)) as original_orders,
                    SUM(COALESCE(cancelled_orders, 0)) as cancelled_orders,
                    SUM(COALESCE(potential_lost, 0)) as potential_lost,
                    SUM(COALESCE(ads_spend, 0)) as ads_spend,
                    SUM(COALESCE(ads_sales, 0)) as ads_sales,
                    SUM(COALESCE(new_client, 0)) as new_clients,
                    SUM(COALESCE(active_client, 0)) as active_clients,
                    SUM(COALESCE(returned_client, 0)) as returned_clients
                FROM gojek_stats
                WHERE restaurant_id = {restaurant_id}
                AND stat_date BETWEEN '{start_date}' AND '{end_date}'
                """
                gojek_df = pd.read_sql_query(gojek_query, conn)
                
                # Получаем фейковые заказы за период
                fake_stats = self._get_fake_orders_for_period(restaurant_name, start_date, end_date)
                
                # Формируем результат
                result = {
                    'restaurant_name': restaurant_name,
                    'period': f"{start_date} — {end_date}",
                    
                    # Grab данные
                    'grab_original_orders': int(grab_df.iloc[0]['original_orders']) if not grab_df.empty else 0,
                    'grab_original_sales': int(grab_df.iloc[0]['original_sales']) if not grab_df.empty else 0,
                    'grab_cancelled_orders': int(grab_df.iloc[0]['cancelled_orders']) if not grab_df.empty else 0,
                    'grab_fake_orders': fake_stats['grab_fake_orders'],
                    'grab_fake_amount': fake_stats['grab_fake_amount'],
                    
                    # Gojek данные
                    'gojek_original_orders': int(gojek_df.iloc[0]['original_orders']) if not gojek_df.empty else 0,
                    'gojek_original_sales': int(gojek_df.iloc[0]['original_sales']) if not gojek_df.empty else 0,
                    'gojek_cancelled_orders': int(gojek_df.iloc[0]['cancelled_orders']) if not gojek_df.empty else 0,
                    'gojek_potential_lost': int(gojek_df.iloc[0]['potential_lost']) if not gojek_df.empty else 0,
                    'gojek_fake_orders': fake_stats['gojek_fake_orders'],
                    'gojek_fake_amount': fake_stats['gojek_fake_amount'],
                    
                    # Реклама
                    'grab_ads_spend': int(grab_df.iloc[0]['ads_spend']) if not grab_df.empty else 0,
                    'grab_ads_sales': int(grab_df.iloc[0]['ads_sales']) if not grab_df.empty else 0,
                    'gojek_ads_spend': int(gojek_df.iloc[0]['ads_spend']) if not gojek_df.empty else 0,
                    'gojek_ads_sales': int(gojek_df.iloc[0]['ads_sales']) if not gojek_df.empty else 0,
                    
                    # Выплаты
                    'grab_payouts': int(grab_df.iloc[0]['payouts']) if not grab_df.empty else 0,
                    
                    # Клиенты
                    'grab_new_customers': int(grab_df.iloc[0]['new_customers']) if not grab_df.empty else 0,
                    'grab_repeated_customers': int(grab_df.iloc[0]['repeated_customers']) if not grab_df.empty else 0,
                    'grab_reactivated_customers': int(grab_df.iloc[0]['reactivated_customers']) if not grab_df.empty else 0,
                    'gojek_new_clients': int(gojek_df.iloc[0]['new_clients']) if not gojek_df.empty else 0,
                    'gojek_active_clients': int(gojek_df.iloc[0]['active_clients']) if not gojek_df.empty else 0,
                    'gojek_returned_clients': int(gojek_df.iloc[0]['returned_clients']) if not gojek_df.empty else 0,
                }
                
                # Рассчитываем финальные (очищенные) данные
                result['grab_final_orders'] = (result['grab_original_orders'] - 
                                             result['grab_cancelled_orders'] - 
                                             result['grab_fake_orders'])
                result['gojek_final_orders'] = (result['gojek_original_orders'] - 
                                              result['gojek_cancelled_orders'] - 
                                              result['gojek_fake_orders'])
                
                result['grab_final_sales'] = result['grab_original_sales'] - result['grab_fake_amount']
                result['gojek_final_sales'] = (result['gojek_original_sales'] - 
                                             result['gojek_fake_amount'] - 
                                             result['gojek_potential_lost'])
                
                # Итоговые данные
                result['total_final_orders'] = result['grab_final_orders'] + result['gojek_final_orders']
                result['total_final_sales'] = result['grab_final_sales'] + result['gojek_final_sales']
                result['total_ads_spend'] = result['grab_ads_spend'] + result['gojek_ads_spend']
                result['total_ads_sales'] = result['grab_ads_sales'] + result['gojek_ads_sales']
                
                # Средний чек
                result['grab_avg_check'] = (result['grab_final_sales'] / result['grab_final_orders'] 
                                          if result['grab_final_orders'] > 0 else 0)
                result['gojek_avg_check'] = (result['gojek_final_sales'] / result['gojek_final_orders'] 
                                           if result['gojek_final_orders'] > 0 else 0)
                
                # ROAS
                result['grab_roas'] = (result['grab_ads_sales'] / result['grab_ads_spend'] 
                                     if result['grab_ads_spend'] > 0 else 0)
                result['gojek_roas'] = (result['gojek_ads_sales'] / result['gojek_ads_spend'] 
                                      if result['gojek_ads_spend'] > 0 else 0)
                
                return result
                
        except Exception as e:
            print(f"❌ Ошибка получения статистики: {e}")
            return None
    
    def _get_fake_orders_for_period(self, restaurant_name, start_date, end_date):
        """Подсчитывает фейковые заказы за период"""
        if not self.fake_orders_filter:
            return {'grab_fake_orders': 0, 'gojek_fake_orders': 0, 
                   'grab_fake_amount': 0, 'gojek_fake_amount': 0}
        
        # Конвертируем даты в формат fake orders (DD/MM/YYYY)
        from datetime import datetime
        start_dt = datetime.strptime(start_date, '%Y-%m-%d')
        end_dt = datetime.strptime(end_date, '%Y-%m-%d')
        
        grab_fakes = 0
        gojek_fakes = 0
        grab_fake_amount = 0
        gojek_fake_amount = 0
        
        # Получаем данные из фильтра
        for order in self.fake_orders_filter.fake_orders_data:
            if order.get('restaurant') == restaurant_name:
                try:
                    order_date_str = order.get('date', '')
                    if '/' in order_date_str:
                        order_dt = datetime.strptime(order_date_str, '%d/%m/%Y')
                        
                        if start_dt <= order_dt <= end_dt:
                            quantity = int(order.get('quantity', 0))
                            amount = int(order.get('amount', 0))
                            
                            platform = order.get('platform', '').lower()
                            if platform == 'grab':
                                grab_fakes += quantity
                                grab_fake_amount += amount
                            elif platform == 'gojek':
                                gojek_fakes += quantity
                                gojek_fake_amount += amount
                except:
                    continue
        
        return {
            'grab_fake_orders': grab_fakes,
            'gojek_fake_orders': gojek_fakes,
            'grab_fake_amount': grab_fake_amount,
            'gojek_fake_amount': gojek_fake_amount
        }

    def generate_executive_summary(self, restaurant_name, start_date, end_date):
        """
        Генерирует правильное исполнительное резюме:
        - Общая выручка: исходные данные из базы
        - Детализация: с указанием fake orders
        - Метрики: за вычетом отмененных, потерянных и fake
        """
        stats = self.get_period_statistics_with_corrections(restaurant_name, start_date, end_date)
        if not stats:
            return ["❌ Нет данных для генерации исполнительного резюме"]
        
        results = []
        
        # Исходные данные (как в базе)
        total_raw_sales = stats['grab_original_sales'] + stats['gojek_original_sales']
        total_raw_orders = stats['grab_original_orders'] + stats['gojek_original_orders']
        
        # Успешные заказы (за вычетом отмененных, потерянных, fake)
        successful_orders = stats['total_final_orders']
        
        # Средний чек за вычетом всех корректировок
        avg_check = stats['total_final_sales'] / successful_orders if successful_orders > 0 else 0
        grab_avg_check = stats['grab_final_sales'] / stats['grab_final_orders'] if stats['grab_final_orders'] > 0 else 0
        gojek_avg_check = stats['gojek_final_sales'] / stats['gojek_final_orders'] if stats['gojek_final_orders'] > 0 else 0
        
        # Дневная выручка (по исходным данным)
        from datetime import datetime
        start_dt = datetime.strptime(start_date, '%Y-%m-%d')
        end_dt = datetime.strptime(end_date, '%Y-%m-%d')
        days_count = (end_dt - start_dt).days + 1
        daily_revenue = total_raw_sales / days_count
        
        # Общее количество клиентов
        total_grab_clients = stats['grab_new_customers'] + stats['grab_repeated_customers'] + stats['grab_reactivated_customers']
        total_gojek_clients = stats['gojek_new_clients'] + stats['gojek_active_clients'] + stats['gojek_returned_clients']
        total_clients = total_grab_clients + total_gojek_clients
        
        # Расчет потерянных заказов для Gojek (из суммы потерь)
        gojek_lost_orders = int(stats['gojek_potential_lost'] / 150000) if stats['gojek_potential_lost'] > 0 else 0  # Примерно 150k за заказ
        
        # Рейтинг (нужно рассчитать из базы)
        avg_rating = self._get_average_rating(restaurant_name, start_date, end_date)
        
        results.append("📊 1. ИСПОЛНИТЕЛЬНОЕ РЕЗЮМЕ")
        results.append("----------------------------------------")
        results.append(f"💰 Общая выручка: {total_raw_sales:,} IDR (GRAB: {stats['grab_original_sales']:,} + GOJEK: {stats['gojek_original_sales']:,})")
        results.append(f"📦 Общие заказы: {total_raw_orders:,}")
        results.append(f"   ├── 📱 GRAB: {stats['grab_original_orders']:,} (успешно: {stats['grab_final_orders']:,}, отменено: {stats['grab_cancelled_orders']}, fake: {stats['grab_fake_orders']})")
        results.append(f"   └── 🛵 GOJEK: {stats['gojek_original_orders']:,} (успешно: {stats['gojek_final_orders']:,}, отменено: {stats['gojek_cancelled_orders']}, потеряно: {gojek_lost_orders}, fake: {stats['gojek_fake_orders']})")
        results.append(f"   💡 Успешных заказов: {successful_orders:,}")
        results.append(f"💵 Средний чек: {avg_check:,.0f} IDR")
        results.append(f"   ├── 📱 GRAB: {grab_avg_check:,.0f} IDR ({stats['grab_final_sales']:,} ÷ {stats['grab_final_orders']:,})")
        results.append(f"   └── 🛵 GOJEK: {gojek_avg_check:,.0f} IDR ({stats['gojek_final_sales']:,} ÷ {stats['gojek_final_orders']:,})")
        results.append(f"📊 Дневная выручка: {daily_revenue:,.0f} IDR (средняя по рабочим дням)")
        results.append(f"⭐ Средний рейтинг: {avg_rating:.2f}/5.0")
        results.append(f"👥 Обслужено клиентов: {total_clients:,}")
        results.append(f"   ├── 📱 GRAB: {total_grab_clients:,} (новые: {stats['grab_new_customers']}, повторные: {stats['grab_repeated_customers']}, реактивированные: {stats['grab_reactivated_customers']})")
        results.append(f"   └── 🛵 GOJEK: {total_gojek_clients:,} (новые: {stats['gojek_new_clients']}, активные: {stats['gojek_active_clients']}, возвратившиеся: {stats['gojek_returned_clients']})")
        results.append(f"   💡 Общий охват: {total_clients:,} уникальных клиентов")
        results.append(f"💸 Маркетинговый бюджет: {stats['total_ads_spend']:,} IDR ({stats['total_ads_spend']/stats['total_final_sales']*100:.1f}% от выручки)")
        
        # Расчет всех трех метрик для профессионального анализа
        grab_percent_total = stats['grab_ads_spend']/stats['total_final_sales']*100
        gojek_percent_total = stats['gojek_ads_spend']/stats['total_final_sales']*100
        
        grab_percent_own = stats['grab_ads_spend']/stats['grab_final_sales']*100 if stats['grab_final_sales'] > 0 else 0
        gojek_percent_own = stats['gojek_ads_spend']/stats['gojek_final_sales']*100 if stats['gojek_final_sales'] > 0 else 0
        
        grab_percent_ads = stats['grab_ads_spend']/stats['grab_ads_sales']*100 if stats['grab_ads_sales'] > 0 else 0
        gojek_percent_ads = stats['gojek_ads_spend']/stats['gojek_ads_sales']*100 if stats['gojek_ads_sales'] > 0 else 0
        
        results.append("📊 Детализация маркетинговых затрат:")
        results.append("   ┌─ 📱 GRAB:")
        results.append(f"   │  💰 Бюджет: {stats['grab_ads_spend']:,} IDR ({stats['grab_ads_spend']/stats['total_ads_spend']*100:.1f}% общего бюджета)")
        results.append(f"   │  📈 {grab_percent_total:.1f}% от общей выручки | {grab_percent_own:.1f}% от выручки GRAB | {grab_percent_ads:.1f}% от рекламных продаж")
        results.append("   └─ 🛵 GOJEK:")
        results.append(f"      💰 Бюджет: {stats['gojek_ads_spend']:,} IDR ({stats['gojek_ads_spend']/stats['total_ads_spend']*100:.1f}% общего бюджета)")
        results.append(f"      📈 {gojek_percent_total:.1f}% от общей выручки | {gojek_percent_own:.1f}% от выручки GOJEK | {gojek_percent_ads:.1f}% от рекламных продаж")
        results.append("")
        results.append("💡 Интерпретация метрик:")
        results.append("   • % от общей выручки → финансовая нагрузка на бизнес")
        results.append("   • % от выручки канала → операционная эффективность платформы")
        results.append("   • % от рекламных продаж канала → стоимость генерации рекламного дохода платформы")
        results.append("")
        results.append("🎯 ROAS АНАЛИЗ:")
        results.append(f"├── 📱 GRAB: {stats['grab_roas']:.2f}x (продажи: {stats['grab_ads_sales']:,} IDR / бюджет: {stats['grab_ads_spend']:,} IDR)")
        results.append(f"├── 🛵 GOJEK: {stats['gojek_roas']:.2f}x (продажи: {stats['gojek_ads_sales']:,} IDR / бюджет: {stats['gojek_ads_spend']:,} IDR)")
        
        total_roas = stats['total_ads_sales'] / stats['total_ads_spend'] if stats['total_ads_spend'] > 0 else 0
        results.append(f"└── 🎯 ОБЩИЙ: {total_roas:.2f}x (продажи: {stats['total_ads_sales']:,} IDR / бюджет: {stats['total_ads_spend']:,} IDR)")
        
        # Добавляем анализ продаж и трендов
        results.append("")
        sales_trends = self._get_sales_trends_analysis(restaurant_name, start_date, end_date)
        results.extend(sales_trends)
        
        # Добавляем детальный анализ клиентской базы
        results.append("")
        customer_analysis = self._get_customer_base_analysis(restaurant_name, start_date, end_date)
        results.extend(customer_analysis)
        
        # Добавляем маркетинговую эффективность и воронку
        results.append("")
        marketing_analysis = self._get_marketing_effectiveness_analysis(restaurant_name, start_date, end_date)
        results.extend(marketing_analysis)
        
        # Добавляем финансовые показатели
        results.append("")
        financial_metrics = self._get_financial_metrics(restaurant_name, start_date, end_date)
        results.extend(financial_metrics)
        
        # Добавляем операционные метрики
        results.append("")
        operational_metrics = self._get_operational_metrics(restaurant_name, start_date, end_date)
        results.extend(operational_metrics)
        
        # Добавляем операционные сбои
        results.append("")
        operational_issues = self._get_operational_issues_analysis(restaurant_name, start_date, end_date)
        results.extend(operational_issues)
        
        # Добавляем анализ рейтингов
        results.append("")
        ratings_analysis = self._get_ratings_analysis(restaurant_name, start_date, end_date)
        results.extend(ratings_analysis)
        
        # ДЕТЕКТИВНЫЙ АНАЛИЗ - САМАЯ ВАЖНАЯ ЧАСТЬ!
        results.append("")
        results.append("🔍 5. ДЕТЕКТИВНЫЙ АНАЛИЗ ПРОБЛЕМ")
        results.append("----------------------------------------")
        detective_analysis = self.generate_detective_analysis(restaurant_name, start_date, end_date)
        results.extend(detective_analysis)
        
        return results
    
    def generate_detective_analysis(self, restaurant_name, start_date, end_date):
        """
        ПУБЛИЧНЫЙ метод для детективного анализа - САМАЯ ВАЖНАЯ ЧАСТЬ!
        Возвращает детальный анализ проблемных дней и их причин
        """
        return self.analyze_restaurant_performance(restaurant_name, start_date, end_date, use_ml=False)
    
    def _get_average_rating(self, restaurant_name, start_date, end_date):
        """Получает средний рейтинг за период"""
        try:
            with sqlite3.connect('database.sqlite') as conn:
                restaurant_query = f"SELECT id FROM restaurants WHERE name = '{restaurant_name}'"
                restaurant_df = pd.read_sql_query(restaurant_query, conn)
                if restaurant_df.empty:
                    return 4.5
                
                restaurant_id = restaurant_df.iloc[0]['id']
                
                # Средний рейтинг по всем платформам
                query = f"""
                SELECT 
                    AVG(CASE WHEN g.rating > 0 THEN g.rating ELSE NULL END) as grab_avg,
                    AVG(CASE WHEN gj.rating > 0 THEN gj.rating ELSE NULL END) as gojek_avg,
                    COUNT(CASE WHEN g.rating > 0 THEN 1 END) as grab_days,
                    COUNT(CASE WHEN gj.rating > 0 THEN 1 END) as gojek_days
                FROM 
                    (SELECT DISTINCT stat_date FROM grab_stats 
                     WHERE restaurant_id = {restaurant_id} 
                     AND stat_date BETWEEN '{start_date}' AND '{end_date}') dates
                LEFT JOIN grab_stats g ON g.restaurant_id = {restaurant_id} 
                    AND g.stat_date = dates.stat_date
                LEFT JOIN gojek_stats gj ON gj.restaurant_id = {restaurant_id} 
                    AND gj.stat_date = dates.stat_date
                """
                
                df = pd.read_sql_query(query, conn)
                if not df.empty:
                    grab_avg = df.iloc[0]['grab_avg'] or 0
                    gojek_avg = df.iloc[0]['gojek_avg'] or 0
                    grab_days = df.iloc[0]['grab_days'] or 0
                    gojek_days = df.iloc[0]['gojek_days'] or 0
                    
                    # Взвешенное среднее по количеству дней с рейтингом
                    if grab_days + gojek_days > 0:
                        total_rating = (grab_avg * grab_days + gojek_avg * gojek_days) / (grab_days + gojek_days)
                        return total_rating if total_rating > 0 else 4.5
                    
                return 4.5
        except Exception as e:
            print(f"⚠️ Ошибка расчета рейтинга: {e}")
            return 4.5

    def _get_operational_issues_analysis(self, restaurant_name, start_date, end_date):
        """Анализ операционных сбоев платформ"""
        try:
            conn = sqlite3.connect('database.sqlite')
            cursor = conn.cursor()
            
            # Получаем restaurant_id
            restaurant_query = f"SELECT id FROM restaurants WHERE name = '{restaurant_name}'"
            cursor.execute(restaurant_query)
            restaurant_result = cursor.fetchone()
            if not restaurant_result:
                return []
                
            restaurant_id = restaurant_result[0]
            
            # Получаем общую статистику для расчета потерь
            stats = self.get_period_statistics_with_corrections(restaurant_name, start_date, end_date)
            total_sales = stats['grab_final_sales'] + stats['gojek_final_sales']
            
            # Рассчитываем количество дней в периоде
            from datetime import datetime, timedelta
            start_dt = datetime.strptime(start_date, '%Y-%m-%d')
            end_dt = datetime.strptime(end_date, '%Y-%m-%d')
            days_count = (end_dt - start_dt).days + 1
            
            # Среднедневная и среднечасовая выручка
            daily_avg = total_sales / days_count
            hourly_avg = daily_avg / 24
            
            # Доли платформ
            grab_share = stats['grab_final_sales'] / total_sales if total_sales > 0 else 0
            gojek_share = stats['gojek_final_sales'] / total_sales if total_sales > 0 else 0
            
            # Часовая выручка по платформам
            grab_hourly = hourly_avg * grab_share
            gojek_hourly = hourly_avg * gojek_share
            
            # Анализ GRAB сбоев
            cursor.execute('''
            SELECT stat_date, offline_rate 
            FROM grab_stats 
            WHERE restaurant_id = ? AND stat_date BETWEEN ? AND ?
            AND offline_rate > 60
            ORDER BY offline_rate DESC
            ''', (restaurant_id, start_date, end_date))
            grab_issues = cursor.fetchall()
            
            # Анализ GOJEK сбоев (close_time в формате HH:MM:SS, ищем >1 часа)
            cursor.execute('''
            SELECT stat_date, close_time 
            FROM gojek_stats 
            WHERE restaurant_id = ? AND stat_date BETWEEN ? AND ?
            AND close_time IS NOT NULL AND close_time != ''
            AND (CAST(substr(close_time, 1, instr(close_time, ':')-1) AS INTEGER) > 0 
                 OR (CAST(substr(close_time, 1, instr(close_time, ':')-1) AS INTEGER) = 1 
                     AND CAST(substr(close_time, instr(close_time, ':')+1, 2) AS INTEGER) > 0))
            ORDER BY close_time DESC
            ''', (restaurant_id, start_date, end_date))
            gojek_issues = cursor.fetchall()
            
            # Рассчитываем общее время сбоев и потери
            grab_total_hours = 0
            gojek_total_hours = 0
            
            for date, rate in grab_issues:
                total_minutes = rate
                hours = total_minutes / 60
                grab_total_hours += hours
                
            for date, close_time_str in gojek_issues:
                # Парсим HH:MM:SS
                time_parts = close_time_str.split(':')
                hours = int(time_parts[0]) + int(time_parts[1])/60 + int(time_parts[2])/3600
                gojek_total_hours += hours
            
            grab_losses = grab_total_hours * grab_hourly
            gojek_losses = gojek_total_hours * gojek_hourly
            total_losses = grab_losses + gojek_losses
            
            results = []
            results.append("🔧 ОПЕРАЦИОННЫЕ СБОИ ПЛАТФОРМ:")
            results.append(f"├── 📱 GRAB: {len(grab_issues)} критичных дня ({grab_total_hours:.2f}ч общее время)")
            results.append(f"├── 🛵 GOJEK: {len(gojek_issues)} критичных дня ({gojek_total_hours:.2f}ч общее время)")
            results.append(f"└── 💸 Потенциальные потери: {total_losses:,.0f} IDR ({total_losses/total_sales*100:.2f}% от выручки)")
            results.append(f"   ├── 📱 GRAB потери: {grab_losses:,.0f} IDR ({grab_total_hours:.2f}ч × {grab_hourly:,.0f} IDR/ч)")
            results.append(f"   └── 🛵 GOJEK потери: {gojek_losses:,.0f} IDR ({gojek_total_hours:.2f}ч × {gojek_hourly:,.0f} IDR/ч)")
            results.append("")
            
            if grab_issues or gojek_issues:
                results.append("🚨 КРИТИЧНЫЕ СБОИ (>1 часа):")
                
                issue_num = 1
                for date, rate in grab_issues[:5]:
                    # Правильная формула: offline_rate% / 60 = часы
                    total_minutes = rate
                    hours = int(total_minutes // 60)
                    minutes = int(total_minutes % 60)
                    loss = (total_minutes / 60) * grab_hourly
                    results.append(f"   {issue_num}. {date}: GRAB offline {hours}:{minutes:02d}:00 (потери: ~{loss:,.0f} IDR)")
                    issue_num += 1
                    
                for date, close_time_str in gojek_issues[:5]:
                    # close_time уже в формате HH:MM:SS
                    time_parts = close_time_str.split(':')
                    hours_decimal = int(time_parts[0]) + int(time_parts[1])/60 + int(time_parts[2])/3600
                    loss = hours_decimal * gojek_hourly
                    results.append(f"   {issue_num}. {date}: GOJEK offline {close_time_str} (потери: ~{loss:,.0f} IDR)")
                    issue_num += 1
            
            conn.close()
            return results
            
        except Exception as e:
            return [f"❌ Ошибка анализа сбоев: {e}"]
    
    def _get_ratings_analysis(self, restaurant_name, start_date, end_date):
        """Анализ качества обслуживания и рейтингов"""
        try:
            conn = sqlite3.connect('database.sqlite')
            cursor = conn.cursor()
            
            # Получаем restaurant_id
            restaurant_query = f"SELECT id FROM restaurants WHERE name = '{restaurant_name}'"
            cursor.execute(restaurant_query)
            restaurant_result = cursor.fetchone()
            if not restaurant_result:
                return []
                
            restaurant_id = restaurant_result[0]
            
            # Получаем данные о рейтингах GOJEK
            cursor.execute('''
            SELECT 
                SUM(one_star_ratings) as stars_1,
                SUM(two_star_ratings) as stars_2,
                SUM(three_star_ratings) as stars_3,
                SUM(four_star_ratings) as stars_4,
                SUM(five_star_ratings) as stars_5,
                SUM(orders) as total_orders_raw,
                SUM(cancelled_orders) as cancelled_orders,
                SUM(potential_lost) as potential_lost
            FROM gojek_stats 
            WHERE restaurant_id = ? AND stat_date BETWEEN ? AND ?
            ''', (restaurant_id, start_date, end_date))
            
            ratings_data = cursor.fetchone()
            conn.close()
            
            if not ratings_data or not any(ratings_data[:5]):
                return ["⭐ Данные о рейтингах недоступны"]
            
            stars_1, stars_2, stars_3, stars_4, stars_5, total_orders_raw, cancelled_orders, potential_lost = ratings_data
            total_ratings = sum(ratings_data[:5])
            
            if total_ratings == 0:
                return ["⭐ Рейтинги отсутствуют за период"]
            
            # Получаем fake orders для правильного расчета
            if hasattr(self, 'fake_orders_filter') and self.fake_orders_filter:
                fake_stats = self._get_fake_orders_for_period(restaurant_name, start_date, end_date)
                gojek_fake_orders = fake_stats.get('gojek_fake_orders', 0)
            else:
                gojek_fake_orders = 0
            
            # Рассчитываем успешные заказы (за вычетом отмененных, потерянных и fake)
            lost_orders = int(potential_lost / 150000) if potential_lost else 0  # примерно 150k за заказ
            successful_orders = total_orders_raw - cancelled_orders - lost_orders - gojek_fake_orders
            
            # Расчет средней оценки
            avg_rating = (1*stars_1 + 2*stars_2 + 3*stars_3 + 4*stars_4 + 5*stars_5) / total_ratings
            
            results = []
            results.append("⭐ КАЧЕСТВО ОБСЛУЖИВАНИЯ И УДОВЛЕТВОРЕННОСТЬ (GOJEK)")
            results.append("────────────────────────────────────────────────────────────────────────────")
            results.append(f"📊 Распределение оценок (всего: {total_ratings}):")
            results.append(f"  ⭐⭐⭐⭐⭐ 5 звезд: {stars_5} ({stars_5/total_ratings*100:.1f}%)")
            results.append(f"  ⭐⭐⭐⭐ 4 звезды: {stars_4} ({stars_4/total_ratings*100:.1f}%)")
            results.append(f"  ⭐⭐⭐ 3 звезды: {stars_3} ({stars_3/total_ratings*100:.1f}%)")
            results.append(f"  ⭐⭐ 2 звезды: {stars_2} ({stars_2/total_ratings*100:.1f}%)")
            results.append(f"  ⭐ 1 звезда: {stars_1} ({stars_1/total_ratings*100:.1f}%)")
            results.append("")
            results.append(f"📈 Индекс удовлетворенности: {avg_rating:.2f}/5.0")
            results.append(f"🚨 Негативные отзывы (1-2★): {stars_1 + stars_2} ({(stars_1 + stars_2)/total_ratings*100:.1f}%)")
            results.append("")
            
            bad_ratings = total_ratings - stars_5
            if bad_ratings > 0 and successful_orders > 0:
                orders_per_bad_rating = successful_orders / bad_ratings
                results.append("📊 Частота плохих оценок (не 5★):")
                results.append(f"  📈 Плохих оценок всего: {bad_ratings} из {total_ratings} ({bad_ratings/total_ratings*100:.1f}%)")
                results.append(f"  📦 Успешных заказов GOJEK на 1 плохую оценку: {orders_per_bad_rating:.1f}")
                results.append(f"  💡 Это означает: каждый {int(orders_per_bad_rating)}-й успешный заказ GOJEK получает оценку не 5★")
                results.append(f"  🔧 Расчет: {successful_orders} успешных заказов (за вычетом {cancelled_orders} отмененных + {lost_orders} потерянных + {gojek_fake_orders} fake)")
            
            return results
            
        except Exception as e:
            return [f"❌ Ошибка анализа рейтингов: {e}"]

    def _get_financial_metrics(self, restaurant_name, start_date, end_date):
        """Получение финансовых показателей"""
        try:
            conn = sqlite3.connect('database.sqlite')
            cursor = conn.cursor()
            
            # Получаем restaurant_id
            restaurant_query = f"SELECT id FROM restaurants WHERE name = '{restaurant_name}'"
            cursor.execute(restaurant_query)
            restaurant_result = cursor.fetchone()
            if not restaurant_result:
                return []
                
            restaurant_id = restaurant_result[0]
            
            results = []
            results.append("💳 ФИНАНСОВЫЕ ПОКАЗАТЕЛИ")
            results.append("──────────────────────────────────────────────────────────────────────────────")
            
            # Получаем выплаты
            cursor.execute('''
            SELECT SUM(payouts) FROM grab_stats 
            WHERE restaurant_id = ? AND stat_date BETWEEN ? AND ?
            ''', (restaurant_id, start_date, end_date))
            grab_payouts = cursor.fetchone()[0] or 0
            
            cursor.execute('''
            SELECT SUM(payouts) FROM gojek_stats 
            WHERE restaurant_id = ? AND stat_date BETWEEN ? AND ?
            ''', (restaurant_id, start_date, end_date))
            gojek_payouts = cursor.fetchone()[0] or 0
            
            # Получаем статистику для ROAS
            stats = self.get_period_statistics_with_corrections(restaurant_name, start_date, end_date)
            
            results.append("💰 Выплаты:")
            results.append(f"├── 📱 GRAB: {grab_payouts:,} IDR")
            if gojek_payouts > 0:
                results.append(f"└── 🛵 GOJEK: {gojek_payouts:,} IDR")
            else:
                results.append("└── 🛵 GOJEK: данные недоступны")
            
            results.append("")
            results.append("📊 Рекламная эффективность:")
            results.append(f"├── 💰 Общие рекламные продажи: {stats['total_ads_sales']:,} IDR")
            total_sales = stats['grab_final_sales'] + stats['gojek_final_sales']
            results.append(f"├── 📈 Доля от общих продаж: {stats['total_ads_sales']/total_sales*100:.1f}%")
            results.append(f"├── 🎯 GRAB ROAS: {stats['grab_roas']:.2f}x ({'отличная' if stats['grab_roas'] > 10 else 'хорошая' if stats['grab_roas'] > 5 else 'низкая'} эффективность)")
            results.append(f"└── 🎯 GOJEK ROAS: {stats['gojek_roas']:.2f}x ({'превосходная' if stats['gojek_roas'] > 20 else 'отличная' if stats['gojek_roas'] > 10 else 'хорошая'} эффективность)")
            
            results.append("")
            results.append("💵 Реальные поступления ресторану:")
            
            # Расчет для GRAB
            grab_platform_commission = stats['grab_final_sales'] - grab_payouts - stats['grab_ads_spend']
            grab_total_deduction = stats['grab_final_sales'] - grab_payouts
            grab_commission_rate = grab_total_deduction / stats['grab_final_sales'] * 100
            grab_platform_rate = grab_platform_commission / stats['grab_final_sales'] * 100
            grab_ads_rate = stats['grab_ads_spend'] / stats['grab_final_sales'] * 100
            
            # Расчет для GOJEK  
            gojek_platform_commission = stats['gojek_final_sales'] - gojek_payouts - stats['gojek_ads_spend']
            gojek_total_deduction = stats['gojek_final_sales'] - gojek_payouts
            gojek_commission_rate = gojek_total_deduction / stats['gojek_final_sales'] * 100
            gojek_platform_rate = gojek_platform_commission / stats['gojek_final_sales'] * 100
            gojek_ads_rate = stats['gojek_ads_spend'] / stats['gojek_final_sales'] * 100
            
            total_receipts = grab_payouts + gojek_payouts
            results.append(f"💰 Общие поступления: {total_receipts:,} IDR")
            results.append(f"├── 📱 GRAB: {grab_payouts:,} IDR (выручка: {stats['grab_final_sales']:,} - удержания: {grab_commission_rate:.1f}%)")
            results.append(f"│   ├── 🏛️ Комиссия платформы: {grab_platform_commission:,} IDR ({grab_platform_rate:.1f}%)")
            results.append(f"│   └── 📈 Рекламный бюджет: {stats['grab_ads_spend']:,} IDR ({grab_ads_rate:.1f}%)")
            results.append(f"└── 🛵 GOJEK: {gojek_payouts:,} IDR (выручка: {stats['gojek_final_sales']:,} - удержания: {gojek_commission_rate:.1f}%)")
            results.append(f"    ├── 🏛️ Комиссия платформы: {gojek_platform_commission:,} IDR ({gojek_platform_rate:.1f}%)")
            results.append(f"    └── 📈 Рекламный бюджет: {stats['gojek_ads_spend']:,} IDR ({gojek_ads_rate:.1f}%)")
            
            conn.close()
            return results
            
        except Exception as e:
            return [f"❌ Ошибка получения финансовых показателей: {e}"]
    
    def _get_operational_metrics(self, restaurant_name, start_date, end_date):
        """Получение операционных метрик"""
        try:
            conn = sqlite3.connect('database.sqlite')
            cursor = conn.cursor()
            
            # Получаем restaurant_id
            restaurant_query = f"SELECT id FROM restaurants WHERE name = '{restaurant_name}'"
            cursor.execute(restaurant_query)
            restaurant_result = cursor.fetchone()
            if not restaurant_result:
                return []
                
            restaurant_id = restaurant_result[0]
            
            results = []
            results.append("⏰ ОПЕРАЦИОННЫЕ МЕТРИКИ")
            results.append("──────────────────────────────────────────────────────────────────────────────")
            
            # GRAB метрики (driver_waiting_time это JSON)
            cursor.execute('''
            SELECT driver_waiting_time FROM grab_stats 
            WHERE restaurant_id = ? AND stat_date BETWEEN ? AND ?
            AND driver_waiting_time IS NOT NULL AND driver_waiting_time != ''
            ''', (restaurant_id, start_date, end_date))
            grab_waiting_results = cursor.fetchall()
            
            # Парсим JSON и получаем среднее время ожидания GRAB
            grab_waiting_times = []
            for row in grab_waiting_results:
                try:
                    import json
                    if row[0]:
                        data = json.loads(row[0])
                        if isinstance(data, dict) and 'min' in data:
                            grab_waiting_times.append(float(data['min']))
                        elif isinstance(data, (int, float)):
                            grab_waiting_times.append(float(data))
                except:
                    continue
            
            grab_waiting = sum(grab_waiting_times) / len(grab_waiting_times) if grab_waiting_times else 0
            
            # GOJEK метрики (время в формате TIME: HH:MM:SS, driver_waiting в минутах)
            cursor.execute('''
            SELECT preparation_time, delivery_time, driver_waiting 
            FROM gojek_stats 
            WHERE restaurant_id = ? AND stat_date BETWEEN ? AND ?
            AND preparation_time IS NOT NULL
            ''', (restaurant_id, start_date, end_date))
            gojek_results = cursor.fetchall()
            
            gojek_prep_times = []
            gojek_delivery_times = []
            gojek_waiting_times = []
            
            for row in gojek_results:
                try:
                    # Парсим TIME поля (HH:MM:SS -> минуты)
                    if row[0]:  # preparation_time
                        prep_parts = str(row[0]).split(':')
                        prep_minutes = int(prep_parts[0]) * 60 + int(prep_parts[1]) + int(prep_parts[2]) / 60
                        gojek_prep_times.append(prep_minutes)
                    
                    if row[1]:  # delivery_time  
                        del_parts = str(row[1]).split(':')
                        del_minutes = int(del_parts[0]) * 60 + int(del_parts[1]) + int(del_parts[2]) / 60
                        gojek_delivery_times.append(del_minutes)
                    
                    if row[2] is not None:  # driver_waiting (уже в минутах)
                        gojek_waiting_times.append(float(row[2]))
                except:
                    continue
            
            gojek_prep = sum(gojek_prep_times) / len(gojek_prep_times) if gojek_prep_times else 0
            gojek_delivery = sum(gojek_delivery_times) / len(gojek_delivery_times) if gojek_delivery_times else 0
            gojek_waiting = sum(gojek_waiting_times) / len(gojek_waiting_times) if gojek_waiting_times else 0
            
            results.append("🟢 GRAB:")
            results.append(f"└── ⏰ Время ожидания водителей: {grab_waiting:.1f} мин")
            results.append("")
            results.append("🟠 GOJEK:")
            results.append(f"├── ⏱️ Время приготовления: {gojek_prep:.1f} мин")
            results.append(f"├── 🚗 Время доставки: {gojek_delivery:.1f} мин")
            results.append(f"└── ⏰ Время ожидания водителей: {gojek_waiting:.1f} мин")
            results.append("")
            
            # Добавляем операционную эффективность (отмененные заказы и потери)
            stats = self.get_period_statistics_with_corrections(restaurant_name, start_date, end_date)
            
            results.append("⚠️ ОПЕРАЦИОННАЯ ЭФФЕКТИВНОСТЬ:")
            results.append("🚫 Отмененные заказы:")
            results.append(f"├── 📱 GRAB: {stats['grab_cancelled_orders']} заказа (отмена по ресторану)")
            results.append(f"└── 🛵 GOJEK: {stats['gojek_cancelled_orders']} заказа (дефицит товара)")
            total_cancelled = stats['grab_cancelled_orders'] + stats['gojek_cancelled_orders']
            total_orders = stats['grab_original_orders'] + stats['gojek_original_orders']
            results.append(f"💡 Всего отмененных: {total_cancelled} заказов ({total_cancelled/total_orders*100:.1f}% от общих)")
            results.append("")
            
            results.append("💔 Реальные потери от операционных проблем:")
            # Расчет потерь GRAB по среднему чеку
            grab_avg_check = stats['grab_final_sales'] / stats['grab_final_orders'] if stats['grab_final_orders'] > 0 else 0
            grab_cancelled_losses = stats['grab_cancelled_orders'] * grab_avg_check
            
            results.append(f"├── 💸 GRAB отмененные: {grab_cancelled_losses:,.0f} IDR ({stats['grab_cancelled_orders']} × {grab_avg_check:,.0f} средний чек)")
            results.append(f"├── 💸 GOJEK потерянные: {stats['gojek_potential_lost']:,} IDR (конкретные заказы)")
            
            total_losses = grab_cancelled_losses + stats['gojek_potential_lost']
            total_sales = stats['grab_final_sales'] + stats['gojek_final_sales']
            results.append(f"└── 📊 Общие потери: {total_losses:,.0f} IDR ({total_losses/total_sales*100:.2f}% от выручки)")
            
            conn.close()
            return results
            
        except Exception as e:
            return [f"❌ Ошибка получения операционных метрик: {e}"]

    def _get_sales_trends_analysis(self, restaurant_name, start_date, end_date):
        """Анализ продаж и трендов"""
        try:
            conn = sqlite3.connect('database.sqlite')
            cursor = conn.cursor()
            
            # Получаем restaurant_id
            restaurant_query = f"SELECT id FROM restaurants WHERE name = '{restaurant_name}'"
            cursor.execute(restaurant_query)
            restaurant_result = cursor.fetchone()
            if not restaurant_result:
                return []
                
            restaurant_id = restaurant_result[0]
            
            results = []
            results.append("📈 2. АНАЛИЗ ПРОДАЖ И ТРЕНДОВ")
            results.append("----------------------------------------")
            
            # Получаем продажи по дням (объединяем GRAB и GOJEK по датам)
            cursor.execute('''
            SELECT 
                stat_date,
                SUM(grab_sales + gojek_sales) as daily_total
            FROM (
                SELECT 
                    stat_date,
                    SUM(sales) as grab_sales,
                    0 as gojek_sales
                FROM grab_stats 
                WHERE restaurant_id = ? AND stat_date BETWEEN ? AND ?
                GROUP BY stat_date
                UNION ALL
                SELECT 
                    stat_date,
                    0 as grab_sales,
                    SUM(sales) as gojek_sales
                FROM gojek_stats 
                WHERE restaurant_id = ? AND stat_date BETWEEN ? AND ?
                GROUP BY stat_date
            ) combined
            GROUP BY stat_date
            ORDER BY stat_date
            ''', (restaurant_id, start_date, end_date, restaurant_id, start_date, end_date))
            
            daily_data = cursor.fetchall()
            
            # Группируем по месяцам
            from datetime import datetime
            monthly_sales = {}
            monthly_days = {}
            weekend_sales = []
            weekday_sales = []
            all_sales = []
            
            for date_str, sales in daily_data:
                all_sales.append(sales)
                
                # Месячная группировка
                date_obj = datetime.strptime(date_str, '%Y-%m-%d')
                month_key = date_obj.strftime('%Y-%m')
                if month_key not in monthly_sales:
                    monthly_sales[month_key] = 0
                    monthly_days[month_key] = 0
                monthly_sales[month_key] += sales
                monthly_days[month_key] += 1
                
                # Выходные vs будни
                weekday = date_obj.weekday()
                if weekday >= 5:  # Суббота и воскресенье
                    weekend_sales.append(sales)
                else:
                    weekday_sales.append(sales)
            
            # Динамика по месяцам
            results.append("📊 Динамика по месяцам:")
            for month_key in sorted(monthly_sales.keys()):
                month_name = 'Апрель' if month_key == '2025-04' else 'Май'
                avg_daily = monthly_sales[month_key] / monthly_days[month_key]
                results.append(f"  {month_name}: {monthly_sales[month_key]:,} IDR ({monthly_days[month_key]} дней, {avg_daily:,.0f} IDR/день)")
            
            results.append("")
            
            # Выходные vs будни
            avg_weekend = sum(weekend_sales) / len(weekend_sales) if weekend_sales else 0
            avg_weekday = sum(weekday_sales) / len(weekday_sales) if weekday_sales else 0
            weekend_effect = (avg_weekend - avg_weekday) / avg_weekday * 100 if avg_weekday > 0 else 0
            
            results.append("🗓️ Выходные vs Будни:")
            results.append(f"  📅 Средние продажи в выходные: {avg_weekend:,.0f} IDR")
            results.append(f"  📅 Средние продажи в будни: {avg_weekday:,.0f} IDR")
            results.append(f"  📊 Эффект выходных: {weekend_effect:+.1f}%")
            
            # Анализ рабочих дней
            days_count = len(all_sales)
            if not all_sales:
                return [f"❌ Нет данных о продажах за период {start_date} - {end_date}"]
            
            max_sales = max(all_sales)
            min_sales = min(all_sales)
            avg_sales = sum(all_sales) / days_count
            range_percent = (max_sales - min_sales) / min_sales * 100 if min_sales > 0 else 0
            
            # Коэффициент вариации
            import statistics
            cv = statistics.stdev(all_sales) / avg_sales * 100 if avg_sales > 0 else 0
            
            # Находим даты лучшего и худшего дня
            best_day = max(daily_data, key=lambda x: x[1])
            worst_day = min(daily_data, key=lambda x: x[1])
            
            results.append(f"📊 АНАЛИЗ РАБОЧИХ ДНЕЙ ({days_count} дней):")
            results.append(f"🏆 Лучший день: {best_day[0]} - {best_day[1]:,} IDR")
            results.append(f"📉 Худший день: {worst_day[0]} - {worst_day[1]:,} IDR")
            results.append(f"📊 Разброс продаж: {range_percent:.1f}% (только рабочие дни)")
            results.append(f"📈 Средние продажи: {avg_sales:,.0f} IDR/день")
            results.append(f"📊 Коэффициент вариации: {cv:.1f}% (стабильность продаж)")
            
            conn.close()
            return results
            
        except Exception as e:
            return [f"❌ Ошибка анализа продаж и трендов: {e}"]

    def _get_customer_base_analysis(self, restaurant_name, start_date, end_date):
        """Детальный анализ клиентской базы"""
        try:
            conn = sqlite3.connect('database.sqlite')
            cursor = conn.cursor()
            
            # Получаем restaurant_id
            restaurant_query = f"SELECT id FROM restaurants WHERE name = '{restaurant_name}'"
            cursor.execute(restaurant_query)
            restaurant_result = cursor.fetchone()
            if not restaurant_result:
                return []
                
            restaurant_id = restaurant_result[0]
            
            results = []
            results.append("👥 3. ДЕТАЛЬНЫЙ АНАЛИЗ КЛИЕНТСКОЙ БАЗЫ")
            results.append("----------------------------------------")
            
            # GRAB клиенты
            cursor.execute('''
            SELECT 
                SUM(new_customers) as new_customers,
                SUM(repeated_customers) as repeated_customers, 
                SUM(reactivated_customers) as reactivated_customers,
                SUM(earned_new_customers) as earned_new,
                SUM(earned_repeated_customers) as earned_repeated,
                SUM(earned_reactivated_customers) as earned_reactivated
            FROM grab_stats 
            WHERE restaurant_id = ? AND stat_date BETWEEN ? AND ?
            ''', (restaurant_id, start_date, end_date))
            
            grab_result = cursor.fetchone()
            grab_new = grab_result[0] or 0
            grab_repeat = grab_result[1] or 0
            grab_react = grab_result[2] or 0
            grab_new_earned = grab_result[3] or 0
            grab_repeat_earned = grab_result[4] or 0
            grab_react_earned = grab_result[5] or 0
            
            # GOJEK клиенты
            cursor.execute('''
            SELECT 
                SUM(new_client) as new_clients,
                SUM(active_client) as active_clients,
                SUM(returned_client) as returned_clients
            FROM gojek_stats 
            WHERE restaurant_id = ? AND stat_date BETWEEN ? AND ?
            ''', (restaurant_id, start_date, end_date))
            
            gojek_result = cursor.fetchone()
            gojek_new = gojek_result[0] or 0
            gojek_active = gojek_result[1] or 0
            gojek_returned = gojek_result[2] or 0
            
            # Общая статистика
            total_new = grab_new + gojek_new
            total_repeat = grab_repeat + gojek_active
            total_reactivated = grab_react + gojek_returned
            total_clients = total_new + total_repeat + total_reactivated
            
            results.append("📊 Структура клиентской базы (GRAB + GOJEK):")
            results.append(f"  🆕 Новые клиенты: {total_new:,} ({total_new/total_clients*100:.1f}%)")
            results.append(f"    📱 GRAB: {grab_new:,} | 🛵 GOJEK: {gojek_new:,}")
            results.append(f"  🔄 Повторные клиенты: {total_repeat:,} ({total_repeat/total_clients*100:.1f}%)")
            results.append(f"    📱 GRAB: {grab_repeat:,} | 🛵 GOJEK: {gojek_active:,}")
            results.append(f"  📲 Реактивированные: {total_reactivated:,} ({total_reactivated/total_clients*100:.1f}%)")
            results.append(f"    📱 GRAB: {grab_react:,} | 🛵 GOJEK: {gojek_returned:,}")
            results.append("")
            
            # Доходность по типам клиентов (только GRAB с рекламы)
            grab_new_avg = grab_new_earned / grab_new if grab_new > 0 else 0
            grab_repeat_avg = grab_repeat_earned / grab_repeat if grab_repeat > 0 else 0
            grab_react_avg = grab_react_earned / grab_react if grab_react > 0 else 0
            
            results.append("💰 Доходность по типам клиентов (только GRAB, только с рекламы):")
            results.append(f"  🆕 Новые: {grab_new_earned:,} IDR (средний чек: {grab_new_avg:,.0f} IDR) - только {grab_new:,} клиентов GRAB")
            results.append(f"  🔄 Повторные: {grab_repeat_earned:,} IDR (средний чек: {grab_repeat_avg:,.0f} IDR) - только {grab_repeat:,} клиентов GRAB")
            results.append(f"  📲 Реактивированные: {grab_react_earned:,} IDR (средний чек: {grab_react_avg:,.0f} IDR) - только {grab_react:,} клиентов GRAB")
            results.append("")
            results.append(f"  ⚠️ КРИТИЧНО: Данные о доходах от {gojek_new + gojek_active + gojek_returned:,} клиентов GOJEK ОТСУТСТВУЮТ в базе данных")
            results.append("  📊 Это означает, что реальная доходность может быть выше указанной")
            results.append("")
            
            # Приобретение новых клиентов по месяцам
            cursor.execute('''
            SELECT 
                strftime('%Y-%m', stat_date) as month,
                SUM(grab_new + gojek_new) as monthly_new
            FROM (
                SELECT stat_date, new_customers as grab_new, 0 as gojek_new FROM grab_stats 
                WHERE restaurant_id = ? AND stat_date BETWEEN ? AND ?
                UNION ALL
                SELECT stat_date, 0 as grab_new, new_client as gojek_new FROM gojek_stats 
                WHERE restaurant_id = ? AND stat_date BETWEEN ? AND ?
            ) monthly
            GROUP BY month
            ORDER BY month
            ''', (restaurant_id, start_date, end_date, restaurant_id, start_date, end_date))
            
            monthly_new = cursor.fetchall()
            results.append("📈 Приобретение новых клиентов по месяцам:")
            for month, new_count in monthly_new:
                month_name = 'Апрель' if month == '2025-04' else 'Май'
                results.append(f"  {month_name}: {new_count:,} новых клиентов")
            
            conn.close()
            return results
            
        except Exception as e:
            return [f"❌ Ошибка анализа клиентской базы: {e}"]

    def _get_marketing_effectiveness_analysis(self, restaurant_name, start_date, end_date):
        """Анализ маркетинговой эффективности и воронки"""
        try:
            conn = sqlite3.connect('database.sqlite')
            cursor = conn.cursor()
            
            # Получаем restaurant_id
            restaurant_query = f"SELECT id FROM restaurants WHERE name = '{restaurant_name}'"
            cursor.execute(restaurant_query)
            restaurant_result = cursor.fetchone()
            if not restaurant_result:
                return []
                
            restaurant_id = restaurant_result[0]
            
            results = []
            results.append("📈 4. МАРКЕТИНГОВАЯ ЭФФЕКТИВНОСТЬ И ВОРОНКА")
            results.append("----------------------------------------")
            
            # GRAB маркетинговая воронка
            cursor.execute('''
            SELECT 
                SUM(impressions) as total_impressions,
                SUM(unique_menu_visits) as menu_visits,
                SUM(unique_add_to_carts) as add_to_carts,
                SUM(ads_orders) as ads_orders,
                SUM(ads_spend) as ads_spend
            FROM grab_stats 
            WHERE restaurant_id = ? AND stat_date BETWEEN ? AND ?
            ''', (restaurant_id, start_date, end_date))
            
            grab_funnel = cursor.fetchone()
            impressions = grab_funnel[0] or 0
            menu_visits = grab_funnel[1] or 0
            add_to_carts = grab_funnel[2] or 0
            ads_orders = grab_funnel[3] or 0
            ads_spend = grab_funnel[4] or 0
            
            results.append("📊 Маркетинговая воронка (только GRAB - GOJEK не предоставляет данные воронки):")
            results.append(f"  👁️ Показы рекламы: {impressions:,}")
            
            if impressions > 0 and menu_visits > 0:
                ctr = menu_visits / impressions * 100
                results.append(f"  🔗 Посещения меню: {menu_visits:,} (CTR: {ctr:.2f}%)")
                
                if add_to_carts > 0:
                    visit_to_cart = add_to_carts / menu_visits * 100
                    results.append(f"  🛒 Добавления в корзину: {add_to_carts:,} (конверсия: {visit_to_cart:.2f}% от кликов)")
                    
                    if ads_orders > 0:
                        cart_to_order = ads_orders / add_to_carts * 100
                        results.append(f"  📦 Заказы от рекламы: {ads_orders:,} (конверсия: {cart_to_order:.1f}% от корзины)")
                        
                        results.append("")
                        results.append("  📊 КЛЮЧЕВЫЕ КОНВЕРСИИ:")
                        
                        impression_to_order = ads_orders / impressions * 100
                        click_to_order = ads_orders / menu_visits * 100
                        
                        results.append(f"  • 🎯 Показ → Заказ: {impression_to_order:.2f}% (основная метрика эффективности)")
                        results.append(f"  • 🔗 Клик → Заказ: {click_to_order:.1f}% (качество трафика)")
                        results.append(f"  • 🛒 Корзина → Заказ: {cart_to_order:.1f}% (качество UX)")
            
            results.append("")
            results.append("💸 Стоимость привлечения (только GRAB):")
            
            if menu_visits > 0:
                cost_per_click = ads_spend / menu_visits
                results.append(f"  💰 Стоимость клика: {cost_per_click:,.0f} IDR")
                
            if ads_orders > 0:
                cost_per_order = ads_spend / ads_orders
                results.append(f"  💰 Стоимость заказа: {cost_per_order:,.0f} IDR")
            
            # ROAS по месяцам
            results.append("🎯 ROAS по месяцам (GRAB + GOJEK):")
            
            # GRAB по месяцам
            cursor.execute('''
            SELECT 
                strftime('%Y-%m', stat_date) as month,
                SUM(ads_sales) as monthly_ads_sales,
                SUM(ads_spend) as monthly_ads_spend
            FROM grab_stats 
            WHERE restaurant_id = ? AND stat_date BETWEEN ? AND ?
            GROUP BY strftime('%Y-%m', stat_date)
            ORDER BY month
            ''', (restaurant_id, start_date, end_date))
            
            grab_monthly = cursor.fetchall()
            grab_dict = {month: (sales, spend) for month, sales, spend in grab_monthly}
            
            # GOJEK по месяцам
            cursor.execute('''
            SELECT 
                strftime('%Y-%m', stat_date) as month,
                SUM(ads_sales) as monthly_ads_sales,
                SUM(ads_spend) as monthly_ads_spend
            FROM gojek_stats 
            WHERE restaurant_id = ? AND stat_date BETWEEN ? AND ?
            GROUP BY strftime('%Y-%m', stat_date)
            ORDER BY month
            ''', (restaurant_id, start_date, end_date))
            
            gojek_monthly = cursor.fetchall()
            gojek_dict = {month: (sales, spend) for month, sales, spend in gojek_monthly}
            
            # Объединяем и выводим результаты
            for month in ['2025-04', '2025-05']:
                month_name = 'Апрель' if month == '2025-04' else 'Май'
                grab_sales, grab_spend = grab_dict.get(month, (0, 0))
                gojek_sales, gojek_spend = gojek_dict.get(month, (0, 0))
                
                total_sales = grab_sales + gojek_sales
                total_spend = grab_spend + gojek_spend
                total_roas = total_sales / total_spend if total_spend > 0 else 0
                
                grab_roas = grab_sales / grab_spend if grab_spend > 0 else 0
                gojek_roas = gojek_sales / gojek_spend if gojek_spend > 0 else 0
                
                results.append(f"  {month_name}: {total_roas:.2f}x")
                results.append(f"    📱 GRAB: {grab_roas:.2f}x (продажи: {grab_sales:,} / бюджет: {grab_spend:,.0f})")
                results.append(f"    🛵 GOJEK: {gojek_roas:.2f}x (продажи: {gojek_sales:,} / бюджет: {gojek_spend:,.0f})")
            
            conn.close()
            return results
            
        except Exception as e:
            return [f"❌ Ошибка анализа маркетинговой эффективности: {e}"]

# Совместимость с main.py
class ProperMLDetectiveAnalysis:
    """Обертка для совместимости с main.py"""
    
    def __init__(self):
        self.analyzer = ProductionSalesAnalyzer()
    
    def analyze_restaurant_performance(self, restaurant_name, start_date, end_date):
        """Главная функция для вызова из main.py"""
        return self.analyzer.analyze_restaurant_performance(restaurant_name, start_date, end_date)

# Для тестирования
if __name__ == "__main__":
    analyzer = ProductionSalesAnalyzer()
    results = analyzer.analyze_restaurant_performance("Only Eggs", "2025-04-01", "2025-05-31")
    for result in results:
        print(result)