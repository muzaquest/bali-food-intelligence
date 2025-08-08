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
                            
                            if order.get('platform') == 'Grab':
                                grab_fakes += quantity
                                grab_fake_amount += amount
                            elif order.get('platform') == 'Gojek':
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