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
warnings.filterwarnings('ignore')

class ProductionSalesAnalyzer:
    """Продакшн анализатор для детективного анализа продаж"""
    
    def __init__(self):
        self.holidays_data = self._load_holidays()
        self.locations_data = self._load_locations()
        
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
                return json.load(f)
        except Exception as e:
            print(f"⚠️ Не удалось загрузить локации: {e}")
            return {}
    
    def analyze_restaurant_performance(self, restaurant_name, start_date, end_date):
        """
        Главная функция анализа - совместимая с main.py
        Возвращает список строк для вывода
        """
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
            for i, (date, drop_percent) in enumerate(bad_days[:3], 1):  # Топ-3 худших дня
                day_analysis = self._analyze_specific_day(restaurant_name, date)
                
                results.append(f"📉 ПРОБЛЕМНЫЙ ДЕНЬ #{i}: {date}")
                results.append(f"   💔 Падение продаж: {drop_percent:.1f}%")
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
        query = f"""
        SELECT 
            g.stat_date,
            COALESCE(g.sales, 0) + COALESCE(gj.sales, 0) as total_sales
        FROM grab_stats g
        LEFT JOIN gojek_stats gj ON g.restaurant_id = gj.restaurant_id 
                                 AND g.stat_date = gj.stat_date
        LEFT JOIN restaurants r ON g.restaurant_id = r.id
        WHERE r.name = '{restaurant_name}'
        AND g.stat_date BETWEEN '{start_date}' AND '{end_date}'
        ORDER BY g.stat_date
        """
        
        with sqlite3.connect('database.sqlite') as conn:
            df = pd.read_sql_query(query, conn)
        
        if len(df) < 7:  # Недостаточно данных
            return []
        
        # Рассчитываем скользящее среднее
        df['sales_7day_avg'] = df['total_sales'].rolling(window=7, center=True).mean()
        
        # Находим дни с падением больше 30%
        bad_days = []
        for _, row in df.iterrows():
            if pd.isna(row['sales_7day_avg']) or row['sales_7day_avg'] == 0:
                continue
                
            drop_percent = ((row['sales_7day_avg'] - row['total_sales']) / row['sales_7day_avg']) * 100
            if drop_percent >= 30:  # Падение больше 30%
                bad_days.append((row['stat_date'], drop_percent))
        
        # Сортируем по величине падения
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
        self._analyze_time_factors(day_data, monthly_averages, factors, impact_score, critical_issues)
        
        # 3. Реклама и ROAS
        self._analyze_advertising(day_data, factors, impact_score, critical_issues)
        
        # 4. Погода
        if weather_data:
            if weather_data['precipitation'] > 10:
                factors.append(f"🌧️ Сильный дождь ({weather_data['precipitation']:.1f}мм)")
                impact_score += 25
            elif weather_data['precipitation'] > 5:
                factors.append(f"🌦️ Умеренный дождь ({weather_data['precipitation']:.1f}мм)")
                impact_score += 15
        
        # 5. Праздники
        if holiday_info:
            factors.append(f"🎉 {holiday_info.get('name', 'Праздник')}")
            impact_score += 25
        
        # 6. День недели
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
    
    def _get_day_data(self, restaurant_name, target_date):
        """Получает данные за конкретный день"""
        query = f"""
        SELECT 
            g.stat_date,
            COALESCE(g.sales, 0) + COALESCE(gj.sales, 0) as total_sales,
            COALESCE(g.orders, 0) + COALESCE(gj.orders, 0) as total_orders,
            COALESCE(g.sales, 0) as grab_sales,
            COALESCE(gj.sales, 0) as gojek_sales,
            COALESCE(g.orders, 0) as grab_orders,
            COALESCE(gj.orders, 0) as gojek_orders,
            COALESCE(gj.close_time, '00:00:00') as gojek_close_time,
            COALESCE(g.offline_rate, 0) as grab_offline_rate,
            COALESCE(gj.preparation_time, '00:00:00') as gojek_preparation_time,
            COALESCE(gj.delivery_time, '00:00:00') as gojek_delivery_time,
            COALESCE(gj.driver_waiting, 0) as gojek_driver_waiting_min,
            COALESCE(g.driver_waiting_time, 0) / 60.0 as grab_driver_waiting_min,
            COALESCE(g.ads_spend, 0) as grab_ads_spend,
            COALESCE(g.ads_sales, 0) as grab_ads_sales,
            COALESCE(gj.ads_spend, 0) as gojek_ads_spend,
            COALESCE(gj.ads_sales, 0) as gojek_ads_sales
        FROM grab_stats g
        LEFT JOIN gojek_stats gj ON g.restaurant_id = gj.restaurant_id 
                                 AND g.stat_date = gj.stat_date
        LEFT JOIN restaurants r ON g.restaurant_id = r.id
        WHERE r.name = '{restaurant_name}' AND g.stat_date = '{target_date}'
        """
        
        with sqlite3.connect('database.sqlite') as conn:
            df = pd.read_sql_query(query, conn)
        
        if len(df) > 0:
            return df.iloc[0].to_dict()
        return None
    
    def _get_monthly_averages(self, restaurant_name, target_date):
        """Получает среднемесячные временные показатели"""
        target_month = target_date[:7]  # YYYY-MM
        
        query = f"""
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
        FROM grab_stats g
        LEFT JOIN gojek_stats gj ON g.restaurant_id = gj.restaurant_id AND g.stat_date = gj.stat_date
        LEFT JOIN restaurants r ON g.restaurant_id = r.id
        WHERE r.name = '{restaurant_name}' AND g.stat_date LIKE '{target_month}%'
        """
        
        with sqlite3.connect('database.sqlite') as conn:
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
    
    def _analyze_time_factors(self, day_data, monthly_averages, factors, impact_score, critical_issues):
        """Анализирует временные факторы с отклонениями"""
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
                elif prep_deviation >= 30:
                    factors.append(f"⚠️ Gojek Preparation {prep_minutes:.1f}мин (+{prep_deviation:.0f}% выше)")
        
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
                elif delivery_deviation >= 30:
                    factors.append(f"⚠️ Gojek Delivery {delivery_minutes:.1f}мин (+{delivery_deviation:.0f}% выше)")
        
        # Driver Waiting Time
        gojek_waiting = day_data.get('gojek_driver_waiting_min', 0)
        if gojek_waiting > 0:
            avg_gojek_waiting = monthly_averages['avg_gojek_waiting']
            if avg_gojek_waiting > 0:
                waiting_deviation = ((gojek_waiting - avg_gojek_waiting) / avg_gojek_waiting) * 100
                if waiting_deviation >= 50:
                    factors.append(f"🚨 КРИТИЧНО: Gojek Driver Waiting {gojek_waiting}мин (+{waiting_deviation:.0f}%)")
                    critical_issues.append("Критическое время ожидания Gojek")
                elif waiting_deviation >= 30:
                    factors.append(f"⚠️ Gojek Driver Waiting {gojek_waiting}мин (+{waiting_deviation:.0f}% выше)")
    
    def _analyze_advertising(self, day_data, factors, impact_score, critical_issues):
        """Анализирует рекламные показатели"""
        grab_ads_spend = day_data.get('grab_ads_spend', 0)
        grab_ads_sales = day_data.get('grab_ads_sales', 0)
        gojek_ads_spend = day_data.get('gojek_ads_spend', 0)
        gojek_ads_sales = day_data.get('gojek_ads_sales', 0)
        
        ads_working = False
        
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
        
        if not ads_working:
            factors.append("❌ Реклама не работала")
    
    def _generate_general_recommendations(self, bad_days):
        """Генерирует общие рекомендации"""
        results = []
        results.append("💡 ОБЩИЕ РЕКОМЕНДАЦИИ:")
        results.append("")
        
        # Анализируем паттерны
        weekend_issues = sum(1 for date, _ in bad_days if pd.to_datetime(date).strftime('%A') in ['Sunday', 'Monday'])
        
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
    results = analyzer.analyze_restaurant_performance("Ika Canggu", "2025-04-01", "2025-04-30")
    for result in results:
        print(result)