#!/usr/bin/env python3
"""
🔍 УМНЫЙ АНАЛИЗАТОР ПЛОХИХ ДНЕЙ ПРОДАЖ
═══════════════════════════════════════════════════════════════════════════════
✅ Реализует четкую логику анализа причин падения продаж
✅ Приоритизация факторов по важности влияния
✅ Конкретные вердикты и рекомендации для клиента
✅ Автоматическое выявление и анализ проблемных дней
"""

import sqlite3
import pandas as pd
import requests
from datetime import datetime, timedelta
from statistics import mean, median
import json
import time

class SmartBadDaysAnalyzer:
    """Умный анализатор плохих дней продаж"""
    
    def __init__(self, db_path='database.sqlite'):
        self.db_path = db_path
        self.weather_cache = {}
        self.holidays_data = self._load_holidays_data()
        
    def analyze_bad_days(self, restaurant_name=None, days_to_analyze=30):
        """Анализирует плохие дни продаж"""
        
        print("🔍 УМНЫЙ АНАЛИЗАТОР ПЛОХИХ ДНЕЙ ПРОДАЖ")
        print("=" * 80)
        print("🎯 Цель: Найти причины падения продаж и дать рекомендации")
        print("=" * 80)
        
        # 1. Загружаем и категоризируем дни
        daily_data = self._load_daily_sales_data(restaurant_name, days_to_analyze)
        bad_days = self._categorize_days(daily_data)
        
        # 2. Анализируем каждый плохой день
        analyzed_days = []
        for _, bad_day in bad_days.iterrows():
            analysis = self._analyze_single_bad_day(bad_day)
            analyzed_days.append(analysis)
            
        # 3. Генерируем итоговый отчет
        self._generate_summary_report(analyzed_days)
        
        return analyzed_days
        
    def _load_daily_sales_data(self, restaurant_name, days_count):
        """Загружает дневные данные продаж"""
        
        print(f"\n📊 ЗАГРУЗКА ДАННЫХ ЗА ПОСЛЕДНИЕ {days_count} ДНЕЙ")
        print("-" * 60)
        
        conn = sqlite3.connect(self.db_path)
        
        # Фильтр по ресторану если указан
        restaurant_filter = ""
        if restaurant_name:
            restaurant_filter = f"AND r.name = '{restaurant_name}'"
            
        query = f"""
        SELECT 
            g.stat_date,
            r.name as restaurant_name,
            r.id as restaurant_id,
            
            -- Продажи и заказы
            COALESCE(g.sales, 0) + COALESCE(gj.sales, 0) as total_sales,
            COALESCE(g.orders, 0) + COALESCE(gj.orders, 0) as total_orders,
            COALESCE(g.sales, 0) as grab_sales,
            COALESCE(gj.sales, 0) as gojek_sales,
            
            -- Операционные показатели
            COALESCE(g.cancelled_orders, 0) + COALESCE(gj.cancelled_orders, 0) as cancelled_orders,
            COALESCE(g.close_time_minutes, 0) as close_time_minutes,
            
            -- Маркетинг
            COALESCE(g.ad_spend, 0) + COALESCE(gj.ad_spend, 0) as total_ad_spend,
            CASE WHEN COALESCE(g.ad_spend, 0) + COALESCE(gj.ad_spend, 0) > 0 
                 THEN (COALESCE(g.sales, 0) + COALESCE(gj.sales, 0)) / (COALESCE(g.ad_spend, 0) + COALESCE(gj.ad_spend, 0))
                 ELSE 0 END as roas,
                 
            -- Рейтинг
            COALESCE(g.rating, gj.rating, 4.0) as rating,
            
            -- Временные признаки
            CAST(strftime('%w', g.stat_date) AS INTEGER) as day_of_week,
            CAST(strftime('%m', g.stat_date) AS INTEGER) as month,
            CAST(strftime('%d', g.stat_date) AS INTEGER) as day
            
        FROM grab_stats g
        LEFT JOIN gojek_stats gj ON g.restaurant_id = gj.restaurant_id 
                                 AND g.stat_date = gj.stat_date
        LEFT JOIN restaurants r ON g.restaurant_id = r.id
        WHERE g.stat_date >= date('now', '-{days_count} days')
        AND r.name IS NOT NULL
        {restaurant_filter}
        AND (g.sales > 0 OR gj.sales > 0)
        ORDER BY g.stat_date DESC
        """
        
        df = pd.read_sql_query(query, conn)
        conn.close()
        
        if restaurant_name:
            print(f"✅ Загружено {len(df)} дней для ресторана '{restaurant_name}'")
        else:
            print(f"✅ Загружено {len(df)} записей по {df['restaurant_name'].nunique()} ресторанам")
            
        return df
        
    def _categorize_days(self, daily_data):
        """Категоризирует дни по уровню продаж"""
        
        print(f"\n🔍 КАТЕГОРИЗАЦИЯ ДНЕЙ ПО ПРОДАЖАМ")
        print("-" * 40)
        
        # Рассчитываем статистические пороги
        mean_sales = daily_data['total_sales'].mean()
        std_sales = daily_data['total_sales'].std()
        
        # Перцентили
        p5 = daily_data['total_sales'].quantile(0.05)
        p10 = daily_data['total_sales'].quantile(0.10)
        p25 = daily_data['total_sales'].quantile(0.25)
        
        print(f"📊 СТАТИСТИЧЕСКИЕ ПОРОГИ:")
        print(f"   • Среднее: {mean_sales:,.0f} IDR")
        print(f"   • Стандартное отклонение: {std_sales:,.0f} IDR")
        print(f"   • 5% перцентиль: {p5:,.0f} IDR")
        print(f"   • 10% перцентиль: {p10:,.0f} IDR")
        print(f"   • 25% перцентиль: {p25:,.0f} IDR")
        
        # Категоризируем дни
        critical_days = daily_data[daily_data['total_sales'] < p5]
        very_bad_days = daily_data[(daily_data['total_sales'] >= p5) & 
                                  (daily_data['total_sales'] < p10)]
        bad_days = daily_data[(daily_data['total_sales'] >= p10) & 
                             (daily_data['total_sales'] < p25)]
        
        print(f"\n📅 КАТЕГОРИИ ДНЕЙ:")
        print(f"   🔴 КРИТИЧЕСКИЕ дни (< 5%): {len(critical_days)}")
        print(f"   🟠 ОЧЕНЬ ПЛОХИЕ дни (5-10%): {len(very_bad_days)}")
        print(f"   🟡 ПЛОХИЕ дни (10-25%): {len(bad_days)}")
        
        # Для анализа берем критические и очень плохие дни
        problem_days = pd.concat([critical_days, very_bad_days])
        
        print(f"\n🎯 ВЫБРАНО ДЛЯ АНАЛИЗА: {len(problem_days)} проблемных дней")
        
        return problem_days.sort_values('stat_date', ascending=False)
        
    def _analyze_single_bad_day(self, day_data):
        """Анализирует один плохой день по приоритетной логике"""
        
        date = day_data['stat_date']
        sales = day_data['total_sales']
        restaurant = day_data['restaurant_name']
        
        print(f"\n🔍 АНАЛИЗ ДНЯ: {date} - {restaurant}")
        print(f"💰 Продажи: {sales:,.0f} IDR")
        print("-" * 50)
        
        analysis = {
            'date': date,
            'restaurant': restaurant,
            'sales': sales,
            'reasons': [],
            'severity': 'unknown',
            'recommendations': []
        }
        
        # Определяем серьезность падения
        # Для этого нужна базовая линия - возьмем среднее за последние 30 дней
        baseline = self._get_baseline_sales(restaurant, date)
        if baseline > 0:
            drop_percent = ((sales - baseline) / baseline) * 100
            analysis['drop_percent'] = drop_percent
            analysis['baseline'] = baseline
            
            if drop_percent < -50:
                analysis['severity'] = 'critical'
            elif drop_percent < -30:
                analysis['severity'] = 'serious'
            elif drop_percent < -15:
                analysis['severity'] = 'noticeable'
            else:
                analysis['severity'] = 'minor'
                
        # ПРИОРИТЕТНАЯ ПРОВЕРКА ФАКТОРОВ
        
        # 1. ПРАЗДНИКИ (приоритет 1)
        holiday_impact = self._check_holiday_impact(date)
        if holiday_impact:
            analysis['reasons'].append(holiday_impact)
            print(f"🎉 {holiday_impact['description']}")
            
        # 2. ОПЕРАЦИОННЫЕ ПРОБЛЕМЫ (приоритет 2 - могут быть критичными)
        operational_impact = self._check_operational_issues(day_data)
        if operational_impact:
            analysis['reasons'].append(operational_impact)
            print(f"🚚 {operational_impact['description']}")
            
        # 3. МАРКЕТИНГ (приоритет 3)
        marketing_impact = self._check_marketing_issues(day_data, baseline)
        if marketing_impact:
            analysis['reasons'].append(marketing_impact)
            print(f"📱 {marketing_impact['description']}")
            
        # 4. РЕЙТИНГ (приоритет 4)
        rating_impact = self._check_rating_issues(day_data)
        if rating_impact:
            analysis['reasons'].append(rating_impact)
            print(f"⭐ {rating_impact['description']}")
            
        # 5. ДЕНЬ НЕДЕЛИ (приоритет 5)
        weekday_impact = self._check_weekday_patterns(day_data)
        if weekday_impact:
            analysis['reasons'].append(weekday_impact)
            print(f"📅 {weekday_impact['description']}")
            
        # 6. ПОГОДА (приоритет 6)
        weather_impact = self._check_weather_impact(date)
        if weather_impact:
            analysis['reasons'].append(weather_impact)
            print(f"🌧️ {weather_impact['description']}")
            
        # Генерируем рекомендации
        analysis['recommendations'] = self._generate_recommendations(analysis)
        
        if not analysis['reasons']:
            print("❓ Причина падения не определена - требует дополнительного анализа")
            
        return analysis
        
    def _check_holiday_impact(self, date):
        """Проверяет влияние праздников"""
        
        if date in self.holidays_data:
            holiday = self.holidays_data[date]
            impact_percent = holiday.get('expected_impact', 0)
            
            if abs(impact_percent) > 15:  # Порог значимости
                return {
                    'factor': 'holiday',
                    'impact_percent': impact_percent,
                    'description': f"Праздник '{holiday['name']}' - {holiday['impact']}",
                    'priority': 1
                }
        return None
        
    def _check_operational_issues(self, day_data):
        """Проверяет операционные проблемы"""
        
        issues = []
        
        # Проверяем время закрытия
        close_time = day_data.get('close_time_minutes', 0)
        if close_time > 240:  # Больше 4 часов закрыт
            impact = min(-80, -(close_time / 720 * 100))  # Максимум -80%
            issues.append(f"Закрыт {close_time//60}ч {close_time%60}мин ({impact:.0f}%)")
            
        # Проверяем отмены
        cancelled = day_data.get('cancelled_orders', 0)
        total_orders = day_data.get('total_orders', 1)
        if cancelled > 0 and total_orders > 0:
            cancel_rate = (cancelled / (total_orders + cancelled)) * 100
            if cancel_rate > 30:  # Высокий процент отмен
                impact = -min(50, cancel_rate)
                issues.append(f"Высокий % отмен: {cancel_rate:.1f}% ({impact:.0f}%)")
                
        # Проверяем проблемы с платформами
        grab_sales = day_data.get('grab_sales', 0)
        gojek_sales = day_data.get('gojek_sales', 0)
        
        if grab_sales == 0 and gojek_sales > 0:
            issues.append("Проблемы с Grab (-30%)")
        elif gojek_sales == 0 and grab_sales > 0:
            issues.append("Проблемы с Gojek (-25%)")
            
        if issues:
            return {
                'factor': 'operational',
                'impact_percent': -50,  # Примерная оценка
                'description': "Операционные проблемы: " + "; ".join(issues),
                'priority': 2
            }
        return None
        
    def _check_marketing_issues(self, day_data, baseline):
        """Проверяет проблемы с маркетингом"""
        
        ad_spend = day_data.get('total_ad_spend', 0)
        roas = day_data.get('roas', 0)
        
        # Получаем среднее для сравнения (упрощенно)
        avg_ad_spend = baseline * 0.05 if baseline > 0 else 100000  # 5% от продаж
        
        issues = []
        
        # Проверяем бюджет рекламы
        if ad_spend < avg_ad_spend * 0.5:  # Снижен более чем в 2 раза
            reduction = ((avg_ad_spend - ad_spend) / avg_ad_spend) * 100
            impact = -min(40, reduction * 0.5)  # Примерное влияние
            issues.append(f"Бюджет рекламы снижен на {reduction:.0f}% ({impact:.0f}%)")
            
        # Проверяем ROAS
        if roas > 0 and roas < 1.5:  # Низкий ROAS
            impact = -20
            issues.append(f"Низкий ROAS: {roas:.1f} ({impact}%)")
            
        if issues:
            return {
                'factor': 'marketing',
                'impact_percent': -25,  # Примерная оценка
                'description': "Проблемы с маркетингом: " + "; ".join(issues),
                'priority': 3
            }
        return None
        
    def _check_rating_issues(self, day_data):
        """Проверяет проблемы с рейтингом"""
        
        rating = day_data.get('rating', 4.0)
        
        if rating < 3.5:  # Низкий рейтинг
            impact = -30
            return {
                'factor': 'rating',
                'impact_percent': impact,
                'description': f"Низкий рейтинг: {rating:.1f} ({impact}%)",
                'priority': 4
            }
        elif rating < 4.0:  # Средний рейтинг
            impact = -15
            return {
                'factor': 'rating',
                'impact_percent': impact,
                'description': f"Рейтинг ниже среднего: {rating:.1f} ({impact}%)",
                'priority': 4
            }
        return None
        
    def _check_weekday_patterns(self, day_data):
        """Проверяет паттерны дня недели"""
        
        day_of_week = day_data.get('day_of_week', 0)
        weekdays = ['Понедельник', 'Вторник', 'Среда', 'Четверг', 'Пятница', 'Суббота', 'Воскресенье']
        
        # Типичные паттерны (упрощенно)
        weekday_impacts = {
            0: -15,  # Понедельник
            1: -5,   # Вторник
            2: 0,    # Среда
            3: 0,    # Четверг
            4: 5,    # Пятница
            5: 10,   # Суббота
            6: 5     # Воскресенье
        }
        
        impact = weekday_impacts.get(day_of_week, 0)
        if abs(impact) > 10:
            return {
                'factor': 'weekday',
                'impact_percent': impact,
                'description': f"{weekdays[day_of_week]} - обычно {impact:+}% от среднего",
                'priority': 5
            }
        return None
        
    def _check_weather_impact(self, date):
        """Проверяет влияние погоды"""
        
        # Получаем погоду (координаты Денпасара)
        weather = self._get_weather_for_date(-8.6705, 115.2126, date)
        
        rain = weather['rain']
        
        if rain > 25:  # Экстремальный дождь
            return {
                'factor': 'weather',
                'impact_percent': -10,
                'description': f"Экстремальный дождь {rain:.1f}мм (-10%)",
                'priority': 6
            }
        elif rain > 15:  # Сильный дождь
            return {
                'factor': 'weather',
                'impact_percent': -7,
                'description': f"Сильный дождь {rain:.1f}мм (-7%)",
                'priority': 6
            }
        return None
        
    def _generate_recommendations(self, analysis):
        """Генерирует рекомендации на основе анализа"""
        
        recommendations = []
        
        for reason in analysis['reasons']:
            factor = reason['factor']
            
            if factor == 'holiday':
                recommendations.extend([
                    "Заранее планировать праздники",
                    "Увеличить бонусы курьерам в праздничные дни",
                    "Предупреждать клиентов о возможных задержках"
                ])
            elif factor == 'operational':
                recommendations.extend([
                    "Устранить операционные проблемы немедленно",
                    "Проверить работу кухни и персонала",
                    "Наладить работу с платформами доставки"
                ])
            elif factor == 'marketing':
                recommendations.extend([
                    "Восстановить бюджет рекламы",
                    "Оптимизировать рекламные кампании",
                    "Улучшить ROAS"
                ])
            elif factor == 'rating':
                recommendations.extend([
                    "Улучшить качество еды и сервиса",
                    "Активно работать с отзывами",
                    "Провести тренинг персонала"
                ])
            elif factor == 'weather':
                recommendations.extend([
                    "Мониторить прогноз погоды",
                    "Подготовить план на случай непогоды"
                ])
                
        return list(set(recommendations))  # Убираем дубликаты
        
    def _get_baseline_sales(self, restaurant_name, date):
        """Получает базовые продажи для сравнения"""
        
        conn = sqlite3.connect(self.db_path)
        
        query = f"""
        SELECT AVG(COALESCE(g.sales, 0) + COALESCE(gj.sales, 0)) as avg_sales
        FROM grab_stats g
        LEFT JOIN gojek_stats gj ON g.restaurant_id = gj.restaurant_id 
                                 AND g.stat_date = gj.stat_date
        LEFT JOIN restaurants r ON g.restaurant_id = r.id
        WHERE r.name = '{restaurant_name}'
        AND g.stat_date BETWEEN date('{date}', '-30 days') AND date('{date}', '-1 day')
        AND (g.sales > 0 OR gj.sales > 0)
        """
        
        result = pd.read_sql_query(query, conn)
        conn.close()
        
        return result['avg_sales'].iloc[0] if len(result) > 0 else 0
        
    def _load_holidays_data(self):
        """Загружает данные о праздниках"""
        
        # Упрощенная база праздников (в реальности загружать из JSON)
        holidays = {
            '2024-01-01': {'name': 'New Year', 'expected_impact': 50, 'impact': 'Люди празднуют дома'},
            '2024-02-10': {'name': 'Chinese New Year', 'expected_impact': 40, 'impact': 'Китайская община празднует'},
            '2024-03-11': {'name': 'Nyepi', 'expected_impact': -95, 'impact': 'День тишины - никто не работает'},
            '2024-04-10': {'name': 'Eid al-Fitr', 'expected_impact': -45, 'impact': 'Курьеры-мусульмане празднуют'},
            '2024-12-25': {'name': 'Christmas', 'expected_impact': -40, 'impact': 'Курьеры-христиане с семьями'},
            # Добавить остальные 164 праздника...
        }
        
        return holidays
        
    def _get_weather_for_date(self, lat, lon, date):
        """Получает погоду для даты"""
        
        cache_key = f"{lat}_{lon}_{date}"
        
        if cache_key in self.weather_cache:
            return self.weather_cache[cache_key]
            
        default_weather = {'temp': 28.0, 'rain': 0.0, 'wind': 5.0}
        
        try:
            url = "https://archive-api.open-meteo.com/v1/archive"
            params = {
                'latitude': lat,
                'longitude': lon,
                'start_date': date,
                'end_date': date,
                'hourly': 'temperature_2m,precipitation,wind_speed_10m',
                'timezone': 'Asia/Jakarta'
            }
            
            response = requests.get(url, params=params, timeout=5)
            if response.status_code == 200:
                data = response.json()
                hourly = data.get('hourly', {})
                
                if hourly:
                    temperatures = hourly.get('temperature_2m', [])
                    precipitation = hourly.get('precipitation', [])
                    wind_speed = hourly.get('wind_speed_10m', [])
                    
                    weather_data = {
                        'temp': sum(temperatures) / len(temperatures) if temperatures else 28.0,
                        'rain': sum(precipitation) if precipitation else 0.0,
                        'wind': sum(wind_speed) / len(wind_speed) if wind_speed else 5.0
                    }
                    
                    self.weather_cache[cache_key] = weather_data
                    return weather_data
                    
        except Exception:
            pass
            
        self.weather_cache[cache_key] = default_weather
        return default_weather
        
    def _generate_summary_report(self, analyzed_days):
        """Генерирует итоговый отчет"""
        
        print(f"\n📋 ИТОГОВЫЙ ОТЧЕТ ПО ПЛОХИМ ДНЯМ")
        print("=" * 80)
        
        if not analyzed_days:
            print("✅ Проблемных дней не найдено!")
            return
            
        # Статистика по факторам
        factor_counts = {}
        for day in analyzed_days:
            for reason in day['reasons']:
                factor = reason['factor']
                factor_counts[factor] = factor_counts.get(factor, 0) + 1
                
        print(f"📊 ОСНОВНЫЕ ПРИЧИНЫ ПЛОХИХ ПРОДАЖ:")
        factor_names = {
            'holiday': '🎉 Праздники',
            'operational': '🚚 Операционные проблемы',
            'marketing': '📱 Маркетинг',
            'rating': '⭐ Рейтинг',
            'weekday': '📅 День недели',
            'weather': '🌧️ Погода'
        }
        
        for factor, count in sorted(factor_counts.items(), key=lambda x: x[1], reverse=True):
            factor_name = factor_names.get(factor, factor)
            percentage = (count / len(analyzed_days)) * 100
            print(f"   {factor_name}: {count} дней ({percentage:.1f}%)")
            
        # Рекомендации
        all_recommendations = []
        for day in analyzed_days:
            all_recommendations.extend(day['recommendations'])
            
        unique_recommendations = list(set(all_recommendations))
        
        print(f"\n💡 ОБЩИЕ РЕКОМЕНДАЦИИ:")
        for i, rec in enumerate(unique_recommendations[:10], 1):
            print(f"   {i}. {rec}")
            
        print(f"\n🎯 ИТОГО ПРОАНАЛИЗИРОВАНО: {len(analyzed_days)} проблемных дней")

def main():
    """Демонстрация работы умного анализатора"""
    
    analyzer = SmartBadDaysAnalyzer()
    
    # Анализируем плохие дни за последний месяц
    results = analyzer.analyze_bad_days(days_to_analyze=30)
    
    print(f"\n🎉 АНАЛИЗ ЗАВЕРШЕН!")
    print(f"✅ Найдены причины плохих продаж и даны рекомендации")

if __name__ == "__main__":
    main()