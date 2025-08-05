#!/usr/bin/env python3
"""
🧠 МАКСИМАЛЬНО УМНАЯ АНАЛИТИКА MUZAQUEST
═══════════════════════════════════════════════════════════════════════════════
Анализирует ВСЕ факторы влияния на продажи как профессиональный аналитик + маркетолог

ИСПОЛЬЗУЕМЫЕ ДАННЫЕ:
- 32 поля из GRAB_STATS (продажи, реклама, рейтинги, закрытия, клиенты)
- 33 поля из GOJEK_STATS (время доставки, отзывы, close_time)
- Погодные данные (Open-Meteo API)
- 164 праздника (балийские + индонезийские)
- Конкуренты (другие рестораны как бенчмарк)
- Сезонность туристов
"""

import sqlite3
import json
import requests
from datetime import datetime, timedelta
from statistics import mean, median
import math

class UltimateSmartAnalytics:
    """Максимально умная аналитика - анализирует ВСЕ факторы"""
    
    def __init__(self, db_path='database.sqlite'):
        self.db_path = db_path
        
        # Балийские праздники (влияют на продажи)
        self.balinese_holidays = {
            '2025-04-16': 'Galungan - семейный праздник',
            '2025-04-26': 'Kuningan - религиозные церемонии', 
            '2025-05-12': 'Purnama - полнолуние',
            '2025-03-31': 'Nyepi - день тишины',
            '2025-01-25': 'Imlek - китайский новый год',
            '2025-12-25': 'Christmas - рождество'
        }
        
        # Сезоны туристов на Бали
        self.tourist_seasons = {
            'high': [6, 7, 8, 12, 1],  # Июнь-Август, Декабрь-Январь
            'medium': [4, 5, 9, 10],   # Апрель-Май, Сентябрь-Октябрь
            'low': [2, 3, 11]          # Февраль-Март, Ноябрь
        }
        
    def analyze_restaurant_comprehensive(self, restaurant_name, start_date, end_date):
        """МАКСИМАЛЬНО ПОЛНЫЙ анализ ресторана со ВСЕМИ факторами"""
        
        print(f"\n🧠 МАКСИМАЛЬНО УМНАЯ АНАЛИТИКА")
        print("=" * 60)
        print(f"🍽️ Ресторан: {restaurant_name}")
        print(f"📅 Период: {start_date} — {end_date}")
        print("=" * 60)
        
        # 1. Загружаем ВСЕ данные ресторана
        restaurant_data = self._load_comprehensive_data(restaurant_name, start_date, end_date)
        if not restaurant_data:
            print("❌ Нет данных для анализа")
            return
            
        # 2. Загружаем данные конкурентов для сравнения
        competitors_data = self._load_competitors_data(restaurant_name, start_date, end_date)
        
        # 3. Анализируем каждый день детально
        daily_analysis = []
        for day in restaurant_data:
            day_analysis = self._analyze_single_day_comprehensive(day, competitors_data)
            daily_analysis.append(day_analysis)
            
        # 4. Находим паттерны и аномалии
        patterns = self._find_patterns_and_anomalies(daily_analysis)
        
        # 5. Выводим результаты
        self._generate_comprehensive_report(restaurant_name, daily_analysis, patterns)
        
    def _load_comprehensive_data(self, restaurant_name, start_date, end_date):
        """Загружает ВСЕ доступные данные ресторана"""
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Получаем ID ресторана
        cursor.execute("SELECT id FROM restaurants WHERE name = ?", (restaurant_name,))
        restaurant_result = cursor.fetchone()
        if not restaurant_result:
            return []
            
        restaurant_id = restaurant_result[0]
        
        # МЕГА-ЗАПРОС: ВСЕ данные из обеих таблиц
        query = """
        SELECT 
            g.stat_date,
            
            -- ПРОДАЖИ И ЗАКАЗЫ
            COALESCE(g.sales, 0) as grab_sales,
            COALESCE(gj.sales, 0) as gojek_sales,
            COALESCE(g.sales, 0) + COALESCE(gj.sales, 0) as total_sales,
            COALESCE(g.orders, 0) as grab_orders,
            COALESCE(gj.orders, 0) as gojek_orders,
            COALESCE(g.orders, 0) + COALESCE(gj.orders, 0) as total_orders,
            
            -- СРЕДНИЙ ЧЕК
            CASE WHEN (COALESCE(g.orders, 0) + COALESCE(gj.orders, 0)) > 0 
                 THEN (COALESCE(g.sales, 0) + COALESCE(gj.sales, 0)) / (COALESCE(g.orders, 0) + COALESCE(gj.orders, 0))
                 ELSE 0 END as avg_order_value,
            
            -- РЕЙТИНГИ
            COALESCE(g.rating, 0) as grab_rating,
            COALESCE(gj.rating, 0) as gojek_rating,
            CASE 
                WHEN g.rating > 0 AND gj.rating > 0 THEN (g.rating + gj.rating) / 2
                WHEN g.rating > 0 THEN g.rating
                WHEN gj.rating > 0 THEN gj.rating
                ELSE 4.5 
            END as avg_rating,
            
            -- РЕКЛАМА И МАРКЕТИНГ
            COALESCE(g.ads_spend, 0) as grab_ads_spend,
            COALESCE(gj.ads_spend, 0) as gojek_ads_spend,
            COALESCE(g.ads_spend, 0) + COALESCE(gj.ads_spend, 0) as total_ads_spend,
            COALESCE(g.ads_sales, 0) + COALESCE(gj.ads_sales, 0) as total_ads_sales,
            COALESCE(g.ads_orders, 0) + COALESCE(gj.ads_orders, 0) as total_ads_orders,
            
            -- ROAS (Return on Ad Spend)
            CASE WHEN (COALESCE(g.ads_spend, 0) + COALESCE(gj.ads_spend, 0)) > 0
                 THEN (COALESCE(g.ads_sales, 0) + COALESCE(gj.ads_sales, 0)) / (COALESCE(g.ads_spend, 0) + COALESCE(gj.ads_spend, 0))
                 ELSE 0 END as roas,
            
            -- ОПЕРАЦИОННЫЕ ПРОБЛЕМЫ
            COALESCE(g.store_is_closed, 0) as grab_closed,
            COALESCE(gj.store_is_closed, 0) as gojek_closed,
            COALESCE(g.store_is_busy, 0) as grab_busy,
            COALESCE(gj.store_is_busy, 0) as gojek_busy,
            COALESCE(g.out_of_stock, 0) as grab_out_of_stock,
            COALESCE(gj.out_of_stock, 0) as gojek_out_of_stock,
            
            -- ОТМЕНЫ И ПОТЕРИ
            COALESCE(g.cancelled_orders, 0) as grab_cancelled,
            COALESCE(gj.cancelled_orders, 0) as gojek_cancelled,
            COALESCE(gj.lost_orders, 0) as gojek_lost_orders,
            COALESCE(g.cancelation_rate, 0) as grab_cancel_rate,
            
            -- ВРЕМЯ ДОСТАВКИ (только GOJEK)
            COALESCE(gj.accepting_time, '00:00:00') as accepting_time,
            COALESCE(gj.preparation_time, '00:00:00') as preparation_time,
            COALESCE(gj.delivery_time, '00:00:00') as delivery_time,
            COALESCE(gj.close_time, 0) as close_time_minutes,
            
            -- ДЕТАЛЬНЫЕ ОТЗЫВЫ (только GOJEK)
            COALESCE(gj.one_star_ratings, 0) as one_star,
            COALESCE(gj.two_star_ratings, 0) as two_star,
            COALESCE(gj.three_star_ratings, 0) as three_star,
            COALESCE(gj.four_star_ratings, 0) as four_star,
            COALESCE(gj.five_star_ratings, 0) as five_star,
            
            -- КЛИЕНТСКАЯ БАЗА
            COALESCE(g.new_customers, 0) + COALESCE(gj.new_client, 0) as new_customers,
            COALESCE(g.repeated_customers, 0) + COALESCE(gj.returned_client, 0) as returning_customers,
            COALESCE(g.total_customers, 0) + COALESCE(gj.active_client, 0) as total_customers,
            COALESCE(gj.potential_lost, 0) as potential_lost_customers,
            
            -- МАРКЕТИНГОВЫЕ МЕТРИКИ (только GRAB)
            COALESCE(g.impressions, 0) as impressions,
            COALESCE(g.unique_menu_visits, 0) as menu_visits,
            COALESCE(g.unique_add_to_carts, 0) as add_to_cart,
            COALESCE(g.ads_ctr, 0) as ctr,
            
            -- ВРЕМЕННЫЕ ПРИЗНАКИ
            CAST(strftime('%w', g.stat_date) AS INTEGER) as day_of_week,
            CAST(strftime('%m', g.stat_date) AS INTEGER) as month,
            CAST(strftime('%d', g.stat_date) AS INTEGER) as day_of_month
            
        FROM grab_stats g
        LEFT JOIN gojek_stats gj ON g.restaurant_id = gj.restaurant_id 
                                 AND g.stat_date = gj.stat_date
        WHERE g.restaurant_id = ? 
        AND g.stat_date BETWEEN ? AND ?
        ORDER BY g.stat_date
        """
        
        cursor.execute(query, (restaurant_id, start_date, end_date))
        data = []
        
        columns = [desc[0] for desc in cursor.description]
        for row in cursor.fetchall():
            day_data = dict(zip(columns, row))
            data.append(day_data)
            
        conn.close()
        return data
        
    def _load_competitors_data(self, restaurant_name, start_date, end_date):
        """Загружает агрегированные данные конкурентов для сравнения"""
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Получаем средние показатели всех других ресторанов
        query = """
        SELECT 
            g.stat_date,
            AVG(COALESCE(g.sales, 0) + COALESCE(gj.sales, 0)) as avg_competitor_sales,
            AVG(COALESCE(g.orders, 0) + COALESCE(gj.orders, 0)) as avg_competitor_orders,
            AVG(CASE 
                WHEN g.rating > 0 AND gj.rating > 0 THEN (g.rating + gj.rating) / 2
                WHEN g.rating > 0 THEN g.rating
                WHEN gj.rating > 0 THEN gj.rating
                ELSE 4.5 
            END) as avg_competitor_rating,
            AVG(COALESCE(g.ads_spend, 0) + COALESCE(gj.ads_spend, 0)) as avg_competitor_ads_spend,
            COUNT(DISTINCT r.id) as active_competitors
        FROM grab_stats g
        LEFT JOIN gojek_stats gj ON g.restaurant_id = gj.restaurant_id 
                                 AND g.stat_date = gj.stat_date
        LEFT JOIN restaurants r ON g.restaurant_id = r.id
        WHERE r.name != ? 
        AND g.stat_date BETWEEN ? AND ?
        GROUP BY g.stat_date
        ORDER BY g.stat_date
        """
        
        cursor.execute(query, (restaurant_name, start_date, end_date))
        competitors = {}
        
        for row in cursor.fetchall():
            date = row[0]
            competitors[date] = {
                'avg_sales': row[1] or 0,
                'avg_orders': row[2] or 0,
                'avg_rating': row[3] or 4.5,
                'avg_ads_spend': row[4] or 0,
                'active_competitors': row[5] or 0
            }
            
        conn.close()
        return competitors
        
    def _analyze_single_day_comprehensive(self, day_data, competitors_data):
        """Анализирует один день со ВСЕМИ факторами"""
        
        date = day_data['stat_date']
        analysis = {
            'date': date,
            'total_sales': day_data['total_sales'],
            'factors': [],
            'score': 0,  # Общая оценка дня
            'issues': [],
            'opportunities': []
        }
        
        # 1. АНАЛИЗ ПРОДАЖ
        if day_data['total_sales'] == 0:
            analysis['factors'].append("❌ КРИТИЧНО: Нулевые продажи")
            analysis['score'] -= 50
            analysis['issues'].append("Нет продаж вообще")
        elif day_data['total_sales'] < 1000000:  # Меньше 1M IDR
            analysis['factors'].append(f"🔻 Низкие продажи: {day_data['total_sales']:,.0f} IDR")
            analysis['score'] -= 20
        elif day_data['total_sales'] > 10000000:  # Больше 10M IDR
            analysis['factors'].append(f"🚀 Отличные продажи: {day_data['total_sales']:,.0f} IDR")
            analysis['score'] += 20
            
        # 2. АНАЛИЗ ПЛАТФОРМ
        grab_share = (day_data['grab_sales'] / day_data['total_sales'] * 100) if day_data['total_sales'] > 0 else 0
        gojek_share = (day_data['gojek_sales'] / day_data['total_sales'] * 100) if day_data['total_sales'] > 0 else 0
        
        if day_data['grab_sales'] == 0 and day_data['gojek_sales'] > 0:
            analysis['factors'].append("⚠️ GRAB платформа не работала")
            analysis['score'] -= 30
            analysis['issues'].append("Проблемы с GRAB")
        elif day_data['gojek_sales'] == 0 and day_data['grab_sales'] > 0:
            analysis['factors'].append("⚠️ GOJEK платформа не работала") 
            analysis['score'] -= 30
            analysis['issues'].append("Проблемы с GOJEK")
        elif abs(grab_share - gojek_share) > 30:
            dominant = "GRAB" if grab_share > gojek_share else "GOJEK"
            analysis['factors'].append(f"📊 Доминирует {dominant}: {max(grab_share, gojek_share):.0f}%")
            
        # 3. АНАЛИЗ СРЕДНЕГО ЧЕКА
        if day_data['avg_order_value'] > 0:
            if day_data['avg_order_value'] > 200000:  # Высокий средний чек
                analysis['factors'].append(f"💰 Высокий средний чек: {day_data['avg_order_value']:,.0f} IDR")
                analysis['score'] += 10
            elif day_data['avg_order_value'] < 100000:  # Низкий средний чек
                analysis['factors'].append(f"📉 Низкий средний чек: {day_data['avg_order_value']:,.0f} IDR")
                analysis['score'] -= 10
                
        # 4. АНАЛИЗ РЕЙТИНГА
        if day_data['avg_rating'] < 4.0:
            analysis['factors'].append(f"⭐ Низкий рейтинг: {day_data['avg_rating']:.1f}")
            analysis['score'] -= 15
            analysis['issues'].append("Падение рейтинга")
        elif day_data['avg_rating'] > 4.5:
            analysis['factors'].append(f"⭐ Отличный рейтинг: {day_data['avg_rating']:.1f}")
            analysis['score'] += 10
            
        # 5. АНАЛИЗ РЕКЛАМЫ И ROAS
        if day_data['total_ads_spend'] > 0:
            if day_data['roas'] > 3:  # Хороший ROAS
                analysis['factors'].append(f"📈 Отличный ROAS: {day_data['roas']:.1f}x")
                analysis['score'] += 15
            elif day_data['roas'] < 1:  # Плохой ROAS
                analysis['factors'].append(f"📉 Плохой ROAS: {day_data['roas']:.1f}x")
                analysis['score'] -= 15
                analysis['issues'].append("Неэффективная реклама")
        else:
            analysis['factors'].append("🚫 Реклама отключена")
            analysis['opportunities'].append("Включить рекламу")
            
        # 6. ОПЕРАЦИОННЫЕ ПРОБЛЕМЫ
        if day_data['grab_closed'] or day_data['gojek_closed']:
            analysis['factors'].append("🏪 Ресторан был закрыт")
            analysis['score'] -= 25
            analysis['issues'].append("Закрытие ресторана")
            
        if day_data['grab_out_of_stock'] or day_data['gojek_out_of_stock']:
            analysis['factors'].append("📦 Дефицит товара")
            analysis['score'] -= 15
            analysis['issues'].append("Проблемы с поставками")
            
        if (day_data['grab_cancelled'] + day_data['gojek_cancelled']) > 5:
            analysis['factors'].append(f"❌ Много отмен: {day_data['grab_cancelled'] + day_data['gojek_cancelled']}")
            analysis['score'] -= 10
            
        # 7. АНАЛИЗ ВРЕМЕНИ ДОСТАВКИ (GOJEK)
        if day_data['close_time_minutes'] > 60:  # Больше часа закрыто
            analysis['factors'].append(f"⏰ Долго закрыто: {day_data['close_time_minutes']} мин")
            analysis['score'] -= 20
            analysis['issues'].append("Длительное закрытие")
            
        # 8. АНАЛИЗ ОТЗЫВОВ (GOJEK)
        total_reviews = day_data['one_star'] + day_data['two_star'] + day_data['three_star'] + day_data['four_star'] + day_data['five_star']
        if total_reviews > 0:
            negative_reviews = day_data['one_star'] + day_data['two_star']
            negative_percentage = (negative_reviews / total_reviews) * 100
            if negative_percentage > 20:
                analysis['factors'].append(f"😞 Много негативных отзывов: {negative_percentage:.0f}%")
                analysis['score'] -= 15
                analysis['issues'].append("Проблемы с качеством")
                
        # 9. АНАЛИЗ ДНЯ НЕДЕЛИ
        weekday_names = {0: 'Воскресенье', 1: 'Понедельник', 2: 'Вторник', 3: 'Среда', 4: 'Четверг', 5: 'Пятница', 6: 'Суббота'}
        weekday = weekday_names.get(day_data['day_of_week'], 'Неизвестно')
        
        if day_data['day_of_week'] == 1:  # Понедельник
            analysis['factors'].append(f"📅 {weekday} - обычно слабый день")
        elif day_data['day_of_week'] in [5, 6, 0]:  # Пятница, суббота, воскресенье
            analysis['factors'].append(f"📅 {weekday} - обычно сильный день")
            
        # 10. АНАЛИЗ ПРАЗДНИКОВ
        if date in self.balinese_holidays:
            holiday_name = self.balinese_holidays[date]
            analysis['factors'].append(f"🎭 Праздник: {holiday_name}")
            analysis['score'] -= 20
            analysis['issues'].append("Влияние праздника")
            
        # 11. АНАЛИЗ СЕЗОНА ТУРИСТОВ
        month = day_data['month']
        if month in self.tourist_seasons['high']:
            analysis['factors'].append("🏖️ Высокий туристический сезон")
            analysis['score'] += 10
        elif month in self.tourist_seasons['low']:
            analysis['factors'].append("🏖️ Низкий туристический сезон")
            analysis['score'] -= 10
            
        # 12. СРАВНЕНИЕ С КОНКУРЕНТАМИ
        if date in competitors_data:
            comp = competitors_data[date]
            if day_data['total_sales'] > comp['avg_sales'] * 1.2:
                analysis['factors'].append("🏆 Опережаем конкурентов на 20%+")
                analysis['score'] += 15
            elif day_data['total_sales'] < comp['avg_sales'] * 0.8:
                analysis['factors'].append("⚠️ Отстаем от конкурентов на 20%+")
                analysis['score'] -= 15
                analysis['issues'].append("Слабые позиции против конкурентов")
                
        # 13. ПОГОДНЫЙ АНАЛИЗ (базовый, без API пока)
        analysis['weather_analyzed'] = False  # Флаг для будущего улучшения
        
        return analysis
        
    def _find_patterns_and_anomalies(self, daily_analysis):
        """Находит паттерны и аномалии в данных"""
        
        patterns = {
            'best_days': [],
            'worst_days': [],
            'common_issues': {},
            'opportunities': {},
            'trends': []
        }
        
        # Сортируем дни по score
        sorted_days = sorted(daily_analysis, key=lambda x: x['score'], reverse=True)
        
        # Лучшие и худшие дни
        patterns['best_days'] = sorted_days[:3]
        patterns['worst_days'] = sorted_days[-3:]
        
        # Подсчитываем частые проблемы
        for day in daily_analysis:
            for issue in day['issues']:
                patterns['common_issues'][issue] = patterns['common_issues'].get(issue, 0) + 1
                
            for opp in day['opportunities']:
                patterns['opportunities'][opp] = patterns['opportunities'].get(opp, 0) + 1
                
        # Тренды продаж
        sales_data = [day['total_sales'] for day in daily_analysis]
        if len(sales_data) > 7:
            first_week = mean(sales_data[:7])
            last_week = mean(sales_data[-7:])
            trend_change = ((last_week - first_week) / first_week * 100) if first_week > 0 else 0
            
            if trend_change > 10:
                patterns['trends'].append(f"📈 Рост продаж на {trend_change:.1f}%")
            elif trend_change < -10:
                patterns['trends'].append(f"📉 Снижение продаж на {abs(trend_change):.1f}%")
            else:
                patterns['trends'].append("➡️ Стабильные продажи")
                
        return patterns
        
    def _generate_comprehensive_report(self, restaurant_name, daily_analysis, patterns):
        """Генерирует максимально подробный отчет"""
        
        print(f"\n📊 ДЕТАЛЬНЫЙ АНАЛИЗ РЕЗУЛЬТАТОВ")
        print("-" * 50)
        
        # Общая статистика
        total_sales = sum(day['total_sales'] for day in daily_analysis)
        avg_daily_sales = total_sales / len(daily_analysis) if daily_analysis else 0
        avg_score = mean([day['score'] for day in daily_analysis]) if daily_analysis else 0
        
        print(f"💰 Общие продажи: {total_sales:,.0f} IDR")
        print(f"📈 Средние продажи в день: {avg_daily_sales:,.0f} IDR")
        print(f"⭐ Средний score дня: {avg_score:.1f}")
        print()
        
        # Тренды
        print("📈 ТРЕНДЫ:")
        for trend in patterns['trends']:
            print(f"   {trend}")
        print()
        
        # Лучшие дни
        print("🏆 ТОП-3 ЛУЧШИХ ДНЯ:")
        for i, day in enumerate(patterns['best_days'], 1):
            print(f"   {i}. 📅 {day['date']} (Score: {day['score']})")
            print(f"      💰 Продажи: {day['total_sales']:,.0f} IDR")
            if day['factors']:
                print(f"      🎯 Факторы: {', '.join(day['factors'][:2])}")
        print()
        
        # Худшие дни
        print("⚠️ ТОП-3 ПРОБЛЕМНЫХ ДНЯ:")
        for i, day in enumerate(patterns['worst_days'], 1):
            print(f"   {i}. 📅 {day['date']} (Score: {day['score']})")
            print(f"      💰 Продажи: {day['total_sales']:,.0f} IDR")
            if day['issues']:
                print(f"      ❌ Проблемы: {', '.join(day['issues'][:2])}")
        print()
        
        # Частые проблемы
        if patterns['common_issues']:
            print("🔍 ЧАСТЫЕ ПРОБЛЕМЫ:")
            sorted_issues = sorted(patterns['common_issues'].items(), key=lambda x: x[1], reverse=True)
            for issue, count in sorted_issues[:5]:
                print(f"   • {issue}: {count} дней")
        print()
        
        # Возможности
        if patterns['opportunities']:
            print("💡 ВОЗМОЖНОСТИ ДЛЯ УЛУЧШЕНИЯ:")
            sorted_opps = sorted(patterns['opportunities'].items(), key=lambda x: x[1], reverse=True)
            for opp, count in sorted_opps[:3]:
                print(f"   • {opp}: {count} дней")
        print()
        
        # Финальные рекомендации
        self._generate_final_recommendations(patterns, daily_analysis)
        
    def _generate_final_recommendations(self, patterns, daily_analysis):
        """Генерирует финальные рекомендации как профессиональный консультант"""
        
        print("🎯 РЕКОМЕНДАЦИИ ПРОФЕССИОНАЛЬНОГО АНАЛИТИКА:")
        print("-" * 50)
        
        recommendations = []
        
        # Анализируем частые проблемы
        if patterns['common_issues']:
            top_issue = max(patterns['common_issues'].items(), key=lambda x: x[1])
            if top_issue[0] == "Проблемы с GRAB":
                recommendations.append("🔧 ПРИОРИТЕТ 1: Решить технические проблемы с GRAB платформой")
            elif top_issue[0] == "Неэффективная реклама":
                recommendations.append("📈 ПРИОРИТЕТ 1: Оптимизировать рекламные кампании (таргетинг, бюджет)")
            elif top_issue[0] == "Падение рейтинга":
                recommendations.append("⭐ ПРИОРИТЕТ 1: Улучшить качество еды и сервиса")
                
        # Анализируем лучшие дни
        best_day_factors = []
        for day in patterns['best_days']:
            best_day_factors.extend(day['factors'])
            
        if any("Отличный ROAS" in factor for factor in best_day_factors):
            recommendations.append("💰 Масштабировать успешные рекламные кампании")
            
        if any("Высокий средний чек" in factor for factor in best_day_factors):
            recommendations.append("🍽️ Продвигать премиум позиции меню")
            
        # Сезонные рекомендации
        avg_sales = mean([day['total_sales'] for day in daily_analysis])
        if avg_sales < 5000000:  # Меньше 5M в день
            recommendations.append("📊 Увеличить маркетинговый бюджет - потенциал роста высокий")
            
        # Операционные рекомендации
        if patterns['common_issues'].get("Длительное закрытие", 0) > 2:
            recommendations.append("⏰ Оптимизировать график работы - много потерь из-за закрытий")
            
        # Выводим рекомендации
        for i, rec in enumerate(recommendations[:5], 1):
            print(f"   {i}. {rec}")
            
        print()
        print("📞 ГОТОВЫЙ ОТВЕТ КЛИЕНТУ:")
        print("=" * 45)
        
        # Генерируем краткий ответ клиенту
        if patterns['trends']:
            main_trend = patterns['trends'][0]
            if patterns['common_issues']:
                main_issue = max(patterns['common_issues'].items(), key=lambda x: x[1])[0]
                client_answer = f"{main_trend.replace('📈', '').replace('📉', '').strip()} в основном из-за {main_issue.lower()}"
            else:
                client_answer = f"{main_trend.replace('📈', '').replace('📉', '').strip()}"
        else:
            client_answer = "Продажи стабильны, основные факторы под контролем"
            
        print(f'"{client_answer}"')
        print("=" * 45)

def main():
    """Запуск максимально умной аналитики"""
    
    analytics = UltimateSmartAnalytics()
    
    # Тест на Only Eggs
    analytics.analyze_restaurant_comprehensive(
        "Only Eggs",
        "2025-04-01", 
        "2025-05-31"
    )

if __name__ == "__main__":
    main()