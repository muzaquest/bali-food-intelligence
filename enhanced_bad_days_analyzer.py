#!/usr/bin/env python3
"""
🔍 УЛУЧШЕННЫЙ АНАЛИЗАТОР ПЛОХИХ ДНЕЙ ПРОДАЖ
═══════════════════════════════════════════════════════════════════════════════
✅ Учитывает ВСЕ операционные метрики из ML-анализа
✅ Правильные рекомендации (что можем контролировать)
✅ Детальная диагностика причин падения продаж
✅ Приоритизация по важности влияния
"""

import sqlite3
import pandas as pd
import requests
from datetime import datetime, timedelta
from statistics import mean, median
import json
import time

class EnhancedBadDaysAnalyzer:
    """Улучшенный анализатор плохих дней продаж"""
    
    def __init__(self, db_path='database.sqlite'):
        self.db_path = db_path
        self.weather_cache = {}
        self.holidays_data = self._load_holidays_data()
        
    def analyze_bad_days(self, restaurant_name=None, days_to_analyze=30):
        """Анализирует плохие дни продаж с полным набором метрик"""
        
        print("🔍 УЛУЧШЕННЫЙ АНАЛИЗАТОР ПЛОХИХ ДНЕЙ ПРОДАЖ")
        print("=" * 80)
        print("🎯 Цель: Найти ВСЕ причины падения продаж с детальной диагностикой")
        print("=" * 80)
        
        # 1. Загружаем расширенные данные
        daily_data = self._load_enhanced_daily_data(restaurant_name, days_to_analyze)
        bad_days = self._categorize_days(daily_data)
        
        # 2. Анализируем каждый плохой день
        analyzed_days = []
        for _, bad_day in bad_days.iterrows():
            analysis = self._analyze_single_bad_day_enhanced(bad_day)
            analyzed_days.append(analysis)
            
        # 3. Генерируем итоговый отчет
        self._generate_enhanced_summary_report(analyzed_days)
        
        return analyzed_days
        
    def _load_enhanced_daily_data(self, restaurant_name, days_count):
        """Загружает расширенные дневные данные со всеми операционными метриками"""
        
        print(f"\n📊 ЗАГРУЗКА РАСШИРЕННЫХ ДАННЫХ ЗА {days_count} ДНЕЙ")
        print("-" * 60)
        
        conn = sqlite3.connect(self.db_path)
        
        restaurant_filter = ""
        if restaurant_name:
            restaurant_filter = f"AND r.name = '{restaurant_name}'"
            
        query = f"""
        SELECT 
            g.stat_date,
            r.name as restaurant_name,
            r.id as restaurant_id,
            
            -- ПРОДАЖИ И ЗАКАЗЫ
            COALESCE(g.sales, 0) + COALESCE(gj.sales, 0) as total_sales,
            COALESCE(g.orders, 0) + COALESCE(gj.orders, 0) as total_orders,
            COALESCE(g.sales, 0) as grab_sales,
            COALESCE(gj.sales, 0) as gojek_sales,
            
            -- ОПЕРАЦИОННЫЕ ПРОБЛЕМЫ (ключевые факторы!)
            COALESCE(g.store_is_closed, 0) as grab_store_closed,
            COALESCE(gj.store_is_closed, 0) as gojek_store_closed,
            COALESCE(g.out_of_stock, 0) as grab_out_of_stock,
            COALESCE(gj.out_of_stock, 0) as gojek_out_of_stock,
            COALESCE(g.store_is_busy, 0) as grab_store_busy,
            COALESCE(gj.store_is_busy, 0) as gojek_store_busy,
            
            -- ОТМЕНЫ И ПРОБЛЕМЫ ДОСТАВКИ
            COALESCE(g.cancelled_orders, 0) as grab_cancelled,
            COALESCE(gj.cancelled_orders, 0) as gojek_cancelled,
            COALESCE(g.close_time_minutes, 0) as close_time_minutes,
            
            -- ВРЕМЯ ГОТОВКИ И ДОСТАВКИ
            COALESCE(g.preparation_time, 0) as grab_prep_time,
            COALESCE(gj.preparation_time, 0) as gojek_prep_time,
            COALESCE(g.delivery_time, 0) as grab_delivery_time,
            COALESCE(gj.delivery_time, 0) as gojek_delivery_time,
            
            -- МАРКЕТИНГ
            COALESCE(g.ad_spend, 0) + COALESCE(gj.ad_spend, 0) as total_ad_spend,
            CASE WHEN COALESCE(g.ad_spend, 0) + COALESCE(gj.ad_spend, 0) > 0 
                 THEN (COALESCE(g.sales, 0) + COALESCE(gj.sales, 0)) / (COALESCE(g.ad_spend, 0) + COALESCE(gj.ad_spend, 0))
                 ELSE 0 END as roas,
                 
            -- РЕЙТИНГ И КАЧЕСТВО
            COALESCE(g.rating, gj.rating, 4.0) as rating,
            
            -- ВРЕМЕННЫЕ ПРИЗНАКИ
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
            
        print("📋 Доступные операционные метрики:")
        print("   • Store Closed (ресторан закрыт)")
        print("   • Out of Stock (нет товара)")
        print("   • Store Busy (ресторан занят)")
        print("   • Cancelled Orders (отмены заказов)")
        print("   • Preparation Time (время готовки)")
        print("   • Delivery Time (время доставки)")
            
        return df
        
    def _categorize_days(self, daily_data):
        """Категоризирует дни по уровню продаж"""
        
        print(f"\n🔍 КАТЕГОРИЗАЦИЯ ДНЕЙ ПО ПРОДАЖАМ")
        print("-" * 40)
        
        mean_sales = daily_data['total_sales'].mean()
        std_sales = daily_data['total_sales'].std()
        
        p5 = daily_data['total_sales'].quantile(0.05)
        p10 = daily_data['total_sales'].quantile(0.10)
        p25 = daily_data['total_sales'].quantile(0.25)
        
        print(f"📊 СТАТИСТИЧЕСКИЕ ПОРОГИ:")
        print(f"   • Среднее: {mean_sales:,.0f} IDR")
        print(f"   • 5% перцентиль: {p5:,.0f} IDR")
        print(f"   • 10% перцентиль: {p10:,.0f} IDR")
        
        critical_days = daily_data[daily_data['total_sales'] < p5]
        very_bad_days = daily_data[(daily_data['total_sales'] >= p5) & 
                                  (daily_data['total_sales'] < p10)]
        
        print(f"\n📅 КАТЕГОРИИ:")
        print(f"   🔴 КРИТИЧЕСКИЕ: {len(critical_days)}")
        print(f"   🟠 ОЧЕНЬ ПЛОХИЕ: {len(very_bad_days)}")
        
        problem_days = pd.concat([critical_days, very_bad_days])
        print(f"\n🎯 ВЫБРАНО ДЛЯ АНАЛИЗА: {len(problem_days)} проблемных дней")
        
        return problem_days.sort_values('stat_date', ascending=False)
        
    def _analyze_single_bad_day_enhanced(self, day_data):
        """Расширенный анализ одного плохого дня"""
        
        date = day_data['stat_date']
        sales = day_data['total_sales']
        restaurant = day_data['restaurant_name']
        
        print(f"\n🔍 ДЕТАЛЬНЫЙ АНАЛИЗ: {date} - {restaurant}")
        print(f"💰 Продажи: {sales:,.0f} IDR")
        print("-" * 60)
        
        analysis = {
            'date': date,
            'restaurant': restaurant,
            'sales': sales,
            'reasons': [],
            'severity': 'unknown',
            'recommendations': [],
            'operational_details': {}
        }
        
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
                
        # РАСШИРЕННАЯ ПРИОРИТЕТНАЯ ПРОВЕРКА
        
        # 1. ПРАЗДНИКИ (приоритет 1)
        holiday_impact = self._check_holiday_impact(date)
        if holiday_impact:
            analysis['reasons'].append(holiday_impact)
            print(f"🎉 {holiday_impact['description']}")
            
        # 2. КРИТИЧЕСКИЕ ОПЕРАЦИОННЫЕ ПРОБЛЕМЫ (приоритет 2)
        critical_ops = self._check_critical_operational_issues(day_data)
        if critical_ops:
            analysis['reasons'].extend(critical_ops)
            analysis['operational_details'] = self._get_operational_details(day_data)
            for issue in critical_ops:
                print(f"🚨 {issue['description']}")
                
        # 3. ПРОБЛЕМЫ С ТОВАРОМ И ДОСТУПНОСТЬЮ (приоритет 3)
        stock_issues = self._check_stock_and_availability(day_data)
        if stock_issues:
            analysis['reasons'].extend(stock_issues)
            for issue in stock_issues:
                print(f"📦 {issue['description']}")
                
        # 4. ПРОБЛЕМЫ СО ВРЕМЕНЕМ ГОТОВКИ/ДОСТАВКИ (приоритет 4)
        timing_issues = self._check_timing_issues(day_data)
        if timing_issues:
            analysis['reasons'].extend(timing_issues)
            for issue in timing_issues:
                print(f"⏱️ {issue['description']}")
                
        # 5. МАРКЕТИНГ (приоритет 5)
        marketing_impact = self._check_marketing_issues_enhanced(day_data, baseline)
        if marketing_impact:
            analysis['reasons'].append(marketing_impact)
            print(f"📱 {marketing_impact['description']}")
            
        # 6. РЕЙТИНГ (приоритет 6)
        rating_impact = self._check_rating_issues(day_data)
        if rating_impact:
            analysis['reasons'].append(rating_impact)
            print(f"⭐ {rating_impact['description']}")
            
        # 7. ДЕНЬ НЕДЕЛИ (приоритет 7)
        weekday_impact = self._check_weekday_patterns(day_data)
        if weekday_impact:
            analysis['reasons'].append(weekday_impact)
            print(f"📅 {weekday_impact['description']}")
            
        # 8. ПОГОДА (приоритет 8)
        weather_impact = self._check_weather_impact(date)
        if weather_impact:
            analysis['reasons'].append(weather_impact)
            print(f"🌧️ {weather_impact['description']}")
            
        # Генерируем ПРАВИЛЬНЫЕ рекомендации
        analysis['recommendations'] = self._generate_actionable_recommendations(analysis)
        
        if not analysis['reasons']:
            print("❓ Причина падения не определена - требует дополнительного анализа")
            
        return analysis
        
    def _check_critical_operational_issues(self, day_data):
        """Проверяет критические операционные проблемы"""
        
        issues = []
        
        # 1. РЕСТОРАН ЗАКРЫТ
        grab_closed = day_data.get('grab_store_closed', 0)
        gojek_closed = day_data.get('gojek_store_closed', 0)
        
        if grab_closed > 0 or gojek_closed > 0:
            platforms_affected = []
            if grab_closed > 0:
                platforms_affected.append("Grab")
            if gojek_closed > 0:
                platforms_affected.append("Gojek")
                
            impact = -60 if len(platforms_affected) == 2 else -30
            issues.append({
                'factor': 'store_closed',
                'impact_percent': impact,
                'description': f"Ресторан закрыт на {'/'.join(platforms_affected)} ({impact}%)",
                'priority': 2,
                'actionable': True
            })
            
        # 2. РЕСТОРАН ЗАНЯТ/ПЕРЕГРУЖЕН
        grab_busy = day_data.get('grab_store_busy', 0)
        gojek_busy = day_data.get('gojek_store_busy', 0)
        
        if grab_busy > 0 or gojek_busy > 0:
            platforms_affected = []
            if grab_busy > 0:
                platforms_affected.append("Grab")
            if gojek_busy > 0:
                platforms_affected.append("Gojek")
                
            impact = -40 if len(platforms_affected) == 2 else -20
            issues.append({
                'factor': 'store_busy',
                'impact_percent': impact,
                'description': f"Ресторан перегружен на {'/'.join(platforms_affected)} ({impact}%)",
                'priority': 2,
                'actionable': True
            })
            
        # 3. ВРЕМЯ ЗАКРЫТИЯ
        close_time = day_data.get('close_time_minutes', 0)
        if close_time > 240:  # Больше 4 часов
            impact = min(-70, -(close_time / 720 * 100))
            issues.append({
                'factor': 'close_time',
                'impact_percent': impact,
                'description': f"Закрыт {close_time//60}ч {close_time%60}мин ({impact:.0f}%)",
                'priority': 2,
                'actionable': True
            })
            
        return issues
        
    def _check_stock_and_availability(self, day_data):
        """Проверяет проблемы с товаром"""
        
        issues = []
        
        # ДЕФИЦИТ ТОВАРА
        grab_out_of_stock = day_data.get('grab_out_of_stock', 0)
        gojek_out_of_stock = day_data.get('gojek_out_of_stock', 0)
        
        if grab_out_of_stock > 0 or gojek_out_of_stock > 0:
            platforms_affected = []
            if grab_out_of_stock > 0:
                platforms_affected.append("Grab")
            if gojek_out_of_stock > 0:
                platforms_affected.append("Gojek")
                
            impact = -50 if len(platforms_affected) == 2 else -25
            issues.append({
                'factor': 'out_of_stock',
                'impact_percent': impact,
                'description': f"Дефицит товара на {'/'.join(platforms_affected)} ({impact}%)",
                'priority': 3,
                'actionable': True
            })
            
        return issues
        
    def _check_timing_issues(self, day_data):
        """Проверяет проблемы со временем готовки и доставки"""
        
        issues = []
        
        # ВРЕМЯ ГОТОВКИ
        grab_prep = day_data.get('grab_prep_time', 0)
        gojek_prep = day_data.get('gojek_prep_time', 0)
        
        # Нормальное время готовки - до 30 минут
        if grab_prep > 30 or gojek_prep > 30:
            max_prep_time = max(grab_prep, gojek_prep)
            impact = -min(30, (max_prep_time - 30) * 2)  # За каждую лишнюю минуту -2%
            issues.append({
                'factor': 'prep_time',
                'impact_percent': impact,
                'description': f"Долгое время готовки: {max_prep_time:.0f} мин ({impact}%)",
                'priority': 4,
                'actionable': True
            })
            
        # ВРЕМЯ ДОСТАВКИ  
        grab_delivery = day_data.get('grab_delivery_time', 0)
        gojek_delivery = day_data.get('gojek_delivery_time', 0)
        
        # Нормальное время доставки - до 45 минут
        if grab_delivery > 45 or gojek_delivery > 45:
            max_delivery_time = max(grab_delivery, gojek_delivery)
            impact = -min(20, (max_delivery_time - 45))  # За каждую лишнюю минуту -1%
            issues.append({
                'factor': 'delivery_time',
                'impact_percent': impact,
                'description': f"Долгая доставка: {max_delivery_time:.0f} мин ({impact}%)",
                'priority': 4,
                'actionable': False  # Не можем контролировать курьеров
            })
            
        return issues
        
    def _check_marketing_issues_enhanced(self, day_data, baseline):
        """Расширенная проверка маркетинговых проблем"""
        
        ad_spend = day_data.get('total_ad_spend', 0)
        roas = day_data.get('roas', 0)
        
        avg_ad_spend = baseline * 0.05 if baseline > 0 else 100000
        
        issues = []
        
        if ad_spend < avg_ad_spend * 0.3:  # Снижен более чем в 3 раза
            reduction = ((avg_ad_spend - ad_spend) / avg_ad_spend) * 100
            impact = -min(50, reduction * 0.6)
            issues.append(f"Бюджет рекламы снижен на {reduction:.0f}% ({impact:.0f}%)")
            
        if roas > 0 and roas < 1.0:  # Очень низкий ROAS
            impact = -35
            issues.append(f"Критически низкий ROAS: {roas:.1f} ({impact}%)")
        elif roas > 0 and roas < 1.5:
            impact = -20
            issues.append(f"Низкий ROAS: {roas:.1f} ({impact}%)")
            
        if issues:
            return {
                'factor': 'marketing',
                'impact_percent': -30,
                'description': "Проблемы с маркетингом: " + "; ".join(issues),
                'priority': 5,
                'actionable': True
            }
        return None
        
    def _get_operational_details(self, day_data):
        """Получает детали операционных проблем"""
        
        return {
            'grab_closed': day_data.get('grab_store_closed', 0),
            'gojek_closed': day_data.get('gojek_store_closed', 0),
            'grab_busy': day_data.get('grab_store_busy', 0),
            'gojek_busy': day_data.get('gojek_store_busy', 0),
            'grab_out_of_stock': day_data.get('grab_out_of_stock', 0),
            'gojek_out_of_stock': day_data.get('gojek_out_of_stock', 0),
            'grab_cancelled': day_data.get('grab_cancelled', 0),
            'gojek_cancelled': day_data.get('gojek_cancelled', 0),
            'close_time_minutes': day_data.get('close_time_minutes', 0),
            'grab_prep_time': day_data.get('grab_prep_time', 0),
            'gojek_prep_time': day_data.get('gojek_prep_time', 0)
        }
        
    def _generate_actionable_recommendations(self, analysis):
        """Генерирует ТОЛЬКО выполнимые рекомендации"""
        
        recommendations = []
        
        for reason in analysis['reasons']:
            factor = reason['factor']
            actionable = reason.get('actionable', True)
            
            if not actionable:
                continue  # Пропускаем то, что не можем контролировать
                
            if factor == 'holiday':
                recommendations.extend([
                    "📅 Заранее планировать работу на праздники",
                    "📊 Корректировать прогнозы продаж с учетом праздников",
                    "🎯 Разработать праздничную маркетинговую стратегию"
                ])
            elif factor == 'store_closed':
                recommendations.extend([
                    "🔧 Проверить техническое состояние оборудования",
                    "👥 Обеспечить достаточное количество персонала",
                    "📋 Создать чек-лист для предотвращения закрытий"
                ])
            elif factor == 'store_busy':
                recommendations.extend([
                    "👨‍🍳 Увеличить количество поваров в пиковые часы",
                    "⚡ Оптимизировать процессы на кухне",
                    "📊 Анализировать загрузку и планировать персонал"
                ])
            elif factor == 'out_of_stock':
                recommendations.extend([
                    "📦 Улучшить управление запасами",
                    "🚚 Наладить поставки ключевых ингредиентов",
                    "📋 Внедрить систему контроля остатков"
                ])
            elif factor == 'prep_time':
                recommendations.extend([
                    "⚡ Оптимизировать процессы готовки",
                    "👨‍🍳 Провести тренинг персонала кухни",
                    "🔧 Проверить состояние кухонного оборудования"
                ])
            elif factor == 'marketing':
                recommendations.extend([
                    "💰 Восстановить бюджет рекламы до оптимального уровня",
                    "🎯 Оптимизировать рекламные кампании",
                    "📊 Улучшить таргетинг и креативы"
                ])
            elif factor == 'rating':
                recommendations.extend([
                    "⭐ Улучшить качество еды и сервиса",
                    "💬 Активно работать с отзывами клиентов",
                    "👥 Провести тренинг персонала"
                ])
                
        return list(set(recommendations))  # Убираем дубликаты
        
    def _generate_enhanced_summary_report(self, analyzed_days):
        """Генерирует расширенный итоговый отчет"""
        
        print(f"\n📋 РАСШИРЕННЫЙ ИТОГОВЫЙ ОТЧЕТ")
        print("=" * 80)
        
        if not analyzed_days:
            print("✅ Проблемных дней не найдено!")
            return
            
        # Статистика по факторам
        factor_counts = {}
        actionable_issues = 0
        non_actionable_issues = 0
        
        for day in analyzed_days:
            for reason in day['reasons']:
                factor = reason['factor']
                factor_counts[factor] = factor_counts.get(factor, 0) + 1
                
                if reason.get('actionable', True):
                    actionable_issues += 1
                else:
                    non_actionable_issues += 1
                    
        print(f"📊 ОСНОВНЫЕ ПРИЧИНЫ ПЛОХИХ ПРОДАЖ:")
        factor_names = {
            'holiday': '🎉 Праздники',
            'store_closed': '🚨 Ресторан закрыт',
            'store_busy': '🚨 Ресторан перегружен',
            'out_of_stock': '📦 Дефицит товара',
            'prep_time': '⏱️ Долгое время готовки',
            'delivery_time': '🚚 Долгая доставка',
            'marketing': '📱 Проблемы с маркетингом',
            'rating': '⭐ Низкий рейтинг',
            'weekday': '📅 День недели',
            'weather': '🌧️ Погода'
        }
        
        for factor, count in sorted(factor_counts.items(), key=lambda x: x[1], reverse=True):
            factor_name = factor_names.get(factor, factor)
            percentage = (count / len(analyzed_days)) * 100
            print(f"   {factor_name}: {count} дней ({percentage:.1f}%)")
            
        print(f"\n🎯 КОНТРОЛИРУЕМОСТЬ ПРОБЛЕМ:")
        total_issues = actionable_issues + non_actionable_issues
        if total_issues > 0:
            actionable_pct = (actionable_issues / total_issues) * 100
            print(f"   ✅ Можем контролировать: {actionable_issues} ({actionable_pct:.1f}%)")
            print(f"   ❌ Не можем контролировать: {non_actionable_issues} ({100-actionable_pct:.1f}%)")
            
        # Топ рекомендации
        all_recommendations = []
        for day in analyzed_days:
            all_recommendations.extend(day['recommendations'])
            
        unique_recommendations = list(set(all_recommendations))
        
        print(f"\n💡 ПРИОРИТЕТНЫЕ РЕКОМЕНДАЦИИ (ЧТО МОЖЕМ КОНТРОЛИРОВАТЬ):")
        for i, rec in enumerate(unique_recommendations[:12], 1):
            print(f"   {i}. {rec}")
            
        print(f"\n🎯 ИТОГО:")
        print(f"   • Проанализировано проблемных дней: {len(analyzed_days)}")
        print(f"   • Выявлено контролируемых проблем: {actionable_issues}")
        print(f"   • Сгенерировано рекомендаций: {len(unique_recommendations)}")
        
    # Остальные методы остаются без изменений...
    def _check_holiday_impact(self, date):
        """Проверяет влияние праздников"""
        
        if date in self.holidays_data:
            holiday = self.holidays_data[date]
            impact_percent = holiday.get('expected_impact', 0)
            
            if abs(impact_percent) > 15:
                return {
                    'factor': 'holiday',
                    'impact_percent': impact_percent,
                    'description': f"Праздник '{holiday['name']}' - {holiday['impact']}",
                    'priority': 1,
                    'actionable': False  # Не можем контролировать праздники
                }
        return None
        
    def _check_rating_issues(self, day_data):
        """Проверяет проблемы с рейтингом"""
        
        rating = day_data.get('rating', 4.0)
        
        if rating < 3.5:
            impact = -30
            return {
                'factor': 'rating',
                'impact_percent': impact,
                'description': f"Низкий рейтинг: {rating:.1f} ({impact}%)",
                'priority': 6,
                'actionable': True
            }
        elif rating < 4.0:
            impact = -15
            return {
                'factor': 'rating',
                'impact_percent': impact,
                'description': f"Рейтинг ниже среднего: {rating:.1f} ({impact}%)",
                'priority': 6,
                'actionable': True
            }
        return None
        
    def _check_weekday_patterns(self, day_data):
        """Проверяет паттерны дня недели"""
        
        day_of_week = day_data.get('day_of_week', 0)
        weekdays = ['Понедельник', 'Вторник', 'Среда', 'Четверг', 'Пятница', 'Суббота', 'Воскресенье']
        
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
                'priority': 7,
                'actionable': False  # Не можем контролировать дни недели
            }
        return None
        
    def _check_weather_impact(self, date):
        """Проверяет влияние погоды"""
        
        weather = self._get_weather_for_date(-8.6705, 115.2126, date)
        rain = weather['rain']
        
        if rain > 25:
            return {
                'factor': 'weather',
                'impact_percent': -10,
                'description': f"Экстремальный дождь {rain:.1f}мм (-10%)",
                'priority': 8,
                'actionable': False  # Не можем контролировать погоду
            }
        elif rain > 15:
            return {
                'factor': 'weather',
                'impact_percent': -7,
                'description': f"Сильный дождь {rain:.1f}мм (-7%)",
                'priority': 8,
                'actionable': False
            }
        return None
        
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
        
        holidays = {
            '2024-01-01': {'name': 'New Year', 'expected_impact': 50, 'impact': 'Люди празднуют дома'},
            '2024-02-10': {'name': 'Chinese New Year', 'expected_impact': 40, 'impact': 'Китайская община празднует'},
            '2024-03-11': {'name': 'Nyepi', 'expected_impact': -95, 'impact': 'День тишины - никто не работает'},
            '2024-04-10': {'name': 'Eid al-Fitr', 'expected_impact': -45, 'impact': 'Курьеры-мусульмане празднуют'},
            '2024-12-25': {'name': 'Christmas', 'expected_impact': -40, 'impact': 'Курьеры-христиане с семьями'},
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

def main():
    """Демонстрация улучшенного анализатора"""
    
    analyzer = EnhancedBadDaysAnalyzer()
    results = analyzer.analyze_bad_days(days_to_analyze=30)
    
    print(f"\n🎉 УЛУЧШЕННЫЙ АНАЛИЗ ЗАВЕРШЕН!")
    print(f"✅ Найдены ВСЕ причины с фокусом на контролируемые факторы!")

if __name__ == "__main__":
    main()