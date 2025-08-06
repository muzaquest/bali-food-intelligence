#!/usr/bin/env python3
"""
🎯 ФИНАЛЬНЫЙ АНАЛИЗАТОР ДЛЯ КЛИЕНТА
═══════════════════════════════════════════════════════════════════════════════
✅ Максимально полезная информация для бизнеса
✅ Конкретные цифры потерь и возможностей
✅ Четкие действия с приоритетами
"""

import sqlite3
import pandas as pd
import requests
from datetime import datetime, timedelta

class FinalClientAnalyzer:
    """Финальный анализатор для клиента"""
    
    def __init__(self, db_path='database.sqlite'):
        self.db_path = db_path
        
    def generate_client_report(self, restaurant_name, days=60):
        """Генерирует полный отчет для клиента"""
        
        print(f"🎯 ФИНАЛЬНЫЙ АНАЛИЗ: '{restaurant_name}'")
        print("=" * 60)
        
        # Загружаем данные
        data = self._load_business_data(restaurant_name, days)
        
        if data.empty:
            return "❌ Недостаточно данных для анализа"
            
        # Проводим бизнес-анализ
        business_analysis = self._conduct_business_analysis(data)
        
        # Генерируем отчет
        return self._create_executive_summary(business_analysis, restaurant_name)
        
    def _load_business_data(self, restaurant_name, days):
        """Загружает бизнес-данные"""
        
        conn = sqlite3.connect(self.db_path)
        
        query = f"""
        SELECT 
            g.stat_date,
            
            -- ФИНАНСОВЫЕ ПОКАЗАТЕЛИ
            COALESCE(g.sales, 0) + COALESCE(gj.sales, 0) as daily_sales,
            COALESCE(g.orders, 0) + COALESCE(gj.orders, 0) as daily_orders,
            COALESCE(g.ads_spend, 0) + COALESCE(gj.ads_spend, 0) as ad_spend,
            COALESCE(g.ads_sales, 0) + COALESCE(gj.ads_sales, 0) as ad_sales,
            
            -- ПЛАТФОРМЫ ОТДЕЛЬНО
            COALESCE(g.sales, 0) as grab_sales,
            COALESCE(gj.sales, 0) as gojek_sales,
            COALESCE(g.orders, 0) as grab_orders,
            COALESCE(gj.orders, 0) as gojek_orders,
            
            -- КАЧЕСТВО И СЕРВИС
            COALESCE(g.rating, gj.rating, 4.0) as rating,
            COALESCE(g.cancelled_orders, 0) + COALESCE(gj.cancelled_orders, 0) as cancelled_orders,
            
            -- ОПЕРАЦИОННЫЕ ИНДИКАТОРЫ
            COALESCE(g.store_is_closed, 0) as grab_closed,
            COALESCE(gj.store_is_closed, 0) as gojek_closed,
            COALESCE(g.out_of_stock, 0) as grab_out_of_stock,
            COALESCE(gj.out_of_stock, 0) as gojek_out_of_stock,
            COALESCE(g.store_is_busy, 0) as grab_busy,
            COALESCE(gj.store_is_busy, 0) as gojek_busy,
            
            -- ВРЕМЯ И ЭФФЕКТИВНОСТЬ
            COALESCE(gj.preparation_time, '00:00:00') as prep_time,
            COALESCE(gj.delivery_time, '00:00:00') as delivery_time,
            
            -- КАЛЕНДАРНЫЕ ДАННЫЕ
            CAST(strftime('%w', g.stat_date) AS INTEGER) as weekday,
            CAST(strftime('%d', g.stat_date) AS INTEGER) as day,
            CAST(strftime('%m', g.stat_date) AS INTEGER) as month
            
        FROM grab_stats g
        LEFT JOIN gojek_stats gj ON g.restaurant_id = gj.restaurant_id 
                                 AND g.stat_date = gj.stat_date
        LEFT JOIN restaurants r ON g.restaurant_id = r.id
        WHERE r.name = '{restaurant_name}'
        AND g.stat_date >= date('now', '-{days} days')
        ORDER BY g.stat_date DESC
        """
        
        df = pd.read_sql_query(query, conn)
        conn.close()
        
        print(f"✅ Загружено {len(df)} дней данных")
        
        # Добавляем вычисляемые показатели
        df['avg_check'] = df['daily_sales'] / df['daily_orders'].replace(0, 1)
        df['cancellation_rate'] = df['cancelled_orders'] / df['daily_orders'].replace(0, 1) * 100
        df['roas'] = df['ad_sales'] / df['ad_spend'].replace(0, 1)
        df['grab_share'] = df['grab_sales'] / df['daily_sales'].replace(0, 1) * 100
        df['gojek_share'] = df['gojek_sales'] / df['daily_sales'].replace(0, 1) * 100
        
        # Операционные проблемы
        df['has_problems'] = ((df['grab_closed'] + df['gojek_closed'] + 
                              df['grab_out_of_stock'] + df['gojek_out_of_stock'] + 
                              df['grab_busy'] + df['gojek_busy']) > 0).astype(int)
        
        return df
        
    def _conduct_business_analysis(self, data):
        """Проводит бизнес-анализ"""
        
        analysis = {}
        
        # 1. БАЗОВЫЕ МЕТРИКИ
        analysis['total_days'] = len(data)
        analysis['avg_daily_sales'] = data['daily_sales'].mean()
        analysis['total_revenue'] = data['daily_sales'].sum()
        analysis['avg_daily_orders'] = data['daily_orders'].mean()
        analysis['avg_check'] = data['avg_check'].mean()
        analysis['avg_rating'] = data['rating'].mean()
        
        print(f"📊 КЛЮЧЕВЫЕ МЕТРИКИ:")
        print(f"   • Общая выручка: {analysis['total_revenue']:,.0f} IDR")
        print(f"   • Средняя выручка в день: {analysis['avg_daily_sales']:,.0f} IDR")
        print(f"   • Средний чек: {analysis['avg_check']:,.0f} IDR")
        print(f"   • Заказов в день: {analysis['avg_daily_orders']:.0f}")
        
        # 2. АНАЛИЗ ПЛАТФОРМ
        grab_total = data['grab_sales'].sum()
        gojek_total = data['gojek_sales'].sum()
        total_sales = grab_total + gojek_total
        
        analysis['grab_share'] = (grab_total / total_sales) * 100 if total_sales > 0 else 0
        analysis['gojek_share'] = (gojek_total / total_sales) * 100 if total_sales > 0 else 0
        analysis['grab_revenue'] = grab_total
        analysis['gojek_revenue'] = gojek_total
        
        # 3. МАРКЕТИНГОВАЯ ЭФФЕКТИВНОСТЬ
        marketing_data = data[data['ad_spend'] > 0]
        if len(marketing_data) > 0:
            analysis['avg_roas'] = marketing_data['roas'].mean()
            analysis['total_ad_spend'] = marketing_data['ad_spend'].sum()
            analysis['total_ad_revenue'] = marketing_data['ad_sales'].sum()
            analysis['marketing_days'] = len(marketing_data)
        else:
            analysis['avg_roas'] = 0
            analysis['total_ad_spend'] = 0
            analysis['total_ad_revenue'] = 0
            analysis['marketing_days'] = 0
            
        # 4. ОПЕРАЦИОННЫЕ ПРОБЛЕМЫ
        problem_days = data[data['has_problems'] == 1]
        analysis['problem_days_count'] = len(problem_days)
        analysis['problem_rate'] = (len(problem_days) / len(data)) * 100
        
        if len(problem_days) > 0:
            # Рассчитываем потери от проблем
            avg_normal_sales = data[data['has_problems'] == 0]['daily_sales'].mean()
            problem_sales = problem_days['daily_sales'].sum()
            potential_sales = len(problem_days) * avg_normal_sales
            analysis['operational_losses'] = potential_sales - problem_sales
        else:
            analysis['operational_losses'] = 0
            
        # 5. АНАЛИЗ ТРЕНДОВ
        recent_week = data.head(7)
        older_week = data.tail(7)
        
        if len(recent_week) >= 7 and len(older_week) >= 7:
            recent_avg = recent_week['daily_sales'].mean()
            older_avg = older_week['daily_sales'].mean()
            analysis['trend_change'] = ((recent_avg - older_avg) / older_avg) * 100
        else:
            analysis['trend_change'] = 0
            
        # 6. ЛУЧШИЕ И ХУДШИЕ ДНИ
        analysis['best_day'] = data.loc[data['daily_sales'].idxmax()]
        analysis['worst_day'] = data.loc[data['daily_sales'].idxmin()]
        
        # 7. АНАЛИЗ ДНЯ НЕДЕЛИ
        weekday_performance = data.groupby('weekday')['daily_sales'].agg(['mean', 'count']).reset_index()
        weekday_performance['weekday_name'] = weekday_performance['weekday'].map({
            0: 'Воскресенье', 1: 'Понедельник', 2: 'Вторник', 3: 'Среда', 
            4: 'Четверг', 5: 'Пятница', 6: 'Суббота'
        })
        
        analysis['best_weekday'] = weekday_performance.loc[weekday_performance['mean'].idxmax()]
        analysis['worst_weekday'] = weekday_performance.loc[weekday_performance['mean'].idxmin()]
        
        return analysis
        
    def _create_executive_summary(self, analysis, restaurant_name):
        """Создает исполнительное резюме"""
        
        report = []
        
        # ЗАГОЛОВОК
        report.append(f"📈 БИЗНЕС-ОТЧЕТ: {restaurant_name}")
        report.append("=" * 80)
        report.append(f"📅 Анализируемый период: {analysis['total_days']} дней")
        report.append("")
        
        # ФИНАНСОВЫЕ РЕЗУЛЬТАТЫ
        report.append("💰 ФИНАНСОВЫЕ РЕЗУЛЬТАТЫ:")
        report.append(f"   🎯 Общая выручка: {analysis['total_revenue']:,.0f} IDR")
        report.append(f"   📊 Средняя выручка в день: {analysis['avg_daily_sales']:,.0f} IDR")
        report.append(f"   🛒 Средний чек: {analysis['avg_check']:,.0f} IDR")
        report.append(f"   📦 Заказов в день: {analysis['avg_daily_orders']:.0f}")
        report.append(f"   ⭐ Средний рейтинг: {analysis['avg_rating']:.1f}/5.0")
        report.append("")
        
        # АНАЛИЗ ПЛАТФОРМ
        report.append("📱 АНАЛИЗ ПЛАТФОРМ:")
        report.append(f"   🟢 Grab: {analysis['grab_revenue']:,.0f} IDR ({analysis['grab_share']:.1f}%)")
        report.append(f"   🟠 Gojek: {analysis['gojek_revenue']:,.0f} IDR ({analysis['gojek_share']:.1f}%)")
        
        if analysis['grab_share'] > 70:
            report.append("   ⚠️  Слишком большая зависимость от Grab")
        elif analysis['gojek_share'] > 70:
            report.append("   ⚠️  Слишком большая зависимость от Gojek")
        else:
            report.append("   ✅ Хорошее распределение между платформами")
        report.append("")
        
        # МАРКЕТИНГ
        if analysis['marketing_days'] > 0:
            report.append("📢 МАРКЕТИНГОВАЯ ЭФФЕКТИВНОСТЬ:")
            report.append(f"   💸 Потрачено на рекламу: {analysis['total_ad_spend']:,.0f} IDR")
            report.append(f"   💰 Выручка от рекламы: {analysis['total_ad_revenue']:,.0f} IDR")
            report.append(f"   📈 ROAS: {analysis['avg_roas']:.1f} (каждая 1000 IDR рекламы = {analysis['avg_roas']*1000:.0f} IDR выручки)")
            
            if analysis['avg_roas'] < 2.0:
                report.append("   🔴 Реклама неэффективна - ROAS слишком низкий")
            elif analysis['avg_roas'] > 5.0:
                report.append("   🟢 Отличная реклама - стоит увеличить бюджет")
            else:
                report.append("   🟡 Реклама работает нормально")
        else:
            report.append("📢 МАРКЕТИНГ:")
            report.append("   ❌ Реклама не запускалась - упущенная возможность!")
        report.append("")
        
        # ОПЕРАЦИОННЫЕ ПРОБЛЕМЫ
        if analysis['problem_days_count'] > 0:
            report.append("🚨 ОПЕРАЦИОННЫЕ ПРОБЛЕМЫ:")
            report.append(f"   📊 Проблемных дней: {analysis['problem_days_count']} из {analysis['total_days']} ({analysis['problem_rate']:.1f}%)")
            report.append(f"   💸 Потери от проблем: {analysis['operational_losses']:,.0f} IDR")
            report.append("   🔧 Требуется улучшение операционных процессов")
        else:
            report.append("✅ ОПЕРАЦИОННАЯ СТАБИЛЬНОСТЬ:")
            report.append("   🎯 Операционных проблем не выявлено")
        report.append("")
        
        # ТРЕНДЫ
        report.append("📈 АНАЛИЗ ТРЕНДОВ:")
        if abs(analysis['trend_change']) < 5:
            report.append(f"   📊 Стабильные продажи ({analysis['trend_change']:+.1f}%)")
        elif analysis['trend_change'] > 5:
            report.append(f"   🚀 Позитивный тренд: рост на {analysis['trend_change']:.1f}%")
        else:
            report.append(f"   📉 Негативный тренд: падение на {abs(analysis['trend_change']):.1f}%")
        report.append("")
        
        # ЛУЧШИЕ И ХУДШИЕ ДНИ
        report.append("🏆 ЛУЧШИЕ И ХУДШИЕ РЕЗУЛЬТАТЫ:")
        best_day = analysis['best_day']
        worst_day = analysis['worst_day']
        
        report.append(f"   🥇 Лучший день: {best_day['stat_date']} - {best_day['daily_sales']:,.0f} IDR")
        report.append(f"   🥉 Худший день: {worst_day['stat_date']} - {worst_day['daily_sales']:,.0f} IDR")
        
        performance_gap = ((best_day['daily_sales'] - worst_day['daily_sales']) / best_day['daily_sales']) * 100
        report.append(f"   📊 Разброс результатов: {performance_gap:.1f}%")
        report.append("")
        
        # АНАЛИЗ ДНЯ НЕДЕЛИ
        report.append("📅 ЭФФЕКТИВНОСТЬ ПО ДНЯМ НЕДЕЛИ:")
        best_wd = analysis['best_weekday']
        worst_wd = analysis['worst_weekday']
        
        report.append(f"   🏆 Лучший: {best_wd['weekday_name']} - {best_wd['mean']:,.0f} IDR")
        report.append(f"   📉 Слабый: {worst_wd['weekday_name']} - {worst_wd['mean']:,.0f} IDR")
        
        weekday_gap = ((best_wd['mean'] - worst_wd['mean']) / best_wd['mean']) * 100
        if weekday_gap > 30:
            report.append(f"   ⚠️  Большая разница ({weekday_gap:.1f}%) - нужно усилить слабые дни")
        report.append("")
        
        # РЕКОМЕНДАЦИИ
        recommendations = self._generate_recommendations(analysis)
        
        report.append("🎯 ПРИОРИТЕТНЫЕ РЕКОМЕНДАЦИИ:")
        for i, rec in enumerate(recommendations, 1):
            report.append(f"   {i}. {rec}")
        report.append("")
        
        # ФИНАНСОВЫЙ ПОТЕНЦИАЛ
        potential_gains = self._calculate_potential(analysis)
        
        report.append("💎 ПОТЕНЦИАЛ РОСТА:")
        for gain in potential_gains:
            report.append(f"   • {gain}")
        report.append("")
        
        # ИТОГОВАЯ ОЦЕНКА
        score = self._calculate_business_score(analysis)
        report.append(f"🎖️  ОБЩАЯ ОЦЕНКА БИЗНЕСА: {score['rating']} ({score['score']}/100)")
        report.append(f"   {score['comment']}")
        
        return "\n".join(report)
        
    def _generate_recommendations(self, analysis):
        """Генерирует рекомендации"""
        
        recommendations = []
        
        # Операционные проблемы - приоритет 1
        if analysis['operational_losses'] > 0:
            recommendations.append(f"🚨 СРОЧНО: Устранить операционные проблемы (экономия {analysis['operational_losses']:,.0f} IDR)")
            
        # Маркетинг - приоритет 2
        if analysis['marketing_days'] == 0:
            recommendations.append("🚀 Запустить рекламные кампании для увеличения продаж")
        elif analysis['avg_roas'] > 5.0:
            recommendations.append(f"💰 Увеличить рекламный бюджет (текущий ROAS: {analysis['avg_roas']:.1f})")
        elif analysis['avg_roas'] < 2.0:
            recommendations.append("🎯 Оптимизировать рекламные кампании (низкий ROAS)")
            
        # Платформы - приоритет 3
        if analysis['grab_share'] > 80:
            recommendations.append("📱 Развивать присутствие на Gojek (снизить зависимость от Grab)")
        elif analysis['gojek_share'] > 80:
            recommendations.append("📱 Развивать присутствие на Grab (снизить зависимость от Gojek)")
            
        # Слабые дни - приоритет 4
        weekday_gap = ((analysis['best_weekday']['mean'] - analysis['worst_weekday']['mean']) / analysis['best_weekday']['mean']) * 100
        if weekday_gap > 30:
            recommendations.append(f"📅 Усилить маркетинг в {analysis['worst_weekday']['weekday_name']}")
            
        # Тренды - приоритет 5
        if analysis['trend_change'] < -10:
            recommendations.append("📉 Срочно выявить причины падения продаж")
            
        return recommendations[:5]  # Топ-5 рекомендаций
        
    def _calculate_potential(self, analysis):
        """Рассчитывает потенциал роста"""
        
        potential = []
        
        # Потенциал от устранения проблем
        if analysis['operational_losses'] > 0:
            monthly_savings = analysis['operational_losses'] * (30 / analysis['total_days'])
            potential.append(f"Устранение проблем: +{monthly_savings:,.0f} IDR/месяц")
            
        # Потенциал от маркетинга
        if analysis['avg_roas'] > 3.0 and analysis['total_ad_spend'] > 0:
            additional_budget = analysis['total_ad_spend'] * 0.5  # +50% к бюджету
            additional_revenue = additional_budget * analysis['avg_roas']
            monthly_potential = additional_revenue * (30 / analysis['total_days'])
            potential.append(f"Увеличение рекламы: +{monthly_potential:,.0f} IDR/месяц")
            
        # Потенциал от выравнивания дней недели
        weekday_gap = analysis['best_weekday']['mean'] - analysis['worst_weekday']['mean']
        if weekday_gap > analysis['avg_daily_sales'] * 0.3:
            weekly_potential = weekday_gap * 0.5  # Улучшение на 50%
            monthly_potential = weekly_potential * 4
            potential.append(f"Усиление слабых дней: +{monthly_potential:,.0f} IDR/месяц")
            
        if not potential:
            potential.append("Бизнес работает эффективно, крупных резервов не выявлено")
            
        return potential
        
    def _calculate_business_score(self, analysis):
        """Рассчитывает общую оценку бизнеса"""
        
        score = 0
        
        # Операционная стабильность (30 баллов)
        if analysis['problem_rate'] == 0:
            score += 30
        elif analysis['problem_rate'] < 10:
            score += 20
        elif analysis['problem_rate'] < 20:
            score += 10
            
        # Маркетинговая эффективность (25 баллов)
        if analysis['avg_roas'] > 5.0:
            score += 25
        elif analysis['avg_roas'] > 3.0:
            score += 20
        elif analysis['avg_roas'] > 2.0:
            score += 15
        elif analysis['avg_roas'] > 1.0:
            score += 10
            
        # Качество сервиса (20 баллов)
        if analysis['avg_rating'] >= 4.7:
            score += 20
        elif analysis['avg_rating'] >= 4.5:
            score += 15
        elif analysis['avg_rating'] >= 4.0:
            score += 10
        elif analysis['avg_rating'] >= 3.5:
            score += 5
            
        # Стабильность трендов (15 баллов)
        if analysis['trend_change'] > 10:
            score += 15
        elif analysis['trend_change'] > 0:
            score += 10
        elif analysis['trend_change'] > -10:
            score += 5
            
        # Диверсификация платформ (10 баллов)
        platform_balance = abs(analysis['grab_share'] - 50)
        if platform_balance < 10:
            score += 10
        elif platform_balance < 20:
            score += 7
        elif platform_balance < 30:
            score += 5
            
        # Определяем рейтинг
        if score >= 85:
            rating = "ОТЛИЧНО 🏆"
            comment = "Бизнес работает на высоком уровне"
        elif score >= 70:
            rating = "ХОРОШО ✅"
            comment = "Хорошие результаты, есть точки роста"
        elif score >= 55:
            rating = "УДОВЛЕТВОРИТЕЛЬНО 🟡"
            comment = "Стабильно, но требует улучшений"
        elif score >= 40:
            rating = "ТРЕБУЕТ ВНИМАНИЯ 🟠"
            comment = "Есть серьезные проблемы для решения"
        else:
            rating = "КРИТИЧЕСКОЕ СОСТОЯНИЕ 🔴"
            comment = "Необходимы срочные меры"
            
        return {
            'score': score,
            'rating': rating,
            'comment': comment
        }

def main():
    """Демонстрация финального анализатора"""
    
    analyzer = FinalClientAnalyzer()
    
    # Генерируем отчет для Only Eggs
    result = analyzer.generate_client_report("Only Eggs", 60)
    
    print("\n" + "="*100)
    print("📋 ФИНАЛЬНЫЙ ОТЧЕТ ДЛЯ КЛИЕНТА:")
    print("="*100)
    print(result)
    print("="*100)

if __name__ == "__main__":
    main()