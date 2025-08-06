#!/usr/bin/env python3
"""
🧠 УМНЫЙ АНАЛИЗАТОР РЕСТОРАНА
═══════════════════════════════════════════════════════════════════════════════
✅ Анализирует реальные причины падения продаж
✅ Сравнивает с историческими паттернами
✅ Дает конкретные рекомендации клиенту
"""

import sqlite3
import pandas as pd
import requests
from datetime import datetime, timedelta

class SmartRestaurantAnalyzer:
    """Умный анализатор ресторана"""
    
    def __init__(self, db_path='database.sqlite'):
        self.db_path = db_path
        
    def analyze_restaurant_detailed(self, restaurant_name, days=60):
        """Детальный анализ ресторана для клиента"""
        
        print(f"🧠 УМНЫЙ АНАЛИЗ РЕСТОРАНА '{restaurant_name}'")
        print("=" * 60)
        
        # Загружаем данные
        data = self._load_comprehensive_data(restaurant_name, days)
        
        if data.empty:
            return "❌ Недостаточно данных для анализа"
            
        # Анализируем паттерны
        analysis = self._comprehensive_analysis(data, restaurant_name)
        
        # Генерируем отчет для клиента
        return self._generate_executive_report(analysis, restaurant_name)
        
    def _load_comprehensive_data(self, restaurant_name, days):
        """Загружает полные данные ресторана"""
        
        conn = sqlite3.connect(self.db_path)
        
        query = f"""
        SELECT 
            g.stat_date,
            
            -- ПРОДАЖИ И ЗАКАЗЫ
            COALESCE(g.sales, 0) + COALESCE(gj.sales, 0) as total_sales,
            COALESCE(g.orders, 0) + COALESCE(gj.orders, 0) as total_orders,
            COALESCE(g.sales, 0) as grab_sales,
            COALESCE(gj.sales, 0) as gojek_sales,
            COALESCE(g.orders, 0) as grab_orders,
            COALESCE(gj.orders, 0) as gojek_orders,
            
            -- МАРКЕТИНГ И ЭФФЕКТИВНОСТЬ
            COALESCE(g.ads_spend, 0) + COALESCE(gj.ads_spend, 0) as total_ads,
            COALESCE(g.ads_sales, 0) + COALESCE(gj.ads_sales, 0) as ads_sales,
            
            -- ОПЕРАЦИОННЫЕ ПРОБЛЕМЫ
            COALESCE(g.store_is_closed, 0) as grab_closed,
            COALESCE(gj.store_is_closed, 0) as gojek_closed,
            COALESCE(g.out_of_stock, 0) as grab_out_of_stock,
            COALESCE(gj.out_of_stock, 0) as gojek_out_of_stock,
            COALESCE(g.store_is_busy, 0) as grab_busy,
            COALESCE(gj.store_is_busy, 0) as gojek_busy,
            
            -- ОТМЕНЫ И КАЧЕСТВО
            COALESCE(g.cancelled_orders, 0) as grab_cancelled,
            COALESCE(gj.cancelled_orders, 0) as gojek_cancelled,
            COALESCE(g.rating, gj.rating, 4.0) as rating,
            
            -- ВРЕМЕННЫЕ ФАКТОРЫ
            CAST(strftime('%w', g.stat_date) AS INTEGER) as day_of_week,
            CAST(strftime('%d', g.stat_date) AS INTEGER) as day_of_month,
            CAST(strftime('%m', g.stat_date) AS INTEGER) as month,
            
            -- ВРЕМЯ ДОСТАВКИ (только Gojek)
            COALESCE(gj.preparation_time, '00:00:00') as prep_time,
            COALESCE(gj.delivery_time, '00:00:00') as delivery_time
            
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
        
        # Добавляем вычисляемые поля
        df['avg_order_value'] = df['total_sales'] / df['total_orders'].replace(0, 1)
        df['cancel_rate'] = (df['grab_cancelled'] + df['gojek_cancelled']) / df['total_orders'].replace(0, 1) * 100
        df['roas'] = df['ads_sales'] / df['total_ads'].replace(0, 1)
        df['operational_issues'] = (df['grab_closed'] + df['gojek_closed'] + 
                                   df['grab_out_of_stock'] + df['gojek_out_of_stock'] + 
                                   df['grab_busy'] + df['gojek_busy'])
        
        return df
        
    def _comprehensive_analysis(self, data, restaurant_name):
        """Проводит комплексный анализ"""
        
        analysis = {
            'restaurant_name': restaurant_name,
            'total_days': len(data),
            'date_range': f"{data['stat_date'].min()} - {data['stat_date'].max()}",
            'problems': [],
            'opportunities': [],
            'recommendations': [],
            'key_insights': []
        }
        
        # 1. ОБЩАЯ ПРОИЗВОДИТЕЛЬНОСТЬ
        avg_sales = data['total_sales'].mean()
        avg_orders = data['total_orders'].mean()
        avg_aov = data['avg_order_value'].mean()
        
        analysis['avg_daily_sales'] = avg_sales
        analysis['avg_daily_orders'] = avg_orders
        analysis['avg_order_value'] = avg_aov
        
        print(f"📊 СРЕДНИЕ ПОКАЗАТЕЛИ:")
        print(f"   • Продажи в день: {avg_sales:,.0f} IDR")
        print(f"   • Заказы в день: {avg_orders:.0f}")
        print(f"   • Средний чек: {avg_aov:,.0f} IDR")
        
        # 2. АНАЛИЗ ТРЕНДОВ
        recent_data = data.head(7)  # Последние 7 дней
        older_data = data.tail(7)   # Первые 7 дней из периода
        
        recent_avg = recent_data['total_sales'].mean()
        older_avg = older_data['total_sales'].mean()
        
        if recent_avg < older_avg * 0.9:  # Падение больше 10%
            trend_change = ((recent_avg - older_avg) / older_avg) * 100
            analysis['problems'].append(f"📉 Негативный тренд: продажи упали на {abs(trend_change):.1f}%")
            analysis['recommendations'].append("🔍 Срочно выявить причины снижения продаж")
            
        elif recent_avg > older_avg * 1.1:  # Рост больше 10%
            trend_change = ((recent_avg - older_avg) / older_avg) * 100
            analysis['opportunities'].append(f"📈 Позитивный тренд: рост продаж на {trend_change:.1f}%")
            
        # 3. АНАЛИЗ ДНЯ НЕДЕЛИ
        weekday_performance = data.groupby('day_of_week')['total_sales'].mean()
        weekdays = ['Воскресенье', 'Понедельник', 'Вторник', 'Среда', 'Четверг', 'Пятница', 'Суббота']
        
        best_day = weekday_performance.idxmax()
        worst_day = weekday_performance.idxmin()
        
        best_sales = weekday_performance[best_day]
        worst_sales = weekday_performance[worst_day]
        
        analysis['key_insights'].append(f"🏆 Лучший день: {weekdays[best_day]} ({best_sales:,.0f} IDR)")
        analysis['key_insights'].append(f"📉 Слабый день: {weekdays[worst_day]} ({worst_sales:,.0f} IDR)")
        
        if (best_sales - worst_sales) / best_sales > 0.3:  # Разница больше 30%
            analysis['recommendations'].append(f"📅 Усилить маркетинг в {weekdays[worst_day]}")
            
        # 4. ОПЕРАЦИОННЫЕ ПРОБЛЕМЫ
        problem_days = data[data['operational_issues'] > 0]
        
        if len(problem_days) > 0:
            problem_rate = (len(problem_days) / len(data)) * 100
            analysis['problems'].append(f"🚨 Операционные проблемы в {problem_rate:.0f}% дней")
            
            # Детализируем проблемы
            if problem_days['grab_closed'].sum() > 0:
                analysis['problems'].append("🚨 Ресторан закрывался на Grab")
                analysis['recommendations'].append("🔧 Проверить стабильность интеграции с Grab")
                
            if problem_days['gojek_closed'].sum() > 0:
                analysis['problems'].append("🚨 Ресторан закрывался на Gojek")
                analysis['recommendations'].append("🔧 Проверить стабильность интеграции с Gojek")
                
            if problem_days['grab_out_of_stock'].sum() > 0 or problem_days['gojek_out_of_stock'].sum() > 0:
                analysis['problems'].append("📦 Проблемы с наличием товара")
                analysis['recommendations'].append("📦 Улучшить планирование запасов")
                
        # 5. АНАЛИЗ МАРКЕТИНГА
        marketing_data = data[data['total_ads'] > 0]
        
        if len(marketing_data) > 0:
            avg_roas = marketing_data['roas'].mean()
            analysis['avg_roas'] = avg_roas
            
            if avg_roas < 2.0:
                analysis['problems'].append(f"📱 Низкая эффективность рекламы: ROAS {avg_roas:.1f}")
                analysis['recommendations'].append("🎯 Оптимизировать рекламные кампании")
            elif avg_roas > 4.0:
                analysis['opportunities'].append(f"📱 Отличная реклама: ROAS {avg_roas:.1f}")
                analysis['recommendations'].append("💰 Увеличить рекламный бюджет")
                
        else:
            analysis['problems'].append("📱 Отсутствует реклама")
            analysis['recommendations'].append("🚀 Запустить рекламные кампании")
            
        # 6. АНАЛИЗ КАЧЕСТВА
        avg_rating = data['rating'].mean()
        analysis['avg_rating'] = avg_rating
        
        if avg_rating < 4.0:
            analysis['problems'].append(f"⭐ Низкий рейтинг: {avg_rating:.1f}")
            analysis['recommendations'].append("⭐ Улучшить качество еды и сервиса")
        elif avg_rating > 4.5:
            analysis['opportunities'].append(f"⭐ Отличный рейтинг: {avg_rating:.1f}")
            
        # 7. АНАЛИЗ ОТМЕН
        avg_cancel_rate = data['cancel_rate'].mean()
        
        if avg_cancel_rate > 10:
            analysis['problems'].append(f"❌ Высокий процент отмен: {avg_cancel_rate:.1f}%")
            analysis['recommendations'].append("⚡ Улучшить скорость обслуживания")
            
        # 8. ПОИСК ХУДШИХ ДНЕЙ
        worst_days = data.nsmallest(3, 'total_sales')
        analysis['worst_days'] = []
        
        for _, day in worst_days.iterrows():
            day_analysis = self._analyze_specific_day(day, data)
            analysis['worst_days'].append(day_analysis)
            
        return analysis
        
    def _analyze_specific_day(self, day_data, all_data):
        """Анализирует конкретный день"""
        
        date = day_data['stat_date']
        sales = day_data['total_sales']
        avg_sales = all_data['total_sales'].mean()
        
        day_analysis = {
            'date': date,
            'sales': sales,
            'drop_percent': ((sales - avg_sales) / avg_sales) * 100,
            'reasons': []
        }
        
        # Проверяем конкретные причины
        if day_data['operational_issues'] > 0:
            if day_data['grab_closed'] > 0:
                day_analysis['reasons'].append("Ресторан был закрыт на Grab")
            if day_data['gojek_closed'] > 0:
                day_analysis['reasons'].append("Ресторан был закрыт на Gojek")
            if day_data['grab_out_of_stock'] > 0:
                day_analysis['reasons'].append("Нет товара на Grab")
            if day_data['gojek_out_of_stock'] > 0:
                day_analysis['reasons'].append("Нет товара на Gojek")
                
        # Проверяем маркетинг
        avg_ads = all_data['total_ads'].mean()
        if day_data['total_ads'] < avg_ads * 0.5:
            day_analysis['reasons'].append("Низкий бюджет рекламы")
            
        # Проверяем день недели
        weekdays = ['Воскресенье', 'Понедельник', 'Вторник', 'Среда', 'Четверг', 'Пятница', 'Суббота']
        day_name = weekdays[day_data['day_of_week']]
        
        if day_data['day_of_week'] in [0, 1]:  # Вс, Пн
            day_analysis['reasons'].append(f"Слабый день недели ({day_name})")
            
        # Проверяем отмены
        if day_data['cancel_rate'] > 15:
            day_analysis['reasons'].append(f"Много отмен ({day_data['cancel_rate']:.1f}%)")
            
        # Проверяем рейтинг
        if day_data['rating'] < 4.0:
            day_analysis['reasons'].append(f"Низкий рейтинг ({day_data['rating']:.1f})")
            
        if not day_analysis['reasons']:
            day_analysis['reasons'].append("Внешние факторы (погода, конкуренты, события)")
            
        return day_analysis
        
    def _generate_executive_report(self, analysis, restaurant_name):
        """Генерирует исполнительный отчет для клиента"""
        
        report = []
        
        # ЗАГОЛОВОК
        report.append(f"📊 ИСПОЛНИТЕЛЬНЫЙ ОТЧЕТ: '{restaurant_name}'")
        report.append("=" * 70)
        report.append(f"📅 Период: {analysis['date_range']} ({analysis['total_days']} дней)")
        report.append("")
        
        # КЛЮЧЕВЫЕ ПОКАЗАТЕЛИ
        report.append("💰 КЛЮЧЕВЫЕ ПОКАЗАТЕЛИ:")
        report.append(f"   • Средние продажи в день: {analysis['avg_daily_sales']:,.0f} IDR")
        report.append(f"   • Средний чек: {analysis['avg_order_value']:,.0f} IDR")
        report.append(f"   • Заказов в день: {analysis['avg_daily_orders']:.0f}")
        
        if 'avg_roas' in analysis:
            report.append(f"   • ROAS (эффективность рекламы): {analysis['avg_roas']:.1f}")
        if 'avg_rating' in analysis:
            report.append(f"   • Средний рейтинг: {analysis['avg_rating']:.1f}/5.0")
        report.append("")
        
        # ПРОБЛЕМЫ
        if analysis['problems']:
            report.append("🚨 ВЫЯВЛЕННЫЕ ПРОБЛЕМЫ:")
            for i, problem in enumerate(analysis['problems'], 1):
                report.append(f"   {i}. {problem}")
            report.append("")
            
        # ВОЗМОЖНОСТИ
        if analysis['opportunities']:
            report.append("🚀 ВОЗМОЖНОСТИ ДЛЯ РОСТА:")
            for i, opportunity in enumerate(analysis['opportunities'], 1):
                report.append(f"   {i}. {opportunity}")
            report.append("")
            
        # КЛЮЧЕВЫЕ ИНСАЙТЫ
        if analysis['key_insights']:
            report.append("💡 КЛЮЧЕВЫЕ ИНСАЙТЫ:")
            for insight in analysis['key_insights']:
                report.append(f"   • {insight}")
            report.append("")
            
        # ХУДШИЕ ДНИ
        if analysis['worst_days']:
            report.append("📉 АНАЛИЗ ХУДШИХ ДНЕЙ:")
            for day in analysis['worst_days']:
                report.append(f"   {day['date']}: {day['sales']:,.0f} IDR ({day['drop_percent']:+.1f}%)")
                for reason in day['reasons'][:2]:  # Топ-2 причины
                    report.append(f"      • {reason}")
            report.append("")
            
        # РЕКОМЕНДАЦИИ
        if analysis['recommendations']:
            report.append("🎯 ПРИОРИТЕТНЫЕ РЕКОМЕНДАЦИИ:")
            unique_recommendations = list(set(analysis['recommendations']))
            for i, rec in enumerate(unique_recommendations[:8], 1):
                report.append(f"   {i}. {rec}")
            report.append("")
            
        # ИТОГОВАЯ ОЦЕНКА
        problem_count = len(analysis['problems'])
        opportunity_count = len(analysis['opportunities'])
        
        if problem_count > opportunity_count:
            status = "ТРЕБУЕТ ВНИМАНИЯ"
            color = "🔴"
        elif opportunity_count > problem_count:
            status = "ХОРОШИЕ ПЕРСПЕКТИВЫ"  
            color = "🟢"
        else:
            status = "СТАБИЛЬНОЕ СОСТОЯНИЕ"
            color = "🟡"
            
        report.append(f"{color} ОБЩАЯ ОЦЕНКА: {status}")
        report.append(f"   • Выявлено проблем: {problem_count}")
        report.append(f"   • Найдено возможностей: {opportunity_count}")
        
        return "\n".join(report)

def main():
    """Демонстрация умного анализатора"""
    
    analyzer = SmartRestaurantAnalyzer()
    
    # Анализируем Only Eggs
    result = analyzer.analyze_restaurant_detailed("Only Eggs", 60)
    
    print("\n" + "="*80)
    print("📋 ГОТОВЫЙ ОТЧЕТ ДЛЯ КЛИЕНТА:")
    print("="*80)
    print(result)
    print("="*80)

if __name__ == "__main__":
    main()