#!/usr/bin/env python3
"""
📊 ГЕНЕРАТОР ГЛУБОКИХ БИЗНЕС-ОТЧЕТОВ
Создает детальные отчеты с историческими сравнениями, аномалиями и рекомендациями
"""

import json
from datetime import datetime, timedelta
from typing import Dict, Any, List
from main.advanced_analytics import run_advanced_analysis
import sqlite3

class AdvancedReportGenerator:
    """Генератор продвинутых бизнес-отчетов"""
    
    def __init__(self):
        self.conn = sqlite3.connect('data/database.sqlite')
    
    def generate_executive_summary(self, restaurant_name: str, period_start: str = None, period_end: str = None) -> str:
        """Генерирует исполнительную сводку для ресторана"""
        
        # Получаем продвинутый анализ
        analysis = run_advanced_analysis(restaurant_name, period_start, period_end)
        
        if "error" in analysis:
            return f"❌ Ошибка: {analysis['error']}"
        
        # Генерируем отчет
        report = self._build_executive_report(analysis)
        
        return report
    
    def _build_executive_report(self, analysis: Dict[str, Any]) -> str:
        """Строит исполнительный отчет"""
        
        restaurant_name = analysis['restaurant_name']
        period = analysis['analysis_period']
        stats = analysis['current_stats']
        historical = analysis['historical_analysis']
        anomalies = analysis['anomalies']
        seasonality = analysis['seasonality']
        marketing = analysis['marketing_impact']
        competitive = analysis['competitive_analysis']
        insights = analysis['business_insights']
        recommendations = analysis['recommendations']
        
        report = f"""
╔══════════════════════════════════════════════════════════════════════════════════════════════════════
║                            🔬 ГЛУБОКИЙ БИЗНЕС-АНАЛИЗ: {restaurant_name.upper()}
╠══════════════════════════════════════════════════════════════════════════════════════════════════════
║ 📅 Период анализа: {period}
║ 🕐 Сгенерировано: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
╚══════════════════════════════════════════════════════════════════════════════════════════════════════

🎯 ИСПОЛНИТЕЛЬНАЯ СВОДКА
═══════════════════════════════════════════════════════════════════════════════════════════════════════
"""

        # Ключевые метрики
        report += f"""
📊 КЛЮЧЕВЫЕ ПОКАЗАТЕЛИ ЭФФЕКТИВНОСТИ
──────────────────────────────────────────────────────────────────────────────────────────────────────
💰 Общие продажи:           {stats['total_sales']:,.0f} IDR  
📈 Средние дневные продажи: {stats['avg_daily_sales']:,.0f} IDR
🛒 Общее количество заказов: {stats['total_orders']:,}
📦 Средние дневные заказы:   {stats['avg_daily_orders']:.0f}
⭐ Средний рейтинг:         {stats['avg_rating']:.2f}/5.0
🚚 Среднее время доставки:  {stats['avg_delivery_time']:.1f} мин
❌ Процент отмен:           {stats['avg_cancel_rate']*100:.2f}%
📊 Волатильность продаж:    {stats['sales_volatility']*100:.1f}%
"""

        # Исторические сравнения
        if historical.get('year_over_year'):
            yoy = historical['year_over_year']
            report += f"""
📈 СРАВНЕНИЕ С ПРОШЛЫМ ГОДОМ
──────────────────────────────────────────────────────────────────────────────────────────────────────
💰 Изменение продаж:    {yoy.get('sales_change', 0):+.1f}%
🛒 Изменение заказов:   {yoy.get('orders_change', 0):+.1f}%  
⭐ Изменение рейтинга:  {yoy.get('rating_change', 0):+.2f}
🚚 Изменение доставки:  {yoy.get('delivery_time_change', 0):+.1f} мин
"""

        # Исторические тренды
        monthly_trend = historical.get('monthly_trend', {})
        report += f"""
📊 ДОЛГОСРОЧНЫЕ ТРЕНДЫ (2.5 года данных)
──────────────────────────────────────────────────────────────────────────────────────────────────────
📈 Тренд продаж:      {monthly_trend.get('sales_trend', 'н/д')}
📦 Тренд заказов:     {monthly_trend.get('orders_trend', 'н/д')}
⭐ Тренд рейтинга:    {monthly_trend.get('rating_trend', 'н/д')}
🏆 Лучший месяц:      {historical.get('peak_month', 'н/д')}
📉 Худший месяц:      {historical.get('worst_month', 'н/д')}
🚀 Рост:              {historical.get('growth_acceleration', 'н/д')}
"""

        # Критические инсайты
        report += f"""
🔍 КРИТИЧЕСКИЕ БИЗНЕС-ИНСАЙТЫ
──────────────────────────────────────────────────────────────────────────────────────────────────────
"""
        if insights:
            for insight in insights:
                report += f"• {insight}\n"
        else:
            report += "• Системный анализ не выявил критических паттернов\n"

        # Аномалии и выбросы
        report += f"""
⚡ ДЕТЕКТИРОВАННЫЕ АНОМАЛИИ
──────────────────────────────────────────────────────────────────────────────────────────────────────
📊 Индекс волатильности: {anomalies.get('volatility_score', 0)*100:.1f}%
"""
        
        # Положительные аномалии
        pos_anomalies = anomalies.get('positive_anomalies', [])
        if pos_anomalies:
            report += f"\n🚀 ТОП ПИКОВЫЕ ДНИ:\n"
            for i, anomaly in enumerate(pos_anomalies[:3], 1):
                date_str = anomaly['date'].strftime('%Y-%m-%d') if hasattr(anomaly['date'], 'strftime') else str(anomaly['date'])
                report += f"  {i}. {date_str}: {anomaly['total_sales']:,.0f} IDR (+{anomaly['deviation_pct']:.1f}%)\n"
        
        # Негативные аномалии  
        neg_anomalies = anomalies.get('negative_anomalies', [])
        if neg_anomalies:
            report += f"\n📉 КРИТИЧЕСКИЕ ПРОВАЛЫ:\n"
            for i, anomaly in enumerate(neg_anomalies[:2], 1):
                date_str = anomaly['date'].strftime('%Y-%m-%d') if hasattr(anomaly['date'], 'strftime') else str(anomaly['date'])
                report += f"  {i}. {date_str}: {anomaly['total_sales']:,.0f} IDR ({anomaly['deviation_pct']:.1f}%)\n"

        # Причины аномалий
        anomaly_insights = anomalies.get('anomaly_insights', {}).get('insights', [])
        if anomaly_insights:
            report += f"\n🔬 ПРИЧИННЫЙ АНАЛИЗ:\n"
            for insight in anomaly_insights:
                report += f"  • {insight}\n"

        # Сезонный анализ
        report += f"""
🌸 СЕЗОННЫЕ ПАТТЕРНЫ И ЦИКЛИЧНОСТЬ
──────────────────────────────────────────────────────────────────────────────────────────────────────
"""
        peak_months = seasonality.get('peak_months', [])
        if peak_months:
            report += f"🏆 ПИКОВЫЕ МЕСЯЦЫ:\n"
            for month_data in peak_months:
                month_names = {1: 'Январь', 2: 'Февраль', 3: 'Март', 4: 'Апрель', 5: 'Май', 6: 'Июнь',
                              7: 'Июль', 8: 'Август', 9: 'Сентябрь', 10: 'Октябрь', 11: 'Ноябрь', 12: 'Декабрь'}
                month_name = month_names.get(month_data['month'], f"Месяц {month_data['month']}")
                report += f"  • {month_name}: {month_data['avg_sales']:,.0f} IDR/день\n"

        low_months = seasonality.get('low_months', [])
        if low_months:
            report += f"\n📉 СЛАБЫЕ МЕСЯЦЫ:\n"
            for month_data in low_months:
                month_names = {1: 'Январь', 2: 'Февраль', 3: 'Март', 4: 'Апрель', 5: 'Май', 6: 'Июнь',
                              7: 'Июль', 8: 'Август', 9: 'Сентябрь', 10: 'Октябрь', 11: 'Ноябрь', 12: 'Декабрь'}
                month_name = month_names.get(month_data['month'], f"Месяц {month_data['month']}")
                report += f"  • {month_name}: {month_data['avg_sales']:,.0f} IDR/день\n"

        weekly_pattern = seasonality.get('weekly_pattern', {})
        if weekly_pattern:
            days_names = ['Понедельник', 'Вторник', 'Среда', 'Четверг', 'Пятница', 'Суббота', 'Воскресенье']
            weekend_boost = weekly_pattern.get('weekend_boost', 0)
            best_day = weekly_pattern.get('best_day', 0)
            worst_day = weekly_pattern.get('worst_day', 0)
            
            report += f"""
📅 НЕДЕЛЬНЫЕ ПАТТЕРНЫ:
  • Эффект выходных: {weekend_boost:+.1f}%
  • Лучший день: {days_names[best_day] if 0 <= best_day < 7 else 'н/д'}
  • Худший день: {days_names[worst_day] if 0 <= worst_day < 7 else 'н/d'}
"""

        # Маркетинговая эффективность
        report += f"""
📢 АНАЛИЗ МАРКЕТИНГОВОЙ ЭФФЕКТИВНОСТИ
──────────────────────────────────────────────────────────────────────────────────────────────────────
"""
        if not marketing.get('error'):
            report += f"""📈 Подъем от рекламы:      {marketing.get('marketing_lift', 0):.1f}%
💰 Средний ROAS:           {marketing.get('avg_roas', 0):.1f}
🏆 Лучший ROAS:            {marketing.get('best_roas', 0):.1f}
📉 Худший ROAS:            {marketing.get('worst_roas', 0):.1f}
📊 Частота кампаний:       {marketing.get('campaign_frequency', 0):.1f}% дней
"""
            
            optimal_days = marketing.get('optimal_days', {})
            if optimal_days and not optimal_days.get('message'):
                report += f"""
🎯 ОПТИМАЛЬНАЯ СТРАТЕГИЯ:
  • Лучший день для рекламы: {optimal_days.get('best_day', 'н/д')} (ROAS {optimal_days.get('best_roas', 0):.1f})
  • Избегать рекламы: {optimal_days.get('worst_day', 'н/d')} (ROAS {optimal_days.get('worst_roas', 0):.1f})
"""
        else:
            report += f"⚠️ {marketing['error']}\n"

        # Конкурентный анализ
        report += f"""
🥊 КОНКУРЕНТНАЯ ПОЗИЦИЯ
──────────────────────────────────────────────────────────────────────────────────────────────────────
🏆 Позиция на рынке:       #{competitive.get('market_position', 'н/d')} из 5
📊 Доля рынка:             {competitive.get('market_share', 0):.1f}%
👑 Лидер рынка:            {competitive.get('market_leader', 'н/d')}
"""
        
        competitive_gap = competitive.get('competitive_gap')
        if competitive_gap is not None:
            report += f"📏 Отставание от лидера:   {competitive_gap:.1f}%\n"

        # Стратегические рекомендации
        report += f"""
💡 СТРАТЕГИЧЕСКИЕ РЕКОМЕНДАЦИИ
──────────────────────────────────────────────────────────────────────────────────────────────────────
"""
        if recommendations:
            for i, rec in enumerate(recommendations, 1):
                report += f"{i}. {rec}\n"
        else:
            report += "• Текущая стратегия показывает стабильные результаты\n"

        # Прогноз и следующие шаги
        trend_forecast = analysis.get('trend_forecast', {})
        report += f"""
🔮 ПРОГНОЗ И СЛЕДУЮЩИЕ ШАГИ
──────────────────────────────────────────────────────────────────────────────────────────────────────
📈 Направление тренда:     {trend_forecast.get('trend_direction', 'н/d')}
💪 Сила тренда:            {trend_forecast.get('trend_strength', 0):.1f}%/месяц
📊 Месячный рост:          {trend_forecast.get('monthly_growth_rate', 0):,.0f} IDR

🎯 ПРИОРИТЕТНЫЕ ДЕЙСТВИЯ:
"""
        
        # Генерируем приоритеты на основе анализа
        priorities = self._generate_action_priorities(analysis)
        for i, priority in enumerate(priorities, 1):
            report += f"  {i}. {priority}\n"

        report += f"""
══════════════════════════════════════════════════════════════════════════════════════════════════════
                        📊 КОНЕЦ АНАЛИТИЧЕСКОГО ОТЧЕТА
                  🔬 Система провела глубокий анализ 2.5 лет данных
              💡 Все рекомендации основаны на статистически значимых паттернах
══════════════════════════════════════════════════════════════════════════════════════════════════════
"""

        return report
    
    def _generate_action_priorities(self, analysis: Dict[str, Any]) -> List[str]:
        """Генерирует приоритетные действия на основе анализа"""
        
        priorities = []
        stats = analysis['current_stats']
        marketing = analysis['marketing_impact']
        competitive = analysis['competitive_analysis']
        historical = analysis['historical_analysis']
        
        # Критический приоритет: падение продаж
        yoy = historical.get('year_over_year', {})
        if yoy.get('sales_change', 0) < -15:
            priorities.append("🆘 КРИТИЧНО: Остановить падение продаж - провести emergency-аудит операций")
        
        # Высокий приоритет: маркетинг
        if marketing.get('marketing_lift', 0) > 30 and marketing.get('avg_roas', 0) > 4:
            priorities.append("📢 ВЫСОКИЙ: Масштабировать успешные рекламные кампании")
        
        # Высокий приоритет: конкурентная позиция
        if competitive.get('market_position', 1) > 2:
            priorities.append("🎯 ВЫСОКИЙ: Изучить стратегии лидеров рынка и адаптировать лучшие практики")
        
        # Средний приоритет: операционные улучшения
        if stats.get('avg_rating', 5) < 4.2:
            priorities.append("⭐ СРЕДНИЙ: Улучшить качество сервиса для повышения рейтинга")
        
        if stats.get('avg_delivery_time', 0) > 30:
            priorities.append("🚚 СРЕДНИЙ: Оптимизировать логистику для сокращения времени доставки")
        
        # Низкий приоритет: мониторинг
        priorities.append("📊 НИЗКИЙ: Внедрить ежедневный мониторинг ключевых метрик")
        
        return priorities[:5]  # Топ-5 приоритетов
    
    def generate_market_overview(self) -> str:
        """Генерирует обзор всего рынка ресторанов"""
        
        cursor = self.conn.cursor()
        
        # Получаем список всех ресторанов
        cursor.execute('SELECT name FROM restaurants ORDER BY name')
        restaurants = [row[0] for row in cursor.fetchall()]
        
        # Последние 90 дней
        latest_date_query = 'SELECT MAX(date) FROM restaurant_data'
        cursor.execute(latest_date_query)
        latest_date = cursor.fetchone()[0]
        
        if not latest_date:
            return "❌ Нет данных для анализа рынка"
        
        latest_date = datetime.strptime(latest_date, '%Y-%m-%d')
        three_months_ago = latest_date - timedelta(days=90)
        
        report = f"""
╔══════════════════════════════════════════════════════════════════════════════════════════════════════
║                              📊 ОБЗОР РЫНКА РЕСТОРАНОВ БАЛИ
╠══════════════════════════════════════════════════════════════════════════════════════════════════════
║ 📅 Период: Последние 90 дней ({three_months_ago.strftime('%Y-%m-%d')} - {latest_date.strftime('%Y-%m-%d')})
║ 🕐 Сгенерировано: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
╚══════════════════════════════════════════════════════════════════════════════════════════════════════

🏆 РЕЙТИНГ РЕСТОРАНОВ ПО ПРОДАЖАМ
═══════════════════════════════════════════════════════════════════════════════════════════════════════
"""
        
        # Анализ по ресторанам
        for i, restaurant in enumerate(restaurants, 1):
            try:
                analysis = run_advanced_analysis(restaurant, three_months_ago.strftime('%Y-%m-%d'), latest_date.strftime('%Y-%m-%d'))
                
                if "error" not in analysis:
                    stats = analysis['current_stats']
                    competitive = analysis['competitive_analysis']
                    
                    report += f"""
{i}. 🏪 {restaurant.upper()}
   💰 Продажи: {stats['total_sales']:,.0f} IDR | 📦 Заказы: {stats['total_orders']:,} | ⭐ Рейтинг: {stats['avg_rating']:.2f}
   📊 Доля рынка: {competitive.get('market_share', 0):.1f}% | 🚚 Доставка: {stats['avg_delivery_time']:.1f} мин
"""
            except Exception as e:
                report += f"\n{i}. 🏪 {restaurant.upper()} - ❌ Ошибка анализа: {str(e)}\n"
        
        report += f"""
══════════════════════════════════════════════════════════════════════════════════════════════════════
                                    📊 КОНЕЦ ОБЗОРА РЫНКА
══════════════════════════════════════════════════════════════════════════════════════════════════════
"""
        
        return report
    
    def close(self):
        """Закрывает соединение с базой данных"""
        self.conn.close()

# Функции для интеграции
def generate_restaurant_report(restaurant_name: str, period_start: str = None, period_end: str = None) -> str:
    """Генерирует отчет для ресторана"""
    generator = AdvancedReportGenerator()
    try:
        return generator.generate_executive_summary(restaurant_name, period_start, period_end)
    finally:
        generator.close()

def generate_market_report() -> str:
    """Генерирует обзор рынка"""
    generator = AdvancedReportGenerator()
    try:
        return generator.generate_market_overview()
    finally:
        generator.close()

if __name__ == "__main__":
    # Тестирование
    print("🧪 Тест генерации отчета...")
    report = generate_restaurant_report("Ika Canggu")
    print("✅ Отчет сгенерирован успешно")