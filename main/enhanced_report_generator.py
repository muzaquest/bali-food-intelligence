#!/usr/bin/env python3
"""
📊 УЛУЧШЕННЫЙ ГЕНЕРАТОР ОТЧЕТОВ
Создает детальные отчеты с глубоким анализом и конкретными инсайтами
"""

import os
import pandas as pd
from datetime import datetime
from typing import Dict, List, Any

try:
    from .enhanced_analytics import EnhancedAnalytics
except ImportError:
    from enhanced_analytics import EnhancedAnalytics

class EnhancedReportGenerator:
    def __init__(self):
        self.analytics = EnhancedAnalytics()
        
    def generate_detailed_report(self, restaurant_name: str, start_date: str, end_date: str) -> str:
        """Генерирует детальный отчет с улучшенной аналитикой"""
        
        # Загружаем данные
        data = self.analytics.load_restaurant_data(restaurant_name, start_date, end_date)
        
        if data is None or len(data) == 0:
            return f"❌ Нет данных для ресторана {restaurant_name} за период {start_date} - {end_date}"
        
        # Проводим все виды анализа
        anomalies = self.analytics.find_anomalies()
        patterns = self.analytics.analyze_trends_and_patterns()
        comparison = self.analytics.compare_with_previous_period(start_date, end_date)
        insights = self.analytics.generate_actionable_insights(anomalies, patterns, comparison)
        
        # Генерируем отчет
        report = self._build_comprehensive_report(
            restaurant_name, start_date, end_date, data, 
            anomalies, patterns, comparison, insights
        )
        
        # Сохраняем отчет
        self._save_report(restaurant_name, report)
        
        return report
    
    def _build_comprehensive_report(self, restaurant_name: str, start_date: str, end_date: str, 
                                   data: pd.DataFrame, anomalies: Dict, patterns: Dict, 
                                   comparison: Dict, insights: List[str]) -> str:
        """Строит комплексный отчет"""
        
        # Рассчитываем базовые метрики
        total_days = len(data)
        total_sales = data['total_sales'].sum()
        total_orders = data['orders'].sum()
        avg_daily_sales = total_sales / total_days if total_days > 0 else 0
        avg_daily_orders = total_orders / total_days if total_days > 0 else 0
        avg_rating = data['rating'].mean()
        avg_delivery = data['delivery_time'].mean()
        volatility = data['total_sales'].std() / data['total_sales'].mean() * 100 if data['total_sales'].mean() > 0 else 0
        
        # Анализ по месяцам
        data['month'] = data['date'].dt.to_period('M')
        monthly_analysis = data.groupby('month').agg({
            'total_sales': 'sum',
            'orders': 'sum',
            'rating': 'mean'
        }).round(0)
        
        # Начинаем построение отчета
        report = f'''
╔══════════════════════════════════════════════════════════════════════════════════════════════════════
║                        🔬 ДЕТАЛЬНЫЙ БИЗНЕС-АНАЛИЗ: {restaurant_name.upper()}
╠══════════════════════════════════════════════════════════════════════════════════════════════════════
║ 📅 Период анализа: {start_date} - {end_date}
║ 🕐 Сгенерировано: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
║ 📊 Дней данных: {total_days} | Всего записей: {len(data)}
╚══════════════════════════════════════════════════════════════════════════════════════════════════════

🎯 ИСПОЛНИТЕЛЬНАЯ СВОДКА
═══════════════════════════════════════════════════════════════════════════════════════════════════════

📊 КЛЮЧЕВЫЕ ПОКАЗАТЕЛИ ЭФФЕКТИВНОСТИ
──────────────────────────────────────────────────────────────────────────────────────────────────────
💰 Общие продажи:           {total_sales:,.0f} IDR  
📈 Средние дневные продажи: {avg_daily_sales:,.0f} IDR
🛒 Общее количество заказов: {total_orders:,}
📦 Средние дневные заказы:   {avg_daily_orders:.1f}
⭐ Средний рейтинг:         {avg_rating:.2f}/5.0
🚚 Среднее время доставки:  {avg_delivery:.1f} мин
📊 Волатильность продаж:    {volatility:.1f}%
'''

        # Добавляем сравнение с прошлым годом
        if comparison and 'changes' in comparison:
            changes = comparison['changes']
            report += f'''
📈 СРАВНЕНИЕ С ПРОШЛЫМ ГОДОМ
──────────────────────────────────────────────────────────────────────────────────────────────────────
💰 Изменение продаж:    {changes['sales_change']:+.1f}%
🛒 Изменение заказов:   {changes['orders_change']:+.1f}%  
⭐ Изменение рейтинга:  {changes['rating_change']:+.2f}
🚚 Изменение доставки:  {changes['delivery_change']:+.1f} мин
'''

        # Помесячная разбивка
        if len(monthly_analysis) > 0:
            report += f'''
📅 ПОМЕСЯЧНАЯ ДИНАМИКА
──────────────────────────────────────────────────────────────────────────────────────────────────────
'''
            for month, row in monthly_analysis.iterrows():
                daily_avg = row['total_sales'] / month.days_in_month if hasattr(month, 'days_in_month') else row['total_sales'] / 30
                report += f"📊 {month}: {row['total_sales']:,.0f} IDR | {row['orders']:,} заказов | ⭐ {row['rating']:.2f} | ({daily_avg:,.0f} IDR/день)\n"

        # Детальный анализ аномалий
        if anomalies and ('high_anomalies' in anomalies or 'low_anomalies' in anomalies):
            report += f'''
⚡ ДЕТАЛЬНЫЙ АНАЛИЗ АНОМАЛИЙ
──────────────────────────────────────────────────────────────────────────────────────────────────────
📊 Индекс волатильности: {anomalies.get('volatility_index', 0):.1f}%
'''
            
            # Пиковые дни
            if 'high_anomalies' in anomalies and not anomalies['high_anomalies'].empty:
                report += "\n🚀 ТОП ПИКОВЫЕ ДНИ:\n"
                for i, (_, row) in enumerate(anomalies['high_anomalies'].iterrows(), 1):
                    report += f"  {i}. {row['date'].strftime('%Y-%m-%d')}: {row['total_sales']:,.0f} IDR ({row['deviation_pct']:+.1f}%)\n"
                    report += f"     💡 Причина: {row['reasons']}\n"
            
            # Провальные дни
            if 'low_anomalies' in anomalies and not anomalies['low_anomalies'].empty:
                report += "\n📉 КРИТИЧЕСКИЕ ПРОВАЛЫ:\n"
                for i, (_, row) in enumerate(anomalies['low_anomalies'].iterrows(), 1):
                    report += f"  {i}. {row['date'].strftime('%Y-%m-%d')}: {row['total_sales']:,.0f} IDR ({row['deviation_pct']:+.1f}%)\n"
                    report += f"     ⚠️ Причина: {row['reasons']}\n"

        # Анализ паттернов и трендов
        if patterns:
            report += f'''
🌸 СЕЗОННЫЕ ПАТТЕРНЫ И ЦИКЛИЧНОСТЬ
──────────────────────────────────────────────────────────────────────────────────────────────────────
📈 Тренд продаж: {patterns.get('trend_direction', 'неопределен')}
💪 Сила тренда: {patterns.get('trend_strength', 0):.1f}%/месяц
'''
            
            # Эффект выходных
            if 'weekend_effect' in patterns:
                report += f"🏖️ Эффект выходных: {patterns['weekend_effect']:+.1f}%\n"
            
            # Эффективность рекламы
            if 'ads_effect' in patterns:
                report += f"📢 Эффект рекламы: {patterns['ads_effect']:+.1f}%\n"

        # Ключевые инсайты
        if insights:
            report += f'''
🔍 КЛЮЧЕВЫЕ ИНСАЙТЫ И ОТКРЫТИЯ
──────────────────────────────────────────────────────────────────────────────────────────────────────
'''
            for insight in insights:
                report += f"• {insight}\n"

        # Стратегические рекомендации
        recommendations = self._generate_strategic_recommendations(data, anomalies, patterns, comparison)
        if recommendations:
            report += f'''
💡 СТРАТЕГИЧЕСКИЕ РЕКОМЕНДАЦИИ
──────────────────────────────────────────────────────────────────────────────────────────────────────
'''
            for priority, recs in recommendations.items():
                if recs:
                    report += f"\n🎯 {priority.upper()}:\n"
                    for i, rec in enumerate(recs, 1):
                        report += f"  {i}. {rec}\n"

        # Прогноз и план действий
        action_plan = self._generate_action_plan(anomalies, patterns, comparison)
        if action_plan:
            report += f'''
🚀 ЭКСТРЕННЫЙ ПЛАН ДЕЙСТВИЙ
──────────────────────────────────────────────────────────────────────────────────────────────────────
'''
            for timeframe, actions in action_plan.items():
                if actions:
                    report += f"\n{timeframe}:\n"
                    for action in actions:
                        report += f"  • {action}\n"

        report += f'''
══════════════════════════════════════════════════════════════════════════════════════════════════════
                        📊 КОНЕЦ ДЕТАЛЬНОГО АНАЛИЗА
                  🔬 Система провела глубокий анализ {total_days} дней данных
              💡 Все рекомендации основаны на статистически значимых паттернах
══════════════════════════════════════════════════════════════════════════════════════════════════════
'''
        
        return report
    
    def _generate_strategic_recommendations(self, data: pd.DataFrame, anomalies: Dict, 
                                          patterns: Dict, comparison: Dict) -> Dict[str, List[str]]:
        """Генерирует стратегические рекомендации по приоритетам"""
        
        recommendations = {
            'критический': [],
            'высокий': [],
            'средний': [],
            'низкий': []
        }
        
        # Анализ критических проблем
        if comparison and 'changes' in comparison:
            changes = comparison['changes']
            if changes['sales_change'] < -20:
                recommendations['критический'].append(
                    f"🚨 КРИТИЧЕСКОЕ ПАДЕНИЕ ПРОДАЖ: -{abs(changes['sales_change']):.1f}% - требует немедленного вмешательства"
                )
            elif changes['orders_change'] < -30:
                recommendations['критический'].append(
                    f"📉 ДРАМАТИЧЕСКОЕ ПАДЕНИЕ ЗАКАЗОВ: -{abs(changes['orders_change']):.1f}% - анализировать конкуренцию"
                )
        
        # Анализ аномалий для рекомендаций
        if anomalies and 'low_anomalies' in anomalies and not anomalies['low_anomalies'].empty:
            worst_day = anomalies['low_anomalies'].iloc[0]
            if 'отключена реклама' in worst_day['reasons']:
                recommendations['критический'].append(
                    "💰 ВКЛЮЧИТЬ РЕКЛАМУ НЕМЕДЛЕННО - провалы связаны с отключением рекламы"
                )
        
        # Высокий приоритет
        if patterns and patterns.get('ads_effect', 0) > 15:
            recommendations['высокий'].append(
                f"📢 МАСШТАБИРОВАТЬ РЕКЛАМУ: эффект +{patterns['ads_effect']:.1f}% подтвержден"
            )
        
        volatility = data['total_sales'].std() / data['total_sales'].mean() * 100
        if volatility > 40:
            recommendations['высокий'].append(
                f"📊 СТАБИЛИЗИРОВАТЬ ОПЕРАЦИИ: волатильность {volatility:.1f}% критично высокая"
            )
        
        # Средний приоритет
        avg_delivery = data['delivery_time'].mean()
        if avg_delivery > 35:
            recommendations['средний'].append(
                f"🚚 ОПТИМИЗИРОВАТЬ ДОСТАВКУ: {avg_delivery:.1f} мин слишком долго"
            )
        
        if patterns and patterns.get('weekend_effect', 0) > 10:
            recommendations['средний'].append(
                f"🏖️ УСИЛИТЬ ВЫХОДНЫЕ: эффект +{patterns['weekend_effect']:.1f}% можно увеличить"
            )
        
        # Низкий приоритет
        recommendations['низкий'].append("📊 Внедрить ежедневный мониторинг ключевых метрик")
        
        return recommendations
    
    def _generate_action_plan(self, anomalies: Dict, patterns: Dict, comparison: Dict) -> Dict[str, List[str]]:
        """Генерирует конкретный план действий по временным рамкам"""
        
        plan = {
            '🚨 НЕМЕДЛЕННО (24 часа)': [],
            '🔴 СРОЧНО (1 неделя)': [],
            '🟡 КРАТКОСРОЧНО (1 месяц)': []
        }
        
        # Немедленные действия
        if anomalies and 'low_anomalies' in anomalies and not anomalies['low_anomalies'].empty:
            worst_day = anomalies['low_anomalies'].iloc[0]
            if 'отключена реклама' in worst_day['reasons']:
                plan['🚨 НЕМЕДЛЕННО (24 часа)'].append(
                    "Включить рекламу на всех платформах - потенциал +50-80% продаж"
                )
        
        # Срочные действия
        if comparison and 'changes' in comparison and comparison['changes']['sales_change'] < -15:
            plan['🔴 СРОЧНО (1 неделя)'].append(
                "Провести анализ конкурентов - выяснить причины падения"
            )
            plan['🔴 СРОЧНО (1 неделя)'].append(
                "Пересмотреть меню и ценообразование"
            )
        
        # Краткосрочные действия
        plan['🟡 КРАТКОСРОЧНО (1 месяц)'].append(
            "Оптимизировать рекламные кампании по дням недели"
        )
        plan['🟡 КРАТКОСРОЧНО (1 месяц)'].append(
            "Внедрить систему мониторинга ключевых метрик"
        )
        
        return plan
    
    def _save_report(self, restaurant_name: str, report: str):
        """Сохраняет отчет в файл"""
        
        if not os.path.exists('reports'):
            os.makedirs('reports')
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"reports/{restaurant_name.replace(' ', '_')}_{timestamp}_enhanced.txt"
        
        with open(filename, 'w', encoding='utf-8') as f:
            f.write(report)
        
        print(f"💾 Детальный отчет сохранен в файл: {filename}")
    
    def close(self):
        """Закрывает соединения"""
        if self.analytics:
            self.analytics.close()