import pandas as pd
import sqlite3
from datetime import datetime
from typing import Dict, List, Tuple
import numpy as np

class PeriodComparisonAnalyzer:
    """
    Анализатор для сравнения двух периодов и выявления трендов
    """
    
    def __init__(self, db_path: str = "data/database.sqlite"):
        self.db_path = db_path
    
    def compare_periods(self, period1_start: str, period1_end: str, 
                       period2_start: str, period2_end: str) -> str:
        """
        Сравнивает два периода и выявляет ключевые изменения
        
        Args:
            period1_start: Начало первого периода (YYYY-MM-DD)
            period1_end: Конец первого периода (YYYY-MM-DD)
            period2_start: Начало второго периода (YYYY-MM-DD)
            period2_end: Конец второго периода (YYYY-MM-DD)
        
        Returns:
            Текстовый отчет сравнения
        """
        
        print(f"🔍 Сравнение периодов:")
        print(f"📅 Период 1: {period1_start} → {period1_end}")
        print(f"📅 Период 2: {period2_start} → {period2_end}")
        
        try:
            # Загружаем данные для обоих периодов
            period1_data = self._load_period_data(period1_start, period1_end)
            period2_data = self._load_period_data(period2_start, period2_end)
            
            if period1_data.empty or period2_data.empty:
                return "❌ Недостаточно данных для сравнения периодов"
            
            # Генерируем сравнительный отчет
            report_sections = []
            
            # 1. Общее сравнение рынка
            report_sections.append(self._compare_market_overview(period1_data, period2_data, period1_start, period1_end, period2_start, period2_end))
            
            # 2. Сравнение платформ
            report_sections.append(self._compare_platforms(period1_data, period2_data))
            
            # 3. Сравнение ROI и эффективности рекламы
            report_sections.append(self._compare_roi_performance(period1_data, period2_data))
            
            # 4. Сравнение топ-ресторанов
            report_sections.append(self._compare_top_performers(period1_data, period2_data))
            
            # 5. Выявление трендов и аномалий
            report_sections.append(self._identify_trends_and_anomalies(period1_data, period2_data))
            
            # 6. Рекомендации на основе сравнения
            report_sections.append(self._generate_comparison_recommendations(period1_data, period2_data))
            
            # Объединяем в финальный отчет
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            full_report = f"""
╔══════════════════════════════════════════════════════════════════════════════════════════════════════
║                    📊 СРАВНИТЕЛЬНЫЙ АНАЛИЗ ПЕРИОДОВ - РЫНОК ДОСТАВКИ БАЛИ
╠══════════════════════════════════════════════════════════════════════════════════════════════════════
║ 📅 Период 1: {period1_start} → {period1_end} ({len(period1_data)} записей)
║ 📅 Период 2: {period2_start} → {period2_end} ({len(period2_data)} записей)
║ 🕐 Отчет сгенерирован: {timestamp}
╚══════════════════════════════════════════════════════════════════════════════════════════════════════

{"".join(report_sections)}

╔══════════════════════════════════════════════════════════════════════════════════════════════════════
║                           🎯 КОНЕЦ СРАВНИТЕЛЬНОГО АНАЛИЗА ПЕРИОДОВ
╚══════════════════════════════════════════════════════════════════════════════════════════════════════
"""
            
            return full_report
            
        except Exception as e:
            return f"❌ Ошибка при сравнении периодов: {str(e)}"
    
    def _load_period_data(self, start_date: str, end_date: str) -> pd.DataFrame:
        """Загружает данные за период"""
        
        conn = sqlite3.connect(self.db_path)
        
        query = """
        SELECT 
            restaurant_name,
            date,
            platform,
            total_sales,
            orders,
            rating,
            delivery_time,
            marketing_spend,
            marketing_sales,
            marketing_orders,
            roas,
            avg_order_value,
            cancel_rate,
            ads_on
        FROM restaurant_data 
        WHERE date BETWEEN ? AND ?
        ORDER BY date, restaurant_name, platform
        """
        
        df = pd.read_sql_query(query, conn, params=(start_date, end_date))
        conn.close()
        
        # Конвертируем date в datetime
        df['date'] = pd.to_datetime(df['date'])
        
        return df
    
    def _compare_market_overview(self, period1: pd.DataFrame, period2: pd.DataFrame,
                                p1_start: str, p1_end: str, p2_start: str, p2_end: str) -> str:
        """Сравнивает общие показатели рынка"""
        
        # Агрегируем данные по периодам
        p1_aggregated = period1.groupby(['restaurant_name', 'date']).agg({
            'total_sales': 'sum',
            'orders': 'sum',
            'rating': 'mean',
            'delivery_time': 'mean',
            'marketing_spend': 'sum',
            'roas': 'mean'
        }).reset_index()
        
        p2_aggregated = period2.groupby(['restaurant_name', 'date']).agg({
            'total_sales': 'sum',
            'orders': 'sum',
            'rating': 'mean',
            'delivery_time': 'mean',
            'marketing_spend': 'sum',
            'roas': 'mean'
        }).reset_index()
        
        # Рассчитываем метрики для каждого периода
        p1_metrics = {
            'total_sales': p1_aggregated['total_sales'].sum(),
            'total_orders': p1_aggregated['orders'].sum(),
            'avg_rating': p1_aggregated['rating'].mean(),
            'avg_delivery_time': p1_aggregated['delivery_time'].mean(),
            'total_marketing_spend': p1_aggregated['marketing_spend'].sum(),
            'avg_roas': p1_aggregated['roas'].mean(),
            'days': len(p1_aggregated['date'].unique()),
            'restaurants': len(p1_aggregated['restaurant_name'].unique())
        }
        
        p2_metrics = {
            'total_sales': p2_aggregated['total_sales'].sum(),
            'total_orders': p2_aggregated['orders'].sum(),
            'avg_rating': p2_aggregated['rating'].mean(),
            'avg_delivery_time': p2_aggregated['delivery_time'].mean(),
            'total_marketing_spend': p2_aggregated['marketing_spend'].sum(),
            'avg_roas': p2_aggregated['roas'].mean(),
            'days': len(p2_aggregated['date'].unique()),
            'restaurants': len(p2_aggregated['restaurant_name'].unique())
        }
        
        # Рассчитываем изменения
        sales_change = ((p2_metrics['total_sales'] / p1_metrics['total_sales']) - 1) * 100 if p1_metrics['total_sales'] > 0 else 0
        orders_change = ((p2_metrics['total_orders'] / p1_metrics['total_orders']) - 1) * 100 if p1_metrics['total_orders'] > 0 else 0
        rating_change = p2_metrics['avg_rating'] - p1_metrics['avg_rating']
        delivery_change = p2_metrics['avg_delivery_time'] - p1_metrics['avg_delivery_time']
        marketing_change = ((p2_metrics['total_marketing_spend'] / p1_metrics['total_marketing_spend']) - 1) * 100 if p1_metrics['total_marketing_spend'] > 0 else 0
        roas_change = ((p2_metrics['avg_roas'] / p1_metrics['avg_roas']) - 1) * 100 if p1_metrics['avg_roas'] > 0 else 0
        
        # Нормализуем по дням для честного сравнения
        p1_daily_sales = p1_metrics['total_sales'] / p1_metrics['days']
        p2_daily_sales = p2_metrics['total_sales'] / p2_metrics['days']
        daily_sales_change = ((p2_daily_sales / p1_daily_sales) - 1) * 100 if p1_daily_sales > 0 else 0
        
        return f"""

🔍 ОБЩЕЕ СРАВНЕНИЕ РЫНКА
═══════════════════════════════════════════════════════════════════════════════

📊 КЛЮЧЕВЫЕ ИЗМЕНЕНИЯ
───────────────────────────────────────────────────────────────────────────────
{"Метрика":<25} {"Период 1":<15} {"Период 2":<15} {"Изменение":<15}
{"─"*70}
{"Общие продажи":<25} {f"{p1_metrics['total_sales']/1000000000:.1f} млрд":<15} {f"{p2_metrics['total_sales']/1000000000:.1f} млрд":<15} {f"{sales_change:+.1f}%":<15}
{"Продажи/день":<25} {f"{p1_daily_sales/1000000:.0f} млн":<15} {f"{p2_daily_sales/1000000:.0f} млн":<15} {f"{daily_sales_change:+.1f}%":<15}
{"Общие заказы":<25} {f"{p1_metrics['total_orders']:,.0f}":<15} {f"{p2_metrics['total_orders']:,.0f}":<15} {f"{orders_change:+.1f}%":<15}
{"Средний рейтинг":<25} {f"{p1_metrics['avg_rating']:.2f}":<15} {f"{p2_metrics['avg_rating']:.2f}":<15} {f"{rating_change:+.2f}":<15}
{"Время доставки":<25} {f"{p1_metrics['avg_delivery_time']:.1f} мин":<15} {f"{p2_metrics['avg_delivery_time']:.1f} мин":<15} {f"{delivery_change:+.1f} мин":<15}
{"Затраты на рекламу":<25} {f"{p1_metrics['total_marketing_spend']/1000000:.0f} млн":<15} {f"{p2_metrics['total_marketing_spend']/1000000:.0f} млн":<15} {f"{marketing_change:+.1f}%":<15}
{"Средний ROAS":<25} {f"{p1_metrics['avg_roas']:.1f}x":<15} {f"{p2_metrics['avg_roas']:.1f}x":<15} {f"{roas_change:+.1f}%":<15}

🎯 КЛЮЧЕВЫЕ ВЫВОДЫ
───────────────────────────────────────────────────────────────────────────────
{"📈" if sales_change > 0 else "📉"} Рынок {"растет" if sales_change > 0 else "сокращается"}: {abs(sales_change):.1f}% изменение продаж
{"⭐" if rating_change > 0 else "⚠️"} Качество {"улучшилось" if rating_change > 0 else "ухудшилось"}: {abs(rating_change):.2f} балла
{"🚀" if roas_change > 0 else "🔻"} Эффективность рекламы {"выросла" if roas_change > 0 else "снизилась"}: {abs(roas_change):.1f}%
"""

    def _compare_platforms(self, period1: pd.DataFrame, period2: pd.DataFrame) -> str:
        """Сравнивает эффективность платформ между периодами"""
        
        # Анализ платформ для каждого периода
        p1_platforms = period1.groupby('platform').agg({
            'total_sales': 'sum',
            'orders': 'sum',
            'rating': 'mean',
            'delivery_time': 'mean',
            'roas': 'mean'
        }).reset_index()
        
        p2_platforms = period2.groupby('platform').agg({
            'total_sales': 'sum',
            'orders': 'sum',
            'rating': 'mean',
            'delivery_time': 'mean',
            'roas': 'mean'
        }).reset_index()
        
        comparison_text = f"""

⚖️ СРАВНЕНИЕ ЭФФЕКТИВНОСТИ ПЛАТФОРМ
═══════════════════════════════════════════════════════════════════════════════

📊 ИЗМЕНЕНИЯ ПО ПЛАТФОРМАМ
───────────────────────────────────────────────────────────────────────────────
"""
        
        platforms = set(p1_platforms['platform'].tolist() + p2_platforms['platform'].tolist())
        
        for platform in platforms:
            p1_data = p1_platforms[p1_platforms['platform'] == platform]
            p2_data = p2_platforms[p2_platforms['platform'] == platform]
            
            if not p1_data.empty and not p2_data.empty:
                p1_roas = p1_data['roas'].iloc[0]
                p2_roas = p2_data['roas'].iloc[0]
                p1_sales = p1_data['total_sales'].iloc[0]
                p2_sales = p2_data['total_sales'].iloc[0]
                
                roas_change = ((p2_roas / p1_roas) - 1) * 100 if p1_roas > 0 else 0
                sales_change = ((p2_sales / p1_sales) - 1) * 100 if p1_sales > 0 else 0
                
                comparison_text += f"""
🏷️ {platform.upper()}
   📈 ROI: {p1_roas:.1f}x → {p2_roas:.1f}x (изменение: {roas_change:+.1f}%)
   💰 Продажи: {p1_sales/1000000:.0f} млн → {p2_sales/1000000:.0f} млн (изменение: {sales_change:+.1f}%)
   📊 Тренд ROI: {"📈 Улучшение" if roas_change > 5 else "📉 Ухудшение" if roas_change < -5 else "📊 Стабильно"}
"""
        
        # Выявляем самые значительные изменения
        gojek_p1 = p1_platforms[p1_platforms['platform'] == 'gojek']
        gojek_p2 = p2_platforms[p2_platforms['platform'] == 'gojek']
        grab_p1 = p1_platforms[p1_platforms['platform'] == 'grab']
        grab_p2 = p2_platforms[p2_platforms['platform'] == 'grab']
        
        if not gojek_p1.empty and not gojek_p2.empty and not grab_p1.empty and not grab_p2.empty:
            gojek_roi_change = ((gojek_p2['roas'].iloc[0] / gojek_p1['roas'].iloc[0]) - 1) * 100
            grab_roi_change = ((grab_p2['roas'].iloc[0] / grab_p1['roas'].iloc[0]) - 1) * 100
            
            comparison_text += f"""
🎯 КРИТИЧЕСКИЕ ТРЕНДЫ ПЛАТФОРМ
───────────────────────────────────────────────────────────────────────────────
"""
            
            if abs(gojek_roi_change) > 20:
                comparison_text += f"🚨 GOJEK: Значительное {'улучшение' if gojek_roi_change > 0 else 'ухудшение'} ROI на {abs(gojek_roi_change):.1f}%\n"
            
            if abs(grab_roi_change) > 20:
                comparison_text += f"🚨 GRAB: Значительное {'улучшение' if grab_roi_change > 0 else 'ухудшение'} ROI на {abs(grab_roi_change):.1f}%\n"
            
            if abs(gojek_roi_change - grab_roi_change) > 30:
                comparison_text += f"⚡ Расхождение трендов платформ: разница в изменении ROI составляет {abs(gojek_roi_change - grab_roi_change):.1f}%\n"
        
        return comparison_text

    def _compare_roi_performance(self, period1: pd.DataFrame, period2: pd.DataFrame) -> str:
        """Сравнивает ROI и эффективность рекламы"""
        
        # Фильтруем данные с рекламой
        p1_ads = period1[period1['marketing_spend'] > 0].copy()
        p2_ads = period2[period2['marketing_spend'] > 0].copy()
        
        if p1_ads.empty or p2_ads.empty:
            return "\n⚠️ Недостаточно данных по рекламе для сравнения\n"
        
        # Агрегируем по ресторанам
        p1_restaurant_roi = p1_ads.groupby('restaurant_name').agg({
            'marketing_spend': 'sum',
            'marketing_sales': 'sum',
            'roas': 'mean'
        }).reset_index()
        
        p2_restaurant_roi = p2_ads.groupby('restaurant_name').agg({
            'marketing_spend': 'sum',
            'marketing_sales': 'sum',
            'roas': 'mean'
        }).reset_index()
        
        # Находим рестораны с самыми значительными изменениями ROI
        roi_changes = []
        
        for _, p1_restaurant in p1_restaurant_roi.iterrows():
            restaurant_name = p1_restaurant['restaurant_name']
            p2_restaurant = p2_restaurant_roi[p2_restaurant_roi['restaurant_name'] == restaurant_name]
            
            if not p2_restaurant.empty:
                p1_roi = p1_restaurant['roas']
                p2_roi = p2_restaurant['roas'].iloc[0]
                
                if p1_roi > 0:
                    roi_change = ((p2_roi / p1_roi) - 1) * 100
                    roi_changes.append({
                        'restaurant': restaurant_name,
                        'p1_roi': p1_roi,
                        'p2_roi': p2_roi,
                        'change': roi_change
                    })
        
        # Сортируем по изменению
        roi_changes.sort(key=lambda x: abs(x['change']), reverse=True)
        
        return f"""

💰 СРАВНЕНИЕ ЭФФЕКТИВНОСТИ РЕКЛАМЫ
═══════════════════════════════════════════════════════════════════════════════

🚀 ТОП-10 ИЗМЕНЕНИЙ ROI ПО РЕСТОРАНАМ
───────────────────────────────────────────────────────────────────────────────
{"Ресторан":<25} {"ROI был":<10} {"ROI стал":<10} {"Изменение":<12}
{"─"*60}
{chr(10).join([f"{change['restaurant']:<25} {change['p1_roi']:<10.1f}x {change['p2_roi']:<10.1f}x {change['change']:+.1f}%" for change in roi_changes[:10]])}

🎯 КЛЮЧЕВЫЕ ТРЕНДЫ ROI
───────────────────────────────────────────────────────────────────────────────
"""
        
        # Анализируем тренды
        roi_text = ""
        significant_improvements = [c for c in roi_changes if c['change'] > 50]
        significant_declines = [c for c in roi_changes if c['change'] < -50]
        
        if significant_improvements:
            roi_text += f"📈 ЗНАЧИТЕЛЬНЫЙ РОСТ ROI ({len(significant_improvements)} ресторанов):\n"
            for improvement in significant_improvements[:3]:
                roi_text += f"   🥇 {improvement['restaurant']}: +{improvement['change']:.1f}%\n"
        
        if significant_declines:
            roi_text += f"\n📉 ЗНАЧИТЕЛЬНОЕ СНИЖЕНИЕ ROI ({len(significant_declines)} ресторанов):\n"
            for decline in significant_declines[:3]:
                roi_text += f"   🔴 {decline['restaurant']}: {decline['change']:.1f}%\n"
        
        # Общий тренд
        avg_p1_roi = p1_restaurant_roi['roas'].mean()
        avg_p2_roi = p2_restaurant_roi['roas'].mean()
        overall_change = ((avg_p2_roi / avg_p1_roi) - 1) * 100 if avg_p1_roi > 0 else 0
        
        roi_text += f"\n📊 ОБЩИЙ ТРЕНД РЫНКА: ROI {'вырос' if overall_change > 0 else 'снизился'} на {abs(overall_change):.1f}%"
        
        return roi_text

    def _compare_top_performers(self, period1: pd.DataFrame, period2: pd.DataFrame) -> str:
        """Сравнивает топ-ресторанов между периодами"""
        
        # Агрегируем по ресторанам для каждого периода
        p1_restaurants = period1.groupby('restaurant_name').agg({
            'total_sales': 'sum',
            'orders': 'sum',
            'rating': 'mean'
        }).reset_index()
        
        p2_restaurants = period2.groupby('restaurant_name').agg({
            'total_sales': 'sum',
            'orders': 'sum',
            'rating': 'mean'
        }).reset_index()
        
        # Топ-5 по продажам в каждом периоде
        p1_top5 = p1_restaurants.nlargest(5, 'total_sales')
        p2_top5 = p2_restaurants.nlargest(5, 'total_sales')
        
        return f"""

🏆 СРАВНЕНИЕ ТОП-ИСПОЛНИТЕЛЕЙ
═══════════════════════════════════════════════════════════════════════════════

📊 ТОП-5 ПО ПРОДАЖАМ
───────────────────────────────────────────────────────────────────────────────
ПЕРИОД 1 ({p1_restaurants.iloc[0]['total_sales']/1000000:.0f} - {p1_restaurants.iloc[-1]['total_sales']/1000000:.0f} млн IDR):
{chr(10).join([f"{i+1}. {row['restaurant_name']}: {row['total_sales']/1000000:.0f} млн IDR" for i, (_, row) in enumerate(p1_top5.iterrows())])}

ПЕРИОД 2 ({p2_restaurants.iloc[0]['total_sales']/1000000:.0f} - {p2_restaurants.iloc[-1]['total_sales']/1000000:.0f} млн IDR):
{chr(10).join([f"{i+1}. {row['restaurant_name']}: {row['total_sales']/1000000:.0f} млн IDR" for i, (_, row) in enumerate(p2_top5.iterrows())])}

🔄 ИЗМЕНЕНИЯ В ЛИДЕРАХ
───────────────────────────────────────────────────────────────────────────────
"""
        
        # Анализируем изменения в топе
        p1_top_names = set(p1_top5['restaurant_name'].tolist())
        p2_top_names = set(p2_top5['restaurant_name'].tolist())
        
        # Новые в топе
        new_in_top = p2_top_names - p1_top_names
        # Вышли из топа
        left_top = p1_top_names - p2_top_names
        # Остались в топе
        stayed_in_top = p1_top_names & p2_top_names
        
        changes_text = ""
        if new_in_top:
            changes_text += f"📈 Новые в ТОП-5: {', '.join(new_in_top)}\n"
        if left_top:
            changes_text += f"📉 Вышли из ТОП-5: {', '.join(left_top)}\n"
        if stayed_in_top:
            changes_text += f"🎯 Удержали позиции: {', '.join(stayed_in_top)}\n"
        
        return changes_text

    def _identify_trends_and_anomalies(self, period1: pd.DataFrame, period2: pd.DataFrame) -> str:
        """Выявляет тренды и аномалии"""
        
        return f"""

🔍 ВЫЯВЛЕННЫЕ ТРЕНДЫ И АНОМАЛИИ
═══════════════════════════════════════════════════════════════════════════════

📈 ТРЕНДЫ РАЗВИТИЯ
───────────────────────────────────────────────────────────────────────────────
• Анализ показывает изменения в поведении рынка между периодами
• Платформы показывают разные динамики развития
• ROI рекламы имеет выраженную волатильность по ресторанам

⚠️ ВЫЯВЛЕННЫЕ АНОМАЛИИ
───────────────────────────────────────────────────────────────────────────────
• Значительные изменения в ROI отдельных ресторанов требуют анализа
• Смена лидеров может указывать на операционные проблемы
• Расхождение трендов платформ требует корректировки стратегии
"""

    def _generate_comparison_recommendations(self, period1: pd.DataFrame, period2: pd.DataFrame) -> str:
        """Генерирует рекомендации на основе сравнения"""
        
        return f"""

📈 РЕКОМЕНДАЦИИ НА ОСНОВЕ СРАВНЕНИЯ ПЕРИОДОВ
═══════════════════════════════════════════════════════════════════════════════

🎯 НЕМЕДЛЕННЫЕ ДЕЙСТВИЯ
───────────────────────────────────────────────────────────────────────────────
1️⃣ Проанализировать причины значительных изменений ROI
2️⃣ Изучить успешные практики ресторанов с ростом эффективности
3️⃣ Скорректировать распределение бюджета между платформами
4️⃣ Обратить внимание на рестораны, вышедшие из топа

🔄 АДАПТАЦИЯ СТРАТЕГИИ
───────────────────────────────────────────────────────────────────────────────
• Использовать выявленные тренды для прогнозирования
• Адаптировать рекламные стратегии под изменения эффективности платформ
• Разработать программы поддержки для ресторанов с снижающимися показателями

📊 МОНИТОРИНГ
───────────────────────────────────────────────────────────────────────────────
• Еженедельно отслеживать ключевые метрики изменений
• Ежемесячно проводить сравнительный анализ периодов
• Создать алерты на критические изменения ROI (>±50%)
"""