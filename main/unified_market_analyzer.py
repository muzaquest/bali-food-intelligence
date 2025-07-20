import pandas as pd
import sqlite3
from datetime import datetime, timedelta
import statistics
from typing import Dict, List, Tuple, Optional
import numpy as np
from main.weather_calendar_api import WeatherCalendarAPI
from main.openai_analytics import OpenAIAnalytics

class UnifiedMarketAnalyzer:
    """
    Унифицированный анализатор рынка - генерирует полный отчет по всему рынку
    включая аномалии, погодные факторы, праздники, тренды и ИИ-рекомендации
    """
    
    def __init__(self, db_path: str = "data/database.sqlite"):
        self.db_path = db_path
        self.weather_api = WeatherCalendarAPI()
        self.openai_analytics = OpenAIAnalytics()
    
    def generate_full_market_report(self, start_date: str, end_date: str) -> str:
        """
        Генерирует полный отчет по рынку с глубокой аналитикой
        
        Args:
            start_date: Дата начала в формате YYYY-MM-DD
            end_date: Дата окончания в формате YYYY-MM-DD
        
        Returns:
            Полный текстовый отчет с анализом
        """
        
        print(f"🔬 Генерация ПОЛНОГО рыночного анализа")
        print(f"📅 Период: {start_date} → {end_date}")
        
        try:
            # Загружаем данные
            market_data = self._load_market_data(start_date, end_date)
            
            if market_data.empty:
                return "❌ Нет данных для анализа рынка за указанный период"
            
            # Генерируем все секции отчета
            report_sections = []
            
            # 1. Исполнительная сводка
            report_sections.append(self._generate_executive_summary(market_data, start_date, end_date))
            
            # 2. Анализ аномалий
            report_sections.append(self._generate_anomaly_analysis(market_data, start_date, end_date))
            
            # 3. Анализ внешних факторов (погода + праздники)
            report_sections.append(self._generate_external_factors_analysis(market_data, start_date, end_date))
            
            # 4. Сегментный анализ
            report_sections.append(self._generate_segment_analysis(market_data))
            
            # 5. Конкурентный анализ
            report_sections.append(self._generate_competitive_analysis(market_data))
            
            # 6. Тренды и прогнозы
            report_sections.append(self._generate_trends_analysis(market_data, start_date, end_date))
            
            # 7. ИИ-рекомендации
            report_sections.append(self._generate_ai_recommendations(market_data, start_date, end_date))
            
            # 8. Детальная статистика
            report_sections.append(self._generate_detailed_statistics(market_data))
            
            # Объединяем в финальный отчет
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            full_report = f"""
╔══════════════════════════════════════════════════════════════════════════════════════════════════════
║                              🏢 ПОЛНЫЙ АНАЛИЗ РЫНКА РЕСТОРАНОВ БАЛИ
╠══════════════════════════════════════════════════════════════════════════════════════════════════════
║ 📅 Период анализа: {start_date} → {end_date}
║ 🏪 Ресторанов в анализе: {len(market_data['restaurant_name'].unique())}
║ 📊 Общих записей: {len(market_data):,}
║ 🕐 Отчет сгенерирован: {timestamp}
╚══════════════════════════════════════════════════════════════════════════════════════════════════════

{"".join(report_sections)}

╔══════════════════════════════════════════════════════════════════════════════════════════════════════
║                                    🎯 КОНЕЦ ПОЛНОГО АНАЛИЗА РЫНКА
╚══════════════════════════════════════════════════════════════════════════════════════════════════════
"""
            
            return full_report
            
        except Exception as e:
            return f"❌ Ошибка при генерации полного рыночного отчета: {str(e)}"
    
    def _load_market_data(self, start_date: str, end_date: str) -> pd.DataFrame:
        """Загружает данные по всему рынку за период"""
        
        conn = sqlite3.connect(self.db_path)
        
        query = """
        SELECT 
            restaurant_name,
            date,
            platform,
            total_sales,
            orders,
            rating,
            delivery_time
        FROM restaurant_data 
        WHERE date BETWEEN ? AND ?
        ORDER BY date, restaurant_name, platform
        """
        
        df = pd.read_sql_query(query, conn, params=(start_date, end_date))
        conn.close()
        
        # Конвертируем date в datetime
        df['date'] = pd.to_datetime(df['date'])
        
        # Агрегируем данные по ресторанам и дням (объединяем Grab и Gojek)
        aggregated = df.groupby(['restaurant_name', 'date']).agg({
            'total_sales': 'sum',
            'orders': 'sum', 
            'rating': 'mean',
            'delivery_time': 'mean'
        }).reset_index()
        
        # Переименовываем колонки для совместимости
        aggregated = aggregated.rename(columns={
            'orders': 'total_orders',
            'rating': 'avg_rating',
            'delivery_time': 'avg_delivery_time'
        })
        
        print(f"✅ Загружены данные: {len(df)} записей, агрегированы в {len(aggregated)} записей за {start_date} → {end_date}")
        
        return aggregated
    
    def _generate_executive_summary(self, data: pd.DataFrame, start_date: str, end_date: str) -> str:
        """Генерирует исполнительную сводку рынка"""
        
        # Агрегируем по дням для рыночных трендов
        daily_market = data.groupby('date').agg({
            'total_sales': 'sum',
            'total_orders': 'sum',
            'avg_rating': 'mean',
            'avg_delivery_time': 'mean'
        }).reset_index()
        
        # Основные метрики
        total_sales = data['total_sales'].sum()
        total_orders = data['total_orders'].sum()
        avg_daily_sales = daily_market['total_sales'].mean()
        avg_daily_orders = daily_market['total_orders'].mean()
        market_avg_rating = data['avg_rating'].mean()
        market_avg_delivery = data['avg_delivery_time'].mean()
        
        # Количество активных ресторанов
        active_restaurants = len(data['restaurant_name'].unique())
        days_analyzed = len(daily_market)
        
        # Тренды (первая vs вторая половина периода)
        mid_point = len(daily_market) // 2
        first_half = daily_market.iloc[:mid_point]
        second_half = daily_market.iloc[mid_point:]
        
        sales_change = ((second_half['total_sales'].mean() / first_half['total_sales'].mean()) - 1) * 100
        orders_change = ((second_half['total_orders'].mean() / first_half['total_orders'].mean()) - 1) * 100
        rating_change = second_half['avg_rating'].mean() - first_half['avg_rating'].mean()
        
        sales_trend = "📈" if sales_change > 0 else "📉"
        orders_trend = "📈" if orders_change > 0 else "📉"
        rating_trend = "⭐" if rating_change > 0 else "⭐"
        
        # Топ и худшие дни
        best_day = daily_market.loc[daily_market['total_sales'].idxmax()]
        worst_day = daily_market.loc[daily_market['total_sales'].idxmin()]
        
        return f"""

🎯 ИСПОЛНИТЕЛЬНАЯ СВОДКА РЫНКА
═══════════════════════════════════════════════════════════════════════════════

📊 КЛЮЧЕВЫЕ РЫНОЧНЫЕ ПОКАЗАТЕЛИ
───────────────────────────────────────────────────────────────────────────────
💰 Общий оборот рынка:      {total_sales:,.0f} IDR {sales_trend}
📈 Среднедневный оборот:    {avg_daily_sales:,.0f} IDR
🛒 Общее количество заказов: {total_orders:,.0f} {orders_trend}
📦 Среднедневные заказы:    {avg_daily_orders:,.0f}
⭐ Средний рейтинг рынка:   {market_avg_rating:.2f}/5.0 {rating_trend}
🚚 Среднее время доставки: {market_avg_delivery:.1f} мин
🏪 Активных ресторанов:     {active_restaurants}
📅 Дней в анализе:          {days_analyzed}

📊 ДИНАМИКА РЫНКА
───────────────────────────────────────────────────────────────────────────────
💹 Изменение продаж:        {sales_change:+.1f}% (2-я половина vs 1-я)
📈 Изменение заказов:       {orders_change:+.1f}% (2-я половина vs 1-я)
⭐ Изменение рейтинга:      {rating_change:+.2f} балла

🏆 ЭКСТРЕМАЛЬНЫЕ ДНИ
───────────────────────────────────────────────────────────────────────────────
🥇 Лучший день:  {best_day['date'].strftime('%Y-%m-%d')} - {best_day['total_sales']:,.0f} IDR
🔴 Худший день: {worst_day['date'].strftime('%Y-%m-%d')} - {worst_day['total_sales']:,.0f} IDR
"""

    def _generate_anomaly_analysis(self, data: pd.DataFrame, start_date: str, end_date: str) -> str:
        """Анализирует аномалии в рыночных данных"""
        
        # Агрегируем по дням
        daily_market = data.groupby('date').agg({
            'total_sales': 'sum',
            'total_orders': 'sum',
            'avg_rating': 'mean'
        }).reset_index()
        
        # Находим аномалии в продажах (отклонения > 2 стандартных отклонений)
        sales_mean = daily_market['total_sales'].mean()
        sales_std = daily_market['total_sales'].std()
        
        # Аномалии продаж
        sales_anomalies = daily_market[
            (daily_market['total_sales'] < sales_mean - 2*sales_std) |
            (daily_market['total_sales'] > sales_mean + 2*sales_std)
        ].copy()
        
        # Аномалии заказов
        orders_mean = daily_market['total_orders'].mean()
        orders_std = daily_market['total_orders'].std()
        
        orders_anomalies = daily_market[
            (daily_market['total_orders'] < orders_mean - 2*orders_std) |
            (daily_market['total_orders'] > orders_mean + 2*orders_std)
        ].copy()
        
        # Дни недели анализ
        daily_market['weekday'] = daily_market['date'].dt.day_name()
        weekly_patterns = daily_market.groupby('weekday')['total_sales'].mean().sort_values(ascending=False)
        
        # Аномальные рестораны (сильные отклонения от средних показателей рынка)
        restaurant_stats = data.groupby('restaurant_name').agg({
            'total_sales': 'mean',
            'total_orders': 'mean',
            'avg_rating': 'mean'
        }).reset_index()
        
        # Находим сильных и слабых игроков
        market_median_sales = restaurant_stats['total_sales'].median()
        top_performers = restaurant_stats[restaurant_stats['total_sales'] > market_median_sales * 3].head(5)
        underperformers = restaurant_stats[restaurant_stats['total_sales'] < market_median_sales * 0.3].head(5)
        
        anomaly_text = f"""

🔍 АНАЛИЗ АНОМАЛИЙ И ПАТТЕРНОВ
═══════════════════════════════════════════════════════════════════════════════

📉 АНОМАЛЬНЫЕ ДНИ ПО ПРОДАЖАМ
───────────────────────────────────────────────────────────────────────────────
Критерий: отклонение > 2σ от среднего ({sales_mean:,.0f} IDR ± {sales_std:,.0f})

"""
        
        if len(sales_anomalies) > 0:
            for _, anomaly in sales_anomalies.head(5).iterrows():
                deviation = ((anomaly['total_sales'] - sales_mean) / sales_std)
                anomaly_type = "📈 Пик" if deviation > 0 else "📉 Спад"
                anomaly_text += f"{anomaly_type}: {anomaly['date'].strftime('%Y-%m-%d')} - {anomaly['total_sales']:,.0f} IDR (отклонение: {deviation:.1f}σ)\n"
        else:
            anomaly_text += "✅ Значительных аномалий в продажах не обнаружено\n"
            
        anomaly_text += f"""
📦 АНОМАЛЬНЫЕ ДНИ ПО ЗАКАЗАМ  
───────────────────────────────────────────────────────────────────────────────
Критерий: отклонение > 2σ от среднего ({orders_mean:,.0f} заказов ± {orders_std:,.0f})

"""
        
        if len(orders_anomalies) > 0:
            for _, anomaly in orders_anomalies.head(5).iterrows():
                deviation = ((anomaly['total_orders'] - orders_mean) / orders_std)
                anomaly_type = "📈 Пик" if deviation > 0 else "📉 Спад"
                anomaly_text += f"{anomaly_type}: {anomaly['date'].strftime('%Y-%m-%d')} - {anomaly['total_orders']:,.0f} заказов (отклонение: {deviation:.1f}σ)\n"
        else:
            anomaly_text += "✅ Значительных аномалий в заказах не обнаружено\n"
            
        anomaly_text += f"""
📅 ПАТТЕРНЫ ПО ДНЯМ НЕДЕЛИ
───────────────────────────────────────────────────────────────────────────────
"""
        for day, avg_sales in weekly_patterns.head(7).items():
            anomaly_text += f"{day}: {avg_sales:,.0f} IDR в среднем\n"
            
        anomaly_text += f"""
🏆 ТОП-ИСПОЛНИТЕЛИ (выше {market_median_sales*3:,.0f} IDR/день)
───────────────────────────────────────────────────────────────────────────────
"""
        for _, restaurant in top_performers.iterrows():
            anomaly_text += f"🥇 {restaurant['restaurant_name']}: {restaurant['total_sales']:,.0f} IDR/день в среднем\n"
            
        anomaly_text += f"""
⚠️ ТРЕБУЮТ ВНИМАНИЯ (ниже {market_median_sales*0.3:,.0f} IDR/день)
───────────────────────────────────────────────────────────────────────────────
"""
        for _, restaurant in underperformers.iterrows():
            anomaly_text += f"🔴 {restaurant['restaurant_name']}: {restaurant['total_sales']:,.0f} IDR/день в среднем\n"
        
        return anomaly_text

    def _generate_external_factors_analysis(self, data: pd.DataFrame, start_date: str, end_date: str) -> str:
        """Анализирует влияние внешних факторов (погода, праздники)"""
        
        try:
            # Получаем данные о погоде
            weather_data = self.weather_api.get_historical_weather(start_date, end_date)
            
            # Получаем данные о праздниках  
            holidays_data = self.weather_api.get_holidays(start_date, end_date)
            
            # Агрегируем продажи по дням
            daily_sales = data.groupby('date')['total_sales'].sum().reset_index()
            daily_sales['date_str'] = daily_sales['date'].dt.strftime('%Y-%m-%d')
            
            external_text = f"""

🌍 АНАЛИЗ ВНЕШНИХ ФАКТОРОВ
═══════════════════════════════════════════════════════════════════════════════

🌦️ ВЛИЯНИЕ ПОГОДЫ
───────────────────────────────────────────────────────────────────────────────
"""
            
            if weather_data:
                # Анализируем корреляцию с погодой
                rainy_days = []
                hot_days = []
                
                for weather in weather_data:
                    date_str = weather['date']
                    temp = weather.get('temperature', 0)
                    precipitation = weather.get('precipitation', 0)
                    
                    # Находим продажи в этот день
                    day_sales = daily_sales[daily_sales['date_str'] == date_str]
                    if not day_sales.empty:
                        sales = day_sales['total_sales'].iloc[0]
                        
                        if precipitation > 5:  # Дождливый день
                            rainy_days.append({'date': date_str, 'sales': sales, 'rain': precipitation})
                        if temp > 32:  # Жаркий день
                            hot_days.append({'date': date_str, 'sales': sales, 'temp': temp})
                
                # Анализ дождливых дней
                if rainy_days:
                    avg_rainy_sales = sum([day['sales'] for day in rainy_days]) / len(rainy_days)
                    avg_normal_sales = daily_sales['total_sales'].mean()
                    rain_impact = ((avg_rainy_sales / avg_normal_sales) - 1) * 100
                    
                    external_text += f"☔ Дождливые дни ({len(rainy_days)} дней с осадками >5мм):\n"
                    external_text += f"   Средние продажи: {avg_rainy_sales:,.0f} IDR vs {avg_normal_sales:,.0f} IDR в обычные дни\n"
                    external_text += f"   Влияние дождя: {rain_impact:+.1f}% к продажам\n\n"
                    
                    # Худшие дождливые дни
                    worst_rainy = sorted(rainy_days, key=lambda x: x['sales'])[:3]
                    external_text += "🌧️ Самые сложные дождливые дни:\n"
                    for day in worst_rainy:
                        external_text += f"   {day['date']}: {day['sales']:,.0f} IDR (осадки: {day['rain']:.1f}мм)\n"
                
                # Анализ жарких дней
                if hot_days:
                    avg_hot_sales = sum([day['sales'] for day in hot_days]) / len(hot_days)
                    avg_normal_sales = daily_sales['total_sales'].mean()
                    heat_impact = ((avg_hot_sales / avg_normal_sales) - 1) * 100
                    
                    external_text += f"\n🌡️ Жаркие дни ({len(hot_days)} дней с температурой >32°C):\n"
                    external_text += f"   Средние продажи: {avg_hot_sales:,.0f} IDR vs {avg_normal_sales:,.0f} IDR в обычные дни\n"
                    external_text += f"   Влияние жары: {heat_impact:+.1f}% к продажам\n"
                
            else:
                external_text += "⚠️ Данные о погоде недоступны\n"
            
            external_text += f"""
🎉 ВЛИЯНИЕ ПРАЗДНИКОВ
───────────────────────────────────────────────────────────────────────────────
"""
            
            if holidays_data:
                holiday_impact = []
                
                for holiday in holidays_data:
                    holiday_date = holiday['date']
                    holiday_name = holiday['name']
                    
                    # Находим продажи в праздничный день
                    day_sales = daily_sales[daily_sales['date_str'] == holiday_date]
                    if not day_sales.empty:
                        sales = day_sales['total_sales'].iloc[0]
                        avg_sales = daily_sales['total_sales'].mean()
                        impact = ((sales / avg_sales) - 1) * 100
                        
                        holiday_impact.append({
                            'date': holiday_date,
                            'name': holiday_name,
                            'sales': sales,
                            'impact': impact
                        })
                
                if holiday_impact:
                    # Сортируем по влиянию
                    holiday_impact.sort(key=lambda x: x['impact'], reverse=True)
                    
                    external_text += f"📊 Анализ {len(holiday_impact)} праздничных дней:\n\n"
                    
                    for holiday in holiday_impact:
                        impact_icon = "📈" if holiday['impact'] > 0 else "📉"
                        external_text += f"{impact_icon} {holiday['name']} ({holiday['date']}):\n"
                        external_text += f"   Продажи: {holiday['sales']:,.0f} IDR ({holiday['impact']:+.1f}% к среднему)\n\n"
                else:
                    external_text += "ℹ️ Праздничные дни в анализируемом периоде не влияли значительно на продажи\n"
            else:
                external_text += "⚠️ Данные о праздниках недоступны\n"
            
            return external_text
            
        except Exception as e:
            return f"""

🌍 АНАЛИЗ ВНЕШНИХ ФАКТОРОВ
═══════════════════════════════════════════════════════════════════════════════
⚠️ Ошибка при анализе внешних факторов: {str(e)}
"""

    def _generate_segment_analysis(self, data: pd.DataFrame) -> str:
        """Анализирует рынок по сегментам/категориям"""
        
        # Простая категоризация по названиям ресторанов
        segments = {
            'Pizza': ['PIZZA', 'SLICE'],
            'Sushi': ['SUSHI', 'NINJA'],
            'Healthy': ['HEALTHY', 'FIT', 'PROTEIN', 'PLANT'],
            'Burgers': ['BURGER', 'SMASH'],
            'Asian': ['IKA', 'DODO', 'TEAMO'],
            'Eggs': ['EGG', 'ONLY EGGS'],
            'Premium': ['PRANA', 'BALAGAN', 'SOUL KITCHEN']
        }
        
        segment_stats = {}
        
        for segment_name, keywords in segments.items():
            segment_restaurants = []
            
            for _, row in data.iterrows():
                restaurant_name = row['restaurant_name'].upper()
                if any(keyword in restaurant_name for keyword in keywords):
                    segment_restaurants.append(row)
            
            if segment_restaurants:
                segment_df = pd.DataFrame(segment_restaurants)
                
                segment_stats[segment_name] = {
                    'restaurants_count': len(segment_df['restaurant_name'].unique()),
                    'total_sales': segment_df['total_sales'].sum(),
                    'total_orders': segment_df['total_orders'].sum(),
                    'avg_rating': segment_df['avg_rating'].mean(),
                    'avg_delivery_time': segment_df['avg_delivery_time'].mean(),
                    'avg_order_value': segment_df['total_sales'].sum() / segment_df['total_orders'].sum() if segment_df['total_orders'].sum() > 0 else 0
                }
        
        # Сортируем сегменты по продажам
        sorted_segments = sorted(segment_stats.items(), key=lambda x: x[1]['total_sales'], reverse=True)
        
        segment_text = f"""

🍽️ СЕГМЕНТНЫЙ АНАЛИЗ РЫНКА
═══════════════════════════════════════════════════════════════════════════════

📊 АНАЛИЗ ПО КАТЕГОРИЯМ КУХНИ
───────────────────────────────────────────────────────────────────────────────
"""
        
        total_market_sales = data['total_sales'].sum()
        
        for segment_name, stats in sorted_segments:
            market_share = (stats['total_sales'] / total_market_sales) * 100
            
            segment_text += f"""
🏷️ {segment_name.upper()}
   🏪 Ресторанов: {stats['restaurants_count']}
   💰 Продажи: {stats['total_sales']:,.0f} IDR ({market_share:.1f}% рынка)
   📦 Заказы: {stats['total_orders']:,.0f}
   💸 Средний чек: {stats['avg_order_value']:,.0f} IDR
   ⭐ Рейтинг: {stats['avg_rating']:.2f}/5.0
   🚚 Доставка: {stats['avg_delivery_time']:.1f} мин
"""
        
        # Анализ лидеров и аутсайдеров
        if sorted_segments:
            leader = sorted_segments[0]
            outsider = sorted_segments[-1] if len(sorted_segments) > 1 else None
            
            segment_text += f"""
🏆 СЕГМЕНТНЫЕ ИНСАЙТЫ
───────────────────────────────────────────────────────────────────────────────
🥇 Лидирующий сегмент: {leader[0]} ({leader[1]['total_sales']/total_market_sales*100:.1f}% рынка)
   → Высокий спрос, стабильная категория для инвестиций
"""
            
            if outsider:
                segment_text += f"""🔴 Нишевый сегмент: {outsider[0]} ({outsider[1]['total_sales']/total_market_sales*100:.1f}% рынка)
   → Возможности для роста или специализации
"""
        
        return segment_text

    def _generate_competitive_analysis(self, data: pd.DataFrame) -> str:
        """Анализирует конкурентную среду"""
        
        # Анализ по ресторанам
        restaurant_stats = data.groupby('restaurant_name').agg({
            'total_sales': ['sum', 'mean'],
            'total_orders': ['sum', 'mean'], 
            'avg_rating': 'mean',
            'avg_delivery_time': 'mean'
        }).round(2)
        
        # Упрощаем названия колонок
        restaurant_stats.columns = ['total_sales', 'avg_daily_sales', 'total_orders', 'avg_daily_orders', 'avg_rating', 'avg_delivery_time']
        restaurant_stats = restaurant_stats.reset_index()
        
        # Рассчитываем средний чек
        restaurant_stats['avg_order_value'] = restaurant_stats['total_sales'] / restaurant_stats['total_orders']
        restaurant_stats['avg_order_value'] = restaurant_stats['avg_order_value'].fillna(0)
        
        # Топ и худшие исполнители
        top_by_sales = restaurant_stats.nlargest(5, 'total_sales')
        bottom_by_sales = restaurant_stats.nsmallest(5, 'total_sales')
        top_by_rating = restaurant_stats.nlargest(5, 'avg_rating')
        bottom_by_rating = restaurant_stats.nsmallest(5, 'avg_rating')
        
        # Анализ концентрации рынка
        total_market_sales = restaurant_stats['total_sales'].sum()
        top_3_share = top_by_sales.head(3)['total_sales'].sum() / total_market_sales * 100
        top_10_share = top_by_sales.head(10)['total_sales'].sum() / total_market_sales * 100
        
        competitive_text = f"""

🏁 КОНКУРЕНТНЫЙ АНАЛИЗ
═══════════════════════════════════════════════════════════════════════════════

📊 КОНЦЕНТРАЦИЯ РЫНКА
───────────────────────────────────────────────────────────────────────────────
🥇 ТОП-3 ресторана контролируют: {top_3_share:.1f}% рынка
🏆 ТОП-10 ресторанов контролируют: {top_10_share:.1f}% рынка
📈 Уровень конкуренции: {"Высокий (фрагментированный рынок)" if top_3_share < 30 else "Средний (умеренная концентрация)" if top_3_share < 50 else "Низкий (концентрированный рынок)"}

🏆 ТОП-5 ПО ПРОДАЖАМ
───────────────────────────────────────────────────────────────────────────────
"""
        
        for i, (_, restaurant) in enumerate(top_by_sales.iterrows(), 1):
            market_share = (restaurant['total_sales'] / total_market_sales) * 100
            competitive_text += f"{i}. {restaurant['restaurant_name']}\n"
            competitive_text += f"   💰 {restaurant['total_sales']:,.0f} IDR ({market_share:.1f}% рынка)\n"
            competitive_text += f"   📦 {restaurant['total_orders']:,.0f} заказов | 💸 {restaurant['avg_order_value']:,.0f} IDR/заказ\n"
            competitive_text += f"   ⭐ {restaurant['avg_rating']:.2f}/5.0 | 🚚 {restaurant['avg_delivery_time']:.1f} мин\n\n"
        
        competitive_text += f"""⭐ ТОП-5 ПО КАЧЕСТВУ (рейтинг)
───────────────────────────────────────────────────────────────────────────────
"""
        
        for i, (_, restaurant) in enumerate(top_by_rating.iterrows(), 1):
            competitive_text += f"{i}. {restaurant['restaurant_name']}: ⭐ {restaurant['avg_rating']:.2f}/5.0\n"
        
        competitive_text += f"""
⚠️ ТРЕБУЮТ УЛУЧШЕНИЯ (низкие рейтинги)
───────────────────────────────────────────────────────────────────────────────
"""
        
        for i, (_, restaurant) in enumerate(bottom_by_rating.iterrows(), 1):
            competitive_text += f"{i}. {restaurant['restaurant_name']}: ⭐ {restaurant['avg_rating']:.2f}/5.0 (риск оттока клиентов)\n"
        
        competitive_text += f"""
🔴 ОТСТАЮЩИЕ ПО ПРОДАЖАМ
───────────────────────────────────────────────────────────────────────────────
"""
        
        for i, (_, restaurant) in enumerate(bottom_by_sales.iterrows(), 1):
            competitive_text += f"{i}. {restaurant['restaurant_name']}: {restaurant['total_sales']:,.0f} IDR (нужна поддержка)\n"
        
        return competitive_text

    def _generate_trends_analysis(self, data: pd.DataFrame, start_date: str, end_date: str) -> str:
        """Анализирует тренды и строит прогнозы"""
        
        # Агрегируем по дням
        daily_trends = data.groupby('date').agg({
            'total_sales': 'sum',
            'total_orders': 'sum',
            'avg_rating': 'mean',
            'avg_delivery_time': 'mean'
        }).reset_index()
        
        # Добавляем день недели и номер недели
        daily_trends['weekday'] = daily_trends['date'].dt.day_name()
        daily_trends['week_number'] = daily_trends['date'].dt.isocalendar().week
        
        # Анализ трендов по неделям
        weekly_trends = daily_trends.groupby('week_number').agg({
            'total_sales': 'mean',
            'total_orders': 'mean',
            'avg_rating': 'mean'
        }).reset_index()
        
        # Рассчитываем тренд (простая линейная регрессия)
        weeks = list(range(len(weekly_trends)))
        sales_trend_slope = np.polyfit(weeks, weekly_trends['total_sales'], 1)[0] if len(weeks) > 1 else 0
        orders_trend_slope = np.polyfit(weeks, weekly_trends['total_orders'], 1)[0] if len(weeks) > 1 else 0
        
        # Прогноз на следующую неделю
        if len(weekly_trends) > 0:
            last_week_sales = weekly_trends['total_sales'].iloc[-1]
            predicted_sales = last_week_sales + sales_trend_slope
            sales_change_percent = (sales_trend_slope / last_week_sales) * 100 if last_week_sales > 0 else 0
        else:
            predicted_sales = 0
            sales_change_percent = 0
        
        # Анализ сезонности по дням недели
        weekday_stats = daily_trends.groupby('weekday')['total_sales'].agg(['mean', 'std']).reset_index()
        weekday_stats = weekday_stats.sort_values('mean', ascending=False)
        
        trends_text = f"""

📈 АНАЛИЗ ТРЕНДОВ И ПРОГНОЗЫ
═══════════════════════════════════════════════════════════════════════════════

📊 ЕЖЕНЕДЕЛЬНАЯ ДИНАМИКА
───────────────────────────────────────────────────────────────────────────────
💹 Тренд продаж: {sales_trend_slope:+,.0f} IDR/неделю ({sales_change_percent:+.1f}%/неделя)
📦 Тренд заказов: {orders_trend_slope:+,.0f} заказов/неделю
🔮 Прогноз на следующую неделю: {predicted_sales:,.0f} IDR/день в среднем

📅 СЕЗОННОСТЬ ПО ДНЯМ НЕДЕЛИ
───────────────────────────────────────────────────────────────────────────────
"""
        
        for _, day_stat in weekday_stats.iterrows():
            trends_text += f"{day_stat['weekday']}: {day_stat['mean']:,.0f} IDR в среднем (±{day_stat['std']:,.0f})\n"
        
        # Определяем лучшие и худшие дни
        best_day = weekday_stats.iloc[0]
        worst_day = weekday_stats.iloc[-1]
        
        trends_text += f"""
🏆 КЛЮЧЕВЫЕ ИНСАЙТЫ
───────────────────────────────────────────────────────────────────────────────
🥇 Самый сильный день: {best_day['weekday']} ({best_day['mean']:,.0f} IDR в среднем)
🔴 Самый слабый день: {worst_day['weekday']} ({worst_day['mean']:,.0f} IDR в среднем)
📊 Разница: {((best_day['mean'] / worst_day['mean']) - 1) * 100:.1f}% между лучшим и худшим днем

🔮 РЕКОМЕНДАЦИИ ПО ТРЕНДАМ
───────────────────────────────────────────────────────────────────────────────
"""
        
        if sales_trend_slope > 0:
            trends_text += "📈 Положительный тренд - рынок растет, хорошее время для экспансии\n"
        elif sales_trend_slope < 0:
            trends_text += "📉 Отрицательный тренд - нужны меры по стимулированию спроса\n"
        else:
            trends_text += "📊 Стабильный рынок - фокус на операционной эффективности\n"
        
        trends_text += f"🎯 Оптимизировать операции в {worst_day['weekday']} для повышения продаж\n"
        trends_text += f"💪 Использовать успешную модель {best_day['weekday']} для других дней\n"
        
        return trends_text

    def _generate_ai_recommendations(self, data: pd.DataFrame, start_date: str, end_date: str) -> str:
        """Генерирует ИИ-рекомендации на основе анализа"""
        
        try:
            # Подготавливаем данные для ИИ-анализа
            market_summary = {
                'total_sales': data['total_sales'].sum(),
                'total_orders': data['total_orders'].sum(),
                'avg_rating': data['avg_rating'].mean(),
                'restaurants_count': len(data['restaurant_name'].unique()),
                'period': f"{start_date} to {end_date}"
            }
            
            # Топ и худшие исполнители
            restaurant_stats = data.groupby('restaurant_name').agg({
                'total_sales': 'sum',
                'avg_rating': 'mean'
            }).reset_index()
            
            top_performers = restaurant_stats.nlargest(3, 'total_sales')['restaurant_name'].tolist()
            low_performers = restaurant_stats.nsmallest(3, 'total_sales')['restaurant_name'].tolist()
            
            # Данные по дням недели
            daily_trends = data.groupby(data['date'].dt.day_name())['total_sales'].mean().to_dict()
            
            prompt = f"""
Ты - эксперт по аналитике ресторанного бизнеса. Проанализируй данные рынка доставки еды на Бали и дай конкретные рекомендации.

ДАННЫЕ РЫНКА:
- Период: {market_summary['period']}
- Общие продажи: {market_summary['total_sales']:,.0f} IDR
- Общие заказы: {market_summary['total_orders']:,.0f}
- Средний рейтинг: {market_summary['avg_rating']:.2f}/5.0
- Количество ресторанов: {market_summary['restaurants_count']}

ЛИДЕРЫ: {', '.join(top_performers)}
ОТСТАЮЩИЕ: {', '.join(low_performers)}

ПРОДАЖИ ПО ДНЯМ: {daily_trends}

Дай 5-7 конкретных рекомендаций для:
1. Владельцев отстающих ресторанов
2. Общих рыночных стратегий
3. Операционных улучшений
4. Маркетинговых активностей

Ответ должен быть практичным и действенным, на русском языке.
"""
            
            ai_response = self.openai_analytics.get_insights(prompt)
            
            return f"""

🤖 ИИ-РЕКОМЕНДАЦИИ И СТРАТЕГИЧЕСКИЕ ИНСАЙТЫ
═══════════════════════════════════════════════════════════════════════════════

{ai_response}
"""
            
        except Exception as e:
            return f"""

🤖 ИИ-РЕКОМЕНДАЦИИ И СТРАТЕГИЧЕСКИЕ ИНСАЙТЫ
═══════════════════════════════════════════════════════════════════════════════

⚠️ ИИ-анализ временно недоступен: {str(e)}

💡 БАЗОВЫЕ РЕКОМЕНДАЦИИ НА ОСНОВЕ ДАННЫХ:

🎯 ДЛЯ ОТСТАЮЩИХ РЕСТОРАНОВ:
- Проанализировать успешные практики лидеров рынка
- Улучшить качество обслуживания для повышения рейтинга
- Оптимизировать меню и ценообразование
- Усилить маркетинговые активности

📊 ДЛЯ РЫНОЧНЫХ СТРАТЕГИЙ:
- Фокус на дни с низкими продажами для роста
- Развитие категорий с высоким потенциалом
- Улучшение логистики доставки
- Стандартизация качества услуг
"""

    def _generate_detailed_statistics(self, data: pd.DataFrame) -> str:
        """Генерирует детальную статистику"""
        
        # Общая статистика
        total_sales = data['total_sales'].sum()
        total_orders = data['total_orders'].sum()
        avg_order_value = total_sales / total_orders if total_orders > 0 else 0
        
        # Статистика по платформам (данные уже агрегированы)
        # Для простоты покажем общие метрики, так как данные уже объединены
        
        # Распределение по времени доставки
        delivery_stats = data['avg_delivery_time'].describe()
        
        # Статистика по рейтингам
        rating_stats = data['avg_rating'].describe()
        
        return f"""

📊 ДЕТАЛЬНАЯ СТАТИСТИКА РЫНКА
═══════════════════════════════════════════════════════════════════════════════

💰 ФИНАНСОВЫЕ ПОКАЗАТЕЛИ
───────────────────────────────────────────────────────────────────────────────
Общий оборот:           {total_sales:,.0f} IDR
Общее количество заказов: {total_orders:,.0f}
Средний чек:            {avg_order_value:,.0f} IDR

 📱 ОБЩИЕ ПОКАЗАТЕЛИ ПЛАТФОРМ
───────────────────────────────────────────────────────────────────────────────
Объединенные данные Grab + Gojek за период

🚚 СТАТИСТИКА ДОСТАВКИ
───────────────────────────────────────────────────────────────────────────────
Среднее время:          {delivery_stats['mean']:.1f} мин
Медианное время:        {delivery_stats['50%']:.1f} мин  
Быстрейшая доставка:    {delivery_stats['min']:.1f} мин
Самая долгая:           {delivery_stats['max']:.1f} мин

⭐ СТАТИСТИКА РЕЙТИНГОВ
───────────────────────────────────────────────────────────────────────────────
Средний рейтинг:        {rating_stats['mean']:.2f}/5.0
Медианный рейтинг:      {rating_stats['50%']:.2f}/5.0
Лучший рейтинг:         {rating_stats['max']:.2f}/5.0
Худший рейтинг:         {rating_stats['min']:.2f}/5.0

📈 ОПЕРАЦИОННЫЕ МЕТРИКИ
───────────────────────────────────────────────────────────────────────────────
Активных ресторанов:    {len(data['restaurant_name'].unique())}
Дней в анализе:         {len(data['date'].unique())}
Записей данных:         {len(data):,}
Средних записей/ресторан: {len(data)/len(data['restaurant_name'].unique()):.1f}
"""

    def close(self):
        """Закрывает соединения с API"""
        try:
            if hasattr(self.weather_api, 'close'):
                self.weather_api.close()
        except:
            pass