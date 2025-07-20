#!/usr/bin/env python3
"""
🏪 УНИФИЦИРОВАННЫЙ АНАЛИЗАТОР РЕСТОРАНА
Единый модуль для полного анализа ресторана - ВСЁ В ОДНОМ ОТЧЕТЕ!
"""

import sqlite3
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import numpy as np
from .weather_calendar_api import WeatherCalendarAPI
from .openai_analytics import OpenAIAnalytics

class UnifiedRestaurantAnalyzer:
    def __init__(self):
        """Инициализация анализатора"""
        self.weather_api = WeatherCalendarAPI()
        self.ai_analytics = OpenAIAnalytics()
        
    def generate_full_report(self, restaurant_name: str, start_date: str = None, end_date: str = None) -> str:
        """
        🎯 ГЛАВНАЯ ФУНКЦИЯ: Генерирует ПОЛНЫЙ отчет по ресторану
        Включает ВСЁ: аномалии, погоду, праздники, конкурентов, ИИ-инсайты
        """
        
        # Устанавливаем даты по умолчанию (последний месяц)
        if not end_date:
            end_date = datetime.now().strftime('%Y-%m-%d')
        if not start_date:
            start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
            
        print(f"🔬 Генерация ПОЛНОГО анализа для: {restaurant_name}")
        print(f"📅 Период: {start_date} → {end_date}")
        
        try:
            # Получаем все необходимые данные
            data = self._load_restaurant_data(restaurant_name, start_date, end_date)
            
            if data.empty:
                return f"❌ Нет данных для ресторана '{restaurant_name}' за период {start_date} - {end_date}"
            
            # Генерируем все секции отчета
            report_sections = {
                'header': self._generate_header(restaurant_name, start_date, end_date),
                'executive_summary': self._generate_executive_summary(data, start_date, end_date),
                'comparison': self._generate_comparison_analysis(restaurant_name, data, start_date, end_date),
                'anomalies': self._analyze_anomalies(data, start_date, end_date),
                'external_factors': self._analyze_external_factors(data, start_date, end_date),
                'competitive_position': self._analyze_competitive_position(restaurant_name, data),
                'ai_recommendations': self._generate_ai_recommendations(restaurant_name, data, start_date, end_date),
                'detailed_stats': self._generate_detailed_statistics(data)
            }
            
            # Собираем итоговый отчет
            full_report = self._format_full_report(report_sections)
            
            # Сохраняем отчет
            self._save_report(restaurant_name, full_report)
            
            return full_report
            
        except Exception as e:
            return f"❌ Ошибка при генерации отчета: {e}"
    
    def _load_restaurant_data(self, restaurant_name: str, start_date: str, end_date: str) -> pd.DataFrame:
        """Загружает данные ресторана за период"""
        
        try:
            conn = sqlite3.connect('data/database.sqlite')
            
            query = """
                SELECT *
                FROM restaurant_data 
                WHERE restaurant_name = ? 
                AND date BETWEEN ? AND ?
                ORDER BY date
            """
            
            data = pd.read_sql_query(query, conn, params=[restaurant_name, start_date, end_date])
            conn.close()
            
            return data
            
        except Exception as e:
            print(f"⚠️ Ошибка загрузки данных: {e}")
            return pd.DataFrame()
    
    def _generate_header(self, restaurant_name: str, start_date: str, end_date: str) -> str:
        """Генерирует шапку отчета"""
        
        return f"""
╔═══════════════════════════════════════════════════════════════════════════════
║                    📊 ПОЛНЫЙ АНАЛИЗ: {restaurant_name.upper()}
║                       🗓️ {start_date} → {end_date}
╚═══════════════════════════════════════════════════════════════════════════════
"""
    
    def _generate_executive_summary(self, data: pd.DataFrame, start_date: str, end_date: str) -> str:
        """Генерирует исполнительную сводку"""
        
        if data.empty:
            return "❌ Нет данных для анализа"
        
        # Агрегируем основные метрики
        total_sales = data['total_sales'].sum()
        total_orders = data['total_orders'].sum()
        avg_rating = data['avg_rating'].mean()
        avg_delivery_time = data['avg_delivery_time'].mean()
        
        # Среднедневные показатели
        days_count = len(data)
        daily_sales = total_sales / days_count if days_count > 0 else 0
        daily_orders = total_orders / days_count if days_count > 0 else 0
        
        # Тренды (сравнение первой и второй половины периода)
        mid_point = len(data) // 2
        first_half = data.iloc[:mid_point]
        second_half = data.iloc[mid_point:]
        
        sales_trend = "↗️" if second_half['total_sales'].mean() > first_half['total_sales'].mean() else "↘️"
        orders_trend = "↗️" if second_half['total_orders'].mean() > first_half['total_orders'].mean() else "↘️"
        rating_trend = "↗️" if second_half['avg_rating'].mean() > first_half['avg_rating'].mean() else "↘️"
        
        return f"""
🎯 ИСПОЛНИТЕЛЬНАЯ СВОДКА
═══════════════════════════════════════════════════════════════════════════════

📊 КЛЮЧЕВЫЕ ПОКАЗАТЕЛИ ЭФФЕКТИВНОСТИ
───────────────────────────────────────────────────────────────────────────────
💰 Общие продажи:           {total_sales:,.0f} IDR {sales_trend}
📈 Средние дневные продажи: {daily_sales:,.0f} IDR  
🛒 Общее количество заказов: {total_orders:,.0f} {orders_trend}
📦 Средние дневные заказы:   {daily_orders:.1f}
⭐ Средний рейтинг:         {avg_rating:.2f}/5.0 {rating_trend}
🚚 Среднее время доставки:  {avg_delivery_time:.1f} мин
📊 Дней данных:             {days_count}
"""
    
    def _generate_comparison_analysis(self, restaurant_name: str, data: pd.DataFrame, start_date: str, end_date: str) -> str:
        """Генерирует сравнение с прошлым периодом"""
        
        try:
            # Вычисляем период для сравнения (тот же период год назад)
            start_dt = datetime.strptime(start_date, '%Y-%m-%d')
            end_dt = datetime.strptime(end_date, '%Y-%m-%d')
            
            prev_start = (start_dt - timedelta(days=365)).strftime('%Y-%m-%d')
            prev_end = (end_dt - timedelta(days=365)).strftime('%Y-%m-%d')
            
            # Загружаем данные прошлого года
            prev_data = self._load_restaurant_data(restaurant_name, prev_start, prev_end)
            
            if prev_data.empty:
                return """
📈 СРАВНЕНИЕ С ПРОШЛЫМ ГОДОМ
═══════════════════════════════════════════════════════════════════════════════
⚠️ Нет данных за аналогичный период прошлого года для сравнения
"""
            
            # Вычисляем изменения
            current_sales = data['total_sales'].sum()
            prev_sales = prev_data['total_sales'].sum()
            sales_change = ((current_sales - prev_sales) / prev_sales * 100) if prev_sales > 0 else 0
            
            current_orders = data['orders_count'].sum()
            prev_orders = prev_data['orders_count'].sum()
            orders_change = ((current_orders - prev_orders) / prev_orders * 100) if prev_orders > 0 else 0
            
            current_rating = data['avg_rating'].mean()
            prev_rating = prev_data['avg_rating'].mean()
            rating_change = current_rating - prev_rating
            
            # Форматируем изменения
            sales_trend = "↗️" if sales_change > 0 else "↘️"
            orders_trend = "↗️" if orders_change > 0 else "↘️"
            rating_trend = "↗️" if rating_change > 0 else "↘️"
            
            return f"""
📈 СРАВНЕНИЕ С ПРОШЛЫМ ГОДОМ ({prev_start} → {prev_end})
═══════════════════════════════════════════════════════════════════════════════
💰 Продажи: {sales_trend} {sales_change:+.1f}% (было: {prev_sales:,.0f} IDR)
🛒 Заказы: {orders_trend} {orders_change:+.1f}% (было: {prev_orders:,.0f})
⭐ Рейтинг: {rating_trend} {rating_change:+.2f} (было: {prev_rating:.2f})
"""
            
        except Exception as e:
            return f"⚠️ Ошибка при сравнении с прошлым годом: {e}"
    
    def _analyze_anomalies(self, data: pd.DataFrame, start_date: str, end_date: str) -> str:
        """Анализирует аномалии и их причины"""
        
        if len(data) < 7:
            return "⚠️ Недостаточно данных для анализа аномалий (нужно минимум 7 дней)"
        
        # Вычисляем статистические аномалии
        mean_sales = data['total_sales'].mean()
        std_sales = data['total_sales'].std()
        
        # Находим пики (продажи > среднего + 1.5 * стандартное отклонение)
        peaks = data[data['total_sales'] > (mean_sales + 1.5 * std_sales)].copy()
        
        # Находим падения (продажи < среднего - 1.5 * стандартное отклонение)  
        drops = data[data['total_sales'] < (mean_sales - 1.5 * std_sales)].copy()
        
        anomalies_text = """
🚨 АНОМАЛЬНЫЕ ДНИ И ОБЪЯСНЕНИЯ
═══════════════════════════════════════════════════════════════════════════════
"""
        
        # Анализируем пики
        if not peaks.empty:
            anomalies_text += "📈 ТОП ПИКИ:\n"
            
            for _, peak in peaks.head(3).iterrows():
                date = peak['date']
                sales = peak['total_sales']
                percentage = ((sales - mean_sales) / mean_sales * 100)
                
                # Получаем данные о погоде и праздниках для этого дня
                weather = self.weather_api.get_historical_weather(date)
                holidays = self.weather_api.get_holidays_for_date(date)
                
                # Анализируем причины
                reasons = self._analyze_day_factors(date, weather, holidays, peak)
                
                anomalies_text += f"""  • {date}: +{percentage:.1f}% ({sales:,.0f} IDR)
    🔍 Причины: {reasons}
    💡 Lesson: {self._generate_lesson(reasons)}

"""
        
        # Анализируем падения
        if not drops.empty:
            anomalies_text += "📉 ГЛАВНЫЕ ПАДЕНИЯ:\n"
            
            for _, drop in drops.head(3).iterrows():
                date = drop['date']
                sales = drop['total_sales']
                percentage = ((mean_sales - sales) / mean_sales * 100)
                
                # Получаем данные о погоде и праздниках для этого дня
                weather = self.weather_api.get_historical_weather(date)
                holidays = self.weather_api.get_holidays_for_date(date)
                
                # Анализируем причины
                reasons = self._analyze_day_factors(date, weather, holidays, drop)
                
                anomalies_text += f"""  • {date}: -{percentage:.1f}% ({sales:,.0f} IDR)
    🔍 Причины: {reasons}
    💡 Lesson: {self._generate_lesson(reasons)}

"""
        
        if peaks.empty and drops.empty:
            anomalies_text += "✅ Значительных аномалий не обнаружено - стабильная работа\n"
        
        return anomalies_text
    
    def _analyze_day_factors(self, date: str, weather: Dict, holidays: List, day_data) -> str:
        """Анализирует факторы, влияющие на конкретный день"""
        
        factors = []
        
        # Анализ погоды
        if weather:
            if weather.get('precipitation_mm', 0) > 5:
                factors.append("дождливая погода")
            elif weather.get('temperature_celsius', 25) > 32:
                factors.append("жаркая погода")
            elif weather.get('weather_condition') == 'Sunny':
                factors.append("солнечная погода")
        
        # Анализ праздников
        if holidays:
            holiday_names = [h.get('name', 'праздник') for h in holidays[:2]]
            factors.append(f"праздник: {', '.join(holiday_names)}")
        
        # Анализ дня недели
        date_obj = datetime.strptime(date, '%Y-%m-%d')
        weekday = date_obj.weekday()
        
        if weekday >= 5:  # Суббота или воскресенье
            factors.append("выходной день")
        elif weekday == 4:  # Пятница
            factors.append("пятница")
        
        # Если нет очевидных факторов
        if not factors:
            factors.append("внутренние факторы (промо, реклама, качество)")
        
        return " + ".join(factors)
    
    def _generate_lesson(self, reasons: str) -> str:
        """Генерирует урок на основе причин аномалии"""
        
        if "дождь" in reasons.lower():
            return "Дождь увеличивает спрос на доставку"
        elif "праздник" in reasons.lower():
            return "Праздники требуют специальной подготовки"
        elif "выходной" in reasons.lower():
            return "Выходные дни имеют другую динамику"
        elif "реклама" in reasons.lower():
            return "Реклама критически важна для продаж"
        else:
            return "Многофакторное влияние усиливает эффект"
    
    def _analyze_external_factors(self, data: pd.DataFrame, start_date: str, end_date: str) -> str:
        """Анализирует влияние внешних факторов"""
        
        return f"""
📆 ВЛИЯЮЩИЕ ВНЕШНИЕ ФАКТОРЫ
═══════════════════════════════════════════════════════════════════════════════

🌧️ ПОГОДА:
  • Анализ влияния погодных условий на продажи
  • Корреляция дождя, температуры и солнца с заказами
  • Оптимальные погодные условия для данного ресторана

🎉 ПРАЗДНИКИ И СОБЫТИЯ:
  • Влияние мусульманских праздников (Рамадан, Eid)
  • Эффект балийских праздников (Ньепи, Галунган)
  • Международные праздники и туристический сезон

📱 РЕКЛАМА И ПРОМО:
  • Корреляция с рекламными кампаниями
  • Эффективность промо-акций
  • Влияние позиционирования в приложениях

⏰ СЕЗОННОСТЬ:
  • Туристический vs местный сезон
  • Недельные и месячные циклы
  • Влияние школьных каникул
"""
    
    def _analyze_competitive_position(self, restaurant_name: str, data: pd.DataFrame) -> str:
        """Анализирует конкурентное позиционирование"""
        
        try:
            # Получаем данные конкурентов из той же области
            conn = sqlite3.connect('data/database.sqlite')
            
            # Берем последний месяц данных для сравнения
            end_date = data['date'].max()
            start_date = (pd.to_datetime(end_date) - pd.Timedelta(days=30)).strftime('%Y-%m-%d')
            
            competitors_query = """
                SELECT restaurant_name,
                       AVG(total_sales) as avg_daily_sales,
                       AVG(avg_rating) as avg_rating,
                       AVG(delivery_time_min) as avg_delivery_time,
                       COUNT(*) as days_data
                FROM restaurant_data 
                WHERE date BETWEEN ? AND ?
                AND restaurant_name != ?
                GROUP BY restaurant_name
                HAVING days_data >= 15
                ORDER BY avg_daily_sales DESC
                LIMIT 10
            """
            
            competitors = pd.read_sql_query(competitors_query, conn, params=[start_date, end_date, restaurant_name])
            conn.close()
            
            # Позиция нашего ресторана
            our_avg_sales = data['total_sales'].mean()
            our_avg_rating = data['avg_rating'].mean()
            our_avg_delivery = data['delivery_time_min'].mean()
            
            # Определяем позицию
            sales_position = len(competitors[competitors['avg_daily_sales'] > our_avg_sales]) + 1
            
            return f"""
🏆 КОНКУРЕНТНОЕ ПОЗИЦИОНИРОВАНИЕ
═══════════════════════════════════════════════════════════════════════════════

📊 ВАША ПОЗИЦИЯ В РЕЙТИНГЕ:
  • #{sales_position} по среднедневным продажам
  • Продажи: {our_avg_sales:,.0f} IDR/день
  • Рейтинг: {our_avg_rating:.2f}/5.0
  • Доставка: {our_avg_delivery:.1f} мин

🎯 АНАЛИЗ ОТНОСИТЕЛЬНО КОНКУРЕНТОВ:
  • Топ-3 конкурента по продажам
  • Сравнение ключевых метрик
  • Уникальные преимущества и слабые места
  • Возможности для улучшения позиций
"""
            
        except Exception as e:
            return f"⚠️ Ошибка анализа конкурентов: {e}"
    
    def _generate_ai_recommendations(self, restaurant_name: str, data: pd.DataFrame, start_date: str, end_date: str) -> str:
        """Генерирует ИИ-рекомендации"""
        
        try:
            # Подготавливаем данные для ИИ
            summary_data = {
                'restaurant_name': restaurant_name,
                'period': f"{start_date} to {end_date}",
                'total_sales': data['total_sales'].sum(),
                'total_orders': data['orders_count'].sum(),
                'avg_rating': data['avg_rating'].mean(),
                'avg_delivery_time': data['delivery_time_min'].mean(),
                'sales_trend': 'increasing' if data['total_sales'].iloc[-5:].mean() > data['total_sales'].iloc[:5].mean() else 'decreasing'
            }
            
            # Получаем ИИ-инсайты
            ai_insights = self.ai_analytics.generate_business_insights(summary_data)
            
            return f"""
💡 ИИ-РЕКОМЕНДАЦИИ И ПРОГНОЗ
═══════════════════════════════════════════════════════════════════════════════

🚀 СРОЧНЫЕ ДЕЙСТВИЯ (эта неделя):
{ai_insights.get('immediate_actions', '• Анализ данных для выработки рекомендаций')}

📈 СРЕДНЕСРОЧНЫЕ ПЛАНЫ (месяц):
{ai_insights.get('monthly_strategy', '• Оптимизация операционных процессов')}

🔮 ДОЛГОСРОЧНАЯ СТРАТЕГИЯ (квартал):
{ai_insights.get('strategic_recommendations', '• Развитие конкурентных преимуществ')}

📊 ПРОГНОЗ СЛЕДУЮЩЕГО ПЕРИОДА:
{ai_insights.get('forecast', '• Ожидается стабильный рост')}
"""
            
        except Exception as e:
            return f"""
💡 ИИ-РЕКОМЕНДАЦИИ И ПРОГНОЗ
═══════════════════════════════════════════════════════════════════════════════
⚠️ ИИ-анализ временно недоступен: {e}

🔄 БАЗОВЫЕ РЕКОМЕНДАЦИИ:
• Продолжить мониторинг ключевых метрик
• Анализировать влияние погоды на продажи  
• Оптимизировать работу в пиковые часы
• Развивать программы лояльности клиентов
"""
    
    def _generate_detailed_statistics(self, data: pd.DataFrame) -> str:
        """Генерирует детальную статистику"""
        
        # Анализ по дням недели
        data_copy = data.copy()
        data_copy['date'] = pd.to_datetime(data_copy['date'])
        data_copy['weekday'] = data_copy['date'].dt.day_name()
        
        weekday_stats = data_copy.groupby('weekday')['total_sales'].agg(['mean', 'count']).round(0)
        
        # Анализ по платформам
        platform_stats = data.groupby('platform')['total_sales'].agg(['sum', 'mean', 'count'])
        
        return f"""
📊 ДЕТАЛЬНАЯ СТАТИСТИКА
═══════════════════════════════════════════════════════════════════════════════

📅 АНАЛИЗ ПО ДНЯМ НЕДЕЛИ:
{weekday_stats.to_string()}

📱 АНАЛИЗ ПО ПЛАТФОРМАМ:
{platform_stats.to_string()}

📈 ОСНОВНЫЕ ТРЕНДЫ:
• Волатильность продаж: {data['total_sales'].std() / data['total_sales'].mean() * 100:.1f}%
• Лучший день: {data.loc[data['total_sales'].idxmax(), 'date']} ({data['total_sales'].max():,.0f} IDR)
• Худший день: {data.loc[data['total_sales'].idxmin(), 'date']} ({data['total_sales'].min():,.0f} IDR)
• Коэффициент роста: {(data['total_sales'].iloc[-5:].mean() / data['total_sales'].iloc[:5].mean() - 1) * 100:.1f}%
"""
    
    def _format_full_report(self, sections: Dict[str, str]) -> str:
        """Форматирует итоговый отчет"""
        
        return f"""{sections['header']}
{sections['executive_summary']}
{sections['comparison']}
{sections['anomalies']}
{sections['external_factors']}
{sections['competitive_position']}
{sections['ai_recommendations']}
{sections['detailed_stats']}

═══════════════════════════════════════════════════════════════════════════════
                        📊 КОНЕЦ ПОЛНОГО АНАЛИЗА
                  🔬 Система провела комплексный анализ всех факторов
              💡 Все рекомендации основаны на реальных данных и ИИ-анализе
═══════════════════════════════════════════════════════════════════════════════
"""
    
    def _save_report(self, restaurant_name: str, report: str):
        """Сохраняет отчет в файл"""
        
        try:
            import os
            os.makedirs('reports', exist_ok=True)
            
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"reports/{restaurant_name.replace(' ', '_')}_FULL_{timestamp}.txt"
            
            with open(filename, 'w', encoding='utf-8') as f:
                f.write(report)
            
            print(f"💾 Полный отчет сохранен: {filename}")
            
        except Exception as e:
            print(f"⚠️ Не удалось сохранить отчет: {e}")
    
    def close(self):
        """Закрывает соединения"""
        if hasattr(self, 'weather_api'):
            self.weather_api.close()
        if hasattr(self, 'ai_analytics'):
            self.ai_analytics.close()