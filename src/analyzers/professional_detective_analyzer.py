"""
Профессиональный детективный анализатор уровня крупных аналитических компаний
Четкая структура, конкретные выводы, презентабельный формат
"""

import sqlite3
import pandas as pd
import json
import requests
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional

# Импорт fake orders filter если доступен
try:
    from ..utils.fake_orders_filter import FakeOrdersFilter
    FAKE_ORDERS_AVAILABLE = True
except ImportError:
    FAKE_ORDERS_AVAILABLE = False


class ProfessionalDetectiveAnalyzer:
    """Профессиональный детективный анализ продаж уровня крупных компаний"""
    
    def __init__(self):
        """Инициализация анализатора"""
        self.holidays_data = self._load_holidays()
        self.locations_data = self._load_locations()
        
        if FAKE_ORDERS_AVAILABLE:
            self.fake_orders_filter = FakeOrdersFilter()
        else:
            self.fake_orders_filter = None
    
    def analyze_sales_performance(self, restaurant_name: str, start_date: str, end_date: str) -> List[str]:
        """
        Главный метод профессионального анализа продаж
        
        Args:
            restaurant_name: Название ресторана
            start_date: Начальная дата (YYYY-MM-DD)
            end_date: Конечная дата (YYYY-MM-DD)
            
        Returns:
            List[str]: Профессиональный отчет
        """
        results = []
        
        # Заголовок
        results.append("🔍 7. АНАЛИЗ ПРИЧИН ИЗМЕНЕНИЯ ПРОДАЖ")
        results.append("=" * 80)
        results.append("")
        
        # Получаем базовые данные
        period_stats = self._get_period_statistics(restaurant_name, start_date, end_date)
        if not period_stats:
            results.append("❌ Недостаточно данных для анализа")
            return results
        
        # 1. Обзор периода
        results.extend(self._format_period_overview(period_stats, start_date, end_date))
        results.append("")
        
        # 2. Выявление проблемных дней
        problem_days = self._identify_problem_days(restaurant_name, start_date, end_date)
        results.extend(self._format_problem_days_summary(problem_days))
        results.append("")
        
        # 3. Детальный анализ ТОП-5 проблемных дней
        if problem_days:
            results.extend(self._analyze_top_problem_days(problem_days[:5], restaurant_name))
            results.append("")
        
        # 4. Сводка и рекомендации
        results.extend(self._generate_executive_summary(problem_days, period_stats))
        
        return results
    
    def _get_period_statistics(self, restaurant_name: str, start_date: str, end_date: str) -> Optional[Dict]:
        """Получает статистику за период"""
        try:
            with sqlite3.connect('database.sqlite') as conn:
                # Получаем ID ресторана
                restaurant_query = f"SELECT id FROM restaurants WHERE name = '{restaurant_name}'"
                restaurant_df = pd.read_sql_query(restaurant_query, conn)
                if restaurant_df.empty:
                    return None
                
                restaurant_id = restaurant_df.iloc[0]['id']
                
                # Получаем агрегированные данные за период
                query = f"""
                WITH all_dates AS (
                    SELECT stat_date FROM grab_stats
                    WHERE restaurant_id = {restaurant_id}
                    AND stat_date BETWEEN '{start_date}' AND '{end_date}'
                    UNION
                    SELECT stat_date FROM gojek_stats
                    WHERE restaurant_id = {restaurant_id}
                    AND stat_date BETWEEN '{start_date}' AND '{end_date}'
                )
                SELECT
                    COUNT(DISTINCT ad.stat_date) as total_days,
                    SUM(COALESCE(g.sales, 0) + COALESCE(gj.sales, 0)) as total_sales,
                    SUM(COALESCE(g.orders, 0) + COALESCE(gj.orders, 0)) as total_orders,
                    AVG(COALESCE(g.sales, 0) + COALESCE(gj.sales, 0)) as avg_daily_sales,
                    MIN(COALESCE(g.sales, 0) + COALESCE(gj.sales, 0)) as min_daily_sales,
                    MAX(COALESCE(g.sales, 0) + COALESCE(gj.sales, 0)) as max_daily_sales
                FROM all_dates ad
                LEFT JOIN grab_stats g ON ad.stat_date = g.stat_date AND g.restaurant_id = {restaurant_id}
                LEFT JOIN gojek_stats gj ON ad.stat_date = gj.stat_date AND gj.restaurant_id = {restaurant_id}
                """
                
                df = pd.read_sql_query(query, conn)
                if df.empty:
                    return None
                
                return df.iloc[0].to_dict()
                
        except Exception as e:
            print(f"Ошибка получения статистики: {e}")
            return None
    
    def _identify_problem_days(self, restaurant_name: str, start_date: str, end_date: str) -> List[Tuple]:
        """Выявляет проблемные дни с продажами"""
        try:
            with sqlite3.connect('database.sqlite') as conn:
                # Получаем ID ресторана
                restaurant_query = f"SELECT id FROM restaurants WHERE name = '{restaurant_name}'"
                restaurant_df = pd.read_sql_query(restaurant_query, conn)
                if restaurant_df.empty:
                    return []
                
                restaurant_id = restaurant_df.iloc[0]['id']
                
                # Получаем ежедневные продажи
                query = f"""
                WITH all_dates AS (
                    SELECT stat_date FROM grab_stats
                    WHERE restaurant_id = {restaurant_id}
                    AND stat_date BETWEEN '{start_date}' AND '{end_date}'
                    UNION
                    SELECT stat_date FROM gojek_stats
                    WHERE restaurant_id = {restaurant_id}
                    AND stat_date BETWEEN '{start_date}' AND '{end_date}'
                )
                SELECT
                    ad.stat_date,
                    COALESCE(g.sales, 0) + COALESCE(gj.sales, 0) as total_sales,
                    COALESCE(g.orders, 0) + COALESCE(gj.orders, 0) as total_orders
                FROM all_dates ad
                LEFT JOIN grab_stats g ON ad.stat_date = g.stat_date AND g.restaurant_id = {restaurant_id}
                LEFT JOIN gojek_stats gj ON ad.stat_date = gj.stat_date AND gj.restaurant_id = {restaurant_id}
                ORDER BY total_sales ASC
                """
                
                df = pd.read_sql_query(query, conn)
                if df.empty:
                    return []
                
                # Рассчитываем медиану и пороги
                median_sales = df['total_sales'].median()
                q25_sales = df['total_sales'].quantile(0.25)
                mean_sales = df['total_sales'].mean()
                
                problem_days = []
                
                for _, row in df.iterrows():
                    date = row['stat_date']
                    sales = row['total_sales']
                    
                    # Критерии проблемного дня
                    if sales < q25_sales:  # Нижний квартиль
                        deviation_from_median = ((median_sales - sales) / median_sales) * 100
                        deviation_from_mean = ((mean_sales - sales) / mean_sales) * 100
                        
                        problem_type = "critical" if sales < median_sales * 0.5 else "significant"
                        problem_days.append((date, sales, deviation_from_median, deviation_from_mean, problem_type))
                
                # Сортируем по убыванию отклонения
                problem_days.sort(key=lambda x: x[2], reverse=True)
                
                return problem_days[:15]  # Топ-15 проблемных дней
                
        except Exception as e:
            print(f"Ошибка выявления проблемных дней: {e}")
            return []
    
    def _format_period_overview(self, stats: Dict, start_date: str, end_date: str) -> List[str]:
        """Форматирует обзор периода"""
        results = []
        
        results.append("📊 ОБЗОР ПЕРИОДА")
        results.append("─" * 40)
        results.append(f"📅 Период анализа:     {start_date} — {end_date}")
        results.append(f"📈 Общие продажи:     {stats['total_sales']:>12,.0f} IDR")
        results.append(f"📦 Общие заказы:      {stats['total_orders']:>12,.0f}")
        results.append(f"📊 Дней в анализе:    {stats['total_days']:>12.0f}")
        results.append(f"💰 Средние продажи:   {stats['avg_daily_sales']:>12,.0f} IDR/день")
        results.append(f"📉 Минимум за день:   {stats['min_daily_sales']:>12,.0f} IDR")
        results.append(f"📈 Максимум за день:  {stats['max_daily_sales']:>12,.0f} IDR")
        
        # Расчет волатильности
        volatility = (stats['max_daily_sales'] - stats['min_daily_sales']) / stats['avg_daily_sales'] * 100
        results.append(f"📊 Волатильность:     {volatility:>12.1f}%")
        
        return results
    
    def _format_problem_days_summary(self, problem_days: List[Tuple]) -> List[str]:
        """Форматирует сводку проблемных дней"""
        results = []
        
        results.append("🚨 ВЫЯВЛЕННЫЕ ПРОБЛЕМНЫЕ ДНИ")
        results.append("─" * 40)
        
        if not problem_days:
            results.append("✅ Критических проблем с продажами не выявлено")
            return results
        
        results.append(f"📊 Найдено проблемных дней: {len(problem_days)}")
        results.append("")
        
        # Категоризация по типам проблем
        critical_days = [d for d in problem_days if d[4] == "critical"]
        significant_days = [d for d in problem_days if d[4] == "significant"]
        
        if critical_days:
            results.append(f"🔴 Критические проблемы:  {len(critical_days)} дней")
        if significant_days:
            results.append(f"🟠 Значительные проблемы: {len(significant_days)} дней")
        
        results.append("")
        results.append("📉 ТОП-5 НАИБОЛЕЕ ПРОБЛЕМНЫХ ДНЕЙ:")
        results.append("    Дата       │   Продажи   │ Отклонение │ Тип")
        results.append("─" * 50)
        
        for i, (date, sales, deviation, _, problem_type) in enumerate(problem_days[:5], 1):
            icon = "🔴" if problem_type == "critical" else "🟠"
            results.append(f"{i}. {date} │ {sales:>9,.0f} IDR │   -{deviation:>5.1f}%   │ {icon}")
        
        return results
    
    def _analyze_top_problem_days(self, problem_days: List[Tuple], restaurant_name: str) -> List[str]:
        """Детальный анализ топ проблемных дней"""
        results = []
        
        results.append("🔬 ДЕТАЛЬНЫЙ АНАЛИЗ ПРОБЛЕМНЫХ ДНЕЙ")
        results.append("=" * 80)
        results.append("")
        
        for i, (date, sales, deviation, _, problem_type) in enumerate(problem_days, 1):
            results.append(f"📉 ПРОБЛЕМНЫЙ ДЕНЬ #{i}: {date}")
            results.append("─" * 50)
            
            # Получаем детальные данные за день
            day_analysis = self._analyze_specific_day(restaurant_name, date, sales, deviation)
            results.extend(day_analysis)
            results.append("")
        
        return results
    
    def _analyze_specific_day(self, restaurant_name: str, date: str, sales: float, deviation: float) -> List[str]:
        """Анализирует конкретный день"""
        results = []
        
        # Получаем данные за день
        day_data = self._get_day_detailed_data(restaurant_name, date)
        if not day_data:
            results.append("❌ Нет детальных данных за этот день")
            return results
        
        # Базовая информация
        results.append(f"💰 Продажи: {sales:,.0f} IDR (отклонение: -{deviation:.1f}%)")
        results.append(f"📦 Заказы: {day_data['total_orders']} (Grab: {day_data['grab_orders']}, Gojek: {day_data['gojek_orders']})")
        results.append(f"💵 Средний чек: {sales/day_data['total_orders']:,.0f} IDR" if day_data['total_orders'] > 0 else "💵 Средний чек: н/д")
        results.append("")
        
        # Анализ причин
        factors = []
        impact_score = 0
        
        # 1. Операционные проблемы
        operational_factors, op_score = self._analyze_operational_issues(day_data)
        factors.extend(operational_factors)
        impact_score += op_score
        
        # 2. Внешние факторы
        external_factors, ext_score = self._analyze_external_factors(restaurant_name, date)
        factors.extend(external_factors)
        impact_score += ext_score
        
        # 3. Маркетинг и качество
        marketing_factors, mk_score = self._analyze_marketing_quality(day_data)
        factors.extend(marketing_factors)
        impact_score += mk_score
        
        # Выводим факторы
        if factors:
            results.append("🔍 ВЫЯВЛЕННЫЕ ПРИЧИНЫ:")
            for j, factor in enumerate(factors, 1):
                results.append(f"   {j}. {factor}")
        else:
            results.append("🔍 ВЫЯВЛЕННЫЕ ПРИЧИНЫ: Внутренние операционные факторы")
        
        results.append("")
        
        # Оценка критичности
        if impact_score >= 80:
            severity = "🔴 КРИТИЧЕСКОЕ"
            recommendation = "Требуется немедленное вмешательство"
        elif impact_score >= 50:
            severity = "🟠 ЗНАЧИТЕЛЬНОЕ"
            recommendation = "Необходимы корректирующие меры"
        elif impact_score >= 30:
            severity = "🟡 УМЕРЕННОЕ"
            recommendation = "Рекомендуется мониторинг"
        else:
            severity = "🟢 НИЗКОЕ"
            recommendation = "Естественные колебания"
        
        results.append(f"📊 Оценка влияния: {severity} (балл: {impact_score})")
        results.append(f"💡 Рекомендация: {recommendation}")
        
        return results
    
    def _get_day_detailed_data(self, restaurant_name: str, date: str) -> Optional[Dict]:
        """Получает детальные данные за конкретный день"""
        try:
            with sqlite3.connect('database.sqlite') as conn:
                # Получаем ID ресторана
                restaurant_query = f"SELECT id FROM restaurants WHERE name = '{restaurant_name}'"
                restaurant_df = pd.read_sql_query(restaurant_query, conn)
                if restaurant_df.empty:
                    return None
                
                restaurant_id = restaurant_df.iloc[0]['id']
                
                # Получаем данные Grab
                grab_query = f"""
                SELECT 
                    COALESCE(sales, 0) as grab_sales,
                    COALESCE(orders, 0) as grab_orders,
                    COALESCE(cancelled_orders, 0) as grab_cancelled,
                    COALESCE(rating, 0) as grab_rating,
                    COALESCE(ads_spend, 0) as grab_ads_spend,
                    COALESCE(ads_sales, 0) as grab_ads_sales,
                    COALESCE(offline_rate, 0) as grab_offline_rate,
                    COALESCE(driver_waiting_time, 0) as grab_driver_waiting
                FROM grab_stats 
                WHERE restaurant_id = {restaurant_id} AND stat_date = '{date}'
                """
                
                # Получаем данные Gojek
                gojek_query = f"""
                SELECT 
                    COALESCE(sales, 0) as gojek_sales,
                    COALESCE(orders, 0) as gojek_orders,
                    COALESCE(cancelled_orders, 0) as gojek_cancelled,
                    COALESCE(lost_orders, 0) as gojek_lost,
                    COALESCE(rating, 0) as gojek_rating,
                    COALESCE(ads_spend, 0) as gojek_ads_spend,
                    COALESCE(ads_sales, 0) as gojek_ads_sales,
                    COALESCE(close_time, 0) as gojek_close_time,
                    COALESCE(preparation_time, '00:00:00') as gojek_preparation_time,
                    COALESCE(delivery_time, '00:00:00') as gojek_delivery_time,
                    COALESCE(driver_waiting, 0) as gojek_driver_waiting
                FROM gojek_stats 
                WHERE restaurant_id = {restaurant_id} AND stat_date = '{date}'
                """
                
                grab_df = pd.read_sql_query(grab_query, conn)
                gojek_df = pd.read_sql_query(gojek_query, conn)
                
                # Объединяем данные
                result = {}
                
                if not grab_df.empty:
                    grab_data = grab_df.iloc[0].to_dict()
                    result.update(grab_data)
                else:
                    # Заполняем нулями
                    for key in ['grab_sales', 'grab_orders', 'grab_cancelled', 'grab_rating', 
                               'grab_ads_spend', 'grab_ads_sales', 'grab_offline_rate', 'grab_driver_waiting']:
                        result[key] = 0
                
                if not gojek_df.empty:
                    gojek_data = gojek_df.iloc[0].to_dict()
                    result.update(gojek_data)
                else:
                    # Заполняем нулями
                    for key in ['gojek_sales', 'gojek_orders', 'gojek_cancelled', 'gojek_lost', 'gojek_rating',
                               'gojek_ads_spend', 'gojek_ads_sales', 'gojek_close_time', 'gojek_driver_waiting']:
                        result[key] = 0
                    # Отдельно для времен
                    for key in ['gojek_preparation_time', 'gojek_delivery_time']:
                        result[key] = '00:00:00'
                
                # Рассчитываем общие показатели
                result['total_orders'] = result['grab_orders'] + result['gojek_orders']
                result['total_ads_spend'] = result['grab_ads_spend'] + result['gojek_ads_spend']
                result['total_ads_sales'] = result['grab_ads_sales'] + result['gojek_ads_sales']
                
                return result
                
        except Exception as e:
            print(f"Ошибка получения данных за день: {e}")
            return None
    
    def _analyze_operational_issues(self, day_data: Dict) -> Tuple[List[str], int]:
        """
        Анализирует операционные проблемы
        
        offline_rate от Grab - метрика недоступности ресторана:
        - 0% = нормальная работа
        - >100% = накопительные сбои системы, технические проблемы
        - >300% = критические системные сбои (как в случае 357%)
        """
        factors = []
        impact_score = 0
        
        # 1. Проблемы с платформами
        grab_offline = day_data.get('grab_offline_rate', 0)
        if grab_offline > 300:
            factors.append(f"🚨 Grab: критические сбои системы (offline rate {grab_offline:.0f}%)")
            impact_score += 50
        elif grab_offline > 100:
            factors.append(f"🚨 Grab: серьезные технические проблемы (offline rate {grab_offline:.0f}%)")
            impact_score += 40
        elif grab_offline > 50:
            factors.append(f"⚠️ Grab: повышенная нестабильность (offline rate {grab_offline:.0f}%)")
            impact_score += 30
        elif grab_offline > 20:
            factors.append(f"⚠️ Grab: частичная недоступность ({grab_offline:.0f}% времени)")
            impact_score += 20
        
        # 2. Выключение Gojek (close_time - время когда программа была выключена)
        gojek_close_time_raw = day_data.get('gojek_close_time', 0)
        
        # close_time может быть в формате "H:MM:SS" или числом
        close_time_str = str(gojek_close_time_raw) if gojek_close_time_raw else "0:0:0"
        
        if close_time_str not in ["0:0:0", "0", "None"] and gojek_close_time_raw:
            # Парсим время выключения
            try:
                if ":" in close_time_str:
                    parts = close_time_str.split(":")
                    hours = int(parts[0]) if parts[0] else 0
                    minutes = int(parts[1]) if len(parts) > 1 and parts[1] else 0
                    
                    if hours < 12:  # Выключение утром критично
                        factors.append(f"🚨 Программа Gojek выключена в {hours:02d}:{minutes:02d}")
                        impact_score += 45
                    else:  # Выключение вечером менее критично
                        factors.append(f"⚠️ Программа Gojek выключена в {hours:02d}:{minutes:02d}")
                        impact_score += 25
                else:
                    # Если число - интерпретируем как минуты
                    total_minutes = int(float(close_time_str))
                    if total_minutes > 0:
                        hours = total_minutes // 60
                        minutes = total_minutes % 60
                        factors.append(f"⚠️ Программа Gojek выключена ({hours}ч {minutes}м)")
                        impact_score += 30
            except:
                factors.append(f"⚠️ Проблемы с работой программы Gojek")
                impact_score += 20
        
        # 3. Операционные времена
        prep_time = self._time_to_minutes(day_data.get('gojek_preparation_time', '00:00:00'))
        if prep_time > 30:
            factors.append(f"⏱️ Длительная готовка: {prep_time} минут")
            impact_score += 15
        
        delivery_time = self._time_to_minutes(day_data.get('gojek_delivery_time', '00:00:00'))
        if delivery_time > 45:
            factors.append(f"🚚 Длительная доставка: {delivery_time} минут")
            impact_score += 15
        
        # 4. Ожидание водителей
        grab_waiting_raw = day_data.get('grab_driver_waiting', 0)
        gojek_waiting_raw = day_data.get('gojek_driver_waiting', 0)
        
        # Безопасное преобразование gojek_waiting
        try:
            gojek_waiting = int(gojek_waiting_raw) if gojek_waiting_raw else 0
        except:
            gojek_waiting = 0
        
        # Grab driver_waiting_time в секундах, конвертируем в минуты
        grab_waiting = 0
        try:
            if grab_waiting_raw and str(grab_waiting_raw) != '0':
                import json
                if isinstance(grab_waiting_raw, str):
                    grab_data = json.loads(grab_waiting_raw)
                    if isinstance(grab_data, dict) and 'average' in grab_data:
                        grab_waiting = float(grab_data['average']) / 60  # секунды в минуты
                    elif isinstance(grab_data, (int, float)):
                        grab_waiting = float(grab_data) / 60  # секунды в минуты
                elif isinstance(grab_waiting_raw, (int, float)):
                    grab_waiting = float(grab_waiting_raw) / 60  # секунды в минуты
        except:
            grab_waiting = 0
        
        if grab_waiting > 15:  # > 15 минут
            factors.append(f"⏰ Долгое ожидание водителей Grab: {grab_waiting:.1f} мин")
            impact_score += 10
        elif grab_waiting > 10:  # > 10 минут
            factors.append(f"⏰ Повышенное ожидание водителей Grab: {grab_waiting:.1f} мин")
            impact_score += 5
        
        if gojek_waiting > 15:
            factors.append(f"⏰ Долгое ожидание водителей Gojek: {gojek_waiting} мин")
            impact_score += 10
        
        # 5. Отмененные и потерянные заказы
        grab_cancelled = day_data.get('grab_cancelled', 0)
        gojek_cancelled = day_data.get('gojek_cancelled', 0)
        gojek_lost = day_data.get('gojek_lost', 0)
        
        total_orders = day_data.get('total_orders', 0)
        if total_orders > 0:
            cancellation_rate = (grab_cancelled + gojek_cancelled + gojek_lost) / total_orders * 100
            if cancellation_rate > 15:
                factors.append(f"📉 Высокий процент отмен: {cancellation_rate:.1f}%")
                impact_score += 20
            elif cancellation_rate > 10:
                factors.append(f"⚠️ Повышенные отмены: {cancellation_rate:.1f}%")
                impact_score += 10
        
        return factors, impact_score
    
    def _analyze_external_factors(self, restaurant_name: str, date: str) -> Tuple[List[str], int]:
        """Анализирует внешние факторы"""
        factors = []
        impact_score = 0
        
        # 1. Погода
        weather_data = self._get_weather_data(restaurant_name, date)
        if weather_data:
            precipitation = weather_data.get('precipitation', 0)
            temperature = weather_data.get('temperature', 27)
            
            if precipitation > 10:
                factors.append(f"🌧️ Сильный дождь: {precipitation:.1f}мм")
                impact_score += 25
            elif precipitation > 5:
                factors.append(f"🌦️ Умеренный дождь: {precipitation:.1f}мм")
                impact_score += 15
            elif precipitation > 1:
                factors.append(f"🌤️ Легкий дождь: {precipitation:.1f}мм")
                impact_score += 5
            
            if temperature < 22 or temperature > 35:
                factors.append(f"🌡️ Экстремальная температура: {temperature:.1f}°C")
                impact_score += 10
        
        # 2. Праздники
        holiday_info = self._check_holiday(date)
        if holiday_info:
            factors.append(f"🎉 {holiday_info}")
            impact_score += 15
        
        # 3. День недели
        date_obj = datetime.strptime(date, '%Y-%m-%d')
        weekday = date_obj.weekday()
        
        if weekday == 6:  # Воскресенье
            factors.append("📅 Воскресенье (обычно слабый день)")
            impact_score += 5
        elif weekday == 0:  # Понедельник
            factors.append("📅 Понедельник (начало недели)")
            impact_score += 3
        
        return factors, impact_score
    
    def _analyze_marketing_quality(self, day_data: Dict) -> Tuple[List[str], int]:
        """Анализирует маркетинг и качество"""
        factors = []
        impact_score = 0
        
        # 1. ROAS анализ
        grab_spend = day_data.get('grab_ads_spend', 0)
        grab_sales = day_data.get('grab_ads_sales', 0)
        gojek_spend = day_data.get('gojek_ads_spend', 0)
        gojek_sales = day_data.get('gojek_ads_sales', 0)
        
        if grab_spend > 0:
            grab_roas = grab_sales / grab_spend
            if grab_roas < 2:
                factors.append(f"📉 Низкий ROAS Grab: {grab_roas:.1f}")
                impact_score += 20
            elif grab_roas > 10:
                factors.append(f"📈 Отличный ROAS Grab: {grab_roas:.1f}")
                # Положительный фактор, не добавляем к impact_score
        
        if gojek_spend > 0:
            gojek_roas = gojek_sales / gojek_spend
            if gojek_roas < 2:
                factors.append(f"📉 Низкий ROAS Gojek: {gojek_roas:.1f}")
                impact_score += 20
            elif gojek_roas > 10:
                factors.append(f"📈 Отличный ROAS Gojek: {gojek_roas:.1f}")
        
        # 2. Рейтинги
        grab_rating = day_data.get('grab_rating', 0)
        gojek_rating = day_data.get('gojek_rating', 0)
        
        if grab_rating > 0 and grab_rating < 4.5:
            factors.append(f"⭐ Низкий рейтинг Grab: {grab_rating:.2f}")
            impact_score += 15
        
        if gojek_rating > 0 and gojek_rating < 4.5:
            factors.append(f"⭐ Низкий рейтинг Gojek: {gojek_rating:.2f}")
            impact_score += 15
        
        return factors, impact_score
    
    def _generate_executive_summary(self, problem_days: List[Tuple], period_stats: Dict) -> List[str]:
        """Генерирует исполнительную сводку"""
        results = []
        
        results.append("📋 ИСПОЛНИТЕЛЬНАЯ СВОДКА")
        results.append("=" * 80)
        results.append("")
        
        if not problem_days:
            results.append("✅ ЗАКЛЮЧЕНИЕ: Существенных проблем с продажами не выявлено")
            results.append("💡 РЕКОМЕНДАЦИЯ: Продолжить текущую стратегию")
            return results
        
        # Анализ проблем
        critical_days = [d for d in problem_days if d[4] == "critical"]
        total_lost_revenue = sum([period_stats['avg_daily_sales'] - d[1] for d in problem_days])
        
        results.append("🔍 КЛЮЧЕВЫЕ ВЫВОДЫ:")
        results.append(f"   • Выявлено {len(problem_days)} проблемных дней из {period_stats['total_days']:.0f}")
        results.append(f"   • {len(critical_days)} дней с критическими проблемами")
        results.append(f"   • Потенциальные потери: {total_lost_revenue:,.0f} IDR")
        results.append("")
        
        # Главные рекомендации
        results.append("💡 ПРИОРИТЕТНЫЕ ДЕЙСТВИЯ:")
        results.append("   1. 🔧 Мониторинг стабильности платформ (Grab/Gojek)")
        results.append("   2. ⏱️ Оптимизация операционных процессов")
        results.append("   3. 🌤️ Планирование с учетом погодных условий")
        results.append("   4. 📊 Еженедельный анализ ключевых метрик")
        results.append("")
        
        # ROI от улучшений
        potential_recovery = total_lost_revenue * 0.7  # 70% потерь можно восстановить
        results.append(f"🎯 ПОТЕНЦИАЛЬНЫЙ ЭФФЕКТ: +{potential_recovery:,.0f} IDR/месяц")
        
        return results
    
    # Вспомогательные методы
    def _load_holidays(self) -> Dict:
        """Загружает данные о праздниках"""
        try:
            with open('data/comprehensive_holiday_analysis.json', 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            print(f"Ошибка загрузки праздников: {e}")
            return {}
    
    def _load_locations(self) -> Dict:
        """Загружает данные о локациях ресторанов"""
        try:
            with open('data/bali_restaurant_locations.json', 'r', encoding='utf-8') as f:
                data = json.load(f)
                locations = {}
                if 'restaurants' in data:
                    for restaurant in data['restaurants']:
                        locations[restaurant['name']] = {
                            'latitude': restaurant['latitude'],
                            'longitude': restaurant['longitude'],
                            'location': restaurant['location']
                        }
                return locations
        except Exception as e:
            print(f"Ошибка загрузки локаций: {e}")
            return {}
    
    def _get_weather_data(self, restaurant_name: str, date: str) -> Optional[Dict]:
        """Получает данные о погоде за конкретный день"""
        if restaurant_name not in self.locations_data:
            return None
        
        try:
            location = self.locations_data[restaurant_name]
            lat = location['latitude']
            lon = location['longitude']
            
            url = f"https://api.open-meteo.com/v1/forecast"
            params = {
                'latitude': lat,
                'longitude': lon,
                'daily': 'precipitation_sum,temperature_2m_mean',
                'start_date': date,
                'end_date': date,
                'timezone': 'Asia/Jakarta'
            }
            
            response = requests.get(url, params=params, timeout=10)
            if response.status_code == 200:
                data = response.json()
                if 'daily' in data and data['daily']['time']:
                    return {
                        'precipitation': data['daily']['precipitation_sum'][0] or 0,
                        'temperature': data['daily']['temperature_2m_mean'][0] or 27
                    }
        except Exception as e:
            print(f"Ошибка получения погоды: {e}")
        
        return None
    
    def _check_holiday(self, date: str) -> Optional[str]:
        """Проверяет является ли день праздником"""
        if date in self.holidays_data:
            holiday = self.holidays_data[date]
            return f"{holiday.get('name', 'Праздник')} ({holiday.get('type', 'local')})"
        return None
    
    def _time_to_minutes(self, time_str: str) -> int:
        """Конвертирует время HH:MM:SS в минуты"""
        if not time_str or time_str in ['00:00:00', '0:0:0', '0:00:00', '00:0:0']:
            return 0
        
        try:
            parts = time_str.split(':')
            hours = int(parts[0]) if parts[0] else 0
            minutes = int(parts[1]) if len(parts) > 1 and parts[1] else 0
            return hours * 60 + minutes
        except:
            return 0