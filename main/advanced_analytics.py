#!/usr/bin/env python3
"""
🔬 ПРОДВИНУТАЯ СИСТЕМА БИЗНЕС-АНАЛИТИКИ
Находит скрытые паттерны, аномалии и взаимосвязи в 2.5 годах данных
"""

import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Any
import warnings
warnings.filterwarnings('ignore')

class AdvancedRestaurantAnalytics:
    """Продвинутая система анализа ресторанов"""
    
    def __init__(self):
        self.conn = sqlite3.connect('data/database.sqlite')
        self.data = None
        self.load_data()
    
    def load_data(self):
        """Загружает и подготавливает все данные"""
        query = '''
            SELECT * FROM restaurant_data
            ORDER BY date, restaurant_name, platform
        '''
        
        self.data = pd.read_sql_query(query, self.conn)
        self.data['date'] = pd.to_datetime(self.data['date'])
        
        # Создаём дополнительные поля для анализа
        self.data['year'] = self.data['date'].dt.year
        self.data['month'] = self.data['date'].dt.month
        self.data['quarter'] = self.data['date'].dt.quarter
        self.data['day_of_week'] = self.data['date'].dt.dayofweek
        self.data['week_of_year'] = self.data['date'].dt.isocalendar().week
        self.data['is_weekend'] = self.data['day_of_week'].isin([5, 6])
        
        print(f"✅ Загружено {len(self.data):,} записей за {self.data['date'].min().strftime('%Y-%m-%d')} - {self.data['date'].max().strftime('%Y-%m-%d')}")
    
    def analyze_restaurant_deep(self, restaurant_name: str, period_start: str = None, period_end: str = None) -> Dict[str, Any]:
        """Глубокий анализ ресторана с историческими сравнениями"""
        
        print(f"🔬 ГЛУБОКИЙ АНАЛИЗ: {restaurant_name.upper()}")
        print("=" * 80)
        
        # Фильтруем данные ресторана
        restaurant_data = self.data[self.data['restaurant_name'] == restaurant_name].copy()
        
        if restaurant_data.empty:
            return {"error": f"Ресторан {restaurant_name} не найден"}
        
        # Применяем фильтр периода если указан
        if period_start and period_end:
            mask = (restaurant_data['date'] >= period_start) & (restaurant_data['date'] <= period_end)
            current_period = restaurant_data[mask].copy()
            analysis_period = f"{period_start} - {period_end}"
        else:
            # Последние 3 месяца как текущий период
            latest_date = restaurant_data['date'].max()
            three_months_ago = latest_date - timedelta(days=90)
            current_period = restaurant_data[restaurant_data['date'] >= three_months_ago].copy()
            analysis_period = f"{three_months_ago.strftime('%Y-%m-%d')} - {latest_date.strftime('%Y-%m-%d')}"
        
        if current_period.empty:
            return {"error": "Нет данных за указанный период"}
        
        # Базовая статистика текущего периода
        current_stats = self.calculate_period_stats(current_period)
        
        # Исторические сравнения
        historical_analysis = self.analyze_historical_trends(restaurant_data, current_period)
        
        # Поиск аномалий
        anomalies = self.detect_anomalies(restaurant_data, current_period)
        
        # Анализ сезонности
        seasonality = self.analyze_seasonality(restaurant_data)
        
        # Анализ влияния маркетинга
        marketing_impact = self.analyze_marketing_effectiveness(restaurant_data, current_period)
        
        # Конкурентный анализ
        competitive_analysis = self.analyze_competition(restaurant_name, current_period)
        
        # Прогнозирование трендов
        trend_forecast = self.forecast_trends(restaurant_data)
        
        # Бизнес-инсайты
        business_insights = self.generate_business_insights(restaurant_data, current_period, historical_analysis)
        
        # Критические рекомендации
        recommendations = self.generate_strategic_recommendations(
            restaurant_data, current_stats, historical_analysis, marketing_impact, competitive_analysis
        )
        
        return {
            "restaurant_name": restaurant_name,
            "analysis_period": analysis_period,
            "current_stats": current_stats,
            "historical_analysis": historical_analysis,
            "anomalies": anomalies,
            "seasonality": seasonality,
            "marketing_impact": marketing_impact,
            "competitive_analysis": competitive_analysis,
            "trend_forecast": trend_forecast,
            "business_insights": business_insights,
            "recommendations": recommendations
        }
    
    def calculate_period_stats(self, period_data: pd.DataFrame) -> Dict[str, Any]:
        """Вычисляет статистику за период"""
        
        # Агрегируем по дням (суммируем платформы)
        daily_stats = period_data.groupby('date').agg({
            'total_sales': 'sum',
            'orders': 'sum',
            'rating': 'mean',
            'delivery_time': 'mean',
            'cancel_rate': 'mean',
            'ads_on': 'max',  # если хотя бы на одной платформе была реклама
            'roas': 'mean'
        }).reset_index()
        
        return {
            "total_sales": daily_stats['total_sales'].sum(),
            "avg_daily_sales": daily_stats['total_sales'].mean(),
            "total_orders": daily_stats['orders'].sum(),
            "avg_daily_orders": daily_stats['orders'].mean(),
            "avg_rating": daily_stats['rating'].mean(),
            "avg_delivery_time": daily_stats['delivery_time'].mean(),
            "avg_cancel_rate": daily_stats['cancel_rate'].mean(),
            "days_with_ads": daily_stats['ads_on'].sum(),
            "avg_roas": daily_stats[daily_stats['roas'] > 0]['roas'].mean() if (daily_stats['roas'] > 0).any() else 0,
            "days_analyzed": len(daily_stats),
            "sales_volatility": daily_stats['total_sales'].std() / daily_stats['total_sales'].mean() if daily_stats['total_sales'].mean() > 0 else 0
        }
    
    def analyze_historical_trends(self, full_data: pd.DataFrame, current_period: pd.DataFrame) -> Dict[str, Any]:
        """Анализирует исторические тренды"""
        
        # Группируем по месяцам для анализа трендов
        monthly_data = full_data.groupby([full_data['date'].dt.to_period('M'), 'platform']).agg({
            'total_sales': 'sum',
            'orders': 'sum',
            'rating': 'mean'
        }).reset_index()
        
        monthly_data['date'] = monthly_data['date'].dt.to_timestamp()
        
        # Суммируем платформы
        monthly_combined = monthly_data.groupby('date').agg({
            'total_sales': 'sum',
            'orders': 'sum', 
            'rating': 'mean'
        }).reset_index()
        
        # Тренды
        sales_trend = self.calculate_trend(monthly_combined['total_sales'].values)
        orders_trend = self.calculate_trend(monthly_combined['orders'].values)
        rating_trend = self.calculate_trend(monthly_combined['rating'].values)
        
        # Сравнение с прошлым годом
        current_period_start = current_period['date'].min()
        current_period_end = current_period['date'].max()
        
        # Тот же период прошлого года
        last_year_start = current_period_start - timedelta(days=365)
        last_year_end = current_period_end - timedelta(days=365)
        
        last_year_data = full_data[
            (full_data['date'] >= last_year_start) & 
            (full_data['date'] <= last_year_end)
        ]
        
        # Сравнение показателей
        current_stats = self.calculate_period_stats(current_period)
        last_year_stats = self.calculate_period_stats(last_year_data) if not last_year_data.empty else None
        
        yoy_comparison = {}
        if last_year_stats:
            yoy_comparison = {
                "sales_change": ((current_stats['total_sales'] / last_year_stats['total_sales']) - 1) * 100,
                "orders_change": ((current_stats['total_orders'] / last_year_stats['total_orders']) - 1) * 100,
                "rating_change": current_stats['avg_rating'] - last_year_stats['avg_rating'],
                "delivery_time_change": current_stats['avg_delivery_time'] - last_year_stats['avg_delivery_time']
            }
        
        return {
            "monthly_trend": {
                "sales_trend": sales_trend,
                "orders_trend": orders_trend, 
                "rating_trend": rating_trend
            },
            "year_over_year": yoy_comparison,
            "peak_month": monthly_combined.loc[monthly_combined['total_sales'].idxmax(), 'date'].strftime('%Y-%m'),
            "worst_month": monthly_combined.loc[monthly_combined['total_sales'].idxmin(), 'date'].strftime('%Y-%m'),
            "growth_acceleration": self.calculate_growth_acceleration(monthly_combined['total_sales'].values)
        }
    
    def detect_anomalies(self, full_data: pd.DataFrame, current_period: pd.DataFrame) -> Dict[str, Any]:
        """Детектирует аномалии в данных"""
        
        # Дневные данные (суммированные по платформам)
        daily_data = full_data.groupby('date').agg({
            'total_sales': 'sum',
            'orders': 'sum',
            'rating': 'mean',
            'ads_on': 'max',
            'roas': 'mean'
        }).reset_index()
        
        # Статистические аномалии (выбросы)
        sales_mean = daily_data['total_sales'].mean()
        sales_std = daily_data['total_sales'].std()
        
        # Аномалии = больше 2.5 стандартных отклонений
        anomaly_threshold = 2.5
        
        positive_anomalies = daily_data[
            daily_data['total_sales'] > (sales_mean + anomaly_threshold * sales_std)
        ].copy()
        
        negative_anomalies = daily_data[
            daily_data['total_sales'] < (sales_mean - anomaly_threshold * sales_std)
        ].copy()
        
        # Анализ причин аномалий
        positive_anomalies['deviation_pct'] = ((positive_anomalies['total_sales'] / sales_mean) - 1) * 100
        negative_anomalies['deviation_pct'] = ((negative_anomalies['total_sales'] / sales_mean) - 1) * 100
        
        # Топ аномалии
        top_positive = positive_anomalies.nlargest(5, 'total_sales')[['date', 'total_sales', 'orders', 'ads_on', 'roas', 'deviation_pct']].to_dict('records')
        top_negative = negative_anomalies.nsmallest(5, 'total_sales')[['date', 'total_sales', 'orders', 'ads_on', 'roas', 'deviation_pct']].to_dict('records')
        
        # Анализ причин аномалий
        anomaly_insights = self.analyze_anomaly_causes(full_data, top_positive, top_negative)
        
        return {
            "positive_anomalies": top_positive,
            "negative_anomalies": top_negative,
            "anomaly_insights": anomaly_insights,
            "volatility_score": sales_std / sales_mean if sales_mean > 0 else 0
        }
    
    def analyze_seasonality(self, full_data: pd.DataFrame) -> Dict[str, Any]:
        """Анализирует сезонные паттерны"""
        
        # Месячная сезонность
        monthly_seasonality = full_data.groupby('month')['total_sales'].mean().to_dict()
        peak_months = sorted(monthly_seasonality.items(), key=lambda x: x[1], reverse=True)[:3]
        low_months = sorted(monthly_seasonality.items(), key=lambda x: x[1])[:3]
        
        # Недельная сезонность  
        weekly_seasonality = full_data.groupby('day_of_week')['total_sales'].mean()
        
        # Праздничный эффект
        holiday_effect = self.analyze_holiday_impact(full_data)
        
        return {
            "peak_months": [{"month": m, "avg_sales": round(s, 2)} for m, s in peak_months],
            "low_months": [{"month": m, "avg_sales": round(s, 2)} for m, s in low_months],
            "weekly_pattern": {
                "weekend_boost": (weekly_seasonality[5:7].mean() / weekly_seasonality[0:5].mean() - 1) * 100,
                "best_day": weekly_seasonality.idxmax(),
                "worst_day": weekly_seasonality.idxmin()
            },
            "holiday_impact": holiday_effect
        }
    
    def analyze_marketing_effectiveness(self, full_data: pd.DataFrame, current_period: pd.DataFrame) -> Dict[str, Any]:
        """Анализирует эффективность маркетинга"""
        
        # Сравнение дней с рекламой и без
        with_ads = full_data[full_data['ads_on'] == 1]
        without_ads = full_data[full_data['ads_on'] == 0]
        
        if len(with_ads) == 0 or len(without_ads) == 0:
            return {"error": "Недостаточно данных для анализа маркетинга"}
        
        marketing_lift = (with_ads['total_sales'].mean() / without_ads['total_sales'].mean() - 1) * 100
        
        # ROAS анализ
        roas_data = with_ads[with_ads['roas'] > 0]
        
        # Тренды ROAS со временем
        roas_trends = self.analyze_roas_trends(full_data)
        
        # Оптимальные дни для рекламы
        optimal_days = self.find_optimal_marketing_days(full_data)
        
        return {
            "marketing_lift": marketing_lift,
            "avg_roas": roas_data['roas'].mean() if not roas_data.empty else 0,
            "best_roas": roas_data['roas'].max() if not roas_data.empty else 0,
            "worst_roas": roas_data['roas'].min() if not roas_data.empty else 0,
            "roas_trends": roas_trends,
            "optimal_days": optimal_days,
            "campaign_frequency": len(with_ads) / len(full_data) * 100
        }
    
    def analyze_competition(self, restaurant_name: str, current_period: pd.DataFrame) -> Dict[str, Any]:
        """Анализирует конкурентную позицию"""
        
        # Данные по всем ресторанам за тот же период
        period_start = current_period['date'].min()
        period_end = current_period['date'].max()
        
        all_restaurants = self.data[
            (self.data['date'] >= period_start) & 
            (self.data['date'] <= period_end)
        ].groupby('restaurant_name').agg({
            'total_sales': 'sum',
            'orders': 'sum',
            'rating': 'mean'
        }).round(2)
        
        # Позиция в рейтинге
        all_restaurants = all_restaurants.sort_values('total_sales', ascending=False)
        position = list(all_restaurants.index).index(restaurant_name) + 1 if restaurant_name in all_restaurants.index else None
        
        # Доля рынка
        total_market = all_restaurants['total_sales'].sum()
        restaurant_sales = all_restaurants.loc[restaurant_name, 'total_sales'] if restaurant_name in all_restaurants.index else 0
        market_share = (restaurant_sales / total_market * 100) if total_market > 0 else 0
        
        # Лидер рынка
        market_leader = all_restaurants.index[0] if not all_restaurants.empty else None
        
        return {
            "market_position": position,
            "market_share": market_share,
            "market_leader": market_leader,
            "competitors": all_restaurants.to_dict('index'),
            "competitive_gap": (all_restaurants.iloc[0]['total_sales'] / restaurant_sales - 1) * 100 if restaurant_sales > 0 else None
        }
    
    def forecast_trends(self, full_data: pd.DataFrame) -> Dict[str, Any]:
        """Прогнозирует тренды"""
        
        # Месячные данные для прогнозирования
        monthly_data = full_data.groupby(full_data['date'].dt.to_period('M'))['total_sales'].sum()
        
        # Простой линейный тренд
        x = np.arange(len(monthly_data))
        y = monthly_data.values
        
        if len(y) > 3:
            trend_coef = np.polyfit(x, y, 1)[0]
            trend_direction = "растет" if trend_coef > 0 else "падает"
            trend_strength = abs(trend_coef) / np.mean(y) * 100
        else:
            trend_direction = "недостаточно данных"
            trend_strength = 0
        
        return {
            "trend_direction": trend_direction,
            "trend_strength": trend_strength,
            "monthly_growth_rate": trend_coef if len(y) > 3 else 0
        }
    
    def generate_business_insights(self, full_data: pd.DataFrame, current_period: pd.DataFrame, historical_analysis: Dict) -> List[str]:
        """Генерирует бизнес-инсайты"""
        
        insights = []
        
        # Анализ роста
        if historical_analysis.get('year_over_year'):
            yoy = historical_analysis['year_over_year']
            if yoy.get('sales_change', 0) > 20:
                insights.append(f"🚀 Впечатляющий рост: продажи выросли на {yoy['sales_change']:.1f}% по сравнению с прошлым годом")
            elif yoy.get('sales_change', 0) < -10:
                insights.append(f"⚠️ Тревожное падение: продажи упали на {abs(yoy['sales_change']):.1f}% по сравнению с прошлым годом")
        
        # Анализ рейтинга
        current_rating = current_period['rating'].mean()
        if current_rating > 4.5:
            insights.append(f"⭐ Исключительное качество: рейтинг {current_rating:.2f}/5.0 - в топ-10% ресторанов")
        elif current_rating < 4.0:
            insights.append(f"⚠️ Проблемы с качеством: рейтинг {current_rating:.2f}/5.0 требует внимания")
        
        # Анализ волатильности
        daily_sales = current_period.groupby('date')['total_sales'].sum()
        volatility = daily_sales.std() / daily_sales.mean()
        if volatility > 0.4:
            insights.append(f"📊 Высокая волатильность: продажи колеблются на {volatility*100:.1f}% - нужна стабилизация")
        
        return insights
    
    def generate_strategic_recommendations(self, full_data: pd.DataFrame, current_stats: Dict, 
                                         historical_analysis: Dict, marketing_impact: Dict, 
                                         competitive_analysis: Dict) -> List[str]:
        """Генерирует стратегические рекомендации"""
        
        recommendations = []
        
        # Маркетинговые рекомендации
        if marketing_impact.get('marketing_lift', 0) > 50:
            recommendations.append(f"📢 КРИТИЧНО: Реклама дает +{marketing_impact['marketing_lift']:.1f}% к продажам - увеличить бюджет")
        
        if marketing_impact.get('avg_roas', 0) > 5:
            recommendations.append(f"💰 Отличный ROAS {marketing_impact['avg_roas']:.1f} - масштабировать успешные кампании")
        elif marketing_impact.get('avg_roas', 0) < 2:
            recommendations.append(f"⚠️ Низкий ROAS {marketing_impact['avg_roas']:.1f} - пересмотреть рекламную стратегию")
        
        # Конкурентные рекомендации
        if competitive_analysis.get('market_position', 999) > 3:
            recommendations.append(f"🎯 Низкая позиция #{competitive_analysis['market_position']} - изучить стратегии лидеров")
        
        # Операционные рекомендации
        if current_stats.get('avg_delivery_time', 0) > 35:
            recommendations.append(f"⏱️ Долгая доставка {current_stats['avg_delivery_time']:.1f} мин - оптимизировать логистику")
        
        if current_stats.get('avg_cancel_rate', 0) > 0.05:
            recommendations.append(f"❌ Высокий % отмен {current_stats['avg_cancel_rate']*100:.1f}% - улучшить процессы")
        
        return recommendations
    
    # Вспомогательные методы
    def calculate_trend(self, values: np.ndarray) -> str:
        """Вычисляет тренд временного ряда"""
        if len(values) < 3:
            return "недостаточно данных"
        
        x = np.arange(len(values))
        slope = np.polyfit(x, values, 1)[0]
        
        if abs(slope) < np.std(values) * 0.1:
            return "стабильный"
        elif slope > 0:
            return "растущий"
        else:
            return "падающий"
    
    def calculate_growth_acceleration(self, values: np.ndarray) -> str:
        """Определяет ускорение роста"""
        if len(values) < 4:
            return "недостаточно данных"
        
        # Вторая производная
        first_diff = np.diff(values)
        second_diff = np.diff(first_diff)
        
        avg_acceleration = np.mean(second_diff)
        
        if avg_acceleration > np.std(second_diff):
            return "ускоряется"
        elif avg_acceleration < -np.std(second_diff):
            return "замедляется"
        else:
            return "стабильный"
    
    def analyze_anomaly_causes(self, full_data: pd.DataFrame, positive_anomalies: List, negative_anomalies: List) -> Dict[str, Any]:
        """Анализирует причины аномалий"""
        
        insights = []
        
        # Анализ положительных аномалий
        for anomaly in positive_anomalies[:3]:
            date = anomaly['date']
            if isinstance(date, str):
                date = pd.to_datetime(date)
            
            day_data = full_data[full_data['date'] == date]
            ads_active = day_data['ads_on'].max() == 1
            avg_roas = day_data['roas'].mean()
            
            factors = []
            if ads_active and avg_roas > 5:
                factors.append(f"успешная реклама (ROAS {avg_roas:.1f})")
            if date.weekday() >= 5:
                factors.append("выходной день")
            
            insight = f"📈 {date.strftime('%Y-%m-%d')}: +{anomaly['deviation_pct']:.1f}% благодаря " + ", ".join(factors) if factors else "неизвестным факторам"
            insights.append(insight)
        
        # Анализ негативных аномалий
        for anomaly in negative_anomalies[:2]:
            date = anomaly['date']
            if isinstance(date, str):
                date = pd.to_datetime(date)
            
            factors = []
            if date.month == 3 and date.day in [11, 22, 29]:  # Возможные даты Nyepi
                factors.append("Nyepi (день тишины)")
            
            insight = f"📉 {date.strftime('%Y-%m-%d')}: {anomaly['deviation_pct']:.1f}% из-за " + ", ".join(factors) if factors else "неизвестных факторов"
            insights.append(insight)
        
        return {"insights": insights}
    
    def analyze_holiday_impact(self, full_data: pd.DataFrame) -> Dict[str, Any]:
        """Анализирует влияние праздников"""
        
        # Праздничные даты (примерно)
        holiday_effects = {}
        
        new_year_data = full_data[
            (full_data['date'].dt.month == 1) & 
            (full_data['date'].dt.day == 1)
        ]
        
        if not new_year_data.empty:
            avg_sales = full_data['total_sales'].mean()
            ny_effect = (new_year_data['total_sales'].mean() / avg_sales - 1) * 100
            holiday_effects['New Year'] = ny_effect
        
        return holiday_effects
    
    def analyze_roas_trends(self, full_data: pd.DataFrame) -> Dict[str, Any]:
        """Анализирует тренды ROAS"""
        
        roas_data = full_data[full_data['roas'] > 0]
        
        if roas_data.empty:
            return {"trend": "нет данных"}
        
        # Группируем по месяцам
        monthly_roas = roas_data.groupby(roas_data['date'].dt.to_period('M'))['roas'].mean()
        
        if len(monthly_roas) < 3:
            return {"trend": "недостаточно данных"}
        
        trend = self.calculate_trend(monthly_roas.values)
        
        return {
            "trend": trend,
            "best_month": monthly_roas.idxmax().strftime('%Y-%m'),
            "worst_month": monthly_roas.idxmin().strftime('%Y-%m')
        }
    
    def find_optimal_marketing_days(self, full_data: pd.DataFrame) -> Dict[str, Any]:
        """Находит оптимальные дни для рекламы"""
        
        roas_by_day = full_data[full_data['roas'] > 0].groupby('day_of_week')['roas'].mean()
        
        if roas_by_day.empty:
            return {"message": "нет данных о ROAS"}
        
        days_names = ['Понедельник', 'Вторник', 'Среда', 'Четверг', 'Пятница', 'Суббота', 'Воскресенье']
        
        best_day = roas_by_day.idxmax()
        worst_day = roas_by_day.idxmin()
        
        return {
            "best_day": days_names[best_day],
            "best_roas": roas_by_day[best_day],
            "worst_day": days_names[worst_day], 
            "worst_roas": roas_by_day[worst_day]
        }
    
    def close(self):
        """Закрывает соединение с базой данных"""
        self.conn.close()

# Функция для интеграции с основной системой
def run_advanced_analysis(restaurant_name: str, period_start: str = None, period_end: str = None) -> Dict[str, Any]:
    """Запускает продвинутый анализ ресторана"""
    
    analytics = AdvancedRestaurantAnalytics()
    try:
        result = analytics.analyze_restaurant_deep(restaurant_name, period_start, period_end)
        return result
    finally:
        analytics.close()

if __name__ == "__main__":
    # Тестирование
    result = run_advanced_analysis("Ika Canggu", "2024-04-01", "2024-06-30")
    print("🧪 Тест продвинутой аналитики завершен")