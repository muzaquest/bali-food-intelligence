#!/usr/bin/env python3
"""
🔬 УЛУЧШЕННАЯ СИСТЕМА АНАЛИТИКИ
Расширенный анализ с детальными инсайтами, аномалиями и рекомендациями
"""

import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Any
import warnings
warnings.filterwarnings('ignore')

class EnhancedAnalytics:
    def __init__(self, db_path='data/database.sqlite'):
        self.conn = sqlite3.connect(db_path)
        self.data = None
        self.restaurant_data = None
        
    def load_restaurant_data(self, restaurant_name: str, start_date: str = None, end_date: str = None):
        """Загружает данные ресторана с фильтрацией по датам"""
        
        query = '''
            SELECT * FROM restaurant_data
            WHERE restaurant_name = ?
        '''
        params = [restaurant_name]
        
        if start_date and end_date:
            query += ' AND date >= ? AND date <= ?'
            params.extend([start_date, end_date])
        
        query += ' ORDER BY date, platform'
        
        self.restaurant_data = pd.read_sql_query(query, self.conn, params=params)
        self.restaurant_data['date'] = pd.to_datetime(self.restaurant_data['date'])
        
        # Агрегируем по дням (суммируем Grab + Gojek)
        daily_data = self.restaurant_data.groupby('date').agg({
            'total_sales': 'sum',
            'orders': 'sum',
            'rating': 'mean',
            'delivery_time': 'mean',
            'marketing_spend': 'sum',
            'marketing_sales': 'sum',
            'weather_condition': 'first',
            'temperature_celsius': 'mean',
            'is_weekend': 'first',
            'is_holiday': 'first',
            'is_tourist_high_season': 'first'
        }).reset_index()
        
        # Добавляем вычисляемые поля
        daily_data['avg_order_value'] = daily_data['total_sales'] / daily_data['orders'].replace(0, 1)
        daily_data['roas'] = daily_data['marketing_sales'] / daily_data['marketing_spend'].replace(0, 1)
        daily_data['ads_on'] = (daily_data['marketing_spend'] > 0).astype(int)
        
        self.data = daily_data
        return self.data
    
    def find_anomalies(self, threshold: float = 2.0) -> Dict[str, Any]:
        """Детальный поиск аномалий с объяснениями"""
        
        if self.data is None or len(self.data) == 0:
            return {}
        
        # Рассчитываем Z-score для продаж
        mean_sales = self.data['total_sales'].mean()
        std_sales = self.data['total_sales'].std()
        self.data['z_score'] = (self.data['total_sales'] - mean_sales) / std_sales
        
        # Находим аномалии
        high_anomalies = self.data[self.data['z_score'] > threshold].copy()
        low_anomalies = self.data[self.data['z_score'] < -threshold].copy()
        
        # Анализируем причины аномалий
        def analyze_anomaly_reasons(row):
            reasons = []
            
            # Реклама
            if row['ads_on'] == 1 and row['roas'] > 50:
                reasons.append(f"успешная реклама (ROAS {row['roas']:.1f})")
            elif row['ads_on'] == 0:
                reasons.append("отключена реклама")
            
            # День недели
            if row['is_weekend'] == 1:
                reasons.append("выходной день")
            
            # Детальный анализ погоды
            if row['weather_condition'] == 'Rainy':
                if row.get('precipitation_mm', 0) > 10:
                    reasons.append(f"сильный дождь ({row.get('precipitation_mm', 0):.1f}мм)")
                else:
                    reasons.append("дождливая погода")
            elif row['weather_condition'] == 'Stormy':
                reasons.append("шторм/гроза - мало водителей")
            elif row['weather_condition'] == 'Sunny':
                if row.get('temperature_celsius', 28) > 33:
                    reasons.append(f"очень жарко ({row.get('temperature_celsius', 28):.1f}°C)")
                elif row.get('temperature_celsius', 28) > 30:
                    reasons.append("жаркая солнечная погода")
                else:
                    reasons.append("хорошая погода")
            
            # Специфические праздники
            if row['is_holiday'] == 1:
                # Проверяем дату для определения праздника
                date_str = row['date'].strftime('%m-%d') if hasattr(row['date'], 'strftime') else str(row['date'])[5:]
                
                if date_str in ['03-14', '03-25']:  # Nyepi
                    reasons.append("Nyepi (день тишины) - полный запрет деятельности")
                elif date_str in ['01-01']:
                    reasons.append("Новый год")
                elif date_str in ['04-10', '04-11']:  # Eid al-Fitr
                    reasons.append("Ураза-байрам - семейные празднования")
                elif date_str in ['08-17']:
                    reasons.append("День независимости Индонезии")
                elif date_str in ['12-25']:
                    reasons.append("Рождество")
                else:
                    reasons.append("праздничный день")
            
            # Туристический сезон (высокий/низкий)
            if row['is_tourist_high_season'] == 1:
                reasons.append("пиковый туристический сезон")
            
            # Дополнительные факторы
            month = row['date'].month if hasattr(row['date'], 'month') else int(str(row['date'])[5:7])
            
            # Сезон дождей на Бали
            if month in [12, 1, 2, 3]:
                if row['weather_condition'] != 'Rainy':
                    reasons.append("сухой день в сезон дождей")
            
            # Рамадан (примерно март-апрель)
            if month in [3, 4] and not row.get('is_holiday', 0):
                reasons.append("период Рамадана - изменение режима питания")
            
            return ", ".join(reasons) if reasons else "стандартные условия"
        
        # Добавляем анализ причин
        if not high_anomalies.empty:
            high_anomalies['reasons'] = high_anomalies.apply(analyze_anomaly_reasons, axis=1)
            high_anomalies['deviation_pct'] = ((high_anomalies['total_sales'] - mean_sales) / mean_sales * 100)
        
        if not low_anomalies.empty:
            low_anomalies['reasons'] = low_anomalies.apply(analyze_anomaly_reasons, axis=1)
            low_anomalies['deviation_pct'] = ((low_anomalies['total_sales'] - mean_sales) / mean_sales * 100)
        
        return {
            'mean_sales': mean_sales,
            'std_sales': std_sales,
            'high_anomalies': high_anomalies.head(5),  # Топ-5 пиков
            'low_anomalies': low_anomalies.head(5),    # Топ-5 провалов
            'volatility_index': std_sales / mean_sales * 100
        }
    
    def analyze_trends_and_patterns(self) -> Dict[str, Any]:
        """Анализ трендов, сезонности и паттернов"""
        
        if self.data is None or len(self.data) == 0:
            return {}
        
        # Добавляем временные поля
        self.data['day_of_week'] = self.data['date'].dt.dayofweek
        self.data['month'] = self.data['date'].dt.month
        self.data['week'] = self.data['date'].dt.isocalendar().week
        
        # Анализ по дням недели
        weekly_pattern = self.data.groupby('day_of_week').agg({
            'total_sales': ['mean', 'count'],
            'orders': 'mean',
            'roas': 'mean'
        }).round(0)
        
        # Анализ выходных vs будни
        weekend_analysis = self.data.groupby('is_weekend').agg({
            'total_sales': 'mean',
            'orders': 'mean'
        })
        
        weekend_effect = ((weekend_analysis.loc[1, 'total_sales'] - weekend_analysis.loc[0, 'total_sales']) 
                         / weekend_analysis.loc[0, 'total_sales'] * 100)
        
        # Анализ эффективности рекламы
        ads_analysis = self.data.groupby('ads_on').agg({
            'total_sales': 'mean',
            'orders': 'mean'
        })
        
        if len(ads_analysis) > 1:
            ads_effect = ((ads_analysis.loc[1, 'total_sales'] - ads_analysis.loc[0, 'total_sales']) 
                         / ads_analysis.loc[0, 'total_sales'] * 100)
        else:
            ads_effect = 0
        
        # Анализ погодных условий
        weather_analysis = self.data.groupby('weather_condition').agg({
            'total_sales': 'mean',
            'orders': 'mean'
        }).sort_values('total_sales', ascending=False)
        
        # Тренд продаж (линейная регрессия)
        if len(self.data) > 1:
            x = np.arange(len(self.data))
            y = self.data['total_sales'].values
            
            # Простая линейная регрессия
            A = np.vstack([x, np.ones(len(x))]).T
            slope, intercept = np.linalg.lstsq(A, y, rcond=None)[0]
            
            trend_direction = "растет" if slope > 0 else "падает" if slope < 0 else "стабильный"
            trend_strength = abs(slope) / self.data['total_sales'].mean() * 100 * 30  # Месячный тренд
        else:
            trend_direction = "недостаточно данных"
            trend_strength = 0
        
        return {
            'weekly_pattern': weekly_pattern,
            'weekend_effect': weekend_effect,
            'ads_effect': ads_effect,
            'weather_analysis': weather_analysis,
            'trend_direction': trend_direction,
            'trend_strength': trend_strength,
            'best_day': weekly_pattern.loc[weekly_pattern[('total_sales', 'mean')].idxmax()],
            'worst_day': weekly_pattern.loc[weekly_pattern[('total_sales', 'mean')].idxmin()]
        }
    
    def compare_with_previous_period(self, start_date: str, end_date: str) -> Dict[str, Any]:
        """Сравнение с аналогичным периодом прошлого года"""
        
        # Рассчитываем даты прошлого года
        start_dt = datetime.strptime(start_date, '%Y-%m-%d')
        end_dt = datetime.strptime(end_date, '%Y-%m-%d')
        
        prev_start = start_dt.replace(year=start_dt.year - 1)
        prev_end = end_dt.replace(year=end_dt.year - 1)
        
        # Загружаем данные прошлого года
        prev_query = '''
            SELECT 
                SUM(total_sales) as prev_sales,
                SUM(orders) as prev_orders,
                AVG(rating) as prev_rating,
                AVG(delivery_time) as prev_delivery
            FROM restaurant_data
            WHERE restaurant_name = ? AND date >= ? AND date <= ?
        '''
        
        prev_data = pd.read_sql_query(
            prev_query, 
            self.conn, 
            params=[self.restaurant_data['restaurant_name'].iloc[0], 
                   prev_start.strftime('%Y-%m-%d'), 
                   prev_end.strftime('%Y-%m-%d')]
        ).iloc[0]
        
        # Текущие данные
        current_sales = self.data['total_sales'].sum()
        current_orders = self.data['orders'].sum()
        current_rating = self.data['rating'].mean()
        current_delivery = self.data['delivery_time'].mean()
        
        # Расчет изменений
        changes = {}
        if prev_data['prev_sales'] and prev_data['prev_sales'] > 0:
            changes['sales_change'] = ((current_sales - prev_data['prev_sales']) / prev_data['prev_sales'] * 100)
        else:
            changes['sales_change'] = 0
            
        if prev_data['prev_orders'] and prev_data['prev_orders'] > 0:
            changes['orders_change'] = ((current_orders - prev_data['prev_orders']) / prev_data['prev_orders'] * 100)
        else:
            changes['orders_change'] = 0
            
        changes['rating_change'] = current_rating - (prev_data['prev_rating'] or 0)
        changes['delivery_change'] = current_delivery - (prev_data['prev_delivery'] or 0)
        
        return {
            'current': {
                'sales': current_sales,
                'orders': current_orders,
                'rating': current_rating,
                'delivery': current_delivery
            },
            'previous': {
                'sales': prev_data['prev_sales'] or 0,
                'orders': prev_data['prev_orders'] or 0,
                'rating': prev_data['prev_rating'] or 0,
                'delivery': prev_data['prev_delivery'] or 0
            },
            'changes': changes
        }
    
    def generate_actionable_insights(self, anomalies: Dict, patterns: Dict, comparison: Dict) -> List[str]:
        """Генерирует конкретные рекомендации на основе анализа"""
        
        insights = []
        
        # Анализ аномалий
        if anomalies and 'high_anomalies' in anomalies and not anomalies['high_anomalies'].empty:
            best_day = anomalies['high_anomalies'].iloc[0]
            insights.append(f"🚀 ПИКОВЫЙ ДЕНЬ: {best_day['date'].strftime('%Y-%m-%d')} "
                          f"(+{best_day['deviation_pct']:.1f}%) благодаря {best_day['reasons']}")
        
        if anomalies and 'low_anomalies' in anomalies and not anomalies['low_anomalies'].empty:
            worst_day = anomalies['low_anomalies'].iloc[0]
            insights.append(f"📉 ПРОВАЛЬНЫЙ ДЕНЬ: {worst_day['date'].strftime('%Y-%m-%d')} "
                          f"({worst_day['deviation_pct']:.1f}%) из-за {worst_day['reasons']}")
        
        # Анализ трендов
        if patterns:
            if patterns['ads_effect'] > 10:
                insights.append(f"💰 РЕКЛАМА ЭФФЕКТИВНА: +{patterns['ads_effect']:.1f}% продаж при включении")
            elif patterns['ads_effect'] < -10:
                insights.append(f"⚠️ РЕКЛАМА НЕЭФФЕКТИВНА: {patterns['ads_effect']:.1f}% при включении")
            
            if patterns['weekend_effect'] > 5:
                insights.append(f"🏖️ ВЫХОДНЫЕ ПРИНОСЯТ БОЛЬШЕ: +{patterns['weekend_effect']:.1f}%")
            elif patterns['weekend_effect'] < -5:
                insights.append(f"🏢 БУДНИ ЭФФЕКТИВНЕЕ: {abs(patterns['weekend_effect']):.1f}%")
        
        # Сравнение с прошлым годом
        if comparison and 'changes' in comparison:
            changes = comparison['changes']
            if changes['sales_change'] < -10:
                insights.append(f"🚨 КРИТИЧЕСКОЕ ПАДЕНИЕ: продажи упали на {abs(changes['sales_change']):.1f}% год к году")
            elif changes['sales_change'] > 10:
                insights.append(f"📈 ОТЛИЧНЫЙ РОСТ: продажи выросли на {changes['sales_change']:.1f}% год к году")
        
        return insights

    def close(self):
        """Закрывает соединение с БД"""
        if self.conn:
            self.conn.close()