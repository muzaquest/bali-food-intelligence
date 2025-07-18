#!/usr/bin/env python3
"""
Генератор отчетов по периодам для ресторанов
Недельные, месячные и квартальные отчеты с ML анализом
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import json
import sqlite3
from client_data_adapter import ClientDataAdapter
from main import analyze_restaurant
import matplotlib.pyplot as plt
import seaborn as sns
from io import BytesIO
import base64

class ReportsGenerator:
    def __init__(self, db_path, ml_model_path=None):
        self.adapter = ClientDataAdapter(db_path)
        self.ml_model_path = ml_model_path
        
    def generate_weekly_report(self, restaurant_id, weeks_back=4):
        """
        Генерирует недельный отчет с ML анализом
        """
        print(f"📊 ГЕНЕРАЦИЯ НЕДЕЛЬНОГО ОТЧЕТА (последние {weeks_back} недель)")
        print("=" * 60)
        
        # Получаем данные
        end_date = datetime.now().date()
        start_date = end_date - timedelta(weeks=weeks_back)
        
        df = self.adapter.load_combined_sales_data(start_date, end_date)
        restaurant_data = df[df['restaurant_id'] == restaurant_id]
        
        if len(restaurant_data) == 0:
            return {"error": "Нет данных для ресторана"}
        
        restaurant_name = restaurant_data['name'].iloc[0]
        
        # Группируем по неделям
        restaurant_data['week_start'] = restaurant_data['date'].dt.to_period('W').dt.start_time
        weekly_stats = restaurant_data.groupby('week_start').agg({
            'sales': ['sum', 'mean', 'std'],
            'orders': ['sum', 'mean'],
            'rating': 'mean',
            'cancellation_rate': 'mean',
            'ads_spend': 'sum'
        }).round(2)
        
        # Flatten column names
        weekly_stats.columns = ['_'.join(col).strip() for col in weekly_stats.columns]
        
        # Вычисляем изменения
        weekly_stats['sales_change'] = weekly_stats['sales_sum'].pct_change() * 100
        weekly_stats['orders_change'] = weekly_stats['orders_sum'].pct_change() * 100
        
        # ML анализ для каждой недели
        ml_insights = []
        for week_start in weekly_stats.index:
            week_end = week_start + timedelta(days=6)
            week_data = restaurant_data[
                (restaurant_data['date'] >= week_start) & 
                (restaurant_data['date'] <= week_end)
            ]
            
            if len(week_data) > 0:
                # Анализируем каждый день недели
                daily_insights = []
                for _, day_data in week_data.iterrows():
                    if self.ml_model_path:
                        try:
                            insight = analyze_restaurant(
                                restaurant_id, 
                                day_data['date'].strftime('%Y-%m-%d')
                            )
                            daily_insights.append(insight)
                        except:
                            pass
                
                # Агрегируем инсайты недели
                week_insight = self._aggregate_weekly_insights(daily_insights)
                week_insight['week_start'] = week_start.strftime('%Y-%m-%d')
                ml_insights.append(week_insight)
        
        # Создаем итоговый отчет
        report = {
            'restaurant_name': restaurant_name,
            'restaurant_id': restaurant_id,
            'period': f"{start_date} - {end_date}",
            'weeks_analyzed': weeks_back,
            'summary': {
                'total_sales': weekly_stats['sales_sum'].sum(),
                'total_orders': weekly_stats['orders_sum'].sum(),
                'avg_rating': weekly_stats['rating_mean'].mean(),
                'avg_cancellation_rate': weekly_stats['cancellation_rate_mean'].mean(),
                'total_ads_spend': weekly_stats['ads_spend_sum'].sum()
            },
            'trends': {
                'sales_trend': self._calculate_trend(weekly_stats['sales_sum']),
                'orders_trend': self._calculate_trend(weekly_stats['orders_sum']),
                'rating_trend': self._calculate_trend(weekly_stats['rating_mean'])
            },
            'weekly_breakdown': weekly_stats.to_dict('index'),
            'ml_insights': ml_insights,
            'recommendations': self._generate_weekly_recommendations(weekly_stats, ml_insights)
        }
        
        return report
    
    def generate_monthly_report(self, restaurant_id, months_back=3):
        """
        Генерирует месячный отчет с ML анализом
        """
        print(f"📊 ГЕНЕРАЦИЯ МЕСЯЧНОГО ОТЧЕТА (последние {months_back} месяца)")
        print("=" * 60)
        
        # Получаем данные
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=30*months_back)
        
        df = self.adapter.load_combined_sales_data(start_date, end_date)
        restaurant_data = df[df['restaurant_id'] == restaurant_id]
        
        if len(restaurant_data) == 0:
            return {"error": "Нет данных для ресторана"}
        
        restaurant_name = restaurant_data['name'].iloc[0]
        
        # Группируем по месяцам
        restaurant_data['month'] = restaurant_data['date'].dt.to_period('M')
        monthly_stats = restaurant_data.groupby('month').agg({
            'sales': ['sum', 'mean', 'std'],
            'orders': ['sum', 'mean'],
            'rating': 'mean',
            'cancellation_rate': 'mean',
            'ads_spend': 'sum'
        }).round(2)
        
        # Flatten column names
        monthly_stats.columns = ['_'.join(col).strip() for col in monthly_stats.columns]
        
        # Вычисляем изменения
        monthly_stats['sales_change'] = monthly_stats['sales_sum'].pct_change() * 100
        monthly_stats['orders_change'] = monthly_stats['orders_sum'].pct_change() * 100
        
        # Сезонный анализ
        seasonal_analysis = self._analyze_seasonal_patterns(restaurant_data)
        
        # Создаем итоговый отчет
        report = {
            'restaurant_name': restaurant_name,
            'restaurant_id': restaurant_id,
            'period': f"{start_date} - {end_date}",
            'months_analyzed': months_back,
            'summary': {
                'total_sales': monthly_stats['sales_sum'].sum(),
                'total_orders': monthly_stats['orders_sum'].sum(),
                'avg_rating': monthly_stats['rating_mean'].mean(),
                'avg_cancellation_rate': monthly_stats['cancellation_rate_mean'].mean(),
                'total_ads_spend': monthly_stats['ads_spend_sum'].sum(),
                'avg_monthly_sales': monthly_stats['sales_sum'].mean(),
                'sales_volatility': monthly_stats['sales_sum'].std()
            },
            'trends': {
                'sales_trend': self._calculate_trend(monthly_stats['sales_sum']),
                'orders_trend': self._calculate_trend(monthly_stats['orders_sum']),
                'rating_trend': self._calculate_trend(monthly_stats['rating_mean'])
            },
            'monthly_breakdown': monthly_stats.to_dict('index'),
            'seasonal_analysis': seasonal_analysis,
            'recommendations': self._generate_monthly_recommendations(monthly_stats, seasonal_analysis)
        }
        
        return report
    
    def generate_quarterly_report(self, restaurant_id, quarters_back=4):
        """
        Генерирует квартальный отчет с ML анализом
        """
        print(f"📊 ГЕНЕРАЦИЯ КВАРТАЛЬНОГО ОТЧЕТА (последние {quarters_back} квартала)")
        print("=" * 60)
        
        # Получаем данные
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=90*quarters_back)
        
        df = self.adapter.load_combined_sales_data(start_date, end_date)
        restaurant_data = df[df['restaurant_id'] == restaurant_id]
        
        if len(restaurant_data) == 0:
            return {"error": "Нет данных для ресторана"}
        
        restaurant_name = restaurant_data['name'].iloc[0]
        
        # Группируем по кварталам
        restaurant_data['quarter'] = restaurant_data['date'].dt.to_period('Q')
        quarterly_stats = restaurant_data.groupby('quarter').agg({
            'sales': ['sum', 'mean', 'std'],
            'orders': ['sum', 'mean'],
            'rating': 'mean',
            'cancellation_rate': 'mean',
            'ads_spend': 'sum'
        }).round(2)
        
        # Flatten column names
        quarterly_stats.columns = ['_'.join(col).strip() for col in quarterly_stats.columns]
        
        # Вычисляем изменения
        quarterly_stats['sales_change'] = quarterly_stats['sales_sum'].pct_change() * 100
        quarterly_stats['orders_change'] = quarterly_stats['orders_sum'].pct_change() * 100
        
        # Годовой анализ
        yearly_analysis = self._analyze_yearly_patterns(restaurant_data)
        
        # ROI анализ
        roi_analysis = self._calculate_roi_analysis(quarterly_stats)
        
        # Создаем итоговый отчет
        report = {
            'restaurant_name': restaurant_name,
            'restaurant_id': restaurant_id,
            'period': f"{start_date} - {end_date}",
            'quarters_analyzed': quarters_back,
            'summary': {
                'total_sales': quarterly_stats['sales_sum'].sum(),
                'total_orders': quarterly_stats['orders_sum'].sum(),
                'avg_rating': quarterly_stats['rating_mean'].mean(),
                'avg_cancellation_rate': quarterly_stats['cancellation_rate_mean'].mean(),
                'total_ads_spend': quarterly_stats['ads_spend_sum'].sum(),
                'avg_quarterly_sales': quarterly_stats['sales_sum'].mean(),
                'sales_volatility': quarterly_stats['sales_sum'].std(),
                'growth_rate': self._calculate_growth_rate(quarterly_stats['sales_sum'])
            },
            'trends': {
                'sales_trend': self._calculate_trend(quarterly_stats['sales_sum']),
                'orders_trend': self._calculate_trend(quarterly_stats['orders_sum']),
                'rating_trend': self._calculate_trend(quarterly_stats['rating_mean'])
            },
            'quarterly_breakdown': quarterly_stats.to_dict('index'),
            'yearly_analysis': yearly_analysis,
            'roi_analysis': roi_analysis,
            'recommendations': self._generate_quarterly_recommendations(quarterly_stats, yearly_analysis, roi_analysis)
        }
        
        return report
    
    def generate_comprehensive_report(self, restaurant_id):
        """
        Генерирует комплексный отчет со всеми периодами
        """
        print(f"📊 ГЕНЕРАЦИЯ КОМПЛЕКСНОГО ОТЧЕТА")
        print("=" * 60)
        
        weekly = self.generate_weekly_report(restaurant_id, 4)
        monthly = self.generate_monthly_report(restaurant_id, 3)
        quarterly = self.generate_quarterly_report(restaurant_id, 4)
        
        comprehensive = {
            'restaurant_name': weekly.get('restaurant_name', 'Unknown'),
            'restaurant_id': restaurant_id,
            'generated_at': datetime.now().isoformat(),
            'weekly_report': weekly,
            'monthly_report': monthly,
            'quarterly_report': quarterly,
            'executive_summary': self._generate_executive_summary(weekly, monthly, quarterly)
        }
        
        return comprehensive
    
    def _aggregate_weekly_insights(self, daily_insights):
        """Агрегирует ML инсайты за неделю"""
        if not daily_insights:
            return {}
        
        # Собираем все факторы
        all_factors = {}
        for insight in daily_insights:
            if 'top_factors' in insight:
                for factor, impact in insight['top_factors'].items():
                    if factor not in all_factors:
                        all_factors[factor] = []
                    all_factors[factor].append(impact)
        
        # Усредняем факторы
        avg_factors = {k: np.mean(v) for k, v in all_factors.items()}
        
        return {
            'avg_factors': avg_factors,
            'days_analyzed': len(daily_insights),
            'dominant_factor': max(avg_factors.items(), key=lambda x: abs(x[1])) if avg_factors else None
        }
    
    def _calculate_trend(self, series):
        """Вычисляет тренд временного ряда"""
        if len(series) < 2:
            return "insufficient_data"
        
        # Простая линейная регрессия
        x = np.arange(len(series))
        y = series.values
        
        # Удаляем NaN
        mask = ~np.isnan(y)
        if mask.sum() < 2:
            return "insufficient_data"
        
        x_clean = x[mask]
        y_clean = y[mask]
        
        slope = np.polyfit(x_clean, y_clean, 1)[0]
        
        if slope > 0.05:
            return "growing"
        elif slope < -0.05:
            return "declining"
        else:
            return "stable"
    
    def _analyze_seasonal_patterns(self, data):
        """Анализирует сезонные паттерны"""
        data['month'] = data['date'].dt.month
        data['day_of_week'] = data['date'].dt.dayofweek
        
        monthly_avg = data.groupby('month')['sales'].mean()
        weekly_avg = data.groupby('day_of_week')['sales'].mean()
        
        return {
            'best_month': monthly_avg.idxmax(),
            'worst_month': monthly_avg.idxmin(),
            'best_day_of_week': weekly_avg.idxmax(),
            'worst_day_of_week': weekly_avg.idxmin(),
            'monthly_pattern': monthly_avg.to_dict(),
            'weekly_pattern': weekly_avg.to_dict()
        }
    
    def _analyze_yearly_patterns(self, data):
        """Анализирует годовые паттерны"""
        data['year'] = data['date'].dt.year
        yearly_stats = data.groupby('year').agg({
            'sales': ['sum', 'mean'],
            'orders': ['sum', 'mean'],
            'rating': 'mean'
        })
        
        return {
            'yearly_growth': yearly_stats['sales']['sum'].pct_change().iloc[-1] * 100 if len(yearly_stats) > 1 else 0,
            'yearly_stats': yearly_stats.to_dict()
        }
    
    def _calculate_roi_analysis(self, quarterly_stats):
        """Вычисляет ROI анализ"""
        if 'ads_spend_sum' not in quarterly_stats.columns:
            return {}
        
        quarterly_stats['roi'] = (quarterly_stats['sales_sum'] / quarterly_stats['ads_spend_sum'].replace(0, 1)) * 100
        
        return {
            'avg_roi': quarterly_stats['roi'].mean(),
            'best_roi_quarter': quarterly_stats['roi'].idxmax(),
            'worst_roi_quarter': quarterly_stats['roi'].idxmin(),
            'roi_trend': self._calculate_trend(quarterly_stats['roi'])
        }
    
    def _calculate_growth_rate(self, series):
        """Вычисляет совокупный темп роста"""
        if len(series) < 2:
            return 0
        
        first_value = series.iloc[0]
        last_value = series.iloc[-1]
        
        if first_value == 0:
            return 0
        
        periods = len(series) - 1
        growth_rate = ((last_value / first_value) ** (1/periods) - 1) * 100
        
        return growth_rate
    
    def _generate_weekly_recommendations(self, weekly_stats, ml_insights):
        """Генерирует рекомендации на основе недельного анализа"""
        recommendations = []
        
        # Анализ трендов
        if 'sales_change' in weekly_stats.columns:
            avg_change = weekly_stats['sales_change'].mean()
            if avg_change < -5:
                recommendations.append("⚠️ Продажи снижаются. Рассмотрите увеличение маркетинговых активностей")
            elif avg_change > 10:
                recommendations.append("📈 Отличный рост! Масштабируйте успешные стратегии")
        
        # Анализ рейтинга
        if 'rating_mean' in weekly_stats.columns:
            avg_rating = weekly_stats['rating_mean'].mean()
            if avg_rating < 4.0:
                recommendations.append("⭐ Рейтинг ниже 4.0. Улучшите качество обслуживания")
        
        # ML инсайты
        if ml_insights:
            dominant_factors = {}
            for insight in ml_insights:
                if 'avg_factors' in insight:
                    for factor, impact in insight['avg_factors'].items():
                        if factor not in dominant_factors:
                            dominant_factors[factor] = []
                        dominant_factors[factor].append(impact)
            
            for factor, impacts in dominant_factors.items():
                avg_impact = np.mean(impacts)
                if abs(avg_impact) > 0.05:
                    if 'rain' in factor and avg_impact < 0:
                        recommendations.append("🌧️ Дождь сильно влияет на продажи. Подготовьте промо для плохой погоды")
                    elif 'ads' in factor and avg_impact < 0:
                        recommendations.append("📱 Проблемы с рекламой. Проверьте кампании")
        
        return recommendations
    
    def _generate_monthly_recommendations(self, monthly_stats, seasonal_analysis):
        """Генерирует рекомендации на основе месячного анализа"""
        recommendations = []
        
        # Сезонные рекомендации
        if seasonal_analysis:
            best_month = seasonal_analysis.get('best_month')
            worst_month = seasonal_analysis.get('worst_month')
            
            if best_month and worst_month:
                recommendations.append(f"📅 Лучший месяц: {best_month}. Подготовьтесь к повышенному спросу")
                recommendations.append(f"📅 Слабый месяц: {worst_month}. Запланируйте промо-акции")
        
        # Анализ волатильности
        if 'sales_sum' in monthly_stats.columns:
            volatility = monthly_stats['sales_sum'].std()
            mean_sales = monthly_stats['sales_sum'].mean()
            cv = volatility / mean_sales if mean_sales > 0 else 0
            
            if cv > 0.3:
                recommendations.append("📊 Высокая волатильность продаж. Улучшите прогнозирование")
        
        return recommendations
    
    def _generate_quarterly_recommendations(self, quarterly_stats, yearly_analysis, roi_analysis):
        """Генерирует рекомендации на основе квартального анализа"""
        recommendations = []
        
        # ROI анализ
        if roi_analysis and 'avg_roi' in roi_analysis:
            avg_roi = roi_analysis['avg_roi']
            if avg_roi < 200:
                recommendations.append("💰 Низкий ROI рекламы. Оптимизируйте рекламные кампании")
            elif avg_roi > 500:
                recommendations.append("🎯 Отличный ROI! Увеличьте рекламный бюджет")
        
        # Годовой рост
        if yearly_analysis and 'yearly_growth' in yearly_analysis:
            growth = yearly_analysis['yearly_growth']
            if growth < 0:
                recommendations.append("📉 Отрицательный годовой рост. Требуется стратегический пересмотр")
            elif growth > 20:
                recommendations.append("🚀 Отличный рост! Рассмотрите расширение")
        
        return recommendations
    
    def _generate_executive_summary(self, weekly, monthly, quarterly):
        """Генерирует исполнительное резюме"""
        return {
            'key_metrics': {
                'weekly_sales_trend': weekly.get('trends', {}).get('sales_trend', 'unknown'),
                'monthly_sales_trend': monthly.get('trends', {}).get('sales_trend', 'unknown'),
                'quarterly_sales_trend': quarterly.get('trends', {}).get('sales_trend', 'unknown'),
                'overall_health': self._assess_overall_health(weekly, monthly, quarterly)
            },
            'critical_actions': self._identify_critical_actions(weekly, monthly, quarterly),
            'opportunities': self._identify_opportunities(weekly, monthly, quarterly)
        }
    
    def _assess_overall_health(self, weekly, monthly, quarterly):
        """Оценивает общее состояние ресторана"""
        trends = [
            weekly.get('trends', {}).get('sales_trend', 'stable'),
            monthly.get('trends', {}).get('sales_trend', 'stable'),
            quarterly.get('trends', {}).get('sales_trend', 'stable')
        ]
        
        growing_count = trends.count('growing')
        declining_count = trends.count('declining')
        
        if growing_count >= 2:
            return "excellent"
        elif declining_count >= 2:
            return "concerning"
        else:
            return "stable"
    
    def _identify_critical_actions(self, weekly, monthly, quarterly):
        """Определяет критические действия"""
        actions = []
        
        # Собираем все рекомендации
        all_recommendations = []
        all_recommendations.extend(weekly.get('recommendations', []))
        all_recommendations.extend(monthly.get('recommendations', []))
        all_recommendations.extend(quarterly.get('recommendations', []))
        
        # Фильтруем критические
        critical_keywords = ['⚠️', '📉', '❌', 'снижаются', 'низкий', 'проблемы']
        for rec in all_recommendations:
            if any(keyword in rec for keyword in critical_keywords):
                actions.append(rec)
        
        return actions[:5]  # Топ 5 критических действий
    
    def _identify_opportunities(self, weekly, monthly, quarterly):
        """Определяет возможности роста"""
        opportunities = []
        
        # Собираем все рекомендации
        all_recommendations = []
        all_recommendations.extend(weekly.get('recommendations', []))
        all_recommendations.extend(monthly.get('recommendations', []))
        all_recommendations.extend(quarterly.get('recommendations', []))
        
        # Фильтруем возможности
        opportunity_keywords = ['📈', '🚀', '🎯', '💰', 'отличный', 'увеличьте', 'масштабируйте']
        for rec in all_recommendations:
            if any(keyword in rec for keyword in opportunity_keywords):
                opportunities.append(rec)
        
        return opportunities[:5]  # Топ 5 возможностей
    
    def save_report_to_json(self, report, filename):
        """Сохраняет отчет в JSON файл"""
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(report, f, ensure_ascii=False, indent=2, default=str)
        print(f"✅ Отчет сохранен в {filename}")
    
    def close(self):
        """Закрывает соединение с базой данных"""
        self.adapter.close()

# Пример использования
if __name__ == "__main__":
    # Создаем генератор отчетов
    generator = ReportsGenerator("path_to_your_database.db")
    
    # Генерируем отчеты для ресторана
    restaurant_id = 1
    
    print("📊 Генерация недельного отчета...")
    weekly_report = generator.generate_weekly_report(restaurant_id)
    generator.save_report_to_json(weekly_report, f"weekly_report_{restaurant_id}.json")
    
    print("\n📊 Генерация месячного отчета...")
    monthly_report = generator.generate_monthly_report(restaurant_id)
    generator.save_report_to_json(monthly_report, f"monthly_report_{restaurant_id}.json")
    
    print("\n📊 Генерация квартального отчета...")
    quarterly_report = generator.generate_quarterly_report(restaurant_id)
    generator.save_report_to_json(quarterly_report, f"quarterly_report_{restaurant_id}.json")
    
    print("\n📊 Генерация комплексного отчета...")
    comprehensive_report = generator.generate_comprehensive_report(restaurant_id)
    generator.save_report_to_json(comprehensive_report, f"comprehensive_report_{restaurant_id}.json")
    
    generator.close()
    print("\n✅ Все отчеты сгенерированы!")