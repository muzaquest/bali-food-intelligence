#!/usr/bin/env python3
"""
Система бизнес-аналитики для ресторанов
Автоматический анализ продаж с рекомендациями для принятия решений
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import json
import logging
from typing import Dict, List, Optional, Tuple

from data_loader import load_data_for_training, get_restaurant_data

logger = logging.getLogger(__name__)

class BusinessIntelligenceSystem:
    """
    Система бизнес-аналитики для автоматического анализа продаж ресторанов
    
    Цели:
    - Автоматизировать аналитику без участия аналитика
    - Получать рекомендации что делать
    - Понимать причины изменения продаж
    - Давать понятные отчеты менеджерам
    """
    
    def __init__(self):
        pass
        
    def analyze_sales_change(self, restaurant_name: str, date: str, 
                           period_days: int = 7) -> Dict:
        """
        Анализ изменения продаж с рекомендациями
        
        Args:
            restaurant_name: Название ресторана
            date: Дата анализа
            period_days: Количество дней для анализа тренда
            
        Returns:
            Подробный отчет с причинами и рекомендациями
        """
        logger.info(f"Анализ продаж для {restaurant_name} на {date}")
        
        try:
            # Загружаем данные ресторана
            df = get_restaurant_data(restaurant_name)
            if df is None:
                return {"error": f"Нет данных для ресторана {restaurant_name}"}
            
            # Получаем данные за период
            target_date = pd.to_datetime(date)
            period_start = target_date - timedelta(days=period_days)
            period_df = df[
                (pd.to_datetime(df['date']) >= period_start) & 
                (pd.to_datetime(df['date']) <= target_date)
            ].copy()
            
            if period_df.empty:
                return {"error": f"Нет данных за период {period_start} - {target_date}"}
            
            # Анализируем изменения
            analysis = self._analyze_period_changes(period_df, restaurant_name, date)
            
            # Получаем рекомендации
            recommendations = self._generate_recommendations(analysis)
            
            # Создаем итоговый отчет
            report = {
                "restaurant_name": restaurant_name,
                "analysis_date": date,
                "period_analyzed": f"{period_start.strftime('%Y-%m-%d')} - {date}",
                "summary": analysis["summary"],
                "key_factors": analysis["key_factors"],
                "recommendations": recommendations,
                "detailed_analysis": analysis["detailed_analysis"],
                "charts": analysis.get("charts", []),
                "timestamp": datetime.now().isoformat()
            }
            
            return report
            
        except Exception as e:
            logger.error(f"Ошибка анализа продаж: {e}")
            return {"error": str(e)}
    
    def _analyze_period_changes(self, df: pd.DataFrame, restaurant_name: str, 
                              target_date: str) -> Dict:
        """Анализ изменений за период"""
        
        # Сортируем по дате
        df = df.sort_values('date').reset_index(drop=True)
        
        # Получаем данные за последние дни
        latest_data = df.tail(3)  # Последние 3 дня
        earlier_data = df.head(len(df) - 3)  # Предыдущие дни
        
        # Рассчитываем изменения
        latest_sales = latest_data['total_sales'].mean()
        earlier_sales = earlier_data['total_sales'].mean()
        
        if earlier_sales > 0:
            sales_change = ((latest_sales - earlier_sales) / earlier_sales) * 100
        else:
            sales_change = 0
        
        # Анализируем факторы
        factors_analysis = self._analyze_factors(df, latest_data, earlier_data)
        
        # Создаем сводку
        summary = {
            "sales_change_percent": round(sales_change, 1),
            "sales_trend": "рост" if sales_change > 0 else "спад" if sales_change < -2 else "стабильно",
            "latest_period_sales": round(latest_sales, 0),
            "earlier_period_sales": round(earlier_sales, 0),
            "absolute_change": round(latest_sales - earlier_sales, 0)
        }
        
        return {
            "summary": summary,
            "key_factors": factors_analysis["key_factors"],
            "detailed_analysis": factors_analysis["detailed_analysis"]
        }
    
    def _analyze_factors(self, df: pd.DataFrame, latest_data: pd.DataFrame, 
                        earlier_data: pd.DataFrame) -> Dict:
        """Анализ факторов влияния на продажи"""
        
        factors = {}
        key_factors = []
        
        # 1. Реклама
        if 'ads_on' in latest_data.columns and 'ads_on' in earlier_data.columns:
            latest_ads = latest_data['ads_on'].mean()
            earlier_ads = earlier_data['ads_on'].mean()
            ads_change = latest_ads - earlier_ads
            
            if abs(ads_change) > 0.1:
                factor = {
                    "factor": "Реклама",
                    "change": "включена" if ads_change > 0 else "отключена",
                    "impact": "положительный" if ads_change > 0 else "негативный",
                    "confidence": "высокая"
                }
                factors["advertising"] = factor
                key_factors.append(factor)
        
        # 2. Рейтинг
        if 'rating' in latest_data.columns and 'rating' in earlier_data.columns:
            latest_rating = latest_data['rating'].mean()
            earlier_rating = earlier_data['rating'].mean()
            rating_change = latest_rating - earlier_rating
            
            if abs(rating_change) > 0.1:
                factor = {
                    "factor": "Рейтинг",
                    "change": f"{rating_change:+.1f}",
                    "impact": "положительный" if rating_change > 0 else "негативный",
                    "confidence": "высокая"
                }
                factors["rating"] = factor
                key_factors.append(factor)
        
        # 3. Отмены
        if 'cancel_rate' in latest_data.columns and 'cancel_rate' in earlier_data.columns:
            latest_cancels = latest_data['cancel_rate'].mean()
            earlier_cancels = earlier_data['cancel_rate'].mean()
            cancel_change = latest_cancels - earlier_cancels
            
            if abs(cancel_change) > 0.02:
                factor = {
                    "factor": "Отмены заказов",
                    "change": f"{cancel_change:+.1%}",
                    "impact": "негативный" if cancel_change > 0 else "положительный",
                    "confidence": "высокая"
                }
                factors["cancellations"] = factor
                key_factors.append(factor)
        
        # 4. Погода
        if 'temp_c' in latest_data.columns and 'rain_mm' in latest_data.columns:
            latest_weather = latest_data[['temp_c', 'rain_mm']].mean()
            earlier_weather = earlier_data[['temp_c', 'rain_mm']].mean()
            
            temp_change = latest_weather['temp_c'] - earlier_weather['temp_c']
            rain_change = latest_weather['rain_mm'] - earlier_weather['rain_mm']
            
            if abs(temp_change) > 3 or abs(rain_change) > 5:
                weather_impact = "негативный" if rain_change > 5 or abs(temp_change) > 5 else "нейтральный"
                factor = {
                    "factor": "Погода",
                    "change": f"температура {temp_change:+.1f}°C, дождь {rain_change:+.1f}мм",
                    "impact": weather_impact,
                    "confidence": "средняя"
                }
                factors["weather"] = factor
                if weather_impact == "негативный":
                    key_factors.append(factor)
        
        # 5. Праздники
        if 'is_holiday' in latest_data.columns and 'is_holiday' in earlier_data.columns:
            latest_holidays = latest_data['is_holiday'].sum()
            earlier_holidays = earlier_data['is_holiday'].sum()
            
            if latest_holidays > 0 or earlier_holidays > 0:
                factor = {
                    "factor": "Праздники",
                    "change": f"праздничных дней: {latest_holidays}",
                    "impact": "негативный" if latest_holidays > 0 else "нейтральный",
                    "confidence": "высокая"
                }
                factors["holidays"] = factor
                if latest_holidays > 0:
                    key_factors.append(factor)
        
        # 6. День недели
        try:
            latest_weekends = latest_data['date'].apply(lambda x: pd.to_datetime(x).weekday() >= 5).sum()
            weekend_effect = "положительный" if latest_weekends > 0 else "нейтральный"
            
            if latest_weekends > 0:
                factor = {
                    "factor": "Выходные дни",
                    "change": f"выходных дней: {latest_weekends}",
                    "impact": weekend_effect,
                    "confidence": "средняя"
                }
                factors["weekends"] = factor
        except:
            pass
        
        return {
            "key_factors": key_factors[:5],  # Топ-5 факторов
            "detailed_analysis": factors
        }
    
    def _generate_recommendations(self, analysis: Dict) -> List[Dict]:
        """Генерация рекомендаций на основе анализа"""
        
        recommendations = []
        summary = analysis["summary"]
        factors = analysis["detailed_analysis"]
        
        # Рекомендации по рекламе
        if "advertising" in factors:
            ads_factor = factors["advertising"]
            if ads_factor["change"] == "отключена":
                recommendations.append({
                    "priority": "ВЫСОКИЙ",
                    "category": "Реклама",
                    "action": "Включить рекламу",
                    "description": "Реклама была отключена, что негативно повлияло на продажи",
                    "expected_impact": "Увеличение продаж на 15-25%",
                    "implementation": "Включить рекламу на Gojek и Grab в течение 24 часов"
                })
            elif ads_factor["change"] == "включена" and summary["sales_trend"] == "рост":
                recommendations.append({
                    "priority": "СРЕДНИЙ",
                    "category": "Реклама",
                    "action": "Увеличить рекламный бюджет",
                    "description": "Реклама показывает хорошие результаты",
                    "expected_impact": "Дополнительное увеличение продаж на 10-15%",
                    "implementation": "Увеличить дневной бюджет на 20-30%"
                })
        
        # Рекомендации по рейтингу
        if "rating" in factors:
            rating_factor = factors["rating"]
            if rating_factor["impact"] == "негативный":
                recommendations.append({
                    "priority": "ВЫСОКИЙ",
                    "category": "Качество сервиса",
                    "action": "Улучшить качество обслуживания",
                    "description": "Снижение рейтинга негативно влияет на продажи",
                    "expected_impact": "Восстановление продаж на 10-20%",
                    "implementation": "Проверить качество еды, скорость доставки, работу с клиентами"
                })
        
        # Рекомендации по отменам
        if "cancellations" in factors:
            cancel_factor = factors["cancellations"]
            if cancel_factor["impact"] == "негативный":
                recommendations.append({
                    "priority": "ВЫСОКИЙ",
                    "category": "Логистика",
                    "action": "Сократить время доставки",
                    "description": "Высокий процент отмен из-за долгой доставки",
                    "expected_impact": "Снижение отмен на 30-50%",
                    "implementation": "Оптимизировать маршруты, добавить курьеров в пиковые часы"
                })
        
        # Рекомендации по погоде
        if "weather" in factors:
            weather_factor = factors["weather"]
            if weather_factor["impact"] == "негативный":
                recommendations.append({
                    "priority": "НИЗКИЙ",
                    "category": "Маркетинг",
                    "action": "Адаптировать предложения под погоду",
                    "description": "Плохая погода снижает заказы",
                    "expected_impact": "Частичная компенсация погодного эффекта",
                    "implementation": "Предложить горячие напитки в дождь, холодные в жару"
                })
        
        # Общие рекомендации
        if summary["sales_trend"] == "спад":
            recommendations.append({
                "priority": "СРЕДНИЙ",
                "category": "Общие меры",
                "action": "Комплексный анализ операций",
                "description": "Продажи снижаются, нужен детальный анализ",
                "expected_impact": "Выявление скрытых проблем",
                "implementation": "Проанализировать меню, цены, конкурентов, отзывы клиентов"
            })
        
        # Сортируем по приоритету
        priority_order = {"ВЫСОКИЙ": 1, "СРЕДНИЙ": 2, "НИЗКИЙ": 3}
        recommendations.sort(key=lambda x: priority_order.get(x["priority"], 4))
        
        return recommendations
    
    def generate_weekly_report(self, restaurant_name: str, weeks_back: int = 4) -> Dict:
        """Генерация недельного отчета"""
        
        logger.info(f"Генерация недельного отчета для {restaurant_name}")
        
        try:
            # Загружаем данные
            df = get_restaurant_data(restaurant_name)
            if df is None:
                return {"error": f"Нет данных для ресторана {restaurant_name}"}
            
            # Определяем период
            df['date'] = pd.to_datetime(df['date'])
            end_date = df['date'].max()
            start_date = end_date - timedelta(weeks=weeks_back)
            
            # Фильтруем данные
            period_df = df[df['date'] >= start_date].copy()
            
            # Группируем по неделям
            period_df['week'] = period_df['date'].dt.isocalendar().week
            period_df['year'] = period_df['date'].dt.year
            period_df['week_start'] = period_df['date'].dt.to_period('W').dt.start_time
            
            weekly_stats = period_df.groupby('week_start').agg({
                'total_sales': ['sum', 'mean'],
                'orders': 'sum',
                'rating': 'mean',
                'cancel_rate': 'mean',
                'ads_on': 'mean'
            }).round(2)
            
            # Анализируем тренды
            sales_trend = self._analyze_weekly_trends(weekly_stats)
            
            # Преобразуем weekly_stats в JSON-совместимый формат
            weekly_breakdown = {}
            for week_start in weekly_stats.index:
                week_key = week_start.strftime('%Y-%m-%d')
                weekly_breakdown[week_key] = {
                    'total_sales': float(weekly_stats.loc[week_start, ('total_sales', 'sum')]),
                    'avg_daily_sales': float(weekly_stats.loc[week_start, ('total_sales', 'mean')]),
                    'total_orders': int(weekly_stats.loc[week_start, ('orders', 'sum')]),
                    'avg_rating': float(weekly_stats.loc[week_start, ('rating', 'mean')]),
                    'avg_cancel_rate': float(weekly_stats.loc[week_start, ('cancel_rate', 'mean')]),
                    'avg_ads_on': float(weekly_stats.loc[week_start, ('ads_on', 'mean')])
                }
            
            # Создаем отчет
            report = {
                "restaurant_name": restaurant_name,
                "report_type": "weekly",
                "period": f"{start_date.strftime('%Y-%m-%d')} - {end_date.strftime('%Y-%m-%d')}",
                "weeks_analyzed": weeks_back,
                "summary": {
                    "total_sales": float(weekly_stats[('total_sales', 'sum')].sum()),
                    "average_weekly_sales": float(weekly_stats[('total_sales', 'sum')].mean()),
                    "total_orders": int(weekly_stats[('orders', 'sum')].sum()),
                    "average_rating": float(weekly_stats[('rating', 'mean')].mean()),
                    "average_cancel_rate": float(weekly_stats[('cancel_rate', 'mean')].mean()),
                },
                "trends": sales_trend,
                "weekly_breakdown": weekly_breakdown,
                "recommendations": self._generate_weekly_recommendations(sales_trend),
                "timestamp": datetime.now().isoformat()
            }
            
            return report
            
        except Exception as e:
            logger.error(f"Ошибка генерации недельного отчета: {e}")
            return {"error": str(e)}
    
    def _analyze_weekly_trends(self, weekly_stats: pd.DataFrame) -> Dict:
        """Анализ недельных трендов"""
        
        sales_data = weekly_stats[('total_sales', 'sum')]
        
        # Тренд продаж
        if len(sales_data) >= 2:
            recent_weeks = sales_data.tail(2).mean()
            earlier_weeks = sales_data.head(len(sales_data) - 2).mean()
            if earlier_weeks > 0:
                trend_change = ((recent_weeks - earlier_weeks) / earlier_weeks) * 100
            else:
                trend_change = 0
        else:
            trend_change = 0
        
        # Волатильность
        if sales_data.mean() > 0:
            volatility = sales_data.std() / sales_data.mean() * 100
        else:
            volatility = 0
        
        return {
            "sales_trend_percent": round(trend_change, 1),
            "trend_direction": "рост" if trend_change > 5 else "спад" if trend_change < -5 else "стабильно",
            "volatility_percent": round(volatility, 1),
            "stability": "стабильные" if volatility < 15 else "нестабильные" if volatility > 30 else "умеренные"
        }
    
    def _generate_weekly_recommendations(self, trends: Dict) -> List[Dict]:
        """Генерация рекомендаций на основе недельных трендов"""
        
        recommendations = []
        
        if trends["trend_direction"] == "спад":
            recommendations.append({
                "priority": "ВЫСОКИЙ",
                "category": "Стратегия",
                "action": "Срочные меры по восстановлению продаж",
                "description": f"Продажи снижаются на {abs(trends['sales_trend_percent'])}% в неделю",
                "implementation": "Проанализировать конкурентов, обновить меню, запустить акции"
            })
        
        if trends["stability"] == "нестабильные":
            recommendations.append({
                "priority": "СРЕДНИЙ",
                "category": "Операции",
                "action": "Стабилизировать операционные процессы",
                "description": f"Высокая волатильность продаж ({trends['volatility_percent']}%)",
                "implementation": "Стандартизировать процессы, улучшить планирование"
            })
        
        return recommendations
    
    def test_hypothesis(self, restaurant_name: str, hypothesis: str, 
                       start_date: str, end_date: str) -> Dict:
        """Тестирование гипотез"""
        
        logger.info(f"Тестирование гипотезы для {restaurant_name}: {hypothesis}")
        
        try:
            # Загружаем данные
            df = get_restaurant_data(restaurant_name)
            if df is None:
                return {"error": f"Нет данных для ресторана {restaurant_name}"}
            
            # Фильтруем по периоду
            df['date'] = pd.to_datetime(df['date'])
            period_df = df[
                (df['date'] >= start_date) & 
                (df['date'] <= end_date)
            ].copy()
            
            # Анализируем гипотезу
            if "реклама" in hypothesis.lower() or "ads" in hypothesis.lower():
                result = self._test_advertising_hypothesis(period_df)
            else:
                result = {"conclusion": "Неизвестный тип гипотезы"}
            
            return {
                "restaurant_name": restaurant_name,
                "hypothesis": hypothesis,
                "period": f"{start_date} - {end_date}",
                "result": result,
                "timestamp": datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Ошибка тестирования гипотезы: {e}")
            return {"error": str(e)}
    
    def _test_advertising_hypothesis(self, df: pd.DataFrame) -> Dict:
        """Тестирование гипотезы о влиянии рекламы"""
        
        # Разделяем данные на периоды с рекламой и без
        ads_on_data = df[df['ads_on'] > 0.5]
        ads_off_data = df[df['ads_on'] <= 0.5]
        
        if len(ads_on_data) == 0 or len(ads_off_data) == 0:
            return {"conclusion": "Недостаточно данных для сравнения"}
        
        # Сравниваем продажи
        ads_on_sales = ads_on_data['total_sales'].mean()
        ads_off_sales = ads_off_data['total_sales'].mean()
        
        if ads_off_sales > 0:
            improvement = ((ads_on_sales - ads_off_sales) / ads_off_sales) * 100
        else:
            improvement = 0
        
        return {
            "conclusion": "Реклама эффективна" if improvement > 10 else "Реклама малоэффективна",
            "improvement_percent": round(improvement, 1),
            "ads_on_average_sales": round(ads_on_sales, 0),
            "ads_off_average_sales": round(ads_off_sales, 0),
            "confidence": "высокая" if abs(improvement) > 15 else "средняя"
        }
    
    def generate_executive_summary(self, restaurant_name: str) -> Dict:
        """Генерация краткого отчета для руководства"""
        
        logger.info(f"Генерация краткого отчета для {restaurant_name}")
        
        try:
            # Загружаем данные для определения доступного периода
            df = get_restaurant_data(restaurant_name)
            if df is None:
                return {"error": f"Нет данных для ресторана {restaurant_name}"}
            
            # Используем последнюю доступную дату из данных
            df['date'] = pd.to_datetime(df['date'])
            end_date = df['date'].max().strftime('%Y-%m-%d')
            start_date = (df['date'].max() - timedelta(days=30)).strftime('%Y-%m-%d')
            
            # Получаем детальный анализ
            detailed_analysis = self.analyze_sales_change(restaurant_name, end_date, 30)
            
            if "error" in detailed_analysis:
                return detailed_analysis
            
            # Создаем краткий отчет
            summary = {
                "restaurant_name": restaurant_name,
                "period": "Последние 30 дней",
                "analysis_period": f"{start_date} - {end_date}",
                "key_metrics": detailed_analysis["summary"],
                "top_3_factors": detailed_analysis["key_factors"][:3],
                "priority_actions": [
                    rec for rec in detailed_analysis["recommendations"] 
                    if rec["priority"] == "ВЫСОКИЙ"
                ][:3],
                "overall_status": self._determine_overall_status(detailed_analysis),
                "timestamp": datetime.now().isoformat()
            }
            
            return summary
            
        except Exception as e:
            logger.error(f"Ошибка генерации краткого отчета: {e}")
            return {"error": str(e)}
    
    def _determine_overall_status(self, analysis: Dict) -> str:
        """Определение общего статуса ресторана"""
        
        summary = analysis["summary"]
        sales_change = summary["sales_change_percent"]
        
        if sales_change > 10:
            return "ОТЛИЧНО - Сильный рост продаж"
        elif sales_change > 5:
            return "ХОРОШО - Умеренный рост продаж"
        elif sales_change > -5:
            return "НОРМАЛЬНО - Стабильные продажи"
        elif sales_change > -15:
            return "ТРЕБУЕТ ВНИМАНИЯ - Снижение продаж"
        else:
            return "КРИТИЧНО - Значительное падение продаж"

# Функции для удобного использования

def analyze_restaurant_performance(restaurant_name: str, date: str = None) -> Dict:
    """Анализ эффективности ресторана"""
    if date is None:
        date = datetime.now().strftime('%Y-%m-%d')
    
    bi_system = BusinessIntelligenceSystem()
    return bi_system.analyze_sales_change(restaurant_name, date)

def get_weekly_report(restaurant_name: str, weeks: int = 4) -> Dict:
    """Получение недельного отчета"""
    bi_system = BusinessIntelligenceSystem()
    return bi_system.generate_weekly_report(restaurant_name, weeks)

def get_executive_summary(restaurant_name: str) -> Dict:
    """Получение краткого отчета для руководства"""
    bi_system = BusinessIntelligenceSystem()
    return bi_system.generate_executive_summary(restaurant_name)

def test_business_hypothesis(restaurant_name: str, hypothesis: str, 
                           days_back: int = 30) -> Dict:
    """Тестирование бизнес-гипотезы"""
    bi_system = BusinessIntelligenceSystem()
    
    # Загружаем данные для определения доступного периода
    df = get_restaurant_data(restaurant_name)
    if df is None:
        return {"error": f"Нет данных для ресторана {restaurant_name}"}
    
    # Используем последнюю доступную дату из данных
    df['date'] = pd.to_datetime(df['date'])
    end_date = df['date'].max().strftime('%Y-%m-%d')
    start_date = (df['date'].max() - timedelta(days=days_back)).strftime('%Y-%m-%d')
    
    return bi_system.test_hypothesis(restaurant_name, hypothesis, start_date, end_date)