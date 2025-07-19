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

class AdvancedAnalyticsEngine:
    """
    Продвинутый аналитический движок для поиска скрытых взаимосвязей и аномалий
    
    Цель: Заменить ручной анализ клиента автоматическим поиском инсайтов
    """
    
    def __init__(self):
        self.correlation_threshold = 0.3
        self.anomaly_threshold = 2.0  # Z-score для определения аномалий
        
    def find_sales_anomalies(self, df: pd.DataFrame, restaurant_name: str) -> List[Dict]:
        """Поиск аномалий в продажах с анализом причин"""
        anomalies = []
        
        # Вычисляем Z-score для продаж
        df['sales_zscore'] = np.abs((df['total_sales'] - df['total_sales'].mean()) / df['total_sales'].std())
        
        # Находим аномальные дни
        anomaly_days = df[df['sales_zscore'] > self.anomaly_threshold].copy()
        
        for _, day in anomaly_days.iterrows():
            anomaly = {
                'date': day['date'].strftime('%Y-%m-%d'),
                'sales': day['total_sales'],
                'deviation': f"{((day['total_sales'] - df['total_sales'].mean()) / df['total_sales'].mean()) * 100:+.1f}%",
                'possible_causes': []
            }
            
            # Анализируем возможные причины
            
            # 1. Погодные условия
            if day['rain_mm'] > 20:
                anomaly['possible_causes'].append(f"Сильный дождь ({day['rain_mm']:.1f}мм) - могли отменить заказы")
            elif day['temp_c'] > 32:
                anomaly['possible_causes'].append(f"Очень жарко ({day['temp_c']:.1f}°C) - люди реже заказывают")
            elif day['temp_c'] < 26:
                anomaly['possible_causes'].append(f"Прохладно ({day['temp_c']:.1f}°C) - могли заказывать больше горячего")
            
            # 2. Праздники
            if day['is_holiday']:
                holiday_name = day.get('holiday_name', 'Праздник')
                if day['total_sales'] > df['total_sales'].mean():
                    anomaly['possible_causes'].append(f"Праздник ({holiday_name}) - увеличенный спрос")
                else:
                    anomaly['possible_causes'].append(f"Праздник ({holiday_name}) - возможно закрыты или мало водителей")
            
            # 3. День недели
            weekday_avg = df[df['day_of_week'] == day['day_of_week']]['total_sales'].mean()
            if abs(day['total_sales'] - weekday_avg) > weekday_avg * 0.3:
                weekdays = ['Пн', 'Вт', 'Ср', 'Чт', 'Пт', 'Сб', 'Вс']
                day_name = weekdays[int(day['day_of_week'])]
                anomaly['possible_causes'].append(f"Нетипично для {day_name} (обычно {weekday_avg:,.0f} IDR)")
            
            # 4. Рейтинг
            if day['rating'] < 4.5:
                anomaly['possible_causes'].append(f"Низкий рейтинг ({day['rating']:.2f}) - могли потерять клиентов")
            
            # 5. Реклама
            if day['ads_on'] == 0:
                anomaly['possible_causes'].append("Реклама была отключена")
            elif day['roas'] < 5:
                anomaly['possible_causes'].append(f"Низкий ROAS ({day['roas']:.1f}) - неэффективная реклама")
            
            # 6. Отмены заказов
            if day['cancel_rate'] > 0.1:
                anomaly['possible_causes'].append(f"Высокий процент отмен ({day['cancel_rate']*100:.1f}%) - проблемы с кухней/доставкой")
            
            anomalies.append(anomaly)
        
        return sorted(anomalies, key=lambda x: abs(float(x['deviation'].replace('%', '').replace('+', ''))), reverse=True)
    
    def find_correlations(self, df: pd.DataFrame) -> Dict[str, List[Dict]]:
        """Поиск скрытых корреляций между факторами"""
        correlations = {
            'strong_positive': [],
            'strong_negative': [],
            'interesting_patterns': []
        }
        
        # Основные числовые колонки для анализа корреляций
        numeric_cols = ['total_sales', 'orders', 'rating', 'temp_c', 'rain_mm', 
                       'ads_on', 'roas', 'cancel_rate', 'day_of_week']
        
        # Вычисляем корреляционную матрицу
        correlation_matrix = df[numeric_cols].corr()
        
        # Анализируем сильные корреляции
        for i, col1 in enumerate(numeric_cols):
            for j, col2 in enumerate(numeric_cols):
                if i < j:  # Избегаем дублирования
                    corr_value = correlation_matrix.loc[col1, col2]
                    
                    if abs(corr_value) > self.correlation_threshold:
                        correlation_info = {
                            'factor1': col1,
                            'factor2': col2,
                            'correlation': corr_value,
                            'strength': self._get_correlation_strength(abs(corr_value)),
                            'interpretation': self._interpret_correlation(col1, col2, corr_value)
                        }
                        
                        if corr_value > self.correlation_threshold:
                            correlations['strong_positive'].append(correlation_info)
                        elif corr_value < -self.correlation_threshold:
                            correlations['strong_negative'].append(correlation_info)
        
        # Ищем интересные паттерны
        patterns = self._find_interesting_patterns(df)
        correlations['interesting_patterns'] = patterns
        
        return correlations
    
    def _get_correlation_strength(self, corr_value: float) -> str:
        """Определяет силу корреляции"""
        if corr_value > 0.7:
            return "очень сильная"
        elif corr_value > 0.5:
            return "сильная"
        elif corr_value > 0.3:
            return "умеренная"
        else:
            return "слабая"
    
    def _interpret_correlation(self, factor1: str, factor2: str, corr_value: float) -> str:
        """Интерпретирует корреляцию в понятных терминах"""
        interpretations = {
            ('total_sales', 'orders'): "Больше заказов = больше продаж",
            ('total_sales', 'rating'): "Высокий рейтинг привлекает клиентов",
            ('total_sales', 'rain_mm'): "Дождь влияет на заказы",
            ('total_sales', 'temp_c'): "Температура влияет на аппетит",
            ('total_sales', 'ads_on'): "Реклама влияет на продажи",
            ('rating', 'cancel_rate'): "Отмены снижают рейтинг",
            ('orders', 'day_of_week'): "Есть любимые дни для заказов",
            ('rain_mm', 'orders'): "Дождь меняет поведение клиентов"
        }
        
        key1 = (factor1, factor2)
        key2 = (factor2, factor1)
        
        base_interpretation = interpretations.get(key1) or interpretations.get(key2) or f"Связь между {factor1} и {factor2}"
        
        if corr_value > 0:
            return f"{base_interpretation} (положительная связь)"
        else:
            return f"{base_interpretation} (обратная связь)"
    
    def _find_interesting_patterns(self, df: pd.DataFrame) -> List[Dict]:
        """Поиск интересных паттернов в данных"""
        patterns = []
        
        # 1. Анализ эффективности рекламы по дням недели
        ads_effectiveness = df[df['ads_on'] == 1].groupby('day_of_week').agg({
            'total_sales': 'mean',
            'roas': 'mean'
        }).round(2)
        
        if not ads_effectiveness.empty:
            best_day = ads_effectiveness['roas'].idxmax()
            worst_day = ads_effectiveness['roas'].idxmin()
            weekdays = ['Понедельник', 'Вторник', 'Среда', 'Четверг', 'Пятница', 'Суббота', 'Воскресенье']
            
            patterns.append({
                'type': 'advertising_pattern',
                'description': f"Реклама наиболее эффективна в {weekdays[best_day]} (ROAS: {ads_effectiveness.loc[best_day, 'roas']:.1f}), наименее - в {weekdays[worst_day]} (ROAS: {ads_effectiveness.loc[worst_day, 'roas']:.1f})"
            })
        
        # 2. Анализ влияния погоды на продажи
        rainy_days = df[df['rain_mm'] > 10]['total_sales'].mean()
        sunny_days = df[df['rain_mm'] == 0]['total_sales'].mean()
        
        if not pd.isna(rainy_days) and not pd.isna(sunny_days) and sunny_days > 0:
            if rainy_days > sunny_days:
                weather_effect = f"В дождливые дни продажи выше на {((rainy_days - sunny_days) / sunny_days * 100):+.1f}% - люди не хотят выходить"
            else:
                weather_effect = f"В дождливые дни продажи ниже на {((sunny_days - rainy_days) / sunny_days * 100):+.1f}% - проблемы с доставкой"
            
            patterns.append({
                'type': 'weather_pattern',
                'description': weather_effect
            })
        
        # 3. Анализ влияния рейтинга на продажи
        high_rating_days = df[df['rating'] >= 4.7]['total_sales'].mean()
        low_rating_days = df[df['rating'] < 4.5]['total_sales'].mean()
        
        if not pd.isna(high_rating_days) and not pd.isna(low_rating_days) and low_rating_days > 0 and high_rating_days > low_rating_days:
            rating_impact = ((high_rating_days - low_rating_days) / low_rating_days * 100)
            patterns.append({
                'type': 'rating_impact',
                'description': f"Высокий рейтинг (4.7+) увеличивает продажи на {rating_impact:+.1f}% по сравнению с низким (<4.5)"
            })
        
        # 4. Анализ праздничных дней
        holiday_sales = df[df['is_holiday'] == True]['total_sales'].mean()
        regular_sales = df[df['is_holiday'] == False]['total_sales'].mean()
        
        if not pd.isna(holiday_sales) and not pd.isna(regular_sales) and regular_sales > 0:
            holiday_effect = ((holiday_sales - regular_sales) / regular_sales * 100)
            if holiday_effect > 0:
                patterns.append({
                    'type': 'holiday_effect',
                    'description': f"Праздники увеличивают продажи на {holiday_effect:+.1f}% - люди заказывают больше еды"
                })
            else:
                patterns.append({
                    'type': 'holiday_effect',
                    'description': f"Праздники снижают продажи на {abs(holiday_effect):+.1f}% - возможно, меньше водителей или закрыты кухни"
                })
        
        return patterns
    
    def analyze_trends(self, df: pd.DataFrame, restaurant_name: str) -> Dict:
        """Анализ трендов и изменений за разные периоды"""
        trends = {}
        
        # Анализ по месяцам
        df['month'] = df['date'].dt.month
        monthly_stats = df.groupby('month').agg({
            'total_sales': 'mean',
            'orders': 'mean',
            'rating': 'mean',
            'roas': 'mean'
        }).round(2)
        
        if not monthly_stats.empty:
            trends['monthly'] = {
                'best_month': monthly_stats['total_sales'].idxmax(),
                'worst_month': monthly_stats['total_sales'].idxmin(),
                'best_sales': monthly_stats['total_sales'].max(),
                'worst_sales': monthly_stats['total_sales'].min()
            }
        
        # Анализ ROAS за последние 3-6 месяцев
        recent_data = df[df['date'] >= df['date'].max() - timedelta(days=180)]
        if len(recent_data) > 0:
            recent_roas = recent_data[recent_data['ads_on'] == 1]['roas']
            older_data = df[(df['date'] < df['date'].max() - timedelta(days=180)) & (df['ads_on'] == 1)]
            
            if len(recent_roas) > 0 and len(older_data) > 0:
                recent_avg_roas = recent_roas.mean()
                older_avg_roas = older_data['roas'].mean()
                if older_avg_roas > 0:
                    roas_change = ((recent_avg_roas - older_avg_roas) / older_avg_roas * 100)
                    
                    trends['roas_trend'] = {
                        'recent_roas': recent_avg_roas,
                        'older_roas': older_avg_roas,
                        'change_percent': roas_change,
                        'interpretation': f"ROAS за последние 6 месяцев {'просел' if roas_change < 0 else 'вырос'} на {abs(roas_change):.1f}%"
                    }
        
        return trends

def generate_deep_analytics_report(restaurant_name: str, start_date: str, end_date: str) -> Dict:
    """
    Генерирует глубокий аналитический отчет с поиском аномалий и корреляций
    """
    logger.info(f"Генерация глубокого анализа для {restaurant_name} за период {start_date} - {end_date}")
    
    try:
        # Загружаем данные
        df = get_restaurant_data(restaurant_name)
        if df is None:
            return {"error": f"Нет данных для ресторана {restaurant_name}"}
        
        df['date'] = pd.to_datetime(df['date'])
        period_df = df[(df['date'] >= start_date) & (df['date'] <= end_date)].copy()
        
        if period_df.empty:
            return {"error": f"Нет данных за период {start_date} - {end_date}"}
        
        # Создаем продвинутый аналитический движок
        analytics_engine = AdvancedAnalyticsEngine()
        
        # Базовая статистика
        base_stats = {
            'total_sales': period_df['total_sales'].sum(),
            'avg_daily_sales': period_df['total_sales'].mean(),
            'total_orders': period_df['orders'].sum(),
            'avg_rating': period_df['rating'].mean(),
            'days_analyzed': len(period_df)
        }
        
        # Поиск аномалий
        anomalies = analytics_engine.find_sales_anomalies(period_df, restaurant_name)
        
        # Поиск корреляций
        correlations = analytics_engine.find_correlations(period_df)
        
        # Анализ трендов
        trends = analytics_engine.analyze_trends(period_df, restaurant_name)
        
        # Формируем итоговый отчет
        report = {
            'restaurant_name': restaurant_name,
            'period': f"{start_date} - {end_date}",
            'base_statistics': base_stats,
            'anomalies': anomalies[:10],  # Топ-10 аномалий
            'correlations': correlations,
            'trends': trends,
            'insights_count': len(anomalies) + len(correlations['strong_positive']) + len(correlations['strong_negative']) + len(correlations['interesting_patterns']),
            'generated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        
        return report
        
    except Exception as e:
        logger.error(f"Ошибка генерации глубокого анализа: {e}")
        return {"error": f"Ошибка анализа: {str(e)}"}

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

def generate_client_report(restaurant_name: str, start_date: str, end_date: str) -> Dict:
    """
    Генерирует подробный клиентский отчет за период
    
    Args:
        restaurant_name: Название ресторана
        start_date: Начальная дата периода (YYYY-MM-DD)
        end_date: Конечная дата периода (YYYY-MM-DD)
    
    Returns:
        Детальный отчет с анализом и рекомендациями
    """
    logger.info(f"Генерация клиентского отчета для {restaurant_name} за период {start_date} - {end_date}")
    
    try:
        # Загружаем данные ресторана
        df = get_restaurant_data(restaurant_name)
        if df is None:
            return {"error": f"Нет данных для ресторана {restaurant_name}"}
        
        df['date'] = pd.to_datetime(df['date'])
        
        # Фильтруем по периоду
        period_df = df[(df['date'] >= start_date) & (df['date'] <= end_date)]
        
        if period_df.empty:
            return {"error": f"Нет данных за период {start_date} - {end_date}"}
        
        # Основные метрики
        total_sales = period_df['total_sales'].sum()
        total_orders = period_df['orders'].sum()
        avg_check = total_sales / total_orders if total_orders > 0 else 0
        avg_daily_sales = period_df['total_sales'].mean()
        days_count = len(period_df)
        
        # Анализ рекламы
        ads_days = period_df[period_df['ads_on'] == 1]
        no_ads_days = period_df[period_df['ads_on'] == 0]
        
        ads_effectiveness = {}
        if len(ads_days) > 0 and len(no_ads_days) > 0:
            ads_avg_sales = ads_days['total_sales'].mean()
            no_ads_avg_sales = no_ads_days['total_sales'].mean()
            ads_impact = ((ads_avg_sales - no_ads_avg_sales) / no_ads_avg_sales) * 100
            
            ads_effectiveness = {
                'days_with_ads': len(ads_days),
                'days_without_ads': len(no_ads_days),
                'ads_percentage': len(ads_days) / len(period_df) * 100,
                'avg_sales_with_ads': ads_avg_sales,
                'avg_sales_without_ads': no_ads_avg_sales,
                'ads_impact_percent': ads_impact
            }
        
        # Анализ по месяцам
        period_df['month'] = period_df['date'].dt.month
        period_df['month_name'] = period_df['date'].dt.strftime('%B')
        
        monthly_analysis = {}
        for month in period_df['month'].unique():
            month_data = period_df[period_df['month'] == month]
            month_name = month_data['month_name'].iloc[0]
            
            monthly_analysis[month_name] = {
                'sales': month_data['total_sales'].sum(),
                'orders': month_data['orders'].sum(),
                'avg_rating': month_data['rating'].mean(),
                'days_with_ads': (month_data['ads_on'] == 1).sum(),
                'total_days': len(month_data),
                'avg_daily_sales': month_data['total_sales'].mean()
            }
        
        # Тренд продаж
        first_month_sales = list(monthly_analysis.values())[0]['sales']
        last_month_sales = list(monthly_analysis.values())[-1]['sales']
        
        sales_growth = 0
        if first_month_sales > 0:
            sales_growth = ((last_month_sales - first_month_sales) / first_month_sales) * 100
        
        # Анализ рейтинга и качества
        avg_rating = period_df['rating'].mean()
        avg_delivery_time = period_df['delivery_time'].mean()
        avg_cancel_rate = period_df['cancel_rate'].mean()
        
        # Анализ праздников
        holiday_impact = {}
        if 'is_holiday' in period_df.columns:
            holiday_days = period_df[period_df['is_holiday'] == True]
            regular_days = period_df[period_df['is_holiday'] == False]
            
            if len(holiday_days) > 0 and len(regular_days) > 0:
                holiday_avg_sales = holiday_days['total_sales'].mean()
                regular_avg_sales = regular_days['total_sales'].mean()
                holiday_impact_percent = ((holiday_avg_sales - regular_avg_sales) / regular_avg_sales) * 100
                
                holiday_impact = {
                    'holiday_days': len(holiday_days),
                    'regular_days': len(regular_days),
                    'holiday_avg_sales': holiday_avg_sales,
                    'regular_avg_sales': regular_avg_sales,
                    'holiday_impact_percent': holiday_impact_percent
                }
        
        # Генерируем выводы и рекомендации
        conclusions = []
        recommendations = []
        
        # Выводы по рекламе
        if ads_effectiveness:
            impact = ads_effectiveness['ads_impact_percent']
            if impact > 20:
                conclusions.append(f"✅ Реклама высокоэффективна - повышает продажи на {impact:.1f}%")
                recommendations.append("Увеличить рекламный бюджет для максимизации эффекта")
            elif impact > 0:
                conclusions.append(f"⚠️ Реклама эффективна - повышает продажи на {impact:.1f}%")
                recommendations.append("Оптимизировать рекламные кампании для повышения эффективности")
            else:
                conclusions.append(f"❌ Реклама неэффективна - снижает продажи на {abs(impact):.1f}%")
                recommendations.append("Срочно пересмотреть рекламную стратегию")
        
        # Выводы по росту
        if sales_growth > 10:
            conclusions.append(f"📈 Отличный рост продаж - {sales_growth:.1f}% за период")
            recommendations.append("Масштабировать успешные практики")
        elif sales_growth > 0:
            conclusions.append(f"📊 Умеренный рост продаж - {sales_growth:.1f}% за период")
            recommendations.append("Найти дополнительные точки роста")
        else:
            conclusions.append(f"📉 Снижение продаж - {abs(sales_growth):.1f}% за период")
            recommendations.append("Срочно проанализировать причины снижения")
        
        # Выводы по качеству
        if avg_rating < 4.5:
            conclusions.append(f"⚠️ Рейтинг требует внимания - {avg_rating:.2f}")
            recommendations.append("Улучшить качество обслуживания и продукции")
        
        if avg_cancel_rate > 0.1:
            conclusions.append(f"❌ Высокий уровень отмен - {avg_cancel_rate*100:.1f}%")
            recommendations.append("Снизить время подготовки заказов")
        
        # Выводы по праздникам
        if holiday_impact and holiday_impact['holiday_impact_percent'] > 50:
            conclusions.append(f"🎉 Праздники значительно повышают продажи - {holiday_impact['holiday_impact_percent']:.1f}%")
            recommendations.append("Подготовить специальные предложения к праздникам")
        
        # Формируем итоговый отчет
        report = {
            'restaurant_name': restaurant_name,
            'period': f"{start_date} - {end_date}",
            'summary': {
                'total_sales': total_sales,
                'total_orders': total_orders,
                'avg_check': avg_check,
                'avg_daily_sales': avg_daily_sales,
                'days_count': days_count,
                'avg_rating': avg_rating,
                'avg_delivery_time': avg_delivery_time,
                'avg_cancel_rate': avg_cancel_rate * 100
            },
            'advertising_analysis': ads_effectiveness,
            'monthly_analysis': monthly_analysis,
            'sales_growth_percent': sales_growth,
            'holiday_impact': holiday_impact,
            'conclusions': conclusions,
            'recommendations': recommendations,
            'generated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        
        return report
        
    except Exception as e:
        logger.error(f"Ошибка генерации клиентского отчета: {e}")
        return {"error": f"Ошибка генерации отчета: {str(e)}"}