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

from main.data_integration import load_data_with_all_features, prepare_features_with_all_enhancements
from main.data_loader import get_restaurant_data

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

class CausalAnalysisEngine:
    """
    Причинно-следственный анализ драйверов роста заказов
    
    Цель: Найти управляемые факторы, которые увеличивают количество заказов
    """
    
    def __init__(self):
        self.order_drivers = [
            'rating', 'delivery_time', 'cancel_rate', 'ads_on', 
            'roas', 'temp_c', 'rain_mm', 'is_holiday', 'day_of_week'
        ]
        self.platforms = ['gojek', 'grab']  # Будем определять по данным
        
    def analyze_order_drivers_per_restaurant(self, df: pd.DataFrame, restaurant_name: str) -> Dict:
        """Анализ драйверов заказов для конкретного ресторана"""
        analysis = {
            'restaurant_name': restaurant_name,
            'order_correlations': {},
            'period_comparisons': {},
            'actionable_insights': [],
            'growth_levers': {}
        }
        
        # 1. Корреляция факторов с количеством заказов
        order_correlations = {}
        for driver in self.order_drivers:
            if driver in df.columns:
                correlation = df['orders'].corr(df[driver])
                if abs(correlation) > 0.2:  # Значимые корреляции
                    order_correlations[driver] = {
                        'correlation': correlation,
                        'strength': self._get_correlation_strength(abs(correlation)),
                        'impact_interpretation': self._interpret_order_impact(driver, correlation)
                    }
        
        analysis['order_correlations'] = order_correlations
        
        # 2. Анализ периодов: до/после изменений
        period_analysis = self._analyze_before_after_periods(df)
        analysis['period_comparisons'] = period_analysis
        
        # 3. Определение платформенных эффектов (если можно определить)
        platform_effect = self._analyze_platform_effect(df)
        if platform_effect:
            analysis['platform_effect'] = platform_effect
        
        # 4. Генерация управляемых рекомендаций
        growth_levers = self._identify_growth_levers(df, order_correlations)
        analysis['growth_levers'] = growth_levers
        
        # 5. Конкретные инсайты
        actionable_insights = self._generate_actionable_insights(df, order_correlations, growth_levers)
        analysis['actionable_insights'] = actionable_insights
        
        return analysis
    
    def compare_restaurants_performance(self, restaurants_data: Dict) -> Dict:
        """Сравнительный анализ ресторанов для выявления факторов успеха"""
        comparison = {
            'top_performers': {},
            'underperformers': {},
            'success_factors': {},
            'differentiation_insights': []
        }
        
        # Сортируем рестораны по среднему количеству заказов
        restaurant_performance = {}
        for name, df in restaurants_data.items():
            avg_orders = df['orders'].mean()
            order_growth = self._calculate_order_growth(df)
            restaurant_performance[name] = {
                'avg_orders': avg_orders,
                'order_growth': order_growth,
                'avg_rating': df['rating'].mean(),
                'avg_delivery_time': df['delivery_time'].mean(),
                'avg_cancel_rate': df['cancel_rate'].mean(),
                'ads_usage_percent': (df['ads_on'].sum() / len(df)) * 100
            }
        
        # Определяем топ и аутсайдеров
        sorted_performance = sorted(restaurant_performance.items(), 
                                  key=lambda x: x[1]['avg_orders'], reverse=True)
        
        top_3 = dict(sorted_performance[:3])
        bottom_3 = dict(sorted_performance[-3:])
        
        comparison['top_performers'] = top_3
        comparison['underperformers'] = bottom_3
        
        # Анализируем факторы успеха
        success_factors = self._analyze_success_factors(top_3, bottom_3)
        comparison['success_factors'] = success_factors
        
        # Генерируем инсайты дифференциации
        differentiation_insights = self._generate_differentiation_insights(top_3, bottom_3)
        comparison['differentiation_insights'] = differentiation_insights
        
        return comparison
    
    def _analyze_before_after_periods(self, df: pd.DataFrame) -> Dict:
        """Анализ периодов до/после изменений ключевых метрик"""
        period_analysis = {}
        
        # Анализ изменения рейтинга
        rating_analysis = self._analyze_rating_periods(df)
        if rating_analysis:
            period_analysis['rating_change'] = rating_analysis
        
        # Анализ изменения рекламы
        ads_analysis = self._analyze_ads_periods(df)
        if ads_analysis:
            period_analysis['ads_change'] = ads_analysis
        
        # Анализ изменения отмен
        cancellation_analysis = self._analyze_cancellation_periods(df)
        if cancellation_analysis:
            period_analysis['cancellation_change'] = cancellation_analysis
        
        return period_analysis
    
    def _analyze_rating_periods(self, df: pd.DataFrame) -> Dict:
        """Анализ влияния изменения рейтинга на заказы"""
        df_sorted = df.sort_values('date')
        
        # Находим значительные изменения рейтинга
        df_sorted['rating_change'] = df_sorted['rating'].diff()
        significant_changes = df_sorted[abs(df_sorted['rating_change']) > 0.2]
        
        if len(significant_changes) == 0:
            return None
        
        # Берем самое значительное изменение
        max_change = significant_changes.loc[abs(significant_changes['rating_change']).idxmax()]
        change_date = max_change['date']
        
        # Сравниваем периоды до и после (30 дней)
        before_period = df_sorted[
            (df_sorted['date'] >= change_date - timedelta(days=30)) & 
            (df_sorted['date'] < change_date)
        ]
        after_period = df_sorted[
            (df_sorted['date'] >= change_date) & 
            (df_sorted['date'] <= change_date + timedelta(days=30))
        ]
        
        if len(before_period) < 10 or len(after_period) < 10:
            return None
        
        return {
            'change_date': change_date.strftime('%Y-%m-%d'),
            'rating_change': max_change['rating_change'],
            'before_avg_orders': before_period['orders'].mean(),
            'after_avg_orders': after_period['orders'].mean(),
            'orders_change_percent': ((after_period['orders'].mean() - before_period['orders'].mean()) / before_period['orders'].mean()) * 100,
            'interpretation': f"Изменение рейтинга на {max_change['rating_change']:+.2f} привело к изменению заказов на {((after_period['orders'].mean() - before_period['orders'].mean()) / before_period['orders'].mean()) * 100:+.1f}%"
        }
    
    def _analyze_ads_periods(self, df: pd.DataFrame) -> Dict:
        """Анализ влияния включения/выключения рекламы на заказы"""
        df_sorted = df.sort_values('date')
        
        # Находим переходы в рекламной активности
        df_sorted['ads_change'] = df_sorted['ads_on'].diff()
        ads_toggles = df_sorted[abs(df_sorted['ads_change']) > 0.5]
        
        if len(ads_toggles) == 0:
            return None
        
        # Анализируем включение рекламы (переход с 0 на 1)
        ads_turn_on = ads_toggles[ads_toggles['ads_change'] > 0.5]
        if len(ads_turn_on) == 0:
            return None
        
        # Берем первое включение рекламы
        toggle_date = ads_turn_on.iloc[0]['date']
        
        # Сравниваем периоды
        before_period = df_sorted[
            (df_sorted['date'] >= toggle_date - timedelta(days=30)) & 
            (df_sorted['date'] < toggle_date)
        ]
        after_period = df_sorted[
            (df_sorted['date'] >= toggle_date) & 
            (df_sorted['date'] <= toggle_date + timedelta(days=30))
        ]
        
        if len(before_period) < 10 or len(after_period) < 10:
            return None
        
        return {
            'change_date': toggle_date.strftime('%Y-%m-%d'),
            'change_type': 'ads_turned_on',
            'before_avg_orders': before_period['orders'].mean(),
            'after_avg_orders': after_period['orders'].mean(),
            'orders_change_percent': ((after_period['orders'].mean() - before_period['orders'].mean()) / before_period['orders'].mean()) * 100,
            'interpretation': f"Включение рекламы привело к изменению заказов на {((after_period['orders'].mean() - before_period['orders'].mean()) / before_period['orders'].mean()) * 100:+.1f}%"
        }
    
    def _analyze_cancellation_periods(self, df: pd.DataFrame) -> Dict:
        """Анализ влияния изменения уровня отмен на заказы"""
        df_sorted = df.sort_values('date')
        
        # Находим значительные изменения в отменах
        df_sorted['cancel_change'] = df_sorted['cancel_rate'].diff()
        significant_changes = df_sorted[abs(df_sorted['cancel_change']) > 0.05]  # 5% изменение
        
        if len(significant_changes) == 0:
            return None
        
        # Берем самое значительное изменение
        max_change = significant_changes.loc[abs(significant_changes['cancel_change']).idxmax()]
        change_date = max_change['date']
        
        # Сравниваем периоды
        before_period = df_sorted[
            (df_sorted['date'] >= change_date - timedelta(days=30)) & 
            (df_sorted['date'] < change_date)
        ]
        after_period = df_sorted[
            (df_sorted['date'] >= change_date) & 
            (df_sorted['date'] <= change_date + timedelta(days=30))
        ]
        
        if len(before_period) < 10 or len(after_period) < 10:
            return None
        
        return {
            'change_date': change_date.strftime('%Y-%m-%d'),
            'cancel_rate_change': max_change['cancel_change'],
            'before_avg_orders': before_period['orders'].mean(),
            'after_avg_orders': after_period['orders'].mean(),
            'orders_change_percent': ((after_period['orders'].mean() - before_period['orders'].mean()) / before_period['orders'].mean()) * 100,
            'interpretation': f"Изменение уровня отмен на {max_change['cancel_change']:+.2%} привело к изменению заказов на {((after_period['orders'].mean() - before_period['orders'].mean()) / before_period['orders'].mean()) * 100:+.1f}%"
        }
    
    def _analyze_platform_effect(self, df: pd.DataFrame) -> Dict:
        """Анализ эффекта платформы (если можно определить)"""
        # Пока используем эвристику: если delivery_time < 25, то gojek, иначе grab
        if 'delivery_time' in df.columns:
            df['platform_estimate'] = df['delivery_time'].apply(lambda x: 'gojek' if x < 25 else 'grab')
            
            platform_stats = df.groupby('platform_estimate').agg({
                'orders': 'mean',
                'total_sales': 'mean',
                'rating': 'mean',
                'cancel_rate': 'mean'
            }).round(2)
            
            if len(platform_stats) > 1:
                gojek_orders = platform_stats.loc['gojek', 'orders'] if 'gojek' in platform_stats.index else 0
                grab_orders = platform_stats.loc['grab', 'orders'] if 'grab' in platform_stats.index else 0
                
                if gojek_orders > 0 and grab_orders > 0:
                    better_platform = 'gojek' if gojek_orders > grab_orders else 'grab'
                    difference = abs(gojek_orders - grab_orders) / min(gojek_orders, grab_orders) * 100
                    
                    return {
                        'gojek_avg_orders': gojek_orders,
                        'grab_avg_orders': grab_orders,
                        'better_platform': better_platform,
                        'difference_percent': difference,
                        'interpretation': f"Платформа {better_platform} показывает на {difference:.1f}% больше заказов"
                    }
        
        return None
    
    def _identify_growth_levers(self, df: pd.DataFrame, correlations: Dict) -> Dict:
        """Определение управляемых рычагов роста"""
        levers = {}
        
        # Анализируем каждый фактор на предмет возможности улучшения
        current_metrics = {
            'rating': df['rating'].mean(),
            'delivery_time': df['delivery_time'].mean(),
            'cancel_rate': df['cancel_rate'].mean(),
            'ads_usage': (df['ads_on'].sum() / len(df)) * 100
        }
        
        # Потенциал улучшения рейтинга
        if 'rating' in correlations and correlations['rating']['correlation'] > 0:
            current_rating = current_metrics['rating']
            if current_rating < 4.8:
                potential_improvement = (4.8 - current_rating) / current_rating * 100
                order_impact = correlations['rating']['correlation'] * potential_improvement
                levers['rating_improvement'] = {
                    'current_value': current_rating,
                    'target_value': 4.8,
                    'potential_order_increase': f"{order_impact:.1f}%",
                    'actionability': 'high',
                    'recommendation': f"Повысить рейтинг с {current_rating:.2f} до 4.8+ для увеличения заказов"
                }
        
        # Потенциал сокращения времени доставки
        if 'delivery_time' in correlations and correlations['delivery_time']['correlation'] < 0:
            current_time = current_metrics['delivery_time']
            if current_time > 20:
                target_time = max(15, current_time * 0.8)
                improvement_percent = (current_time - target_time) / current_time * 100
                order_impact = abs(correlations['delivery_time']['correlation']) * improvement_percent
                levers['delivery_improvement'] = {
                    'current_value': current_time,
                    'target_value': target_time,
                    'potential_order_increase': f"{order_impact:.1f}%",
                    'actionability': 'medium',
                    'recommendation': f"Сократить время доставки с {current_time:.0f} до {target_time:.0f} минут"
                }
        
        # Потенциал снижения отмен
        if 'cancel_rate' in correlations and correlations['cancel_rate']['correlation'] < 0:
            current_cancels = current_metrics['cancel_rate']
            if current_cancels > 0.03:
                target_cancels = max(0.02, current_cancels * 0.5)
                improvement_percent = (current_cancels - target_cancels) / current_cancels * 100
                order_impact = abs(correlations['cancel_rate']['correlation']) * improvement_percent
                levers['cancellation_reduction'] = {
                    'current_value': current_cancels * 100,
                    'target_value': target_cancels * 100,
                    'potential_order_increase': f"{order_impact:.1f}%",
                    'actionability': 'high',
                    'recommendation': f"Снизить отмены с {current_cancels*100:.1f}% до {target_cancels*100:.1f}%"
                }
        
        # Потенциал увеличения рекламы
        if 'ads_on' in correlations and correlations['ads_on']['correlation'] > 0:
            current_ads = current_metrics['ads_usage']
            if current_ads < 80:
                target_ads = min(90, current_ads + 20)
                improvement_percent = (target_ads - current_ads) / max(current_ads, 1)
                order_impact = correlations['ads_on']['correlation'] * improvement_percent * 10
                levers['advertising_increase'] = {
                    'current_value': current_ads,
                    'target_value': target_ads,
                    'potential_order_increase': f"{order_impact:.1f}%",
                    'actionability': 'high',
                    'recommendation': f"Увеличить рекламную активность с {current_ads:.0f}% до {target_ads:.0f}% дней"
                }
        
        return levers
    
    def _generate_actionable_insights(self, df: pd.DataFrame, correlations: Dict, levers: Dict) -> List[str]:
        """Генерация конкретных рекомендаций"""
        insights = []
        
        # Топ-3 приоритета по воздействию на заказы
        if levers:
            sorted_levers = sorted(levers.items(), 
                                 key=lambda x: float(x[1]['potential_order_increase'].replace('%', '')), 
                                 reverse=True)
            
            insights.append("🎯 ПРИОРИТЕТНЫЕ ДЕЙСТВИЯ ДЛЯ РОСТА ЗАКАЗОВ:")
            
            for i, (lever_name, lever_data) in enumerate(sorted_levers[:3], 1):
                actionability_emoji = {"high": "🟢", "medium": "🟡", "low": "🔴"}
                emoji = actionability_emoji.get(lever_data['actionability'], "⚪")
                
                insights.append(f"{i}. {emoji} {lever_data['recommendation']} "
                              f"(потенциал: +{lever_data['potential_order_increase']} заказов)")
        
        # Специфические инсайты
        avg_orders = df['orders'].mean()
        if avg_orders < 10:
            insights.append("⚠️ КРИТИЧНО: Очень низкое количество заказов - требуется комплексная работа")
        elif avg_orders < 20:
            insights.append("📊 ПОТЕНЦИАЛ: Среднее количество заказов - есть место для роста")
        else:
            insights.append("✅ ХОРОШО: Высокое количество заказов - фокус на удержании")
        
        return insights
    
    def _calculate_order_growth(self, df: pd.DataFrame) -> float:
        """Расчет роста заказов за период"""
        df_sorted = df.sort_values('date')
        if len(df_sorted) < 60:  # Меньше 2 месяцев данных
            return 0
        
        first_month = df_sorted.head(30)['orders'].mean()
        last_month = df_sorted.tail(30)['orders'].mean()
        
        if first_month > 0:
            return ((last_month - first_month) / first_month) * 100
        return 0
    
    def _analyze_success_factors(self, top_performers: Dict, underperformers: Dict) -> Dict:
        """Анализ факторов успеха"""
        success_factors = {}
        
        # Средние значения для топ и аутсайдеров
        top_avg = {
            'rating': np.mean([data['avg_rating'] for data in top_performers.values()]),
            'delivery_time': np.mean([data['avg_delivery_time'] for data in top_performers.values()]),
            'cancel_rate': np.mean([data['avg_cancel_rate'] for data in top_performers.values()]),
            'ads_usage': np.mean([data['ads_usage_percent'] for data in top_performers.values()])
        }
        
        bottom_avg = {
            'rating': np.mean([data['avg_rating'] for data in underperformers.values()]),
            'delivery_time': np.mean([data['avg_delivery_time'] for data in underperformers.values()]),
            'cancel_rate': np.mean([data['avg_cancel_rate'] for data in underperformers.values()]),
            'ads_usage': np.mean([data['ads_usage_percent'] for data in underperformers.values()])
        }
        
        # Определяем ключевые различия
        for factor in top_avg.keys():
            difference = abs(top_avg[factor] - bottom_avg[factor])
            relative_difference = difference / max(abs(bottom_avg[factor]), 0.001) * 100
            
            if relative_difference > 10:  # Значимое различие
                success_factors[factor] = {
                    'top_avg': top_avg[factor],
                    'bottom_avg': bottom_avg[factor],
                    'difference_percent': relative_difference,
                    'is_success_factor': top_avg[factor] > bottom_avg[factor] if factor != 'cancel_rate' and factor != 'delivery_time' else top_avg[factor] < bottom_avg[factor]
                }
        
        return success_factors
    
    def _generate_differentiation_insights(self, top_performers: Dict, underperformers: Dict) -> List[str]:
        """Генерация инсайтов дифференциации"""
        insights = []
        
        # Сравнение средних показателей
        top_avg_orders = np.mean([data['avg_orders'] for data in top_performers.values()])
        bottom_avg_orders = np.mean([data['avg_orders'] for data in underperformers.values()])
        
        difference = ((top_avg_orders - bottom_avg_orders) / bottom_avg_orders) * 100
        
        insights.append(f"📊 Топ-рестораны получают на {difference:.1f}% больше заказов в среднем")
        
        # Конкретные различия
        top_rating = np.mean([data['avg_rating'] for data in top_performers.values()])
        bottom_rating = np.mean([data['avg_rating'] for data in underperformers.values()])
        
        if (top_rating - bottom_rating) > 0.2:
            insights.append(f"⭐ Рейтинг: топ-рестораны {top_rating:.2f} vs аутсайдеры {bottom_rating:.2f}")
        
        top_ads = np.mean([data['ads_usage_percent'] for data in top_performers.values()])
        bottom_ads = np.mean([data['ads_usage_percent'] for data in underperformers.values()])
        
        if abs(top_ads - bottom_ads) > 15:
            insights.append(f"📢 Реклама: топ-рестораны используют {top_ads:.0f}% дней vs {bottom_ads:.0f}% у аутсайдеров")
        
        return insights
    
    def _interpret_order_impact(self, factor: str, correlation: float) -> str:
        """Интерпретация влияния фактора на заказы"""
        impact_map = {
            'rating': f"Рейтинг {'положительно' if correlation > 0 else 'отрицательно'} влияет на количество заказов",
            'delivery_time': f"Время доставки {'увеличивает' if correlation > 0 else 'снижает'} количество заказов",
            'cancel_rate': f"Отмены {'увеличивают' if correlation > 0 else 'снижают'} количество заказов",
            'ads_on': f"Реклама {'увеличивает' if correlation > 0 else 'снижает'} количество заказов",
            'temp_c': f"Температура {'увеличивает' if correlation > 0 else 'снижает'} количество заказов",
            'rain_mm': f"Дождь {'увеличивает' if correlation > 0 else 'снижает'} количество заказов"
        }
        
        return impact_map.get(factor, f"Фактор {factor} влияет на заказы")
    
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

def generate_causal_analysis_report(restaurant_name: str = None, start_date: str = None, end_date: str = None) -> Dict:
    """
    Генерирует причинно-следственный анализ драйверов роста заказов
    """
    causal_engine = CausalAnalysisEngine()
    
    try:
        if restaurant_name:
            # Анализ конкретного ресторана
            df = get_restaurant_data(restaurant_name)
            if df is None:
                return {"error": f"Нет данных для ресторана {restaurant_name}"}
            
            df['date'] = pd.to_datetime(df['date'])
            
            if start_date and end_date:
                df = df[(df['date'] >= start_date) & (df['date'] <= end_date)].copy()
            
            if df.empty:
                return {"error": "Нет данных за указанный период"}
            
            # Анализ драйверов для конкретного ресторана
            restaurant_analysis = causal_engine.analyze_order_drivers_per_restaurant(df, restaurant_name)
            
            return {
                'type': 'single_restaurant',
                'restaurant_analysis': restaurant_analysis,
                'generated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
        
        else:
            # Сравнительный анализ всех ресторанов
            all_data = load_data_with_all_features()
            if all_data.empty:
                return {"error": "Нет данных для анализа"}
            
            # Группируем по ресторанам
            restaurants_data = {}
            for restaurant in all_data['restaurant_name'].unique():
                if pd.notna(restaurant):
                    restaurant_df = all_data[all_data['restaurant_name'] == restaurant].copy()
                    if len(restaurant_df) > 30:  # Минимум данных для анализа
                        restaurants_data[restaurant] = restaurant_df
            
            if len(restaurants_data) < 3:
                return {"error": "Недостаточно данных для сравнительного анализа"}
            
            # Сравнительный анализ
            comparison_analysis = causal_engine.compare_restaurants_performance(restaurants_data)
            
            # Индивидуальный анализ топ-3 ресторанов
            individual_analyses = {}
            for restaurant in list(comparison_analysis['top_performers'].keys())[:3]:
                individual_analyses[restaurant] = causal_engine.analyze_order_drivers_per_restaurant(
                    restaurants_data[restaurant], restaurant
                )
            
            return {
                'type': 'comparative',
                'comparison_analysis': comparison_analysis,
                'individual_analyses': individual_analyses,
                'total_restaurants_analyzed': len(restaurants_data),
                'generated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
    
    except Exception as e:
        logger.error(f"Ошибка причинно-следственного анализа: {e}")
        return {"error": f"Ошибка анализа: {str(e)}"}

def generate_deep_analytics_report(restaurant_name: str, start_date: str, end_date: str) -> Dict:
    """
    Генерирует глубокий аналитический отчет с поиском аномалий и корреляций
    """
    logger.info(f"Генерация глубокого анализа для {restaurant_name} за период {start_date} - {end_date}")
    
    try:
        # Загружаем данные для конкретного ресторана
        restaurant_df = get_restaurant_data(restaurant_name)
        if restaurant_df is None:
            return {"error": f"Нет данных для ресторана {restaurant_name}"}
        
        restaurant_df['date'] = pd.to_datetime(restaurant_df['date'])
        period_df = restaurant_df[(restaurant_df['date'] >= start_date) & (restaurant_df['date'] <= end_date)].copy()
        
        # Загружаем полную базу данных для конкурентного анализа
        from data_integration import load_data_with_all_features
        df = load_data_with_all_features()
        df['date'] = pd.to_datetime(df['date'])
        
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
        
        # Временной анализ с YoY сравнениями
        market_engine = MarketIntelligenceEngine()
        temporal_analysis = market_engine._analyze_temporal_patterns(period_df, df)
        
        # Конкурентное сравнение (топ-5 ресторанов по заказам за тот же период)
        period_start = pd.to_datetime(start_date)
        period_end = pd.to_datetime(end_date)
        all_restaurants_period = df[(df['date'] >= period_start) & (df['date'] <= period_end)].copy()
        
        competitor_analysis = {}
        if not all_restaurants_period.empty:
            days_in_period = (period_end - period_start).days + 1
            restaurant_performance = all_restaurants_period.groupby('restaurant_name').agg({
                'orders': 'sum',
                'total_sales': 'sum'
            }).reset_index()
            restaurant_performance['avg_orders_per_day'] = restaurant_performance['orders'] / days_in_period
            
            # Сортируем по заказам и получаем ранг текущего ресторана
            restaurant_performance = restaurant_performance.sort_values('orders', ascending=False).reset_index(drop=True)
            
            top_competitors = restaurant_performance.head(5)[['restaurant_name', 'avg_orders_per_day', 'orders', 'total_sales']]
            
            current_rank = None
            if restaurant_name in restaurant_performance['restaurant_name'].values:
                current_rank = restaurant_performance[restaurant_performance['restaurant_name'] == restaurant_name].index[0] + 1
            
            competitor_analysis = {
                'top_performers': top_competitors.to_dict('records'),
                'current_restaurant_rank': current_rank
            }
        
        # Формируем итоговый отчет
        report = {
            'restaurant_name': restaurant_name,
            'period': f"{start_date} - {end_date}",
            'base_statistics': base_stats,
            'anomalies': anomalies[:10],  # Топ-10 аномалий
            'correlations': correlations,
            'trends': trends,
            'temporal_analysis': temporal_analysis,
            'competitor_analysis': competitor_analysis,
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

class MarketIntelligenceEngine:
    """
    Комплексная рыночная аналитика уровня топ-маркетолога
    
    Анализирует всю базу ресторанов по:
    - Платформам (Grab vs Gojek)
    - Временным периодам (YoY, QoQ)
    - Рыночным трендам
    - Аномалиям и возможностям
    """
    
    def __init__(self):
        self.platforms = ['grab', 'gojek']
        self.key_metrics = [
            'total_sales', 'orders', 'rating', 'delivery_time', 
            'cancel_rate', 'ads_on', 'roas'
        ]
        
    def analyze_market_overview(self, df: pd.DataFrame, start_date: str, end_date: str) -> Dict:
        """Комплексный анализ всего рынка за период"""
        
        # Фильтруем данные по периоду
        df['date'] = pd.to_datetime(df['date'])
        period_df = df[(df['date'] >= start_date) & (df['date'] <= end_date)].copy()
        
        analysis = {
            'period': f"{start_date} - {end_date}",
            'market_overview': self._get_market_overview(period_df),
            'platform_analysis': self._analyze_platforms(period_df),
            'restaurant_performance': self._analyze_restaurant_performance(period_df),
            'advertising_intelligence': self._analyze_advertising_trends(period_df),
            'operational_insights': self._analyze_operational_metrics(period_df),
            'temporal_analysis': self._analyze_temporal_patterns(period_df, df),
            'market_anomalies': self._detect_market_anomalies(period_df),
            'strategic_recommendations': [],
            'expert_hypotheses': []
        }
        
        # Генерируем стратегические рекомендации и гипотезы
        analysis['strategic_recommendations'] = self._generate_strategic_recommendations(analysis)
        analysis['expert_hypotheses'] = self._generate_expert_hypotheses(analysis, df)
        
        return analysis
    
    def _get_market_overview(self, df: pd.DataFrame) -> Dict:
        """Обзор рынка: ключевые показатели"""
        total_restaurants = df['restaurant_name'].nunique()
        total_days = (df['date'].max() - df['date'].min()).days + 1
        
        return {
            'total_restaurants': total_restaurants,
            'total_days_analyzed': total_days,
            'total_sales': df['total_sales'].sum(),
            'total_orders': df['orders'].sum(),
            'average_daily_sales_per_restaurant': df.groupby('restaurant_name')['total_sales'].sum().mean(),
            'average_daily_orders_per_restaurant': df.groupby('restaurant_name')['orders'].sum().mean(),
            'market_average_rating': df['rating'].mean(),
            'market_average_delivery_time': df['delivery_time'].mean(),
            'market_cancel_rate': df['cancel_rate'].mean(),
            'ads_adoption_rate': (df['ads_on'].sum() / len(df)) * 100,
            'average_roas': df[df['ads_on'] == 1]['roas'].mean()
        }
    
    def _analyze_platforms(self, df: pd.DataFrame) -> Dict:
        """Глубокий анализ платформ Grab vs Gojek"""
        
        # Определяем платформу по времени доставки (эвристика)
        df['platform_estimate'] = df['delivery_time'].apply(
            lambda x: 'gojek' if x < 25 else 'grab'
        )
        
        platform_analysis = {}
        
        for platform in ['grab', 'gojek']:
            platform_data = df[df['platform_estimate'] == platform]
            
            if len(platform_data) > 0:
                platform_analysis[platform] = {
                    'market_share_by_records': (len(platform_data) / len(df)) * 100,
                    'total_sales': platform_data['total_sales'].sum(),
                    'total_orders': platform_data['orders'].sum(),
                    'average_order_value': platform_data['total_sales'].sum() / platform_data['orders'].sum() if platform_data['orders'].sum() > 0 else 0,
                    'average_rating': platform_data['rating'].mean(),
                    'average_delivery_time': platform_data['delivery_time'].mean(),
                    'cancel_rate': platform_data['cancel_rate'].mean(),
                    'ads_adoption': (platform_data['ads_on'].sum() / len(platform_data)) * 100,
                    'average_roas': platform_data[platform_data['ads_on'] == 1]['roas'].mean(),
                    'restaurants_count': platform_data['restaurant_name'].nunique(),
                    'sales_per_restaurant': platform_data.groupby('restaurant_name')['total_sales'].sum().mean(),
                    'orders_per_restaurant': platform_data.groupby('restaurant_name')['orders'].sum().mean()
                }
        
        # Сравнительный анализ
        if 'grab' in platform_analysis and 'gojek' in platform_analysis:
            grab = platform_analysis['grab']
            gojek = platform_analysis['gojek']
            
            platform_analysis['comparison'] = {
                'sales_leader': 'grab' if grab['total_sales'] > gojek['total_sales'] else 'gojek',
                'orders_leader': 'grab' if grab['total_orders'] > gojek['total_orders'] else 'gojek',
                'efficiency_leader': 'grab' if grab['average_order_value'] > gojek['average_order_value'] else 'gojek',
                'service_leader': 'grab' if grab['average_rating'] > gojek['average_rating'] else 'gojek',
                'speed_leader': 'grab' if grab['average_delivery_time'] < gojek['average_delivery_time'] else 'gojek',
                'reliability_leader': 'grab' if grab['cancel_rate'] < gojek['cancel_rate'] else 'gojek',
                'ads_adoption_leader': 'grab' if grab['ads_adoption'] > gojek['ads_adoption'] else 'gojek',
                'roas_leader': 'grab' if grab['average_roas'] > gojek['average_roas'] else 'gojek',
                
                'sales_difference_pct': abs(grab['total_sales'] - gojek['total_sales']) / min(grab['total_sales'], gojek['total_sales']) * 100,
                'aov_difference_pct': abs(grab['average_order_value'] - gojek['average_order_value']) / min(grab['average_order_value'], gojek['average_order_value']) * 100,
                'delivery_time_difference': abs(grab['average_delivery_time'] - gojek['average_delivery_time']),
                'roas_difference_pct': abs(grab['average_roas'] - gojek['average_roas']) / min(grab['average_roas'], gojek['average_roas']) * 100 if not pd.isna(grab['average_roas']) and not pd.isna(gojek['average_roas']) else 0
            }
        
        return platform_analysis
    
    def _analyze_restaurant_performance(self, df: pd.DataFrame) -> Dict:
        """Анализ эффективности ресторанов"""
        
        restaurant_stats = df.groupby('restaurant_name').agg({
            'total_sales': ['sum', 'mean'],
            'orders': ['sum', 'mean'],
            'rating': 'mean',
            'delivery_time': 'mean',
            'cancel_rate': 'mean',
            'ads_on': ['sum', 'count'],
            'roas': 'mean'
        }).round(2)
        
        # Упрощаем колонки
        restaurant_stats.columns = [
            'total_sales', 'avg_daily_sales', 'total_orders', 'avg_daily_orders',
            'avg_rating', 'avg_delivery_time', 'avg_cancel_rate', 
            'ads_days', 'total_days', 'avg_roas'
        ]
        
        restaurant_stats['ads_adoption_pct'] = (restaurant_stats['ads_days'] / restaurant_stats['total_days']) * 100
        restaurant_stats['avg_order_value'] = restaurant_stats['total_sales'] / restaurant_stats['total_orders']
        
        # Топ и аутсайдеры
        top_by_sales = restaurant_stats.nlargest(5, 'total_sales')
        bottom_by_sales = restaurant_stats.nsmallest(5, 'total_sales')
        
        top_by_orders = restaurant_stats.nlargest(5, 'total_orders')
        top_by_efficiency = restaurant_stats.nlargest(5, 'avg_order_value')
        top_by_rating = restaurant_stats.nlargest(5, 'avg_rating')
        
        return {
            'total_restaurants': len(restaurant_stats),
            'top_performers': {
                'by_sales': top_by_sales[['total_sales', 'avg_daily_sales', 'total_orders']].to_dict('index'),
                'by_orders': top_by_orders[['total_orders', 'avg_daily_orders', 'total_sales']].to_dict('index'),
                'by_efficiency': top_by_efficiency[['avg_order_value', 'total_sales', 'total_orders']].to_dict('index'),
                'by_rating': top_by_rating[['avg_rating', 'total_sales', 'total_orders']].to_dict('index')
            },
            'underperformers': {
                'by_sales': bottom_by_sales[['total_sales', 'avg_daily_sales', 'total_orders']].to_dict('index')
            },
            'market_distribution': {
                'sales_concentration': (top_by_sales['total_sales'].sum() / restaurant_stats['total_sales'].sum()) * 100,
                'orders_concentration': (top_by_orders['total_orders'].sum() / restaurant_stats['total_orders'].sum()) * 100,
                'average_aov': restaurant_stats['avg_order_value'].mean(),
                'aov_std': restaurant_stats['avg_order_value'].std(),
                'rating_distribution': {
                    '4.8+': len(restaurant_stats[restaurant_stats['avg_rating'] >= 4.8]),
                    '4.5-4.8': len(restaurant_stats[(restaurant_stats['avg_rating'] >= 4.5) & (restaurant_stats['avg_rating'] < 4.8)]),
                    '4.0-4.5': len(restaurant_stats[(restaurant_stats['avg_rating'] >= 4.0) & (restaurant_stats['avg_rating'] < 4.5)]),
                    '<4.0': len(restaurant_stats[restaurant_stats['avg_rating'] < 4.0])
                }
            }
        }
    
    def _analyze_advertising_trends(self, df: pd.DataFrame) -> Dict:
        """Анализ рекламных трендов и эффективности"""
        
        ads_data = df[df['ads_on'] == 1].copy()
        no_ads_data = df[df['ads_on'] == 0].copy()
        
        if len(ads_data) == 0 or len(no_ads_data) == 0:
            return {"error": "Недостаточно данных для анализа рекламы"}
        
        # Базовая статистика рекламы
        ads_stats = {
            'total_ads_days': len(ads_data),
            'ads_penetration': (len(ads_data) / len(df)) * 100,
            'average_roas': ads_data['roas'].mean(),
            'median_roas': ads_data['roas'].median(),
            'roas_std': ads_data['roas'].std(),
            'best_roas': ads_data['roas'].max(),
            'worst_roas': ads_data['roas'].min()
        }
        
        # Эффективность рекламы vs без рекламы
        ads_performance = {
            'avg_sales_with_ads': ads_data['total_sales'].mean(),
            'avg_sales_without_ads': no_ads_data['total_sales'].mean(),
            'avg_orders_with_ads': ads_data['orders'].mean(),
            'avg_orders_without_ads': no_ads_data['orders'].mean(),
            'avg_rating_with_ads': ads_data['rating'].mean(),
            'avg_rating_without_ads': no_ads_data['rating'].mean()
        }
        
        ads_performance['sales_lift'] = ((ads_performance['avg_sales_with_ads'] - ads_performance['avg_sales_without_ads']) / ads_performance['avg_sales_without_ads']) * 100
        ads_performance['orders_lift'] = ((ads_performance['avg_orders_with_ads'] - ads_performance['avg_orders_without_ads']) / ads_performance['avg_orders_without_ads']) * 100
        
        # Анализ по ресторанам
        restaurant_ads_analysis = df.groupby('restaurant_name').agg({
            'ads_on': ['sum', 'count'],
            'roas': 'mean',
            'total_sales': 'mean'
        }).round(2)
        
        restaurant_ads_analysis.columns = ['ads_days', 'total_days', 'avg_roas', 'avg_daily_sales']
        restaurant_ads_analysis['ads_adoption_pct'] = (restaurant_ads_analysis['ads_days'] / restaurant_ads_analysis['total_days']) * 100
        
        # Категории по использованию рекламы
        heavy_advertisers = restaurant_ads_analysis[restaurant_ads_analysis['ads_adoption_pct'] > 80]
        light_advertisers = restaurant_ads_analysis[restaurant_ads_analysis['ads_adoption_pct'] < 20]
        moderate_advertisers = restaurant_ads_analysis[
            (restaurant_ads_analysis['ads_adoption_pct'] >= 20) & 
            (restaurant_ads_analysis['ads_adoption_pct'] <= 80)
        ]
        
        # ROAS по дням недели
        roas_by_weekday = ads_data.groupby('day_of_week')['roas'].mean().round(2).to_dict()
        orders_by_weekday_ads = ads_data.groupby('day_of_week')['orders'].mean().round(2).to_dict()
        
        return {
            'basic_stats': ads_stats,
            'performance_comparison': ads_performance,
            'advertiser_segments': {
                'heavy_advertisers': {
                    'count': len(heavy_advertisers),
                    'avg_roas': heavy_advertisers['avg_roas'].mean(),
                    'avg_daily_sales': heavy_advertisers['avg_daily_sales'].mean(),
                    'names': heavy_advertisers.index.tolist()[:5]
                },
                'moderate_advertisers': {
                    'count': len(moderate_advertisers),
                    'avg_roas': moderate_advertisers['avg_roas'].mean(),
                    'avg_daily_sales': moderate_advertisers['avg_daily_sales'].mean()
                },
                'light_advertisers': {
                    'count': len(light_advertisers),
                    'avg_roas': light_advertisers['avg_roas'].mean() if len(light_advertisers) > 0 else 0,
                    'avg_daily_sales': light_advertisers['avg_daily_sales'].mean() if len(light_advertisers) > 0 else 0,
                    'names': light_advertisers.index.tolist()[:5] if len(light_advertisers) > 0 else []
                }
            },
            'temporal_patterns': {
                'roas_by_weekday': roas_by_weekday,
                'orders_by_weekday_with_ads': orders_by_weekday_ads,
                'best_ads_day': max(roas_by_weekday, key=roas_by_weekday.get) if roas_by_weekday else None,
                'worst_ads_day': min(roas_by_weekday, key=roas_by_weekday.get) if roas_by_weekday else None
            }
        }
    
    def _analyze_operational_metrics(self, df: pd.DataFrame) -> Dict:
        """Анализ операционных метрик"""
        
        # Временные тренды ключевых метрик
        monthly_trends = df.groupby(df['date'].dt.to_period('M')).agg({
            'delivery_time': 'mean',
            'cancel_rate': 'mean',
            'rating': 'mean',
            'orders': 'sum',
            'total_sales': 'sum'
        }).round(2)
        
        # Распределение времени доставки
        delivery_distribution = {
            'under_20min': len(df[df['delivery_time'] < 20]),
            '20_30min': len(df[(df['delivery_time'] >= 20) & (df['delivery_time'] < 30)]),
            '30_40min': len(df[(df['delivery_time'] >= 30) & (df['delivery_time'] < 40)]),
            'over_40min': len(df[df['delivery_time'] >= 40])
        }
        
        # Анализ отмен
        cancel_analysis = {
            'total_orders': df['orders'].sum(),
            'estimated_cancellations': (df['orders'] * df['cancel_rate']).sum(),
            'cancel_rate_distribution': {
                'excellent_0_2pct': len(df[df['cancel_rate'] < 0.02]),
                'good_2_5pct': len(df[(df['cancel_rate'] >= 0.02) & (df['cancel_rate'] < 0.05)]),
                'average_5_10pct': len(df[(df['cancel_rate'] >= 0.05) & (df['cancel_rate'] < 0.10)]),
                'poor_over_10pct': len(df[df['cancel_rate'] >= 0.10])
            },
            'worst_performers': df.nlargest(5, 'cancel_rate')[['restaurant_name', 'cancel_rate', 'orders']].to_dict('records')
        }
        
        # Корреляция операционных метрик
        operational_correlations = df[['delivery_time', 'cancel_rate', 'rating', 'orders', 'total_sales']].corr().round(3).to_dict()
        
        return {
            'delivery_performance': {
                'average_delivery_time': df['delivery_time'].mean(),
                'delivery_time_std': df['delivery_time'].std(),
                'fastest_restaurants': df.nsmallest(5, 'delivery_time')[['restaurant_name', 'delivery_time']].to_dict('records'),
                'slowest_restaurants': df.nlargest(5, 'delivery_time')[['restaurant_name', 'delivery_time']].to_dict('records'),
                'distribution': delivery_distribution
            },
            'cancellation_analysis': cancel_analysis,
            'rating_trends': {
                'average_rating': df['rating'].mean(),
                'rating_std': df['rating'].std(),
                'top_rated': df.nlargest(5, 'rating')[['restaurant_name', 'rating']].to_dict('records'),
                'lowest_rated': df.nsmallest(5, 'rating')[['restaurant_name', 'rating']].to_dict('records')
            },
            'operational_correlations': operational_correlations,
            'monthly_trends': monthly_trends.to_dict() if not monthly_trends.empty else {}
        }
    
    def _analyze_temporal_patterns(self, period_df: pd.DataFrame, full_df: pd.DataFrame) -> Dict:
        """Анализ временных паттернов и сравнение с предыдущими периодами"""
        
        period_start = period_df['date'].min()
        period_end = period_df['date'].max()
        period_days = (period_end - period_start).days + 1
        
        # Сравнение с тем же периодом прошлого года (YoY)
        yoy_start = period_start - pd.DateOffset(years=1)
        yoy_end = period_end - pd.DateOffset(years=1)
        yoy_df = full_df[(full_df['date'] >= yoy_start) & (full_df['date'] <= yoy_end)].copy()
        
        # Сравнение с предыдущим кварталом (QoQ)
        qoq_start = period_start - pd.DateOffset(months=3)
        qoq_end = period_end - pd.DateOffset(months=3)
        qoq_df = full_df[(full_df['date'] >= qoq_start) & (full_df['date'] <= qoq_end)].copy()
        
        # Метрики текущего периода
        current_metrics = {
            'total_sales': period_df['total_sales'].sum(),
            'total_orders': period_df['orders'].sum(),
            'avg_rating': period_df['rating'].mean(),
            'avg_delivery_time': period_df['delivery_time'].mean(),
            'avg_cancel_rate': period_df['cancel_rate'].mean(),
            'avg_roas': period_df[period_df['ads_on'] == 1]['roas'].mean()
        }
        
        temporal_analysis = {
            'current_period': current_metrics,
            'comparisons': {}
        }
        
        # YoY сравнение
        if len(yoy_df) > 0:
            yoy_metrics = {
                'total_sales': yoy_df['total_sales'].sum(),
                'total_orders': yoy_df['orders'].sum(),
                'avg_rating': yoy_df['rating'].mean(),
                'avg_delivery_time': yoy_df['delivery_time'].mean(),
                'avg_cancel_rate': yoy_df['cancel_rate'].mean(),
                'avg_roas': yoy_df[yoy_df['ads_on'] == 1]['roas'].mean()
            }
            
            yoy_changes = {}
            for metric in current_metrics:
                if not pd.isna(current_metrics[metric]) and not pd.isna(yoy_metrics[metric]) and yoy_metrics[metric] != 0:
                    yoy_changes[metric] = ((current_metrics[metric] - yoy_metrics[metric]) / yoy_metrics[metric]) * 100
                else:
                    yoy_changes[metric] = 0
            
            temporal_analysis['comparisons']['year_over_year'] = {
                'previous_period': yoy_metrics,
                'changes_pct': yoy_changes
            }
        
        # QoQ сравнение
        if len(qoq_df) > 0:
            qoq_metrics = {
                'total_sales': qoq_df['total_sales'].sum(),
                'total_orders': qoq_df['orders'].sum(),
                'avg_rating': qoq_df['rating'].mean(),
                'avg_delivery_time': qoq_df['delivery_time'].mean(),
                'avg_cancel_rate': qoq_df['cancel_rate'].mean(),
                'avg_roas': qoq_df[qoq_df['ads_on'] == 1]['roas'].mean()
            }
            
            qoq_changes = {}
            for metric in current_metrics:
                if not pd.isna(current_metrics[metric]) and not pd.isna(qoq_metrics[metric]) and qoq_metrics[metric] != 0:
                    qoq_changes[metric] = ((current_metrics[metric] - qoq_metrics[metric]) / qoq_metrics[metric]) * 100
                else:
                    qoq_changes[metric] = 0
            
            temporal_analysis['comparisons']['quarter_over_quarter'] = {
                'previous_period': qoq_metrics,
                'changes_pct': qoq_changes
            }
        
        # Недельные паттерны
        weekly_patterns = period_df.groupby('day_of_week').agg({
            'total_sales': 'mean',
            'orders': 'mean',
            'rating': 'mean'
        }).round(2).to_dict()
        
        temporal_analysis['weekly_patterns'] = weekly_patterns
        
        return temporal_analysis
    
    def _detect_market_anomalies(self, df: pd.DataFrame) -> Dict:
        """Обнаружение рыночных аномалий"""
        
        # Аномалии по продажам
        df['sales_zscore'] = np.abs((df['total_sales'] - df['total_sales'].mean()) / df['total_sales'].std())
        sales_anomalies = df[df['sales_zscore'] > 2.5].nlargest(10, 'sales_zscore')
        
        # Аномалии по ROAS
        roas_data = df[df['ads_on'] == 1].copy()
        if len(roas_data) > 0:
            roas_data['roas_zscore'] = np.abs((roas_data['roas'] - roas_data['roas'].mean()) / roas_data['roas'].std())
            roas_anomalies = roas_data[roas_data['roas_zscore'] > 2].nlargest(10, 'roas_zscore')
        else:
            roas_anomalies = pd.DataFrame()
        
        # Аномалии по времени доставки
        df['delivery_zscore'] = np.abs((df['delivery_time'] - df['delivery_time'].mean()) / df['delivery_time'].std())
        delivery_anomalies = df[df['delivery_zscore'] > 2].nlargest(10, 'delivery_zscore')
        
        return {
            'sales_anomalies': sales_anomalies[['restaurant_name', 'date', 'total_sales', 'sales_zscore']].to_dict('records'),
            'roas_anomalies': roas_anomalies[['restaurant_name', 'date', 'roas', 'roas_zscore']].to_dict('records') if not roas_anomalies.empty else [],
            'delivery_anomalies': delivery_anomalies[['restaurant_name', 'date', 'delivery_time', 'delivery_zscore']].to_dict('records'),
            'summary': {
                'total_sales_anomalies': len(sales_anomalies),
                'total_roas_anomalies': len(roas_anomalies) if not roas_anomalies.empty else 0,
                'total_delivery_anomalies': len(delivery_anomalies)
            }
        }
    
    def _generate_strategic_recommendations(self, analysis: Dict) -> List[str]:
        """Генерация стратегических рекомендаций топ-маркетолога"""
        recommendations = []
        
        # Анализ платформ
        if 'comparison' in analysis.get('platform_analysis', {}):
            comparison = analysis['platform_analysis']['comparison']
            
            # Рекомендации по платформам
            if comparison['sales_difference_pct'] > 20:
                leader = comparison['sales_leader']
                recommendations.append(f"🎯 КРИТИЧНО: {leader.title()} доминирует в продажах с разрывом {comparison['sales_difference_pct']:.1f}%. Пересмотреть стратегию на отстающей платформе.")
            
            if comparison['roas_difference_pct'] > 30:
                leader = comparison['roas_leader']
                recommendations.append(f"💰 РЕКЛАМА: {leader.title()} показывает значительно лучший ROAS. Перераспределить рекламный бюджет.")
        
        # Анализ рекламы
        ads_analysis = analysis.get('advertising_intelligence', {})
        if 'performance_comparison' in ads_analysis:
            perf = ads_analysis['performance_comparison']
            if perf['sales_lift'] > 50:
                recommendations.append(f"🚀 ВОЗМОЖНОСТЬ: Реклама дает {perf['sales_lift']:.1f}% прирост продаж. Увеличить рекламные бюджеты.")
            elif perf['sales_lift'] < 10:
                recommendations.append(f"⚠️ ПРОБЛЕМА: Реклама дает только {perf['sales_lift']:.1f}% прирост. Пересмотреть креативы и таргетинг.")
        
        # Анализ операций
        ops_analysis = analysis.get('operational_insights', {})
        if 'cancellation_analysis' in ops_analysis:
            cancel_analysis = ops_analysis['cancellation_analysis']
            cancel_rate = cancel_analysis['estimated_cancellations'] / cancel_analysis['total_orders'] if cancel_analysis['total_orders'] > 0 else 0
            if cancel_rate > 0.08:
                recommendations.append(f"🔴 СРОЧНО: Высокий уровень отмен ({cancel_rate*100:.1f}%). Проверить качество кухни и доставки.")
        
        # Временной анализ
        temporal = analysis.get('temporal_analysis', {})
        if 'comparisons' in temporal:
            if 'year_over_year' in temporal['comparisons']:
                yoy = temporal['comparisons']['year_over_year']['changes_pct']
                if yoy.get('total_sales', 0) < -10:
                    recommendations.append(f"📉 ТРЕВОГА: Продажи упали на {abs(yoy['total_sales']):.1f}% год к году. Требуется антикризисная стратегия.")
                elif yoy.get('total_sales', 0) > 30:
                    recommendations.append(f"📈 УСПЕХ: Рост продаж {yoy['total_sales']:.1f}% год к году. Масштабировать успешные практики.")
        
        # Аномалии
        anomalies = analysis.get('market_anomalies', {})
        if anomalies.get('summary', {}).get('total_sales_anomalies', 0) > 10:
            recommendations.append("🔍 ВНИМАНИЕ: Обнаружено много аномалий в продажах. Провести детальное расследование.")
        
        return recommendations
    
    def _generate_expert_hypotheses(self, analysis: Dict, full_df: pd.DataFrame) -> List[str]:
        """Генерация экспертных гипотез на основе данных"""
        hypotheses = []
        
        # Гипотезы по платформам
        platform_analysis = analysis.get('platform_analysis', {})
        if 'comparison' in platform_analysis:
            comp = platform_analysis['comparison']
            if comp['delivery_time_difference'] > 5:
                fast_platform = comp['speed_leader']
                hypotheses.append(f"💡 ГИПОТЕЗА: {fast_platform.title()} быстрее доставляет на {comp['delivery_time_difference']:.1f} мин. Возможно, лучшая логистика или больше курьеров.")
            
            if comp['aov_difference_pct'] > 15:
                efficient_platform = comp['efficiency_leader']
                hypotheses.append(f"💰 ГИПОТЕЗА: {efficient_platform.title()} имеет выше средний чек. Возможно, лучший ассортимент или клиентская база.")
        
        # Гипотезы по рекламе
        ads_intel = analysis.get('advertising_intelligence', {})
        if 'advertiser_segments' in ads_intel:
            heavy = ads_intel['advertiser_segments']['heavy_advertisers']
            light = ads_intel['advertiser_segments']['light_advertisers']
            
            if heavy['avg_daily_sales'] > light['avg_daily_sales'] * 2:
                hypotheses.append("📢 ГИПОТЕЗА: Активная реклама создает экспоненциальный эффект роста, а не линейный.")
            
            if 'temporal_patterns' in ads_intel:
                patterns = ads_intel['temporal_patterns']
                if patterns.get('best_ads_day') and patterns.get('worst_ads_day'):
                    best_day = patterns['best_ads_day']
                    worst_day = patterns['worst_ads_day']
                    hypotheses.append(f"📅 ГИПОТЕЗА: {best_day} - лучший день для рекламы, {worst_day} - худший. Возможно, связано с поведением потребителей.")
        
        # Гипотезы по операциям
        ops = analysis.get('operational_insights', {})
        if 'operational_correlations' in ops:
            corr = ops['operational_correlations']
            delivery_rating_corr = corr.get('delivery_time', {}).get('rating', 0)
            if delivery_rating_corr < -0.3:
                hypotheses.append("⏱️ ГИПОТЕЗА: Время доставки критично влияет на рейтинг. Каждая минута задержки снижает лояльность.")
        
        # Гипотезы по временным трендам
        temporal = analysis.get('temporal_analysis', {})
        if 'weekly_patterns' in temporal:
            weekly = temporal['weekly_patterns']
            if 'total_sales' in weekly:
                sales_by_day = weekly['total_sales']
                max_day = max(sales_by_day, key=sales_by_day.get)
                min_day = min(sales_by_day, key=sales_by_day.get)
                difference = (sales_by_day[max_day] - sales_by_day[min_day]) / sales_by_day[min_day] * 100
                
                if difference > 50:
                    weekdays = ['Пн', 'Вт', 'Ср', 'Чт', 'Пт', 'Сб', 'Вс']
                    hypotheses.append(f"📊 ГИПОТЕЗА: {weekdays[max_day]} превосходит {weekdays[min_day]} на {difference:.1f}%. Возможно, связано с зарплатами или досугом.")
        
        # Гипотезы по сезонности (если есть данные за разные месяцы)
        current_period = analysis.get('period', '')
        if 'апрель' in current_period.lower() or 'april' in current_period.lower():
            hypotheses.append("🌸 ГИПОТЕЗА: Весенний период может показывать рост из-за улучшения погоды и настроения клиентов.")
        
        return hypotheses

def generate_market_intelligence_report(start_date: str, end_date: str) -> Dict:
    """
    Генерирует комплексный рыночный анализ всей базы ресторанов
    """
    logger.info(f"Генерация рыночной аналитики за период {start_date} - {end_date}")
    
    try:
        # Загружаем полную базу данных
        full_df = load_data_with_all_features()
        if full_df.empty:
            return {"error": "Нет данных для анализа"}
        
        # Создаем движок рыночной аналитики
        market_engine = MarketIntelligenceEngine()
        
        # Проводим комплексный анализ
        analysis = market_engine.analyze_market_overview(full_df, start_date, end_date)
        
        # Добавляем метаданные
        analysis['metadata'] = {
            'total_data_points': len(full_df),
            'data_period': f"{full_df['date'].min().strftime('%Y-%m-%d')} - {full_df['date'].max().strftime('%Y-%m-%d')}",
            'analysis_period': f"{start_date} - {end_date}",
            'generated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'analysis_type': 'market_intelligence'
        }
        
        return analysis
        
    except Exception as e:
        logger.error(f"Ошибка генерации рыночной аналитики: {e}")
        return {"error": f"Ошибка анализа: {str(e)}"}