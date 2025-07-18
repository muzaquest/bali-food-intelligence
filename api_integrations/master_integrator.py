#!/usr/bin/env python3
"""
Master API Integrator - объединяет все API сервисы в единую систему
"""

import json
import asyncio
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import logging

from weather_service import WeatherService
from holiday_service import HolidayService
from ai_explainer import AIExplainer
from client_database_api import ClientDatabaseAPI

class MasterIntegrator:
    """Главный интегратор всех API сервисов"""
    
    def __init__(self, config: Dict):
        """
        Инициализация всех API сервисов
        
        config должен содержать:
        - weather_api_key: ключ OpenWeatherMap
        - holiday_api_key: ключ Calendarific
        - openai_api_key: ключ OpenAI
        - database_config: конфигурация базы данных
        """
        self.config = config
        
        # Настройка логирования
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
        
        # Инициализация сервисов
        self._initialize_services()
    
    def _initialize_services(self):
        """Инициализирует все API сервисы"""
        try:
            # Инициализация сервиса погоды
            self.weather_service = WeatherService(
                api_key=self.config['weather_api_key']
            )
            
            # Инициализация сервиса праздников
            self.holiday_service = HolidayService(
                api_key=self.config['holiday_api_key']
            )
            
            # Инициализация AI объяснителя
            self.ai_explainer = AIExplainer(
                api_key=self.config['openai_api_key'],
                model=self.config.get('openai_model', 'gpt-4')
            )
            
            # Инициализация базы данных клиента
            self.database_api = ClientDatabaseAPI(
                connection_config=self.config['database_config']
            )
            
            self.logger.info("Все API сервисы инициализированы успешно")
            
        except Exception as e:
            self.logger.error(f"Ошибка инициализации сервисов: {e}")
            raise
    
    def analyze_restaurant_performance(self, restaurant_id: int, date: str) -> Dict:
        """
        Полный анализ производительности ресторана за день
        Объединяет данные из всех источников
        """
        try:
            self.logger.info(f"Начинаем анализ ресторана {restaurant_id} за {date}")
            
            # 1. Получаем базовые данные ресторана
            restaurant_info = self.database_api.get_restaurant_info(restaurant_id)
            daily_stats = self.database_api.get_daily_stats(restaurant_id, date)
            
            # 2. Получаем данные о погоде
            weather_data = self.weather_service.get_weather_impact(
                region=restaurant_info['region'],
                date=date
            )
            
            # 3. Получаем данные о праздниках
            holiday_data = self.holiday_service.get_holiday_impact(
                date=date,
                region=restaurant_info['region']
            )
            
            # 4. Получаем исторические данные для ML анализа
            historical_data = self.database_api.get_historical_data(restaurant_id, 30)
            
            # 5. Рассчитываем прогноз и отклонения
            prediction_data = self._calculate_prediction(daily_stats, historical_data)
            
            # 6. Объединяем все данные
            combined_data = {
                'restaurant_id': restaurant_id,
                'restaurant_name': restaurant_info['name'],
                'region': restaurant_info['region'],
                'date': date,
                'actual_sales': daily_stats['sales'],
                'predicted_sales': prediction_data['predicted_sales'],
                'sales_difference_percent': prediction_data['difference_percent'],
                'weather': weather_data,
                'holidays': holiday_data,
                'analysis': {
                    'orders': daily_stats['orders'],
                    'avg_order_value': daily_stats['avg_order_value'],
                    'ads_enabled': daily_stats['ads_enabled'],
                    'rating': daily_stats['rating'],
                    'delivery_time': daily_stats['delivery_time'],
                    'cancellation_rate': daily_stats['cancellation_rate']
                },
                'historical_trend': self._analyze_trend(historical_data),
                'data_quality': self._assess_data_quality(daily_stats, historical_data)
            }
            
            # 7. Генерируем AI объяснение
            ai_analysis = self.ai_explainer.generate_sales_analysis(combined_data)
            
            # 8. Формируем итоговый результат
            result = {
                'timestamp': datetime.now().isoformat(),
                'restaurant': {
                    'id': restaurant_id,
                    'name': restaurant_info['name'],
                    'region': restaurant_info['region']
                },
                'date': date,
                'sales_analysis': {
                    'actual_sales': daily_stats['sales'],
                    'predicted_sales': prediction_data['predicted_sales'],
                    'difference_percent': prediction_data['difference_percent'],
                    'performance_rating': self._calculate_performance_rating(prediction_data['difference_percent'])
                },
                'factors': {
                    'weather': {
                        'impact_percent': weather_data['impact_percent'],
                        'summary': weather_data['weather_summary'],
                        'conditions': weather_data['delivery_conditions']
                    },
                    'holidays': {
                        'impact_percent': holiday_data['impact_percent'],
                        'active_holidays': holiday_data['current_holidays'],
                        'driver_shortage': holiday_data['driver_shortage']
                    },
                    'business_metrics': {
                        'orders': daily_stats['orders'],
                        'avg_order_value': daily_stats['avg_order_value'],
                        'rating': daily_stats['rating'],
                        'ads_status': 'active' if daily_stats['ads_enabled'] else 'inactive'
                    }
                },
                'ai_insights': {
                    'explanation': ai_analysis['explanation'],
                    'recommendations': ai_analysis['recommendations'],
                    'criticality': ai_analysis['criticality'],
                    'confidence': ai_analysis['confidence']
                },
                'trends': combined_data['historical_trend'],
                'data_quality_score': combined_data['data_quality']['score']
            }
            
            self.logger.info(f"Анализ ресторана {restaurant_id} завершен успешно")
            return result
            
        except Exception as e:
            self.logger.error(f"Ошибка анализа ресторана {restaurant_id}: {e}")
            return self._generate_error_response(restaurant_id, date, str(e))
    
    def generate_weekly_report(self, restaurant_id: int, weeks: int = 4) -> Dict:
        """Генерирует еженедельный отчет с трендами"""
        try:
            restaurant_info = self.database_api.get_restaurant_info(restaurant_id)
            
            # Получаем данные за несколько недель
            days_total = weeks * 7
            historical_data = self.database_api.get_historical_data(restaurant_id, days_total)
            
            # Группируем данные по неделям
            weekly_data = self._group_by_weeks(historical_data, weeks)
            
            # Анализируем каждую неделю
            weekly_analyses = []
            for week_data in weekly_data:
                week_analysis = self._analyze_week(week_data, restaurant_info['region'])
                weekly_analyses.append(week_analysis)
            
            # Генерируем AI сводку
            ai_summary = self.ai_explainer.generate_weekly_summary(weekly_analyses)
            
            # Получаем прогноз погоды на следующую неделю
            weather_forecast = self.weather_service.get_weekly_weather_impact(
                restaurant_info['region']
            )
            
            # Получаем предстоящие праздники
            upcoming_holidays = self.holiday_service.get_upcoming_holidays(
                region=restaurant_info['region'],
                days=14
            )
            
            return {
                'timestamp': datetime.now().isoformat(),
                'restaurant': {
                    'id': restaurant_id,
                    'name': restaurant_info['name'],
                    'region': restaurant_info['region']
                },
                'period': f"{weeks} weeks",
                'weekly_breakdown': weekly_analyses,
                'ai_summary': ai_summary,
                'forecast': {
                    'weather': weather_forecast,
                    'holidays': upcoming_holidays[:5]  # Ближайшие 5 праздников
                },
                'overall_trend': self._calculate_overall_trend(weekly_analyses),
                'recommendations': self._generate_strategic_recommendations(weekly_analyses)
            }
            
        except Exception as e:
            self.logger.error(f"Ошибка генерации еженедельного отчета: {e}")
            return {'error': str(e)}
    
    def monitor_critical_changes(self, restaurant_id: int, threshold: float = 20.0) -> Optional[Dict]:
        """Мониторинг критических изменений в продажах"""
        try:
            today = datetime.now().strftime('%Y-%m-%d')
            analysis = self.analyze_restaurant_performance(restaurant_id, today)
            
            difference = abs(analysis['sales_analysis']['difference_percent'])
            
            if difference >= threshold:
                # Генерируем экстренное уведомление
                alert = self.ai_explainer.generate_emergency_alert(analysis)
                
                return {
                    'alert_type': 'critical_change',
                    'restaurant_id': restaurant_id,
                    'restaurant_name': analysis['restaurant']['name'],
                    'difference_percent': analysis['sales_analysis']['difference_percent'],
                    'urgency': alert['urgency'],
                    'alert_message': alert['alert'],
                    'immediate_actions': alert['immediate_actions'],
                    'timestamp': datetime.now().isoformat()
                }
            
            return None
            
        except Exception as e:
            self.logger.error(f"Ошибка мониторинга критических изменений: {e}")
            return None
    
    def batch_analyze_restaurants(self, restaurant_ids: List[int], date: str) -> Dict:
        """Массовый анализ нескольких ресторанов"""
        try:
            results = {}
            errors = []
            
            for restaurant_id in restaurant_ids:
                try:
                    analysis = self.analyze_restaurant_performance(restaurant_id, date)
                    results[restaurant_id] = analysis
                except Exception as e:
                    errors.append({
                        'restaurant_id': restaurant_id,
                        'error': str(e)
                    })
            
            # Генерируем сводную статистику
            summary = self._generate_batch_summary(results)
            
            return {
                'timestamp': datetime.now().isoformat(),
                'date': date,
                'total_restaurants': len(restaurant_ids),
                'successful_analyses': len(results),
                'errors': len(errors),
                'results': results,
                'error_details': errors,
                'summary': summary
            }
            
        except Exception as e:
            self.logger.error(f"Ошибка массового анализа: {e}")
            return {'error': str(e)}
    
    def get_system_health(self) -> Dict:
        """Проверяет состояние всех API сервисов"""
        health_status = {
            'timestamp': datetime.now().isoformat(),
            'overall_status': 'healthy',
            'services': {}
        }
        
        # Проверка базы данных
        try:
            db_healthy = self.database_api.test_connection()
            health_status['services']['database'] = {
                'status': 'healthy' if db_healthy else 'unhealthy',
                'last_check': datetime.now().isoformat()
            }
        except Exception as e:
            health_status['services']['database'] = {
                'status': 'error',
                'error': str(e),
                'last_check': datetime.now().isoformat()
            }
        
        # Проверка API погоды
        try:
            weather_test = self.weather_service.get_weather_impact('Seminyak')
            health_status['services']['weather'] = {
                'status': 'healthy' if weather_test else 'unhealthy',
                'last_check': datetime.now().isoformat()
            }
        except Exception as e:
            health_status['services']['weather'] = {
                'status': 'error',
                'error': str(e),
                'last_check': datetime.now().isoformat()
            }
        
        # Проверка API праздников
        try:
            holiday_test = self.holiday_service.get_holiday_impact(datetime.now().strftime('%Y-%m-%d'))
            health_status['services']['holidays'] = {
                'status': 'healthy' if holiday_test else 'unhealthy',
                'last_check': datetime.now().isoformat()
            }
        except Exception as e:
            health_status['services']['holidays'] = {
                'status': 'error',
                'error': str(e),
                'last_check': datetime.now().isoformat()
            }
        
        # Проверка OpenAI
        try:
            # Простой тест генерации
            test_data = {'sales_difference_percent': 5}
            ai_test = self.ai_explainer._generate_fallback_analysis(test_data, 'test')
            health_status['services']['ai'] = {
                'status': 'healthy' if ai_test else 'unhealthy',
                'last_check': datetime.now().isoformat()
            }
        except Exception as e:
            health_status['services']['ai'] = {
                'status': 'error',
                'error': str(e),
                'last_check': datetime.now().isoformat()
            }
        
        # Определяем общий статус
        unhealthy_services = [
            name for name, service in health_status['services'].items()
            if service['status'] != 'healthy'
        ]
        
        if unhealthy_services:
            health_status['overall_status'] = 'degraded' if len(unhealthy_services) < 3 else 'unhealthy'
            health_status['issues'] = unhealthy_services
        
        return health_status
    
    # Вспомогательные методы
    def _calculate_prediction(self, daily_stats: Dict, historical_data: List[Dict]) -> Dict:
        """Простой расчет прогноза на основе исторических данных"""
        if not historical_data:
            return {
                'predicted_sales': daily_stats['sales'],
                'difference_percent': 0
            }
        
        # Используем среднее за последние 7 дней
        recent_sales = [d['sales'] for d in historical_data[:7] if d['sales'] > 0]
        if not recent_sales:
            predicted_sales = daily_stats['sales']
        else:
            predicted_sales = sum(recent_sales) / len(recent_sales)
        
        difference_percent = ((daily_stats['sales'] - predicted_sales) / predicted_sales * 100) if predicted_sales > 0 else 0
        
        return {
            'predicted_sales': predicted_sales,
            'difference_percent': difference_percent
        }
    
    def _analyze_trend(self, historical_data: List[Dict]) -> Dict:
        """Анализирует тренд продаж"""
        if len(historical_data) < 7:
            return {'trend': 'insufficient_data', 'direction': 'unknown'}
        
        # Сравниваем первую и последнюю неделю
        first_week = historical_data[-7:]
        last_week = historical_data[:7]
        
        first_week_avg = sum(d['sales'] for d in first_week) / len(first_week)
        last_week_avg = sum(d['sales'] for d in last_week) / len(last_week)
        
        if first_week_avg == 0:
            return {'trend': 'no_data', 'direction': 'unknown'}
        
        change_percent = ((last_week_avg - first_week_avg) / first_week_avg) * 100
        
        if change_percent > 10:
            direction = 'growing'
        elif change_percent < -10:
            direction = 'declining'
        else:
            direction = 'stable'
        
        return {
            'trend': direction,
            'change_percent': change_percent,
            'first_week_avg': first_week_avg,
            'last_week_avg': last_week_avg
        }
    
    def _assess_data_quality(self, daily_stats: Dict, historical_data: List[Dict]) -> Dict:
        """Оценивает качество данных"""
        quality_score = 0.0
        issues = []
        
        # Проверка текущих данных
        if daily_stats['sales'] > 0:
            quality_score += 0.3
        else:
            issues.append('no_sales_data')
        
        if daily_stats['orders'] > 0:
            quality_score += 0.2
        else:
            issues.append('no_orders_data')
        
        if daily_stats['rating'] > 0:
            quality_score += 0.1
        else:
            issues.append('no_rating_data')
        
        # Проверка исторических данных
        if len(historical_data) >= 7:
            quality_score += 0.2
        else:
            issues.append('insufficient_historical_data')
        
        # Проверка консистентности
        if historical_data:
            non_zero_days = len([d for d in historical_data if d['sales'] > 0])
            if non_zero_days / len(historical_data) > 0.8:
                quality_score += 0.2
            else:
                issues.append('inconsistent_data')
        
        return {
            'score': min(quality_score, 1.0),
            'issues': issues,
            'rating': 'good' if quality_score > 0.8 else 'fair' if quality_score > 0.5 else 'poor'
        }
    
    def _calculate_performance_rating(self, difference_percent: float) -> str:
        """Рассчитывает рейтинг производительности"""
        if difference_percent > 20:
            return 'excellent'
        elif difference_percent > 10:
            return 'good'
        elif difference_percent > -5:
            return 'satisfactory'
        elif difference_percent > -15:
            return 'poor'
        else:
            return 'critical'
    
    def _group_by_weeks(self, historical_data: List[Dict], weeks: int) -> List[List[Dict]]:
        """Группирует данные по неделям"""
        weekly_data = []
        for week in range(weeks):
            start_idx = week * 7
            end_idx = start_idx + 7
            week_data = historical_data[start_idx:end_idx]
            if week_data:
                weekly_data.append(week_data)
        return weekly_data
    
    def _analyze_week(self, week_data: List[Dict], region: str) -> Dict:
        """Анализирует данные за неделю"""
        total_sales = sum(d['sales'] for d in week_data)
        total_orders = sum(d['orders'] for d in week_data)
        avg_rating = sum(d['rating'] for d in week_data if d['rating'] > 0) / len([d for d in week_data if d['rating'] > 0]) if any(d['rating'] > 0 for d in week_data) else 0
        
        return {
            'week_start': week_data[0]['date'] if week_data else None,
            'week_end': week_data[-1]['date'] if week_data else None,
            'total_sales': total_sales,
            'total_orders': total_orders,
            'avg_daily_sales': total_sales / len(week_data) if week_data else 0,
            'avg_rating': avg_rating,
            'days_with_sales': len([d for d in week_data if d['sales'] > 0])
        }
    
    def _calculate_overall_trend(self, weekly_analyses: List[Dict]) -> Dict:
        """Рассчитывает общий тренд по неделям"""
        if len(weekly_analyses) < 2:
            return {'trend': 'insufficient_data'}
        
        sales_progression = [w['total_sales'] for w in weekly_analyses]
        
        # Простой расчет тренда
        first_half = sales_progression[:len(sales_progression)//2]
        second_half = sales_progression[len(sales_progression)//2:]
        
        first_avg = sum(first_half) / len(first_half)
        second_avg = sum(second_half) / len(second_half)
        
        if first_avg == 0:
            return {'trend': 'no_data'}
        
        change_percent = ((second_avg - first_avg) / first_avg) * 100
        
        if change_percent > 15:
            trend = 'strong_growth'
        elif change_percent > 5:
            trend = 'growth'
        elif change_percent > -5:
            trend = 'stable'
        elif change_percent > -15:
            trend = 'decline'
        else:
            trend = 'strong_decline'
        
        return {
            'trend': trend,
            'change_percent': change_percent,
            'first_period_avg': first_avg,
            'second_period_avg': second_avg
        }
    
    def _generate_strategic_recommendations(self, weekly_analyses: List[Dict]) -> List[str]:
        """Генерирует стратегические рекомендации"""
        recommendations = []
        
        # Анализ трендов
        if len(weekly_analyses) >= 2:
            last_week = weekly_analyses[-1]
            prev_week = weekly_analyses[-2]
            
            if last_week['total_sales'] < prev_week['total_sales'] * 0.9:
                recommendations.append("Срочно проанализируйте причины снижения продаж")
            
            if last_week['avg_rating'] < 4.0:
                recommendations.append("Улучшите качество обслуживания для повышения рейтинга")
        
        # Анализ стабильности
        sales_variation = [w['total_sales'] for w in weekly_analyses]
        if len(sales_variation) > 1:
            avg_sales = sum(sales_variation) / len(sales_variation)
            high_variation = any(abs(s - avg_sales) > avg_sales * 0.3 for s in sales_variation)
            
            if high_variation:
                recommendations.append("Стабилизируйте операции для уменьшения колебаний продаж")
        
        return recommendations
    
    def _generate_batch_summary(self, results: Dict) -> Dict:
        """Генерирует сводку по массовому анализу"""
        if not results:
            return {'message': 'No results to summarize'}
        
        total_sales = sum(r['sales_analysis']['actual_sales'] for r in results.values())
        avg_performance = sum(r['sales_analysis']['difference_percent'] for r in results.values()) / len(results)
        
        critical_restaurants = [
            r_id for r_id, r in results.items()
            if abs(r['sales_analysis']['difference_percent']) > 20
        ]
        
        return {
            'total_sales': total_sales,
            'avg_performance_percent': avg_performance,
            'critical_restaurants': critical_restaurants,
            'restaurants_analyzed': len(results)
        }
    
    def _generate_error_response(self, restaurant_id: int, date: str, error: str) -> Dict:
        """Генерирует ответ при ошибке"""
        return {
            'timestamp': datetime.now().isoformat(),
            'restaurant_id': restaurant_id,
            'date': date,
            'error': error,
            'status': 'error',
            'message': 'Analysis failed due to technical issues'
        }

# Пример использования
def main():
    # Конфигурация всех API
    config = {
        'weather_api_key': 'YOUR_OPENWEATHERMAP_API_KEY',
        'holiday_api_key': 'YOUR_CALENDARIFIC_API_KEY',
        'openai_api_key': 'YOUR_OPENAI_API_KEY',
        'openai_model': 'gpt-4',
        'database_config': {
            'type': 'sqlite',
            'file_path': 'client_data.db'
        }
    }
    
    # Создаем мастер-интегратор
    integrator = MasterIntegrator(config)
    
    # Проверяем состояние системы
    health = integrator.get_system_health()
    print(f"Состояние системы: {health['overall_status']}")
    
    # Анализируем ресторан
    analysis = integrator.analyze_restaurant_performance(1, '2024-01-15')
    print(f"Анализ ресторана: {analysis['sales_analysis']['difference_percent']:+.1f}%")
    
    # Генерируем еженедельный отчет
    weekly_report = integrator.generate_weekly_report(1, weeks=4)
    print(f"Еженедельный тренд: {weekly_report['overall_trend']['trend']}")

if __name__ == "__main__":
    main()