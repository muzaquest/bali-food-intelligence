#!/usr/bin/env python3
"""
Пример интеграции ML-системы анализа продаж с реальными данными для Бали
"""

import json
import pandas as pd
from datetime import datetime, timedelta
from bali_data_collector import BaliDataCollector
from bali_config import BALI_REGIONS, OPTIMIZATION_RULES

class BaliSalesAnalyzer:
    """Главный класс для анализа продаж на Бали"""
    
    def __init__(self):
        self.data_collector = BaliDataCollector()
        self.restaurants_data = {}
        
    def analyze_sales_drop(self, restaurant_name: str, location: str, date: str, 
                          actual_sales: float, previous_sales: float) -> dict:
        """
        Анализ причин изменения продаж
        
        Args:
            restaurant_name: Название ресторана
            location: Локация (seminyak, ubud, canggu, etc.)
            date: Дата анализа (YYYY-MM-DD)
            actual_sales: Фактические продажи
            previous_sales: Продажи предыдущего дня
            
        Returns:
            Детальный анализ причин изменения
        """
        
        print(f"🔍 Анализ продаж {restaurant_name} в {location} на {date}")
        print(f"💰 Продажи: {actual_sales:,.0f} (было: {previous_sales:,.0f})")
        
        # Расчет изменения
        change_percent = ((actual_sales - previous_sales) / previous_sales) * 100
        change_abs = actual_sales - previous_sales
        
        print(f"📈 Изменение: {change_percent:+.1f}% ({change_abs:+,.0f})")
        
        # Собираем внешние данные
        external_data = self.data_collector.collect_all_data(location, date)
        
        # Анализируем факторы
        factors = self._analyze_factors(external_data, change_percent)
        
        # Генерируем рекомендации
        recommendations = self._generate_recommendations(factors, external_data)
        
        # Формируем результат
        result = {
            'restaurant': restaurant_name,
            'location': location,
            'date': date,
            'actual_sales': actual_sales,
            'previous_sales': previous_sales,
            'change_percent': round(change_percent, 1),
            'change_absolute': round(change_abs, 0),
            'factors': factors,
            'external_data': external_data,
            'recommendations': recommendations,
            'timestamp': datetime.now().isoformat()
        }
        
        # Выводим результат
        self._print_analysis_result(result)
        
        return result
    
    def _analyze_factors(self, external_data: dict, change_percent: float) -> dict:
        """Анализ факторов влияния"""
        factors = {
            'weather_impact': 0,
            'holiday_impact': 0,
            'driver_impact': 0,
            'tourist_impact': 0,
            'total_external_impact': 0
        }
        
        # Влияние погоды
        if external_data['rain_mm'] > 20:
            factors['weather_impact'] = -15  # Сильный дождь -15%
        elif external_data['rain_mm'] > 10:
            factors['weather_impact'] = -8   # Умеренный дождь -8%
        elif external_data['rain_mm'] > 5:
            factors['weather_impact'] = -3   # Легкий дождь -3%
        
        if external_data['is_extreme_weather']:
            factors['weather_impact'] -= 5   # Дополнительно за экстремальную погоду
        
        # Влияние праздников
        if external_data['is_hindu_holiday']:
            factors['holiday_impact'] = -12  # Индуистские праздники сильно влияют на Бали
        elif external_data['is_muslim_holiday']:
            factors['holiday_impact'] = -8   # Мусульманские праздники влияют на водителей
        elif external_data['is_national_holiday']:
            factors['holiday_impact'] = -10  # Национальные праздники
        
        # Влияние доступности водителей
        driver_availability = external_data['driver_availability']
        if driver_availability < 0.6:
            factors['driver_impact'] = -20   # Критическая нехватка водителей
        elif driver_availability < 0.8:
            factors['driver_impact'] = -10   # Умеренная нехватка
        elif driver_availability > 1.0:
            factors['driver_impact'] = 5     # Избыток водителей
        
        # Влияние туристического сезона
        tourist_density = external_data['tourist_density']
        if tourist_density > 1.0:
            factors['tourist_impact'] = 10   # Высокий туристический сезон
        elif tourist_density < 0.6:
            factors['tourist_impact'] = -5   # Низкий сезон
        
        # Общее влияние внешних факторов
        factors['total_external_impact'] = sum([
            factors['weather_impact'],
            factors['holiday_impact'], 
            factors['driver_impact'],
            factors['tourist_impact']
        ])
        
        return factors
    
    def _generate_recommendations(self, factors: dict, external_data: dict) -> list:
        """Генерация рекомендаций на основе анализа"""
        recommendations = []
        
        # Рекомендации по погоде
        if external_data['rain_mm'] > 15:
            recommendations.extend([
                "🌧️ Увеличить комиссию за доставку в дождь",
                "🎯 Запустить промо 'Дождливый день' со скидкой на доставку",
                "💰 Повысить бонусы водителям за работу в дождь",
                "📱 Отправить push-уведомления о доставке в непогоду"
            ])
        
        # Рекомендации по праздникам
        if external_data['is_muslim_holiday']:
            recommendations.extend([
                "🕌 Убрать свинину из меню на время праздника",
                "🥘 Увеличить количество халяльных блюд",
                "⏰ Скорректировать время работы под молитвы",
                "👥 Привлечь дополнительных водителей-немусульман"
            ])
        
        if external_data['is_hindu_holiday']:
            recommendations.extend([
                "🛕 Предложить специальное праздничное меню",
                "🎉 Подготовить промо к местному празднику",
                "📅 Скорректировать график работы",
                "🎯 Сфокусироваться на туристах в этот день"
            ])
        
        # Рекомендации по водителям
        if external_data['driver_availability'] < 0.7:
            recommendations.extend([
                "🚗 Увеличить бонусы водителям на 20-30%",
                "⚡ Подключить дополнительные сервисы доставки",
                "📞 Связаться с партнерами для привлечения водителей",
                "⏱️ Увеличить время доставки в приложении"
            ])
        
        # Рекомендации по туристическому сезону
        if external_data['tourist_density'] > 1.0:
            recommendations.extend([
                "🌍 Добавить английские описания блюд",
                "🎯 Увеличить рекламу в туристических зонах",
                "⏰ Продлить время работы",
                "💳 Принимать международные карты"
            ])
        elif external_data['tourist_density'] < 0.6:
            recommendations.extend([
                "🏠 Сфокусироваться на местных жителях",
                "🍛 Предложить традиционные индонезийские блюда",
                "💰 Снизить цены для привлечения местных",
                "📱 Реклама на местных платформах"
            ])
        
        return recommendations
    
    def _print_analysis_result(self, result: dict):
        """Красивый вывод результатов анализа"""
        print("\n" + "="*60)
        print("🎯 АНАЛИЗ ПРИЧИН ИЗМЕНЕНИЯ ПРОДАЖ")
        print("="*60)
        
        # Основная информация
        print(f"🏪 Ресторан: {result['restaurant']}")
        print(f"📍 Локация: {result['location']}")
        print(f"📅 Дата: {result['date']}")
        print(f"💰 Продажи: {result['actual_sales']:,.0f} (было: {result['previous_sales']:,.0f})")
        
        change = result['change_percent']
        emoji = "📈" if change > 0 else "📉"
        print(f"{emoji} Изменение: {change:+.1f}% ({result['change_absolute']:+,.0f})")
        
        # Внешние факторы
        print(f"\n🌍 ВНЕШНИЕ ФАКТОРЫ:")
        ext_data = result['external_data']
        
        print(f"🌧️ Погода: {ext_data['rain_mm']:.1f}мм дождя, {ext_data['temperature']:.1f}°C")
        print(f"🚗 Доступность водителей: {ext_data['driver_availability']:.1%}")
        print(f"🏖️ Туристический сезон: {ext_data['tourist_season']} ({ext_data['tourist_density']:.1%})")
        
        if ext_data['holiday_names']:
            print(f"🎉 Праздники: {', '.join(ext_data['holiday_names'])}")
        
        # Анализ влияния
        print(f"\n📊 ВЛИЯНИЕ ФАКТОРОВ:")
        factors = result['factors']
        
        if factors['weather_impact'] != 0:
            print(f"🌧️ Погода: {factors['weather_impact']:+.0f}%")
        if factors['holiday_impact'] != 0:
            print(f"🎉 Праздники: {factors['holiday_impact']:+.0f}%")
        if factors['driver_impact'] != 0:
            print(f"🚗 Водители: {factors['driver_impact']:+.0f}%")
        if factors['tourist_impact'] != 0:
            print(f"🏖️ Туристы: {factors['tourist_impact']:+.0f}%")
        
        total_impact = factors['total_external_impact']
        print(f"📈 Общее влияние внешних факторов: {total_impact:+.0f}%")
        
        # Рекомендации
        if result['recommendations']:
            print(f"\n💡 РЕКОМЕНДАЦИИ:")
            for i, rec in enumerate(result['recommendations'][:8], 1):
                print(f"{i:2d}. {rec}")
        
        print("="*60)
    
    def batch_analyze_week(self, restaurant_name: str, location: str, 
                          start_date: str, sales_data: dict) -> list:
        """
        Анализ продаж за неделю
        
        Args:
            restaurant_name: Название ресторана
            location: Локация
            start_date: Начальная дата (YYYY-MM-DD)
            sales_data: Словарь {дата: продажи}
            
        Returns:
            Список результатов анализа по дням
        """
        
        print(f"\n📊 АНАЛИЗ ПРОДАЖ ЗА НЕДЕЛЮ: {restaurant_name}")
        print(f"📍 Локация: {location}")
        print(f"📅 Период: {start_date} - {len(sales_data)} дней")
        print("="*60)
        
        results = []
        previous_sales = None
        
        for date, sales in sorted(sales_data.items()):
            if previous_sales is not None:
                result = self.analyze_sales_drop(
                    restaurant_name, location, date, sales, previous_sales
                )
                results.append(result)
                print()  # Разделитель между днями
            
            previous_sales = sales
        
        # Сводная статистика
        if results:
            changes = [r['change_percent'] for r in results]
            avg_change = sum(changes) / len(changes)
            
            print(f"\n📈 СВОДНАЯ СТАТИСТИКА:")
            print(f"Среднее изменение: {avg_change:.1f}%")
            print(f"Максимальный рост: {max(changes):.1f}%")
            print(f"Максимальное падение: {min(changes):.1f}%")
            
            # Самые частые рекомендации
            all_recommendations = []
            for r in results:
                all_recommendations.extend(r['recommendations'])
            
            if all_recommendations:
                from collections import Counter
                common_recs = Counter(all_recommendations).most_common(5)
                print(f"\n🔥 ЧАСТЫЕ РЕКОМЕНДАЦИИ:")
                for rec, count in common_recs:
                    print(f"{count}x {rec}")
        
        return results

def main():
    """Пример использования системы"""
    
    print("🚀 СИСТЕМА АНАЛИЗА ПРОДАЖ ДЛЯ БАЛИ")
    print("="*60)
    
    # Создаем анализатор
    analyzer = BaliSalesAnalyzer()
    
    # Пример 1: Анализ конкретного дня
    print("\n1️⃣ АНАЛИЗ КОНКРЕТНОГО ДНЯ")
    
    result = analyzer.analyze_sales_drop(
        restaurant_name="Warung Bali Asli",
        location="seminyak",
        date="2024-01-15",
        actual_sales=2500000,  # 2.5 млн рупий
        previous_sales=3200000  # 3.2 млн рупий
    )
    
    # Сохраняем результат
    with open('analysis_result.json', 'w', encoding='utf-8') as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    
    print(f"\n💾 Результат сохранен в analysis_result.json")
    
    # Пример 2: Анализ за неделю
    print("\n\n2️⃣ АНАЛИЗ ЗА НЕДЕЛЮ")
    
    # Пример данных продаж за неделю
    weekly_sales = {
        '2024-01-15': 2500000,
        '2024-01-16': 2800000,
        '2024-01-17': 2200000,
        '2024-01-18': 3100000,
        '2024-01-19': 2900000,
        '2024-01-20': 3400000,
        '2024-01-21': 2700000,
    }
    
    weekly_results = analyzer.batch_analyze_week(
        restaurant_name="Warung Bali Asli",
        location="seminyak",
        start_date="2024-01-15",
        sales_data=weekly_sales
    )
    
    # Сохраняем недельные результаты
    with open('weekly_analysis.json', 'w', encoding='utf-8') as f:
        json.dump(weekly_results, f, ensure_ascii=False, indent=2)
    
    print(f"\n💾 Недельный анализ сохранен в weekly_analysis.json")

if __name__ == "__main__":
    main()