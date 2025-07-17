#!/usr/bin/env python3
"""
Демонстрация системы анализа продаж для Бали
Простая версия без внешних зависимостей
"""

import json
import random
from datetime import datetime, timedelta

class BaliSalesAnalyzerDemo:
    """Демонстрация анализатора продаж для Бали"""
    
    def __init__(self):
        # Настройки для Бали
        self.bali_regions = {
            'seminyak': {'tourist_ratio': 0.9, 'type': 'beach_luxury'},
            'ubud': {'tourist_ratio': 0.8, 'type': 'cultural'},
            'canggu': {'tourist_ratio': 0.8, 'type': 'beach_surf'},
            'denpasar': {'tourist_ratio': 0.3, 'type': 'city'},
            'sanur': {'tourist_ratio': 0.7, 'type': 'beach_family'}
        }
        
        self.weather_impact = {
            'light_rain': -3,
            'moderate_rain': -8,
            'heavy_rain': -15,
            'extreme_rain': -25
        }
        
        self.holiday_impact = {
            'muslim_holiday': -8,
            'hindu_holiday': -12,
            'national_holiday': -10
        }
    
    def simulate_external_data(self, location: str, date: str) -> dict:
        """Симуляция внешних данных для демонстрации"""
        random.seed(hash(date + location))  # Детерминированность для одинаковых входных данных
        
        # Парсим дату
        date_obj = datetime.strptime(date, '%Y-%m-%d')
        month = date_obj.month
        day_of_week = date_obj.weekday()
        
        # Симулируем погоду
        if month in [12, 1, 2, 3]:  # Сезон дождей
            rain_mm = random.uniform(5, 30)
        else:
            rain_mm = random.uniform(0, 15)
        
        # Определяем категорию дождя
        if rain_mm > 25:
            rain_category = 'extreme_rain'
        elif rain_mm > 15:
            rain_category = 'heavy_rain'
        elif rain_mm > 8:
            rain_category = 'moderate_rain'
        else:
            rain_category = 'light_rain'
        
        # Симулируем праздники
        is_friday = day_of_week == 4
        is_special_date = date_obj.day in [1, 15, 17, 25]
        
        is_muslim_holiday = is_friday or random.random() < 0.05
        is_hindu_holiday = is_special_date or random.random() < 0.03
        is_national_holiday = random.random() < 0.02
        
        # Доступность водителей
        driver_availability = 1.0
        
        if is_muslim_holiday:
            driver_availability *= 0.7
        if is_hindu_holiday:
            driver_availability *= 0.6
        if rain_mm > 15:
            driver_availability *= 0.5
        
        # Туристический сезон
        region_data = self.bali_regions.get(location, {'tourist_ratio': 0.5})
        
        if month in [6, 7, 8, 12, 1]:  # Высокий сезон
            tourist_multiplier = 1.3
            season_type = 'high'
        elif month in [4, 5, 9, 10]:  # Средний сезон
            tourist_multiplier = 1.0
            season_type = 'shoulder'
        else:  # Низкий сезон
            tourist_multiplier = 0.7
            season_type = 'low'
        
        tourist_density = region_data['tourist_ratio'] * tourist_multiplier
        
        return {
            'date': date,
            'location': location,
            'temperature': round(28 + random.uniform(-3, 3), 1),
            'humidity': round(75 + random.uniform(-10, 15), 1),
            'rain_mm': round(rain_mm, 1),
            'rain_category': rain_category,
            'is_muslim_holiday': is_muslim_holiday,
            'is_hindu_holiday': is_hindu_holiday,
            'is_national_holiday': is_national_holiday,
            'driver_availability': round(driver_availability, 2),
            'tourist_density': round(tourist_density, 2),
            'tourist_season': season_type,
            'region_type': region_data.get('type', 'unknown'),
            'day_of_week': day_of_week,
            'is_weekend': day_of_week >= 5
        }
    
    def analyze_sales_change(self, restaurant_name: str, location: str, date: str,
                           actual_sales: float, previous_sales: float) -> dict:
        """Анализ изменения продаж"""
        
        # Расчет изменения
        change_percent = ((actual_sales - previous_sales) / previous_sales) * 100
        change_absolute = actual_sales - previous_sales
        
        # Получаем внешние данные
        external_data = self.simulate_external_data(location, date)
        
        # Анализ факторов
        factors = self._analyze_factors(external_data)
        
        # Генерация рекомендаций
        recommendations = self._generate_recommendations(external_data, factors)
        
        # Объяснение изменений
        explanation = self._generate_explanation(factors, change_percent)
        
        result = {
            'restaurant': restaurant_name,
            'location': location,
            'date': date,
            'actual_sales': int(actual_sales),
            'previous_sales': int(previous_sales),
            'change_percent': round(change_percent, 1),
            'change_absolute': int(change_absolute),
            'external_data': external_data,
            'factors': factors,
            'recommendations': recommendations,
            'explanation': explanation,
            'timestamp': datetime.now().isoformat()
        }
        
        return result
    
    def _analyze_factors(self, external_data: dict) -> dict:
        """Анализ факторов влияния"""
        factors = {
            'weather_impact': 0,
            'holiday_impact': 0,
            'driver_impact': 0,
            'tourist_impact': 0,
            'total_external_impact': 0
        }
        
        # Влияние погоды
        rain_category = external_data['rain_category']
        factors['weather_impact'] = self.weather_impact.get(rain_category, 0)
        
        # Влияние праздников
        if external_data['is_hindu_holiday']:
            factors['holiday_impact'] = self.holiday_impact['hindu_holiday']
        elif external_data['is_muslim_holiday']:
            factors['holiday_impact'] = self.holiday_impact['muslim_holiday']
        elif external_data['is_national_holiday']:
            factors['holiday_impact'] = self.holiday_impact['national_holiday']
        
        # Влияние доступности водителей
        driver_availability = external_data['driver_availability']
        if driver_availability < 0.6:
            factors['driver_impact'] = -20
        elif driver_availability < 0.8:
            factors['driver_impact'] = -10
        elif driver_availability > 1.0:
            factors['driver_impact'] = 5
        
        # Влияние туристического сезона
        tourist_density = external_data['tourist_density']
        if tourist_density > 1.0:
            factors['tourist_impact'] = 10
        elif tourist_density < 0.6:
            factors['tourist_impact'] = -5
        
        # Общее влияние
        factors['total_external_impact'] = sum([
            factors['weather_impact'],
            factors['holiday_impact'],
            factors['driver_impact'],
            factors['tourist_impact']
        ])
        
        return factors
    
    def _generate_recommendations(self, external_data: dict, factors: dict) -> list:
        """Генерация рекомендаций"""
        recommendations = []
        
        # Рекомендации по погоде
        if external_data['rain_mm'] > 15:
            recommendations.extend([
                "🌧️ Увеличить комиссию за доставку в дождь на 15-20%",
                "🎯 Запустить промо 'Дождливый день' с бесплатной доставкой",
                "💰 Повысить бонусы водителям за работу в дождь",
                "📱 Отправить push-уведомления клиентам о возможных задержках"
            ])
        
        # Рекомендации по праздникам
        if external_data['is_muslim_holiday']:
            recommendations.extend([
                "🕌 Временно убрать свинину из меню",
                "🥘 Увеличить количество халяльных блюд",
                "⏰ Скорректировать время работы под молитвы",
                "👥 Привлечь дополнительных водителей-немусульман"
            ])
        
        if external_data['is_hindu_holiday']:
            recommendations.extend([
                "🛕 Предложить специальное праздничное меню",
                "🎉 Подготовить промо к индуистскому празднику",
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
                "⏰ Продлить время работы до 24:00",
                "💳 Принимать международные карты"
            ])
        elif external_data['tourist_density'] < 0.6:
            recommendations.extend([
                "🏠 Сфокусироваться на местных жителях",
                "🍛 Предложить традиционные индонезийские блюда",
                "💰 Снизить цены для привлечения местных",
                "📱 Реклама на местных платформах (Instagram, TikTok)"
            ])
        
        return recommendations[:8]  # Ограничиваем до 8 рекомендаций
    
    def _generate_explanation(self, factors: dict, change_percent: float) -> str:
        """Генерация объяснения изменений"""
        explanations = []
        
        if factors['weather_impact'] < -5:
            explanations.append(f"дождь ({factors['weather_impact']:+.0f}%)")
        elif factors['weather_impact'] < -2:
            explanations.append(f"легкий дождь ({factors['weather_impact']:+.0f}%)")
        
        if factors['holiday_impact'] < -5:
            explanations.append(f"праздник ({factors['holiday_impact']:+.0f}%)")
        
        if factors['driver_impact'] < -5:
            explanations.append(f"нехватка водителей ({factors['driver_impact']:+.0f}%)")
        elif factors['driver_impact'] > 2:
            explanations.append(f"больше водителей ({factors['driver_impact']:+.0f}%)")
        
        if factors['tourist_impact'] > 5:
            explanations.append(f"туристический сезон ({factors['tourist_impact']:+.0f}%)")
        elif factors['tourist_impact'] < -3:
            explanations.append(f"низкий сезон ({factors['tourist_impact']:+.0f}%)")
        
        if explanations:
            return f"Основные факторы изменения: {', '.join(explanations)}"
        else:
            return "Изменения в основном связаны с внутренними факторами ресторана"
    
    def print_analysis(self, result: dict):
        """Красивый вывод результатов анализа"""
        print("\n" + "="*70)
        print("🎯 АНАЛИЗ ПРИЧИН ИЗМЕНЕНИЯ ПРОДАЖ НА БАЛИ")
        print("="*70)
        
        # Основная информация
        print(f"🏪 Ресторан: {result['restaurant']}")
        print(f"📍 Локация: {result['location']}")
        print(f"📅 Дата: {result['date']}")
        print(f"💰 Продажи: {result['actual_sales']:,} IDR (было: {result['previous_sales']:,} IDR)")
        
        change = result['change_percent']
        emoji = "📈" if change > 0 else "📉"
        print(f"{emoji} Изменение: {change:+.1f}% ({result['change_absolute']:+,} IDR)")
        
        # Внешние факторы
        print(f"\n🌍 ВНЕШНИЕ ФАКТОРЫ:")
        ext_data = result['external_data']
        
        print(f"🌧️ Погода: {ext_data['rain_mm']}мм дождя, {ext_data['temperature']}°C, влажность {ext_data['humidity']}%")
        print(f"🚗 Доступность водителей: {ext_data['driver_availability']:.0%}")
        print(f"🏖️ Туристический сезон: {ext_data['tourist_season']} ({ext_data['tourist_density']:.0%} плотность)")
        print(f"🏛️ Тип района: {ext_data['region_type']}")
        
        # Праздники
        holidays = []
        if ext_data['is_muslim_holiday']:
            holidays.append("мусульманский")
        if ext_data['is_hindu_holiday']:
            holidays.append("индуистский")
        if ext_data['is_national_holiday']:
            holidays.append("национальный")
        
        if holidays:
            print(f"🎉 Праздники: {', '.join(holidays)}")
        
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
        
        # Объяснение
        print(f"\n💡 ОБЪЯСНЕНИЕ:")
        print(f"   {result['explanation']}")
        
        # Рекомендации
        if result['recommendations']:
            print(f"\n🎯 РЕКОМЕНДАЦИИ:")
            for i, rec in enumerate(result['recommendations'], 1):
                print(f"{i:2d}. {rec}")
        
        print("="*70)

def main():
    """Главная функция демонстрации"""
    
    print("🚀 СИСТЕМА АНАЛИЗА ПРОДАЖ ДЛЯ БАЛИ")
    print("Специально адаптированная для особенностей Индонезии")
    print("="*70)
    
    # Создаем анализатор
    analyzer = BaliSalesAnalyzerDemo()
    
    # Тестовые сценарии
    test_cases = [
        {
            'name': 'Warung Bali Asli',
            'location': 'seminyak',
            'date': '2024-01-15',  # Сезон дождей
            'actual_sales': 2500000,
            'previous_sales': 3200000,
            'scenario': 'Падение продаж в дождливый день'
        },
        {
            'name': 'Ubud Organic Cafe',
            'location': 'ubud',
            'date': '2024-07-20',  # Высокий туристический сезон
            'actual_sales': 4200000,
            'previous_sales': 3800000,
            'scenario': 'Рост продаж в туристический сезон'
        },
        {
            'name': 'Canggu Surf Cafe',
            'location': 'canggu',
            'date': '2024-03-17',  # Индуистский праздник
            'actual_sales': 1800000,
            'previous_sales': 2600000,
            'scenario': 'Падение продаж в праздник'
        }
    ]
    
    results = []
    
    for i, case in enumerate(test_cases, 1):
        print(f"\n{i}️⃣ СЦЕНАРИЙ: {case['scenario']}")
        print("-" * 50)
        
        result = analyzer.analyze_sales_change(
            restaurant_name=case['name'],
            location=case['location'],
            date=case['date'],
            actual_sales=case['actual_sales'],
            previous_sales=case['previous_sales']
        )
        
        analyzer.print_analysis(result)
        results.append(result)
    
    # Сохраняем результаты
    with open('bali_analysis_results.json', 'w', encoding='utf-8') as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    
    print(f"\n💾 Результаты сохранены в bali_analysis_results.json")
    
    # Сводная статистика
    print(f"\n📊 СВОДНАЯ СТАТИСТИКА:")
    changes = [r['change_percent'] for r in results]
    print(f"Среднее изменение: {sum(changes)/len(changes):.1f}%")
    print(f"Диапазон изменений: от {min(changes):.1f}% до {max(changes):.1f}%")
    
    # Самые частые рекомендации
    all_recommendations = []
    for r in results:
        all_recommendations.extend(r['recommendations'])
    
    from collections import Counter
    common_recs = Counter(all_recommendations).most_common(5)
    
    print(f"\n🔥 ЧАСТЫЕ РЕКОМЕНДАЦИИ:")
    for rec, count in common_recs:
        print(f"{count}x {rec}")
    
    print(f"\n✅ ДЕМОНСТРАЦИЯ ЗАВЕРШЕНА")
    print("Система готова к интеграции с вашими данными!")

if __name__ == "__main__":
    main()