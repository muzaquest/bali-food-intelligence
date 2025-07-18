#!/usr/bin/env python3
"""
Holiday API Integration для анализа влияния праздников на продажи в Бали
"""

import requests
import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional

class HolidayService:
    """Сервис для работы с API праздников Calendarific"""
    
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://calendarific.com/api/v2"
        
        # Важные праздники Бали и их влияние на продажи
        self.bali_holidays = {
            # Мусульманские праздники (влияют на водителей)
            'Eid al-Fitr': {'impact': -0.40, 'driver_shortage': True, 'type': 'muslim'},
            'Eid al-Adha': {'impact': -0.35, 'driver_shortage': True, 'type': 'muslim'},
            'Mawlid al-Nabi': {'impact': -0.20, 'driver_shortage': True, 'type': 'muslim'},
            'Isra and Mi\'raj': {'impact': -0.15, 'driver_shortage': True, 'type': 'muslim'},
            
            # Индуистские праздники (влияют на туристов и местных)
            'Nyepi': {'impact': -0.60, 'driver_shortage': True, 'type': 'hindu'},  # День тишины
            'Galungan': {'impact': -0.25, 'driver_shortage': False, 'type': 'hindu'},
            'Kuningan': {'impact': -0.20, 'driver_shortage': False, 'type': 'hindu'},
            'Saraswati': {'impact': -0.15, 'driver_shortage': False, 'type': 'hindu'},
            'Pagerwesi': {'impact': -0.10, 'driver_shortage': False, 'type': 'hindu'},
            
            # Национальные праздники
            'Independence Day': {'impact': -0.30, 'driver_shortage': True, 'type': 'national'},
            'Pancasila Day': {'impact': -0.25, 'driver_shortage': True, 'type': 'national'},
            'Labor Day': {'impact': -0.20, 'driver_shortage': True, 'type': 'national'},
            'New Year\'s Day': {'impact': -0.35, 'driver_shortage': True, 'type': 'national'},
            'Christmas Day': {'impact': -0.30, 'driver_shortage': True, 'type': 'national'},
            
            # Международные праздники
            'Valentine\'s Day': {'impact': 0.15, 'driver_shortage': False, 'type': 'international'},
            'Mother\'s Day': {'impact': 0.10, 'driver_shortage': False, 'type': 'international'},
        }
    
    def get_holidays(self, year: int, country: str = 'ID') -> List[Dict]:
        """Получает список праздников на год"""
        url = f"{self.base_url}/holidays"
        
        params = {
            'api_key': self.api_key,
            'country': country,
            'year': year
        }
        
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            
            holidays = []
            for holiday in data['response']['holidays']:
                holidays.append({
                    'name': holiday['name'],
                    'date': holiday['date']['iso'],
                    'description': holiday['description'],
                    'type': holiday['type'],
                    'locations': holiday.get('locations', 'All')
                })
            
            return holidays
        except requests.RequestException as e:
            print(f"Ошибка получения праздников: {e}")
            return self._get_fallback_holidays(year)
    
    def get_holiday_impact(self, date: str, region: str = None) -> Dict:
        """Анализирует влияние праздников на продажи в конкретную дату"""
        target_date = datetime.fromisoformat(date.replace('Z', '+00:00')).date()
        year = target_date.year
        
        holidays = self.get_holidays(year)
        
        # Проверяем точное совпадение даты
        current_holidays = []
        for holiday in holidays:
            holiday_date = datetime.fromisoformat(holiday['date']).date()
            if holiday_date == target_date:
                current_holidays.append(holiday)
        
        # Проверяем близкие даты (день до/после)
        nearby_holidays = []
        for holiday in holidays:
            holiday_date = datetime.fromisoformat(holiday['date']).date()
            days_diff = abs((holiday_date - target_date).days)
            if 1 <= days_diff <= 2:
                nearby_holidays.append({
                    'holiday': holiday,
                    'days_diff': days_diff
                })
        
        # Анализируем влияние
        total_impact = 0
        reasons = []
        driver_shortage = False
        holiday_types = []
        
        # Основные праздники
        for holiday in current_holidays:
            holiday_name = holiday['name']
            impact_data = self._get_holiday_impact_data(holiday_name)
            
            total_impact += impact_data['impact']
            reasons.append(f"{holiday_name} ({impact_data['impact']*100:+.0f}%)")
            
            if impact_data['driver_shortage']:
                driver_shortage = True
            
            holiday_types.append(impact_data['type'])
        
        # Близкие праздники (меньшее влияние)
        for nearby in nearby_holidays:
            holiday_name = nearby['holiday']['name']
            impact_data = self._get_holiday_impact_data(holiday_name)
            
            # Уменьшаем влияние в зависимости от расстояния
            reduced_impact = impact_data['impact'] * (0.5 if nearby['days_diff'] == 1 else 0.3)
            total_impact += reduced_impact
            
            reasons.append(f"{holiday_name} через {nearby['days_diff']} дня ({reduced_impact*100:+.0f}%)")
        
        # Особые случаи для Бали
        if self._is_ramadan_period(target_date):
            total_impact -= 0.10  # -10% во время Рамадана
            reasons.append("Период Рамадана (-10%)")
            driver_shortage = True
        
        if self._is_tourist_season(target_date):
            # Праздники в туристический сезон имеют меньшее влияние
            if total_impact < 0:
                total_impact *= 0.7  # Уменьшаем негативное влияние
                reasons.append("Туристический сезон компенсирует потери")
        
        return {
            'holiday_impact': round(total_impact, 3),
            'impact_percent': round(total_impact * 100, 1),
            'current_holidays': [h['name'] for h in current_holidays],
            'nearby_holidays': [h['holiday']['name'] for h in nearby_holidays],
            'reasons': reasons,
            'driver_shortage': driver_shortage,
            'holiday_types': list(set(holiday_types)),
            'recommendations': self._get_holiday_recommendations(total_impact, driver_shortage)
        }
    
    def get_upcoming_holidays(self, region: str = None, days: int = 30) -> List[Dict]:
        """Получает список предстоящих праздников"""
        today = datetime.now().date()
        end_date = today + timedelta(days=days)
        
        holidays = self.get_holidays(today.year)
        if today.year != end_date.year:
            holidays.extend(self.get_holidays(end_date.year))
        
        upcoming = []
        for holiday in holidays:
            holiday_date = datetime.fromisoformat(holiday['date']).date()
            if today <= holiday_date <= end_date:
                days_until = (holiday_date - today).days
                impact_data = self._get_holiday_impact_data(holiday['name'])
                
                upcoming.append({
                    'name': holiday['name'],
                    'date': holiday['date'],
                    'days_until': days_until,
                    'impact': impact_data['impact'],
                    'driver_shortage': impact_data['driver_shortage'],
                    'type': impact_data['type'],
                    'recommendations': self._get_holiday_recommendations(
                        impact_data['impact'], 
                        impact_data['driver_shortage']
                    )
                })
        
        return sorted(upcoming, key=lambda x: x['days_until'])
    
    def get_monthly_holiday_calendar(self, year: int, month: int) -> Dict:
        """Получает календарь праздников на месяц"""
        holidays = self.get_holidays(year)
        
        monthly_holidays = []
        for holiday in holidays:
            holiday_date = datetime.fromisoformat(holiday['date']).date()
            if holiday_date.month == month:
                impact_data = self._get_holiday_impact_data(holiday['name'])
                monthly_holidays.append({
                    'name': holiday['name'],
                    'date': holiday_date.day,
                    'impact': impact_data['impact'],
                    'driver_shortage': impact_data['driver_shortage'],
                    'type': impact_data['type']
                })
        
        # Анализ месяца
        total_impact = sum(h['impact'] for h in monthly_holidays)
        critical_days = [h for h in monthly_holidays if h['impact'] < -0.2]
        
        return {
            'month': month,
            'year': year,
            'holidays': monthly_holidays,
            'total_impact': round(total_impact, 3),
            'critical_days': len(critical_days),
            'recommendations': self._get_monthly_recommendations(monthly_holidays)
        }
    
    def _get_holiday_impact_data(self, holiday_name: str) -> Dict:
        """Получает данные о влиянии праздника"""
        # Точное совпадение
        if holiday_name in self.bali_holidays:
            return self.bali_holidays[holiday_name]
        
        # Поиск по частичному совпадению
        for name, data in self.bali_holidays.items():
            if name.lower() in holiday_name.lower() or holiday_name.lower() in name.lower():
                return data
        
        # Значения по умолчанию
        return {
            'impact': -0.10,
            'driver_shortage': False,
            'type': 'other'
        }
    
    def _is_ramadan_period(self, date: datetime.date) -> bool:
        """Проверяет, попадает ли дата в период Рамадана"""
        # Примерные даты Рамадана (нужно обновлять каждый год)
        ramadan_periods = {
            2024: (datetime(2024, 3, 10).date(), datetime(2024, 4, 9).date()),
            2025: (datetime(2025, 2, 28).date(), datetime(2025, 3, 29).date()),
        }
        
        if date.year in ramadan_periods:
            start, end = ramadan_periods[date.year]
            return start <= date <= end
        
        return False
    
    def _is_tourist_season(self, date: datetime.date) -> bool:
        """Проверяет, является ли дата туристическим сезоном"""
        month = date.month
        return month in [6, 7, 8, 12, 1]  # Июнь-август, декабрь-январь
    
    def _get_holiday_recommendations(self, impact: float, driver_shortage: bool) -> List[str]:
        """Генерирует рекомендации на основе влияния праздника"""
        recommendations = []
        
        if impact < -0.3:
            recommendations.append("Критическое снижение продаж - подготовьте специальные предложения")
            recommendations.append("Рассмотрите временное закрытие или сокращение часов работы")
        elif impact < -0.2:
            recommendations.append("Значительное снижение продаж - снизьте запасы на 20-30%")
            recommendations.append("Запланируйте промо-акции для компенсации потерь")
        elif impact < -0.1:
            recommendations.append("Умеренное снижение продаж - снизьте запасы на 10-15%")
        elif impact > 0.1:
            recommendations.append("Увеличение продаж - подготовьте дополнительные запасы")
            recommendations.append("Рассмотрите специальные предложения для праздника")
        
        if driver_shortage:
            recommendations.append("Ожидается нехватка водителей - договоритесь заранее")
            recommendations.append("Увеличьте время доставки в системе")
            recommendations.append("Подготовьте альтернативные способы доставки")
        
        return recommendations
    
    def _get_monthly_recommendations(self, holidays: List[Dict]) -> List[str]:
        """Генерирует рекомендации на месяц"""
        recommendations = []
        
        critical_days = len([h for h in holidays if h['impact'] < -0.2])
        if critical_days > 2:
            recommendations.append("Месяц с высокой праздничной активностью - планируйте запасы заранее")
        
        driver_shortage_days = len([h for h in holidays if h['driver_shortage']])
        if driver_shortage_days > 1:
            recommendations.append("Множественная нехватка водителей - заключите договоры с резервными службами")
        
        return recommendations
    
    def _get_fallback_holidays(self, year: int) -> List[Dict]:
        """Возвращает основные праздники при ошибке API"""
        return [
            {'name': 'New Year\'s Day', 'date': f'{year}-01-01', 'description': 'Новый год', 'type': 'national'},
            {'name': 'Independence Day', 'date': f'{year}-08-17', 'description': 'День независимости', 'type': 'national'},
            {'name': 'Christmas Day', 'date': f'{year}-12-25', 'description': 'Рождество', 'type': 'national'},
        ]

# Пример использования
def main():
    # Замените на ваш API ключ от Calendarific
    api_key = "YOUR_CALENDARIFIC_API_KEY"
    
    holiday_service = HolidayService(api_key)
    
    # Анализ влияния праздников на конкретную дату
    impact = holiday_service.get_holiday_impact("2024-08-17")  # День независимости
    print(f"Влияние праздников: {impact['impact_percent']}%")
    print(f"Причины: {', '.join(impact['reasons'])}")
    
    # Предстоящие праздники
    upcoming = holiday_service.get_upcoming_holidays(days=30)
    print(f"\nПредстоящие праздники ({len(upcoming)}):")
    for holiday in upcoming[:5]:
        print(f"- {holiday['name']}: {holiday['impact']*100:+.0f}% через {holiday['days_until']} дней")

if __name__ == "__main__":
    main()