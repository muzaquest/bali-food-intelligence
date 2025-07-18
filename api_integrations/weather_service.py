#!/usr/bin/env python3
"""
Weather API Integration для анализа влияния погоды на продажи в Бали
"""

import requests
import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional

class WeatherService:
    """Сервис для работы с API погоды OpenWeatherMap"""
    
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://api.openweathermap.org/data/2.5"
        
        # Координаты регионов Бали
        self.bali_regions = {
            'Seminyak': {'lat': -8.6906, 'lon': 115.1737},
            'Ubud': {'lat': -8.5069, 'lon': 115.2625},
            'Canggu': {'lat': -8.6482, 'lon': 115.1342},
            'Denpasar': {'lat': -8.6705, 'lon': 115.2126},
            'Sanur': {'lat': -8.6881, 'lon': 115.2608},
            'Nusa Dua': {'lat': -8.8017, 'lon': 115.2289},
            'Jimbaran': {'lat': -8.7983, 'lon': 115.1614},
            'Kuta': {'lat': -8.7205, 'lon': 115.1693}
        }
    
    def get_current_weather(self, region: str) -> Dict:
        """Получает текущую погоду для региона Бали"""
        if region not in self.bali_regions:
            raise ValueError(f"Неизвестный регион: {region}")
        
        coords = self.bali_regions[region]
        url = f"{self.base_url}/weather"
        
        params = {
            'lat': coords['lat'],
            'lon': coords['lon'],
            'appid': self.api_key,
            'units': 'metric',
            'lang': 'ru'
        }
        
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            
            return {
                'temperature': data['main']['temp'],
                'humidity': data['main']['humidity'],
                'pressure': data['main']['pressure'],
                'precipitation': data.get('rain', {}).get('1h', 0),
                'wind_speed': data['wind']['speed'],
                'condition': data['weather'][0]['main'].lower(),
                'description': data['weather'][0]['description'],
                'timestamp': datetime.now().isoformat()
            }
        except requests.RequestException as e:
            print(f"Ошибка получения погоды: {e}")
            return self._get_fallback_weather()
    
    def get_weather_forecast(self, region: str, days: int = 7) -> List[Dict]:
        """Получает прогноз погоды на несколько дней"""
        if region not in self.bali_regions:
            raise ValueError(f"Неизвестный регион: {region}")
        
        coords = self.bali_regions[region]
        url = f"{self.base_url}/forecast"
        
        params = {
            'lat': coords['lat'],
            'lon': coords['lon'],
            'appid': self.api_key,
            'units': 'metric',
            'lang': 'ru'
        }
        
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            
            forecast = []
            for item in data['list'][:days * 8]:  # 8 записей в день (каждые 3 часа)
                forecast.append({
                    'date': item['dt_txt'],
                    'temperature': item['main']['temp'],
                    'humidity': item['main']['humidity'],
                    'precipitation': item.get('rain', {}).get('3h', 0),
                    'wind_speed': item['wind']['speed'],
                    'condition': item['weather'][0]['main'].lower(),
                    'description': item['weather'][0]['description']
                })
            
            return forecast
        except requests.RequestException as e:
            print(f"Ошибка получения прогноза: {e}")
            return []
    
    def get_weather_impact(self, region: str, date: str = None) -> Dict:
        """Анализирует влияние погоды на продажи"""
        weather = self.get_current_weather(region)
        
        impact = 0
        reasons = []
        
        # Анализ осадков
        precipitation = weather['precipitation']
        if precipitation > 15:  # Очень сильный дождь
            impact -= 0.30  # -30% продаж
            reasons.append(f"Очень сильный дождь ({precipitation:.1f}мм)")
        elif precipitation > 10:  # Сильный дождь
            impact -= 0.25  # -25% продаж
            reasons.append(f"Сильный дождь ({precipitation:.1f}мм)")
        elif precipitation > 5:  # Умеренный дождь
            impact -= 0.15  # -15% продаж
            reasons.append(f"Умеренный дождь ({precipitation:.1f}мм)")
        elif precipitation > 1:  # Легкий дождь
            impact -= 0.08  # -8% продаж
            reasons.append(f"Легкий дождь ({precipitation:.1f}мм)")
        
        # Анализ ветра
        wind_speed = weather['wind_speed']
        if wind_speed > 15:  # Сильный ветер
            impact -= 0.10  # -10% продаж
            reasons.append(f"Сильный ветер ({wind_speed:.1f}м/с)")
        elif wind_speed > 10:  # Умеренный ветер
            impact -= 0.05  # -5% продаж
            reasons.append(f"Умеренный ветер ({wind_speed:.1f}м/с)")
        
        # Анализ температуры
        temperature = weather['temperature']
        if temperature > 35:  # Очень жарко
            impact -= 0.05  # -5% продаж
            reasons.append(f"Очень жарко ({temperature:.1f}°C)")
        elif temperature < 20:  # Прохладно для Бали
            impact -= 0.03  # -3% продаж
            reasons.append(f"Прохладно ({temperature:.1f}°C)")
        
        # Хорошая погода
        if impact == 0:
            impact = 0.05  # +5% продаж
            reasons.append("Хорошая погода для доставки")
        
        return {
            'weather_impact': round(impact, 3),
            'impact_percent': round(impact * 100, 1),
            'reasons': reasons,
            'weather_summary': self._get_weather_summary(weather),
            'delivery_conditions': self._get_delivery_conditions(weather),
            'raw_weather': weather
        }
    
    def get_weekly_weather_impact(self, region: str) -> Dict:
        """Анализирует влияние погоды на неделю вперед"""
        forecast = self.get_weather_forecast(region, 7)
        
        daily_impacts = []
        for i in range(0, len(forecast), 8):  # Каждый день
            day_data = forecast[i:i+8]
            if not day_data:
                continue
            
            # Средние значения за день
            avg_precipitation = sum(item['precipitation'] for item in day_data) / len(day_data)
            avg_wind = sum(item['wind_speed'] for item in day_data) / len(day_data)
            avg_temp = sum(item['temperature'] for item in day_data) / len(day_data)
            
            # Создаем объект погоды для анализа
            day_weather = {
                'precipitation': avg_precipitation,
                'wind_speed': avg_wind,
                'temperature': avg_temp,
                'condition': day_data[0]['condition']
            }
            
            impact = self._calculate_weather_impact(day_weather)
            daily_impacts.append({
                'date': day_data[0]['date'][:10],
                'impact': impact,
                'weather': day_weather
            })
        
        return {
            'weekly_forecast': daily_impacts,
            'avg_impact': sum(day['impact'] for day in daily_impacts) / len(daily_impacts),
            'best_day': max(daily_impacts, key=lambda x: x['impact']),
            'worst_day': min(daily_impacts, key=lambda x: x['impact'])
        }
    
    def _calculate_weather_impact(self, weather: Dict) -> float:
        """Вычисляет влияние погоды на продажи"""
        impact = 0
        
        precipitation = weather['precipitation']
        if precipitation > 15:
            impact -= 0.30
        elif precipitation > 10:
            impact -= 0.25
        elif precipitation > 5:
            impact -= 0.15
        elif precipitation > 1:
            impact -= 0.08
        
        wind_speed = weather['wind_speed']
        if wind_speed > 15:
            impact -= 0.10
        elif wind_speed > 10:
            impact -= 0.05
        
        temperature = weather['temperature']
        if temperature > 35:
            impact -= 0.05
        elif temperature < 20:
            impact -= 0.03
        
        if impact == 0:
            impact = 0.05
        
        return round(impact, 3)
    
    def _get_weather_summary(self, weather: Dict) -> str:
        """Создает краткое описание погоды"""
        temp = weather['temperature']
        precip = weather['precipitation']
        wind = weather['wind_speed']
        
        if precip > 10:
            return f"Дождливо, {temp:.1f}°C, ветер {wind:.1f}м/с"
        elif precip > 1:
            return f"Небольшой дождь, {temp:.1f}°C, ветер {wind:.1f}м/с"
        else:
            return f"Ясно, {temp:.1f}°C, ветер {wind:.1f}м/с"
    
    def _get_delivery_conditions(self, weather: Dict) -> str:
        """Оценивает условия для доставки"""
        precip = weather['precipitation']
        wind = weather['wind_speed']
        
        if precip > 15 or wind > 15:
            return "Плохие условия для доставки"
        elif precip > 5 or wind > 10:
            return "Сложные условия для доставки"
        elif precip > 1:
            return "Удовлетворительные условия"
        else:
            return "Отличные условия для доставки"
    
    def _get_fallback_weather(self) -> Dict:
        """Возвращает погоду по умолчанию при ошибке API"""
        return {
            'temperature': 28.0,
            'humidity': 80,
            'pressure': 1013,
            'precipitation': 0,
            'wind_speed': 5,
            'condition': 'clear',
            'description': 'ясно',
            'timestamp': datetime.now().isoformat()
        }

# Пример использования
def main():
    # Замените на ваш API ключ от OpenWeatherMap
    api_key = "YOUR_OPENWEATHERMAP_API_KEY"
    
    weather_service = WeatherService(api_key)
    
    # Анализ погоды для Seminyak
    impact = weather_service.get_weather_impact("Seminyak")
    print(f"Влияние погоды на продажи: {impact['impact_percent']}%")
    print(f"Причины: {', '.join(impact['reasons'])}")
    
    # Прогноз на неделю
    weekly = weather_service.get_weekly_weather_impact("Seminyak")
    print(f"Средний недельный прогноз: {weekly['avg_impact']*100:.1f}%")

if __name__ == "__main__":
    main()