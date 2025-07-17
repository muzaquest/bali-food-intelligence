"""
Модуль для сбора данных из внешних API для анализа продаж на Бали
"""

import requests
import pandas as pd
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import time

from bali_config import API_CONFIG, BALI_HOLIDAYS, WEATHER_FACTORS, DRIVER_FACTORS, BALI_REGIONS

logger = logging.getLogger(__name__)

class BaliDataCollector:
    """Класс для сбора данных из различных API для Бали"""
    
    def __init__(self):
        self.weather_api_key = API_CONFIG['weather']['openweather_api_key']
        self.holidays_api_key = API_CONFIG['holidays']['calendarific_api_key']
        self.grab_api_key = API_CONFIG['grab']['api_key']
        self.gojek_api_key = API_CONFIG['gojek']['api_key']
        
    def get_weather_data(self, location: str, date: str) -> Dict:
        """Получение погодных данных для конкретной локации и даты"""
        try:
            # Находим координаты локации
            location_data = next(
                (loc for loc in API_CONFIG['weather']['locations'] 
                 if loc['name'].lower() == location.lower()), 
                None
            )
            
            if not location_data:
                logger.warning(f"Локация {location} не найдена")
                return self._get_default_weather()
            
            # Если API ключ не настроен, возвращаем тестовые данные
            if not self.weather_api_key:
                return self._get_simulated_weather(location, date)
            
            # Запрос к OpenWeatherMap API
            url = f"https://api.openweathermap.org/data/2.5/weather"
            params = {
                'lat': location_data['lat'],
                'lon': location_data['lon'],
                'appid': self.weather_api_key,
                'units': 'metric'
            }
            
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            
            # Обработка данных
            weather_data = {
                'location': location,
                'date': date,
                'temperature': data['main']['temp'],
                'humidity': data['main']['humidity'],
                'pressure': data['main']['pressure'],
                'rain_mm': data.get('rain', {}).get('1h', 0),
                'wind_speed': data['wind']['speed'],
                'weather_main': data['weather'][0]['main'],
                'weather_description': data['weather'][0]['description'],
                'clouds': data['clouds']['all'],
            }
            
            # Добавляем вычисляемые поля
            weather_data.update(self._calculate_weather_features(weather_data))
            
            logger.info(f"Получены погодные данные для {location}")
            return weather_data
            
        except Exception as e:
            logger.error(f"Ошибка получения погодных данных: {e}")
            return self._get_default_weather()
    
    def _get_simulated_weather(self, location: str, date: str) -> Dict:
        """Симуляция погодных данных для тестирования"""
        import random
        
        # Базовые значения для Бали
        base_temp = 28
        base_humidity = 75
        
        # Сезонные корректировки
        date_obj = datetime.strptime(date, '%Y-%m-%d')
        month = date_obj.month
        
        # Сезон дождей
        if month in WEATHER_FACTORS['monsoon_months']:
            rain_mm = random.uniform(5, 30)
            humidity = random.uniform(80, 95)
            temp = base_temp + random.uniform(-2, 1)
        else:
            rain_mm = random.uniform(0, 10)
            humidity = random.uniform(65, 85)
            temp = base_temp + random.uniform(-1, 3)
        
        weather_data = {
            'location': location,
            'date': date,
            'temperature': round(temp, 1),
            'humidity': round(humidity, 1),
            'pressure': random.uniform(1010, 1020),
            'rain_mm': round(rain_mm, 1),
            'wind_speed': random.uniform(5, 15),
            'weather_main': 'Rain' if rain_mm > 10 else 'Clouds',
            'weather_description': 'heavy rain' if rain_mm > 20 else 'light rain' if rain_mm > 5 else 'partly cloudy',
            'clouds': random.randint(20, 90),
        }
        
        weather_data.update(self._calculate_weather_features(weather_data))
        return weather_data
    
    def _calculate_weather_features(self, weather_data: Dict) -> Dict:
        """Вычисление дополнительных погодных признаков"""
        features = {}
        
        # Категории дождя
        rain_mm = weather_data['rain_mm']
        if rain_mm > WEATHER_FACTORS['extreme_rain_threshold']:
            features['rain_category'] = 'extreme'
        elif rain_mm > WEATHER_FACTORS['heavy_rain_threshold']:
            features['rain_category'] = 'heavy'
        elif rain_mm > 5:
            features['rain_category'] = 'moderate'
        else:
            features['rain_category'] = 'light'
        
        # Комфортная температура
        temp = weather_data['temperature']
        temp_range = WEATHER_FACTORS['comfortable_temp_range']
        features['is_comfortable_temp'] = temp_range[0] <= temp <= temp_range[1]
        
        # Высокая влажность
        features['is_high_humidity'] = weather_data['humidity'] > WEATHER_FACTORS['high_humidity_threshold']
        
        # Экстремальные условия
        features['is_extreme_weather'] = (
            rain_mm > WEATHER_FACTORS['extreme_rain_threshold'] or
            temp > WEATHER_FACTORS['hot_temperature_threshold'] or
            features['is_high_humidity']
        )
        
        # Влияние на доставку
        features['delivery_impact'] = self._calculate_delivery_impact(weather_data)
        
        return features
    
    def _calculate_delivery_impact(self, weather_data: Dict) -> float:
        """Расчет влияния погоды на доставку (0-1, где 1 - максимальное негативное влияние)"""
        impact = 0.0
        
        # Влияние дождя
        rain_mm = weather_data['rain_mm']
        if rain_mm > 30:
            impact += 0.8
        elif rain_mm > 15:
            impact += 0.5
        elif rain_mm > 5:
            impact += 0.2
        
        # Влияние температуры
        temp = weather_data['temperature']
        if temp > 35:
            impact += 0.3
        elif temp < 20:
            impact += 0.2
        
        # Влияние влажности
        if weather_data['humidity'] > 90:
            impact += 0.2
        
        # Влияние ветра
        if weather_data['wind_speed'] > 20:
            impact += 0.3
        
        return min(impact, 1.0)
    
    def get_holidays_data(self, date: str) -> Dict:
        """Получение данных о праздниках для конкретной даты"""
        try:
            date_obj = datetime.strptime(date, '%Y-%m-%d')
            
            # Если API ключ не настроен, используем локальные данные
            if not self.holidays_api_key:
                return self._get_simulated_holidays(date_obj)
            
            # Запрос к Calendarific API
            url = "https://calendarific.com/api/v2/holidays"
            params = {
                'api_key': self.holidays_api_key,
                'country': 'ID',
                'year': date_obj.year,
                'month': date_obj.month,
                'day': date_obj.day
            }
            
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            holidays = data.get('response', {}).get('holidays', [])
            
            # Обработка данных
            holiday_data = {
                'date': date,
                'is_national_holiday': False,
                'is_muslim_holiday': False,
                'is_hindu_holiday': False,
                'is_local_holiday': False,
                'holiday_names': [],
                'holiday_types': [],
                'holiday_impact': 0.0
            }
            
            for holiday in holidays:
                holiday_name = holiday['name'].lower()
                holiday_type = holiday.get('type', [])
                
                holiday_data['holiday_names'].append(holiday['name'])
                holiday_data['holiday_types'].extend(holiday_type)
                
                # Определяем тип праздника
                if any(word in holiday_name for word in ['eid', 'ramadan', 'mawlid', 'isra']):
                    holiday_data['is_muslim_holiday'] = True
                elif any(word in holiday_name for word in ['nyepi', 'galungan', 'kuningan', 'saraswati']):
                    holiday_data['is_hindu_holiday'] = True
                elif 'national' in holiday_type:
                    holiday_data['is_national_holiday'] = True
                else:
                    holiday_data['is_local_holiday'] = True
            
            # Расчет влияния праздника
            holiday_data['holiday_impact'] = self._calculate_holiday_impact(holiday_data)
            
            logger.info(f"Получены данные о праздниках для {date}")
            return holiday_data
            
        except Exception as e:
            logger.error(f"Ошибка получения данных о праздниках: {e}")
            return self._get_default_holidays(date)
    
    def _get_simulated_holidays(self, date_obj: datetime) -> Dict:
        """Симуляция данных о праздниках"""
        import random
        
        # Простая симуляция на основе дат
        is_friday = date_obj.weekday() == 4
        is_special_date = date_obj.day in [1, 15, 17]  # Специальные даты
        
        holiday_data = {
            'date': date_obj.strftime('%Y-%m-%d'),
            'is_national_holiday': random.random() < 0.05,  # 5% вероятность
            'is_muslim_holiday': is_friday or random.random() < 0.03,
            'is_hindu_holiday': is_special_date or random.random() < 0.02,
            'is_local_holiday': random.random() < 0.01,
            'holiday_names': [],
            'holiday_types': [],
            'holiday_impact': 0.0
        }
        
        # Добавляем названия праздников
        if holiday_data['is_muslim_holiday']:
            holiday_data['holiday_names'].append('Friday Prayer' if is_friday else 'Muslim Holiday')
        if holiday_data['is_hindu_holiday']:
            holiday_data['holiday_names'].append('Hindu Festival')
        
        holiday_data['holiday_impact'] = self._calculate_holiday_impact(holiday_data)
        return holiday_data
    
    def _calculate_holiday_impact(self, holiday_data: Dict) -> float:
        """Расчет влияния праздника на продажи"""
        impact = 0.0
        
        if holiday_data['is_muslim_holiday']:
            impact += 0.3  # Значительное влияние на водителей
        if holiday_data['is_hindu_holiday']:
            impact += 0.4  # Большое влияние на Бали
        if holiday_data['is_national_holiday']:
            impact += 0.5  # Максимальное влияние
        if holiday_data['is_local_holiday']:
            impact += 0.2  # Умеренное влияние
        
        return min(impact, 1.0)
    
    def get_driver_availability(self, location: str, date: str, hour: int) -> Dict:
        """Расчет доступности водителей с учетом местных факторов"""
        try:
            date_obj = datetime.strptime(date, '%Y-%m-%d')
            
            # Базовая доступность
            base_availability = 1.0
            
            # Влияние времени молитвы
            if hour in DRIVER_FACTORS['prayer_times']:
                base_availability *= 0.8
            
            # Влияние дня недели
            if date_obj.weekday() == 4:  # Пятница
                base_availability *= DRIVER_FACTORS['friday_prayer_impact']
            
            # Получаем данные о праздниках
            holiday_data = self.get_holidays_data(date)
            if holiday_data['is_muslim_holiday']:
                base_availability *= DRIVER_FACTORS['ramadan_impact']
            if holiday_data['is_national_holiday'] or holiday_data['is_hindu_holiday']:
                base_availability *= DRIVER_FACTORS['holiday_impact']
            
            # Влияние погоды
            weather_data = self.get_weather_data(location, date)
            if weather_data['rain_mm'] > 15:
                base_availability *= DRIVER_FACTORS['rain_impact']
            
            # Влияние района
            region_data = BALI_REGIONS.get(location.lower(), {})
            if region_data.get('type') == 'beach_luxury':
                base_availability *= 1.1  # Больше водителей в туристических зонах
            
            driver_data = {
                'location': location,
                'date': date,
                'hour': hour,
                'availability_score': round(base_availability, 2),
                'factors': {
                    'prayer_time': hour in DRIVER_FACTORS['prayer_times'],
                    'friday_prayer': date_obj.weekday() == 4,
                    'holiday_impact': holiday_data['holiday_impact'],
                    'weather_impact': weather_data.get('delivery_impact', 0),
                    'region_type': region_data.get('type', 'unknown')
                }
            }
            
            return driver_data
            
        except Exception as e:
            logger.error(f"Ошибка расчета доступности водителей: {e}")
            return {'availability_score': 0.8, 'factors': {}}
    
    def get_tourist_density(self, location: str, date: str) -> Dict:
        """Расчет плотности туристов для локации и даты"""
        try:
            date_obj = datetime.strptime(date, '%Y-%m-%d')
            month = date_obj.month
            
            # Определяем туристический сезон
            if month in [6, 7, 8, 12, 1]:  # Высокий сезон
                season_multiplier = 1.3
                season_type = 'high'
            elif month in [4, 5, 9, 10]:  # Средний сезон
                season_multiplier = 1.0
                season_type = 'shoulder'
            else:  # Низкий сезон
                season_multiplier = 0.7
                season_type = 'low'
            
            # Базовая плотность по району
            region_data = BALI_REGIONS.get(location.lower(), {})
            base_density = region_data.get('tourist_ratio', 0.5)
            
            # Влияние дня недели
            day_of_week = date_obj.weekday()
            if day_of_week >= 5:  # Выходные
                day_multiplier = 1.2
            else:
                day_multiplier = 1.0
            
            # Финальная плотность
            tourist_density = base_density * season_multiplier * day_multiplier
            
            tourist_data = {
                'location': location,
                'date': date,
                'tourist_density': round(tourist_density, 2),
                'season_type': season_type,
                'season_multiplier': season_multiplier,
                'day_multiplier': day_multiplier,
                'base_density': base_density,
                'region_type': region_data.get('type', 'unknown')
            }
            
            return tourist_data
            
        except Exception as e:
            logger.error(f"Ошибка расчета плотности туристов: {e}")
            return {'tourist_density': 0.5}
    
    def collect_all_data(self, location: str, date: str, hour: int = 12) -> Dict:
        """Сбор всех данных для конкретной локации, даты и времени"""
        logger.info(f"Сбор данных для {location}, {date}, {hour}:00")
        
        try:
            # Собираем все данные
            weather_data = self.get_weather_data(location, date)
            holiday_data = self.get_holidays_data(date)
            driver_data = self.get_driver_availability(location, date, hour)
            tourist_data = self.get_tourist_density(location, date)
            
            # Объединяем данные
            combined_data = {
                'location': location,
                'date': date,
                'hour': hour,
                'timestamp': datetime.now().isoformat(),
                
                # Погодные данные
                'temperature': weather_data.get('temperature', 28),
                'humidity': weather_data.get('humidity', 75),
                'rain_mm': weather_data.get('rain_mm', 0),
                'wind_speed': weather_data.get('wind_speed', 10),
                'rain_category': weather_data.get('rain_category', 'light'),
                'is_comfortable_temp': weather_data.get('is_comfortable_temp', True),
                'is_high_humidity': weather_data.get('is_high_humidity', False),
                'is_extreme_weather': weather_data.get('is_extreme_weather', False),
                'weather_delivery_impact': weather_data.get('delivery_impact', 0),
                
                # Праздники
                'is_national_holiday': holiday_data.get('is_national_holiday', False),
                'is_muslim_holiday': holiday_data.get('is_muslim_holiday', False),
                'is_hindu_holiday': holiday_data.get('is_hindu_holiday', False),
                'is_local_holiday': holiday_data.get('is_local_holiday', False),
                'holiday_impact': holiday_data.get('holiday_impact', 0),
                'holiday_names': holiday_data.get('holiday_names', []),
                
                # Водители
                'driver_availability': driver_data.get('availability_score', 0.8),
                'driver_factors': driver_data.get('factors', {}),
                
                # Туристы
                'tourist_density': tourist_data.get('tourist_density', 0.5),
                'tourist_season': tourist_data.get('season_type', 'shoulder'),
                'region_type': tourist_data.get('region_type', 'unknown'),
            }
            
            logger.info(f"Данные собраны успешно для {location}")
            return combined_data
            
        except Exception as e:
            logger.error(f"Ошибка сбора данных: {e}")
            return self._get_default_combined_data(location, date, hour)
    
    def _get_default_weather(self) -> Dict:
        """Данные по умолчанию для погоды"""
        return {
            'temperature': 28,
            'humidity': 75,
            'rain_mm': 5,
            'wind_speed': 10,
            'rain_category': 'light',
            'is_comfortable_temp': True,
            'is_high_humidity': False,
            'is_extreme_weather': False,
            'delivery_impact': 0.1
        }
    
    def _get_default_holidays(self, date: str) -> Dict:
        """Данные по умолчанию для праздников"""
        return {
            'date': date,
            'is_national_holiday': False,
            'is_muslim_holiday': False,
            'is_hindu_holiday': False,
            'is_local_holiday': False,
            'holiday_impact': 0.0,
            'holiday_names': []
        }
    
    def _get_default_combined_data(self, location: str, date: str, hour: int) -> Dict:
        """Данные по умолчанию для всех источников"""
        return {
            'location': location,
            'date': date,
            'hour': hour,
            'temperature': 28,
            'humidity': 75,
            'rain_mm': 5,
            'driver_availability': 0.8,
            'tourist_density': 0.5,
            'is_muslim_holiday': False,
            'is_hindu_holiday': False,
            'holiday_impact': 0.0,
            'weather_delivery_impact': 0.1
        }

# Пример использования
if __name__ == "__main__":
    collector = BaliDataCollector()
    
    # Тестируем сбор данных
    data = collector.collect_all_data('seminyak', '2024-01-15', 18)
    print(json.dumps(data, indent=2, ensure_ascii=False))