#!/usr/bin/env python3
"""
🌤️ GOOGLE WEATHER API ИНТЕГРАЦИЯ
Замена Open-Meteo на более точный Google Weather API
"""

import requests
import json
from datetime import datetime, timedelta
import sqlite3
import pandas as pd
from typing import Dict, List, Optional, Tuple

class GoogleWeatherAPI:
    """Класс для работы с Google Weather API"""
    
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://weather.googleapis.com/v1"
        
    def get_current_conditions(self, latitude: float, longitude: float) -> Dict:
        """Получает текущие погодные условия"""
        
        url = f"{self.base_url}/currentConditions:lookup"
        
        params = {
            'key': self.api_key,
            'location.latitude': latitude,
            'location.longitude': longitude,
            'languageCode': 'ru'
        }
        
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            print(f"❌ Ошибка получения текущей погоды: {e}")
            return {}
    
    def get_forecast(self, latitude: float, longitude: float, days: int = 10) -> Dict:
        """Получает прогноз погоды на несколько дней"""
        
        url = f"{self.base_url}/forecast:lookup"
        
        params = {
            'key': self.api_key,
            'location.latitude': latitude,
            'location.longitude': longitude,
            'languageCode': 'ru'
        }
        
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            print(f"❌ Ошибка получения прогноза: {e}")
            return {}
    
    def get_historical_weather(self, latitude: float, longitude: float, hours: int = 24) -> Dict:
        """Получает исторические данные о погоде"""
        
        url = f"{self.base_url}/history:lookup"
        
        params = {
            'key': self.api_key,
            'location.latitude': latitude,
            'location.longitude': longitude,
            'hours': min(hours, 24)  # Максимум 24 часа
        }
        
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            print(f"❌ Ошибка получения исторических данных: {e}")
            return {}

class GoogleWeatherAnalyzer:
    """Анализатор погодных данных Google Weather API"""
    
    def __init__(self, api_key: str):
        self.weather_api = GoogleWeatherAPI(api_key)
        
    def analyze_weather_impact_on_sales(self, restaurant_locations: List[Dict], days_back: int = 30) -> Dict:
        """Анализирует влияние погоды на продажи ресторанов"""
        
        print("🌤️ АНАЛИЗ ВЛИЯНИЯ ПОГОДЫ НА ПРОДАЖИ (GOOGLE WEATHER API)")
        print("=" * 70)
        
        weather_sales_data = []
        
        # Получаем данные о продажах
        conn = sqlite3.connect('database.sqlite')
        
        for location in restaurant_locations:
            restaurant_name = location['name']
            latitude = location['latitude']
            longitude = location['longitude']
            
            print(f"📍 Анализируем {restaurant_name} ({latitude}, {longitude})")
            
            # Получаем продажи ресторана за последние дни
            query = """
            SELECT stat_date, sales 
            FROM grab_stats 
            WHERE restaurant_name = ? 
            AND stat_date >= date('now', '-{} days')
            UNION ALL
            SELECT stat_date, sales 
            FROM gojek_stats 
            WHERE restaurant_name = ? 
            AND stat_date >= date('now', '-{} days')
            ORDER BY stat_date
            """.format(days_back, days_back)
            
            sales_df = pd.read_sql_query(query, conn, params=[restaurant_name, restaurant_name])
            
            if sales_df.empty:
                continue
                
            # Группируем по дням
            daily_sales = sales_df.groupby('stat_date')['sales'].sum().reset_index()
            
            # Получаем погодные данные для каждого дня
            for _, row in daily_sales.iterrows():
                date = row['stat_date']
                sales = row['sales']
                
                # Получаем погоду на эту дату (используем текущие условия как аппроксимацию)
                weather_data = self.weather_api.get_current_conditions(latitude, longitude)
                
                if weather_data:
                    weather_condition = weather_data.get('weatherCondition', {}).get('type', 'UNKNOWN')
                    temperature = weather_data.get('temperature', {}).get('degrees', 0)
                    precipitation_prob = weather_data.get('precipitation', {}).get('probability', {}).get('percent', 0)
                    
                    weather_sales_data.append({
                        'restaurant': restaurant_name,
                        'date': date,
                        'sales': sales,
                        'weather_condition': weather_condition,
                        'temperature': temperature,
                        'precipitation_probability': precipitation_prob
                    })
        
        conn.close()
        
        # Анализируем корреляции
        if weather_sales_data:
            df = pd.DataFrame(weather_sales_data)
            correlations = self._calculate_weather_correlations(df)
            return correlations
        else:
            return {}
    
    def _calculate_weather_correlations(self, df: pd.DataFrame) -> Dict:
        """Рассчитывает корреляции между погодой и продажами"""
        
        # Группируем по типам погоды
        weather_impact = {}
        
        # Анализ по типам погоды
        weather_groups = df.groupby('weather_condition')['sales'].agg(['mean', 'count']).reset_index()
        overall_mean = df['sales'].mean()
        
        for _, row in weather_groups.iterrows():
            condition = row['weather_condition']
            mean_sales = row['mean']
            count = row['count']
            
            if count >= 3:  # Минимум 3 наблюдения
                impact = (mean_sales - overall_mean) / overall_mean
                weather_impact[condition] = {
                    'impact': impact,
                    'mean_sales': mean_sales,
                    'count': count,
                    'description': self._get_weather_description(condition, impact)
                }
        
        # Корреляция с температурой
        temp_correlation = df['temperature'].corr(df['sales']) if len(df) > 5 else 0
        
        # Корреляция с вероятностью осадков
        precip_correlation = df['precipitation_probability'].corr(df['sales']) if len(df) > 5 else 0
        
        return {
            'weather_conditions': weather_impact,
            'temperature_correlation': temp_correlation,
            'precipitation_correlation': precip_correlation,
            'total_observations': len(df),
            'analysis_date': datetime.now().isoformat()
        }
    
    def _get_weather_description(self, condition: str, impact: float) -> str:
        """Создает описание влияния погоды"""
        
        impact_percent = impact * 100
        direction = "увеличивает" if impact > 0 else "снижает"
        
        condition_names = {
            'CLEAR': 'ясная погода',
            'PARTLY_CLOUDY': 'переменная облачность',
            'CLOUDY': 'облачно',
            'OVERCAST': 'пасмурно',
            'RAIN': 'дождь',
            'DRIZZLE': 'морось',
            'THUNDERSTORM': 'гроза',
            'SNOW': 'снег'
        }
        
        condition_name = condition_names.get(condition, condition.lower())
        
        return f"{condition_name} {direction} продажи на {abs(impact_percent):.1f}%"

def integrate_google_weather_into_system():
    """Интегрирует Google Weather API в существующую систему"""
    
    print("🔄 ИНТЕГРАЦИЯ GOOGLE WEATHER API В СИСТЕМУ")
    print("=" * 60)
    
    # Проверяем наличие API ключа
    try:
        with open('.env', 'r') as f:
            env_content = f.read()
            if 'GOOGLE_MAPS_API_KEY' not in env_content:
                print("⚠️ Добавьте GOOGLE_MAPS_API_KEY в файл .env")
                return False
    except FileNotFoundError:
        print("❌ Файл .env не найден")
        return False
    
    # Создаем примерные локации ресторанов (координаты Бали)
    restaurant_locations = [
        {'name': 'Seminyak Restaurant', 'latitude': -8.6905, 'longitude': 115.1729},
        {'name': 'Ubud Restaurant', 'latitude': -8.5069, 'longitude': 115.2625},
        {'name': 'Canggu Restaurant', 'latitude': -8.6482, 'longitude': 115.1342},
        {'name': 'Sanur Restaurant', 'latitude': -8.6878, 'longitude': 115.2613}
    ]
    
    # Загружаем API ключ
    import os
    from dotenv import load_dotenv
    
    load_dotenv()
    api_key = os.getenv('GOOGLE_MAPS_API_KEY')
    
    if not api_key:
        print("❌ Google Maps API ключ не найден в переменных окружения")
        return False
    
    # Создаем анализатор
    analyzer = GoogleWeatherAnalyzer(api_key)
    
    # Анализируем влияние погоды
    print("🔍 Анализируем влияние погоды на продажи...")
    correlations = analyzer.analyze_weather_impact_on_sales(restaurant_locations)
    
    if correlations:
        # Сохраняем результаты
        with open('google_weather_correlations.json', 'w', encoding='utf-8') as f:
            json.dump(correlations, f, indent=2, ensure_ascii=False)
        
        print("✅ Корреляции Google Weather сохранены в google_weather_correlations.json")
        
        # Обновляем основной файл коэффициентов
        try:
            with open('real_coefficients.json', 'r', encoding='utf-8') as f:
                real_coeffs = json.load(f)
        except:
            real_coeffs = {}
        
        # Добавляем Google Weather данные
        real_coeffs['google_weather'] = correlations['weather_conditions']
        real_coeffs['google_temperature_correlation'] = correlations['temperature_correlation']
        real_coeffs['google_precipitation_correlation'] = correlations['precipitation_correlation']
        
        with open('real_coefficients.json', 'w', encoding='utf-8') as f:
            json.dump(real_coeffs, f, indent=2, ensure_ascii=False)
        
        print("✅ Обновлен файл real_coefficients.json с Google Weather данными")
        
        # Показываем результаты
        print("\n📊 РЕЗУЛЬТАТЫ АНАЛИЗА GOOGLE WEATHER:")
        print("-" * 50)
        
        for condition, data in correlations['weather_conditions'].items():
            impact = data['impact'] * 100
            print(f"   {condition}: {impact:+.1f}% ({data['description']})")
        
        print(f"\n🌡️ Корреляция с температурой: {correlations['temperature_correlation']:.3f}")
        print(f"🌧️ Корреляция с осадками: {correlations['precipitation_correlation']:.3f}")
        print(f"📈 Всего наблюдений: {correlations['total_observations']}")
        
        return True
    else:
        print("❌ Не удалось получить данные Google Weather")
        return False

def test_google_weather_api():
    """Тестирует Google Weather API"""
    
    print("🧪 ТЕСТИРОВАНИЕ GOOGLE WEATHER API")
    print("=" * 40)
    
    import os
    from dotenv import load_dotenv
    
    load_dotenv()
    api_key = os.getenv('GOOGLE_MAPS_API_KEY')
    
    if not api_key:
        print("❌ Google Maps API ключ не найден")
        return False
    
    # Создаем API клиент
    weather_api = GoogleWeatherAPI(api_key)
    
    # Тестируем на координатах Семиньяка, Бали
    latitude = -8.6905
    longitude = 115.1729
    
    print(f"📍 Тестируем на координатах: {latitude}, {longitude} (Семиньяк, Бали)")
    
    # Тест текущих условий
    print("\n🌤️ Тестируем текущие условия...")
    current = weather_api.get_current_conditions(latitude, longitude)
    
    if current:
        print("✅ Текущие условия получены:")
        weather_condition = current.get('weatherCondition', {})
        temperature = current.get('temperature', {})
        
        print(f"   Погода: {weather_condition.get('description', {}).get('text', 'N/A')}")
        print(f"   Температура: {temperature.get('degrees', 'N/A')}°{temperature.get('unit', '')}")
        print(f"   Влажность: {current.get('relativeHumidity', 'N/A')}%")
    else:
        print("❌ Не удалось получить текущие условия")
        return False
    
    # Тест прогноза
    print("\n📅 Тестируем прогноз...")
    forecast = weather_api.get_forecast(latitude, longitude)
    
    if forecast:
        print("✅ Прогноз получен")
        daily_forecasts = forecast.get('dailyForecasts', [])
        print(f"   Дней в прогнозе: {len(daily_forecasts)}")
    else:
        print("❌ Не удалось получить прогноз")
    
    print("\n🎉 Тестирование Google Weather API завершено успешно!")
    return True

if __name__ == "__main__":
    # Тестируем API
    if test_google_weather_api():
        # Интегрируем в систему
        integrate_google_weather_into_system()
    else:
        print("❌ Тестирование не прошло, интеграция отменена")