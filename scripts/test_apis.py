#!/usr/bin/env python3
"""
🔧 ТЕСТИРОВАНИЕ ВСЕХ API ИНТЕГРАЦИЙ
Проверяет работу Open-Meteo, OpenAI и Calendarific API
"""

import os
import sys
import requests
from datetime import datetime
from dotenv import load_dotenv

# Добавляем корневую папку в путь для импорта
sys.path.append('..')

# Загружаем переменные окружения
load_dotenv('../.env')

def test_open_meteo_api():
    """Тестирует Open-Meteo API (бесплатный)"""
    print("🌤️ ТЕСТИРОВАНИЕ OPEN-METEO API")
    print("-" * 40)
    
    try:
        # Тестируем исторические данные для Бали
        params = {
            'latitude': -8.4095,
            'longitude': 115.1889,
            'start_date': '2025-01-01',
            'end_date': '2025-01-01',
            'daily': 'temperature_2m_mean,precipitation_sum,weather_code',
            'timezone': 'Asia/Jakarta'
        }
        
        response = requests.get('https://archive-api.open-meteo.com/v1/archive', params=params)
        
        if response.status_code == 200:
            data = response.json()
            daily = data.get('daily', {})
            
            print(f"✅ Статус: {response.status_code}")
            print(f"📍 Координаты: {data.get('latitude')}, {data.get('longitude')}")
            print(f"🌡️ Температура: {daily.get('temperature_2m_mean', [None])[0]}°C")
            print(f"🌧️ Осадки: {daily.get('precipitation_sum', [None])[0]}mm")
            print(f"☁️ Код погоды: {daily.get('weather_code', [None])[0]}")
            return True
        else:
            print(f"❌ Ошибка: {response.status_code}")
            print(f"📝 Ответ: {response.text}")
            return False
            
    except Exception as e:
        print(f"❌ Исключение: {e}")
        return False

def test_openai_api():
    """Тестирует OpenAI API"""
    print("\n🤖 ТЕСТИРОВАНИЕ OPENAI API")
    print("-" * 40)
    
    api_key = os.getenv('OPENAI_API_KEY')
    if not api_key:
        print("❌ OPENAI_API_KEY не найден в .env")
        return False
    
    print(f"🔑 API ключ: {api_key[:20]}...{api_key[-10:]}")
    
    try:
        headers = {
            'Authorization': f'Bearer {api_key}',
            'Content-Type': 'application/json'
        }
        
        data = {
            'model': 'gpt-3.5-turbo',
            'messages': [
                {'role': 'user', 'content': 'Привет! Это тест API.'}
            ],
            'max_tokens': 50
        }
        
        response = requests.post('https://api.openai.com/v1/chat/completions', 
                               headers=headers, json=data)
        
        if response.status_code == 200:
            result = response.json()
            message = result['choices'][0]['message']['content']
            print(f"✅ Статус: {response.status_code}")
            print(f"🤖 Ответ: {message}")
            print(f"💰 Токены: {result['usage']['total_tokens']}")
            return True
        else:
            print(f"❌ Ошибка: {response.status_code}")
            print(f"📝 Ответ: {response.text}")
            return False
            
    except Exception as e:
        print(f"❌ Исключение: {e}")
        return False

def test_calendarific_api():
    """Тестирует Calendarific API"""
    print("\n📅 ТЕСТИРОВАНИЕ CALENDARIFIC API")
    print("-" * 40)
    
    api_key = os.getenv('CALENDARIFIC_API_KEY')
    if not api_key:
        print("❌ CALENDARIFIC_API_KEY не найден в .env")
        return False
    
    print(f"🔑 API ключ: {api_key[:10]}...{api_key[-5:]}")
    
    try:
        params = {
            'api_key': api_key,
            'country': 'ID',
            'year': 2025,
            'type': 'national'
        }
        
        response = requests.get('https://calendarific.com/api/v2/holidays', params=params)
        
        if response.status_code == 200:
            data = response.json()
            holidays = data.get('response', {}).get('holidays', [])
            
            print(f"✅ Статус: {response.status_code}")
            print(f"🏝️ Страна: Indonesia")
            print(f"📅 Год: 2025")
            print(f"🎉 Найдено праздников: {len(holidays)}")
            
            # Показываем первые 3 праздника
            for holiday in holidays[:3]:
                name = holiday.get('name', 'N/A')
                date = holiday.get('date', {}).get('iso', 'N/A')
                print(f"  • {date}: {name}")
            
            return True
        else:
            print(f"❌ Ошибка: {response.status_code}")
            print(f"📝 Ответ: {response.text}")
            return False
            
    except Exception as e:
        print(f"❌ Исключение: {e}")
        return False

def test_google_maps_api():
    """Тестирует Google Maps API"""
    print("\n🗺️ ТЕСТИРОВАНИЕ GOOGLE MAPS API")
    print("-" * 40)
    
    api_key = os.getenv('GOOGLE_MAPS_API_KEY')
    if not api_key:
        print("❌ GOOGLE_MAPS_API_KEY не найден в .env")
        return False
    
    print(f"🔑 API ключ: {api_key[:20]}...{api_key[-10:]}")
    
    try:
        # Тестируем геокодирование
        params = {
            'address': 'Canggu, Bali, Indonesia',
            'key': api_key
        }
        
        response = requests.get('https://maps.googleapis.com/maps/api/geocode/json', params=params)
        
        if response.status_code == 200:
            data = response.json()
            results = data.get('results', [])
            
            if results:
                location = results[0]['geometry']['location']
                formatted_address = results[0]['formatted_address']
                
                print(f"✅ Статус: {response.status_code}")
                print(f"📍 Адрес: {formatted_address}")
                print(f"🌐 Координаты: {location['lat']}, {location['lng']}")
                return True
            else:
                print("❌ Результаты не найдены")
                return False
        else:
            print(f"❌ Ошибка: {response.status_code}")
            print(f"📝 Ответ: {response.text}")
            return False
            
    except Exception as e:
        print(f"❌ Исключение: {e}")
        return False

def main():
    """Запускает все тесты API"""
    print("🚀 ПОЛНОЕ ТЕСТИРОВАНИЕ API ИНТЕГРАЦИЙ")
    print("=" * 50)
    print(f"📅 Время тестирования: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    results = {}
    
    # Тестируем все API
    results['open_meteo'] = test_open_meteo_api()
    results['openai'] = test_openai_api()
    results['calendarific'] = test_calendarific_api()
    results['google_maps'] = test_google_maps_api()
    
    # Итоговый отчет
    print("\n📊 ИТОГОВЫЙ ОТЧЕТ")
    print("=" * 50)
    
    total_tests = len(results)
    passed_tests = sum(results.values())
    
    for api_name, status in results.items():
        status_icon = "✅" if status else "❌"
        print(f"{status_icon} {api_name.upper()}: {'РАБОТАЕТ' if status else 'НЕ РАБОТАЕТ'}")
    
    print()
    print(f"🎯 РЕЗУЛЬТАТ: {passed_tests}/{total_tests} API работают корректно")
    
    if passed_tests == total_tests:
        print("🎉 ВСЕ API ИНТЕГРАЦИИ ГОТОВЫ К РАБОТЕ!")
    else:
        print("⚠️ Некоторые API требуют настройки")
    
    return passed_tests == total_tests

if __name__ == "__main__":
    main()