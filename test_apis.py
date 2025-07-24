#!/usr/bin/env python3
"""
🔧 ТЕСТИРОВАНИЕ ВСЕХ API ИНТЕГРАЦИЙ
Проверяет работу Open-Meteo, OpenAI и Calendarific API
"""

import os
import requests
from datetime import datetime
from dotenv import load_dotenv

# Загружаем переменные окружения
load_dotenv()

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
        
        response = requests.get('https://archive-api.open-meteo.com/v1/archive', 
                              params=params, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            daily = data.get('daily', {})
            
            if daily and len(daily.get('time', [])) > 0:
                temp = daily.get('temperature_2m_mean', [0])[0]
                precip = daily.get('precipitation_sum', [0])[0]
                weather_code = daily.get('weather_code', [0])[0]
                
                print("✅ Open-Meteo API: РАБОТАЕТ")
                print(f"   📅 Дата: 2025-01-01")
                print(f"   🌡️ Температура: {temp}°C")
                print(f"   🌧️ Осадки: {precip}mm")
                print(f"   🌤️ Код погоды: {weather_code}")
                print(f"   💰 Стоимость: БЕСПЛАТНО!")
                return True
            else:
                print("⚠️ Open-Meteo API: Нет данных")
                return False
        else:
            print(f"❌ Open-Meteo API: HTTP {response.status_code}")
            return False
            
    except Exception as e:
        print(f"❌ Open-Meteo API: Ошибка - {e}")
        return False

def test_openai_api():
    """Тестирует OpenAI API"""
    print("\n🤖 ТЕСТИРОВАНИЕ OPENAI API")
    print("-" * 40)
    
    api_key = os.getenv('OPENAI_API_KEY')
    if not api_key or api_key == 'ВСТАВИТЬ_ВАШ_КЛЮЧ_OPENAI_СЮДА':
        print("❌ OpenAI API: Ключ не настроен")
        return False
    
    try:
        import openai
        
        # Настраиваем клиент
        client = openai.OpenAI(api_key=api_key)
        
        # Тестовый запрос
        response = client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[
                {"role": "user", "content": "Привет! Это тест API. Ответь одним словом: работает?"}
            ],
            max_tokens=10
        )
        
        answer = response.choices[0].message.content.strip()
        
        print("✅ OpenAI API: РАБОТАЕТ")
        print(f"   💬 Ответ: {answer}")
        print(f"   🏷️ Модель: gpt-3.5-turbo")
        print(f"   💰 Стоимость: ~$0.001 за запрос")
        return True
        
    except Exception as e:
        print(f"❌ OpenAI API: Ошибка - {e}")
        return False

def test_calendarific_api():
    """Тестирует Calendarific API"""
    print("\n📅 ТЕСТИРОВАНИЕ CALENDARIFIC API")
    print("-" * 40)
    
    api_key = os.getenv('CALENDAR_API_KEY')
    if not api_key or api_key == 'ВСТАВИТЬ_ВАШ_КЛЮЧ_КАЛЕНДАРЯ_СЮДА':
        print("❌ Calendarific API: Ключ не настроен")
        return False
    
    try:
        # Тестируем получение праздников Индонезии
        params = {
            'api_key': api_key,
            'country': 'ID',
            'year': 2025,
            'type': 'national'
        }
        
        response = requests.get('https://calendarific.com/api/v2/holidays', 
                              params=params, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            
            if data.get('meta', {}).get('code') == 200:
                holidays = data.get('response', {}).get('holidays', [])
                
                print("✅ Calendarific API: РАБОТАЕТ")
                print(f"   🎪 Найдено праздников: {len(holidays)}")
                
                if holidays:
                    first_holiday = holidays[0]
                    print(f"   📅 Первый праздник: {first_holiday.get('name')}")
                    print(f"   📆 Дата: {first_holiday.get('date', {}).get('iso')}")
                
                print(f"   💰 Лимит: 1000 запросов/месяц (бесплатно)")
                return True
            else:
                error_msg = data.get('meta', {}).get('error_detail', 'Неизвестная ошибка')
                print(f"❌ Calendarific API: {error_msg}")
                return False
        else:
            print(f"❌ Calendarific API: HTTP {response.status_code}")
            return False
            
    except Exception as e:
        print(f"❌ Calendarific API: Ошибка - {e}")
        return False

def main():
    """Главная функция тестирования"""
    print("🔧 ТЕСТИРОВАНИЕ ВСЕХ API ИНТЕГРАЦИЙ")
    print("=" * 50)
    
    # Тестируем все API
    results = {
        'open_meteo': test_open_meteo_api(),
        'openai': test_openai_api(),
        'calendarific': test_calendarific_api()
    }
    
    # Подводим итоги
    print("\n📊 ИТОГИ ТЕСТИРОВАНИЯ")
    print("=" * 50)
    
    working_apis = sum(results.values())
    total_apis = len(results)
    
    print(f"✅ Работающих API: {working_apis}/{total_apis}")
    
    if results['open_meteo']:
        print("✅ Погода: Open-Meteo (БЕСПЛАТНО)")
    else:
        print("❌ Погода: Open-Meteo недоступен")
    
    if results['openai']:
        print("✅ AI: OpenAI GPT (настроен)")
    else:
        print("❌ AI: OpenAI не настроен")
    
    if results['calendarific']:
        print("✅ Календарь: Calendarific (настроен)")
    else:
        print("❌ Календарь: Calendarific не настроен")
    
    # Рекомендации
    print("\n💡 РЕКОМЕНДАЦИИ:")
    
    if working_apis == total_apis:
        print("🎉 ВСЕ API РАБОТАЮТ! Система готова к использованию.")
        print("🚀 Можно запускать полный анализ с максимальной точностью.")
    elif working_apis >= 1:
        print("⚠️ Частичная функциональность доступна.")
        if not results['openai']:
            print("   🤖 Без OpenAI: ограниченный AI-анализ")
        if not results['calendarific']:
            print("   📅 Без Calendarific: статический список праздников")
    else:
        print("🔴 КРИТИЧНО: Ни один API не работает!")
        print("   📋 Проверьте интернет-соединение и API ключи")
    
    print(f"\n💰 ЭКОНОМИЯ: Open-Meteo бесплатен (экономия $480/год)!")
    
    return working_apis == total_apis

if __name__ == "__main__":
    main()