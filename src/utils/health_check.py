#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
🔍 ПРОВЕРКА РАБОТОСПОСОБНОСТИ СИСТЕМЫ MUZAQUEST
===============================================
Скрипт для проверки всех компонентов системы analytics
"""

import sqlite3
import json
import os
import sys
import subprocess
from datetime import datetime, timedelta

def check_database():
    """Проверка работоспособности базы данных"""
    print("📊 ПРОВЕРКА БАЗЫ ДАННЫХ")
    print("-" * 40)
    
    if not os.path.exists('database.sqlite'):
        print("❌ База данных не найдена: database.sqlite")
        return False
    
    try:
        conn = sqlite3.connect('database.sqlite')
        cursor = conn.cursor()
        
        # Проверяем основные таблицы
        tables_to_check = {
            'restaurants': 'Рестораны',
            'grab_stats': 'Статистика Grab',
            'gojek_stats': 'Статистика Gojek'
        }
        
        all_good = True
        total_records = 0
        
        for table, description in tables_to_check.items():
            try:
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = cursor.fetchone()[0]
                print(f"✅ {description}: {count:,} записей")
                total_records += count
                
                if count == 0:
                    print(f"⚠️ Предупреждение: Таблица {table} пуста")
                    all_good = False
                    
            except sqlite3.Error as e:
                print(f"❌ Ошибка таблицы {table}: {e}")
                all_good = False
        
        # Проверяем актуальность данных
        try:
            cursor.execute("""
                SELECT 
                    MIN(stat_date) as min_date,
                    MAX(stat_date) as max_date
                FROM (
                    SELECT stat_date FROM grab_stats
                    UNION ALL
                    SELECT stat_date FROM gojek_stats
                )
            """)
            min_date, max_date = cursor.fetchone()
            
            if min_date and max_date:
                print(f"📅 Период данных: {min_date} → {max_date}")
                
                # Проверяем свежесть данных
                max_date_obj = datetime.strptime(max_date, '%Y-%m-%d')
                days_old = (datetime.now() - max_date_obj).days
                
                if days_old <= 7:
                    print(f"✅ Данные свежие (обновлены {days_old} дней назад)")
                elif days_old <= 30:
                    print(f"⚠️ Данные устарели ({days_old} дней назад)")
                else:
                    print(f"❌ Данные сильно устарели ({days_old} дней назад)")
                    all_good = False
            
        except Exception as e:
            print(f"⚠️ Не удалось проверить актуальность данных: {e}")
        
        conn.close()
        
        print(f"📊 Всего записей в БД: {total_records:,}")
        print()
        
        return all_good
        
    except sqlite3.Error as e:
        print(f"❌ Ошибка подключения к БД: {e}")
        return False

def check_location_data():
    """Проверка файла с координатами ресторанов"""
    print("🗺️ ПРОВЕРКА КООРДИНАТ РЕСТОРАНОВ")
    print("-" * 40)
    
    locations_file = 'data/bali_restaurant_locations.json'
    
    if not os.path.exists(locations_file):
        print(f"❌ Файл координат не найден: {locations_file}")
        return False
    
    try:
        with open(locations_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        restaurant_count = len(data.get('restaurants', []))
        print(f"✅ Загружено координат: {restaurant_count} ресторанов")
        
        # Проверяем корректность координат
        invalid_coords = 0
        for restaurant in data.get('restaurants', []):
            lat = restaurant.get('latitude')
            lon = restaurant.get('longitude')
            
            if not lat or not lon:
                invalid_coords += 1
                continue
                
            # Проверяем что координаты в пределах Бали
            if not (-9.0 <= lat <= -8.0 and 114.5 <= lon <= 116.0):
                print(f"⚠️ Подозрительные координаты для {restaurant.get('name')}: {lat}, {lon}")
        
        if invalid_coords > 0:
            print(f"❌ Найдено {invalid_coords} ресторанов с некорректными координатами")
            return False
        
        last_updated = data.get('last_updated', 'неизвестно')
        print(f"📅 Последнее обновление: {last_updated}")
        print()
        
        return True
        
    except json.JSONDecodeError:
        print(f"❌ Файл {locations_file} поврежден (некорректный JSON)")
        return False
    except Exception as e:
        print(f"❌ Ошибка проверки координат: {e}")
        return False

def check_api_keys():
    """Проверка наличия API ключей"""
    print("🔑 ПРОВЕРКА API КЛЮЧЕЙ")
    print("-" * 40)
    
    env_file = '.env'
    
    if not os.path.exists(env_file):
        print(f"⚠️ Файл .env не найден - API функции могут не работать")
        return False
    
    try:
        with open(env_file, 'r') as f:
            env_content = f.read()
        
        required_keys = {
            'OPENAI_API_KEY': 'OpenAI (AI анализ)',
            'CALENDARIFIC_API_KEY': 'Calendarific (праздники)',
            # Weather API не требует ключа (Open-Meteo)
        }
        
        all_keys_present = True
        
        for key, description in required_keys.items():
            if key in env_content and len(env_content.split(f'{key}=')[1].split('\n')[0].strip()) > 10:
                print(f"✅ {description}: настроен")
            else:
                print(f"❌ {description}: отсутствует или некорректен")
                all_keys_present = False
        
        # Weather API (Open-Meteo) бесплатный
        print(f"✅ Weather API (Open-Meteo): бесплатный, не требует ключа")
        
        print()
        return all_keys_present
        
    except Exception as e:
        print(f"❌ Ошибка проверки .env: {e}")
        return False

def check_dependencies():
    """Проверка установленных зависимостей"""
    print("📦 ПРОВЕРКА ЗАВИСИМОСТЕЙ")
    print("-" * 40)
    
    required_packages = {
        'pandas': 'Обработка данных',
        'sqlite3': 'База данных (встроенный)',
        'streamlit': 'Веб-интерфейс',
        'requests': 'API запросы',
        'plotly': 'Графики',
        'scikit-learn': 'ML модели',
        'prophet': 'Прогнозирование',
        'openpyxl': 'Excel файлы'
    }
    
    all_installed = True
    
    for package, description in required_packages.items():
        try:
            if package == 'sqlite3':
                import sqlite3
                print(f"✅ {description}: {sqlite3.sqlite_version}")
            else:
                __import__(package)
                try:
                    version = __import__(package).__version__
                    print(f"✅ {description}: v{version}")
                except:
                    print(f"✅ {description}: установлен")
                    
        except ImportError:
            print(f"❌ {description}: НЕ УСТАНОВЛЕН")
            all_installed = False
    
    print()
    return all_installed

def check_web_app():
    """Проверка веб-приложения"""
    print("🌐 ПРОВЕРКА ВЕБ-ПРИЛОЖЕНИЯ")
    print("-" * 40)
    
    web_app_file = 'web_app_optimized.py'
    
    if not os.path.exists(web_app_file):
        print(f"❌ Веб-приложение не найдено: {web_app_file}")
        return False
    
    try:
        # Проверяем синтаксис
        with open(web_app_file, 'r', encoding='utf-8') as f:
            content = f.read()
        
        compile(content, web_app_file, 'exec')
        print(f"✅ Синтаксис {web_app_file}: корректен")
        
        # Проверяем ключевые функции
        if 'load_restaurants' in content:
            print("✅ Функция загрузки ресторанов: найдена")
        else:
            print("❌ Функция загрузки ресторанов: отсутствует")
            return False
            
        if 'run_analysis' in content:
            print("✅ Функция запуска анализа: найдена")
        else:
            print("❌ Функция запуска анализа: отсутствует")
            return False
        
        print()
        return True
        
    except SyntaxError as e:
        print(f"❌ Синтаксическая ошибка в {web_app_file}: {e}")
        return False
    except Exception as e:
        print(f"❌ Ошибка проверки веб-приложения: {e}")
        return False

def check_main_analyzer():
    """Проверка основного анализатора"""
    print("🔬 ПРОВЕРКА ОСНОВНОГО АНАЛИЗАТОРА")
    print("-" * 40)
    
    main_file = 'main.py'
    
    if not os.path.exists(main_file):
        print(f"❌ Основной анализатор не найден: {main_file}")
        return False
    
    try:
        # Проверяем что основные функции присутствуют
        with open(main_file, 'r', encoding='utf-8') as f:
            content = f.read()
        
        required_functions = {
            'analyze_restaurant': 'Анализ ресторана',
            'analyze_market': 'Анализ рынка',
            'list_restaurants': 'Список ресторанов'
        }
        
        all_functions_present = True
        
        for func, description in required_functions.items():
            if f'def {func}(' in content:
                print(f"✅ {description}: найдена")
            else:
                print(f"❌ {description}: отсутствует")
                all_functions_present = False
        
        print()
        return all_functions_present
        
    except Exception as e:
        print(f"❌ Ошибка проверки main.py: {e}")
        return False

def run_quick_test():
    """Быстрый тест системы"""
    print("⚡ БЫСТРЫЙ ТЕСТ СИСТЕМЫ")
    print("-" * 40)
    
    try:
        # Тестируем получение списка ресторанов
        result = subprocess.run([
            sys.executable, 'main.py', 'list'
        ], capture_output=True, text=True, timeout=30)
        
        if result.returncode == 0:
            lines = result.stdout.strip().split('\n')
            restaurant_lines = [l for l in lines if '🍽️' in l]
            print(f"✅ Список ресторанов: получено {len(restaurant_lines)} ресторанов")
        else:
            print(f"❌ Ошибка получения списка ресторанов: {result.stderr}")
            return False
        
        print()
        return True
        
    except subprocess.TimeoutExpired:
        print("❌ Тайм-аут при тестировании системы")
        return False
    except Exception as e:
        print(f"❌ Ошибка тестирования: {e}")
        return False

def main():
    """Главная функция проверки"""
    
    print("🔍 MUZAQUEST ANALYTICS - ПРОВЕРКА СИСТЕМЫ")
    print("=" * 60)
    print(f"🕐 Время проверки: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    # Список всех проверок
    checks = [
        ("База данных", check_database),
        ("Координаты ресторанов", check_location_data),
        ("API ключи", check_api_keys),
        ("Зависимости Python", check_dependencies),
        ("Веб-приложение", check_web_app),
        ("Основной анализатор", check_main_analyzer),
        ("Быстрый тест", run_quick_test)
    ]
    
    results = []
    
    # Выполняем все проверки
    for check_name, check_func in checks:
        try:
            result = check_func()
            results.append((check_name, result))
        except Exception as e:
            print(f"❌ Критическая ошибка в проверке '{check_name}': {e}")
            results.append((check_name, False))
        print()
    
    # Итоговый отчет
    print("📋 ИТОГОВЫЙ ОТЧЕТ")
    print("=" * 60)
    
    passed = 0
    total = len(results)
    
    for check_name, result in results:
        status = "✅ ПРОЙДЕНА" if result else "❌ НЕ ПРОЙДЕНА"
        print(f"{status:15} | {check_name}")
        if result:
            passed += 1
    
    print("-" * 60)
    print(f"📊 РЕЗУЛЬТАТ: {passed}/{total} проверок пройдено")
    
    if passed == total:
        print("🎉 ВСЕ ПРОВЕРКИ ПРОЙДЕНЫ! Система готова к работе.")
        sys.exit(0)
    elif passed >= total * 0.8:
        print("⚠️ Большинство проверок пройдено, но есть проблемы.")
        sys.exit(1)
    else:
        print("❌ КРИТИЧЕСКИЕ ПРОБЛЕМЫ! Система требует исправления.")
        sys.exit(2)

if __name__ == "__main__":
    main()