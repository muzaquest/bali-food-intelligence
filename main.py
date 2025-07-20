#!/usr/bin/env python3
"""
🎯 ГЛАВНЫЙ CLI ДЛЯ ПРОДВИНУТОЙ СИСТЕМЫ АНАЛИТИКИ РЕСТОРАНОВ
Интегрирует все компоненты системы: глубокую аналитику, генерацию отчетов, аномалии
"""

import argparse
import sys
import sqlite3
from datetime import datetime, timedelta
from main.report_generator import generate_restaurant_report, generate_market_report
from main.advanced_analytics import run_advanced_analysis

def list_restaurants():
    """Показывает список доступных ресторанов"""
    print("🏪 ДОСТУПНЫЕ РЕСТОРАНЫ")
    print("=" * 50)
    
    try:
        # Используем реальную базу данных
        conn = sqlite3.connect('data/database.sqlite')
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT restaurant_name,
                   COUNT(DISTINCT date) as days_data,
                   MIN(date) as first_date,
                   MAX(date) as last_date,
                   SUM(CASE WHEN platform = 'grab' THEN 1 ELSE 0 END) as grab_records,
                   SUM(CASE WHEN platform = 'gojek' THEN 1 ELSE 0 END) as gojek_records
            FROM restaurant_data
            GROUP BY restaurant_name
            ORDER BY restaurant_name
        ''')
        
        restaurants = cursor.fetchall()
        
        for i, (name, days, first_date, last_date, grab_records, gojek_records) in enumerate(restaurants, 1):
            print(f"{i}. 🍽️ {name}")
            if days:
                print(f"   📊 Данных: {days} дней ({first_date} → {last_date})")
                print(f"   📈 Grab: {grab_records} записей | Gojek: {gojek_records} записей")
            else:
                print(f"   📊 Данных: нет")
            print()
        
        conn.close()
        
    except Exception as e:
        print(f"❌ Ошибка при получении списка ресторанов: {e}")

def generate_unified_restaurant_report(restaurant_name: str, period_start: str = None, period_end: str = None):
    """🎯 НОВАЯ ФУНКЦИЯ: Генерирует ПОЛНЫЙ унифицированный отчет - ВСЁ В ОДНОМ!"""
    
    try:
        from main.unified_restaurant_analyzer import UnifiedRestaurantAnalyzer
        
        print(f"🏪 ПОЛНЫЙ АНАЛИЗ РЕСТОРАНА: {restaurant_name.upper()}")
        print("=" * 80)
        print("💡 Включает: аномалии, погоду, праздники, конкурентов, ИИ-рекомендации")
        print()
        
        analyzer = UnifiedRestaurantAnalyzer()
        report = analyzer.generate_full_report(restaurant_name, period_start, period_end)
        
        print(report)
        analyzer.close()
        
    except ImportError as e:
        print(f"⚠️ Новый анализатор недоступен: {e}")
        print("🔄 Переключаемся на стандартную систему...")
        generate_full_report(restaurant_name, period_start, period_end)
    except Exception as e:
        print(f"❌ Ошибка при генерации полного отчета: {e}")

def generate_full_report(restaurant_name: str, period_start: str = None, period_end: str = None):
    """Генерирует полный отчет для ресторана (старая версия)"""
    print(f"🔬 ГЕНЕРАЦИЯ ДЕТАЛЬНОГО АНАЛИЗА ДЛЯ: {restaurant_name.upper()}")
    print("=" * 80)
    
    try:
        # Пробуем использовать улучшенный генератор отчетов
        try:
            from main.enhanced_report_generator import EnhancedReportGenerator
            enhanced_gen = EnhancedReportGenerator()
            
            # Устанавливаем даты по умолчанию если не указаны
            start_date = period_start or '2024-01-01'
            end_date = period_end or '2025-06-30'
            
            report = enhanced_gen.generate_detailed_report(restaurant_name, start_date, end_date)
            print(report)
            
            enhanced_gen.close()
            return
            
        except ImportError as e:
            print(f"⚠️ Улучшенная аналитика недоступна: {e}")
            print("🔄 Переключаемся на стандартную систему...")
        
        # Fallback к старой системе
        report = generate_restaurant_report(restaurant_name, period_start, period_end)
        print(report)
        
        # Сохраняем отчет в файл
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"reports/{restaurant_name.replace(' ', '_')}_{timestamp}.txt"
        
        try:
            import os
            os.makedirs('reports', exist_ok=True)
            
            with open(filename, 'w', encoding='utf-8') as f:
                f.write(report)
            
            print(f"💾 Отчет сохранен в файл: {filename}")
            
        except Exception as e:
            print(f"⚠️ Не удалось сохранить отчет в файл: {e}")
            
    except Exception as e:
        print(f"❌ Ошибка при генерации отчета: {e}")

def generate_market_overview():
    """Генерирует полный обзор рынка с глубокой аналитикой"""
    print("🏢 ПОЛНЫЙ АНАЛИЗ РЫНКА")
    print("=" * 50)
    print("💡 Включает: аномалии, погоду, праздники, сегменты, конкурентов, тренды, ИИ-рекомендации")
    print()
    
    try:
        # Пробуем использовать новый полноценный анализатор
        try:
            from main.unified_market_analyzer import UnifiedMarketAnalyzer
            analyzer = UnifiedMarketAnalyzer()
            
            # Устанавливаем даты по умолчанию (последние 90 дней)
            end_date = datetime.now().strftime('%Y-%m-%d')
            start_date = (datetime.now() - timedelta(days=90)).strftime('%Y-%m-%d')
            
            # Генерируем полный рыночный отчет
            full_report = analyzer.generate_full_market_report(start_date, end_date)
            
            # Выводим отчет
            print(full_report)
            
            # Сохраняем в файл
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"reports/full_market_analysis_{timestamp}.txt"
            
            import os
            os.makedirs("reports", exist_ok=True)
            with open(filename, 'w', encoding='utf-8') as f:
                f.write(full_report)
            
            print(f"\n💾 Полный рыночный анализ сохранен в файл: {filename}")
            
            analyzer.close()
            return
            
        except ImportError as e:
            print(f"⚠️ Новый рыночный анализатор недоступен: {e}")
            print("🔄 Переключаемся на базовую систему...")
        
        # Fallback к старой системе
        report = generate_market_report()
        print(report)
        
        # Сохраняем отчет
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"reports/market_overview_{timestamp}.txt"
        
        try:
            import os
            os.makedirs('reports', exist_ok=True)
            
            with open(filename, 'w', encoding='utf-8') as f:
                f.write(report)
            
            print(f"💾 Обзор рынка сохранен в файл: {filename}")
            
        except Exception as e:
            print(f"⚠️ Не удалось сохранить обзор в файл: {e}")
            
    except Exception as e:
        print(f"❌ Ошибка при генерации обзора рынка: {e}")

def quick_analysis(restaurant_name: str):
    """Быстрый анализ ресторана"""
    print(f"⚡ БЫСТРЫЙ АНАЛИЗ: {restaurant_name.upper()}")
    print("=" * 50)
    
    try:
        # Последние 30 дней
        end_date = datetime.now()
        start_date = end_date - timedelta(days=30)
        
        analysis = run_advanced_analysis(
            restaurant_name, 
            start_date.strftime('%Y-%m-%d'), 
            end_date.strftime('%Y-%m-%d')
        )
        
        if "error" in analysis:
            print(f"❌ {analysis['error']}")
            return
        
        stats = analysis['current_stats']
        competitive = analysis['competitive_analysis']
        insights = analysis['business_insights']
        recommendations = analysis['recommendations']
        
        print(f"📊 ОСНОВНЫЕ МЕТРИКИ (последние 30 дней):")
        print(f"💰 Общие продажи: {stats['total_sales']:,.0f} IDR")
        print(f"📦 Заказов: {stats['total_orders']:,}")
        print(f"⭐ Рейтинг: {stats['avg_rating']:.2f}/5.0")
        print(f"🚚 Доставка: {stats['avg_delivery_time']:.1f} мин")
        print(f"🏆 Позиция на рынке: #{competitive.get('market_position', 'н/д')}")
        print(f"📊 Доля рынка: {competitive.get('market_share', 0):.1f}%")
        
        if insights:
            print(f"\n🔍 КЛЮЧЕВЫЕ ИНСАЙТЫ:")
            for insight in insights[:3]:
                print(f"• {insight}")
        
        if recommendations:
            print(f"\n💡 ТОП РЕКОМЕНДАЦИИ:")
            for i, rec in enumerate(recommendations[:3], 1):
                print(f"{i}. {rec}")
                
    except Exception as e:
        print(f"❌ Ошибка при анализе: {e}")

def validate_system():
    """Проверяет целостность системы"""
    print("🔧 ПРОВЕРКА СИСТЕМЫ")
    print("=" * 30)
    
    checks = []
    
    # Проверка базы данных
    try:
        conn = sqlite3.connect('data/database.sqlite')
        cursor = conn.cursor()
        
        # Проверяем таблицы
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [row[0] for row in cursor.fetchall()]
        
        required_tables = ['restaurants', 'restaurant_data']
        missing_tables = [t for t in required_tables if t not in tables]
        
        if missing_tables:
            checks.append(f"❌ Отсутствуют таблицы: {missing_tables}")
        else:
            checks.append("✅ Структура базы данных корректна")
        
        # Проверяем данные
        cursor.execute("SELECT COUNT(*) FROM restaurant_data")
        data_count = cursor.fetchone()[0]
        
        if data_count > 1000:
            checks.append(f"✅ База данных содержит {data_count:,} записей")
        else:
            checks.append(f"⚠️ Мало данных: только {data_count:,} записей")
        
        # Проверяем диапазон дат
        cursor.execute("SELECT MIN(date), MAX(date) FROM restaurant_data")
        date_range = cursor.fetchone()
        
        if date_range[0] and date_range[1]:
            start_date = datetime.strptime(date_range[0], '%Y-%m-%d')
            end_date = datetime.strptime(date_range[1], '%Y-%m-%d')
            days_total = (end_date - start_date).days + 1
            
            if days_total > 365:
                checks.append(f"✅ Хороший диапазон данных: {days_total} дней ({date_range[0]} → {date_range[1]})")
            else:
                checks.append(f"⚠️ Ограниченный диапазон: {days_total} дней")
        
        conn.close()
        
    except Exception as e:
        checks.append(f"❌ Ошибка базы данных: {e}")
    
    # Проверка модулей
    try:
        from main.advanced_analytics import AdvancedRestaurantAnalytics
        checks.append("✅ Модуль аналитики загружен")
    except Exception as e:
        checks.append(f"❌ Ошибка модуля аналитики: {e}")
    
    try:
        from main.report_generator import AdvancedReportGenerator
        checks.append("✅ Генератор отчетов загружен")
    except Exception as e:
        checks.append(f"❌ Ошибка генератора отчетов: {e}")
    
    # Выводим результаты
    for check in checks:
        print(check)
    
    # Общий статус
    errors = [c for c in checks if c.startswith('❌')]
    warnings = [c for c in checks if c.startswith('⚠️')]
    
    print(f"\n📊 ИТОГО:")
    print(f"✅ Успешно: {len(checks) - len(errors) - len(warnings)}")
    print(f"⚠️ Предупреждений: {len(warnings)}")
    print(f"❌ Ошибок: {len(errors)}")
    
    if errors:
        print(f"\n🚨 СИСТЕМА ТРЕБУЕТ ИСПРАВЛЕНИЙ")
        return False
    elif warnings:
        print(f"\n⚠️ СИСТЕМА РАБОТАЕТ С ОГРАНИЧЕНИЯМИ")
        return True
    else:
        print(f"\n🎉 СИСТЕМА ПОЛНОСТЬЮ ФУНКЦИОНАЛЬНА")
        return True

def test_system():
    """Тестирует основные функции системы"""
    print("🧪 ТЕСТИРОВАНИЕ СИСТЕМЫ")
    print("=" * 40)
    
    # Получаем первый ресторан для теста
    try:
        conn = sqlite3.connect('data/database.sqlite')
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM restaurants LIMIT 1")
        test_restaurant = cursor.fetchone()
        conn.close()
        
        if not test_restaurant:
            print("❌ Нет ресторанов для тестирования")
            return False
        
        test_restaurant = test_restaurant[0]
        print(f"🎯 Тестируем на ресторане: {test_restaurant}")
        
    except Exception as e:
        print(f"❌ Ошибка получения тестового ресторана: {e}")
        return False
    
    tests_passed = 0
    total_tests = 0
    
    # Тест 1: Быстрый анализ
    total_tests += 1
    print(f"\n🧪 Тест 1: Быстрый анализ...")
    try:
        quick_analysis(test_restaurant)
        print("✅ Быстрый анализ работает")
        tests_passed += 1
    except Exception as e:
        print(f"❌ Ошибка быстрого анализа: {e}")
    
    # Тест 2: Продвинутая аналитика
    total_tests += 1
    print(f"\n🧪 Тест 2: Продвинутая аналитика...")
    try:
        analysis = run_advanced_analysis(test_restaurant)
        if "error" not in analysis:
            print("✅ Продвинутая аналитика работает")
            tests_passed += 1
        else:
            print(f"❌ Ошибка аналитики: {analysis['error']}")
    except Exception as e:
        print(f"❌ Ошибка продвинутой аналитики: {e}")
    
    # Тест 3: Генерация отчета (краткий)
    total_tests += 1
    print(f"\n🧪 Тест 3: Генерация отчета...")
    try:
        # Ограничиваем период для быстрого теста
        end_date = datetime.now()
        start_date = end_date - timedelta(days=7)
        
        report = generate_restaurant_report(
            test_restaurant, 
            start_date.strftime('%Y-%m-%d'), 
            end_date.strftime('%Y-%m-%d')
        )
        
        if len(report) > 500:  # Проверяем что отчет содержательный
            print("✅ Генерация отчетов работает")
            tests_passed += 1
        else:
            print("❌ Отчет слишком короткий")
    except Exception as e:
        print(f"❌ Ошибка генерации отчета: {e}")
    
    # Результаты тестирования
    print(f"\n📊 РЕЗУЛЬТАТЫ ТЕСТИРОВАНИЯ:")
    print(f"✅ Пройдено: {tests_passed}/{total_tests}")
    print(f"❌ Ошибок: {total_tests - tests_passed}/{total_tests}")
    
    if tests_passed == total_tests:
        print(f"\n🎉 ВСЕ ТЕСТЫ ПРОШЛИ УСПЕШНО")
        return True
    else:
        print(f"\n⚠️ НЕКОТОРЫЕ ФУНКЦИИ ТРЕБУЮТ ИСПРАВЛЕНИЯ")
        return False

def update_weather_data(start_date: str = None, end_date: str = None):
    """Обновляет данные о погоде и праздниках в базе данных"""
    print("🌤️ ОБНОВЛЕНИЕ ДАННЫХ О ПОГОДЕ И ПРАЗДНИКАХ")
    print("=" * 60)
    
    try:
        from main.weather_calendar_api import WeatherCalendarAPI
        
        # Устанавливаем даты по умолчанию
        if not start_date:
            start_date = "2025-01-01"
        if not end_date:
            end_date = "2025-06-30"
        
        print(f"📅 Период обновления: {start_date} - {end_date}")
        
        # Создаем API клиент
        weather_api = WeatherCalendarAPI()
        
        # Проверяем наличие API ключей
        if not weather_api.weather_api_key and not weather_api.calendar_api_key:
            print("⚠️ API ключи не настроены - используются улучшенные симулированные данные")
            print("🔧 Для получения реальных данных добавьте:")
            print("   - WEATHER_API_KEY (OpenWeatherMap)")
            print("   - CALENDAR_API_KEY (Calendarific)")
        
        # Обновляем данные
        updated_count = weather_api.update_database_with_real_data(start_date, end_date)
        
        print(f"✅ Обновление завершено: {updated_count} записей")
        
        # Анализируем влияние погоды на примере
        print("\n🔍 АНАЛИЗ ВЛИЯНИЯ ПОГОДЫ (пример - Ika Canggu):")
        impact = weather_api.analyze_weather_impact("Ika Canggu")
        
        if impact:
            print(f"🌧️ Влияние дождя: {impact.get('rain_impact_percent', 0):.1f}%")
            print(f"🌡️ Влияние температуры: {impact.get('temperature_impact_percent', 0):.1f}%")
            print(f"☀️ Лучшая погода: {impact.get('best_weather', 'N/A')}")
            print(f"🌧️ Худшая погода: {impact.get('worst_weather', 'N/A')}")
        
        print("\n💡 Теперь отчеты будут содержать более точные причины аномалий!")
        
    except ImportError:
        print("❌ Модуль weather_calendar_api не найден")
    except Exception as e:
        print(f"❌ Ошибка при обновлении: {e}")
        import traceback
        traceback.print_exc()

def check_api_status():
    """Проверяет статус всех API ключей и их работоспособность"""
    print("🔑 ПРОВЕРКА СТАТУСА API КЛЮЧЕЙ")
    print("=" * 60)
    
    import os
    from dotenv import load_dotenv
    
    # Загружаем переменные из .env файла
    load_dotenv()
    
    # Проверяем переменные окружения
    apis = {
        'WEATHER_API_KEY': os.getenv('WEATHER_API_KEY'),
        'CALENDAR_API_KEY': os.getenv('CALENDAR_API_KEY'), 
        'OPENAI_API_KEY': os.getenv('OPENAI_API_KEY')
    }
    
    print("📋 СТАТУС ПЕРЕМЕННЫХ ОКРУЖЕНИЯ:")
    for api_name, api_key in apis.items():
        status = "✅ НАЙДЕН" if api_key else "❌ НЕ НАЙДЕН"
        masked_key = f"{api_key[:8]}...{api_key[-4:]}" if api_key else "None"
        print(f"  {api_name}: {status} ({masked_key})")
    
    print("\n🧪 ТЕСТИРОВАНИЕ API:")
    
    # Тест Weather API
    try:
        from main.weather_calendar_api import WeatherCalendarAPI
        weather_api = WeatherCalendarAPI()
        
        if weather_api.weather_api_key:
            print("🌤️  Weather API: Попытка получения данных...")
            weather = weather_api.get_historical_weather('2025-05-15')
            print(f"   ✅ Успешно: {weather['weather_condition']}, {weather['temperature_celsius']:.1f}°C")
        else:
            print("🌤️  Weather API: ❌ Ключ не настроен - используются симулированные данные")
            
        if weather_api.calendar_api_key:
            print("📅 Calendar API: Попытка получения праздников...")
            holidays = weather_api.get_holidays(2025)
            print(f"   ✅ Успешно: найдено {len(holidays)} праздников")
        else:
            print("📅 Calendar API: ❌ Ключ не настроен - используются базовые праздники")
            
    except Exception as e:
        print(f"❌ Ошибка проверки Weather/Calendar API: {e}")
    
    # Тест OpenAI API
    try:
        from main.openai_analytics import OpenAIAnalytics
        openai_api = OpenAIAnalytics()
        
        if openai_api.enabled:
            print("🤖 OpenAI API: Попытка генерации инсайтов...")
            test_data = {
                'total_sales': 1000000,
                'roas': 12.5,
                'avg_rating': 4.8,
                'avg_delivery_time': 28
            }
            insights = openai_api.generate_business_insights(test_data)
            print("   ✅ Успешно: AI анализ работает")
        else:
            print("🤖 OpenAI API: ❌ Ключ не настроен - используются стандартные инсайты")
            
    except Exception as e:
        print(f"❌ Ошибка проверки OpenAI API: {e}")
    
    print("\n💡 ИНСТРУКЦИИ ПО НАСТРОЙКЕ:")
    
    if not apis['WEATHER_API_KEY']:
        print("🌤️  Weather API (OpenWeatherMap):")
        print("   export WEATHER_API_KEY='your_openweathermap_key'")
        
    if not apis['CALENDAR_API_KEY']: 
        print("📅 Calendar API (Calendarific):")
        print("   export CALENDAR_API_KEY='your_calendarific_key'")
        
    if not apis['OPENAI_API_KEY']:
        print("🤖 OpenAI API:")
        print("   export OPENAI_API_KEY='your_openai_key'")
    
    print("\n📚 Подробная инструкция: см. WEATHER_API_SETUP.md")
    
    # Показываем текущие возможности
    enabled_apis = sum(1 for key in apis.values() if key)
    total_apis = len(apis)
    
    print(f"\n📊 ИТОГО: {enabled_apis}/{total_apis} API настроено")
    
    if enabled_apis == 0:
        print("⚠️  Система работает БЕЗ внешних API (используются улучшенные симуляции)")
    elif enabled_apis == total_apis:
        print("🎉 ВСЕ API НАСТРОЕНЫ - максимальная точность анализа!")
    else:
        print("🔧 Частичная настройка - добавьте остальные API для полной функциональности")

def main():
    """Главная функция CLI"""
    parser = argparse.ArgumentParser(
        description='🔬 Продвинутая система аналитики ресторанов с глубоким анализом 2.5 лет данных',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
🎯 НОВЫЕ УПРОЩЕННЫЕ КОМАНДЫ:
  python main.py restaurant "Ika Canggu"                 # 🏪 ПОЛНЫЙ анализ ресторана (ВСЁ В ОДНОМ!)
  python main.py market                                  # 🌍 ПОЛНЫЙ анализ всего рынка

📋 Остальные команды:
  python main.py list                                    # Список ресторанов
  python main.py report "Ika Canggu" --start 2024-01-01 # Старый формат отчета
  python main.py quick "Prana Restaurant"                # Быстрый анализ
  python main.py validate                                # Проверка системы
  python main.py check-apis                              # Статус API
        """
    )
    
    parser.add_argument('command', choices=['list', 'report', 'restaurant', 'quick', 'market', 'compare', 'intelligent', 'validate', 'test', 'update-weather', 'check-apis'],
                       help='Команда для выполнения')
    parser.add_argument('restaurant', nargs='?', help='Название ресторана')
    parser.add_argument('--start', help='Дата начала периода (YYYY-MM-DD)')
    parser.add_argument('--end', help='Дата окончания периода (YYYY-MM-DD)')
    parser.add_argument('--period1-start', help='Начало первого периода для сравнения (YYYY-MM-DD)')
    parser.add_argument('--period1-end', help='Конец первого периода для сравнения (YYYY-MM-DD)')
    parser.add_argument('--period2-start', help='Начало второго периода для сравнения (YYYY-MM-DD)')
    parser.add_argument('--period2-end', help='Конец второго периода для сравнения (YYYY-MM-DD)')
    
    if len(sys.argv) == 1:
        parser.print_help()
        return
    
    args = parser.parse_args()
    
    print("🔬 ПРОДВИНУТАЯ СИСТЕМА АНАЛИТИКИ РЕСТОРАНОВ")
    print(f"🕐 Время запуска: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)
    
    try:
        if args.command == 'list':
            list_restaurants()
            
        elif args.command == 'report':
            if not args.restaurant:
                print("❌ Укажите название ресторана для отчета")
                parser.print_help()
                return
            generate_full_report(args.restaurant, args.start, args.end)
            
        elif args.command == 'restaurant':
            if not args.restaurant:
                print("❌ Укажите название ресторана для полного анализа")
                parser.print_help()
                return
            generate_unified_restaurant_report(args.restaurant, args.start, args.end)
            
        elif args.command == 'quick':
            if not args.restaurant:
                print("❌ Укажите название ресторана для быстрого анализа")
                parser.print_help()
                return
            quick_analysis(args.restaurant)
            
        elif args.command == 'market':
            generate_market_overview()
            
        elif args.command == 'compare':
            # Сравнение двух периодов
            period1_start = getattr(args, 'period1_start', None)
            period1_end = getattr(args, 'period1_end', None)
            period2_start = getattr(args, 'period2_start', None)
            period2_end = getattr(args, 'period2_end', None)
            
            if not period1_start or not period1_end or not period2_start or not period2_end:
                print("❌ Для сравнения необходимо указать все даты:")
                print("   python3 main.py compare --period1-start YYYY-MM-DD --period1-end YYYY-MM-DD --period2-start YYYY-MM-DD --period2-end YYYY-MM-DD")
                return
            
            from main.period_comparison_analyzer import PeriodComparisonAnalyzer
            analyzer = PeriodComparisonAnalyzer()
            
            print("🔬 СИСТЕМА СРАВНИТЕЛЬНОГО АНАЛИЗА ПЕРИОДОВ")
            print("=" * 50)
            
            report = analyzer.compare_periods(
                period1_start, period1_end,
                period2_start, period2_end
            )
            
            print(report)
            
            # Сохраняем отчет
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"reports/period_comparison_{timestamp}.txt"
            
            try:
                import os
                os.makedirs('reports', exist_ok=True)
                
                with open(filename, 'w', encoding='utf-8') as f:
                    f.write(report)
                
                print(f"💾 Сравнительный анализ сохранен в файл: {filename}")
                
            except Exception as e:
                print(f"⚠️ Не удалось сохранить отчет в файл: {e}")
            
        elif args.command == 'intelligent':
            # Интеллектуальный анализ аномалий
            start_date = args.start or '2025-04-01'
            end_date = args.end or '2025-06-22'
            
            try:
                from main.intelligent_anomaly_detector import IntelligentAnomalyDetector
                
                detector = IntelligentAnomalyDetector()
                
                print("🧠 ИНТЕЛЛЕКТУАЛЬНАЯ СИСТЕМА ПОИСКА АНОМАЛИЙ")
                print("=" * 60)
                print("🎯 Система автоматически находит ВСЁ интересное без указания конкретных метрик!")
                print()
                
                # Запускаем полный интеллектуальный анализ
                findings = detector.analyze_everything(start_date, end_date)
                
                # Генерируем полный отчет
                report = detector.generate_intelligent_report(findings)
                print(report)
                
                # Сохраняем отчет
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                filename = f"reports/intelligent_analysis_{timestamp}.txt"
                
                try:
                    import os
                    os.makedirs('reports', exist_ok=True)
                    
                    with open(filename, 'w', encoding='utf-8') as f:
                        f.write(report)
                    
                    print(f"\n💾 Интеллектуальный анализ сохранен в файл: {filename}")
                    
                except Exception as e:
                    print(f"⚠️ Не удалось сохранить отчет в файл: {e}")
                    
            except ImportError:
                print("❌ Для интеллектуального анализа требуются дополнительные библиотеки:")
                print("   pip install scikit-learn scipy")
            except Exception as e:
                print(f"❌ Ошибка интеллектуального анализа: {e}")
            
        elif args.command == 'validate':
            validate_system()
            
        elif args.command == 'test':
            test_system()
            
        elif args.command == 'update-weather':
            update_weather_data(args.start, args.end)
            
        elif args.command == 'check-apis':
            check_api_status()
            
    except KeyboardInterrupt:
        print("\n\n⚠️ Операция прервана пользователем")
    except Exception as e:
        print(f"\n❌ Неожиданная ошибка: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()