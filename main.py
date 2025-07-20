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
        conn = sqlite3.connect('database.sqlite')
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

def generate_full_report(restaurant_name: str, period_start: str = None, period_end: str = None):
    """Генерирует полный отчет для ресторана"""
    print(f"🔬 ГЕНЕРАЦИЯ ГЛУБОКОГО АНАЛИЗА ДЛЯ: {restaurant_name.upper()}")
    print("=" * 80)
    
    try:
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
    """Генерирует обзор рынка"""
    print("📊 ГЕНЕРАЦИЯ ОБЗОРА РЫНКА РЕСТОРАНОВ")
    print("=" * 50)
    
    try:
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

def main():
    """Главная функция CLI"""
    parser = argparse.ArgumentParser(
        description='🔬 Продвинутая система аналитики ресторанов с глубоким анализом 2.5 лет данных',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Примеры использования:
  python main.py list                                    # Список ресторанов
  python main.py report "Ika Canggu"                     # Полный отчет
  python main.py report "Ika Canggu" --start 2024-01-01 --end 2024-06-30
  python main.py quick "Prana Restaurant"                # Быстрый анализ
  python main.py market                                  # Обзор рынка
  python main.py validate                                # Проверка системы
  python main.py test                                    # Тестирование
        """
    )
    
    parser.add_argument('command', choices=['list', 'report', 'quick', 'market', 'validate', 'test'],
                       help='Команда для выполнения')
    parser.add_argument('restaurant', nargs='?', help='Название ресторана')
    parser.add_argument('--start', help='Дата начала периода (YYYY-MM-DD)')
    parser.add_argument('--end', help='Дата окончания периода (YYYY-MM-DD)')
    
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
            
        elif args.command == 'quick':
            if not args.restaurant:
                print("❌ Укажите название ресторана для быстрого анализа")
                parser.print_help()
                return
            quick_analysis(args.restaurant)
            
        elif args.command == 'market':
            generate_market_overview()
            
        elif args.command == 'validate':
            validate_system()
            
        elif args.command == 'test':
            test_system()
            
    except KeyboardInterrupt:
        print("\n\n⚠️ Операция прервана пользователем")
    except Exception as e:
        print(f"\n❌ Неожиданная ошибка: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()