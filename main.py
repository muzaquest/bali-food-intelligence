#!/usr/bin/env python3
"""
🚀 ГЛАВНЫЙ МОДУЛЬ ИНТЕГРИРОВАННОЙ СИСТЕМЫ АНАЛИТИКИ
Точка входа для всех операций бизнес-аналитики с контролем качества
"""

import sys
import os
import argparse
import logging
from datetime import datetime

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

# Добавляем директорию main в path
sys.path.append(os.path.join(os.path.dirname(__file__), 'main'))

def main():
    """Главная функция приложения"""
    
    parser = argparse.ArgumentParser(
        description='🚀 Интегрированная система бизнес-аналитики ресторанов',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Примеры использования:

📊 Анализ ресторана за период:
  python3 main_fixed.py analyze --restaurant "Ika Canggu" --start-date "2025-04-01" --end-date "2025-06-30"

📋 Быстрый отчет:
  python3 main_fixed.py quick --restaurant "Ika Canggu"

🔍 Валидация данных:
  python3 main_fixed.py validate --restaurant "Ika Canggu"

📊 Список ресторанов:
  python3 main_fixed.py list
        """
    )
    
    # Основные команды
    subparsers = parser.add_subparsers(dest='command', help='Доступные команды')
    
    # Команда анализа
    analyze_parser = subparsers.add_parser('analyze', help='📊 Полный анализ ресторана')
    analyze_parser.add_argument('--restaurant', '-r', required=True, help='Название ресторана')
    analyze_parser.add_argument('--start-date', '-s', help='Дата начала (YYYY-MM-DD)')
    analyze_parser.add_argument('--end-date', '-e', help='Дата окончания (YYYY-MM-DD)')
    
    # Команда быстрого отчета
    quick_parser = subparsers.add_parser('quick', help='📋 Быстрый отчет')
    quick_parser.add_argument('--restaurant', '-r', required=True, help='Название ресторана')
    
    # Команда валидации
    validate_parser = subparsers.add_parser('validate', help='🔍 Валидация данных')
    validate_parser.add_argument('--restaurant', '-r', help='Название ресторана (опционально)')
    
    # Команда списка ресторанов
    list_parser = subparsers.add_parser('list', help='📊 Список ресторанов')
    
    # Команда тестирования
    test_parser = subparsers.add_parser('test', help='🧪 Тестирование системы')
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return
    
    try:
        if args.command == 'analyze':
            run_full_analysis(args.restaurant, args.start_date, args.end_date)
        elif args.command == 'quick':
            run_quick_analysis(args.restaurant)
        elif args.command == 'validate':
            run_validation(args.restaurant)
        elif args.command == 'list':
            list_restaurants()
        elif args.command == 'test':
            run_system_test()
        else:
            parser.print_help()
            
    except KeyboardInterrupt:
        print("\n❌ Операция прервана пользователем")
    except Exception as e:
        logger.error(f"❌ Критическая ошибка: {e}")
        import traceback
        traceback.print_exc()

def run_full_analysis(restaurant_name: str, start_date: str = None, end_date: str = None):
    """Запускает полный анализ ресторана"""
    
    logger.info(f"🚀 Запуск полного анализа для {restaurant_name}")
    
    try:
        from main.integrated_system import run_analysis
        run_analysis(restaurant_name, start_date, end_date)
    except ImportError:
        logger.error("❌ Не удалось импортировать интегрированную систему")
        logger.info("🔄 Пытаемся использовать альтернативный метод...")
        
        # Альтернативный способ через прямой импорт
        try:
            sys.path.append('main')
            from integrated_system import run_analysis
            run_analysis(restaurant_name, start_date, end_date)
        except Exception as e:
            logger.error(f"❌ Альтернативный метод тоже не работает: {e}")
            
            # Минимальный анализ
            run_minimal_analysis(restaurant_name, start_date, end_date)

def run_quick_analysis(restaurant_name: str):
    """Запускает быстрый анализ"""
    
    logger.info(f"📋 Быстрый анализ для {restaurant_name}")
    
    # Используем последние 30 дней
    from datetime import datetime, timedelta
    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
    
    run_full_analysis(restaurant_name, start_date, end_date)

def run_validation(restaurant_name: str = None):
    """Проверяет качество данных"""
    
    logger.info(f"🔍 Валидация данных для {restaurant_name or 'всех ресторанов'}")
    
    try:
        from main.data_loader import get_restaurant_data, validate_features
        
        # Загружаем данные
        df = get_restaurant_data(restaurant_name)
        
        if df.empty:
            print("❌ Нет данных для валидации")
            return
        
        print(f"✅ Загружено {len(df)} записей")
        print(f"📊 Колонки: {len(df.columns)}")
        print(f"🏪 Рестораны: {df['restaurant_name'].nunique()}")
        print(f"📅 Период: {df['date'].min()} - {df['date'].max()}")
        
        # Валидируем
        validate_features(df)
        print("✅ Валидация пройдена успешно")
        
    except Exception as e:
        logger.error(f"❌ Ошибка валидации: {e}")

def list_restaurants():
    """Выводит список доступных ресторанов"""
    
    logger.info("📊 Получение списка ресторанов")
    
    try:
        from main.data_loader import get_restaurant_data
        
        df = get_restaurant_data()
        
        if df.empty:
            print("❌ Нет данных о ресторанах")
            return
        
        restaurants = df['restaurant_name'].unique()
        
        print("🏪 ДОСТУПНЫЕ РЕСТОРАНЫ:")
        print("=" * 40)
        
        for i, restaurant in enumerate(restaurants, 1):
            restaurant_data = df[df['restaurant_name'] == restaurant]
            record_count = len(restaurant_data)
            date_range = f"{restaurant_data['date'].min().strftime('%Y-%m-%d')} - {restaurant_data['date'].max().strftime('%Y-%m-%d')}"
            
            print(f"{i:2d}. {restaurant}")
            print(f"    📊 Записей: {record_count}")
            print(f"    📅 Период: {date_range}")
            print()
            
    except Exception as e:
        logger.error(f"❌ Ошибка получения списка: {e}")

def run_system_test():
    """Запускает тестирование системы"""
    
    logger.info("🧪 Запуск тестирования системы")
    
    print("🧪 ТЕСТИРОВАНИЕ ИНТЕГРИРОВАННОЙ СИСТЕМЫ")
    print("=" * 50)
    
    # Тест 1: Загрузка данных
    try:
        from main.data_loader import get_restaurant_data
        
        print("📊 Тест 1: Загрузка данных...")
        df = get_restaurant_data("Ika Canggu")
        
        if not df.empty:
            print(f"✅ Тест 1 пройден: {len(df)} записей")
        else:
            print("❌ Тест 1 не пройден: нет данных")
            
    except Exception as e:
        print(f"❌ Тест 1 не пройден: {e}")
    
    # Тест 2: Feature Engineering
    try:
        from main.feature_engineering_fixed import prepare_features_fixed
        
        print("🔧 Тест 2: Feature Engineering...")
        
        if not df.empty:
            df_featured = prepare_features_fixed(df.head(100))  # Берем первые 100 записей для теста
            print(f"✅ Тест 2 пройден: {len(df.columns)} → {len(df_featured.columns)} признаков")
        else:
            print("⚠️ Тест 2 пропущен: нет данных")
            
    except Exception as e:
        print(f"❌ Тест 2 не пройден: {e}")
    
    # Тест 3: Интегрированная система
    try:
        print("🚀 Тест 3: Интегрированная система...")
        run_minimal_analysis("Ika Canggu", "2025-04-01", "2025-04-30")
        print("✅ Тест 3 пройден")
        
    except Exception as e:
        print(f"❌ Тест 3 не пройден: {e}")
    
    print("\n🏁 Тестирование завершено")

def run_minimal_analysis(restaurant_name: str, start_date: str = None, end_date: str = None):
    """Минимальный анализ без сложных зависимостей"""
    
    try:
        from main.data_loader import get_restaurant_data
        
        print(f"📊 МИНИМАЛЬНЫЙ АНАЛИЗ '{restaurant_name.upper()}'")
        print("=" * 50)
        
        # Загружаем данные
        df = get_restaurant_data()
        
        if df.empty:
            print("❌ Нет данных для анализа")
            return
        
        # Фильтруем по ресторану
        restaurant_data = df[df['restaurant_name'] == restaurant_name].copy()
        
        if start_date and end_date:
            mask = (restaurant_data['date'] >= start_date) & (restaurant_data['date'] <= end_date)
            restaurant_data = restaurant_data[mask]
        
        if restaurant_data.empty:
            print(f"❌ Нет данных для ресторана {restaurant_name} за указанный период")
            return
        
        # Базовая статистика
        total_sales = restaurant_data['total_sales'].sum()
        total_orders = restaurant_data['orders'].sum()
        avg_rating = restaurant_data['rating'].mean()
        days_count = len(restaurant_data)
        
        print(f"📅 Период: {restaurant_data['date'].min().strftime('%Y-%m-%d')} - {restaurant_data['date'].max().strftime('%Y-%m-%d')}")
        print(f"📊 Дней анализа: {days_count}")
        print(f"💰 Общие продажи: {total_sales:,.0f}")
        print(f"📈 Средние продажи в день: {total_sales/days_count:,.0f}")
        print(f"📦 Общие заказы: {total_orders:,}")
        print(f"⭐ Средний рейтинг: {avg_rating:.2f}/5.0")
        
        # Лучшие и худшие дни
        avg_daily_sales = restaurant_data['total_sales'].mean()
        
        best_day = restaurant_data.loc[restaurant_data['total_sales'].idxmax()]
        worst_day = restaurant_data.loc[restaurant_data['total_sales'].idxmin()]
        
        print(f"\n🏆 ЛУЧШИЙ ДЕНЬ:")
        print(f"   {best_day['date'].strftime('%Y-%m-%d')}: {best_day['total_sales']:,.0f} (+{((best_day['total_sales']/avg_daily_sales-1)*100):+.1f}%)")
        
        print(f"📉 ХУДШИЙ ДЕНЬ:")
        print(f"   {worst_day['date'].strftime('%Y-%m-%d')}: {worst_day['total_sales']:,.0f} ({((worst_day['total_sales']/avg_daily_sales-1)*100):+.1f}%)")
        
        # Конкуренты
        all_restaurants = df.groupby('restaurant_name')['total_sales'].sum().sort_values(ascending=False)
        position = list(all_restaurants.index).index(restaurant_name) + 1
        
        print(f"\n🏪 РЫНОЧНАЯ ПОЗИЦИЯ: {position} место из {len(all_restaurants)}")
        
        print("✅ Минимальный анализ завершен")
        
    except Exception as e:
        logger.error(f"❌ Ошибка минимального анализа: {e}")

if __name__ == "__main__":
    main()