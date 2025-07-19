"""
Главный модуль для запуска анализа причин изменения продаж
"""
import argparse
import logging
import sys
from datetime import datetime, timedelta
import json
import os

from model import train_sales_model, load_trained_model
from business_intelligence_system import (
    BusinessIntelligenceSystem,
    analyze_restaurant_performance,
    get_weekly_report,
    get_executive_summary,
    test_business_hypothesis
)
from data_loader import load_data_for_training, get_restaurants_list
from utils import setup_logging, validate_date, format_currency
from config import MODEL_PATH, RESULTS_PATH

# Настройка логирования
setup_logging()
logger = logging.getLogger(__name__)

def train_model_command(args):
    """Команда для обучения модели"""
    logger.info("=== Обучение модели ===")
    
    try:
        predictor = train_sales_model(
            start_date=args.start_date,
            end_date=args.end_date,
            model_type=args.model_type,
            optimize_hyperparams=args.optimize,
            save_model=True
        )
        
        if predictor:
            logger.info("Модель успешно обучена и сохранена")
            print(f"R² score: {predictor.training_metrics.get('test_r2', 'N/A'):.4f}")
            print(f"Количество признаков: {predictor.training_metrics.get('feature_count', 'N/A')}")
            return True
        else:
            logger.error("Ошибка обучения модели")
            return False
    except Exception as e:
        logger.error(f"Ошибка обучения модели: {e}")
        return False

def analyze_command(args):
    """Команда для анализа конкретного случая"""
    logger.info(f"=== Анализ изменения продаж ===")
    logger.info(f"Ресторан: {args.restaurant}")
    logger.info(f"Дата: {args.date}")
    
    # Проверяем валидность даты
    if not validate_date(args.date):
        logger.error("Неверный формат даты. Используйте YYYY-MM-DD")
        return False
    
    try:
        # Выполняем анализ через новую бизнес-систему
        result = analyze_restaurant_performance(args.restaurant, args.date)
        
        if "error" in result:
            logger.error(f"Ошибка анализа: {result['error']}")
            return False
        
        # Выводим результаты
        print("\n" + "="*80)
        print("🎯 АНАЛИЗ ПРОДАЖ РЕСТОРАНА")
        print("="*80)
        print(f"📍 Ресторан: {result['restaurant_name']}")
        print(f"📅 Дата анализа: {result['analysis_date']}")
        print(f"📊 Период: {result['period_analyzed']}")
        
        # Основные метрики
        summary = result['summary']
        print(f"\n📈 ОСНОВНЫЕ ПОКАЗАТЕЛИ:")
        print(f"  • Изменение продаж: {summary['sales_change_percent']:+.1f}%")
        print(f"  • Тренд: {summary['sales_trend']}")
        print(f"  • Текущие продажи: {format_currency(summary['latest_period_sales'])}")
        print(f"  • Предыдущие продажи: {format_currency(summary['earlier_period_sales'])}")
        print(f"  • Абсолютное изменение: {format_currency(summary['absolute_change'])}")
        
        # Ключевые факторы
        if result['key_factors']:
            print(f"\n🔍 КЛЮЧЕВЫЕ ФАКТОРЫ ВЛИЯНИЯ:")
            for i, factor in enumerate(result['key_factors'], 1):
                impact_emoji = "📈" if factor['impact'] == "положительный" else "📉"
                print(f"  {i}. {impact_emoji} {factor['factor']}: {factor['change']}")
                print(f"     Влияние: {factor['impact']} (уверенность: {factor['confidence']})")
        
        # Рекомендации
        if result['recommendations']:
            print(f"\n💡 РЕКОМЕНДАЦИИ К ДЕЙСТВИЮ:")
            for i, rec in enumerate(result['recommendations'], 1):
                priority_emoji = "🔴" if rec['priority'] == "ВЫСОКИЙ" else "🟡" if rec['priority'] == "СРЕДНИЙ" else "🟢"
                print(f"  {i}. {priority_emoji} {rec['category']}: {rec['action']}")
                print(f"     {rec['description']}")
                print(f"     Ожидаемый эффект: {rec.get('expected_impact', 'Не указан')}")
                print(f"     Как выполнить: {rec.get('implementation', 'Не указано')}")
                print()
        
        # Сохраняем результаты
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        result_filename = f"{args.restaurant}_{args.date}_{timestamp}.json"
        result_path = os.path.join(RESULTS_PATH, result_filename)
        
        os.makedirs(RESULTS_PATH, exist_ok=True)
        with open(result_path, 'w', encoding='utf-8') as f:
            json.dump(result, f, ensure_ascii=False, indent=2)
        
        print(f"📄 Детальные результаты сохранены в: {result_path}")
        return True
        
    except Exception as e:
        logger.error(f"Неожиданная ошибка: {e}")
        return False

def weekly_report_command(args):
    """Команда для генерации недельного отчета"""
    logger.info(f"=== Недельный отчет ===")
    logger.info(f"Ресторан: {args.restaurant}")
    logger.info(f"Недель назад: {args.weeks}")
    
    try:
        # Генерируем недельный отчет
        result = get_weekly_report(args.restaurant, args.weeks)
        
        if "error" in result:
            logger.error(f"Ошибка генерации отчета: {result['error']}")
            return False
        
        # Выводим результаты
        print("\n" + "="*80)
        print("📊 НЕДЕЛЬНЫЙ ОТЧЕТ")
        print("="*80)
        print(f"📍 Ресторан: {result['restaurant_name']}")
        print(f"📅 Период: {result['period']}")
        print(f"📈 Недель проанализировано: {result['weeks_analyzed']}")
        
        # Сводка
        summary = result['summary']
        print(f"\n💰 ФИНАНСОВЫЕ ПОКАЗАТЕЛИ:")
        print(f"  • Общие продажи: {format_currency(summary['total_sales'])}")
        print(f"  • Средние недельные продажи: {format_currency(summary['average_weekly_sales'])}")
        print(f"  • Общее количество заказов: {summary['total_orders']}")
        print(f"  • Средний рейтинг: {summary['average_rating']:.2f}")
        print(f"  • Средний процент отмен: {summary['average_cancel_rate']:.1%}")
        
        # Тренды
        trends = result['trends']
        print(f"\n📈 ТРЕНДЫ:")
        print(f"  • Тренд продаж: {trends['trend_direction']} ({trends['sales_trend_percent']:+.1f}%)")
        print(f"  • Стабильность: {trends['stability']} (волатильность: {trends['volatility_percent']:.1f}%)")
        
        # Рекомендации
        if result['recommendations']:
            print(f"\n💡 РЕКОМЕНДАЦИИ:")
            for i, rec in enumerate(result['recommendations'], 1):
                priority_emoji = "🔴" if rec['priority'] == "ВЫСОКИЙ" else "🟡" if rec['priority'] == "СРЕДНИЙ" else "🟢"
                print(f"  {i}. {priority_emoji} {rec['category']}: {rec['action']}")
                print(f"     {rec['description']}")
                print(f"     Как выполнить: {rec.get('implementation', 'Не указано')}")
                print()
        
        # Сохраняем результаты
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        result_filename = f"{args.restaurant}_weekly_{timestamp}.json"
        result_path = os.path.join(RESULTS_PATH, result_filename)
        
        os.makedirs(RESULTS_PATH, exist_ok=True)
        with open(result_path, 'w', encoding='utf-8') as f:
            json.dump(result, f, ensure_ascii=False, indent=2)
        
        print(f"📄 Детальный отчет сохранен в: {result_path}")
        return True
        
    except Exception as e:
        logger.error(f"Ошибка генерации недельного отчета: {e}")
        return False

def executive_summary_command(args):
    """Команда для генерации краткого отчета для руководства"""
    logger.info(f"=== Краткий отчет для руководства ===")
    logger.info(f"Ресторан: {args.restaurant}")
    
    try:
        # Генерируем краткий отчет
        result = get_executive_summary(args.restaurant)
        
        if "error" in result:
            logger.error(f"Ошибка генерации отчета: {result['error']}")
            return False
        
        # Выводим результаты
        print("\n" + "="*80)
        print("🎯 КРАТКИЙ ОТЧЕТ ДЛЯ РУКОВОДСТВА")
        print("="*80)
        print(f"📍 Ресторан: {result['restaurant_name']}")
        print(f"📅 Период: {result['period']}")
        
        # Общий статус
        print(f"\n🎯 ОБЩИЙ СТАТУС:")
        print(f"  {result['overall_status']}")
        
        # Ключевые метрики
        metrics = result['key_metrics']
        print(f"\n📊 КЛЮЧЕВЫЕ МЕТРИКИ:")
        print(f"  • Изменение продаж: {metrics['sales_change_percent']:+.1f}%")
        print(f"  • Тренд: {metrics['sales_trend']}")
        print(f"  • Текущие продажи: {format_currency(metrics['latest_period_sales'])}")
        
        # Топ-3 фактора
        if result['top_3_factors']:
            print(f"\n🔍 ТОП-3 ФАКТОРА ВЛИЯНИЯ:")
            for i, factor in enumerate(result['top_3_factors'], 1):
                impact_emoji = "📈" if factor['impact'] == "положительный" else "📉"
                print(f"  {i}. {impact_emoji} {factor['factor']}: {factor['change']}")
        
        # Приоритетные действия
        if result['priority_actions']:
            print(f"\n🚨 ПРИОРИТЕТНЫЕ ДЕЙСТВИЯ:")
            for i, action in enumerate(result['priority_actions'], 1):
                print(f"  {i}. {action['category']}: {action['action']}")
                print(f"     {action['description']}")
                print()
        
        return True
        
    except Exception as e:
        logger.error(f"Ошибка генерации краткого отчета: {e}")
        return False

def test_hypothesis_command(args):
    """Команда для тестирования гипотез"""
    logger.info(f"=== Тестирование гипотезы ===")
    logger.info(f"Ресторан: {args.restaurant}")
    logger.info(f"Гипотеза: {args.hypothesis}")
    
    try:
        # Тестируем гипотезу
        result = test_business_hypothesis(args.restaurant, args.hypothesis, args.days)
        
        if "error" in result:
            logger.error(f"Ошибка тестирования гипотезы: {result['error']}")
            return False
        
        # Выводим результаты
        print("\n" + "="*80)
        print("🧪 ТЕСТИРОВАНИЕ ГИПОТЕЗЫ")
        print("="*80)
        print(f"📍 Ресторан: {result['restaurant_name']}")
        print(f"🔬 Гипотеза: {result['hypothesis']}")
        print(f"📅 Период: {result['period']}")
        
        # Результат
        test_result = result['result']
        print(f"\n📊 РЕЗУЛЬТАТ:")
        print(f"  • Заключение: {test_result['conclusion']}")
        
        if 'improvement_percent' in test_result:
            print(f"  • Улучшение: {test_result['improvement_percent']:+.1f}%")
        
        if 'confidence' in test_result:
            print(f"  • Уверенность: {test_result['confidence']}")
        
        return True
        
    except Exception as e:
        logger.error(f"Ошибка тестирования гипотезы: {e}")
        return False

def deep_analysis_command(args):
    """Глубокий анализ с поиском аномалий и корреляций"""
    logger.info("=== Глубокий анализ ===")
    
    try:
        from business_intelligence_system import generate_deep_analytics_report
        
        report = generate_deep_analytics_report(args.restaurant, args.start_date, args.end_date)
        
        if 'error' not in report:
            print(f"\n🔍 ГЛУБОКИЙ АНАЛИЗ: {report['restaurant_name']}")
            print(f"📅 Период: {report['period']}")
            print("=" * 60)
            
            # Базовая статистика
            stats = report['base_statistics']
            print(f"\n📊 БАЗОВАЯ СТАТИСТИКА:")
            print(f"  • Общие продажи: {stats['total_sales']:,.0f} IDR")
            print(f"  • Средние продажи в день: {stats['avg_daily_sales']:,.0f} IDR")
            print(f"  • Общие заказы: {stats['total_orders']:,}")
            print(f"  • Средний рейтинг: {stats['avg_rating']:.2f}")
            print(f"  • Дней проанализировано: {stats['days_analyzed']}")
            
            # Аномалии
            if report['anomalies']:
                print(f"\n🚨 АНОМАЛИИ И ОТКЛОНЕНИЯ (топ-5):")
                for i, anomaly in enumerate(report['anomalies'][:5], 1):
                    print(f"  {i}. {anomaly['date']} - Отклонение: {anomaly['deviation']}")
                    print(f"     Продажи: {anomaly['sales']:,.0f} IDR")
                    if anomaly['possible_causes']:
                        print(f"     Возможные причины:")
                        for cause in anomaly['possible_causes']:
                            print(f"       • {cause}")
                    print()
            
            # Корреляции
            correlations = report['correlations']
            print(f"\n🔗 СИЛЬНЫЕ КОРРЕЛЯЦИИ:")
            
            if correlations['strong_positive']:
                print(f"  📈 ПОЛОЖИТЕЛЬНЫЕ СВЯЗИ:")
                for corr in correlations['strong_positive']:
                    print(f"    • {corr['interpretation']} (r={corr['correlation']:.3f})")
            
            if correlations['strong_negative']:
                print(f"  📉 ОБРАТНЫЕ СВЯЗИ:")
                for corr in correlations['strong_negative']:
                    print(f"    • {corr['interpretation']} (r={corr['correlation']:.3f})")
            
            # Паттерны
            if correlations['interesting_patterns']:
                print(f"\n🎯 ИНТЕРЕСНЫЕ ПАТТЕРНЫ:")
                for pattern in correlations['interesting_patterns']:
                    print(f"  • {pattern['description']}")
            
            # Тренды
            trends = report['trends']
            if trends:
                print(f"\n📈 ТРЕНДЫ И ИЗМЕНЕНИЯ:")
                if 'monthly' in trends:
                    monthly = trends['monthly']
                    print(f"  • Лучший месяц: {monthly['best_month']} ({monthly['best_sales']:,.0f} IDR)")
                    print(f"  • Худший месяц: {monthly['worst_month']} ({monthly['worst_sales']:,.0f} IDR)")
                
                if 'roas_trend' in trends:
                    roas_trend = trends['roas_trend']
                    print(f"  • {roas_trend['interpretation']}")
            
            print(f"\n🎉 ИТОГО НАЙДЕНО ИНСАЙТОВ: {report['insights_count']}")
            print(f"📅 Отчет сгенерирован: {report['generated_at']}")
            
            return True
        else:
            logger.error(f"Ошибка глубокого анализа: {report['error']}")
            return False
            
    except Exception as e:
        logger.error(f"Ошибка выполнения глубокого анализа: {e}")
        return False

def info_command(args):
    """Команда для получения информации о модели"""
    logger.info("=== Информация о модели ===")
    
    try:
        if not os.path.exists(MODEL_PATH):
            print("❌ Модель не найдена")
            print(f"Путь: {MODEL_PATH}")
            print("Для обучения модели выполните: python main.py train")
            return False
        
        # Загружаем модель
        predictor = load_trained_model()
        
        if predictor is None:
            print("❌ Не удалось загрузить модель")
            return False
        
        print("\n" + "="*50)
        print("🤖 ИНФОРМАЦИЯ О МОДЕЛИ")
        print("="*50)
        print(f"📊 Тип модели: {predictor.model_type}")
        print(f"📈 R² score: {predictor.training_metrics.get('test_r2', 'N/A'):.4f}")
        print(f"🎯 Количество признаков: {predictor.training_metrics.get('feature_count', 'N/A')}")
        print(f"📅 Дата обучения: {predictor.training_metrics.get('training_date', 'N/A')}")
        print(f"💾 Размер файла: {os.path.getsize(MODEL_PATH) / 1024 / 1024:.1f} MB")
        
        return True
        
    except Exception as e:
        logger.error(f"Ошибка получения информации о модели: {e}")
        return False

def list_restaurants_command(args):
    """Показать список всех ресторанов"""
    logger.info("=== Список ресторанов ===")
    
    try:
        restaurants = get_restaurants_list()
        
        if not restaurants:
            logger.error("Нет доступных ресторанов")
            return False
        
        print(f"\nНайдено {len(restaurants)} ресторанов:")
        for restaurant in restaurants:
            print(f"  • {restaurant}")
        
        return True
        
    except Exception as e:
        logger.error(f"Ошибка получения списка ресторанов: {e}")
        return False

def main():
    """Главная функция"""
    parser = argparse.ArgumentParser(
        description="Система бизнес-аналитики для ресторанов",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Примеры использования:

  # Анализ конкретного случая
  python main.py analyze --restaurant "Canggu Surf Cafe" --date "2023-06-15"

  # Недельный отчет
  python main.py weekly --restaurant "Canggu Surf Cafe" --weeks 4

  # Краткий отчет для руководства
  python main.py summary --restaurant "Canggu Surf Cafe"

  # Глубокий анализ с аномалиями и корреляциями
  python main.py deep --restaurant "Ika Canggu" --start-date "2024-04-01" --end-date "2024-06-30"

  # Тестирование гипотезы
  python main.py test --restaurant "Canggu Surf Cafe" --hypothesis "реклама эффективна"

  # Обучение модели
  python main.py train --model-type random_forest

  # Информация о модели
  python main.py info

  # Список ресторанов
  python main.py list
        """
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Доступные команды')
    
    # Команда анализа
    analyze_parser = subparsers.add_parser('analyze', help='Анализ конкретного случая')
    analyze_parser.add_argument('--restaurant', required=True, help='Название ресторана')
    analyze_parser.add_argument('--date', required=True, help='Дата анализа (YYYY-MM-DD)')
    
    # Команда недельного отчета
    weekly_parser = subparsers.add_parser('weekly', help='Недельный отчет')
    weekly_parser.add_argument('--restaurant', required=True, help='Название ресторана')
    weekly_parser.add_argument('--weeks', type=int, default=4, help='Количество недель назад')
    
    # Команда краткого отчета
    summary_parser = subparsers.add_parser('summary', help='Краткий отчет для руководства')
    summary_parser.add_argument('--restaurant', required=True, help='Название ресторана')
    
    # Команда тестирования гипотез
    test_parser = subparsers.add_parser('test', help='Тестирование гипотез')
    test_parser.add_argument('--restaurant', required=True, help='Название ресторана')
    test_parser.add_argument('--hypothesis', required=True, help='Гипотеза для тестирования')
    test_parser.add_argument('--days', type=int, default=30, help='Количество дней для анализа')
    
    # Команда глубокого анализа
    deep_parser = subparsers.add_parser('deep', help='Глубокий анализ с аномалиями и корреляциями')
    deep_parser.add_argument('--restaurant', required=True, help='Название ресторана')
    deep_parser.add_argument('--start-date', required=True, help='Начальная дата (YYYY-MM-DD)')
    deep_parser.add_argument('--end-date', required=True, help='Конечная дата (YYYY-MM-DD)')
    
    # Команда обучения модели
    train_parser = subparsers.add_parser('train', help='Обучение модели')
    train_parser.add_argument('--model-type', choices=['random_forest', 'xgboost', 'linear'], 
                             default='random_forest', help='Тип модели')
    train_parser.add_argument('--start-date', help='Начальная дата (YYYY-MM-DD)')
    train_parser.add_argument('--end-date', help='Конечная дата (YYYY-MM-DD)')
    train_parser.add_argument('--optimize', action='store_true', help='Оптимизация гиперпараметров')
    
    # Команда информации о модели
    info_parser = subparsers.add_parser('info', help='Информация о модели')
    
    # Команда списка ресторанов
    list_parser = subparsers.add_parser('list', help='Список ресторанов')
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return
    
    # Выполняем команду
    success = False
    
    if args.command == 'analyze':
        success = analyze_command(args)
    elif args.command == 'weekly':
        success = weekly_report_command(args)
    elif args.command == 'summary':
        success = executive_summary_command(args)
    elif args.command == 'test':
        success = test_hypothesis_command(args)
    elif args.command == 'deep':
        success = deep_analysis_command(args)
    elif args.command == 'train':
        success = train_model_command(args)
    elif args.command == 'info':
        success = info_command(args)
    elif args.command == 'list':
        success = list_restaurants_command(args)
    
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()