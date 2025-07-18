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
from explain import explain_sales_change, SalesExplainer
from data_loader import get_restaurant_data, load_data_for_training
from utils import setup_logging, validate_date, format_currency
from config import MODEL_PATH, RESULTS_PATH

# Настройка логирования
setup_logging()
logger = logging.getLogger(__name__)

def train_model_command(args):
    """Команда для обучения модели"""
    logger.info("=== Обучение модели ===")
    
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

def analyze_command(args):
    """Команда для анализа конкретного случая"""
    logger.info(f"=== Анализ изменения продаж ===")
    logger.info(f"Ресторан: {args.restaurant}")
    logger.info(f"Дата: {args.date}")
    
    # Проверяем валидность даты
    if not validate_date(args.date):
        logger.error("Неверный формат даты. Используйте YYYY-MM-DD")
        return False
    
    # Проверяем существование модели
    if not os.path.exists(MODEL_PATH):
        logger.error(f"Модель не найдена: {MODEL_PATH}")
        logger.info("Сначала обучите модель: python main.py train")
        return False
    
    # Выполняем анализ
    result = explain_sales_change(
        restaurant_name=args.restaurant,
        date=args.date,
        save_results=True
    )
    
    if result:
        print("\n" + "="*50)
        print("РЕЗУЛЬТАТ АНАЛИЗА")
        print("="*50)
        print(f"Ресторан: {result['restaurant']}")
        print(f"Дата: {result['date']}")
        print(f"Фактические продажи: {format_currency(result['actual_sales'])}")
        print(f"Прогноз продаж: {format_currency(result['predicted_sales'])}")
        print(f"Изменение: {result['change_percent']:.1f}%")
        print(f"\nОбъяснение: {result['explanation']}")
        
        print(f"\nТоп-3 фактора влияния:")
        for factor, impact in result['top_factors'].items():
            impact_percent = impact * 100
            print(f"  • {factor}: {impact_percent:+.1f}%")
        
        print(f"\nДетальные результаты сохранены в: {RESULTS_PATH}")
        return True
    else:
        logger.error("Ошибка анализа")
        return False

def batch_analyze_command(args):
    """Команда для пакетного анализа"""
    logger.info("=== Пакетный анализ ===")
    
    # Проверяем существование модели
    if not os.path.exists(MODEL_PATH):
        logger.error(f"Модель не найдена: {MODEL_PATH}")
        return False
    
    # Создаем список дат
    start_date = datetime.strptime(args.start_date, '%Y-%m-%d')
    end_date = datetime.strptime(args.end_date, '%Y-%m-%d')
    
    date_range = []
    current_date = start_date
    while current_date <= end_date:
        date_range.append(current_date.strftime('%Y-%m-%d'))
        current_date += timedelta(days=1)
    
    # Создаем объяснитель
    explainer = SalesExplainer()
    if explainer.predictor is None:
        return False
    
    # Получаем данные
    df = get_restaurant_data(args.restaurant)
    if df is None:
        return False
    
    # Выполняем пакетный анализ
    results = explainer.batch_explain(df, args.restaurant, date_range)
    
    if results:
        print(f"\nПроанализировано {len(results)} дней из {len(date_range)}")
        
        # Сохраняем результаты
        batch_filename = os.path.join(
            RESULTS_PATH, 
            f"{args.restaurant}_batch_{args.start_date}_{args.end_date}.json"
        )
        
        with open(batch_filename, 'w', encoding='utf-8') as f:
            json.dump(results, f, ensure_ascii=False, indent=2)
        
        print(f"Результаты сохранены в: {batch_filename}")
        
        # Создаем сводный отчет
        summary = explainer.create_summary_report(args.restaurant, date_range)
        if summary:
            print("\n" + "="*50)
            print("СВОДНЫЙ ОТЧЕТ")
            print("="*50)
            print(f"Ресторан: {summary['restaurant']}")
            print(f"Период: {summary['period']}")
            print(f"Среднее изменение продаж: {summary['average_change']:.1f}%")
            
            if summary['top_positive_factors']:
                print(f"\nТоп положительных факторов:")
                for factor, impact in summary['top_positive_factors'].items():
                    print(f"  • {factor}: +{impact*100:.1f}%")
            
            if summary['top_negative_factors']:
                print(f"\nТоп негативных факторов:")
                for factor, impact in summary['top_negative_factors'].items():
                    print(f"  • {factor}: {impact*100:.1f}%")
            
            if summary['recommendations']:
                print(f"\nРекомендации:")
                for rec in summary['recommendations']:
                    print(f"  • {rec}")
        
        return True
    else:
        logger.error("Ошибка пакетного анализа")
        return False

def info_command(args):
    """Команда для получения информации о модели"""
    logger.info("=== Информация о модели ===")
    
    if not os.path.exists(MODEL_PATH):
        print("Модель не найдена")
        print(f"Путь: {MODEL_PATH}")
        print("Сначала обучите модель: python main.py train")
        return False
    
    predictor = load_trained_model()
    if predictor:
        info = predictor.get_model_info()
        
        print(f"Статус модели: {info['status']}")
        print(f"Тип модели: {info['model_type']}")
        print(f"Количество признаков: {info['feature_count']}")
        
        if 'metrics' in info:
            metrics = info['metrics']
            print(f"\nМетрики качества:")
            
            # Безопасное форматирование метрик
            test_r2 = metrics.get('test_r2', 'N/A')
            test_mse = metrics.get('test_mse', 'N/A')
            test_mae = metrics.get('test_mae', 'N/A')
            cv_mean_r2 = metrics.get('cv_mean_r2', 'N/A')
            
            if isinstance(test_r2, (int, float)):
                print(f"  R² (тест): {test_r2:.4f}")
            else:
                print(f"  R² (тест): {test_r2}")
                
            if isinstance(test_mse, (int, float)):
                print(f"  MSE (тест): {test_mse:.2f}")
            else:
                print(f"  MSE (тест): {test_mse}")
                
            if isinstance(test_mae, (int, float)):
                print(f"  MAE (тест): {test_mae:.2f}")
            else:
                print(f"  MAE (тест): {test_mae}")
                
            if isinstance(cv_mean_r2, (int, float)):
                print(f"  Кросс-валидация R²: {cv_mean_r2:.4f}")
            else:
                print(f"  Кросс-валидация R²: {cv_mean_r2}")
        
        # Показываем важность признаков
        importance = predictor.get_feature_importance(top_n=10)
        if importance is not None:
            print(f"\nТоп-10 важных признаков:")
            for _, row in importance.iterrows():
                print(f"  • {row['feature']}: {row['importance']:.4f}")
        
        return True
    else:
        logger.error("Ошибка загрузки модели")
        return False

def list_restaurants_command(args):
    """Команда для получения списка ресторанов"""
    logger.info("=== Список ресторанов ===")
    
    df = load_data_for_training()
    if df is None:
        logger.error("Не удалось загрузить данные")
        return False
    
    restaurants = df['restaurant_name'].unique()
    print(f"Найдено {len(restaurants)} ресторанов:")
    
    for restaurant in sorted(restaurants):
        # Подсчитываем количество записей
        count = len(df[df['restaurant_name'] == restaurant])
        print(f"  • {restaurant} ({count} записей)")
    
    return True

def main():
    """Главная функция"""
    parser = argparse.ArgumentParser(
        description="ML-модель объяснимого анализа причин изменения продаж",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Примеры использования:

  # Обучение модели
  python main.py train --model-type random_forest

  # Анализ конкретного случая
  python main.py analyze --restaurant "Honeycomb" --date "2023-06-15"

  # Пакетный анализ за период
  python main.py batch --restaurant "Honeycomb" --start-date "2023-06-01" --end-date "2023-06-30"

  # Информация о модели
  python main.py info

  # Список ресторанов
  python main.py list
        """
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Доступные команды')
    
    # Команда обучения
    train_parser = subparsers.add_parser('train', help='Обучение модели')
    train_parser.add_argument('--start-date', help='Начальная дата (YYYY-MM-DD)')
    train_parser.add_argument('--end-date', help='Конечная дата (YYYY-MM-DD)')
    train_parser.add_argument('--model-type', choices=['random_forest', 'gradient_boosting'], 
                             default='random_forest', help='Тип модели')
    train_parser.add_argument('--optimize', action='store_true', 
                             help='Оптимизация гиперпараметров')
    
    # Команда анализа
    analyze_parser = subparsers.add_parser('analyze', help='Анализ конкретного случая')
    analyze_parser.add_argument('--restaurant', required=True, help='Название ресторана')
    analyze_parser.add_argument('--date', required=True, help='Дата анализа (YYYY-MM-DD)')
    
    # Команда пакетного анализа
    batch_parser = subparsers.add_parser('batch', help='Пакетный анализ за период')
    batch_parser.add_argument('--restaurant', required=True, help='Название ресторана')
    batch_parser.add_argument('--start-date', required=True, help='Начальная дата (YYYY-MM-DD)')
    batch_parser.add_argument('--end-date', required=True, help='Конечная дата (YYYY-MM-DD)')
    
    # Команда информации
    info_parser = subparsers.add_parser('info', help='Информация о модели')
    
    # Команда списка ресторанов
    list_parser = subparsers.add_parser('list', help='Список ресторанов')
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return 1
    
    # Выполняем команду
    try:
        if args.command == 'train':
            success = train_model_command(args)
        elif args.command == 'analyze':
            success = analyze_command(args)
        elif args.command == 'batch':
            success = batch_analyze_command(args)
        elif args.command == 'info':
            success = info_command(args)
        elif args.command == 'list':
            success = list_restaurants_command(args)
        else:
            logger.error(f"Неизвестная команда: {args.command}")
            success = False
        
        return 0 if success else 1
        
    except KeyboardInterrupt:
        logger.info("Прервано пользователем")
        return 1
    except Exception as e:
        logger.error(f"Неожиданная ошибка: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main())