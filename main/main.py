"""
Главный модуль для запуска анализа причин изменения продаж
"""
import argparse
import logging
import sys
from datetime import datetime, timedelta
from typing import Dict
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

def causal_analysis_command(args):
    """Причинно-следственный анализ драйверов роста заказов"""
    logger.info("=== Причинно-следственный анализ ===")
    
    try:
        from business_intelligence_system import generate_causal_analysis_report
        
        if args.compare_all:
            # Сравнительный анализ всех ресторанов
            report = generate_causal_analysis_report()
        else:
            # Анализ конкретного ресторана
            report = generate_causal_analysis_report(args.restaurant, args.start_date, args.end_date)
        
        if 'error' not in report:
            if report['type'] == 'single_restaurant':
                print_single_restaurant_causal_analysis(report)
            else:
                print_comparative_causal_analysis(report)
            return True
        else:
            logger.error(f"Ошибка причинно-следственного анализа: {report['error']}")
            return False
            
    except Exception as e:
        logger.error(f"Ошибка выполнения причинно-следственного анализа: {e}")
        return False

def print_single_restaurant_causal_analysis(report: Dict):
    """Печать результатов анализа конкретного ресторана"""
    analysis = report['restaurant_analysis']
    
    print(f"\n🎯 ПРИЧИННО-СЛЕДСТВЕННЫЙ АНАЛИЗ: {analysis['restaurant_name']}")
    print("=" * 70)
    
    # Драйверы заказов
    if analysis['order_correlations']:
        print(f"\n📈 ДРАЙВЕРЫ КОЛИЧЕСТВА ЗАКАЗОВ:")
        for driver, data in analysis['order_correlations'].items():
            strength_emoji = {"очень сильная": "🔥", "сильная": "💪", "умеренная": "📊", "слабая": "📉"}
            emoji = strength_emoji.get(data['strength'], "📊")
            
            print(f"  {emoji} {data['impact_interpretation']}")
            print(f"     Корреляция: {data['correlation']:+.3f} ({data['strength']})")
    
    # Анализ периодов до/после
    if analysis['period_comparisons']:
        print(f"\n⏱️ АНАЛИЗ ИЗМЕНЕНИЙ ПО ПЕРИОДАМ:")
        
        for change_type, change_data in analysis['period_comparisons'].items():
            print(f"\n  📅 {change_data['interpretation']}")
            print(f"     Дата изменения: {change_data['change_date']}")
            print(f"     До: {change_data['before_avg_orders']:.1f} заказов/день")
            print(f"     После: {change_data['after_avg_orders']:.1f} заказов/день")
    
    # Платформенный эффект
    if 'platform_effect' in analysis:
        platform = analysis['platform_effect']
        print(f"\n📱 ПЛАТФОРМЕННЫЙ ЭФФЕКТ:")
        print(f"  {platform['interpretation']}")
        print(f"  Gojek: {platform['gojek_avg_orders']:.1f} заказов/день")
        print(f"  Grab: {platform['grab_avg_orders']:.1f} заказов/день")
    
    # Рычаги роста
    if analysis['growth_levers']:
        print(f"\n🚀 УПРАВЛЯЕМЫЕ РЫЧАГИ РОСТА:")
        
        sorted_levers = sorted(analysis['growth_levers'].items(), 
                             key=lambda x: float(x[1]['potential_order_increase'].replace('%', '')), 
                             reverse=True)
        
        for i, (lever_name, lever_data) in enumerate(sorted_levers, 1):
            actionability_emoji = {"high": "🟢", "medium": "🟡", "low": "🔴"}
            emoji = actionability_emoji.get(lever_data['actionability'], "⚪")
            
            print(f"  {i}. {emoji} {lever_data['recommendation']}")
            print(f"     Потенциал: +{lever_data['potential_order_increase']} заказов")
            print(f"     Текущее значение: {lever_data['current_value']:.1f}")
            print(f"     Целевое значение: {lever_data['target_value']:.1f}")
            print()
    
    # Конкретные рекомендации
    if analysis['actionable_insights']:
        print(f"\n💡 КОНКРЕТНЫЕ РЕКОМЕНДАЦИИ:")
        for insight in analysis['actionable_insights']:
            print(f"  {insight}")

def print_comparative_causal_analysis(report: Dict):
    """Печать результатов сравнительного анализа"""
    comparison = report['comparison_analysis']
    
    print(f"\n🏆 СРАВНИТЕЛЬНЫЙ АНАЛИЗ РЕСТОРАНОВ")
    print(f"Проанализировано ресторанов: {report['total_restaurants_analyzed']}")
    print("=" * 70)
    
    # Топ-исполнители
    print(f"\n🥇 ТОП-ИСПОЛНИТЕЛИ:")
    for i, (name, data) in enumerate(comparison['top_performers'].items(), 1):
        print(f"  {i}. {name}")
        print(f"     Заказов в день: {data['avg_orders']:.1f}")
        print(f"     Рост заказов: {data['order_growth']:+.1f}%")
        print(f"     Рейтинг: {data['avg_rating']:.2f}")
        print(f"     Время доставки: {data['avg_delivery_time']:.1f} мин")
        print(f"     Отмены: {data['avg_cancel_rate']*100:.1f}%")
        print(f"     Реклама: {data['ads_usage_percent']:.0f}% дней")
        print()
    
    # Аутсайдеры
    print(f"\n📉 АУТСАЙДЕРЫ:")
    for i, (name, data) in enumerate(comparison['underperformers'].items(), 1):
        print(f"  {i}. {name}")
        print(f"     Заказов в день: {data['avg_orders']:.1f}")
        print(f"     Рост заказов: {data['order_growth']:+.1f}%")
        print(f"     Рейтинг: {data['avg_rating']:.2f}")
        print(f"     Время доставки: {data['avg_delivery_time']:.1f} мин")
        print(f"     Отмены: {data['avg_cancel_rate']*100:.1f}%")
        print(f"     Реклама: {data['ads_usage_percent']:.0f}% дней")
        print()
    
    # Факторы успеха
    if comparison['success_factors']:
        print(f"\n🎯 КЛЮЧЕВЫЕ ФАКТОРЫ УСПЕХА:")
        for factor, data in comparison['success_factors'].items():
            if data['is_success_factor']:
                factor_names = {
                    'rating': 'Рейтинг',
                    'delivery_time': 'Время доставки',
                    'cancel_rate': 'Уровень отмен',
                    'ads_usage': 'Использование рекламы'
                }
                
                factor_name = factor_names.get(factor, factor)
                print(f"  🔑 {factor_name}:")
                print(f"     Топ: {data['top_avg']:.2f}")
                print(f"     Аутсайдеры: {data['bottom_avg']:.2f}")
                print(f"     Различие: {data['difference_percent']:.1f}%")
    
    # Инсайты дифференциации
    if comparison['differentiation_insights']:
        print(f"\n💡 ИНСАЙТЫ ДИФФЕРЕНЦИАЦИИ:")
        for insight in comparison['differentiation_insights']:
            print(f"  {insight}")
    
    # Индивидуальный анализ топ-ресторанов
    if 'individual_analyses' in report:
        print(f"\n🔍 ДЕТАЛЬНЫЙ АНАЛИЗ ТОП-РЕСТОРАНОВ:")
        for restaurant, analysis in report['individual_analyses'].items():
            print(f"\n📍 {restaurant}:")
            
            if analysis['growth_levers']:
                top_lever = sorted(analysis['growth_levers'].items(), 
                                 key=lambda x: float(x[1]['potential_order_increase'].replace('%', '')), 
                                 reverse=True)[0]
                
                lever_data = top_lever[1]
                print(f"  🚀 Главный рычаг роста: {lever_data['recommendation']}")
                print(f"     Потенциал: +{lever_data['potential_order_increase']} заказов")

def market_intelligence_command(args):
    """Комплексный рыночный анализ всей базы ресторанов"""
    logger.info("=== Рыночная аналитика ===")
    
    try:
        from business_intelligence_system import generate_market_intelligence_report
        
        report = generate_market_intelligence_report(args.start_date, args.end_date)
        
        if 'error' not in report:
            print_market_intelligence_report(report)
            return True
        else:
            logger.error(f"Ошибка рыночной аналитики: {report['error']}")
            return False
            
    except Exception as e:
        logger.error(f"Ошибка выполнения рыночной аналитики: {e}")
        return False

def print_market_intelligence_report(report: Dict):
    """Печать результатов комплексного рыночного анализа"""
    
    print(f"\n🌍 КОМПЛЕКСНЫЙ РЫНОЧНЫЙ АНАЛИЗ")
    print(f"📅 Период анализа: {report['period']}")
    print("=" * 80)
    
    # Обзор рынка
    market = report['market_overview']
    print(f"\n📊 ОБЗОР РЫНКА:")
    print(f"  🏪 Ресторанов: {market['total_restaurants']}")
    print(f"  📅 Дней проанализировано: {market['total_days_analyzed']}")
    print(f"  💰 Общие продажи: {market['total_sales']:,.0f} IDR")
    print(f"  📦 Общие заказы: {market['total_orders']:,}")
    print(f"  ⭐ Средний рейтинг: {market['market_average_rating']:.2f}")
    print(f"  🚚 Среднее время доставки: {market['market_average_delivery_time']:.1f} мин")
    print(f"  ❌ Средний процент отмен: {market['market_cancel_rate']*100:.1f}%")
    print(f"  📢 Использование рекламы: {market['ads_adoption_rate']:.1f}% дней")
    print(f"  💎 Средний ROAS: {market['average_roas']:.1f}")
    
    # Анализ платформ
    platform_analysis = report.get('platform_analysis', {})
    if 'comparison' in platform_analysis:
        comp = platform_analysis['comparison']
        grab = platform_analysis.get('grab', {})
        gojek = platform_analysis.get('gojek', {})
        
        print(f"\n🏆 СРАВНЕНИЕ ПЛАТФОРМ:")
        print(f"  💰 Лидер по продажам: {comp['sales_leader'].title()} (разрыв: {comp['sales_difference_pct']:.1f}%)")
        print(f"  📦 Лидер по заказам: {comp['orders_leader'].title()}")
        print(f"  💎 Лидер по эффективности: {comp['efficiency_leader'].title()} (AOV разрыв: {comp['aov_difference_pct']:.1f}%)")
        print(f"  ⚡ Лидер по скорости: {comp['speed_leader'].title()} (разрыв: {comp['delivery_time_difference']:.1f} мин)")
        print(f"  📢 Лидер по рекламе: {comp['roas_leader'].title()} (ROAS разрыв: {comp['roas_difference_pct']:.1f}%)")
        
        print(f"\n  📈 ДЕТАЛИЗАЦИЯ ПО ПЛАТФОРМАМ:")
        if grab:
            print(f"    🟢 GRAB:")
            print(f"      • Доля рынка: {grab['market_share_by_records']:.1f}%")
            print(f"      • Продажи: {grab['total_sales']:,.0f} IDR")
            print(f"      • AOV: {grab['average_order_value']:,.0f} IDR")
            print(f"      • Рейтинг: {grab['average_rating']:.2f}")
            print(f"      • Время доставки: {grab['average_delivery_time']:.1f} мин")
            print(f"      • Отмены: {grab['cancel_rate']*100:.1f}%")
            print(f"      • ROAS: {grab['average_roas']:.1f}")
        
        if gojek:
            print(f"    🟡 GOJEK:")
            print(f"      • Доля рынка: {gojek['market_share_by_records']:.1f}%")
            print(f"      • Продажи: {gojek['total_sales']:,.0f} IDR")
            print(f"      • AOV: {gojek['average_order_value']:,.0f} IDR")
            print(f"      • Рейтинг: {gojek['average_rating']:.2f}")
            print(f"      • Время доставки: {gojek['average_delivery_time']:.1f} мин")
            print(f"      • Отмены: {gojek['cancel_rate']*100:.1f}%")
            print(f"      • ROAS: {gojek['average_roas']:.1f}")
    
    # Топ-исполнители
    rest_perf = report.get('restaurant_performance', {})
    if 'top_performers' in rest_perf:
        print(f"\n🥇 ТОП-ИСПОЛНИТЕЛИ:")
        
        top_sales = rest_perf['top_performers']['by_sales']
        print(f"  💰 ПО ПРОДАЖАМ:")
        for i, (name, data) in enumerate(list(top_sales.items())[:3], 1):
            print(f"    {i}. {name}: {data['total_sales']:,.0f} IDR ({data['total_orders']:,} заказов)")
        
        top_orders = rest_perf['top_performers']['by_orders']
        print(f"  📦 ПО ЗАКАЗАМ:")
        for i, (name, data) in enumerate(list(top_orders.items())[:3], 1):
            print(f"    {i}. {name}: {data['total_orders']:,} заказов ({data['total_sales']:,.0f} IDR)")
        
        top_efficiency = rest_perf['top_performers']['by_efficiency']
        print(f"  💎 ПО ЭФФЕКТИВНОСТИ (AOV):")
        for i, (name, data) in enumerate(list(top_efficiency.items())[:3], 1):
            print(f"    {i}. {name}: {data['avg_order_value']:,.0f} IDR за заказ")
    
    # Рекламная аналитика
    ads_intel = report.get('advertising_intelligence', {})
    if 'performance_comparison' in ads_intel:
        perf = ads_intel['performance_comparison']
        print(f"\n📢 РЕКЛАМНАЯ АНАЛИТИКА:")
        print(f"  🚀 Эффект рекламы: +{perf['sales_lift']:.1f}% к продажам, +{perf['orders_lift']:.1f}% к заказам")
        print(f"  💰 Продажи с рекламой: {perf['avg_sales_with_ads']:,.0f} IDR/день")
        print(f"  💰 Продажи без рекламы: {perf['avg_sales_without_ads']:,.0f} IDR/день")
        
        if 'advertiser_segments' in ads_intel:
            segments = ads_intel['advertiser_segments']
            print(f"\n  📊 СЕГМЕНТЫ РЕКЛАМОДАТЕЛЕЙ:")
            print(f"    🔥 Активные (80%+ дней): {segments['heavy_advertisers']['count']} ресторанов")
            print(f"       ROAS: {segments['heavy_advertisers']['avg_roas']:.1f}")
            print(f"       Продажи: {segments['heavy_advertisers']['avg_daily_sales']:,.0f} IDR/день")
            
            print(f"    🔸 Умеренные (20-80% дней): {segments['moderate_advertisers']['count']} ресторанов")
            print(f"       ROAS: {segments['moderate_advertisers']['avg_roas']:.1f}")
            
            print(f"    🔹 Слабые (<20% дней): {segments['light_advertisers']['count']} ресторанов")
            if segments['light_advertisers']['avg_roas'] > 0:
                print(f"       ROAS: {segments['light_advertisers']['avg_roas']:.1f}")
        
        if 'temporal_patterns' in ads_intel:
            patterns = ads_intel['temporal_patterns']
            if patterns.get('best_ads_day') and patterns.get('worst_ads_day'):
                weekdays = ['Пн', 'Вт', 'Ср', 'Чт', 'Пт', 'Сб', 'Вс']
                best_day = weekdays[patterns['best_ads_day']]
                worst_day = weekdays[patterns['worst_ads_day']]
                best_roas = patterns['roas_by_weekday'][patterns['best_ads_day']]
                worst_roas = patterns['roas_by_weekday'][patterns['worst_ads_day']]
                print(f"  📅 Лучший день для рекламы: {best_day} (ROAS: {best_roas:.1f})")
                print(f"  📅 Худший день для рекламы: {worst_day} (ROAS: {worst_roas:.1f})")
    
    # Временной анализ
    temporal = report.get('temporal_analysis', {})
    if 'comparisons' in temporal:
        print(f"\n📈 ВРЕМЕННОЙ АНАЛИЗ:")
        
        if 'year_over_year' in temporal['comparisons']:
            yoy = temporal['comparisons']['year_over_year']['changes_pct']
            print(f"  📅 ГОД К ГОДУ:")
            print(f"    💰 Продажи: {yoy['total_sales']:+.1f}%")
            print(f"    📦 Заказы: {yoy['total_orders']:+.1f}%")
            print(f"    ⭐ Рейтинг: {yoy['avg_rating']:+.1f}%")
            print(f"    🚚 Время доставки: {yoy['avg_delivery_time']:+.1f}%")
            print(f"    💎 ROAS: {yoy['avg_roas']:+.1f}%")
        
        if 'quarter_over_quarter' in temporal['comparisons']:
            qoq = temporal['comparisons']['quarter_over_quarter']['changes_pct']
            print(f"  📅 КВАРТАЛ К КВАРТАЛУ:")
            print(f"    💰 Продажи: {qoq['total_sales']:+.1f}%")
            print(f"    📦 Заказы: {qoq['total_orders']:+.1f}%")
            print(f"    ⭐ Рейтинг: {qoq['avg_rating']:+.1f}%")
    
    # Аномалии
    anomalies = report.get('market_anomalies', {})
    if anomalies.get('summary'):
        summary = anomalies['summary']
        print(f"\n🚨 РЫНОЧНЫЕ АНОМАЛИИ:")
        print(f"  📊 Аномалии продаж: {summary['total_sales_anomalies']}")
        print(f"  📢 Аномалии ROAS: {summary['total_roas_anomalies']}")
        print(f"  🚚 Аномалии доставки: {summary['total_delivery_anomalies']}")
        
        if anomalies.get('sales_anomalies'):
            print(f"\n  🔥 ТОП-3 АНОМАЛИИ ПРОДАЖ:")
            for i, anomaly in enumerate(anomalies['sales_anomalies'][:3], 1):
                date_str = anomaly['date'].strftime('%Y-%m-%d') if hasattr(anomaly['date'], 'strftime') else str(anomaly['date'])[:10]
                print(f"    {i}. {anomaly['restaurant_name']} ({date_str}): {anomaly['total_sales']:,.0f} IDR")
    
    # Стратегические рекомендации
    if report.get('strategic_recommendations'):
        print(f"\n🎯 СТРАТЕГИЧЕСКИЕ РЕКОМЕНДАЦИИ:")
        for i, rec in enumerate(report['strategic_recommendations'], 1):
            print(f"  {i}. {rec}")
    
    # Экспертные гипотезы
    if report.get('expert_hypotheses'):
        print(f"\n💡 ЭКСПЕРТНЫЕ ГИПОТЕЗЫ:")
        for i, hyp in enumerate(report['expert_hypotheses'], 1):
            print(f"  {i}. {hyp}")
    
    print(f"\n📄 Отчет сгенерирован: {report['metadata']['generated_at']}")
    print(f"📊 Проанализировано записей: {report['metadata']['total_data_points']:,}")

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
            
            # YoY сравнения
            if 'temporal_analysis' in report and 'comparisons' in report['temporal_analysis']:
                temporal = report['temporal_analysis']
                if 'year_over_year' in temporal['comparisons']:
                    yoy = temporal['comparisons']['year_over_year']['changes_pct']
                    print(f"\n📈 ИЗМЕНЕНИЯ ГОД К ГОДУ (vs 2024):")
                    if 'total_sales' in yoy:
                        change = yoy['total_sales']
                        arrow = "↑" if change > 0 else "↓"
                        print(f"  • Продажи: {arrow}{abs(change):.1f}%")
                    if 'total_orders' in yoy:
                        change = yoy['total_orders']
                        arrow = "↑" if change > 0 else "↓"
                        print(f"  • Заказы: {arrow}{abs(change):.1f}%")
                    if 'avg_rating' in yoy:
                        change = yoy['avg_rating']
                        arrow = "↑" if change > 0 else "↓"
                        print(f"  • Рейтинг: {arrow}{abs(change):.1f}%")
            
            # Конкурентный анализ
            if 'competitor_analysis' in report and report['competitor_analysis']['top_performers']:
                competitors = report['competitor_analysis']
                print(f"\n🏆 КОНКУРЕНТНОЕ СРАВНЕНИЕ (ТОП-5 ПО ЗАКАЗАМ):")
                for i, comp in enumerate(competitors['top_performers'], 1):
                    print(f"  {i}. {comp['restaurant_name']}: {comp['avg_orders_per_day']:.1f} заказов/день")
                
                if competitors['current_restaurant_rank']:
                    print(f"\n📊 Позиция {report['restaurant_name']}: #{competitors['current_restaurant_rank']} место")
            
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
            
            # Критические рекомендации на основе YoY анализа
            if 'temporal_analysis' in report and 'comparisons' in report['temporal_analysis']:
                temporal = report['temporal_analysis']
                if 'year_over_year' in temporal['comparisons']:
                    yoy = temporal['comparisons']['year_over_year']['changes_pct']
                    print(f"\n🚨 КРИТИЧЕСКИЕ РЕКОМЕНДАЦИИ:")
                    
                    if yoy.get('total_sales', 0) < -10:
                        print(f"  🔴 ТРЕВОГА: Продажи упали на {abs(yoy['total_sales']):.1f}% год к году!")
                        print(f"     Требуется антикризисная стратегия и немедленные действия")
                    elif yoy.get('total_sales', 0) > 30:
                        print(f"  🟢 УСПЕХ: Рост продаж {yoy['total_sales']:.1f}% год к году!")
                        print(f"     Масштабировать успешные практики")
                    
                    if yoy.get('total_orders', 0) < -20:
                        print(f"  🔴 КРИТИЧНО: Заказы упали на {abs(yoy['total_orders']):.1f}% год к году!")
                        print(f"     Проблемы с привлечением клиентов, усилить маркетинг")
            
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

  # Причинно-следственный анализ драйверов роста заказов
  python main.py causal --restaurant "Ika Canggu" --start-date "2024-04-01" --end-date "2024-06-30"
  python main.py causal --compare-all

  # Комплексный рыночный анализ всей базы ресторанов
  python main.py market --start-date "2025-04-01" --end-date "2025-06-21"

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
    
    # Команда причинно-следственного анализа
    causal_parser = subparsers.add_parser('causal', help='Причинно-следственный анализ драйверов роста заказов')
    causal_parser.add_argument('--restaurant', help='Название ресторана (опционально для индивидуального анализа)')
    causal_parser.add_argument('--start-date', help='Начальная дата (YYYY-MM-DD)')
    causal_parser.add_argument('--end-date', help='Конечная дата (YYYY-MM-DD)')
    causal_parser.add_argument('--compare-all', action='store_true', help='Сравнительный анализ всех ресторанов')
    
    # Команда рыночной аналитики
    market_parser = subparsers.add_parser('market', help='Комплексный рыночный анализ всей базы ресторанов')
    market_parser.add_argument('--start-date', required=True, help='Начальная дата (YYYY-MM-DD)')
    market_parser.add_argument('--end-date', required=True, help='Конечная дата (YYYY-MM-DD)')
    
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
    elif args.command == 'causal':
        success = causal_analysis_command(args)
    elif args.command == 'market':
        success = market_intelligence_command(args)
    elif args.command == 'train':
        success = train_model_command(args)
    elif args.command == 'info':
        success = info_command(args)
    elif args.command == 'list':
        success = list_restaurants_command(args)
    
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()