#!/usr/bin/env python3
"""
🔧 ИНТЕГРИРОВАННАЯ СИСТЕМА БИЗНЕС-АНАЛИТИКИ
Объединяет исправленные модули в единую систему с контролем качества
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any
import logging
import sys
import os

# Добавляем текущую директорию в path
sys.path.append(os.path.dirname(__file__))

# Импортируем исправленные модули
from data_loader_fixed import get_restaurant_data, get_multiple_restaurants, validate_features
from feature_engineering_fixed import prepare_features_fixed, clean_features, prepare_for_model
from business_intelligence_system import BusinessIntelligenceSystem

logger = logging.getLogger(__name__)

class IntegratedAnalyticsSystem:
    """
    Интегрированная система бизнес-аналитики
    
    Объединяет:
    - Контролируемую загрузку данных (белый список полей)
    - Безопасный feature engineering (без утечек данных)
    - Оригинальную систему анализа (живые инсайты)
    """
    
    def __init__(self):
        self.business_system = BusinessIntelligenceSystem()
        self.data_quality_checks = True
        
    def load_and_prepare_data(self, restaurant_name: Optional[str] = None) -> pd.DataFrame:
        """
        Загружает и подготавливает данные с полным контролем качества
        
        Args:
            restaurant_name: Название ресторана
            
        Returns:
            DataFrame с подготовленными данными
        """
        
        logger.info(f"🔧 Загрузка и подготовка данных для: {restaurant_name or 'ВСЕ РЕСТОРАНЫ'}")
        
        try:
            # 1. Загружаем данные через исправленный loader
            logger.info("📊 Шаг 1: Загрузка базовых данных...")
            df = get_restaurant_data(restaurant_name)
            
            if df.empty:
                logger.warning("⚠️ Нет данных для анализа")
                return df
            
            logger.info(f"✅ Загружено: {len(df)} записей, {len(df.columns)} полей")
            
            # 2. Создаём признаки через исправленный feature engineering
            logger.info("🔧 Шаг 2: Создание дополнительных признаков...")
            df_featured = prepare_features_fixed(df)
            
            logger.info(f"✅ Создано признаков: {len(df_featured.columns)} (+{len(df_featured.columns) - len(df)})")
            
            # 3. Очищаем данные
            logger.info("🧹 Шаг 3: Очистка данных...")
            df_clean = clean_features(df_featured)
            
            # 4. Финальная валидация
            logger.info("🔍 Шаг 4: Финальная валидация...")
            validate_features(df_clean)
            
            logger.info("✅ Данные готовы для анализа")
            return df_clean
            
        except Exception as e:
            logger.error(f"❌ Ошибка подготовки данных: {e}")
            raise
    
    def generate_business_report(self, restaurant_name: str, 
                               start_date: Optional[str] = None,
                               end_date: Optional[str] = None) -> Dict[str, Any]:
        """
        Генерирует бизнес-отчёт в стиле оригинальной системы
        
        Args:
            restaurant_name: Название ресторана
            start_date: Дата начала (YYYY-MM-DD)
            end_date: Дата окончания (YYYY-MM-DD)
            
        Returns:
            Словарь с результатами анализа
        """
        
        logger.info(f"📊 Генерация бизнес-отчёта для {restaurant_name}")
        
        try:
            # 1. Загружаем данные
            df = self.load_and_prepare_data()
            
            if df.empty:
                return {"error": "Нет данных для анализа"}
            
            # 2. Фильтруем по ресторану и периоду
            restaurant_data = df[df['restaurant_name'] == restaurant_name].copy()
            
            if start_date and end_date:
                mask = (restaurant_data['date'] >= start_date) & (restaurant_data['date'] <= end_date)
                restaurant_data = restaurant_data[mask]
            
            if restaurant_data.empty:
                return {"error": f"Нет данных для ресторана {restaurant_name} за указанный период"}
            
            # 3. Базовая статистика
            total_sales = restaurant_data['total_sales'].sum()
            total_orders = restaurant_data['orders'].sum()
            avg_rating = restaurant_data['rating'].mean()
            days_count = len(restaurant_data)
            
            # 4. Анализ по месяцам
            restaurant_data['month'] = restaurant_data['date'].dt.month
            monthly_stats = restaurant_data.groupby('month').agg({
                'total_sales': 'sum',
                'orders': 'sum', 
                'rating': 'mean'
            }).round(2)
            
            # 5. Поиск аномалий (лучшие и худшие дни)
            restaurant_data['daily_sales'] = restaurant_data['total_sales']
            avg_daily_sales = restaurant_data['daily_sales'].mean()
            
            top_days = restaurant_data.nlargest(3, 'daily_sales')[['date', 'daily_sales', 'orders']].to_dict('records')
            worst_days = restaurant_data.nsmallest(3, 'daily_sales')[['date', 'daily_sales', 'orders']].to_dict('records')
            
            # Добавляем процентные отклонения
            for day in top_days:
                day['deviation_pct'] = ((day['daily_sales'] / avg_daily_sales) - 1) * 100
            
            for day in worst_days:
                day['deviation_pct'] = ((day['daily_sales'] / avg_daily_sales) - 1) * 100
            
            # 6. Анализ маркетинга (если есть данные)
            marketing_analysis = {}
            if 'marketing_spend' in restaurant_data.columns:
                with_marketing = restaurant_data[restaurant_data['marketing_spend'] > 0]
                without_marketing = restaurant_data[restaurant_data['marketing_spend'] == 0]
                
                if len(with_marketing) > 0 and len(without_marketing) > 0:
                    marketing_effect = ((with_marketing['daily_sales'].mean() / without_marketing['daily_sales'].mean()) - 1) * 100
                    marketing_analysis = {
                        'days_with_marketing': len(with_marketing),
                        'days_without_marketing': len(without_marketing),
                        'avg_sales_with_marketing': with_marketing['daily_sales'].mean(),
                        'avg_sales_without_marketing': without_marketing['daily_sales'].mean(),
                        'marketing_effect_pct': marketing_effect
                    }
            
            # 7. Конкурентный анализ
            all_data = df[(df['date'] >= restaurant_data['date'].min()) & 
                         (df['date'] <= restaurant_data['date'].max())]
            
            competitor_stats = all_data.groupby('restaurant_name').agg({
                'total_sales': 'sum',
                'orders': 'sum'
            }).sort_values('total_sales', ascending=False)
            
            competitor_stats['orders_per_day'] = competitor_stats['orders'] / days_count
            
            # Позиция ресторана
            restaurant_position = list(competitor_stats.index).index(restaurant_name) + 1 if restaurant_name in competitor_stats.index else None
            
            # 8. Составляем отчёт
            report = {
                "restaurant_name": restaurant_name,
                "period": {
                    "start_date": restaurant_data['date'].min().strftime('%Y-%m-%d'),
                    "end_date": restaurant_data['date'].max().strftime('%Y-%m-%d'),
                    "days_count": days_count
                },
                "financial_metrics": {
                    "total_sales": float(total_sales),
                    "avg_daily_sales": float(total_sales / days_count),
                    "total_orders": int(total_orders),
                    "avg_orders_per_day": float(total_orders / days_count),
                    "avg_order_value": float(total_sales / total_orders) if total_orders > 0 else 0
                },
                "quality_metrics": {
                    "avg_rating": float(avg_rating),
                    "rating_category": self._get_rating_category(avg_rating)
                },
                "monthly_breakdown": monthly_stats.to_dict('index'),
                "anomalies": {
                    "best_days": top_days,
                    "worst_days": worst_days
                },
                "marketing_analysis": marketing_analysis,
                "market_position": {
                    "position": restaurant_position,
                    "total_restaurants": len(competitor_stats),
                    "competitors": competitor_stats.to_dict('index')
                },
                "insights": self._generate_insights(restaurant_data, marketing_analysis, restaurant_position),
                "recommendations": self._generate_recommendations(restaurant_data, marketing_analysis, restaurant_position)
            }
            
            logger.info("✅ Бизнес-отчёт сгенерирован успешно")
            return report
            
        except Exception as e:
            logger.error(f"❌ Ошибка генерации отчёта: {e}")
            return {"error": str(e)}
    
    def _get_rating_category(self, rating: float) -> str:
        """Определяет категорию рейтинга"""
        if rating >= 4.5:
            return "ОТЛИЧНО"
        elif rating >= 4.0:
            return "ХОРОШО"
        elif rating >= 3.5:
            return "УДОВЛЕТВОРИТЕЛЬНО"
        else:
            return "ТРЕБУЕТ УЛУЧШЕНИЯ"
    
    def _generate_insights(self, data: pd.DataFrame, marketing: Dict, position: Optional[int]) -> List[str]:
        """Генерирует ключевые инсайты"""
        insights = []
        
        avg_daily_sales = data['total_sales'].mean()
        avg_rating = data['rating'].mean()
        
        # Анализ продаж
        if avg_daily_sales > 1000:
            insights.append("✅ Высокие ежедневные продажи")
        else:
            insights.append("⚠️ Продажи ниже целевого уровня")
        
        # Анализ рейтинга
        if avg_rating >= 4.3:
            insights.append("⭐ Отличные оценки клиентов")
        elif avg_rating >= 4.0:
            insights.append("👍 Хорошие оценки клиентов")
        else:
            insights.append("⚠️ Рейтинг требует улучшения")
        
        # Анализ маркетинга
        if marketing and marketing.get('marketing_effect_pct', 0) > 50:
            insights.append("🚨 Реклама критически важна!")
        elif marketing and marketing.get('marketing_effect_pct', 0) > 20:
            insights.append("📢 Реклама эффективна")
        
        # Анализ позиции
        if position == 1:
            insights.append("🏆 Лидер рынка")
        elif position and position <= 3:
            insights.append("🥈 Топ-3 ресторан")
        else:
            insights.append("📈 Есть потенциал для роста")
        
        return insights
    
    def _generate_recommendations(self, data: pd.DataFrame, marketing: Dict, position: Optional[int]) -> List[str]:
        """Генерирует рекомендации"""
        recommendations = []
        
        avg_daily_sales = data['total_sales'].mean()
        
        # Рекомендации по продажам
        if avg_daily_sales < 1000:
            recommendations.append("🎯 Рассмотреть акции для увеличения продаж")
        
        # Рекомендации по маркетингу
        if marketing and marketing.get('days_without_marketing', 0) > marketing.get('days_with_marketing', 0):
            recommendations.append("📢 Увеличить частоту рекламных кампаний")
        
        # Рекомендации по позиции
        if position and position > 3:
            recommendations.append("🚀 Изучить стратегии лидеров рынка")
        
        return recommendations
    
    def print_formatted_report(self, report: Dict[str, Any]) -> None:
        """Выводит отчёт в красивом формате"""
        
        if "error" in report:
            print(f"❌ ОШИБКА: {report['error']}")
            return
        
        print(f"📊 АНАЛИЗ '{report['restaurant_name'].upper()}' | {report['period']['start_date']} - {report['period']['end_date']}")
        print("=" * 80)
        
        print("🎯 ИСПОЛНИТЕЛЬНОЕ РЕЗЮМЕ")
        fin = report['financial_metrics']
        qual = report['quality_metrics']
        pos = report['market_position']
        
        if pos['position'] == 1:
            status = "лидирует на рынке"
        elif pos['position'] <= 3:
            status = f"занимает {pos['position']}-е место в топ-3"
        else:
            status = f"находится на {pos['position']}-м месте из {pos['total_restaurants']}"
        
        print(f"{report['restaurant_name']} {status} с рейтингом {qual['avg_rating']:.2f}⭐")
        
        print(f"\n📈 КЛЮЧЕВЫЕ ПОКАЗАТЕЛИ")
        print(f"💰 ФИНАНСОВЫЕ РЕЗУЛЬТАТЫ:")
        print(f"* Общие продажи: {fin['total_sales']:,.0f}")
        print(f"* Средние продажи в день: {fin['avg_daily_sales']:,.0f}")
        print(f"* Общие заказы: {fin['total_orders']:,} за {report['period']['days_count']} дней")
        print(f"* Средний рейтинг: {qual['avg_rating']:.2f} ⭐ ({qual['rating_category']})")
        
        # Месячная разбивка
        if report['monthly_breakdown']:
            print(f"\n🎯 ТРЕНДЫ ПО МЕСЯЦАМ")
            print(f"📊 ПОМЕСЯЧНАЯ ДИНАМИКА:")
            for month, stats in report['monthly_breakdown'].items():
                month_name = {4: 'АПРЕЛЬ', 5: 'МАЙ', 6: 'ИЮНЬ', 7: 'ИЮЛЬ'}.get(month, f'МЕСЯЦ {month}')
                print(f"* {month_name}: {stats['total_sales']:,.0f}, {stats['orders']:,} заказов, {stats['rating']:.2f}⭐")
        
        # Аномалии
        print(f"\n🔍 АНАЛИЗ АНОМАЛИЙ")
        print(f"📈 ЛУЧШИЕ ДНИ:")
        for i, day in enumerate(report['anomalies']['best_days'], 1):
            date = day['date'] if isinstance(day['date'], str) else day['date'].strftime('%Y-%m-%d')
            print(f"{i}. {date}: {day['daily_sales']:,.0f} (+{day['deviation_pct']:.1f}%) - {day['orders']} заказов")
        
        print(f"📉 ХУДШИЕ ДНИ:")
        for i, day in enumerate(report['anomalies']['worst_days'], 1):
            date = day['date'] if isinstance(day['date'], str) else day['date'].strftime('%Y-%m-%d')
            print(f"{i}. {date}: {day['daily_sales']:,.0f} ({day['deviation_pct']:.1f}%) - {day['orders']} заказов")
        
        # Маркетинг
        if report['marketing_analysis']:
            ma = report['marketing_analysis']
            print(f"\n📢 АНАЛИЗ МАРКЕТИНГА:")
            print(f"* Дней с рекламой: {ma['days_with_marketing']}/{ma['days_with_marketing'] + ma['days_without_marketing']}")
            print(f"* Средние продажи с рекламой: {ma['avg_sales_with_marketing']:,.0f}")
            print(f"* Средние продажи без рекламы: {ma['avg_sales_without_marketing']:,.0f}")
            print(f"* Эффект рекламы: {ma['marketing_effect_pct']:+.1f}%")
            
            if ma['marketing_effect_pct'] > 50:
                print(f"🚨 КРИТИЧНО: Реклама критически важна!")
        
        # Конкуренты
        print(f"\n⚠️ СРАВНЕНИЕ С КОНКУРЕНТАМИ")
        print(f"📊 ПОЗИЦИЯ В РЫНКЕ:")
        for i, (name, stats) in enumerate(report['market_position']['competitors'].items(), 1):
            marker = "👈" if name == report['restaurant_name'] else "  "
            print(f"{marker} {i}. {name}: {stats['orders_per_day']:.1f} заказа/день")
        
        # Инсайты
        print(f"\n🔍 КЛЮЧЕВЫЕ ИНСАЙТЫ:")
        for insight in report['insights']:
            print(f"   • {insight}")
        
        # Рекомендации
        if report['recommendations']:
            print(f"\n💡 РЕКОМЕНДАЦИИ:")
            for rec in report['recommendations']:
                print(f"   • {rec}")

def run_analysis(restaurant_name: str, start_date: str = None, end_date: str = None) -> None:
    """Запускает полный анализ ресторана"""
    
    logging.basicConfig(level=logging.INFO)
    
    print("🚀 ЗАПУСК ИНТЕГРИРОВАННОЙ СИСТЕМЫ БИЗНЕС-АНАЛИТИКИ")
    print("=" * 60)
    
    system = IntegratedAnalyticsSystem()
    
    try:
        report = system.generate_business_report(restaurant_name, start_date, end_date)
        system.print_formatted_report(report)
        
    except Exception as e:
        print(f"❌ КРИТИЧЕСКАЯ ОШИБКА: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    # Тестирование интегрированной системы
    run_analysis("Ika Canggu", "2025-04-01", "2025-06-30")