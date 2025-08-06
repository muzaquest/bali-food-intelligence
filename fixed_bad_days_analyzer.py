#!/usr/bin/env python3
"""
🔧 ИСПРАВЛЕННЫЙ АНАЛИЗАТОР ПЛОХИХ ДНЕЙ ПРОДАЖ
═══════════════════════════════════════════════════════════════════════════════
✅ Использует РЕАЛЬНЫЕ колонки из базы данных
✅ Находит конкретные причины падения продаж
✅ Дает четкие рекомендации клиенту
"""

import sqlite3
import pandas as pd
import requests
from datetime import datetime, timedelta
import json

class FixedBadDaysAnalyzer:
    """Исправленный анализатор с правильными колонками"""
    
    def __init__(self, db_path='database.sqlite'):
        self.db_path = db_path
        self.weather_cache = {}
        
    def analyze_restaurant_problems(self, restaurant_name, days=30):
        """Основной метод анализа проблем ресторана"""
        
        print(f"🔍 АНАЛИЗ ПРОБЛЕМ РЕСТОРАНА '{restaurant_name}'")
        print("=" * 60)
        
        # 1. Загружаем данные с правильными колонками
        data = self._load_real_data(restaurant_name, days)
        
        if data.empty:
            return "❌ Нет данных для анализа"
            
        # 2. Находим проблемные дни
        bad_days = self._find_bad_days(data)
        
        if bad_days.empty:
            return "✅ Проблемных дней не найдено!"
            
        # 3. Анализируем каждый плохой день
        detailed_analysis = []
        
        for _, day in bad_days.iterrows():
            analysis = self._analyze_single_day(day, data)
            detailed_analysis.append(analysis)
            
        # 4. Генерируем отчет для клиента
        return self._generate_client_report(detailed_analysis, restaurant_name)
        
    def _load_real_data(self, restaurant_name, days):
        """Загружает данные с РЕАЛЬНЫМИ колонками"""
        
        conn = sqlite3.connect(self.db_path)
        
        # Используем только существующие колонки
        query = f"""
        SELECT 
            g.stat_date,
            r.name as restaurant_name,
            
            -- ПРОДАЖИ И ЗАКАЗЫ
            COALESCE(g.sales, 0) + COALESCE(gj.sales, 0) as total_sales,
            COALESCE(g.orders, 0) + COALESCE(gj.orders, 0) as total_orders,
            
            -- ОПЕРАЦИОННЫЕ ПРОБЛЕМЫ (реальные колонки!)
            COALESCE(g.store_is_closed, 0) as grab_closed,
            COALESCE(gj.store_is_closed, 0) as gojek_closed,
            COALESCE(g.out_of_stock, 0) as grab_out_of_stock,
            COALESCE(gj.out_of_stock, 0) as gojek_out_of_stock,
            COALESCE(g.store_is_busy, 0) as grab_busy,
            COALESCE(gj.store_is_busy, 0) as gojek_busy,
            
            -- ОТМЕНЫ
            COALESCE(g.cancelled_orders, 0) as grab_cancelled,
            COALESCE(gj.cancelled_orders, 0) as gojek_cancelled,
            
            -- ВРЕМЯ (только у Gojek!)
            COALESCE(gj.accepting_time, '00:00:00') as accepting_time,
            COALESCE(gj.preparation_time, '00:00:00') as preparation_time,
            COALESCE(gj.delivery_time, '00:00:00') as delivery_time,
            COALESCE(gj.close_time, 0) as close_time_minutes,
            
            -- МАРКЕТИНГ
            COALESCE(g.ads_spend, 0) + COALESCE(gj.ads_spend, 0) as total_ad_spend,
            COALESCE(g.ads_sales, 0) + COALESCE(gj.ads_sales, 0) as ads_sales,
            
            -- РЕЙТИНГ
            COALESCE(g.rating, gj.rating, 4.0) as rating,
            
            -- ВРЕМЕННЫЕ ПРИЗНАКИ
            CAST(strftime('%w', g.stat_date) AS INTEGER) as day_of_week,
            CAST(strftime('%m', g.stat_date) AS INTEGER) as month
            
        FROM grab_stats g
        LEFT JOIN gojek_stats gj ON g.restaurant_id = gj.restaurant_id 
                                 AND g.stat_date = gj.stat_date
        LEFT JOIN restaurants r ON g.restaurant_id = r.id
        WHERE r.name = '{restaurant_name}'
        AND g.stat_date >= date('now', '-{days} days')
        AND (g.sales > 0 OR gj.sales > 0)
        ORDER BY g.stat_date DESC
        """
        
        df = pd.read_sql_query(query, conn)
        conn.close()
        
        print(f"✅ Загружено {len(df)} дней данных")
        return df
        
    def _find_bad_days(self, data):
        """Находит дни с плохими продажами"""
        
        # Рассчитываем пороги
        mean_sales = data['total_sales'].mean()
        p10 = data['total_sales'].quantile(0.10)
        p5 = data['total_sales'].quantile(0.05)
        
        print(f"📊 ПОРОГИ ПРОДАЖ:")
        print(f"   • Среднее: {mean_sales:,.0f} IDR")
        print(f"   • 10% перцентиль: {p10:,.0f} IDR")
        print(f"   • 5% перцентиль: {p5:,.0f} IDR")
        
        # Находим плохие дни
        bad_days = data[data['total_sales'] < p10].copy()
        
        print(f"\n🔴 НАЙДЕНО ПЛОХИХ ДНЕЙ: {len(bad_days)}")
        
        return bad_days.sort_values('stat_date', ascending=False)
        
    def _analyze_single_day(self, day_data, all_data):
        """Анализирует один плохой день"""
        
        date = day_data['stat_date']
        sales = day_data['total_sales']
        
        print(f"\n🔍 АНАЛИЗ: {date}")
        print(f"💰 Продажи: {sales:,.0f} IDR")
        
        analysis = {
            'date': date,
            'sales': sales,
            'problems': [],
            'recommendations': []
        }
        
        # Рассчитываем базовую линию
        baseline = all_data['total_sales'].mean()
        drop_percent = ((sales - baseline) / baseline) * 100
        analysis['baseline'] = baseline
        analysis['drop_percent'] = drop_percent
        
        print(f"📉 Падение: {drop_percent:.1f}% от среднего")
        
        # ПРОВЕРЯЕМ КОНКРЕТНЫЕ ПРОБЛЕМЫ
        
        # 1. РЕСТОРАН ЗАКРЫТ
        if day_data['grab_closed'] > 0:
            analysis['problems'].append("🚨 Ресторан был закрыт на Grab")
            analysis['recommendations'].append("🔧 Проверить техническую интеграцию с Grab")
            print("   🚨 ПРОБЛЕМА: Ресторан закрыт на Grab")
            
        if day_data['gojek_closed'] > 0:
            analysis['problems'].append("🚨 Ресторан был закрыт на Gojek")
            analysis['recommendations'].append("🔧 Проверить техническую интеграцию с Gojek")
            print("   🚨 ПРОБЛЕМА: Ресторан закрыт на Gojek")
            
        # 2. НЕТ ТОВАРА
        if day_data['grab_out_of_stock'] > 0:
            analysis['problems'].append("📦 Дефицит товара на Grab")
            analysis['recommendations'].append("📦 Улучшить управление запасами")
            print("   📦 ПРОБЛЕМА: Нет товара на Grab")
            
        if day_data['gojek_out_of_stock'] > 0:
            analysis['problems'].append("📦 Дефицит товара на Gojek")
            analysis['recommendations'].append("📦 Улучшить управление запасами")
            print("   📦 ПРОБЛЕМА: Нет товара на Gojek")
            
        # 3. РЕСТОРАН ЗАНЯТ
        if day_data['grab_busy'] > 0:
            analysis['problems'].append("🚨 Ресторан перегружен на Grab")
            analysis['recommendations'].append("👨‍🍳 Увеличить персонал в пиковые часы")
            print("   🚨 ПРОБЛЕМА: Ресторан перегружен на Grab")
            
        if day_data['gojek_busy'] > 0:
            analysis['problems'].append("🚨 Ресторан перегружен на Gojek")
            analysis['recommendations'].append("👨‍🍳 Увеличить персонал в пиковые часы")
            print("   🚨 ПРОБЛЕМА: Ресторан перегружен на Gojek")
            
        # 4. МНОГО ОТМЕН
        total_cancelled = day_data['grab_cancelled'] + day_data['gojek_cancelled']
        total_orders = day_data['total_orders']
        
        if total_orders > 0:
            cancel_rate = (total_cancelled / total_orders) * 100
            if cancel_rate > 15:  # Больше 15% отмен
                analysis['problems'].append(f"❌ Высокий процент отмен: {cancel_rate:.1f}%")
                analysis['recommendations'].append("⚡ Улучшить скорость обслуживания")
                print(f"   ❌ ПРОБЛЕМА: Много отмен ({cancel_rate:.1f}%)")
                
        # 5. ДОЛГОЕ ВРЕМЯ ГОТОВКИ
        prep_time = day_data['preparation_time']
        if prep_time and prep_time != '00:00:00':
            # Конвертируем время в минуты
            time_parts = prep_time.split(':')
            if len(time_parts) >= 2:
                prep_minutes = int(time_parts[0]) * 60 + int(time_parts[1])
                if prep_minutes > 30:
                    analysis['problems'].append(f"⏱️ Долгое время готовки: {prep_minutes} мин")
                    analysis['recommendations'].append("⚡ Оптимизировать процессы на кухне")
                    print(f"   ⏱️ ПРОБЛЕМА: Долгое время готовки ({prep_minutes} мин)")
                    
        # 6. НИЗКИЙ МАРКЕТИНГ
        avg_ad_spend = all_data['total_ad_spend'].mean()
        if day_data['total_ad_spend'] < avg_ad_spend * 0.5:  # Меньше 50% от среднего
            analysis['problems'].append("📱 Низкий бюджет рекламы")
            analysis['recommendations'].append("💰 Увеличить рекламный бюджет")
            print("   📱 ПРОБЛЕМА: Низкий бюджет рекламы")
            
        # 7. НИЗКИЙ РЕЙТИНГ
        if day_data['rating'] < 4.0:
            analysis['problems'].append(f"⭐ Низкий рейтинг: {day_data['rating']:.1f}")
            analysis['recommendations'].append("⭐ Улучшить качество еды и сервиса")
            print(f"   ⭐ ПРОБЛЕМА: Низкий рейтинг ({day_data['rating']:.1f})")
            
        # 8. ПЛОХОЙ ДЕНЬ НЕДЕЛИ
        weekdays = ['Воскресенье', 'Понедельник', 'Вторник', 'Среда', 'Четверг', 'Пятница', 'Суббота']
        day_name = weekdays[day_data['day_of_week']]
        
        if day_data['day_of_week'] in [0, 1]:  # Воскресенье, Понедельник
            analysis['problems'].append(f"📅 Слабый день: {day_name}")
            print(f"   📅 ФАКТОР: Слабый день недели ({day_name})")
            
        # 9. ПРОВЕРЯЕМ ПОГОДУ
        weather_impact = self._check_weather(date)
        if weather_impact:
            analysis['problems'].append(weather_impact)
            print(f"   🌧️ ФАКТОР: {weather_impact}")
            
        if not analysis['problems']:
            analysis['problems'].append("❓ Причина падения требует дополнительного анализа")
            analysis['recommendations'].append("🔍 Провести детальную проверку операций")
            print("   ❓ Причина не определена")
            
        return analysis
        
    def _check_weather(self, date):
        """Проверяет погодные условия"""
        
        try:
            # Координаты центра Бали
            lat, lon = -8.6705, 115.2126
            
            url = "https://archive-api.open-meteo.com/v1/archive"
            params = {
                'latitude': lat,
                'longitude': lon,
                'start_date': date,
                'end_date': date,
                'hourly': 'precipitation',
                'timezone': 'Asia/Jakarta'
            }
            
            response = requests.get(url, params=params, timeout=5)
            if response.status_code == 200:
                data = response.json()
                hourly = data.get('hourly', {})
                precipitation = hourly.get('precipitation', [])
                
                if precipitation:
                    total_rain = sum(precipitation)
                    if total_rain > 20:
                        return f"Сильный дождь: {total_rain:.1f}мм"
                    elif total_rain > 10:
                        return f"Умеренный дождь: {total_rain:.1f}мм"
                        
        except Exception:
            pass
            
        return None
        
    def _generate_client_report(self, analyzed_days, restaurant_name):
        """Генерирует отчет для клиента"""
        
        if not analyzed_days:
            return f"✅ У ресторана '{restaurant_name}' нет серьезных проблем с продажами!"
            
        report = []
        report.append(f"📋 ОТЧЕТ ПО РЕСТОРАНУ '{restaurant_name}'")
        report.append("=" * 60)
        
        # Общая статистика
        total_loss = sum(day['baseline'] - day['sales'] for day in analyzed_days)
        report.append(f"💰 ОБЩИЕ ПОТЕРИ: {total_loss:,.0f} IDR за {len(analyzed_days)} дней")
        
        # Группируем проблемы
        all_problems = []
        all_recommendations = []
        
        for day in analyzed_days:
            all_problems.extend(day['problems'])
            all_recommendations.extend(day['recommendations'])
            
        # Считаем частоту проблем
        problem_counts = {}
        for problem in all_problems:
            problem_counts[problem] = problem_counts.get(problem, 0) + 1
            
        report.append(f"\n🔍 ГЛАВНЫЕ ПРИЧИНЫ ПРОБЛЕМ:")
        for problem, count in sorted(problem_counts.items(), key=lambda x: x[1], reverse=True)[:5]:
            percentage = (count / len(analyzed_days)) * 100
            report.append(f"   • {problem} ({count} дней, {percentage:.0f}%)")
            
        # Уникальные рекомендации
        unique_recommendations = list(set(all_recommendations))
        
        report.append(f"\n💡 РЕКОМЕНДАЦИИ ПО УЛУЧШЕНИЮ:")
        for i, rec in enumerate(unique_recommendations[:8], 1):
            report.append(f"{i}. {rec}")
            
        # Детали по дням
        report.append(f"\n📅 ДЕТАЛИ ПО ПРОБЛЕМНЫМ ДНЯМ:")
        
        for day in analyzed_days[:5]:  # Показываем топ-5 худших дней
            report.append(f"\n{day['date']} - Продажи: {day['sales']:,.0f} IDR ({day['drop_percent']:+.1f}%)")
            for problem in day['problems'][:3]:  # Топ-3 проблемы дня
                report.append(f"  • {problem}")
                
        # Итоговая оценка
        controllable_problems = [p for p in all_problems if not any(x in p for x in ['день', 'дождь', 'Воскресенье', 'Понедельник'])]
        controllable_pct = (len(controllable_problems) / len(all_problems)) * 100 if all_problems else 0
        
        report.append(f"\n🎯 ИТОГОВАЯ ОЦЕНКА:")
        report.append(f"   • Контролируемых проблем: {controllable_pct:.0f}%")
        report.append(f"   • Потенциал улучшения: ВЫСОКИЙ" if controllable_pct > 60 else "   • Потенциал улучшения: СРЕДНИЙ")
        
        return "\n".join(report)

def main():
    """Демонстрация исправленного анализатора"""
    
    analyzer = FixedBadDaysAnalyzer()
    
    # Тестируем на ресторане Only Eggs
    result = analyzer.analyze_restaurant_problems("Only Eggs", 60)
    
    print("\n" + "="*80)
    print("📋 ГОТОВЫЙ ОТЧЕТ ДЛЯ КЛИЕНТА:")
    print("="*80)
    print(result)
    print("="*80)

if __name__ == "__main__":
    main()