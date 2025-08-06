#!/usr/bin/env python3
"""
🎯 АНАЛИЗ КОНКРЕТНОЙ ДАТЫ - ONLY EGGS 8 АПРЕЛЯ 2025
═══════════════════════════════════════════════════════════════════════════════
Анализируем почему упали продажи у Only Eggs 8 апреля 2025 года
"""

import sqlite3
import pandas as pd
import requests
import json
import os
from datetime import datetime, timedelta

class SpecificDateAnalyzer:
    """Анализатор для конкретной даты"""
    
    def __init__(self, db_path='database.sqlite'):
        self.db_path = db_path
        self.holidays_data = {}
        self.restaurant_locations = {}
        
        # Загружаем реальные данные
        self._load_real_holidays_data()
        self._load_real_restaurant_locations()
        
    def analyze_specific_date(self, restaurant_name, target_date):
        """Анализирует конкретную дату"""
        
        print(f"🎯 АНАЛИЗ КОНКРЕТНОЙ ДАТЫ")
        print("═" * 80)
        print(f"📅 Ресторан: {restaurant_name}")
        print(f"📅 Дата: {target_date}")
        print("═" * 80)
        
        # 1. Проверяем есть ли данные по этому ресторану
        restaurant_data = self._get_restaurant_data(restaurant_name, target_date)
        
        if restaurant_data is None:
            return f"❌ Нет данных по ресторану '{restaurant_name}' на {target_date}"
            
        # 2. Получаем контекст - данные за период вокруг даты
        context_data = self._get_context_data(restaurant_name, target_date)
        
        # 3. Анализируем факторы
        analysis = self._analyze_date_factors(restaurant_data, context_data, target_date, restaurant_name)
        
        # 4. Генерируем отчет
        return self._generate_date_report(analysis, restaurant_name, target_date)
        
    def _load_real_holidays_data(self):
        """Загружает реальные праздники"""
        holidays_file = 'data/comprehensive_holiday_analysis.json'
        
        if os.path.exists(holidays_file):
            with open(holidays_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                self.holidays_data = data.get('results', {})
            print(f"✅ Загружено {len(self.holidays_data)} праздников")
        else:
            print("❌ Файл с праздниками не найден!")
            
    def _load_real_restaurant_locations(self):
        """Загружает координаты ресторанов"""
        locations_file = 'data/bali_restaurant_locations.json'
        
        if os.path.exists(locations_file):
            with open(locations_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                
            for restaurant in data.get('restaurants', []):
                name = restaurant['name']
                self.restaurant_locations[name] = {
                    'lat': restaurant['latitude'],
                    'lon': restaurant['longitude'],
                    'location': restaurant['location'],
                    'zone': restaurant['zone']
                }
            print(f"✅ Загружено {len(self.restaurant_locations)} локаций")
        else:
            print("❌ Файл с локациями не найден!")
            
    def _get_restaurant_data(self, restaurant_name, target_date):
        """Получает данные ресторана на конкретную дату"""
        
        conn = sqlite3.connect(self.db_path)
        
        query = f"""
        SELECT 
            g.stat_date,
            r.name as restaurant_name,
            
            -- ПРОДАЖИ И ЗАКАЗЫ
            COALESCE(g.sales, 0) + COALESCE(gj.sales, 0) as total_sales,
            COALESCE(g.orders, 0) + COALESCE(gj.orders, 0) as total_orders,
            COALESCE(g.sales, 0) as grab_sales,
            COALESCE(gj.sales, 0) as gojek_sales,
            COALESCE(g.orders, 0) as grab_orders,
            COALESCE(gj.orders, 0) as gojek_orders,
            
            -- ОПЕРАЦИОННЫЕ ПРОБЛЕМЫ
            COALESCE(g.store_is_closed, 0) as grab_closed,
            COALESCE(gj.store_is_closed, 0) as gojek_closed,
            COALESCE(g.out_of_stock, 0) as grab_out_of_stock,
            COALESCE(gj.out_of_stock, 0) as gojek_out_of_stock,
            COALESCE(g.store_is_busy, 0) as grab_busy,
            COALESCE(gj.store_is_busy, 0) as gojek_busy,
            
            -- ОТМЕНЫ И КАЧЕСТВО
            COALESCE(g.cancelled_orders, 0) as grab_cancelled,
            COALESCE(gj.cancelled_orders, 0) as gojek_cancelled,
            COALESCE(g.rating, gj.rating, 4.0) as rating,
            
            -- МАРКЕТИНГ
            COALESCE(g.ads_spend, 0) + COALESCE(gj.ads_spend, 0) as total_ads_spend,
            COALESCE(g.ads_sales, 0) + COALESCE(gj.ads_sales, 0) as total_ads_sales,
            
            -- ВРЕМЯ
            COALESCE(gj.preparation_time, '00:00:00') as preparation_time,
            COALESCE(gj.delivery_time, '00:00:00') as delivery_time,
            
            -- КАЛЕНДАРНЫЕ ДАННЫЕ
            CAST(strftime('%w', g.stat_date) AS INTEGER) as day_of_week,
            CAST(strftime('%d', g.stat_date) AS INTEGER) as day_of_month,
            CAST(strftime('%m', g.stat_date) AS INTEGER) as month
            
        FROM grab_stats g
        LEFT JOIN gojek_stats gj ON g.restaurant_id = gj.restaurant_id 
                                 AND g.stat_date = gj.stat_date
        LEFT JOIN restaurants r ON g.restaurant_id = r.id
        WHERE r.name = '{restaurant_name}'
        AND g.stat_date = '{target_date}'
        """
        
        df = pd.read_sql_query(query, conn)
        conn.close()
        
        if len(df) > 0:
            row = df.iloc[0]
            print(f"📊 Данные на {target_date}:")
            print(f"   💰 Общие продажи: {row['total_sales']:,.0f} IDR")
            print(f"   📦 Общие заказы: {row['total_orders']}")
            print(f"   🟢 Grab: {row['grab_sales']:,.0f} IDR ({row['grab_orders']} заказов)")
            print(f"   🟠 Gojek: {row['gojek_sales']:,.0f} IDR ({row['gojek_orders']} заказов)")
            return row
        else:
            return None
            
    def _get_context_data(self, restaurant_name, target_date):
        """Получает контекстные данные за период вокруг даты"""
        
        target_dt = datetime.strptime(target_date, '%Y-%m-%d')
        start_date = (target_dt - timedelta(days=30)).strftime('%Y-%m-%d')
        end_date = (target_dt + timedelta(days=7)).strftime('%Y-%m-%d')
        
        conn = sqlite3.connect(self.db_path)
        
        query = f"""
        SELECT 
            g.stat_date,
            COALESCE(g.sales, 0) + COALESCE(gj.sales, 0) as total_sales,
            COALESCE(g.orders, 0) + COALESCE(gj.orders, 0) as total_orders,
            COALESCE(g.ads_spend, 0) + COALESCE(gj.ads_spend, 0) as total_ads_spend,
            CAST(strftime('%w', g.stat_date) AS INTEGER) as day_of_week
        FROM grab_stats g
        LEFT JOIN gojek_stats gj ON g.restaurant_id = gj.restaurant_id 
                                 AND g.stat_date = gj.stat_date
        LEFT JOIN restaurants r ON g.restaurant_id = r.id
        WHERE r.name = '{restaurant_name}'
        AND g.stat_date >= '{start_date}'
        AND g.stat_date <= '{end_date}'
        AND (g.sales > 0 OR gj.sales > 0)
        ORDER BY g.stat_date
        """
        
        df = pd.read_sql_query(query, conn)
        conn.close()
        
        if len(df) > 0:
            avg_sales = df['total_sales'].mean()
            avg_orders = df['total_orders'].mean()
            
            print(f"📈 КОНТЕКСТ (30 дней до и 7 дней после):")
            print(f"   📊 Средние продажи: {avg_sales:,.0f} IDR")
            print(f"   📦 Средние заказы: {avg_orders:.0f}")
            print(f"   📅 Дней в анализе: {len(df)}")
            
            return {
                'avg_sales': avg_sales,
                'avg_orders': avg_orders,
                'data': df
            }
        else:
            return None
            
    def _analyze_date_factors(self, day_data, context_data, target_date, restaurant_name):
        """Анализирует факторы влияния на конкретную дату"""
        
        analysis = {
            'date': target_date,
            'restaurant': restaurant_name,
            'sales': day_data['total_sales'],
            'orders': day_data['total_orders'],
            'factors': [],
            'impact_score': 0
        }
        
        if context_data:
            analysis['avg_sales'] = context_data['avg_sales']
            analysis['drop_percent'] = ((day_data['total_sales'] - context_data['avg_sales']) / context_data['avg_sales']) * 100
            
            print(f"📉 ОТКЛОНЕНИЕ ОТ СРЕДНЕГО: {analysis['drop_percent']:+.1f}%")
        
        # ФАКТОР 1: Операционные проблемы
        if day_data['grab_closed'] > 0:
            analysis['factors'].append("🚨 Ресторан был закрыт на Grab")
            analysis['impact_score'] += 30
            
        if day_data['gojek_closed'] > 0:
            analysis['factors'].append("🚨 Ресторан был закрыт на Gojek")
            analysis['impact_score'] += 30
            
        if day_data['grab_out_of_stock'] > 0:
            analysis['factors'].append("📦 Нехватка товара на Grab")
            analysis['impact_score'] += 20
            
        if day_data['gojek_out_of_stock'] > 0:
            analysis['factors'].append("📦 Нехватка товара на Gojek")
            analysis['impact_score'] += 20
            
        if day_data['grab_busy'] > 0:
            analysis['factors'].append("🚨 Ресторан перегружен на Grab")
            analysis['impact_score'] += 15
            
        if day_data['gojek_busy'] > 0:
            analysis['factors'].append("🚨 Ресторан перегружен на Gojek")
            analysis['impact_score'] += 15
        
        # ФАКТОР 2: Праздники
        if target_date in self.holidays_data:
            holiday = self.holidays_data[target_date]
            analysis['factors'].append(f"🎉 {holiday['name']} ({holiday.get('category', 'Праздник')})")
            
            # Разные праздники влияют по-разному
            impact_scores = {
                'balinese': 25,
                'muslim': 20,
                'indonesian': 15,
                'international': 10,
                'chinese': 5
            }
            holiday_type = holiday.get('type', 'unknown')
            analysis['impact_score'] += impact_scores.get(holiday_type, 10)
        
        # ФАКТОР 3: Погода
        weather_impact = self._get_weather_impact(target_date, restaurant_name)
        if weather_impact:
            analysis['factors'].append(weather_impact['description'])
            analysis['impact_score'] += weather_impact['impact_score']
        
        # ФАКТОР 4: День недели
        weekdays = ['Воскресенье', 'Понедельник', 'Вторник', 'Среда', 'Четверг', 'Пятница', 'Суббота']
        day_name = weekdays[day_data['day_of_week']]
        analysis['day_of_week'] = day_name
        
        if day_data['day_of_week'] in [0, 1]:  # Воскресенье, Понедельник
            analysis['factors'].append(f"📅 Слабый день недели ({day_name})")
            analysis['impact_score'] += 5
        
        # ФАКТОР 5: Маркетинг
        if context_data:
            avg_ads = context_data['data']['total_ads_spend'].mean()
            if day_data['total_ads_spend'] < avg_ads * 0.5:
                analysis['factors'].append("📱 Низкий рекламный бюджет")
                analysis['impact_score'] += 10
        
        # ФАКТОР 6: Качество
        if day_data['rating'] < 4.0:
            analysis['factors'].append(f"⭐ Низкий рейтинг: {day_data['rating']:.1f}")
            analysis['impact_score'] += 15
        
        # ФАКТОР 7: Отмены
        total_cancelled = day_data['grab_cancelled'] + day_data['gojek_cancelled']
        if day_data['total_orders'] > 0:
            cancel_rate = (total_cancelled / day_data['total_orders']) * 100
            if cancel_rate > 15:
                analysis['factors'].append(f"❌ Много отмен: {cancel_rate:.1f}%")
                analysis['impact_score'] += 10
        
        if not analysis['factors']:
            analysis['factors'].append("❓ Внешние факторы (конкуренты, локальные события)")
        
        return analysis
        
    def _get_weather_impact(self, date, restaurant_name):
        """Получает влияние погоды на дату"""
        
        if restaurant_name not in self.restaurant_locations:
            return None
            
        location = self.restaurant_locations[restaurant_name]
        lat, lon = location['lat'], location['lon']
        
        weather_data = self._fetch_weather_data(date, lat, lon)
        if not weather_data:
            return None
            
        precipitation = weather_data.get('precipitation', 0)
        temperature = weather_data.get('temperature', 25)
        
        impact_score = 0
        descriptions = []
        
        # Анализ дождя
        if precipitation > 10:
            descriptions.append(f"🌧️ Сильный дождь ({precipitation:.1f}мм)")
            impact_score += 25
        elif precipitation > 5:
            descriptions.append(f"🌦️ Умеренный дождь ({precipitation:.1f}мм)")
            impact_score += 15
        elif precipitation > 1:
            descriptions.append(f"☔ Небольшой дождь ({precipitation:.1f}мм)")
            impact_score += 5
            
        # Анализ температуры
        if temperature > 35:
            descriptions.append(f"🔥 Очень жарко ({temperature:.1f}°C)")
            impact_score += 10
        elif temperature < 20:
            descriptions.append(f"❄️ Прохладно ({temperature:.1f}°C)")
            impact_score += 5
            
        if descriptions:
            return {
                'description': " + ".join(descriptions),
                'impact_score': impact_score,
                'precipitation': precipitation,
                'temperature': temperature
            }
            
        return None
        
    def _fetch_weather_data(self, date, lat, lon):
        """Получает данные погоды из Open-Meteo API"""
        
        try:
            url = "https://archive-api.open-meteo.com/v1/archive"
            params = {
                'latitude': lat,
                'longitude': lon,
                'start_date': date,
                'end_date': date,
                'daily': 'precipitation_sum,temperature_2m_mean',
                'timezone': 'Asia/Jakarta'
            }
            
            print(f"🌤️ Запрашиваю погоду для {date}...")
            response = requests.get(url, params=params, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                daily = data.get('daily', {})
                
                if daily.get('precipitation_sum') and daily.get('temperature_2m_mean'):
                    weather = {
                        'precipitation': daily['precipitation_sum'][0],
                        'temperature': daily['temperature_2m_mean'][0],
                        'source': 'Open-Meteo API'
                    }
                    
                    print(f"   ☔ Осадки: {weather['precipitation']:.1f}мм")
                    print(f"   🌡️ Температура: {weather['temperature']:.1f}°C")
                    
                    return weather
                    
        except Exception as e:
            print(f"   ⚠️ Ошибка получения погоды: {e}")
            
        return None
        
    def _generate_date_report(self, analysis, restaurant_name, target_date):
        """Генерирует отчет по конкретной дате"""
        
        report = []
        report.append(f"📋 АНАЛИЗ ПАДЕНИЯ ПРОДАЖ: {restaurant_name}")
        report.append(f"📅 ДАТА: {target_date}")
        report.append("═" * 80)
        
        # Основные показатели
        report.append(f"💰 ФИНАНСОВЫЕ ПОКАЗАТЕЛИ:")
        report.append(f"   • Общие продажи: {analysis['sales']:,.0f} IDR")
        report.append(f"   • Общие заказы: {analysis['orders']}")
        
        if 'avg_sales' in analysis:
            report.append(f"   • Среднее за период: {analysis['avg_sales']:,.0f} IDR")
            report.append(f"   • Отклонение: {analysis['drop_percent']:+.1f}%")
            
            if analysis['drop_percent'] < -20:
                report.append(f"   🔴 ЗНАЧИТЕЛЬНОЕ ПАДЕНИЕ!")
            elif analysis['drop_percent'] < -10:
                report.append(f"   🟡 Умеренное снижение")
            elif analysis['drop_percent'] > 10:
                report.append(f"   🟢 Рост продаж")
            else:
                report.append(f"   ⚪ Нормальные показатели")
        
        report.append("")
        
        # День недели
        report.append(f"📅 ДЕНЬ НЕДЕЛИ: {analysis['day_of_week']}")
        report.append("")
        
        # Факторы влияния
        if analysis['factors']:
            report.append(f"🔍 ФАКТОРЫ ВЛИЯНИЯ:")
            for i, factor in enumerate(analysis['factors'], 1):
                report.append(f"   {i}. {factor}")
        else:
            report.append(f"✅ Негативных факторов не выявлено")
            
        report.append("")
        
        # Общий скор влияния
        report.append(f"📊 ОБЩИЙ СКОР ВЛИЯНИЯ: {analysis['impact_score']}")
        
        if analysis['impact_score'] > 50:
            report.append("   🔴 ВЫСОКОЕ негативное влияние")
        elif analysis['impact_score'] > 25:
            report.append("   🟡 СРЕДНЕЕ негативное влияние")
        elif analysis['impact_score'] > 0:
            report.append("   🟢 НИЗКОЕ негативное влияние")
        else:
            report.append("   ✅ Негативного влияния не выявлено")
            
        report.append("")
        report.append("═" * 80)
        report.append("📊 АНАЛИЗ ОСНОВАН НА РЕАЛЬНЫХ ДАННЫХ")
        
        return "\n".join(report)

def main():
    """Анализируем Only Eggs 18 мая 2025"""
    
    analyzer = SpecificDateAnalyzer()
    
    # Анализируем конкретную дату
    result = analyzer.analyze_specific_date("Only Eggs", "2025-05-18")
    
    print("\n" + "="*100)
    print("📋 РЕЗУЛЬТАТ АНАЛИЗА:")
    print("="*100)
    print(result)
    print("="*100)

if __name__ == "__main__":
    main()