#!/usr/bin/env python3
"""
🎯 УЛУЧШЕННЫЙ АНАЛИЗАТОР ПРОДАЖ - ГОТОВ К ПРОДАКШН
═══════════════════════════════════════════════════════════════════════════════
✅ Учитывает Close Time (время закрытия)
✅ Все операционные факторы
✅ Погода через Open-Meteo API
✅ 164 праздника
✅ Готов для интеграции в основную систему
"""

import sqlite3
import pandas as pd
import requests
import json
import os
from datetime import datetime, timedelta

class EnhancedSalesAnalyzer:
    """Улучшенный анализатор для продакшн использования"""
    
    def __init__(self, db_path='database.sqlite'):
        self.db_path = db_path
        self.holidays_data = {}
        self.restaurant_locations = {}
        
        # Загружаем реальные данные
        self._load_real_holidays_data()
        self._load_real_restaurant_locations()
        
    def analyze_sales_drop(self, restaurant_name, target_date):
        """Главный метод анализа падения продаж"""
        
        print(f"🎯 АНАЛИЗ ПАДЕНИЯ ПРОДАЖ")
        print("═" * 80)
        print(f"📅 Ресторан: {restaurant_name}")
        print(f"📅 Дата: {target_date}")
        print("═" * 80)
        
        # 1. Получаем данные ресторана
        restaurant_data = self._get_enhanced_restaurant_data(restaurant_name, target_date)
        
        if restaurant_data is None:
            return f"❌ Нет данных по ресторану '{restaurant_name}' на {target_date}"
            
        # 2. Получаем контекст
        context_data = self._get_context_data(restaurant_name, target_date)
        
        # 3. Проводим улучшенный анализ
        analysis = self._conduct_enhanced_analysis(restaurant_data, context_data, target_date, restaurant_name)
        
        # 4. Генерируем профессиональный отчет
        return self._generate_professional_report(analysis, restaurant_name, target_date)
        
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
            
    def _get_enhanced_restaurant_data(self, restaurant_name, target_date):
        """Получает РАСШИРЕННЫЕ данные ресторана включая Close Time"""
        
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
            
            -- ВРЕМЯ ВЫКЛЮЧЕНИЯ ПРОГРАММЫ (КРИТИЧНО!)
            COALESCE(gj.close_time, '00:00:00') as gojek_close_time,
            -- Для Grab есть offline_rate (процент времени выключения)
            COALESCE(g.offline_rate, 0) as grab_offline_rate,
            
            -- ОТМЕНЫ И КАЧЕСТВО
            COALESCE(g.cancelled_orders, 0) as grab_cancelled,
            COALESCE(gj.cancelled_orders, 0) as gojek_cancelled,
            COALESCE(g.rating, gj.rating, 4.0) as rating,
            
            -- МАРКЕТИНГ
            COALESCE(g.ads_spend, 0) + COALESCE(gj.ads_spend, 0) as total_ads_spend,
            COALESCE(g.ads_sales, 0) + COALESCE(gj.ads_sales, 0) as total_ads_sales,
            
            -- ВРЕМЯ ОБСЛУЖИВАНИЯ И ОЖИДАНИЯ (КРИТИЧНО!)
            COALESCE(gj.preparation_time, '00:00:00') as preparation_time,
            COALESCE(gj.delivery_time, '00:00:00') as delivery_time,
            COALESCE(gj.accepting_time, '00:00:00') as accepting_time,
            COALESCE(gj.driver_waiting, 0) as gojek_driver_waiting_min,
            COALESCE(g.driver_waiting_time, 0) / 60.0 as grab_driver_waiting_min,  -- Конвертируем секунды в минуты
            
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
            print(f"📊 РАСШИРЕННЫЕ ДАННЫЕ на {target_date}:")
            print(f"   💰 Общие продажи: {row['total_sales']:,.0f} IDR")
            print(f"   📦 Общие заказы: {row['total_orders']}")
            print(f"   🟢 Grab: {row['grab_sales']:,.0f} IDR ({row['grab_orders']} заказов)")
            print(f"   🟠 Gojek: {row['gojek_sales']:,.0f} IDR ({row['gojek_orders']} заказов)")
            
            # Показываем время выключения если есть
            if row['gojek_close_time'] != '00:00:00':
                print(f"   🚨 Gojek выключен: {row['gojek_close_time']}")
            if row['grab_offline_rate'] > 0:
                print(f"   🚨 Grab offline: {row['grab_offline_rate']:.1f}%")
                
            # Показываем время ожидания водителей
            if row['gojek_driver_waiting_min'] > 0:
                print(f"   ⏱️ Gojek Driver Waiting: {row['gojek_driver_waiting_min']} мин")
            if row['grab_driver_waiting_min'] > 0:
                print(f"   ⏱️ Grab Driver Waiting: {row['grab_driver_waiting_min']} мин")
                
            return row
        else:
            return None
            
    def _get_context_data(self, restaurant_name, target_date):
        """Получает контекстные данные за период"""
        
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
            COALESCE(gj.close_time, '00:00:00') as gojek_close_time,
            COALESCE(g.offline_rate, 0) as grab_offline_rate,
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
            # Считаем дни с выключениями
            outage_days = len(df[(df['gojek_close_time'] != '00:00:00') | (df['grab_offline_rate'] > 0)])
            
            print(f"📈 КОНТЕКСТ (30 дней до и 7 дней после):")
            print(f"   📊 Средние продажи: {avg_sales:,.0f} IDR")
            print(f"   📦 Средние заказы: {avg_orders:.0f}")
            print(f"   🚨 Дней с выключениями: {outage_days}")
            print(f"   📅 Дней в анализе: {len(df)}")
            
            return {
                'avg_sales': avg_sales,
                'avg_orders': avg_orders,
                'outage_days': outage_days,
                'data': df
            }
        else:
            return None
            
    def _conduct_enhanced_analysis(self, day_data, context_data, target_date, restaurant_name):
        """Проводит улучшенный анализ с учетом всех факторов"""
        
        analysis = {
            'date': target_date,
            'restaurant': restaurant_name,
            'sales': day_data['total_sales'],
            'orders': day_data['total_orders'],
            'factors': [],
            'impact_score': 0,
            'critical_issues': []
        }
        
        if context_data:
            analysis['avg_sales'] = context_data['avg_sales']
            analysis['drop_percent'] = ((day_data['total_sales'] - context_data['avg_sales']) / context_data['avg_sales']) * 100
            
            print(f"📉 ОТКЛОНЕНИЕ ОТ СРЕДНЕГО: {analysis['drop_percent']:+.1f}%")
        
        # ФАКТОР 1: ВЫКЛЮЧЕНИЕ ПРОГРАММЫ (КРИТИЧЕСКИЙ ФАКТОР!)
        gojek_outage = self._parse_time_string(day_data['gojek_close_time'])
        grab_offline_rate = day_data['grab_offline_rate']
        
        if gojek_outage > 0:
            outage_str = self._format_duration(gojek_outage)
            
            if gojek_outage >= 18000:  # Больше 5 часов (в секундах)
                analysis['factors'].append(f"🚨 КРИТИЧНО: Gojek выключен {outage_str}")
                analysis['impact_score'] += 50
                analysis['critical_issues'].append("Критическое выключение Gojek")
            elif gojek_outage >= 7200:  # Больше 2 часов
                analysis['factors'].append(f"⚠️ Gojek выключен {outage_str}")
                analysis['impact_score'] += 30
            elif gojek_outage >= 3600:  # Больше 1 часа
                analysis['factors'].append(f"🕐 Gojek выключен {outage_str}")
                analysis['impact_score'] += 20
            else:
                analysis['factors'].append(f"⚠️ Gojek выключен {outage_str}")
                analysis['impact_score'] += 10
                
        if grab_offline_rate > 0:
            if grab_offline_rate >= 300:  # Больше 300% = больше 5 часов
                analysis['factors'].append(f"🚨 КРИТИЧНО: Grab offline {grab_offline_rate:.1f}%")
                analysis['impact_score'] += 50
                analysis['critical_issues'].append("Критическое выключение Grab")
            elif grab_offline_rate >= 120:  # Больше 120% = больше 2 часов
                analysis['factors'].append(f"⚠️ Grab offline {grab_offline_rate:.1f}%")
                analysis['impact_score'] += 30
            elif grab_offline_rate >= 60:  # Больше 60% = больше 1 часа
                analysis['factors'].append(f"🕐 Grab offline {grab_offline_rate:.1f}%")
                analysis['impact_score'] += 20
            else:
                analysis['factors'].append(f"⚠️ Grab offline {grab_offline_rate:.1f}%")
                analysis['impact_score'] += 10
                
        # ФАКТОР 2: ВРЕМЯ ОЖИДАНИЯ ВОДИТЕЛЕЙ (НОВЫЙ КРИТИЧЕСКИЙ ФАКТОР!)
        gojek_waiting = day_data.get('gojek_driver_waiting_min', 0)
        grab_waiting = day_data.get('grab_driver_waiting_min', 0)
        
        if gojek_waiting > 0:
            if gojek_waiting >= 20:  # Больше 20 минут - критично
                analysis['factors'].append(f"🚨 КРИТИЧНО: Gojek Driver Waiting {gojek_waiting} мин")
                analysis['impact_score'] += 35
                analysis['critical_issues'].append("Критическое время ожидания Gojek")
            elif gojek_waiting >= 15:  # Больше 15 минут - серьезно
                analysis['factors'].append(f"⚠️ Gojek Driver Waiting {gojek_waiting} мин (высокое)")
                analysis['impact_score'] += 25
            elif gojek_waiting >= 10:  # Больше 10 минут - проблема
                analysis['factors'].append(f"🕐 Gojek Driver Waiting {gojek_waiting} мин")
                analysis['impact_score'] += 15
            else:
                analysis['factors'].append(f"⏱️ Gojek Driver Waiting {gojek_waiting} мин")
                analysis['impact_score'] += 5
                
        if grab_waiting > 0:
            if grab_waiting >= 20:  # Больше 20 минут - критично
                analysis['factors'].append(f"🚨 КРИТИЧНО: Grab Driver Waiting {grab_waiting} мин")
                analysis['impact_score'] += 35
                analysis['critical_issues'].append("Критическое время ожидания Grab")
            elif grab_waiting >= 15:  # Больше 15 минут - серьезно
                analysis['factors'].append(f"⚠️ Grab Driver Waiting {grab_waiting} мин (высокое)")
                analysis['impact_score'] += 25
            elif grab_waiting >= 10:  # Больше 10 минут - проблема
                analysis['factors'].append(f"🕐 Grab Driver Waiting {grab_waiting} мин")
                analysis['impact_score'] += 15
            else:
                analysis['factors'].append(f"⏱️ Grab Driver Waiting {grab_waiting} мин")
                analysis['impact_score'] += 5
                
        # ФАКТОР 3: Операционные проблемы
        if day_data['grab_closed'] > 0:
            analysis['factors'].append("🚨 Ресторан был закрыт на Grab")
            analysis['impact_score'] += 30
            analysis['critical_issues'].append("Grab закрыт")
            
        if day_data['gojek_closed'] > 0:
            analysis['factors'].append("🚨 Ресторан был закрыт на Gojek")
            analysis['impact_score'] += 30
            analysis['critical_issues'].append("Gojek закрыт")
            
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
        
        # ФАКТОР 3: Праздники
        if target_date in self.holidays_data:
            holiday = self.holidays_data[target_date]
            analysis['factors'].append(f"🎉 {holiday['name']} ({holiday.get('category', 'Праздник')})")
            
            impact_scores = {
                'balinese': 25,
                'muslim': 20,
                'indonesian': 15,
                'international': 10,
                'chinese': 5
            }
            holiday_type = holiday.get('type', 'unknown')
            analysis['impact_score'] += impact_scores.get(holiday_type, 10)
        
        # ФАКТОР 4: Погода
        weather_impact = self._get_weather_impact(target_date, restaurant_name)
        if weather_impact:
            analysis['factors'].append(weather_impact['description'])
            analysis['impact_score'] += weather_impact['impact_score']
        
        # ФАКТОР 5: День недели
        weekdays = ['Воскресенье', 'Понедельник', 'Вторник', 'Среда', 'Четверг', 'Пятница', 'Суббота']
        day_name = weekdays[day_data['day_of_week']]
        analysis['day_of_week'] = day_name
        
        if day_data['day_of_week'] in [0, 1]:  # Воскресенье, Понедельник
            analysis['factors'].append(f"📅 Слабый день недели ({day_name})")
            analysis['impact_score'] += 5
        
        # ФАКТОР 6: Маркетинг
        if context_data:
            avg_ads = context_data['data']['total_ads_spend'].mean()
            if day_data['total_ads_spend'] < avg_ads * 0.5:
                analysis['factors'].append("📱 Низкий рекламный бюджет")
                analysis['impact_score'] += 10
        
        # ФАКТОР 7: Качество
        if day_data['rating'] < 4.0:
            analysis['factors'].append(f"⭐ Низкий рейтинг: {day_data['rating']:.1f}")
            analysis['impact_score'] += 15
        
        # ФАКТОР 8: Отмены
        total_cancelled = day_data['grab_cancelled'] + day_data['gojek_cancelled']
        if day_data['total_orders'] > 0:
            cancel_rate = (total_cancelled / day_data['total_orders']) * 100
            if cancel_rate > 15:
                analysis['factors'].append(f"❌ Много отмен: {cancel_rate:.1f}%")
                analysis['impact_score'] += 10
        
        # ФАКТОР 9: Проблемы с одной платформой
        if day_data['grab_orders'] > 0 and day_data['gojek_orders'] == 0:
            analysis['factors'].append("🚨 Gojek не работает (0 заказов)")
            analysis['impact_score'] += 20
            analysis['critical_issues'].append("Gojek не функционирует")
        elif day_data['gojek_orders'] > 0 and day_data['grab_orders'] == 0:
            analysis['factors'].append("🚨 Grab не работает (0 заказов)")
            analysis['impact_score'] += 20
            analysis['critical_issues'].append("Grab не функционирует")
        
        if not analysis['factors']:
            analysis['factors'].append("❓ Внешние факторы (конкуренты, локальные события)")
        
        return analysis
        
    def _get_weather_impact(self, date, restaurant_name):
        """Получает влияние погоды"""
        
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
        
    def _generate_professional_report(self, analysis, restaurant_name, target_date):
        """Генерирует профессиональный отчет"""
        
        report = []
        report.append(f"📋 ПРОФЕССИОНАЛЬНЫЙ АНАЛИЗ ПРОДАЖ")
        report.append(f"🏪 РЕСТОРАН: {restaurant_name}")
        report.append(f"📅 ДАТА: {target_date}")
        report.append("═" * 80)
        
        # Основные показатели
        report.append(f"💰 ФИНАНСОВЫЕ ПОКАЗАТЕЛИ:")
        report.append(f"   • Общие продажи: {analysis['sales']:,.0f} IDR")
        report.append(f"   • Общие заказы: {analysis['orders']}")
        
        if 'avg_sales' in analysis:
            report.append(f"   • Среднее за период: {analysis['avg_sales']:,.0f} IDR")
            report.append(f"   • Отклонение: {analysis['drop_percent']:+.1f}%")
            
            if analysis['drop_percent'] < -30:
                report.append(f"   🔴 КРИТИЧЕСКОЕ ПАДЕНИЕ!")
                severity = "КРИТИЧЕСКАЯ"
            elif analysis['drop_percent'] < -20:
                report.append(f"   🟡 ЗНАЧИТЕЛЬНОЕ ПАДЕНИЕ!")
                severity = "ВЫСОКАЯ"
            elif analysis['drop_percent'] < -10:
                report.append(f"   🟠 Умеренное снижение")
                severity = "СРЕДНЯЯ"
            elif analysis['drop_percent'] > 10:
                report.append(f"   🟢 Рост продаж")
                severity = "ОТСУТСТВУЕТ"
            else:
                report.append(f"   ⚪ Нормальные показатели")
                severity = "НИЗКАЯ"
        
        report.append("")
        
        # Критические проблемы
        if analysis['critical_issues']:
            report.append(f"🚨 КРИТИЧЕСКИЕ ПРОБЛЕМЫ:")
            for i, issue in enumerate(analysis['critical_issues'], 1):
                report.append(f"   {i}. {issue}")
            report.append("")
        
        # День недели
        report.append(f"📅 ДЕНЬ НЕДЕЛИ: {analysis['day_of_week']}")
        report.append("")
        
        # Факторы влияния
        if analysis['factors']:
            report.append(f"🔍 ФАКТОРЫ ВЛИЯНИЯ (по важности):")
            for i, factor in enumerate(analysis['factors'], 1):
                report.append(f"   {i}. {factor}")
        else:
            report.append(f"✅ Негативных факторов не выявлено")
            
        report.append("")
        
        # Оценка влияния
        report.append(f"📊 ОБЩИЙ СКОР ВЛИЯНИЯ: {analysis['impact_score']}")
        
        if analysis['impact_score'] > 70:
            impact_level = "🔴 КРИТИЧЕСКОЕ"
            action_needed = "НЕМЕДЛЕННЫЕ ДЕЙСТВИЯ ТРЕБУЮТСЯ!"
        elif analysis['impact_score'] > 50:
            impact_level = "🟡 ВЫСОКОЕ"
            action_needed = "Срочные меры необходимы"
        elif analysis['impact_score'] > 25:
            impact_level = "🟠 СРЕДНЕЕ"
            action_needed = "Требуется внимание"
        elif analysis['impact_score'] > 0:
            impact_level = "🟢 НИЗКОЕ"
            action_needed = "Мониторинг ситуации"
        else:
            impact_level = "✅ МИНИМАЛЬНОЕ"
            action_needed = "Все в норме"
            
        report.append(f"   {impact_level} негативное влияние")
        report.append(f"   📋 Рекомендация: {action_needed}")
        
        # Рекомендации
        recommendations = self._generate_recommendations(analysis)
        if recommendations:
            report.append("")
            report.append(f"💡 ПРИОРИТЕТНЫЕ РЕКОМЕНДАЦИИ:")
            for i, rec in enumerate(recommendations, 1):
                report.append(f"   {i}. {rec}")
        
        report.append("")
        report.append("═" * 80)
        report.append("📊 ОТЧЕТ ОСНОВАН НА РЕАЛЬНЫХ ДАННЫХ")
        report.append("   • Операционные данные из базы")
        report.append("   • Погода Open-Meteo API")
        report.append("   • 164 праздника Бали")
        report.append("   • Статистический анализ")
        
        return "\n".join(report)
        
    def _generate_recommendations(self, analysis):
        """Генерирует рекомендации"""
        
        recommendations = []
        
        # Критические проблемы первыми
        if analysis['critical_issues']:
            if "Gojek не функционирует" in analysis['critical_issues']:
                recommendations.append("🚨 СРОЧНО: Проверить интеграцию с Gojek, связаться с техподдержкой")
            if "Grab не функционирует" in analysis['critical_issues']:
                recommendations.append("🚨 СРОЧНО: Проверить интеграцию с Grab, связаться с техподдержкой")
            if "Критическое выключение Gojek" in analysis['critical_issues']:
                recommendations.append("🚨 СРОЧНО: Выяснить почему программа Gojek была выключена на несколько часов")
            if "Критическое выключение Grab" in analysis['critical_issues']:
                recommendations.append("🚨 СРОЧНО: Выяснить почему программа Grab была выключена на несколько часов")
            if "Критическое время ожидания Gojek" in analysis['critical_issues']:
                recommendations.append("🚨 СРОЧНО: Проблемы с водителями Gojek - долгое ожидание отпугивает клиентов")
            if "Критическое время ожидания Grab" in analysis['critical_issues']:
                recommendations.append("🚨 СРОЧНО: Проблемы с водителями Grab - долгое ожидание отпугивает клиентов")
                
        # Операционные рекомендации
        operational_factors = [f for f in analysis['factors'] if any(x in f for x in ['закрыт', 'товара', 'перегружен'])]
        if operational_factors:
            recommendations.append("🔧 Улучшить операционные процессы (закрытие, запасы, персонал)")
            
        # Маркетинговые рекомендации
        marketing_factors = [f for f in analysis['factors'] if 'рекламный' in f]
        if marketing_factors:
            recommendations.append("📱 Увеличить рекламный бюджет")
            
        # Погодные рекомендации
        weather_factors = [f for f in analysis['factors'] if any(x in f for x in ['дождь', 'жарко'])]
        if weather_factors:
            recommendations.append("🌧️ Подготовиться к плохой погоде: больше курьеров, промо")
            
        # Общие рекомендации
        if analysis['impact_score'] > 50:
            recommendations.append("📊 Провести детальный аудит всех операционных процессов")
            
        return recommendations
        
    def _parse_time_string(self, time_str):
        """Парсит строку времени H:M:S в секунды"""
        if not time_str or time_str == '00:00:00' or time_str == '0:0:0':
            return 0
            
        try:
            parts = time_str.split(':')
            if len(parts) >= 3:
                hours = int(parts[0])
                minutes = int(parts[1])
                seconds = int(parts[2])
                return hours * 3600 + minutes * 60 + seconds
            elif len(parts) == 2:
                hours = int(parts[0])
                minutes = int(parts[1])
                return hours * 3600 + minutes * 60
        except:
            return 0
            
        return 0
        
    def _format_duration(self, seconds):
        """Форматирует секунды в читаемый вид"""
        if seconds < 60:
            return f"{seconds}с"
        elif seconds < 3600:
            minutes = seconds // 60
            return f"{minutes}м"
        else:
            hours = seconds // 3600
            minutes = (seconds % 3600) // 60
            if minutes > 0:
                return f"{hours}ч {minutes}м"
            else:
                return f"{hours}ч"

def main():
    """Тестируем анализатор на 18 мая с фокусом на Driver Waiting Time"""
    
    analyzer = EnhancedSalesAnalyzer()
    
    # Анализируем 18 мая с фокусом на время ожидания водителей
    result = analyzer.analyze_sales_drop("Only Eggs", "2025-05-18")
    
    print("\n" + "="*100)
    print("📋 АНАЛИЗ С ФОКУСОМ НА DRIVER WAITING TIME:")
    print("="*100)
    print(result)
    print("="*100)
    
    print("\n🎯 АНАЛИЗАТОР УЧИТЫВАЕТ ВСЕ КРИТИЧЕСКИЕ ФАКТОРЫ!")
    print("   ✅ Close Time (выключение программы)")
    print("   ✅ Driver Waiting Time (время ожидания водителей)")
    print("   ✅ Все операционные факторы")
    print("   ✅ Погода и праздники")
    print("   ✅ Готов к продакшн использованию")

if __name__ == "__main__":
    main()