#!/usr/bin/env python3
"""
🎯 АНАЛИЗАТОР ЦЕЛЕВОЙ АУДИТОРИИ ПО СТРАНАМ
═══════════════════════════════════════════════════════════════════════════════
Определяет из каких стран приезжают клиенты ресторанов на основе корреляции
продаж с туристическими потоками
"""

import pandas as pd
import numpy as np
import sqlite3
from datetime import datetime
import json
from scipy.stats import pearsonr
import warnings
warnings.filterwarnings('ignore')

class TargetAudienceAnalyzer:
    """Анализатор целевой аудитории ресторанов по странам"""
    
    def __init__(self, db_path='database.sqlite'):
        self.db_path = db_path
        self.tourist_data_2024 = None
        self.tourist_data_2025 = None
        self.country_mapping = {
            'AUSTRALIAN': 'Australia',
            'CHINA': 'China', 
            'INDIA': 'India',
            'JAPAN': 'Japan',
            'KOREAN SOUTH': 'South Korea',
            'MALAYSIAN': 'Malaysia',
            'SINGAPORE': 'Singapore',
            'THAILAND': 'Thailand',
            'VIETNAM': 'Vietnam',
            'RUSSIAN': 'Russia',
            'AMERICAN': 'USA',
            'GERMAN': 'Germany',
            'FRENCH': 'France',
            'BRITISH': 'United Kingdom',
            'DUTCH': 'Netherlands'
        }
        
    def load_tourist_data(self):
        """Загружает туристические данные из XLS файлов"""
        
        print("🌍 Загружаем туристические данные по странам...")
        
        try:
            # Загружаем 2024
            df_2024 = pd.read_excel('Kunjungan_Wisatawan_Bali_2024.xls', engine='xlrd', header=None)
            self.tourist_data_2024 = self._parse_tourist_data(df_2024, 2024)
            
            # Загружаем 2025  
            df_2025 = pd.read_excel('Kunjungan_Wisatawan_Bali_2025.xls', engine='xlrd', header=None)
            self.tourist_data_2025 = self._parse_tourist_data(df_2025, 2025)
            
            print(f"✅ Загружено данных 2024: {len(self.tourist_data_2024)} стран")
            print(f"✅ Загружено данных 2025: {len(self.tourist_data_2025)} стран")
            
            return True
            
        except Exception as e:
            print(f"❌ Ошибка загрузки туристических данных: {e}")
            return False
    
    def _parse_tourist_data(self, df, year):
        """Парсит туристические данные из DataFrame"""
        
        tourist_data = {}
        
        # Ищем страны и их данные
        for i, row in df.iterrows():
            for j, cell in enumerate(row):
                if pd.notna(cell):
                    cell_str = str(cell).upper()
                    
                    # Проверяем если это название страны
                    for country_key, country_name in self.country_mapping.items():
                        if country_key in cell_str and len(cell_str) < 50:
                            
                            # Собираем числовые данные справа от названия страны
                            monthly_data = []
                            for k in range(j + 1, min(len(row), j + 13)):  # Максимум 12 месяцев
                                val = row.iloc[k] if hasattr(row, 'iloc') else row[k]
                                if pd.notna(val) and str(val).replace('.', '').replace(',', '').replace('-', '').isdigit():
                                    monthly_data.append(int(float(val)))
                            
                            if len(monthly_data) >= 3:  # Минимум 3 месяца данных
                                tourist_data[country_name] = {
                                    'monthly_data': monthly_data,
                                    'total': sum(monthly_data),
                                    'average_monthly': sum(monthly_data) / len(monthly_data),
                                    'raw_name': cell_str,
                                    'year': year
                                }
                            break
        
        return tourist_data
    
    def analyze_restaurant_target_audience(self, restaurant_name, start_date=None, end_date=None):
        """Анализирует целевую аудиторию конкретного ресторана"""
        
        if not self.tourist_data_2024 and not self.load_tourist_data():
            return None
            
        print(f"🎯 Анализ целевой аудитории для ресторана: {restaurant_name}")
        print("=" * 60)
        
        # Загружаем данные ресторана
        restaurant_sales = self._get_restaurant_monthly_sales(restaurant_name, start_date, end_date)
        
        if restaurant_sales is None or len(restaurant_sales) < 3:
            return {
                'error': 'Недостаточно данных по продажам ресторана',
                'restaurant': restaurant_name
            }
        
        # Анализируем корреляции с туристическими потоками
        correlations = self._calculate_country_correlations(restaurant_sales)
        
        # Определяем целевую аудиторию
        target_audience = self._determine_target_audience(correlations, restaurant_sales)
        
        return target_audience
    
    def _get_restaurant_monthly_sales(self, restaurant_name, start_date=None, end_date=None):
        """Получает месячные продажи ресторана"""
        
        try:
            conn = sqlite3.connect(self.db_path)
            
            # Получаем ID ресторана
            restaurant_query = "SELECT id FROM restaurants WHERE LOWER(name) LIKE ?"
            restaurant_result = pd.read_sql_query(restaurant_query, conn, params=[f'%{restaurant_name.lower()}%'])
            
            if restaurant_result.empty:
                print(f"❌ Ресторан '{restaurant_name}' не найден")
                return None
                
                                      restaurant_id = int(restaurant_result.iloc[0]['id'])
             
             # Получаем продажи по месяцам (объединяем Grab + Gojek)
             sales_query = """
                 SELECT 
                     strftime('%Y-%m', stat_date) as month,
                     SUM(COALESCE(grab_sales, 0) + COALESCE(gojek_sales, 0)) as total_sales
                 FROM (
                     SELECT stat_date, sales as grab_sales, 0 as gojek_sales
                     FROM grab_stats 
                     WHERE restaurant_id = ?
                     UNION ALL
                     SELECT stat_date, 0 as grab_sales, sales as gojek_sales
                     FROM gojek_stats 
                     WHERE restaurant_id = ?
                 ) combined_data
                 GROUP BY strftime('%Y-%m', stat_date)
                 HAVING SUM(COALESCE(grab_sales, 0) + COALESCE(gojek_sales, 0)) > 0
                 ORDER BY month
             """
            
            sales_data = pd.read_sql_query(sales_query, conn, params=[restaurant_id, restaurant_id])
            conn.close()
            
            if sales_data.empty:
                return None
                
            # Преобразуем в месячные данные
            monthly_sales = {}
            for _, row in sales_data.iterrows():
                year_month = row['month']
                year, month = year_month.split('-')
                month_key = f"{year}-{int(month):02d}"
                monthly_sales[month_key] = float(row['total_sales'])
            
            print(f"📊 Найдено данных по продажам: {len(monthly_sales)} месяцев")
            print(f"📅 Период: {min(monthly_sales.keys())} - {max(monthly_sales.keys())}")
            
            return monthly_sales
            
        except Exception as e:
            print(f"❌ Ошибка получения данных ресторана: {e}")
            return None
    
    def _calculate_country_correlations(self, restaurant_sales):
        """Вычисляет корреляции продаж ресторана с туристическими потоками по странам"""
        
        correlations = {}
        
        # Объединяем данные 2024 и 2025
        all_tourist_data = {}
        if self.tourist_data_2024:
            all_tourist_data.update(self.tourist_data_2024)
        if self.tourist_data_2025:
            # Объединяем с данными 2024
            for country, data_2025 in self.tourist_data_2025.items():
                if country in all_tourist_data:
                    # Добавляем данные 2025 к данным 2024
                    all_tourist_data[country]['monthly_data'].extend(data_2025['monthly_data'])
                    all_tourist_data[country]['total'] += data_2025['total']
                else:
                    all_tourist_data[country] = data_2025
        
        # Для каждой страны вычисляем корреляцию
        for country, tourist_data in all_tourist_data.items():
            
            # Создаем временные ряды для сравнения
            tourist_monthly = tourist_data['monthly_data']
            
            # Синхронизируем данные ресторана с туристическими данными
            sales_values = []
            tourist_values = []
            
            # Простейший случай - берем последовательные месяцы
            if len(tourist_monthly) >= len(restaurant_sales):
                # Берем последние N месяцев туристических данных
                tourist_subset = tourist_monthly[-len(restaurant_sales):]
                sales_subset = list(restaurant_sales.values())
            else:
                # Берем первые N месяцев продаж
                sales_subset = list(restaurant_sales.values())[:len(tourist_monthly)]
                tourist_subset = tourist_monthly
            
            if len(sales_subset) >= 3 and len(tourist_subset) >= 3 and len(sales_subset) == len(tourist_subset):
                try:
                    correlation, p_value = pearsonr(sales_subset, tourist_subset)
                    
                    correlations[country] = {
                        'correlation': correlation,
                        'p_value': p_value,
                        'significance': 'высокая' if p_value < 0.05 else 'средняя' if p_value < 0.1 else 'низкая',
                        'strength': 'сильная' if abs(correlation) > 0.7 else 'умеренная' if abs(correlation) > 0.4 else 'слабая',
                        'total_tourists': tourist_data['total'],
                        'direction': 'положительная' if correlation > 0 else 'отрицательная'
                    }
                    
                except Exception as e:
                    print(f"⚠️ Ошибка расчета корреляции для {country}: {e}")
        
        return correlations
    
    def _determine_target_audience(self, correlations, restaurant_sales):
        """Определяет целевую аудиторию на основе корреляций"""
        
        # Сортируем страны по силе корреляции
        sorted_countries = sorted(
            correlations.items(), 
            key=lambda x: abs(x[1]['correlation']), 
            reverse=True
        )
        
        # Фильтруем только положительные и значимые корреляции
        target_countries = []
        for country, data in sorted_countries:
            if (data['correlation'] > 0.3 and  # Умеренная положительная корреляция
                data['p_value'] < 0.15):       # Относительно значимая
                target_countries.append((country, data))
        
        # Анализируем объемы продаж
        total_sales = sum(restaurant_sales.values())
        avg_monthly_sales = total_sales / len(restaurant_sales)
        
        # Формируем результат
        result = {
            'restaurant': list(restaurant_sales.keys())[0].split('-')[0] if restaurant_sales else 'Unknown',
            'analysis_period': f"{min(restaurant_sales.keys())} - {max(restaurant_sales.keys())}",
            'total_sales': total_sales,
            'avg_monthly_sales': avg_monthly_sales,
            'target_audience': [],
            'all_correlations': correlations,
            'summary': {}
        }
        
        if target_countries:
            # ТОП-3 целевых стран
            for i, (country, data) in enumerate(target_countries[:3]):
                result['target_audience'].append({
                    'rank': i + 1,
                    'country': country,
                    'correlation': round(data['correlation'], 3),
                    'strength': data['strength'],
                    'significance': data['significance'],
                    'total_tourists': data['total_tourists'],
                    'market_share_estimate': self._estimate_market_share(data, total_sales)
                })
            
            # Суммарная статистика
            primary_target = target_countries[0]
            result['summary'] = {
                'primary_target': primary_target[0],
                'confidence': 'высокая' if primary_target[1]['correlation'] > 0.7 else 'средняя',
                'total_target_countries': len(target_countries),
                'recommendation': self._generate_recommendation(target_countries, avg_monthly_sales)
            }
        else:
            result['summary'] = {
                'primary_target': 'Не определена',
                'confidence': 'низкая',
                'total_target_countries': 0,
                'recommendation': 'Недостаточно данных для определения целевой аудитории'
            }
        
        return result
    
    def _estimate_market_share(self, correlation_data, total_sales):
        """Оценивает долю рынка на основе корреляции и объема туристов"""
        
        # Простая оценка на основе силы корреляции и объема туристов
        correlation_factor = abs(correlation_data['correlation'])
        tourist_volume = correlation_data['total_tourists']
        
        # Нормализуем объем туристов (предполагаем что 100,000+ это высокий объем)
        volume_factor = min(tourist_volume / 100000, 2.0)
        
        # Оценочная доля рынка (в процентах)
        estimated_share = correlation_factor * volume_factor * 30  # Максимум ~60%
        
        return round(min(estimated_share, 60), 1)  # Ограничиваем 60%
    
    def _generate_recommendation(self, target_countries, avg_sales):
        """Генерирует рекомендации для маркетинга"""
        
        if not target_countries:
            return "Проведите дополнительное исследование целевой аудитории"
        
        primary = target_countries[0]
        country = primary[0]
        correlation = primary[1]['correlation']
        
        if correlation > 0.7:
            return f"Сосредоточьте маркетинг на туристах из {country}. Высокая корреляция указывает на сильную связь с продажами."
        elif correlation > 0.5:
            return f"Рассмотрите {country} как основную целевую аудиторию. Умеренная корреляция требует дополнительной проверки."
        else:
            return f"Слабая связь с {country}. Рекомендуется анализ других факторов влияния на продажи."

def analyze_restaurant_target_audience(restaurant_name):
    """Удобная функция для анализа целевой аудитории ресторана"""
    
    analyzer = TargetAudienceAnalyzer()
    result = analyzer.analyze_restaurant_target_audience(restaurant_name)
    
    if result and 'error' not in result:
        print("\n🎯 АНАЛИЗ ЦЕЛЕВОЙ АУДИТОРИИ")
        print("=" * 50)
        print(f"🏪 Ресторан: {restaurant_name}")
        print(f"📅 Период: {result['analysis_period']}")
        print(f"💰 Общие продажи: {result['total_sales']:,.0f} IDR")
        print(f"📊 Средние месячные: {result['avg_monthly_sales']:,.0f} IDR")
        print()
        
        if result['target_audience']:
            print("🌍 ЦЕЛЕВАЯ АУДИТОРИЯ:")
            for target in result['target_audience']:
                print(f"  {target['rank']}. {target['country']}")
                print(f"     Корреляция: {target['correlation']:.3f} ({target['strength']})")
                print(f"     Туристов: {target['total_tourists']:,}")
                print(f"     Оценочная доля: {target['market_share_estimate']}%")
                print()
        
        print("💡 РЕКОМЕНДАЦИЯ:")
        print(f"   {result['summary']['recommendation']}")
        
        return result
    else:
        print(f"❌ Не удалось проанализировать целевую аудиторию для {restaurant_name}")
        return None

if __name__ == "__main__":
    # Тестирование
    result = analyze_restaurant_target_audience("Ika Kero")