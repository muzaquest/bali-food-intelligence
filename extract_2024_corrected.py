#!/usr/bin/env python3
"""
🏖️ ИСПРАВЛЕННОЕ ИЗВЛЕЧЕНИЕ ДАННЫХ 2024
Работает с реальной структурой файла 2024 года
"""

import pandas as pd
import numpy as np
import json
from datetime import datetime
import sqlite3

def extract_2024_data_corrected():
    """Исправленное извлечение данных за 2024"""
    
    print("🏖️ ИСПРАВЛЕННОЕ ИЗВЛЕЧЕНИЕ ДАННЫХ 2024")
    print("=" * 60)
    
    try:
        # Читаем файл с правильным заголовком
        df = pd.read_excel('Kunjungan_Wisatawan_Bali_2024.xls', header=2)
        
        print(f"✅ Файл 2024 загружен: {df.shape[0]} строк, {df.shape[1]} столбцов")
        
        # Месячные столбцы
        month_columns = ['JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUNE', 'JULY', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC']
        
        # СНИЖАЕМ ТРЕБОВАНИЯ - принимаем строки с хотя бы 3 месяцами данных
        valid_rows_2024 = []
        
        for idx, row in df.iterrows():
            monthly_values = {}
            valid_months = 0
            
            for month_idx, month_col in enumerate(month_columns):
                try:
                    val = pd.to_numeric(row[month_col], errors='coerce')
                    if pd.notna(val) and val >= 0:  # Принимаем даже нули
                        monthly_values[month_idx + 1] = int(val)
                        if val > 0:
                            valid_months += 1
                except:
                    continue
            
            # СНИЖАЕМ ТРЕБОВАНИЯ: минимум 3 месяца с данными > 0
            if valid_months >= 3:
                nationality = str(row.get('NATIONALITY', 'Unknown')).strip()
                total_tourists = sum(v for v in monthly_values.values() if v > 0)
                
                # СНИЖАЕМ ПОРОГ: минимум 500 туристов
                if total_tourists > 500:
                    valid_rows_2024.append({
                        'nationality': nationality,
                        'monthly_values': monthly_values,
                        'total_tourists': total_tourists,
                        'valid_months': valid_months,
                        'year': 2024
                    })
        
        print(f"✅ Найдено {len(valid_rows_2024)} стран с данными за 2024")
        
        if valid_rows_2024:
            # Сортируем по количеству туристов
            valid_rows_2024.sort(key=lambda x: x['total_tourists'], reverse=True)
            
            print("\n📊 ТОП-15 СТРАН ПО КОЛИЧЕСТВУ ТУРИСТОВ 2024:")
            print("-" * 80)
            
            for i, row_data in enumerate(valid_rows_2024[:15]):
                nationality = row_data['nationality'][:25]
                total = row_data['total_tourists']
                valid = row_data['valid_months']
                print(f"{i+1:2d}. {nationality:25} | {total:>10,} | {valid:2d} месяцев")
        
        # Агрегируем по месяцам за 2024
        monthly_totals_2024 = {}
        
        for month_num in range(1, 13):
            total_tourists = 0
            countries_with_data = 0
            
            for row in valid_rows_2024:
                if month_num in row['monthly_values']:
                    tourists = row['monthly_values'][month_num]
                    if tourists > 0:
                        total_tourists += tourists
                        countries_with_data += 1
            
            if total_tourists > 0:
                monthly_totals_2024[month_num] = {
                    'tourists': total_tourists,
                    'countries_count': countries_with_data,
                    'year': 2024
                }
        
        print(f"\n✅ Агрегированы данные 2024 по {len(monthly_totals_2024)} месяцам")
        
        # Показываем месячные данные 2024
        if monthly_totals_2024:
            print("\n📊 ТУРИСТИЧЕСКИЕ ДАННЫЕ 2024 ПО МЕСЯЦАМ:")
            print("-" * 70)
            
            month_names = [
                "Январь", "Февраль", "Март", "Апрель", "Май", "Июнь",
                "Июль", "Август", "Сентябрь", "Октябрь", "Ноябрь", "Декабрь"
            ]
            
            total_2024 = sum(data['tourists'] for data in monthly_totals_2024.values())
            
            for month_num in sorted(monthly_totals_2024.keys()):
                data = monthly_totals_2024[month_num]
                month_name = month_names[month_num - 1]
                tourists = data['tourists']
                countries = data['countries_count']
                percentage = (tourists / total_2024) * 100 if total_2024 > 0 else 0
                
                print(f"{month_num:2d}. {month_name:10} | {tourists:>10,} | {percentage:5.1f}% | {countries:3d} стран")
            
            print(f"\n📈 ИТОГО 2024: {total_2024:,} туристов за {len(monthly_totals_2024)} месяцев")
        
        return monthly_totals_2024, valid_rows_2024
        
    except Exception as e:
        print(f"❌ Ошибка извлечения данных 2024: {e}")
        return {}, []

def create_combined_analysis_corrected(monthly_2024, monthly_2025):
    """Создает исправленный объединенный анализ"""
    
    print("\n🔗 СОЗДАНИЕ ИСПРАВЛЕННОГО ОБЪЕДИНЕННОГО АНАЛИЗА 2024-2025")
    print("=" * 70)
    
    if not monthly_2024 and not monthly_2025:
        print("❌ Нет данных для анализа")
        return None
    
    try:
        # Показываем сравнение
        print("\n📊 СРАВНЕНИЕ ТУРИСТИЧЕСКИХ ПОТОКОВ 2024 vs 2025:")
        print("-" * 80)
        
        month_names = [
            "Январь", "Февраль", "Март", "Апрель", "Май", "Июнь",
            "Июль", "Август", "Сентябрь", "Октябрь", "Ноябрь", "Декабрь"
        ]
        
        comparison_data = []
        
        for month_num in range(1, 13):
            tourists_2024 = monthly_2024.get(month_num, {}).get('tourists', 0)
            tourists_2025 = monthly_2025.get(month_num, {}).get('tourists', 0)
            
            change = 0
            if tourists_2024 > 0 and tourists_2025 > 0:
                change = ((tourists_2025 - tourists_2024) / tourists_2024) * 100
            
            if tourists_2024 > 0 or tourists_2025 > 0:
                comparison_data.append({
                    'month': month_num,
                    'month_name': month_names[month_num - 1],
                    'tourists_2024': tourists_2024,
                    'tourists_2025': tourists_2025,
                    'change_percent': change
                })
                
                status = "📈" if change > 5 else "📉" if change < -5 else "➡️"
                print(f"{month_num:2d}. {month_names[month_num-1]:10} | 2024: {tourists_2024:>10,} | 2025: {tourists_2025:>10,} | {status} {change:+5.1f}%")
        
        # Рассчитываем общую статистику
        total_2024 = sum(item['tourists_2024'] for item in comparison_data)
        total_2025 = sum(item['tourists_2025'] for item in comparison_data)
        
        print(f"\n📈 ИТОГО:")
        print(f"   2024: {total_2024:,} туристов за {len(monthly_2024)} месяцев")
        print(f"   2025: {total_2025:,} туристов за {len(monthly_2025)} месяцев")
        
        # Создаем лучшие доступные коэффициенты
        print("\n🔄 СОЗДАНИЕ НАИЛУЧШИХ ДОСТУПНЫХ КОЭФФИЦИЕНТОВ:")
        print("-" * 60)
        
        # Используем все доступные данные
        combined_monthly_data = {}
        
        # Приоритет: данные 2024 (полный год), затем 2025
        for month_num in range(1, 13):
            tourists_2024 = monthly_2024.get(month_num, {}).get('tourists', 0)
            tourists_2025 = monthly_2025.get(month_num, {}).get('tourists', 0)
            
            if tourists_2024 > 0:
                # Предпочитаем данные 2024 (полный год)
                combined_monthly_data[month_num] = {
                    'tourists': tourists_2024,
                    'source': 'Данные 2024 (полный год)',
                    'priority': 1
                }
            elif tourists_2025 > 0:
                # Используем данные 2025 если нет 2024
                combined_monthly_data[month_num] = {
                    'tourists': tourists_2025,
                    'source': 'Данные 2025 (частичный год)',
                    'priority': 2
                }
        
        if not combined_monthly_data:
            print("❌ Нет данных для создания коэффициентов")
            return None
        
        # Рассчитываем коэффициенты
        total_combined = sum(data['tourists'] for data in combined_monthly_data.values())
        avg_monthly = total_combined / len(combined_monthly_data)
        
        combined_coefficients = {}
        
        for month_num, data in combined_monthly_data.items():
            coefficient = data['tourists'] / avg_monthly
            combined_coefficients[month_num] = {
                'coefficient': coefficient,
                'tourists': data['tourists'],
                'source': data['source']
            }
        
        print(f"✅ Создано коэффициентов: {len(combined_coefficients)}")
        
        # Классифицируем сезоны
        coefficients = [data['coefficient'] for data in combined_coefficients.values()]
        mean_coeff = np.mean(coefficients)
        std_coeff = np.std(coefficients)
        
        high_threshold = mean_coeff + 0.3 * std_coeff
        low_threshold = mean_coeff - 0.3 * std_coeff
        
        combined_seasons = {
            'высокий_сезон': {
                'месяцы': [], 
                'коэффициент': 0, 
                'описание': 'Пик туристического сезона (данные 2024-2025)',
                'источник': 'Объединенные данные Бали 2024-2025'
            },
            'средний_сезон': {
                'месяцы': [], 
                'коэффициент': 0, 
                'описание': 'Средний туристический сезон (данные 2024-2025)',
                'источник': 'Объединенные данные Бали 2024-2025'
            },
            'низкий_сезон': {
                'месяцы': [], 
                'коэффициент': 0, 
                'описание': 'Низкий туристический сезон (данные 2024-2025)',
                'источник': 'Объединенные данные Бали 2024-2025'
            }
        }
        
        for month_num, data in combined_coefficients.items():
            coeff = data['coefficient']
            
            if coeff >= high_threshold:
                combined_seasons['высокий_сезон']['месяцы'].append(month_num)
            elif coeff <= low_threshold:
                combined_seasons['низкий_сезон']['месяцы'].append(month_num)
            else:
                combined_seasons['средний_сезон']['месяцы'].append(month_num)
        
        # Средние коэффициенты для сезонов
        for season_name, season_data in combined_seasons.items():
            if season_data['месяцы']:
                season_coeffs = [combined_coefficients[month]['coefficient'] for month in season_data['месяцы']]
                season_data['коэффициент'] = float(np.mean(season_coeffs))
            else:
                season_data['коэффициент'] = 1.0
        
        print("\n✅ ФИНАЛЬНАЯ КЛАССИФИКАЦИЯ СЕЗОНОВ (ОБЪЕДИНЕННЫЕ ДАННЫЕ 2024-2025):")
        print("-" * 80)
        
        for season_name, season_data in combined_seasons.items():
            months_names = [month_names[month-1] for month in season_data['месяцы'] if month <= 12]
            coeff = season_data['коэффициент']
            change = (coeff - 1) * 100
            print(f"{season_name.upper():15} | {coeff:5.2f} ({change:+.0f}%) | {', '.join(months_names)}")
        
        # Рассчитываем корреляцию с продажами
        conn = sqlite3.connect('database.sqlite')
        
        query = """
        SELECT 
            stat_date as date,
            sales
        FROM (
            SELECT stat_date, sales FROM grab_stats WHERE sales > 0
            UNION ALL
            SELECT stat_date, sales FROM gojek_stats WHERE sales > 0
        )
        ORDER BY date
        """
        
        sales_df = pd.read_sql_query(query, conn)
        conn.close()
        
        sales_df['sales'] = pd.to_numeric(sales_df['sales'], errors='coerce')
        sales_df = sales_df.dropna(subset=['sales'])
        sales_df['date'] = pd.to_datetime(sales_df['date'])
        sales_df['month'] = sales_df['date'].dt.month
        
        monthly_sales = sales_df.groupby('month')['sales'].mean().reset_index()
        
        # Корреляция
        correlation_data = []
        
        for month_num, coeff_data in combined_coefficients.items():
            tourists = coeff_data['tourists']
            sales_data = monthly_sales[monthly_sales['month'] == month_num]
            avg_sales = sales_data['sales'].iloc[0] if len(sales_data) > 0 else 0
            
            if tourists > 0 and avg_sales > 0:
                correlation_data.append({
                    'month': month_num,
                    'tourists': tourists,
                    'avg_sales': avg_sales
                })
        
        correlation = 0
        if len(correlation_data) >= 3:
            df_corr = pd.DataFrame(correlation_data)
            correlation = df_corr['tourists'].corr(df_corr['avg_sales'])
        
        print(f"\n🔗 КОРРЕЛЯЦИЯ ОБЪЕДИНЕННЫХ ДАННЫХ ТУРИСТЫ ↔ ПРОДАЖИ: {correlation:.3f}")
        
        # Сохраняем результаты
        combined_result = {
            'seasonal_patterns': combined_seasons,
            'monthly_coefficients': combined_coefficients,
            'correlation_tourists_sales': float(correlation),
            'comparison_2024_2025': comparison_data,
            'analysis_metadata': {
                'source': 'Combined tourist data 2024-2025 (Kunjungan_Wisatawan_Bali)',
                'total_tourists_2024': total_2024,
                'total_tourists_2025': total_2025,
                'months_2024': len(monthly_2024),
                'months_2025': len(monthly_2025),
                'total_months_analyzed': len(combined_coefficients),
                'correlation_strength': 'Strong' if abs(correlation) > 0.7 else 'Moderate' if abs(correlation) > 0.4 else 'Weak',
                'data_quality': 'High' if len(combined_coefficients) >= 10 else 'Medium',
                'created_at': datetime.now().isoformat()
            }
        }
        
        with open('combined_tourist_correlations_2024_2025.json', 'w', encoding='utf-8') as f:
            json.dump(combined_result, f, indent=2, ensure_ascii=False)
        
        print(f"\n💾 Объединенные туристические корреляции сохранены в combined_tourist_correlations_2024_2025.json")
        
        return combined_result
        
    except Exception as e:
        print(f"❌ Ошибка создания объединенного анализа: {e}")
        return None

def load_2025_data():
    """Загружает данные 2025"""
    
    print("\n🏖️ ЗАГРУЗКА ДАННЫХ 2025")
    print("=" * 40)
    
    try:
        with open('real_tourist_correlations.json', 'r', encoding='utf-8') as f:
            data_2025 = json.load(f)
        
        monthly_coeffs_2025 = data_2025['monthly_coefficients']
        monthly_totals_2025 = {}
        
        for month_num, data in monthly_coeffs_2025.items():
            monthly_totals_2025[int(month_num)] = {
                'tourists': data['tourists'],
                'countries_count': data.get('countries_count', 96),
                'year': 2025
            }
        
        print(f"✅ Загружены данные 2025 по {len(monthly_totals_2025)} месяцам")
        return monthly_totals_2025
        
    except Exception as e:
        print(f"❌ Ошибка загрузки данных 2025: {e}")
        return {}

def main():
    """Главная функция"""
    
    print("🏖️ ИСПРАВЛЕННЫЙ ОБЪЕДИНЕННЫЙ АНАЛИЗ ТУРИСТИЧЕСКИХ ДАННЫХ 2024-2025")
    print("=" * 80)
    
    # Извлекаем данные 2024
    monthly_2024, rows_2024 = extract_2024_data_corrected()
    
    # Загружаем данные 2025
    monthly_2025 = load_2025_data()
    
    if monthly_2024 or monthly_2025:
        # Создаем объединенный анализ
        result = create_combined_analysis_corrected(monthly_2024, monthly_2025)
        
        if result:
            print("\n🎉 ИСПРАВЛЕННЫЙ ОБЪЕДИНЕННЫЙ АНАЛИЗ ЗАВЕРШЕН!")
            print("✅ Проанализированы все доступные данные 2024-2025")
            print("✅ Созданы наилучшие доступные коэффициенты")
            print("✅ Рассчитана корреляция с продажами")
            print("✅ Получена максимально точная картина сезонности")
            
            print(f"\n📊 ИТОГОВЫЕ РЕЗУЛЬТАТЫ:")
            print(f"   📈 Корреляция: {result['correlation_tourists_sales']:.3f}")
            print(f"   🏖️ Туристов 2024: {result['analysis_metadata']['total_tourists_2024']:,}")
            print(f"   🏖️ Туристов 2025: {result['analysis_metadata']['total_tourists_2025']:,}")
            print(f"   📅 Месяцев проанализировано: {result['analysis_metadata']['total_months_analyzed']}")
            print(f"   📊 Качество данных: {result['analysis_metadata']['data_quality']}")
            
            return True
    
    print("❌ Не удалось создать объединенный анализ")
    return False

if __name__ == "__main__":
    main()