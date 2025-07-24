#!/usr/bin/env python3
"""
🏖️ ИЗВЛЕЧЕНИЕ ПОЛНЫХ ТУРИСТИЧЕСКИХ ДАННЫХ ЗА 12 МЕСЯЦЕВ
Ищет и извлекает данные за все 12 месяцев из файла
"""

import pandas as pd
import numpy as np
import json
from datetime import datetime
import sqlite3

def find_complete_monthly_data():
    """Ищет строки с данными за все 12 месяцев"""
    
    print("🔍 ПОИСК ПОЛНЫХ МЕСЯЧНЫХ ДАННЫХ (12 МЕСЯЦЕВ)")
    print("=" * 60)
    
    try:
        # Читаем файл с правильным заголовком
        df = pd.read_excel('Kunjungan_Wisatawan_Bali_2025.xls', header=2)
        
        print(f"✅ Файл загружен: {df.shape[0]} строк, {df.shape[1]} столбцов")
        print(f"📊 Столбцы: {list(df.columns)}")
        
        # Ищем месячные столбцы
        month_columns = ['JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUNE', 'JULY', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC']
        available_months = [col for col in month_columns if col in df.columns]
        
        print(f"\n✅ Найдено месячных столбцов: {len(available_months)}")
        print(f"   Месяцы: {available_months}")
        
        if len(available_months) < 12:
            print("⚠️ Не все 12 месяцев найдены, ищем альтернативные названия...")
            
            # Ищем альтернативные названия месяцев
            alt_month_patterns = {
                'JAN': ['jan', 'january', 'январь'],
                'FEB': ['feb', 'february', 'февраль'],
                'MAR': ['mar', 'march', 'март'],
                'APR': ['apr', 'april', 'апрель'],
                'MAY': ['may', 'май'],
                'JUN': ['jun', 'june', 'июнь'],
                'JUL': ['jul', 'july', 'июль'],
                'AUG': ['aug', 'august', 'август'],
                'SEP': ['sep', 'september', 'сентябрь'],
                'OCT': ['oct', 'october', 'октябрь'],
                'NOV': ['nov', 'november', 'ноябрь'],
                'DEC': ['dec', 'december', 'декабрь']
            }
            
            for col in df.columns:
                col_lower = str(col).lower()
                for month_key, patterns in alt_month_patterns.items():
                    if any(pattern in col_lower for pattern in patterns):
                        if month_key not in available_months and col not in available_months:
                            available_months.append(col)
                            print(f"   Найден альтернативный месяц: {col} -> {month_key}")
        
        # Ищем строки с данными за все доступные месяцы
        complete_rows = []
        
        for idx, row in df.iterrows():
            monthly_values = []
            
            for month_col in available_months:
                try:
                    val = pd.to_numeric(row[month_col], errors='coerce')
                    if pd.notna(val) and val > 0:
                        monthly_values.append(val)
                    else:
                        break  # Если хотя бы одно значение отсутствует, пропускаем строку
                except:
                    break
            
            # Если есть данные за все доступные месяцы
            if len(monthly_values) == len(available_months):
                complete_rows.append({
                    'row_index': idx,
                    'nationality': str(row.get('NATIONALITY', 'Unknown')),
                    'values': monthly_values,
                    'total': sum(monthly_values),
                    'months_count': len(monthly_values)
                })
        
        print(f"\n✅ Найдено {len(complete_rows)} строк с полными месячными данными")
        
        if complete_rows:
            # Сортируем по общему количеству туристов
            complete_rows.sort(key=lambda x: x['total'], reverse=True)
            
            print("\n📊 ТОП-10 СТРАН/СТРОК ПО КОЛИЧЕСТВУ ТУРИСТОВ:")
            print("-" * 70)
            
            for i, row_data in enumerate(complete_rows[:10]):
                nationality = row_data['nationality'][:20]  # Обрезаем длинные названия
                print(f"{i+1:2d}. {nationality:20} | {row_data['total']:>10,} | {row_data['months_count']} месяцев")
        
        return complete_rows, available_months
        
    except Exception as e:
        print(f"❌ Ошибка поиска данных: {e}")
        return [], []

def create_aggregated_monthly_data(complete_rows, available_months):
    """Создает агрегированные месячные данные"""
    
    print(f"\n🔄 СОЗДАНИЕ АГРЕГИРОВАННЫХ МЕСЯЧНЫХ ДАННЫХ")
    print("=" * 60)
    
    if not complete_rows:
        print("❌ Нет данных для агрегации")
        return None
    
    try:
        # Суммируем данные по всем странам для каждого месяца
        month_totals = {}
        
        for month_idx, month_col in enumerate(available_months):
            month_num = month_idx + 1
            total_tourists = sum(row['values'][month_idx] for row in complete_rows)
            
            month_totals[month_num] = {
                'tourists': int(total_tourists),
                'month_name': month_col,
                'countries_count': len(complete_rows)
            }
        
        print(f"✅ Агрегированы данные по {len(month_totals)} месяцам")
        
        # Показываем результаты
        print("\n📊 АГРЕГИРОВАННЫЕ ТУРИСТИЧЕСКИЕ ДАННЫЕ ПО МЕСЯЦАМ:")
        print("-" * 70)
        
        month_names = [
            "Январь", "Февраль", "Март", "Апрель", "Май", "Июнь",
            "Июль", "Август", "Сентябрь", "Октябрь", "Ноябрь", "Декабрь"
        ]
        
        total_tourists = sum(data['tourists'] for data in month_totals.values())
        
        for month_num in sorted(month_totals.keys()):
            data = month_totals[month_num]
            month_name = month_names[month_num - 1] if month_num <= 12 else f"Месяц {month_num}"
            tourists = data['tourists']
            percentage = (tourists / total_tourists) * 100 if total_tourists > 0 else 0
            
            print(f"{month_num:2d}. {month_name:10} | {tourists:>10,} | {percentage:5.1f}% | {data['countries_count']} стран")
        
        print(f"\n📈 ИТОГО: {total_tourists:,} туристов за {len(month_totals)} месяцев")
        
        return month_totals
        
    except Exception as e:
        print(f"❌ Ошибка агрегации данных: {e}")
        return None

def create_real_tourist_correlations_full(month_totals):
    """Создает корреляции с полными данными"""
    
    print(f"\n🔗 СОЗДАНИЕ РЕАЛЬНЫХ ТУРИСТИЧЕСКИХ КОРРЕЛЯЦИЙ")
    print("=" * 60)
    
    if not month_totals:
        print("❌ Нет месячных данных")
        return None
    
    try:
        # Загружаем продажи
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
        
        # Преобразуем данные
        sales_df['sales'] = pd.to_numeric(sales_df['sales'], errors='coerce')
        sales_df = sales_df.dropna(subset=['sales'])
        sales_df['date'] = pd.to_datetime(sales_df['date'])
        sales_df['month'] = sales_df['date'].dt.month
        
        # Группируем по месяцам
        monthly_sales = sales_df.groupby('month')['sales'].mean().reset_index()
        
        print(f"✅ Загружены продажи по {len(monthly_sales)} месяцам")
        
        # Создаем корреляционные данные
        correlation_data = []
        
        for month_num, tourist_data in month_totals.items():
            tourists = tourist_data['tourists']
            sales_data = monthly_sales[monthly_sales['month'] == month_num]
            avg_sales = sales_data['sales'].iloc[0] if len(sales_data) > 0 else 0
            
            if tourists > 0 and avg_sales > 0:
                correlation_data.append({
                    'month': month_num,
                    'tourists': tourists,
                    'avg_sales': avg_sales
                })
        
        if len(correlation_data) >= 3:
            # Рассчитываем корреляцию
            df_corr = pd.DataFrame(correlation_data)
            correlation = df_corr['tourists'].corr(df_corr['avg_sales'])
            
            print(f"🔗 КОРРЕЛЯЦИЯ ТУРИСТЫ ↔ ПРОДАЖИ: {correlation:.3f}")
            
            # Создаем коэффициенты
            total_tourists = sum(item['tourists'] for item in correlation_data)
            avg_tourists = total_tourists / len(correlation_data)
            
            tourist_coefficients = {}
            
            for item in correlation_data:
                month_num = item['month']
                tourists = item['tourists']
                coefficient = tourists / avg_tourists
                
                tourist_coefficients[month_num] = {
                    'coefficient': coefficient,
                    'tourists': tourists,
                    'avg_sales': item['avg_sales']
                }
            
            # Классифицируем сезоны
            coefficients = [data['coefficient'] for data in tourist_coefficients.values()]
            mean_coeff = np.mean(coefficients)
            std_coeff = np.std(coefficients)
            
            high_threshold = mean_coeff + 0.3 * std_coeff
            low_threshold = mean_coeff - 0.3 * std_coeff
            
            seasons = {
                'высокий_сезон': {'месяцы': [], 'коэффициент': 0, 'описание': 'Пик туристического сезона (реальные данные)'},
                'средний_сезон': {'месяцы': [], 'коэффициент': 0, 'описание': 'Средний туристический сезон (реальные данные)'},
                'низкий_сезон': {'месяцы': [], 'коэффициент': 0, 'описание': 'Низкий туристический сезон (реальные данные)'}
            }
            
            for month_num, data in tourist_coefficients.items():
                coeff = data['coefficient']
                
                if coeff >= high_threshold:
                    seasons['высокий_сезон']['месяцы'].append(month_num)
                elif coeff <= low_threshold:
                    seasons['низкий_сезон']['месяцы'].append(month_num)
                else:
                    seasons['средний_сезон']['месяцы'].append(month_num)
            
            # Средние коэффициенты для сезонов
            for season_name, season_data in seasons.items():
                if season_data['месяцы']:
                    season_coeffs = [tourist_coefficients[month]['coefficient'] for month in season_data['месяцы']]
                    season_data['коэффициент'] = float(np.mean(season_coeffs))
                else:
                    season_data['коэффициент'] = 1.0
            
            print("\n✅ РЕАЛЬНАЯ КЛАССИФИКАЦИЯ СЕЗОНОВ (ПОЛНЫЕ ТУРИСТИЧЕСКИЕ ДАННЫЕ):")
            print("-" * 70)
            
            month_names = [
                "Январь", "Февраль", "Март", "Апрель", "Май", "Июнь",
                "Июль", "Август", "Сентябрь", "Октябрь", "Ноябрь", "Декабрь"
            ]
            
            for season_name, season_data in seasons.items():
                months_names = [month_names[month-1] for month in season_data['месяцы'] if month <= 12]
                print(f"{season_name.upper():15} | {season_data['коэффициент']:5.2f} | {', '.join(months_names)}")
            
            # Сохраняем результаты
            result = {
                'seasonal_patterns': seasons,
                'monthly_coefficients': tourist_coefficients,
                'correlation_tourists_sales': float(correlation),
                'analysis_metadata': {
                    'source': 'Complete tourist arrivals data (Kunjungan_Wisatawan_Bali_2025.xls)',
                    'total_tourists': total_tourists,
                    'months_analyzed': len(correlation_data),
                    'correlation_strength': 'Strong' if abs(correlation) > 0.7 else 'Moderate' if abs(correlation) > 0.4 else 'Weak',
                    'countries_included': len([row for row in correlation_data]),
                    'created_at': datetime.now().isoformat()
                }
            }
            
            with open('real_tourist_correlations.json', 'w', encoding='utf-8') as f:
                json.dump(result, f, indent=2, ensure_ascii=False)
            
            print(f"\n💾 Полные туристические корреляции сохранены в real_tourist_correlations.json")
            
            return result
        else:
            print("❌ Недостаточно данных для корреляции")
            return None
            
    except Exception as e:
        print(f"❌ Ошибка создания корреляций: {e}")
        return None

def main():
    """Главная функция"""
    
    print("🏖️ ИЗВЛЕЧЕНИЕ ПОЛНЫХ ТУРИСТИЧЕСКИХ ДАННЫХ БАЛИ")
    print("=" * 70)
    
    # Ищем полные месячные данные
    complete_rows, available_months = find_complete_monthly_data()
    
    if complete_rows and available_months:
        # Создаем агрегированные данные
        month_totals = create_aggregated_monthly_data(complete_rows, available_months)
        
        if month_totals:
            # Создаем корреляции
            result = create_real_tourist_correlations_full(month_totals)
            
            if result:
                print("\n🎉 АНАЛИЗ ПОЛНЫХ ТУРИСТИЧЕСКИХ ДАННЫХ ЗАВЕРШЕН!")
                print("✅ Извлечены данные по всем доступным месяцам")
                print("✅ Агрегированы данные по всем странам")
                print("✅ Рассчитана корреляция с продажами")
                print("✅ Созданы научные сезонные коэффициенты")
                
                print(f"\n📊 ИТОГОВЫЕ РЕЗУЛЬТАТЫ:")
                print(f"   Корреляция: {result['correlation_tourists_sales']:.3f}")
                print(f"   Сила связи: {result['analysis_metadata']['correlation_strength']}")
                print(f"   Туристов проанализировано: {result['analysis_metadata']['total_tourists']:,}")
                print(f"   Месяцев проанализировано: {result['analysis_metadata']['months_analyzed']}")
                
                return True
    
    print("❌ Не удалось извлечь полные туристические данные")
    return False

if __name__ == "__main__":
    main()