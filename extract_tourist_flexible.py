#!/usr/bin/env python3
"""
🏖️ ГИБКОЕ ИЗВЛЕЧЕНИЕ ТУРИСТИЧЕСКИХ ДАННЫХ
Работает с неполными данными и извлекает максимум информации
"""

import pandas as pd
import numpy as np
import json
from datetime import datetime
import sqlite3

def flexible_tourist_extraction():
    """Гибкое извлечение туристических данных"""
    
    print("🔍 ГИБКОЕ ИЗВЛЕЧЕНИЕ ТУРИСТИЧЕСКИХ ДАННЫХ")
    print("=" * 60)
    
    try:
        # Читаем файл
        df = pd.read_excel('Kunjungan_Wisatawan_Bali_2025.xls', header=2)
        
        print(f"✅ Файл загружен: {df.shape[0]} строк, {df.shape[1]} столбцов")
        
        # Месячные столбцы
        month_columns = ['JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUNE', 'JULY', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC']
        
        # Ищем строки с хотя бы некоторыми данными
        valid_rows = []
        
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
            
            # Если есть хотя бы 3 месяца с данными > 0
            if valid_months >= 3:
                nationality = str(row.get('NATIONALITY', 'Unknown')).strip()
                total_tourists = sum(v for v in monthly_values.values() if v > 0)
                
                if total_tourists > 100:  # Минимальный порог
                    valid_rows.append({
                        'row_index': idx,
                        'nationality': nationality,
                        'monthly_values': monthly_values,
                        'total_tourists': total_tourists,
                        'valid_months': valid_months
                    })
        
        print(f"✅ Найдено {len(valid_rows)} строк с валидными данными")
        
        if valid_rows:
            # Сортируем по количеству туристов
            valid_rows.sort(key=lambda x: x['total_tourists'], reverse=True)
            
            print("\n📊 ТОП-15 СТРАН ПО КОЛИЧЕСТВУ ТУРИСТОВ:")
            print("-" * 80)
            
            for i, row_data in enumerate(valid_rows[:15]):
                nationality = row_data['nationality'][:25]
                total = row_data['total_tourists']
                valid = row_data['valid_months']
                print(f"{i+1:2d}. {nationality:25} | {total:>10,} | {valid:2d} месяцев")
        
        return valid_rows, month_columns
        
    except Exception as e:
        print(f"❌ Ошибка извлечения: {e}")
        return [], []

def aggregate_monthly_totals(valid_rows, month_columns):
    """Агрегирует данные по месяцам"""
    
    print(f"\n🔄 АГРЕГАЦИЯ ДАННЫХ ПО МЕСЯЦАМ")
    print("=" * 50)
    
    if not valid_rows:
        print("❌ Нет данных для агрегации")
        return None
    
    try:
        # Суммируем по месяцам
        monthly_totals = {}
        
        for month_num in range(1, 13):
            total_tourists = 0
            countries_with_data = 0
            
            for row in valid_rows:
                if month_num in row['monthly_values']:
                    tourists = row['monthly_values'][month_num]
                    if tourists > 0:
                        total_tourists += tourists
                        countries_with_data += 1
            
            if total_tourists > 0:
                monthly_totals[month_num] = {
                    'tourists': total_tourists,
                    'countries_count': countries_with_data,
                    'month_name': month_columns[month_num - 1]
                }
        
        print(f"✅ Агрегированы данные по {len(monthly_totals)} месяцам")
        
        # Показываем результаты
        print("\n📊 ТУРИСТИЧЕСКИЕ ДАННЫЕ ПО МЕСЯЦАМ:")
        print("-" * 70)
        
        month_names = [
            "Январь", "Февраль", "Март", "Апрель", "Май", "Июнь",
            "Июль", "Август", "Сентябрь", "Октябрь", "Ноябрь", "Декабрь"
        ]
        
        total_all_months = sum(data['tourists'] for data in monthly_totals.values())
        
        for month_num in sorted(monthly_totals.keys()):
            data = monthly_totals[month_num]
            month_name = month_names[month_num - 1]
            tourists = data['tourists']
            countries = data['countries_count']
            percentage = (tourists / total_all_months) * 100 if total_all_months > 0 else 0
            
            print(f"{month_num:2d}. {month_name:10} | {tourists:>10,} | {percentage:5.1f}% | {countries:3d} стран")
        
        print(f"\n📈 ИТОГО: {total_all_months:,} туристов за {len(monthly_totals)} месяцев")
        
        return monthly_totals
        
    except Exception as e:
        print(f"❌ Ошибка агрегации: {e}")
        return None

def create_final_tourist_correlations(monthly_totals):
    """Создает финальные туристические корреляции"""
    
    print(f"\n🔗 СОЗДАНИЕ ФИНАЛЬНЫХ ТУРИСТИЧЕСКИХ КОРРЕЛЯЦИЙ")
    print("=" * 60)
    
    if not monthly_totals:
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
        
        for month_num, tourist_data in monthly_totals.items():
            tourists = tourist_data['tourists']
            sales_data = monthly_sales[monthly_sales['month'] == month_num]
            avg_sales = sales_data['sales'].iloc[0] if len(sales_data) > 0 else 0
            
            if tourists > 0 and avg_sales > 0:
                correlation_data.append({
                    'month': month_num,
                    'tourists': tourists,
                    'avg_sales': avg_sales,
                    'countries_count': tourist_data['countries_count']
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
                    'avg_sales': item['avg_sales'],
                    'countries_count': item['countries_count']
                }
            
            # Классифицируем сезоны
            coefficients = [data['coefficient'] for data in tourist_coefficients.values()]
            mean_coeff = np.mean(coefficients)
            std_coeff = np.std(coefficients)
            
            high_threshold = mean_coeff + 0.4 * std_coeff
            low_threshold = mean_coeff - 0.4 * std_coeff
            
            seasons = {
                'высокий_сезон': {
                    'месяцы': [], 
                    'коэффициент': 0, 
                    'описание': 'Пик туристического сезона (реальные данные Бали)',
                    'источник': 'Kunjungan_Wisatawan_Bali_2025.xls'
                },
                'средний_сезон': {
                    'месяцы': [], 
                    'коэффициент': 0, 
                    'описание': 'Средний туристический сезон (реальные данные Бали)',
                    'источник': 'Kunjungan_Wisatawan_Bali_2025.xls'
                },
                'низкий_сезон': {
                    'месяцы': [], 
                    'коэффициент': 0, 
                    'описание': 'Низкий туристический сезон (реальные данные Бали)',
                    'источник': 'Kunjungan_Wisatawan_Bali_2025.xls'
                }
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
            
            print("\n✅ ФИНАЛЬНАЯ КЛАССИФИКАЦИЯ СЕЗОНОВ (РЕАЛЬНЫЕ ТУРИСТИЧЕСКИЕ ДАННЫЕ БАЛИ):")
            print("-" * 80)
            
            month_names = [
                "Январь", "Февраль", "Март", "Апрель", "Май", "Июнь",
                "Июль", "Август", "Сентябрь", "Октябрь", "Ноябрь", "Декабрь"
            ]
            
            for season_name, season_data in seasons.items():
                months_names = [month_names[month-1] for month in season_data['месяцы'] if month <= 12]
                coeff = season_data['коэффициент']
                change = (coeff - 1) * 100
                print(f"{season_name.upper():15} | {coeff:5.2f} ({change:+.0f}%) | {', '.join(months_names)}")
            
            # Сравнение с предыдущими коэффициентами
            print("\n📊 СРАВНЕНИЕ С ПРЕДЫДУЩИМИ КОЭФФИЦИЕНТАМИ:")
            print("-" * 60)
            print("БЫЛО (из продаж):")
            print("   Высокий сезон: 1.15 (+15%) | Январь, Февраль, Июль, Август")
            print("   Средний сезон: 0.96 (-4%)  | Март, Октябрь, Ноябрь, Декабрь")
            print("   Низкий сезон:  0.89 (-11%) | Апрель, Май, Июнь, Сентябрь")
            
            print("\nСТАЛО (из туристических данных):")
            for season_name, season_data in seasons.items():
                months_names = [month_names[month-1] for month in season_data['месяцы'] if month <= 12]
                coeff = season_data['коэффициент']
                change = (coeff - 1) * 100
                print(f"   {season_name.replace('_', ' ').title()}: {coeff:.2f} ({change:+.0f}%) | {', '.join(months_names)}")
            
            # Сохраняем результаты
            result = {
                'seasonal_patterns': seasons,
                'monthly_coefficients': tourist_coefficients,
                'correlation_tourists_sales': float(correlation),
                'analysis_metadata': {
                    'source': 'Real tourist arrivals data (Kunjungan_Wisatawan_Bali_2025.xls)',
                    'total_tourists': total_tourists,
                    'months_analyzed': len(correlation_data),
                    'correlation_strength': 'Strong' if abs(correlation) > 0.7 else 'Moderate' if abs(correlation) > 0.4 else 'Weak',
                    'countries_included': sum(item['countries_count'] for item in correlation_data) // len(correlation_data),
                    'data_quality': 'High' if len(correlation_data) >= 10 else 'Medium' if len(correlation_data) >= 6 else 'Low',
                    'created_at': datetime.now().isoformat()
                }
            }
            
            with open('real_tourist_correlations.json', 'w', encoding='utf-8') as f:
                json.dump(result, f, indent=2, ensure_ascii=False)
            
            print(f"\n💾 Финальные туристические корреляции сохранены в real_tourist_correlations.json")
            
            return result
        else:
            print("❌ Недостаточно данных для корреляции")
            return None
            
    except Exception as e:
        print(f"❌ Ошибка создания корреляций: {e}")
        return None

def main():
    """Главная функция"""
    
    print("🏖️ ГИБКОЕ ИЗВЛЕЧЕНИЕ ТУРИСТИЧЕСКИХ ДАННЫХ БАЛИ")
    print("=" * 70)
    
    # Извлекаем данные
    valid_rows, month_columns = flexible_tourist_extraction()
    
    if valid_rows:
        # Агрегируем по месяцам
        monthly_totals = aggregate_monthly_totals(valid_rows, month_columns)
        
        if monthly_totals:
            # Создаем корреляции
            result = create_final_tourist_correlations(monthly_totals)
            
            if result:
                print("\n🎉 ГИБКОЕ ИЗВЛЕЧЕНИЕ ТУРИСТИЧЕСКИХ ДАННЫХ ЗАВЕРШЕНО!")
                print("✅ Извлечены данные из реального файла Бали")
                print("✅ Агрегированы данные по всем странам")
                print("✅ Рассчитана корреляция туристы ↔ продажи")
                print("✅ Созданы научные сезонные коэффициенты")
                
                print(f"\n📊 ФИНАЛЬНЫЕ РЕЗУЛЬТАТЫ:")
                print(f"   📈 Корреляция: {result['correlation_tourists_sales']:.3f}")
                print(f"   🎯 Сила связи: {result['analysis_metadata']['correlation_strength']}")
                print(f"   🏖️ Туристов: {result['analysis_metadata']['total_tourists']:,}")
                print(f"   📅 Месяцев: {result['analysis_metadata']['months_analyzed']}")
                print(f"   🌍 Стран: ~{result['analysis_metadata']['countries_included']}")
                print(f"   📊 Качество данных: {result['analysis_metadata']['data_quality']}")
                
                return True
    
    print("❌ Не удалось извлечь туристические данные")
    return False

if __name__ == "__main__":
    main()