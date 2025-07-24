#!/usr/bin/env python3
"""
🏖️ ДЕТАЛЬНЫЙ АНАЛИЗ ФАЙЛА ТУРИСТИЧЕСКИХ ДАННЫХ БАЛИ
Более глубокий анализ структуры файла и извлечение данных
"""

import pandas as pd
import numpy as np
import json
from datetime import datetime
import sqlite3

def analyze_excel_structure():
    """Детальный анализ структуры Excel файла"""
    
    print("🔍 ДЕТАЛЬНЫЙ АНАЛИЗ СТРУКТУРЫ ФАЙЛА")
    print("=" * 60)
    
    try:
        # Читаем файл с разными параметрами
        print("📂 Загрузка файла с различными настройками...")
        
        # Попробуем прочитать без заголовков
        df_no_header = pd.read_excel('Kunjungan_Wisatawan_Bali_2025.xls', header=None)
        print(f"✅ Без заголовков: {df_no_header.shape[0]} строк, {df_no_header.shape[1]} столбцов")
        
        # Попробуем найти строку с месяцами
        print("\n🔍 ПОИСК СТРОКИ С МЕСЯЦАМИ:")
        print("-" * 40)
        
        month_patterns = ['jan', 'feb', 'mar', 'apr', 'may', 'jun', 'jul', 'aug', 'sep', 'oct', 'nov', 'dec',
                         'januari', 'februari', 'maret', 'april', 'mei', 'juni', 'juli', 'agustus', 'september', 'oktober', 'november', 'desember']
        
        header_row = None
        
        for idx, row in df_no_header.iterrows():
            row_text = ' '.join([str(cell).lower() for cell in row if pd.notna(cell)])
            month_count = sum(1 for pattern in month_patterns if pattern in row_text)
            
            if month_count >= 3:  # Если найдено 3+ месяца
                print(f"✅ Найдена строка с месяцами в строке {idx}: {month_count} месяцев")
                print(f"   Содержимое: {list(row)[:10]}...")
                header_row = idx
                break
        
        if header_row is not None:
            # Читаем файл с найденным заголовком
            df = pd.read_excel('Kunjungan_Wisatawan_Bali_2025.xls', header=header_row)
            print(f"\n✅ Перезагружен с заголовком в строке {header_row}")
            print(f"   Новые столбцы: {list(df.columns)}")
            
            return df, header_row
        else:
            print("⚠️ Строка с месяцами не найдена, используем исходный файл")
            return df_no_header, None
            
    except Exception as e:
        print(f"❌ Ошибка анализа структуры: {e}")
        return None, None

def find_data_patterns(df):
    """Ищет паттерны данных в файле"""
    
    print("\n🔍 ПОИСК ПАТТЕРНОВ ДАННЫХ")
    print("=" * 40)
    
    try:
        # Ищем строки с большими числами (потенциально туристы)
        numeric_rows = []
        
        for idx, row in df.iterrows():
            numeric_values = []
            for cell in row:
                try:
                    val = pd.to_numeric(cell, errors='coerce')
                    if pd.notna(val) and val > 1000:  # Больше 1000 - потенциально туристы
                        numeric_values.append(val)
                except:
                    continue
            
            if len(numeric_values) >= 3:  # Минимум 3 числа
                numeric_rows.append({
                    'row_index': idx,
                    'values': numeric_values,
                    'total': sum(numeric_values),
                    'count': len(numeric_values)
                })
        
        print(f"✅ Найдено {len(numeric_rows)} строк с потенциальными туристическими данными")
        
        # Сортируем по общему количеству туристов
        numeric_rows.sort(key=lambda x: x['total'], reverse=True)
        
        # Показываем топ-5 строк
        print("\n📊 ТОП-5 СТРОК С НАИБОЛЬШИМИ ЧИСЛАМИ:")
        print("-" * 50)
        
        for i, row_data in enumerate(numeric_rows[:5]):
            print(f"{i+1}. Строка {row_data['row_index']:3d}: {row_data['count']} значений, сумма: {row_data['total']:>10,.0f}")
            print(f"   Значения: {[int(v) for v in row_data['values'][:10]]}")
        
        return numeric_rows
        
    except Exception as e:
        print(f"❌ Ошибка поиска паттернов: {e}")
        return []

def extract_monthly_data_smart(df, numeric_rows):
    """Умное извлечение месячных данных"""
    
    print("\n🧠 УМНОЕ ИЗВЛЕЧЕНИЕ МЕСЯЧНЫХ ДАННЫХ")
    print("=" * 50)
    
    try:
        if not numeric_rows:
            print("❌ Нет данных для извлечения")
            return None
        
        # Берем строку с наибольшей суммой (вероятно, это итоговые данные)
        best_row_data = numeric_rows[0]
        best_row_idx = best_row_data['row_index']
        
        print(f"✅ Выбрана строка {best_row_idx} с суммой {best_row_data['total']:,.0f}")
        
        # Получаем данные из этой строки
        row = df.iloc[best_row_idx]
        
        # Извлекаем все числовые значения больше 1000
        monthly_data = {}
        values = []
        
        for col_idx, cell in enumerate(row):
            try:
                val = pd.to_numeric(cell, errors='coerce')
                if pd.notna(val) and val > 1000:
                    values.append((col_idx, int(val)))
            except:
                continue
        
        # Если у нас есть 12 значений, предполагаем что это месяцы
        if len(values) == 12:
            print("✅ Найдено ровно 12 значений - вероятно, месячные данные!")
            
            for i, (col_idx, value) in enumerate(values):
                month_num = i + 1
                monthly_data[month_num] = {
                    'tourists': value,
                    'column_index': col_idx,
                    'source': f'Строка {best_row_idx}, столбец {col_idx}'
                }
        
        # Если не 12, попробуем другую логику
        elif len(values) > 6:
            print(f"⚠️ Найдено {len(values)} значений (не 12)")
            print("   Пробуем распределить по месяцам...")
            
            # Берем первые 12 или все доступные
            for i, (col_idx, value) in enumerate(values[:12]):
                month_num = i + 1
                monthly_data[month_num] = {
                    'tourists': value,
                    'column_index': col_idx,
                    'source': f'Строка {best_row_idx}, столбец {col_idx} (предположительно)'
                }
        
        if monthly_data:
            print(f"\n📊 ИЗВЛЕЧЕННЫЕ МЕСЯЧНЫЕ ДАННЫЕ:")
            print("-" * 50)
            
            month_names = [
                "Январь", "Февраль", "Март", "Апрель", "Май", "Июнь",
                "Июль", "Август", "Сентябрь", "Октябрь", "Ноябрь", "Декабрь"
            ]
            
            total_tourists = sum(data['tourists'] for data in monthly_data.values())
            
            for month_num in sorted(monthly_data.keys()):
                data = monthly_data[month_num]
                month_name = month_names[month_num - 1]
                tourists = data['tourists']
                percentage = (tourists / total_tourists) * 100 if total_tourists > 0 else 0
                
                print(f"{month_num:2d}. {month_name:10} | {tourists:>10,} | {percentage:5.1f}%")
            
            print(f"\n📈 ИТОГО: {total_tourists:,} туристов")
            
            return monthly_data
        else:
            print("❌ Не удалось извлечь месячные данные")
            return None
            
    except Exception as e:
        print(f"❌ Ошибка извлечения данных: {e}")
        return None

def create_tourist_correlations(monthly_data):
    """Создает корреляции с продажами"""
    
    print("\n🔗 СОЗДАНИЕ КОРРЕЛЯЦИЙ С ПРОДАЖАМИ")
    print("=" * 50)
    
    if not monthly_data:
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
        
        for month_num in range(1, 13):
            tourists = monthly_data.get(month_num, {}).get('tourists', 0)
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
                'высокий_сезон': {'месяцы': [], 'коэффициент': 0},
                'средний_сезон': {'месяцы': [], 'коэффициент': 0},
                'низкий_сезон': {'месяцы': [], 'коэффициент': 0}
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
            
            print("\n✅ КЛАССИФИКАЦИЯ СЕЗОНОВ (РЕАЛЬНЫЕ ТУРИСТИЧЕСКИЕ ДАННЫЕ):")
            print("-" * 60)
            
            month_names = [
                "Январь", "Февраль", "Март", "Апрель", "Май", "Июнь",
                "Июль", "Август", "Сентябрь", "Октябрь", "Ноябрь", "Декабрь"
            ]
            
            for season_name, season_data in seasons.items():
                months_names = [month_names[month-1] for month in season_data['месяцы']]
                print(f"{season_name.upper():15} | {season_data['коэффициент']:5.2f} | {', '.join(months_names)}")
            
            # Сохраняем результаты
            result = {
                'seasonal_patterns': seasons,
                'monthly_coefficients': tourist_coefficients,
                'correlation_tourists_sales': float(correlation),
                'analysis_metadata': {
                    'source': 'Real tourist arrivals (Kunjungan_Wisatawan_Bali_2025.xls)',
                    'total_tourists': total_tourists,
                    'months_analyzed': len(correlation_data),
                    'correlation_strength': 'Strong' if abs(correlation) > 0.7 else 'Moderate' if abs(correlation) > 0.4 else 'Weak',
                    'created_at': datetime.now().isoformat()
                }
            }
            
            with open('real_tourist_correlations.json', 'w', encoding='utf-8') as f:
                json.dump(result, f, indent=2, ensure_ascii=False)
            
            print(f"\n💾 Сохранено в real_tourist_correlations.json")
            
            return result
        else:
            print("❌ Недостаточно данных для корреляции")
            return None
            
    except Exception as e:
        print(f"❌ Ошибка создания корреляций: {e}")
        return None

def main():
    """Главная функция"""
    
    print("🏖️ ДЕТАЛЬНЫЙ АНАЛИЗ ТУРИСТИЧЕСКИХ ДАННЫХ БАЛИ")
    print("=" * 70)
    
    # Анализируем структуру
    df, header_row = analyze_excel_structure()
    
    if df is not None:
        # Ищем паттерны данных
        numeric_rows = find_data_patterns(df)
        
        if numeric_rows:
            # Извлекаем месячные данные
            monthly_data = extract_monthly_data_smart(df, numeric_rows)
            
            if monthly_data:
                # Создаем корреляции
                result = create_tourist_correlations(monthly_data)
                
                if result:
                    print("\n🎉 АНАЛИЗ ТУРИСТИЧЕСКИХ ДАННЫХ ЗАВЕРШЕН!")
                    print("✅ Извлечены реальные туристические данные")
                    print("✅ Рассчитана корреляция с продажами")
                    print("✅ Созданы научные сезонные коэффициенты")
                    
                    print(f"\n📊 РЕЗУЛЬТАТЫ:")
                    print(f"   Корреляция: {result['correlation_tourists_sales']:.3f}")
                    print(f"   Сила связи: {result['analysis_metadata']['correlation_strength']}")
                    print(f"   Туристов проанализировано: {result['analysis_metadata']['total_tourists']:,}")
                    
                    return True
    
    print("❌ Не удалось проанализировать файл")
    return False

if __name__ == "__main__":
    main()