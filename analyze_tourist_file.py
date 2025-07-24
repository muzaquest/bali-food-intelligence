#!/usr/bin/env python3
"""
🏖️ АНАЛИЗ РЕАЛЬНОГО ФАЙЛА ТУРИСТИЧЕСКИХ ДАННЫХ БАЛИ
Анализирует Kunjungan_Wisatawan_Bali_2025.xls и создает научные коэффициенты
"""

import pandas as pd
import numpy as np
import json
from datetime import datetime
import sqlite3

def analyze_tourist_excel_file():
    """Анализирует Excel файл с туристическими данными"""
    
    print("🏖️ АНАЛИЗ РЕАЛЬНОГО ФАЙЛА ТУРИСТИЧЕСКИХ ДАННЫХ БАЛИ")
    print("=" * 70)
    
    try:
        # Читаем Excel файл
        print("📂 Загрузка файла Kunjungan_Wisatawan_Bali_2025.xls...")
        
        # Пробуем разные способы чтения файла
        try:
            # Сначала пробуем openpyxl
            df = pd.read_excel('Kunjungan_Wisatawan_Bali_2025.xls', engine='openpyxl')
        except:
            try:
                # Если не получилось, пробуем xlrd
                df = pd.read_excel('Kunjungan_Wisatawan_Bali_2025.xls', engine='xlrd')
            except:
                # Если и это не получилось, читаем первый лист
                df = pd.read_excel('Kunjungan_Wisatawan_Bali_2025.xls', sheet_name=0)
        
        print(f"✅ Файл загружен! Размер: {df.shape[0]} строк, {df.shape[1]} столбцов")
        
        # Показываем структуру данных
        print("\n📊 СТРУКТУРА ДАННЫХ:")
        print("-" * 50)
        print("Столбцы:", list(df.columns))
        print("\nПервые 5 строк:")
        print(df.head())
        
        # Ищем столбцы с месяцами или датами
        month_columns = []
        date_columns = []
        
        for col in df.columns:
            col_str = str(col).lower()
            if any(month in col_str for month in ['jan', 'feb', 'mar', 'apr', 'may', 'jun', 
                                                  'jul', 'aug', 'sep', 'oct', 'nov', 'dec',
                                                  'januari', 'februari', 'maret', 'april', 'mei', 'juni',
                                                  'juli', 'agustus', 'september', 'oktober', 'november', 'desember']):
                month_columns.append(col)
            elif any(date_word in col_str for date_word in ['date', 'tanggal', 'bulan', 'month']):
                date_columns.append(col)
        
        print(f"\n🗓️ Найдено столбцов с месяцами: {len(month_columns)}")
        if month_columns:
            print("   ", month_columns[:10])  # Показываем первые 10
            
        print(f"📅 Найдено столбцов с датами: {len(date_columns)}")
        if date_columns:
            print("   ", date_columns)
        
        return df, month_columns, date_columns
        
    except Exception as e:
        print(f"❌ Ошибка загрузки файла: {e}")
        return None, [], []

def extract_monthly_tourist_data(df, month_columns):
    """Извлекает месячные данные о туристах"""
    
    print("\n🔍 ИЗВЛЕЧЕНИЕ МЕСЯЧНЫХ ДАННЫХ О ТУРИСТАХ")
    print("=" * 60)
    
    try:
        if not month_columns:
            print("⚠️ Месячные столбцы не найдены, пробуем альтернативный анализ...")
            return analyze_alternative_structure(df)
        
        # Пробуем найти строки с суммарными данными
        tourist_data = {}
        
        # Ищем строки с общими данными (Total, Jumlah, Sum и т.д.)
        total_rows = df[df.iloc[:, 0].astype(str).str.contains('total|jumlah|sum|всего', case=False, na=False)]
        
        if len(total_rows) > 0:
            print(f"✅ Найдено {len(total_rows)} строк с суммарными данными")
            
            # Берем первую строку с суммами
            total_row = total_rows.iloc[0]
            
            # Извлекаем данные по месяцам
            for col in month_columns:
                try:
                    value = pd.to_numeric(total_row[col], errors='coerce')
                    if pd.notna(value) and value > 0:
                        # Определяем номер месяца по названию столбца
                        month_num = get_month_number_from_column(col)
                        if month_num:
                            tourist_data[month_num] = {
                                'tourists': int(value),
                                'column_name': col,
                                'source': 'Суммарная строка'
                            }
                except:
                    continue
        
        # Если не нашли суммарные строки, пробуем суммировать все строки
        if not tourist_data:
            print("⚠️ Суммарные строки не найдены, пробуем суммировать данные...")
            
            for col in month_columns:
                try:
                    # Суммируем все числовые значения в столбце
                    numeric_values = pd.to_numeric(df[col], errors='coerce')
                    total_value = numeric_values.sum()
                    
                    if total_value > 0:
                        month_num = get_month_number_from_column(col)
                        if month_num:
                            tourist_data[month_num] = {
                                'tourists': int(total_value),
                                'column_name': col,
                                'source': 'Сумма по столбцу'
                            }
                except:
                    continue
        
        if tourist_data:
            print(f"✅ Извлечено данных по {len(tourist_data)} месяцам")
            
            # Показываем результаты
            print("\n📊 ТУРИСТИЧЕСКИЕ ДАННЫЕ ПО МЕСЯЦАМ:")
            print("-" * 60)
            
            month_names = [
                "Январь", "Февраль", "Март", "Апрель", "Май", "Июнь",
                "Июль", "Август", "Сентябрь", "Октябрь", "Ноябрь", "Декабрь"
            ]
            
            total_tourists = sum(data['tourists'] for data in tourist_data.values())
            
            for month_num in sorted(tourist_data.keys()):
                data = tourist_data[month_num]
                month_name = month_names[month_num - 1]
                tourists = data['tourists']
                percentage = (tourists / total_tourists) * 100 if total_tourists > 0 else 0
                
                print(f"{month_num:2d}. {month_name:10} | {tourists:>10,} туристов | {percentage:5.1f}% | {data['source']}")
            
            print(f"\n📈 ИТОГО: {total_tourists:,} туристов за год")
            
            return tourist_data
        else:
            print("❌ Не удалось извлечь туристические данные")
            return None
            
    except Exception as e:
        print(f"❌ Ошибка извлечения данных: {e}")
        return None

def get_month_number_from_column(col_name):
    """Определяет номер месяца по названию столбца"""
    
    col_str = str(col_name).lower()
    
    # Английские месяцы
    en_months = {
        'jan': 1, 'feb': 2, 'mar': 3, 'apr': 4, 'may': 5, 'jun': 6,
        'jul': 7, 'aug': 8, 'sep': 9, 'oct': 10, 'nov': 11, 'dec': 12
    }
    
    # Индонезийские месяцы
    id_months = {
        'januari': 1, 'februari': 2, 'maret': 3, 'april': 4, 'mei': 5, 'juni': 6,
        'juli': 7, 'agustus': 8, 'september': 9, 'oktober': 10, 'november': 11, 'desember': 12
    }
    
    # Русские месяцы
    ru_months = {
        'январь': 1, 'февраль': 2, 'март': 3, 'апрель': 4, 'май': 5, 'июнь': 6,
        'июль': 7, 'август': 8, 'сентябрь': 9, 'октябрь': 10, 'ноябрь': 11, 'декабрь': 12
    }
    
    all_months = {**en_months, **id_months, **ru_months}
    
    for month_name, month_num in all_months.items():
        if month_name in col_str:
            return month_num
    
    # Пробуем найти числа (1-12)
    import re
    numbers = re.findall(r'\b(\d{1,2})\b', col_str)
    for num_str in numbers:
        num = int(num_str)
        if 1 <= num <= 12:
            return num
    
    return None

def analyze_alternative_structure(df):
    """Альтернативный анализ структуры файла"""
    
    print("\n🔍 АЛЬТЕРНАТИВНЫЙ АНАЛИЗ СТРУКТУРЫ ФАЙЛА")
    print("=" * 60)
    
    # Ищем любые числовые данные, которые могут быть туристическими
    numeric_columns = df.select_dtypes(include=[np.number]).columns
    
    print(f"📊 Найдено {len(numeric_columns)} числовых столбцов")
    
    if len(numeric_columns) > 0:
        print("Числовые столбцы:", list(numeric_columns))
        
        # Показываем статистику по числовым столбцам
        print("\n📈 СТАТИСТИКА ПО ЧИСЛОВЫМ ДАННЫМ:")
        print(df[numeric_columns].describe())
    
    # Ищем столбцы с большими числами (потенциально туристы)
    potential_tourist_columns = []
    
    for col in numeric_columns:
        max_val = df[col].max()
        if pd.notna(max_val) and max_val > 1000:  # Предполагаем, что туристов больше 1000
            potential_tourist_columns.append(col)
    
    print(f"\n🏖️ Потенциальные столбцы с туристическими данными: {potential_tourist_columns}")
    
    return None

def create_real_tourist_correlations(tourist_data):
    """Создает реальные туристические корреляции с продажами"""
    
    print("\n🔬 СОЗДАНИЕ РЕАЛЬНЫХ ТУРИСТИЧЕСКИХ КОРРЕЛЯЦИЙ")
    print("=" * 60)
    
    if not tourist_data:
        print("❌ Нет туристических данных для корреляции")
        return None
    
    try:
        # Загружаем данные продаж из базы данных
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
        
        # Группируем продажи по месяцам
        monthly_sales = sales_df.groupby('month')['sales'].mean().reset_index()
        
        print(f"✅ Загружено продаж по {len(monthly_sales)} месяцам")
        
        # Создаем объединенный датасет
        correlation_data = []
        
        for month_num in range(1, 13):
            tourists = tourist_data.get(month_num, {}).get('tourists', 0)
            sales_data = monthly_sales[monthly_sales['month'] == month_num]
            avg_sales = sales_data['sales'].iloc[0] if len(sales_data) > 0 else 0
            
            if tourists > 0 and avg_sales > 0:
                correlation_data.append({
                    'month': month_num,
                    'tourists': tourists,
                    'avg_sales': avg_sales
                })
        
        if len(correlation_data) >= 3:  # Нужно минимум 3 точки для корреляции
            # Рассчитываем корреляцию
            df_corr = pd.DataFrame(correlation_data)
            
            correlation = df_corr['tourists'].corr(df_corr['avg_sales'])
            
            print(f"🔗 КОРРЕЛЯЦИЯ ТУРИСТЫ ↔ ПРОДАЖИ: {correlation:.3f}")
            
            # Создаем новые туристические коэффициенты
            total_tourists = sum(item['tourists'] for item in correlation_data)
            avg_tourists_per_month = total_tourists / len(correlation_data)
            
            real_tourist_coefficients = {}
            
            for item in correlation_data:
                month_num = item['month']
                tourists = item['tourists']
                coefficient = tourists / avg_tourists_per_month  # Коэффициент относительно среднего
                
                real_tourist_coefficients[month_num] = {
                    'coefficient': coefficient,
                    'tourists': tourists,
                    'avg_sales': item['avg_sales']
                }
            
            # Классифицируем сезоны на основе реальных туристических данных
            coefficients = [data['coefficient'] for data in real_tourist_coefficients.values()]
            mean_coeff = np.mean(coefficients)
            std_coeff = np.std(coefficients)
            
            high_threshold = mean_coeff + 0.3 * std_coeff
            low_threshold = mean_coeff - 0.3 * std_coeff
            
            real_seasons = {
                'высокий_сезон': {'месяцы': [], 'коэффициент': 0, 'описание': 'Пик туристического сезона (реальные данные)'},
                'средний_сезон': {'месяцы': [], 'коэффициент': 0, 'описание': 'Средний туристический сезон (реальные данные)'},
                'низкий_сезон': {'месяцы': [], 'коэффициент': 0, 'описание': 'Низкий туристический сезон (реальные данные)'}
            }
            
            # Классифицируем месяцы
            for month_num, data in real_tourist_coefficients.items():
                coeff = data['coefficient']
                
                if coeff >= high_threshold:
                    real_seasons['высокий_сезон']['месяцы'].append(month_num)
                elif coeff <= low_threshold:
                    real_seasons['низкий_сезон']['месяцы'].append(month_num)
                else:
                    real_seasons['средний_сезон']['месяцы'].append(month_num)
            
            # Рассчитываем средние коэффициенты для сезонов
            for season_name, season_data in real_seasons.items():
                if season_data['месяцы']:
                    season_coeffs = [real_tourist_coefficients[month]['coefficient'] for month in season_data['месяцы']]
                    season_data['коэффициент'] = float(np.mean(season_coeffs))
                else:
                    season_data['коэффициент'] = 1.0
            
            print("\n✅ РЕАЛЬНАЯ КЛАССИФИКАЦИЯ СЕЗОНОВ (на основе туристических данных):")
            print("-" * 70)
            
            month_names = [
                "Январь", "Февраль", "Март", "Апрель", "Май", "Июнь",
                "Июль", "Август", "Сентябрь", "Октябрь", "Ноябрь", "Декабрь"
            ]
            
            for season_name, season_data in real_seasons.items():
                months_names = [month_names[month-1] for month in season_data['месяцы']]
                print(f"{season_name.upper():15} | {season_data['коэффициент']:5.2f} | {', '.join(months_names)}")
            
            # Сохраняем результаты
            real_tourist_data = {
                'seasonal_patterns': real_seasons,
                'monthly_coefficients': real_tourist_coefficients,
                'correlation_tourists_sales': float(correlation),
                'analysis_metadata': {
                    'source': 'Real tourist arrivals data (Kunjungan_Wisatawan_Bali_2025.xls)',
                    'total_tourists_analyzed': total_tourists,
                    'months_with_data': len(correlation_data),
                    'correlation_strength': 'Strong' if abs(correlation) > 0.7 else 'Moderate' if abs(correlation) > 0.4 else 'Weak',
                    'created_at': datetime.now().isoformat()
                }
            }
            
            with open('real_tourist_correlations.json', 'w', encoding='utf-8') as f:
                json.dump(real_tourist_data, f, indent=2, ensure_ascii=False)
            
            print(f"\n💾 Реальные туристические корреляции сохранены в real_tourist_correlations.json")
            
            return real_tourist_data
            
        else:
            print("❌ Недостаточно данных для расчета корреляции")
            return None
            
    except Exception as e:
        print(f"❌ Ошибка создания корреляций: {e}")
        return None

def main():
    """Главная функция анализа туристического файла"""
    
    print("🏖️ АНАЛИЗ РЕАЛЬНОГО ФАЙЛА ТУРИСТИЧЕСКИХ ДАННЫХ БАЛИ")
    print("=" * 80)
    
    # Анализируем Excel файл
    df, month_columns, date_columns = analyze_tourist_excel_file()
    
    if df is not None:
        # Извлекаем туристические данные
        tourist_data = extract_monthly_tourist_data(df, month_columns)
        
        if tourist_data:
            # Создаем реальные корреляции с продажами
            correlations = create_real_tourist_correlations(tourist_data)
            
            if correlations:
                print("\n🎉 АНАЛИЗ ТУРИСТИЧЕСКИХ ДАННЫХ ЗАВЕРШЕН!")
                print("✅ Созданы реальные туристические коэффициенты")
                print("✅ Рассчитана корреляция туристы ↔ продажи")
                print("✅ Заменены эмпирические данные на научные")
                
                # Показываем сравнение
                print(f"\n📊 КОРРЕЛЯЦИЯ: {correlations['correlation_tourists_sales']:.3f}")
                print(f"🎯 СИЛА СВЯЗИ: {correlations['analysis_metadata']['correlation_strength']}")
                
                return True
            else:
                print("❌ Не удалось создать корреляции")
                return False
        else:
            print("❌ Не удалось извлечь туристические данные")
            return False
    else:
        print("❌ Не удалось загрузить файл")
        return False

if __name__ == "__main__":
    success = main()
    if not success:
        print("\n💡 РЕКОМЕНДАЦИИ:")
        print("1. Проверьте формат файла (возможно, нужна конвертация)")
        print("2. Убедитесь, что файл содержит месячные данные")
        print("3. Возможно, нужно указать конкретный лист Excel файла")