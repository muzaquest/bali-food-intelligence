#!/usr/bin/env python3
"""
🔍 ДЕТАЛЬНЫЙ АНАЛИЗ ФАЙЛА ТУРИСТИЧЕСКИХ ДАННЫХ 2024
Анализирует структуру файла 2024 года для корректного извлечения данных
"""

import pandas as pd
import numpy as np

def analyze_2024_file_structure():
    """Детальный анализ структуры файла 2024"""
    
    print("🔍 ДЕТАЛЬНЫЙ АНАЛИЗ ФАЙЛА 2024")
    print("=" * 50)
    
    try:
        # Читаем файл разными способами
        print("📂 Попытка 1: Чтение с header=2")
        df = pd.read_excel('Kunjungan_Wisatawan_Bali_2024.xls', header=2)
        print(f"   Размер: {df.shape[0]} строк, {df.shape[1]} столбцов")
        print(f"   Столбцы: {list(df.columns)}")
        
        # Показываем первые строки
        print("\n📊 ПЕРВЫЕ 10 СТРОК:")
        print("-" * 40)
        for i in range(min(10, len(df))):
            row = df.iloc[i]
            nationality = str(row.get('NATIONALITY', 'N/A'))[:20]
            jan_val = str(row.get('JAN', 'N/A'))[:10]
            total_val = str(row.get('TOTAL', 'N/A'))[:10]
            print(f"{i:2d}. {nationality:20} | JAN: {jan_val:10} | TOTAL: {total_val:10}")
        
        # Ищем строки с числовыми данными
        print("\n🔍 ПОИСК СТРОК С ЧИСЛОВЫМИ ДАННЫМИ:")
        print("-" * 50)
        
        month_columns = ['JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUNE', 'JULY', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC']
        
        numeric_rows = []
        
        for idx, row in df.iterrows():
            numeric_count = 0
            total_value = 0
            monthly_data = {}
            
            for month_col in month_columns:
                if month_col in df.columns:
                    try:
                        val = pd.to_numeric(row[month_col], errors='coerce')
                        if pd.notna(val) and val > 0:
                            numeric_count += 1
                            total_value += val
                            monthly_data[month_col] = val
                    except:
                        continue
            
            if numeric_count >= 6:  # Минимум 6 месяцев с данными
                nationality = str(row.get('NATIONALITY', 'Unknown')).strip()
                numeric_rows.append({
                    'index': idx,
                    'nationality': nationality,
                    'numeric_months': numeric_count,
                    'total_tourists': total_value,
                    'monthly_data': monthly_data
                })
        
        print(f"✅ Найдено {len(numeric_rows)} строк с числовыми данными")
        
        if numeric_rows:
            # Сортируем по количеству туристов
            numeric_rows.sort(key=lambda x: x['total_tourists'], reverse=True)
            
            print("\n📊 ТОП-15 СТРОК С НАИБОЛЬШИМИ ЧИСЛАМИ:")
            print("-" * 70)
            
            for i, row_data in enumerate(numeric_rows[:15]):
                nationality = row_data['nationality'][:25]
                total = row_data['total_tourists']
                months = row_data['numeric_months']
                print(f"{i+1:2d}. {nationality:25} | {total:>12,.0f} | {months:2d} месяцев")
            
            # Анализируем лучшую строку
            best_row = numeric_rows[0]
            print(f"\n🎯 ДЕТАЛЬНЫЙ АНАЛИЗ ЛУЧШЕЙ СТРОКИ ({best_row['nationality']}):")
            print("-" * 60)
            
            month_names = {
                'JAN': 'Январь', 'FEB': 'Февраль', 'MAR': 'Март', 'APR': 'Апрель',
                'MAY': 'Май', 'JUNE': 'Июнь', 'JULY': 'Июль', 'AUG': 'Август',
                'SEP': 'Сентябрь', 'OCT': 'Октябрь', 'NOV': 'Ноябрь', 'DEC': 'Декабрь'
            }
            
            for month_col, value in best_row['monthly_data'].items():
                month_name = month_names.get(month_col, month_col)
                print(f"   {month_name:10}: {value:>10,.0f}")
            
            return numeric_rows
        else:
            print("❌ Не найдено строк с числовыми данными")
            
            # Попробуем другие заголовки
            print("\n🔄 ПРОБУЕМ ДРУГИЕ ВАРИАНТЫ ЗАГОЛОВКОВ:")
            for header_row in [0, 1, 3, 4, 5]:
                try:
                    df_alt = pd.read_excel('Kunjungan_Wisatawan_Bali_2024.xls', header=header_row)
                    print(f"   Header={header_row}: {df_alt.shape[0]} строк, {df_alt.shape[1]} столбцов")
                    print(f"   Столбцы: {list(df_alt.columns)[:5]}...")
                except Exception as e:
                    print(f"   Header={header_row}: Ошибка - {e}")
            
            return []
        
    except Exception as e:
        print(f"❌ Ошибка анализа файла 2024: {e}")
        return []

def try_alternative_reading():
    """Пробует альтернативные способы чтения файла"""
    
    print("\n🔄 АЛЬТЕРНАТИВНЫЕ СПОСОБЫ ЧТЕНИЯ ФАЙЛА 2024")
    print("=" * 60)
    
    try:
        # Читаем без заголовков
        df_no_header = pd.read_excel('Kunjungan_Wisatawan_Bali_2024.xls', header=None)
        print(f"✅ Без заголовков: {df_no_header.shape[0]} строк, {df_no_header.shape[1]} столбцов")
        
        # Ищем строки с месяцами
        month_patterns = ['jan', 'feb', 'mar', 'apr', 'may', 'jun', 'jul', 'aug', 'sep', 'oct', 'nov', 'dec']
        
        for idx, row in df_no_header.iterrows():
            if idx > 10:  # Проверяем только первые 10 строк
                break
                
            row_text = ' '.join([str(cell).lower() for cell in row if pd.notna(cell)])
            month_count = sum(1 for pattern in month_patterns if pattern in row_text)
            
            if month_count >= 6:
                print(f"✅ Найдена строка с месяцами в строке {idx}: {month_count} месяцев")
                print(f"   Содержимое: {list(row)[:10]}...")
                
                # Пробуем читать с этим заголовком
                df_with_header = pd.read_excel('Kunjungan_Wisatawan_Bali_2024.xls', header=idx)
                print(f"   С заголовком {idx}: {df_with_header.shape[0]} строк, {df_with_header.shape[1]} столбцов")
                print(f"   Новые столбцы: {list(df_with_header.columns)}")
                
                return df_with_header, idx
        
        print("⚠️ Строка с месяцами не найдена в первых 10 строках")
        return None, None
        
    except Exception as e:
        print(f"❌ Ошибка альтернативного чтения: {e}")
        return None, None

def main():
    """Главная функция"""
    
    print("🔍 ДЕТАЛЬНЫЙ АНАЛИЗ ТУРИСТИЧЕСКОГО ФАЙЛА 2024")
    print("=" * 70)
    
    # Основной анализ
    numeric_rows = analyze_2024_file_structure()
    
    if not numeric_rows:
        # Пробуем альтернативные способы
        df_alt, header_idx = try_alternative_reading()
        
        if df_alt is not None:
            print(f"\n✅ Найден альтернативный способ чтения с header={header_idx}")
            # Можно повторить анализ с новым DataFrame
        else:
            print("\n❌ Не удалось найти подходящий способ чтения файла 2024")
    else:
        print(f"\n✅ Анализ завершен, найдено {len(numeric_rows)} строк с данными")

if __name__ == "__main__":
    main()