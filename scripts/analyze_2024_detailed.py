#!/usr/bin/env python3
"""
📊 ДЕТАЛЬНЫЙ АНАЛИЗ ТУРИСТИЧЕСКИХ ДАННЫХ 2024
═══════════════════════════════════════════════════════════════════════════════
Анализирует туристические данные Бали за 2024 год по месяцам и национальностям
"""

import pandas as pd
import json
from datetime import datetime

def analyze_tourist_data_2024():
    """Анализирует туристические данные за 2024 год"""
    
    try:
        # Читаем файл с туристическими данными
        df = pd.read_excel('Kunjungan_Wisatawan_Bali_2024.xls', header=2)
        
        print("🏝️ АНАЛИЗ ТУРИСТИЧЕСКИХ ДАННЫХ БАЛИ 2024")
        print("=" * 60)
        
        # Основная информация
        print(f"📊 Размер данных: {df.shape}")
        print(f"📅 Колонки: {list(df.columns)}")
        print()
        
        # Анализ по месяцам
        monthly_data = {}
        total_tourists = 0
        
        months = ['Januari', 'Februari', 'Maret', 'April', 'Mei', 'Juni', 
                 'Juli', 'Agustus', 'September', 'Oktober', 'November', 'Desember']
        
        for i, month in enumerate(months, 1):
            if month in df.columns:
                month_total = df[month].sum()
                monthly_data[f"{i:02d}"] = int(month_total) if pd.notna(month_total) else 0
                total_tourists += monthly_data[f"{i:02d}"]
                print(f"📅 {month}: {monthly_data[f'{i:02d}']:,} туристов")
        
        print()
        print(f"🎯 ИТОГО за 2024: {total_tourists:,} туристов")
        
        # Анализ по странам
        print("\n🌍 ТОП-10 СТРАН:")
        country_totals = {}
        
        for month in months:
            if month in df.columns:
                for idx, row in df.iterrows():
                    if pd.notna(row.get('Negara', '')) and pd.notna(row.get(month, 0)):
                        country = str(row['Negara']).strip()
                        if country not in country_totals:
                            country_totals[country] = 0
                        country_totals[country] += int(row[month]) if pd.notna(row[month]) else 0
        
        # Сортируем страны по количеству туристов
        top_countries = sorted(country_totals.items(), key=lambda x: x[1], reverse=True)[:10]
        
        for i, (country, total) in enumerate(top_countries, 1):
            percentage = (total / total_tourists * 100) if total_tourists > 0 else 0
            print(f"{i:2}. {country}: {total:,} ({percentage:.1f}%)")
        
        # Сохраняем результаты
        results = {
            'year': 2024,
            'total_tourists': total_tourists,
            'monthly_breakdown': monthly_data,
            'top_countries': dict(top_countries),
            'analysis_date': datetime.now().isoformat()
        }
        
        with open('data/tourist_analysis_2024.json', 'w', encoding='utf-8') as f:
            json.dump(results, f, ensure_ascii=False, indent=2)
        
        print(f"\n✅ Результаты сохранены в data/tourist_analysis_2024.json")
        
        return results
        
    except Exception as e:
        print(f"❌ Ошибка при анализе: {e}")
        return None

if __name__ == "__main__":
    analyze_tourist_data_2024()