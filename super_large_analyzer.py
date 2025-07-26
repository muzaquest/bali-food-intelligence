#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from simple_large_analyzer import SimpleLargeAnalyzer
import pandas as pd

class SuperLargeAnalyzer(SimpleLargeAnalyzer):
    def run_super_analysis(self):
        """Запускает супер масштабный анализ"""
        print("🌍 СУПЕР МАСШТАБНЫЙ АНАЛИЗ СО ВСЕМИ РЕСТОРАНАМИ")
        print("=" * 55)
        
        # Загружаем данные
        locations = self.load_restaurant_locations()
        sales_data = self.get_sample_data(8000)  # Большая выборка
        
        if not locations:
            print("❌ Нет координат ресторанов")
            return
        
        # Фильтруем рестораны с координатами
        restaurants_with_coords = [name for name in sales_data['restaurant_name'].unique() 
                                  if name in locations]
        
        print(f"📍 Ресторанов с координатами: {len(restaurants_with_coords)}")
        
        filtered_data = sales_data[sales_data['restaurant_name'].isin(restaurants_with_coords)]
        
        # Собираем погодные данные
        weather_data = []
        processed = 0
        
        print("🌤️ Сбор погодных данных с увеличенной выборкой...")
        
        # Берем уникальные комбинации ресторан-дата
        unique_combos = filtered_data[['restaurant_name', 'date']].drop_duplicates()
        
        # Ограничиваем до 1000 запросов для экономии API и времени
        sample_combos = unique_combos.sample(min(1000, len(unique_combos)), random_state=42)
        
        print(f"📊 Обрабатываем {len(sample_combos)} уникальных комбинаций из {len(unique_combos)} доступных")
        
        for _, row in sample_combos.iterrows():
            restaurant_name = row['restaurant_name']
            date = row['date']
            
            if restaurant_name in locations:
                location = locations[restaurant_name]
                
                weather = self.get_weather(
                    location['latitude'], 
                    location['longitude'], 
                    date
                )
                
                if weather:
                    # Получаем продажи для этого дня
                    day_sales = filtered_data[
                        (filtered_data['restaurant_name'] == restaurant_name) & 
                        (filtered_data['date'] == date)
                    ]
                    
                    if not day_sales.empty:
                        record = {
                            'restaurant': restaurant_name,
                            'date': date,
                            'zone': location.get('zone', 'Unknown'),
                            'area': location.get('area', 'Unknown'),
                            'total_sales': day_sales['total_sales'].sum(),
                            'total_orders': day_sales['total_orders'].sum(),
                            'cancelled_orders': day_sales['grab_cancelled'].fillna(0).sum(),
                            'temperature': weather['temperature'],
                            'rain': weather['rain'],
                            'wind': weather['wind']
                        }
                        weather_data.append(record)
            
            processed += 1
            if processed % 100 == 0:
                print(f"   Обработано: {processed}/{len(sample_combos)} ({len(weather_data)} успешных)")
                import time
                time.sleep(2)  # Пауза для API
        
        if not weather_data:
            print("❌ Не удалось получить погодные данные")
            return
        
        # Анализируем результаты
        df = pd.DataFrame(weather_data)
        print(f"
✅ Собрано для СУПЕР анализа: {len(df):,} записей")
        print(f"📍 Ресторанов в анализе: {df['restaurant'].nunique()}")
        print(f"🌍 Зон в анализе: {df['zone'].nunique()}")
        
        self.analyze_patterns(df)

def main():
    analyzer = SuperLargeAnalyzer()
    analyzer.run_super_analysis()

if __name__ == "__main__":
    main()
