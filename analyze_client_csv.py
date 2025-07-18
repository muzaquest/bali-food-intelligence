#!/usr/bin/env python3
"""
Анализ CSV файлов клиента и подготовка к ML
"""

import pandas as pd
import numpy as np
import os
from datetime import datetime, timedelta
import json
import sqlite3

class ClientCSVAnalyzer:
    def __init__(self, csv_folder='.'):
        self.csv_folder = csv_folder
        self.restaurants_df = None
        self.grab_df = None
        self.gojek_df = None
        
    def load_csv_files(self):
        """Загружает все CSV файлы"""
        print("🔍 ЗАГРУЗКА CSV ФАЙЛОВ")
        print("=" * 50)
        
        # Ищем CSV файлы
        csv_files = [f for f in os.listdir(self.csv_folder) if f.endswith('.csv')]
        print(f"Найдено CSV файлов: {len(csv_files)}")
        
        for file in csv_files:
            print(f"  - {file}")
        
        # Загружаем restaurants.csv
        restaurants_file = None
        for file in csv_files:
            if 'restaurant' in file.lower():
                restaurants_file = file
                break
        
        if restaurants_file:
            self.restaurants_df = pd.read_csv(os.path.join(self.csv_folder, restaurants_file))
            print(f"\n✅ Загружен {restaurants_file}: {len(self.restaurants_df)} записей")
        else:
            print("\n❌ Файл restaurants.csv не найден")
        
        # Загружаем grab_stats.csv
        grab_file = None
        for file in csv_files:
            if 'grab' in file.lower():
                grab_file = file
                break
        
        if grab_file:
            self.grab_df = pd.read_csv(os.path.join(self.csv_folder, grab_file))
            print(f"✅ Загружен {grab_file}: {len(self.grab_df)} записей")
        else:
            print("❌ Файл grab_stats.csv не найден")
        
        # Загружаем gojek_stats.csv
        gojek_file = None
        for file in csv_files:
            if 'gojek' in file.lower():
                gojek_file = file
                break
        
        if gojek_file:
            self.gojek_df = pd.read_csv(os.path.join(self.csv_folder, gojek_file))
            print(f"✅ Загружен {gojek_file}: {len(self.gojek_df)} записей")
        else:
            print("❌ Файл gojek_stats.csv не найден")
        
        return True
    
    def analyze_structure(self):
        """Анализирует структуру данных"""
        print("\n🔍 АНАЛИЗ СТРУКТУРЫ ДАННЫХ")
        print("=" * 50)
        
        # Анализ ресторанов
        if self.restaurants_df is not None:
            print("\n🏪 ТАБЛИЦА РЕСТОРАНОВ:")
            print(f"  Записей: {len(self.restaurants_df)}")
            print(f"  Колонки: {list(self.restaurants_df.columns)}")
            
            if 'name' in self.restaurants_df.columns:
                print("  Примеры названий:")
                for name in self.restaurants_df['name'].head(5):
                    print(f"    - {name}")
            
            print("  Пример данных:")
            print(self.restaurants_df.head(3).to_string())
        
        # Анализ Grab данных
        if self.grab_df is not None:
            print("\n🟢 GRAB СТАТИСТИКА:")
            print(f"  Записей: {len(self.grab_df)}")
            print(f"  Колонки: {list(self.grab_df.columns)}")
            
            # Анализ дат
            if 'date' in self.grab_df.columns:
                self.grab_df['date'] = pd.to_datetime(self.grab_df['date'])
                print(f"  Период: {self.grab_df['date'].min()} - {self.grab_df['date'].max()}")
                print(f"  Дней с данными: {self.grab_df['date'].nunique()}")
            
            # Анализ продаж
            sales_columns = [col for col in self.grab_df.columns if 'sales' in col.lower() or 'revenue' in col.lower()]
            if sales_columns:
                print(f"  Колонки продаж: {sales_columns}")
                for col in sales_columns:
                    if pd.api.types.is_numeric_dtype(self.grab_df[col]):
                        print(f"    {col}: среднее = {self.grab_df[col].mean():,.0f}")
            
            print("  Пример данных:")
            print(self.grab_df.head(3).to_string())
        
        # Анализ Gojek данных
        if self.gojek_df is not None:
            print("\n🔵 GOJEK СТАТИСТИКА:")
            print(f"  Записей: {len(self.gojek_df)}")
            print(f"  Колонки: {list(self.gojek_df.columns)}")
            
            # Анализ дат
            if 'date' in self.gojek_df.columns:
                self.gojek_df['date'] = pd.to_datetime(self.gojek_df['date'])
                print(f"  Период: {self.gojek_df['date'].min()} - {self.gojek_df['date'].max()}")
                print(f"  Дней с данными: {self.gojek_df['date'].nunique()}")
            
            # Анализ продаж
            sales_columns = [col for col in self.gojek_df.columns if 'sales' in col.lower() or 'revenue' in col.lower()]
            if sales_columns:
                print(f"  Колонки продаж: {sales_columns}")
                for col in sales_columns:
                    if pd.api.types.is_numeric_dtype(self.gojek_df[col]):
                        print(f"    {col}: среднее = {self.gojek_df[col].mean():,.0f}")
            
            print("  Пример данных:")
            print(self.gojek_df.head(3).to_string())
    
    def analyze_data_quality(self):
        """Анализирует качество данных"""
        print("\n🎯 АНАЛИЗ КАЧЕСТВА ДАННЫХ")
        print("=" * 50)
        
        # Анализ полноты данных
        datasets = [
            ("Рестораны", self.restaurants_df),
            ("Grab", self.grab_df),
            ("Gojek", self.gojek_df)
        ]
        
        for name, df in datasets:
            if df is not None:
                print(f"\n📊 {name.upper()}:")
                print(f"  Всего записей: {len(df)}")
                
                # Анализ пропущенных значений
                missing_data = df.isnull().sum()
                if missing_data.sum() > 0:
                    print("  Пропущенные значения:")
                    for col, missing in missing_data.items():
                        if missing > 0:
                            percentage = (missing / len(df)) * 100
                            print(f"    {col}: {missing} ({percentage:.1f}%)")
                else:
                    print("  ✅ Пропущенных значений нет")
                
                # Анализ дубликатов
                if len(df) > 0:
                    duplicates = df.duplicated().sum()
                    if duplicates > 0:
                        print(f"  ⚠️ Дубликатов: {duplicates}")
                    else:
                        print("  ✅ Дубликатов нет")
    
    def create_combined_dataset(self):
        """Создает объединенный датасет для ML"""
        print("\n🔧 СОЗДАНИЕ ОБЪЕДИНЕННОГО ДАТАСЕТА")
        print("=" * 50)
        
        combined_data = []
        
        # Обрабатываем Grab данные
        if self.grab_df is not None:
            grab_processed = self.grab_df.copy()
            grab_processed['platform'] = 'grab'
            combined_data.append(grab_processed)
            print(f"✅ Добавлены Grab данные: {len(grab_processed)} записей")
        
        # Обрабатываем Gojek данные
        if self.gojek_df is not None:
            gojek_processed = self.gojek_df.copy()
            gojek_processed['platform'] = 'gojek'
            combined_data.append(gojek_processed)
            print(f"✅ Добавлены Gojek данные: {len(gojek_processed)} записей")
        
        if not combined_data:
            print("❌ Нет данных для объединения")
            return None
        
        # Объединяем данные
        combined_df = pd.concat(combined_data, ignore_index=True)
        
        # Добавляем информацию о ресторанах
        if self.restaurants_df is not None:
            # Пытаемся найти колонку для связи
            restaurant_id_col = None
            for col in combined_df.columns:
                if 'restaurant' in col.lower() and 'id' in col.lower():
                    restaurant_id_col = col
                    break
            
            if restaurant_id_col:
                combined_df = combined_df.merge(
                    self.restaurants_df,
                    left_on=restaurant_id_col,
                    right_on='id',
                    how='left',
                    suffixes=('', '_restaurant')
                )
                print(f"✅ Добавлена информация о ресторанах")
        
        print(f"📊 Итоговый датасет: {len(combined_df)} записей")
        print(f"📊 Колонки: {list(combined_df.columns)}")
        
        return combined_df
    
    def prepare_for_ml(self, combined_df):
        """Подготавливает данные для ML"""
        print("\n🤖 ПОДГОТОВКА ДЛЯ ML")
        print("=" * 50)
        
        if combined_df is None:
            return None
        
        # Определяем ключевые колонки
        key_columns = {
            'date': None,
            'restaurant_id': None,
            'restaurant_name': None,
            'sales': None,
            'orders': None,
            'region': None
        }
        
        # Ищем колонки
        for col in combined_df.columns:
            col_lower = col.lower()
            
            if 'date' in col_lower and key_columns['date'] is None:
                key_columns['date'] = col
            elif 'restaurant_id' == col_lower:
                key_columns['restaurant_id'] = col
            elif 'name' in col_lower and key_columns['restaurant_name'] is None:
                key_columns['restaurant_name'] = col
            elif 'total_sales' in col_lower:
                key_columns['sales'] = col
            elif 'sales' in col_lower and key_columns['sales'] is None:
                if pd.api.types.is_numeric_dtype(combined_df[col]):
                    key_columns['sales'] = col
            elif 'total_orders' in col_lower:
                key_columns['orders'] = col
            elif 'orders' in col_lower and key_columns['orders'] is None:
                if pd.api.types.is_numeric_dtype(combined_df[col]):
                    key_columns['orders'] = col
            elif 'region' in col_lower or 'location' in col_lower:
                key_columns['region'] = col
        
        print("🔍 Найденные ключевые колонки:")
        for key, col in key_columns.items():
            status = "✅" if col else "❌"
            print(f"  {status} {key}: {col}")
        
        # Создаем ML датасет
        ml_columns = []
        column_mapping = {}
        
        for key, col in key_columns.items():
            if col and col in combined_df.columns:
                ml_columns.append(col)
                column_mapping[col] = key
        
        if not ml_columns:
            print("❌ Не найдены ключевые колонки для ML")
            return None
        
        ml_df = combined_df[ml_columns].copy()
        
        # Переименовываем колонки
        rename_dict = {col: key for col, key in column_mapping.items()}
        ml_df = ml_df.rename(columns=rename_dict)
        
        # Обрабатываем даты
        if 'date' in ml_df.columns:
            ml_df['date'] = pd.to_datetime(ml_df['date'])
        
        # Добавляем недостающие колонки со значениями по умолчанию
        if 'region' not in ml_df.columns:
            ml_df['region'] = 'Unknown'
        
        if 'orders' not in ml_df.columns and 'sales' in ml_df.columns:
            # Оцениваем количество заказов на основе продаж
            ml_df['orders'] = (ml_df['sales'] / 50000).round().astype(int)  # Предполагаем средний чек 50k IDR
        
        # Добавляем дополнительные поля
        if 'sales' in ml_df.columns and 'orders' in ml_df.columns:
            ml_df['avg_order_value'] = ml_df['sales'] / ml_df['orders'].replace(0, 1)
        
        ml_df['ads_enabled'] = True  # По умолчанию
        ml_df['rating'] = 4.0  # По умолчанию
        ml_df['delivery_time'] = 30  # По умолчанию
        
        print(f"✅ ML датасет готов: {len(ml_df)} записей")
        print(f"📊 Колонки: {list(ml_df.columns)}")
        
        return ml_df
    
    def save_to_sqlite(self, ml_df, db_path='client_data.db'):
        """Сохраняет данные в SQLite для ML системы"""
        print(f"\n💾 СОХРАНЕНИЕ В SQLITE: {db_path}")
        print("=" * 50)
        
        if ml_df is None:
            print("❌ Нет данных для сохранения")
            return False
        
        # Создаем подключение к базе данных
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Создаем таблицу ресторанов
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS restaurants (
                id INTEGER PRIMARY KEY,
                name TEXT,
                region TEXT,
                rating REAL,
                avg_delivery_time INTEGER,
                commission_rate REAL
            )
        ''')
        
        # Заполняем таблицу ресторанов
        if 'restaurant_name' in ml_df.columns:
            restaurants = ml_df[['restaurant_name', 'region']].drop_duplicates()
            
            for idx, row in restaurants.iterrows():
                cursor.execute('''
                    INSERT OR IGNORE INTO restaurants (name, region, rating, avg_delivery_time, commission_rate)
                    VALUES (?, ?, ?, ?, ?)
                ''', (row['restaurant_name'], row['region'], 4.0, 30, 0.25))
        
        # Создаем таблицу статистики
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS grab_stats (
                id INTEGER PRIMARY KEY,
                restaurant_id INTEGER,
                date DATE,
                sales REAL,
                orders INTEGER,
                avg_order_value REAL,
                ads_spend REAL,
                ads_enabled BOOLEAN,
                rating REAL,
                delivery_time INTEGER
            )
        ''')
        
        # Получаем соответствие имен ресторанов и их ID
        cursor.execute('SELECT id, name FROM restaurants')
        restaurant_mapping = {name: id for id, name in cursor.fetchall()}
        
        # Заполняем статистику
        for _, row in ml_df.iterrows():
            restaurant_name = row.get('restaurant_name', 'Unknown')
            restaurant_id = restaurant_mapping.get(restaurant_name, 1)
            
            cursor.execute('''
                INSERT INTO grab_stats 
                (restaurant_id, date, sales, orders, avg_order_value, ads_spend, ads_enabled, rating, delivery_time)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                restaurant_id,
                str(row.get('date')) if row.get('date') is not None else None,
                row.get('sales', 0),
                row.get('orders', 0),
                row.get('avg_order_value', 0),
                0,  # ads_spend
                row.get('ads_enabled', True),
                row.get('rating', 4.0),
                row.get('delivery_time', 30)
            ))
        
        conn.commit()
        conn.close()
        
        print(f"✅ Данные сохранены в {db_path}")
        print(f"📊 Ресторанов: {len(restaurants) if 'restaurants' in locals() else 0}")
        print(f"📊 Записей продаж: {len(ml_df)}")
        
        return True
    
    def generate_summary_report(self, ml_df):
        """Генерирует сводный отчет"""
        print("\n📊 СВОДНЫЙ ОТЧЕТ")
        print("=" * 50)
        
        if ml_df is None:
            print("❌ Нет данных для отчета")
            return
        
        # Общая статистика
        print("📈 ОБЩАЯ СТАТИСТИКА:")
        if 'date' in ml_df.columns:
            print(f"  Период данных: {ml_df['date'].min()} - {ml_df['date'].max()}")
            print(f"  Дней с данными: {ml_df['date'].nunique()}")
        
        if 'restaurant_name' in ml_df.columns:
            print(f"  Количество ресторанов: {ml_df['restaurant_name'].nunique()}")
        
        print(f"  Всего записей: {len(ml_df)}")
        
        # Статистика по продажам
        if 'sales' in ml_df.columns:
            print(f"\n💰 ПРОДАЖИ:")
            print(f"  Общие продажи: {ml_df['sales'].sum():,.0f} IDR")
            print(f"  Средние дневные продажи: {ml_df['sales'].mean():,.0f} IDR")
            print(f"  Максимальные продажи за день: {ml_df['sales'].max():,.0f} IDR")
            print(f"  Минимальные продажи за день: {ml_df['sales'].min():,.0f} IDR")
        
        # Топ ресторанов
        if 'restaurant_name' in ml_df.columns and 'sales' in ml_df.columns:
            print(f"\n🏆 ТОП РЕСТОРАНОВ ПО ПРОДАЖАМ:")
            top_restaurants = ml_df.groupby('restaurant_name')['sales'].sum().sort_values(ascending=False).head(5)
            for i, (name, sales) in enumerate(top_restaurants.items(), 1):
                print(f"  {i}. {name}: {sales:,.0f} IDR")
        
        # Региональная статистика
        if 'region' in ml_df.columns and 'sales' in ml_df.columns:
            print(f"\n🗺️ ПО РЕГИОНАМ:")
            regional_stats = ml_df.groupby('region')['sales'].agg(['sum', 'mean', 'count'])
            for region, stats in regional_stats.iterrows():
                print(f"  {region}: {stats['sum']:,.0f} IDR (среднее: {stats['mean']:,.0f}, записей: {stats['count']})")
    
    def run_full_analysis(self):
        """Запускает полный анализ"""
        print("🚀 ПОЛНЫЙ АНАЛИЗ CSV ДАННЫХ КЛИЕНТА")
        print("=" * 60)
        
        # Загружаем файлы
        if not self.load_csv_files():
            return False
        
        # Анализируем структуру
        self.analyze_structure()
        
        # Анализируем качество
        self.analyze_data_quality()
        
        # Создаем объединенный датасет
        combined_df = self.create_combined_dataset()
        
        # Подготавливаем для ML
        ml_df = self.prepare_for_ml(combined_df)
        
        # Сохраняем в SQLite
        if ml_df is not None:
            self.save_to_sqlite(ml_df)
            
            # Сохраняем CSV для проверки
            ml_df.to_csv('client_ml_data.csv', index=False)
            print(f"✅ ML данные также сохранены в client_ml_data.csv")
        
        # Генерируем отчет
        self.generate_summary_report(ml_df)
        
        print("\n🎉 АНАЛИЗ ЗАВЕРШЕН!")
        print("=" * 60)
        print("📋 Следующие шаги:")
        print("1. Проверьте файл client_data.db")
        print("2. Запустите: python3 main.py train")
        print("3. Протестируйте: python3 main.py analyze --restaurant_id 1 --date 2024-01-15")
        
        return True

def main():
    print("🔍 Анализатор CSV данных клиента")
    print("Поместите CSV файлы в текущую папку и запустите скрипт")
    print("=" * 60)
    
    analyzer = ClientCSVAnalyzer()
    analyzer.run_full_analysis()

if __name__ == "__main__":
    main()