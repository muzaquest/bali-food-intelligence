#!/usr/bin/env python3
"""
Анализ базы данных клиента и подготовка для ML системы
"""

import sqlite3
import pandas as pd
import os
from datetime import datetime, timedelta
import json

def analyze_database_structure(db_path):
    """
    Анализирует структуру базы данных клиента
    """
    print("🔍 АНАЛИЗ СТРУКТУРЫ БАЗЫ ДАННЫХ")
    print("=" * 50)
    
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Получаем список таблиц
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        
        print(f"📊 Найдено таблиц: {len(tables)}")
        
        for table_name in tables:
            table_name = table_name[0]
            print(f"\n🗂️  ТАБЛИЦА: {table_name}")
            print("-" * 30)
            
            # Структура таблицы
            cursor.execute(f"PRAGMA table_info({table_name})")
            columns = cursor.fetchall()
            
            print("📋 Колонки:")
            for col in columns:
                col_name, col_type, not_null, default, pk = col[1], col[2], col[3], col[4], col[5]
                pk_mark = " (PK)" if pk else ""
                null_mark = " NOT NULL" if not_null else ""
                print(f"  - {col_name}: {col_type}{pk_mark}{null_mark}")
            
            # Количество записей
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            count = cursor.fetchone()[0]
            print(f"📊 Записей: {count:,}")
            
            # Пример данных (первые 3 строки)
            if count > 0:
                cursor.execute(f"SELECT * FROM {table_name} LIMIT 3")
                sample_data = cursor.fetchall()
                column_names = [desc[0] for desc in cursor.description]
                
                print("📝 Пример данных:")
                for i, row in enumerate(sample_data, 1):
                    print(f"  Строка {i}:")
                    for col_name, value in zip(column_names, row):
                        print(f"    {col_name}: {value}")
        
        conn.close()
        return True
        
    except Exception as e:
        print(f"❌ Ошибка анализа базы: {e}")
        return False

def analyze_data_quality(db_path):
    """
    Анализирует качество данных для ML
    """
    print("\n🎯 АНАЛИЗ КАЧЕСТВА ДАННЫХ ДЛЯ ML")
    print("=" * 50)
    
    try:
        conn = sqlite3.connect(db_path)
        
        # Анализируем таблицу ресторанов
        print("🏪 АНАЛИЗ РЕСТОРАНОВ:")
        restaurants_df = pd.read_sql_query("SELECT * FROM restaurants", conn)
        print(f"  Всего ресторанов: {len(restaurants_df)}")
        
        if 'name' in restaurants_df.columns:
            print("  Названия ресторанов:")
            for name in restaurants_df['name'].head(10):
                print(f"    - {name}")
        
        # Анализируем Grab данные
        print("\n🟢 АНАЛИЗ GRAB ДАННЫХ:")
        try:
            grab_df = pd.read_sql_query("SELECT * FROM grab_stats", conn)
            print(f"  Записей: {len(grab_df):,}")
            
            if len(grab_df) > 0:
                print("  Колонки:")
                for col in grab_df.columns:
                    non_null_count = grab_df[col].notna().sum()
                    print(f"    - {col}: {non_null_count:,} не-NULL ({non_null_count/len(grab_df)*100:.1f}%)")
                
                # Анализ дат
                if 'date' in grab_df.columns:
                    grab_df['date'] = pd.to_datetime(grab_df['date'])
                    date_range = f"{grab_df['date'].min()} - {grab_df['date'].max()}"
                    print(f"  Период данных: {date_range}")
                    
                    # Проверяем полноту данных по дням
                    days_total = (grab_df['date'].max() - grab_df['date'].min()).days + 1
                    days_with_data = grab_df['date'].nunique()
                    print(f"  Покрытие по дням: {days_with_data}/{days_total} ({days_with_data/days_total*100:.1f}%)")
        
        except Exception as e:
            print(f"  ❌ Ошибка анализа Grab: {e}")
        
        # Анализируем Gojek данные
        print("\n🔵 АНАЛИЗ GOJEK ДАННЫХ:")
        try:
            gojek_df = pd.read_sql_query("SELECT * FROM gojek_stats", conn)
            print(f"  Записей: {len(gojek_df):,}")
            
            if len(gojek_df) > 0:
                print("  Колонки:")
                for col in gojek_df.columns:
                    non_null_count = gojek_df[col].notna().sum()
                    print(f"    - {col}: {non_null_count:,} не-NULL ({non_null_count/len(gojek_df)*100:.1f}%)")
                
                # Анализ дат
                if 'date' in gojek_df.columns:
                    gojek_df['date'] = pd.to_datetime(gojek_df['date'])
                    date_range = f"{gojek_df['date'].min()} - {gojek_df['date'].max()}"
                    print(f"  Период данных: {date_range}")
        
        except Exception as e:
            print(f"  ❌ Ошибка анализа Gojek: {e}")
        
        conn.close()
        return True
        
    except Exception as e:
        print(f"❌ Ошибка анализа качества: {e}")
        return False

def create_ml_data_adapter(db_path):
    """
    Создает адаптер для подготовки данных клиента к ML системе
    """
    print("\n🔧 СОЗДАНИЕ АДАПТЕРА ДАННЫХ")
    print("=" * 50)
    
    adapter_code = '''#!/usr/bin/env python3
"""
Адаптер для данных клиента - подготовка к ML системе
"""

import sqlite3
import pandas as pd
from datetime import datetime, timedelta
import numpy as np

class ClientDataAdapter:
    def __init__(self, db_path):
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
    
    def load_restaurants(self):
        """Загружает список ресторанов"""
        query = """
        SELECT 
            id,
            name,
            COALESCE(region, 'Unknown') as region,
            grab_restaurant_id,
            gojek_restaurant_id,
            connected_date
        FROM restaurants
        """
        return pd.read_sql_query(query, self.conn)
    
    def load_combined_sales_data(self, start_date=None, end_date=None):
        """
        Объединяет данные Grab и Gojek в единый датасет
        """
        date_filter = ""
        if start_date and end_date:
            date_filter = f"WHERE date BETWEEN '{start_date}' AND '{end_date}'"
        
        # Grab данные
        grab_query = f"""
        SELECT 
            restaurant_id,
            date,
            COALESCE(total_sales, sales, 0) as sales,
            COALESCE(total_orders, orders, 0) as orders,
            COALESCE(ads_spend, 0) as ads_spend,
            COALESCE(avg_rating, rating, 0) as rating,
            COALESCE(cancellation_rate, 0) as cancellation_rate,
            COALESCE(avg_preparation_time, 0) as prep_time,
            'grab' as platform
        FROM grab_stats
        {date_filter}
        """
        
        # Gojek данные
        gojek_query = f"""
        SELECT 
            restaurant_id,
            date,
            COALESCE(total_sales, sales, 0) as sales,
            COALESCE(total_orders, orders, 0) as orders,
            COALESCE(ads_spend, 0) as ads_spend,
            COALESCE(avg_rating, rating, 0) as rating,
            COALESCE(cancellation_rate, 0) as cancellation_rate,
            COALESCE(avg_preparation_time, 0) as prep_time,
            'gojek' as platform
        FROM gojek_stats
        {date_filter}
        """
        
        try:
            grab_df = pd.read_sql_query(grab_query, self.conn)
            gojek_df = pd.read_sql_query(gojek_query, self.conn)
            
            # Объединяем данные
            combined_df = pd.concat([grab_df, gojek_df], ignore_index=True)
            
            # Преобразуем даты
            combined_df['date'] = pd.to_datetime(combined_df['date'])
            
            # Добавляем информацию о ресторанах
            restaurants_df = self.load_restaurants()
            combined_df = combined_df.merge(
                restaurants_df[['id', 'name', 'region']], 
                left_on='restaurant_id', 
                right_on='id', 
                how='left'
            )
            
            return combined_df
            
        except Exception as e:
            print(f"❌ Ошибка загрузки данных: {e}")
            return pd.DataFrame()
    
    def prepare_for_ml(self, df):
        """
        Подготавливает данные для ML системы
        """
        # Группируем по ресторану и дате (суммируем Grab + Gojek)
        ml_df = df.groupby(['restaurant_id', 'name', 'region', 'date']).agg({
            'sales': 'sum',
            'orders': 'sum',
            'ads_spend': 'sum',
            'rating': 'mean',
            'cancellation_rate': 'mean',
            'prep_time': 'mean'
        }).reset_index()
        
        # Вычисляем дополнительные метрики
        ml_df['avg_order_value'] = ml_df['sales'] / ml_df['orders'].replace(0, 1)
        ml_df['ads_enabled'] = ml_df['ads_spend'] > 0
        
        # Переименовываем колонки для совместимости с ML системой
        ml_df = ml_df.rename(columns={
            'name': 'restaurant_name',
            'prep_time': 'delivery_time'
        })
        
        return ml_df
    
    def get_weekly_report(self, restaurant_id, weeks_back=1):
        """
        Генерирует недельный отчет для ресторана
        """
        end_date = datetime.now().date()
        start_date = end_date - timedelta(weeks=weeks_back)
        
        df = self.load_combined_sales_data(start_date, end_date)
        restaurant_data = df[df['restaurant_id'] == restaurant_id]
        
        if len(restaurant_data) == 0:
            return None
        
        # Группируем по неделям
        restaurant_data['week'] = restaurant_data['date'].dt.isocalendar().week
        weekly_stats = restaurant_data.groupby('week').agg({
            'sales': ['sum', 'mean', 'std'],
            'orders': ['sum', 'mean'],
            'rating': 'mean',
            'cancellation_rate': 'mean'
        }).round(2)
        
        return weekly_stats
    
    def get_monthly_report(self, restaurant_id, months_back=1):
        """
        Генерирует месячный отчет для ресторана
        """
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=30*months_back)
        
        df = self.load_combined_sales_data(start_date, end_date)
        restaurant_data = df[df['restaurant_id'] == restaurant_id]
        
        if len(restaurant_data) == 0:
            return None
        
        # Группируем по месяцам
        restaurant_data['month'] = restaurant_data['date'].dt.to_period('M')
        monthly_stats = restaurant_data.groupby('month').agg({
            'sales': ['sum', 'mean', 'std'],
            'orders': ['sum', 'mean'],
            'rating': 'mean',
            'cancellation_rate': 'mean',
            'ads_spend': 'sum'
        }).round(2)
        
        return monthly_stats
    
    def get_quarterly_report(self, restaurant_id, quarters_back=1):
        """
        Генерирует квартальный отчет для ресторана
        """
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=90*quarters_back)
        
        df = self.load_combined_sales_data(start_date, end_date)
        restaurant_data = df[df['restaurant_id'] == restaurant_id]
        
        if len(restaurant_data) == 0:
            return None
        
        # Группируем по кварталам
        restaurant_data['quarter'] = restaurant_data['date'].dt.to_period('Q')
        quarterly_stats = restaurant_data.groupby('quarter').agg({
            'sales': ['sum', 'mean', 'std'],
            'orders': ['sum', 'mean'],
            'rating': 'mean',
            'cancellation_rate': 'mean',
            'ads_spend': 'sum'
        }).round(2)
        
        return quarterly_stats
    
    def export_for_ml_training(self, output_path='client_data_for_ml.csv'):
        """
        Экспортирует данные в формате для обучения ML
        """
        df = self.load_combined_sales_data()
        ml_df = self.prepare_for_ml(df)
        
        # Сохраняем в CSV
        ml_df.to_csv(output_path, index=False)
        print(f"✅ Данные экспортированы в {output_path}")
        print(f"📊 Записей: {len(ml_df):,}")
        print(f"🏪 Ресторанов: {ml_df['restaurant_name'].nunique()}")
        print(f"📅 Период: {ml_df['date'].min()} - {ml_df['date'].max()}")
        
        return output_path
    
    def close(self):
        self.conn.close()

# Пример использования
if __name__ == "__main__":
    # Укажите путь к вашей базе данных
    adapter = ClientDataAdapter("path_to_your_database.db")
    
    # Экспортируем данные для ML
    csv_file = adapter.export_for_ml_training()
    
    # Пример отчетов
    restaurant_id = 1
    
    print("\\n📊 НЕДЕЛЬНЫЙ ОТЧЕТ:")
    weekly = adapter.get_weekly_report(restaurant_id)
    if weekly is not None:
        print(weekly)
    
    print("\\n📊 МЕСЯЧНЫЙ ОТЧЕТ:")
    monthly = adapter.get_monthly_report(restaurant_id)
    if monthly is not None:
        print(monthly)
    
    print("\\n📊 КВАРТАЛЬНЫЙ ОТЧЕТ:")
    quarterly = adapter.get_quarterly_report(restaurant_id)
    if quarterly is not None:
        print(quarterly)
    
    adapter.close()
'''
    
    # Сохраняем адаптер
    with open('client_data_adapter.py', 'w', encoding='utf-8') as f:
        f.write(adapter_code)
    
    print("✅ Адаптер создан: client_data_adapter.py")
    
    return True

def main():
    print("🚀 АНАЛИЗ БАЗЫ ДАННЫХ КЛИЕНТА")
    print("=" * 60)
    
    # Запрашиваем путь к базе данных
    db_path = input("📁 Укажите путь к SQL файлу/базе данных: ").strip()
    
    if not os.path.exists(db_path):
        print(f"❌ Файл не найден: {db_path}")
        return
    
    # Анализируем структуру
    if analyze_database_structure(db_path):
        # Анализируем качество данных
        analyze_data_quality(db_path)
        
        # Создаем адаптер
        create_ml_data_adapter(db_path)
        
        print("\n🎉 АНАЛИЗ ЗАВЕРШЕН!")
        print("=" * 60)
        print("📋 Следующие шаги:")
        print("1. Изучите результаты анализа выше")
        print("2. Отредактируйте client_data_adapter.py под вашу структуру")
        print("3. Запустите: python3 client_data_adapter.py")
        print("4. Используйте созданный CSV для обучения ML")
        
    else:
        print("❌ Не удалось проанализировать базу данных")

if __name__ == "__main__":
    main()