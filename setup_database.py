#!/usr/bin/env python3
"""
🔧 Скрипт для автоматической настройки базы данных
Автоматически скачивает и проверяет базу данных для системы аналитики
"""

import os
import sys
import sqlite3
import urllib.request
from urllib.error import URLError

DATABASE_URL = "https://github.com/muzaquest/bali-food-intelligence/raw/main/database.sqlite"
DATABASE_FILE = "database.sqlite"
EXPECTED_SIZE_MB = 4.0

def download_database():
    """Скачивает базу данных с GitHub"""
    print("📥 Скачиваю базу данных...")
    try:
        urllib.request.urlretrieve(DATABASE_URL, DATABASE_FILE)
        print("✅ База данных успешно скачана!")
        return True
    except URLError as e:
        print(f"❌ Ошибка при скачивании: {e}")
        return False
    except Exception as e:
        print(f"❌ Неожиданная ошибка: {e}")
        return False

def verify_database():
    """Проверяет целостность базы данных"""
    if not os.path.exists(DATABASE_FILE):
        return False
    
    # Проверяем размер файла
    size_mb = os.path.getsize(DATABASE_FILE) / (1024 * 1024)
    if size_mb < EXPECTED_SIZE_MB * 0.8:  # Допускаем 20% отклонение
        print(f"❌ Размер базы данных слишком мал: {size_mb:.1f}MB (ожидается ~{EXPECTED_SIZE_MB}MB)")
        return False
    
    # Проверяем структуру базы данных
    try:
        conn = sqlite3.connect(DATABASE_FILE)
        cursor = conn.cursor()
        
        # Проверяем наличие основных таблиц
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [row[0] for row in cursor.fetchall()]
        
        required_tables = ['restaurants', 'grab_stats', 'gojek_stats']
        missing_tables = [table for table in required_tables if table not in tables]
        
        if missing_tables:
            print(f"❌ Отсутствуют таблицы: {missing_tables}")
            conn.close()
            return False
        
        # Проверяем количество записей
        cursor.execute("SELECT COUNT(*) FROM restaurants")
        restaurant_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM grab_stats")
        grab_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM gojek_stats")
        gojek_count = cursor.fetchone()[0]
        
        conn.close()
        
        print(f"✅ База данных проверена:")
        print(f"   📊 Ресторанов: {restaurant_count}")
        print(f"   📈 Записей GRAB: {grab_count}")
        print(f"   📱 Записей GOJEK: {gojek_count}")
        print(f"   💾 Размер: {size_mb:.1f}MB")
        
        return True
        
    except Exception as e:
        print(f"❌ Ошибка при проверке базы данных: {e}")
        return False

def main():
    """Основная функция"""
    print("🔧 НАСТРОЙКА БАЗЫ ДАННЫХ MUZAQUEST ANALYTICS")
    print("=" * 50)
    
    # Проверяем текущее состояние
    if os.path.exists(DATABASE_FILE):
        print("📁 База данных найдена, проверяю целостность...")
        if verify_database():
            print("\n🎉 База данных готова к использованию!")
            return
        else:
            print("\n⚠️ База данных повреждена, переустанавливаю...")
            os.remove(DATABASE_FILE)
    else:
        print("📁 База данных не найдена")
    
    # Скачиваем базу данных
    if download_database():
        if verify_database():
            print("\n🎉 База данных успешно установлена и готова к использованию!")
        else:
            print("\n❌ Установленная база данных повреждена")
            sys.exit(1)
    else:
        print("\n❌ Не удалось скачать базу данных")
        print("📥 Попробуйте скачать вручную:")
        print(f"   wget {DATABASE_URL}")
        sys.exit(1)

if __name__ == "__main__":
    main()