#!/usr/bin/env python3
"""
🔧 Скрипт для автоматической настройки базы данных
Автоматически скачивает и проверяет базу данных для системы аналитики
"""

import os
import sys
import sqlite3
import urllib.request

def download_database():
    """Скачивает базу данных если её нет"""
    db_path = "../database.sqlite"
    
    if os.path.exists(db_path):
        print("✅ База данных уже существует")
        return True
    
    print("📥 Скачиваю базу данных...")
    try:
        # URL базы данных (замените на реальный)
        db_url = "https://github.com/muzaquest/bali-food-intelligence/raw/main/database.sqlite"
        urllib.request.urlretrieve(db_url, db_path)
        print("✅ База данных успешно скачана")
        return True
    except Exception as e:
        print(f"❌ Ошибка скачивания: {e}")
        return False

def check_database():
    """Проверяет целостность базы данных"""
    db_path = "../database.sqlite"
    
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Проверяем основные таблицы
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = cursor.fetchall()
        
        print("📊 Найденные таблицы:")
        for table in tables:
            cursor.execute(f"SELECT COUNT(*) FROM {table[0]}")
            count = cursor.fetchone()[0]
            print(f"  - {table[0]}: {count} записей")
        
        conn.close()
        return True
        
    except Exception as e:
        print(f"❌ Ошибка проверки БД: {e}")
        return False

def setup_environment():
    """Настраивает окружение"""
    env_path = "../.env"
    
    if os.path.exists(env_path):
        print("✅ Файл .env уже существует")
        return True
    
    print("⚙️ Создаю файл .env...")
    env_content = """# API Ключи для системы аналитики
OPENAI_API_KEY=your_openai_key_here
CALENDARIFIC_API_KEY=your_calendarific_key_here
GOOGLE_MAPS_API_KEY=your_google_maps_key_here

# Настройки базы данных
DATABASE_PATH=database.sqlite

# Настройки отчетов
REPORT_LANGUAGE=ru
ENABLE_ML_ANALYSIS=true
"""
    
    try:
        with open(env_path, 'w', encoding='utf-8') as f:
            f.write(env_content)
        print("✅ Файл .env создан")
        return True
    except Exception as e:
        print(f"❌ Ошибка создания .env: {e}")
        return False

def main():
    """Основная функция настройки"""
    print("🚀 Настройка системы аналитики ресторанов")
    print("=" * 50)
    
    # Проверяем/скачиваем базу данных
    if not download_database():
        print("❌ Не удалось получить базу данных")
        return False
    
    # Проверяем целостность
    if not check_database():
        print("❌ База данных повреждена")
        return False
    
    # Настраиваем окружение
    if not setup_environment():
        print("❌ Не удалось настроить окружение")
        return False
    
    print("\n🎉 Настройка завершена успешно!")
    print("📋 Следующие шаги:")
    print("1. Отредактируйте .env файл с вашими API ключами")
    print("2. Запустите: python main.py")
    
    return True

if __name__ == "__main__":
    main()