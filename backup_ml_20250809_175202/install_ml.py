#!/usr/bin/env python3
"""
🔧 Установщик ML библиотек для системы аналитики ресторанов
Автоматически устанавливает все необходимые зависимости для машинного обучения
"""

import subprocess
import sys
import os

def install_package(package):
    """Устанавливает пакет через pip"""
    try:
        subprocess.check_call([sys.executable, "-m", "pip", "install", package])
        print(f"✅ {package} установлен успешно")
        return True
    except subprocess.CalledProcessError:
        print(f"❌ Ошибка установки {package}")
        return False

def check_package(package):
    """Проверяет установлен ли пакет"""
    try:
        __import__(package)
        return True
    except ImportError:
        return False

def main():
    """Основная функция установки"""
    print("🚀 УСТАНОВКА ML БИБЛИОТЕК")
    print("=" * 40)
    
    # Список необходимых пакетов
    packages = [
        "scikit-learn",
        "pandas", 
        "numpy",
        "requests",
        "python-dotenv",
        "openpyxl",
        "xlrd"
    ]
    
    # Проверяем и устанавливаем пакеты
    for package in packages:
        package_name = package.split("==")[0]  # Убираем версию если есть
        
        if check_package(package_name.replace("-", "_")):
            print(f"✅ {package} уже установлен")
        else:
            print(f"📦 Устанавливаю {package}...")
            install_package(package)
    
    print("\n🎉 Установка завершена!")
    print("💡 Теперь можно запускать: python main.py")

if __name__ == "__main__":
    main()