#!/usr/bin/env python3
"""
🤖 УСТАНОВКА ML ЗАВИСИМОСТЕЙ ДЛЯ MUZAQUEST ANALYTICS
================================================================
Быстрая установка всех ML библиотек на Replit
"""

import subprocess
import sys
import os

def install_package(package):
    """Установка пакета с обработкой ошибок"""
    try:
        print(f"📦 Устанавливаю {package}...")
        result = subprocess.run([
            sys.executable, "-m", "pip", "install", package, "--break-system-packages"
        ], capture_output=True, text=True)
        
        if result.returncode == 0:
            print(f"✅ {package} успешно установлен!")
            return True
        else:
            print(f"⚠️ Ошибка установки {package}: {result.stderr}")
            return False
    except Exception as e:
        print(f"❌ Исключение при установке {package}: {e}")
        return False

def main():
    print("🤖 MUZAQUEST ANALYTICS - УСТАНОВКА ML ЗАВИСИМОСТЕЙ")
    print("=" * 60)
    print("🚀 Устанавливаем библиотеки машинного обучения...")
    print()
    
    # Основные ML пакеты
    ml_packages = [
        "scikit-learn>=1.3.0",
        "scipy>=1.10.0",
        "numpy>=1.24.0",
        "pandas>=1.5.0"
    ]
    
    # Опциональные пакеты
    optional_packages = [
        "matplotlib>=3.6.0",
        "seaborn>=0.12.0"
    ]
    
    print("📊 ОСНОВНЫЕ ML ПАКЕТЫ:")
    print("-" * 30)
    success_count = 0
    
    for package in ml_packages:
        if install_package(package):
            success_count += 1
        print()
    
    print("📈 ОПЦИОНАЛЬНЫЕ ПАКЕТЫ:")
    print("-" * 30)
    
    for package in optional_packages:
        install_package(package)
        print()
    
    print("=" * 60)
    print(f"✅ Установлено {success_count}/{len(ml_packages)} основных пакетов")
    
    # Проверяем установку
    print("\n🔍 ПРОВЕРКА УСТАНОВКИ:")
    print("-" * 30)
    
    try:
        import sklearn
        print("✅ scikit-learn:", sklearn.__version__)
    except ImportError:
        print("❌ scikit-learn не установлен")
    
    try:
        import scipy
        print("✅ scipy:", scipy.__version__)
    except ImportError:
        print("❌ scipy не установлен")
    
    try:
        import numpy as np
        print("✅ numpy:", np.__version__)
    except ImportError:
        print("❌ numpy не установлен")
    
    try:
        import pandas as pd
        print("✅ pandas:", pd.__version__)
    except ImportError:
        print("❌ pandas не установлен")
    
    print("\n🎯 ДОПОЛНИТЕЛЬНЫЕ КОМАНДЫ:")
    print("-" * 30)
    print("📊 Для временных рядов: pip install prophet")
    print("📈 Для визуализации: pip install plotly")
    print("🔮 Для продвинутого ML: pip install xgboost lightgbm")
    
    print("\n🚀 ГОТОВО! Теперь можно использовать ML функции:")
    print("   python main.py analyze 'Ika Kero' --start 2025-04-01 --end 2025-06-30")
    print("   python ml_full_database_analysis.py")

if __name__ == "__main__":
    main()