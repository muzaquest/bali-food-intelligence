#!/usr/bin/env python3
"""
🧪 ТЕСТ ML СИСТЕМЫ ДЛЯ REPLIT
═══════════════════════════════════════════════════════════════════════════════
Проверяет все ML компоненты после обновления
"""

import sys
import os
import traceback

def test_imports():
    """Тестирует импорт всех ML модулей"""
    print("🔍 Тестируем импорт ML модулей...")
    
    results = {}
    
    # Основные библиотеки
    try:
        import pandas as pd
        results['pandas'] = f"✅ {pd.__version__}"
    except Exception as e:
        results['pandas'] = f"❌ {e}"
    
    try:
        import numpy as np
        results['numpy'] = f"✅ {np.__version__}"
    except Exception as e:
        results['numpy'] = f"❌ {e}"
    
    try:
        import sklearn
        results['scikit-learn'] = f"✅ {sklearn.__version__}"
    except Exception as e:
        results['scikit-learn'] = f"❌ {e}"
    
    try:
        import prophet
        results['prophet'] = "✅ OK"
    except Exception as e:
        results['prophet'] = f"⚠️ {e}"
    
    try:
        import shap
        results['shap'] = "✅ OK"
    except Exception as e:
        results['shap'] = f"⚠️ {e}"
    
    # Наши ML модули
    try:
        from ml_models import RestaurantMLAnalyzer
        results['ml_models'] = "✅ RestaurantMLAnalyzer"
    except Exception as e:
        results['ml_models'] = f"❌ {e}"
    
    try:
        from proper_ml_detective_analysis import ProperMLDetectiveAnalysis
        results['detective_analysis'] = "✅ ProperMLDetectiveAnalysis"
    except Exception as e:
        results['detective_analysis'] = f"❌ {e}"
    
    try:
        from ai_query_processor import AIQueryProcessor
        results['ai_processor'] = "✅ AIQueryProcessor"
    except Exception as e:
        results['ai_processor'] = f"❌ {e}"
    
    return results

def test_data_files():
    """Проверяет наличие ML данных"""
    print("📊 Проверяем ML данные...")
    
    files_to_check = [
        'database.sqlite',
        'data/mega_weather_analysis.json',
        'data/real_holiday_impact_analysis.json',
        'data/comprehensive_holiday_analysis.json',
        'data/bali_restaurant_locations.json'
    ]
    
    results = {}
    for file_path in files_to_check:
        if os.path.exists(file_path):
            size = os.path.getsize(file_path)
            results[file_path] = f"✅ {size:,} байт"
        else:
            results[file_path] = "❌ Не найден"
    
    return results

def test_ai_functionality():
    """Тестирует AI функциональность"""
    print("🤖 Тестируем AI функциональность...")
    
    try:
        from ai_query_processor import AIQueryProcessor
        ai = AIQueryProcessor()
        
        # Простой тест
        result = ai._get_ml_model_info()
        if result and 'algorithm' in result:
            return "✅ AI помощник работает"
        else:
            return "⚠️ AI помощник частично работает"
            
    except Exception as e:
        return f"❌ Ошибка AI: {e}"

def main():
    """Основная функция тестирования"""
    print("🧪 ТЕСТ ML СИСТЕМЫ MUZAQUEST")
    print("=" * 50)
    
    # Тест импортов
    import_results = test_imports()
    print("\n📦 ИМПОРТ МОДУЛЕЙ:")
    for module, status in import_results.items():
        print(f"  {module}: {status}")
    
    # Тест данных
    data_results = test_data_files()
    print("\n📊 ФАЙЛЫ ДАННЫХ:")
    for file_path, status in data_results.items():
        print(f"  {file_path}: {status}")
    
    # Тест AI
    ai_result = test_ai_functionality()
    print(f"\n🤖 AI ФУНКЦИОНАЛЬНОСТЬ:")
    print(f"  {ai_result}")
    
    # Общий результат
    print("\n" + "=" * 50)
    
    critical_modules = ['pandas', 'numpy', 'scikit-learn', 'ml_models', 'detective_analysis', 'ai_processor']
    critical_ok = all('✅' in import_results.get(mod, '') for mod in critical_modules)
    
    data_ok = all('✅' in status for status in data_results.values())
    ai_ok = '✅' in ai_result
    
    if critical_ok and data_ok and ai_ok:
        print("🎉 ВСЕ ТЕСТЫ ПРОЙДЕНЫ! Система готова к работе!")
        return 0
    else:
        print("⚠️ ОБНАРУЖЕНЫ ПРОБЛЕМЫ. Проверьте установку зависимостей.")
        if not critical_ok:
            print("   - Проблемы с ML модулями")
        if not data_ok:
            print("   - Проблемы с файлами данных")
        if not ai_ok:
            print("   - Проблемы с AI функциональностью")
        return 1

if __name__ == "__main__":
    sys.exit(main())