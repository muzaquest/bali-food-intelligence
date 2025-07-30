#!/usr/bin/env python3
"""
Тестирование доступности ML анализа
"""

print("🔍 ТЕСТИРОВАНИЕ ML АНАЛИЗА")
print("=" * 40)

# 1. Проверяем импорт ML класса
try:
    from proper_ml_detective_analysis import ProperMLDetectiveAnalysis
    print("✅ ProperMLDetectiveAnalysis импортирован")
    ML_DETECTIVE_AVAILABLE = True
except ImportError as e:
    print(f"❌ ProperMLDetectiveAnalysis недоступен: {e}")
    ML_DETECTIVE_AVAILABLE = False

# 2. Проверяем зависимости
dependencies = ['pandas', 'numpy', 'sklearn', 'shap']
for dep in dependencies:
    try:
        __import__(dep)
        print(f"✅ {dep}: OK")
    except ImportError:
        print(f"❌ {dep}: НЕ УСТАНОВЛЕН")

# 3. Тестируем создание объекта
if ML_DETECTIVE_AVAILABLE:
    try:
        analyzer = ProperMLDetectiveAnalysis()
        print("✅ Объект ML анализа создан")
        
        # Проверяем метод
        if hasattr(analyzer, 'analyze_restaurant_performance'):
            print("✅ Метод analyze_restaurant_performance найден")
        else:
            print("❌ Метод analyze_restaurant_performance НЕ НАЙДЕН")
            
    except Exception as e:
        print(f"❌ Ошибка создания объекта: {e}")

print("\n🎯 ИТОГ:")
if ML_DETECTIVE_AVAILABLE:
    print("✅ ML анализ ДОСТУПЕН и будет работать в отчетах")
else:
    print("❌ ML анализ НЕДОСТУПЕН - будет использоваться простой анализ трендов")