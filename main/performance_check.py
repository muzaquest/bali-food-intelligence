#!/usr/bin/env python3
"""
Скрипт проверки производительности системы с максимальными данными
"""

import time
import os
import sys
import psutil
import pandas as pd
import numpy as np
from datetime import datetime
import logging

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def measure_time(func):
    """Декоратор для измерения времени выполнения"""
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        execution_time = end_time - start_time
        print(f"⏱️ {func.__name__}: {execution_time:.2f} секунд")
        return result, execution_time
    return wrapper

def get_memory_usage():
    """Получить использование памяти"""
    process = psutil.Process(os.getpid())
    memory_info = process.memory_info()
    return memory_info.rss / 1024 / 1024  # MB

def check_file_sizes():
    """Проверка размеров файлов модели"""
    print("\n📦 РАЗМЕРЫ ФАЙЛОВ:")
    print("=" * 50)
    
    files_to_check = [
        'client_sales_model.joblib',
        'scaler.joblib',
        'data/database.sqlite'
    ]
    
    total_size = 0
    for file_path in files_to_check:
        if os.path.exists(file_path):
            size_mb = os.path.getsize(file_path) / 1024 / 1024
            total_size += size_mb
            print(f"  📄 {file_path}: {size_mb:.2f} MB")
        else:
            print(f"  ❌ {file_path}: файл не найден")
    
    print(f"  📊 Общий размер: {total_size:.2f} MB")
    
    if total_size > 500:
        print("  ⚠️ ПРЕДУПРЕЖДЕНИЕ: Большой размер может вызвать проблемы на бесплатных платформах")
    elif total_size > 100:
        print("  💡 ИНФОРМАЦИЯ: Размер в пределах нормы, но стоит следить")
    else:
        print("  ✅ ОТЛИЧНО: Компактный размер")
    
    return total_size

@measure_time
def test_data_loading():
    """Тест загрузки данных"""
    print("\n📊 ТЕСТ ЗАГРУЗКИ ДАННЫХ")
    print("=" * 50)
    
    try:
        from main.data_integration import load_data_with_all_features
        
        memory_before = get_memory_usage()
        df = load_data_with_all_features()
        memory_after = get_memory_usage()
        
        memory_used = memory_after - memory_before
        
        print(f"  📈 Загружено записей: {len(df)}")
        print(f"  🔧 Количество полей: {len(df.columns)}")
        print(f"  💾 Память до загрузки: {memory_before:.1f} MB")
        print(f"  💾 Память после загрузки: {memory_after:.1f} MB")
        print(f"  📊 Использовано памяти: {memory_used:.1f} MB")
        
        if memory_used > 500:
            print("  ⚠️ ПРЕДУПРЕЖДЕНИЕ: Высокое потребление памяти")
        else:
            print("  ✅ ПАМЯТЬ: В пределах нормы")
        
        return df
        
    except Exception as e:
        print(f"  ❌ ОШИБКА: {e}")
        return None

@measure_time 
def test_feature_engineering(df):
    """Тест создания features"""
    print("\n🌟 ТЕСТ FEATURE ENGINEERING")
    print("=" * 50)
    
    try:
        from main.data_integration import prepare_features_with_all_enhancements
        
        memory_before = get_memory_usage()
        enhanced_df = prepare_features_with_all_enhancements(df.head(1000))  # Тестируем на 1000 записей
        memory_after = get_memory_usage()
        
        memory_used = memory_after - memory_before
        features_added = len(enhanced_df.columns) - len(df.columns)
        
        print(f"  🔧 Исходных полей: {len(df.columns)}")
        print(f"  🌟 Итоговых полей: {len(enhanced_df.columns)}")
        print(f"  ➕ Добавлено features: {features_added}")
        print(f"  💾 Использовано памяти: {memory_used:.1f} MB")
        
        return enhanced_df
        
    except Exception as e:
        print(f"  ❌ ОШИБКА: {e}")
        return df

@measure_time
def test_model_training():
    """Тест обучения модели"""
    print("\n🤖 ТЕСТ ОБУЧЕНИЯ МОДЕЛИ")
    print("=" * 50)
    
    try:
        # Создаем небольшой тестовый датасет
        from main.data_integration import load_data_with_all_features, prepare_features_with_all_enhancements
        
        df = load_data_with_all_features()
        if df.empty:
            print("  ❌ Нет данных для обучения")
            return None
        
        # Берем только часть данных для быстрого теста
        test_df = df.head(1000)
        enhanced_df = prepare_features_with_all_enhancements(test_df)
        
        # Подготовка данных для обучения
        target_column = 'total_sales'
        exclude_columns = ['date', 'restaurant_name', 'platform', 'holiday_name', 
                          'day_name', 'month_name', 'weather_condition', 'temp_category',
                          'rain_category', 'weather_combination', 'special_period_combination',
                          'day_category', target_column]
        
        feature_columns = [col for col in enhanced_df.columns if col not in exclude_columns]
        
        X = enhanced_df[feature_columns].fillna(0)
        y = enhanced_df[target_column]
        
        print(f"  🔧 Количество features: {len(feature_columns)}")
        print(f"  📊 Размер обучающей выборки: {len(X)}")
        
        # Быстрое обучение Random Forest
        from sklearn.ensemble import RandomForestRegressor
        from sklearn.model_selection import train_test_split
        
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
        
        memory_before = get_memory_usage()
        
        # Обучаем модель с ограниченными параметрами для скорости
        model = RandomForestRegressor(
            n_estimators=10,  # Мало деревьев для быстроты
            max_depth=10,     # Ограниченная глубина
            random_state=42,
            n_jobs=1
        )
        
        model.fit(X_train, y_train)
        
        memory_after = get_memory_usage()
        memory_used = memory_after - memory_before
        
        # Проверяем качество
        score = model.score(X_test, y_test)
        
        print(f"  📈 R² Score: {score:.3f}")
        print(f"  💾 Память для модели: {memory_used:.1f} MB")
        
        # Проверяем важность признаков
        feature_importance = pd.DataFrame({
            'feature': feature_columns,
            'importance': model.feature_importances_
        }).sort_values('importance', ascending=False)
        
        print(f"  🔝 Топ-5 важных признаков:")
        for i, row in feature_importance.head(5).iterrows():
            print(f"    {row['feature']}: {row['importance']:.4f}")
        
        return model, feature_importance
        
    except Exception as e:
        print(f"  ❌ ОШИБКА: {e}")
        print(f"  📝 Нет данных для обучения")
        return None, None

@measure_time
def test_report_generation():
    """Тест генерации отчёта"""
    print("\n📋 ТЕСТ ГЕНЕРАЦИИ ОТЧЁТА")
    print("=" * 50)
    
    try:
        from business_intelligence_system import MarketIntelligenceEngine
        
        memory_before = get_memory_usage()
        
        # Создаем engine и генерируем быстрый анализ
        engine = MarketIntelligenceEngine()
        
        # Тестовый анализ для небольшого периода
        result = engine.generate_deep_analysis(
            restaurant_name="Ika Canggu",
            start_date="2025-04-01", 
            end_date="2025-04-10"  # Только 10 дней для скорости
        )
        
        memory_after = get_memory_usage()
        memory_used = memory_after - memory_before
        
        print(f"  💾 Память для анализа: {memory_used:.1f} MB")
        
        if 'error' in result:
            print(f"  ❌ ОШИБКА: {result['error']}")
            return False
        else:
            print(f"  ✅ АНАЛИЗ: Успешно сгенерирован")
            return True
            
    except Exception as e:
        print(f"  ❌ ОШИБКА: {e}")
        return False

def analyze_feature_usage(model, feature_importance_df):
    """Анализ использования features"""
    print("\n🔍 АНАЛИЗ ИСПОЛЬЗОВАНИЯ FEATURES")
    print("=" * 50)
    
    if model is None or feature_importance_df is None:
        print("  ❌ Нет данных о модели для анализа")
        return
    
    total_features = len(feature_importance_df)
    important_features = len(feature_importance_df[feature_importance_df['importance'] > 0.001])  # > 0.1%
    zero_importance = len(feature_importance_df[feature_importance_df['importance'] == 0])
    
    print(f"  📊 Всего features: {total_features}")
    print(f"  ✅ Важных features (>0.1%): {important_features}")
    print(f"  ❌ Бесполезных features: {zero_importance}")
    print(f"  📈 Использование: {(important_features/total_features)*100:.1f}%")
    
    if zero_importance > total_features * 0.3:
        print(f"  ⚠️ РЕКОМЕНДАЦИЯ: {zero_importance} features не используются - можно оптимизировать")
    else:
        print(f"  ✅ ОТЛИЧНО: Большинство features полезны")
    
    # Анализ новых типов features
    enhanced_feature_types = {
        'weather': [f for f in feature_importance_df['feature'] if any(w in f.lower() for w in ['temp', 'weather', 'rain', 'wind', 'humid'])],
        'calendar': [f for f in feature_importance_df['feature'] if any(c in f.lower() for c in ['holiday', 'weekend', 'month', 'day_of_week'])],
        'customer': [f for f in feature_importance_df['feature'] if any(c in f.lower() for c in ['customer', 'retention', 'new_customer'])],
        'operational': [f for f in feature_importance_df['feature'] if any(o in f.lower() for o in ['efficiency', 'acceptance', 'completion'])],
        'quality': [f for f in feature_importance_df['feature'] if any(q in f.lower() for q in ['rating', 'star', 'quality'])],
        'lag': [f for f in feature_importance_df['feature'] if 'lag' in f.lower()],
        'rolling': [f for f in feature_importance_df['feature'] if 'rolling' in f.lower()]
    }
    
    print(f"\n  🔍 АНАЛИЗ ПО ТИПАМ FEATURES:")
    for feature_type, features in enhanced_feature_types.items():
        if features:
            avg_importance = feature_importance_df[feature_importance_df['feature'].isin(features)]['importance'].mean()
            print(f"    {feature_type}: {len(features)} features, средняя важность: {avg_importance:.4f}")

def performance_recommendations():
    """Рекомендации по производительности"""
    print("\n💡 РЕКОМЕНДАЦИИ ПО ОПТИМИЗАЦИИ")
    print("=" * 50)
    
    recommendations = [
        "🔧 Для Replit: Используйте модель с max_depth=15, n_estimators=50",
        "📦 Сжатие: Сохраняйте модель с compress=3 в joblib",
        "💾 Память: Используйте batch processing для больших датасетов",
        "⚡ Скорость: Кэшируйте повторяющиеся вычисления",
        "🗂️ Features: Удалите features с importance < 0.001",
        "📊 Данные: Используйте sample для развития/тестирования"
    ]
    
    for rec in recommendations:
        print(f"  {rec}")

def main():
    """Главная функция тестирования производительности"""
    print("🚀 ТЕСТИРОВАНИЕ ПРОИЗВОДИТЕЛЬНОСТИ СИСТЕМЫ")
    print("=" * 60)
    print(f"⏰ Время начала: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)
    
    # Проверка размеров файлов
    total_size = check_file_sizes()
    
    # Тест загрузки данных
    df, load_time = test_data_loading()
    
    if df is not None:
        # Тест feature engineering  
        enhanced_df, fe_time = test_feature_engineering(df)
        
        # Тест обучения модели
        result = test_model_training()
        if result[0] is not None:
            (model, feature_importance), train_time = result
        else:
            model, feature_importance, train_time = None, None, result[1]
        
        # Тест генерации отчёта
        report_success, report_time = test_report_generation()
        
        # Анализ использования features
        analyze_feature_usage(model, feature_importance)
        
        # SHAP анализ если есть данные
        if enhanced_df is not None and len(enhanced_df) > 0:
            try:
                print("\n🔍 ЗАПУСК SHAP АНАЛИЗА")
                print("=" * 50)
                from shap_analysis import run_comprehensive_feature_analysis
                shap_report = run_comprehensive_feature_analysis(enhanced_df.head(500))
                if shap_report:
                    print("  ✅ SHAP анализ завершён успешно")
            except Exception as e:
                print(f"  ⚠️ SHAP анализ недоступен: {e}")
        
        # Итоговая сводка
        print("\n📊 ИТОГОВАЯ СВОДКА ПРОИЗВОДИТЕЛЬНОСТИ")
        print("=" * 60)
        print(f"  ⏱️ Загрузка данных: {load_time:.2f} сек")
        print(f"  ⏱️ Feature engineering: {fe_time:.2f} сек") 
        print(f"  ⏱️ Обучение модели: {train_time:.2f} сек")
        print(f"  ⏱️ Генерация отчёта: {report_time:.2f} сек")
        print(f"  📦 Размер файлов: {total_size:.1f} MB")
        print(f"  💾 Пиковое потребление памяти: {get_memory_usage():.1f} MB")
        
        total_time = load_time + fe_time + train_time + report_time
        
        print(f"\n⏱️ ОБЩЕЕ ВРЕМЯ: {total_time:.2f} секунд")
        
        if total_time > 300:  # 5 минут
            print("  ⚠️ МЕДЛЕННО: Может быть проблема на Replit")
        elif total_time > 120:  # 2 минуты
            print("  💡 ПРИЕМЛЕМО: Но стоит оптимизировать")
        else:
            print("  ✅ БЫСТРО: Подходит для продакшена")
    
    # Рекомендации
    performance_recommendations()
    
    print("\n🎯 ЗАВЕРШЕНО!")

if __name__ == "__main__":
    main()