#!/usr/bin/env python3
"""
🔬 ПОЛНЫЙ ML АНАЛИЗ БАЗЫ ДАННЫХ (УСТАРЕВШАЯ ВЕРСИЯ)
Старая версия системы машинного обучения для анализа ресторанов
"""

import sqlite3
import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error, r2_score
import json
from datetime import datetime

def load_database():
    """Загрузка данных из базы"""
    try:
        conn = sqlite3.connect('database.sqlite')
        df = pd.read_sql_query("SELECT * FROM restaurants", conn)
        conn.close()
        print(f"✅ Загружено {len(df)} записей из базы данных")
        return df
    except Exception as e:
        print(f"❌ Ошибка загрузки данных: {e}")
        return None

def prepare_features(df):
    """Подготовка признаков для ML"""
    try:
        # Базовые признаки
        features = []
        feature_names = []
        
        # Числовые признаки
        numeric_cols = ['rating', 'orders', 'avg_order_value']
        for col in numeric_cols:
            if col in df.columns:
                features.append(df[col].fillna(0))
                feature_names.append(col)
        
        # Категориальные признаки
        if 'platform' in df.columns:
            platform_dummies = pd.get_dummies(df['platform'], prefix='platform')
            for col in platform_dummies.columns:
                features.append(platform_dummies[col])
                feature_names.append(col)
        
        if 'city' in df.columns:
            city_dummies = pd.get_dummies(df['city'], prefix='city')
            for col in city_dummies.columns:
                features.append(city_dummies[col])
                feature_names.append(col)
        
        # Объединяем признаки
        X = np.column_stack(features) if features else np.array([]).reshape(len(df), 0)
        
        print(f"✅ Подготовлено {X.shape[1]} признаков")
        return X, feature_names
        
    except Exception as e:
        print(f"❌ Ошибка подготовки признаков: {e}")
        return None, None

def train_ml_model(X, y, feature_names):
    """Обучение ML модели"""
    try:
        # Разделение данных
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42
        )
        
        # Обучение модели
        model = RandomForestRegressor(n_estimators=100, random_state=42)
        model.fit(X_train, y_train)
        
        # Предсказания
        y_pred_train = model.predict(X_train)
        y_pred_test = model.predict(X_test)
        
        # Метрики
        train_mae = mean_absolute_error(y_train, y_pred_train)
        test_mae = mean_absolute_error(y_test, y_pred_test)
        train_r2 = r2_score(y_train, y_pred_train)
        test_r2 = r2_score(y_test, y_pred_test)
        
        # Важность признаков
        feature_importance = list(zip(feature_names, model.feature_importances_))
        feature_importance.sort(key=lambda x: x[1], reverse=True)
        
        results = {
            "train_mae": train_mae,
            "test_mae": test_mae,
            "train_r2": train_r2,
            "test_r2": test_r2,
            "feature_importance": feature_importance[:10]  # Топ 10
        }
        
        print(f"✅ Модель обучена. Test R²: {test_r2:.3f}")
        return model, results
        
    except Exception as e:
        print(f"❌ Ошибка обучения модели: {e}")
        return None, None

def generate_insights(results, df):
    """Генерация бизнес-инсайтов"""
    insights = []
    
    # Анализ важности признаков
    if results and 'feature_importance' in results:
        top_features = results['feature_importance'][:3]
        insights.append("🔍 КЛЮЧЕВЫЕ ФАКТОРЫ УСПЕХА:")
        for feature, importance in top_features:
            insights.append(f"  - {feature}: {importance:.3f}")
    
    # Анализ по городам
    if 'city' in df.columns and 'sales' in df.columns:
        city_stats = df.groupby('city')['sales'].agg(['mean', 'count']).round(2)
        insights.append("\n🏙️ АНАЛИЗ ПО ГОРОДАМ:")
        for city, stats in city_stats.head(5).iterrows():
            insights.append(f"  - {city}: ${stats['mean']:,.2f} (n={stats['count']})")
    
    # Анализ по платформам
    if 'platform' in df.columns and 'sales' in df.columns:
        platform_stats = df.groupby('platform')['sales'].agg(['mean', 'count']).round(2)
        insights.append("\n📱 АНАЛИЗ ПО ПЛАТФОРМАМ:")
        for platform, stats in platform_stats.iterrows():
            insights.append(f"  - {platform}: ${stats['mean']:,.2f} (n={stats['count']})")
    
    return "\n".join(insights)

def main():
    """Основная функция анализа"""
    print("🔬 ПОЛНЫЙ ML АНАЛИЗ БАЗЫ ДАННЫХ")
    print("=" * 50)
    
    # Загрузка данных
    df = load_database()
    if df is None:
        return
    
    # Проверка наличия целевой переменной
    if 'sales' not in df.columns:
        print("❌ Столбец 'sales' не найден в данных")
        return
    
    # Подготовка данных
    X, feature_names = prepare_features(df)
    if X is None:
        return
    
    y = df['sales'].fillna(0)
    
    # Обучение модели
    model, results = train_ml_model(X, y, feature_names)
    if model is None:
        return
    
    # Генерация отчета
    print("\n📊 РЕЗУЛЬТАТЫ АНАЛИЗА:")
    print(f"Train MAE: {results['train_mae']:,.2f}")
    print(f"Test MAE: {results['test_mae']:,.2f}")
    print(f"Train R²: {results['train_r2']:.3f}")
    print(f"Test R²: {results['test_r2']:.3f}")
    
    # Бизнес-инсайты
    insights = generate_insights(results, df)
    print(f"\n{insights}")
    
    # Сохранение результатов
    try:
        with open('legacy_ml_results.json', 'w', encoding='utf-8') as f:
            json.dump(results, f, ensure_ascii=False, indent=2, default=str)
        print(f"\n💾 Результаты сохранены в legacy_ml_results.json")
    except Exception as e:
        print(f"❌ Ошибка сохранения: {e}")
    
    print(f"\n🎉 Анализ завершен!")

if __name__ == "__main__":
    main()