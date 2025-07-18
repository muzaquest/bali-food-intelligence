#!/usr/bin/env python3
"""
Обучение ML модели на данных клиента
"""

import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error, r2_score
import sqlite3
import joblib
import os
from datetime import datetime, timedelta

def load_client_data(db_path='client_data.db'):
    """Загружает данные клиента из SQLite"""
    print("📊 Загрузка данных клиента...")
    
    conn = sqlite3.connect(db_path)
    
    # Загружаем данные продаж
    query = """
    SELECT 
        g.restaurant_id,
        g.date,
        g.sales,
        g.orders,
        g.avg_order_value,
        g.ads_enabled,
        g.rating,
        g.delivery_time,
        r.name as restaurant_name,
        r.region
    FROM grab_stats g
    LEFT JOIN restaurants r ON g.restaurant_id = r.id
    ORDER BY g.restaurant_id, g.date
    """
    
    df = pd.read_sql_query(query, conn)
    conn.close()
    
    print(f"✅ Загружено {len(df)} записей")
    print(f"📅 Период: {df['date'].min()} - {df['date'].max()}")
    print(f"🏪 Ресторанов: {df['restaurant_id'].nunique()}")
    
    return df

def create_features(df):
    """Создает признаки для ML модели"""
    print("🔧 Создание признаков...")
    
    # Конвертируем дату
    df['date'] = pd.to_datetime(df['date'])
    
    # Временные признаки
    df['year'] = df['date'].dt.year
    df['month'] = df['date'].dt.month
    df['day'] = df['date'].dt.day
    df['day_of_week'] = df['date'].dt.dayofweek
    df['week_of_year'] = df['date'].dt.isocalendar().week
    
    # Сезонные признаки
    df['is_weekend'] = df['day_of_week'].isin([5, 6]).astype(int)
    df['is_high_season'] = df['month'].isin([6, 7, 8]).astype(int)  # Высокий сезон
    df['is_low_season'] = df['month'].isin([3, 4, 5, 9, 10]).astype(int)  # Низкий сезон
    
    # Лаговые признаки (продажи предыдущих дней)
    df = df.sort_values(['restaurant_id', 'date'])
    for lag in [1, 2, 3, 7]:
        df[f'sales_lag_{lag}'] = df.groupby('restaurant_id')['sales'].shift(lag)
    
    # Скользящие средние
    for window in [3, 7, 14]:
        df[f'sales_ma_{window}'] = df.groupby('restaurant_id')['sales'].rolling(window=window, min_periods=1).mean().reset_index(0, drop=True)
    
    # Относительные изменения
    df['sales_change_1d'] = df.groupby('restaurant_id')['sales'].pct_change()
    df['sales_change_7d'] = df.groupby('restaurant_id')['sales'].pct_change(periods=7)
    
    # Кодирование категориальных признаков
    df['region_encoded'] = pd.Categorical(df['region']).codes
    df['restaurant_encoded'] = pd.Categorical(df['restaurant_name']).codes
    
    # Бизнес-признаки
    df['ads_enabled'] = df['ads_enabled'].astype(int)
    df['rating_normalized'] = (df['rating'] - df['rating'].mean()) / df['rating'].std()
    df['delivery_time_normalized'] = (df['delivery_time'] - df['delivery_time'].mean()) / df['delivery_time'].std()
    
    print(f"✅ Создано {len(df.columns)} признаков")
    
    return df

def prepare_ml_data(df):
    """Подготавливает данные для ML"""
    print("🎯 Подготовка данных для ML...")
    
    # Выбираем признаки для модели (без лаговых для начала)
    feature_columns = [
        'year', 'month', 'day', 'day_of_week', 'week_of_year',
        'is_weekend', 'is_high_season', 'is_low_season',
        'region_encoded', 'restaurant_encoded',
        'orders', 'avg_order_value', 'ads_enabled',
        'rating_normalized', 'delivery_time_normalized'
    ]
    
    # Добавляем лаговые признаки только если они есть и не все NaN
    lag_features = ['sales_lag_1', 'sales_lag_2', 'sales_lag_3', 'sales_lag_7']
    ma_features = ['sales_ma_3', 'sales_ma_7', 'sales_ma_14']
    change_features = ['sales_change_1d', 'sales_change_7d']
    
    for feature_group in [lag_features, ma_features, change_features]:
        for feature in feature_group:
            if feature in df.columns and not df[feature].isna().all():
                feature_columns.append(feature)
    
    # Проверяем наличие всех признаков
    available_features = [col for col in feature_columns if col in df.columns]
    print(f"📊 Используемые признаки: {len(available_features)} из {len(feature_columns)}")
    
    # Берем только те строки, где есть основные данные
    df_clean = df.dropna(subset=['sales', 'orders', 'region_encoded', 'restaurant_encoded'])
    
    X = df_clean[available_features]
    y = df_clean['sales']
    
    # Заполняем NaN нулями для лаговых признаков
    X = X.fillna(0)
    
    # Убираем бесконечные значения
    X = X.replace([np.inf, -np.inf], 0)
    
    print(f"✅ Подготовлено {len(X)} образцов для обучения")
    print(f"📊 Признаки: {available_features}")
    
    return X, y, available_features

def train_model(X, y, feature_names):
    """Обучает ML модель"""
    print("🤖 Обучение ML модели...")
    
    # Разделяем на обучение и тест
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, shuffle=True
    )
    
    print(f"📊 Обучающая выборка: {len(X_train)} образцов")
    print(f"📊 Тестовая выборка: {len(X_test)} образцов")
    
    # Создаем и обучаем модель
    model = RandomForestRegressor(
        n_estimators=100,
        max_depth=15,
        min_samples_split=5,
        min_samples_leaf=2,
        random_state=42,
        n_jobs=-1
    )
    
    print("🔄 Обучение модели...")
    model.fit(X_train, y_train)
    
    # Оценка модели
    train_pred = model.predict(X_train)
    test_pred = model.predict(X_test)
    
    train_r2 = r2_score(y_train, train_pred)
    test_r2 = r2_score(y_test, test_pred)
    train_rmse = np.sqrt(mean_squared_error(y_train, train_pred))
    test_rmse = np.sqrt(mean_squared_error(y_test, test_pred))
    
    print(f"\n📈 РЕЗУЛЬТАТЫ ОБУЧЕНИЯ:")
    print(f"🎯 R² на обучении: {train_r2:.4f}")
    print(f"🎯 R² на тесте: {test_r2:.4f}")
    print(f"📊 RMSE на обучении: {train_rmse:,.0f}")
    print(f"📊 RMSE на тесте: {test_rmse:,.0f}")
    
    # Важность признаков
    feature_importance = pd.DataFrame({
        'feature': feature_names,
        'importance': model.feature_importances_
    }).sort_values('importance', ascending=False)
    
    print(f"\n🔝 ТОП-10 ВАЖНЫХ ПРИЗНАКОВ:")
    for i, row in feature_importance.head(10).iterrows():
        print(f"  {row['feature']}: {row['importance']:.4f}")
    
    return model, feature_importance, {
        'train_r2': train_r2,
        'test_r2': test_r2,
        'train_rmse': train_rmse,
        'test_rmse': test_rmse
    }

def save_model(model, feature_names, feature_importance, metrics):
    """Сохраняет модель и метаданные"""
    print("💾 Сохранение модели...")
    
    # Создаем папку для моделей
    os.makedirs('models', exist_ok=True)
    
    # Сохраняем модель
    model_path = 'models/client_sales_model.joblib'
    joblib.dump(model, model_path)
    
    # Сохраняем метаданные
    metadata = {
        'model_type': 'RandomForestRegressor',
        'feature_names': feature_names,
        'feature_importance': feature_importance.to_dict('records'),
        'metrics': metrics,
        'trained_at': datetime.now().isoformat(),
        'model_path': model_path
    }
    
    metadata_path = 'models/client_model_metadata.json'
    import json
    with open(metadata_path, 'w') as f:
        json.dump(metadata, f, indent=2, default=str)
    
    print(f"✅ Модель сохранена: {model_path}")
    print(f"✅ Метаданные сохранены: {metadata_path}")
    
    return model_path, metadata_path

def main():
    print("🚀 ОБУЧЕНИЕ ML МОДЕЛИ НА ДАННЫХ КЛИЕНТА")
    print("=" * 60)
    
    try:
        # Загружаем данные
        df = load_client_data()
        
        # Создаем признаки
        df_with_features = create_features(df)
        
        # Подготавливаем для ML
        X, y, feature_names = prepare_ml_data(df_with_features)
        
        # Обучаем модель
        model, feature_importance, metrics = train_model(X, y, feature_names)
        
        # Сохраняем модель
        model_path, metadata_path = save_model(model, feature_names, feature_importance, metrics)
        
        print("\n🎉 ОБУЧЕНИЕ ЗАВЕРШЕНО УСПЕШНО!")
        print("=" * 60)
        print("📋 Следующие шаги:")
        print("1. Протестируйте модель: python3 test_client_model.py")
        print("2. Создайте отчеты: python3 generate_client_reports.py")
        print("3. Интегрируйте с вашей системой")
        
        if metrics['test_r2'] > 0.7:
            print(f"\n✅ Отличное качество модели! R² = {metrics['test_r2']:.3f}")
        elif metrics['test_r2'] > 0.5:
            print(f"\n⚠️ Хорошее качество модели. R² = {metrics['test_r2']:.3f}")
        else:
            print(f"\n❌ Качество модели требует улучшения. R² = {metrics['test_r2']:.3f}")
        
    except Exception as e:
        print(f"\n❌ Ошибка обучения: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()