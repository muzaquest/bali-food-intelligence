#!/usr/bin/env python3
"""
Тестирование обученной ML модели и генерация анализа
"""

import pandas as pd
import numpy as np
import sqlite3
import joblib
import json
from datetime import datetime, timedelta

def load_model():
    """Загружает обученную модель"""
    print("🤖 Загрузка модели...")
    
    model = joblib.load('models/client_sales_model.joblib')
    
    with open('models/client_model_metadata.json', 'r') as f:
        metadata = json.load(f)
    
    print(f"✅ Модель загружена: {metadata['model_type']}")
    print(f"📅 Обучена: {metadata['trained_at']}")
    print(f"🎯 R² на тесте: {metadata['metrics']['test_r2']:.4f}")
    
    return model, metadata

def prepare_prediction_data(restaurant_id, date, db_path='client_data.db'):
    """Подготавливает данные для предсказания"""
    print(f"📊 Подготовка данных для ресторана {restaurant_id} на {date}")
    
    conn = sqlite3.connect(db_path)
    
    # Загружаем исторические данные для создания лаговых признаков
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
    WHERE g.restaurant_id = ? AND DATE(g.date) <= ?
    ORDER BY g.date DESC
    LIMIT 30
    """
    
    df = pd.read_sql_query(query, conn, params=(restaurant_id, date))
    conn.close()
    
    if len(df) == 0:
        print(f"❌ Нет данных для ресторана {restaurant_id}")
        return None
    
    # Создаем признаки (используем ту же логику что при обучении)
    df['date'] = pd.to_datetime(df['date'])
    
    # Находим целевую дату
    target_date = pd.to_datetime(date)
    df['date'] = pd.to_datetime(df['date'])
    target_row = df[df['date'].dt.date == target_date.date()]
    
    if len(target_row) == 0:
        print(f"❌ Нет данных на дату {date}")
        return None
    
    # Создаем признаки для целевой даты
    target_data = target_row.iloc[0].copy()
    
    # Временные признаки
    target_data['year'] = target_date.year
    target_data['month'] = target_date.month
    target_data['day'] = target_date.day
    target_data['day_of_week'] = target_date.dayofweek
    target_data['week_of_year'] = target_date.isocalendar().week
    
    # Сезонные признаки
    target_data['is_weekend'] = int(target_date.dayofweek in [5, 6])
    target_data['is_high_season'] = int(target_date.month in [6, 7, 8])
    target_data['is_low_season'] = int(target_date.month in [3, 4, 5, 9, 10])
    
    # Кодирование категориальных признаков
    all_regions = ['Seminyak', 'Ubud', 'Canggu', 'Denpasar', 'Sanur']
    all_restaurants = ['Warung Bali Asli', 'Ubud Organic Cafe', 'Canggu Surf Cafe', 
                      'Denpasar Local', 'Sanur Beach Resto', 'Nusa Dua Fine Dining',
                      'Jimbaran Seafood', 'Kuta Night Market']
    
    target_data['region_encoded'] = all_regions.index(target_data['region']) if target_data['region'] in all_regions else 0
    target_data['restaurant_encoded'] = all_restaurants.index(target_data['restaurant_name']) if target_data['restaurant_name'] in all_restaurants else 0
    
    # Нормализация (используем средние значения из обучающих данных)
    target_data['rating_normalized'] = (target_data['rating'] - 4.0) / 0.5
    target_data['delivery_time_normalized'] = (target_data['delivery_time'] - 30) / 10
    
    # Лаговые признаки
    df_sorted = df.sort_values('date')
    target_idx = df_sorted[df_sorted['date'] == target_date].index[0]
    
    for lag in [1, 2, 3, 7]:
        lag_idx = target_idx - lag
        if lag_idx >= 0 and lag_idx < len(df_sorted):
            target_data[f'sales_lag_{lag}'] = df_sorted.iloc[lag_idx]['sales']
        else:
            target_data[f'sales_lag_{lag}'] = 0
    
    # Скользящие средние
    for window in [3, 7, 14]:
        if len(df_sorted) >= window:
            recent_sales = df_sorted.iloc[-window:]['sales']
            target_data[f'sales_ma_{window}'] = recent_sales.mean()
        else:
            target_data[f'sales_ma_{window}'] = df_sorted['sales'].mean()
    
    # Относительные изменения
    if len(df_sorted) >= 2:
        target_data['sales_change_1d'] = (df_sorted.iloc[-1]['sales'] - df_sorted.iloc[-2]['sales']) / df_sorted.iloc[-2]['sales']
    else:
        target_data['sales_change_1d'] = 0
    
    if len(df_sorted) >= 8:
        target_data['sales_change_7d'] = (df_sorted.iloc[-1]['sales'] - df_sorted.iloc[-8]['sales']) / df_sorted.iloc[-8]['sales']
    else:
        target_data['sales_change_7d'] = 0
    
    # Бизнес-признаки
    target_data['ads_enabled'] = int(target_data['ads_enabled'])
    
    return target_data

def predict_sales(model, metadata, target_data):
    """Делает предсказание продаж"""
    feature_names = metadata['feature_names']
    
    # Создаем вектор признаков
    X = []
    for feature in feature_names:
        if feature in target_data:
            X.append(target_data[feature])
        else:
            X.append(0)  # Значение по умолчанию
    
    X = np.array(X).reshape(1, -1)
    
    # Делаем предсказание
    predicted_sales = model.predict(X)[0]
    
    # Получаем важность признаков для объяснения
    feature_importance = {
        feature: importance 
        for feature, importance in zip(feature_names, model.feature_importances_)
    }
    
    return predicted_sales, feature_importance

def analyze_restaurant_day(restaurant_id, date, db_path='client_data.db'):
    """Полный анализ ресторана на конкретную дату"""
    print(f"🎯 АНАЛИЗ РЕСТОРАНА {restaurant_id} НА ДАТУ {date}")
    print("=" * 60)
    
    # Загружаем модель
    model, metadata = load_model()
    
    # Подготавливаем данные
    target_data = prepare_prediction_data(restaurant_id, date, db_path)
    if target_data is None:
        return None
    
    # Делаем предсказание
    predicted_sales, feature_importance = predict_sales(model, metadata, target_data)
    
    # Получаем фактические продажи
    actual_sales = target_data['sales']
    
    # Вычисляем отклонение
    sales_diff = actual_sales - predicted_sales
    sales_diff_percent = (sales_diff / predicted_sales) * 100
    
    # Создаем отчет
    report = {
        'restaurant_id': restaurant_id,
        'restaurant_name': target_data['restaurant_name'],
        'region': target_data['region'],
        'date': date,
        'actual_sales': float(actual_sales),
        'predicted_sales': float(predicted_sales),
        'sales_difference': float(sales_diff),
        'sales_difference_percent': float(sales_diff_percent),
        'analysis': {
            'orders': int(target_data['orders']),
            'avg_order_value': float(target_data['avg_order_value']),
            'ads_enabled': bool(target_data['ads_enabled']),
            'rating': float(target_data['rating']),
            'delivery_time': int(target_data['delivery_time']),
            'is_weekend': bool(target_data['is_weekend']),
            'is_high_season': bool(target_data['is_high_season'])
        },
        'feature_importance': {k: float(v) for k, v in sorted(feature_importance.items(), key=lambda x: x[1], reverse=True)[:10]},
        'generated_at': datetime.now().isoformat()
    }
    
    # Выводим результаты
    print(f"🏪 Ресторан: {target_data['restaurant_name']}")
    print(f"📍 Регион: {target_data['region']}")
    print(f"📅 Дата: {date}")
    print(f"💰 Фактические продажи: {actual_sales:,.0f} IDR")
    print(f"🎯 Предсказанные продажи: {predicted_sales:,.0f} IDR")
    print(f"📊 Отклонение: {sales_diff:+,.0f} IDR ({sales_diff_percent:+.1f}%)")
    
    print(f"\n📈 АНАЛИЗ ФАКТОРОВ:")
    print(f"  📦 Заказов: {target_data['orders']}")
    print(f"  💵 Средний чек: {target_data['avg_order_value']:,.0f} IDR")
    print(f"  📱 Реклама: {'Включена' if target_data['ads_enabled'] else 'Выключена'}")
    print(f"  ⭐ Рейтинг: {target_data['rating']:.1f}")
    print(f"  🚚 Время доставки: {target_data['delivery_time']} мин")
    print(f"  📅 Выходной: {'Да' if target_data['is_weekend'] else 'Нет'}")
    print(f"  🏖️ Высокий сезон: {'Да' if target_data['is_high_season'] else 'Нет'}")
    
    print(f"\n🔝 ТОП-5 ВАЖНЫХ ПРИЗНАКОВ:")
    for i, (feature, importance) in enumerate(list(feature_importance.items())[:5], 1):
        print(f"  {i}. {feature}: {importance:.4f}")
    
    return report

def test_multiple_restaurants():
    """Тестирует модель на нескольких ресторанах"""
    print("🧪 ТЕСТИРОВАНИЕ МОДЕЛИ НА НЕСКОЛЬКИХ РЕСТОРАНАХ")
    print("=" * 60)
    
    test_cases = [
        (1, '2023-06-15'),  # Warung Bali Asli, высокий сезон
        (2, '2023-03-10'),  # Ubud Organic Cafe, низкий сезон
        (3, '2023-12-25'),  # Canggu Surf Cafe, праздник
        (6, '2023-07-01'),  # Nusa Dua Fine Dining, пик сезона
        (8, '2023-04-15'),  # Kuta Night Market, обычный день
    ]
    
    results = []
    
    for restaurant_id, date in test_cases:
        print(f"\n{'='*40}")
        result = analyze_restaurant_day(restaurant_id, date)
        if result:
            results.append(result)
    
    # Сводная статистика
    print(f"\n📊 СВОДНАЯ СТАТИСТИКА:")
    print("=" * 60)
    
    if results:
        avg_error = np.mean([abs(r['sales_difference_percent']) for r in results])
        print(f"📈 Средняя ошибка: {avg_error:.1f}%")
        
        accurate_predictions = sum(1 for r in results if abs(r['sales_difference_percent']) < 10)
        print(f"🎯 Точных предсказаний (±10%): {accurate_predictions}/{len(results)} ({accurate_predictions/len(results)*100:.1f}%)")
        
        # Сохраняем результаты
        with open('test_results.json', 'w', encoding='utf-8') as f:
            json.dump(results, f, ensure_ascii=False, indent=2, default=str)
        
        print(f"💾 Результаты сохранены в test_results.json")
    
    return results

def main():
    print("🚀 ТЕСТИРОВАНИЕ ML МОДЕЛИ КЛИЕНТА")
    print("=" * 60)
    
    # Тестируем на нескольких ресторанах
    results = test_multiple_restaurants()
    
    print("\n🎉 ТЕСТИРОВАНИЕ ЗАВЕРШЕНО!")
    print("=" * 60)
    print("📋 Следующие шаги:")
    print("1. Проверьте результаты в test_results.json")
    print("2. Запустите generate_client_reports.py для создания отчетов")
    print("3. Интегрируйте модель в вашу систему")

if __name__ == "__main__":
    main()