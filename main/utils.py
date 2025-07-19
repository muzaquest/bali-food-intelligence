"""
Вспомогательные функции для ML-модели анализа продаж
"""
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import json
import logging

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def setup_logging():
    """Настройка логирования"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

def validate_date(date_str):
    """Валидация формата даты"""
    try:
        datetime.strptime(date_str, '%Y-%m-%d')
        return True
    except ValueError:
        return False

def calculate_percentage_change(current, previous):
    """Расчет процентного изменения"""
    if previous == 0:
        return 0
    return ((current - previous) / previous) * 100

def format_currency(amount):
    """Форматирование валюты"""
    return f"{amount:,.0f}"

def get_day_of_week(date_str):
    """Получение дня недели (0=понедельник, 6=воскресенье)"""
    date_obj = datetime.strptime(date_str, '%Y-%m-%d')
    return date_obj.weekday()

def create_result_json(restaurant, date, actual_sales, predicted_sales, 
                      shap_values, feature_names, explanation):
    """Создание JSON результата анализа"""
    change_percent = calculate_percentage_change(actual_sales, predicted_sales)
    
    # Топ-3 факторов по абсолютному значению SHAP
    shap_dict = dict(zip(feature_names, shap_values))
    top_factors = dict(sorted(shap_dict.items(), 
                             key=lambda x: abs(x[1]), reverse=True)[:3])
    
    result = {
        "restaurant": restaurant,
        "date": date,
        "actual_sales": int(actual_sales),
        "predicted_sales": int(predicted_sales),
        "change_percent": round(change_percent, 1),
        "top_factors": {k: round(v, 3) for k, v in top_factors.items()},
        "explanation": explanation,
        "timestamp": datetime.now().isoformat()
    }
    
    return result

def save_result_to_file(result, filename):
    """Сохранение результата в файл"""
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    logger.info(f"Результат сохранен в {filename}")

def generate_explanation(shap_values, feature_names, threshold=0.05):
    """Генерация человеко-понятного объяснения"""
    explanations = []
    
    # Словарь для перевода названий признаков
    feature_translations = {
        'ads_on': 'реклама',
        'rain_mm': 'дождь',
        'rating': 'рейтинг',
        'temp_c': 'температура',
        'is_holiday': 'праздник',
        'cancel_rate': 'отмены',
        'position': 'позиция',
        'roas': 'ROAS'
    }
    
    shap_dict = dict(zip(feature_names, shap_values))
    
    # Сортируем по абсолютному значению влияния
    sorted_factors = sorted(shap_dict.items(), 
                           key=lambda x: abs(x[1]), reverse=True)
    
    for feature, impact in sorted_factors:
        if abs(impact) > threshold:
            feature_name = feature_translations.get(feature, feature)
            impact_percent = int(impact * 100)
            
            if impact > 0:
                explanations.append(f"+{impact_percent}% от {feature_name}")
            else:
                explanations.append(f"{impact_percent}% от {feature_name}")
    
    if explanations:
        return f"Изменение продаж: {', '.join(explanations[:3])}"
    else:
        return "Значительных факторов влияния не выявлено"

def check_data_quality(df):
    """Проверка качества данных"""
    issues = []
    
    # Проверка на пропущенные значения
    missing_cols = df.columns[df.isnull().any()].tolist()
    if missing_cols:
        issues.append(f"Пропущенные значения в колонках: {missing_cols}")
    
    # Проверка на дубликаты
    duplicates = df.duplicated().sum()
    if duplicates > 0:
        issues.append(f"Найдено {duplicates} дубликатов")
    
    # Проверка на отрицательные продажи
    if 'total_sales' in df.columns:
        negative_sales = (df['total_sales'] < 0).sum()
        if negative_sales > 0:
            issues.append(f"Найдено {negative_sales} записей с отрицательными продажами")
    
    if issues:
        logger.warning("Проблемы с качеством данных:")
        for issue in issues:
            logger.warning(f"  - {issue}")
    else:
        logger.info("Качество данных: OK")
    
    return issues

def create_sample_data():
    """Создание примера данных для тестирования"""
    np.random.seed(42)
    
    dates = pd.date_range('2023-01-01', '2023-12-31', freq='D')
    restaurants = ['Honeycomb', 'Pizza Palace', 'Burger King', 'Sushi Master']
    
    data = []
    for restaurant in restaurants:
        for date in dates:
            # Базовые продажи с трендом и сезонностью
            base_sales = 15000000 + np.random.normal(0, 2000000)
            
            # Влияние дня недели
            day_of_week = date.weekday()
            if day_of_week >= 5:  # выходные
                base_sales *= 1.2
            
            # Влияние праздников
            is_holiday = np.random.choice([0, 1], p=[0.9, 0.1])
            if is_holiday:
                base_sales *= 1.5
            
            # Влияние рекламы
            ads_on = np.random.choice([0, 1], p=[0.3, 0.7])
            if ads_on:
                base_sales *= 1.1
            
            # Влияние погоды
            rain_mm = np.random.exponential(2)
            temp_c = 25 + np.random.normal(0, 5)
            
            if rain_mm > 10:
                base_sales *= 0.9
            
            data.append({
                'restaurant_name': restaurant,
                'date': date.strftime('%Y-%m-%d'),
                'total_sales': max(0, int(base_sales)),
                'ads_sales': int(base_sales * 0.3) if ads_on else 0,
                'rating': np.clip(np.random.normal(4.5, 0.3), 1, 5),
                'roas': np.random.uniform(2, 8) if ads_on else 0,
                'position': np.random.randint(1, 20),
                'cancel_rate': np.random.uniform(0.05, 0.25),
                'ads_on': ads_on,
                'rain_mm': rain_mm,
                'temp_c': temp_c,
                'is_holiday': is_holiday,
                'day_of_week': day_of_week
            })
    
    return pd.DataFrame(data)