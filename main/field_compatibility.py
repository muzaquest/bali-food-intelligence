#!/usr/bin/env python3
"""
Модуль для обеспечения совместимости названий полей
между базовой и улучшенной системами загрузки данных
"""

import pandas as pd
import logging

logger = logging.getLogger(__name__)

# Mapping старых названий полей на новые в улучшенной системе
FIELD_COMPATIBILITY_MAP = {
    # Основные поля (остаются теми же)
    'date': 'date',
    'restaurant_name': 'restaurant_name',
    'total_sales': 'total_sales',
    'orders': 'orders',
    'rating': 'rating',
    'platform': 'platform',
    
    # Временные поля gojek
    'delivery_time': 'delivery_time_minutes',  # ОСНОВНАЯ ПРОБЛЕМА
    'accepting_time': 'accepting_time_minutes',
    'preparation_time': 'preparation_time_minutes',
    
    # Поля которые могут отсутствовать в базовой системе
    'cancel_rate': 'cancel_rate',
    'ads_on': 'ads_on',
    'roas': 'roas',
    'avg_order_value': 'avg_order_value',
    
    # Погодные поля (только в улучшенной системе)
    'temperature_celsius': 'temperature_celsius',
    'humidity_percent': 'humidity_percent',
    'precipitation_mm': 'precipitation_mm',
    'is_rainy': 'is_rainy',
    'is_hot': 'is_hot',
    'weather_condition': 'weather_condition',
    
    # Календарные поля (только в улучшенной системе)
    'is_weekend': 'is_weekend',
    'is_holiday': 'is_holiday',
    'day_of_week': 'day_of_week',
    'month': 'month',
    'is_tourist_high_season': 'is_tourist_high_season'
}

def ensure_field_compatibility(df: pd.DataFrame) -> pd.DataFrame:
    """
    Обеспечивает совместимость полей между базовой и улучшенной системами
    
    Создает недостающие поля с разумными значениями по умолчанию
    """
    if df.empty:
        return df
    
    df_copy = df.copy()
    
    try:
        # 1. Создаем поле delivery_time если его нет, но есть delivery_time_minutes
        if 'delivery_time' not in df_copy.columns and 'delivery_time_minutes' in df_copy.columns:
            df_copy['delivery_time'] = df_copy['delivery_time_minutes']
            logger.info("✅ Создано поле 'delivery_time' из 'delivery_time_minutes'")
        
        # 2. Создаем поле delivery_time_minutes если его нет, но есть delivery_time
        elif 'delivery_time_minutes' not in df_copy.columns and 'delivery_time' in df_copy.columns:
            df_copy['delivery_time_minutes'] = df_copy['delivery_time']
            logger.info("✅ Создано поле 'delivery_time_minutes' из 'delivery_time'")
        
        # 3. Если нет ни одного из полей времени доставки, создаем синтетическое
        elif 'delivery_time' not in df_copy.columns and 'delivery_time_minutes' not in df_copy.columns:
            # Синтетическое время доставки на основе платформы
            if 'platform' in df_copy.columns:
                df_copy['delivery_time'] = df_copy['platform'].apply(
                    lambda x: 25 if x == 'gojek' else 30
                )
            else:
                df_copy['delivery_time'] = 30  # Значение по умолчанию
            
            df_copy['delivery_time_minutes'] = df_copy['delivery_time']
            logger.info("✅ Создано синтетическое поле 'delivery_time'")
        
        # 4. Аналогично для других временных полей
        time_field_pairs = [
            ('accepting_time', 'accepting_time_minutes'),
            ('preparation_time', 'preparation_time_minutes')
        ]
        
        for old_field, new_field in time_field_pairs:
            if old_field not in df_copy.columns and new_field in df_copy.columns:
                df_copy[old_field] = df_copy[new_field]
            elif new_field not in df_copy.columns and old_field in df_copy.columns:
                df_copy[new_field] = df_copy[old_field]
        
        # 5. Создаем базовые поля если их нет
        essential_fields = {
            'cancel_rate': 0.05,  # 5% по умолчанию
            'ads_on': 0,          # Реклама выключена по умолчанию
            'roas': 0,            # Нет возврата от рекламы
            'avg_order_value': lambda df: df['total_sales'] / (df['orders'] + 1e-8) if 'total_sales' in df.columns and 'orders' in df.columns else 50000
        }
        
        for field, default_value in essential_fields.items():
            if field not in df_copy.columns:
                if callable(default_value):
                    df_copy[field] = default_value(df_copy)
                else:
                    df_copy[field] = default_value
                logger.info(f"✅ Создано поле '{field}' со значением по умолчанию")
        
        # 6. Погодные поля (если нет, создаем нейтральные)
        weather_defaults = {
            'temperature_celsius': 29,     # Средняя температура в Бали
            'humidity_percent': 75,        # Средняя влажность
            'precipitation_mm': 0,         # Без дождя по умолчанию
            'is_rainy': False,
            'is_hot': False,
            'weather_condition': 'clear'
        }
        
        for field, default_value in weather_defaults.items():
            if field not in df_copy.columns:
                df_copy[field] = default_value
        
        # 7. Календарные поля (если нет, создаем на основе даты)
        if 'date' in df_copy.columns:
            df_copy['date'] = pd.to_datetime(df_copy['date'])
            
            if 'day_of_week' not in df_copy.columns:
                df_copy['day_of_week'] = df_copy['date'].dt.dayofweek
            
            if 'is_weekend' not in df_copy.columns:
                df_copy['is_weekend'] = df_copy['day_of_week'].isin([5, 6])
            
            if 'month' not in df_copy.columns:
                df_copy['month'] = df_copy['date'].dt.month
            
            if 'is_holiday' not in df_copy.columns:
                df_copy['is_holiday'] = False  # Упрощение
            
            if 'is_tourist_high_season' not in df_copy.columns:
                df_copy['is_tourist_high_season'] = df_copy['month'].isin([7, 8, 12, 1])
        
        logger.info(f"✅ Обеспечена совместимость полей: {len(df_copy.columns)} итоговых полей")
        return df_copy
        
    except Exception as e:
        logger.error(f"❌ Ошибка при обеспечении совместимости полей: {e}")
        return df

def check_required_fields(df: pd.DataFrame, required_fields: list) -> dict:
    """
    Проверяет наличие обязательных полей в DataFrame
    
    Returns:
        dict: {'missing': [список отсутствующих полей], 'available': [список доступных полей]}
    """
    if df.empty:
        return {'missing': required_fields, 'available': []}
    
    available_fields = []
    missing_fields = []
    
    for field in required_fields:
        if field in df.columns:
            available_fields.append(field)
        else:
            missing_fields.append(field)
    
    return {
        'missing': missing_fields,
        'available': available_fields
    }

def get_field_mapping_info() -> dict:
    """Возвращает информацию о mapping полей"""
    return {
        'compatibility_map': FIELD_COMPATIBILITY_MAP,
        'critical_mappings': {
            'delivery_time': 'delivery_time_minutes',
            'accepting_time': 'accepting_time_minutes',
            'preparation_time': 'preparation_time_minutes'
        },
        'weather_fields': [
            'temperature_celsius', 'humidity_percent', 'precipitation_mm',
            'is_rainy', 'is_hot', 'weather_condition'
        ],
        'calendar_fields': [
            'is_weekend', 'is_holiday', 'day_of_week', 'month', 'is_tourist_high_season'
        ]
    }

if __name__ == "__main__":
    # Тест функций совместимости
    print("🧪 ТЕСТИРОВАНИЕ СОВМЕСТИМОСТИ ПОЛЕЙ")
    
    # Создаем тестовый DataFrame
    test_df = pd.DataFrame({
        'date': pd.date_range('2024-01-01', periods=5),
        'restaurant_name': ['Test'] * 5,
        'total_sales': [1000, 1500, 2000, 1200, 1800],
        'orders': [20, 30, 40, 25, 35],
        'delivery_time_minutes': [25, 30, 28, 32, 27]  # Новое поле
    })
    
    print(f"📊 Исходные поля: {list(test_df.columns)}")
    
    compatible_df = ensure_field_compatibility(test_df)
    
    print(f"✅ После совместимости: {len(compatible_df.columns)} полей")
    
    # Проверяем обязательные поля
    required = ['delivery_time', 'cancel_rate', 'is_weekend', 'temperature_celsius']
    check_result = check_required_fields(compatible_df, required)
    
    print(f"🔍 Доступные обязательные поля: {check_result['available']}")
    print(f"❌ Отсутствующие поля: {check_result['missing']}")