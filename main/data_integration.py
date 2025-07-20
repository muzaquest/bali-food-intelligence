#!/usr/bin/env python3
"""
Модуль интеграции улучшенной системы загрузки данных
Обеспечивает плавный переход от базовой к расширенной системе
"""

import pandas as pd
import logging
from typing import Optional
from main.field_compatibility import ensure_field_compatibility

logger = logging.getLogger(__name__)

def load_data_with_all_features(db_path: str = None) -> pd.DataFrame:
    """
    Главная функция загрузки данных с максимальной детализацией
    
    Автоматически выбирает между улучшенной и базовой системой
    """
    
    # Попытка использовать улучшенную систему
    try:
        from main.data_loader_enhanced import load_data_enhanced
        logger.info("🚀 ИСПОЛЬЗУЕМ УЛУЧШЕННУЮ СИСТЕМУ (все поля + погода + календарь)")
        
        enhanced_df = load_data_enhanced(db_path)
        
        if not enhanced_df.empty:
            logger.info(f"✅ Загружено с улучшенной системой: {len(enhanced_df)} записей с {len(enhanced_df.columns)} полями")
            # Обеспечиваем совместимость полей
            enhanced_df = ensure_field_compatibility(enhanced_df)
            return enhanced_df
        else:
            logger.warning("⚠️ Улучшенная система вернула пустой результат")
            
    except ImportError as e:
        logger.warning(f"⚠️ Улучшенная система недоступна: {e}")
    except Exception as e:
        logger.error(f"❌ Ошибка в улучшенной системе: {e}")
    
    # Резерв: базовая система
    try:
        from main.data_loader import load_data_for_training
        logger.info("🔄 Используем базовую систему как резерв")
        
        basic_df = load_data_for_training(db_path)
        
        if not basic_df.empty:
            logger.info(f"✅ Загружено с базовой системой: {len(basic_df)} записей с {len(basic_df.columns)} полями")
            # Обеспечиваем совместимость полей
            basic_df = ensure_field_compatibility(basic_df)
            return basic_df
        else:
            logger.error("❌ Базовая система также вернула пустой результат")
            
    except Exception as e:
        logger.error(f"❌ Ошибка в базовой системе: {e}")
    
    # Если все не удалось
    logger.error("💥 КРИТИЧЕСКАЯ ОШИБКА: Не удалось загрузить данные ни одной системой")
    return pd.DataFrame()

def prepare_features_with_all_enhancements(df: pd.DataFrame) -> pd.DataFrame:
    """
    Создание features с максимальными улучшениями
    
    Автоматически выбирает между улучшенной и базовой системой
    """
    
    if df.empty:
        logger.warning("⚠️ Получен пустой DataFrame для обработки features")
        return df
    
    # Попытка использовать улучшенную систему feature engineering
    try:
        from main.feature_engineering_enhanced import prepare_features_enhanced
        logger.info("🚀 ИСПОЛЬЗУЕМ УЛУЧШЕННЫЙ FEATURE ENGINEERING")
        
        enhanced_df = prepare_features_enhanced(df)
        
        if not enhanced_df.empty and len(enhanced_df.columns) >= len(df.columns):
            new_features = len(enhanced_df.columns) - len(df.columns)
            logger.info(f"✅ Создано {new_features} новых features с улучшенной системой")
            return enhanced_df
        else:
            logger.warning("⚠️ Улучшенная система feature engineering не дала результата")
            
    except ImportError as e:
        logger.warning(f"⚠️ Улучшенная система feature engineering недоступна: {e}")
    except Exception as e:
        logger.error(f"❌ Ошибка в улучшенной системе feature engineering: {e}")
    
    # Резерв: базовая система
    try:
        from main.feature_engineering import prepare_features
        logger.info("🔄 Используем базовую систему feature engineering как резерв")
        
        basic_df = prepare_features(df)
        
        if not basic_df.empty:
            new_features = len(basic_df.columns) - len(df.columns)
            logger.info(f"✅ Создано {new_features} новых features с базовой системой")
            return basic_df
        else:
            logger.error("❌ Базовая система feature engineering также не дала результата")
            
    except Exception as e:
        logger.error(f"❌ Ошибка в базовой системе feature engineering: {e}")
    
    # Если все не удалось, возвращаем исходные данные
    logger.warning("⚠️ Возвращаем исходные данные без дополнительных features")
    return df

def get_system_status() -> dict:
    """Проверка статуса доступных систем"""
    
    status = {
        'enhanced_loader_available': False,
        'enhanced_features_available': False,
        'basic_loader_available': False,
        'basic_features_available': False,
        'recommendation': 'unknown'
    }
    
    # Проверка улучшенного загрузчика
    try:
        from main.data_loader_enhanced import load_data_enhanced
        status['enhanced_loader_available'] = True
    except ImportError:
        pass
    
    # Проверка улучшенного feature engineering
    try:
        from main.feature_engineering_enhanced import prepare_features_enhanced
        status['enhanced_features_available'] = True
    except ImportError:
        pass
    
    # Проверка базового загрузчика
    try:
        from main.data_loader import load_data_for_training
        status['basic_loader_available'] = True
    except ImportError:
        pass
    
    # Проверка базового feature engineering
    try:
        from main.feature_engineering import prepare_features
        status['basic_features_available'] = True
    except ImportError:
        pass
    
    # Рекомендация
    if status['enhanced_loader_available'] and status['enhanced_features_available']:
        status['recommendation'] = 'enhanced_full'
    elif status['enhanced_loader_available']:
        status['recommendation'] = 'enhanced_loader_only'
    elif status['basic_loader_available'] and status['basic_features_available']:
        status['recommendation'] = 'basic_full'
    else:
        status['recommendation'] = 'error'
    
    return status

def print_system_status():
    """Вывод статуса систем"""
    status = get_system_status()
    
    print("🔍 СТАТУС СИСТЕМ ЗАГРУЗКИ И ОБРАБОТКИ ДАННЫХ:")
    print("=" * 60)
    
    print(f"📊 Улучшенный загрузчик данных: {'✅ Доступен' if status['enhanced_loader_available'] else '❌ Недоступен'}")
    print(f"🌟 Улучшенный feature engineering: {'✅ Доступен' if status['enhanced_features_available'] else '❌ Недоступен'}")
    print(f"📈 Базовый загрузчик данных: {'✅ Доступен' if status['basic_loader_available'] else '❌ Недоступен'}")
    print(f"🔧 Базовый feature engineering: {'✅ Доступен' if status['basic_features_available'] else '❌ Недоступен'}")
    
    print(f"\n🎯 РЕКОМЕНДАЦИЯ:")
    if status['recommendation'] == 'enhanced_full':
        print("✅ Используйте МАКСИМАЛЬНУЮ систему (все поля + погода + календарь)")
    elif status['recommendation'] == 'enhanced_loader_only':
        print("🔄 Используйте улучшенный загрузчик + базовый feature engineering")
    elif status['recommendation'] == 'basic_full':
        print("⚠️ Используйте базовую систему (ограниченная функциональность)")
    else:
        print("❌ ОШИБКА: Критические компоненты недоступны")

if __name__ == "__main__":
    # Проверка статуса систем
    print_system_status()
    
    # Тест загрузки данных
    print("\n🧪 ТЕСТИРОВАНИЕ ИНТЕГРИРОВАННОЙ СИСТЕМЫ:")
    
    df = load_data_with_all_features()
    if not df.empty:
        print(f"📊 Данные загружены: {len(df)} записей с {len(df.columns)} полями")
        
        enhanced_df = prepare_features_with_all_enhancements(df)
        print(f"🌟 После feature engineering: {len(enhanced_df.columns)} полей")
        print(f"🔧 Добавлено: {len(enhanced_df.columns) - len(df.columns)} новых features")
    else:
        print("❌ Не удалось загрузить данные")