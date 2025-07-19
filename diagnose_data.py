#!/usr/bin/env python3
"""
Диагностика проблем в данных
"""

import pandas as pd
import numpy as np
from data_loader import load_data_for_training
from feature_engineering import FeatureEngineer

def diagnose_data():
    """Диагностируем проблемы в данных"""
    
    print("🔍 ДИАГНОСТИКА ДАННЫХ")
    print("=" * 50)
    
    # Загружаем сырые данные
    df = load_data_for_training()
    print(f"✅ Загружено {len(df)} записей")
    
    # Создаем feature engineer
    engineer = FeatureEngineer()
    
    # Подготавливаем признаки
    print("\n🔧 Подготавливаем признаки...")
    df_prepared = engineer.prepare_features(df)
    
    print(f"✅ Подготовлено {len(df_prepared)} записей с {len(df_prepared.columns)} колонками")
    
    # Проверяем на проблемы
    print("\n🚨 ПРОВЕРКА НА ПРОБЛЕМЫ:")
    
    for col in df_prepared.columns:
        if df_prepared[col].dtype in ['float64', 'int64', 'float32', 'int32']:
            
            # Проверяем на infinity
            inf_count = np.isinf(df_prepared[col]).sum()
            if inf_count > 0:
                print(f"❌ {col}: {inf_count} infinity значений")
                
            # Проверяем на NaN
            nan_count = df_prepared[col].isna().sum()
            if nan_count > 0:
                print(f"⚠️  {col}: {nan_count} NaN значений")
                
            # Проверяем на очень большие числа
            if not df_prepared[col].isna().all():
                max_val = df_prepared[col].max()
                min_val = df_prepared[col].min()
                
                if abs(max_val) > 1e10 or abs(min_val) > 1e10:
                    print(f"⚠️  {col}: Очень большие значения ({min_val:.2e} to {max_val:.2e})")
    
    # Выводим статистику по основным признакам
    print("\n📊 СТАТИСТИКА КЛЮЧЕВЫХ ПРИЗНАКОВ:")
    key_features = ['delta_sales_prev', 'target', 'pct_change_sales', 'rolling_std_7']
    
    for feature in key_features:
        if feature in df_prepared.columns:
            print(f"\n{feature}:")
            print(f"  Count: {df_prepared[feature].count()}")
            print(f"  Mean: {df_prepared[feature].mean():.2f}")
            print(f"  Std: {df_prepared[feature].std():.2f}")
            print(f"  Min: {df_prepared[feature].min():.2f}")
            print(f"  Max: {df_prepared[feature].max():.2f}")
            print(f"  Infinity: {np.isinf(df_prepared[feature]).sum()}")
            print(f"  NaN: {df_prepared[feature].isna().sum()}")

if __name__ == "__main__":
    diagnose_data()