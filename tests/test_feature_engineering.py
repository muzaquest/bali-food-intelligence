"""
Тесты для модуля feature_engineering
КРИТИЧЕСКАЯ ПРОВЕРКА НА DATA LEAKAGE
"""
import pytest
import pandas as pd
import numpy as np
import sys
import os

# Добавляем путь к модулям
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from feature_engineering import FeatureEngineer


class TestFeatureEngineer:
    
    @pytest.fixture
    def sample_data(self):
        """Создает тестовые данные"""
        np.random.seed(42)
        data = {
            'restaurant_name': ['Restaurant_A'] * 10 + ['Restaurant_B'] * 10,
            'date': pd.date_range('2023-01-01', periods=20),
            'total_sales': np.random.randint(100, 500, 20),
            'rating': np.random.uniform(3.0, 5.0, 20),
            'cancel_rate': np.random.uniform(0.0, 0.3, 20),
            'ads_on': np.random.randint(0, 2, 20),
            'is_holiday': np.random.randint(0, 2, 20)
        }
        return pd.DataFrame(data)
    
    def test_delta_sales_prev_no_leakage(self, sample_data):
        """🚨 КРИТИЧЕСКИЙ ТЕСТ: Проверка отсутствия утечки данных в delta_sales_prev"""
        engineer = FeatureEngineer()
        df_with_features = engineer.create_temporal_features(sample_data)
        
        # Проверяем, что delta_sales_prev не совпадает с target
        df_with_target = engineer.create_target_variable(df_with_features)
        
        # delta_sales_prev должна быть сдвинута относительно target
        if 'delta_sales_prev' in df_with_target.columns and 'target' in df_with_target.columns:
            # Убираем NaN для сравнения
            valid_mask = ~(df_with_target['delta_sales_prev'].isna() | df_with_target['target'].isna())
            if valid_mask.sum() > 0:
                delta_vals = df_with_target.loc[valid_mask, 'delta_sales_prev'].values
                target_vals = df_with_target.loc[valid_mask, 'target'].values
                # Они не должны быть идентичными
                assert not np.array_equal(delta_vals, target_vals), "🚨 КРИТИЧНО: delta_sales_prev и target идентичны - есть data leakage!"
        
    def test_no_future_leakage(self, sample_data):
        """Тест на отсутствие использования будущей информации"""
        engineer = FeatureEngineer()
        df_features = engineer.create_temporal_features(sample_data)
        
        # Первые строки должны иметь NaN в delta_sales_prev для каждого ресторана
        for restaurant in sample_data['restaurant_name'].unique():
            restaurant_data = df_features[df_features['restaurant_name'] == restaurant].sort_values('date')
            if len(restaurant_data) >= 2:
                # Первая строка должна быть NaN
                assert pd.isna(restaurant_data.iloc[0]['delta_sales_prev']), f"Первая строка для {restaurant} должна быть NaN"
                # Вторая строка тоже должна быть NaN (используем shift(1) - shift(2))
                assert pd.isna(restaurant_data.iloc[1]['delta_sales_prev']), f"Вторая строка для {restaurant} должна быть NaN"
        
    def test_rolling_means_shifted(self, sample_data):
        """Тест что скользящие средние сдвинуты для избежания data leakage"""
        engineer = FeatureEngineer()
        df_features = engineer.create_temporal_features(sample_data)
        
        # Проверяем, что первые значения rolling_mean_3 и rolling_mean_7 - NaN
        for restaurant in sample_data['restaurant_name'].unique():
            restaurant_data = df_features[df_features['restaurant_name'] == restaurant].sort_values('date')
            if len(restaurant_data) >= 1:
                # Первое значение должно быть NaN из-за shift(1)
                assert pd.isna(restaurant_data.iloc[0]['rolling_mean_3']), f"rolling_mean_3 должно быть NaN для первой строки {restaurant}"
                assert pd.isna(restaurant_data.iloc[0]['rolling_mean_7']), f"rolling_mean_7 должно быть NaN для первой строки {restaurant}"
        
    def test_feature_creation(self, sample_data):
        """Базовый тест создания признаков"""
        engineer = FeatureEngineer()
        result = engineer.prepare_features(sample_data)
        
        assert result is not None
        assert len(result) > 0
        assert 'delta_sales_prev' in result.columns
        assert 'target' in result.columns


class TestBasicFunctionality:
    """Базовые тесты, которые должны проходить всегда"""
    
    def test_imports(self):
        """Тест импортов основных модулей"""
        try:
            import pandas as pd
            import numpy as np
            import sklearn
            assert True
        except ImportError as e:
            pytest.fail(f"Ошибка импорта базовых библиотек: {e}")
            
    def test_pandas_basic(self):
        """Тест базовой функциональности pandas"""
        df = pd.DataFrame({'A': [1, 2, 3], 'B': [4, 5, 6]})
        assert len(df) == 3
        assert 'A' in df.columns
        assert 'B' in df.columns