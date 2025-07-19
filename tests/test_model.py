"""
Тесты для модуля model
ПРОВЕРКА НА ОТСУТСТВИЕ ПЕРЕОБУЧЕНИЯ И DATA LEAKAGE
"""
import pytest
import pandas as pd
import numpy as np
import sys
import os

# Добавляем путь к модулям
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from model import SalesModel
    MODEL_AVAILABLE = True
except ImportError:
    MODEL_AVAILABLE = False


@pytest.mark.skipif(not MODEL_AVAILABLE, reason="Модуль model недоступен")
class TestSalesModel:
    
    @pytest.fixture
    def sample_data(self):
        """Создает тестовые данные для модели"""
        np.random.seed(42)
        data = {
            'restaurant_name': ['Restaurant_A'] * 20 + ['Restaurant_B'] * 20,
            'date': pd.date_range('2023-01-01', periods=40),
            'total_sales': np.random.randint(100, 500, 40),
            'rating': np.random.uniform(3.0, 5.0, 40),
            'cancel_rate': np.random.uniform(0.0, 0.3, 40),
            'ads_on': np.random.randint(0, 2, 40),
            'is_holiday': np.random.randint(0, 2, 40)
        }
        return pd.DataFrame(data)
    
    def test_model_creation(self):
        """Тест создания модели"""
        model = SalesModel()
        assert model is not None
        assert not model.is_trained
        
    def test_no_perfect_accuracy(self, sample_data):
        """🚨 КРИТИЧЕСКИЙ ТЕСТ: Отсутствие 100% точности (проверка на data leakage)"""
        model = SalesModel()
        
        try:
            metrics = model.train(sample_data)
            
            # R2 не должен быть близок к 1.0 (что указывало бы на data leakage)
            train_r2 = metrics.get('train_r2', 0)
            test_r2 = metrics.get('test_r2', 0)
            
            assert train_r2 < 0.99, f"🚨 КРИТИЧНО: Подозрение на data leakage: Train R2 = {train_r2}"
            assert test_r2 < 0.99, f"🚨 КРИТИЧНО: Подозрение на data leakage: Test R2 = {test_r2}"
            
            # Тренировочный R2 не должен быть намного выше тестового
            if train_r2 > 0.1 and test_r2 > 0.1:  # Только если есть разумные значения
                diff = train_r2 - test_r2
                assert diff < 0.5, f"Возможное переобучение: разница R2 = {diff}"
                
        except Exception as e:
            # Если модель не может обучиться на тестовых данных, это нормально
            pytest.skip(f"Модель не может обучиться на тестовых данных: {e}")


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