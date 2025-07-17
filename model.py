"""
Модуль для обучения и сохранения ML-модели
"""
import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
from sklearn.model_selection import cross_val_score, GridSearchCV
from sklearn.metrics import mean_squared_error, r2_score, mean_absolute_error
import joblib
import logging
from datetime import datetime
import os

from config import MODEL_PARAMS, MODEL_PATH, SCALER_PATH, CV_FOLDS, MIN_R2_SCORE, RANDOM_STATE
from data_loader import load_data_for_training
from feature_engineering import FeatureEngineer

logger = logging.getLogger(__name__)

class SalesPredictor:
    """Класс для обучения и предсказания изменений продаж"""
    
    def __init__(self, model_type='random_forest'):
        self.model_type = model_type
        self.model = None
        self.feature_engineer = FeatureEngineer()
        self.is_trained = False
        self.feature_names = []
        self.training_metrics = {}
        
    def _create_model(self):
        """Создание модели в зависимости от типа"""
        if self.model_type == 'random_forest':
            return RandomForestRegressor(**MODEL_PARAMS)
        elif self.model_type == 'gradient_boosting':
            return GradientBoostingRegressor(
                n_estimators=MODEL_PARAMS['n_estimators'],
                max_depth=MODEL_PARAMS['max_depth'],
                random_state=MODEL_PARAMS['random_state']
            )
        else:
            raise ValueError(f"Неподдерживаемый тип модели: {self.model_type}")
    
    def train(self, df, target_col='target', optimize_hyperparams=False):
        """Обучение модели"""
        logger.info(f"Начинаю обучение модели {self.model_type}")
        
        # Подготавливаем данные
        X_train, X_test, y_train, y_test = self.feature_engineer.prepare_data_for_training(
            df, target_col, scale=False  # Random Forest не требует масштабирования
        )
        
        self.feature_names = self.feature_engineer.feature_names
        
        # Создаем модель
        if optimize_hyperparams:
            self.model = self._optimize_hyperparameters(X_train, y_train)
        else:
            self.model = self._create_model()
        
        # Обучаем модель
        logger.info("Обучение модели...")
        self.model.fit(X_train, y_train)
        
        # Оцениваем качество
        self._evaluate_model(X_train, X_test, y_train, y_test)
        
        self.is_trained = True
        logger.info("Модель успешно обучена")
        
        return self.training_metrics
    
    def _optimize_hyperparameters(self, X_train, y_train):
        """Оптимизация гиперпараметров"""
        logger.info("Оптимизация гиперпараметров...")
        
        if self.model_type == 'random_forest':
            param_grid = {
                'n_estimators': [50, 100, 200],
                'max_depth': [5, 10, 15, None],
                'min_samples_split': [2, 5, 10],
                'min_samples_leaf': [1, 2, 4]
            }
        else:
            param_grid = {
                'n_estimators': [50, 100, 200],
                'max_depth': [3, 5, 7],
                'learning_rate': [0.01, 0.1, 0.2]
            }
        
        base_model = self._create_model()
        
        grid_search = GridSearchCV(
            base_model,
            param_grid,
            cv=3,
            scoring='r2',
            n_jobs=-1,
            verbose=1
        )
        
        grid_search.fit(X_train, y_train)
        
        logger.info(f"Лучшие параметры: {grid_search.best_params_}")
        logger.info(f"Лучший R2 score: {grid_search.best_score_:.4f}")
        
        return grid_search.best_estimator_
    
    def _evaluate_model(self, X_train, X_test, y_train, y_test):
        """Оценка качества модели"""
        logger.info("Оценка качества модели...")
        
        # Предсказания
        y_train_pred = self.model.predict(X_train)
        y_test_pred = self.model.predict(X_test)
        
        # Метрики для обучающей выборки
        train_r2 = r2_score(y_train, y_train_pred)
        train_mse = mean_squared_error(y_train, y_train_pred)
        train_mae = mean_absolute_error(y_train, y_train_pred)
        
        # Метрики для тестовой выборки
        test_r2 = r2_score(y_test, y_test_pred)
        test_mse = mean_squared_error(y_test, y_test_pred)
        test_mae = mean_absolute_error(y_test, y_test_pred)
        
        # Кросс-валидация
        cv_scores = cross_val_score(self.model, X_train, y_train, cv=CV_FOLDS, scoring='r2')
        
        # Сохраняем метрики
        self.training_metrics = {
            'train_r2': train_r2,
            'train_mse': train_mse,
            'train_mae': train_mae,
            'test_r2': test_r2,
            'test_mse': test_mse,
            'test_mae': test_mae,
            'cv_mean_r2': cv_scores.mean(),
            'cv_std_r2': cv_scores.std(),
            'feature_count': len(self.feature_names)
        }
        
        # Логируем результаты
        logger.info("=== Результаты обучения ===")
        logger.info(f"Обучающая выборка R²: {train_r2:.4f}")
        logger.info(f"Тестовая выборка R²: {test_r2:.4f}")
        logger.info(f"Кросс-валидация R²: {cv_scores.mean():.4f} ± {cv_scores.std():.4f}")
        logger.info(f"Тестовая выборка MSE: {test_mse:.2f}")
        logger.info(f"Тестовая выборка MAE: {test_mae:.2f}")
        
        # Проверяем качество
        if test_r2 < MIN_R2_SCORE:
            logger.warning(f"R² score ({test_r2:.4f}) ниже минимального порога ({MIN_R2_SCORE})")
        else:
            logger.info(f"Модель соответствует требованиям качества (R² = {test_r2:.4f})")
    
    def get_feature_importance(self, top_n=20):
        """Получение важности признаков"""
        if not self.is_trained:
            logger.error("Модель не обучена")
            return None
        
        if not hasattr(self.model, 'feature_importances_'):
            logger.error("Модель не поддерживает feature_importances_")
            return None
        
        importance_df = pd.DataFrame({
            'feature': self.feature_names,
            'importance': self.model.feature_importances_
        }).sort_values('importance', ascending=False)
        
        return importance_df.head(top_n)
    
    def predict(self, df, restaurant_name, date):
        """Предсказание для конкретного ресторана и даты"""
        if not self.is_trained:
            logger.error("Модель не обучена")
            return None
        
        # Подготавливаем образец
        X_sample = self.feature_engineer.prepare_single_sample(df, restaurant_name, date)
        
        if X_sample is None:
            return None
        
        # Предсказываем
        prediction = self.model.predict(X_sample)[0]
        
        # Получаем фактические продажи
        sample_data = df[
            (df['restaurant_name'] == restaurant_name) & 
            (df['date'] == date)
        ]
        
        if not sample_data.empty:
            actual_sales = sample_data['total_sales'].iloc[0]
            predicted_sales = actual_sales + prediction  # prediction это изменение
        else:
            actual_sales = None
            predicted_sales = None
        
        return {
            'prediction': prediction,
            'actual_sales': actual_sales,
            'predicted_sales': predicted_sales,
            'restaurant': restaurant_name,
            'date': date
        }
    
    def save_model(self, model_path=None, scaler_path=None):
        """Сохранение модели"""
        if not self.is_trained:
            logger.error("Модель не обучена")
            return False
        
        model_path = model_path or MODEL_PATH
        scaler_path = scaler_path or SCALER_PATH
        
        try:
            # Создаем директории если нужно
            os.makedirs(os.path.dirname(model_path), exist_ok=True)
            os.makedirs(os.path.dirname(scaler_path), exist_ok=True)
            
            # Сохраняем модель
            model_data = {
                'model': self.model,
                'feature_engineer': self.feature_engineer,
                'feature_names': self.feature_names,
                'model_type': self.model_type,
                'training_metrics': self.training_metrics,
                'trained_at': datetime.now().isoformat()
            }
            
            joblib.dump(model_data, model_path)
            logger.info(f"Модель сохранена в {model_path}")
            
            return True
            
        except Exception as e:
            logger.error(f"Ошибка сохранения модели: {e}")
            return False
    
    def load_model(self, model_path=None):
        """Загрузка модели"""
        model_path = model_path or MODEL_PATH
        
        try:
            model_data = joblib.load(model_path)
            
            self.model = model_data['model']
            self.feature_engineer = model_data['feature_engineer']
            self.feature_names = model_data['feature_names']
            self.model_type = model_data['model_type']
            self.training_metrics = model_data.get('training_metrics', {})
            self.is_trained = True
            
            logger.info(f"Модель загружена из {model_path}")
            logger.info(f"Дата обучения: {model_data.get('trained_at', 'неизвестно')}")
            
            return True
            
        except Exception as e:
            logger.error(f"Ошибка загрузки модели: {e}")
            return False
    
    def retrain(self, df, target_col='target'):
        """Переобучение модели на новых данных"""
        logger.info("Переобучение модели...")
        
        if self.is_trained:
            logger.info("Модель уже обучена, выполняю переобучение...")
        
        return self.train(df, target_col)
    
    def get_model_info(self):
        """Получение информации о модели"""
        if not self.is_trained:
            return {"status": "not_trained"}
        
        return {
            "status": "trained",
            "model_type": self.model_type,
            "feature_count": len(self.feature_names),
            "metrics": self.training_metrics
        }

def train_sales_model(start_date=None, end_date=None, model_type='random_forest', 
                     optimize_hyperparams=False, save_model=True):
    """Главная функция для обучения модели"""
    logger.info("Начинаю обучение модели анализа продаж")
    
    # Загружаем данные
    df = load_data_for_training(start_date, end_date)
    if df is None:
        logger.error("Не удалось загрузить данные")
        return None
    
    # Создаем и обучаем модель
    predictor = SalesPredictor(model_type)
    metrics = predictor.train(df, optimize_hyperparams=optimize_hyperparams)
    
    # Сохраняем модель
    if save_model:
        predictor.save_model()
    
    # Выводим важность признаков
    importance = predictor.get_feature_importance()
    if importance is not None:
        logger.info("Топ-10 важных признаков:")
        for _, row in importance.head(10).iterrows():
            logger.info(f"  {row['feature']}: {row['importance']:.4f}")
    
    return predictor

def load_trained_model(model_path=None):
    """Загрузка обученной модели"""
    predictor = SalesPredictor()
    if predictor.load_model(model_path):
        return predictor
    return None

if __name__ == "__main__":
    # Пример использования
    import sys
    sys.path.append('.')
    
    # Обучение модели
    predictor = train_sales_model(
        model_type='random_forest',
        optimize_hyperparams=False,
        save_model=True
    )
    
    if predictor:
        print("Модель успешно обучена!")
        print(f"Метрики: {predictor.training_metrics}")
    else:
        print("Ошибка обучения модели")