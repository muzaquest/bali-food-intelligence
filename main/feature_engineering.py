"""
Модуль для генерации признаков и подготовки данных для ML-модели
"""
import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler, LabelEncoder
from sklearn.model_selection import train_test_split
import logging
from datetime import datetime
from config import (
    INTERNAL_FEATURES, EXTERNAL_FEATURES, TEMPORAL_FEATURES,
    ROLLING_WINDOW, LAG_DAYS, TEST_SIZE, RANDOM_STATE
)

logger = logging.getLogger(__name__)

class FeatureEngineer:
    """Класс для генерации признаков"""
    
    def __init__(self):
        self.scaler = StandardScaler()
        self.label_encoders = {}
        self.feature_names = []
    
    def create_temporal_features(self, df):
        """Создание временных признаков"""
        logger.info("Создание временных признаков...")
        
        # Убеждаемся, что date - это datetime
        if not pd.api.types.is_datetime64_any_dtype(df['date']):
            df['date'] = pd.to_datetime(df['date'])
        
        # Сортируем по ресторану и дате
        df = df.sort_values(['restaurant_name', 'date']).reset_index(drop=True)
        
        # Лаговые признаки
        df['lag_1_sales'] = df.groupby('restaurant_name')['total_sales'].shift(1)
        df['lag_2_sales'] = df.groupby('restaurant_name')['total_sales'].shift(2)
        df['lag_7_sales'] = df.groupby('restaurant_name')['total_sales'].shift(7)
        
        # Скользящие средние (сдвинуты для избежания data leakage)
        df['rolling_mean_3'] = df.groupby('restaurant_name')['total_sales'].transform(
            lambda x: x.rolling(window=3, min_periods=1).mean()
        ).shift(1)  # Сдвигаем для избежания утечки
        
        df['rolling_mean_7'] = df.groupby('restaurant_name')['total_sales'].transform(
            lambda x: x.rolling(window=7, min_periods=1).mean()
        ).shift(1)  # Сдвигаем для избежания утечки
        
        df['rolling_std_7'] = df.groupby('restaurant_name')['total_sales'].transform(
            lambda x: x.rolling(window=7, min_periods=1).std()
        ).shift(1)  # Сдвигаем для избежания утечки
        
        # ИСПРАВЛЕНИЕ DATA LEAKAGE: Изменение продаж относительно предыдущего дня
        # Используем лаги вместо diff, чтобы избежать утечки данных из будущего
        df['delta_sales_prev'] = df.groupby('restaurant_name')['total_sales'].shift(1) - df.groupby('restaurant_name')['total_sales'].shift(2)
        
        # Процентное изменение (с лагом для избежания data leakage)
        df['pct_change_sales'] = df.groupby('restaurant_name')['total_sales'].shift(1).pct_change()
        
        # Тренд продаж (сравнение с недельным средним, без data leakage)
        df['sales_vs_week_avg'] = df['lag_1_sales'] / (df['rolling_mean_7'] + 1e-8)
        
        # Волатильность продаж
        df['sales_volatility'] = df['rolling_std_7'] / (df['rolling_mean_7'] + 1e-8)
        
        # Заполняем пропущенные значения
        temporal_cols = ['lag_1_sales', 'lag_2_sales', 'lag_7_sales', 
                        'rolling_mean_3', 'rolling_mean_7', 'rolling_std_7',
                        'delta_sales_prev', 'pct_change_sales', 'sales_vs_week_avg', 'sales_volatility']
        
        for col in temporal_cols:
            if col in df.columns:
                df[col] = df[col].fillna(0)
        
        logger.info(f"Создано {len(temporal_cols)} временных признаков")
        return df
    
    def create_seasonal_features(self, df):
        """Создание сезонных признаков"""
        logger.info("Создание сезонных признаков...")
        
        # Убеждаемся, что date - это datetime
        if not pd.api.types.is_datetime64_any_dtype(df['date']):
            df['date'] = pd.to_datetime(df['date'])
        
        # Временные признаки
        df['month'] = df['date'].dt.month
        df['quarter'] = df['date'].dt.quarter
        df['day_of_month'] = df['date'].dt.day
        df['day_of_week'] = df['date'].dt.dayofweek
        df['week_of_year'] = df['date'].dt.isocalendar().week
        
        # Циклические признаки
        df['month_sin'] = np.sin(2 * np.pi * df['month'] / 12)
        df['month_cos'] = np.cos(2 * np.pi * df['month'] / 12)
        df['day_of_week_sin'] = np.sin(2 * np.pi * df['day_of_week'] / 7)
        df['day_of_week_cos'] = np.cos(2 * np.pi * df['day_of_week'] / 7)
        
        # Признаки выходных и рабочих дней
        df['is_weekend'] = (df['day_of_week'] >= 5).astype(int)
        df['is_monday'] = (df['day_of_week'] == 0).astype(int)
        df['is_friday'] = (df['day_of_week'] == 4).astype(int)
        
        seasonal_features = [
            'month', 'quarter', 'day_of_month', 'week_of_year',
            'month_sin', 'month_cos', 'day_of_week_sin', 'day_of_week_cos',
            'is_weekend', 'is_monday', 'is_friday'
        ]
        
        logger.info(f"Создано {len(seasonal_features)} сезонных признаков")
        return df
    
    def create_restaurant_features(self, df):
        """Создание признаков на уровне ресторана"""
        logger.info("Создание признаков ресторана...")
        
        # Средние показатели по ресторану
        restaurant_stats = df.groupby('restaurant_name').agg({
            'total_sales': ['mean', 'std', 'median'],
            'rating': ['mean', 'std'],
            'cancel_rate': ['mean', 'std'],
            'ads_on': 'mean'
        })
        
        # Сглаживаем названия колонок
        restaurant_stats.columns = [f'restaurant_{col[0]}_{col[1]}' for col in restaurant_stats.columns]
        restaurant_stats = restaurant_stats.reset_index()
        
        # Объединяем с основными данными
        df = df.merge(restaurant_stats, on='restaurant_name', how='left')
        
        # Относительные показатели
        df['sales_vs_restaurant_mean'] = df['total_sales'] / (df['restaurant_total_sales_mean'] + 1e-8)
        df['rating_vs_restaurant_mean'] = df['rating'] / (df['restaurant_rating_mean'] + 1e-8)
        
        restaurant_features = [col for col in df.columns if col.startswith('restaurant_')]
        restaurant_features.extend(['sales_vs_restaurant_mean', 'rating_vs_restaurant_mean'])
        
        logger.info(f"Создано {len(restaurant_features)} признаков ресторана")
        return df
    
    def create_weather_features(self, df):
        """Создание признаков погоды"""
        logger.info("Создание признаков погоды...")
        
        # Категории дождя
        df['rain_category'] = pd.cut(df['rain_mm'], 
                                   bins=[-1, 0, 5, 15, float('inf')],
                                   labels=[0, 1, 2, 3])  # Используем числа вместо строк
        
        # Категории температуры
        df['temp_category'] = pd.cut(df['temp_c'],
                                   bins=[-float('inf'), 20, 25, 30, float('inf')],
                                   labels=[0, 1, 2, 3])  # Используем числа вместо строк
        
        # Экстремальные погодные условия
        df['extreme_weather'] = ((df['rain_mm'] > 20) | (df['temp_c'] < 15) | (df['temp_c'] > 35)).astype(int)
        
        # Комфортная погода
        df['comfortable_weather'] = ((df['rain_mm'] <= 2) & (df['temp_c'] >= 20) & (df['temp_c'] <= 30)).astype(int)
        
        # Заполняем пропущенные значения
        df['rain_category'] = df['rain_category'].fillna(0).astype(int)
        df['temp_category'] = df['temp_category'].fillna(1).astype(int)
        
        weather_features = [
            'rain_category', 'temp_category', 'extreme_weather', 'comfortable_weather'
        ]
        
        logger.info(f"Создано {len(weather_features)} признаков погоды")
        return df
    
    def create_target_variable(self, df):
        """Создание целевой переменной"""
        logger.info("Создание целевой переменной...")
        
        # Убеждаемся, что date - это datetime
        if not pd.api.types.is_datetime64_any_dtype(df['date']):
            df['date'] = pd.to_datetime(df['date'])
        
        # Сортируем по ресторану и дате
        df = df.sort_values(['restaurant_name', 'date']).reset_index(drop=True)
        
        # Создаем целевую переменную - изменение продаж
        df['target'] = df.groupby('restaurant_name')['total_sales'].diff()
        
        # Альтернативный вариант - процентное изменение
        df['target_pct'] = df.groupby('restaurant_name')['total_sales'].pct_change()
        
        # Заполняем пропущенные значения
        df['target'] = df['target'].fillna(0)
        df['target_pct'] = df['target_pct'].fillna(0)
        
        logger.info("Целевая переменная создана")
        return df
    
    def prepare_features(self, df):
        """Подготовка всех признаков"""
        logger.info("Начинаю подготовку признаков...")
        
        # Копируем DataFrame чтобы не изменять исходный
        df = df.copy()
        
        # Создаем временные признаки
        df = self.create_temporal_features(df)
        
        # Создаем сезонные признаки
        df = self.create_seasonal_features(df)
        
        # Создаем признаки ресторана
        df = self.create_restaurant_features(df)
        
        # Создаем признаки погоды
        df = self.create_weather_features(df)
        
        # Создаем целевую переменную
        df = self.create_target_variable(df)
        
        # Определяем итоговый список признаков
        feature_candidates = [
            # Временные признаки
            'lag_1_sales', 'lag_2_sales', 'lag_7_sales',
            'rolling_mean_3', 'rolling_mean_7', 'rolling_std_7',
            'delta_sales_prev', 'pct_change_sales', 'sales_vs_week_avg', 'sales_volatility',
            
            # Сезонные признаки
            'month', 'quarter', 'day_of_month', 'week_of_year',
            'month_sin', 'month_cos', 'day_of_week_sin', 'day_of_week_cos',
            'is_weekend', 'is_monday', 'is_friday',
            
            # Признаки ресторана
            'sales_vs_restaurant_mean', 'rating_vs_restaurant_mean',
            
            # Признаки погоды
            'rain_category', 'temp_category', 'extreme_weather', 'comfortable_weather',
            
            # Основные признаки
            'rating', 'cancel_rate', 'ads_on', 'is_holiday'
        ]
        
        # Добавляем признаки ресторана
        restaurant_features = [col for col in df.columns if col.startswith('restaurant_')]
        feature_candidates.extend(restaurant_features)
        
        # Фильтруем только существующие признаки
        available_features = [f for f in feature_candidates if f in df.columns]
        self.feature_names = available_features
        
        logger.info(f"Подготовлено {len(available_features)} признаков")
        return df
    
    def split_data(self, df, target_col='target'):
        """Разделение данных на обучающую и тестовую выборки"""
        logger.info("Разделение данных...")
        
        # Убираем записи без целевой переменной
        df_clean = df.dropna(subset=[target_col])
        
        # Выбираем признаки и целевую переменную
        X = df_clean[self.feature_names]
        y = df_clean[target_col]
        
        # Разделяем данные
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=TEST_SIZE, random_state=RANDOM_STATE, stratify=None
        )
        
        logger.info(f"Обучающая выборка: {X_train.shape[0]} записей")
        logger.info(f"Тестовая выборка: {X_test.shape[0]} записей")
        
        return X_train, X_test, y_train, y_test
    
    def scale_features(self, X_train, X_test):
        """Масштабирование признаков"""
        logger.info("Масштабирование признаков...")
        
        X_train_scaled = self.scaler.fit_transform(X_train)
        X_test_scaled = self.scaler.transform(X_test)
        
        # Преобразуем обратно в DataFrame
        X_train_scaled = pd.DataFrame(X_train_scaled, columns=X_train.columns, index=X_train.index)
        X_test_scaled = pd.DataFrame(X_test_scaled, columns=X_test.columns, index=X_test.index)
        
        return X_train_scaled, X_test_scaled
    
    def prepare_data_for_training(self, df, target_col='target', scale=True):
        """Полная подготовка данных для обучения"""
        logger.info("Полная подготовка данных для обучения...")
        
        # Подготавливаем признаки
        df_prepared = self.prepare_features(df)
        
        # Разделяем данные
        X_train, X_test, y_train, y_test = self.split_data(df_prepared, target_col)
        
        # Масштабируем признаки (опционально)
        if scale:
            X_train, X_test = self.scale_features(X_train, X_test)
        
        logger.info("Данные готовы для обучения")
        return X_train, X_test, y_train, y_test
    
    def prepare_single_sample(self, df, restaurant_name, date):
        """Подготовка одного образца для предсказания"""
        logger.info(f"Подготовка образца для {restaurant_name} на {date}")
        
        # Подготавливаем признаки
        df_prepared = self.prepare_features(df)
        
        # Убеждаемся, что date в правильном формате
        if isinstance(date, str):
            date = pd.to_datetime(date)
        
        # Фильтруем нужную запись
        sample = df_prepared[
            (df_prepared['restaurant_name'] == restaurant_name) & 
            (df_prepared['date'] == date)
        ]
        
        if sample.empty:
            logger.error(f"Не найдены данные для {restaurant_name} на {date}")
            return None
        
        # Выбираем признаки
        X_sample = sample[self.feature_names]
        
        # Масштабируем если нужно
        if hasattr(self, 'scaler') and self.scaler is not None:
            try:
                X_sample_scaled = self.scaler.transform(X_sample)
                X_sample = pd.DataFrame(
                    X_sample_scaled,
                    columns=X_sample.columns,
                    index=X_sample.index
                )
            except Exception as e:
                logger.warning(f"Не удалось масштабировать признаки: {e}")
        
        return X_sample

def prepare_data_for_model(df, target_col='target', scale=True):
    """Главная функция для подготовки данных"""
    engineer = FeatureEngineer()
    return engineer.prepare_data_for_training(df, target_col, scale)