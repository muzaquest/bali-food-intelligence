"""
Конфигурация для ML-модели анализа продаж
"""
import os

# Пути к файлам
DATABASE_PATH = "client_data.db"
MODEL_PATH = "models/client_sales_model.joblib"
SCALER_PATH = "models/scaler.joblib"
RESULTS_PATH = "results/"

# Параметры модели
MODEL_PARAMS = {
    'n_estimators': 100,
    'max_depth': 10,
    'min_samples_split': 5,
    'min_samples_leaf': 2,
    'random_state': 42
}

# Параметры обучения
TEST_SIZE = 0.2
RANDOM_STATE = 42
CV_FOLDS = 5
MIN_R2_SCORE = 0.7

# Параметры признаков
ROLLING_WINDOW = 3
LAG_DAYS = 1

# Категории признаков
INTERNAL_FEATURES = [
    'total_sales', 'ads_sales', 'rating', 'roas', 
    'position', 'cancel_rate', 'ads_on'
]

EXTERNAL_FEATURES = [
    'rain_mm', 'temp_c', 'is_holiday', 'day_of_week'
]

TEMPORAL_FEATURES = [
    'lag_1_sales', 'rolling_mean_3', 'delta_sales_prev'
]

# Создание директорий
os.makedirs("models", exist_ok=True)
os.makedirs("results", exist_ok=True)
os.makedirs("data", exist_ok=True)