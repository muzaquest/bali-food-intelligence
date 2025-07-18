"""
Конфигурация для анализа продаж на Бали
Учитывает местные особенности: мусульманские праздники, сезоны дождей, туристические потоки
"""

import os
from datetime import datetime, timedelta

# Базовые настройки
DATABASE_PATH = "bali_deliverybooster.db"
MODEL_PATH = "models/bali_sales_model.joblib"
RESULTS_PATH = "results/bali/"

# Параметры модели (оптимизированы для Бали)
MODEL_PARAMS = {
    'n_estimators': 150,  # больше деревьев для сложных зависимостей
    'max_depth': 12,
    'min_samples_split': 3,
    'min_samples_leaf': 2,
    'random_state': 42
}

# Временные зоны и локализация
TIMEZONE = 'Asia/Makassar'  # Бали UTC+8
LOCALE = 'id_ID'  # Индонезия

# Специфичные для Бали признаки
BALI_SPECIFIC_FEATURES = [
    'is_muslim_holiday',      # Мусульманские праздники
    'is_hindu_holiday',       # Индуистские праздники (Nyepi, Galungan)
    'is_tourist_season',      # Туристический сезон
    'is_rainy_season',        # Сезон дождей
    'driver_availability',    # Доступность водителей
    'tourist_density',        # Плотность туристов
    'local_event',           # Местные события/фестивали
    'rice_harvest_season',   # Сезон сбора риса (влияет на местных)
]

# Погодные факторы для Бали
WEATHER_FACTORS = {
    'heavy_rain_threshold': 20,      # мм/час - сильный дождь
    'extreme_rain_threshold': 50,    # мм/час - экстремальный дождь
    'high_humidity_threshold': 85,   # % влажности
    'hot_temperature_threshold': 32, # °C
    'comfortable_temp_range': (24, 30),  # °C
    'monsoon_months': [12, 1, 2, 3],     # Месяцы муссонов
}

# Праздники и события Бали
BALI_HOLIDAYS = {
    'muslim_holidays': [
        'ramadan_start', 'ramadan_end', 'eid_al_fitr', 'eid_al_adha',
        'mawlid_al_nabi', 'isra_miraj'
    ],
    'hindu_holidays': [
        'nyepi',           # День тишины
        'galungan',        # Праздник добра
        'kuningan',        # Завершение Galungan
        'saraswati',       # День знаний
        'pagerwesi',       # День железного забора
    ],
    'national_holidays': [
        'independence_day', 'pancasila_day', 'kartini_day'
    ],
    'tourist_events': [
        'bali_arts_festival', 'kite_festival', 'bali_spirit_festival'
    ]
}

# Туристические сезоны
TOURIST_SEASONS = {
    'high_season': [6, 7, 8, 12, 1],      # Июнь-Август, Декабрь-Январь
    'shoulder_season': [4, 5, 9, 10],      # Апрель-Май, Сентябрь-Октябрь
    'low_season': [2, 3, 11],              # Февраль-Март, Ноябрь
}

# Районы Бали с разной спецификой
BALI_REGIONS = {
    'denpasar': {
        'type': 'city',
        'tourist_ratio': 0.3,
        'local_ratio': 0.7,
        'business_hours': (6, 23),
    },
    'ubud': {
        'type': 'cultural',
        'tourist_ratio': 0.8,
        'local_ratio': 0.2,
        'business_hours': (7, 22),
    },
    'seminyak': {
        'type': 'beach_luxury',
        'tourist_ratio': 0.9,
        'local_ratio': 0.1,
        'business_hours': (8, 24),
    },
    'canggu': {
        'type': 'beach_surf',
        'tourist_ratio': 0.8,
        'local_ratio': 0.2,
        'business_hours': (7, 23),
    },
    'sanur': {
        'type': 'beach_family',
        'tourist_ratio': 0.7,
        'local_ratio': 0.3,
        'business_hours': (7, 22),
    }
}

# Факторы доступности водителей
DRIVER_FACTORS = {
    'prayer_times': [5, 12, 15, 18, 19],  # Время молитв (снижение активности)
    'ramadan_impact': 0.7,                # Коэффициент активности в Рамадан
    'friday_prayer_impact': 0.8,          # Пятничная молитва
    'holiday_impact': 0.6,                # Праздничные дни
    'rain_impact': 0.5,                   # Сильный дождь
}

# API конфигурация
API_CONFIG = {
    'weather': {
        'openweather_api_key': os.getenv('OPENWEATHER_API_KEY'),
        'locations': [
            {'name': 'Denpasar', 'lat': -8.6500, 'lon': 115.2167},
            {'name': 'Ubud', 'lat': -8.5069, 'lon': 115.2625},
            {'name': 'Seminyak', 'lat': -8.6919, 'lon': 115.1717},
            {'name': 'Canggu', 'lat': -8.6482, 'lon': 115.1376},
            {'name': 'Sanur', 'lat': -8.6881, 'lon': 115.2608},
        ]
    },
    'holidays': {
        'calendarific_api_key': os.getenv('CALENDARIFIC_API_KEY'),
        'country': 'ID',  # Индонезия
        'timezone': TIMEZONE,
    },
    'grab': {
        'api_key': os.getenv('GRAB_API_KEY'),
        'base_url': 'https://partner-api.grab.com',
    },
    'gojek': {
        'api_key': os.getenv('GOJEK_API_KEY'),
        'base_url': 'https://api.gojek.com',
    }
}

# Веса важности факторов (для начальной настройки)
FEATURE_WEIGHTS = {
    # Временные факторы
    'lag_1_sales': 0.25,
    'rolling_mean_7': 0.20,
    'sales_trend': 0.15,
    
    # Погодные факторы
    'rain_mm': 0.12,
    'humidity': 0.08,
    'temperature': 0.05,
    
    # Праздники и события
    'is_muslim_holiday': 0.10,
    'is_hindu_holiday': 0.08,
    'is_tourist_season': 0.07,
    
    # Бизнес-факторы
    'ads_on': 0.15,
    'rating': 0.12,
    'driver_availability': 0.10,
    
    # Локальные факторы
    'tourist_density': 0.06,
    'local_event': 0.04,
}

# Пороги для алертов
ALERT_THRESHOLDS = {
    'sales_drop': -0.20,        # Падение продаж на 20%
    'rating_drop': -0.1,        # Падение рейтинга на 0.1
    'driver_shortage': 0.3,     # Нехватка водителей на 30%
    'extreme_weather': 50,      # Экстремальные осадки
    'holiday_impact': -0.15,    # Влияние праздников
}

# Рекомендации по оптимизации
OPTIMIZATION_RULES = {
    'rainy_day': {
        'condition': 'rain_mm > 15',
        'actions': [
            'increase_delivery_fee',
            'activate_rain_promo',
            'boost_driver_incentives'
        ]
    },
    'muslim_holiday': {
        'condition': 'is_muslim_holiday == 1',
        'actions': [
            'reduce_pork_menu',
            'increase_halal_options',
            'adjust_operating_hours'
        ]
    },
    'tourist_season': {
        'condition': 'is_tourist_season == 1',
        'actions': [
            'increase_english_menu',
            'boost_tourist_area_ads',
            'extend_operating_hours'
        ]
    },
    'low_rating': {
        'condition': 'rating < 4.3',
        'actions': [
            'pause_ads_temporarily',
            'focus_on_quality_improvement',
            'increase_customer_service'
        ]
    }
}

# Создание директорий
os.makedirs("models", exist_ok=True)
os.makedirs("results/bali", exist_ok=True)
os.makedirs("data/bali", exist_ok=True)
os.makedirs("logs", exist_ok=True)