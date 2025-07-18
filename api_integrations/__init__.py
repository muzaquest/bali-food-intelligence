#!/usr/bin/env python3
"""
API Integrations Package для системы анализа продаж ресторанов Бали

Этот пакет содержит все необходимые интеграции для:
- Анализа погоды (OpenWeatherMap)
- Анализа праздников (Calendarific)
- Генерации AI объяснений (OpenAI)
- Подключения к базе данных клиента
- Мастер-интегратора для объединения всех сервисов
"""

__version__ = "1.0.0"
__author__ = "Restaurant Analytics Team"
__email__ = "support@analytics.com"

# Импорты основных классов
from .weather_service import WeatherService
from .holiday_service import HolidayService
from .ai_explainer import AIExplainer
from .client_database_api import ClientDatabaseAPI
from .master_integrator import MasterIntegrator

# Публичный API пакета
__all__ = [
    'WeatherService',
    'HolidayService',
    'AIExplainer',
    'ClientDatabaseAPI',
    'MasterIntegrator'
]

# Конфигурация по умолчанию
DEFAULT_CONFIG = {
    'weather_api_key': None,
    'holiday_api_key': None,
    'openai_api_key': None,
    'openai_model': 'gpt-4',
    'database_config': {
        'type': 'sqlite',
        'file_path': 'client_data.db'
    }
}

# Поддерживаемые регионы Бали
BALI_REGIONS = [
    'Seminyak',
    'Ubud', 
    'Canggu',
    'Denpasar',
    'Sanur',
    'Nusa Dua',
    'Jimbaran',
    'Kuta'
]

# Поддерживаемые типы баз данных
SUPPORTED_DB_TYPES = [
    'sqlite',
    'mysql',
    'postgresql',
    'api'
]

def create_integrator(config=None):
    """
    Упрощенная функция для создания MasterIntegrator
    
    Args:
        config (dict): Конфигурация API ключей
        
    Returns:
        MasterIntegrator: Готовый к использованию интегратор
    """
    if config is None:
        config = DEFAULT_CONFIG.copy()
    
    return MasterIntegrator(config)

def validate_config(config):
    """
    Валидация конфигурации API
    
    Args:
        config (dict): Конфигурация для проверки
        
    Returns:
        bool: True если конфигурация валидна
        
    Raises:
        ValueError: Если конфигурация невалидна
    """
    required_keys = ['weather_api_key', 'holiday_api_key', 'openai_api_key']
    
    for key in required_keys:
        if key not in config or not config[key]:
            raise ValueError(f"Отсутствует обязательный ключ: {key}")
    
    if 'database_config' not in config:
        raise ValueError("Отсутствует конфигурация базы данных")
    
    db_config = config['database_config']
    if 'type' not in db_config:
        raise ValueError("Не указан тип базы данных")
    
    if db_config['type'] not in SUPPORTED_DB_TYPES:
        raise ValueError(f"Неподдерживаемый тип БД: {db_config['type']}")
    
    return True

def get_version_info():
    """
    Возвращает информацию о версии пакета
    
    Returns:
        dict: Информация о версии и зависимостях
    """
    return {
        'version': __version__,
        'author': __author__,
        'email': __email__,
        'supported_regions': BALI_REGIONS,
        'supported_databases': SUPPORTED_DB_TYPES,
        'dependencies': {
            'requests': '>=2.31.0',
            'openai': '>=1.3.0',
            'pandas': '>=2.0.0',
            'numpy': '>=1.24.0',
            'scikit-learn': '>=1.3.0'
        }
    }

# Примеры использования
USAGE_EXAMPLES = {
    'basic_analysis': '''
from api_integrations import create_integrator

config = {
    'weather_api_key': 'YOUR_WEATHER_KEY',
    'holiday_api_key': 'YOUR_HOLIDAY_KEY',
    'openai_api_key': 'YOUR_OPENAI_KEY',
    'database_config': {
        'type': 'sqlite',
        'file_path': 'client_data.db'
    }
}

integrator = create_integrator(config)
analysis = integrator.analyze_restaurant_performance(1, '2024-01-15')
print(f"Отклонение продаж: {analysis['sales_analysis']['difference_percent']:+.1f}%")
''',
    
    'batch_analysis': '''
from api_integrations import MasterIntegrator

integrator = MasterIntegrator(config)
restaurant_ids = [1, 2, 3, 4, 5]
results = integrator.batch_analyze_restaurants(restaurant_ids, '2024-01-15')
print(f"Проанализировано: {results['successful_analyses']} ресторанов")
''',
    
    'health_check': '''
from api_integrations import create_integrator

integrator = create_integrator(config)
health = integrator.get_system_health()
print(f"Состояние системы: {health['overall_status']}")
'''
}

def print_usage_example(example_name='basic_analysis'):
    """
    Выводит пример использования
    
    Args:
        example_name (str): Название примера
    """
    if example_name in USAGE_EXAMPLES:
        print(f"Пример использования - {example_name}:")
        print(USAGE_EXAMPLES[example_name])
    else:
        print(f"Доступные примеры: {list(USAGE_EXAMPLES.keys())}")

# Информация о пакете для отладки
def debug_info():
    """Выводит отладочную информацию о пакете"""
    print("🔍 Отладочная информация пакета api_integrations:")
    print(f"Версия: {__version__}")
    print(f"Автор: {__author__}")
    print(f"Email: {__email__}")
    print(f"Поддерживаемые регионы: {', '.join(BALI_REGIONS)}")
    print(f"Поддерживаемые БД: {', '.join(SUPPORTED_DB_TYPES)}")
    print("✅ Пакет готов к использованию!")

if __name__ == "__main__":
    debug_info()
    print("\n" + "="*50)
    print_usage_example('basic_analysis')