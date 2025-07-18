# 🚀 РУКОВОДСТВО ПО НАСТРОЙКЕ API ИНТЕГРАЦИЙ

## 📋 СПИСОК НЕОБХОДИМЫХ API КЛЮЧЕЙ

### 1. 🌤️ OpenWeatherMap API
- **Сайт**: https://openweathermap.org/api
- **Цена**: Бесплатно до 1000 запросов/день, $40/месяц за 100,000 запросов
- **Что получаем**: Текущая погода, прогноз на 5 дней, исторические данные

### 2. 📅 Calendarific API
- **Сайт**: https://calendarific.com/
- **Цена**: Бесплатно до 1000 запросов/месяц, $15/месяц за 10,000 запросов
- **Что получаем**: Праздники для любой страны, включая Индонезию

### 3. 🤖 OpenAI API
- **Сайт**: https://platform.openai.com/
- **Цена**: ~$20-200/месяц в зависимости от использования
- **Что получаем**: GPT-4 для генерации объяснений и рекомендаций

### 4. 📊 База данных клиента
- **Тип**: SQLite, MySQL, PostgreSQL или REST API
- **Цена**: Зависит от клиента
- **Что получаем**: Актуальные данные о продажах, заказах, рейтингах

---

## 🔧 ПОШАГОВАЯ НАСТРОЙКА

### Шаг 1: Получение API ключей

#### OpenWeatherMap
1. Зарегистрируйтесь на https://openweathermap.org/
2. Перейдите в раздел "API keys"
3. Создайте новый ключ для проекта
4. Скопируйте ключ (например: `a1b2c3d4e5f6g7h8i9j0k1l2m3n4o5p6`)

#### Calendarific
1. Зарегистрируйтесь на https://calendarific.com/
2. Перейдите в Dashboard
3. Скопируйте ваш API ключ
4. Сохраните ключ (например: `1234567890abcdef1234567890abcdef12345678`)

#### OpenAI
1. Зарегистрируйтесь на https://platform.openai.com/
2. Перейдите в раздел "API Keys"
3. Создайте новый секретный ключ
4. Скопируйте ключ (например: `sk-1234567890abcdef1234567890abcdef12345678`)

### Шаг 2: Настройка конфигурации

Создайте файл `config.json`:

```json
{
  "weather_api_key": "YOUR_OPENWEATHERMAP_API_KEY",
  "holiday_api_key": "YOUR_CALENDARIFIC_API_KEY", 
  "openai_api_key": "YOUR_OPENAI_API_KEY",
  "openai_model": "gpt-4",
  "database_config": {
    "type": "sqlite",
    "file_path": "client_data.db"
  }
}
```

### Шаг 3: Установка зависимостей

```bash
pip install requests openai pandas numpy scikit-learn
```

### Шаг 4: Тестирование интеграций

Создайте файл `test_apis.py`:

```python
#!/usr/bin/env python3
import json
from api_integrations.master_integrator import MasterIntegrator

# Загружаем конфигурацию
with open('config.json', 'r') as f:
    config = json.load(f)

# Создаем интегратор
integrator = MasterIntegrator(config)

# Проверяем состояние системы
health = integrator.get_system_health()
print(f"Состояние системы: {health['overall_status']}")

for service, status in health['services'].items():
    print(f"- {service}: {status['status']}")

# Тестируем анализ ресторана
try:
    analysis = integrator.analyze_restaurant_performance(1, '2024-01-15')
    print(f"\n✅ Анализ ресторана успешен")
    print(f"Отклонение продаж: {analysis['sales_analysis']['difference_percent']:+.1f}%")
except Exception as e:
    print(f"❌ Ошибка анализа: {e}")
```

---

## 🔄 КОНФИГУРАЦИИ ДЛЯ РАЗНЫХ БАЗ ДАННЫХ

### SQLite (локальная база)
```json
{
  "database_config": {
    "type": "sqlite",
    "file_path": "client_data.db"
  }
}
```

### MySQL
```json
{
  "database_config": {
    "type": "mysql",
    "host": "localhost",
    "port": 3306,
    "database": "restaurant_analytics",
    "username": "your_username",
    "password": "your_password"
  }
}
```

### PostgreSQL
```json
{
  "database_config": {
    "type": "postgresql",
    "host": "localhost",
    "port": 5432,
    "database": "restaurant_analytics",
    "username": "your_username",
    "password": "your_password"
  }
}
```

### REST API
```json
{
  "database_config": {
    "type": "api",
    "api_url": "https://api.client.com/v1",
    "api_key": "your_api_key"
  }
}
```

---

## 🧪 ТЕСТИРОВАНИЕ КАЖДОГО API

### Тест API погоды
```python
from api_integrations.weather_service import WeatherService

weather = WeatherService("YOUR_OPENWEATHERMAP_API_KEY")
impact = weather.get_weather_impact("Seminyak")
print(f"Влияние погоды: {impact['impact_percent']}%")
```

### Тест API праздников
```python
from api_integrations.holiday_service import HolidayService

holidays = HolidayService("YOUR_CALENDARIFIC_API_KEY")
impact = holidays.get_holiday_impact("2024-08-17")  # День независимости
print(f"Влияние праздников: {impact['impact_percent']}%")
```

### Тест OpenAI
```python
from api_integrations.ai_explainer import AIExplainer

ai = AIExplainer("YOUR_OPENAI_API_KEY")
test_data = {
    'restaurant_name': 'Test Restaurant',
    'sales_difference_percent': 15.0
}
result = ai.generate_sales_analysis(test_data)
print(f"AI объяснение: {result['explanation'][:100]}...")
```

---

## 📊 МОНИТОРИНГ И ЛИМИТЫ

### OpenWeatherMap
- **Бесплатный план**: 1000 запросов/день
- **Рекомендация**: Кэшируйте данные на 10-15 минут
- **Мониторинг**: Проверяйте заголовок `X-RateLimit-Remaining`

### Calendarific
- **Бесплатный план**: 1000 запросов/месяц
- **Рекомендация**: Кэшируйте праздники на месяц
- **Мониторинг**: Проверяйте поле `quota_remaining`

### OpenAI
- **Лимиты**: По токенам, ~$0.03 за 1K токенов GPT-4
- **Рекомендация**: Оптимизируйте промпты, используйте кэширование
- **Мониторинг**: Отслеживайте `usage.total_tokens`

---

## ⚡ ОПТИМИЗАЦИЯ ПРОИЗВОДИТЕЛЬНОСТИ

### 1. Кэширование данных
```python
import redis
from datetime import timedelta

# Кэш для погоды (15 минут)
redis_client = redis.Redis(host='localhost', port=6379, db=0)

def get_cached_weather(region):
    key = f"weather:{region}"
    cached = redis_client.get(key)
    if cached:
        return json.loads(cached)
    
    # Получаем новые данные
    data = weather_service.get_weather_impact(region)
    redis_client.setex(key, timedelta(minutes=15), json.dumps(data))
    return data
```

### 2. Асинхронные запросы
```python
import asyncio
import aiohttp

async def fetch_all_data_async(restaurant_id, date):
    async with aiohttp.ClientSession() as session:
        # Параллельные запросы к API
        tasks = [
            fetch_weather_async(session, region),
            fetch_holidays_async(session, date),
            fetch_db_data_async(session, restaurant_id, date)
        ]
        return await asyncio.gather(*tasks)
```

### 3. Батчевая обработка
```python
def analyze_multiple_restaurants(restaurant_ids, date):
    # Группируем по регионам для оптимизации API запросов
    regions = {}
    for r_id in restaurant_ids:
        info = db.get_restaurant_info(r_id)
        if info['region'] not in regions:
            regions[info['region']] = []
        regions[info['region']].append(r_id)
    
    # Получаем данные по регионам
    weather_data = {}
    for region in regions.keys():
        weather_data[region] = weather_service.get_weather_impact(region)
    
    # Анализируем каждый ресторан
    results = {}
    for r_id in restaurant_ids:
        info = db.get_restaurant_info(r_id)
        results[r_id] = analyze_with_cached_data(
            r_id, date, weather_data[info['region']]
        )
    
    return results
```

---

## 🔒 БЕЗОПАСНОСТЬ И ЛУЧШИЕ ПРАКТИКИ

### 1. Хранение ключей
```bash
# Используйте переменные окружения
export OPENWEATHER_API_KEY="your_key_here"
export CALENDARIFIC_API_KEY="your_key_here"
export OPENAI_API_KEY="your_key_here"
```

```python
import os
from dotenv import load_dotenv

load_dotenv()

config = {
    'weather_api_key': os.getenv('OPENWEATHER_API_KEY'),
    'holiday_api_key': os.getenv('CALENDARIFIC_API_KEY'),
    'openai_api_key': os.getenv('OPENAI_API_KEY')
}
```

### 2. Обработка ошибок
```python
def safe_api_call(func, *args, **kwargs):
    max_retries = 3
    for attempt in range(max_retries):
        try:
            return func(*args, **kwargs)
        except requests.exceptions.RequestException as e:
            if attempt == max_retries - 1:
                logger.error(f"API call failed after {max_retries} attempts: {e}")
                return None
            time.sleep(2 ** attempt)  # Экспоненциальная задержка
```

### 3. Логирование
```python
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('api_integrations.log'),
        logging.StreamHandler()
    ]
)
```

---

## 🚨 TROUBLESHOOTING

### Частые ошибки и решения

#### 1. "Invalid API key"
**Решение**: Проверьте правильность ключа и его активацию

#### 2. "Rate limit exceeded"
**Решение**: Добавьте задержки между запросами или увеличьте план

#### 3. "Connection timeout"
**Решение**: Увеличьте timeout в запросах
```python
response = requests.get(url, timeout=30)
```

#### 4. "Database connection failed"
**Решение**: Проверьте параметры подключения и доступность базы

#### 5. "OpenAI quota exceeded"
**Решение**: Проверьте баланс аккаунта и лимиты

### Диагностика проблем
```python
def diagnose_system():
    print("🔍 Диагностика системы...")
    
    # Проверка интернет-соединения
    try:
        requests.get("https://google.com", timeout=5)
        print("✅ Интернет-соединение: OK")
    except:
        print("❌ Интернет-соединение: FAILED")
    
    # Проверка каждого API
    integrator = MasterIntegrator(config)
    health = integrator.get_system_health()
    
    for service, status in health['services'].items():
        if status['status'] == 'healthy':
            print(f"✅ {service}: OK")
        else:
            print(f"❌ {service}: {status.get('error', 'FAILED')}")
```

---

## 💡 РЕКОМЕНДАЦИИ ПО ИСПОЛЬЗОВАНИЮ

### 1. Частота обновлений
- **Погода**: Каждые 15-30 минут
- **Праздники**: Раз в день
- **AI анализ**: По требованию
- **База данных**: В реальном времени

### 2. Оптимальные настройки
```json
{
  "cache_settings": {
    "weather_cache_minutes": 15,
    "holiday_cache_hours": 24,
    "ai_cache_minutes": 5
  },
  "retry_settings": {
    "max_retries": 3,
    "retry_delay": 2,
    "timeout_seconds": 30
  },
  "batch_settings": {
    "max_batch_size": 50,
    "concurrent_requests": 5
  }
}
```

### 3. Мониторинг затрат
```python
def track_api_costs():
    costs = {
        'openweather': 0.0,
        'calendarific': 0.0,
        'openai': 0.0
    }
    
    # Отслеживание запросов
    request_counts = {
        'openweather': 0,
        'calendarific': 0,
        'openai_tokens': 0
    }
    
    return costs, request_counts
```

---

## 🎯 ГОТОВЫЕ СЦЕНАРИИ ИСПОЛЬЗОВАНИЯ

### Ежедневный анализ
```python
def daily_analysis():
    today = datetime.now().strftime('%Y-%m-%d')
    restaurants = db.get_all_restaurants()
    
    for restaurant in restaurants:
        analysis = integrator.analyze_restaurant_performance(
            restaurant['id'], today
        )
        
        # Отправка отчета менеджеру
        send_daily_report(restaurant, analysis)
```

### Мониторинг критических изменений
```python
def critical_monitoring():
    restaurants = db.get_all_restaurants()
    
    for restaurant in restaurants:
        alert = integrator.monitor_critical_changes(
            restaurant['id'], threshold=20.0
        )
        
        if alert:
            send_alert_notification(alert)
```

### Еженедельные отчеты
```python
def weekly_reports():
    restaurants = db.get_all_restaurants()
    
    for restaurant in restaurants:
        report = integrator.generate_weekly_report(
            restaurant['id'], weeks=4
        )
        
        send_weekly_report(restaurant, report)
```

---

## 📈 МАСШТАБИРОВАНИЕ

### Для больших объемов данных
1. **Используйте очереди задач** (Celery, RQ)
2. **Кэшируйте результаты** (Redis, Memcached)
3. **Распределите нагрузку** (Load balancer)
4. **Мониторьте производительность** (Prometheus, Grafana)

### Архитектура для продакшена
```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Load Balancer │    │   API Gateway   │    │   Auth Service  │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │
         └───────────────────────┼───────────────────────┘
                                 │
                    ┌─────────────────────────┐
                    │   Master Integrator     │
                    │   (Main Application)    │
                    └─────────────────────────┘
                                 │
    ┌────────────────────────────┼────────────────────────────┐
    │                            │                            │
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Weather API   │    │   Holiday API   │    │   OpenAI API    │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                                 │
                    ┌─────────────────────────┐
                    │   Client Database       │
                    │   (PostgreSQL/MySQL)    │
                    └─────────────────────────┘
```

---

## ✅ ЧЕКЛИСТ ГОТОВНОСТИ

- [ ] Получены все API ключи
- [ ] Настроена конфигурация
- [ ] Установлены зависимости
- [ ] Протестированы все API
- [ ] Настроено кэширование
- [ ] Добавлено логирование
- [ ] Настроена обработка ошибок
- [ ] Проведено нагрузочное тестирование
- [ ] Настроен мониторинг
- [ ] Подготовлена документация

**После выполнения всех пунктов система готова к продакшену! 🚀**