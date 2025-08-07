# 🌐 ИНТЕГРАЦИЯ API БАЗЫ ДАННЫХ

## 📋 **ОБЗОР**

Вместо замены локального файла `database.sqlite`, можно подключиться к обновляемой базе данных через **REST API**. Это элегантное и современное решение!

---

## 🎯 **ПРЕИМУЩЕСТВА API ПОДХОДА**

### ✅ **VS Замена файлов:**
- **Никаких backup'ов** - данные всегда актуальные
- **Автоматические обновления** - без перезапуска приложения  
- **Масштабируемость** - можете подключить множество клиентов
- **Безопасность** - аутентификация через API ключи
- **Мониторинг** - логи всех запросов в реальном времени

### ✅ **VS Символические ссылки:**
- **Работает везде** - Windows, Linux, Mac, облачные сервисы
- **Контроль доступа** - API ключи, rate limiting
- **Кеширование** - умное кеширование для производительности

### ✅ **VS Docker volumes:**
- **Простота** - не требует Docker инфраструктуры
- **Гибкость** - можно работать из любого окружения
- **Отладка** - легко тестировать и мониторить

---

## 🚀 **АРХИТЕКТУРА РЕШЕНИЯ**

```
📊 MUZAQUEST Analytics App
           ↓
🌐 API Коннектор (database_api_connector.py)
           ↓ 
🔗 REST API (https://api.muzaquest.com/v1)
           ↓
🗄️ Обновляемая база данных (живая)
```

### 🔄 **Кеширование для производительности:**
```
1. Запрос данных → Проверка кеша (5 мин TTL)
2. Если кеш валиден → Возврат из кеша ⚡
3. Если кеш устарел → API запрос → Обновление кеша → Возврат данных
```

---

## 🔧 **ИНТЕГРАЦИЯ С СУЩЕСТВУЮЩИМ КОДОМ**

### 1️⃣ **Минимальная интеграция (рекомендуется)**

#### **Файл: `config.py` (создать)**
```python
# Настройки подключения к API БД
USE_API_DATABASE = True  # Переключатель: True = API, False = локальный файл
API_DATABASE_URL = "https://api.muzaquest.com/v1"
API_DATABASE_KEY = "your-api-key-here"  # Из .env файла
CACHE_TTL = 300  # 5 минут кеша
```

#### **Файл: `main.py` (изменить начало)**
```python
# Добавить в начало main.py
import config
from database_api_connector import setup_api_database, DatabaseAdapter

# Инициализация базы данных
if config.USE_API_DATABASE:
    print("🌐 Используем API базу данных...")
    api_connector = setup_api_database(config.API_DATABASE_URL, config.API_DATABASE_KEY)
    if api_connector:
        db_adapter = DatabaseAdapter(api_connector)
        print("✅ API база подключена")
    else:
        print("❌ Fallback на локальную базу")
        config.USE_API_DATABASE = False

def get_restaurant_data_full(restaurant_name, start_date=None, end_date=None):
    """ОБНОВЛЕННАЯ функция для работы с API"""
    if config.USE_API_DATABASE:
        # Используем API
        try:
            # Получаем статистику через API
            grab_stats = api_connector.get_restaurant_stats(
                restaurant_name, start_date, end_date, 'grab'
            )
            gojek_stats = api_connector.get_restaurant_stats(
                restaurant_name, start_date, end_date, 'gojek'
            )
            
            # Конвертируем в pandas DataFrame
            grab_df = pd.DataFrame(grab_stats)
            gojek_df = pd.DataFrame(gojek_stats)
            
            # Объединяем данные (как раньше)
            # ... существующая логика объединения
            return combined_data
            
        except Exception as e:
            print(f"⚠️ Ошибка API, fallback на локальную БД: {e}")
            config.USE_API_DATABASE = False
    
    # Fallback на локальную БД (существующий код)
    conn = sqlite3.connect("database.sqlite")
    # ... существующий код
```

### 2️⃣ **Обновление веб-интерфейса**

#### **Файл: `web_app_optimized.py` (изменить)**
```python
# Добавить в начало файла
import config
from database_api_connector import setup_api_database

# Обновить функцию load_restaurants
@st.cache_data(ttl=300)
def load_restaurants():
    """Загрузка списка ресторанов (API или локальная БД)"""
    if config.USE_API_DATABASE:
        try:
            restaurants_data = api_connector.get_restaurants()
            return [r['name'] for r in restaurants_data]
        except Exception as e:
            st.warning(f"⚠️ API недоступен, используем локальную БД: {e}")
            # Fallback на локальную БД
    
    # Существующий код для локальной БД
    try:
        conn = sqlite3.connect('database.sqlite')
        query = """
        SELECT DISTINCT restaurant_name 
        FROM grab_stats 
        UNION 
        SELECT DISTINCT restaurant_name 
        FROM gojek_stats 
        ORDER BY restaurant_name
        """
        restaurants = pd.read_sql_query(query, conn)
        conn.close()
        return restaurants['restaurant_name'].tolist()
    except Exception as e:
        st.error(f"Ошибка загрузки ресторанов: {e}")
        return []
```

### 3️⃣ **Обновление AI агента**

#### **Файл: `ai_query_processor.py` (изменить)**
```python
# Добавить в __init__
import config
from database_api_connector import setup_api_database

class AIQueryProcessor:
    def __init__(self):
        # Инициализация API коннектора
        if config.USE_API_DATABASE:
            self.api_connector = setup_api_database(
                config.API_DATABASE_URL, 
                config.API_DATABASE_KEY
            )
            self.use_api = bool(self.api_connector)
        else:
            self.use_api = False
        
        # Остальная инициализация...

    def _get_all_restaurant_names(self):
        """Получение списка всех ресторанов (API или локальная БД)"""
        if self.use_api:
            try:
                restaurants_data = self.api_connector.get_restaurants()
                return [r['name'] for r in restaurants_data]
            except Exception as e:
                print(f"⚠️ API недоступен: {e}")
                # Fallback на локальную БД
        
        # Существующий код для локальной БД
        try:
            conn = sqlite3.connect(self.db_path)
            query = "SELECT DISTINCT name FROM restaurants ORDER BY name"
            restaurants = pd.read_sql_query(query, conn)
            conn.close()
            return restaurants['name'].tolist()
        except:
            return []
```

---

## 🔧 **НАСТРОЙКА API НА СТОРОНЕ ПОСТАВЩИКА ДАННЫХ**

### 📋 **Требуемые API endpoints:**

#### **1. GET /health**
```json
{
  "status": "ok",
  "version": "1.0.0",
  "restaurants_count": 59,
  "last_update": "2025-07-26T10:30:00Z"
}
```

#### **2. GET /restaurants**
```json
{
  "restaurants": [
    {
      "id": 1,
      "name": "Ika Kero",
      "location": "Canggu",
      "created_at": "2024-01-01"
    },
    ...
  ]
}
```

#### **3. GET /restaurant-stats**
```
Параметры:
- restaurant: "Ika Kero"
- platform: "grab"|"gojek"|"all"
- start_date: "2025-01-01" (опционально)
- end_date: "2025-07-26" (опционально)
```

```json
{
  "restaurant": "Ika Kero",
  "platform": "grab",
  "period": {"start": "2025-01-01", "end": "2025-07-26"},
  "stats": [
    {
      "date": "2025-01-01",
      "sales": 1250000,
      "orders": 45,
      "rating": 4.2,
      "new_customers": 15,
      "repeat_customers": 30,
      // ... все остальные поля из БД
    },
    ...
  ]
}
```

#### **4. GET /market-overview**
```json
{
  "period": {"start": "2025-01-01", "end": "2025-07-26"},
  "total_sales": 125000000,
  "total_restaurants": 59,
  "avg_sales_per_restaurant": 2118644,
  "top_restaurants": [...],
  "platforms": {
    "grab": {"sales": 75000000, "share": 0.6},
    "gojek": {"sales": 50000000, "share": 0.4}
  }
}
```

#### **5. GET /restaurant-location**
```json
{
  "restaurant": "Ika Kero",
  "location": {
    "latitude": -8.6488,
    "longitude": 115.1373,
    "location": "Canggu",
    "area": "Badung",
    "zone": "South"
  }
}
```

---

## 🔒 **БЕЗОПАСНОСТЬ И АУТЕНТИФИКАЦИЯ**

### 🔑 **API Key через .env файл:**
```bash
# .env
API_DATABASE_URL=https://api.muzaquest.com/v1
API_DATABASE_KEY=muzaquest_live_api_key_2025_secure
```

### 🛡️ **Заголовки запросов:**
```
Authorization: Bearer muzaquest_live_api_key_2025_secure
Content-Type: application/json
User-Agent: MuzaquestAnalytics/1.0
```

---

## ⚡ **ПРОИЗВОДИТЕЛЬНОСТЬ И КЕШИРОВАНИЕ**

### 📊 **Умное кеширование:**
- **Список ресторанов**: кеш 30 минут (редко меняется)
- **Статистика**: кеш 5 минут (обновляется часто)
- **Координаты**: кеш 24 часа (практически не меняются)
- **Рыночный обзор**: кеш 10 минут

### 🚀 **Оптимизации:**
```python
# Batch запросы для множественных ресторанов
GET /restaurant-stats?restaurants=Ika_Kero,Prana,Accent&platform=all

# Сжатие ответов
Accept-Encoding: gzip, deflate

# Условные запросы
If-Modified-Since: Thu, 26 Jul 2025 10:30:00 GMT
```

---

## 🔧 **РАЗВЕРТЫВАНИЕ И МОНИТОРИНГ**

### 📊 **Мониторинг API:**
```python
# Добавить в health_check.py
def check_api_database():
    """Проверка API базы данных"""
    if not config.USE_API_DATABASE:
        return True
    
    try:
        return api_connector.test_connection()
    except Exception as e:
        print(f"❌ API база недоступна: {e}")
        return False
```

### 🔄 **Graceful degradation:**
```python
# Автоматический fallback на локальную БД
if api_request_fails:
    print("⚠️ API недоступен, переключаемся на локальную БД")
    config.USE_API_DATABASE = False
    # Система продолжает работать с последней локальной копией
```

---

## 🎯 **МИГРАЦИЯ НА API: ПОШАГОВЫЙ ПЛАН**

### 📅 **Фаза 1: Подготовка (1 день)**
1. Клиент разворачивает REST API для своей БД
2. Предоставляет URL и API ключ
3. Тестируем подключение

### 📅 **Фаза 2: Интеграция (1 день)**
1. Добавляем `database_api_connector.py` в проект
2. Создаем `config.py` с настройками
3. Обновляем 3-4 ключевые функции

### 📅 **Фаза 3: Тестирование (1 день)**
1. Тестируем все функции с API
2. Проверяем fallback на локальную БД
3. Валидируем производительность

### 📅 **Фаза 4: Production (1 день)**
1. Переключаем `USE_API_DATABASE = True`
2. Развертываем на Replit/сервере
3. Мониторим работу

---

## ✅ **ИТОГОВЫЕ ПРЕИМУЩЕСТВА ДЛЯ КЛИЕНТА**

### 🎯 **Для бизнеса:**
- **Real-time данные** - всегда актуальная информация
- **Никаких простоев** - обновления без перезапуска
- **Масштабируемость** - можно подключить много аналитиков
- **Безопасность** - контроль доступа через API ключи

### 🔧 **Для разработки:**
- **Минимальные изменения** кода (3-4 функции)
- **Graceful degradation** - fallback на локальную БД
- **Простое тестирование** - легко переключаться между режимами
- **Мониторинг** - полная видимость всех запросов

### 🚀 **Для развертывания:**
- **Работает везде** - Replit, AWS, Heroku, локально
- **Нет зависимостей** от файловой системы
- **Автоматические backup'ы** не нужны
- **Hotswap данных** - мгновенные обновления

---

## 🎉 **ЗАКЛЮЧЕНИЕ**

**API подход - это современное, элегантное и масштабируемое решение!**

Вместо копирования файлов и перезапусков, клиент получает **real-time подключение** к живой базе данных с автоматическим кешированием для производительности.

**Система готова к интеграции! Нужны только URL и API ключ от клиента.** 🌐✨