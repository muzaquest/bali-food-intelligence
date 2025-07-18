# Следующие шаги для запуска системы анализа продаж

## 1. ДАННЫЕ РЕСТОРАНОВ 📊

### Основная база данных
Нужна SQLite база с таблицами:

```sql
-- Рестораны
CREATE TABLE restaurants (
    id INTEGER PRIMARY KEY,
    name TEXT,
    region TEXT,  -- Seminyak, Ubud, Canggu, Denpasar, Sanur
    cuisine_type TEXT,
    rating REAL,
    avg_delivery_time INTEGER,
    commission_rate REAL
);

-- Статистика Grab
CREATE TABLE grab_stats (
    id INTEGER PRIMARY KEY,
    restaurant_id INTEGER,
    date DATE,
    sales REAL,
    orders INTEGER,
    avg_order_value REAL,
    ads_spend REAL,
    ads_enabled BOOLEAN,
    FOREIGN KEY (restaurant_id) REFERENCES restaurants(id)
);

-- Статистика Gojek
CREATE TABLE gojek_stats (
    id INTEGER PRIMARY KEY,
    restaurant_id INTEGER,
    date DATE,
    sales REAL,
    orders INTEGER,
    avg_order_value REAL,
    ads_spend REAL,
    ads_enabled BOOLEAN,
    FOREIGN KEY (restaurant_id) REFERENCES restaurants(id)
);
```

### Какие данные нужны от вас:
- **Исторические данные продаж** (минимум 6 месяцев)
- **Список ресторанов** с регионами
- **Данные о рекламе** (включена/выключена, бюджет)
- **Рейтинги и время доставки**

## 2. API КЛЮЧИ 🔑

### Обязательные API:
1. **OpenWeatherMap** (погода)
   - Регистрация: https://openweathermap.org/api
   - Нужен: `OPENWEATHER_API_KEY`
   - Стоимость: Бесплатно до 1000 запросов/день

2. **Calendarific** (праздники)
   - Регистрация: https://calendarific.com/
   - Нужен: `CALENDARIFIC_API_KEY`
   - Стоимость: Бесплатно до 1000 запросов/месяц

### Опциональные API:
3. **Grab API** (если доступно)
   - Для получения данных о водителях
   - Нужен: `GRAB_API_KEY`

4. **Gojek API** (если доступно)
   - Для получения данных о водителях
   - Нужен: `GOJEK_API_KEY`

## 3. КОНФИГУРАЦИЯ СИСТЕМЫ ⚙️

### Создать файл `.env`:
```env
# Погода
OPENWEATHER_API_KEY=your_openweather_key_here
OPENWEATHER_CITY_ID=1650357  # Denpasar, Bali

# Праздники
CALENDARIFIC_API_KEY=your_calendarific_key_here
CALENDARIFIC_COUNTRY=ID  # Indonesia

# Опциональные
GRAB_API_KEY=your_grab_key_here
GOJEK_API_KEY=your_gojek_key_here

# База данных
DATABASE_PATH=./data/sales_data.db

# Модель
MODEL_PATH=./models/
```

## 4. НАСТРОЙКА РЕГИОНОВ 🗺️

### Информация о ваших ресторанах:
Нужно знать для каждого региона:
- **Процент туристов** vs местных жителей
- **Пиковые часы заказов**
- **Особенности доставки** (пробки, дороги)
- **Конкуренция** в регионе

### Пример заполнения:
```python
REGIONS = {
    'Seminyak': {
        'tourist_ratio': 0.8,  # 80% туристов
        'peak_hours': [12, 13, 19, 20],
        'delivery_difficulty': 'high',  # пробки
        'competition_level': 'very_high'
    },
    'Ubud': {
        'tourist_ratio': 0.7,
        'peak_hours': [12, 13, 18, 19],
        'delivery_difficulty': 'medium',
        'competition_level': 'high'
    }
    # ... остальные регионы
}
```

## 5. ЗАПУСК СИСТЕМЫ 🚀

### Пошаговый запуск:

1. **Установка зависимостей:**
```bash
pip install -r requirements.txt
```

2. **Подготовка данных:**
```bash
# Загрузка исторических данных
python -c "from data_loader import load_data; load_data()"
```

3. **Обучение модели:**
```bash
python main.py train
```

4. **Тестовый анализ:**
```bash
python main.py analyze --restaurant_id 1 --date 2024-01-15
```

## 6. АВТОМАТИЗАЦИЯ 🤖

### Ежедневный анализ:
```bash
# Добавить в crontab
0 9 * * * cd /path/to/project && python main.py batch --days 1
```

### Telegram бот (опционально):
- Нужен `TELEGRAM_BOT_TOKEN`
- Уведомления о резких изменениях продаж
- Ежедневные отчеты

## 7. ЧТО НУЖНО ОТ ВАС ПРЯМО СЕЙЧАС 📋

### Критически важно:
1. **Данные продаж** - CSV файлы или доступ к базе
2. **Список ресторанов** с регионами
3. **OpenWeatherMap API ключ** (бесплатный)
4. **Calendarific API ключ** (бесплатный)

### Можно настроить позже:
- Grab/Gojek API (если доступно)
- Telegram бот
- Автоматические уведомления

## 8. ПРИМЕРЫ ИСПОЛЬЗОВАНИЯ 💡

### Ежедневный анализ:
```python
from main import analyze_restaurant
result = analyze_restaurant(restaurant_id=1, date='2024-01-15')
print(result['explanation'])
```

### Batch анализ:
```python
from main import batch_analyze
results = batch_analyze(start_date='2024-01-01', end_date='2024-01-31')
```

## 9. ОЖИДАЕМЫЕ РЕЗУЛЬТАТЫ 📈

После настройки система будет:
- **Предсказывать изменения продаж** с точностью 75-85%
- **Объяснять причины** падения/роста
- **Давать рекомендации** по улучшению
- **Отслеживать влияние** погоды, праздников, рекламы

## 10. ПОДДЕРЖКА И РАЗВИТИЕ 🔧

### Дальнейшие улучшения:
- Интеграция с POS-системами
- Прогнозирование спроса
- A/B тестирование рекламы
- Анализ конкурентов

---

**Готовы начать? Пришлите данные продаж и получите API ключи!**