# 📚 Полное руководство пользователя системы анализа ресторанов

## 🚀 Быстрый старт

### Установка и запуск
```bash
# Установка зависимостей
pip install -r requirements.txt

# Запуск системы
python business_intelligence_system.py
```

### Первый анализ
```python
from business_intelligence_system import MarketIntelligenceEngine

# Создание экземпляра системы
engine = MarketIntelligenceEngine()

# Быстрый анализ ресторана
result = engine.generate_quick_analysis("Ika Canggu")
print(result)
```

## 📊 Типы отчётов

### 1. Быстрый анализ (`generate_quick_analysis`)
**Назначение:** Получение основных метрик и трендов за последние 30 дней

**Команда:**
```python
result = engine.generate_quick_analysis(
    restaurant_name="Ika Canggu",
    days=30  # Опционально, по умолчанию 30
)
```

**Содержание отчёта:**
- Средние продажи и заказы
- Тренды роста/снижения
- Основные метрики эффективности
- Топ дни по продажам

### 2. Глубокий анализ (`generate_deep_analysis`)
**Назначение:** Детальное исследование с прогнозами и рекомендациями

**Команда:**
```python
result = engine.generate_deep_analysis(
    restaurant_name="Ika Canggu",
    start_date="2025-03-01",
    end_date="2025-04-01"
)
```

**Содержание отчёта:**
- Все метрики быстрого анализа
- Прогноз продаж на следующие 7 дней
- Анализ влияния погоды
- Календарные эффекты (выходные, праздники)
- Конкретные рекомендации

### 3. Сравнительный анализ (`compare_restaurants`)
**Назначение:** Сопоставление показателей двух ресторанов

**Команда:**
```python
result = engine.compare_restaurants(
    restaurant1="Ika Canggu",
    restaurant2="Bambu Indah",
    start_date="2025-03-01",
    end_date="2025-04-01"
)
```

**Содержание отчёта:**
- Сравнение ключевых метрик
- Выявление сильных и слабых сторон
- Рекомендации по улучшению

### 4. Прогнозирование (`predict_sales`)
**Назначение:** Прогноз продаж на указанные даты

**Команда:**
```python
predictions = engine.predict_sales(
    restaurant_name="Ika Canggu",
    target_dates=["2025-04-15", "2025-04-16", "2025-04-17"]
)
```

## 📏 Объяснение метрик

### Основные метрики продаж
- **`total_sales`** - Общая выручка в рупиях
- **`total_orders`** - Количество заказов
- **`avg_order_value`** - Средний чек (total_sales / total_orders)
- **`sales_growth`** - Рост продаж в % по сравнению с предыдущим периодом

### Операционные метрики
- **`delivery_time_minutes`** - Среднее время доставки в минутах
- **`cancellation_rate`** - Процент отменённых заказов
- **`rating`** - Средняя оценка ресторана (1-5 звёзд)
- **`discount_amount`** - Сумма скидок в рупиях

### Клиентские метрики (расширенные)
- **`customer_retention_rate`** - Процент возвращающихся клиентов
- **`new_customer_ratio`** - Доля новых клиентов
- **`lifetime_value`** - Ожидаемая стоимость клиента
- **`churn_probability`** - Вероятность оттока клиентов

### Качественные метрики (расширенные)
- **`food_quality_score`** - Оценка качества еды (на основе отзывов)
- **`service_quality_score`** - Оценка качества обслуживания
- **`presentation_score`** - Оценка подачи блюд
- **`freshness_score`** - Оценка свежести продуктов

### Маркетинговые метрики (расширенные)
- **`promotion_effectiveness`** - Эффективность акций
- **`viral_coefficient`** - Коэффициент виральности (рекомендации)
- **`social_media_mentions`** - Упоминания в соцсетях
- **`brand_awareness_score`** - Показатель узнаваемости бренда

### Погодные влияния
- **`weather_impact`** - Влияние погоды на продажи (-1 до 1)
- **`temperature_optimal`** - Оптимальная температура для продаж
- **`rain_impact`** - Влияние дождя на заказы

### Календарные эффекты
- **`weekend_boost`** - Прирост продаж в выходные (%)
- **`holiday_impact`** - Влияние праздников на продажи
- **`month_seasonality`** - Сезонный фактор месяца

## 🎯 Примеры использования

### Пример 1: Анализ падения продаж
```python
# Проверяем ресторан с проблемами
engine = MarketIntelligenceEngine()

# Глубокий анализ за последний месяц
result = engine.generate_deep_analysis(
    restaurant_name="Problem Restaurant",
    start_date="2025-03-01",
    end_date="2025-04-01"
)

# Смотрим ключевые проблемы
print("Проблемы:")
for issue in result.get('issues', []):
    print(f"- {issue}")

print("Рекомендации:")
for rec in result.get('recommendations', []):
    print(f"- {rec}")
```

### Пример 2: Оптимизация меню
```python
# Анализ эффективности блюд
result = engine.generate_deep_analysis("Ika Canggu")

# Проверяем метрики качества
food_score = result['analysis']['food_quality_score']
presentation = result['analysis']['presentation_score']

if food_score < 4.0:
    print("⚠️ Рекомендация: Улучшить качество блюд")
if presentation < 4.0:
    print("⚠️ Рекомендация: Улучшить подачу блюд")
```

### Пример 3: Планирование промо-акций
```python
# Прогноз на неделю для планирования акций
predictions = engine.predict_sales(
    restaurant_name="Ika Canggu",
    target_dates=[
        "2025-04-15", "2025-04-16", "2025-04-17", 
        "2025-04-18", "2025-04-19", "2025-04-20", "2025-04-21"
    ]
)

# Находим дни с низкими прогнозами для акций
for date, sales in predictions.items():
    if sales < 5000000:  # Менее 5 млн рупий
        print(f"💡 {date}: Рекомендуется промо-акция (прогноз: {sales:,.0f})")
```

### Пример 4: Мониторинг конкурентов
```python
# Сравнение с конкурентом
comparison = engine.compare_restaurants(
    restaurant1="Ika Canggu",
    restaurant2="Competitor Restaurant"
)

# Анализ преимуществ
strengths = comparison.get('restaurant1_strengths', [])
print("Наши преимущества:")
for strength in strengths:
    print(f"✅ {strength}")

weaknesses = comparison.get('restaurant1_weaknesses', [])
print("Области для улучшения:")
for weakness in weaknesses:
    print(f"📈 {weakness}")
```

## 🔧 Расширенные возможности

### Интеграция с собственной базой данных
```python
# Подключение к клиентской базе
from data_integration import load_data_with_all_features

# Загрузка всех доступных данных
df = load_data_with_all_features('path/to/client/database.sqlite')
print(f"Загружено полей: {len(df.columns)}")
```

### Кастомные периоды анализа
```python
# Анализ за конкретный квартал
result = engine.generate_deep_analysis(
    restaurant_name="Ika Canggu",
    start_date="2025-01-01",
    end_date="2025-03-31"
)
```

### Экспорт результатов
```python
import json

# Сохранение анализа в файл
result = engine.generate_deep_analysis("Ika Canggu")
with open('analysis_report.json', 'w', encoding='utf-8') as f:
    json.dump(result, f, ensure_ascii=False, indent=2)
```

## ⚡ Советы по производительности

### Для быстрых проверок
- Используйте `generate_quick_analysis()` для ежедневного мониторинга
- Ограничивайте периоды анализа (не более 3 месяцев для глубокого анализа)

### Для детальных исследований
- `generate_deep_analysis()` идеален для недельных/месячных отчётов
- Используйте прогнозы для планирования на 1-2 недели вперёд

### Оптимизация памяти
- Система автоматически загружает только необходимые данные
- Максимальное потребление памяти: ~150 MB
- Время полного анализа: 30-60 секунд

## 🚨 Устранение проблем

### Частые ошибки

**"Restaurant not found"**
```python
# Проверьте точное название ресторана
available = engine.get_available_restaurants()
print("Доступные рестораны:", available)
```

**Медленная работа**
```python
# Проверьте производительность
from performance_check import main as performance_check
performance_check()
```

**Отсутствуют данные за период**
```python
# Проверьте доступные даты
data_range = engine.get_data_range("Ika Canggu")
print(f"Данные доступны с {data_range['start']} по {data_range['end']}")
```

## 📈 Интерпретация результатов

### Понимание трендов
- **Рост > 5%**: Отличная динамика
- **Рост 0-5%**: Стабильный рост  
- **Рост < 0%**: Требует внимания

### Оценка качества
- **4.5-5.0**: Превосходно
- **4.0-4.4**: Хорошо
- **3.5-3.9**: Удовлетворительно
- **< 3.5**: Требует улучшения

### Прогнозы
- **Точность**: 85-90% для 7-дневных прогнозов
- **Доверительные интервалы**: ±15% от прогноза
- **Обновление**: Рекомендуется каждые 2-3 дня

## 🔄 Автоматизация

### Ежедневные отчёты
```python
import schedule
import time

def daily_report():
    engine = MarketIntelligenceEngine()
    result = engine.generate_quick_analysis("Ika Canggu")
    # Отправка по email или сохранение в файл
    
schedule.every().day.at("09:00").do(daily_report)

while True:
    schedule.run_pending()
    time.sleep(60)
```

### Недельные аналитические сводки
```python
def weekly_deep_analysis():
    engine = MarketIntelligenceEngine()
    from datetime import datetime, timedelta
    
    end_date = datetime.now()
    start_date = end_date - timedelta(days=7)
    
    result = engine.generate_deep_analysis(
        restaurant_name="Ika Canggu",
        start_date=start_date.strftime("%Y-%m-%d"),
        end_date=end_date.strftime("%Y-%m-%d")
    )
    
    # Обработка результатов
```

## 📞 Поддержка

### Логирование
Система автоматически записывает лог-файлы в папку `logs/`

### Диагностика
```python
# Проверка состояния системы
from business_intelligence_system import MarketIntelligenceEngine

engine = MarketIntelligenceEngine()
status = engine.system_health_check()
print(status)
```

### Обновления данных
Система автоматически адаптируется к новым полям в базе данных клиента. При обновлении базы данных (каждые 1-2 дня) система будет использовать все доступные поля для максимально точного анализа.

---

*Версия документации: 2.0*  
*Последнее обновление: Январь 2025*