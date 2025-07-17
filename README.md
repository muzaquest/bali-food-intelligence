# 🧠 ML-модель объяснимого анализа причин изменения продаж

Полноценная ML-модель для анализа и объяснения причин изменения продаж в ресторанах на платформах Grab/Gojek с использованием SHAP для интерпретации результатов.

## 🎯 Возможности

- **Объяснимый анализ**: Детальное объяснение причин роста или спада продаж
- **Многофакторный анализ**: Учет внутренних и внешних факторов (реклама, погода, праздники)
- **SHAP интерпретация**: Визуализация вклада каждого признака в предсказание
- **Пакетный анализ**: Анализ трендов за любой период
- **Сводные отчеты**: Рекомендации на основе анализа данных

## 📋 Требования

- Python 3.10+
- Зависимости из `requirements.txt`

## 🚀 Быстрый старт

### 1. Установка зависимостей

```bash
pip install -r requirements.txt
```

### 2. Обучение модели

```bash
python main.py train --model-type random_forest
```

### 3. Анализ конкретного случая

```bash
python main.py analyze --restaurant "Honeycomb" --date "2023-06-15"
```

## 📁 Структура проекта

```
project/
│
├── data_loader.py           # Загрузка данных из SQLite
├── feature_engineering.py   # Генерация признаков
├── model.py                 # Обучение ML-модели
├── explain.py               # SHAP-анализ и объяснения
├── main.py                  # CLI интерфейс
├── utils.py                 # Вспомогательные функции
├── config.py                # Конфигурация
├── demo.ipynb               # Демо в Jupyter
├── requirements.txt         # Зависимости
└── README.md               # Документация
```

## 🔧 Использование

### Команды CLI

#### Обучение модели
```bash
# Базовое обучение
python main.py train

# С оптимизацией гиперпараметров
python main.py train --optimize

# Gradient Boosting вместо Random Forest
python main.py train --model-type gradient_boosting

# Обучение на определенном периоде
python main.py train --start-date "2023-01-01" --end-date "2023-12-31"
```

#### Анализ изменения продаж
```bash
# Анализ конкретного дня
python main.py analyze --restaurant "Honeycomb" --date "2023-06-15"

# Пример вывода:
# ==================================================
# РЕЗУЛЬТАТ АНАЛИЗА
# ==================================================
# Ресторан: Honeycomb
# Дата: 2023-06-15
# Фактические продажи: 18,500,000
# Прогноз продаж: 14,300,000
# Изменение: -22.7%
# 
# Объяснение: Изменение продаж: -12% от реклама, -8% от дождь, -4% от рейтинг
# 
# Топ-3 фактора влияния:
#   • ads_on: -12.0%
#   • rain_mm: -8.0%
#   • rating: -4.0%
```

#### Пакетный анализ
```bash
# Анализ за период
python main.py batch --restaurant "Honeycomb" --start-date "2023-06-01" --end-date "2023-06-30"
```

#### Информация о модели
```bash
python main.py info

# Пример вывода:
# Статус модели: trained
# Тип модели: random_forest
# Количество признаков: 47
# 
# Метрики качества:
#   R² (тест): 0.8234
#   MSE (тест): 1234567.89
#   MAE (тест): 987.65
#   Кросс-валидация R²: 0.8156
```

#### Список ресторанов
```bash
python main.py list

# Пример вывода:
# Найдено 4 ресторанов:
#   • Burger King (365 записей)
#   • Honeycomb (365 записей)
#   • Pizza Palace (365 записей)
#   • Sushi Master (365 записей)
```

### Программный интерфейс

```python
from model import train_sales_model, load_trained_model
from explain import explain_sales_change

# Обучение модели
predictor = train_sales_model(model_type='random_forest')

# Анализ конкретного случая
result = explain_sales_change("Honeycomb", "2023-06-15")

# Результат в формате JSON
{
  "restaurant": "Honeycomb",
  "date": "2023-06-15", 
  "actual_sales": 18500000,
  "predicted_sales": 14300000,
  "change_percent": -22.7,
  "top_factors": {
    "ads_on": -0.12,
    "rain_mm": -0.08,
    "rating": -0.04
  },
  "explanation": "Изменение продаж: -12% от реклама, -8% от дождь, -4% от рейтинг"
}
```

## 📊 Признаки модели

### Внутренние факторы
- `total_sales` - общие продажи
- `ads_sales` - продажи от рекламы
- `rating` - рейтинг ресторана
- `roas` - возврат на рекламные расходы
- `position` - позиция в поиске
- `cancel_rate` - процент отмен
- `ads_on` - включена ли реклама

### Внешние факторы
- `rain_mm` - количество осадков
- `temp_c` - температура
- `is_holiday` - праздничный день
- `day_of_week` - день недели

### Временные признаки
- `lag_1_sales` - продажи предыдущего дня
- `rolling_mean_3` - скользящее среднее за 3 дня
- `delta_sales_prev` - изменение относительно предыдущего дня
- `sales_trend` - тренд продаж
- `rating_change` - изменение рейтинга

## 📈 Качество модели

- **R² score**: > 0.70 (требование ТЗ)
- **Кросс-валидация**: 5-fold CV
- **Метрики**: R², MSE, MAE
- **Интерпретируемость**: SHAP values для каждого предсказания

## 🔍 SHAP Анализ

Модель использует SHAP (SHapley Additive exPlanations) для объяснения предсказаний:

- **Waterfall plots** - пошаговое объяснение вклада признаков
- **Feature importance** - глобальная важность признаков
- **Bar plots** - топ факторов для конкретного случая
- **Детальные таблицы** - численные значения всех вкладов

## 📁 Результаты

Все результаты сохраняются в папке `results/`:
- JSON файлы с результатами анализа
- SHAP графики (PNG)
- Детальные таблицы (CSV)
- Пакетные отчеты

## 🛠️ Конфигурация

Основные параметры в `config.py`:
- Пути к файлам и моделям
- Параметры модели Random Forest
- Пороги качества
- Список признаков

## 🔄 Переобучение

Модель поддерживает переобучение на новых данных:
```python
from model import SalesPredictor

predictor = SalesPredictor()
predictor.load_model()
predictor.retrain(new_data)
```

## 🎯 Примеры использования

### 1. Ежедневный анализ
```bash
# Скрипт для ежедневного анализа
python main.py analyze --restaurant "Honeycomb" --date $(date +%Y-%m-%d)
```

### 2. Еженедельный отчет
```bash
# Анализ за прошлую неделю
python main.py batch --restaurant "Honeycomb" --start-date "2023-06-01" --end-date "2023-06-07"
```

### 3. Сравнение ресторанов
```python
restaurants = ["Honeycomb", "Pizza Palace", "Burger King"]
for restaurant in restaurants:
    result = explain_sales_change(restaurant, "2023-06-15")
    print(f"{restaurant}: {result['change_percent']:.1f}%")
```

## 🚧 Развитие

Заложена архитектура для:
- REST API через FastAPI
- Telegram/Notion интеграция
- Многозаведенческий анализ
- Автоматические отчеты

## 🐛 Решение проблем

### Модель не найдена
```bash
# Сначала обучите модель
python main.py train
```

### Данные не найдены
```bash
# Проверьте наличие базы данных
ls -la deliverybooster.db
```

### Низкое качество модели
```bash
# Попробуйте оптимизацию гиперпараметров
python main.py train --optimize
```

## 📝 Лицензия

MIT License

## 👥 Авторы

Разработано согласно техническому заданию для анализа продаж ресторанов.

---

**Готово к использованию!** 🚀

Модель обучена, протестирована и готова к анализу причин изменения продаж в реальном времени.