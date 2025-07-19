# MuzaQuest Business Intelligence System

Система бизнес-аналитики для ресторанов с машинным обучением.

## 🚀 Быстрый старт

### 1. Установка зависимостей
```bash
cd main/
pip install -r requirements.txt
```

### 2. Использование

#### Анализ ресторана за период
```bash
python3 main.py deep --restaurant "Ika Canggu" --start-date "2025-04-01" --end-date "2025-06-30"
```

#### Рыночный анализ
```bash
python3 main.py market --start-date "2025-04-01" --end-date "2025-06-30"
```

#### Краткий отчет для руководства
```bash
python3 main.py summary --restaurant "Ika Canggu"
```

#### Список всех ресторанов
```bash
python3 main.py list
```

## 📊 Доступные команды

- `analyze` - Анализ конкретного случая
- `deep` - Глубокий анализ с аномалиями и YoY сравнениями
- `summary` - Краткий отчет для руководства
- `market` - Комплексный рыночный анализ
- `weekly` - Недельный отчет
- `causal` - Причинно-следственный анализ
- `test` - Тестирование гипотез
- `train` - Обучение модели
- `info` - Информация о модели
- `list` - Список ресторанов

## 🔧 Переобучение модели

```bash
python3 quick_retrain.py
```

## 📁 Структура

- `main.py` - Основной интерфейс
- `business_intelligence_system.py` - Система аналитики
- `model.py` - ML модель
- `feature_engineering.py` - Создание признаков
- `data/database.sqlite` - База данных (59 ресторанов)
- `models/client_sales_model.joblib` - Обученная модель