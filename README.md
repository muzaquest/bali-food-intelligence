# 🍽️ Muzaquest Bali Food Intelligence

> **Интеллектуальная система анализа продаж ресторанов на Бали с ML и детективным анализом**

[![Python](https://img.shields.io/badge/Python-3.8%2B-blue)](https://python.org)
[![ML](https://img.shields.io/badge/ML-Ready-green)](https://scikit-learn.org)
[![Status](https://img.shields.io/badge/Status-Production-success)](https://replit.com)

## 🚀 Возможности

### 🕵️ **Детективный анализ продаж**
- **Автоматическое обнаружение** проблемных дней с падением продаж
- **Факторный анализ** с 12+ параметрами влияния
- **Погодная аналитика** через Open-Meteo API
- **164 праздника** (балийские, мусульманские, международные)
- **Временные показатели**: доставка, готовка, ожидание водителей
- **ROAS анализ** эффективности рекламы

### 🤖 **ML система**
- **67+ факторов** для машинного обучения
- **RandomForest + SHAP** для объяснимого AI
- **Прогнозирование** продаж и трендов
- **Геолокационная аналитика** 59 ресторанов

### 📊 **Данные**
- **База данных**: 13,000+ записей продаж
- **Платформы**: Grab, Gojek
- **Период**: 2024-2025
- **Туристические данные**: посещаемость Бали

## 📁 Структура проекта

```
muzaquest-bali-food-intelligence/
├── README.md                     # Главная документация
├── main.py                      # 🎯 Точка входа
├── database.sqlite              # База данных (4MB)
├── requirements.txt             # Зависимости
│
├── src/                         # 📦 Исходный код
│   ├── analyzers/              # 🔍 Анализаторы
│   │   └── production_sales_analyzer.py  # Основной детективный анализатор
│   ├── ml_models/              # 🤖 ML модели
│   ├── api/                    # 🌐 Веб интерфейсы
│   └── utils/                  # 🛠️ Утилиты
│
├── data/                       # 📊 Данные
│   ├── comprehensive_holiday_analysis.json  # 164 праздника
│   ├── bali_restaurant_locations.json       # GPS координаты
│   └── tourism/                             # Туристические данные
│
├── docs/                       # 📚 Документация
│   └── guides/                 # Руководства по развертыванию
│
└── tests/                      # 🧪 Тесты
```

## ⚡ Быстрый старт

### 1. Установка
```bash
git clone https://github.com/muzaquest/bali-food-intelligence.git
cd bali-food-intelligence
pip install -r requirements.txt
```

### 2. Запуск детективного анализа
```python
from src.analyzers import ProductionSalesAnalyzer

analyzer = ProductionSalesAnalyzer()
result = analyzer.analyze_restaurant_performance(
    restaurant_name="Ika Canggu",
    start_date="2025-04-01", 
    end_date="2025-05-31"
)

for line in result:
    print(line)
```

### 3. Веб интерфейс
```bash
python main.py
# Откройте http://localhost:5000
```

## 📈 Пример анализа

```
🔍 ДЕТЕКТИВНЫЙ АНАЛИЗ: Ika Canggu
📅 Период: 2025-04-01 - 2025-05-31
🚨 Найдено 5 проблемных дней

📉 ПРОБЛЕМНЫЙ ДЕНЬ #1: 2025-05-11
   💔 Падение продаж: 32.6%

   🔍 ФАКТОРЫ ВЛИЯНИЯ:
      1. 🚨 КРИТИЧНО: Grab offline 480% (8 часов!)
      2. 🌧️ Сильный дождь (26.7мм)
      3. ✅ Grab ROAS отличный: 19.5
      4. 📅 Слабый день недели (Sunday)

   🚨 КРИТИЧЕСКИЕ ПРОБЛЕМЫ:
      • Критическое выключение Grab

   📊 ОЦЕНКА: 🔴 КРИТИЧЕСКОЕ негативное влияние
```

## 🎯 Ключевые компоненты

### 🔍 ProductionSalesAnalyzer
**Главный анализатор** - `src/analyzers/production_sales_analyzer.py`

**Возможности:**
- ✅ Автоматический поиск проблемных дней
- ✅ Анализ временных отклонений (доставка +134%)
- ✅ Погодные условия (дождь, температура)
- ✅ 164 праздника с влиянием на продажи
- ✅ ROAS и эффективность рекламы
- ✅ Рейтинги и качество сервиса
- ✅ Close Time (выключение программ)

### 📊 Данные

**База данных:** `database.sqlite` (4MB)
- 13,000+ записей продаж
- Grab + Gojek статистика
- Временные метрики
- Рекламные данные

**Праздники:** `data/comprehensive_holiday_analysis.json`
- 164 праздника
- Балийские, мусульманские, международные
- Влияние на продажи в %

**Локации:** `data/bali_restaurant_locations.json`  
- 59 ресторанов
- GPS координаты для погоды
- Зоны: пляж, горы, центр, клифы

## 🚀 Развертывание на Replit

См. подробные инструкции: [`docs/guides/REPLIT_PRODUCTION_UPDATE.md`](docs/guides/REPLIT_PRODUCTION_UPDATE.md)

**Ключевые файлы для Replit:**
1. `src/analyzers/production_sales_analyzer.py` ← заменяет старый анализатор
2. `database.sqlite` ← база данных
3. `data/comprehensive_holiday_analysis.json` ← праздники
4. `data/bali_restaurant_locations.json` ← координаты

## 📚 Документация

- [`docs/guides/QUICK_START_GUIDE.md`](docs/guides/QUICK_START_GUIDE.md) - Быстрый старт
- [`docs/guides/REPLIT_PRODUCTION_UPDATE.md`](docs/guides/REPLIT_PRODUCTION_UPDATE.md) - Обновление Replit
- [`docs/guides/API_DATABASE_INTEGRATION.md`](docs/guides/API_DATABASE_INTEGRATION.md) - API интеграция

## 🤝 Команда

**Muzaquest Team** - Интеллектуальная аналитика для ресторанного бизнеса на Бали

## 📄 Лицензия

MIT License - см. [LICENSE](LICENSE)