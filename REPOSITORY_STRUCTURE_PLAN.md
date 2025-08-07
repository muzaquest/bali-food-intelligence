# 📁 ПЛАН НОВОЙ СТРУКТУРЫ РЕПОЗИТОРИЯ

## 🎯 Цель: Организованная структура для разработчиков

```
muzaquest-bali-food-intelligence/
├── README.md                           # Главная документация
├── main.py                            # Главная точка входа
├── requirements.txt                   # Зависимости
├── database.sqlite                    # База данных
├── .env.example                      # Пример переменных окружения
│
├── 📁 src/                           # Исходный код
│   ├── analyzers/                    # Анализаторы продаж
│   │   ├── production_sales_analyzer.py    # 🎯 ОСНОВНОЙ анализатор
│   │   ├── proper_ml_detective_analysis.py # ML детективный анализ
│   │   └── __init__.py
│   │
│   ├── ml_models/                    # ML модели и системы
│   │   ├── ultimate_complete_ml_system.py  # Полная ML система
│   │   ├── ml_models.py                    # ML модели
│   │   └── __init__.py
│   │
│   ├── api/                         # API и веб интерфейсы
│   │   ├── ai_query_processor.py    # AI обработчик запросов
│   │   ├── web_app.py              # Веб приложение
│   │   └── __init__.py
│   │
│   └── utils/                       # Утилиты
│       ├── database_api_connector.py # Коннектор БД
│       ├── weather_intelligence.py  # Погодная аналитика
│       ├── health_check.py         # Проверка здоровья системы
│       └── __init__.py
│
├── 📁 data/                         # Данные
│   ├── comprehensive_holiday_analysis.json  # 164 праздника
│   ├── bali_restaurant_locations.json       # Координаты ресторанов
│   ├── README.md                            # Описание данных
│   └── tourism/                             # Туристические данные
│       ├── Kunjungan_Wisatawan_Bali_2024.xls
│       └── Table-1-7-Final-1-1.xls
│
├── 📁 docs/                         # Документация
│   ├── guides/                      # Руководства
│   │   ├── REPLIT_PRODUCTION_UPDATE.md
│   │   ├── QUICK_START_GUIDE.md
│   │   └── API_DATABASE_INTEGRATION.md
│   └── examples/                    # Примеры использования
│
├── 📁 scripts/                      # Скрипты
│   ├── setup_database.py
│   └── update_database.sh
│
└── 📁 tests/                        # Тесты
    └── test_analyzers.py
```

## 🗑️ Файлы для удаления (дубликаты/устаревшие):

### Устаревшие анализаторы:
- enhanced_sales_analyzer.py (заменен на production_sales_analyzer.py)
- smart_bad_days_analyzer.py
- enhanced_bad_days_analyzer.py
- fixed_bad_days_analyzer.py
- reverse_analysis_bad_days.py
- real_data_sales_analyzer.py
- analyze_specific_date.py

### Устаревшие ML файлы:
- ml_based_smart_analytics.py
- ml_data_analyzer.py
- complete_ml_analyzer.py
- smart_restaurant_analyzer.py
- ultimate_smart_analytics.py

### Устаревшие веб приложения:
- web_app_optimized.py (оставляем только web_app.py)
- simple_professional_analysis.py
- clean_professional_analysis.py

### Лишние документы:
- Все MD файлы с анализами конкретных дней
- Дубликаты инструкций

## ✅ Ключевые файлы для сохранения:

1. **production_sales_analyzer.py** - основной детективный анализатор
2. **database.sqlite** - база данных
3. **data/comprehensive_holiday_analysis.json** - 164 праздника
4. **data/bali_restaurant_locations.json** - координаты ресторанов
5. **Туристические данные** - все XLS файлы
6. **main.py** - точка входа