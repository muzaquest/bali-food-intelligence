# 🧹 ПЛАН ОЧИСТКИ GITHUB РЕПОЗИТОРИЯ

## ✅ **КРИТИЧЕСКИ ВАЖНЫЕ ФАЙЛЫ (НЕ УДАЛЯТЬ!):**

### 📊 **Основные данные:**
- `database.sqlite` (4MB) - основная база данных с 59 ресторанами
- `Kunjungan_Wisatawan_Bali_2024.xls` (73KB) - туристические данные 2024
- `Kunjungan_Wisatawan_Bali_2025.xls` (83KB) - туристические данные 2025
- `1.-Data-Kunjungan-2024.xls` (73KB) - дубликат туристических данных 2024
- `1.-Data-Kunjungan-2025-3.xls` (83KB) - дубликат туристических данных 2025

### 🔑 **Конфигурация:**
- `.env` - API ключи (OpenAI, Weather, Calendar, Google Maps)
- `.gitignore` - правила игнорирования
- `requirements.txt` - зависимости Python
- `requirements_ml.txt` - ML зависимости

### 🚀 **Основной код:**
- `main.py` (172KB) - главная программа
- `platform_breakdown_functions.py` - функции разделения данных
- `color_coding_system.py` - цветовое кодирование
- `ml_models.py` - ML модели
- `proper_ml_detective_analysis.py` - детективный анализ

### 📋 **Документация:**
- `README.md` (56KB) - основные инструкции
- `LICENSE` - лицензия

## 🗂️ **ФАЙЛЫ ДЛЯ ОРГАНИЗАЦИИ В ПАПКИ:**

### 📁 **scripts/** (вспомогательные скрипты)
- `analyze_*.py` - скрипты анализа
- `extract_*.py` - скрипты извлечения данных
- `calculate_*.py` - скрипты расчетов
- `integrate_*.py` - скрипты интеграции
- `test_*.py` - тестовые скрипты
- `setup_*.py` - скрипты настройки

### 📁 **data/** (данные и конфигурации)
- `*.json` - конфигурационные файлы
- `bali_restaurant_locations.json` - локации ресторанов
- `real_coefficients.json` - коэффициенты
- `advanced_analysis.json` - результаты анализа

### 📁 **docs/** (документация)
- Все `*.md` файлы кроме `README.md`
- `*.txt` отчеты

### 📁 **legacy/** (устаревшие файлы)
- `main_replit.py` - старая версия для Replit
- `ml_full_database_analysis.py` - старый анализ

## 🗑️ **ФАЙЛЫ К УДАЛЕНИЮ:**

### 📊 **Дублированные отчеты:**
- `final_test.txt`, `test_report.txt`, `updated_report.txt`
- `full_client_report.txt`, `market_full_report.txt`
- Множественные `*_REPORT.md` с похожим содержанием

### 🔄 **Промежуточные результаты:**
- `proper_ml_analysis_report.txt`
- Старые `ml_detective_results.json`

## 🌿 **ВЕТКИ К УДАЛЕНИЮ:**
- `cursor/bc-3baa7209-3bba-4802-b8dc-592b881503de-db3f` - временная ветка
- `fixed-ml-detective-analysis` - исправления уже в main

## 📋 **ИТОГОВАЯ СТРУКТУРА:**

```
/
├── main.py                 # Главная программа
├── README.md              # Инструкции
├── database.sqlite        # База данных
├── .env                   # API ключи
├── requirements.txt       # Зависимости
├── LICENSE                # Лицензия
├── Kunjungan_Wisatawan_Bali_2024.xls  # Туристы 2024
├── Kunjungan_Wisatawan_Bali_2025.xls  # Туристы 2025
├── platform_breakdown_functions.py   # Новые функции
├── color_coding_system.py             # Цветовое кодирование
├── scripts/               # Вспомогательные скрипты
├── data/                  # Конфигурации и данные
├── docs/                  # Документация
└── reports/               # Папка для отчетов (пустая)
```

## 🎯 **СЛЕДУЮЩИЕ ШАГИ:**
1. Создать папки
2. Переместить файлы
3. Удалить дубликаты
4. Удалить лишние ветки
5. Обновить .gitignore
6. Коммит и пуш в main