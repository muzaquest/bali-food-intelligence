# 🔄 ИНСТРУКЦИИ ДЛЯ РУЧНОЙ СИНХРОНИЗАЦИИ С GITHUB

## 🎯 **ЦЕЛЬ:** Подготовить main ветку для развертывания на Replit

## 📋 **ЧТО НУЖНО СДЕЛАТЬ В GITHUB:**

### 1. 🌿 **Очистить ветки (через веб-интерфейс)**
```
Удалить ветки:
- cursor/bc-3baa7209-3bba-4802-b8dc-592b881503de-db3f
- fixed-ml-detective-analysis

Оставить только: main
```

### 2. 📁 **Создать папки в main ветке**
```
Создать папки:
/scripts/
/data/
/docs/
/legacy/
```

### 3. 🔄 **Переместить файлы (КРИТИЧНО ДЛЯ REPLIT)**

#### 📊 **В папку data/**
```
Переместить:
- bali_restaurant_locations.json → data/
- real_coefficients.json → data/
- advanced_analysis.json → data/
- combined_tourist_correlations_2024_2025.json → data/
- location_factors.json → data/
- location_sales_analysis.json → data/
- ml_detective_results.json → data/
- proper_ml_results.json → data/
- realistic_factor_params.json → data/
- real_tourist_correlations.json → data/
- scientific_tourist_coefficients.json → data/
- weather_correlations.json → data/
```

#### 🔧 **В папку scripts/**
```
Переместить:
- analyze_*.py → scripts/
- extract_*.py → scripts/
- calculate_*.py → scripts/
- integrate_*.py → scripts/
- test_apis.py → scripts/
- setup_database.py → scripts/
- updated_detective_analysis.py → scripts/
```

#### 📖 **В папку docs/**
```
Переместить ВСЕ .md файлы КРОМЕ README.md:
- API_SETUP_GUIDE.md → docs/
- PLATFORM_SEPARATION_SUCCESS_REPORT.md → docs/
- SISTEMA_CAPABILITIES_REPORT.md → docs/
- (и все остальные .md файлы)

Переместить все .txt файлы:
- final_test.txt → docs/
- full_client_report.txt → docs/
- (и все остальные .txt файлы)
```

#### 🗂️ **В папку legacy/**
```
Переместить:
- main_replit.py → legacy/
- ml_full_database_analysis.py → legacy/
- install_ml.py → legacy/
```

### 4. 🗑️ **Удалить дубликаты**
```
Удалить:
- 1.-Data-Kunjungan-2024.xls (дубликат Kunjungan_Wisatawan_Bali_2024.xls)
- 1.-Data-Kunjungan-2025-3.xls (дубликат Kunjungan_Wisatawan_Bali_2025.xls)
```

### 5. ✅ **Оставить в корне ТОЛЬКО**
```
Финальная структура корня:
/
├── main.py                    # Основная программа
├── README.md                  # Инструкции
├── database.sqlite            # База данных (4MB)
├── .env                       # API ключи
├── requirements.txt           # Зависимости
├── requirements_ml.txt        # ML зависимости
├── LICENSE                    # Лицензия
├── .gitignore                 # Git правила
├── .replit                    # Replit конфиг
├── Kunjungan_Wisatawan_Bali_2024.xls  # Туристы 2024
├── Kunjungan_Wisatawan_Bali_2025.xls  # Туристы 2025
├── platform_breakdown_functions.py   # Новые функции
├── color_coding_system.py             # Цветовое кодирование
├── ml_models.py               # ML модели
├── proper_ml_detective_analysis.py   # Детективный анализ
├── scripts/                   # Скрипты
├── data/                      # Данные
├── docs/                      # Документация
├── legacy/                    # Устаревшие файлы
└── reports/                   # Папка для отчетов
```

## 🚀 **КРИТИЧНО ДЛЯ REPLIT:**

### ✅ **Обязательно проверить:**
1. **main.py** - в корне (Replit ищет его там)
2. **requirements.txt** - в корне (для установки зависимостей)
3. **database.sqlite** - в корне (путь в коде: './database.sqlite')
4. **.env** - в корне (API ключи)
5. **Туристические .xls файлы** - в корне (код ссылается на них)

### ⚠️ **ВАЖНО ДЛЯ ИМПОРТОВ:**
После перемещения файлов в папки, нужно обновить импорты в main.py:

```python
# БЫЛО:
from platform_breakdown_functions import ...

# СТАЛО (если переместили):
from platform_breakdown_functions import ...  # (оставляем в корне!)

# Для других файлов:
import sys
sys.path.append('./scripts')
sys.path.append('./data')
```

## 🎯 **АЛЬТЕРНАТИВНЫЙ БЫСТРЫЙ СПОСОБ:**

### 📦 **Создать новый репозиторий:**
1. Создать новый репозиторий muzaquest-analytics-clean
2. Загрузить только нужные файлы в правильной структуре
3. Перенести на него Replit

## 💡 **МОЯ РЕКОМЕНДАЦИЯ:**

**Давайте я подготовлю точный список команд для GitHub веб-интерфейса**, чтобы вы могли быстро все организовать для Replit!

**Какой вариант предпочитаете?**
1. 🔄 Ручная организация текущего репозитория
2. 📦 Создание нового чистого репозитория
3. ⏳ Ждать восстановления терминала