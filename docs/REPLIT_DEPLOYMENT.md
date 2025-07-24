# 🚀 РАЗВЕРТЫВАНИЕ НА REPLIT

## 📋 **ПОШАГОВАЯ ИНСТРУКЦИЯ**

### 1️⃣ **Создание Replit проекта**
```bash
# Создайте новый Python Repl
# Название: bali-restaurant-analytics
# Язык: Python
```

### 2️⃣ **Загрузка файлов**
Загрузите следующие файлы в корень проекта:
- `main.py` - основная программа
- `ml_models.py` - ML модели  
- `proper_ml_detective_analysis.py` - детективный анализ
- `platform_breakdown_functions.py` - функции разделения данных
- `color_coding_system.py` - цветовое кодирование
- `database.sqlite` - база данных (4MB)
- `requirements.txt` - зависимости Python
- `Kunjungan_Wisatawan_Bali_2024.xls` - туристические данные 2024
- `Kunjungan_Wisatawan_Bali_2025.xls` - туристические данные 2025

### 3️⃣ **Настройка переменных окружения**
В Replit создайте переменные в разделе "Secrets":
```env
OPENAI_API_KEY=sk-proj-ваш_ключ_openai
CALENDARIFIC_API_KEY=ваш_ключ_calendarific  
GOOGLE_MAPS_API_KEY=ваш_ключ_google_maps
```

### 4️⃣ **Установка зависимостей**
```bash
pip install -r requirements.txt
```

### 5️⃣ **Запуск программы**
```bash
python main.py
```

## 🔧 **КОНФИГУРАЦИЯ REPLIT**

### `.replit` файл:
```toml
run = "python main.py"
language = "python3"
modules = ["python-3.11"]

[nix]
channel = "stable-22_11"

[deployment]
run = ["sh", "-c", "python main.py"]
```

### `pyproject.toml` (опционально):
```toml
[tool.poetry]
name = "bali-restaurant-analytics"
version = "1.0.0"
description = "Система аналитики ресторанов на Бали"

[tool.poetry.dependencies]
python = "^3.11"
pandas = "^2.0.0"
numpy = "^1.24.0"
requests = "^2.31.0"
python-dotenv = "^1.0.0"
scikit-learn = "^1.3.0"
```

## 📊 **ПРОВЕРКА РАБОТОСПОСОБНОСТИ**

### Тест API подключений:
```python
# В консоли Replit:
from scripts.test_apis import test_all_apis
test_all_apis()
```

### Тест базы данных:
```python
# В консоли Replit:
from scripts.analyze_database import main
main()
```

## 🎯 **ОЖИДАЕМЫЙ РЕЗУЛЬТАТ**

После успешного развертывания программа должна:
- ✅ Подключиться к базе данных SQLite
- ✅ Загрузить туристические данные
- ✅ Подключиться к API (OpenAI, Weather, Calendar, Maps)
- ✅ Генерировать полные отчеты с ML анализом
- ✅ Предоставлять детективный анализ влияющих факторов

## 🚨 **ВОЗМОЖНЫЕ ПРОБЛЕМЫ**

### Проблема: Не найдена база данных
**Решение:** 
```bash
python scripts/setup_database.py
```

### Проблема: Ошибки API
**Решение:** Проверьте переменные окружения в Secrets

### Проблема: Не хватает зависимостей
**Решение:**
```bash
pip install --upgrade -r requirements.txt
```

## 📱 **ИНТЕРФЕЙС REPLIT**

После запуска программа предложит:
1. **Выбор ресторана** из базы данных
2. **Период анализа** (по умолчанию - последние 30 дней)
3. **Тип отчета** (краткий/полный/детективный)

## 🎉 **ГОТОВО!**

Ваша система аналитики ресторанов готова к работе на Replit!