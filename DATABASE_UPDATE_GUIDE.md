# 🔄 ИНСТРУКЦИЯ ПО ПОДКЛЮЧЕНИЮ ОБНОВЛЯЕМОЙ БАЗЫ ДАННЫХ

## 📋 **КРАТКОЕ РЕЗЮМЕ**

Система MUZAQUEST Analytics готова работать с обновляемой базой данных **БЕЗ ИЗМЕНЕНИЙ КОДА**. Нужно только правильно организовать процесс обновления базы.

---

## 🎯 **СХЕМА ПОДКЛЮЧЕНИЯ ОБНОВЛЯЕМОЙ БД**

### 1️⃣ **ТЕКУЩАЯ СТРУКТУРА ФАЙЛОВ**
```
📁 muzaquest-project/
├── 📄 database.sqlite          ← ОСНОВНАЯ БАЗА (заменяется)
├── 📁 data/
│   ├── 📄 bali_restaurant_locations.json  ← Координаты ресторанов
│   ├── 📄 scientific_tourist_coefficients.json
│   └── 📄 combined_tourist_correlations_2024_2025.json
├── 📄 main.py                  ← Основной анализ
├── 📄 web_app_optimized.py     ← Веб-интерфейс
├── 📄 ai_query_processor.py    ← AI агент
└── 📄 .env                     ← API ключи
```

### 2️⃣ **ГДЕ И КАК РАЗМЕЩАТЬ ОБНОВЛЯЕМУЮ БД**

#### ✅ **ПРАВИЛЬНО:**
```bash
# База должна быть в КОРНЕВОЙ папке проекта
/workspace/database.sqlite          ← ТУТ!
```

#### ❌ **НЕПРАВИЛЬНО:**
```bash
/workspace/data/database.sqlite     ← НЕ ТУТ!
/workspace/db/database.sqlite       ← НЕ ТУТ!
/workspace/databases/database.sqlite ← НЕ ТУТ!
```

---

## 🚀 **ВАРИАНТЫ ПОДКЛЮЧЕНИЯ ОБНОВЛЯЕМОЙ БД**

### 🔄 **ВАРИАНТ 1: ЗАМЕНА ФАЙЛА (РЕКОМЕНДУЕТСЯ)**

#### **Для ручного обновления:**
```bash
# 1. Останавливаем приложение (если запущено)
pkill -f "python.*web_app_optimized.py"

# 2. Создаем backup старой базы
cp database.sqlite database_backup_$(date +%Y%m%d_%H%M%S).sqlite

# 3. Заменяем на новую базу
cp /path/to/new/database.sqlite ./database.sqlite

# 4. Запускаем приложение
python web_app_optimized.py
```

#### **Для автоматического обновления (скрипт):**
```bash
#!/bin/bash
# update_database.sh

# Конфигурация
PROJECT_DIR="/path/to/muzaquest-project"
NEW_DB_PATH="/path/to/updated/database.sqlite"
BACKUP_DIR="$PROJECT_DIR/backups"

# Создаем папку для бэкапов
mkdir -p "$BACKUP_DIR"

# Останавливаем приложение
cd "$PROJECT_DIR"
pkill -f "python.*web_app_optimized.py"

# Создаем backup с временной меткой
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
cp database.sqlite "$BACKUP_DIR/database_backup_$TIMESTAMP.sqlite"

# Заменяем базу
cp "$NEW_DB_PATH" ./database.sqlite

# Проверяем целостность новой базы
sqlite3 database.sqlite "SELECT COUNT(*) FROM restaurants;" > /dev/null

if [ $? -eq 0 ]; then
    echo "✅ База данных успешно обновлена: $TIMESTAMP"
    
    # Перезапускаем приложение
    nohup python web_app_optimized.py > app.log 2>&1 &
    echo "✅ Приложение перезапущено"
else
    echo "❌ Ошибка в новой базе! Восстанавливаем backup..."
    cp "$BACKUP_DIR/database_backup_$TIMESTAMP.sqlite" ./database.sqlite
    nohup python web_app_optimized.py > app.log 2>&1 &
fi
```

### 🔗 **ВАРИАНТ 2: СИМВОЛИЧЕСКАЯ ССЫЛКА**

```bash
# 1. Переименовываем текущую базу
mv database.sqlite database_static.sqlite

# 2. Создаем символическую ссылку на обновляемую базу
ln -s /path/to/live/database.sqlite ./database.sqlite

# Теперь система будет автоматически видеть изменения
# в /path/to/live/database.sqlite
```

### 🐳 **ВАРИАНТ 3: DOCKER VOLUME (ДЛЯ ПРОДАКШЕНА)**

```dockerfile
# Dockerfile
FROM python:3.9-slim

WORKDIR /app
COPY . .

# База будет монтироваться как volume
VOLUME ["/app/database.sqlite"]

RUN pip install -r requirements.txt

CMD ["python", "web_app_optimized.py"]
```

```bash
# Запуск с внешней базой
docker run -v /host/path/database.sqlite:/app/database.sqlite muzaquest-app
```

---

## 📊 **ОБНОВЛЕНИЕ НОВЫХ РЕСТОРАНОВ**

### 🗺️ **Добавление координат новых ресторанов:**

Когда добавляются новые рестораны, нужно обновить файл с координатами:

```json
# data/bali_restaurant_locations.json
{
  "last_updated": "2025-07-26",
  "total_restaurants": 62,
  "restaurants": [
    {
      "name": "Новый Ресторан",
      "latitude": -8.4095,
      "longitude": 115.1889,
      "location": "Canggu",
      "area": "Badung", 
      "zone": "South"
    }
  ]
}
```

#### **Автоматический скрипт для добавления координат:**
```python
# add_restaurant_location.py
import json
import sys

def add_restaurant_location(name, lat, lon, location, area, zone):
    """Добавляет координаты нового ресторана"""
    
    with open('data/bali_restaurant_locations.json', 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    # Проверяем что ресторан еще не добавлен
    for restaurant in data['restaurants']:
        if restaurant['name'].lower() == name.lower():
            print(f"❌ Ресторан '{name}' уже существует!")
            return False
    
    # Добавляем новый ресторан
    new_restaurant = {
        "name": name,
        "latitude": lat,
        "longitude": lon,
        "location": location,
        "area": area,
        "zone": zone
    }
    
    data['restaurants'].append(new_restaurant)
    data['total_restaurants'] = len(data['restaurants'])
    data['last_updated'] = "2025-07-26"
    
    # Сохраняем обновленный файл
    with open('data/bali_restaurant_locations.json', 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    
    print(f"✅ Ресторан '{name}' добавлен успешно!")
    return True

if __name__ == "__main__":
    if len(sys.argv) != 7:
        print("Использование: python add_restaurant_location.py 'Название' lat lon 'Локация' 'Район' 'Зона'")
        print("Пример: python add_restaurant_location.py 'New Cafe' -8.4095 115.1889 'Canggu' 'Badung' 'South'")
        sys.exit(1)
    
    name, lat, lon, location, area, zone = sys.argv[1:7]
    add_restaurant_location(name, float(lat), float(lon), location, area, zone)
```

---

## 🔧 **АВТОМАТИЗАЦИЯ ОБНОВЛЕНИЙ**

### ⏰ **ВАРИАНТ 1: CRON JOB (Linux/Mac)**

```bash
# Редактируем crontab
crontab -e

# Добавляем строку для обновления каждые 2 дня в 3:00
0 3 */2 * * /path/to/muzaquest-project/update_database.sh

# Для ежедневного обновления в 3:00
0 3 * * * /path/to/muzaquest-project/update_database.sh
```

### 📅 **ВАРИАНТ 2: SCHEDULED TASKS (Windows)**

```batch
rem update_database.bat
@echo off
cd "C:\path\to\muzaquest-project"

rem Останавливаем приложение
taskkill /f /im python.exe

rem Создаем backup
copy database.sqlite "backups\database_backup_%date:~-4,4%%date:~-10,2%%date:~-7,2%_%time:~0,2%%time:~3,2%.sqlite"

rem Копируем новую базу
copy "C:\path\to\new\database.sqlite" database.sqlite

rem Запускаем приложение
start python web_app_optimized.py
```

### 🐍 **ВАРИАНТ 3: PYTHON SCHEDULER**

```python
# database_updater.py
import schedule
import time
import shutil
import sqlite3
import subprocess
from datetime import datetime

def update_database():
    """Автоматическое обновление базы данных"""
    
    try:
        # Путь к новой базе (например, из сетевой папки)
        new_db_path = "/network/share/updated_database.sqlite"
        current_db_path = "./database.sqlite"
        
        # Проверяем что новая база существует и корректна
        conn = sqlite3.connect(new_db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM restaurants")
        restaurant_count = cursor.fetchone()[0]
        conn.close()
        
        if restaurant_count > 0:
            # Создаем backup
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_path = f"./backups/database_backup_{timestamp}.sqlite"
            shutil.copy2(current_db_path, backup_path)
            
            # Заменяем базу
            shutil.copy2(new_db_path, current_db_path)
            
            print(f"✅ База данных обновлена: {timestamp}")
            print(f"📊 Ресторанов в новой базе: {restaurant_count}")
            
        else:
            print("❌ Новая база пуста или повреждена!")
            
    except Exception as e:
        print(f"❌ Ошибка обновления базы: {e}")

# Настраиваем расписание
schedule.every(2).days.at("03:00").do(update_database)

print("🕐 Планировщик обновления БД запущен...")
print("📅 Обновление каждые 2 дня в 3:00")

while True:
    schedule.run_pending()
    time.sleep(3600)  # Проверяем каждый час
```

---

## 🌐 **РАЗВЕРТЫВАНИЕ НА СЕРВЕРАХ**

### 🚀 **REPLIT (РЕКОМЕНДУЕТСЯ ДЛЯ КЛИЕНТА)**

```python
# .replit
[nix]
channel = "stable-22_11"

[deployment]
run = ["python", "web_app_optimized.py"]

[env]
DATABASE_URL = "sqlite:///database.sqlite"
```

**Обновление базы на Replit:**
1. Загружаем новую базу через файловый менеджер
2. Переименовываем в `database.sqlite`
3. Replit автоматически перезапускается

### ☁️ **CLOUD DEPLOYMENT**

#### **AWS/GCP/Azure:**
```yaml
# docker-compose.yml
version: '3.8'
services:
  muzaquest-app:
    build: .
    ports:
      - "8501:8501"
    volumes:
      - ./database.sqlite:/app/database.sqlite
      - ./data:/app/data
    environment:
      - PYTHONPATH=/app
```

#### **Heroku:**
```python
# requirements.txt должен включать
sqlite3  # Уже в Python
pandas>=1.3.0
streamlit>=1.24.0
# ... остальные зависимости
```

---

## ⚠️ **ВАЖНЫЕ МОМЕНТЫ И РЕКОМЕНДАЦИИ**

### 🔒 **БЕЗОПАСНОСТЬ:**
1. **Всегда создавайте backup** перед обновлением
2. **Проверяйте целостность** новой базы
3. **Логируйте все операции** обновления
4. **Ограничьте доступ** к файлу базы данных

### 📊 **МОНИТОРИНГ:**
```python
# health_check.py
import sqlite3
import sys

def check_database_health():
    """Проверка работоспособности базы"""
    try:
        conn = sqlite3.connect('database.sqlite')
        cursor = conn.cursor()
        
        # Проверяем основные таблицы
        tables = ['restaurants', 'grab_stats', 'gojek_stats']
        for table in tables:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
            print(f"✅ {table}: {count} записей")
        
        conn.close()
        return True
        
    except Exception as e:
        print(f"❌ Ошибка базы данных: {e}")
        return False

if __name__ == "__main__":
    if check_database_health():
        print("✅ База данных работает корректно")
        sys.exit(0)
    else:
        print("❌ Проблемы с базой данных")
        sys.exit(1)
```

### 📈 **ПРОИЗВОДИТЕЛЬНОСТЬ:**
- **TTL кеш 5 минут** - оптимально для обновлений раз в 1-2 дня
- При **>100 ресторанах** - увеличить до 10 минут
- При **>1000 записей** - рассмотреть PostgreSQL

---

## 🎯 **БЫСТРЫЙ СТАРТ ДЛЯ КЛИЕНТА**

### 📝 **ПРОСТАЯ ИНСТРУКЦИЯ:**

1. **Получили новую базу** от поставщика данных
2. **Остановили приложение** (если запущено)
3. **Заменили файл** `database.sqlite` в корневой папке
4. **Запустили приложение** заново
5. **Проверили работу** - новые рестораны должны появиться в списке

### 🔄 **ДЛЯ АВТОМАТИЗАЦИИ:**
1. Настроить скрипт `update_database.sh`
2. Добавить в cron job
3. Система будет обновляться автоматически

---

## ✅ **ЗАКЛЮЧЕНИЕ**

**Система полностью готова к работе с обновляемой базой данных!**

**Главное правило:** База должна называться `database.sqlite` и находиться в корневой папке проекта.

**Все остальное работает автоматически!** 🎉