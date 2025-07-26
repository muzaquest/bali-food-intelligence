#!/bin/bash

# 🔄 АВТОМАТИЧЕСКОЕ ОБНОВЛЕНИЕ БАЗЫ ДАННЫХ MUZAQUEST
# ==================================================
# Этот скрипт безопасно обновляет базу данных с backup'ом

# Конфигурация (НАСТРОЙТЕ ПОД СЕБЯ!)
PROJECT_DIR="/path/to/muzaquest-project"
NEW_DB_PATH="/path/to/updated/database.sqlite"
BACKUP_DIR="$PROJECT_DIR/backups"
LOG_FILE="$PROJECT_DIR/update.log"

# Функция логирования
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"
}

# Создаем папку для бэкапов
mkdir -p "$BACKUP_DIR"

log "🔄 Начинаем обновление базы данных..."

# Переходим в папку проекта
cd "$PROJECT_DIR" || {
    log "❌ Ошибка: Не найдена папка проекта $PROJECT_DIR"
    exit 1
}

# Проверяем что новая база существует
if [ ! -f "$NEW_DB_PATH" ]; then
    log "❌ Ошибка: Новая база не найдена по пути $NEW_DB_PATH"
    exit 1
fi

# Останавливаем приложение
log "⏹️ Останавливаем приложение..."
pkill -f "python.*web_app_optimized.py"
pkill -f "streamlit.*web_app_optimized.py"
sleep 5

# Создаем backup с временной меткой
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
BACKUP_FILE="$BACKUP_DIR/database_backup_$TIMESTAMP.sqlite"

if [ -f "database.sqlite" ]; then
    log "💾 Создаем backup: $BACKUP_FILE"
    cp database.sqlite "$BACKUP_FILE"
    
    if [ $? -eq 0 ]; then
        log "✅ Backup создан успешно"
    else
        log "❌ Ошибка создания backup!"
        exit 1
    fi
else
    log "⚠️ Старая база не найдена (возможно, первый запуск)"
fi

# Проверяем целостность новой базы
log "🔍 Проверяем целостность новой базы..."
RESTAURANT_COUNT=$(sqlite3 "$NEW_DB_PATH" "SELECT COUNT(*) FROM restaurants;" 2>/dev/null)

if [ $? -eq 0 ] && [ "$RESTAURANT_COUNT" -gt 0 ]; then
    log "✅ Новая база корректна. Ресторанов: $RESTAURANT_COUNT"
else
    log "❌ Ошибка: Новая база повреждена или пуста!"
    exit 1
fi

# Заменяем базу
log "🔄 Заменяем базу данных..."
cp "$NEW_DB_PATH" ./database.sqlite

if [ $? -eq 0 ]; then
    log "✅ База данных обновлена успешно"
else
    log "❌ Ошибка замены базы! Восстанавливаем backup..."
    
    if [ -f "$BACKUP_FILE" ]; then
        cp "$BACKUP_FILE" ./database.sqlite
        log "✅ Backup восстановлен"
    fi
    
    exit 1
fi

# Финальная проверка обновленной базы
log "🔍 Проверяем обновленную базу..."
FINAL_COUNT=$(sqlite3 "./database.sqlite" "SELECT COUNT(*) FROM restaurants;" 2>/dev/null)

if [ $? -eq 0 ] && [ "$FINAL_COUNT" -gt 0 ]; then
    log "✅ Финальная проверка пройдена. Ресторанов: $FINAL_COUNT"
else
    log "❌ Ошибка: Обновленная база не работает!"
    
    # Восстанавливаем backup
    if [ -f "$BACKUP_FILE" ]; then
        cp "$BACKUP_FILE" ./database.sqlite
        log "✅ Backup восстановлен"
    fi
    
    exit 1
fi

# Перезапускаем приложение
log "🚀 Перезапускаем приложение..."

# Выбираем метод запуска
if command -v streamlit &> /dev/null; then
    # Streamlit доступен
    nohup streamlit run web_app_optimized.py --server.port 8501 > app.log 2>&1 &
    log "✅ Приложение запущено через Streamlit на порту 8501"
else
    # Обычный Python
    nohup python web_app_optimized.py > app.log 2>&1 &
    log "✅ Приложение запущено через Python"
fi

# Ждем несколько секунд и проверяем что приложение запустилось
sleep 10

if pgrep -f "web_app_optimized.py" > /dev/null; then
    log "✅ Приложение успешно запущено"
else
    log "⚠️ Предупреждение: Приложение может не запуститься. Проверьте app.log"
fi

# Очистка старых backup'ов (оставляем последние 10)
log "🧹 Очистка старых backup'ов..."
cd "$BACKUP_DIR"
ls -t database_backup_*.sqlite | tail -n +11 | xargs -r rm
REMAINING_BACKUPS=$(ls database_backup_*.sqlite 2>/dev/null | wc -l)
log "📁 Оставлено backup'ов: $REMAINING_BACKUPS"

# Финальная сводка
log "🎉 ОБНОВЛЕНИЕ ЗАВЕРШЕНО!"
log "📊 Статистика:"
log "   • Время: $TIMESTAMP"
log "   • Ресторанов в новой базе: $FINAL_COUNT"
log "   • Backup сохранен: $BACKUP_FILE"
log "   • Приложение: перезапущено"
log "=================================="

exit 0