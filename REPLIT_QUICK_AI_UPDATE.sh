#!/bin/bash

# 🧠 БЫСТРОЕ ОБНОВЛЕНИЕ УМНОГО AI В REPLIT
# ===============================================

echo "🚀 Обновляем AI помощника до детективного уровня..."

# Обновляем код из GitHub
echo "📥 Загружаем последние изменения..."
git fetch origin
git reset --hard origin/main

echo "✅ Код обновлен!"

# Проверяем ключевые функции
echo "🔍 Проверяем новые функции..."

if grep -q "_get_smart_period" ai_query_processor.py; then
    echo "✅ Умное определение периода - ОК"
else
    echo "❌ Функция _get_smart_period не найдена"
fi

if grep -q "_analyze_sales_drop_detective" ai_query_processor.py; then
    echo "✅ Детективный анализ - ОК"
else
    echo "❌ Функция _analyze_sales_drop_detective не найдена"
fi

if grep -q "start_date=None, end_date=None" ai_query_processor.py; then
    echo "✅ Фильтрация по датам - ОК"
else
    echo "❌ Фильтрация по датам не найдена"
fi

echo ""
echo "🎉 ОБНОВЛЕНИЕ ЗАВЕРШЕНО!"
echo ""
echo "📋 Что теперь умеет ваш AI:"
echo "• 🔍 Детективный анализ продаж"
echo "• 📅 Умное определение периодов"
echo "• 💎 Анализ премиализации"
echo "• 🌤️ Сезонные инсайты"
echo "• 🏄‍♂️ Локационная специфика"
echo ""
echo "🧪 Протестируйте: 'Почему у Ika Canggu упали продажи?'"
echo ""
echo "🚀 Перезапустите приложение: python main.py"