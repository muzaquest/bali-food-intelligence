#!/bin/bash

# 🚀 БЫСТРОЕ ОБНОВЛЕНИЕ REPLIT
# =============================

echo "🔄 Начинаем обновление Replit..."

# 1. Обновляем репозиторий
echo "📡 Обновляем репозиторий..."
git remote set-url origin https://github.com/muzaquest/bali-food-intelligence.git
git fetch origin
git checkout main
git pull origin main

# 2. Проверяем обновление
echo "✅ Проверяем последние коммиты..."
git log --oneline -3

# 3. Устанавливаем зависимости
echo "📦 Устанавливаем зависимости..."
pip install -r requirements.txt
pip install -r requirements_ml.txt

# 4. Проверяем ML модули
echo "🤖 Проверяем ML модули..."
python3 -c "
try:
    import sklearn
    print('✅ scikit-learn:', sklearn.__version__)
except:
    print('❌ scikit-learn не установлен')

try:
    import pandas as pd
    print('✅ pandas:', pd.__version__)
except:
    print('❌ pandas не установлен')

try:
    import prophet
    print('✅ prophet: OK')
except:
    print('⚠️ prophet не установлен')

try:
    import shap
    print('✅ shap: OK')
except:
    print('⚠️ shap не установлен')
"

# 5. Проверяем ключевые файлы
echo "📁 Проверяем ML файлы..."
if [ -f "ml_models.py" ]; then
    echo "✅ ml_models.py найден"
else
    echo "❌ ml_models.py отсутствует"
fi

if [ -f "proper_ml_detective_analysis.py" ]; then
    echo "✅ proper_ml_detective_analysis.py найден"
else
    echo "❌ proper_ml_detective_analysis.py отсутствует"
fi

if [ -f "ai_query_processor.py" ]; then
    echo "✅ ai_query_processor.py найден"
else
    echo "❌ ai_query_processor.py отсутствует"
fi

# 6. Проверяем данные
echo "📊 Проверяем ML данные..."
ls -la data/*.json | head -4

# 7. Проверяем базу данных
echo "🗄️ Проверяем базу данных..."
if [ -f "database.sqlite" ]; then
    echo "✅ database.sqlite найдена"
else
    echo "⚠️ database.sqlite не найдена, копируем из data/"
    cp data/database.sqlite ./
fi

echo ""
echo "🎉 ОБНОВЛЕНИЕ ЗАВЕРШЕНО!"
echo ""
echo "🚀 Для запуска используйте:"
echo "   python3 main.py"
echo "   ИЛИ"
echo "   streamlit run web_app_optimized.py --server.port 8080"
echo ""
echo "✅ Система готова к работе!"