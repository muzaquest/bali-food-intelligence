# 🚀 ИНСТРУКЦИЯ ПО РАЗВЕРТЫВАНИЮ НА REPLIT

## 📋 **БЫСТРЫЙ СТАРТ**

### 1️⃣ **Импорт проекта в Replit**
1. Откройте [replit.com](https://replit.com)
2. Нажмите **"Create Repl"**
3. Выберите **"Import from GitHub"**
4. Вставьте URL: `https://github.com/muzaquest/bali-food-intelligence`
5. Нажмите **"Import from GitHub"**

### 2️⃣ **Настройка API ключей**
1. Откройте файл `.env` в Replit
2. Заполните ваши API ключи:
```bash
# OpenAI API (обязательно для AI анализа)
OPENAI_API_KEY=sk-your-openai-key-here

# Calendarific API (обязательно для анализа праздников)
CALENDARIFIC_API_KEY=your-calendarific-key-here

# Google Maps API (опционально)
GOOGLE_MAPS_API_KEY=your-google-maps-key-here
```

### 3️⃣ **Запуск приложения**
1. В Replit нажмите зеленую кнопку **"Run"**
2. Или выполните в терминале:
```bash
streamlit run web_app_optimized.py --server.port 8080 --server.address 0.0.0.0
```

### 4️⃣ **Проверка работы**
- Откройте предоставленный Replit URL
- Должен открыться веб-интерфейс MUZAQUEST Analytics
- Проверьте что загрузился список ресторанов

---

## ⚠️ **ВАЖНЫЕ МОМЕНТЫ**

### 🔑 **API Ключи:**
- **OpenAI**: Обязательно для AI анализа
- **Calendarific**: Обязательно для анализа праздников  
- **Weather API**: Не нужен (используем бесплатный Open-Meteo)

### 📊 **База данных:**
- Файл `database.sqlite` уже включен в проект
- Для обновляемой БД см. `DATABASE_UPDATE_GUIDE.md`
- Для API подключения см. `API_DATABASE_INTEGRATION.md`

### 🐛 **Решение проблем:**
```bash
# Если ошибки с зависимостями:
pip install -r requirements.txt

# Если порт занят:
streamlit run web_app_optimized.py --server.port 8081

# Проверка работоспособности:
python health_check.py
```

---

## 🎯 **ФУНКЦИИ ПРИЛОЖЕНИЯ**

### 🏠 **Главная страница:**
- Обзор рынка ресторанов Бали
- Ключевые метрики
- Статистика по платформам

### 🏪 **Анализ ресторана:**
- Выбор из 59 ресторанов
- Временные периоды: Неделя, Месяц, Квартал, Полгода
- Полный анализ всех 63 параметров + API

### 🏢 **Анализ рынка:**
- Рыночный обзор и бенчмаркинг
- Сегментация и корреляции
- ML поиск аномалий

### 💬 **AI Помощник:**
- Свободные запросы на естественном языке
- Детективный анализ продаж
- Анализ аномалий и трендов

---

## 📚 **ДОПОЛНИТЕЛЬНАЯ ДОКУМЕНТАЦИЯ**

- `README.md` - Полное описание проекта
- `DATABASE_UPDATE_GUIDE.md` - Подключение обновляемой БД
- `API_DATABASE_INTEGRATION.md` - API решение
- `DATA_ACCURACY_GUARANTEE.md` - Гарантии точности данных

---

## 🎉 **ГОТОВО!**

После выполнения этих шагов у вас будет **полностью рабочая система аналитики ресторанов** с AI агентом, ML моделями и всеми 164 балийскими праздниками!

**URL приложения будет вида:** `https://your-repl-name.username.repl.co`