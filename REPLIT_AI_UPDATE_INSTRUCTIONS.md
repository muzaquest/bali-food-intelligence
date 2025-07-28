# 🧠 ОБНОВЛЕНИЕ УМНОГО AI В REPLIT

## 🎯 ЦЕЛЬ: Обновить AI помощника до детективного уровня

### 📋 **ШАГИ ДЛЯ REPLIT:**

#### **1. Обновление через Git (РЕКОМЕНДУЕТСЯ):**
```bash
# В консоли Replit выполните:
git pull origin main

# Если есть конфликты:
git fetch origin
git reset --hard origin/main

# Перезапустите:
python main.py
```

#### **2. Ручное обновление (если Git не работает):**

**Откройте файл `ai_query_processor.py` и найдите функцию `_get_smart_period`**

Если её НЕТ - добавьте после строки `def _get_restaurant_data`:

```python
def _get_smart_period(self, query):
    """Определяет период из запроса пользователя"""
    query_lower = query.lower()
    
    # Зимний период
    if any(word in query_lower for word in ['зим', 'декабр', 'январ', 'февр']):
        return "2024-12-01", "2025-02-28"
    
    # Весенний период  
    elif any(word in query_lower for word in ['весн', 'апрел', 'май', 'июн']):
        return "2025-04-01", "2025-06-30"
    
    # Последние месяцы
    elif any(word in query_lower for word in ['последн', 'текущ', 'сейчас']):
        return "2025-04-01", "2025-06-30"  # Последний доступный период
    
    # По умолчанию - последний квартал
    else:
        return "2025-04-01", "2025-06-30"
```

#### **3. Обновите функцию `_get_restaurant_data`:**

**Найдите строку:**
```python
def _get_restaurant_data(self, restaurant_name):
```

**Замените на:**
```python
def _get_restaurant_data(self, restaurant_name, start_date=None, end_date=None):
    """Получение данных ресторана из базы с фильтрацией по периоду"""
    try:
        conn = sqlite3.connect(self.db_path)
        
        # Сначала получаем restaurant_id
        restaurant_query = "SELECT id FROM restaurants WHERE LOWER(name) LIKE ?"
        restaurant_result = pd.read_sql_query(restaurant_query, conn, params=[f'%{restaurant_name.lower()}%'])
        
        if restaurant_result.empty:
            conn.close()
            return None
            
        restaurant_id = int(restaurant_result.iloc[0]['id'])
        
        # Подготавливаем фильтр по датам
        date_filter = ""
        grab_params = [restaurant_id]
        gojek_params = [restaurant_id]
        
        if start_date and end_date:
            date_filter = " AND stat_date BETWEEN ? AND ?"
            grab_params.extend([start_date, end_date])
            gojek_params.extend([start_date, end_date])
        
        # Grab данные с фильтрацией по датам
        grab_query = f"SELECT SUM(sales) as sales, SUM(orders) as orders, AVG(rating) as rating FROM grab_stats WHERE restaurant_id = ?{date_filter}"
        grab_data = pd.read_sql_query(grab_query, conn, params=grab_params)
        
        # ... остальной код функции остается таким же
```

#### **4. Добавьте детективный анализ:**

**Добавьте в конец файла перед последней функцией:**

```python
def _analyze_sales_drop_detective(self, restaurant_name, query):
    """Детективный анализ 'падения' продаж с полным разбором"""
    try:
        # Определяем периоды для сравнения из запроса
        query_lower = query.lower()
        
        # Если упоминается конкретный период
        if "весн" in query_lower:
            current_period = ("2025-04-01", "2025-06-30", "Весна 2025")
            previous_period = ("2024-12-01", "2025-02-28", "Зима 2024-25")
        elif "зим" in query_lower:
            current_period = ("2024-12-01", "2025-02-28", "Зима 2024-25")
            previous_period = ("2024-09-01", "2024-11-30", "Осень 2024")
        else:
            # По умолчанию сравниваем последние два квартала
            current_period = ("2025-04-01", "2025-06-30", "Весна 2025")
            previous_period = ("2024-12-01", "2025-02-28", "Зима 2024-25")
        
        # Получаем данные за оба периода
        current_data = self._get_restaurant_data(restaurant_name, current_period[0], current_period[1])
        previous_data = self._get_restaurant_data(restaurant_name, previous_period[0], previous_period[1])
        
        if not current_data or not previous_data:
            return f"❌ Недостаточно данных для анализа {restaurant_name}"
        
        # Вычисляем изменения
        sales_change = ((current_data['total_sales'] - previous_data['total_sales']) / previous_data['total_sales']) * 100
        orders_change = ((current_data['total_orders'] - previous_data['total_orders']) / previous_data['total_orders']) * 100
        aov_change = ((current_data['avg_order_value'] - previous_data['avg_order_value']) / previous_data['avg_order_value']) * 100
        
        # Детективный вердикт
        if sales_change > 0:
            main_verdict = f"🔍 ДЕТЕКТИВНЫЙ АНАЛИЗ: Продажи {restaurant_name} НЕ УПАЛИ!"
            sales_verdict = f"📈 РОСТ: +{sales_change:.1f}% (а не падение!)"
        else:
            main_verdict = f"🔍 ДЕТЕКТИВНЫЙ АНАЛИЗ: Разбираем снижение продаж {restaurant_name}"
            sales_verdict = f"📉 СНИЖЕНИЕ: {sales_change:.1f}%"
        
        detective_report = f"""
{main_verdict}

📊 **ФАКТЫ:**
• {current_period[2]}: {current_data['total_sales']:,.0f} IDR
• {previous_period[2]}: {previous_data['total_sales']:,.0f} IDR  
• {sales_verdict}

📈 **ДЕТАЛЬНАЯ КАРТИНА:**
• Заказы: {current_data['total_orders']:,.0f} vs {previous_data['total_orders']:,.0f} ({orders_change:+.1f}%)
• Средний чек: {current_data['avg_order_value']:,.0f} vs {previous_data['avg_order_value']:,.0f} IDR ({aov_change:+.1f}%)
• Рейтинг: {current_data.get('avg_rating', 'N/A')} vs {previous_data.get('avg_rating', 'N/A')}
"""

        # Бизнес-инсайты
        insights = []
        
        if aov_change > 5:
            insights.append("💎 **ИНСАЙТ: ПРЕМИАЛИЗАЦИЯ**")
            insights.append(f"• Средний чек вырос с {previous_data['avg_order_value']:,.0f} до {current_data['avg_order_value']:,.0f} IDR (+{aov_change:.1f}%)")
            insights.append("• Меньше заказов, но дороже = умная стратегия")
        
        # Финальный вывод
        if sales_change > 0 and aov_change > 5:
            conclusion = "💡 **ВЫВОД:** Это не падение, а эволюция в премиум!"
        elif sales_change > 0:
            conclusion = "💡 **ВЫВОД:** Продажи растут, никакого падения нет!"
        else:
            conclusion = "💡 **ВЫВОД:** Требуется детальный анализ операционных проблем"
        
        if insights:
            detective_report += "\n" + "\n".join(insights)
        
        detective_report += f"\n\n{conclusion}"
        
        return detective_report
        
    except Exception as e:
        return f"❌ Ошибка при анализе: {str(e)}"
```

#### **5. Обновите обработчик запросов:**

**Найдите строку:**
```python
if self._is_sales_drop_analysis(query_lower):
    return self._analyze_sales_drop(original_query)
```

**Замените на:**
```python
if self._is_sales_drop_analysis(query_lower):
    restaurant_name = self._extract_restaurant_name(original_query)
    if restaurant_name:
        return self._analyze_sales_drop_detective(restaurant_name, original_query)
    else:
        return "❌ Укажите название ресторана для детективного анализа"
```

## ✅ **ПРОВЕРКА РАБОТЫ:**

После обновления протестируйте:
- Спросите: *"Почему у Ika Canggu упали продажи?"*
- Должен ответить детективным анализом с фактами и выводами

## 🎯 **ОЖИДАЕМЫЙ РЕЗУЛЬТАТ:**

AI теперь будет отвечать как детектив:
```
🔍 ДЕТЕКТИВНЫЙ АНАЛИЗ: Продажи Ika Canggu НЕ УПАЛИ!

📊 ФАКТЫ:
• Весна 2025: 1,080,876,000 IDR
• Зима 2024-25: 1,024,234,567 IDR  
• 📈 РОСТ: +5.3% (а не падение!)

💎 ИНСАЙТ: ПРЕМИАЛИЗАЦИЯ
• Средний чек вырос с 366,884 до 408,186 IDR (+11.3%)
• Меньше заказов, но дороже = умная стратегия

💡 ВЫВОД: Это не падение, а эволюция в премиум!
```

## 🚨 **ЕСЛИ ЧТО-ТО НЕ РАБОТАЕТ:**

1. Проверьте, что файл сохранился
2. Перезапустите приложение: `python main.py`  
3. Проверьте консоль на ошибки
4. Если проблемы - напишите, поможем!