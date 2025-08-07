# 🔧 ИСПРАВЛЕНИЕ ОТСУТСТВУЮЩИХ ФАКТОРОВ В REPLIT

## 🎯 **ПРОБЛЕМА:**
В Replit показываются не все факторы анализа:
- ❌ Нет fake orders исключений
- ❌ Нет конкретных праздников (Purnama, etc.)  
- ❌ Нет операционных времен (preparation, driver waiting)
- ❌ Нет детальных SHAP объяснений ML

## ✅ **ЛОКАЛЬНО ВСЕ РАБОТАЕТ:**
```
📉 ПРОБЛЕМНЫЙ ДЕНЬ #1: 2025-04-21
   🔍 ФАКТОРЫ ВЛИЯНИЯ:
      1. 🚨 FAKE ORDERS ИСКЛЮЧЕНЫ: 21 заказов (55,000 IDR)
      2.    📱 Grab fake: 17 заказов (51,000 IDR)
      3.    🛵 Gojek fake: 4 заказов (4,000 IDR)
      4. ⚠️ Gojek Preparation 24.6мин (+38% выше)
      5. ⚠️ Gojek Driver Waiting 14мин (+46% выше)
      6. 🎉 Purnama (Full Moon)
      7. 🌧️ Сильный дождь (10.8мм)
```

---

## 🔧 **РЕШЕНИЯ:**

### **1️⃣ ПРОВЕРИТЬ ФАЙЛЫ ДАННЫХ В REPLIT:**

**Убедиться что есть файлы:**
```bash
ls -la fake_orders_data.json
ls -la data/comprehensive_holiday_analysis.json  
ls -la data/bali_restaurant_locations.json
```

**Если нет - скопировать из GitHub:**
```bash
# Fake orders data
curl -o fake_orders_data.json https://raw.githubusercontent.com/muzaquest/bali-food-intelligence/main/fake_orders_data.json

# Holiday data  
curl -o data/comprehensive_holiday_analysis.json https://raw.githubusercontent.com/muzaquest/bali-food-intelligence/main/data/comprehensive_holiday_analysis.json

# Restaurant locations
curl -o data/bali_restaurant_locations.json https://raw.githubusercontent.com/muzaquest/bali-food-intelligence/main/data/bali_restaurant_locations.json
```

### **2️⃣ ПРОВЕРИТЬ ВЕРСИЮ КОДА:**

**В main.py должно быть:**
```python
from src.analyzers import ProductionSalesAnalyzer

detective_analyzer = ProductionSalesAnalyzer()
detective_results = detective_analyzer.analyze_restaurant_performance(
    restaurant_name, start_date, end_date, use_ml=False  # или True если ML работает
)
```

### **3️⃣ ПРОВЕРИТЬ ИНТЕРНЕТ ДОСТУП:**

**Тест погодного API:**
```python
import requests
url = "https://api.open-meteo.com/v1/forecast?latitude=-8.4095&longitude=115.1889&daily=precipitation_sum,temperature_2m_mean&start_date=2025-04-21&end_date=2025-04-21"
response = requests.get(url)
print(f"Weather API: {response.status_code}")
```

**Тест Google Sheets:**
```python
import requests  
url = "https://docs.google.com/spreadsheets/d/1LRkQeh6lzgRY96HECT5nc5cZKjA475LZHcuRipX14qM/export?format=csv&gid=1724820690"
response = requests.get(url)
print(f"Google Sheets: {response.status_code}")
```

### **4️⃣ ПРОВЕРИТЬ ИМПОРТЫ:**

**В начале main.py должно быть:**
```python
# Fake orders filter (опционально)
try:
    from src.utils.fake_orders_filter import FakeOrdersFilter
    FAKE_ORDERS_AVAILABLE = True
except ImportError:
    FAKE_ORDERS_AVAILABLE = False
    print("⚠️ Fake orders filter недоступен")
```

---

## 🧪 **ТЕСТ ДИАГНОСТИКИ В REPLIT:**

**Добавить в начало main.py для диагностики:**
```python
print("🔍 ДИАГНОСТИКА REPLIT:")
print("=" * 50)

# Проверка файлов
import os
files_to_check = [
    "fake_orders_data.json",
    "data/comprehensive_holiday_analysis.json", 
    "data/bali_restaurant_locations.json"
]

for file in files_to_check:
    exists = os.path.exists(file)
    print(f"{'✅' if exists else '❌'} {file}: {'EXISTS' if exists else 'MISSING'}")

# Проверка интернета
try:
    import requests
    response = requests.get("https://api.open-meteo.com/v1/forecast?latitude=-8.4095&longitude=115.1889&daily=precipitation_sum&start_date=2025-04-21&end_date=2025-04-21", timeout=5)
    print(f"✅ Weather API: {response.status_code}")
except Exception as e:
    print(f"❌ Weather API: {e}")

# Проверка анализатора
try:
    from src.analyzers import ProductionSalesAnalyzer
    analyzer = ProductionSalesAnalyzer()
    print("✅ ProductionSalesAnalyzer: LOADED")
    
    # Тест на одном дне
    test_results = analyzer.analyze_restaurant_performance(
        'Only Eggs', '2025-04-21', '2025-04-21', use_ml=False
    )
    
    result_text = '\n'.join(test_results)
    print(f"✅ Test results: {len(test_results)} lines")
    print(f"{'✅' if 'FAKE ORDERS' in result_text else '❌'} Fake orders in results")
    print(f"{'✅' if 'Preparation' in result_text else '❌'} Operation times in results")
    
except Exception as e:
    print(f"❌ ProductionSalesAnalyzer: {e}")

print("=" * 50)
```

---

## 🎯 **ОЖИДАЕМЫЙ РЕЗУЛЬТАТ ПОСЛЕ ИСПРАВЛЕНИЯ:**

**В Replit должно показаться:**
```
📉 ПРОБЛЕМНЫЙ ДЕНЬ #1: 2025-04-21
   💔 Падение продаж: 75.7%
   
   🔍 ФАКТОРЫ ВЛИЯНИЯ:
      1. 🚨 FAKE ORDERS ИСКЛЮЧЕНЫ: 21 заказов (55,000 IDR)
      2.    📱 Grab fake: 17 заказов (51,000 IDR)  
      3.    🛵 Gojek fake: 4 заказов (4,000 IDR)
      4. ⚠️ Gojek Preparation 24.6мин (+38% выше)
      5. ⚠️ Gojek Driver Waiting 14мин (+46% выше)
      6. 🎉 Purnama (Full Moon)
      7. 🌧️ Сильный дождь (10.8мм)
   
   🚨 КРИТИЧЕСКИЕ ПРОБЛЕМЫ:
      • Fake orders обнаружены и исключены
      • Критическое время готовки Gojek
   
   📊 ОЦЕНКА: 🔴 КРИТИЧЕСКОЕ негативное влияние
```

---

## 🚀 **БЫСТРОЕ РЕШЕНИЕ:**

**Если сложно исправить - временно можно:**
1. Скопировать файлы данных из GitHub
2. Убедиться что интернет доступен
3. Обновить версию `ProductionSalesAnalyzer`

**Результат:** Все 17 факторов будут показаны как в примере выше! 🎯