# Быстрый старт - 5 минут ⚡

## Протестировать систему БЕЗ ваших данных

### 1. Запустить демо:
```bash
python bali_demo_simple.py
```

Это покажет как работает система на примере 3 сценариев:
- Дождливый день (-22% продаж)
- Туристический сезон (+10% продаж)  
- Праздничный день (-31% продаж)

### 2. Получить API ключи (5 минут):

**OpenWeatherMap** (погода):
1. Идите на https://openweathermap.org/api
2. Нажмите "Sign Up" 
3. Подтвердите email
4. Скопируйте API ключ

**Calendarific** (праздники):
1. Идите на https://calendarific.com/
2. Нажмите "Get API Key"
3. Зарегистрируйтесь
4. Скопируйте API ключ

### 3. Создать файл .env:
```env
OPENWEATHER_API_KEY=ваш_ключ_погоды
CALENDARIFIC_API_KEY=ваш_ключ_праздников
```

### 4. Запустить с реальными данными:
```bash
python bali_integration_example.py
```

## Что нужно от вас для полноценной работы:

### ОБЯЗАТЕЛЬНО:
1. **CSV файл продаж** с колонками:
   - date (дата)
   - restaurant_name (название ресторана)
   - sales (продажи в рупиях)
   - orders (количество заказов)
   - ads_enabled (реклама вкл/выкл)

2. **Список ресторанов** с регионами:
   - Seminyak, Ubud, Canggu, Denpasar, Sanur

### ОПЦИОНАЛЬНО:
- Grab/Gojek API (если есть доступ)
- Telegram бот токен
- Данные о рейтингах

## Формат данных продаж:

```csv
date,restaurant_name,region,sales,orders,ads_enabled,rating
2024-01-15,Honeycomb,Seminyak,18500000,45,true,4.5
2024-01-15,Bali Bowls,Ubud,12300000,32,false,4.2
2024-01-16,Honeycomb,Seminyak,15200000,38,true,4.5
```

## Пример результата:

```json
{
  "restaurant": "Honeycomb",
  "date": "2024-01-15", 
  "sales_change": -22.7,
  "main_factors": {
    "heavy_rain": -15.2,
    "hindu_holiday": -8.1,
    "driver_shortage": -6.3
  },
  "recommendations": [
    "Увеличить промо-акции в дождливые дни",
    "Подготовиться к праздникам заранее",
    "Найти дополнительных водителей"
  ]
}
```

---

**Готовы? Запустите `python bali_demo_simple.py` прямо сейчас!**