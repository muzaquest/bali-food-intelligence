# 📊 Данные для анализа ресторанов Бали

## 🎯 Ключевые файлы данных

### 🎉 `comprehensive_holiday_analysis.json` (49KB)
**164 праздника для Бали**
- Балийские праздники (Galungan, Kuningan, Purnama, Tilem)
- Мусульманские праздники (Eid al-Fitr, Mawlid)
- Международные праздники (New Year, Christmas, Labor Day)
- Китайские праздники (Chinese New Year, Mid-Autumn)
- **Влияние на продажи** для каждого праздника в %

### 📍 `bali_restaurant_locations.json` (21KB)
**GPS координаты 59 ресторанов**
- Точные координаты (latitude, longitude)
- Зоны: Beach, Central, Mountain, Cliff
- Локации: Canggu, Seminyak, Ubud, Uluwatu, Jimbaran
- Используется для получения локальной погоды

## 🏝️ Туристические данные

### 📁 `tourism/`
- `Kunjungan_Wisatawan_Bali_2024.xls` - Данные посещаемости 2024
- `Table-1-7-Final-1-1.xls` - Детальная статистика по странам
- `1.-Data-Kunjungan-2024.xls` - Дополнительные данные 2024
- `1.-Data-Kunjungan-2025-3.xls` - Данные 2025 (до мая)

**Содержит:**
- Посещаемость по месяцам
- Топ-страны туристов
- Сезонные колебания
- Корреляции с продажами ресторанов

## 🌧️ Погодные данные

Получаются в реальном времени через Open-Meteo API:
- Температура
- Осадки 
- Влажность
- Ветер

## 📈 Структура данных

### Праздники (`comprehensive_holiday_analysis.json`)
```json
{
  "2025-04-21": {
    "name": "Odalan Temple Festival",
    "type": "balinese",
    "category": "Балийский",
    "religion": "hindu",
    "sales": 1793000,
    "impact_percent": -25.7
  }
}
```

### Локации (`bali_restaurant_locations.json`)
```json
{
  "restaurants": [
    {
      "name": "Only Eggs",
      "latitude": -8.788,
      "longitude": 115.167,
      "location": "Jimbaran",
      "zone": "South"
    }
  ]
}
```

## 🔄 Обновление данных

- **Праздники**: Статические (164 праздника на год)
- **Локации**: Статические (59 ресторанов)
- **Туризм**: Обновляется ежемесячно
- **Погода**: Реальное время через API