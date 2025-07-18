# 🚀 Руководство по интеграции системы анализа продаж для Бали

## 🎯 Что у вас уже есть

✅ **Полная система анализа продаж** специально адаптированная для Бали  
✅ **Учет местных особенностей**: мусульманские/индуистские праздники, сезоны дождей, туристические потоки  
✅ **Автоматический сбор данных** из API погоды, праздников, платформ доставки  
✅ **Интеллектуальные рекомендации** на основе анализа факторов  

## 📊 Какие данные нужны от вас

### 1. **Основные данные продаж** (обязательно)
```python
{
    "restaurant_name": "Warung Bali Asli",
    "location": "seminyak",  # seminyak, ubud, canggu, denpasar, sanur
    "date": "2024-01-15",
    "total_sales": 2500000,  # в рупиях
    "previous_day_sales": 3200000
}
```

### 2. **Дополнительные бизнес-данные** (желательно)
```python
{
    "ads_sales": 750000,        # продажи от рекламы
    "ads_budget": 200000,       # бюджет рекламы
    "rating": 4.6,              # рейтинг ресторана
    "cancel_rate": 0.12,        # процент отмен
    "delivery_time": 35,        # среднее время доставки (мин)
    "orders_count": 85,         # количество заказов
    "average_order": 29412,     # средний чек
    "platform": "grab"          # grab, gojek, both
}
```

### 3. **API ключи** (опционально, для реальных данных)
```bash
# Погода
export OPENWEATHER_API_KEY="your_api_key"

# Праздники
export CALENDARIFIC_API_KEY="your_api_key"

# Платформы доставки
export GRAB_API_KEY="your_api_key"
export GOJEK_API_KEY="your_api_key"
```

## 🔧 Как интегрировать с вашими данными

### Вариант 1: Простая интеграция (рекомендуется)
```python
from bali_demo_simple import BaliSalesAnalyzerDemo

# Создаем анализатор
analyzer = BaliSalesAnalyzerDemo()

# Анализируем ваши данные
result = analyzer.analyze_sales_change(
    restaurant_name="Ваш ресторан",
    location="seminyak",  # ваша локация
    date="2024-01-15",    # дата анализа
    actual_sales=2500000,  # фактические продажи
    previous_sales=3200000 # продажи предыдущего дня
)

# Выводим результат
analyzer.print_analysis(result)
```

### Вариант 2: Полная интеграция с API
```python
from bali_integration_example import BaliSalesAnalyzer

# Создаем анализатор с API
analyzer = BaliSalesAnalyzer()

# Анализируем с реальными данными погоды/праздников
result = analyzer.analyze_sales_drop(
    restaurant_name="Ваш ресторан",
    location="seminyak",
    date="2024-01-15",
    actual_sales=2500000,
    previous_sales=3200000
)
```

### Вариант 3: Пакетный анализ за период
```python
# Данные продаж за неделю
weekly_sales = {
    '2024-01-15': 2500000,
    '2024-01-16': 2800000,
    '2024-01-17': 2200000,
    '2024-01-18': 3100000,
    '2024-01-19': 2900000,
    '2024-01-20': 3400000,
    '2024-01-21': 2700000,
}

# Анализируем всю неделю
results = analyzer.batch_analyze_week(
    restaurant_name="Ваш ресторан",
    location="seminyak",
    start_date="2024-01-15",
    sales_data=weekly_sales
)
```

## 🏝️ Особенности для Бали

### 1. **Поддерживаемые локации**
- **Seminyak** - пляжная luxury зона (90% туристов)
- **Ubud** - культурный центр (80% туристов)
- **Canggu** - серфинг зона (80% туристов)
- **Denpasar** - городской центр (30% туристов)
- **Sanur** - семейный пляж (70% туристов)

### 2. **Учитываемые праздники**
- **Мусульманские**: Рамадан, Ид аль-Фитр, пятничные молитвы
- **Индуистские**: Nyepi, Galungan, Kuningan, Saraswati
- **Национальные**: День независимости, День Панчасила

### 3. **Сезонные факторы**
- **Сезон дождей**: Декабрь-Март (больше влияние на продажи)
- **Туристический сезон**: Июнь-Август, Декабрь-Январь
- **Низкий сезон**: Февраль-Март, Ноябрь

### 4. **Факторы водителей**
- **Время молитв**: 5:00, 12:00, 15:00, 18:00, 19:00
- **Пятничные молитвы**: Снижение активности
- **Праздники**: Значительное влияние на доступность

## 📈 Примеры результатов

### Пример 1: Падение продаж в дождь
```
🎯 АНАЛИЗ ПРИЧИН ИЗМЕНЕНИЯ ПРОДАЖ НА БАЛИ
🏪 Ресторан: Warung Bali Asli
📍 Локация: seminyak
📅 Дата: 2024-01-15
💰 Продажи: 2,500,000 IDR (было: 3,200,000 IDR)
📉 Изменение: -21.9% (-700,000 IDR)

🌍 ВНЕШНИЕ ФАКТОРЫ:
🌧️ Погода: 10.3мм дождя, 29.2°C, влажность 83.0%
🚗 Доступность водителей: 60%
🏖️ Туристический сезон: high (117% плотность)

📊 ВЛИЯНИЕ ФАКТОРОВ:
🌧️ Погода: -8%
🎉 Праздники: -12%
🚗 Водители: -10%
🏖️ Туристы: +10%

🎯 РЕКОМЕНДАЦИИ:
1. 🌧️ Увеличить комиссию за доставку в дождь на 15-20%
2. 🎯 Запустить промо 'Дождливый день' с бесплатной доставкой
3. 💰 Повысить бонусы водителям за работу в дождь
4. 🛕 Предложить специальное праздничное меню
```

## 🔌 Интеграция с вашими системами

### 1. **Автоматический анализ каждый день**
```python
import schedule
import time

def daily_analysis():
    # Получаем данные из вашей системы
    today_sales = get_sales_from_your_system()
    yesterday_sales = get_yesterday_sales()
    
    # Анализируем
    result = analyzer.analyze_sales_change(
        restaurant_name="Ваш ресторан",
        location="seminyak",
        date=datetime.now().strftime('%Y-%m-%d'),
        actual_sales=today_sales,
        previous_sales=yesterday_sales
    )
    
    # Отправляем результат в Telegram/Email/Slack
    send_to_telegram(result)

# Запускаем каждый день в 23:00
schedule.every().day.at("23:00").do(daily_analysis)

while True:
    schedule.run_pending()
    time.sleep(60)
```

### 2. **Webhook для получения данных**
```python
from flask import Flask, request, jsonify

app = Flask(__name__)

@app.route('/analyze_sales', methods=['POST'])
def analyze_sales():
    data = request.json
    
    result = analyzer.analyze_sales_change(
        restaurant_name=data['restaurant'],
        location=data['location'],
        date=data['date'],
        actual_sales=data['actual_sales'],
        previous_sales=data['previous_sales']
    )
    
    return jsonify(result)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
```

### 3. **Интеграция с Grab/Gojek API**
```python
# Автоматическое получение данных продаж
def get_grab_sales(date):
    headers = {'Authorization': f'Bearer {GRAB_API_KEY}'}
    response = requests.get(f'https://partner-api.grab.com/sales?date={date}', headers=headers)
    return response.json()['total_sales']

# Автоматический анализ
def auto_analyze():
    today = datetime.now().strftime('%Y-%m-%d')
    yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    
    today_sales = get_grab_sales(today)
    yesterday_sales = get_grab_sales(yesterday)
    
    result = analyzer.analyze_sales_change(
        restaurant_name="Ваш ресторан",
        location="seminyak",
        date=today,
        actual_sales=today_sales,
        previous_sales=yesterday_sales
    )
    
    return result
```

## 🎯 Конкретные рекомендации для вашего бизнеса

### По погоде:
- **Дождь > 15мм**: Увеличить комиссию на 15-20%, бонусы водителям +30%
- **Экстремальный дождь**: Временно приостановить доставку, промо на самовывоз

### По праздникам:
- **Мусульманские**: Убрать свинину, халяльные блюда, корректировка времени
- **Индуистские**: Специальное меню, промо для туристов
- **Национальные**: Патриотическое меню, скидки для местных

### По туристическому сезону:
- **Высокий сезон**: Английское меню, продление работы, международные карты
- **Низкий сезон**: Фокус на местных, традиционные блюда, снижение цен

### По водителям:
- **Нехватка**: Бонусы +20-30%, дополнительные сервисы, увеличение времени доставки
- **Время молитв**: Корректировка ожиданий клиентов, альтернативные слоты

## 📱 Уведомления и алерты

### Telegram бот для ежедневных отчетов:
```python
import telebot

bot = telebot.TeleBot('YOUR_BOT_TOKEN')

def send_daily_report(result):
    message = f"""
🏪 {result['restaurant']}
📅 {result['date']}
📈 Изменение: {result['change_percent']:+.1f}%
💰 Продажи: {result['actual_sales']:,} IDR

💡 {result['explanation']}

🎯 Рекомендации:
""" + '\n'.join([f"• {rec}" for rec in result['recommendations'][:5]])
    
    bot.send_message(chat_id='YOUR_CHAT_ID', text=message)
```

## 🚀 Следующие шаги

1. **Протестируйте систему** на ваших исторических данных
2. **Настройте API ключи** для реальных данных погоды/праздников
3. **Интегрируйте с вашими системами** (POS, CRM, платформы доставки)
4. **Настройте автоматические отчеты** в Telegram/Email
5. **Обучите команду** использовать рекомендации системы

## 📞 Поддержка

Система готова к работе! Если нужна помощь с интеграцией:

1. **Запустите демо**: `python3 bali_demo_simple.py`
2. **Измените данные** в коде под ваши
3. **Добавьте API ключи** в переменные окружения
4. **Настройте автоматизацию** под ваши процессы

**Результат**: Точная аналитика причин изменения продаж с конкретными рекомендациями для вашего бизнеса на Бали! 🎯