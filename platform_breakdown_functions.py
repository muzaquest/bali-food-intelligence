#!/usr/bin/env python3
"""
🔄 ФУНКЦИИ ДЛЯ КОРРЕКТНОГО РАЗДЕЛЕНИЯ ДАННЫХ ПО ПЛАТФОРМАМ
═══════════════════════════════════════════════════════════════════════════════
Обеспечивает прозрачность и корректность в отчетах
"""

def generate_platform_breakdown(metric_name, grab_value, gojek_value, unit="", methodology_note=""):
    """Генерирует разбивку метрики по платформам"""
    
    total_value = grab_value + gojek_value if isinstance(grab_value, (int, float)) and isinstance(gojek_value, (int, float)) else "N/A"
    
    return f"""
📊 {metric_name.upper()}:
├── 📱 GRAB: {grab_value:,.2f}{unit} if isinstance(grab_value, (int, float)) else grab_value
├── 🛵 GOJEK: {gojek_value:,.2f}{unit} if isinstance(gojek_value, (int, float)) else gojek_value
└── 🎯 ОБЩИЙ: {total_value:,.2f}{unit} if isinstance(total_value, (int, float)) else total_value

{f'⚠️ МЕТОДИКА: {methodology_note}' if methodology_note else ''}
"""

def generate_roas_breakdown(grab_sales, grab_spend, gojek_sales, gojek_spend):
    """Специальная функция для ROAS с объяснением методики"""
    
    grab_roas = grab_sales / grab_spend if grab_spend > 0 else 0
    gojek_roas = gojek_sales / gojek_spend if gojek_spend > 0 else 0
    total_roas = (grab_sales + gojek_sales) / (grab_spend + gojek_spend) if (grab_spend + gojek_spend) > 0 else 0
    
    return f"""
🎯 ROAS АНАЛИЗ:
├── 📱 GRAB: {grab_roas:.2f}x (продажи: {grab_sales:,.0f} IDR / бюджет: {grab_spend:,.0f} IDR)
├── 🛵 GOJEK: {gojek_roas:.2f}x (продажи: {gojek_sales:,.0f} IDR / бюджет: {gojek_spend:,.0f} IDR)
└── 🎯 ОБЩИЙ: {total_roas:.2f}x (продажи: {grab_sales + gojek_sales:,.0f} IDR / бюджет: {grab_spend + gojek_spend:,.0f} IDR)

⚠️ МЕТОДИКА: 
• GRAB: учитывает только прямые продажи от рекламы
• GOJEK: включает все продажи в период активной рекламы
• Сравнение с рынком: только рестораны с полными рекламными данными
"""

def generate_data_limitations():
    """Генерирует стандартный блок ограничений данных"""
    
    return """
🚨 ОГРАНИЧЕНИЯ ДАННЫХ:
• 📊 Воронка продаж: только GRAB (GOJEK API не предоставляет показы/клики)
• 💰 Финансовые метрики: GRAB + GOJEK (продажи, бюджеты, ROAS полностью доступны)
• 🏆 Сравнения с рынком: 54 из 59 ресторанов (у 5 нет рекламных данных)
• 📈 Тренды: данные доступны с разных дат для разных ресторанов

⚠️ МЕТОДОЛОГИЯ РАСЧЕТОВ:
• Заказы: включены все заказы из систем платформ
• Средний чек: общие продажи ÷ общие заказы
• ROAS: атрибуционная модель (GRAB точная, GOJEK периодная)
"""

def generate_methodology_note(metric_type):
    """Возвращает объяснение методики для конкретной метрики"""
    
    methodologies = {
        'roas': 'GRAB считает прямые продажи от рекламы, GOJEK - все продажи в период рекламы',
        'conversion': 'Воронка доступна только для GRAB из-за ограничений API GOJEK',
        'demographics': 'Демографические данные предоставляет только GRAB',
        'benchmark': 'Сравнение только с ресторанами, имеющими полные рекламные данные (54 из 59)',
        'retention': 'GRAB использует app_user_id, GOJEK - device_id (могут давать разные результаты)',
        'seasonality': 'Туристические данные объединены за 2024-2025 годы'
    }
    
    return methodologies.get(metric_type, 'Методика расчета стандартная для отрасли')

def add_platform_indicators(text, platform):
    """Добавляет визуальные индикаторы платформы"""
    
    indicators = {
        'grab': '📱',
        'gojek': '🛵', 
        'combined': '🎯',
        'limited': '⚠️'
    }
    
    return f"{indicators.get(platform, '📊')} {text}"

def generate_comparison_context(current_value, benchmark_value, metric_name):
    """Генерирует контекст для сравнения с рынком"""
    
    if benchmark_value == 0:
        return f"⚠️ Нет данных для сравнения {metric_name} с рынком"
    
    diff_percent = ((current_value - benchmark_value) / benchmark_value) * 100
    
    if diff_percent > 0:
        status = "🟢 ВЫШЕ"
        arrow = "↗️"
    elif diff_percent < 0:
        status = "🔴 НИЖЕ" 
        arrow = "↘️"
    else:
        status = "🟡 НА УРОВНЕ"
        arrow = "➡️"
    
    return f"""
📊 {metric_name.upper()} vs РЫНОК:
├── Ваш результат: {current_value:.2f}
├── Средний рынок: {benchmark_value:.2f}
└── Отклонение: {status} на {abs(diff_percent):.1f}% {arrow}
"""

# Пример использования
if __name__ == "__main__":
    # Тестируем функции
    roas_breakdown = generate_roas_breakdown(1630617700, 78535833, 2869355600, 63333068)
    print(roas_breakdown)
    
    limitations = generate_data_limitations()
    print(limitations)