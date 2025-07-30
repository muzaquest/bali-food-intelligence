#!/usr/bin/env python3
"""
🎨 СИСТЕМА ЦВЕТОВОГО КОДИРОВАНИЯ ИСТОЧНИКОВ ДАННЫХ
═══════════════════════════════════════════════════════════════════════════════
Обеспечивает визуальную идентификацию источников данных в отчетах
"""

# ANSI цветовые коды для терминала
class Colors:
    # Основные цвета
    RED = '\033[91m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    BLUE = '\033[94m'
    PURPLE = '\033[95m'
    CYAN = '\033[96m'
    WHITE = '\033[97m'
    
    # Фоновые цвета
    BG_RED = '\033[101m'
    BG_GREEN = '\033[102m'
    BG_YELLOW = '\033[103m'
    BG_BLUE = '\033[104m'
    
    # Стили
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'
    ITALIC = '\033[3m'
    
    # Сброс
    RESET = '\033[0m'

# Цветовая схема для платформ
PLATFORM_COLORS = {
    'grab': Colors.GREEN + Colors.BOLD,      # 📱 GRAB - зеленый жирный
    'gojek': Colors.BLUE + Colors.BOLD,      # 🛵 GOJEK - синий жирный  
    'combined': Colors.PURPLE + Colors.BOLD, # 🎯 ОБЩИЙ - фиолетовый жирный
    'limited': Colors.YELLOW + Colors.BOLD,  # ⚠️ ОГРАНИЧЕНО - желтый жирный
    'error': Colors.RED + Colors.BOLD,       # ❌ ОШИБКА - красный жирный
    'success': Colors.GREEN,                 # ✅ УСПЕХ - зеленый
    'warning': Colors.YELLOW,                # ⚠️ ПРЕДУПРЕЖДЕНИЕ - желтый
    'info': Colors.CYAN                      # ℹ️ ИНФОРМАЦИЯ - голубой
}

def colorize_text(text, color_key):
    """Окрашивает текст в соответствии с типом данных"""
    color = PLATFORM_COLORS.get(color_key, '')
    return f"{color}{text}{Colors.RESET}"

def colorize_platform_data(text, platform):
    """Окрашивает данные в зависимости от платформы"""
    if 'GRAB' in text:
        return colorize_text(text, 'grab')
    elif 'GOJEK' in text:
        return colorize_text(text, 'gojek')
    elif 'ОБЩИЙ' in text or 'ИТОГО' in text:
        return colorize_text(text, 'combined')
    elif 'только' in text.lower() or 'ограничен' in text.lower():
        return colorize_text(text, 'limited')
    else:
        return text

def generate_colored_roas_breakdown(grab_sales, grab_spend, gojek_sales, gojek_spend):
    """Генерирует цветную разбивку ROAS"""
    
    grab_roas = grab_sales / grab_spend if grab_spend > 0 else 0
    gojek_roas = gojek_sales / gojek_spend if gojek_spend > 0 else 0
    total_roas = (grab_sales + gojek_sales) / (grab_spend + gojek_spend) if (grab_spend + gojek_spend) > 0 else 0
    
    grab_line = colorize_text(f"├── 📱 GRAB: {grab_roas:.2f}x (продажи: {grab_sales:,.0f} IDR / бюджет: {grab_spend:,.0f} IDR)", 'grab')
    gojek_line = colorize_text(f"├── 🛵 GOJEK: {gojek_roas:.2f}x (продажи: {gojek_sales:,.0f} IDR / бюджет: {gojek_spend:,.0f} IDR)", 'gojek')
    total_line = colorize_text(f"└── 🎯 ОБЩИЙ: {total_roas:.2f}x (продажи: {grab_sales + gojek_sales:,.0f} IDR / бюджет: {grab_spend + gojek_spend:,.0f} IDR)", 'combined')
    
    methodology = colorize_text("⚠️ МЕТОДИКА:", 'warning')
    grab_method = colorize_text("• GRAB: только прямые продажи от рекламных кампаний (поле ads_sales)", 'grab')
    gojek_method = colorize_text("• GOJEK: только прямые продажи от рекламных кампаний (поле ads_sales)", 'gojek')
    benchmark = colorize_text("• Обе платформы используют одинаковую логику расчета ROAS", 'info')
    
    return f"""
🎯 ROAS АНАЛИЗ:
{grab_line}
{gojek_line}
{total_line}

{methodology} 
{grab_method}
{gojek_method}
{benchmark}
"""

def generate_colored_limitations():
    """Генерирует цветной блок ограничений"""
    
    title = colorize_text("🚨 ОГРАНИЧЕНИЯ ДАННЫХ:", 'error')
    funnel = colorize_text("• 📊 Воронка продаж: только GRAB (GOJEK API не предоставляет показы/клики)", 'limited')
    demo = colorize_text("• 👥 Демографика клиентов: не предоставляется платформами", 'limited')
    finance = colorize_text("• 💰 Все финансовые метрики: GRAB + GOJEK (продажи, бюджеты, ROAS)", 'success')
    benchmark = colorize_text("• 🏆 Сравнения с рынком: 54 из 59 ресторанов (у 5 нет рекламных данных)", 'warning')
    trends = colorize_text("• 📈 Тренды: данные доступны с разных дат для разных ресторанов", 'info')
    
    return f"""
{title}
{funnel}
{demo}
{finance}
{benchmark}
{trends}
"""

def generate_colored_benchmark_comparison(current_value, benchmark_value, metric_name):
    """Генерирует цветное сравнение с бенчмарком"""
    
    if benchmark_value == 0:
        return colorize_text(f"⚠️ Нет данных для сравнения {metric_name} с рынком", 'warning')
    
    diff_percent = ((current_value - benchmark_value) / benchmark_value) * 100
    
    if diff_percent > 0:
        status_color = 'success'
        status_text = "🟢 ВЫШЕ"
        arrow = "↗️"
    elif diff_percent < 0:
        status_color = 'error'
        status_text = "🔴 НИЖЕ" 
        arrow = "↘️"
    else:
        status_color = 'info'
        status_text = "🟡 НА УРОВНЕ"
        arrow = "➡️"
    
    title = colorize_text(f"📊 {metric_name.upper()} vs РЫНОК:", 'info')
    current = colorize_text(f"├── Ваш результат: {current_value:.2f}", 'combined')
    benchmark = colorize_text(f"├── Средний рынок: {benchmark_value:.2f}", 'info')
    difference = colorize_text(f"└── Отклонение: {status_text} на {abs(diff_percent):.1f}% {arrow}", status_color)
    
    return f"""
{title}
{current}
{benchmark}
{difference}
"""

def add_platform_color_indicators():
    """Добавляет легенду цветового кодирования"""
    
    legend_title = colorize_text("🎨 ЦВЕТОВАЯ ЛЕГЕНДА:", 'info')
    grab_legend = colorize_text("📱 GRAB данные", 'grab')
    gojek_legend = colorize_text("🛵 GOJEK данные", 'gojek')
    combined_legend = colorize_text("🎯 Объединенные данные", 'combined')
    limited_legend = colorize_text("⚠️ Ограниченные данные", 'limited')
    
    return f"""
{legend_title}
{grab_legend} | {gojek_legend} | {combined_legend} | {limited_legend}
"""

# Функция для проверки поддержки цветов в терминале
def supports_color():
    """Проверяет поддерживает ли терминал цвета"""
    import os
    return hasattr(os.sys.stdout, 'isatty') and os.sys.stdout.isatty()

# Пример использования
if __name__ == "__main__":
    print("🎨 ТЕСТИРОВАНИЕ ЦВЕТОВОГО КОДИРОВАНИЯ")
    print("=" * 50)
    
    # Тест цветной разбивки ROAS
    colored_roas = generate_colored_roas_breakdown(1630617700, 78535833, 2869355600, 63333068)
    print(colored_roas)
    
    # Тест цветных ограничений
    colored_limitations = generate_colored_limitations()
    print(colored_limitations)
    
    # Тест легенды
    legend = add_platform_color_indicators()
    print(legend)