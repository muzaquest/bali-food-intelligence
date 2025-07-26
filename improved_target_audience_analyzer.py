#!/usr/bin/env python3
"""
🎯 УЛУЧШЕННЫЙ АНАЛИЗАТОР ЦЕЛЕВОЙ АУДИТОРИИ
═══════════════════════════════════════════════════════════════════════════════
Учитывает средний чек, сезонность и специфику туристов по странам
"""

import sqlite3
import pandas as pd
import numpy as np
from scipy.stats import pearsonr

def analyze_target_audience_improved(restaurant_name):
    """Улучшенный анализ целевой аудитории с учетом среднего чека и сезонности"""
    
    print(f"🎯 УЛУЧШЕННЫЙ АНАЛИЗ ЦЕЛЕВОЙ АУДИТОРИИ: {restaurant_name}")
    print("=" * 70)
    
    try:
        conn = sqlite3.connect('database.sqlite')
        
        # Получаем ID ресторана
        restaurant_query = "SELECT id, name FROM restaurants WHERE LOWER(name) LIKE ?"
        restaurant_result = pd.read_sql_query(restaurant_query, conn, params=[f'%{restaurant_name.lower()}%'])
        
        if restaurant_result.empty:
            return f"❌ Ресторан '{restaurant_name}' не найден"
            
        restaurant_id = int(restaurant_result.iloc[0]['id'])
        actual_name = restaurant_result.iloc[0]['name']
        
        # Получаем детальные данные по месяцам
        detailed_query = """
            SELECT 
                strftime('%Y-%m', stat_date) as month,
                strftime('%Y', stat_date) as year,
                CAST(strftime('%m', stat_date) AS INTEGER) as month_num,
                AVG(CASE WHEN orders > 0 THEN sales/orders ELSE 0 END) as avg_check,
                SUM(sales) as total_sales,
                SUM(orders) as total_orders
            FROM (
                SELECT stat_date, sales, orders FROM grab_stats WHERE restaurant_id = ? AND sales > 0
                UNION ALL
                SELECT stat_date, sales, orders FROM gojek_stats WHERE restaurant_id = ? AND sales > 0
            )
            GROUP BY strftime('%Y-%m', stat_date)
            ORDER BY month
        """
        
        data = pd.read_sql_query(detailed_query, conn, params=[restaurant_id, restaurant_id])
        conn.close()
        
        if data.empty or len(data) < 3:
            return f"❌ Недостаточно данных для анализа {actual_name}"
        
        # 1. АНАЛИЗ СРЕДНЕГО ЧЕКА
        avg_check_analysis = analyze_average_check(data)
        
        # 2. АНАЛИЗ СЕЗОННОСТИ
        seasonality_analysis = analyze_seasonality(data)
        
        # 3. ОПРЕДЕЛЕНИЕ ЦЕЛЕВОЙ АУДИТОРИИ
        target_audience = determine_target_audience_improved(avg_check_analysis, seasonality_analysis, data)
        
        # 4. ФОРМИРОВАНИЕ ОТЧЕТА
        return format_improved_analysis_result(actual_name, data, avg_check_analysis, seasonality_analysis, target_audience)
        
    except Exception as e:
        return f"❌ Ошибка анализа: {e}"

def analyze_average_check(data):
    """Анализ среднего чека для определения типа клиентов"""
    
    avg_checks = data['avg_check'].dropna()
    overall_avg_check = avg_checks.mean()
    
    # Пороги для определения типа аудитории
    if overall_avg_check >= 400000:  # 400k+ IDR
        check_category = "premium_tourists"
        check_description = "Премиум туристы"
        confidence = "очень высокая"
    elif overall_avg_check >= 300000:  # 300-400k IDR
        check_category = "tourists"
        check_description = "Туристы среднего класса"
        confidence = "высокая"
    elif overall_avg_check >= 200000:  # 200-300k IDR
        check_category = "mixed"
        check_description = "Смешанная аудитория"
        confidence = "средняя"
    else:  # <200k IDR
        check_category = "locals"
        check_description = "Местные жители"
        confidence = "высокая"
    
    return {
        'average_check': overall_avg_check,
        'category': check_category,
        'description': check_description,
        'confidence': confidence,
        'trend': analyze_check_trend(data)
    }

def analyze_check_trend(data):
    """Анализирует тренд среднего чека"""
    
    if len(data) < 6:
        return "недостаточно данных"
    
    recent_checks = data.tail(6)['avg_check'].mean()
    early_checks = data.head(6)['avg_check'].mean()
    
    if recent_checks > early_checks * 1.1:
        return "растет (премиумизация)"
    elif recent_checks < early_checks * 0.9:
        return "падает (уход ЦА)"
    else:
        return "стабильный"

def analyze_seasonality(data):
    """Анализ сезонности для определения национальности туристов"""
    
    # Группируем по месяцам
    monthly_data = data.groupby('month_num').agg({
        'total_sales': 'mean',
        'avg_check': 'mean'
    }).reset_index()
    
    # Анализируем зимние vs летние месяцы
    winter_months = [12, 1, 2]  # Пик российских туристов
    summer_months = [6, 7, 8]   # Спад российских туристов
    
    winter_data = data[data['month_num'].isin(winter_months)]
    summer_data = data[data['month_num'].isin(summer_months)]
    
    seasonality_result = {
        'pattern': 'unknown',
        'confidence': 'низкая',
        'description': 'Недостаточно данных'
    }
    
    if len(winter_data) > 0 and len(summer_data) > 0:
        winter_avg = winter_data['total_sales'].mean()
        summer_avg = summer_data['total_sales'].mean()
        
        if winter_avg > summer_avg * 1.15:  # Зима на 15%+ выше
            seasonality_result = {
                'pattern': 'russian_tourists',
                'confidence': 'высокая',
                'description': f'Зимний пик (+{((winter_avg/summer_avg-1)*100):.1f}%) указывает на российских туристов',
                'winter_avg': winter_avg,
                'summer_avg': summer_avg
            }
        elif summer_avg > winter_avg * 1.15:  # Лето на 15%+ выше
            seasonality_result = {
                'pattern': 'european_tourists',
                'confidence': 'высокая', 
                'description': f'Летний пик (+{((summer_avg/winter_avg-1)*100):.1f}%) указывает на европейских туристов',
                'winter_avg': winter_avg,
                'summer_avg': summer_avg
            }
        else:
            seasonality_result = {
                'pattern': 'stable_local',
                'confidence': 'средняя',
                'description': 'Стабильность по сезонам указывает на местную аудиторию',
                'winter_avg': winter_avg,
                'summer_avg': summer_avg
            }
    
    return seasonality_result

def determine_target_audience_improved(avg_check_analysis, seasonality_analysis, data):
    """Определяет целевую аудиторию на основе комплексного анализа"""
    
    check_category = avg_check_analysis['category']
    seasonal_pattern = seasonality_analysis['pattern']
    
    # Логика определения ЦА
    if check_category in ['premium_tourists', 'tourists']:
        if seasonal_pattern == 'russian_tourists':
            return {
                'primary_audience': 'Российские туристы',
                'confidence': 'очень высокая',
                'evidence': [
                    f"Высокий средний чек {avg_check_analysis['average_check']:,.0f} IDR",
                    seasonality_analysis['description'],
                    "Премиум сегмент характерен для русских туристов на Бали"
                ],
                'recommendations': [
                    "Русскоязычное меню и персонал",
                    "Маркетинг в русскоязычных соцсетях",
                    "Акции в зимний период (пик сезона)",
                    "Учет российских праздников и традиций"
                ]
            }
        elif seasonal_pattern == 'european_tourists':
            return {
                'primary_audience': 'Европейские туристы',
                'confidence': 'высокая',
                'evidence': [
                    f"Высокий средний чек {avg_check_analysis['average_check']:,.0f} IDR",
                    seasonality_analysis['description'],
                    "Летний пик характерен для европейцев"
                ],
                'recommendations': [
                    "Англоязычное обслуживание",
                    "Европейская кухня и стандарты",
                    "Маркетинг на лето",
                    "Учет европейских каникул"
                ]
            }
        else:
            return {
                'primary_audience': 'Международные туристы',
                'confidence': 'средняя',
                'evidence': [
                    f"Высокий средний чек {avg_check_analysis['average_check']:,.0f} IDR указывает на туристов",
                    "Сезонность не выражена четко"
                ],
                'recommendations': [
                    "Мультиязычное обслуживание",
                    "Международная кухня",
                    "Гибкий маркетинг"
                ]
            }
    else:
        return {
            'primary_audience': 'Местные жители',
            'confidence': 'высокая',
            'evidence': [
                f"Низкий средний чек {avg_check_analysis['average_check']:,.0f} IDR",
                "Отсутствие выраженной сезонности"
            ],
            'recommendations': [
                "Локальный маркетинг",
                "Программы лояльности",
                "Индонезийская кухня",
                "Акции в будние дни"
            ]
        }

def format_improved_analysis_result(restaurant_name, data, avg_check_analysis, seasonality_analysis, target_audience):
    """Форматирует результат улучшенного анализа"""
    
    total_sales = data['total_sales'].sum()
    period = f"{data['month'].min()} - {data['month'].max()}"
    
    result = f"""
🎯 **УЛУЧШЕННЫЙ АНАЛИЗ ЦЕЛЕВОЙ АУДИТОРИИ**

🏪 **Ресторан:** {restaurant_name}
📅 **Период:** {period}
💰 **Общие продажи:** {total_sales:,.0f} IDR
📊 **Месяцев данных:** {len(data)}

💳 **АНАЛИЗ СРЕДНЕГО ЧЕКА:**
═══════════════════════════
💰 **Средний чек:** {avg_check_analysis['average_check']:,.0f} IDR
🎯 **Категория:** {avg_check_analysis['description']}
📈 **Тренд:** {avg_check_analysis['trend']}
🎪 **Уверенность:** {avg_check_analysis['confidence']}

🌡️ **АНАЛИЗ СЕЗОННОСТИ:**
═══════════════════════════
📊 **Паттерн:** {seasonality_analysis['description']}
🎯 **Уверенность:** {seasonality_analysis['confidence']}
"""
    
    if 'winter_avg' in seasonality_analysis:
        result += f"""
❄️ **Зима:** {seasonality_analysis['winter_avg']:,.0f} IDR
☀️ **Лето:** {seasonality_analysis['summer_avg']:,.0f} IDR
"""
    
    result += f"""
🎯 **ЦЕЛЕВАЯ АУДИТОРИЯ:**
═══════════════════════════
👥 **Основная ЦА:** {target_audience['primary_audience']}
🎪 **Уверенность:** {target_audience['confidence']}

📋 **ДОКАЗАТЕЛЬСТВА:**
"""
    
    for evidence in target_audience['evidence']:
        result += f"   ✅ {evidence}\n"
    
    result += f"""
💡 **РЕКОМЕНДАЦИИ:**
"""
    
    for recommendation in target_audience['recommendations']:
        result += f"   🚀 {recommendation}\n"
    
    return result

if __name__ == "__main__":
    # Тестирование на Ika Kero
    result = analyze_target_audience_improved("Ika Kero")
    print(result)