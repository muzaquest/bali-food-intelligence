#!/usr/bin/env python3
"""
Быстрое исправление анализа целевой аудитории
"""

import sqlite3
import pandas as pd

def quick_target_audience_analysis(restaurant_name):
    """Быстрый анализ целевой аудитории с учетом среднего чека"""
    
    try:
        conn = sqlite3.connect('database.sqlite')
        
        # Получаем ID ресторана
        restaurant_query = "SELECT id, name FROM restaurants WHERE LOWER(name) LIKE ?"
        restaurant_result = pd.read_sql_query(restaurant_query, conn, params=[f'%{restaurant_name.lower()}%'])
        
        if restaurant_result.empty:
            return f"❌ Ресторан '{restaurant_name}' не найден"
            
        restaurant_id = int(restaurant_result.iloc[0]['id'])
        actual_name = restaurant_result.iloc[0]['name']
        
        # Получаем средний чек и общие данные
        analysis_query = """
            SELECT 
                AVG(CASE WHEN orders > 0 THEN sales/orders ELSE 0 END) as avg_check,
                SUM(sales) as total_sales,
                COUNT(DISTINCT strftime('%Y-%m', stat_date)) as months_count,
                strftime('%Y-%m', MIN(stat_date)) as period_start,
                strftime('%Y-%m', MAX(stat_date)) as period_end
            FROM (
                SELECT stat_date, sales, orders FROM grab_stats WHERE restaurant_id = ? AND sales > 0
                UNION ALL
                SELECT stat_date, sales, orders FROM gojek_stats WHERE restaurant_id = ? AND sales > 0
            )
        """
        
        data = pd.read_sql_query(analysis_query, conn, params=[restaurant_id, restaurant_id])
        
        # Анализ сезонности - зимние vs летние месяцы
        seasonal_query = """
            SELECT 
                CAST(strftime('%m', stat_date) AS INTEGER) as month_num,
                AVG(sales) as avg_monthly_sales
            FROM (
                SELECT stat_date, sales FROM grab_stats WHERE restaurant_id = ? AND sales > 0
                UNION ALL
                SELECT stat_date, sales FROM gojek_stats WHERE restaurant_id = ? AND sales > 0
            )
            GROUP BY CAST(strftime('%m', stat_date) AS INTEGER)
        """
        
        seasonal_data = pd.read_sql_query(seasonal_query, conn, params=[restaurant_id, restaurant_id])
        conn.close()
        
        if data.empty:
            return f"❌ Нет данных для анализа {actual_name}"
        
        avg_check = data.iloc[0]['avg_check']
        total_sales = data.iloc[0]['total_sales']
        months_count = data.iloc[0]['months_count']
        period = f"{data.iloc[0]['period_start']} - {data.iloc[0]['period_end']}"
        
        # Анализируем сезонность
        winter_months = seasonal_data[seasonal_data['month_num'].isin([12, 1, 2])]
        summer_months = seasonal_data[seasonal_data['month_num'].isin([6, 7, 8])]
        
        seasonality_text = "Недостаточно данных для анализа сезонности"
        target_audience = "Неопределено"
        confidence = "низкая"
        evidence = []
        recommendations = []
        
        # Определяем целевую аудиторию по среднему чеку
        if avg_check >= 350000:  # 350k+ IDR
            check_category = "Премиум туристы"
            target_audience = "Российские туристы"
            confidence = "очень высокая"
            evidence = [
                f"Очень высокий средний чек {avg_check:,.0f} IDR",
                "Премиум сегмент характерен для русских туристов на Бали"
            ]
            recommendations = [
                "Русскоязычное меню и персонал",
                "Маркетинг в русскоязычных соцсетях",
                "Акции в зимний период (пик русского туризма)",
                "Учет российских праздников и традиций"
            ]
        elif avg_check >= 300000:  # 300-350k IDR
            check_category = "Туристы среднего класса"
            target_audience = "Российские туристы"
            confidence = "высокая"
            evidence = [
                f"Высокий средний чек {avg_check:,.0f} IDR",
                "Туристический уровень цен характерен для русской ЦА"
            ]
            recommendations = [
                "Русскоязычное меню и персонал",
                "Маркетинг в русскоязычных соцсетях",
                "Акции в зимний период"
            ]
        elif avg_check >= 200000:  # 200-300k IDR
            check_category = "Смешанная аудитория"
            target_audience = "Туристы + местные жители"
            confidence = "средняя"
            evidence = [f"Средний чек {avg_check:,.0f} IDR указывает на смешанную аудиторию"]
            recommendations = ["Мультиязычное обслуживание", "Гибкая ценовая политика"]
        else:  # <200k IDR
            check_category = "Местные жители"
            target_audience = "Местные жители"
            confidence = "высокая"
            evidence = [f"Низкий средний чек {avg_check:,.0f} IDR характерен для местных"]
            recommendations = ["Локальный маркетинг", "Программы лояльности", "Индонезийская кухня"]
        
        # Добавляем анализ сезонности если данные есть
        if len(winter_months) > 0 and len(summer_months) > 0:
            winter_avg = winter_months['avg_monthly_sales'].mean()
            summer_avg = summer_months['avg_monthly_sales'].mean()
            
            if winter_avg > summer_avg * 1.15:
                seasonality_text = f"Зимний пик (+{((winter_avg/summer_avg-1)*100):.1f}%) подтверждает российскую ЦА"
                if "Российские туристы" not in target_audience:
                    target_audience = "Российские туристы (по сезонности)"
                    confidence = "высокая"
                evidence.append(seasonality_text)
            elif summer_avg > winter_avg * 1.15:
                seasonality_text = f"Летний пик (+{((summer_avg/winter_avg-1)*100):.1f}%) указывает на европейских туристов"
                if avg_check >= 300000:
                    target_audience = "Европейские туристы"
                    evidence.append(seasonality_text)
            else:
                seasonality_text = "Стабильность по сезонам"
        
        # Формируем ответ
        result = f"""
🎯 **УЛУЧШЕННЫЙ АНАЛИЗ ЦЕЛЕВОЙ АУДИТОРИИ**

🏪 **Ресторан:** {actual_name}
📅 **Период:** {period}
💰 **Общие продажи:** {total_sales:,.0f} IDR
📊 **Месяцев данных:** {months_count}

💳 **АНАЛИЗ СРЕДНЕГО ЧЕКА:**
═══════════════════════════
💰 **Средний чек:** {avg_check:,.0f} IDR
🎯 **Категория:** {check_category}

🌡️ **АНАЛИЗ СЕЗОННОСТИ:**
═══════════════════════════
📊 **Паттерн:** {seasonality_text}

🎯 **ЦЕЛЕВАЯ АУДИТОРИЯ:**
═══════════════════════════
👥 **Основная ЦА:** {target_audience}
🎪 **Уверенность:** {confidence}

📋 **ДОКАЗАТЕЛЬСТВА:**
"""
        
        for evidence_item in evidence:
            result += f"   ✅ {evidence_item}\n"
        
        result += f"""
💡 **РЕКОМЕНДАЦИИ:**
"""
        
        for recommendation in recommendations:
            result += f"   🚀 {recommendation}\n"
        
        return result
        
    except Exception as e:
        return f"❌ Ошибка анализа: {e}"

if __name__ == "__main__":
    # Тестирование
    print("🧪 Тест 1: Ika Canggu")
    result1 = quick_target_audience_analysis("Ika Canggu")
    print(result1)
    
    print("\n" + "="*70 + "\n")
    
    print("🧪 Тест 2: Ika Kero")
    result2 = quick_target_audience_analysis("Ika Kero")
    print(result2)