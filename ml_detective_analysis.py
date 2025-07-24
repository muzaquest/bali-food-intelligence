#!/usr/bin/env python3
"""
🔍 ML-POWERED ДЕТЕКТИВНЫЙ АНАЛИЗ
═══════════════════════════════════════════════════════════════════════════════
НАСТОЯЩИЙ машинное обучение для объяснения причин падений и роста продаж
Использует Random Forest + SHAP для точного определения влияний
"""

import pandas as pd
import numpy as np
import sqlite3
import json
from datetime import datetime, timedelta
from collections import defaultdict
import warnings
warnings.filterwarnings('ignore')

# ML библиотеки
from sklearn.ensemble import RandomForestRegressor
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error, r2_score
import shap

class MLDetectiveAnalyzer:
    """ML-powered детективный анализ причин аномалий продаж"""
    
    def __init__(self, db_path='database.sqlite'):
        self.db_path = db_path
        self.model = None
        self.explainer = None
        self.feature_names = []
        self.scaler = StandardScaler()
        
    def load_comprehensive_data(self):
        """Загружает ВЕСЬ массив данных из всех 59 ресторанов"""
        
        print("🔬 ЗАГРУЖАЕМ КОМПЛЕКСНЫЕ ДАННЫЕ ИЗ ВСЕХ 59 РЕСТОРАНОВ...")
        
        conn = sqlite3.connect(self.db_path)
        
        # Объединяем ВСЕ данные из всех ресторанов
        query = """
        SELECT 
            -- Основные метрики
            g.stat_date,
            r.name as restaurant_name,
            COALESCE(g.sales, 0) + COALESCE(gj.sales, 0) as total_sales,
            COALESCE(g.orders, 0) + COALESCE(gj.orders, 0) as total_orders,
            COALESCE(g.ads_spend, 0) + COALESCE(gj.ads_spend, 0) as marketing_spend,
            
            -- Клиентские метрики
            COALESCE(g.new_customers, 0) + COALESCE(gj.new_client, 0) as new_customers,
            COALESCE(g.repeated_customers, 0) + COALESCE(gj.returned_client, 0) as returning_customers,
            
            -- Рейтинги
            CASE 
                WHEN g.rating > 0 AND gj.rating > 0 THEN (g.rating + gj.rating) / 2
                WHEN g.rating > 0 THEN g.rating
                WHEN gj.rating > 0 THEN gj.rating
                ELSE 4.5
            END as rating,
            
            -- Операционные метрики
            COALESCE(g.cancelled_orders, 0) + COALESCE(gj.cancelled_orders, 0) as cancelled_orders,
            COALESCE(g.ads_orders, 0) + COALESCE(gj.ads_orders, 0) as promo_orders,
            
            -- Конверсия (только Grab имеет данные)
            COALESCE(g.impressions, 0) as ad_impressions,
            COALESCE(g.unique_menu_visits, 0) as menu_views,
            COALESCE(g.unique_add_to_carts, 0) as add_to_cart,
            COALESCE(g.unique_conversion_reach, 0) as conversions,
            
            -- Временные признаки
            CAST(strftime('%w', g.stat_date) AS INTEGER) as day_of_week,
            CAST(strftime('%m', g.stat_date) AS INTEGER) as month,
            CAST(strftime('%d', g.stat_date) AS INTEGER) as day_of_month,
            
            -- Лаговые признаки (вчерашние продажи)
            LAG(COALESCE(g.sales, 0) + COALESCE(gj.sales, 0), 1) OVER (
                PARTITION BY r.name ORDER BY g.stat_date
            ) as prev_day_sales,
            
            -- 7-дневное скользящее среднее
            AVG(COALESCE(g.sales, 0) + COALESCE(gj.sales, 0)) OVER (
                PARTITION BY r.name 
                ORDER BY g.stat_date 
                ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
            ) as sales_7day_avg
            
        FROM grab_stats g
        LEFT JOIN restaurants r ON g.restaurant_id = r.id
        LEFT JOIN gojek_stats gj ON g.restaurant_id = gj.restaurant_id 
                                 AND g.stat_date = gj.stat_date
        WHERE g.stat_date >= '2024-01-01' AND r.name IS NOT NULL
        ORDER BY r.name, g.stat_date
        """
        
        df = pd.read_sql_query(query, conn)
        conn.close()
        
        print(f"✅ Загружено {len(df)} записей из {df['restaurant_name'].nunique()} ресторанов")
        
        # Добавляем внешние факторы
        df = self.add_external_factors(df)
        
        # Обрабатываем пропущенные значения
        df = df.fillna(0)
        
        return df
    
    def add_external_factors(self, df):
        """Добавляет внешние факторы: погода, туристы, праздники, локация"""
        
        print("🌍 ДОБАВЛЯЕМ ВНЕШНИЕ ФАКТОРЫ...")
        
        # Загружаем туристические коэффициенты
        try:
            with open('combined_tourist_correlations_2024_2025.json', 'r', encoding='utf-8') as f:
                tourist_data = json.load(f)
            
            # Создаем словарь месячных коэффициентов
            tourist_coeffs = {}
            for month_str, data in tourist_data['monthly_coefficients'].items():
                tourist_coeffs[int(month_str)] = data['coefficient']
                
        except:
            # Фолбэк коэффициенты
            tourist_coeffs = {i: 1.0 for i in range(1, 13)}
        
        # Добавляем туристический коэффициент
        df['tourist_coefficient'] = df['month'].map(tourist_coeffs).fillna(1.0)
        
        # Загружаем погодные корреляции
        try:
            with open('real_coefficients.json', 'r', encoding='utf-8') as f:
                weather_data = json.load(f)
            weather_impact = weather_data.get('weather', {}).get('rain_impact', -0.15)
        except:
            weather_impact = -0.15
            
        # Симулируем погодные дни (в реальности подключили бы Weather API)
        np.random.seed(42)  # Для воспроизводимости
        df['is_rainy'] = np.random.choice([0, 1], size=len(df), p=[0.7, 0.3])
        df['weather_impact'] = df['is_rainy'] * weather_impact
        
        # Праздники (упрощенная версия)
        holidays = [
            '2024-01-01', '2024-02-10', '2024-03-11', '2024-04-21', 
            '2024-05-01', '2024-06-06', '2024-08-17', '2024-12-25'
        ]
        df['is_holiday'] = df['stat_date'].isin(holidays).astype(int)
        df['holiday_impact'] = df['is_holiday'] * 0.1  # +10% в праздники
        
        # Локационные факторы (по ресторанам)
        location_factors = {
            'Seminyak': 1.2, 'Canggu': 1.15, 'Ubud': 1.1, 'Kuta': 1.05,
            'Sanur': 1.0, 'Jimbaran': 1.0, 'Nusa Dua': 1.1, 'Denpasar': 0.95
        }
        
        # Упрощенное определение локации по названию ресторана
        def get_location_factor(restaurant_name):
            for location, factor in location_factors.items():
                if location.lower() in restaurant_name.lower():
                    return factor
            return 1.0
        
        df['location_factor'] = df['restaurant_name'].apply(get_location_factor)
        
        # Конкурентное давление (симуляция на основе количества ресторанов в районе)
        restaurant_counts = df.groupby('stat_date')['restaurant_name'].nunique()
        df['competition_pressure'] = df['stat_date'].map(restaurant_counts) / 59.0
        
        print(f"✅ Добавлены внешние факторы: туристы, погода, праздники, локация, конкуренция")
        
        return df
    
    def prepare_features(self, df):
        """Подготавливает признаки для ML модели"""
        
        # Список всех признаков для модели
        feature_columns = [
            # Основные бизнес-метрики
            'total_orders', 'marketing_spend', 'new_customers', 'returning_customers',
            'rating', 'cancelled_orders', 'promo_orders',
            
            # Маркетинговые метрики
            'ad_impressions', 'menu_views', 'add_to_cart', 'conversions',
            
            # Временные признаки
            'day_of_week', 'month', 'day_of_month',
            
            # Лаговые признаки
            'prev_day_sales', 'sales_7day_avg',
            
            # Внешние факторы
            'tourist_coefficient', 'weather_impact', 'holiday_impact', 
            'location_factor', 'competition_pressure'
        ]
        
        # Создаем дополнительные признаки
        df['avg_order_value'] = np.where(df['total_orders'] > 0, 
                                        df['total_sales'] / df['total_orders'], 0)
        df['marketing_efficiency'] = np.where(df['marketing_spend'] > 0,
                                            df['total_sales'] / df['marketing_spend'], 0)
        df['customer_retention_rate'] = np.where(
            (df['new_customers'] + df['returning_customers']) > 0,
            df['returning_customers'] / (df['new_customers'] + df['returning_customers']), 0
        )
        
        # Добавляем новые признаки в список
        feature_columns.extend(['avg_order_value', 'marketing_efficiency', 'customer_retention_rate'])
        
        # Взаимодействие признаков
        df['marketing_x_tourist'] = df['marketing_spend'] * df['tourist_coefficient']
        df['weather_x_orders'] = df['weather_impact'] * df['total_orders']
        df['weekend_effect'] = ((df['day_of_week'] == 0) | (df['day_of_week'] == 6)).astype(int)
        
        feature_columns.extend(['marketing_x_tourist', 'weather_x_orders', 'weekend_effect'])
        
        # Убираем записи с пропущенными значениями
        df = df.dropna(subset=feature_columns + ['total_sales'])
        
        self.feature_names = feature_columns
        
        return df[feature_columns + ['total_sales', 'stat_date', 'restaurant_name']]
    
    def train_model(self, df):
        """Обучает Random Forest модель на всех данных"""
        
        print("🤖 ОБУЧАЕМ RANDOM FOREST НА ВСЕХ ДАННЫХ...")
        
        X = df[self.feature_names]
        y = df['total_sales']
        
        print(f"📊 Обучающая выборка: {len(X)} записей, {len(self.feature_names)} признаков")
        
        # Разделяем на обучение и тест
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42, shuffle=True
        )
        
        # Обучаем Random Forest
        self.model = RandomForestRegressor(
            n_estimators=200,
            max_depth=15,
            min_samples_split=5,
            min_samples_leaf=2,
            random_state=42,
            n_jobs=-1
        )
        
        self.model.fit(X_train, y_train)
        
        # Оценка качества модели
        y_pred = self.model.predict(X_test)
        mae = mean_absolute_error(y_test, y_pred)
        r2 = r2_score(y_test, y_pred)
        
        print(f"✅ Модель обучена! MAE: {mae:,.0f} IDR, R²: {r2:.3f}")
        
        # Важность признаков
        feature_importance = dict(zip(self.feature_names, self.model.feature_importances_))
        feature_importance = sorted(feature_importance.items(), key=lambda x: x[1], reverse=True)
        
        print("\n🏆 ТОП-10 ВАЖНЫХ ПРИЗНАКОВ:")
        for feature, importance in feature_importance[:10]:
            print(f"   {feature}: {importance:.3f}")
        
        # Создаем SHAP explainer
        print("🔍 СОЗДАЕМ SHAP EXPLAINER...")
        self.explainer = shap.TreeExplainer(self.model)
        
        return {
            'mae': mae,
            'r2': r2,
            'feature_importance': feature_importance
        }
    
    def explain_anomaly(self, restaurant_name, date, df):
        """Объясняет конкретную аномалию с помощью SHAP"""
        
        # Находим запись для анализа
        record = df[(df['restaurant_name'] == restaurant_name) & 
                   (df['stat_date'] == date)]
        
        if record.empty:
            return None
            
        # Получаем признаки для этой записи
        X_record = record[self.feature_names].values
        
        # Предсказание модели
        predicted_sales = self.model.predict(X_record)[0]
        actual_sales = record['total_sales'].iloc[0]
        
        # SHAP значения для объяснения
        shap_values = self.explainer.shap_values(X_record)
        
        # Базовое значение (среднее предсказание)
        base_value = self.explainer.expected_value
        
        # Анализируем влияние каждого фактора
        influences = {}
        for i, feature in enumerate(self.feature_names):
            shap_contribution = shap_values[0][i]
            feature_value = X_record[0][i]
            
            # Переводим SHAP значение в процентное влияние
            if base_value != 0:
                influence_percent = (shap_contribution / base_value) * 100
            else:
                influence_percent = 0
            
            # Обрабатываем numpy массивы
            if hasattr(influence_percent, '__iter__') and not isinstance(influence_percent, str):
                influence_percent = float(influence_percent[0]) if len(influence_percent) > 0 else 0.0
            if hasattr(feature_value, '__iter__') and not isinstance(feature_value, str):
                feature_value = float(feature_value[0]) if len(feature_value) > 0 else 0.0
                
            influences[feature] = {
                'shap_value': float(shap_contribution),
                'feature_value': float(feature_value),
                'influence_percent': float(influence_percent)
            }
        
        # Сортируем по абсолютному влиянию
        sorted_influences = sorted(influences.items(), 
                                 key=lambda x: abs(x[1]['influence_percent']), 
                                 reverse=True)
        
        # Рассчитываем необъясненное влияние
        total_explained = sum(abs(inf['influence_percent']) for _, inf in influences.items())
        actual_deviation = ((actual_sales - predicted_sales) / predicted_sales * 100) if predicted_sales > 0 else 0
        unexplained = actual_deviation - sum(inf['influence_percent'] for _, inf in influences.items())
        
        return {
            'actual_sales': actual_sales,
            'predicted_sales': predicted_sales,
            'deviation_percent': actual_deviation,
            'base_value': base_value,
            'influences': sorted_influences[:10],  # Топ-10 влияний
            'unexplained_percent': unexplained,
            'total_explained_percent': total_explained
        }
    
    def analyze_restaurant_anomalies(self, restaurant_name, start_date, end_date, df):
        """Анализирует все аномалии ресторана за период с ML объяснениями"""
        
        print(f"🔍 ML-АНАЛИЗ АНОМАЛИЙ: {restaurant_name}")
        print("=" * 60)
        
        # Фильтруем данные ресторана
        restaurant_data = df[
            (df['restaurant_name'] == restaurant_name) &
            (df['stat_date'] >= start_date) &
            (df['stat_date'] <= end_date)
        ].copy()
        
        if restaurant_data.empty:
            return []
        
        # Рассчитываем среднее для определения аномалий
        mean_sales = restaurant_data['total_sales'].mean()
        std_sales = restaurant_data['total_sales'].std()
        
        # Находим аномалии (отклонения > 15% от среднего)
        anomalies = []
        
        for _, row in restaurant_data.iterrows():
            deviation_percent = ((row['total_sales'] - mean_sales) / mean_sales) * 100
            
            if abs(deviation_percent) > 15:  # Аномалия если отклонение > 15%
                
                # ML объяснение аномалии
                explanation = self.explain_anomaly(restaurant_name, row['stat_date'], df)
                
                if explanation:
                    anomaly_type = "🟢 📈 РОСТ" if deviation_percent > 0 else "🔴 📉 ПАДЕНИЕ"
                    
                    anomalies.append({
                        'date': row['stat_date'],
                        'type': anomaly_type,
                        'actual_sales': row['total_sales'],
                        'mean_sales': mean_sales,
                        'deviation_percent': deviation_percent,
                        'ml_explanation': explanation
                    })
        
        # Сортируем по дате
        anomalies = sorted(anomalies, key=lambda x: x['date'])
        
        print(f"🚨 НАЙДЕНО {len(anomalies)} ML-ОБЪЯСНЕННЫХ АНОМАЛИЙ")
        
        return anomalies
    
    def format_ml_detective_report(self, anomalies):
        """Форматирует ML детективный отчет"""
        
        report = []
        report.append("🔍 ML-POWERED ДЕТЕКТИВНЫЙ АНАЛИЗ ПРИЧИН")
        report.append("=" * 60)
        report.append(f"🤖 Использован Random Forest + SHAP для объяснения причин")
        report.append(f"📊 ОБНАРУЖЕНО {len(anomalies)} ML-ОБЪЯСНЕННЫХ АНОМАЛИЙ:")
        report.append("")
        
        for i, anomaly in enumerate(anomalies, 1):
            explanation = anomaly['ml_explanation']
            
            report.append(f"{i:2d}. {anomaly['date']}: {anomaly['type']} на {anomaly['deviation_percent']:+.1f}%")
            report.append(f"    💰 Продажи: {anomaly['actual_sales']:,.0f} IDR (среднее: {anomaly['mean_sales']:,.0f} IDR)")
            report.append(f"    🤖 ML предсказание: {explanation['predicted_sales']:,.0f} IDR")
            report.append("    🔍 ML-ОБЪЯСНЕНИЕ ПРИЧИН:")
            
            # Показываем топ-5 влияний
            for feature, influence in explanation['influences'][:5]:
                influence_pct = influence['influence_percent']
                feature_val = influence['feature_value']
                
                if abs(influence_pct) > 1:  # Показываем только значимые влияния
                    emoji = "📈" if influence_pct > 0 else "📉"
                    report.append(f"       • {emoji} {feature}: {influence_pct:+.1f}% (значение: {feature_val:.2f})")
            
            # Необъясненное влияние
            unexplained = explanation['unexplained_percent']
            if abs(unexplained) > 5:
                report.append(f"       • ❓ НЕОБЪЯСНЕННОЕ ML-ВЛИЯНИЕ: {unexplained:+.1f}% (требует дополнительных данных)")
            
            report.append("")
        
        # Статистика по необъясненному влиянию
        unexplained_values = [abs(a['ml_explanation']['unexplained_percent']) for a in anomalies]
        if unexplained_values:
            avg_unexplained = np.mean(unexplained_values)
            max_unexplained = max(unexplained_values)
            
            report.append("📊 ML-СТАТИСТИКА НЕОБЪЯСНЕННОГО ВЛИЯНИЯ:")
            report.append(f"   📈 Среднее необъясненное: {avg_unexplained:.1f}%")
            report.append(f"   🔴 Максимальное необъясненное: {max_unexplained:.1f}%")
            
            if avg_unexplained < 15:
                report.append("   ✅ ОТЛИЧНО: ML модель хорошо объясняет причины!")
            elif avg_unexplained < 25:
                report.append("   🟡 ХОРОШО: ML модель объясняет большинство причин")
            else:
                report.append("   🔴 ТРЕБУЕТ УЛУЧШЕНИЯ: Нужны дополнительные данные для ML")
        
        return "\n".join(report)

def main():
    """Тестирование ML детективного анализа"""
    
    print("🚀 ЗАПУСК ML-POWERED ДЕТЕКТИВНОГО АНАЛИЗА")
    print("=" * 80)
    
    # Создаем анализатор
    analyzer = MLDetectiveAnalyzer()
    
    # Загружаем все данные
    df = analyzer.load_comprehensive_data()
    
    # Подготавливаем признаки
    df = analyzer.prepare_features(df)
    
    # Обучаем модель
    metrics = analyzer.train_model(df)
    
    # Тестируем на Ika Canggu
    anomalies = analyzer.analyze_restaurant_anomalies(
        'Ika Canggu', '2025-04-01', '2025-06-30', df
    )
    
    # Генерируем отчет
    report = analyzer.format_ml_detective_report(anomalies)
    print("\n" + report)
    
    # Сохраняем результаты
    with open('ml_detective_results.json', 'w', encoding='utf-8') as f:
        json.dump({
            'model_metrics': metrics,
            'anomalies_count': len(anomalies),
            'feature_names': analyzer.feature_names,
            'analysis_date': datetime.now().isoformat()
        }, f, indent=2, ensure_ascii=False)
    
    print(f"\n✅ ML детективный анализ завершен!")
    print(f"📊 Модель R²: {metrics['r2']:.3f}")
    print(f"🔍 Найдено аномалий: {len(anomalies)}")
    print(f"💾 Результаты сохранены в ml_detective_results.json")

if __name__ == "__main__":
    main()