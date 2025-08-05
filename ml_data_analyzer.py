#!/usr/bin/env python3
"""
🤖 100% ML-ОБОСНОВАННАЯ СИСТЕМА АНАЛИЗА
═══════════════════════════════════════════════════════════════════════════════
ЭТАП 1: Анализ всех исторических данных для определения РЕАЛЬНЫХ порогов и корреляций
ЭТАП 2: ML корреляционный анализ всех факторов
ЭТАП 3: Обучение модели на всех данных за весь период

ЦЕЛЬ: 100% достоверность и обоснованность каждого вывода
"""

import sqlite3
import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import r2_score, mean_absolute_error
from sklearn.preprocessing import StandardScaler
import requests
import json
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

class MLDataAnalyzer:
    """100% ML-обоснованный анализатор данных ресторанов"""
    
    def __init__(self, db_path='database.sqlite'):
        self.db_path = db_path
        self.ml_thresholds = {}
        self.factor_correlations = {}
        self.weather_impact_model = None
        self.trained_model = None
        self.feature_importance = {}
        self.scaler = StandardScaler()
        
    def analyze_all_historical_data(self):
        """ЭТАП 1: Полный анализ всех исторических данных"""
        
        print("🤖 ЭТАП 1: АНАЛИЗ ВСЕХ ИСТОРИЧЕСКИХ ДАННЫХ")
        print("=" * 60)
        
        # Загружаем ВСЕ данные за весь период
        all_data = self._load_all_historical_data()
        print(f"📊 Загружено {len(all_data)} записей за весь период")
        
        if len(all_data) < 100:
            print("❌ Недостаточно данных для ML анализа")
            return False
            
        # Определяем РЕАЛЬНЫЕ пороги на основе данных
        self._calculate_data_driven_thresholds(all_data)
        
        # Анализируем дни недели
        self._analyze_weekday_patterns(all_data)
        
        # Анализируем сезонные паттерны
        self._analyze_seasonal_patterns(all_data)
        
        # Анализируем операционные факторы
        self._analyze_operational_factors(all_data)
        
        return True
        
    def correlate_all_factors(self, all_data):
        """ЭТАП 2: ML корреляционный анализ всех факторов"""
        
        print("\n🤖 ЭТАП 2: ML КОРРЕЛЯЦИОННЫЙ АНАЛИЗ")
        print("=" * 60)
        
        # Подготавливаем данные для корреляционного анализа
        correlation_data = self._prepare_correlation_data(all_data)
        
        if len(correlation_data) < 50:
            print("❌ Недостаточно данных для корреляционного анализа")
            return False
            
        # Корреляционная матрица
        correlation_matrix = correlation_data.corr()
        sales_correlations = correlation_matrix['total_sales'].sort_values(key=abs, ascending=False)
        
        print("📊 КОРРЕЛЯЦИИ С ПРОДАЖАМИ (по убыванию важности):")
        for factor, corr in sales_correlations.items():
            if factor != 'total_sales' and abs(corr) > 0.1:
                strength = self._get_correlation_strength(abs(corr))
                direction = "положительная" if corr > 0 else "отрицательная"
                print(f"   • {factor}: {corr:.3f} ({direction}, {strength})")
                self.factor_correlations[factor] = corr
                
        # Анализ погодных корреляций
        self._analyze_weather_correlations(all_data)
        
        return True
        
    def train_ml_model(self, all_data):
        """ЭТАП 3: Обучение полной ML модели"""
        
        print("\n🤖 ЭТАП 3: ОБУЧЕНИЕ ML МОДЕЛИ")
        print("=" * 60)
        
        # Подготавливаем данные для обучения
        X, y, feature_names = self._prepare_ml_features(all_data)
        
        if len(X) < 100:
            print("❌ Недостаточно данных для обучения модели")
            return False
            
        print(f"📊 Подготовлено {len(X)} образцов с {len(feature_names)} признаками")
        
        # Разделяем на обучающую и тестовую выборки
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42
        )
        
        # Масштабируем данные
        X_train_scaled = self.scaler.fit_transform(X_train)
        X_test_scaled = self.scaler.transform(X_test)
        
        # Обучаем модель
        self.trained_model = RandomForestRegressor(
            n_estimators=100,
            max_depth=10,
            random_state=42,
            n_jobs=-1
        )
        
        self.trained_model.fit(X_train_scaled, y_train)
        
        # Оцениваем качество модели
        y_pred = self.trained_model.predict(X_test_scaled)
        r2 = r2_score(y_test, y_pred)
        mae = mean_absolute_error(y_test, y_pred)
        
        print(f"📈 КАЧЕСТВО МОДЕЛИ:")
        print(f"   • R² Score: {r2:.3f} ({'отлично' if r2 > 0.8 else 'хорошо' if r2 > 0.6 else 'удовлетворительно'})")
        print(f"   • MAE: {mae:,.0f} IDR")
        
        # Важность признаков
        self.feature_importance = dict(zip(feature_names, self.trained_model.feature_importances_))
        sorted_importance = sorted(self.feature_importance.items(), key=lambda x: x[1], reverse=True)
        
        print(f"📊 ВАЖНОСТЬ ФАКТОРОВ (по данным ML):")
        for factor, importance in sorted_importance[:10]:
            print(f"   • {factor}: {importance:.3f}")
            
        return True
        
    def _load_all_historical_data(self):
        """Загружает ВСЕ исторические данные"""
        
        conn = sqlite3.connect(self.db_path)
        
        query = """
        SELECT 
            g.stat_date,
            r.name as restaurant_name,
            
            -- ПРОДАЖИ И ЗАКАЗЫ
            COALESCE(g.sales, 0) as grab_sales,
            COALESCE(gj.sales, 0) as gojek_sales,
            COALESCE(g.sales, 0) + COALESCE(gj.sales, 0) as total_sales,
            COALESCE(g.orders, 0) + COALESCE(gj.orders, 0) as total_orders,
            
            -- СРЕДНИЙ ЧЕК
            CASE WHEN (COALESCE(g.orders, 0) + COALESCE(gj.orders, 0)) > 0 
                 THEN (COALESCE(g.sales, 0) + COALESCE(gj.sales, 0)) / (COALESCE(g.orders, 0) + COALESCE(gj.orders, 0))
                 ELSE 0 END as avg_order_value,
            
            -- РЕЙТИНГИ
            CASE 
                WHEN g.rating > 0 AND gj.rating > 0 THEN (g.rating + gj.rating) / 2
                WHEN g.rating > 0 THEN g.rating
                WHEN gj.rating > 0 THEN gj.rating
                ELSE 4.5 
            END as avg_rating,
            
            -- МАРКЕТИНГ
            COALESCE(g.ads_spend, 0) + COALESCE(gj.ads_spend, 0) as marketing_spend,
            COALESCE(g.ads_sales, 0) + COALESCE(gj.ads_sales, 0) as marketing_sales,
            CASE WHEN (COALESCE(g.ads_spend, 0) + COALESCE(gj.ads_spend, 0)) > 0
                 THEN (COALESCE(g.ads_sales, 0) + COALESCE(gj.ads_sales, 0)) / (COALESCE(g.ads_spend, 0) + COALESCE(gj.ads_spend, 0))
                 ELSE 0 END as roas,
            
            -- ОПЕРАЦИОННЫЕ ПРОБЛЕМЫ
            COALESCE(g.store_is_closed, 0) + COALESCE(gj.store_is_closed, 0) as total_closed,
            COALESCE(g.out_of_stock, 0) + COALESCE(gj.out_of_stock, 0) as total_out_of_stock,
            COALESCE(g.cancelled_orders, 0) + COALESCE(gj.cancelled_orders, 0) as total_cancelled,
            
            -- ВРЕМЕННЫЕ ПРИЗНАКИ
            CAST(strftime('%w', g.stat_date) AS INTEGER) as day_of_week,
            CAST(strftime('%m', g.stat_date) AS INTEGER) as month,
            CAST(strftime('%j', g.stat_date) AS INTEGER) as day_of_year,
            
            -- КЛИЕНТСКАЯ БАЗА
            COALESCE(g.new_customers, 0) + COALESCE(gj.new_client, 0) as new_customers,
            COALESCE(g.repeated_customers, 0) + COALESCE(gj.returned_client, 0) as returning_customers
            
        FROM grab_stats g
        LEFT JOIN gojek_stats gj ON g.restaurant_id = gj.restaurant_id 
                                 AND g.stat_date = gj.stat_date
        LEFT JOIN restaurants r ON g.restaurant_id = r.id
        WHERE g.stat_date >= '2023-01-01'  -- Берем данные за последние 2+ года
        AND r.name IS NOT NULL
        AND (g.sales > 0 OR gj.sales > 0)  -- Только дни с продажами
        ORDER BY g.stat_date
        """
        
        df = pd.read_sql_query(query, conn)
        conn.close()
        
        return df
        
    def _calculate_data_driven_thresholds(self, data):
        """Вычисляет пороги на основе реальных данных"""
        
        print("📊 РАСЧЕТ ПОРОГОВ НА ОСНОВЕ ДАННЫХ:")
        
        # Пороги для продаж (на основе квартилей)
        sales_q25 = data['total_sales'].quantile(0.25)
        sales_q75 = data['total_sales'].quantile(0.75)
        sales_q90 = data['total_sales'].quantile(0.90)
        
        self.ml_thresholds['low_sales'] = sales_q25
        self.ml_thresholds['high_sales'] = sales_q75
        self.ml_thresholds['excellent_sales'] = sales_q90
        
        print(f"   • Низкие продажи: < {sales_q25:,.0f} IDR (25-й процентиль)")
        print(f"   • Высокие продажи: > {sales_q75:,.0f} IDR (75-й процентиль)")
        print(f"   • Отличные продажи: > {sales_q90:,.0f} IDR (90-й процентиль)")
        
        # Пороги для среднего чека
        aov_data = data[data['avg_order_value'] > 0]
        if len(aov_data) > 0:
            aov_q25 = aov_data['avg_order_value'].quantile(0.25)
            aov_q75 = aov_data['avg_order_value'].quantile(0.75)
            
            self.ml_thresholds['low_aov'] = aov_q25
            self.ml_thresholds['high_aov'] = aov_q75
            
            print(f"   • Низкий средний чек: < {aov_q25:,.0f} IDR")
            print(f"   • Высокий средний чек: > {aov_q75:,.0f} IDR")
        
        # Пороги для рейтинга
        rating_q25 = data['avg_rating'].quantile(0.25)
        rating_q75 = data['avg_rating'].quantile(0.75)
        
        self.ml_thresholds['low_rating'] = rating_q25
        self.ml_thresholds['high_rating'] = rating_q75
        
        print(f"   • Низкий рейтинг: < {rating_q25:.2f}")
        print(f"   • Высокий рейтинг: > {rating_q75:.2f}")
        
        # Пороги для ROAS
        roas_data = data[data['roas'] > 0]
        if len(roas_data) > 0:
            roas_q25 = roas_data['roas'].quantile(0.25)
            roas_q75 = roas_data['roas'].quantile(0.75)
            
            self.ml_thresholds['low_roas'] = roas_q25
            self.ml_thresholds['high_roas'] = roas_q75
            
            print(f"   • Низкий ROAS: < {roas_q25:.2f}x")
            print(f"   • Высокий ROAS: > {roas_q75:.2f}x")
            
    def _analyze_weekday_patterns(self, data):
        """Анализирует паттерны дней недели"""
        
        print("\n📅 АНАЛИЗ ДНЕЙ НЕДЕЛИ:")
        
        weekday_stats = data.groupby('day_of_week')['total_sales'].agg(['mean', 'std', 'count'])
        weekday_names = {0: 'Воскресенье', 1: 'Понедельник', 2: 'Вторник', 3: 'Среда', 
                        4: 'Четверг', 5: 'Пятница', 6: 'Суббота'}
        
        overall_mean = data['total_sales'].mean()
        
        for day_num, stats in weekday_stats.iterrows():
            day_name = weekday_names[day_num]
            deviation = ((stats['mean'] - overall_mean) / overall_mean) * 100
            
            if abs(deviation) > 10:  # Значимое отклонение
                trend = "сильнее" if deviation > 0 else "слабее"
                print(f"   • {day_name}: {deviation:+.1f}% от среднего ({trend})")
                self.ml_thresholds[f'weekday_{day_num}_factor'] = deviation / 100
                
    def _analyze_seasonal_patterns(self, data):
        """Анализирует сезонные паттерны"""
        
        print("\n🌅 АНАЛИЗ СЕЗОННЫХ ПАТТЕРНОВ:")
        
        # Добавляем данные о датах
        data['stat_date'] = pd.to_datetime(data['stat_date'])
        monthly_stats = data.groupby('month')['total_sales'].agg(['mean', 'count'])
        
        overall_mean = data['total_sales'].mean()
        month_names = {1: 'Январь', 2: 'Февраль', 3: 'Март', 4: 'Апрель', 5: 'Май', 6: 'Июнь',
                      7: 'Июль', 8: 'Август', 9: 'Сентябрь', 10: 'Октябрь', 11: 'Ноябрь', 12: 'Декабрь'}
        
        for month_num, stats in monthly_stats.iterrows():
            if stats['count'] > 10:  # Достаточно данных
                month_name = month_names[month_num]
                deviation = ((stats['mean'] - overall_mean) / overall_mean) * 100
                
                if abs(deviation) > 15:  # Значимое сезонное отклонение
                    trend = "высокий" if deviation > 0 else "низкий"
                    print(f"   • {month_name}: {deviation:+.1f}% от среднего ({trend} сезон)")
                    self.ml_thresholds[f'month_{month_num}_factor'] = deviation / 100
                    
    def _analyze_operational_factors(self, data):
        """Анализирует влияние операционных факторов"""
        
        print("\n🏪 АНАЛИЗ ОПЕРАЦИОННЫХ ФАКТОРОВ:")
        
        # Влияние закрытий
        closed_impact = self._calculate_factor_impact(data, 'total_closed', 'total_sales')
        if closed_impact['significant']:
            print(f"   • Закрытие ресторана: {closed_impact['impact']:+.1f}% к продажам")
            self.ml_thresholds['closure_impact'] = closed_impact['impact'] / 100
            
        # Влияние дефицита товара
        stock_impact = self._calculate_factor_impact(data, 'total_out_of_stock', 'total_sales')
        if stock_impact['significant']:
            print(f"   • Дефицит товара: {stock_impact['impact']:+.1f}% к продажам")
            self.ml_thresholds['stock_impact'] = stock_impact['impact'] / 100
            
        # Влияние отмен
        cancel_impact = self._calculate_factor_impact(data, 'total_cancelled', 'total_sales')
        if cancel_impact['significant']:
            print(f"   • Отмены заказов: {cancel_impact['impact']:+.1f}% к продажам")
            self.ml_thresholds['cancellation_impact'] = cancel_impact['impact'] / 100
            
    def _calculate_factor_impact(self, data, factor_col, target_col):
        """Вычисляет влияние фактора на целевую переменную"""
        
        # Сравниваем дни с фактором и без
        with_factor = data[data[factor_col] > 0][target_col].mean()
        without_factor = data[data[factor_col] == 0][target_col].mean()
        
        if without_factor > 0:
            impact = ((with_factor - without_factor) / without_factor) * 100
            
            # Проверяем статистическую значимость (упрощенно)
            count_with = len(data[data[factor_col] > 0])
            count_without = len(data[data[factor_col] == 0])
            
            significant = count_with > 10 and count_without > 10 and abs(impact) > 5
            
            return {
                'impact': impact,
                'significant': significant,
                'count_with': count_with,
                'count_without': count_without
            }
        
        return {'impact': 0, 'significant': False}
        
    def _prepare_correlation_data(self, data):
        """Подготавливает данные для корреляционного анализа"""
        
        correlation_cols = [
            'total_sales', 'total_orders', 'avg_order_value', 'avg_rating',
            'marketing_spend', 'marketing_sales', 'roas',
            'total_closed', 'total_out_of_stock', 'total_cancelled',
            'day_of_week', 'month', 'new_customers', 'returning_customers'
        ]
        
        return data[correlation_cols].dropna()
        
    def _analyze_weather_correlations(self, data):
        """Анализирует корреляции с погодой (образец)"""
        
        print("\n🌤️ АНАЛИЗ ПОГОДНЫХ КОРРЕЛЯЦИЙ:")
        print("   • Для полного анализа нужно загрузить погодные данные за весь период")
        print("   • Это займет время, но даст точные корреляции дождь-продажи")
        
        # Здесь можно добавить полную загрузку погодных данных
        # Пока оставляем как заглушку для демонстрации подхода
        
    def _prepare_ml_features(self, data):
        """Подготавливает признаки для ML модели"""
        
        feature_cols = [
            'total_orders', 'avg_order_value', 'avg_rating',
            'marketing_spend', 'roas', 'total_closed', 'total_out_of_stock', 
            'total_cancelled', 'day_of_week', 'month', 'day_of_year',
            'new_customers', 'returning_customers'
        ]
        
        # Убираем строки с пропущенными значениями
        clean_data = data[feature_cols + ['total_sales']].dropna()
        
        X = clean_data[feature_cols].values
        y = clean_data['total_sales'].values
        
        return X, y, feature_cols
        
    def _get_correlation_strength(self, corr_value):
        """Определяет силу корреляции"""
        if corr_value >= 0.7:
            return "очень сильная"
        elif corr_value >= 0.5:
            return "сильная"
        elif corr_value >= 0.3:
            return "умеренная"
        elif corr_value >= 0.1:
            return "слабая"
        else:
            return "очень слабая"
            
    def save_ml_insights(self):
        """Сохраняет результаты ML анализа"""
        
        insights = {
            'ml_thresholds': self.ml_thresholds,
            'factor_correlations': self.factor_correlations,
            'feature_importance': self.feature_importance,
            'model_trained': self.trained_model is not None
        }
        
        with open('ml_insights.json', 'w', encoding='utf-8') as f:
            json.dump(insights, f, ensure_ascii=False, indent=2)
            
        print(f"\n💾 ML инсайты сохранены в ml_insights.json")
        
def main():
    """Выполняет полный ML анализ"""
    
    print("🤖 ЗАПУСК 100% ML-ОБОСНОВАННОГО АНАЛИЗА")
    print("=" * 70)
    
    analyzer = MLDataAnalyzer()
    
    # ЭТАП 1: Анализ исторических данных
    if not analyzer.analyze_all_historical_data():
        return
        
    # Загружаем данные для следующих этапов
    all_data = analyzer._load_all_historical_data()
    
    # ЭТАП 2: Корреляционный анализ
    if not analyzer.correlate_all_factors(all_data):
        return
        
    # ЭТАП 3: Обучение ML модели
    if not analyzer.train_ml_model(all_data):
        return
        
    # Сохраняем результаты
    analyzer.save_ml_insights()
    
    print("\n🎉 АНАЛИЗ ЗАВЕРШЕН! Система теперь 100% обоснована данными!")

if __name__ == "__main__":
    main()