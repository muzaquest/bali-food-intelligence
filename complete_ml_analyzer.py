#!/usr/bin/env python3
"""
🤖 ПОЛНЫЙ ML АНАЛИЗ СО ВСЕМИ ДАННЫМИ
═══════════════════════════════════════════════════════════════════════════════
Использует ВСЕ 35 колонок из gojek_stats + 32 колонки из grab_stats
Обучается на ВСЕХ доступных факторах из базы данных
"""

import sqlite3
import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import r2_score, mean_absolute_error
from sklearn.preprocessing import StandardScaler
import json
import warnings
warnings.filterwarnings('ignore')

class CompleteMLAnalyzer:
    """ML анализ с использованием ВСЕХ данных"""
    
    def __init__(self, db_path='database.sqlite'):
        self.db_path = db_path
        self.complete_ml_thresholds = {}
        self.complete_correlations = {}
        self.complete_feature_importance = {}
        self.trained_model = None
        self.scaler = StandardScaler()
        
    def analyze_complete_dataset(self):
        """Анализ ПОЛНОГО датасета со всеми факторами"""
        
        print("🤖 ПОЛНЫЙ ML АНАЛИЗ ВСЕХ ДАННЫХ")
        print("=" * 70)
        
        # Загружаем ВСЕ данные со всеми колонками
        complete_data = self._load_complete_data()
        print(f"📊 Загружено {len(complete_data)} записей с {len(complete_data.columns)} колонками")
        
        if len(complete_data) < 100:
            print("❌ Недостаточно данных для анализа")
            return False
            
        # Показываем все доступные факторы
        self._show_all_factors(complete_data)
        
        # Анализируем корреляции ВСЕХ факторов
        self._analyze_all_correlations(complete_data)
        
        # Обучаем модель на ВСЕХ факторах
        self._train_complete_model(complete_data)
        
        # Сохраняем полные результаты
        self._save_complete_insights()
        
        return True
        
    def _load_complete_data(self):
        """Загружает ВСЕ доступные данные"""
        
        conn = sqlite3.connect(self.db_path)
        
        # МЕГА-ЗАПРОС со ВСЕМИ колонками
        query = """
        SELECT 
            g.stat_date,
            r.name as restaurant_name,
            
            -- ========== ПРОДАЖИ И ЗАКАЗЫ ==========
            COALESCE(g.sales, 0) as grab_sales,
            COALESCE(gj.sales, 0) as gojek_sales,
            COALESCE(g.sales, 0) + COALESCE(gj.sales, 0) as total_sales,
            COALESCE(g.orders, 0) as grab_orders,
            COALESCE(gj.orders, 0) as gojek_orders,
            COALESCE(g.orders, 0) + COALESCE(gj.orders, 0) as total_orders,
            
            -- ========== СРЕДНИЙ ЧЕК ==========
            CASE WHEN COALESCE(g.orders, 0) > 0 
                 THEN COALESCE(g.sales, 0) / COALESCE(g.orders, 0)
                 ELSE 0 END as grab_aov,
            CASE WHEN COALESCE(gj.orders, 0) > 0 
                 THEN COALESCE(gj.sales, 0) / COALESCE(gj.orders, 0)
                 ELSE 0 END as gojek_aov,
            CASE WHEN (COALESCE(g.orders, 0) + COALESCE(gj.orders, 0)) > 0 
                 THEN (COALESCE(g.sales, 0) + COALESCE(gj.sales, 0)) / (COALESCE(g.orders, 0) + COALESCE(gj.orders, 0))
                 ELSE 0 END as total_aov,
            
            -- ========== РЕЙТИНГИ ==========
            COALESCE(g.rating, 4.5) as grab_rating,
            COALESCE(gj.rating, 4.5) as gojek_rating,
            (COALESCE(g.rating, 4.5) + COALESCE(gj.rating, 4.5)) / 2 as avg_rating,
            
            -- ========== МАРКЕТИНГ ==========
            COALESCE(g.ads_spend, 0) as grab_ads_spend,
            COALESCE(gj.ads_spend, 0) as gojek_ads_spend,
            COALESCE(g.ads_spend, 0) + COALESCE(gj.ads_spend, 0) as total_ads_spend,
            COALESCE(g.ads_sales, 0) as grab_ads_sales,
            COALESCE(gj.ads_sales, 0) as gojek_ads_sales,
            COALESCE(g.ads_sales, 0) + COALESCE(gj.ads_sales, 0) as total_ads_sales,
            COALESCE(g.ads_orders, 0) as grab_ads_orders,
            COALESCE(gj.ads_orders, 0) as gojek_ads_orders,
            COALESCE(g.ads_orders, 0) + COALESCE(gj.ads_orders, 0) as total_ads_orders,
            
            -- ROAS
            CASE WHEN COALESCE(g.ads_spend, 0) > 0
                 THEN COALESCE(g.ads_sales, 0) / COALESCE(g.ads_spend, 0)
                 ELSE 0 END as grab_roas,
            CASE WHEN COALESCE(gj.ads_spend, 0) > 0
                 THEN COALESCE(gj.ads_sales, 0) / COALESCE(gj.ads_spend, 0)
                 ELSE 0 END as gojek_roas,
            CASE WHEN (COALESCE(g.ads_spend, 0) + COALESCE(gj.ads_spend, 0)) > 0
                 THEN (COALESCE(g.ads_sales, 0) + COALESCE(gj.ads_sales, 0)) / (COALESCE(g.ads_spend, 0) + COALESCE(gj.ads_spend, 0))
                 ELSE 0 END as total_roas,
            
            -- ========== ОПЕРАЦИОННЫЕ ПРОБЛЕМЫ ==========
            COALESCE(g.store_is_closed, 0) as grab_closed,
            COALESCE(gj.store_is_closed, 0) as gojek_closed,
            COALESCE(g.store_is_busy, 0) as grab_busy,
            COALESCE(gj.store_is_busy, 0) as gojek_busy,
            COALESCE(g.out_of_stock, 0) as grab_out_of_stock,
            COALESCE(gj.out_of_stock, 0) as gojek_out_of_stock,
            COALESCE(g.cancelled_orders, 0) as grab_cancelled,
            COALESCE(gj.cancelled_orders, 0) as gojek_cancelled,
            
            -- ========== ВРЕМЯ ДОСТАВКИ ==========
            COALESCE(gj.accepting_time, 0) as accepting_time,
            COALESCE(gj.preparation_time, 0) as preparation_time,
            COALESCE(gj.delivery_time, 0) as delivery_time,
            
            -- ========== КОНВЕРСИЯ И ЭФФЕКТИВНОСТЬ ==========
            COALESCE(gj.lost_orders, 0) as lost_orders,
            COALESCE(gj.realized_orders_percentage, 100) as realized_orders_percentage,
            COALESCE(gj.accepted_orders, 0) as accepted_orders,
            COALESCE(gj.incoming_orders, 0) as incoming_orders,
            COALESCE(gj.marked_ready, 0) as marked_ready,
            COALESCE(g.offline_rate, 0) as offline_rate,
            COALESCE(g.cancelation_rate, 0) as cancelation_rate,
            
            -- ========== ДЕТАЛЬНЫЕ РЕЙТИНГИ ==========
            COALESCE(gj.one_star_ratings, 0) as one_star,
            COALESCE(gj.two_star_ratings, 0) as two_star,
            COALESCE(gj.three_star_ratings, 0) as three_star,
            COALESCE(gj.four_star_ratings, 0) as four_star,
            COALESCE(gj.five_star_ratings, 0) as five_star,
            
            -- ========== КЛИЕНТСКАЯ БАЗА ==========
            COALESCE(g.new_customers, 0) as grab_new_customers,
            COALESCE(gj.new_client, 0) as gojek_new_customers,
            COALESCE(g.repeated_customers, 0) as grab_repeated_customers,
            COALESCE(gj.returned_client, 0) as gojek_repeated_customers,
            COALESCE(g.reactivated_customers, 0) as reactivated_customers,
            COALESCE(g.total_customers, 0) as total_customers,
            COALESCE(gj.active_client, 0) as active_clients,
            
            -- ========== МАРКЕТИНГОВАЯ ЭФФЕКТИВНОСТЬ ==========
            COALESCE(g.ads_ctr, 0) as ads_ctr,
            COALESCE(g.impressions, 0) as impressions,
            COALESCE(g.unique_impressions_reach, 0) as unique_impressions,
            COALESCE(g.unique_menu_visits, 0) as menu_visits,
            COALESCE(g.unique_add_to_carts, 0) as add_to_carts,
            COALESCE(g.unique_conversion_reach, 0) as conversions,
            
            -- ========== ВРЕМЕННЫЕ ПРИЗНАКИ ==========
            CAST(strftime('%w', g.stat_date) AS INTEGER) as day_of_week,
            CAST(strftime('%m', g.stat_date) AS INTEGER) as month,
            CAST(strftime('%j', g.stat_date) AS INTEGER) as day_of_year,
            CAST(strftime('%W', g.stat_date) AS INTEGER) as week_of_year,
            
            -- ========== ФИНАНСЫ ==========
            COALESCE(g.earned_new_customers, 0) as earned_new,
            COALESCE(g.earned_repeated_customers, 0) as earned_repeated,
            COALESCE(g.earned_reactivated_customers, 0) as earned_reactivated,
            COALESCE(g.payouts, 0) as grab_payouts,
            COALESCE(gj.payouts, 0) as gojek_payouts
            
        FROM grab_stats g
        LEFT JOIN gojek_stats gj ON g.restaurant_id = gj.restaurant_id 
                                 AND g.stat_date = gj.stat_date
        LEFT JOIN restaurants r ON g.restaurant_id = r.id
        WHERE g.stat_date >= '2023-01-01'
        AND r.name IS NOT NULL
        AND (g.sales > 0 OR gj.sales > 0)
        ORDER BY g.stat_date
        """
        
        df = pd.read_sql_query(query, conn)
        conn.close()
        
        return df
        
    def _show_all_factors(self, data):
        """Показывает все доступные факторы"""
        
        print("\n📊 ВСЕ ФАКТОРЫ В АНАЛИЗЕ:")
        print("=" * 50)
        
        factor_groups = {
            'ПРОДАЖИ': ['grab_sales', 'gojek_sales', 'total_sales'],
            'ЗАКАЗЫ': ['grab_orders', 'gojek_orders', 'total_orders'],
            'СРЕДНИЙ ЧЕК': ['grab_aov', 'gojek_aov', 'total_aov'],
            'РЕЙТИНГИ': ['grab_rating', 'gojek_rating', 'avg_rating'],
            'МАРКЕТИНГ': ['grab_ads_spend', 'gojek_ads_spend', 'total_ads_spend', 'grab_roas', 'gojek_roas', 'total_roas'],
            'ОПЕРАЦИОННЫЕ': ['grab_closed', 'gojek_closed', 'grab_busy', 'gojek_busy', 'grab_out_of_stock', 'gojek_out_of_stock'],
            'ВРЕМЯ': ['accepting_time', 'preparation_time', 'delivery_time'],
            'КОНВЕРСИЯ': ['lost_orders', 'realized_orders_percentage', 'accepted_orders', 'incoming_orders'],
            'ДЕТАЛЬНЫЕ РЕЙТИНГИ': ['one_star', 'two_star', 'three_star', 'four_star', 'five_star'],
            'КЛИЕНТЫ': ['grab_new_customers', 'gojek_new_customers', 'grab_repeated_customers', 'gojek_repeated_customers'],
            'МАРКЕТИНГ ДЕТАЛИ': ['ads_ctr', 'impressions', 'unique_impressions', 'menu_visits', 'add_to_carts', 'conversions'],
            'ВРЕМЕННЫЕ': ['day_of_week', 'month', 'day_of_year', 'week_of_year'],
            'ФИНАНСЫ': ['earned_new', 'earned_repeated', 'grab_payouts', 'gojek_payouts']
        }
        
        total_factors = 0
        for group, factors in factor_groups.items():
            available_factors = [f for f in factors if f in data.columns]
            if available_factors:
                print(f"✅ {group}: {len(available_factors)} факторов")
                total_factors += len(available_factors)
                for factor in available_factors[:3]:  # Показываем первые 3
                    non_zero = (data[factor] != 0).sum()
                    print(f"   • {factor}: {non_zero:,} записей с данными")
                if len(available_factors) > 3:
                    print(f"   • ... и еще {len(available_factors) - 3} факторов")
            else:
                print(f"❌ {group}: нет данных")
                
        print(f"\n🎯 ИТОГО ФАКТОРОВ ДЛЯ ML: {total_factors}")
        
    def _analyze_all_correlations(self, data):
        """Анализирует корреляции ВСЕХ факторов"""
        
        print(f"\n🤖 КОРРЕЛЯЦИОННЫЙ АНАЛИЗ ВСЕХ ФАКТОРОВ")
        print("=" * 60)
        
        # Исключаем нечисловые колонки
        numeric_data = data.select_dtypes(include=[np.number])
        
        # Удаляем колонки с константными значениями
        non_constant_cols = []
        for col in numeric_data.columns:
            if numeric_data[col].std() > 0:
                non_constant_cols.append(col)
                
        clean_data = numeric_data[non_constant_cols].dropna()
        print(f"📊 Анализируем {len(clean_data.columns)} числовых факторов")
        
        if 'total_sales' in clean_data.columns:
            # Корреляция с продажами
            correlations = clean_data.corr()['total_sales'].sort_values(key=abs, ascending=False)
            
            print(f"\n🔍 ТОП-20 КОРРЕЛЯЦИЙ С ПРОДАЖАМИ:")
            correlations_list = list(correlations.items())
            for i, (factor, corr) in enumerate(correlations_list[:20], 1):
                if factor != 'total_sales' and abs(corr) > 0.05:
                    strength = self._get_correlation_strength(abs(corr))
                    direction = "+" if corr > 0 else "-"
                    print(f"   {i:2d}. {factor}: {direction}{abs(corr):.3f} ({strength})")
                    self.complete_correlations[factor] = corr
                    
        return clean_data
        
    def _train_complete_model(self, data):
        """Обучает модель на ВСЕХ доступных факторах"""
        
        print(f"\n🤖 ОБУЧЕНИЕ ПОЛНОЙ ML МОДЕЛИ")
        print("=" * 60)
        
        # Подготавливаем данные для ML
        numeric_data = data.select_dtypes(include=[np.number])
        
        # Исключаем целевую переменную и ID колонки
        feature_cols = [col for col in numeric_data.columns 
                       if col not in ['total_sales', 'grab_sales', 'gojek_sales'] 
                       and numeric_data[col].std() > 0]
        
        clean_data = numeric_data[feature_cols + ['total_sales']].dropna()
        
        if len(clean_data) < 100:
            print("❌ Недостаточно данных для обучения")
            return False
            
        X = clean_data[feature_cols].values
        y = clean_data['total_sales'].values
        
        print(f"📊 Обучаем на {len(X)} образцах с {len(feature_cols)} признаками")
        
        # Разделяем данные
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42
        )
        
        # Масштабируем
        X_train_scaled = self.scaler.fit_transform(X_train)
        X_test_scaled = self.scaler.transform(X_test)
        
        # Обучаем модель
        self.trained_model = RandomForestRegressor(
            n_estimators=200,
            max_depth=15,
            random_state=42,
            n_jobs=-1
        )
        
        self.trained_model.fit(X_train_scaled, y_train)
        
        # Оцениваем
        y_pred = self.trained_model.predict(X_test_scaled)
        r2 = r2_score(y_test, y_pred)
        mae = mean_absolute_error(y_test, y_pred)
        
        print(f"📈 КАЧЕСТВО ПОЛНОЙ МОДЕЛИ:")
        print(f"   • R² Score: {r2:.4f} ({'ОТЛИЧНО' if r2 > 0.9 else 'ХОРОШО' if r2 > 0.7 else 'НОРМА'})")
        print(f"   • MAE: {mae:,.0f} IDR")
        
        # Важность признаков
        self.complete_feature_importance = dict(zip(feature_cols, self.trained_model.feature_importances_))
        sorted_importance = sorted(self.complete_feature_importance.items(), key=lambda x: x[1], reverse=True)
        
        print(f"\n🎯 ТОП-15 ВАЖНЫХ ФАКТОРОВ (по ML):")
        for i, (factor, importance) in enumerate(sorted_importance[:15], 1):
            if importance > 0.001:  # Только значимые
                print(f"   {i:2d}. {factor}: {importance:.4f} ({importance*100:.2f}%)")
                
        return True
        
    def _get_correlation_strength(self, corr_value):
        """Определяет силу корреляции"""
        if corr_value >= 0.8:
            return "очень сильная"
        elif corr_value >= 0.6:
            return "сильная"
        elif corr_value >= 0.4:
            return "умеренная"
        elif corr_value >= 0.2:
            return "слабая"
        else:
            return "очень слабая"
            
    def _save_complete_insights(self):
        """Сохраняет полные ML инсайты"""
        
        complete_insights = {
            'complete_correlations': self.complete_correlations,
            'complete_feature_importance': self.complete_feature_importance,
            'model_quality': {
                'trained': self.trained_model is not None,
                'factors_count': len(self.complete_feature_importance)
            }
        }
        
        with open('complete_ml_insights.json', 'w', encoding='utf-8') as f:
            json.dump(complete_insights, f, ensure_ascii=False, indent=2)
            
        print(f"\n💾 Полные ML инсайты сохранены в complete_ml_insights.json")
        
def main():
    """Запуск полного ML анализа"""
    
    print("🚀 ЗАПУСК ПОЛНОГО ML АНАЛИЗА ВСЕХ ДАННЫХ")
    print("=" * 80)
    
    analyzer = CompleteMLAnalyzer()
    success = analyzer.analyze_complete_dataset()
    
    if success:
        print(f"\n🎉 ПОЛНЫЙ ML АНАЛИЗ ЗАВЕРШЕН!")
        print(f"   • Использованы ВСЕ доступные данные из базы")
        print(f"   • Найдены корреляции всех факторов")
        print(f"   • Обучена модель на максимальном количестве признаков")
        print(f"   • Результаты сохранены для использования в аналитике")
    else:
        print(f"❌ Ошибка при выполнении анализа")

if __name__ == "__main__":
    main()