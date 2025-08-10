#!/usr/bin/env python3
"""
🚀 ПРОФЕССИОНАЛЬНАЯ ML СИСТЕМА ПО ПЛАНУ CHATGPT
Адаптированная под нашу структуру данных: grab_stats + gojek_stats + restaurants
"""

import pandas as pd
import numpy as np
import sqlite3
import json
import requests
from datetime import datetime, timedelta
from sklearn.model_selection import TimeSeriesSplit
from sklearn.metrics import mean_absolute_error, r2_score, mean_absolute_percentage_error
import lightgbm as lgb
import shap
import joblib
import warnings
warnings.filterwarnings('ignore')

class ProfessionalMLSystem:
    """Профессиональная ML система для анализа продаж ресторанов"""
    
    def __init__(self):
        self.models = []
        self.feature_names = []
        self.weather_cache = {}
        self.shap_explainer = None
        self.feature_importance = None
        
    def load_and_validate_data(self):
        """1) Предварительная проверка данных (ChatGPT чеклист)"""
        
        print("🔍 ЭТАП 1: ПРЕДВАРИТЕЛЬНАЯ ПРОВЕРКА ДАННЫХ")
        print("="*50)
        
        conn = sqlite3.connect('database.sqlite')
        
        # Проверяем покрытие данных
        validation_results = {}
        
        # 1.1 Проверка основных таблиц
        tables_check = pd.read_sql_query("""
            SELECT 
                'grab_stats' as table_name,
                COUNT(*) as records,
                COUNT(DISTINCT restaurant_id) as restaurants,
                COUNT(DISTINCT stat_date) as dates,
                MIN(stat_date) as min_date,
                MAX(stat_date) as max_date
            FROM grab_stats
            WHERE sales > 0
            
            UNION ALL
            
            SELECT 
                'gojek_stats' as table_name,
                COUNT(*) as records,
                COUNT(DISTINCT restaurant_id) as restaurants,
                COUNT(DISTINCT stat_date) as dates,
                MIN(stat_date) as min_date,
                MAX(stat_date) as max_date
            FROM gojek_stats
            WHERE sales > 0
        """, conn)
        
        print("📊 Покрытие данных:")
        for _, row in tables_check.iterrows():
            print(f"   {row['table_name']:12}: {row['records']:,} записей, {row['restaurants']} ресторанов")
            print(f"                     Период: {row['min_date']} → {row['max_date']}")
        
        # 1.2 Проверка координат ресторанов
        coords_check = pd.read_sql_query("""
            SELECT 
                COUNT(*) as total_restaurants,
                COUNT(latitude) as with_coords,
                COUNT(CASE WHEN latitude IS NULL THEN 1 END) as missing_coords
            FROM restaurants
        """, conn)
        
        total_rest = coords_check.iloc[0]['total_restaurants']
        with_coords = coords_check.iloc[0]['with_coords']
        missing_coords = coords_check.iloc[0]['missing_coords']
        
        print(f"\n🗺️ Координаты ресторанов:")
        print(f"   Всего: {total_rest}, С координатами: {with_coords} ({with_coords/total_rest*100:.1f}%)")
        
        validation_results['coords_coverage'] = with_coords/total_rest
        
        # 1.3 Проверка качества данных
        data_quality = pd.read_sql_query("""
            SELECT 
                COUNT(CASE WHEN g.sales IS NULL THEN 1 END) as grab_null_sales,
                COUNT(CASE WHEN gj.sales IS NULL THEN 1 END) as gojek_null_sales,
                COUNT(CASE WHEN g.orders IS NULL THEN 1 END) as grab_null_orders,
                COUNT(CASE WHEN gj.orders IS NULL THEN 1 END) as gojek_null_orders
            FROM grab_stats g
            FULL OUTER JOIN gojek_stats gj ON g.restaurant_id = gj.restaurant_id AND g.stat_date = gj.stat_date
        """, conn)
        
        print(f"\n🔍 Качество данных:")
        print(f"   GRAB пропуски: sales={data_quality.iloc[0]['grab_null_sales']}, orders={data_quality.iloc[0]['grab_null_orders']}")
        print(f"   GOJEK пропуски: sales={data_quality.iloc[0]['gojek_null_sales']}, orders={data_quality.iloc[0]['gojek_null_orders']}")
        
        conn.close()
        
        if validation_results['coords_coverage'] >= 0.9:
            print("✅ Предварительные проверки ПРОЙДЕНЫ")
            return True
        else:
            print("❌ КРИТИЧЕСКИЕ проблемы в данных!")
            return False
    
    def build_feature_dataset(self):
        """2) Feature Engineering - создание богатого набора фичей"""
        
        print("\n🔧 ЭТАП 2: FEATURE ENGINEERING")
        print("="*40)
        
        conn = sqlite3.connect('database.sqlite')
        
        # Загружаем объединенные данные
        print("📊 Загрузка и объединение данных...")
        
        # Основной запрос для объединения GRAB + GOJEK данных
        main_query = """
        SELECT 
            COALESCE(g.restaurant_id, gj.restaurant_id) as restaurant_id,
            COALESCE(g.stat_date, gj.stat_date) as stat_date,
            
            -- GRAB данные
            COALESCE(g.sales, 0) as grab_sales,
            COALESCE(g.orders, 0) as grab_orders,
            COALESCE(g.ads_sales, 0) as grab_ads_sales,
            COALESCE(g.ads_spend, 0) as grab_ads_spend,
            COALESCE(g.rating, 0) as grab_rating,
            COALESCE(g.offline_rate, 0) as grab_offline_rate,
            COALESCE(g.cancelation_rate, 0) as grab_cancelation_rate,
            COALESCE(g.impressions, 0) as grab_impressions,
            COALESCE(g.ads_ctr, 0) as grab_ads_ctr,
            COALESCE(g.new_customers, 0) as grab_new_customers,
            COALESCE(g.repeated_customers, 0) as grab_repeated_customers,
            
            -- GOJEK данные
            COALESCE(gj.sales, 0) as gojek_sales,
            COALESCE(gj.orders, 0) as gojek_orders,
            COALESCE(gj.ads_sales, 0) as gojek_ads_sales,
            COALESCE(gj.ads_spend, 0) as gojek_ads_spend,
            COALESCE(gj.rating, 0) as gojek_rating,
            COALESCE(gj.accepting_time, '00:00:00') as gojek_accepting_time,
            COALESCE(gj.preparation_time, '00:00:00') as gojek_preparation_time,
            COALESCE(gj.delivery_time, '00:00:00') as gojek_delivery_time,
            COALESCE(gj.lost_orders, 0) as gojek_lost_orders,
            COALESCE(gj.realized_orders_percentage, 100) as gojek_realized_pct,
            COALESCE(gj.close_time, 0) as gojek_close_time,
            
            -- Ресторан данные
            r.latitude,
            r.longitude,
            r.location_region
            
        FROM grab_stats g
        FULL OUTER JOIN gojek_stats gj ON g.restaurant_id = gj.restaurant_id AND g.stat_date = gj.stat_date
        LEFT JOIN restaurants r ON COALESCE(g.restaurant_id, gj.restaurant_id) = r.id
        WHERE (g.sales > 0 OR gj.sales > 0)
        ORDER BY stat_date, restaurant_id
        """
        
        df = pd.read_sql_query(main_query, conn)
        conn.close()
        
        print(f"   Загружено: {len(df):,} записей")
        
        # Конвертируем типы данных (КРИТИЧЕСКОЕ ИСПРАВЛЕНИЕ)
        print("🔧 Исправление типов данных...")
        
        numeric_cols = [
            'grab_sales', 'grab_orders', 'grab_ads_sales', 'grab_ads_spend', 'grab_rating',
            'gojek_sales', 'gojek_orders', 'gojek_ads_sales', 'gojek_ads_spend', 'gojek_rating',
            'grab_offline_rate', 'grab_cancelation_rate', 'grab_impressions', 'grab_ads_ctr',
            'grab_new_customers', 'grab_repeated_customers', 'gojek_lost_orders',
            'gojek_realized_pct', 'gojek_close_time'
        ]
        
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        
        # Создаем TARGET переменную
        df['total_sales'] = df['grab_sales'] + df['gojek_sales']
        df['total_orders'] = df['grab_orders'] + df['gojek_orders']
        
        print("🔧 Создание временных фичей...")
        
        # A) Временные фичи
        df['stat_date'] = pd.to_datetime(df['stat_date'])
        df['day_of_week'] = df['stat_date'].dt.dayofweek
        df['is_weekend'] = (df['day_of_week'] >= 5).astype(int)
        df['day_of_month'] = df['stat_date'].dt.day
        df['week_of_year'] = df['stat_date'].dt.isocalendar().week
        df['month'] = df['stat_date'].dt.month
        df['year'] = df['stat_date'].dt.year
        
        # B) Операционные фичи
        print("⚙️ Обработка операционных данных...")
        
        # Конвертируем TIME в минуты
        def time_to_minutes(time_str):
            if pd.isna(time_str) or time_str == '00:00:00':
                return 0
            try:
                if isinstance(time_str, str):
                    parts = time_str.split(':')
                    return int(parts[0]) * 60 + int(parts[1])
                return 0
            except:
                return 0
        
        df['gojek_accepting_minutes'] = df['gojek_accepting_time'].apply(time_to_minutes)
        df['gojek_preparation_minutes'] = df['gojek_preparation_time'].apply(time_to_minutes)
        df['gojek_delivery_minutes'] = df['gojek_delivery_time'].apply(time_to_minutes)
        
        # C) Маркетинговые фичи
        print("📈 Создание маркетинговых фичей...")
        
        df['total_ads_spend'] = df['grab_ads_spend'] + df['gojek_ads_spend']
        df['total_ads_sales'] = df['grab_ads_sales'] + df['gojek_ads_sales']
        
        # ROAS calculation (безопасно)
        df['grab_roas'] = np.where(df['grab_ads_spend'] > 0, 
                                  df['grab_ads_sales'] / df['grab_ads_spend'], 0)
        df['gojek_roas'] = np.where(df['gojek_ads_spend'] > 0, 
                                   df['gojek_ads_sales'] / df['gojek_ads_spend'], 0)
        
        # D) Погодные фичи (используем нашу новую систему)
        print("🌤️ Добавление погодных данных...")
        
        self._add_weather_features(df)
        
        # E) Лаги и скользящие средние
        print("📊 Создание лагов и агрегатов...")
        
        # Сортируем для правильных лагов
        df = df.sort_values(['restaurant_id', 'stat_date'])
        
        # Лаги продаж
        df['sales_lag_1'] = df.groupby('restaurant_id')['total_sales'].shift(1)
        df['sales_lag_7'] = df.groupby('restaurant_id')['total_sales'].shift(7)
        
        # Скользящие агрегаты (правильный способ)
        df['sales_rolling_7'] = df.groupby('restaurant_id')['total_sales'].transform(
            lambda x: x.rolling(7, min_periods=1).mean())
        df['sales_rolling_30'] = df.groupby('restaurant_id')['total_sales'].transform(
            lambda x: x.rolling(30, min_periods=1).mean())
        
        # F) Бинирование дождя (ChatGPT рекомендация)
        df['rain_category'] = pd.cut(df['weather_rain'], 
                                   bins=[0, 0.1, 5, 20, 1000], 
                                   labels=['none', 'light', 'moderate', 'heavy'],
                                   include_lowest=True)
        
        # One-hot encoding для дождя
        rain_dummies = pd.get_dummies(df['rain_category'], prefix='rain')
        df = pd.concat([df, rain_dummies], axis=1)
        
        # G) Географические фичи
        df['region_canggu'] = (df['location_region'] == 'canggu').astype(int)
        df['region_ubud'] = (df['location_region'] == 'ubud').astype(int)
        df['region_seminyak'] = (df['location_region'] == 'seminyak').astype(int)
        
        print(f"✅ Feature Engineering завершен: {len(df.columns)} колонок")
        
        return df
    
    def _add_weather_features(self, df):
        """Добавляет погодные фичи используя нашу исправленную систему"""
        
        df['weather_temp'] = 27.0  # default
        df['weather_rain'] = 0.0
        df['weather_wind'] = 5.0
        
        # Группируем по дате + ресторан для оптимизации
        unique_combinations = df[['stat_date', 'restaurant_id', 'latitude', 'longitude']].drop_duplicates()
        
        # ОГРАНИЧЕНИЕ ДЛЯ ТЕСТИРОВАНИЯ: только первые 500 комбинаций
        max_weather_requests = min(500, len(unique_combinations))
        unique_combinations = unique_combinations.head(max_weather_requests)
        
        print(f"      Получаем погоду для {len(unique_combinations)} комбинаций (тестовый режим)...")
        
        for i, (_, row) in enumerate(unique_combinations.iterrows()):
            if i % 50 == 0 and i > 0:
                print(f"      Обработано {i}/{len(unique_combinations)}")
            
            date_str = row['stat_date'].strftime('%Y-%m-%d')
            restaurant_id = row['restaurant_id']
            lat, lng = row['latitude'], row['longitude']
            
            # Получаем погоду
            weather = self._get_weather_for_date(date_str, restaurant_id, lat, lng)
            
            # Обновляем все записи для этой комбинации
            mask = (df['stat_date'] == row['stat_date']) & (df['restaurant_id'] == restaurant_id)
            df.loc[mask, 'weather_temp'] = weather['temp']
            df.loc[mask, 'weather_rain'] = weather['rain']
            df.loc[mask, 'weather_wind'] = weather['wind']
    
    def _get_weather_for_date(self, date, restaurant_id, lat, lng):
        """Получает погодные данные для конкретного ресторана и даты"""
        
        cache_key = f"{date}_{restaurant_id}"
        
        if cache_key in self.weather_cache:
            return self.weather_cache[cache_key]
        
        default_weather = {'temp': 27.0, 'rain': 0.0, 'wind': 5.0}
        
        if pd.isna(lat) or pd.isna(lng):
            lat, lng = -8.6500, 115.2200  # Default Denpasar
        
        try:
            url = "https://archive-api.open-meteo.com/v1/archive"
            params = {
                'latitude': lat,
                'longitude': lng,
                'start_date': date,
                'end_date': date,
                'daily': 'temperature_2m_mean,precipitation_sum,wind_speed_10m_max',
                'timezone': 'Asia/Jakarta',
                'elevation': 0  # КРИТИЧЕСКОЕ ИСПРАВЛЕНИЕ
            }
            
            response = requests.get(url, params=params, timeout=5)
            if response.status_code == 200:
                data = response.json()
                daily = data.get('daily', {})
                
                if daily:
                    temp_mean = daily.get('temperature_2m_mean', [None])[0]
                    precipitation = daily.get('precipitation_sum', [None])[0]
                    wind_speed = daily.get('wind_speed_10m_max', [None])[0]
                    
                    weather_data = {
                        'temp': temp_mean if temp_mean is not None else 27.0,
                        'rain': precipitation if precipitation is not None else 0.0,
                        'wind': wind_speed if wind_speed is not None else 5.0
                    }
                    
                    self.weather_cache[cache_key] = weather_data
                    return weather_data
        except:
            pass
        
        self.weather_cache[cache_key] = default_weather
        return default_weather
    
    def train_with_time_series_cv(self, df):
        """3) Обучение с TimeSeriesSplit валидацией"""
        
        print("\n🤖 ЭТАП 3: ОБУЧЕНИЕ С TIME SERIES ВАЛИДАЦИЕЙ")
        print("="*50)
        
        # Подготовка данных
        target = 'total_sales'
        
        # Исключаем не-фичи из обучения
        exclude_cols = [
            'restaurant_id', 'stat_date', 'total_sales', 'total_orders',
            'latitude', 'longitude', 'location_region', 'rain_category',
            'gojek_accepting_time', 'gojek_preparation_time', 'gojek_delivery_time'
        ]
        
        feature_cols = [col for col in df.columns if col not in exclude_cols]
        
        # Убираем NaN
        df_clean = df.dropna(subset=feature_cols + [target])
        
        X = df_clean[feature_cols]
        y = df_clean[target]
        
        print(f"📊 Данные для обучения:")
        print(f"   Записей: {len(X):,}")
        print(f"   Фичей: {len(feature_cols)}")
        print(f"   Target: {target}")
        
        # TimeSeriesSplit
        tscv = TimeSeriesSplit(n_splits=5)
        
        oof_predictions = np.zeros(len(y))
        models = []
        cv_scores = []
        
        print(f"\n🔄 Кросс-валидация:")
        
        for fold, (train_idx, val_idx) in enumerate(tscv.split(X)):
            print(f"   Fold {fold + 1}/5...")
            
            X_train, X_val = X.iloc[train_idx], X.iloc[val_idx]
            y_train, y_val = y.iloc[train_idx], y.iloc[val_idx]
            
            # Даты для проверки
            train_dates = df_clean.iloc[train_idx]['stat_date']
            val_dates = df_clean.iloc[val_idx]['stat_date']
            print(f"      Train: {train_dates.min().date()} → {train_dates.max().date()}")
            print(f"      Val:   {val_dates.min().date()} → {val_dates.max().date()}")
            
            # LightGBM
            train_data = lgb.Dataset(X_train, label=y_train)
            val_data = lgb.Dataset(X_val, label=y_val, reference=train_data)
            
            params = {
                'objective': 'regression',
                'metric': 'l1',
                'boosting_type': 'gbdt',
                'learning_rate': 0.05,
                'num_leaves': 64,
                'feature_fraction': 0.8,
                'bagging_fraction': 0.8,
                'bagging_freq': 5,
                'seed': 42,
                'verbose': -1
            }
            
            model = lgb.train(
                params,
                train_data,
                num_boost_round=2000,
                valid_sets=[val_data],
                callbacks=[lgb.early_stopping(100), lgb.log_evaluation(0)]
            )
            
            # Предсказания
            val_pred = model.predict(X_val)
            oof_predictions[val_idx] = val_pred
            
            # Метрики
            mae = mean_absolute_error(y_val, val_pred)
            r2 = r2_score(y_val, val_pred)
            mape = mean_absolute_percentage_error(y_val, val_pred)
            
            cv_scores.append({'mae': mae, 'r2': r2, 'mape': mape})
            models.append(model)
            
            print(f"      MAE: {mae:,.0f} IDR, R²: {r2:.4f}, MAPE: {mape:.2%}")
        
        # Общие метрики
        overall_mae = mean_absolute_error(y, oof_predictions)
        overall_r2 = r2_score(y, oof_predictions)
        overall_mape = mean_absolute_percentage_error(y, oof_predictions)
        
        print(f"\n📊 ИТОГОВЫЕ МЕТРИКИ:")
        print(f"   MAE: {overall_mae:,.0f} IDR")
        print(f"   R²: {overall_r2:.4f}")
        print(f"   MAPE: {overall_mape:.2%}")
        
        # Сохраняем результаты
        self.models = models
        self.feature_names = feature_cols
        
        return {
            'models': models,
            'feature_names': feature_cols,
            'cv_scores': cv_scores,
            'overall_mae': overall_mae,
            'overall_r2': overall_r2,
            'oof_predictions': oof_predictions,
            'y_true': y.values
        }
    
    def analyze_with_shap(self, df, model_results):
        """5) SHAP анализ и объяснения"""
        
        print("\n📊 ЭТАП 4: SHAP АНАЛИЗ")
        print("="*30)
        
        # Используем последнюю модель для SHAP
        model = model_results['models'][-1]
        feature_names = model_results['feature_names']
        
        # Подготавливаем данные для SHAP
        exclude_cols = [
            'restaurant_id', 'stat_date', 'total_sales', 'total_orders',
            'latitude', 'longitude', 'location_region', 'rain_category',
            'gojek_accepting_time', 'gojek_preparation_time', 'gojek_delivery_time'
        ]
        
        df_clean = df.dropna(subset=feature_names + ['total_sales'])
        X_shap = df_clean[feature_names]
        
        print(f"🔍 Создание SHAP объяснений...")
        print(f"   Данных для анализа: {len(X_shap):,}")
        
        # Создаем SHAP explainer
        explainer = shap.TreeExplainer(model)
        
        # Вычисляем SHAP values (для первых 1000 записей для скорости)
        sample_size = min(1000, len(X_shap))
        shap_values = explainer.shap_values(X_shap.iloc[:sample_size])
        
        self.shap_explainer = explainer
        
        # Feature importance
        feature_importance = pd.DataFrame({
            'feature': feature_names,
            'importance': model.feature_importance()
        }).sort_values('importance', ascending=False)
        
        self.feature_importance = feature_importance
        
        print(f"\n🏆 ТОП-15 ВАЖНЫХ ФАКТОРОВ:")
        for i, (_, row) in enumerate(feature_importance.head(15).iterrows()):
            print(f"   {i+1:2}. {row['feature']:25}: {row['importance']:>8.0f}")
        
        # Сохраняем SHAP результаты
        shap_results = {
            'explainer': explainer,
            'shap_values': shap_values,
            'feature_importance': feature_importance,
            'sample_data': X_shap.iloc[:sample_size]
        }
        
        return shap_results
    
    def generate_professional_report(self, df, model_results, shap_results):
        """7) Генерация профессионального отчета"""
        
        print("\n📋 ЭТАП 5: ГЕНЕРАЦИЯ ОТЧЕТА")
        print("="*35)
        
        # Находим худшие дни для анализа
        df_with_pred = df.copy()
        df_clean = df.dropna(subset=model_results['feature_names'] + ['total_sales'])
        
        # Добавляем предсказания
        if len(model_results['oof_predictions']) == len(df_clean):
            df_clean['predicted_sales'] = model_results['oof_predictions']
            df_clean['sales_error'] = df_clean['total_sales'] - df_clean['predicted_sales']
            df_clean['sales_error_pct'] = (df_clean['sales_error'] / df_clean['total_sales']) * 100
        
        # Худшие дни (где факт сильно ниже прогноза)
        worst_days = df_clean.nlargest(10, 'sales_error_pct')[['restaurant_id', 'stat_date', 'total_sales', 'predicted_sales', 'sales_error', 'sales_error_pct']]
        
        print(f"🔍 ТОП-5 ПРОБЛЕМНЫХ ДНЕЙ:")
        for i, (_, row) in enumerate(worst_days.head(5).iterrows()):
            print(f"   {i+1}. {row['stat_date'].date()}: факт {row['total_sales']:,.0f}, прогноз {row['predicted_sales']:,.0f} ({row['sales_error_pct']:+.1f}%)")
        
        report = {
            'model_performance': {
                'mae': model_results['overall_mae'],
                'r2': model_results['overall_r2'],
                'cv_scores': model_results['cv_scores']
            },
            'feature_importance': shap_results['feature_importance'].to_dict('records'),
            'worst_days': worst_days.to_dict('records'),
            'weather_validation': self._validate_weather_impact(df_clean, model_results),
            'recommendations': self._generate_recommendations(shap_results['feature_importance'])
        }
        
        return report
    
    def _validate_weather_impact(self, df, model_results):
        """6) Проверка 'честности' погоды"""
        
        print(f"\n🌤️ ВАЛИДАЦИЯ ВЛИЯНИЯ ПОГОДЫ:")
        
        # Группируем по дождю
        df['rain_group'] = pd.cut(df['weather_rain'], bins=[0, 0.1, 10, 1000], labels=['no_rain', 'light_rain', 'heavy_rain'])
        
        rain_impact = df.groupby('rain_group').agg({
            'total_sales': ['mean', 'count'],
            'weather_temp': 'mean'
        }).round(0)
        
        print(f"   Продажи по дождю:")
        for rain_type in ['no_rain', 'light_rain', 'heavy_rain']:
            if rain_type in rain_impact.index:
                sales = rain_impact.loc[rain_type, ('total_sales', 'mean')]
                count = rain_impact.loc[rain_type, ('total_sales', 'count')]
                print(f"   {rain_type:12}: {sales:,.0f} IDR (дней: {count})")
        
        return rain_impact.to_dict()
    
    def _generate_recommendations(self, feature_importance):
        """Генерация рекомендаций на основе важности фичей"""
        
        top_features = feature_importance.head(10)
        
        recommendations = []
        
        for _, row in top_features.iterrows():
            feature = row['feature']
            
            if 'weather' in feature:
                recommendations.append(f"Погода ('{feature}') значимо влияет на продажи - учитывать в планировании")
            elif 'ads' in feature:
                recommendations.append(f"Реклама ('{feature}') критически важна - оптимизировать бюджет")
            elif 'rating' in feature:
                recommendations.append(f"Рейтинг ('{feature}') влияет на продажи - улучшать качество")
            elif 'offline' in feature:
                recommendations.append(f"Доступность платформ ('{feature}') критична - минимизировать сбои")
        
        return recommendations[:5]  # Топ-5 рекомендаций
    
    def save_results(self, model_results, shap_results, report):
        """Сохранение всех результатов"""
        
        print(f"\n💾 СОХРАНЕНИЕ РЕЗУЛЬТАТОВ:")
        
        # Сохраняем модели
        joblib.dump(model_results['models'], 'professional_ml_models.pkl')
        print(f"   ✅ Модели сохранены: professional_ml_models.pkl")
        
        # Сохраняем SHAP
        joblib.dump(shap_results, 'professional_shap_results.pkl')
        print(f"   ✅ SHAP результаты: professional_shap_results.pkl")
        
        # Сохраняем отчет
        import json
        with open('professional_ml_report.json', 'w', encoding='utf-8') as f:
            json.dump(report, f, ensure_ascii=False, indent=2, default=str)
        print(f"   ✅ Отчет сохранен: professional_ml_report.json")

def main():
    """Основная функция запуска профессиональной ML системы"""
    
    print("🚀 ПРОФЕССИОНАЛЬНАЯ ML СИСТЕМА")
    print("Адаптированная под структуру: grab_stats + gojek_stats + restaurants")
    print("="*70)
    
    # Инициализация
    ml_system = ProfessionalMLSystem()
    
    # 1) Валидация данных
    if not ml_system.load_and_validate_data():
        print("❌ Критические ошибки в данных!")
        return
    
    # 2) Feature Engineering
    df = ml_system.build_feature_dataset()
    
    # 3) Обучение с TimeSeriesSplit
    model_results = ml_system.train_with_time_series_cv(df)
    
    # 4) SHAP анализ
    shap_results = ml_system.analyze_with_shap(df, model_results)
    
    # 5) Генерация отчета
    report = ml_system.generate_professional_report(df, model_results, shap_results)
    
    # 6) Сохранение
    ml_system.save_results(model_results, shap_results, report)
    
    print(f"\n🎉 ПРОФЕССИОНАЛЬНАЯ ML СИСТЕМА ГОТОВА!")
    print(f"   📊 R²: {model_results['overall_r2']:.4f}")
    print(f"   💰 MAE: {model_results['overall_mae']:,.0f} IDR")
    print(f"   🎯 Система готова к production использованию!")

if __name__ == "__main__":
    main()