#!/usr/bin/env python3
"""
🤖 ИНТЕГРИРОВАННЫЙ ML + ДЕТЕКТИВНЫЙ АНАЛИЗ
═══════════════════════════════════════════════════════════════════════════════
Объединяет детективный анализ с ML объяснимостью (SHAP)

✅ ВОЗМОЖНОСТИ:
- Детективный анализ проблемных дней
- ML прогнозирование продаж  
- SHAP объяснимость причин
- Интеграция внешних факторов
- Рекомендации на основе ML
"""

import sqlite3
import pandas as pd
import numpy as np
import json
import requests
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')
import os
import json
import joblib

# ML библиотеки (опциональные)
try:
    from sklearn.ensemble import RandomForestRegressor
    from sklearn.model_selection import train_test_split
    from sklearn.metrics import r2_score, mean_absolute_error
    import shap
    ML_AVAILABLE = True
except ImportError:
    ML_AVAILABLE = False
    print("⚠️ ML библиотеки недоступны. Установите: pip install scikit-learn shap")

from .production_sales_analyzer import ProductionSalesAnalyzer

class IntegratedMLDetective:
    """Интегрированный ML + детективный анализатор"""
    
    def __init__(self):
        # Детективный анализатор (существующий)
        self.detective = ProductionSalesAnalyzer()
        
        # ML компоненты
        self.ml_model = None
        self.shap_explainer = None
        self.feature_names = []
        self.model_trained = False
        
        # Инициализируем ML если доступен
        if ML_AVAILABLE:
            self.ml_model = RandomForestRegressor(
                n_estimators=100,
                max_depth=10,
                random_state=42,
                n_jobs=-1
            )
            # Try load global model
            try:
                model_path = os.path.join('models', 'global_sales_rf.joblib')
                feats_path = os.path.join('models', 'global_sales_rf_features.json')
                if os.path.exists(model_path) and os.path.exists(feats_path):
                    self.ml_model = joblib.load(model_path)
                    with open(feats_path, 'r', encoding='utf-8') as f:
                        self.feature_names = json.load(f).get('features', [])
                    self.model_trained = True
            except Exception:
                pass
    
    def analyze_with_ml_explanations(self, restaurant_name, start_date, end_date):
        """
        ГЛАВНЫЙ МЕТОД: детективный анализ + ML объяснения
        """
        print("🤖 Запуск интегрированного ML + детективного анализа...")
        
        # 1. Получаем стандартный детективный анализ БЕЗ ML (избегаем рекурсии!)
        detective_results = self.detective._standard_detective_analysis(
            restaurant_name, start_date, end_date
        )
        
        if not ML_AVAILABLE:
            detective_results.append("")
            detective_results.append("⚠️ ML анализ недоступен - установите scikit-learn и shap")
            return detective_results
        
        # 2. Обучаем ML модель если еще не обучена
        if not self.model_trained:
            print("🧠 Обучение ML модели...")
            self._train_ml_model(restaurant_name)
        
        # 3. Добавляем ML объяснения для проблемных дней
        ml_enhanced_results = []
        
        for i, result in enumerate(detective_results):
            ml_enhanced_results.append(result)
            
            # Ищем проблемные дни для ML анализа
            if "ПРОБЛЕМНЫЙ ДЕНЬ" in result and "2025-" in result:
                # Извлекаем дату из строки
                date_str = self._extract_date_from_result(result)
                if date_str:
                    print(f"🔍 ML анализ для {date_str}...")
                    ml_explanation = self._get_ml_explanation_for_day(
                        restaurant_name, date_str
                    )
                    ml_enhanced_results.append("")
                    ml_enhanced_results.extend(ml_explanation)
                    
                    # What-if сценарии
                    what_if = self._simulate_what_if_for_day(restaurant_name, date_str)
                    if what_if:
                        ml_enhanced_results.append("")
                        ml_enhanced_results.append("   🔮 WHAT‑IF (контрфакты):")
                        ml_enhanced_results.extend([f"      • {line}" for line in what_if])
        
        # 4. Добавляем общую ML сводку
        ml_enhanced_results.append("")
        ml_enhanced_results.append("🤖 ML СВОДКА И РЕКОМЕНДАЦИИ:")
        ml_enhanced_results.append("=" * 50)
        ml_summary = self._generate_ml_summary(restaurant_name, start_date, end_date)
        ml_enhanced_results.extend(ml_summary)
        
        # 5. Недельный анализ наибольшей просадки (WoW) с SHAP факторами
        weekly = self._summarize_biggest_week_drop(restaurant_name, start_date, end_date)
        if weekly:
            ml_enhanced_results.append("")
            ml_enhanced_results.append("📉 НЕДЕЛЬНЫЙ ПРОСАДОЧНЫЙ ПЕРИОД (WoW):")
            ml_enhanced_results.append("-" * 50)
            ml_enhanced_results.extend(weekly)
        
        return ml_enhanced_results
    
    def _train_ml_model(self, restaurant_name):
        """Обучает ML модель на исторических данных"""
        
        try:
            # Получаем исторические данные для обучения
            training_data = self._prepare_training_data(restaurant_name)
            
            if len(training_data) < 10:
                print("⚠️ Недостаточно данных для обучения ML модели")
                return
            
            # Подготавливаем признаки и целевую переменную
            features_df = training_data.drop(['sales', 'date'], axis=1)
            target = training_data['sales']
            
            self.feature_names = list(features_df.columns)
            
            # Разделяем на обучение и тест
            X_train, X_test, y_train, y_test = train_test_split(
                features_df, target, test_size=0.2, random_state=42
            )
            
            # Обучаем модель
            self.ml_model.fit(X_train, y_train)
            
            # Создаем SHAP объяснитель
            self.shap_explainer = shap.TreeExplainer(self.ml_model)
            
            # Оцениваем качество
            y_pred = self.ml_model.predict(X_test)
            r2 = r2_score(y_test, y_pred)
            mae = mean_absolute_error(y_test, y_pred)
            
            print(f"✅ ML модель обучена: R² = {r2:.3f}, MAE = {mae:,.0f} IDR")
            self.model_trained = True
            
        except Exception as e:
            print(f"❌ Ошибка обучения ML модели: {e}")
            self.model_trained = False
    
    def _prepare_training_data(self, restaurant_name):
        """Подготавливает данные для обучения ML модели"""
        
        conn = sqlite3.connect('database.sqlite')
        
        # Получаем ID ресторана
        restaurant_query = f"SELECT id FROM restaurants WHERE name = '{restaurant_name}'"
        restaurant_df = pd.read_sql_query(restaurant_query, conn)
        
        if restaurant_df.empty:
            return pd.DataFrame()
        
        restaurant_id = restaurant_df.iloc[0]['id']
        
        # Объединенный запрос данных за последние 6 месяцев
        query = f"""
        WITH all_dates AS (
            SELECT stat_date FROM grab_stats 
            WHERE restaurant_id = {restaurant_id}
            AND stat_date >= date('now', '-6 months')
            UNION
            SELECT stat_date FROM gojek_stats 
            WHERE restaurant_id = {restaurant_id}
            AND stat_date >= date('now', '-6 months')
        )
        SELECT 
            ad.stat_date as date,
            COALESCE(g.sales, 0) + COALESCE(gj.sales, 0) as sales,
            
            -- Внешние факторы
            CASE WHEN strftime('%w', ad.stat_date) IN ('0', '6') THEN 1 ELSE 0 END as is_weekend,
            CAST(strftime('%w', ad.stat_date) AS INTEGER) as day_of_week,
            
            -- Операционные факторы  
            COALESCE(g.offline_rate, 0) as grab_offline_rate,
            CASE WHEN gj.close_time IS NOT NULL AND gj.close_time != '00:00:00' THEN 1 ELSE 0 END as gojek_closed,
            
            -- Временные метрики (в минутах)
            CASE WHEN gj.preparation_time IS NOT NULL AND gj.preparation_time != '00:00:00'
                THEN (CAST(substr(gj.preparation_time, 1, 2) AS INTEGER) * 60 + 
                      CAST(substr(gj.preparation_time, 4, 2) AS INTEGER))
                ELSE 15 END as preparation_minutes,
                
            CASE WHEN gj.delivery_time IS NOT NULL AND gj.delivery_time != '00:00:00'
                THEN (CAST(substr(gj.delivery_time, 1, 2) AS INTEGER) * 60 + 
                      CAST(substr(gj.delivery_time, 7, 2) AS INTEGER))
                ELSE 20 END as delivery_minutes,
            
            -- Маркетинговые факторы
            COALESCE(g.ads_spend, 0) + COALESCE(gj.ads_spend, 0) as total_ads_spend,
            COALESCE(g.impressions, 0) as impressions,
            
            -- Качественные факторы
            COALESCE(g.rating, gj.rating, 4.5) as rating,
            COALESCE(g.orders, 0) + COALESCE(gj.orders, 0) as total_orders
            
        FROM all_dates ad
        LEFT JOIN grab_stats g ON ad.stat_date = g.stat_date AND g.restaurant_id = {restaurant_id}
        LEFT JOIN gojek_stats gj ON ad.stat_date = gj.stat_date AND gj.restaurant_id = {restaurant_id}
        ORDER BY ad.stat_date
        """
        
        df = pd.read_sql_query(query, conn)
        conn.close()
        
        if df.empty:
            return df
        
        # Добавляем признаки праздников
        df['is_holiday'] = df['date'].apply(self._check_holiday)
        
        # Добавляем погодные данные (упрощенно для обучения)
        df['precipitation'] = 0  # Будем получать из API при реальном анализе
        df['temperature'] = 27   # Средняя температура для Бали
        
        # Добавляем скользящие средние (исторические)
        df['sales_7day_avg'] = df['sales'].rolling(window=7, min_periods=1).mean().shift(1)
        df['sales_30day_avg'] = df['sales'].rolling(window=30, min_periods=1).mean().shift(1)
        
        # Убираем строки с NaN
        df = df.dropna()
        
        return df
    
    def _get_ml_explanation_for_day(self, restaurant_name, target_date):
        """Получает ML объяснение для конкретного дня"""
        
        if not self.model_trained:
            return ["⚠️ ML модель не обучена"]
        
        try:
            # Подготавливаем признаки для целевого дня
            features = self._prepare_features_for_date(restaurant_name, target_date)
            
            if not features:
                return ["⚠️ Не удалось подготовить признаки для ML анализа"]
            
            # Получаем реальные продажи
            actual_sales = self._get_actual_sales(restaurant_name, target_date)
            
            # Делаем прогноз
            feature_array = np.array([list(features.values())])
            predicted_sales = self.ml_model.predict(feature_array)[0]
            
            # Получаем SHAP объяснение
            shap_values = self.shap_explainer.shap_values(feature_array)[0]
            
            # Формируем объяснение
            explanation = []
            explanation.append("🧠 ML ОБЪЯСНЕНИЕ (SHAP):")
            explanation.append(f"   💰 Реальные продажи: {actual_sales:,.0f} IDR")
            explanation.append(f"   🤖 ML прогноз: {predicted_sales:,.0f} IDR")
            
            deviation_pct = ((actual_sales - predicted_sales) / predicted_sales) * 100 if predicted_sales else 0
            explanation.append(f"   📊 Отклонение: {deviation_pct:+.1f}%")
            explanation.append("")
            
            # Сортируем факторы по важности
            feature_importance = list(zip(self.feature_names, features.values(), shap_values))
            feature_importance.sort(key=lambda x: abs(x[2]), reverse=True)
            
            explanation.append("   🔍 ГЛАВНЫЕ ФАКТОРЫ ВЛИЯНИЯ:")
            
            shown = 0
            for i, (feature_name, feature_value, shap_value) in enumerate(feature_importance):
                if abs(shap_value) < 50000:  # Игнорируем малозначимые
                    continue
                impact_pct = (shap_value / predicted_sales) * 100 if predicted_sales else 0
                formatted_name = self._format_feature_name(feature_name)
                if shap_value > 0:
                    explanation.append(f"      {i+1}. ✅ {formatted_name}: +{impact_pct:.1f}% влияния (+{shap_value:,.0f} IDR)")
                else:
                    explanation.append(f"      {i+1}. 🚨 {formatted_name}: {impact_pct:.1f}% влияния ({shap_value:,.0f} IDR)")
                shown += 1
                if shown >= 5:
                    break
            
            # Добавляем ML рекомендации
            explanation.append("")
            explanation.append("   💡 ML РЕКОМЕНДАЦИИ:")
            ml_recommendations = self._generate_ml_recommendations(feature_importance, predicted_sales)
            explanation.extend([f"      • {rec}" for rec in ml_recommendations])
            
            return explanation
            
        except Exception as e:
            return [f"❌ Ошибка ML анализа: {e}"]
    
    def _simulate_what_if_for_day(self, restaurant_name: str, target_date: str):
        """Простые контрфакты: без дождя, +20% реклама, без простоев платформ."""
        try:
            if not self.model_trained:
                return []
            base_feats = self._prepare_features_for_date(restaurant_name, target_date)
            if not base_feats:
                return []
            actual = self._get_actual_sales(restaurant_name, target_date)
            base_pred = float(self.ml_model.predict(np.array([list(base_feats.values())]))[0])
            scenarios = []
            # 1) Без дождя
            f1 = dict(base_feats); f1['precipitation'] = 0.0
            pred1 = float(self.ml_model.predict(np.array([list(f1.values())]))[0])
            scenarios.append(("Без дождя", pred1 - actual))
            # 2) +20% рекламы
            f2 = dict(base_feats); f2['total_ads_spend'] = base_feats.get('total_ads_spend', 0.0) * 1.2
            pred2 = float(self.ml_model.predict(np.array([list(f2.values())]))[0])
            scenarios.append(("+20% рекламного бюджета", pred2 - actual))
            # 3) Без простоев (обе платформы стабильно)
            f3 = dict(base_feats); f3['grab_offline_rate'] = 0.0; f3['gojek_closed'] = 0
            pred3 = float(self.ml_model.predict(np.array([list(f3.values())]))[0])
            scenarios.append(("Без простоев платформ", pred3 - actual))
            # Форматируем
            lines = [f"{name}: +{delta:,.0f} IDR" if delta>0 else f"{name}: {delta:,.0f} IDR" for name, delta in scenarios]
            return lines
        except Exception:
            return []
    
    def _prepare_features_for_date(self, restaurant_name, target_date):
        """Подготавливает признаки для конкретной даты"""
        
        conn = sqlite3.connect('database.sqlite')
        
        # Получаем ID ресторана
        restaurant_query = f"SELECT id FROM restaurants WHERE name = '{restaurant_name}'"
        restaurant_df = pd.read_sql_query(restaurant_query, conn)
        
        if restaurant_df.empty:
            return {}
        
        restaurant_id = restaurant_df.iloc[0]['id']
        
        # Получаем данные за целевую дату
        query = f"""
        SELECT 
            g.offline_rate,
            gj.close_time,
            gj.preparation_time,
            gj.delivery_time,
            g.ads_spend as grab_ads_spend,
            gj.ads_spend as gojek_ads_spend,
            g.impressions as grab_impressions,
            COALESCE(g.rating, 0) as rating_grab,
            COALESCE(gj.rating, 0) as rating_gojek,
            (COALESCE(g.orders, 0) + COALESCE(gj.orders, 0)) as total_orders,
            (COALESCE(g.sales, 0) + COALESCE(gj.sales, 0)) as total_sales
        FROM grab_stats g
        LEFT JOIN gojek_stats gj ON g.restaurant_id = gj.restaurant_id 
                                   AND g.stat_date = gj.stat_date
        WHERE g.restaurant_id = {restaurant_id} AND g.stat_date = '{target_date}'
        """
        
        df = pd.read_sql_query(query, conn)
        
        # История для rolling признаков
        hist_q = f"""
        WITH all_dates AS (
            SELECT stat_date d, COALESCE(sales,0) s FROM grab_stats WHERE restaurant_id={restaurant_id}
            UNION ALL
            SELECT stat_date d, COALESCE(sales,0) s FROM gojek_stats WHERE restaurant_id={restaurant_id}
        )
        SELECT d as date, SUM(s) total FROM all_dates GROUP BY d ORDER BY d
        """
        hist = pd.read_sql_query(hist_q, conn)
        conn.close()
        
        if df.empty:
            return {}
        
        row = df.iloc[0]
        
        # Дата
        date_obj = datetime.strptime(target_date, '%Y-%m-%d')
        
        # Rolling признаки по истории ресторана
        sales_7day_avg = 0.0
        sales_30day_avg = 0.0
        sales_gradient_7 = 0.0
        if not hist.empty:
            hist['date'] = pd.to_datetime(hist['date'])
            hist = hist.sort_values('date')
            hist['total'] = pd.to_numeric(hist['total'], errors='coerce').fillna(0)
            hist.set_index('date', inplace=True)
            if date_obj in hist.index:
                # сдвиг на 1 день, используем только прошлое
                before = hist.loc[:date_obj - pd.Timedelta(days=1)]['total']
            else:
                before = hist.loc[:date_obj]['total']
            if not before.empty:
                sales_7day_avg = before.rolling(window=7, min_periods=1).mean().iloc[-1]
                sales_30day_avg = before.rolling(window=30, min_periods=1).mean().iloc[-1]
                if len(before) >= 7:
                    sales_gradient_7 = before.iloc[-1] - before.iloc[-7]
        
        # Праздники
        is_holiday = 1 if self._check_holiday(target_date) else 0
        
        # Погода (из существующего метода)
        weather_data = self.detective._get_weather_data(restaurant_name, target_date)
        precipitation = weather_data['precipitation'] if weather_data else 0.0
        temperature = weather_data['temperature'] if weather_data else 27.0
        
        # Туризм
        tourism_index = 0.0
        try:
            from src.utils.tourism_loader import load_tourism_index
            tourism_map = load_tourism_index()
            tourism_index = float(tourism_map.get(target_date, 0.0))
        except Exception:
            pass
        
        # Приведение времен к минутам
        def to_min(x):
            if not x or x in ['00:00:00','0:0:0','0:00:00','00:0:0']:
                return 0.0
            try:
                h,m,s = map(int, str(x).split(':'))
                return h*60 + m + s/60.0
            except Exception:
                return 0.0
        
        features = {
            'grab_offline_rate': float(row['offline_rate'] or 0),
            'gojek_close_minutes': to_min(row['close_time']) if pd.notna(row['close_time']) else 0.0,
            'gojek_preparation_minutes': to_min(row['preparation_time']) if pd.notna(row['preparation_time']) else 0.0,
            'gojek_delivery_minutes': to_min(row['delivery_time']) if pd.notna(row['delivery_time']) else 0.0,
            'grab_driver_waiting': 0.0,  # нет прямых данных в gojek/grab для дня
            'gojek_driver_waiting': 0.0,
            'ads_spend_total': float(row['grab_ads_spend'] or 0) + float(row['gojek_ads_spend'] or 0),
            'impressions_total': float(row['grab_impressions'] or 0),
            'rating_grab': float(row['rating_grab'] or 0),
            'rating_gojek': float(row['rating_gojek'] or 0),
            'is_weekend': 1 if date_obj.weekday() >= 5 else 0,
            'day_of_week': date_obj.weekday(),
            'is_holiday': is_holiday,
            'precipitation': precipitation,
            'temperature': temperature,
            'sales_7day_avg': float(sales_7day_avg or 0.0),
            'sales_30day_avg': float(sales_30day_avg or 0.0),
            'sales_gradient_7': float(sales_gradient_7 or 0.0),
            'tourism_index': float(tourism_index or 0.0),
        }
        
        return features
    
    def _get_historical_features(self, restaurant_name, target_date):
        """Получает исторические признаки (скользящие средние)"""
        
        # Для простоты используем фиксированные средние значения
        # В реальной реализации здесь был бы запрос к БД
        return {
            'sales_7day_avg': 5000000,  # 5M IDR средние за неделю
            'sales_30day_avg': 6000000  # 6M IDR средние за месяц
        }
    
    def _time_to_minutes(self, time_str):
        """Конвертирует время HH:MM:SS в минуты"""
        if not time_str or time_str in ['00:00:00', '0:0:0', '0:00:00', '00:0:0']:
            return 0
        
        try:
            parts = time_str.split(':')
            hours = int(parts[0]) if parts[0] else 0
            minutes = int(parts[1]) if len(parts) > 1 and parts[1] else 0
            return hours * 60 + minutes
        except:
            return 0
    
    def _get_actual_sales(self, restaurant_name, target_date):
        """Получает реальные продажи за день"""
        
        day_data = self.detective._get_day_data(restaurant_name, target_date)
        if day_data:
            return day_data.get('total_sales', 0)
        return 0
    
    def _check_holiday(self, date_str):
        """Проверяет является ли день праздником"""
        return date_str in self.detective.holidays_data
    
    def _format_feature_name(self, feature_name):
        """Форматирует название признака для отчета"""
        
        name_mapping = {
            'grab_offline_rate': '📱 Grab offline rate',
            'gojek_closed': '🛵 Gojek закрыт',
            'preparation_minutes': '⏱️ Время готовки',
            'delivery_minutes': '🚚 Время доставки',
            'total_ads_spend': '💰 Расходы на рекламу',
            'is_holiday': '🎉 Праздник',
            'is_weekend': '📅 Выходной',
            'rating': '⭐ Рейтинг',
            'precipitation': '🌧️ Осадки (мм)',
            'temperature': '🌡️ Температура (°C)',
            'sales_7day_avg': '📈 Средние продажи (7 дней)',
            'sales_30day_avg': '📈 Средние продажи (30 дней)'
        }
        
        return name_mapping.get(feature_name, feature_name)
    
    def _generate_ml_recommendations(self, feature_importance, predicted_sales):
        """Генерирует рекомендации на основе ML анализа"""
        
        recommendations = []
        
        for feature_name, feature_value, shap_value in feature_importance[:3]:
            if abs(shap_value) < 100000:  # Игнорируем малозначимые
                continue
                
            impact_idr = abs(shap_value)
            
            if feature_name == 'grab_offline_rate' and shap_value < 0:
                recommendations.append(f"Стабилизация Grab = потенциал +{impact_idr:,.0f} IDR")
            elif feature_name in ['preparation_minutes', 'delivery_minutes'] and shap_value < 0:
                recommendations.append(f"Оптимизация времени = потенциал +{impact_idr:,.0f} IDR")
            elif feature_name == 'precipitation' and shap_value < 0:
                recommendations.append(f"Планирование под дождь = потенциал +{impact_idr:,.0f} IDR")
            elif feature_name == 'is_holiday' and shap_value < 0:
                recommendations.append(f"Учет праздников = потенциал +{impact_idr:,.0f} IDR")
            elif feature_name == 'total_ads_spend' and shap_value > 0:
                recommendations.append(f"Увеличение рекламы = потенциал +{impact_idr:,.0f} IDR")
            elif feature_name == 'rating' and shap_value < 0:
                recommendations.append(f"Улучшение рейтинга = потенциал +{impact_idr:,.0f} IDR")
        
        if not recommendations:
            recommendations.append("Основные факторы работают нормально")
        
        return recommendations
    
    def _generate_ml_summary(self, restaurant_name, start_date, end_date):
        """Генерирует общую ML сводку"""
        
        summary = []
        
        if self.model_trained:
            # Получаем важность признаков из модели
            feature_importances = list(zip(self.feature_names, self.ml_model.feature_importances_))
            feature_importances.sort(key=lambda x: x[1], reverse=True)
            
            summary.append("🏆 ГЛАВНЫЕ ДРАЙВЕРЫ ПРОДАЖ (важность в ML модели):")
            
            for i, (feature_name, importance) in enumerate(feature_importances[:5]):
                formatted_name = self._format_feature_name(feature_name)
                summary.append(f"   {i+1}. {formatted_name}: {importance*100:.1f}% важности")
            
            summary.append("")
            summary.append("💡 ОБЩИЕ ML РЕКОМЕНДАЦИИ:")
            summary.append("   1. 🚨 Стабильность платформ - критический фактор")
            summary.append("   2. ⏱️ Оптимизация операционных времен")
            summary.append("   3. 🌤️ Планирование под погодные условия")
            summary.append("   4. 🎉 Учет праздников в планировании")
            summary.append("   5. 💰 Балансировка рекламных бюджетов")
            summary.append("   6. ⭐ Мониторинг качества обслуживания")
        else:
            summary.append("⚠️ ML модель не обучена - недостаточно данных")
        
        return summary
    
    def _summarize_biggest_week_drop(self, restaurant_name: str, start_date: str, end_date: str):
        """Находит наибольшую недельную просадку (WoW) и суммирует ключевые SHAP‑факторы недели."""
        try:
            with sqlite3.connect('database.sqlite') as conn:
                # Сумма продаж по дням в периоде
                q = f"""
                WITH all_dates AS (
                    SELECT stat_date d, COALESCE(sales,0) s FROM grab_stats WHERE restaurant_id=(SELECT id FROM restaurants WHERE name='{restaurant_name}') AND stat_date BETWEEN '{start_date}' AND '{end_date}'
                    UNION ALL
                    SELECT stat_date d, COALESCE(sales,0) s FROM gojek_stats WHERE restaurant_id=(SELECT id FROM restaurants WHERE name='{restaurant_name}') AND stat_date BETWEEN '{start_date}' AND '{end_date}'
                )
                SELECT d as date, SUM(s) total FROM all_dates GROUP BY d ORDER BY d
                """
                df = pd.read_sql_query(q, conn)
            if df.empty:
                return []
            df['date'] = pd.to_datetime(df['date'])
            df['week'] = df['date'].dt.isocalendar().week
            wk = df.groupby('week', as_index=False)['total'].sum().sort_values('week')
            wk['prev'] = wk['total'].shift(1)
            wk['wow'] = (wk['total'] - wk['prev'])/wk['prev']*100.0
            wk = wk.dropna(subset=['wow'])
            if wk.empty:
                return []
            worst_week = wk.loc[wk['wow'].idxmin()]
            target_week = int(worst_week['week'])
            change = float(worst_week['wow'])
            # Даты выбранной недели
            dates = df[df['week']==target_week]['date'].dt.strftime('%Y-%m-%d').tolist()
            # SHAP суммы по неделе (абсолютные значения)
            if not self.model_trained:
                return [f"Неделя #{target_week}: {change:.1f}% vs пред. неделя (ML не обучен)"]
            agg = {}
            for d in dates:
                feats = self._prepare_features_for_date(restaurant_name, d)
                if not feats:
                    continue
                arr = np.array([list(feats.values())])
                shap_vals = self.shap_explainer.shap_values(arr)[0]
                for name, val in zip(self.feature_names, shap_vals):
                    agg[name] = agg.get(name, 0.0) + abs(float(val))
            if not agg:
                return [f"Неделя #{target_week}: {change:.1f}% vs пред. неделя"]
            top = sorted([(k,v) for k,v in agg.items()], key=lambda x: x[1], reverse=True)[:5]
            lines = [f"Неделя #{target_week}: {change:.1f}% vs пред. неделя"]
            lines.append("Топ‑факторы недели:")
            for name, val in top:
                lines.append(f"• {self._format_feature_name(name)}")
            return lines
        except Exception:
            return []
    
    def _extract_date_from_result(self, result_line):
        """Извлекает дату из строки результата"""
        
        import re
        date_pattern = r'(2025-\d{2}-\d{2})'
        match = re.search(date_pattern, result_line)
        return match.group(1) if match else None


# Класс-обертка для совместимости с main.py
class ProperMLDetectiveAnalysis:
    """Обертка для совместимости с существующим кодом"""
    
    def __init__(self):
        self.integrated = IntegratedMLDetective()
    
    def run_ml_detective_analysis(self, restaurant_name, start_date=None, end_date=None):
        """Метод для совместимости с main.py"""
        
        if not start_date:
            end_date = datetime.now().strftime('%Y-%m-%d')
            start_date = (datetime.now() - timedelta(days=60)).strftime('%Y-%m-%d')
        
        return self.integrated.analyze_with_ml_explanations(
            restaurant_name, start_date, end_date
        )