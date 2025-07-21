"""
🤖 MUZAQUEST ANALYTICS - МАШИННОЕ ОБУЧЕНИЕ
═══════════════════════════════════════════════════════════════════════════════
Продвинутые ML модели для анализа ресторанного бизнеса
"""

import pandas as pd
import numpy as np
import sqlite3
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

# ML библиотеки
try:
    from sklearn.ensemble import RandomForestRegressor, IsolationForest
    from sklearn.linear_model import LinearRegression
    from sklearn.cluster import KMeans
    from sklearn.preprocessing import StandardScaler
    from sklearn.model_selection import train_test_split
    from sklearn.metrics import mean_absolute_error, r2_score
    SKLEARN_AVAILABLE = True
except ImportError:
    SKLEARN_AVAILABLE = False
    print("⚠️ Установите scikit-learn: pip install scikit-learn")

try:
    from prophet import Prophet
    PROPHET_AVAILABLE = True
except ImportError:
    PROPHET_AVAILABLE = False
    print("⚠️ Установите Prophet: pip install prophet")

class RestaurantMLAnalyzer:
    """Класс для ML анализа ресторанных данных"""
    
    def __init__(self, db_path='database.sqlite'):
        self.db_path = db_path
        self.scaler = StandardScaler()
        self.models = {}
        
    def load_restaurant_data(self, restaurant_id, start_date=None, end_date=None):
        """Загружает данные ресторана для ML анализа"""
        
        conn = sqlite3.connect(self.db_path)
        
        # Объединяем данные Grab и Gojek
        query = """
        WITH combined_data AS (
            SELECT 
                g.stat_date,
                g.rating as grab_rating,
                g.sales as grab_sales,
                g.orders as grab_orders,
                g.ads_spend,
                g.ads_sales,
                g.new_customers,
                g.repeated_customers,
                g.store_is_closed,
                g.out_of_stock,
                g.ads_ctr,
                g.impressions,
                gj.rating as gojek_rating,
                gj.sales as gojek_sales,
                gj.orders as gojek_orders,
                gj.realized_orders_percentage,
                gj.one_star_ratings,
                gj.five_star_ratings
            FROM grab_stats g
            LEFT JOIN gojek_stats gj ON g.restaurant_id = gj.restaurant_id 
                                     AND g.stat_date = gj.stat_date
            WHERE g.restaurant_id = ?
        )
        SELECT 
            stat_date,
            COALESCE(grab_rating, gojek_rating, 4.5) as rating,
            (COALESCE(grab_sales, 0) + COALESCE(gojek_sales, 0)) as total_sales,
            (COALESCE(grab_orders, 0) + COALESCE(gojek_orders, 0)) as total_orders,
            COALESCE(ads_spend, 0) as marketing_spend,
            COALESCE(ads_sales, 0) as marketing_sales,
            COALESCE(new_customers, 0) as new_customers,
            COALESCE(repeated_customers, 0) as repeat_customers,
            COALESCE(store_is_closed, 0) as is_closed,
            COALESCE(out_of_stock, 0) as out_of_stock,
            COALESCE(ads_ctr, 0) as ctr,
            COALESCE(impressions, 0) as impressions,
            COALESCE(realized_orders_percentage, 90) as order_completion,
            COALESCE(one_star_ratings, 0) as negative_reviews,
            COALESCE(five_star_ratings, 0) as positive_reviews
        FROM combined_data
        ORDER BY stat_date
        """
        
        df = pd.read_sql_query(query, conn, params=[restaurant_id])
        conn.close()
        
        # Обработка данных
        df['stat_date'] = pd.to_datetime(df['stat_date'])
        df = df.sort_values('stat_date')
        
        # Фильтрация по датам
        if start_date:
            df = df[df['stat_date'] >= start_date]
        if end_date:
            df = df[df['stat_date'] <= end_date]
            
        # Создание дополнительных признаков
        df = self._create_features(df)
        
        return df
    
    def _create_features(self, df):
        """Создает дополнительные признаки для ML"""
        
        # Временные признаки
        df['day_of_week'] = df['stat_date'].dt.dayofweek
        df['month'] = df['stat_date'].dt.month
        df['is_weekend'] = df['day_of_week'].isin([5, 6]).astype(int)
        
        # Лаговые признаки (предыдущие значения)
        df['sales_lag_1'] = df['total_sales'].shift(1)
        df['sales_lag_7'] = df['total_sales'].shift(7)
        df['rating_lag_1'] = df['rating'].shift(1)
        
        # Скользящие средние
        df['sales_ma_7'] = df['total_sales'].rolling(window=7, min_periods=1).mean()
        df['sales_ma_30'] = df['total_sales'].rolling(window=30, min_periods=1).mean()
        
        # Производные метрики
        df['avg_order_value'] = df['total_sales'] / (df['total_orders'] + 1)
        df['marketing_efficiency'] = df['marketing_sales'] / (df['marketing_spend'] + 1)
        df['customer_retention'] = df['repeat_customers'] / (df['new_customers'] + df['repeat_customers'] + 1)
        
        # Операционные индикаторы
        df['operational_issues'] = df['is_closed'] + df['out_of_stock']
        
        return df
    
    def train_sales_prediction_model(self, df, model_type='random_forest'):
        """Обучает модель прогнозирования продаж"""
        
        if not SKLEARN_AVAILABLE:
            return None, "Scikit-learn не установлен"
            
        # Подготовка признаков
        feature_columns = [
            'rating', 'total_orders', 'marketing_spend', 'new_customers', 
            'repeat_customers', 'day_of_week', 'month', 'is_weekend',
            'sales_lag_1', 'sales_lag_7', 'avg_order_value', 
            'marketing_efficiency', 'customer_retention', 'operational_issues',
            'order_completion', 'negative_reviews', 'positive_reviews'
        ]
        
        # Удаляем строки с NaN
        df_clean = df.dropna(subset=feature_columns + ['total_sales'])
        
        if len(df_clean) < 10:
            return None, "Недостаточно данных для обучения"
            
        X = df_clean[feature_columns]
        y = df_clean['total_sales']
        
        # Разделение на train/test
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42
        )
        
        # Масштабирование
        X_train_scaled = self.scaler.fit_transform(X_train)
        X_test_scaled = self.scaler.transform(X_test)
        
        # Выбор модели
        if model_type == 'linear':
            model = LinearRegression()
            model.fit(X_train_scaled, y_train)
        else:  # random_forest
            model = RandomForestRegressor(
                n_estimators=100, 
                max_depth=10, 
                random_state=42,
                n_jobs=-1
            )
            model.fit(X_train, y_train)  # RF не требует масштабирования
            
        # Оценка модели
        if model_type == 'linear':
            y_pred = model.predict(X_test_scaled)
        else:
            y_pred = model.predict(X_test)
            
        mae = mean_absolute_error(y_test, y_pred)
        r2 = r2_score(y_test, y_pred)
        
        # Важность признаков (для Random Forest)
        feature_importance = None
        if model_type == 'random_forest':
            feature_importance = dict(zip(feature_columns, model.feature_importances_))
            feature_importance = sorted(feature_importance.items(), key=lambda x: x[1], reverse=True)
        
        self.models['sales_prediction'] = {
            'model': model,
            'type': model_type,
            'features': feature_columns,
            'mae': mae,
            'r2': r2,
            'feature_importance': feature_importance
        }
        
        return model, {
            'mae': mae,
            'r2': r2,
            'feature_importance': feature_importance
        }
    
    def detect_anomalies(self, df, contamination=0.1):
        """Детекция аномалий с помощью Isolation Forest"""
        
        if not SKLEARN_AVAILABLE:
            return None, "Scikit-learn не установлен"
            
        # Признаки для детекции аномалий
        anomaly_features = [
            'total_sales', 'total_orders', 'rating', 'marketing_spend',
            'avg_order_value', 'customer_retention', 'operational_issues'
        ]
        
        df_clean = df.dropna(subset=anomaly_features)
        
        if len(df_clean) < 10:
            return None, "Недостаточно данных"
            
        X = df_clean[anomaly_features]
        X_scaled = self.scaler.fit_transform(X)
        
        # Isolation Forest
        iso_forest = IsolationForest(
            contamination=contamination,
            random_state=42,
            n_jobs=-1
        )
        
        anomaly_labels = iso_forest.fit_predict(X_scaled)
        anomaly_scores = iso_forest.score_samples(X_scaled)
        
        # Добавляем результаты в DataFrame
        df_clean = df_clean.copy()
        df_clean['is_anomaly'] = (anomaly_labels == -1)
        df_clean['anomaly_score'] = anomaly_scores
        
        # Топ аномалий
        anomalies = df_clean[df_clean['is_anomaly']].sort_values('anomaly_score')
        
        return anomalies, {
            'total_anomalies': len(anomalies),
            'anomaly_rate': len(anomalies) / len(df_clean) * 100
        }
    
    def segment_customers(self, df, n_clusters=3):
        """Сегментация клиентов с помощью K-Means"""
        
        if not SKLEARN_AVAILABLE:
            return None, "Scikit-learn не установлен"
            
        # Агрегируем данные по периодам для сегментации
        segment_features = [
            'avg_order_value', 'customer_retention', 'marketing_efficiency',
            'rating', 'total_orders'
        ]
        
        df_agg = df.groupby(df.index // 7).agg({  # Группировка по неделям
            'avg_order_value': 'mean',
            'customer_retention': 'mean', 
            'marketing_efficiency': 'mean',
            'rating': 'mean',
            'total_orders': 'sum'
        }).dropna()
        
        if len(df_agg) < n_clusters:
            return None, "Недостаточно данных для кластеризации"
            
        X = df_agg[segment_features]
        X_scaled = self.scaler.fit_transform(X)
        
        # K-Means кластеризация
        kmeans = KMeans(n_clusters=n_clusters, random_state=42, n_init=10)
        clusters = kmeans.fit_predict(X_scaled)
        
        df_agg['cluster'] = clusters
        
        # Анализ кластеров
        cluster_analysis = {}
        for i in range(n_clusters):
            cluster_data = df_agg[df_agg['cluster'] == i]
            cluster_analysis[i] = {
                'size': len(cluster_data),
                'avg_order_value': cluster_data['avg_order_value'].mean(),
                'customer_retention': cluster_data['customer_retention'].mean(),
                'rating': cluster_data['rating'].mean(),
                'description': self._describe_cluster(cluster_data, segment_features)
            }
        
        return df_agg, cluster_analysis
    
    def _describe_cluster(self, cluster_data, features):
        """Описывает характеристики кластера"""
        
        descriptions = []
        
        if cluster_data['avg_order_value'].mean() > 400000:
            descriptions.append("Премиум-сегмент")
        elif cluster_data['avg_order_value'].mean() > 250000:
            descriptions.append("Средний сегмент")
        else:
            descriptions.append("Бюджетный сегмент")
            
        if cluster_data['customer_retention'].mean() > 0.6:
            descriptions.append("Высокая лояльность")
        elif cluster_data['customer_retention'].mean() > 0.4:
            descriptions.append("Средняя лояльность")
        else:
            descriptions.append("Низкая лояльность")
            
        if cluster_data['rating'].mean() > 4.7:
            descriptions.append("Отличное качество")
        elif cluster_data['rating'].mean() > 4.5:
            descriptions.append("Хорошее качество")
        else:
            descriptions.append("Требует улучшения")
            
        return " | ".join(descriptions)
    
    def forecast_sales(self, df, periods=30):
        """Прогнозирование продаж с помощью Prophet"""
        
        if not PROPHET_AVAILABLE:
            return None, "Prophet не установлен"
            
        # Подготовка данных для Prophet
        prophet_df = df[['stat_date', 'total_sales']].copy()
        prophet_df.columns = ['ds', 'y']
        prophet_df = prophet_df.dropna()
        
        if len(prophet_df) < 30:
            return None, "Недостаточно исторических данных"
            
        # Создание и обучение модели Prophet
        model = Prophet(
            daily_seasonality=True,
            weekly_seasonality=True,
            yearly_seasonality=True,
            changepoint_prior_scale=0.05
        )
        
        # Добавляем внешние факторы
        if 'marketing_spend' in df.columns:
            model.add_regressor('marketing_spend')
            prophet_df['marketing_spend'] = df['marketing_spend'].values[:len(prophet_df)]
            
        if 'is_weekend' in df.columns:
            model.add_regressor('is_weekend')
            prophet_df['is_weekend'] = df['is_weekend'].values[:len(prophet_df)]
        
        model.fit(prophet_df)
        
        # Создание прогноза
        future = model.make_future_dataframe(periods=periods)
        
        # Заполняем будущие значения регрессоров
        if 'marketing_spend' in prophet_df.columns:
            avg_marketing = prophet_df['marketing_spend'].tail(30).mean()
            future['marketing_spend'] = future['marketing_spend'].fillna(avg_marketing)
            
        if 'is_weekend' in prophet_df.columns:
            future['is_weekend'] = future['ds'].dt.dayofweek.isin([5, 6]).astype(int)
        
        forecast = model.predict(future)
        
        return forecast, model
    
    def generate_ml_insights(self, restaurant_id, start_date=None, end_date=None):
        """Генерирует ML инсайты для ресторана"""
        
        insights = []
        insights.append("🤖 МАШИННОЕ ОБУЧЕНИЕ - ДЕТАЛЬНЫЙ АНАЛИЗ")
        insights.append("=" * 60)
        
        # Загружаем данные
        df = self.load_restaurant_data(restaurant_id, start_date, end_date)
        
        if len(df) < 10:
            insights.append("❌ Недостаточно данных для ML анализа")
            return insights
            
        insights.append(f"📊 Загружено {len(df)} записей для анализа")
        insights.append("")
        
        # 1. Модель прогнозирования продаж
        insights.append("🎯 1. МОДЕЛЬ ПРОГНОЗИРОВАНИЯ ПРОДАЖ")
        insights.append("-" * 40)
        
        model, metrics = self.train_sales_prediction_model(df, 'random_forest')
        if model:
            insights.append(f"✅ Random Forest модель обучена")
            insights.append(f"📊 Точность (R²): {metrics['r2']:.3f}")
            insights.append(f"📊 Средняя ошибка: {metrics['mae']:,.0f} IDR")
            insights.append("")
            
            # Важность признаков
            if metrics['feature_importance']:
                insights.append("🔍 ТОП-5 ФАКТОРОВ, ВЛИЯЮЩИХ НА ПРОДАЖИ:")
                for i, (feature, importance) in enumerate(metrics['feature_importance'][:5]):
                    feature_name = self._translate_feature(feature)
                    insights.append(f"  {i+1}. {feature_name}: {importance:.3f}")
                insights.append("")
        
        # 2. Детекция аномалий
        insights.append("🚨 2. АВТОМАТИЧЕСКАЯ ДЕТЕКЦИЯ АНОМАЛИЙ")
        insights.append("-" * 40)
        
        anomalies, anomaly_stats = self.detect_anomalies(df)
        if anomalies is not None:
            insights.append(f"🔍 Обнаружено {anomaly_stats['total_anomalies']} аномалий")
            insights.append(f"📊 Доля аномалий: {anomaly_stats['anomaly_rate']:.1f}%")
            
            if len(anomalies) > 0:
                insights.append("")
                insights.append("📉 ТОП-3 АНОМАЛЬНЫХ ДНЯ:")
                for i, (idx, row) in enumerate(anomalies.head(3).iterrows()):
                    date = row['stat_date'].strftime('%Y-%m-%d')
                    sales = row['total_sales']
                    score = row['anomaly_score']
                    insights.append(f"  {i+1}. {date}: {sales:,.0f} IDR (аномальность: {abs(score):.3f})")
            insights.append("")
        
        # 3. Сегментация периодов
        insights.append("📊 3. АВТОМАТИЧЕСКАЯ СЕГМЕНТАЦИЯ ПЕРИОДОВ")
        insights.append("-" * 40)
        
        segments, cluster_analysis = self.segment_customers(df)
        if segments is not None:
            insights.append(f"🎯 Выделено {len(cluster_analysis)} сегментов:")
            for cluster_id, analysis in cluster_analysis.items():
                insights.append(f"  Сегмент {cluster_id + 1}: {analysis['description']}")
                insights.append(f"    • Размер: {analysis['size']} периодов")
                insights.append(f"    • Средний чек: {analysis['avg_order_value']:,.0f} IDR")
                insights.append(f"    • Лояльность: {analysis['customer_retention']:.1%}")
            insights.append("")
        
        # 4. Прогноз продаж
        insights.append("🔮 4. ПРОГНОЗ ПРОДАЖ (PROPHET)")
        insights.append("-" * 40)
        
        forecast, prophet_model = self.forecast_sales(df, periods=14)
        if forecast is not None:
            # Последние фактические продажи
            recent_avg = df['total_sales'].tail(7).mean()
            
            # Прогноз на следующие 7 дней
            future_forecast = forecast.tail(14).head(7)
            forecast_avg = future_forecast['yhat'].mean()
            
            growth_rate = (forecast_avg - recent_avg) / recent_avg * 100
            
            insights.append(f"📈 Прогноз на следующие 7 дней:")
            insights.append(f"  • Средние продажи: {forecast_avg:,.0f} IDR/день")
            insights.append(f"  • Изменение: {growth_rate:+.1f}% к текущему периоду")
            
            # Доверительные интервалы
            forecast_min = future_forecast['yhat_lower'].mean()
            forecast_max = future_forecast['yhat_upper'].mean()
            insights.append(f"  • Диапазон: {forecast_min:,.0f} - {forecast_max:,.0f} IDR")
            insights.append("")
        
        # 5. ML рекомендации
        insights.append("💡 5. ML-РЕКОМЕНДАЦИИ")
        insights.append("-" * 40)
        
        # Анализ важности факторов
        if model and metrics['feature_importance']:
            top_factor = metrics['feature_importance'][0]
            factor_name = self._translate_feature(top_factor[0])
            insights.append(f"🎯 Главный фактор роста: {factor_name}")
            
            if 'marketing' in top_factor[0].lower():
                insights.append("  💡 Рекомендация: Оптимизировать маркетинговые расходы")
            elif 'rating' in top_factor[0].lower():
                insights.append("  💡 Рекомендация: Фокус на качестве обслуживания")
            elif 'operational' in top_factor[0].lower():
                insights.append("  💡 Рекомендация: Улучшить операционную эффективность")
        
        # Прогнозные рекомендации
        if forecast is not None and growth_rate < -5:
            insights.append("⚠️ Прогноз показывает снижение продаж")
            insights.append("  💡 Рекомендация: Принять превентивные меры")
        elif forecast is not None and growth_rate > 10:
            insights.append("🚀 Прогноз показывает рост продаж")
            insights.append("  💡 Рекомендация: Подготовиться к увеличению спроса")
        
        return insights
    
    def _translate_feature(self, feature):
        """Переводит названия признаков на русский"""
        
        translations = {
            'rating': 'Рейтинг',
            'marketing_spend': 'Маркетинговые расходы',
            'marketing_efficiency': 'Эффективность маркетинга',
            'customer_retention': 'Удержание клиентов',
            'avg_order_value': 'Средний чек',
            'operational_issues': 'Операционные проблемы',
            'total_orders': 'Количество заказов',
            'is_weekend': 'Выходные дни',
            'order_completion': 'Выполнение заказов',
            'negative_reviews': 'Негативные отзывы',
            'positive_reviews': 'Позитивные отзывы',
            'sales_lag_1': 'Продажи предыдущего дня',
            'sales_lag_7': 'Продажи неделю назад'
        }
        
        return translations.get(feature, feature)

def analyze_restaurant_with_ml(restaurant_name, start_date=None, end_date=None):
    """Основная функция ML анализа ресторана"""
    
    # Получаем ID ресторана
    conn = sqlite3.connect('database.sqlite')
    cursor = conn.cursor()
    cursor.execute("SELECT id FROM restaurants WHERE name = ?", (restaurant_name,))
    result = cursor.fetchone()
    conn.close()
    
    if not result:
        return ["❌ Ресторан не найден"]
    
    restaurant_id = result[0]
    
    # Создаем ML анализатор
    ml_analyzer = RestaurantMLAnalyzer()
    
    # Генерируем ML инсайты
    ml_insights = ml_analyzer.generate_ml_insights(
        restaurant_id, start_date, end_date
    )
    
    return ml_insights

if __name__ == "__main__":
    # Тестирование ML модулей
    print("🤖 Тестирование ML модулей...")
    
    if SKLEARN_AVAILABLE:
        print("✅ Scikit-learn доступен")
    else:
        print("❌ Scikit-learn не установлен")
        
    if PROPHET_AVAILABLE:
        print("✅ Prophet доступен")
    else:
        print("❌ Prophet не установлен")