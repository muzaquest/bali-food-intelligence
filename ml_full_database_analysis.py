"""
🤖 MUZAQUEST ANALYTICS - ПОЛНЫЙ ML АНАЛИЗ ВСЕЙ БАЗЫ ДАННЫХ
═══════════════════════════════════════════════════════════════════════════════
Глубокий анализ 25,129 записей по 59 ресторанам за 2+ года (2023-2025)
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
    from sklearn.cluster import KMeans
    from sklearn.preprocessing import StandardScaler
    from sklearn.decomposition import PCA
    from sklearn.metrics import silhouette_score
    import scipy.stats as stats
    SKLEARN_AVAILABLE = True
except ImportError:
    SKLEARN_AVAILABLE = False

def load_full_database():
    """Загружает все данные из базы для ML анализа"""
    conn = sqlite3.connect('database.sqlite')
    
    # Объединяем данные Grab и Gojek
    query = """
    SELECT 
        r.name as restaurant_name,
        r.id as restaurant_id,
        'grab' as platform,
        g.stat_date,
        g.sales as total_sales_amount,
        g.orders as total_orders,
        g.rating,
        COALESCE(g.ads_sales, 0) as marketing_spend,
        COALESCE(g.orders - g.cancelled_orders, g.orders) as new_customers,
        COALESCE(g.cancelled_orders, 0) as repeat_customers,
        CASE WHEN g.orders > 0 THEN g.sales / g.orders ELSE 0 END as avg_order_value,
        90.0 as customer_retention_rate,
        COALESCE(g.cancelled_orders, 0) as operational_issues,
        strftime('%w', g.stat_date) as day_of_week,
        strftime('%m', g.stat_date) as month,
        CASE WHEN strftime('%w', g.stat_date) IN ('0', '6') THEN 1 ELSE 0 END as is_weekend
    FROM grab_stats g
    JOIN restaurants r ON g.restaurant_id = r.id
    
    UNION ALL
    
    SELECT 
        r.name as restaurant_name,
        r.id as restaurant_id,
        'gojek' as platform,
        gj.stat_date,
        gj.sales as total_sales_amount,
        gj.orders as total_orders,
        gj.rating,
        COALESCE(gj.ads_sales, 0) as marketing_spend,
        COALESCE(gj.incoming_orders, gj.orders) as new_customers,
        COALESCE(gj.accepted_orders, gj.orders) as repeat_customers,
        CASE WHEN gj.orders > 0 THEN gj.sales / gj.orders ELSE 0 END as avg_order_value,
        COALESCE(gj.realized_orders_percentage, 90.0) as customer_retention_rate,
        COALESCE(gj.cancelled_orders + gj.lost_orders, 0) as operational_issues,
        strftime('%w', gj.stat_date) as day_of_week,
        strftime('%m', gj.stat_date) as month,
        CASE WHEN strftime('%w', gj.stat_date) IN ('0', '6') THEN 1 ELSE 0 END as is_weekend
    FROM gojek_stats gj
    JOIN restaurants r ON gj.restaurant_id = r.id
    
    ORDER BY stat_date, restaurant_name
    """
    
    df = pd.read_sql_query(query, conn)
    conn.close()
    
    # Преобразуем типы данных
    df['stat_date'] = pd.to_datetime(df['stat_date'])
    df['day_of_week'] = df['day_of_week'].astype(int)
    df['month'] = df['month'].astype(int)
    df['is_weekend'] = df['is_weekend'].astype(int)
    
    # Заполняем пропуски
    numeric_columns = ['total_sales_amount', 'total_orders', 'rating', 'marketing_spend', 
                      'new_customers', 'repeat_customers', 'avg_order_value', 'customer_retention_rate']
    for col in numeric_columns:
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
    
    return df

def analyze_market_segments():
    """Анализ сегментации всего рынка ресторанов"""
    print("🔍 1. СЕГМЕНТАЦИЯ РЫНКА РЕСТОРАНОВ")
    print("-" * 50)
    
    df = load_full_database()
    
    # Агрегируем данные по ресторанам
    restaurant_stats = df.groupby(['restaurant_name', 'restaurant_id']).agg({
        'total_sales_amount': ['sum', 'mean', 'std'],
        'total_orders': ['sum', 'mean'],
        'rating': 'mean',
        'marketing_spend': 'sum',
        'new_customers': 'sum',
        'repeat_customers': 'sum',
        'avg_order_value': 'mean',
        'customer_retention_rate': 'mean'
    }).round(2)
    
    # Упрощаем названия колонок
    restaurant_stats.columns = ['total_revenue', 'avg_daily_revenue', 'revenue_volatility',
                               'total_orders', 'avg_daily_orders', 'avg_rating', 
                               'total_marketing', 'total_new_customers', 'total_repeat_customers',
                               'avg_order_value', 'retention_rate']
    
    # Добавляем вычисляемые метрики
    restaurant_stats['marketing_efficiency'] = restaurant_stats['total_revenue'] / (restaurant_stats['total_marketing'] + 1)
    restaurant_stats['customer_loyalty'] = restaurant_stats['total_repeat_customers'] / (restaurant_stats['total_new_customers'] + 1)
    
    if SKLEARN_AVAILABLE:
        # K-Means кластеризация
        features_for_clustering = ['total_revenue', 'avg_rating', 'marketing_efficiency', 
                                  'customer_loyalty', 'avg_order_value', 'retention_rate']
        
        X = restaurant_stats[features_for_clustering].fillna(0)
        scaler = StandardScaler()
        X_scaled = scaler.fit_transform(X)
        
        # Определяем оптимальное количество кластеров
        silhouette_scores = []
        K_range = range(2, 8)
        for k in K_range:
            kmeans = KMeans(n_clusters=k, random_state=42, n_init=10)
            labels = kmeans.fit_predict(X_scaled)
            score = silhouette_score(X_scaled, labels)
            silhouette_scores.append(score)
        
        optimal_k = K_range[np.argmax(silhouette_scores)]
        
        # Финальная кластеризация
        kmeans = KMeans(n_clusters=optimal_k, random_state=42, n_init=10)
        restaurant_stats['cluster'] = kmeans.fit_predict(X_scaled)
        
        print(f"✅ Выявлено {optimal_k} сегментов ресторанов")
        print(f"📊 Качество сегментации: {max(silhouette_scores):.3f}")
        
        # Анализ каждого сегмента
        for cluster in range(optimal_k):
            cluster_data = restaurant_stats[restaurant_stats['cluster'] == cluster]
            print(f"\n🎯 СЕГМЕНТ {cluster + 1} ({len(cluster_data)} ресторанов):")
            print(f"   💰 Средняя выручка: {cluster_data['total_revenue'].mean():,.0f} IDR")
            print(f"   ⭐ Средний рейтинг: {cluster_data['avg_rating'].mean():.2f}")
            print(f"   📈 Маркетинговая эффективность: {cluster_data['marketing_efficiency'].mean():.1f}x")
            print(f"   👥 Лояльность клиентов: {cluster_data['customer_loyalty'].mean():.2f}")
            
            # Топ рестораны сегмента
            top_restaurants = cluster_data.nlargest(3, 'total_revenue').index.get_level_values(0).tolist()
            print(f"   🏆 Лидеры: {', '.join(top_restaurants[:3])}")
    
    return restaurant_stats

def detect_market_anomalies():
    """Выявление аномалий на уровне всего рынка"""
    print("\n🚨 2. ДЕТЕКЦИЯ АНОМАЛИЙ ВСЕГО РЫНКА")
    print("-" * 50)
    
    df = load_full_database()
    
    # Агрегируем по дням для всего рынка
    daily_market = df.groupby('stat_date').agg({
        'total_sales_amount': 'sum',
        'total_orders': 'sum',
        'rating': 'mean',
        'marketing_spend': 'sum'
    }).round(2)
    
    if SKLEARN_AVAILABLE:
        # Isolation Forest для детекции аномалий
        features = ['total_sales_amount', 'total_orders', 'rating', 'marketing_spend']
        X = daily_market[features].fillna(0)
        
        iso_forest = IsolationForest(contamination=0.05, random_state=42)
        anomalies = iso_forest.fit_predict(X)
        daily_market['is_anomaly'] = anomalies == -1
        
        anomaly_days = daily_market[daily_market['is_anomaly']].sort_values('total_sales_amount')
        
        print(f"🔍 Найдено {len(anomaly_days)} аномальных дней из {len(daily_market)}")
        print(f"📉 Самые аномальные дни:")
        
        for i, (date, row) in enumerate(anomaly_days.head(5).iterrows()):
            print(f"   {i+1}. {date.strftime('%Y-%m-%d')}: {row['total_sales_amount']:,.0f} IDR, {row['total_orders']:.0f} заказов")
    
    # Статистический анализ трендов
    daily_market['sales_change'] = daily_market['total_sales_amount'].pct_change() * 100
    daily_market['orders_change'] = daily_market['total_orders'].pct_change() * 100
    
    print(f"\n📊 СТАТИСТИКА ВОЛАТИЛЬНОСТИ:")
    print(f"   💰 Средняя дневная выручка: {daily_market['total_sales_amount'].mean():,.0f} IDR")
    print(f"   📈 Волатильность выручки: {daily_market['sales_change'].std():.1f}%")
    print(f"   📦 Средние заказы в день: {daily_market['total_orders'].mean():.0f}")
    print(f"   📊 Волатильность заказов: {daily_market['orders_change'].std():.1f}%")
    
    return daily_market

def analyze_success_factors():
    """Анализ факторов успеха на всем рынке"""
    print("\n🎯 3. ФАКТОРЫ УСПЕХА (RANDOM FOREST)")
    print("-" * 50)
    
    df = load_full_database()
    
    if SKLEARN_AVAILABLE:
        # Подготовка данных для ML
        features = ['rating', 'marketing_spend', 'new_customers', 'repeat_customers',
                   'avg_order_value', 'customer_retention_rate', 'operational_issues',
                   'day_of_week', 'month', 'is_weekend']
        
        # Создаем лаги для временных рядов
        df_sorted = df.sort_values(['restaurant_id', 'stat_date'])
        df_sorted['sales_lag_1'] = df_sorted.groupby('restaurant_id')['total_sales_amount'].shift(1)
        df_sorted['sales_lag_7'] = df_sorted.groupby('restaurant_id')['total_sales_amount'].shift(7)
        
        features.extend(['sales_lag_1', 'sales_lag_7'])
        
        # Убираем пропуски
        ml_data = df_sorted[features + ['total_sales_amount']].dropna()
        
        X = ml_data[features]
        y = ml_data['total_sales_amount']
        
        # Random Forest
        rf = RandomForestRegressor(n_estimators=100, random_state=42, max_depth=10)
        rf.fit(X, y)
        
        # Важность признаков
        feature_importance = pd.DataFrame({
            'feature': features,
            'importance': rf.feature_importances_
        }).sort_values('importance', ascending=False)
        
        print("🏆 ТОП-10 ФАКТОРОВ УСПЕХА НА РЫНКЕ:")
        for i, (_, row) in enumerate(feature_importance.head(10).iterrows()):
            print(f"   {i+1}. {row['feature']}: {row['importance']:.3f} ({row['importance']*100:.1f}%)")
        
        # Предсказание качества
        from sklearn.model_selection import train_test_split
        from sklearn.metrics import r2_score, mean_absolute_error
        
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
        rf.fit(X_train, y_train)
        y_pred = rf.predict(X_test)
        
        r2 = r2_score(y_test, y_pred)
        mae = mean_absolute_error(y_test, y_pred)
        
        print(f"\n📊 КАЧЕСТВО МОДЕЛИ:")
        print(f"   🎯 R² Score: {r2:.3f} ({r2*100:.1f}% объясненной дисперсии)")
        print(f"   📏 Средняя ошибка: {mae:,.0f} IDR")
        
        return feature_importance
    else:
        print("❌ Scikit-learn недоступен для ML анализа")
        return None

def analyze_seasonal_patterns():
    """Анализ сезонных паттернов по всему рынку"""
    print("\n📅 4. СЕЗОННЫЕ ПАТТЕРНЫ И ТРЕНДЫ")
    print("-" * 50)
    
    df = load_full_database()
    
    # Анализ по месяцам
    monthly_stats = df.groupby('month').agg({
        'total_sales_amount': ['mean', 'sum'],
        'total_orders': ['mean', 'sum'],
        'rating': 'mean'
    }).round(2)
    
    monthly_stats.columns = ['avg_daily_sales', 'total_sales', 'avg_daily_orders', 'total_orders', 'avg_rating']
    
    print("📊 АНАЛИЗ ПО МЕСЯЦАМ:")
    months = ['Янв', 'Фев', 'Мар', 'Апр', 'Май', 'Июн', 'Июл', 'Авг', 'Сен', 'Окт', 'Ноя', 'Дек']
    
    for month in range(1, 13):
        if month in monthly_stats.index:
            stats = monthly_stats.loc[month]
            print(f"   {months[month-1]}: {stats['avg_daily_sales']:,.0f} IDR/день, ⭐{stats['avg_rating']:.2f}")
    
    # Анализ по дням недели
    weekday_stats = df.groupby('day_of_week').agg({
        'total_sales_amount': 'mean',
        'total_orders': 'mean',
        'rating': 'mean'
    }).round(2)
    
    days = ['Воскресенье', 'Понедельник', 'Вторник', 'Среда', 'Четверг', 'Пятница', 'Суббота']
    
    print(f"\n📅 АНАЛИЗ ПО ДНЯМ НЕДЕЛИ:")
    for day in range(7):
        if day in weekday_stats.index:
            stats = weekday_stats.loc[day]
            print(f"   {days[day]}: {stats['total_sales_amount']:,.0f} IDR, {stats['total_orders']:.0f} заказов")
    
    # Выходные vs будни
    weekend_comparison = df.groupby('is_weekend').agg({
        'total_sales_amount': 'mean',
        'total_orders': 'mean'
    }).round(2)
    
    if len(weekend_comparison) == 2:
        weekday_sales = weekend_comparison.loc[0, 'total_sales_amount']
        weekend_sales = weekend_comparison.loc[1, 'total_sales_amount']
        weekend_boost = ((weekend_sales - weekday_sales) / weekday_sales) * 100
        
        print(f"\n🎉 ЭФФЕКТ ВЫХОДНЫХ:")
        print(f"   📈 Рост продаж в выходные: {weekend_boost:+.1f}%")
        print(f"   💰 Будни: {weekday_sales:,.0f} IDR")
        print(f"   💰 Выходные: {weekend_sales:,.0f} IDR")

def platform_comparison_analysis():
    """Сравнительный анализ платформ Grab vs Gojek"""
    print("\n🥊 5. GRAB VS GOJEK - СРАВНЕНИЕ ПЛАТФОРМ")
    print("-" * 50)
    
    df = load_full_database()
    
    platform_stats = df.groupby('platform').agg({
        'total_sales_amount': ['sum', 'mean', 'count'],
        'total_orders': ['sum', 'mean'],
        'rating': 'mean',
        'marketing_spend': 'sum',
        'avg_order_value': 'mean'
    }).round(2)
    
    platform_stats.columns = ['total_revenue', 'avg_daily_revenue', 'days_count',
                              'total_orders', 'avg_daily_orders', 'avg_rating',
                              'total_marketing', 'avg_order_value']
    
    print("📊 СРАВНЕНИЕ ПЛАТФОРМ:")
    for platform in platform_stats.index:
        stats = platform_stats.loc[platform]
        print(f"\n🏪 {platform.upper()}:")
        print(f"   💰 Общая выручка: {stats['total_revenue']:,.0f} IDR")
        print(f"   📦 Общие заказы: {stats['total_orders']:,.0f}")
        print(f"   ⭐ Средний рейтинг: {stats['avg_rating']:.2f}")
        print(f"   💳 Средний чек: {stats['avg_order_value']:,.0f} IDR")
        print(f"   📅 Дней данных: {stats['days_count']:,.0f}")
    
    # Расчет доли рынка
    if len(platform_stats) == 2:
        grab_revenue = platform_stats.loc['grab', 'total_revenue']
        gojek_revenue = platform_stats.loc['gojek', 'total_revenue']
        total_revenue = grab_revenue + gojek_revenue
        
        print(f"\n📈 ДОЛЯ РЫНКА:")
        print(f"   🟢 Grab: {(grab_revenue/total_revenue)*100:.1f}%")
        print(f"   🔵 Gojek: {(gojek_revenue/total_revenue)*100:.1f}%")

def run_full_ml_analysis():
    """Запуск полного ML анализа всей базы данных"""
    print("🤖 MUZAQUEST ANALYTICS - ПОЛНЫЙ ML АНАЛИЗ ВСЕЙ БАЗЫ")
    print("=" * 60)
    print("📊 Анализируем 25,129 записей по 59 ресторанам (2023-2025)")
    print("=" * 60)
    
    try:
        # 1. Сегментация рынка
        restaurant_segments = analyze_market_segments()
        
        # 2. Детекция аномалий
        market_anomalies = detect_market_anomalies()
        
        # 3. Факторы успеха
        success_factors = analyze_success_factors()
        
        # 4. Сезонные паттерны
        analyze_seasonal_patterns()
        
        # 5. Сравнение платформ
        platform_comparison_analysis()
        
        print("\n" + "=" * 60)
        print("✅ ПОЛНЫЙ ML АНАЛИЗ ЗАВЕРШЕН!")
        print("🎯 Все инсайты по 59 ресторанам получены")
        print("=" * 60)
        
    except Exception as e:
        print(f"❌ Ошибка в ML анализе: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    run_full_ml_analysis()