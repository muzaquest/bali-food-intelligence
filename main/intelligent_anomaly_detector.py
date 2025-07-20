import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Any
import sqlite3
from scipy import stats
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import DBSCAN
from sklearn.ensemble import IsolationForest

class IntelligentAnomalyDetector:
    """
    Интеллектуальный детектор аномалий и трендов
    Автоматически находит ВСЁ интересное без указания конкретных метрик
    """
    
    def __init__(self, db_path: str = "data/database.sqlite"):
        self.db_path = db_path
        
        # Определяем все возможные метрики для анализа
        self.numerical_metrics = [
            'total_sales', 'orders', 'rating', 'delivery_time', 
            'marketing_spend', 'marketing_sales', 'roas', 
            'avg_order_value', 'cancel_rate'
        ]
        
        # Пороги значимости для разных типов изменений
        self.significance_thresholds = {
            'critical': 3.0,    # 3 стандартных отклонения
            'major': 2.0,       # 2 стандартных отклонения 
            'moderate': 1.5,    # 1.5 стандартных отклонения
            'percentage_change': 20.0,  # 20% изменение
            'correlation_strength': 0.7  # Сильная корреляция
        }
    
    def analyze_everything(self, start_date: str, end_date: str, 
                          comparison_start: str = None, comparison_end: str = None) -> Dict:
        """
        Анализирует ВСЁ и находит все интересные паттерны автоматически
        """
        
        print(f"🧠 ИНТЕЛЛЕКТУАЛЬНЫЙ ПОИСК АНОМАЛИЙ И ТРЕНДОВ")
        print(f"📅 Основной период: {start_date} → {end_date}")
        if comparison_start and comparison_end:
            print(f"📅 Период сравнения: {comparison_start} → {comparison_end}")
        
        # Загружаем данные
        current_data = self._load_comprehensive_data(start_date, end_date)
        comparison_data = None
        if comparison_start and comparison_end:
            comparison_data = self._load_comprehensive_data(comparison_start, comparison_end)
        
        # Запускаем все виды анализа
        findings = {
            'timestamp': datetime.now().isoformat(),
            'period': f"{start_date} to {end_date}",
            'critical_findings': [],
            'major_findings': [],
            'interesting_patterns': [],
            'hidden_correlations': [],
            'performance_anomalies': [],
            'trend_changes': [],
            'outlier_restaurants': [],
            'platform_insights': [],
            'seasonal_discoveries': [],
            'unexpected_behaviors': []
        }
        
        try:
            # 1. Статистические аномалии (изолированные выбросы)
            findings['performance_anomalies'] = self._detect_statistical_anomalies(current_data)
            
            # 2. Кластерный анализ (необычные группы)
            findings['outlier_restaurants'] = self._detect_clustering_anomalies(current_data)
            
            # 3. Временные аномалии (необычные дни/недели)
            findings['seasonal_discoveries'] = self._detect_temporal_anomalies(current_data)
            
            # 4. Скрытые корреляции между метриками
            findings['hidden_correlations'] = self._discover_hidden_correlations(current_data)
            
            # 5. Аномалии платформ
            findings['platform_insights'] = self._analyze_platform_anomalies(current_data)
            
            # 6. Сравнительный анализ (если есть данные для сравнения)
            if comparison_data is not None:
                findings['trend_changes'] = self._detect_trend_changes(current_data, comparison_data)
            
            # 7. Необычное поведение отдельных ресторанов
            findings['unexpected_behaviors'] = self._detect_behavioral_anomalies(current_data)
            
            # 8. Ранжируем находки по важности
            findings = self._rank_findings_by_importance(findings)
            
            return findings
            
        except Exception as e:
            print(f"❌ Ошибка интеллектуального анализа: {e}")
            return findings
    
    def _load_comprehensive_data(self, start_date: str, end_date: str) -> pd.DataFrame:
        """Загружает все доступные данные за период"""
        
        conn = sqlite3.connect(self.db_path)
        
        query = """
        SELECT 
            restaurant_name,
            date,
            platform,
            total_sales,
            orders,
            rating,
            delivery_time,
            marketing_spend,
            marketing_sales,
            marketing_orders,
            roas,
            avg_order_value,
            cancel_rate,
            ads_on,
            weather_condition,
            temperature_celsius,
            precipitation_mm,
            is_holiday,
            is_weekend
        FROM restaurant_data 
        WHERE date BETWEEN ? AND ?
        ORDER BY date, restaurant_name, platform
        """
        
        df = pd.read_sql_query(query, conn, params=(start_date, end_date))
        conn.close()
        
        df['date'] = pd.to_datetime(df['date'])
        df['weekday'] = df['date'].dt.day_name()
        df['month'] = df['date'].dt.month
        df['week'] = df['date'].dt.isocalendar().week
        
        return df
    
    def _detect_statistical_anomalies(self, data: pd.DataFrame) -> List[Dict]:
        """Находит статистические аномалии в любых метриках"""
        
        anomalies = []
        
        # Анализируем каждую числовую метрику
        for metric in self.numerical_metrics:
            if metric in data.columns and data[metric].notna().sum() > 10:
                
                # Используем Isolation Forest для поиска аномалий
                values = data[data[metric].notna()][metric].values.reshape(-1, 1)
                
                if len(values) > 20:  # Достаточно данных для анализа
                    iso_forest = IsolationForest(contamination=0.1, random_state=42)
                    outliers = iso_forest.fit_predict(values)
                    
                    # Находим аномальные записи
                    anomaly_indices = np.where(outliers == -1)[0]
                    
                    if len(anomaly_indices) > 0:
                        metric_data = data[data[metric].notna()].iloc[anomaly_indices]
                        
                        # Группируем аномалии по ресторанам
                        restaurant_anomalies = {}
                        for _, row in metric_data.iterrows():
                            restaurant = row['restaurant_name']
                            if restaurant not in restaurant_anomalies:
                                restaurant_anomalies[restaurant] = []
                            restaurant_anomalies[restaurant].append({
                                'metric': metric,
                                'value': row[metric],
                                'date': row['date'].strftime('%Y-%m-%d'),
                                'platform': row['platform']
                            })
                        
                        # Создаем находки для ресторанов с множественными аномалиями
                        for restaurant, rest_anomalies in restaurant_anomalies.items():
                            if len(rest_anomalies) >= 2:  # Минимум 2 аномалии
                                severity = self._calculate_severity(len(rest_anomalies), len(data[data['restaurant_name'] == restaurant]))
                                
                                anomalies.append({
                                    'type': 'statistical_anomaly',
                                    'severity': severity,
                                    'restaurant': restaurant,
                                    'metric': metric,
                                    'anomaly_count': len(rest_anomalies),
                                    'details': rest_anomalies[:5],  # Показываем только первые 5
                                    'description': f"Ресторан {restaurant} показывает {len(rest_anomalies)} аномальных значений по метрике {metric}"
                                })
        
        return sorted(anomalies, key=lambda x: x['severity'], reverse=True)
    
    def _detect_clustering_anomalies(self, data: pd.DataFrame) -> List[Dict]:
        """Находит рестораны с необычными паттернами поведения через кластеризацию"""
        
        outliers = []
        
        try:
            # Агрегируем данные по ресторанам
            restaurant_features = data.groupby('restaurant_name').agg({
                'total_sales': ['mean', 'std', 'sum'],
                'orders': ['mean', 'std', 'sum'],
                'rating': ['mean', 'std'],
                'delivery_time': ['mean', 'std'],
                'roas': ['mean', 'std'],
                'avg_order_value': ['mean', 'std'],
                'cancel_rate': ['mean', 'std']
            }).fillna(0)
            
            # Упрощаем названия колонок
            restaurant_features.columns = ['_'.join(col) for col in restaurant_features.columns]
            
            # Стандартизируем данные
            scaler = StandardScaler()
            features_scaled = scaler.fit_transform(restaurant_features)
            
            # Применяем DBSCAN для поиска выбросов
            dbscan = DBSCAN(eps=1.5, min_samples=3)
            clusters = dbscan.fit_predict(features_scaled)
            
            # Находим выбросы (кластер -1)
            outlier_mask = clusters == -1
            outlier_restaurants = restaurant_features.index[outlier_mask].tolist()
            
            for restaurant in outlier_restaurants:
                # Анализируем почему этот ресторан является выбросом
                rest_data = data[data['restaurant_name'] == restaurant]
                
                unusual_metrics = []
                for metric in self.numerical_metrics:
                    if metric in rest_data.columns:
                        rest_values = rest_data[metric].dropna()
                        if len(rest_values) > 0:
                            market_values = data[data['restaurant_name'] != restaurant][metric].dropna()
                            
                            if len(market_values) > 0:
                                rest_mean = rest_values.mean()
                                market_mean = market_values.mean()
                                market_std = market_values.std()
                                
                                if market_std > 0:
                                    z_score = abs(rest_mean - market_mean) / market_std
                                    if z_score > 2:  # Значительное отклонение
                                        unusual_metrics.append({
                                            'metric': metric,
                                            'restaurant_value': rest_mean,
                                            'market_average': market_mean,
                                            'deviation': z_score
                                        })
                
                if unusual_metrics:
                    severity = self._calculate_severity(len(unusual_metrics), len(self.numerical_metrics))
                    
                    outliers.append({
                        'type': 'behavioral_outlier',
                        'severity': severity,
                        'restaurant': restaurant,
                        'unusual_metrics': unusual_metrics,
                        'description': f"Ресторан {restaurant} показывает необычное поведение по {len(unusual_metrics)} метрикам"
                    })
            
        except Exception as e:
            print(f"⚠️ Ошибка кластерного анализа: {e}")
        
        return sorted(outliers, key=lambda x: x['severity'], reverse=True)
    
    def _detect_temporal_anomalies(self, data: pd.DataFrame) -> List[Dict]:
        """Находит необычные временные паттерны"""
        
        temporal_anomalies = []
        
        # Анализ по дням недели
        weekday_analysis = self._analyze_weekday_patterns(data)
        temporal_anomalies.extend(weekday_analysis)
        
        # Анализ по неделям
        weekly_analysis = self._analyze_weekly_patterns(data)
        temporal_anomalies.extend(weekly_analysis)
        
        # Анализ праздничных дней
        holiday_analysis = self._analyze_holiday_patterns(data)
        temporal_anomalies.extend(holiday_analysis)
        
        return sorted(temporal_anomalies, key=lambda x: x['severity'], reverse=True)
    
    def _analyze_weekday_patterns(self, data: pd.DataFrame) -> List[Dict]:
        """Анализирует паттерны по дням недели"""
        
        patterns = []
        
        # Агрегируем по дням недели
        weekday_stats = data.groupby('weekday').agg({
            'total_sales': ['mean', 'std'],
            'orders': ['mean', 'std'],
            'rating': ['mean', 'std']
        })
        
        # Находим дни с необычными показателями
        for metric in ['total_sales', 'orders', 'rating']:
            if (metric, 'mean') in weekday_stats.columns:
                means = weekday_stats[(metric, 'mean')]
                stds = weekday_stats[(metric, 'std')]
                
                overall_mean = means.mean()
                overall_std = means.std()
                
                for weekday in means.index:
                    if overall_std > 0:
                        z_score = abs(means[weekday] - overall_mean) / overall_std
                        
                        if z_score > 1.5:  # Необычный день
                            direction = "выше" if means[weekday] > overall_mean else "ниже"
                            percentage = abs((means[weekday] / overall_mean - 1) * 100)
                            
                            patterns.append({
                                'type': 'weekday_anomaly',
                                'severity': min(z_score / 3.0, 1.0),
                                'weekday': weekday,
                                'metric': metric,
                                'value': means[weekday],
                                'market_average': overall_mean,
                                'deviation_percent': percentage,
                                'description': f"{weekday} показывает {metric} на {percentage:.1f}% {direction} среднего"
                            })
        
        return patterns
    
    def _analyze_weekly_patterns(self, data: pd.DataFrame) -> List[Dict]:
        """Анализирует паттерны по неделям"""
        
        patterns = []
        
        # Агрегируем по неделям
        weekly_data = data.groupby(['week']).agg({
            'total_sales': 'sum',
            'orders': 'sum'
        }).reset_index()
        
        for metric in ['total_sales', 'orders']:
            if metric in weekly_data.columns:
                values = weekly_data[metric]
                mean_val = values.mean()
                std_val = values.std()
                
                if std_val > 0:
                    # Находим недели с аномальными показателями
                    for _, week_data in weekly_data.iterrows():
                        z_score = abs(week_data[metric] - mean_val) / std_val
                        
                        if z_score > 2:  # Аномальная неделя
                            direction = "выше" if week_data[metric] > mean_val else "ниже"
                            percentage = abs((week_data[metric] / mean_val - 1) * 100)
                            
                            patterns.append({
                                'type': 'weekly_anomaly',
                                'severity': min(z_score / 3.0, 1.0),
                                'week': week_data['week'],
                                'metric': metric,
                                'value': week_data[metric],
                                'market_average': mean_val,
                                'deviation_percent': percentage,
                                'description': f"Неделя {week_data['week']} показывает {metric} на {percentage:.1f}% {direction} среднего"
                            })
        
        return patterns
    
    def _analyze_holiday_patterns(self, data: pd.DataFrame) -> List[Dict]:
        """Анализирует влияние праздников"""
        
        patterns = []
        
        if 'is_holiday' in data.columns:
            holiday_data = data[data['is_holiday'] == 1]
            normal_data = data[data['is_holiday'] == 0]
            
            if len(holiday_data) > 0 and len(normal_data) > 0:
                for metric in ['total_sales', 'orders']:
                    if metric in data.columns:
                        holiday_mean = holiday_data.groupby('date')[metric].sum().mean()
                        normal_mean = normal_data.groupby('date')[metric].sum().mean()
                        
                        if normal_mean > 0:
                            impact = ((holiday_mean / normal_mean) - 1) * 100
                            
                            if abs(impact) > 20:  # Значительное влияние праздников
                                patterns.append({
                                    'type': 'holiday_impact',
                                    'severity': min(abs(impact) / 100, 1.0),
                                    'metric': metric,
                                    'holiday_average': holiday_mean,
                                    'normal_average': normal_mean,
                                    'impact_percent': impact,
                                    'description': f"Праздники изменяют {metric} на {impact:+.1f}%"
                                })
        
        return patterns
    
    def _discover_hidden_correlations(self, data: pd.DataFrame) -> List[Dict]:
        """Находит скрытые корреляции между метриками"""
        
        correlations = []
        
        # Создаем матрицу корреляций для всех числовых метрик
        numeric_data = data[self.numerical_metrics].dropna()
        
        if len(numeric_data) > 20:
            corr_matrix = numeric_data.corr()
            
            # Ищем сильные корреляции (исключая автокорреляции)
            for i, metric1 in enumerate(corr_matrix.columns):
                for j, metric2 in enumerate(corr_matrix.columns):
                    if i < j:  # Избегаем дубликатов и автокорреляций
                        correlation = corr_matrix.loc[metric1, metric2]
                        
                        if abs(correlation) > self.significance_thresholds['correlation_strength']:
                            # Проверяем, является ли эта корреляция неожиданной
                            if self._is_unexpected_correlation(metric1, metric2, correlation):
                                correlations.append({
                                    'type': 'hidden_correlation',
                                    'severity': abs(correlation),
                                    'metric1': metric1,
                                    'metric2': metric2,
                                    'correlation': correlation,
                                    'strength': 'сильная положительная' if correlation > 0 else 'сильная отрицательная',
                                    'description': f"Неожиданная {('положительная' if correlation > 0 else 'отрицательная')} корреляция между {metric1} и {metric2} ({correlation:.3f})"
                                })
        
        return sorted(correlations, key=lambda x: x['severity'], reverse=True)
    
    def _analyze_platform_anomalies(self, data: pd.DataFrame) -> List[Dict]:
        """Анализирует аномалии между платформами"""
        
        platform_insights = []
        
        if 'platform' in data.columns:
            platforms = data['platform'].unique()
            
            for metric in self.numerical_metrics:
                if metric in data.columns:
                    platform_stats = data.groupby('platform')[metric].agg(['mean', 'std']).fillna(0)
                    
                    if len(platforms) >= 2:
                        # Сравниваем все пары платформ
                        for i, platform1 in enumerate(platforms):
                            for platform2 in platforms[i+1:]:
                                if platform1 in platform_stats.index and platform2 in platform_stats.index:
                                    mean1 = platform_stats.loc[platform1, 'mean']
                                    mean2 = platform_stats.loc[platform2, 'mean']
                                    
                                    if mean1 > 0 and mean2 > 0:
                                        ratio = max(mean1, mean2) / min(mean1, mean2)
                                        
                                        if ratio > 2:  # Значительная разница между платформами
                                            better_platform = platform1 if mean1 > mean2 else platform2
                                            worse_platform = platform2 if mean1 > mean2 else platform1
                                            percentage_diff = ((max(mean1, mean2) / min(mean1, mean2)) - 1) * 100
                                            
                                            platform_insights.append({
                                                'type': 'platform_disparity',
                                                'severity': min(ratio / 5.0, 1.0),
                                                'metric': metric,
                                                'better_platform': better_platform,
                                                'worse_platform': worse_platform,
                                                'ratio': ratio,
                                                'percentage_diff': percentage_diff,
                                                'description': f"Платформа {better_platform} показывает {metric} в {ratio:.1f} раза лучше чем {worse_platform}"
                                            })
        
        return sorted(platform_insights, key=lambda x: x['severity'], reverse=True)
    
    def _detect_trend_changes(self, current_data: pd.DataFrame, comparison_data: pd.DataFrame) -> List[Dict]:
        """Обнаруживает изменения трендов между периодами"""
        
        trend_changes = []
        
        # Сравниваем агрегированные метрики
        current_agg = current_data.groupby('restaurant_name')[self.numerical_metrics].mean()
        comparison_agg = comparison_data.groupby('restaurant_name')[self.numerical_metrics].mean()
        
        common_restaurants = set(current_agg.index) & set(comparison_agg.index)
        
        for restaurant in common_restaurants:
            for metric in self.numerical_metrics:
                if metric in current_agg.columns and metric in comparison_agg.columns:
                    current_val = current_agg.loc[restaurant, metric]
                    comparison_val = comparison_agg.loc[restaurant, metric]
                    
                    if pd.notna(current_val) and pd.notna(comparison_val) and comparison_val != 0:
                        change_percent = ((current_val / comparison_val) - 1) * 100
                        
                        if abs(change_percent) > self.significance_thresholds['percentage_change']:
                            trend_changes.append({
                                'type': 'trend_change',
                                'severity': min(abs(change_percent) / 100, 1.0),
                                'restaurant': restaurant,
                                'metric': metric,
                                'current_value': current_val,
                                'previous_value': comparison_val,
                                'change_percent': change_percent,
                                'direction': 'улучшение' if change_percent > 0 else 'ухудшение',
                                'description': f"{restaurant}: {metric} изменился на {change_percent:+.1f}%"
                            })
        
        return sorted(trend_changes, key=lambda x: x['severity'], reverse=True)
    
    def _detect_behavioral_anomalies(self, data: pd.DataFrame) -> List[Dict]:
        """Обнаруживает необычное поведение отдельных ресторанов"""
        
        behaviors = []
        
        # Анализируем каждый ресторан на предмет необычного поведения
        for restaurant in data['restaurant_name'].unique():
            rest_data = data[data['restaurant_name'] == restaurant]
            
            # Проверяем консистентность метрик
            inconsistencies = self._check_metric_consistency(rest_data)
            if inconsistencies:
                behaviors.extend(inconsistencies)
            
            # Проверяем сезонные аномалии
            seasonal_anomalies = self._check_seasonal_consistency(rest_data, restaurant)
            if seasonal_anomalies:
                behaviors.extend(seasonal_anomalies)
        
        return sorted(behaviors, key=lambda x: x['severity'], reverse=True)
    
    def _check_metric_consistency(self, restaurant_data: pd.DataFrame) -> List[Dict]:
        """Проверяет консистентность метрик внутри ресторана"""
        
        inconsistencies = []
        restaurant_name = restaurant_data['restaurant_name'].iloc[0]
        
        # Проверяем соотношение продаж и заказов (средний чек)
        if 'total_sales' in restaurant_data.columns and 'orders' in restaurant_data.columns:
            daily_data = restaurant_data.groupby('date').agg({
                'total_sales': 'sum',
                'orders': 'sum'
            })
            
            daily_data = daily_data[daily_data['orders'] > 0]
            if len(daily_data) > 10:
                daily_data['avg_order_value'] = daily_data['total_sales'] / daily_data['orders']
                
                # Ищем дни с аномальным средним чеком
                aov_mean = daily_data['avg_order_value'].mean()
                aov_std = daily_data['avg_order_value'].std()
                
                if aov_std > 0:
                    anomalous_days = daily_data[abs(daily_data['avg_order_value'] - aov_mean) > 2 * aov_std]
                    
                    if len(anomalous_days) > 2:
                        inconsistencies.append({
                            'type': 'metric_inconsistency',
                            'severity': min(len(anomalous_days) / len(daily_data), 1.0),
                            'restaurant': restaurant_name,
                            'issue': 'volatile_order_value',
                            'anomalous_days': len(anomalous_days),
                            'total_days': len(daily_data),
                            'description': f"{restaurant_name}: {len(anomalous_days)} дней с аномальным средним чеком"
                        })
        
        return inconsistencies
    
    def _check_seasonal_consistency(self, restaurant_data: pd.DataFrame, restaurant_name: str) -> List[Dict]:
        """Проверяет сезонную консистентность ресторана"""
        
        anomalies = []
        
        # Анализируем продажи по дням недели
        if 'total_sales' in restaurant_data.columns:
            weekday_sales = restaurant_data.groupby('weekday')['total_sales'].mean()
            
            if len(weekday_sales) >= 6:  # Есть данные по большинству дней недели
                sales_std = weekday_sales.std()
                sales_mean = weekday_sales.mean()
                
                if sales_mean > 0:
                    cv = sales_std / sales_mean  # Коэффициент вариации
                    
                    if cv > 0.5:  # Высокая вариативность по дням недели
                        anomalies.append({
                            'type': 'seasonal_inconsistency',
                            'severity': min(cv, 1.0),
                            'restaurant': restaurant_name,
                            'issue': 'high_weekday_variability',
                            'coefficient_variation': cv,
                            'description': f"{restaurant_name}: очень нестабильные продажи по дням недели (CV={cv:.2f})"
                        })
        
        return anomalies
    
    def _rank_findings_by_importance(self, findings: Dict) -> Dict:
        """Ранжирует находки по важности и переносит самые важные в соответствующие категории"""
        
        all_findings = []
        
        # Собираем все находки с их важностью
        for category, finding_list in findings.items():
            if isinstance(finding_list, list):
                for finding in finding_list:
                    if isinstance(finding, dict) and 'severity' in finding:
                        finding['category'] = category
                        all_findings.append(finding)
        
        # Сортируем по важности
        all_findings.sort(key=lambda x: x['severity'], reverse=True)
        
        # Распределяем по категориям важности
        for finding in all_findings:
            if finding['severity'] >= 0.8:
                findings['critical_findings'].append(finding)
            elif finding['severity'] >= 0.5:
                findings['major_findings'].append(finding)
            else:
                findings['interesting_patterns'].append(finding)
        
        # Ограничиваем количество находок в каждой категории
        for category in ['critical_findings', 'major_findings', 'interesting_patterns']:
            findings[category] = findings[category][:10]  # Максимум 10 в каждой категории
        
        return findings
    
    def _calculate_severity(self, anomaly_count: int, total_count: int) -> float:
        """Рассчитывает серьезность аномалии"""
        if total_count == 0:
            return 0.0
        
        ratio = anomaly_count / total_count
        return min(ratio * 2, 1.0)  # Максимум 1.0
    
    def _is_unexpected_correlation(self, metric1: str, metric2: str, correlation: float) -> bool:
        """Определяет, является ли корреляция неожиданной"""
        
        # Определяем ожидаемые корреляции
        expected_positive = [
            ('total_sales', 'orders'),
            ('total_sales', 'marketing_spend'),
            ('marketing_spend', 'marketing_sales'),
            ('rating', 'total_sales')
        ]
        
        expected_negative = [
            ('rating', 'delivery_time'),
            ('delivery_time', 'total_sales'),
            ('cancel_rate', 'rating'),
            ('cancel_rate', 'total_sales')
        ]
        
        pair = (metric1, metric2) if metric1 < metric2 else (metric2, metric1)
        
        # Если корреляция ожидаемая, то она не неожиданная
        if correlation > 0 and pair in expected_positive:
            return False
        if correlation < 0 and pair in expected_negative:
            return False
        
        # Все остальные сильные корреляции считаем неожиданными
        return True
    
    def generate_intelligent_report(self, findings: Dict) -> str:
        """Генерирует отчет об интеллектуальном анализе"""
        
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        report = f"""
╔══════════════════════════════════════════════════════════════════════════════════════════════════════
║                    🧠 ИНТЕЛЛЕКТУАЛЬНЫЙ АНАЛИЗ - АВТОМАТИЧЕСКОЕ ОБНАРУЖЕНИЕ АНОМАЛИЙ
╠══════════════════════════════════════════════════════════════════════════════════════════════════════
║ 📅 Период анализа: {findings['period']}
║ 🕐 Отчет сгенерирован: {timestamp}
║ 🎯 Найдено: {len(findings['critical_findings'])} критических, {len(findings['major_findings'])} важных, {len(findings['interesting_patterns'])} интересных паттернов
╚══════════════════════════════════════════════════════════════════════════════════════════════════════

🚨 КРИТИЧЕСКИЕ НАХОДКИ (требуют немедленного внимания)
═══════════════════════════════════════════════════════════════════════════════
"""
        
        if findings['critical_findings']:
            for i, finding in enumerate(findings['critical_findings'], 1):
                report += f"\n{i}. 🔴 {finding['description']}\n"
                report += f"   Серьезность: {finding['severity']:.1%} | Тип: {finding['type']}\n"
                if 'restaurant' in finding:
                    report += f"   Ресторан: {finding['restaurant']}\n"
        else:
            report += "\n✅ Критических проблем не обнаружено\n"
        
        report += f"""

⚠️ ВАЖНЫЕ НАХОДКИ (требуют анализа)
═══════════════════════════════════════════════════════════════════════════════
"""
        
        if findings['major_findings']:
            for i, finding in enumerate(findings['major_findings'], 1):
                report += f"\n{i}. 🟡 {finding['description']}\n"
                report += f"   Серьезность: {finding['severity']:.1%} | Тип: {finding['type']}\n"
        else:
            report += "\n✅ Значительных проблем не обнаружено\n"
        
        report += f"""

💡 ИНТЕРЕСНЫЕ ПАТТЕРНЫ (для дальнейшего изучения)
═══════════════════════════════════════════════════════════════════════════════
"""
        
        if findings['interesting_patterns']:
            for i, finding in enumerate(findings['interesting_patterns'], 1):
                report += f"\n{i}. 🔍 {finding['description']}\n"
                report += f"   Тип: {finding['type']}\n"
        else:
            report += "\n📊 Дополнительных паттернов не найдено\n"
        
        # Добавляем специальные секции для определенных типов находок
        if findings['hidden_correlations']:
            report += f"""

🔗 СКРЫТЫЕ КОРРЕЛЯЦИИ
═══════════════════════════════════════════════════════════════════════════════
"""
            for correlation in findings['hidden_correlations'][:5]:
                report += f"• {correlation['description']}\n"
        
        if findings['platform_insights']:
            report += f"""

📱 ИНСАЙТЫ ПЛАТФОРМ
═══════════════════════════════════════════════════════════════════════════════
"""
            for insight in findings['platform_insights'][:5]:
                report += f"• {insight['description']}\n"
        
        report += f"""

🎯 АВТОМАТИЧЕСКИЕ РЕКОМЕНДАЦИИ
═══════════════════════════════════════════════════════════════════════════════
"""
        
        # Генерируем рекомендации на основе находок
        recommendations = self._generate_automatic_recommendations(findings)
        for i, rec in enumerate(recommendations, 1):
            report += f"{i}. {rec}\n"
        
        report += f"""

╔══════════════════════════════════════════════════════════════════════════════════════════════════════
║                           🎯 КОНЕЦ ИНТЕЛЛЕКТУАЛЬНОГО АНАЛИЗА
╚══════════════════════════════════════════════════════════════════════════════════════════════════════
"""
        
        return report
    
    def _generate_automatic_recommendations(self, findings: Dict) -> List[str]:
        """Генерирует автоматические рекомендации на основе находок"""
        
        recommendations = []
        
        # Рекомендации на основе критических находок
        for finding in findings['critical_findings']:
            if finding['type'] == 'platform_disparity':
                recommendations.append(f"Перераспределить бюджет в пользу платформы {finding['better_platform']} (преимущество {finding['percentage_diff']:.0f}%)")
            
            elif finding['type'] == 'statistical_anomaly':
                recommendations.append(f"Провести аудит ресторана {finding['restaurant']} по метрике {finding['metric']}")
            
            elif finding['type'] == 'trend_change' and finding['change_percent'] < -30:
                recommendations.append(f"Срочно разобраться с падением показателей ресторана {finding['restaurant']}")
        
        # Рекомендации на основе корреляций
        for correlation in findings['hidden_correlations']:
            if correlation['correlation'] > 0.8:
                recommendations.append(f"Использовать связь между {correlation['metric1']} и {correlation['metric2']} для оптимизации")
        
        # Общие рекомендации
        if len(findings['critical_findings']) == 0:
            recommendations.append("Система работает стабильно - продолжить текущую стратегию")
        
        if not recommendations:
            recommendations.append("Регулярно проводить интеллектуальный анализ для раннего выявления проблем")
        
        return recommendations[:10]  # Максимум 10 рекомендаций