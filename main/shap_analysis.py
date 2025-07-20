#!/usr/bin/env python3
"""
SHAP Analysis Module для объяснения важности признаков
Предоставляет детальный анализ влияния каждого признака на прогнозы модели
"""

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
# import seaborn as sns  # Закомментировано для совместимости
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
import warnings
warnings.filterwarnings('ignore')

try:
    import shap
    SHAP_AVAILABLE = True
except ImportError:
    SHAP_AVAILABLE = False
    print("⚠️ SHAP не установлен. Используйте: pip install shap")

class FeatureImportanceAnalyzer:
    """Анализатор важности признаков с использованием SHAP"""
    
    def __init__(self):
        self.model = None
        self.explainer = None
        self.shap_values = None
        self.feature_names = None
        self.X_sample = None
        
    def train_model_for_analysis(self, df, target_column='total_sales', sample_size=1000):
        """Обучение модели для SHAP анализа"""
        print("🤖 Обучение модели для SHAP анализа...")
        
        # Подготовка данных
        exclude_columns = ['date', 'restaurant_name', 'platform', 'holiday_name', 
                          'day_name', 'month_name', 'weather_condition', 'temp_category',
                          'rain_category', 'weather_combination', 'special_period_combination',
                          'day_category', target_column]
        
        feature_columns = [col for col in df.columns if col not in exclude_columns]
        
        X = df[feature_columns].fillna(0)
        y = df[target_column].fillna(0)
        
        # Берём выборку для анализа (SHAP может быть медленным на больших данных)
        if len(X) > sample_size:
            X = X.sample(n=sample_size, random_state=42)
            y = y.loc[X.index]
        
        self.feature_names = feature_columns
        self.X_sample = X
        
        # Обучение модели
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
        
        self.model = RandomForestRegressor(
            n_estimators=50,  # Меньше деревьев для скорости
            max_depth=15,
            random_state=42,
            n_jobs=1
        )
        
        self.model.fit(X_train, y_train)
        
        # Проверяем качество
        score = self.model.score(X_test, y_test)
        print(f"  📈 R² Score: {score:.3f}")
        print(f"  🔧 Количество признаков: {len(feature_columns)}")
        print(f"  📊 Размер выборки: {len(X)}")
        
        return score
    
    def calculate_shap_values(self, max_display=20):
        """Вычисление SHAP значений"""
        if not SHAP_AVAILABLE:
            print("❌ SHAP не доступен. Используем стандартный feature importance")
            return self.get_basic_feature_importance(max_display)
        
        if self.model is None:
            print("❌ Модель не обучена")
            return None
            
        print("🔍 Вычисление SHAP значений...")
        
        try:
            # Создаём SHAP explainer
            self.explainer = shap.TreeExplainer(self.model)
            
            # Вычисляем SHAP значения для выборки
            sample_size = min(100, len(self.X_sample))  # Ограничиваем размер для скорости
            X_shap = self.X_sample.sample(n=sample_size, random_state=42)
            
            self.shap_values = self.explainer.shap_values(X_shap)
            
            print(f"  ✅ SHAP значения вычислены для {sample_size} записей")
            
            return self.analyze_shap_results(max_display)
            
        except Exception as e:
            print(f"❌ Ошибка при вычислении SHAP: {e}")
            print("🔄 Используем стандартный анализ важности")
            return self.get_basic_feature_importance(max_display)
    
    def analyze_shap_results(self, max_display=20):
        """Анализ результатов SHAP"""
        if self.shap_values is None:
            return None
            
        # Средняя абсолютная важность каждого признака
        mean_abs_shap = np.abs(self.shap_values).mean(axis=0)
        
        # Создаём DataFrame с результатами
        importance_df = pd.DataFrame({
            'feature': self.feature_names,
            'shap_importance': mean_abs_shap,
            'mean_shap_value': self.shap_values.mean(axis=0),
            'abs_mean_shap': np.abs(self.shap_values.mean(axis=0))
        }).sort_values('shap_importance', ascending=False)
        
        print(f"\n🔝 ТОП-{max_display} ВАЖНЫХ ПРИЗНАКОВ (SHAP):")
        print("=" * 60)
        
        for i, row in importance_df.head(max_display).iterrows():
            direction = "📈" if row['mean_shap_value'] > 0 else "📉"
            print(f"  {direction} {row['feature']:<40} {row['shap_importance']:.4f}")
        
        return importance_df
    
    def get_basic_feature_importance(self, max_display=20):
        """Базовый анализ важности признаков без SHAP"""
        if self.model is None:
            print("❌ Модель не обучена")
            return None
            
        importance_df = pd.DataFrame({
            'feature': self.feature_names,
            'importance': self.model.feature_importances_
        }).sort_values('importance', ascending=False)
        
        print(f"\n🔝 ТОП-{max_display} ВАЖНЫХ ПРИЗНАКОВ (стандартный):")
        print("=" * 60)
        
        for i, row in importance_df.head(max_display).iterrows():
            print(f"  📊 {row['feature']:<40} {row['importance']:.4f}")
        
        return importance_df
    
    def analyze_feature_categories(self, importance_df):
        """Анализ важности по категориям признаков"""
        if importance_df is None:
            return
            
        print(f"\n📋 АНАЛИЗ ПО КАТЕГОРИЯМ ПРИЗНАКОВ:")
        print("=" * 60)
        
        # Определяем категории признаков
        categories = {
            'Базовые метрики': ['total_sales', 'total_orders', 'rating', 'delivery_time'],
            'Клиентские': ['customer_', 'retention', 'lifetime', 'churn'],
            'Качество': ['quality_', 'food_', 'service_', 'presentation', 'freshness'],
            'Маркетинг': ['promotion', 'viral', 'social_media', 'brand_awareness'],
            'Погода': ['weather_', 'temperature', 'rain_', 'humidity'],
            'Календарь': ['weekend', 'holiday', 'month_', 'day_', 'season'],
            'Лаги': ['_lag_', '_prev_'],
            'Скользящие': ['_rolling_', '_ma_'],
            'Тренды': ['_trend_', '_slope_'],
            'Взаимодействия': ['_x_', '_interaction_']
        }
        
        category_importance = {}
        
        for category, keywords in categories.items():
            category_features = []
            for _, row in importance_df.iterrows():
                feature = row['feature']
                if any(keyword in feature.lower() for keyword in keywords):
                    category_features.append(row)
            
            if category_features:
                # Используем shap_importance если доступно, иначе importance
                importance_col = 'shap_importance' if 'shap_importance' in importance_df.columns else 'importance'
                total_importance = sum(f[importance_col] for f in category_features)
                category_importance[category] = {
                    'total_importance': total_importance,
                    'feature_count': len(category_features),
                    'avg_importance': total_importance / len(category_features) if category_features else 0
                }
        
        # Сортируем категории по важности
        sorted_categories = sorted(category_importance.items(), 
                                 key=lambda x: x[1]['total_importance'], 
                                 reverse=True)
        
        for category, stats in sorted_categories:
            print(f"  📂 {category:<20} Важность: {stats['total_importance']:.3f} "
                  f"({stats['feature_count']} признаков)")
    
    def generate_feature_insights(self, importance_df, top_n=10):
        """Генерация инсайтов о важности признаков"""
        if importance_df is None:
            return []
            
        insights = []
        importance_col = 'shap_importance' if 'shap_importance' in importance_df.columns else 'importance'
        
        top_features = importance_df.head(top_n)
        total_importance = importance_df[importance_col].sum()
        top_importance_share = top_features[importance_col].sum() / total_importance
        
        insights.append(f"🎯 ТОП-{top_n} признаков объясняют {top_importance_share:.1%} от общей важности модели")
        
        # Анализ доминирующих признаков
        most_important = top_features.iloc[0]
        if most_important[importance_col] > 0.1:  # Более 10%
            insights.append(f"⭐ Доминирующий признак: '{most_important['feature']}' "
                          f"({most_important[importance_col]:.1%} важности)")
        
        # Анализ распределения важности
        high_importance = len(importance_df[importance_df[importance_col] > 0.01])  # > 1%
        medium_importance = len(importance_df[(importance_df[importance_col] > 0.001) & 
                                            (importance_df[importance_col] <= 0.01)])  # 0.1%-1%
        
        insights.append(f"📊 Высокая важность: {high_importance} признаков, "
                       f"средняя: {medium_importance} признаков")
        
        # SHAP-специфичные инсайты
        if 'mean_shap_value' in importance_df.columns:
            positive_impact = len(importance_df[importance_df['mean_shap_value'] > 0])
            negative_impact = len(importance_df[importance_df['mean_shap_value'] < 0])
            
            insights.append(f"📈 Положительное влияние: {positive_impact} признаков, "
                           f"отрицательное: {negative_impact} признаков")
        
        return insights
    
    def create_summary_report(self, importance_df):
        """Создание итогового отчёта по важности признаков"""
        print(f"\n📊 ИТОГОВЫЙ ОТЧЁТ ПО ВАЖНОСТИ ПРИЗНАКОВ")
        print("=" * 60)
        
        if importance_df is None:
            print("❌ Нет данных для анализа")
            return
        
        # Общая статистика
        total_features = len(importance_df)
        importance_col = 'shap_importance' if 'shap_importance' in importance_df.columns else 'importance'
        
        print(f"  🔢 Общее количество признаков: {total_features}")
        print(f"  📊 Метод анализа: {'SHAP' if 'shap_importance' in importance_df.columns else 'Стандартный'}")
        
        # Анализ по категориям
        self.analyze_feature_categories(importance_df)
        
        # Генерация инсайтов
        insights = self.generate_feature_insights(importance_df)
        
        print(f"\n💡 КЛЮЧЕВЫЕ ИНСАЙТЫ:")
        print("=" * 60)
        for insight in insights:
            print(f"  {insight}")
        
        return {
            'total_features': total_features,
            'analysis_method': 'SHAP' if 'shap_importance' in importance_df.columns else 'Standard',
            'importance_data': importance_df,
            'insights': insights
        }

def run_comprehensive_feature_analysis(df, target_column='total_sales'):
    """Запуск полного анализа важности признаков"""
    print("🚀 ЗАПУСК КОМПЛЕКСНОГО АНАЛИЗА ВАЖНОСТИ ПРИЗНАКОВ")
    print("=" * 60)
    
    analyzer = FeatureImportanceAnalyzer()
    
    try:
        # Обучение модели
        score = analyzer.train_model_for_analysis(df, target_column)
        
        if score < 0.5:
            print("⚠️ Предупреждение: Низкое качество модели. Результаты могут быть неточными.")
        
        # Анализ важности признаков
        importance_df = analyzer.calculate_shap_values(max_display=25)
        
        # Создание итогового отчёта
        report = analyzer.create_summary_report(importance_df)
        
        return report
        
    except Exception as e:
        print(f"❌ Ошибка при анализе: {e}")
        return None

if __name__ == "__main__":
    # Пример использования
    print("🔍 Модуль SHAP анализа важности признаков")
    print("Для использования импортируйте функцию run_comprehensive_feature_analysis")
    
    if not SHAP_AVAILABLE:
        print("\n📦 Для полного функционала установите SHAP:")
        print("pip install shap")
    else:
        print("\n✅ SHAP доступен - полный функционал активен")