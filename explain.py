"""
Модуль для объяснения предсказаний с помощью SHAP
"""
import pandas as pd
import numpy as np
import shap
import matplotlib.pyplot as plt
import seaborn as sns
import logging
from datetime import datetime
import os

from model import load_trained_model
from data_loader import get_restaurant_data
from utils import generate_explanation, create_result_json, save_result_to_file
from config import RESULTS_PATH

logger = logging.getLogger(__name__)

class SalesExplainer:
    """Класс для объяснения предсказаний изменений продаж"""
    
    def __init__(self, model_path=None):
        self.predictor = load_trained_model(model_path)
        self.explainer = None
        self.shap_values = None
        
        if self.predictor is None:
            logger.error("Не удалось загрузить модель")
            return
        
        logger.info("Модель загружена успешно")
        
    def _create_explainer(self, background_data):
        """Создание SHAP explainer"""
        logger.info("Создание SHAP explainer...")
        
        try:
            # Для Random Forest используем TreeExplainer
            if self.predictor.model_type == 'random_forest':
                self.explainer = shap.TreeExplainer(self.predictor.model)
            else:
                # Для других моделей используем KernelExplainer
                self.explainer = shap.KernelExplainer(
                    self.predictor.model.predict,
                    background_data
                )
            
            logger.info("SHAP explainer создан успешно")
            return True
            
        except Exception as e:
            logger.error(f"Ошибка создания SHAP explainer: {e}")
            return False
    
    def explain_prediction(self, df, restaurant_name, date, save_plots=True):
        """Объяснение предсказания для конкретного случая"""
        if self.predictor is None:
            logger.error("Модель не загружена")
            return None
        
        logger.info(f"Объяснение предсказания для {restaurant_name} на {date}")
        
        # Получаем данные ресторана
        restaurant_df = get_restaurant_data(restaurant_name)
        if restaurant_df is None:
            logger.error(f"Данные для ресторана {restaurant_name} не найдены")
            return None
        
        # Подготавливаем данные для объяснения
        df_prepared = self.predictor.feature_engineer.prepare_features(restaurant_df)
        
        # Создаем background data для SHAP (выборка из исторических данных)
        background_sample = df_prepared.sample(min(100, len(df_prepared)), random_state=42)
        background_features = background_sample[self.predictor.feature_names]
        
        # Создаем explainer
        if not self._create_explainer(background_features):
            return None
        
        # Подготавливаем конкретный образец
        sample_data = df_prepared[
            (df_prepared['restaurant_name'] == restaurant_name) & 
            (df_prepared['date'] == date)
        ]
        
        if sample_data.empty:
            logger.error(f"Данные для {restaurant_name} на {date} не найдены")
            return None
        
        X_sample = sample_data[self.predictor.feature_names]
        
        # Получаем SHAP values
        try:
            if self.predictor.model_type == 'random_forest':
                shap_values = self.explainer.shap_values(X_sample)
            else:
                shap_values = self.explainer.shap_values(X_sample, nsamples=100)
            
            # Если shap_values многомерный, берем первый элемент
            if len(shap_values.shape) > 1:
                shap_values = shap_values[0]
            
            self.shap_values = shap_values
            
        except Exception as e:
            logger.error(f"Ошибка вычисления SHAP values: {e}")
            return None
        
        # Делаем предсказание
        prediction_result = self.predictor.predict(df_prepared, restaurant_name, date)
        if prediction_result is None:
            logger.error("Не удалось сделать предсказание")
            return None
        
        # Генерируем объяснение
        explanation = generate_explanation(
            shap_values, 
            self.predictor.feature_names,
            threshold=0.01
        )
        
        # Создаем результат
        result = create_result_json(
            restaurant_name,
            date,
            prediction_result.get('actual_sales', 0),
            prediction_result.get('predicted_sales', 0),
            shap_values,
            self.predictor.feature_names,
            explanation
        )
        
        # Сохраняем графики
        if save_plots:
            self._save_explanation_plots(
                restaurant_name, 
                date, 
                X_sample, 
                shap_values
            )
        
        return result
    
    def _save_explanation_plots(self, restaurant_name, date, X_sample, shap_values):
        """Сохранение графиков объяснения"""
        try:
            # Создаем директорию для результатов
            results_dir = os.path.join(RESULTS_PATH, f"{restaurant_name}_{date}")
            os.makedirs(results_dir, exist_ok=True)
            
            # 1. Waterfall plot
            plt.figure(figsize=(12, 8))
            shap.waterfall_plot(
                shap.Explanation(
                    values=shap_values,
                    base_values=self.explainer.expected_value,
                    data=X_sample.iloc[0].values,
                    feature_names=self.predictor.feature_names
                ),
                show=False
            )
            plt.title(f'SHAP Waterfall Plot - {restaurant_name} ({date})')
            plt.tight_layout()
            plt.savefig(os.path.join(results_dir, 'waterfall_plot.png'), dpi=300, bbox_inches='tight')
            plt.close()
            
            # 2. Bar plot с топ-10 признаков
            plt.figure(figsize=(10, 6))
            shap_df = pd.DataFrame({
                'feature': self.predictor.feature_names,
                'shap_value': shap_values,
                'abs_shap': np.abs(shap_values)
            }).sort_values('abs_shap', ascending=False).head(10)
            
            colors = ['red' if x < 0 else 'green' for x in shap_df['shap_value']]
            plt.barh(range(len(shap_df)), shap_df['shap_value'], color=colors)
            plt.yticks(range(len(shap_df)), shap_df['feature'])
            plt.xlabel('SHAP Value')
            plt.title(f'Top 10 Feature Contributions - {restaurant_name} ({date})')
            plt.grid(axis='x', alpha=0.3)
            plt.tight_layout()
            plt.savefig(os.path.join(results_dir, 'feature_importance.png'), dpi=300, bbox_inches='tight')
            plt.close()
            
            # 3. Таблица с детальными значениями
            detailed_df = pd.DataFrame({
                'Признак': self.predictor.feature_names,
                'Значение': X_sample.iloc[0].values,
                'SHAP_вклад': shap_values,
                'Абс_вклад': np.abs(shap_values)
            }).sort_values('Абс_вклад', ascending=False)
            
            detailed_df.to_csv(
                os.path.join(results_dir, 'detailed_explanation.csv'),
                index=False,
                encoding='utf-8'
            )
            
            logger.info(f"Графики сохранены в {results_dir}")
            
        except Exception as e:
            logger.error(f"Ошибка сохранения графиков: {e}")
    
    def batch_explain(self, df, restaurant_name, date_range):
        """Объяснение для нескольких дат"""
        results = []
        
        for date in date_range:
            try:
                result = self.explain_prediction(df, restaurant_name, date, save_plots=False)
                if result:
                    results.append(result)
                    logger.info(f"Обработано: {restaurant_name} - {date}")
            except Exception as e:
                logger.error(f"Ошибка обработки {restaurant_name} - {date}: {e}")
                continue
        
        return results
    
    def get_global_feature_importance(self, df, sample_size=1000):
        """Глобальная важность признаков на основе SHAP"""
        if self.predictor is None:
            logger.error("Модель не загружена")
            return None
        
        logger.info("Вычисление глобальной важности признаков...")
        
        # Подготавливаем данные
        df_prepared = self.predictor.feature_engineer.prepare_features(df)
        
        # Берем случайную выборку
        sample_df = df_prepared.sample(min(sample_size, len(df_prepared)), random_state=42)
        X_sample = sample_df[self.predictor.feature_names]
        
        # Создаем explainer
        background_sample = X_sample.sample(min(100, len(X_sample)), random_state=42)
        if not self._create_explainer(background_sample):
            return None
        
        # Вычисляем SHAP values
        try:
            if self.predictor.model_type == 'random_forest':
                shap_values = self.explainer.shap_values(X_sample)
            else:
                shap_values = self.explainer.shap_values(X_sample, nsamples=100)
            
            # Средняя абсолютная важность
            mean_shap = np.mean(np.abs(shap_values), axis=0)
            
            importance_df = pd.DataFrame({
                'feature': self.predictor.feature_names,
                'mean_abs_shap': mean_shap
            }).sort_values('mean_abs_shap', ascending=False)
            
            return importance_df
            
        except Exception as e:
            logger.error(f"Ошибка вычисления глобальной важности: {e}")
            return None
    
    def create_summary_report(self, restaurant_name, date_range):
        """Создание сводного отчета"""
        logger.info(f"Создание сводного отчета для {restaurant_name}")
        
        # Получаем данные
        df = get_restaurant_data(restaurant_name)
        if df is None:
            return None
        
        # Анализируем каждую дату
        results = self.batch_explain(df, restaurant_name, date_range)
        
        if not results:
            logger.error("Нет результатов для анализа")
            return None
        
        # Создаем сводку
        summary = {
            'restaurant': restaurant_name,
            'period': f"{min(date_range)} - {max(date_range)}",
            'total_days': len(date_range),
            'analyzed_days': len(results),
            'average_change': np.mean([r['change_percent'] for r in results]),
            'top_positive_factors': {},
            'top_negative_factors': {},
            'recommendations': []
        }
        
        # Анализируем факторы
        all_factors = {}
        for result in results:
            for factor, impact in result['top_factors'].items():
                if factor not in all_factors:
                    all_factors[factor] = []
                all_factors[factor].append(impact)
        
        # Средние значения факторов
        avg_factors = {k: np.mean(v) for k, v in all_factors.items()}
        
        # Топ положительных и отрицательных факторов
        positive_factors = {k: v for k, v in avg_factors.items() if v > 0}
        negative_factors = {k: v for k, v in avg_factors.items() if v < 0}
        
        summary['top_positive_factors'] = dict(sorted(positive_factors.items(), 
                                                     key=lambda x: x[1], reverse=True)[:5])
        summary['top_negative_factors'] = dict(sorted(negative_factors.items(), 
                                                     key=lambda x: x[1])[:5])
        
        # Рекомендации
        if 'ads_on' in negative_factors:
            summary['recommendations'].append("Рассмотрите увеличение рекламных расходов")
        
        if 'rain_mm' in negative_factors:
            summary['recommendations'].append("Разработайте стратегию для дождливых дней")
        
        if 'rating' in negative_factors:
            summary['recommendations'].append("Работайте над улучшением рейтинга")
        
        return summary

def explain_sales_change(restaurant_name, date, model_path=None, save_results=True):
    """Главная функция для объяснения изменения продаж"""
    logger.info(f"Анализ изменения продаж для {restaurant_name} на {date}")
    
    # Создаем объяснитель
    explainer = SalesExplainer(model_path)
    if explainer.predictor is None:
        return None
    
    # Получаем данные
    df = get_restaurant_data(restaurant_name)
    if df is None:
        return None
    
    # Объясняем предсказание
    result = explainer.explain_prediction(df, restaurant_name, date)
    
    if result and save_results:
        # Сохраняем результат
        filename = os.path.join(RESULTS_PATH, f"{restaurant_name}_{date}_analysis.json")
        save_result_to_file(result, filename)
    
    return result

if __name__ == "__main__":
    # Пример использования
    result = explain_sales_change("Honeycomb", "2023-06-15")
    if result:
        print("Анализ завершен успешно!")
        print(f"Изменение продаж: {result['change_percent']}%")
        print(f"Объяснение: {result['explanation']}")
    else:
        print("Ошибка анализа")