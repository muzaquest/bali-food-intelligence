#!/usr/bin/env python3
"""
Скрипт для переобучения модели после исправления data leakage
"""

import logging
import sys
from datetime import datetime
from model import train_sales_model

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def main():
    """Переобучение модели с исправленными признаками"""
    
    print("🚀 ПЕРЕОБУЧЕНИЕ МОДЕЛИ ПОСЛЕ ИСПРАВЛЕНИЯ DATA LEAKAGE")
    print("=" * 60)
    
    try:
        logger.info("Начинаем переобучение модели...")
        
        # Обучаем модель с оптимизацией гиперпараметров
        predictor = train_sales_model(
            start_date=None,
            end_date=None,
            model_type='random_forest',
            optimize_hyperparams=True,
            save_model=True
        )
        
        if predictor and predictor.is_trained:
            print("\n✅ МОДЕЛЬ УСПЕШНО ПЕРЕОБУЧЕНА!")
            print("=" * 40)
            
            # Выводим метрики
            metrics = predictor.training_metrics
            print(f"📊 R² Score: {metrics.get('test_r2', 'N/A'):.4f}")
            print(f"📊 MAE: {metrics.get('test_mae', 'N/A'):.2f}")
            print(f"📊 RMSE: {metrics.get('test_rmse', 'N/A'):.2f}")
            print(f"🔧 Количество признаков: {metrics.get('feature_count', 'N/A')}")
            print(f"📈 Cross-validation R²: {metrics.get('cv_r2_mean', 'N/A'):.4f} ± {metrics.get('cv_r2_std', 'N/A'):.4f}")
            
            # Проверяем реалистичность метрик
            r2_score = metrics.get('test_r2', 0)
            if r2_score > 0.95:
                print("\n⚠️  ВНИМАНИЕ: R² очень высокий - возможна утечка данных!")
            elif r2_score > 0.8:
                print("\n✅ ОТЛИЧНЫЙ результат: модель точная, но реалистичная")
            elif r2_score > 0.5:
                print("\n✅ ХОРОШИЙ результат: модель полезная и реалистичная")
            elif r2_score > 0.3:
                print("\n✅ ПРИЕМЛЕМЫЙ результат: модель выявляет паттерны")
            else:
                print("\n⚠️  НИЗКИЙ результат: может требоваться дополнительная настройка")
            
            # Показываем важность признаков
            print("\n🔍 ТОП-10 ВАЖНЫХ ПРИЗНАКОВ:")
            print("-" * 40)
            importance_df = predictor.get_feature_importance(top_n=10)
            for idx, row in importance_df.iterrows():
                print(f"  {idx+1:2d}. {row['feature']:20s} {row['importance']:.4f}")
            
            print(f"\n💾 Модель сохранена в: models/client_sales_model.joblib")
            print(f"📋 Метаданные: models/client_model_metadata.json")
            
            print("\n🎉 ГОТОВО! Модель готова к использованию!")
            return True
            
        else:
            print("\n❌ ОШИБКА: Не удалось обучить модель")
            logger.error("Обучение модели завершилось неудачно")
            return False
            
    except Exception as e:
        print(f"\n❌ КРИТИЧЕСКАЯ ОШИБКА: {e}")
        logger.error(f"Ошибка переобучения: {e}")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)