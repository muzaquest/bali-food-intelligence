#!/usr/bin/env python3
"""
Быстрое переобучение модели без оптимизации гиперпараметров
"""

import logging
import sys
from model import train_sales_model

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def main():
    """Быстрое переобучение модели"""
    
    print("⚡ БЫСТРОЕ ПЕРЕОБУЧЕНИЕ МОДЕЛИ")
    print("=" * 50)
    
    try:
        logger.info("Начинаем быстрое переобучение...")
        
        # Обучаем модель БЕЗ оптимизации гиперпараметров (быстрее)
        predictor = train_sales_model(
            start_date=None,
            end_date=None,
            model_type='random_forest',
            optimize_hyperparams=False,  # Быстрее!
            save_model=True
        )
        
        if predictor:
            print("\n✅ МОДЕЛЬ УСПЕШНО ПЕРЕОБУЧЕНА!")
            print("=" * 50)
            
            # Получаем важность признаков
            importance = predictor.get_feature_importance()
            print("\n📊 ТОП-10 ВАЖНЫХ ПРИЗНАКОВ:")
            if hasattr(importance, 'head'):
                for feature, score in importance.head(10).items():
                    print(f"  {feature}: {score:.4f}")
            else:
                print("  Важность признаков недоступна")
            
            # Получаем метрики
            metrics = predictor.training_metrics
            if metrics:
                print(f"\n📈 КАЧЕСТВО МОДЕЛИ:")
                print(f"  R² (обучение): {metrics.get('train_r2', 'N/A'):.4f}")
                print(f"  R² (тест): {metrics.get('test_r2', 'N/A'):.4f}")
                print(f"  MAE (тест): {metrics.get('test_mae', 'N/A'):.2f}")
            
            print(f"\n💾 Модель сохранена в: models/client_sales_model.joblib")
            print("🎉 Переобучение завершено успешно!")
            
        else:
            print("❌ Ошибка при переобучении модели")
            return 1
            
    except Exception as e:
        logger.error(f"Ошибка переобучения: {e}")
        print(f"\n❌ КРИТИЧЕСКАЯ ОШИБКА: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(main())