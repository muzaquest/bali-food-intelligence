"""
Анализаторы продаж
Модули для детективного анализа падений продаж и ML прогнозирования
"""

from .production_sales_analyzer import ProductionSalesAnalyzer

# ML интеграция (опциональная)
try:
    from .integrated_ml_detective import IntegratedMLDetective, ProperMLDetectiveAnalysis
    __all__ = ['ProductionSalesAnalyzer', 'IntegratedMLDetective', 'ProperMLDetectiveAnalysis']
except ImportError:
    __all__ = ['ProductionSalesAnalyzer']