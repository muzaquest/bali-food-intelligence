"""
Анализаторы продаж
Модули для детективного анализа падений продаж и ML прогнозирования
"""

from .production_sales_analyzer import ProductionSalesAnalyzer
from .enhanced_executive_summary import EnhancedExecutiveSummary

# ML интеграция (опциональная)
try:
    from .integrated_ml_detective import IntegratedMLDetective, ProperMLDetectiveAnalysis
    __all__ = ['ProductionSalesAnalyzer', 'EnhancedExecutiveSummary', 'IntegratedMLDetective', 'ProperMLDetectiveAnalysis']
except ImportError:
    __all__ = ['ProductionSalesAnalyzer', 'EnhancedExecutiveSummary']