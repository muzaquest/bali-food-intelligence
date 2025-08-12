"""
Анализаторы продаж
Модули для детективного анализа падений продаж и ML прогнозирования
"""

from .production_sales_analyzer import ProductionSalesAnalyzer
from .enhanced_executive_summary import EnhancedExecutiveSummary

# Совместимость: предоставляем ProfessionalDetectiveAnalyzer как алиас
ProfessionalDetectiveAnalyzer = ProductionSalesAnalyzer

# ML интеграция (опциональная)
try:
    from .integrated_ml_detective import IntegratedMLDetective, ProperMLDetectiveAnalysis
    __all__ = ['ProductionSalesAnalyzer', 'EnhancedExecutiveSummary', 'IntegratedMLDetective', 'ProperMLDetectiveAnalysis', 'ProfessionalDetectiveAnalyzer']
except ImportError:
    __all__ = ['ProductionSalesAnalyzer', 'EnhancedExecutiveSummary', 'ProfessionalDetectiveAnalyzer']
