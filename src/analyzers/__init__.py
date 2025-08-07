"""
Анализаторы продаж
Модули для детективного анализа падений продаж и ML прогнозирования
"""

from .production_sales_analyzer import ProductionSalesAnalyzer
from .enhanced_executive_summary import EnhancedExecutiveSummary
from .professional_detective_analyzer import ProfessionalDetectiveAnalyzer

# ML интеграция (опциональная)
try:
    from .integrated_ml_detective import IntegratedMLDetective, ProperMLDetectiveAnalysis
    __all__ = ['ProductionSalesAnalyzer', 'EnhancedExecutiveSummary', 'ProfessionalDetectiveAnalyzer', 'IntegratedMLDetective', 'ProperMLDetectiveAnalysis']
except ImportError:
    __all__ = ['ProductionSalesAnalyzer', 'EnhancedExecutiveSummary', 'ProfessionalDetectiveAnalyzer']