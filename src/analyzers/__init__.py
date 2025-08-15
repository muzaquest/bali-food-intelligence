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

# Экспорт профессионального детективного анализатора (если доступен)
try:
	from .professional_detective_analyzer import ProfessionalDetectiveAnalyzer  # type: ignore
	__all__.append('ProfessionalDetectiveAnalyzer')  # type: ignore
except Exception:
	# Фолбэк на резервную версию из backup_ml_*/
	try:
		from backup_ml_20250809_175202.professional_detective_analyzer import ProfessionalDetectiveAnalyzer  # type: ignore
		__all__.append('ProfessionalDetectiveAnalyzer')  # type: ignore
	except Exception:
		pass
