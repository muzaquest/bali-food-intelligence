#!/usr/bin/env python3
"""
Тесты для анализаторов продаж
"""

import unittest
import sys
import os

# Добавляем src в путь
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from analyzers import ProductionSalesAnalyzer

class TestProductionSalesAnalyzer(unittest.TestCase):
    """Тесты для основного анализатора продаж"""
    
    def setUp(self):
        """Настройка тестов"""
        self.analyzer = ProductionSalesAnalyzer()
    
    def test_analyzer_initialization(self):
        """Тест инициализации анализатора"""
        self.assertIsNotNone(self.analyzer)
        self.assertIsNotNone(self.analyzer.holidays_data)
        self.assertIsNotNone(self.analyzer.locations_data)
    
    def test_holidays_data_loaded(self):
        """Тест загрузки данных о праздниках"""
        self.assertGreater(len(self.analyzer.holidays_data), 0)
        # Проверяем что есть данные за 2025 год
        has_2025_data = any('2025-' in date for date in self.analyzer.holidays_data.keys())
        self.assertTrue(has_2025_data)
    
    def test_locations_data_loaded(self):
        """Тест загрузки локаций ресторанов"""
        self.assertGreater(len(self.analyzer.locations_data), 0)
        # Проверяем что есть Only Eggs
        self.assertIn('Only Eggs', self.analyzer.locations_data)
        
        # Проверяем структуру данных локации
        only_eggs = self.analyzer.locations_data['Only Eggs']
        self.assertIn('latitude', only_eggs)
        self.assertIn('longitude', only_eggs)
    
    def test_analyze_specific_day_format(self):
        """Тест формата вывода анализа конкретного дня"""
        # Тестируем на известной дате
        result = self.analyzer._analyze_specific_day('Only Eggs', '2025-05-15')
        
        # Результат должен быть списком строк
        self.assertIsInstance(result, list)
        self.assertGreater(len(result), 0)
        
        # Первая строка должна содержать информацию о продажах
        first_line = result[0]
        self.assertIn('Продажи:', first_line)
        self.assertIn('IDR', first_line)

if __name__ == '__main__':
    unittest.main()