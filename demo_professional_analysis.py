#!/usr/bin/env python3
"""
🔍 ДЕМО ПРОФЕССИОНАЛЬНОГО АНАЛИЗА ПРОДАЖ
═══════════════════════════════════════════════════════════════════════════════
Запуск: python3 demo_professional_analysis.py
"""

import sys
import os

# Добавляем текущую папку в путь для импорта
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from clean_professional_analysis import analyze_sales_changes_professional

def main():
    """Демо профессионального анализа"""
    
    print("🔍 ДЕМО ПРОФЕССИОНАЛЬНОГО ДЕТЕКТИВНОГО АНАЛИЗА")
    print("=" * 60)
    print("💼 Анализ как настоящий аналитик - фокус на конкретных проблемах")
    print("=" * 60)
    print()
    
    # Примеры анализа
    restaurants = ["Only Eggs", "Geprek Bensu", "Warung Bebek"]
    
    for restaurant in restaurants:
        print(f"\n🍽️ РЕСТОРАН: {restaurant}")
        print("-" * 50)
        
        try:
            analyze_sales_changes_professional(
                restaurant,
                "2025-04-01", "2025-05-31",  # Анализируемый период
                "2025-01-30", "2025-03-31"   # Предыдущий период для сравнения
            )
        except Exception as e:
            print(f"❌ Ошибка анализа для {restaurant}: {e}")
        
        print("\n" + "="*60)
        
    print("\n🎯 ДЕМО ЗАВЕРШЕНО!")
    print("💡 Как видите, анализ:")
    print("   • Находит конкретные проблемные дни")
    print("   • Анализирует реальные причины")
    print("   • Обнаруживает проблемы с платформами")
    print("   • Дает готовый ответ клиенту")
    print("   • БЕЗ эмодзи-бардака!")

if __name__ == "__main__":
    main()