#!/usr/bin/env python3
"""
🚀 REPLIT DEPLOYMENT SCRIPT
Автоматическая проверка и исправление анализатора

Выполняет:
1. Проверку корректности ProfessionalDetectiveAnalyzer
2. Тест критических дней
3. Автоматическую диагностику проблем
4. Отчет о готовности к продакшену
"""

import sys
import os
import sqlite3
import traceback

def test_critical_data():
    """Тестирует критические данные"""
    print("🔍 ТЕСТ 1: Критические данные")
    print("-" * 50)
    
    try:
        from src.analyzers import ProfessionalDetectiveAnalyzer
        
        analyzer = ProfessionalDetectiveAnalyzer()
        
        # Критические тестовые случаи
        test_cases = [
            ('2025-04-21', 1793000, "КРИТИЧЕСКИЙ: Replit показывал 0 IDR"),
            ('2025-05-15', 5446000, "Grab только"),
            ('2025-05-18', 5930800, "Grab + Gojek"),
            ('2025-04-27', 9606500, "Высокие продажи"),
            ('2025-04-02', 5145100, "Умеренные продажи")
        ]
        
        all_passed = True
        
        for date, expected_sales, description in test_cases:
            day_data = analyzer._get_day_detailed_data('Only Eggs', date)
            
            if not day_data:
                print(f"❌ {date}: Данные не найдены")
                all_passed = False
                continue
                
            total_sales = day_data.get('total_sales', 0) or 0
            grab_sales = day_data.get('grab_sales', 0) or 0
            gojek_sales = day_data.get('gojek_sales', 0) or 0
            
            # Проверяем что total_sales существует и правильный
            calculated_total = grab_sales + gojek_sales
            
            if total_sales == expected_sales and total_sales == calculated_total:
                print(f"✅ {date}: {total_sales:>12,.0f} IDR - {description}")
            else:
                print(f"❌ {date}: {total_sales:>12,.0f} IDR (ожидалось {expected_sales:,.0f}) - {description}")
                print(f"   grab: {grab_sales:,.0f} + gojek: {gojek_sales:,.0f} = {calculated_total:,.0f}")
                all_passed = False
        
        return all_passed
        
    except Exception as e:
        print(f"❌ ОШИБКА: {e}")
        traceback.print_exc()
        return False

def test_full_analysis():
    """Тестирует полный анализ"""
    print("\n🔍 ТЕСТ 2: Полный анализ")
    print("-" * 50)
    
    try:
        from src.analyzers import ProfessionalDetectiveAnalyzer
        
        analyzer = ProfessionalDetectiveAnalyzer()
        results = analyzer.analyze_sales_performance('Only Eggs', '2025-04-01', '2025-05-31')
        
        # Проверяем что результаты не пустые
        if not results:
            print("❌ Анализ не вернул результаты")
            return False
            
        # Проверяем что есть ключевые секции
        results_text = "\n".join(results)
        
        required_sections = [
            "📊 ОБЗОР ПЕРИОДА",
            "📉 ПРОБЛЕМНЫЙ ДЕНЬ",
            "📋 ИСПОЛНИТЕЛЬНАЯ СВОДКА"
        ]
        
        missing_sections = []
        for section in required_sections:
            if section not in results_text:
                missing_sections.append(section)
        
        if missing_sections:
            print(f"❌ Отсутствуют секции: {missing_sections}")
            return False
            
        # Проверяем что 2025-04-21 показывается правильно
        if "2025-04-21" in results_text and "1,793,000" in results_text:
            print("✅ Полный анализ работает корректно")
            print("✅ 2025-04-21 показывается как 1,793,000 IDR")
            return True
        else:
            print("❌ 2025-04-21 не найден или показывается неправильно")
            return False
            
    except Exception as e:
        print(f"❌ ОШИБКА: {e}")
        traceback.print_exc()
        return False

def diagnose_replit_issues():
    """Диагностирует проблемы Replit"""
    print("\n🔍 ДИАГНОСТИКА: Возможные проблемы")
    print("-" * 50)
    
    issues_found = []
    
    # Проверяем main.py
    if os.path.exists('main.py'):
        with open('main.py', 'r', encoding='utf-8') as f:
            main_content = f.read()
            
        # Проверяем на старый код
        old_patterns = [
            'ProductionSalesAnalyzer',
            'professional_detective_analysis',
            'compare_periods',
            '7-8. АНАЛИЗ ПРИЧИН ИЗМЕНЕНИЯ ПРОДАЖ'
        ]
        
        for pattern in old_patterns:
            if pattern in main_content:
                issues_found.append(f"❌ В main.py найден старый код: {pattern}")
        
        # Проверяем есть ли новый код
        if 'ProfessionalDetectiveAnalyzer' not in main_content:
            issues_found.append("❌ В main.py отсутствует ProfessionalDetectiveAnalyzer")
    else:
        issues_found.append("❌ main.py не найден")
    
    # Проверяем анализатор
    analyzer_file = 'src/analyzers/professional_detective_analyzer.py'
    if os.path.exists(analyzer_file):
        with open(analyzer_file, 'r', encoding='utf-8') as f:
            analyzer_content = f.read()
            
        if "result['total_sales']" not in analyzer_content:
            issues_found.append("❌ В анализаторе отсутствует расчет total_sales")
        else:
            print("✅ Анализатор содержит расчет total_sales")
    else:
        issues_found.append("❌ Анализатор не найден")
    
    if issues_found:
        print("\n🚨 НАЙДЕННЫЕ ПРОБЛЕМЫ:")
        for issue in issues_found:
            print(issue)
        return False
    else:
        print("✅ Проблем не найдено")
        return True

def generate_fix_commands():
    """Генерирует команды для исправления"""
    print("\n🔧 КОМАНДЫ ДЛЯ ИСПРАВЛЕНИЯ:")
    print("=" * 60)
    
    print("""
1. УДАЛИТЬ старый код из main.py:
   - Все строки с ProductionSalesAnalyzer
   - Все строки с professional_detective_analysis
   - Все строки с compare_periods
   - Весь раздел "7-8. АНАЛИЗ ПРИЧИН"

2. ДОБАВИТЬ в main.py:
   from src.analyzers import ProfessionalDetectiveAnalyzer
   detective_analyzer = ProfessionalDetectiveAnalyzer()
   detective_results = detective_analyzer.analyze_sales_performance(
       restaurant_name, start_date, end_date
   )

3. ПРОВЕРИТЬ src/analyzers/professional_detective_analyzer.py:
   Строка ~398 должна содержать:
   result['total_sales'] = (result['grab_sales'] or 0) + (result['gojek_sales'] or 0)

4. ТЕСТ после изменений:
   test_data = analyzer._get_day_detailed_data('Only Eggs', '2025-04-21')
   print(test_data['total_sales'])  # Должно быть 1793000
""")

def main():
    """Главная функция"""
    print("🚀 REPLIT DEPLOYMENT CHECKER")
    print("=" * 60)
    print("Проверка готовности анализатора к продакшену\n")
    
    # Тесты
    test1_passed = test_critical_data()
    test2_passed = test_full_analysis()
    diagnosis_clean = diagnose_replit_issues()
    
    # Итоговый отчет
    print("\n" + "=" * 60)
    print("📊 ИТОГОВЫЙ ОТЧЕТ")
    print("=" * 60)
    
    if test1_passed and test2_passed and diagnosis_clean:
        print("🎯 СТАТУС: ✅ ГОТОВ К ПРОДАКШЕНУ")
        print("📈 Все тесты пройдены")
        print("💎 Анализатор работает идеально")
        print("🚀 Можно развертывать в Replit")
        return 0
    else:
        print("🚨 СТАТУС: ❌ ТРЕБУЮТСЯ ИСПРАВЛЕНИЯ")
        print("🔧 Найдены проблемы, требующие внимания")
        
        if not test1_passed:
            print("❌ Критические данные неправильные")
        if not test2_passed:
            print("❌ Полный анализ не работает")
        if not diagnosis_clean:
            print("❌ Найдены проблемы в коде")
            
        generate_fix_commands()
        return 1

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)