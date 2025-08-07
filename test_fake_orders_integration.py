#!/usr/bin/env python3
"""
Тест интеграции fake orders с ProductionSalesAnalyzer
"""

import sys
import os

# Добавляем путь к src
sys.path.append('src')
sys.path.append('.')

from src.analyzers.production_sales_analyzer import ProductionSalesAnalyzer

def test_fake_orders_integration():
    print("🧪 ТЕСТИРОВАНИЕ ИНТЕГРАЦИИ FAKE ORDERS")
    print("=" * 60)
    
    try:
        # Создаем анализатор
        analyzer = ProductionSalesAnalyzer()
        
        # Тестируем рестораны с известными fake orders
        test_cases = [
            {"restaurant": "Only Eggs", "date": "2025-07-29"},  # День с максимальным количеством fake orders
            {"restaurant": "Pinkman", "date": "2025-07-03"},    # Активная дата
            {"restaurant": "Ika Canggu", "date": "2025-06-10"}  # Еще одна активная дата
        ]
        
        for test_case in test_cases:
            restaurant = test_case["restaurant"]
            date = test_case["date"]
            
            print(f"\n🔍 ТЕСТ: {restaurant} на {date}")
            print("-" * 40)
            
            # Получаем данные за день
            day_data = analyzer._get_day_data(restaurant, date)
            
            if day_data:
                print(f"📊 Данные найдены:")
                print(f"   Grab: {day_data['grab_sales']:,.0f} IDR ({day_data['grab_orders']} заказов)")
                print(f"   Gojek: {day_data['gojek_sales']:,.0f} IDR ({day_data['gojek_orders']} заказов)")
                print(f"   Всего: {day_data['total_sales']:,.0f} IDR ({day_data['total_orders']} заказов)")
                
                # Проверяем наличие fake orders
                if 'fake_orders_detected' in day_data:
                    fake_info = day_data['fake_orders_detected']
                    print(f"🚨 FAKE ORDERS ОБНАРУЖЕНЫ:")
                    print(f"   Grab fake: {fake_info['grab_fake_orders']} заказов ({fake_info['grab_fake_amount']:,.0f} IDR)")
                    print(f"   Gojek fake: {fake_info['gojek_fake_orders']} заказов ({fake_info['gojek_fake_amount']:,.0f} IDR)")
                    
                    total_fake = fake_info['grab_fake_orders'] + fake_info['gojek_fake_orders']
                    total_fake_amount = fake_info['grab_fake_amount'] + fake_info['gojek_fake_amount']
                    print(f"   ⚠️ ВСЕГО ИСКЛЮЧЕНО: {total_fake} fake orders ({total_fake_amount:,.0f} IDR)")
                else:
                    print("✅ Fake orders не обнаружены")
                    
            else:
                print("❌ Данные не найдены")
        
        # Тест полного анализа
        print(f"\n🎯 ПОЛНЫЙ АНАЛИЗ С FAKE ORDERS ФИЛЬТРАЦИЕЙ")
        print("=" * 60)
        
        result = analyzer.analyze_restaurant_performance("Only Eggs", "2025-07-29", "2025-07-29", use_ml=False)
        
        print("📋 РЕЗУЛЬТАТ АНАЛИЗА:")
        for line in result[:20]:  # Первые 20 строк
            print(line)
            
        # Проверяем упоминание fake orders в отчете
        fake_mentioned = any("FAKE ORDERS" in line for line in result)
        print(f"\n🔍 Fake orders упомянуты в отчете: {'✅ Да' if fake_mentioned else '❌ Нет'}")
        
    except Exception as e:
        print(f"❌ Ошибка тестирования: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_fake_orders_integration()