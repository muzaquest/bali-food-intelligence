#!/usr/bin/env python3
"""
🎯 MUZAQUEST ANALYTICS - ДЕМОНСТРАЦИЯ НА REPLIT
================================================================
"""

import os
import sys

def main():
    print("🎯 MUZAQUEST ANALYTICS - СИСТЕМА ПОЛНОГО АНАЛИЗА + API")
    print("=" * 80)
    print("🚀 Используем ВСЕ 63 поля из grab_stats и gojek_stats!")
    print("🌐 + OpenAI API + Weather API + Calendar API")
    print("🔍 + ДЕТЕКТИВНЫЙ АНАЛИЗ ПРИЧИН падений/роста ⭐ НОВИНКА!")
    print("=" * 80)
    print()
    
    print("📋 ДОСТУПНЫЕ КОМАНДЫ:")
    print()
    print("1️⃣ 📋 Список ресторанов:")
    print("   python main.py list")
    print()
    print("2️⃣ 🔬 Полный анализ ресторана (ВСЕ 63 параметра + API):")
    print("   python main.py analyze \"Ika Canggu\"")
    print("   python main.py analyze \"Ika Canggu\" --start 2025-04-01 --end 2025-06-22")
    print()
    print("3️⃣ 🌍 Анализ всего рынка:")
    print("   python main.py market")
    print("   python main.py market --start 2025-04-01 --end 2025-06-22")
    print()
    print("4️⃣ 🌐 Проверка статуса API:")
    print("   python main.py check-apis")
    print()
    
    print("🔍 НОВЫЕ ДЕТЕКТИВНЫЕ ВОЗМОЖНОСТИ:")
    print("=" * 50)
    print("📊 Анализ причин падений и роста продаж")
    print("🎯 Выявление конкретных факторов влияния")
    print("💡 Эмпирические правила бизнеса")
    print("📈 Корреляционный анализ факторов")
    print("🚨 Практические рекомендации")
    print()
    
    print("💡 ПРИМЕРЫ ИСПОЛЬЗОВАНИЯ НА REPLIT:")
    print("=" * 50)
    
    while True:
        print("\n📋 Выберите действие:")
        print("1. 📋 Показать список ресторанов")
        print("2. 🔬 Анализ ресторана 'Ika Canggu' (демо)")
        print("3. 🌍 Анализ рынка (демо)")
        print("4. 🌐 Проверить API")
        print("5. ❌ Выход")
        
        choice = input("\n👉 Введите номер (1-5): ").strip()
        
        if choice == '1':
            os.system("python main.py list")
        elif choice == '2':
            print("\n🔬 Запускаем полный анализ ресторана 'Ika Canggu'...")
            os.system("python main.py analyze 'Ika Canggu' --start 2025-06-01 --end 2025-06-05")
        elif choice == '3':
            print("\n🌍 Запускаем анализ всего рынка...")
            os.system("python main.py market --start 2025-06-01 --end 2025-06-05")
        elif choice == '4':
            os.system("python main.py check-apis")
        elif choice == '5':
            print("\n👋 До свидания!")
            break
        else:
            print("❌ Неверный выбор. Попробуйте снова.")

if __name__ == "__main__":
    main()