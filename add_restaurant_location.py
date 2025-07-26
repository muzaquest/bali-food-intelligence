#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
🗺️ ДОБАВЛЕНИЕ КООРДИНАТ НОВОГО РЕСТОРАНА
========================================
Скрипт для добавления координат новых ресторанов в систему MUZAQUEST
"""

import json
import sys
import os
from datetime import datetime

def add_restaurant_location(name, lat, lon, location, area, zone):
    """Добавляет координаты нового ресторана"""
    
    locations_file = 'data/bali_restaurant_locations.json'
    
    # Проверяем что файл существует
    if not os.path.exists(locations_file):
        print(f"❌ Файл {locations_file} не найден!")
        print("   Убедитесь что запускаете скрипт из корневой папки проекта")
        return False
    
    try:
        # Загружаем существующие данные
        with open(locations_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Проверяем что ресторан еще не добавлен
        for restaurant in data['restaurants']:
            if restaurant['name'].lower() == name.lower():
                print(f"❌ Ресторан '{name}' уже существует!")
                print(f"   Координаты: {restaurant['latitude']}, {restaurant['longitude']}")
                print(f"   Локация: {restaurant['location']}")
                return False
        
        # Валидация координат
        if not (-90 <= lat <= 90):
            print(f"❌ Некорректная широта: {lat} (должна быть от -90 до 90)")
            return False
            
        if not (-180 <= lon <= 180):
            print(f"❌ Некорректная долгота: {lon} (должна быть от -180 до 180)")
            return False
        
        # Проверяем что координаты в районе Бали
        if not (-9.0 <= lat <= -8.0 and 114.5 <= lon <= 116.0):
            print(f"⚠️ Предупреждение: Координаты {lat}, {lon} могут быть вне Бали")
            response = input("Продолжить? (y/n): ")
            if response.lower() != 'y':
                print("❌ Операция отменена")
                return False
        
        # Добавляем новый ресторан
        new_restaurant = {
            "name": name,
            "latitude": lat,
            "longitude": lon,
            "location": location,
            "area": area,
            "zone": zone
        }
        
        # Обновляем данные
        data['restaurants'].append(new_restaurant)
        data['total_restaurants'] = len(data['restaurants'])
        data['last_updated'] = datetime.now().strftime("%Y-%m-%d")
        
        # Сортируем рестораны по названию
        data['restaurants'].sort(key=lambda x: x['name'])
        
        # Создаем backup
        backup_file = f"{locations_file}.backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        with open(backup_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        
        # Сохраняем обновленный файл
        with open(locations_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        
        print(f"✅ Ресторан '{name}' добавлен успешно!")
        print(f"📍 Координаты: {lat}, {lon}")
        print(f"📍 Локация: {location}, {area}, {zone}")
        print(f"📊 Всего ресторанов: {data['total_restaurants']}")
        print(f"💾 Backup создан: {backup_file}")
        
        return True
        
    except json.JSONDecodeError:
        print(f"❌ Ошибка: Файл {locations_file} поврежден (некорректный JSON)")
        return False
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        return False

def list_restaurants():
    """Показывает список существующих ресторанов"""
    
    locations_file = 'data/bali_restaurant_locations.json'
    
    if not os.path.exists(locations_file):
        print(f"❌ Файл {locations_file} не найден!")
        return
    
    try:
        with open(locations_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        print(f"📊 СПИСОК РЕСТОРАНОВ ({data['total_restaurants']} всего)")
        print("=" * 60)
        
        for i, restaurant in enumerate(data['restaurants'], 1):
            print(f"{i:2d}. 🍽️ {restaurant['name']}")
            print(f"    📍 {restaurant['latitude']}, {restaurant['longitude']}")
            print(f"    🏝️ {restaurant['location']}, {restaurant['area']}, {restaurant['zone']}")
            print()
            
    except Exception as e:
        print(f"❌ Ошибка: {e}")

def show_help():
    """Показывает справку по использованию"""
    
    print("""
🗺️ ДОБАВЛЕНИЕ КООРДИНАТ РЕСТОРАНОВ MUZAQUEST
===========================================

📋 КОМАНДЫ:

1️⃣ Добавить новый ресторан:
   python add_restaurant_location.py add "Название" lat lon "Локация" "Район" "Зона"

2️⃣ Показать список ресторанов:
   python add_restaurant_location.py list

3️⃣ Показать справку:
   python add_restaurant_location.py help

📍 ПРИМЕРЫ:

✅ Добавить ресторан в Canggu:
   python add_restaurant_location.py add "Warung Sunset" -8.6488 115.1373 "Canggu" "Badung" "South"

✅ Добавить ресторан в Ubud:
   python add_restaurant_location.py add "Organic Garden" -8.5069 115.2625 "Ubud" "Gianyar" "Central"

✅ Добавить ресторан в Seminyak:
   python add_restaurant_location.py add "Beach Club" -8.6918 115.1723 "Seminyak" "Badung" "South"

🏝️ ПОПУЛЯРНЫЕ ЛОКАЦИИ БАЛИ:

📍 Южный Бали (South):
   • Canggu, Badung: -8.6488, 115.1373
   • Seminyak, Badung: -8.6918, 115.1723
   • Kuta, Badung: -8.7203, 115.1680
   • Jimbaran, Badung: -8.7892, 115.1663

📍 Центральный Бали (Central):
   • Ubud, Gianyar: -8.5069, 115.2625
   • Denpasar, Denpasar: -8.4095, 115.1889
   • Sanur, Denpasar: -8.6845, 115.2629

📍 Северный Бали (North):
   • Singaraja, Buleleng: -8.1120, 115.0882
   • Lovina, Buleleng: -8.1579, 115.0282

⚠️ ВАЖНО:
• Запускайте скрипт из корневой папки проекта
• Координаты должны быть в пределах Бали
• Система автоматически создает backup файла
""")

def main():
    """Главная функция"""
    
    if len(sys.argv) < 2:
        show_help()
        return
    
    command = sys.argv[1].lower()
    
    if command == 'help':
        show_help()
        
    elif command == 'list':
        list_restaurants()
        
    elif command == 'add':
        if len(sys.argv) != 8:
            print("❌ Ошибка: Неверное количество аргументов для команды 'add'")
            print("\n📋 Правильное использование:")
            print("python add_restaurant_location.py add \"Название\" lat lon \"Локация\" \"Район\" \"Зона\"")
            print("\n📍 Пример:")
            print("python add_restaurant_location.py add \"New Cafe\" -8.4095 115.1889 \"Canggu\" \"Badung\" \"South\"")
            return
        
        try:
            name = sys.argv[2]
            lat = float(sys.argv[3])
            lon = float(sys.argv[4])
            location = sys.argv[5]
            area = sys.argv[6]
            zone = sys.argv[7]
            
            success = add_restaurant_location(name, lat, lon, location, area, zone)
            
            if success:
                print("\n🎉 Координаты добавлены! Теперь:")
                print("1. ✅ Новый ресторан будет виден в веб-интерфейсе")
                print("2. ✅ AI агент сможет анализировать его погоду")
                print("3. ✅ API интеграции будут работать")
                print("4. ✅ Все отчеты включат новый ресторан")
                
        except ValueError:
            print("❌ Ошибка: Координаты должны быть числами")
            print("📍 Пример: -8.4095 115.1889")
            
    else:
        print(f"❌ Неизвестная команда: {command}")
        print("📋 Доступные команды: add, list, help")
        show_help()

if __name__ == "__main__":
    main()