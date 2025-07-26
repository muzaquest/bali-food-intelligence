#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
АВТОМАТИЧЕСКИЙ ГЕОЛОКАТОР РЕСТОРАНОВ
===================================

Автоматически находит GPS координаты всех ресторанов из базы
используя Google Maps API и сохраняет в bali_restaurant_locations.json
"""

import sqlite3
import requests
import json
import os
import time
from datetime import datetime

class AutoGeolocator:
    def __init__(self):
        self.db_path = 'database.sqlite'
        self.google_api_key = os.getenv('GOOGLE_MAPS_API_KEY', 'YOUR_API_KEY_HERE')
        self.locations_file = 'data/bali_restaurant_locations.json'
        self.backup_file = 'data/bali_restaurant_locations_backup.json'
        
        # Загружаем существующие координаты
        self.existing_locations = self.load_existing_locations()
        
    def load_existing_locations(self):
        """Загружает существующие координаты"""
        try:
            with open(self.locations_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            return {r['name']: r for r in data['restaurants']}
        except:
            return {}
    
    def get_all_restaurants(self):
        """Получает список всех ресторанов из базы"""
        conn = sqlite3.connect(self.db_path)
        query = "SELECT DISTINCT name FROM restaurants ORDER BY name"
        restaurants = [row[0] for row in conn.execute(query).fetchall()]
        conn.close()
        return restaurants
    
    def search_restaurant_location(self, restaurant_name):
        """Ищет координаты ресторана через Google Maps API"""
        
        # Если уже есть координаты, пропускаем
        if restaurant_name in self.existing_locations:
            return self.existing_locations[restaurant_name]
        
        # Формируем поисковый запрос
        search_query = f"{restaurant_name} restaurant Bali Indonesia"
        
        try:
            url = "https://maps.googleapis.com/maps/api/geocoding/json"
            params = {
                'address': search_query,
                'key': self.google_api_key,
                'region': 'id',  # Indonesia
                'components': 'country:ID'  # Only Indonesia
            }
            
            response = requests.get(url, params=params, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                
                if data['status'] == 'OK' and len(data['results']) > 0:
                    result = data['results'][0]
                    location = result['geometry']['location']
                    
                    # Извлекаем информацию о местоположении
                    address_components = result.get('address_components', [])
                    formatted_address = result.get('formatted_address', '')
                    
                    # Определяем зону и район
                    area = self.extract_area(address_components, formatted_address)
                    zone = self.determine_zone(location['lat'], location['lng'], area)
                    
                    restaurant_data = {
                        'name': restaurant_name,
                        'latitude': location['lat'],
                        'longitude': location['lng'],
                        'location': area,
                        'area': area,
                        'zone': zone,
                        'formatted_address': formatted_address,
                        'found_automatically': True,
                        'search_query': search_query,
                        'timestamp': datetime.now().isoformat()
                    }
                    
                    return restaurant_data
                    
                else:
                    print(f"   ❌ Не найден: {restaurant_name} (статус: {data.get('status', 'UNKNOWN')})")
                    return None
            else:
                print(f"   ⚠️ Ошибка API для {restaurant_name}: {response.status_code}")
                return None
                
        except Exception as e:
            print(f"   ⚠️ Ошибка поиска {restaurant_name}: {e}")
            return None
    
    def extract_area(self, address_components, formatted_address):
        """Извлекает район из компонентов адреса"""
        # Приоритет: sublocality > locality > administrative_area_level_2
        for component in address_components:
            types = component.get('types', [])
            if 'sublocality' in types or 'sublocality_level_1' in types:
                return component['long_name']
        
        for component in address_components:
            types = component.get('types', [])
            if 'locality' in types:
                return component['long_name']
        
        # Если ничего не найдено, пытаемся извлечь из formatted_address
        if 'Canggu' in formatted_address:
            return 'Canggu'
        elif 'Seminyak' in formatted_address:
            return 'Seminyak'
        elif 'Ubud' in formatted_address:
            return 'Ubud'
        elif 'Uluwatu' in formatted_address:
            return 'Uluwatu'
        elif 'Kerobokan' in formatted_address:
            return 'Kerobokan'
        elif 'Jimbaran' in formatted_address:
            return 'Jimbaran'
        elif 'Sanur' in formatted_address:
            return 'Sanur'
        elif 'Denpasar' in formatted_address:
            return 'Denpasar'
        else:
            return 'Unknown'
    
    def determine_zone(self, lat, lng, area):
        """Определяет зону на основе координат и района"""
        # Приблизительные границы зон Бали
        if area.lower() in ['canggu', 'berawa', 'echo beach']:
            return 'Beach'
        elif area.lower() in ['uluwatu', 'pecatu', 'bingin']:
            return 'Cliff'
        elif area.lower() in ['ubud', 'tegallalang', 'payangan']:
            return 'Mountain'
        elif area.lower() in ['seminyak', 'kerobokan', 'legian', 'kuta']:
            return 'Central'
        elif area.lower() in ['jimbaran', 'nusa dua']:
            return 'South'
        elif area.lower() in ['sanur', 'denpasar']:
            return 'East'
        else:
            # Определяем по координатам
            if lat < -8.7:  # Южная часть
                if lng < 115.1:
                    return 'Cliff'  # Uluwatu area
                else:
                    return 'South'  # Jimbaran area
            elif lat > -8.5:  # Северная часть
                return 'Mountain'  # Ubud area
            elif lng < 115.15:  # Западная часть
                return 'Beach'  # Canggu area
            else:
                return 'Central'  # Seminyak/Kerobokan area
    
    def run_geolocation(self):
        """Запускает автоматическое определение координат"""
        print("🌍 АВТОМАТИЧЕСКИЙ ПОИСК GPS КООРДИНАТ")
        print("=" * 45)
        
        restaurants = self.get_all_restaurants()
        print(f"📊 Всего ресторанов для обработки: {len(restaurants)}")
        print(f"📍 Уже найдено координат: {len(self.existing_locations)}")
        
        new_restaurants = [r for r in restaurants if r not in self.existing_locations]
        print(f"🔍 Нужно найти: {len(new_restaurants)} ресторанов")
        
        if not new_restaurants:
            print("✅ Все рестораны уже имеют координаты!")
            return self.existing_locations
        
        # Создаем резервную копию
        if os.path.exists(self.locations_file):
            with open(self.locations_file, 'r', encoding='utf-8') as f:
                backup_data = f.read()
            with open(self.backup_file, 'w', encoding='utf-8') as f:
                f.write(backup_data)
            print(f"💾 Создана резервная копия: {self.backup_file}")
        
        print(f"\n🔍 Поиск координат через Google Maps API...")
        
        all_locations = dict(self.existing_locations)
        found_count = 0
        failed_count = 0
        
        for i, restaurant in enumerate(new_restaurants, 1):
            print(f"\n{i:2d}/{len(new_restaurants)} Поиск: {restaurant}")
            
            location_data = self.search_restaurant_location(restaurant)
            
            if location_data:
                all_locations[restaurant] = location_data
                found_count += 1
                print(f"   ✅ Найден: {location_data['area']}, {location_data['zone']} зона")
                print(f"      📍 {location_data['latitude']:.4f}, {location_data['longitude']:.4f}")
            else:
                failed_count += 1
            
            # Пауза между запросами
            if i < len(new_restaurants):
                time.sleep(2)  # 2 секунды между запросами
        
        # Сохраняем результаты
        self.save_locations(all_locations)
        
        print(f"\n📊 РЕЗУЛЬТАТЫ ПОИСКА:")
        print(f"   ✅ Найдено: {found_count}")
        print(f"   ❌ Не найдено: {failed_count}")
        print(f"   📍 Всего координат: {len(all_locations)}")
        
        return all_locations
    
    def save_locations(self, locations):
        """Сохраняет координаты в файл"""
        try:
            os.makedirs('data', exist_ok=True)
            
            # Конвертируем в нужный формат
            restaurants_list = []
            for name, data in locations.items():
                restaurants_list.append(data)
            
            # Сортируем по имени
            restaurants_list.sort(key=lambda x: x['name'])
            
            output_data = {
                'restaurants': restaurants_list,
                'total_count': len(restaurants_list),
                'last_updated': datetime.now().isoformat(),
                'auto_generated': True
            }
            
            with open(self.locations_file, 'w', encoding='utf-8') as f:
                json.dump(output_data, f, ensure_ascii=False, indent=2)
            
            print(f"\n💾 Координаты сохранены: {self.locations_file}")
            
        except Exception as e:
            print(f"⚠️ Ошибка сохранения: {e}")
    
    def show_statistics(self):
        """Показывает статистику по зонам"""
        try:
            with open(self.locations_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            restaurants = data['restaurants']
            
            print(f"\n📊 СТАТИСТИКА ПО ЗОНАМ:")
            print("=" * 25)
            
            zones = {}
            areas = {}
            
            for restaurant in restaurants:
                zone = restaurant.get('zone', 'Unknown')
                area = restaurant.get('area', 'Unknown')
                
                if zone not in zones:
                    zones[zone] = 0
                zones[zone] += 1
                
                if area not in areas:
                    areas[area] = 0
                areas[area] += 1
            
            print("🌍 По зонам:")
            for zone, count in sorted(zones.items()):
                print(f"   📍 {zone}: {count} ресторанов")
            
            print(f"\n🏘️ По районам:")
            for area, count in sorted(areas.items()):
                print(f"   📍 {area}: {count} ресторанов")
            
        except Exception as e:
            print(f"⚠️ Ошибка чтения статистики: {e}")

def main():
    """Запускает автоматический поиск координат"""
    geolocator = AutoGeolocator()
    
    print("🗺️ АВТОМАТИЧЕСКИЙ ГЕОЛОКАТОР РЕСТОРАНОВ")
    print("Поиск GPS координат всех ресторанов через Google Maps API")
    print("=" * 65)
    
    # Запускаем поиск
    locations = geolocator.run_geolocation()
    
    # Показываем статистику
    geolocator.show_statistics()
    
    print(f"\n🎉 ГЕОЛОКАЦИЯ ЗАВЕРШЕНА!")
    print(f"📍 Теперь можно анализировать погоду для всех ресторанов!")

if __name__ == "__main__":
    main()