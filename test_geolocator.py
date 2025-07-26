
# Тестовая версия без API - создает координаты на основе названий
import sqlite3
import json
import os
from datetime import datetime

def create_test_locations():
    """Создает тестовые координаты на основе названий ресторанов"""
    
    # Получаем все рестораны
    conn = sqlite3.connect('database.sqlite')
    restaurants = [row[0] for row in conn.execute('SELECT DISTINCT name FROM restaurants ORDER BY name').fetchall()]
    conn.close()
    
    print(f'📊 Создаем тестовые координаты для {len(restaurants)} ресторанов...')
    
    # Базовые координаты зон Бали
    zone_coords = {
        'Canggu': {'lat': -8.671, 'lng': 115.213, 'zone': 'Beach'},
        'Seminyak': {'lat': -8.618, 'lng': 115.169, 'zone': 'Central'},
        'Kerobokan': {'lat': -8.618, 'lng': 115.169, 'zone': 'Central'},
        'Ubud': {'lat': -8.507, 'lng': 115.262, 'zone': 'Mountain'},
        'Uluwatu': {'lat': -8.829, 'lng': 115.084, 'zone': 'Cliff'},
        'Jimbaran': {'lat': -8.789, 'lng': 115.166, 'zone': 'South'},
        'Sanur': {'lat': -8.670, 'lng': 115.262, 'zone': 'East'},
        'Berawa': {'lat': -8.671, 'lng': 115.213, 'zone': 'Beach'}
    }
    
    locations = []
    
    for i, restaurant in enumerate(restaurants):
        # Определяем зону на основе названия
        area = 'Canggu'  # По умолчанию
        
        if 'Ubud' in restaurant:
            area = 'Ubud'
        elif 'Uluwatu' in restaurant:
            area = 'Uluwatu'
        elif 'Jimbaran' in restaurant:
            area = 'Jimbaran'
        elif 'Sanur' in restaurant:
            area = 'Sanur'
        elif 'Berawa' in restaurant:
            area = 'Berawa'
        elif 'Seminyak' in restaurant:
            area = 'Seminyak'
        elif 'Kero' in restaurant:
            area = 'Kerobokan'
        elif 'Canggu' in restaurant:
            area = 'Canggu'
        else:
            # Распределяем по зонам равномерно
            areas = ['Canggu', 'Seminyak', 'Kerobokan', 'Ubud', 'Uluwatu', 'Jimbaran']
            area = areas[i % len(areas)]
        
        base_coords = zone_coords[area]
        
        # Добавляем небольшое случайное смещение
        offset = (i % 10) * 0.001
        
        location_data = {
            'name': restaurant,
            'latitude': base_coords['lat'] + offset,
            'longitude': base_coords['lng'] + offset,
            'location': area,
            'area': area,
            'zone': base_coords['zone'],
            'formatted_address': f'{restaurant}, {area}, Bali, Indonesia',
            'found_automatically': False,
            'test_generated': True,
            'timestamp': datetime.now().isoformat()
        }
        
        locations.append(location_data)
    
    # Сохраняем
    os.makedirs('data', exist_ok=True)
    
    output_data = {
        'restaurants': locations,
        'total_count': len(locations),
        'last_updated': datetime.now().isoformat(),
        'test_generated': True,
        'note': 'Тестовые координаты - замените на реальные через Google Maps API'
    }
    
    with open('data/bali_restaurant_locations.json', 'w', encoding='utf-8') as f:
        json.dump(output_data, f, ensure_ascii=False, indent=2)
    
    print(f'💾 Тестовые координаты сохранены: data/bali_restaurant_locations.json')
    
    # Статистика
    zones = {}
    for loc in locations:
        zone = loc['zone']
        if zone not in zones:
            zones[zone] = 0
        zones[zone] += 1
    
    print(f'')
    print(f'📊 РАСПРЕДЕЛЕНИЕ ПО ЗОНАМ:')
    for zone, count in zones.items():
        print(f'   📍 {zone}: {count} ресторанов')
    
    return locations

if __name__ == "__main__":
    create_test_locations()
