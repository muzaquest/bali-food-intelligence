import json


def get_location_factor(restaurant_location):
    """Получает локационный коэффициент для ресторана"""
    
    try:
        with open('real_coefficients.json', 'r', encoding='utf-8') as f:
            coeffs = json.load(f)
        
        location_factors = coeffs.get('location_factors', {})
        
        # Определяем район по координатам или названию
        if isinstance(restaurant_location, str):
            # Поиск по названию района
            for district, data in location_factors.items():
                if district.lower() in restaurant_location.lower():
                    return data['factor']
        
        # По умолчанию возвращаем средний коэффициент
        return 1.0
        
    except Exception as e:
        print(f"Ошибка получения локационного коэффициента: {e}")
        return 1.0
