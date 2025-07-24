#!/usr/bin/env python3
"""
📍 GOOGLE MAPS ЛОКАЦИОННЫЙ АНАЛИЗАТОР
Анализ локации ресторанов с помощью Google Maps API
"""

import requests
import json
import sqlite3
import pandas as pd
from datetime import datetime
from typing import Dict, List, Optional, Tuple
import os
from dotenv import load_dotenv

class GoogleLocationAnalyzer:
    """Анализатор локации ресторанов через Google Maps API"""
    
    def __init__(self, api_key: str):
        self.api_key = api_key
        
    def geocode_address(self, address: str) -> Dict:
        """Преобразует адрес в координаты"""
        
        url = "https://maps.googleapis.com/maps/api/geocode/json"
        
        params = {
            'address': address,
            'key': self.api_key,
            'language': 'ru'
        }
        
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            
            if data['status'] == 'OK' and data['results']:
                result = data['results'][0]
                location = result['geometry']['location']
                
                return {
                    'latitude': location['lat'],
                    'longitude': location['lng'],
                    'formatted_address': result['formatted_address'],
                    'place_id': result['place_id'],
                    'address_components': result.get('address_components', [])
                }
            else:
                print(f"❌ Не удалось геокодировать адрес: {address}")
                return {}
                
        except requests.RequestException as e:
            print(f"❌ Ошибка геокодирования: {e}")
            return {}
    
    def reverse_geocode(self, latitude: float, longitude: float) -> Dict:
        """Преобразует координаты в адрес"""
        
        url = "https://maps.googleapis.com/maps/api/geocode/json"
        
        params = {
            'latlng': f"{latitude},{longitude}",
            'key': self.api_key,
            'language': 'ru'
        }
        
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            
            if data['status'] == 'OK' and data['results']:
                result = data['results'][0]
                
                return {
                    'formatted_address': result['formatted_address'],
                    'place_id': result['place_id'],
                    'address_components': result.get('address_components', [])
                }
            else:
                print(f"❌ Не удалось найти адрес для координат: {latitude}, {longitude}")
                return {}
                
        except requests.RequestException as e:
            print(f"❌ Ошибка обратного геокодирования: {e}")
            return {}
    
    def find_nearby_restaurants(self, latitude: float, longitude: float, radius: int = 1000) -> List[Dict]:
        """Находит рестораны рядом с указанными координатами"""
        
        url = "https://maps.googleapis.com/maps/api/place/nearbysearch/json"
        
        params = {
            'location': f"{latitude},{longitude}",
            'radius': radius,
            'type': 'restaurant',
            'key': self.api_key,
            'language': 'ru'
        }
        
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            
            if data['status'] == 'OK':
                restaurants = []
                
                for place in data.get('results', []):
                    restaurant_info = {
                        'name': place.get('name', 'Неизвестно'),
                        'place_id': place.get('place_id'),
                        'rating': place.get('rating', 0),
                        'user_ratings_total': place.get('user_ratings_total', 0),
                        'price_level': place.get('price_level', 0),
                        'latitude': place['geometry']['location']['lat'],
                        'longitude': place['geometry']['location']['lng'],
                        'vicinity': place.get('vicinity', ''),
                        'types': place.get('types', [])
                    }
                    restaurants.append(restaurant_info)
                
                return restaurants
            else:
                print(f"❌ Ошибка поиска ресторанов: {data.get('status')}")
                return []
                
        except requests.RequestException as e:
            print(f"❌ Ошибка поиска ресторанов: {e}")
            return []
    
    def find_tourist_attractions(self, latitude: float, longitude: float, radius: int = 2000) -> List[Dict]:
        """Находит туристические достопримечательности рядом"""
        
        url = "https://maps.googleapis.com/maps/api/place/nearbysearch/json"
        
        params = {
            'location': f"{latitude},{longitude}",
            'radius': radius,
            'type': 'tourist_attraction',
            'key': self.api_key,
            'language': 'ru'
        }
        
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            
            if data['status'] == 'OK':
                attractions = []
                
                for place in data.get('results', []):
                    attraction_info = {
                        'name': place.get('name', 'Неизвестно'),
                        'place_id': place.get('place_id'),
                        'rating': place.get('rating', 0),
                        'user_ratings_total': place.get('user_ratings_total', 0),
                        'latitude': place['geometry']['location']['lat'],
                        'longitude': place['geometry']['location']['lng'],
                        'vicinity': place.get('vicinity', ''),
                        'types': place.get('types', [])
                    }
                    attractions.append(attraction_info)
                
                return attractions
            else:
                print(f"❌ Ошибка поиска достопримечательностей: {data.get('status')}")
                return []
                
        except requests.RequestException as e:
            print(f"❌ Ошибка поиска достопримечательностей: {e}")
            return []
    
    def analyze_location_quality(self, latitude: float, longitude: float) -> Dict:
        """Анализирует качество локации для ресторана"""
        
        print(f"📍 АНАЛИЗ ЛОКАЦИИ: {latitude}, {longitude}")
        print("=" * 60)
        
        # Получаем адрес
        address_info = self.reverse_geocode(latitude, longitude)
        
        # Находим конкурентов
        competitors = self.find_nearby_restaurants(latitude, longitude, 1000)
        
        # Находим достопримечательности
        attractions = self.find_tourist_attractions(latitude, longitude, 2000)
        
        # Анализируем конкуренцию
        competition_analysis = self._analyze_competition(competitors)
        
        # Анализируем туристический потенциал
        tourist_analysis = self._analyze_tourist_potential(attractions)
        
        # Рассчитываем общий индекс локации
        location_score = self._calculate_location_score(
            competition_analysis, tourist_analysis
        )
        
        analysis_result = {
            'coordinates': {'latitude': latitude, 'longitude': longitude},
            'address': address_info.get('formatted_address', 'Неизвестно'),
            'competition_analysis': competition_analysis,
            'tourist_analysis': tourist_analysis,
            'location_score': location_score,
            'recommendations': self._generate_recommendations(
                competition_analysis, tourist_analysis, location_score
            ),
            'analysis_date': datetime.now().isoformat()
        }
        
        return analysis_result
    
    def _analyze_competition(self, competitors: List[Dict]) -> Dict:
        """Анализирует конкурентную среду"""
        
        if not competitors:
            return {
                'total_competitors': 0,
                'avg_rating': 0,
                'avg_price_level': 0,
                'competition_density': 'низкая',
                'top_competitor': None
            }
        
        # Основная статистика
        total_competitors = len(competitors)
        ratings = [c['rating'] for c in competitors if c['rating'] > 0]
        price_levels = [c['price_level'] for c in competitors if c['price_level'] > 0]
        
        avg_rating = sum(ratings) / len(ratings) if ratings else 0
        avg_price_level = sum(price_levels) / len(price_levels) if price_levels else 0
        
        # Определяем плотность конкуренции
        if total_competitors >= 20:
            density = 'очень высокая'
        elif total_competitors >= 10:
            density = 'высокая'
        elif total_competitors >= 5:
            density = 'средняя'
        else:
            density = 'низкая'
        
        # Находим топ конкурента
        top_competitor = max(competitors, key=lambda x: x['rating'] * x['user_ratings_total']) if competitors else None
        
        return {
            'total_competitors': total_competitors,
            'avg_rating': round(avg_rating, 2),
            'avg_price_level': round(avg_price_level, 1),
            'competition_density': density,
            'top_competitor': {
                'name': top_competitor['name'],
                'rating': top_competitor['rating'],
                'reviews': top_competitor['user_ratings_total']
            } if top_competitor else None
        }
    
    def _analyze_tourist_potential(self, attractions: List[Dict]) -> Dict:
        """Анализирует туристический потенциал локации"""
        
        if not attractions:
            return {
                'total_attractions': 0,
                'avg_rating': 0,
                'tourist_potential': 'низкий',
                'top_attraction': None
            }
        
        total_attractions = len(attractions)
        ratings = [a['rating'] for a in attractions if a['rating'] > 0]
        avg_rating = sum(ratings) / len(ratings) if ratings else 0
        
        # Определяем туристический потенциал
        if total_attractions >= 10 and avg_rating >= 4.0:
            potential = 'очень высокий'
        elif total_attractions >= 5 and avg_rating >= 3.5:
            potential = 'высокий'
        elif total_attractions >= 2:
            potential = 'средний'
        else:
            potential = 'низкий'
        
        # Находим топ достопримечательность
        top_attraction = max(attractions, key=lambda x: x['rating'] * x['user_ratings_total']) if attractions else None
        
        return {
            'total_attractions': total_attractions,
            'avg_rating': round(avg_rating, 2),
            'tourist_potential': potential,
            'top_attraction': {
                'name': top_attraction['name'],
                'rating': top_attraction['rating'],
                'reviews': top_attraction['user_ratings_total']
            } if top_attraction else None
        }
    
    def _calculate_location_score(self, competition_analysis: Dict, tourist_analysis: Dict) -> Dict:
        """Рассчитывает общий индекс качества локации (0-100)"""
        
        score = 50  # Базовый балл
        
        # Корректировка по конкуренции
        density = competition_analysis['competition_density']
        if density == 'низкая':
            score += 20  # Мало конкурентов - хорошо
        elif density == 'средняя':
            score += 10  # Умеренная конкуренция
        elif density == 'высокая':
            score -= 5   # Много конкурентов
        else:  # очень высокая
            score -= 15  # Слишком много конкурентов
        
        # Корректировка по туристическому потенциалу
        potential = tourist_analysis['tourist_potential']
        if potential == 'очень высокий':
            score += 25
        elif potential == 'высокий':
            score += 15
        elif potential == 'средний':
            score += 5
        # низкий - без изменений
        
        # Корректировка по качеству конкурентов
        avg_competitor_rating = competition_analysis['avg_rating']
        if avg_competitor_rating >= 4.5:
            score += 5  # Высокие стандарты в районе
        elif avg_competitor_rating <= 3.0:
            score -= 10  # Низкое качество в районе
        
        # Ограничиваем диапазон 0-100
        score = max(0, min(100, score))
        
        # Определяем категорию
        if score >= 80:
            category = 'отличная'
        elif score >= 65:
            category = 'хорошая'
        elif score >= 50:
            category = 'средняя'
        elif score >= 35:
            category = 'ниже среднего'
        else:
            category = 'плохая'
        
        return {
            'score': score,
            'category': category,
            'max_score': 100
        }
    
    def _generate_recommendations(self, competition_analysis: Dict, tourist_analysis: Dict, location_score: Dict) -> List[str]:
        """Генерирует рекомендации по локации"""
        
        recommendations = []
        
        # Рекомендации по конкуренции
        density = competition_analysis['competition_density']
        if density == 'низкая':
            recommendations.append("✅ Отличная возможность - мало конкурентов в районе")
        elif density == 'очень высокая':
            recommendations.append("⚠️ Высокая конкуренция - нужна четкая дифференциация")
        
        # Рекомендации по туристическому потенциалу
        potential = tourist_analysis['tourist_potential']
        if potential in ['высокий', 'очень высокий']:
            recommendations.append("🏖️ Высокий туристический поток - ориентируйтесь на туристов")
        elif potential == 'низкий':
            recommendations.append("🏠 Фокус на местных жителей - туристов мало")
        
        # Рекомендации по общему баллу
        score = location_score['score']
        if score >= 80:
            recommendations.append("🎉 Превосходная локация для ресторана!")
        elif score >= 65:
            recommendations.append("👍 Хорошая локация с потенциалом роста")
        elif score < 50:
            recommendations.append("🤔 Рассмотрите альтернативные локации")
        
        # Рекомендации по ценовой категории
        avg_price = competition_analysis['avg_price_level']
        if avg_price >= 3:
            recommendations.append("💰 Район премиум-сегмента - высокие цены оправданы")
        elif avg_price <= 1:
            recommendations.append("💵 Бюджетный район - конкурируйте ценой")
        
        return recommendations

def analyze_restaurant_locations():
    """Анализирует локации ресторанов из базы данных"""
    
    print("📍 АНАЛИЗ ЛОКАЦИЙ РЕСТОРАНОВ")
    print("=" * 50)
    
    # Загружаем API ключ
    load_dotenv()
    api_key = os.getenv('GOOGLE_MAPS_API_KEY')
    
    if not api_key:
        print("❌ Google Maps API ключ не найден в .env")
        return False
    
    # Создаем анализатор
    analyzer = GoogleLocationAnalyzer(api_key)
    
    # Получаем уникальные рестораны из базы
    conn = sqlite3.connect('database.sqlite')
    
    query = """
    SELECT DISTINCT restaurant_name 
    FROM grab_stats 
    WHERE restaurant_name IS NOT NULL 
    LIMIT 5
    """
    
    restaurants_df = pd.read_sql_query(query, conn)
    conn.close()
    
    if restaurants_df.empty:
        print("❌ Рестораны в базе данных не найдены")
        return False
    
    # Примерные координаты для ресторанов Бали
    bali_locations = [
        {'name': 'Seminyak Restaurant', 'latitude': -8.6905, 'longitude': 115.1729},
        {'name': 'Ubud Restaurant', 'latitude': -8.5069, 'longitude': 115.2625},
        {'name': 'Canggu Restaurant', 'latitude': -8.6482, 'longitude': 115.1342},
        {'name': 'Sanur Restaurant', 'latitude': -8.6878, 'longitude': 115.2613},
        {'name': 'Denpasar Restaurant', 'latitude': -8.6500, 'longitude': 115.2167}
    ]
    
    location_analyses = []
    
    for location in bali_locations:
        print(f"\n🔍 Анализируем: {location['name']}")
        
        analysis = analyzer.analyze_location_quality(
            location['latitude'], 
            location['longitude']
        )
        
        analysis['restaurant_name'] = location['name']
        location_analyses.append(analysis)
        
        # Показываем результаты
        print(f"📊 Результаты анализа {location['name']}:")
        print(f"   Индекс локации: {analysis['location_score']['score']}/100 ({analysis['location_score']['category']})")
        print(f"   Конкурентов: {analysis['competition_analysis']['total_competitors']}")
        print(f"   Достопримечательностей: {analysis['tourist_analysis']['total_attractions']}")
        print(f"   Туристический потенциал: {analysis['tourist_analysis']['tourist_potential']}")
        
        print("   Рекомендации:")
        for rec in analysis['recommendations']:
            print(f"     {rec}")
    
    # Сохраняем результаты
    with open('location_analysis_results.json', 'w', encoding='utf-8') as f:
        json.dump(location_analyses, f, indent=2, ensure_ascii=False)
    
    print(f"\n✅ Анализ локаций завершен!")
    print(f"📄 Результаты сохранены в location_analysis_results.json")
    
    # Находим лучшую локацию
    best_location = max(location_analyses, key=lambda x: x['location_score']['score'])
    
    print(f"\n🏆 ЛУЧШАЯ ЛОКАЦИЯ: {best_location['restaurant_name']}")
    print(f"   Индекс: {best_location['location_score']['score']}/100")
    print(f"   Категория: {best_location['location_score']['category']}")
    
    return True

def test_google_geocoding():
    """Тестирует Google Geocoding API"""
    
    print("🧪 ТЕСТИРОВАНИЕ GOOGLE GEOCODING API")
    print("=" * 45)
    
    load_dotenv()
    api_key = os.getenv('GOOGLE_MAPS_API_KEY')
    
    if not api_key:
        print("❌ Google Maps API ключ не найден")
        return False
    
    analyzer = GoogleLocationAnalyzer(api_key)
    
    # Тест геокодирования адреса
    print("📍 Тестируем геокодирование адреса...")
    test_address = "Seminyak, Bali, Indonesia"
    
    geocode_result = analyzer.geocode_address(test_address)
    
    if geocode_result:
        print(f"✅ Адрес геокодирован успешно:")
        print(f"   Адрес: {geocode_result['formatted_address']}")
        print(f"   Координаты: {geocode_result['latitude']}, {geocode_result['longitude']}")
        
        # Тест обратного геокодирования
        print("\n🔄 Тестируем обратное геокодирование...")
        reverse_result = analyzer.reverse_geocode(
            geocode_result['latitude'], 
            geocode_result['longitude']
        )
        
        if reverse_result:
            print(f"✅ Обратное геокодирование успешно:")
            print(f"   Адрес: {reverse_result['formatted_address']}")
        else:
            print("❌ Обратное геокодирование не удалось")
            return False
        
        # Тест поиска ресторанов
        print("\n🍽️ Тестируем поиск ресторанов рядом...")
        restaurants = analyzer.find_nearby_restaurants(
            geocode_result['latitude'],
            geocode_result['longitude']
        )
        
        if restaurants:
            print(f"✅ Найдено {len(restaurants)} ресторанов")
            print("   Топ-3 ресторана:")
            for i, restaurant in enumerate(restaurants[:3], 1):
                print(f"   {i}. {restaurant['name']} (рейтинг: {restaurant['rating']})")
        else:
            print("❌ Рестораны не найдены")
        
        print("\n🎉 Все тесты прошли успешно!")
        return True
    else:
        print("❌ Геокодирование не удалось")
        return False

if __name__ == "__main__":
    # Тестируем API
    if test_google_geocoding():
        # Анализируем локации
        analyze_restaurant_locations()
    else:
        print("❌ Тестирование не прошло")