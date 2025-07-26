#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
🚀 ПРИМЕР API СЕРВЕРА ДЛЯ MUZAQUEST
===================================
Простой REST API сервер для предоставления доступа к БД через HTTP
"""

from flask import Flask, request, jsonify
import sqlite3
import pandas as pd
from datetime import datetime
import os
from functools import wraps

app = Flask(__name__)

# Конфигурация
DATABASE_PATH = 'database.sqlite'
API_KEY = 'muzaquest_live_api_key_2025_secure'  # В продакшене из environment

def require_api_key(f):
    """Декоратор для проверки API ключа"""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        provided_key = request.headers.get('Authorization')
        
        if provided_key:
            # Извлекаем токен из "Bearer TOKEN"
            try:
                token = provided_key.split(' ')[1]
                if token == API_KEY:
                    return f(*args, **kwargs)
            except IndexError:
                pass
        
        return jsonify({'error': 'Unauthorized', 'message': 'Valid API key required'}), 401
    
    return decorated_function

def get_db_connection():
    """Получение соединения с БД"""
    try:
        conn = sqlite3.connect(DATABASE_PATH)
        conn.row_factory = sqlite3.Row  # Для работы с именованными столбцами
        return conn
    except Exception as e:
        print(f"❌ Ошибка подключения к БД: {e}")
        return None

@app.route('/health', methods=['GET'])
def health_check():
    """Проверка работоспособности API"""
    try:
        conn = get_db_connection()
        if not conn:
            return jsonify({'status': 'error', 'message': 'Database unavailable'}), 500
        
        # Подсчитываем рестораны
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) as count FROM restaurants")
        restaurants_count = cursor.fetchone()['count']
        
        # Получаем последнюю дату обновления
        cursor.execute("""
            SELECT MAX(stat_date) as last_date 
            FROM (
                SELECT stat_date FROM grab_stats 
                UNION ALL 
                SELECT stat_date FROM gojek_stats
            )
        """)
        last_update = cursor.fetchone()['last_date']
        
        conn.close()
        
        return jsonify({
            'status': 'ok',
            'version': '1.0.0',
            'restaurants_count': restaurants_count,
            'last_update': f"{last_update}T00:00:00Z" if last_update else None,
            'timestamp': datetime.now().isoformat()
        })
        
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/restaurants', methods=['GET'])
@require_api_key
def get_restaurants():
    """Получение списка всех ресторанов"""
    try:
        conn = get_db_connection()
        if not conn:
            return jsonify({'error': 'Database unavailable'}), 500
        
        cursor = conn.cursor()
        cursor.execute("""
            SELECT id, name, created_at
            FROM restaurants 
            ORDER BY name
        """)
        
        restaurants = []
        for row in cursor.fetchall():
            restaurants.append({
                'id': row['id'],
                'name': row['name'],
                'created_at': row['created_at']
            })
        
        conn.close()
        
        return jsonify({
            'restaurants': restaurants,
            'count': len(restaurants)
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/restaurant-stats', methods=['GET'])
@require_api_key
def get_restaurant_stats():
    """Получение статистики ресторана"""
    try:
        # Получаем параметры
        restaurant_name = request.args.get('restaurant')
        platform = request.args.get('platform', 'all')
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        
        if not restaurant_name:
            return jsonify({'error': 'Parameter "restaurant" is required'}), 400
        
        conn = get_db_connection()
        if not conn:
            return jsonify({'error': 'Database unavailable'}), 500
        
        # Получаем ID ресторана
        cursor = conn.cursor()
        cursor.execute("SELECT id FROM restaurants WHERE name = ?", (restaurant_name,))
        restaurant_row = cursor.fetchone()
        
        if not restaurant_row:
            return jsonify({'error': f'Restaurant "{restaurant_name}" not found'}), 404
        
        restaurant_id = restaurant_row['id']
        
        # Формируем запрос в зависимости от платформы
        stats = []
        
        if platform in ['grab', 'all']:
            # Запрос данных Grab
            query = """
                SELECT stat_date, sales, orders, rating, 
                       new_customers, repeat_customers, reactivated_customers,
                       ads_spend, ads_sales, ads_orders,
                       impressions, clicks, conversions
                FROM grab_stats 
                WHERE restaurant_id = ?
            """
            params = [restaurant_id]
            
            if start_date:
                query += " AND stat_date >= ?"
                params.append(start_date)
            if end_date:
                query += " AND stat_date <= ?"
                params.append(end_date)
            
            query += " ORDER BY stat_date"
            
            cursor.execute(query, params)
            
            for row in cursor.fetchall():
                stat = {
                    'date': row['stat_date'],
                    'platform': 'grab',
                    'sales': row['sales'],
                    'orders': row['orders'],
                    'rating': row['rating'],
                    'new_customers': row['new_customers'],
                    'repeat_customers': row['repeat_customers'],
                    'reactivated_customers': row['reactivated_customers'],
                    'ads_spend': row['ads_spend'],
                    'ads_sales': row['ads_sales'],
                    'ads_orders': row['ads_orders'],
                    'impressions': row['impressions'],
                    'clicks': row['clicks'],
                    'conversions': row['conversions']
                }
                stats.append(stat)
        
        if platform in ['gojek', 'all']:
            # Запрос данных Gojek
            query = """
                SELECT stat_date, sales, orders, rating,
                       new_customers, repeat_customers, reactivated_customers,
                       ads_spend, ads_sales, ads_orders
                FROM gojek_stats 
                WHERE restaurant_id = ?
            """
            params = [restaurant_id]
            
            if start_date:
                query += " AND stat_date >= ?"
                params.append(start_date)
            if end_date:
                query += " AND stat_date <= ?"
                params.append(end_date)
            
            query += " ORDER BY stat_date"
            
            cursor.execute(query, params)
            
            for row in cursor.fetchall():
                stat = {
                    'date': row['stat_date'],
                    'platform': 'gojek',
                    'sales': row['sales'],
                    'orders': row['orders'],
                    'rating': row['rating'],
                    'new_customers': row['new_customers'],
                    'repeat_customers': row['repeat_customers'],
                    'reactivated_customers': row['reactivated_customers'],
                    'ads_spend': row['ads_spend'],
                    'ads_sales': row['ads_sales'],
                    'ads_orders': row['ads_orders']
                }
                stats.append(stat)
        
        conn.close()
        
        return jsonify({
            'restaurant': restaurant_name,
            'platform': platform,
            'period': {
                'start': start_date,
                'end': end_date
            },
            'stats': stats,
            'count': len(stats)
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/market-overview', methods=['GET'])
@require_api_key
def get_market_overview():
    """Получение обзорных данных по рынку"""
    try:
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        
        conn = get_db_connection()
        if not conn:
            return jsonify({'error': 'Database unavailable'}), 500
        
        cursor = conn.cursor()
        
        # Базовый запрос для агрегированных данных
        date_filter = ""
        params = []
        
        if start_date:
            date_filter += " AND stat_date >= ?"
            params.append(start_date)
        if end_date:
            date_filter += " AND stat_date <= ?"
            params.append(end_date)
        
        # Общая статистика
        cursor.execute(f"""
            SELECT 
                SUM(COALESCE(g.sales, 0) + COALESCE(gj.sales, 0)) as total_sales,
                COUNT(DISTINCT COALESCE(g.restaurant_id, gj.restaurant_id)) as total_restaurants,
                SUM(COALESCE(g.sales, 0)) as grab_sales,
                SUM(COALESCE(gj.sales, 0)) as gojek_sales
            FROM grab_stats g
            FULL OUTER JOIN gojek_stats gj ON g.restaurant_id = gj.restaurant_id 
                AND g.stat_date = gj.stat_date
            WHERE 1=1 {date_filter}
        """, params)
        
        overview_row = cursor.fetchone()
        
        total_sales = overview_row['total_sales'] or 0
        total_restaurants = overview_row['total_restaurants'] or 0
        grab_sales = overview_row['grab_sales'] or 0
        gojek_sales = overview_row['gojek_sales'] or 0
        
        # Топ рестораны
        cursor.execute(f"""
            SELECT r.name, 
                   SUM(COALESCE(g.sales, 0) + COALESCE(gj.sales, 0)) as total_sales
            FROM restaurants r
            LEFT JOIN grab_stats g ON r.id = g.restaurant_id
            LEFT JOIN gojek_stats gj ON r.id = gj.restaurant_id
            WHERE 1=1 {date_filter}
            GROUP BY r.id, r.name
            HAVING total_sales > 0
            ORDER BY total_sales DESC
            LIMIT 10
        """, params)
        
        top_restaurants = []
        for row in cursor.fetchall():
            top_restaurants.append({
                'name': row['name'],
                'sales': row['total_sales']
            })
        
        conn.close()
        
        return jsonify({
            'period': {
                'start': start_date,
                'end': end_date
            },
            'total_sales': total_sales,
            'total_restaurants': total_restaurants,
            'avg_sales_per_restaurant': total_sales / total_restaurants if total_restaurants > 0 else 0,
            'platforms': {
                'grab': {
                    'sales': grab_sales,
                    'share': grab_sales / total_sales if total_sales > 0 else 0
                },
                'gojek': {
                    'sales': gojek_sales,
                    'share': gojek_sales / total_sales if total_sales > 0 else 0
                }
            },
            'top_restaurants': top_restaurants
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/restaurant-location', methods=['GET'])
@require_api_key
def get_restaurant_location():
    """Получение координат ресторана"""
    try:
        restaurant_name = request.args.get('restaurant')
        
        if not restaurant_name:
            return jsonify({'error': 'Parameter "restaurant" is required'}), 400
        
        # Загружаем из JSON файла
        try:
            import json
            with open('data/bali_restaurant_locations.json', 'r', encoding='utf-8') as f:
                locations_data = json.load(f)
            
            for restaurant in locations_data.get('restaurants', []):
                if restaurant['name'].lower() == restaurant_name.lower():
                    return jsonify({
                        'restaurant': restaurant_name,
                        'location': {
                            'latitude': restaurant['latitude'],
                            'longitude': restaurant['longitude'],
                            'location': restaurant['location'],
                            'area': restaurant['area'],
                            'zone': restaurant['zone']
                        }
                    })
            
        except FileNotFoundError:
            pass
        
        # Если не найден, возвращаем координаты центра Бали
        return jsonify({
            'restaurant': restaurant_name,
            'location': {
                'latitude': -8.4095,
                'longitude': 115.1889,
                'location': 'Denpasar',
                'area': 'Denpasar',
                'zone': 'Central'
            }
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.errorhandler(404)
def not_found(error):
    return jsonify({'error': 'Endpoint not found'}), 404

@app.errorhandler(500)
def internal_error(error):
    return jsonify({'error': 'Internal server error'}), 500

if __name__ == '__main__':
    print("🚀 MUZAQUEST API SERVER")
    print("=" * 40)
    print(f"📄 База данных: {DATABASE_PATH}")
    print(f"🔑 API ключ: {API_KEY[:20]}...")
    print(f"🌐 Запуск сервера...")
    print()
    print("📋 Доступные endpoints:")
    print("• GET /health - проверка работоспособности")
    print("• GET /restaurants - список ресторанов")
    print("• GET /restaurant-stats - статистика ресторана")
    print("• GET /market-overview - обзор рынка")
    print("• GET /restaurant-location - координаты ресторана")
    print()
    print("🔗 Пример запроса:")
    print(f"curl -H 'Authorization: Bearer {API_KEY}' http://localhost:5000/restaurants")
    print()
    
    # Проверяем наличие БД
    if not os.path.exists(DATABASE_PATH):
        print(f"❌ База данных не найдена: {DATABASE_PATH}")
        print("   Убедитесь что файл database.sqlite находится в текущей папке")
        exit(1)
    
    # Запускаем сервер
    app.run(host='0.0.0.0', port=5000, debug=True)