"""
Common test fixtures for MuzaQuest Mini App
"""

import pytest
import pandas as pd
import numpy as np
from unittest.mock import Mock, patch
import sqlite3
import tempfile
import os


@pytest.fixture
def sample_data():
    """Sample restaurant data for testing."""
    return pd.DataFrame({
        'restaurant_name': ['Test Restaurant 1', 'Test Restaurant 2'],
        'date': ['2023-01-01', '2023-01-02'],
        'sales': [1000, 1200],
        'orders': [50, 60],
        'temperature': [25.5, 26.0],
        'is_weekend': [False, True],
        'is_holiday': [False, False]
    })


@pytest.fixture
def mock_database():
    """Mock database connection for testing."""
    with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as tmp:
        db_path = tmp.name
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Create test tables
    cursor.execute('''
        CREATE TABLE restaurants (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            location TEXT
        )
    ''')
    
    cursor.execute('''
        CREATE TABLE grab_stats (
            id INTEGER PRIMARY KEY,
            restaurant_name TEXT,
            date TEXT,
            sales REAL,
            orders INTEGER
        )
    ''')
    
    # Insert test data
    cursor.execute("INSERT INTO restaurants (name, location) VALUES (?, ?)", 
                   ("Test Restaurant", "Test Location"))
    
    cursor.execute('''
        INSERT INTO grab_stats (restaurant_name, date, sales, orders) 
        VALUES (?, ?, ?, ?)
    ''', ("Test Restaurant", "2023-01-01", 1000.0, 50))
    
    conn.commit()
    conn.close()
    
    yield db_path
    
    # Cleanup
    os.unlink(db_path)


@pytest.fixture
def mock_model():
    """Mock ML model for testing."""
    model = Mock()
    model.predict.return_value = np.array([1100.0, 1300.0])
    model.score.return_value = 0.95
    return model


@pytest.fixture
def mock_config():
    """Mock configuration for testing."""
    return {
        'database_path': 'test.db',
        'model_path': 'test_model.joblib',
        'api_keys': {
            'weather': 'test_weather_key',
            'holiday': 'test_holiday_key'
        }
    }


@pytest.fixture
def mock_weather_api():
    """Mock weather API response."""
    with patch('api_integrations.weather_service.WeatherService') as mock:
        mock.return_value.get_weather.return_value = {
            'temperature': 25.5,
            'humidity': 60,
            'weather_condition': 'sunny'
        }
        yield mock


@pytest.fixture
def mock_holiday_api():
    """Mock holiday API response."""
    with patch('api_integrations.holiday_service.HolidayService') as mock:
        mock.return_value.is_holiday.return_value = False
        yield mock


@pytest.fixture
def sample_restaurant_data():
    """Sample restaurant data with full features."""
    np.random.seed(42)
    dates = pd.date_range('2023-01-01', periods=100, freq='D')
    
    data = {
        'date': dates,
        'restaurant_name': ['Test Restaurant'] * 100,
        'sales': np.random.normal(1000, 200, 100),
        'orders': np.random.poisson(50, 100),
        'temperature': np.random.normal(25, 5, 100),
        'humidity': np.random.normal(60, 10, 100),
        'is_weekend': [d.weekday() >= 5 for d in dates],
        'is_holiday': np.random.choice([True, False], 100, p=[0.1, 0.9]),
        'weather_condition': np.random.choice(['sunny', 'cloudy', 'rainy'], 100),
        'day_of_week': [d.weekday() for d in dates],
        'month': [d.month for d in dates]
    }
    
    return pd.DataFrame(data)


@pytest.fixture(autouse=True)
def setup_test_environment():
    """Setup test environment variables."""
    os.environ['TESTING'] = '1'
    os.environ['DATABASE_URL'] = 'sqlite:///:memory:'
    yield
    # Cleanup
    if 'TESTING' in os.environ:
        del os.environ['TESTING']
    if 'DATABASE_URL' in os.environ:
        del os.environ['DATABASE_URL']