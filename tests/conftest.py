# tests/conftest.py
"""
Pytest configuration and fixtures
"""

import pytest
import os
import psycopg2
from unittest.mock import Mock, MagicMock

# Set test environment
os.environ['DB_HOST'] = 'localhost'
os.environ['DB_NAME'] = 'test_agroclimate'
os.environ['LOG_LEVEL'] = 'DEBUG'


@pytest.fixture(scope='session')
def db_connection():
    """Create database connection for tests"""
    conn = psycopg2.connect(
        host=os.getenv('DB_HOST', 'localhost'),
        database=os.getenv('DB_NAME', 'test_agroclimate'),
        user=os.getenv('DB_USER', 'postgres'),
        password=os.getenv('DB_PASSWORD', 'postgres')
    )
    yield conn
    conn.close()


@pytest.fixture
def mock_logger():
    """Mock logger for unit tests"""
    logger = Mock()
    logger.log_extract = Mock()
    logger.log_transform = Mock()
    logger.log_load = Mock()
    logger.log_error = Mock()
    logger.logger = Mock()
    return logger


@pytest.fixture
def sample_soil_data():
    """Sample soil data for testing"""
    return {
        'latitude': 41.8781,
        'longitude': -87.6298,
        'clay_0_5cm': 25.5,
        'sand_0_5cm': 35.2,
        'silt_0_5cm': 39.3,
        'ph_0_5cm': 6.5,
        'organic_carbon_0_5cm': 12.4,
        'bulk_density_0_5cm': 1.35,
        'water_capacity_0_5cm': 0.25,
        'texture': 'Loam'
    }


@pytest.fixture
def sample_weather_data():
    """Sample weather data for testing"""
    return {
        'latitude': 41.8781,
        'longitude': -87.6298,
        'date': '2024-01-15',
        'temp_max': 25.5,
        'temp_min': 15.2,
        'temp_mean': 20.1,
        'precipitation': 5.2,
        'evapotranspiration': 3.1,
        'solar_radiation': 18.5,
        'humidity': 65.0,
        'wind_speed': 3.5,
        'weather_code': 1
    }


@pytest.fixture
def sample_crop_text():
    """Sample agricultural text for NLP testing"""
    return """
    Wheat (Triticum aestivum) requires optimal temperatures between 20°C and 25°C 
    for grain filling. The crop needs approximately 450-650 mm of water during 
    the growing season, equivalent to about 5-8 mm per day during peak demand. 
    Wheat prefers full sun exposure of 8-10 hours daily and grows best in soils 
    with pH between 6.0 and 7.5.
    """