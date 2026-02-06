# etl/transform/transformers.py
from typing import List, Dict, Tuple, Any
from datetime import datetime
import hashlib
import json

from etl.extract.soil_api import SoilData
from etl.extract.weather_api import WeatherData
from etl.transform.nlp_extractor import ExtractedRequirements

class DataTransformer:
    """Transform raw extracted data into warehouse-ready format"""
    
    @staticmethod
    def transform_soil(soil_data: SoilData, location_key: int) -> Dict:
        """Transform SoilData to database schema"""
        return {
            "location_key": location_key,
            "soil_texture": soil_data.texture,
            "clay_content_0_5cm": soil_data.clay_0_5cm,
            "sand_content_0_5cm": soil_data.sand_0_5cm,
            "silt_content_0_5cm": soil_data.silt_0_5cm,
            "ph_level_0_5cm": soil_data.ph_0_5cm,
            "organic_carbon_0_5cm": soil_data.organic_carbon_0_5cm,
            "bulk_density_0_5cm": soil_data.bulk_density_0_5cm,
            "water_capacity_0_5cm": soil_data.water_capacity_0_5cm,
            "soil_depth_cm": 5,
            "extraction_date": datetime.now().date(),
            "metadata": json.dumps({
                "source": "SoilGrids",
                "timestamp": soil_data.extraction_timestamp,
                "coordinates": {
                    "lat": soil_data.latitude,
                    "lon": soil_data.longitude
                }
            })
        }
    
    @staticmethod
    def transform_weather(weather_data: WeatherData, location_key: int) -> Dict:
        """Transform WeatherData to database schema"""
        date_key = int(weather_data.date.replace("-", ""))
        
        return {
            "location_key": location_key,
            "date_key": date_key,
            "latitude": weather_data.latitude,
            "longitude": weather_data.longitude,
            "temp_max_c": weather_data.temp_max,
            "temp_min_c": weather_data.temp_min,
            "temp_mean_c": weather_data.temp_mean,
            "precipitation_mm": weather_data.precipitation,
            "evapotranspiration_mm": weather_data.evapotranspiration,
            "solar_radiation_mj_m2": weather_data.solar_radiation,
            "humidity_percent": weather_data.humidity,
            "wind_speed_ms": weather_data.wind_speed,
            "weather_code": weather_data.weather_code
        }
    
    @staticmethod
    def transform_crop_requirements(extracted: ExtractedRequirements) -> Dict:
        """Transform NLP extraction to crop dimension"""
        return {
            "crop_name": extracted.crop_name,
            "optimal_temp_min_c": extracted.temp_min_c,
            "optimal_temp_max_c": extracted.temp_max_c,
            "water_requirement_mm_day": extracted.water_mm_day,
            "sunlight_hours_min": extracted.sunlight_hours,
            "sunlight_hours_max": extracted.sunlight_hours,  # Simplified
            "soil_ph_preference_min": extracted.ph_min,
            "soil_ph_preference_max": extracted.ph_max,
            "extraction_confidence": extracted.confidence_score,
            "extraction_date": datetime.now().date(),
            "source_urls": extracted.raw_evidence
        }
    
    @staticmethod
    def generate_location_hash(lat: float, lon: float) -> str:
        """Generate unique hash for location"""
        return hashlib.md5(f"{lat:.6f},{lon:.6f}".encode()).hexdigest()