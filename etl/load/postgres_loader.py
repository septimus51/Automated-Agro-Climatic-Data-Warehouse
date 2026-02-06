# etl/load/postgres_loader.py
from typing import List, Dict, Any, Optional
from psycopg2.extras import execute_values
from etl.utils.database import PostgresManager
from etl.utils.logger import ETLLogger
from etl.config import ETLConfig

class WarehouseLoader:
    """
    Idempotent loader for data warehouse
    Implements upsert patterns and transaction management
    """
    
    def __init__(self, db_manager: PostgresManager, logger: ETLLogger, batch_id: str):
        self.db = db_manager
        self.logger = logger
        self.batch_id = batch_id
    
    def load_locations(self, locations: List[Dict]) -> Dict[int, int]:
        """
        Load locations, handling SCD Type 2
        Returns: mapping of coordinate hash to location_key
        """
        if not locations:
            return {}
        
        location_map = {}
        
        for loc in locations:
            lat, lon = loc['latitude'], loc['longitude']
            loc_hash = loc['location_hash']
            
            # Check if location exists and is current
            existing = self.db.fetch_one(
                "SELECT location_key FROM dim_location WHERE location_hash = %s AND is_current = TRUE",
                (loc_hash,)
            )
            
            if existing:
                location_map[loc_hash] = existing['location_key']
                continue
            
            # Insert new location
            query = """
                INSERT INTO dim_location (
                    latitude, longitude, country_code, country_name,
                    admin_region, location_hash, effective_date
                ) VALUES (
                    %(latitude)s, %(longitude)s, %(country_code)s, %(country_name)s,
                    %(admin_region)s, %(location_hash)s, CURRENT_DATE
                )
                RETURNING location_key;
            """
            
            try:
                result = self.db.fetch_one(query, loc)
                if result:
                    location_map[loc_hash] = result['location_key']
                    self.logger.log_load("dim_location", 1)
            except Exception as e:
                self.logger.log_error(e, f"Loading location ({lat}, {lon})")
        
        return location_map
    
    def load_soil_data(self, soil_records: List[Dict]) -> int:
        """Load soil dimension records"""
        if not soil_records:
            return 0
        
        query = """
            INSERT INTO dim_soil (
                location_key, soil_texture, clay_content_0_5cm, sand_content_0_5cm,
                silt_content_0_5cm, ph_level_0_5cm, organic_carbon_0_5cm,
                bulk_density_0_5cm, water_capacity_0_5cm, soil_depth_cm,
                extraction_date, metadata
            ) VALUES %s
            ON CONFLICT (location_key, extraction_date) DO UPDATE SET
                soil_texture = EXCLUDED.soil_texture,
                ph_level_0_5cm = EXCLUDED.ph_level_0_5cm,
                metadata = EXCLUDED.metadata;
        """
        
        values = [(
            r['location_key'], r['soil_texture'], r['clay_content_0_5cm'],
            r['sand_content_0_5cm'], r['silt_content_0_5cm'], r['ph_level_0_5cm'],
            r['organic_carbon_0_5cm'], r['bulk_density_0_5cm'],
            r['water_capacity_0_5cm'], r['soil_depth_cm'],
            r['extraction_date'], r['metadata']
        ) for r in soil_records]
        
        try:
            self.db.execute_batch(query, values)
            self.logger.log_load("dim_soil", len(soil_records))
            return len(soil_records)
        except Exception as e:
            self.logger.log_error(e, "Bulk loading soil data")
            return 0
    
    def load_weather_data(self, weather_records: List[Dict]) -> int:
        """
        Load weather fact data with idempotency check
        Uses batch_id for traceability
        """
        if not weather_records:
            return 0
        
        # Add batch_id to all records
        for record in weather_records:
            record['batch_id'] = self.batch_id
        
        query = """
            INSERT INTO fact_weather (
                location_key, date_key, latitude, longitude, temp_max_c,
                temp_min_c, temp_mean_c, precipitation_mm, evapotranspiration_mm,
                solar_radiation_mj_m2, humidity_percent, wind_speed_ms,
                weather_code, batch_id
            ) VALUES %s
            ON CONFLICT (date_key, location_key) DO UPDATE SET
                temp_max_c = EXCLUDED.temp_max_c,
                temp_min_c = EXCLUDED.temp_min_c,
                temp_mean_c = EXCLUDED.temp_mean_c,
                precipitation_mm = EXCLUDED.precipitation_mm,
                batch_id = EXCLUDED.batch_id;
        """
        
        values = [(
            r['location_key'], r['date_key'], r['latitude'], r['longitude'],
            r['temp_max_c'], r['temp_min_c'], r['temp_mean_c'],
            r['precipitation_mm'], r['evapotranspiration_mm'],
            r['solar_radiation_mj_m2'], r['humidity_percent'],
            r['wind_speed_ms'], r['weather_code'], r['batch_id']
        ) for r in weather_records]
        
        try:
            self.db.execute_batch(query, values, page_size=1000)
            self.logger.log_load("fact_weather", len(weather_records))
            return len(weather_records)
        except Exception as e:
            self.logger.log_error(e, "Bulk loading weather data")
            return 0
    
    def load_crop_requirements(self, crop_records: List[Dict]) -> int:
        """Load crop dimension with conflict handling"""
        if not crop_records:
            return 0
        
        query = """
            INSERT INTO dim_crop (
                crop_name, optimal_temp_min_c, optimal_temp_max_c,
                water_requirement_mm_day, sunlight_hours_min, sunlight_hours_max,
                soil_ph_preference_min, soil_ph_preference_max,
                extraction_confidence, extraction_date, source_urls
            ) VALUES %s
            ON CONFLICT (crop_name) DO UPDATE SET
                optimal_temp_min_c = EXCLUDED.optimal_temp_min_c,
                optimal_temp_max_c = EXCLUDED.optimal_temp_max_c,
                water_requirement_mm_day = EXCLUDED.water_requirement_mm_day,
                extraction_confidence = EXCLUDED.extraction_confidence,
                extraction_date = EXCLUDED.extraction_date;
        """
        
        values = [(
            r['crop_name'], r['optimal_temp_min_c'], r['optimal_temp_max_c'],
            r['water_requirement_mm_day'], r['sunlight_hours_min'],
            r['sunlight_hours_max'], r['soil_ph_preference_min'],
            r['soil_ph_preference_max'], r['extraction_confidence'],
            r['extraction_date'], r['source_urls']
        ) for r in crop_records]
        
        try:
            self.db.execute_batch(query, values)
            self.logger.log_load("dim_crop", len(crop_records))
            return len(crop_records)
        except Exception as e:
            self.logger.log_error(e, "Loading crop requirements")
            return 0
    
    def audit_completion(self, status: str, records_processed: int, 
                        error_msg: Optional[str] = None):
        """Write audit log entry"""
        query = """
            UPDATE etl_audit_log 
            SET end_time = CURRENT_TIMESTAMP,
                status = %s,
                records_processed = %s,
                error_message = %s
            WHERE batch_id = %s;
        """
        self.db.execute_batch(query, [(status, records_processed, error_msg, self.batch_id)])