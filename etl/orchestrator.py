# etl/orchestrator.py
import sys
import argparse
from datetime import datetime, timedelta
from typing import List, Tuple
import json

from etl.config import ETLConfig
from etl.utils.logger import ETLLogger
from etl.utils.database import PostgresManager
from etl.extract.soil_api import SoilGridsExtractor, SoilData
from etl.extract.weather_api import OpenMeteoExtractor, WeatherData
from etl.extract.web_scraper import CropRequirementScraper
from etl.transform.nlp_extractor import CropRequirementExtractor
from etl.transform.transformers import DataTransformer
from etl.load.postgres_loader import WarehouseLoader

class ETLPipeline:
    """
    Main ETL orchestrator
    Manages extraction, transformation, and loading workflows
    """
    
    def __init__(self):
        self.config = ETLConfig()
        self.logger = ETLLogger("etl_orchestrator")
        self.db = PostgresManager(self.config)
        self.transformer = DataTransformer()
        
    def run_soil_pipeline(self, coordinates: List[Tuple[float, float]]) -> int:
        """Execute soil data ETL"""
        batch_id = self.logger.start_batch("soil_pipeline")
        self._init_audit(batch_id, "soil_extraction")
        
        try:
            # Extract
            extractor = SoilGridsExtractor(self.config, self.logger)
            soil_data = extractor.extract(coordinates)
            
            if not soil_data:
                self._complete_audit(batch_id, "SUCCESS", 0)
                return 0
            
            # Transform locations first
            locations = []
            for sd in soil_data:
                loc_hash = self.transformer.generate_location_hash(sd.latitude, sd.longitude)
                locations.append({
                    'latitude': sd.latitude,
                    'longitude': sd.longitude,
                    'country_code': None,  # Would need reverse geocoding
                    'country_name': None,
                    'admin_region': None,
                    'location_hash': loc_hash
                })
            
            # Load
            loader = WarehouseLoader(self.db, self.logger, batch_id)
            location_map = loader.load_locations(locations)
            
            # Transform and load soil data
            soil_records = []
            for sd in soil_data:
                loc_hash = self.transformer.generate_location_hash(sd.latitude, sd.longitude)
                loc_key = location_map.get(loc_hash)
                if loc_key:
                    record = self.transformer.transform_soil(sd, loc_key)
                    soil_records.append(record)
            
            loaded = loader.load_soil_data(soil_records)
            loader.audit_completion("SUCCESS", loaded)
            
            return loaded
            
        except Exception as e:
            self.logger.log_error(e, "Soil pipeline failure")
            loader = WarehouseLoader(self.db, self.logger, batch_id)
            loader.audit_completion("FAILED", 0, str(e))
            raise
    
    def run_weather_pipeline(self, 
                            coordinates: List[Tuple[float, float]],
                            start_date: str,
                            end_date: str) -> int:
        """Execute weather data ETL"""
        batch_id = self.logger.start_batch("weather_pipeline")
        self._init_audit(batch_id, "weather_extraction")
        
        try:
            extractor = OpenMeteoExtractor(self.config, self.logger)
            loader = WarehouseLoader(self.db, self.logger, batch_id)
            
            # Ensure locations exist
            locations = []
            for lat, lon in coordinates:
                loc_hash = self.transformer.generate_location_hash(lat, lon)
                locations.append({
                    'latitude': lat, 'longitude': lon,
                    'location_hash': loc_hash,
                    'country_code': None, 'country_name': None, 'admin_region': None
                })
            
            location_map = loader.load_locations(locations)
            
            total_loaded = 0
            for lat, lon in coordinates:
                # Extract
                weather_data = extractor.extract_historical(lat, lon, start_date, end_date)
                
                # Transform
                loc_hash = self.transformer.generate_location_hash(lat, lon)
                loc_key = location_map.get(loc_hash)
                
                if not loc_key:
                    continue
                
                weather_records = [
                    self.transformer.transform_weather(wd, loc_key)
                    for wd in weather_data
                ]
                
                # Load
                loaded = loader.load_weather_data(weather_records)
                total_loaded += loaded
            
            loader.audit_completion("SUCCESS", total_loaded)
            return total_loaded
            
        except Exception as e:
            self.logger.log_error(e, "Weather pipeline failure")
            loader = WarehouseLoader(self.db, self.logger, batch_id)
            loader.audit_completion("FAILED", 0, str(e))
            raise
    
    def run_crop_pipeline(self, crop_list: List[str]) -> int:
        """Execute crop requirements ETL with NLP"""
        batch_id = self.logger.start_batch("crop_pipeline")
        self._init_audit(batch_id, "crop_extraction")
        
        try:
            # Extract
            scraper = CropRequirementScraper(self.config, self.logger)
            sources = scraper.scrape_multiple_crops(crop_list)
            
            # Transform (NLP)
            nlp_extractor = CropRequirementExtractor()
            extracted_reqs = nlp_extractor.batch_extract(sources)
            
            # Transform to DB schema
            crop_records = [
                self.transformer.transform_crop_requirements(er)
                for er in extracted_reqs
            ]
            
            # Load
            loader = WarehouseLoader(self.db, self.logger, batch_id)
            loaded = loader.load_crop_requirements(crop_records)
            
            loader.audit_completion("SUCCESS", loaded)
            return loaded
            
        except Exception as e:
            self.logger.log_error(e, "Crop pipeline failure")
            loader = WarehouseLoader(self.db, self.logger, batch_id)
            loader.audit_completion("FAILED", 0, str(e))
            raise
    
    def _init_audit(self, batch_id: str, pipeline_name: str):
        """Initialize audit record"""
        query = """
            INSERT INTO etl_audit_log (batch_id, pipeline_name, start_time, status)
            VALUES (%s, %s, CURRENT_TIMESTAMP, 'RUNNING');
        """
        # Utiliser execute simple pour un seul enregistrement
        with self.db.cursor() as cur:
            cur.execute(query, (batch_id, pipeline_name))
    
    def run_full_pipeline(self, 
                         coordinates: List[Tuple[float, float]],
                         crops: List[str],
                         weather_start: str,
                         weather_end: str):
        """Execute complete ETL workflow"""
        self.logger.logger.info("Starting full ETL pipeline")
        
        results = {
            'soil': 0,
            'weather': 0,
            'crops': 0
        }
        
        try:
            # Phase 1: Static data (Soil)
            self.logger.logger.info("Phase 1: Soil data extraction")
            results['soil'] = self.run_soil_pipeline(coordinates)
            
            # Phase 2: Time-series data (Weather)
            self.logger.logger.info("Phase 2: Weather data extraction")
            results['weather'] = self.run_weather_pipeline(
                coordinates, weather_start, weather_end
            )
            
            # Phase 3: Knowledge base (Crops)
            self.logger.logger.info("Phase 3: Crop requirements extraction")
            results['crops'] = self.run_crop_pipeline(crops)
            
            self.logger.logger.info(f"Pipeline complete: {results}")
            return results
            
        except Exception as e:
            self.logger.logger.error(f"Pipeline failed: {e}")
            raise

def main():
    parser = argparse.ArgumentParser(description='Agro-Climate ETL Pipeline')
    parser.add_argument('--mode', choices=['soil', 'weather', 'crop', 'full'], 
                       default='full', help='ETL mode')
    parser.add_argument('--coords', type=str, help='JSON array of [lat,lon] pairs')
    parser.add_argument('--crops', type=str, help='Comma-separated crop names')
    parser.add_argument('--start-date', type=str, help='Weather start YYYY-MM-DD')
    parser.add_argument('--end-date', type=str, help='Weather end YYYY-MM-DD')
    
    args = parser.parse_args()
    
    # Default coordinates (sample agricultural regions)
    default_coords = [
        (41.8781, -87.6298),   # US Corn Belt (Chicago)
        (52.5200, 13.4050),    # Germany
        (-23.5505, -46.6333),  # Brazil
        (28.6139, 77.2090),    # India
    ]
    
    default_crops = ['wheat', 'maize', 'rice', 'soybean', 'potato']
    
    coords = json.loads(args.coords) if args.coords else default_coords
    crops = args.crops.split(',') if args.crops else default_crops
    start = args.start_date or (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')
    end = args.end_date or datetime.now().strftime('%Y-%m-%d')
    
    pipeline = ETLPipeline()
    
    if args.mode == 'soil':
        pipeline.run_soil_pipeline(coords)
    elif args.mode == 'weather':
        pipeline.run_weather_pipeline(coords, start, end)
    elif args.mode == 'crop':
        pipeline.run_crop_pipeline(crops)
    else:
        pipeline.run_full_pipeline(coords, crops, start, end)

if __name__ == "__main__":
    main()