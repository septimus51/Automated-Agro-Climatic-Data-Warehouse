# etl/config.py
import os
from dataclasses import dataclass
from typing import Optional
import logging

@dataclass
class DatabaseConfig:
    host: str = os.getenv('DB_HOST', 'postgres')
    port: int = int(os.getenv('DB_PORT', 5432))
    database: str = os.getenv('DB_NAME', 'agroclimate')
    user: str = os.getenv('DB_USER', 'etl_user')
    password: str = os.getenv('DB_PASSWORD', 'etl_password')
    
    @property
    def connection_string(self) -> str:
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"

@dataclass
class APIConfig:
    # SoilGrids API
    soil_api_url: str = "https://rest.isric.org/soilgrids/v2.0/properties/query"
    soil_api_timeout: int = 30
    
    # Open-Meteo API
    weather_api_url: str = "https://api.open-meteo.com/v1/forecast"
    weather_archive_url: str = "https://archive-api.open-meteo.com/v1/archive"
    weather_api_timeout: int = 30
    
    # Rate limiting
    requests_per_second: float = 1.0

@dataclass
class ScrapingConfig:
    user_agent: str = "AgroClimateDataBot/1.0 (Research Project)"
    request_delay: float = 2.0
    timeout: int = 30
    max_retries: int = 3
    respect_robots_txt: bool = True

class ETLConfig:
    def __init__(self):
        self.db = DatabaseConfig()
        self.api = APIConfig()
        self.scraping = ScrapingConfig()
        self.batch_size: int = int(os.getenv('ETL_BATCH_SIZE', 1000))
        self.log_level: str = os.getenv('LOG_LEVEL', 'INFO')
        self.data_retention_days: int = int(os.getenv('DATA_RETENTION_DAYS', 365))
        
    def setup_logging(self) -> logging.Logger:
        logging.basicConfig(
            level=getattr(logging, self.log_level),
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.StreamHandler(),
                logging.FileHandler('/app/logs/etl.log')
            ]
        )
        return logging.getLogger('etl_pipeline')