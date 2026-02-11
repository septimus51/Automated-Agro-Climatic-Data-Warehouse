# etl/config.py
import os
import socket
from dataclasses import dataclass
from typing import Optional
import logging

def get_db_host() -> str:
    """
    Détecte automatiquement l'hôte PostgreSQL.
    Priorité : 
    1. Variable d'environnement DB_HOST si définie explicitement
    2. 'postgres' si on peut résoudre ce nom (réseau Docker)
    3. 'localhost' par défaut (exécution locale)
    """
    # Si DB_HOST est explicitement défini et différent de 'postgres', l'utiliser
    env_host = os.getenv('DB_HOST')
    if env_host and env_host != 'postgres':
        return env_host
    
    # Essayer de résoudre 'postgres' (nom de service Docker)
    try:
        socket.gethostbyname('postgres')
        return 'postgres'
    except socket.gaierror:
        pass
    
    # Essayer localhost
    try:
        socket.gethostbyname('localhost')
        return 'localhost'
    except socket.gaierror:
        pass
    
    # Fallback sur 127.0.0.1
    return '127.0.0.1'

def is_running_in_docker() -> bool:
    """Vérifie si le code s'exécute dans un conteneur Docker."""
    return os.path.exists('/.dockerenv') or os.getenv('DOCKER_CONTAINER') == 'true'

@dataclass
class DatabaseConfig:
    host: str = None  # Sera initialisé dans __post_init__
    port: int = None
    database: str = None
    user: str = None
    password: str = None
    
    def __post_init__(self):
        """Initialise les valeurs par défaut après la création de l'instance."""
        if self.host is None:
            self.host = get_db_host()
        if self.port is None:
            self.port = int(os.getenv('DB_PORT', 5432))
        if self.database is None:
            self.database = os.getenv('DB_NAME', 'agroclimate')
        if self.user is None:
            self.user = os.getenv('DB_USER', 'etl_user')
        if self.password is None:
            self.password = os.getenv('DB_PASSWORD', 'etl_password')
    
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
        # Créer le répertoire logs si nécessaire (pour exécution locale)
        log_dir = '/app/logs' if is_running_in_docker() else './logs'
        os.makedirs(log_dir, exist_ok=True)
        
        logging.basicConfig(
            level=getattr(logging, self.log_level),
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.StreamHandler(),
                logging.FileHandler(f'{log_dir}/etl.log')
            ]
        )
        return logging.getLogger('etl_pipeline')