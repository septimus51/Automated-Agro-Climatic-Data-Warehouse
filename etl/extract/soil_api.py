# etl/extract/soil_api.py
import requests
import time
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
import hashlib

from etl.config import ETLConfig
from etl.utils.logger import ETLLogger
from etl.utils.validators import GeoValidator

@dataclass
class SoilData:
    latitude: float
    longitude: float
    clay_0_5cm: Optional[float]
    sand_0_5cm: Optional[float]
    silt_0_5cm: Optional[float]
    ph_0_5cm: Optional[float]
    organic_carbon_0_5cm: Optional[float]
    bulk_density_0_5cm: Optional[float]
    water_capacity_0_5cm: Optional[float]
    texture: Optional[str]
    extraction_timestamp: str

class SoilGridsExtractor:
    """
    Extractor for ISRIC SoilGrids API v2.0
    API Docs: https://rest.isric.org/soilgrids/v2.0/docs
    """
    
    DEPTHS = ["0-5cm"]
    PROPERTIES = ["clay", "sand", "silt", "phh2o", "soc", "bdod", "wv0010"]
    VALUES = ["mean"]
    
    def __init__(self, config: ETLConfig, logger: ETLLogger):
        self.config = config
        self.logger = logger
        self.session = requests.Session()
        self.session.headers.update({
            "Accept": "application/json",
            "User-Agent": "AgroClimate-ETL/1.0"
        })
        self._last_request_time = 0
    
    def _rate_limit(self):
        """Respect API rate limits"""
        elapsed = time.time() - self._last_request_time
        if elapsed < 1.0 / self.config.api.requests_per_second:
            time.sleep(1.0 / self.config.api.requests_per_second - elapsed)
        self._last_request_time = time.time()
    
    def _make_request(self, lat: float, lon: float) -> Dict:
        """Execute API request with retries"""
        self._rate_limit()
        
        params = {
            "lon": lon,
            "lat": lat,
            "depth": self.DEPTHS,
            "properties": self.PROPERTIES,
            "value": self.VALUES
        }
        
        for attempt in range(3):
            try:
                response = self.session.get(
                    self.config.api.soil_api_url,
                    params=params,
                    timeout=self.config.api.soil_api_timeout
                )
                response.raise_for_status()
                return response.json()
            except requests.exceptions.RequestException as e:
                if attempt == 2:
                    raise
                time.sleep(2 ** attempt)  # Exponential backoff
        
        return {}
    
    def _parse_response(self, data: Dict, lat: float, lon: float) -> SoilData:
        """Parse SoilGrids JSON response"""
        properties = data.get("properties", {}).get("layers", [])
        
        def get_value(prop_name: str) -> Optional[float]:
            for layer in properties:
                if layer.get("name") == prop_name:
                    depths = layer.get("depths", [])
                    for depth in depths:
                        if depth.get("range", {}).get("top_depth") == 0:
                            values = depth.get("values", {})
                            return values.get("mean")
            return None
        
        # Map SoilGrids codes to our schema
        # Note: SoilGrids values are often scaled (e.g., ph * 10, soc / 10)
        ph_raw = get_value("phh2o")
        soc_raw = get_value("soc")
        
        return SoilData(
            latitude=lat,
            longitude=lon,
            clay_0_5cm=get_value("clay"),
            sand_0_5cm=get_value("sand"),
            silt_0_5cm=get_value("silt"),
            ph_0_5cm=ph_raw / 10 if ph_raw else None,  # SoilGrids stores pH * 10
            organic_carbon_0_5cm=soc_raw / 10 if soc_raw else None,  # SOC in g/kg, convert if needed
            bulk_density_0_5cm=get_value("bdod"),
            water_capacity_0_5cm=get_value("wv0010"),
            texture=self._infer_texture(
                get_value("clay"), 
                get_value("sand"), 
                get_value("silt")
            ),
            extraction_timestamp=data.get("timeStamp", "")
        )
    
    def _infer_texture(self, clay: Optional[float], 
                      sand: Optional[float], 
                      silt: Optional[float]) -> Optional[str]:
        """Infer USDA texture class from particle size distribution"""
        if not all([clay, sand, silt]):
            return None
        
        # Simplified texture triangle logic
        if sand >= 85 and silt + 1.5 * clay < 15:
            return "Sand"
        elif silt >= 80 and clay < 12:
            return "Silt"
        elif clay >= 40:
            return "Clay"
        elif sand >= 52 and silt + 2 * clay < 50:
            return "Sandy Loam"
        elif silt >= 50 and clay < 27:
            return "Silt Loam"
        elif clay >= 27 and clay < 40 and sand > 20:
            return "Clay Loam"
        else:
            return "Loam"
    
    def extract(self, coordinates: List[Tuple[float, float]]) -> List[SoilData]:
        """
        Extract soil data for multiple coordinates
        Implements idempotency checking
        """
        results = []
        
        for lat, lon in coordinates:
            # Validate coordinates
            valid, error = GeoValidator.validate_coordinates(lat, lon)
            if not valid:
                self.logger.log_error(ValueError(error), f"Soil extraction for ({lat}, {lon})")
                continue
            
            # Check idempotency
            coord_hash = hashlib.md5(f"{lat:.6f},{lon:.6f}".encode()).hexdigest()
            # Idempotency check done at load phase for soil (static data)
            
            try:
                raw_data = self._make_request(lat, lon)
                soil_data = self._parse_response(raw_data, lat, lon)
                results.append(soil_data)
                self.logger.log_extract("SoilGrids API", 1)
            except Exception as e:
                self.logger.log_error(e, f"SoilGrids API request ({lat}, {lon})")
        
        return results