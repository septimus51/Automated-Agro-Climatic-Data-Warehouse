# etl/extract/weather_api.py
import requests
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass

from etl.config import ETLConfig
from etl.utils.logger import ETLLogger
from etl.utils.validators import GeoValidator

@dataclass
class WeatherData:
    latitude: float
    longitude: float
    date: str
    temp_max: Optional[float]
    temp_min: Optional[float]
    temp_mean: Optional[float]
    precipitation: Optional[float]
    evapotranspiration: Optional[float]
    solar_radiation: Optional[float]
    humidity: Optional[float]
    wind_speed: Optional[float]
    weather_code: Optional[int]

class OpenMeteoExtractor:
    """
    Extractor for Open-Meteo API
    No API key required, respects CC BY 4.0 license
    Docs: https://open-meteo.com/en/docs
    """
    
    def __init__(self, config: ETLConfig, logger: ETLLogger):
        self.config = config
        self.logger = logger
        self.session = requests.Session()
        self._last_request_time = 0
    
    def _rate_limit(self):
        elapsed = time.time() - self._last_request_time
        if elapsed < 1.0:  # Open-Meteo recommends 1 req/sec for free tier
            time.sleep(1.0 - elapsed)
        self._last_request_time = time.time()
    
    def extract_historical(self, 
                          lat: float, 
                          lon: float, 
                          start_date: str,
                          end_date: str) -> List[WeatherData]:
        """
        Extract historical weather data
        date format: YYYY-MM-DD
        """
        valid, error = GeoValidator.validate_coordinates(lat, lon)
        if not valid:
            raise ValueError(error)
        
        self._rate_limit()
        
        params = {
            "latitude": lat,
            "longitude": lon,
            "start_date": start_date,
            "end_date": end_date,
            "daily": [
                "temperature_2m_max",
                "temperature_2m_min",
                "temperature_2m_mean",
                "precipitation_sum",
                "et0_fao_evapotranspiration",
                "shortwave_radiation_sum",
                "relative_humidity_2m_mean",
                "wind_speed_10m_max",
                "weather_code"
            ],
            "timezone": "auto"
        }
        
        try:
            response = self.session.get(
                self.config.api.weather_archive_url,
                params=params,
                timeout=self.config.api.weather_api_timeout
            )
            response.raise_for_status()
            data = response.json()
            
            return self._parse_daily_data(data, lat, lon)
            
        except requests.exceptions.RequestException as e:
            self.logger.log_error(e, f"Open-Meteo API request ({lat}, {lon})")
            raise
    
    def _parse_daily_data(self, data: Dict, lat: float, lon: float) -> List[WeatherData]:
        """Parse Open-Meteo daily response"""
        daily = data.get("daily", {})
        dates = daily.get("time", [])
        
        results = []
        for i, date in enumerate(dates):
            weather = WeatherData(
                latitude=lat,
                longitude=lon,
                date=date,
                temp_max=self._safe_get(daily.get("temperature_2m_max"), i),
                temp_min=self._safe_get(daily.get("temperature_2m_min"), i),
                temp_mean=self._safe_get(daily.get("temperature_2m_mean"), i),
                precipitation=self._safe_get(daily.get("precipitation_sum"), i),
                evapotranspiration=self._safe_get(daily.get("et0_fao_evapotranspiration"), i),
                solar_radiation=self._safe_get(daily.get("shortwave_radiation_sum"), i),
                humidity=self._safe_get(daily.get("relative_humidity_2m_mean"), i),
                wind_speed=self._safe_get(daily.get("wind_speed_10m_max"), i),
                weather_code=self._safe_get(daily.get("weather_code"), i)
            )
            results.append(weather)
        
        return results
    
    @staticmethod
    def _safe_get(arr, idx, default=None):
        if arr and len(arr) > idx:
            val = arr[idx]
            return val if val is not None else default
        return default
    
    def extract_forecast(self, lat: float, lon: float, days: int = 7) -> List[WeatherData]:
        """Extract forecast data (for near-real-time updates)"""
        end_date = datetime.now() + timedelta(days=days)
        return self.extract_historical(
            lat, lon,
            datetime.now().strftime("%Y-%m-%d"),
            end_date.strftime("%Y-%m-%d")
        )