# etl/utils/validators.py
from typing import Tuple, Optional
import re

class GeoValidator:
    @staticmethod
    def validate_coordinates(lat: float, lon: float) -> Tuple[bool, Optional[str]]:
        if not (-90 <= lat <= 90):
            return False, f"Latitude {lat} out of range [-90, 90]"
        if not (-180 <= lon <= 180):
            return False, f"Longitude {lon} out of range [-180, 180]"
        return True, None
    
    @staticmethod
    def normalize_coordinates(lat: float, lon: float, precision: int = 6) -> Tuple[float, float]:
        """Round to reduce precision errors"""
        return round(lat, precision), round(lon, precision)

class CropDataValidator:
    TEMP_PATTERN = re.compile(r'(-?\d+\.?\d*)\s*(?:°?[Cc])?')
    WATER_PATTERN = re.compile(r'(\d+\.?\d*)\s*(mm|cm|L|liters?)')
    SUN_PATTERN = re.compile(r'(\d+\.?\d*)\s*(?:hours?|hrs?|h)')
    
    @classmethod
    def extract_temperature(cls, text: str) -> Tuple[Optional[float], Optional[float]]:
        """Extract min/max temp from text like '20-30°C'"""
        matches = cls.TEMP_PATTERN.findall(text)
        if len(matches) >= 2:
            temps = [float(m[0]) for m in matches[:2]]
            return min(temps), max(temps)
        return None, None