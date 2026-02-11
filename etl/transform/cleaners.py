# etl/transform/cleaners.py
"""
Data Cleaning and Preprocessing Utilities
"""

import re
import unicodedata
from typing import Optional, List, Dict, Any
from datetime import datetime
import pandas as pd
import numpy as np


class TextCleaner:
    """
    Text preprocessing for NLP pipelines
    Handles agricultural text normalization
    """
    
    # Common abbreviations in agricultural texts
    ABBREVIATIONS = {
        'temp.': 'temperature',
        'temp': 'temperature',
        'max.': 'maximum',
        'max': 'maximum',
        'min.': 'minimum', 
        'min': 'minimum',
        'opt.': 'optimal',
        'opt': 'optimal',
        'req.': 'required',
        'req': 'required',
        'precip.': 'precipitation',
        'precip': 'precipitation',
        'evap.': 'evapotranspiration',
        'evap': 'evapotranspiration',
        'hum.': 'humidity',
        'hum': 'humidity',
        'moist.': 'moisture',
        'moist': 'moisture',
        'ph': 'pH',
        'mm': 'millimeters',
        'cm': 'centimeters',
        'kg/ha': 'kilograms per hectare',
        't/ha': 'tons per hectare',
        '°c': '°C',
        'deg c': '°C',
        'degrees c': '°C',
        'deg celsius': '°C',
    }
    
    # Units normalization mapping
    UNITS_MAP = {
        'millimeters': 'mm',
        'millimeter': 'mm',
        'mm/day': 'mm/day',
        'mm d-1': 'mm/day',
        'mm per day': 'mm/day',
        'liters': 'L',
        'liter': 'L',
        'l/m2': 'L/m²',
        'hours': 'hours',
        'hour': 'hours',
        'hrs': 'hours',
        'hr': 'hours',
        'h': 'hours',
        'celsius': '°C',
        'centigrade': '°C',
        'fahrenheit': '°F',
        'percent': '%',
        'percentage': '%',
    }
    
    def __init__(self):
        self.contraction_pattern = re.compile(r"(\w+)'(\w+)")
        self.whitespace_pattern = re.compile(r'\s+')
        self.special_chars_pattern = re.compile(r'[^\w\s\.\,\;\:\-\(\)\[\]\{\}\°\%\']')
        
    def clean(self, text: str, aggressive: bool = False) -> str:
        """
        Main cleaning pipeline
        
        Args:
            text: Raw input text
            aggressive: If True, removes more noise (useful for NLP)
        """
        if not text or not isinstance(text, str):
            return ""
        
        # Basic normalization
        text = self._normalize_unicode(text)
        text = self._expand_abbreviations(text)
        text = self._normalize_units(text)
        
        if aggressive:
            text = self._remove_citations(text)
            text = self._remove_references(text)
            text = self._standardize_numbers(text)
        
        # Final cleanup
        text = self._clean_whitespace(text)
        text = self._normalize_case(text)
        
        return text.strip()
    
    def _normalize_unicode(self, text: str) -> str:
        """Normalize unicode characters"""
        return unicodedata.normalize('NFKC', text)
    
    def _expand_abbreviations(self, text: str) -> str:
        """Expand common agricultural abbreviations"""
        # Sort by length (longest first) to avoid partial matches
        sorted_abbrs = sorted(self.ABBREVIATIONS.items(), key=lambda x: len(x[0]), reverse=True)
        
        for abbr, full in sorted_abbrs:
            # Use word boundaries to avoid matching inside words
            pattern = re.compile(r'\b' + re.escape(abbr) + r'\b', re.IGNORECASE)
            text = pattern.sub(full, text)
        return text
    
    def _normalize_units(self, text: str) -> str:
        """Standardize unit representations"""
        for variant, standard in self.UNITS_MAP.items():
            pattern = re.compile(r'\b' + re.escape(variant) + r'\b', re.IGNORECASE)
            text = pattern.sub(standard, text)
        return text
    
    def _remove_citations(self, text: str) -> str:
        """Remove academic citations like [1], (Author, 2020)"""
        # Remove bracket citations [1], [2,3], etc.
        text = re.sub(r'\[\d+(?:,\s*\d+)*\]', '', text)
        # Remove author-year citations
        text = re.sub(r'\([A-Z][a-z]+(?:\s+et\s+al\.?)?,\s*\d{4}[a-z]?\)', '', text)
        # Remove "Also see..." references
        text = re.sub(r'also see.*?(?:for more|more info|details).*', '', text, flags=re.IGNORECASE)
        return text
    
    def _remove_references(self, text: str) -> str:
        """Remove reference sections and URLs"""
        # Remove URLs
        text = re.sub(r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', '', text)
        # Remove "References" section and everything after
        text = re.split(r'\n\s*References?\s*\n', text, flags=re.IGNORECASE)[0]
        return text
    
    def _standardize_numbers(self, text: str) -> str:
        """Standardize number formats"""
        # Convert written numbers to digits for key terms
        number_words = {
            'zero': '0', 'one': '1', 'two': '2', 'three': '3', 'four': '4',
            'five': '5', 'six': '6', 'seven': '7', 'eight': '8', 'nine': '9',
            'ten': '10', 'twenty': '20', 'thirty': '30'
        }
        
        for word, digit in number_words.items():
            pattern = re.compile(r'\b' + word + r'\b', re.IGNORECASE)
            text = pattern.sub(digit, text)
        
        return text
    
    def _clean_whitespace(self, text: str) -> str:
        """Normalize whitespace"""
        text = self.whitespace_pattern.sub(' ', text)
        return text
    
    def _normalize_case(self, text: str) -> str:
        """Smart case normalization"""
        # Keep acronyms uppercase, normalize rest
        lines = []
        for line in text.split('\n'):
            # Detect if line is mostly uppercase (likely header)
            if sum(1 for c in line if c.isupper()) > len(line) * 0.5:
                lines.append(line.title())
            else:
                lines.append(line.lower())
        return '\n'.join(lines)
    
    def extract_sentences(self, text: str) -> List[str]:
        """Split text into sentences for sentence-level NLP"""
        # Handle common abbreviations that might break sentence detection
        text = re.sub(r'(Dr|Mr|Mrs|Ms|Prof|Sr|Jr|vs|vol|fig|et al)\.', r'\1<DOT>', text)
        sentences = re.split(r'(?<=[.!?])\s+', text)
        sentences = [s.replace('<DOT>', '.').strip() for s in sentences if len(s) > 10]
        return sentences


class DataCleaner:
    """
    Structured data cleaning for API responses and DataFrames
    """
    
    # Valid ranges for agricultural data validation
    VALID_RANGES = {
        'latitude': (-90, 90),
        'longitude': (-180, 180),
        'temperature_c': (-50, 60),
        'precipitation_mm': (0, 2000),
        'ph': (0, 14),
        'soil_moisture': (0, 100),
        'humidity': (0, 100),
        'wind_speed': (0, 200),
    }
    
    def __init__(self):
        self.text_cleaner = TextCleaner()
        self.validation_errors = []
    
    def clean_soil_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Clean and validate soil data from API
        
        Args:
            data: Raw soil data dictionary
            
        Returns:
            Cleaned data dictionary
        """
        cleaned = {}
        
        # Coordinate validation
        lat = data.get('latitude')
        lon = data.get('longitude')
        
        if self._validate_range('latitude', lat) and self._validate_range('longitude', lon):
            cleaned['latitude'] = round(float(lat), 6)
            cleaned['longitude'] = round(float(lon), 6)
        else:
            raise ValueError(f"Invalid coordinates: ({lat}, {lon})")
        
        # Soil properties - handle missing and scale values
        cleaned['clay_content'] = self._clean_percentage(data.get('clay_0_5cm'))
        cleaned['sand_content'] = self._clean_percentage(data.get('sand_0_5cm'))
        cleaned['silt_content'] = self._clean_percentage(data.get('silt_0_5cm'))
        cleaned['ph_level'] = self._clean_ph(data.get('ph_0_5cm'))
        cleaned['organic_carbon'] = self._clean_numeric(data.get('organic_carbon_0_5cm'))
        cleaned['bulk_density'] = self._clean_numeric(data.get('bulk_density_0_5cm'))
        cleaned['water_capacity'] = self._clean_numeric(data.get('water_capacity_0_5cm'))
        
        # Texture classification validation
        texture = data.get('texture')
        if texture and texture in ['Sand', 'Sandy Loam', 'Loam', 'Silt Loam', 
                                    'Silt', 'Clay Loam', 'Silty Clay Loam', 
                                    'Sandy Clay Loam', 'Sandy Clay', 'Silty Clay', 'Clay']:
            cleaned['texture'] = texture
        else:
            cleaned['texture'] = self._infer_texture(
                cleaned.get('clay_content'),
                cleaned.get('sand_content'),
                cleaned.get('silt_content')
            )
        
        return cleaned
    
    def clean_weather_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Clean and validate weather data
        
        Args:
            data: Raw weather data dictionary
            
        Returns:
            Cleaned data dictionary
        """
        cleaned = {}
        
        # Date parsing
        date_str = data.get('date')
        if date_str:
            try:
                cleaned['date'] = pd.to_datetime(date_str).strftime('%Y-%m-%d')
            except (ValueError, pd.errors.OutOfBoundsDatetime):
                self.validation_errors.append(f"Invalid date: {date_str}")
                cleaned['date'] = None
        
        # Temperature validation with outlier detection
        for temp_key in ['temp_max', 'temp_min', 'temp_mean']:
            temp = data.get(temp_key)
            cleaned[temp_key] = self._clean_temperature(temp)
        
        # Ensure temp_max >= temp_min
        if (cleaned.get('temp_max') is not None and 
            cleaned.get('temp_min') is not None and
            cleaned['temp_max'] < cleaned['temp_min']):
            cleaned['temp_max'], cleaned['temp_min'] = cleaned['temp_min'], cleaned['temp_max']
        
        # Precipitation (non-negative)
        precip = data.get('precipitation')
        cleaned['precipitation'] = max(0, self._clean_numeric(precip)) if precip is not None else None
        
        # Evapotranspiration
        evap = data.get('evapotranspiration')
        cleaned['evapotranspiration'] = self._clean_numeric(evap)
        
        # Solar radiation (must be non-negative)
        solar = data.get('solar_radiation')
        cleaned['solar_radiation'] = max(0, self._clean_numeric(solar)) if solar is not None else None
        
        # Humidity (0-100)
        humidity = data.get('humidity')
        cleaned['humidity'] = self._clamp(self._clean_numeric(humidity), 0, 100)
        
        # Wind speed (non-negative)
        wind = data.get('wind_speed')
        cleaned['wind_speed'] = max(0, self._clean_numeric(wind)) if wind is not None else None
        
        return cleaned
    
    def clean_crop_requirements(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Clean extracted crop requirement data
        
        Args:
            data: Raw extraction results
            
        Returns:
            Cleaned and validated crop requirements
        """
        cleaned = {}
        
        # Crop name standardization
        crop_name = data.get('crop_name', '')
        cleaned['crop_name'] = self._standardize_crop_name(crop_name)
        
        # Temperature ranges
        temp_min = data.get('temp_min_c')
        temp_max = data.get('temp_max_c')
        
        if temp_min is not None and temp_max is not None:
            # Ensure min < max
            if temp_min > temp_max:
                temp_min, temp_max = temp_max, temp_min
            
            # Validate reasonable crop temperatures
            if self._validate_range('temperature_c', temp_min) and \
               self._validate_range('temperature_c', temp_max):
                cleaned['temp_min_c'] = round(temp_min, 1)
                cleaned['temp_max_c'] = round(temp_max, 1)
            else:
                cleaned['temp_min_c'] = None
                cleaned['temp_max_c'] = None
                self.validation_errors.append(
                    f"Temperature out of range for {crop_name}: {temp_min}-{temp_max}°C"
                )
        else:
            cleaned['temp_min_c'] = temp_min
            cleaned['temp_max_c'] = temp_max
        
        # Water requirements (convert various units to mm/day)
        water = data.get('water_mm_day')
        cleaned['water_mm_day'] = self._normalize_water_requirement(water)
        
        # Sunlight (0-24 hours)
        sun = data.get('sunlight_hours')
        if sun is not None:
            cleaned['sunlight_hours'] = self._clamp(sun, 0, 24)
        
        # pH range
        ph_min = data.get('ph_min')
        ph_max = data.get('ph_max')
        
        if ph_min is not None and ph_max is not None:
            if ph_min > ph_max:
                ph_min, ph_max = ph_max, ph_min
            cleaned['ph_min'] = self._clamp(ph_min, 0, 14)
            cleaned['ph_max'] = self._clamp(ph_max, 0, 14)
        else:
            cleaned['ph_min'] = ph_min
            cleaned['ph_max'] = ph_max
        
        # Confidence score (0-1)
        confidence = data.get('confidence', 0)
        cleaned['confidence'] = self._clamp(confidence, 0, 1)
        
        return cleaned
    
    def _clean_percentage(self, value: Optional[float]) -> Optional[float]:
        """Clean percentage values (0-100)"""
        if value is None:
            return None
        
        # Handle values that might be stored as 0-1 instead of 0-100
        if 0 <= value <= 1:
            return round(value * 100, 2)
        elif 0 <= value <= 100:
            return round(value, 2)
        else:
            return None
    
    def _clean_ph(self, value: Optional[float]) -> Optional[float]:
        """Clean pH values"""
        if value is None:
            return None
        
        # SoilGrids stores pH * 10
        if 0 <= value <= 14:
            return round(value, 2)
        elif 0 <= value <= 140:  # Likely scaled
            return round(value / 10, 2)
        else:
            return None
    
    def _clean_temperature(self, value: Optional[float]) -> Optional[float]:
        """Clean and validate temperature"""
        if value is None:
            return None
        
        # Check for obvious unit errors (Fahrenheit vs Celsius)
        # If value > 60, likely Fahrenheit - convert
        if value > 60:
            value = (value - 32) * 5/9
        
        if self._validate_range('temperature_c', value):
            return round(value, 1)
        return None
    
    def _clean_numeric(self, value: Any) -> Optional[float]:
        """Clean numeric values"""
        if value is None:
            return None
        
        try:
            num = float(value)
            if np.isnan(num) or np.isinf(num):
                return None
            return round(num, 3)
        except (ValueError, TypeError):
            return None
    
    def _normalize_water_requirement(self, value: Optional[float]) -> Optional[float]:
        """Convert various water units to mm/day"""
        if value is None:
            return None
        
        # Assume mm/day if reasonable range
        if 1.0 <= value <= 40:
            return round(value, 2)
        # If small (likely cm/day), convert to mm
        elif 0.01 <= value < 1.0:  # Changed from < 0.1 to < 1.0
            return round(value * 10, 2)  # Convert cm to mm
        # If large, might be weekly
        elif 40 < value <= 350:
            return round(value / 7, 2)  # Convert weekly to daily
        
        return None
    
    def _standardize_crop_name(self, name: str) -> str:
        """Standardize crop names"""
        if not name:
            return "Unknown"
        
        name = name.strip().lower()
        
        # Common name mappings
        name_map = {
            'maize': 'Maize',
            'corn': 'Maize',
            'zea mays': 'Maize',
            'wheat': 'Wheat',
            'triticum': 'Wheat',
            'bread wheat': 'Wheat',
            'durum wheat': 'Wheat',
            'rice': 'Rice',
            'oryza sativa': 'Rice',
            'paddy': 'Rice',
            'soybean': 'Soybean',
            'soy': 'Soybean',
            'glycine max': 'Soybean',
            'soya': 'Soybean',
            'potato': 'Potato',
            'solanum tuberosum': 'Potato',
            'irish potato': 'Potato',
            'tomato': 'Tomato',
            'solanum lycopersicum': 'Tomato',
            'barley': 'Barley',
            'hordeum vulgare': 'Barley',
            'cotton': 'Cotton',
            'gossypium': 'Cotton',
        }
        
        return name_map.get(name, name.title())
    
    def _infer_texture(self, clay: Optional[float], 
                      sand: Optional[float], 
                      silt: Optional[float]) -> Optional[str]:
        """Infer USDA texture class"""
        if not all([clay, sand, silt]):
            return None
        
        # Normalize to 100%
        total = clay + sand + silt
        if total == 0:
            return None
        
        clay_pct = (clay / total) * 100
        sand_pct = (sand / total) * 100
        silt_pct = (silt / total) * 100
        
        # USDA Texture Triangle simplified
        if sand_pct >= 85 and silt_pct + 1.5 * clay_pct < 15:
            return "Sand"
        elif silt_pct >= 80 and clay_pct < 12:
            return "Silt"
        elif clay_pct >= 40:
            return "Clay"
        elif sand_pct >= 52 and silt_pct + 2 * clay_pct < 50:
            return "Sandy Loam"
        elif silt_pct >= 50 and clay_pct < 27:
            return "Silt Loam"
        elif clay_pct >= 27 and clay_pct < 40 and sand_pct > 20:
            return "Clay Loam"
        else:
            return "Loam"
    
    def _validate_range(self, field: str, value: Any) -> bool:
        """Validate value is within acceptable range"""
        if value is None:
            return False
        
        min_val, max_val = self.VALID_RANGES.get(field, (float('-inf'), float('inf')))
        
        try:
            num = float(value)
            return min_val <= num <= max_val
        except (ValueError, TypeError):
            return False
    
    def _clamp(self, value: Optional[float], min_val: float, max_val: float) -> Optional[float]:
        """Clamp value to range"""
        if value is None:
            return None
        return max(min_val, min(max_val, value))
    
    def get_validation_report(self) -> Dict[str, Any]:
        """Get report of validation errors"""
        return {
            'error_count': len(self.validation_errors),
            'errors': self.validation_errors[:10],  # Limit to first 10
            'timestamp': datetime.now().isoformat()
        }
    
    def clear_errors(self):
        """Clear validation error log"""
        self.validation_errors = []