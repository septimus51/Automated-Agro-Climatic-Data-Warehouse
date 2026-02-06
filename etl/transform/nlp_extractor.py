# etl/transform/nlp_extractor.py
import spacy
import re
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
import logging

# Load spaCy model (would be downloaded in setup)
try:
    nlp = spacy.load("en_core_web_sm")
except OSError:
    nlp = None
    logging.warning("spaCy model not found. Run: python -m spacy download en_core_web_sm")

@dataclass
class ExtractedRequirements:
    crop_name: str
    temp_min_c: Optional[float]
    temp_max_c: Optional[float]
    water_mm_day: Optional[float]
    sunlight_hours: Optional[float]
    ph_min: Optional[float]
    ph_max: Optional[float]
    confidence_score: float
    extraction_method: str
    raw_evidence: List[str]

class CropRequirementExtractor:
    """
    NLP-based extraction of crop requirements from agricultural texts
    Uses hybrid approach: regex patterns + spaCy NER + rule-based extraction
    """
    
    # Regex patterns for different requirement types
    PATTERNS = {
        'temperature_range': [
            r'(?:temperature|temp)[^\d]*(\d+)[°°\s]*[Cc][^\d]*(?:to|and|-)[^\d]*(\d+)[°°\s]*[Cc]',
            r'(\d+)\s*°?[Cc]\s*(?:to|-)\s*(\d+)\s*°?[Cc]',
            r'optimal.*?(\d+)[°°\s]*[Cc].*?(?:to|and|-).*?(\d+)[°°\s]*[Cc]',
            r'grow.*?between.*?(\d+)[°°\s]*[Cc].*?and.*?(\d+)[°°\s]*[Cc]'
        ],
        'water_requirement': [
            r'(\d+\.?\d*)\s*(?:mm|millimeters?)\s*(?:per|\/)\s*(?:day|d)',
            r'water.*?(\d+\.?\d*)\s*(?:mm|millimeters?)',
            r'irrigation.*?(\d+\.?\d*)\s*(?:mm|L)',
            r'requires?\s+(\d+\.?\d*)\s*(?:mm|cm)\s*(?:of\s+)?water'
        ],
        'sunlight': [
            r'(\d+\.?\d*)\s*(?:hours?|hrs?|h)\s*(?:of\s+)?(?:sun|light|daylight)',
            r'sun.*?(\d+)[\s-]*(?:hours?|hrs?)',
            r'full\s+sun.*?(\d+)\s*(?:hours?|hrs?)',
            r'light.*?(\d+)\s*(?:hours?|hrs?)'
        ],
        'ph_range': [
            r'pH\s+(\d+\.?\d*)\s*(?:to|-)\s*(\d+\.?\d*)',
            r'pH.*?range.*?(\d+\.?\d*).*?(?:to|-).*?(\d+\.?\d*)',
            r'(?:acidic|alkaline).*?pH\s+(\d+\.?\d*)\s*(?:to|-)\s*(\d+\.?\d*)'
        ]
    }
    
    def __init__(self):
        self.nlp = nlp
        self.confidence_weights = {
            'regex_exact': 0.9,
            'regex_partial': 0.6,
            'nlp_entity': 0.7,
            'contextual': 0.5
        }
    
    def extract(self, text: str, crop_name: str) -> ExtractedRequirements:
        """Main extraction pipeline"""
        text_lower = text.lower()
        evidence = []
        
        # Temperature extraction
        temp_min, temp_max, temp_evidence = self._extract_temperature(text)
        if temp_evidence:
            evidence.extend(temp_evidence)
        
        # Water extraction
        water, water_evidence = self._extract_water(text)
        if water_evidence:
            evidence.append(water_evidence)
        
        # Sunlight extraction
        sun, sun_evidence = self._extract_sunlight(text)
        if sun_evidence:
            evidence.append(sun_evidence)
        
        # pH extraction
        ph_min, ph_max, ph_evidence = self._extract_ph(text)
        if ph_evidence:
            evidence.extend(ph_evidence)
        
        # Calculate overall confidence
        confidence = self._calculate_confidence(
            temp_min is not None,
            water is not None,
            sun is not None,
            ph_min is not None,
            len(evidence)
        )
        
        return ExtractedRequirements(
            crop_name=crop_name,
            temp_min_c=temp_min,
            temp_max_c=temp_max,
            water_mm_day=water,
            sunlight_hours=sun,
            ph_min=ph_min,
            ph_max=ph_max,
            confidence_score=confidence,
            extraction_method="hybrid_regex_spacy",
            raw_evidence=evidence[:5]  # Top 5 evidence snippets
        )
    
    def _extract_temperature(self, text: str) -> Tuple[Optional[float], Optional[float], List[str]]:
        """Extract temperature ranges with evidence"""
        for pattern in self.PATTERNS['temperature_range']:
            matches = re.finditer(pattern, text, re.IGNORECASE)
            for match in matches:
                try:
                    min_temp = float(match.group(1))
                    max_temp = float(match.group(2))
                    # Validate reasonable crop temperatures
                    if -10 <= min_temp <= 50 and -10 <= max_temp <= 50:
                        return min_temp, max_temp, [match.group(0)]
                except (ValueError, IndexError):
                    continue
        return None, None, []
    
    def _extract_water(self, text: str) -> Tuple[Optional[float], Optional[str]]:
        """Extract water requirements"""
        for pattern in self.PATTERNS['water_requirement']:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                try:
                    value = float(match.group(1))
                    # Validate reasonable range (0.1 to 50 mm/day)
                    if 0.1 <= value <= 50:
                        return value, match.group(0)
                except ValueError:
                    continue
        return None, None
    
    def _extract_sunlight(self, text: str) -> Tuple[Optional[float], Optional[str]]:
        """Extract sunlight requirements"""
        for pattern in self.PATTERNS['sunlight']:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                try:
                    hours = float(match.group(1))
                    if 0 <= hours <= 24:
                        return hours, match.group(0)
                except ValueError:
                    continue
        
        # Look for qualitative descriptions
        if 'full sun' in text.lower():
            return 6.0, "full sun (inferred 6+ hours)"
        elif 'partial shade' in text.lower():
            return 3.0, "partial shade (inferred 3-6 hours)"
        
        return None, None
    
    def _extract_ph(self, text: str) -> Tuple[Optional[float], Optional[float], List[str]]:
        """Extract pH preferences"""
        for pattern in self.PATTERNS['ph_range']:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                try:
                    min_ph = float(match.group(1))
                    max_ph = float(match.group(2))
                    if 3.0 <= min_ph <= 9.0 and 3.0 <= max_ph <= 9.0:
                        return min_ph, max_ph, [match.group(0)]
                except (ValueError, IndexError):
                    continue
        return None, None, []
    
    def _calculate_confidence(self, has_temp: bool, has_water: bool, 
                             has_sun: bool, has_ph: bool, evidence_count: int) -> float:
        """Calculate confidence score based on extraction completeness"""
        base_score = 0.0
        if has_temp:
            base_score += 0.3
        if has_water:
            base_score += 0.3
        if has_sun:
            base_score += 0.2
        if has_ph:
            base_score += 0.2
        
        # Bonus for multiple evidence sources
        evidence_bonus = min(evidence_count * 0.05, 0.2)
        
        return min(base_score + evidence_bonus, 1.0)
    
    def batch_extract(self, sources: List) -> List[ExtractedRequirements]:
        """Process multiple crop sources"""
        results = []
        for source in sources:
            extracted = self.extract(source.raw_text, source.crop_name)
            results.append(extracted)
        return results