# tests/test_transformers.py
"""
Tests for data transformation modules
"""

import pytest
from etl.transform.cleaners import DataCleaner, TextCleaner
from etl.transform.nlp_extractor import CropRequirementExtractor, ExtractedRequirements
from etl.transform.transformers import DataTransformer


class TestTextCleaner:
    """Test text preprocessing"""
    
    def test_abbreviation_expansion(self):
        """Test agricultural abbreviation expansion"""
        cleaner = TextCleaner()
        
        text = "Temp. should be opt. between 20-30 deg c"
        result = cleaner.clean(text)
        
        assert 'temperature' in result
        assert 'optimal' in result
        assert '°C' in result or '°c' in result
    
    def test_citation_removal(self):
        """Test removal of academic citations"""
        cleaner = TextCleaner()
        
        text = "Wheat needs water [1,2]. Also see (Smith, 2020) for more."
        result = cleaner.clean(text, aggressive=True)
        
        assert '[1,2]' not in result
        assert '(Smith, 2020)' not in result
        assert 'wheat needs water' in result.lower() 
    
    def test_sentence_extraction(self):
        """Test sentence splitting"""
        cleaner = TextCleaner()
        
        text = "First sentence about wheat. Second sentence about water. Short."
        sentences = cleaner.extract_sentences(text)
        
        assert len(sentences) == 2  # Short sentence filtered out
        assert 'wheat' in sentences[0]


class TestDataCleaner:
    """Test structured data cleaning"""
    
    def test_soil_data_cleaning(self, sample_soil_data):
        """Test soil data validation and cleaning"""
        cleaner = DataCleaner()
        
        result = cleaner.clean_soil_data(sample_soil_data)
        
        assert result['ph_level'] == 6.5
        assert result['texture'] == 'Loam'
        assert 0 <= result['clay_content'] <= 100
    
    def test_temperature_conversion(self):
        """Test Fahrenheit to Celsius conversion"""
        cleaner = DataCleaner()
        
        data = {'temperature': 77}  # 77°F = 25°C
        result = cleaner._clean_temperature(77)
        
        assert result == 25.0  # Should convert and round
    
    def test_ph_scaling(self):
        """Test pH value scaling from SoilGrids format"""
        cleaner = DataCleaner()
        
        # SoilGrids stores pH * 10
        assert cleaner._clean_ph(65) == 6.5
        assert cleaner._clean_ph(6.5) == 6.5  # Already correct scale
        assert cleaner._clean_ph(150) is None  # Invalid
    
    def test_water_normalization(self):
        """Test water requirement unit normalization"""
        cleaner = DataCleaner()
        
        # Various inputs should all convert to mm/day
        assert cleaner._normalize_water_requirement(5.0) == 5.0
        assert cleaner._normalize_water_requirement(0.5) == 5.0  # cm to mm
        assert cleaner._normalize_water_requirement(49.0) == 7.0  # weekly to daily


class TestNLPCropExtractor:
    """Test NLP extraction"""
    
    def test_temperature_extraction(self, sample_crop_text):
        """Test temperature range extraction"""
        extractor = CropRequirementExtractor()
        
        result = extractor.extract(sample_crop_text, "wheat")
        
        assert result.temp_min_c == 20.0
        assert result.temp_max_c == 25.0
        assert result.confidence_score > 0.5
    
    def test_water_extraction(self, sample_crop_text):
        """Test water requirement extraction"""
        extractor = CropRequirementExtractor()
        
        result = extractor.extract(sample_crop_text, "wheat")
        
        # Should extract 5-8 mm/day from "450-650 mm... 5-8 mm per day"
        assert result.water_mm_day is not None
        assert 5 <= result.water_mm_day <= 8
    
    def test_sunlight_extraction(self, sample_crop_text):
        """Test sunlight hours extraction"""
        extractor = CropRequirementExtractor()
        
        result = extractor.extract(sample_crop_text, "wheat")
        
        assert result.sunlight_hours is not None
        assert 8 <= result.sunlight_hours <= 10


class TestDataTransformer:
    """Test data transformation to warehouse schema"""
    
    def test_location_hash_generation(self):
        """Test consistent hash generation"""
        transformer = DataTransformer()
        
        hash1 = transformer.generate_location_hash(41.878113, -87.629799)
        hash2 = transformer.generate_location_hash(41.878113, -87.629799)
        hash3 = transformer.generate_location_hash(41.878114, -87.629799)
        
        assert hash1 == hash2  # Same coordinates = same hash
        assert hash1 != hash3  # Different coordinates = different hash
        assert len(hash1) == 32  # MD5 hex length