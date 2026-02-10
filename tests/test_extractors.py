# tests/test_extractors.py
"""
Tests for data extraction modules
"""

import pytest
import responses
import requests
from unittest.mock import Mock, patch
from datetime import datetime

from etl.extract.soil_api import SoilGridsExtractor, SoilData
from etl.extract.weather_api import OpenMeteoExtractor, WeatherData
from etl.extract.web_scraper import CropRequirementScraper, CropRequirementSource
from etl.config import ETLConfig


class TestSoilGridsExtractor:
    """Test suite for SoilGrids API extractor"""
    
    @responses.activate
    def test_successful_extraction(self, mock_logger):
        """Test successful API response parsing"""
        # Mock API response
        api_response = {
            "properties": {
                "layers": [
                    {
                        "name": "clay",
                        "depths": [{
                            "range": {"top_depth": 0, "bottom_depth": 5},
                            "values": {"mean": 250}  # Stored as g/kg * 10
                        }]
                    },
                    {
                        "name": "phh2o",
                        "depths": [{
                            "range": {"top_depth": 0},
                            "values": {"mean": 65}  # pH * 10
                        }]
                    }
                ]
            },
            "timeStamp": "2024-01-15T10:00:00Z"
        }
        
        responses.add(
            responses.GET,
            'https://rest.isric.org/soilgrids/v2.0/properties/query',
            json=api_response,
            status=200
        )
        
        config = ETLConfig()
        extractor = SoilGridsExtractor(config, mock_logger)
        
        result = extractor.extract([(41.8781, -87.6298)])
        
        assert len(result) == 1
        assert isinstance(result[0], SoilData)
        assert result[0].clay_0_5cm == 250
        assert result[0].ph_0_5cm == 6.5  # Scaled from 65
    
    @responses.activate
    def test_api_failure_handling(self, mock_logger):
        """Test graceful handling of API failures"""
        responses.add(
            responses.GET,
            'https://rest.isric.org/soilgrids/v2.0/properties/query',
            status=500
        )
        
        config = ETLConfig()
        extractor = SoilGridsExtractor(config, mock_logger)
        
        # Should not raise, should log error and return empty list
        result = extractor.extract([(41.8781, -87.6298)])
        
        assert len(result) == 0
        mock_logger.log_error.assert_called()


class TestOpenMeteoExtractor:
    """Test suite for Open-Meteo API extractor"""
    
    @responses.activate
    def test_historical_weather_extraction(self, mock_logger):
        """Test historical weather data extraction"""
        api_response = {
            "daily": {
                "time": ["2024-01-01", "2024-01-02"],
                "temperature_2m_max": [5.2, 6.1],
                "temperature_2m_min": [-2.1, -1.5],
                "temperature_2m_mean": [1.5, 2.3],
                "precipitation_sum": [0.0, 2.5],
                "et0_fao_evapotranspiration": [1.2, 0.8]
            }
        }
        
        responses.add(
            responses.GET,
            'https://archive-api.open-meteo.com/v1/archive',
            json=api_response,
            status=200
        )
        
        config = ETLConfig()
        extractor = OpenMeteoExtractor(config, mock_logger)
        
        result = extractor.extract_historical(52.52, 13.405, "2024-01-01", "2024-01-02")
        
        assert len(result) == 2
        assert result[0].temp_max == 5.2
        assert result[0].date == "2024-01-01"
    
    def test_coordinate_validation(self, mock_logger):
        """Test invalid coordinate rejection"""
        config = ETLConfig()
        extractor = OpenMeteoExtractor(config, mock_logger)
        
        with pytest.raises(ValueError):
            extractor.extract_historical(95.0, 200.0, "2024-01-01", "2024-01-02")


class TestCropRequirementScraper:
    """Test suite for web scraper"""
    
    @responses.activate
    def test_fao_scraping(self, mock_logger):
        """Test FAO website scraping"""
        html_content = """
        <html>
        <body>
            <div class="content">
                <h1>Wheat Production Guide</h1>
                <p>Wheat requires temperatures between 20-25°C.</p>
                <p>Water needs: 450-650mm per season.</p>
            </div>
        </body>
        </html>
        """
        
        responses.add(
            responses.GET,
            'https://www.fao.org/3/x8699e/x8699e04.htm',
            body=html_content,
            status=200
        )
        
        config = ETLConfig()
        scraper = CropRequirementScraper(config, mock_logger)
        
        result = scraper.scrape_fao_crop_profile('wheat')
        
        assert result is not None
        assert result.crop_name == 'wheat'
        assert '20-25°C' in result.raw_text
        assert result.reliability_score == 0.95
    
    def test_invalid_crop_handling(self, mock_logger):
        """Test handling of unsupported crops"""
        config = ETLConfig()
        scraper = CropRequirementScraper(config, mock_logger)
        
        result = scraper.scrape_fao_crop_profile('unknown_crop_12345')
        
        assert result is None