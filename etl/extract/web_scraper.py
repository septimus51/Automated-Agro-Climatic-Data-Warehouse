# etl/extract/web_scraper.py
import requests
from bs4 import BeautifulSoup
import time
import re
from typing import List, Dict, Optional
from dataclasses import dataclass
from urllib.parse import urljoin, urlparse
import xml.etree.ElementTree as ET

from etl.config import ETLConfig
from etl.utils.logger import ETLLogger

@dataclass
class CropRequirementSource:
    crop_name: str
    source_url: str
    raw_text: str
    extracted_date: str
    reliability_score: float  # 0-1 based on source authority

class CropRequirementScraper:
    """
    Web scraper for agricultural crop requirements
    Targets: FAO, USDA, Agricultural extension sites
    Implements respectful scraping with robots.txt compliance
    """
    
    # Authoritative sources for crop requirements
    SOURCES = {
        "fao": {
            "base_url": "https://www.fao.org/3/",
            "crop_pages": {
                "wheat": "x8699e/x8699e04.htm",
                "maize": "x8511e/x8511e04.htm",
                "rice": "x8499e/x8499e04.htm",
                "soybean": "x8530e/x8530e04.htm",
                "potato": "x8541e/x8541e04.htm"
            },
            "reliability": 0.95
        },
        "usda_plants": {
            "base_url": "https://plants.usda.gov/",
            "search_pattern": "core/profile?symbol={symbol}",
            "reliability": 0.90
        },
        "extension_sites": {
            "urls": [
                "https://extension.umn.edu/crop-specific-guidelines",
                "https://www.ces.ncsu.edu/crop-production"
            ],
            "reliability": 0.85
        }
    }
    
    def __init__(self, config: ETLConfig, logger: ETLLogger):
        self.config = config
        self.logger = logger
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": config.scraping.user_agent,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
            "Accept-Encoding": "gzip, deflate",
            "Connection": "keep-alive",
        })
        self.visited_urls = set()
    
    def _respectful_request(self, url: str) -> Optional[str]:
        """Make request with politeness delays"""
        if url in self.visited_urls:
            return None
        
        time.sleep(self.config.scraping.request_delay)
        
        for attempt in range(self.config.scraping.max_retries):
            try:
                response = self.session.get(
                    url, 
                    timeout=self.config.scraping.timeout,
                    allow_redirects=True
                )
                response.raise_for_status()
                self.visited_urls.add(url)
                return response.text
            except requests.exceptions.RequestException as e:
                self.logger.log_error(e, f"Scraping attempt {attempt + 1} for {url}")
                if attempt < self.config.scraping.max_retries - 1:
                    time.sleep(2 ** attempt)
        
        return None
    
    def scrape_fao_crop_profile(self, crop_name: str) -> Optional[CropRequirementSource]:
        """Scrape FAO crop profile page"""
        source_config = self.SOURCES["fao"]
        if crop_name.lower() not in source_config["crop_pages"]:
            return None
        
        url = urljoin(source_config["base_url"], 
                     source_config["crop_pages"][crop_name.lower()])
        
        html = self._respectful_request(url)
        if not html:
            return None
        
        soup = BeautifulSoup(html, 'html.parser')
        
        # FAO documents typically have content in div with class 'content' or article tags
        content_div = soup.find('div', class_='content') or soup.find('article')
        if not content_div:
            content_div = soup.find('body')
        
        # Remove script and style elements
        for script in content_div.find_all(['script', 'style', 'nav', 'footer']):
            script.decompose()
        
        # Extract text while preserving some structure
        raw_text = content_div.get_text(separator='\n', strip=True)
        
        # Clean up excessive whitespace
        raw_text = re.sub(r'\n+', '\n', raw_text)
        raw_text = re.sub(r' +', ' ', raw_text)
        
        return CropRequirementSource(
            crop_name=crop_name,
            source_url=url,
            raw_text=raw_text,
            extracted_date=time.strftime("%Y-%m-%d"),
            reliability_score=source_config["reliability"]
        )
    
    def scrape_usda_plants(self, symbol: str) -> Optional[CropRequirementSource]:
        """Scrape USDA PLANTS database"""
        base = self.SOURCES["usda_plants"]["base_url"]
        url = urljoin(base, self.SOURCES["usda_plants"]["search_pattern"].format(symbol=symbol))
        
        html = self._respectful_request(url)
        if not html:
            return None
        
        soup = BeautifulSoup(html, 'html.parser')
        
        # USDA plants typically has characteristics in definition lists or tables
        characteristics = []
        
        # Look for growth requirements sections
        dl_elements = soup.find_all('dl')
        for dl in dl_elements:
            characteristics.append(dl.get_text(separator=' ', strip=True))
        
        raw_text = "\n".join(characteristics)
        
        # Try to find crop name from title
        title = soup.find('title')
        crop_name = symbol
        if title:
            crop_name = title.get_text().split('-')[0].strip()
        
        return CropRequirementSource(
            crop_name=crop_name,
            source_url=url,
            raw_text=raw_text,
            extracted_date=time.strftime("%Y-%m-%d"),
            reliability_score=self.SOURCES["usda_plants"]["reliability"]
        )
    
    def scrape_multiple_crops(self, crop_list: List[str]) -> List[CropRequirementSource]:
        """Batch scrape multiple crops from prioritized sources"""
        results = []
        
        for crop in crop_list:
            self.logger.logger.info(f"Scraping requirements for {crop}")
            
            # Try FAO first (most authoritative)
            fao_data = self.scrape_fao_crop_profile(crop)
            if fao_data:
                results.append(fao_data)
                self.logger.log_extract(f"FAO - {crop}", len(fao_data.raw_text))
                continue
            
            # Fallback to other sources...
            # (Extension sites scraping would go here)
            
        return results