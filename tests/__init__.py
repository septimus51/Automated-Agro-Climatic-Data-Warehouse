"""
=============================================================================
AGRO-CLIMATIC DATA WAREHOUSE - TEST SUITE
=============================================================================

Package de tests pour le pipeline ETL agro-climatique.

Structure des tests:
- Unit tests: Tests isolés avec mocks (extracteurs, transformers)
- Integration tests: Tests avec vraie base de données
- E2E tests: Tests du pipeline complet (déconseillé en CI)

Usage:
    # Tous les tests
    pytest tests/ -v
    
    # Uniquement unit tests
    pytest tests/ -v -m "not integration"
    
    # Avec couverture
    pytest tests/ --cov=etl --cov-report=html
    
    # Tests spécifiques
    pytest tests/test_extractors.py::TestSoilGridsExtractor -v
"""

import os
import sys
import pytest

# =============================================================================
# CONFIGURATION DES TESTS
# =============================================================================

# Ajoute le répertoire parent au path pour imports
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

# Marqueurs pytest personnalisés
def pytest_configure(config):
    config.addinivalue_line(
        "markers", "integration: marque les tests nécessitant une vraie base de données"
    )
    config.addinivalue_line(
        "markers", "slow: marque les tests lents (scraping, API externes)"
    )
    config.addinivalue_line(
        "markers", "nlp: marque les tests nécessitant les modèles spaCy"
    )

# =============================================================================
# VARIABLES D'ENVIRONNEMENT PAR DÉFAUT POUR TESTS
# =============================================================================

# Mode test automatique
os.environ.setdefault('TESTING', 'true')
os.environ.setdefault('LOG_LEVEL', 'DEBUG')

# Database de test (différente de la prod!)
os.environ.setdefault('DB_HOST', 'localhost')
os.environ.setdefault('DB_PORT', '5432')
os.environ.setdefault('DB_NAME', 'test_agroclimate')
os.environ.setdefault('DB_USER', 'postgres')
os.environ.setdefault('DB_PASSWORD', 'postgres')

# API - mode sandbox/test quand disponible
os.environ.setdefault('SOIL_API_URL', 'https://rest.isric.org/soilgrids/v2.0/properties/query')
os.environ.setdefault('WEATHER_API_URL', 'https://archive-api.open-meteo.com/v1/archive')

# Désactiver certains appels en test pour éviter les rate limits
os.environ.setdefault('MOCK_EXTERNAL_APIS', 'true')
os.environ.setdefault('SKIP_SLOW_TESTS', 'false')

# =============================================================================
# FIXTURES GLOBALES (disponibles dans tous les fichiers de test)
# =============================================================================

@pytest.fixture(scope='session')
def test_config():
    """Configuration ETL pour les tests"""
    from etl.config import ETLConfig
    return ETLConfig()

@pytest.fixture(scope='function')
def temp_directory(tmp_path):
    """Répertoire temporaire pour fichiers de test"""
    return tmp_path

@pytest.fixture(scope='function')
def sample_coordinates():
    """Coordonnées de test (régions agricoles connues)"""
    return [
        (41.8781, -87.6298),   # Chicago, USA (Corn Belt)
        (52.5200, 13.4050),    # Berlin, Allemagne
        (-23.5505, -46.6333),  # São Paulo, Brésil
    ]

# =============================================================================
# UTILITAIRES DE TEST
# =============================================================================

class TestHelpers:
    """Méthodes utilitaires pour les tests"""
    
    @staticmethod
    def load_json_fixture(filename: str) -> dict:
        """Charge un fichier JSON depuis le dossier fixtures"""
        import json
        fixture_path = os.path.join(PROJECT_ROOT, 'tests', 'fixtures', filename)
        with open(fixture_path, 'r') as f:
            return json.load(f)
    
    @staticmethod
    def assert_dict_structure(d: dict, required_keys: list):
        """Vérifie qu'un dict contient toutes les clés requises"""
        for key in required_keys:
            assert key in d, f"Clé manquante: {key}"
    
    @staticmethod
    def mock_response(status=200, json_data=None, text=None):
        """Crée une réponse mock pour requests"""
        class MockResponse:
            def __init__(self):
                self.status_code = status
                self._json = json_data or {}
                self.text = text or ""
            
            def json(self):
                return self._json
            
            def raise_for_status(self):
                if self.status_code >= 400:
                    raise Exception(f"HTTP {self.status_code}")
        
        return MockResponse()

# Export pour import facile
__all__ = ['TestHelpers', 'PROJECT_ROOT']