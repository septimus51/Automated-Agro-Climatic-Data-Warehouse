# =============================================================================
# AGRO-CLIMATIC DATA WAREHOUSE - MAKEFILE
# Usage: make [target]
# =============================================================================

# Variables
PYTHON := python3
PIP := pip3
VENV := venv
VENV_BIN := $(VENV)/bin
VENV_PYTHON := $(VENV_BIN)/python
VENV_PIP := $(VENV_BIN)/pip

# Docker
COMPOSE := docker-compose
DB_CONTAINER := agroclimate_db

# Couleurs pour output
BLUE := \033[36m
GREEN := \033[32m
YELLOW := \033[33m
RED := \033[31m
NC := \033[0m # No Color

# =============================================================================
# HELP - Affiche toutes les commandes disponibles
# =============================================================================

.PHONY: help
help: ## Affiche cette aide
	@echo "$(BLUE)╔══════════════════════════════════════════════════════════════╗$(NC)"
	@echo "$(BLUE)║     AGRO-CLIMATIC DATA WAREHOUSE - COMMANDES DISPONIBLES    ║$(NC)"
	@echo "$(BLUE)╚══════════════════════════════════════════════════════════════╝$(NC)"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  $(GREEN)%-20s$(NC) %s\n", $$1, $$2}'

# =============================================================================
# INSTALLATION / DÉSINSTALLATION
# =============================================================================

.PHONY: install
install: ## Installe tout l'environnement (venv + deps + db + schema)
	@echo "$(BLUE) Installation complète...$(NC)"
	$(MAKE) venv-create
	$(MAKE) deps-install
	$(MAKE) nlp-models
	$(MAKE) db-start
	$(MAKE) db-init
	$(MAKE) verify
	@echo "$(GREEN) Installation terminée !$(NC)"
	@echo "$(YELLOW) Activez l'environnement: source $(VENV)/bin/activate$(NC)"

.PHONY: uninstall
uninstall: ## Désinstalle tout (venv + conteneurs + données)
	@echo "$(RED)  Suppression complète de l'environnement...$(NC)"
	$(MAKE) db-stop
	$(MAKE) db-clean
	$(MAKE) venv-remove
	$(MAKE) clean
	@echo "$(GREEN) Désinstallation terminée$(NC)"

.PHONY: reinstall
reinstall: ## Réinstalle tout from scratch
	@echo "$(YELLOW) Réinstallation complète...$(NC)"
	$(MAKE) uninstall
	$(MAKE) install

# =============================================================================
# VIRTUAL ENVIRONMENT
# =============================================================================

.PHONY: venv-create
venv-create: ## Crée le virtual environment
	@echo "$(BLUE) Création du virtual environment...$(NC)"
	test -d $(VENV) || $(PYTHON) -m venv $(VENV)
	@echo "$(GREEN) Virtual environment créé$(NC)"

.PHONY: venv-remove
venv-remove: ## Supprime le virtual environment
	@echo "$(RED)  Suppression du virtual environment...$(NC)"
	rm -rf $(VENV)
	@echo "$(GREEN) Virtual environment supprimé$(NC)"

.PHONY: venv-activate
venv-activate: ## Affiche la commande pour activer le venv
	@echo "$(YELLOW) Exécutez: source $(VENV)/bin/activate$(NC)"

# =============================================================================
# DÉPENDANCES
# =============================================================================

.PHONY: deps-install
deps-install: ## Installe les dépendances Python
	@echo "$(BLUE)  Installation des dépendances...$(NC)"
	$(VENV_PIP) install --upgrade pip
	$(VENV_PIP) install -r requirements.txt
	@echo "$(GREEN) Dépendances installées$(NC)"

.PHONY: deps-update
deps-update: ## Met à jour les dépendances
	@echo "$(BLUE) Mise à jour des dépendances...$(NC)"
	$(VENV_PIP) install --upgrade -r requirements.txt
	@echo "$(GREEN) Dépendances mises à jour$(NC)"

.PHONY: deps-freeze
deps-freeze: ## Génère requirements.txt depuis l'env actuel
	@echo "$(BLUE) Génération de requirements.txt...$(NC)"
	$(VENV_PIP) freeze > requirements.txt
	@echo "$(GREEN) requirements.txt généré$(NC)"

# =============================================================================
# MODÈLES NLP
# =============================================================================

.PHONY: nlp-models
nlp-models: ## Télécharge les modèles spaCy
	@echo "$(BLUE) Téléchargement des modèles NLP...$(NC)"
	$(VENV_PYTHON) -m spacy download en_core_web_sm
	@echo "$(GREEN) Modèles NLP prêts$(NC)"

# =============================================================================
# BASE DE DONNÉES (DOCKER)
# =============================================================================

.PHONY: db-start
db-start: ## Démarre PostgreSQL dans Docker
	@echo "$(BLUE) Démarrage de PostgreSQL...$(NC)"
	$(COMPOSE) up -d postgres
	sleep 5
	@echo "$(GREEN) PostgreSQL démarré$(NC)"

.PHONY: db-stop
db-stop: ## Arrête PostgreSQL
	@echo "$(YELLOW) Arrêt de PostgreSQL...$(NC)"
	$(COMPOSE) stop postgres
	@echo "$(GREEN) PostgreSQL arrêté$(NC)"

.PHONY: db-restart
db-restart: ## Redémarre PostgreSQL
	$(MAKE) db-stop
	$(MAKE) db-start

.PHONY: db-clean
db-clean: ## Supprime le conteneur et les volumes
	@echo "$(RED)  Suppression des conteneurs et volumes...$(NC)"
	$(COMPOSE) down -v
	docker rm -f $(DB_CONTAINER) 2>/dev/null || true
	@echo "$(GREEN) Conteneurs et volumes supprimés$(NC)"

.PHONY: db-logs
db-logs: ## Affiche les logs PostgreSQL
	$(COMPOSE) logs -f postgres

.PHONY: db-shell
db-shell: ## Ouvre un shell psql interactif
	docker exec -it $(DB_CONTAINER) psql -U etl_user -d agroclimate

# =============================================================================
# SCHÉMA BASE DE DONNÉES
# =============================================================================

.PHONY: db-init
db-init: ## Crée la base, l'utilisateur et le schéma
	@echo "$(BLUE) Initialisation du schéma...$(NC)"
	docker exec $(DB_CONTAINER) psql -U postgres -c "DO \$$\$$ BEGIN IF NOT EXISTS (SELECT FROM pg_user WHERE usename = 'etl_user') THEN CREATE USER etl_user WITH PASSWORD 'etl_password' SUPERUSER; END IF; END \$$\$$;" 2>/dev/null || true
	docker exec $(DB_CONTAINER) psql -U postgres -c "SELECT 'CREATE DATABASE agroclimate OWNER etl_user' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'agroclimate')\gexec" 2>/dev/null || true
	docker cp db/init/01-schema.sql $(DB_CONTAINER):/tmp/
	docker exec $(DB_CONTAINER) psql -U etl_user -d agroclimate -f /tmp/01-schema.sql
	@echo "$(GREEN) Schéma créé$(NC)"

.PHONY: db-reset
db-reset: ## Réinitialise la base (supprime et recrée)
	@echo "$(YELLOW) Réinitialisation de la base...$(NC)"
	docker exec $(DB_CONTAINER) psql -U postgres -c "DROP DATABASE IF EXISTS agroclimate;" 2>/dev/null || true
	$(MAKE) db-init
	@echo "$(GREEN) Base réinitialisée$(NC)"

.PHONY: db-migrate
db-migrate: ## Exécute les migrations (futur)
	@echo "$(YELLOW) Migrations (à implémenter avec Alembic)$(NC)"

# =============================================================================
# TESTS
# =============================================================================

.PHONY: test
test: ## Lance tous les tests
	@echo "$(BLUE) Lancement des tests...$(NC)"
	$(VENV_PYTHON) -m pytest tests/ -v --tb=short
	@echo "$(GREEN) Tests terminés$(NC)"

.PHONY: test-unit
test-unit: ## Lance uniquement les tests unitaires
	@echo "$(BLUE) Tests unitaires...$(NC)"
	$(VENV_PYTHON) -m pytest tests/ -v -m "not integration" --tb=short

.PHONY: test-cov
test-cov: ## Lance les tests avec couverture
	@echo "$(BLUE) Tests avec couverture...$(NC)"
	$(VENV_PYTHON) -m pytest tests/ --cov=etl --cov-report=html --cov-report=term
	@echo "$(GREEN) Rapport de couverture généré: htmlcov/index.html$(NC)"

.PHONY: test-quick
test-quick: ## Tests rapides (sans lents)
	@echo "$(BLUE) Tests rapides...$(NC)"
	$(VENV_PYTHON) -m pytest tests/ -v --ignore=tests/test_integration.py -x

# =============================================================================
# ETL PIPELINE
# =============================================================================

.PHONY: etl-full
etl-full: ## Lance le pipeline ETL complet
	@echo "$(BLUE) Lancement du pipeline ETL complet...$(NC)"
	$(VENV_PYTHON) -m etl.orchestrator --mode full

.PHONY: etl-soil
etl-soil: ## Lance uniquement l'extraction des sols
	@echo "$(BLUE) Extraction des données de sol...$(NC)"
	$(VENV_PYTHON) -m etl.orchestrator --mode soil

.PHONY: etl-weather
etl-weather: ## Lance uniquement l'extraction météo
	@echo "$(BLUE)  Extraction des données météo...$(NC)"
	$(VENV_PYTHON) -m etl.orchestrator --mode weather

.PHONY: etl-crop
etl-crop: ## Lance uniquement l'extraction des cultures
	@echo "$(BLUE) Extraction des données de cultures...$(NC)"
	$(VENV_PYTHON) -m etl.orchestrator --mode crop

# =============================================================================
# VÉRIFICATION & DEBUG
# =============================================================================

.PHONY: verify
verify: ## Vérifie que tout fonctionne
	@echo "$(BLUE) Vérification de l'installation...$(NC)"
	@echo "$(YELLOW)1. Python...$(NC)"
	$(VENV_PYTHON) --version
	@echo "$(YELLOW)2. Dépendances...$(NC)"
	$(VENV_PYTHON) -c "import pandas, psycopg2, spacy; print(' Core OK')"
	@echo "$(YELLOW)3. Base de données...$(NC)"
	$(VENV_PYTHON) -c "import sys; sys.path.insert(0, '.'); from etl.utils.database import PostgresManager; from etl.config import ETLConfig; db = PostgresManager(ETLConfig()); result = db.fetch_one('SELECT count(*) as n FROM information_schema.tables WHERE table_schema=\'public\''); print(f' {result[\"n\"]} tables trouvées')"
	@echo "$(YELLOW)4. Docker...$(NC)"
	docker ps --format "table {{.Names}}\t{{.Status}}" | grep $(DB_CONTAINER) && echo " Container OK" || echo " Container non trouvé"
	@echo "$(GREEN) Vérification terminée$(NC)"

.PHONY: status
status: ## Affiche le statut de tous les composants
	@echo "$(BLUE) Statut du système$(NC)"
	@echo ""
	@echo "$(YELLOW)Docker:$(NC)"
	docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}" | grep agroclimate || echo "  Aucun conteneur agroclimate"
	@echo ""
	@echo "$(YELLOW)Virtual Environment:$(NC)"
	test -d $(VENV) && echo "   Venv présent" || echo "   Venv absent"
	@echo ""
	@echo "$(YELLOW)Base de données:$(NC)"
	docker exec $(DB_CONTAINER) psql -U etl_user -d agroclimate -c "SELECT ' Connecté - ' || count(*) || ' tables' FROM information_schema.tables WHERE table_schema='public';" 2>/dev/null || echo "   Non accessible"

# =============================================================================
# NETTOYAGE
# =============================================================================

.PHONY: clean
clean: ## Nettoie les fichiers temporaires
	@echo "$(BLUE) Nettoyage...$(NC)"
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete 2>/dev/null || true
	find . -type d -name "*.egg-info" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name "htmlcov" -exec rm -rf {} + 2>/dev/null || true
	rm -rf .coverage .mypy_cache 2>/dev/null || true
	@echo "$(GREEN) Nettoyage terminé$(NC)"

.PHONY: clean-all
clean-all: clean ## Nettoyage complet + données
	@echo "$(RED)  Suppression des données...$(NC)"
	rm -rf data/processed/* logs/* 2>/dev/null || true
	@echo "$(GREEN) Données supprimées$(NC)"

# =============================================================================
# DÉVELOPPEMENT
# =============================================================================

.PHONY: format
format: ## Formate le code avec black
	@echo "$(BLUE) Formatage du code...$(NC)"
	$(VENV_PYTHON) -m black etl/ tests/
	@echo "$(GREEN) Code formaté$(NC)"

.PHONY: lint
lint: ## Vérifie le code avec ruff et mypy
	@echo "$(BLUE) Linting...$(NC)"
	$(VENV_PYTHON) -m ruff check etl/ tests/
	$(VENV_PYTHON) -m mypy etl/ --ignore-missing-imports
	@echo "$(GREEN) Linting terminé$(NC)"

.PHONY: notebook
notebook: ## Lance Jupyter Lab
	@echo "$(BLUE) Démarrage de Jupyter...$(NC)"
	$(VENV_PYTHON) -m jupyter lab --ip=0.0.0.0 --port=8888 --no-browser

# =============================================================================
# PRODUCTION (FUTUR)
# =============================================================================

.PHONY: build
build: ## Build l'image Docker ETL
	@echo "$(BLUE) Build de l'image ETL...$(NC)"
	$(COMPOSE) build etl

.PHONY: up
up: ## Démarre tous les services (prod mode)
	@echo "$(BLUE) Démarrage de tous les services...$(NC)"
	$(COMPOSE) up -d

.PHONY: down
down: ## Arrête tous les services
	@echo "$(YELLOW) Arrêt de tous les services...$(NC)"
	$(COMPOSE) down