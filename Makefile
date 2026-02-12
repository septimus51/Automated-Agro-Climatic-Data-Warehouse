# =============================================================================
# AGRO-CLIMATIC DATA WAREHOUSE - MAKEFILE
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

# Colors
BLUE := \033[36m
GREEN := \033[32m
YELLOW := \033[33m
RED := \033[31m
NC := \033[0m

# =============================================================================
# HELP
# =============================================================================

.PHONY: help
help:
	@echo "$(BLUE)Available commands:$(NC)"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  $(GREEN)%-20s$(NC) %s\n", $$1, $$2}'

# =============================================================================
# INSTALLATION
# =============================================================================

.PHONY: install
install: ## Full installation
	@echo "$(BLUE)Starting installation...$(NC)"
	$(MAKE) venv-create
	$(MAKE) deps-install
	$(MAKE) nlp-models
	$(MAKE) db-start
	$(MAKE) db-wait
	$(MAKE) db-init
	$(MAKE) verify
	@echo "$(GREEN)Installation complete!$(NC)"
	@echo "$(YELLOW)Activate environment: source $(VENV)/bin/activate$(NC)"

.PHONY: uninstall
uninstall: ## Remove everything
	@echo "$(RED)Removing everything...$(NC)"
	$(MAKE) db-stop
	$(MAKE) db-clean
	$(MAKE) venv-remove
	$(MAKE) clean
	@echo "$(GREEN)Uninstall complete$(NC)"

.PHONY: reinstall
reinstall: ## Complete reset and reinstall
	@echo "$(YELLOW)Reinstalling...$(NC)"
	$(MAKE) uninstall
	$(MAKE) install

# =============================================================================
# VIRTUAL ENVIRONMENT
# =============================================================================

.PHONY: venv-create
venv-create:
	@echo "$(BLUE)Creating virtual environment...$(NC)"
	test -d $(VENV) || $(PYTHON) -m venv $(VENV)
	@echo "$(GREEN)Virtual environment created$(NC)"

.PHONY: venv-remove
venv-remove:
	@echo "$(RED)Removing virtual environment...$(NC)"
	rm -rf $(VENV)
	@echo "$(GREEN)Virtual environment removed$(NC)"

# =============================================================================
# DEPENDENCIES
# =============================================================================

.PHONY: deps-install
deps-install:
	@echo "$(BLUE)Installing dependencies...$(NC)"
	$(VENV_PIP) install --upgrade pip
	$(VENV_PIP) install -r requirements.txt
	@echo "$(GREEN)Dependencies installed$(NC)"

.PHONY: nlp-models
nlp-models:
	@echo "$(BLUE)Downloading NLP models...$(NC)"
	$(VENV_PYTHON) -m spacy download en_core_web_sm
	@echo "$(GREEN)NLP models ready$(NC)"

# =============================================================================
# DATABASE
# =============================================================================

.PHONY: db-start
db-start:
	@echo "$(BLUE)Starting PostgreSQL...$(NC)"
	$(COMPOSE) up -d postgres
	@echo "$(GREEN)PostgreSQL container started$(NC)"

.PHONY: db-wait
db-wait: ## Wait for PostgreSQL to be ready
	@echo "$(YELLOW)Waiting for PostgreSQL to be ready...$(NC)"
	@for i in 1 2 3 4 5 6 7 8 9 10; do \
		if docker exec $(DB_CONTAINER) pg_isready -U postgres > /dev/null 2>&1; then \
			echo "$(GREEN)PostgreSQL is ready!$(NC)"; \
			exit 0; \
		fi; \
		echo "Attempt $$i/10... waiting 3s"; \
		sleep 3; \
	done; \
	echo "$(RED)PostgreSQL failed to start$(NC)"; \
	exit 1

.PHONY: db-init
db-init:
	@echo "$(BLUE)Initializing database schema...$(NC)"
	docker exec $(DB_CONTAINER) psql -U postgres -c "CREATE USER etl_user WITH PASSWORD 'etl_password' SUPERUSER;" 2>/dev/null || echo "User already exists"
	docker exec $(DB_CONTAINER) psql -U postgres -c "CREATE DATABASE agroclimate OWNER etl_user;" 2>/dev/null || echo "Database already exists"
	docker cp db/init/01-schema.sql $(DB_CONTAINER):/tmp/
	docker exec $(DB_CONTAINER) psql -U etl_user -d agroclimate -f /tmp/01-schema.sql
	@echo "$(GREEN)Schema created$(NC)"

.PHONY: db-stop
db-stop:
	@echo "$(YELLOW)Stopping PostgreSQL...$(NC)"
	$(COMPOSE) stop postgres || true
	@echo "$(GREEN)PostgreSQL stopped$(NC)"

.PHONY: db-clean
db-clean:
	@echo "$(RED)Removing containers and volumes...$(NC)"
	$(COMPOSE) down -v || true
	docker rm -f $(DB_CONTAINER) 2>/dev/null || true
	@echo "$(GREEN)Containers and volumes removed$(NC)"

.PHONY: db-shell
db-shell:
	docker exec -it $(DB_CONTAINER) psql -U etl_user -d agroclimate

# =============================================================================
# TESTS
# =============================================================================

.PHONY: test
test:
	@echo "$(BLUE)Running tests...$(NC)"
	$(VENV_PYTHON) -m pytest tests/ -v --tb=short

.PHONY: verify
verify:
	@echo "$(BLUE)Verifying installation...$(NC)"
	$(VENV_PYTHON) --version
	$(VENV_PYTHON) -c "import pandas, psycopg2, spacy; print('Core dependencies OK')"
	docker ps --format "table {{.Names}}\t{{.Status}}" | grep $(DB_CONTAINER) && echo "Container OK" || echo "Container not found"

# =============================================================================
# ETL
# =============================================================================

.PHONY: etl-full
etl-full:
	$(VENV_PYTHON) -m etl.orchestrator --mode full

.PHONY: etl-soil
etl-soil:
	$(VENV_PYTHON) -m etl.orchestrator --mode soil

.PHONY: etl-weather
etl-weather:
	$(VENV_PYTHON) -m etl.orchestrator --mode weather

.PHONY: etl-crop
etl-crop:
	$(VENV_PYTHON) -m etl.orchestrator --mode crop

# =============================================================================
# CLEANING
# =============================================================================

.PHONY: clean
clean:
	@echo "$(BLUE)Cleaning temporary files...$(NC)"
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete 2>/dev/null || true
	find . -type d -name "*.egg-info" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true
	rm -rf .coverage .mypy_cache 2>/dev/null || true
	@echo "$(GREEN)Cleaning complete$(NC)"

# ============================================================================
# WORKFLOW TESTING TARGETS
# ============================================================================

.PHONY: test-workflows test-prereqs test-quality test-audit

test-workflows:
	venv/bin/python -m pytest tests/test_workflows.py -v --tb=short -x

test-prereqs:
	venv/bin/python -m pytest tests/test_workflows.py::TestPrerequisites -v

test-quality:
	venv/bin/python -m pytest tests/test_workflows.py::TestDataQualityRules -v

test-audit:
	venv/bin/python -m pytest tests/test_workflows.py::TestETLAudit -v

# Quick validation without running ETL
validate-data: test-prereqs test-quality test-audit
	@echo " Data validation complete"