# Automated-Agro-Climatic-Data-Warehouse

[![Python](https://img.shields.io/badge/python-3.10%2B-blue)](https://www.python.org/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-blue)](https://www.postgresql.org/)
[![Docker](https://img.shields.io/badge/Docker-enabled-2496ED)](https://www.docker.com/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

Automated ETL pipeline combining soil data, weather patterns, and crop requirements for agricultural analysis.

---

## Table of Contents

- [Quick Start](#quick-start)
- [Architecture](#architecture)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Usage](#usage)
- [Project Structure](#project-structure)
- [Data Sources](#data-sources)
- [Database Schema](#database-schema)
- [Development](#development)
- [Testing](#testing)
- [Troubleshooting](#troubleshooting)
- [Roadmap](#roadmap)

---

## Quick Start

```bash
# 1. Clone the repository
git clone https://github.com/your-username/agro-climate-warehouse.git
cd agro-climate-warehouse

# 2. Install everything
make install

# 3. Verify installation
make verify

# 4. Run ETL pipeline
make etl-full
```
## Architecture

Data Sources:
- SoilGrids API (soil properties)
- Open-Meteo API (weather data)
- Agricultural websites (crop requirements via NLP)

        |
        v
ETL Pipeline:
- Extract: API clients + web scraper
- Transform: Data cleaning + NLP extraction
- Load: PostgreSQL with Star Schema

        |
        v
Data Warehouse:
- PostgreSQL 15 + PostGIS
- Star Schema (dimensions + facts)
- Partitioned tables for time-series

## Prerequisites

| Tool           | Version | Install Command                                   |
| -------------- | ------- | ------------------------------------------------- |
| Python         | 3.10+   | `sudo apt install python3 python3-venv`           |
| Docker         | 20.10+  | [Get Docker](https://docs.docker.com/get-docker/) |
| Docker Compose | 2.0+    | Included with Docker                              |
| Make           | 4.0+    | `sudo apt install make`                           |

## Installation

```bash
make install
```

## Usage
### Main Make Commands

| Command          | Description                                |
| ---------------- | ------------------------------------------ |
| `make help`      | Show all available commands                |
| `make install`   | Full installation                          |
| `make uninstall` | Remove everything (venv, data, containers) |
| `make reinstall` | Complete reset and reinstall               |
| `make status`    | Check system status                        |

### Running ETL Pipeline

#### Full pipeline
```bash
make etl-full
```
#### Individual components
```bash
make etl-soil      # Soil data only
make etl-weather   # Weather data only
make etl-crop      # Crop requirements only
```
### Database Access
```bash
make db-shell
```
## Project Structure

agro-climate-warehouse/
├── Makefile                    # Main project commands
├── docker-compose.yml          # Docker services configuration
├── Dockerfile                  # ETL Python image
├── requirements.txt            # Python dependencies
├── .env.example               # Configuration template
│
├── db/
│   └── init/
│       └── 01-schema.sql      # PostgreSQL schema (Star Schema)
│
├── etl/                       # Main ETL package
│   ├── __init__.py
│   ├── config.py              # Centralized configuration
│   ├── orchestrator.py        # Pipeline orchestrator
│   │
│   ├── extract/               # Extraction module
│   │   ├── __init__.py
│   │   ├── soil_api.py        # SoilGrids API client
│   │   ├── weather_api.py     # Open-Meteo API client
│   │   └── web_scraper.py     # FAO/USDA web scraper
│   │
│   ├── transform/             # Transformation module
│   │   ├── __init__.py
│   │   ├── cleaners.py        # Data/text cleaning
│   │   ├── nlp_extractor.py   # NLP entity extraction
│   │   └── transformers.py    # DWH schema mapping
│   │
│   ├── load/                  # Loading module
│   │   ├── __init__.py
│   │   └── postgres_loader.py # Idempotent PostgreSQL loader
│   │
│   └── utils/                 # Utilities module
│       ├── __init__.py
│       ├── logger.py          # Structured logging
│       ├── database.py        # PostgreSQL connection manager
│       └── validators.py      # Geo/data validators
│
├── tests/                     # Test suite
│   ├── __init__.py
│   ├── conftest.py            # Pytest fixtures
│   ├── test_extractors.py     # Extraction tests
│   └── test_transformers.py   # Transformation tests
│
├── data/                      # Data directory (gitignored)
│   ├── raw/                   # Downloaded raw data
│   └── processed/             # Transformed data
│
└── logs/                      # Application logs

## Data Source

| Source                                               | Type         | Data                                           | Cost | Rate Limit         |
| ---------------------------------------------------- | ------------ | ---------------------------------------------- | ---- | ------------------ |
| [SoilGrids](https://www.isric.org/explore/soilgrids) | API          | Soil texture, pH, organic carbon               | Free | 1 req/sec          |
| [Open-Meteo](https://open-meteo.com/)                | API          | Temperature, precipitation, evapotranspiration | Free | 10k req/day        |
| [FAO](https://www.fao.org/)                          | Web Scraping | Crop requirements                              | Free | Respect robots.txt |
| [USDA](https://plants.usda.gov/)                     | Web Scraping | Crop characteristics                           | Free | Educational use    |

## Database Schema

### Star Schema Design
#### Dimension Tables:
dim_location - Geographic coordinates (SCD Type 2)
dim_soil - Soil properties (SCD Type 1)
dim_crop - Crop characteristics with NLP extraction
dim_date - Standard date dimension
#### Fact Tables:
fact_weather - Daily weather measurements (partitioned by month)
fact_soil - Point-in-time soil measurements
fact_crop_suitability - Crop-location compatibility analysis
#### Audit Tables:
etl_audit_log - Pipeline execution tracking
etl_idempotency_keys - Duplicate prevention
#### Technical Features
PostGIS for geospatial indexing
Table partitioning for time-series data
SCD Type 2 for location history tracking
JSONB for flexible metadata storage

## Development

### Environment Setup
#### Copy and edit configuration
```bash
cp .env.example .env
```
#### Edit .env with your parameters
```bash
# Database
DB_HOST=localhost
DB_PORT=5432
DB_NAME=agroclimate
DB_USER=etl_user
DB_PASSWORD=etl_password

# API Configuration
API_RATE_LIMIT=1.0           # Requests per second

# ETL Settings
LOG_LEVEL=INFO               # DEBUG, INFO, WARNING, ERROR
ETL_BATCH_SIZE=1000          # INSERT batch size
```
### Activate Environment
```bash
source venv/bin/activate
```
## Testing

```bash
# Run all tests
make test

# Unit tests only (no database)
make test-unit

# Tests with coverage report
make test-cov

# Quick tests (skip slow ones)
make test-quick
```