# scripts/setup.sh
#!/bin/bash
set -e

echo "Setting up Agro-Climate Warehouse..."

# Create directories
mkdir -p data/raw data/processed logs

# Install Python dependencies
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
python -m spacy download en_core_web_sm

# Setup database
docker-compose up -d postgres
sleep 10  # Wait for PostgreSQL

# Run migrations
docker-compose exec postgres psql -U etl_user -d agroclimate -f /migrations/V2__add_indices.sql

echo "Setup complete. Run 'docker-compose up etl' to start ETL."