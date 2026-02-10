#!/bin/bash
set -e

echo "🌱 Setting up Agro-Climate Warehouse..."

# =============================================================================
# 1. CRÉER LES DOSSIERS
# =============================================================================
echo "📁 Creating directories..."
mkdir -p data/raw data/processed logs

# =============================================================================
# 2. PYTHON VENV ET DÉPENDANCES
# =============================================================================
echo "🐍 Setting up Python environment..."

# Créer venv si inexistant
if [ ! -d "venv" ]; then
    python3 -m venv venv
    echo "✅ Virtual environment created"
fi

# Activer venv
source venv/bin/activate

# Mettre à jour pip et installer dépendances
pip install --upgrade pip
pip install -r requirements.txt

# Télécharger modèle spaCy
python -m spacy download en_core_web_sm

echo "✅ Python dependencies installed"

# =============================================================================
# 3. DÉMARRER POSTGRESQL (S'IL NE TOURNE PAS DÉJÀ)
# =============================================================================
echo "🐳 Starting PostgreSQL..."

if ! docker ps | grep -q "agroclimate_db"; then
    docker-compose up -d postgres
    echo "⏳ Waiting for PostgreSQL to start..."
    sleep 10
else
    echo "✅ PostgreSQL already running"
fi

# =============================================================================
# 4. CRÉER LA BASE ET L'UTILISATEUR (SI NÉCESSAIRE)
# =============================================================================
echo "👤 Setting up database user..."

docker exec agroclimate_db psql -U postgres -c "
    DO \$\$
    BEGIN
        IF NOT EXISTS (SELECT FROM pg_user WHERE usename = 'etl_user') THEN
            CREATE USER etl_user WITH PASSWORD 'etl_password' SUPERUSER;
        END IF;
    END
    \$\$;
" 2>/dev/null || true

docker exec agroclimate_db psql -U postgres -c "
    SELECT 'CREATE DATABASE agroclimate OWNER etl_user'
    WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'agroclimate')\gexec
" 2>/dev/null || true

# =============================================================================
# 5. CRÉER LE SCHÉMA
# =============================================================================
echo "📜 Creating database schema..."

# Copier le schéma dans le conteneur
docker cp db/init/01-schema.sql agroclimate_db:/tmp/

# Exécuter le schéma
docker exec agroclimate_db psql -U etl_user -d agroclimate -f /tmp/01-schema.sql

echo "✅ Schema created"

# =============================================================================
# 6. VÉRIFICATION FINALE
# =============================================================================
echo "🧪 Testing connection..."

python -c "
import sys
sys.path.insert(0, '.')
from etl.utils.database import PostgresManager
from etl.config import ETLConfig

db = PostgresManager(ETLConfig())
result = db.fetch_one('SELECT count(*) as n FROM information_schema.tables WHERE table_schema=\'public\'')
print(f'✅ {result[\"n\"]} tables ready')
tables = db.fetch_many(\"SELECT table_name FROM information_schema.tables WHERE table_schema='public' ORDER BY table_name\")
for t in tables:
    print(f'   - {t[\"table_name\"]}')
"

echo ""
echo "🎉 Setup complete!"
echo ""
echo "Next steps:"
echo "  1. Activate venv: source venv/bin/activate"
echo "  2. Run tests: pytest tests/ -v"
echo "  3. Run ETL: python -m etl.orchestrator --mode full"