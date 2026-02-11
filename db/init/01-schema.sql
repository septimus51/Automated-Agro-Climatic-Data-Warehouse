-- db/init/01-schema.sql

-- Enable PostGIS for geospatial queries
CREATE EXTENSION IF NOT EXISTS postgis;

-- ==========================================
-- DIMENSION TABLES
-- ==========================================

-- Dim Location: Central geographic dimension
-- Design: Slowly Changing Dimension Type 2 (SCD2) ready with hashdiff
CREATE TABLE dim_location (
    location_key SERIAL PRIMARY KEY,
    latitude DECIMAL(10, 8) NOT NULL,
    longitude DECIMAL(11, 8) NOT NULL,
    country_code VARCHAR(2),
    country_name VARCHAR(100),
    admin_region VARCHAR(100),
    climate_zone VARCHAR(50),
    elevation_meters INT,
    location_hash VARCHAR(32) UNIQUE NOT NULL, -- MD5 of lat,long
    effective_date DATE NOT NULL DEFAULT CURRENT_DATE,
    expiration_date DATE DEFAULT '9999-12-31',
    is_current BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_location_coords ON dim_location(latitude, longitude);
CREATE INDEX idx_location_current ON dim_location(is_current) WHERE is_current = TRUE;

-- Dim Soil: Soil characteristics dimension
-- Design: Type 1 SCD (soil properties relatively static)
CREATE TABLE dim_soil (
    soil_key SERIAL PRIMARY KEY,
    location_key INT REFERENCES dim_location(location_key),
    soil_source VARCHAR(50) DEFAULT 'SoilGrids',
    soil_texture VARCHAR(20),
    clay_content_0_5cm DECIMAL(5,2),
    sand_content_0_5cm DECIMAL(5,2),
    silt_content_0_5cm DECIMAL(5,2),
    ph_level_0_5cm DECIMAL(4,2),
    organic_carbon_0_5cm DECIMAL(6,3),
    bulk_density_0_5cm DECIMAL(5,3),
    water_capacity_0_5cm DECIMAL(5,2),
    soil_depth_cm INT,
    extraction_date DATE NOT NULL,
    metadata JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    CONSTRAINT unique_soil_location_date UNIQUE (location_key, extraction_date)
);

CREATE INDEX idx_soil_location ON dim_soil(location_key);
CREATE INDEX idx_soil_ph ON dim_soil(ph_level_0_5cm);

-- Dim Crop: Crop types and their requirements
-- Design: Type 1 SCD with versioning for requirement updates
CREATE TABLE dim_crop (
    crop_key SERIAL PRIMARY KEY,
    crop_name VARCHAR(100) NOT NULL,
    crop_family VARCHAR(50),
    crop_variety VARCHAR(100),
    optimal_temp_min_c DECIMAL(4,1),
    optimal_temp_max_c DECIMAL(4,1),
    absolute_temp_min_c DECIMAL(4,1),
    absolute_temp_max_c DECIMAL(4,1),
    water_requirement_mm_day DECIMAL(4,2),
    water_requirement_source VARCHAR(200),
    sunlight_hours_min DECIMAL(3,1),
    sunlight_hours_max DECIMAL(3,1),
    growing_period_days INT,
    soil_ph_preference_min DECIMAL(4,2),
    soil_ph_preference_max DECIMAL(4,2),
    nitrogen_requirement_kg_ha DECIMAL(6,2),
    extraction_confidence DECIMAL(3,2), -- NLP confidence score
    source_urls TEXT[], -- Array of sources
    extraction_date DATE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_crop_name ON dim_crop(crop_name);
CREATE INDEX idx_crop_temp ON dim_crop(optimal_temp_min_c, optimal_temp_max_c);
-- Unique constraint for ON CONFLICT support
ALTER TABLE dim_crop 
ADD CONSTRAINT unique_crop_name UNIQUE (crop_name);
-- Dim Date: Standard date dimension
CREATE TABLE dim_date (
    date_key INT PRIMARY KEY, -- YYYYMMDD format
    full_date DATE NOT NULL UNIQUE,
    day_of_week INT,
    day_name VARCHAR(10),
    day_of_month INT,
    day_of_year INT,
    week_of_year INT,
    month_number INT,
    month_name VARCHAR(10),
    quarter INT,
    year INT,
    is_weekend BOOLEAN,
    season_northern VARCHAR(10),
    season_southern VARCHAR(10),
    is_growing_season BOOLEAN DEFAULT FALSE
);

-- Populate date dimension
INSERT INTO dim_date
SELECT 
    TO_CHAR(d, 'YYYYMMDD')::INT as date_key,
    d as full_date,
    EXTRACT(DOW FROM d) as day_of_week,
    TO_CHAR(d, 'Day') as day_name,
    EXTRACT(DAY FROM d) as day_of_month,
    EXTRACT(DOY FROM d) as day_of_year,
    EXTRACT(WEEK FROM d) as week_of_year,
    EXTRACT(MONTH FROM d) as month_number,
    TO_CHAR(d, 'Month') as month_name,
    EXTRACT(QUARTER FROM d) as quarter,
    EXTRACT(YEAR FROM d) as year,
    EXTRACT(DOW FROM d) IN (0, 6) as is_weekend,
    CASE 
        WHEN EXTRACT(MONTH FROM d) IN (3,4,5) THEN 'Spring'
        WHEN EXTRACT(MONTH FROM d) IN (6,7,8) THEN 'Summer'
        WHEN EXTRACT(MONTH FROM d) IN (9,10,11) THEN 'Autumn'
        ELSE 'Winter'
    END as season_northern,
    CASE 
        WHEN EXTRACT(MONTH FROM d) IN (9,10,11) THEN 'Spring'
        WHEN EXTRACT(MONTH FROM d) IN (12,1,2) THEN 'Summer'
        WHEN EXTRACT(MONTH FROM d) IN (3,4,5) THEN 'Autumn'
        ELSE 'Winter'
    END as season_southern
FROM generate_series('2020-01-01'::DATE, '2030-12-31'::DATE, '1 day'::INTERVAL) d;

-- ==========================================
-- FACT TABLES
-- ==========================================

-- Fact Weather: Daily weather measurements
-- Design: Partitioned by date for performance
CREATE TABLE fact_weather (
    weather_key BIGSERIAL,
    location_key INT NOT NULL REFERENCES dim_location(location_key),
    date_key INT NOT NULL REFERENCES dim_date(date_key),
    latitude DECIMAL(10, 8) NOT NULL,
    longitude DECIMAL(11, 8) NOT NULL,
    temp_max_c DECIMAL(4,1),
    temp_min_c DECIMAL(4,1),
    temp_mean_c DECIMAL(4,1),
    precipitation_mm DECIMAL(5,2),
    evapotranspiration_mm DECIMAL(5,2),
    solar_radiation_mj_m2 DECIMAL(6,2),
    humidity_percent DECIMAL(5,2),
    wind_speed_ms DECIMAL(4,1),
    weather_code INT, -- WMO weather code
    data_source VARCHAR(50) DEFAULT 'Open-Meteo',
    batch_id VARCHAR(50), -- For idempotency
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (date_key, location_key)
) PARTITION BY RANGE (date_key);
-- Function to create partitions automatically
CREATE OR REPLACE FUNCTION create_weather_partitions(year INT)
RETURNS void AS $$
DECLARE
    start_date DATE;
    end_date DATE;
    partition_name TEXT;
BEGIN
    FOR i IN 1..12 LOOP
        start_date := make_date(year, i, 1);
        end_date := start_date + INTERVAL '1 month';
        partition_name := 'fact_weather_' || year || '_' || LPAD(i::TEXT, 2, '0');
        
        EXECUTE format('CREATE TABLE IF NOT EXISTS %I PARTITION OF fact_weather FOR VALUES FROM (%L) TO (%L)', 
                      partition_name, start_date, end_date);
    END LOOP;
END;
$$ LANGUAGE plpgsql;

-- Create partitions for current year and next 2 years
SELECT create_weather_partitions(2024);
SELECT create_weather_partitions(2025);
SELECT create_weather_partitions(2026);

CREATE INDEX idx_weather_location_date ON fact_weather(location_key, date_key);
CREATE INDEX idx_weather_temp ON fact_weather(temp_mean_c);

-- Fact Soil: Point-in-time soil measurements
CREATE TABLE fact_soil (
    soil_fact_key BIGSERIAL PRIMARY KEY,
    location_key INT NOT NULL REFERENCES dim_location(location_key),
    soil_key INT NOT NULL REFERENCES dim_soil(soil_key),
    measurement_date DATE NOT NULL,
    moisture_percent DECIMAL(5,2),
    temperature_c DECIMAL(4,1),
    ph_measured DECIMAL(4,2),
    batch_id VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_fact_soil_location ON fact_soil(location_key);
CREATE INDEX idx_fact_soil_date ON fact_soil(measurement_date);

-- Fact Crop Suitability: Analysis results linking crops to locations
CREATE TABLE fact_crop_suitability (
    suitability_key BIGSERIAL PRIMARY KEY,
    location_key INT NOT NULL REFERENCES dim_location(location_key),
    crop_key INT NOT NULL REFERENCES dim_crop(crop_key),
    soil_key INT REFERENCES dim_soil(soil_key),
    analysis_date DATE NOT NULL,
    temp_match_score DECIMAL(3,2), -- 0-1 score
    water_adequacy_score DECIMAL(3,2),
    soil_ph_match_score DECIMAL(3,2),
    overall_suitability_score DECIMAL(3,2),
    limiting_factor VARCHAR(100),
    recommendation_text TEXT,
    batch_id VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_suitability_location ON fact_crop_suitability(location_key);
CREATE INDEX idx_suitability_crop ON fact_crop_suitability(crop_key);
CREATE INDEX idx_suitability_score ON fact_crop_suitability(overall_suitability_score);

-- ==========================================
-- AUDIT AND CONTROL TABLES
-- ==========================================

CREATE TABLE etl_audit_log (
    audit_id SERIAL PRIMARY KEY,
    batch_id VARCHAR(50) UNIQUE NOT NULL,
    pipeline_name VARCHAR(100) NOT NULL,
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP,
    status VARCHAR(20) NOT NULL, -- RUNNING, SUCCESS, FAILED
    records_processed INT,
    records_inserted INT,
    records_updated INT,
    records_failed INT,
    error_message TEXT,
    execution_metadata JSONB
);

CREATE INDEX idx_audit_batch ON etl_audit_log(batch_id);
CREATE INDEX idx_audit_time ON etl_audit_log(start_time);

-- Idempotency control
CREATE TABLE etl_idempotency_keys (
    key_hash VARCHAR(64) PRIMARY KEY,
    entity_type VARCHAR(50) NOT NULL, -- 'weather', 'soil', 'crop'
    entity_key VARCHAR(100) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Materialized view for common analytical queries
CREATE MATERIALIZED VIEW mv_location_crop_compatibility AS
SELECT 
    l.location_key,
    l.latitude,
    l.longitude,
    c.crop_name,
    c.optimal_temp_min_c,
    c.optimal_temp_max_c,
    s.ph_level_0_5cm,
    CASE 
        WHEN s.ph_level_0_5cm BETWEEN c.soil_ph_preference_min AND c.soil_ph_preference_max 
        THEN 'Compatible' ELSE 'Incompatible' 
    END as ph_compatibility
FROM dim_location l
JOIN dim_soil s ON l.location_key = s.location_key
CROSS JOIN dim_crop c
WHERE l.is_current = TRUE;

CREATE UNIQUE INDEX idx_mv_compatibility ON mv_location_crop_compatibility(location_key, crop_name);