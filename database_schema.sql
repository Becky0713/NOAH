-- NYC Housing Hub Database Schema
-- PostgreSQL + PostGIS for geospatial data

-- Enable PostGIS extension
CREATE EXTENSION IF NOT EXISTS postgis;

-- Main housing projects table
CREATE TABLE housing_projects (
    id SERIAL PRIMARY KEY,
    project_id VARCHAR(50) UNIQUE NOT NULL,
    project_name TEXT,
    building_id VARCHAR(50),
    house_number VARCHAR(20),
    street_name VARCHAR(100),
    borough VARCHAR(50),
    postcode VARCHAR(10),
    bbl VARCHAR(20),
    bin VARCHAR(20),
    community_board VARCHAR(20),
    council_district INTEGER,
    census_tract VARCHAR(20),
    neighborhood_tabulation_area VARCHAR(20),
    
    -- Geographic coordinates
    latitude DECIMAL(10, 8),
    longitude DECIMAL(11, 8),
    latitude_internal DECIMAL(10, 8),
    longitude_internal DECIMAL(11, 8),
    geom GEOMETRY(POINT, 4326), -- PostGIS geometry column
    
    -- Project details
    project_start_date DATE,
    project_completion_date DATE,
    building_completion_date DATE,
    reporting_construction_type VARCHAR(50),
    extended_affordability_status VARCHAR(10),
    prevailing_wage_status VARCHAR(50),
    
    -- Unit counts
    extremely_low_income_units INTEGER DEFAULT 0,
    very_low_income_units INTEGER DEFAULT 0,
    low_income_units INTEGER DEFAULT 0,
    moderate_income_units INTEGER DEFAULT 0,
    middle_income_units INTEGER DEFAULT 0,
    other_income_units INTEGER DEFAULT 0,
    
    -- Bedroom counts
    studio_units INTEGER DEFAULT 0,
    _1_br_units INTEGER DEFAULT 0,
    _2_br_units INTEGER DEFAULT 0,
    _3_br_units INTEGER DEFAULT 0,
    _4_br_units INTEGER DEFAULT 0,
    _5_br_units INTEGER DEFAULT 0,
    _6_br_units INTEGER DEFAULT 0,
    unknown_br_units INTEGER DEFAULT 0,
    
    -- Summary counts
    counted_rental_units INTEGER DEFAULT 0,
    counted_homeownership_units INTEGER DEFAULT 0,
    all_counted_units INTEGER DEFAULT 0,
    total_units INTEGER DEFAULT 0,
    
    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    data_source VARCHAR(50) DEFAULT 'socrata'
);

-- Create spatial index for geographic queries
CREATE INDEX idx_housing_projects_geom ON housing_projects USING GIST (geom);

-- Create indexes for common queries
CREATE INDEX idx_housing_projects_borough ON housing_projects (borough);
CREATE INDEX idx_housing_projects_total_units ON housing_projects (total_units);
CREATE INDEX idx_housing_projects_start_date ON housing_projects (project_start_date);
CREATE INDEX idx_housing_projects_created_at ON housing_projects (created_at);

-- Function to update the geometry column when coordinates change
CREATE OR REPLACE FUNCTION update_housing_geometry()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.latitude IS NOT NULL AND NEW.longitude IS NOT NULL THEN
        NEW.geom = ST_SetSRID(ST_MakePoint(NEW.longitude, NEW.latitude), 4326);
    END IF;
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Trigger to automatically update geometry
CREATE TRIGGER trigger_update_housing_geometry
    BEFORE INSERT OR UPDATE ON housing_projects
    FOR EACH ROW
    EXECUTE FUNCTION update_housing_geometry();

-- View for common queries
CREATE VIEW housing_summary AS
SELECT 
    borough,
    COUNT(*) as project_count,
    SUM(total_units) as total_units_sum,
    SUM(all_counted_units) as affordable_units_sum,
    AVG(total_units) as avg_units_per_project,
    MIN(project_start_date) as earliest_start,
    MAX(project_start_date) as latest_start
FROM housing_projects
WHERE project_start_date IS NOT NULL
GROUP BY borough
ORDER BY total_units_sum DESC;
