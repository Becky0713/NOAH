-- Migration: Create housing_projects table with PostGIS support
-- This creates the main table for storing NYC affordable housing data

-- Enable PostGIS extension if not already enabled
CREATE EXTENSION IF NOT EXISTS postgis;

-- Create the main housing projects table
CREATE TABLE IF NOT EXISTS housing_projects (
    -- Primary key
    project_id VARCHAR(50) PRIMARY KEY,
    
    -- Basic project information
    project_name TEXT,
    building_id VARCHAR(50),
    
    -- Address information
    house_number VARCHAR(20),
    street_name VARCHAR(100),
    borough VARCHAR(50),
    postcode VARCHAR(10),
    bbl VARCHAR(20),
    bin VARCHAR(20),
    
    -- Administrative divisions
    community_board VARCHAR(50),
    council_district INTEGER,
    census_tract VARCHAR(20),
    neighborhood_tabulation_area VARCHAR(20),
    
    -- Geographic coordinates (WGS84)
    latitude DECIMAL(10, 8),
    longitude DECIMAL(11, 8),
    latitude_internal DECIMAL(10, 8),
    longitude_internal DECIMAL(11, 8),
    
    -- PostGIS geometry column for spatial queries
    geom GEOMETRY(POINT, 4326),
    
    -- Project dates
    project_start_date DATE,
    project_completion_date DATE,
    building_completion_date DATE,
    
    -- Project characteristics
    reporting_construction_type VARCHAR(100),
    extended_affordability_status VARCHAR(100),
    prevailing_wage_status VARCHAR(100),
    
    -- Income-restricted unit counts
    extremely_low_income_units INTEGER DEFAULT 0,
    very_low_income_units INTEGER DEFAULT 0,
    low_income_units INTEGER DEFAULT 0,
    moderate_income_units INTEGER DEFAULT 0,
    middle_income_units INTEGER DEFAULT 0,
    other_income_units INTEGER DEFAULT 0,
    
    -- Bedroom unit counts
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
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Create spatial index for geographic queries
CREATE INDEX IF NOT EXISTS idx_housing_projects_geom 
ON housing_projects USING GIST (geom);

-- Create indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_housing_projects_borough 
ON housing_projects (borough);

CREATE INDEX IF NOT EXISTS idx_housing_projects_total_units 
ON housing_projects (total_units);

CREATE INDEX IF NOT EXISTS idx_housing_projects_project_start_date 
ON housing_projects (project_start_date);

CREATE INDEX IF NOT EXISTS idx_housing_projects_project_completion_date 
ON housing_projects (project_completion_date);

-- Create function to automatically update the geometry column
CREATE OR REPLACE FUNCTION update_housing_projects_geom()
RETURNS TRIGGER AS $$
BEGIN
    -- Update geometry from latitude/longitude
    IF NEW.latitude IS NOT NULL AND NEW.longitude IS NOT NULL THEN
        NEW.geom = ST_SetSRID(ST_MakePoint(NEW.longitude, NEW.latitude), 4326);
    END IF;
    
    -- Update timestamp
    NEW.updated_at = CURRENT_TIMESTAMP;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create trigger to automatically update geometry and timestamp
DROP TRIGGER IF EXISTS trigger_update_housing_projects_geom ON housing_projects;
CREATE TRIGGER trigger_update_housing_projects_geom
    BEFORE INSERT OR UPDATE ON housing_projects
    FOR EACH ROW
    EXECUTE FUNCTION update_housing_projects_geom();

-- Create view for common queries (with computed fields)
CREATE OR REPLACE VIEW housing_projects_summary AS
SELECT 
    project_id,
    project_name,
    building_id,
    CONCAT(house_number, ' ', street_name) as address,
    borough,
    postcode,
    latitude,
    longitude,
    geom,
    project_start_date,
    project_completion_date,
    total_units,
    all_counted_units as affordable_units,
    extremely_low_income_units,
    very_low_income_units,
    low_income_units,
    moderate_income_units,
    middle_income_units,
    studio_units,
    _1_br_units,
    _2_br_units,
    _3_br_units,
    _4_br_units,
    _5_br_units,
    _6_br_units,
    created_at,
    updated_at
FROM housing_projects;

-- Add comments for documentation
COMMENT ON TABLE housing_projects IS 'NYC Affordable Housing Production by Building data from Socrata';
COMMENT ON COLUMN housing_projects.geom IS 'PostGIS geometry point in WGS84 (SRID 4326)';
COMMENT ON COLUMN housing_projects.project_id IS 'Unique project identifier from Socrata';
COMMENT ON COLUMN housing_projects.all_counted_units IS 'Total affordable units (income-restricted)';
COMMENT ON COLUMN housing_projects.total_units IS 'Total units in the building';
