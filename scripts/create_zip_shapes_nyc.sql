-- Create NYC-only ZIP shapes table
-- This table contains only ZIP codes within New York City (10000-11699)
-- It filters the full zip_shapes_geojson table to only include NYC ZIP codes

-- Drop table if exists (for re-creation)
DROP TABLE IF EXISTS zip_shapes_nyc;

-- Create table with NYC ZIP codes only
CREATE TABLE zip_shapes_nyc AS
SELECT 
    zip_code,
    geojson
FROM zip_shapes_geojson
WHERE zip_code IS NOT NULL 
  AND geojson IS NOT NULL
  AND (
    -- NYC ZIP codes: 10000-11699
    (zip_code >= '10000' AND zip_code <= '11699')
    OR
    -- Also handle 5-digit numeric format
    (CAST(zip_code AS TEXT) ~ '^(10[0-9]{3}|11[0-6][0-9]{2})$')
  );

-- Add index for faster lookups
CREATE INDEX IF NOT EXISTS idx_zip_shapes_nyc_zip_code ON zip_shapes_nyc(zip_code);

-- Add comment
COMMENT ON TABLE zip_shapes_nyc IS 'NYC-only ZIP code shapes filtered from zip_shapes_geojson. Contains ZIP codes 10000-11699.';

