# Database Setup Guide

This guide explains how to set up the PostgreSQL + PostGIS database for the NYC Housing Hub project.

## Architecture

```
Socrata API → Data Pipeline → PostgreSQL + PostGIS → FastAPI → Streamlit Frontend
```

## Prerequisites

1. **PostgreSQL with PostGIS extension**
2. **Python 3.10+**
3. **Socrata API Token** (optional, for data updates)

## Quick Start

### 1. Using Docker (Recommended)

```bash
# Start PostgreSQL + PostGIS database
docker-compose -f docker-compose.dev.yml up postgres -d

# Initialize database and run migrations
python scripts/init_database.py

# Run data pipeline to populate database
python scripts/run_data_pipeline.py

# Start the full application
docker-compose -f docker-compose.dev.yml up
```

### 2. Using Local PostgreSQL

1. **Install PostgreSQL + PostGIS**
   ```bash
   # macOS
   brew install postgresql postgis
   
   # Ubuntu/Debian
   sudo apt-get install postgresql postgresql-contrib postgis
   ```

2. **Create Database**
   ```bash
   # Connect to PostgreSQL
   psql -U postgres
   
   # Create database
   CREATE DATABASE nyc_housing;
   \q
   ```

3. **Configure Environment**
   ```bash
   # Copy environment template
   cp env.example .env
   
   # Edit .env with your database credentials
   DB_HOST=localhost
   DB_PORT=5432
   DB_USER=postgres
   DB_PASSWORD=your_password
   DB_NAME=nyc_housing
   SOCRATA_APP_TOKEN=your_token_here
   ```

4. **Initialize Database**
   ```bash
   python scripts/init_database.py
   ```

5. **Populate Data**
   ```bash
   python scripts/run_data_pipeline.py
   ```

## Database Schema

### Main Table: `housing_projects`

The main table stores all NYC affordable housing data with the following key features:

- **Primary Key**: `project_id` (from Socrata)
- **Geographic Data**: `latitude`, `longitude`, `geom` (PostGIS geometry)
- **Address**: `house_number`, `street_name`, `borough`, `postcode`
- **Unit Counts**: `total_units`, `all_counted_units`, bedroom breakdowns
- **Income Categories**: Various income-restricted unit counts
- **Timestamps**: `created_at`, `updated_at`

### PostGIS Features

- **Spatial Index**: GIST index on geometry column for fast spatial queries
- **Automatic Geometry Updates**: Trigger updates geometry from lat/lng
- **Coordinate System**: WGS84 (SRID 4326)

### Views

- **`housing_projects_summary`**: Simplified view with computed fields for API responses

## Scripts

### Database Management

- **`scripts/init_database.py`**: Initialize database and run migrations
- **`scripts/run_data_pipeline.py`**: Sync data from Socrata API
- **`scripts/db_client.py`**: Database utilities and statistics

### Usage Examples

```bash
# Initialize database
python scripts/init_database.py

# Run full data sync
python scripts/run_data_pipeline.py

# Check database statistics
python scripts/db_client.py

# Start development environment
docker-compose -f docker-compose.dev.yml up
```

## Data Pipeline

The data pipeline (`backend/data_pipeline.py`) handles:

1. **Data Fetching**: Retrieves data from Socrata API in batches
2. **Data Normalization**: Converts string numbers, dates, and coordinates
3. **Data Validation**: Ensures data integrity before insertion
4. **Upsert Operations**: Updates existing records or inserts new ones
5. **Error Handling**: Robust error handling and logging

### Pipeline Features

- **Batch Processing**: Processes data in configurable batches (default: 1000 records)
- **Incremental Updates**: Uses `ON CONFLICT` for efficient updates
- **Data Cleaning**: Handles missing values and type conversions
- **Logging**: Comprehensive logging for monitoring and debugging

## API Integration

The FastAPI backend can be configured to use the database instead of direct Socrata API calls:

```python
# In backend/config.py
DATA_PROVIDER = "database"  # Use database instead of "socrata"
```

## Monitoring

### Database Statistics

```python
# Get database statistics
python scripts/db_client.py
```

### Log Files

- **Data Pipeline**: `data_pipeline.log`
- **Application**: Check console output

## Troubleshooting

### Common Issues

1. **Connection Refused**
   - Check if PostgreSQL is running
   - Verify connection parameters in `.env`

2. **Permission Denied**
   - Ensure database user has proper permissions
   - Check if database exists

3. **PostGIS Extension Missing**
   - Install PostGIS extension: `CREATE EXTENSION postgis;`

4. **Data Pipeline Fails**
   - Check Socrata API token
   - Verify network connectivity
   - Check logs for specific errors

### Performance Optimization

1. **Spatial Indexes**: Already created for geographic queries
2. **Regular Indexes**: Created for common query patterns
3. **Batch Processing**: Configure batch size based on available memory
4. **Connection Pooling**: Uses asyncpg connection pool

## Production Deployment

For production deployment:

1. **Use managed PostgreSQL service** (AWS RDS, Google Cloud SQL, etc.)
2. **Configure proper security** (SSL, firewall rules)
3. **Set up monitoring** (database metrics, query performance)
4. **Regular backups** (automated backup strategy)
5. **Environment variables** (secure credential management)

## Data Sources

- **Primary**: NYC Open Data - Affordable Housing Production by Building
- **Dataset ID**: `hg8x-zxpr`
- **API**: Socrata Open Data API
- **Update Frequency**: As needed (manual or scheduled)
