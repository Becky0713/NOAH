"""
Check completion dates in database to see if we have data for:
- 10+ years old (before 2015)
- 20+ years old (before 2005)
- 50+ years old (before 1985)
"""

import psycopg2
import pandas as pd
from datetime import datetime
import os
from dotenv import load_dotenv
import sys
import argparse

# Load environment variables
load_dotenv()

def get_db_connection():
    """Get database connection - try multiple sources"""
    # Try environment variables first
    db_host = os.getenv("DB_HOST")
    db_port = os.getenv("DB_PORT", "5432")
    db_name = os.getenv("DB_NAME")
    db_user = os.getenv("DB_USER")
    db_password = os.getenv("DB_PASSWORD")
    
    # If not found, try reading from Streamlit secrets.toml format
    if not db_host:
        try:
            import toml
            secrets_path = os.path.join(os.path.dirname(__file__), "..", ".streamlit", "secrets.toml")
            if os.path.exists(secrets_path):
                with open(secrets_path, 'r') as f:
                    secrets = toml.load(f)
                    if "secrets" in secrets:
                        db_host = secrets["secrets"].get("db_host")
                        db_port = secrets["secrets"].get("db_port", "5432")
                        db_name = secrets["secrets"].get("db_name")
                        db_user = secrets["secrets"].get("db_user")
                        db_password = secrets["secrets"].get("db_password")
        except Exception as e:
            print(f"Note: Could not read from secrets.toml: {e}")
            pass
    
    # Try command line arguments
    parser = argparse.ArgumentParser(description='Check completion dates in database')
    parser.add_argument('--db-host', help='Database host')
    parser.add_argument('--db-port', help='Database port', default='5432')
    parser.add_argument('--db-name', help='Database name')
    parser.add_argument('--db-user', help='Database user')
    parser.add_argument('--db-password', help='Database password')
    args, unknown = parser.parse_known_args()
    
    if args.db_host:
        db_host = args.db_host
        db_port = args.db_port or db_port
        db_name = args.db_name or db_name
        db_user = args.db_user or db_user
        db_password = args.db_password or db_password
    
    if not db_host:
        print("=" * 60)
        print("ERROR: Database credentials not found!")
        print("=" * 60)
        print("Please provide database credentials in one of these ways:")
        print("1. Set environment variables: DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD")
        print("2. Create .env file with these variables")
        print("3. Use command line arguments: --db-host, --db-port, --db-name, --db-user, --db-password")
        print()
        print("Example:")
        print("  python check_completion_dates.py --db-host HOST --db-name DBNAME --db-user USER --db-password PASS")
        sys.exit(1)
    
    try:
        return psycopg2.connect(
            host=db_host,
            port=int(db_port),
            dbname=db_name,
            user=db_user,
            password=db_password,
            sslmode="require"
        )
    except Exception as e:
        print(f"Error connecting to database: {e}")
        sys.exit(1)

def check_completion_dates():
    """Check completion dates in database"""
    conn = get_db_connection()
    
    # Current year
    current_year = 2025
    
    # Calculate cutoff dates
    date_10_years = f"{current_year - 10}-01-01"  # 2015-01-01
    date_20_years = f"{current_year - 20}-01-01"  # 2005-01-01
    date_50_years = f"{current_year - 50}-01-01"  # 1985-01-01
    
    print(f"Checking completion dates (as of {current_year}):")
    print(f"  10+ years old: before {date_10_years}")
    print(f"  20+ years old: before {date_20_years}")
    print(f"  50+ years old: before {date_50_years}")
    print()
    
    # Query to check data availability
    query = """
    SELECT 
        COUNT(*) as total_records,
        COUNT(building_completion_date) as has_building_date,
        COUNT(project_completion_date) as has_project_date,
        COUNT(CASE WHEN building_completion_date IS NOT NULL OR project_completion_date IS NOT NULL THEN 1 END) as has_any_date,
        
        -- 10+ years old
        COUNT(CASE 
            WHEN (building_completion_date IS NOT NULL AND building_completion_date < %s)
               OR (building_completion_date IS NULL AND project_completion_date IS NOT NULL AND project_completion_date < %s)
            THEN 1 
        END) as count_10_years,
        
        -- 20+ years old
        COUNT(CASE 
            WHEN (building_completion_date IS NOT NULL AND building_completion_date < %s)
               OR (building_completion_date IS NULL AND project_completion_date IS NOT NULL AND project_completion_date < %s)
            THEN 1 
        END) as count_20_years,
        
        -- 50+ years old
        COUNT(CASE 
            WHEN (building_completion_date IS NOT NULL AND building_completion_date < %s)
               OR (building_completion_date IS NULL AND project_completion_date IS NOT NULL AND project_completion_date < %s)
            THEN 1 
        END) as count_50_years,
        
        -- Date ranges
        MIN(building_completion_date) as min_building_date,
        MAX(building_completion_date) as max_building_date,
        MIN(project_completion_date) as min_project_date,
        MAX(project_completion_date) as max_project_date
        
    FROM housing_projects;
    """
    
    df = pd.read_sql_query(
        query, 
        conn,
        params=[date_10_years, date_10_years, date_20_years, date_20_years, date_50_years, date_50_years]
    )
    
    print("=== Database Statistics ===")
    print(f"Total records: {df['total_records'].iloc[0]}")
    print(f"Records with building_completion_date: {df['has_building_date'].iloc[0]}")
    print(f"Records with project_completion_date: {df['has_project_date'].iloc[0]}")
    print(f"Records with any completion date: {df['has_any_date'].iloc[0]}")
    print()
    print("=== Age Distribution ===")
    print(f"10+ years old (before {date_10_years}): {df['count_10_years'].iloc[0]} records")
    print(f"20+ years old (before {date_20_years}): {df['count_20_years'].iloc[0]} records")
    print(f"50+ years old (before {date_50_years}): {df['count_50_years'].iloc[0]} records")
    print()
    print("=== Date Ranges ===")
    print(f"Building completion dates: {df['min_building_date'].iloc[0]} to {df['max_building_date'].iloc[0]}")
    print(f"Project completion dates: {df['min_project_date'].iloc[0]} to {df['max_project_date'].iloc[0]}")
    print()
    
    # Sample records for each age category
    print("=== Sample Records ===")
    
    # 10+ years old
    sample_10 = """
    SELECT project_id, project_name, building_completion_date, project_completion_date,
           CASE 
               WHEN building_completion_date IS NOT NULL THEN building_completion_date
               ELSE project_completion_date
           END as effective_date
    FROM housing_projects
    WHERE (building_completion_date IS NOT NULL AND building_completion_date < %s)
       OR (building_completion_date IS NULL AND project_completion_date IS NOT NULL AND project_completion_date < %s)
    LIMIT 5;
    """
    df_10 = pd.read_sql_query(sample_10, conn, params=[date_10_years, date_10_years])
    print(f"\nSample 10+ years old records ({len(df_10)} shown):")
    print(df_10.to_string(index=False))
    
    # 20+ years old
    sample_20 = """
    SELECT project_id, project_name, building_completion_date, project_completion_date,
           CASE 
               WHEN building_completion_date IS NOT NULL THEN building_completion_date
               ELSE project_completion_date
           END as effective_date
    FROM housing_projects
    WHERE (building_completion_date IS NOT NULL AND building_completion_date < %s)
       OR (building_completion_date IS NULL AND project_completion_date IS NOT NULL AND project_completion_date < %s)
    LIMIT 5;
    """
    df_20 = pd.read_sql_query(sample_20, conn, params=[date_20_years, date_20_years])
    print(f"\nSample 20+ years old records ({len(df_20)} shown):")
    print(df_20.to_string(index=False))
    
    # 50+ years old
    sample_50 = """
    SELECT project_id, project_name, building_completion_date, project_completion_date,
           CASE 
               WHEN building_completion_date IS NOT NULL THEN building_completion_date
               ELSE project_completion_date
           END as effective_date
    FROM housing_projects
    WHERE (building_completion_date IS NOT NULL AND building_completion_date < %s)
       OR (building_completion_date IS NULL AND project_completion_date IS NOT NULL AND project_completion_date < %s)
    LIMIT 5;
    """
    df_50 = pd.read_sql_query(sample_50, conn, params=[date_50_years, date_50_years])
    print(f"\nSample 50+ years old records ({len(df_50)} shown):")
    print(df_50.to_string(index=False))
    
    conn.close()
    
    return df

if __name__ == "__main__":
    try:
        check_completion_dates()
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

