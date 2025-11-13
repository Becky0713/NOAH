#!/usr/bin/env python3
"""
Build ZIP-level tables from tract-level ACS data using zip_tract_crosswalk

This script:
1. Creates noah_zip_income from tract-level median income
2. Creates noah_zip_rentburden from tract-level rent burden
3. Creates noah_affordability_analysis (unified table)

Uses weighted aggregation: ZIP_value = SUM(tract_value * tot_ratio)
"""

import psycopg2
import pandas as pd
import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

def get_db_connection():
    """Get database connection from environment or Streamlit secrets"""
    try:
        # Try environment variables first (for local/script execution)
        import os
        db_host = os.getenv("DB_HOST")
        db_port = os.getenv("DB_PORT", "5432")
        db_name = os.getenv("DB_NAME")
        db_user = os.getenv("DB_USER")
        db_password = os.getenv("DB_PASSWORD")
        
        if not all([db_host, db_name, db_user, db_password]):
            # Try Streamlit secrets (for deployed environment)
            import streamlit as st
            db_host = st.secrets["secrets"]["db_host"]
            db_port = int(st.secrets["secrets"]["db_port"])
            db_name = st.secrets["secrets"]["db_name"]
            db_user = st.secrets["secrets"]["db_user"]
            db_password = st.secrets["secrets"]["db_password"]
    except Exception:
        # Fallback: read from .env file
        from dotenv import load_dotenv
        load_dotenv()
        import os
        db_host = os.getenv("DB_HOST")
        db_port = os.getenv("DB_PORT", "5432")
        db_name = os.getenv("DB_NAME")
        db_user = os.getenv("DB_USER")
        db_password = os.getenv("DB_PASSWORD")
    
    if not all([db_host, db_name, db_user, db_password]):
        raise ValueError("Database credentials not found. Set DB_HOST, DB_NAME, DB_USER, DB_PASSWORD")
    
    return psycopg2.connect(
        host=db_host,
        port=int(db_port),
        dbname=db_name,
        user=db_user,
        password=db_password,
        sslmode="require"
    )

def find_tract_table(conn, table_patterns):
    """Find tract-level table matching patterns"""
    for pattern in table_patterns:
        query = f"""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public' 
        AND table_name LIKE '{pattern}'
        LIMIT 1;
        """
        result = pd.read_sql_query(query, conn)
        if not result.empty:
            return result.iloc[0]['table_name']
    return None

def find_column(conn, table_name, column_patterns):
    """Find column matching patterns in table"""
    query = f"""
    SELECT column_name 
    FROM information_schema.columns 
    WHERE table_name = '{table_name}'
    ORDER BY ordinal_position;
    """
    columns_df = pd.read_sql_query(query, conn)
    if columns_df.empty:
        return None
    
    column_names = columns_df['column_name'].tolist()
    
    for pattern in column_patterns:
        for col in column_names:
            if pattern.lower() in col.lower():
                return col
    
    return None

def find_geoid_column(conn, table_name):
    """Find GEOID column in tract table"""
    return find_column(conn, table_name, ['geoid', 'geo_id', 'tract', 'census_tract', 'tract_id'])

def build_zip_income_table(conn):
    """Build noah_zip_income from tract-level income data"""
    print("📊 Building noah_zip_income table...")
    
    # Find tract-level income table
    income_table = find_tract_table(conn, ['median_income', 'median_household_income', 'income', 'b19013'])
    if not income_table:
        print("⚠️  Could not find tract-level income table")
        return False
    
    print(f"   Found income table: {income_table}")
    
    # Find columns
    geoid_col = find_geoid_column(conn, income_table)
    income_col = find_column(conn, income_table, ['median_household_income', 'median_income', 'income'])
    
    if not geoid_col or not income_col:
        print(f"⚠️  Could not find required columns in {income_table}")
        return False
    
    print(f"   Using GEOID column: {geoid_col}")
    print(f"   Using income column: {income_col}")
    
    # Load crosswalk
    print("   Loading zip_tract_crosswalk...")
    crosswalk_query = """
    SELECT zip_code, tract, tot_ratio
    FROM zip_tract_crosswalk
    WHERE zip_code IS NOT NULL AND tract IS NOT NULL AND tot_ratio IS NOT NULL;
    """
    crosswalk_df = pd.read_sql_query(crosswalk_query, conn)
    
    if crosswalk_df.empty:
        print("⚠️  zip_tract_crosswalk table is empty or not found")
        return False
    
    print(f"   Loaded {len(crosswalk_df)} crosswalk records")
    
    # Load tract-level income data
    print("   Loading tract-level income data...")
    income_query = f"""
    SELECT "{geoid_col}" as geoid, "{income_col}" as median_income
    FROM {income_table}
    WHERE "{geoid_col}" IS NOT NULL AND "{income_col}" IS NOT NULL;
    """
    income_df = pd.read_sql_query(income_query, conn)
    
    if income_df.empty:
        print("⚠️  No tract-level income data found")
        return False
    
    print(f"   Loaded {len(income_df)} tract records")
    
    # Clean GEOID: ensure 11-digit format
    income_df['geoid'] = income_df['geoid'].astype(str).str.strip()
    # Remove any prefixes like "1400000US" or "0600000US"
    income_df['geoid'] = income_df['geoid'].str.replace(r'^\d+US', '', regex=True)
    income_df['geoid'] = income_df['geoid'].str.zfill(11)  # Pad to 11 digits
    
    crosswalk_df['tract'] = crosswalk_df['tract'].astype(str).str.strip().str.zfill(11)
    
    # Merge crosswalk with income data
    merged = crosswalk_df.merge(income_df, left_on='tract', right_on='geoid', how='inner')
    
    if merged.empty:
        print("⚠️  No matching tracts found between crosswalk and income data")
        return False
    
    print(f"   Matched {len(merged)} tract-ZIP pairs")
    
    # Convert income to numeric
    merged['median_income'] = pd.to_numeric(merged['median_income'], errors='coerce')
    merged = merged[merged['median_income'].notna()]
    
    # Weighted aggregation: SUM(tract_value * tot_ratio)
    zip_income = merged.groupby('zip_code').apply(
        lambda x: (x['median_income'] * x['tot_ratio']).sum()
    ).reset_index(name='median_income_usd')
    
    # Create table
    print("   Creating noah_zip_income table...")
    with conn.cursor() as cur:
        cur.execute("""
        DROP TABLE IF EXISTS noah_zip_income;
        CREATE TABLE noah_zip_income (
            zip_code VARCHAR(5) PRIMARY KEY,
            median_income_usd NUMERIC(10, 2)
        );
        """)
        conn.commit()
    
    # Insert data
    for _, row in zip_income.iterrows():
        with conn.cursor() as cur:
            cur.execute("""
            INSERT INTO noah_zip_income (zip_code, median_income_usd)
            VALUES (%s, %s)
            ON CONFLICT (zip_code) DO UPDATE SET
                median_income_usd = EXCLUDED.median_income_usd;
            """, (str(row['zip_code']).strip()[:5], float(row['median_income_usd'])))
        conn.commit()
    
    print(f"✅ Created noah_zip_income with {len(zip_income)} ZIP codes")
    return True

def build_zip_rentburden_table(conn):
    """Build noah_zip_rentburden from tract-level rent burden data"""
    print("\n📊 Building noah_zip_rentburden table...")
    
    # Find tract-level rent burden table
    burden_table = find_tract_table(conn, ['rent_burden', 'b25070', 'burden'])
    if not burden_table:
        print("⚠️  Could not find tract-level rent burden table")
        return False
    
    print(f"   Found rent burden table: {burden_table}")
    
    # Find columns
    geoid_col = find_geoid_column(conn, burden_table)
    burden_col = find_column(conn, burden_table, ['rent_burden_rate', 'burden_rate', 'rent_burden'])
    severe_col = find_column(conn, burden_table, ['severe_burden_rate', 'severe_burden', 'severe_rent_burden'])
    
    if not geoid_col:
        print(f"⚠️  Could not find GEOID column in {burden_table}")
        return False
    
    print(f"   Using GEOID column: {geoid_col}")
    if burden_col:
        print(f"   Using rent burden column: {burden_col}")
    if severe_col:
        print(f"   Using severe burden column: {severe_col}")
    
    # Load crosswalk
    print("   Loading zip_tract_crosswalk...")
    crosswalk_query = """
    SELECT zip_code, tract, tot_ratio
    FROM zip_tract_crosswalk
    WHERE zip_code IS NOT NULL AND tract IS NOT NULL AND tot_ratio IS NOT NULL;
    """
    crosswalk_df = pd.read_sql_query(crosswalk_query, conn)
    
    if crosswalk_df.empty:
        print("⚠️  zip_tract_crosswalk table is empty or not found")
        return False
    
    # Load tract-level burden data
    print("   Loading tract-level rent burden data...")
    select_cols = [f'"{geoid_col}" as geoid']
    if burden_col:
        select_cols.append(f'"{burden_col}" as rent_burden_rate')
    if severe_col:
        select_cols.append(f'"{severe_col}" as severe_burden_rate')
    
    burden_query = f"""
    SELECT {', '.join(select_cols)}
    FROM {burden_table}
    WHERE "{geoid_col}" IS NOT NULL;
    """
    burden_df = pd.read_sql_query(burden_query, conn)
    
    if burden_df.empty:
        print("⚠️  No tract-level rent burden data found")
        return False
    
    print(f"   Loaded {len(burden_df)} tract records")
    
    # Clean GEOID
    burden_df['geoid'] = burden_df['geoid'].astype(str).str.strip()
    burden_df['geoid'] = burden_df['geoid'].str.replace(r'^\d+US', '', regex=True)
    burden_df['geoid'] = burden_df['geoid'].str.zfill(11)
    
    crosswalk_df['tract'] = crosswalk_df['tract'].astype(str).str.strip().str.zfill(11)
    
    # Merge crosswalk with burden data
    merged = crosswalk_df.merge(burden_df, left_on='tract', right_on='geoid', how='inner')
    
    if merged.empty:
        print("⚠️  No matching tracts found between crosswalk and burden data")
        return False
    
    print(f"   Matched {len(merged)} tract-ZIP pairs")
    
    # Convert to numeric
    if 'rent_burden_rate' in merged.columns:
        merged['rent_burden_rate'] = pd.to_numeric(merged['rent_burden_rate'], errors='coerce')
        # If values are < 1, they're decimals (convert to percentage)
        if merged['rent_burden_rate'].max() < 1:
            merged['rent_burden_rate'] = merged['rent_burden_rate'] * 100
    
    if 'severe_burden_rate' in merged.columns:
        merged['severe_burden_rate'] = pd.to_numeric(merged['severe_burden_rate'], errors='coerce')
        if merged['severe_burden_rate'].max() < 1:
            merged['severe_burden_rate'] = merged['severe_burden_rate'] * 100
    
    # Weighted aggregation
    agg_dict = {}
    if 'rent_burden_rate' in merged.columns:
        agg_dict['rent_burden_rate'] = lambda x: (x['rent_burden_rate'] * x['tot_ratio']).sum()
    if 'severe_burden_rate' in merged.columns:
        agg_dict['severe_burden_rate'] = lambda x: (x['severe_burden_rate'] * x['tot_ratio']).sum()
    
    zip_burden = merged.groupby('zip_code').agg(agg_dict).reset_index()
    
    # Create table
    print("   Creating noah_zip_rentburden table...")
    with conn.cursor() as cur:
        cur.execute("""
        DROP TABLE IF EXISTS noah_zip_rentburden;
        CREATE TABLE noah_zip_rentburden (
            zip_code VARCHAR(5) PRIMARY KEY,
            rent_burden_rate NUMERIC(5, 2),
            severe_burden_rate NUMERIC(5, 2)
        );
        """)
        conn.commit()
    
    # Insert data
    for _, row in zip_burden.iterrows():
        with conn.cursor() as cur:
            cur.execute("""
            INSERT INTO noah_zip_rentburden (zip_code, rent_burden_rate, severe_burden_rate)
            VALUES (%s, %s, %s)
            ON CONFLICT (zip_code) DO UPDATE SET
                rent_burden_rate = EXCLUDED.rent_burden_rate,
                severe_burden_rate = EXCLUDED.severe_burden_rate;
            """, (
                str(row['zip_code']).strip()[:5],
                float(row.get('rent_burden_rate', 0)) if pd.notna(row.get('rent_burden_rate')) else None,
                float(row.get('severe_burden_rate', 0)) if pd.notna(row.get('severe_burden_rate')) else None
            ))
        conn.commit()
    
    print(f"✅ Created noah_zip_rentburden with {len(zip_burden)} ZIP codes")
    return True

def build_affordability_analysis_table(conn):
    """Build unified noah_affordability_analysis table"""
    print("\n📊 Building noah_affordability_analysis table...")
    
    # Load ZIP-level income
    income_query = """
    SELECT zip_code, median_income_usd
    FROM noah_zip_income;
    """
    income_df = pd.read_sql_query(income_query, conn)
    
    if income_df.empty:
        print("⚠️  noah_zip_income table is empty")
        return False
    
    # Load ZIP-level rent burden
    burden_query = """
    SELECT zip_code, rent_burden_rate, severe_burden_rate
    FROM noah_zip_rentburden;
    """
    burden_df = pd.read_sql_query(burden_query, conn)
    
    # Load StreetEasy median rent (already at ZIP level)
    rent_query = """
    SELECT DISTINCT zipcode, 
           COALESCE(median_rent, rent, median_rent_price, average_rent, rent_price) as median_rent_usd
    FROM noah_streeteasy_medianrent_2025_10
    WHERE zipcode IS NOT NULL;
    """
    
    # Try to find rent column dynamically
    try:
        column_query = """
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_name = 'noah_streeteasy_medianrent_2025_10'
        AND (column_name LIKE '%rent%' OR column_name LIKE '%price%')
        ORDER BY ordinal_position;
        """
        rent_cols_df = pd.read_sql_query(column_query, conn)
        if not rent_cols_df.empty:
            rent_col = rent_cols_df.iloc[0]['column_name']
            zip_col = None
            for col in ['zipcode', 'zip_code', 'postcode', 'postal_code', 'zip']:
                zip_query = f"""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = 'noah_streeteasy_medianrent_2025_10'
                AND column_name = '{col}';
                """
                if not pd.read_sql_query(zip_query, conn).empty:
                    zip_col = col
                    break
            
            if rent_col and zip_col:
                rent_query = f"""
                SELECT DISTINCT "{zip_col}" as zipcode, "{rent_col}" as median_rent_usd
                FROM noah_streeteasy_medianrent_2025_10
                WHERE "{zip_col}" IS NOT NULL AND "{rent_col}" IS NOT NULL;
                """
    except Exception:
        pass
    
    rent_df = pd.read_sql_query(rent_query, conn)
    
    # Merge all three datasets
    merged = income_df.copy()
    
    if not burden_df.empty:
        merged = merged.merge(burden_df, on='zip_code', how='left')
    
    if not rent_df.empty:
        rent_df['zipcode'] = rent_df['zipcode'].astype(str).str.extract(r'(\d{5})', expand=False)
        rent_df = rent_df[rent_df['zipcode'].notna()]
        merged = merged.merge(rent_df, left_on='zip_code', right_on='zipcode', how='left', suffixes=('', '_rent'))
        merged = merged.drop(columns=['zipcode'], errors='ignore')
    
    # Calculate rent-to-income ratio
    merged['median_rent_usd'] = pd.to_numeric(merged['median_rent_usd'], errors='coerce')
    merged['median_income_usd'] = pd.to_numeric(merged['median_income_usd'], errors='coerce')
    
    # ratio = median_rent_usd / (median_income_usd / 12)
    merged['rent_to_income_ratio'] = merged.apply(
        lambda row: row['median_rent_usd'] / (row['median_income_usd'] / 12) 
        if pd.notna(row['median_rent_usd']) and pd.notna(row['median_income_usd']) and row['median_income_usd'] > 0 
        else None,
        axis=1
    )
    
    # Create table
    print("   Creating noah_affordability_analysis table...")
    with conn.cursor() as cur:
        cur.execute("""
        DROP TABLE IF EXISTS noah_affordability_analysis;
        CREATE TABLE noah_affordability_analysis (
            zip_code VARCHAR(5) PRIMARY KEY,
            median_income_usd NUMERIC(10, 2),
            rent_burden_rate NUMERIC(5, 2),
            severe_burden_rate NUMERIC(5, 2),
            median_rent_usd NUMERIC(10, 2),
            rent_to_income_ratio NUMERIC(5, 3)
        );
        """)
        conn.commit()
    
    # Insert data
    for _, row in merged.iterrows():
        with conn.cursor() as cur:
            cur.execute("""
            INSERT INTO noah_affordability_analysis (
                zip_code, median_income_usd, rent_burden_rate, severe_burden_rate,
                median_rent_usd, rent_to_income_ratio
            )
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (zip_code) DO UPDATE SET
                median_income_usd = EXCLUDED.median_income_usd,
                rent_burden_rate = EXCLUDED.rent_burden_rate,
                severe_burden_rate = EXCLUDED.severe_burden_rate,
                median_rent_usd = EXCLUDED.median_rent_usd,
                rent_to_income_ratio = EXCLUDED.rent_to_income_ratio;
            """, (
                str(row['zip_code']).strip()[:5],
                float(row['median_income_usd']) if pd.notna(row['median_income_usd']) else None,
                float(row.get('rent_burden_rate', 0)) if pd.notna(row.get('rent_burden_rate')) else None,
                float(row.get('severe_burden_rate', 0)) if pd.notna(row.get('severe_burden_rate')) else None,
                float(row['median_rent_usd']) if pd.notna(row['median_rent_usd']) else None,
                float(row['rent_to_income_ratio']) if pd.notna(row['rent_to_income_ratio']) else None
            ))
        conn.commit()
    
    print(f"✅ Created noah_affordability_analysis with {len(merged)} ZIP codes")
    return True

def main():
    """Main execution"""
    print("=" * 60)
    print("Building ZIP-level tables from tract-level ACS data")
    print("=" * 60)
    
    conn = None
    try:
        conn = get_db_connection()
        print("✅ Connected to database\n")
        
        # Step 1: Build ZIP income table
        success1 = build_zip_income_table(conn)
        
        # Step 2: Build ZIP rent burden table
        success2 = build_zip_rentburden_table(conn)
        
        # Step 3: Build unified affordability analysis table
        if success1 or success2:
            success3 = build_affordability_analysis_table(conn)
        else:
            print("\n⚠️  Skipping affordability analysis table (no source data)")
            success3 = False
        
        print("\n" + "=" * 60)
        if success1 and success2 and success3:
            print("✅ All tables built successfully!")
        elif success1 or success2:
            print("⚠️  Some tables built with warnings (see above)")
        else:
            print("❌ Failed to build tables (see errors above)")
        print("=" * 60)
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    main()

