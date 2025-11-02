"""
Import CSV data into SQLite database
This script helps you import rent burden data from CSV file
"""

import sqlite3
import pandas as pd
import sys
from pathlib import Path

# Database path
DB_PATH = Path(__file__).parent.parent / "frontend" / "data" / "rent_burden.db"
DB_PATH.parent.mkdir(parents=True, exist_ok=True)

def create_table(conn):
    """Create rent_burden table"""
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS rent_burden (
            geo_id TEXT PRIMARY KEY,
            tract_name TEXT,
            rent_burden_rate REAL,
            severe_burden_rate REAL
        )
    """)
    conn.commit()
    print("✅ Table created: rent_burden")

def import_from_csv(csv_path):
    """Import data from CSV file"""
    print(f"📥 Reading CSV file: {csv_path}")
    df = pd.read_csv(csv_path)
    
    print(f"📊 Found {len(df)} rows")
    print(f"📋 Columns: {df.columns.tolist()}")
    
    # Connect to database
    conn = sqlite3.connect(DB_PATH)
    create_table(conn)
    
    # Import data
    df.to_sql('rent_burden', conn, if_exists='replace', index=False)
    
    # Verify import
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM rent_burden")
    count = cursor.fetchone()[0]
    
    print(f"✅ Successfully imported {count} rows to {DB_PATH}")
    
    # Show sample data
    sample = pd.read_sql("SELECT * FROM rent_burden LIMIT 5", conn)
    print("\n📋 Sample data:")
    print(sample)
    
    conn.close()
    print(f"\n✅ Database ready at: {DB_PATH}")

def print_help():
    """Print help message"""
    print("""
📋 Usage:
    python scripts/import_csv_to_sqlite.py <your_csv_file.csv>

📝 CSV Format:
    Your CSV should have these columns:
    - geo_id (census tract GEOID)
    - tract_name (tract name)
    - rent_burden_rate (0.0 to 1.0)
    - severe_burden_rate (0.0 to 1.0)

Example CSV:
geo_id,tract_name,rent_burden_rate,severe_burden_rate
36061000100,Census Tract 1,0.45,0.23
36061000200,Census Tract 2,0.52,0.28

Then:
    git add frontend/data/rent_burden.db
    git commit -m "Add rent burden database"
    git push origin master
""")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print_help()
        sys.exit(1)
    
    csv_path = Path(sys.argv[1])
    if not csv_path.exists():
        print(f"❌ File not found: {csv_path}")
        sys.exit(1)
    
    import_from_csv(csv_path)

