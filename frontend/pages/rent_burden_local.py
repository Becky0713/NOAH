"""
Rent Burden Visualization Page (Local SQLite version)
Uses local SQLite database instead of PostgreSQL
"""

import streamlit as st
import pandas as pd
import sqlite3
import json
import os
from pathlib import Path

# Set page config for this page
st.set_page_config(
    page_title="NYC Housing Hub - Rent Burden",
    page_icon="🗽",
    layout="wide"
)

def get_local_db_connection():
    """Get connection to local SQLite database"""
    db_path = Path(__file__).parent.parent / "data" / "rent_burden.db"
    return sqlite3.connect(db_path)

def fetch_rent_burden_data_local():
    """Fetch rent burden data from local SQLite"""
    try:
        conn = get_local_db_connection()
        query = """
        SELECT 
            geo_id,
            tract_name,
            rent_burden_rate,
            severe_burden_rate
        FROM rent_burden
        WHERE rent_burden_rate IS NOT NULL;
        """
        df = pd.read_sql_query(query, conn)
        conn.close()
        return df
    except Exception as e:
        st.error(f"❌ Database error: {e}")
        return pd.DataFrame()

def render_rent_burden_page():
    """Render the main rent burden visualization page"""
    
    # Page header
    st.title("🗽 NYC Rent Burden Dashboard (Local Database)")
    st.markdown("""
    Visualize rent burden rates across NYC census tracts using local SQLite database.
    **Darker colors** indicate higher rent burden (less affordable housing).
    """)
    
    # Add back to dashboard button
    col1, col2, col3 = st.columns([1, 1, 1])
    with col2:
        if st.button("← Back to Dashboard", type="secondary", use_container_width=True):
            st.switch_page("app.py")
    
    # Load data
    with st.spinner("Loading rent burden data from local database..."):
        try:
            df = fetch_rent_burden_data_local()
        except Exception as e:
            st.error(f"❌ Error loading data: {e}")
            df = pd.DataFrame()
    
    if df.empty:
        st.warning("⚠️ No rent burden data available in local database.")
        st.info("📋 **To add data:**")
        st.markdown("""
        1. Create a CSV file with columns: `geo_id`, `tract_name`, `rent_burden_rate`, `severe_burden_rate`
        2. Run: `python scripts/import_csv_to_sqlite.py your_data.csv`
        3. Commit and push the database file
        """)
        st.stop()
    
    # Display summary statistics
    st.subheader("📊 Summary Statistics")
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Total Tracts", f"{len(df):,}")
    
    with col2:
        st.metric("Avg Rent Burden", f"{df['rent_burden_rate'].mean():.1%}")
    
    with col3:
        st.metric("Max Rent Burden", f"{df['rent_burden_rate'].max():.1%}")
    
    with col4:
        st.metric("Severe Burden Avg", f"{df['severe_burden_rate'].mean():.1%}")
    
    st.divider()
    
    # Show data table
    st.subheader("📊 Rent Burden Data")
    st.dataframe(df, use_container_width=True, height=400)
    
    # Download button
    csv = df.to_csv(index=False)
    st.download_button(
        "📥 Download as CSV",
        csv,
        "rent_burden_data.csv",
        "text/csv"
    )

def main():
    """Main function"""
    render_rent_burden_page()

if __name__ == "__main__":
    main()

