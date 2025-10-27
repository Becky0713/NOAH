"""
Rent Burden Visualization Page
Displays rent burden rate choropleth map
"""

import streamlit as st
import pandas as pd
import json
import os
from pathlib import Path
import psycopg2
from psycopg2.extras import RealDictCursor

# Set page config for this page
st.set_page_config(
    page_title="NYC Housing Hub - Rent Burden",
    page_icon="🗽",
    layout="wide"
)

def get_db_connection():
    """Get database connection from environment variables"""
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", "5432"),
        dbname=os.getenv("DB_NAME", "noah_dashboard"),
        user=os.getenv("DB_USER", "postgres"),
        password=os.getenv("DB_PASSWORD", "")
    )

def fetch_rent_burden_data():
    """Fetch rent burden data from PostgreSQL"""
    try:
        conn = get_db_connection()
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
        st.error(f"❌ Database connection error: {e}")
        return pd.DataFrame()

def load_geojson(geojson_path):
    """Load GeoJSON file"""
    try:
        with open(geojson_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        st.error(f"❌ GeoJSON file not found: {geojson_path}")
        return None
    except json.JSONDecodeError:
        st.error("❌ Invalid JSON format in GeoJSON file")
        return None

def render_rent_burden_page():
    """Render the main rent burden visualization page"""
    
    # Page header
    st.title("🗽 NYC Rent Burden Dashboard")
    st.markdown("""
    Visualize rent burden rates across NYC census tracts. **Darker colors** indicate 
    higher rent burden (less affordable housing), while **lighter colors** indicate 
    more affordable housing.
    """)
    
    # Add back to dashboard button
    col1, col2, col3 = st.columns([1, 1, 1])
    with col2:
        if st.button("← Back to Dashboard", type="secondary", use_container_width=True):
            st.switch_page("app.py")
    
    # Load data
    with st.spinner("Loading rent burden data..."):
        df = fetch_rent_burden_data()
    
    if df.empty:
        st.warning("⚠️ No rent burden data available. Please ensure the rent_burden table exists in your database.")
        
        # Show database connection help
        with st.expander("📋 Database Setup Help"):
            st.markdown("""
            **Required Database Setup:**
            
            1. **Create the `rent_burden` table:**
               ```sql
               CREATE TABLE rent_burden (
                   geo_id TEXT PRIMARY KEY,
                   tract_name TEXT,
                   rent_burden_rate DECIMAL,
                   severe_burden_rate DECIMAL,
                   geometry GEOMETRY(POLYGON, 4326)
               );
               ```
            
            2. **Load your rent burden data** into the table
            
            3. **Set environment variables:**
               - `DB_HOST`: PostgreSQL host
               - `DB_PORT`: PostgreSQL port (default: 5432)
               - `DB_NAME`: Database name (default: noah_dashboard)
               - `DB_USER`: Database user
               - `DB_PASSWORD`: Database password
            """)
        st.stop()
    
    # Display summary statistics
    st.subheader("📊 Summary Statistics")
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            "Total Tracts",
            f"{len(df):,}",
            help="Number of census tracts with data"
        )
    
    with col2:
        st.metric(
            "Avg Rent Burden",
            f"{df['rent_burden_rate'].mean():.1%}",
            help="Average rent burden rate"
        )
    
    with col3:
        st.metric(
            "Max Rent Burden",
            f"{df['rent_burden_rate'].max():.1%}",
            help="Highest rent burden rate"
        )
    
    with col4:
        st.metric(
            "Severe Burden Avg",
            f"{df['severe_burden_rate'].mean():.1%}",
            help="Average severe burden rate"
        )
    
    st.divider()
    
    # GeoJSON loading section
    geojson_path = Path(__file__).parent.parent / "data" / "nyc_tracts.geojson"
    geojson_data = None
    
    if geojson_path.exists():
        with st.spinner("Loading census tract boundaries..."):
            geojson_data = load_geojson(geojson_path)
    else:
        st.warning("⚠️ GeoJSON file not found. Map visualization requires `nyc_tracts.geojson` file.")
        
        with st.expander("📥 How to get NYC Census Tracts GeoJSON"):
            st.markdown("""
            1. **NYC Open Data:**
               - Visit: https://data.cityofnewyork.us/City-Government/2010-Census-Tracts/37yn-as6i
               - Download as GeoJSON
               - Save as `frontend/data/nyc_tracts.geojson`
            
            2. **US Census TIGER:**
               - Visit: https://www.census.gov/cgi-bin/geo/shapefiles/
               - Select "Census Tracts" and "New York County"
               - Convert shapefile to GeoJSON using ogr2ogr
            """)
    
    # Visualization section
    if geojson_data and not df.empty:
        st.subheader("🗺️ Rent Burden Choropleth Map")
        
        try:
            import plotly.express as px
            
            # Create choropleth map
            fig = px.choropleth_mapbox(
                df,
                geojson=geojson_data,
                featureidkey="properties.GEOID",
                locations="geo_id",
                color="rent_burden_rate",
                color_continuous_scale="Reds",
                range_color=(0, 1),
                mapbox_style="carto-positron",
                zoom=9,
                center={"lat": 40.7128, "lon": -74.0060},
                opacity=0.7,
                hover_name="tract_name",
                hover_data={
                    "rent_burden_rate": ":.2%",
                    "severe_burden_rate": ":.2%",
                    "geo_id": True
                },
                labels={
                    "rent_burden_rate": "Rent Burden Rate",
                    "severe_burden_rate": "Severe Burden Rate"
                },
                title="Rent Burden Rate by Census Tract<br><sub>Darker = Less Affordable Housing</sub>"
            )
            
            # Update layout
            fig.update_layout(
                height=700,
                margin={"r": 0, "t": 50, "l": 0, "b": 0}
            )
            
            # Display the map
            st.plotly_chart(fig, use_container_width=True)
            
            # Add Mapbox token notice
            st.info("💡 To use satellite imagery or other Mapbox styles, add your Mapbox token to Streamlit secrets as `MAPBOX_TOKEN`")
            
        except ImportError:
            st.error("❌ Plotly not installed. Please install: `pip install plotly`")
    elif not df.empty:
        # Fallback: Show data table if no map
        st.subheader("📋 Rent Burden Data")
        st.dataframe(df, use_container_width=True, height=400)
    
    # Data table section
    with st.expander("📋 View Raw Data"):
        st.dataframe(df, use_container_width=True)

def main():
    """Main function"""
    render_rent_burden_page()

if __name__ == "__main__":
    main()
