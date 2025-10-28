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
    """Get database connection from Streamlit secrets"""
    # Debug: Show available secrets
    st.write("🔍 Debug - Available secrets:", list(st.secrets.keys()))
    
    # Read from Streamlit secrets (lowercase keys)
    try:
        # Streamlit secrets with nested structure
        return psycopg2.connect(
            host=st.secrets["secrets"]["db_host"],
            port=int(st.secrets["secrets"]["db_port"]),
            dbname=st.secrets["secrets"]["db_name"],
            user=st.secrets["secrets"]["db_user"],
            password=st.secrets["secrets"]["db_password"],
            sslmode="require"
        )
    except KeyError as e:
        st.error(f"❌ Missing secret: {e}")
        st.error("Please add your database credentials to Streamlit Secrets")
        st.stop()
    except Exception as e:
        st.error(f"❌ Failed to read secrets: {e}")
        st.stop()

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
        try:
            df = fetch_rent_burden_data()
        except Exception as e:
            st.error(f"❌ Database connection error: {e}")
            df = pd.DataFrame()
    
    if df.empty:
        st.warning("⚠️ No rent burden data available.")
        st.info("📋 **Quick Check:**")
        st.text("1. Ensure rent_burden table exists in your database")
        st.text("2. Set DB_* environment variables in Streamlit Secrets")
        st.text("3. Verify database connection is working")
        
        with st.expander("📋 Show Database Connection Help"):
            st.markdown("""
            **Required Database Setup:**
            
            1. **Create the `rent_burden` table:**
               ```sql
               CREATE TABLE rent_burden (
                   geo_id TEXT PRIMARY KEY,
                   tract_name TEXT,
                   rent_burden_rate DECIMAL,
                   severe_burden_rate DECIMAL
               );
               ```
            
            2. **Set Streamlit Secrets:**
               ```toml
               [secrets]
               DB_HOST = "your-postgres-host"
               DB_NAME = "noah_dashboard"
               DB_USER = "your-username"
               DB_PASSWORD = "your-password"
               ```
            """)
        
        # Try to show raw connection test
        with st.expander("🔧 Test Database Connection"):
            if st.button("Test Connection"):
                try:
                    conn = get_db_connection()
                    cursor = conn.cursor()
                    cursor.execute("SELECT version();")
                    version = cursor.fetchone()
                    st.success(f"✅ Connected! PostgreSQL version: {version[0]}")
                    cursor.close()
                    conn.close()
                except Exception as e:
                    st.error(f"❌ Connection failed: {e}")
        
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
    
    # Show data first
    st.subheader("📊 Rent Burden Data")
    st.dataframe(df, use_container_width=True, height=400)
    
    # Download button
    csv = df.to_csv(index=False)
    st.download_button(
        "📥 Download Data as CSV",
        csv,
        "rent_burden_data.csv",
        "text/csv"
    )
    
    st.divider()
    
    # GeoJSON loading section (optional for map visualization)
    geojson_path = Path(__file__).parent.parent / "data" / "nyc_tracts.geojson"
    geojson_data = None
    
    if geojson_path.exists():
        with st.spinner("Loading census tract boundaries..."):
            geojson_data = load_geojson(geojson_path)
    else:
        st.info("💡 **Tip:** Add `nyc_tracts.geojson` to enable choropleth map visualization")
    
    # Visualization section
    if geojson_data and not df.empty:
        st.subheader("🗺️ Rent Burden Choropleth Map")
        
        # Debug GeoJSON structure
        if geojson_data and len(geojson_data.get('features', [])) > 0:
            first_feature = geojson_data['features'][0]
            st.write("🔍 Debug - GeoJSON properties:", list(first_feature.get('properties', {}).keys()))
            st.write("🔍 Debug - Sample data geo_ids:", df['geo_id'].head().tolist())
        
        try:
            import plotly.express as px
            
            # Try different possible GEOID field names
            possible_geoid_fields = [
                "properties.GEOID", 
                "properties.geoid", 
                "properties.TRACTCE", 
                "properties.tractce",
                "properties.CT2010",
                "properties.ct2010"
            ]
            
            # Find the correct field
            geoid_field = "properties.GEOID"  # default
            if geojson_data and len(geojson_data.get('features', [])) > 0:
                props = geojson_data['features'][0].get('properties', {})
                for field in possible_geoid_fields:
                    field_name = field.replace("properties.", "")
                    if field_name in props:
                        geoid_field = field
                        st.write(f"✅ Using GeoJSON field: {field}")
                        break
            
            # Create choropleth map
            fig = px.choropleth_mapbox(
                df,
                geojson=geojson_data,
                featureidkey=geoid_field,
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
