"""
Income Map Page
Displays median household income by census tract on an interactive map
"""

import streamlit as st
import pandas as pd
import pydeck as pdk
import psycopg2
from pathlib import Path

# Set page config
st.set_page_config(
    page_title="NYC Housing Hub - Income Map",
    page_icon="💰",
    layout="wide"
)

def get_db_connection():
    """Get database connection from Streamlit secrets"""
    try:
        return psycopg2.connect(
            host=st.secrets["secrets"]["db_host"],
            port=int(st.secrets["secrets"]["db_port"]),
            dbname=st.secrets["secrets"]["db_name"],
            user=st.secrets["secrets"]["db_user"],
            password=st.secrets["secrets"]["db_password"],
            sslmode="require"
        )
    except Exception as e:
        st.error(f"❌ Database connection error: {e}")
        st.stop()

def get_geo_coordinates_from_db(geo_ids):
    """Get latitude and longitude for given geo_ids from database"""
    try:
        conn = get_db_connection()
        
        # Query rent_burden table to find matching geo_ids
        # Format: Convert 1400000US to match 0600000US if needed
        geo_id_list = "', '".join(geo_ids)
        
        # Try to match geo_ids - might need to adjust format
        query = f"""
        SELECT DISTINCT geo_id, latitude, longitude, tract_name
        FROM rent_burden
        WHERE geo_id IN ('{geo_id_list}')
        OR geo_id IN (SELECT REPLACE(geo_id, '0600000US', '1400000US') FROM rent_burden WHERE geo_id LIKE '0600000US%')
        """
        
        df = pd.read_sql_query(query, conn)
        conn.close()
        return df
    except Exception as e:
        st.warning(f"Could not fetch coordinates from database: {e}")
        return pd.DataFrame()

def load_income_data():
    """Load income data directly from the project's PostgreSQL database."""

    table_candidates = [
        "median_income",
        "median_household_income",
    ]

    try:
        conn = get_db_connection()
        for table_name in table_candidates:
            try:
                query = f"""
                SELECT
                    geo_id,
                    tract_name,
                    median_household_income
                FROM {table_name}
                WHERE median_household_income IS NOT NULL
                AND median_household_income != '<NA>'
                """
                df = pd.read_sql_query(query, conn)
                if not df.empty:
                    conn.close()
                    st.success(f"✅ Loaded income data from database table `{table_name}`")
                    return df
            except Exception:
                # Move on to next candidate table
                continue
        conn.close()
    except Exception as e:
        st.error(f"❌ Failed to load income data from database: {e}")

    return pd.DataFrame()

def render_income_map():
    """Render income map page"""
    
    st.title("💰 NYC Median Household Income Map")
    st.markdown("""
    Visualize median household income by census tract using data stored in your PostgreSQL database.
    **Darker colors** indicate higher income areas, while **lighter colors** indicate lower income areas.
    """)
    
    # Load income data
    with st.spinner("Loading income data..."):
        income_df = load_income_data()
    
    if income_df.empty:
        st.info("💡 No income data was found in the configured database. Please ensure the `median_income` (or `median_household_income`) table exists and contains `geo_id`, `tract_name`, `median_household_income` columns.")
        return
    
    # Clean data
    # Remove rows with <NA> or invalid income values
    income_df = income_df[income_df['median_household_income'].notna()]
    income_df = income_df[income_df['median_household_income'] != '<NA>']
    income_df = income_df[income_df['median_household_income'] != 'Geography']
    
    # Convert income to numeric
    income_df['median_household_income'] = pd.to_numeric(
        income_df['median_household_income'], 
        errors='coerce'
    )
    income_df = income_df[income_df['median_household_income'].notna()]
    
    if income_df.empty:
        st.warning("⚠️ No valid income data found.")
        return
    
    # Get coordinates from database
    st.info(f"📊 Loaded {len(income_df)} census tracts. Fetching coordinates...")
    
    # Normalize geo_id format (handle both 1400000US and 0600000US)
    geo_ids = income_df['geo_id'].astype(str).tolist()
    
    # Try to get coordinates
    coord_df = get_geo_coordinates_from_db(geo_ids)
    
    if coord_df.empty:
        st.warning("⚠️ Could not find coordinates for these geo_ids in the database.")
        st.info("💡 **Solution Options:**")
        st.markdown("""
        1. **Add coordinates to your database:** Update the `rent_burden` table with matching geo_ids and their coordinates
        2. **Upload data with coordinates:** Include latitude/longitude columns in your CSV
        3. **Use Census Geocoding API:** We can add functionality to fetch coordinates from Census API
        """)
        
        # Show sample data
        with st.expander("📋 Sample Data"):
            st.dataframe(income_df.head(10))
        return
    
    # Merge income data with coordinates
    # Handle different geo_id formats
    coord_df['geo_id_normalized'] = coord_df['geo_id'].astype(str).str.replace('0600000US', '1400000US')
    income_df['geo_id_normalized'] = income_df['geo_id'].astype(str)
    
    merged_df = income_df.merge(
        coord_df[['geo_id_normalized', 'latitude', 'longitude']],
        on='geo_id_normalized',
        how='left'
    )
    
    # Filter to rows with coordinates
    map_df = merged_df[merged_df['latitude'].notna() & merged_df['longitude'].notna()].copy()
    
    if map_df.empty:
        st.warning("⚠️ No matching coordinates found. Please ensure geo_ids match between datasets.")
        return
    
    st.success(f"✅ Found coordinates for {len(map_df)} census tracts!")
    
    # Display statistics
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Total Tracts", f"{len(map_df):,}")
    with col2:
        st.metric("Median Income", f"${map_df['median_household_income'].median():,.0f}")
    with col3:
        st.metric("Average Income", f"${map_df['median_household_income'].mean():,.0f}")
    with col4:
        st.metric("Max Income", f"${map_df['median_household_income'].max():,.0f}")
    
    st.divider()
    
    # Create map
    st.subheader("🗺️ Interactive Income Map")
    
    # Calculate center point
    center_lat = map_df['latitude'].mean()
    center_lon = map_df['longitude'].mean()
    
    # Normalize income for color coding (0-255 scale)
    min_income = map_df['median_household_income'].min()
    max_income = map_df['median_household_income'].max()
    income_range = max_income - min_income
    
    # Color based on income (green = high income, red = low income)
    def get_income_color(income):
        if income_range == 0:
            return [128, 128, 128, 140]  # Gray if no variation
        
        normalized = (income - min_income) / income_range
        # Use green-blue gradient (high income = green, low income = red)
        if normalized > 0.7:
            return [0, 200, 0, 180]      # Green (high income)
        elif normalized > 0.4:
            return [100, 200, 100, 160]  # Light green
        elif normalized > 0.2:
            return [255, 200, 0, 160]    # Yellow
        else:
            return [255, 100, 0, 180]    # Orange-red (low income)
    
    map_df['color'] = map_df['median_household_income'].apply(get_income_color)
    
    # Point size based on income (larger = higher income)
    map_df['radius'] = map_df['median_household_income'].apply(
        lambda x: max(50, min(300, (x - min_income) / income_range * 200 + 50))
    )
    
    # Create PyDeck layer
    layer = pdk.Layer(
        "ScatterplotLayer",
        data=map_df,
        get_position="[longitude, latitude]",
        get_radius="radius",
        radius_min_pixels=3,
        radius_max_pixels=50,
        get_fill_color="color",
        pickable=True,
    )
    
    # Create tooltip
    tooltip = {
        "html": """
        <b>{tract_name}</b><br/>
        GEOID: {geo_id}<br/>
        Median Household Income: ${median_household_income:,.0f}
        """,
        "style": {"backgroundColor": "#262730", "color": "white"},
    }
    
    # Create view state
    view_state = pdk.ViewState(
        latitude=center_lat, 
        longitude=center_lon, 
        zoom=11, 
        pitch=0
    )
    
    # Render map
    st.pydeck_chart(
        pdk.Deck(
            layers=[layer],
            initial_view_state=view_state,
            tooltip=tooltip,
            map_style='mapbox://styles/mapbox/light-v9'
        )
    )
    
    # Legend
    st.markdown("""
    **Color Legend:**
    - 🟢 **Green**: High income (>70th percentile)
    - 🟡 **Yellow**: Medium income (20th-40th percentile)
    - 🟠 **Orange/Red**: Low income (<20th percentile)
    """)
    
    # Data table
    with st.expander("📋 View Data Table"):
        display_df = map_df[['tract_name', 'geo_id', 'median_household_income', 'latitude', 'longitude']].copy()
        st.dataframe(display_df, use_container_width=True)

if __name__ == "__main__":
    render_income_map()

