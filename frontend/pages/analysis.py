"""
Analysis Page
Displays multiple map visualizations for median rent, income, rent burden, and rent-to-income ratio
"""

import streamlit as st
import pandas as pd
import pydeck as pdk
import psycopg2
import plotly.express as px
import plotly.graph_objects as go

# Set page config
st.set_page_config(
    page_title="NYC Housing Hub - Analysis",
    page_icon="📊",
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
    except KeyError as e:
        st.error(f"❌ Missing secret: {e}")
        st.stop()
    except Exception as e:
        st.error(f"❌ Database connection error: {e}")
        st.stop()

def normalize_borough_name(borough):
    """Normalize borough name for matching"""
    if not borough:
        return None
    borough_lower = str(borough).lower().strip()
    borough_map = {
        'manhattan': 'Manhattan',
        'new york': 'Manhattan',
        'new york county': 'Manhattan',
        'brooklyn': 'Brooklyn',
        'kings': 'Brooklyn',
        'kings county': 'Brooklyn',
        'queens': 'Queens',
        'queens county': 'Queens',
        'bronx': 'Bronx',
        'bronx county': 'Bronx',
        'staten island': 'Staten Island',
        'richmond': 'Staten Island',
        'richmond county': 'Staten Island'
    }
    return borough_map.get(borough_lower, borough)

@st.cache_data(show_spinner=False, ttl=3600)
def fetch_median_rent_data():
    """Fetch median rent data by bedroom type from noah_streeteasy_medianrent_2025_10"""
    try:
        conn = get_db_connection()
        
        # Get column names
        column_query = """
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_name = 'noah_streeteasy_medianrent_2025_10'
        ORDER BY ordinal_position;
        """
        columns_df = pd.read_sql_query(column_query, conn)
        
        if columns_df.empty:
            conn.close()
            return pd.DataFrame()
        
        column_names = columns_df['column_name'].tolist()
        
        # Find bedroom type columns (studio, 1br, 2br, 3+br)
        bedroom_cols = {}
        rent_cols = {}
        
        for col in column_names:
            col_lower = col.lower()
            if ('studio' in col_lower or '0br' in col_lower or 'efficiency' in col_lower) and ('rent' in col_lower or 'median' in col_lower or 'price' in col_lower):
                bedroom_cols.setdefault('studio', col)
            elif (
                any(token in col_lower for token in ['1br', '1_br', 'one', '1-bedroom', 'one_bed'])
                and ('rent' in col_lower or 'median' in col_lower or 'price' in col_lower)
            ):
                bedroom_cols.setdefault('1br', col)
            elif (
                any(token in col_lower for token in ['2br', '2_br', 'two', 'two_bed', '2-bedroom'])
                and ('rent' in col_lower or 'median' in col_lower or 'price' in col_lower)
            ):
                bedroom_cols.setdefault('2br', col)
            elif (
                any(token in col_lower for token in ['3br', '3_br', '3+', 'three', 'three_bed', '4br', '4_br', 'five', '5br', '6br'])
                and ('rent' in col_lower or 'median' in col_lower or 'price' in col_lower)
            ):
                bedroom_cols.setdefault('3+br', col)
        
        # Find location columns
        zip_col = None
        borough_col = None
        area_col = None
        
        for col in ['zipcode', 'zip_code', 'postcode', 'postal_code', 'zip', 'zcta']:
            if col in column_names:
                zip_col = col
                break
        
        for col in ['borough', 'borough_name', 'county', 'county_name']:
            if col in column_names:
                borough_col = col
                break
        
        for col in ['area_name', 'area', 'region', 'region_name', 'neighborhood']:
            if col in column_names:
                area_col = col
                break
        
        if not bedroom_cols:
            conn.close()
            st.warning("⚠️ Could not find bedroom type rent columns")
            st.info(f"Available columns in `noah_streeteasy_medianrent_2025_10`: {column_names}")
            return pd.DataFrame()
        
        # Build query
        select_cols = list(bedroom_cols.values())
        if zip_col:
            select_cols.append(zip_col)
        if borough_col:
            select_cols.append(borough_col)
        if area_col:
            select_cols.append(area_col)
        
        select_str = ", ".join([f'"{col}"' for col in select_cols])
        
        query = f"""
        SELECT {select_str}
        FROM noah_streeteasy_medianrent_2025_10
        """
        
        df = pd.read_sql_query(query, conn)
        conn.close()
        
        if df.empty:
            return pd.DataFrame()
        
        # Rename bedroom columns
        for bed_type, col_name in bedroom_cols.items():
            df = df.rename(columns={col_name: f'rent_{bed_type}'})
        
        # Prepare location columns
        if zip_col:
            df['zipcode'] = df[zip_col].astype(str).str.extract(r'(\d{5})', expand=False)
        if borough_col:
            df['borough'] = df[borough_col].apply(normalize_borough_name)
        if area_col:
            df['area_name'] = df[area_col].astype(str)
        
        return df
    except Exception as e:
        st.warning(f"⚠️ Could not fetch median rent data: {str(e)[:200]}")
        return pd.DataFrame()

@st.cache_data(show_spinner=False, ttl=3600)
def fetch_median_income_data():
    """Fetch median income data from median_income table"""
    try:
        conn = get_db_connection()
        
        # Try to get column names
        column_query = """
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_name = 'median_income'
        ORDER BY ordinal_position;
        """
        columns_df = pd.read_sql_query(column_query, conn)
        
        if columns_df.empty:
            conn.close()
            return pd.DataFrame()
        
        column_names = columns_df['column_name'].tolist()
        
        # Find income column
        income_col = None
        for col in ['median_household_income', 'median_income', 'income', 'household_income']:
            if col in column_names:
                income_col = col
                break
        
        if not income_col:
            for col in column_names:
                if 'income' in col.lower():
                    income_col = col
                    break
        
        if not income_col:
            conn.close()
            st.warning("⚠️ Could not find income column")
            return pd.DataFrame()
        
        # Find location columns
        zip_col = None
        borough_col = None
        area_col = None
        
        for col in ['zipcode', 'zip_code', 'postcode', 'postal_code', 'zip', 'zcta']:
            if col in column_names:
                zip_col = col
                break
        
        for col in ['borough', 'borough_name', 'county', 'county_name']:
            if col in column_names:
                borough_col = col
                break
        
        for col in ['area_name', 'area', 'region', 'region_name', 'neighborhood', 'tract_name']:
            if col in column_names:
                area_col = col
                break
        
        # Build query
        select_cols = [income_col]
        if zip_col:
            select_cols.append(zip_col)
        if borough_col:
            select_cols.append(borough_col)
        if area_col:
            select_cols.append(area_col)
        
        select_str = ", ".join([f'"{col}"' for col in select_cols])
        
        query = f"""
        SELECT {select_str}
        FROM median_income
        WHERE "{income_col}" IS NOT NULL
        """
        
        df = pd.read_sql_query(query, conn)
        conn.close()
        
        if df.empty:
            return pd.DataFrame()
        
        # Rename income column
        df = df.rename(columns={income_col: 'median_income'})
        
        # Convert to numeric
        df['median_income'] = pd.to_numeric(df['median_income'], errors='coerce')
        df = df[df['median_income'].notna()]
        
        # Prepare location columns
        if zip_col:
            df['zipcode'] = df[zip_col].astype(str).str.extract(r'(\d{5})', expand=False)
        if borough_col:
            df['borough'] = df[borough_col].apply(normalize_borough_name)
        if area_col:
            df['area_name'] = df[area_col].astype(str)
        
        return df
    except Exception as e:
        st.warning(f"⚠️ Could not fetch median income data: {str(e)[:200]}")
        return pd.DataFrame()

@st.cache_data(show_spinner=False, ttl=3600)
def fetch_rent_burden_analysis_data():
    """Fetch rent burden data for analysis"""
    try:
        conn = get_db_connection()
        
        # Try noah_zip_rentburden first
        column_query = """
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_name = 'noah_zip_rentburden'
        ORDER BY ordinal_position;
        """
        columns_df = pd.read_sql_query(column_query, conn)
        
        if not columns_df.empty:
            column_names = columns_df['column_name'].tolist()
            
            # Find rent burden column
            burden_col = None
            for col in column_names:
                if 'rent' in col.lower() and 'burden' in col.lower():
                    burden_col = col
                    break
            
            # Find zip code column
            zip_col = None
            for col in ['zipcode', 'zip_code', 'postcode', 'postal_code', 'zip', 'zcta']:
                if col in column_names:
                    zip_col = col
                    break
            
            if burden_col and zip_col:
                query = f"""
                SELECT "{zip_col}" as zipcode, "{burden_col}" as rent_burden_rate
                FROM noah_zip_rentburden
                WHERE "{burden_col}" IS NOT NULL
                """
                df = pd.read_sql_query(query, conn)
                df['zipcode'] = df['zipcode'].astype(str).str.extract(r'(\d{5})', expand=False)
                df = df[df['zipcode'].notna()]
                df['rent_burden_rate'] = pd.to_numeric(df['rent_burden_rate'], errors='coerce')
                df = df[df['rent_burden_rate'].notna()]
                conn.close()
                return df
        
        conn.close()
        return pd.DataFrame()
    except Exception as e:
        st.warning(f"⚠️ Could not fetch rent burden data: {str(e)[:200]}")
        return pd.DataFrame()

def create_color_scale(values, reverse=False):
    """Create color scale: red for worst/low, green for best/high"""
    if values.empty or values.isna().all():
        return ['#808080'] * len(values)
    
    min_val = values.min()
    max_val = values.max()
    
    if min_val == max_val:
        return ['#808080'] * len(values)
    
    normalized = (values - min_val) / (max_val - min_val)
    
    if reverse:
        # Red for high values (worst), green for low values (best)
        colors = []
        for n in normalized:
            if pd.isna(n):
                colors.append('#808080')
            elif n > 0.7:
                colors.append('#d73027')  # Dark red
            elif n > 0.4:
                colors.append('#fc8d59')  # Orange-red
            elif n > 0.2:
                colors.append('#fee08b')  # Yellow
            else:
                colors.append('#1a9850')  # Green
    else:
        # Green for high values (best), red for low values (worst)
        colors = []
        for n in normalized:
            if pd.isna(n):
                colors.append('#808080')
            elif n > 0.7:
                colors.append('#1a9850')  # Green
            elif n > 0.4:
                colors.append('#91cf60')  # Light green
            elif n > 0.2:
                colors.append('#fee08b')  # Yellow
            else:
                colors.append('#d73027')  # Red
    
    return colors

def get_coordinates_for_locations(df, location_col='zipcode'):
    """Get coordinates for locations from database"""
    try:
        conn = get_db_connection()
        
        if location_col == 'zipcode':
            # Get coordinates by zipcode from rent_burden table
            zipcodes = df[location_col].dropna().unique().tolist()
            if not zipcodes:
                conn.close()
                return df
            
            # Use parameterized query to avoid SQL injection
            placeholders = ','.join(['%s'] * len(zipcodes))
            coord_query = f"""
            SELECT DISTINCT 
                postcode,
                ST_Y(geom) AS latitude,
                ST_X(geom) AS longitude
            FROM rent_burden
            WHERE postcode = ANY(%s)
            AND geom IS NOT NULL
            """
            coord_df = pd.read_sql_query(coord_query, conn, params=(zipcodes,))
            conn.close()
            
            if not coord_df.empty:
                coord_df['postcode'] = coord_df['postcode'].astype(str).str.extract(r'(\d{5})', expand=False)
                df = df.merge(coord_df, left_on=location_col, right_on='postcode', how='left')
        
        return df
    except Exception as e:
        st.warning(f"⚠️ Could not fetch coordinates: {str(e)[:100]}")
        return df

def render_map_visualization(df, value_col, title, reverse=False, location_col='zipcode'):
    """Render a map visualization with color coding"""
    if df.empty or value_col not in df.columns:
        st.warning(f"⚠️ No data available for {title}")
        return None
    
    # Filter out invalid data
    map_df = df[[value_col, location_col]].dropna(subset=[value_col, location_col]).copy()
    
    if map_df.empty:
        st.warning(f"⚠️ No valid data for {title}")
        return None
    
    # Get coordinates
    map_df = get_coordinates_for_locations(map_df, location_col)
    
    # If we have coordinates, create a scatter map
    if 'latitude' in map_df.columns and 'longitude' in map_df.columns:
        map_df = map_df[map_df['latitude'].notna() & map_df['longitude'].notna()].copy()
        
        if not map_df.empty:
            # Create color scale
            colors = create_color_scale(map_df[value_col], reverse=reverse)
            map_df['color'] = colors
            
            # Create PyDeck map
            center_lat = map_df['latitude'].mean()
            center_lon = map_df['longitude'].mean()
            
            # Convert colors to RGB
            def hex_to_rgb(hex_color):
                hex_color = hex_color.lstrip('#')
                return [int(hex_color[i:i+2], 16) for i in (0, 2, 4)] + [180]
            
            map_df['color_rgb'] = map_df['color'].apply(hex_to_rgb)
            
            # Format value for tooltip
            if 'rent' in value_col.lower() or 'income' in value_col.lower():
                map_df['value_display'] = map_df[value_col].apply(lambda x: f"${x:,.0f}" if pd.notna(x) else "N/A")
            elif 'ratio' in value_col.lower():
                map_df['value_display'] = map_df[value_col].apply(lambda x: f"{x:.2f}" if pd.notna(x) else "N/A")
            else:
                map_df['value_display'] = map_df[value_col].apply(lambda x: f"{x:.1f}%" if pd.notna(x) else "N/A")
            
            layer = pdk.Layer(
                "ScatterplotLayer",
                data=map_df.to_dict('records'),
                get_position="[longitude, latitude]",
                get_radius=150,
                radius_min_pixels=8,
                radius_max_pixels=60,
                get_fill_color="color_rgb",
                pickable=True,
            )
            
            # Ensure tooltip fields exist
            if 'area_name' not in map_df.columns:
                if location_col in map_df.columns:
                    map_df['area_name'] = map_df[location_col]
                else:
                    map_df['area_name'] = 'N/A'
            
            tooltip = {
                "html": "<b>Location:</b> {area_name}<br/><b>" + title + ":</b> {value_display}",
                "style": {"backgroundColor": "#262730", "color": "white"},
            }
            
            view_state = pdk.ViewState(
                latitude=center_lat,
                longitude=center_lon,
                zoom=11,
                pitch=0
            )
            
            return pdk.Deck(
                layers=[layer],
                initial_view_state=view_state,
                tooltip=tooltip,
                map_style='mapbox://styles/mapbox/light-v9'
            )
    
    # Fallback: show data table if no coordinates
    return None

def render_analysis_page():
    """Render the analysis page"""
    st.title("📊 Analysis Dashboard")
    st.markdown("""
    Visualize key housing metrics across NYC neighborhoods and zip codes.
    Use filters below to explore specific areas and bedroom types.
    """)
    
    # Filters
    col1, col2, col3 = st.columns(3)
    
    with col1:
        bedroom_type = st.selectbox(
            "Bedroom Type",
            options=["All", "Studio", "1BR", "2BR", "3+BR"],
            index=0
        )
    
    with col2:
        location_filter = st.text_input(
            "Location Search (zip code, neighborhood, or borough)",
            placeholder="e.g., 10025, Upper West Side, Manhattan",
            key="location_search"
        )
    
    with col3:
        map_type = st.selectbox(
            "Map Type",
            options=["Median Rent", "Median Income", "Rent Burden", "Rent-to-Income Ratio"],
            index=0
        )
    
    # Load data
    with st.spinner("Loading data..."):
        rent_df = fetch_median_rent_data()
        income_df = fetch_median_income_data()
        burden_df = fetch_rent_burden_analysis_data()
    
    # Apply filters
    if location_filter:
        location_lower = location_filter.lower()
        if rent_df is not None and not rent_df.empty:
            if 'zipcode' in rent_df.columns:
                rent_df = rent_df[rent_df['zipcode'].astype(str).str.contains(location_lower, case=False, na=False)]
            if 'area_name' in rent_df.columns:
                rent_df = rent_df[rent_df['area_name'].astype(str).str.contains(location_lower, case=False, na=False)]
            if 'borough' in rent_df.columns:
                rent_df = rent_df[rent_df['borough'].astype(str).str.contains(location_lower, case=False, na=False)]
        
        if income_df is not None and not income_df.empty:
            if 'zipcode' in income_df.columns:
                income_df = income_df[income_df['zipcode'].astype(str).str.contains(location_lower, case=False, na=False)]
            if 'area_name' in income_df.columns:
                income_df = income_df[income_df['area_name'].astype(str).str.contains(location_lower, case=False, na=False)]
            if 'borough' in income_df.columns:
                income_df = income_df[income_df['borough'].astype(str).str.contains(location_lower, case=False, na=False)]
        
        if burden_df is not None and not burden_df.empty:
            if 'zipcode' in burden_df.columns:
                burden_df = burden_df[burden_df['zipcode'].astype(str).str.contains(location_lower, case=False, na=False)]
    
    # Show top 3 worst cases when map is not displayed
    st.markdown("### 📋 Top 3 Most Critical Areas")
    
    # Calculate and display worst cases for each metric
    col1, col2, col3, col4 = st.columns(4)
    
    # 1. Lowest Median Rent
    if not rent_df.empty and bedroom_type != "All":
        bed_col = f'rent_{bedroom_type.lower().replace("+", "")}'
        if bed_col in rent_df.columns and rent_df[bed_col].notna().any():
            worst_rent = rent_df.nsmallest(3, bed_col)
            with col1:
                st.markdown("**🔴 Lowest Median Rent**")
                if not worst_rent.empty:
                    for _, row in worst_rent.iterrows():
                        location = str(row.get('area_name', '') or row.get('zipcode', '') or row.get('borough', '') or 'N/A')
                        rent_val = row.get(bed_col)
                        if pd.notna(rent_val):
                            st.write(f"**{location}**<br/>${rent_val:,.0f}", unsafe_allow_html=True)
                else:
                    st.write("No data available")
        else:
            with col1:
                st.markdown("**🔴 Lowest Median Rent**")
                st.write("No data available")

    # 2. Lowest Median Income
    if not income_df.empty and 'median_income' in income_df.columns:
        worst_income = income_df.nsmallest(3, 'median_income')
        with col2:
            st.markdown("**🔴 Lowest Median Income**")
            if not worst_income.empty:
                for _, row in worst_income.iterrows():
                    location = str(row.get('area_name', '') or row.get('zipcode', '') or row.get('borough', '') or 'N/A')
                    income_val = row.get('median_income')
                    if pd.notna(income_val):
                        st.write(f"**{location}**<br/>${income_val:,.0f}", unsafe_allow_html=True)
            else:
                st.write("No data available")
    else:
        with col2:
            st.markdown("**🔴 Lowest Median Income**")
            st.write("No data available")

    # 3. Highest Rent Burden
    if not burden_df.empty and 'rent_burden_rate' in burden_df.columns:
        worst_burden = burden_df.nlargest(3, 'rent_burden_rate')
        with col3:
            st.markdown("**🔴 Highest Rent Burden**")
            if not worst_burden.empty:
                for _, row in worst_burden.iterrows():
                    location = str(row.get('zipcode', '') or row.get('area_name', '') or 'N/A')
                    burden_val = row.get('rent_burden_rate')
                    if pd.notna(burden_val):
                        st.write(f"**{location}**<br/>{burden_val:.1f}%", unsafe_allow_html=True)
            else:
                st.write("No data available")
    else:
        with col3:
            st.markdown("**🔴 Highest Rent Burden**")
            st.write("No data available")
    
    # 4. Highest Rent-to-Income Ratio (if we can calculate)
    if not rent_df.empty and not income_df.empty:
        # Merge rent and income data
        if bedroom_type != "All":
            bed_col = f'rent_{bedroom_type.lower().replace("+", "")}'
            if bed_col in rent_df.columns and 'zipcode' in rent_df.columns and 'zipcode' in income_df.columns:
                merged = rent_df.merge(income_df, on='zipcode', how='inner', suffixes=('_rent', '_income'))
                if 'median_income' in merged.columns and merged['median_income'].notna().any():
                    merged['rent_to_income'] = merged[bed_col] / merged['median_income']
                    merged = merged[merged['rent_to_income'].notna() & (merged['rent_to_income'] > 0)]
                    worst_ratio = merged.nlargest(3, 'rent_to_income') if not merged.empty else pd.DataFrame()
                    with col4:
                        st.markdown("**🔴 Highest Rent-to-Income Ratio**")
                        if not worst_ratio.empty:
                            for _, row in worst_ratio.iterrows():
                                location = str(row.get('area_name', '') or row.get('zipcode', '') or 'N/A')
                                ratio_val = row.get('rent_to_income')
                                if pd.notna(ratio_val):
                                    st.write(f"**{location}**<br/>{ratio_val:.2f}", unsafe_allow_html=True)
                        else:
                            st.write("No data available")
            else:
                with col4:
                    st.markdown("**🔴 Highest Rent-to-Income Ratio**")
                    st.write("No data available")
    else:
        with col4:
            st.markdown("**🔴 Highest Rent-to-Income Ratio**")
            st.write("No data available")
    
    st.divider()
    
    # Map visualization section
    st.markdown("### 🗺️ Map Visualizations")
    
    # Create buttons for each map type
    map_cols = st.columns(4)
    
    with map_cols[0]:
        show_rent_map = st.button("📊 Show Median Rent Map", use_container_width=True)
    with map_cols[1]:
        show_income_map = st.button("💰 Show Median Income Map", use_container_width=True)
    with map_cols[2]:
        show_burden_map = st.button("📈 Show Rent Burden Map", use_container_width=True)
    with map_cols[3]:
        show_ratio_map = st.button("📉 Show Rent-to-Income Ratio Map", use_container_width=True)
    
    # Display selected map
    if show_rent_map:
        st.markdown("---")
        if bedroom_type != "All":
            bed_col = f'rent_{bedroom_type.lower().replace("+", "")}'
            if bed_col in rent_df.columns and not rent_df[bed_col].isna().all():
                st.subheader(f"📊 Median Rent Map - {bedroom_type}")
                st.markdown(f"**Color Legend:** 🔴 Red = Lowest Rent | 🟢 Green = Highest Rent")
                map_obj = render_map_visualization(rent_df, bed_col, f"Median Rent ({bedroom_type})", reverse_color=True)
                if map_obj:
                    st.pydeck_chart(map_obj, use_container_width=True)
                else:
                    st.info("Map visualization requires coordinate data. Showing data table instead.")
                    display_df = rent_df[[bed_col, 'zipcode', 'area_name', 'borough']].dropna(subset=[bed_col]).sort_values(bed_col)
                    st.dataframe(display_df.head(20), use_container_width=True)
            else:
                st.warning(f"⚠️ No {bedroom_type} rent data available.")
        else:
            st.warning("⚠️ Please select a bedroom type to view median rent map.")
    
    if show_income_map:
        st.markdown("---")
        st.subheader("💰 Median Income Map")
        st.markdown("**Color Legend:** 🔴 Red = Lowest Income | 🟢 Green = Highest Income")
        if not income_df.empty and income_df['median_income'].notna().any():
            map_obj = render_map_visualization(income_df, 'median_income', "Median Income", reverse_color=True)
            if map_obj:
                st.pydeck_chart(map_obj, use_container_width=True)
            else:
                st.info("Map visualization requires coordinate data. Showing data table instead.")
                display_df = income_df[['median_income', 'zipcode', 'area_name', 'borough']].dropna(subset=['median_income']).sort_values('median_income')
                st.dataframe(display_df.head(20), use_container_width=True)
        else:
            st.warning("⚠️ No median income data available.")
    
    if show_burden_map:
        st.markdown("---")
        st.subheader("📈 Rent Burden Map")
        st.markdown("**Color Legend:** 🟢 Green = Lowest Burden | 🔴 Red = Highest Burden")
        if not burden_df.empty and burden_df['rent_burden_rate'].notna().any():
            map_obj = render_map_visualization(burden_df, 'rent_burden_rate', "Rent Burden Rate", reverse_color=False)
            if map_obj:
                st.pydeck_chart(map_obj, use_container_width=True)
            else:
                st.info("Map visualization requires coordinate data. Showing data table instead.")
                display_df = burden_df[['rent_burden_rate', 'zipcode']].dropna(subset=['rent_burden_rate']).sort_values('rent_burden_rate', ascending=False)
                st.dataframe(display_df.head(20), use_container_width=True)
        else:
            st.warning("⚠️ No rent burden data available.")
    
    if show_ratio_map:
        st.markdown("---")
        st.subheader("📉 Rent-to-Income Ratio Map")
        st.markdown("**Color Legend:** 🟢 Green = Lower Ratio (Better) | 🔴 Red = Higher Ratio (Worse)")
        if bedroom_type != "All" and not rent_df.empty and not income_df.empty:
            bed_col = f'rent_{bedroom_type.lower().replace("+", "")}'
            if bed_col in rent_df.columns:
                # Merge and calculate ratio
                merged = rent_df.merge(income_df, on='zipcode', how='inner', suffixes=('_rent', '_income'))
                if 'median_income' in merged.columns and merged['median_income'].notna().any():
                    merged['rent_to_income'] = merged[bed_col] / merged['median_income']
                    merged = merged[merged['rent_to_income'].notna() & (merged['rent_to_income'] > 0)]
                    if not merged.empty:
                        map_obj = render_map_visualization(merged, 'rent_to_income', "Rent-to-Income Ratio", reverse_color=False)
                        if map_obj:
                            st.pydeck_chart(map_obj, use_container_width=True)
                        else:
                            st.info("Map visualization requires coordinate data. Showing data table instead.")
                            display_df = merged[['rent_to_income', 'zipcode', 'area_name']].sort_values('rent_to_income', ascending=False)
                            st.dataframe(display_df.head(20), use_container_width=True)
                    else:
                        st.warning("⚠️ No valid rent-to-income ratio data after calculation.")
                else:
                    st.warning("⚠️ Could not calculate rent-to-income ratio (missing income data).")
            else:
                st.warning(f"⚠️ No {bedroom_type} rent data available.")
        else:
            st.warning("⚠️ Please select a bedroom type to view rent-to-income ratio map.")

if __name__ == "__main__":
    render_analysis_page()

