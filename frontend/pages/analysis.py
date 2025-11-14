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
    """Fetch ZIP-level median income data - auto-detect table and columns"""
    try:
        conn = get_db_connection()
        
        # Find ZIP-level income table
        table_query = """
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public' 
        AND (table_name LIKE '%zip%income%' OR table_name LIKE '%income%zip%')
        ORDER BY table_name;
        """
        tables_df = pd.read_sql_query(table_query, conn)
        
        if tables_df.empty:
            conn.close()
            return pd.DataFrame()
        
        # Try each table until we find one with data
        for table_name in tables_df['table_name'].tolist():
            try:
                # Get columns
                col_query = f"""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = '{table_name}'
                ORDER BY ordinal_position;
                """
                cols_df = pd.read_sql_query(col_query, conn)
                column_names = cols_df['column_name'].tolist()
                
                # Find zip and income columns
                zip_col = None
                for col in ['zip_code', 'zipcode', 'zip', 'postcode', 'postal_code', 'zcta']:
                    if col in column_names:
                        zip_col = col
                        break
                
                income_col = None
                for col in ['median_income_usd', 'median_income', 'median_household_income', 'income', 'household_income']:
                    if col in column_names:
                        income_col = col
                        break
                
                if zip_col and income_col:
                    query = f"""
                    SELECT "{zip_col}" as zipcode, "{income_col}" as median_income
                    FROM {table_name}
                    WHERE "{zip_col}" IS NOT NULL AND "{income_col}" IS NOT NULL;
                    """
                    df = pd.read_sql_query(query, conn)
                    
                    if not df.empty:
                        df['zipcode'] = df['zipcode'].astype(str).str.extract(r'(\d{5})', expand=False)
                        df = df[df['zipcode'].notna()]
                        df['median_income'] = pd.to_numeric(df['median_income'], errors='coerce')
                        df = df[df['median_income'].notna()]
                        conn.close()
                        return df
            except Exception:
                continue
        
        conn.close()
        return pd.DataFrame()
    except Exception as e:
        st.warning(f"⚠️ Could not fetch median income data: {str(e)[:200]}")
        return pd.DataFrame()

@st.cache_data(show_spinner=False, ttl=3600)
def fetch_rent_burden_analysis_data():
    """Fetch ZIP-level rent burden data - auto-detect table and columns"""
    try:
        conn = get_db_connection()
        
        # Find ZIP-level rent burden table
        table_query = """
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public' 
        AND (table_name LIKE '%zip%burden%' OR table_name LIKE '%burden%zip%' OR table_name LIKE '%rentburden%')
        ORDER BY table_name;
        """
        tables_df = pd.read_sql_query(table_query, conn)
        
        if tables_df.empty:
            conn.close()
            return pd.DataFrame()
        
        # Try each table until we find one with data
        for table_name in tables_df['table_name'].tolist():
            try:
                # Get columns
                col_query = f"""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = '{table_name}'
                ORDER BY ordinal_position;
                """
                cols_df = pd.read_sql_query(col_query, conn)
                column_names = cols_df['column_name'].tolist()
                
                # Find zip and burden columns
                zip_col = None
                for col in ['zip_code', 'zipcode', 'zip', 'postcode', 'postal_code', 'zcta']:
                    if col in column_names:
                        zip_col = col
                        break
                
                burden_col = None
                for col in ['rent_burden_rate', 'burden_rate', 'rent_burden', 'burden']:
                    if col in column_names:
                        burden_col = col
                        break
                
                if zip_col and burden_col:
                    query = f"""
                    SELECT "{zip_col}" as zipcode, "{burden_col}" as rent_burden_rate
                    FROM {table_name}
                    WHERE "{zip_col}" IS NOT NULL AND "{burden_col}" IS NOT NULL;
                    """
                    df = pd.read_sql_query(query, conn)
                    
                    if not df.empty:
                        df['zipcode'] = df['zipcode'].astype(str).str.extract(r'(\d{5})', expand=False)
                        df = df[df['zipcode'].notna()]
                        df['rent_burden_rate'] = pd.to_numeric(df['rent_burden_rate'], errors='coerce')
                        # If values are < 1, convert from decimal to percentage
                        if df['rent_burden_rate'].max() < 1:
                            df['rent_burden_rate'] = df['rent_burden_rate'] * 100
                        df = df[df['rent_burden_rate'].notna()]
                        conn.close()
                        return df
            except Exception:
                continue
        
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
            
            # Clean zipcodes to 5-digit format
            zipcodes_clean = [str(z).strip()[:5] for z in zipcodes if str(z).strip()[:5].isdigit()]
            if not zipcodes_clean:
                conn.close()
                return df
            
            # Try multiple approaches to get coordinates
            coord_df = pd.DataFrame()
            
            # Approach 1: Try rent_burden table with postcode column
            try:
                coord_query = """
                SELECT DISTINCT 
                    postcode,
                    ST_Y(geom) AS latitude,
                    ST_X(geom) AS longitude
                FROM rent_burden
                WHERE postcode::text = ANY(%s)
                AND geom IS NOT NULL
                """
                coord_df = pd.read_sql_query(coord_query, conn, params=(zipcodes_clean,))
            except Exception:
                pass
            
            # Approach 2: If no results, try with zipcode column name
            if coord_df.empty:
                try:
                    coord_query = """
                    SELECT DISTINCT 
                        zipcode,
                        ST_Y(geom) AS latitude,
                        ST_X(geom) AS longitude
                    FROM rent_burden
                    WHERE zipcode::text = ANY(%s)
                    AND geom IS NOT NULL
                    """
                    coord_df = pd.read_sql_query(coord_query, conn, params=(zipcodes_clean,))
                    if not coord_df.empty:
                        coord_df = coord_df.rename(columns={'zipcode': 'postcode'})
                except Exception:
                    pass
            
            # Approach 3: Try noah_zip_rentburden or other ZIP-level tables
            if coord_df.empty:
                try:
                    # Check if we can get coordinates from any table with zipcode
                    coord_query = """
                    SELECT DISTINCT 
                        zip_code,
                        ST_Y(geom) AS latitude,
                        ST_X(geom) AS longitude
                    FROM noah_zip_rentburden
                    WHERE zip_code::text = ANY(%s)
                    AND geom IS NOT NULL
                    """
                    coord_df = pd.read_sql_query(coord_query, conn, params=(zipcodes_clean,))
                    if not coord_df.empty:
                        coord_df = coord_df.rename(columns={'zip_code': 'postcode'})
                except Exception:
                    pass
            
            conn.close()
            
            if not coord_df.empty:
                # Clean postcode to 5-digit format
                coord_df['postcode'] = coord_df['postcode'].astype(str).str.extract(r'(\d{5})', expand=False)
                coord_df = coord_df[coord_df['postcode'].notna()]
                
                # Clean input zipcode to match
                df['zipcode_clean'] = df[location_col].astype(str).str.extract(r'(\d{5})', expand=False)
                
                # Merge on cleaned zipcodes
                df = df.merge(coord_df, left_on='zipcode_clean', right_on='postcode', how='left')
                
                # Drop temporary column
                if 'zipcode_clean' in df.columns:
                    df = df.drop(columns=['zipcode_clean'])
                if 'postcode' in df.columns and 'zipcode' in df.columns:
                    df = df.drop(columns=['postcode'])
        
        return df
    except Exception as e:
        st.warning(f"⚠️ Could not fetch coordinates: {str(e)[:200]}")
        import traceback
        st.code(traceback.format_exc()[:500])
        return df

def render_map_visualization(df, value_col, title, reverse=False, location_col='zipcode'):
    """Render a map visualization with color coding"""
    try:
        if df.empty or value_col not in df.columns:
            st.warning(f"⚠️ No data available for {title}")
            return None
        
        # Check if location_col exists
        if location_col not in df.columns:
            st.warning(f"⚠️ Location column '{location_col}' not found in data")
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
                try:
                    colors = create_color_scale(map_df[value_col], reverse=reverse)
                    if len(colors) != len(map_df):
                        st.warning(f"⚠️ Color scale length mismatch. Using default colors.")
                        colors = ['#d73027'] * len(map_df)  # Default red
                    map_df['color'] = colors
                except Exception as e:
                    st.warning(f"⚠️ Error creating color scale: {str(e)[:100]}")
                    map_df['color'] = '#d73027'  # Default red
                
                # Create PyDeck map
                center_lat = float(map_df['latitude'].mean())
                center_lon = float(map_df['longitude'].mean())
                
                # Convert colors to RGB with error handling
                def hex_to_rgb(hex_color):
                    try:
                        if isinstance(hex_color, str):
                            hex_color = hex_color.lstrip('#')
                            if len(hex_color) == 6:
                                return [int(hex_color[i:i+2], 16) for i in (0, 2, 4)] + [180]
                        # Default to red if invalid
                        return [215, 48, 39, 180]  # Red default
                    except Exception:
                        return [215, 48, 39, 180]  # Red default
                
                map_df['color_rgb'] = map_df['color'].apply(hex_to_rgb)
                
                # Format value for tooltip
                if 'rent' in value_col.lower() and 'burden' not in value_col.lower() and 'income' not in value_col.lower():
                    map_df['value_display'] = map_df[value_col].apply(lambda x: f"${x:,.0f}" if pd.notna(x) else "N/A")
                elif 'income' in value_col.lower():
                    map_df['value_display'] = map_df[value_col].apply(lambda x: f"${x:,.0f}" if pd.notna(x) else "N/A")
                elif 'ratio' in value_col.lower():
                    map_df['value_display'] = map_df[value_col].apply(lambda x: f"{x:.2f}" if pd.notna(x) else "N/A")
                else:
                    map_df['value_display'] = map_df[value_col].apply(lambda x: f"{x:.1f}%" if pd.notna(x) else "N/A")
                
                # Ensure tooltip fields exist
                if 'area_name' not in map_df.columns:
                    if location_col in map_df.columns:
                        map_df['area_name'] = map_df[location_col].astype(str)
                    else:
                        map_df['area_name'] = 'N/A'
                
                # Convert DataFrame to list of dicts, ensuring all values are JSON-serializable
                map_data = []
                for _, row in map_df.iterrows():
                    try:
                        record = {
                            'longitude': float(row['longitude']) if pd.notna(row['longitude']) else None,
                            'latitude': float(row['latitude']) if pd.notna(row['latitude']) else None,
                            'color_rgb': row['color_rgb'] if isinstance(row['color_rgb'], list) else [215, 48, 39, 180],
                            'area_name': str(row.get('area_name', 'N/A')),
                            'value_display': str(row.get('value_display', 'N/A'))
                        }
                        # Only add if coordinates are valid
                        if record['longitude'] is not None and record['latitude'] is not None:
                            map_data.append(record)
                    except Exception:
                        continue
                
                if not map_data:
                    st.warning(f"⚠️ No valid coordinate data for {title}")
                    return None
                
                layer = pdk.Layer(
                    "ScatterplotLayer",
                    data=map_data,
                    get_position="[longitude, latitude]",
                    get_radius=150,
                    radius_min_pixels=8,
                    radius_max_pixels=60,
                    get_fill_color="color_rgb",
                    pickable=True,
                )
                
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
    except Exception as e:
        st.error(f"❌ Error rendering map: {str(e)[:200]}")
        import traceback
        st.code(traceback.format_exc())
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
    
    # Prepare ZIP-level data for Top 3 Critical Areas
    def prepare_top3_critical_areas():
        """Prepare top 3 ZIP codes for each critical metric"""
        results = {
            'lowest_income': [],
            'highest_burden': [],
            'highest_ratio': []
        }
        
        # 1. Lowest Median Income (ZIP-level only)
        # Directly query ZIP-level income table
        try:
            conn = get_db_connection()
            
            # Find ZIP-level income table
            table_query = """
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND (table_name LIKE '%zip%income%' OR table_name LIKE '%income%zip%')
            ORDER BY table_name;
            """
            tables_df = pd.read_sql_query(table_query, conn)
            
            income_zip = pd.DataFrame()
            for table_name in tables_df['table_name'].tolist():
                try:
                    col_query = f"""
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_name = '{table_name}';
                    """
                    cols_df = pd.read_sql_query(col_query, conn)
                    column_names = cols_df['column_name'].tolist()
                    
                    zip_col = None
                    for col in ['zip_code', 'zipcode', 'zip', 'postcode', 'postal_code']:
                        if col in column_names:
                            zip_col = col
                            break
                    
                    income_col = None
                    for col in ['median_income_usd', 'median_income', 'median_household_income', 'income']:
                        if col in column_names:
                            income_col = col
                            break
                    
                    if zip_col and income_col:
                        query = f"""
                        SELECT "{zip_col}" as zipcode, "{income_col}" as median_income
                        FROM {table_name}
                        WHERE "{zip_col}" IS NOT NULL 
                        AND "{income_col}" IS NOT NULL
                        AND "{income_col}" > 0
                        AND CAST("{zip_col}" AS TEXT) ~ '^10[0-9]{3}$|^11[0-6][0-9]{2}$'
                        ORDER BY "{income_col}" ASC
                        LIMIT 3;
                        """
                        income_zip = pd.read_sql_query(query, conn)
                        if not income_zip.empty:
                            break
                except Exception:
                    continue
            
            conn.close()
            
            if not income_zip.empty:
                income_zip['zipcode'] = income_zip['zipcode'].astype(str).str.extract(r'(\d{5})', expand=False)
                income_zip = income_zip[income_zip['zipcode'].notna()]
                income_zip['median_income'] = pd.to_numeric(income_zip['median_income'], errors='coerce')
                # Filter: only NYC ZIPs (100xx-116xx) and income > 10000 (reasonable minimum)
                income_zip = income_zip[
                    (income_zip['zipcode'].str.match(r'^(10[0-9]{3}|11[0-6][0-9]{2})$', na=False)) &
                    (income_zip['median_income'].notna()) &
                    (income_zip['median_income'] > 10000)  # Minimum reasonable income
                ]
                
                if not income_zip.empty:
                    income_zip = income_zip.nsmallest(3, 'median_income')
                    for _, row in income_zip.iterrows():
                        zipcode = str(row['zipcode']).strip()[:5]
                        income_val = float(row['median_income'])
                        if income_val > 10000:  # Double check
                            results['lowest_income'].append({
                                'zipcode': zipcode,
                                'value': income_val,
                                'display': f"ZIP {zipcode} — ${income_val:,.0f}"
                            })
        except Exception as e:
            st.warning(f"⚠️ Error fetching lowest income: {str(e)[:100]}")
        
        # Fill to 3 items if needed
        while len(results['lowest_income']) < 3:
            results['lowest_income'].append({
                'zipcode': None,
                'value': None,
                'display': "Data unavailable"
            })
        
        # 2. Highest Rent Burden (ZIP-level only)
        # Directly query ZIP-level rent burden table
        try:
            conn = get_db_connection()
            
            # Find ZIP-level rent burden table
            table_query = """
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND (table_name LIKE '%zip%burden%' OR table_name LIKE '%burden%zip%' OR table_name LIKE '%rentburden%')
            ORDER BY table_name;
            """
            tables_df = pd.read_sql_query(table_query, conn)
            
            burden_zip = pd.DataFrame()
            for table_name in tables_df['table_name'].tolist():
                try:
                    col_query = f"""
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_name = '{table_name}';
                    """
                    cols_df = pd.read_sql_query(col_query, conn)
                    column_names = cols_df['column_name'].tolist()
                    
                    zip_col = None
                    for col in ['zip_code', 'zipcode', 'zip', 'postcode', 'postal_code']:
                        if col in column_names:
                            zip_col = col
                            break
                    
                    burden_col = None
                    for col in ['rent_burden_rate', 'burden_rate', 'rent_burden', 'burden']:
                        if col in column_names:
                            burden_col = col
                            break
                    
                    if zip_col and burden_col:
                        query = f"""
                        SELECT "{zip_col}" as zipcode, "{burden_col}" as rent_burden_rate
                        FROM {table_name}
                        WHERE "{zip_col}" IS NOT NULL 
                        AND "{burden_col}" IS NOT NULL
                        AND "{burden_col}" > 0
                        AND CAST("{zip_col}" AS TEXT) ~ '^10[0-9]{3}$|^11[0-6][0-9]{2}$'
                        ORDER BY "{burden_col}" DESC
                        LIMIT 3;
                        """
                        burden_zip = pd.read_sql_query(query, conn)
                        if not burden_zip.empty:
                            break
                except Exception:
                    continue
            
            conn.close()
            
            if not burden_zip.empty:
                burden_zip['zipcode'] = burden_zip['zipcode'].astype(str).str.extract(r'(\d{5})', expand=False)
                burden_zip = burden_zip[burden_zip['zipcode'].notna()]
                burden_zip['rent_burden_rate'] = pd.to_numeric(burden_zip['rent_burden_rate'], errors='coerce')
                # If values are < 1, convert from decimal to percentage
                if burden_zip['rent_burden_rate'].max() < 1:
                    burden_zip['rent_burden_rate'] = burden_zip['rent_burden_rate'] * 100
                # Filter: only NYC ZIPs (100xx-116xx) and burden > 5% (exclude invalid/too low data)
                burden_zip = burden_zip[
                    (burden_zip['zipcode'].str.match(r'^(10[0-9]{3}|11[0-6][0-9]{2})$', na=False)) &
                    (burden_zip['rent_burden_rate'].notna()) &
                    (burden_zip['rent_burden_rate'] > 5)  # Exclude very low values that might be invalid
                ]
                
                if not burden_zip.empty:
                    burden_zip = burden_zip.nlargest(3, 'rent_burden_rate')
                    for _, row in burden_zip.iterrows():
                        zipcode = str(row['zipcode']).strip()[:5]
                        burden_val = float(row['rent_burden_rate'])
                        if burden_val > 5:  # Double check
                            results['highest_burden'].append({
                                'zipcode': zipcode,
                                'value': burden_val,
                                'display': f"ZIP {zipcode} — {burden_val:.0f}%"
                            })
        except Exception as e:
            st.warning(f"⚠️ Error fetching highest rent burden: {str(e)[:100]}")
        
        # Fill to 3 items if needed
        while len(results['highest_burden']) < 3:
            results['highest_burden'].append({
                'zipcode': None,
                'value': None,
                'display': "Data unavailable"
            })
        
        # 3. Highest Rent-to-Income Ratio (ZIP-level only)
        # Calculate from ZIP-level rent and income tables
        try:
            conn = get_db_connection()
            
            # Find ZIP-level income table
            income_table = None
            income_table_query = """
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND (table_name LIKE '%zip%income%' OR table_name LIKE '%income%zip%')
            ORDER BY table_name
            LIMIT 1;
            """
            income_tables = pd.read_sql_query(income_table_query, conn)
            if not income_tables.empty:
                income_table = income_tables.iloc[0]['table_name']
            
            # Find ZIP-level rent table (StreetEasy)
            rent_table = 'noah_streeteasy_medianrent_2025_10'
            
            if income_table:
                # Get column names for income table
                income_cols_query = f"""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = '{income_table}';
                """
                income_cols = pd.read_sql_query(income_cols_query, conn)
                income_col_names = income_cols['column_name'].tolist()
                
                income_zip_col = None
                for col in ['zip_code', 'zipcode', 'zip', 'postcode', 'postal_code']:
                    if col in income_col_names:
                        income_zip_col = col
                        break
                
                income_val_col = None
                for col in ['median_income_usd', 'median_income', 'median_household_income', 'income']:
                    if col in income_col_names:
                        income_val_col = col
                        break
                
                # Get column names for rent table
                rent_cols_query = f"""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = '{rent_table}';
                """
                rent_cols = pd.read_sql_query(rent_cols_query, conn)
                rent_col_names = rent_cols['column_name'].tolist()
                
                rent_zip_col = None
                for col in ['zip_code', 'zipcode', 'zip', 'postcode', 'postal_code']:
                    if col in rent_col_names:
                        rent_zip_col = col
                        break
                
                rent_val_col = None
                # Check if using bedroom_type column structure
                if 'bedroom_type' in rent_col_names and 'median_rent_usd' in rent_col_names:
                    # Use 1BR as default if "All" selected
                    bed_filter = "1BR" if bedroom_type == "All" else bedroom_type
                    query = f"""
                    SELECT "{rent_zip_col}" as zipcode, "{rent_val_col or 'median_rent_usd'}" as median_rent
                    FROM {rent_table}
                    WHERE "{rent_zip_col}" IS NOT NULL 
                    AND "{rent_val_col or 'median_rent_usd'}" IS NOT NULL
                    AND bedroom_type = '{bed_filter}';
                    """
                else:
                    # Try to find rent column
                    for col in ['median_rent_usd', 'median_rent', 'rent', 'rent_price']:
                        if col in rent_col_names:
                            rent_val_col = col
                            break
                    
                    if rent_zip_col and rent_val_col:
                        query = f"""
                        SELECT "{rent_zip_col}" as zipcode, "{rent_val_col}" as median_rent
                        FROM {rent_table}
                        WHERE "{rent_zip_col}" IS NOT NULL AND "{rent_val_col}" IS NOT NULL;
                        """
                    else:
                        query = None
                
                if query and income_zip_col and income_val_col:
                    # Get income data
                    income_query = f"""
                    SELECT "{income_zip_col}" as zipcode, "{income_val_col}" as median_income
                    FROM {income_table}
                    WHERE "{income_zip_col}" IS NOT NULL AND "{income_val_col}" IS NOT NULL;
                    """
                    income_df_merge = pd.read_sql_query(income_query, conn)
                    
                    # Get rent data
                    rent_df_merge = pd.read_sql_query(query, conn)
                    
                    if not income_df_merge.empty and not rent_df_merge.empty:
                        # Clean and merge
                        income_df_merge['zipcode'] = income_df_merge['zipcode'].astype(str).str.extract(r'(\d{5})', expand=False)
                        rent_df_merge['zipcode'] = rent_df_merge['zipcode'].astype(str).str.extract(r'(\d{5})', expand=False)
                        
                        # Filter to NYC ZIPs only
                        income_df_merge = income_df_merge[
                            income_df_merge['zipcode'].str.match(r'^(10[0-9]{3}|11[0-6][0-9]{2})$', na=False)
                        ]
                        rent_df_merge = rent_df_merge[
                            rent_df_merge['zipcode'].str.match(r'^(10[0-9]{3}|11[0-6][0-9]{2})$', na=False)
                        ]
                        
                        merged = income_df_merge.merge(rent_df_merge, on='zipcode', how='inner')
                        merged['median_income'] = pd.to_numeric(merged['median_income'], errors='coerce')
                        merged['median_rent'] = pd.to_numeric(merged['median_rent'], errors='coerce')
                        # Filter: income > 0, rent > 0, and both are reasonable values
                        merged = merged[
                            (merged['median_income'] > 10000) &  # Minimum reasonable income
                            (merged['median_rent'] > 500) &      # Minimum reasonable rent
                            (merged['median_income'] < 500000) &  # Maximum reasonable income
                            (merged['median_rent'] < 20000)      # Maximum reasonable rent
                        ]
                        
                        # Calculate ratio: median_rent / (median_income / 12)
                        merged['rent_to_income'] = merged['median_rent'] / (merged['median_income'] / 12)
                        merged = merged[merged['rent_to_income'].notna() & (merged['rent_to_income'] > 0)]
                        merged = merged.nlargest(3, 'rent_to_income')
                        
                        for _, row in merged.iterrows():
                            zipcode = str(row['zipcode']).strip()[:5]
                            ratio_val = float(row['rent_to_income'])
                            results['highest_ratio'].append({
                                'zipcode': zipcode,
                                'value': ratio_val,
                                'display': f"ZIP {zipcode} — {ratio_val:.2f}"
                            })
            
            conn.close()
        except Exception as e:
            st.warning(f"⚠️ Error calculating rent-to-income ratio: {str(e)[:100]}")
        
        # Fill to 3 items if needed - show "Pending" for rent-to-income ratio
        while len(results['highest_ratio']) < 3:
            results['highest_ratio'].append({
                'zipcode': None,
                'value': None,
                'display': "Pending"
            })
        
        return results
    
    # Get top 3 critical areas data
    top3_data = prepare_top3_critical_areas()
    
    # Render Top 3 Most Critical Areas section
    st.markdown("### 📋 Top 3 Most Critical Areas")
    
    # Three-column layout
    col1, col2, col3 = st.columns(3)
    
    # Column 1: Lowest Median Income
    with col1:
        st.markdown("**🔴 Lowest Income ZIPs**")
        st.markdown("<div style='margin-top: 0.5rem;'></div>", unsafe_allow_html=True)
        for item in top3_data['lowest_income'][:3]:
            st.markdown(f"<div style='padding: 0.25rem 0; font-family: monospace;'>{item['display']}</div>", unsafe_allow_html=True)
    
    # Column 2: Highest Rent Burden
    with col2:
        st.markdown("**🔴 Highest Rent Burden ZIPs**")
        st.markdown("<div style='margin-top: 0.5rem;'></div>", unsafe_allow_html=True)
        for item in top3_data['highest_burden'][:3]:
            st.markdown(f"<div style='padding: 0.25rem 0; font-family: monospace;'>{item['display']}</div>", unsafe_allow_html=True)
    
    # Column 3: Highest Rent-to-Income Ratio
    with col3:
        st.markdown("**🔴 Highest Rent-to-Income Ratio ZIPs**")
        st.markdown("<div style='margin-top: 0.5rem;'></div>", unsafe_allow_html=True)
        for item in top3_data['highest_ratio'][:3]:
            st.markdown(f"<div style='padding: 0.25rem 0; font-family: monospace;'>{item['display']}</div>", unsafe_allow_html=True)
    
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

