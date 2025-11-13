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
    """Fetch ZIP-level median income data from noah_zip_income table"""
    try:
        conn = get_db_connection()
        
        # Try new ZIP-level table first
        query = """
        SELECT zip_code as zipcode, median_income_usd as median_income
        FROM noah_zip_income
        WHERE zip_code IS NOT NULL AND median_income_usd IS NOT NULL;
        """
        df = pd.read_sql_query(query, conn)
        conn.close()
        
        if not df.empty:
            df['zipcode'] = df['zipcode'].astype(str).str.extract(r'(\d{5})', expand=False)
            df = df[df['zipcode'].notna()]
            df['median_income'] = pd.to_numeric(df['median_income'], errors='coerce')
            df = df[df['median_income'].notna()]
            return df
        
        # Fallback to old table if new one doesn't exist
        conn = get_db_connection()
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
        income_col = None
        for col in ['median_household_income', 'median_income', 'income', 'household_income']:
            if col in column_names:
                income_col = col
                break
        
        if not income_col:
            conn.close()
            return pd.DataFrame()
        
        zip_col = None
        for col in ['zipcode', 'zip_code', 'postcode', 'postal_code', 'zip', 'zcta']:
            if col in column_names:
                zip_col = col
                break
        
        select_cols = [income_col]
        if zip_col:
            select_cols.append(zip_col)
        
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
        
        df = df.rename(columns={income_col: 'median_income'})
        df['median_income'] = pd.to_numeric(df['median_income'], errors='coerce')
        df = df[df['median_income'].notna()]
        
        if zip_col:
            df['zipcode'] = df[zip_col].astype(str).str.extract(r'(\d{5})', expand=False)
            df = df[df['zipcode'].notna()]
        
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
    
    # Prepare ZIP-level data for Top 3 Critical Areas
    def prepare_top3_critical_areas():
        """Prepare top 3 ZIP codes for each critical metric"""
        results = {
            'lowest_income': [],
            'highest_burden': [],
            'highest_ratio': []
        }
        
        # 1. Lowest Median Income (ZIP-level only)
        # Try unified table first, then fallback to individual table
        try:
            conn = get_db_connection()
            unified_query = """
            SELECT zip_code as zipcode, median_income_usd as median_income
            FROM noah_affordability_analysis
            WHERE zip_code IS NOT NULL AND median_income_usd IS NOT NULL
            ORDER BY median_income_usd ASC
            LIMIT 3;
            """
            income_zip = pd.read_sql_query(unified_query, conn)
            conn.close()
            
            if not income_zip.empty:
                for _, row in income_zip.iterrows():
                    zipcode = str(row['zipcode']).strip()[:5]
                    income_val = float(row['median_income'])
                    results['lowest_income'].append({
                        'zipcode': zipcode,
                        'value': income_val,
                        'display': f"ZIP {zipcode} — ${income_val:,.0f}"
                    })
        except Exception:
            # Fallback to individual income table
            if not income_df.empty and 'median_income' in income_df.columns and 'zipcode' in income_df.columns:
                income_zip = income_df[['zipcode', 'median_income']].copy()
                income_zip = income_zip[income_zip['zipcode'].notna() & income_zip['median_income'].notna()]
                income_zip = income_zip[income_zip['zipcode'].astype(str).str.len() == 5]
                income_zip = income_zip.nsmallest(3, 'median_income')
                
                for _, row in income_zip.iterrows():
                    zipcode = str(row['zipcode']).strip()[:5]
                    income_val = float(row['median_income'])
                    results['lowest_income'].append({
                        'zipcode': zipcode,
                        'value': income_val,
                        'display': f"ZIP {zipcode} — ${income_val:,.0f}"
                    })
        
        # Fill to 3 items if needed
        while len(results['lowest_income']) < 3:
            results['lowest_income'].append({
                'zipcode': None,
                'value': None,
                'display': "Data unavailable"
            })
        
        # 2. Highest Rent Burden (ZIP-level only)
        # Try unified table first, then fallback to individual table
        try:
            conn = get_db_connection()
            unified_query = """
            SELECT zip_code as zipcode, rent_burden_rate
            FROM noah_affordability_analysis
            WHERE zip_code IS NOT NULL AND rent_burden_rate IS NOT NULL
            ORDER BY rent_burden_rate DESC
            LIMIT 3;
            """
            burden_zip = pd.read_sql_query(unified_query, conn)
            conn.close()
            
            if not burden_zip.empty:
                for _, row in burden_zip.iterrows():
                    zipcode = str(row['zipcode']).strip()[:5]
                    burden_val = float(row['rent_burden_rate'])
                    results['highest_burden'].append({
                        'zipcode': zipcode,
                        'value': burden_val,
                        'display': f"ZIP {zipcode} — {burden_val:.0f}%"
                    })
        except Exception:
            # Fallback to individual burden table
            if not burden_df.empty and 'rent_burden_rate' in burden_df.columns and 'zipcode' in burden_df.columns:
                burden_zip = burden_df[['zipcode', 'rent_burden_rate']].copy()
                burden_zip = burden_zip[burden_zip['zipcode'].notna() & burden_zip['rent_burden_rate'].notna()]
                burden_zip = burden_zip[burden_zip['zipcode'].astype(str).str.len() == 5]
                burden_zip['rent_burden_rate'] = pd.to_numeric(burden_zip['rent_burden_rate'], errors='coerce')
                if burden_zip['rent_burden_rate'].max() < 1:
                    burden_zip['rent_burden_rate'] = burden_zip['rent_burden_rate'] * 100
                burden_zip = burden_zip.nlargest(3, 'rent_burden_rate')
                
                for _, row in burden_zip.iterrows():
                    zipcode = str(row['zipcode']).strip()[:5]
                    burden_val = float(row['rent_burden_rate'])
                    results['highest_burden'].append({
                        'zipcode': zipcode,
                        'value': burden_val,
                        'display': f"ZIP {zipcode} — {burden_val:.0f}%"
                    })
        
        # Fill to 3 items if needed
        while len(results['highest_burden']) < 3:
            results['highest_burden'].append({
                'zipcode': None,
                'value': None,
                'display': "Data unavailable"
            })
        
        # 3. Highest Rent-to-Income Ratio (ZIP-level only)
        # Try unified table first (has pre-calculated ratio)
        try:
            conn = get_db_connection()
            unified_query = """
            SELECT zip_code as zipcode, rent_to_income_ratio as rent_to_income
            FROM noah_affordability_analysis
            WHERE zip_code IS NOT NULL AND rent_to_income_ratio IS NOT NULL
            ORDER BY rent_to_income_ratio DESC
            LIMIT 3;
            """
            ratio_zip = pd.read_sql_query(unified_query, conn)
            conn.close()
            
            if not ratio_zip.empty:
                for _, row in ratio_zip.iterrows():
                    zipcode = str(row['zipcode']).strip()[:5]
                    ratio_val = float(row['rent_to_income'])
                    results['highest_ratio'].append({
                        'zipcode': zipcode,
                        'value': ratio_val,
                        'display': f"ZIP {zipcode} — {ratio_val:.2f}"
                    })
        except Exception:
            # Fallback: calculate from rent and income tables
            if not rent_df.empty and not income_df.empty:
                if bedroom_type != "All":
                    bed_col = f'rent_{bedroom_type.lower().replace("+", "")}'
                else:
                    bed_col = 'rent_1br'
                
                if bed_col in rent_df.columns and 'zipcode' in rent_df.columns and 'zipcode' in income_df.columns:
                    rent_zip = rent_df[['zipcode', bed_col]].copy()
                    rent_zip = rent_zip[rent_zip['zipcode'].notna() & rent_zip[bed_col].notna()]
                    rent_zip = rent_zip[rent_zip['zipcode'].astype(str).str.len() == 5]
                    
                    income_zip = income_df[['zipcode', 'median_income']].copy()
                    income_zip = income_zip[income_zip['zipcode'].notna() & income_zip['median_income'].notna()]
                    income_zip = income_zip[income_zip['zipcode'].astype(str).str.len() == 5]
                    
                    merged = rent_zip.merge(income_zip, on='zipcode', how='inner')
                    merged = merged[(merged[bed_col] > 0) & (merged['median_income'] > 0)]
                    
                    merged['rent_to_income'] = merged[bed_col] / (merged['median_income'] / 12)
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
        
        # Fill to 3 items if needed
        while len(results['highest_ratio']) < 3:
            results['highest_ratio'].append({
                'zipcode': None,
                'value': None,
                'display': "Data unavailable"
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

