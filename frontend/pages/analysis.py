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
import json

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
    """Fetch median rent data by bedroom type from zip_median_rent"""
    try:
        conn = get_db_connection()
        
        # Try to find the rent table - prioritize zip_median_rent
        table_name = None
        table_query = """
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public' 
        AND (table_name = 'zip_median_rent' OR table_name LIKE '%median%rent%' OR table_name LIKE '%rent%zip%')
        ORDER BY 
            CASE 
                WHEN table_name = 'zip_median_rent' THEN 1
                WHEN table_name LIKE '%zip%rent%' THEN 2
                ELSE 3
            END,
            table_name
        LIMIT 1;
        """
        tables_df = pd.read_sql_query(table_query, conn)
        
        if tables_df.empty:
            conn.close()
            st.warning("⚠️ No median rent table found (looking for `zip_median_rent` or similar)")
            return pd.DataFrame()
        
        table_name = tables_df.iloc[0]['table_name']
        
        # Get column names
        column_query = f"""
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_name = '{table_name}'
        ORDER BY ordinal_position;
        """
        columns_df = pd.read_sql_query(column_query, conn)
        
        if columns_df.empty:
            conn.close()
            st.warning(f"⚠️ Table `{table_name}` has no columns")
            return pd.DataFrame()
        
        column_names = columns_df['column_name'].tolist()
        
        # Check if table uses bedroom_type column structure (pivoted format)
        has_bedroom_type_col = 'bedroom_type' in column_names
        has_median_rent_col = any('median_rent' in col.lower() or 'rent' in col.lower() for col in column_names)
        
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
        
        # Approach 1: If table has bedroom_type column, pivot it
        if has_bedroom_type_col and has_median_rent_col:
            # Find rent value column
            rent_val_col = None
            for col in ['median_rent_usd', 'median_rent', 'rent', 'rent_price', 'rent_usd']:
                if col in column_names:
                    rent_val_col = col
                    break
            
            if rent_val_col and zip_col:
                # Query all data
                select_cols = [zip_col, 'bedroom_type', rent_val_col]
                if borough_col:
                    select_cols.append(borough_col)
                if area_col:
                    select_cols.append(area_col)
                
                select_str = ", ".join([f'"{col}"' for col in select_cols])
                query = f"""
                SELECT {select_str}
                FROM {table_name}
                WHERE "{rent_val_col}" IS NOT NULL
                """
                
                df = pd.read_sql_query(query, conn)
                conn.close()
                
                if not df.empty:
                    # Pivot to get rent_studio, rent_1br, etc.
                    pivot_df = df.pivot_table(
                        index=zip_col,
                        columns='bedroom_type',
                        values=rent_val_col,
                        aggfunc='first'
                    ).reset_index()
                    
                    # Rename columns to rent_studio, rent_1br, etc.
                    pivot_df.columns = [f'rent_{str(col).lower().replace("+", "").replace(" ", "")}' if col != zip_col else col for col in pivot_df.columns]
                    
                    # Merge back location info
                    location_cols = [col for col in [zip_col, borough_col, area_col] if col and col != zip_col]
                    if location_cols:
                        df_location = df[[zip_col] + location_cols].drop_duplicates(subset=[zip_col])
                        pivot_df = pivot_df.merge(df_location, on=zip_col, how='left')
                    
                    df = pivot_df
                    
                    # Prepare location columns
                    if zip_col:
                        df['zipcode'] = df[zip_col].astype(str).str.extract(r'(\d{5})', expand=False)
                    if borough_col:
                        df['borough'] = df[borough_col].apply(normalize_borough_name)
                    if area_col:
                        df['area_name'] = df[area_col].astype(str)
                    
                    return df
        
        # Approach 2: Try to find separate columns for each bedroom type
        bedroom_cols = {}
        
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
        
        if not bedroom_cols:
            conn.close()
            st.warning("⚠️ Could not find bedroom type rent columns")
            st.info(f"Available columns in `{table_name}`: {column_names}")
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
        FROM {table_name}
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
        import traceback
        st.code(traceback.format_exc()[:500])
        return pd.DataFrame()

@st.cache_data(show_spinner=False, ttl=3600)
def fetch_median_income_data():
    """Fetch ZIP-level median income data - auto-detect table and columns"""
    try:
        conn = get_db_connection()
        
        # Priority order: try known table names first, then auto-detect
        priority_tables = ['noah_zip_income', 'zip_income', 'zip_median_income']
        
        # Find ZIP-level income table
        table_query = """
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public' 
        AND (table_name LIKE '%zip%income%' OR table_name LIKE '%income%zip%' OR table_name = 'noah_zip_income')
        ORDER BY 
            CASE 
                WHEN table_name = 'noah_zip_income' THEN 1
                WHEN table_name LIKE 'zip%income%' THEN 2
                ELSE 3
            END,
            table_name;
        """
        tables_df = pd.read_sql_query(table_query, conn)
        
        if tables_df.empty:
            conn.close()
            st.warning("⚠️ No ZIP-level income tables found in database")
            return pd.DataFrame()
        
        # Try priority tables first
        all_tables = tables_df['table_name'].tolist()
        # Reorder: priority tables first
        ordered_tables = []
        for priority in priority_tables:
            if priority in all_tables:
                ordered_tables.append(priority)
        # Add remaining tables
        for table in all_tables:
            if table not in ordered_tables:
                ordered_tables.append(table)
        
        # Try each table until we find one with data
        for table_name in ordered_tables:
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
                    WHERE "{zip_col}" IS NOT NULL AND "{income_col}" IS NOT NULL
                    AND "{income_col}" > 0;
                    """
                    df = pd.read_sql_query(query, conn)
                    
                    if not df.empty:
                        df['zipcode'] = df['zipcode'].astype(str).str.extract(r'(\d{5})', expand=False)
                        df = df[df['zipcode'].notna()]
                        df['median_income'] = pd.to_numeric(df['median_income'], errors='coerce')
                        df = df[df['median_income'].notna() & (df['median_income'] > 0)]
                        
                        # Filter to NYC ZIPs only (100xx-116xx)
                        df = df[df['zipcode'].str.match(r'^(10[0-9]{3}|11[0-6][0-9]{2})$', na=False)]
                        
                        if not df.empty:
                            conn.close()
                            return df
            except Exception as e:
                # Log error but continue trying other tables
                continue
        
        conn.close()
        st.warning("⚠️ Found ZIP-level income tables but no valid data")
        return pd.DataFrame()
    except Exception as e:
        st.warning(f"⚠️ Could not fetch median income data: {str(e)[:200]}")
        import traceback
        st.code(traceback.format_exc()[:500])
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

@st.cache_data(show_spinner=False, ttl=3600)
def load_zip_shapes():
    """Load ZIP code shapes from zip_shapes_geojson table"""
    try:
        conn = get_db_connection()
        query = """
        SELECT zip_code, geojson
        FROM zip_shapes_geojson
        WHERE zip_code IS NOT NULL AND geojson IS NOT NULL;
        """
        df = pd.read_sql_query(query, conn)
        conn.close()
        
        if df.empty:
            return pd.DataFrame()
        
        # Clean zip_code to 5-digit format
        df['zip_code'] = df['zip_code'].astype(str).str.extract(r'(\d{5})', expand=False)
        df = df[df['zip_code'].notna()]
        
        # Parse GeoJSON text into Python dict
        df['json_obj'] = df['geojson'].apply(json.loads)
        
        return df
    except Exception as e:
        st.warning(f"⚠️ Could not load ZIP shapes: {str(e)[:200]}")
        return pd.DataFrame()

def render_map_visualization(df, value_col, title, reverse=False, location_col='zipcode'):
    """Render a ZIP-level map visualization using GeoJSON shapes"""
    try:
        if df.empty or value_col not in df.columns:
            st.warning(f"⚠️ No data available for {title}")
            return None
        
        # Check if location_col exists
        if location_col not in df.columns:
            st.warning(f"⚠️ Location column '{location_col}' not found in data")
            return None
        
        # Filter out invalid data
        map_df = df.dropna(subset=[value_col, location_col]).copy()
        
        if map_df.empty:
            st.warning(f"⚠️ No valid data for {title}")
            return None
        
        # Clean zipcode to 5-digit format
        map_df['zipcode_clean'] = map_df[location_col].astype(str).str.extract(r'(\d{5})', expand=False)
        map_df = map_df[map_df['zipcode_clean'].notna()]
        
        if map_df.empty:
            st.warning(f"⚠️ No valid ZIP codes for {title}")
            return None
        
        # Load ZIP shapes
        zip_shapes = load_zip_shapes()
        if zip_shapes.empty:
            st.warning("⚠️ Could not load ZIP code shapes from database")
            return None
        
        # Merge data with GeoJSON shapes
        merged_df = map_df.merge(
            zip_shapes[['zip_code', 'json_obj']],
            left_on='zipcode_clean',
            right_on='zip_code',
            how='inner'
        )
        
        if merged_df.empty:
            st.warning(f"⚠️ No matching ZIP shapes found for {title}")
            return None
        
        # Create color scale based on values
        try:
            value_series = pd.to_numeric(merged_df[value_col], errors='coerce')
            valid_mask = value_series.notna()
            
            if not valid_mask.any():
                st.warning(f"⚠️ No valid numeric values in '{value_col}' column")
                return None
            
            # Create colors for all rows
            colors = ['#808080'] * len(merged_df)  # Default gray
            
            # Create color scale for valid values
            if valid_mask.sum() > 0:
                valid_values = value_series[valid_mask]
                valid_colors = create_color_scale(valid_values, reverse=reverse)
                
                # Assign colors to valid rows
                valid_positions = [i for i, is_valid in enumerate(valid_mask) if is_valid]
                for pos_idx, color in zip(valid_positions, valid_colors):
                    if pos_idx < len(colors):
                        colors[pos_idx] = color
            
            merged_df['color'] = colors
            
            # Convert hex colors to RGB
            def hex_to_rgb(hex_color):
                try:
                    if isinstance(hex_color, str):
                        hex_color = hex_color.lstrip('#')
                        if len(hex_color) == 6:
                            return [int(hex_color[i:i+2], 16) for i in (0, 2, 4)] + [180]
                    return [128, 128, 128, 180]  # Default gray
                except Exception:
                    return [128, 128, 128, 180]  # Default gray
            
            merged_df['color_rgb'] = merged_df['color'].apply(hex_to_rgb)
            
        except Exception as e:
            st.warning(f"⚠️ Error creating color scale: {str(e)[:200]}")
            merged_df['color_rgb'] = [[128, 128, 128, 180]] * len(merged_df)
        
        # Format value for tooltip
        if 'rent' in value_col.lower() and 'burden' not in value_col.lower() and 'income' not in value_col.lower():
            merged_df['value_display'] = merged_df[value_col].apply(lambda x: f"${x:,.0f}" if pd.notna(x) else "N/A")
        elif 'income' in value_col.lower():
            merged_df['value_display'] = merged_df[value_col].apply(lambda x: f"${x:,.0f}" if pd.notna(x) else "N/A")
        elif 'ratio' in value_col.lower():
            merged_df['value_display'] = merged_df[value_col].apply(lambda x: f"{x:.2f}" if pd.notna(x) else "N/A")
        else:
            merged_df['value_display'] = merged_df[value_col].apply(lambda x: f"{x:.1f}%" if pd.notna(x) else "N/A")
        
        # Prepare GeoJSON features with properties for tooltip and color
        geojson_features = []
        for idx, row in merged_df.iterrows():
            try:
                # Get the GeoJSON feature
                geojson_feat = row['json_obj']
                
                # Ensure it's a Feature object
                if isinstance(geojson_feat, dict):
                    # Add properties for tooltip and color
                    if 'properties' not in geojson_feat:
                        geojson_feat['properties'] = {}
                    
                    geojson_feat['properties']['zipcode'] = str(row.get('zipcode_clean', 'N/A'))
                    geojson_feat['properties']['value_display'] = str(row.get('value_display', 'N/A'))
                    geojson_feat['properties']['color_rgb'] = row['color_rgb']
                    
                    geojson_features.append(geojson_feat)
            except Exception:
                continue
        
        if not geojson_features:
            st.warning(f"⚠️ No valid GeoJSON features for {title}")
            return None
        
        # Create GeoJSON layer
        layer = pdk.Layer(
            "GeoJsonLayer",
            data=geojson_features,
            pickable=True,
            stroked=True,
            filled=True,
            get_fill_color="properties.color_rgb",
            get_line_color=[255, 255, 255],
            line_width_min_pixels=1,
            opacity=0.8,
        )
        
        # Create tooltip
        tooltip = {
            "html": "<b>ZIP Code:</b> {properties.zipcode}<br/><b>" + title + ":</b> {properties.value_display}",
            "style": {"backgroundColor": "#262730", "color": "white"},
        }
        
        # Use NYC-centered view state
        view_state = pdk.ViewState(
            latitude=40.7,
            longitude=-73.95,
            zoom=9,
            pitch=0
        )
        
        return pdk.Deck(
            layers=[layer],
            initial_view_state=view_state,
            tooltip=tooltip,
            map_style='mapbox://styles/mapbox/light-v9'
        )
        
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
            'highest_burden': []
        }
        
        # 1. Lowest Median Income (ZIP-level only)
        # Directly query ZIP-level income table
        try:
            conn = get_db_connection()
            
            # Find ZIP-level income table - prioritize zip_median_income
            table_query = """
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND (table_name = 'zip_median_income' OR table_name LIKE '%zip%income%' OR table_name LIKE '%income%zip%')
            ORDER BY 
                CASE 
                    WHEN table_name = 'zip_median_income' THEN 1
                    WHEN table_name LIKE '%zip%income%' THEN 2
                    ELSE 3
                END,
                table_name;
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
                        AND "{income_col}" > 10000
                        AND CAST("{zip_col}" AS TEXT) ~ '^10[0-9]{{3}}$|^11[0-6][0-9]{{2}}$'
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
        
        return results
    
    # Get top 3 critical areas data
    top3_data = prepare_top3_critical_areas()
    
    # Render Top 3 Most Critical Areas section
    st.markdown("### 📋 Top 3 Most Critical Areas")
    
    # Two-column layout
    col1, col2 = st.columns(2)
    
    # Column 1: Lowest Median Income
    with col1:
        st.markdown("**🔴 Lowest Income ZIPs**")
        st.markdown("<div style='margin-top: 0.5rem;'></div>", unsafe_allow_html=True)
        if top3_data['lowest_income']:
            for item in top3_data['lowest_income'][:3]:
                st.markdown(f"<div style='padding: 0.25rem 0; font-family: monospace;'>{item['display']}</div>", unsafe_allow_html=True)
        else:
            st.markdown("<div style='padding: 0.25rem 0; font-family: monospace;'>Data unavailable</div>", unsafe_allow_html=True)
    
    # Column 2: Highest Rent Burden
    with col2:
        st.markdown("**🔴 Highest Rent Burden ZIPs**")
        st.markdown("<div style='margin-top: 0.5rem;'></div>", unsafe_allow_html=True)
        if top3_data['highest_burden']:
            for item in top3_data['highest_burden'][:3]:
                st.markdown(f"<div style='padding: 0.25rem 0; font-family: monospace;'>{item['display']}</div>", unsafe_allow_html=True)
        else:
            st.markdown("<div style='padding: 0.25rem 0; font-family: monospace;'>Data unavailable</div>", unsafe_allow_html=True)
    
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
                map_obj = render_map_visualization(rent_df, bed_col, f"Median Rent ({bedroom_type})", reverse=True)
                if map_obj:
                    st.pydeck_chart(map_obj, use_container_width=True)
                    
                    # Add CSV download button below map
                    display_cols = [bed_col, 'zipcode']
                    if 'area_name' in rent_df.columns:
                        display_cols.append('area_name')
                    if 'borough' in rent_df.columns:
                        display_cols.append('borough')
                    display_df = rent_df[display_cols].dropna(subset=[bed_col]).sort_values(bed_col)
                    csv = display_df.to_csv(index=False)
                    st.download_button(
                        label="📥 Download Data as CSV",
                        data=csv,
                        file_name=f"median_rent_{bedroom_type.lower()}.csv",
                        mime="text/csv"
                    )
                else:
                    st.error(f"❌ Failed to render map for Median Rent ({bedroom_type}). Please check data and ZIP shapes.")
            else:
                st.warning(f"⚠️ No {bedroom_type} rent data available.")
        else:
            st.warning("⚠️ Please select a bedroom type to view median rent map.")
    
    if show_income_map:
        st.markdown("---")
        st.subheader("💰 Median Income Map")
        st.markdown("**Color Legend:** 🔴 Red = Lowest Income | 🟢 Green = Highest Income")
        if not income_df.empty and income_df['median_income'].notna().any():
            # Show data summary
            st.info(f"📊 Loaded {len(income_df)} ZIP codes with median income data. Range: ${income_df['median_income'].min():,.0f} - ${income_df['median_income'].max():,.0f}")
            
            map_obj = render_map_visualization(income_df, 'median_income', "Median Income", reverse=True)
            if map_obj:
                st.pydeck_chart(map_obj, use_container_width=True)
                
                # Add CSV download button below map
                display_cols = ['median_income', 'zipcode']
                if 'area_name' in income_df.columns:
                    display_cols.append('area_name')
                if 'borough' in income_df.columns:
                    display_cols.append('borough')
                display_df = income_df[display_cols].dropna(subset=['median_income']).sort_values('median_income')
                csv = display_df.to_csv(index=False)
                st.download_button(
                    label="📥 Download Data as CSV",
                    data=csv,
                    file_name="median_income.csv",
                    mime="text/csv"
                )
            else:
                st.error("❌ Failed to render median income map. Please check data and ZIP shapes.")
        else:
            st.warning("⚠️ No median income data available.")
            st.info("💡 **Troubleshooting:**")
            st.markdown("""
            - Ensure `zip_median_income` or similar ZIP-level income table exists in your database
            - Check that the table has `zip_code`/`zipcode` and `median_income`/`median_income_usd` columns
            - Verify data is filtered to NYC ZIP codes (100xx-116xx)
            """)
    
    if show_burden_map:
        st.markdown("---")
        st.subheader("📈 Rent Burden Map")
        st.markdown("**Color Legend:** 🟢 Green = Lowest Burden | 🔴 Red = Highest Burden")
        if not burden_df.empty and burden_df['rent_burden_rate'].notna().any():
            map_obj = render_map_visualization(burden_df, 'rent_burden_rate', "Rent Burden Rate", reverse=False)
            if map_obj:
                st.pydeck_chart(map_obj, use_container_width=True)
                
                # Add CSV download button below map
                display_df = burden_df[['rent_burden_rate', 'zipcode']].dropna(subset=['rent_burden_rate']).sort_values('rent_burden_rate', ascending=False)
                csv = display_df.to_csv(index=False)
                st.download_button(
                    label="📥 Download Data as CSV",
                    data=csv,
                    file_name="rent_burden_rate.csv",
                    mime="text/csv"
                )
            else:
                st.error("❌ Failed to render rent burden map. Please check data and ZIP shapes.")
        else:
            st.warning("⚠️ No rent burden data available.")
    
    if show_ratio_map:
        st.markdown("---")
        st.subheader(f"📉 Rent-to-Income Ratio Map - {bedroom_type}")
        st.markdown("**Color Legend:** 🟢 Green = Lower Ratio (Better) | 🔴 Red = Higher Ratio (Worse)")
        
        # Map bedroom_type to ratio column name
        ratio_col_mapping = {
            "All": "ratio_all",
            "Studio": "ratio_studio",
            "1BR": "ratio_1br",
            "2BR": "ratio_2br",
            "3+BR": "ratio_3plus"
        }
        
        ratio_col_name = ratio_col_mapping.get(bedroom_type, "ratio_all")
        
        # Fetch rent-to-income ratio data from zip_rent_income_ratio table
        try:
            conn = get_db_connection()
            
            # Find zip_rent_income_ratio table
            table_query = """
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND (table_name = 'zip_rent_income_ratio' OR table_name LIKE '%rent%income%ratio%')
            ORDER BY 
                CASE 
                    WHEN table_name = 'zip_rent_income_ratio' THEN 1
                    ELSE 2
                END,
                table_name
            LIMIT 1;
            """
            tables_df = pd.read_sql_query(table_query, conn)
            
            if not tables_df.empty:
                ratio_table = tables_df.iloc[0]['table_name']
                
                # Get column names
                col_query = f"""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = '{ratio_table}'
                ORDER BY ordinal_position;
                """
                cols_df = pd.read_sql_query(col_query, conn)
                column_names = cols_df['column_name'].tolist()
                
                # Find zip column
                zip_col = None
                for col in ['zip_code', 'zipcode', 'zip', 'postcode', 'postal_code']:
                    if col in column_names:
                        zip_col = col
                        break
                
                # Check if the requested ratio column exists
                if zip_col and ratio_col_name in column_names:
                    query = f"""
                    SELECT "{zip_col}" as zipcode, "{ratio_col_name}" as rent_to_income
                    FROM {ratio_table}
                    WHERE "{zip_col}" IS NOT NULL AND "{ratio_col_name}" IS NOT NULL;
                    """
                    ratio_df = pd.read_sql_query(query, conn)
                    conn.close()
                    
                    if not ratio_df.empty:
                        ratio_df['zipcode'] = ratio_df['zipcode'].astype(str).str.extract(r'(\d{5})', expand=False)
                        ratio_df = ratio_df[ratio_df['zipcode'].notna()]
                        ratio_df['rent_to_income'] = pd.to_numeric(ratio_df['rent_to_income'], errors='coerce')
                        ratio_df = ratio_df[ratio_df['rent_to_income'].notna() & (ratio_df['rent_to_income'] > 0)]
                        
                        # Filter to NYC ZIPs
                        ratio_df = ratio_df[ratio_df['zipcode'].str.match(r'^(10[0-9]{3}|11[0-6][0-9]{2})$', na=False)]
                        
                        if not ratio_df.empty:
                            map_obj = render_map_visualization(ratio_df, 'rent_to_income', f"Rent-to-Income Ratio ({bedroom_type})", reverse=False)
                            if map_obj:
                                st.pydeck_chart(map_obj, use_container_width=True)
                                
                                # Add CSV download button below map
                                display_df = ratio_df[['rent_to_income', 'zipcode']].sort_values('rent_to_income', ascending=False)
                                csv = display_df.to_csv(index=False)
                                st.download_button(
                                    label="📥 Download Data as CSV",
                                    data=csv,
                                    file_name=f"rent_to_income_ratio_{bedroom_type.lower().replace('+', 'plus')}.csv",
                                    mime="text/csv"
                                )
                            else:
                                st.error(f"❌ Failed to render rent-to-income ratio map. Please check data and ZIP shapes.")
                        else:
                            st.warning("⚠️ No valid rent-to-income ratio data after filtering.")
                    else:
                        st.warning(f"⚠️ No data found for {bedroom_type} in rent-to-income ratio table.")
                else:
                    conn.close()
                    if not zip_col:
                        st.warning(f"⚠️ Could not find zip column in {ratio_table}")
                    elif ratio_col_name not in column_names:
                        st.warning(f"⚠️ Column '{ratio_col_name}' not found in {ratio_table}. Available columns: {column_names}")
            else:
                conn.close()
                st.warning("⚠️ Table `zip_rent_income_ratio` not found. Please ensure the table exists in your database.")
        except Exception as e:
            st.warning(f"⚠️ Could not fetch rent-to-income ratio data: {str(e)[:200]}")
            import traceback
            st.code(traceback.format_exc()[:500])

if __name__ == "__main__":
    render_analysis_page()

