"""
Analysis Page
Displays multiple map visualizations for median rent, income, and rent burden
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

def filter_to_nyc_zip(df, zip_col="zipcode"):
    """
    Filter DataFrame to only include NYC ZIP codes.
    
    NYC ZIP codes generally fall into these prefixes:
    100, 101, 102, 103, 104, 110, 111, 112, 113, 114, 116
    
    Args:
        df: DataFrame to filter
        zip_col: Name of the ZIP code column (default: "zipcode")
    
    Returns:
        Filtered DataFrame containing only NYC ZIP codes
    """
    if df.empty or zip_col not in df.columns:
        return df
    
    # Convert to string and extract 5-digit ZIP codes
    df = df.copy()
    df[zip_col] = df[zip_col].astype(str).str.extract(r'(\d{5})', expand=False)
    
    # Filter to NYC ZIP codes: 10000-11699
    # This covers: 100xx, 101xx, 102xx, 103xx, 104xx, 110xx, 111xx, 112xx, 113xx, 114xx, 116xx
    nyc_pattern = r'^(10[0-9]{3}|11[0-6][0-9]{2})$'
    df = df[df[zip_col].str.match(nyc_pattern, na=False)]
    
    return df

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
                    # Apply NYC ZIP filter before processing
                    df = filter_to_nyc_zip(df, zip_col)
                    
                    if df.empty:
                        conn.close()
                        return pd.DataFrame()
                    
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
        
        # Filter to NYC ZIPs only using helper function
        df = filter_to_nyc_zip(df, 'zipcode')
        
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
        # Updated: prioritize zip_median_income as the main table
        priority_tables = ['zip_median_income', 'noah_zip_income', 'zip_income']
        
        # Find ZIP-level income table
        # Updated: prioritize zip_median_income
        table_query = """
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public' 
        AND (table_name = 'zip_median_income' OR table_name LIKE '%zip%income%' OR table_name LIKE '%income%zip%' OR table_name = 'noah_zip_income')
        ORDER BY 
            CASE 
                WHEN table_name = 'zip_median_income' THEN 1
                WHEN table_name = 'noah_zip_income' THEN 2
                WHEN table_name LIKE 'zip%income%' THEN 3
                ELSE 4
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
                
                # Find zip, income, and borough columns
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
                
                borough_col = None
                for col in ['borough', 'borough_name', 'county', 'county_name']:
                    if col in column_names:
                        borough_col = col
                        break
                
                if zip_col and income_col:
                    # Build SELECT clause
                    select_cols = [f'"{zip_col}" as zipcode', f'"{income_col}" as median_income']
                    if borough_col:
                        select_cols.append(f'"{borough_col}" as borough')
                    select_str = ", ".join(select_cols)
                    
                    query = f"""
                    SELECT {select_str}
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
                        
                        # Add borough column if available
                        if borough_col and 'borough' in df.columns:
                            # Normalize borough names
                            df['borough'] = df['borough'].apply(normalize_borough_name)
                        elif borough_col:
                            # If borough_col was detected but column doesn't exist after query, try to get it again
                            # This shouldn't happen, but handle it gracefully
                            pass
                        
                        # Filter to NYC ZIPs only using helper function
                        df = filter_to_nyc_zip(df, 'zipcode')
                        
                        if not df.empty:
                            conn.close()
                            # Debug: Check if borough column exists
                            if 'borough' not in df.columns and borough_col:
                                # Try to add borough column by re-querying if needed
                                # But for now, just return what we have
                                pass
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
    """Fetch ZIP-level rent burden data from zip_rent_burden_ny table only"""
    try:
        conn = get_db_connection()
        
        # Only use zip_rent_burden_ny table
        table_name = 'zip_rent_burden_ny'
        
        # Check if table exists
        table_check = """
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public' 
        AND table_name = 'zip_rent_burden_ny';
        """
        tables_df = pd.read_sql_query(table_check, conn)
        
        if tables_df.empty:
            conn.close()
            st.warning("⚠️ Table `zip_rent_burden_ny` not found")
            return pd.DataFrame()
        
        # Get columns
        col_query = f"""
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_name = '{table_name}'
        ORDER BY ordinal_position;
        """
        cols_df = pd.read_sql_query(col_query, conn)
        column_names = cols_df['column_name'].tolist()
        
        # Find zip, burden, and borough columns
        zip_col = None
        for col in ['zip_code', 'zipcode', 'zip', 'postcode', 'postal_code', 'zcta']:
            if col in column_names:
                zip_col = col
                break
        
        borough_col = None
        for col in ['borough', 'borough_name', 'county', 'county_name']:
            if col in column_names:
                borough_col = col
                break
        
        burden_col = None
        for col in ['rent_burden_rate', 'burden_rate', 'rent_burden', 'burden']:
            if col in column_names:
                burden_col = col
                break
        
        if zip_col and burden_col:
            # Build SELECT clause including borough if available
            select_cols = [f'"{zip_col}" as zipcode', f'"{burden_col}" as rent_burden_rate']
            if borough_col:
                select_cols.append(f'"{borough_col}" as borough')
            select_str = ", ".join(select_cols)
            
            query = f"""
            SELECT {select_str}
            FROM {table_name}
            WHERE "{zip_col}" IS NOT NULL AND "{burden_col}" IS NOT NULL;
            """
            df = pd.read_sql_query(query, conn)
            conn.close()
            
            if not df.empty:
                df['zipcode'] = df['zipcode'].astype(str).str.extract(r'(\d{5})', expand=False)
                df = df[df['zipcode'].notna()]
                # Add borough column if available
                if borough_col and 'borough' in df.columns:
                    df['borough'] = df['borough'].apply(normalize_borough_name)
                # Filter to NYC ZIPs only using helper function
                df = filter_to_nyc_zip(df, 'zipcode')
                df['rent_burden_rate'] = pd.to_numeric(df['rent_burden_rate'], errors='coerce')
                # If values are < 1, convert from decimal to percentage
                if not df.empty and df['rent_burden_rate'].max() < 1:
                    df['rent_burden_rate'] = df['rent_burden_rate'] * 100
                df = df[df['rent_burden_rate'].notna()]
                return df
        else:
            conn.close()
            st.warning(f"⚠️ Could not find zip or burden columns in {table_name}")
            return pd.DataFrame()
        
        conn.close()
        return pd.DataFrame()
    except Exception as e:
        st.warning(f"⚠️ Could not fetch rent burden data: {str(e)[:200]}")
        return pd.DataFrame()

def create_color_scale(values, reverse=False, is_rent_burden=False):
    """
    Create color scale: red for worst/low, green for best/high
    
    Args:
        values: Series of numeric values
        reverse: If True, high values = red (worst), low values = green (best)
        is_rent_burden: If True, use special rent burden color logic:
            - <30%: Green (darker green for lower values, lighter/yellowish for higher)
            - 30-50%: Yellow (lighter yellow for lower, more orange for higher)
            - >50%: Red (lighter red for lower, darker red for higher)
    """
    if values.empty or values.isna().all():
        return ['#808080'] * len(values)
    
    min_val = values.min()
    max_val = values.max()
    
    if min_val == max_val:
        return ['#808080'] * len(values)
    
    if is_rent_burden:
        # Special color logic for rent burden based on percentage thresholds
        colors = []
        for val in values:
            if pd.isna(val):
                colors.append('#808080')
            elif val < 30:
                # Green range: <30%
                # Lower values = darker green, higher values = lighter green/yellowish
                # Map 0-30% to green gradient: #1a9850 (dark green) to #91cf60 (light green) to #fee08b (yellow-green)
                normalized = (val - min_val) / max(30 - min_val, 1)  # Normalize within 0-30 range
                if normalized < 0.33:
                    # Dark green for very low values (0-10%)
                    colors.append('#1a9850')
                elif normalized < 0.67:
                    # Medium green (10-20%)
                    colors.append('#66c2a5')
                else:
                    # Light green/yellowish (20-30%)
                    colors.append('#fee08b')
            elif val < 50:
                # Yellow range: 30-50%
                # Lower values = lighter yellow, higher values = more orange
                # Map 30-50% to yellow-orange gradient
                normalized = (val - 30) / 20  # Normalize within 30-50 range
                if normalized < 0.5:
                    # Yellow (30-40%)
                    colors.append('#fee08b')
                else:
                    # Orange-yellow (40-50%)
                    colors.append('#fc8d59')
            else:
                # Red range: >50%
                # Lower values = lighter red, higher values = darker red
                # Map 50%+ to red gradient: #fc8d59 (orange-red) to #d73027 (dark red)
                if max_val > 50:
                    normalized = (val - 50) / max(max_val - 50, 1)  # Normalize within 50-max range
                else:
                    normalized = 0
                
                if normalized < 0.33:
                    # Light red/orange (50-60%)
                    colors.append('#fc8d59')
                elif normalized < 0.67:
                    # Medium red (60-70%)
                    colors.append('#e34a33')
                else:
                    # Dark red (70%+)
                    colors.append('#d73027')
        return colors
    
    # Original logic for other metrics
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
    """Load ZIP code shapes from zip_shapes_nyc table (NYC-only), with fallback to zip_shapes_geojson"""
    try:
        conn = get_db_connection()
        
        # Try zip_shapes_nyc first (NYC-only table)
        try:
            query = """
            SELECT zip_code, geojson
            FROM zip_shapes_nyc
            WHERE zip_code IS NOT NULL AND geojson IS NOT NULL;
            """
            df = pd.read_sql_query(query, conn)
            conn.close()
            
            if not df.empty:
                # Clean zip_code to 5-digit format
                df['zip_code'] = df['zip_code'].astype(str).str.extract(r'(\d{5})', expand=False)
                df = df[df['zip_code'].notna()]
                
                # Parse GeoJSON text into Python dict
                df['json_obj'] = df['geojson'].apply(json.loads)
                
                return df
        except Exception:
            # Table doesn't exist, fall back to zip_shapes_geojson with filtering
            pass
        
        # Fallback: Use zip_shapes_geojson and filter to NYC ZIPs
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
        
        # Filter to NYC ZIPs only (10000-11699)
        df = filter_to_nyc_zip(df, 'zip_code')
        
        # Parse GeoJSON text into Python dict
        df['json_obj'] = df['geojson'].apply(json.loads)
        
        return df
    except Exception as e:
        st.warning(f"⚠️ Could not load ZIP shapes: {str(e)[:200]}")
        return pd.DataFrame()

def render_map_visualization(df, value_col, title, reverse=False, location_col='zipcode', show_nyc_boundary=False):
    """Render a ZIP-level map visualization using GeoJSON shapes
    
    Args:
        df: DataFrame with data to display
        value_col: Column name for the value to visualize
        title: Title for the map
        reverse: Whether to reverse the color scale
        location_col: Column name for ZIP code/location
        show_nyc_boundary: Whether to show NYC boundary box outline
    """
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
        
        # Clean zipcode to 5-digit format and filter to NYC ZIPs only
        map_df['zipcode_clean'] = map_df[location_col].astype(str).str.extract(r'(\d{5})', expand=False)
        map_df = map_df[map_df['zipcode_clean'].notna()]
        
        # Filter to NYC ZIP codes only (100xx-116xx)
        map_df = filter_to_nyc_zip(map_df, 'zipcode_clean')
        
        if map_df.empty:
            st.warning(f"⚠️ No valid NYC ZIP codes for {title}")
            return None
        
        # Load ZIP shapes
        zip_shapes = load_zip_shapes()
        if zip_shapes.empty:
            st.warning("⚠️ Could not load ZIP code shapes from database")
            return None
        
        # Merge data with GeoJSON shapes
        # Use left join to include all NYC ZIP shapes, even if they don't have data
        # This ensures the full NYC outline is visible with gray for missing data
        # Note: After merge, zip_code from zip_shapes will be preserved
        # Preserve all columns from map_df, including borough if it exists
        merged_df = zip_shapes[['zip_code', 'json_obj']].merge(
            map_df,
            left_on='zip_code',
            right_on='zipcode_clean',
            how='left',
            suffixes=('_shapes', '_data')  # Use clearer suffixes to avoid conflicts
        )
        
        # Ensure zip_code is preserved (it should be from zip_shapes)
        # After merge with suffixes, zip_code from shapes becomes zip_code_shapes
        # Check for both possible column names
        if 'zip_code_shapes' in merged_df.columns:
            merged_df['zip_code'] = merged_df['zip_code_shapes']
        elif 'zip_code' not in merged_df.columns:
            st.error(f"❌ zip_code column lost during merge. Available columns: {merged_df.columns.tolist()}")
            return None
        
        if merged_df.empty:
            st.warning(f"⚠️ No ZIP shapes available for {title}")
            return None
        
        # Fill missing values with NaN so they show as gray
        if value_col not in merged_df.columns:
            merged_df[value_col] = None
        
        # Ensure zipcode_clean exists for all rows (use zip_code from shapes)
        # After left join with suffixes, zip_code from shapes becomes zip_code_shapes
        # Create zipcode_clean from the correct column
        if 'zip_code_shapes' in merged_df.columns:
            merged_df['zipcode_clean'] = merged_df['zip_code_shapes']
        elif 'zip_code' in merged_df.columns:
            merged_df['zipcode_clean'] = merged_df['zip_code']
        else:
            st.error(f"❌ Missing zip_code column after merge. Available columns: {merged_df.columns.tolist()}")
            return None
        
        # Fill any NaN values in zipcode_clean
        if 'zip_code_shapes' in merged_df.columns:
            merged_df['zipcode_clean'] = merged_df['zipcode_clean'].fillna(merged_df['zip_code_shapes'])
        elif 'zip_code' in merged_df.columns:
            merged_df['zipcode_clean'] = merged_df['zipcode_clean'].fillna(merged_df['zip_code'])
        
        # Create color scale based on values
        try:
            value_series = pd.to_numeric(merged_df[value_col], errors='coerce')
            valid_mask = value_series.notna()
            
            # Create colors for all rows - default gray for missing data
            colors = ['#808080'] * len(merged_df)  # Default gray for missing data
            
            # Create color scale for valid values only
            # Check if this is rent burden data (has 'burden' in value_col name)
            is_rent_burden = 'burden' in value_col.lower() and 'rent' in value_col.lower()
            
            if valid_mask.sum() > 0:
                valid_values = value_series[valid_mask]
                valid_colors = create_color_scale(valid_values, reverse=reverse, is_rent_burden=is_rent_burden)
                
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
        # Handle missing values - show "No data" for ZIPs without data
        if 'rent' in value_col.lower() and 'burden' not in value_col.lower() and 'income' not in value_col.lower():
            merged_df['value_display'] = merged_df[value_col].apply(lambda x: f"${x:,.0f}" if pd.notna(x) else "No data")
        elif 'income' in value_col.lower():
            merged_df['value_display'] = merged_df[value_col].apply(lambda x: f"${x:,.0f}" if pd.notna(x) else "No data")
        elif 'ratio' in value_col.lower():
            merged_df['value_display'] = merged_df[value_col].apply(lambda x: f"{x:.2f}" if pd.notna(x) else "No data")
        else:
            merged_df['value_display'] = merged_df[value_col].apply(lambda x: f"{x:.1f}%" if pd.notna(x) else "No data")
        
        # Prepare GeoJSON features with properties for tooltip and color
        geojson_features = []
        for idx, row in merged_df.iterrows():
            try:
                # Get the GeoJSON feature
                geojson_feat = row['json_obj']
                
                # Ensure it's a Feature object
                if isinstance(geojson_feat, dict):
                    # Add/update properties for tooltip and color
                    if 'properties' not in geojson_feat:
                        geojson_feat['properties'] = {}
                    
                    # Store values in properties for tooltip
                    # Get zipcode from the merged dataframe - prioritize zip_code from shapes
                    zipcode_val = str(row.get('zip_code_shapes', row.get('zip_code', row.get('zipcode_clean', row.get('zipcode', 'N/A')))))
                    value_display_val = str(row.get('value_display', 'N/A'))
                    
                    # Get borough if available (check both possible column names after merge)
                    borough_val = row.get('borough', row.get('borough_data', None))
                    if pd.notna(borough_val) and str(borough_val) not in ['N/A', 'nan', 'None', '']:
                        borough_display = str(borough_val)
                    else:
                        borough_display = None
                    
                    # Ensure we have valid values
                    if zipcode_val == 'N/A' or zipcode_val == 'nan' or zipcode_val == 'None':
                        zipcode_val = str(row.get('zip_code_shapes', row.get('zip_code', row.get('zipcode_clean', 'N/A'))))
                    if value_display_val == 'N/A' or value_display_val == 'nan' or value_display_val == 'None':
                        value_display_val = 'No data'
                    
                    # Ensure properties dict exists and is properly set
                    if 'properties' not in geojson_feat:
                        geojson_feat['properties'] = {}
                    
                    # Set properties for tooltip access
                    # PyDeck GeoJsonLayer tooltip uses {properties.field_name} format
                    # Ensure values are clean strings
                    if pd.isna(zipcode_val) or zipcode_val == 'nan' or zipcode_val == 'None':
                        zipcode_val = str(row.get('zip_code_shapes', row.get('zip_code', 'N/A')))
                    if pd.isna(value_display_val) or value_display_val == 'nan' or value_display_val == 'None':
                        value_display_val = 'No data'
                    
                    # Set properties for tooltip - PyDeck accesses properties directly
                    # Ensure all values are clean strings and not NaN/None
                    geojson_feat['properties']['zipcode'] = str(zipcode_val) if zipcode_val and str(zipcode_val) not in ['N/A', 'nan', 'None', ''] else 'N/A'
                    geojson_feat['properties']['value_display'] = str(value_display_val) if value_display_val and str(value_display_val) not in ['N/A', 'nan', 'None', ''] else 'No data'
                    geojson_feat['properties']['color_rgb'] = row['color_rgb']
                    
                    # Add borough to properties if available
                    if borough_display:
                        geojson_feat['properties']['borough'] = str(borough_display)
                    
                    # PyDeck GeoJsonLayer tooltip accesses properties directly, so we need to ensure
                    # the properties are set correctly. Some versions may need direct property access.
                    # Set at top level as well for compatibility
                    geojson_feat['zipcode'] = geojson_feat['properties']['zipcode']
                    geojson_feat['value_display'] = geojson_feat['properties']['value_display']
                    if borough_display:
                        geojson_feat['borough'] = geojson_feat['properties']['borough']
                    
                    geojson_features.append(geojson_feat)
            except Exception as e:
                continue
        
        if not geojson_features:
            st.warning(f"⚠️ No valid GeoJSON features for {title}")
            return None
        
        # Create GeoJSON layer for ZIP codes
        layers = []
        
        zip_layer = pdk.Layer(
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
        layers.append(zip_layer)
        
        # Add NYC boundary box outline if requested
        if show_nyc_boundary:
            # NYC approximate bounding box coordinates
            # North: 40.9176, South: 40.4774, East: -73.7004, West: -74.2591
            nyc_boundary = {
                "type": "Feature",
                "properties": {},
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [[
                        [-74.2591, 40.4774],  # Southwest
                        [-73.7004, 40.4774],  # Southeast
                        [-73.7004, 40.9176],  # Northeast
                        [-74.2591, 40.9176],  # Northwest
                        [-74.2591, 40.4774]   # Close polygon
                    ]]
                }
            }
            
            boundary_layer = pdk.Layer(
                "GeoJsonLayer",
                data=[nyc_boundary],
                pickable=False,
                stroked=True,
                filled=False,
                get_line_color=[0, 0, 0, 255],  # Black outline
                line_width_min_pixels=2,
                opacity=1.0,
            )
            layers.append(boundary_layer)
        
        # Create tooltip - PyDeck GeoJsonLayer tooltip syntax
        # PyDeck GeoJsonLayer tooltip can access properties directly
        # Try multiple syntax formats for compatibility
        # Format 1: Direct property access (most common)
        tooltip_html = "<b>ZIP Code:</b> {zipcode}<br/><b>" + title + ":</b> {value_display}"
        # Check if any feature has borough property
        has_borough = any('borough' in feat.get('properties', {}) for feat in geojson_features if isinstance(feat, dict))
        if has_borough:
            tooltip_html += "<br/><b>Borough:</b> {borough}"
        
        tooltip = {
            "html": tooltip_html,
            "style": {"backgroundColor": "#262730", "color": "white", "fontSize": "12px"},
        }
        
        # Use NYC-centered view state (automatically centered on NYC)
        # NYC approximate center: latitude 40.7, longitude -73.95
        view_state = pdk.ViewState(
            latitude=40.7,
            longitude=-73.95,
            zoom=10,  # Slightly zoomed in to focus on NYC
            pitch=0
        )
        
        return pdk.Deck(
            layers=layers,
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
            options=["Studio", "1BR", "2BR", "3+BR"],
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
            options=["Median Rent", "Median Income", "Rent Burden"],
            index=0
        )
    
    # Load data (for maps - show all NYC data, not filtered by location)
    with st.spinner("Loading data..."):
        rent_df = fetch_median_rent_data()
        income_df = fetch_median_income_data()
        burden_df = fetch_rent_burden_analysis_data()
    
    # Note: Maps display all NYC data, location_filter is only used for Value Lookup
    
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
            
            # Only use zip_rent_burden_ny table
            table_name = 'zip_rent_burden_ny'
            
            # Check if table exists
            table_check = """
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_name = 'zip_rent_burden_ny';
            """
            tables_df = pd.read_sql_query(table_check, conn)
            
            if not tables_df.empty:
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
                        AND CAST("{zip_col}" AS TEXT) ~ '^10[0-9]{{3}}$|^11[0-6][0-9]{{2}}$'
                        ORDER BY "{burden_col}" DESC
                        LIMIT 3;
                        """
                        burden_zip = pd.read_sql_query(query, conn)
                except Exception:
                    burden_zip = pd.DataFrame()
            else:
                burden_zip = pd.DataFrame()
            
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
    
    # Value Lookup - shows value based on three filters
    st.markdown("### 📋 Value Lookup")
    
    # Check if all three filters are selected
    if bedroom_type and location_filter and map_type:
        # Extract ZIP code from location filter
        zip_match = None
        if location_filter:
            import re
            zip_pattern = r'\b(\d{5})\b'
            zip_matches = re.findall(zip_pattern, location_filter)
            if zip_matches:
                zip_match = zip_matches[0]
        
        # Try to get value based on map_type
        value_display = None
        value_label = None
        
        try:
            if map_type == "Median Rent":
                # Fetch median rent data
                if not rent_df.empty and zip_match:
                    # Map bedroom type to column
                    bed_col_map = {
                        "Studio": "rent_studio",
                        "1BR": "rent_1br",
                        "2BR": "rent_2br",
                        "3+BR": "rent_3plus"
                    }
                    bed_col = bed_col_map.get(bedroom_type)
                    if bed_col and bed_col in rent_df.columns:
                        zip_data = rent_df[rent_df['zipcode'] == zip_match]
                        if not zip_data.empty:
                            value = zip_data[bed_col].iloc[0]
                            if pd.notna(value):
                                value_display = f"${value:,.0f}"
                                value_label = f"Median Rent ({bedroom_type})"
            
            elif map_type == "Median Income":
                # Check if location is "All NYC" or similar
                NYC_MEDIAN_INCOME = 79713
                location_lower = location_filter.lower().strip() if location_filter else ""
                
                # Check if user is asking for NYC-wide value
                if any(keyword in location_lower for keyword in ['all nyc', 'nyc', 'new york city', 'entire nyc', 'citywide']):
                    value_display = f"${NYC_MEDIAN_INCOME:,}"
                    value_label = "NYC-Wide Median Income"
                elif not income_df.empty:
                    # Check if location is a borough name
                    borough_names = {
                        'manhattan': 'Manhattan',
                        'brooklyn': 'Brooklyn',
                        'queens': 'Queens',
                        'bronx': 'Bronx',
                        'staten island': 'Staten Island'
                    }
                    
                    matched_borough = None
                    for key, borough in borough_names.items():
                        if key in location_lower:
                            matched_borough = borough
                            break
                    
                    if matched_borough and 'borough' in income_df.columns:
                        # Calculate borough-level median income (average of all ZIPs in that borough)
                        borough_data = income_df[income_df['borough'] == matched_borough]
                        if not borough_data.empty:
                            # Use median (not mean) for more accurate representation
                            borough_median = borough_data['median_income'].median()
                            if pd.notna(borough_median):
                                value_display = f"${borough_median:,.0f}"
                                value_label = f"Median Income ({matched_borough})"
                    elif zip_match:
                        # Try ZIP code match
                        zip_data = income_df[income_df['zipcode'] == zip_match]
                        if not zip_data.empty:
                            value = income_df['median_income'].iloc[0]
                            if pd.notna(value):
                                value_display = f"${value:,.0f}"
                                value_label = "Median Income"
            
            elif map_type == "Rent Burden":
                # Fetch rent burden data
                if not burden_df.empty and zip_match:
                    zip_data = burden_df[burden_df['zipcode'] == zip_match]
                    if not zip_data.empty:
                        value = burden_df['rent_burden_rate'].iloc[0]
                        if pd.notna(value):
                            value_display = f"{value:.1f}%"
                            value_label = "Rent Burden Rate"
            
        
        except Exception as e:
            st.warning(f"⚠️ Error fetching value: {str(e)[:200]}")
        
        # Display the value in a box
        if value_display:
            st.success(f"""
            **{value_label}**  
            **Location:** {location_filter}  
            **Value:** {value_display}
            """)
        else:
            st.info("ℹ️ No data found for the selected filters. Please check your location input and ensure data exists for that ZIP code.")
    else:
        st.info("ℹ️ Please select all three filters (Bedroom Type, Location, and Map Type) to see the value.")
    
    st.divider()
    
    # Critical Areas List - Most Severe ZIP Codes
    st.markdown("### 🔴 Most Critical Areas")
    st.markdown("View the ZIP codes with the lowest median income or highest rent burden.")
    
    # Filters for critical areas
    col1, col2, col3 = st.columns(3)
    
    with col1:
        metric_type = st.selectbox(
            "Metric Type",
            options=["Lowest Median Income", "Highest Rent Burden"],
            index=0,
            key="critical_metric"
        )
    
    with col2:
        borough_filter = st.selectbox(
            "Borough",
            options=["All NYC", "Manhattan", "Brooklyn", "Queens", "Bronx", "Staten Island"],
            index=0,
            key="critical_borough"
        )
    
    with col3:
        num_results = st.number_input(
            "Number of Results",
            min_value=1,
            max_value=50,
            value=3,
            step=1,
            key="critical_num_results"
        )
    
    # Function to get critical ZIP codes
    def get_critical_zip_codes(metric_type, borough_filter, num_results):
        """Get the most critical ZIP codes based on metric type and borough filter"""
        try:
            conn = get_db_connection()
            results = []
            
            # Borough ZIP ranges for fallback (if borough column not available)
            borough_zip_ranges = {
                "Manhattan": r'^(10[0-2][0-9]{2})$',
                "Brooklyn": r'^(11[2-3][0-9]{2})$',
                "Queens": r'^(11[0-1][0-9]{2}|114[0-9]{2})$',
                "Bronx": r'^(104[0-9]{2})$',
                "Staten Island": r'^(103[0-9]{2})$'
            }
            
            if metric_type == "Lowest Median Income":
                # Find ZIP-level income table
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
                    table_name
                LIMIT 1;
                """
                tables_df = pd.read_sql_query(table_query, conn)
                
                if not tables_df.empty:
                    table_name = tables_df.iloc[0]['table_name']
                    
                    # Get columns
                    col_query = f"""
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_name = '{table_name}'
                    ORDER BY ordinal_position;
                    """
                    cols_df = pd.read_sql_query(col_query, conn)
                    column_names = cols_df['column_name'].tolist()
                    
                    zip_col = None
                    income_col = None
                    borough_col = None
                    
                    for col in ['zip_code', 'zipcode', 'zip', 'postcode', 'postal_code']:
                        if col in column_names:
                            zip_col = col
                            break
                    
                    for col in ['median_income_usd', 'median_income', 'median_household_income', 'income']:
                        if col in column_names:
                            income_col = col
                            break
                    
                    for col in ['borough', 'borough_name', 'county', 'county_name']:
                        if col in column_names:
                            borough_col = col
                            break
                    
                    if zip_col and income_col:
                        # Build query with borough filtering
                        # For "All NYC": query all data, then sort and take top N
                        # For specific borough: filter by borough column directly
                        if borough_filter == "All NYC":
                            # Query all NYC data, sort, then limit
                            query = f"""
                            SELECT "{zip_col}" as zipcode, "{income_col}" as median_income
                            FROM {table_name}
                            WHERE "{zip_col}" IS NOT NULL 
                            AND "{income_col}" IS NOT NULL
                            AND "{income_col}" > 10000
                            AND CAST("{zip_col}" AS TEXT) ~ '^(10[0-9]{{3}}|11[0-6][0-9]{{2}})$'
                            ORDER BY "{income_col}" ASC
                            LIMIT {num_results};
                            """
                        else:
                            # Filter by borough column directly (if available)
                            if borough_col:
                                # First, check if borough column has any non-null values
                                check_query = f"""
                                SELECT COUNT(*) as total_count,
                                       COUNT(DISTINCT "{borough_col}") as distinct_boroughs,
                                       COUNT(CASE WHEN "{borough_col}" IS NOT NULL AND TRIM("{borough_col}") != '' THEN 1 END) as non_null_count
                                FROM {table_name}
                                WHERE "{zip_col}" IS NOT NULL 
                                AND "{income_col}" IS NOT NULL
                                AND "{income_col}" > 10000;
                                """
                                check_df = pd.read_sql_query(check_query, conn)
                                
                                # If borough column is mostly empty, fall back to ZIP pattern
                                if not check_df.empty and check_df.iloc[0]['non_null_count'] == 0:
                                    # Borough column exists but is empty, use ZIP pattern fallback
                                    borough_zip_ranges = {
                                        "Manhattan": r'^(10[0-2][0-9]{2})$',
                                        "Brooklyn": r'^(11[2-3][0-9]{2})$',
                                        "Queens": r'^(11[0-1][0-9]{2}|114[0-9]{2})$',
                                        "Bronx": r'^(104[0-9]{2})$',
                                        "Staten Island": r'^(103[0-9]{2})$'
                                    }
                                    zip_pattern = borough_zip_ranges.get(borough_filter, r'^(10[0-9]{3}|11[0-6][0-9]{2})$')
                                    query = f"""
                                    SELECT "{zip_col}" as zipcode, "{income_col}" as median_income
                                    FROM {table_name}
                                    WHERE "{zip_col}" IS NOT NULL 
                                    AND "{income_col}" IS NOT NULL
                                    AND "{income_col}" > 10000
                                    AND CAST("{zip_col}" AS TEXT) ~ '{zip_pattern}'
                                    ORDER BY "{income_col}" ASC
                                    LIMIT {num_results};
                                    """
                                else:
                                    # Borough column has data, try to match
                                    # Normalize borough filter to match database values
                                    # Try multiple variations of borough names
                                    borough_variations = {
                                        "Manhattan": ["manhattan", "new york", "new york county"],
                                        "Brooklyn": ["brooklyn", "kings", "kings county"],
                                        "Queens": ["queens", "queens county"],
                                        "Bronx": ["bronx", "bronx county"],
                                        "Staten Island": ["staten island", "richmond", "richmond county"]
                                    }
                                    
                                    # Build WHERE clause with multiple OR conditions for borough matching
                                    borough_conditions = []
                                    if borough_filter in borough_variations:
                                        for variant in borough_variations[borough_filter]:
                                            borough_conditions.append(f"LOWER(TRIM(\"{borough_col}\")) = LOWER('{variant}')")
                                    
                                    # Also try exact match with normalized name
                                    borough_conditions.append(f"LOWER(TRIM(\"{borough_col}\")) = LOWER('{borough_filter}')")
                                    
                                    borough_where = "(" + " OR ".join(borough_conditions) + ")" if borough_conditions else f"LOWER(TRIM(\"{borough_col}\")) = LOWER('{borough_filter}')"
                                    
                                    query = f"""
                                    SELECT "{zip_col}" as zipcode, "{income_col}" as median_income
                                    FROM {table_name}
                                    WHERE "{zip_col}" IS NOT NULL 
                                    AND "{income_col}" IS NOT NULL
                                    AND "{income_col}" > 10000
                                    AND "{borough_col}" IS NOT NULL
                                    AND TRIM("{borough_col}") != ''
                                    AND {borough_where}
                                    ORDER BY "{income_col}" ASC
                                    LIMIT {num_results};
                                    """
                            else:
                                # Fallback to ZIP pattern if borough column not available
                                borough_zip_ranges = {
                                    "Manhattan": r'^(10[0-2][0-9]{2})$',
                                    "Brooklyn": r'^(11[2-3][0-9]{2})$',
                                    "Queens": r'^(11[0-1][0-9]{2}|114[0-9]{2})$',
                                    "Bronx": r'^(104[0-9]{2})$',
                                    "Staten Island": r'^(103[0-9]{2})$'
                                }
                                zip_pattern = borough_zip_ranges.get(borough_filter, r'^(10[0-9]{3}|11[0-6][0-9]{2})$')
                                query = f"""
                                SELECT "{zip_col}" as zipcode, "{income_col}" as median_income
                                FROM {table_name}
                                WHERE "{zip_col}" IS NOT NULL 
                                AND "{income_col}" IS NOT NULL
                                AND "{income_col}" > 10000
                                AND CAST("{zip_col}" AS TEXT) ~ '{zip_pattern}'
                                ORDER BY "{income_col}" ASC
                                LIMIT {num_results};
                                """
                        df = pd.read_sql_query(query, conn)
                        
                        if not df.empty:
                            df['zipcode'] = df['zipcode'].astype(str).str.extract(r'(\d{5})', expand=False)
                            df = df[df['zipcode'].notna()]
                            df['median_income'] = pd.to_numeric(df['median_income'], errors='coerce')
                            df = df[df['median_income'].notna() & (df['median_income'] > 10000)]
                            
                            for _, row in df.iterrows():
                                zipcode = str(row['zipcode']).strip()[:5]
                                income = float(row['median_income'])
                                results.append({
                                    'zipcode': zipcode,
                                    'value': income,
                                    'display': f"ZIP {zipcode} — ${income:,.0f}"
                                })
            
            elif metric_type == "Highest Rent Burden":
                # Use zip_rent_burden_ny table
                table_name = 'zip_rent_burden_ny'
                
                # Check if table exists
                table_check = """
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = 'zip_rent_burden_ny';
                """
                tables_df = pd.read_sql_query(table_check, conn)
                
                if not tables_df.empty:
                    # Get columns
                    col_query = f"""
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_name = '{table_name}'
                    ORDER BY ordinal_position;
                    """
                    cols_df = pd.read_sql_query(col_query, conn)
                    column_names = cols_df['column_name'].tolist()
                    
                    zip_col = None
                    burden_col = None
                    borough_col = None
                    
                    for col in ['zip_code', 'zipcode', 'zip', 'postcode', 'postal_code']:
                        if col in column_names:
                            zip_col = col
                            break
                    
                    for col in ['rent_burden_rate', 'burden_rate', 'rent_burden', 'burden']:
                        if col in column_names:
                            burden_col = col
                            break
                    
                    for col in ['borough', 'borough_name', 'county', 'county_name']:
                        if col in column_names:
                            borough_col = col
                            break
                    
                    if zip_col and burden_col:
                        # Build query with borough filtering
                        # For "All NYC": query all data, then sort and take top N
                        # For specific borough: filter by borough column directly
                        if borough_filter == "All NYC":
                            # Query all NYC data, sort, then limit
                            query = f"""
                            SELECT "{zip_col}" as zipcode, "{burden_col}" as rent_burden_rate
                            FROM {table_name}
                            WHERE "{zip_col}" IS NOT NULL 
                            AND "{burden_col}" IS NOT NULL
                            AND "{burden_col}" > 0
                            AND CAST("{zip_col}" AS TEXT) ~ '^(10[0-9]{{3}}|11[0-6][0-9]{{2}})$'
                            ORDER BY "{burden_col}" DESC
                            LIMIT {num_results};
                            """
                        else:
                            # Filter by borough column directly (if available)
                            if borough_col:
                                # First, check if borough column has any non-null values
                                check_query = f"""
                                SELECT COUNT(*) as total_count,
                                       COUNT(DISTINCT "{borough_col}") as distinct_boroughs,
                                       COUNT(CASE WHEN "{borough_col}" IS NOT NULL AND TRIM("{borough_col}") != '' THEN 1 END) as non_null_count
                                FROM {table_name}
                                WHERE "{zip_col}" IS NOT NULL 
                                AND "{burden_col}" IS NOT NULL
                                AND "{burden_col}" > 0;
                                """
                                check_df = pd.read_sql_query(check_query, conn)
                                
                                # If borough column is mostly empty, fall back to ZIP pattern
                                if not check_df.empty and check_df.iloc[0]['non_null_count'] == 0:
                                    # Borough column exists but is empty, use ZIP pattern fallback
                                    borough_zip_ranges = {
                                        "Manhattan": r'^(10[0-2][0-9]{2})$',
                                        "Brooklyn": r'^(11[2-3][0-9]{2})$',
                                        "Queens": r'^(11[0-1][0-9]{2}|114[0-9]{2})$',
                                        "Bronx": r'^(104[0-9]{2})$',
                                        "Staten Island": r'^(103[0-9]{2})$'
                                    }
                                    zip_pattern = borough_zip_ranges.get(borough_filter, r'^(10[0-9]{3}|11[0-6][0-9]{2})$')
                                    query = f"""
                                    SELECT "{zip_col}" as zipcode, "{burden_col}" as rent_burden_rate
                                    FROM {table_name}
                                    WHERE "{zip_col}" IS NOT NULL 
                                    AND "{burden_col}" IS NOT NULL
                                    AND "{burden_col}" > 0
                                    AND CAST("{zip_col}" AS TEXT) ~ '{zip_pattern}'
                                    ORDER BY "{burden_col}" DESC
                                    LIMIT {num_results};
                                    """
                                else:
                                    # Borough column has data, try to match
                                    # Normalize borough filter to match database values
                                    # Try multiple variations of borough names
                                    borough_variations = {
                                        "Manhattan": ["manhattan", "new york", "new york county"],
                                        "Brooklyn": ["brooklyn", "kings", "kings county"],
                                        "Queens": ["queens", "queens county"],
                                        "Bronx": ["bronx", "bronx county"],
                                        "Staten Island": ["staten island", "richmond", "richmond county"]
                                    }
                                    
                                    # Build WHERE clause with multiple OR conditions for borough matching
                                    borough_conditions = []
                                    if borough_filter in borough_variations:
                                        for variant in borough_variations[borough_filter]:
                                            borough_conditions.append(f"LOWER(TRIM(\"{borough_col}\")) = LOWER('{variant}')")
                                    
                                    # Also try exact match with normalized name
                                    borough_conditions.append(f"LOWER(TRIM(\"{borough_col}\")) = LOWER('{borough_filter}')")
                                    
                                    borough_where = "(" + " OR ".join(borough_conditions) + ")" if borough_conditions else f"LOWER(TRIM(\"{borough_col}\")) = LOWER('{borough_filter}')"
                                    
                                    query = f"""
                                    SELECT "{zip_col}" as zipcode, "{burden_col}" as rent_burden_rate
                                    FROM {table_name}
                                    WHERE "{zip_col}" IS NOT NULL 
                                    AND "{burden_col}" IS NOT NULL
                                    AND "{burden_col}" > 0
                                    AND "{borough_col}" IS NOT NULL
                                    AND TRIM("{borough_col}") != ''
                                    AND {borough_where}
                                    ORDER BY "{burden_col}" DESC
                                    LIMIT {num_results};
                                    """
                            else:
                                # Fallback to ZIP pattern if borough column not available
                                borough_zip_ranges = {
                                    "Manhattan": r'^(10[0-2][0-9]{2})$',
                                    "Brooklyn": r'^(11[2-3][0-9]{2})$',
                                    "Queens": r'^(11[0-1][0-9]{2}|114[0-9]{2})$',
                                    "Bronx": r'^(104[0-9]{2})$',
                                    "Staten Island": r'^(103[0-9]{2})$'
                                }
                                zip_pattern = borough_zip_ranges.get(borough_filter, r'^(10[0-9]{3}|11[0-6][0-9]{2})$')
                                query = f"""
                                SELECT "{zip_col}" as zipcode, "{burden_col}" as rent_burden_rate
                                FROM {table_name}
                                WHERE "{zip_col}" IS NOT NULL 
                                AND "{burden_col}" IS NOT NULL
                                AND "{burden_col}" > 0
                                AND CAST("{zip_col}" AS TEXT) ~ '{zip_pattern}'
                                ORDER BY "{burden_col}" DESC
                                LIMIT {num_results};
                                """
                        df = pd.read_sql_query(query, conn)
                        
                        if not df.empty:
                            df['zipcode'] = df['zipcode'].astype(str).str.extract(r'(\d{5})', expand=False)
                            df = df[df['zipcode'].notna()]
                            df['rent_burden_rate'] = pd.to_numeric(df['rent_burden_rate'], errors='coerce')
                            # If values are < 1, convert from decimal to percentage
                            if df['rent_burden_rate'].max() < 1:
                                df['rent_burden_rate'] = df['rent_burden_rate'] * 100
                            df = df[df['rent_burden_rate'].notna() & (df['rent_burden_rate'] > 5)]
                            
                            for _, row in df.iterrows():
                                zipcode = str(row['zipcode']).strip()[:5]
                                burden = float(row['rent_burden_rate'])
                                results.append({
                                    'zipcode': zipcode,
                                    'value': burden,
                                    'display': f"ZIP {zipcode} — {burden:.1f}%"
                                })
            
            conn.close()
            return results
        
        except Exception as e:
            st.warning(f"⚠️ Error fetching critical ZIP codes: {str(e)[:200]}")
            return []
    
    # Get and display critical ZIP codes
    critical_results = get_critical_zip_codes(metric_type, borough_filter, num_results)
    
    if critical_results:
        st.markdown(f"**{metric_type}** ({borough_filter})")
        st.markdown(f"Showing top {len(critical_results)} results:")
        
        # Display as a numbered list
        for idx, item in enumerate(critical_results, 1):
            st.markdown(f"{idx}. {item['display']}")
        
        # Also show as a table for better readability
        display_df = pd.DataFrame(critical_results)
        st.dataframe(
            display_df[['zipcode', 'value', 'display']],
            use_container_width=True,
            hide_index=True
        )
    else:
        st.info(f"ℹ️ No data found for {metric_type} in {borough_filter}. Please try a different filter.")
    
    st.divider()
    
    # Map visualization section
    st.markdown("### 🗺️ Map Visualizations")
    
    # Create buttons for each map type
    map_cols = st.columns(3)
    
    with map_cols[0]:
        show_rent_map = st.button("📊 Show Median Rent Map", use_container_width=True)
    with map_cols[1]:
        show_income_map = st.button("💰 Show Median Income Map", use_container_width=True)
    with map_cols[2]:
        show_burden_map = st.button("📈 Show Rent Burden Map", use_container_width=True)
    
    # Display selected map
    if show_rent_map:
        st.markdown("---")
        st.subheader("📊 Median Rent Map")
        st.markdown("**Color Legend:** 🔴 Red = Highest Rent | 🟢 Green = Lowest Rent")
        
        # Median Rent requires bedroom type selection
        if bedroom_type:
            bed_col_map = {
                "Studio": "rent_studio",
                "1BR": "rent_1br",
                "2BR": "rent_2br",
                "3+BR": "rent_3plus"
            }
            bed_col = bed_col_map.get(bedroom_type)
            
            if bed_col and not rent_df.empty and bed_col in rent_df.columns:
                rent_filtered = rent_df[rent_df[bed_col].notna()].copy()
                if not rent_filtered.empty:
                    st.info(f"📊 Loaded {len(rent_filtered)} ZIP codes with {bedroom_type} rent data. Range: ${rent_filtered[bed_col].min():,.0f} - ${rent_filtered[bed_col].max():,.0f}")
                    
                    map_obj = render_map_visualization(rent_filtered, bed_col, f"Median Rent ({bedroom_type})", reverse=False)
                    if map_obj:
                        st.pydeck_chart(map_obj, use_container_width=True)
                        
                        # Add CSV download button below map
                        display_cols = [bed_col, 'zipcode']
                        if 'area_name' in rent_filtered.columns:
                            display_cols.append('area_name')
                        if 'borough' in rent_filtered.columns:
                            display_cols.append('borough')
                        display_df = rent_filtered[display_cols].dropna(subset=[bed_col]).sort_values(bed_col)
                        csv = display_df.to_csv(index=False)
                        st.download_button(
                            label="📥 Download Data as CSV",
                            data=csv,
                            file_name=f"median_rent_{bedroom_type.lower().replace('+', 'plus')}.csv",
                            mime="text/csv"
                        )
                    else:
                        st.error(f"❌ Failed to render map for Median Rent ({bedroom_type}). Please check data and ZIP shapes.")
                else:
                    st.warning(f"⚠️ No {bedroom_type} rent data available.")
            else:
                st.warning(f"⚠️ No {bedroom_type} rent data available.")
        # Note: bedroom_type is already selected in the filters above, no need to select again
    
    if show_income_map:
        st.markdown("---")
        st.subheader("💰 Median Income Map")
        st.markdown("**Color Legend:** 🔴 Red = Lowest Income | 🟢 Green = Highest Income")
        
        # Display NYC-wide median income (hardcoded value)
        NYC_MEDIAN_INCOME = 79713
        st.success(f"📊 **NYC-Wide Median Income:** ${NYC_MEDIAN_INCOME:,}")
        
        if not income_df.empty and income_df['median_income'].notna().any():
            # Show borough-level median income if borough column is available
            if 'borough' in income_df.columns:
                borough_stats = []
                for borough in ['Manhattan', 'Brooklyn', 'Queens', 'Bronx', 'Staten Island']:
                    borough_data = income_df[income_df['borough'] == borough]
                    if not borough_data.empty:
                        borough_median = borough_data['median_income'].median()
                        if pd.notna(borough_median):
                            borough_stats.append(f"**{borough}:** ${borough_median:,.0f}")
                
                if borough_stats:
                    st.markdown("**Borough-Level Median Income:** " + " | ".join(borough_stats))
            
            # Fix: Use reverse=False so that low income = red, high income = green
            map_obj = render_map_visualization(income_df, 'median_income', "Median Income", reverse=False)
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
        st.markdown("**Note:** Map shows NYC ZIP codes only (100xx-116xx). Black outline indicates NYC boundary.")
        if not burden_df.empty and burden_df['rent_burden_rate'].notna().any():
            # Filter to NYC ZIPs only before rendering
            burden_df_nyc = filter_to_nyc_zip(burden_df.copy(), 'zipcode')
            if not burden_df_nyc.empty:
                # Fix: Use reverse=True so that high burden (severe) = red, low burden (good) = green
                map_obj = render_map_visualization(burden_df_nyc, 'rent_burden_rate', "Rent Burden Rate", reverse=True, show_nyc_boundary=True)
                if map_obj:
                    st.pydeck_chart(map_obj, use_container_width=True)
                    
                    # Add CSV download button below map
                    display_df = burden_df_nyc[['rent_burden_rate', 'zipcode']].dropna(subset=['rent_burden_rate']).sort_values('rent_burden_rate', ascending=False)
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
                st.warning("⚠️ No NYC ZIP codes found in rent burden data.")
        else:
            st.warning("⚠️ No rent burden data available.")
    

if __name__ == "__main__":
    render_analysis_page()

