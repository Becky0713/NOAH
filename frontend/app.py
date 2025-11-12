"""
NYC Housing Hub - Redesigned Frontend
New layout with left filter panel, center map, right info card, and top navigation
"""

import json
import os
import requests
import streamlit as st
import pandas as pd
import pydeck as pdk
from typing import List, Dict, Any
from pathlib import Path
import time

# Backend URL
BACKEND_URL = "https://nyc-housing-backend.onrender.com"

# Zillow ZORI (Zip-level rent index) CSV
ZILLOW_ZORI_URL = "https://files.zillowstatic.com/research/public_csvs/zori/Zip_ZORI_AllHomesPlusMultifamily_SSA.csv"

@st.cache_data(show_spinner=False, ttl=86400)
def fetch_zillow_rent_data():
    """Fetch the latest Zillow ZIP-level rent data for New York State."""
    try:
        zillow_df = pd.read_csv(ZILLOW_ZORI_URL)
        # Keep only New York State ZIP codes
        zillow_df = zillow_df[zillow_df["StateName"] == "NY"].copy()
        if zillow_df.empty:
            return pd.DataFrame(columns=["zipcode", "average_rent"]), None

        latest_month = zillow_df.columns[-1]

        zillow_df = zillow_df[["RegionName", latest_month]].copy()
        zillow_df.columns = ["zipcode", "average_rent"]

        zillow_df["zipcode"] = zillow_df["zipcode"].astype(str).str.zfill(5)
        zillow_df["average_rent"] = (
            pd.to_numeric(zillow_df["average_rent"], errors="coerce")
            .round()
            .astype("Int64")
        )

        return zillow_df, latest_month
    except Exception as exc:  # noqa: BLE001
        st.warning(f"⚠️ Failed to fetch Zillow rent data: {str(exc)[:150]}")
        return pd.DataFrame(columns=["zipcode", "average_rent"]), None

# Load glossary data
@st.cache_data
def load_glossary_data() -> List[Dict[str, Any]]:
    """Load glossary data from JSON file"""
    try:
        glossary_path = Path(__file__).parent / "data" / "glossary.json"
        with open(glossary_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        st.error(f"Failed to load glossary data: {e}")
        return []

# API functions with retry logic for Render cold starts
def _make_request_with_retry(url: str, params: dict = None, max_retries: int = 3) -> requests.Response:
    """Make HTTP request with retry logic for Render cold starts"""
    # Increased timeout for Render cold starts (can take 30-60 seconds)
    timeout_seconds = 60
    
    for attempt in range(max_retries):
        try:
            resp = requests.get(url, params=params, timeout=timeout_seconds)
            resp.raise_for_status()
            return resp
        except requests.exceptions.Timeout:
            if attempt < max_retries - 1:
                wait_time = 2 ** attempt
                st.warning(f"⏳ Request timeout (attempt {attempt + 1}/{max_retries}). Waiting {wait_time}s before retry...")
                time.sleep(wait_time)  # Exponential backoff
                continue
            else:
                st.error(f"❌ Request timed out after {max_retries} attempts. The backend may be slow or unavailable.")
                raise
        except requests.exceptions.RequestException as e:
            if attempt < max_retries - 1:
                wait_time = 2 ** attempt
                st.warning(f"⚠️ Request failed (attempt {attempt + 1}/{max_retries}): {str(e)[:100]}. Retrying in {wait_time}s...")
                time.sleep(wait_time)
                continue
            else:
                st.error(f"❌ Request failed after {max_retries} attempts: {str(e)[:200]}")
                raise

@st.cache_data(show_spinner=False, ttl=60)
def fetch_regions() -> List[Dict[str, Any]]:
    resp = _make_request_with_retry(f"{BACKEND_URL}/v1/regions")
    return resp.json()

@st.cache_data(show_spinner=False, ttl=60)
def fetch_field_metadata() -> List[Dict[str, Any]]:
    resp = _make_request_with_retry(f"{BACKEND_URL}/metadata/fields")
    return resp.json()

def fetch_records_paginated(
    fields: List[str],
    limit: int,
    borough: str = "",
    min_units: int = 0,
    max_units: int = 0,
    start_date_from: str = "",
    start_date_to: str = ""
) -> List[Dict[str, Any]]:
    """Fetch records with pagination support for large datasets"""
    all_records = []
    offset = 0
    # For small limits, fetch all at once; for larger, use batches
    batch_size = min(limit, 1000)  # Fetch in batches, but don't exceed limit
    
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    try:
        while len(all_records) < limit:
            current_limit = min(batch_size, limit - len(all_records))
            
            status_text.text(f"📡 Fetching data... {len(all_records)}/{limit} records")
            if len(all_records) > 0:
                progress_bar.progress(min(len(all_records) / limit, 1.0))
            
            params = {
                "fields": ",".join(fields),
                "limit": current_limit,
                "offset": offset,
                "min_units": min_units,
                "max_units": max_units,
                "start_date_from": start_date_from,
                "start_date_to": start_date_to
            }
            if borough:
                params["borough"] = borough
            
            try:
                resp = _make_request_with_retry(f"{BACKEND_URL}/v1/records", params=params)
                batch = resp.json()
            except Exception as e:
                # If request fails, return what we have so far
                st.error(f"❌ Failed to fetch data: {str(e)[:200]}")
                if all_records:
                    st.warning(f"⚠️ Showing {len(all_records)} records that were loaded before the error.")
                break
            
            if not batch or not isinstance(batch, list):
                break  # No more data or invalid response
            
            all_records.extend(batch)
            offset += len(batch)
            
            # If we got less than requested, we've reached the end
            if len(batch) < current_limit:
                break
            
            # Stop if we've reached the limit
            if len(all_records) >= limit:
                break
    except Exception as e:
        st.error(f"❌ Error during data fetching: {str(e)[:200]}")
    finally:
        if len(all_records) > 0:
            progress_bar.progress(1.0)
            status_text.text(f"✅ Loaded {len(all_records)} records")
            time.sleep(0.5)  # Brief pause to show completion
        progress_bar.empty()
        status_text.empty()
    
    return all_records[:limit] if all_records else []  # Return exactly up to limit, or empty list

def get_db_connection():
    """Get database connection from Streamlit secrets"""
    try:
        import psycopg2
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

@st.cache_data(show_spinner=False, ttl=3600)
def fetch_zip_rent_burden_data():
    """Fetch rent burden data by zip code from noah_zip_rentburden table"""
    try:
        conn = get_db_connection()
        
        # Try to find the zip code column (could be zipcode, zip_code, postcode, postal_code, etc.)
        # First, get column names
        column_query = """
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_name = 'noah_zip_rentburden'
        ORDER BY ordinal_position;
        """
        columns_df = pd.read_sql_query(column_query, conn)
        
        if columns_df.empty:
            conn.close()
            return pd.DataFrame()
        
        column_names = columns_df['column_name'].tolist()
        
        # Find zip code column
        zip_col = None
        for col in ['zipcode', 'zip_code', 'postcode', 'postal_code', 'zip', 'zcta']:
            if col in column_names:
                zip_col = col
                break
        
        if not zip_col:
            # Try to find any column with 'zip' or 'post' in name
            for col in column_names:
                if 'zip' in col.lower() or 'post' in col.lower():
                    zip_col = col
                    break
        
        if not zip_col:
            conn.close()
            st.warning("⚠️ Could not find zip code column in noah_zip_rentburden table")
            return pd.DataFrame()
        
        # Find rent burden columns
        rent_burden_cols = []
        for col in column_names:
            col_lower = col.lower()
            if ('rent' in col_lower and 'burden' in col_lower) or ('rent' in col_lower and 'cost' in col_lower):
                rent_burden_cols.append(col)
        
        if not rent_burden_cols:
            # Try alternative names
            for col in column_names:
                col_lower = col.lower()
                if 'burden' in col_lower or ('cost' in col_lower and 'burden' in col_lower):
                    rent_burden_cols.append(col)
        
        # If still no columns found, show all columns for debugging
        if not rent_burden_cols:
            st.warning(f"⚠️ Could not find rent burden columns. Available columns: {', '.join(column_names)}")
            # Try to use any column that might be rent burden related
            for col in column_names:
                if any(keyword in col.lower() for keyword in ['rate', 'percent', 'pct', '%']):
                    rent_burden_cols.append(col)
        
        # Build query - select zip code and rent burden columns
        select_cols = [zip_col] + rent_burden_cols
        select_str = ", ".join([f'"{col}"' for col in select_cols])
        
        query = f"""
        SELECT {select_str}
        FROM noah_zip_rentburden
        WHERE "{zip_col}" IS NOT NULL
        """
        
        df = pd.read_sql_query(query, conn)
        conn.close()
        
        # Rename zip column to standard name for merging
        df = df.rename(columns={zip_col: 'zipcode'})
        
        # Clean zipcode - extract 5-digit zip
        df['zipcode'] = df['zipcode'].astype(str).str.extract(r'(\d{5})', expand=False)
        df = df[df['zipcode'].notna()]
        
        # Rename rent burden columns to standard names if needed
        # Look for columns that should be renamed to rent_burden_rate and severe_burden_rate
        for col in df.columns:
            col_lower = col.lower()
            if col != 'zipcode':
                if 'severe' in col_lower or '50' in col_lower:
                    if 'rent_burden_rate' not in df.columns or 'severe_burden_rate' not in df.columns:
                        df = df.rename(columns={col: 'severe_burden_rate'})
                elif 'rent_burden_rate' not in df.columns:
                    df = df.rename(columns={col: 'rent_burden_rate'})
        
        # Debug info
        if st.session_state.get('show_rent_burden_debug', False):
            st.write(f"**Rent burden data loaded:** {len(df)} rows")
            st.write(f"**Columns:** {list(df.columns)}")
            st.write(f"**Sample zipcodes:** {df['zipcode'].head(5).tolist()}")
        
        return df
    except Exception as e:
        st.warning(f"⚠️ Could not fetch rent burden data: {str(e)[:200]}")
        return pd.DataFrame()

def fetch_median_income_data():
    """Fetch median household income data from database"""
    try:
        import psycopg2
        
        conn = psycopg2.connect(
            host=st.secrets["secrets"]["db_host"],
            port=int(st.secrets["secrets"]["db_port"]),
            dbname=st.secrets["secrets"]["db_name"],
            user=st.secrets["secrets"]["db_user"],
            password=st.secrets["secrets"]["db_password"],
            sslmode="require"
        )
        
        query = """
        SELECT geo_id, tract_name, median_household_income
        FROM median_household_income
        WHERE median_household_income IS NOT NULL
        AND median_household_income != '<NA>'
        AND median_household_income != 'Geography'
        """
        
        df = pd.read_sql_query(query, conn)
        conn.close()
        
        # Convert income to numeric
        df['median_household_income'] = pd.to_numeric(
            df['median_household_income'], 
            errors='coerce'
        )
        df = df[df['median_household_income'].notna()]
        
        return df
    except Exception as e:
        # Database might not be available or table doesn't exist
        return pd.DataFrame()

def render_top_navigation():
    """Render top navigation bar"""
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.markdown("# 🏠 NYC Housing Hub")
    
    with col2:
        if st.button("📋 Info Panel", use_container_width=True):
            st.session_state.show_info_card = not st.session_state.show_info_card
            st.rerun()

def render_filter_panel():
    """Render left filter panel"""
    with st.container():
        # Sample size with option to show all
        st.markdown("#### 📊 Data Options")
        show_all = st.checkbox("Show All Projects (may take longer)", value=False)
        if show_all:
            sample_size = 10000  # Large number to fetch all (backend will handle pagination if needed)
        else:
            sample_size = st.slider("Sample Size", min_value=10, max_value=5000, value=100, step=50)
        
        # Location filters
        st.markdown("#### 📍 Location Filters")
        borough_options = ["", "Manhattan", "Brooklyn", "Queens", "Bronx", "Staten Island"]
        selected_borough = st.selectbox("Borough", borough_options, index=0)
        
        # Postcode filter
        postcode_filter = st.text_input(
            "Postcode (e.g., 10025)",
            placeholder="Enter postcode to filter...",
            key="postcode_filter"
        )
        
        # Street name filter
        street_name_filter = st.text_input(
            "Street Name (e.g., Broadway)",
            placeholder="Enter street name to filter...",
            key="street_name_filter"
        )
        
        return {
            "sample_size": sample_size,
            "borough": selected_borough,
            "postcode": postcode_filter.strip() if postcode_filter else "",
            "street_name": street_name_filter.strip() if street_name_filter else "",
            "min_units": 0,
            "max_units": 0,
            "start_date_from": "",
            "start_date_to": ""
        }

def render_map(data: pd.DataFrame):
    """Render interactive map using PyDeck"""
    if data.empty:
        st.info("No data available for mapping.")
        return
        
    # Check if coordinate columns exist
    coord_cols = ["latitude", "longitude"]
    missing_coords = [col for col in coord_cols if col not in data.columns]
    if missing_coords:
        st.error(f"Missing coordinate columns: {missing_coords}")
        st.write("Available columns:", list(data.columns))
        return
    
    # Convert coordinate columns to numeric, handling string values
    for col in coord_cols:
        if col in data.columns:
            data[col] = pd.to_numeric(data[col], errors='coerce')
    
    # Check for valid coordinates
    coord_data = data[coord_cols].dropna()
    if coord_data.empty:
        st.info("No valid coordinates found in data.")
        st.write("Sample data:", data[coord_cols].head())
        return
    
    # Filter data with coordinates
    df_geo = data.dropna(subset=["latitude", "longitude"]).copy()
    
    if df_geo.empty:
        st.info("No valid coordinates found.")
        return
    
    # Calculate center point
    center_lat = df_geo["latitude"].mean()
    center_lon = df_geo["longitude"].mean()
    
    # Adjust point size based on affordable units (more affordable units = larger circle)
    # Use affordable_units if available, otherwise fall back to total_units
    df_geo["radius"] = df_geo.apply(
        lambda row: max(20, min(200, (row.get("affordable_units", row.get("total_units", 0)) or 0) * 1.5)),
        axis=1
    )
    
    # Use single color for all points (blue)
    # Create a list of colors for each row
    df_geo["color"] = [[0, 100, 200, 140]] * len(df_geo)  # Blue color for all points
    
    # Create PyDeck layer
    layer = pdk.Layer(
        "ScatterplotLayer",
        data=df_geo,
        get_position="[longitude, latitude]",
        get_radius="radius",
        radius_min_pixels=3,
        radius_max_pixels=50,
        get_fill_color="color",
        pickable=True,
    )
    
    # Ensure all tooltip fields exist with defaults
    # Handle both column access methods
    if 'project_id' not in df_geo.columns:
        df_geo['project_id'] = pd.Series([''] * len(df_geo), dtype=str, index=df_geo.index)
    df_geo['project_id'] = df_geo['project_id'].fillna('').astype(str)
    
    if 'borough' not in df_geo.columns:
        if 'region' in df_geo.columns:
            df_geo['borough'] = df_geo['region'].fillna('')
        else:
            df_geo['borough'] = pd.Series([''] * len(df_geo), dtype=str, index=df_geo.index)
    else:
        df_geo['borough'] = df_geo['borough'].fillna(df_geo.get('region', '')).fillna('')
    
    if 'postcode' not in df_geo.columns:
        df_geo['postcode'] = pd.Series([''] * len(df_geo), dtype=str, index=df_geo.index)
    df_geo['postcode'] = df_geo['postcode'].fillna('').astype(str)

    # Prepare Zillow average rent display
    if 'average_rent' not in df_geo.columns:
        df_geo['average_rent'] = pd.Series([pd.NA] * len(df_geo), index=df_geo.index)
    df_geo['average_rent_display'] = df_geo['average_rent'].apply(
        lambda x: f"${int(x):,}" if pd.notna(x) else "N/A"
    )
    
    # Use building_completion_date if available, otherwise fall back to project_completion_date
    # Check if building_completion_display already exists (from data processing)
    if 'building_completion_display' not in df_geo.columns:
        if 'building_completion_date' not in df_geo.columns:
            # Try to use project_completion_date as fallback
            if 'project_completion_date' in df_geo.columns:
                df_geo['building_completion_date'] = df_geo['project_completion_date']
            else:
                df_geo['building_completion_date'] = pd.Series([''] * len(df_geo), dtype=str, index=df_geo.index)
        else:
            # If building_completion_date exists but is mostly empty, use project_completion_date as fallback
            non_empty_count = df_geo['building_completion_date'].notna().sum()
            if non_empty_count < len(df_geo) * 0.1:  # If less than 10% have data, use project_completion_date
                if 'project_completion_date' in df_geo.columns:
                    df_geo['building_completion_date'] = df_geo['project_completion_date'].fillna(df_geo['building_completion_date'])
        
        # Format building completion date (show "In Progress" if empty)
        df_geo['building_completion_display'] = df_geo['building_completion_date'].fillna('').apply(
            lambda x: "In Progress" if not x or str(x).strip() == '' else str(x)
        )
    
    # Ensure numeric fields exist
    for field in ['extremely_low_income_units', 'very_low_income_units', 'low_income_units',
                  'studio_units', '_1_br_units', '_2_br_units', 'counted_rental_units']:
        if field not in df_geo.columns:
            df_geo[field] = 0
        else:
            df_geo[field] = pd.to_numeric(df_geo[field], errors='coerce').fillna(0).astype(int)
    
    # Prepare rent burden display for tooltip
    if 'rent_burden_rate' in df_geo.columns:
        df_geo['rent_burden_display'] = df_geo['rent_burden_rate'].apply(
            lambda x: f"{float(x):.1f}%" if pd.notna(x) and x != '' and str(x).strip() != '' else "N/A"
        )
    else:
        df_geo['rent_burden_display'] = pd.Series(['N/A'] * len(df_geo), index=df_geo.index)
    
    if 'severe_burden_rate' in df_geo.columns:
        df_geo['severe_burden_display'] = df_geo['severe_burden_rate'].apply(
            lambda x: f"{float(x):.1f}%" if pd.notna(x) and x != '' and str(x).strip() != '' else "N/A"
        )
    else:
        df_geo['severe_burden_display'] = pd.Series(['N/A'] * len(df_geo), index=df_geo.index)
    
    # PyDeck uses {field_name} for variables in tooltip
    tooltip = {
        "html": """
        <b>Project ID: {project_id}</b><br/>
        Borough: {borough}<br/>
        Postcode: {postcode}<br/>
        Average Rent (Zillow): {average_rent_display}<br/>
        Rent Burden Rate: {rent_burden_display}<br/>
        Severe Burden Rate: {severe_burden_display}<br/>
        Building Completion: {building_completion_display}<br/>
        <br/>
        <b>Income-Restricted Units:</b><br/>
        Extremely Low: {extremely_low_income_units} | Very Low: {very_low_income_units} | Low: {low_income_units}<br/>
        <br/>
        <b>Bedroom Units:</b><br/>
        Studio: {studio_units} | 1-BR: {_1_br_units} | 2-BR: {_2_br_units}<br/>
        <br/>
        Counted Rental Units: {counted_rental_units}
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
    map_result = st.pydeck_chart(
        pdk.Deck(
            layers=[layer], 
            initial_view_state=view_state, 
            tooltip=tooltip
        ),
        use_container_width=True
    )
    
    # Project ID search and selection
    if not df_geo.empty:
        st.markdown("### 🔍 Search by Project ID")
        
        # Get unique project IDs for suggestions (cache to avoid recomputing)
        if 'cached_project_ids' not in st.session_state or len(st.session_state.cached_project_ids) != len(df_geo):
            project_ids = sorted([str(pid) for pid in df_geo['project_id'].dropna().unique().tolist() if pid])
            st.session_state.cached_project_ids = project_ids
        else:
            project_ids = st.session_state.cached_project_ids
        
        # Search input with autocomplete suggestions
        search_col1, search_col2 = st.columns([3, 1])
        
        with search_col1:
            # Preserve search query in input
            default_search = st.session_state.get('last_search_id', '')
            search_query = st.text_input(
                "Enter Project ID to search:",
                placeholder="Type project ID or select from dropdown...",
                key="project_id_search",
                value=default_search if default_search else ''
            )
        
        with search_col2:
            # Quick select from dropdown (limit to first 100 for performance)
            dropdown_options = ["None"] + project_ids[:100]
            # Try to preserve current selection
            current_dropdown = st.session_state.get('last_search_id', 'None')
            if current_dropdown in dropdown_options:
                dropdown_index = dropdown_options.index(current_dropdown)
            else:
                dropdown_index = 0
            
            selected_from_dropdown = st.selectbox(
                "Or select from list:",
                options=dropdown_options,
                index=dropdown_index,
                key="project_id_dropdown"
            )
        
        # Determine which search to use
        search_id = None
        if selected_from_dropdown != "None":
            search_id = selected_from_dropdown
        elif search_query and search_query.strip():
            search_id = search_query.strip()
        
        # If we have a selected project from previous search, use its ID as search_id
        if not search_id and st.session_state.selected_project is not None:
            search_id = str(st.session_state.selected_project.get('project_id', ''))
            # Update input field to show the current project ID
            if search_id:
                st.session_state.last_search_id = search_id
        
        # Search for project - optimized to avoid unnecessary searches and reruns
        if search_id:
            # Only search if the search_id changed
            current_search = st.session_state.get('last_search_id', '')
            if search_id != current_search:
                # Convert project_id to string once for better performance
                project_id_str = df_geo['project_id'].astype(str)
                
                # Try exact match first (much faster than contains)
                exact_match = df_geo[project_id_str == search_id]
                
                if not exact_match.empty:
                    matching_projects = exact_match
                else:
                    # Fall back to contains search only if no exact match
                    matching_projects = df_geo[project_id_str.str.contains(search_id, case=False, na=False)]
                
                if not matching_projects.empty:
                    if len(matching_projects) == 1:
                        # Exact match, show it immediately
                        selected_project = matching_projects.iloc[0].to_dict()
                        # Ensure all data is properly extracted
                        st.session_state.selected_project = selected_project
                        st.session_state.show_info_card = True
                        st.session_state.last_search_id = search_id
                        st.success(f"✅ Found Project ID: {search_id}")
                        # Rerun to ensure info card displays properly
                        st.rerun()
                    else:
                        # Multiple matches, show list
                        st.info(f"Found {len(matching_projects)} matching projects. Select one:")
                        match_options = [f"{row.get('project_id', 'N/A')} - {row.get('project_name', 'N/A')}" 
                                        for idx, row in matching_projects.head(10).iterrows()]
                        selected_match = st.selectbox("Select project:", options=["None"] + match_options, key="project_match_select")
                        
                        if selected_match != "None":
                            match_idx = match_options.index(selected_match)
                            selected_project = matching_projects.iloc[match_idx].to_dict()
                            st.session_state.selected_project = selected_project
                            st.session_state.show_info_card = True
                            st.session_state.last_search_id = search_id
                            # Rerun to ensure info card displays properly
                            st.rerun()
                else:
                    st.warning(f"⚠️ No project found with ID: {search_id}")
                    st.session_state.last_search_id = search_id
                    st.session_state.selected_project = None
        
        # Show info card below search if project is selected (always check this)
        if st.session_state.selected_project is not None:
            # Show success message if we have a selected project
            current_display_id = str(st.session_state.selected_project.get('project_id', ''))
            if current_display_id:
                st.success(f"✅ Showing Project ID: {current_display_id}")
            st.divider()
            render_info_card_section()
        
        # Download CSV button
        st.markdown("### 📥 Download Data")
        csv = df_geo.to_csv(index=False)
        st.download_button(
            "📥 Download Full Dataset as CSV",
            csv,
            "nyc_housing_projects.csv",
            "text/csv",
            use_container_width=True
        )

def render_info_card_section():
    """Render the info card section below search"""
    st.markdown("### 📋 Project Details")
    
    # Add a close button
    if st.button("❌ Close", type="secondary", use_container_width=True):
        st.session_state.show_info_card = False
        st.session_state.selected_project = None
        st.session_state.last_search_id = ''  # Clear search to allow re-search
        st.rerun()
    
    # Always check if project is selected (don't rely on show_info_card flag)
    if st.session_state.selected_project is not None:
        project = st.session_state.selected_project
        
        # Helper function to format values
        def get_val(key, default='N/A'):
            val = project.get(key, default)
            if val is None or val == '' or (isinstance(val, float) and pd.isna(val)):
                return default
            return val
        
        # Basic Info Section
        st.markdown("#### 📍 Basic Information")
        st.write(f"**Project ID:** {get_val('project_id')}")
        st.write(f"**Project Name:** {get_val('project_name')}")
        st.write(f"**Borough:** {get_val('borough', get_val('region'))}")
        st.write(f"**Postcode:** {get_val('postcode')}")

        avg_rent_val = project.get('average_rent', None)
        rent_display = 'N/A'
        if avg_rent_val is not None and avg_rent_val != 'N/A' and not pd.isna(avg_rent_val):
            try:
                rent_display = f"${int(float(avg_rent_val)):,}"
            except (ValueError, TypeError):  # noqa: BLE001
                rent_display = 'N/A'

        zillow_label = st.session_state.get('zillow_latest_month_label') or st.session_state.get('zillow_latest_month')
        if zillow_label:
            st.write(f"**Average Rent (Zillow ZORI {zillow_label}):** {rent_display}")
        else:
            st.write(f"**Average Rent (Zillow ZORI):** {rent_display}")
        
        # Rent Burden information
        rent_burden_val = project.get('rent_burden_rate', None)
        rent_burden_display = 'N/A'
        if rent_burden_val is not None and rent_burden_val != 'N/A' and not pd.isna(rent_burden_val):
            try:
                rent_burden_display = f"{float(rent_burden_val):.1f}%"
            except (ValueError, TypeError):  # noqa: BLE001
                rent_burden_display = 'N/A'
        st.write(f"**Rent Burden Rate:** {rent_burden_display}")
        
        severe_burden_val = project.get('severe_burden_rate', None)
        severe_burden_display = 'N/A'
        if severe_burden_val is not None and severe_burden_val != 'N/A' and not pd.isna(severe_burden_val):
            try:
                severe_burden_display = f"{float(severe_burden_val):.1f}%"
            except (ValueError, TypeError):  # noqa: BLE001
                severe_burden_display = 'N/A'
        st.write(f"**Severe Burden Rate:** {severe_burden_display}")

        st.write(f"**Building ID:** {get_val('building_id')}")
        st.write(f"**BBL:** {get_val('bbl')}")
        st.write(f"**BIN:** {get_val('bin')}")
        
        # Project Dates Section
        st.markdown("#### 📅 Project Dates")
        st.write(f"**Project Start Date:** {get_val('project_start_date')}")
        st.write(f"**Project Completion Date:** {get_val('project_completion_date')}")
        
        # Use building_completion_date if available, otherwise use project_completion_date
        building_date = get_val('building_completion_date', None)
        if not building_date or building_date == 'N/A' or building_date == '':
            # Fall back to project_completion_date
            building_date = get_val('project_completion_date', 'In Progress')
        if building_date == '' or building_date == 'N/A':
            building_date = 'In Progress'
        st.write(f"**Building Completion Date:** {building_date}")
        
        # Extended Affordability
        ext_afford = get_val('extended_affordability_status', 'N/A')
        if ext_afford != 'N/A':
            st.write(f"**Extended Affordability Only:** {ext_afford}")
        
        # Income-Restricted Units Section
        st.markdown("#### 💰 Income-Restricted Units")
        col1, col2 = st.columns(2)
        with col1:
            st.metric("Extremely Low", get_val('extremely_low_income_units', 0))
            st.metric("Very Low", get_val('very_low_income_units', 0))
            st.metric("Low", get_val('low_income_units', 0))
        with col2:
            st.metric("Moderate", get_val('moderate_income_units', 0))
            st.metric("Middle", get_val('middle_income_units', 0))
            st.metric("Other", get_val('other_income_units', 0))
        
        # Bedroom Units Section
        st.markdown("#### 🏠 Bedroom Units")
        col1, col2 = st.columns(2)
        with col1:
            st.metric("Studio", get_val('studio_units', 0))
            st.metric("1-BR", get_val('_1_br_units', 0))
            st.metric("2-BR", get_val('_2_br_units', 0))
            st.metric("3-BR", get_val('_3_br_units', 0))
        with col2:
            st.metric("4-BR", get_val('_4_br_units', 0))
            st.metric("5-BR", get_val('_5_br_units', 0))
            st.metric("6-BR+", get_val('_6_br_units', 0))
            st.metric("Unknown BR", get_val('unknown_br_units', 0))
        
        # Unit Counts Section
        st.markdown("#### 📊 Unit Counts")
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Total Units", get_val('total_units', 0))
        with col2:
            st.metric("Counted Rental", get_val('counted_rental_units', 0))
        with col3:
            st.metric("Counted Homeownership", get_val('counted_homeownership_units', 0))
    else:
        st.info("Select a Project ID above to view details.")

def main():
    """Main application"""
    st.set_page_config(
        page_title="NYC Housing Hub",
        layout="wide",
        initial_sidebar_state="collapsed"  # Start with collapsed sidebar
    )
    
    # Initialize session state
    if 'selected_fields' not in st.session_state:
        # Request all needed fields from API
        st.session_state.selected_fields = [
            "project_id", "project_name", "house_number", "street_name", "latitude", "longitude",
            "borough", "postcode", "building_id", "building_completion_date",
            "extremely_low_income_units", "very_low_income_units", "low_income_units",
            "moderate_income_units", "middle_income_units", "other_income_units",
            "studio_units", "_1_br_units", "_2_br_units", "_3_br_units",
            "_4_br_units", "_5_br_units", "_6_br_units", "unknown_br_units",
            "counted_rental_units", "counted_homeownership_units",
            "total_units", "all_counted_units",
            "project_start_date", "project_completion_date",
            "extended_affordability_status", "bbl", "bin"
        ]
        st.session_state.fields_confirmed = True
    
    if "show_info_card" not in st.session_state:
        st.session_state.show_info_card = False
    if "selected_project" not in st.session_state:
        st.session_state.selected_project = None
    if "last_search_id" not in st.session_state:
        st.session_state.last_search_id = ''
    if "cached_project_ids" not in st.session_state:
        st.session_state.cached_project_ids = []
    
    # Top navigation
    render_top_navigation()
    
    # Collapsible filters in sidebar
    with st.sidebar:
        filter_params = render_filter_panel()
    
    # Main layout: Full width for map and info card
    col_map = st.container()
    
    with col_map:
        # Center map - takes majority of screen
        st.markdown("### 🗺️ Interactive Map")
        
        # Fetch data
        try:
            # Fetch records with pagination
            records = fetch_records_paginated(
                st.session_state.selected_fields,
                limit=filter_params["sample_size"],
                borough=filter_params["borough"],
                min_units=filter_params["min_units"],
                max_units=filter_params["max_units"],
                start_date_from=filter_params["start_date_from"],
                start_date_to=filter_params["start_date_to"]
            )
            
            if records and len(records) > 0:
                df = pd.DataFrame(records)
                
                # Extract data from _raw field - this contains ALL fields from Socrata API
                if '_raw' in df.columns:
                    raw_data = df['_raw'].apply(lambda x: x if isinstance(x, dict) else {})
                    
                    # First, extract all available fields from raw data
                    # Get all unique keys from all raw records
                    all_keys = set()
                    for raw in raw_data:
                        if isinstance(raw, dict):
                            all_keys.update(raw.keys())
                    
                    # Extract all fields from raw data (these are the actual Socrata field names)
                    for key in all_keys:
                        if key not in df.columns:
                            df[key] = raw_data.apply(lambda x: x.get(key) if isinstance(x, dict) else None)
                    
                    # Now check if key fields exist and fill from _raw if needed
                    # project_id is a critical field - try multiple possible field names
                    project_id_found = False
                    for name in ['project_id', 'projectid', 'id', 'project__id', 'projectid_number']:
                        if name in all_keys:
                            df['project_id'] = raw_data.apply(lambda x: x.get(name) if isinstance(x, dict) else None)
                            project_id_found = True
                            break
                    
                    # If project_id is still missing, check if it's already in the result from backend
                    if 'project_id' not in df.columns and not project_id_found:
                        # Backend API might have already added it
                        if 'project_id' not in df.columns:
                            df['project_id'] = pd.Series([''] * len(df), dtype=str, index=df.index)
                    
                    # Do the same for other critical fields
                    field_mappings = {
                        'building_id': ['building_id', 'buildingid', 'building'],
                        'building_completion_date': ['building_completion_date', 'buildingcompletiondate', 'building_completion'],
                        'extended_affordability_status': ['extended_affordability_status', 'extendedaffordabilitystatus', 'extended_affordability'],
                        'postcode': ['postcode', 'postal_code', 'zip_code', 'zipcode'],
                        'street_name': ['street_name', 'streetname', 'street', 'street__name'],
                    }
                    
                    for target_field, possible_names in field_mappings.items():
                        if target_field not in df.columns or df[target_field].isna().all():
                            for name in possible_names:
                                if name in all_keys:
                                    df[target_field] = raw_data.apply(lambda x: x.get(name) if isinstance(x, dict) else None)
                                    break
                
                # Ensure required fields exist with defaults (handle missing columns)
                # Priority: use extracted field, then region, then empty
                if 'borough' not in df.columns:
                    # Use 'region' column if it exists, otherwise fill with empty string
                    if 'region' in df.columns:
                        df['borough'] = df['region'].fillna('')
                    else:
                        df['borough'] = pd.Series([''] * len(df), dtype=str, index=df.index)
                elif df['borough'].isna().all():
                    # If borough is all NaN, try to fill from region column
                    if 'region' in df.columns:
                        df['borough'] = df['region'].fillna('')
                    else:
                        df['borough'] = pd.Series([''] * len(df), dtype=str, index=df.index)
                
                # Ensure street_name column exists
                if 'street_name' not in df.columns:
                    df['street_name'] = pd.Series([''] * len(df), dtype=str, index=df.index)
                else:
                    df['street_name'] = df['street_name'].fillna('')
                
                for col in ['project_id', 'postcode']:
                    if col not in df.columns:
                        # Create a new column with the same length as the DataFrame
                        df[col] = pd.Series([''] * len(df), dtype=str, index=df.index)
                    else:
                        # Fill NaN values
                        df[col] = df[col].fillna('')

                # Merge Zillow rent data by ZIP code (postcode)
                zillow_df, zillow_latest_month = fetch_zillow_rent_data()

                if zillow_latest_month:
                    try:
                        month_label = pd.to_datetime(zillow_latest_month).strftime("%b %Y")
                    except Exception:  # noqa: BLE001
                        month_label = zillow_latest_month
                    st.session_state["zillow_latest_month"] = zillow_latest_month
                    st.session_state["zillow_latest_month_label"] = month_label
                else:
                    st.session_state["zillow_latest_month"] = None
                    st.session_state["zillow_latest_month_label"] = None

                if not zillow_df.empty:
                    df['postcode_clean'] = df['postcode'].astype(str).str.extract(r'(\d{5})', expand=False)
                    df = df.merge(zillow_df, how='left', left_on='postcode_clean', right_on='zipcode')
                    df.drop(columns=['zipcode', 'postcode_clean'], inplace=True)
                    if 'average_rent' in df.columns:
                        df['average_rent'] = df['average_rent'].astype('Int64')
                else:
                    df['average_rent'] = pd.Series([pd.NA] * len(df), dtype='Int64')
                
                # Merge rent burden data by ZIP code
                rent_burden_df = fetch_zip_rent_burden_data()
                if not rent_burden_df.empty:
                    df['postcode_clean'] = df['postcode'].astype(str).str.extract(r'(\d{5})', expand=False)
                    df = df.merge(rent_burden_df, how='left', left_on='postcode_clean', right_on='zipcode')
                    df.drop(columns=['zipcode', 'postcode_clean'], inplace=True, errors='ignore')
                    
                    # Debug: show merge results
                    matched_count = df[df['rent_burden_rate'].notna()].shape[0] if 'rent_burden_rate' in df.columns else 0
                    if matched_count > 0:
                        st.success(f"✅ Matched rent burden data for {matched_count} projects")
                    else:
                        st.warning(f"⚠️ Rent burden data loaded but no matches found. Check zip code format in both datasets.")
                else:
                    st.warning("⚠️ No rent burden data found in database. Check if `noah_zip_rentburden` table exists.")
                
                # Handle building_completion_date with fallback to project_completion_date
                if 'building_completion_date' not in df.columns:
                    # Try to use project_completion_date as fallback
                    if 'project_completion_date' in df.columns:
                        df['building_completion_date'] = df['project_completion_date']
                    else:
                        df['building_completion_date'] = pd.Series([''] * len(df), dtype=str, index=df.index)
                else:
                    # If building_completion_date exists but is mostly empty, use project_completion_date as fallback
                    non_empty_count = df['building_completion_date'].notna().sum()
                    if non_empty_count < len(df) * 0.1:  # If less than 10% have data, use project_completion_date
                        if 'project_completion_date' in df.columns:
                            df['building_completion_date'] = df['project_completion_date'].fillna(df['building_completion_date'])
                    else:
                        df['building_completion_date'] = df['building_completion_date'].fillna('')
                
                # Create building_completion_display column for tooltip
                # First ensure building_completion_date has a value (use project_completion_date if needed)
                if 'building_completion_date' in df.columns:
                    # Fill missing values with project_completion_date if available
                    if 'project_completion_date' in df.columns:
                        df['building_completion_date'] = df['building_completion_date'].fillna(df['project_completion_date'])
                elif 'project_completion_date' in df.columns:
                    # If building_completion_date doesn't exist, use project_completion_date
                    df['building_completion_date'] = df['project_completion_date']
                
                # Now create the display column
                df['building_completion_display'] = df['building_completion_date'].fillna('').apply(
                    lambda x: "In Progress" if not x or str(x).strip() == '' else str(x)
                )
                
                # Set defaults for numeric fields and ensure they're numeric
                numeric_fields = ['extremely_low_income_units', 'very_low_income_units', 'low_income_units',
                                 'moderate_income_units', 'middle_income_units', 'other_income_units',
                                 'studio_units', '_1_br_units', '_2_br_units', '_3_br_units',
                                 '_4_br_units', '_5_br_units', '_6_br_units', 'unknown_br_units',
                                 'counted_rental_units', 'counted_homeownership_units',
                                 'total_units', 'all_counted_units']
                for col in numeric_fields:
                    if col not in df.columns:
                        # Create a new column with the same length as the DataFrame
                        df[col] = pd.Series([0] * len(df), dtype=int, index=df.index)
                    else:
                        # Convert to numeric, handling strings and NaN
                        df[col] = pd.to_numeric(df[col], errors='coerce')
                        df[col] = df[col].fillna(0).astype(int)
                
                # Apply frontend filters (postcode and street name)
                original_count = len(df)
                
                # Filter by postcode if provided
                if filter_params.get("postcode") and filter_params["postcode"]:
                    postcode_filter = filter_params["postcode"].strip()
                    if 'postcode' in df.columns:
                        # Convert postcode to string for comparison
                        df['postcode_str'] = df['postcode'].astype(str)
                        df = df[df['postcode_str'].str.contains(postcode_filter, case=False, na=False)]
                    elif 'postal_code' in df.columns:
                        df['postcode_str'] = df['postal_code'].astype(str)
                        df = df[df['postcode_str'].str.contains(postcode_filter, case=False, na=False)]
                    elif 'zip_code' in df.columns:
                        df['postcode_str'] = df['zip_code'].astype(str)
                        df = df[df['postcode_str'].str.contains(postcode_filter, case=False, na=False)]
                
                # Filter by street name if provided (fuzzy match)
                if filter_params.get("street_name") and filter_params["street_name"]:
                    street_filter = filter_params["street_name"].strip().lower()
                    
                    # Create a mask to check multiple fields
                    mask = pd.Series([False] * len(df), index=df.index)
                    
                    # Check all possible street name fields
                    street_fields = ['street_name', 'streetname', 'street', 'street__name', 'street_name_1']
                    for field in street_fields:
                        if field in df.columns:
                            field_str = df[field].astype(str).fillna('').str.lower()
                            mask = mask | field_str.str.contains(street_filter, case=False, na=False)
                    
                    # Check address field (which may contain street name)
                    if 'address' in df.columns:
                        address_str = df['address'].astype(str).fillna('').str.lower()
                        mask = mask | address_str.str.contains(street_filter, case=False, na=False)
                    
                    # Check house_number + street_name combination
                    if 'house_number' in df.columns:
                        # Try with street_name field
                        if 'street_name' in df.columns:
                            full_address = (df['house_number'].astype(str).fillna('') + ' ' + 
                                           df['street_name'].astype(str).fillna('')).str.strip().str.lower()
                            mask = mask | full_address.str.contains(street_filter, case=False, na=False)
                        # Also try with any street field
                        for field in street_fields:
                            if field in df.columns:
                                full_address = (df['house_number'].astype(str).fillna('') + ' ' + 
                                               df[field].astype(str).fillna('')).str.strip().str.lower()
                                mask = mask | full_address.str.contains(street_filter, case=False, na=False)
                    
                    # Check project_name field (may contain street name)
                    if 'project_name' in df.columns:
                        project_name_str = df['project_name'].astype(str).fillna('').str.lower()
                        mask = mask | project_name_str.str.contains(street_filter, case=False, na=False)
                    
                    # Apply the mask
                    if mask.any():
                        df = df[mask]
                    else:
                        # If no matches found, set df to empty and show warning
                        df = df.iloc[0:0]  # Create empty DataFrame with same columns
                        st.warning(f"⚠️ No projects found matching street name: '{filter_params['street_name']}'. Try checking available fields in Debug Info.")
                
                filtered_count = len(df)
                if original_count != filtered_count:
                    st.info(f"📍 Showing {filtered_count} of {original_count} projects (filtered by location)")
                else:
                    st.write(f"📍 Showing {len(df)} projects")
                
                # Debug: Show available fields if requested (especially useful for street name filtering)
                if st.checkbox("🔍 Show Debug Info", value=False):
                    with st.expander("📋 Debug Information"):
                        st.write("**Available columns:**", sorted(list(df.columns)))
                        # Show sample of street-related fields
                        street_related_cols = [col for col in df.columns if any(keyword in col.lower() for keyword in ['street', 'address', 'house', 'name'])]
                        if street_related_cols:
                            st.write("**Street/Address related columns:**", street_related_cols)
                            if len(df) > 0:
                                st.write("**Sample values from street-related fields:**")
                                sample_df = df[street_related_cols].head(5)
                                st.dataframe(sample_df)
                        st.write("**Sample raw data (first row):**")
                        if '_raw' in df.columns and len(df) > 0:
                            sample_raw = df['_raw'].iloc[0]
                            if isinstance(sample_raw, dict):
                                st.json(sample_raw)
                            else:
                                st.write(sample_raw)
                        st.write("**Data sample (first row):**")
                        st.dataframe(df.head(1))
                
                # Render map
                render_map(df)
            else:
                st.warning("⚠️ No data found with current filters.")
                st.info("💡 **Troubleshooting tips:**")
                st.markdown("""
                - Check if the backend is running (may take 30-60s for first request on Render)
                - Try refreshing the page
                - Check if your filters are too restrictive
                - Verify backend URL: `{BACKEND_URL}`
                """.format(BACKEND_URL=BACKEND_URL))
                
        except Exception as e:
            st.error(f"❌ Failed to fetch data: {str(e)[:300]}")
            st.info("💡 **This might be due to:**")
            st.markdown("""
            - Backend is starting up (first request can take 30-60 seconds on Render)
            - Network connectivity issues
            - Backend service is down
            
            **Try refreshing the page in a few moments.**
            """)

if __name__ == "__main__":
    main()
