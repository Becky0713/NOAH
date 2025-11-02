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
    for attempt in range(max_retries):
        try:
            resp = requests.get(url, params=params, timeout=30)
            resp.raise_for_status()
            return resp
        except requests.exceptions.Timeout:
            if attempt < max_retries - 1:
                st.warning(f"Request timeout (attempt {attempt + 1}/{max_retries}). Retrying...")
                time.sleep(2 ** attempt)  # Exponential backoff
                continue
            else:
                raise
        except requests.exceptions.RequestException as e:
            if attempt < max_retries - 1:
                st.warning(f"Request failed (attempt {attempt + 1}/{max_retries}): {e}. Retrying...")
                time.sleep(2 ** attempt)
                continue
            else:
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
    batch_size = 1000  # Fetch in batches of 1000
    
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    while len(all_records) < limit:
        current_limit = min(batch_size, limit - len(all_records))
        
        status_text.text(f"Fetching data... {len(all_records)}/{limit} records")
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
        
        resp = _make_request_with_retry(f"{BACKEND_URL}/v1/records", params=params)
        batch = resp.json()
        
        if not batch:
            break  # No more data
        
        all_records.extend(batch)
        offset += len(batch)
        
        # If we got less than requested, we've reached the end
        if len(batch) < current_limit:
            break
        
        # Stop if we've reached the limit
        if len(all_records) >= limit:
            break
    
    progress_bar.progress(1.0)
    status_text.text(f"✅ Loaded {len(all_records)} records")
    time.sleep(0.5)  # Brief pause to show completion
    progress_bar.empty()
    status_text.empty()
    
    return all_records[:limit]  # Return exactly up to limit

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
        st.markdown("### 🔍 Filters")
        
        # Basic filters
        st.markdown("#### Base Filters")
        
        # Region selection
        try:
            regions = fetch_regions()
            region_options = {r["name"]: r["id"] for r in regions}
            selected_name = st.selectbox("Region", list(region_options.keys()), index=0)
            selected_region = region_options[selected_name]
        except Exception as e:
            st.error(f"Failed to fetch regions: {e}")
            selected_region = "manhattan"
        
        # Sample size
        # Sample size with option to show all
        show_all = st.checkbox("Show All Projects (may take longer)", value=False)
        if show_all:
            sample_size = 10000  # Large number to fetch all (backend will handle pagination if needed)
        else:
            sample_size = st.slider("Sample Size", min_value=10, max_value=5000, value=500, step=50)
        
        # Borough filter
        borough_options = ["", "Manhattan", "Brooklyn", "Queens", "Bronx", "Staten Island"]
        selected_borough = st.selectbox("Borough", borough_options, index=0)
        
        # Unit count range
        st.markdown("#### Unit Count Range")
        col1, col2 = st.columns(2)
        with col1:
            min_units = st.number_input("Min Units", min_value=0, value=0, step=1)
        with col2:
            max_units = st.number_input("Max Units", min_value=0, value=0, step=1, help="0 means no limit")
        
        # Date range
        st.markdown("#### Project Start Date")
        col3, col4 = st.columns(2)
        with col3:
            start_date_from = st.date_input("From", value=None)
        with col4:
            start_date_to = st.date_input("To", value=None)
        
        # Convert dates to strings
        start_date_from_str = start_date_from.strftime("%Y-%m-%d") if start_date_from else ""
        start_date_to_str = start_date_to.strftime("%Y-%m-%d") if start_date_to else ""
        
        # Add More Fields section
        st.markdown("---")
        st.markdown("#### 📋 Add More Fields")
        
        try:
            meta = fetch_field_metadata()
            all_fields = [m["field_name"] for m in meta]
        except Exception as e:
            st.warning(f"Failed to fetch field metadata: {e}")
            # Fallback field list
            all_fields = [
                "community_board", "census_tract", "postcode", "bbl", "bin",
                "council_district", "neighborhood_tabulation_area", "project_name",
                "building_id", "studio_units", "_1_br_units", "_2_br_units", "_3_br_units",
                "extremely_low_income_units", "very_low_income_units", "low_income_units",
                "moderate_income_units", "middle_income_units", "other_income_units",
                "counted_rental_units", "counted_homeownership_units",
                "reporting_construction_type", "extended_affordability_status", "prevailing_wage_status"
            ]
        
        # Core fields (always included)
        core_fields = [
            "project_name", "house_number", "street_name", "total_units", 
            "all_counted_units", "studio_units", "_1_br_units", "_2_br_units", 
            "_3_br_units", "project_completion_date"
        ]
        
        # Available additional fields
        additional_fields = [f for f in all_fields if f not in core_fields]
        
        # Field selection
        selected_additional = st.multiselect(
            "Select additional fields to display:",
            additional_fields,
            default=[],
            help="Choose fields to add to the Info Card"
        )
        
        # Confirm button
        if st.button("✅ Confirm Field Selection", type="primary", use_container_width=True):
            st.session_state.selected_fields = core_fields + selected_additional
            st.session_state.fields_confirmed = True
            st.success(f"Added {len(selected_additional)} additional fields!")
        
        # Show current selection
        if 'selected_fields' in st.session_state:
            st.markdown(f"**Current fields ({len(st.session_state.selected_fields)}):**")
            for field in st.session_state.selected_fields[:5]:  # Show first 5
                st.markdown(f"• {field}")
            if len(st.session_state.selected_fields) > 5:
                st.markdown(f"• ... and {len(st.session_state.selected_fields) - 5} more")
        
        return {
            "region": selected_region,
            "sample_size": sample_size,
            "borough": selected_borough,
            "min_units": min_units,
            "max_units": max_units,
            "start_date_from": start_date_from_str,
            "start_date_to": start_date_to_str
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
    
    # Calculate affordability ratio for color coding
    df_geo["affordability_ratio"] = df_geo.apply(
        lambda row: (row.get("affordable_units", 0) / row.get("total_units", 1)) if row.get("total_units", 0) > 0 else 0, 
        axis=1
    )
    
    # Adjust point size based on total units
    df_geo["radius"] = df_geo["total_units"].fillna(1).apply(lambda x: max(20, min(200, x * 1.5)))
    
    # Color based on affordability ratio (green = high affordability, red = low affordability)
    df_geo["color"] = df_geo["affordability_ratio"].apply(
        lambda x: [0, 255, 0, 140] if x > 0.7 else [255, 255, 0, 140] if x > 0.3 else [255, 0, 0, 140]
    )
    
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
    df_geo['project_id'] = df_geo.get('project_id', '')
    df_geo['borough'] = df_geo.get('borough', df_geo.get('region', ''))
    df_geo['postcode'] = df_geo.get('postcode', '')
    df_geo['building_completion_date'] = df_geo.get('building_completion_date', '')
    df_geo['extremely_low_income_units'] = df_geo.get('extremely_low_income_units', 0)
    df_geo['very_low_income_units'] = df_geo.get('very_low_income_units', 0)
    df_geo['low_income_units'] = df_geo.get('low_income_units', 0)
    df_geo['studio_units'] = df_geo.get('studio_units', 0)
    df_geo['_1_br_units'] = df_geo.get('_1_br_units', 0)
    df_geo['_2_br_units'] = df_geo.get('_2_br_units', 0)
    df_geo['counted_rental_units'] = df_geo.get('counted_rental_units', 0)
    
    # Format building completion date (show "In Progress" if empty)
    df_geo['building_completion_display'] = df_geo['building_completion_date'].apply(
        lambda x: "In Progress" if pd.isna(x) or not x or str(x).strip() == '' else str(x)
    )
    
    # PyDeck uses {field_name} for variables in tooltip
    tooltip = {
        "html": """
        <b>Project ID: {project_id}</b><br/>
        Borough: {borough}<br/>
        Postcode: {postcode}<br/>
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
        
        # Get unique project IDs for suggestions
        project_ids = sorted([str(pid) for pid in df_geo['project_id'].dropna().unique().tolist() if pid])
        
        # Search input with autocomplete suggestions
        search_col1, search_col2 = st.columns([3, 1])
        
        with search_col1:
            search_query = st.text_input(
                "Enter Project ID to search:",
                placeholder="Type project ID or select from dropdown...",
                key="project_id_search"
            )
        
        with search_col2:
            # Quick select from dropdown
            selected_from_dropdown = st.selectbox(
                "Or select from list:",
                options=["None"] + project_ids[:100],  # Limit dropdown to first 100 for performance
                index=0,
                key="project_id_dropdown"
            )
        
        # Determine which search to use
        search_id = None
        if selected_from_dropdown != "None":
            search_id = selected_from_dropdown
        elif search_query and search_query.strip():
            search_id = search_query.strip()
        
        # Search for project
        if search_id:
            matching_projects = df_geo[df_geo['project_id'].astype(str).str.contains(search_id, case=False, na=False)]
            
            if not matching_projects.empty:
                if len(matching_projects) == 1:
                    # Exact match, show it
                    selected_project = matching_projects.iloc[0].to_dict()
                    st.session_state.selected_project = selected_project
                    st.session_state.show_info_card = True
                    st.success(f"✅ Found Project ID: {search_id}")
                    st.rerun()
                else:
                    # Multiple matches, show list
                    st.info(f"Found {len(matching_projects)} matching projects. Select one:")
                    match_options = [f"{row.get('project_id', 'N/A')} - {row.get('project_name', 'N/A')}" 
                                    for idx, row in matching_projects.head(10).iterrows()]
                    selected_match = st.selectbox("Select project:", options=["None"] + match_options)
                    
                    if selected_match != "None":
                        match_idx = match_options.index(selected_match)
                        selected_project = matching_projects.iloc[match_idx].to_dict()
                        st.session_state.selected_project = selected_project
                        st.session_state.show_info_card = True
                        st.rerun()
            else:
                st.warning(f"⚠️ No project found with ID: {search_id}")
        
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
    """Render the info card section as a floating panel"""
    st.markdown("### 📋 Project Details")
    
    # Add a close button
    if st.button("❌ Close", type="secondary", use_container_width=True):
        st.session_state.show_info_card = False
        st.session_state.selected_project = None
        st.rerun()
    
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
        st.write(f"**Building ID:** {get_val('building_id')}")
        st.write(f"**BBL:** {get_val('bbl')}")
        st.write(f"**BIN:** {get_val('bin')}")
        
        # Project Dates Section
        st.markdown("#### 📅 Project Dates")
        st.write(f"**Project Start Date:** {get_val('project_start_date')}")
        st.write(f"**Project Completion Date:** {get_val('project_completion_date')}")
        
        building_date = get_val('building_completion_date', 'In Progress')
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
    
    # Top navigation
    render_top_navigation()
    
    # Collapsible filters in sidebar
    with st.sidebar:
        st.markdown("### 🔧 Filters")
        filter_params = render_filter_panel()
    
    # Main layout: Map takes 70-75% width, info card takes 20-25%
    if st.session_state.show_info_card:
        col_map, col_info = st.columns([0.75, 0.25])
    else:
        col_map = st.container()
        col_info = None
    
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
            
            if records:
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
                    
                    # Extract all fields from raw data
                    for key in all_keys:
                        if key not in df.columns:
                            df[key] = raw_data.apply(lambda x: x.get(key) if isinstance(x, dict) else None)
                    
                    # Also extract specific fields we need (with multiple possible field name variations)
                    field_mappings = {
                        'project_id': ['project_id', 'projectid', 'id'],
                        'building_id': ['building_id', 'buildingid', 'building'],
                        'building_completion_date': ['building_completion_date', 'buildingcompletiondate', 'building_completion'],
                        'extended_affordability_status': ['extended_affordability_status', 'extendedaffordabilitystatus', 'extended_affordability'],
                    }
                    
                    for target_field, possible_names in field_mappings.items():
                        if target_field not in df.columns:
                            for name in possible_names:
                                if name in all_keys:
                                    df[target_field] = raw_data.apply(lambda x: x.get(name) if isinstance(x, dict) else None)
                                    break
                
                # Ensure required fields exist with defaults (handle missing columns)
                # Priority: use extracted field, then region, then empty
                if 'borough' not in df.columns:
                    df['borough'] = df.get('region', '')
                elif df['borough'].isna().all():
                    df['borough'] = df.get('region', '')
                
                for col in ['project_id', 'postcode', 'building_completion_date']:
                    if col not in df.columns:
                        df[col] = ''
                    # Fill NaN values
                    df[col] = df[col].fillna('')
                
                # Set defaults for numeric fields and ensure they're numeric
                numeric_fields = ['extremely_low_income_units', 'very_low_income_units', 'low_income_units',
                                 'moderate_income_units', 'middle_income_units', 'other_income_units',
                                 'studio_units', '_1_br_units', '_2_br_units', '_3_br_units',
                                 '_4_br_units', '_5_br_units', '_6_br_units', 'unknown_br_units',
                                 'counted_rental_units', 'counted_homeownership_units',
                                 'total_units', 'all_counted_units']
                for col in numeric_fields:
                    if col not in df.columns:
                        df[col] = 0
                    else:
                        # Convert to numeric, handling strings and NaN
                        df[col] = pd.to_numeric(df[col], errors='coerce')
                        df[col] = df[col].fillna(0).astype(int)
                
                st.write(f"📍 Showing {len(df)} projects")
                
                # Render map
                render_map(df)
            else:
                st.info("No data found with current filters.")
                
        except Exception as e:
            st.error(f"Failed to fetch data: {e}")
    
    if col_info:
        with col_info:
            # Right info card - floating panel style
            render_info_card_section()

if __name__ == "__main__":
    main()
