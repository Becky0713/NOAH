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

@st.cache_data(show_spinner=False, ttl=60)
def fetch_records(
    fields: List[str],
    limit: int,
    borough: str = "",
    min_units: int = 0,
    max_units: int = 0,
    start_date_from: str = "",
    start_date_to: str = ""
) -> List[Dict[str, Any]]:
    params = {
        "fields": ",".join(fields),
        "limit": limit,
        "min_units": min_units,
        "max_units": max_units,
        "start_date_from": start_date_from,
        "start_date_to": start_date_to
    }
    if borough:
        params["borough"] = borough
    resp = _make_request_with_retry(f"{BACKEND_URL}/v1/records", params=params)
    return resp.json()

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
        sample_size = st.slider("Sample Size", min_value=10, max_value=1000, value=100, step=10)
        
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
    
    # Create tooltip with median income if available
    median_income_tooltip = ""
    if 'median_household_income' in df_geo.columns and df_geo['median_household_income'].notna().any():
        median_income_tooltip = "<br/>Median Household Income: ${median_household_income:,.0f}"
    
    # PyDeck uses double curly braces for variables
    tooltip = {
        "html": """
        <b>{project_name}</b><br/>
        Address: {house_number} {street_name}<br/>
        Borough: {region}<br/>
        Total Units: {total_units}<br/>
        Affordable Units: {affordable_units}<br/>
        Affordability: {affordability_ratio:.1%}<br/>
        Studio: {studio_units} | 1BR: {_1_br_units} | 2BR: {_2_br_units} | 3BR: {_3_br_units}""" + median_income_tooltip + """
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
    
    # Handle map interactions (simplified for now)
    # Note: PyDeck doesn't support click events directly in Streamlit
    # We'll use a simple table selection instead
    if not df_geo.empty:
        st.markdown("### 📊 Select a Project")
        # Display project selection table
        st.dataframe(
            df_geo[['project_name', 'address', 'total_units', 'affordable_units', 'region']].head(20),
            use_container_width=True,
            hide_index=True
        )
        
        # Add project selection using selectbox
        if not df_geo.empty:
            project_options = [f"{row['project_name']} - {row['address']}" for idx, row in df_geo.head(20).iterrows()]
            selected_option = st.selectbox("Select a project to view details:", ["None"] + project_options)
            
            if selected_option != "None":
                selected_idx = project_options.index(selected_option)
                selected_project = df_geo.iloc[selected_idx].to_dict()
                st.session_state.selected_project = selected_project
                st.session_state.show_info_card = True
                st.rerun()

def render_info_card_section():
    """Render the info card section as a floating panel"""
    st.markdown("### 📋 Project Details")
    
    # Add a close button
    if st.button("❌ Close", type="secondary", use_container_width=True):
        st.session_state.show_info_card = False
        st.session_state.selected_project = None
        st.rerun()
    
    if st.session_state.selected_project is not None:
        # Display selected project details
        project = st.session_state.selected_project
        
        # Basic Info Section
        st.markdown("#### 📍 Basic Info")
        st.write(f"**Project:** {project.get('project_name', 'N/A')}")
        st.write(f"**Address:** {project.get('house_number', '')} {project.get('street_name', '')}")
        st.write(f"**Borough:** {project.get('region', 'N/A')}")
        st.write(f"**Postcode:** {project.get('postcode', 'N/A')}")
        if project.get('median_household_income'):
            st.write(f"**Median Household Income:** ${project.get('median_household_income'):,.0f}")
        
        # Units Info Section
        st.markdown("#### 🏠 Units Info")
        col1, col2 = st.columns(2)
        with col1:
            st.metric("Total Units", project.get('total_units', 0))
            st.metric("Affordable Units", project.get('affordable_units', 0))
        with col2:
            st.metric("Studio", project.get('studio_units', 0))
            st.metric("1-Bedroom", project.get('_1_br_units', 0))
        
        # Timeline Section
        st.markdown("#### 📅 Timeline")
        st.write(f"**Start Date:** {project.get('project_start_date', 'N/A')}")
        st.write(f"**Completion Date:** {project.get('project_completion_date', 'N/A')}")
        
        # Additional Info Section (if user confirmed new fields)
        if st.session_state.get('fields_confirmed', False):
            st.markdown("#### ℹ️ Additional Info")
            additional_fields = [f for f in st.session_state.selected_fields 
                               if f not in ['project_name', 'house_number', 'street_name', 'total_units', 
                                          'all_counted_units', 'studio_units', '_1_br_units', '_2_br_units', 
                                          '_3_br_units', 'project_start_date', 'project_completion_date', 'region', 'postcode']]
            
            for field in additional_fields:
                if field in project and project[field] is not None:
                    st.write(f"**{field.replace('_', ' ').title()}:** {project[field]}")
    else:
        st.info("Click on a project marker on the map to view details.")

def main():
    """Main application"""
    st.set_page_config(
        page_title="NYC Housing Hub",
        layout="wide",
        initial_sidebar_state="collapsed"  # Start with collapsed sidebar
    )
    
    # Initialize session state
    if 'selected_fields' not in st.session_state:
        st.session_state.selected_fields = [
            "project_name", "house_number", "street_name", "latitude", "longitude", "total_units", 
            "all_counted_units", "studio_units", "_1_br_units", "_2_br_units", 
            "_3_br_units", "project_completion_date"
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
            records = fetch_records(
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
                
                # Fetch and merge median income data
                income_df = fetch_median_income_data()
                if not income_df.empty:
                    # Try to match by geo_id if available
                    if 'geo_id' in df.columns:
                        # Normalize geo_id formats (handle both 1400000US and 0600000US)
                        income_df_normalized = income_df.copy()
                        income_df_normalized['geo_id_normalized'] = income_df_normalized['geo_id'].astype(str).str.replace('1400000US', '0600000US')
                        df['geo_id_normalized'] = df['geo_id'].astype(str).str.replace('1400000US', '0600000US')
                        
                        df = df.merge(
                            income_df_normalized[['geo_id_normalized', 'median_household_income']],
                            on='geo_id_normalized',
                            how='left'
                        )
                        # Also try direct match with original geo_id
                        df = df.merge(
                            income_df[['geo_id', 'median_household_income']],
                            on='geo_id',
                            how='left',
                            suffixes=('', '_direct')
                        )
                        # Use direct match if normalized didn't work
                        df['median_household_income'] = df['median_household_income'].fillna(df.get('median_household_income_direct', pd.Series()))
                        df = df.drop(columns=['median_household_income_direct'], errors='ignore')
                    # Try to match by tract_name if geo_id doesn't work
                    if 'tract_name' in df.columns and ('median_household_income' not in df.columns or df['median_household_income'].isna().all()):
                        df = df.merge(
                            income_df[['tract_name', 'median_household_income']],
                            on='tract_name',
                            how='left',
                            suffixes=('', '_from_tract')
                        )
                        df['median_household_income'] = df['median_household_income'].fillna(df.get('median_household_income_from_tract', pd.Series()))
                        df = df.drop(columns=['median_household_income_from_tract'], errors='ignore')
                
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
