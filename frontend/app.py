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

def render_top_navigation():
    """Render top navigation bar"""
    col1, col2, col3 = st.columns([3, 1, 1])
    
    with col1:
        st.markdown("# 🏠 NYC Housing Hub")
    
    with col2:
        if st.button("📚 Glossary", use_container_width=True):
            st.switch_page("pages/glossary.py")
    
    with col3:
        if st.button("ℹ️ About", use_container_width=True):
            st.switch_page("pages/about.py")

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
    # Debug: Show available columns
    st.write(f"🔍 Debug - Available columns: {list(data.columns)}")
    
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
    
    # Adjust point size and color based on unit count
    df_geo["radius"] = df_geo["total_units"].fillna(1).apply(lambda x: max(20, min(200, x * 2)))
    df_geo["color"] = df_geo["total_units"].fillna(0).apply(
        lambda x: [0, 255, 0, 140] if x < 50 else [255, 165, 0, 140] if x < 200 else [255, 0, 0, 140]
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
    
    # Create tooltip
    tooltip = {
        "html": """
        <b>{project_name}</b><br/>
        Address: {house_number} {street_name}<br/>
        Borough: {borough}<br/>
        Total Units: {total_units}<br/>
        Affordable Units: {all_counted_units}<br/>
        Studio: {studio_units} | 1BR: {_1_br_units} | 2BR: {_2_br_units} | 3BR: {_3_br_units}
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
            tooltip=tooltip
        ),
        use_container_width=True
    )

def render_info_card(data: pd.DataFrame, selected_fields: List[str]):
    """Render right info card with selected fields"""
    if data.empty:
        st.info("No data available to display.")
        return
    
    st.markdown("### 📊 Project Information")
    
    # Show first few records
    display_data = data.head(10)  # Show first 10 records
    
    for idx, row in display_data.iterrows():
        with st.expander(f"🏠 {row.get('project_name', 'Unknown Project')}", expanded=False):
            # Create columns for better layout
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown("**Basic Info:**")
                st.write(f"**Address:** {row.get('house_number', '')} {row.get('street_name', '')}")
                st.write(f"**Borough:** {row.get('borough', 'Unknown')}")
                st.write(f"**Total Units:** {row.get('total_units', 0)}")
                st.write(f"**Affordable Units:** {row.get('all_counted_units', 0)}")
            
            with col2:
                st.markdown("**Unit Distribution:**")
                st.write(f"**Studio:** {row.get('studio_units', 0)}")
                st.write(f"**1-Bedroom:** {row.get('_1_br_units', 0)}")
                st.write(f"**2-Bedroom:** {row.get('_2_br_units', 0)}")
                st.write(f"**3-Bedroom:** {row.get('_3_br_units', 0)}")
            
            # Show additional fields if selected
            additional_fields = [f for f in selected_fields if f not in [
                'project_name', 'house_number', 'street_name', 'total_units', 
                'all_counted_units', 'studio_units', '_1_br_units', '_2_br_units', '_3_br_units'
            ]]
            
            if additional_fields:
                st.markdown("**Additional Information:**")
                for field in additional_fields:
                    if field in row and pd.notna(row[field]):
                        field_display = field.replace('_', ' ').title()
                        st.write(f"**{field_display}:** {row[field]}")
            
            # Completion year
            completion_date = row.get('project_completion_date', '')
            if completion_date:
                try:
                    year = pd.to_datetime(completion_date).year
                    st.write(f"**Completion Year:** {year}")
                except:
                    st.write(f"**Completion Date:** {completion_date}")

def main():
    """Main application"""
    st.set_page_config(
        page_title="NYC Housing Hub",
        layout="wide",
        initial_sidebar_state="expanded"
    )
    
    # Initialize session state
    if 'selected_fields' not in st.session_state:
        st.session_state.selected_fields = [
            "project_name", "house_number", "street_name", "total_units", 
            "all_counted_units", "studio_units", "_1_br_units", "_2_br_units", 
            "_3_br_units", "project_completion_date"
        ]
        st.session_state.fields_confirmed = True
    
    # Top navigation
    render_top_navigation()
    
    # Main layout
    col1, col2, col3 = st.columns([1, 2, 1])
    
    with col1:
        # Left filter panel
        filter_params = render_filter_panel()
    
    with col2:
        # Center map
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
                st.write(f"📍 Showing {len(df)} projects")
                
                # Debug: Show sample data
                st.write("🔍 Debug - Sample data structure:")
                st.write(df.head(2))
                
                # Render map
                render_map(df)
            else:
                st.info("No data found with current filters.")
                
        except Exception as e:
            st.error(f"Failed to fetch data: {e}")
    
    with col3:
        # Right info card
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
                render_info_card(df, st.session_state.selected_fields)
            else:
                st.info("No data available for info card.")
                
        except Exception as e:
            st.error(f"Failed to load info card: {e}")

if __name__ == "__main__":
    main()
