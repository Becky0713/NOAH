import json
from typing import Any, Dict, List

import pandas as pd
import requests
import streamlit as st
import pydeck as pdk


# Backend base URL - works for both local and deployed environments
import os
BACKEND_URL = os.getenv("BACKEND_URL", "http://127.0.0.1:8000")


@st.cache_data(show_spinner=False, ttl=60)
def fetch_regions() -> List[Dict[str, Any]]:
    resp = requests.get(f"{BACKEND_URL}/v1/regions", timeout=15)
    resp.raise_for_status()
    return resp.json()


@st.cache_data(show_spinner=False, ttl=60)
def fetch_summary(region_id: str, limit: int = 25) -> Dict[str, Any]:
    params = {"region_id": region_id, "limit": limit}
    resp = requests.get(f"{BACKEND_URL}/v1/housing/summary", params=params, timeout=20)
    resp.raise_for_status()
    return resp.json()


@st.cache_data(show_spinner=False, ttl=300)
def fetch_field_metadata() -> List[Dict[str, Any]]:
    resp = requests.get(f"{BACKEND_URL}/metadata/fields", timeout=20)
    resp.raise_for_status()
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
    resp = requests.get(f"{BACKEND_URL}/v1/records", params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()


def render_metrics(summary: Dict[str, Any]) -> None:
    region_summary = summary.get("region_summary", {})
    listing_count = region_summary.get("listing_count")
    median_rent = region_summary.get("median_rent")
    average_rent = region_summary.get("average_rent")
    vacancy_rate = region_summary.get("vacancy_rate")

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Listings", f"{listing_count:,}" if listing_count is not None else "-")
    c2.metric("Median Rent", f"${median_rent:,.0f}" if median_rent else "-")
    c3.metric("Average Rent", f"${average_rent:,.0f}" if average_rent else "-")
    c4.metric("Vacancy Rate", f"{vacancy_rate:.1%}" if vacancy_rate is not None else "-")


def listings_to_df(listings: List[Dict[str, Any]]) -> pd.DataFrame:
    if not listings:
        return pd.DataFrame(columns=["id", "address", "latitude", "longitude", "bedrooms", "bathrooms", "rent", "source"])
    return pd.DataFrame(listings)


def render_map(df: pd.DataFrame) -> None:
    if df.empty or df[["latitude", "longitude"]].dropna().empty:
        st.info("No geographic coordinates available for mapping.")
        return

    df_geo = df.dropna(subset=["latitude", "longitude"]).copy()
    midpoint = (df_geo["latitude"].mean(), df_geo["longitude"].mean())

    # Adjust point size and color based on unit count
    df_geo["radius"] = df_geo["total_units"].fillna(1).apply(lambda x: max(20, min(200, x * 2)))
    df_geo["color"] = df_geo["total_units"].fillna(0).apply(
        lambda x: [255, 140, 0, 140] if x < 50 else [255, 69, 0, 140] if x < 200 else [255, 0, 0, 140]
    )

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

    tooltip = {
        "html": """
        <b>{address}</b><br/>
        Borough: {region}<br/>
        Total Units: {total_units}<br/>
        Affordable Units: {affordable_units}<br/>
        Project Start: {project_start_date}
        """,
        "style": {"backgroundColor": "#262730", "color": "white"},
    }

    view_state = pdk.ViewState(latitude=midpoint[0], longitude=midpoint[1], zoom=11, pitch=0)
    st.pydeck_chart(pdk.Deck(layers=[layer], initial_view_state=view_state, tooltip=tooltip), width="stretch")


def render_distribution(df: pd.DataFrame) -> None:
    # Simple distribution example using total_units
    if df.empty or ("total_units" not in df.columns) or df["total_units"].dropna().empty:
        st.info("No unit count distribution data available.")
        return
    st.bar_chart(df["total_units"].dropna(), width="stretch")


def main() -> None:
    st.set_page_config(page_title="NYC Housing Hub", layout="wide")
    st.title("NYC Housing Hub — Affordable Housing Dashboard")

    with st.sidebar:
        st.subheader("Filters")
        try:
            regions = fetch_regions()
        except Exception as exc:  # noqa: BLE001
            st.error(f"Failed to fetch regions: {exc}")
            st.stop()

        region_options = {r["name"]: r["id"] for r in regions}
        selected_name = st.selectbox("Region", list(region_options.keys()), index=0)
        selected_region = region_options[selected_name]
        sample_size = st.slider("Sample Size (Records)", min_value=5, max_value=500, value=50, step=5)
        
        # Borough filtering (for /v1/records)
        borough_options = ["", "Manhattan", "Brooklyn", "Queens", "Bronx", "Staten Island"]
        selected_borough = st.selectbox("Borough Filter", borough_options, index=0, help="Empty value means no filter")
        
        # Unit count range filtering
        st.subheader("Unit Count Filter")
        col1, col2 = st.columns(2)
        with col1:
            min_units = st.number_input("Min Units", min_value=0, value=0, step=1)
        with col2:
            max_units = st.number_input("Max Units", min_value=0, value=0, step=1, help="0 means no limit")
        
        # Date range filtering
        st.subheader("Project Start Date Filter")
        col3, col4 = st.columns(2)
        with col3:
            start_date_from = st.date_input("Start Date (From)", value=None)
        with col4:
            start_date_to = st.date_input("Start Date (To)", value=None)
        
        # Convert dates to strings
        start_date_from_str = start_date_from.strftime("%Y-%m-%d") if start_date_from else ""
        start_date_to_str = start_date_to.strftime("%Y-%m-%d") if start_date_to else ""

        # Field multi-select (from /metadata/fields)
        try:
            meta = fetch_field_metadata()
        except Exception as exc:  # noqa: BLE001
            st.error(f"Failed to fetch field metadata: {exc}")
            st.stop()
        all_fields = [m["field_name"] for m in meta]

        core_raw_fields = [
            "house_number",
            "street_name",
            "latitude",
            "longitude",
            "borough",
            "total_units",
            "affordable_units",
            "project_start_date",
            "project_completion_date",
        ]
        optional_fields = [f for f in all_fields if f not in core_raw_fields]
        selected_optional = st.multiselect("Additional Fields", optional_fields, default=[])

    # Backend /v1/housing/summary retained (example metrics), record data from /v1/records
    try:
        summary = fetch_summary(selected_region, limit=sample_size)
    except Exception as exc:  # noqa: BLE001
        st.warning(f"Sample summary fetch failed (doesn't affect record queries): {exc}")
        summary = {"region_summary": {"listing_count": None, "median_rent": None, "average_rent": None, "vacancy_rate": None}}

    render_metrics(summary)

    select_fields = core_raw_fields + selected_optional
    try:
        records = fetch_records(
            select_fields, 
            limit=sample_size, 
            borough=selected_borough,
            min_units=min_units,
            max_units=max_units,
            start_date_from=start_date_from_str,
            start_date_to=start_date_to_str
        )
    except Exception as exc:  # noqa: BLE001
        st.error(f"Failed to fetch records: {exc}")
        st.stop()

    # Normalize to DataFrame (using standard keys provided by backend)
    df_norm = pd.DataFrame(records)
    # Extract additional columns selected by user from _raw, append to display table
    extra_cols: List[str] = selected_optional
    if "_raw" in df_norm.columns and extra_cols:
        raw_df = pd.json_normalize(df_norm.pop("_raw"))
        for col in extra_cols:
            if col in raw_df.columns:
                df_norm[col] = raw_df[col]

    # Map + Distribution
    left, right = st.columns((2, 1))
    with left:
        st.subheader("Map")
        render_map(df_norm)
    with right:
        st.subheader("Unit Count Distribution (Sample)")
        render_distribution(df_norm)

    with st.expander("View Data", expanded=False):
        st.dataframe(df_norm, width="stretch", hide_index=True)


if __name__ == "__main__":
    main()



