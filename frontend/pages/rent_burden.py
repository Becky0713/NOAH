"""
Rent Burden Visualization Page
Displays rent burden rates by NYC boroughs in bar chart
"""

import streamlit as st
import pandas as pd
import psycopg2
from pathlib import Path

# Set page config for this page
st.set_page_config(
    page_title="NYC Housing Hub - Rent Burden",
    page_icon="🗽",
    layout="wide"
)

def get_db_connection():
    """Get database connection from Streamlit secrets"""
    # Streamlit secrets with nested structure
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
        st.error("Please add your database credentials to Streamlit Secrets")
        st.stop()
    except Exception as e:
        st.error(f"❌ Failed to read secrets: {e}")
        st.stop()

def fetch_rent_burden_data():
    """Fetch rent burden data from PostgreSQL"""
    try:
        conn = get_db_connection()
        query = """
        SELECT 
            geo_id,
            tract_name,
            rent_burden_rate,
            severe_burden_rate
        FROM rent_burden
        WHERE rent_burden_rate IS NOT NULL;
        """
        df = pd.read_sql_query(query, conn)
        conn.close()
        return df
    except Exception as e:
        st.error(f"❌ Database connection error: {e}")
        return pd.DataFrame()

def get_variable_mapping():
    """Get mapping from variable codes to income_bracket and rent_bracket"""
    # Based on B25074 variable structure
    mapping = {}
    
    # Income brackets and their variable ranges
    income_ranges = [
        ("Less than $10,000", (2, 8)),
        ("$10,000 to $19,999", (9, 15)),
        ("$20,000 to $34,999", (16, 22)),
        ("$35,000 to $49,999", (23, 29)),
        ("$50,000 to $74,999", (30, 36)),
        ("$75,000 to $99,999", (37, 43)),
        ("$100,000 to $149,999", (44, 50)),
        ("$150,000 or more", (51, 57))
    ]
    
    # Rent brackets mapping
    rent_brackets = [
        "Less than 20.0 percent",
        "20.0 to 24.9 percent",
        "25.0 to 29.9 percent",
        "30.0 to 34.9 percent",
        "35.0 to 39.9 percent",
        "40.0 to 49.9 percent",
        "50.0 percent or more"
    ]
    
    # Build mapping
    var_num = 2  # Start from B25074_002E (skip 001 which is Total)
    for income_bracket, (start, end) in income_ranges:
        for rent_idx, rent_bracket in enumerate(rent_brackets):
            if var_num <= 57:  # Max variable number based on typical B25074 structure
                variable = f"B25074_{var_num:03d}E"
                mapping[variable] = {
                    'income_bracket': income_bracket,
                    'rent_bracket': rent_bracket,
                    'rent_bracket_short': f"Rent {rent_brackets[rent_idx][:10]}"
                }
                var_num += 1
    
    return mapping

def fetch_rent_income_distribution():
    """Fetch rent income distribution data from PostgreSQL"""
    try:
        conn = get_db_connection()
        query = """
        SELECT 
            geo_id,
            tract_name,
            variable,
            household_count
        FROM rent_income_distribution
        WHERE household_count IS NOT NULL;
        """
        df = pd.read_sql_query(query, conn)
        conn.close()
        return df
    except Exception as e:
        # Table might not exist yet, return empty dataframe
        return pd.DataFrame()

def render_income_rent_distribution():
    """Render stacked bar chart showing income bracket vs rent burden"""
    st.subheader("📊 Income-Rent Burden Distribution")
    
    with st.spinner("Loading income-rent distribution data..."):
        df = fetch_rent_income_distribution()
    
    if df.empty:
        st.info("💡 Income-rent distribution data is not available. Please ensure `rent_income_distribution` table exists in your database.")
        return
    
    # Get variable mapping
    var_mapping = get_variable_mapping()
    
    # Extract borough from tract_name
    def get_borough_from_tract_name(tract_name):
        if not tract_name:
            return None
        tract_str = str(tract_name)
        if " borough" in tract_str:
            return tract_str.split(" borough")[0].strip()
        return None
    
    df['borough'] = df['tract_name'].apply(get_borough_from_tract_name)
    df = df[df['borough'].notna()]
    
    # Add income_bracket and rent_bracket from mapping
    df['income_bracket'] = df['variable'].map(lambda x: var_mapping.get(x, {}).get('income_bracket'))
    df['rent_bracket'] = df['variable'].map(lambda x: var_mapping.get(x, {}).get('rent_bracket'))
    
    # Filter out unmapped variables
    df = df[df['income_bracket'].notna() & df['rent_bracket'].notna()]
    
    if df.empty:
        st.warning("⚠️ Could not map variables to income/rent brackets. Please check variable codes.")
        return
    
    # Borough selector
    boroughs = sorted(df['borough'].unique())
    selected_borough = st.selectbox(
        "Select Borough:",
        options=["All"] + list(boroughs),
        index=0
    )
    
    # Filter by borough
    if selected_borough != "All":
        df_filtered = df[df['borough'] == selected_borough].copy()
    else:
        df_filtered = df.copy()
    
    # Aggregate by income_bracket and rent_bracket
    aggregated = df_filtered.groupby(['income_bracket', 'rent_bracket'])['household_count'].sum().reset_index()
    
    # Pivot for stacked bar chart
    pivot_df = aggregated.pivot(index='income_bracket', columns='rent_bracket', values='household_count').fillna(0)
    
    # Order income brackets
    income_order = [
        "Less than $10,000",
        "$10,000 to $19,999",
        "$20,000 to $34,999",
        "$35,000 to $49,999",
        "$50,000 to $74,999",
        "$75,000 to $99,999",
        "$100,000 to $149,999",
        "$150,000 or more"
    ]
    
    # Filter to only include existing brackets
    income_order = [inc for inc in income_order if inc in pivot_df.index]
    pivot_df = pivot_df.loc[income_order]
    
    # Order rent brackets (from low to high burden)
    rent_order = [
        "Less than 20.0 percent",
        "20.0 to 24.9 percent",
        "25.0 to 29.9 percent",
        "30.0 to 34.9 percent",
        "35.0 to 39.9 percent",
        "40.0 to 49.9 percent",
        "50.0 percent or more"
    ]
    
    # Only include columns that exist
    rent_order = [rent for rent in rent_order if rent in pivot_df.columns]
    pivot_df = pivot_df[rent_order]
    
    # Create stacked bar chart
    import plotly.graph_objects as go
    
    fig = go.Figure()
    
    # Color map for rent brackets
    colors = {
        "Less than 20.0 percent": "#10b981",      # Green (low burden)
        "20.0 to 24.9 percent": "#84cc16",        # Light green
        "25.0 to 29.9 percent": "#eab308",        # Yellow
        "30.0 to 34.9 percent": "#f97316",        # Orange
        "35.0 to 39.9 percent": "#ef4444",       # Red
        "40.0 to 49.9 percent": "#dc2626",       # Dark red
        "50.0 percent or more": "#991b1b"        # Very dark red (severe burden)
    }
    
    # Add traces for each rent bracket
    for rent_bracket in rent_order:
        if rent_bracket in pivot_df.columns:
            fig.add_trace(go.Bar(
                name=rent_bracket.replace(" percent", "%").replace("0.0", "0"),
                x=pivot_df.index,
                y=pivot_df[rent_bracket],
                marker_color=colors.get(rent_bracket, "#94a3b8"),
                text=[f"{int(x):,}" if x > 0 else "" for x in pivot_df[rent_bracket]],
                textposition='inside',
            ))
    
    # Update layout
    fig.update_layout(
        barmode='stack',
        xaxis_title="Income Bracket",
        yaxis_title="Household Count",
        height=600,
        legend=dict(
            title="Rent Burden (% of Income)",
            orientation="v",
            yanchor="top",
            y=1,
            xanchor="left",
            x=1.05
        ),
        title=f"Household Rent Burden by Income Bracket{' - ' + selected_borough if selected_borough != 'All' else ''}",
        hovermode='x unified'
    )
    
    st.plotly_chart(fig, use_container_width=True)
    
    # Download button
    csv_download = aggregated.to_csv(index=False)
    st.download_button(
        "📥 Download Income-Rent Distribution as CSV",
        csv_download,
        "rent_income_distribution.csv",
        "text/csv"
    )

def aggregate_by_borough(df):
    """Aggregate rent burden data by borough"""
    # Extract borough from tract_name
    # Format: "Bronx borough, Bronx County, New York"
    def get_borough_from_tract_name(tract_name):
        if not tract_name:
            return None
        tract_str = str(tract_name)
        # Extract text before " borough"
        if " borough" in tract_str:
            borough = tract_str.split(" borough")[0].strip()
            return borough
        return None
    
    # Add borough column from tract_name
    df['borough'] = df['tract_name'].apply(get_borough_from_tract_name)
    
    # Filter out None boroughs
    df = df[df['borough'].notna()]
    
    # Aggregate by borough
    borough_stats = df.groupby('borough').agg({
        'rent_burden_rate': 'mean',
        'severe_burden_rate': 'mean'
    }).reset_index()
    
    # Sort by borough name
    borough_order = ["Bronx", "Brooklyn", "Manhattan", "Queens", "Staten Island"]
    borough_stats['borough'] = pd.Categorical(borough_stats['borough'], categories=borough_order, ordered=True)
    borough_stats = borough_stats.sort_values('borough')
    
    return borough_stats

def render_rent_burden_page():
    """Render the main rent burden visualization page"""
    
    # Page header
    st.title("🗽 NYC Rent Burden Dashboard")
    st.markdown("""
    Visualize rent burden rates across NYC boroughs. **Darker colors** indicate 
    higher rent burden (less affordable housing), while **lighter colors** indicate 
    more affordable housing.
    """)
    
    # Add back to dashboard button
    col1, col2, col3 = st.columns([1, 1, 1])
    with col2:
        if st.button("← Back to Dashboard", type="secondary", use_container_width=True):
            st.switch_page("app.py")
    
    # Load data
    with st.spinner("Loading rent burden data..."):
        try:
            df = fetch_rent_burden_data()
        except Exception as e:
            st.error(f"❌ Error loading data: {e}")
            df = pd.DataFrame()
    
    if df.empty:
        st.warning("⚠️ No rent burden data available.")
        st.info("📋 **Quick Check:**")
        st.text("1. Ensure rent_burden table exists in your database")
        st.text("2. Set DB_* environment variables in Streamlit Secrets")
        st.text("3. Verify database connection is working")
        st.stop()
    
    # Aggregate by borough
    borough_stats = aggregate_by_borough(df)
    
    if borough_stats.empty:
        st.warning("⚠️ Could not extract borough information from tract_name.")
        st.info("Please ensure tract_name follows format: 'Bronx borough, Bronx County, New York'")
        st.dataframe(df.head(10))
        st.stop()
    
    # Display summary statistics
    st.subheader("📊 Summary Statistics by Borough")
    
    # Add definition comments
    st.markdown("""
    **Cost Burden (Rent Burden):** Households paying more than 30% of income toward gross rent (Rent / Income > 0.30)
    
    **Severe Cost Burden (Severe Rent Burden):** Households paying more than 50% of income toward gross rent (Rent / Income > 0.50)
    """)
    
    # Create metrics
    col1, col2, col3, col4, col5 = st.columns(5)
    metrics_data = []
    for idx, row in borough_stats.iterrows():
        metrics_data.append({
            'borough': row['borough'],
            'rent_burden': row['rent_burden_rate'],
            'severe_burden': row['severe_burden_rate']
        })
    
    for i, col in enumerate([col1, col2, col3, col4, col5]):
        with col:
            if i < len(metrics_data):
                data = metrics_data[i]
                st.metric(
                    data['borough'],
                    f"{data['rent_burden']:.1%}",
                    delta=f"Severe: {data['severe_burden']:.1%}",
                    delta_color="inverse"
                )
    
    st.divider()
    
    # Create bar chart
    st.subheader("📊 Rent Burden Rate by Borough")
    
    import plotly.graph_objects as go
    from plotly.subplots import make_subplots
    
    # Create grouped bar chart
    fig = go.Figure()
    
    # Add rent burden rate bars
    fig.add_trace(go.Bar(
        name='Rent Burden Rate',
        x=borough_stats['borough'],
        y=borough_stats['rent_burden_rate'],
        marker_color='#3b82f6',  # Blue
        text=[f"{x:.1%}" for x in borough_stats['rent_burden_rate']],
        textposition='outside',
    ))
    
    # Add severe burden rate bars
    fig.add_trace(go.Bar(
        name='Severe Burden Rate',
        x=borough_stats['borough'],
        y=borough_stats['severe_burden_rate'],
        marker_color='#dc2626',  # Darker red
        text=[f"{x:.1%}" for x in borough_stats['severe_burden_rate']],
        textposition='outside',
    ))
    
    # Update layout
    fig.update_layout(
        barmode='group',
        xaxis_title="Borough",
        yaxis_title="Rate (%)",
        yaxis=dict(
            tickformat='.0%',
            range=[0, max(borough_stats[['rent_burden_rate', 'severe_burden_rate']].max()) * 1.2]
        ),
        height=500,
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        ),
        title="Rent Burden and Severe Burden Rates by Borough",
        hovermode='x unified'
    )
    
    st.plotly_chart(fig, use_container_width=True)
    
    # Download button
    csv = borough_stats.to_csv(index=False)
    st.download_button(
        "📥 Download Borough Statistics as CSV",
        csv,
        "rent_burden_by_borough.csv",
        "text/csv"
    )
    
    st.divider()
    
    # Income-Rent Distribution Visualization
    render_income_rent_distribution()

def main():
    """Main function"""
    render_rent_burden_page()

if __name__ == "__main__":
    main()