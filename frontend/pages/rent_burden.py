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

def extract_borough_from_geo_id(geo_id):
    """Extract borough name from GEOID"""
    # GEOID format: 0600000US3600508510
    # County codes:
    # 005 = Bronx County
    # 047 = Kings County (Brooklyn)
    # 061 = New York County (Manhattan)
    # 081 = Queens County
    # 085 = Richmond County (Staten Island)
    
    if not geo_id or len(str(geo_id)) < 15:
        return None
    
    geo_id_str = str(geo_id)
    # Extract county code (positions 9-11)
    county_code = geo_id_str[9:12] if len(geo_id_str) > 11 else None
    
    borough_map = {
        "005": "Bronx",
        "047": "Brooklyn",
        "061": "Manhattan",
        "081": "Queens",
        "085": "Staten Island"
    }
    
    return borough_map.get(county_code)

def aggregate_by_borough(df):
    """Aggregate rent burden data by borough"""
    # Extract borough from geo_id
    def get_borough(geo_id):
        if not geo_id or len(str(geo_id)) < 15:
            return None
        geo_id_str = str(geo_id)
        county_code = geo_id_str[9:12] if len(geo_id_str) > 11 else None
        
        borough_map = {
            "005": "Bronx",
            "047": "Brooklyn",
            "061": "Manhattan",
            "081": "Queens",
            "085": "Staten Island"
        }
        return borough_map.get(county_code)
    
    # Add borough column
    df['borough'] = df['geo_id'].apply(get_borough)
    
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
        st.warning("⚠️ Could not extract borough information from geo_id.")
        st.info("Please ensure geo_id follows format: 0600000US3600508510")
        st.dataframe(df.head(10))
        st.stop()
    
    # Display summary statistics
    st.subheader("📊 Summary Statistics by Borough")
    
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
        marker_color='#ef4444',  # Red
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
    
    # Data table
    st.subheader("📋 Detailed Statistics")
    
    # Format for display
    display_df = borough_stats.copy()
    display_df['Rent Burden Rate'] = display_df['rent_burden_rate'].apply(lambda x: f"{x:.2%}")
    display_df['Severe Burden Rate'] = display_df['severe_burden_rate'].apply(lambda x: f"{x:.2%}")
    display_df = display_df[['borough', 'Rent Burden Rate', 'Severe Burden Rate']]
    display_df.columns = ['Borough', 'Rent Burden Rate', 'Severe Burden Rate']
    
    st.dataframe(display_df, use_container_width=True, hide_index=True)
    
    # Download button
    csv = borough_stats.to_csv(index=False)
    st.download_button(
        "📥 Download Borough Statistics as CSV",
        csv,
        "rent_burden_by_borough.csv",
        "text/csv"
    )

def main():
    """Main function"""
    render_rent_burden_page()

if __name__ == "__main__":
    main()