"""
Glossary Page
Displays a searchable and sortable table of field definitions
"""

import streamlit as st
import pandas as pd
import json
from pathlib import Path
from typing import List, Dict, Any

# Set page config for this page
st.set_page_config(
    page_title="NYC Housing Hub - Glossary",
    page_icon="📚",
    layout="wide"
)

def load_glossary_data() -> List[Dict[str, Any]]:
    """Load glossary data from JSON file"""
    try:
        glossary_path = Path(__file__).parent.parent / "data" / "glossary.json"
        with open(glossary_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        st.error("Glossary data file not found. Please ensure glossary.json exists.")
        return []
    except json.JSONDecodeError:
        st.error("Invalid JSON format in glossary data file.")
        return []

def render_glossary_page():
    """Render the main glossary page"""
    
    # Page header
    st.title("📚 Data Field Glossary")
    st.markdown("""
    This glossary provides definitions for all data fields used in the NYC Housing Hub dashboard. 
    Use the search bar below to quickly find specific fields or browse through all available terms.
    """)
    
    # Add back to dashboard button
    if st.button("← Back to Dashboard", type="secondary"):
        st.switch_page("app.py")
    
    # Load glossary data
    glossary_data = load_glossary_data()
    if not glossary_data:
        st.stop()
    
    # Convert to DataFrame for easier manipulation
    df = pd.DataFrame(glossary_data)
    
    # Search and filter section
    st.subheader("🔍 Search & Filter")
    
    col1, col2, col3 = st.columns([2, 1, 1])
    
    with col1:
        search_term = st.text_input(
            "Search fields...",
            placeholder="Type field name, API field, or description...",
            help="Search across field names, API fields, and descriptions"
        )
    
    with col2:
        sort_by = st.selectbox(
            "Sort by",
            options=["Field Name", "API Field", "Description"],
            index=0
        )
    
    with col3:
        sort_order = st.selectbox(
            "Order",
            options=["Ascending", "Descending"],
            index=0
        )
    
    # Filter data based on search term
    if search_term:
        search_lower = search_term.lower()
        mask = (
            df['field_name'].str.lower().str.contains(search_lower, na=False) |
            df['api_field'].str.lower().str.contains(search_lower, na=False) |
            df['description'].str.lower().str.contains(search_lower, na=False)
        )
        filtered_df = df[mask]
    else:
        filtered_df = df.copy()
    
    # Sort data
    sort_column = {
        "Field Name": "field_name",
        "API Field": "api_field", 
        "Description": "description"
    }[sort_by]
    
    ascending = sort_order == "Ascending"
    filtered_df = filtered_df.sort_values(by=sort_column, ascending=ascending)
    
    # Display results
    st.subheader(f"📋 Results ({len(filtered_df)} fields)")
    
    if len(filtered_df) == 0:
        st.info("No fields found matching your search criteria.")
        return
    
    # Create expandable table for better mobile experience
    if st.checkbox("📱 Mobile-friendly view (accordion style)", value=False):
        render_accordion_view(filtered_df)
    else:
        render_table_view(filtered_df)

def render_table_view(df: pd.DataFrame):
    """Render glossary as a standard table"""
    # Style the DataFrame for better presentation
    styled_df = df.copy()
    styled_df.columns = ["Field Name", "API Field", "Description"]
    
    # Display the table with custom styling
    st.dataframe(
        styled_df,
        use_container_width=True,
        hide_index=True,
        height=600,
        column_config={
            "Field Name": st.column_config.TextColumn(
                "Field Name",
                help="Human-readable field name",
                width="medium"
            ),
            "API Field": st.column_config.TextColumn(
                "API Field",
                help="Programmatic field name used in API",
                width="small"
            ),
            "Description": st.column_config.TextColumn(
                "Description",
                help="Detailed description of the field",
                width="large"
            )
        }
    )
    
    # Add download option
    csv = df.to_csv(index=False)
    st.download_button(
        label="📥 Download as CSV",
        data=csv,
        file_name="nyc_housing_glossary.csv",
        mime="text/csv"
    )

def render_accordion_view(df: pd.DataFrame):
    """Render glossary as expandable accordion items"""
    for idx, row in df.iterrows():
        with st.expander(f"**{row['field_name']}** (`{row['api_field']}`)", expanded=False):
            col1, col2 = st.columns([1, 2])
            
            with col1:
                st.markdown("**API Field:**")
                st.code(row['api_field'], language="text")
            
            with col2:
                st.markdown("**Description:**")
                st.markdown(row['description'])
            
            st.divider()

def main():
    """Main function"""
    render_glossary_page()

if __name__ == "__main__":
    main()
