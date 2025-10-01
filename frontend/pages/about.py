"""
About Page
Information about the NYC Housing Hub project
"""

import streamlit as st

def render_about_page():
    """Render the about page"""
    st.title("ℹ️ About NYC Housing Hub")
    
    # Add back to dashboard button
    if st.button("← Back to Dashboard", type="secondary"):
        st.switch_page("app.py")
    
    st.markdown("""
    ## 🏠 NYC Housing Hub - Affordable Housing Data Dashboard
    
    The NYC Housing Hub is a comprehensive data visualization platform that provides insights into 
    New York City's affordable housing development projects. Built with modern web technologies, 
    it offers interactive maps, detailed analytics, and comprehensive data exploration tools.
    
    ### 🎯 Key Features
    
    - **Interactive Map Visualization**: Explore housing projects across NYC with detailed geographic data
    - **Multi-dimensional Filtering**: Filter by borough, unit count, project dates, and more
    - **Comprehensive Data Fields**: Access to all 41+ data fields from NYC Open Data
    - **Real-time Data**: Direct integration with NYC's Socrata Open Data API
    - **Mobile-friendly Design**: Responsive layout that works on all devices
    - **Data Glossary**: Complete field definitions and API documentation
    
    ### 📊 Data Sources
    
    - **Primary Dataset**: [Affordable Housing Production by Building](https://data.cityofnewyork.us/Housing-Development/Affordable-Housing-Production-by-Building/hg8x-zxpr)
    - **Data Provider**: NYC Open Data (Socrata API)
    - **Update Frequency**: Real-time via API
    - **Coverage**: All five NYC boroughs
    
    ### 🛠️ Technology Stack
    
    - **Frontend**: Streamlit, PyDeck, Pandas
    - **Backend**: FastAPI, Python
    - **Database**: PostgreSQL + PostGIS (planned)
    - **Deployment**: Render (Backend), Streamlit Cloud (Frontend)
    - **Data Processing**: Async data pipeline with error handling
    
    ### 📈 Project Statistics
    
    The dashboard currently provides access to:
    - **8,000+** housing projects
    - **41+** data fields per project
    - **5** NYC boroughs covered
    - **Real-time** data updates
    
    ### 🔧 Development
    
    This project is built with:
    - **Modular Architecture**: Separate frontend and backend components
    - **API-First Design**: RESTful API for data access
    - **Error Handling**: Robust error management and user feedback
    - **Performance Optimization**: Caching and efficient data processing
    - **Extensibility**: Easy to add new data sources and features
    
    ### 📚 Documentation
    
    - **API Documentation**: Available at the backend URL + `/docs`
    - **Data Glossary**: Complete field definitions and descriptions
    - **Source Code**: Available on GitHub
    - **Deployment Guide**: Step-by-step setup instructions
    
    ### 🤝 Contributing
    
    This project is open to contributions and improvements. Areas for enhancement include:
    - Additional data sources and datasets
    - Advanced analytics and visualizations
    - Mobile app development
    - Performance optimizations
    - User experience improvements
    
    ### 📞 Contact
    
    For questions, suggestions, or technical support, please refer to the project documentation
    or create an issue in the project repository.
    
    ---
    
    **Built with ❤️ for NYC's affordable housing community**
    """)

def main():
    """Main function"""
    render_about_page()

if __name__ == "__main__":
    main()
