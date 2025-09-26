# NYC Housing Hub

A comprehensive dashboard for exploring New York City affordable housing data, built with FastAPI and Streamlit.

## Features

### Backend API (FastAPI)
- **Metadata Endpoint**: `/metadata/fields` - Returns all 41 available fields with descriptions
- **Records Endpoint**: `/v1/records` - Supports multi-dimensional filtering and pagination
- **Socrata Integration**: Full integration with NYC Open Data (Socrata) dataset hg8x-zxpr
- **Error Handling**: Comprehensive error handling and rate limiting protection
- **Database Support**: SQLite integration for local data storage and ingestion

### Frontend Dashboard (Streamlit)
- **Multi-dimensional Filtering**: Borough, unit count range, project start date range
- **Field Selection**: Multi-select from 41 available fields for additional columns
- **Interactive Map**: Point size and color based on unit count with hover details
- **Data Visualization**: Unit count distribution charts and data tables
- **Real-time Queries**: All filters apply in real-time with pagination support

### Data Sources
- **NYC Open Data (Socrata)**: Dataset hg8x-zxpr (Affordable Housing Production by Building)
- **Application Token Support**: For higher rate limits and production use
- **Full Data Ingestion**: Complete 41-column data ingestion capability

## Quick Start

### Prerequisites
- Python 3.9+
- Virtual environment (recommended)

### Installation

1. **Clone the repository**
```bash
git clone <your-repo-url>
cd nyc-housing-hub
```

2. **Create and activate virtual environment**
```bash
python3 -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
```

3. **Install dependencies**
```bash
python -m pip install --upgrade pip
pip install -r requirements.txt
```

4. **Configure environment variables**
Create a `.env` file in the project root:
```env
DATA_PROVIDER=socrata
SOCRATA_BASE_URL=https://data.cityofnewyork.us
SOCRATA_DATASET_ID=hg8x-zxpr
SOCRATA_APP_TOKEN=your_app_token_here
```

### Running the Application

1. **Start the backend**
```bash
source .venv/bin/activate
uvicorn backend.main:app --reload --port 8000
```

2. **Start the frontend**
```bash
source .venv/bin/activate
streamlit run frontend/app.py --server.port=8501
```

3. **Access the application**
- **Frontend Dashboard**: http://127.0.0.1:8501
- **Backend API**: http://127.0.0.1:8000
- **API Documentation**: http://127.0.0.1:8000/docs

### Data Ingestion

To ingest the complete dataset into local SQLite database:

```bash
source .venv/bin/activate
python -m backend.ingest_socrata
```

This will:
- Fetch metadata from Socrata to determine all 41 columns
- Download all records in batches
- Store data in SQLite at `./data/nyc_housing.db`
- Create table `affordable_housing_buildings`

## API Endpoints

### Core Endpoints

- `GET /health` - Health check
- `GET /v1/regions` - List available regions (boroughs)
- `GET /metadata/fields` - List all available fields with descriptions
- `GET /v1/records` - Query housing records with filtering

### Records Endpoint Parameters

The `/v1/records` endpoint supports comprehensive filtering:

```
GET /v1/records?fields=house_number,street_name,borough&limit=100&offset=0&borough=Manhattan&min_units=10&max_units=200&start_date_from=2020-01-01&start_date_to=2023-12-31
```

**Parameters:**
- `fields`: Comma-separated field names (default: core fields)
- `limit`: Maximum records to return (1-1000, default: 100)
- `offset`: Records to skip for pagination (default: 0)
- `borough`: Filter by borough (empty = no filter)
- `min_units`: Minimum unit count (default: 0)
- `max_units`: Maximum unit count (0 = no limit, default: 0)
- `start_date_from`: Project start date from (YYYY-MM-DD)
- `start_date_to`: Project start date to (YYYY-MM-DD)

**Response Format:**
```json
[
  {
    "address": "123 Main St",
    "latitude": 40.7128,
    "longitude": -74.0060,
    "region": "Manhattan",
    "total_units": 50,
    "affordable_units": 25,
    "project_start_date": "2020-01-01",
    "project_completion_date": "2022-06-01",
    "_raw": { /* original selected columns */ }
  }
]
```

## Frontend Features

### Dashboard Components

1. **Metrics Cards**: Display key statistics (listings count, median rent, average rent, vacancy rate)

2. **Interactive Filters**:
   - Region selection
   - Borough filtering
   - Unit count range (min/max)
   - Project start date range
   - Additional field selection from 41 available fields

3. **Map Visualization**:
   - Scatter plot with size/color based on unit count
   - Hover tooltips with detailed information
   - Zoom and pan capabilities

4. **Data Tables**:
   - Sortable columns
   - Pagination support
   - Export capabilities

### Usage

1. **Filter Data**: Use sidebar filters to narrow down results
2. **Select Fields**: Choose additional fields from the metadata
3. **Explore Map**: Click on points to see detailed information
4. **View Distribution**: Check unit count distribution charts
5. **Export Data**: Use the data table to view and export filtered results

## Configuration

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `DATA_PROVIDER` | Data source provider (example/socrata/census) | `socrata` |
| `SOCRATA_BASE_URL` | Socrata API base URL | `https://data.cityofnewyork.us` |
| `SOCRATA_APP_TOKEN` | Socrata application token | `None` |
| `SOCRATA_DATASET_ID` | Target dataset ID | `hg8x-zxpr` |
| `DB_PATH` | SQLite database path | `./data/nyc_housing.db` |

### Field Mappings

The system uses configurable field mappings for data normalization:

- `socrata_field_id`: Primary key field
- `socrata_field_address`: Address field (combined from house_number + street_name)
- `socrata_field_latitude`: Latitude coordinate
- `socrata_field_longitude`: Longitude coordinate
- `socrata_field_region`: Region/borough field
- `socrata_field_total_units`: Total unit count
- `socrata_field_affordable_units`: Affordable unit count
- `socrata_field_project_start_date`: Project start date
- `socrata_field_project_completion_date`: Project completion date

## Deployment

### Local Development
Follow the Quick Start guide above for local development setup.

### Production Deployment

1. **Environment Setup**:
   ```bash
   # Set production environment variables
   export DATA_PROVIDER=socrata
   export SOCRATA_APP_TOKEN=your_production_token
   export CORS_ALLOW_ORIGINS=https://yourdomain.com
   ```

2. **Backend Deployment**:
   ```bash
   # Using Gunicorn (recommended for production)
   gunicorn backend.main:app -w 4 -k uvicorn.workers.UvicornWorker --bind 0.0.0.0:8000
   ```

3. **Frontend Deployment**:
   - Deploy to Streamlit Community Cloud
   - Or use Docker with custom deployment
   - Configure secrets.toml for production backend URL

### Docker Deployment

```dockerfile
# Dockerfile example
FROM python:3.9-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .
EXPOSE 8000 8501

CMD ["sh", "-c", "uvicorn backend.main:app --host 0.0.0.0 --port 8000 & streamlit run frontend/app.py --server.port=8501 --server.address=0.0.0.0"]
```

## Data Schema

### Core Fields (Always Returned)
- `address`: Combined address (house_number + street_name)
- `latitude`: Latitude coordinate
- `longitude`: Longitude coordinate
- `region`: Borough name
- `total_units`: Total number of units
- `affordable_units`: Number of affordable units
- `project_start_date`: Project start date
- `project_completion_date`: Project completion date

### Additional Fields (Selectable)
All 41 fields from the NYC Open Data dataset are available for selection, including:
- Project details (ID, name, type)
- Building information (BIN, BBL, completion date)
- Unit breakdown by income level and bedroom count
- Geographic details (community board, council district, census tract)
- Construction and affordability details

## Troubleshooting

### Common Issues

1. **Port Already in Use**:
   ```bash
   # Find and kill processes using ports 8000/8501
   lsof -i :8000
   lsof -i :8501
   kill -9 <PID>
   ```

2. **Socrata Rate Limiting**:
   - Add `SOCRATA_APP_TOKEN` to `.env`
   - Reduce query frequency
   - Use local SQLite database for development

3. **Missing Dependencies**:
   ```bash
   pip install --upgrade -r requirements.txt
   ```

4. **Database Issues**:
   ```bash
   # Recreate database
   rm -rf data/
   python -m backend.ingest_socrata
   ```

### Error Codes

- `400`: Bad request (unsupported provider)
- `404`: Resource not found
- `500`: Internal server error
- `502`: Upstream provider error (check SOCRATA_APP_TOKEN)

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Acknowledgments

- NYC Open Data for providing the affordable housing dataset
- Socrata for the data platform and API
- FastAPI and Streamlit communities for excellent frameworks

## Support

For issues and questions:
1. Check the troubleshooting section above
2. Review the API documentation at `/docs`
3. Open an issue on GitHub
4. Check the logs for detailed error messages

---

**NYC Housing Hub** - Making affordable housing data accessible and explorable.