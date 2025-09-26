# Deployment Guide

## GitHub Repository Setup

### 1. Create GitHub Repository

1. Go to GitHub and create a new repository named `nyc-housing-hub`
2. **Do not** initialize with README, .gitignore, or license (we already have these)

### 2. Initialize and Push to GitHub

```bash
# Initialize git repository
git init

# Add all files
git add .

# Create initial commit
git commit -m "Initial commit: NYC Housing Hub with English localization"

# Add remote origin (replace with your GitHub username)
git remote add origin https://github.com/YOUR_USERNAME/nyc-housing-hub.git

# Push to GitHub
git push -u origin main
```

### 3. Repository Structure

Your repository should have this structure:
```
nyc-housing-hub/
├── backend/
│   ├── clients/
│   │   ├── base.py
│   │   ├── census_client.py
│   │   ├── example_client.py
│   │   └── socrata_client.py
│   ├── api_router.py
│   ├── config.py
│   ├── db.py
│   ├── ingest_socrata.py
│   ├── main.py
│   └── models.py
├── frontend/
│   └── app.py
├── .streamlit/
│   └── config.toml
├── .gitignore
├── docker-compose.yml
├── Dockerfile
├── README.md
├── DEPLOYMENT.md
└── requirements.txt
```

## Deployment Options

### Option 1: Streamlit Community Cloud (Recommended)

1. **Connect Repository**:
   - Go to [share.streamlit.io](https://share.streamlit.io)
   - Sign in with GitHub
   - Click "New app"
   - Select your repository: `YOUR_USERNAME/nyc-housing-hub`
   - Branch: `main`
   - Main file path: `frontend/app.py`

2. **Configure Secrets**:
   - In Streamlit Cloud, go to Settings → Secrets
   - Add:
   ```toml
   backend_url = "https://your-backend-url.herokuapp.com"
   ```

3. **Deploy Backend Separately**:
   - Deploy backend to Heroku, Railway, or similar
   - Update the backend_url in Streamlit secrets

### Option 2: Docker Deployment

1. **Local Docker**:
   ```bash
   # Build and run
   docker-compose up --build
   
   # Access:
   # Frontend: http://localhost:8501
   # Backend: http://localhost:8000
   ```

2. **Production Docker**:
   ```bash
   # Build image
   docker build -t nyc-housing-hub .
   
   # Run container
   docker run -p 8000:8000 -p 8501:8501 \
     -e SOCRATA_APP_TOKEN=your_token \
     nyc-housing-hub
   ```

### Option 3: Manual Server Deployment

1. **Server Setup**:
   ```bash
   # Clone repository
   git clone https://github.com/YOUR_USERNAME/nyc-housing-hub.git
   cd nyc-housing-hub
   
   # Setup virtual environment
   python3 -m venv .venv
   source .venv/bin/activate
   
   # Install dependencies
   pip install -r requirements.txt
   ```

2. **Environment Configuration**:
   ```bash
   # Create .env file
   cat > .env << EOF
   DATA_PROVIDER=socrata
   SOCRATA_APP_TOKEN=your_token_here
   CORS_ALLOW_ORIGINS=https://yourdomain.com
   EOF
   ```

3. **Start Services**:
   ```bash
   # Backend (using systemd or PM2)
   uvicorn backend.main:app --host 0.0.0.0 --port 8000
   
   # Frontend
   streamlit run frontend/app.py --server.port=8501 --server.headless=true
   ```

## Environment Variables

### Required for Production

```env
# Data source configuration
DATA_PROVIDER=socrata
SOCRATA_APP_TOKEN=your_production_token

# CORS settings
CORS_ALLOW_ORIGINS=https://yourdomain.com,https://your-frontend-domain.com

# Database (optional for production)
DB_PATH=/app/data/nyc_housing.db
```

### Optional Configuration

```env
# HTTP settings
HTTP_TIMEOUT_SECONDS=30

# Application settings
ENVIRONMENT=production
APP_NAME=NYC Housing Hub

# Field mappings (if different from defaults)
SOCRATA_FIELD_REGION=borough
SOCRATA_FIELD_TOTAL_UNITS=total_units
```

## Production Considerations

### 1. Security
- Use environment variables for sensitive data
- Enable HTTPS in production
- Set proper CORS origins
- Use a reverse proxy (nginx) for SSL termination

### 2. Performance
- Use a production ASGI server (Gunicorn + Uvicorn workers)
- Enable database connection pooling
- Implement caching for frequently accessed data
- Use CDN for static assets

### 3. Monitoring
- Set up logging
- Monitor API response times
- Track error rates
- Set up alerts for service downtime

### 4. Data Management
- Schedule regular data ingestion
- Implement data validation
- Set up database backups
- Monitor storage usage

## Troubleshooting Deployment

### Common Issues

1. **Import Errors**:
   ```bash
   # Ensure all dependencies are installed
   pip install -r requirements.txt
   ```

2. **Port Conflicts**:
   ```bash
   # Check port usage
   lsof -i :8000
   lsof -i :8501
   ```

3. **Environment Variables**:
   ```bash
   # Verify environment variables
   env | grep SOCRATA
   ```

4. **Database Issues**:
   ```bash
   # Recreate database
   rm -rf data/
   python -m backend.ingest_socrata
   ```

### Logs and Debugging

1. **Backend Logs**:
   ```bash
   # Check uvicorn logs
   tail -f logs/backend.log
   ```

2. **Frontend Logs**:
   ```bash
   # Check Streamlit logs
   tail -f logs/streamlit.log
   ```

3. **Application Logs**:
   ```bash
   # Check application logs
   journalctl -u nyc-housing-hub -f
   ```

## Scaling Considerations

### Horizontal Scaling
- Use load balancer for multiple backend instances
- Implement database clustering
- Use Redis for session storage
- Consider microservices architecture

### Vertical Scaling
- Increase server resources
- Optimize database queries
- Implement caching layers
- Use CDN for static content

## Maintenance

### Regular Tasks
- Update dependencies monthly
- Monitor data freshness
- Review and rotate API tokens
- Backup database regularly
- Update documentation

### Monitoring
- Set up health checks
- Monitor API usage
- Track user engagement
- Monitor system resources

---

For additional support, refer to the main README.md or open an issue on GitHub.
