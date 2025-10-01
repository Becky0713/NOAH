#!/usr/bin/env python3
"""
Local Development Startup Script
Starts the application with database integration
"""

import asyncio
import logging
import subprocess
import sys
import time
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

def check_dependencies():
    """Check if required dependencies are installed"""
    try:
        import asyncpg
        import psycopg2
        logger.info("✅ Database dependencies found")
    except ImportError as e:
        logger.error(f"❌ Missing database dependencies: {e}")
        logger.error("Please run: pip install asyncpg psycopg2-binary")
        return False
    
    return True

def check_database_connection():
    """Check if database is accessible"""
    try:
        from backend.clients.database_client import DatabaseHousingClient
        from backend.config import settings
        
        async def test_connection():
            client = DatabaseHousingClient()
            try:
                await client._get_pool()
                await client.close()
                return True
            except Exception as e:
                logger.error(f"Database connection failed: {e}")
                return False
        
        return asyncio.run(test_connection())
    except Exception as e:
        logger.error(f"Database check failed: {e}")
        return False

def start_backend():
    """Start the FastAPI backend"""
    logger.info("🚀 Starting FastAPI backend...")
    try:
        subprocess.Popen([
            sys.executable, "-m", "uvicorn", 
            "backend.main:app", 
            "--reload", 
            "--port", "8000",
            "--host", "0.0.0.0"
        ])
        logger.info("✅ Backend started on http://localhost:8000")
        return True
    except Exception as e:
        logger.error(f"❌ Failed to start backend: {e}")
        return False

def start_frontend():
    """Start the Streamlit frontend"""
    logger.info("🚀 Starting Streamlit frontend...")
    try:
        subprocess.Popen([
            sys.executable, "-m", "streamlit", 
            "run", "frontend/app.py",
            "--server.port", "8501",
            "--server.address", "0.0.0.0"
        ])
        logger.info("✅ Frontend started on http://localhost:8501")
        return True
    except Exception as e:
        logger.error(f"❌ Failed to start frontend: {e}")
        return False

def main():
    """Main function"""
    logger.info("🏠 Starting NYC Housing Hub locally...")
    
    # Check if we're in the right directory
    if not (Path.cwd() / "backend").exists():
        logger.error("Please run this script from the project root directory")
        sys.exit(1)
    
    # Check dependencies
    if not check_dependencies():
        sys.exit(1)
    
    # Check database connection
    logger.info("Checking database connection...")
    if not check_database_connection():
        logger.warning("⚠️  Database not accessible. Starting with Socrata API instead.")
        logger.info("To use database:")
        logger.info("1. Start PostgreSQL with PostGIS")
        logger.info("2. Run: python scripts/setup_database.py")
        logger.info("3. Set DATA_PROVIDER=database in .env")
    
    # Start services
    backend_success = start_backend()
    if not backend_success:
        sys.exit(1)
    
    # Wait a moment for backend to start
    time.sleep(2)
    
    frontend_success = start_frontend()
    if not frontend_success:
        sys.exit(1)
    
    logger.info("🎉 NYC Housing Hub started successfully!")
    logger.info("📱 Frontend: http://localhost:8501")
    logger.info("🔧 Backend API: http://localhost:8000")
    logger.info("📚 API Docs: http://localhost:8000/docs")
    logger.info("")
    logger.info("Press Ctrl+C to stop all services")

if __name__ == "__main__":
    try:
        main()
        # Keep the script running
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("👋 Shutting down...")
        sys.exit(0)
