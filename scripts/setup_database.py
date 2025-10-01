#!/usr/bin/env python3
"""
Database Setup Script
Initializes PostgreSQL database and runs data pipeline
"""

import asyncio
import logging
import sys
from pathlib import Path

# Add backend to Python path
backend_path = Path(__file__).parent.parent / "backend"
sys.path.insert(0, str(backend_path))

from data_pipeline import DataPipeline
from config import settings

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

async def setup_database():
    """Complete database setup process"""
    logger.info("Starting database setup...")
    
    # Step 1: Initialize database
    logger.info("Step 1: Initializing database...")
    from scripts.init_database import main as init_db
    await init_db()
    
    # Step 2: Run data pipeline
    logger.info("Step 2: Running data pipeline...")
    pipeline = DataPipeline()
    
    try:
        await pipeline.initialize()
        await pipeline.run_full_sync(batch_size=500)  # Smaller batches for stability
        logger.info("Data pipeline completed successfully!")
    except Exception as e:
        logger.error(f"Data pipeline failed: {e}")
        raise
    finally:
        await pipeline.close()
    
    # Step 3: Verify data
    logger.info("Step 3: Verifying data...")
    from scripts.db_client import DatabaseClient
    
    client = DatabaseClient()
    try:
        await client.connect()
        stats = await client.get_database_stats()
        
        logger.info("Database setup completed successfully!")
        logger.info(f"Total records: {stats['total_records']}")
        logger.info(f"Records with coordinates: {stats['with_coordinates']}")
        logger.info(f"Total units: {stats['unit_stats'].get('total_units', 0)}")
        logger.info(f"Total affordable units: {stats['unit_stats'].get('total_affordable_units', 0)}")
        
        # Show borough breakdown
        logger.info("Records by borough:")
        for borough in stats['by_borough'][:5]:  # Top 5
            logger.info(f"  {borough['borough']}: {borough['count']} projects, {borough['total_units']} units")
            
    except Exception as e:
        logger.error(f"Database verification failed: {e}")
        raise
    finally:
        await client.close()

async def main():
    """Main function"""
    try:
        await setup_database()
        logger.info("🎉 Database setup completed successfully!")
    except Exception as e:
        logger.error(f"❌ Database setup failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    # Check if we're in the right directory
    if not (Path.cwd() / "backend").exists():
        logger.error("Please run this script from the project root directory")
        sys.exit(1)
    
    # Run setup
    asyncio.run(main())
