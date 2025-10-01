#!/usr/bin/env python3
"""
NYC Housing Data Pipeline Runner
Runs the data pipeline to sync Socrata data to PostgreSQL database
"""

import asyncio
import logging
import sys
import os
from pathlib import Path

# Add backend to Python path
backend_path = Path(__file__).parent.parent / "backend"
sys.path.insert(0, str(backend_path))

from data_pipeline import DataPipeline
from config import settings

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('data_pipeline.log')
    ]
)

logger = logging.getLogger(__name__)

async def main():
    """Main function to run the data pipeline"""
    logger.info("Starting NYC Housing Data Pipeline...")
    logger.info(f"Database: {settings.db_host}:{settings.db_port}/{settings.db_name}")
    logger.info(f"Socrata Dataset: {settings.socrata_dataset_id}")
    
    pipeline = DataPipeline()
    
    try:
        # Initialize connections
        logger.info("Initializing database and HTTP connections...")
        await pipeline.initialize()
        
        # Run full sync
        logger.info("Starting full data synchronization...")
        await pipeline.run_full_sync(batch_size=1000)
        
        logger.info("Data pipeline completed successfully!")
        
    except Exception as e:
        logger.error(f"Data pipeline failed: {e}")
        raise
    finally:
        # Clean up connections
        logger.info("Closing connections...")
        await pipeline.close()

if __name__ == "__main__":
    # Check if we're in the right directory
    if not (Path.cwd() / "backend").exists():
        logger.error("Please run this script from the project root directory")
        sys.exit(1)
    
    # Run the pipeline
    asyncio.run(main())
