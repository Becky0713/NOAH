#!/usr/bin/env python3
"""
Database Status Check Script
Checks database connection and data status
"""

import asyncio
import logging
import sys
from pathlib import Path

# Add backend to Python path
backend_path = Path(__file__).parent.parent / "backend"
sys.path.insert(0, str(backend_path))

from clients.database_client import DatabaseHousingClient
from config import settings

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

async def check_database():
    """Check database status and data"""
    logger.info("Checking database status...")
    
    client = DatabaseHousingClient()
    
    try:
        # Test connection
        logger.info("Testing database connection...")
        await client._get_pool()
        logger.info("✅ Database connection successful")
        
        # Get statistics
        logger.info("Fetching database statistics...")
        stats = await client.get_database_stats()
        
        logger.info("📊 Database Statistics:")
        logger.info(f"  Total records: {stats['total_records']}")
        logger.info(f"  With coordinates: {stats['with_coordinates']}")
        logger.info(f"  Total units: {stats['unit_stats'].get('total_units', 0)}")
        logger.info(f"  Total affordable units: {stats['unit_stats'].get('total_affordable_units', 0)}")
        
        # Show borough breakdown
        logger.info("📍 Records by borough:")
        for borough in stats['by_borough']:
            logger.info(f"  {borough['borough']}: {borough['count']} projects, {borough['total_units']} units")
        
        # Test API endpoints
        logger.info("Testing API endpoints...")
        
        # Test regions
        regions = await client.fetch_region_summary("Manhattan")
        logger.info(f"✅ Region summary test: {regions['listing_count']} listings in Manhattan")
        
        # Test listings
        listings = await client.fetch_listings("Manhattan", limit=5)
        logger.info(f"✅ Listings test: {len(listings)} listings retrieved")
        
        # Test metadata
        metadata = await client.fetch_metadata_fields()
        logger.info(f"✅ Metadata test: {len(metadata)} fields available")
        
        # Test records
        records = await client.fetch_records(
            ["project_name", "total_units", "borough"], 
            limit=5
        )
        logger.info(f"✅ Records test: {len(records)} records retrieved")
        
        logger.info("🎉 All database checks passed!")
        
    except Exception as e:
        logger.error(f"❌ Database check failed: {e}")
        raise
    finally:
        await client.close()

async def main():
    """Main function"""
    try:
        await check_database()
    except Exception as e:
        logger.error(f"Database check failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    # Check if we're in the right directory
    if not (Path.cwd() / "backend").exists():
        logger.error("Please run this script from the project root directory")
        sys.exit(1)
    
    # Run check
    asyncio.run(main())
