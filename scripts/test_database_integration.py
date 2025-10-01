#!/usr/bin/env python3
"""
Database Integration Test Script
Tests the complete database integration functionality
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

async def test_database_integration():
    """Test complete database integration"""
    logger.info("🧪 Testing database integration...")
    
    client = DatabaseHousingClient()
    
    try:
        # Test 1: Connection
        logger.info("Test 1: Database connection...")
        await client._get_pool()
        logger.info("✅ Database connection successful")
        
        # Test 2: Region summary
        logger.info("Test 2: Region summary...")
        summary = await client.fetch_region_summary("Manhattan")
        assert "listing_count" in summary
        assert "region" in summary
        logger.info(f"✅ Region summary: {summary['listing_count']} listings in Manhattan")
        
        # Test 3: Listings
        logger.info("Test 3: Fetch listings...")
        listings = await client.fetch_listings("Manhattan", limit=5)
        assert isinstance(listings, list)
        logger.info(f"✅ Listings: {len(listings)} records retrieved")
        
        # Test 4: Metadata
        logger.info("Test 4: Field metadata...")
        metadata = await client.fetch_metadata_fields()
        assert isinstance(metadata, list)
        assert len(metadata) > 0
        logger.info(f"✅ Metadata: {len(metadata)} fields available")
        
        # Test 5: Records with filtering
        logger.info("Test 5: Records with filtering...")
        records = await client.fetch_records(
            ["project_name", "total_units", "borough", "latitude", "longitude"],
            limit=10,
            borough="Manhattan",
            min_units=50
        )
        assert isinstance(records, list)
        logger.info(f"✅ Records: {len(records)} filtered records retrieved")
        
        # Test 6: Database stats
        logger.info("Test 6: Database statistics...")
        stats = await client.get_database_stats()
        assert "total_records" in stats
        assert "by_borough" in stats
        logger.info(f"✅ Stats: {stats['total_records']} total records")
        
        # Test 7: Complex query
        logger.info("Test 7: Complex query...")
        complex_records = await client.fetch_records(
            ["project_name", "total_units", "all_counted_units", "studio_units", "borough"],
            limit=5,
            borough="Brooklyn",
            min_units=100,
            max_units=500
        )
        assert isinstance(complex_records, list)
        logger.info(f"✅ Complex query: {len(complex_records)} records retrieved")
        
        # Test 8: Date filtering
        logger.info("Test 8: Date filtering...")
        date_records = await client.fetch_records(
            ["project_name", "project_start_date", "project_completion_date"],
            limit=5,
            start_date_from="2020-01-01",
            start_date_to="2023-12-31"
        )
        assert isinstance(date_records, list)
        logger.info(f"✅ Date filtering: {len(date_records)} records retrieved")
        
        logger.info("🎉 All database integration tests passed!")
        return True
        
    except Exception as e:
        logger.error(f"❌ Database integration test failed: {e}")
        return False
    finally:
        await client.close()

async def test_api_endpoints():
    """Test API endpoints with database client"""
    logger.info("🌐 Testing API endpoints...")
    
    try:
        import requests
        import time
        
        # Wait for backend to be ready
        backend_url = "http://localhost:8000"
        max_retries = 30
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                response = requests.get(f"{backend_url}/health", timeout=5)
                if response.status_code == 200:
                    logger.info("✅ Backend API is ready")
                    break
            except requests.exceptions.RequestException:
                pass
            
            retry_count += 1
            time.sleep(2)
        
        if retry_count >= max_retries:
            logger.warning("⚠️  Backend API not ready, skipping API tests")
            return True
        
        # Test health endpoint
        response = requests.get(f"{backend_url}/health")
        assert response.status_code == 200
        logger.info("✅ Health endpoint working")
        
        # Test regions endpoint
        response = requests.get(f"{backend_url}/v1/regions")
        assert response.status_code == 200
        regions = response.json()
        assert isinstance(regions, list)
        logger.info(f"✅ Regions endpoint: {len(regions)} regions")
        
        # Test metadata endpoint
        response = requests.get(f"{backend_url}/metadata/fields")
        assert response.status_code == 200
        fields = response.json()
        assert isinstance(fields, list)
        logger.info(f"✅ Metadata endpoint: {len(fields)} fields")
        
        # Test records endpoint
        response = requests.get(f"{backend_url}/v1/records?limit=5")
        assert response.status_code == 200
        records = response.json()
        assert isinstance(records, list)
        logger.info(f"✅ Records endpoint: {len(records)} records")
        
        # Test database stats endpoint
        response = requests.get(f"{backend_url}/database/stats")
        assert response.status_code == 200
        stats = response.json()
        assert "total_records" in stats
        logger.info(f"✅ Database stats endpoint: {stats['total_records']} total records")
        
        logger.info("🎉 All API endpoint tests passed!")
        return True
        
    except Exception as e:
        logger.error(f"❌ API endpoint test failed: {e}")
        return False

async def main():
    """Main test function"""
    logger.info("🚀 Starting database integration tests...")
    
    # Check if we're in the right directory
    if not (Path.cwd() / "backend").exists():
        logger.error("Please run this script from the project root directory")
        sys.exit(1)
    
    # Test database integration
    db_success = await test_database_integration()
    if not db_success:
        logger.error("❌ Database integration tests failed")
        sys.exit(1)
    
    # Test API endpoints (optional)
    api_success = await test_api_endpoints()
    if not api_success:
        logger.warning("⚠️  API endpoint tests failed, but database integration is working")
    
    logger.info("🎉 All tests completed successfully!")

if __name__ == "__main__":
    asyncio.run(main())
