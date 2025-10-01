#!/usr/bin/env python3
"""
Database Client Script
Provides utilities for database management and querying
"""

import asyncio
import logging
import sys
from pathlib import Path
from typing import List, Dict, Any

# Add backend to Python path
backend_path = Path(__file__).parent.parent / "backend"
sys.path.insert(0, str(backend_path))

import asyncpg
from config import settings

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

class DatabaseClient:
    def __init__(self):
        self.conn = None
    
    async def connect(self):
        """Connect to database"""
        self.conn = await asyncpg.connect(
            host=settings.db_host,
            port=settings.db_port,
            user=settings.db_user,
            password=settings.db_password,
            database=settings.db_name
        )
    
    async def close(self):
        """Close database connection"""
        if self.conn:
            await self.conn.close()
    
    async def get_stats(self) -> Dict[str, Any]:
        """Get database statistics"""
        if not self.conn:
            await self.connect()
        
        stats = {}
        
        # Total records
        stats['total_records'] = await self.conn.fetchval(
            "SELECT COUNT(*) FROM housing_projects"
        )
        
        # Records by borough
        borough_stats = await self.conn.fetch("""
            SELECT borough, COUNT(*) as count, SUM(total_units) as total_units
            FROM housing_projects 
            WHERE borough IS NOT NULL
            GROUP BY borough 
            ORDER BY count DESC
        """)
        stats['by_borough'] = [dict(row) for row in borough_stats]
        
        # Records with coordinates
        stats['with_coordinates'] = await self.conn.fetchval(
            "SELECT COUNT(*) FROM housing_projects WHERE latitude IS NOT NULL AND longitude IS NOT NULL"
        )
        
        # Date range
        date_range = await self.conn.fetchrow("""
            SELECT 
                MIN(project_start_date) as earliest_start,
                MAX(project_start_date) as latest_start,
                MIN(project_completion_date) as earliest_completion,
                MAX(project_completion_date) as latest_completion
            FROM housing_projects
        """)
        stats['date_range'] = dict(date_range) if date_range else {}
        
        # Unit statistics
        unit_stats = await self.conn.fetchrow("""
            SELECT 
                SUM(total_units) as total_units,
                SUM(all_counted_units) as total_affordable_units,
                AVG(total_units) as avg_units_per_project,
                MAX(total_units) as max_units_per_project
            FROM housing_projects
        """)
        stats['unit_stats'] = dict(unit_stats) if unit_stats else {}
        
        return stats
    
    async def search_projects(
        self, 
        borough: str = None,
        min_units: int = None,
        max_units: int = None,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """Search for housing projects"""
        if not self.conn:
            await self.connect()
        
        where_conditions = []
        params = []
        param_count = 0
        
        if borough:
            param_count += 1
            where_conditions.append(f"borough = ${param_count}")
            params.append(borough)
        
        if min_units is not None:
            param_count += 1
            where_conditions.append(f"total_units >= ${param_count}")
            params.append(min_units)
        
        if max_units is not None:
            param_count += 1
            where_conditions.append(f"total_units <= ${param_count}")
            params.append(max_units)
        
        where_clause = " AND ".join(where_conditions) if where_conditions else "1=1"
        
        query = f"""
            SELECT 
                project_id,
                project_name,
                CONCAT(house_number, ' ', street_name) as address,
                borough,
                latitude,
                longitude,
                total_units,
                all_counted_units as affordable_units,
                project_start_date,
                project_completion_date
            FROM housing_projects
            WHERE {where_clause}
            ORDER BY total_units DESC
            LIMIT ${param_count + 1}
        """
        params.append(limit)
        
        rows = await self.conn.fetch(query, *params)
        return [dict(row) for row in rows]

async def main():
    """Main function for database client"""
    client = DatabaseClient()
    
    try:
        await client.connect()
        logger.info("Connected to database")
        
        # Get statistics
        stats = await client.get_stats()
        logger.info(f"Database Statistics:")
        logger.info(f"  Total records: {stats['total_records']}")
        logger.info(f"  With coordinates: {stats['with_coordinates']}")
        logger.info(f"  Total units: {stats['unit_stats'].get('total_units', 0)}")
        logger.info(f"  Total affordable units: {stats['unit_stats'].get('total_affordable_units', 0)}")
        
        # Show borough breakdown
        logger.info("Records by borough:")
        for borough in stats['by_borough'][:5]:  # Top 5
            logger.info(f"  {borough['borough']}: {borough['count']} projects, {borough['total_units']} units")
        
        # Search example
        logger.info("\nSearching for projects with 100+ units...")
        projects = await client.search_projects(min_units=100, limit=5)
        for project in projects:
            logger.info(f"  {project['project_name']} - {project['total_units']} units in {project['borough']}")
        
    except Exception as e:
        logger.error(f"Database client error: {e}")
        raise
    finally:
        await client.close()

if __name__ == "__main__":
    # Check if we're in the right directory
    if not (Path.cwd() / "backend").exists():
        logger.error("Please run this script from the project root directory")
        sys.exit(1)
    
    # Run client
    asyncio.run(main())
