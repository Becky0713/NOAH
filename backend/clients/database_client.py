"""
Database Client
Queries data from PostgreSQL + PostGIS database
"""

import asyncio
import logging
from typing import Any, Dict, List, Optional
import asyncpg
from .base import BaseHousingClient
from ..config import settings

logger = logging.getLogger(__name__)

class DatabaseHousingClient(BaseHousingClient):
    """PostgreSQL + PostGIS database client for housing data."""
    
    def __init__(self):
        self.pool: Optional[asyncpg.Pool] = None
    
    async def _get_pool(self) -> asyncpg.Pool:
        """Get or create database connection pool"""
        if self.pool is None:
            self.pool = await asyncpg.create_pool(
                host=settings.db_host,
                port=settings.db_port,
                user=settings.db_user,
                password=settings.db_password,
                database=settings.db_name,
                min_size=1,
                max_size=10
            )
        return self.pool
    
    async def close(self):
        """Close database connection pool"""
        if self.pool:
            await self.pool.close()
            self.pool = None
    
    async def fetch_region_summary(self, region_id: str) -> Dict[str, Any]:
        """Fetch summary statistics for a region from database"""
        try:
            pool = await self._get_pool()
            async with pool.acquire() as conn:
                # Count total projects in region
                count_query = """
                    SELECT COUNT(*) as listing_count
                    FROM housing_projects 
                    WHERE borough = $1
                """
                count_result = await conn.fetchrow(count_query, region_id)
                listing_count = count_result['listing_count'] if count_result else 0
                
                # Get unit statistics
                stats_query = """
                    SELECT 
                        AVG(total_units) as avg_units,
                        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY total_units) as median_units
                    FROM housing_projects 
                    WHERE borough = $1 AND total_units > 0
                """
                stats_result = await conn.fetchrow(stats_query, region_id)
                
                region = {"id": region_id, "name": region_id.replace("_", " ").title()}
                return {
                    "region": region,
                    "listing_count": listing_count,
                    "median_rent": None,  # Not applicable for housing units
                    "average_rent": None,  # Not applicable for housing units
                    "vacancy_rate": None,
                }
        except Exception as e:
            logger.error(f"Database error in fetch_region_summary: {e}")
            return {
                "region": {"id": region_id, "name": region_id.replace("_", " ").title()},
                "listing_count": 0,
                "median_rent": None,
                "average_rent": None,
                "vacancy_rate": None,
            }
    
    async def fetch_listings(self, region_id: str, limit: int = 50) -> List[Dict[str, Any]]:
        """Fetch housing listings for a region from database"""
        try:
            pool = await self._get_pool()
            async with pool.acquire() as conn:
                query = """
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
                    WHERE borough = $1
                    ORDER BY total_units DESC
                    LIMIT $2
                """
                rows = await conn.fetch(query, region_id, limit)
                return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"Database error in fetch_listings: {e}")
            return []
    
    async def fetch_metadata_fields(self) -> List[Dict[str, Any]]:
        """Fetch field metadata from database schema"""
        try:
            pool = await self._get_pool()
            async with pool.acquire() as conn:
                # Get column information from database
                query = """
                    SELECT 
                        column_name as field_name,
                        data_type as data_type,
                        COALESCE(col_description(c.oid, ordinal_position), '') as description
                    FROM information_schema.columns c
                    LEFT JOIN pg_class t ON t.relname = c.table_name
                    WHERE table_name = 'housing_projects'
                    AND column_name NOT IN ('created_at', 'updated_at', 'geom')
                    ORDER BY ordinal_position
                """
                rows = await conn.fetch(query)
                
                # Map database types to user-friendly types
                type_mapping = {
                    'character varying': 'text',
                    'integer': 'number',
                    'numeric': 'number',
                    'date': 'calendar_date',
                    'timestamp with time zone': 'calendar_date',
                    'double precision': 'number',
                    'text': 'text'
                }
                
                fields = []
                for row in rows:
                    field_name = row['field_name']
                    data_type = type_mapping.get(row['data_type'], 'text')
                    description = row['description'] or f"Database field: {field_name}"
                    
                    fields.append({
                        "field_name": field_name,
                        "data_type": data_type,
                        "description": description
                    })
                
                return fields
        except Exception as e:
            logger.error(f"Database error in fetch_metadata_fields: {e}")
            return []
    
    async def fetch_records(
        self,
        fields: List[str],
        limit: int = 100,
        offset: int = 0,
        borough: str = "",
        min_units: int = 0,
        max_units: int = 0,
        start_date_from: str = "",
        start_date_to: str = ""
    ) -> List[Dict[str, Any]]:
        """Fetch housing records from database with filtering"""
        try:
            pool = await self._get_pool()
            async with pool.acquire() as conn:
                # Build SELECT clause
                select_fields = []
                for field in fields:
                    if field in ['house_number', 'street_name']:
                        select_fields.append(field)
                    elif field == 'address':
                        select_fields.append("CONCAT(house_number, ' ', street_name) as address")
                    else:
                        select_fields.append(field)
                
                select_clause = ", ".join(select_fields)
                
                # Build WHERE clause
                where_conditions = []
                params = []
                param_count = 0
                
                if borough:
                    param_count += 1
                    where_conditions.append(f"borough = ${param_count}")
                    params.append(borough)
                
                if min_units > 0:
                    param_count += 1
                    where_conditions.append(f"total_units >= ${param_count}")
                    params.append(min_units)
                
                if max_units > 0:
                    param_count += 1
                    where_conditions.append(f"total_units <= ${param_count}")
                    params.append(max_units)
                
                if start_date_from:
                    param_count += 1
                    where_conditions.append(f"project_start_date >= ${param_count}")
                    params.append(start_date_from)
                
                if start_date_to:
                    param_count += 1
                    where_conditions.append(f"project_start_date <= ${param_count}")
                    params.append(start_date_to)
                
                where_clause = " AND ".join(where_conditions) if where_conditions else "1=1"
                
                # Add pagination parameters
                param_count += 1
                params.append(limit)
                param_count += 1
                params.append(offset)
                
                query = f"""
                    SELECT {select_clause}
                    FROM housing_projects
                    WHERE {where_clause}
                    ORDER BY total_units DESC
                    LIMIT ${param_count - 1} OFFSET ${param_count}
                """
                
                rows = await conn.fetch(query, *params)
                return [dict(row) for row in rows]
                
        except Exception as e:
            logger.error(f"Database error in fetch_records: {e}")
            return []
    
    async def get_database_stats(self) -> Dict[str, Any]:
        """Get database statistics"""
        try:
            pool = await self._get_pool()
            async with pool.acquire() as conn:
                # Total records
                total_query = "SELECT COUNT(*) as total FROM housing_projects"
                total_result = await conn.fetchrow(total_query)
                total_records = total_result['total'] if total_result else 0
                
                # Records by borough
                borough_query = """
                    SELECT borough, COUNT(*) as count, SUM(total_units) as total_units
                    FROM housing_projects 
                    WHERE borough IS NOT NULL
                    GROUP BY borough 
                    ORDER BY count DESC
                """
                borough_rows = await conn.fetch(borough_query)
                borough_stats = [dict(row) for row in borough_rows]
                
                # Records with coordinates
                coords_query = """
                    SELECT COUNT(*) as count 
                    FROM housing_projects 
                    WHERE latitude IS NOT NULL AND longitude IS NOT NULL
                """
                coords_result = await conn.fetchrow(coords_query)
                with_coordinates = coords_result['count'] if coords_result else 0
                
                # Date range
                date_query = """
                    SELECT 
                        MIN(project_start_date) as earliest_start,
                        MAX(project_start_date) as latest_start,
                        MIN(project_completion_date) as earliest_completion,
                        MAX(project_completion_date) as latest_completion
                    FROM housing_projects
                """
                date_result = await conn.fetchrow(date_query)
                date_range = dict(date_result) if date_result else {}
                
                # Unit statistics
                unit_query = """
                    SELECT 
                        SUM(total_units) as total_units,
                        SUM(all_counted_units) as total_affordable_units,
                        AVG(total_units) as avg_units_per_project,
                        MAX(total_units) as max_units_per_project
                    FROM housing_projects
                """
                unit_result = await conn.fetchrow(unit_query)
                unit_stats = dict(unit_result) if unit_result else {}
                
                return {
                    "total_records": total_records,
                    "with_coordinates": with_coordinates,
                    "by_borough": borough_stats,
                    "date_range": date_range,
                    "unit_stats": unit_stats
                }
                
        except Exception as e:
            logger.error(f"Database error in get_database_stats: {e}")
            return {
                "total_records": 0,
                "with_coordinates": 0,
                "by_borough": [],
                "date_range": {},
                "unit_stats": {}
            }
