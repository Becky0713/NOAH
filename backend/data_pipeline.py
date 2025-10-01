"""
NYC Housing Data Pipeline
Ingests data from Socrata API and stores in PostgreSQL database
"""

import asyncio
import logging
from datetime import datetime
from typing import List, Dict, Any, Optional
import asyncpg
import httpx
from .config import settings

logger = logging.getLogger(__name__)

class DataPipeline:
    def __init__(self):
        self.db_pool: Optional[asyncpg.Pool] = None
        self.http_client: Optional[httpx.AsyncClient] = None
    
    async def initialize(self):
        """Initialize database connection pool and HTTP client"""
        # Database connection
        self.db_pool = await asyncpg.create_pool(
            host=settings.db_host,
            port=settings.db_port,
            user=settings.db_user,
            password=settings.db_password,
            database=settings.db_name,
            min_size=1,
            max_size=10
        )
        
        # HTTP client for Socrata API
        self.http_client = httpx.AsyncClient(
            timeout=30,
            headers={"X-App-Token": settings.socrata_app_token} if settings.socrata_app_token else {}
        )
    
    async def close(self):
        """Close connections"""
        if self.db_pool:
            await self.db_pool.close()
        if self.http_client:
            await self.http_client.aclose()
    
    async def fetch_socrata_data(self, limit: int = 1000, offset: int = 0) -> List[Dict[str, Any]]:
        """Fetch data from Socrata API"""
        url = f"{settings.socrata_base_url}/resource/{settings.socrata_dataset_id}.json"
        params = {
            "$limit": limit,
            "$offset": offset,
            "$order": "project_id"
        }
        
        try:
            response = await self.http_client.get(url, params=params)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Failed to fetch Socrata data: {e}")
            return []
    
    def normalize_data(self, raw_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Normalize and clean data from Socrata"""
        normalized = []
        
        for item in raw_data:
            try:
                # Convert string numbers to integers/floats
                def safe_int(value, default=0):
                    try:
                        return int(float(value)) if value else default
                    except (ValueError, TypeError):
                        return default
                
                def safe_float(value, default=None):
                    try:
                        return float(value) if value else default
                    except (ValueError, TypeError):
                        return default
                
                def safe_date(value):
                    if not value:
                        return None
                    try:
                        # Handle Socrata date format
                        if 'T' in str(value):
                            return datetime.fromisoformat(str(value).replace('Z', '+00:00')).date()
                        return datetime.strptime(str(value), '%Y-%m-%d').date()
                    except (ValueError, TypeError):
                        return None
                
                normalized_item = {
                    'project_id': item.get('project_id'),
                    'project_name': item.get('project_name'),
                    'building_id': item.get('building_id'),
                    'house_number': item.get('house_number'),
                    'street_name': item.get('street_name'),
                    'borough': item.get('borough'),
                    'postcode': item.get('postcode'),
                    'bbl': item.get('bbl'),
                    'bin': item.get('bin'),
                    'community_board': item.get('community_board'),
                    'council_district': safe_int(item.get('council_district')),
                    'census_tract': item.get('census_tract'),
                    'neighborhood_tabulation_area': item.get('neighborhood_tabulation_area'),
                    
                    # Coordinates
                    'latitude': safe_float(item.get('latitude')),
                    'longitude': safe_float(item.get('longitude')),
                    'latitude_internal': safe_float(item.get('latitude_internal')),
                    'longitude_internal': safe_float(item.get('longitude_internal')),
                    
                    # Dates
                    'project_start_date': safe_date(item.get('project_start_date')),
                    'project_completion_date': safe_date(item.get('project_completion_date')),
                    'building_completion_date': safe_date(item.get('building_completion_date')),
                    
                    # Project details
                    'reporting_construction_type': item.get('reporting_construction_type'),
                    'extended_affordability_status': item.get('extended_affordability_status'),
                    'prevailing_wage_status': item.get('prevailing_wage_status'),
                    
                    # Unit counts
                    'extremely_low_income_units': safe_int(item.get('extremely_low_income_units')),
                    'very_low_income_units': safe_int(item.get('very_low_income_units')),
                    'low_income_units': safe_int(item.get('low_income_units')),
                    'moderate_income_units': safe_int(item.get('moderate_income_units')),
                    'middle_income_units': safe_int(item.get('middle_income_units')),
                    'other_income_units': safe_int(item.get('other_income_units')),
                    
                    # Bedroom counts
                    'studio_units': safe_int(item.get('studio_units')),
                    '_1_br_units': safe_int(item.get('_1_br_units')),
                    '_2_br_units': safe_int(item.get('_2_br_units')),
                    '_3_br_units': safe_int(item.get('_3_br_units')),
                    '_4_br_units': safe_int(item.get('_4_br_units')),
                    '_5_br_units': safe_int(item.get('_5_br_units')),
                    '_6_br_units': safe_int(item.get('_6_br_units')),
                    'unknown_br_units': safe_int(item.get('unknown_br_units')),
                    
                    # Summary counts
                    'counted_rental_units': safe_int(item.get('counted_rental_units')),
                    'counted_homeownership_units': safe_int(item.get('counted_homeownership_units')),
                    'all_counted_units': safe_int(item.get('all_counted_units')),
                    'total_units': safe_int(item.get('total_units')),
                }
                
                normalized.append(normalized_item)
                
            except Exception as e:
                logger.warning(f"Failed to normalize item {item.get('project_id', 'unknown')}: {e}")
                continue
        
        return normalized
    
    async def upsert_data(self, data: List[Dict[str, Any]]):
        """Upsert data into database"""
        if not self.db_pool:
            raise RuntimeError("Database pool not initialized")
        
        async with self.db_pool.acquire() as conn:
            for item in data:
                try:
                    await conn.execute("""
                        INSERT INTO housing_projects (
                            project_id, project_name, building_id, house_number, street_name,
                            borough, postcode, bbl, bin, community_board, council_district,
                            census_tract, neighborhood_tabulation_area, latitude, longitude,
                            latitude_internal, longitude_internal, project_start_date,
                            project_completion_date, building_completion_date,
                            reporting_construction_type, extended_affordability_status,
                            prevailing_wage_status, extremely_low_income_units,
                            very_low_income_units, low_income_units, moderate_income_units,
                            middle_income_units, other_income_units, studio_units,
                            _1_br_units, _2_br_units, _3_br_units, _4_br_units,
                            _5_br_units, _6_br_units, unknown_br_units,
                            counted_rental_units, counted_homeownership_units,
                            all_counted_units, total_units
                        ) VALUES (
                            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15,
                            $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28,
                            $29, $30, $31, $32, $33, $34, $35, $36, $37, $38, $39, $40, $41
                        )
                        ON CONFLICT (project_id) DO UPDATE SET
                            project_name = EXCLUDED.project_name,
                            building_id = EXCLUDED.building_id,
                            house_number = EXCLUDED.house_number,
                            street_name = EXCLUDED.street_name,
                            borough = EXCLUDED.borough,
                            postcode = EXCLUDED.postcode,
                            bbl = EXCLUDED.bbl,
                            bin = EXCLUDED.bin,
                            community_board = EXCLUDED.community_board,
                            council_district = EXCLUDED.council_district,
                            census_tract = EXCLUDED.census_tract,
                            neighborhood_tabulation_area = EXCLUDED.neighborhood_tabulation_area,
                            latitude = EXCLUDED.latitude,
                            longitude = EXCLUDED.longitude,
                            latitude_internal = EXCLUDED.latitude_internal,
                            longitude_internal = EXCLUDED.longitude_internal,
                            project_start_date = EXCLUDED.project_start_date,
                            project_completion_date = EXCLUDED.project_completion_date,
                            building_completion_date = EXCLUDED.building_completion_date,
                            reporting_construction_type = EXCLUDED.reporting_construction_type,
                            extended_affordability_status = EXCLUDED.extended_affordability_status,
                            prevailing_wage_status = EXCLUDED.prevailing_wage_status,
                            extremely_low_income_units = EXCLUDED.extremely_low_income_units,
                            very_low_income_units = EXCLUDED.very_low_income_units,
                            low_income_units = EXCLUDED.low_income_units,
                            moderate_income_units = EXCLUDED.moderate_income_units,
                            middle_income_units = EXCLUDED.middle_income_units,
                            other_income_units = EXCLUDED.other_income_units,
                            studio_units = EXCLUDED.studio_units,
                            _1_br_units = EXCLUDED._1_br_units,
                            _2_br_units = EXCLUDED._2_br_units,
                            _3_br_units = EXCLUDED._3_br_units,
                            _4_br_units = EXCLUDED._4_br_units,
                            _5_br_units = EXCLUDED._5_br_units,
                            _6_br_units = EXCLUDED._6_br_units,
                            unknown_br_units = EXCLUDED.unknown_br_units,
                            counted_rental_units = EXCLUDED.counted_rental_units,
                            counted_homeownership_units = EXCLUDED.counted_homeownership_units,
                            all_counted_units = EXCLUDED.all_counted_units,
                            total_units = EXCLUDED.total_units,
                            updated_at = CURRENT_TIMESTAMP
                    """, *[
                        item['project_id'], item['project_name'], item['building_id'],
                        item['house_number'], item['street_name'], item['borough'],
                        item['postcode'], item['bbl'], item['bin'], item['community_board'],
                        item['council_district'], item['census_tract'], item['neighborhood_tabulation_area'],
                        item['latitude'], item['longitude'], item['latitude_internal'],
                        item['longitude_internal'], item['project_start_date'], item['project_completion_date'],
                        item['building_completion_date'], item['reporting_construction_type'],
                        item['extended_affordability_status'], item['prevailing_wage_status'],
                        item['extremely_low_income_units'], item['very_low_income_units'],
                        item['low_income_units'], item['moderate_income_units'],
                        item['middle_income_units'], item['other_income_units'],
                        item['studio_units'], item['_1_br_units'], item['_2_br_units'],
                        item['_3_br_units'], item['_4_br_units'], item['_5_br_units'],
                        item['_6_br_units'], item['unknown_br_units'], item['counted_rental_units'],
                        item['counted_homeownership_units'], item['all_counted_units'], item['total_units']
                    ])
                except Exception as e:
                    logger.error(f"Failed to upsert item {item.get('project_id', 'unknown')}: {e}")
                    continue
    
    async def run_full_sync(self, batch_size: int = 1000):
        """Run full data synchronization"""
        logger.info("Starting full data sync...")
        
        offset = 0
        total_processed = 0
        
        while True:
            # Fetch batch from Socrata
            raw_data = await self.fetch_socrata_data(limit=batch_size, offset=offset)
            if not raw_data:
                break
            
            # Normalize data
            normalized_data = self.normalize_data(raw_data)
            
            # Upsert to database
            await self.upsert_data(normalized_data)
            
            total_processed += len(normalized_data)
            offset += batch_size
            
            logger.info(f"Processed {total_processed} records...")
            
            # If we got less than batch_size, we're done
            if len(raw_data) < batch_size:
                break
        
        logger.info(f"Full sync completed. Total records processed: {total_processed}")

async def main():
    """Main function to run the data pipeline"""
    pipeline = DataPipeline()
    
    try:
        await pipeline.initialize()
        await pipeline.run_full_sync()
    finally:
        await pipeline.close()

if __name__ == "__main__":
    asyncio.run(main())
