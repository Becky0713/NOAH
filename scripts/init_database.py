#!/usr/bin/env python3
"""
Database Initialization Script
Creates the PostgreSQL database and runs migrations
"""

import asyncio
import logging
import sys
import os
from pathlib import Path

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

async def create_database():
    """Create the database if it doesn't exist"""
    # Connect to postgres database to create our target database
    conn = await asyncpg.connect(
        host=settings.db_host,
        port=settings.db_port,
        user=settings.db_user,
        password=settings.db_password,
        database='postgres'  # Connect to default postgres database
    )
    
    try:
        # Check if database exists
        result = await conn.fetchval(
            "SELECT 1 FROM pg_database WHERE datname = $1", 
            settings.db_name
        )
        
        if result:
            logger.info(f"Database '{settings.db_name}' already exists")
        else:
            # Create database
            await conn.execute(f'CREATE DATABASE "{settings.db_name}"')
            logger.info(f"Database '{settings.db_name}' created successfully")
            
    finally:
        await conn.close()

async def run_migrations():
    """Run database migrations"""
    # Connect to our target database
    conn = await asyncpg.connect(
        host=settings.db_host,
        port=settings.db_port,
        user=settings.db_user,
        password=settings.db_password,
        database=settings.db_name
    )
    
    try:
        # Read and execute migration file
        migration_file = Path(__file__).parent.parent / "backend" / "migrations" / "001_create_housing_projects_table.sql"
        
        if not migration_file.exists():
            logger.error(f"Migration file not found: {migration_file}")
            return
        
        with open(migration_file, 'r') as f:
            migration_sql = f.read()
        
        # Execute migration
        await conn.execute(migration_sql)
        logger.info("Database migration completed successfully")
        
    finally:
        await conn.close()

async def main():
    """Main function to initialize database"""
    logger.info("Initializing NYC Housing Database...")
    logger.info(f"Host: {settings.db_host}:{settings.db_port}")
    logger.info(f"Database: {settings.db_name}")
    logger.info(f"User: {settings.db_user}")
    
    try:
        # Create database
        await create_database()
        
        # Run migrations
        await run_migrations()
        
        logger.info("Database initialization completed successfully!")
        
    except Exception as e:
        logger.error(f"Database initialization failed: {e}")
        raise

if __name__ == "__main__":
    # Check if we're in the right directory
    if not (Path.cwd() / "backend").exists():
        logger.error("Please run this script from the project root directory")
        sys.exit(1)
    
    # Run initialization
    asyncio.run(main())
