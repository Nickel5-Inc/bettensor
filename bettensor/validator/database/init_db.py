"""
Database initialization script.
"""

import os
import sys
import logging
from pathlib import Path
from .postgres_init import create_postgres_tables

def main():
    """Initialize the database."""
    # Set up logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    logger = logging.getLogger(__name__)
    
    try:
        # Get database configuration from environment or use defaults
        host = os.getenv("BETTENSOR_DB_HOST", "localhost")
        port = int(os.getenv("BETTENSOR_DB_PORT", "5432"))
        user = os.getenv("BETTENSOR_DB_USER", "postgres")
        password = os.getenv("BETTENSOR_DB_PASSWORD", "postgres")
        dbname = os.getenv("BETTENSOR_DB_NAME", "bettensor_validator")
        
        # Create tables
        create_postgres_tables(
            host=host,
            port=port,
            user=user,
            password=password,
            dbname=dbname
        )
        
        logger.info("Database initialization completed successfully")
        return 0
        
    except Exception as e:
        logger.error(f"Database initialization failed: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main()) 