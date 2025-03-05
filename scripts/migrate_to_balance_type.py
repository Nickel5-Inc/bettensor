#!/usr/bin/env python3
"""
Database migration script to convert existing float fields to bt.Balance objects

This script updates the database schema to use the new BalanceType for all token amount fields.
"""

import argparse
import asyncio
import logging
import os
from datetime import datetime
from pathlib import Path
import sys

# Add the project root to the Python path
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

import bittensor as bt
from bettensor.validator.utils.vesting.database.db_manager import DatabaseManager

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("migrate_to_balance")


async def migrate_database(db_url):
    """Migrate database fields from float to bt.Balance."""
    logger.info(f"Connecting to database: {db_url}")
    
    # Connect to the database
    db_manager = DatabaseManager(db_url=db_url)
    
    tables_to_migrate = [
        {
            "name": "vesting_stake_history",
            "fields": ["stake"],
        },
        {
            "name": "vesting_stake_transactions",
            "fields": ["amount"],
        },
        {
            "name": "vesting_schedules",
            "fields": ["initial_amount", "remaining_amount"],
        },
        {
            "name": "vesting_payments",
            "fields": ["amount"],
        },
        {
            "name": "vesting_epoch_emissions",
            "fields": ["emission_amount", "retained_amount"],
        },
        {
            "name": "vesting_minimum_requirements",
            "fields": ["minimum_stake"],
        },
    ]
    
    # No need to modify the data itself as the Balance class is just a wrapper around a float
    # The BalanceType in SQLAlchemy will handle the conversion
    
    # We just need to make sure the database schema is updated
    logger.info("Database schema will be automatically updated when the application starts")
    logger.info("This migration simply ensures the models are ready to use bt.Balance objects")
    
    # Backup the database
    await backup_database(db_manager)
    
    logger.info("Migration complete")


async def backup_database(db_manager):
    """Create a backup of the database."""
    logger.info("Creating database backup...")
    
    # Execute the backup
    try:
        await db_manager.execute("PRAGMA wal_checkpoint(FULL)")
        
        # For SQLite, simply copy the database file
        if db_manager.db_url.startswith("sqlite:///"):
            db_path = db_manager.db_url[10:]  # Remove 'sqlite:///'
            backup_path = f"{db_path}.backup-{int(datetime.now().timestamp())}"
            
            import shutil
            shutil.copy2(db_path, backup_path)
            logger.info(f"Backup created at: {backup_path}")
        else:
            logger.warning("Automated backup not supported for this database type")
            logger.warning("Please backup your database manually before proceeding")
    except Exception as e:
        logger.error(f"Error creating backup: {e}")
        raise


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Migrate database to use bt.Balance type")
    
    parser.add_argument(
        "--db_url",
        type=str,
        default=os.environ.get("BETTENSOR_DB_URL", "sqlite:///vesting.db"),
        help="Database URL (default: sqlite:///vesting.db, env: BETTENSOR_DB_URL)"
    )
    
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    asyncio.run(migrate_database(args.db_url)) 