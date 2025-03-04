#!/usr/bin/env python3

import asyncio
import bittensor as bt
from bettensor.validator.utils.database.database_manager import DatabaseManager

async def fix_database():
    """Add missing ROI columns to the miner_stats table."""
    print("Starting database repair...")
    
    # Initialize database manager
    db_manager = DatabaseManager()
    await db_manager.initialize()
    
    # Check if columns exist first to avoid errors
    print("Checking for missing columns...")
    table_info_query = "PRAGMA table_info(miner_stats)"
    columns = await db_manager.fetch_all(table_info_query)
    
    # Extract existing column names
    existing_columns = [col['name'] for col in columns]
    
    # Define new columns to add
    new_columns = {
        "miner_15_day_roi": "REAL DEFAULT 0",
        "miner_30_day_roi": "REAL DEFAULT 0",
        "miner_45_day_roi": "REAL DEFAULT 0"
    }
    
    # Add missing columns
    for column_name, column_type in new_columns.items():
        if column_name not in existing_columns:
            print(f"Adding missing column: {column_name}")
            alter_query = f"ALTER TABLE miner_stats ADD COLUMN {column_name} {column_type}"
            await db_manager.execute_query(alter_query)
            print(f"Added column {column_name} to miner_stats table")
        else:
            print(f"Column {column_name} already exists")
    
    print("Database repair completed successfully")

if __name__ == "__main__":
    asyncio.run(fix_database()) 