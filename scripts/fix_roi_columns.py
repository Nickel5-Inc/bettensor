#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Script to fix ROI columns in the database

This script adds missing ROI columns to the miner_stats table and initializes
them with default values. Run this if you encounter errors related to
missing ROI columns.

Usage:
    python scripts/fix_roi_columns.py --db_path /path/to/database.db
"""

import argparse
import sqlite3
import os
import sys
import logging
import traceback
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)

logger = logging.getLogger(__name__)

def backup_database(db_path):
    """Create a backup of the database before making changes"""
    backup_path = f"{db_path}.bak"
    try:
        # Check if database exists
        if not os.path.exists(db_path):
            logger.error(f"Database file {db_path} does not exist")
            return False
            
        # Create a backup
        with open(db_path, 'rb') as src, open(backup_path, 'wb') as dst:
            dst.write(src.read())
        
        logger.info(f"Created backup at {backup_path}")
        return True
    except Exception as e:
        logger.error(f"Failed to create backup: {e}")
        return False

def fix_roi_columns(db_path):
    """
    Add missing ROI columns to the miner_stats table and initialize them with default values.
    """
    logger.info(f"Fixing ROI columns in database at {db_path}")
    
    try:
        # Connect to the database
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Check if miner_stats table exists
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='miner_stats'")
        if not cursor.fetchone():
            logger.error("miner_stats table does not exist in the database")
            return False
        
        # Get existing columns
        cursor.execute("PRAGMA table_info(miner_stats)")
        columns = cursor.fetchall()
        column_names = [col[1] for col in columns]
        logger.info(f"Existing columns: {column_names}")
        
        # Define ROI columns to add
        roi_columns = {
            "miner_15_day_roi": "REAL DEFAULT 0.0",
            "miner_30_day_roi": "REAL DEFAULT 0.0",
            "miner_45_day_roi": "REAL DEFAULT 0.0"
        }
        
        # Add missing columns
        for column_name, column_type in roi_columns.items():
            if column_name not in column_names:
                try:
                    alter_query = f"ALTER TABLE miner_stats ADD COLUMN {column_name} {column_type}"
                    cursor.execute(alter_query)
                    logger.info(f"Added column {column_name} to miner_stats table")
                except sqlite3.OperationalError as e:
                    if "duplicate column name" in str(e):
                        logger.info(f"Column {column_name} already exists, skipping")
                    else:
                        logger.error(f"Error adding column {column_name}: {e}")
                        # Continue with other columns
            else:
                logger.info(f"Column {column_name} already exists, skipping")
        
        # Set default values for NULL entries
        update_query = """
            UPDATE miner_stats
            SET miner_15_day_roi = COALESCE(miner_15_day_roi, 0.0),
                miner_30_day_roi = COALESCE(miner_30_day_roi, 0.0),
                miner_45_day_roi = COALESCE(miner_45_day_roi, 0.0)
            WHERE miner_15_day_roi IS NULL OR miner_30_day_roi IS NULL OR miner_45_day_roi IS NULL
        """
        cursor.execute(update_query)
        rows_updated = cursor.rowcount
        logger.info(f"Updated {rows_updated} rows with NULL ROI values")
        
        # Verify the columns were added correctly
        cursor.execute("PRAGMA table_info(miner_stats)")
        columns_after = cursor.fetchall()
        column_names_after = [col[1] for col in columns_after]
        
        # Check if all columns are present
        missing_columns = [col for col in roi_columns.keys() if col not in column_names_after]
        if missing_columns:
            logger.error(f"Failed to add columns: {missing_columns}")
            return False
        
        # Commit changes
        conn.commit()
        logger.info("ROI columns fixed successfully")
        
        # Return success
        return True
        
    except Exception as e:
        logger.error(f"Error fixing ROI columns: {e}")
        logger.error(traceback.format_exc())
        return False
    finally:
        if 'conn' in locals() and conn:
            conn.close()

def main():
    parser = argparse.ArgumentParser(description="Fix ROI columns in the database")
    parser.add_argument(
        "--db_path", 
        type=str, 
        required=True,
        help="Path to the database file"
    )
    parser.add_argument(
        "--no_backup", 
        action="store_true",
        help="Skip database backup before making changes"
    )
    
    args = parser.parse_args()
    
    # Validate database path
    db_path = args.db_path
    if not os.path.exists(db_path):
        logger.error(f"Database file not found: {db_path}")
        sys.exit(1)
    
    # Create backup unless --no_backup is specified
    if not args.no_backup:
        if not backup_database(db_path):
            logger.error("Failed to create backup, aborting")
            sys.exit(1)
    
    # Fix ROI columns
    success = fix_roi_columns(db_path)
    
    if success:
        logger.info("ROI columns fixed successfully")
        sys.exit(0)
    else:
        logger.error("Failed to fix ROI columns")
        sys.exit(1)

if __name__ == "__main__":
    main() 