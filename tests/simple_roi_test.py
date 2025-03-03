#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Simple test script for ROI metrics database schema.
"""

import argparse
import asyncio
import logging
import os
import sys
import sqlite3
import time
from datetime import datetime, timezone

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)

logger = logging.getLogger(__name__)


def initialize_db_columns(db_path):
    """Create the database schema with ROI columns."""
    logger.info(f"Initializing database at {db_path}")
    
    # SQL statements to create miner_stats table with ROI columns
    statements = [
        """
        CREATE TABLE IF NOT EXISTS miner_stats (
            miner_uid INTEGER PRIMARY KEY,
            miner_hotkey TEXT UNIQUE,
            miner_coldkey TEXT,
            miner_rank INTEGER DEFAULT 0,
            miner_status TEXT DEFAULT 'active',
            miner_cash REAL DEFAULT 0.0,
            miner_current_incentive REAL DEFAULT 0.0,
            miner_current_tier INTEGER DEFAULT 1,
            miner_current_scoring_window INTEGER DEFAULT 0,
            miner_current_composite_score REAL DEFAULT 0.0,
            miner_current_roi REAL DEFAULT 0.0,
            miner_current_clv_avg REAL DEFAULT 0.0,
            miner_last_prediction_date TEXT,
            miner_lifetime_earnings REAL DEFAULT 0.0,
            miner_lifetime_wager_amount REAL DEFAULT 0.0,
            miner_lifetime_roi REAL DEFAULT 0.0,
            miner_lifetime_predictions INTEGER DEFAULT 0,
            miner_lifetime_wins INTEGER DEFAULT 0,
            miner_lifetime_losses INTEGER DEFAULT 0,
            miner_win_loss_ratio REAL DEFAULT 0.0,
            most_recent_weight REAL DEFAULT 0.0
        )
        """
    ]
    
    # Check if columns need to be added
    add_column_statements = []
    
    conn = None
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Execute the base table creation
        for statement in statements:
            cursor.execute(statement)
        
        # Check if columns exist and add them if they don't
        cursor.execute("PRAGMA table_info(miner_stats)")
        columns = cursor.fetchall()
        column_names = [col[1] for col in columns]
        
        roi_columns = {
            "miner_15_day_roi": "REAL DEFAULT 0.0",
            "miner_30_day_roi": "REAL DEFAULT 0.0", 
            "miner_45_day_roi": "REAL DEFAULT 0.0"
        }
        
        for col_name, col_type in roi_columns.items():
            if col_name not in column_names:
                cursor.execute(f"ALTER TABLE miner_stats ADD COLUMN {col_name} {col_type}")
                logger.info(f"Added column {col_name} to miner_stats table")
            
        # Add some test data
        test_miners = [42, 43, 44, 45]
        for miner in test_miners:
            cursor.execute(
                """
                INSERT OR REPLACE INTO miner_stats 
                (miner_uid, miner_current_tier, miner_15_day_roi, miner_30_day_roi, miner_45_day_roi, miner_lifetime_roi)
                VALUES (?, ?, ?, ?, ?, ?)
                """, 
                (miner, 3, 0.15, 0.12, 0.10, 0.08)
            )
            
        # Add a miner with negative ROI
        cursor.execute(
            """
            INSERT OR REPLACE INTO miner_stats 
            (miner_uid, miner_current_tier, miner_15_day_roi, miner_30_day_roi, miner_45_day_roi, miner_lifetime_roi)
            VALUES (?, ?, ?, ?, ?, ?)
            """, 
            (46, 3, -0.05, -0.08, -0.10, -0.12)
        )
        
        conn.commit()
        logger.info("Database initialization successful")
        
    except sqlite3.Error as e:
        logger.error(f"SQLite error: {e}")
        return False
    finally:
        if conn:
            conn.close()
    
    return True


def verify_roi_columns(db_path):
    """Verify that the ROI columns were added and can store values."""
    logger.info("Verifying ROI columns in database")
    
    conn = None
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Check if columns exist
        cursor.execute("PRAGMA table_info(miner_stats)")
        columns = cursor.fetchall()
        
        # Adjust this to match the format of SQLite pragma output
        # Columns are (id, name, type, notnull, default_value, pk)
        column_names = [col[1] for col in columns]
        
        logger.info(f"Found columns: {column_names}")
        
        roi_columns = ["miner_15_day_roi", "miner_30_day_roi", "miner_45_day_roi", "miner_lifetime_roi"]
        missing_columns = [col for col in roi_columns if col not in column_names]
        
        if missing_columns:
            logger.error(f"Missing ROI columns: {missing_columns}")
            return False
        
        logger.info("All ROI columns exist in miner_stats table")
        
        # Verify data can be read
        cursor.execute(
            """
            SELECT 
                miner_uid, 
                miner_current_tier,
                miner_15_day_roi, 
                miner_30_day_roi, 
                miner_45_day_roi, 
                miner_lifetime_roi
            FROM miner_stats
            """
        )
        
        rows = cursor.fetchall()
        for row in rows:
            miner_uid, tier, roi_15, roi_30, roi_45, roi_lifetime = row
            logger.info(f"Miner {miner_uid} (Tier {tier}):")
            logger.info(f"  15-day ROI: {roi_15:.4f}")
            logger.info(f"  30-day ROI: {roi_30:.4f}")
            logger.info(f"  45-day ROI: {roi_45:.4f}")
            logger.info(f"  Lifetime ROI: {roi_lifetime:.4f}")
            
            # Check if negative ROI would lead to tier demotion (simple simulation)
            if tier >= 3 and roi_15 < 0:
                logger.info(f"  Miner {miner_uid} would be demoted from tier {tier} due to negative ROI")
        
        return True
        
    except sqlite3.Error as e:
        logger.error(f"SQLite error: {e}")
        return False
    finally:
        if conn:
            conn.close()


def main():
    """Main function."""
    parser = argparse.ArgumentParser(description="Test the ROI metrics database schema")
    parser.add_argument(
        "--db-path", 
        type=str, 
        default=":memory:",
        help="Path to SQLite database file (default: in-memory)"
    )
    
    args = parser.parse_args()
    
    # If using a file, make sure the directory exists
    if args.db_path != ":memory:":
        db_dir = os.path.dirname(args.db_path)
        if db_dir and not os.path.exists(db_dir):
            os.makedirs(db_dir)
    
    try:
        if initialize_db_columns(args.db_path):
            if verify_roi_columns(args.db_path):
                logger.info("ROI metrics database schema test completed successfully")
            else:
                logger.error("ROI metrics database schema verification failed")
                sys.exit(1)
        else:
            logger.error("ROI metrics database schema initialization failed")
            sys.exit(1)
            
    except Exception as e:
        logger.error(f"Error during testing: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main() 