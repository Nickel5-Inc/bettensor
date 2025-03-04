#!/usr/bin/env python3

import os
import sqlite3
import argparse
import sys

def reset_database_schema(db_path, backup=True):
    """Reset the database schema to fix column issues."""
    print(f"Checking database at: {db_path}")
    
    # Backup database if requested
    if backup:
        backup_path = f"{db_path}.backup.{os.path.getmtime(db_path)}"
        print(f"Creating backup at: {backup_path}")
        try:
            with open(db_path, 'rb') as src, open(backup_path, 'wb') as dst:
                dst.write(src.read())
            print("Backup created successfully")
        except Exception as e:
            print(f"Error creating backup: {e}")
            sys.exit(1)
    
    # Connect to database
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        print("Connected to database")
    except Exception as e:
        print(f"Error connecting to database: {e}")
        sys.exit(1)
    
    try:
        # First add the missing columns to miner_stats table
        print("Adding ROI columns to miner_stats table...")
        try:
            cursor.execute("ALTER TABLE miner_stats ADD COLUMN miner_15_day_roi REAL DEFAULT 0.0")
            print("Added miner_15_day_roi column")
        except sqlite3.OperationalError as e:
            if "duplicate column name" in str(e):
                print("Column miner_15_day_roi already exists")
            else:
                print(f"Error adding miner_15_day_roi column: {e}")
        
        try:
            cursor.execute("ALTER TABLE miner_stats ADD COLUMN miner_30_day_roi REAL DEFAULT 0.0")
            print("Added miner_30_day_roi column")
        except sqlite3.OperationalError as e:
            if "duplicate column name" in str(e):
                print("Column miner_30_day_roi already exists")
            else:
                print(f"Error adding miner_30_day_roi column: {e}")
        
        try:
            cursor.execute("ALTER TABLE miner_stats ADD COLUMN miner_45_day_roi REAL DEFAULT 0.0")
            print("Added miner_45_day_roi column")
        except sqlite3.OperationalError as e:
            if "duplicate column name" in str(e):
                print("Column miner_45_day_roi already exists")
            else:
                print(f"Error adding miner_45_day_roi column: {e}")
                
        # Get table information
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [table[0] for table in cursor.fetchall()]
        print(f"Found tables: {', '.join(tables)}")
        
        # Check if miner_stats_backup exists
        if 'miner_stats_backup' in tables:
            print("Found miner_stats_backup table, dropping it...")
            cursor.execute("DROP TABLE miner_stats_backup")
            print("Table dropped successfully")
        
        # Create new miner_stats_backup table with all columns
        print("Creating new miner_stats_backup table...")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS miner_stats_backup (
                miner_uid INTEGER,
                miner_hotkey TEXT,
                miner_coldkey TEXT,
                miner_rank INTEGER,
                miner_status TEXT,
                miner_cash REAL,
                miner_current_incentive REAL,
                miner_current_tier INTEGER,
                miner_current_scoring_window INTEGER,
                miner_current_composite_score REAL,
                miner_current_entropy_score REAL,
                miner_current_sharpe_ratio REAL,
                miner_current_sortino_ratio REAL,
                miner_current_roi REAL,
                miner_current_clv_avg REAL,
                miner_last_prediction_date TEXT,
                miner_lifetime_earnings REAL,
                miner_lifetime_wager_amount REAL,
                miner_lifetime_roi REAL,
                miner_lifetime_predictions INTEGER,
                miner_lifetime_wins INTEGER,
                miner_lifetime_losses INTEGER,
                miner_win_loss_ratio REAL,
                most_recent_weight REAL DEFAULT 0.0,
                miner_15_day_roi REAL DEFAULT 0.0,
                miner_30_day_roi REAL DEFAULT 0.0,
                miner_45_day_roi REAL DEFAULT 0.0
            )
        """)
        
        # Check schema of miner_stats table
        cursor.execute("PRAGMA table_info(miner_stats)")
        columns = cursor.fetchall()
        print(f"miner_stats table now has {len(columns)} columns:")
        for col in columns:
            print(f"  - {col[1]} ({col[2]})")
        
        # Commit changes
        conn.commit()
        print("Database schema updates completed successfully")
        
    except Exception as e:
        print(f"Error updating database schema: {e}")
        conn.rollback()
        sys.exit(1)
    finally:
        conn.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Reset Bettensor validator database schema to fix column issues")
    parser.add_argument("--db-path", default="/root/bettensor/bettensor/validator/state/validator.db", help="Path to validator database")
    parser.add_argument("--no-backup", action="store_true", help="Skip database backup")
    args = parser.parse_args()
    
    reset_database_schema(args.db_path, not args.no_backup) 