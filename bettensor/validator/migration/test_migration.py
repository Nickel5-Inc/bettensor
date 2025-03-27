#!/usr/bin/env python3
# The MIT License (MIT)
# Copyright © 2023 Bettensor
# See license at: https://github.com/bettensor/bettensor/blob/main/LICENSE

import argparse
import os
import sys
import logging
import time
import configparser
import subprocess
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("migration_test")

def setup_sqlite_test_db(sqlite_path: str) -> bool:
    """Create a test SQLite database with sample data.
    
    Args:
        sqlite_path: Path to create the test SQLite database
        
    Returns:
        bool: True if setup successful, False otherwise
    """
    try:
        import sqlite3
        
        # Create the directory if it doesn't exist
        os.makedirs(os.path.dirname(sqlite_path), exist_ok=True)
        
        # Connect to SQLite
        conn = sqlite3.connect(sqlite_path)
        cursor = conn.cursor()
        
        # Create tables for migration testing
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS miner_stats (
            id INTEGER PRIMARY KEY,
            hotkey TEXT NOT NULL,
            blocks INTEGER DEFAULT 0,
            total_rewards REAL DEFAULT 0.0,
            last_updated TEXT
        )
        ''')
        
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS block_data (
            block_id INTEGER PRIMARY KEY,
            timestamp INTEGER NOT NULL,
            miner TEXT NOT NULL,
            rewards REAL DEFAULT 0.0
        )
        ''')
        
        # Insert sample data
        cursor.execute("INSERT INTO miner_stats (hotkey, blocks, total_rewards, last_updated) VALUES (?, ?, ?, ?)",
                      ("miner1", 100, 50.5, "2023-10-01"))
        cursor.execute("INSERT INTO miner_stats (hotkey, blocks, total_rewards, last_updated) VALUES (?, ?, ?, ?)",
                      ("miner2", 200, 100.25, "2023-10-02"))
        
        for i in range(1, 301):
            cursor.execute("INSERT INTO block_data (timestamp, miner, rewards) VALUES (?, ?, ?)",
                         (int(time.time()) - i*3600, "miner1" if i % 3 else "miner2", 0.5))
        
        conn.commit()
        conn.close()
        logger.info(f"Created test SQLite database at {sqlite_path}")
        return True
    except Exception as e:
        logger.error(f"Failed to create test SQLite database: {e}")
        return False

def setup_postgres_connection(args):
    """Test the PostgreSQL connection settings and create a test database.
    
    Args:
        args: Command line arguments with PostgreSQL connection settings
        
    Returns:
        bool: True if connection successful, False otherwise
    """
    try:
        import psycopg2
        
        logger.info(f"Testing PostgreSQL connection to {args.postgres_host}:{args.postgres_port}")
        
        # Connect to default postgres database
        conn = psycopg2.connect(
            host=args.postgres_host,
            port=args.postgres_port,
            user=args.postgres_user,
            password=args.postgres_password,
            database="postgres"
        )
        conn.autocommit = True
        cursor = conn.cursor()
        
        # Drop test database if it exists
        cursor.execute(f"DROP DATABASE IF EXISTS {args.postgres_dbname}")
        
        # Create test database
        cursor.execute(f"CREATE DATABASE {args.postgres_dbname}")
        logger.info(f"Created test PostgreSQL database: {args.postgres_dbname}")
        
        conn.close()
        
        # Test connection to the new database
        conn = psycopg2.connect(
            host=args.postgres_host,
            port=args.postgres_port,
            user=args.postgres_user,
            password=args.postgres_password,
            database=args.postgres_dbname
        )
        conn.close()
        
        logger.info("PostgreSQL connection test successful")
        return True
    except Exception as e:
        logger.error(f"PostgreSQL connection test failed: {e}")
        return False

def run_migration(args):
    """Run the migration script.
    
    Args:
        args: Command line arguments
        
    Returns:
        bool: True if migration successful, False otherwise
    """
    try:
        cmd = [
            sys.executable, "-m", "bettensor.validator.migration.auto_migrate",
            "--force",
            "--sqlite-path", args.sqlite_path,
            "--postgres-host", args.postgres_host,
            "--postgres-port", str(args.postgres_port),
            "--postgres-user", args.postgres_user,
            "--postgres-password", args.postgres_password,
            "--postgres-dbname", args.postgres_dbname
        ]
        
        logger.info(f"Running migration command: {' '.join(cmd)}")
        
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        
        stdout, stderr = process.communicate()
        
        if process.returncode == 0:
            logger.info("Migration completed successfully")
            logger.info(f"Output: {stdout}")
            return True
        else:
            logger.error(f"Migration failed with exit code {process.returncode}")
            logger.error(f"Error: {stderr}")
            return False
    except Exception as e:
        logger.error(f"Failed to run migration: {e}")
        return False

def verify_migration(args):
    """Verify that data was properly migrated to PostgreSQL.
    
    Args:
        args: Command line arguments
        
    Returns:
        bool: True if verification successful, False otherwise
    """
    try:
        import psycopg2
        
        # Connect to PostgreSQL
        conn = psycopg2.connect(
            host=args.postgres_host,
            port=args.postgres_port,
            user=args.postgres_user,
            password=args.postgres_password,
            database=args.postgres_dbname
        )
        cursor = conn.cursor()
        
        # Check if tables exist
        cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
        tables = cursor.fetchall()
        table_names = [table[0] for table in tables]
        
        expected_tables = ["miner_stats", "block_data"]
        missing_tables = [table for table in expected_tables if table not in table_names]
        
        if missing_tables:
            logger.error(f"Missing tables in PostgreSQL: {missing_tables}")
            return False
            
        # Check row counts
        cursor.execute("SELECT COUNT(*) FROM miner_stats")
        miner_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM block_data")
        block_count = cursor.fetchone()[0]
        
        logger.info(f"PostgreSQL data: {miner_count} miner records, {block_count} block records")
        
        # Compare with SQLite
        import sqlite3
        sqlite_conn = sqlite3.connect(args.sqlite_path)
        sqlite_cursor = sqlite_conn.cursor()
        
        sqlite_cursor.execute("SELECT COUNT(*) FROM miner_stats")
        sqlite_miner_count = sqlite_cursor.fetchone()[0]
        
        sqlite_cursor.execute("SELECT COUNT(*) FROM block_data")
        sqlite_block_count = sqlite_cursor.fetchone()[0]
        
        logger.info(f"SQLite data: {sqlite_miner_count} miner records, {sqlite_block_count} block records")
        
        sqlite_conn.close()
        
        if miner_count != sqlite_miner_count or block_count != sqlite_block_count:
            logger.error("Data count mismatch between SQLite and PostgreSQL")
            return False
            
        # Check sample data
        cursor.execute("SELECT hotkey, blocks, total_rewards FROM miner_stats LIMIT 2")
        pg_miners = cursor.fetchall()
        
        conn.close()
        
        if len(pg_miners) < 2:
            logger.error("Missing miner data in PostgreSQL")
            return False
            
        logger.info("Migration verification completed successfully")
        return True
    except Exception as e:
        logger.error(f"Migration verification failed: {e}")
        return False

def test_database_factory():
    """Test that the database factory correctly handles both database types.
    
    Returns:
        bool: True if test successful, False otherwise
    """
    try:
        from bettensor.validator.utils.database.database_factory import DatabaseFactory
        
        # Test SQLite config
        sqlite_config = {
            'type': 'sqlite',
            'path': './test_sqlite.db'
        }
        
        sqlite_manager = DatabaseFactory.create_database_manager(sqlite_config)
        if not sqlite_manager:
            logger.error("Failed to create SQLite database manager")
            return False
            
        logger.info(f"Successfully created SQLite database manager: {type(sqlite_manager).__name__}")
        
        # Test PostgreSQL config
        postgres_config = {
            'type': 'postgres',
            'host': 'localhost',
            'port': 5432,
            'user': 'postgres',
            'password': 'password',
            'dbname': 'test_db'
        }
        
        postgres_manager = DatabaseFactory.create_database_manager(postgres_config)
        if not postgres_manager:
            logger.error("Failed to create PostgreSQL database manager")
            return False
            
        logger.info(f"Successfully created PostgreSQL database manager: {type(postgres_manager).__name__}")
        
        return True
    except Exception as e:
        logger.error(f"Database factory test failed: {e}")
        return False

def main():
    parser = argparse.ArgumentParser(description="Test PostgreSQL migration")
    parser.add_argument("--sqlite-path", type=str, default="/tmp/test_migration.db",
                        help="Path to create test SQLite database")
    parser.add_argument("--postgres-host", type=str, default="localhost",
                        help="PostgreSQL host")
    parser.add_argument("--postgres-port", type=int, default=5432,
                        help="PostgreSQL port")
    parser.add_argument("--postgres-user", type=str, default="postgres",
                        help="PostgreSQL user")
    parser.add_argument("--postgres-password", type=str, default="password",
                        help="PostgreSQL password")
    parser.add_argument("--postgres-dbname", type=str, default="test_migration",
                        help="PostgreSQL database name")
    
    args = parser.parse_args()
    
    logger.info("Starting PostgreSQL migration test")
    
    # Step 1: Create test SQLite database
    logger.info("Step 1: Setting up test SQLite database")
    if not setup_sqlite_test_db(args.sqlite_path):
        logger.error("Failed to set up test SQLite database. Exiting.")
        return 1
    
    # Step 2: Test PostgreSQL connection
    logger.info("Step 2: Testing PostgreSQL connection")
    if not setup_postgres_connection(args):
        logger.error("Failed to connect to PostgreSQL. Please check your credentials and ensure PostgreSQL is running.")
        return 1
    
    # Step 3: Run migration
    logger.info("Step 3: Running migration")
    if not run_migration(args):
        logger.error("Migration failed. Check the error messages above.")
        return 1
    
    # Step 4: Verify migration
    logger.info("Step 4: Verifying migration")
    if not verify_migration(args):
        logger.error("Migration verification failed.")
        return 1
    
    # Step 5: Test database factory
    logger.info("Step 5: Testing database factory")
    if not test_database_factory():
        logger.error("Database factory test failed.")
        return 1
    
    logger.info("All tests passed! PostgreSQL migration is working correctly.")
    return 0

if __name__ == "__main__":
    sys.exit(main()) 