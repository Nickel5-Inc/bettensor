# The MIT License (MIT)
# Copyright © 2023 Bettensor
# See license at: https://github.com/bettensor/bettensor/blob/main/LICENSE

"""
Integration test for PostgreSQL with Bettensor Validator.
This script tests the integration of PostgreSQL with the validator's core systems.
"""

import os
import sys
import logging
import argparse
import tempfile
import shutil
import asyncio
import random
import time

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("postgres_integration_test")

async def test_postgres_database_manager(args):
    """Test the PostgreSQL database manager functionality.
    
    Args:
        args: Command line arguments
    
    Returns:
        bool: True if test passed, False otherwise
    """
    try:
        # Import after function call to avoid import errors
        from bettensor.validator.utils.database.postgres_manager import PostgresDatabaseManager
        
        # Create PostgreSQL manager
        postgres_manager = PostgresDatabaseManager(
            host=args.postgres_host,
            port=args.postgres_port,
            user=args.postgres_user,
            password=args.postgres_password,
            dbname=args.postgres_dbname
        )
        
        # Create test tables
        await postgres_manager.execute("""
        CREATE TABLE IF NOT EXISTS test_miners (
            id SERIAL PRIMARY KEY,
            hotkey TEXT NOT NULL,
            blocks INTEGER DEFAULT 0,
            total_rewards REAL DEFAULT 0.0
        )
        """)
        
        await postgres_manager.execute("""
        CREATE TABLE IF NOT EXISTS test_blocks (
            id SERIAL PRIMARY KEY,
            timestamp INTEGER NOT NULL,
            miner TEXT NOT NULL,
            rewards REAL DEFAULT 0.0
        )
        """)
        
        logger.info("Created test tables")
        
        # Insert test data
        test_miners = [
            ("miner1", 100, 50.5),
            ("miner2", 200, 100.25),
            ("miner3", 150, 75.75)
        ]
        
        for miner in test_miners:
            await postgres_manager.execute(
                "INSERT INTO test_miners (hotkey, blocks, total_rewards) VALUES ($1, $2, $3)",
                miner
            )
        
        test_blocks = [
            (int(time.time()) - i*3600, f"miner{(i % 3) + 1}", 0.5)
            for i in range(10)
        ]
        
        for block in test_blocks:
            await postgres_manager.execute(
                "INSERT INTO test_blocks (timestamp, miner, rewards) VALUES ($1, $2, $3)",
                block
            )
        
        logger.info("Inserted test data")
        
        # Query data
        miners = await postgres_manager.fetchall("SELECT * FROM test_miners")
        blocks = await postgres_manager.fetchall("SELECT * FROM test_blocks")
        
        if len(miners) != 3:
            logger.error(f"Expected 3 miners, got {len(miners)}")
            return False
        
        if len(blocks) != 10:
            logger.error(f"Expected 10 blocks, got {len(blocks)}")
            return False
        
        logger.info("Query test successful")
        
        # Test transaction
        async with postgres_manager.transaction():
            await postgres_manager.execute("UPDATE test_miners SET blocks = blocks + 1 WHERE hotkey = 'miner1'")
            await postgres_manager.execute(
                "INSERT INTO test_blocks (timestamp, miner, rewards) VALUES ($1, $2, $3)",
                (int(time.time()), "miner1", 0.75)
            )
        
        # Verify transaction
        miner1 = await postgres_manager.fetchone(
            "SELECT blocks FROM test_miners WHERE hotkey = 'miner1'"
        )
        if miner1[0] != 101:
            logger.error(f"Expected miner1 to have 101 blocks, got {miner1[0]}")
            return False
        
        block_count = await postgres_manager.fetchone("SELECT COUNT(*) FROM test_blocks")
        if block_count[0] != 11:
            logger.error(f"Expected 11 blocks, got {block_count[0]}")
            return False
        
        logger.info("Transaction test successful")
        
        # Test backup and restore
        backup_file = tempfile.mktemp(suffix=".sql")
        await postgres_manager.backup_database(backup_file)
        logger.info(f"Created backup at {backup_file}")
        
        # Drop tables
        await postgres_manager.execute("DROP TABLE test_miners")
        await postgres_manager.execute("DROP TABLE test_blocks")
        
        # Restore database
        await postgres_manager.restore_database(backup_file)
        logger.info("Restored database from backup")
        
        # Verify restore
        restored_miners = await postgres_manager.fetchall("SELECT * FROM test_miners")
        restored_blocks = await postgres_manager.fetchall("SELECT * FROM test_blocks")
        
        if len(restored_miners) != 3:
            logger.error(f"After restore: Expected 3 miners, got {len(restored_miners)}")
            return False
        
        if len(restored_blocks) != 11:
            logger.error(f"After restore: Expected 11 blocks, got {len(restored_blocks)}")
            return False
        
        logger.info("Restore test successful")
        
        # Cleanup
        await postgres_manager.execute("DROP TABLE test_miners")
        await postgres_manager.execute("DROP TABLE test_blocks")
        await postgres_manager.close()
        
        if os.path.exists(backup_file):
            os.remove(backup_file)
        
        logger.info("PostgreSQL database manager test passed")
        return True
    
    except Exception as e:
        logger.error(f"PostgreSQL database manager test failed: {e}")
        return False

async def test_database_factory(args):
    """Test the database factory.
    
    Args:
        args: Command line arguments
    
    Returns:
        bool: True if test passed, False otherwise
    """
    try:
        # Import after function call to avoid import errors
        from bettensor.validator.utils.database.database_factory import DatabaseFactory
        
        # Create temp directory
        temp_dir = tempfile.mkdtemp(prefix="bettensor_test_")
        
        try:
            # Create SQLite config
            sqlite_config = {
                "type": "sqlite",
                "path": os.path.join(temp_dir, "test.db")
            }
            
            # Create PostgreSQL config
            postgres_config = {
                "type": "postgres",
                "host": args.postgres_host,
                "port": args.postgres_port,
                "user": args.postgres_user,
                "password": args.postgres_password,
                "dbname": args.postgres_dbname
            }
            
            # Test config saving and loading
            config_file = os.path.join(temp_dir, "database.cfg")
            
            # Save and load SQLite config
            DatabaseFactory.save_config(sqlite_config, config_file)
            loaded_sqlite_config = DatabaseFactory.load_config(config_file)
            
            if loaded_sqlite_config["type"] != "sqlite" or loaded_sqlite_config["path"] != sqlite_config["path"]:
                logger.error("SQLite config load/save mismatch")
                return False
            
            # Save and load PostgreSQL config
            DatabaseFactory.save_config(postgres_config, config_file)
            loaded_postgres_config = DatabaseFactory.load_config(config_file)
            
            if (loaded_postgres_config["type"] != "postgres" or 
                loaded_postgres_config["host"] != postgres_config["host"] or
                loaded_postgres_config["port"] != postgres_config["port"]):
                logger.error("PostgreSQL config load/save mismatch")
                return False
            
            # Test manager creation
            sqlite_manager = DatabaseFactory.create_database_manager(sqlite_config)
            postgres_manager = DatabaseFactory.create_database_manager(postgres_config)
            
            logger.info(f"Created SQLite manager: {type(sqlite_manager).__name__}")
            logger.info(f"Created PostgreSQL manager: {type(postgres_manager).__name__}")
            
            # Clean up
            await sqlite_manager.close()
            await postgres_manager.close()
            
            logger.info("Database factory test passed")
            return True
            
        finally:
            # Clean up temp directory
            shutil.rmtree(temp_dir)
    
    except Exception as e:
        logger.error(f"Database factory test failed: {e}")
        return False

async def test_migration_system(args):
    """Test the migration system.
    
    Args:
        args: Command line arguments
    
    Returns:
        bool: True if test passed, False otherwise
    """
    try:
        # Import after function call to avoid import errors
        from bettensor.validator.migration.postgres_migration import PostgresMigration
        import sqlite3
        
        # Create temp directory
        temp_dir = tempfile.mkdtemp(prefix="bettensor_migration_test_")
        
        try:
            # Create SQLite database
            sqlite_path = os.path.join(temp_dir, "test.db")
            conn = sqlite3.connect(sqlite_path)
            cursor = conn.cursor()
            
            # Create test tables
            cursor.execute("""
            CREATE TABLE test_miners (
                id INTEGER PRIMARY KEY,
                hotkey TEXT NOT NULL,
                blocks INTEGER DEFAULT 0,
                total_rewards REAL DEFAULT 0.0
            )
            """)
            
            cursor.execute("""
            CREATE TABLE test_blocks (
                id INTEGER PRIMARY KEY,
                timestamp INTEGER NOT NULL,
                miner TEXT NOT NULL,
                rewards REAL DEFAULT 0.0
            )
            """)
            
            # Insert test data
            test_miners = [
                ("miner1", 100, 50.5),
                ("miner2", 200, 100.25),
                ("miner3", 150, 75.75)
            ]
            
            for miner in test_miners:
                cursor.execute(
                    "INSERT INTO test_miners (hotkey, blocks, total_rewards) VALUES (?, ?, ?)",
                    miner
                )
            
            test_blocks = [
                (int(time.time()) - i*3600, f"miner{(i % 3) + 1}", 0.5)
                for i in range(10)
            ]
            
            for block in test_blocks:
                cursor.execute(
                    "INSERT INTO test_blocks (timestamp, miner, rewards) VALUES (?, ?, ?)",
                    block
                )
            
            conn.commit()
            conn.close()
            
            logger.info(f"Created test SQLite database at {sqlite_path}")
            
            # Create migration instance
            migrator = PostgresMigration(
                sqlite_path=sqlite_path,
                postgres_host=args.postgres_host,
                postgres_port=args.postgres_port,
                postgres_user=args.postgres_user,
                postgres_password=args.postgres_password,
                postgres_dbname=args.postgres_dbname
            )
            
            # Perform migration
            await migrator.migrate()
            logger.info("Migration completed")
            
            # Verify migration
            await migrator.verify_migration()
            logger.info("Migration verification passed")
            
            # Drop tables
            postgres_manager = migrator.postgres_manager
            await postgres_manager.execute("DROP TABLE test_miners")
            await postgres_manager.execute("DROP TABLE test_blocks")
            await postgres_manager.close()
            
            logger.info("Migration system test passed")
            return True
            
        finally:
            # Clean up temp directory
            shutil.rmtree(temp_dir)
    
    except Exception as e:
        logger.error(f"Migration system test failed: {e}")
        return False

async def main_async():
    """Main async function."""
    parser = argparse.ArgumentParser(description="PostgreSQL integration test")
    parser.add_argument("--postgres-host", type=str, default="localhost",
                        help="PostgreSQL host")
    parser.add_argument("--postgres-port", type=int, default=5432,
                        help="PostgreSQL port")
    parser.add_argument("--postgres-user", type=str, default="postgres",
                        help="PostgreSQL user")
    parser.add_argument("--postgres-password", type=str, default="password",
                        help="PostgreSQL password")
    parser.add_argument("--postgres-dbname", type=str, default="test_integration",
                        help="PostgreSQL database name")
    
    args = parser.parse_args()
    
    # Drop database if exists (for clean test)
    try:
        import psycopg2
        conn = psycopg2.connect(
            host=args.postgres_host,
            port=args.postgres_port,
            user=args.postgres_user,
            password=args.postgres_password,
            database="postgres"  # Connect to default database
        )
        conn.autocommit = True
        cursor = conn.cursor()
        cursor.execute(f"DROP DATABASE IF EXISTS {args.postgres_dbname}")
        cursor.execute(f"CREATE DATABASE {args.postgres_dbname}")
        conn.close()
        logger.info(f"Created clean database {args.postgres_dbname}")
    except Exception as e:
        logger.error(f"Failed to create clean database: {e}")
        return 1
    
    # Run tests
    tests_passed = 0
    tests_failed = 0
    
    # Test PostgreSQL database manager
    logger.info("Testing PostgreSQL database manager")
    if await test_postgres_database_manager(args):
        tests_passed += 1
    else:
        tests_failed += 1
    
    # Test database factory
    logger.info("Testing database factory")
    if await test_database_factory(args):
        tests_passed += 1
    else:
        tests_failed += 1
    
    # Test migration system
    logger.info("Testing migration system")
    if await test_migration_system(args):
        tests_passed += 1
    else:
        tests_failed += 1
    
    # Print results
    logger.info(f"Tests completed: {tests_passed} passed, {tests_failed} failed")
    
    return 0 if tests_failed == 0 else 1

def main():
    """Entry point for the script."""
    try:
        return asyncio.run(main_async())
    except KeyboardInterrupt:
        logger.info("Test interrupted by user")
        return 130
    except Exception as e:
        logger.error(f"Unhandled exception: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main()) 
#!/usr/bin/env python3
 