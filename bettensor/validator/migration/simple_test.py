#!/usr/bin/env python3
# The MIT License (MIT)
# Copyright © 2023 Bettensor
# See license at: https://github.com/bettensor/bettensor/blob/main/LICENSE

"""
Simple test script for PostgreSQL integration with Bettensor Validator.
"""

import os
import sys
import argparse
import logging
import tempfile
import asyncio

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("postgres_test")

async def test_postgres_connection(args):
    """Test basic PostgreSQL connection"""
    try:
        import psycopg2
        
        # Test connection
        logger.info(f"Testing connection to PostgreSQL: {args.postgres_host}:{args.postgres_port}")
        conn = psycopg2.connect(
            host=args.postgres_host,
            port=args.postgres_port,
            user=args.postgres_user,
            password=args.postgres_password,
            database="postgres"  # Connect to default database
        )
        conn.autocommit = True
        cursor = conn.cursor()
        
        # Create test database
        try:
            cursor.execute(f"DROP DATABASE IF EXISTS {args.postgres_dbname}")
            cursor.execute(f"CREATE DATABASE {args.postgres_dbname}")
            logger.info(f"Created test database: {args.postgres_dbname}")
        except Exception as e:
            logger.error(f"Failed to create test database: {e}")
            return False
        finally:
            conn.close()
        
        # Connect to test database
        logger.info(f"Connecting to test database: {args.postgres_dbname}")
        conn = psycopg2.connect(
            host=args.postgres_host,
            port=args.postgres_port,
            user=args.postgres_user,
            password=args.postgres_password,
            database=args.postgres_dbname
        )
        cursor = conn.cursor()
        
        # Create test table
        cursor.execute("""
        CREATE TABLE test_table (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL
        )
        """)
        
        # Insert test data
        cursor.execute("INSERT INTO test_table (name) VALUES (%s)", ("test1",))
        cursor.execute("INSERT INTO test_table (name) VALUES (%s)", ("test2",))
        conn.commit()
        
        # Query data
        cursor.execute("SELECT * FROM test_table")
        rows = cursor.fetchall()
        logger.info(f"Query result: {rows}")
        
        if len(rows) != 2:
            logger.error(f"Expected 2 rows, got {len(rows)}")
            return False
        
        # Clean up
        cursor.execute("DROP TABLE test_table")
        conn.commit()
        conn.close()
        
        logger.info("PostgreSQL connection test passed")
        return True
    except Exception as e:
        logger.error(f"PostgreSQL connection test failed: {e}")
        return False

async def test_postgres_manager(args):
    """Test PostgreSQL database manager"""
    try:
        # Import the PostgreSQL manager
        from bettensor.validator.utils.database.postgres_manager import PostgresDatabaseManager
        
        # Create manager
        logger.info("Creating PostgresDatabaseManager")
        manager = PostgresDatabaseManager(
            host=args.postgres_host,
            port=args.postgres_port,
            user=args.postgres_user,
            password=args.postgres_password,
            dbname=args.postgres_dbname
        )
        
        # Create test table
        await manager.execute("""
        CREATE TABLE test_manager_table (
            id SERIAL PRIMARY KEY,
            value TEXT NOT NULL
        )
        """)
        
        # Insert data
        await manager.execute("INSERT INTO test_manager_table (value) VALUES ($1)", ("value1",))
        await manager.execute("INSERT INTO test_manager_table (value) VALUES ($1)", ("value2",))
        
        # Query data
        rows = await manager.fetchall("SELECT * FROM test_manager_table")
        logger.info(f"Query result: {rows}")
        
        if len(rows) != 2:
            logger.error(f"Expected 2 rows, got {len(rows)}")
            return False
        
        # Test transaction
        async with manager.transaction():
            await manager.execute("INSERT INTO test_manager_table (value) VALUES ($1)", ("value3",))
            await manager.execute("UPDATE test_manager_table SET value = 'updated' WHERE id = 1")
        
        # Verify transaction
        rows = await manager.fetchall("SELECT * FROM test_manager_table ORDER BY id")
        logger.info(f"After transaction: {rows}")
        
        if len(rows) != 3 or rows[0][1] != "updated":
            logger.error("Transaction test failed")
            return False
        
        # Test backup and restore
        backup_file = tempfile.mktemp(suffix=".sql")
        await manager.backup_database(backup_file)
        logger.info(f"Created backup at {backup_file}")
        
        # Drop table
        await manager.execute("DROP TABLE test_manager_table")
        
        # Restore database
        await manager.restore_database(backup_file)
        logger.info("Restored database from backup")
        
        # Verify restore
        rows = await manager.fetchall("SELECT * FROM test_manager_table ORDER BY id")
        logger.info(f"After restore: {rows}")
        
        if len(rows) != 3 or rows[0][1] != "updated":
            logger.error("Restore test failed")
            return False
        
        # Clean up
        await manager.execute("DROP TABLE test_manager_table")
        await manager.close()
        
        if os.path.exists(backup_file):
            os.remove(backup_file)
        
        logger.info("PostgreSQL manager test passed")
        return True
    except Exception as e:
        logger.error(f"PostgreSQL manager test failed: {e}")
        return False

async def test_database_factory(args):
    """Test database factory"""
    try:
        # Import database factory
        from bettensor.validator.utils.database.database_factory import DatabaseFactory
        
        # Test create SQLite manager
        sqlite_config = {
            "type": "sqlite",
            "path": ":memory:"
        }
        
        sqlite_manager = DatabaseFactory.create_database_manager(sqlite_config)
        logger.info(f"Created SQLite manager: {type(sqlite_manager).__name__}")
        
        # Test create PostgreSQL manager
        postgres_config = {
            "type": "postgres",
            "host": args.postgres_host,
            "port": args.postgres_port,
            "user": args.postgres_user,
            "password": args.postgres_password,
            "dbname": args.postgres_dbname
        }
        
        postgres_manager = DatabaseFactory.create_database_manager(postgres_config)
        logger.info(f"Created PostgreSQL manager: {type(postgres_manager).__name__}")
        
        # Test config save/load
        config_file = tempfile.mktemp(suffix=".cfg")
        
        DatabaseFactory.save_config(postgres_config, config_file)
        logger.info(f"Saved config to {config_file}")
        
        loaded_config = DatabaseFactory.load_config(config_file)
        logger.info(f"Loaded config: {loaded_config}")
        
        if loaded_config["type"] != "postgres" or loaded_config["host"] != args.postgres_host:
            logger.error("Config load/save test failed")
            return False
        
        # Clean up
        await sqlite_manager.close()
        await postgres_manager.close()
        
        if os.path.exists(config_file):
            os.remove(config_file)
        
        logger.info("Database factory test passed")
        return True
    except Exception as e:
        logger.error(f"Database factory test failed: {e}")
        return False

async def run_tests(args):
    """Run all tests"""
    tests_passed = 0
    tests_failed = 0
    
    # Test PostgreSQL connection
    logger.info("=== Testing PostgreSQL connection ===")
    if await test_postgres_connection(args):
        tests_passed += 1
    else:
        tests_failed += 1
    
    # Test PostgreSQL manager
    logger.info("=== Testing PostgreSQL manager ===")
    if await test_postgres_manager(args):
        tests_passed += 1
    else:
        tests_failed += 1
    
    # Test database factory
    logger.info("=== Testing database factory ===")
    if await test_database_factory(args):
        tests_passed += 1
    else:
        tests_failed += 1
    
    # Print results
    logger.info(f"=== Test results: {tests_passed} passed, {tests_failed} failed ===")
    
    return tests_failed == 0

def main():
    """Main function"""
    parser = argparse.ArgumentParser(description="Test PostgreSQL integration")
    parser.add_argument("--postgres-host", type=str, default="localhost",
                        help="PostgreSQL host")
    parser.add_argument("--postgres-port", type=int, default=5432,
                        help="PostgreSQL port")
    parser.add_argument("--postgres-user", type=str, default="postgres",
                        help="PostgreSQL user")
    parser.add_argument("--postgres-password", type=str, default="password",
                        help="PostgreSQL password")
    parser.add_argument("--postgres-dbname", type=str, default="bettensor_test",
                        help="PostgreSQL database name")
    
    args = parser.parse_args()
    
    # Run tests
    try:
        success = asyncio.run(run_tests(args))
        return 0 if success else 1
    except KeyboardInterrupt:
        logger.info("Test interrupted by user")
        return 130
    except Exception as e:
        logger.error(f"Unhandled exception: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main()) 