import os
import sys
import asyncio
import sqlite3
import traceback
import tempfile
import subprocess
import configparser
import time
from datetime import datetime
from pathlib import Path
import bittensor as bt
import psycopg2
from psycopg2.extras import execute_values
import json
import re

# Add parent directory to path so we can import our modules
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from bettensor.validator.utils.database.database_manager import DatabaseManager as SQLiteDatabaseManager
from bettensor.validator.utils.database.postgres_database_manager import PostgresDatabaseManager
from bettensor.validator.utils.database.database_factory import DatabaseFactory

class PostgresMigration:
    """
    Migration tool to transfer data from SQLite to PostgreSQL.
    Handles the entire migration process including validation and rollback.
    """
    
    def __init__(self, sqlite_path=None, postgres_config=None):
        """
        Initialize the migration tool.
        
        Args:
            sqlite_path: Path to the SQLite database file
            postgres_config: PostgreSQL connection configuration dictionary
        """
        self.sqlite_path = sqlite_path or "./bettensor/validator/state/validator.db"
        self.postgres_config = postgres_config or {
            "host": "localhost",
            "port": 5432,
            "user": "postgres",
            "password": "",
            "dbname": "bettensor_validator"
        }
        
        self.sqlite_db = None
        self.postgres_db = None
        self.migration_started = False
        self.migration_completed = False
        self.error = None
        self.progress = 0
        self.total_steps = 0
        self.current_step = 0
        self.migration_time = 0
        self.start_time = None
        self.tables_migrated = []
        self.current_table = None

    async def check_prerequisites(self):
        """
        Check if all prerequisites for migration are met.
        
        Returns:
            bool: True if all prerequisites are met, False otherwise
        """
        bt.logging.info("Checking migration prerequisites...")
        
        # Check if SQLite database exists
        if not Path(self.sqlite_path).exists():
            bt.logging.error(f"SQLite database not found at {self.sqlite_path}")
            return False
            
        # Check if SQLite database is readable
        try:
            conn = sqlite3.connect(self.sqlite_path)
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = cursor.fetchall()
            conn.close()
            
            if not tables:
                bt.logging.error("SQLite database exists but contains no tables")
                return False
                
        except Exception as e:
            bt.logging.error(f"Error reading SQLite database: {e}")
            return False
            
        # Check if PostgreSQL is installed
        try:
            result = subprocess.run(
                ['psql', '--version'], 
                stdout=subprocess.PIPE, 
                stderr=subprocess.PIPE
            )
            if result.returncode != 0:
                bt.logging.error("PostgreSQL client not installed or not in PATH")
                return False
                
        except FileNotFoundError:
            bt.logging.error("PostgreSQL client (psql) not found. Please install PostgreSQL client tools.")
            return False
            
        # Check PostgreSQL connection
        try:
            conn = psycopg2.connect(
                host=self.postgres_config["host"],
                port=self.postgres_config["port"],
                user=self.postgres_config["user"],
                password=self.postgres_config["password"],
                dbname="postgres"  # Connect to default database
            )
            conn.close()
            
        except Exception as e:
            bt.logging.error(f"Error connecting to PostgreSQL: {e}")
            return False
            
        bt.logging.info("All prerequisites for migration are met")
        return True
    
    async def create_postgres_database(self):
        """
        Create the PostgreSQL database if it doesn't exist.
        
        Returns:
            bool: True if database was created or exists, False otherwise
        """
        bt.logging.info(f"Ensuring PostgreSQL database {self.postgres_config['dbname']} exists...")
        
        try:
            # Connect to default database
            conn = psycopg2.connect(
                host=self.postgres_config["host"],
                port=self.postgres_config["port"],
                user=self.postgres_config["user"],
                password=self.postgres_config["password"],
                dbname="postgres"
            )
            conn.autocommit = True
            cursor = conn.cursor()
            
            # Check if database exists
            cursor.execute(f"SELECT 1 FROM pg_database WHERE datname = %s", 
                          (self.postgres_config["dbname"],))
            exists = cursor.fetchone()
            
            if not exists:
                bt.logging.info(f"Creating database {self.postgres_config['dbname']}")
                cursor.execute(f"CREATE DATABASE {self.postgres_config['dbname']}")
                bt.logging.info(f"Database {self.postgres_config['dbname']} created")
            else:
                bt.logging.info(f"Database {self.postgres_config['dbname']} already exists")
                
            conn.close()
            return True
            
        except Exception as e:
            bt.logging.error(f"Error creating PostgreSQL database: {e}")
            return False
    
    def _get_sqlite_tables(self, conn):
        """
        Get list of tables from SQLite database.
        
        Args:
            conn: SQLite connection
            
        Returns:
            list: List of table names
        """
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
        tables = cursor.fetchall()
        return [table[0] for table in tables]
    
    def _get_sqlite_table_schema(self, conn, table):
        """
        Get schema information for an SQLite table.
        
        Args:
            conn: SQLite connection
            table: Table name
            
        Returns:
            tuple: (column names, column types, primary key)
        """
        cursor = conn.cursor()
        cursor.execute(f"PRAGMA table_info({table})")
        info = cursor.fetchall()
        
        columns = []
        types = []
        primary_key = None
        
        for col in info:
            cid, name, type_name, notnull, default_value, pk = col
            columns.append(name)
            types.append(type_name)
            if pk == 1:
                primary_key = name
                
        return columns, types, primary_key
    
    def _map_sqlite_to_postgres_type(self, sqlite_type):
        """
        Map SQLite data type to PostgreSQL data type.
        
        Args:
            sqlite_type: SQLite data type
            
        Returns:
            str: PostgreSQL data type
        """
        sqlite_type = sqlite_type.upper()
        
        # Common type mappings
        type_map = {
            'INTEGER': 'INTEGER',
            'REAL': 'DOUBLE PRECISION',
            'TEXT': 'TEXT',
            'BLOB': 'BYTEA',
            'BOOLEAN': 'BOOLEAN',
            'DATETIME': 'TIMESTAMP',
            'DATE': 'DATE',
            'TIME': 'TIME',
            'TIMESTAMP': 'TIMESTAMP',
            'NUMERIC': 'NUMERIC',
            'FLOAT': 'DOUBLE PRECISION',
            'DOUBLE': 'DOUBLE PRECISION',
        }
        
        for sqlite_pattern, postgres_type in type_map.items():
            if sqlite_pattern in sqlite_type:
                return postgres_type
                
        # Default to TEXT for unknown types
        return 'TEXT'
    
    def _generate_postgres_create_table(self, table, columns, types, primary_key):
        """
        Generate PostgreSQL CREATE TABLE statement.
        
        Args:
            table: Table name
            columns: List of column names
            types: List of column types
            primary_key: Primary key column
            
        Returns:
            str: CREATE TABLE statement
        """
        # Map SQLite types to PostgreSQL types
        pg_types = [self._map_sqlite_to_postgres_type(t) for t in types]
        
        # Combine columns and types
        column_defs = []
        for i, col in enumerate(columns):
            pg_type = pg_types[i]
            # Add PRIMARY KEY constraint if needed
            if primary_key and col == primary_key:
                column_defs.append(f'"{col}" {pg_type} PRIMARY KEY')
            else:
                column_defs.append(f'"{col}" {pg_type}')
                
        # Build CREATE TABLE statement
        create_table = f'CREATE TABLE IF NOT EXISTS "{table}" (\n'
        create_table += ',\n'.join(column_defs)
        create_table += '\n);'
        
        return create_table
    
    def _placeholder_to_postgres(self, query):
        """
        Convert SQLite placeholders (?) to PostgreSQL placeholders (%s).
        
        Args:
            query: SQLite query
            
        Returns:
            str: PostgreSQL query
        """
        # Replace ? with %s, but only outside of quotes
        in_single_quote = False
        in_double_quote = False
        result = ""
        
        for char in query:
            if char == "'" and not in_double_quote:
                in_single_quote = not in_single_quote
            elif char == '"' and not in_single_quote:
                in_double_quote = not in_double_quote
            elif char == '?' and not in_single_quote and not in_double_quote:
                result += '%s'
                continue
                
            result += char
            
        return result
    
    def _convert_sqlite_to_postgres_functions(self, query):
        """
        Convert SQLite functions to PostgreSQL functions.
        
        Args:
            query: SQLite query
            
        Returns:
            str: PostgreSQL query
        """
        # Replace AUTOINCREMENT with SERIAL
        query = re.sub(r'INTEGER\s+PRIMARY\s+KEY\s+AUTOINCREMENT', 'SERIAL PRIMARY KEY', query, flags=re.IGNORECASE)
        
        # Replace IFNULL with COALESCE
        query = re.sub(r'IFNULL\(([^,]+),([^)]+)\)', r'COALESCE(\1,\2)', query, flags=re.IGNORECASE)
        
        # Replace SQLite datetime functions with PostgreSQL equivalents
        query = re.sub(r'datetime\(([^)]+)\)', r'to_timestamp(\1)', query, flags=re.IGNORECASE)
        
        # Replace date('now') with NOW()
        query = re.sub(r"date\('now'\)", r"CURRENT_DATE", query, flags=re.IGNORECASE)
        
        # Replace datetime('now') with NOW()
        query = re.sub(r"datetime\('now'\)", r"NOW()", query, flags=re.IGNORECASE)
        
        # Replace strftime with to_char
        query = re.sub(r"strftime\('([^']+)',\s*([^)]+)\)", r"to_char(\2, '\1')", query, flags=re.IGNORECASE)
        
        return query
    
    async def extract_data_from_sqlite(self):
        """
        Extract all data from SQLite database.
        
        Returns:
            dict: Dictionary mapping table names to their data
        """
        bt.logging.info("Extracting data from SQLite database...")
        
        try:
            conn = sqlite3.connect(self.sqlite_path)
            conn.row_factory = sqlite3.Row
            
            tables = self._get_sqlite_tables(conn)
            result = {}
            
            total_tables = len(tables)
            self.total_steps = total_tables * 2  # Extract and load for each table
            
            for i, table in enumerate(tables):
                self.current_step = i
                self.current_table = table
                self.progress = (i / self.total_steps) * 100
                
                bt.logging.info(f"Extracting data from table '{table}' ({i+1}/{total_tables})")
                
                # Get table schema
                columns, types, primary_key = self._get_sqlite_table_schema(conn, table)
                
                # Get table data
                cursor = conn.cursor()
                cursor.execute(f'SELECT * FROM "{table}"')
                rows = cursor.fetchall()
                
                data = []
                for row in rows:
                    # Convert row to dictionary
                    row_dict = dict(row)
                    data.append(row_dict)
                
                result[table] = {
                    'columns': columns,
                    'types': types,
                    'primary_key': primary_key,
                    'data': data
                }
                
                bt.logging.info(f"Extracted {len(data)} rows from table '{table}'")
                
            conn.close()
            return result
            
        except Exception as e:
            bt.logging.error(f"Error extracting data from SQLite: {e}")
            bt.logging.error(traceback.format_exc())
            raise
    
    async def transform_data_for_postgres(self, tables_data):
        """
        Transform data extracted from SQLite for PostgreSQL import.
        
        Args:
            tables_data: Dictionary of table data from extract_data_from_sqlite
            
        Returns:
            dict: Transformed data ready for PostgreSQL import
        """
        bt.logging.info("Transforming data for PostgreSQL...")
        
        transformed_data = {}
        
        for table, table_info in tables_data.items():
            bt.logging.info(f"Transforming data for table '{table}'")
            
            columns = table_info['columns']
            data = table_info['data']
            
            # Transform data types as needed
            transformed_rows = []
            for row in data:
                transformed_row = {}
                for column in columns:
                    value = row[column]
                    
                    # Handle specific type conversions
                    if value is not None:
                        # Convert INTEGER (1/0) to BOOLEAN
                        if isinstance(value, int) and (value == 0 or value == 1):
                            # Keep as is, PostgreSQL accepts 0/1 for booleans
                            transformed_row[column] = value
                        else:
                            transformed_row[column] = value
                    else:
                        transformed_row[column] = None
                        
                transformed_rows.append(transformed_row)
                
            transformed_data[table] = {
                'columns': columns,
                'types': table_info['types'],
                'primary_key': table_info['primary_key'],
                'data': transformed_rows
            }
            
        return transformed_data
    
    async def load_data_into_postgres(self, tables_data):
        """
        Load transformed data into PostgreSQL.
        
        Args:
            tables_data: Dictionary of transformed table data
            
        Returns:
            bool: True if successful, False otherwise
        """
        bt.logging.info("Loading data into PostgreSQL...")
        
        try:
            # Initialize PostgreSQL database manager and create tables
            postgres_db = PostgresDatabaseManager(
                host=self.postgres_config["host"],
                port=self.postgres_config["port"],
                user=self.postgres_config["user"],
                password=self.postgres_config["password"],
                dbname=self.postgres_config["dbname"]
            )
            
            # Initialize database schema
            await postgres_db.initialize()
            
            # Connect directly for bulk loading
            conn = psycopg2.connect(
                host=self.postgres_config["host"],
                port=self.postgres_config["port"],
                user=self.postgres_config["user"],
                password=self.postgres_config["password"],
                dbname=self.postgres_config["dbname"]
            )
            conn.autocommit = False
            cursor = conn.cursor()
            
            total_tables = len(tables_data)
            for i, (table, table_info) in enumerate(tables_data.items()):
                self.current_step = i + len(tables_data)  # Continue from extract steps
                self.current_table = table
                self.progress = (self.current_step / self.total_steps) * 100
                
                columns = table_info['columns']
                data = table_info['data']
                
                bt.logging.info(f"Loading data into table '{table}' ({i+1}/{total_tables})")
                
                if not data:
                    bt.logging.info(f"Table '{table}' has no data to import")
                    continue
                    
                # Prepare column list for INSERT statement
                columns_str = ', '.join([f'"{col}"' for col in columns])
                
                # Clear existing data from table
                cursor.execute(f'TRUNCATE TABLE "{table}" CASCADE')
                
                # Prepare data for batch insert
                rows_to_insert = []
                for row in data:
                    row_values = [row[col] for col in columns]
                    rows_to_insert.append(row_values)
                
                # Use execute_values for efficient batch insertion
                if rows_to_insert:
                    placeholders = ', '.join(['%s'] * len(columns))
                    insert_query = f'INSERT INTO "{table}" ({columns_str}) VALUES ({placeholders})'
                    
                    # For large datasets, insert in batches of 1000
                    batch_size = 1000
                    for i in range(0, len(rows_to_insert), batch_size):
                        batch = rows_to_insert[i:i+batch_size]
                        cursor.executemany(insert_query, batch)
                        
                    # Commit after each table
                    conn.commit()
                    
                bt.logging.info(f"Loaded {len(data)} rows into table '{table}'")
                self.tables_migrated.append(table)
                
            conn.close()
            await postgres_db.cleanup()
            
            return True
            
        except Exception as e:
            bt.logging.error(f"Error loading data into PostgreSQL: {e}")
            bt.logging.error(traceback.format_exc())
            return False
    
    async def verify_data_integrity(self):
        """
        Verify data integrity after migration by comparing row counts.
        
        Returns:
            bool: True if verification passes, False otherwise
        """
        bt.logging.info("Verifying data integrity...")
        
        try:
            # Connect to SQLite
            sqlite_conn = sqlite3.connect(self.sqlite_path)
            sqlite_cursor = sqlite_conn.cursor()
            
            # Connect to PostgreSQL
            pg_conn = psycopg2.connect(
                host=self.postgres_config["host"],
                port=self.postgres_config["port"],
                user=self.postgres_config["user"],
                password=self.postgres_config["password"],
                dbname=self.postgres_config["dbname"]
            )
            pg_cursor = pg_conn.cursor()
            
            # Get SQLite tables
            tables = self._get_sqlite_tables(sqlite_conn)
            
            # Check row counts for each table
            all_match = True
            for table in tables:
                # Get SQLite row count
                sqlite_cursor.execute(f'SELECT COUNT(*) FROM "{table}"')
                sqlite_count = sqlite_cursor.fetchone()[0]
                
                # Get PostgreSQL row count
                pg_cursor.execute(f'SELECT COUNT(*) FROM "{table}"')
                pg_count = pg_cursor.fetchone()[0]
                
                if sqlite_count == pg_count:
                    bt.logging.info(f"Table '{table}': Row counts match ({sqlite_count})")
                else:
                    bt.logging.error(f"Table '{table}': Row counts don't match! SQLite: {sqlite_count}, PostgreSQL: {pg_count}")
                    all_match = False
                    
            sqlite_conn.close()
            pg_conn.close()
            
            if all_match:
                bt.logging.info("Data integrity verification passed")
            else:
                bt.logging.error("Data integrity verification failed")
                
            return all_match
            
        except Exception as e:
            bt.logging.error(f"Error verifying data integrity: {e}")
            bt.logging.error(traceback.format_exc())
            return False
    
    async def update_config(self, db_type="postgres"):
        """
        Update configuration to use the specified database.
        
        Args:
            db_type: Database type to use ('sqlite' or 'postgres')
            
        Returns:
            bool: True if config was updated successfully, False otherwise
        """
        try:
            # Save configuration using DatabaseFactory
            config = {
                "type": db_type,
                "host": self.postgres_config["host"],
                "port": self.postgres_config["port"],
                "user": self.postgres_config["user"],
                "password": self.postgres_config["password"],
                "dbname": self.postgres_config["dbname"],
                "path": self.sqlite_path
            }
            
            DatabaseFactory.save_config(config)
            
            bt.logging.info(f"Updated configuration to use {db_type} database")
            return True
            
        except Exception as e:
            bt.logging.error(f"Error updating configuration: {e}")
            return False
    
    async def create_postgres_backup(self):
        """
        Create a PostgreSQL backup for state sync.
        
        Returns:
            bool: True if backup was created successfully, False otherwise
        """
        try:
            # Initialize PostgreSQL database manager
            postgres_db = PostgresDatabaseManager(
                host=self.postgres_config["host"],
                port=self.postgres_config["port"],
                user=self.postgres_config["user"],
                password=self.postgres_config["password"],
                dbname=self.postgres_config["dbname"]
            )
            
            # Create backup
            backup_path = Path("./bettensor/validator/state/postgres_backup.dump")
            success = await postgres_db.create_verified_backup(backup_path)
            
            await postgres_db.cleanup()
            
            if success:
                bt.logging.info(f"Created PostgreSQL backup at {backup_path}")
            else:
                bt.logging.error("Failed to create PostgreSQL backup")
                
            return success
            
        except Exception as e:
            bt.logging.error(f"Error creating PostgreSQL backup: {e}")
            bt.logging.error(traceback.format_exc())
            return False
    
    async def rollback_migration(self):
        """
        Rollback the migration if it fails.
        
        Returns:
            bool: True if rollback was successful, False otherwise
        """
        bt.logging.warning("Rolling back migration...")
        
        try:
            # Update config to use SQLite
            await self.update_config(db_type="sqlite")
            
            # Clean up PostgreSQL resources
            if self.postgres_db:
                await self.postgres_db.cleanup()
                
            bt.logging.info("Migration rolled back successfully")
            return True
            
        except Exception as e:
            bt.logging.error(f"Error rolling back migration: {e}")
            bt.logging.error(traceback.format_exc())
            return False
    
    async def migrate(self):
        """
        Execute the full migration process.
        
        Returns:
            bool: True if migration was successful, False otherwise
        """
        self.start_time = time.time()
        self.migration_started = True
        
        try:
            bt.logging.info("Starting PostgreSQL migration...")
            
            # Check prerequisites
            if not await self.check_prerequisites():
                raise RuntimeError("Migration prerequisites not met")
                
            # Create PostgreSQL database
            if not await self.create_postgres_database():
                raise RuntimeError("Failed to create PostgreSQL database")
                
            # Extract data from SQLite
            tables_data = await self.extract_data_from_sqlite()
            
            # Transform data
            transformed_data = await self.transform_data_for_postgres(tables_data)
            
            # Load data into PostgreSQL
            if not await self.load_data_into_postgres(transformed_data):
                raise RuntimeError("Failed to load data into PostgreSQL")
                
            # Verify data integrity
            if not await self.verify_data_integrity():
                raise RuntimeError("Data integrity verification failed")
                
            # Update configuration to use PostgreSQL
            if not await self.update_config(db_type="postgres"):
                raise RuntimeError("Failed to update configuration")
                
            # Create initial PostgreSQL backup
            if not await self.create_postgres_backup():
                bt.logging.warning("Failed to create initial PostgreSQL backup, continuing anyway")
                
            self.migration_completed = True
            self.migration_time = time.time() - self.start_time
            
            bt.logging.info(f"Migration completed successfully in {self.migration_time:.2f} seconds")
            return True
            
        except Exception as e:
            self.error = str(e)
            bt.logging.error(f"Migration failed: {e}")
            bt.logging.error(traceback.format_exc())
            
            # Attempt rollback
            await self.rollback_migration()
            
            self.migration_time = time.time() - self.start_time
            return False
    
    def get_status(self):
        """
        Get the current migration status.
        
        Returns:
            dict: Migration status information
        """
        return {
            "started": self.migration_started,
            "completed": self.migration_completed,
            "error": self.error,
            "progress": self.progress,
            "current_step": self.current_step,
            "total_steps": self.total_steps,
            "current_table": self.current_table,
            "tables_migrated": self.tables_migrated,
            "migration_time": self.migration_time
        }


async def main():
    """Main function to run migration from command line."""
    import argparse
    
    parser = argparse.ArgumentParser(description="PostgreSQL Migration Tool")
    parser.add_argument("--sqlite-path", help="Path to SQLite database file")
    parser.add_argument("--host", default="localhost", help="PostgreSQL host")
    parser.add_argument("--port", type=int, default=5432, help="PostgreSQL port")
    parser.add_argument("--user", default="postgres", help="PostgreSQL user")
    parser.add_argument("--password", default="", help="PostgreSQL password")
    parser.add_argument("--dbname", default="bettensor_validator", help="PostgreSQL database name")
    
    args = parser.parse_args()
    
    postgres_config = {
        "host": args.host,
        "port": args.port,
        "user": args.user,
        "password": args.password,
        "dbname": args.dbname
    }
    
    migration = PostgresMigration(
        sqlite_path=args.sqlite_path,
        postgres_config=postgres_config
    )
    
    success = await migration.migrate()
    
    if success:
        print("Migration completed successfully")
        sys.exit(0)
    else:
        print(f"Migration failed: {migration.error}")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main()) 