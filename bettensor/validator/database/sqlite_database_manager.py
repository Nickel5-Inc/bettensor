import os
import sqlite3
import shutil
import asyncio
import bittensor as bt
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
from sqlalchemy import text
import fcntl
import time
import threading
import weakref

class SQLiteDatabaseLock:
    """
    File-based locking mechanism for SQLite databases to prevent concurrent access.
    Uses advisory file locking via fcntl.
    """
    _locks = {}
    _lock = threading.Lock()
    
    @classmethod
    def acquire(cls, db_path):
        """
        Acquire a lock for the specified database file.
        
        Args:
            db_path: Path to the SQLite database
            
        Returns:
            Lock object if successful
        """
        with cls._lock:
            if db_path in cls._locks:
                # Reuse existing lock for this path
                lock = cls._locks[db_path]
                lock.ref_count += 1
                return lock
            
            # Create new lock for this path
            lock = SQLiteDatabaseLock(db_path)
            cls._locks[db_path] = lock
            return lock
    
    def __init__(self, db_path):
        """
        Initialize the lock for a specific database file.
        
        Args:
            db_path: Path to the SQLite database file
        """
        self.db_path = db_path
        self.lock_path = f"{db_path}.lock"
        self.ref_count = 1
        self.lock_file = None
        self.acquired = False
        
        # Create lock file if it doesn't exist
        Path(self.lock_path).touch(exist_ok=True)
        
    def __enter__(self):
        """Context manager entry point - acquire lock"""
        self.acquire_lock()
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit point - release lock"""
        self.release_lock()
    
    def acquire_lock(self, timeout=30, retry_interval=0.1):
        """
        Acquire the lock with timeout and retry.
        
        Args:
            timeout: Maximum time to wait for lock in seconds
            retry_interval: Time between retries in seconds
            
        Returns:
            bool: True if successful, False otherwise
            
        Raises:
            TimeoutError: If lock cannot be acquired within timeout
        """
        if self.acquired:
            return True
            
        start_time = time.time()
        
        # Open the lock file
        self.lock_file = open(self.lock_path, 'w')
        
        # Try to acquire exclusive lock with timeout
        while True:
            try:
                fcntl.flock(self.lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                self.acquired = True
                bt.logging.debug(f"Acquired lock for {self.db_path}")
                return True
            except IOError:
                # Check if we've exceeded timeout
                if time.time() - start_time > timeout:
                    self.lock_file.close()
                    self.lock_file = None
                    raise TimeoutError(f"Could not acquire lock for {self.db_path} within {timeout} seconds")
                
                # Wait and retry
                time.sleep(retry_interval)
    
    def release_lock(self):
        """Release the lock"""
        with SQLiteDatabaseLock._lock:
            self.ref_count -= 1
            
            if self.ref_count <= 0:
                if self.acquired and self.lock_file:
                    fcntl.flock(self.lock_file.fileno(), fcntl.LOCK_UN)
                    self.lock_file.close()
                    self.lock_file = None
                    self.acquired = False
                    bt.logging.debug(f"Released lock for {self.db_path}")
                
                # Remove from locks dictionary
                if self.db_path in SQLiteDatabaseLock._locks:
                    del SQLiteDatabaseLock._locks[self.db_path]

class SQLiteDatabaseManager:
    """
    SQLite database manager for the validator.
    """
    def __init__(self, database_path):
        """
        Initialize the SQLite database manager
        
        Args:
            database_path: Path to SQLite database file
        """
        self.database_path = database_path
        self.connection = None  # Connection will be initialized in initialize method
        self._lock = None
    
    async def initialize(self):
        """
        Initialize the database connection and ensure it's valid
        
        Returns:
            bool: True if initialization was successful
        """
        try:
            # Check if we're using an in-memory database
            is_memory_db = self.database_path == ":memory:"
            
            # For file-based databases, check if the directory exists
            if not is_memory_db:
                db_dir = os.path.dirname(self.database_path)
                if db_dir and not os.path.exists(db_dir):
                    os.makedirs(db_dir, exist_ok=True)
                    bt.logging.info(f"Created database directory: {db_dir}")
                
                # Acquire lock before accessing the database
                try:
                    self._lock = SQLiteDatabaseLock.acquire(self.database_path)
                    self._lock.acquire_lock()
                except TimeoutError as e:
                    bt.logging.error(f"Could not acquire database lock: {e}")
                    return False
            
            # Try to connect to the database
            self.connection = sqlite3.connect(self.database_path)
            self.connection.row_factory = sqlite3.Row
            
            # Set pragmas for better performance and reliability
            cursor = self.connection.cursor()
            cursor.execute("PRAGMA journal_mode = WAL")
            cursor.execute("PRAGMA synchronous = NORMAL")
            cursor.execute("PRAGMA foreign_keys = ON")
            cursor.execute("PRAGMA busy_timeout = 30000")
            cursor.execute("PRAGMA temp_store = MEMORY")
            
            # Check database integrity
            if not is_memory_db and not await self.check_database_integrity():
                bt.logging.error("Database integrity check failed")
                return False
                
            bt.logging.info(f"Successfully initialized SQLite database: {self.database_path}")
            return True
            
        except sqlite3.DatabaseError as e:
            bt.logging.error(f"Database error during initialization: {e}")
            return False
        except Exception as e:
            bt.logging.error(f"Error initializing SQLite database: {e}")
            return False
    
    async def check_database_integrity(self):
        """
        Check if the database is valid and has integrity
        
        Returns:
            bool: True if database passes integrity checks
        """
        try:
            if not self.connection:
                return False
                
            cursor = self.connection.cursor()
            result = cursor.execute("PRAGMA integrity_check").fetchone()
            
            if result and result[0] == "ok":
                bt.logging.debug("Database integrity check passed")
                return True
            else:
                bt.logging.error(f"Database integrity check failed: {result}")
                return False
                
        except sqlite3.DatabaseError as e:
            bt.logging.error(f"Database error during integrity check: {e}")
            return False
        except Exception as e:
            bt.logging.error(f"Error checking database integrity: {e}")
            return False
    
    async def backup_table(self, table_name, backup_table_name):
        """
        Create a backup of a table.
        
        Args:
            table_name: Name of the table to backup
            backup_table_name: Name of the backup table
            
        Returns:
            bool: True if backup was successful
        """
        try:
            # Get table schema
            cursor = self.connection.cursor()
            cursor.execute(f"PRAGMA table_info({table_name})")
            columns = cursor.fetchall()
            
            if not columns:
                bt.logging.warning(f"Table {table_name} does not exist, cannot create backup")
                return False
            
            # Create backup table with same schema
            column_defs = []
            column_names = []
            for col in columns:
                name = col[1]
                type_name = col[2]
                notnull = "NOT NULL" if col[3] == 1 else ""
                pk = "PRIMARY KEY" if col[5] == 1 else ""
                column_defs.append(f"{name} {type_name} {notnull} {pk}".strip())
                column_names.append(name)
            
            create_sql = f"CREATE TABLE IF NOT EXISTS {backup_table_name} ({', '.join(column_defs)})"
            cursor.execute(create_sql)
            
            # Delete existing data from backup table
            cursor.execute(f"DELETE FROM {backup_table_name}")
            
            # Copy data from original table to backup
            cursor.execute(f"INSERT INTO {backup_table_name} SELECT * FROM {table_name}")
            
            self.connection.commit()
            bt.logging.info(f"Backed up {table_name} to {backup_table_name}")
            return True
            
        except Exception as e:
            bt.logging.error(f"Error backing up table {table_name}: {e}")
            return False
    
    async def restore_table_from_backup(self, table_name, backup_table_name):
        """
        Restore a table from a backup, with schema awareness for column differences.
        
        Args:
            table_name: Name of the table to restore to
            backup_table_name: Name of the backup table
            
        Returns:
            bool: True if restore was successful
        """
        try:
            # Check if backup table exists
            cursor = self.connection.cursor()
            cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{backup_table_name}'")
            if not cursor.fetchone():
                bt.logging.warning(f"Backup table {backup_table_name} does not exist, cannot restore")
                return False
            
            # Get schema for both tables
            cursor.execute(f"PRAGMA table_info({table_name})")
            target_columns = cursor.fetchall()
            target_column_names = [col[1] for col in target_columns]
            
            cursor.execute(f"PRAGMA table_info({backup_table_name})")
            backup_columns = cursor.fetchall()
            backup_column_names = [col[1] for col in backup_columns]
            
            # Check for schema differences
            if len(target_column_names) != len(backup_column_names):
                bt.logging.warning(f"Schema mismatch: {table_name} has {len(target_column_names)} columns, {backup_table_name} has {len(backup_column_names)} columns")
                
                # Find common columns
                common_columns = [col for col in backup_column_names if col in target_column_names]
                
                if not common_columns:
                    bt.logging.error(f"No common columns between {table_name} and {backup_table_name}, cannot restore")
                    return False
                
                # Delete existing data
                cursor.execute(f"DELETE FROM {table_name}")
                
                # Insert data with explicit column mapping
                columns_str = ", ".join(common_columns)
                restore_sql = f"""
                    INSERT OR REPLACE INTO {table_name} ({columns_str})
                    SELECT {columns_str} FROM {backup_table_name}
                    WHERE EXISTS (SELECT 1 FROM {backup_table_name})
                """
                cursor.execute(restore_sql)
            else:
                # Simple case - schemas match
                cursor.execute(f"DELETE FROM {table_name}")
                cursor.execute(f"INSERT INTO {table_name} SELECT * FROM {backup_table_name}")
            
            self.connection.commit()
            bt.logging.info(f"Restored {table_name} from {backup_table_name}")
            return True
            
        except Exception as e:
            bt.logging.error(f"Error restoring table {table_name} from {backup_table_name}: {e}")
            self.connection.rollback()
            return False 

    async def fetch_all(self, query, params=None):
        """
        Execute a query and fetch all results
        
        Args:
            query: SQL query to execute (can be string or SQLAlchemy TextClause)
            params: Parameters for the query
            
        Returns:
            List of rows as dictionaries
        """
        try:
            if self.connection is None:
                bt.logging.error("Database connection is not initialized")
                return []
                
            cursor = self.connection.cursor()
            
            # Convert SQLAlchemy text object to string if needed
            if hasattr(query, 'text'):
                query = query.text
                
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            
            rows = cursor.fetchall()
            return rows
        except Exception as e:
            bt.logging.error(f"Error executing query: {e}")
            return []
            
    async def fetch_one(self, query, params=None):
        """
        Execute a query and fetch one result
        
        Args:
            query: SQL query to execute (can be string or SQLAlchemy TextClause)
            params: Parameters for the query
            
        Returns:
            Single row as dictionary or None
        """
        try:
            if self.connection is None:
                bt.logging.error("Database connection is not initialized")
                return None
                
            cursor = self.connection.cursor()
            
            # Convert SQLAlchemy text object to string if needed
            if hasattr(query, 'text'):
                query = query.text
                
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            
            return cursor.fetchone()
        except Exception as e:
            bt.logging.error(f"Error executing query: {e}")
            return None
            
    async def execute_query(self, query, params=None):
        """
        Execute a query without returning results
        
        Args:
            query: SQL query to execute (can be string or SQLAlchemy TextClause)
            params: Parameters for the query
            
        Returns:
            True if successful, False otherwise
        """
        try:
            if self.connection is None:
                bt.logging.error("Database connection is not initialized")
                return False
                
            cursor = self.connection.cursor()
            
            # Convert SQLAlchemy text object to string if needed
            if hasattr(query, 'text'):
                query = query.text
                
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            
            self.connection.commit()
            return True
        except Exception as e:
            bt.logging.error(f"Error executing query: {e}")
            if self.connection:
                self.connection.rollback()
            return False
            
    async def create_backup(self, backup_path):
        """
        Create a backup of the database
        
        Args:
            backup_path: Path to store backup
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Close current connection to avoid locks
            if self.connection:
                self.connection.close()
                self.connection = None
            
            # Release current lock if we have one
            if self._lock:
                self._lock.release_lock()
                self._lock = None
            
            # Copy database file to backup location
            shutil.copy2(self.database_path, backup_path)
            
            # Reopen connection with lock
            self._lock = SQLiteDatabaseLock.acquire(self.database_path)
            self._lock.acquire_lock()
            self.connection = sqlite3.connect(self.database_path)
            self.connection.row_factory = sqlite3.Row
            
            bt.logging.info(f"Created database backup at {backup_path}")
            return True
        except Exception as e:
            bt.logging.error(f"Error creating database backup: {e}")
            return False
            
    async def close(self):
        """
        Close the database connection and release lock
        """
        try:
            if self.connection:
                self.connection.close()
                self.connection = None
                
            # Release lock if we have one
            if self._lock:
                self._lock.release_lock()
                self._lock = None
                
        except Exception as e:
            bt.logging.error(f"Error closing database connection: {e}")
            
    async def create_verified_backup(self, backup_path):
        """
        Create a backup and verify its integrity
        
        Args:
            backup_path: Path to store backup
            
        Returns:
            bool: True if successful and verified, False otherwise
        """
        try:
            # Create backup
            backup_success = await self.create_backup(backup_path)
            if not backup_success:
                return False
            
            # Verify backup integrity
            test_conn = sqlite3.connect(backup_path)
            cursor = test_conn.cursor()
            
            # Check integrity
            cursor.execute("PRAGMA integrity_check")
            result = cursor.fetchone()
            if not result or result[0] != "ok":
                bt.logging.error(f"Backup integrity check failed: {result}")
                test_conn.close()
                return False
            
            # Test core tables
            core_tables = ["miner_stats", "game_data", "predictions"]
            for table in core_tables:
                try:
                    cursor.execute(f"SELECT COUNT(*) FROM {table}")
                except sqlite3.Error as e:
                    bt.logging.error(f"Table {table} check failed in backup: {e}")
                    test_conn.close()
                    return False
            
            test_conn.close()
            return True
            
        except Exception as e:
            bt.logging.error(f"Error creating verified backup: {e}")
            return False 