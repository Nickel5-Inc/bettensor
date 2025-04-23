import asyncio
from pathlib import Path
import time
import bittensor as bt
import os
import traceback
import async_timeout
import uuid
import subprocess
import json
import hashlib
import re
from typing import List, Dict

from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy import text, inspect
from sqlalchemy.exc import SQLAlchemyError
from contextlib import asynccontextmanager
from urllib.parse import quote_plus
import asyncpg

from bettensor.validator.database.schema import metadata

class PostgresDatabaseManager:
    """
    PostgreSQL implementation of the database manager.
    Handles connections, query execution, and database operations for PostgreSQL.
    """
    _instance = None
    
    @classmethod
    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = object.__new__(cls)
            cls._instance._initialized = False
        return cls._instance
        
    def __init__(self, database_path=None, host="localhost", port=5432, user="postgres", 
                 password=None, dbname="bettensor_validator", pool_size=10, max_overflow=20):
        """
        Initialize the PostgreSQL database manager.
        
        Args:
            database_path: Ignored for PostgreSQL, kept for compatibility
            host: PostgreSQL server hostname
            port: PostgreSQL server port
            user: PostgreSQL username
            password: PostgreSQL password
            dbname: Database name
            pool_size: Connection pool size
            max_overflow: Maximum number of connections to allow above pool_size
        """
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.dbname = dbname
        self.pool_size = pool_size
        self.max_overflow = max_overflow
        
        if not self._initialized:
            self._transactions = {}
            self._shutting_down = False
            self._active_sessions = set()
            self._cleanup_event = asyncio.Event()
            self._cleanup_task = None
            self._connection_attempts = 0
            self._max_connection_attempts = 50
            self._connection_attempt_reset = time.time()
            self._connection_reset_interval = 60
            self.default_timeout = 30
            self._initialized = True
            
    async def _initialize_engine(self):
        """Initialize the SQLAlchemy engine for PostgreSQL with proper credentials."""
        if hasattr(self, 'engine') and self.engine:
            return
            
        # Safely encode password for URL
        password_str = f":{quote_plus(self.password)}@" if self.password else "@"
        
        bt.logging.debug(f"PostgreSQL password received in manager: length={len(self.password) if self.password else 0}")
        bt.logging.debug(f"Password string for URL: {password_str}")
        
        connection_url = (
            f"postgresql+asyncpg://{self.user}"
            f"{password_str}"
            f"{self.host}:{self.port}/{self.dbname}"
        )
        
        # Check for malformed URL - this format should be user:password@host:port/dbname
        # Fix connection URL if needed
        if "/@" in connection_url:
            # Password is empty, URL has format user@host:port/dbname
            fixed_url = connection_url.replace("/@", "@")
            bt.logging.warning(f"Fixed malformed connection URL (empty password). Using: postgresql+asyncpg://{self.user}@{self.host}:{self.port}/{self.dbname}")
            connection_url = fixed_url
        
        bt.logging.debug(f"Connection URL (password hidden): postgresql+asyncpg://{self.user}:****@{self.host}:{self.port}/{self.dbname}")
        
        self.engine = create_async_engine(
            connection_url,
            echo=False,
            pool_size=self.pool_size,
            max_overflow=self.max_overflow,
            pool_timeout=30,
            pool_recycle=1800,  # Recycle connections after 30 minutes
            pool_pre_ping=True  # Enable connection health checks
        )
        
        self.async_session = sessionmaker(
            bind=self.engine,
            class_=AsyncSession,
            expire_on_commit=False
        )
        
    async def ensure_database_exists(self):
        """Ensure the PostgreSQL database exists, create it if not."""
        try:
            # Connect to default 'postgres' database to check if our database exists
            bt.logging.debug(f"Connecting to PostgreSQL with: host={self.host}, port={self.port}, user={self.user}, password_length={len(self.password) if self.password else 0}")
            
            conn = await asyncpg.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                database="postgres"
            )
            bt.logging.debug("PostgreSQL connection successful")
            
            # Check if database exists
            exists = await conn.fetchval(
                "SELECT 1 FROM pg_database WHERE datname = $1",
                self.dbname
            )
            
            if not exists:
                bt.logging.info(f"Creating database {self.dbname}")
                # Creating database requires to be outside of transaction
                await conn.execute(f"CREATE DATABASE {self.dbname}")
                bt.logging.info(f"Database {self.dbname} created successfully")
            else:
                bt.logging.debug(f"Database {self.dbname} already exists")
                
            await conn.close()
            return True
            
        except Exception as e:
            bt.logging.error(f"Error ensuring database exists: {e}")
            bt.logging.error(f"Connection details: user={self.user}, host={self.host}, port={self.port}, dbname=postgres")
            bt.logging.error(traceback.format_exc())
            
            # Try with hardcoded password as emergency fallback
            if "password authentication failed" in str(e):
                bt.logging.warning("Attempting emergency connection with hardcoded password")
                try:
                    conn = await asyncpg.connect(
                        host=self.host,
                        port=self.port,
                        user=self.user,
                        password="postgres",
                        database="postgres"
                    )
                    bt.logging.warning("Emergency connection succeeded - please fix your configuration")
                    await conn.close()
                except Exception as fallback_error:
                    bt.logging.error(f"Emergency connection also failed: {fallback_error}")
            
            return False
            
    @asynccontextmanager
    async def get_session(self):
        """Session context manager for PostgreSQL with improved error handling and connection management"""
        if self._shutting_down:
            raise RuntimeError("Database manager is shutting down")
            
        # Ensure engine is initialized
        await self._initialize_engine()
            
        session = None
        try:
            # Cleanup stale connections first
            await self._cleanup_stale_connections()
            
            # Create session
            session = self.async_session()
            session.created_at = time.time()
            self._active_sessions.add(session)
            
            yield session
            
            # Commit any pending changes if no error occurred
            if session and session.in_transaction():
                try:
                    async with async_timeout.timeout(5):
                        await session.commit()
                except asyncio.TimeoutError:
                    await session.rollback()
                    raise
                except Exception:
                    await session.rollback()
                    raise
                
        except asyncio.CancelledError:
            bt.logging.warning("Session operation cancelled, performing cleanup")
            if session:
                try:
                    async with async_timeout.timeout(1):
                        if session.in_transaction():
                            await session.rollback()
                        await session.close()
                except Exception as e:
                    bt.logging.error(f"Error during session cleanup after cancellation: {e}")
            raise
            
        except Exception as e:
            bt.logging.error(f"Database error: {e}")
            if session:
                try:
                    async with async_timeout.timeout(1):
                        if session.in_transaction():
                            await session.rollback()
                        await session.close()
                except Exception as cleanup_error:
                    bt.logging.error(f"Error during session cleanup: {cleanup_error}")
            raise
            
        finally:
            # Always clean up the session
            if session:
                try:
                    async with async_timeout.timeout(1):
                        if session in self._active_sessions:
                            self._active_sessions.remove(session)
                        if session.in_transaction():
                            await session.rollback()
                        await session.close()
                except Exception as e:
                    bt.logging.error(f"Error during final session cleanup: {e}")

    async def _safe_close_session(self, session):
        """Safely close a session with timeout and error handling"""
        if not session:
            return
            
        try:
            # First try to rollback any pending transaction
            if session.in_transaction():
                try:
                    async with async_timeout.timeout(5):
                        await session.rollback()
                except Exception:
                    # Suppress logging during cleanup to avoid deadlocks
                    pass
                    
            # Then close the session
            async with async_timeout.timeout(5):  # 5 second timeout for closing
                await session.close()
                
        except (asyncio.TimeoutError, GeneratorExit, Exception):
            # Suppress all errors during cleanup
            pass
        finally:
            # Always remove from active sessions
            try:
                if session in self._active_sessions:
                    self._active_sessions.remove(session)
            except Exception:
                pass

    async def _acquire_connection(self):
        """Acquire a database connection for PostgreSQL."""
        try:
            if not hasattr(self, 'engine') or not self.engine:
                await self._initialize_engine()

            connection = await self.engine.connect()
            return connection

        except Exception as e:
            bt.logging.error(f"Error acquiring connection: {str(e)}")
            raise

    async def _cleanup_stale_connections(self):
        """Cleanup stale connections and sessions"""
        try:
            current_time = time.time()
            stale_timeout = 30  # 30 seconds
            
            # Cleanup stale sessions
            stale_sessions = [
                session for session in self._active_sessions
                if hasattr(session, 'created_at') and 
                current_time - session.created_at > stale_timeout
            ]
            
            for session in stale_sessions:
                bt.logging.warning(f"Cleaning up stale session created at {session.created_at}")
                await self._safe_close_session(session)
                
        except Exception as e:
            bt.logging.error(f"Error cleaning up stale connections: {e}")
            
    async def _safe_close_connection(self, connection):
        """Safely close a connection with timeout and error handling"""
        if not connection:
            return
            
        try:
            async with async_timeout.timeout(5):  # 5 second timeout
                await connection.close()
        except Exception as e:
            # Suppress error logging during cleanup
            pass

    async def cleanup(self):
        """Close all database connections and perform cleanup"""
        bt.logging.info("Cleaning up database connections")
        self._shutting_down = True
        
        # First cancel cleanup task if running
        if self._cleanup_task and not self._cleanup_task.done():
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                pass
                
        # Close all active sessions
        sessions = list(self._active_sessions)
        for session in sessions:
            await self._safe_close_session(session)
            
        # Dispose of the engine
        if hasattr(self, 'engine') and self.engine:
            await self.engine.dispose()
            
        bt.logging.info("Database cleanup completed")
        
    async def close_session(self, session):
        """Close a specific session"""
        await self._safe_close_session(session)
        
    async def _cleanup_sessions(self):
        """Cleanup all sessions"""
        for session in list(self._active_sessions):
            await self._safe_close_session(session)
        
    @asynccontextmanager
    async def get_long_running_session(self):
        """Get a session for long-running operations with extended timeouts"""
        await self._initialize_engine()
        session = None
        
        try:
            session = self.async_session()
            session.created_at = time.time()
            self._active_sessions.add(session)
            
            # Execute statement to test connection
            await session.execute(text("SELECT 1"))
            
            yield session
            
            if session.in_transaction():
                await session.commit()
                
        except Exception as e:
            bt.logging.error(f"Error in long running session: {e}")
            if session and session.in_transaction():
                await session.rollback()
            raise
            
        finally:
            if session:
                if session in self._active_sessions:
                    self._active_sessions.remove(session)
                    
                await session.close()
                
    def _convert_params(self, params):
        """
        Convert parameters to the format expected by PostgreSQL.
        SQLite uses ? placeholders, PostgreSQL uses %s 
        """
        if params is None:
            return None
        
        # If params is a dictionary, convert types as needed
        if isinstance(params, dict):
            converted_params = {}
            for key, value in params.items():
                # Convert specific numeric parameters that need to be strings
                if key == 'miner_uid' and isinstance(value, int):
                    converted_params[key] = str(value)
                elif isinstance(value, int) and key.endswith('_uid'):
                    converted_params[key] = str(value)
                # Handle array values
                elif isinstance(value, (list, tuple)):
                    # For external_id arrays, convert strings to integers
                    if key == 'game_ids' or key == 'external_id':
                        converted_params[key] = [int(x) for x in value]
                    else:
                        converted_params[key] = value
                else:
                    converted_params[key] = value
            return converted_params
        
        # If params is a tuple/list, convert items as needed
        elif isinstance(params, (list, tuple)):
            # If it's a list of dictionaries or tuples, convert each item
            if params and isinstance(params[0], (dict, tuple, list)):
                # Special handling for array parameters
                if len(params) == 1 and isinstance(params[0], (list, tuple)):
                    # Convert string array elements to integers for ANY queries
                    try:
                        return ([int(x) for x in params[0]],)
                    except (ValueError, TypeError):
                        # If conversion fails, return original values
                        return params
                return [self._convert_params(item) for item in params]
            
            # Single list/tuple of values - convert to tuple
            converted_params = []
            for value in params:
                if isinstance(value, (list, tuple)):
                    # Try to convert array elements to integers
                    try:
                        converted_params.append([int(x) for x in value])
                    except (ValueError, TypeError):
                        converted_params.append(value)
                else:
                    converted_params.append(value)
            return tuple(converted_params)  # Always return as tuple for SQLAlchemy
        
        # Return unchanged for other cases
        return params
        
    async def execute_query(self, query, params=None):
        """
        Execute a query with parameters.
        Converts SQLite-style queries to PostgreSQL format.
        """
        # Convert query syntax to PostgreSQL format
        # <<< REMOVED QUERY CONVERSION >>>
        # postgres_query = self._convert_query_syntax(query)
        postgres_query = query # Use original query
        
        async with self.get_session() as session:
            try:
                # <<< MODIFIED to remove _convert_params >>>
                # <<< ADDED WRAP for dict params >>>
                execute_params = [params] if isinstance(params, dict) else params
                result = await session.execute(text(postgres_query), execute_params)
                
                if postgres_query.strip().upper().startswith("SELECT"):
                    # For SELECT queries, fetch all rows
                    rows = result.fetchall()
                    # Convert to dict-like objects for compatibility with existing code
                    return [dict(zip(row.keys(), row)) for row in rows]
                else:
                    # For non-SELECT queries, return rowcount
                    return result.rowcount
                    
            except Exception as e:
                bt.logging.error(f"Error executing query: {postgres_query}")
                bt.logging.error(f"Parameters: {params}")
                bt.logging.error(f"Error: {str(e)}")
                raise
                
    async def fetch_all(self, query, params=None):
        """
        Fetch all rows from the database.
        
        Args:
            query: The SQL query to execute
            params: Optional query parameters
        """
        try:
            async with self.get_session() as session:
                # Convert query syntax and parameters if needed
                # <<< REMOVED QUERY CONVERSION >>>
                # postgres_query = self._convert_query_syntax(query)
                postgres_query = query # Use original query
                
                # Handle parameters
                # <<< KEPT PARAMETER HANDLING for now, might need adjustment if issues persist >>>
                if params is not None:
                    if isinstance(params, (list, tuple)):
                        # If it's a single-element tuple/list containing a list of IDs
                        if len(params) == 1 and isinstance(params[0], (list, tuple)):
                            # Convert string IDs to integers for ANY queries
                            try:
                                postgres_params = ([int(x) for x in params[0]],)
                            except (ValueError, TypeError):
                                bt.logging.warning("Failed to convert array elements to integers, using original values")
                                postgres_params = params
                        else:
                            postgres_params = tuple(
                                int(p) if isinstance(p, str) and p.isdigit() else p 
                                for p in params
                            )
                    elif isinstance(params, dict):
                        postgres_params = {}
                        for k, v in params.items():
                            if isinstance(v, (list, tuple)):
                                try:
                                    postgres_params[k] = [int(x) if isinstance(x, str) and x.isdigit() else x for x in v]
                                except (ValueError, TypeError):
                                    postgres_params[k] = v
                            elif isinstance(v, str) and v.isdigit():
                                postgres_params[k] = int(v)
                            # Explicitly handle booleans to prevent conversion to int
                            elif isinstance(v, bool):
                                postgres_params[k] = v 
                            else:
                                postgres_params[k] = v
                    else:
                        postgres_params = int(params) if isinstance(params, str) and params.isdigit() else params
                else:
                    postgres_params = None

                try:
                    # Execute query with parameters
                    # <<< MODIFIED to remove _convert_params (already processed above) >>>
                    # <<< ADDED WRAP for dict params >>>
                    execute_params = [postgres_params] if isinstance(postgres_params, dict) else postgres_params
                    result = await session.execute(text(postgres_query), execute_params)
                    rows = result.all()
                    if not rows:
                        return []
                    return [dict(zip(result.keys(), row)) for row in rows]
                except Exception as e:
                    if "current transaction is aborted" in str(e):
                        await session.rollback()
                        result = await session.execute(text(postgres_query), postgres_params)
                        rows = result.all()
                        if not rows:
                            return []
                        return [dict(zip(result.keys(), row)) for row in rows]
                    raise
        except Exception as e:
            bt.logging.error(f"Error fetching all rows: {str(e)}")
            raise
                
    async def fetch_one(self, query, params=None):
        """
        Fetch a single row from the database.
        
        Args:
            query: The SQL query to execute
            params: Optional query parameters
        """
        try:
            async with self.get_session() as session:
                # Convert query syntax and parameters if needed
                # <<< REMOVED QUERY CONVERSION >>>
                # postgres_query = self._convert_query_syntax(query)
                postgres_query = query # Use original query
                # <<< MODIFIED to remove _convert_params >>>
                postgres_params = params # Pass original dict/tuple/None
                
                # Execute query with parameters
                # <<< WRAP dict params in a list >>>
                execute_params = [postgres_params] if isinstance(postgres_params, dict) else postgres_params
                result = await session.execute(text(postgres_query), execute_params)
                row = result.first()
                if row is None:
                    return None
                # Get column names from result.keys()
                return dict(zip(result.keys(), row))
        except Exception as e:
            bt.logging.error(f"Error fetching one row: {str(e)}")
            raise
                
    async def executemany(self, query: str, params: List[tuple | Dict], column_names: List[str] | None = None):
        """Execute a query with multiple parameter sets."""
        if not self.engine:
            await self._initialize_engine()
            
        # Use original query with :named parameters
        postgres_query = query 
        bt.logging.trace(f"Executing many with query: {postgres_query}")
        # bt.logging.trace(f"Executing many with params (first 5): {params[:5]}") # Be careful logging sensitive data

        try:
            async with self.get_session() as session:
                # Pass the original params list directly to session.execute
                # SQLAlchemy handles list of tuples or list of dicts for executemany
                result = await session.execute(text(postgres_query), params)
                # No explicit commit needed here if session uses autocommit or manages transaction externally
                return result
        except Exception as e:
            bt.logging.error(f"Error executing query: \n{query}")
            # Log parameters safely (avoid logging sensitive info)
            if params:
                 first_param = params[0]
                 if isinstance(first_param, dict):
                     log_params = first_param
                 elif isinstance(first_param, tuple):
                     if column_names and len(column_names) == len(first_param):
                         # Use column_names if available and length matches
                         log_params = {column_names[i]: v for i, v in enumerate(first_param)}
                     else:
                         # Fallback to arg_i if column_names are missing or mismatched
                         log_params = {f'arg_{i}': v for i, v in enumerate(first_param)}
                 else:
                     # Fallback for unexpected type
                     log_params = {f'arg_0': first_param}
                 bt.logging.error(f"Sample Parameters: {log_params}") 
            else:
                 bt.logging.error("Parameters: []")
            bt.logging.error(f"Error: {e}")
            # Re-raise the specific SQLAlchemy/DBAPI error for detailed trace
            raise e 
        
    async def begin_transaction(self):
        """Begin a database transaction"""
        session = self.async_session()
        await session.begin()
        transaction_id = str(uuid.uuid4())
        self._transactions[transaction_id] = session
        return transaction_id
        
    async def commit_transaction(self, transaction_id):
        """Commit a transaction"""
        if transaction_id not in self._transactions:
            raise ValueError(f"Transaction {transaction_id} not found")
            
        session = self._transactions[transaction_id]
        try:
            await session.commit()
        finally:
            await session.close()
            del self._transactions[transaction_id]
            
    async def rollback_transaction(self, transaction_id):
        """Rollback a transaction"""
        if transaction_id not in self._transactions:
            raise ValueError(f"Transaction {transaction_id} not found")
            
        session = self._transactions[transaction_id]
        try:
            await session.rollback()
        finally:
            await session.close()
            del self._transactions[transaction_id]
            
    async def close(self):
        """Alias for cleanup"""
        await self.cleanup()
        
    async def reconnect(self):
        """Reconnect to the database"""
        await self.cleanup()
        await self._initialize_engine()
        
    async def has_pending_operations(self) -> bool:
        """Check if there are pending operations"""
        # Check for active sessions
        if self._active_sessions:
            return True
            
        # Check for active transactions
        if self._transactions:
            return True
            
        return False
        
    async def wait_for_locks_to_clear(self, timeout=30):
        """
        Wait for any PostgreSQL locks to clear before proceeding.
        
        Args:
            timeout: Maximum time in seconds to wait for locks to clear
        """
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            try:
                async with self.get_session() as session:
                    # Query for active locks
                    result = await session.execute(text("""
                        SELECT count(*) as lock_count
                        FROM pg_locks l
                        JOIN pg_stat_activity a ON l.pid = a.pid
                        WHERE a.datname = :dbname
                    """), {"dbname": self.dbname})
                    
                    row = result.fetchone()
                    lock_count = row[0] if row else 0
                    
                    if lock_count == 0:
                        return True
                        
                    bt.logging.info(f"Waiting for {lock_count} database locks to clear...")
                    
            except Exception as e:
                bt.logging.warning(f"Error checking for locks: {e}")
                
            await asyncio.sleep(1)
            
        bt.logging.warning(f"Timed out waiting for database locks to clear after {timeout} seconds")
        return False
        
    async def safe_shutdown(self):
        """Safely shut down the database"""
        # First wait for locks to clear
        await self.wait_for_locks_to_clear()
        
        # Then clean up connections
        await self.cleanup()
        
    async def create_backup(self, backup_path: Path) -> bool:
        """
        Create a PostgreSQL database backup using pg_dump.
        
        Args:
            backup_path: Path where the backup file will be saved
        
        Returns:
            bool: True if backup was successful, False otherwise
        """
        try:
            # Ensure backup directory exists
            backup_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Build pg_dump command
            cmd = [
                "pg_dump",
                "-h", self.host,
                "-p", str(self.port),
                "-U", self.user,
                "-F", "c",  # Custom format (compressed)
                "-f", str(backup_path),
                self.dbname
            ]
            
            # Set password environment variable for pg_dump
            env = os.environ.copy()
            if self.password:
                env["PGPASSWORD"] = self.password
                
            # Execute pg_dump
            process = await asyncio.create_subprocess_exec(
                *cmd,
                env=env,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            
            stdout, stderr = await process.communicate()
            
            if process.returncode != 0:
                bt.logging.error(f"pg_dump failed: {stderr.decode()}")
                return False
                
            bt.logging.info(f"Created PostgreSQL backup at {backup_path}")
            return True
            
        except Exception as e:
            bt.logging.error(f"Error creating database backup: {e}")
            bt.logging.error(traceback.format_exc())
            return False
            
    async def verify_backup(self, backup_path: Path) -> bool:
        """
        Verify a PostgreSQL database backup using pg_restore.
        
        Args:
            backup_path: Path to the backup file
            
        Returns:
            bool: True if backup is valid, False otherwise
        """
        try:
            # Check if backup file exists
            if not backup_path.exists():
                bt.logging.error(f"Backup file does not exist: {backup_path}")
                return False
                
            # Build pg_restore command (test only, don't actually restore)
            cmd = [
                "pg_restore",
                "-h", self.host,
                "-p", str(self.port),
                "-U", self.user,
                "--list",  # Just list contents, don't restore
                str(backup_path)
            ]
            
            # Set password environment variable for pg_restore
            env = os.environ.copy()
            if self.password:
                env["PGPASSWORD"] = self.password
                
            # Execute pg_restore
            process = await asyncio.create_subprocess_exec(
                *cmd,
                env=env,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            
            stdout, stderr = await process.communicate()
            
            if process.returncode != 0:
                bt.logging.error(f"pg_restore verification failed: {stderr.decode()}")
                return False
                
            bt.logging.info(f"Verified PostgreSQL backup at {backup_path}")
            return True
            
        except Exception as e:
            bt.logging.error(f"Error verifying database backup: {e}")
            bt.logging.error(traceback.format_exc())
            return False
            
    async def create_verified_backup(self, backup_path: Path) -> bool:
        """
        Create and verify a database backup.
        
        Args:
            backup_path: Path where the backup file will be saved
            
        Returns:
            bool: True if backup was created and verified successfully, False otherwise
        """
        if await self.create_backup(backup_path):
            return await self.verify_backup(backup_path)
        return False
        
    async def dispose(self):
        """Dispose of database resources"""
        if hasattr(self, 'engine') and self.engine:
            await self.engine.dispose()
            
    async def update_miner_weights(self, weight_updates, max_retries=5, retry_delay=1):
        """
        Update miner weights in the database.
        
        Args:
            weight_updates: List of (weight, miner_uid) tuples
            max_retries: Maximum number of retry attempts
            retry_delay: Delay between retry attempts in seconds
        """
        if not weight_updates:
            return
            
        # PostgreSQL update query using miner_stats table
        query = """
        UPDATE miner_stats 
        SET most_recent_weight = :weight
        WHERE miner_uid = :miner_uid
        """
        
        # Convert weights to list of parameter dictionaries with proper types
        params_list = [
            {
                "miner_uid": int(miner_uid),  # Ensure miner_uid is integer
                "weight": float(weight)  # Ensure weight is float
            } 
            for weight, miner_uid in weight_updates
        ]
        
        try:
            async with self.get_long_running_session() as session:
                for params in params_list:
                    await session.execute(text(query), params)
                await session.commit()
                bt.logging.debug(f"Updated weights for {len(params_list)} miners")
        except Exception as e:
            bt.logging.error(f"Error updating miner weights: {e}")
            raise
                    
    async def initialize(self, force=False):
        """Initialize the database with required tables using SQLAlchemy metadata."""
        # Reset shutdown flag if forcing re-initialization
        if force:
            self._shutting_down = False
        
        try:
            # Ensure the database and engine exist before creating tables
            if not await self.ensure_database_exists():
                raise Exception("Failed to ensure database exists")
            await self._initialize_engine() 
            
            # Create tables using metadata
            bt.logging.info("Initializing database tables...")
            try:
                async with self.engine.begin() as conn:
                    # Re-introduce drop logic when force=True
                    if force:
                        bt.logging.warning("Force flag is True. Dropping all tables based on current metadata...")
                        await conn.run_sync(metadata.drop_all)
                        bt.logging.info("Tables dropped successfully.")
                        
                    # NOTE: Drop logic now handled by MigrationManager based on setup.cfg
                    # Use run_sync for the synchronous metadata.create_all method
                    bt.logging.info("Creating tables if they don't exist (or recreating if forced)...")
                    await conn.run_sync(metadata.create_all)
                bt.logging.info("Table creation/check completed.")
            except Exception as e:
                bt.logging.error(f"Error creating tables from metadata: {e}")
                bt.logging.error(traceback.format_exc())
                raise
                
            # Execute functions, triggers, and initial data within a session
            async with self.get_session() as session:
                async with session.begin(): # Use session.begin() for transaction context
                    bt.logging.info("Creating functions and triggers...")
                    # --- REMOVED ALL CREATE TABLE STATEMENTS --- 
                    
                    # --- REMOVED ALTER TABLE statement ---
                    
                    # Create cleanup functions and triggers - each in a separate statement
                    # Ensure these are idempotent (CREATE OR REPLACE, DROP IF EXISTS)
                    # 1. Create delete_old_predictions function (Assuming this relates to entropy_predictions)
                    await session.execute(text("""
                        CREATE OR REPLACE FUNCTION delete_old_entropy_predictions() RETURNS TRIGGER AS $$
                        BEGIN
                            DELETE FROM entropy_predictions
                            WHERE prediction_date < (NOW() - INTERVAL '45 days');
                            RETURN NEW;
                        END;
                        $$ LANGUAGE plpgsql;
                    """))

                    # 2. Drop old trigger if exists - separate statement
                    await session.execute(text("""
                        DROP TRIGGER IF EXISTS delete_old_entropy_predictions_trigger ON entropy_predictions;
                    """))

                    # 3. Create new trigger - separate statement
                    await session.execute(text("""
                        CREATE TRIGGER delete_old_entropy_predictions_trigger
                        AFTER INSERT ON entropy_predictions
                        FOR EACH ROW EXECUTE FUNCTION delete_old_entropy_predictions();
                    """))
                    
                    # --- Adding other triggers from old SQLite schema --- 
                    # Adjusting for PostgreSQL syntax and potential table name differences
                    
                    # Trigger for predictions table (if needed, similar to entropy_predictions)
                    await session.execute(text("""
                        CREATE OR REPLACE FUNCTION delete_old_predictions() RETURNS TRIGGER AS $$
                        BEGIN
                            DELETE FROM predictions
                            WHERE prediction_date < (NOW() - INTERVAL '50 days');
                            RETURN NEW;
                        END;
                        $$ LANGUAGE plpgsql;
                    """))
                    await session.execute(text("DROP TRIGGER IF EXISTS delete_old_predictions_trigger ON predictions;"))
                    await session.execute(text("""
                        CREATE TRIGGER delete_old_predictions_trigger
                        AFTER INSERT ON predictions
                        FOR EACH ROW EXECUTE FUNCTION delete_old_predictions();
                    """))
                    
                    # Trigger for game_data table
                    await session.execute(text("""
                        CREATE OR REPLACE FUNCTION delete_old_game_data() RETURNS TRIGGER AS $$
                        BEGIN
                            DELETE FROM game_data
                            WHERE event_start_date < (NOW() - INTERVAL '50 days');
                            RETURN NEW;
                        END;
                        $$ LANGUAGE plpgsql;
                    """))
                    await session.execute(text("DROP TRIGGER IF EXISTS delete_old_game_data_trigger ON game_data;"))
                    await session.execute(text("""
                        CREATE TRIGGER delete_old_game_data_trigger
                        AFTER INSERT ON game_data
                        FOR EACH ROW EXECUTE FUNCTION delete_old_game_data();
                    """))
                    
                    # Trigger for score_state table
                    await session.execute(text("""
                        CREATE OR REPLACE FUNCTION delete_old_score_state() RETURNS TRIGGER AS $$
                        BEGIN
                            DELETE FROM score_state
                            WHERE last_update_date < (NOW() - INTERVAL '7 days');
                            RETURN NEW;
                        END;
                        $$ LANGUAGE plpgsql;
                    """))
                    await session.execute(text("DROP TRIGGER IF EXISTS delete_old_score_state_trigger ON score_state;"))
                    await session.execute(text("""
                        CREATE TRIGGER delete_old_score_state_trigger
                        AFTER INSERT ON score_state
                        FOR EACH ROW EXECUTE FUNCTION delete_old_score_state();
                    """))

                    # Trigger for entropy_closed_games table 
                    await session.execute(text("""
                        CREATE OR REPLACE FUNCTION cleanup_old_entropy_closed_games() RETURNS TRIGGER AS $$
                        BEGIN
                            DELETE FROM entropy_closed_games
                            WHERE close_time < (NOW() - INTERVAL '45 days');
                            RETURN NEW;
                        END;
                        $$ LANGUAGE plpgsql;
                    """))
                    await session.execute(text("DROP TRIGGER IF EXISTS cleanup_old_entropy_closed_games_trigger ON entropy_closed_games;"))
                    await session.execute(text("""
                        CREATE TRIGGER cleanup_old_entropy_closed_games_trigger
                        AFTER INSERT ON entropy_closed_games
                        FOR EACH ROW EXECUTE FUNCTION cleanup_old_entropy_closed_games();
                    """))
                    
                    # --- Removed cleanup for entropy_game_pools as it was complex and potentially incorrect ---
                    # Revisit if specific cleanup logic for this table is needed.

                    # Insert initial version if not exists
                    bt.logging.info("Ensuring database version is set...")
                    await session.execute(text("""
                        INSERT INTO db_version (version) VALUES (1)
                        ON CONFLICT (version) DO NOTHING;
                    """))
                    
                    # Commit transaction handled by async with session.begin()
                    bt.logging.info("Functions, triggers, and version check completed.")

                bt.logging.info("Database initialization sequence completed successfully")
                return True

        except Exception as e:
            bt.logging.error(f"Error during database initialization: {str(e)}")
            bt.logging.error(traceback.format_exc()) # Log full traceback for initialization errors
            # Rollback is handled by asynccontextmanager if session was active
            raise
            
    @asynccontextmanager
    async def transaction(self):
        """
        Transaction context manager.
        Handles commit/rollback automatically.
        """
        async with self.get_session() as session:
            async with session.begin():
                yield session
                
    async def ensure_connection(self):
        """Ensure a database connection exists and is working"""
        try:
            async with self.get_session() as session:
                await session.execute(text("SELECT 1"))
            return True
        except Exception as e:
            bt.logging.error(f"Database connection check failed: {e}")
            return False
            
    async def _test_connection(self):
        """Test the database connection"""
        return await self.ensure_connection()

    def _convert_query_syntax(self, query):
        """
        Convert SQLite query syntax to PostgreSQL compatible syntax.
        
        Args:
            query: SQLite query string or SQLAlchemy TextClause
            
        Returns:
            PostgreSQL compatible query string
        """
        # Handle SQLAlchemy TextClause objects
        if hasattr(query, 'text'):
            query = query.text
            
        # Convert SQLite paramstyle to PostgreSQL
        query = query.replace('?', '%s')
        
        # Convert SQLite functions
        query = query.replace("datetime('now')", "CURRENT_TIMESTAMP")
        
        # Convert INSERT OR REPLACE to INSERT ... ON CONFLICT
        if "INSERT OR REPLACE INTO" in query:
            # Extract table name and columns
            match = re.match(r"INSERT OR REPLACE INTO (\w+)\s*\((.*?)\)", query)
            if match:
                table_name = match.group(1)
                columns = match.group(2)
                
                # Replace with ON CONFLICT syntax
                query = query.replace(
                    "INSERT OR REPLACE INTO",
                    f"INSERT INTO"
                )
                
                # For entropy_system_state table, we know the primary key is 'id'
                if table_name == "entropy_system_state":
                    query += " ON CONFLICT (id) DO UPDATE SET "
                else:
                    # For other tables, try to identify primary key columns
                    pk_cols = []
                    for col in columns.split(','):
                        col = col.strip()
                        if col.endswith('_id') or col == 'id' or col == 'prediction_id' or col == 'game_id':
                            pk_cols.append(col)
                    
                    if not pk_cols:
                        # If no obvious primary key columns found, use all columns
                        pk_cols = [col.strip() for col in columns.split(',')]
                    
                    query += f" ON CONFLICT ({', '.join(pk_cols)}) DO UPDATE SET "
                
                # Add column updates
                cols = [c.strip() for c in columns.split(',')]
                updates = [f"{c} = EXCLUDED.{c}" for c in cols]
                query += ", ".join(updates)
        
        return query 