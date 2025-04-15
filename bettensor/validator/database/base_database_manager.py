import asyncio
from pathlib import Path
import time
import bittensor as bt
import os
import traceback
import async_timeout
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from contextlib import asynccontextmanager
import weakref

class BaseDatabaseManager:
    """
    Abstract base class for database managers.
    Defines the common interface that both SQLite and PostgreSQL implementations must provide.
    """
    _instance = None
    
    @classmethod
    def __new__(cls, *args, **kwargs):
        # This method is implemented in subclasses based on their singleton needs
        raise NotImplementedError("Subclasses must implement __new__")
        
    def __init__(self, database_path: str):
        """Initialize the database manager"""
        self.database_path = database_path
        self.db_path = database_path  # For compatibility
        self._shutting_down = False
        self._active_sessions = set()
        self._cleanup_event = asyncio.Event()
        self._cleanup_task = None
        self._connection_attempts = 0
        self._max_connection_attempts = 50
        self._connection_attempt_reset = time.time()
        self._connection_reset_interval = 60
        self.default_timeout = 30
        self._initialized = False

    @asynccontextmanager
    async def get_session(self):
        """Session context manager with improved error handling and connection management"""
        raise NotImplementedError("Subclasses must implement get_session")

    async def _safe_close_session(self, session):
        """Safely close a session with timeout and error handling"""
        raise NotImplementedError("Subclasses must implement _safe_close_session")

    async def _acquire_connection(self):
        """Acquire a database connection."""
        raise NotImplementedError("Subclasses must implement _acquire_connection")

    async def _cleanup_stale_connections(self):
        """Cleanup stale connections and sessions"""
        raise NotImplementedError("Subclasses must implement _cleanup_stale_connections")
        
    async def _safe_close_connection(self, connection):
        """Safely close a connection with timeout and error handling"""
        raise NotImplementedError("Subclasses must implement _safe_close_connection")

    async def cleanup(self):
        """Close all database connections and perform cleanup"""
        raise NotImplementedError("Subclasses must implement cleanup")
        
    async def close_session(self, session):
        """Close a specific session"""
        raise NotImplementedError("Subclasses must implement close_session")
        
    async def _cleanup_sessions(self):
        """Cleanup all sessions"""
        raise NotImplementedError("Subclasses must implement _cleanup_sessions")
        
    @asynccontextmanager
    async def get_long_running_session(self):
        """Get a session for long-running operations"""
        raise NotImplementedError("Subclasses must implement get_long_running_session")
        
    def _convert_params(self, params):
        """Convert parameters to the format expected by the database"""
        raise NotImplementedError("Subclasses must implement _convert_params")
        
    async def execute_query(self, query, params=None):
        """Execute a query with parameters"""
        raise NotImplementedError("Subclasses must implement execute_query")
        
    async def fetch_all(self, query, params=None):
        """Fetch all rows from a query"""
        raise NotImplementedError("Subclasses must implement fetch_all")
        
    async def fetch_one(self, query, params=None):
        """Fetch one row from a query"""
        raise NotImplementedError("Subclasses must implement fetch_one")
        
    async def executemany(self, query, params_list, column_names=None, max_retries=5, retry_delay=1):
        """Execute many queries"""
        raise NotImplementedError("Subclasses must implement executemany")
        
    async def begin_transaction(self):
        """Begin a database transaction"""
        raise NotImplementedError("Subclasses must implement begin_transaction")
        
    async def commit_transaction(self, transaction_id):
        """Commit a transaction"""
        raise NotImplementedError("Subclasses must implement commit_transaction")
        
    async def rollback_transaction(self, transaction_id):
        """Rollback a transaction"""
        raise NotImplementedError("Subclasses must implement rollback_transaction")
        
    async def close(self):
        """Close database connections"""
        raise NotImplementedError("Subclasses must implement close")
        
    async def reconnect(self):
        """Reconnect to the database"""
        raise NotImplementedError("Subclasses must implement reconnect")
        
    async def has_pending_operations(self) -> bool:
        """Check if there are pending operations"""
        raise NotImplementedError("Subclasses must implement has_pending_operations")
        
    async def safe_shutdown(self):
        """Safely shut down the database"""
        raise NotImplementedError("Subclasses must implement safe_shutdown")
        
    async def create_backup(self, backup_path: Path) -> bool:
        """Create a database backup"""
        raise NotImplementedError("Subclasses must implement create_backup")
        
    async def verify_backup(self, backup_path: Path) -> bool:
        """Verify a database backup"""
        raise NotImplementedError("Subclasses must implement verify_backup")
        
    async def create_verified_backup(self, backup_path: Path) -> bool:
        """Create and verify a database backup"""
        raise NotImplementedError("Subclasses must implement create_verified_backup")
        
    async def dispose(self):
        """Dispose of database resources"""
        raise NotImplementedError("Subclasses must implement dispose")
        
    async def update_miner_weights(self, weight_updates, max_retries=5, retry_delay=1):
        """Update miner weights"""
        raise NotImplementedError("Subclasses must implement update_miner_weights")
        
    async def initialize(self, force=False):
        """Initialize the database"""
        raise NotImplementedError("Subclasses must implement initialize")
        
    @asynccontextmanager
    async def transaction(self):
        """Transaction context manager"""
        raise NotImplementedError("Subclasses must implement transaction")
        
    async def ensure_connection(self):
        """Ensure a database connection exists"""
        raise NotImplementedError("Subclasses must implement ensure_connection") 