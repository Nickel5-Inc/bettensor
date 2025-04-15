import asyncio
import logging
import os
import time
import traceback
from typing import Dict, List, Optional, Set, Tuple, Union, Any

import bittensor as bt
import aiosqlite
import asyncpg

import importlib.util
import contextlib
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    create_async_engine,
    async_scoped_session,
)
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import text

# For type checking
from sqlalchemy.engine.cursor import CursorResult


class DatabaseManager:
    """Class to manage database connections."""

    def __init__(
        self,
        db_url: str,
        create_db_automatically: bool = True
    ):
        """
        Initialize the database manager.

        Args:
            db_url (str): URL of the database to connect to.
            create_db_automatically (bool): Whether to create the database automatically if it doesn't exist.
        """
        self.db_url = db_url
        self.create_db_automatically = create_db_automatically
        self.engine = None
        self.session_factory = None
        self.session = None
        self.initialized = False
        self.is_postgres = "postgresql" in db_url

        try:
            # Parse the connection URL to extract the database name
            if self.is_postgres:
                # Parse PostgreSQL URL
                # Format: postgresql[+asyncpg]://username:password@host:port/dbname
                parts = db_url.split("/")
                if len(parts) > 3:  # Has a database name
                    self.dbname = parts[-1].split("?")[0]  # Remove query parameters if any
                    host_parts = parts[2].split("@")
                    if len(host_parts) > 1:  # Has authentication
                        self.host = host_parts[1].split(":")[0]
                    else:
                        self.host = host_parts[0].split(":")[0]
                bt.logging.info(f"Postgresql database: {self.dbname} on {self.host}")
            else:
                # SQLite database
                self.db_path = db_url.replace("sqlite+aiosqlite:///", "")
                bt.logging.info(f"SQLite database: {self.db_path}")
        except Exception as e:
            bt.logging.error(f"Error parsing database URL: {e}")

    def is_postgresql(self) -> bool:
        """Check if the database is PostgreSQL.
        
        Returns:
            bool: True if using PostgreSQL, False if using SQLite
        """
        return self.is_postgres
        
    async def initialize(self):
        """Initialize the database connection."""
        if self.initialized:
            bt.logging.debug("Database already initialized.")
            return True

        try:
            if self.is_postgres:
                # For PostgreSQL, we need to make sure the database exists
                await self._ensure_postgres_db_exists()

            # Create the engine with longer timeouts for both PostgreSQL and SQLite
            connect_args = {}
            if "sqlite" in self.db_url:
                connect_args = {
                    "timeout": 60.0,  # Connection timeout in seconds
                    "check_same_thread": False,
                }

            self.engine = create_async_engine(
                self.db_url,
                connect_args=connect_args,
                pool_size=5,
                max_overflow=10,
                pool_timeout=60,
                pool_recycle=3600,
                pool_pre_ping=True,
                echo=False,
            )

            # Create session factory with autocommit disabled
            self.session_factory = sessionmaker(
                self.engine, class_=AsyncSession, expire_on_commit=False, autocommit=False
            )

            # Create scoped session
            self.session = async_scoped_session(
                self.session_factory, scopefunc=asyncio.current_task
            )

            self.initialized = True
            bt.logging.info("Database connection initialized successfully.")
            return True
        except Exception as e:
            bt.logging.error(f"Error initializing database connection: {e}")
            bt.logging.error(traceback.format_exc())
            return False

    @contextlib.asynccontextmanager
    async def get_session(self):
        """Get a database session."""
        if not self.initialized:
            if not await self.initialize():
                raise Exception("Failed to initialize database connection.")

        session = self.session_factory()
        try:
            yield session
            await session.commit()
        except Exception as e:
            await session.rollback()
            raise e
        finally:
            await session.close()

    @contextlib.asynccontextmanager
    async def get_long_running_session(self):
        """Get a database session for long-running operations."""
        if not self.initialized:
            if not await self.initialize():
                raise Exception("Failed to initialize database connection.")

        session = self.session_factory()
        try:
            yield session
        except Exception as e:
            await session.rollback()
            raise e
        finally:
            await session.close()

    async def _ensure_postgres_db_exists(self):
        """Ensure that the PostgreSQL database exists."""
        try:
            if not hasattr(self, 'dbname') or not hasattr(self, 'host'):
                bt.logging.warning("Could not determine PostgreSQL database name or host.")
                return
                
            # Connect to the 'postgres' database to check if our database exists
            default_db_url = self.db_url.rsplit('/', 1)[0] + '/postgres'
            default_db_url = default_db_url.replace('+asyncpg', '')
            
            bt.logging.debug(f"Connecting to default database to check if {self.dbname} exists")
            conn = await asyncpg.connect(default_db_url)
            
            try:
                # Check if the database exists
                result = await conn.fetchval(
                    "SELECT 1 FROM pg_database WHERE datname = $1", self.dbname
                )
                
                if not result and self.create_db_automatically:
                    bt.logging.info(f"Database {self.dbname} does not exist. Creating it...")
                    # Create the database
                    await conn.execute(f'CREATE DATABASE "{self.dbname}"')
                    bt.logging.info(f"Database {self.dbname} created successfully.")
                elif result:
                    bt.logging.debug(f"Database {self.dbname} already exists.")
                else:
                    bt.logging.warning(f"Database {self.dbname} does not exist and automatic creation is disabled.")
            finally:
                await conn.close()
                
        except Exception as e:
            bt.logging.error(f"Error ensuring PostgreSQL database exists: {e}")
            bt.logging.error(traceback.format_exc())
            # Continue anyway, as the database might still exist
            
    async def close(self):
        """Close the database connection."""
        if self.engine:
            await self.engine.dispose()
            self.initialized = False
            bt.logging.info("Database connection closed.") 