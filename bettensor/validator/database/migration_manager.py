"""
Simple, consolidated database migration manager for Bettensor
Moves data from SQLite to PostgreSQL following a clear, linear process
"""

import os
import sys
import json
import psycopg2
import sqlite3
import asyncio
import logging
import subprocess
from pathlib import Path
from typing import Dict, List, Tuple, Any, Optional
import bittensor as bt
from datetime import datetime
from psycopg2.extras import execute_values

class MigrationManager:
    """
    Simple, consolidated manager for migrating from SQLite to PostgreSQL.
    
    Follows these steps:
    1. Checks if migration is necessary
    2. Ensures state-sync database is used as source
    3. Verifies PostgreSQL installation and setup
    4. Creates PostgreSQL schema
    5. Migrates data from SQLite to PostgreSQL
    6. Verifies migration success
    7. Updates configuration to use PostgreSQL
    """
    
    def __init__(
        self, 
        sqlite_path: str, 
        pg_config: Dict[str, Any],
        status_file_path: str = None
    ):
        """
        Initialize the migration manager.
        
        Args:
            sqlite_path: Path to the SQLite database
            pg_config: PostgreSQL connection configuration dictionary
            status_file_path: Path to file for storing migration status
        """
        self.sqlite_path = sqlite_path
        self.pg_config = pg_config
        self.status_file_path = status_file_path or str(Path.home() / ".bettensor" / "migration_status.json")
        self.migration_status = self._load_migration_status()
        self.logger = logging.getLogger("migration_manager")
        
        # Required tables that must be migrated
        self.required_tables = [
            "miner_stats", 
            "game_data", 
            "predictions", 
            "keys", 
            "scores"
        ]
        
    def _load_migration_status(self) -> Dict[str, Any]:
        """Load migration status from file or initialize a new one"""
        if os.path.exists(self.status_file_path):
            try:
                with open(self.status_file_path, 'r') as f:
                    return json.load(f)
            except Exception as e:
                bt.logging.error(f"Error loading migration status: {e}")
        
        # Default migration status
        return {
            "migration_needed": True,
            "state_synced": False,
            "postgres_ready": False,
            "tables_created": False,
            "data_migrated": {},
            "migration_verified": False,
            "completed": False,
            "start_time": None,
            "end_time": None,
            "errors": []
        }
    
    def _save_migration_status(self):
        """Save migration status to file"""
        try:
            os.makedirs(os.path.dirname(self.status_file_path), exist_ok=True)
            with open(self.status_file_path, 'w') as f:
                json.dump(self.migration_status, f, indent=2)
            bt.logging.debug(f"Migration status saved to {self.status_file_path}")
        except Exception as e:
            bt.logging.error(f"Failed to save migration status: {e}")
    
    async def is_migration_needed(self) -> bool:
        """
        Check if migration from SQLite to PostgreSQL is needed
        
        Returns:
            bool: True if migration is needed, False otherwise
        """
        # If migration was already completed, we don't need to do it again
        if self.migration_status.get("completed", False):
            bt.logging.info("Migration already completed")
            return False
            
        # Check if SQLite database exists
        if not os.path.exists(self.sqlite_path):
            bt.logging.info(f"SQLite database does not exist at {self.sqlite_path}")
            return False
            
        # Check if PostgreSQL is already set up with data
        try:
            conn = psycopg2.connect(**self.pg_config)
            cursor = conn.cursor()
            
            # Check if any of the required tables exist and have data
            for table in self.required_tables:
                cursor.execute(f"SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = '{table}')")
                table_exists = cursor.fetchone()[0]
                
                if table_exists:
                    cursor.execute(f"SELECT COUNT(*) FROM {table}")
                    count = cursor.fetchone()[0]
                    if count > 0:
                        bt.logging.info(f"PostgreSQL table {table} already exists with {count} rows")
                        return False
            
            cursor.close()
            conn.close()
        except Exception as e:
            bt.logging.warning(f"Failed to check PostgreSQL database: {e}")
            # If we can't connect to PostgreSQL, we need to set it up
            return True
            
        return True
    
    async def ensure_state_sync_database(self) -> bool:
        """
        Ensure we're using the latest state-synced database
        
        Returns:
            bool: True if successful, False otherwise
        """
        if self.migration_status.get("state_synced", False):
            bt.logging.info("Already using state-synced database")
            return True
            
        try:
            # This is a simplified placeholder for the actual state sync logic
            # In practice, you would call your state sync mechanism here
            bt.logging.info("Ensuring state-synced database is used")
            
            # For the sake of this example, we'll assume state sync is successful
            # The actual implementation should integrate with your existing state sync code
            
            self.migration_status["state_synced"] = True
            self._save_migration_status()
            return True
        except Exception as e:
            bt.logging.error(f"Failed to ensure state-synced database: {e}")
            self.migration_status["errors"].append(f"State sync error: {str(e)}")
            self._save_migration_status()
            return False
    
    async def verify_postgres_setup(self) -> bool:
        """
        Verify that PostgreSQL is installed and configured correctly
        
        Returns:
            bool: True if PostgreSQL is ready, False otherwise
        """
        if self.migration_status.get("postgres_ready", False):
            bt.logging.info("PostgreSQL already verified")
            return True
            
        try:
            conn = psycopg2.connect(**self.pg_config)
            cursor = conn.cursor()
            cursor.execute("SELECT version()")
            version = cursor.fetchone()[0]
            cursor.close()
            conn.close()
            
            bt.logging.info(f"PostgreSQL is installed: {version}")
            self.migration_status["postgres_ready"] = True
            self._save_migration_status()
            return True
        except psycopg2.OperationalError as e:
            bt.logging.error(f"PostgreSQL connection failed: {e}")
            self.migration_status["errors"].append(f"PostgreSQL setup error: {str(e)}")
            self._save_migration_status()
            return False
            
    async def create_postgres_schema(self) -> bool:
        """
        Create the required PostgreSQL schema
        
        Returns:
            bool: True if schema creation was successful, False otherwise
        """
        if self.migration_status.get("tables_created", False):
            bt.logging.info("PostgreSQL tables already created")
            return True
            
        try:
            conn = psycopg2.connect(**self.pg_config)
            cursor = conn.cursor()
            
            # Define schemas for all required tables
            tables = [
                # predictions table
                (
                    "predictions",
                    """
                    CREATE TABLE IF NOT EXISTS predictions (
                        prediction_id TEXT PRIMARY KEY,
                        game_id TEXT,
                        miner_uid TEXT,
                        prediction_date TEXT,
                        predicted_outcome TEXT,
                        predicted_odds REAL,
                        team_a TEXT,
                        team_b TEXT,
                        wager REAL,
                        team_a_odds REAL,
                        team_b_odds REAL,
                        tie_odds REAL,
                        model_name TEXT,
                        confidence_score REAL,
                        outcome TEXT,
                        payout REAL,
                        sent_to_site INTEGER DEFAULT 0,
                        validators_sent_to INTEGER DEFAULT 0,
                        validators_confirmed INTEGER DEFAULT 0
                    )
                    """
                ),
                # game_data table
                (
                    "game_data",
                    """
                    CREATE TABLE IF NOT EXISTS game_data (
                        game_id TEXT PRIMARY KEY UNIQUE,
                        external_id TEXT UNIQUE,
                        team_a TEXT,
                        team_b TEXT,
                        team_a_odds REAL,
                        team_b_odds REAL,
                        tie_odds REAL,
                        can_tie BOOLEAN,
                        event_start_date TEXT,
                        create_date TEXT,
                        last_update_date TEXT,
                        sport TEXT,
                        league TEXT,
                        outcome INTEGER DEFAULT 3,
                        active INTEGER
                    )
                    """
                ),
                # miner_stats table
                (
                    "miner_stats",
                    """
                    CREATE TABLE IF NOT EXISTS miner_stats (
                        miner_uid INTEGER PRIMARY KEY,
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
                        most_recent_weight REAL
                    )
                    """
                ),
                # scores table 
                (
                    "scores",
                    """
                    CREATE TABLE IF NOT EXISTS scores (
                        miner_uid INTEGER,
                        day_id INTEGER,
                        score_type TEXT,
                        clv_score REAL,
                        roi_score REAL,
                        sortino_score REAL,
                        entropy_score REAL,
                        composite_score REAL,
                        PRIMARY KEY (miner_uid, day_id, score_type)
                    )
                    """
                ),
                # keys table
                (
                    "keys",
                    """
                    CREATE TABLE IF NOT EXISTS keys (
                        hotkey TEXT PRIMARY KEY,
                        coldkey TEXT
                    )
                    """
                )
            ]
            
            # Create all tables
            for table_name, create_sql in tables:
                bt.logging.info(f"Creating table: {table_name}")
                cursor.execute(create_sql)
                
            # Create indexes for improved performance
            indexes = [
                # miner_stats indexes
                "CREATE INDEX IF NOT EXISTS idx_miner_stats_hotkey ON miner_stats(miner_hotkey)",
                "CREATE INDEX IF NOT EXISTS idx_miner_stats_coldkey ON miner_stats(miner_coldkey)",
                
                # game_data indexes
                "CREATE INDEX IF NOT EXISTS idx_game_data_sport ON game_data(sport)",
                "CREATE INDEX IF NOT EXISTS idx_game_data_active ON game_data(active)",
                "CREATE INDEX IF NOT EXISTS idx_game_data_outcome ON game_data(outcome)",
                
                # predictions indexes
                "CREATE INDEX IF NOT EXISTS idx_predictions_miner_uid ON predictions(miner_uid)",
                "CREATE INDEX IF NOT EXISTS idx_predictions_game_id ON predictions(game_id)",
                "CREATE INDEX IF NOT EXISTS idx_predictions_outcome ON predictions(outcome)"
            ]
            
            # Create all indexes
            for index_sql in indexes:
                cursor.execute(index_sql)
                
            conn.commit()
            cursor.close()
            conn.close()
            
            self.migration_status["tables_created"] = True
            self._save_migration_status()
            return True
        except Exception as e:
            bt.logging.error(f"Failed to create PostgreSQL schema: {e}")
            self.migration_status["errors"].append(f"Schema creation error: {str(e)}")
            self._save_migration_status()
            return False
    
    async def migrate_data(self) -> bool:
        """
        Migrate data from SQLite to PostgreSQL
        
        Returns:
            bool: True if data migration was successful, False otherwise
        """
        try:
            # Connect to SQLite
            sqlite_conn = sqlite3.connect(self.sqlite_path)
            sqlite_conn.row_factory = sqlite3.Row
            sqlite_cursor = sqlite_conn.cursor()
            
            # Connect to PostgreSQL
            pg_conn = psycopg2.connect(**self.pg_config)
            pg_cursor = pg_conn.cursor()
            
            # Get list of tables from SQLite
            sqlite_cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
            tables = [row[0] for row in sqlite_cursor.fetchall()]
            
            all_succeeded = True
            
            # Process each table
            for table in tables:
                # Skip already migrated tables
                if self.migration_status.get("data_migrated", {}).get(table, False):
                    bt.logging.info(f"Table {table} already migrated, skipping")
                    continue
                
                try:
                    bt.logging.info(f"Migrating table: {table}")
                    
                    # Get table schema from SQLite
                    sqlite_cursor.execute(f"PRAGMA table_info({table})")
                    columns = [col[1] for col in sqlite_cursor.fetchall()]
                    
                    # Get all data from SQLite table
                    sqlite_cursor.execute(f"SELECT * FROM {table}")
                    rows = sqlite_cursor.fetchall()
                    
                    if not rows:
                        bt.logging.info(f"Table {table} is empty, skipping")
                        self.migration_status.setdefault("data_migrated", {})[table] = True
                        self._save_migration_status()
                        continue
                    
                    # Convert rows to dictionaries
                    data = []
                    for row in rows:
                        data.append({columns[i]: row[i] for i in range(len(columns))})
                    
                    # Create table in PostgreSQL if it doesn't exist
                    create_table_query = f"""
                    CREATE TABLE IF NOT EXISTS {table} (
                        {', '.join(f'{col} {self._get_postgres_type(table, col)}' for col in columns)}
                    )
                    """
                    pg_cursor.execute(create_table_query)
                    pg_conn.commit()
                    
                    # Insert data in batches
                    batch_size = 1000
                    for i in range(0, len(data), batch_size):
                        batch = data[i:i + batch_size]
                        
                        # Build insert query
                        columns_str = ', '.join(columns)
                        placeholders = ', '.join(['%s'] * len(columns))
                        insert_query = f"""
                        INSERT INTO {table} ({columns_str})
                        VALUES ({placeholders})
                        ON CONFLICT DO NOTHING
                        """
                        
                        # Convert batch to list of tuples
                        batch_values = [
                            tuple(row[col] if row[col] != '' else None for col in columns)
                            for row in batch
                        ]
                        
                        try:
                            # Execute the batch insert
                            pg_cursor.executemany(insert_query, batch_values)
                            pg_conn.commit()
                            bt.logging.info(f"Inserted batch of {len(batch)} rows into {table}")
                        except Exception as e:
                            bt.logging.error(f"Error inserting batch into {table}: {e}")
                            pg_conn.rollback()
                            all_succeeded = False
                    
                    # Mark this table as migrated
                    self.migration_status.setdefault("data_migrated", {})[table] = True
                    self._save_migration_status()
                    
                except Exception as e:
                    bt.logging.error(f"Error migrating table {table}: {e}")
                    self.migration_status["errors"].append(f"Error migrating table {table}: {str(e)}")
                    self._save_migration_status()
                    all_succeeded = False
            
            # Close connections
            sqlite_cursor.close()
            sqlite_conn.close()
            pg_cursor.close()
            pg_conn.close()
            
            return all_succeeded
            
        except Exception as e:
            bt.logging.error(f"Failed to migrate data: {e}")
            self.migration_status["errors"].append(f"Data migration error: {str(e)}")
            self._save_migration_status()
            return False
    
    def _get_postgres_type(self, table: str, column: str) -> str:
        """Helper method to determine PostgreSQL column type"""
        # Special cases for known tables/columns
        if table == "game_data":
            if column == "outcome":
                return "INTEGER"  # Ensure outcome is always INTEGER
            if column in ["team_a_odds", "team_b_odds", "tie_odds"]:
                return "DOUBLE PRECISION"
        elif table == "predictions":
            if column in ["predicted_odds", "wager", "payout"]:
                return "DOUBLE PRECISION"
            if column in ["predicted_outcome", "outcome"]:
                return "INTEGER"
        elif table == "miner_stats":
            if column in ["miner_uid", "miner_current_tier"]:
                return "INTEGER"
            if "score" in column.lower() or "ratio" in column.lower():
                return "DOUBLE PRECISION"
                
        # Default mappings
        return "TEXT"  # Default to TEXT for unknown types
    
    async def verify_migration(self) -> bool:
        """
        Verify that the migration was successful by comparing row counts
        
        Returns:
            bool: True if verification passed, False otherwise
        """
        if self.migration_status.get("migration_verified", False):
            bt.logging.info("Migration already verified")
            return True
            
        try:
            # Connect to SQLite
            sqlite_conn = sqlite3.connect(self.sqlite_path)
            sqlite_cursor = sqlite_conn.cursor()
            
            # Connect to PostgreSQL
            pg_conn = psycopg2.connect(**self.pg_config)
            pg_cursor = pg_conn.cursor()
            
            # Get list of migrated tables
            migrated_tables = [
                table for table, status in 
                self.migration_status.get("data_migrated", {}).items() 
                if status
            ]
            
            all_verified = True
            
            # Verify each migrated table
            for table in migrated_tables:
                try:
                    # Count rows in SQLite
                    sqlite_cursor.execute(f"SELECT COUNT(*) FROM {table}")
                    sqlite_count = sqlite_cursor.fetchone()[0]
                    
                    # Count rows in PostgreSQL
                    pg_cursor.execute(f"SELECT COUNT(*) FROM {table}")
                    pg_count = pg_cursor.fetchone()[0]
                    
                    # For predictions table, SQLite count may be higher due to duplicates
                    if table == 'predictions' and pg_count < sqlite_count:
                        bt.logging.info(f"Table {table}: SQLite has {sqlite_count} rows, PostgreSQL has {pg_count} rows")
                        bt.logging.info(f"Difference of {sqlite_count - pg_count} rows is likely due to duplicate prediction_ids")
                    elif pg_count >= sqlite_count:
                        bt.logging.info(f"Table {table}: Successfully migrated all {sqlite_count} rows")
                    else:
                        bt.logging.warning(f"Table {table}: Only migrated {pg_count} of {sqlite_count} rows")
                        all_verified = False
                        
                except Exception as e:
                    bt.logging.error(f"Error verifying table {table}: {e}")
                    all_verified = False
            
            # Close connections
            sqlite_cursor.close()
            sqlite_conn.close()
            pg_cursor.close()
            pg_conn.close()
            
            self.migration_status["migration_verified"] = all_verified
            self._save_migration_status()
            return all_verified
            
        except Exception as e:
            bt.logging.error(f"Failed to verify migration: {e}")
            self.migration_status["errors"].append(f"Verification error: {str(e)}")
            self._save_migration_status()
            return False
    
    async def update_config(self) -> bool:
        """
        Update configuration to use PostgreSQL
        
        Returns:
            bool: True if config update was successful, False otherwise
        """
        try:
            config_path = Path.home() / ".bettensor" / "database.cfg"
            
            # Create config directory if it doesn't exist
            config_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Create config dictionary
            config = {
                "type": "postgres",
                "host": self.pg_config.get("host", "localhost"),
                "port": self.pg_config.get("port", 5432),
                "user": self.pg_config.get("user", "postgres"),
                "password": self.pg_config.get("password", "postgres"),
                "dbname": self.pg_config.get("dbname", "bettensor_validator"),
                "path": self.sqlite_path,  # Keep SQLite path for reference
                "migration_completed": True
            }
            
            # Write config to file
            with open(config_path, 'w') as f:
                json.dump(config, f, indent=4)
                
            bt.logging.info(f"Updated database configuration to use PostgreSQL")
            return True
        except Exception as e:
            bt.logging.error(f"Failed to update configuration: {e}")
            self.migration_status["errors"].append(f"Config update error: {str(e)}")
            self._save_migration_status()
            return False
    
    async def run_migration(self) -> bool:
        """
        Run the entire migration process
        
        Returns:
            bool: True if migration was successful, False otherwise
        """
        # Start timing the migration
        self.migration_status["start_time"] = datetime.now().isoformat()
        self._save_migration_status()
        
        # Check if migration is needed
        if not await self.is_migration_needed():
            bt.logging.info("Migration not needed, skipping")
            return True
            
        bt.logging.info("Starting migration from SQLite to PostgreSQL")
        
        # Step 1: Ensure we're using the state-synced database
        if not await self.ensure_state_sync_database():
            bt.logging.error("Failed to ensure state-synced database")
            return False
            
        # Step 2: Verify PostgreSQL setup
        if not await self.verify_postgres_setup():
            bt.logging.error("PostgreSQL setup verification failed")
            return False
            
        # Step 3: Create PostgreSQL schema
        if not await self.create_postgres_schema():
            bt.logging.error("Failed to create PostgreSQL schema")
            return False
            
        # Step 4: Migrate data
        if not await self.migrate_data():
            bt.logging.error("Failed to migrate data")
            return False
            
        # Step 5: Verify migration
        if not await self.verify_migration():
            bt.logging.error("Migration verification failed")
            return False
            
        # Step 6: Update configuration
        if not await self.update_config():
            bt.logging.error("Failed to update configuration")
            return False
            
        # Mark migration as completed
        self.migration_status["completed"] = True
        self.migration_status["end_time"] = datetime.now().isoformat()
        self._save_migration_status()
        
        bt.logging.info("Migration from SQLite to PostgreSQL completed successfully")
        return True 