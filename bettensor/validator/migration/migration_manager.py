# Add necessary imports
from pathlib import Path
import bittensor as bt
import sqlite3
import psycopg2
import os
import json
from datetime import datetime, timezone, timedelta
import asyncio
import time
import traceback
from typing import Any, Optional
import dateutil.parser as parser
from sqlalchemy import create_engine
from bettensor.validator.database.schema import metadata # Import SQLAlchemy metadata
from urllib.parse import quote_plus
import configparser # Import configparser
from bettensor.validator.database.postgres_database_manager import PostgresDatabaseManager # Changed to PostgresDatabaseManager
from bettensor.validator.utils.state_sync import StateSync


# Helper function for safe type conversion
def safe_convert(value: Any, target_type: type, default: Any = None) -> Any:
    if value is None or value == '':
        return default
    try:
        if target_type == bool:
            # Handle common boolean representations
            if isinstance(value, str):
                if value.lower() in ['true', '1', 't', 'yes', 'y']:
                    return True
                elif value.lower() in ['false', '0', 'f', 'no', 'n', '']:
                    return False
            # Try converting via int first, fallback to direct bool
            try:
                return bool(int(value)) 
            except ValueError:
                 return bool(value)
        elif target_type == datetime:
            # Attempt to parse various datetime string formats
            if isinstance(value, str):
                 # Remove 'Z' if present and replace with +00:00
                if value.endswith('Z'):
                    value = value[:-1] + '+00:00'
                 
                # Define potential formats
                formats_to_try = [
                    "%Y-%m-%d %H:%M:%S.%f%z", # Format with space separator and timezone
                    "%Y-%m-%dT%H:%M:%S.%f%z", # ISO format with T separator and timezone
                    "%Y-%m-%d %H:%M:%S%z",   # Format with space, no microseconds
                    "%Y-%m-%dT%H:%M:%S%z",   # ISO format with T, no microseconds
                    "%Y-%m-%d %H:%M:%S.%f", # Naive datetime with space
                    "%Y-%m-%dT%H:%M:%S.%f", # Naive datetime with T
                    "%Y-%m-%d %H:%M:%S",     # Naive datetime, no microseconds
                    "%Y-%m-%dT%H:%M:%S",     # Naive datetime with T, no microseconds
                    "%Y-%m-%d"             # Date only
                ]
                
                dt_obj = None
                for fmt in formats_to_try:
                    try:
                        dt_obj = datetime.strptime(value, fmt)
                        # If format includes timezone (%z), it will be aware
                        # If format doesn't include timezone, make it UTC aware if possible
                        if dt_obj.tzinfo is None and timezone.utc: # Check if timezone.utc is available
                             dt_obj = dt_obj.replace(tzinfo=timezone.utc)
                        break # Stop trying formats once one succeeds
                    except ValueError:
                        continue # Try the next format
                        
                if dt_obj is None:
                    bt.logging.warning(f"Could not parse date string '{value}' with known formats. Returning default: {default}")
                    return default
                return dt_obj
                    
            elif isinstance(value, (int, float)):
                 # Handle timestamps (assume UTC)
                 try:
                     # Use timezone.utc if available, otherwise rely on system default
                     tz = timezone.utc if timezone.utc else None 
                     return datetime.fromtimestamp(value, tz=tz)
                 except Exception:
                     bt.logging.warning(f"Could not convert timestamp: {value}. Returning default: {default}")
                     return default
            # If it's already a datetime object, return it
            elif isinstance(value, datetime):
                 return value
        elif target_type == float:
            return float(value)
        elif target_type == int:
            # Handle potential float strings being converted to int
            if isinstance(value, str) and '.' in value:
                 return int(float(value))
            return int(value)
        elif target_type == str:
             return str(value)
             
        # Fallback for other types
        return target_type(value)
    except (ValueError, TypeError, Exception) as e:
        bt.logging.warning(f"Conversion error for value '{value}' to type '{target_type.__name__}': {e}. Returning default: {default}")
        return default

# Type mapping dictionary (SQLite column -> target PG type)
# Add more specific mappings as needed
TYPE_MAPPING = {
    'BOOLEAN': bool,
    'INTEGER': int,
    'REAL': float,
    'FLOAT': float,
    'DOUBLE PRECISION': float,
    'TEXT': str,
    'VARCHAR': str,
    'TIMESTAMP': datetime,
    'TIMESTAMP WITH TIME ZONE': datetime,
    'DATE': datetime # Assuming DATE from SQLite should map to datetime
}

# Helper function to filter pg_config for psycopg2
def _filter_psycopg2_config(pg_config: dict) -> dict:
    valid_keys = {'host', 'port', 'user', 'password', 'dbname', 'database', 'connection_factory', 'cursor_factory', 'async_', 'sslmode', 'sslkey', 'sslcert', 'sslrootcert', 'sslcrl', 'requirepeer', 'krbsrvname', 'gsslib', 'connect_timeout', 'client_encoding', 'options', 'application_name', 'fallback_application_name', 'keepalives', 'keepalives_idle', 'keepalives_interval', 'keepalives_count', 'tcp_user_timeout', 'replication', 'target_session_attrs'}
    
    # Start with only valid keys from original config
    filtered_config = {k: v for k, v in pg_config.items() if k in valid_keys}
    
    # Ensure 'database' is the preferred key, remove 'dbname' if both exist
    if 'database' not in filtered_config and 'dbname' in filtered_config:
         filtered_config['database'] = filtered_config['dbname'] # Use dbname value if database key is missing
         
    # Explicitly remove 'dbname' if 'database' key is now present 
    if 'database' in filtered_config and 'dbname' in filtered_config:
        del filtered_config['dbname']
        
    return filtered_config

class MigrationManager:
    """Manages database migrations between SQLite and PostgreSQL."""
    
    def __init__(self, sqlite_path: str, pg_config: dict, db_manager: PostgresDatabaseManager, state_sync_manager: StateSync):
        """
        Initialize the migration manager.
        
        Args:
            sqlite_path: Path to SQLite database
            pg_config: PostgreSQL configuration dictionary (used by psycopg2)
            db_manager: Instance of the PostgresDatabaseManager.
            state_sync_manager: Instance of the StateSync manager.
        """
        self.sqlite_path = sqlite_path
        self.pg_config = pg_config
        self.db_manager = db_manager
        self.state_sync_manager = state_sync_manager
        self.migration_status = {
            "started": False,
            "completed": False,
            "errors": [],
            "data_migrated": {},
            "last_error": None,
            "last_attempt": None
        }
        self._load_migration_status()
        
    def _load_migration_status(self):
        """Load migration status from file."""
        try:
            status_file = Path(self.sqlite_path).parent / "migration_status.json"
            if status_file.exists():
                with open(status_file, "r") as f:
                    self.migration_status.update(json.load(f))
        except Exception as e:
            bt.logging.error(f"Error loading migration status: {e}")
            
    def _save_migration_status(self):
        """Save migration status to file."""
        try:
            status_file = Path(self.sqlite_path).parent / "migration_status.json"
            with open(status_file, "w") as f:
                json.dump(self.migration_status, f)
        except Exception as e:
            bt.logging.error(f"Error saving migration status: {e}")
            
    async def check_migration_needed(self) -> bool:
        """
        Check if migration is needed based *only* on status file and SQLite source.
        Excludes the force flag check, which is handled in migrate().
        
        Returns:
            bool: True if migration source exists and status is not 'completed', False otherwise.
        """
        # 1. Check if migration was already marked as completed
        if self.migration_status.get("completed", False):
            bt.logging.info("Migration already marked as completed in status file.")
            return False
            
        # 2. Check if SQLite database exists and has data
        if not os.path.exists(self.sqlite_path):
            bt.logging.info("No SQLite database found. Marking migration as completed (nothing to migrate).")
            self.migration_status["completed"] = True
            self._save_migration_status()
            return False
            
        sqlite_size = os.path.getsize(self.sqlite_path)
        if sqlite_size < 1024:  # Less than 1KB
            bt.logging.info("SQLite database is effectively empty. Marking migration as completed.")
            self.migration_status["completed"] = True
            self._save_migration_status()
            return False
            
        # 3. If SQLite exists, has data, and status is not completed, then migration is needed.
        bt.logging.info("SQLite source exists and migration not marked as completed. Migration is needed.")
        return True

    async def _drop_all_pg_tables(self) -> None:
        """Drops all tables in the configured PostgreSQL database using the db_manager."""
        bt.logging.warning("Dropping and recreating all PostgreSQL tables via db_manager.initialize(force=True) as requested by force_db_reset...")
        try:
            # Ensure the db_manager is initialized before attempting to drop tables
            if not self.db_manager or not self.db_manager.engine:
                 bt.logging.warning("db_manager not initialized, attempting to initialize now before dropping tables.")
                 await self.db_manager.initialize() # Attempt initialization
            
            # Check again after attempting initialization
            if self.db_manager and self.db_manager.engine:
                # Use initialize(force=True) which handles dropping and recreating schema
                await self.db_manager.initialize(force=True) 
                bt.logging.info("Successfully dropped and recreated all PostgreSQL tables via initialize(force=True).")
            else:
                bt.logging.error("Failed to initialize db_manager. Cannot drop/recreate PostgreSQL tables.")
                # Potentially raise an exception or handle this error state appropriately
        except Exception as e:
            bt.logging.error(f"Error dropping/recreating PostgreSQL tables via initialize(force=True): {e}")
            bt.logging.error(traceback.format_exc())
            # Decide how to handle this error - maybe raise it?
            raise # Re-raise the exception to signal failure

    async def _get_postgres_column_types(self, pg_cursor) -> dict:
        """Fetches column names and data types from PostgreSQL information_schema."""
        column_types = {}
        try:
            # Query information_schema for tables defined in our SQLAlchemy metadata
            table_names_in_schema = list(metadata.tables.keys())
            if not table_names_in_schema:
                bt.logging.warning("No tables found in SQLAlchemy metadata. Cannot fetch PG column types.")
                return {}
                
            # Format table names for SQL IN clause
            table_name_placeholders = ",".join(["%s"] * len(table_names_in_schema))
            
            query = f"""
                SELECT table_name, column_name, data_type 
                FROM information_schema.columns 
                WHERE table_schema = 'public' AND table_name IN ({table_name_placeholders});
            """
            
            pg_cursor.execute(query, table_names_in_schema)
            rows = pg_cursor.fetchall()
            
            for row in rows:
                table_name, column_name, data_type = row
                if table_name not in column_types:
                    column_types[table_name] = {}
                column_types[table_name][column_name] = data_type.upper() # Store type in uppercase for consistency
            
            bt.logging.debug(f"Successfully fetched PG column types for tables: {list(column_types.keys())}")
            return column_types
            
        except psycopg2.Error as e:
            bt.logging.error(f"Error fetching PostgreSQL column types: {e}")
            # Depending on severity, you might want to raise or return empty dict
            raise # Re-raise to halt migration if schema info is unavailable
        except Exception as e:
            bt.logging.error(f"Unexpected error fetching PostgreSQL column types: {e}")
            raise

    async def _transfer_data_from_sqlite(self) -> None:
        """Handles the core logic of transferring data from SQLite to PostgreSQL."""
        bt.logging.info("Starting data transfer from SQLite to PostgreSQL...")
        sqlite_conn = None
        pg_conn = None
        pg_cursor = None
        uid_to_hotkey_map = {} # To store uid -> hotkey mapping

        try:
            # 1. Connect to SQLite
            sqlite_conn = sqlite3.connect(self.sqlite_path)
            sqlite_cursor = sqlite_conn.cursor()
            bt.logging.info("Connected to SQLite database.")

            # --- Pre-fetch miner UID to Hotkey mapping from SQLite miner_stats --- 
            try:
                sqlite_cursor.execute("SELECT miner_uid, miner_hotkey FROM miner_stats")
                rows = sqlite_cursor.fetchall()
                uid_to_hotkey_map = {row[0]: row[1] for row in rows if row[0] is not None and row[1] is not None}
                bt.logging.info(f"Successfully fetched {len(uid_to_hotkey_map)} UID-to-Hotkey mappings from SQLite miner_stats.")
            except sqlite3.Error as e:
                bt.logging.error(f"Failed to fetch UID-Hotkey map from SQLite miner_stats: {e}. Migration cannot reliably proceed.")
                raise # Re-raise critical error
            # --- End pre-fetch --- 

            # 2. Connect to PostgreSQL
            try:
                connect_config = _filter_psycopg2_config(self.pg_config)
                pg_conn = psycopg2.connect(**connect_config)
                pg_cursor = pg_conn.cursor()
                bt.logging.info("Connected to PostgreSQL database.")
            except psycopg2.Error as db_err:
                bt.logging.error(f"Could not connect to PostgreSQL: {db_err}. Migration cannot proceed.")
                if sqlite_conn: sqlite_conn.close() # Close SQLite connection on PG failure
                raise # Re-raise to be caught by the outer try/except
                
            # 3. Get PG column types for validation
            pg_column_types = await self._get_postgres_column_types(pg_cursor)
            bt.logging.debug(f"Fetched PostgreSQL column types: {pg_column_types}")
                
            # 4. Get list of tables from SQLite
            sqlite_cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            sqlite_table_names = {table[0] for table in sqlite_cursor.fetchall()}
            bt.logging.debug(f"Found SQLite tables: {sqlite_table_names}")
            
            # Exclude SQLite internal tables
            sqlite_table_names = {t for t in sqlite_table_names if not t.startswith('sqlite_')}

            # Define migration order (ensure miner_stats is processed if needed, though map helps)
            # entropy_game_pools must come before entropy_predictions due to FK constraint
            ordered_tables = []
            preferred_order = ['miner_stats', 'entropy_game_pools', 'game_data', 'predictions', 'entropy_predictions'] 
            remaining_tables = set(sqlite_table_names)

            for table in preferred_order:
                if table in remaining_tables:
                    ordered_tables.append(table)
                    remaining_tables.remove(table)
            
            ordered_tables.extend(sorted(list(remaining_tables)))
            bt.logging.info(f"Processing tables in order: {ordered_tables}")

            total_rows_migrated = 0
            self.migration_status["data_migrated"] = {} # Reset stats for this run

            # 5. Iterate through SQLite tables in the determined order
            for table_name in ordered_tables:
                bt.logging.info(f"Processing table: {table_name}")
                
                if table_name not in pg_column_types:
                    bt.logging.warning(f"Table '{table_name}' exists in SQLite but not defined in PostgreSQL schema (via SQLAlchemy metadata). Skipping.")
                    self.migration_status["data_migrated"][table_name] = {"status": "skipped (not in PG schema)", "rows": 0}
                    continue
                
                sqlite_cursor.execute(f"PRAGMA table_info('{table_name}')")
                columns_info = sqlite_cursor.fetchall()
                sqlite_column_names = [col[1] for col in columns_info]
                
                pg_table_columns = pg_column_types.get(table_name, {})
                pg_column_names_set = set(pg_table_columns.keys())
                
                # Identify columns present in SQLite but potentially missing/different in PG schema
                ignored_sqlite_columns = [col for col in sqlite_column_names if col not in pg_column_names_set]
                if ignored_sqlite_columns:
                    bt.logging.warning(f"Table '{table_name}': SQLite columns {ignored_sqlite_columns} not in PG schema def or will be ignored.")

                # Determine the final list of columns to migrate TO in PostgreSQL
                # Start with columns present in BOTH SQLite and PG schema
                target_pg_columns = [col for col in sqlite_column_names if col in pg_column_names_set]
                
                # --- Check if miner_hotkey is required by PG but not in SQLite columns --- 
                hotkey_required_in_pg = 'miner_hotkey' in pg_column_names_set
                hotkey_in_sqlite = 'miner_hotkey' in sqlite_column_names
                # Updated tables list for hotkey lookup
                needs_hotkey_lookup = hotkey_required_in_pg and table_name in ('predictions', 'entropy_predictions', 'entropy_miner_scores', 'scores')
                add_hotkey_column = hotkey_required_in_pg and not hotkey_in_sqlite and needs_hotkey_lookup

                if add_hotkey_column:
                    bt.logging.info(f"Table '{table_name}': Adding 'miner_hotkey' column to migration target list as it's required by PG but not found in SQLite PRAGMA info.")
                    target_pg_columns.append('miner_hotkey')
                # --- End hotkey check --- 

                # --- Exclude auto-generated ID columns for specific tables --- 
                if table_name == 'scores' and 'score_id' in target_pg_columns:
                    bt.logging.info(f"Table '{table_name}': Excluding auto-generated 'score_id' column from migration target list.")
                    target_pg_columns.remove('score_id')
                # --- End exclude ID --- 
                
                if not target_pg_columns:
                    bt.logging.warning(f"Table '{table_name}': No common or addable columns found for migration. Skipping table.")
                    self.migration_status["data_migrated"][table_name] = {"status": "skipped (no matching columns)", "rows": 0}
                    continue
                    
                bt.logging.debug(f"Migrating TO PostgreSQL columns for table '{table_name}': {target_pg_columns}")
                
                # Fetch data ONLY for columns present in SQLite that are also in target_pg_columns (excluding potentially added miner_hotkey)
                fetch_sqlite_columns = [col for col in target_pg_columns if col in sqlite_column_names]
                column_selector = ", ".join([f'"{col}"' for col in fetch_sqlite_columns])
                sqlite_cursor.execute(f"SELECT {column_selector} FROM '{table_name}'")
                
                rows_migrated_for_table = 0
                skipped_rows_for_table = 0
                batch_size = 1000 
                batch_values = []
                
                # Get indices for lookup columns if needed
                uid_index_sqlite = fetch_sqlite_columns.index('miner_uid') if 'miner_uid' in fetch_sqlite_columns else -1
                hotkey_index_sqlite = fetch_sqlite_columns.index('miner_hotkey') if hotkey_in_sqlite else -1
                hotkey_index_target = target_pg_columns.index('miner_hotkey') if hotkey_required_in_pg else -1 # Index in the final PG insert list

                while True:
                    rows = sqlite_cursor.fetchmany(batch_size)
                    if not rows:
                        break
                        
                    batch_values.clear()
                    
                    for sqlite_row in rows:
                        processed_row = [None] * len(target_pg_columns) # Initialize with Nones for the target size
                        valid_row = True
                        current_uid = None
                        
                        # Map SQLite data to the target PG positions
                        for i, sqlite_value in enumerate(sqlite_row):
                            sqlite_col_name = fetch_sqlite_columns[i]
                            target_index = target_pg_columns.index(sqlite_col_name)
                            
                            # Store UID for potential hotkey lookup
                            if i == uid_index_sqlite:
                                current_uid = sqlite_value # Store the raw UID value before conversion

                            pg_type_str = pg_table_columns.get(sqlite_col_name, 'TEXT')
                            target_type = TYPE_MAPPING.get(pg_type_str.upper(), str)
                            converted_value = safe_convert(sqlite_value, target_type)
                            
                            # Handle potential conversion issues (excluding hotkey for now)
                            if sqlite_col_name != 'miner_hotkey' and sqlite_value is not None and converted_value is None and target_type != type(None):
                                bt.logging.debug(f"Potential conversion issue for table '{table_name}', column '{sqlite_col_name}', value '{sqlite_value}' -> None")
                                
                            processed_row[target_index] = converted_value
                            
                        # --- Perform miner_hotkey lookup/assignment if needed --- 
                        if needs_hotkey_lookup and hotkey_index_target != -1:
                            looked_up_hotkey = None
                            if current_uid is not None:
                                looked_up_hotkey = uid_to_hotkey_map.get(current_uid)
                                
                            if looked_up_hotkey is not None:
                                processed_row[hotkey_index_target] = looked_up_hotkey
                            else:
                                # Check if hotkey was present in SQLite but was NULL
                                original_sqlite_hotkey = sqlite_row[hotkey_index_sqlite] if hotkey_in_sqlite and hotkey_index_sqlite != -1 else None
                                if hotkey_in_sqlite and original_sqlite_hotkey is None:
                                    # Hotkey was explicitly NULL in SQLite, use empty string
                                     processed_row[hotkey_index_target] = ''
                                elif current_uid is not None:
                                    # UID exists but wasn't in the map
                                    processed_row[hotkey_index_target] = '' # Use empty string as default
                                else:
                                    # UID itself was missing or null, use empty string
                                    processed_row[hotkey_index_target] = ''
                        # --- End hotkey lookup --- 
                                
                        # Final check for None in NOT NULL columns (though lookup should handle hotkey)
                        # This is a safeguard, might need more specific checks based on actual schema constraints
                        for idx, val in enumerate(processed_row):
                             col_name = target_pg_columns[idx]
                             # A more robust check would involve inspecting PG schema for NOT NULL constraints
                             # For now, we only explicitly handled miner_hotkey
                             if val is None and col_name == 'miner_hotkey' and hotkey_required_in_pg: # Double check hotkey case
                                 bt.logging.error(f"Logic Error: miner_hotkey is still None before insert for table '{table_name}'. Row: {sqlite_row}. Processed: {processed_row}")
                                 processed_row[idx] = '' # Force empty string again
                                 
                        if valid_row:
                            batch_values.append(tuple(processed_row))
                        else:
                            skipped_rows_for_table += 1
                            # Log skipped row details if needed
                            # bt.logging.warning(f"Skipping row in table '{table_name}' due to processing errors: {processed_row}")
                            
                    # Insert batch into PostgreSQL
                    if batch_values:
                        # --- ADDED: FK Check for entropy_predictions --- 
                        if table_name == 'entropy_predictions':
                            bt.logging.debug(f"Performing FK check for entropy_predictions batch...")
                            # Get indices for game_id and outcome within the batch_values tuples
                            try:
                                game_id_idx = target_pg_columns.index('game_id')
                                outcome_idx = target_pg_columns.index('outcome')
                            except ValueError:
                                bt.logging.error(f"Could not find 'game_id' or 'outcome' columns in target_pg_columns for entropy_predictions. Skipping FK check.")
                                # Proceed without FK check if columns aren't found (shouldn't happen)
                            else:
                                # Extract (game_id, outcome) pairs from the batch
                                batch_pairs = set((row[game_id_idx], row[outcome_idx]) for row in batch_values if row[game_id_idx] is not None and row[outcome_idx] is not None)
                                bt.logging.debug(f"Checking existence of {len(batch_pairs)} unique (game_id, outcome) pairs from batch.")
                                
                                if batch_pairs:
                                    # Query existing pairs in PostgreSQL
                                    existing_pairs_query = "SELECT game_id, outcome FROM entropy_game_pools WHERE (game_id, outcome) = ANY(%s)"
                                    pg_cursor.execute(existing_pairs_query, (list(batch_pairs),))
                                    existing_pairs_set = set(pg_cursor.fetchall())
                                    bt.logging.debug(f"Found {len(existing_pairs_set)} existing pairs in entropy_game_pools.")
                                    
                                    # Filter the batch
                                    original_batch_size = len(batch_values)
                                    filtered_batch = []
                                    skipped_rows_this_batch = 0
                                    for row in batch_values:
                                        pair = (row[game_id_idx], row[outcome_idx])
                                        if pair in existing_pairs_set:
                                            filtered_batch.append(row)
                                        else:
                                            skipped_rows_this_batch += 1
                                            bt.logging.warning(f"Skipping row for entropy_predictions due to missing FK pair {pair} in entropy_game_pools.")
                                            # Log the skipped row details if needed for debugging
                                            # bt.logging.debug(f"Skipped row data: {row}")
                                            
                                    batch_values = filtered_batch # Replace original batch with filtered one
                                    skipped_rows_for_table += skipped_rows_this_batch
                                    if skipped_rows_this_batch > 0:
                                         bt.logging.warning(f"Skipped {skipped_rows_this_batch} rows from batch due to FK violation for entropy_predictions.")
                        # --- END FK Check ---
                        
                        # Proceed with insertion only if batch is not empty after filtering
                        if not batch_values:
                            bt.logging.debug(f"Skipping insertion for table '{table_name}' as batch is empty after FK checks.")
                            continue # Skip to next batch/table

                        try:
                            placeholders = ",".join(["%s"] * len(target_pg_columns))
                            # Quote column names to handle reserved words
                            quoted_columns = ', '.join([f'"{col}"' for col in target_pg_columns])
                            insert_sql = f"INSERT INTO {table_name} ({quoted_columns}) VALUES ({placeholders}) ON CONFLICT DO NOTHING"
                            
                            pg_cursor.executemany(insert_sql, batch_values)
                            pg_conn.commit() # Commit after each successful batch
                            
                            rows_migrated_this_batch = len(batch_values) # Approximation
                            rows_migrated_for_table += rows_migrated_this_batch
                            total_rows_migrated += rows_migrated_this_batch
                            bt.logging.debug(f"Processed batch of {len(batch_values)} rows for table '{table_name}' (using ON CONFLICT DO NOTHING). Migrated count approx.")

                        except psycopg2.Error as insert_err:
                            pg_conn.rollback() # Rollback failed batch
                            bt.logging.error(f"Error inserting batch into '{table_name}': {insert_err}")
                            bt.logging.error(f"Failed batch data sample (first row): {batch_values[0] if batch_values else 'N/A'}")
                            skipped_rows_for_table += len(batch_values)
                            bt.logging.warning(f"Skipping batch of {len(batch_values)} rows for table '{table_name}' due to insertion error.")
                            
                bt.logging.info(f"Finished processing table '{table_name}'. Approx Rows Migrated: {rows_migrated_for_table}, Rows Skipped in Failed Batches: {skipped_rows_for_table}")
                self.migration_status["data_migrated"][table_name] = {
                    "status": "completed" if skipped_rows_for_table == 0 else "completed_with_errors",
                    "rows_migrated_approx": rows_migrated_for_table,
                    "rows_skipped_in_batches": skipped_rows_for_table
                }

            # Check overall completion status based on individual table results
            all_tables_completed = all(status.get("status", "").startswith("completed") for status in self.migration_status["data_migrated"].values())
            if all_tables_completed:
                bt.logging.info(f"Data migration completed. Total approx rows migrated across all tables: {total_rows_migrated}")
                self.migration_status["completed"] = True # Mark migration as completed
                self.migration_status["last_error"] = None
            else:
                bt.logging.warning(f"Data migration finished, but some tables had errors. Total approx rows migrated: {total_rows_migrated}")
                self.migration_status["completed"] = False # Mark as not fully completed due to errors
                # Keep the last specific error if one occurred during transfer
                if not self.migration_status.get("last_error"):
                     self.migration_status["last_error"] = "Migration completed with batch errors."

        except (sqlite3.Error, psycopg2.Error, Exception) as e:
            bt.logging.error(f"Migration failed: {e}")
            bt.logging.error(traceback.format_exc())
            self.migration_status["errors"] = self.migration_status.get("errors", []) + [str(e)]
            self.migration_status["last_error"] = str(e)
            # Rollback any pending PG transaction
            if pg_conn:
                try: pg_conn.rollback() 
                except Exception as rb_err: bt.logging.error(f"Error during rollback: {rb_err}")
            raise # Re-raise the exception after logging and status update
        finally:
            # Close connections
            if sqlite_conn: sqlite_conn.close()
            if pg_cursor: pg_cursor.close()
            if pg_conn: pg_conn.close()
            bt.logging.info("Database connections closed.")
            # Save status regardless of outcome
            self.migration_status["last_attempt"] = datetime.now(timezone.utc).isoformat()
            self._save_migration_status()

    async def migrate(self) -> bool:
        """
        Perform the migration from SQLite to PostgreSQL.
        Handles forced migration via setup.cfg.
        Checks for source data existence *after* any forced drop.
        
        Returns:
            bool: True if any migration action (forced drop or data transfer) occurred, False otherwise.
        """
        force_migration_flag = False
        migration_occurred = False # Track if any action was taken
        perform_data_transfer = False # Flag to track if data transfer should happen

        try:
            # --- Read setup.cfg for force_db_reset flag --- 
            config_parser = configparser.ConfigParser()
            try:
                config_parser.read('setup.cfg')
                if 'metadata' in config_parser:
                    force_migration_flag = config_parser.getboolean('metadata', 'force_db_reset', fallback=False)
            except Exception as cfg_err:
                 bt.logging.error(f"Error reading force_db_reset from setup.cfg: {cfg_err}. Assuming False.")
                 force_migration_flag = False
            # --- End setup.cfg read --- 

            # --- Handle Forced Migration FIRST --- 
            if force_migration_flag:
                bt.logging.warning("Force flag set: Dropping/recreating PostgreSQL schema.")
                try:
                    await self._drop_all_pg_tables() # This calls db_manager.initialize(force=True)
                    # Reset migration status since we wiped the DB
                    self.migration_status["completed"] = False 
                    self.migration_status["data_migrated"] = {} 
                    self.migration_status["started"] = False
                    self.migration_status["errors"] = [] # Clear errors on forced reset
                    self.migration_status["last_error"] = None
                    self._save_migration_status()
                    bt.logging.info("Migration status file reset due to forced table drop/recreate.")
                    migration_occurred = True # Indicate action was taken
                    
                    # --- Check SQLite source *AFTER* forced reset --- 
                    sqlite_exists_and_has_data = False
                    if os.path.exists(self.sqlite_path):
                        sqlite_size = os.path.getsize(self.sqlite_path)
                        if sqlite_size >= 1024:
                             sqlite_exists_and_has_data = True
                        else:
                             bt.logging.info("SQLite source exists but is empty (<1KB).")
                    else:
                        bt.logging.info("SQLite source does not exist.")
                        
                    if sqlite_exists_and_has_data:
                        bt.logging.info("Forced reset completed, and valid SQLite source data exists. Data transfer will proceed.")
                        perform_data_transfer = True # We need to transfer data to the new empty DB
                    else:
                        bt.logging.info("Forced reset completed, but no valid SQLite source data found. Skipping data transfer.")
                        # Mark as completed now, as the DB is reset and no data needs transfer
                        self.migration_status["completed"] = True 
                        self._save_migration_status()
                        
                except Exception as drop_err:
                    bt.logging.error(f"Error during forced table drop/recreate: {drop_err}. Migration cannot proceed safely.")
                    bt.logging.error(traceback.format_exc())
                    self.migration_status["last_error"] = f"Forced drop failed: {drop_err}"
                    self._save_migration_status()
                    return False # Return False on critical failure during forced drop
            
            # --- Handle Regular Migration (only if NOT forced) --- 
            else: 
                 needs_regular_migration = await self.check_migration_needed() # Checks status and SQLite
                 bt.logging.info(f"Regular Check: Migration Needed result (based on status file & SQLite source): {needs_regular_migration}")
                 if needs_regular_migration:
                     bt.logging.info("Regular migration required.")
                     perform_data_transfer = True

            # --- Perform Data Transfer (if flagged by either path) --- 
            if perform_data_transfer:
                bt.logging.info("Starting data transfer from SQLite to PostgreSQL...")
                migration_occurred = True # Data transfer counts as an action
                self.migration_status["started"] = True
                self.migration_status["last_attempt"] = datetime.now(timezone.utc).isoformat()
                self.migration_status["completed"] = False # Ensure completed is false before starting
                self.migration_status["data_migrated"] = {} # Reset specific table status
                self._save_migration_status() 
                
                try:
                    await self._transfer_data_from_sqlite() # This handles its own completion status update on success
                    
                    # Check completion status again after transfer attempt
                    if self.migration_status.get("completed", False):
                        bt.logging.info("Data transfer step completed successfully.")
                        # Return True as migration action (transfer) was successful
                        return True 
                    else:
                        bt.logging.error("Data transfer step failed or partially failed. See previous errors.")
                        # _transfer_data_from_sqlite should have set last_error and completed=False
                        return False # Return False as transfer failed
                except Exception as transfer_err:
                     bt.logging.error(f"Error during _transfer_data_from_sqlite call: {transfer_err}")
                     bt.logging.error(traceback.format_exc())
                     # Ensure status reflects failure
                     self.migration_status["completed"] = False
                     self.migration_status["last_error"] = f"Transfer failed: {transfer_err}"
                     self._save_migration_status()
                     return False # Transfer failed
            
            # --- No Action Case --- 
            # This path is reached if: 
            # - Not forced AND check_migration_needed was False OR
            # - Forced, but no SQLite data found (completion status handled above)
            if not migration_occurred:
                bt.logging.info("Migration not required (source data missing, already complete, or not forced and not needed). No action taken.")
                # Final check to ensure status is marked complete if needed
                if not self.migration_status.get("completed", False):
                     # We might need to check `check_migration_needed` again here in case status file was corrupt/missing initially
                     final_check_needed = await self.check_migration_needed() 
                     if not final_check_needed:
                         bt.logging.info("Marking migration as completed based on final checks.")
                         self.migration_status["completed"] = True
                         self._save_migration_status()
                         
                return False # Return False indicating no migration *action* was taken this run
            
            # If we get here, it means a forced drop occurred but no data transfer was performed (because no source data).
            # Return True because an action (the drop) did occur, and status was marked completed inside the force block.
            return True

        except Exception as e:
            # Catch-all for unexpected errors during the migrate() flow itself
            bt.logging.error(f"Unexpected error during migration process orchestration: {e}")
            bt.logging.error(traceback.format_exc())
            self.migration_status["last_error"] = f"Migration orchestration error: {str(e)}"
            self.migration_status["errors"] = self.migration_status.get("errors", []) + [f"Orchestration: {str(e)}"]
            self._save_migration_status()
            return False # Return False on overall failure
            