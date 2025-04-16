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
    
    def __init__(self, sqlite_path: str, pg_config: dict):
        """
        Initialize the migration manager.
        
        Args:
            sqlite_path: Path to SQLite database
            pg_config: PostgreSQL configuration dictionary (used by psycopg2)
        """
        self.sqlite_path = sqlite_path
        self.pg_config = pg_config
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
        Check if migration is needed (excluding the force flag).
        
        Returns:
            bool: True if migration is needed, False otherwise
        """
        try:
            # Check if migration was already completed
            if self.migration_status.get("completed", False):
                bt.logging.info("Migration already completed according to status file.")
                return False
                
            # Check if SQLite database exists and has data
            if not os.path.exists(self.sqlite_path):
                bt.logging.info("No SQLite database found, skipping migration")
                self.migration_status["completed"] = True
                self._save_migration_status()
                return False
                
            # Check SQLite database size
            sqlite_size = os.path.getsize(self.sqlite_path)
            if sqlite_size < 1024:  # Less than 1KB
                bt.logging.info("SQLite database is empty, skipping migration")
                self.migration_status["completed"] = True
                self._save_migration_status()
                return False
                
            # Connect to PostgreSQL to check if tables exist and have data
            try:
                # Filter config before connecting
                connect_config = _filter_psycopg2_config(self.pg_config)
                conn = psycopg2.connect(**connect_config)
                cursor = conn.cursor()
            except psycopg2.Error as db_err:
                 bt.logging.warning(f"Could not connect to PostgreSQL during check_migration_needed: {db_err}. Assuming migration is needed.")
                 # It's likely the DB doesn't exist or connection params are wrong.
                 # If DB doesn't exist, migration IS needed.
                 # If params are wrong, migration won't work anyway, but returning True is safer.
                 return True

            try:
                # Check if key tables exist and have data
                tables_to_check = ["game_data", "predictions", "miner_stats"]
                tables_exist = True
                tables_have_data = True
                
                for table in tables_to_check:
                    cursor.execute(f"""
                        SELECT EXISTS (
                            SELECT FROM information_schema.tables 
                            WHERE table_name = %s
                        )
                    """, (table,))
                    
                    if not cursor.fetchone()[0]:
                        tables_exist = False
                        break
                        
                    cursor.execute(f"SELECT COUNT(*) FROM {table}")
                    if cursor.fetchone()[0] == 0:
                        tables_have_data = False
                        
                if tables_exist and tables_have_data:
                    bt.logging.info("PostgreSQL database already has data, skipping regular migration.")
                    # Don't mark as completed here, force might still apply
                    # self.migration_status["completed"] = True 
                    # self._save_migration_status()
                    return False
                    
                return True # Needs migration if PG tables don't exist or are empty
                
            finally:
                if 'cursor' in locals() and cursor: cursor.close()
                if 'conn' in locals() and conn: conn.close()
                
        except Exception as e:
            bt.logging.error(f"Error checking migration status: {e}")
            self.migration_status["last_error"] = str(e)
            self._save_migration_status()
            return True # Assume needed if check fails
            
    async def migrate(self) -> bool:
        """
        Perform the migration from SQLite to PostgreSQL.
        Handles forced migration via setup.cfg.
        
        Returns:
            bool: True if migration was successful or forced drop occurred, False otherwise
        """
        force_migration_flag = False
        migration_occurred = False # Track if any migration action was taken
        try:
            # --- Read setup.cfg for force_db_reset flag --- 
            config_parser = configparser.ConfigParser()
            try:
                config_parser.read('setup.cfg')
                if 'metadata' in config_parser:
                    # Use getboolean for robust True/False parsing
                    force_migration_flag = config_parser.getboolean('metadata', 'force_db_reset', fallback=False)
            except Exception as cfg_err:
                 bt.logging.error(f"Error reading force_db_reset from setup.cfg: {cfg_err}")
                 force_migration_flag = False # Default to false if file read fails
            # --- End setup.cfg read --- 

            if force_migration_flag:
                bt.logging.warning("setup.cfg flag [metadata]force_db_reset is set. Attempting to drop all tables.")
                try:
                    # Need a synchronous SQLAlchemy engine for metadata.drop_all
                    sync_conn_url = (
                        f"postgresql+psycopg2://{self.pg_config.get('user', 'postgres')}"
                        f":{quote_plus(self.pg_config.get('password', ''))}@"
                        f"{self.pg_config.get('host', 'localhost')}:{self.pg_config.get('port', 5432)}/"
                        # Connect to the specific database to drop tables within it
                        f"{self.pg_config.get('database', self.pg_config.get('dbname'))}"
                    )
                    sync_engine = create_engine(sync_conn_url)
                    with sync_engine.connect() as connection:
                        metadata.drop_all(bind=connection) 
                    sync_engine.dispose()
                    bt.logging.info("Successfully dropped tables due to force_db_reset flag.")
                    # Reset migration status file since we wiped the DB
                    self.migration_status["completed"] = False 
                    self.migration_status["data_migrated"] = {} 
                    self.migration_status["started"] = False
                    self._save_migration_status()
                    bt.logging.info("Migration status file reset due to forced table drop.")
                    migration_occurred = True # Indicate action was taken
                except Exception as drop_err:
                    bt.logging.error(f"Error dropping tables during forced migration: {drop_err}")
                    bt.logging.error(traceback.format_exc())
                    bt.logging.warning("Proceeding with migration attempt despite table drop failure.") 
                    # Don't return False here, allow subsequent initialization to try creating tables
            
            # --- Check if *regular* migration is needed --- 
            regular_migration_needed = await self.check_migration_needed()
            
            if not regular_migration_needed and not force_migration_flag:
                bt.logging.info("Migration not required (already complete or source/target state indicates no action needed) and not forced.")
                return False # Return False indicating no migration *action* was taken this run
            
            if regular_migration_needed:
                bt.logging.info("Starting data migration from SQLite to PostgreSQL...")
                migration_occurred = True # Data migration counts as an action
                self.migration_status["started"] = True
                self.migration_status["last_attempt"] = datetime.now().isoformat()
                
                # Reset individual table status only if migration wasn't marked fully complete before
                if not self.migration_status.get("completed", False):
                    bt.logging.info("Migration status not marked as complete. Resetting individual table migration statuses.")
                    self.migration_status["data_migrated"] = {} 
                self._save_migration_status() 
                
                # Connect to SQLite
                sqlite_conn = sqlite3.connect(self.sqlite_path)
                sqlite_conn.row_factory = sqlite3.Row
                
                try:
                    # Get list of tables from SQLite
                    cursor = sqlite_conn.cursor()
                    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
                    all_sqlite_tables = {row[0] for row in cursor.fetchall()} # Use a set for efficient lookup
                    
                    # --- Define and Enforce Migration Order --- 
                    preferred_order = [
                        'miner_stats', 
                        'entropy_game_pools', # Must come before entropy_predictions
                        'predictions', 
                        'entropy_predictions' # Depends on entropy_game_pools
                        # Add other tables here if they have specific dependencies
                    ]
                    
                    tables_to_migrate_ordered = []
                    processed_tables = set()
                    
                    # Add preferred tables that exist in SQLite
                    for table_name in preferred_order:
                        if table_name in all_sqlite_tables:
                            tables_to_migrate_ordered.append(table_name)
                            processed_tables.add(table_name)
                        
                    # Add remaining tables from SQLite
                    for table_name in all_sqlite_tables:
                        if table_name not in processed_tables:
                            tables_to_migrate_ordered.append(table_name)
                    
                    bt.logging.info(f"Determined migration order: {tables_to_migrate_ordered}")
                    # --- End Migration Order Enforcement ---

                    # Connect to PostgreSQL - FILTER CONFIG HERE
                    try:
                        connect_config = _filter_psycopg2_config(self.pg_config)
                        pg_conn = psycopg2.connect(**connect_config)
                        pg_conn.autocommit = False
                        pg_cursor = pg_conn.cursor()
                    except psycopg2.Error as db_err:
                        bt.logging.error(f"Failed to connect to PostgreSQL for data migration: {db_err}")
                        raise # Re-raise to be caught by the outer try/except
                    
                    try:
                        # Get PG column types for target tables
                        pg_column_types = await self._get_postgres_column_types(pg_cursor)
                        valid_entropy_fk_pairs = set() # Initialize outside the loop
                        
                        # --- Pre-fetch hotkeys --- 
                        hotkey_map = {}
                        try:
                            bt.logging.info("Fetching miner hotkeys from SQLite miner_stats for all table migrations...")
                            cursor.execute("SELECT miner_uid, miner_hotkey FROM miner_stats")
                            hotkey_rows = cursor.fetchall()
                            hotkey_map = {row['miner_uid']: row['miner_hotkey'] for row in hotkey_rows if row['miner_uid'] is not None}
                            bt.logging.info(f"Fetched {len(hotkey_map)} hotkeys.")
                        except Exception as hk_err:
                            bt.logging.error(f"Failed to fetch hotkeys from SQLite: {hk_err}. Hotkeys might be missing in migrated tables.")
                        # --- End pre-fetch hotkeys ---
                        
                        # Process each table IN THE DETERMINED ORDER
                        for table in tables_to_migrate_ordered:
                            # Skip the backup table if it exists
                            if table == 'miner_stats_backup':
                                bt.logging.info(f"Skipping unnecessary table: {table}")
                                continue
                            
                            if self.migration_status.get("data_migrated", {}).get(table, False):
                                bt.logging.info(f"Table {table} already migrated, skipping")
                                continue
                            
                            bt.logging.info(f"Migrating data for table: {table}")
                            
                            # --- Post-migration step for entropy_game_pools: Fetch valid FK pairs --- 
                            if table == 'entropy_game_pools':
                                 try:
                                     # Ensure the current transaction for entropy_game_pools is committed
                                     # The loop below handles commit after successful batch insert
                                     # Fetch the keys AFTER the table data is expected to be there
                                     bt.logging.info("Fetching valid (game_id, outcome) pairs from migrated entropy_game_pools...")
                                     pg_cursor.execute("SELECT game_id, outcome FROM entropy_game_pools")
                                     valid_entropy_fk_pairs = {tuple(row) for row in pg_cursor.fetchall()} 
                                     bt.logging.info(f"Fetched {len(valid_entropy_fk_pairs)} valid FK pairs for entropy_predictions.")
                                 except Exception as fk_fetch_err:
                                     bt.logging.error(f"Failed to fetch FK pairs from entropy_game_pools: {fk_fetch_err}. Migration for entropy_predictions might fail.")
                            # --- End FK Fetch ---
                            
                            # Get SQLite schema
                            cursor.execute(f"PRAGMA table_info({table})")
                            sqlite_columns_info = cursor.fetchall()
                            sqlite_columns = [col[1] for col in sqlite_columns_info]
                            
                            # Get target PG columns
                            target_table_types = pg_column_types.get(table, {})
                            if not target_table_types:
                                bt.logging.warning(f"Could not determine PG columns for table '{table}'. Skipping type conversions.")
                                
                            # --- MODIFICATION START: Exclude score_id for scores table --- 
                            target_pg_columns_for_insert = list(target_table_types.keys())
                            # Create a map from sqlite column name to its index
                            sqlite_col_index_map = {name: idx for idx, name in enumerate(sqlite_columns)}
                            
                            if table == 'scores':
                                if 'score_id' in target_pg_columns_for_insert:
                                    bt.logging.debug(f"Excluding 'score_id' from INSERT statement for table '{table}'.")
                                    target_pg_columns_for_insert.remove('score_id')
                            # --- MODIFICATION END --- 
                            
                            # Get data from SQLite
                            cursor.execute(f"SELECT {', '.join(sqlite_columns)} FROM {table}")
                            rows = cursor.fetchall()
                            
                            if not rows:
                                bt.logging.info(f"Table {table} is empty, skipping")
                                self.migration_status.setdefault("data_migrated", {})[table] = True
                                self._save_migration_status()
                                continue
                            
                            # Insert data in batches
                            batch_size = 1000
                            skipped_row_count = 0
                            for i in range(0, len(rows), batch_size):
                                batch = rows[i:i + batch_size]
                                values_to_insert = []
                                
                                for row_idx, row in enumerate(batch):
                                    row_data_map = {} 
                                    conversion_success = True
                                    
                                    # Process SQLite columns based on *all* target PG columns first
                                    # We still need to potentially convert all data, even if not inserted
                                    for target_col_name in target_table_types.keys():
                                        if target_col_name in sqlite_col_index_map:
                                            sqlite_col_idx = sqlite_col_index_map[target_col_name]
                                            sqlite_val = row[sqlite_col_idx]
                                            target_pg_type_str = target_table_types.get(target_col_name)
                                            target_py_type = None
                                            # Datetime conversion first
                                            if target_pg_type_str and 'TIMESTAMP' in target_pg_type_str.upper():
                                                converted_val = safe_convert(sqlite_val, datetime)
                                                target_py_type = datetime # Mark as handled
                                            # Generic type conversion if not handled above
                                            if target_py_type is None and target_pg_type_str:
                                                for sql_type_key, py_type in TYPE_MAPPING.items():
                                                    if sql_type_key in target_pg_type_str.upper():
                                                        target_py_type = py_type
                                                        break
                                            # Apply conversion if type found and not datetime
                                            if target_py_type and target_py_type != datetime: 
                                                converted_val = safe_convert(sqlite_val, target_py_type)
                                            # Fallback if no type mapping or already handled datetime
                                            elif target_py_type != datetime:
                                                 converted_val = None if sqlite_val == '' else sqlite_val
                                                  
                                            row_data_map[target_col_name] = converted_val
                                        
                                    # Add/Populate columns specific to the predictions table migration
                                    if table == 'predictions':
                                        # Populate miner_hotkey (if target column exists)
                                        if 'miner_hotkey' in target_pg_columns_for_insert:
                                            miner_uid_val = row_data_map.get('miner_uid')
                                            if miner_uid_val is not None:
                                                hotkey = hotkey_map.get(miner_uid_val)
                                                if hotkey:
                                                    row_data_map['miner_hotkey'] = hotkey
                                                else:
                                                    bt.logging.warning(f"[Predictions] Hotkey not found for miner_uid {miner_uid_val} in row {row_idx}. Setting hotkey to None.")
                                                    row_data_map['miner_hotkey'] = None
                                            else:
                                                bt.logging.warning(f"[Predictions] miner_uid is None in row {row_idx}. Cannot fetch hotkey. Setting hotkey to None.")
                                                row_data_map['miner_hotkey'] = None
                                        # Populate prediction_type
                                        if 'prediction_type' in target_pg_columns_for_insert:
                                            row_data_map['prediction_type'] = 'Moneyline' # Set default for old data

                                    # Add/Populate columns specific to the scores table migration
                                    elif table == 'scores':
                                        # Populate miner_hotkey (if target column exists)
                                        if 'miner_hotkey' in target_pg_columns_for_insert:
                                            miner_uid_val = row_data_map.get('miner_uid')
                                            if miner_uid_val is not None:
                                                hotkey = hotkey_map.get(miner_uid_val)
                                                if hotkey:
                                                    row_data_map['miner_hotkey'] = hotkey
                                                else:
                                                    bt.logging.warning(f"[Scores] Hotkey not found for miner_uid {miner_uid_val} in row {row_idx}. Setting hotkey to None.")
                                                    row_data_map['miner_hotkey'] = None
                                            else:
                                                bt.logging.warning(f"[Scores] miner_uid is None in row {row_idx}. Cannot fetch hotkey. Setting hotkey to None.")
                                                row_data_map['miner_hotkey'] = None

                                    # Add/Populate columns specific to the entropy_predictions table migration
                                    elif table == 'entropy_predictions':
                                        # Populate miner_hotkey (if target column exists)
                                        if 'miner_hotkey' in target_pg_columns_for_insert:
                                            miner_uid_val = row_data_map.get('miner_uid')
                                            if miner_uid_val is not None:
                                                hotkey = hotkey_map.get(miner_uid_val)
                                                if hotkey:
                                                    row_data_map['miner_hotkey'] = hotkey
                                                else:
                                                    bt.logging.warning(f"[EntropyPred] Hotkey not found for miner_uid {miner_uid_val} in row {row_idx}. Setting hotkey to None.")
                                                    row_data_map['miner_hotkey'] = None
                                            else:
                                                bt.logging.warning(f"[EntropyPred] miner_uid is None in row {row_idx}. Cannot fetch hotkey. Setting hotkey to None.")
                                                row_data_map['miner_hotkey'] = None

                                    # Add/Populate columns specific to the entropy_miner_scores table migration
                                    elif table == 'entropy_miner_scores':
                                        # Populate miner_hotkey (if target column exists)
                                        if 'miner_hotkey' in target_pg_columns_for_insert:
                                            miner_uid_val = row_data_map.get('miner_uid')
                                            if miner_uid_val is not None:
                                                hotkey = hotkey_map.get(miner_uid_val)
                                                if hotkey:
                                                    row_data_map['miner_hotkey'] = hotkey
                                                else:
                                                    bt.logging.warning(f"[EntropyMinerScores] Hotkey not found for miner_uid {miner_uid_val} in row {row_idx}. Setting hotkey to None.")
                                                    row_data_map['miner_hotkey'] = None
                                            else:
                                                bt.logging.warning(f"[EntropyMinerScores] miner_uid is None in row {row_idx}. Cannot fetch hotkey. Setting hotkey to None.")
                                                row_data_map['miner_hotkey'] = None

                                    # --- FK Check for entropy_predictions --- 
                                    should_skip_row = False
                                    if table == 'entropy_predictions':
                                        game_id_val = row_data_map.get('game_id')
                                        outcome_val = row_data_map.get('outcome')
                                        if game_id_val is not None and outcome_val is not None:
                                            fk_pair = (game_id_val, outcome_val)
                                            if fk_pair not in valid_entropy_fk_pairs:
                                                bt.logging.warning(f"Skipping row {row_idx} for '{table}': FK pair {fk_pair} not found in entropy_game_pools.")
                                                should_skip_row = True
                                                skipped_row_count += 1
                                        else:
                                            bt.logging.warning(f"Skipping row {row_idx} for '{table}': game_id or outcome is None.")
                                            should_skip_row = True
                                            skipped_row_count += 1
                                    # --- End FK Check --- 
                                    
                                    if should_skip_row:
                                        continue # Skip to the next row in the batch
                                        
                                    # Build the final tuple using ONLY the columns needed for INSERT
                                    final_row_tuple = []
                                    # --- MODIFICATION START: Iterate over columns needed for insert --- 
                                    for col_name in target_pg_columns_for_insert:
                                        # We should already have the converted value in row_data_map
                                        if col_name in row_data_map:
                                            final_row_tuple.append(row_data_map[col_name])
                                        else:
                                            # This case might happen if a target column wasn't in SQLite
                                            # or if hotkey lookup failed etc.
                                            bt.logging.warning(f"Value for target column '{col_name}' missing in row {row_idx} data map for INSERT. Appending None.")
                                            final_row_tuple.append(None)
                                    # --- MODIFICATION END --- 
                                             
                                    if conversion_success: # You might need more robust error checks
                                        values_to_insert.append(tuple(final_row_tuple))
                                    else: 
                                        bt.logging.error(f"Skipping row {row_idx} in batch for table {table} due to conversion errors.")

                                if not values_to_insert: continue
                                
                                # Build insert query using ONLY columns needed for insert
                                # --- MODIFICATION START: Use target_pg_columns_for_insert --- 
                                target_columns_str = ", ".join(f'"{col}"' for col in target_pg_columns_for_insert) 
                                placeholders = ", ".join(["%s"] * len(target_pg_columns_for_insert))
                                # --- MODIFICATION END --- 
                                insert_query = f'INSERT INTO "{table}" ({target_columns_str}) VALUES ({placeholders}) ON CONFLICT DO NOTHING'
                                
                                try:
                                    pg_cursor.executemany(insert_query, values_to_insert)
                                    pg_conn.commit()
                                    bt.logging.info(f"Inserted {len(values_to_insert)} rows (batch {i//batch_size + 1}) into {table}")
                                except Exception as e:
                                    pg_conn.rollback()
                                    bt.logging.error(f"Error inserting batch into {table}: {e}")
                                    # --- MODIFICATION: Log the actual query and sample data causing error --- 
                                    bt.logging.error(f"Failed Query: {insert_query}")
                                    bt.logging.error(f"Sample Failed Data Row (first): {values_to_insert[0] if values_to_insert else 'N/A'}")
                                    # --- END MODIFICATION ---
                                    raise

                            if skipped_row_count > 0:
                                 bt.logging.warning(f"Skipped a total of {skipped_row_count} rows for table '{table}' due to FK violations or missing data.")
                                 
                            # Mark table as migrated
                            self.migration_status.setdefault("data_migrated", {})[table] = True
                            self._save_migration_status()
                            
                        # Migration completed successfully
                        self.migration_status["completed"] = True
                        self.migration_status["errors"] = []
                        self._save_migration_status()
                        
                        bt.logging.info("Migration completed successfully")
                        return True
                        
                    finally:
                        pg_cursor.close()
                        pg_conn.close()
                    
                finally:
                    sqlite_conn.close()
                
            return migration_occurred # Return True if *any* action (drop or data transfer) happened

        except Exception as e:
            bt.logging.error(f"Migration process failed: {e}")
            bt.logging.error(traceback.format_exc())
            self.migration_status["last_error"] = str(e)
            self.migration_status["errors"].append(str(e))
            self._save_migration_status()
            return False # Return False on overall failure
            
    async def _get_postgres_column_types(self, cursor) -> dict[str, dict[str, str]]:
        """Fetch column names and data types for all tables in the public schema."""
        query = """
        SELECT table_name, column_name, data_type 
        FROM information_schema.columns
        WHERE table_schema = 'public';
        """
        try:
            cursor.execute(query)
            rows = cursor.fetchall()
            table_types = {}
            for table_name, column_name, data_type in rows:
                if table_name not in table_types:
                    table_types[table_name] = {}
                table_types[table_name][column_name] = data_type
            return table_types
        except Exception as e:
            bt.logging.error(f"Failed to fetch PostgreSQL schema information: {e}")
            return {}
            
    def _get_postgres_type(self, table: str, column: str) -> str:
        """Helper method to determine PostgreSQL column type"""
        # Special cases for known tables/columns
        if table == "game_data":
            if column == "outcome":
                return "INTEGER"  # Ensure outcome is always INTEGER
            if column in ["team_a_odds", "team_b_odds", "tie_odds"]:
                return "DOUBLE PRECISION"
        elif table == "predictions":
            if column in ["predicted_odds", "wager", "payout", "odds"]:
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

    # Remove or comment out the migrate_data method if migrate() handles conversion
    # async def migrate_data(self) -> bool:
    #     ...

    # ... (rest of the existing methods) ...

    # ... (rest of the existing methods) ... 