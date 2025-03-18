"""
Database schema definitions for the vesting system.

This module defines all database tables, indices, and relationships used by the vesting system.
It provides a central location for schema definitions, which can be used by the DatabaseManager
for table creation and management.
"""

import logging
from typing import Dict, List, Any

logger = logging.getLogger(__name__)

# Transaction flow categories
INFLOW = "inflow"
OUTFLOW = "outflow"
NEUTRAL = "neutral"
EMISSION = "emission"

# Transaction types
ADD_STAKE = "add_stake"
REMOVE_STAKE = "remove_stake"
TRANSFER_OWNERSHIP = "transfer_ownership"
BURN_STAKE = "burn_stake"
MOVE_STAKE = "move_stake"
SWAP_STAKE = "swap_stake"
EMISSION_REWARD = "emission"
UNKNOWN = "unknown"

def get_vesting_tables() -> Dict[str, Dict[str, Any]]:
    """
    Get the schema definitions for all vesting system tables.
    
    Returns:
        Dict[str, Dict[str, Any]]: Dictionary of table names to table definitions
    """
    tables = {
        # Consolidated table for tracking all stake transactions
        "stake_transactions": {
            "columns": [
                ("id", "INTEGER PRIMARY KEY AUTOINCREMENT"),
                ("block_number", "INTEGER NOT NULL"),
                ("timestamp", "INTEGER NOT NULL"),
                ("transaction_type", "TEXT NOT NULL"),  # add_stake, remove_stake, etc.
                ("flow_type", "TEXT NOT NULL"),         # inflow, outflow, neutral, emission
                ("hotkey", "TEXT NOT NULL"),
                ("coldkey", "TEXT"),
                ("amount", "REAL NOT NULL"),            # positive for additions, negative for removals
                ("stake_after", "REAL NOT NULL"),       # total stake after this transaction
                ("manual_stake_after", "REAL NOT NULL"),# manual stake after this transaction
                ("earned_stake_after", "REAL NOT NULL"),# earned stake after this transaction
                ("tx_hash", "TEXT"),
                ("origin_netuid", "INTEGER"),
                ("destination_netuid", "INTEGER"),
                ("destination_coldkey", "TEXT"),
                ("destination_hotkey", "TEXT"),
                ("change_type", "TEXT"),                # manual, emission, epoch
                ("epoch", "INTEGER"),
                ("validated", "INTEGER DEFAULT 0")
            ],
            "indices": [
                ("idx_stake_transactions_hotkey", "hotkey"),
                ("idx_stake_transactions_coldkey", "coldkey"),
                ("idx_stake_transactions_block", "block_number"),
                ("idx_stake_transactions_timestamp", "timestamp"),
                ("idx_stake_transactions_type", "transaction_type"),
                ("idx_stake_transactions_flow", "flow_type")
            ]
        },
        
        # Consolidated metrics table for miners
        "miner_metrics": {
            "columns": [
                ("hotkey", "TEXT PRIMARY KEY"),
                ("coldkey", "TEXT"),
                ("total_stake", "REAL NOT NULL DEFAULT 0"),
                ("manual_stake", "REAL NOT NULL DEFAULT 0"),
                ("earned_stake", "REAL NOT NULL DEFAULT 0"),
                ("first_stake_timestamp", "INTEGER"),
                ("last_update", "INTEGER NOT NULL"),
                ("total_tranches", "INTEGER NOT NULL DEFAULT 0"),
                ("active_tranches", "INTEGER NOT NULL DEFAULT 0"),
                ("avg_tranche_age", "INTEGER NOT NULL DEFAULT 0"),
                ("oldest_tranche_age", "INTEGER NOT NULL DEFAULT 0"),
                ("manual_tranches", "INTEGER NOT NULL DEFAULT 0"),
                ("emission_tranches", "INTEGER NOT NULL DEFAULT 0")
            ],
            "indices": [
                ("idx_miner_metrics_coldkey", "coldkey")
            ]
        },
        
        # Coldkey metrics table (aggregation across multiple hotkeys)
        "coldkey_metrics": {
            "columns": [
                ("coldkey", "TEXT PRIMARY KEY"),
                ("total_stake", "REAL NOT NULL DEFAULT 0"),
                ("manual_stake", "REAL NOT NULL DEFAULT 0"),
                ("earned_stake", "REAL NOT NULL DEFAULT 0"),
                ("hotkey_count", "INTEGER NOT NULL DEFAULT 0"),
                ("last_update", "INTEGER NOT NULL")
            ],
            "indices": []
        },
        
        # Consolidated tranche table with exit information
        "stake_tranches": {
            "columns": [
                ("id", "INTEGER PRIMARY KEY AUTOINCREMENT"),
                ("hotkey", "TEXT NOT NULL"),
                ("coldkey", "TEXT"),
                ("initial_amount", "REAL NOT NULL"),
                ("remaining_amount", "REAL NOT NULL"),
                ("entry_timestamp", "INTEGER NOT NULL"),
                ("is_emission", "INTEGER NOT NULL"),
                ("last_update", "INTEGER NOT NULL"),
                ("exit_timestamp", "INTEGER"),
                ("exit_amount", "REAL"),
                ("exit_reason", "TEXT"),
                ("is_active", "INTEGER NOT NULL DEFAULT 1")
            ],
            "indices": [
                ("idx_stake_tranches_hotkey", "hotkey"),
                ("idx_stake_tranches_active", "is_active"),
                ("idx_stake_tranches_entry", "entry_timestamp")
            ]
        },
        
        # Module state table (unchanged)
        "vesting_module_state": {
            "columns": [
                ("module_name", "TEXT PRIMARY KEY"),
                ("last_block", "INTEGER"),
                ("last_timestamp", "INTEGER"),
                ("last_epoch", "INTEGER"),
                ("module_data", "TEXT")  # JSON serialized state
            ],
            "indices": []
        }
    }
    
    return tables

def get_create_table_statements() -> List[str]:
    """
    Generate CREATE TABLE statements for all vesting system tables.
    
    Returns:
        List[str]: List of SQL statements to create all tables
    """
    tables = get_vesting_tables()
    statements = []
    
    for table_name, table_def in tables.items():
        columns_sql = ", ".join([f"{name} {type_}" for name, type_ in table_def["columns"]])
        create_stmt = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns_sql})"
        statements.append(create_stmt)
        
        # Add index creation statements
        for index_name, column in table_def["indices"]:
            index_stmt = f"CREATE INDEX IF NOT EXISTS {index_name} ON {table_name} ({column})"
            statements.append(index_stmt)
    
    return statements

def create_vesting_tables(db_manager) -> bool:
    """
    Create all vesting system tables in the database.
    
    Args:
        db_manager: Database manager instance
        
    Returns:
        bool: True if all tables were created successfully
    """
    try:
        statements = get_create_table_statements()
        for stmt in statements:
            db_manager.execute_query(stmt)
        
        logger.info(f"Created {len(statements)} vesting system tables and indices")
        return True
    except Exception as e:
        logger.error(f"Failed to create vesting system tables: {e}")
        return False

async def create_vesting_tables_async(db_manager) -> bool:
    """
    Create all vesting system tables in the database asynchronously.
    
    Args:
        db_manager: Database manager instance
        
    Returns:
        bool: True if all tables were created successfully
    """
    try:
        statements = get_create_table_statements()
        for stmt in statements:
            await db_manager.execute_query(stmt)
        
        logger.info(f"Created {len(statements)} vesting system tables and indices")
        return True
    except Exception as e:
        logger.error(f"Failed to create vesting system tables: {e}")
        return False 