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
        # Table for tracking individual stake transactions
        "stake_transactions": {
            "columns": [
                ("id", "INTEGER PRIMARY KEY AUTOINCREMENT"),
                ("block_number", "INTEGER NOT NULL"),
                ("timestamp", "INTEGER NOT NULL"),
                ("transaction_type", "TEXT NOT NULL"),
                ("flow_type", "TEXT NOT NULL"),  # inflow, outflow, neutral, emission
                ("hotkey", "TEXT NOT NULL"),
                ("coldkey", "TEXT"),
                ("call_amount", "REAL"),  # Amount specified in the call
                ("final_amount", "REAL"),  # Actual amount after fees
                ("fee", "REAL"),  # Transaction fee
                ("tx_hash", "TEXT"),
                ("origin_netuid", "INTEGER"),
                ("destination_netuid", "INTEGER"),
                ("destination_coldkey", "TEXT"),
                ("destination_hotkey", "TEXT"),
                ("validated", "INTEGER DEFAULT 0")  # Boolean flag
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
        
        # Table for tracking balance changes over time
        "stake_balance_changes": {
            "columns": [
                ("id", "INTEGER PRIMARY KEY AUTOINCREMENT"),
                ("block_number", "INTEGER NOT NULL"),
                ("timestamp", "INTEGER NOT NULL"),
                ("hotkey", "TEXT NOT NULL"),
                ("coldkey", "TEXT"),
                ("stake", "REAL NOT NULL"),
                ("stake_change", "REAL"),
                ("change_type", "TEXT"),  # manual, emission, epoch
                ("epoch", "INTEGER")
            ],
            "indices": [
                ("idx_stake_balance_changes_hotkey", "hotkey"),
                ("idx_stake_balance_changes_coldkey", "coldkey"),
                ("idx_stake_balance_changes_block", "block_number"),
                ("idx_stake_balance_changes_timestamp", "timestamp"),
                ("idx_stake_balance_changes_epoch", "epoch")
            ]
        },
        
        # Table for tracking stake metrics per hotkey
        "stake_metrics": {
            "columns": [
                ("hotkey", "TEXT PRIMARY KEY"),
                ("coldkey", "TEXT"),
                ("total_stake", "REAL NOT NULL DEFAULT 0"),
                ("manual_stake", "REAL NOT NULL DEFAULT 0"),
                ("earned_stake", "REAL NOT NULL DEFAULT 0"),
                ("first_stake_timestamp", "INTEGER"),
                ("last_update", "INTEGER NOT NULL")
            ],
            "indices": [
                ("idx_stake_metrics_coldkey", "coldkey")
            ]
        },
        
        # Table for tracking aggregated stake metrics per coldkey
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
        
        # Table for tracking stake change history for analytics
        "stake_change_history": {
            "columns": [
                ("id", "INTEGER PRIMARY KEY AUTOINCREMENT"),
                ("timestamp", "INTEGER NOT NULL"),
                ("hotkey", "TEXT NOT NULL"),
                ("coldkey", "TEXT"),
                ("change_type", "TEXT NOT NULL"),  # manual, emission, epoch
                ("flow_type", "TEXT NOT NULL"),  # inflow, outflow, neutral, emission
                ("amount", "REAL NOT NULL"),
                ("total_stake_after", "REAL NOT NULL"),
                ("manual_stake_after", "REAL NOT NULL"),
                ("earned_stake_after", "REAL NOT NULL")
            ],
            "indices": [
                ("idx_stake_change_history_hotkey", "hotkey"),
                ("idx_stake_change_history_coldkey", "coldkey"),
                ("idx_stake_change_history_timestamp", "timestamp"),
                ("idx_stake_change_history_type", "change_type"),
                ("idx_stake_change_history_flow", "flow_type")
            ]
        },
        
        # Table for tracking last processed block per module
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
            db_manager.execute(stmt)
        
        logger.info(f"Created {len(statements)} vesting system tables and indices")
        return True
    except Exception as e:
        logger.error(f"Failed to create vesting system tables: {e}")
        return False 