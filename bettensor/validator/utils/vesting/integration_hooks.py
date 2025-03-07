"""
Integration hooks for the vesting system.

This module provides hooks for the vesting system to integrate with:
1. The DatabaseManager for schema initialization
2. The Validator for deregistration handling
"""

import logging
from typing import List, Optional

from bettensor.validator.utils.vesting.database_schema import create_vesting_tables

logger = logging.getLogger(__name__)

# ----------------------
# Database Integration
# ----------------------

async def initialize_vesting_schema(db_manager):
    """
    Initialize vesting system database schema.
    
    This should be called from DatabaseManager.initialize() to create
    all vesting-related tables and indices.
    
    Args:
        db_manager: Database manager instance
        
    Returns:
        bool: True if successful
    """
    logger.info("Initializing vesting system database schema")
    return await create_vesting_tables(db_manager)

# ----------------------
# Validator Integration
# ----------------------

async def handle_miner_deregistration(vesting_system, hotkey: str) -> bool:
    """
    Handle miner deregistration in the vesting system.
    
    This should be called from the validator when a miner is deregistered.
    It cleans up all vesting data associated with the hotkey.
    
    Args:
        vesting_system: Initialized VestingSystem instance
        hotkey: Hotkey of the deregistered miner
        
    Returns:
        bool: True if cleanup was successful
    """
    if not vesting_system:
        logger.warning("Cannot handle deregistration: vesting_system is None")
        return False
        
    try:
        logger.info(f"Cleaning up vesting data for deregistered miner: {hotkey}")
        count = await vesting_system.handle_deregistered_keys(deregistered_hotkeys=[hotkey])
        return count > 0
    except Exception as e:
        logger.error(f"Error handling miner deregistration for {hotkey}: {e}")
        return False
        
async def batch_handle_deregistrations(vesting_system, deregistered_hotkeys: List[str]) -> int:
    """
    Handle batch deregistration of multiple miners.
    
    Args:
        vesting_system: Initialized VestingSystem instance
        deregistered_hotkeys: List of hotkeys to deregister
        
    Returns:
        int: Number of successfully cleaned up miners
    """
    if not vesting_system or not deregistered_hotkeys:
        return 0
        
    try:
        logger.info(f"Batch cleaning vesting data for {len(deregistered_hotkeys)} deregistered miners")
        return await vesting_system.handle_deregistered_keys(deregistered_hotkeys=deregistered_hotkeys)
    except Exception as e:
        logger.error(f"Error batch handling deregistrations: {e}")
        return 0 