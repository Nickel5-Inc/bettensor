"""
Vesting system integration module.

This module contains all integration points to connect the vesting system 
with the rest of the Bettensor framework, including:

1. ScoringSystem integration for weight adjustments
2. DatabaseManager integration for schema initialization  
3. Validator integration for handling deregistration events

It serves as a single entry point for all vesting system integration needs.
"""

import logging
import functools
import numpy as np
from typing import Dict, List, Optional, Callable, Any, Union
import asyncio

from bettensor.validator.vesting.system import VestingSystem
from bettensor.validator.vesting.database_schema import create_vesting_tables_async

logger = logging.getLogger(__name__)

# ----------------------
# Database Integration
# ----------------------

async def initialize_vesting_tables(db_manager) -> bool:
    """
    Initialize vesting system database tables.
    
    This function is intended to be called by the DatabaseManager during
    initialization to create all vesting system tables.
    
    Args:
        db_manager: Database manager instance
        
    Returns:
        bool: True if tables were created successfully
    """
    logger.info("Initializing vesting system database schema")
    from bettensor.validator.vesting.database_schema import create_vesting_tables_async
    
    # First attempt to create tables
    success = await create_vesting_tables_async(db_manager)
    if not success:
        logger.warning("Failed to create vesting tables on first attempt, retrying...")
        await asyncio.sleep(0.5)  # Small delay before retry
        success = await create_vesting_tables_async(db_manager)
    
    # Verify tables were actually created
    if success:
        # Check that a critical table exists as verification
        try:
            result = await db_manager.fetch_one("""
                SELECT name FROM sqlite_master 
                WHERE type='table' AND name='stake_tranches'
            """)
            if not result:
                logger.error("stake_tranches table was not created despite successful creation call")
                # Try one more time with explicit creation
                await db_manager.execute_query("""
                    CREATE TABLE IF NOT EXISTS stake_tranches (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        hotkey TEXT NOT NULL,
                        coldkey TEXT,
                        initial_amount REAL NOT NULL,
                        remaining_amount REAL NOT NULL,
                        entry_timestamp INTEGER NOT NULL,
                        is_emission INTEGER NOT NULL,
                        last_update INTEGER NOT NULL
                    )
                """)
                # Add index
                await db_manager.execute_query("""
                    CREATE INDEX IF NOT EXISTS idx_stake_tranches_hotkey ON stake_tranches(hotkey)
                """)
                logger.info("stake_tranches table created explicitly")
                return True
        except Exception as e:
            logger.error(f"Error verifying table creation: {e}")
            return False
    
    return success

# ----------------------
# Validator Integration
# ----------------------

async def handle_deregistered_miner(vesting_system: Optional[VestingSystem], hotkey: str) -> bool:
    """
    Handle deregistration of a miner in the vesting system.
    
    This function should be called by the validator when a miner is deregistered
    to clean up associated vesting data.
    
    Args:
        vesting_system: The vesting system instance
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

async def batch_handle_deregistrations(vesting_system: Optional[VestingSystem], 
                                   deregistered_hotkeys: List[str]) -> int:
    """
    Handle batch deregistration of multiple miners.
    
    Args:
        vesting_system: The vesting system instance
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

# ----------------------
# Factory Functions
# ----------------------

def get_vesting_system(subtensor, subnet_id, db_manager) -> VestingSystem:
    """
    Get or create a vesting system instance.
    
    Args:
        subtensor: Subtensor instance
        subnet_id: Subnet ID
        db_manager: Database manager instance
        
    Returns:
        VestingSystem: The vesting system instance
    """
    return VestingSystem(
        subtensor=subtensor,
        subnet_id=subnet_id,
        db_manager=db_manager
    )

async def apply_vesting_multipliers(vesting_system: VestingSystem,
                               weights: np.ndarray, 
                               uids: List[int], 
                               hotkeys: List[str]) -> np.ndarray:
    """
    Apply vesting multipliers to weights.
    
    This function should be called by the validator during weight setting
    to apply vesting multipliers to the base weights.
    
    Args:
        vesting_system: The vesting system instance
        weights: Base weights to apply multipliers to
        uids: UIDs corresponding to the weights
        hotkeys: Hotkeys corresponding to the UIDs
        
    Returns:
        np.ndarray: Adjusted weights with vesting multipliers applied
    """
    if vesting_system is None:
        logger.warning("Cannot apply vesting multipliers: vesting_system is None")
        return weights
        
    try:
        logger.info(f"Applying vesting multipliers to {len(weights)} weights")
        return await vesting_system.apply_vesting_multipliers(
            weights=weights,
            uids=uids,
            hotkeys=hotkeys
        )
    except Exception as e:
        logger.error(f"Error applying vesting multipliers: {e}", exc_info=True)
        return weights

# ----------------------
# Scoring System Integration
# ----------------------

class VestingIntegration:
    """
    Integrates the vesting system with the scoring system.
    
    This class provides a clean integration by monkey-patching the scoring system's
    calculate_weights method to apply vesting multipliers to the weights.
    """
    
    def __init__(
        self,
        scoring_system: Any,
        vesting_system: VestingSystem,
        hotkey_uid_mapping_func: Optional[Callable] = None
    ):
        """
        Initialize the vesting integration.
        
        Args:
            scoring_system: The scoring system to integrate with
            vesting_system: The vesting system to use for multipliers
            hotkey_uid_mapping_func: Optional function to map UIDs to hotkeys
                Function signature: List[int] -> List[str]
                If not provided, the integration will try to use metagraph.hotkeys
        """
        self.scoring_system = scoring_system
        self.vesting_system = vesting_system
        self.hotkey_uid_mapping_func = hotkey_uid_mapping_func
        
        # Store original calculate_weights method
        self.original_calculate_weights = scoring_system.calculate_weights
        
        # Flag to track if integration is installed
        self.is_installed = False
        
        # Flag to control whether vesting impacts weights
        self.impact_weights = True
    
    def install(self, impact_weights: bool = True):
        """
        Install the vesting integration by patching the calculate_weights method.
        
        Args:
            impact_weights: If True, vesting multipliers will be applied to weights.
                           If False, the system will run in shadow mode, tracking everything
                           but not modifying the weights.
        
        Returns:
            bool: True if installation was successful
        """
        if self.is_installed:
            logger.warning("Vesting integration is already installed")
            return False
        
        # Store the impact_weights setting
        self.impact_weights = impact_weights
        
        try:
            # Define the patched method using a closure to preserve self reference
            @functools.wraps(self.original_calculate_weights)
            def patched_calculate_weights(*args, **kwargs):
                # Call the original method to get base weights
                original_weights = self.original_calculate_weights(*args, **kwargs)
                
                # Apply vesting multipliers if impact_weights is True, otherwise return original
                if self.impact_weights:
                    return self._apply_vesting_multipliers(original_weights)
                else:
                    # Still calculate multipliers for tracking/stats but don't apply them
                    asyncio.create_task(self._calculate_without_applying(original_weights))
                    return original_weights
            
            # Install the patched method
            self.scoring_system.calculate_weights = patched_calculate_weights
            
            # Store a reference to the original method on the scoring system
            self.scoring_system._original_calculate_weights = self.original_calculate_weights
            
            self.is_installed = True
            mode_str = "enabled" if self.impact_weights else "shadow mode (tracking only)"
            logger.info(f"Vesting integration installed successfully in {mode_str}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to install vesting integration: {e}")
            return False
    
    async def _calculate_without_applying(self, original_weights: np.ndarray) -> None:
        """
        Calculate vesting multipliers without applying them (for shadow mode).
        This allows tracking multiplier calculations even when not applying them to weights.
        
        Args:
            original_weights: Original weights from scoring system
        """
        try:
            # Skip if weights are empty or all zero
            if original_weights is None or np.sum(original_weights) <= 0:
                return
            
            # Get UIDs with non-zero weights
            uids = np.where(original_weights > 0)[0].tolist()
            
            # Get hotkeys for UIDs
            hotkeys = self._get_hotkeys_for_uids(uids)
            
            # If no hotkeys could be mapped, return
            if not hotkeys:
                return
            
            # Calculate multipliers but don't use the result
            await apply_vesting_multipliers(
                vesting_system=self.vesting_system,
                weights=original_weights,
                uids=uids,
                hotkeys=hotkeys
            )
            
            logger.debug(f"Calculated vesting multipliers for {len(uids)} miners in shadow mode")
        except Exception as e:
            logger.error(f"Error calculating vesting multipliers in shadow mode: {e}")
    
    def uninstall(self):
        """
        Uninstall the vesting integration and restore original method.
        
        Returns:
            bool: True if uninstallation was successful
        """
        if not self.is_installed:
            logger.warning("Vesting integration is not installed")
            return False
        
        try:
            # Restore original method
            self.scoring_system.calculate_weights = self.original_calculate_weights
            
            # Remove reference to original method if it exists
            if hasattr(self.scoring_system, '_original_calculate_weights'):
                delattr(self.scoring_system, '_original_calculate_weights')
            
            self.is_installed = False
            logger.info("Vesting integration uninstalled successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to uninstall vesting integration: {e}")
            return False
    
    async def _apply_vesting_multipliers(self, original_weights: np.ndarray) -> np.ndarray:
        """
        Apply vesting multipliers to the weights.
        
        Args:
            original_weights: Original weights from scoring system
            
        Returns:
            np.ndarray: Adjusted weights with vesting multipliers applied
        """
        try:
            # Skip if weights are empty or all zero
            if original_weights is None or np.sum(original_weights) <= 0:
                logger.warning("Original weights are empty or all zero, skipping vesting multipliers")
                return original_weights
            
            # Get UIDs with non-zero weights
            uids = np.where(original_weights > 0)[0].tolist()
            
            # Get hotkeys for UIDs
            hotkeys = self._get_hotkeys_for_uids(uids)
            
            # If no hotkeys could be mapped, return original weights
            if not hotkeys:
                logger.warning("Could not map UIDs to hotkeys, returning original weights")
                return original_weights
            
            # Apply vesting multipliers
            adjusted_weights = await apply_vesting_multipliers(
                vesting_system=self.vesting_system,
                weights=original_weights,
                uids=uids,
                hotkeys=hotkeys
            )
            
            logger.info(f"Applied vesting multipliers to {len(uids)} miners with non-zero weights")
            return adjusted_weights
            
        except Exception as e:
            logger.error(f"Error applying vesting multipliers: {e}")
            return original_weights
    
    def _get_hotkeys_for_uids(self, uids: List[int]) -> List[str]:
        """
        Get hotkeys for UIDs.
        
        Args:
            uids: List of UIDs to get hotkeys for
            
        Returns:
            List[str]: List of hotkeys
        """
        try:
            # Use provided mapping function if available
            if self.hotkey_uid_mapping_func:
                return self.hotkey_uid_mapping_func(uids)
            
            # Try to use metagraph.hotkeys
            if hasattr(self.scoring_system, 'metagraph') and hasattr(self.scoring_system.metagraph, 'hotkeys'):
                return [self.scoring_system.metagraph.hotkeys[uid] for uid in uids if uid < len(self.scoring_system.metagraph.hotkeys)]
            
            # If the scoring system has a hotkeys attribute, use that
            if hasattr(self.scoring_system, 'hotkeys'):
                return [self.scoring_system.hotkeys[uid] for uid in uids if uid < len(self.scoring_system.hotkeys)]
            
            logger.warning("Could not find hotkeys for UIDs")
            return []
            
        except Exception as e:
            logger.error(f"Error getting hotkeys for UIDs: {e}")
            return [] 