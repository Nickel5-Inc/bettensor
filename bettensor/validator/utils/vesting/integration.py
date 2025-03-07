"""
Vesting system integration module.

This module contains hooks and integration points to connect the vesting system 
with the rest of the Bettensor framework, including the validator and database manager.
"""

import logging
import functools
import numpy as np
from typing import Dict, List, Optional, Callable, Any

from bettensor.validator.utils.vesting.system import VestingSystem

logger = logging.getLogger(__name__)

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
    
    def install(self):
        """
        Install the vesting integration by patching the calculate_weights method.
        
        Returns:
            bool: True if installation was successful
        """
        if self.is_installed:
            logger.warning("Vesting integration is already installed")
            return False
        
        try:
            # Define the patched method using a closure to preserve self reference
            @functools.wraps(self.original_calculate_weights)
            def patched_calculate_weights(*args, **kwargs):
                # Call the original method to get base weights
                original_weights = self.original_calculate_weights(*args, **kwargs)
                
                # Apply vesting multipliers
                return self._apply_vesting_multipliers(original_weights)
            
            # Install the patched method
            self.scoring_system.calculate_weights = patched_calculate_weights
            
            # Store a reference to the original method on the scoring system
            self.scoring_system._original_calculate_weights = self.original_calculate_weights
            
            self.is_installed = True
            logger.info("Vesting integration installed successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to install vesting integration: {e}")
            return False
    
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
            adjusted_weights = await self.vesting_system.apply_vesting_multipliers(
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

# Import only when needed to avoid circular imports
def get_vesting_system(subtensor, subnet_id, db_manager):
    """
    Get or create a vesting system instance.
    
    This is a factory function to get a vesting system instance while avoiding
    circular imports.
    
    Args:
        subtensor: Subtensor instance
        subnet_id: Subnet ID
        db_manager: Database manager instance
        
    Returns:
        VestingSystem: The vesting system instance
    """
    from bettensor.validator.utils.vesting.system import VestingSystem
    return VestingSystem(
        subtensor=subtensor,
        subnet_id=subnet_id,
        db_manager=db_manager
    )

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
    from bettensor.validator.utils.vesting.database_schema import create_vesting_tables
    return create_vesting_tables(db_manager)

async def handle_deregistered_miner(subtensor, subnet_id, db_manager, hotkey: str) -> bool:
    """
    Handle deregistration of a miner in the vesting system.
    
    This function should be called by the validator when a miner is deregistered
    to clean up associated vesting data.
    
    Args:
        subtensor: Subtensor instance
        subnet_id: Subnet ID
        db_manager: Database manager instance
        hotkey: Hotkey of the deregistered miner
        
    Returns:
        bool: True if cleanup was successful
    """
    vesting_system = get_vesting_system(subtensor, subnet_id, db_manager)
    return await vesting_system.cleanup_deregistered_miner(hotkey)

async def apply_vesting_multipliers(subtensor, subnet_id, db_manager, 
                               weights, uids, hotkeys) -> List[float]:
    """
    Apply vesting multipliers to weights.
    
    This function should be called by the validator during weight setting
    to apply vesting multipliers to the base weights.
    
    Args:
        subtensor: Subtensor instance
        subnet_id: Subnet ID
        db_manager: Database manager instance
        weights: Base weights to apply multipliers to
        uids: UIDs corresponding to the weights
        hotkeys: Hotkeys corresponding to the weights
        
    Returns:
        List[float]: Weights with vesting multipliers applied
    """
    vesting_system = get_vesting_system(subtensor, subnet_id, db_manager)
    return vesting_system.apply_vesting_multipliers(weights, uids, hotkeys) 