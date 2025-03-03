"""
Integration of the vesting rewards system with the scoring system.
"""

import logging
import numpy as np
import bittensor as bt

from bettensor.validator.utils.vesting.tracker import VestingTracker
from bettensor.validator.utils.vesting.multipliers import calculate_multiplier

logger = logging.getLogger(__name__)


class VestingSystemIntegration:
    """
    Integrates the vesting rewards system with the scoring system.
    
    This class handles:
    1. Applying vesting multipliers to scores
    2. Recording emissions during weight calculation
    3. Updating stake tracking
    """
    
    def __init__(self, scoring_system, vesting_tracker: VestingTracker):
        """
        Initialize the integration.
        
        Args:
            scoring_system: The scoring system to integrate with
            vesting_tracker: The vesting tracker to use
        """
        self.scoring_system = scoring_system
        self.vesting_tracker = vesting_tracker
        
        # Save original methods for patching
        self._original_calculate_weights = scoring_system.calculate_weights
        
    def install(self):
        """
        Install the vesting system integration into the scoring system.
        """
        # Replace calculate_weights method with our patched version
        self.scoring_system.calculate_weights = self._patched_calculate_weights
        logger.info("Vesting rewards system integration installed")
        
    def uninstall(self):
        """
        Remove the vesting system integration from the scoring system.
        """
        # Restore original methods
        self.scoring_system.calculate_weights = self._original_calculate_weights
        logger.info("Vesting rewards system integration uninstalled")
    
    async def _patched_calculate_weights(self, day=None):
        """
        Patched version of calculate_weights that applies vesting multipliers.
        
        This method:
        1. Gets weights calculated by the original method
        2. Updates stake tracking in the vesting system
        3. Applies vesting multipliers to the weights
        4. Records emissions based on the weights
        
        Args:
            day: The day to calculate weights for (defaults to current day)
            
        Returns:
            np.ndarray: The modified weights after applying vesting multipliers
        """
        # Get weights from original method
        weights = self._original_calculate_weights(day)
        
        if weights is None or not np.any(weights > 0):
            return weights
            
        try:
            # Update stake tracking
            await self.vesting_tracker.update_key_associations()
            await self.vesting_tracker.update_stake_history()
            
            # Make a copy of the weights to modify
            modified_weights = weights.copy()
            total_weight_before = modified_weights.sum()
            
            if total_weight_before <= 0:
                logger.warning("No positive weights to apply multipliers to")
                return weights
                
            # Apply multipliers for each miner with non-zero weight
            miner_uids = np.where(modified_weights > 0)[0]
            multipliers = np.ones(self.scoring_system.num_miners)
            
            multiplier_counts = {
                "1.0-1.1": 0,
                "1.1-1.2": 0,
                "1.2-1.3": 0,
                "1.3-1.4": 0,
                "1.4-1.5": 0,
            }
            
            for uid in miner_uids:
                # Get hotkey from metagraph (will need to implement this correctly)
                try:
                    if hasattr(self.scoring_system, 'validator') and self.scoring_system.validator:
                        hotkey = self.scoring_system.validator.metagraph.hotkeys[uid]
                    else:
                        hotkey = str(uid)  # Fallback when not available
                        
                    # Get holding metrics
                    holding_pct = await self.vesting_tracker.get_holding_percentage(hotkey)
                    holding_days = await self.vesting_tracker.get_holding_duration(hotkey)
                    
                    # Calculate multiplier
                    multiplier = calculate_multiplier(holding_pct, holding_days)
                    multipliers[uid] = multiplier
                    
                    # Apply multiplier
                    modified_weights[uid] *= multiplier
                    
                    # Update stats
                    if multiplier <= 1.1:
                        multiplier_counts["1.0-1.1"] += 1
                    elif multiplier <= 1.2:
                        multiplier_counts["1.1-1.2"] += 1
                    elif multiplier <= 1.3:
                        multiplier_counts["1.2-1.3"] += 1
                    elif multiplier <= 1.4:
                        multiplier_counts["1.3-1.4"] += 1
                    else:
                        multiplier_counts["1.4-1.5"] += 1
                        
                    # Record emissions based on original weight
                    await self.vesting_tracker.record_emissions(
                        hotkey, 
                        weights[uid] * 100  # Scale to more meaningful units
                    )
                    
                except Exception as e:
                    logger.error(f"Error applying vesting multiplier for miner {uid}: {e}")
                    # Continue with other miners
            
            # Renormalize weights
            total_weight_after = modified_weights.sum()
            if total_weight_after > 0:
                modified_weights = modified_weights * (total_weight_before / total_weight_after)
            
            # Log multiplier statistics
            logger.info("Vesting multiplier distribution:")
            for range_str, count in multiplier_counts.items():
                logger.info(f"  {range_str}: {count} miners")
                
            logger.info(f"Average multiplier: {multipliers[multipliers > 0].mean():.4f}")
            logger.info(f"Max multiplier: {multipliers.max():.4f}")
            
            # Return the modified weights
            return modified_weights
            
        except Exception as e:
            logger.error(f"Error in patched calculate_weights: {e}")
            # If anything goes wrong, return the original weights
            return weights 