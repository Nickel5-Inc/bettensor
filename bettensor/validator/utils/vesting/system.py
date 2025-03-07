"""
Vesting system for Bettensor.

This module implements a vesting rewards system that tracks miners who hold 
their rewards rather than immediately selling them, and provides multipliers
to their scores based on the amount and duration of holding.
"""

import logging
import asyncio
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple, Any, Union

import bittensor as bt
import numpy as np

from bettensor.validator.utils.database.database_manager import DatabaseManager
from bettensor.validator.utils.vesting.blockchain_monitor import BlockchainMonitor
from bettensor.validator.utils.vesting.stake_tracker import StakeTracker

logger = logging.getLogger(__name__)

class VestingSystem:
    """
    Vesting system for Bettensor.
    
    This system integrates stake tracking with the scoring system to provide
    vesting multipliers based on stake holding metrics.
    
    The system tracks:
    1. Manual stake transactions (add/remove)
    2. Epoch-based balance changes (rewards/emissions)
    3. Holding metrics (percentage and duration)
    
    It then applies multipliers to miner weights based on these metrics.
    """
    
    def __init__(
        self,
        subtensor: 'bt.subtensor',
        subnet_id: int,
        db_manager: DatabaseManager,
        minimum_stake: float = 0.3,
        retention_window_days: int = 30,
        retention_target: float = 0.9,
        max_multiplier: float = 1.5,
        use_background_thread: bool = True,
        query_interval_seconds: int = 300
    ):
        """
        Initialize the vesting system.
        
        Args:
            subtensor: Initialized subtensor instance
            subnet_id: The subnet ID to monitor
            db_manager: Database manager for persistent storage
            minimum_stake: Minimum stake required to receive a multiplier
            retention_window_days: Window for calculating retention metrics
            retention_target: Target retention percentage for max multiplier
            max_multiplier: Maximum multiplier to apply
            use_background_thread: Whether to run blockchain monitoring in a background thread
            query_interval_seconds: Interval between blockchain queries in seconds
        """
        self.subtensor = subtensor
        self.subnet_id = subnet_id
        self.db_manager = db_manager
        
        # Configuration
        self.minimum_stake = minimum_stake
        self.retention_window_days = retention_window_days
        self.retention_target = retention_target
        self.max_multiplier = max_multiplier
        self.use_background_thread = use_background_thread
        
        # Components
        self.blockchain_monitor = BlockchainMonitor(
            subtensor=subtensor,
            subnet_id=subnet_id,
            db_manager=db_manager,
            query_interval_seconds=query_interval_seconds,
            auto_start_thread=False  # We'll start it manually after initialization
        )
        
        self.stake_tracker = StakeTracker(
            db_manager=db_manager
        )
        
        # Cache for minimum stake requirement
        self._minimum_stake_cache = None
        self._minimum_stake_last_updated = None
        self._minimum_stake_ttl = 3600  # 1 hour
        
        # Event handlers
        self._epoch_boundary_handlers = []
    
    async def initialize(self):
        """
        Initialize the vesting system.
        
        This method initializes the blockchain monitor and stake tracker,
        and ensures all required database tables exist.
        
        Returns:
            bool: True if initialization was successful, False otherwise
        """
        try:
            # Initialize components
            await self.blockchain_monitor.initialize()
            await self.stake_tracker.initialize()
            
            # Register event handlers for stake changes
            self._register_stake_change_handler()
            
            # Start background thread if enabled
            if self.use_background_thread:
                self.blockchain_monitor.start_background_thread()
                logger.info("Started blockchain monitor in background thread")
            
            logger.info("Vesting system initialized successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to initialize vesting system: {e}")
            return False
    
    def _register_stake_change_handler(self):
        """
        Register handlers for stake changes detected by the blockchain monitor.
        
        This method sets up the necessary event handlers to process stake changes
        and update the stake tracker when emissions are detected.
        """
        # This is a placeholder for future event-based implementation
        # Currently, the stake changes are processed directly in the blockchain monitor
        # and the stake tracker is updated in the update() method
        pass
    
    async def register_epoch_boundary_handler(self, handler):
        """
        Register a handler to be called when an epoch boundary is detected.
        
        Args:
            handler: Async function to call when an epoch boundary is detected
        """
        if callable(handler):
            self._epoch_boundary_handlers.append(handler)
            logger.info(f"Registered epoch boundary handler: {handler.__name__}")
    
    async def shutdown(self):
        """
        Shutdown the vesting system.
        
        This method stops the background thread if it's running.
        
        Returns:
            bool: True if shutdown was successful, False otherwise
        """
        try:
            # Stop background thread if running
            if self.use_background_thread and self.blockchain_monitor.is_running:
                self.blockchain_monitor.stop_background_thread()
                logger.info("Stopped blockchain monitor background thread")
            
            logger.info("Vesting system shutdown successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to shutdown vesting system: {e}")
            return False
    
    async def update(self, epoch: int):
        """
        Update the vesting system state.
        
        This method:
        1. Tracks manual stake transactions
        2. Tracks balance changes for the given epoch
        3. Updates stake metrics based on these changes
        
        If using a background thread, this method does nothing as the
        updates are handled by the thread.
        
        Args:
            epoch: Current epoch number
            
        Returns:
            bool: True if update was successful, False otherwise
        """
        # Skip if using background thread
        if self.use_background_thread and self.blockchain_monitor.is_running:
            logger.debug("Skipping manual update as blockchain monitor is running in background thread")
            return True
        
        try:
            # Track manual transactions
            num_transactions = await self.blockchain_monitor.track_manual_transactions()
            if num_transactions > 0:
                logger.info(f"Tracked {num_transactions} manual stake transactions")
            
            # Track balance changes
            changes = await self.blockchain_monitor.track_balance_changes(epoch)
            
            # Update stake metrics for each change
            for hotkey, change in changes.items():
                # Get coldkey for hotkey
                coldkey = await self.blockchain_monitor._get_coldkey_for_hotkey(hotkey)
                
                # Record stake change
                change_type = "reward" if change > 0 else "penalty"
                await self.stake_tracker.record_stake_change(
                    hotkey=hotkey,
                    coldkey=coldkey,
                    amount=change,
                    change_type=change_type
                )
            
            logger.info(f"Updated stake metrics for {len(changes)} miners")
            return True
            
        except Exception as e:
            logger.error(f"Error updating vesting system: {e}")
            return False
    
    async def process_emissions(self, epoch: int):
        """
        Process emissions from the most recent epoch boundary.
        
        This method retrieves the emissions calculated by the blockchain monitor
        and updates the stake tracker accordingly.
        
        Args:
            epoch: Current epoch number
            
        Returns:
            bool: True if processing was successful, False otherwise
        """
        try:
            # Get emissions from the database
            emissions = await self._get_emissions_for_epoch(epoch)
            
            # Update stake metrics for each emission
            for emission in emissions:
                uid = emission['uid']
                hotkey = emission['hotkey']
                true_emission = emission['true_emission']
                
                # Skip if no hotkey or zero emission
                if not hotkey or abs(true_emission) < 0.000001:
                    continue
                
                # Get coldkey for hotkey
                coldkey = await self.blockchain_monitor._get_coldkey_for_hotkey(hotkey)
                
                # Record stake change
                change_type = "reward" if true_emission > 0 else "penalty"
                await self.stake_tracker.record_stake_change(
                    hotkey=hotkey,
                    coldkey=coldkey,
                    amount=true_emission,
                    change_type=change_type
                )
                
                logger.debug(f"Processed emission for UID {uid}: {true_emission:.6f}τ")
            
            logger.info(f"Processed {len(emissions)} emissions for epoch {epoch}")
            return True
            
        except Exception as e:
            logger.error(f"Error processing emissions: {e}")
            return False
    
    async def _get_emissions_for_epoch(self, epoch: int) -> List[Dict[str, Any]]:
        """
        Get emissions for a specific epoch.
        
        Args:
            epoch: Epoch number
            
        Returns:
            List[Dict[str, Any]]: List of emission records
        """
        try:
            # Get the epoch boundary block for the specified epoch
            epoch_boundary = await self.db_manager.fetch_one("""
                SELECT block_num FROM epoch_boundaries
                WHERE id = ?
            """, (epoch,))
            
            if not epoch_boundary:
                logger.warning(f"No epoch boundary found for epoch {epoch}")
                return []
            
            block_num = epoch_boundary['block_num']
            
            # Get emissions for the epoch boundary block
            emissions = await self.db_manager.fetch_all("""
                SELECT uid, hotkey, true_emission FROM stake_changes
                WHERE block_num = ?
            """, (block_num,))
            
            return emissions
            
        except Exception as e:
            logger.error(f"Error getting emissions for epoch {epoch}: {e}")
            return []
    
    async def get_true_emissions(self, hotkey: str, window_days: int = None) -> float:
        """
        Get the true emissions for a hotkey over a specified time window.
        
        Args:
            hotkey: Hotkey to get emissions for
            window_days: Number of days to look back (defaults to retention_window_days)
            
        Returns:
            float: Total true emissions for the hotkey
        """
        try:
            if window_days is None:
                window_days = self.retention_window_days
            
            # Calculate start time
            start_time = int((datetime.now(timezone.utc) - timedelta(days=window_days)).timestamp())
            
            # Get emissions from stake_changes table
            emissions = await self.db_manager.fetch_all("""
                SELECT SUM(true_emission) as total_emission FROM stake_changes
                WHERE hotkey = ? AND timestamp >= ?
            """, (hotkey, start_time))
            
            if emissions and emissions[0]['total_emission'] is not None:
                return float(emissions[0]['total_emission'])
            
            return 0.0
            
        except Exception as e:
            logger.error(f"Error getting true emissions for {hotkey}: {e}")
            return 0.0
    
    async def apply_vesting_multipliers(self, weights: np.ndarray, uids: List[int], hotkeys: List[str]) -> np.ndarray:
        """
        Apply vesting multipliers to the scoring weights.
        
        This method:
        1. Checks if each miner meets the minimum stake requirement
        2. Calculates holding metrics for each miner
        3. Applies a multiplier based on these metrics
        
        Args:
            weights: Array of weights to adjust
            uids: List of UIDs corresponding to the weights
            hotkeys: List of hotkeys corresponding to the weights
            
        Returns:
            np.ndarray: Adjusted weights with vesting multipliers applied
        """
        try:
            # Get minimum stake requirement
            min_stake = await self._get_minimum_stake_requirement()
            
            # Create a copy of the weights to adjust
            adjusted_weights = weights.copy()
            
            # Track miners that don't meet requirements
            zero_weight_indices = []
            
            # Apply multipliers for each miner
            for i, (uid, hotkey) in enumerate(zip(uids, hotkeys)):
                # Skip if weight is already zero
                if weights[i] <= 0:
                    continue
                
                # Get stake metrics
                metrics = await self.stake_tracker.get_stake_metrics(hotkey)
                
                # If no metrics or below minimum stake, set weight to zero
                if not metrics or metrics['total_stake'] < min_stake:
                    zero_weight_indices.append(i)
                    continue
                
                # Calculate holding metrics
                holding_percentage, holding_duration = await self.stake_tracker.calculate_holding_metrics(
                    hotkey=hotkey,
                    window_days=self.retention_window_days
                )
                
                # Calculate multiplier
                multiplier = self._calculate_multiplier(holding_percentage, holding_duration)
                
                # Apply multiplier
                adjusted_weights[i] *= multiplier
                
                logger.debug(f"UID {uid}: Applied multiplier {multiplier:.2f} "
                           f"(holding: {holding_percentage:.2f}, duration: {holding_duration})")
            
            # Set weights to zero for miners that don't meet requirements
            if zero_weight_indices:
                adjusted_weights[zero_weight_indices] = 0.0
                logger.info(f"Set weights to zero for {len(zero_weight_indices)} miners below minimum stake")
            
            # Renormalize weights if needed
            total_weight = np.sum(adjusted_weights)
            if total_weight > 0:
                adjusted_weights = adjusted_weights / total_weight
            
            return adjusted_weights
            
        except Exception as e:
            logger.error(f"Error applying vesting multipliers: {e}")
            logger.warning("Returning original weights due to error")
            return weights
    
    def _calculate_multiplier(self, holding_percentage: float, holding_duration: int) -> float:
        """
        Calculate the vesting multiplier based on holding metrics.
        
        The multiplier is calculated based on:
        1. The percentage of stake that is held (vs. sold)
        2. The duration of holding
        
        Args:
            holding_percentage: Percentage of stake held (0.0 to 1.0)
            holding_duration: Duration of holding in days
            
        Returns:
            float: Multiplier to apply (1.0 to max_multiplier)
        """
        # Base multiplier based on holding percentage
        percentage_factor = min(1.0, holding_percentage / self.retention_target)
        
        # Duration factor (saturates at retention_window_days)
        duration_factor = min(1.0, holding_duration / self.retention_window_days)
        
        # Combined factor (geometric mean)
        combined_factor = (percentage_factor * duration_factor) ** 0.5
        
        # Calculate multiplier (linear interpolation between 1.0 and max_multiplier)
        multiplier = 1.0 + combined_factor * (self.max_multiplier - 1.0)
        
        return multiplier
    
    async def _get_minimum_stake_requirement(self) -> float:
        """
        Get the minimum stake requirement with caching.
        
        This method returns the configured minimum stake requirement,
        with an option to dynamically calculate it based on network metrics
        in the future.
        
        Returns:
            float: Minimum stake requirement in TAO
        """
        current_time = datetime.now(timezone.utc)
        
        # Check if cache is valid
        if (self._minimum_stake_cache is not None and 
            self._minimum_stake_last_updated is not None and
            (current_time - self._minimum_stake_last_updated).total_seconds() < self._minimum_stake_ttl):
            return self._minimum_stake_cache
        
        # For now, just return the configured value
        # In the future, this could be calculated dynamically based on network metrics
        self._minimum_stake_cache = self.minimum_stake
        self._minimum_stake_last_updated = current_time
        
        return self._minimum_stake_cache
    
    async def get_vesting_stats(self) -> Dict:
        """
        Get current vesting system statistics.
        
        Returns:
            Dict: Dictionary of vesting system statistics
        """
        try:
            # Get stake distribution
            distribution = await self.stake_tracker.get_stake_distribution()
            
            # Calculate average holding metrics
            total_holding_percentage = 0.0
            total_holding_duration = 0
            num_miners = 0
            
            for hotkey in distribution.get('hotkeys', []):
                holding_percentage, holding_duration = await self.stake_tracker.calculate_holding_metrics(
                    hotkey=hotkey,
                    window_days=self.retention_window_days
                )
                
                total_holding_percentage += holding_percentage
                total_holding_duration += holding_duration
                num_miners += 1
            
            avg_holding_percentage = total_holding_percentage / num_miners if num_miners > 0 else 0.0
            avg_holding_duration = total_holding_duration / num_miners if num_miners > 0 else 0
            
            # Get epoch statistics
            epoch_count = await self.db_manager.fetch_one("""
                SELECT COUNT(*) as count FROM epoch_boundaries
            """)
            epoch_count = epoch_count['count'] if epoch_count else 0
            
            # Get emission statistics
            emission_stats = await self.db_manager.fetch_one("""
                SELECT 
                    COUNT(*) as count,
                    SUM(true_emission) as total_emission,
                    AVG(true_emission) as avg_emission
                FROM stake_changes
                WHERE true_emission > 0
            """)
            
            # Return statistics
            return {
                'num_miners': num_miners,
                'total_stake': sum(distribution.get('stakes', [])),
                'avg_stake': sum(distribution.get('stakes', [])) / num_miners if num_miners > 0 else 0.0,
                'min_stake_requirement': await self._get_minimum_stake_requirement(),
                'avg_holding_percentage': avg_holding_percentage,
                'avg_holding_duration': avg_holding_duration,
                'retention_window_days': self.retention_window_days,
                'retention_target': self.retention_target,
                'max_multiplier': self.max_multiplier,
                'epoch_count': epoch_count,
                'emission_count': emission_stats['count'] if emission_stats else 0,
                'total_emission': emission_stats['total_emission'] if emission_stats and emission_stats['total_emission'] is not None else 0.0,
                'avg_emission': emission_stats['avg_emission'] if emission_stats and emission_stats['avg_emission'] is not None else 0.0
            }
            
        except Exception as e:
            logger.error(f"Error getting vesting stats: {e}")
            return {
                'error': str(e)
            } 