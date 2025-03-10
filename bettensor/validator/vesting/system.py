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

from bettensor.validator.database.database_manager import DatabaseManager
from bettensor.validator.vesting.blockchain_monitor import BlockchainMonitor
from bettensor.validator.vesting.stake_tracker import StakeTracker
from bettensor.validator.vesting.transaction_monitor import TransactionMonitor
from bettensor.validator.vesting.stake_tracker import INFLOW, OUTFLOW, NEUTRAL, EMISSION

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
        query_interval_seconds: int = 300,
        detailed_transaction_tracking: bool = True
    ):
        """
        Initialize the vesting system.
        
        Args:
            subtensor: Subtensor client for blockchain queries
            subnet_id: Subnet ID to monitor
            db_manager: Database manager
            minimum_stake: Minimum stake required for eligibility (default: 0.3 τ)
            retention_window_days: Window for calculating retention metrics (default: 30 days)
            retention_target: Target retention percentage for max multiplier (default: 0.9)
            max_multiplier: Maximum multiplier to apply (default: 1.5)
            use_background_thread: Whether to use a background thread for monitoring
            query_interval_seconds: How often to poll for updates (seconds)
            detailed_transaction_tracking: Whether to use detailed transaction tracking
        """
        self.subtensor = subtensor
        self.subnet_id = subnet_id
        self.db_manager = db_manager
        
        # Configuration parameters
        self.minimum_stake = minimum_stake
        self.retention_window_days = retention_window_days
        self.retention_target = retention_target
        self.max_multiplier = max_multiplier
        self.use_background_thread = use_background_thread
        self.detailed_transaction_tracking = detailed_transaction_tracking
        
        # Create components
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
        
        # Transaction monitor for detailed transaction tracking
        if detailed_transaction_tracking:
            self.transaction_monitor = TransactionMonitor(
                subtensor=subtensor,
                subnet_id=subnet_id,
                db_manager=db_manager,
                verbose=False
            )
        else:
            self.transaction_monitor = None
        
        # Cache for minimum stake requirement
        self._minimum_stake_cache = None
        self._minimum_stake_last_updated = None
        self._minimum_stake_ttl = 3600  # 1 hour
        
        # Event handlers
        self._epoch_boundary_handlers = []
    
    async def initialize(self, auto_init_metagraph: bool = True):
        """
        Initialize the vesting system.
        
        This method initializes the blockchain monitor and stake tracker,
        and ensures all required database tables exist. It also optionally
        initializes miner data from the metagraph.
        
        Args:
            auto_init_metagraph: Whether to automatically initialize miner data from metagraph
            
        Returns:
            bool: True if initialization was successful
        """
        try:
            logger.info("Initializing vesting system")
            
            # Initialize components
            await self.blockchain_monitor.initialize()
            await self.stake_tracker.initialize()
            
            # Initialize transaction monitor if enabled
            if self.detailed_transaction_tracking and self.transaction_monitor:
                success = await self.transaction_monitor.initialize()
                if not success:
                    logger.warning("Failed to initialize transaction monitor, continuing without detailed transaction tracking")
                    self.detailed_transaction_tracking = False
                    self.transaction_monitor = None
            
            # Register event handlers for stake changes
            self._register_stake_change_handler()
            
            # Start background thread if enabled
            if self.use_background_thread:
                self.blockchain_monitor.start_background_thread()
                logger.info("Started blockchain monitor in background thread")
            
            # Start transaction monitoring if enabled
            if self.detailed_transaction_tracking and self.transaction_monitor:
                success = self.transaction_monitor.start_monitoring()
                if not success:
                    logger.warning("Failed to start transaction monitor, continuing without detailed transaction tracking")
                    self.detailed_transaction_tracking = False
                    self.transaction_monitor = None
                else:
                    logger.info("Started detailed transaction monitoring")
            
            # Initialize from metagraph if requested
            if auto_init_metagraph:
                # Check if we have any existing data first
                results = await self.db_manager.fetch_all("""
                    SELECT COUNT(*) as count FROM stake_metrics
                """)
                count = results[0]['count'] if results else 0
                
                if count == 0:
                    logger.info("No existing stake metrics found, initializing from metagraph")
                    await self.initialize_from_metagraph()
                else:
                    logger.info(f"Found {count} existing stake metric entries, skipping metagraph initialization")
            
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
        Shutdown the vesting system and clean up resources.
        
        This method stops all monitoring and cleans up resources.
        
        Returns:
            bool: True if shutdown was successful
        """
        try:
            logger.info("Shutting down vesting system")
            
            # Stop blockchain monitoring
            if self.use_background_thread:
                logger.info("Stopping blockchain monitor background thread")
                self.blockchain_monitor.stop_background_thread()
            
            # Stop transaction monitoring if enabled
            if self.transaction_monitor:
                logger.info("Stopping transaction monitor")
                success = self.transaction_monitor.stop_monitoring()
                if not success:
                    logger.warning("Failed to cleanly stop transaction monitor")
            
            logger.info("Vesting system shut down successfully")
            return True
        except Exception as e:
            logger.error(f"Error shutting down vesting system: {e}")
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
    
    async def get_detailed_stake_transactions(self, hotkey: str = None, coldkey: str = None, 
                                           days: int = None, transaction_type: str = None) -> List[Dict]:
        """
        Get detailed stake transactions for a hotkey or coldkey.
        
        Args:
            hotkey: Filter by hotkey (optional)
            coldkey: Filter by coldkey (optional)
            days: Number of days to look back (optional)
            transaction_type: Filter by transaction type (optional)
            
        Returns:
            List[Dict]: List of detailed stake transactions
        """
        try:
            # Build query
            query = "SELECT * FROM detailed_stake_transactions WHERE 1=1"
            params = []
            
            # Apply filters
            if hotkey:
                query += " AND hotkey = ?"
                params.append(hotkey)
            
            if coldkey:
                query += " AND coldkey = ?"
                params.append(coldkey)
            
            if transaction_type:
                query += " AND transaction_type = ?"
                params.append(transaction_type)
            
            if days:
                start_time = datetime.now(timezone.utc) - timedelta(days=days)
                query += " AND timestamp >= ?"
                params.append(start_time)
            
            query += " ORDER BY timestamp DESC"
            
            # Execute query
            transactions = await self.db_manager.fetch_all(query, params)
            
            return transactions
            
        except Exception as e:
            logger.error(f"Error getting detailed stake transactions: {e}")
            return []
    
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
            
            # Get transaction statistics if detailed tracking is enabled
            transaction_stats = {}
            if self.detailed_transaction_tracking:
                transaction_count = await self.db_manager.fetch_one("""
                    SELECT COUNT(*) as count FROM detailed_stake_transactions
                """)
                
                add_stake_count = await self.db_manager.fetch_one("""
                    SELECT COUNT(*) as count FROM detailed_stake_transactions
                    WHERE transaction_type = 'add_stake'
                """)
                
                remove_stake_count = await self.db_manager.fetch_one("""
                    SELECT COUNT(*) as count FROM detailed_stake_transactions
                    WHERE transaction_type = 'remove_stake'
                """)
                
                transaction_stats = {
                    'total_transactions': transaction_count['count'] if transaction_count else 0,
                    'add_stake_transactions': add_stake_count['count'] if add_stake_count else 0,
                    'remove_stake_transactions': remove_stake_count['count'] if remove_stake_count else 0,
                }
            
            # Get tranche statistics
            tranche_stats = await self.db_manager.fetch_one("""
                SELECT 
                    COUNT(*) as total_tranches,
                    SUM(CASE WHEN is_emission = 1 THEN 1 ELSE 0 END) as emission_tranches,
                    SUM(CASE WHEN is_emission = 0 THEN 1 ELSE 0 END) as manual_tranches,
                    SUM(initial_amount) as total_initial_amount,
                    SUM(remaining_amount) as total_remaining_amount,
                    SUM(CASE WHEN is_emission = 1 THEN initial_amount ELSE 0 END) as emission_initial_amount,
                    SUM(CASE WHEN is_emission = 1 THEN remaining_amount ELSE 0 END) as emission_remaining_amount,
                    AVG(CASE WHEN is_emission = 1 THEN remaining_amount / initial_amount ELSE NULL END) as avg_emission_retention
                FROM stake_tranches
            """)
            
            # Get tranche exit statistics
            exit_stats = await self.db_manager.fetch_one("""
                SELECT 
                    COUNT(*) as total_exits,
                    SUM(exit_amount) as total_exit_amount,
                    AVG(exit_amount) as avg_exit_amount
                FROM stake_tranche_exits
            """)
            
            # Return statistics
            stats = {
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
                'avg_emission': emission_stats['avg_emission'] if emission_stats and emission_stats['avg_emission'] is not None else 0.0,
                
                # Tranche statistics
                'tranche_stats': {
                    'total_tranches': tranche_stats['total_tranches'] if tranche_stats else 0,
                    'emission_tranches': tranche_stats['emission_tranches'] if tranche_stats else 0,
                    'manual_tranches': tranche_stats['manual_tranches'] if tranche_stats else 0,
                    'total_initial_amount': tranche_stats['total_initial_amount'] if tranche_stats else 0,
                    'total_remaining_amount': tranche_stats['total_remaining_amount'] if tranche_stats else 0,
                    'emission_initial_amount': tranche_stats['emission_initial_amount'] if tranche_stats else 0,
                    'emission_remaining_amount': tranche_stats['emission_remaining_amount'] if tranche_stats else 0,
                    'avg_emission_retention': tranche_stats['avg_emission_retention'] if tranche_stats and tranche_stats['avg_emission_retention'] is not None else 0,
                    'total_exits': exit_stats['total_exits'] if exit_stats else 0,
                    'total_exit_amount': exit_stats['total_exit_amount'] if exit_stats and exit_stats['total_exit_amount'] is not None else 0,
                    'avg_exit_amount': exit_stats['avg_exit_amount'] if exit_stats and exit_stats['avg_exit_amount'] is not None else 0
                }
            }
            
            # Add transaction stats if available
            if transaction_stats:
                stats.update(transaction_stats)
            
            return stats
            
        except Exception as e:
            logger.error(f"Error getting vesting stats: {e}")
            return {
                'error': str(e)
            }
    
    async def initialize_from_metagraph(self, metagraph: 'bt.metagraph' = None, force_update: bool = False):
        """
        Initialize hotkeys, associated coldkeys, and stake balances from the metagraph.
        
        This method fetches the current state of miners from the metagraph and
        creates initial stake metrics for them if they don't already exist in the database.
        
        Args:
            metagraph: Metagraph instance to use. If not provided, will fetch from subtensor.
            force_update: If True, will update existing entries even if they exist.
            
        Returns:
            bool: True if initialization was successful
        """
        try:
            # Get metagraph if not provided
            if metagraph is None:
                metagraph = self.subtensor.metagraph(self.subnet_id)
                # Ensure metagraph is synced
                metagraph.sync(subtensor=self.subtensor)
            
            # Get current stake metrics to check which hotkeys already exist
            existing_hotkeys = []
            if not force_update:
                results = await self.db_manager.fetch_all("""
                    SELECT hotkey FROM stake_metrics
                """)
                existing_hotkeys = [row['hotkey'] for row in results] if results else []
            
            init_count = 0
            update_count = 0
            
            # Process each hotkey in the metagraph
            for idx, (hotkey, coldkey, stake) in enumerate(zip(metagraph.hotkeys, metagraph.coldkeys, metagraph.stake)):
                if hotkey not in existing_hotkeys or force_update:
                    # Format stake amount (convert from torch if needed)
                    stake_amount = float(stake) if hasattr(stake, 'item') else float(stake)
                    
                    # If existing, we're updating
                    is_update = hotkey in existing_hotkeys
                    
                    # Get timestamp (current for new entries)
                    timestamp = datetime.now(timezone.utc)
                    
                    if is_update:
                        # Update existing entry - just update the total manual stake
                        # but preserve earned stake and other metrics
                        metrics = await self.stake_tracker.get_stake_metrics(hotkey)
                        if metrics:
                            # Calculate net difference to apply
                            stake_diff = stake_amount - metrics['total_stake']
                            if abs(stake_diff) > 0.000001:  # Avoid tiny floating point changes
                                # Record the difference as a change
                                change_type = "metagraph_update"
                                flow_type = INFLOW if stake_diff > 0 else OUTFLOW
                                
                                await self.stake_tracker.record_stake_change(
                                    hotkey=hotkey,
                                    coldkey=coldkey,
                                    amount=stake_diff,
                                    change_type=change_type,
                                    flow_type=flow_type,
                                    timestamp=timestamp
                                )
                                update_count += 1
                                
                                logger.info(
                                    f"Updated existing stake for {hotkey} from metagraph: "
                                    f"adjustment of {stake_diff:.6f} TAO"
                                )
                    else:
                        # Create new tranche for initial stake
                        tranche_id = await self.stake_tracker._create_new_tranche(
                            hotkey=hotkey,
                            coldkey=coldkey,
                            amount=stake_amount,
                            is_emission=False,  # Initial stake is manual, not emissions
                            timestamp=timestamp
                        )
                        
                        # Create initial stake metrics
                        await self.db_manager.execute_query("""
                            INSERT INTO stake_metrics 
                            (hotkey, coldkey, total_stake, manual_stake, earned_stake, last_update)
                            VALUES (?, ?, ?, ?, ?, ?)
                        """, (
                            hotkey,
                            coldkey,
                            stake_amount,
                            stake_amount,  # All initial stake is manual
                            0.0,  # No earned stake initially
                            timestamp
                        ))
                        
                        # Record in history
                        await self.db_manager.execute_query("""
                            INSERT INTO stake_change_history 
                            (timestamp, hotkey, coldkey, amount, change_type, flow_type, 
                            prev_total_stake, new_total_stake,
                            prev_manual_stake, new_manual_stake,
                            prev_earned_stake, new_earned_stake)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """, (
                            timestamp,
                            hotkey,
                            coldkey,
                            stake_amount,
                            "metagraph_init",
                            INFLOW,
                            0.0,  # prev_total_stake
                            stake_amount,  # new_total_stake
                            0.0,  # prev_manual_stake
                            stake_amount,  # new_manual_stake
                            0.0,  # prev_earned_stake
                            0.0,  # new_earned_stake
                        ))
                        
                        init_count += 1
                        logger.info(f"Initialized stake for {hotkey} from metagraph: {stake_amount:.6f} TAO")
                    
                    # Update aggregated metrics
                    await self.stake_tracker._update_aggregated_tranche_metrics(hotkey)
                    await self.stake_tracker._update_coldkey_metrics(coldkey)
            
            logger.info(f"Metagraph initialization complete: {init_count} new entries, {update_count} updates")
            return True
            
        except Exception as e:
            logger.error(f"Error initializing from metagraph: {e}")
            return False
    
    async def handle_deregistered_keys(self, deregistered_hotkeys: List[str] = None):
        """
        Handle deregistered keys by cleaning up their data.
        
        This method:
        1. Identifies deregistered miners by comparing the metagraph with the database
        2. Deletes all data associated with deregistered miners
        
        Args:
            deregistered_hotkeys: List of specific hotkeys to deregister.
                If None, will compare metagraph with database to find deregistered miners.
                
        Returns:
            int: Number of deregistered miners whose data was cleaned up
        """
        try:
            # If no specific hotkeys provided, identify deregistered miners
            if deregistered_hotkeys is None:
                # Get current hotkeys from metagraph
                metagraph = self.subtensor.metagraph(self.subnet_id)
                metagraph.sync(subtensor=self.subtensor)
                current_hotkeys = set(metagraph.hotkeys)
                
                # Get all hotkeys in our database
                results = await self.db_manager.fetch_all("""
                    SELECT DISTINCT hotkey FROM stake_metrics
                """)
                db_hotkeys = set(row['hotkey'] for row in results) if results else set()
                
                # Find hotkeys that are in our database but not in the metagraph (deregistered)
                deregistered_hotkeys = list(db_hotkeys - current_hotkeys)
            
            if not deregistered_hotkeys:
                logger.info("No deregistered miners found")
                return 0
            
            # For each deregistered hotkey, clean up all related data
            for hotkey in deregistered_hotkeys:
                # Get the coldkey first so we can update coldkey metrics later
                coldkey_result = await self.db_manager.fetch_one("""
                    SELECT coldkey FROM stake_metrics WHERE hotkey = ?
                """, (hotkey,))
                
                coldkey = coldkey_result['coldkey'] if coldkey_result else None
                
                # Delete from stake_metrics
                await self.db_manager.execute_query("""
                    DELETE FROM stake_metrics WHERE hotkey = ?
                """, (hotkey,))
                
                # Delete from aggregated_tranche_metrics
                await self.db_manager.execute_query("""
                    DELETE FROM aggregated_tranche_metrics WHERE hotkey = ?
                """, (hotkey,))
                
                # Get all tranche IDs for this hotkey
                tranche_results = await self.db_manager.fetch_all("""
                    SELECT id FROM stake_tranches WHERE hotkey = ?
                """, (hotkey,))
                
                if tranche_results:
                    tranche_ids = [row['id'] for row in tranche_results]
                    
                    # Delete from stake_tranche_exits for all associated tranches
                    for tranche_id in tranche_ids:
                        await self.db_manager.execute_query("""
                            DELETE FROM stake_tranche_exits WHERE tranche_id = ?
                        """, (tranche_id,))
                
                # Delete from stake_tranches
                await self.db_manager.execute_query("""
                    DELETE FROM stake_tranches WHERE hotkey = ?
                """, (hotkey,))
                
                # Delete from stake_change_history
                await self.db_manager.execute_query("""
                    DELETE FROM stake_change_history WHERE hotkey = ?
                """, (hotkey,))
                
                logger.info(f"Cleaned up data for deregistered miner: {hotkey}")
                
                # Update coldkey metrics if we know the coldkey
                if coldkey:
                    # Check if this coldkey has any remaining hotkeys
                    remaining_hotkeys = await self.stake_tracker.get_all_hotkeys_for_coldkey(coldkey)
                    
                    if remaining_hotkeys:
                        # Update coldkey metrics since some hotkeys were removed
                        await self.stake_tracker._update_coldkey_metrics(coldkey)
                    else:
                        # No remaining hotkeys, delete coldkey metrics
                        await self.db_manager.execute_query("""
                            DELETE FROM coldkey_metrics WHERE coldkey = ?
                        """, (coldkey,))
                        
                        logger.info(f"Removed coldkey metrics for {coldkey} (no remaining hotkeys)")
            
            logger.info(f"Deregistration cleanup complete: removed data for {len(deregistered_hotkeys)} miners")
            return len(deregistered_hotkeys)
            
        except Exception as e:
            logger.error(f"Error handling deregistered keys: {e}")
            return 0
    
    async def cleanup_deregistered_miner(self, hotkey: str) -> bool:
        """
        Clean up all data for a specific deregistered miner.
        
        This method is designed to be called from the validator when a 
        miner is deregistered, ensuring data is properly cleaned up.
        
        Args:
            hotkey: Hotkey of the deregistered miner
            
        Returns:
            bool: True if cleanup was successful
        """
        try:
            logger.info(f"Cleaning up vesting data for deregistered miner: {hotkey}")
            count = await self.handle_deregistered_keys(deregistered_hotkeys=[hotkey])
            success = count > 0
            
            if success:
                logger.info(f"Successfully cleaned up vesting data for {hotkey}")
            else:
                logger.warning(f"No data found to clean up for {hotkey}")
                
            return success
        except Exception as e:
            logger.error(f"Error cleaning up vesting data for {hotkey}: {e}")
            return False 