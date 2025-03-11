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
import traceback

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
        transaction_query_interval_seconds: int = 60,
        detailed_transaction_tracking: bool = True,
        thread_priority: str = "low"
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
            transaction_query_interval_seconds: How often to check for transactions (seconds)
            detailed_transaction_tracking: Whether to use detailed transaction tracking
            thread_priority: Thread priority - "low", "normal", or "high"
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
        self.thread_priority = thread_priority
        self.query_interval_seconds = query_interval_seconds
        self.transaction_query_interval_seconds = transaction_query_interval_seconds
        
        # Adjust thread scheduling based on priority
        self.thread_nice_value = {
            "low": 10,      # Lower CPU priority
            "normal": 0,    # Normal priority
            "high": -10     # Higher CPU priority (use with caution)
        }.get(thread_priority, 0)
        
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
            logger.debug(f"Initializing transaction monitor with query_interval_seconds={transaction_query_interval_seconds}")
            # Use mainnet endpoint for transaction monitoring with a unique connection
            self.transaction_monitor = TransactionMonitor(
                subtensor=subtensor,
                subnet_id=subnet_id,
                db_manager=db_manager,
                verbose=True,  # Enable verbose logging
                query_interval_seconds=transaction_query_interval_seconds,
                explicit_endpoint="wss://entrypoint-finney.opentensor.ai:443"  # Explicitly use mainnet endpoint
            )
        else:
            self.transaction_monitor = None
        
        # Cache for minimum stake requirement
        self._minimum_stake_cache = None
        self._minimum_stake_last_updated = None
        self._minimum_stake_ttl = 3600  # 1 hour
        
        # Event handlers
        self._epoch_boundary_handlers = []
    
    async def initialize(self, auto_init_metagraph: bool = True, **kwargs):
        """
        Initialize the vesting system.
        
        This method initializes the blockchain monitor and stake tracker,
        and ensures all required database tables exist. It also optionally
        initializes miner data from the metagraph.
        
        Args:
            auto_init_metagraph: Whether to automatically initialize miner data from metagraph
            **kwargs: Additional parameters that override init parameters (e.g., minimum_stake)
            
        Returns:
            bool: True if initialization was successful
        """
        try:
            logger.info("Initializing vesting system")
            
            # Update configuration with any provided kwargs
            for key, value in kwargs.items():
                if hasattr(self, key):
                    logger.debug(f"Updating {key} from {getattr(self, key)} to {value}")
                    setattr(self, key, value)
            
            logger.debug(f"Vesting system configuration: use_background_thread={self.use_background_thread}, detailed_transaction_tracking={self.detailed_transaction_tracking}")
            
            # Ensure all required tables exist
            logger.debug("Ensuring all required tables exist")
            tables_exist = await self._ensure_tables_exist()
            if not tables_exist:
                logger.error("Failed to ensure required tables exist")
                return False
                
            # Initialize stake tracker
            logger.debug("Initializing stake tracker")
            self.stake_tracker = StakeTracker(self.db_manager)
            if not await self.stake_tracker.initialize():
                logger.error("Failed to initialize stake tracker")
                return False
                
            # Initialize blockchain monitor
            logger.debug(f"Initializing blockchain monitor with query_interval_seconds={self.query_interval_seconds}")
            self.blockchain_monitor = BlockchainMonitor(
                subtensor=self.subtensor,
                subnet_id=self.subnet_id,
                db_manager=self.db_manager,
                query_interval_seconds=self.query_interval_seconds,
                auto_start_thread=False  # Don't auto-start
            )
            if not await self.blockchain_monitor.initialize():
                logger.error("Failed to initialize blockchain monitor")
                return False
                
            # Initialize transaction monitor if detailed tracking is enabled
            if self.detailed_transaction_tracking and self.transaction_monitor is None:
                logger.debug(f"Initializing transaction monitor with query_interval_seconds={self.transaction_query_interval_seconds}")
                # Use mainnet endpoint for transaction monitoring with a unique connection
                # Make sure this is a completely separate connection from the blockchain monitor
                self.transaction_monitor = TransactionMonitor(
                    subtensor=self.subtensor,
                    subnet_id=self.subnet_id,
                    db_manager=self.db_manager,
                    verbose=True,  # Enable verbose logging
                    query_interval_seconds=self.transaction_query_interval_seconds,
                    explicit_endpoint="wss://entrypoint-finney.opentensor.ai:443"  # Explicitly use mainnet endpoint
                )
                
                # Force creation of a new substrate connection to avoid thread conflicts
                try:
                    from substrateinterface import SubstrateInterface
                    # Initialize the transaction_monitor's substrate interface here to keep it separate
                    self.transaction_monitor.substrate = None
                except Exception as e:
                    logger.warning(f"Could not pre-clear transaction monitor substrate: {e}")
            
            if self.detailed_transaction_tracking and self.transaction_monitor:
                if not await self.transaction_monitor.initialize():
                    logger.error("Failed to initialize transaction monitor")
                    return False
                    
                # Register the stake change handler
                logger.debug("Registering stake change handler")
                self._register_stake_change_handler()
            
            # Initialize from metagraph if requested
            if auto_init_metagraph:
                logger.info("Auto-initializing from metagraph")
                success = await self.initialize_from_metagraph()
                if not success:
                    logger.warning("Failed to initialize from metagraph")
                    # Continue anyway
                else:
                    logger.debug("Successfully initialized from metagraph")
            
            # Start the background thread if requested
            if self.use_background_thread:
                logger.debug(f"Starting background threads with priority: {self.thread_priority}")
                success = self.start_background_thread(self.thread_priority)
                if success:
                    logger.debug("Background threads started successfully")
                else:
                    logger.warning("Failed to start background threads")
            else:
                logger.debug("Background threads disabled, not starting monitoring")
                
            logger.info("Vesting system initialization completed successfully")
            return True
        except Exception as e:
            logger.error(f"Failed to initialize vesting system: {e}")
            logger.exception("Initialization error details:")
            return False
    
    async def _ensure_tables_exist(self):
        """
        Ensure all required vesting tables exist in the database.
        
        Returns:
            bool: True if all tables exist
        """
        try:
            # Get list of all required tables
            required_tables = [
                'blockchain_state',
                'stake_transactions',
                'stake_balance_changes',
                'stake_metrics',
                'coldkey_metrics', 
                'stake_change_history',
                'vesting_module_state',
                'stake_tranches'
            ]
            
            # Check if each table exists
            existing_tables = []
            for table in required_tables:
                result = await self.db_manager.fetch_one(f"""
                    SELECT name FROM sqlite_master 
                    WHERE type='table' AND name='{table}'
                """)
                if result:
                    existing_tables.append(table)
            
            # Log the results
            if len(existing_tables) == len(required_tables):
                logger.info(f"All {len(required_tables)} required vesting tables exist")
                return True
            else:
                missing = set(required_tables) - set(existing_tables)
                logger.warning(f"Missing vesting tables: {', '.join(missing)}")
                
                # Try to create tables explicitly
                from bettensor.validator.vesting.database_schema import create_vesting_tables_async
                logger.info("Attempting to create missing vesting tables...")
                await create_vesting_tables_async(self.db_manager)
                
                # Verify tables were created
                for table in missing:
                    result = await self.db_manager.fetch_one(f"""
                        SELECT name FROM sqlite_master 
                        WHERE type='table' AND name='{table}'
                    """)
                    if not result:
                        logger.error(f"Failed to create table: {table}")
                        return False
                
                logger.info("Successfully created all missing vesting tables")
                return True
                
        except Exception as e:
            logger.error(f"Error checking vesting tables: {e}")
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
        
        This method stops all background threads and cleans up resources.
        It should be called when the validator is shutting down.
        
        Returns:
            bool: True if shutdown was successful
        """
        try:
            logger.info("Shutting down vesting system")
            
            # Stop blockchain monitor thread
            if hasattr(self, 'blockchain_monitor'):
                logger.debug("Stopping blockchain monitor thread")
                success = self.blockchain_monitor.stop_background_thread(timeout=10)
                if not success:
                    logger.warning("Failed to gracefully stop blockchain monitor thread")
            
            # Stop transaction monitor thread
            if self.detailed_transaction_tracking and hasattr(self, 'transaction_monitor') and self.transaction_monitor:
                logger.debug("Stopping transaction monitor thread")
                success = self.transaction_monitor.stop_monitoring()
                if not success:
                    logger.warning("Failed to gracefully stop transaction monitor thread")
            
            # Ensure all database operations are completed
            if hasattr(self, 'db_manager'):
                logger.debug("Finalizing database operations")
                try:
                    # Perform any final database operations
                    await asyncio.wait_for(
                        self._save_monitoring_state(),
                        timeout=5
                    )
                except asyncio.TimeoutError:
                    logger.warning("Timed out waiting for final database operations")
            
            logger.info("Vesting system shut down successfully")
            return True
            
        except Exception as e:
            logger.error(f"Error shutting down vesting system: {e}")
            return False
            
    async def _save_monitoring_state(self):
        """
        Save the current monitoring state to the database.
        
        This method is called during shutdown to ensure all state is preserved.
        """
        try:
            # Save blockchain monitor state
            if hasattr(self, 'blockchain_monitor'):
                await self.blockchain_monitor._save_last_processed_blocks()
                
            # Save any other state that needs to be persisted
            # (specific to your implementation)
            
            return True
        except Exception as e:
            logger.error(f"Error saving monitoring state: {e}")
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
    
    async def initialize_from_metagraph(self, force_refresh=False):
        """
        Initialize vesting system data from the current metagraph.
        
        This method creates initial stake metrics entries for all miners
        in the metagraph. It's typically called once during initialization.
        
        Args:
            force_refresh (bool): If True, check if the tables are empty before
                                  reinitializing. Only clears existing data if 
                                  tables are empty or have minimal data.
        
        Returns:
            bool: True if initialization was successful, False otherwise
        """
        try:
            logger.info(f"Initializing vesting system from metagraph (force_refresh={force_refresh})")
            
            # Get current metagraph
            self._metagraph = self.subtensor.metagraph(netuid=self.subnet_id)
            
            # Check if stake_tranches table exists
            try:
                result = await self.db_manager.fetch_one("""
                    SELECT name FROM sqlite_master 
                    WHERE type='table' AND name='stake_tranches'
                """)
                if not result:
                    logger.warning("stake_tranches table does not exist, cannot initialize from metagraph")
                    return False
            except Exception as e:
                logger.error(f"Error checking for stake_tranches table: {e}")
                return False
            
            # Check if tables have substantial data before clearing
            should_clear = False
            if force_refresh:
                # Check current data state
                try:
                    # Check stake_metrics
                    metrics_result = await self.db_manager.fetch_one("""
                        SELECT COUNT(*) as count, COALESCE(SUM(total_stake), 0) as total_stake
                        FROM stake_metrics
                    """)
                    metrics_count = metrics_result['count'] if metrics_result else 0
                    metrics_stake = metrics_result['total_stake'] if metrics_result else 0
                    
                    # Check stake_tranches
                    tranches_result = await self.db_manager.fetch_one("""
                        SELECT COUNT(*) as count, COALESCE(SUM(remaining_amount), 0) as total_stake
                        FROM stake_tranches
                    """)
                    tranches_count = tranches_result['count'] if tranches_result else 0
                    tranches_stake = tranches_result['total_stake'] if tranches_result else 0
                    
                    logger.info(f"Current stake_metrics: {metrics_count} rows, {metrics_stake:.2f} total stake")
                    logger.info(f"Current stake_tranches: {tranches_count} rows, {tranches_stake:.2f} total stake")
                    
                    # Only clear if tables are empty or have minimal data (less than 5% of miners in metagraph)
                    metagraph_miners = len(self._metagraph.hotkeys)
                    if metrics_count == 0 or tranches_count == 0:
                        logger.info("Tables are empty or missing data, will clear and reinitialize")
                        should_clear = True
                    elif metrics_count < (0.05 * metagraph_miners) or metrics_stake < 0.1:
                        logger.info("Tables have minimal data, will clear and reinitialize")
                        should_clear = True
                    else:
                        logger.info("Tables have substantial existing data, will preserve and only add missing miners")
                        should_clear = False
                except Exception as e:
                    logger.error(f"Error checking existing data: {e}")
                    # Be conservative - don't clear if we can't check
                    should_clear = False
            
            # Clear data if needed
            if should_clear:
                logger.info("Clearing existing stake data for reinitialization")
                try:
                    # Only truncate data, don't drop tables
                    await self.db_manager.execute_query("DELETE FROM stake_tranches")
                    await self.db_manager.execute_query("DELETE FROM stake_metrics")
                    await self.db_manager.execute_query("DELETE FROM aggregated_tranche_metrics")
                    logger.info("Successfully cleared existing stake data")
                    existing_hotkeys = set()  # No existing hotkeys after clearing
                except Exception as e:
                    logger.error(f"Error clearing stake data: {e}")
                    # Continue anyway, as we'll use upserts
            
            # Get all existing hotkeys in stake_metrics
            results = await self.db_manager.fetch_all("""
                SELECT hotkey FROM stake_metrics
            """)
            existing_hotkeys = {row['hotkey'] for row in results} if results else set()
            logger.info(f"Found {len(existing_hotkeys)} existing hotkeys in stake_metrics")
            
            # Prepare data for initial stake metrics
            now = datetime.now(timezone.utc)
            timestamp = int(now.timestamp())
            stakes = []
            metrics = []
            
            # Log the number of hotkeys in the metagraph
            logger.info(f"Metagraph contains {len(self._metagraph.hotkeys)} hotkeys")
            
            # Process miners in the metagraph
            for idx, hotkey in enumerate(self._metagraph.hotkeys):
                if hotkey in existing_hotkeys:
                    continue
                    
                stake = float(self._metagraph.S[idx])
                
                # Handle missing owner_by_hotkey method
                if hasattr(self._metagraph, 'owner_by_hotkey'):
                    coldkey = self._metagraph.owner_by_hotkey(hotkey)
                elif hasattr(self._metagraph, 'coldkeys'):
                    # If metagraph has coldkeys attribute, use it
                    coldkey = self._metagraph.coldkeys[idx] if idx < len(self._metagraph.coldkeys) else None
                else:
                    # Fallback case
                    logger.warning(f"Cannot determine coldkey for hotkey {hotkey}, using placeholder")
                    coldkey = "unknown_coldkey"
                
                if stake > 0 and coldkey:
                    # Add stake tranche entry
                    stakes.append((
                        hotkey,
                        coldkey,
                        stake,
                        stake,
                        timestamp,
                        False,
                        timestamp
                    ))
                    
                    # Add stake metrics entry
                    metrics.append({
                        'hotkey': hotkey,
                        'coldkey': coldkey,
                        'total_stake': stake,
                        'manual_stake': stake,
                        'earned_stake': 0,
                        'first_stake_timestamp': timestamp,
                        'last_update': timestamp
                    })
            
            if not stakes:
                logger.info("No new miners to initialize")
                
                # Verify if we already have stakeholders with non-zero stake in the database
                result = await self.db_manager.fetch_one("""
                    SELECT COUNT(*) AS count, SUM(total_stake) AS total
                    FROM stake_metrics
                    WHERE total_stake > 0
                """)
                
                if result:
                    count = result['count'] or 0
                    total = result['total'] or 0
                    logger.info(f"Database already contains {count} stakeholders with {total:.2f} total stake")
                    
                    if count > 0:
                        # We already have stake data, no need to re-initialize
                        return True
                
                return True
                
            logger.info(f"Initializing {len(stakes)} new miners")
            
            # Add stake tranches
            try:
                await self.db_manager.executemany("""
                    INSERT INTO stake_tranches 
                    (hotkey, coldkey, initial_amount, remaining_amount, entry_timestamp, is_emission, last_update)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, stakes)
                logger.info(f"Added {len(stakes)} stake tranches")
            except Exception as e:
                logger.error(f"Error initializing from metagraph (stake_tranches): {e}")
                # Continue with stake metrics if possible
            
            # Add stake metrics
            try:
                # We use a different approach to avoid issues with column ordering
                for metric in metrics:
                    await self.db_manager.execute_query("""
                        INSERT INTO stake_metrics
                        (hotkey, coldkey, total_stake, manual_stake, earned_stake, first_stake_timestamp, last_update)
                        VALUES (:hotkey, :coldkey, :total_stake, :manual_stake, :earned_stake, :first_stake_timestamp, :last_update)
                        ON CONFLICT(hotkey) DO UPDATE SET
                        coldkey = :coldkey,
                        total_stake = :total_stake,
                        manual_stake = :manual_stake,
                        earned_stake = :earned_stake,
                        last_update = :last_update
                    """, metric)
                logger.info(f"Added {len(metrics)} stake metrics entries")
            except Exception as e:
                logger.error(f"Error updating stake metrics: {e}")
            
            # Update aggregated_tranche_metrics
            try:
                # Create initial aggregated tranche metrics
                for metric in metrics:
                    await self.db_manager.execute_query("""
                        INSERT INTO aggregated_tranche_metrics
                        (hotkey, coldkey, total_tranches, active_tranches, avg_tranche_age, 
                         oldest_tranche_age, total_tranche_amount, manual_tranches, 
                         emission_tranches, emission_amount, manual_amount, last_update)
                        VALUES (:hotkey, :coldkey, 1, 1, 0, 0, :total_stake, 1, 0, 0, :manual_stake, :last_update)
                        ON CONFLICT(hotkey) DO UPDATE SET
                        coldkey = :coldkey,
                        total_tranche_amount = :total_stake,
                        manual_amount = :manual_stake,
                        last_update = :last_update
                    """, metric)
                logger.info(f"Added {len(metrics)} aggregated tranche metrics")
            except Exception as e:
                logger.error(f"Error initializing aggregated tranche metrics: {e}")
            
            # Update coldkey metrics
            try:
                # Get unique coldkeys from the metrics
                unique_coldkeys = set(metric['coldkey'] for metric in metrics if metric['coldkey'])
                logger.info(f"Updating metrics for {len(unique_coldkeys)} unique coldkeys")
                
                # Update metrics for each coldkey
                for coldkey in unique_coldkeys:
                    # Calculate aggregated metrics for this coldkey
                    coldkey_metrics = [m for m in metrics if m['coldkey'] == coldkey]
                    total_stake = sum(m['total_stake'] for m in coldkey_metrics)
                    manual_stake = sum(m['manual_stake'] for m in coldkey_metrics)
                    earned_stake = sum(m['earned_stake'] for m in coldkey_metrics)
                    num_hotkeys = len(coldkey_metrics)
                    
                    # Insert or update coldkey metrics
                    await self.db_manager.execute_query("""
                        INSERT INTO coldkey_metrics 
                        (coldkey, total_stake, manual_stake, earned_stake, hotkey_count, last_update)
                        VALUES (?, ?, ?, ?, ?, ?)
                        ON CONFLICT(coldkey) DO UPDATE SET
                        total_stake = excluded.total_stake,
                        manual_stake = excluded.manual_stake,
                        earned_stake = excluded.earned_stake,
                        hotkey_count = excluded.hotkey_count,
                        last_update = excluded.last_update
                    """, (
                        coldkey,
                        total_stake,
                        manual_stake,
                        earned_stake,
                        num_hotkeys,
                        int(datetime.now(timezone.utc).timestamp())
                    ))
                
                logger.info(f"Updated metrics for {len(unique_coldkeys)} coldkeys")
            except Exception as e:
                logger.error(f"Error updating coldkey metrics: {e}")
                # Continue with verification even if coldkey metrics update fails
            
            # Verify initialization
            verification = await self._verify_initialization()
            if verification['success']:
                logger.info(f"Successfully initialized {len(stakes)} miners from metagraph")
                logger.info(f"Total stakers: {verification['total_stakers']}, Total stake: {verification['total_stake']:.2f}")
                
                # Set initialization flag in blockchain_state
                try:
                    await self.db_manager.execute_query("""
                        INSERT OR REPLACE INTO blockchain_state (key, value)
                        VALUES ('vesting_metagraph_initialized', 'true')
                    """)
                    logger.info("Set vesting_metagraph_initialized flag to true")
                except Exception as e:
                    logger.error(f"Error setting initialization flag: {e}")
                
                # As a final safety measure, force an update of all coldkey metrics
                await self.update_all_coldkey_metrics()
                
                return True
            else:
                logger.warning(f"Verification failed: {verification['reason']}")
                return False
            
        except Exception as e:
            logger.error(f"Failed to initialize from metagraph: {e}")
            return False
            
    async def _verify_initialization(self):
        """
        Verify that the initialization was successful by checking the stake metrics.
        
        Returns:
            dict: Verification result with success, staker count, and total stake
        """
        try:
            # Verify that stake_metrics has entries with non-zero stake
            result = await self.db_manager.fetch_one("""
                SELECT COUNT(*) AS count, SUM(total_stake) AS total
                FROM stake_metrics
                WHERE total_stake > 0
            """)
            
            if not result:
                return {
                    'success': False,
                    'reason': 'No result from verification query',
                    'total_stakers': 0,
                    'total_stake': 0
                }
                
            count = result['count'] or 0
            total = result['total'] or 0
            
            if count == 0:
                return {
                    'success': False,
                    'reason': 'No stakers with non-zero stake found',
                    'total_stakers': count,
                    'total_stake': total
                }
                
            # Check tranches too
            tranches_result = await self.db_manager.fetch_one("""
                SELECT COUNT(*) AS count, SUM(remaining_amount) AS total
                FROM stake_tranches
                WHERE remaining_amount > 0
            """)
            
            if not tranches_result or tranches_result['count'] == 0:
                return {
                    'success': False,
                    'reason': 'No stake tranches with non-zero remaining amount found',
                    'total_stakers': count,
                    'total_stake': total
                }
                
            # Check coldkey_metrics too
            coldkey_result = await self.db_manager.fetch_one("""
                SELECT COUNT(*) AS count, SUM(total_stake) AS total
                FROM coldkey_metrics
                WHERE total_stake > 0
            """)
            
            if not coldkey_result or coldkey_result['count'] == 0:
                logger.warning("No coldkey metrics with non-zero stake found, they may need to be updated")
                # We'll consider this a warning but not a failure
            else:
                logger.info(f"Found {coldkey_result['count']} coldkeys with {coldkey_result['total']:.2f} total stake")
                
            return {
                'success': True,
                'total_stakers': count,
                'total_stake': total
            }
            
        except Exception as e:
            logger.error(f"Error verifying initialization: {e}")
            return {
                'success': False,
                'reason': f'Error during verification: {str(e)}',
                'total_stakers': 0,
                'total_stake': 0
            }
    
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
    
    async def get_diagnostic_info(self) -> Dict[str, Any]:
        """
        Get diagnostic information about the vesting system.
        
        This method collects various statistics and status information 
        about the vesting system and its data.
        
        Returns:
            Dict[str, Any]: Diagnostic information
        """
        try:
            info = {
                'system_status': 'operational',
                'metagraph_initialized': False,
                'tables': {
                    'stake_metrics': {'count': 0, 'total_stake': 0},
                    'stake_tranches': {'count': 0, 'total_stake': 0},
                    'aggregated_tranche_metrics': {'count': 0, 'total_stake': 0},
                    'coldkey_metrics': {'count': 0},
                    'stake_change_history': {'count': 0},
                    'stake_transactions': {'count': 0}
                },
                'errors': []
            }
            
            # Check if tables exist
            for table in [
                'stake_metrics', 'stake_tranches', 'aggregated_tranche_metrics',
                'coldkey_metrics', 'stake_change_history', 'stake_transactions'
            ]:
                try:
                    result = await self.db_manager.fetch_one(f"""
                        SELECT name FROM sqlite_master 
                        WHERE type='table' AND name='{table}'
                    """)
                    if not result:
                        info['errors'].append(f"Table {table} does not exist")
                except Exception as e:
                    info['errors'].append(f"Error checking {table} table: {str(e)}")
            
            # Check if vesting_metagraph_initialized flag is set
            try:
                result = await self.db_manager.fetch_one("""
                    SELECT value FROM blockchain_state 
                    WHERE key = 'vesting_metagraph_initialized'
                """)
                info['metagraph_initialized'] = result and result['value'] == 'true'
            except Exception as e:
                info['errors'].append(f"Error checking initialization flag: {str(e)}")
            
            # Get stake metrics stats
            try:
                result = await self.db_manager.fetch_one("""
                    SELECT COUNT(*) as count, SUM(total_stake) as total 
                    FROM stake_metrics
                    WHERE total_stake > 0
                """)
                if result:
                    info['tables']['stake_metrics'] = {
                        'count': result['count'] or 0,
                        'total_stake': round(result['total'] or 0, 4)
                    }
            except Exception as e:
                info['errors'].append(f"Error getting stake metrics: {str(e)}")
            
            # Get stake tranches stats
            try:
                result = await self.db_manager.fetch_one("""
                    SELECT COUNT(*) as count, SUM(remaining_amount) as total 
                    FROM stake_tranches
                    WHERE remaining_amount > 0
                """)
                if result:
                    info['tables']['stake_tranches'] = {
                        'count': result['count'] or 0,
                        'total_stake': round(result['total'] or 0, 4)
                    }
            except Exception as e:
                info['errors'].append(f"Error getting stake tranches: {str(e)}")
            
            # Get aggregated tranche metrics stats
            try:
                result = await self.db_manager.fetch_one("""
                    SELECT COUNT(*) as count, SUM(total_tranche_amount) as total 
                    FROM aggregated_tranche_metrics
                    WHERE total_tranche_amount > 0
                """)
                if result:
                    info['tables']['aggregated_tranche_metrics'] = {
                        'count': result['count'] or 0,
                        'total_stake': round(result['total'] or 0, 4)
                    }
            except Exception as e:
                info['errors'].append(f"Error getting aggregated metrics: {str(e)}")
            
            # Get coldkey metrics stats
            try:
                result = await self.db_manager.fetch_one("""
                    SELECT COUNT(*) as count, SUM(total_stake) as total_stake, SUM(hotkey_count) as total_hotkeys
                    FROM coldkey_metrics
                    WHERE total_stake > 0
                """)
                if result:
                    info['tables']['coldkey_metrics'] = {
                        'count': result['count'] or 0,
                        'total_stake': round(result['total_stake'] or 0, 4),
                        'total_hotkeys': result['total_hotkeys'] or 0
                    }
            except Exception as e:
                info['errors'].append(f"Error getting coldkey metrics: {str(e)}")
            
            # Get stake change history stats
            try:
                result = await self.db_manager.fetch_one("""
                    SELECT COUNT(*) as count FROM stake_change_history
                """)
                if result:
                    info['tables']['stake_change_history'] = {
                        'count': result['count'] or 0
                    }
            except Exception as e:
                info['errors'].append(f"Error getting stake change history: {str(e)}")
            
            # Get stake transactions stats
            try:
                result = await self.db_manager.fetch_one("""
                    SELECT COUNT(*) as count FROM stake_transactions
                """)
                if result:
                    info['tables']['stake_transactions'] = {
                        'count': result['count'] or 0
                    }
            except Exception as e:
                info['errors'].append(f"Error getting stake transactions: {str(e)}")
            
            # Set overall system status based on errors and data
            if info['errors']:
                info['system_status'] = 'error'
            elif (info['tables']['stake_metrics']['count'] == 0 or
                  info['tables']['stake_tranches']['count'] == 0):
                info['system_status'] = 'no_data'
            elif not info['metagraph_initialized']:
                info['system_status'] = 'not_initialized'
            
            return info
            
        except Exception as e:
            logger.error(f"Error getting diagnostic info: {e}")
            return {
                'system_status': 'error',
                'errors': [f"Failed to get diagnostic info: {str(e)}"]
            }
    
    async def update_all_coldkey_metrics(self):
        """
        Force an update of all coldkey metrics by scanning all stake_metrics entries.
        This ensures coldkey_metrics table is fully populated and up-to-date.
        """
        try:
            logger.info("Forcing update of all coldkey metrics")
            
            # Get unique coldkeys from stake_metrics
            results = await self.db_manager.fetch_all("""
                SELECT DISTINCT coldkey FROM stake_metrics WHERE coldkey IS NOT NULL
            """)
            
            if not results:
                logger.warning("No coldkeys found in stake_metrics table")
                return
                
            unique_coldkeys = [row['coldkey'] for row in results]
            logger.info(f"Found {len(unique_coldkeys)} unique coldkeys to update")
            
            # Update metrics for each coldkey using stake_tracker
            for coldkey in unique_coldkeys:
                try:
                    await self.stake_tracker._update_coldkey_metrics(coldkey)
                except Exception as e:
                    logger.error(f"Error updating metrics for coldkey {coldkey}: {e}")
            
            # Verify the results
            result = await self.db_manager.fetch_one("""
                SELECT COUNT(*) as count, SUM(total_stake) as total_stake, SUM(hotkey_count) as total_hotkeys
                FROM coldkey_metrics
            """)
            
            if result:
                logger.info(f"Updated {result['count']} coldkeys with {result['total_stake']:.2f} total stake " +
                          f"and {result['total_hotkeys']} hotkeys")
            else:
                logger.warning("No coldkey metrics found after update")
                
        except Exception as e:
            logger.error(f"Error updating all coldkey metrics: {e}")
    
    async def update_coldkey_hotkey_relationships(self):
        """
        Explicitly update coldkey-hotkey relationships based on metagraph data.
        
        This method ensures that coldkey_metrics is properly populated with
        relationships between coldkeys and their associated hotkeys.
        """
        try:
            logger.info("Updating coldkey-hotkey relationships from metagraph")
            
            # Get current metagraph
            self._metagraph = self.subtensor.metagraph(netuid=self.subnet_id)
            
            # Create a mapping of hotkeys to coldkeys
            hotkey_to_coldkey = {}
            coldkey_to_hotkeys = {}
            
            # Log the size of the metagraph
            logger.info(f"Metagraph contains {len(self._metagraph.hotkeys)} hotkeys")
            
            # Process each hotkey in the metagraph
            for idx, hotkey in enumerate(self._metagraph.hotkeys):
                # Get coldkey for this hotkey
                if hasattr(self._metagraph, 'owner_by_hotkey'):
                    coldkey = self._metagraph.owner_by_hotkey(hotkey)
                elif hasattr(self._metagraph, 'coldkeys') and idx < len(self._metagraph.coldkeys):
                    coldkey = self._metagraph.coldkeys[idx]
                else:
                    logger.warning(f"Cannot determine coldkey for hotkey {hotkey}")
                    continue
                    
                if coldkey:
                    hotkey_to_coldkey[hotkey] = coldkey
                    
                    if coldkey not in coldkey_to_hotkeys:
                        coldkey_to_hotkeys[coldkey] = []
                    coldkey_to_hotkeys[coldkey].append(hotkey)
            
            # Log the number of coldkeys found
            logger.info(f"Found {len(coldkey_to_hotkeys)} unique coldkeys in metagraph")
            
            # Store hotkey to coldkey relationships in stake_metrics table if needed
            for hotkey, coldkey in hotkey_to_coldkey.items():
                # Check if entry exists
                result = await self.db_manager.fetch_one("""
                    SELECT coldkey FROM stake_metrics WHERE hotkey = ?
                """, (hotkey,))
                
                if not result:
                    # Need to create a new entry with default values
                    await self.db_manager.execute_query("""
                        INSERT INTO stake_metrics
                        (hotkey, coldkey, total_stake, manual_stake, earned_stake, first_stake_timestamp, last_update)
                        VALUES (?, ?, 0, 0, 0, ?, ?)
                    """, (
                        hotkey,
                        coldkey,
                        int(datetime.now(timezone.utc).timestamp()),
                        int(datetime.now(timezone.utc).timestamp())
                    ))
                    logger.debug(f"Created new stake_metrics entry for hotkey {hotkey}, coldkey {coldkey}")
                elif result['coldkey'] != coldkey:
                    # Update coldkey if different
                    await self.db_manager.execute_query("""
                        UPDATE stake_metrics SET coldkey = ? WHERE hotkey = ?
                    """, (coldkey, hotkey))
                    logger.debug(f"Updated coldkey for hotkey {hotkey} from {result['coldkey']} to {coldkey}")
            
            # Now update coldkey_metrics table
            for coldkey, hotkeys in coldkey_to_hotkeys.items():
                # Update coldkey metrics
                await self.stake_tracker._update_coldkey_metrics(coldkey)
                
            # Verify results
            result = await self.db_manager.fetch_one("""
                SELECT COUNT(*) as count FROM coldkey_metrics
            """)
            coldkey_count = result['count'] if result else 0
            
            if coldkey_count > 0:
                logger.info(f"Successfully updated {coldkey_count} coldkey metrics records")
            else:
                logger.warning("No coldkey metrics records were created")
                
            # Return success/failure
            return coldkey_count > 0
                
        except Exception as e:
            logger.error(f"Error updating coldkey-hotkey relationships: {e}")
            logger.debug(traceback.format_exc())
            return False
    
    def start_background_thread(self, thread_priority: str = 'low'):
        """
        Start the background monitoring thread.
        
        Args:
            thread_priority: Priority of the monitoring thread ('low', 'normal', 'high')
        """
        try:
            # Convert priority string to nice value
            thread_nice_value = {
                'low': 10,
                'normal': 0,
                'high': -10
            }.get(thread_priority.lower(), 0)
            
            # Start blockchain monitoring thread
            if hasattr(self, 'blockchain_monitor') and self.blockchain_monitor is not None:
                self.blockchain_monitor.start_background_thread(thread_nice_value=thread_nice_value)
                logger.info(f"Started blockchain monitor thread (priority: {thread_priority})")
            
            # Start transaction monitoring thread if enabled
            if self.detailed_transaction_tracking and hasattr(self, 'transaction_monitor') and self.transaction_monitor is not None:
                success = self.transaction_monitor.start_monitoring(thread_nice_value=thread_nice_value)
                if not success:
                    logger.warning("Failed to start transaction monitoring thread")
                else:
                    logger.info(f"Started transaction monitoring thread (priority: {thread_priority})")
                    
            logger.info("Background monitoring threads started successfully")
            return True
        except Exception as e:
            logger.error(f"Error starting background threads: {e}")
            return False 