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
import json
import os
import time
from substrateinterface import SubstrateInterface
import argparse

from bettensor.validator.database.database_manager import DatabaseManager
from bettensor.validator.vesting.blockchain_monitor import BlockchainMonitor
from bettensor.validator.vesting.stake_tracker import StakeTracker
from bettensor.validator.vesting.transaction_monitor import TransactionMonitor
from bettensor.validator.vesting.stake_tracker import INFLOW, OUTFLOW, NEUTRAL, EMISSION
from bettensor.validator.vesting.database_schema import create_vesting_tables_async

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
        thread_priority: str = "low",
        explicit_endpoint: str = None
    ):
        """
        Initialize the vesting system.
        
        Args:
            subtensor: The subtensor client.
            subnet_id: The subnet ID to monitor.
            db_manager: The database manager to use for storage.
            minimum_stake: The minimum stake required for multiplier.
            retention_window_days: The number of days to consider for retention calculation.
            retention_target: The target retention percentage (0-1) for full multiplier.
            max_multiplier: The maximum multiplier to apply to miner weights.
            use_background_thread: Whether to use a background thread for periodic updates.
            query_interval_seconds: How often to query blockchain for updates (in seconds).
            transaction_query_interval_seconds: How often to query for transactions (in seconds).
            detailed_transaction_tracking: Whether to use detailed transaction tracking.
            thread_priority: Thread priority ("low", "normal", "high").
            explicit_endpoint: Optional explicit endpoint URL for blockchain queries.
        """
        self.subtensor = subtensor
        self.subnet_id = subnet_id
        self.db_manager = db_manager
        self.minimum_stake = minimum_stake
        self.retention_window_days = retention_window_days
        self.retention_target = retention_target
        self.max_multiplier = max_multiplier
        self.use_background_thread = use_background_thread
        self.query_interval_seconds = query_interval_seconds
        self.transaction_query_interval_seconds = transaction_query_interval_seconds
        self.detailed_transaction_tracking = detailed_transaction_tracking
        self.thread_priority = thread_priority
        self.explicit_endpoint = explicit_endpoint
        self.validator_take = 0.18  # Default validator take percentage
        
        # Log initialization parameters
        logger.info(f"Initializing VestingSystem for subnet {subnet_id}")
        logger.info(f"Parameters: min_stake={minimum_stake}, window={retention_window_days}d, target={retention_target}, max_mult={max_multiplier}")
        logger.info(f"Threading: use_bg_thread={use_background_thread}, thread_priority={thread_priority}")
        logger.info(f"Intervals: query={query_interval_seconds}s, tx_query={transaction_query_interval_seconds}s")
        logger.info(f"Explicit endpoint: {explicit_endpoint}")
        
        # Initialize blockchain monitor
        self.blockchain_monitor = BlockchainMonitor(
            subtensor=subtensor,
            subnet_id=subnet_id,
            db_manager=db_manager,
            query_interval_seconds=query_interval_seconds
        )
        
        # Initialize transaction monitor (if detailed tracking is enabled)
        if detailed_transaction_tracking:
            try:
                # Override: Use subnet 30 for transaction monitoring regardless of validator subnet
                transaction_monitor_subnet_id = 30
                logger.info(f"Creating transaction monitor with subnet_id: {transaction_monitor_subnet_id} (overridden from validator subnet {subnet_id})")
                
                # Create a dedicated subtensor instance connected to Finney for transaction monitoring
                try:
                    import bittensor as bt
                    import argparse
                    
                    # Create a proper config object for Finney network
                    parser = argparse.ArgumentParser()
                    bt.subtensor.add_args(parser)
                    config = bt.config(parser)
                    
                    # Explicitly set the network to Finney mainnet
                    config.subtensor.network = "finney"
                    finney_mainnet_endpoint = "wss://entrypoint-finney.opentensor.ai:443"
                    config.subtensor.chain_endpoint = finney_mainnet_endpoint  # Explicit Finney mainnet endpoint
                    
                    # Create a dedicated subtensor instance for transaction monitoring
                    logger.info("Creating dedicated subtensor instance connected to Finney mainnet for transaction monitoring")
                    logger.info(f"Using Finney mainnet endpoint: {config.subtensor.chain_endpoint}")
                    tx_monitor_subtensor = bt.subtensor(config=config)
                    logger.info(f"Successfully created dedicated subtensor instance for transaction monitoring: {tx_monitor_subtensor.network} at {tx_monitor_subtensor.chain_endpoint}")
                except Exception as e:
                    logger.error(f"Failed to create dedicated subtensor for transaction monitoring: {e}")
                    logger.debug(traceback.format_exc())
                    logger.info("Falling back to using validator's subtensor instance")
                    tx_monitor_subtensor = subtensor
                    finney_mainnet_endpoint = None
                
                self.transaction_monitor = TransactionMonitor(
                    subtensor=tx_monitor_subtensor,  # Use the dedicated Finney-connected subtensor
                    subnet_id=transaction_monitor_subnet_id,  # Use subnet 30 specifically for transaction monitoring
                    db_manager=db_manager,
                    verbose=True,  # Enable verbose logging to help debug
                    explicit_endpoint=finney_mainnet_endpoint  # Override with the Finney mainnet endpoint
                )
                
                # Start the transaction monitoring
                logger.info("Starting transaction monitor")
                self.transaction_monitor.start_monitoring(thread_nice_value=10 if thread_priority.lower() == 'low' else 0)
                logger.info(f"Transaction monitor started - monitoring subnet {transaction_monitor_subnet_id} on {tx_monitor_subtensor.network} network")
                    
            except Exception as e:
                logger.error(f"Failed to create transaction monitor: {e}")
                logger.debug(traceback.format_exc())
                self.transaction_monitor = None
                
        else:
            logger.info("Detailed transaction tracking disabled")
            self.transaction_monitor = None
        
        # Cache for minimum stake requirement
        self._minimum_stake_cache = None
        self._minimum_stake_last_updated = None
        self._minimum_stake_ttl = 3600  # 1 hour
        
        # Initialize stake tracker
        self.stake_tracker = StakeTracker(self.db_manager)
        
        # Event handlers
        self._epoch_boundary_handlers = []
    
    async def initialize(self) -> bool:
        """
        Initialize the vesting system and verify the database state.
            
        Returns:
            bool: True if initialization was successful
        """
        try:
            logger.info("Initializing vesting system...")
            
            # Create the tables if they don't exist
            logger.debug("Creating or checking vesting tables using execute_query")
            tables_created = await create_vesting_tables_async(self.db_manager)
            if not tables_created:
                logger.error("Failed to create vesting tables")
                return False
                
            # Verify the database state
            await self._verify_database_state()
            
            # Initialize the stake tracker
            await self.stake_tracker.initialize()
            
            # Check if transaction monitor is properly initialized
            if self.transaction_monitor:
                if not hasattr(self.transaction_monitor, 'substrate') or not self.transaction_monitor.substrate:
                    logger.warning("Transaction monitor is not fully initialized, attempting to initialize it")
                    try:
                        # Try to initialize the transaction monitor if it's not already initialized
                        if hasattr(self.transaction_monitor, 'initialize'):
                            await self.transaction_monitor.initialize()
                    except Exception as e:
                        logger.error(f"Failed to initialize transaction monitor: {e}")
            
            # Check if we have state info
            state_records = await self.db_manager.fetch_all(
                "SELECT * FROM vesting_module_state"
            )
            
            if not state_records:
                # Set default state
                start_block = 0
                if self.transaction_monitor:
                    try:
                        # Check if transaction monitor is initialized
                        if not hasattr(self.transaction_monitor, 'substrate') or not self.transaction_monitor.substrate:
                            logger.warning("Transaction monitor not fully initialized, attempting to initialize it")
                            if hasattr(self.transaction_monitor, 'initialize'):
                                await self.transaction_monitor.initialize()
                        
                        # Get current block number from the transaction monitor
                        if hasattr(self.transaction_monitor, 'get_current_block_number'):
                            start_block = self.transaction_monitor.get_current_block_number()
                            logger.info(f"Setting start_block to current block: {start_block}")
                        else:
                            logger.warning("Transaction monitor does not have get_current_block_number method")
                    except Exception as e:
                        logger.warning(f"Failed to get current block number: {e}, using 0 as fallback")
                        start_block = 0
                
                # Create vesting system state record
                system_state = {
                    "netuid": self.subnet_id,
                    "last_validated_block": start_block,
                    "valid_validators": "",
                    "validator_permit": "default",
                    "validator_take": self.validator_take,
                    "validator_logits": "",
                    "validator_trust": "",
                    "start_block": start_block,
                    "min_stake_requirement": self.minimum_stake
                }
                
                # Insert as JSON in module_data field
                await self.db_manager.execute_query(
                    """
                    INSERT INTO vesting_module_state (
                        module_name, last_block, last_timestamp, last_epoch, module_data
                    ) VALUES (?, ?, ?, ?, ?)
                    """,
                    (
                        "vesting_system", start_block, time.time(), 0, json.dumps(system_state)
                    )
                )
            else:
                # Update with current settings if needed
                state = state_records[0]
                
                # Check if there's a module_data field with our settings
                if 'module_data' in state and state['module_data']:
                    try:
                        system_state = json.loads(state['module_data'])
                        
                        # Check if we need to update any settings
                        update_needed = False
                        
                        if 'validator_take' in system_state and system_state['validator_take'] != self.validator_take:
                            system_state['validator_take'] = self.validator_take
                            update_needed = True
                            
                        if 'min_stake_requirement' in system_state and system_state['min_stake_requirement'] != self.minimum_stake:
                            system_state['min_stake_requirement'] = self.minimum_stake
                            update_needed = True
                        
                        if update_needed:
                            await self.db_manager.execute_query(
                                "UPDATE vesting_module_state SET module_data = ?, last_timestamp = ? WHERE module_name = ?",
                                (json.dumps(system_state), time.time(), "vesting_system")
                            )
                    except Exception as e:
                        logger.error(f"Error updating vesting system state: {e}")
            
            logger.info(f"✅ Vesting system initialized for netuid {self.subnet_id}")
            return True
            
        except Exception as e:
            logger.error(f"Error initializing vesting system: {e}")
            return False
    
    async def _ensure_tables_exist(self):
        """
        Ensure all required vesting tables exist in the database.
        
        Returns:
            bool: True if all tables exist
        """
        try:
            # Import here to avoid circular imports
            from bettensor.validator.vesting.database_schema import create_vesting_tables_async
            
            # Use the dedicated function to create all vesting tables
            logger.info("Creating or checking vesting tables - with fix for minimum_stake parameter")
            success = await create_vesting_tables_async(self.db_manager)
            if success:
                logger.info("Vesting tables created or verified successfully")
            else:
                logger.warning("Failed to create or verify vesting tables")
            
            # Double-check that tables exist
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
            
            # Check current data state
            should_clear = False
            if force_refresh:
                # Check current data state
                try:
                    # Check miner_metrics
                    metrics_result = await self.db_manager.fetch_one("""
                        SELECT COUNT(*) as count, COALESCE(SUM(total_stake), 0) as total_stake
                        FROM miner_metrics
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
                    
                    logger.info(f"Current miner_metrics: {metrics_count} rows, {metrics_stake:.2f} total stake")
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
                    await self.db_manager.execute_query("DELETE FROM miner_metrics")
                    await self.db_manager.execute_query("DELETE FROM stake_transactions")
                    await self.db_manager.execute_query("DELETE FROM stake_balance_changes")
                    await self.db_manager.execute_query("DELETE FROM stake_metrics")
                    await self.db_manager.execute_query("DELETE FROM coldkey_metrics")
                    await self.db_manager.execute_query("DELETE FROM stake_change_history")
                    await self.db_manager.execute_query("DELETE FROM vesting_module_state")
                    logger.info("Successfully cleared existing stake data")
                    existing_hotkeys = set()  # No existing hotkeys after clearing
                except Exception as e:
                    logger.error(f"Error clearing stake data: {e}")
                    # Continue anyway, as we'll use upserts
            
            # Get all existing hotkeys in miner_metrics
            results = await self.db_manager.fetch_all("""
                SELECT hotkey FROM miner_metrics
            """)
            existing_hotkeys = {row['hotkey'] for row in results} if results else set()
            logger.info(f"Found {len(existing_hotkeys)} existing hotkeys in miner_metrics")
            
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
                    
                    # Add miner_metrics entry
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
                    FROM miner_metrics
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
                # Continue with miner_metrics if possible
            
            # Add miner_metrics
            try:
                # We use a different approach to avoid issues with column ordering
                for metric in metrics:
                    await self.db_manager.execute_query("""
                        INSERT INTO miner_metrics
                        (hotkey, coldkey, total_stake, manual_stake, earned_stake, first_stake_timestamp, last_update)
                        VALUES (:hotkey, :coldkey, :total_stake, :manual_stake, :earned_stake, :first_stake_timestamp, :last_update)
                        ON CONFLICT(hotkey) DO UPDATE SET
                        coldkey = :coldkey,
                        total_stake = :total_stake,
                        manual_stake = :manual_stake,
                        earned_stake = :earned_stake,
                        last_update = :last_update
                    """, metric)
                logger.info(f"Added {len(metrics)} miner_metrics entries")
            except Exception as e:
                logger.error(f"Error updating miner_metrics: {e}")
            
            # Update coldkey metrics
            try:
                # Get unique coldkeys from the metrics
                unique_coldkeys = set(metric['coldkey'] for metric in metrics if metric['coldkey'])
                logger.info(f"Updating metrics for {len(unique_coldkeys)} unique coldkeys")
                
                # Update metrics for each coldkey
                for coldkey in unique_coldkeys:
                    try:
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
                    except Exception as e:
                        logger.error(f"Error updating metrics for coldkey {coldkey}: {e}")
                
                logger.info(f"Updated metrics for {len(unique_coldkeys)} coldkeys")
            except Exception as e:
                logger.error(f"Error updating coldkey metrics: {e}")
                # Continue with verification even if coldkey metrics update fails
            
            # Verify initialization
            verification = await self._verify_initialization()
            if verification['success']:
                logger.info(f"Successfully initialized {len(stakes)} miners from metagraph")
                logger.info(f"Total stakers: {verification['total_stakers']}, Total stake: {verification['total_stake']:.2f}")
                
                # Set initialization flag in vesting_module_state
                try:
                    # Get current system state record
                    result = await self.db_manager.fetch_one("""
                        SELECT module_data FROM vesting_module_state
                        WHERE module_name = 'vesting_system'
                    """)
                    
                    if result and result['module_data']:
                        # Update existing record
                        try:
                            system_state = json.loads(result['module_data'])
                            system_state['metagraph_initialized'] = True
                            
                            await self.db_manager.execute_query("""
                                UPDATE vesting_module_state 
                                SET module_data = ?, last_timestamp = ?
                                WHERE module_name = 'vesting_system'
                            """, (json.dumps(system_state), int(time.time())))
                            
                            logger.info("Updated metagraph_initialized flag to true in vesting_module_state")
                        except Exception as e:
                            logger.error(f"Error updating metagraph_initialized flag: {e}")
                    else:
                        # Create new record
                        system_state = {
                            "netuid": self.subnet_id,
                            "last_validated_block": 0,
                            "validator_take": self.validator_take,
                            "min_stake_requirement": self.minimum_stake,
                            "metagraph_initialized": True
                        }
                        
                        await self.db_manager.execute_query("""
                            INSERT INTO vesting_module_state
                            (module_name, last_block, last_timestamp, last_epoch, module_data)
                            VALUES (?, ?, ?, ?, ?)
                        """, ('vesting_system', 0, time.time(), 0, json.dumps(system_state)))
                        
                        logger.info("Created vesting_module_state record with metagraph_initialized flag")
                except Exception as e:
                    logger.error(f"Error setting metagraph_initialized flag: {e}")
                
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
        Verify that the initialization was successful by checking the miner_metrics.
        
        Returns:
            dict: Verification result with success, staker count, and total stake
        """
        try:
            # Verify that miner_metrics has entries with non-zero stake
            result = await self.db_manager.fetch_one("""
                SELECT COUNT(*) AS count, SUM(total_stake) AS total
                FROM miner_metrics
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
                    SELECT DISTINCT hotkey FROM miner_metrics
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
                    SELECT coldkey FROM miner_metrics WHERE hotkey = ?
                """, (hotkey,))
                coldkey = coldkey_result['coldkey'] if coldkey_result else None
                
                # Delete from miner_metrics
                await self.db_manager.execute_query("""
                    DELETE FROM miner_metrics WHERE hotkey = ?
                """, (hotkey,))
                
                # Delete from stake_tranches
                await self.db_manager.execute_query("""
                    DELETE FROM stake_tranches WHERE hotkey = ?
                """, (hotkey,))
                
                # Delete from stake_transactions
                await self.db_manager.execute_query("""
                    DELETE FROM stake_transactions WHERE hotkey = ?
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
                    'miner_metrics': {'count': 0, 'total_stake': 0},
                    'coldkey_metrics': {'count': 0, 'total_stake': 0},
                    'stake_transactions': {'count': 0},
                    'stake_tranches': {'count': 0, 'active_count': 0, 'total_remaining': 0},
                    'vesting_module_state': {'count': 0}
                },
                'errors': []
            }
            
            # Check if tables exist
            for table in [
                'miner_metrics', 'stake_tranches', 'vesting_module_state'
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
            
            # Check if metagraph initialization is marked in vesting_module_state
            try:
                result = await self.db_manager.fetch_one("""
                    SELECT module_data FROM vesting_module_state 
                    WHERE module_name = 'vesting_system'
                """)
                if result and result['module_data']:
                    try:
                        module_data = json.loads(result['module_data'])
                        info['metagraph_initialized'] = module_data.get('metagraph_initialized', False)
                    except Exception as e:
                        info['errors'].append(f"Error parsing module_data: {str(e)}")
                else:
                    info['metagraph_initialized'] = False
            except Exception as e:
                info['errors'].append(f"Error checking initialization flag: {str(e)}")
            
            # Get miner_metrics stats
            try:
                result = await self.db_manager.fetch_one("""
                    SELECT COUNT(*) as count, SUM(total_stake) as total 
                    FROM miner_metrics
                """)
                if result:
                    info['tables']['miner_metrics'] = {
                        'count': result['count'] or 0,
                        'total_stake': round(result['total'] or 0, 4)
                    }
            except Exception as e:
                info['errors'].append(f"Error getting miner_metrics: {str(e)}")
            
            # Get stake tranches stats
            try:
                result = await self.db_manager.fetch_one("""
                    SELECT COUNT(*) as count, SUM(remaining_amount) as total 
                    FROM stake_tranches
                """)
                if result:
                    info['tables']['stake_tranches'] = {
                        'count': result['count'] or 0,
                        'total_stake': round(result['total'] or 0, 4)
                    }
            except Exception as e:
                info['errors'].append(f"Error getting stake tranches: {str(e)}")
            
            # Set overall system status based on errors and data
            if info['errors']:
                info['system_status'] = 'error'
            elif (info['tables']['miner_metrics']['count'] == 0 or
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
    
    async def update_all_coldkey_metrics(self) -> bool:
        """
        Force an update of all coldkey metrics by scanning all miner_metrics entries.
        
        Returns:
            bool: True if update was successful
        """
        try:
            # Get unique coldkeys from miner_metrics
            coldkeys_result = await self.db_manager.fetch_all("""
                SELECT DISTINCT coldkey FROM miner_metrics WHERE coldkey IS NOT NULL
            """)
            
            if not coldkeys_result:
                logger.warning("No coldkeys found in miner_metrics table")
                return False
                
            coldkeys = [row['coldkey'] for row in coldkeys_result]
            logger.info(f"Updating metrics for {len(coldkeys)} coldkeys")
            
            updated_count = 0
            
            # Update metrics for each coldkey
            for coldkey in coldkeys:
                try:
                    # Get all hotkeys for this coldkey
                    hotkeys_result = await self.db_manager.fetch_all("""
                        SELECT hotkey, total_stake, manual_stake, earned_stake
                        FROM miner_metrics
                        WHERE coldkey = ?
                    """, (coldkey,))  # Use a tuple with a trailing comma for a single parameter
                    
                    if not hotkeys_result:
                        logger.warning(f"No hotkeys found for coldkey {coldkey}")
                        continue
                        
                    # Calculate aggregated metrics
                    hotkeys = [row['hotkey'] for row in hotkeys_result]
                    total_stake = sum(row['total_stake'] for row in hotkeys_result)
                    manual_stake = sum(row['manual_stake'] for row in hotkeys_result)
                    earned_stake = sum(row['earned_stake'] for row in hotkeys_result)
                    timestamp = int(datetime.now(timezone.utc).timestamp())
                    
                    # Update coldkey metrics
                    await self.db_manager.execute_query("""
                        INSERT OR REPLACE INTO coldkey_metrics
                        (coldkey, total_stake, manual_stake, earned_stake, hotkey_count, last_update)
                        VALUES (?, ?, ?, ?, ?, ?)
                    """, (coldkey, total_stake, manual_stake, earned_stake, len(hotkeys), timestamp))
                    
                    updated_count += 1
                    
                except Exception as e:
                    logger.error(f"Error updating metrics for coldkey {coldkey}: {e}")
            
            logger.info(f"Successfully updated metrics for {updated_count}/{len(coldkeys)} coldkeys")
            return updated_count > 0
                
        except Exception as e:
            logger.error(f"Error updating all coldkey metrics: {e}")
            return False
    
    async def _verify_database_state(self, verbose: bool = False):
        """Verify the current state of the vesting database."""
        try:
            # Check that all required tables exist
            table_names = [
                'miner_metrics',
                'coldkey_metrics',
                'stake_transactions',
                'stake_tranches',
                'vesting_module_state'
            ]
            
            tables_checked = 0
            for table_name in table_names:
                try:
                    await self.db_manager.fetch_one(f"SELECT COUNT(*) as count FROM {table_name}")
                    tables_checked += 1
                except Exception as e:
                    logger.error(f"Table {table_name} check failed: {e}")
                    return False
            
            if verbose:
                logger.info(f"All {tables_checked} vesting tables exist and are accessible")
            
            # Check miner_metrics
            try:
                metrics_result = await self.db_manager.fetch_one("""
                    SELECT COUNT(*) as count, SUM(total_stake) as total_stake 
                    FROM miner_metrics
                """)
                
                metrics_count = metrics_result['count']
                metrics_stake = metrics_result['total_stake'] or 0
                
                if verbose:
                    logger.info(f"Current miner_metrics: {metrics_count} rows, {metrics_stake:.2f} total stake")
            except Exception as e:
                logger.error(f"Error checking miner_metrics: {e}")
                return False
            
            # Check tranche consistency if requested
            if verbose:
                try:
                    tranches_result = await self.db_manager.fetch_one("""
                        SELECT COUNT(*) as count, 
                               SUM(remaining_amount) as total_remaining,
                               SUM(CASE WHEN is_active = 1 THEN 1 ELSE 0 END) as active_count
                        FROM stake_tranches
                    """)
                    
                    tranches_count = tranches_result['count']
                    active_tranches = tranches_result['active_count']
                    total_remaining = tranches_result['total_remaining'] or 0
                    
                    logger.info(f"Current tranches: {tranches_count} total, {active_tranches} active, {total_remaining:.2f} TAO remaining")
                    
                    # Verify total stake matches
                    stake_diff = abs(metrics_stake - total_remaining)
                    if stake_diff > 0.01:
                        logger.warning(f"Stake mismatch: metrics={metrics_stake:.2f}, tranches={total_remaining:.2f}, diff={stake_diff:.2f}")
                except Exception as e:
                    logger.error(f"Error checking tranches: {e}")
            
            return True
        except Exception as e:
            logger.error(f"Error verifying database state: {e}")
            return False
    
    async def get_hotkeys_with_minimum_stake(self, minimum_stake: float = None) -> List[str]:
        """
        Get all hotkeys with at least the minimum stake.
        
        Args:
            minimum_stake: Minimum stake amount required (defaults to self.minimum_stake)
            
        Returns:
            List[str]: List of hotkeys with at least the minimum stake
        """
        min_stake = minimum_stake if minimum_stake is not None else self.minimum_stake
        
        try:
            # Get hotkeys from miner_metrics table with minimum stake
            results = await self.db_manager.fetch_all("""
                SELECT hotkey
                FROM miner_metrics
                WHERE total_stake >= ?
                ORDER BY total_stake DESC
            """, (min_stake,))
            
            return [row['hotkey'] for row in results]
            
        except Exception as e:
            logger.error(f"Error getting hotkeys with minimum stake: {e}")
            return []

    async def reset_vesting_database(self, keep_metagraph_data: bool = False) -> bool:
        """
        Reset (clear) all data in the vesting database tables.
        
        Args:
            keep_metagraph_data: If True, keep netuid and metagraph data
            
        Returns:
            bool: True if reset was successful
        """
        try:
            logger.warning("Resetting vesting database - ALL VESTING DATA WILL BE LOST")
            
            # Delete data from the main vesting tables
            await self.db_manager.execute_query("DELETE FROM miner_metrics")
            await self.db_manager.execute_query("DELETE FROM coldkey_metrics")
            await self.db_manager.execute_query("DELETE FROM stake_transactions")
            await self.db_manager.execute_query("DELETE FROM stake_tranches")
            
            if not keep_metagraph_data:
                await self.db_manager.execute_query("DELETE FROM vesting_module_state")
            
            logger.info("Successfully reset vesting database")
            
            # Reinitialize the system
            await self.stake_tracker.initialize()
            
            return True
        except Exception as e:
            logger.error(f"Error resetting vesting database: {e}")
            return False

    async def initialize_hotkeys_from_metagraph(self, metagraph, stake_info: Optional[dict] = None) -> bool:
        """
        Initialize stake metrics for all hotkeys from the metagraph.
        
        This method takes a snapshot of the current stake state in the metagraph
        and stores it in the vesting system database.
        
        Args:
            metagraph: Bittensor metagraph
            stake_info: Optional dictionary of stake info to use instead of metagraph
            
        Returns:
            bool: True if initialization was successful
        """
        try:
            # Get all existing hotkeys in miner_metrics
            existing_result = await self.db_manager.fetch_all("""
                SELECT hotkey FROM miner_metrics
            """)
            existing_hotkeys = [row['hotkey'] for row in existing_result]
            logger.info(f"Found {len(existing_hotkeys)} existing hotkeys in miner_metrics")
            
            # Get all hotkeys from metagraph
            if stake_info is None:
                logger.info("Getting hotkeys and stake from metagraph")
                hotkeys = metagraph.hotkeys
                stakes = metagraph.S.tolist()
                try:
                    coldkeys = metagraph.coldkeys
                except:
                    # Older metagraph might not have coldkeys
                    coldkeys = ["unknown"] * len(hotkeys)
                    
                # Convert to a dictionary
                stake_info = {
                    hotkeys[i]: {
                        'stake': float(stakes[i]),
                        'coldkey': coldkeys[i] if i < len(coldkeys) else "unknown"
                    } for i in range(len(hotkeys))
                }
            
            # Count totals for logging
            total_hotkeys = len(stake_info)
            total_stake = sum(info['stake'] for info in stake_info.values())
            new_count = 0
            new_stake = 0
            updated_count = 0
            updated_stake = 0
            
            # Current timestamp
            now = datetime.now(timezone.utc)
            timestamp = int(now.timestamp())
            
            # Get existing data to avoid duplicate work
            existing_stake = {}
            if existing_hotkeys:
                existing_data = await self.db_manager.fetch_all("""
                    SELECT hotkey, total_stake, coldkey
                    FROM miner_metrics
                    WHERE hotkey IN ({})
                """.format(','.join(['?'] * len(existing_hotkeys))), existing_hotkeys)
                
                for row in existing_data:
                    existing_stake[row['hotkey']] = {
                        'stake': row['total_stake'],
                        'coldkey': row['coldkey']
                    }
            
            # Process each hotkey
            for hotkey, info in stake_info.items():
                stake = info['stake']
                coldkey = info['coldkey']
                
                # Skip if stake is zero or too small
                if stake < 0.001:
                    continue
                    
                # Check if hotkey already exists with same stake and coldkey
                if hotkey in existing_stake:
                    existing_info = existing_stake[hotkey]
                    if abs(existing_info['stake'] - stake) < 0.001 and existing_info['coldkey'] == coldkey:
                        # Skip if stake and coldkey are unchanged
                        continue
                
                if hotkey in existing_hotkeys:
                    # Update existing
                    updated_count += 1
                    updated_stake += stake
                else:
                    # Add new
                    new_count += 1
                    new_stake += stake
                
                # Insert or update miner_metrics table
                await self.db_manager.execute_query("""
                    INSERT OR REPLACE INTO miner_metrics 
                    (hotkey, coldkey, total_stake, manual_stake, earned_stake, first_stake_timestamp, last_update,
                     total_tranches, active_tranches, avg_tranche_age, oldest_tranche_age, manual_tranches, emission_tranches)
                    VALUES (?, ?, ?, ?, 0, ?, ?, 0, 0, 0, 0, 0, 0)
                """, [hotkey, coldkey, stake, stake, timestamp, timestamp])
                
                # Create a corresponding tranche
                await self.db_manager.execute_query("""
                    INSERT INTO stake_tranches
                    (hotkey, coldkey, initial_amount, remaining_amount, entry_timestamp, is_emission, last_update, is_active)
                    VALUES (?, ?, ?, ?, ?, 0, ?, 1)
                """, [hotkey, coldkey, stake, stake, timestamp, timestamp])
                
                # Add a stake transaction record
                await self.db_manager.execute_query("""
                    INSERT INTO stake_transactions
                    (block_number, timestamp, transaction_type, flow_type, hotkey, coldkey, 
                     amount, stake_after, manual_stake_after, earned_stake_after, change_type)
                    VALUES (0, ?, 'initial_import', 'inflow', ?, ?, ?, ?, ?, 0, 'manual')
                """, [timestamp, hotkey, coldkey, stake, stake, stake])
            
            # Update coldkey metrics
            await self._update_coldkey_metrics()
            
            # Log results
            logger.info(f"Initialized {total_hotkeys} hotkeys with {total_stake:.2f} total stake from metagraph")
            logger.info(f"Added {new_count} new hotkeys with {new_stake:.2f} stake")
            logger.info(f"Updated {updated_count} existing hotkeys with {updated_stake:.2f} stake")
            
            # Verify that miner_metrics has entries with non-zero stake
            verification_result = await self.db_manager.fetch_one("""
                SELECT COUNT(*) as count, SUM(total_stake) as total_stake
                FROM miner_metrics
                WHERE total_stake > 0
            """)
            
            if verification_result and verification_result['count'] > 0:
                logger.info(f"Verification successful: {verification_result['count']} miners with {verification_result['total_stake']:.2f} total stake")
                return True
            else:
                logger.error("Verification failed: No miners with stake found after initialization")
                return False
                
        except Exception as e:
            logger.error(f"Error initializing hotkeys from metagraph: {e}")
            logger.debug(traceback.format_exc())
            return False
    
    async def remove_hotkey(self, hotkey: str) -> bool:
        """
        Remove a hotkey from the vesting system.
        
        Args:
            hotkey: Hotkey to remove
            
        Returns:
            bool: True if removal was successful
        """
        try:
            # Get the coldkey to update coldkey metrics after removal
            coldkey_result = await self.db_manager.fetch_one("""
                SELECT coldkey FROM miner_metrics WHERE hotkey = ?
            """, [hotkey])
            
            coldkey = coldkey_result['coldkey'] if coldkey_result else None
            
            # Delete from miner_metrics
            await self.db_manager.execute_query("""
                DELETE FROM miner_metrics WHERE hotkey = ?
            """, [hotkey])
            
            # Delete from stake_tranches
            await self.db_manager.execute_query("""
                DELETE FROM stake_tranches WHERE hotkey = ?
            """, [hotkey])
            
            # Delete from stake_transactions
            await self.db_manager.execute_query("""
                DELETE FROM stake_transactions WHERE hotkey = ?
            """, [hotkey])
            
            logger.info(f"Removed hotkey {hotkey} from vesting system")
            
            # Update coldkey metrics if a coldkey was found
            if coldkey:
                try:
                    await self._update_coldkey_metrics(coldkey)
                    logger.debug(f"Updated metrics for coldkey {coldkey} after removing hotkey {hotkey}")
                except Exception as e:
                    logger.error(f"Error updating coldkey metrics after hotkey removal: {e}")
            
            return True
        except Exception as e:
            logger.error(f"Error removing hotkey {hotkey}: {e}")
            return False

    async def get_database_info(self) -> Dict:
        """
        Get information about the vesting database.
        
        Returns:
            Dict: Information about tables and metrics
        """
        info = {
            'tables': {
                'miner_metrics': {'count': 0, 'total_stake': 0},
                'coldkey_metrics': {'count': 0, 'total_stake': 0},
                'stake_transactions': {'count': 0},
                'stake_tranches': {'count': 0, 'active_count': 0, 'total_remaining': 0},
                'vesting_module_state': {'count': 0}
            },
            'last_updated': None
        }
        
        # List of tables to check
        tables = [
            'miner_metrics', 'coldkey_metrics', 'stake_transactions',
            'stake_tranches', 'vesting_module_state'
        ]
        
        try:
            # Check each table
            for table_name in tables:
                count_result = await self.db_manager.fetch_one(f"""
                    SELECT COUNT(*) as count FROM {table_name}
                """)
                
                info['tables'][table_name]['count'] = count_result['count']
            
            # Get total stake from miner_metrics
            stake_result = await self.db_manager.fetch_one("""
                SELECT SUM(total_stake) as total_stake FROM miner_metrics
            """)
            
            info['tables']['miner_metrics']['total_stake'] = float(stake_result['total_stake'] or 0)
            
            # Get stake from coldkey_metrics
            coldkey_result = await self.db_manager.fetch_one("""
                SELECT SUM(total_stake) as total_stake FROM coldkey_metrics
            """)
            
            info['tables']['coldkey_metrics']['total_stake'] = float(coldkey_result['total_stake'] or 0)
            
            # Get tranche stats
            tranche_result = await self.db_manager.fetch_one("""
                SELECT 
                    COUNT(*) as count,
                    SUM(CASE WHEN is_active = 1 THEN 1 ELSE 0 END) as active_count,
                    SUM(remaining_amount) as total_remaining
                FROM stake_tranches
            """)
            
            info['tables']['stake_tranches']['active_count'] = tranche_result['active_count']
            info['tables']['stake_tranches']['total_remaining'] = float(tranche_result['total_remaining'] or 0)
            
            # Get last updated timestamp from module state
            timestamp_result = await self.db_manager.fetch_one("""
                SELECT MAX(last_timestamp) as last_updated FROM vesting_module_state
            """)
            
            if timestamp_result and timestamp_result['last_updated']:
                info['last_updated'] = timestamp_result['last_updated']
            
            return info
        except Exception as e:
            logger.error(f"Error getting database info: {e}")
            return info

    async def _update_hotkey_coldkey_mapping(self, metagraph) -> bool:
        """
        Update hotkey to coldkey mappings from the metagraph.
        
        Args:
            metagraph: Bittensor metagraph
            
        Returns:
            bool: True if update was successful
        """
        try:
            # Get hotkeys and coldkeys from metagraph
            hotkeys = metagraph.hotkeys
            coldkeys = metagraph.coldkeys if hasattr(metagraph, 'coldkeys') else None
            
            if not coldkeys or len(coldkeys) != len(hotkeys):
                logger.warning("Metagraph does not have valid coldkeys")
                return False
                
            updates = 0
            total = len(hotkeys)
            
            logger.info(f"Updating hotkey-coldkey mappings for {total} hotkeys")
            
            # Store hotkey to coldkey relationships in miner_metrics table if needed
            for i, (hotkey, coldkey) in enumerate(zip(hotkeys, coldkeys)):
                try:
                    # Check if we already have this hotkey and its coldkey
                    existing = await self.db_manager.fetch_one("""
                        SELECT coldkey FROM miner_metrics WHERE hotkey = ?
                    """, [hotkey])
                    
                    if not existing:
                        # New hotkey - insert with zero stake
                        await self.db_manager.execute_query("""
                            INSERT INTO miner_metrics
                            (hotkey, coldkey, total_stake, manual_stake, earned_stake, 
                            first_stake_timestamp, last_update, total_tranches, active_tranches,
                            avg_tranche_age, oldest_tranche_age, manual_tranches, emission_tranches)
                            VALUES (?, ?, 0, 0, 0, ?, ?, 0, 0, 0, 0, 0, 0)
                        """, [hotkey, coldkey, int(datetime.now(timezone.utc).timestamp()), 
                             int(datetime.now(timezone.utc).timestamp())])
                        logger.debug(f"Created new miner_metrics entry for hotkey {hotkey}, coldkey {coldkey}")
                        updates += 1
                    elif existing['coldkey'] != coldkey:
                        # Update coldkey if different
                        await self.db_manager.execute_query("""
                            UPDATE miner_metrics SET coldkey = ? WHERE hotkey = ?
                        """, [coldkey, hotkey])
                        updates += 1
                except Exception as miner_metrics_error:
                    logger.error(f"Error updating miner_metrics for hotkey {hotkey}, coldkey {coldkey}: {miner_metrics_error}")
            
            logger.info(f"Updated {updates}/{total} hotkey-coldkey mappings")
            
            # Update all coldkey metrics
            await self.update_all_coldkey_metrics()
            
            return True
            
        except Exception as e:
            logger.error(f"Error updating hotkey-coldkey mappings: {e}")
            return False

    def start_background_thread(self, thread_priority: str = 'low'):
        """
        Start a background thread for monitoring.
        
        This starts an asyncio-based thread that runs the update() method periodically.
        
        Args:
            thread_priority: Priority level for the thread
        """
        try:
            import threading
            import time
            import asyncio
            
            if self.monitoring_thread is not None and self.monitoring_thread.is_alive():
                logger.warning("Background thread is already running")
                return
                
            def _background_thread():
                logger.info(f"Starting background thread with priority {thread_priority}")
                
                # Try to set thread priority
                if thread_priority.lower() == 'high':
                    nice_value = -10
                elif thread_priority.lower() == 'normal':
                    nice_value = 0
                else:
                    nice_value = 10
                    
                try:
                    import os
                    os.nice(nice_value)
                    logger.info(f"Set thread priority with nice value {nice_value}")
                except Exception as e:
                    logger.warning(f"Could not set thread priority: {e}")
                
                # Create a new event loop for this thread
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                
                # Run the monitoring method periodically
                while not self.stop_event.is_set():
                    try:
                        if self.stopping:
                            logger.info("Stopping background thread due to stop flag")
                            break
                            
                        # Call update method
                        logger.debug("Running periodic update in background thread")
                        loop.run_until_complete(self.update(None))
                        
                        # Sleep for the query interval
                        sleep_time = self.query_interval_seconds
                        logger.debug(f"Sleeping for {sleep_time} seconds until next update")
                        
                        # Use a loop with small sleeps to allow for faster shutdown
                        for _ in range(sleep_time):
                            if self.stop_event.is_set():
                                break
                            time.sleep(1)
                            
                    except Exception as e:
                        logger.error(f"Error in background thread: {e}")
                        logger.debug(traceback.format_exc())
                        time.sleep(10)  # Sleep a bit longer on error
                
                loop.close()
                logger.info("Background thread exiting")
            
            # Create and start the thread
            self.monitoring_thread = threading.Thread(
                target=_background_thread,
                daemon=True,
                name="VestingMonitor"
            )
            self.monitoring_thread.start()
            logger.info(f"Started background thread: {self.monitoring_thread.name}")
            
        except Exception as e:
            logger.error(f"Error starting background threads: {e}")
            return False 

