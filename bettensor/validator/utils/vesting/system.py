"""
Vesting system for Bettensor validators.

This module provides a comprehensive vesting system that rewards miners
for holding their stakes rather than immediately selling them.
"""

import logging
import asyncio
from typing import Optional, Dict, Any, List

import bittensor as bt
import numpy as np

from bettensor.validator.utils.database import DatabaseManager
from bettensor.validator.utils.vesting.core import (
    calculate_multiplier,
    VestingScheduler,
    StakeTracker
)
from bettensor.validator.utils.vesting.blockchain import SubtensorClient

logger = logging.getLogger(__name__)


class VestingSystem:
    """
    Complete vesting system for validators.
    
    This class integrates all components of the vesting system:
    1. Stake tracking
    2. Blockchain monitoring
    3. Vesting schedules and payments
    4. Score multipliers
    
    It provides a high-level interface for validators to integrate
    the vesting system into their scoring and rewards process.
    """
    
    def __init__(
        self,
        db_manager: DatabaseManager,
        metagraph: 'bt.metagraph.Metagraph',
        subnet_id: int,
        vesting_duration_days: int = 30,
        vesting_interval_days: int = 1,
        min_vesting_amount: float = 0.1,
        network: str = "finney",
        update_interval_seconds: int = 300
    ):
        """
        Initialize the vesting system.
        
        Args:
            db_manager: Database manager for persistent storage
            metagraph: Bittensor metagraph
            subnet_id: Subnet ID
            vesting_duration_days: Duration for vesting schedules in days
            vesting_interval_days: Interval between vesting payments in days
            min_vesting_amount: Minimum amount for creating a vesting schedule
            network: Bittensor network to connect to
            update_interval_seconds: Interval between background updates
        """
        self.db_manager = db_manager
        self.metagraph = metagraph
        self.subnet_id = subnet_id
        self.vesting_duration_days = vesting_duration_days
        self.vesting_interval_days = vesting_interval_days
        self.min_vesting_amount = min_vesting_amount
        self.network = network
        self.update_interval = update_interval_seconds
        
        # Initialize components
        self.subtensor_client = SubtensorClient(
            subnet_id=subnet_id,
            network=network
        )
        
        self.stake_tracker = StakeTracker(
            db_manager=db_manager,
            metagraph=metagraph,
            subtensor_client=self.subtensor_client
        )
        
        self.vesting_scheduler = VestingScheduler(
            db_manager=db_manager,
            vesting_duration_days=vesting_duration_days,
            vesting_interval_days=vesting_interval_days,
            min_vesting_amount=min_vesting_amount
        )
        
        # State
        self._running = False
        self._background_task = None
        self._scoring_system = None
        self._original_calculate_weights = None
    
    async def start(self):
        """Start the vesting system."""
        if self._running:
            logger.warning("Vesting system already running")
            return
            
        # Connect to blockchain
        connected = await self.subtensor_client.connect()
        if not connected:
            logger.error("Failed to connect to Subtensor. Vesting system not started.")
            return
            
        # Initialize database
        await self._initialize_database()
        
        # Load data
        await self.stake_tracker.update_key_associations()
        await self.stake_tracker.update_stake_history()
        
        # Start background task
        self._running = True
        self._background_task = asyncio.create_task(self._background_loop())
        
        logger.info("Vesting system started")
    
    async def stop(self):
        """Stop the vesting system."""
        if not self._running:
            logger.warning("Vesting system not running")
            return
            
        # Stop background task
        self._running = False
        if self._background_task:
            self._background_task.cancel()
            try:
                await self._background_task
            except asyncio.CancelledError:
                pass
            self._background_task = None
        
        # Restore original scoring if integrated
        if self._scoring_system and self._original_calculate_weights:
            self._scoring_system.calculate_weights = self._original_calculate_weights
            self._scoring_system = None
            self._original_calculate_weights = None
            
        logger.info("Vesting system stopped")
    
    async def _initialize_database(self):
        """Initialize database tables."""
        from bettensor.validator.utils.vesting.database.models import Base
        
        try:
            # Create tables
            engine = self.db_manager.get_engine()
            Base.metadata.create_all(engine)
            logger.info("Database tables initialized")
            
        except Exception as e:
            logger.error(f"Error initializing database: {e}")
    
    async def _background_loop(self):
        """Background loop for periodic updates."""
        try:
            while self._running:
                try:
                    # Update stake information
                    await self.stake_tracker.update_key_associations()
                    await self.stake_tracker.update_stake_history()
                    
                    # Process vesting payments
                    payments_processed = await self.vesting_scheduler.process_vesting_payments()
                    if payments_processed > 0:
                        logger.info(f"Processed {payments_processed} vesting payments")
                    
                    # Check for stake changes
                    stake_changes = await self.subtensor_client.detect_stake_changes()
                    
                    # Record stake transactions
                    for event_type, events in stake_changes.items():
                        for event in events:
                            # Record transactions for add_stake and remove_stake
                            # move_stake would need additional processing
                            if event_type in ["add_stake", "remove_stake"]:
                                await self.stake_tracker.record_stake_transaction(
                                    transaction_type=event_type,
                                    hotkey=event["hotkey"],
                                    coldkey=event["coldkey"],
                                    amount=event["amount"],
                                    extrinsic_hash="auto_detected",  # Not from blockchain
                                    block_number=0,  # Not from blockchain
                                    block_timestamp=event["timestamp"]
                                )
                    
                    # Update transaction history
                    transactions = await self.subtensor_client.query_transactions()
                    for tx in transactions:
                        if "transaction_type" in tx and tx["transaction_type"] in ["add_stake", "remove_stake", "move_stake"]:
                            await self.stake_tracker.record_stake_transaction(
                                transaction_type=tx["transaction_type"],
                                hotkey=tx["hotkey"],
                                coldkey=tx["coldkey"],
                                amount=tx["amount"],
                                extrinsic_hash=tx["extrinsic_hash"],
                                block_number=tx["block_number"],
                                block_timestamp=tx["block_timestamp"],
                                source_hotkey=tx.get("source_hotkey")
                            )
                    
                    # Check epoch boundary
                    epoch_changed = await self.subtensor_client.check_epoch_boundary()
                    if epoch_changed:
                        logger.info("Epoch boundary detected, performing full update")
                        # Add any epoch boundary specific logic here
                    
                except Exception as e:
                    logger.error(f"Error in background loop: {e}")
                
                # Wait for next update
                await asyncio.sleep(self.update_interval)
                
        except asyncio.CancelledError:
            # Task was cancelled, clean up
            logger.info("Background task cancelled")
            
        except Exception as e:
            logger.error(f"Background task failed: {e}")
    
    async def create_vesting_schedule(self, hotkey: str, amount: float):
        """
        Create a vesting schedule for a miner.
        
        Args:
            hotkey: Miner's hotkey
            amount: Amount to vest
            
        Returns:
            The ID of the created schedule or None if creation failed
        """
        try:
            # Get coldkey
            coldkey = ""
            async with self.db_manager.async_session() as session:
                from bettensor.validator.utils.vesting.database.models import HotkeyColdkeyAssociation
                result = await session.execute(
                    session.query(HotkeyColdkeyAssociation)
                    .filter(HotkeyColdkeyAssociation.hotkey == hotkey)
                )
                association = result.first()
                
                if association:
                    coldkey = association.coldkey
            
            if not coldkey:
                logger.warning(f"No coldkey found for hotkey {hotkey}")
                return None
            
            # Create vesting schedule
            schedule_id = await self.vesting_scheduler.create_vesting_schedule(
                hotkey=hotkey,
                coldkey=coldkey,
                amount=amount
            )
            
            return schedule_id
            
        except Exception as e:
            logger.error(f"Error creating vesting schedule: {e}")
            return None
    
    async def get_holding_metrics(self, hotkey: str):
        """
        Get holding metrics for a hotkey.
        
        Args:
            hotkey: Miner's hotkey
            
        Returns:
            Dict with holding metrics
        """
        try:
            # Calculate holding metrics
            holding_percentage, holding_duration = await self.stake_tracker.calculate_holding_metrics(hotkey)
            
            # Calculate multiplier
            multiplier = calculate_multiplier(holding_percentage, holding_duration)
            
            return {
                "hotkey": hotkey,
                "holding_percentage": holding_percentage,
                "holding_duration_days": holding_duration,
                "multiplier": multiplier
            }
            
        except Exception as e:
            logger.error(f"Error getting holding metrics: {e}")
            return {
                "hotkey": hotkey,
                "error": str(e)
            }
    
    async def get_vesting_summary(self, hotkey: str):
        """
        Get vesting summary for a hotkey.
        
        Args:
            hotkey: Miner's hotkey
            
        Returns:
            Dict with vesting summary
        """
        try:
            # Get holding metrics
            holding_metrics = await self.get_holding_metrics(hotkey)
            
            # Get vesting schedules
            vesting_summary = await self.vesting_scheduler.get_vesting_summary(hotkey)
            
            # Get stake changes
            stake_changes = await self.stake_tracker.get_stake_changes(hotkey)
            
            # Combine results
            summary = {
                **holding_metrics,
                **vesting_summary,
                **stake_changes
            }
            
            return summary
            
        except Exception as e:
            logger.error(f"Error getting vesting summary: {e}")
            return {
                "hotkey": hotkey,
                "error": str(e)
            }
    
    def integrate_with_scoring(self, scoring_system):
        """
        Integrate the vesting system with a scoring system.
        
        This patches the scoring system's calculate_weights method to
        apply vesting multipliers to scores.
        
        Args:
            scoring_system: The scoring system to integrate with
        """
        if self._scoring_system:
            logger.warning("Vesting system already integrated with a scoring system")
            return
            
        # Save original method
        self._scoring_system = scoring_system
        self._original_calculate_weights = scoring_system.calculate_weights
        
        # Replace with patched version
        scoring_system.calculate_weights = self._patched_calculate_weights
        
        logger.info("Vesting system integrated with scoring system")
    
    async def _patched_calculate_weights(self, day=None):
        """
        Patched version of calculate_weights that applies vesting multipliers.
        
        Args:
            day: The day to calculate weights for
            
        Returns:
            The modified weights with vesting multipliers applied
        """
        if not self._original_calculate_weights:
            logger.error("Original calculate_weights method not found")
            return None
            
        # Call original method
        weights = self._original_calculate_weights(day)
        
        if weights is None or not np.any(weights > 0):
            return weights
            
        try:
            # Make a copy of the weights
            modified_weights = weights.copy()
            
            # Get all UIDs with non-zero weights
            uids = np.where(modified_weights > 0)[0]
            
            # Apply multipliers for each UID
            for uid in uids:
                # Get hotkey
                hotkey = self.metagraph.hotkeys[uid]
                
                # Calculate holding metrics and multiplier
                holding_percentage, holding_duration = await self.stake_tracker.calculate_holding_metrics(hotkey)
                multiplier = calculate_multiplier(holding_percentage, holding_duration)
                
                # Apply multiplier
                if multiplier > 1.0:
                    modified_weights[uid] *= multiplier
            
            # Normalize weights
            if np.sum(modified_weights) > 0:
                total_weight = np.sum(modified_weights)
                modified_weights = modified_weights / total_weight
            
            return modified_weights
            
        except Exception as e:
            logger.error(f"Error applying vesting multipliers: {e}")
            return weights  # Return original weights on error 