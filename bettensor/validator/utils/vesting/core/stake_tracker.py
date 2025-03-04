"""
Stake tracking for the vesting rewards system.

This module provides functionality to track stake balances and calculate
holding metrics for miners.
"""

import logging
from datetime import datetime, timedelta
from typing import Dict, List, Set, Tuple, Optional, Any

import bittensor as bt
from sqlalchemy import and_, func

from bettensor.validator.utils.database import DatabaseManager
from bettensor.validator.utils.vesting.database.models import (
    HotkeyColdkeyAssociation,
    StakeHistory,
    StakeTransaction
)
from bettensor.validator.utils.vesting.blockchain import SubtensorClient

logger = logging.getLogger(__name__)


class StakeTracker:
    """
    Tracks stake balances and calculates holding metrics for miners.
    
    This class is responsible for:
    1. Tracking stake balances for each miner
    2. Maintaining hotkey-coldkey associations
    3. Recording stake history
    4. Calculating holding percentages and durations
    """
    
    def __init__(
        self,
        db_manager: DatabaseManager,
        metagraph: bt.metagraph,
        subtensor_client: SubtensorClient
    ):
        """
        Initialize the StakeTracker.
        
        Args:
            db_manager: Database manager for persistent storage
            metagraph: Bittensor metagraph for accessing stake information
            subtensor_client: Client for Subtensor blockchain interactions
        """
        self.db_manager = db_manager
        self.metagraph = metagraph
        self.subtensor_client = subtensor_client
        
        # Cache for performance
        self._hotkey_to_coldkey: Dict[str, str] = {}
        self._stake_history: Dict[str, List[Tuple[datetime, float]]] = {}
    
    async def update_key_associations(self):
        """Update associations between hotkeys and coldkeys."""
        try:
            # Get stake dict from Subtensor
            stake_dict = await self.subtensor_client.get_stake_dict()
            
            # Update database
            async with self.db_manager.async_session() as session:
                for hotkey, coldkeys in stake_dict.items():
                    if not coldkeys:
                        continue
                        
                    # Get the coldkey with the highest stake
                    max_stake_coldkey = max(coldkeys.items(), key=lambda x: x[1])[0]
                    
                    # Check if already exists
                    result = await session.execute(
                        session.query(HotkeyColdkeyAssociation)
                        .filter(HotkeyColdkeyAssociation.hotkey == hotkey)
                    )
                    association = result.first()
                    
                    if association:
                        # Update if changed
                        if association.coldkey != max_stake_coldkey:
                            association.coldkey = max_stake_coldkey
                            association.last_updated = datetime.utcnow()
                    else:
                        # Create new association
                        association = HotkeyColdkeyAssociation(
                            hotkey=hotkey,
                            coldkey=max_stake_coldkey,
                            last_updated=datetime.utcnow()
                        )
                        session.add(association)
                
                # Commit changes
                await session.commit()
                
            # Update cache
            await self._load_key_associations()
            
            logger.info("Updated key associations")
            
        except Exception as e:
            logger.error(f"Error updating key associations: {e}")
    
    async def _load_key_associations(self):
        """Load key associations from database to cache."""
        try:
            self._hotkey_to_coldkey = {}
            
            async with self.db_manager.async_session() as session:
                result = await session.execute(
                    session.query(HotkeyColdkeyAssociation)
                )
                associations = result.all()
                
                for association in associations:
                    self._hotkey_to_coldkey[association.hotkey] = association.coldkey
                    
            logger.info(f"Loaded {len(self._hotkey_to_coldkey)} key associations")
            
        except Exception as e:
            logger.error(f"Error loading key associations: {e}")
    
    async def update_stake_history(self):
        """Update stake history for all miners."""
        try:
            # Get current stake dict
            stake_dict = await self.subtensor_client.get_stake_dict()
            now = datetime.utcnow()
            
            # Calculate stake for each hotkey
            hotkey_stakes = {}
            for hotkey, coldkeys in stake_dict.items():
                stake = sum(coldkeys.values())
                hotkey_stakes[hotkey] = stake
            
            # Update database
            async with self.db_manager.async_session() as session:
                for hotkey, stake in hotkey_stakes.items():
                    # Skip if no stake
                    if stake <= 0:
                        continue
                        
                    # Get coldkey from cache or default to empty
                    coldkey = self._hotkey_to_coldkey.get(hotkey, "")
                    
                    # Create stake history record
                    history = StakeHistory(
                        hotkey=hotkey,
                        coldkey=coldkey,
                        timestamp=now,
                        stake=stake
                    )
                    session.add(history)
                
                # Commit changes
                await session.commit()
                
            # Update cache
            await self._load_stake_history()
            
            logger.info(f"Updated stake history for {len(hotkey_stakes)} miners")
            
        except Exception as e:
            logger.error(f"Error updating stake history: {e}")
    
    async def _load_stake_history(self):
        """Load stake history from database to cache."""
        try:
            self._stake_history = {}
            
            # Get cutoff date (30 days ago)
            cutoff_date = datetime.utcnow() - timedelta(days=30)
            
            async with self.db_manager.async_session() as session:
                result = await session.execute(
                    session.query(StakeHistory)
                    .filter(StakeHistory.timestamp >= cutoff_date)
                    .order_by(StakeHistory.timestamp)
                )
                histories = result.all()
                
                for history in histories:
                    if history.hotkey not in self._stake_history:
                        self._stake_history[history.hotkey] = []
                        
                    self._stake_history[history.hotkey].append(
                        (history.timestamp, history.stake)
                    )
                    
            logger.info(f"Loaded stake history for {len(self._stake_history)} miners")
            
        except Exception as e:
            logger.error(f"Error loading stake history: {e}")
    
    async def calculate_holding_metrics(self, hotkey: str) -> Tuple[float, int]:
        """
        Calculate holding percentage and duration for a hotkey.
        
        Args:
            hotkey: The hotkey to calculate metrics for
            
        Returns:
            Tuple of (holding_percentage, holding_duration_days)
        """
        try:
            # Default values
            holding_percentage = 0.0
            holding_duration_days = 0
            
            # Check if we have stake history
            if hotkey not in self._stake_history or not self._stake_history[hotkey]:
                return holding_percentage, holding_duration_days
                
            # Get stake history
            history = self._stake_history[hotkey]
            
            # Calculate metrics based on stake changes
            if len(history) < 2:
                # Not enough history
                return holding_percentage, holding_duration_days
                
            # Sort by timestamp (should already be sorted)
            history.sort(key=lambda x: x[0])
            
            # Get current stake and initial stake
            current_timestamp, current_stake = history[-1]
            initial_timestamp, initial_stake = history[0]
            
            # Find the lowest stake value in the period
            min_stake = min(item[1] for item in history)
            
            # Skip if no meaningful stake
            if current_stake < 0.1 or initial_stake < 0.1:
                return holding_percentage, holding_duration_days
                
            # Calculate holding percentage
            # This is the percentage of initial stake still held
            if current_stake >= initial_stake:
                # Held everything or gained
                holding_percentage = 1.0
            else:
                # Held some portion
                holding_percentage = current_stake / initial_stake
                
            # Calculate holding duration
            # This is how long they've maintained a significant stake
            duration = current_timestamp - initial_timestamp
            holding_duration_days = duration.days
            
            # If stake dropped below 50% at any point, reduce duration
            if min_stake < initial_stake * 0.5:
                holding_duration_days = max(0, holding_duration_days // 2)
                
            return holding_percentage, holding_duration_days
            
        except Exception as e:
            logger.error(f"Error calculating holding metrics for {hotkey}: {e}")
            return 0.0, 0
    
    async def record_stake_transaction(
        self,
        transaction_type: str,
        hotkey: str,
        coldkey: str,
        amount: float,
        extrinsic_hash: str,
        block_number: int,
        block_timestamp: datetime,
        source_hotkey: Optional[str] = None
    ):
        """
        Record a stake transaction.
        
        Args:
            transaction_type: Type of transaction (add_stake, remove_stake, move_stake)
            hotkey: Target hotkey
            coldkey: Coldkey involved
            amount: Amount of stake
            extrinsic_hash: Blockchain extrinsic hash
            block_number: Block number
            block_timestamp: Block timestamp
            source_hotkey: Source hotkey for move_stake transactions
        """
        try:
            async with self.db_manager.async_session() as session:
                # Check if transaction already exists
                result = await session.execute(
                    session.query(StakeTransaction)
                    .filter(StakeTransaction.extrinsic_hash == extrinsic_hash)
                )
                existing = result.first()
                
                if existing:
                    logger.debug(f"Transaction {extrinsic_hash} already recorded")
                    return
                    
                # Create transaction record
                transaction = StakeTransaction(
                    extrinsic_hash=extrinsic_hash,
                    block_number=block_number,
                    block_timestamp=block_timestamp,
                    hotkey=hotkey,
                    coldkey=coldkey,
                    transaction_type=transaction_type,
                    amount=amount,
                    source_hotkey=source_hotkey
                )
                session.add(transaction)
                
                # Commit changes
                await session.commit()
                
            logger.info(
                f"Recorded {transaction_type} transaction: "
                f"{amount} TAO for {hotkey} in block {block_number}"
            )
            
        except Exception as e:
            logger.error(f"Error recording stake transaction: {e}")
            
    async def get_stake_changes(self, hotkey: str, days: int = 30) -> Dict[str, float]:
        """
        Get stake changes for a hotkey over the specified period.
        
        Args:
            hotkey: The hotkey to get changes for
            days: Number of days to look back
            
        Returns:
            Dictionary with stake change metrics
        """
        try:
            cutoff_date = datetime.utcnow() - timedelta(days=days)
            
            # Initialize result
            result = {
                "total_added": 0.0,
                "total_removed": 0.0,
                "manual_adjustments": 0.0,
                "rewards_earned": 0.0,
                "net_change": 0.0
            }
            
            async with self.db_manager.async_session() as session:
                # Get add_stake transactions
                add_stake_result = await session.execute(
                    session.query(func.sum(StakeTransaction.amount))
                    .filter(
                        and_(
                            StakeTransaction.hotkey == hotkey,
                            StakeTransaction.transaction_type == "add_stake",
                            StakeTransaction.block_timestamp >= cutoff_date
                        )
                    )
                )
                add_stake_sum = add_stake_result.scalar() or 0.0
                
                # Get remove_stake transactions
                remove_stake_result = await session.execute(
                    session.query(func.sum(StakeTransaction.amount))
                    .filter(
                        and_(
                            StakeTransaction.hotkey == hotkey,
                            StakeTransaction.transaction_type == "remove_stake",
                            StakeTransaction.block_timestamp >= cutoff_date
                        )
                    )
                )
                remove_stake_sum = remove_stake_result.scalar() or 0.0
                
                # Get stake history change
                # First record in period
                first_record_result = await session.execute(
                    session.query(StakeHistory)
                    .filter(
                        and_(
                            StakeHistory.hotkey == hotkey,
                            StakeHistory.timestamp >= cutoff_date
                        )
                    )
                    .order_by(StakeHistory.timestamp)
                    .limit(1)
                )
                first_record = first_record_result.first()
                
                # Latest record
                latest_record_result = await session.execute(
                    session.query(StakeHistory)
                    .filter(StakeHistory.hotkey == hotkey)
                    .order_by(StakeHistory.timestamp.desc())
                    .limit(1)
                )
                latest_record = latest_record_result.first()
                
                # Calculate net change from history
                initial_stake = first_record.stake if first_record else 0.0
                current_stake = latest_record.stake if latest_record else 0.0
                net_change = current_stake - initial_stake
                
                # Calculate manual adjustments and rewards
                manual_adjustments = add_stake_sum - remove_stake_sum
                rewards_earned = net_change - manual_adjustments
                
                # Fill result
                result["total_added"] = add_stake_sum
                result["total_removed"] = remove_stake_sum
                result["manual_adjustments"] = manual_adjustments
                result["rewards_earned"] = max(0.0, rewards_earned)  # Can't be negative
                result["net_change"] = net_change
                
            return result
            
        except Exception as e:
            logger.error(f"Error getting stake changes for {hotkey}: {e}")
            return {
                "total_added": 0.0,
                "total_removed": 0.0,
                "manual_adjustments": 0.0,
                "rewards_earned": 0.0,
                "net_change": 0.0,
                "error": str(e)
            } 