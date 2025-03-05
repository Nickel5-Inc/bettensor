"""
Stake tracking system for validators.

This module is responsible for tracking stake balances, recording stake
transactions, and maintaining hotkey-coldkey associations.
"""

import logging
import math
import time
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional, Any, Union, Set

import bittensor as bt
from sqlalchemy import and_, func

from bettensor.validator.utils.database import DatabaseManager
from bettensor.validator.utils.vesting.database.models import (
    HotkeyColdkeyAssociation,
    StakeHistory,
    StakeTransaction
)
from bettensor.validator.utils.vesting.blockchain import SubtensorClient
from bettensor.validator.utils.vesting.core.retry import TransactionManager, with_retries

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
        subtensor_client: SubtensorClient,
        cache_ttl: int = 300
    ):
        """
        Initialize the StakeTracker.
        
        Args:
            db_manager: Database manager for persistent storage
            metagraph: Bittensor metagraph for accessing stake information
            subtensor_client: Client for Subtensor blockchain interactions
            cache_ttl: Cache time-to-live in seconds
        """
        self.db_manager = db_manager
        self.metagraph = metagraph
        self.subtensor_client = subtensor_client
        
        # Get DynamicInfo for the subnet to handle tao/alpha conversions
        self.dynamic_info = self.subtensor_client.get_dynamic_info()
        
        # Initialize transaction manager
        self.transaction_manager = TransactionManager(db_manager)
        
        # Cache for performance
        self._hotkey_to_coldkey: Dict[str, str] = {}
        self._stake_history: Dict[str, List[Tuple[datetime, float]]] = {}
        self._stake_cache = {}
        self._history_cache = {}
        self._cache_ttl = cache_ttl
    
    @with_retries()
    async def update_key_associations(self):
        """Update associations between hotkeys and coldkeys using batch processing."""
        try:
            stake_dict = await self.subtensor_client.get_stake_dict()
            
            if not stake_dict:
                logger.warning("No stake data received from Subtensor")
                return
            
            # Prepare batch data
            now = datetime.utcnow()
            updates = []
            
            for hotkey, coldkeys in stake_dict.items():
                if coldkeys:
                    max_stake_coldkey = max(coldkeys.items(), key=lambda x: x[1])[0]
                    updates.append({
                        "hotkey": hotkey,
                        "coldkey": max_stake_coldkey,
                        "last_updated": now
                    })
            
            if not updates:
                return
            
            # Use transaction manager for batch insert with conflict resolution
            await self.transaction_manager.execute_batch_insert(
                table_name="vesting_hotkey_coldkey_associations",
                columns=["hotkey", "coldkey", "last_updated"],
                values=updates,
                conflict_resolution="""
                ON CONFLICT (hotkey) DO UPDATE SET
                    coldkey = EXCLUDED.coldkey,
                    last_updated = EXCLUDED.last_updated
                WHERE 
                    vesting_hotkey_coldkey_associations.coldkey != EXCLUDED.coldkey
                    OR vesting_hotkey_coldkey_associations.last_updated < EXCLUDED.last_updated
                """
            )
            
            # Update cache efficiently
            self._hotkey_to_coldkey = {
                update["hotkey"]: update["coldkey"]
                for update in updates
            }
            
            logger.info(f"Updated {len(updates)} key associations in batch")
            
        except Exception as e:
            logger.error(f"Error updating key associations: {e}")
            raise
    
    async def _load_key_associations(self):
        """Load key associations from database to cache using a single query."""
        try:
            async with self.db_manager.async_session() as session:
                result = await session.execute(
                    """
                    SELECT hotkey, coldkey
                    FROM vesting_hotkey_coldkey_associations
                    WHERE last_updated >= :cutoff_date
                    """,
                    {
                        "cutoff_date": datetime.utcnow() - timedelta(days=30)
                    }
                )
                
                self._hotkey_to_coldkey = {
                    row[0]: row[1] for row in result
                }
                    
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
            
            # Batch database operations
            async with self.db_manager.async_session() as session:
                # Get all hotkeys for which we need to update stake history
                hotkeys_to_update = []
                stake_records = []
                
                # Get coldkeys in batch for efficiency
                hotkey_list = list(hotkey_stakes.keys())
                if not hotkey_list:
                    logger.warning("No hotkeys found in metagraph")
                    return
                    
                # Query all relevant hotkey-coldkey associations in one query
                assoc_result = await session.execute(
                    session.query(HotkeyColdkeyAssociation)
                    .filter(HotkeyColdkeyAssociation.hotkey.in_(hotkey_list))
                )
                
                # Create a mapping of hotkey to coldkey
                hotkey_to_coldkey_map = {assoc.hotkey: assoc.coldkey for assoc in assoc_result}
                
                # Create stake history records in a batch
                for hotkey, stake in hotkey_stakes.items():
                    # Skip if no stake
                    if stake <= 0:
                        continue
                        
                    # Get coldkey from cache or default to empty
                    coldkey = hotkey_to_coldkey_map.get(hotkey, "")
                    
                    # Create stake history record
                    stake_records.append(
                        StakeHistory(
                            hotkey=hotkey,
                            coldkey=coldkey,
                            timestamp=now,
                            stake=stake
                        )
                    )
                
                # Add all records at once
                if stake_records:
                    session.add_all(stake_records)
                    await session.commit()
                    logger.info(f"Updated stake history for {len(stake_records)} miners in a batch operation")
                
            # Update cache
            await self._load_stake_history()
            
        except Exception as e:
            logger.error(f"Error updating stake history: {e}")
    
    async def _load_stake_history(self, window_days: int = 30, max_records_per_hotkey: int = 100):
        """
        Load stake history from database to cache using a sliding window approach.
        
        Args:
            window_days: Number of days to look back (default: 30)
            max_records_per_hotkey: Maximum number of records to load per hotkey (default: 100)
        """
        try:
            self._stake_history = {}
            
            # Get cutoff date (window_days ago)
            cutoff_date = datetime.utcnow() - timedelta(days=window_days)
            
            # Get active hotkeys with stake from recent records
            async with self.db_manager.async_session() as session:
                # First get distinct hotkeys with recent stake
                recent_hotkeys_result = await session.execute(
                    session.query(StakeHistory.hotkey)
                    .filter(StakeHistory.timestamp >= cutoff_date)
                    .group_by(StakeHistory.hotkey)
                    .having(func.max(StakeHistory.stake) > 0)  # Only include hotkeys with positive stake
                )
                
                recent_hotkeys = [row[0] for row in recent_hotkeys_result]
                
                if not recent_hotkeys:
                    logger.warning("No recent stake history found")
                    return
                
                # For each hotkey, get limited history with most recent first
                for hotkey_batch in _batch_list(recent_hotkeys, 50):  # Process in batches of 50 hotkeys
                    query = (
                        session.query(StakeHistory)
                        .filter(
                            StakeHistory.hotkey.in_(hotkey_batch),
                            StakeHistory.timestamp >= cutoff_date
                        )
                        .order_by(StakeHistory.timestamp.desc())
                    )
                    
                    # Use an efficient approach to get limited records per hotkey
                    histories_result = await session.execute(query)
                    histories = histories_result.all()
                    
                    # Process results efficiently
                    hotkey_record_count = {}  # Track count per hotkey
                    
                    for history in histories:
                        hotkey = history.hotkey
                        
                        # Initialize entry if needed
                        if hotkey not in self._stake_history:
                            self._stake_history[hotkey] = []
                            hotkey_record_count[hotkey] = 0
                        
                        # Skip if we've reached the max records for this hotkey
                        if hotkey_record_count.get(hotkey, 0) >= max_records_per_hotkey:
                            continue
                        
                        # Add record and increment count
                        self._stake_history[hotkey].append(
                            (history.timestamp, history.stake)
                        )
                        hotkey_record_count[hotkey] = hotkey_record_count.get(hotkey, 0) + 1
                
                # Sort histories by timestamp (oldest first)
                for hotkey in self._stake_history:
                    self._stake_history[hotkey].sort(key=lambda x: x[0])
                    
            logger.info(f"Loaded stake history for {len(self._stake_history)} hotkeys with window of {window_days} days")
            
        except Exception as e:
            logger.error(f"Error loading stake history: {e}")

    @with_retries()
    async def update_stake_history_batch(self, stake_updates: List[Dict[str, Any]]) -> None:
        """Update stake history for multiple hotkeys in batch with retry support."""
        if not stake_updates:
            return
            
        try:
            # Define operations for the transaction
            async def insert_history(session):
                await session.execute(
                    """
                    INSERT INTO vesting_stake_history 
                        (hotkey, stake, coldkey, timestamp)
                    SELECT 
                        u.hotkey,
                        u.stake,
                        u.coldkey,
                        u.timestamp
                    FROM UNNEST(:values) AS u(
                        hotkey text,
                        stake float,
                        coldkey text,
                        timestamp timestamp
                    )
                    """,
                    {"values": stake_updates}
                )
            
            async def update_cache(session):
                now = datetime.utcnow()
                for update in stake_updates:
                    hotkey = update["hotkey"]
                    self._stake_cache[hotkey] = (update["stake"], now)
                    if hotkey in self._history_cache:
                        self._history_cache[hotkey]["entries"].append(update)
                        self._history_cache[hotkey]["last_updated"] = now
            
            # Execute operations in transaction
            await self.transaction_manager.execute_in_transaction(
                operations=[insert_history, update_cache]
            )
            
            logger.info(f"Updated stake history for {len(stake_updates)} entries")
            
        except Exception as e:
            logger.error(f"Error updating stake history in batch: {e}")
            raise
            
    async def get_stake_history(
        self,
        hotkey: str,
        days: int = 30
    ) -> List[Tuple[datetime, float]]:
        """
        Get stake history for a hotkey.
        
        Args:
            hotkey: Validator hotkey address
            days: Number of days of history to retrieve
            
        Returns:
            List of (timestamp, stake) tuples
        """
        cache_key = f"{hotkey}_{days}"
        if cache_key in self._history_cache:
            return self._history_cache[cache_key]
            
        # Calculate date range
        now = datetime.utcnow()
        start_date = now - timedelta(days=days)
        
        async with self.db_manager.session() as session:
            result = await session.execute(
                """
                SELECT block_timestamp, amount
                FROM vesting_stake_history
                WHERE hotkey = :hotkey AND block_timestamp >= :start_date
                ORDER BY block_timestamp ASC
                """,
                {"hotkey": hotkey, "start_date": start_date}
            )
            
            history = [(row[0], row[1]) for row in result]
            
            # Convert alpha amounts back to tao for consistency in results
            history = [(timestamp, self.dynamic_info.alpha_to_tao(amount).tao) 
                      for timestamp, amount in history]
            
            self._history_cache[cache_key] = history
            return history
            
    async def get_current_stake(self, hotkey: str) -> float:
        """
        Get current stake for a hotkey with caching.
        
        Args:
            hotkey: The hotkey to get stake for
            
        Returns:
            Current stake amount
        """
        try:
            now = datetime.utcnow()
            
            # Check cache
            if hotkey in self._stake_cache:
                stake, cache_time = self._stake_cache[hotkey]
                if (now - cache_time).total_seconds() < self._cache_ttl:
                    return stake
            
            async with self.db_manager.async_session() as session:
                result = await session.execute(
                    """
                    SELECT stake
                    FROM vesting_stake_history
                    WHERE hotkey = :hotkey
                    ORDER BY timestamp DESC
                    LIMIT 1
                    """,
                    {"hotkey": hotkey}
                )
                
                row = result.first()
                stake = float(row[0]) if row else 0.0
                
                # Update cache
                self._stake_cache[hotkey] = (stake, now)
                
                return stake
                
        except Exception as e:
            logger.error(f"Error getting current stake: {e}")
            return 0.0
            
    async def get_stake_changes(self, hotkey: str, days: int = 30) -> Dict[str, float]:
        """
        Get stake changes summary for a hotkey.
        
        Args:
            hotkey: The hotkey to get changes for
            days: Number of days to analyze
            
        Returns:
            Dictionary with net_change and percentage_change
        """
        try:
            history = await self.get_stake_history(hotkey, days)
            
            if not history:
                return {"net_change": 0.0, "percentage_change": 0.0}
            
            current_stake = history[-1][1]
            initial_stake = history[0][1]
            
            net_change = current_stake - initial_stake
            percentage_change = (
                ((current_stake / initial_stake) - 1) * 100
                if initial_stake > 0 else 0.0
            )
            
            return {
                "net_change": net_change,
                "percentage_change": percentage_change
            }
            
        except Exception as e:
            logger.error(f"Error calculating stake changes: {e}")
            return {"net_change": 0.0, "percentage_change": 0.0}
            
    def clear_cache(self, hotkey: str = None):
        """
        Clear cache entries.
        
        Args:
            hotkey: Optional specific hotkey to clear cache for
        """
        if hotkey:
            self._stake_cache.pop(hotkey, None)
            self._history_cache.pop(hotkey, None)
        else:
            self._stake_cache.clear()
            self._history_cache.clear()

    async def calculate_holding_metrics_batch(self, hotkeys: List[str]) -> Dict[str, Tuple[float, int]]:
        """
        Calculate holding percentage and duration for multiple hotkeys at once.
        
        Args:
            hotkeys: The list of hotkeys to calculate metrics for
            
        Returns:
            Dict mapping hotkeys to tuples of (holding_percentage, holding_duration_days)
        """
        try:
            results = {}
            
            # Skip if no hotkeys
            if not hotkeys:
                return results
                
            # Get stake history for all hotkeys from cache
            for hotkey in hotkeys:
                # Default values
                holding_percentage = 0.0
                holding_duration_days = 0
                
                # Check if we have stake history
                if hotkey not in self._stake_history or not self._stake_history[hotkey]:
                    results[hotkey] = (holding_percentage, holding_duration_days)
                    continue
                    
                # Get stake history
                history = self._stake_history[hotkey]
                
                # Calculate metrics based on stake changes
                if len(history) < 2:
                    # Not enough history
                    results[hotkey] = (holding_percentage, holding_duration_days)
                    continue
                    
                # Get current stake and initial stake
                current_timestamp, current_stake = history[-1]
                initial_timestamp, initial_stake = history[0]
                
                # Find the lowest stake value in the period
                min_stake = min(item[1] for item in history)
                
                # Skip if no meaningful stake
                if current_stake < 0.1 or initial_stake < 0.1:
                    results[hotkey] = (holding_percentage, holding_duration_days)
                    continue
                    
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
                    
                results[hotkey] = (holding_percentage, holding_duration_days)
            
            return results
            
        except Exception as e:
            logger.error(f"Error calculating holding metrics batch: {e}")
            return {hotkey: (0.0, 0) for hotkey in hotkeys}

    async def calculate_holding_metrics(self, hotkey: str) -> Tuple[float, int]:
        """
        Calculate holding percentage and duration for a hotkey.
        
        Args:
            hotkey: The hotkey to calculate metrics for
            
        Returns:
            Tuple of (holding_percentage, holding_duration_days)
        """
        try:
            # Use batch calculation for efficiency
            results = await self.calculate_holding_metrics_batch([hotkey])
            return results.get(hotkey, (0.0, 0))
            
        except Exception as e:
            logger.error(f"Error calculating holding metrics for {hotkey}: {e}")
            return 0.0, 0
    
    @with_retries()
    async def record_stake_transactions_batch(
        self,
        transactions: List[Dict[str, Any]]
    ) -> None:
        """Record multiple stake transactions in batch with retry support."""
        if not transactions:
            return
            
        try:
            # Check existing transactions
            extrinsic_hashes = [t["extrinsic_hash"] for t in transactions]
            
            async def check_existing(session):
                result = await session.execute(
                    """
                    SELECT extrinsic_hash
                    FROM vesting_stake_transactions
                    WHERE extrinsic_hash = ANY(:hashes)
                    """,
                    {"hashes": extrinsic_hashes}
                )
                return {row[0] for row in result}
            
            existing_hashes = await check_existing(None)
            
            # Filter out existing transactions
            new_transactions = [
                t for t in transactions 
                if t["extrinsic_hash"] not in existing_hashes
            ]
            
            if not new_transactions:
                return
            
            # Convert amounts from tao to alpha before storing
            converted_transactions = []
            for tx in new_transactions:
                # Create a copy of the transaction to avoid modifying the original
                tx_copy = tx.copy()
                
                # Get the amount (either float or bt.Balance)
                tao_amount = tx_copy["amount"]
                
                # If it's not already a Balance object, convert it
                if not isinstance(tao_amount, bt.Balance):
                    tao_amount = bt.Balance.from_tao(tao_amount)
                
                # Convert from tao to alpha
                alpha_amount = self.dynamic_info.tao_to_alpha(tao_amount)
                tx_copy["amount"] = alpha_amount
                
                # Store both original tao amount and converted alpha amount for logging
                tx_copy["amount_tao"] = tao_amount
                tx_copy["amount_alpha"] = alpha_amount
                
                converted_transactions.append(tx_copy)
            
            # Use transaction manager for batch insert
            await self.transaction_manager.execute_batch_insert(
                table_name="vesting_stake_transactions",
                columns=[
                    "extrinsic_hash", "block_number", "block_timestamp",
                    "hotkey", "coldkey", "transaction_type", "amount",
                    "source_hotkey"
                ],
                values=converted_transactions
            )
            
            for tx in converted_transactions:
                logger.info(
                    f"Recorded {tx['transaction_type']} transaction for {tx['hotkey']}: "
                    f"{tx['amount_tao']} tao ({tx['amount_alpha']} alpha)"
                )
            
        except Exception as e:
            logger.error(f"Error recording stake transactions in batch: {e}")
            raise
    
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
        """Record a single stake transaction using batch method."""
        await self.record_stake_transactions_batch([{
            "transaction_type": transaction_type,
            "hotkey": hotkey,
            "coldkey": coldkey,
            "amount": amount,
            "extrinsic_hash": extrinsic_hash,
            "block_number": block_number,
            "block_timestamp": block_timestamp,
            "source_hotkey": source_hotkey
        }])


def _batch_list(items, batch_size):
    """Helper function to batch a list into chunks."""
    for i in range(0, len(items), batch_size):
        yield items[i:i + batch_size] 