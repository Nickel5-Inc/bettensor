"""
Stake tracking module for the vesting system.

This module tracks stake metrics and history for miners, including:
- Current stake values
- Manual vs. earned stake
- Holding metrics
"""

import logging
import asyncio
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Tuple, Union, Any
import traceback

import numpy as np

from bettensor.validator.database.database_manager import DatabaseManager

logger = logging.getLogger(__name__)

# Transaction flow categories (matching those in transaction_monitor.py)
INFLOW = "inflow"         # Stake added to the hotkey
OUTFLOW = "outflow"       # Stake removed from the hotkey
NEUTRAL = "neutral"       # No net change to the hotkey's stake
EMISSION = "emission"     # Stake added via network emissions/rewards

class StakeTracker:
    """
    Tracks stake metrics and history for miners.
    
    This class manages:
    1. Current stake amounts
    2. Manual vs. earned stake tracking
    3. Holding metrics calculations
    4. Many-to-one hotkey-coldkey relationships
    """
    
    def __init__(self, db_manager: 'DatabaseManager'):
        """
        Initialize the stake tracker.
        
        Args:
            db_manager: Database manager for persistent storage
        """
        self.db_manager = db_manager
    
    async def initialize(self):
        """
        Initialize the StakeTracker.
        
        This method prepares the stake tracker for operation but no longer 
        creates database tables as this is now handled centrally by the 
        DatabaseManager using schema definitions from database_schema.py.
        """
        logger.info("Initializing StakeTracker")
        # Table creation is now handled centrally by DatabaseManager
        # This method remains for API compatibility
        return True
    
    # DEPRECATED: Kept for backward compatibility
    async def _ensure_tables_exist(self):
        """
        DEPRECATED: Table creation is now handled centrally by DatabaseManager.
        
        This method is maintained for backward compatibility only and does nothing.
        Table schemas are defined in database_schema.py and created by DatabaseManager.
        """
        logger.debug("_ensure_tables_exist() is deprecated - tables are created centrally")
        return
    
    async def record_stake_change(
        self,
        hotkey: str,
        coldkey: str,
        amount: float,
        change_type: str,
        flow_type: Optional[str] = None,
        timestamp: Optional[datetime] = None
    ):
        """
        Record a stake change and update metrics.
        
        Args:
            hotkey: Miner hotkey
            coldkey: Associated coldkey
            amount: Stake amount changed (positive or negative)
            change_type: Type of change (add_stake, remove_stake, etc.)
            flow_type: Direction of flow (inflow, outflow, neutral, emission)
            timestamp: When the change occurred (defaults to now)
        """
        # Use current time if not provided
        if timestamp is None:
            timestamp = datetime.now(timezone.utc)
            
        # Log detailed information about the stake change
        logger.info(f"Recording stake change: hotkey={hotkey[:10]}..., type={change_type}, amount={amount:.4f} TAO, flow={flow_type}")
        
        # Get current stake metrics for this hotkey
        metrics = await self.get_stake_metrics(hotkey)
        if metrics is None:
            logger.info(f"First stake record for hotkey {hotkey[:10]}..., initializing metrics")
            # Initialize new stake metrics record
            metrics = {
                'hotkey': hotkey,
                'coldkey': coldkey,
                'total_stake': 0.0,
                'manual_stake': 0.0,
                'earned_stake': 0.0,
                'first_stake_timestamp': int(timestamp.timestamp()),
                'last_update': int(timestamp.timestamp()),
                'total_tranches': 0,
                'active_tranches': 0,
                'avg_tranche_age': 0,
                'oldest_tranche_age': 0,
                'manual_tranches': 0,
                'emission_tranches': 0
            }
            
        # Process stake change based on flow type
        if flow_type == INFLOW:
            # Record a stake addition
            if change_type != EMISSION:
                logger.debug(f"Adding {amount:.4f} TAO manual stake to {hotkey[:10]}...")
                metrics['manual_stake'] += amount
            else:
                logger.debug(f"Adding {amount:.4f} TAO earned stake to {hotkey[:10]}...")
                metrics['earned_stake'] += amount
                
            metrics['total_stake'] += amount
            
            # Create a new stake tranche
            tranche_id = await self._create_new_tranche(
                hotkey=hotkey,
                coldkey=coldkey,
                amount=amount,
                is_emission=(change_type == EMISSION),
                timestamp=timestamp
            )
            
        elif flow_type == OUTFLOW:
            # Record a stake removal
            logger.debug(f"Removing {amount:.4f} TAO stake from {hotkey[:10]}...")
            
            # Update metrics - never let values go below zero
            remaining = amount
            
            # Check if we're consuming tranches
            success = await self._consume_tranches_filo(
                hotkey=hotkey,
                amount_to_consume=amount,
                timestamp=timestamp,
                reason=change_type
            )
            
            if not success:
                logger.warning(f"Failed to find enough tranches to consume {amount:.4f} TAO for {hotkey[:10]}...")
                
            # First consume earned stake, then manual stake if needed
            if metrics['earned_stake'] > 0:
                earned_consumed = min(metrics['earned_stake'], remaining)
                metrics['earned_stake'] -= earned_consumed
                remaining -= earned_consumed
                logger.debug(f"Consumed {earned_consumed:.4f} TAO of earned stake from {hotkey[:10]}...")
                
            if remaining > 0 and metrics['manual_stake'] > 0:
                manual_consumed = min(metrics['manual_stake'], remaining)
                metrics['manual_stake'] -= manual_consumed
                remaining -= manual_consumed
                logger.debug(f"Consumed {manual_consumed:.4f} TAO of manual stake from {hotkey[:10]}...")
                
            # Update total stake
            metrics['total_stake'] = max(0, metrics['total_stake'] - amount)
            
        # Update last update timestamp
        metrics['last_update'] = int(timestamp.timestamp())
        
        # Record this transaction in the consolidated transactions table
        transaction_data = {
            'block_number': 0,  # Will be updated by blockchain monitor
            'timestamp': int(timestamp.timestamp()),
            'transaction_type': change_type,
            'flow_type': flow_type,
            'hotkey': hotkey,
            'coldkey': coldkey,
            'amount': amount if flow_type == INFLOW else -amount, 
            'stake_after': metrics['total_stake'],
            'manual_stake_after': metrics['manual_stake'],
            'earned_stake_after': metrics['earned_stake'],
            'change_type': 'manual' if change_type != EMISSION else 'emission'
        }
        
        await self.db_manager.execute_query("""
            INSERT INTO stake_transactions
            (block_number, timestamp, transaction_type, flow_type, hotkey, coldkey, 
            amount, stake_after, manual_stake_after, earned_stake_after, change_type)
            VALUES (:block_number, :timestamp, :transaction_type, :flow_type, :hotkey, :coldkey,
            :amount, :stake_after, :manual_stake_after, :earned_stake_after, :change_type)
        """, transaction_data)
        
        # Update the database record for miner metrics
        await self.db_manager.execute_query("""
            INSERT OR REPLACE INTO miner_metrics
            (hotkey, coldkey, total_stake, manual_stake, earned_stake, 
            first_stake_timestamp, last_update, total_tranches, active_tranches,
            avg_tranche_age, oldest_tranche_age, manual_tranches, emission_tranches)
            VALUES (:hotkey, :coldkey, :total_stake, :manual_stake, :earned_stake, 
            :first_stake_timestamp, :last_update, :total_tranches, :active_tranches,
            :avg_tranche_age, :oldest_tranche_age, :manual_tranches, :emission_tranches)
        """, metrics)
        
        logger.info(f"Updated stake metrics for {hotkey[:10]}...: total={metrics['total_stake']:.4f}, manual={metrics['manual_stake']:.4f}, earned={metrics['earned_stake']:.4f}")
        
        # Update coldkey metrics
        await self._update_coldkey_metrics(coldkey)
        
        # Update tranche metrics
        await self._update_tranche_metrics(hotkey)
        
        return metrics
    
    async def _update_coldkey_metrics(self, coldkey: str):
        """
        Update metrics for a coldkey by aggregating all associated hotkeys.
        
        Args:
            coldkey: Coldkey to update metrics for
        """
        if not coldkey:
            logger.error("Cannot update coldkey metrics: coldkey is empty or None")
            return False
        
        try:
            # Get all hotkeys associated with this coldkey
            try:
                hotkeys = await self.get_all_hotkeys_for_coldkey(coldkey)
                logger.debug(f"Found {len(hotkeys)} hotkeys for coldkey {coldkey}")
            except Exception as e:
                logger.error(f"Error getting hotkeys for coldkey {coldkey}: {e}")
                logger.debug(traceback.format_exc())
                return False
            
            # If no hotkeys, no metrics to update
            if not hotkeys:
                logger.warning(f"No hotkeys found for coldkey {coldkey}, cannot update metrics")
                return False
            
            # Get current time
            timestamp = datetime.now(timezone.utc)
            
            # Aggregate metrics for all hotkeys
            total_stake = 0.0
            manual_stake = 0.0
            earned_stake = 0.0
            hotkeys_with_metrics = 0
            
            for hotkey in hotkeys:
                try:
                    metrics = await self.get_stake_metrics(hotkey)
                    if metrics:
                        total_stake += metrics['total_stake']
                        manual_stake += metrics['manual_stake']
                        earned_stake += metrics['earned_stake']
                        hotkeys_with_metrics += 1
                    else:
                        logger.warning(f"No stake metrics found for hotkey {hotkey}")
                except Exception as e:
                    logger.error(f"Error getting stake metrics for hotkey {hotkey}: {e}")
                    continue
            
            logger.debug(f"Aggregated metrics for coldkey {coldkey}: {hotkeys_with_metrics}/{len(hotkeys)} hotkeys with metrics, total stake: {total_stake}")
            
            # Update or insert coldkey metrics
            try:
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
                """, [coldkey, total_stake, manual_stake, earned_stake, hotkeys_with_metrics, int(timestamp.timestamp())])
                
                logger.debug(f"Updated coldkey metrics for {coldkey}: total={total_stake:.4f}, manual={manual_stake:.4f}, earned={earned_stake:.4f}")
                return True
            except Exception as e:
                logger.error(f"Error updating coldkey metrics for {coldkey}: {e}")
                logger.debug(traceback.format_exc())
                return False
        except Exception as e:
            logger.error(f"Failed to update coldkey metrics for {coldkey}: {e}")
            logger.debug(traceback.format_exc())
            return False
    
    async def get_stake_metrics(self, hotkey: str) -> Optional[Dict]:
        """
        Get stake metrics for a specific hotkey.
        
        Args:
            hotkey: Miner hotkey
            
        Returns:
            Dict or None: Stake metrics or None if not found
        """
        try:
            # Retrieve metrics from new miner_metrics table
            result = await self.db_manager.execute_query("""
                SELECT * FROM miner_metrics WHERE hotkey = ?
            """, [hotkey], fetch_one=True)
            
            if result:
                return dict(result)
            else:
                return None
        except Exception as e:
            logger.error(f"Error retrieving stake metrics for {hotkey}: {e}")
            return None
    
    async def get_all_hotkeys_for_coldkey(self, coldkey: str) -> List[str]:
        """
        Get all hotkeys associated with a coldkey.
        
        Args:
            coldkey: Coldkey to look up
            
        Returns:
            List[str]: List of associated hotkeys
        """
        try:
            # Query from new miner_metrics table
            results = await self.db_manager.execute_query("""
                SELECT hotkey FROM miner_metrics WHERE coldkey = :coldkey
            """, {'coldkey': coldkey})
            
            return [row['hotkey'] for row in results]
        except Exception as e:
            logger.error(f"Error retrieving hotkeys for coldkey {coldkey}: {e}")
            return []
    
    async def get_all_stake_metrics(self) -> List[Dict]:
        """
        Get stake metrics for all miners.
        
        Returns:
            List[Dict]: List of stake metrics for all miners
        """
        try:
            # Query from new miner_metrics table
            results = await self.db_manager.execute_query("""
                SELECT * 
                FROM miner_metrics
                ORDER BY total_stake DESC
            """)
            
            return [dict(row) for row in results]
        except Exception as e:
            logger.error(f"Error retrieving all stake metrics: {e}")
            return []
    
    async def _create_new_tranche(
        self, 
        hotkey: str, 
        coldkey: str, 
        amount: float, 
        is_emission: bool,
        timestamp: Optional[datetime] = None
    ) -> int:
        """
        Create a new stake tranche.
        
        Args:
            hotkey: Miner hotkey
            coldkey: Associated coldkey
            amount: Initial amount
            is_emission: Whether this is an emission reward
            timestamp: Entry timestamp (defaults to now)
            
        Returns:
            int: Tranche ID
        """
        if timestamp is None:
            timestamp = datetime.now(timezone.utc)
            
        ts = int(timestamp.timestamp())
        
        try:
            # Insert tranche into the consolidated tranche table
            result = await self.db_manager.execute_query("""
                INSERT INTO stake_tranches
                (hotkey, coldkey, initial_amount, remaining_amount, entry_timestamp, is_emission, last_update, is_active)
                VALUES (?, ?, ?, ?, ?, ?, ?, 1)
            """, [hotkey, coldkey, amount, amount, ts, 1 if is_emission else 0, ts], get_last_row_id=True)
            
            tranche_id = result
            logger.debug(f"Created new tranche {tranche_id} for {hotkey[:10]}...: amount={amount:.4f}, is_emission={is_emission}")
            
            # Update tranche metrics
            await self._update_tranche_metrics(hotkey)
            
            return tranche_id
        except Exception as e:
            logger.error(f"Error creating new tranche for {hotkey}: {e}")
            logger.debug(traceback.format_exc())
            return -1
    
    async def _consume_tranches_filo(
        self, 
        hotkey: str, 
        amount_to_consume: float,
        timestamp: Optional[datetime] = None,
        reason: str = "withdrawal"
    ) -> bool:
        """
        Consume stake tranches using First-In, Last-Out (FILO) strategy.
        
        Args:
            hotkey: Miner hotkey
            amount_to_consume: Amount to consume
            timestamp: Exit timestamp (defaults to now)
            reason: Reason for consumption
            
        Returns:
            bool: True if sufficient tranches were found and consumed
        """
        if timestamp is None:
            timestamp = datetime.now(timezone.utc)
            
        ts = int(timestamp.timestamp())
        
        try:
            # Get active tranches, newest first for FILO
            tranches = await self.db_manager.execute_query("""
                SELECT id, remaining_amount 
                FROM stake_tranches 
                WHERE hotkey = ? AND is_active = 1 AND remaining_amount > 0
                ORDER BY entry_timestamp DESC
            """, [hotkey])
            
            if not tranches:
                logger.warning(f"No active tranches found for {hotkey[:10]}...")
                return False
                
            tranches = [dict(t) for t in tranches]
            
            # Calculate total available amount
            total_available = sum(t['remaining_amount'] for t in tranches)
            if total_available < amount_to_consume:
                logger.warning(f"Insufficient tranches for {hotkey[:10]}...: need {amount_to_consume:.4f}, have {total_available:.4f}")
                return False
                
            # Consume tranches
            remaining_to_consume = amount_to_consume
            
            for tranche in tranches:
                if remaining_to_consume <= 0:
                    break
                    
                tranche_id = tranche['id']
                available = tranche['remaining_amount']
                
                # Determine how much to consume from this tranche
                to_consume = min(available, remaining_to_consume)
                remaining = available - to_consume
                
                # Update the tranche in the database
                if remaining <= 0:
                    # Fully consumed, mark as inactive
                    await self.db_manager.execute_query("""
                        UPDATE stake_tranches
                        SET remaining_amount = 0, 
                            is_active = 0,
                            exit_timestamp = ?,
                            exit_amount = ?,
                            exit_reason = ?,
                            last_update = ?
                        WHERE id = ?
                    """, [ts, to_consume, reason, ts, tranche_id])
                else:
                    # Partially consumed
                    await self.db_manager.execute_query("""
                        UPDATE stake_tranches
                        SET remaining_amount = ?,
                            exit_amount = COALESCE(exit_amount, 0) + ?,
                            last_update = ?
                        WHERE id = ?
                    """, [remaining, to_consume, ts, tranche_id])
                
                logger.debug(f"Consumed {to_consume:.4f} from tranche {tranche_id} for {hotkey[:10]}..., remaining={remaining:.4f}")
                
                remaining_to_consume -= to_consume
            
            # Update tranche metrics after consumption
            await self._update_tranche_metrics(hotkey)
            
            return True
        except Exception as e:
            logger.error(f"Error consuming tranches for {hotkey}: {e}")
            logger.debug(traceback.format_exc())
            return False
    
    async def _update_tranche_metrics(self, hotkey: str):
        """
        Update aggregated tranche metrics for a hotkey.
        
        Args:
            hotkey: Miner hotkey
        """
        try:
            # Get current time
            now = datetime.now(timezone.utc)
            now_ts = int(now.timestamp())
            
            # Get metrics for this hotkey
            metrics = await self.get_stake_metrics(hotkey)
            if not metrics:
                logger.warning(f"No metrics record found for {hotkey[:10]}... when updating tranche metrics")
                return
                
            # Get all tranches for this hotkey
            tranches = await self.db_manager.execute_query("""
                SELECT * FROM stake_tranches WHERE hotkey = ?
            """, [hotkey])
            
            if not tranches:
                logger.debug(f"No tranches found for {hotkey[:10]}...")
                return
                
            tranches = [dict(t) for t in tranches]
            
            # Calculate metrics
            active_tranches = sum(1 for t in tranches if t['is_active'] == 1 and t['remaining_amount'] > 0)
            total_tranches = len(tranches)
            
            manual_tranches = sum(1 for t in tranches if t['is_emission'] == 0)
            emission_tranches = sum(1 for t in tranches if t['is_emission'] == 1)
            
            # Calculate tranche ages
            tranche_ages = []
            for t in tranches:
                if t['is_active'] == 1 and t['remaining_amount'] > 0:
                    age = now_ts - t['entry_timestamp']
                    if age > 0:
                        tranche_ages.append(age)
            
            avg_tranche_age = int(sum(tranche_ages) / len(tranche_ages)) if tranche_ages else 0
            oldest_tranche_age = max(tranche_ages) if tranche_ages else 0
            
            # Update the miner_metrics record
            updated_metrics = {
                'hotkey': hotkey,
                'coldkey': metrics['coldkey'],
                'total_stake': metrics['total_stake'],
                'manual_stake': metrics['manual_stake'],
                'earned_stake': metrics['earned_stake'],
                'first_stake_timestamp': metrics['first_stake_timestamp'],
                'last_update': now_ts,
                'total_tranches': total_tranches,
                'active_tranches': active_tranches,
                'avg_tranche_age': avg_tranche_age,
                'oldest_tranche_age': oldest_tranche_age,
                'manual_tranches': manual_tranches,
                'emission_tranches': emission_tranches
            }
            
            await self.db_manager.execute_query("""
                UPDATE miner_metrics
                SET total_tranches = :total_tranches,
                    active_tranches = :active_tranches,
                    avg_tranche_age = :avg_tranche_age,
                    oldest_tranche_age = :oldest_tranche_age,
                    manual_tranches = :manual_tranches,
                    emission_tranches = :emission_tranches,
                    last_update = :last_update
                WHERE hotkey = :hotkey
            """, updated_metrics)
            
            logger.debug(f"Updated tranche metrics for {hotkey[:10]}...: active={active_tranches}/{total_tranches}, avg_age={avg_tranche_age}s")
            
        except Exception as e:
            logger.error(f"Error updating tranche metrics for {hotkey}: {e}")
            logger.debug(traceback.format_exc()) 