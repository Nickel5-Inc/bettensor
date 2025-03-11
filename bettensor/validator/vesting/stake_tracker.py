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
                'last_update': int(timestamp.timestamp())
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
            await self._create_new_tranche(
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
                timestamp=timestamp
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
        
        # Update the database record
        await self.db_manager.execute_query("""
            INSERT OR REPLACE INTO stake_metrics
            (hotkey, coldkey, total_stake, manual_stake, earned_stake, first_stake_timestamp, last_update)
            VALUES (:hotkey, :coldkey, :total_stake, :manual_stake, :earned_stake, :first_stake_timestamp, :last_update)
        """, metrics)
        
        logger.info(f"Updated stake metrics for {hotkey[:10]}...: total={metrics['total_stake']:.4f}, manual={metrics['manual_stake']:.4f}, earned={metrics['earned_stake']:.4f}")
        
        # Update coldkey metrics
        await self._update_coldkey_metrics(coldkey)
        
        # Update aggregated tranche metrics
        await self._update_aggregated_tranche_metrics(hotkey)
        
        return metrics
    
    async def _update_coldkey_metrics(self, coldkey: str):
        """
        Update metrics for a coldkey by aggregating all associated hotkeys.
        
        Args:
            coldkey: Coldkey to update metrics for
        """
        try:
            # Get all hotkeys associated with this coldkey
            hotkeys = await self.get_all_hotkeys_for_coldkey(coldkey)
            
            # If no hotkeys, no metrics to update
            if not hotkeys:
                return
            
            # Get current time
            timestamp = datetime.now(timezone.utc)
            
            # Aggregate metrics for all hotkeys
            total_stake = 0.0
            manual_stake = 0.0
            earned_stake = 0.0
            
            for hotkey in hotkeys:
                metrics = await self.get_stake_metrics(hotkey)
                if metrics:
                    total_stake += metrics['total_stake']
                    manual_stake += metrics['manual_stake']
                    earned_stake += metrics['earned_stake']
            
            # Update or insert coldkey metrics
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
                len(hotkeys),
                timestamp
            ))
            
        except Exception as e:
            logger.error(f"Error updating coldkey metrics for {coldkey}: {e}")
    
    async def get_stake_metrics(self, hotkey: str) -> Optional[Dict]:
        """
        Get current stake metrics for a hotkey.
        
        Args:
            hotkey: Hotkey to get metrics for
            
        Returns:
            Dict: Stake metrics or None if not found
        """
        try:
            result = await self.db_manager.fetch_one("""
                SELECT * FROM stake_metrics WHERE hotkey = ?
            """, (hotkey,))
            
            return dict(result) if result else None
            
        except Exception as e:
            logger.error(f"Error getting stake metrics for {hotkey}: {e}")
            return None
    
    async def get_coldkey_metrics(self, coldkey: str) -> Optional[Dict]:
        """
        Get aggregated metrics for a coldkey.
        
        Args:
            coldkey: Coldkey to get metrics for
            
        Returns:
            Dict: Coldkey metrics or None if not found
        """
        try:
            result = await self.db_manager.fetch_one("""
                SELECT * FROM coldkey_metrics WHERE coldkey = ?
            """, (coldkey,))
            
            return dict(result) if result else None
            
        except Exception as e:
            logger.error(f"Error getting coldkey metrics for {coldkey}: {e}")
            return None
    
    async def get_all_hotkeys_for_coldkey(self, coldkey: str) -> List[str]:
        """
        Get all hotkeys associated with a coldkey.
        
        Args:
            coldkey: Coldkey to get hotkeys for
            
        Returns:
            List[str]: List of hotkeys
        """
        try:
            results = await self.db_manager.fetch_all("""
                SELECT hotkey FROM stake_metrics WHERE coldkey = :coldkey
            """, {"coldkey": coldkey})
            
            return [row['hotkey'] for row in results]
            
        except Exception as e:
            logger.error(f"Error getting hotkeys for coldkey {coldkey}: {e}")
            return []
    
    async def calculate_holding_metrics(
        self,
        hotkey: str,
        window_days: int = 30
    ) -> Tuple[float, int]:
        """
        Calculate stake holding metrics for a hotkey.
        
        This method calculates:
        1. Holding percentage: What percentage of emissions are still held
        2. Holding duration: Weighted average duration of holding (in days)
        
        Args:
            hotkey: Hotkey to calculate metrics for
            window_days: Window in days to consider for metrics
            
        Returns:
            Tuple[float, int]: (holding_percentage, holding_duration)
        """
        try:
            # First check if we have cached metrics
            metrics = await self.db_manager.fetch_one("""
                SELECT weighted_holding_days, total_emission_received, remaining_emission_stake, holding_percentage, last_update
                FROM aggregated_tranche_metrics
                WHERE hotkey = ?
            """, (hotkey,))
            
            if metrics:
                # Check if metrics are recent enough (within last hour)
                last_update = metrics['last_update']
                current_time = datetime.now(timezone.utc)
                
                if (current_time - last_update).total_seconds() < 3600:  # 1 hour cache
                    # Use cached metrics
                    holding_percentage = metrics['holding_percentage']
                    holding_duration = int(min(metrics['weighted_holding_days'], window_days))
                    
                    logger.debug(
                        f"Using cached holding metrics for {hotkey}: "
                        f"holding_percentage={holding_percentage:.2%}, "
                        f"holding_duration={holding_duration} days"
                    )
                    
                    return holding_percentage, holding_duration
            
            # If no cached metrics or they're too old, recalculate
            await self._update_aggregated_tranche_metrics(hotkey)
            
            # Fetch the updated metrics
            updated_metrics = await self.db_manager.fetch_one("""
                SELECT weighted_holding_days, holding_percentage
                FROM aggregated_tranche_metrics
                WHERE hotkey = ?
            """, (hotkey,))
            
            if updated_metrics:
                holding_percentage = updated_metrics['holding_percentage']
                holding_duration = int(min(updated_metrics['weighted_holding_days'], window_days))
                
                logger.debug(
                    f"Calculated fresh holding metrics for {hotkey}: "
                    f"holding_percentage={holding_percentage:.2%}, "
                    f"holding_duration={holding_duration} days"
                )
                
                return holding_percentage, holding_duration
            
            # If still no metrics, return zeros
            logger.warning(f"No holding metrics available for {hotkey}")
            return 0.0, 0
            
        except Exception as e:
            logger.error(f"Error calculating holding metrics for {hotkey}: {e}")
            return 0.0, 0
    
    async def get_stake_distribution(self) -> Dict[str, List[float]]:
        """
        Get current stake distribution across all miners.
        
        Returns:
            Dict: Distribution data with keys 'hotkeys', 'stakes', 'manual_stakes', 'earned_stakes'
        """
        try:
            results = await self.db_manager.fetch_all("""
                SELECT hotkey, total_stake, manual_stake, earned_stake 
                FROM stake_metrics 
                ORDER BY total_stake DESC
            """)
            
            hotkeys = []
            stakes = []
            manual_stakes = []
            earned_stakes = []
            
            for row in results:
                hotkeys.append(row['hotkey'])
                stakes.append(row['total_stake'])
                manual_stakes.append(row['manual_stake'])
                earned_stakes.append(row['earned_stake'])
            
            return {
                'hotkeys': hotkeys,
                'stakes': stakes,
                'manual_stakes': manual_stakes,
                'earned_stakes': earned_stakes
            }
            
        except Exception as e:
            logger.error(f"Error getting stake distribution: {e}")
            return {
                'hotkeys': [],
                'stakes': [],
                'manual_stakes': [],
                'earned_stakes': []
            }
    
    async def get_stake_change_history(
        self,
        hotkey: Optional[str] = None,
        coldkey: Optional[str] = None,
        days: Optional[int] = None,
        flow_type: Optional[str] = None
    ) -> List[Dict]:
        """
        Get stake change history for a hotkey or coldkey.
        
        Args:
            hotkey: Filter by hotkey (optional)
            coldkey: Filter by coldkey (optional)
            days: Number of days to look back (optional)
            flow_type: Filter by flow type (optional)
            
        Returns:
            List[Dict]: List of stake changes
        """
        try:
            # Build query
            query = "SELECT * FROM stake_change_history WHERE 1=1"
            params = []
            
            # Apply filters
            if hotkey:
                query += " AND hotkey = ?"
                params.append(hotkey)
            
            if coldkey:
                query += " AND coldkey = ?"
                params.append(coldkey)
            
            if flow_type:
                query += " AND flow_type = ?"
                params.append(flow_type)
            
            if days:
                start_time = datetime.now(timezone.utc) - timedelta(days=days)
                query += " AND timestamp >= ?"
                params.append(start_time)
            
            query += " ORDER BY timestamp DESC"
            
            # Execute query
            results = await self.db_manager.fetch_all(query, params)
            
            return results
            
        except Exception as e:
            logger.error(f"Error getting stake change history: {e}")
            return []
    
    async def get_retention_metrics(self, hotkey: str, window_days: int = 30) -> Dict:
        """
        Get detailed retention metrics for a hotkey.
        
        Args:
            hotkey: Hotkey to get metrics for
            window_days: Number of days to look back
            
        Returns:
            Dict: Retention metrics
        """
        try:
            # Get current stake metrics
            metrics = await self.get_stake_metrics(hotkey)
            if not metrics:
                return {
                    'total_stake': 0,
                    'manual_stake': 0,
                    'earned_stake': 0,
                    'total_inflow': 0,
                    'total_outflow': 0,
                    'total_emission': 0,
                    'holding_percentage': 0,
                    'holding_duration': 0
                }
            
            # Calculate start time for the window
            start_time = datetime.now(timezone.utc) - timedelta(days=window_days)
            
            # Get stake changes in the window
            changes = await self.db_manager.fetch_all("""
                SELECT * FROM stake_change_history 
                WHERE hotkey = ? AND timestamp >= ?
                ORDER BY timestamp
            """, (hotkey, start_time))
            
            # Calculate total inflow, outflow, and emission
            total_inflow = sum(c['amount'] for c in changes if c['flow_type'] == INFLOW)
            total_outflow = sum(abs(c['amount']) for c in changes if c['flow_type'] == OUTFLOW)
            total_emission = sum(c['amount'] for c in changes if c['flow_type'] == EMISSION)
            
            # Calculate holding metrics
            holding_percentage, holding_duration = await self.calculate_holding_metrics(
                hotkey, window_days
            )
            
            return {
                'total_stake': metrics['total_stake'],
                'manual_stake': metrics['manual_stake'],
                'earned_stake': metrics['earned_stake'],
                'total_inflow': total_inflow,
                'total_outflow': total_outflow,
                'total_emission': total_emission,
                'holding_percentage': holding_percentage,
                'holding_duration': holding_duration
            }
            
        except Exception as e:
            logger.error(f"Error getting retention metrics for {hotkey}: {e}")
            return {
                'error': str(e)
            }
    
    async def _create_new_tranche(self, hotkey: str, coldkey: str, amount: float, is_emission: bool, timestamp: datetime):
        """
        Create a new stake tranche entry.
        
        Args:
            hotkey: Miner hotkey
            coldkey: Associated coldkey
            amount: Amount of stake in the tranche
            is_emission: Whether this is from emissions (True) or manual stake (False)
            timestamp: When the tranche was created
            
        Returns:
            int: Tranche ID if successful, None otherwise
        """
        try:
            # Log tranche creation
            tranche_type = "emission" if is_emission else "manual"
            logger.info(f"Creating new {tranche_type} stake tranche: hotkey={hotkey[:10]}..., amount={amount:.4f} TAO")
            
            # Insert new tranche
            result = await self.db_manager.execute_query("""
                INSERT INTO stake_tranches 
                (hotkey, coldkey, initial_amount, remaining_amount, entry_timestamp, is_emission, last_update)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                hotkey, 
                coldkey, 
                amount, 
                amount, 
                int(timestamp.timestamp()), 
                1 if is_emission else 0,
                int(timestamp.timestamp())
            ))
            
            # Get the tranche ID (last row ID)
            tranche_id = result.lastrowid if hasattr(result, 'lastrowid') else None
            
            if tranche_id:
                logger.debug(f"Created tranche ID {tranche_id}: {tranche_type}, {amount:.4f} TAO for {hotkey[:10]}...")
                
                # Count total tranches for this hotkey
                count_result = await self.db_manager.fetch_one("""
                    SELECT COUNT(*) as count, SUM(remaining_amount) as total
                    FROM stake_tranches 
                    WHERE hotkey = ? AND remaining_amount > 0
                """, (hotkey,))
                
                if count_result:
                    logger.debug(f"Hotkey {hotkey[:10]}... now has {count_result['count']} active tranches " +
                                f"with {count_result['total']:.4f} total TAO")
            else:
                logger.warning(f"Failed to get tranche ID for new {tranche_type} stake: {amount:.4f} TAO for {hotkey[:10]}...")
            
            return tranche_id
            
        except Exception as e:
            logger.error(f"Error creating new tranche: {e}")
            return None
    
    async def _consume_tranches_filo(self, hotkey: str, amount_to_consume: float, timestamp: datetime):
        """
        Consume stake tranches using FILO accounting (Last In, First Out).
        
        Args:
            hotkey: Miner hotkey
            amount_to_consume: Amount of stake to consume
            timestamp: When the consumption occurred
            
        Returns:
            Tuple[float, List[Dict]]: Amount consumed and details of consumed tranches
        """
        logger.info(f"Consuming {amount_to_consume:.4f} TAO from tranches for hotkey={hotkey[:10]}...")
        
        # Get all active tranches for this hotkey, ordered by timestamp descending (newest first)
        tranches = await self.db_manager.fetch_all("""
            SELECT id, hotkey, coldkey, initial_amount, remaining_amount, entry_timestamp, is_emission, last_update
            FROM stake_tranches
            WHERE hotkey = ? AND remaining_amount > 0
            ORDER BY entry_timestamp DESC
        """, (hotkey,))
        
        if not tranches:
            logger.warning(f"No active tranches found for hotkey {hotkey[:10]}...")
            return 0, []
            
        logger.debug(f"Found {len(tranches)} active tranches for hotkey {hotkey[:10]}...")
        
        # Count emission vs manual tranches
        emission_tranches = sum(1 for t in tranches if t['is_emission'])
        manual_tranches = len(tranches) - emission_tranches
        emission_amount = sum(t['remaining_amount'] for t in tranches if t['is_emission'])
        manual_amount = sum(t['remaining_amount'] for t in tranches if not t['is_emission'])
        logger.debug(f"Tranche breakdown: {emission_tranches} emission tranches ({emission_amount:.4f} TAO), " +
                    f"{manual_tranches} manual tranches ({manual_amount:.4f} TAO)")
        
        total_consumed = 0
        consumed_details = []
        remaining_to_consume = amount_to_consume
        
        for tranche in tranches:
            if remaining_to_consume <= 0:
                break
            
            tranche_id = tranche['id']
            tranche_remaining = tranche['remaining_amount']
            
            # Calculate how much to take from this tranche
            amount_from_tranche = min(remaining_to_consume, tranche_remaining)
            new_remaining = tranche_remaining - amount_from_tranche
            
            # Update the tranche's remaining amount
            await self.db_manager.execute_query("""
                UPDATE stake_tranches 
                SET remaining_amount = ?, last_update = ? 
                WHERE id = ?
            """, (new_remaining, timestamp, tranche_id))
            
            # Record the exit (partial or complete)
            await self.db_manager.execute_query("""
                INSERT INTO stake_tranche_exits 
                (tranche_id, exit_amount, exit_timestamp)
                VALUES (?, ?, ?)
            """, (tranche_id, amount_from_tranche, timestamp))
            
            # Track consumption
            total_consumed += amount_from_tranche
            remaining_to_consume -= amount_from_tranche
            
            # Record details for return
            consumed_details.append({
                'tranche_id': tranche_id,
                'amount_consumed': amount_from_tranche,
                'is_emission': tranche['is_emission'],
                'entry_timestamp': tranche['entry_timestamp'],
                'holding_days': (timestamp - tranche['entry_timestamp']).days,
                'completely_consumed': new_remaining <= 0
            })
            
            logger.debug(
                f"Consumed {amount_from_tranche} TAO from {'emission' if tranche['is_emission'] else 'manual'} "
                f"tranche {tranche_id} for {hotkey} (remaining: {new_remaining} TAO)"
            )
        
        return total_consumed, consumed_details
    
    async def _update_aggregated_tranche_metrics(self, hotkey: str):
        """
        Calculate and update aggregated tranche metrics for a hotkey.
        
        This method computes metrics including:
        - Weighted average holding duration for emissions
        - Total emissions received
        - Remaining emission stake
        - Holding percentage (remaining vs. received emissions)
        
        These metrics are stored in the aggregated_tranche_metrics table
        for efficient retrieval when calculating vesting multipliers.
        
        Args:
            hotkey: The hotkey to update metrics for
        """
        try:
            current_time = datetime.now(timezone.utc)
            
            # Get all active tranches for this hotkey
            tranches = await self.db_manager.fetch_all("""
                SELECT id, initial_amount, remaining_amount, entry_timestamp, is_emission
                FROM stake_tranches
                WHERE hotkey = ?
            """, (hotkey,))
            
            if not tranches:
                # No tranches for this hotkey
                await self.db_manager.execute_query("""
                    INSERT INTO aggregated_tranche_metrics
                    (hotkey, weighted_holding_days, total_emission_received, remaining_emission_stake, holding_percentage, last_update)
                    VALUES (?, 0, 0, 0, 0, ?)
                    ON CONFLICT(hotkey) DO UPDATE SET
                    weighted_holding_days = 0,
                    total_emission_received = 0,
                    remaining_emission_stake = 0,
                    holding_percentage = 0,
                    last_update = excluded.last_update
                """, (hotkey, current_time))
                return
            
            # Convert to numpy arrays for faster computation
            tranche_ids = np.array([t['id'] for t in tranches])
            initial_amounts = np.array([t['initial_amount'] for t in tranches])
            remaining_amounts = np.array([t['remaining_amount'] for t in tranches])
            entry_timestamps = np.array([t['entry_timestamp'] for t in tranches])
            is_emission = np.array([t['is_emission'] for t in tranches], dtype=bool)
            
            # Calculate holding days for each tranche
            holding_days = np.array([(current_time - ts).days for ts in entry_timestamps])
            
            # Filter for emission tranches
            emission_mask = is_emission
            emission_initial = initial_amounts[emission_mask]
            emission_remaining = remaining_amounts[emission_mask]
            emission_holding_days = holding_days[emission_mask]
            
            # Calculate total emission metrics
            total_emission_received = np.sum(emission_initial) if len(emission_initial) > 0 else 0
            remaining_emission_stake = np.sum(emission_remaining) if len(emission_remaining) > 0 else 0
            
            # Calculate holding percentage
            holding_percentage = (remaining_emission_stake / total_emission_received) if total_emission_received > 0 else 0
            
            # Calculate weighted holding days (weighted by remaining amount)
            if remaining_emission_stake > 0 and len(emission_remaining) > 0:
                weights = emission_remaining / remaining_emission_stake
                weighted_holding_days = np.sum(emission_holding_days * weights)
            else:
                weighted_holding_days = 0
            
            # Update the aggregated metrics
            await self.db_manager.execute_query("""
                INSERT INTO aggregated_tranche_metrics
                (hotkey, weighted_holding_days, total_emission_received, remaining_emission_stake, holding_percentage, last_update)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(hotkey) DO UPDATE SET
                weighted_holding_days = excluded.weighted_holding_days,
                total_emission_received = excluded.total_emission_received,
                remaining_emission_stake = excluded.remaining_emission_stake,
                holding_percentage = excluded.holding_percentage,
                last_update = excluded.last_update
            """, (
                hotkey,
                weighted_holding_days,
                total_emission_received,
                remaining_emission_stake,
                holding_percentage,
                current_time
            ))
            
            logger.debug(
                f"Updated aggregated tranche metrics for {hotkey}: "
                f"holding_days={weighted_holding_days:.2f}, "
                f"emission_received={total_emission_received:.6f}, "
                f"emission_remaining={remaining_emission_stake:.6f}, "
                f"holding_percentage={holding_percentage:.2%}"
            )
        
        except Exception as e:
            logger.error(f"Error updating aggregated tranche metrics for {hotkey}: {e}") 
    
    async def get_tranche_details(self, hotkey: str, include_exits: bool = False) -> List[Dict]:
        """
        Get detailed information about all tranches for a hotkey.
        
        Args:
            hotkey: The hotkey to get tranche details for
            include_exits: Whether to include exit history for each tranche
            
        Returns:
            List[Dict]: List of tranche details
        """
        try:
            # Get all tranches for this hotkey
            tranches = await self.db_manager.fetch_all("""
                SELECT id, initial_amount, remaining_amount, entry_timestamp, is_emission, last_update
                FROM stake_tranches
                WHERE hotkey = ?
                ORDER BY entry_timestamp ASC
            """, (hotkey,))
            
            if not tranches:
                return []
            
            # Convert to list of dicts
            result = []
            current_time = datetime.now(timezone.utc)
            
            for tranche in tranches:
                tranche_dict = dict(tranche)
                
                # Calculate holding days
                tranche_dict['holding_days'] = (current_time - tranche['entry_timestamp']).days
                
                # Calculate percentage remaining
                if tranche['initial_amount'] > 0:
                    tranche_dict['percent_remaining'] = tranche['remaining_amount'] / tranche['initial_amount']
                else:
                    tranche_dict['percent_remaining'] = 0.0
                    
                # Add tranche type
                tranche_dict['type'] = 'emission' if tranche['is_emission'] else 'manual'
                
                # Include exit history if requested
                if include_exits:
                    exits = await self.db_manager.fetch_all("""
                        SELECT exit_amount, exit_timestamp
                        FROM stake_tranche_exits
                        WHERE tranche_id = ?
                        ORDER BY exit_timestamp ASC
                    """, (tranche['id'],))
                    
                    tranche_dict['exits'] = [dict(exit) for exit in exits]
                    tranche_dict['exit_count'] = len(exits)
                
                result.append(tranche_dict)
            
            return result
            
        except Exception as e:
            logger.error(f"Error getting tranche details for {hotkey}: {e}")
            return [] 