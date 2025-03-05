"""
Stake requirements and emissions retention for the vesting system.

This module provides functionality to enforce minimum stake requirements
and calculate emissions retention multipliers.
"""

import logging
from datetime import datetime, timedelta
from typing import Dict, List, Set, Tuple, Optional, Any, Union
import numpy as np

from bittensor.utils.balance import Balance
import bittensor as bt

from bettensor.validator.utils.database import DatabaseManager
from bettensor.validator.utils.vesting.database.models import (
    StakeHistory,
    StakeTransaction,
    EpochEmissions,
    StakeMinimumRequirement,
    HotkeyColdkeyAssociation
)
from sqlalchemy import and_, func
from bettensor.validator.utils.vesting.core.retry import TransactionManager, with_retries

logger = logging.getLogger(__name__)


class StakeRequirements:
    """
    Enforces minimum stake requirements and calculates emissions retention.
    
    This class is responsible for:
    1. Checking if miners meet minimum stake requirements
    2. Tracking emissions by epoch
    3. Calculating emissions retention rates
    4. Computing retention-based multipliers
    """
    
    def __init__(
        self,
        db_manager: DatabaseManager,
        metagraph: 'bt.metagraph.Metagraph',
        subtensor_client: Optional['SubtensorClient'] = None,
        minimum_stake: float = 0.3,
        retention_window_days: int = 30,
        retention_target: float = 0.9  # Target: retain 90% of emissions
    ):
        """
        Initialize the stake requirements handler.
        
        Args:
            db_manager: Database manager for persistent storage
            metagraph: Bittensor metagraph for accessing stake information
            subtensor_client: Client for Subtensor blockchain interactions
            minimum_stake: Minimum stake required per hotkey (in TAO)
            retention_window_days: Number of days to consider for retention calculation
            retention_target: Target retention rate (0.0-1.0) for full multiplier
        """
        self.db_manager = db_manager
        self.metagraph = metagraph
        self.subtensor_client = subtensor_client
        
        # Get DynamicInfo for tao/alpha conversions if subtensor_client is provided
        self.dynamic_info = None
        if self.subtensor_client:
            self.dynamic_info = self.subtensor_client.get_dynamic_info()
        
        # Store minimum stake in tao
        self.minimum_stake = minimum_stake
        # Convert to alpha if dynamic_info is available
        self.minimum_stake_alpha = (
            self.dynamic_info.tao_to_alpha(minimum_stake).tao 
            if self.dynamic_info else minimum_stake
        )
        
        self.retention_window_days = retention_window_days
        self.retention_target = retention_target
        
        # Initialize transaction manager
        self.transaction_manager = TransactionManager(db_manager)
        
        # Cache for performance
        self._requirement_cache = {}  # hotkey -> (is_meeting_requirement, timestamp)
        self._retention_multiplier_cache = {}  # hotkey -> (multiplier, timestamp)
        self._cache_ttl = 600  # 10 minutes in seconds
    
    @with_retries()
    async def update_minimum_requirements(self):
        """Update minimum stake requirements with retry support."""
        try:
            now = datetime.utcnow()
            
            # Get valid hotkeys from metagraph
            valid_hotkeys = [h for h in self.metagraph.hotkeys if not h.startswith("0x")]
            if not valid_hotkeys:
                logger.warning("No valid hotkeys found in metagraph")
                return
                
            # Define transaction operations
            async def get_current_data(session):
                result = await session.execute(
                    """
                    WITH LatestStakes AS (
                        SELECT 
                            h.hotkey,
                            h.stake,
                            h.coldkey
                        FROM 
                            vesting_stake_history h
                        INNER JOIN (
                            SELECT 
                                hotkey, 
                                MAX(timestamp) as max_timestamp
                            FROM 
                                vesting_stake_history
                            WHERE 
                                hotkey = ANY(:hotkeys)
                            GROUP BY 
                                hotkey
                        ) latest ON h.hotkey = latest.hotkey 
                        AND h.timestamp = latest.max_timestamp
                    )
                    SELECT 
                        v.hotkey,
                        COALESCE(ls.stake, 0.0) as stake,
                        COALESCE(ls.coldkey, a.coldkey) as coldkey,
                        r.is_meeting_requirement as current_requirement
                    FROM 
                        (SELECT UNNEST(:hotkeys) as hotkey) v
                    LEFT JOIN LatestStakes ls ON v.hotkey = ls.hotkey
                    LEFT JOIN vesting_hotkey_coldkey_associations a ON v.hotkey = a.hotkey
                    LEFT JOIN vesting_stake_minimum_requirements r ON v.hotkey = r.hotkey
                    """,
                    {"hotkeys": valid_hotkeys}
                )
                return result.fetchall()
            
            async def process_requirements(session, data):
                to_update = []
                to_create = []
                
                for row in data:
                    hotkey = row[0]
                    stake = row[1]
                    coldkey = row[2] or ""
                    current_requirement = row[3]
                    
                    # Try metagraph if stake is 0
                    if stake == 0:
                        try:
                            uid = self.metagraph.hotkeys.index(hotkey)
                            stake = float(self.metagraph.S[uid])
                        except (ValueError, IndexError):
                            stake = 0.0
                    
                    # Check if requirement is met
                    is_meeting_requirement = stake >= self.minimum_stake
                    
                    # Update cache
                    self._requirement_cache[hotkey] = (is_meeting_requirement, now)
                    
                    if current_requirement is not None:
                        to_update.append({
                            "hotkey": hotkey,
                            "coldkey": coldkey,
                            "is_meeting_requirement": is_meeting_requirement,
                            "last_updated": now
                        })
                    else:
                        to_create.append({
                            "hotkey": hotkey,
                            "coldkey": coldkey,
                            "minimum_stake": self.minimum_stake,
                            "is_meeting_requirement": is_meeting_requirement,
                            "last_updated": now
                        })
                
                # Batch update existing records
                if to_update:
                    await self.transaction_manager.execute_batch_insert(
                        table_name="vesting_stake_minimum_requirements",
                        columns=["hotkey", "coldkey", "is_meeting_requirement", "last_updated"],
                        values=to_update,
                        conflict_resolution="""
                        ON CONFLICT (hotkey) DO UPDATE SET
                            coldkey = EXCLUDED.coldkey,
                            is_meeting_requirement = EXCLUDED.is_meeting_requirement,
                            last_updated = EXCLUDED.last_updated
                        """
                    )
                
                # Batch insert new records
                if to_create:
                    await self.transaction_manager.execute_batch_insert(
                        table_name="vesting_stake_minimum_requirements",
                        columns=["hotkey", "coldkey", "minimum_stake", "is_meeting_requirement", "last_updated"],
                        values=to_create
                    )
                
                return len(to_update) + len(to_create)
            
            # Execute transaction
            data = await get_current_data(None)
            count = await process_requirements(None, data)
            
            logger.info(f"Updated minimum stake requirements for {count} hotkeys")
            
        except Exception as e:
            logger.error(f"Error updating minimum requirements: {e}")
            raise
    
    @with_retries()
    async def check_minimum_requirements(self, hotkeys: Optional[List[str]] = None) -> Dict[str, bool]:
        """Check minimum stake requirements for hotkeys with retry support."""
        try:
            if not hotkeys:
                hotkeys = [h for h in self.metagraph.hotkeys if not h.startswith("0x")]
            
            if not hotkeys:
                return {}
            
            now = datetime.utcnow()
            result = {}
            uncached_hotkeys = []
            
            # Check cache first
            for hotkey in hotkeys:
                cache_key = f"minimum_requirement_{hotkey}"
                if cache_key in self._requirement_cache:
                    cached_value, cached_time = self._requirement_cache[cache_key]
                    if (now - cached_time).total_seconds() < self._cache_ttl:
                        result[hotkey] = cached_value
                        continue
                uncached_hotkeys.append(hotkey)
            
            if not uncached_hotkeys:
                return result
            
            # Get requirements for uncached hotkeys
            async def get_requirements_batch(session):
                query_result = await session.execute(
                    """
                    SELECT 
                        v.hotkey,
                        COALESCE(r.is_meeting_requirement, false) as meets_requirement
                    FROM 
                        (SELECT UNNEST(:hotkeys) as hotkey) v
                    LEFT JOIN vesting_stake_minimum_requirements r 
                    ON v.hotkey = r.hotkey
                    """,
                    {"hotkeys": uncached_hotkeys}
                )
                return {row[0]: row[1] for row in query_result}
            
            requirements = await get_requirements_batch(None)
            
            # Update cache and result
            for hotkey in uncached_hotkeys:
                meets_requirement = requirements.get(hotkey, False)
                cache_key = f"minimum_requirement_{hotkey}"
                self._requirement_cache[cache_key] = (meets_requirement, now)
                result[hotkey] = meets_requirement
            
            return result
            
        except Exception as e:
            logger.error(f"Error checking minimum requirements: {e}")
            raise
    
    @with_retries()
    async def check_coldkey_total_requirements(self) -> Dict[str, bool]:
        """Check total stake requirements for coldkeys with retry support."""
        try:
            # Get current stake totals by coldkey
            async def get_coldkey_totals(session):
                result = await session.execute(
                    """
                    WITH LatestStakes AS (
                        SELECT 
                            h.hotkey,
                            h.coldkey,
                            h.stake
                        FROM 
                            vesting_stake_history h
                        INNER JOIN (
                            SELECT 
                                hotkey, 
                                MAX(timestamp) as max_timestamp
                            FROM 
                                vesting_stake_history
                            GROUP BY 
                                hotkey
                        ) latest ON h.hotkey = latest.hotkey 
                        AND h.timestamp = latest.max_timestamp
                    )
                    SELECT 
                        coldkey,
                        SUM(stake) as total_stake
                    FROM LatestStakes
                    WHERE coldkey != ''
                    GROUP BY coldkey
                    """,
                )
                return {row[0]: row[1] for row in result}
            
            coldkey_totals = await get_coldkey_totals(None)
            
            # Check requirements for each coldkey
            requirements = {}
            for coldkey, total_stake in coldkey_totals.items():
                requirements[coldkey] = total_stake >= self.minimum_stake
            
            return requirements
            
        except Exception as e:
            logger.error(f"Error checking coldkey total requirements: {e}")
            raise
    
    @with_retries()
    async def record_epoch_emissions(self, epoch: int, emissions: Dict[str, float]):
        """
        Record emissions for a specific epoch.
        
        Args:
            epoch: The epoch number
            emissions: Dict mapping hotkeys to emission amounts (in TAO)
        """
        if not emissions:
            return
        
        try:
            # Process in groups to avoid overwhelming the database
            processed_count = 0
            total_count = len(emissions)
            
            async def get_coldkeys(session):
                # Get all mappings at once for efficiency
                result = await session.execute(
                    """
                    SELECT hotkey, coldkey 
                    FROM vesting_hotkey_coldkey_associations
                    WHERE hotkey = ANY(:hotkeys)
                    """,
                    {"hotkeys": list(emissions.keys())}
                )
                
                return {row[0]: row[1] for row in result}
                
            hotkey_to_coldkey = await get_coldkeys(None)
            
            # Prepare records
            emissions_records = []
            
            for hotkey, amount in emissions.items():
                coldkey = hotkey_to_coldkey.get(hotkey, "")
                
                if not coldkey:
                    logger.warning(f"No coldkey found for hotkey {hotkey}, skipping emission record")
                    continue
                
                # Convert amount from tao to alpha if dynamic_info is available
                amount_alpha = (
                    self.dynamic_info.tao_to_alpha(amount).tao
                    if self.dynamic_info else amount
                )
                
                emissions_records.append({
                    "epoch": epoch,
                    "hotkey": hotkey,
                    "coldkey": coldkey,
                    "amount": amount_alpha,  # Store in alpha units
                    "retained_amount": amount_alpha,  # Initially retain 100%
                    "timestamp": datetime.utcnow()
                })
                processed_count += 1
            
            # Batch insert
            for batch in self._batch_list(emissions_records, 100):
                await self.transaction_manager.execute_batch_insert(
                    table_name="vesting_epoch_emissions",
                    columns=[
                        "epoch", "hotkey", "coldkey", "amount", 
                        "retained_amount", "timestamp"
                    ],
                    values=batch
                )
            
            logger.info(f"Recorded {processed_count}/{total_count} emissions for epoch {epoch}")
            
        except Exception as e:
            logger.error(f"Error recording epoch emissions: {e}")
            raise
    
    @with_retries()
    async def update_retained_emissions(self):
        """
        Update retained emissions based on stake transactions.
        
        This method analyzes stake removals and adjusts retained emissions
        accordingly, prioritizing deductions from manually added stake before
        affecting retained emissions from rewards.
        """
        try:
            # Set the retention window
            cutoff_date = datetime.utcnow() - timedelta(days=self.retention_window_days)
            
            async def get_emissions_to_process(session):
                # Get emissions within the retention window
                result = await session.execute(
                    """
                    SELECT id, hotkey, coldkey, epoch, amount, retained_amount, timestamp
                    FROM vesting_epoch_emissions
                    WHERE timestamp >= :cutoff_date
                    ORDER BY hotkey, timestamp
                    """,
                    {"cutoff_date": cutoff_date}
                )
                
                # Group by hotkey for easier processing
                emissions_by_hotkey = {}
                for row in result:
                    hotkey = row[1]
                    if hotkey not in emissions_by_hotkey:
                        emissions_by_hotkey[hotkey] = []
                    
                    # Convert back to tao for calculations if needed
                    amount = row[4]
                    retained = row[5]
                    
                    if self.dynamic_info:
                        # These values are stored in alpha, convert to tao for consistency with stake transactions
                        amount = self.dynamic_info.alpha_to_tao(amount).tao
                        retained = self.dynamic_info.alpha_to_tao(retained).tao
                    
                    emissions_by_hotkey[hotkey].append({
                        "id": row[0],
                        "hotkey": hotkey,
                        "coldkey": row[2],
                        "epoch": row[3],
                        "amount": amount,
                        "retained_amount": retained,
                        "timestamp": row[6]
                    })
                
                return emissions_by_hotkey
            
            emissions_by_hotkey = await get_emissions_to_process(None)
            
            if not emissions_by_hotkey:
                logger.info("No emissions to update")
                return
            
            logger.info(f"Processing retained emissions updates for {len(emissions_by_hotkey)} hotkeys")
            
            # Process hotkeys in batches to manage memory usage
            hotkeys = list(emissions_by_hotkey.keys())
            updates = []
            
            for hotkey_batch in self._batch_list(hotkeys, 50):
                # Fetch all stake reductions for this batch in a single query
                async def get_reductions_batch(session):
                    result = await session.execute(
                        """
                        SELECT hotkey, SUM(amount) as total_removed
                        FROM vesting_stake_transactions
                        WHERE hotkey = ANY(:hotkeys)
                          AND transaction_type = 'remove_stake'
                          AND block_timestamp >= :cutoff_date
                        GROUP BY hotkey
                        """,
                        {"hotkeys": hotkey_batch, "cutoff_date": cutoff_date}
                    )
                    
                    removed_by_hotkey = {}
                    for row in result:
                        hotkey = row[0]
                        amount = row[1]
                        
                        # Convert from alpha to tao if needed
                        if self.dynamic_info:
                            amount = self.dynamic_info.alpha_to_tao(amount).tao
                            
                        removed_by_hotkey[hotkey] = amount
                    
                    return removed_by_hotkey
                
                removed_by_hotkey = await get_reductions_batch(None)
                
                # Process each hotkey in the batch
                for hotkey in hotkey_batch:
                    emissions = emissions_by_hotkey.get(hotkey, [])
                    total_removed = removed_by_hotkey.get(hotkey, 0)
                    
                    if not emissions or total_removed <= 0:
                        continue
                    
                    # Also get manually added stake to prioritize deductions from it
                    async def get_manual_stake(session):
                        result = await session.execute(
                            """
                            SELECT SUM(amount) as total_added
                            FROM vesting_stake_transactions
                            WHERE hotkey = :hotkey
                              AND transaction_type = 'add_stake'
                              AND block_timestamp >= :cutoff_date
                            """,
                            {"hotkey": hotkey, "cutoff_date": cutoff_date}
                        )
                        
                        row = result.fetchone()
                        amount = row[0] if row and row[0] else 0
                        
                        # Convert from alpha to tao if needed
                        if self.dynamic_info and amount > 0:
                            amount = self.dynamic_info.alpha_to_tao(amount).tao
                            
                        return amount
                    
                    manually_added = await get_manual_stake(None)
                    
                    # Calculate how much to deduct from retained emissions
                    # Prioritize deductions from manually added stake
                    remaining_deduction = max(0, total_removed - manually_added)
                    
                    if remaining_deduction <= 0:
                        # All removals covered by manually added stake
                        continue
                    
                    logger.info(f"Adjusting retained emissions for {hotkey}: "
                               f"total removed={total_removed}, manually added={manually_added}, "
                               f"remaining deduction={remaining_deduction}")
                    
                    # Update each emission record, oldest first
                    for emission in sorted(emissions, key=lambda e: e["timestamp"]):
                        if remaining_deduction <= 0:
                            break
                            
                        # Calculate how much to reduce this emission by
                        current_retained = emission["retained_amount"]
                        reduction = min(remaining_deduction, current_retained)
                        new_retained = max(0, current_retained - reduction)
                        remaining_deduction -= reduction
                        
                        if current_retained != new_retained:
                            # Convert back to alpha for storage if needed
                            new_retained_alpha = new_retained
                            if self.dynamic_info:
                                new_retained_alpha = self.dynamic_info.tao_to_alpha(new_retained).tao
                                
                            updates.append({
                                "id": emission["id"],
                                "new_retained": new_retained_alpha
                            })
            
            # Commit updates in bulk
            if updates:
                chunk_size = 100
                for i in range(0, len(updates), chunk_size):
                    chunk = updates[i:i + chunk_size]
                    
                    placeholders = ", ".join([f"(:id{i}, :new_retained{i})" for i in range(len(chunk))])
                    params = {}
                    for idx, update in enumerate(chunk):
                        params[f"id{idx}"] = update["id"]
                        params[f"new_retained{idx}"] = update["new_retained"]
                    
                    async with self.db_manager.session() as session:
                        # Use a temporary table for bulk updates
                        await session.execute(
                            f"""
                            WITH updates(id, new_retained) AS (
                                VALUES {placeholders}
                            )
                            UPDATE vesting_epoch_emissions AS e
                            SET retained_amount = u.new_retained
                            FROM updates u
                            WHERE e.id = u.id
                            """,
                            params
                        )
                        await session.commit()
                
                logger.info(f"Updated retained amounts for {len(updates)} emission records")
            else:
                logger.info("No retained emissions needed updating")
            
        except Exception as e:
            logger.error(f"Error updating retained emissions: {e}")
            raise
    
    def _batch_list(self, items, batch_size):
        """Helper function to batch a list into chunks."""
        for i in range(0, len(items), batch_size):
            yield items[i:i + batch_size]
    
    @with_retries()
    async def calculate_retention_multipliers_batch(
        self,
        hotkeys: List[str]
    ) -> Dict[str, float]:
        """Calculate retention multipliers for multiple hotkeys with retry support."""
        try:
            if not hotkeys:
                return {}
            
            cutoff_date = datetime.utcnow() - timedelta(days=self.retention_window_days)
            now = datetime.utcnow()
            
            # Check cache first
            result = {}
            uncached_hotkeys = []
            
            for hotkey in hotkeys:
                cache_key = f"retention_multiplier_{hotkey}"
                if cache_key in self._retention_multiplier_cache:
                    cached_value, cached_time = self._retention_multiplier_cache[cache_key]
                    if (now - cached_time).total_seconds() < self._cache_ttl:
                        result[hotkey] = cached_value
                        continue
                uncached_hotkeys.append(hotkey)
            
            if not uncached_hotkeys:
                return result
            
            # Get emission and retention data for uncached hotkeys
            async def get_retention_data_batch(session):
                query_result = await session.execute(
                    """
                    SELECT 
                        hotkey,
                        SUM(emission_amount) as total_emissions,
                        SUM(retained_amount) as total_retained
                    FROM vesting_epoch_emissions
                    WHERE hotkey = ANY(:hotkeys)
                    AND timestamp >= :cutoff_date
                    GROUP BY hotkey
                    """,
                    {
                        "hotkeys": uncached_hotkeys,
                        "cutoff_date": cutoff_date
                    }
                )
                return {row[0]: (row[1], row[2]) for row in query_result}
            
            retention_data = await get_retention_data_batch(None)
            
            # Calculate multipliers for uncached hotkeys
            for hotkey in uncached_hotkeys:
                total_emissions, total_retained = retention_data.get(hotkey, (0.0, 0.0))
                
                if total_emissions == 0:
                    multiplier = 1.0
                else:
                    retention_ratio = total_retained / total_emissions
                    multiplier = min(1.0, retention_ratio / self.retention_target)
                
                # Update cache and result
                cache_key = f"retention_multiplier_{hotkey}"
                self._retention_multiplier_cache[cache_key] = (multiplier, now)
                result[hotkey] = multiplier
            
            return result
            
        except Exception as e:
            logger.error(f"Error calculating batch retention multipliers: {e}")
            raise
    
    @with_retries()
    async def calculate_retention_multiplier(self, hotkey: str) -> float:
        """Calculate retention multiplier for a hotkey with retry support."""
        try:
            # Check cache first
            cache_key = f"retention_multiplier_{hotkey}"
            if cache_key in self._retention_multiplier_cache:
                cached_value, cached_time = self._retention_multiplier_cache[cache_key]
                if (datetime.utcnow() - cached_time).total_seconds() < self._cache_ttl:
                    return cached_value
            
            cutoff_date = datetime.utcnow() - timedelta(days=self.retention_window_days)
            
            # Get emission and retention data
            async def get_retention_data(session):
                result = await session.execute(
                    """
                    SELECT 
                        SUM(emission_amount) as total_emissions,
                        SUM(retained_amount) as total_retained
                    FROM vesting_epoch_emissions
                    WHERE hotkey = :hotkey
                    AND timestamp >= :cutoff_date
                    """,
                    {
                        "hotkey": hotkey,
                        "cutoff_date": cutoff_date
                    }
                )
                return result.fetchone()
            
            row = await get_retention_data(None)
            total_emissions, total_retained = row if row else (0.0, 0.0)
            
            if total_emissions == 0:
                multiplier = 1.0
            else:
                retention_ratio = total_retained / total_emissions
                # Linear scaling between 0 and target retention
                multiplier = min(1.0, retention_ratio / self.retention_target)
            
            # Update cache
            self._retention_multiplier_cache[cache_key] = (multiplier, datetime.utcnow())
            
            return multiplier
            
        except Exception as e:
            logger.error(f"Error calculating retention multiplier for {hotkey}: {e}")
            raise
    
    @with_retries()
    async def apply_minimum_requirements_filter(self, weights: np.ndarray) -> np.ndarray:
        """Apply minimum stake requirements filter to weights with retry support."""
        try:
            if len(weights) == 0:
                return weights
            
            hotkeys = [h for h in self.metagraph.hotkeys if not h.startswith("0x")]
            if len(hotkeys) != len(weights):
                logger.error("Mismatch between weights and hotkeys length")
                return weights
            
            # Get requirements status for all hotkeys
            requirements = await self.check_minimum_requirements(hotkeys)
            
            # Create mask array
            mask = np.ones_like(weights)
            for idx, hotkey in enumerate(hotkeys):
                if not requirements.get(hotkey, False):
                    mask[idx] = 0
            
            # Apply mask
            filtered_weights = weights * mask
            
            # Normalize if any weights remain
            if filtered_weights.sum() > 0:
                filtered_weights = filtered_weights / filtered_weights.sum()
            
            return filtered_weights
            
        except Exception as e:
            logger.error(f"Error applying minimum requirements filter: {e}")
            raise
    
    @with_retries()
    async def apply_retention_multipliers(self, weights: np.ndarray) -> np.ndarray:
        """Apply retention multipliers to weights with retry support."""
        try:
            if len(weights) == 0:
                return weights
            
            hotkeys = [h for h in self.metagraph.hotkeys if not h.startswith("0x")]
            if len(hotkeys) != len(weights):
                logger.error("Mismatch between weights and hotkeys length")
                return weights
            
            # Get multipliers for all hotkeys
            multipliers = await self.calculate_retention_multipliers_batch(hotkeys)
            
            # Create multiplier array
            multiplier_array = np.array([multipliers.get(hotkey, 1.0) for hotkey in hotkeys])
            
            # Apply multipliers
            adjusted_weights = weights * multiplier_array
            
            # Normalize if any weights remain
            if adjusted_weights.sum() > 0:
                adjusted_weights = adjusted_weights / adjusted_weights.sum()
            
            return adjusted_weights
            
        except Exception as e:
            logger.error(f"Error applying retention multipliers: {e}")
            raise 