"""
Stake and emissions tracking for the vesting rewards system.
"""

import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Set

import bittensor as bt

from bettensor.validator.utils.database import DatabaseManager

logger = logging.getLogger(__name__)


class VestingTracker:
    """
    Tracks miner stake and emissions for calculating vesting rewards.
    
    This class is responsible for:
    1. Tracking stake amounts for each coldkey
    2. Recording emissions for each hotkey
    3. Monitoring stake changes to detect sells
    4. Calculating holding percentages and durations
    """
    
    def __init__(
        self,
        db_manager: DatabaseManager,
        metagraph: bt.metagraph,
        subnet_id: int,
    ):
        """
        Initialize the VestingTracker.
        
        Args:
            db_manager: Database manager for storing stake and emission data
            metagraph: Bittensor metagraph for accessing stake information
            subnet_id: The subnet ID to track
        """
        self.db_manager = db_manager
        self.metagraph = metagraph
        self.subnet_id = subnet_id
        
        # Cache structures for performance
        self._hotkey_to_coldkey: Dict[str, str] = {}
        self._coldkey_to_hotkeys: Dict[str, List[str]] = {}
        self._stake_history: Dict[str, List[Tuple[datetime, float]]] = {}
        self._emission_history: Dict[str, List[Tuple[datetime, float]]] = {}
        self._holding_percentages: Dict[str, float] = {}
        self._holding_durations: Dict[str, timedelta] = {}
        
        # Initialize database tables if they don't exist
        self._initialize_database()
        
    async def _initialize_database(self):
        """Initialize database tables for vesting tracking."""
        await self.db_manager.execute(
            """
            CREATE TABLE IF NOT EXISTS vesting_stake_history (
                coldkey TEXT,
                timestamp TIMESTAMP,
                stake REAL,
                PRIMARY KEY (coldkey, timestamp)
            )
            """
        )
        
        await self.db_manager.execute(
            """
            CREATE TABLE IF NOT EXISTS vesting_emission_history (
                hotkey TEXT,
                timestamp TIMESTAMP,
                amount REAL,
                PRIMARY KEY (hotkey, timestamp)
            )
            """
        )
        
        await self.db_manager.execute(
            """
            CREATE TABLE IF NOT EXISTS vesting_hotkey_coldkey (
                hotkey TEXT PRIMARY KEY,
                coldkey TEXT
            )
            """
        )
        
        await self.db_manager.execute(
            """
            CREATE TABLE IF NOT EXISTS vesting_metrics (
                hotkey TEXT,
                timestamp TIMESTAMP,
                holding_percentage REAL,
                holding_duration INTEGER,  -- in days
                multiplier REAL,
                PRIMARY KEY (hotkey, timestamp)
            )
            """
        )
    
    async def update_key_associations(self):
        """
        Update the hotkey to coldkey associations from the metagraph.
        """
        try:
            # Clear existing associations
            self._hotkey_to_coldkey = {}
            self._coldkey_to_hotkeys = {}
            
            # Get the latest associations from metagraph
            for uid in range(self.metagraph.n):
                hotkey = self.metagraph.hotkeys[uid]
                if not hotkey:
                    continue
                    
                # Get coldkey from metagraph
                coldkey = self.metagraph.coldkeys[uid]
                if not coldkey:
                    continue
                
                self._hotkey_to_coldkey[hotkey] = coldkey
                
                if coldkey not in self._coldkey_to_hotkeys:
                    self._coldkey_to_hotkeys[coldkey] = []
                
                self._coldkey_to_hotkeys[coldkey].append(hotkey)
            
            # Store in database
            async with self.db_manager.connection() as conn:
                async with conn.cursor() as cursor:
                    # Clear existing table
                    await cursor.execute("DELETE FROM vesting_hotkey_coldkey")
                    
                    # Insert new associations
                    for hotkey, coldkey in self._hotkey_to_coldkey.items():
                        await cursor.execute(
                            "INSERT INTO vesting_hotkey_coldkey (hotkey, coldkey) VALUES (?, ?)",
                            (hotkey, coldkey)
                        )
            
            logger.info(f"Updated {len(self._hotkey_to_coldkey)} hotkey-coldkey associations")
            
        except Exception as e:
            logger.error(f"Error updating key associations: {e}")
            raise
    
    async def update_stake_history(self):
        """
        Update the stake history for all coldkeys.
        """
        try:
            timestamp = datetime.now()
            coldkeys_updated: Set[str] = set()
            
            # For each UID in the metagraph
            for uid in range(self.metagraph.n):
                coldkey = self.metagraph.coldkeys[uid]
                if not coldkey or coldkey in coldkeys_updated:
                    continue
                
                # Get stake from metagraph
                stake = self.metagraph.stake[uid]
                
                # Store in database
                await self.db_manager.execute(
                    """
                    INSERT INTO vesting_stake_history (coldkey, timestamp, stake)
                    VALUES (?, ?, ?)
                    """,
                    (coldkey, timestamp, float(stake))
                )
                
                # Update cache
                if coldkey not in self._stake_history:
                    self._stake_history[coldkey] = []
                
                self._stake_history[coldkey].append((timestamp, float(stake)))
                coldkeys_updated.add(coldkey)
            
            logger.info(f"Updated stake history for {len(coldkeys_updated)} coldkeys")
            
        except Exception as e:
            logger.error(f"Error updating stake history: {e}")
            raise
    
    async def record_emissions(self, hotkey: str, amount: float):
        """
        Record emissions for a hotkey.
        
        Args:
            hotkey: The hotkey receiving emissions
            amount: The amount of tokens emitted
        """
        try:
            timestamp = datetime.now()
            
            # Store in database
            await self.db_manager.execute(
                """
                INSERT INTO vesting_emission_history (hotkey, timestamp, amount)
                VALUES (?, ?, ?)
                """,
                (hotkey, timestamp, amount)
            )
            
            # Update cache
            if hotkey not in self._emission_history:
                self._emission_history[hotkey] = []
            
            self._emission_history[hotkey].append((timestamp, amount))
            
            logger.debug(f"Recorded emission of {amount} tokens for {hotkey}")
            
        except Exception as e:
            logger.error(f"Error recording emissions for {hotkey}: {e}")
            raise
    
    async def calculate_holding_metrics(self, hotkey: str) -> Tuple[float, int]:
        """
        Calculate holding percentage and duration for a hotkey.
        
        Args:
            hotkey: The hotkey to calculate metrics for
            
        Returns:
            Tuple of (holding_percentage, holding_duration_days)
        """
        try:
            coldkey = self._hotkey_to_coldkey.get(hotkey)
            if not coldkey:
                logger.warning(f"No coldkey found for hotkey {hotkey}")
                return 0.0, 0
            
            # Get total emissions for this hotkey
            result = await self.db_manager.fetch_one(
                """
                SELECT SUM(amount) as total_emissions
                FROM vesting_emission_history
                WHERE hotkey = ?
                """,
                (hotkey,)
            )
            
            total_emissions = result["total_emissions"] if result and result["total_emissions"] else 0
            
            if total_emissions == 0:
                return 0.0, 0
            
            # Get the latest stake for this coldkey
            result = await self.db_manager.fetch_one(
                """
                SELECT stake, timestamp
                FROM vesting_stake_history
                WHERE coldkey = ?
                ORDER BY timestamp DESC
                LIMIT 1
                """,
                (coldkey,)
            )
            
            if not result:
                return 0.0, 0
            
            current_stake = result["stake"]
            latest_timestamp = result["timestamp"]
            
            # Get the number of hotkeys for this coldkey to distribute stake evenly
            num_hotkeys = len(self._coldkey_to_hotkeys.get(coldkey, []))
            if num_hotkeys == 0:
                return 0.0, 0
                
            # Approximate stake per hotkey (even distribution)
            stake_per_hotkey = current_stake / num_hotkeys
            
            # Calculate holding percentage
            holding_percentage = min(1.0, stake_per_hotkey / total_emissions) if total_emissions > 0 else 0.0
            
            # Calculate holding duration
            # This is a simplified approach. In a real implementation, we would track when emissions occurred
            # and when stake changes happened to calculate an accurate duration.
            
            # Get the earliest emission timestamp
            result = await self.db_manager.fetch_one(
                """
                SELECT MIN(timestamp) as first_emission
                FROM vesting_emission_history
                WHERE hotkey = ?
                """,
                (hotkey,)
            )
            
            if not result or not result["first_emission"]:
                return holding_percentage, 0
                
            first_emission = result["first_emission"]
            holding_duration_days = (datetime.now() - first_emission).days
            
            # Store in database
            await self.db_manager.execute(
                """
                INSERT OR REPLACE INTO vesting_metrics
                (hotkey, timestamp, holding_percentage, holding_duration, multiplier)
                VALUES (?, ?, ?, ?, 1.0)  # Default multiplier, will be updated separately
                """,
                (hotkey, datetime.now(), holding_percentage, holding_duration_days)
            )
            
            # Update cache
            self._holding_percentages[hotkey] = holding_percentage
            self._holding_durations[hotkey] = timedelta(days=holding_duration_days)
            
            return holding_percentage, holding_duration_days
            
        except Exception as e:
            logger.error(f"Error calculating holding metrics for {hotkey}: {e}")
            return 0.0, 0
    
    async def get_holding_percentage(self, hotkey: str) -> float:
        """
        Get the current holding percentage for a hotkey.
        
        Args:
            hotkey: The hotkey to get the holding percentage for
            
        Returns:
            The holding percentage (0.0-1.0)
        """
        if hotkey in self._holding_percentages:
            return self._holding_percentages[hotkey]
            
        # Calculate if not in cache
        holding_percentage, _ = await self.calculate_holding_metrics(hotkey)
        return holding_percentage
    
    async def get_holding_duration(self, hotkey: str) -> int:
        """
        Get the current holding duration in days for a hotkey.
        
        Args:
            hotkey: The hotkey to get the holding duration for
            
        Returns:
            The holding duration in days
        """
        if hotkey in self._holding_durations:
            return self._holding_durations[hotkey].days
            
        # Calculate if not in cache
        _, holding_duration = await self.calculate_holding_metrics(hotkey)
        return holding_duration
        
    async def load_from_database(self):
        """
        Load cached data from the database.
        """
        try:
            # Load hotkey to coldkey associations
            rows = await self.db_manager.fetch_all(
                "SELECT hotkey, coldkey FROM vesting_hotkey_coldkey"
            )
            
            self._hotkey_to_coldkey = {row["hotkey"]: row["coldkey"] for row in rows}
            
            # Build coldkey to hotkeys mapping
            self._coldkey_to_hotkeys = {}
            for hotkey, coldkey in self._hotkey_to_coldkey.items():
                if coldkey not in self._coldkey_to_hotkeys:
                    self._coldkey_to_hotkeys[coldkey] = []
                self._coldkey_to_hotkeys[coldkey].append(hotkey)
            
            # Load stake history
            rows = await self.db_manager.fetch_all(
                "SELECT coldkey, timestamp, stake FROM vesting_stake_history"
            )
            
            self._stake_history = {}
            for row in rows:
                coldkey = row["coldkey"]
                if coldkey not in self._stake_history:
                    self._stake_history[coldkey] = []
                self._stake_history[coldkey].append((row["timestamp"], row["stake"]))
            
            # Load emission history
            rows = await self.db_manager.fetch_all(
                "SELECT hotkey, timestamp, amount FROM vesting_emission_history"
            )
            
            self._emission_history = {}
            for row in rows:
                hotkey = row["hotkey"]
                if hotkey not in self._emission_history:
                    self._emission_history[hotkey] = []
                self._emission_history[hotkey].append((row["timestamp"], row["amount"]))
            
            # Load holding metrics
            rows = await self.db_manager.fetch_all(
                "SELECT hotkey, holding_percentage, holding_duration FROM vesting_metrics"
            )
            
            self._holding_percentages = {}
            self._holding_durations = {}
            for row in rows:
                hotkey = row["hotkey"]
                self._holding_percentages[hotkey] = row["holding_percentage"]
                self._holding_durations[hotkey] = timedelta(days=row["holding_duration"])
                
            logger.info("Loaded vesting data from database")
            
        except Exception as e:
            logger.error(f"Error loading from database: {e}")
            raise 