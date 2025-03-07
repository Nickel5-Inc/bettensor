"""
StakeTracker for managing stake metrics and calculations in the Bettensor network.

This module provides efficient tracking of:
1. Current stake amounts
2. Manual vs earned stake differentiation
3. Stake metrics per hotkey and coldkey
"""

import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from bettensor.validator.utils.database.database_manager import DatabaseManager

import numpy as np

logger = logging.getLogger(__name__)

class StakeTracker:
    """
    Tracks and manages stake metrics for the vesting system.
    
    This class maintains:
    - Current stake amounts
    - Manual vs earned stake differentiation
    - Aggregated metrics per coldkey
    """
    
    def __init__(self, db_manager: 'DatabaseManager'):
        """
        Initialize the stake tracker.
        
        Args:
            db_manager: Database manager for persistent storage
        """
        self.db_manager = db_manager
        
    async def initialize(self):
        """Initialize the tracker by setting up database tables."""
        await self._ensure_tables_exist()
        
    async def _ensure_tables_exist(self):
        """Ensure required database tables exist."""
        # Table for current stake metrics
        await self.db_manager.execute_query("""
            CREATE TABLE IF NOT EXISTS stake_metrics (
                hotkey TEXT PRIMARY KEY,
                coldkey TEXT NOT NULL,
                total_stake REAL NOT NULL DEFAULT 0,
                manual_stake REAL NOT NULL DEFAULT 0,
                earned_stake REAL NOT NULL DEFAULT 0,
                last_update DATETIME NOT NULL
            )
        """)
        
        # Table for coldkey aggregates
        await self.db_manager.execute_query("""
            CREATE TABLE IF NOT EXISTS coldkey_metrics (
                coldkey TEXT PRIMARY KEY,
                total_stake REAL NOT NULL DEFAULT 0,
                manual_stake REAL NOT NULL DEFAULT 0,
                earned_stake REAL NOT NULL DEFAULT 0,
                num_hotkeys INTEGER NOT NULL DEFAULT 0,
                last_update DATETIME NOT NULL
            )
        """)
        
        # Indices for efficient querying
        await self.db_manager.execute_query(
            "CREATE INDEX IF NOT EXISTS idx_stake_metrics_coldkey ON stake_metrics(coldkey)"
        )
    
    async def record_stake_change(
        self,
        hotkey: str,
        coldkey: str,
        amount: float,
        change_type: str,
        timestamp: Optional[datetime] = None
    ):
        """
        Record a stake change for a hotkey.
        
        Args:
            hotkey: The hotkey associated with the change
            coldkey: The coldkey associated with the hotkey
            amount: The amount of the change (positive or negative)
            change_type: Type of change ('manual_add', 'manual_remove', 'reward', 'emission')
            timestamp: Optional timestamp of the change
        """
        timestamp = timestamp or datetime.now()
        
        # Update stake metrics
        await self.db_manager.execute_query("""
            INSERT INTO stake_metrics 
            (hotkey, coldkey, total_stake, manual_stake, earned_stake, last_update)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(hotkey) DO UPDATE SET
                total_stake = total_stake + ?,
                manual_stake = CASE 
                    WHEN ? IN ('manual_add', 'manual_remove') 
                    THEN manual_stake + ? 
                    ELSE manual_stake 
                END,
                earned_stake = CASE 
                    WHEN ? IN ('reward', 'emission') 
                    THEN earned_stake + ? 
                    ELSE earned_stake 
                END,
                last_update = ?
        """, (
            hotkey, coldkey, amount, 
            amount if change_type in ('manual_add', 'manual_remove') else 0,
            amount if change_type in ('reward', 'emission') else 0,
            timestamp,
            amount, change_type, amount, change_type, amount, timestamp
        ))
        
        # Update coldkey aggregates
        await self._update_coldkey_metrics(coldkey)
    
    async def _update_coldkey_metrics(self, coldkey: str):
        """Update aggregated metrics for a coldkey."""
        # Calculate aggregates from stake_metrics
        metrics = await self.db_manager.fetch_one("""
            SELECT 
                COUNT(*) as num_hotkeys,
                SUM(total_stake) as total_stake,
                SUM(manual_stake) as manual_stake,
                SUM(earned_stake) as earned_stake,
                MAX(last_update) as last_update
            FROM stake_metrics
            WHERE coldkey = ?
        """, (coldkey,))
        
        if metrics:
            await self.db_manager.execute_query("""
                INSERT INTO coldkey_metrics 
                (coldkey, total_stake, manual_stake, earned_stake, num_hotkeys, last_update)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(coldkey) DO UPDATE SET
                    total_stake = ?,
                    manual_stake = ?,
                    earned_stake = ?,
                    num_hotkeys = ?,
                    last_update = ?
            """, (
                coldkey, metrics['total_stake'], metrics['manual_stake'],
                metrics['earned_stake'], metrics['num_hotkeys'], metrics['last_update'],
                metrics['total_stake'], metrics['manual_stake'],
                metrics['earned_stake'], metrics['num_hotkeys'], metrics['last_update']
            ))
    
    async def get_stake_metrics(self, hotkey: str) -> Optional[Dict]:
        """
        Get current stake metrics for a hotkey.
        
        Args:
            hotkey: The hotkey to get metrics for
            
        Returns:
            Dictionary with stake metrics or None if not found
        """
        result = await self.db_manager.fetch_one(
            "SELECT * FROM stake_metrics WHERE hotkey = ?",
            (hotkey,)
        )
        return dict(result) if result else None
    
    async def get_coldkey_metrics(self, coldkey: str) -> Optional[Dict]:
        """
        Get aggregated metrics for a coldkey.
        
        Args:
            coldkey: The coldkey to get metrics for
            
        Returns:
            Dictionary with aggregated metrics or None if not found
        """
        result = await self.db_manager.fetch_one(
            "SELECT * FROM coldkey_metrics WHERE coldkey = ?",
            (coldkey,)
        )
        return dict(result) if result else None
    
    async def get_all_hotkeys_for_coldkey(self, coldkey: str) -> List[str]:
        """Get all hotkeys associated with a coldkey."""
        rows = await self.db_manager.fetch_all(
            "SELECT hotkey FROM stake_metrics WHERE coldkey = ?",
            (coldkey,)
        )
        return [row['hotkey'] for row in rows]
    
    async def calculate_holding_metrics(
        self,
        hotkey: str,
        window_days: int = 30
    ) -> Tuple[float, int]:
        """
        Calculate holding percentage and duration for a hotkey.
        
        Args:
            hotkey: The hotkey to calculate metrics for
            window_days: Number of days to look back
            
        Returns:
            Tuple of (holding_percentage, holding_duration_days)
        """
        metrics = await self.get_stake_metrics(hotkey)
        if not metrics:
            return 0.0, 0
            
        # Calculate holding percentage (earned stake / total earned rewards)
        total_earned = metrics['earned_stake']
        if total_earned <= 0:
            return 0.0, 0
            
        holding_percentage = max(0.0, min(1.0, metrics['total_stake'] / total_earned))
        
        # Calculate holding duration
        cutoff_date = datetime.now() - timedelta(days=window_days)
        last_removal = await self.db_manager.fetch_one("""
            SELECT MAX(last_update) as last_removal
            FROM stake_metrics
            WHERE hotkey = ? 
            AND total_stake < LAG(total_stake) OVER (ORDER BY last_update)
            AND last_update >= ?
        """, (hotkey, cutoff_date))
        
        if last_removal and last_removal['last_removal']:
            holding_duration = (datetime.now() - last_removal['last_removal']).days
        else:
            holding_duration = window_days
            
        return holding_percentage, holding_duration
    
    async def get_stake_distribution(self) -> Dict[str, List[float]]:
        """
        Get the current stake distribution across all miners.
        
        Returns:
            Dictionary with lists for total_stakes, manual_stakes, earned_stakes
        """
        rows = await self.db_manager.fetch_all(
            "SELECT total_stake, manual_stake, earned_stake FROM stake_metrics"
        )
        
        return {
            'total_stakes': [row['total_stake'] for row in rows],
            'manual_stakes': [row['manual_stake'] for row in rows],
            'earned_stakes': [row['earned_stake'] for row in rows]
        } 