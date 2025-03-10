"""
Database models for vesting functionality.

This module defines the database models for tracking miner stake, stake
transactions, vesting schedules, and vesting payments.
"""

from typing import Optional, List, Dict, Any
from datetime import datetime
import json

from bettensor.validator.utils.database.base import BaseModel, Column


class MinerStakeHistory(BaseModel):
    """
    Model for tracking stake history for miners over time.
    
    This records the stake values at different points in time, as well as
    changes in stake and the breakdown of those changes (manual adjustments
    vs earned rewards).
    """
    
    __tablename__ = "miner_stake_history"
    
    id = Column("id", "INTEGER", primary_key=True, autoincrement=True)
    hotkey = Column("hotkey", "TEXT", nullable=False, index=True)
    coldkey = Column("coldkey", "TEXT", nullable=False, index=True)
    timestamp = Column("timestamp", "TIMESTAMP", nullable=False)
    stake_before = Column("stake_before", "REAL", nullable=False)
    stake_after = Column("stake_after", "REAL", nullable=False)
    manual_adjustment = Column("manual_adjustment", "REAL", nullable=False, default=0.0)
    earned_reward = Column("earned_reward", "REAL", nullable=False, default=0.0)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert model to dictionary."""
        return {
            "id": self.id,
            "hotkey": self.hotkey,
            "coldkey": self.coldkey,
            "timestamp": self.timestamp.isoformat() if isinstance(self.timestamp, datetime) else self.timestamp,
            "stake_before": self.stake_before,
            "stake_after": self.stake_after,
            "manual_adjustment": self.manual_adjustment,
            "earned_reward": self.earned_reward,
            "stake_change": self.stake_after - self.stake_before
        }


class StakeTransaction(BaseModel):
    """
    Model for tracking stake transactions on the blockchain.
    
    This includes:
    - AddStake: Adding stake to a hotkey
    - RemoveStake: Removing stake from a hotkey
    - MoveStake: Moving stake between hotkeys (tracked as a pair of Add/Remove)
    """
    
    __tablename__ = "stake_transactions"
    
    id = Column("id", "INTEGER", primary_key=True, autoincrement=True)
    hotkey = Column("hotkey", "TEXT", nullable=False, index=True)
    coldkey = Column("coldkey", "TEXT", nullable=False, index=True)
    transaction_type = Column("transaction_type", "TEXT", nullable=False)  # AddStake, RemoveStake, MoveStake
    amount = Column("amount", "REAL", nullable=False)
    timestamp = Column("timestamp", "TIMESTAMP", nullable=False)
    block_number = Column("block_number", "INTEGER", nullable=False)
    extrinsic_index = Column("extrinsic_index", "TEXT", nullable=True)
    status = Column("status", "TEXT", nullable=False, default="success")  # success, failed, pending
    details = Column("details", "TEXT", nullable=True)  # JSON string for additional data
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert model to dictionary."""
        result = {
            "id": self.id,
            "hotkey": self.hotkey,
            "coldkey": self.coldkey,
            "transaction_type": self.transaction_type,
            "amount": self.amount,
            "timestamp": self.timestamp.isoformat() if isinstance(self.timestamp, datetime) else self.timestamp,
            "block_number": self.block_number,
            "extrinsic_index": self.extrinsic_index,
            "status": self.status
        }
        
        if self.details:
            try:
                result["details"] = json.loads(self.details)
            except (json.JSONDecodeError, TypeError):
                result["details"] = self.details
        
        return result


class HotkeyColdkeyAssociation(BaseModel):
    """Model for tracking associations between hotkeys and coldkeys."""
    
    __tablename__ = "vesting_hotkey_coldkey"
    
    id = Column("id", "INTEGER", primary_key=True, autoincrement=True)
    hotkey = Column("hotkey", "TEXT", nullable=False, unique=True)
    coldkey = Column("coldkey", "TEXT", nullable=False, index=True)
    last_updated = Column("last_updated", "TIMESTAMP", nullable=False, default=datetime.now)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert model to dictionary."""
        return {
            "id": self.id,
            "hotkey": self.hotkey,
            "coldkey": self.coldkey,
            "last_updated": self.last_updated.isoformat() if isinstance(self.last_updated, datetime) else self.last_updated
        }


class VestingSchedule(BaseModel):
    """
    Model for vesting schedules.
    
    A vesting schedule represents a plan to distribute earned rewards over
    a specified period of time.
    """
    
    __tablename__ = "vesting_schedules"
    
    id = Column("id", "INTEGER", primary_key=True, autoincrement=True)
    hotkey = Column("hotkey", "TEXT", nullable=False, index=True)
    coldkey = Column("coldkey", "TEXT", nullable=False, index=True)
    total_amount = Column("total_amount", "REAL", nullable=False)
    start_date = Column("start_date", "TIMESTAMP", nullable=False)
    end_date = Column("end_date", "TIMESTAMP", nullable=False)
    duration_days = Column("duration_days", "INTEGER", nullable=False)
    interval_days = Column("interval_days", "INTEGER", nullable=False)
    amount_per_interval = Column("amount_per_interval", "REAL", nullable=False)
    distributed_amount = Column("distributed_amount", "REAL", nullable=False, default=0.0)
    status = Column("status", "TEXT", nullable=False, default="active")  # active, completed, canceled
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert model to dictionary."""
        return {
            "id": self.id,
            "hotkey": self.hotkey,
            "coldkey": self.coldkey,
            "total_amount": self.total_amount,
            "start_date": self.start_date.isoformat() if isinstance(self.start_date, datetime) else self.start_date,
            "end_date": self.end_date.isoformat() if isinstance(self.end_date, datetime) else self.end_date,
            "duration_days": self.duration_days,
            "interval_days": self.interval_days,
            "amount_per_interval": self.amount_per_interval,
            "distributed_amount": self.distributed_amount,
            "status": self.status,
            "remaining_amount": self.total_amount - self.distributed_amount,
            "progress_percentage": (self.distributed_amount / self.total_amount) * 100 if self.total_amount > 0 else 0
        }


class VestingPayment(BaseModel):
    """
    Model for vesting payments.
    
    Each payment represents a single distribution from a vesting schedule.
    """
    
    __tablename__ = "vesting_payments"
    
    id = Column("id", "INTEGER", primary_key=True, autoincrement=True)
    schedule_id = Column("schedule_id", "INTEGER", nullable=False, index=True)
    hotkey = Column("hotkey", "TEXT", nullable=False, index=True)
    coldkey = Column("coldkey", "TEXT", nullable=False, index=True)
    amount = Column("amount", "REAL", nullable=False)
    payment_date = Column("payment_date", "TIMESTAMP", nullable=False)
    status = Column("status", "TEXT", nullable=False, default="scheduled")  # scheduled, paid, failed
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert model to dictionary."""
        return {
            "id": self.id,
            "schedule_id": self.schedule_id,
            "hotkey": self.hotkey,
            "coldkey": self.coldkey,
            "amount": self.amount,
            "payment_date": self.payment_date.isoformat() if isinstance(self.payment_date, datetime) else self.payment_date,
            "status": self.status
        }


# Schema definitions for database initialization
VESTING_TABLES_SCHEMA = [
    """
    CREATE TABLE IF NOT EXISTS miner_stake_history (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        hotkey TEXT NOT NULL,
        coldkey TEXT NOT NULL,
        timestamp TIMESTAMP NOT NULL,
        stake_before REAL NOT NULL,
        stake_after REAL NOT NULL,
        manual_adjustment REAL NOT NULL DEFAULT 0.0,
        earned_reward REAL NOT NULL DEFAULT 0.0
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_msh_hotkey ON miner_stake_history (hotkey)",
    "CREATE INDEX IF NOT EXISTS idx_msh_coldkey ON miner_stake_history (coldkey)",
    "CREATE INDEX IF NOT EXISTS idx_msh_timestamp ON miner_stake_history (timestamp)",
    
    """
    CREATE TABLE IF NOT EXISTS stake_transactions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        hotkey TEXT NOT NULL,
        coldkey TEXT NOT NULL,
        transaction_type TEXT NOT NULL,
        amount REAL NOT NULL,
        timestamp TIMESTAMP NOT NULL,
        block_number INTEGER NOT NULL,
        extrinsic_index TEXT,
        status TEXT NOT NULL DEFAULT 'success',
        details TEXT
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_st_hotkey ON stake_transactions (hotkey)",
    "CREATE INDEX IF NOT EXISTS idx_st_coldkey ON stake_transactions (coldkey)",
    "CREATE INDEX IF NOT EXISTS idx_st_timestamp ON stake_transactions (timestamp)",
    "CREATE INDEX IF NOT EXISTS idx_st_transaction_type ON stake_transactions (transaction_type)",
    
    """
    CREATE TABLE IF NOT EXISTS vesting_hotkey_coldkey (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        hotkey TEXT NOT NULL UNIQUE,
        coldkey TEXT NOT NULL,
        last_updated TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_vhc_coldkey ON vesting_hotkey_coldkey (coldkey)",
    
    """
    CREATE TABLE IF NOT EXISTS vesting_schedules (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        hotkey TEXT NOT NULL,
        coldkey TEXT NOT NULL,
        total_amount REAL NOT NULL,
        start_date TIMESTAMP NOT NULL,
        end_date TIMESTAMP NOT NULL,
        duration_days INTEGER NOT NULL,
        interval_days INTEGER NOT NULL,
        amount_per_interval REAL NOT NULL,
        distributed_amount REAL NOT NULL DEFAULT 0.0,
        status TEXT NOT NULL DEFAULT 'active'
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_vs_hotkey ON vesting_schedules (hotkey)",
    "CREATE INDEX IF NOT EXISTS idx_vs_coldkey ON vesting_schedules (coldkey)",
    "CREATE INDEX IF NOT EXISTS idx_vs_status ON vesting_schedules (status)",
    
    """
    CREATE TABLE IF NOT EXISTS vesting_payments (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        schedule_id INTEGER NOT NULL,
        hotkey TEXT NOT NULL,
        coldkey TEXT NOT NULL,
        amount REAL NOT NULL,
        payment_date TIMESTAMP NOT NULL,
        status TEXT NOT NULL DEFAULT 'scheduled'
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_vp_schedule_id ON vesting_payments (schedule_id)",
    "CREATE INDEX IF NOT EXISTS idx_vp_hotkey ON vesting_payments (hotkey)",
    "CREATE INDEX IF NOT EXISTS idx_vp_coldkey ON vesting_payments (coldkey)",
    "CREATE INDEX IF NOT EXISTS idx_vp_payment_date ON vesting_payments (payment_date)",
    "CREATE INDEX IF NOT EXISTS idx_vp_status ON vesting_payments (status)"
] 