"""
Database models for the vesting system.

This module defines all database models used by the vesting system,
including stake history, transactions, vesting schedules, and payments.
"""

from datetime import datetime
from typing import Dict, Any, Optional

from sqlalchemy import Column, Integer, Float, String, DateTime, ForeignKey, Boolean, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

Base = declarative_base()


class HotkeyColdkeyAssociation(Base):
    """Association between hotkeys and coldkeys."""
    
    __tablename__ = "vesting_hotkey_coldkey_associations"
    
    id = Column(Integer, primary_key=True)
    hotkey = Column(String(255), index=True, unique=True)
    coldkey = Column(String(255), index=True)
    last_updated = Column(DateTime, default=datetime.utcnow)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "hotkey": self.hotkey,
            "coldkey": self.coldkey,
            "last_updated": self.last_updated
        }


class StakeHistory(Base):
    """Record of stake balance history for miners."""
    
    __tablename__ = "vesting_stake_history"
    
    id = Column(Integer, primary_key=True)
    hotkey = Column(String(255), index=True)
    coldkey = Column(String(255), index=True)
    timestamp = Column(DateTime, default=datetime.utcnow, index=True)
    stake = Column(Float, default=0.0)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "hotkey": self.hotkey,
            "coldkey": self.coldkey,
            "timestamp": self.timestamp,
            "stake": self.stake
        }


class StakeTransaction(Base):
    """Record of stake transactions on the blockchain."""
    
    __tablename__ = "vesting_stake_transactions"
    
    id = Column(Integer, primary_key=True)
    extrinsic_hash = Column(String(255), index=True, unique=True)
    block_number = Column(Integer, index=True)
    block_timestamp = Column(DateTime, index=True)
    hotkey = Column(String(255), index=True)
    coldkey = Column(String(255), index=True)
    transaction_type = Column(String(32), index=True)  # "add_stake", "remove_stake", "move_stake"
    amount = Column(Float)
    source_hotkey = Column(String(255), nullable=True)  # For move_stake only
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "extrinsic_hash": self.extrinsic_hash,
            "block_number": self.block_number,
            "block_timestamp": self.block_timestamp,
            "hotkey": self.hotkey,
            "coldkey": self.coldkey,
            "transaction_type": self.transaction_type,
            "amount": self.amount,
            "source_hotkey": self.source_hotkey
        }


class VestingSchedule(Base):
    """Schedule for vesting reward payments."""
    
    __tablename__ = "vesting_schedules"
    
    id = Column(Integer, primary_key=True)
    hotkey = Column(String(255), index=True)
    coldkey = Column(String(255), index=True)
    start_time = Column(DateTime, index=True)
    end_time = Column(DateTime, index=True)
    initial_amount = Column(Float)
    remaining_amount = Column(Float)
    interval_days = Column(Integer)
    status = Column(String(32), default="active")  # "active", "completed", "cancelled"
    
    # Relationship with payments
    payments = relationship("VestingPayment", back_populates="schedule")
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "id": self.id,
            "hotkey": self.hotkey,
            "coldkey": self.coldkey,
            "start_time": self.start_time,
            "end_time": self.end_time,
            "initial_amount": self.initial_amount,
            "remaining_amount": self.remaining_amount,
            "interval_days": self.interval_days,
            "status": self.status
        }


class VestingPayment(Base):
    """Individual payment from a vesting schedule."""
    
    __tablename__ = "vesting_payments"
    
    id = Column(Integer, primary_key=True)
    schedule_id = Column(Integer, ForeignKey("vesting_schedules.id"), index=True)
    payment_time = Column(DateTime, index=True)
    amount = Column(Float)
    status = Column(String(32), default="scheduled")  # "scheduled", "paid"
    
    # Relationship with schedule
    schedule = relationship("VestingSchedule", back_populates="payments")
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "id": self.id,
            "schedule_id": self.schedule_id,
            "payment_time": self.payment_time,
            "amount": self.amount,
            "status": self.status
        } 