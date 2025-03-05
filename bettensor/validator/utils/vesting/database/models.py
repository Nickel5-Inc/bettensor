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
from sqlalchemy.types import TypeDecorator

import bittensor as bt

Base = declarative_base()


class BalanceType(TypeDecorator):
    """SQLAlchemy type for bittensor.Balance objects"""
    impl = Float
    
    def process_bind_param(self, value, dialect):
        """Convert Balance to float when storing in database"""
        if value is None:
            return None
        if isinstance(value, bt.Balance):
            return value.tao
        return float(value)
        
    def process_result_value(self, value, dialect):
        """Convert float to Balance when loading from database"""
        if value is None:
            return None
        return bt.Balance.from_tao(value)


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
    stake = Column(BalanceType, default=0.0)
    
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
    transaction_type = Column(String(32), index=True)  # "add_stake", "remove_stake", "reward", "penalty"
    amount = Column(BalanceType)
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
    initial_amount = Column(BalanceType)
    remaining_amount = Column(BalanceType)
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
    amount = Column(BalanceType)
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


class EpochEmissions(Base):
    """Record of emissions earned by miners per epoch."""
    
    __tablename__ = "vesting_epoch_emissions"
    
    id = Column(Integer, primary_key=True)
    hotkey = Column(String(255), index=True)
    coldkey = Column(String(255), index=True)
    epoch = Column(Integer, index=True)
    timestamp = Column(DateTime, default=datetime.utcnow, index=True)
    emission_amount = Column(BalanceType, default=0.0)  # Total emissions earned in this epoch
    retained_amount = Column(BalanceType, default=0.0)  # Current amount still retained (updated as stake changes)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "id": self.id,
            "hotkey": self.hotkey,
            "coldkey": self.coldkey,
            "epoch": self.epoch,
            "timestamp": self.timestamp,
            "emission_amount": self.emission_amount,
            "retained_amount": self.retained_amount
        }


class StakeMinimumRequirement(Base):
    """Record of minimum stake requirements by hotkey."""
    
    __tablename__ = "vesting_minimum_requirements"
    
    id = Column(Integer, primary_key=True)
    hotkey = Column(String(255), index=True, unique=True)
    coldkey = Column(String(255), index=True)
    minimum_stake = Column(BalanceType, default=0.3)  # Default minimum of 0.3 TAO equivalent
    last_updated = Column(DateTime, default=datetime.utcnow)
    is_meeting_requirement = Column(Boolean, default=False)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "id": self.id,
            "hotkey": self.hotkey,
            "coldkey": self.coldkey,
            "minimum_stake": self.minimum_stake,
            "last_updated": self.last_updated,
            "is_meeting_requirement": self.is_meeting_requirement
        } 