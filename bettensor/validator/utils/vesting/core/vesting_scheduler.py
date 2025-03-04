"""
Vesting schedule management for the vesting rewards system.

This module provides functionality to create and manage vesting schedules
for miner rewards.
"""

import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any

from sqlalchemy.orm import Session
from sqlalchemy import func, and_

from bettensor.validator.utils.database import DatabaseManager
from bettensor.validator.utils.vesting.database.models import VestingSchedule, VestingPayment

logger = logging.getLogger(__name__)


class VestingScheduler:
    """
    Manages vesting schedules and payments for miners.
    
    This class is responsible for:
    1. Creating vesting schedules for earned rewards
    2. Processing scheduled vesting payments
    3. Tracking vesting status for miners
    """
    
    def __init__(
        self,
        db_manager: DatabaseManager,
        vesting_duration_days: int = 30,
        vesting_interval_days: int = 1,
        min_vesting_amount: float = 0.1
    ):
        """
        Initialize the VestingScheduler.
        
        Args:
            db_manager: Database manager for persistent storage
            vesting_duration_days: Duration of vesting schedules in days
            vesting_interval_days: Interval between vesting payments in days
            min_vesting_amount: Minimum amount for creating a vesting schedule
        """
        self.db_manager = db_manager
        self.vesting_duration_days = vesting_duration_days
        self.vesting_interval_days = vesting_interval_days
        self.min_vesting_amount = min_vesting_amount
    
    async def create_vesting_schedule(
        self,
        hotkey: str,
        coldkey: str,
        amount: float,
        start_time: Optional[datetime] = None
    ) -> Optional[int]:
        """
        Create a new vesting schedule for a miner.
        
        Args:
            hotkey: Miner's hotkey
            coldkey: Miner's coldkey
            amount: Amount to vest
            start_time: Start time for vesting (defaults to now)
            
        Returns:
            The ID of the created schedule or None if creation failed
        """
        if amount < self.min_vesting_amount:
            logger.info(f"Amount {amount} is below minimum vesting amount {self.min_vesting_amount}")
            return None
            
        if start_time is None:
            start_time = datetime.utcnow()
            
        try:
            # Calculate end time
            end_time = start_time + timedelta(days=self.vesting_duration_days)
            
            # Calculate number of payments
            num_payments = self.vesting_duration_days // self.vesting_interval_days
            if self.vesting_duration_days % self.vesting_interval_days != 0:
                num_payments += 1  # Add one more payment for remainder
                
            # Calculate payment amount
            payment_amount = amount / num_payments
            
            async with self.db_manager.async_session() as session:
                # Create vesting schedule
                schedule = VestingSchedule(
                    hotkey=hotkey,
                    coldkey=coldkey,
                    start_time=start_time,
                    end_time=end_time,
                    initial_amount=amount,
                    remaining_amount=amount,
                    interval_days=self.vesting_interval_days,
                    status="active"
                )
                
                session.add(schedule)
                await session.flush()  # Flush to get ID
                
                # Create vesting payments
                for i in range(num_payments):
                    payment_time = start_time + timedelta(days=i * self.vesting_interval_days)
                    
                    # Last payment gets remainder to avoid rounding issues
                    if i == num_payments - 1:
                        payment = VestingPayment(
                            schedule_id=schedule.id,
                            payment_time=payment_time,
                            amount=schedule.remaining_amount,
                            status="scheduled"
                        )
                    else:
                        payment = VestingPayment(
                            schedule_id=schedule.id,
                            payment_time=payment_time,
                            amount=payment_amount,
                            status="scheduled"
                        )
                    
                    session.add(payment)
                
                await session.commit()
                
                logger.info(
                    f"Created vesting schedule for {hotkey}: "
                    f"{amount} TAO over {self.vesting_duration_days} days "
                    f"with {num_payments} payments"
                )
                
                return schedule.id
                
        except Exception as e:
            logger.error(f"Error creating vesting schedule: {e}")
            return None
    
    async def process_vesting_payments(self) -> int:
        """
        Process due vesting payments.
        
        Returns:
            Number of payments processed
        """
        try:
            now = datetime.utcnow()
            payments_processed = 0
            
            async with self.db_manager.async_session() as session:
                # Get due payments
                due_payments = await session.execute(
                    session.query(VestingPayment)
                    .filter(
                        and_(
                            VestingPayment.status == "scheduled",
                            VestingPayment.payment_time <= now
                        )
                    )
                    .order_by(VestingPayment.payment_time)
                )
                due_payments = due_payments.all()
                
                for payment in due_payments:
                    # Get schedule
                    schedule = await session.get(VestingSchedule, payment.schedule_id)
                    
                    if schedule and schedule.status == "active":
                        # Update payment status
                        payment.status = "paid"
                        
                        # Update schedule remaining amount
                        schedule.remaining_amount -= payment.amount
                        
                        # If no remaining amount, mark schedule as completed
                        if schedule.remaining_amount <= 0:
                            schedule.status = "completed"
                            
                        logger.info(
                            f"Processed vesting payment for {schedule.hotkey}: "
                            f"{payment.amount} TAO"
                        )
                        
                        payments_processed += 1
                
                # Commit changes
                if payments_processed > 0:
                    await session.commit()
                    logger.info(f"Processed {payments_processed} vesting payments")
            
            return payments_processed
            
        except Exception as e:
            logger.error(f"Error processing vesting payments: {e}")
            return 0
    
    async def get_vesting_summary(self, hotkey: str) -> Dict[str, Any]:
        """
        Get vesting summary for a hotkey.
        
        Args:
            hotkey: Miner's hotkey
            
        Returns:
            Dictionary with vesting summary
        """
        try:
            summary = {
                "hotkey": hotkey,
                "total_vesting": 0.0,
                "total_paid": 0.0,
                "total_remaining": 0.0,
                "num_active_schedules": 0,
                "next_payment_time": None,
                "next_payment_amount": 0.0
            }
            
            async with self.db_manager.async_session() as session:
                # Get active schedules
                active_schedules = await session.execute(
                    session.query(VestingSchedule)
                    .filter(
                        and_(
                            VestingSchedule.hotkey == hotkey,
                            VestingSchedule.status == "active"
                        )
                    )
                )
                active_schedules = active_schedules.all()
                
                summary["num_active_schedules"] = len(active_schedules)
                
                for schedule in active_schedules:
                    summary["total_vesting"] += schedule.initial_amount
                    summary["total_remaining"] += schedule.remaining_amount
                
                summary["total_paid"] = summary["total_vesting"] - summary["total_remaining"]
                
                # Get next payment
                next_payment = await session.execute(
                    session.query(VestingPayment)
                    .join(VestingSchedule)
                    .filter(
                        and_(
                            VestingSchedule.hotkey == hotkey,
                            VestingSchedule.status == "active",
                            VestingPayment.status == "scheduled"
                        )
                    )
                    .order_by(VestingPayment.payment_time)
                    .limit(1)
                )
                next_payment = next_payment.first()
                
                if next_payment:
                    summary["next_payment_time"] = next_payment.payment_time
                    summary["next_payment_amount"] = next_payment.amount
            
            return summary
            
        except Exception as e:
            logger.error(f"Error getting vesting summary: {e}")
            return {
                "hotkey": hotkey,
                "error": str(e)
            } 