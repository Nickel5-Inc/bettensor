"""
BetTensor Scoring Module. 

This module handles the scoring of miners based on their data. The scoring is intended to select for miners that deploy positive expected value strategies, with some degree of risk management. 
We mostly determine +EV through closing line value analysis.

Inputs: 
- Miner Predictions

Outputs: 
- A NumPy array of the composite scores for all miners, indexed by miner_uid. 
"""


from collections import defaultdict
import json
import numpy as np
import bittensor as bt
from datetime import datetime, timezone, timedelta, date
from typing import List, Dict, Optional
import time
import traceback
import asyncio
import requests
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
import async_timeout
import random
import copy
import scipy.special

from bettensor.validator.utils.database.database_manager import DatabaseManager
from .scoring_data import ScoringData
from .entropy_system import EntropySystem

class ScoringSystem:
    def __init__(
        self,
        db_manager: DatabaseManager,
        num_miners: int = 256,
        max_days: int = 45,
        current_date: Optional[datetime] = None,
        force_rebuild: bool = False,
        min_stake_service = None,
    ):
        """
        Initialize the ScoringSystem.
        
        Args:
            db_manager: Database manager instance for executing queries
            num_miners (int): Number of miners in the system
            max_days (int, optional): Maximum number of days to track. Defaults to 45.
            current_date (datetime, optional): Current date to use. Defaults to None (uses current UTC date).
            force_rebuild (bool, optional): Whether to force a rebuild of historical scores. Defaults to False.
            min_stake_service (optional): Service for checking minimum stake requirements. Defaults to None.
        """
        # Common attributes
        self.db_manager = db_manager
        self.num_miners = num_miners
        self.max_days = max_days
        self.force_rebuild = force_rebuild
        self.validator = None
        self.miner_data = None
        self.min_stake_service = min_stake_service  # Ensure this is set
        
        # Set current date
        if current_date is None:
            current_date = datetime.now(timezone.utc)
        self.current_date = current_date.replace(
            hour=0, minute=0, second=0, microsecond=0, tzinfo=timezone.utc
        )
        self.current_day = self.current_date.timetuple().tm_yday % self.max_days

        # Initialize arrays
        self.clv_scores = np.zeros((num_miners, max_days), dtype=np.float32)
        self.roi_scores = np.zeros((num_miners, max_days), dtype=np.float32)
        self.sortino_scores = np.zeros((num_miners, max_days), dtype=np.float32)
        self.entropy_scores = np.zeros((num_miners, max_days), dtype=np.float32)
        self.composite_scores = np.zeros((num_miners, max_days, 6), dtype=np.float32)  # 6 = daily + 5 tiers
        self.amount_wagered = np.zeros((num_miners, max_days), dtype=np.float32)
        self.tiers = np.ones((num_miners, max_days), dtype=np.int32)

        # Scoring weights
        self.clv_weight = 0.3
        self.roi_weight = 0.3
        self.sortino_weight = 0.3
        self.entropy_weight = 0.1
        self.entropy_window = self.max_days

        # Additional attributes from both init methods
        self.entropy_system = EntropySystem(num_miners, max_days, db_manager=db_manager)
        self.incentives = []
        self.last_update_date = None
        self.scoring_data = None
        
        self.num_tiers = 7  # 5 tiers + 2 for invalid UIDs (0) and empty network slots (-1)
        self.valid_uids = set()
        self.reference_date = datetime(year=2024, month=9, day=30, tzinfo=timezone.utc)
        self.invalid_uids = []
        self.epsilon = 1e-8  # Small constant to prevent division by zero

        # Initialize tier configurations
        self.tier_configs = [
            {
                "window": 0,
                "min_wager": 0,
                "capacity": int(num_miners * 1),
                "incentive": 0,
            },  # Tier -1 for empty slots
            {
                "window": 0,
                "min_wager": 0,
                "capacity": int(num_miners * 1),
                "incentive": 0,
            },  # Tier 0 for invalid UIDs
            {
                "window": 3,
                "min_wager": 0,
                "capacity": int(num_miners * 1.0),
                "incentive": 0.02,
            },  # Tier 1
            {
                "window": 7,
                "min_wager": 4000,
                "capacity": int(num_miners * 0.2),
                "incentive": 0.05,
            },  # Tier 2
            {
                "window": 15,
                "min_wager": 10000,
                "capacity": int(num_miners * 0.2),
                "incentive": 0.23,
            },  # Tier 3
            {
                "window": 30,
                "min_wager": 20000,
                "capacity": int(num_miners * 0.1),
                "incentive": 0.27,
            },  # Tier 4
            {
                "window": 45,
                "min_wager": 35000,
                "capacity": int(num_miners * 0.05),
                "incentive": 0.43,
            },  # Tier 5
        ]
        
        # Map tier indices to names
        self.tier_mapping = {
            0: "daily",  # Daily score
            1: "tier_1",
            2: "tier_2",
            3: "tier_3",
            4: "tier_4",
            5: "tier_5"
        }

        # numpy max integer setting
        max_int = np.iinfo(np.int64).max
        np.set_printoptions(threshold=max_int)

    def set_validator(self, validator):
        """Set the validator instance and initialize ScoringData."""
        self.validator = validator
        self.scoring_data = ScoringData(self)
        
    async def populate_amount_wagered(self):
        """Populate the amount_wagered array from raw prediction data."""
        bt.logging.info("Populating amount_wagered from prediction data...")
        
        try:
            # Ensure reference date is timezone-aware
            if self.reference_date.tzinfo is None:
                self.reference_date = self.reference_date.replace(tzinfo=timezone.utc)
            if self.current_date.tzinfo is None:
                self.current_date = self.current_date.replace(tzinfo=timezone.utc)
            
            # Get predictions for the last max_days days
            cutoff_date = (self.current_date - timedelta(days=self.max_days)).isoformat()
            query = """
                SELECT 
                    miner_uid,
                    DATE(prediction_date) as pred_date,
                    SUM(wager) as daily_wager,
                    COUNT(*) as num_predictions,
                    COUNT(CASE WHEN payout IS NOT NULL THEN 1 END) as scored_predictions
                FROM predictions 
                WHERE miner_uid < :num_miners 
                AND prediction_date >= :cutoff_date
                GROUP BY miner_uid, DATE(prediction_date)
                ORDER BY miner_uid, pred_date
            """
            
            # Reset amount_wagered array
            self.amount_wagered = np.zeros((self.num_miners, self.max_days))
            
            # Get prediction data
            params = {
                "num_miners": self.num_miners,
                "cutoff_date": cutoff_date
            }
            results = await self.db_manager.fetch_all(query, params)
            bt.logging.info(f"Found {len(results)} days of prediction data")
            
            # Process results and populate array
            for row in results:
                try:
                    miner_uid = row['miner_uid']
                    # Parse date and ensure it's timezone-aware
                    pred_date = datetime.strptime(row['pred_date'], '%Y-%m-%d')
                    pred_date = pred_date.replace(
                        hour=0, minute=0, second=0, microsecond=0, tzinfo=timezone.utc
                    )
                    daily_wager = float(row['daily_wager'])
                    num_predictions = row['num_predictions']
                    scored_predictions = row['scored_predictions']
                    
                    # Calculate day index relative to current_date
                    days_from_current = (self.current_date - pred_date).days
                    if days_from_current < 0 or days_from_current >= self.max_days:
                        continue
                        
                    day_idx = (self.current_day - days_from_current) % self.max_days
                    
                    # Store daily wager
                    self.amount_wagered[miner_uid, day_idx] = daily_wager
                    
                    bt.logging.debug(f"Miner {miner_uid} on {pred_date.date()}: "
                                   f"wager={daily_wager:.2f}, "
                                   f"predictions={num_predictions}, "
                                   f"scored={scored_predictions}")
                        
                except Exception as e:
                    bt.logging.error(f"Error processing row {row}: {str(e)}")
                    continue
            
            # Log summary statistics
            active_miners = np.count_nonzero(np.sum(self.amount_wagered, axis=1))
            total_wager = np.sum(self.amount_wagered)
            bt.logging.info(f"Amount wagered populated from predictions:")
            bt.logging.info(f"- Active miners: {active_miners}")
            bt.logging.info(f"- Total wager: {total_wager:.2f}")
            bt.logging.info(f"- Average wager per active miner: {total_wager/active_miners if active_miners > 0 else 0:.2f}")
            
        except Exception as e:
            bt.logging.error(f"Error populating amount_wagered: {str(e)}")
            bt.logging.error(f"Traceback: {traceback.format_exc()}")
            raise

    def advance_day(self, current_date: date):
        bt.logging.debug(f"Attempting to advance day with current_date: {current_date}, last_update_date: {self.last_update_date}")
        
        if self.last_update_date is None:
            self.last_update_date = current_date
            bt.logging.info(f"Set last_update_date to {self.last_update_date} without advancing day.")
            return

        days_passed = (current_date - self.last_update_date).days
        bt.logging.debug(f"Days passed since last update: {days_passed}")

        if days_passed > 0:
            for day in range(1, days_passed + 1):
                # Advance to the next day in the circular buffer
                previous_day = self.current_day
                self.current_day = (self.current_day + 1) % self.max_days
                bt.logging.debug(f"Advancing to day_id={self.current_day}")

                # Copy previous day's cumulative values instead of resetting
                self.amount_wagered[:, self.current_day] = self.amount_wagered[:, previous_day]
                bt.logging.debug(f"Copied amount_wagered from day_id={previous_day} to day_id={self.current_day}")

                # Carry over tier information from the previous day
                self.tiers[:, self.current_day] = self.tiers[:, previous_day]
                bt.logging.debug(f"Copied tiers from day_id={previous_day} to day_id={self.current_day}")

            # Update the last_update_date and current_date
            self.last_update_date = current_date
            self.current_date = datetime.combine(current_date, datetime.min.time(), tzinfo=timezone.utc)
            bt.logging.info(f"Advanced {days_passed} day(s). New current_day index: {self.current_day}")

            if days_passed > 1:
                self.handle_downtime(days_passed)
        else:
            bt.logging.info("No new day to advance. current_day remains unchanged.")

    def handle_downtime(self, days_passed):
        bt.logging.warning(
            f"System was down for {days_passed - 1} day(s). Adjusting scores..."
        )

        # For each day of downtime, we'll copy the last known scores and tiers
        for i in range(1, days_passed):
            prev_day = (self.current_day - days_passed + i) % self.max_days
            current_day = (self.current_day - days_passed + i + 1) % self.max_days

            self.clv_scores[:, current_day] = self.clv_scores[:, prev_day]
            self.roi_scores[:, current_day] = self.roi_scores[:, prev_day]
            self.entropy_scores[:, current_day] = self.entropy_scores[:, prev_day]
            self.composite_scores[:, current_day] = self.composite_scores[:, prev_day]
            self.tiers[:, current_day] = self.tiers[:, prev_day]

        bt.logging.info("Downtime handling complete.")

    async def update_scores(self, predictions, closing_line_odds, results):
        """Update the scores for the current day based on predictions, closing line odds, and results."""
        bt.logging.info(f"Updating scores for day {self.current_day}")
        try:
            if predictions.size > 0 and closing_line_odds.size > 0 and results.size > 0:
                self._update_raw_scores(predictions, closing_line_odds, results)
                # Pass the current_day to _update_composite_scores
                await self._update_composite_scores(self.current_day)
                self.log_score_summary()
            else:
                bt.logging.warning("No data available for score update.")
        except Exception as e:
            bt.logging.error(f"Error updating scores: {str(e)}")
            raise

    def _update_raw_scores(self, predictions, closing_line_odds, results):
        bt.logging.trace(f"Predictions shape: {predictions.shape}")
        bt.logging.trace(f"Closing line odds shape: {closing_line_odds.shape}")
        bt.logging.trace(f"Results shape: {results.shape}")

        # Extract unique external_ids from predictions
        external_ids = np.unique(predictions[:, 1]).astype(int).tolist()
        bt.logging.debug(f"Unique external_ids: {external_ids}")

        # Calculate CLV scores for all predictions
        clv_scores = self._calculate_clv_scores(predictions, closing_line_odds)
        self.clv_scores[:, self.current_day] = clv_scores

        # Calculate ROI scores for all predictions
        roi_scores = self._calculate_roi_scores(predictions, results)
        self.roi_scores[:, self.current_day] = roi_scores

        # Calculate Sortino scores for all predictions
        sortino_scores = self._calculate_risk_scores(predictions, results)
        self.sortino_scores[:, self.current_day] = sortino_scores

        # Reset daily wagers for all miners
        daily_wagers = np.zeros(self.num_miners)

        # Group predictions by miner_id and sum their wagers
        miner_predictions = defaultdict(list)
        for pred in predictions:
            miner_id = int(pred[0])
            miner_predictions[miner_id].append(pred)

        # Process wagers for each miner
        for miner_id, miner_preds in miner_predictions.items():
            daily_wager = 0.0
            for pred in miner_preds: 
                try:
                    wager = float(pred[5])
                    daily_wager += wager
                except (IndexError, ValueError) as e:
                    bt.logging.error(f"Error extracting wager for miner {miner_id}: {e}")
            daily_wagers[miner_id] = daily_wager

        # Update amount_wagered array with daily wagers
        self.amount_wagered[:, self.current_day] = daily_wagers

        bt.logging.info(f"Updated daily wagers for day {self.current_day}")
        bt.logging.info(f"Total daily wager: {np.sum(daily_wagers):.2f}")
        bt.logging.info(f"Number of miners with wagers: {np.count_nonzero(daily_wagers)}")
        bt.logging.debug(f"Daily wager summary - min: {daily_wagers.min():.2f}, "
                        f"max: {daily_wagers.max():.2f}, "
                        f"mean: {daily_wagers.mean():.2f}")

        # Update entropy scores - now returns full array
        entropy_scores = self.entropy_system.get_current_ebdr_scores(
            self.current_date, self.current_day, external_ids
        )
        # Update entire entropy scores array
        self.entropy_scores = entropy_scores

        bt.logging.info(
            f"Entropy scores for current day - "
            f"min: {entropy_scores[:, self.current_day].min():.8f}, "
            f"max: {entropy_scores[:, self.current_day].max():.8f}, "
            f"mean: {entropy_scores[:, self.current_day].mean():.8f}"
        )

    def _meets_tier_requirements(self, miner, tier):
        """
        Check if a miner meets the requirements for a given tier.
        
        Args:
            miner (int): The miner's UID
            tier (int): The tier to check requirements for
            
        Returns:
            bool: True if miner meets requirements, False otherwise
        """
        if tier <= 1:  # Tier 0 and 1 have no requirements
            return True
            
        config = self.tier_configs[tier]
        window = config["window"]
        min_wager = config["min_wager"]
        
        # Default to requiring activity for half the window if min_active_days not specified
        min_active_days_ratio = config.get("min_active_days", 0.5)
        min_days_required = max(1, int(window * min_active_days_ratio))
        
        # Get daily wagers for the window period
        start_idx = (self.current_day - window + 1) % self.max_days
        if start_idx <= self.current_day:
            window_wagers = self.amount_wagered[miner, start_idx:self.current_day + 1]
        else:
            # Handle wraparound case
            window_wagers = np.concatenate([
                self.amount_wagered[miner, start_idx:],
                self.amount_wagered[miner, :self.current_day + 1]
            ])
        
        # Calculate cumulative wager and active days
        cumulative_wager = np.sum(window_wagers)
        active_wager_days = np.count_nonzero(window_wagers)
        
        # Get daily scores for the window period
        if start_idx <= self.current_day:
            window_scores = self.composite_scores[miner, start_idx:self.current_day + 1]
        else:
            window_scores = np.concatenate([
                self.composite_scores[miner, start_idx:],
                self.composite_scores[miner, :self.current_day + 1]
            ])
        active_days = np.count_nonzero(~np.isnan(window_scores))
        
        # Log tier configuration
        bt.logging.trace(f"Checking tier {tier-1} requirements for miner {miner}:")
        bt.logging.trace(f"Window size: {window} days")
        bt.logging.trace(f"Minimum wager required: {min_wager}")
        bt.logging.trace(f"Minimum active days required: {min_days_required} ({min_active_days_ratio*100:.0f}% of window)")
        
        # Log wager details
        bt.logging.trace("Wager details:")
        bt.logging.trace(f"Cumulative wager: {cumulative_wager:.2f}/{min_wager}")
        bt.logging.trace(f"Days with wagers: {active_wager_days}/{window}")
        if active_wager_days > 0:
            bt.logging.trace(f"Average daily wager: {cumulative_wager/active_wager_days:.2f}")
            bt.logging.trace(f"Daily wagers: {[f'{float(w):.2f}' for w in window_wagers]}")

        # Log activity details
        bt.logging.trace("Activity details:")
        bt.logging.trace(f"Days with scores: {active_days}/{window}")
        bt.logging.trace(f"Required active days: {min_days_required}")
        if active_days > 0:
            # Convert scores to float and handle NaN values
            score_strs = []
            for s in window_scores:
                if isinstance(s, np.ndarray):
                    # If it's a composite score array, take the mean
                    s_val = float(np.nanmean(s))
                else:
                    s_val = float(s)
                if np.isnan(s_val):
                    score_strs.append("NaN")
                else:
                    score_strs.append(f"{s_val:.4f}")
            bt.logging.trace(f"Daily scores: {score_strs}")
        
        meets_wager = cumulative_wager >= min_wager
        has_history = active_days >= min_days_required
        meets_requirement = meets_wager and has_history
        
        # Log eligibility result with specific reasons
        bt.logging.trace("Eligibility result:")
        bt.logging.trace(f"Meets wager requirement: {meets_wager}")
        if not meets_wager:
            bt.logging.trace(f"- Needs {min_wager - cumulative_wager:.2f} more in wagers")
        bt.logging.trace(f"Meets history requirement: {has_history}")
        if not has_history:
            bt.logging.trace(f"- Needs {min_days_required - active_days} more active days")
        
        return meets_requirement

    async def manage_tiers(self, invalid_uids, valid_uids):  # Add async
        bt.logging.info("Managing tiers")

        try:
            current_tiers = self.tiers[:, self.current_day].copy()
            composite_scores_day = self.composite_scores[:, self.current_day, :]
            
            bt.logging.debug(f"Composite Scores Shape: {self.composite_scores.shape}")
            bt.logging.info(f"Current tiers before management: {np.bincount(current_tiers, minlength=self.num_tiers)}")

            # Step 1: Check for and perform demotions (top-down)
            for tier in range(self.num_tiers - 1, 1, -1):
                tier_miners = np.where(current_tiers == tier)[0]
                for miner in tier_miners:
                    if not self._meets_tier_requirements(miner, tier):
                        self._cascade_demotion(miner, tier, current_tiers, composite_scores_day)

            # Step 2: Promote and swap (using our new top-down implementation)
            await self._promote_and_swap(current_tiers, composite_scores_day, valid_uids)  # Add valid_uids parameter

            # Update tiers for the current day
            self.tiers[:, self.current_day] = current_tiers

            # Set invalid UIDs to tier 0
            self.tiers[list(invalid_uids), self.current_day] = 0

            # Log the final state AFTER invalid UIDs are set to tier 0
            final_tier_counts_bincount = np.bincount(self.tiers[:, self.current_day], minlength=self.num_tiers)
            bt.logging.info(f"Current tiers after management (incl. invalid): {final_tier_counts_bincount}")
            
            # Optional: Log distribution excluding Tier 0 if desired
            final_tier_counts_dist = [int(np.sum(self.tiers[:, self.current_day] == t)) for t in range(1, self.num_tiers)]
            bt.logging.info(f"Final tier distribution (Tiers 1+): {final_tier_counts_dist}")
            
            bt.logging.info("Tier management completed")

        except Exception as e:
            bt.logging.error(f"Error managing tiers: {str(e)}")
            raise

    async def _promote_and_swap(self, tiers, composite_scores_day, valid_uids):  # Add async and valid_uids
        """
        Promote miners to higher tiers and perform swaps when necessary, ensuring min_wager is respected.
        Working top-down to optimize tier distribution.
        """
        for tier in range(self.num_tiers - 1, 1, -1):  # Start from highest tier (5) down to tier 2
            current_tier_miners = np.where(tiers == tier)[0]
            # Only consider valid miners from tier 2 and above
            lower_tier_miners = np.where((tiers <= tier - 1) & np.isin(np.arange(len(tiers)), list(valid_uids)))[0]
            # Calculate open slots based on tier capacity and current occupancy
            tier_capacity = self.tier_configs[tier]["capacity"]
            current_occupancy = len(current_tier_miners)
            open_slots = max(0, tier_capacity - current_occupancy)
            eligible_miners = [
                miner for miner in lower_tier_miners
                if self._meets_tier_requirements(miner, tier) and miner in valid_uids
            ]
            bt.logging.debug(
                # Corrected log message to reflect the tier being checked for promotions *into*
                f"Checking eligibility for Tier {tier}: {len(eligible_miners)} miners in lower tiers eligible for {open_slots} open slots"
            )

            # Sort eligible miners by composite scores descending
            eligible_miners_sorted = sorted(
                eligible_miners,
                key=lambda x: composite_scores_day[x, tier - 1],
                reverse=True
            )
            if eligible_miners:
                if open_slots > 0:

                    # Promote the best miners to fill open slots
                    promotions = eligible_miners_sorted[:open_slots]
                    for miner in promotions:
                        tiers[miner] = tier
                        bt.logging.info(f"Miner {miner} promoted to tier {tier-1}")

                else:
                    # If tier is full, consider swaps with lower tier miners
                    # Sort current tier miners by score ascending (worst first)
                    current_sorted = sorted(
                        current_tier_miners,
                        key=lambda x: composite_scores_day[x, tier - 1],
                        reverse=False
                    )
                    
                    # Check each potential swap
                    for lower_miner in eligible_miners_sorted:
                            
                        # Compare with worst performing miner in current tier
                        for current_miner in current_sorted:
                            lower_score = composite_scores_day[lower_miner, tier - 1]
                            current_score = composite_scores_day[current_miner, tier - 1]

                            if lower_score > current_score:
                                # Swap tiers
                                lower_tier = tiers[lower_miner]
                                tiers[lower_miner], tiers[current_miner] = tier, lower_tier
                                bt.logging.info(
                                    f"Swapped miner {lower_miner} (↑tier {tier}) with "
                                    f"miner {current_miner} (↓tier {lower_tier})"
                                )
                                # Update sorted lists
                                current_sorted.remove(current_miner)
                                break
                            else:
                                # If best lower tier miner can't beat worst current tier,
                                # no need to check others
                                break

        # Log final tier distribution
        tier_counts = [np.sum(tiers == t) for t in range(1, self.num_tiers)]
        bt.logging.info(f"Final tier distribution: {tier_counts}")

   

    def _get_cumulative_wager(self, miner, window):
        """
        Get the cumulative wager for a miner by summing up daily wagers over the window period.
        
        Args:
            miner (int): The miner's UID
            window (int): Number of days to look back
            
        Returns:
            float: Total cumulative wager amount
        """
        # Calculate indices for the window, handling wraparound
        start_idx = (self.current_day - window + 1) % self.max_days
        if start_idx <= self.current_day:
            wager = np.sum(self.amount_wagered[miner, start_idx:self.current_day + 1])
        else:
            # Handle wraparound case
            wager = np.sum(self.amount_wagered[miner, start_idx:]) + np.sum(self.amount_wagered[miner, :self.current_day + 1])
        
        bt.logging.debug(
            f"Miner {miner} cumulative wager over last {window} days: {wager:.2f} "
            f"(current day {self.current_day}, window start {start_idx})"
        )
        return wager

    def _cascade_demotion(self, miner, current_tier, tiers, composite_scores):
        """
        Demote a miner from the current tier to the next lower tier without violating tier boundaries.

        Args:
            miner (int): The miner's UID.
            current_tier (int): The current tier of the miner.
            tiers (np.ndarray): The array representing current tiers of all miners.
            composite_scores (np.ndarray): The composite scores array for the current day.
        """
        bt.logging.debug(f"Demoting miner {miner} from tier {current_tier-1}")

        # Determine if the miner is valid
        is_valid_miner = miner in self.valid_uids

        # Calculate new tier
        new_tier = current_tier - 1

        if is_valid_miner:
            # Ensure valid miners are not demoted below tier 2
            new_tier = max(new_tier, 2)
        else:
            # Invalid miners can be demoted to tier 1 or 0
            new_tier = max(new_tier, 1)

        tiers[miner] = new_tier
        bt.logging.info(f"Miner {miner} demoted to tier {new_tier - 1}")

        # Recursively check if further demotion is needed
        if not self._meets_tier_requirements(miner, new_tier):
            self._cascade_demotion(miner, new_tier, tiers, composite_scores)

    async def reset_miner(self, miner_uid):
        """
        Completely reset a miner's stats and predictions.
        This is called when a hotkey changes UIDs or when a miner needs to be reset.
        """
        try:
            bt.logging.info(f"Performing complete reset for miner {miner_uid}")
            
            queries = [
                # Clear all stats for the miner
                ("""UPDATE miner_stats 
                    SET miner_hotkey = NULL,
                        miner_coldkey = NULL,
                        miner_rank = NULL,
                        miner_status = NULL,
                        miner_cash = 0,
                        miner_current_incentive = 0,
                        miner_current_tier = 1,
                        miner_current_scoring_window = 0,
                        miner_current_composite_score = NULL,
                        miner_current_sharpe_ratio = NULL,
                        miner_current_sortino_ratio = NULL,
                        miner_current_roi = NULL,
                        miner_current_clv_avg = NULL,
                        miner_last_prediction_date = NULL,
                        miner_lifetime_earnings = 0,
                        miner_lifetime_wager_amount = 0,
                        miner_lifetime_roi = 0,
                        miner_lifetime_predictions = 0,
                        miner_lifetime_wins = 0,
                        miner_lifetime_losses = 0,
                        miner_win_loss_ratio = 0
                    WHERE miner_uid = ?""", 
                 (miner_uid,)),
                 
                # Delete all predictions for this miner
                ("""DELETE FROM predictions 
                    WHERE miner_uid = ?""",
                 (miner_uid,)),
                 
                # Delete all scores for this miner
                ("""DELETE FROM scores 
                    WHERE miner_uid = ?""",
                 (miner_uid,)),
                 
                # Delete entropy predictions for this miner
                ("""DELETE FROM entropy_predictions 
                    WHERE miner_uid = ?""",
                 (miner_uid,)),
                 
                # Delete entropy miner scores for this miner
                ("""DELETE FROM entropy_miner_scores 
                    WHERE miner_uid = ?""",
                 (miner_uid,)),
            ]
            
            for query, params in queries:
                for attempt in range(3):  # Try each query up to 3 times
                    try:
                        await self.db_manager.execute_query(query, params)
                        break
                    except TimeoutError:
                        if attempt == 2:  # Last attempt
                            bt.logging.error(f"Failed to reset miner {miner_uid} after 3 attempts")
                            raise
                        time.sleep(1)  # Wait before retry
            
            # Clear all in-memory data for this miner
            self.composite_scores[miner_uid] = 0
            self.clv_scores[miner_uid] = 0
            self.roi_scores[miner_uid] = 0
            self.sortino_scores[miner_uid] = 0
            self.entropy_scores[miner_uid] = 0
            self.amount_wagered[miner_uid] = 0
            self.tiers[miner_uid] = 1  # Reset to tier 1
            
            # Reset miner's data in the entropy system
            if hasattr(self, 'entropy_system') and self.entropy_system:
                # If entropy system has a reset_miner method, call it
                if hasattr(self.entropy_system, 'reset_miner_data'):
                    self.entropy_system.reset_miner_data(miner_uid)
                # Otherwise, reset the miner's final entropy score directly
                else:
                    for day in range(self.max_days):
                        self.entropy_system.final_scores[day][miner_uid] = 0.0
                        
            # Initialize fresh miner stats for just this miner
            if self.scoring_data:
                # First ensure the row exists
                await self.db_manager.execute_query(
                    "INSERT OR IGNORE INTO miner_stats (miner_uid) VALUES (?)",
                    (miner_uid,)
                )
                # Then update with default values
                await self.db_manager.execute_query(
                    """UPDATE miner_stats 
                       SET miner_current_tier = 1,
                           miner_current_scoring_window = 0,
                           miner_current_composite_score = 0,
                           miner_current_sharpe_ratio = 0,
                           miner_current_sortino_ratio = 0,
                           miner_current_roi = 0,
                           miner_current_clv_avg = 0,
                           miner_lifetime_earnings = 0,
                           miner_lifetime_wager_amount = 0,
                           miner_lifetime_roi = 0,
                           miner_lifetime_predictions = 0,
                           miner_lifetime_wins = 0,
                           miner_lifetime_losses = 0,
                           miner_win_loss_ratio = 0
                       WHERE miner_uid = ?""",
                    (miner_uid,)
                )
                bt.logging.info(f"Re-initialized miner stats for miner {miner_uid}")
                        
            bt.logging.info(f"Successfully reset all data for miner {miner_uid}")
                    
        except Exception as e:
            bt.logging.error(f"Error resetting miner {miner_uid}: {str(e)}")
            bt.logging.error(traceback.format_exc())
            raise

    def get_miner_history(self, miner_uid: int, score_type: str, days: int = None):
        """
        Get the score history for a specific miner and score type.

        Args:
            miner_uid (int): The UID of the miner.
            score_type (str): The type of score to retrieve ('clv', 'roi', 'sortino', 'entropy', 'composite', 'tier').
            days (int, optional): Number of days of history to return. If None, returns all available history.

        Returns:
            np.ndarray: An array containing the miner's score history.
        """
        score_array = getattr(self, f"{score_type}_scores", None)
        if score_array is None:
            raise ValueError(f"Invalid score type: {score_type}")

        if days is None:
            return score_array[miner_uid]
        else:
            return score_array[miner_uid, -days:]

    def log_score_summary(self):
        """
        Log a summary of the current scores, including the entire amount_wagered and tiers arrays for debugging.
        """
        bt.logging.info("=== Score Summary ===")
        for score_name, score_array in [
            ("CLV", self.clv_scores),
            ("ROI", self.roi_scores),
            ("Entropy", self.entropy_scores),
            ("Sortino", self.sortino_scores),
            ("Composite", self.composite_scores),
        ]:
            current_scores = self._get_array_for_day(score_array, self.current_day)
            bt.logging.info(
                f"{score_name} Scores - min: {current_scores.min():.4f}, "
                f"max: {current_scores.max():.4f}, "
                f"mean: {current_scores.mean():.4f}, "
                f"non-zero: {np.count_nonzero(current_scores)}"
            )
        
        # Debug: Print the entire amount_wagered array
        #bt.logging.info("=== Amount Wagered Array ===")
        #np.set_printoptions(threshold=np.inf)  # Remove threshold to print entire array
        #bt.logging.info(f"amount_wagered:\n{self.amount_wagered}")
        #np.set_printoptions(threshold=1000)  # Reset to default threshold

        # Debug: Print the entire tiers array
        #bt.logging.info("=== Tiers Array ===")
        #np.set_printoptions(threshold=np.inf)  # Remove threshold to print entire array
        #bt.logging.info(f"tiers:\n{self.tiers}")
        #np.set_printoptions(threshold=1000)  # Reset to default threshold

    def calculate_weights(self, day=None):
        """Calculate weights with a continuous exponential curve based on tier and score."""
        bt.logging.info("Calculating weights with continuous exponential curve")
        
        weights = np.zeros(self.num_miners)
        if day is None:
            day = self.current_day

        try:
            current_tiers = self.tiers[:, day]
            # Check for any wager in the last 7 days to determine active miners
            recent_start_idx = max(0, day - 7 + 1)
            recent_wagers = np.sum(self.amount_wagered[:, recent_start_idx:day + 1], axis=1)
            has_predictions = recent_wagers > 0
            
            # Get miners meeting minimum stake requirement if service is available
            meets_min_stake = np.ones(self.num_miners, dtype=bool)
            if self.min_stake_service is not None:
                bt.logging.info("Applying minimum stake requirement for weights")
                min_stake_list = self.min_stake_service.get_min_stake_for_all_uids()
                if len(min_stake_list) == self.num_miners:
                    meets_min_stake = np.array(min_stake_list)
                    bt.logging.info(f"Miners meeting minimum stake: {np.sum(meets_min_stake)}/{self.num_miners}")
                    
                    # Log detailed breakdown by tier
                    current_tiers = self.tiers[:, day]
                    for tier in range(1, self.num_tiers):
                        tier_miners = np.where(current_tiers == tier)[0]
                        if len(tier_miners) > 0:
                            meet_stake_in_tier = np.sum(meets_min_stake[tier_miners])
                            bt.logging.info(f"Tier {tier}: {meet_stake_in_tier}/{len(tier_miners)} miners meet min stake")
                else:
                    bt.logging.warning(f"Min stake list length ({len(min_stake_list)}) doesn't match num_miners ({self.num_miners}). Skipping min stake check.")
            
            valid_miners = np.array(list(set(range(self.num_miners)) - self.invalid_uids))
            valid_miners = valid_miners[
                (current_tiers[valid_miners] >= 2) &  # Tier 1+ only
                (current_tiers[valid_miners] < self.num_tiers) &
                has_predictions[valid_miners] &
                meets_min_stake[valid_miners]  # Add min stake requirement
            ]
            
            # Setup weights array with zeros
            weights = np.zeros(self.num_miners)
            
            # Get current tiers for all miners
            current_tiers = self.tiers[:, day].copy()
            
            # Apply penalties for invalid UIDs
            if hasattr(self, 'invalid_uids') and self.invalid_uids:
                current_tiers[list(self.invalid_uids)] = 0
            
            # Create dictionary of populated tiers
            populated_tiers = {}
            for tier in range(1, len(self.tier_configs) + 1):
                miners = np.where(current_tiers == tier)[0]
                if len(miners) > 0:
                    populated_tiers[tier] = {
                        'miners': miners,
                        'base_incentive': self.tier_configs[tier-1]['incentive'],
                        'adjusted_incentive': self.tier_configs[tier-1]['incentive']
                    }
            
            if not populated_tiers:
                return weights
            
            # Create a continuous rank across all miners
            all_miners = []
            miner_tiers = []
            
            # Collect all miners and their tiers
            for tier in sorted(populated_tiers.keys()):
                all_miners.extend(populated_tiers[tier]['miners'])
                miner_tiers.extend([tier] * len(populated_tiers[tier]['miners']))
            
            # Convert to arrays
            all_miners = np.array(all_miners)
            miner_tiers = np.array(miner_tiers)
            
            # Get scores for all miners in their respective tiers
            all_scores = np.zeros(len(all_miners))
            for i, (miner, tier) in enumerate(zip(all_miners, miner_tiers)):
                all_scores[i] = self.composite_scores[miner, day, tier-1]
            
            # Sort by tier (ascending order - higher tier numbers first), then by score (high to low)
            # This ensures tier 5 is at the top, followed by tier 4, etc.
            sort_indices = np.lexsort((all_scores, miner_tiers))[::1]  # Reverse to put higher tiers first
            sorted_miners = all_miners[sort_indices]
            
            # Create continuous ranks from 0 to 1
            total_miners = len(sorted_miners)
            continuous_ranks = np.linspace(0, 1, total_miners)
            
            # Apply exponential function to create Pareto-like distribution
            pareto_exponent = 7  # Controls the steepness (higher = steeper curve)
            
            # Calculate weights using the exponential curve - maintain the curve's shape
            for i, miner in enumerate(sorted_miners):
                weights[miner] = np.exp(pareto_exponent * continuous_ranks[i])
            
            # Create small, fixed "steps" at tier boundaries while preserving the overall curve
            # First, identify tier boundaries in the sorted list
            sorted_tiers = np.zeros(len(sorted_miners), dtype=int)
            for i, miner in enumerate(sorted_miners):
                # Find the tier for this miner
                for tier, data in populated_tiers.items():
                    if miner in data['miners']:
                        sorted_tiers[i] = tier
                        break
            
            # Track the previous miner's tier to detect tier boundaries
            prev_tier = sorted_tiers[0] if len(sorted_miners) > 0 else 0
            tier_step = 0.15  # Much larger step at tier boundaries for clear visual breaks
            
            # Apply small steps at tier boundaries
            adjusted_weights = weights.copy()
            
            # Track total adjustment for each tier
            tier_adjustments = {}
            current_adjustment = 0
            
            # First identify tier boundaries and calculate cumulative adjustment
            for tier in sorted(populated_tiers.keys()):
                if tier > min(populated_tiers.keys()):
                    # Add a step for each tier boundary (except the lowest tier)
                    current_adjustment += tier_step
                tier_adjustments[tier] = current_adjustment
            
            # Apply the adjustments to each miner based on their tier
            for miner in range(self.num_miners):
                for tier, data in populated_tiers.items():
                    if miner in data['miners']:
                        adjusted_weights[miner] += tier_adjustments[tier]
                        break
            
            # Replace weights with adjusted weights
            weights = adjusted_weights
            
            # No need to renormalize the complete weights array, as we want to preserve
            # the distinct tier separation
            
            # Apply penalties for invalid UIDs
            if hasattr(self, 'invalid_uids') and self.invalid_uids:
                weights[list(self.invalid_uids)] = 0
            
            # Apply penalties for miners not meeting min stake requirements
            if self.min_stake_service is not None:
                for miner in range(self.num_miners):
                    if not meets_min_stake[miner]:
                        if weights[miner] > 0:
                            bt.logging.info(f"Zeroing weight for miner {miner} due to insufficient stake")
                            weights[miner] = 0
                
                # Log how many miners got zeroed due to min stake
                zeroed_count = self.num_miners - np.sum(meets_min_stake)
                bt.logging.info(f"Zeroed weights for {zeroed_count} miners due to insufficient stake")
            
            # Renormalize after applying penalties
            if np.sum(weights) > 0:
                weights = weights / np.sum(weights)
            else:
                bt.logging.warning("Total weight is zero. Distributing weights equally among valid miners.")
                if hasattr(self, 'valid_uids') and self.valid_uids:
                    weights[list(self.valid_uids)] = 1 / len(self.valid_uids)
            
            # Log weight distribution by tier
            for tier in populated_tiers:
                tier_miners = np.where(current_tiers == tier)[0]
                if len(tier_miners) > 0:
                    tier_sum = weights[tier_miners].sum()
                    tier_min = weights[tier_miners].min()
                    tier_max = weights[tier_miners].max()
                    tier_spread = tier_max - tier_min
                    # Corrected log to use the actual tier number
                    bt.logging.info(f"Tier {tier} distribution:") 
                    bt.logging.info(f"  Weight range: {tier_min:.6f} - {tier_max:.6f}")
                    bt.logging.info(f"  Weight spread: {tier_spread:.6f}")
                    bt.logging.info(f"  Total weight: {tier_sum:.4f}")
            
            # Calculate approximate 80/20 verification
            top_20pct_count = int(total_miners * 0.2)
            if top_20pct_count > 0:
                top_20pct_weight = weights[sorted_miners[-top_20pct_count:]].sum()
                bt.logging.info(f"Top 20% miners ({top_20pct_count}) get {top_20pct_weight*100:.1f}% of weight")
            
            final_sum = weights.sum()
            bt.logging.info(f"Final weight sum: {final_sum:.6f}")
            
            return weights
        
        except Exception as e:
            bt.logging.error(f"Error calculating weights: {e}")
            bt.logging.error(f"Traceback: {traceback.format_exc()}")
            return weights

    async def scoring_run(self, date, invalid_uids, valid_uids):
        bt.logging.info(f"=== Starting scoring run for date: {date} ===")

        # Update invalid, valid, and empty UIDs
        self.invalid_uids = set(invalid_uids)
        self.valid_uids = set(valid_uids)
        self.empty_uids = set(range(self.num_miners)) - self.valid_uids - self.invalid_uids

        # Create boolean masks for each category
        empty_mask = np.zeros(self.num_miners, dtype=bool)
        empty_mask[list(self.empty_uids)] = True

        invalid_mask = np.zeros(self.num_miners, dtype=bool)
        invalid_mask[list(self.invalid_uids)] = True

        valid_mask = np.zeros(self.num_miners, dtype=bool)
        valid_mask[list(self.valid_uids)] = True

        # Set tiers using boolean masks
        if self.init:
            self.tiers[:, self.current_day] = 2  # Initialize valid UIDs to tier 2
            bt.logging.info(f"Assigned {len(self.valid_uids)} valid UIDs to tier 2.")
            self.init = False

        # Assign empty and invalid UIDs
        self.tiers[empty_mask, self.current_day] = 0
        self.tiers[invalid_mask, self.current_day] = 1

        # Ensure valid UIDs are at least in tier 2
        self.tiers[valid_mask & (self.tiers[:, self.current_day] < 2)] = 2  

        bt.logging.info(f"Assigned {len(self.empty_uids)} empty slots to tier 0.")
        bt.logging.info(f"Assigned {len(self.invalid_uids)} invalid UIDs to tier 1.")

        try:
            current_date = self._ensure_date(date)
            bt.logging.debug(f"Processing scoring_run for date: {current_date}")
            self.advance_day(current_date)  # Pass only the date part

            # Add this: Populate amount_wagered after advancing day
            await self.populate_amount_wagered()

            date_str = current_date.strftime('%Y-%m-%d %H:%M:%S')

            bt.logging.info(
                f"Current day: {self.current_day}, reference date: {self.reference_date}"
            )

            # Add this debugging code before calculating composite scores
            current_tiers = self.tiers[:, self.current_day]
            tier_distribution = [
                int(np.sum(current_tiers == tier))
                for tier in range(0, len(self.tier_configs) + 1)
            ]
            bt.logging.info(f"Current tier distribution: {tier_distribution}")

            (
                predictions,
                closing_line_odds,
                results,
            ) = await self.scoring_data.preprocess_for_scoring(date_str)

            bt.logging.info(
                f"Number of predictions: {predictions.shape[0] if predictions.size > 0 else 0}"
            )

            if predictions.size > 0 and closing_line_odds.size > 0 and results.size > 0:
                bt.logging.info("Updating scores...")
                await self.update_scores(predictions, closing_line_odds, results)
                bt.logging.info("Scores updated successfully.")

                # Calculate total daily wager and active miners
                daily_wagers = self.amount_wagered[:, self.current_day]
                total_wager = np.sum(daily_wagers)
                active_miners = np.count_nonzero(daily_wagers)
                bt.logging.info(f"Total daily wager: {total_wager:.2f}")
                bt.logging.info(f"Number of active miners: {active_miners}")
                if active_miners > 0:
                    bt.logging.info(f"Average wager per active miner: {total_wager/active_miners:.2f}")

                await self.manage_tiers(invalid_uids, valid_uids)

                # Calculate weights using the existing method
                weights = self.calculate_weights()

                # Update most_recent_weight in miner_stats table
                update_weights_query = """
                    UPDATE miner_stats 
                    SET most_recent_weight = ? 
                    WHERE miner_uid = ?
                """
                weight_updates = [(float(weights[i]), i) for i in range(self.num_miners)]
                await self.db_manager.executemany(update_weights_query, weight_updates)
                bt.logging.info("Updated most_recent_weights in miner_stats table")

            else:
                bt.logging.warning(
                    f"No predictions for date {date_str}. Using previous day's weights."
                )
                await self.manage_tiers(invalid_uids, valid_uids)

                previous_day = (self.current_day - 1) % self.max_days
                try:
                    weights = self.calculate_weights(day=previous_day)
                    bt.logging.info(f"Using weights from previous day: {previous_day}")
                    
                    # Update most_recent_weight in miner_stats table using the specialized method
                    weight_updates = [(float(weights[i]), i) for i in range(self.num_miners)]
                    await self.db_manager.update_miner_weights(weight_updates)
                    bt.logging.info("Updated most_recent_weights in miner_stats table using previous day's weights")

                except Exception as e:
                    bt.logging.error(
                        f"Failed to retrieve weights from previous day: {e}. Assigning equal weights."
                    )
                    weights = np.zeros(self.num_miners)
                    weights[list(self.valid_uids)] = 1 / len(self.valid_uids)
                    
                    # Update most_recent_weight with equal weights using the specialized method
                    weight_updates = [(float(weights[i]), i) for i in range(self.num_miners)]
                    await self.db_manager.update_miner_weights(weight_updates)
                    bt.logging.info("Updated most_recent_weights in miner_stats table with equal weights")

            # Assign invalid UIDs to tier 0
            weights[list(self.invalid_uids)] = 0

            # Renormalize weights
            if weights.sum() > 0:
                weights /= weights.sum()
            else:
                bt.logging.warning("Total weight is zero. Distributing weights equally among valid miners.")
                weights[list(self.valid_uids)] = 1 / len(self.valid_uids)

            bt.logging.info(f"Weight sum: {weights.sum():.6f}")
            bt.logging.info(
                f"Min weight: {weights.min():.6f}, Max weight: {weights.max():.6f}"
            )
            bt.logging.info(f"Weights: {weights}")

            # Log final tier distribution
            self.log_score_summary()

            bt.logging.info(f"=== Completed scoring run for date: {date_str} ===")

            # Save state at the end of each run
            await self.save_state()
            await self.scoring_data.update_miner_stats(self.current_day)

            # check that weights are length 256. 
            if len(weights) != 256:
                bt.logging.error(f"Weights are not length 256. They are length {len(weights)}")
                return None
            self.validator.scores = weights

            return weights

        except Exception as e:
            bt.logging.error(f"Error in scoring run: {e}")
            bt.logging.error(f"Traceback: {traceback.format_exc()}")
            raise

    def reset_date(self, new_date):
        """
        Reset the scoring system's date for testing purposes.

        Args:
            new_date (datetime): The new date to set as the current date.
        """
        # Ensure new_date is timezone-aware
        if new_date.tzinfo is None:
            new_date = new_date.replace(tzinfo=timezone.utc)

        self.current_date = new_date
        days_since_reference = (new_date - self.reference_date).days
        self.current_day = days_since_reference % self.max_days

        # Reset tiers to 1 for the current day, using modulo for wraparound
        self.tiers[:, self._get_day_index(self.current_day)] = 1

    def reset_all_miners_to_tier_1(self):
        """
        Reset all miners to tier 1.
        """
        self.tiers.fill(1)

    def _get_day_index(self, day):
        """
        Get the index of a day in the circular buffer.

        Args:
            day (int): The day to get the index for.

        Returns:
            int: The index of the day in the circular buffer.
        """
        return day % self.max_days

    def _get_array_for_day(self, array, day, tier=None):
        """
        Get the array for a specific day and tier.

        Args:
            array (np.ndarray): The array to get the data from.
            day (int): The day to get the data for.
            tier (int, optional): The tier to get the data for. Defaults to None.

        Returns:
            np.ndarray: The array for the specified day and tier.
        """
        if tier is None:
            return array[:, self._get_day_index(day)]
        else:
            return array[:, self._get_day_index(day), tier]

    def _set_array_for_day(self, array, day, value, tier=None):
        """
        Set the array for a specific day and tier.

        Args:
            array (np.ndarray): The array to set the data in.
            day (int): The day to set the data for.
            value (np.ndarray): The value to set in the array.
            tier (int, optional): The tier to set the data for. Defaults to None.
        """
        if tier is None:
            array[:, self._get_day_index(day)] = value
        else:
            array[:, self._get_day_index(day), tier] = value

    async def save_state(self):
        """
        Save the current state of the ScoringSystem to the database, including the amount_wagered and tiers arrays.
        """
        max_retries = 5
        base_delay = 1
        
        # Save current state for rollback
        state_backup = {
            'current_day': self.current_day,
            'current_date': self.current_date,
            'reference_date': self.reference_date,
            'last_update_date': self.last_update_date,
            'invalid_uids': copy.deepcopy(self.invalid_uids),
            'valid_uids': copy.deepcopy(self.valid_uids),
            'amount_wagered': self.amount_wagered.copy(),
            'tiers': self.tiers.copy()
        }
        
        for attempt in range(max_retries):
            try:
                # Use timeout for entire save operation
                async with async_timeout.timeout(60):
                    # Convert data to proper format
                    params = {
                        'current_day': self.current_day,
                        'current_date': self.current_date.isoformat() if self.current_date else None,
                        'reference_date': self.reference_date.isoformat(),
                        'invalid_uids': json.dumps(list(int(uid) for uid in self.invalid_uids)),
                        'valid_uids': json.dumps(list(int(uid) for uid in self.valid_uids)),
                        'last_update_date': self.last_update_date.isoformat() if self.last_update_date else None,
                        'amount_wagered': json.dumps(self.amount_wagered.tolist()),
                        'tiers': json.dumps(self.tiers.tolist())
                    }

                    insert_state_query = """
                        INSERT INTO score_state 
                        (current_day, current_date, reference_date, invalid_uids, valid_uids, last_update_date, amount_wagered, tiers)
                        VALUES (:current_day, :current_date, :reference_date, :invalid_uids, :valid_uids, :last_update_date, :amount_wagered, :tiers)
                        ON CONFLICT(state_id) DO UPDATE SET
                            current_day=excluded.current_day,
                            current_date=excluded.current_date,
                            reference_date=excluded.reference_date,
                            invalid_uids=excluded.invalid_uids,
                            valid_uids=excluded.valid_uids,
                            last_update_date=excluded.last_update_date,
                            amount_wagered=excluded.amount_wagered,
                            tiers=excluded.tiers
                    """

                    async with self.db_manager.get_long_running_session() as session:
                        await session.execute(text(insert_state_query), params)
                        await session.commit()
                        
                        # Now save scores
                        await self.save_scores()
                        
                        # Save entropy system state with conflict resolution
                        if hasattr(self, 'entropy_system'):
                            try:
                                await self.entropy_system.save_state()
                            except Exception as e:
                                bt.logging.error(f"Error saving entropy system state: {e}")
                                bt.logging.error(traceback.format_exc())
                                raise

                        bt.logging.trace("ScoringSystem state saved to database, including amount_wagered and tiers.")
                        return

            except (asyncio.TimeoutError, SQLAlchemyError) as e:
                if attempt < max_retries - 1:
                    # Exponential backoff with jitter
                    delay = base_delay * (2 ** attempt) + random.uniform(0, 1)
                    bt.logging.warning(f"Save attempt {attempt + 1} failed, retrying in {delay:.1f}s: {str(e)}")
                    
                    # Restore state before retry
                    self.current_day = state_backup['current_day']
                    self.current_date = state_backup['current_date']
                    self.reference_date = state_backup['reference_date']
                    self.last_update_date = state_backup['last_update_date']
                    self.invalid_uids = copy.deepcopy(state_backup['invalid_uids'])
                    self.valid_uids = copy.deepcopy(state_backup['valid_uids'])
                    self.amount_wagered = state_backup['amount_wagered'].copy()
                    self.tiers = state_backup['tiers'].copy()
                    
                    await asyncio.sleep(delay)
                    continue
                raise
            except Exception as e:
                bt.logging.error(f"Error saving state to database: {e}")
                bt.logging.error(traceback.format_exc())
                
                # Restore state on error
                self.current_day = state_backup['current_day']
                self.current_date = state_backup['current_date']
                self.reference_date = state_backup['reference_date']
                self.last_update_date = state_backup['last_update_date']
                self.invalid_uids = copy.deepcopy(state_backup['invalid_uids'])
                self.valid_uids = copy.deepcopy(state_backup['valid_uids'])
                self.amount_wagered = state_backup['amount_wagered'].copy()
                self.tiers = state_backup['tiers'].copy()
                
                raise

    async def load_state(self):
        """Load scoring system state from database"""
        try:
            async with self.db_manager.transaction() as session:
                fetch_state_query = """
                    SELECT current_day, current_date, reference_date, invalid_uids, valid_uids, 
                        last_update_date, amount_wagered, tiers, state_id
                    FROM score_state
                    WHERE current_day > 0
                    ORDER BY state_id DESC
                    LIMIT 1
                """
                result = await session.execute(text(fetch_state_query))
                state = result.first()
                if state:
                    state = dict(zip(result.keys(), state))

                # If no state with current_day > 0, fall back to the most recent state
                if not state:
                    fetch_state_query = """
                        SELECT current_day, current_date, reference_date, invalid_uids, valid_uids, 
                            last_update_date, amount_wagered, tiers, state_id
                        FROM score_state
                        ORDER BY state_id DESC
                        LIMIT 1
                    """
                    result = await session.execute(text(fetch_state_query))
                    state = result.first()
                    if state:
                        state = dict(zip(result.keys(), state))
                
                if state:
                    bt.logging.info(f"Found existing state in database with state_id={state['state_id']}")
                    self.current_day = state["current_day"]
                    self.current_date = datetime.fromisoformat(state["current_date"])
                    self.reference_date = datetime.fromisoformat(state["reference_date"])
                    self.last_update_date = (datetime.fromisoformat(state["last_update_date"]).date() 
                                           if state["last_update_date"] else None)
                    
                    # Load UIDs and tiers first
                    self.invalid_uids = set(json.loads(state["invalid_uids"]))
                    self.valid_uids = set(json.loads(state["valid_uids"]))
                    self.tiers = np.array(json.loads(state["tiers"]))
                    
                    # Load scores with skip_rebuild=True since we're in initialization
                    await self.load_scores(skip_rebuild=True)
                    
                    # Verify and populate amount_wagered
                    try:
                        amount_wagered_data = json.loads(state["amount_wagered"])
                        if not amount_wagered_data or len(amount_wagered_data) != self.num_miners:
                            bt.logging.warning("Invalid amount_wagered data in state, repopulating...")
                            await self.populate_amount_wagered()
                        else:
                            self.amount_wagered = np.array(amount_wagered_data)
                    except (json.JSONDecodeError, TypeError):
                        bt.logging.warning("Corrupted amount_wagered data in state, repopulating...")
                        await self.populate_amount_wagered()
                    
                    # Add validation check for current_day
                    if self.current_day == 0:
                        bt.logging.warning("Loading state with current_day=0, checking for more recent states...")
                        check_query = """
                        SELECT MAX(current_day) as max_day
                        FROM score_state
                        WHERE current_day > 0
                        """
                        result = await session.execute(text(check_query))
                        max_day = result.first()
                        if max_day:
                            max_day = dict(zip(result.keys(), max_day))
                            if max_day['max_day'] is not None:
                                bt.logging.warning(f"Found more recent state with day {max_day['max_day']}, but loading day 0 state")
                                # Load the state with the highest current_day
                                fetch_state_query = """
                                    SELECT current_day, current_date, reference_date, invalid_uids, valid_uids, 
                                        last_update_date, amount_wagered, tiers, state_id
                                    FROM score_state
                                    WHERE current_day = ?
                                    ORDER BY state_id DESC
                                    LIMIT 1
                                """
                                result = await session.execute(text(fetch_state_query), {'p0': max_day['max_day']})
                                state = result.first()
                                if state:
                                    state = dict(zip(result.keys(), state))
                                    self.current_day = state["current_day"]
                                    bt.logging.info(f"Updated to load state with current_day={self.current_day}")
                else:
                    bt.logging.warning("No existing state found, initializing fresh state...")
                    await self.populate_amount_wagered()
                    await self.load_scores()  # Allow rebuild in this case

        except Exception as e:
            bt.logging.error(f"Error loading state from database: {e}")
            bt.logging.error(f"Traceback: {traceback.format_exc()}")
            bt.logging.warning("Falling back to fresh state initialization...")
            await self.populate_amount_wagered()
            await self.load_scores()
            raise

    async def save_scores(self):
        score_records = []
        column_names = ['miner_uid', 'day_id', 'score_type', 'clv_score', 'roi_score', 
                       'entropy_score', 'composite_score', 'sortino_score']
        
        for miner in range(self.num_miners):
            # Daily scores
            score_records.append((
                miner,
                self.current_day,
                'daily',
                float(self.clv_scores[miner, self.current_day]),
                float(self.roi_scores[miner, self.current_day]),
                float(self.entropy_scores[miner, self.current_day]),
                float(self.composite_scores[miner, self.current_day, 0]),
                float(self.sortino_scores[miner, self.current_day])
            ))
            
            # Tier-specific scores
            for tier_idx in range(1, self.composite_scores.shape[2]):
                if tier_idx in self.tier_mapping:
                    score_records.append((
                        miner,
                        self.current_day,
                        self.tier_mapping[tier_idx],
                        None,  # clv_score
                        None,  # roi_score
                        None,  # entropy_score
                        float(self.composite_scores[miner, self.current_day, tier_idx]),
                        None   # sortino_score
                    ))

        # Use executemany with column names
        await self.db_manager.executemany(
            """INSERT INTO scores 
               (miner_uid, day_id, score_type, clv_score, roi_score, entropy_score, composite_score, sortino_score)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(miner_uid, day_id, score_type) DO UPDATE SET
               clv_score=excluded.clv_score,
               roi_score=excluded.roi_score,
               entropy_score=excluded.entropy_score,
               composite_score=excluded.composite_score,
               sortino_score=excluded.sortino_score""",
            score_records,
            column_names=column_names
        )

    async def load_scores(self, skip_rebuild=False):
        """
        Load the scores from the database. If missing historical scores are detected,
        attempt to rebuild them from historical prediction data.
        """
        try:
            # If force_rebuild is True and we're not explicitly skipping rebuild, do it
            if self.force_rebuild and not skip_rebuild:
                bt.logging.info("Force rebuild enabled, initiating historical score rebuild...")
                try:
                    # Clear existing scores
                    clear_scores_query = "DELETE FROM scores"
                    await self.db_manager.execute_query(clear_scores_query)
                    await self.rebuild_historical_scores()
                    bt.logging.info("Forced historical score rebuild completed")
                    return
                except Exception as e:
                    bt.logging.error(f"Error during forced rebuild: {e}")
                    bt.logging.error(traceback.format_exc())
                    raise

            # First do a simple check for non-zero scores
            quick_check_query = """
                SELECT COUNT(*) as valid_scores
                FROM scores 
                WHERE composite_score != 0
            """
            result = await self.db_manager.fetch_one(quick_check_query)
            valid_scores = result["valid_scores"] if result else 0
            
            bt.logging.info(f"Found {valid_scores} non-zero composite scores in database")
            
            if valid_scores == 0 and not skip_rebuild:
                bt.logging.warning("No valid scores found in database, checking for historical data...")
                # Check if we have historical prediction data to rebuild from
                check_predictions_query = """
                    SELECT 
                        COUNT(DISTINCT p.prediction_id) as prediction_count,
                        COUNT(DISTINCT DATE(p.prediction_date)) as days_with_predictions,
                        MIN(p.prediction_date) as earliest_prediction,
                        MAX(p.prediction_date) as latest_prediction
                    FROM predictions p
                    JOIN game_data g ON p.game_id = g.external_id
                    WHERE 
                        p.prediction_date >= ? 
                        AND p.prediction_date <= ?
                        AND g.outcome IS NOT NULL
                        AND g.outcome != 'Unfinished'
                """
                params = (
                    (self.current_date - timedelta(days=self.max_days)).isoformat(),
                    self.current_date.isoformat()
                )
                
                pred_result = await self.db_manager.fetch_one(check_predictions_query, params)
                
                if pred_result and pred_result["prediction_count"] > 0:
                    bt.logging.info(
                        f"Found historical data:\n"
                        f"- {pred_result['prediction_count']} predictions\n"
                        f"- {pred_result['days_with_predictions']} days with predictions\n"
                        f"- Date range: {pred_result['earliest_prediction']} to {pred_result['latest_prediction']}"
                    )
                    
                    # Clear existing scores before rebuild
                    clear_scores_query = "DELETE FROM scores"
                    await self.db_manager.execute_query(clear_scores_query)
                    bt.logging.info("Cleared existing scores, starting rebuild...")
                    
                    await self.rebuild_historical_scores()
                    bt.logging.info("Historical scores rebuilt successfully")
                    return
                else:
                    bt.logging.warning("No historical prediction data found for score rebuild")
            else:
                # Do a more detailed check for completeness
                detailed_check_query = """
                    SELECT 
                        COUNT(DISTINCT day_id) as days_with_scores,
                        COUNT(*) as total_entries,
                        SUM(CASE WHEN composite_score != 0 THEN 1 ELSE 0 END) as valid_entries
                    FROM scores 
                    WHERE CASE 
                        WHEN ? <= ? THEN  -- Normal case: oldest <= current
                            day_id BETWEEN ? AND ?
                        ELSE  -- Wrapped case: oldest > current
                            day_id >= ? OR day_id <= ?
                        END
                """
                
                current_day_index = self.current_day
                
                # Get the oldest prediction date within our max_days window
                oldest_prediction_query = """
                    SELECT MIN(prediction_date) as oldest_date
                    FROM predictions 
                    WHERE prediction_date >= date(?, '-45 days')
                    AND prediction_date <= ?
                """
                oldest_result = await self.db_manager.fetch_one(
                    oldest_prediction_query, 
                    (self.current_date.isoformat(), self.current_date.isoformat())
                )
                
                if oldest_result and oldest_result['oldest_date']:
                    oldest_date = datetime.fromisoformat(oldest_result['oldest_date']).date()
                    days_since_oldest = (self.current_date.date() - oldest_date).days
                    oldest_day_index = (current_day_index - min(days_since_oldest, self.max_days - 1)) % self.max_days
                else:
                    oldest_day_index = (current_day_index - self.max_days + 1) % self.max_days
                
                # Calculate total days accounting for circular buffer
                total_days = (
                    current_day_index - oldest_day_index + 1
                    if current_day_index >= oldest_day_index
                    else (self.max_days - oldest_day_index) + current_day_index + 1
                )
                
                bt.logging.info(f"Checking scores from day {oldest_day_index} to {current_day_index} (total: {total_days} days)")
                
                # Use total_days for expected entries calculation
                expected_entries = self.num_miners * 6 * total_days  # daily + 5 tier scores per miner per day
                
                result = await self.db_manager.fetch_one(
                    detailed_check_query, 
                    (
                        oldest_day_index, current_day_index,  # For the WHEN comparison
                        oldest_day_index, current_day_index,  # For the BETWEEN case
                        oldest_day_index, current_day_index   # For the OR case
                    )
                )
                
                if result:
                    bt.logging.info(
                        f"Score completeness check:\n"
                        f"- Days with scores: {result['days_with_scores']}/{total_days}\n"
                        f"- Total entries: {result['total_entries']}/{expected_entries}\n"
                        f"- Valid entries: {result['valid_entries']}/{expected_entries}"
                    )
                    
                    # Only rebuild if less than 60% of entries are valid
                    completeness_ratio = result['valid_entries'] / expected_entries if expected_entries > 0 else 0
                    if completeness_ratio < 0.6 or self.force_rebuild:  # Less than 60% complete or force rebuild
                        bt.logging.warning(f"Score completeness ratio ({completeness_ratio:.1%}) below 60% or force rebuild enabled, initiating rebuild...")
                        await self.rebuild_historical_scores()
                        bt.logging.info("Historical scores rebuilt successfully")
                        return
                    else:
                        bt.logging.info(f"Score completeness ratio ({completeness_ratio:.1%}) above 60%, skipping rebuild")
            
            # Load scores from database into memory arrays
            fetch_scores_query = """
                SELECT miner_uid, day_id, score_type, clv_score, roi_score, 
                       entropy_score, composite_score, sortino_score 
                FROM scores
                WHERE composite_score != 0
                ORDER BY day_id, miner_uid
            """
            
            scores = await self.db_manager.fetch_all(fetch_scores_query)
            
            # Reset score arrays before populating
            self.clv_scores.fill(0)
            self.roi_scores.fill(0)
            self.entropy_scores.fill(0)
            self.sortino_scores.fill(0)
            self.composite_scores.fill(0)
            
            populated_count = 0
            for score in scores:
                miner_uid = score["miner_uid"]
                day_id = score["day_id"]
                score_type = score["score_type"]
                
                if 0 <= miner_uid < self.num_miners and 0 <= day_id < self.max_days:
                    populated_count += 1
                    if score_type == 'daily':
                        self.clv_scores[miner_uid, day_id] = score["clv_score"] or 0.0
                        self.roi_scores[miner_uid, day_id] = score["roi_score"] or 0.0
                        self.entropy_scores[miner_uid, day_id] = score["entropy_score"] or 0.0
                        self.sortino_scores[miner_uid, day_id] = score["sortino_score"] or 0.0
                        self.composite_scores[miner_uid, day_id, 0] = score["composite_score"] or 0.0
                    else:
                        # Map tier scores to the correct index
                        tier_index = list(self.tier_mapping.values()).index(score_type)
                        if 1 <= tier_index < self.composite_scores.shape[2]:
                            self.composite_scores[miner_uid, day_id, tier_index] = score["composite_score"] or 0.0
            
            bt.logging.info(f"Populated {populated_count} score entries from database")
                
        except Exception as e:
            bt.logging.error(f"Error loading scores from database: {e}")
            bt.logging.error(traceback.format_exc())
            raise

    def _ensure_datetime(self, date):
        if isinstance(date, str):
            return datetime.fromisoformat(date).replace(tzinfo=timezone.utc)
        elif isinstance(date, datetime) and date.tzinfo is None:
            return date.replace(tzinfo=timezone.utc)
        return date

    def _ensure_date(self, date_input):
        if isinstance(date_input, str):
            return datetime.fromisoformat(date_input).date()
        elif isinstance(date_input, datetime):
            return date_input.date()
        elif isinstance(date_input, date):
            return date_input
        else:
            raise TypeError("Date input must be a string, datetime, or date object.")

    async def full_reset(self):
        """
        Perform a full reset of the scoring system, clearing all state and history.
        """
        bt.logging.trace("Performing full reset of scoring system...")

        # Reset all score arrays
        self.clv_scores.fill(0)
        self.roi_scores.fill(0)
        self.sortino_scores.fill(0)
        self.amount_wagered.fill(0)
        self.entropy_scores.fill(0)
        self.tiers.fill(1)  # Reset all miners to tier 1
        self.composite_scores.fill(0)

        # Reset current day and date
        self.current_day = 0
        self.current_date = datetime.now(timezone.utc)
        self.last_update_date = None

        # Reset UID sets
        self.invalid_uids = set()
        self.valid_uids = set()
        self.empty_uids = set(range(self.num_miners))

        # Reset entropy system
        self.entropy_system = EntropySystem(self.num_miners, self.max_days)

        # Clear database state
        await self._clear_database_state()
        
        # Repopulate amount_wagered from historical data
        await self.populate_amount_wagered()

        bt.logging.trace("Scoring system full reset completed.")

    async def _clear_database_state(self):
        """
        Clear all scoring-related state from the database. 
        """
        try:
            # Clear score_state table
            await self.db_manager.execute_query("DELETE FROM score_state", None)

            # Clear scores table
            await  self.db_manager.execute_query("DELETE FROM scores", None)

            #Clear score state table
            await self.db_manager.execute_query("DELETE FROM score_state", None)

            # Clear miner_stats table
            await self.db_manager.execute_query("DELETE FROM miner_stats", None)

            bt.logging.info("Database state cleared successfully.")
        except Exception as e:
            bt.logging.error(f"Error clearing database state: {e}")
            raise

    def _calculate_clv_scores(self, predictions, closing_line_odds):
        """
        Calculate Closing Line Value (CLV) scores for miners.

        Args:
            predictions (np.ndarray): Structured data with shape (num_predictions, 6).
            closing_line_odds (np.ndarray): Closing line odds with shape (num_games, 3).

        Returns:
            np.ndarray: CLV scores with shape (num_miners,).
        """
        if predictions.size == 0 or closing_line_odds.size == 0:
            bt.logging.error("Predictions or closing line odds are empty.")
            return np.zeros(self.num_miners)

        clv_scores = np.zeros(self.num_miners)
        prediction_counts = np.zeros(self.num_miners)

        # Create a mapping from external_id to index in closing_line_odds
        unique_external_ids = np.unique(predictions[:, 1]).astype(int).tolist()
        external_id_to_index = {external_id: idx for idx, external_id in enumerate(unique_external_ids)}

        for pred in predictions:
            miner_id, external_id, predicted_outcome, predicted_odds, payout, wager = pred
            miner_id = int(miner_id)
            external_id = int(external_id)
            predicted_outcome = int(predicted_outcome)

            if 0 <= miner_id < self.num_miners:
                if (
                    external_id in external_id_to_index
                    and predicted_outcome < closing_line_odds.shape[1]
                ):
                    closing_odds_index = external_id_to_index[external_id]
                    closing_odds = closing_line_odds[
                        closing_odds_index, predicted_outcome
                    ]

                    if closing_odds > 0:
                        clv = predicted_odds / closing_odds
                        if np.isfinite(clv):
                            clv_scores[miner_id] += clv
                            prediction_counts[miner_id] += 1
                        else:
                            bt.logging.warning(
                                f"Invalid CLV value for miner {miner_id} on external_id {external_id}."
                            )
                    elif predicted_outcome == 2:
                        continue  # Tie outcome
                        # bt.logging.trace(f"No tie odds for game {game_id}. Skipping CLV calculation for this prediction.")
                    else:
                        bt.logging.warning(
                            f"Closing odds are zero for external_id {external_id}, outcome {predicted_outcome}."
                        )
                else:
                    bt.logging.warning(
                        f"Invalid external_id or predicted_outcome: {external_id}, {predicted_outcome}"
                    )
            else:
                bt.logging.warning(
                    f"Invalid miner_id {miner_id} encountered. Skipping this prediction."
                )

        # Avoid division by zero and compute average CLV per miner
        mask = prediction_counts > 0
        clv_scores[mask] /= prediction_counts[mask]

        return clv_scores

    def _calculate_roi_scores(self, predictions, results):
        """
        Calculate Return on Investment (ROI) scores for miners.

        Args:
            predictions (np.ndarray): Structured prediction data with shape (num_predictions, 6).
            results (np.ndarray): Array of game results with shape (num_games, 2).

        Returns:
            np.ndarray: ROI scores with shape (num_miners,), representing percentage returns.
        """
        if predictions.size == 0 or results.size == 0:
            bt.logging.error("Predictions or game results are empty.")
            return np.zeros(self.num_miners)

        roi_scores = np.zeros(self.num_miners)
        prediction_counts = np.zeros(self.num_miners)

        # Create a dictionary mapping external_id to outcome
        game_outcomes = dict(results)

        for pred in predictions:
            miner_id, game_id, predicted_outcome, predicted_odds, payout, wager = pred
            miner_id = int(miner_id)
            game_id = int(game_id)

            # Fetch the actual outcome
            actual_outcome = game_outcomes.get(game_id)
            if actual_outcome is None:
                bt.logging.error(
                    f"No actual outcome found for game_id {game_id}. Skipping ROI calculation for miner {miner_id}."
                )
                continue

            if wager == 0:
                bt.logging.error(
                    f"Wager is zero for miner {miner_id} on game_id {game_id}. Skipping ROI calculation."
                )
                continue

            roi = (payout - wager) / wager  # ROI as a percentage

            # bt.logging.debug(
            #     f"Miner {miner_id} | Game ID (External ID) {game_id} | Predicted Outcome: {predicted_outcome} | "
            #     f"Actual Outcome: {actual_outcome} | Wager: {wager} | Payout: {payout} | ROI: {roi}"
            # )

            if np.isfinite(roi):
                roi_scores[miner_id] += roi
                prediction_counts[miner_id] += 1
            else:
                bt.logging.error(
                    f"Invalid ROI value ({roi}) for miner {miner_id} on game_id {game_id}."
                )

        # Compute average ROI per miner without normalization
        mask = prediction_counts > 0
        roi_scores[mask] /= prediction_counts[mask]

        bt.logging.info(
            f"ROI Scores - min: {roi_scores.min():.4f}, max: {roi_scores.max():.4f}, mean: {roi_scores.mean():.4f}"
        )

        return roi_scores

    def _calculate_risk_scores(self, predictions, results):
        """
        Calculate Risk/Reward (R/R) scores for miners based on daily predictions and results.
        
        Args:
            predictions (np.ndarray): Structured prediction data with shape (num_predictions, 6).
            results (np.ndarray): Array of game results with shape (num_games, 2).
        
        Returns:
            np.ndarray: R/R scores with shape (num_miners,).
        """
        risk_scores = np.zeros(self.num_miners)
        game_outcomes = dict(results)
        
        for pred in predictions:
            miner_id, game_id, predicted_outcome, predicted_odds, payout, wager = pred
            miner_id = int(miner_id)
            game_id = int(game_id)
            
            actual_outcome = game_outcomes.get(game_id)
            if actual_outcome is None or wager <= 0 or predicted_odds <= 0:
                continue
            
            roi = (payout - wager) / wager # ROI as a percentage
            
            implied_prob = 1 / predicted_odds
            adjusted_prob = implied_prob * (1 - 0.05) #adjust for an average 5% house edge
            risk_score = min(1 - adjusted_prob, 1) #risk is the inverse of the adjusted probability, max of 100%
            inverse_risk_score = (1 - risk_score) + 0.00001 #add a small buffer to avoid division by zero, jic

            if roi > 0:
                rr_score = roi / risk_score # "risk adjusted ROI" - roi % must exceed risk to be >1, will generally be < 1 
            else:
                rr_score = max(roi / inverse_risk_score, -10)  # lost prediction, risk adjusted ROI is negative and inversely proportional to risk (more risk = closer to -1), capped at -10 penalty

            risk_scores[miner_id] += rr_score
        
        # Calculate the average R/R score for each miner
        prediction_counts = np.bincount(
            predictions[:, 0].astype(int), minlength=self.num_miners
        )
        with np.errstate(divide='ignore', invalid='ignore'):
            risk_scores = np.divide(
                risk_scores, prediction_counts, 
                out=np.zeros_like(risk_scores), 
                where=prediction_counts != 0
            )
        
        return risk_scores

    def normalize_entropy_scores(self, scores, has_history):
        """
        Normalize entropy scores to [-1, 1] range while preserving sign and relative magnitudes.
        """
        normalized = scores.copy()
        non_zero_mask = scores != 0
        
        if not np.any(non_zero_mask):
            return normalized
            
        # Get scores for miners with both history and non-zero values
        valid_scores = scores[non_zero_mask]
        if len(valid_scores) == 0:
            return normalized
            
        # Find max absolute value to preserve relative magnitudes
        max_abs = np.max(np.abs(valid_scores))
        if max_abs > 0:
            # Normalize to [-1, 1] range while preserving sign
            normalized[non_zero_mask] = normalized[non_zero_mask] / max_abs
            
            # Ensure we don't lose scores due to floating point errors
            normalized[non_zero_mask] = np.sign(normalized[non_zero_mask]) * np.maximum(
                np.abs(normalized[non_zero_mask]), 
                1e-10
            )
        
        bt.logging.debug(f"Entropy normalization stats:")
        bt.logging.debug(f"  Original non-zero scores: {np.sum(scores != 0)}")
        bt.logging.debug(f"  Normalized non-zero scores: {np.sum(normalized != 0)}")
        bt.logging.debug(f"  Max absolute value: {max_abs:.4f}")
        
        # Apply history mask after normalization
        normalized[~has_history] = 0
        
        return normalized

    async def _update_composite_scores(self, day_index):
        """Update composite scores for a specific day."""
        try:
            # Get component scores for this day
            clv = self.clv_scores[:, day_index]
            roi = self.roi_scores[:, day_index]
            entropy = self.entropy_scores[:, day_index]
            sortino = self.sortino_scores[:, day_index]
            
            # Log overall statistics
            bt.logging.debug(f"\n=== Score Statistics for Day {day_index} ===")
            for name, scores in [
                ("CLV", clv), 
                ("ROI", roi), 
                ("Entropy", entropy), 
                ("Sortino", sortino)
            ]:
                active_scores = scores[scores != 0]
                if len(active_scores) > 0:
                    bt.logging.debug(
                        f"{name:>8} - Active miners: {len(active_scores):>3}, "
                        f"Min: {active_scores.min():>8.4f}, "
                        f"Max: {active_scores.max():>8.4f}, "
                        f"Mean: {active_scores.mean():>8.4f}, "
                        f"Std: {active_scores.std():>8.4f}"
                    )

            # Calculate daily composite score (index 0)
            daily_composite = (
                self.clv_weight * clv +
                self.roi_weight * roi +
                self.sortino_weight * sortino +
                self.entropy_weight * entropy
            )
            
            # Log the weighted components for high scores
            high_score_threshold = 5.0
            high_scorers = np.where(daily_composite > high_score_threshold)[0]
            if len(high_scorers) > 0:
                bt.logging.warning(f"\n=== High Composite Scores Detected ===")
                bt.logging.warning(f"Found {len(high_scorers)} miners with scores > {high_score_threshold}")
                for miner in high_scorers:
                    bt.logging.warning(
                        f"\nMiner {miner} Composite: {daily_composite[miner]:.4f}\n"
                        f"  CLV:     {clv[miner]:>8.4f} * {self.clv_weight:.2f} = {clv[miner] * self.clv_weight:>8.4f}\n"
                        f"  ROI:     {roi[miner]:>8.4f} * {self.roi_weight:.2f} = {roi[miner] * self.roi_weight:>8.4f}\n"
                        f"  Sortino: {sortino[miner]:>8.4f} * {self.sortino_weight:.2f} = {sortino[miner] * self.sortino_weight:>8.4f}\n"
                        f"  Entropy: {entropy[miner]:>8.4f} * {self.entropy_weight:.2f} = {entropy[miner] * self.entropy_weight:>8.4f}"
                    )

            self.composite_scores[:, day_index, 0] = daily_composite

            # Calculate rolling averages for each tier
            for tier in range(2, 7):  # Tiers 2-6
                window = self.tier_configs[tier]["window"]
                start_day = (day_index - window + 1) % self.max_days
                
                bt.logging.debug(f"\n=== Processing Tier {tier} ===")
                bt.logging.debug(f"Window size: {window}")
                bt.logging.debug(f"Day range: {start_day} to {day_index}")
                
                # Handle circular buffer for window scores
                if start_day <= day_index:
                    window_scores = self.composite_scores[:, start_day:day_index + 1, 0]
                else:
                    window_scores = np.concatenate([
                        self.composite_scores[:, start_day:, 0],
                        self.composite_scores[:, :day_index + 1, 0]
                    ], axis=1)
                
                # Log window statistics
                active_miners = np.where(np.any(window_scores != 0, axis=1))[0]
                if len(active_miners) > 0:
                    bt.logging.debug(f"Active miners in window: {len(active_miners)}")
                    for miner in active_miners:
                        miner_scores = window_scores[miner]
                        nonzero_scores = miner_scores[miner_scores != 0]
                        if len(nonzero_scores) > 0:
                            bt.logging.debug(
                                f"Miner {miner} - "
                                f"Days with scores: {len(nonzero_scores)}/{window}, "
                                f"Mean: {nonzero_scores.mean():.4f}, "
                                f"Max: {nonzero_scores.max():.4f}"
                            )
                
                # Calculate average over window
                rolling_avg = np.zeros(self.num_miners)
                mask = np.any(window_scores != 0, axis=1)
                
                if np.any(mask):
                    rolling_avg[mask] = np.mean(window_scores[mask], axis=1)
                    
                    # Log any extreme values
                    high_scores = np.where(rolling_avg > 5.0)[0]
                    if len(high_scores) > 0:
                        bt.logging.warning(f"\nHigh scores detected in Tier {tier}:")
                        for miner in high_scores:
                            bt.logging.warning(
                                f"Miner {miner} - "
                                f"Rolling avg: {rolling_avg[miner]:.4f}, "
                                f"Raw scores: {window_scores[miner].tolist()}"
                            )
                
                self.composite_scores[:, day_index, tier-1] = rolling_avg

        except Exception as e:
            bt.logging.error(f"Error updating composite scores: {str(e)}")
            bt.logging.error(traceback.format_exc())

    async def initialize(self):
        """Async initialization method to be called after constructor"""
        max_retries = 3
        retry_delay = 1  # seconds
        
        for attempt in range(max_retries):
            try:
                self.init = await self.load_state()
                await self.scoring_data.initialize()
                self.advance_day(self.current_date.date())
                await self.populate_amount_wagered()
                return
            except Exception as e:
                if attempt < max_retries - 1:
                    bt.logging.warning(f"Initialization attempt {attempt + 1} failed, retrying in {retry_delay}s: {e}")
                    await asyncio.sleep(retry_delay)
                else:
                    bt.logging.error(f"Failed to initialize after {max_retries} attempts")
                    raise

    async def rebuild_historical_scores(self):
        """Rebuilds historical scores for the past 45 days using existing prediction and game data."""
        try:
            bt.logging.info("Starting complete historical score rebuild...")
            
            end_date = self.current_date
            start_date = end_date - timedelta(days=self.max_days)
            
            bt.logging.info(f"Rebuilding scores from {start_date} to {end_date}")
            
            # Track valid history periods
            valid_history = np.zeros((self.num_miners, self.max_days), dtype=bool)
            
            # Process each day
            for days_ago in range(self.max_days - 1, -1, -1):
                process_date = end_date - timedelta(days=days_ago)
                historical_day = (self.current_day - days_ago) % self.max_days
                
                bt.logging.info(f"Processing historical day {process_date.date()} (index: {historical_day})")
                
                # First get all predictions made on this date
                predictions_query = """
                    SELECT p.*
                    FROM predictions p
                    WHERE DATE(p.prediction_date) = DATE(:process_date)
                    ORDER BY p.prediction_date ASC
                """
                
                day_predictions = await self.db_manager.fetch_all(
                    predictions_query, 
                    {"process_date": process_date.isoformat()}
                )

                if not day_predictions:
                    bt.logging.info(f"No predictions found for {process_date.date()}")
                    continue

                # Get all game IDs from these predictions
                game_ids = list(set(p['game_id'] for p in day_predictions))
                
                # Then get the games data
                games_query = """
                    SELECT g.*
                    FROM game_data g
                    WHERE g.external_id IN ({})
                    AND g.outcome IS NOT NULL
                    AND g.outcome != 'Unfinished'
                    AND g.outcome != 3
                    ORDER BY g.event_start_date ASC
                """.format(','.join(f':id_{i}' for i in range(len(game_ids))))
                
                params = {f'id_{i}': gid for i, gid in enumerate(game_ids)}
                day_games = await self.db_manager.fetch_all(games_query, params)

                bt.logging.info(
                    f"Found for {process_date.date()}: {len(day_games)} games with outcomes, "
                    f"{len(day_predictions)} predictions"
                )

                if not day_games:
                    continue
                
                try:
                    # Format game data for validation
                    game_data = {}
                    for game in day_games:
                        game_id = game['external_id']
                        game_data[game_id] = {
                            'external_id': game_id,
                            'team_a': game['team_a'],
                            'team_b': game['team_b'],
                            'team_a_odds': game['team_a_odds'],
                            'team_b_odds': game['team_b_odds'],
                            'tie_odds': game['tie_odds'],
                            'event_start_date': game['event_start_date'],
                            'outcome': game['outcome']
                        }
                    
                    # Validate historical predictions
                    valid_predictions, validation_stats = await self.miner_data.validate_historical_predictions(
                        day_predictions, game_data
                    )
                    
                    if not valid_predictions:
                        bt.logging.warning(f"No valid predictions found for {process_date.date()} after validation")
                        continue
                    
                    # Format arrays for score calculation using only valid predictions
                    pred_array = np.array([
                        [
                            int(p['miner_uid']), 
                            int(p['game_id']), 
                            int(p['predicted_outcome']),
                            float(p['predicted_odds']) if p['predicted_odds'] is not None else 0.0,
                            float(p.get('payout', 0)) if p.get('payout') is not None else 0.0,
                            float(p['wager']) if p['wager'] is not None else 0.0
                        ]
                        for p in valid_predictions
                        if p['predicted_odds'] is not None 
                        and p['wager'] is not None
                    ])
                    
                    # Create results and closing line odds arrays
                    results = np.array([
                        [int(g['external_id']), int(g['outcome'])]
                        for g in day_games
                        if g['outcome'] not in ['Unfinished', None, 3]
                    ])
                    
                    closing_line_odds = np.array([
                        [int(g['external_id']), float(g['team_a_odds']), float(g['team_b_odds']), 
                         float(g['tie_odds']) if g['tie_odds'] else 0.0]
                        for g in day_games
                    ])

                    if pred_array.size > 0:
                        # Mark this day as having valid history for these miners
                        active_miners = np.unique(pred_array[:, 0].astype(int))
                        valid_history[active_miners, historical_day] = True
                        
                        # Calculate scores for this day
                        roi_scores = self._calculate_roi_scores(pred_array, results)
                        clv_scores = self._calculate_clv_scores(pred_array, closing_line_odds)
                        sortino_scores = self._calculate_risk_scores(pred_array, results)
                        
                        # Update score arrays
                        self.roi_scores[:, historical_day] = roi_scores
                        self.clv_scores[:, historical_day] = clv_scores
                        self.sortino_scores[:, historical_day] = sortino_scores
                        
                        # Add predictions to entropy system - only process predictions that have IDs
                        valid_predictions_with_ids = [
                            pred for pred in valid_predictions 
                            if 'prediction_id' in pred and pred['prediction_id'] is not None
                        ]
                        
                        if len(valid_predictions_with_ids) != len(valid_predictions):
                            bt.logging.warning(
                                f"Skipping {len(valid_predictions) - len(valid_predictions_with_ids)} predictions "
                                f"without prediction IDs for {process_date.date()}"
                            )
                        
                        # Add games to entropy system first and wait for all to complete
                        game_tasks = []
                        for game in day_games:
                            try:
                                odds = [
                                    float(game['team_a_odds']), 
                                    float(game['team_b_odds']), 
                                    float(game['tie_odds']) if game['tie_odds'] is not None else 0.0
                                ]
                                
                                # Initialize game with all possible outcomes
                                game_id = game['external_id']
                                num_outcomes = 3 if game['tie_odds'] else 2
                                
                                # First initialize the game pools for all possible outcomes
                                self.entropy_system.game_pools[game_id] = defaultdict(
                                    lambda: {"predictions": [], "entropy_score": 0.0}
                                )
                                for outcome in range(num_outcomes):
                                    self.entropy_system.game_pools[game_id][outcome] = {
                                        "predictions": [],
                                        "entropy_score": 0.0
                                    }
                                    
                                # Then add the game normally
                                task = self.entropy_system.add_new_game(
                                    game_id=game_id,
                                    num_outcomes=num_outcomes,
                                    odds=odds
                                )
                                game_tasks.append(task)
                                bt.logging.debug(f"Added game {game_id} with {num_outcomes} outcomes to entropy system")
                            except Exception as e:
                                bt.logging.error(f"Error adding game {game['external_id']} to entropy system: {e}")
                                continue
                        
                        # Wait for all games to be added
                        await asyncio.gather(*game_tasks)
                        bt.logging.info("All game tasks completed")
                        
                        for pred in valid_predictions_with_ids:
                            try:
                                self.entropy_system.add_prediction(
                                    prediction_id=pred['prediction_id'],
                                    miner_uid=pred['miner_uid'],
                                    game_id=pred['game_id'],
                                    predicted_outcome=pred['predicted_outcome'],
                                    wager=float(pred['wager']),
                                    predicted_odds=float(pred['predicted_odds']),
                                    prediction_date=pred['prediction_date'],
                                    historical_rebuild=True
                                )
                            except Exception as e:
                                bt.logging.error(
                                    f"Error adding prediction to entropy system:\n"
                                    f"  Prediction ID: {pred.get('prediction_id')}\n"
                                    f"  Game ID: {pred.get('game_id')}\n"
                                    f"  Miner: {pred.get('miner_uid')}\n"
                                    f"  Error: {str(e)}"
                                )
                                continue
                        
                        # Get entropy scores
                        entropy_scores = self.entropy_system.get_current_ebdr_scores(
                            datetime.combine(process_date, datetime.min.time()).replace(tzinfo=timezone.utc),
                            historical_day,
                            game_ids
                        )
                        
                        self.entropy_scores[:, historical_day] = entropy_scores[:, historical_day]
                        
                        # Update composite scores for this day
                        await self._update_composite_scores(historical_day)
                        
                        # After processing each day, ensure continuity in tier scores
                        for tier in range(1, 6):
                            window = self.tier_configs[tier + 1]["window"]
                            start_window = (historical_day - window + 1) % self.max_days
                            
                            # Check which miners have any history in this window
                            miners_with_history = np.any(valid_history[:, max(0, historical_day - window + 1):historical_day + 1], axis=1)
                            
                            if np.any(miners_with_history):
                                # Ensure all days in window have scores for these miners
                                for d in range(max(0, historical_day - window + 1), historical_day + 1):
                                    day_idx = d % self.max_days
                                    if not np.any(self.composite_scores[miners_with_history, day_idx, tier]):
                                        # Recalculate tier score for this day
                                        await self._update_composite_scores(day_idx)
                                        bt.logging.debug(f"Filled gap in tier {tier} scores for day {day_idx}")
                
                except Exception as e:
                    bt.logging.error(f"Error processing day {process_date.date()}: {str(e)}")
                    bt.logging.error(traceback.format_exc())
                    continue
            
            return True
            
        except Exception as e:
            bt.logging.error(f"Error in historical rebuild: {e}")
            bt.logging.error(traceback.format_exc())
            return False


    async def retroactively_calculate_entropy_scores(self):
        """Retroactively calculate entropy scores for all historical predictions."""
        try:
            # First get all games from the last 45 days
            cutoff_date = (datetime.now(timezone.utc) - timedelta(days=45)).isoformat()
            games_query = """
                SELECT 
                    id, external_id, team_a_odds, team_b_odds, tie_odds,
                    event_start_date, outcome
                FROM game_data 
                WHERE event_start_date >= ?
                ORDER BY event_start_date ASC
            """
            games = await self.db_manager.fetch_all(games_query, (cutoff_date,))
            bt.logging.info(f"Found {len(games)} games to process")

            # Add games to entropy system
            for game in games:
                try:
                    odds = [
                        float(game['team_a_odds']), 
                        float(game['team_b_odds']), 
                        float(game['tie_odds']) if game['tie_odds'] else 0.0
                    ]
                    await self.entropy_system.add_new_game(
                        game_id=game['external_id'],
                        num_outcomes=3 if game['tie_odds'] else 2,
                        odds=odds
                    )
                    
                    # If game is finished, mark it as closed
                    if game['outcome'] not in ['Unfinished', None]:
                        self.entropy_system.close_game(game['external_id'])
                        
                except Exception as e:
                    bt.logging.error(f"Error adding game {game['external_id']} to entropy system: {e}")
                    continue

            # Now get all predictions for these games
            predictions_query = """
                SELECT 
                    p.prediction_id, p.miner_uid, p.game_id, 
                    p.predicted_outcome, p.predicted_odds, p.wager,
                    p.prediction_date, p.processed
                FROM predictions p
                JOIN game_data g ON p.game_id = g.external_id
                WHERE g.event_start_date >= ?
                ORDER BY p.prediction_date ASC
            """
            predictions = await self.db_manager.fetch_all(predictions_query, (cutoff_date,))
            bt.logging.info(f"Found {len(predictions)} predictions to process")

            # Process predictions in chunks to avoid memory issues
            chunk_size = 1000
            for i in range(0, len(predictions), chunk_size):
                chunk = predictions[i:i+chunk_size]
                bt.logging.info(f"Processing predictions chunk {i//chunk_size + 1}")
                
                for pred in chunk:
                    try:
                        self.entropy_system.add_prediction(
                            prediction_id=pred['prediction_id'],
                            miner_uid=pred['miner_uid'],
                            game_id=pred['game_id'],
                            predicted_outcome=pred['predicted_outcome'],
                            wager=float(pred['wager']),
                            predicted_odds=float(pred['predicted_odds']),
                            prediction_date=pred['prediction_date']
                        )
                    except Exception as e:
                        bt.logging.error(f"Error processing prediction {pred['prediction_id']}: {e}")
                        continue

            # Save the entropy system state
            self.entropy_system.save_state()
            bt.logging.info("Completed retroactive entropy score calculation")

        except Exception as e:
            bt.logging.error(f"Error in retroactive entropy calculation: {e}")
            bt.logging.error(traceback.format_exc())

    async def save_scores_for_day(self, day_id):
        """Save scores for a specific day to the database."""
        try:
            # First ensure miner_stats entries exist for all miners
            miner_stats_query = """
            INSERT OR IGNORE INTO miner_stats (miner_uid)
            VALUES (?)
            """
            miner_stats_params = [(i,) for i in range(self.num_miners)]
            await self.db_manager.executemany(miner_stats_query, miner_stats_params)

            # Delete existing scores for this day
            await self.db_manager.execute_query(
                "DELETE FROM scores WHERE day_id = ?",
                (day_id,)
            )
            
            # Batch insert scores with UPSERT
            score_records = []
            for miner_id in range(self.num_miners):
                # Daily scores
                score_records.append(
                    miner_id, day_id, 'daily',
                    float(self.clv_scores[miner_id, day_id]),
                    float(self.roi_scores[miner_id, day_id]),
                    float(self.entropy_scores[miner_id, day_id]),
                    float(self.composite_scores[miner_id, day_id, 0]),
                    float(self.sortino_scores[miner_id, day_id])
                )
                # Tier-specific scores
                for tier_idx in range(1, self.composite_scores.shape[2]):
                    if tier_idx in self.tier_mapping:
                        score_records.append((
                            miner_id, day_id, self.tier_mapping[tier_idx],
                            None,  # clv_score
                            None,  # roi_score
                            None,  # entropy_score
                            float(self.composite_scores[miner_id, day_id, tier_idx]),
                            None   # sortino_score
                        ))

            # Insert with conflict resolution
            insert_query = """
            INSERT INTO scores 
                (miner_uid, day_id, score_type, clv_score, roi_score, entropy_score, composite_score, sortino_score)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(miner_uid, day_id, score_type) DO UPDATE SET
                clv_score=excluded.clv_score,
                roi_score=excluded.roi_score,
                entropy_score=excluded.entropy_score,
                composite_score=excluded.composite_score,
                sortino_score=excluded.sortino_score
            """
            
            await self.db_manager.executemany(insert_query, score_records)
            
        except Exception as e:
            bt.logging.error(f"Error saving scores for day {day_id}: {str(e)}")
            raise

    # Diagnostic query to understand miner_stats data
    async def debug_miner_stats(self):
        # Diagnostic query to understand miner_stats data
        debug_query = """
        SELECT 
            COUNT(*) as total_rows,
            COUNT(CASE WHEN miner_uid < 256 THEN 1 END) as valid_miners,
            COUNT(CASE WHEN miner_uid < 256 AND miner_last_prediction_date IS NOT NULL THEN 1 END) as miners_with_dates,
            COUNT(CASE WHEN miner_uid < 256 AND miner_lifetime_predictions > 0 THEN 1 END) as miners_with_predictions,
            COUNT(CASE WHEN miner_uid >= 256 THEN 1 END) as invalid_miners
        FROM miner_stats;
        """
        debug_stats = await self.db_manager.fetch_one(debug_query)
        bt.logging.info(f"Detailed stats: {debug_stats}")

        # Also let's see some sample data
        sample_query = """
        SELECT miner_uid, miner_last_prediction_date, miner_lifetime_predictions
        FROM miner_stats
        WHERE miner_uid < 256
        ORDER BY miner_last_prediction_date DESC
        LIMIT 5;
        """
        sample_data = await self.db_manager.fetch_all(sample_query)
        bt.logging.info(f"Sample miner data: {sample_data}")

    @property
    def tier_capacities(self):
        """Get the capacity for each tier."""
        return [config["capacity"] for config in self.tier_configs]

    def check_tier_requirements(self, miner_uid, tier, window_size, min_wager, min_active_days_pct):
        """Check if a miner meets the requirements for a specific tier"""
        min_active_days = int(window_size * min_active_days_pct)
        
        bt.logging.trace(f"Checking tier {tier} requirements for miner {miner_uid}:")
        bt.logging.trace(f"Window size: {window_size} days")
        bt.logging.trace(f"Minimum wager required: {min_wager}")
        bt.logging.trace(f"Minimum active days required: {min_active_days} ({min_active_days_pct*100}% of window)")

        # Get wager details
        cumulative_wager, daily_wagers = self.get_wager_details(miner_uid, window_size)
        avg_daily_wager = cumulative_wager / window_size if window_size > 0 else 0
        days_with_wagers = sum(1 for wager in daily_wagers if wager > 0)

        bt.logging.trace("Wager details:")
        bt.logging.trace(f"Cumulative wager: {cumulative_wager:.2f}/{min_wager}")
        bt.logging.trace(f"Days with wagers: {days_with_wagers}/{window_size}")
        bt.logging.trace(f"Average daily wager: {avg_daily_wager:.2f}")
        bt.logging.trace(f"Daily wagers: {[f'{w:.2f}' for w in daily_wagers]}")

        # Get activity details
        days_with_scores, daily_scores = self.get_activity_details(miner_uid, window_size)
        
        bt.logging.trace("Activity details:")
        bt.logging.trace(f"Days with scores: {days_with_scores}/{window_size}")
        bt.logging.trace(f"Required active days: {min_active_days}")
        bt.logging.trace(f"Daily scores: {[f'{s:.4f}' for s in daily_scores]}")

        # Check requirements
        meets_wager = cumulative_wager >= min_wager
        meets_history = days_with_scores >= min_active_days

        bt.logging.trace("Eligibility result:")
        bt.logging.trace(f"Meets wager requirement: {meets_wager}")
        bt.logging.trace(f"Meets history requirement: {meets_history}")

        return meets_wager and meets_history

    async def run_scoring(self, date=None):
        """Run scoring for a specific date or today"""
        date = date or datetime.now(timezone.utc).date()
        bt.logging.trace(f"=== Starting scoring run for date: {date} ===")
        
        # ... rest of the method ...
        
        date_str = date.strftime("%Y-%m-%d")
        bt.logging.trace(f"=== Completed scoring run for date: {date_str} ===")
