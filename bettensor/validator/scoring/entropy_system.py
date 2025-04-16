import json
from sqlite3 import OperationalError
import traceback
import numpy as np
import math
import bittensor as bt
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Tuple
from scipy.spatial.distance import euclidean
import os
import asyncio
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
import async_timeout
import copy
import random



class EntropySystem:
    state_file_path: str = "./bettensor/validator/state/entropy_system_state.json"
    def __init__(
        self,
        num_miners: int,
        max_days: int,
        db_manager=None  # Add db_manager parameter with default None
    ):
        """
        Initialize the EntropySystem object.

        Args:
            num_miners (int): Maximum number of miners.
            max_days (int): Maximum number of days to store scores.
            db_manager (DatabaseManager, optional): Database manager instance.
        """
        self.num_miners = num_miners
        self.max_days = max_days
        self.current_day = 0
        self.ebdr_scores = np.zeros((num_miners, max_days))
        self.game_outcome_entropies = {}
        self.prediction_counts = {}
        self.game_pools = defaultdict(
            lambda: defaultdict(lambda: {"predictions": [], "entropy_score": 0.0})
        )
        self.miner_predictions = defaultdict(lambda: defaultdict(list))
        self.epsilon = 1e-8
        self.closed_games = set()
        self.game_close_times = {}
        self.db_manager = db_manager  # Store db_manager reference

        # Add tracking for changes since last save
        self._changes_since_save = {
            'new_predictions': set(),  # Set of (game_id, outcome, prediction_id)
            'updated_pools': set(),    # Set of (game_id, outcome)
            'new_closed_games': set(), # Set of game_ids
            'updated_miner_scores': set(), # Set of (day, miner_uid)
        }
        
        # Attempt to load existing state
        asyncio.create_task(self.load_state())

    def _get_array_for_day(self, array: np.ndarray, day: int) -> np.ndarray:
        return array[:, day % self.max_days]

    def _set_array_for_day(self, array: np.ndarray, day: int, values: np.ndarray):
        array[:, day % self.max_days] = values

    async def add_new_game(self, game_id: int, num_outcomes: int, odds: List[float]):
        if game_id in self.game_pools:
            bt.logging.warning(f"Game {game_id} already exists. Skipping.")
            return

        self.game_pools[game_id] = {}

        if len(odds) == 3 and odds[2] == 0.0:
            num_outcomes = 2
        else:
            num_outcomes = len(odds)

        for i in range(num_outcomes):
            if i < len(odds):
                self.game_pools[game_id][i] = {
                    "predictions": [],
                    "entropy_score": self.calculate_initial_entropy(odds[i]),
                }
            else:
                bt.logging.warning(
                    f"Odds not provided for outcome {i} in game {game_id}. Outcome pool not initialized."
                )
                # Outcome pool is not initialized

        bt.logging.debug(
            f"Added new game {game_id} with {num_outcomes} outcomes. Odds: {odds}"
        )

    def close_game(self, game_id: int):
        """
        Mark a game as closed and clear its prediction pools.

        Args:
            game_id (int): Unique identifier for the game.
        """
        if game_id not in self.game_pools:
            bt.logging.error(f"Game {game_id} does not exist. Cannot close.")
            return

        if game_id in self.closed_games:
            bt.logging.warning(f"Game {game_id} is already closed.")
            return

        # Mark the game as closed with timezone-aware datetime
        self.closed_games.add(game_id)
        self.game_close_times[game_id] = datetime.now(timezone.utc)
        bt.logging.info(f"Game {game_id} has been marked as closed.")

        # Track newly closed game
        self._changes_since_save['new_closed_games'].add(game_id)

    def add_prediction(
        self, 
        prediction_id, 
        miner_uid, 
        game_id, 
        predicted_outcome, 
        wager, 
        predicted_odds, 
        prediction_date,
        historical_rebuild: bool = False
    ):
        """Add a prediction to the entropy system and calculate its contribution."""
        try:
            if game_id not in self.game_pools:
                bt.logging.error(f"Game {game_id} does not exist. Cannot add prediction {prediction_id}")
                return

            if predicted_outcome not in self.game_pools[game_id]:
                bt.logging.error(f"Invalid outcome {predicted_outcome} for game {game_id}")
                bt.logging.warning(f"Game pools: {self.game_pools[game_id]}")
                return

            # Only check for closed games during live operation, not during rebuilds
            if not historical_rebuild and game_id in self.closed_games:
                bt.logging.warning(f"Game {game_id} is closed. Cannot add prediction.")
                return

            # Ensure prediction_date is timezone-aware
            if isinstance(prediction_date, str):
                prediction_date = datetime.fromisoformat(prediction_date.replace('Z', '+00:00'))
            if prediction_date.tzinfo is None:
                prediction_date = prediction_date.replace(tzinfo=timezone.utc)

            entropy_contribution = self.calculate_entropy_contribution(
                game_id, predicted_outcome, miner_uid, predicted_odds, wager, prediction_date
            )
            
            if historical_rebuild:
                bt.logging.debug(
                    f"Adding historical prediction: game={game_id}, miner={miner_uid}, "
                    f"outcome={predicted_outcome}, contribution={entropy_contribution:.6f}"
                )
            else:
                bt.logging.debug(
                    f"Adding prediction: game={game_id}, miner={miner_uid}, "
                    f"outcome={predicted_outcome}, contribution={entropy_contribution:.6f}"
                )

            self.game_pools[game_id][predicted_outcome]["predictions"].append({
                "prediction_id": prediction_id,
                "miner_uid": miner_uid,
                "odds": predicted_odds,
                "wager": wager,
                "prediction_date": prediction_date,
                "entropy_contribution": entropy_contribution,
            })
            
            # Track this new prediction
            self._changes_since_save['new_predictions'].add((game_id, predicted_outcome, prediction_id))
            self._changes_since_save['updated_pools'].add((game_id, predicted_outcome))
            
        except Exception as e:
            bt.logging.error(f"Error in add_prediction: {e}")
            bt.logging.error(traceback.format_exc())

    def calculate_prediction_similarity(
        self, 
        game_id: int, 
        outcome: int, 
        miner_uid: int, 
        odds: float,
        prediction_date: datetime,
        wager: float
    ) -> float:
        """
        Calculate how similar this prediction is to existing ones.
        Higher similarity should lead to higher penalties.
        
        Returns:
            float: Similarity score between 0 (unique) and 1 (very similar)
        """
        try:
            pool = self.game_pools[game_id][outcome]
            existing_predictions = pool["predictions"]
            
            if not existing_predictions:
                return 0.0  # First prediction is unique
                
            # Ensure prediction_date is timezone-aware
            if prediction_date.tzinfo is None:
                prediction_date = prediction_date.replace(tzinfo=timezone.utc)
                
            # Get all prediction times
            prediction_times = [
                pred["prediction_date"] if isinstance(pred["prediction_date"], datetime)
                else datetime.fromisoformat(pred["prediction_date"].replace('Z', '+00:00'))
                for pred in existing_predictions
            ]
            
            # Ensure all times are timezone-aware
            prediction_times = [
                pt.replace(tzinfo=timezone.utc) if pt.tzinfo is None else pt
                for pt in prediction_times
            ]
            
            # Calculate time-based similarity
            time_diffs = [(prediction_date - pt).total_seconds() for pt in prediction_times]
            time_similarity = np.mean([1.0 / (1.0 + abs(td)/3600) for td in time_diffs])  # Decay over hours
            
            # Calculate odds similarity
            existing_odds = [float(pred["odds"]) for pred in existing_predictions]
            odds_diffs = [abs(odds - eo) for eo in existing_odds]
            odds_similarity = np.mean([1.0 / (1.0 + diff) for diff in odds_diffs])
            
            # Calculate wager similarity
            existing_wagers = [float(pred["wager"]) for pred in existing_predictions]
            wager_diffs = [abs(wager - ew)/max(wager, ew) for ew in existing_wagers]
            wager_similarity = np.mean([1.0 - diff for diff in wager_diffs])
            
            # Combine similarities with weights
            similarity = (
                0.5 * time_similarity +   # Time is most important (50%)
                0.4 * wager_similarity +  # Wager is second most important (40%)
                0.1 * odds_similarity     # Odds are least important (10%)
            )
            
            bt.logging.debug(
                f"Prediction similarity for game {game_id}:\n"
                f"  time={time_similarity:.3f} (weight: 0.5)\n"
                f"  wager={wager_similarity:.3f} (weight: 0.4)\n"
                f"  odds={odds_similarity:.3f} (weight: 0.1)\n"
                f"  combined={similarity:.3f}"
            )
            
            return max(0.0, min(1.0, similarity))
            
        except Exception as e:
            bt.logging.error(f"Error calculating prediction similarity: {e}")
            bt.logging.error(traceback.format_exc())
            return 0.0

    def calculate_entropy_contribution(
        self, 
        game_id: int, 
        predicted_outcome: int, 
        miner_uid: int, 
        predicted_odds: float,
        wager: float,
        prediction_date: str
    ) -> float:
        """
        Calculate the entropy contribution for a prediction.
        High similarity should result in penalties (negative scores).
        Score range is expanded to [-10, 10].
        
        Returns:
            float: The entropy contribution score between -10 and 10
        """
        # Calculate similarity (0 to 1)
        prediction_similarity = self.calculate_prediction_similarity(
            game_id=game_id,
            outcome=predicted_outcome,
            miner_uid=miner_uid,
            odds=predicted_odds,
            prediction_date=prediction_date,
            wager=wager
        )
        
        # Invert similarity to make it a penalty
        # High similarity (1.0) becomes high penalty (-1.0)
        similarity_penalty = -(prediction_similarity)
        
        # Calculate contrarian component (-1 to 1)
        contrarian_component = self.calculate_contrarian_component(
            game_id, predicted_outcome, miner_uid
        )
        
        # Combine components with weights
        # Note: similarity_penalty is already negative for high similarity
        raw_contribution = 0.6 * similarity_penalty + 0.4 * contrarian_component
        
        # Get all contributions in this pool for proper normalization
        pool = self.game_pools[game_id][predicted_outcome]
        all_contributions = [
            pred["entropy_contribution"] 
            for pred in pool["predictions"]
        ] + [raw_contribution]
        
        # Normalize using the full range of contributions
        if len(all_contributions) > 1:
            min_contrib = min(all_contributions)
            max_contrib = max(all_contributions)
            range_contrib = max_contrib - min_contrib
            if range_contrib > 0:
                normalized_contribution = (raw_contribution - min_contrib) / range_contrib
                # Scale to [-10, 10] range
                final_contribution = (normalized_contribution * 20) - 10
            else:
                final_contribution = 0.0
        else:
            # First prediction in pool
            final_contribution = 0.0
        
        bt.logging.debug(
            f"Entropy calculation for game {game_id}:\n"
            f"  similarity={prediction_similarity:.3f}\n"
            f"  similarity_penalty={similarity_penalty:.3f}\n"
            f"  contrarian={contrarian_component:.3f}\n"
            f"  raw_contribution={raw_contribution:.3f}\n"
            f"  final_contribution={final_contribution:.3f}"
        )
        
        return final_contribution

    def calculate_contrarian_component(self, game_id, predicted_outcome, miner_uid):
        total_predictions = sum(
            len(pool["predictions"]) for pool in self.game_pools[game_id].values()
        )
        if total_predictions == 0:
            return 0.5  # Neutral score for the first prediction

        outcome_predictions = len(self.game_pools[game_id][predicted_outcome]["predictions"])
        outcome_ratio = outcome_predictions / total_predictions

        # Direct inverse relationship
        contrarian_score = 1 - outcome_ratio

        # Ensure the score is between 0 and 1
        contrarian_score = max(0, min(1, contrarian_score))

        # Adjust the scale to make it more granular
        # If outcome_ratio is less than 0.1 (10%), contrarian_score will be >= 0.9
        adjusted_score = contrarian_score**0.5  # This makes the scale more granular

        bt.logging.debug(
            f"Contrarian calculation for game {game_id}, outcome {predicted_outcome}:"
        )
        bt.logging.debug(f"  Total predictions: {total_predictions}")
        bt.logging.debug(f"  Outcome predictions: {outcome_predictions}")
        bt.logging.debug(f"  Outcome ratio: {outcome_ratio:.4f}")
        bt.logging.debug(f"  Contrarian score: {adjusted_score:.4f}")

        return (
            adjusted_score - 0.5
        )  # Center around 0 for consistency with other components

    def calculate_final_entropy_score(self, miner_uid: int) -> float:
        """
        Calculate the final entropy score for a miner for the current day.

        Args:
            miner_id (int): ID of the miner.

        Returns:
            float: Final entropy score.
        """
        contributions = self.miner_predictions[self.current_day].get(miner_uid, [])
        final_score = sum(contributions)
        bt.logging.info(
            f"Final entropy score for miner {miner_uid} on day {self.current_day}: {final_score:.4f}"
        )
        return final_score

    def calculate_final_entropy_scores_for_miners(self) -> Dict[int, float]:
        """
        Calculate and return the final entropy scores for all miners for the current day.

        Returns:
            Dict[int, float]: Mapping from miner ID to their final entropy score.
        """
        final_scores = {}
        for miner_uid in range(self.num_miners):
            score = self.calculate_final_entropy_score(miner_uid)
            final_scores[miner_uid] = score
        return final_scores

    def reset_predictions_for_closed_games(self, current_date: datetime):
        """
        Reset predictions for games that are older than 45 days.
        
        Args:
            current_date (datetime): The current date to calculate age from.
        """
        # Ensure current_date is timezone-aware
        if current_date.tzinfo is None:
            current_date = current_date.replace(tzinfo=timezone.utc)
            
        cutoff_date = current_date - timedelta(days=45)
        
        for game_id in list(self.closed_games):
            game_close_time = self.game_close_times.get(game_id)
            if game_close_time:
                # Ensure game_close_time is timezone-aware
                if game_close_time.tzinfo is None:
                    game_close_time = game_close_time.replace(tzinfo=timezone.utc)
                    
                if game_close_time < cutoff_date:
                    # Remove old predictions but keep the game structure
                    for outcome, pool in self.game_pools[game_id].items():
                        pool["predictions"].clear()
                        pool["entropy_score"] = 0.0
                        bt.logging.debug(f"Cleared old predictions for game {game_id}, outcome {outcome}")
                        
                    # Update tracking sets/dicts
                    self.closed_games.remove(game_id)
                    del self.game_close_times[game_id]
                    bt.logging.debug(f"Removed tracking for old game {game_id}")

    def calculate_initial_entropy(self, initial_odds: float) -> float:
        """
        Calculate the initial entropy for a game outcome based on the odds.

        Args:
            initial_odds (float): Odds for the outcome.

        Returns:
            float: Initial entropy value.
        """
        # Handle invalid odds (negative or zero)
        if initial_odds <= 0:
            return 0.0
        
        prob = 1 / (initial_odds + self.epsilon)
        # Ensure probability is in valid range [0,1]
        prob = max(0, min(1, prob))
        entropy = -prob * math.log2(prob + self.epsilon)
        result = max(entropy, self.epsilon)
        bt.logging.debug(
            f"Initial entropy calculation: odds={initial_odds}, result={result}"
        )
        return result

    def get_current_ebdr_scores(
        self, current_date: datetime, current_day: int, game_ids: List[int]
    ) -> np.ndarray:
        """
        Get EBDR scores for all miners across the scoring window.
        """
        bt.logging.debug(f"Getting EBDR scores for day {current_day}")
        
        # Initialize scores array for all days
        ebdr_scores = np.zeros((self.num_miners, self.max_days))
        
        # First, calculate contributions for each miner from active games
        miner_contributions = defaultdict(float)
        
        for game_id in game_ids:
            if game_id in self.game_pools:
                for outcome, pool in self.game_pools[game_id].items():
                    for prediction in pool["predictions"]:
                        miner_uid = int(prediction["miner_uid"])
                        contribution = prediction["entropy_contribution"]
                        miner_contributions[miner_uid] += contribution
        
        # Store contributions in miner_predictions for the current day
        self.miner_predictions[current_day] = dict(miner_contributions)
        # Track updated miner scores
        for miner_uid in miner_contributions:
            self._changes_since_save['updated_miner_scores'].add((current_day, miner_uid))
        
        # Now populate the scores array using historical data
        for day in range(self.max_days):
            if day in self.miner_predictions:
                for miner_uid, score in self.miner_predictions[day].items():
                    ebdr_scores[miner_uid, day] = score
        
        # Normalize scores for each day
        for day in range(self.max_days):
            max_score = np.max(ebdr_scores[:, day])
            if max_score > 0:
                ebdr_scores[:, day] /= max_score
        
        bt.logging.info(f"EBDR scores calculated for day {current_day}")
        bt.logging.debug(f"Non-zero scores: {np.count_nonzero(ebdr_scores)}")
        bt.logging.debug(f"Max score: {np.max(ebdr_scores)}")
        bt.logging.debug(f"Mean score: {np.mean(ebdr_scores)}")
        
        # Clean up old predictions but keep last 45 days
        self.reset_predictions_for_closed_games(current_date)
        
        return ebdr_scores

    async def save_state(self):
        """Save the current state to both database and file with retry logic"""
        max_retries = 5  # Increased from 3
        base_delay = 1
        
        # Save current state for rollback
        state_backup = {
            'game_pools': copy.deepcopy(self.game_pools),
            'closed_games': copy.deepcopy(self.closed_games),
            'game_close_times': copy.deepcopy(self.game_close_times),
            'miner_predictions': copy.deepcopy(self.miner_predictions),
            'changes_since_save': copy.deepcopy(self._changes_since_save)
        }
        
        for attempt in range(max_retries):
            try:
                # Use timeout for database save
                async with async_timeout.timeout(60):  # Increased timeout for entire save operation
                    # First save to database
                    if await self._save_to_database():
                        # Then save to file
                        self._save_to_file()
                        return True
                        
                    if attempt < max_retries - 1:
                        # Exponential backoff with jitter
                        delay = base_delay * (2 ** attempt) + random.uniform(0, 1)
                        bt.logging.warning(f"Save attempt {attempt + 1} failed, retrying in {delay:.1f}s...")
                        
                        # Restore state before retry
                        self.game_pools = copy.deepcopy(state_backup['game_pools'])
                        self.closed_games = copy.deepcopy(state_backup['closed_games'])
                        self.game_close_times = copy.deepcopy(state_backup['game_close_times'])
                        self.miner_predictions = copy.deepcopy(state_backup['miner_predictions'])
                        self._changes_since_save = copy.deepcopy(state_backup['changes_since_save'])
                        
                        await asyncio.sleep(delay)
                        continue
                        
            except asyncio.TimeoutError:
                bt.logging.error("Timeout saving entropy system state")
                if attempt < max_retries - 1:
                    delay = base_delay * (2 ** attempt) + random.uniform(0, 1)
                    bt.logging.warning(f"Retrying in {delay:.1f}s...")
                    
                    # Restore state before retry
                    self.game_pools = copy.deepcopy(state_backup['game_pools'])
                    self.closed_games = copy.deepcopy(state_backup['closed_games'])
                    self.game_close_times = copy.deepcopy(state_backup['game_close_times'])
                    self.miner_predictions = copy.deepcopy(state_backup['miner_predictions'])
                    self._changes_since_save = copy.deepcopy(state_backup['changes_since_save'])
                    
                    await asyncio.sleep(delay)
                    continue
                raise
            except Exception as e:
                bt.logging.error(f"Error saving entropy system state: {str(e)}")
                bt.logging.error(traceback.format_exc())
                if attempt < max_retries - 1:
                    delay = base_delay * (2 ** attempt) + random.uniform(0, 1)
                    bt.logging.warning(f"Retrying in {delay:.1f}s...")
                    
                    # Restore state before retry
                    self.game_pools = copy.deepcopy(state_backup['game_pools'])
                    self.closed_games = copy.deepcopy(state_backup['closed_games'])
                    self.game_close_times = copy.deepcopy(state_backup['game_close_times'])
                    self.miner_predictions = copy.deepcopy(state_backup['miner_predictions'])
                    self._changes_since_save = copy.deepcopy(state_backup['changes_since_save'])
                    
                    await asyncio.sleep(delay)
                    continue
                raise
                    
        # If all retries failed, restore original state
        self.game_pools = copy.deepcopy(state_backup['game_pools'])
        self.closed_games = copy.deepcopy(state_backup['closed_games'])
        self.game_close_times = copy.deepcopy(state_backup['game_close_times'])
        self.miner_predictions = copy.deepcopy(state_backup['miner_predictions'])
        self._changes_since_save = copy.deepcopy(state_backup['changes_since_save'])
        
        return False

    async def _save_to_database(self):
        """Save only changed state to database tables using batch operations"""
        try:
            if not self.db_manager:
                return False

            # Use a shorter timeout for the database operation
            async with async_timeout.timeout(120):  # Increased timeout to 120 seconds
                async with self.db_manager.get_long_running_session() as session:
                    from sqlalchemy import text
                    
                    # Save system state (always save this as it's small)
                    is_postgres = hasattr(self.db_manager, 'is_postgresql') and self.db_manager.is_postgresql()
                    
                    if is_postgres:
                        await session.execute(
                            text("""
                                INSERT INTO entropy_system_state 
                                (id, current_day, num_miners, max_days, last_processed_date)
                                VALUES (1, :current_day::INTEGER, :num_miners::INTEGER, :max_days::INTEGER, CURRENT_TIMESTAMP)
                                ON CONFLICT (id) DO UPDATE SET
                                    current_day = EXCLUDED.current_day,
                                    num_miners = EXCLUDED.num_miners,
                                    max_days = EXCLUDED.max_days,
                                    last_processed_date = CURRENT_TIMESTAMP
                            """),
                            {
                                "current_day": self.current_day,
                                "num_miners": self.num_miners,
                                "max_days": self.max_days
                            }
                        )
                    else:
                        await session.execute(
                            text("""
                                INSERT INTO entropy_system_state 
                                (id, current_day, num_miners, max_days, last_processed_date)
                                VALUES (1, :current_day, :num_miners, :max_days, CURRENT_TIMESTAMP)
                                ON CONFLICT (id) DO UPDATE SET
                                    current_day = EXCLUDED.current_day,
                                    num_miners = EXCLUDED.num_miners,
                                    max_days = EXCLUDED.max_days,
                                    last_processed_date = EXCLUDED.last_processed_date
                            """),
                            {
                                "current_day": self.current_day,
                                "num_miners": self.num_miners,
                                "max_days": self.max_days
                            }
                        )
                    
                    # Prepare all batch updates first
                    updates = {
                        'pools': [],
                        'predictions': [],
                        'scores': [],
                        'closed_games': []
                    }
                    
                    # Collect updates with progress logging
                    total_updates = (
                        len(self._changes_since_save['updated_pools']) +
                        len(self._changes_since_save['new_predictions']) +
                        len(self._changes_since_save['updated_miner_scores']) +
                        len(self._changes_since_save['new_closed_games'])
                    )
                    
                    if total_updates > 0:
                        bt.logging.info(f"Preparing to save {total_updates} updates")
                    
                    # Collect updates for game pools
                    for game_id, outcome in self._changes_since_save['updated_pools']:
                        pool = self.game_pools[game_id][outcome]
                        updates['pools'].append({
                            'game_id': game_id,
                            'outcome': outcome,
                            'entropy_score': float(pool['entropy_score'])
                        })
                    
                    # Collect updates for predictions
                    for game_id, outcome, prediction_id in self._changes_since_save['new_predictions']:
                        for pred in self.game_pools[game_id][outcome]['predictions']:
                            if pred['prediction_id'] == prediction_id:
                                # Serialize datetime to string if needed
                                pred_date = pred['prediction_date']
                                if isinstance(pred_date, datetime):
                                    pred_date = pred_date.isoformat()
                                    
                                updates['predictions'].append({
                                    'prediction_id': prediction_id,
                                    'game_id': int(game_id), 
                                    'outcome': int(outcome),
                                    'miner_uid': str(pred['miner_uid']),  # Convert to string for PostgreSQL
                                    'odds': float(pred['odds']),
                                    'wager': float(pred['wager']),
                                    'prediction_date': pred_date,
                                    'entropy_contribution': float(pred['entropy_contribution'])
                                })
                                break
                    
                    # Collect updates for miner scores
                    for day, miner_uid in self._changes_since_save['updated_miner_scores']:
                        if day in self.miner_predictions and miner_uid in self.miner_predictions[day]:
                            updates['scores'].append({
                                'miner_uid': int(miner_uid),
                                'day': int(day),
                                'contribution': float(self.miner_predictions[day][miner_uid])
                            })
                    
                    # Collect updates for closed games
                    for game_id in self._changes_since_save['new_closed_games']:
                        close_time = self.game_close_times.get(game_id)
                        if close_time:
                            # Serialize datetime to string if needed
                            if isinstance(close_time, datetime):
                                close_time = close_time.isoformat()
                            updates['closed_games'].append({
                                'game_id': game_id,
                                'close_time': close_time
                            })
                    
                    # Execute all batch updates with smaller batch sizes and progress logging
                    batch_size = 25
                    for update_type, items in updates.items():
                        if not items:
                            continue
                            
                        total_batches = (len(items) + batch_size - 1) // batch_size
                        bt.logging.debug(f"Processing {len(items)} {update_type} updates in {total_batches} batches")
                        
                        for i in range(0, len(items), batch_size):
                            batch = items[i:i + batch_size]
                            batch_num = i // batch_size + 1
                            
                            try:
                                if update_type == 'pools':
                                    if is_postgres:
                                        await session.execute(
                                            text("""INSERT INTO entropy_game_pools 
                                                   (game_id, outcome, entropy_score) 
                                                   VALUES (:game_id::INTEGER, :outcome::INTEGER, :entropy_score::FLOAT)
                                                   ON CONFLICT (game_id, outcome) DO UPDATE SET
                                                   entropy_score = EXCLUDED.entropy_score"""),
                                            batch
                                        )
                                    else:
                                        await session.execute(
                                            text("""INSERT OR REPLACE INTO entropy_game_pools 
                                                   (game_id, outcome, entropy_score) 
                                                   VALUES (:game_id, :outcome, :entropy_score)"""),
                                            batch
                                        )
                                elif update_type == 'predictions':
                                    if is_postgres:
                                        await session.execute(
                                            text("""INSERT INTO entropy_predictions 
                                                   (prediction_id, game_id, outcome, miner_uid, odds, 
                                                    wager, prediction_date, entropy_contribution)
                                                   VALUES (:prediction_id, :game_id::INTEGER, :outcome::INTEGER, :miner_uid::INTEGER, 
                                                          :odds::FLOAT, :wager::FLOAT, :prediction_date, :entropy_contribution::FLOAT)
                                                   ON CONFLICT (prediction_id) DO UPDATE SET
                                                   game_id = EXCLUDED.game_id,
                                                   outcome = EXCLUDED.outcome,
                                                   miner_uid = EXCLUDED.miner_uid,
                                                   odds = EXCLUDED.odds,
                                                   wager = EXCLUDED.wager,
                                                   prediction_date = EXCLUDED.prediction_date,
                                                   entropy_contribution = EXCLUDED.entropy_contribution"""),
                                            batch
                                        )
                                    else:
                                        await session.execute(
                                            text("""INSERT OR REPLACE INTO entropy_predictions 
                                                   (prediction_id, game_id, outcome, miner_uid, odds, 
                                                    wager, prediction_date, entropy_contribution)
                                                   VALUES (:prediction_id, :game_id, :outcome, :miner_uid, 
                                                          :odds, :wager, :prediction_date, :entropy_contribution)"""),
                                            batch
                                        )
                                elif update_type == 'scores':
                                    if is_postgres:
                                        await session.execute(
                                            text("""INSERT INTO entropy_miner_scores
                                                   (miner_uid, day, contribution) 
                                                   VALUES (:miner_uid::INTEGER, :day::INTEGER, :contribution::FLOAT)
                                                   ON CONFLICT (miner_uid, day) DO UPDATE SET
                                                   contribution = EXCLUDED.contribution"""),
                                            batch
                                        )
                                    else:
                                        await session.execute(
                                            text("""INSERT OR REPLACE INTO entropy_miner_scores
                                                   (miner_uid, day, contribution) 
                                                   VALUES (:miner_uid, :day, :contribution)"""),
                                            batch
                                        )
                                elif update_type == 'closed_games':
                                    if is_postgres:
                                        await session.execute(
                                            text("""INSERT INTO entropy_closed_games
                                                   (game_id, close_time) 
                                                   VALUES (:game_id::INTEGER, :close_time)
                                                   ON CONFLICT (game_id) DO UPDATE SET
                                                   close_time = EXCLUDED.close_time"""),
                                            batch
                                        )
                                    else:
                                        await session.execute(
                                            text("""INSERT OR REPLACE INTO entropy_closed_games
                                                   (game_id, close_time) 
                                                   VALUES (:game_id, :close_time)"""),
                                            batch
                                        )
                                
                                # Commit after each batch
                                await session.commit()
                                bt.logging.debug(f"Completed batch {batch_num}/{total_batches} for {update_type}")
                                
                            except Exception as e:
                                bt.logging.error(f"Error saving batch {batch_num} of {update_type}: {e}")
                                raise
                    
                    # Clear change tracking after successful save
                    self._changes_since_save = {
                        'new_predictions': set(),
                        'updated_pools': set(),
                        'new_closed_games': set(),
                        'updated_miner_scores': set(),
                    }
                    
                    bt.logging.info("Successfully saved entropy system state delta to database")
                    return True
                    
        except asyncio.TimeoutError:
            bt.logging.error("Timeout during database save operation")
            return False
        except Exception as e:
            bt.logging.error(f"Error saving to database: {str(e)}")
            bt.logging.error(traceback.format_exc())
            return False

    def _save_to_file(self):
        """Original file-based save logic with proper datetime handling"""
        try:
            if os.path.exists(self.state_file_path):
                backup_path = f"{self.state_file_path}.backup"
                os.replace(self.state_file_path, backup_path)

            # Create a state dictionary with properly serialized datetime objects
            state = {
                "current_day": self.current_day,
                "game_outcome_entropies": self.game_outcome_entropies,
                "prediction_counts": self.prediction_counts,
                "ebdr_scores": self.ebdr_scores.tolist(),
                "game_pools": {
                    str(game_id): {
                        str(outcome): {
                            "predictions": [
                                {
                                    "prediction_id": pred["prediction_id"],
                                    "miner_uid": pred["miner_uid"],
                                    "odds": float(pred["odds"]),
                                    "wager": float(pred["wager"]),
                                    "prediction_date": pred["prediction_date"].isoformat() if isinstance(pred["prediction_date"], datetime) else pred["prediction_date"],
                                    "entropy_contribution": float(pred["entropy_contribution"])
                                }
                                for pred in pool["predictions"]
                            ],
                            "entropy_score": float(pool["entropy_score"])
                        }
                        for outcome, pool in outcomes.items()
                    }
                    for game_id, outcomes in self.game_pools.items()
                },
                "miner_predictions": {
                    str(day): {
                        str(miner_id): float(contributions)
                        for miner_id, contributions in miners.items()
                    }
                    for day, miners in self.miner_predictions.items()
                },
                "closed_games": list(self.closed_games),
                "game_close_times": {
                    str(game_id): game_close_time.isoformat() if isinstance(game_close_time, datetime) else game_close_time
                    for game_id, game_close_time in self.game_close_times.items()
                }
            }

            temp_path = f"{self.state_file_path}.tmp"
            with open(temp_path, "w") as f:
                json.dump(state, f, indent=2)

            # Verify the JSON is valid
            with open(temp_path, "r") as f:
                json.load(f)

            os.replace(temp_path, self.state_file_path)
            bt.logging.info(f"Successfully saved entropy system state to file")

        except Exception as e:
            bt.logging.error(f"Error saving to file: {str(e)}")
            bt.logging.error(traceback.format_exc())

    async def load_state(self):
        """Load state from database, falling back to file if needed"""
        try:
            # First try to load from database
            if await self._load_from_database():
                bt.logging.info("Successfully loaded state from database")
                return
                
            # If no database state, try to load from file and migrate
            if os.path.exists(self.state_file_path):
                bt.logging.info("No database state found, attempting to migrate from file")
                if self._load_from_file():
                    bt.logging.info("Successfully loaded state from file")
                    # Save to database for future use
                    await self._save_to_database()
                    return
                    
            bt.logging.warning("No existing state found, starting fresh")
            
        except Exception as e:
            bt.logging.error(f"Error loading state: {str(e)}")
            bt.logging.error(traceback.format_exc())

    async def _load_from_database(self) -> bool:
        """Load entropy system state from database."""
        try:
            async with self.db_manager.get_long_running_session() as session:
                # Determine if we're using PostgreSQL or SQLite
                is_postgres = hasattr(self.db_manager, 'dbname')
                
                # Load system state - first check what columns we actually have
                if is_postgres:
                    # PostgreSQL equivalent query
                    schema_query = """
                        SELECT column_name 
                        FROM information_schema.columns
                        WHERE table_name = 'entropy_system_state'
                    """
                else:
                    # SQLite original query
                    schema_query = """
                        SELECT sql FROM sqlite_master 
                        WHERE type='table' AND name='entropy_system_state'
                    """
                    
                try:
                    result = await session.execute(text(schema_query))
                    schema_row = result.first()
                    if schema_row:
                        schema_row = dict(zip(result.keys(), schema_row))
                        if is_postgres:
                            bt.logging.debug(f"Entropy system state columns available: {schema_row}")
                        else:
                            bt.logging.debug(f"Entropy system state schema: {schema_row['sql']}")
                except Exception as schema_error:
                    bt.logging.warning(f"Error checking schema, continuing anyway: {schema_error}")

                # Load system state with only the columns we know exist
                state_query = """
                    SELECT current_day, last_processed_date
                    FROM entropy_system_state 
                    WHERE id = 1
                """
                
                try:
                    result = await session.execute(text(state_query))
                    row = result.first()
                    if not row:
                        bt.logging.info("No entropy system state found in database")
                        return False
                except Exception as table_error:
                    bt.logging.error(f"Error querying entropy_system_state table: {table_error}")
                    return False

                # Convert row to dict
                row = dict(zip(result.keys(), row))

                # Access columns by name instead of index
                self.current_day = row['current_day']
                
                # Skip parameter verification since we don't store these in the DB
                # Just log the current values
                bt.logging.info(
                    f"Current system parameters:\n"
                    f"  Miners: {self.num_miners}\n"
                    f"  Max days: {self.max_days}"
                )

                # Load game pools with batching
                pools_query = """
                    SELECT game_id, outcome, entropy_score
                    FROM entropy_game_pools
                """
                result = await session.execute(text(pools_query))
                pools = [dict(zip(result.keys(), row)) for row in result.all()]
                
                # Initialize game pools
                self.game_pools = {}
                
                # Load predictions for each pool in batches
                batch_size = 100
                for i in range(0, len(pools), batch_size):
                    batch = pools[i:i + batch_size]
                    for pool in batch:
                        game_id = pool['game_id']
                        outcome = pool['outcome']
                        
                        if game_id not in self.game_pools:
                            self.game_pools[game_id] = {}
                        
                        predictions_query = """
                            SELECT prediction_id, miner_uid, odds, wager, 
                                   prediction_date, entropy_contribution
                            FROM entropy_predictions
                            WHERE game_id = :game_id AND outcome = :outcome
                        """
                        result = await session.execute(text(predictions_query), 
                                                     {'game_id': game_id, 'outcome': outcome})
                        predictions = [dict(zip(result.keys(), row)) for row in result.all()]
                        
                        self.game_pools[game_id][outcome] = {
                            "predictions": [
                                {
                                    "prediction_id": p['prediction_id'],
                                    "miner_uid": p['miner_uid'],
                                    "odds": p['odds'],
                                    "wager": p['wager'],
                                    "prediction_date": p['prediction_date'],
                                    "entropy_contribution": p['entropy_contribution']
                                }
                                for p in predictions
                            ],
                            "entropy_score": pool['entropy_score']
                        }

                # Load miner predictions in batches
                miner_scores_query = """
                    SELECT miner_uid, day, contribution
                    FROM entropy_miner_scores
                """
                result = await session.execute(text(miner_scores_query))
                miner_scores = [dict(zip(result.keys(), row)) for row in result.all()]
                
                # Initialize miner predictions
                self.miner_predictions = {}
                
                for score in miner_scores:
                    day = score['day']
                    miner_uid = score['miner_uid']
                    if day not in self.miner_predictions:
                        self.miner_predictions[day] = {}
                    self.miner_predictions[day][miner_uid] = score['contribution']

                # Load closed games
                closed_games_query = """
                    SELECT game_id, close_time
                    FROM entropy_closed_games
                """
                result = await session.execute(text(closed_games_query))
                closed_games = [dict(zip(result.keys(), row)) for row in result.all()]
                
                self.closed_games = set(g['game_id'] for g in closed_games)
                self.game_close_times = {
                    g['game_id']: datetime.fromisoformat(g['close_time']) 
                    if isinstance(g['close_time'], str) else g['close_time']
                    for g in closed_games
                }

                bt.logging.info(
                    f"Successfully loaded entropy system state from database:\n"
                    f"  Current day: {self.current_day}\n"
                    f"  Game pools: {len(self.game_pools)}\n"
                    f"  Closed games: {len(self.closed_games)}\n"
                    f"  Days with predictions: {len(self.miner_predictions)}"
                )
                return True

        except Exception as e:
            bt.logging.error(f"Error loading from database: {str(e)}")
            bt.logging.error(traceback.format_exc())
            return False

    def _load_from_file(self) -> bool:
        """Original file-based load logic. Returns True if successful."""
        try:
            if not os.path.exists(self.state_file_path):
                return False

            with open(self.state_file_path, "r") as f:
                state = json.load(f)

            self.current_day = state.get("current_day", 0)
            self.game_outcome_entropies = state.get("game_outcome_entropies", {})
            self.prediction_counts = state.get("prediction_counts", {})
            self.ebdr_scores = np.array(state.get("ebdr_scores", []))
            
            # Load game pools
            self.game_pools = defaultdict(dict)
            for game_id, outcomes in state["game_pools"].items():
                for outcome, pool in outcomes.items():
                    self.game_pools[int(game_id)][int(outcome)] = {
                        "predictions": [
                            {
                                "prediction_id": pred["prediction_id"],
                                "miner_uid": pred["miner_uid"],
                                "odds": pred["odds"],
                                "wager": pred["wager"],
                                "prediction_date": pred["prediction_date"],
                                "entropy_contribution": pred["entropy_contribution"],
                            }
                            for pred in pool["predictions"]
                        ],
                        "entropy_score": float(pool["entropy_score"]),
                    }
                    
            # Load other state components
            self.miner_predictions = {
                int(day): {
                    int(miner_id): float(contribution)
                    for miner_id, contribution in miners.items()
                }
                for day, miners in state["miner_predictions"].items()
            }
            
            self.closed_games = set(state.get("closed_games", []))
            self.game_close_times = {
                int(game_id): datetime.fromisoformat(game_close_time)
                for game_id, game_close_time in state.get("game_close_times", {}).items()
            }
            
            return True
            
        except Exception as e:
            bt.logging.error(f"Error loading from file: {str(e)}")
            bt.logging.error(traceback.format_exc())
            return False

    def _handle_corrupted_state(self):
        """Handle corrupted state file by attempting recovery or reset."""
        try:
            # Try to load backup if it exists
            backup_path = f"{self.state_file_path}.backup"
            if os.path.exists(backup_path):
                bt.logging.info("Attempting to restore from backup...")
                with open(backup_path, "r") as f:
                    state = json.load(f)
                os.replace(backup_path, self.state_file_path)
                bt.logging.info("Successfully restored from backup")
            else:
                bt.logging.warning("No backup found, resetting to fresh state")
                self.reset_state()
        except Exception as e:
            bt.logging.error(f"Error handling corrupted state: {str(e)}")
            self.reset_state()

    def reset_state(self):
        """Reset the entropy system state to initial values."""
        bt.logging.info("Resetting entropy system state")
        
        # Reset game pools
        self.game_pools = {}
        
        # Reset closed games tracking
        self.closed_games = set()
        
        # Reset game close times
        self.game_close_times = {}
        
        # Reset miner predictions tracking
        self.miner_predictions = {}
        
        # Reset any other state variables
        self.last_processed_date = None
        
        bt.logging.debug("Entropy system state has been reset")

    def get_state_summary(self):
        """Get a summary of the current entropy system state."""
        return {
            "num_active_games": len(self.game_pools) - len(self.closed_games),
            "num_closed_games": len(self.closed_games),
            "num_days_with_predictions": len(self.miner_predictions),
            "oldest_game_date": min(self.game_close_times.values()) if self.game_close_times else None,
            "newest_game_date": max(self.game_close_times.values()) if self.game_close_times else None
        }

    def log_state(self):
        """Log the current state of the entropy system."""
        state = self.get_state_summary()
        bt.logging.info("Entropy System State:")
        for key, value in state.items():
            bt.logging.info(f"  {key}: {value}")

    def get_delta_games(self):
        """Return only games that have been added or modified since last save"""
        return self._delta_games if hasattr(self, '_delta_games') else {}

    def get_delta_predictions(self):
        """Return only predictions that have been added since last save"""
        return self._delta_predictions if hasattr(self, '_delta_predictions') else {}

    def clear_delta(self):
        """Clear the delta tracking after successful save"""
        self._delta_games = {}
        self._delta_predictions = {}

