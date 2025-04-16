import os
import json
import time
from typing import List
import uuid
import sqlite3
import requests
import bittensor as bt
from dateutil import parser
from .sports_config import sports_config
from datetime import datetime, timedelta, timezone
from ..scoring.entropy_system import EntropySystem
from .bettensor_api_client import BettensorAPIClient
from ..database.postgres_database_manager import PostgresDatabaseManager
import traceback
import asyncio
import async_timeout
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql.expression import ColumnElement
from sqlalchemy import text


class SportsData:
    """
    SportsData class is responsible for fetching and updating sports data from either BettensorAPI or external API.
    """

    # Constants for chunking and timeouts
    TRANSACTION_TIMEOUT = 20  # Reduced to give more headroom for validator timeout
    CHUNK_SIZE = 5  # Smaller chunks for more granular processing
    ENTROPY_BATCH_SIZE = 2  # Very small batches for entropy updates
    MAX_RETRIES = 3
    
    def __init__(
        self,
        db_manager: PostgresDatabaseManager,
        entropy_system: EntropySystem,
        api_client,
    ):
        self.db_manager = db_manager
        self.entropy_system = entropy_system
        self.api_client = api_client
        self.all_games = []
        self._last_processed_time = None

    async def fetch_and_update_game_data(self, last_api_call):
        """Fetch and update game data with proper transaction and timeout handling"""
        try:
            # Check if we've processed this time period recently
            current_time = datetime.now(timezone.utc)
            if (self._last_processed_time and 
                (current_time - self._last_processed_time) < timedelta(minutes=5)):
                bt.logging.info("Skipping update - last update was less than 5 minutes ago")
                return []

            # Get all games that need updates
            all_games = await self.api_client.fetch_all_game_data(last_api_call)
            bt.logging.info(f"Fetched {len(all_games)} games from API")
            
            if not all_games:
                self._last_processed_time = current_time
                return []

            # Filter out invalid games
            valid_games = [game for game in all_games if isinstance(game, dict) and 'externalId' in game]
            bt.logging.info(f"Found {len(valid_games)} valid games")

            # Group games by date
            games_by_date = self._group_games_by_date(valid_games)
            if not games_by_date:
                self._last_processed_time = current_time
                return []

            # Process games in date order with independent error handling
            inserted_ids = []
            entropy_updates_needed = []
            
            for date, date_games in sorted(games_by_date.items()):
                bt.logging.info(f"Processing {len(date_games)} games for date {date}")
                
                # Process database updates for this date
                date_ids = await self._process_date_games(date, date_games)
                if date_ids:
                    inserted_ids.extend(date_ids)
                    entropy_updates_needed.extend([g for g in date_games if g.get("externalId") in date_ids])
            
            # Process entropy updates in small batches with independent timeouts
            if entropy_updates_needed:
                await self._process_entropy_updates_in_batches(entropy_updates_needed)
            
            # --- Save Entropy System State ONCE --- 
            if entropy_updates_needed:
                 bt.logging.info("Attempting to save final entropy system state...")
                 try:
                     async with async_timeout.timeout(180): # Give ample time for the final save
                          if await self.entropy_system.save_state():
                               bt.logging.info("Successfully saved final entropy system state.")
                          else:
                               bt.logging.error("Failed to save final entropy system state.")
                 except asyncio.TimeoutError:
                      bt.logging.error("Timeout saving final entropy system state.")
                 except Exception as e:
                      bt.logging.error(f"Error saving final entropy system state: {e}")
            # --- End Final Save ---
            
            # Update last processed time only if we had successful updates
            if inserted_ids:
                self._last_processed_time = current_time
                        
            self.all_games = valid_games
            return inserted_ids
                
        except Exception as e:
            bt.logging.error(f"Error in game data update: {str(e)}")
            bt.logging.error(traceback.format_exc())
            return []

    def _cleanup_processed_ids(self):
        """Clean up processed game IDs older than 24 hours to prevent memory growth"""
        try:
            if len(self._processed_game_ids) > 10000:  # Arbitrary limit
                bt.logging.info("Cleaning up processed game IDs cache")
                self._processed_game_ids.clear()
        except Exception as e:
            bt.logging.error(f"Error cleaning up processed IDs: {str(e)}")

    def _group_games_by_date(self, games):
        """Group games by date with validation"""
        games_by_date = {}
        for game in games:
            try:
                if not isinstance(game, dict):
                    bt.logging.warning(f"Skipping invalid game: {game}")
                    continue
                
                # Get date from either 'date' or 'event_start_date' field
                date_str = game.get('date') or game.get('event_start_date')
                if not date_str:
                    bt.logging.warning(f"Skipping game without date: {game}")
                    continue
                    
                date = datetime.fromisoformat(date_str.replace('Z', '+00:00')).date().isoformat()
                games_by_date.setdefault(date, []).append(game)
            except Exception as e:
                bt.logging.error(f"Error processing game: {e}, Game data: {game}")
                continue
        
        bt.logging.info(f"Grouped {sum(len(games) for games in games_by_date.values())} valid games into {len(games_by_date)} dates")
        return games_by_date

    async def _process_date_games(self, date, games):
        """Process games for a specific date."""
        try:
            bt.logging.info(f"Processing {len(games)} games for date {date}")
            
            # First check which games already exist
            external_ids = [str(game['externalId']) for game in games]
            query = """
                SELECT external_id, team_a, team_b, sport, league, event_start_date,
                       active, outcome, team_a_odds, team_b_odds, tie_odds, can_tie
                FROM game_data
                WHERE external_id = ANY(:external_ids)
            """
            params = {"external_ids": external_ids}
            existing_games = await self.db_manager.fetch_all(query, params)
            
            # Create a map of existing games by external_id for quick lookup
            existing_game_map = {str(game['external_id']): game for game in existing_games}
            
            # Process each game
            inserted_ids = []
            for game in games:
                try:
                    external_id = str(game['externalId'])
                    
                    # Skip if game already exists and is not active
                    if external_id in existing_game_map and not existing_game_map[external_id]['active']:
                        continue
                    
                    # Convert odds to float values
                    team_a_odds = float(game.get('teamAOdds', 0))
                    team_b_odds = float(game.get('teamBOdds', 0))
                    tie_odds = float(game.get('tieOdds', 0)) if game.get('tieOdds') is not None else None
                    
                    # Convert outcome to integer
                    outcome_str = game.get('outcome', 'Unfinished')
                    outcome = (
                        0 if outcome_str == 'TeamAWin' else
                        1 if outcome_str == 'TeamBWin' else
                        2 if outcome_str == 'Draw' else
                        3  # Default to Unfinished
                    )
                    
                    # Get event start date, trying multiple common keys
                    event_start_date_str = game.get('eventStartDate')
                    if not event_start_date_str:
                        event_start_date_str = game.get('event_start_date')
                    if not event_start_date_str:
                        event_start_date_str = game.get('eventDate')
                    if not event_start_date_str:
                         event_start_date_str = game.get('startDate')
                    if not event_start_date_str:
                         event_start_date_str = game.get('start_date')
                    if not event_start_date_str:
                         event_start_date_str = game.get('date') # Try 'date' as well

                    if not event_start_date_str:
                        # Log the entire game dictionary for debugging
                        bt.logging.warning(f"Skipping game {external_id}: Missing expected start date key. Raw game data: {game}")
                        continue

                    # Ensure the date is parsed correctly (assuming ISO format)
                    try:
                        event_start_date = datetime.fromisoformat(event_start_date_str.replace("Z", "+00:00"))
                    except ValueError:
                        bt.logging.warning(f"Skipping game {external_id}: Invalid date format '{event_start_date_str}'")
                        continue

                    # Use _prepare_game_params to get the correct dictionary with game_id
                    game_data_params = self._prepare_game_params(game)
                    if not game_data_params:
                        continue # Skip if preparation failed
                    
                    # Insert or update the game - Ensure query includes game_id
                    query = """
                        INSERT INTO game_data (
                            game_id, external_id, team_a, team_b, team_a_odds, team_b_odds,
                            tie_odds, can_tie, event_start_date, create_date,
                            last_update_date, sport, league, outcome, active
                        ) VALUES (
                            :game_id, :external_id, :team_a, :team_b, :team_a_odds, :team_b_odds,
                            :tie_odds, :can_tie, :event_start_date, :create_date,
                            :last_update_date, :sport, :league, :outcome, :active
                        )
                        ON CONFLICT (external_id) DO UPDATE SET
                            team_a_odds = EXCLUDED.team_a_odds,
                            team_b_odds = EXCLUDED.team_b_odds,
                            tie_odds = EXCLUDED.tie_odds,
                            event_start_date = EXCLUDED.event_start_date,
                            active = EXCLUDED.active,
                            outcome = EXCLUDED.outcome,
                            last_update_date = EXCLUDED.last_update_date
                    """
                    
                    await self.db_manager.execute_query(query, game_data_params)
                    inserted_ids.append(external_id)
                    
                except Exception as e:
                    bt.logging.error(f"Error processing game {external_id}: {str(e)}")
                    continue
            
            return inserted_ids
            
        except Exception as e:
            bt.logging.error(f"Error in _process_date_games: {str(e)}")
            bt.logging.error(traceback.format_exc())
            return []

    async def _process_game_chunk_with_retries(self, chunk):
        """Process a single chunk of games with retries"""
        retries = 0
        while retries < self.MAX_RETRIES:
            try:
                async with async_timeout.timeout(self.TRANSACTION_TIMEOUT):
                    async with self.db_manager.transaction() as session:
                        chunk_ids = await self.insert_or_update_games(chunk, session)
                        if chunk_ids:
                            bt.logging.info(f"Successfully processed chunk of {len(chunk)} games")
                            return chunk_ids
            except asyncio.TimeoutError:
                bt.logging.warning(f"Timeout processing chunk (attempt {retries + 1})")
                retries += 1
            except Exception as e:
                bt.logging.error(f"Error processing chunk: {str(e)}")
                bt.logging.error(traceback.format_exc())
                retries += 1
        
        bt.logging.error(f"Failed to process chunk after {self.MAX_RETRIES} attempts")
        return []

    async def _process_entropy_updates_in_batches(self, games):
        """Process entropy updates in very small batches with independent timeouts"""
        for i in range(0, len(games), self.ENTROPY_BATCH_SIZE):
            batch = games[i:i + self.ENTROPY_BATCH_SIZE]
            
            # First update entropy system (memory operations)
            try:
                game_data = self.prepare_game_data_for_entropy(batch)
                for game in game_data:
                    await self.entropy_system.add_new_game(
                        game["id"], 
                        len(game["current_odds"]), 
                        game["current_odds"]
                    )
                bt.logging.debug(f"Added batch {i//self.ENTROPY_BATCH_SIZE + 1} to entropy system")
            except Exception as e:
                bt.logging.error(f"Error updating entropy system for batch {i//self.ENTROPY_BATCH_SIZE + 1}: {str(e)}")
                continue
            
            # REMOVED state saving from within this loop
            # # Then try to save state with retries
            # retries = 0
            # while retries < self.MAX_RETRIES:
            #     try:
            #         # Use a separate long-running session for entropy state save
            #         async with self.db_manager.get_long_running_session() as session:
            #             await self.entropy_system.save_state()
            #             bt.logging.debug(f"Saved entropy state for batch {i//self.ENTROPY_BATCH_SIZE + 1}")
            #             break
            #     except Exception as e:
            #         bt.logging.error(f"Error saving entropy state (attempt {retries + 1}): {str(e)}")
            #         retries += 1
            #         if retries == self.MAX_RETRIES:
            #             bt.logging.error("Failed to save entropy state after max retries")

    async def insert_or_update_games(self, games, session):
        """Insert or update games in the database using the provided session"""
        try:
            inserted_ids = []
            
            # Filter out string entries and ensure we have a list of games
            if isinstance(games, dict):
                games = [games]
            
            valid_games = [g for g in games if isinstance(g, dict)]
            bt.logging.info(f"Inserting/updating {len(valid_games)} games")
            
            for game in valid_games:
                try:
                    external_id = str(game.get("externalId"))
                    if not external_id:
                        continue

                    # Process game data and execute update
                    params = self._prepare_game_params(game)
                    if not params:
                        continue

                    await session.execute(
                        text(self._get_upsert_query()),
                        params
                    )
                    bt.logging.debug(f"Inserted/Updated game with external_id: {external_id}")
                    inserted_ids.append(external_id)

                except asyncio.CancelledError:
                    bt.logging.warning(f"Game processing was cancelled for external_id: {external_id}")
                    raise
                except Exception as e:
                    bt.logging.error(f"Error processing game with external_id {external_id}: {str(e)}")
                    continue

            return inserted_ids
            
        except asyncio.CancelledError:
            bt.logging.warning("Game processing was cancelled")
            raise
        except Exception as e:
            bt.logging.error(f"Error in insert_or_update_games: {str(e)}")
            raise

    def _prepare_game_params(self, game):
        """Prepare parameters for game insertion or update."""
        try:
            # Handle eventStartDate - check camelCase, snake_case, and 'date' keys
            event_start_date_str = game.get('eventStartDate') or game.get('event_start_date') or game.get('date')
            if not event_start_date_str:
                bt.logging.error(f"Missing expected start date key in game data: {game}")
                return None
                
            # Ensure the date is parsed correctly (assuming ISO format)
            try:
                event_start_date = datetime.fromisoformat(event_start_date_str.replace("Z", "+00:00"))
            except ValueError:
                bt.logging.warning(f"Skipping game {game.get('externalId')}: Invalid date format '{event_start_date_str}'")
                return None

            # Convert odds to float values safely
            team_a_odds = float(game.get('teamAOdds', 0.0) or 0.0)
            team_b_odds = float(game.get('teamBOdds', 0.0) or 0.0)
            # Handle potential None or 0 for tieOdds before converting
            tie_odds_raw = game.get('tieOdds')
            tie_odds = float(tie_odds_raw) if tie_odds_raw is not None else None

            # Convert outcome to integer
            outcome_str = game.get('outcome')
            outcome = (
                0 if outcome_str == 'TeamAWin' else
                1 if outcome_str == 'TeamBWin' else
                2 if outcome_str == 'Draw' else
                4 if outcome_str == 'Cancelled' else # Handle Cancelled
                3  # Default to Unfinished/None
            )

            return {
                'game_id': str(uuid.uuid4()),
                'external_id': int(game['externalId']), # Ensure external_id is int
                'team_a': game.get('teamA'),
                'team_b': game.get('teamB'),
                'team_a_odds': team_a_odds,
                'team_b_odds': team_b_odds,
                'tie_odds': tie_odds,
                'can_tie': bool(game.get('canTie', False)),
                'event_start_date': event_start_date, # Pass datetime object
                'create_date': datetime.now(timezone.utc), # Pass datetime object
                'last_update_date': datetime.now(timezone.utc), # Pass datetime object
                'sport': game.get('sport'),
                'league': game.get('league'),
                'outcome': outcome,
                'active': True # Use boolean True
            }
        except KeyError as e:
            bt.logging.error(f"Missing required field in game data: {e}")
            return None
        except Exception as e:
            bt.logging.error(f"Error preparing game parameters: {e}")
            return None

    def _process_game_outcome(self, game):
        """Process game outcome with validation"""
        outcome = game.get("outcome")
        if outcome is None or outcome == "Unfinished":
            return 3
        elif isinstance(outcome, str):
            return (
                0 if outcome == "TeamAWin" else
                1 if outcome == "TeamBWin" else
                2 if outcome == "Draw" else
                3  # Default to Unfinished for unknown strings
            )
        return 3

    def _get_upsert_query(self):
        """Get the SQL query for upserting game data"""
        return """
        INSERT INTO game_data (
            game_id, team_a, team_b, sport, league, external_id, create_date, 
            last_update_date, event_start_date, active, outcome, team_a_odds, 
            team_b_odds, tie_odds, can_tie
        )
        VALUES (
            :game_id, :team_a, :team_b, :sport, :league, :external_id, :create_date,
            :last_update_date, :event_start_date, :active, :outcome, :team_a_odds,
            :team_b_odds, :tie_odds, :can_tie
        )
        ON CONFLICT(external_id) DO UPDATE SET
            team_a_odds = excluded.team_a_odds,
            team_b_odds = excluded.team_b_odds,
            tie_odds = excluded.tie_odds,
            event_start_date = excluded.event_start_date,
            active = excluded.active,
            outcome = excluded.outcome,
            last_update_date = excluded.last_update_date
        """

    def prepare_game_data_for_entropy(self, games):
        game_data = []
        for game in games:
            try:
                # Skip if game is not a dictionary
                if not isinstance(game, dict):
                    bt.logging.debug(f"Skipping non-dict game entry: {game}")
                    continue
                    
                # Check if required fields exist
                if not all(key in game for key in ["externalId", "teamAOdds", "teamBOdds", "sport"]):
                    bt.logging.debug(f"Skipping game missing required fields: {game}")
                    continue
                    
                game_data.append({
                    "id": game["externalId"],
                    "predictions": {},  # No predictions yet for new games
                    "current_odds": [
                        float(game["teamAOdds"]),
                        float(game["teamBOdds"]),
                        float(game.get("drawOdds", 0.0)) if game['sport'] != 'Football' else 0.0
                    ],
                })
            except Exception as e:
                bt.logging.error(f"Error preparing game for entropy: {e}")
                bt.logging.debug(f"Problematic game data: {game}")
                continue
                
        return game_data

    async def update_predictions_with_payouts(self, external_ids):
        """
        Retrieve all predictions associated with the provided external IDs, determine if each prediction won,
        calculate payouts, and update the predictions in the database.

        Args:
            external_ids (List[str]): List of external_id's of the games that were inserted/updated.
        """
        try:
            if not external_ids:
                bt.logging.info("No external IDs provided for updating predictions.")
                return

            # Fetch outcomes for the given external_ids
            query = """
                SELECT external_id, outcome
                FROM game_data
                WHERE external_id IN ({seq})
            """.format(
                seq=",".join(["?"] * len(external_ids))
            )
            game_outcomes = await self.db_manager.fetch_all(query, tuple(external_ids))
            game_outcome_map = {
                external_id: outcome for external_id, outcome in game_outcomes
            }

            bt.logging.info(f"Fetched outcomes for {len(game_outcomes)} games.")

            # Fetch all predictions associated with the external_ids
            query = """
                SELECT prediction_id, miner_uid, game_id, predicted_outcome, predicted_odds, wager
                FROM predictions
                WHERE game_id IN ({seq}) 
            """.format(
                seq=",".join(["?"] * len(external_ids))
            )
            predictions = await self.db_manager.fetch_all(query, tuple(external_ids))

            bt.logging.info(f"Fetched {len(predictions)} predictions to process.")

            for prediction in predictions:
                
                (
                    prediction_id,
                    miner_uid,
                    game_id,
                    predicted_outcome,
                    predicted_odds,
                    wager,
                ) = prediction
                if game_id == "game_id":
                    continue
                actual_outcome = game_outcome_map.get(game_id)

                if actual_outcome is None:
                    bt.logging.warning(
                        f"No outcome found for game {game_id}. Skipping prediction {prediction_id}."
                    )
                    continue

                is_winner = predicted_outcome == actual_outcome
                payout = wager * predicted_odds if is_winner else 0

                update_query = """
                    UPDATE predictions
                    SET result = ?, payout = ?, processed = 1
                    WHERE prediction_id = ?
                """
                await self.db_manager.execute_query(
                    update_query, (is_winner, payout, prediction_id)
                )

                if is_winner:
                    bt.logging.info(
                        f"Prediction {prediction_id}: Miner {miner_uid} won. Payout: {payout}"
                    )
                else:
                    bt.logging.info(
                        f"Prediction {prediction_id}: Miner {miner_uid} lost."
                    )

            # Ensure entropy scores are calculated
            await self.ensure_predictions_have_entropy_score(external_ids)

        except Exception as e:
            
            bt.logging.error(f"Error in update_predictions_with_payouts: {e}")
            raise

    async def ensure_predictions_have_entropy_score(self, external_ids):
        """Ensure all predictions for given games have entropy scores calculated."""
        try:
            query = """
                SELECT p.prediction_id, p.miner_uid, p.game_id, p.predicted_outcome, 
                       p.predicted_odds, p.wager, p.prediction_date
                FROM predictions p
                WHERE p.game_id IN ({seq})
            """.format(seq=",".join(["?"] * len(external_ids)))
            
            predictions = await self.db_manager.fetch_all(query, tuple(external_ids))
            bt.logging.info(f"Processing {len(predictions)} predictions for entropy scores")
            
            for pred in predictions:
                try:
                    # Add prediction to entropy system
                    self.entropy_system.add_prediction(
                        prediction_id=pred['prediction_id'],
                        miner_uid=pred['miner_uid'],
                        game_id=pred['game_id'],
                        predicted_outcome=pred['predicted_outcome'],
                        wager=float(pred['wager']),
                        predicted_odds=float(pred['predicted_odds']),
                        prediction_date=pred['prediction_date']
                    )
                    bt.logging.debug(f"Added prediction {pred['prediction_id']} to entropy system")
                    
                except Exception as e:
                    bt.logging.error(f"Error adding prediction {pred['prediction_id']} to entropy system: {e}")
                    bt.logging.error(f"Traceback:\n{traceback.format_exc()}")
                    continue
                    
        except Exception as e:
            bt.logging.error(f"Error in ensure_predictions_have_entropy_score: {e}")
            bt.logging.error(f"Traceback:\n{traceback.format_exc()}")



