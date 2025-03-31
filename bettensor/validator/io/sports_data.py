import os
import json
import time
from typing import List
import uuid
import sqlite3
import requests
import bittensor as bt
from dateutil import parser
from datetime import datetime, timedelta, timezone
from ..scoring.entropy_system import EntropySystem
from bettensor.validator.database.database_manager import DatabaseManager
import traceback
import asyncio
import async_timeout
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
        db_manager: DatabaseManager,
        entropy_system: EntropySystem,
        api_client,
    ):
        self.db_manager = db_manager
        self.entropy_system = entropy_system
        self.api_client = api_client
        self.all_games = []
        self._last_processed_time = None
        self._processed_game_ids = set()  # Track processed game IDs

    async def fetch_and_update_game_data(self, last_api_call):
        """Fetch and update game data with proper transaction and timeout handling"""
        try:
            # Check if we've processed this time period recently
            current_time = datetime.now(timezone.utc)
            if (self._last_processed_time and 
                (current_time - self._last_processed_time) < timedelta(minutes=5)):
                bt.logging.info("Skipping update - last update was less than 5 minutes ago")
                return []

            # Get only games that need updates
            all_games = await self.api_client.fetch_all_game_data(last_api_call)
            bt.logging.info(f"Fetched {len(all_games)} games from API")
            
            if not all_games:
                self._last_processed_time = current_time
                return []

            # Filter out already processed games and group remaining ones
            unprocessed_games = []
            for game in all_games:
                if not isinstance(game, dict) or 'externalId' not in game:
                    continue
                if game['externalId'] not in self._processed_game_ids:
                    unprocessed_games.append(game)

            bt.logging.info(f"Found {len(unprocessed_games)} unprocessed games")
            if not unprocessed_games:
                self._last_processed_time = current_time
                return []

            # Group unprocessed games by date
            games_by_date = self._group_games_by_date(unprocessed_games)
            if not games_by_date:
                self._last_processed_time = current_time
                return []

            # Process games in date order with independent error handling
            inserted_ids = []
            entropy_updates_needed = []
            
            for date, date_games in sorted(games_by_date.items()):
                bt.logging.info(f"Processing {len(date_games)} games for date {date}")
                
                # Process database updates for this date
                date_ids = await self._process_date_games(date_games)
                if date_ids:
                    inserted_ids.extend(date_ids)
                    # Track successfully processed games
                    self._processed_game_ids.update(date_ids)
                    entropy_updates_needed.extend([g for g in date_games if g.get("externalId") in date_ids])
            
            # Process entropy updates in small batches with independent timeouts
            if entropy_updates_needed:
                await self._process_entropy_updates_in_batches(entropy_updates_needed)
            
            # Update last processed time only if we had successful updates
            if inserted_ids:
                self._last_processed_time = current_time
                
            # Periodically clean up old processed IDs to prevent memory growth
            self._cleanup_processed_ids()
                        
            self.all_games = [g for g in all_games if isinstance(g, dict)]
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
                if not isinstance(game, dict) or 'date' not in game:
                    bt.logging.warning(f"Skipping invalid game: {game}")
                    continue
                    
                date = datetime.fromisoformat(game['date'].replace('Z', '+00:00')).date().isoformat()
                games_by_date.setdefault(date, []).append(game)
            except Exception as e:
                bt.logging.error(f"Error processing game: {e}, Game data: {game}")
                continue
        
        bt.logging.info(f"Grouped {sum(len(games) for games in games_by_date.values())} valid games into {len(games_by_date)} dates")
        return games_by_date

    async def _process_date_games(self, date_games):
        """Process all games for a specific date with chunking and retries"""
        inserted_ids = []
        
        # First check which games actually need updates
        try:
            # Get current game states from database
            external_ids = [g.get("externalId") for g in date_games if isinstance(g, dict)]
            if not external_ids:
                return []

            # Use proper SQLAlchemy parameter binding for IN clause
            placeholders = ','.join([':id_' + str(i) for i in range(len(external_ids))])
            query = f"""
                SELECT external_id, outcome, team_a_odds, team_b_odds, tie_odds, active
                FROM game_data
                WHERE external_id IN ({placeholders})
            """
            
            # Create parameters dict with numbered placeholders
            params = {f'id_{i}': external_id for i, external_id in enumerate(external_ids)}
            
            existing_games = await self.db_manager.fetch_all(query, params)
            
            # Create mapping of existing games using column names
            existing_game_map = {}
            if existing_games:
                for row in existing_games:
                    existing_game_map[row['external_id']] = (
                        row['outcome'],
                        row['team_a_odds'],
                        row['team_b_odds'],
                        row['tie_odds'],
                        row['active']
                    )
            
            # Filter games that need updates
            games_needing_update = []
            for game in date_games:
                try:
                    external_id = game.get("externalId")
                    if not external_id:
                        continue
                        
                    if external_id not in existing_game_map:
                        # New game
                        games_needing_update.append(game)
                        continue
                        
                    existing = existing_game_map[external_id]
                    current_outcome = self._process_game_outcome(game)
                    
                    # Check if any relevant fields have changed
                    if (existing[0] != current_outcome or  # outcome changed
                        abs(float(game.get("teamAOdds", 0)) - float(existing[1])) > 0.001 or  # odds changed
                        abs(float(game.get("teamBOdds", 0)) - float(existing[2])) > 0.001 or
                        abs(float(game.get("drawOdds", 0)) - float(existing[3])) > 0.001):
                        games_needing_update.append(game)
                except (ValueError, TypeError, IndexError) as e:
                    bt.logging.error(f"Error processing game comparison for {game.get('externalId')}: {str(e)}")
                    continue
            
            bt.logging.info(f"Found {len(games_needing_update)} games needing updates out of {len(date_games)} total")
            
            # Process games that need updates in chunks
            for i in range(0, len(games_needing_update), self.CHUNK_SIZE):
                chunk = games_needing_update[i:i + self.CHUNK_SIZE]
                chunk_ids = await self._process_game_chunk_with_retries(chunk)
                if chunk_ids:
                    inserted_ids.extend(chunk_ids)
                    
        except Exception as e:
            bt.logging.error(f"Error checking for game updates: {str(e)}")
            bt.logging.error(traceback.format_exc())
            
        return inserted_ids

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
            
            # Then try to save state with retries
            retries = 0
            while retries < self.MAX_RETRIES:
                try:
                    # Use a separate long-running session for entropy state save
                    async with self.db_manager.get_long_running_session() as session:
                        await self.entropy_system.save_state()
                        bt.logging.debug(f"Saved entropy state for batch {i//self.ENTROPY_BATCH_SIZE + 1}")
                        break
                except Exception as e:
                    bt.logging.error(f"Error saving entropy state (attempt {retries + 1}): {str(e)}")
                    retries += 1
                    if retries == self.MAX_RETRIES:
                        bt.logging.error("Failed to save entropy state after max retries")

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
        """Prepare parameters for game insertion/update"""
        try:
            external_id = str(game.get("externalId"))
            
            # Extract required fields with defaults
            team_a = str(game.get("teamA", ""))
            team_b = str(game.get("teamB", ""))
            sport = str(game.get("sport", ""))
            league = str(game.get("league", ""))
            
            # Handle dates
            create_date = datetime.now(timezone.utc).isoformat()
            last_update_date = datetime.now(timezone.utc).isoformat()
            
            event_start_date = game.get("date", "")
            if not event_start_date:
                return None
                
            event_start_time = datetime.fromisoformat(
                event_start_date.replace('Z', '+00:00')
            ).replace(tzinfo=timezone.utc)

            # Handle outcome
            outcome = self._process_game_outcome(game)
            
            # Set active status
            active = 1
            if outcome != 3 and (datetime.now(timezone.utc) - event_start_time) > timedelta(hours=4):
                active = 0

            # Extract odds
            team_a_odds = float(game.get("teamAOdds", 0))
            team_b_odds = float(game.get("teamBOdds", 0))
            tie_odds = float(game.get("drawOdds", 0))
            
            can_tie = 1 if game.get("canDraw", False) else 0

            return {
                "game_id": str(uuid.uuid4()),
                "team_a": team_a,
                "team_b": team_b,
                "sport": sport,
                "league": league,
                "external_id": external_id,
                "create_date": create_date,
                "last_update_date": last_update_date,
                "event_start_date": event_start_time.isoformat(),
                "active": active,
                "outcome": outcome,
                "team_a_odds": team_a_odds,
                "team_b_odds": team_b_odds,
                "tie_odds": tie_odds,
                "can_tie": can_tie,
            }
            
        except Exception as e:
            bt.logging.error(f"Error preparing game parameters: {str(e)}")
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

    async def _update_games_from_api(self, days_from_now: int):
        """Private method to call the API and extract data for the specified days_from_now"""
        try:
            bt.logging.info(f"Updating games data for {days_from_now} days from now")
            
            # Get the date for which we want to fetch data
            date_to_fetch = (datetime.now(timezone.utc) + timedelta(days=days_from_now)).strftime("%Y-%m-%d")
            
            # Get the game data from the API
            all_gamedata = await self.api_client.get_gamedata_by_day(date_to_fetch)
            gamedata_active = {key: value for key, value in all_gamedata.items() if value.active}
            bt.logging.info(f"Fetched {len(gamedata_active)} active games from API")
            
            # Upsert the game data to the database
            if gamedata_active:
                # Store games in DB
                await self._upsert_games_to_db(gamedata_active)
                
                # Broadcast game updates via WebSocket
                await self._broadcast_game_updates(gamedata_active)
                
                bt.logging.info(f"Successfully updated games for {date_to_fetch}")
            else:
                bt.logging.info(f"No active games found for {date_to_fetch}")
                
            return all_gamedata
            
        except Exception as e:
            bt.logging.error(f"Error updating games data: {e}")
            bt.logging.error(traceback.format_exc())
            return {}

    async def _broadcast_game_updates(self, updated_games):
        """
        Broadcast game updates to connected miners via WebSocket.
        
        Args:
            updated_games: Dictionary of updated games to broadcast
        """
        # Skip if WebSocket manager not available or not enabled
        if not hasattr(self.validator, 'websocket_manager') or not self.validator.websocket_manager:
            return
            
        try:
            bt.logging.info(f"Broadcasting {len(updated_games)} game updates to connected miners")
            
            # Get list of connected miners
            connected_miners = self.validator.websocket_manager.get_connected_miners()
            if not connected_miners:
                bt.logging.info("No miners connected for game update broadcast")
                return
                
            # Send updates to all connected miners
            await self.validator.websocket_manager.broadcast_game_updates(updated_games)
            bt.logging.info(f"Game updates broadcast to {len(connected_miners)} miners")
            
        except Exception as e:
            bt.logging.error(f"Error broadcasting game updates: {e}")
            bt.logging.error(traceback.format_exc())



