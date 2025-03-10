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

    # Constants at the top of sports_data.py
    TRANSACTION_TIMEOUT = 120  # Increase to 2 minutes
    CHUNK_SIZE = 50  # Process games in smaller batches

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

    async def fetch_and_update_game_data(self, last_api_call):
        """Fetch and update game data with proper transaction and timeout handling"""
        try:
            # Set overall timeout for the entire operation
            async with async_timeout.timeout(300):  # 5 minute timeout
                all_games = await self.api_client.fetch_all_game_data(last_api_call)
                bt.logging.info(f"Fetched {len(all_games)} games from API")
                
                if not all_games:
                    return []
                    
                # Process games in chunks
                inserted_ids = []
                for i in range(0, len(all_games), self.CHUNK_SIZE):
                    chunk = all_games[i:i + self.CHUNK_SIZE]
                    try:
                        # Set timeout for each chunk processing
                        async with async_timeout.timeout(self.TRANSACTION_TIMEOUT):
                            # Process chunk with proper context management
                            async with self.db_manager.get_session() as session:
                                chunk_ids = await self._process_game_chunk(chunk, session)
                                inserted_ids.extend(chunk_ids)
                                    
                    except asyncio.CancelledError:
                        bt.logging.warning(f"Chunk {i//self.CHUNK_SIZE + 1} processing was cancelled")
                        raise
                    except Exception as e:
                        bt.logging.error(f"Error in transaction: {str(e)}")
                        bt.logging.error(traceback.format_exc())
                        continue
                    
                self.all_games = all_games
                return inserted_ids
                    
        except asyncio.TimeoutError:
            bt.logging.error("Overall game data update operation timed out")
            raise
        except asyncio.CancelledError:
            bt.logging.warning("Game data update operation was cancelled")
            raise
        except Exception as e:
            bt.logging.error(f"Error fetching game data: {str(e)}")
            bt.logging.error(traceback.format_exc())
            raise

    async def _process_game_chunk(self, chunk, session):
        """Process a chunk of games with proper error handling"""
        try:
            # Insert/update games in database
            chunk_ids = await self.insert_or_update_games(chunk, session)
            
            # Prepare and add games to entropy system
            game_data_for_entropy = self.prepare_game_data_for_entropy(chunk)
            for game in game_data_for_entropy:
                try:
                    # Set timeout for each entropy system update
                    async with async_timeout.timeout(5):  # 5 second timeout per game
                        await self.entropy_system.add_new_game(
                            game["id"], 
                            len(game["current_odds"]), 
                            game["current_odds"]
                        )
                except asyncio.TimeoutError:
                    bt.logging.error(f"Timeout adding game {game['id']} to entropy system")
                    continue
                except Exception as e:
                    bt.logging.error(f"Error adding game {game['id']} to entropy system: {e}")
                    bt.logging.error(traceback.format_exc())
                    continue
            
            # Save entropy system state
            try:
                async with async_timeout.timeout(5):  # 5 second timeout for state save
                    await self.entropy_system.save_state()
            except asyncio.TimeoutError:
                bt.logging.error("Timeout saving entropy system state")
            
            return chunk_ids
            
        except asyncio.CancelledError:
            bt.logging.warning("Game chunk processing was cancelled")
            raise
        except Exception as e:
            bt.logging.error(f"Error processing game chunk: {str(e)}")
            bt.logging.error(traceback.format_exc())
            raise

    async def insert_or_update_games(self, games, session):
        """Insert or update games in the database using the provided session"""
        try:
            inserted_ids = []
            
            # Filter out string entries and ensure we have a list of games
            if isinstance(games, dict):
                games = [games]  # Single game case
            
            valid_games = [g for g in games if isinstance(g, dict)]
            bt.logging.info(f"Inserting/updating {len(valid_games)} games")
            
            for game in valid_games:
                try:
                    # Extract only the fields we need
                    external_id = str(game.get("externalId"))
                    if not external_id:
                        bt.logging.debug(f"Skipping game without external ID: {game}")
                        continue

                    # Check if game exists and get its game_id
                    result = await session.execute(
                        text("SELECT game_id, create_date FROM game_data WHERE external_id = :external_id"),
                        {"external_id": external_id}
                    )
                    existing_game = result.fetchone()
                    
                    if existing_game:
                        game_id = existing_game[0]
                        create_date = existing_game[1]
                    else:
                        game_id = str(uuid.uuid4())
                        create_date = datetime.now(timezone.utc).isoformat()

                    # Extract required fields with defaults
                    team_a = str(game.get("teamA", ""))
                    team_b = str(game.get("teamB", ""))
                    sport = str(game.get("sport", ""))
                    league = str(game.get("league", ""))
                    
                    # Parse dates with proper timezone handling
                    last_update_date = datetime.now(timezone.utc).isoformat()
                    
                    # Handle event start date
                    event_start_date = game.get("date", "")
                    if event_start_date:
                        event_start_time = datetime.fromisoformat(
                            event_start_date.replace('Z', '+00:00')
                        ).replace(tzinfo=timezone.utc)
                    else:
                        bt.logging.error(f"Missing event start date for game: {game}")
                        continue

                    # Handle outcome
                    outcome = game.get("outcome")
                    if outcome is None or outcome == "Unfinished":
                        outcome = 3
                    elif isinstance(outcome, str):
                        outcome = (
                            0 if outcome == "TeamAWin" else
                            1 if outcome == "TeamBWin" else
                            2 if outcome == "Draw" else
                            3  # Default to Unfinished for unknown strings
                        )
                    
                    # Set active status
                    active = 1
                    current_time = datetime.now(timezone.utc)
                    if outcome != 3 and (current_time - event_start_time) > timedelta(hours=4):
                        active = 0

                    # Extract odds
                    team_a_odds = float(game.get("teamAOdds", 0))
                    team_b_odds = float(game.get("teamBOdds", 0))
                    tie_odds = float(game.get("drawOdds", 0))
                    
                    # Determine if game can tie
                    can_tie = 1 if game.get("canDraw", False) else 0

                    # Insert/update the game using the provided session
                    await session.execute(
                        text("""
                        INSERT INTO game_data (game_id, team_a, team_b, sport, league, external_id, create_date, last_update_date,
                                           event_start_date, active, outcome, team_a_odds, team_b_odds, tie_odds, can_tie)
                        VALUES (:game_id, :team_a, :team_b, :sport, :league, :external_id, :create_date, :last_update_date,
                               :event_start_date, :active, :outcome, :team_a_odds, :team_b_odds, :tie_odds, :can_tie)
                        ON CONFLICT(external_id) DO UPDATE SET
                            team_a_odds = excluded.team_a_odds,
                            team_b_odds = excluded.team_b_odds,
                            tie_odds = excluded.tie_odds,
                            event_start_date = excluded.event_start_date,
                            active = excluded.active,
                            outcome = excluded.outcome,
                            last_update_date = excluded.last_update_date
                        """),
                        {
                            "game_id": game_id,
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
                    )
                    bt.logging.debug(f"Inserted/Updated game with external_id: {external_id}")
                    inserted_ids.append(external_id)

                except asyncio.CancelledError:
                    bt.logging.warning(f"Game processing was cancelled for external_id: {external_id}")
                    raise
                except Exception as e:
                    bt.logging.error(f"Error processing game with external_id {external_id}: {str(e)}")
                    bt.logging.error(traceback.format_exc())
                    continue

            return inserted_ids
            
        except asyncio.CancelledError:
            bt.logging.warning("Game processing was cancelled")
            raise
        except Exception as e:
            bt.logging.error(f"Error in insert_or_update_games: {str(e)}")
            bt.logging.error(traceback.format_exc())
            raise

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



