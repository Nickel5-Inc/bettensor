"""
Class to handle and process all incoming miner data.
"""


import asyncio
from collections import defaultdict
import datetime
from datetime import datetime, timezone, timedelta
import traceback
from typing import Dict
import bittensor as bt
from pydantic import ValidationError
from sqlalchemy import text
import torch
from bettensor.protocol import GameData, TeamGame, TeamGamePrediction
from bettensor.validator.utils.database.database_manager import DatabaseManager
import time

from bettensor.validator.utils.io.bettensor_api_client import BettensorAPIClient


"""
Miner Data Methods, Extends the Bettensor Validator Class

"""


class MinerDataMixin:
    # Constants for validation
    EPSILON = 5.0  # For historical validation, allowing larger differences
    NEW_SUBMISSION_EPSILON = 0.01  # For new submissions, requiring closer matches

    def __init__(self, db_manager, metagraph, processed_uids):
        self.db_manager = db_manager
        self.metagraph = metagraph
        self.processed_uids = processed_uids
        self._game_cache = {}
        self._daily_wager_cache = {}
        self._last_cache_clear = time.time()
        self._cache_ttl = 300  # 5 minutes
        self.bettensor_api = BettensorAPIClient(db_manager)

    def _clear_cache_if_needed(self):
        """Clear caches if TTL has expired"""
        current_time = time.time()
        if current_time - self._last_cache_clear > self._cache_ttl:
            self._game_cache.clear()
            self._daily_wager_cache.clear()
            self._last_cache_clear = current_time

    async def _batch_load_games(self, game_ids):
        """Load multiple games at once and cache them"""
        uncached_ids = [gid for gid in game_ids if gid not in self._game_cache]
        if uncached_ids:
            bt.logging.debug(f"Loading uncached games. Sample IDs: {uncached_ids[:5]}...")
            bt.logging.debug(f"Sample ID types: {[type(gid) for gid in uncached_ids[:5]]}")
            
            # Use named parameters for SQLAlchemy
            placeholders = ','.join([f':id{i}' for i in range(len(uncached_ids))])
            query = f"""
                SELECT sport, league, event_start_date, team_a, team_b, 
                       team_a_odds, team_b_odds, tie_odds, outcome, external_id
                FROM game_data 
                WHERE external_id IN ({placeholders})
            """
            
            # Create dictionary of parameters
            params = {f'id{i}': id_val for i, id_val in enumerate(uncached_ids)}
            games = await self.db_manager.fetch_all(query, params)
            
            # Cache games by external_id
            for game in games:
                external_id = game['external_id']
                bt.logging.debug(f"Caching game {external_id} (type: {type(external_id)})")
                self._game_cache[external_id] = game
                # Also cache string version if numeric
                if isinstance(external_id, (int, float)):
                    self._game_cache[str(external_id)] = game
                # Also cache numeric version if string and convertible
                elif isinstance(external_id, str):
                    try:
                        self._game_cache[int(external_id)] = game
                    except ValueError:
                        pass
                
            # Log cache stats
            bt.logging.debug(f"Game cache size: {len(self._game_cache)}")
            bt.logging.debug(f"Found {len(games)} games out of {len(uncached_ids)} requested")
            if len(games) < len(uncached_ids):
                missing_ids = set(uncached_ids) - {g['external_id'] for g in games}
                bt.logging.debug(f"Missing games: {list(missing_ids)[:5]}...")

    async def _get_daily_wager_totals(self, miner_uids):
        """Get daily wager totals for multiple miners at once"""
        current_time = datetime.now(timezone.utc)
        date_key = current_time.date().isoformat()
        
        # Filter out miners we already have cached data for
        uncached_uids = [
            uid for uid in miner_uids 
            if uid not in self._daily_wager_cache or 
            self._daily_wager_cache[uid]['date'] != date_key
        ]
        
        if uncached_uids:
            # Use named parameters for SQLAlchemy
            placeholders = ','.join([f':uid{i}' for i in range(len(uncached_uids))])
            query = f"""
                SELECT miner_uid, COALESCE(SUM(wager), 0) as total 
                FROM predictions 
                WHERE miner_uid IN ({placeholders}) 
                AND DATE(prediction_date) = DATE(:current_time)
                GROUP BY miner_uid
            """
            
            # Create dictionary of parameters
            params = {
                f'uid{i}': uid_val for i, uid_val in enumerate(uncached_uids)
            }
            params['current_time'] = current_time
            
            results = await self.db_manager.fetch_all(query, params)
            
            for result in results:
                self._daily_wager_cache[result['miner_uid']] = {
                    'total': float(result['total']),
                    'date': date_key
                }
            
            # Initialize cache for miners with no predictions today
            for uid in uncached_uids:
                if uid not in self._daily_wager_cache:
                    self._daily_wager_cache[uid] = {'total': 0.0, 'date': date_key}

    async def insert_predictions(self, processed_uids, predictions_list):
        """Insert validated predictions into the database."""
        start_time = time.time()
        validation_stats = defaultdict(int)
        valid_predictions = []
        return_dict = {}

        try:
            current_time = datetime.now(timezone.utc)
            current_date = current_time.date()
            
            # Calculate total predictions to validate
            total_predictions = sum(len(pred_dict) for _, pred_dict in predictions_list)
            bt.logging.info(f"Starting prediction validation phase for {total_predictions} predictions from {len(predictions_list)} miners")

            # Pre-load all game data
            all_game_ids = set()
            all_miner_uids = set()
            for miner_uid, pred_dict in predictions_list:
                all_miner_uids.add(miner_uid)
                for pred in pred_dict.values():
                    all_game_ids.add(pred.game_id)

            bt.logging.debug(f"Pre-loading data for {len(all_game_ids)} unique games from {len(all_miner_uids)} miners")
            await self._batch_load_games(list(all_game_ids))

            # Get existing predictions for duplicate check
            prediction_ids = []
            for _, pred_dict in predictions_list:
                prediction_ids.extend(pred_dict.keys())
            
            existing_predictions = await self._get_existing_predictions(prediction_ids)
            if existing_predictions:
                bt.logging.debug(f"Found {len(existing_predictions)} existing predictions that will be skipped")

            # Sort all predictions chronologically
            all_predictions = []
            for miner_uid, pred_dict in predictions_list:
                for pred_id, pred in pred_dict.items():
                    # Use validator's datetime as prediction time prevent fake prediction time sent by miner
                    pred_date = current_time.isoformat()
                    if isinstance(pred_date, str):
                        pred_datetime = datetime.fromisoformat(pred_date)
                        if pred_datetime.tzinfo is None:
                            pred_datetime = pred_datetime.replace(tzinfo=timezone.utc)
                    else:
                        pred_datetime = pred_date
                    
                    all_predictions.append((miner_uid, pred_id, pred, pred_datetime))
            
            # Sort by prediction timestamp
            all_predictions.sort(key=lambda x: x[3])

            # Group predictions by miner and date
            predictions_by_miner_date = defaultdict(lambda: defaultdict(list))
            for miner_uid, pred_id, pred, pred_datetime in all_predictions:
                pred_date = pred_datetime.date().isoformat()
                predictions_by_miner_date[miner_uid][pred_date].append((pred_id, pred, pred_datetime))

            # Process predictions by miner and date within a transaction
            async with self.db_manager.transaction():
                for miner_uid, dates in predictions_by_miner_date.items():
                    for pred_date, miner_predictions in dates.items():
                        # Get current daily total within the transaction
                        query = """
                            SELECT COALESCE(SUM(wager), 0) as total 
                            FROM predictions 
                            WHERE miner_uid = :miner_uid
                            AND DATE(prediction_date) = DATE(:pred_date)
                        """
                        result = await self.db_manager.fetch_one(
                            query, 
                            {
                                'miner_uid': miner_uid,
                                'pred_date': pred_date
                            }
                        )
                        current_total = float(result['total']) if result else 0.0
                        
                        bt.logging.info(f"Processing predictions for miner {miner_uid} on {pred_date} (current total: ${current_total:.2f})")
                        
                        # Process each prediction
                        for pred_id, pred, pred_datetime in miner_predictions:
                            try:
                                wager = float(pred.wager)
                                
                                # Validate prediction
                                is_valid, message, validation_type, numeric_outcome = await self.validate_prediction(
                                    miner_uid,
                                    pred_id,
                                    {
                                        'prediction_id': pred_id,
                                        'game_id': pred.game_id,
                                        'wager': wager,
                                        'predicted_outcome': pred.predicted_outcome,
                                        'team_a': pred.team_a,
                                        'team_b': pred.team_b,
                                        'team_a_odds': pred.team_a_odds,
                                        'team_b_odds': pred.team_b_odds,
                                        'tie_odds': pred.tie_odds,
                                        'confidence_score': pred.confidence_score,
                                        'model_name': pred.model_name,
                                        'predicted_odds': pred.predicted_odds,
                                        'current_total': current_total,
                                    },
                                    existing_predictions
                                )

                                if is_valid and current_total + wager <= 1000:
                                    # Update running total
                                    current_total += wager
                                    
                                    validation_stats['successful'] += 1
                                    valid_predictions.append((
                                        pred_id, pred.game_id, miner_uid, pred_datetime.isoformat(),
                                        numeric_outcome, pred.predicted_odds, pred.team_a, pred.team_b,
                                        pred.wager, pred.team_a_odds, pred.team_b_odds, pred.tie_odds,
                                        3, pred.model_name, pred.confidence_score
                                    ))
                                    return_dict[pred_id] = (True, "Prediction validated successfully")
                                else:
                                    if current_total + wager > 1000:
                                        message = f"Would exceed daily limit (Current: ${current_total:.2f}, Attempted: ${wager:.2f})"
                                        validation_type = 'daily_limit_exceeded'
                                    validation_stats[validation_type] += 1
                                    return_dict[pred_id] = (False, message)

                            except Exception as e:
                                bt.logging.error(f"Error processing prediction {pred_id}: {str(e)}")
                                bt.logging.error(traceback.format_exc())
                                return_dict[pred_id] = (False, f"Processing error: {str(e)}")
                                continue

                # Batch insert all valid predictions within the transaction
                if valid_predictions:
                    try:
                        await self.db_manager.executemany(
                            """
                            INSERT INTO predictions (
                                prediction_id, game_id, miner_uid, prediction_date,
                                predicted_outcome, predicted_odds, team_a, team_b,
                                wager, team_a_odds, team_b_odds, tie_odds,
                                outcome, model_name, confidence_score
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """,
                            valid_predictions
                        )
                        bt.logging.info(f"Successfully inserted {len(valid_predictions)} predictions")
                    except Exception as e:
                        bt.logging.error(f"Error during batch insert: {str(e)}")
                        bt.logging.error(traceback.format_exc())
                        raise

            # Log validation statistics
            bt.logging.info("Validation Statistics:")
            for reason, count in validation_stats.items():
                if count > 0:
                    percentage = (count / total_predictions) * 100
                    bt.logging.info(f"  {reason}: {count} ({percentage:.1f}%)")

            # Update entropy system
            if valid_predictions:
                entropy_start = time.time()
                success_count = 0
                error_count = 0
                for pred_values in valid_predictions:
                    try:
                        self.scoring_system.entropy_system.add_prediction(
                            pred_values[0],  # prediction_id
                            pred_values[2],  # miner_uid
                            pred_values[1],  # game_id
                            pred_values[4],  # predicted_outcome
                            pred_values[8],  # wager
                            pred_values[5],  # predicted_odds
                            pred_values[3]   # prediction_date
                        )
                        success_count += 1
                    except Exception as e:
                        error_count += 1
                        bt.logging.error(f"Error updating entropy system: {str(e)}")

                entropy_time = time.time() - entropy_start
                bt.logging.info(f"Entropy system updates completed in {entropy_time:.3f}s ({success_count} successful, {error_count} failed)")

            total_time = time.time() - start_time
            bt.logging.info(f"Total prediction processing time: {total_time:.3f}s")
            return return_dict

        except Exception as e:
            bt.logging.error(f"Error in prediction processing: {str(e)}")
            bt.logging.error(traceback.format_exc())
            raise

    async def send_confirmation_synapse(self, miner_uid, predictions):
        """
        Asynchronously sends a confirmation synapse to the miner.

        Args:
            miner_uid: the uid of the miner
            predictions: a dictionary with uids as keys and TeamGamePrediction objects as values
        """
        # Convert success/message tuples to string values
        confirmation_dict = {}
        for pred_id, (success, message) in predictions.items():
            # Ensure values are properly converted to strings
            success_str = str(success) if success is not None else "False"
            message_str = str(message) if message is not None else ""
            confirmation_dict[str(pred_id)] = {
                "success": success_str,
                "message": message_str
            }

        # Get miner stats for uid asynchronously
        miner_stats = await self.db_manager.fetch_one(
            "SELECT * FROM miner_stats WHERE miner_uid = ?", (miner_uid,)
        )

        if miner_stats is None:
            bt.logging.warning(f"No miner_stats found for miner_uid: {miner_uid}")
            confirmation_dict['miner_stats'] = {}
        else:
            # Convert all miner_stats values to strings, properly handling binary data
            miner_stats_str = {}
            for key, value in miner_stats.items():
                if isinstance(value, bytes):
                    try:
                        value = int.from_bytes(value, byteorder='little')
                    except (ValueError, TypeError):
                        try:
                            value = value.decode('utf-8')
                        except UnicodeDecodeError:
                            value = 0
                
                # Convert the value to string, handling special cases
                if key == 'miner_current_tier':
                    # Handle string representation of bytes
                    if isinstance(value, str) and value.startswith('\\x'):
                        try:
                            # Convert string representation of bytes to actual bytes
                            value = bytes.fromhex(value[2:].replace('\\x', ''))
                            value = int.from_bytes(value, byteorder='little')
                        except (ValueError, TypeError):
                            value = 1
                    # Ensure tier is a valid integer string
                    try:
                        value = str(int(value)) if value is not None else "1"
                    except (ValueError, TypeError):
                        value = "1"
                elif isinstance(value, (float, int)):
                    value = str(value)
                elif value is None:
                    value = "0"
                else:
                    value = str(value)
                
                miner_stats_str[key] = value
            
            # Log the converted stats for debugging
            bt.logging.debug(f"Converted miner stats for miner {miner_uid}: {miner_stats_str}")
            confirmation_dict['miner_stats'] = miner_stats_str

        # Convert miner_uid to integer for indexing
        miner_uid_int = int(miner_uid)
        axon = self.metagraph.axons[miner_uid_int]

        # Create synapse with confirmation data
        synapse = GameData.create(
            db_path=self.db_path,
            wallet=self.wallet,
            subnet_version=self.subnet_version,
            neuron_uid=miner_uid_int,
            synapse_type="confirmation",
            confirmation_dict=confirmation_dict,
        )

        bt.logging.info(f"Sending confirmation synapse to miner {miner_uid}, axon: {axon}")
        try:
            # Use the forward method directly instead of query to avoid event loop conflicts
            response = await self.dendrite.forward(
                axons=axon,
                synapse=synapse,
                timeout=self.timeout,
                deserialize=True,
            )
            
            bt.logging.info(f"Confirmation synapse sent to miner {miner_uid}")
            return response
            
        except Exception as e:
            bt.logging.error(f"An error occurred while sending confirmation synapse: {e}")
            bt.logging.error(f"Traceback: {traceback.format_exc()}")
            raise

    async def process_prediction(self, processed_uids: torch.tensor, synapses: list) -> list:
        """processes responses received by miners"""
        start_time = time.time()
        predictions = []  # Change to list to maintain order
        synapse_count = len(synapses)
        total_prediction_count = 0
        bt.logging.info(f"Starting synapse validation phase with {synapse_count} synapses")
        
        try:
            for idx, synapse in enumerate(synapses):
                synapse_start = time.time()
                if not hasattr(synapse, 'prediction_dict') or not hasattr(synapse, 'metadata'):
                    bt.logging.warning(f"Synapse {idx+1}/{synapse_count} is invalid - missing prediction_dict or metadata")
                    continue
                
                prediction_dict = synapse.prediction_dict
                metadata = synapse.metadata
                headers = synapse.to_headers()
                
                prediction_count = len(prediction_dict) if prediction_dict else 0
                bt.logging.debug(f"Synapse {idx+1}/{synapse_count} contains {prediction_count} predictions")
                
                if metadata and hasattr(metadata, "neuron_uid"):
                    uid = metadata.neuron_uid
                    synapse_hotkey = headers.get("bt_header_axon_hotkey")
                    
                    validation_start = time.time()
                    # Hotkey validation
                    if synapse_hotkey != self.metagraph.hotkeys[int(uid)]:
                        bt.logging.warning(f"Synapse {idx+1}/{synapse_count} failed hotkey validation - miner {uid} hotkey mismatch")
                        continue

                    # UID validation
                    if any(pred.miner_uid != uid for pred in prediction_dict.values()):
                        bt.logging.warning(f"Synapse {idx+1}/{synapse_count} failed UID validation - miner {uid} predictions have mismatched UIDs")
                        continue

                    matching_predictions = sum(1 for pred in prediction_dict.values() if pred.miner_uid == uid)
                    validation_time = time.time() - validation_start
                    bt.logging.debug(f"Synapse {idx+1}/{synapse_count} basic validation completed in {validation_time:.3f}s")

                    if matching_predictions != len(prediction_dict):
                        bt.logging.warning(f"Synapse {idx+1}/{synapse_count} failed prediction count validation - miner {uid} has mismatched prediction counts")
                        continue

                    if prediction_dict is not None:
                        predictions.append((uid, prediction_dict))
                        total_prediction_count += len(prediction_dict)
                        bt.logging.debug(f"Synapse {idx+1}/{synapse_count} passed validation - added {len(prediction_dict)} predictions from miner {uid}")
                    else:
                        bt.logging.debug(f"Synapse {idx+1}/{synapse_count} has no predictions from miner {uid}")

                    synapse_time = time.time() - synapse_start
                    bt.logging.debug(f"Synapse {idx+1}/{synapse_count} processing completed in {synapse_time:.3f}s")
                else:
                    bt.logging.warning(f"Synapse {idx+1}/{synapse_count} is invalid - missing metadata or neuron_uid")

            process_time = time.time() - start_time
            valid_synapse_count = len(predictions)
            bt.logging.info(f"Synapse validation phase completed in {process_time:.3f}s")
            bt.logging.info(f"Results: {valid_synapse_count}/{synapse_count} synapses passed validation containing {total_prediction_count} total predictions")
            
            insert_start = time.time()
            prediction_results = await self.insert_predictions(processed_uids, predictions)
            insert_time = time.time() - insert_start
            bt.logging.info(f"Prediction validation and insertion completed in {insert_time:.3f}s")

            # Send confirmations to miners
            confirmation_start = time.time()
            confirmation_count = 0
            for uid, prediction_dict in predictions:
                try:
                    await self.send_confirmation_synapse(uid, {
                        pred_id: prediction_results.get(pred_id, (False, "Processing failed"))
                        for pred_id in prediction_dict.keys()
                    })
                    confirmation_count += 1
                except Exception as e:
                    bt.logging.error(f"Failed to send confirmation to miner {uid}: {str(e)}")
                    bt.logging.error(traceback.format_exc())
            
            confirmation_time = time.time() - confirmation_start
            bt.logging.info(f"Sent confirmations to {confirmation_count}/{len(predictions)} miners in {confirmation_time:.3f}s")

            total_time = time.time() - start_time
            bt.logging.info(f"Total processing time: {total_time:.3f}s")

        except Exception as e:
            bt.logging.error(f"Error during synapse processing: {e}")
            bt.logging.error(traceback.format_exc())
            raise

    def update_recent_games(self):
        bt.logging.info("miner_data.py update_recent_games called")
        current_time = datetime.now(timezone.utc)
        five_hours_ago = current_time - timedelta(hours=4)

        recent_games = self.db_manager.fetch_all(
            """
            SELECT external_id, team_a, team_b, sport, league, event_start_date
            FROM game_data
            WHERE event_start_date < ? AND (outcome = 'Unfinished' OR outcome = 3)
            """,
            (five_hours_ago.isoformat(),),
        )
        bt.logging.info("Recent games: ")
        bt.logging.info(recent_games)

        for game in recent_games:
            external_id, team_a, team_b, sport, league, event_start_date = game
            game_info = {
                "external_id": external_id,
                "team_a": team_a,
                "team_b": team_b,
                "sport": sport,
                "league": league,
                "event_start_date": event_start_date,
            }
            bt.logging.info("Game info: ")
            bt.logging.info(game_info)
            numeric_outcome = self.api_client.determine_winner(game_info)
            bt.logging.info("Outcome: ")
            bt.logging.info(numeric_outcome)

            if numeric_outcome is not None:
                # Update the game outcome in the database
                self.api_client.update_game_outcome(external_id, numeric_outcome)

       
        bt.logging.info(f"Checked {len(recent_games)} games for updates")

    def prepare_game_data_for_entropy(self, predictions):
        game_data = []
        for game_id, game_predictions in predictions.items():
            current_odds = self.get_current_odds(game_id)
            game_data.append(
                {
                    "id": game_id,
                    "predictions": game_predictions,
                    "current_odds": current_odds,
                }
            )
        return game_data

    def get_recent_games(self):
        """retrieves recent games from the database"""
        two_days_ago = (
            datetime.utcnow().replace(tzinfo=datetime.timezone.utc)
            - datetime.timedelta(hours=48)
        ).isoformat()
        return self.db_manager.fetch_all(
            "SELECT id, team_a, team_b, external_id FROM game_data WHERE event_start_date >= ? AND outcome = 'Unfinished'",
            (two_days_ago,),
        )

    def get_current_odds(self, game_id):
        try:
            # Query to fetch the current odds for the given game_id
            query = """
            SELECT team_a_odds, team_b_odds, tie_odds
            FROM game_data
            WHERE id = ? OR external_id = ?
            """
            result = self.db_manager.fetchone(query, (game_id, game_id))
            if result:
                home_odds, away_odds, tie_odds = result
                return [home_odds, away_odds, tie_odds]
            else:
                bt.logging.warning(f"No odds found for game_id: {game_id}")
                return [0.0, 0.0, 0.0]  # Return default values if no odds are found
        except Exception as e:
            bt.logging.error(f"Database error in get_current_odds: {e}")
            return [0.0, 0.0, 0.0]  # Return default values in case of database error

    async def fetch_local_game_data(self, current_time):
        """Fetch game data from the local database."""
        try:
            # Calculate the date range for fetching games
            current_datetime = datetime.fromisoformat(current_time)
            start_date = current_datetime - timedelta(days=4)
            end_date = current_datetime + timedelta(days=8)
            
            bt.logging.debug(f"Querying games between {start_date} and {end_date}")
            
            # Query to fetch game data
            query = """
                SELECT 
                    external_id,
                    event_start_date,
                    team_a,
                    team_b,
                    team_a_odds,
                    team_b_odds,
                    tie_odds,
                    outcome,
                    sport,
                    league,
                    create_date,
                    last_update_date,
                    active,
                    can_tie
                FROM game_data
                WHERE event_start_date BETWEEN :start_date AND :end_date
                ORDER BY event_start_date ASC
            """
            
            params = {
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat()
            }
            
            rows = await self.db_manager.fetch_all(query, params)
            
            if not rows:
                return {}
            
            # Process the results into a dictionary
            gamedata_dict = {}
            for row in rows:
                game_id = str(row['external_id'])  # Convert to string
                gamedata_dict[game_id] = {
                    'game_id': game_id,  # Use string game_id
                    'external_id': game_id,  # Use string external_id
                    'event_start_date': row['event_start_date'],
                    'team_a': row['team_a'],
                    'team_b': row['team_b'],
                    'team_a_odds': row['team_a_odds'],
                    'team_b_odds': row['team_b_odds'],
                    'tie_odds': row['tie_odds'],
                    'outcome': str(row['outcome']),  # Convert outcome to string
                    'sport': row['sport'],
                    'league': row['league'],
                    'create_date': row['create_date'],
                    'last_update_date': row['last_update_date'],
                    'active': row['active'],
                    'can_tie': row['can_tie']
                }
            
            return gamedata_dict
            
        except Exception as e:
            bt.logging.error(f"Error querying and processing game data: {str(e)}")
            bt.logging.error(traceback.format_exc())
            return {}

    async def _prediction_exists(self, prediction_id: str) -> bool:
        """Check if a prediction already exists in the database."""
        query = "SELECT 1 FROM predictions WHERE prediction_id = :prediction_id"
        result = await self.db_manager.fetch_one(query, {"prediction_id": prediction_id})
        return bool(result)

    async def validate_prediction(self, miner_uid: int, prediction_id: str, prediction_data: dict, existing_predictions: set, historical_validation: bool = False) -> tuple[bool, str, str, int]:
        """Validate a single prediction. Returns (is_valid, message, validation_type, numeric_outcome)"""
        start_time = time.time()
        try:
            # Use appropriate epsilon based on validation type
            EPSILON = self.EPSILON if historical_validation else self.NEW_SUBMISSION_EPSILON

            # Basic validation
            if not prediction_id or not prediction_data:
                return False, "Missing prediction data", 'missing_data', -1

            # Check if prediction already exists (skip for historical validation)
            if not historical_validation and prediction_id in existing_predictions:
                return False, f"Prediction {prediction_id} already exists in database", 'duplicate_prediction', -1

            # Validate wager amount
            try:
                wager = float(prediction_data.get('wager', 0))
                if wager <= 0:
                    return False, "Prediction with non-positive wager - nice try", 'invalid_wager', -1
                
                # Check daily wager limit (skip for historical validation)
                if not historical_validation:
                    current_total = prediction_data.get('current_total', 0)
                    if current_total + wager > 1000:
                        return False, f"Prediction would exceed daily limit (Current: ${current_total:.2f}, Attempted: ${wager:.2f})", 'daily_limit_exceeded', -1
                    
            except (ValueError, TypeError):
                return False, "Invalid wager value", 'invalid_wager', -1

            # Game validation
            game_id = prediction_data.get('game_id')
            game = self._game_cache.get(game_id)
            if not game:
                # Try string conversion if numeric
                if isinstance(game_id, (int, float)):
                    game = self._game_cache.get(str(game_id))
                # Try numeric conversion if string
                elif isinstance(game_id, str):
                    try:
                        game = self._game_cache.get(int(game_id))
                    except ValueError:
                        pass
                    
            if not game:
                return False, "Game not found in validator game_data", 'game_not_found', -1

            # Skip game start time validation for historical validation
            if not historical_validation:
                current_time = datetime.now(timezone.utc)
                if current_time >= datetime.fromisoformat(game['event_start_date']).replace(tzinfo=timezone.utc):
                    return False, "Game has already started", 'game_started', -1

            # Validate team names match game data
            submitted_team_a = prediction_data.get('team_a')
            submitted_team_b = prediction_data.get('team_b')
            if submitted_team_a != game['team_a'] or submitted_team_b != game['team_b']:
                return False, "Submitted team names do not match game data", 'team_mismatch', -1

            # Validate submitted odds match game data with proper rounding
            # Use a much smaller epsilon for new submissions, only be lenient for historical validation
            submitted_team_a_odds = float(prediction_data.get('team_a_odds'))
            submitted_team_b_odds = float(prediction_data.get('team_b_odds'))
            game_team_a_odds = float(game['team_a_odds'])
            game_team_b_odds = float(game['team_b_odds'])

            if (abs(submitted_team_a_odds - game_team_a_odds) > EPSILON or
                abs(submitted_team_b_odds - game_team_b_odds) > EPSILON):
                return False, "Submitted team odds do not match current game odds" if not historical_validation else "Submitted team odds differ significantly from game data", 'odds_mismatch', -1

            # Handle tie odds validation separately since it could be None
            if game['tie_odds'] is not None:
                submitted_tie_odds = float(prediction_data.get('tie_odds')) if prediction_data.get('tie_odds') is not None else None
                game_tie_odds = float(game['tie_odds'])
                if submitted_tie_odds is None or abs(submitted_tie_odds - game_tie_odds) > EPSILON:
                    return False, "Submitted tie odds do not match current game odds" if not historical_validation else "Submitted tie odds differ significantly from game data", 'odds_mismatch', -1
            elif prediction_data.get('tie_odds') is not None:
                return False, "Submitted tie odds for game that doesn't support ties", 'odds_mismatch', -1

            # Outcome validation
            predicted_outcome = prediction_data.get('predicted_outcome')
            
            # Handle both string team names and numeric outcomes
            if isinstance(predicted_outcome, (int, float)) or (isinstance(predicted_outcome, str) and predicted_outcome.isdigit()):
                # Convert to int if it's a numeric string
                numeric_outcome = int(predicted_outcome)
                if numeric_outcome == 0:
                    expected_odds = round(float(game['team_a_odds']), 4)
                elif numeric_outcome == 1:
                    expected_odds = round(float(game['team_b_odds']), 4)
                elif numeric_outcome == 2 and game['tie_odds'] is not None:
                    expected_odds = round(float(game['tie_odds']), 4)
                else:
                    return False, f"Invalid numeric predicted_outcome: {predicted_outcome}", 'invalid_outcome', -1
            else:
                # Handle string team names
                if predicted_outcome == game['team_a']:
                    numeric_outcome = 0
                    expected_odds = round(float(game['team_a_odds']), 4)
                elif predicted_outcome == game['team_b']:
                    numeric_outcome = 1
                    expected_odds = round(float(game['team_b_odds']), 4)
                elif str(predicted_outcome).lower() == "tie":
                    numeric_outcome = 2
                    expected_odds = round(float(game['tie_odds']), 4) if game['tie_odds'] is not None else None
                else:
                    return False, f"Invalid predicted_outcome: {predicted_outcome}", 'invalid_outcome', -1

            # Validate that predicted_odds matches with the game's stored odds within tolerance
            submitted_odds = prediction_data.get('predicted_odds')
            if submitted_odds is None:
                return False, "Missing predicted_odds", 'missing_odds', -1
            
            if expected_odds is None:
                return False, "No odds available for the predicted outcome", 'invalid_odds', -1

            submitted_odds = float(submitted_odds)
            if abs(submitted_odds - expected_odds) > EPSILON:
                return False, f"Predicted odds {submitted_odds} do not match current game odds {expected_odds}" if not historical_validation else f"Predicted odds {submitted_odds} differs significantly from game odds {expected_odds}", 'odds_mismatch', -1

            # Validate confidence score if provided
            confidence_score = prediction_data.get('confidence_score')
            if confidence_score is not None:
                try:
                    confidence = float(confidence_score)
                    if not 0 <= confidence <= 1:
                        return False, "If provided, confidence score must be between 0 and 1", 'invalid_confidence', -1
                except (ValueError, TypeError):
                    return False, "Invalid confidence score format", 'invalid_confidence', -1

            return True, "Prediction validated successfully", 'successful', numeric_outcome

        except Exception as e:
            bt.logging.error(f"Error validating prediction: {e}")
            bt.logging.error(f"Validation error details: {traceback.format_exc()}")
            return False, f"Validation error: {str(e)}", 'other_errors', -1

    async def validate_historical_predictions(self, predictions, game_data):
        """Validate historical predictions and fix any issues found."""
        try:
            if not predictions or not game_data:
                return [], {}

            bt.logging.info(f"Starting historical validation of {len(predictions)} predictions")
            
            # Group predictions by date and miner for chronological processing
            predictions_by_date_miner = defaultdict(lambda: defaultdict(list))
            predictions_to_delete = []
            valid_predictions = []
            
            # Get current date in UTC for comparison
            current_date = datetime.now(timezone.utc).date()
            
            # First, get ALL predictions for the dates we're processing
            all_dates = set()
            all_miners = set()
            for pred in predictions:
                # Convert prediction_date to UTC date for consistent grouping
                pred_datetime = datetime.fromisoformat(pred['prediction_date'])
                if pred_datetime.tzinfo is None:
                    pred_datetime = pred_datetime.replace(tzinfo=timezone.utc)
                pred_date = pred_datetime.date().isoformat()
                miner_uid = pred['miner_uid']
                predictions_by_date_miner[pred_date][miner_uid].append(pred)
                all_dates.add(pred_date)
                all_miners.add(miner_uid)

            # Process each date's predictions separately within a transaction
            async with self.db_manager.transaction() as session:
                for pred_date, miners_predictions in sorted(predictions_by_date_miner.items()):
                    bt.logging.info(f"Processing predictions for date {pred_date}")
                    
                    # Check if this is today's date
                    is_current_day = datetime.fromisoformat(pred_date).date() == current_date
                    
                    # Process each miner's predictions
                    for miner_uid, miner_predictions in miners_predictions.items():
                        # Get ALL predictions for this miner and date
                        query = """
                            SELECT prediction_id, prediction_date, wager
                            FROM predictions 
                            WHERE miner_uid = :miner_uid
                            AND DATE(prediction_date) = DATE(:pred_date)
                            ORDER BY prediction_date ASC
                        """
                        params = {
                            'miner_uid': miner_uid,
                            'pred_date': pred_date
                        }
                        
                        result = await session.execute(text(query), params)
                        existing_predictions = result.fetchall()
                        
                        # Create a map of all predictions (both existing and new) by timestamp
                        all_predictions = []
                        
                        # Add existing predictions
                        for pred in existing_predictions:
                            # Access by index: prediction_id is 0, prediction_date is 1, wager is 2
                            all_predictions.append({
                                'timestamp': datetime.fromisoformat(pred[1]),
                                'wager': float(pred[2]),
                                'is_existing': True,
                                'prediction_id': pred[0]
                            })
                        
                        # Add new predictions being validated
                        for pred in miner_predictions:
                            all_predictions.append({
                                'timestamp': datetime.fromisoformat(pred['prediction_date']),
                                'wager': float(pred['wager']),
                                'is_existing': False,
                                'full_pred': pred
                            })
                        
                        # Sort all predictions by timestamp
                        all_predictions.sort(key=lambda x: x['timestamp'])
                        
                        # Process predictions in chronological order
                        current_total = 0.0
                        bt.logging.info(f"Processing predictions for miner {miner_uid} on {pred_date} (starting from 0)")
                        
                        for pred in all_predictions:
                            wager = pred['wager']
                            
                            # Check daily wager limit before processing
                            if current_total + wager > 1000:
                                if pred['is_existing']:
                                    # For existing predictions that exceed the limit, mark them for deletion
                                    predictions_to_delete.append(pred['prediction_id'])
                                    bt.logging.warning(
                                        f"Existing prediction {pred['prediction_id']} exceeds daily wager limit: "
                                        f"miner={miner_uid}, date={pred_date}, "
                                        f"current_total={current_total:.2f}, wager={wager:.2f}"
                                    )
                                continue
                            
                            if pred['is_existing']:
                                # Update running total for valid existing predictions
                                current_total += wager
                                continue
                            
                            # This is a prediction we need to validate
                            prediction = pred['full_pred']
                            prediction_id = prediction['prediction_id']
                            
                            try:
                                game_id = prediction['game_id']
                                
                                # Skip if game not found
                                if game_id not in game_data:
                                    continue
                                    
                                game = game_data[game_id]
                                game_outcome = game.get('outcome', '3')
                                
                                # Convert string outcome to int if needed
                                if isinstance(game_outcome, str):
                                    try:
                                        game_outcome = int(game_outcome)
                                    except ValueError:
                                        game_outcome = 3 if game_outcome == 'Unfinished' else -1
                                
                                # Handle unfinished games (outcome 3)
                                if game_outcome == 3:
                                    game_start = datetime.fromisoformat(game['event_start_date'])
                                    current_time = datetime.now(timezone.utc)
                                    
                                    if current_time < game_start:
                                        # Game hasn't started yet, keep prediction
                                        pass
                                    elif current_time - game_start > timedelta(hours=24):
                                        # Only delete if it's been more than 24 hours since game start
                                        bt.logging.info(f"Deleting prediction {prediction_id} for unfinished game {game_id} that started over 24 hours ago")
                                        predictions_to_delete.append(prediction_id)
                                        continue
                                
                                # For current day predictions, use stricter validation
                                if is_current_day:
                                    is_valid, message, validation_type, numeric_outcome = await self.validate_prediction(
                                        miner_uid,
                                        prediction_id,
                                        {
                                            'prediction_id': prediction_id,
                                            'game_id': game_id,
                                            'wager': wager,
                                            'predicted_outcome': prediction['predicted_outcome'],
                                            'team_a': prediction['team_a'],
                                            'team_b': prediction['team_b'],
                                            'team_a_odds': prediction['team_a_odds'],
                                            'team_b_odds': prediction['team_b_odds'],
                                            'tie_odds': prediction['tie_odds'],
                                            'predicted_odds': prediction['predicted_odds'],
                                            'current_total': current_total,
                                        },
                                        set(),  # Empty set since we're already handling duplicates
                                        historical_validation=True  # Use historical validation even for current day
                                    )
                                    if not is_valid:
                                        bt.logging.warning(f"Current day prediction {prediction_id} failed validation: {message}")
                                        predictions_to_delete.append(prediction_id)
                                        continue
                                
                                # Update running total since this prediction will be valid
                                current_total += wager
                                
                                # Add to valid predictions
                                valid_predictions.append(prediction)
                                    
                            except Exception as e:
                                bt.logging.error(f"Error validating prediction {prediction.get('prediction_id', 'unknown')}: {str(e)}")
                                continue

                # Delete invalid predictions within the transaction
                if predictions_to_delete:
                    try:
                        # Use placeholders for the IN clause
                        placeholders = ','.join([f':id{i}' for i in range(len(predictions_to_delete))])
                        delete_query = f"""
                            DELETE FROM predictions 
                            WHERE prediction_id IN ({placeholders})
                        """
                        
                        # Create parameters dictionary
                        delete_params = {f'id{i}': pid for i, pid in enumerate(predictions_to_delete)}
                        
                        bt.logging.info(f"Deleting {len(predictions_to_delete)} invalid predictions")
                        await session.execute(text(delete_query), delete_params)
                        
                        # Verify deletions
                        verify_query = f"""
                            SELECT COUNT(*) as remaining 
                            FROM predictions 
                            WHERE prediction_id IN ({placeholders})
                        """
                        result = await session.execute(text(verify_query), delete_params)
                        verify_result = result.fetchone()
                        remaining = verify_result[0] if verify_result else 0
                        
                        if remaining > 0:
                            bt.logging.warning(f"{remaining} predictions failed to delete")
                        else:
                            bt.logging.info("Successfully deleted all invalid predictions")
                            
                    except Exception as e:
                        bt.logging.error(f"Error deleting predictions: {str(e)}")
                        bt.logging.error(traceback.format_exc())
                        raise

            return valid_predictions, {}
                
        except Exception as e:
            bt.logging.error(f"Error in historical validation: {str(e)}")
            bt.logging.error(traceback.format_exc())
            return [], {}

    async def _get_existing_predictions(self, prediction_ids: list) -> set:
        """Get a set of prediction IDs that already exist in the database."""
        if not prediction_ids:
            return set()
            
        try:
            # Use named parameters for SQLAlchemy
            placeholders = ','.join([f':id{i}' for i in range(len(prediction_ids))])
            query = f"""
                SELECT prediction_id 
                FROM predictions 
                WHERE prediction_id IN ({placeholders})
            """
            
            # Create dictionary of parameters
            params = {f'id{i}': id_val for i, id_val in enumerate(prediction_ids)}
            
            results = await self.db_manager.fetch_all(query, params)
            return {row['prediction_id'] for row in results}
            
        except Exception as e:
            bt.logging.error(f"Error checking existing predictions: {str(e)}")
            bt.logging.error(traceback.format_exc())
            return set()

