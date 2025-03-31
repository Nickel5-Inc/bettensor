import json
import signal
import sys
from argparse import ArgumentParser
import time
import traceback
from typing import Tuple
import bittensor as bt
import sqlite3
from bettensor.base.neuron import BaseNeuron
from bettensor.protocol import Metadata, GameData
from bettensor.miner.stats.miner_stats import MinerStateManager, MinerStatsHandler
import datetime
import os
import threading
from contextlib import contextmanager
from bettensor.miner.database.database_manager import DatabaseManager
from bettensor.miner.database.games import GamesHandler
from bettensor.miner.database.predictions import PredictionsHandler
from bettensor.miner.utils.cache_manager import CacheManager
from bettensor.miner.interfaces.redis_interface import RedisInterface
from bettensor.miner.models.model_utils import SoccerPredictor, MinerConfig
import uuid
from datetime import datetime, timezone
from bittensor import Synapse
from bettensor.miner.utils.health_check import run_health_check
import asyncio
from bettensor import __spec_version__
from bettensor.miner.utils.websocket_manager import MinerWebSocketManager


class BettensorMiner(BaseNeuron):
    def __init__(self, config=None):
        bt.logging.info("Initializing BettensorMiner")
        super().__init__(config=config)

        bt.logging.info("Setting up configuration")
        if not config:
            config = self.config()
        self.neuron_config = config

        bt.logging.info(f"Neuron config: {self.neuron_config}")

        self.args = self.neuron_config
        self.subnet_version = str(__spec_version__)

        bt.logging.info("Setting up wallet, subtensor, and metagraph")
        try:
            self.wallet, self.subtensor, self.metagraph = self.setup()
            self.miner_uid = str(
                self.metagraph.hotkeys.index(self.wallet.hotkey.ss58_address)
            )
            bt.logging.info(f"Miner initialized with UID: {self.miner_uid}")
        except Exception as e:
            bt.logging.error(f"Error in self.setup(): {e}")
            raise

        # Run health check
        db_params = {
            "db_name": self.args.db_name,
            "db_user": self.args.db_user,
            "db_password": self.args.db_password,
            "db_host": self.args.db_host,
            "db_port": self.args.db_port,
        }
        run_health_check(db_params, self.args.axon.port)

        # Initialize Redis interface
        self.redis_interface = RedisInterface(
            host=self.args.redis_host, port=self.args.redis_port
        )
        if not self.redis_interface.connect():
            bt.logging.warning(
                "Failed to connect to Redis. GUI interfaces will not be available."
            )
            self.gui_available = False
        else:
            bt.logging.info(
                "Redis connection successful. All interfaces (GUI and CLI) are available."
            )
            self.gui_available = True
            # Start Redis listener in a separate thread
            self.redis_thread = threading.Thread(target=self.listen_for_redis_messages)
            self.redis_thread.daemon = True
            self.redis_thread.start()

        # Setup database manager
        bt.logging.info("Initializing database manager")
        db_params = {
            "db_name": self.args.db_name,
            "db_user": self.args.db_user,
            "db_password": self.args.db_password,
            "db_host": self.args.db_host,
            "db_port": self.args.db_port,
            "max_connections": self.args.max_connections,
        }
        self.db_manager = DatabaseManager(**db_params)

        # Setup state manager
        bt.logging.info("Initializing state manager")
        self.miner_hotkey = self.wallet.hotkey.ss58_address
        self.miner_uid = self.miner_uid

        # Initialize state_manager before using it
        self.state_manager = MinerStateManager(
            db_manager=self.db_manager,
            miner_hotkey=self.miner_hotkey,
            miner_uid=self.miner_uid,
        )
        # Create an instance of MinerStatsHandler
        self.stats_handler = MinerStatsHandler(self.state_manager)

        # Setup handlers
        bt.logging.info("Initializing handlers")
        self.predictions_handler = PredictionsHandler(
            self.db_manager, self.state_manager, self.miner_hotkey
        )
        self.games_handler = GamesHandler(self.db_manager, self.predictions_handler)

        # Check and update miner_uid if necessary
        self.update_miner_uid_in_stats_db()

        # Setup cache manager
        bt.logging.info("Initializing cache manager")
        self.cache_manager = CacheManager()

        # Setup other attributes
        bt.logging.info("Setting other attributes")
        self.validator_min_stake = self.args.validator_min_stake
        self.hotkey = self.wallet.hotkey.ss58_address

        bt.logging.info("Setting up signal handlers")
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)

        self.hotkey_blacklisted = False

        bt.logging.info("BettensorMiner initialization complete")

        self.last_incentive_update = None
        self.incentive_update_interval = 600  # Update every 10 minutes

        # Initialize MinerConfig
        self.miner_config = MinerConfig()
        self.validator_confirmation_dict = self.stats_handler.load_validator_confirmation_dict()

        # Initialize WebSocket manager
        self.websocket_enabled = getattr(self.args, "websocket_enabled", "True").lower() == "true"
        self.websocket_manager = None
        self.websocket_connect_task = None
        
        # Ensure event loop exists for async methods
        try:
            self.loop = asyncio.get_event_loop()
        except RuntimeError:
            self.loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.loop)

    def forward(self, synapse: GameData) -> GameData:

        if synapse.metadata.synapse_type == "game_data":
            return_synapse = self._handle_game_data(synapse)
            #bt.logging.info(f"Return synapse: {return_synapse}")
            return return_synapse
        elif synapse.metadata.synapse_type == "confirmation":
            return_synapse = self._handle_confirmation(synapse)
            #bt.logging.info(f"Return synapse: {return_synapse}")
            return return_synapse
        else:
            raise ValueError(f"Unsupported synapse type: {type(synapse)}")

    def _handle_game_data(self, synapse: GameData) -> GameData:
        bt.logging.debug(f"Processing game data: {len(synapse.gamedata_dict)} games")

        try:
            # Process all games, regardless of changes
            updated_games, new_games = self.games_handler.process_games(
                synapse.gamedata_dict
            )

            # Get recent predictions from the database
            recent_predictions = self.predictions_handler.get_recent_predictions()

            # Process predictions for updated and new games
            processed_predictions = self.predictions_handler.process_predictions(
                updated_games, new_games
            )

            # Combine recent predictions with processed predictions
            all_predictions = {**recent_predictions, **processed_predictions}

            # Update cache with all predictions
            self.cache_manager.update_cached_predictions(all_predictions)

            if not all_predictions:
                bt.logging.warning("No predictions available")
                return self._clean_synapse(synapse, "No predictions available")

            # Filter out predictions with unfinished outcomes
            unfinished_predictions = {
                pred_id: pred
                for pred_id, pred in all_predictions.items()
                if pred.outcome == "Unfinished" or pred.outcome == "unfinished"
            }

            if not unfinished_predictions:
                bt.logging.warning("No unfinished predictions available")
                return self._clean_synapse(
                    synapse, "No unfinished predictions available"
                )

            synapse.prediction_dict = unfinished_predictions
            synapse.gamedata_dict = None
            synapse.metadata = self._create_metadata("prediction")

            bt.logging.info(
                f"Number of unfinished predictions added to synapse: {len(unfinished_predictions)}"
            )

            # Update validators_sent_to count for each prediction
            for pred_id in unfinished_predictions:
                self.predictions_handler.update_prediction_sent(pred_id,self.validator_confirmation_dict,synapse.dendrite.hotkey)

        except Exception as e:
            bt.logging.error(f"Error in forward method: {e}")
            return self._clean_synapse(synapse, f"Error in forward method: {e}")
        
        #bt.logging.info(f"Synapse after processing: {synapse}")

        return synapse

    def _handle_confirmation(self, synapse: GameData) -> GameData:
        bt.logging.debug(f"Processing confirmation from {synapse.dendrite.hotkey}")
        if synapse.confirmation_dict:
            miner_stats = synapse.confirmation_dict["miner_stats"]
            self.stats_handler.state_manager.update_stats_from_confirmation(miner_stats)
            prediction_ids = list(synapse.confirmation_dict.keys())
            self.predictions_handler.update_prediction_confirmations(
                prediction_ids, synapse.dendrite.hotkey, self.validator_confirmation_dict
            )
            self.stats_handler.save_validator_confirmation_dict(self.validator_confirmation_dict)
        else:
            bt.logging.warning("Received empty confirmation dict")
        return synapse

    def _check_version(self, synapse_version):
        if synapse_version > self.subnet_version:
            bt.logging.warning(
                f"Received a synapse from a validator with higher subnet version ({synapse_version}) than yours ({self.subnet_version}). Please update the miner, or you may encounter issues."
            )
        elif synapse_version < self.subnet_version:
            bt.logging.warning(
                f"Received a synapse from a validator with lower subnet version ({synapse_version}) than yours ({self.subnet_version}). You can safely ignore this warning."
            )

    def _create_metadata(self, synapse_type):
        return Metadata.create(
            subnet_version=self.subnet_version,
            neuron_uid=self.miner_uid,
            synapse_type=synapse_type,
        )

    def _clean_synapse(self, synapse: GameData, error: str) -> GameData:
        if not synapse.prediction_dict:
            bt.logging.warning("Cleaning synapse due to no predictions available")
            bt.logging.warning(
                f"If you have recently made predictions, please examine your logs for errors and reach out to the dev team if it persists."
            )
        else:
            bt.logging.error(f"Cleaning synapse due to error: {error}")

        synapse.gamedata_dict = None
        synapse.prediction_dict = None
        synapse.metadata = Metadata.create(
            subnet_version=self.subnet_version,
            neuron_uid=self.miner_uid,
            synapse_type="error",
        )
        synapse.error = error
        synapse.error = error
        bt.logging.debug("Synapse cleaned")
        return synapse

    def start(self):
        """Start the miner and all its components"""
        bt.logging.info(f"Starting miner with hotkey {self.wallet.hotkey.ss58_address}")
        
        # Reset daily cash 
        self.daily_cash['total'] = 0
        self.daily_cash['games'] = 0
        self.daily_cash['bets'] = 0
        
        # Start Redis message listening thread
        self.redis_message_thread = threading.Thread(target=self._listen_for_redis_messages, daemon=True)
        self.redis_message_thread.start()
        
        # Start health check thread
        self.health_check_thread = threading.Thread(target=self._health_check_loop, daemon=True)
        self.health_check_thread.start()
        
        # Start predictions check thread
        self.prediction_check_thread = threading.Thread(target=self._check_predictions_loop, daemon=True)
        self.prediction_check_thread.start()
        
        # Initialize WebSocket client if enabled
        if getattr(self.config, "websocket_enabled", False):
            try:
                from bettensor.miner.io.websocket_client import MinerWebSocketManager
                from bettensor.miner.io.predictions import PredictionManager
                
                # Parse WebSocket configuration
                heartbeat_interval = getattr(self.config, "websocket_heartbeat_interval", 60)
                reconnect_base_interval = getattr(self.config, "websocket_reconnect_base_interval", 1)
                max_reconnect_interval = getattr(self.config, "websocket_max_reconnect_interval", 60)
                
                # Create WebSocket manager
                self.websocket_manager = MinerWebSocketManager(
                    miner_hotkey=self.wallet.hotkey.ss58_address,
                    metagraph=self.metagraph,
                    timeout=30
                )
                
                # Set wallet for signing messages
                self.websocket_manager.set_wallet(self.wallet)
                
                # Apply configuration
                self.websocket_manager.set_config(
                    heartbeat_interval=heartbeat_interval,
                    reconnect_base_interval=reconnect_base_interval,
                    max_reconnect_interval=max_reconnect_interval
                )
                
                # Create prediction manager
                self.prediction_manager = PredictionManager(self)
                
                # Register handlers for WebSocket messages
                self.websocket_manager.register_confirmation_handler(self.prediction_manager.handle_confirmation)
                self.websocket_manager.register_game_update_handler(self.prediction_manager.handle_game_update)
                
                # Start WebSocket manager asynchronously
                asyncio.run_coroutine_threadsafe(self.websocket_manager.start(), self.loop)
                
                bt.logging.info(f"WebSocket client started with configuration: "
                               f"heartbeat={heartbeat_interval}s, "
                               f"reconnect_base={reconnect_base_interval}s, "
                               f"max_reconnect={max_reconnect_interval}s")
            except Exception as e:
                bt.logging.error(f"Failed to initialize WebSocket client: {str(e)}")
                bt.logging.error(traceback.format_exc())
        
        bt.logging.info("Miner started")

    def stop(self):
        """Stop the miner and all its components"""
        bt.logging.info("Stopping miner")
        
        # Stop WebSocket manager if enabled
        if hasattr(self, 'websocket_manager'):
            try:
                asyncio.run_coroutine_threadsafe(self.websocket_manager.stop(), self.loop)
                bt.logging.info("WebSocket manager stopped")
            except Exception as e:
                bt.logging.error(f"Error stopping WebSocket manager: {str(e)}")
        
        # Stop Redis client
        if hasattr(self, 'redis_client') and self.redis_client:
            self.redis_client.close()
            
        bt.logging.info("Miner stopped")

    def signal_handler(self, signum, frame):
        """Handle signal for graceful shutdown."""
        bt.logging.info(f"Received signal {signum}, stopping miner")
        self.stop()
        sys.exit(0)

    def setup(self) -> Tuple[bt.wallet, bt.subtensor, bt.metagraph]:
        bt.logging.info("Setting up bittensor objects")
        bt.logging(config=self.neuron_config, logging_dir=self.neuron_config.full_path)
        bt.logging.info(
            f"Initializing miner for subnet: {self.neuron_config.netuid} on network: {self.neuron_config.subtensor.chain_endpoint} with config:\n {self.neuron_config}"
        )

        try:
            wallet = bt.wallet(config=self.neuron_config)
            subtensor = bt.subtensor(config=self.neuron_config)
            metagraph = subtensor.metagraph(self.neuron_config.netuid)
        except AttributeError as e:
            bt.logging.error(f"Unable to setup bittensor objects: {e}")
            sys.exit()

        bt.logging.info(
            f"Bittensor objects initialized:\nMetagraph: {metagraph}\
            \nSubtensor: {subtensor}\nWallet: {wallet}"
        )

        if wallet.hotkey.ss58_address not in metagraph.hotkeys:
            bt.logging.error(
                f"Your miner: {wallet} is not registered to chain connection: {subtensor}. Run btcli register and try again"
            )
            sys.exit()

        bt.logging.info("Bittensor objects setup complete")
        return wallet, subtensor, metagraph

    def check_whitelist(self, hotkey):
        bt.logging.debug(f"Checking whitelist for hotkey: {hotkey}")
        if isinstance(hotkey, bool) or not isinstance(hotkey, str):
            bt.logging.debug(f"Invalid hotkey type: {type(hotkey)}")
            return False

        whitelisted_hotkeys = [
            "5HK5tp6t2S59DywmHRWPBVJeJ86T61KjurYqeooqj8sREpeN",
            "5F4tQyWrhfGVcNhoqeiNsR6KjD4wMZ2kfhLj4oHYuyHbZAc3",
            "5EhvL1FVkQPpMjZX4MAADcW42i3xPSF1KiCpuaxTYVr28sux",
            "5HbLYXUBy1snPR8nfioQ7GoA9x76EELzEq9j7F32vWUQHm1x",
            "5DvTpiniW9s3APmHRYn8FroUWyfnLtrsid5Mtn5EwMXHN2ed",
            "5Hb63SvXBXqZ8zw6mwW1A39fHdqUrJvohXgepyhp2jgWedSB",
        ]

        if hotkey in whitelisted_hotkeys:
            bt.logging.debug(f"Hotkey {hotkey} is whitelisted")
            return True

        bt.logging.debug(f"Hotkey {hotkey} is not whitelisted")
        return False

    def blacklist(self, synapse: GameData) -> Tuple[bool, str]:
        bt.logging.debug(
            f"Checking blacklist for synapse from {synapse.dendrite.hotkey}"
        )
        if self.check_whitelist(hotkey=synapse.dendrite.hotkey):
            bt.logging.info(f"Accepted whitelisted hotkey: {synapse.dendrite.hotkey}")
            return (False, f"Accepted whitelisted hotkey: {synapse.dendrite.hotkey}")

        if synapse.dendrite.hotkey not in self.metagraph.hotkeys:
            bt.logging.info(f"Blacklisted unknown hotkey: {synapse.dendrite.hotkey}")
            return (
                True,
                f"Hotkey {synapse.dendrite.hotkey} was not found from metagraph.hotkeys",
            )

        uid = self.metagraph.hotkeys.index(synapse.dendrite.hotkey)
        if not self.metagraph.validator_permit[uid]:
            bt.logging.info(f"Blacklisted non-validator: {synapse.dendrite.hotkey}")
            return (True, f"Hotkey {synapse.dendrite.hotkey} is not a validator")

        bt.logging.info(f"validator_min_stake: {self.validator_min_stake}")
        stake = float(self.metagraph.S[uid])
        if stake < self.validator_min_stake:
            bt.logging.info(
                f"Blacklisted validator {synapse.dendrite.hotkey} with insufficient stake: {stake}"
            )
            return (
                True,
                f"Hotkey {synapse.dendrite.hotkey} has insufficient stake: {stake}",
            )

        bt.logging.info(
            f"Accepted hotkey: {synapse.dendrite.hotkey} (UID: {uid} - Stake: {stake})"
        )
        return (False, f"Accepted hotkey: {synapse.dendrite.hotkey}")

    def priority(self, synapse: GameData) -> float:
        bt.logging.debug(
            f"Calculating priority for synapse from {synapse.dendrite.hotkey}"
        )

        if self.check_whitelist(hotkey=synapse.dendrite.hotkey):
            bt.logging.debug(
                f"Whitelisted hotkey {synapse.dendrite.hotkey}, returning max priority"
            )
            return 10000000.0

        uid = self.metagraph.hotkeys.index(synapse.dendrite.hotkey)
        stake = float(self.metagraph.S[uid])

        bt.logging.debug(
            f"Prioritized: {synapse.dendrite.hotkey} (UID: {uid} - Stake: {stake})"
        )
        return stake

    def get_current_incentive(self):
        current_time = time.time()

        # Check if it's time to update the incentive
        if (
            self.last_incentive_update is None
            or (current_time - self.last_incentive_update)
            >= self.incentive_update_interval
        ):
            bt.logging.info("Updating current incentive")
            try:
                # Sync the metagraph to get the latest data
                self.metagraph.sync()

                # Get the incentive for this miner
                miner_uid_int = int(self.miner_uid)
                incentive = (
                    self.metagraph.I[miner_uid_int].item()
                    if miner_uid_int < len(self.metagraph.I)
                    else 0.0
                )

                # Update the stats handler with the new incentive
                self.stats_handler.update_current_incentive(incentive)

                self.last_incentive_update = current_time

                bt.logging.info(f"Updated current incentive to: {incentive}")
                return incentive
            except Exception as e:
                bt.logging.error(f"Error updating current incentive: {e}")
                return None
        else:
            # If it's not time to update, return the last known incentive from the stats handler
            return self.stats_handler.get_current_incentive()

    def listen_for_redis_messages(self):
        channel = f"miner:{self.miner_uid}:{self.wallet.hotkey.ss58_address}"
        bt.logging.info(f"Starting to listen for Redis messages on channel: {channel}")

        while True:
            try:
                pubsub = self.redis_interface.subscribe(channel)
                if pubsub is None:
                    bt.logging.error("Failed to subscribe to Redis channel")
                    time.sleep(5)  # Wait before trying to reconnect
                    continue

                bt.logging.info(f"Successfully subscribed to Redis channel: {channel}")

                for message in pubsub.listen():
                    if message["type"] == "message":
                        try:
                            data = json.loads(message["data"])
                            bt.logging.info(f"Received message: {data}")

                            action = data.get("action")
                            if action is None:
                                bt.logging.warning(
                                    f"Received message without 'action' field: {data}"
                                )
                                continue

                            if action == "make_prediction":
                                result = self.process_prediction_request(data)

                                # Send the result back
                                response_key = (
                                    f'response:{data.get("message_id", "unknown")}'
                                )
                                bt.logging.info(
                                    f"Publishing response to key: {response_key}"
                                )
                                self.redis_interface.set(
                                    response_key, json.dumps(result), ex=60
                                )  # Set expiration to 60 seconds
                            elif action == "get_upcoming_game_ids":
                                self.handle_get_upcoming_game_ids(data)
                            else:
                                bt.logging.warning(f"Unknown action: {action}")
                        except json.JSONDecodeError:
                            bt.logging.error(
                                f"Failed to decode JSON message: {message['data']}"
                            )
                        except KeyError as e:
                            bt.logging.error(f"Missing key in message: {e}")
                        except Exception as e:
                            bt.logging.error(f"Error processing message: {str(e)}")

            except Exception as e:
                bt.logging.error(f"Error in Redis listener: {str(e)}")
                time.sleep(5)  # Wait before trying to reconnect

    def process_prediction_request(self, data):
        """Process a prediction request from the CLI or GUI."""
        start_time = time.time()
        try:
            bt.logging.info(f"Processing prediction request: {data}")
            
            predictions = data.get("predictions")
            if not predictions:
                bt.logging.warning(
                    "Received prediction request without 'predictions' field"
                )
                return self.create_prediction_response(False, {})

            results = []
            for prediction in predictions:
                game_id = prediction.get("game_id")
                if not game_id:
                    results.append(
                        {
                            "success": False,
                            "message": "Missing game_id in prediction request",
                        }
                    )
                    continue

                outcome = prediction.get("outcome")
                if not outcome:
                    results.append(
                        {
                            "success": False,
                            "message": "Missing outcome in prediction request",
                        }
                    )
                    continue

                odds = prediction.get("odds")
                if not odds:
                    results.append(
                        {
                            "success": False,
                            "message": "Missing odds in prediction request",
                        }
                    )
                    continue

                wager = prediction.get("wager")
                if not wager:
                    results.append(
                        {
                            "success": False,
                            "message": "Missing wager in prediction request",
                        }
                    )
                    continue

                model_name = prediction.get("model_name", "default")
                confidence_score = prediction.get("confidence_score", 0.5)

                # Check for valid wager
                try:
                    wager_float = float(wager)
                    if wager_float <= 0:
                        results.append(
                            {
                                "success": False,
                                "message": f"Invalid wager amount: {wager}. Must be positive.",
                            }
                        )
                        continue
                except ValueError:
                    results.append(
                        {
                            "success": False,
                            "message": f"Invalid wager format: {wager}. Must be a number.",
                        }
                    )
                    continue

                # Get game data
                game_data = self.predictions_handler.get_game_data(game_id)
                if not game_data:
                    results.append(
                        {
                            "success": False,
                            "message": f"Game {game_id} not found",
                        }
                    )
                    continue

                # Prepare prediction data
                prediction_data = {
                    "outcome": outcome,
                    "odds": odds,
                    "wager": wager,
                    "model_name": model_name,
                    "confidence_score": confidence_score
                }
                
                # Create and push prediction using async method
                result = self.loop.run_until_complete(
                    self.create_and_push_prediction(game_data, prediction_data)
                )
                
                results.append(result)

            # Create response
            response = self.create_prediction_response(True, {"results": results})
            
            return response
            
        except Exception as e:
            bt.logging.error(f"Error processing prediction request: {e}")
            bt.logging.debug(traceback.format_exc())
            return self.create_prediction_response(False, {"error": str(e)})
        finally:
            end_time = time.time()
            bt.logging.debug(f"Prediction request processed in {end_time - start_time:.2f} seconds")

    def create_prediction_response(self, success, original_response):
        bt.logging.info("Creating prediction response")
        current_cash = self.get_miner_cash_from_db()
        bt.logging.info(f"Current miner cash: {current_cash}")
        token_status = "VALID" if success else "INVALID"

        response = {"amountLeft": current_cash, "tokenStatus": token_status}

        bt.logging.info(f"Created prediction response: {response}")
        return response

    def get_miner_cash_from_db(self):
        bt.logging.info("Fetching miner cash directly from database")
        query = "SELECT miner_cash FROM miner_stats WHERE miner_uid = %s"
        try:
            conn, cur = self.db_manager.connection_pool.getconn(), None
            cur = conn.cursor()
            cur.execute(query, (self.miner_uid,))
            result = cur.fetchone()
            if result:
                cash = float(result[0])
                bt.logging.info(f"Fetched miner cash from DB: {cash}")
                return cash
            else:
                bt.logging.warning("No miner cash found in database")
                return 0.0
        except Exception as e:
            bt.logging.error(f"Error fetching miner cash from database: {str(e)}")
            return 0.0
        finally:
            if cur:
                cur.close()
            if conn:
                self.db_manager.connection_pool.putconn(conn)

    def handle_get_upcoming_game_ids(self, data):
        upcoming_game_ids = self.games_handler.get_upcoming_game_ids()

        response = json.dumps(upcoming_game_ids)
        self.redis_interface.set(
            f"response:{data['message_id']}", response, ex=60
        )  # Expire after 60 seconds

    def health_check(self):
        while True:
            bt.logging.info("Miner health check: Still listening for Redis messages")
            time.sleep(300)  # Check every 5 minutes

    def update_miner_uid_in_stats_db(self):
        bt.logging.info("Checking miner_uid in stats database")

        try:
            with self.db_manager.connection_pool.getconn() as conn:
                with conn.cursor() as cur:
                    # Cast miner_uid to string right before the query
                    current_miner_uid = str(self.miner_uid)

                    # Check for existing entry with matching hotkey
                    cur.execute(
                        """
                        SELECT miner_uid FROM miner_stats 
                        WHERE miner_hotkey = %s
                    """,
                        (self.miner_hotkey,),
                    )

                    result = cur.fetchone()

                    if result:
                        existing_miner_uid = str(
                            result[0]
                        )  # Ensure existing_miner_uid is also a string
                        if existing_miner_uid != current_miner_uid:
                            bt.logging.warning(
                                f"Miner UID changed for hotkey {self.miner_hotkey}. Old UID: {existing_miner_uid}, New UID: {current_miner_uid}"
                            )

                            # Update the miner_uid in the miner_stats table
                            cur.execute(
                                """
                                UPDATE miner_stats 
                                SET miner_uid = %s 
                                WHERE miner_hotkey = %s
                            """,
                                (current_miner_uid, self.miner_hotkey),
                            )

                            # Delete all predictions for the old miner_uid
                            cur.execute(
                                "DELETE FROM predictions WHERE minerid = %s",
                                (existing_miner_uid,),
                            )

                            conn.commit()

                            bt.logging.warning(
                                f"Updated miner_uid from {existing_miner_uid} to {current_miner_uid}"
                            )
                            bt.logging.warning(
                                "Deleted all predictions associated with the old miner_uid"
                            )

                            # Reset the miner's stats
                            self.state_manager.initialize_state()
                        else:
                            bt.logging.info(
                                "Miner UID is up to date. No changes necessary."
                            )
                    else:
                        bt.logging.info(
                            "No existing miner stats found. A new entry will be created during initialization."
                        )
                        self.state_manager.initialize_state()

        except Exception as e:
            bt.logging.error(f"Error checking miner_uid in stats database: {str(e)}")
            bt.logging.error(traceback.format_exc())

        # Ensure the state is properly loaded
        self.state_manager.load_state()
        self.stats_handler.load_stats_from_state()

    async def create_and_push_prediction(self, game_data, prediction_data):
        """
        Create and push a prediction to validators via WebSocket and/or HTTP.
        
        Args:
            game_data: Game data for the prediction
            prediction_data: Prediction data to send
        """
        try:
            # Create prediction object
            prediction_id = str(uuid.uuid4())
            team_a = game_data.get("team_a", "Team A")
            team_b = game_data.get("team_b", "Team B")
            
            # Fill in required fields
            prediction = TeamGamePrediction(
                prediction_id=prediction_id,
                game_id=game_data["external_id"],
                miner_uid=self.miner_uid,
                prediction_date=datetime.now(timezone.utc).isoformat(),
                predicted_outcome=prediction_data["outcome"],
                predicted_odds=float(prediction_data["odds"]),
                team_a=team_a,
                team_b=team_b,
                wager=float(prediction_data["wager"]),
                team_a_odds=float(game_data["team_a_odds"]),
                team_b_odds=float(game_data["team_b_odds"]),
                tie_odds=float(game_data.get("tie_odds", 0)),
                model_name=prediction_data.get("model_name", "default"),
                confidence_score=float(prediction_data.get("confidence_score", 0.5)),
                outcome="Unfinished",
                payout=0.0
            )
            
            # Save prediction to database
            self.predictions_handler.save_prediction(prediction)
            
            # Push via WebSocket if enabled
            if self.websocket_enabled and self.websocket_manager:
                await self.websocket_manager.send_prediction(prediction)
                bt.logging.info(f"Pushed prediction {prediction_id} via WebSocket")
            
            return {
                "success": True,
                "prediction_id": prediction_id,
                "message": "Prediction pushed to validators"
            }
        
        except Exception as e:
            bt.logging.error(f"Error creating prediction: {e}")
            bt.logging.debug(traceback.format_exc())
            
            return {
                "success": False,
                "message": f"Error creating prediction: {str(e)}"
            }

    async def handle_ws_confirmation(self, validator_hotkey, confirmation):
        """
        Handle a confirmation message received via WebSocket.
        
        Args:
            validator_hotkey: The validator's hotkey
            confirmation: The ConfirmationMessage object
        """
        try:
            bt.logging.info(f"Received confirmation from validator {validator_hotkey} for prediction {confirmation.prediction_id}")
            
            # Update prediction confirmation
            if confirmation.prediction_id != "miner_stats" and confirmation.prediction_id != "error":
                self.predictions_handler.update_prediction_confirmations(
                    [confirmation.prediction_id], 
                    validator_hotkey, 
                    self.validator_confirmation_dict
                )
            
            # Update miner stats if included
            if confirmation.miner_stats:
                self.stats_handler.state_manager.update_stats_from_confirmation(confirmation.miner_stats)
            
            # Save validator confirmation dict
            self.stats_handler.save_validator_confirmation_dict(self.validator_confirmation_dict)
            
        except Exception as e:
            bt.logging.error(f"Error handling WebSocket confirmation: {e}")
            bt.logging.debug(traceback.format_exc())

    async def handle_ws_game_update(self, validator_hotkey, game_update):
        """
        Handle a game update message received via WebSocket.
        
        Args:
            validator_hotkey: The validator's hotkey
            game_update: The GameUpdateMessage object
        """
        try:
            bt.logging.info(f"Received game updates from validator {validator_hotkey}")
            
            # Process updated games
            if game_update.updated_games:
                updated_games, new_games = self.games_handler.process_games(game_update.updated_games)
                
                # Process predictions for updated games
                self.predictions_handler.process_predictions(updated_games, new_games)
                
                bt.logging.info(f"Processed {len(updated_games)} updated games and {len(new_games)} new games")
            
        except Exception as e:
            bt.logging.error(f"Error handling WebSocket game update: {e}")
            bt.logging.debug(traceback.format_exc())

    @classmethod
    def add_args(cls, parser):
        """Add miner specific arguments to the parser."""
        super().add_args(parser)

        # Database connection parameters
        parser.add_argument(
            "--db_name",
            type=str,
            default="bettensor_miner",
            help="Database name",
        )
        parser.add_argument(
            "--db_user",
            type=str,
            default="postgres",
            help="Database user",
        )
        parser.add_argument(
            "--db_password",
            type=str,
            default="postgres",
            help="Database password",
        )
        parser.add_argument(
            "--db_host",
            type=str,
            default="localhost",
            help="Database host",
        )
        parser.add_argument(
            "--db_port",
            type=int,
            default=5432,
            help="Database port",
        )
        parser.add_argument(
            "--max_connections",
            type=int,
            default=10,
            help="Maximum number of database connections",
        )

        # Redis connection parameters
        parser.add_argument(
            "--redis_host",
            type=str,
            default="localhost",
            help="Redis host",
        )
        parser.add_argument(
            "--redis_port",
            type=int,
            default=6379,
            help="Redis port",
        )

        # Validator min stake
        parser.add_argument(
            "--validator_min_stake",
            type=float,
            default=0.0,
            help="Minimum stake required for a validator to be considered valid",
        )
        
        # WebSocket configuration
        parser.add_argument(
            "--websocket_enabled",
            type=str,
            default="True",
            help="Enable WebSocket connections for real-time communications (True/False)",
        )
        parser.add_argument(
            "--websocket_heartbeat_interval",
            type=int,
            default=60,
            help="Interval in seconds between heartbeat messages",
        )
        parser.add_argument(
            "--websocket_reconnect_base_interval",
            type=int,
            default=5,
            help="Base interval in seconds for reconnection attempts",
        )
        parser.add_argument(
            "--websocket_max_reconnect_interval",
            type=int,
            default=300,
            help="Maximum interval in seconds for reconnection attempts",
        )
        
        return parser
