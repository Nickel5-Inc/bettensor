import asyncio
import os
import sys
import copy
import json
import math
import time
import traceback
import uuid
import torch
import numpy as np
import requests
import bittensor as bt
import concurrent.futures
import argparse
from os import path, rename
from copy import deepcopy
from pathlib import Path
from dotenv import load_dotenv
from typing import Dict, Tuple
from datetime import datetime, timedelta, timezone
from bettensor.base.neuron import BaseNeuron
from bettensor.protocol import TeamGamePrediction
from bettensor.validator.utils.io.website_handler import WebsiteHandler
from bettensor.validator.utils.scoring.scoring import ScoringSystem
from .utils.scoring.entropy_system import EntropySystem
from bettensor.validator.utils.io.sports_data import SportsData
from bettensor.validator.utils.scoring.weights_functions import WeightSetter
from bettensor.validator.utils.database.database_manager import DatabaseManager
from bettensor.validator.utils.io.miner_data import MinerDataMixin
from bettensor.validator.utils.io.bettensor_api_client import BettensorAPIClient
from bettensor.validator.utils.scoring.min_stake import MinStakeService
from bettensor.validator.utils.io.base_api_client import BaseAPIClient
from bettensor.validator.utils.scoring.watchdog import Watchdog
from bettensor import __spec_version__
from types import SimpleNamespace
import configparser

DEFAULT_DB_PATH = "./bettensor/validator/state/validator.db"


class BettensorValidator(BaseNeuron, MinerDataMixin):
    """
    Bettensor Validator Class, Extends the BaseNeuron Class and MinerDataMixin Class

    Contains top-level methods for validator operations.
    """

    @classmethod
    def add_args(cls, parser):
        """Add validator specific arguments to the parser."""
        super().add_args(parser)
        parser.add_argument(
            "--db",
            type=str,
            default=DEFAULT_DB_PATH,
            help="Path to the validator database"
        )
        parser.add_argument(
            "--load_state",
            type=str,
            default="True",
            help="WARNING: Setting this value to False clears the old state."
        )
        parser.add_argument(
            "--max_targets",
            type=int,
            default=256,
            help="Sets the value for the number of targets to query - set to 256 to ensure all miners are queried"
        )
        parser.add_argument(
            "--alpha",
            type=float,
            default=0.9,
            help="The alpha value for the validator."
        )

    @classmethod
    def config(cls):
        """Get config from the argument parser."""
        parser = argparse.ArgumentParser()
        bt.wallet.add_args(parser)
        bt.subtensor.add_args(parser)
        bt.logging.add_args(parser)
        bt.axon.add_args(parser)
        cls.add_args(parser)
        return bt.config(parser)

    def __init__(self, config=None):
        """Initialize the validator."""
        super().__init__(config)
        
        # Initialize neuron_config with alpha from config
        self.neuron_config = SimpleNamespace(alpha=getattr(config, 'alpha', 0.9))
        
        # Initialize validator-specific attributes
        self.timeout = 20
        self.wallet = None
        self.dendrite = None
        self.metagraph = None
        self.scores = None
        self.hotkeys = None
        self.subtensor = None
        #self.min_stake_service = None
        self.axon_port = getattr(self.config, "axon.port", None)
        self.base_path = "./bettensor/validator/"
        self.max_targets = None
        self.target_group = None
        self.blacklisted_miner_hotkeys = None
        self.load_validator_state = None
        self.data_entry = None
        self.uid = None
        self.miner_responses = None
        self.db_path = DEFAULT_DB_PATH
        self.last_stats_update = datetime.now(timezone.utc).date() - timedelta(days=1)
        self.last_api_call = datetime.now(timezone.utc) - timedelta(days=15)
        self.is_primary = None
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=self.determine_max_workers())
        self.subnet_version = str(__spec_version__)  # Convert to string

        self.last_queried_block = 0
        self.last_sent_data_to_website = 0
        self.last_scoring_block = 0
        self.last_set_weights_block = 0
        self.operation_lock = asyncio.Lock()
        self.watchdog = None

        self.is_initialized = False

    @classmethod
    async def create(cls):
        """Create a new validator instance."""
        self = cls(config=cls.config())
        await self.initialize_neuron()
        return self

    def apply_config(self, bt_classes) -> bool:
        """Applies the configuration to specified bittensor classes"""
        try:
            # Apply configuration
            self.neuron_config = self.config(bt_classes=bt_classes)
            
            # Parse arguments after configuration
            self.args = self.parser.parse_args()
            
        except AttributeError as e:
            bt.logging.error(f"Unable to apply validator configuration: {e}")
            raise AttributeError from e
        except OSError as e:
            bt.logging.error(f"Unable to create logging directory: {e}")
            raise OSError from e

        return True

    def determine_max_workers(self):
        num_cores = os.cpu_count()
        if num_cores <= 4:
            return max(1, num_cores - 1)
        else:
            return max(1, num_cores - 2)

    def initialize_connection(self):
        max_retries = 5
        retry_delay = 10
        for attempt in range(max_retries):
            try:
                self.subtensor = bt.subtensor(config=self.config)
                bt.logging.info(f"Connected to {self.config.subtensor.network} network")
                return self.subtensor
            except Exception as e:
                bt.logging.error(f"Failed to initialize subtensor (attempt {attempt+1}/{max_retries}): {str(e)}")
                if attempt < max_retries - 1:
                    bt.logging.info(f"Retrying in {retry_delay} seconds...")
                    time.sleep(retry_delay)
                else:
                    bt.logging.error("Max retries reached. Unable to initialize subtensor.")
                    self.subtensor = None
        return self.subtensor

    def print_chain_endpoint(self):
        if self.subtensor:
            bt.logging.info(f"Current chain endpoint: {self.subtensor.chain_endpoint}")
        else:
            bt.logging.info("Subtensor is not initialized yet.")

    def get_subtensor(self):
        if self.subtensor is None:
            self.subtensor = self.initialize_connection()
        return self.subtensor

    def sync_metagraph(self):
        subtensor = self.get_subtensor()
        self.metagraph.sync(subtensor=subtensor, lite=True)
        return self.metagraph

    def check_vali_reg(self, metagraph, wallet, subtensor) -> bool:
        """validates the validator has registered correctly"""
        if wallet.hotkey.ss58_address not in metagraph.hotkeys:
            bt.logging.error(
                f"your validator: {wallet} is not registered to chain connection: {subtensor}. run btcli register and try again"
            )
            return False

        return True

    def setup_bittensor_objects(
        self, config
    ) -> Tuple[bt.wallet, bt.subtensor, bt.dendrite, bt.metagraph]:
        """sets up the bittensor objects"""
        try:
            wallet = bt.wallet(config=config)
            subtensor = bt.subtensor(config=config)
            dendrite = bt.dendrite(wallet=wallet)
            metagraph = subtensor.metagraph(config.netuid)
        except AttributeError as e:
            bt.logging.error(f"unable to setup bittensor objects: {e}")
            raise AttributeError from e

        self.hotkeys = copy.deepcopy(metagraph.hotkeys)

        return wallet, subtensor, dendrite, metagraph

    def serve_axon(self):
        """Serve the axon to the network"""
        bt.logging.info("Serving axon...")

        self.axon = bt.axon(wallet=self.wallet)

        self.axon.serve(netuid=self.config.netuid, subtensor=self.subtensor)

    async def initialize_neuron(self):
        bt.logging(config=self.config, logging_dir=self.config.full_path)
        bt.logging.info(
            f"Initializing validator for subnet: {self.config.netuid} on network: {self.config.subtensor.chain_endpoint} with config: {self.config}"
        )

        # Setup the bittensor objects
        self.wallet, self.subtensor, self.dendrite, self.metagraph = self.setup_bittensor_objects(
            self.config
        )

        bt.logging.info(
            f"Bittensor objects initialized:\nmetagraph: {self.metagraph}\nsubtensor: {self.subtensor}\nwallet: {self.wallet}"
        )

        # Validate that the validator has registered to the metagraph correctly
        if not self.validator_validation(self.metagraph, self.wallet, self.subtensor):
            raise IndexError("Unable to find validator key from metagraph")

        # Get the unique identity (uid) from the network
        validator_uid = self.metagraph.hotkeys.index(self.wallet.hotkey.ss58_address)

        self.uid = validator_uid
        bt.logging.info(f"Validator is running with uid: {validator_uid}")

        if self.metagraph is not None:
            self.scores = torch.zeros(len(self.metagraph.uids), dtype=torch.float32)

        # Handle load_state configuration
        load_state_str = getattr(self.config, 'load_state', 'True')
        self.load_validator_state = load_state_str.lower() != "false" if isinstance(load_state_str, str) else True

        if self.load_validator_state:
            await self.load_state()
        else:
            self.init_default_scores()

        self.max_targets = getattr(self.config, 'max_targets', 256)
        self.target_group = 0

        # Initialize the database manager
        self.db_manager = DatabaseManager(self.db_path)
        await self.db_manager.initialize()

        # Initialize MinerDataMixin with required parameters
        MinerDataMixin.__init__(self, self.db_manager, self.metagraph, set(self.metagraph.uids))

        # Read force_rebuild_scores from setup.cfg
        config_parser = configparser.ConfigParser()
        try:
            config_parser.read('setup.cfg')
            force_rebuild = config_parser.getboolean('metadata', 'force_rebuild_scores', fallback=False)
            bt.logging.info(f"Force rebuild scores setting: {force_rebuild}")
        except configparser.Error as e:
            bt.logging.error(f"Error reading force_rebuild_scores from setup.cfg: {e}")
            force_rebuild = False

        # Create a MinerDataMixin instance for the scoring system
        scoring_miner_data = MinerDataMixin(self.db_manager, self.metagraph, set(self.metagraph.uids))

        # Initialize the MinStakeService
        self.min_stake_service = MinStakeService(self.subtensor)

        # Initialize the scoring system with force_rebuild setting and miner_data
        self.scoring_system = ScoringSystem(
            self.db_manager,
            num_miners=256,
            max_days=45,
            current_date=datetime.now(timezone.utc),
            force_rebuild=force_rebuild,
            min_stake_service=self.min_stake_service
        )
        # Set the validator and miner_data attributes
        self.scoring_system.set_validator(self)
        self.scoring_system.miner_data = scoring_miner_data
        await self.scoring_system.initialize()

        


        # After initialization, set force_rebuild_scores back to False to prevent future automatic rebuilds
        if force_rebuild:
            try:
                config_parser.set('metadata', 'force_rebuild_scores', 'False')
                with open('setup.cfg', 'w') as f:
                    config_parser.write(f)
                bt.logging.info("Reset force_rebuild_scores to False after initialization")
            except Exception as e:
                bt.logging.error(f"Error resetting force_rebuild_scores in setup.cfg: {e}")

        # Setup Validator Components
        self.api_client = BettensorAPIClient(self.db_manager)

        self.sports_data = SportsData(
            db_manager=self.db_manager,
            api_client=self.api_client,
            entropy_system=self.scoring_system.entropy_system,
        )

        self.weight_setter = WeightSetter(
            metagraph=self.metagraph,
            wallet=self.wallet,
            subtensor=self.subtensor,
            neuron_config=self.config,
            db_path=self.db_path,
        )

        self.website_handler = WebsiteHandler(self)

        self.is_initialized = True
        return True


    def _parse_args(self, parser):
        """Parses the command line arguments"""
        return parser.parse_args()

    def validator_validation(self, metagraph, wallet, subtensor) -> bool:
        """This method validates the validator has registered correctly"""
        if wallet.hotkey.ss58_address not in metagraph.hotkeys:
            bt.logging.error(
                f"Your validator: {wallet} is not registered to chain connection: {subtensor}. Run btcli register and try again"
            )
            return False

        return True

    async def check_hotkeys(self):
        """Checks if some hotkeys have been replaced in the metagraph"""
        bt.logging.info("Checking metagraph hotkeys for changes")
        try:
            if self.scores is None:
                if self.metagraph is not None:
                    self.scores = torch.zeros(len(self.metagraph.uids), dtype=torch.float32)
                else:
                    bt.logging.warning("Metagraph is None, unable to initialize scores")
                    return
                    
            if self.hotkeys:
                # Check if known state len matches with current metagraph hotkey length
                if len(self.hotkeys) == len(self.metagraph.hotkeys):
                    current_hotkeys = self.metagraph.hotkeys
                    for i, hotkey in enumerate(current_hotkeys):
                        if self.hotkeys[i] != hotkey:
                            bt.logging.debug(
                                f"Index '{i}' has mismatching hotkey. Old hotkey: '{self.hotkeys[i]}', New hotkey: '{hotkey}'. Resetting score to 0.0"
                            )
                            self.scores[i] = 0.0
                            await self.scoring_system.reset_miner(i)
                            await self.save_state()
                else:
                    bt.logging.info(
                        f"Init default scores because of state and metagraph hotkey length mismatch. Expected: {len(self.metagraph.hotkeys)} Had: {len(self.hotkeys)}"
                    )
                    self.init_default_scores()

                self.hotkeys = copy.deepcopy(self.metagraph.hotkeys)
            else:
                self.hotkeys = copy.deepcopy(self.metagraph.hotkeys)
                
        except Exception as e:
            bt.logging.error(f"Error in check_hotkeys: {e}")
            bt.logging.error(traceback.format_exc())

    def init_default_scores(self):
        """Initialize default scores for all miners in the network. This method is
        used to reset the scores in case of an internal error"""

        bt.logging.info("Initiating validator with default scores for all miners")

        if self.metagraph is None or self.metagraph.S is None:
            bt.logging.error("Metagraph or metagraph.S is not initialized")
            self.scores = torch.zeros(1, dtype=torch.float32)
        else:
            # Convert numpy array to PyTorch tensor
            if isinstance(self.metagraph.S, np.ndarray):
                metagraph_S_tensor = torch.from_numpy(self.metagraph.S).float()
            elif isinstance(self.metagraph.S, torch.Tensor):
                metagraph_S_tensor = self.metagraph.S.float()
            else:
                bt.logging.error(
                    f"Unexpected type for metagraph.S: {type(self.metagraph.S)}"
                )
                metagraph_S_tensor = torch.zeros(
                    len(self.metagraph.hotkeys), dtype=torch.float32
                )
                self.scores = torch.zeros_like(metagraph_S_tensor, dtype=torch.float32)

        bt.logging.info(f"Validation weights have been initialized: {self.scores}")

    async def save_state(self):
        """Saves the state of the validator to a file"""
        bt.logging.info("Saving validator state")

        bt.logging.info(f"Last api call, save_state: {self.last_api_call}")

        if isinstance(self.last_api_call, str):
            self.last_api_call = datetime.fromisoformat(self.last_api_call)

        bt.logging.info(f"Last api call, save_state: {self.last_api_call}")
        timestamp = self.last_api_call.timestamp()

        # Save the state of the validator to file
        torch.save(
            {
                "step": self.step,
                "scores": self.scores,
                "hotkeys": self.hotkeys,
                "last_updated_block": self.last_updated_block,
                "blacklisted_miner_hotkeys": self.blacklisted_miner_hotkeys,
                "last_api_call": timestamp,
            },
            self.base_path + "state/state.pt",
        )

        # bt.logging.debug(
        #     f"Saved the following state to a file: step: {self.step}, scores: {self.scores}, hotkeys: {self.hotkeys}, "
        #     f"last_updated_block: {self.last_updated_block}, blacklisted_miner_hotkeys: {self.blacklisted_miner_hotkeys}, "
        #     f"last_api_call: {timestamp}"
        # )

    async def reset_validator_state(self, state_path):
        """Inits the default validator state. Should be invoked only
        when an exception occurs and the state needs to reset"""

        # Rename current state file in case manual recovery is needed
        rename(
            state_path,
            f"{state_path}-{int(datetime.now().timestamp())}.autorecovery",
        )

        self.init_default_scores()
        self.step = 0
        self.last_updated_block = 0
        self.hotkeys = None
        self.blacklisted_miner_hotkeys = None

    async def load_state(self):
        state_path = self.base_path + "state/state.pt"
        if path.exists(state_path):
            try:
                bt.logging.info("Loading validator state")
                # Remove weights_only since we trust our own state file
                state = torch.load(state_path, map_location='cpu')
                bt.logging.debug(f"Loaded the following state from file: {state}")
                
                # Safely load each state component
                self.step = int(state.get("step", 0))
                self.scores = state.get("scores", torch.zeros(1, dtype=torch.float32))
                self.hotkeys = state.get("hotkeys", [])
                self.last_updated_block = int(state.get("last_updated_block", 0))
                self.blacklisted_miner_hotkeys = state.get("blacklisted_miner_hotkeys", [])

                # Convert timestamps back to datetime
                last_api_call = state.get("last_api_call")
                if last_api_call is None:
                    self.last_api_call = datetime.now(timezone.utc) - timedelta(minutes=30)
                else:
                    try:
                        self.last_api_call = datetime.fromtimestamp(last_api_call, tz=timezone.utc)
                    except (ValueError, TypeError, OverflowError) as e:
                        bt.logging.warning(
                            f"Invalid last_api_call timestamp: {last_api_call}. Using current time. Error: {e}"
                        )
                        self.last_api_call = datetime.now(timezone.utc)

            except Exception as e:
                bt.logging.error(
                    f"Validator state reset because an exception occurred: {e}"
                )
                await self.reset_validator_state(state_path=state_path)
        else:
            self.init_default_scores()

    async def _get_local_miner_blacklist(self) -> list:
        """Returns the blacklisted miners hotkeys from the local file"""

        # Check if local blacklist exists
        blacklist_file = f"{self.base_path}state/miner_blacklist.json"
        if Path(blacklist_file).is_file():
            # Load the contents of the local blacklist
            bt.logging.trace(f"Reading local blacklist file: {blacklist_file}")
            try:
                with open(blacklist_file, "r", encoding="utf-8") as file:
                    file_content = file.read()

                miner_blacklist = json.loads(file_content)
                if self.validate_miner_blacklist(miner_blacklist):
                    bt.logging.trace(f"Loaded miner blacklist: {miner_blacklist}")
                    return miner_blacklist

                bt.logging.trace(
                    f"Loaded miner blacklist was formatted incorrectly or was empty: {miner_blacklist}"
                )
            except OSError as e:
                bt.logging.error(f"Unable to read blacklist file: {e}")
            except json.JSONDecodeError as e:
                bt.logging.error(
                    f"Unable to parse json from path: {blacklist_file} with error: {e}"
                )
        else:
            bt.logging.trace(f"No local miner blacklist file in path: {blacklist_file}")

        return []

    def validate_miner_blacklist(self, miner_blacklist) -> bool:
        """Validates the miner blacklist. Checks if the list is not empty and if all the hotkeys are in the metagraph"""
        blacklist_file = f"{self.base_path}/miner_blacklist.json"
        if not miner_blacklist:
            return False
        if not all(hotkey in self.metagraph.hotkeys for hotkey in miner_blacklist):
            # Update the blacklist with the valid hotkeys
            valid_hotkeys = [
                hotkey for hotkey in miner_blacklist if hotkey in self.metagraph.hotkeys
            ]
            self.blacklisted_miner_hotkeys = valid_hotkeys
            # Overwrite the old blacklist with the new blacklist
            with open(blacklist_file, "w", encoding="utf-8") as file:
                json.dump(valid_hotkeys, file)
        return True

    def get_uids_to_query(self, all_axons) -> list:
        """Returns the list of uids to query"""

        # Define all_uids at the beginning
        all_uids = set(range(len(self.metagraph.hotkeys)))

        # Get uids with a positive stake
        uids_with_stake = self.metagraph.total_stake >= 0.0
        #bt.logging.trace(f"Uids with a positive stake: {uids_with_stake}")

        # Get uids with an ip address of 0.0.0.0
        invalid_uids = torch.tensor(
            [
                bool(value)
                for value in [
                    ip != "0.0.0.0"
                    for ip in [
                        self.metagraph.neurons[uid].axon_info.ip
                        for uid in self.metagraph.uids.tolist()
                    ]
                ]
            ],
            dtype=torch.bool,
        )

        # Append the validator's axon to invalid_uids
        invalid_uids[self.uid] = True

        #bt.logging.trace(f"Uids with 0.0.0.0 as an ip address or validator's axon: {invalid_uids}")

        # Get uids that have their hotkey blacklisted
        blacklisted_uids = []
        if self.blacklisted_miner_hotkeys:
            for hotkey in self.blacklisted_miner_hotkeys:
                if hotkey in self.metagraph.hotkeys:
                    blacklisted_uids.append(self.metagraph.hotkeys.index(hotkey))
                else:
                    bt.logging.trace(
                        f"Blacklisted hotkey {hotkey} was not found from metagraph"
                    )

            bt.logging.debug(f"Blacklisted the following uids: {blacklisted_uids}")

        # Convert blacklisted uids to tensor
        blacklisted_uids_tensor = torch.tensor(
            [uid not in blacklisted_uids for uid in self.metagraph.uids.tolist()],
            dtype=torch.bool,
        )

        #bt.logging.trace(f"Blacklisted uids: {blacklisted_uids_tensor}")

        # Determine the uids to filter
        uids_to_filter = torch.logical_not(
            ~blacklisted_uids_tensor | ~invalid_uids | ~uids_with_stake
        )

        #bt.logging.trace(f"Uids to filter: {uids_to_filter}")

        # Define uids to query
        uids_to_query = [
            axon
            for axon, keep_flag in zip(all_axons, uids_to_filter)
            if keep_flag.item()
        ]

        # Define uids to filter
        final_axons_to_filter = [
            axon
            for axon, keep_flag in zip(all_axons, uids_to_filter)
            if not keep_flag.item()
        ]

        uids_not_to_query = [
            self.metagraph.hotkeys.index(axon.hotkey) for axon in final_axons_to_filter
        ]

        #bt.logging.trace(f"Final axons to filter: {final_axons_to_filter}")
        #bt.logging.debug(f"Filtered uids: {uids_not_to_query}")

        # Reduce the number of simultaneous uids to query
        if self.max_targets < 256:
            start_idx = self.max_targets * self.target_group
            end_idx = min(
                len(uids_to_query), self.max_targets * (self.target_group + 1)
            )
            if start_idx == end_idx:
                return [], []
            if start_idx >= len(uids_to_query):
                raise IndexError(
                    "Starting index for querying the miners is out-of-bounds"
                )

            if end_idx >= len(uids_to_query):
                end_idx = len(uids_to_query)
                self.target_group = 0
            else:
                self.target_group += 1

            bt.logging.debug(
                f"List indices for uids to query starting from: '{start_idx}' ending with: '{end_idx}'"
            )
            uids_to_query = uids_to_query[start_idx:end_idx]

        list_of_uids = [
            self.metagraph.hotkeys.index(axon.hotkey) for axon in uids_to_query
        ]

        list_of_hotkeys = [axon.hotkey for axon in uids_to_query]

        #bt.logging.trace(f"Sending query to the following hotkeys: {list_of_hotkeys}")

        return uids_to_query, list_of_uids, blacklisted_uids, uids_not_to_query

    def set_weights(self, scores):
        try:
            return self.weight_setter.set_weights(scores)
        except StopIteration:
            bt.logging.warning(
                "StopIteration encountered in set_weights. Handling gracefully."
            )
            return None
        except Exception as e:
            bt.logging.warning(f"Set weights failed: {str(e)}")
            if "WeightVecLengthIsLow" in str(e):
                # Handle this specific error gracefully
                return None
            raise  # Re-raise other exceptions

    def reset_scoring_system(self):
        """
        Resets the scoring/database system across all validators.
        """
        try:
            bt.logging.info("Resetting scoring system(deleting state files)...")
                        
            # Delete state files (./bettensor/validator/state/state.pt, ./bettensor/validator/state/validator.db, ./bettensor/validator/state/entropy_system_state.json)
            for file in ["./bettensor/validator/state/state.pt", "./bettensor/validator/state/validator.db", "./bettensor/validator/state/entropy_system_state.json"]:
                if os.path.exists(file):
                    os.remove(file)
            
        except Exception as e:
            bt.logging.error(f"Error resetting scoring system: {e}")
            bt.logging.error(traceback.format_exc())
            raise

    async def _handle_shutdown(self, signum, frame):
        """Handle shutdown signals gracefully"""
        signal_names = {
            2: "SIGINT",
            15: "SIGTERM",
            1: "SIGHUP"
        }
        signal_name = signal_names.get(signum, f"Signal {signum}")
        
        bt.logging.info(f"Received {signal_name}. Starting graceful shutdown...")
        
        try:
            # Set shutdown flag to prevent new operations
            self.is_shutting_down = True
            
            # Cancel any pending tasks
            if hasattr(self, 'watchdog'):
                self.watchdog.cleanup()
                
            # Wait briefly for tasks to complete
            await asyncio.sleep(1)
            
            # Save state before database cleanup
            try:
                await self.save_state()
            except Exception as e:
                bt.logging.error(f"Error saving state during shutdown: {e}")
            
            # Cleanup database connections
            if hasattr(self, 'db_manager'):
                bt.logging.info("Cleaning up database connections...")
                try:
                    await self.db_manager.cleanup()
                except Exception as e:
                    bt.logging.error(f"Error cleaning up database: {e}")
                
            bt.logging.info("Shutdown completed successfully")
            sys.exit(0)
            
        except Exception as e:
            bt.logging.error(f"Error during {signal_name} shutdown: {e}")
            bt.logging.error(traceback.format_exc())
            sys.exit(1)

    async def query_and_process_axons(self):
        async def db_operation():
            # Wrap database operations
            return await self.db_manager.execute_query(...)
        
        result = await self.db_queue.execute(db_operation)

    def __del__(self):
        """
        Regular (non-async) destructor that ensures resources are cleaned up.
        Note: We should avoid complex cleanup logic in __del__
        """
        if hasattr(self, 'db_manager'):
            bt.logging.info("Object being destroyed, cleanup should have been called explicitly")

    async def cleanup(self):
        """Cleanup validator resources"""
        bt.logging.info("Cleaning up validator resources...")
        try:
            # Cancel any pending tasks first
            if hasattr(self, 'watchdog'):
                self.watchdog.cleanup()
                
            # Wait briefly for tasks to cancel
            await asyncio.sleep(1)
            
            # Then cleanup database
            if hasattr(self, 'db_manager'):
                await self.db_manager.cleanup()
                
            # Finally shutdown executor
            if hasattr(self, 'executor'):
                self.executor.shutdown(wait=True)
                
        except Exception as e:
            bt.logging.error(f"Error during cleanup: {str(e)}")