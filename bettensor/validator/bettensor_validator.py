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
from typing import Dict, Tuple, Optional
from datetime import datetime, timedelta, timezone
from bettensor.base.neuron import BaseNeuron
from bettensor.protocol import TeamGamePrediction
from bettensor.validator.io.website_handler import WebsiteHandler
from bettensor.validator.scoring.scoring import ScoringSystem
from bettensor.validator.scoring.entropy_system import EntropySystem
from bettensor.validator.io.sports_data import SportsData
from bettensor.validator.scoring.weights_functions import WeightSetter
from bettensor.validator.database.database_manager import DatabaseManager
from bettensor.validator.io.miner_data import MinerDataMixin
from bettensor.validator.io.bettensor_api_client import BettensorAPIClient
from bettensor.validator.io.base_api_client import BaseAPIClient
from bettensor.validator.scoring.watchdog import Watchdog
from bettensor.validator.vesting.system import VestingSystem
from bettensor.validator.vesting.integration import VestingIntegration, get_vesting_system, handle_deregistered_miner, batch_handle_deregistrations
from bettensor import __spec_version__
from types import SimpleNamespace
import configparser
from bettensor.validator.vesting.database_schema import create_vesting_tables_async

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
        
        # Vesting system parameters
        parser.add_argument(
            "--vesting.enabled",
            type=str,
            default="False",
            help="Enable vesting impact on weights (True/False)"
        )
        parser.add_argument(
            "--vesting.minimum_stake",
            type=float,
            default=0.3,
            help="Minimum stake required for multiplier"
        )
        parser.add_argument(
            "--vesting.retention_window",
            type=int,
            default=30,
            help="Retention window in days"
        )
        parser.add_argument(
            "--vesting.retention_target",
            type=float,
            default=0.9,
            help="Target retention percentage"
        )
        parser.add_argument(
            "--vesting.max_multiplier",
            type=float,
            default=1.5,
            help="Maximum vesting multiplier"
        )
        parser.add_argument(
            "--vesting.use_background_thread",
            type=str,
            default="True",
            help="Whether to use background threads for vesting system (True/False)"
        )
        parser.add_argument(
            "--vesting.thread_priority",
            type=str,
            default="low",
            choices=["low", "normal", "high"],
            help="Priority for vesting system background threads"
        )
        parser.add_argument(
            "--vesting.query_interval_seconds",
            type=int,
            default=300,
            help="How often to check for blockchain updates (seconds)"
        )
        parser.add_argument(
            "--vesting.transaction_query_interval_seconds",
            type=int,
            default=60,
            help="How often to check for new transactions (seconds)"
        )
        parser.add_argument(
            "--vesting.detailed_transaction_tracking",
            type=str,
            default="True",
            help="Whether to use detailed transaction tracking (True/False)"
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
        
        # Vesting system attributes
        self.vesting_system = None
        self.vesting_integration = None

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
    ) -> Tuple["bt.wallet", "bt.subtensor", "bt.dendrite", "bt.metagraph"]: # type: ignore
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
        """Initializes the neuron by setting up the axon and synapse network components
        Sets up the database, loads state, sets up the axon and synapse, and sets up the API client
        Returns:
            bool: True if initialization was successful, False otherwise
        """
        try:
            bt.logging.info("Initializing validator neuron")
            
            # Check mandatory config parameters
            for param in ['netuid', 'subtensor.network', 'wallet.name', 'wallet.hotkey']:
                if not hasattr(self.config, param):
                    bt.logging.error(f"Missing required config parameter: {param}")
                    return False
            
            # Set up bittensor objects (wallet, subtensor, dendrite, metagraph)
            try:
                # Setup bittensor objects (wallet, subtensor, dendrite, metagraph)
                self.wallet, self.subtensor, self.dendrite, self.metagraph = self.setup_bittensor_objects(self.config)
                if not self.wallet or not self.subtensor or not self.dendrite or not self.metagraph:
                    raise ValueError("Failed to set up bittensor objects")
            except Exception as e:
                bt.logging.error(f"Error in bittensor setup: {e}")
                bt.logging.error(traceback.format_exc())
                return False
                
            # Set up database connection and validators manager
            try:
                # Get DB path and ensure directory exists
                os.makedirs(os.path.dirname(self.config.db), exist_ok=True)
                
                # Create main database manager
                self.db_manager = DatabaseManager(self.config.db)
                
                # Create separate vesting database manager with a different file
                vesting_db_dir = os.path.dirname(self.config.db)
                self.vesting_db_path = os.path.join(vesting_db_dir, "vesting.db")
                bt.logging.info(f"Creating separate vesting database at {self.vesting_db_path}")
                self.vesting_db_manager = DatabaseManager(self.vesting_db_path)
                
                # Log database paths for debugging
                bt.logging.info(f"Main database path: {self.db_manager.database_path}")
                bt.logging.info(f"Vesting database path: {self.vesting_db_manager.database_path}")
                
                # Check if vesting.db file exists
                if os.path.exists(self.vesting_db_path):
                    bt.logging.info(f"Vesting database file found at {self.vesting_db_path}")
                else:
                    bt.logging.warning(f"Vesting database file not found at {self.vesting_db_path}")
                
            except Exception as e:
                bt.logging.error(f"Error in database setup: {e}")
                bt.logging.error(traceback.format_exc())
                return False

            # Setup api client
            if self.config.get("bettensor.api_client.type", "default") == "default":
                self.api_client = BettensorAPIClient(db_manager=self.db_manager)
            else:
                # Base API client for testing
                self.api_client = BaseAPIClient()
            
            # Set up entropy system first
            self.entropy_system = EntropySystem(
                num_miners=256,
                max_days=45,
                db_manager=self.db_manager
            )

            # Initialize sports data handler with correct parameters
            self.sports_data = SportsData(
                db_manager=self.db_manager,
                entropy_system=self.entropy_system,
                api_client=self.api_client
            )

            # Set up website handler
            self.website_handler = WebsiteHandler(validator=self)

            # Get step from loaded state or set to 0 if not found
            state_path = self.base_path + "state/state.pt"
            if os.path.exists(state_path):
                try:
                    state_dict = torch.load(state_path)
                    self.step = state_dict.get("step", 0)
                    bt.logging.info(f"Loaded step {self.step} from saved state")
                except:
                    bt.logging.info("Failed to load state, defaulting to step 0")
                    self.step = 0
                    
            else:
                bt.logging.info("No saved state found, defaulting to step 0")
                self.step = 0

            # Validate the subtensor connection
            if self.subtensor is None:
                bt.logging.error("Failed to connect to subtensor")
                return False
                
            # Print chain endpoint for debugging
            self.print_chain_endpoint()

            # Check validator registration and set up axon/dendrite
            try:
                # Check validator registration
                if not self.check_vali_reg(self.metagraph, self.wallet, self.subtensor):
                    bt.logging.error("Validator is not registered or improperly set up")
                    return False

                # Load state AFTER checking registration
                await self.load_state()

                # Setup axon
                self.serve_axon()

            except Exception as e:
                bt.logging.error(f"Error in subtensor setup: {e}")
                bt.logging.error(traceback.format_exc())
                return False

            # Initialize MinerDataMixin with metagraph
            try:
                if hasattr(self.metagraph, 'uids'):
                    bt.logging.info(f"Initializing MinerDataMixin with {len(self.metagraph.uids)} miners")
                    MinerDataMixin.__init__(self, self.db_manager, self.metagraph, set(self.metagraph.uids))
                else:
                    bt.logging.warning("Metagraph uids not available for MinerDataMixin initialization")
            except Exception as e:
                bt.logging.error(f"Error initializing MinerDataMixin: {e}")
                bt.logging.error(traceback.format_exc())

            # Set up scoring system with correct parameters
            self.scoring_system = ScoringSystem(
                db_manager=self.db_manager,
                num_miners=256,
                max_days=45,
                reference_date=datetime.now(timezone.utc),
                validator=self
            )
            
            # Set up the weight setting functions
            self.weight_setter = WeightSetter(
                metagraph=self.metagraph,
                wallet=self.wallet,
                subtensor=self.subtensor,
                neuron_config=self.config,
                db_path=self.config.db,
            )

            # Initialize vesting system and integration
            try:
                bt.logging.info("Initializing vesting system...")
                
                # Try to read vesting config from setup.cfg
                config_parser = configparser.ConfigParser()
                try:
                    config_parser.read('setup.cfg')
                    vesting_cfg = config_parser['vesting'] if 'vesting' in config_parser else {}
                except Exception as e:
                    bt.logging.debug(f"Could not read vesting config from setup.cfg: {e}")
                    vesting_cfg = {}
                
                # Check DB state to see if it was recently redownloaded
                # Initialize need_metagraph_init with a default value
                need_metagraph_init = True
                
                try:
                    # First, check if the db_version table exists at all
                    table_exists = await self.db_manager.fetch_one(
                        "SELECT name FROM sqlite_master WHERE type='table' AND name='db_version'"
                    )
                    
                    if table_exists:
                        # Check the columns in the db_version table
                        columns = await self.db_manager.fetch_all(
                            "PRAGMA table_info(db_version)"
                        )
                        
                        # Find the column that represents time (might be 'timestamp', 'created_at', etc.)
                        time_column = None
                        for column in columns:
                            col_name = column.get('name', '').lower()
                            if 'time' in col_name or 'date' in col_name:
                                time_column = column.get('name')
                                break
                        
                        if time_column:
                            bt.logging.info(f"Found time column in db_version: {time_column}")
                            
                            # Get the last update using the found column
                            last_update = await self.db_manager.fetch_one(
                                f"SELECT {time_column} FROM db_version ORDER BY {time_column} DESC LIMIT 1"
                            )
                            
                            if last_update and time_column in last_update:
                                now = datetime.now(timezone.utc)
                                try:
                                    # Try to parse the date in different formats
                                    time_value = last_update[time_column]
                                    if isinstance(time_value, str):
                                        if 'Z' in time_value:
                                            last_update_time = datetime.fromisoformat(time_value.replace("Z", "+00:00"))
                                        else:
                                            last_update_time = datetime.fromisoformat(time_value)
                                    else:
                                        # Might be a datetime object already
                                        last_update_time = time_value
                                    
                                    time_since_update = (now - last_update_time).total_seconds() / 60
                                    bt.logging.info(f"Database was last updated {time_since_update:.1f} minutes ago")
                                    
                                    if time_since_update < 10:  # If it was updated in the last 10 minutes
                                        bt.logging.info("Database was recently redownloaded, need to reinitialize vesting integration")
                                        need_metagraph_init = True
                                    else:
                                        bt.logging.info("Database has not been recently redownloaded")
                                        need_metagraph_init = False
                                except Exception as e:
                                    bt.logging.warning(f"Error parsing date from db_version: {e}")
                                    # Fall back to default
                                    need_metagraph_init = True
                            else:
                                bt.logging.warning(f"No values found in db_version.{time_column}")
                        else:
                            bt.logging.warning("No time/date column found in db_version table")
                    else:
                        bt.logging.warning("db_version table not found")
                except Exception as e:
                    bt.logging.warning(f"Error checking db_version table: {e}")
                    bt.logging.debug(traceback.format_exc())
                    
                bt.logging.info(f"Setting need_metagraph_init = {need_metagraph_init}")
                
                # Get vesting configuration parameters (command line takes precedence over setup.cfg)
                # Core parameters
                vesting_enabled_str = getattr(self.config, 'vesting.enabled', vesting_cfg.get('enabled', 'False'))
                vesting_enabled = vesting_enabled_str.lower() == 'true' if isinstance(vesting_enabled_str, str) else bool(vesting_enabled_str)
                
                min_stake = getattr(self.config, 'vesting.minimum_stake', 
                                   float(vesting_cfg.get('minimum_stake', 0.3)))
                
                retention_window = getattr(self.config, 'vesting.retention_window', 
                                          int(vesting_cfg.get('retention_window', 30)))
                
                retention_target = getattr(self.config, 'vesting.retention_target', 
                                          float(vesting_cfg.get('retention_target', 0.9)))
                
                max_multiplier = getattr(self.config, 'vesting.max_multiplier', 
                                        float(vesting_cfg.get('max_multiplier', 1.5)))
                
                # Threading parameters
                use_bg_thread_str = getattr(self.config, 'vesting.use_background_thread', 
                                           vesting_cfg.get('use_background_thread', 'True'))
                use_bg_thread = use_bg_thread_str.lower() == 'true' if isinstance(use_bg_thread_str, str) else bool(use_bg_thread_str)
                
                thread_priority = getattr(self.config, 'vesting.thread_priority', 
                                         vesting_cfg.get('thread_priority', 'low'))
                
                query_interval = getattr(self.config, 'vesting.query_interval_seconds', 
                                        int(vesting_cfg.get('query_interval_seconds', 300)))
                
                tx_query_interval = getattr(self.config, 'vesting.transaction_query_interval_seconds', 
                                           int(vesting_cfg.get('transaction_query_interval_seconds', 60)))
                
                detailed_tx_str = getattr(self.config, 'vesting.detailed_transaction_tracking', 
                                         vesting_cfg.get('detailed_transaction_tracking', 'True'))
                detailed_tx = detailed_tx_str.lower() == 'true' if isinstance(detailed_tx_str, str) else bool(detailed_tx_str)
                
                # Create and initialize the vesting system
                try:
                    self.vesting_system = get_vesting_system(
                        subtensor=self.subtensor,
                        subnet_id=self.config.netuid,
                        db_manager=self.vesting_db_manager  # Use dedicated vesting database manager
                    )
                    
                    # Initialize the vesting system with better error handling
                    try:
                        bt.logging.info("Starting vesting system initialization...")
                        success = await self.vesting_system.initialize(
                            auto_init_metagraph=False,  # We'll handle metagraph init separately
                            minimum_stake=min_stake,
                            retention_window_days=retention_window,
                            retention_target=retention_target,
                            max_multiplier=max_multiplier,
                            use_background_thread=use_bg_thread,
                            thread_priority=thread_priority,
                            query_interval_seconds=query_interval,
                            transaction_query_interval_seconds=tx_query_interval,
                            detailed_transaction_tracking=detailed_tx
                        )
                        
                        if not success:
                            bt.logging.error("Vesting system initialization returned False")
                            raise Exception("Vesting system initialization returned False")
                        
                        bt.logging.info("Vesting system initialized successfully")
                        
                        # ADDED: Force detailed transaction tracking on for debugging
                        self.vesting_system.detailed_transaction_tracking = True
                        bt.logging.info("Forcing detailed transaction tracking ON for debugging")
                        
                        # ADDED: Check if transaction monitoring is active and running
                        if hasattr(self.vesting_system, 'transaction_monitor') and self.vesting_system.transaction_monitor:
                            if hasattr(self.vesting_system.transaction_monitor, 'is_running'):
                                is_running = self.vesting_system.transaction_monitor.is_running
                                bt.logging.info(f"Transaction monitoring status: {'RUNNING' if is_running else 'STOPPED'}")
                                if not is_running:
                                    bt.logging.warning("Transaction monitoring is not running! Attempting to start manually...")
                                    success = self.vesting_system.start_background_thread('normal')
                                    if success:
                                        bt.logging.info("Successfully started background thread manually")
                                        # Check again after 2 seconds
                                        await asyncio.sleep(2)
                                        is_running = self.vesting_system.transaction_monitor.is_running
                                        bt.logging.info(f"Transaction monitoring status after manual start: {'RUNNING' if is_running else 'STILL STOPPED'}")
                                    else:
                                        bt.logging.error("Failed to start background thread manually")
                            else:
                                bt.logging.warning("Transaction monitor doesn't have is_running attribute - implementation has changed")
                        else:
                            bt.logging.error("Transaction monitor not initialized despite detailed_transaction_tracking=True")
                            # Try to create and initialize it manually
                            bt.logging.info("Attempting to create transaction monitor manually...")
                            try:
                                from bettensor.validator.vesting.transaction_monitor import TransactionMonitor
                                self.vesting_system.transaction_monitor = TransactionMonitor(
                                    subtensor=self.subtensor,
                                    subnet_id=self.config.netuid,
                                    db_manager=self.vesting_db_manager,
                                    verbose=True
                                )
                                await self.vesting_system.transaction_monitor.initialize()
                                self.vesting_system.start_background_thread('normal')
                                bt.logging.info("Manually created and started transaction monitor")
                            except Exception as e:
                                bt.logging.error(f"Failed to manually create transaction monitor: {e}")
                        
                        # If we need metagraph initialization (e.g., after db redownload), do it now
                        if need_metagraph_init:
                            bt.logging.info("Initializing vesting data from metagraph (with force refresh)...")
                            metagraph_init_success = await self.vesting_system.initialize_from_metagraph(force_refresh=True)
                            if metagraph_init_success:
                                bt.logging.info("Successfully initialized vesting data from metagraph with force refresh")
                            else:
                                bt.logging.warning("Failed to initialize from metagraph, vesting system may not have stake data")
                        
                        # Check vesting status after initialization 
                        await self.check_vesting_status()
                        
                        # Explicitly update coldkey-hotkey relationships to ensure coldkey_metrics is populated
                        bt.logging.info("Explicitly updating coldkey-hotkey relationships...")
                        success = await self.vesting_system.update_coldkey_hotkey_relationships()
                        if success:
                            bt.logging.info("Successfully updated coldkey-hotkey relationships")
                        else:
                            bt.logging.warning("Failed to update coldkey-hotkey relationships")
                            
                        # Check the transaction monitoring status
                        try:
                            await self.check_transaction_monitoring_status()
                        except Exception as e:
                            bt.logging.error(f"Error checking transaction monitoring status: {e}")
                            bt.logging.debug(traceback.format_exc())
                        
                        # Set up a periodic check to ensure transaction monitoring stays running
                        async def periodic_tx_monitoring_check():
                            try:
                                while True:
                                    await asyncio.sleep(300)  # Check every 5 minutes
                                    bt.logging.info("Running periodic transaction monitoring status check")
                                    try:
                                        await self.check_transaction_monitoring_status()
                                    except Exception as e:
                                        bt.logging.error(f"Error in periodic transaction monitoring check: {e}")
                            except asyncio.CancelledError:
                                bt.logging.info("Periodic transaction monitoring check task cancelled")
                            except Exception as e:
                                bt.logging.error(f"Unexpected error in periodic monitoring task: {e}")
                                bt.logging.debug(traceback.format_exc())
                        
                        # Start the periodic check in a background task
                        try:
                            self.tx_monitoring_check_task = asyncio.create_task(periodic_tx_monitoring_check())
                            bt.logging.info("Started periodic transaction monitoring status check task")
                        except Exception as e:
                            bt.logging.error(f"Failed to start periodic transaction monitoring check: {e}")
                    except Exception as e:
                        bt.logging.error(f"Error during vesting system initialization: {e}")
                        bt.logging.debug(traceback.format_exc())
                        raise
                    
                    # Create and install the integration with the scoring system
                    self.vesting_integration = VestingIntegration(
                        scoring_system=self.scoring_system,
                        vesting_system=self.vesting_system
                    )
                    
                    # Install the integration
                    enabled_status = "enabled" if vesting_enabled else "shadow mode (tracking but not impacting weights)"
                    bt.logging.info(f"Installing vesting integration in {enabled_status}")
                    self.vesting_integration.install(impact_weights=vesting_enabled)
                    
                    bt.logging.info("Vesting system initialized and integrated successfully")
                except Exception as e:
                    bt.logging.error(f"Failed to initialize vesting system: {e}")
                    bt.logging.debug(traceback.format_exc())
                    self.vesting_system = None
                    self.vesting_integration = None
            except Exception as e:
                bt.logging.error(f"Failed to initialize vesting system: {e}")
                bt.logging.debug(traceback.format_exc())
                self.vesting_system = None
                self.vesting_integration = None

            self.is_initialized = True
            return True
            
        except Exception as e:
            bt.logging.error(f"Exception during initialization: {e}")
            bt.logging.error(traceback.format_exc())
            return False

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
                    deregistered_hotkeys = []
                    
                    for i, hotkey in enumerate(current_hotkeys):
                        if self.hotkeys[i] != hotkey:
                            bt.logging.debug(
                                f"Index '{i}' has mismatching hotkey. Old hotkey: '{self.hotkeys[i]}', New hotkey: '{hotkey}'. Resetting score to 0.0"
                            )
                            # Track deregistered hotkey
                            if self.hotkeys[i] not in current_hotkeys:
                                deregistered_hotkeys.append(self.hotkeys[i])
                                
                            self.scores[i] = 0.0
                            await self.scoring_system.reset_miner(i)
                            await self.save_state()
                    
                    # Handle deregistered hotkeys in vesting system
                    if deregistered_hotkeys and self.vesting_system:
                        bt.logging.info(f"Handling {len(deregistered_hotkeys)} deregistered hotkeys in vesting system")
                        try:
                            await batch_handle_deregistrations(
                                vesting_system=self.vesting_system, 
                                deregistered_hotkeys=deregistered_hotkeys
                            )
                        except Exception as e:
                            bt.logging.error(f"Error handling deregistered hotkeys in vesting system: {e}")
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
        if hasattr(self, 'max_targets') and self.max_targets is not None and self.max_targets < 256:
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
        else:
            # If max_targets is None, use all available uids
            bt.logging.debug(f"Using all {len(uids_to_query)} available uids to query (max_targets not set)")
            # Ensure target_group is reset for next time
            self.target_group = 0

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
                        
            # Delete state files (./bettensor/validator/state/state.pt, ./bettensor/validator/state/validator.db, ./bettensor/validator/state/vesting.db, ./bettensor/validator/state/entropy_system_state.json)
            for file in ["./bettensor/validator/state/state.pt", 
                         "./bettensor/validator/state/validator.db", 
                         "./bettensor/validator/state/vesting.db",  # Add vesting.db to the list
                         "./bettensor/validator/state/entropy_system_state.json"]:
                if os.path.exists(file):
                    os.remove(file)
                    bt.logging.info(f"Deleted {file}")
            
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
                bt.logging.info("Cleaning up main database connections...")
                try:
                    await self.db_manager.cleanup()
                except Exception as e:
                    bt.logging.error(f"Error cleaning up main database: {e}")
                    
            # Cleanup vesting database connections
            if hasattr(self, 'vesting_db_manager'):
                bt.logging.info("Cleaning up vesting database connections...")
                try:
                    await self.vesting_db_manager.cleanup()
                except Exception as e:
                    bt.logging.error(f"Error cleaning up vesting database: {e}")
                
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
        if hasattr(self, 'db_manager') or hasattr(self, 'vesting_db_manager'):
            bt.logging.info("Object being destroyed, cleanup should have been called explicitly")

    async def cleanup(self):
        """
        Cleanup resources used by the validator.
        """
        bt.logging.info("Running cleanup...")
        
        # Cancel the transaction monitoring check task if it exists
        if hasattr(self, 'tx_monitoring_check_task') and self.tx_monitoring_check_task:
            bt.logging.info("Cancelling transaction monitoring check task")
            self.tx_monitoring_check_task.cancel()
            try:
                await self.tx_monitoring_check_task
            except asyncio.CancelledError:
                pass
            
        # Cleanup vesting system
        if hasattr(self, 'vesting_system') and self.vesting_system:
            bt.logging.info("Shutting down vesting system...")
            try:
                await self.vesting_system.shutdown()
                bt.logging.info("Vesting system shut down successfully")
            except Exception as e:
                bt.logging.error(f"Error shutting down vesting system: {e}")
        
        # Close database connections
        if hasattr(self, 'db_manager') and self.db_manager:
            bt.logging.info("Closing database connections...")
            try:
                await self.db_manager.close()
                bt.logging.info("Database connections closed")
            except Exception as e:
                bt.logging.error(f"Error closing database connections: {e}")
                
        # Close vesting database connections
        if hasattr(self, 'vesting_db_manager') and self.vesting_db_manager:
            bt.logging.info("Closing vesting database connections...")
            try:
                await self.vesting_db_manager.close()
                bt.logging.info("Vesting database connections closed")
            except Exception as e:
                bt.logging.error(f"Error closing vesting database connections: {e}")
                
        bt.logging.info("Cleanup complete")

    async def check_vesting_status(self):
        """
        Check and print the status of the vesting system.
        
        This method is useful for diagnostics and debugging to verify
        that the vesting system is properly initialized and functioning.
        """
        if not self.vesting_system:
            bt.logging.error("Vesting system is not initialized")
            return False
            
        try:
            # Get diagnostic information
            bt.logging.info("Checking vesting system status...")
            info = await self.vesting_system.get_diagnostic_info()
            
            # Print system status
            status = info['system_status']
            status_emoji = "✅" if status == 'operational' else "❌"
            
            bt.logging.info(f"Vesting System Status: {status_emoji} {status.upper()}")
            bt.logging.info(f"Metagraph initialization: {'✅ COMPLETE' if info['metagraph_initialized'] else '❌ NOT INITIALIZED'}")
            
            # Print table information
            bt.logging.info("\nTable Statistics:")
            for table, stats in info['tables'].items():
                if 'total_stake' in stats:
                    bt.logging.info(f"  {table}: {stats['count']} records, {stats['total_stake']} total stake")
                else:
                    bt.logging.info(f"  {table}: {stats['count']} records")
            
            # Print errors if any
            if info['errors']:
                bt.logging.error("\nErrors detected:")
                for i, error in enumerate(info['errors']):
                    bt.logging.error(f"  {i+1}. {error}")
                    
            # Provide further diagnosis for common issues
            if status != 'operational':
                if status == 'no_data':
                    bt.logging.warning("\nDiagnosis: Vesting system tables exist but contain no data")
                    bt.logging.warning("Possible solutions:")
                    bt.logging.warning("  1. Force reinitialization from metagraph")
                    bt.logging.warning("  2. Check for errors during initialization")
                    
                    # Check specifically for coldkey_metrics
                    if 'coldkey_metrics' in info['tables'] and info['tables']['coldkey_metrics']['count'] == 0:
                        bt.logging.warning("\nDetected missing coldkey-hotkey relationships")
                        bt.logging.warning("Attempting to update coldkey-hotkey relationships...")
                        
                        try:
                            success = await self.vesting_system.update_coldkey_hotkey_relationships()
                            if success:
                                bt.logging.info("✅ Successfully updated coldkey-hotkey relationships")
                            else:
                                bt.logging.warning("❌ Failed to update coldkey-hotkey relationships")
                        except Exception as e:
                            bt.logging.error(f"Error updating coldkey-hotkey relationships: {e}")
                elif status == 'not_initialized':
                    bt.logging.warning("\nDiagnosis: Vesting system needs initialization from metagraph")
                    bt.logging.warning("Possible solutions:")
                    bt.logging.warning("  1. Force reinitialization from metagraph")
                    bt.logging.warning("  2. Ensure blockchain_state table exists")
                
                # Offer to reinitialize from metagraph
                bt.logging.info("\nAttempting to reinitialize from metagraph with force refresh...")
                success = await self.vesting_system.initialize_from_metagraph(force_refresh=True)
                if success:
                    bt.logging.info("✅ Successfully reinitialized from metagraph with force refresh")
                    
                    # Check status again
                    info = await self.vesting_system.get_diagnostic_info()
                    for table, stats in info['tables'].items():
                        if 'total_stake' in stats and stats['total_stake'] > 0:
                            bt.logging.info(f"  {table}: {stats['count']} records, {stats['total_stake']} total stake")
                else:
                    bt.logging.error("❌ Failed to reinitialize from metagraph")
            
            return status == 'operational'
            
        except Exception as e:
            bt.logging.error(f"Error checking vesting status: {e}")
            bt.logging.error(traceback.format_exc())
            return False

    async def check_transaction_monitoring_status(self):
        """
        Check and log the status of transaction monitoring, attempting to restart if not running.
        
        Returns:
            bool: True if transaction monitoring is running, False otherwise
        """
        if not hasattr(self, 'vesting_system') or not self.vesting_system:
            bt.logging.error("Vesting system not initialized, cannot check transaction monitoring status")
            return False
            
        bt.logging.info("Checking transaction monitoring status...")
        
        if not hasattr(self.vesting_system, 'detailed_transaction_tracking'):
            bt.logging.error("Vesting system doesn't have detailed_transaction_tracking attribute")
            return False
            
        bt.logging.info(f"Detailed transaction tracking setting: {self.vesting_system.detailed_transaction_tracking}")
        
        if not self.vesting_system.detailed_transaction_tracking:
            bt.logging.warning("Detailed transaction tracking is disabled, enabling it now for debugging")
            self.vesting_system.detailed_transaction_tracking = True
            
        if not hasattr(self.vesting_system, 'transaction_monitor') or not self.vesting_system.transaction_monitor:
            bt.logging.error("Transaction monitor not initialized despite detailed_transaction_tracking=True")
            
            # Try to create and initialize it manually
            bt.logging.info("Attempting to create transaction monitor manually...")
            try:
                from bettensor.validator.vesting.transaction_monitor import TransactionMonitor
                self.vesting_system.transaction_monitor = TransactionMonitor(
                    subtensor=self.subtensor,
                    subnet_id=self.config.netuid,
                    db_manager=self.vesting_db_manager,
                    verbose=True
                )
                await self.vesting_system.transaction_monitor.initialize()
                self.vesting_system.start_background_thread('normal')
                bt.logging.info("Manually created and started transaction monitor")
            except Exception as e:
                bt.logging.error(f"Failed to manually create transaction monitor: {e}")
                return False
        
        # Check if transaction monitoring is running
        if not hasattr(self.vesting_system.transaction_monitor, 'is_running'):
            bt.logging.warning("Transaction monitor doesn't have is_running property")
            
            # Try accessing 'is_monitoring' attribute instead
            if hasattr(self.vesting_system.transaction_monitor, 'is_monitoring'):
                is_running = self.vesting_system.transaction_monitor.is_monitoring
            else:
                bt.logging.error("Cannot determine transaction monitoring status")
                return False
        else:
            is_running = self.vesting_system.transaction_monitor.is_running
        
        bt.logging.info(f"Transaction monitoring status: {'RUNNING' if is_running else 'STOPPED'}")
        
        if not is_running:
            bt.logging.warning("Transaction monitoring is not running! Attempting to start manually...")
            success = self.vesting_system.start_background_thread('normal')
            
            if success:
                bt.logging.info("Successfully started background thread manually")
                # Check again after 2 seconds
                await asyncio.sleep(2)
                
                if hasattr(self.vesting_system.transaction_monitor, 'is_running'):
                    is_running = self.vesting_system.transaction_monitor.is_running
                elif hasattr(self.vesting_system.transaction_monitor, 'is_monitoring'):
                    is_running = self.vesting_system.transaction_monitor.is_monitoring
                    
                bt.logging.info(f"Transaction monitoring status after manual start: {'RUNNING' if is_running else 'STILL STOPPED'}")
            else:
                bt.logging.error("Failed to start background thread manually")
        
        return is_running