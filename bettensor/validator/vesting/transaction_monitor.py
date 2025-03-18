"""
Transaction monitoring module for the vesting system.

This module provides detailed tracking of stake-related transactions on the
Bittensor blockchain, including detecting, matching, and validating transactions
with their corresponding blockchain events.
"""

import asyncio
import logging
import time
import json
import threading
from datetime import datetime, timezone
import traceback
from typing import Dict, List, Optional, Any, Set, Union, Tuple

import bittensor as bt
from substrateinterface import SubstrateInterface
from substrateinterface.exceptions import SubstrateRequestException

from bettensor.validator.database.database_manager import DatabaseManager

# Transaction flow categories
INFLOW = "inflow"         # Stake added to the hotkey
OUTFLOW = "outflow"       # Stake removed from the hotkey
NEUTRAL = "neutral"       # No net change to the hotkey's stake (e.g., transfer between hotkeys in same subnet)
EMISSION = "emission"     # Stake added via network emissions/rewards

# Transaction types
ADD_STAKE = "add_stake"           # Direct stake addition
REMOVE_STAKE = "remove_stake"     # Direct stake removal
TRANSFER_STAKE = "transfer_stake" # Transfer between hotkeys (can be neutral/inflow/outflow)
MOVE_STAKE = "move_stake"         # Move stake between subnets (inflow/outflow)
SWAP_STAKE = "swap_stake"         # Swap stake between subnets (inflow/outflow, same coldkey/hotkey)
BURN_STAKE = "burn_stake"         # Stake burning (outflow)
EMISSION_REWARD = "emission"      # Emission rewards (inflow)
UNKNOWN = "unknown"

class TransactionMonitor:
    """
    Monitors blockchain for stake-related transactions.
    
    This class provides detailed tracking of stake transactions on the Bittensor blockchain,
    including detecting and matching extrinsics (transaction requests) with their 
    corresponding blockchain events for accurate transaction validation.
    """
    
    def __init__(
        self,
        subtensor: 'bt.subtensor',
        subnet_id: int,
        db_manager: 'DatabaseManager',
        verbose: bool = False,
        query_interval_seconds: int = 60,  # Kept for backward compatibility but unused
        explicit_endpoint: str = None  # Optional explicit endpoint URL
    ):
        """
        Initialize the transaction monitor.
        
        Args:
            subtensor: The subtensor client
            subnet_id: The subnet ID to monitor
            db_manager: Database manager for storage
            verbose: Whether to log verbose details
            query_interval_seconds: Deprecated parameter, kept for backward compatibility.
                                   The monitor watches every block in real-time via subscription.
            explicit_endpoint: Optional explicit endpoint URL for the substrate interface.
                             If not provided, defaults to mainnet endpoint.
        """
        # Initialize subtensor and other parameters
        self.subtensor = subtensor
        self.subnet_id = subnet_id
        self.db_manager = db_manager
        self.verbose = verbose
        self.explicit_endpoint = explicit_endpoint
        
        # Log initialization with explicit configuration
        bt.logging.info(f"TransactionMonitor init - subnet_id: {subnet_id}, verbose: {verbose}, explicit_endpoint: {explicit_endpoint}")
        if hasattr(subtensor, 'chain_endpoint'):
            bt.logging.info(f"Subtensor chain_endpoint: {subtensor.chain_endpoint}")
        else:
            bt.logging.warning("Subtensor has no chain_endpoint attribute")
        
        # Monitoring thread
        self.monitoring_thread = None
        self.stop_event = threading.Event()
        self.substrate = None
        self._last_processed_block = None
        self._last_call_block = 0
        self._pending_calls = {}  # Block -> list of calls
        self._is_initialized = False
        self._thread_lock = threading.RLock()
        self._substrate_lock = threading.RLock()
        
        # Status tracking
        self._status = "STOPPED"
        self._last_error = None
        self._last_successful_block = None
        self._thread_startup_time = None
    
    async def initialize(self):
        """Initialize the transaction monitor."""
        try:
            bt.logging.info("Initializing TransactionMonitor")
            
            # Ensure tables exist
            await self._ensure_tables_exist()
            
            # Try to load last processed block
            self._last_processed_block = await self._load_last_processed_block()
            
            # Force Finney mainnet endpoint if monitoring subnet 30 and on test network
            force_finney_endpoint = None
            if self.subnet_id == 30 and hasattr(self.subtensor, 'chain_endpoint'):
                if 'test' in self.subtensor.chain_endpoint:
                    force_finney_endpoint = "wss://entrypoint-finney.opentensor.ai:443"
                    bt.logging.info(f"⚠️ Detected test network but monitoring subnet 30, forcing Finney mainnet endpoint: {force_finney_endpoint}")

            # Create a dedicated substrate connection for transaction monitoring
            if force_finney_endpoint:
                # Override with Finney mainnet endpoint
                bt.logging.info(f"🔄 Using forced Finney mainnet endpoint: {force_finney_endpoint}")
                self.explicit_endpoint = force_finney_endpoint  # Override the explicit endpoint
                self.substrate = SubstrateInterface(url=force_finney_endpoint)
            elif self.explicit_endpoint:
                bt.logging.info(f"Using explicit endpoint: {self.explicit_endpoint}")
                self.substrate = SubstrateInterface(url=self.explicit_endpoint)
            else:
                # Try to get an endpoint from subtensor
                if hasattr(self.subtensor, 'chain_endpoint') and self.subtensor.chain_endpoint:
                    endpoint = self.subtensor.chain_endpoint
                    bt.logging.info(f"Using subtensor chain_endpoint: {endpoint}")
                    self.substrate = SubstrateInterface(url=endpoint)
                else:
                    try:
                        if hasattr(self.subtensor, 'substrate') and self.subtensor.substrate:
                            bt.logging.info("Using subtensor.substrate's endpoint")
                            substrate_url = self.subtensor.substrate.url
                            bt.logging.info(f"Extracted URL from subtensor.substrate: {substrate_url}")
                            self.substrate = SubstrateInterface(url=substrate_url)
                        else:
                            raise ValueError("No substrate endpoint available from subtensor")
                    except Exception as e:
                        bt.logging.error(f"Error extracting URL from subtensor.substrate: {e}")
                        raise ValueError(f"Failed to determine chain endpoint: {e}")
            
            # Test the connection
            try:
                # Get chain properties instead of using get_chain_info()
                chain_head = self.substrate.get_chain_head()
                
                # Try to get chain name - handle potential exceptions safely
                try:
                    # Different approach to get chain info
                    chain_props = "Unknown"
                    if hasattr(self.substrate, 'get_metadata'):
                        metadata = self.substrate.get_metadata(block_hash=chain_head)
                        if metadata and hasattr(metadata, 'metadata') and hasattr(metadata.metadata, 'name'):
                            chain_props = metadata.metadata.name
                        elif metadata and hasattr(metadata, 'metadata_version'):
                            chain_props = f"Substrate (Metadata v{metadata.metadata_version})"
                    bt.logging.info(f"Connected to blockchain: {chain_props}")
                except Exception as e:
                    bt.logging.warning(f"Could not get chain name: {e}, but connection established")
                
                # Log current block information
                try:
                    current_block = self.substrate.get_block_number(chain_head)
                    bt.logging.info(f"Current chain head: Block #{current_block}")
                except Exception as e:
                    bt.logging.warning(f"Could not get block number: {e}, but connection established")
                
                # Connection successful if we get here
                bt.logging.info("Blockchain connection established successfully")
            except Exception as e:
                bt.logging.error(f"Failed to connect to blockchain: {e}")
                # Handle error but don't raise - we want initialization to succeed even if blockchain connection fails
                bt.logging.warning("Will continue without blockchain connection and retry later")
            
            self._is_initialized = True
            bt.logging.info("TransactionMonitor initialized successfully")
            return True
        except Exception as e:
            bt.logging.error(f"Failed to initialize TransactionMonitor: {e}")
            import traceback
            bt.logging.error(traceback.format_exc())
            self._is_initialized = False
            self._last_error = str(e)
            return False
    
    # DEPRECATED: Kept for backward compatibility
    async def _ensure_tables_exist(self):
        """
        DEPRECATED: Table creation is now handled centrally by DatabaseManager.
        
        This method is maintained for backward compatibility only and does nothing.
        Table schemas are defined in database_schema.py and created by DatabaseManager.
        """
        bt.logging.debug("_ensure_tables_exist() is deprecated - tables are created centrally")
        return
    
    def start_monitoring(self, thread_nice_value: int = 0):
        """Start the transaction monitoring thread."""
        with self._thread_lock:
            if self.monitoring_thread and self.monitoring_thread.is_alive():
                bt.logging.info("Transaction monitoring thread is already running")
                return
            
            bt.logging.info(f"Starting transaction monitoring thread with nice value: {thread_nice_value}")
            self.stop_event.clear()
            self.should_exit = False
            self.is_monitoring = False
            self.subscription = None
            
            try:
                # Create a new thread
                self.monitoring_thread = threading.Thread(
                    target=self._monitoring_thread_loop, 
                    daemon=True,
                    name="TransactionMonitor"
                )
                self._status = "STARTING"
                self._thread_startup_time = time.time()
                
                # Start the thread
                self.monitoring_thread.start()
                bt.logging.info(f"Transaction monitoring thread started: {self.monitoring_thread.name} (ID: {self.monitoring_thread.ident})")
                
                # Try to set thread priority if running on Linux
                if thread_nice_value != 0:
                    try:
                        # Wait a moment for the thread to fully start
                        time.sleep(0.5)
                        
                        # Check if thread ID is available
                        if self.monitoring_thread.ident:
                            import os
                            import subprocess
                            
                            # Use subprocess instead of os.system for better error handling
                            try:
                                # First check if the process exists
                                subprocess.run(['ps', '-p', str(self.monitoring_thread.ident)], check=True, capture_output=True)
                                
                                # Then try to renice it
                                result = subprocess.run(
                                    ['renice', '-n', str(thread_nice_value), '-p', str(self.monitoring_thread.ident)],
                                    capture_output=True,
                                    text=True
                                )
                                
                                if result.returncode == 0:
                                    bt.logging.info(f"Set nice value {thread_nice_value} for thread {self.monitoring_thread.ident}")
                                else:
                                    bt.logging.warning(f"Failed to set thread priority: {result.stderr}")
                            except subprocess.CalledProcessError as e:
                                bt.logging.warning(f"Process ID {self.monitoring_thread.ident} not found for renice: {e}")
                        else:
                            bt.logging.warning("Thread ID not available, cannot set priority")
                    except Exception as e:
                        bt.logging.warning(f"Failed to set thread priority: {e}")
                
                # Set status to running
                self._status = "RUNNING"
                return True
            except Exception as e:
                bt.logging.error(f"Failed to start monitoring thread: {e}")
                self._status = "ERROR"
                return False
    
    def stop_monitoring(self):
        """
        Stop monitoring blockchain for transactions.
        
        Returns:
            bool: True if monitoring stopped successfully
        """
        try:
            bt.logging.info("Stopping transaction monitoring")
            
            # Signal the thread to stop
            self.stop_event.set()
            self.should_exit = True
            self._status = "STOPPING"
            
            # Wait for the monitoring thread to exit (with timeout)
            if self.monitoring_thread and self.monitoring_thread.is_alive():
                try:
                    bt.logging.info(f"Waiting for monitoring thread to exit (ID: {self.monitoring_thread.ident})")
                    self.monitoring_thread.join(timeout=10)
                    if self.monitoring_thread.is_alive():
                        bt.logging.warning("Monitoring thread did not exit within timeout period")
                except Exception as e:
                    bt.logging.error(f"Error waiting for monitoring thread: {e}")
            
            # Save the last processed block
            if self._last_processed_block:
                asyncio.run(self._save_last_processed_block())
            
            # Update status
            self.is_monitoring = False
            self._status = "STOPPED"
            bt.logging.info("Transaction monitoring stopped")
            return True
        except Exception as e:
            bt.logging.error(f"Error stopping transaction monitoring: {e}")
            self._status = "ERROR"
            self._last_error = str(e)
            import traceback
            bt.logging.error(traceback.format_exc())
            return False
    
    def _subscribe_to_blocks(self):
        """Subscribe to new block headers with proper error handling for API differences."""
        try:
            bt.logging.info("Attempting to subscribe to new block headers")
            
            if hasattr(self.substrate, 'subscribe_block_headers'):
                # Newer API version
                self.substrate.subscribe_block_headers(
                    callback=self._block_header_handler
                )
                bt.logging.info("Successfully subscribed to block headers using subscribe_block_headers")
            else:
                # Try the older method if available
                if hasattr(self.substrate, 'subscribe_new_heads'):
                    self.substrate.subscribe_new_heads(
                        callback=self._block_header_handler
                    )
                    bt.logging.info("Successfully subscribed to block headers using subscribe_new_heads")
                else:
                    # As a last resort, try to see if there's any similar method
                    for method_name in dir(self.substrate):
                        if 'subscribe' in method_name.lower() and ('block' in method_name.lower() or 'head' in method_name.lower()):
                            bt.logging.info(f"Trying alternative subscription method: {method_name}")
                            subscription_method = getattr(self.substrate, method_name)
                            if callable(subscription_method):
                                try:
                                    subscription_method(callback=self._block_header_handler)
                                    bt.logging.info(f"Successfully subscribed using alternative method: {method_name}")
                                    return True
                                except Exception as e:
                                    bt.logging.warning(f"Failed to use alternative method {method_name}: {e}")
                    
                    bt.logging.error("No suitable block subscription method found in substrate interface")
                    return False
            
            return True
        except Exception as e:
            bt.logging.error(f"Error subscribing to new blocks: {e}")
            return False
            
    def _monitoring_thread_loop(self):
        """Main loop for the monitoring thread."""
        try:
            if not self._is_initialized:
                bt.logging.info("Initializing TransactionMonitor before starting monitoring")
                asyncio.run(self.initialize())
            
            # Ensure substrate connection is established
            if not self.substrate:
                bt.logging.info("No substrate connection, initializing...")
                self._initialize_substrate()
            
            if not self.substrate:
                bt.logging.error("Failed to establish substrate connection")
                self._status = "ERROR"
                return
            
            bt.logging.info("Starting transaction monitoring loop")
            self.is_monitoring = True
            self._status = "RUNNING"
            
            while not self.should_exit:
                try:
                    self._process_next_block()
                except Exception as e:
                    bt.logging.error(f"Error in monitoring loop: {e}")
                    bt.logging.error(traceback.format_exc())
                    time.sleep(1)  # Avoid tight loop on repeated errors
            
            bt.logging.info("Transaction monitoring stopped")
            self.is_monitoring = False
            self._status = "STOPPED"
        except Exception as e:
            error_msg = f"Error in transaction monitoring thread: {e}"
            bt.logging.error(error_msg)
            bt.logging.error(traceback.format_exc())
            self._status = "ERROR"
            self._last_error = error_msg
            self.is_monitoring = False
    
    def _block_header_handler(self, obj, update_nr, subscription_id):
        """Handle a new block header notification."""
        try:
            # Make block reception more visible in logs with clear border
            bt.logging.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
            bt.logging.info("🔔 NEW BLOCK RECEIVED 🔔")
            
            if self.verbose:
                # Only log the full JSON in verbose mode
                bt.logging.debug("Block header received: %s", json.dumps(obj, indent=2, default=str))
            
            # Extract block number from header
            try:
                block_num_val = obj.get("header", {}).get("number")
                block_num = int(block_num_val, 16) if isinstance(block_num_val, str) else block_num_val
                bt.logging.info(f"🔢 BLOCK NUMBER: {block_num}")
                
                # Get block hash
                block_hash = self.substrate.get_block_hash(block_num)
                if not block_hash:
                    bt.logging.error("Could not compute block hash for block number %s", block_num)
                    return
                bt.logging.info(f"🧩 Block hash: {block_hash}")
            except Exception as e:
                bt.logging.error("Error computing block hash from header: %s", e)
                return
            
            # Log timestamps
            current_time = datetime.now(timezone.utc)
            bt.logging.info(f"🕒 Current time: {current_time.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]} UTC")
            
            # Process this block
            start_time = time.time()
            self._process_block(block_num, block_hash)
            processing_time = time.time() - start_time
            
            # Log success and update status
            self._last_successful_block = block_num
            bt.logging.info(f"✅ Finished processing block #{block_num} in {processing_time:.2f} seconds")
            bt.logging.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
        except Exception as e:
            bt.logging.error(f"Error processing block header content: {e}")
            bt.logging.error(traceback.format_exc())
    
    def _extract_timestamp(self, extrinsics):
        """Extract timestamp from block extrinsics if available."""
        try:
            for extrinsic in extrinsics:
                if isinstance(extrinsic, dict) and 'call' in extrinsic and 'module' in extrinsic['call']:
                    if extrinsic['call']['module'] == 'Timestamp' and 'call' in extrinsic['call'] and extrinsic['call']['call'] == 'set':
                        if 'args' in extrinsic['call'] and 'now' in extrinsic['call']['args']:
                            timestamp = int(extrinsic['call']['args']['now'])
                            return datetime.fromtimestamp(timestamp // 1000, tz=timezone.utc)
            return None
        except Exception as e:
            bt.logging.debug(f"Error extracting timestamp: {e}")
            return None
    
    def _process_block(self, block_num: int, block_hash: str):
        """Process a block for stake-related events."""
        try:
            # Get block data
            block = self.substrate.get_block(block_hash)
            if not block:
                bt.logging.error(f"Could not get block data for block {block_num}")
                return
            
            # Get timestamp from extrinsics
            block_timestamp = self._extract_timestamp(block.get('extrinsics', []))
            if block_timestamp:
                bt.logging.debug(f"Block {block_num} timestamp: {block_timestamp}")
            
            # Process extrinsics first
            self._process_extrinsics(block_num, block.get('extrinsics', []))
            
            # Get and process events
            try:
                events = self.substrate.query("System", "Events", block_hash=block_hash)
                if not events:
                    bt.logging.error(f"Could not get events for block {block_num}")
                    return
                
                # Process events and match with pending calls
                self._process_events(block_num, events)
            except Exception as e:
                bt.logging.error(f"Error querying events for block {block_hash}: {e}")
                bt.logging.error(traceback.format_exc())
            
            # Purge old pending calls
            self._purge_old_calls(block_num)
            
        except Exception as e:
            bt.logging.error(f"Error processing block {block_num}: {e}")
            bt.logging.error(traceback.format_exc())
    
    def _process_events(self, block_num, events_decoded):
        """Process events in a block and match them with pending extrinsic calls."""
        try:
            event_values = events_decoded.value if hasattr(events_decoded, 'value') else events_decoded
            stake_events_found = 0
            
            for idx, event_record in enumerate(event_values):
                try:
                    if self.verbose:
                        bt.logging.info(f"Raw event record {idx}: {str(event_record)}")
                    
                    # Extract event information - handle both dict and object-style access
                    if hasattr(event_record, 'get'):  # Dictionary-like object
                        event = event_record.get('event', {})
                        if hasattr(event, 'get'):
                            module_id = event.get('module_id')
                            event_id = event.get('event_id')
                            attributes = event.get('attributes')
                        else:
                            # Handle case where event is an object
                            module_id = event.module_id if hasattr(event, 'module_id') else None
                            event_id = event.event_id if hasattr(event, 'event_id') else None
                            attributes = event.attributes if hasattr(event, 'attributes') else None
                    else:  # Direct attribute access
                        if hasattr(event_record, 'event'):
                            event = event_record.event
                            module_id = event.module_id if hasattr(event, 'module_id') else None
                            event_id = event.event_id if hasattr(event, 'event_id') else None
                            attributes = event.attributes if hasattr(event, 'attributes') else None
                    
                    # Process SubtensorModule stake events
                    if module_id == 'SubtensorModule' and event_id in ['StakeAdded', 'StakeRemoved', 'StakeTransferred']:
                        try:
                            self._handle_stake_event(module_id, event_id, attributes, idx, block_num)
                            stake_events_found += 1
                        except Exception as e:
                            bt.logging.error(f"Error handling stake event in block {block_num}: {e}")
                            bt.logging.error(traceback.format_exc())
                except Exception as e:
                    bt.logging.error(f"Error processing event {idx} in block {block_num}: {e}")
                    bt.logging.error(traceback.format_exc())
                    continue
            
            bt.logging.info(f"Found {stake_events_found} stake-related events in block {block_num}")
        except Exception as e:
            bt.logging.error(f"Error processing events for block {block_num}: {e}")
            bt.logging.error(traceback.format_exc())
    
    def _get_extrinsics(self, block_hash):
        """
        Get extrinsics for a block.
        
        Args:
            block_hash: Block hash
            
        Returns:
            List of extrinsics
        """
        try:
            block = self.substrate.get_block(block_hash=block_hash)
            return block.get('extrinsics', [])
        except Exception as e:
            bt.logging.error(f"Error getting extrinsics for block {block_hash}: {e}")
            return []
    
    def _process_extrinsics(self, block_num, extrinsics):
        """
        Process extrinsics in a block to record pending stake-related calls.
        
        Args:
            block_num: Block number
            extrinsics: List of extrinsics
            
        Returns:
            int: Number of filtered extrinsics for the target subnet
        """
        try:
            stake_calls_found = 0
            subnet_filtered_count = 0
            bt.logging.info(f"Processing {len(extrinsics)} extrinsics in block {block_num}")
            
            for ext_idx, ext in enumerate(extrinsics):
                try:
                    # Use ext.value if available; otherwise assume ext is a dict.
                    ext_dict = ext.value if hasattr(ext, 'value') else ext
                    
                    if self.verbose:
                        bt.logging.info(f"Raw extrinsic data from block {block_num}, idx {ext_idx}: {json.dumps(ext_dict, indent=2, default=str)}")
                    
                    # Extract the coldkey from the extrinsic's "address" field.
                    coldkey = ext_dict.get('address')
                    
                    call_info = ext_dict.get('call', {})
                    call_module = call_info.get('call_module')
                    call_function = call_info.get('call_function')
                    
                    # Only process stake-related calls from SubtensorModule
                    if call_module == 'SubtensorModule' and call_function in [
                        'add_stake', 'remove_stake', 'add_stake_limit', 'remove_stake_limit', 'add_stake_batch', 'transfer_stake'
                    ]:
                        stake_calls_found += 1
                        if self._process_stake_call(ext_idx, block_num, call_info, coldkey, call_function):
                            subnet_filtered_count += 1
                except Exception as e:
                    bt.logging.error(f"Error processing extrinsic {ext_idx} in block {block_num}: {e}")
                    import traceback
                    bt.logging.error(traceback.format_exc())
                    continue
            
            bt.logging.info(f"Found {stake_calls_found} stake-related extrinsics in block {block_num}, {subnet_filtered_count} for subnet {self.subnet_id}")
            return subnet_filtered_count
        except Exception as e:
            bt.logging.error(f"Error processing extrinsics for block {block_num}: {e}")
            import traceback
            bt.logging.error(traceback.format_exc())
            return 0
    
    def _process_stake_call(self, ext_idx, block_num, call_info, coldkey, call_function):
        """Process a single stake-related call."""
        try:
            # Extract call arguments
            args = call_info.get('args', {})
            
            # Map call functions to transaction types
            call_type_map = {
                'add_stake': ADD_STAKE,
                'remove_stake': REMOVE_STAKE,
                'transfer_stake': TRANSFER_STAKE,
                'move_stake': MOVE_STAKE,
                'swap_stake': SWAP_STAKE,
                'add_stake_limit': ADD_STAKE,      # Maps to same type as non-limit version
                'remove_stake_limit': REMOVE_STAKE, # Maps to same type as non-limit version
                'swap_stake_limit': SWAP_STAKE      # Maps to same type as non-limit version
            }
            
            transaction_type = call_type_map.get(call_function, UNKNOWN)
            if transaction_type == UNKNOWN:
                bt.logging.debug(f"Ignoring unknown call function: {call_function}")
                return False
            
            # Extract common parameters
            hotkey = args.get('hotkey')
            netuid = args.get('netuid')
            
            # Extract amount based on call type
            amount = None
            if call_function in ['add_stake', 'add_stake_limit']:
                amount = args.get('amount_staked')
            elif call_function in ['remove_stake', 'remove_stake_limit']:
                amount = args.get('amount_unstaked')
            elif call_function in ['swap_stake', 'swap_stake_limit']:
                amount = args.get('alpha_amount')
            else:
                amount = args.get('amount')
            
            # Convert amount to float if present
            if amount is not None:
                try:
                    amount = float(amount) / 1e9
                except (TypeError, ValueError) as e:
                    bt.logging.error(f"Error converting amount {amount}: {e}")
                    amount = None
            
            # Extract subnet information for stake movements
            origin_netuid = None
            destination_netuid = None
            if call_function in ['transfer_stake', 'move_stake', 'swap_stake', 'swap_stake_limit']:
                origin_netuid = args.get('origin_netuid', netuid)
                destination_netuid = args.get('destination_netuid', netuid)
            
            # For limit orders, extract limit price and partial flag
            if call_function in ['add_stake_limit', 'remove_stake_limit', 'swap_stake_limit']:
                limit_price = args.get('limit_price')
                allow_partial = args.get('allow_partial', False)
                bt.logging.debug(f"Limit order details - price: {limit_price}, allow_partial: {allow_partial}")
            
            # Store pending call
            call_data = {
                'call_function': call_function,
                'transaction_type': transaction_type,
                'hotkey': hotkey,
                'coldkey': coldkey,
                'amount': amount,
                'netuid': netuid,
                'origin_netuid': origin_netuid,
                'destination_netuid': destination_netuid,
                'timestamp': int(time.time())
            }
            
            # Add to pending calls
            if block_num not in self._pending_calls:
                self._pending_calls[block_num] = []
            self._pending_calls[block_num].append(call_data)
            
            bt.logging.debug(
                f"Recorded pending {call_function} call in block {block_num}: "
                f"hotkey={hotkey}, amount={amount}, "
                f"netuid={netuid}, origin_netuid={origin_netuid}, destination_netuid={destination_netuid}"
            )
            
            return True
        except Exception as e:
            bt.logging.error(f"Error processing stake call {ext_idx} in block {block_num}: {e}")
            bt.logging.error(traceback.format_exc())
            return False
    
    def _determine_flow_type(self, event_id: str, origin_netuid: Optional[int] = None, destination_netuid: Optional[int] = None) -> str:
        """
        Determine the flow type based on event ID and subnet information.
        
        Args:
            event_id: The event ID (e.g., StakeAdded, StakeRemoved, etc.)
            origin_netuid: The source subnet ID for stake movements
            destination_netuid: The destination subnet ID for stake movements
            
        Returns:
            str: The flow type (INFLOW, OUTFLOW, NEUTRAL, or EMISSION)
        """
        # Simple cases first
        if event_id == 'StakeAdded':
            return INFLOW
        elif event_id == 'StakeRemoved':
            return OUTFLOW
        elif event_id == 'EmissionReceived':
            return EMISSION
        
        # For stake movements, we need to consider the subnet IDs
        if event_id in ['StakeTransferred', 'StakeMoved', 'StakeSwapped']:
            # If we don't have subnet info, default to NEUTRAL for transfers
            if origin_netuid is None or destination_netuid is None:
                return NEUTRAL if event_id == 'StakeTransferred' else UNKNOWN
            
            # If this is our monitored subnet
            if origin_netuid == self.subnet_id:
                return OUTFLOW
            elif destination_netuid == self.subnet_id:
                return INFLOW
            else:
                # If neither subnet is ours, treat transfers as neutral and others as unknown
                return NEUTRAL if event_id == 'StakeTransferred' else UNKNOWN
        
        return UNKNOWN

    def _handle_stake_event(self, module_id, event_id, attributes, idx, block_num):
        """Handle a stake-related event."""
        try:
            # Extract event information
            event_hotkey = None
            event_netuid = None
            event_amount = None
            origin_netuid = None
            destination_netuid = None
            
            # Handle different attribute formats
            if isinstance(attributes, dict):
                # Try dictionary access
                event_hotkey = attributes.get('hotkey')
                event_amount = attributes.get('amount')
                try:
                    if event_amount is not None:
                        event_amount = float(event_amount) / 1e9
                except:
                    bt.logging.error(f"Error converting amount from dict: {event_amount}")
                
                # Get subnet information
                event_netuid = attributes.get('netuid')
                origin_netuid = attributes.get('origin_netuid', event_netuid)
                destination_netuid = attributes.get('destination_netuid', event_netuid)
                
            elif isinstance(attributes, (list, tuple)):
                try:
                    # Extract basic info first
                    if len(attributes) > 1:
                        event_hotkey = str(attributes[1])
                    if len(attributes) > 2:
                        try:
                            event_amount = float(attributes[2]) / 1e9
                        except Exception as conv_e:
                            bt.logging.error(f"Error converting event amount: {conv_e}")
                    
                    # Extract subnet information based on event type
                    if event_id in ['StakeTransferred', 'StakeMoved', 'StakeSwapped']:
                        # For transfer/move/swap events, subnet info is in different positions
                        if len(attributes) > 5:  # Standard transfer
                            origin_netuid = int(attributes[4]) if attributes[4] is not None else None
                            destination_netuid = int(attributes[5]) if attributes[5] is not None else None
                        elif len(attributes) > 4:  # Fallback to basic netuid
                            event_netuid = int(attributes[4])
                            origin_netuid = event_netuid
                            destination_netuid = event_netuid
                    else:
                        # For regular stake events
                        if len(attributes) > 4:
                            event_netuid = int(attributes[4])
                            origin_netuid = event_netuid
                            destination_netuid = event_netuid
                            
                except Exception as e:
                    bt.logging.error(f"Error accessing attributes list/tuple: {e}")
            else:
                # Handle custom attribute objects
                try:
                    attr_values = (hasattr(attributes, 'value') and attributes.value) or \
                                (hasattr(attributes, '__iter__') and list(attributes)) or []
                    
                    # Process the values
                    if len(attr_values) > 1:
                        event_hotkey = str(attr_values[1])
                    if len(attr_values) > 2:
                        try:
                            event_amount = float(attr_values[2]) / 1e9
                        except:
                            pass
                    
                    # Extract subnet information
                    if event_id in ['StakeTransferred', 'StakeMoved', 'StakeSwapped'] and len(attr_values) > 5:
                        try:
                            origin_netuid = int(attr_values[4])
                            destination_netuid = int(attr_values[5])
                        except:
                            pass
                    elif len(attr_values) > 4:
                        try:
                            event_netuid = int(attr_values[4])
                            origin_netuid = event_netuid
                            destination_netuid = event_netuid
                        except:
                            pass
                        
                except Exception as e:
                    bt.logging.error(f"Error handling custom attributes object: {e}")
                    bt.logging.info(f"Event {idx} attributes not in expected format: {attributes}")
            
            # Determine transaction and flow types
            transaction_type = self._get_transaction_type(event_id)
            flow_type = self._determine_flow_type(event_id, origin_netuid, destination_netuid)
            
            # Log the decision process for debugging
            bt.logging.debug(
                f"Stake event analysis: event_id={event_id}, "
                f"origin_netuid={origin_netuid}, destination_netuid={destination_netuid}, "
                f"transaction_type={transaction_type}, flow_type={flow_type}"
            )
            
            # Create transaction data
            transaction_data = {
                'transaction_id': f"{block_num}-{idx}",
                'block_number': block_num,
                'transaction_type': transaction_type,
                'flow_type': flow_type,
                'hotkey': event_hotkey,
                'coldkey': None,  # Will be updated from pending calls if matched
                'amount': event_amount,
                'netuid': destination_netuid if flow_type == INFLOW else origin_netuid,
                'origin_netuid': origin_netuid,
                'destination_netuid': destination_netuid,
                'timestamp': int(time.time()),
                'validated': True
            }
            
            # Only store transactions that are relevant to our subnet
            if flow_type != UNKNOWN and (origin_netuid == self.subnet_id or destination_netuid == self.subnet_id):
                asyncio.run(self._store_transaction(transaction_data))
            else:
                bt.logging.debug(f"Skipping irrelevant transaction for subnet {self.subnet_id}")
            
        except Exception as e:
            bt.logging.error(f"Error handling stake event: {e}")
            bt.logging.error(traceback.format_exc())

    def _get_transaction_type(self, event_id):
        """Get transaction type from event ID."""
        event_type_map = {
            'StakeAdded': ADD_STAKE,
            'StakeRemoved': REMOVE_STAKE,
            'StakeTransferred': TRANSFER_STAKE,
            'StakeMoved': MOVE_STAKE,
            'StakeSwapped': SWAP_STAKE,
            'StakeBurned': BURN_STAKE,
            'EmissionReceived': EMISSION_REWARD
        }
        return event_type_map.get(event_id, UNKNOWN)
    
    async def _store_transaction(self, transaction_data):
        """Store a transaction in the database."""
        try:
            # Sanitize the data
            sanitized_data = self._sanitize_transaction_data(transaction_data)
            
            # Check if transaction already exists
            existing_tx = await self.db_manager.fetch_one(
                """
                SELECT id FROM transactions 
                WHERE transaction_id = ? AND block_number = ? AND amount = ?
                """,
                (
                    sanitized_data['transaction_id'],
                    sanitized_data['block_number'],
                    sanitized_data['amount']
                )
            )
            
            if existing_tx:
                bt.logging.debug(f"Transaction {sanitized_data['transaction_id']} already exists in database")
                return
            
            # Insert the transaction
            await self.db_manager.execute_query(
                """
                INSERT INTO transactions (
                    transaction_id, block_number, transaction_type, flow_type,
                    hotkey, coldkey, amount, netuid, timestamp, validated
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    sanitized_data['transaction_id'],
                    sanitized_data['block_number'],
                    sanitized_data['transaction_type'],
                    sanitized_data['flow_type'],
                    sanitized_data['hotkey'],
                    sanitized_data['coldkey'],
                    sanitized_data['amount'],
                    sanitized_data['netuid'],
                    sanitized_data['timestamp'],
                    sanitized_data['validated']
                )
            )
            
            bt.logging.debug(f"Stored transaction {sanitized_data['transaction_id']} in database")
        except Exception as e:
            bt.logging.error(f"Error storing transaction: {e}")
            bt.logging.error(traceback.format_exc())
    
    async def _save_last_processed_block(self):
        """Save the last processed block to the database."""
        try:
            if self._last_processed_block is None:
                return
            
            current_time = int(time.time())
            
            # First ensure the table exists
            try:
                await self.db_manager.execute_query("""
                    CREATE TABLE IF NOT EXISTS vesting_module_state (
                        module_name TEXT PRIMARY KEY,
                        last_block INTEGER,
                        last_timestamp INTEGER,
                        last_epoch INTEGER,
                        module_data TEXT
                    )
                """)
            except Exception as e:
                bt.logging.error(f"Error ensuring vesting_module_state table exists: {e}")
                return
            
            # Save to vesting_module_state table
            try:
                # Check if entry exists
                result = await self.db_manager.fetch_one("""
                    SELECT module_data FROM vesting_module_state
                    WHERE module_name = 'transaction_monitor'
                """)
                
                if result and result['module_data']:
                    # Update existing entry
                    try:
                        data = json.loads(result['module_data'])
                    except:
                        data = {}
                    
                    data['last_processed_block'] = self._last_processed_block
                    
                    await self.db_manager.execute_query("""
                        UPDATE vesting_module_state 
                        SET last_block = ?, last_timestamp = ?, module_data = ?
                        WHERE module_name = 'transaction_monitor'
                    """, (self._last_processed_block, current_time, json.dumps(data)))
                else:
                    # Create new entry
                    data = {'last_processed_block': self._last_processed_block}
                    
                    await self.db_manager.execute_query("""
                        INSERT INTO vesting_module_state 
                        (module_name, last_block, last_timestamp, last_epoch, module_data)
                        VALUES (?, ?, ?, ?, ?)
                    """, ('transaction_monitor', self._last_processed_block, current_time, 0, json.dumps(data)))
            
                bt.logging.debug(f"Saved last processed block: {self._last_processed_block}")
            except Exception as e:
                bt.logging.error(f"Error saving last processed block to vesting_module_state: {e}")
                    
        except Exception as e:
            bt.logging.error(f"Error in _save_last_processed_block: {e}")
    
    def _handle_concurrent_recv_error(self):
        """Handle recovery from concurrent recv error by creating a new connection."""
        bt.logging.info("Attempting to recover from concurrent recv error")
        try:
            time.sleep(5)  # Wait to allow other threads to clean up
            
            # Reset status
            self._status = "RECONNECTING"
            
            # Close old substrate connection
            if self.substrate:
                try:
                    bt.logging.info("Closing old substrate connection")
                    self.substrate.close()
                    self.substrate = None
                except Exception as e:
                    bt.logging.error(f"Error closing old substrate connection: {e}")
            
            # Create new substrate connection
            try:
                bt.logging.info("Creating new substrate connection")
                if self.explicit_endpoint:
                    bt.logging.info(f"Using explicit endpoint: {self.explicit_endpoint}")
                    self.substrate = SubstrateInterface(url=self.explicit_endpoint)
                else:
                    # Try to get endpoint from subtensor
                    if hasattr(self.subtensor, 'chain_endpoint') and self.subtensor.chain_endpoint:
                        endpoint = self.subtensor.chain_endpoint
                        bt.logging.info(f"Using subtensor chain_endpoint: {endpoint}")
                        self.substrate = SubstrateInterface(url=endpoint)
                    else:
                        bt.logging.error("No endpoint available for reconnection")
                        self._status = "ERROR"
                        return
            except Exception as e:
                bt.logging.error(f"Failed to create new substrate connection: {e}")
                self._status = "ERROR"
                return
            
            # Restart monitoring with new connection
            bt.logging.info("Starting monitoring with new connection")
            self.stop_event.clear()
            self.start_monitoring()
        except Exception as e:
            bt.logging.error(f"Error during recovery from concurrent recv error: {e}")
            import traceback
            bt.logging.error(traceback.format_exc())
    
    def _purge_old_calls(self, current_block):
        """
        Purge pending extrinsic calls older than a threshold.
        
        Standard stake calls are purged after 20 blocks.
        Limit orders (add_stake_limit, remove_stake_limit) are retained for 216000 blocks (~1 month).
        
        Args:
            current_block: Current block number
        """
        try:
            # Get counts before purging for logging
            total_calls_before = sum(len(calls) for calls in self._pending_calls.values())
            blocks_before = len(self._pending_calls)
            
            # Create a list of blocks and call indices to delete
            to_delete = []  # (block_num, call_idx) tuples
            
            for block_num, calls in list(self._pending_calls.items()):
                # Check block-level threshold (if all calls from this block should be purged)
                block_age = current_block - block_num
                
                # List of calls to remove from this block
                calls_to_remove = []
                
                for call_idx, call_data in enumerate(calls):
                    # Determine threshold based on call function
                    if call_data.get('call_function') in ['add_stake_limit', 'remove_stake_limit']:
                        threshold = 216000  # ~1 month - same as BlockMonitor.py
                    else:
                        threshold = 20  # ~5 minutes - same as BlockMonitor.py
                    
                    # Check if this call should be purged
                    if block_age >= threshold:
                        bt.logging.info(f"Purging call from block {block_num} (age: {block_age} blocks): "
                                       f"{call_data.get('call_function')} for "
                                       f"hotkey: {call_data.get('hotkey', '')[:10] if call_data.get('hotkey') else 'None'}")
                        calls_to_remove.append(call_idx)
                
                # Sort in reverse to avoid index shifting when we remove
                for call_idx in sorted(calls_to_remove, reverse=True):
                    calls.pop(call_idx)
                
                # If all calls were removed, remove the block entry
                if not calls:
                    to_delete.append(block_num)
            
            # Delete empty block entries
            for block_num in to_delete:
                if block_num in self._pending_calls:
                    del self._pending_calls[block_num]
            
            # Get counts after purging for logging
            total_calls_after = sum(len(calls) for calls in self._pending_calls.values())
            blocks_after = len(self._pending_calls)
            
            purged_calls = total_calls_before - total_calls_after
            purged_blocks = blocks_before - blocks_after
            
            if purged_calls > 0 or purged_blocks > 0:
                bt.logging.info(f"Purged {purged_calls} calls from {purged_blocks} blocks. "
                               f"Remaining: {total_calls_after} calls in {blocks_after} blocks.")
            
            # Final summary of remaining calls
            if total_calls_after > 0:
                # Log details about remaining calls
                bt.logging.info(f"After purge: {total_calls_after} pending calls in {blocks_after} blocks")
                
                # Log details of oldest remaining calls for debugging
                if blocks_after > 0:
                    oldest_block = min(self._pending_calls.keys())
                    oldest_age = current_block - oldest_block
                    oldest_calls = self._pending_calls[oldest_block]
                    bt.logging.info(f"Oldest pending calls: block {oldest_block} (age: {oldest_age} blocks), {len(oldest_calls)} calls")
                    
                    # Log some details about the oldest calls
                    for idx, call in enumerate(oldest_calls[:3]):  # Show at most 3 examples
                        bt.logging.info(f"  Oldest call {idx}: {call.get('call_function')} for "
                                      f"hotkey: {call.get('hotkey', '')[:10] if call.get('hotkey') else 'None'}, "
                                      f"netuid: {call.get('netuid')}")
                    
                    if len(oldest_calls) > 3:
                        bt.logging.info(f"  ... and {len(oldest_calls) - 3} more calls in block {oldest_block}")
        except Exception as e:
            bt.logging.error(f"Error purging old calls: {e}")
            import traceback
            bt.logging.error(traceback.format_exc())
    
    async def _load_last_processed_block(self):
        """Load the last processed block from the database."""
        try:
            bt.logging.info("Loading last processed block from database")
            
            # Try to load from vesting_module_state
            try:
                # First check if the table exists
                table_exists = await self.db_manager.fetch_one("""
                    SELECT name FROM sqlite_master 
                    WHERE type='table' AND name='vesting_module_state'
                """)
                
                if table_exists:
                    result = await self.db_manager.fetch_one("""
                        SELECT module_data, last_block FROM vesting_module_state
                        WHERE module_name = 'transaction_monitor'
                    """)
                    
                    if result and result['module_data']:
                        try:
                            data = json.loads(result['module_data'])
                            block_num = data.get('last_processed_block', result['last_block'])
                            self._last_processed_block = block_num
                            bt.logging.info(f"Loaded last processed block from vesting_module_state: {block_num}")
                            return block_num
                        except Exception as e:
                            bt.logging.error(f"Error parsing module_data: {e}, falling back to last_block")
                            if result['last_block']:
                                self._last_processed_block = result['last_block']
                                bt.logging.info(f"Loaded last processed block from last_block field: {result['last_block']}")
                                return result['last_block']
                    
                    # No entry found or data is empty
                    bt.logging.info("No last processed block found in vesting_module_state")
                else:
                    bt.logging.info("vesting_module_state table doesn't exist yet, creating it")
                    # Create the table if it doesn't exist
                    await self.db_manager.execute_query("""
                        CREATE TABLE IF NOT EXISTS vesting_module_state (
                            module_name TEXT PRIMARY KEY,
                            last_block INTEGER,
                            last_timestamp INTEGER,
                            last_epoch INTEGER,
                            module_data TEXT
                        )
                    """)
                    bt.logging.info("vesting_module_state table created successfully")
            except Exception as e:
                bt.logging.error(f"Error with vesting_module_state table: {e}")
                bt.logging.info("Creating vesting_module_state table if it doesn't exist")
                try:
                    await self.db_manager.execute_query("""
                        CREATE TABLE IF NOT EXISTS vesting_module_state (
                            module_name TEXT PRIMARY KEY,
                            last_block INTEGER,
                            last_timestamp INTEGER,
                            last_epoch INTEGER,
                            module_data TEXT
                        )
                    """)
                    bt.logging.info("Ensured vesting_module_state table exists")
                except Exception as create_e:
                    bt.logging.error(f"Error creating vesting_module_state table: {create_e}")
            
            # If we're here, we couldn't load from database
            # Use current block if possible
            if self.substrate:
                try:
                    current_block = self.get_current_block_number()
                    self._last_processed_block = current_block
                    bt.logging.info(f"Using current block as start point: {current_block}")
                    
                    # Save it immediately to establish a record
                    await self._save_last_processed_block()
                    return current_block
                except Exception as e:
                    bt.logging.error(f"Error getting current block: {e}")
            
            # If all else fails, start from 0
            bt.logging.info("Starting transaction monitoring from block 0")
            self._last_processed_block = 0
            return 0
        except Exception as e:
            bt.logging.error(f"Error loading last processed block: {e}")
            self._last_processed_block = 0
            return 0
    
    def get_current_block_number(self):
        """Get the current block number from the blockchain."""
        try:
            if not self.substrate:
                bt.logging.warning("No substrate connection when getting current block")
                return 0
                
            # Method 1: Using get_chain_head and get_block_number
            try:
                chain_head = self.substrate.get_chain_head()
                if chain_head:
                    block_num = self.substrate.get_block_number(chain_head)
                    bt.logging.info(f"Current block number from chain head: {block_num}")
                    return block_num
            except Exception as e:
                bt.logging.warning(f"Error getting block number from chain head: {e}")
                
            # Method 2: Use get_block
            try:
                latest_block = self.substrate.get_block()
                if latest_block and 'header' in latest_block and 'number' in latest_block['header']:
                    block_num = latest_block['header']['number']
                    bt.logging.info(f"Current block number from latest block: {block_num}")
                    return block_num
            except Exception as e:
                bt.logging.warning(f"Error getting latest block: {e}")
                
            # Method 3: Try subtensor's get_current_block if available
            if hasattr(self.subtensor, 'get_current_block'):
                try:
                    block_num = self.subtensor.get_current_block()
                    bt.logging.info(f"Current block number from subtensor: {block_num}")
                    return block_num
                except Exception as e:
                    bt.logging.warning(f"Error getting block from subtensor: {e}")
                    
            bt.logging.error("Failed to get current block number through any method")
            return 0
        except Exception as e:
            bt.logging.error(f"Error in get_current_block_number: {e}")
            return 0
    
    @property
    def is_running(self):
        """Check if transaction monitoring is running."""
        return self._status == "RUNNING"
    
    def log_status_summary(self):
        """Log a summary of the transaction monitor status."""
        bt.logging.info("===== Transaction Monitor Status Summary =====")
        bt.logging.info(f"Status: {self._status}")
        bt.logging.info(f"Last successful block: {self._last_successful_block}")
        bt.logging.info(f"Last processed block: {self._last_processed_block}")
        bt.logging.info(f"Thread running: {self.monitoring_thread and self.monitoring_thread.is_alive()}")
        bt.logging.info(f"Substrate connection: {'Active' if self.substrate else 'None'}")
        bt.logging.info(f"Explicit endpoint: {self.explicit_endpoint}")
        bt.logging.info(f"Pending calls: {len(self._pending_calls)}")
        bt.logging.info(f"Last error: {self._last_error}")
        bt.logging.info("==========================================")
    
    def _process_next_block(self):
        """Process the next block in the chain."""
        try:
            # Get current block number
            chain_head = self.substrate.get_chain_head()
            current_block = self.substrate.get_block_number(chain_head)
            
            # If we haven't processed any blocks yet, start from current - 50
            # This ensures we don't try to process too old blocks that might be discarded
            if self._last_processed_block is None:
                self._last_processed_block = max(0, current_block - 50)
                bt.logging.info(f"Starting block processing from block {self._last_processed_block}")
            
            next_block = self._last_processed_block + 1
            
            # Don't process future blocks
            if next_block > current_block:
                time.sleep(1)  # Wait for next block
                return
            
            # Check if we're too far behind current block
            blocks_behind = current_block - next_block
            if blocks_behind > 500:  # If we're more than 500 blocks behind
                # Skip ahead to avoid processing discarded blocks
                new_start = max(next_block, current_block - 400)  # Stay within safe range
                bt.logging.warning(f"Too far behind ({blocks_behind} blocks), skipping to block {new_start}")
                self._last_processed_block = new_start - 1
                next_block = new_start
            
            try:
                # Get block hash
                block_hash = self.substrate.get_block_hash(next_block)
                if not block_hash:
                    bt.logging.error(f"Could not get hash for block {next_block}")
                    return
                
                # Process the block
                self._process_block(next_block, block_hash)
                
                # Update last processed block
                self._last_processed_block = next_block
                
                # Save progress periodically (every 100 blocks)
                if next_block % 100 == 0:
                    asyncio.run(self._save_last_processed_block())
                    bt.logging.info(f"Saved progress at block {next_block}")
                
                # Purge old pending calls
                self._purge_old_calls(next_block)
                
            except SubstrateRequestException as e:
                error_msg = str(e)
                if "State already discarded" in error_msg:
                    # Skip this block and move forward
                    bt.logging.warning(f"State discarded for block {next_block}, skipping ahead")
                    self._last_processed_block = next_block  # Skip this block
                elif "concurrent.futures._base.TimeoutError" in error_msg:
                    bt.logging.warning("Detected timeout error, attempting recovery...")
                    self._handle_concurrent_recv_error()
                else:
                    bt.logging.error(f"Substrate request error for block {next_block}: {e}")
                time.sleep(1)  # Wait before retry
            
        except Exception as e:
            bt.logging.error(f"Error processing next block: {e}")
            bt.logging.error(traceback.format_exc())
            time.sleep(1)  # Wait before retry 

    def _initialize_substrate(self):
        """Initialize the substrate connection."""
        try:
            if self.explicit_endpoint:
                bt.logging.info(f"Using explicit endpoint: {self.explicit_endpoint}")
                self.substrate = SubstrateInterface(url=self.explicit_endpoint)
            else:
                # Try to get endpoint from subtensor
                if hasattr(self.subtensor, 'chain_endpoint') and self.subtensor.chain_endpoint:
                    endpoint = self.subtensor.chain_endpoint
                    bt.logging.info(f"Using subtensor chain_endpoint: {endpoint}")
                    self.substrate = SubstrateInterface(url=endpoint)
                else:
                    bt.logging.error("No endpoint available for reconnection")
                    self._status = "ERROR"
                    return
            
            # Test the connection
            try:
                # Get chain properties instead of using get_chain_info()
                chain_head = self.substrate.get_chain_head()
                
                # Try to get chain name - handle potential exceptions safely
                try:
                    # Different approach to get chain info
                    chain_props = "Unknown"
                    if hasattr(self.substrate, 'get_metadata'):
                        metadata = self.substrate.get_metadata(block_hash=chain_head)
                        if metadata and hasattr(metadata, 'metadata') and hasattr(metadata.metadata, 'name'):
                            chain_props = metadata.metadata.name
                        elif metadata and hasattr(metadata, 'metadata_version'):
                            chain_props = f"Substrate (Metadata v{metadata.metadata_version})"
                    bt.logging.info(f"Connected to blockchain: {chain_props}")
                except Exception as e:
                    bt.logging.warning(f"Could not get chain name: {e}, but connection established")
                
                # Log current block information
                try:
                    current_block = self.substrate.get_block_number(chain_head)
                    bt.logging.info(f"Current chain head: Block #{current_block}")
                except Exception as e:
                    bt.logging.warning(f"Could not get block number: {e}, but connection established")
                
                # Connection successful if we get here
                bt.logging.info("Blockchain connection established successfully")
            except Exception as e:
                bt.logging.error(f"Failed to connect to blockchain: {e}")
                # Handle error but don't raise - we want initialization to succeed even if blockchain connection fails
                bt.logging.warning("Will continue without blockchain connection and retry later")
            
            self._is_initialized = True
            bt.logging.info("Blockchain connection established successfully")
        except Exception as e:
            bt.logging.error(f"Error initializing substrate connection: {e}")
            self._status = "ERROR"
            self._last_error = str(e)
            self._is_initialized = False
            bt.logging.error(traceback.format_exc())
    
    def _sanitize_transaction_data(self, transaction_data):
        """Sanitize transaction data before storing it in the database."""
        # Implement the sanitization logic here
        return transaction_data 