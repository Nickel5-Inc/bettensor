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
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any, Set, Union, Tuple

import bittensor as bt
from substrateinterface import SubstrateInterface
from substrateinterface.exceptions import SubstrateRequestException

from bettensor.validator.database.database_manager import DatabaseManager

logger = logging.getLogger(__name__)

# Transaction flow categories
INFLOW = "inflow"         # Stake added to the hotkey
OUTFLOW = "outflow"       # Stake removed from the hotkey
NEUTRAL = "neutral"       # No net change to the hotkey's stake
EMISSION = "emission"     # Stake added via network emissions/rewards

# Transaction types
ADD_STAKE = "add_stake"
REMOVE_STAKE = "remove_stake"
TRANSFER_OWNERSHIP = "transfer_ownership"
BURN_STAKE = "burn_stake"
MOVE_STAKE = "move_stake"
SWAP_STAKE = "swap_stake"
EMISSION_REWARD = "emission"
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
        self.subtensor = subtensor
        self.subnet_id = subnet_id
        self.db_manager = db_manager
        self.verbose = verbose
        # Store for backward compatibility but not used - monitoring happens via real-time block subscription
        self.query_interval_seconds = query_interval_seconds
        
        # Initialize substrate interface for direct queries
        if explicit_endpoint:
            self.substrate_url = explicit_endpoint
            logger.info(f"Using explicit endpoint for transaction monitoring: {self.substrate_url}")
        else:
            # Always use the mainnet endpoint for monitoring stake transactions
            self.substrate_url = "wss://entrypoint-finney.opentensor.ai:443"
            logger.info(f"Using mainnet endpoint for transaction monitoring: {self.substrate_url}")
        
        self.substrate = None  # Will be initialized later
        
        # Monitoring state
        self.subscription = None
        self.is_monitoring = False
        self.should_exit = False
        
        # Dictionary to store pending stake extrinsic calls
        # Keys: unique call IDs; Values: dict with call details
        self.pending_calls = {}
        self.call_counter = 0
    
    async def initialize(self):
        """
        Initialize the transaction monitor.
        
        This method initializes the transaction monitor but no longer creates 
        database tables as this is now handled centrally by the DatabaseManager
        using schema definitions from database_schema.py.
        
        Returns:
            bool: True if initialization was successful
        """
        try:
            logger.info("Initializing TransactionMonitor")
            
            # Log the configuration settings
            logger.info(f"Monitoring subnet ID: {self.subnet_id}")
            logger.info(f"Real-time block monitoring: Enabled (subscription-based)")
            logger.info(f"Verbose mode: {'Enabled' if self.verbose else 'Disabled'}")
            
            # Initialize substrate interface
            try:
                logger.debug(f"Connecting to substrate endpoint: {self.substrate_url}")
                # Use a unique WebSocket connection name to avoid conflicts with other components
                self.substrate = SubstrateInterface(
                    url=self.substrate_url,
                    ws_options={'name': f'tx_monitor_{self.subnet_id}_{int(time.time())}'}
                )
                logger.info(f"Connected to substrate endpoint: {self.substrate_url}")
                
                # Get chain info
                try:
                    chain_info = self.substrate.get_chain_head()
                    if chain_info:
                        if isinstance(chain_info, dict) and 'header' in chain_info:
                            if 'number' in chain_info['header']:
                                logger.info(f"Chain head: #{chain_info['header']['number']}, hash: {chain_info['hash']}")
                            else:
                                logger.info(f"Chain head: hash: {chain_info['hash']}")
                        else:
                            logger.info(f"Connected to chain (response format unknown): {type(chain_info).__name__}")
                except Exception as e:
                    logger.warning(f"Couldn't get chain head: {e}")
            except Exception as e:
                logger.error(f"Failed to connect to substrate endpoint: {e}")
                return False
            
            # Load last processed block from database
            logger.debug("Loading last processed block from database")
            await self._load_last_processed_block()
            
            # Check how many blocks we need to process
            try:
                current_block = self.substrate.get_block_number(None)
                blocks_behind = current_block - self.last_block if self.last_block > 0 else 0
                logger.info(f"Current blockchain block: {current_block}")
                if blocks_behind > 0:
                    logger.info(f"Transaction monitor is {blocks_behind} blocks behind the chain head")
                else:
                    logger.info("Transaction monitor is up to date with the chain")
            except Exception as e:
                logger.warning(f"Couldn't calculate blocks behind: {e}")
            
            logger.info(f"TransactionMonitor initialized. Last processed block: {self.last_block}")
            return True
        except Exception as e:
            logger.error(f"Failed to initialize TransactionMonitor: {e}")
            logger.exception("Initialization error details:")
            return False
    
    # DEPRECATED: Kept for backward compatibility
    async def _ensure_tables_exist(self):
        """
        DEPRECATED: Table creation is now handled centrally by DatabaseManager.
        
        This method is maintained for backward compatibility only and does nothing.
        Table schemas are defined in database_schema.py and created by DatabaseManager.
        """
        logger.debug("_ensure_tables_exist() is deprecated - tables are created centrally")
        return
    
    def start_monitoring(self, thread_nice_value: int = 0):
        """
        Start monitoring blockchain for transactions.
        
        This method starts a subscription to the blockchain to monitor
        for new blocks and process transactions.
        
        Args:
            thread_nice_value: Nice value for thread scheduling (-20 to 19, lower means higher priority)
                               Only works on UNIX systems.
                               
        Returns:
            bool: True if monitoring started successfully
        """
        if self.is_monitoring:
            logger.warning("Transaction monitoring is already running")
            return True
        
        try:
            logger.info(f"Starting transaction monitoring for subnet {self.subnet_id}")
            
            if not self.substrate:
                logger.error("Substrate interface not initialized")
                
                # Try to initialize a new substrate interface
                try:
                    logger.info(f"Attempting to initialize substrate interface to mainnet: {self.substrate_url}")
                    # Use a unique connection name to avoid conflicts
                    connection_name = f'tx_monitor_start_{self.subnet_id}_{int(time.time())}'
                    logger.debug(f"Using unique connection name: {connection_name}")
                    self.substrate = SubstrateInterface(
                        url=self.substrate_url,
                        ws_options={'name': connection_name}
                    )
                    logger.info("Successfully created substrate interface connection to mainnet")
                    
                    # Test connection
                    try:
                        chain_info = self.substrate.get_chain_head()
                        # Properly check the type and structure of chain_info
                        if isinstance(chain_info, dict) and 'header' in chain_info and isinstance(chain_info['header'], dict) and 'number' in chain_info['header']:
                            logger.info(f"Connected to mainnet: #{chain_info['header']['number']} (using {self.substrate_url})")
                        else:
                            # Handle unexpected response format
                            logger.info(f"Connected to mainnet (format unknown): {type(chain_info).__name__} (using {self.substrate_url})")
                            if isinstance(chain_info, dict):
                                logger.debug(f"Chain info keys: {list(chain_info.keys())}")
                            elif isinstance(chain_info, str):
                                logger.debug(f"Chain info (string): {chain_info[:100]}...")
                            else:
                                logger.debug(f"Chain info type: {type(chain_info)}")
                    except Exception as e:
                        logger.warning(f"Connected to mainnet but couldn't get block details: {e} (using {self.substrate_url})")
                except Exception as e:
                    logger.error(f"Failed to initialize substrate interface to mainnet: {e}")
                    logger.exception("Substrate initialization error details:")
                    return False
            
            # Double-check that we have a valid substrate connection
            if not self.substrate:
                logger.error("Still no valid substrate interface after initialization attempt")
                return False
                
            try:
                # Test the substrate connection before proceeding
                chain_info = self.substrate.get_chain_head()
                # Properly check the type and structure of chain_info
                if isinstance(chain_info, dict) and 'header' in chain_info and isinstance(chain_info['header'], dict) and 'number' in chain_info['header']:
                    logger.info(f"Confirmed connection to chain: #{chain_info['header']['number']}")
                else:
                    # Handle unexpected response format
                    logger.info(f"Connected to chain (format unknown): {type(chain_info).__name__}")
                    if isinstance(chain_info, dict):
                        logger.debug(f"Chain info keys: {list(chain_info.keys())}")
                    elif isinstance(chain_info, str):
                        logger.debug(f"Chain info (string): {chain_info[:100]}...")
                    else:
                        logger.debug(f"Chain info type: {type(chain_info)}")
            except Exception as e:
                logger.error(f"Substrate connection test failed: {e}")
                logger.exception("Substrate connection test error details:")
                return False
            
            # Start monitoring in a background thread
            import threading
            self.should_exit = False
            logger.debug("Creating monitoring thread")
            self.monitoring_thread = threading.Thread(
                target=self._monitoring_thread_loop,
                daemon=True,
                name="vesting_transaction_monitor"
            )
            logger.debug("Starting monitoring thread")
            self.monitoring_thread.start()
            
            # Try to set thread priority if on a compatible system
            if thread_nice_value != 0:
                try:
                    import os
                    if hasattr(os, 'nice'):
                        # This is unix-specific - get thread ID and set nice value
                        # Note: This requires appropriate permissions
                        thread_id = self.monitoring_thread.ident
                        if thread_id:
                            logger.debug(f"Setting transaction monitor thread priority to {thread_nice_value}")
                            os.nice(thread_nice_value)
                except (ImportError, AttributeError, PermissionError) as e:
                    logger.debug(f"Could not set thread priority: {e}")
                              
            logger.info("Transaction monitoring started successfully")
            self.is_monitoring = True
            return True
        except Exception as e:
            logger.error(f"Failed to start transaction monitoring: {e}")
            logger.exception("Start monitoring error details:")
            return False
    
    def stop_monitoring(self):
        """
        Stop monitoring blockchain for transactions.
        
        Returns:
            bool: True if monitoring stopped successfully
        """
        if not self.is_monitoring:
            return True
        
        try:
            logger.info("Stopping transaction monitoring")
            self.should_exit = True
            
            # Unsubscribe if subscription is active
            if self.subscription:
                try:
                    self.subscription.unsubscribe()
                except Exception as e:
                    logger.warning(f"Error unsubscribing from blockchain: {e}")
                self.subscription = None
            
            # Wait for monitoring thread to exit
            if hasattr(self, 'monitoring_thread') and self.monitoring_thread.is_alive():
                self.monitoring_thread.join(timeout=5)
            
            self.is_monitoring = False
            logger.info("Transaction monitoring stopped")
            return True
        except Exception as e:
            logger.error(f"Error stopping transaction monitoring: {e}")
            return False
    
    def _monitoring_thread_loop(self):
        """
        Main monitoring thread loop.
        
        This method runs in a separate thread and sets up a subscription
        to the blockchain to receive notifications for new blocks.
        """
        self.is_monitoring = True
        try:
            logger.info("Starting blockchain monitoring thread")
            
            # Subscribe to block headers
            logger.debug("Setting up block header subscription")
            self.subscription = self.substrate.subscribe_block_headers(
                self._block_header_handler
            )
            logger.info("Successfully subscribed to block headers")
            
            # Keep thread alive until should_exit is set
            while not self.should_exit:
                time.sleep(1)
                
            logger.info("Blockchain monitoring thread exiting")
        except Exception as e:
            logger.error(f"Error in blockchain monitoring thread: {e}")
            logger.exception("Monitoring thread error details:")
        finally:
            # Ensure subscription is unsubscribed
            if self.subscription:
                try:
                    logger.debug("Unsubscribing from block headers")
                    self.subscription.unsubscribe()
                    logger.debug("Successfully unsubscribed from block headers")
                except Exception as e:
                    logger.error(f"Error unsubscribing from block headers: {e}")
                self.subscription = None
            
            self.is_monitoring = False
    
    def _block_header_handler(self, block_header, update_nr, subscription_id):
        """
        Handler for new block headers from subscription.
        
        Args:
            block_header: The block header
            update_nr: Update number
            subscription_id: Subscription ID
        """
        try:
            # Extract block number
            block_num_val = block_header.get("header", {}).get("number")
            block_num = int(block_num_val, 16) if isinstance(block_num_val, str) else block_num_val
            
            if self.verbose:
                logger.info(f"Block header received: {block_num}")
            else:
                logger.debug(f"Block header received: {block_num}")
            
            # Get block hash
            block_hash = self.substrate.get_block_hash(block_num)
            if not block_hash:
                logger.error(f"Could not compute block hash for block number {block_num}")
                return
            
            logger.debug(f"Processing block {block_num} (hash: {block_hash})")
            
            # Process block
            self._process_block(block_num, block_hash)
            
            logger.debug(f"Finished processing block {block_num}")
        except Exception as e:
            logger.error(f"Error handling block header: {e}")
            logger.exception(f"Block header handler error details for block {block_num if 'block_num' in locals() else 'unknown'}:")
    
    def _process_block(self, block_num: int, block_hash: str):
        """
        Process a block for transactions and events.
        
        Args:
            block_num: Block number
            block_hash: Block hash
        """
        try:
            # Get block timestamp
            try:
                logger.debug(f"Getting timestamp for block {block_num}")
                timestamp = self.substrate.query(
                    module='Timestamp',
                    storage_function='Now',
                    block_hash=block_hash
                ).value
                block_time = datetime.fromtimestamp(timestamp / 1000, timezone.utc)
                logger.debug(f"Block {block_num} timestamp: {block_time.isoformat()}")
            except Exception as e:
                logger.warning(f"Error getting block timestamp: {e}")
                block_time = datetime.now(timezone.utc)
            
            # Get extrinsics
            logger.debug(f"Getting extrinsics for block {block_num}")
            extrinsics = self._get_extrinsics(block_hash)
            logger.debug(f"Found {len(extrinsics) if extrinsics else 0} extrinsics in block {block_num}")
            
            # Process extrinsics
            if extrinsics:
                logger.debug(f"Processing {len(extrinsics)} extrinsics for block {block_num}")
                self._process_extrinsics(block_num, extrinsics)
            
            # Get events
            try:
                logger.debug(f"Getting events for block {block_num}")
                events_decoded = self.substrate.query(
                    module="System",
                    storage_function="Events",
                    block_hash=block_hash
                )
                
                # Process events
                if events_decoded:
                    logger.debug(f"Processing events for block {block_num}")
                    self._process_events(block_num, events_decoded)
                else:
                    logger.debug(f"No events found for block {block_num}")
            except Exception as e:
                logger.error(f"Error querying events for block {block_hash}: {e}")
            
            # Purge old calls
            self._purge_old_calls(block_num)
            
            # Update last processed block
            self.last_block = block_num
            
            if self.verbose or (block_num % 100 == 0):  # Log every 100 blocks if not verbose
                logger.info(f"Processed block {block_num}")
            else:
                logger.debug(f"Processed block {block_num}")
                
        except Exception as e:
            logger.error(f"Error processing block {block_num}: {e}")
            logger.exception(f"Block processing error details for block {block_num}:")
            # Still update last processed block to avoid getting stuck
            self.last_block = block_num
    
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
            logger.error(f"Error getting extrinsics for block {block_hash}: {e}")
            return []
    
    def _process_extrinsics(self, block_num, extrinsics):
        """Process extrinsics to detect stake-related transactions."""
        if not extrinsics:
            return 0
            
        logger.debug(f"Processing {len(extrinsics)} extrinsics in block {block_num}")
        count = 0
        
        # Process each extrinsic
        for extrinsic in extrinsics:
            try:
                # Process extrinsic here
                if not extrinsic or not hasattr(extrinsic, 'call'):
                    continue
                    
                # Get call data
                call = extrinsic.call
                if not call:
                    continue
                    
                # Check if it's a transaction we care about
                call_module = str(call.get('call_module', ''))
                call_function = str(call.get('call_function', ''))
                
                logger.debug(f"Checking extrinsic: module={call_module}, function={call_function}")
                
                # Handle different transaction types
                if call_module == 'SubtensorModule':
                    if call_function == 'add_stake':
                        count += self._handle_add_stake(block_num, extrinsic)
                    elif call_function == 'remove_stake':
                        count += self._handle_remove_stake(block_num, extrinsic)
                    elif call_function == 'transfer_stake':
                        count += self._handle_transfer_stake(block_num, extrinsic)
                    elif call_function in ['burn', 'burn_stake']:
                        count += self._handle_burn_stake(block_num, extrinsic)
                        
            except Exception as e:
                logger.error(f"Error processing extrinsic in block {block_num}: {e}")
                
        if count > 0:
            logger.info(f"Processed {count} stake-related transactions in block {block_num}")
        
        return count
                
    def _store_transaction(self, transaction_data):
        """Store a transaction in the database and memory."""
        try:
            # Log transaction details
            tx_type = transaction_data.get('transaction_type', 'unknown')
            hotkey = transaction_data.get('hotkey', 'unknown')
            amount = transaction_data.get('amount', 0)
            block_num = transaction_data.get('block_number', 0)
            
            logger.info(f"Captured transaction: type={tx_type}, hotkey={hotkey[:10]}..., amount={amount:.4f} TAO, block={block_num}")
            
            # Store in database (async)
            asyncio.run_coroutine_threadsafe(
                self._store_transaction_async(transaction_data),
                self._async_loop
            )
            
            # Store in memory cache if needed
            tx_hash = transaction_data.get('tx_hash')
            if tx_hash:
                self._transaction_cache[tx_hash] = transaction_data
                
            return True
        except Exception as e:
            logger.error(f"Error storing transaction: {e}")
            return False

    async def _store_transaction_async(self, transaction_data):
        """Asynchronously store a transaction in the database."""
        try:
            # Insert transaction into database
            await self.db_manager.execute_query("""
                INSERT INTO stake_transactions (
                    block_number, timestamp, transaction_type, hotkey, coldkey, 
                    amount, tx_hash, flow_type, transaction_success
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                transaction_data.get('block_number'),
                transaction_data.get('timestamp'),
                transaction_data.get('transaction_type'),
                transaction_data.get('hotkey', 'unknown'),
                transaction_data.get('coldkey', 'unknown'),
                transaction_data.get('amount', 0),
                transaction_data.get('tx_hash'),
                transaction_data.get('flow_type', NEUTRAL),
                transaction_data.get('transaction_success', True)
            ))
            
            logger.debug(f"Successfully stored transaction {transaction_data.get('tx_hash', 'unknown')} in database")
            
            # Notify stake change handlers
            if hasattr(self, 'stake_change_handlers') and self.stake_change_handlers:
                for handler in self.stake_change_handlers:
                    try:
                        await handler(
                            transaction_data.get('hotkey', 'unknown'),
                            transaction_data.get('coldkey', 'unknown'),
                            transaction_data.get('amount', 0),
                            transaction_data.get('transaction_type'),
                            transaction_data.get('flow_type', NEUTRAL),
                            transaction_data.get('timestamp')
                        )
                        logger.debug(f"Notified stake change handler for {transaction_data.get('transaction_type')}")
                    except Exception as e:
                        logger.error(f"Error in stake change handler: {e}")
                        
        except Exception as e:
            logger.error(f"Error storing transaction in database: {e}")
    
    def _process_events(self, block_num, events_decoded):
        """
        Process events in a block and attempt to match them with pending extrinsic calls.
        
        Args:
            block_num: Block number
            events_decoded: Decoded events
        """
        matched_calls = []
        for idx, event_record in enumerate(events_decoded.value):
            try:
                if self.verbose:
                    logger.info(f"Raw event record {idx}: {json.dumps(event_record, indent=2, default=str)}")
                
                event = event_record.get('event', {})
                module_id = event.get('module_id')
                event_id = event.get('event_id')
                attributes = event.get('attributes')
                
                if module_id != 'SubtensorModule':
                    continue
                
                # Comprehensive subnet checking in all event attributes
                subnet_involved = False
                    
                event_hotkey = None
                event_destination_hotkey = None
                event_origin_netuid = None
                event_destination_netuid = None
                event_amount = None
                event_coldkey = None
                event_destination_coldkey = None
                
                # Extract all attributes for subnet checking
                if isinstance(attributes, (list, tuple)):
                    # Parse standard attributes first
                    if event_id in ['StakeAdded', 'StakeRemoved']:
                        # Expected: [coldkey, hotkey, amount, new_total, netuid]
                        if len(attributes) >= 5:
                            event_coldkey = attributes[0]
                            event_hotkey = attributes[1]
                            try:
                                event_amount = float(attributes[2]) / 1e9
                            except Exception as conv_e:
                                logger.error(f"Error converting event amount: {conv_e}")
                            try:
                                event_origin_netuid = int(attributes[4])
                                if event_origin_netuid == self.subnet_id:
                                    subnet_involved = True
                            except Exception as conv_e:
                                logger.error(f"Error converting event netuid: {conv_e}")
                    elif event_id == 'StakeMoved':
                        # Expected: [origin_coldkey, origin_hotkey, origin_netuid, destination_hotkey, destination_netuid, amount]
                        if len(attributes) >= 6:
                            event_coldkey = attributes[0]
                            event_hotkey = attributes[1]  # origin_hotkey
                            event_destination_hotkey = attributes[3]
                            try:
                                event_amount = float(attributes[5]) / 1e9
                            except Exception as conv_e:
                                logger.error(f"Error converting event amount: {conv_e}")
                            try:
                                event_origin_netuid = int(attributes[2])
                                if event_origin_netuid == self.subnet_id:
                                    subnet_involved = True
                            except Exception as conv_e:
                                logger.error(f"Error converting event netuid: {conv_e}")
                            try:
                                event_destination_netuid = int(attributes[4])
                                if event_destination_netuid == self.subnet_id:
                                    subnet_involved = True
                            except Exception as conv_e:
                                logger.error(f"Error converting event destination netuid: {conv_e}")
                    elif event_id == 'StakeTransferred':
                        # Expected: [origin_coldkey, destination_coldkey, hotkey, origin_netuid, destination_netuid, amount]
                        if len(attributes) >= 6:
                            event_coldkey = attributes[0]
                            event_destination_coldkey = attributes[1]
                            event_hotkey = attributes[2]
                            try:
                                event_amount = float(attributes[5]) / 1e9
                            except Exception as conv_e:
                                logger.error(f"Error converting event amount: {conv_e}")
                            try:
                                event_origin_netuid = int(attributes[3])
                                if event_origin_netuid == self.subnet_id:
                                    subnet_involved = True
                            except Exception as conv_e:
                                logger.error(f"Error converting event netuid: {conv_e}")
                            try:
                                event_destination_netuid = int(attributes[4])
                                if event_destination_netuid == self.subnet_id:
                                    subnet_involved = True
                            except Exception as conv_e:
                                logger.error(f"Error converting event destination netuid: {conv_e}")
                    elif event_id == 'StakeSwapped':
                        # Expected: [coldkey, hotkey, origin_netuid, destination_netuid, amount]
                        if len(attributes) >= 5:
                            event_coldkey = attributes[0]
                            event_hotkey = attributes[1]
                            try:
                                event_amount = float(attributes[4]) / 1e9
                            except Exception as conv_e:
                                logger.error(f"Error converting event amount: {conv_e}")
                            try:
                                event_origin_netuid = int(attributes[2])
                                if event_origin_netuid == self.subnet_id:
                                    subnet_involved = True
                            except Exception as conv_e:
                                logger.error(f"Error converting event netuid: {conv_e}")
                            try:
                                event_destination_netuid = int(attributes[3])
                                if event_destination_netuid == self.subnet_id:
                                    subnet_involved = True
                            except Exception as conv_e:
                                logger.error(f"Error converting event destination netuid: {conv_e}")
                    else:
                        if self.verbose:
                            logger.info(f"Event {idx} with ID {event_id} not processed with specific logic")
                    
                    # Deep check all attributes for our subnet ID
                    for i, attr in enumerate(attributes):
                        if isinstance(attr, (int, str)) and str(attr) == str(self.subnet_id):
                            subnet_involved = True
                            logger.info(f"Found subnet {self.subnet_id} in event attribute at position {i}")
                            break
                
                # Skip if not related to our subnet
                if not subnet_involved and self.subnet_id is not None:
                    # Last resort: check raw event data
                    raw_event_str = str(event)
                    if f"netuid': {self.subnet_id}" in raw_event_str or f"netuid': '{self.subnet_id}'" in raw_event_str:
                        subnet_involved = True
                        logger.info(f"Found subnet {self.subnet_id} in raw event data")
                
                if not subnet_involved and self.subnet_id is not None:
                    if self.verbose:
                        logger.debug(f"Skipping event not involving our subnet {self.subnet_id}")
                    continue
                
                # Log event details
                logger.info(
                    f"Event {idx} in block {block_num}: {event_id} - "
                    f"Hotkey: {event_hotkey}, Amount: {event_amount}, "
                    f"Origin Netuid: {event_origin_netuid}, Destination Netuid: {event_destination_netuid}"
                )
                
                # Map event types to call functions
                mapping = {
                    'add_stake': 'StakeAdded',
                    'add_stake_limit': 'StakeAdded',
                    'remove_stake': 'StakeRemoved',
                    'remove_stake_limit': 'StakeRemoved',
                    'move_stake': 'StakeMoved',
                    'transfer_stake': 'StakeTransferred',
                    'swap_stake': 'StakeSwapped'
                }
                
                # Try to match event with pending calls
                for call_id, call_data in list(self.pending_calls.items()):
                    # Check if expected event matches actual event
                    expected_event_id = mapping.get(call_data['call_function'])
                    if expected_event_id != event_id:
                        continue
                    
                    # Match hotkey
                    if call_data['hotkey'] != event_hotkey:
                        continue
                    
                    # Match netuid
                    if call_data['call_function'] in ['move_stake', 'transfer_stake', 'swap_stake']:
                        if call_data.get('origin_netuid') is None or int(call_data.get('origin_netuid')) != event_origin_netuid:
                            continue
                    else:
                        if call_data.get('netuid') is None or int(call_data.get('netuid')) != event_origin_netuid:
                            continue
                    
                    # Calculate final amount and fee
                    if call_data['call_function'] in ['add_stake', 'add_stake_limit', 'move_stake', 'transfer_stake', 'swap_stake']:
                        final_amount = event_amount
                    else:
                        final_amount = call_data['call_amount']
                    
                    call_data['final_amount'] = final_amount
                    fee = None
                    if call_data['call_function'] in ['add_stake', 'add_stake_limit', 'move_stake', 'transfer_stake', 'swap_stake'] and call_data['call_amount'] is not None and event_amount is not None:
                        fee = call_data['call_amount'] - event_amount
                    call_data['fee'] = fee
                    
                    # Log match details
                    logger.info(
                        f"Validated pending call {call_id} (from block {call_data['block_number']}) with event {event_id} in block {block_num}: "
                        f"Coldkey: {call_data['coldkey']}, Hotkey: {event_hotkey}, "
                        f"Final Amount: {final_amount:.9f}"
                        f"{', Fee: ' + str(fee) if fee is not None else ''}, "
                        f"Origin Netuid: {event_origin_netuid}"
                    )
                    
                    # Store the transaction
                    self._store_transaction(call_data)
                    
                    # Remove from pending calls
                    del self.pending_calls[call_id]
                    matched_calls.append(call_id)
            except Exception as e:
                logger.error(f"Error processing event {idx} in block {block_num}: {e}")
        
        # Log matches
        if matched_calls:
            logger.info(f"Matched {len(matched_calls)} calls with events in block {block_num}")
    
    def _purge_old_calls(self, current_block):
        """
        Purge pending calls older than a threshold.
        
        Args:
            current_block: Current block number
        """
        threshold = 20
        to_delete = []
        for call_id, call_data in self.pending_calls.items():
            if current_block - call_data['block_number'] >= threshold:
                logger.info(f"Discarding pending call {call_id} from block {call_data['block_number']} (older than {threshold} blocks)")
                to_delete.append(call_id)
        for call_id in to_delete:
            del self.pending_calls[call_id]
    
    async def _load_last_processed_block(self):
        """
        Load last processed block from database.
        """
        try:
            result = await self.db_manager.fetch_one("""
                SELECT value FROM vesting_module_state
                WHERE module_name = 'transaction_monitor_last_block'
            """)
            
            if result:
                self.last_block = result['value']
            else:
                # Default to current block
                self.last_block = self.subtensor.get_current_block()
                # Store in database
                await self.db_manager.execute_query("""
                    INSERT INTO vesting_module_state (module_name, last_block, last_timestamp)
                    VALUES (?, ?, ?)
                    ON CONFLICT(module_name) DO UPDATE SET
                    last_block = excluded.last_block,
                    last_timestamp = excluded.last_timestamp
                """, ('transaction_monitor_last_block', self.last_block, int(time.time())))
            
            logger.info(f"Loaded last processed block: {self.last_block}")
        except Exception as e:
            logger.error(f"Error loading last processed block: {e}")
            self.last_block = self.subtensor.get_current_block()
    
    @property
    def is_running(self):
        """Check if monitoring is running."""
        return self.is_monitoring 