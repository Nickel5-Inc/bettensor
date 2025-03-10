"""
Transaction monitoring module for the vesting system.

This module provides detailed tracking of stake-related transactions on the
Bittensor blockchain, including detecting, matching, and validating transactions
with their corresponding blockchain events.
"""

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
        verbose: bool = False
    ):
        """
        Initialize the transaction monitor.
        
        Args:
            subtensor: The subtensor client
            subnet_id: The subnet ID to monitor
            db_manager: Database manager for storage
            verbose: Whether to log verbose details
        """
        self.subtensor = subtensor
        self.subnet_id = subnet_id
        self.db_manager = db_manager
        self.verbose = verbose
        
        # Initialize substrate interface for direct queries
        network = self.subtensor.network
        self.substrate_url = f"wss://entrypoint-{network}.opentensor.ai:443"
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
            
            # Initialize substrate interface
            try:
                self.substrate = SubstrateInterface(
                    url=self.substrate_url
                )
                logger.info(f"Connected to substrate endpoint: {self.substrate_url}")
            except Exception as e:
                logger.error(f"Failed to connect to substrate endpoint: {e}")
                return False
            
            # Load last processed block from database
            await self._load_last_processed_block()
            
            logger.info(f"TransactionMonitor initialized. Last processed block: {self.last_block}")
            return True
        except Exception as e:
            logger.error(f"Failed to initialize TransactionMonitor: {e}")
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
    
    def start_monitoring(self):
        """
        Start monitoring blockchain for transactions.
        
        This method starts a subscription to the blockchain to monitor
        for new blocks and process transactions.
        
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
                return False
            
            # Start monitoring in a background thread
            import threading
            self.should_exit = False
            self.monitoring_thread = threading.Thread(
                target=self._monitoring_thread_loop,
                daemon=True
            )
            self.monitoring_thread.start()
            
            self.is_monitoring = True
            logger.info("Transaction monitoring started")
            return True
        except Exception as e:
            logger.error(f"Failed to start transaction monitoring: {e}")
            self.is_monitoring = False
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
        Main loop for the monitoring thread.
        """
        try:
            logger.info("Starting blockchain monitoring thread")
            
            # Subscribe to block headers
            self.subscription = self.substrate.subscribe_block_headers(
                self._block_header_handler
            )
            
            # Keep thread alive until should_exit is set
            while not self.should_exit:
                time.sleep(1)
                
            logger.info("Blockchain monitoring thread exiting")
        except Exception as e:
            logger.error(f"Error in blockchain monitoring thread: {e}")
        finally:
            # Ensure subscription is unsubscribed
            if self.subscription:
                try:
                    self.subscription.unsubscribe()
                except:
                    pass
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
            
            # Process block
            self._process_block(block_num, block_hash)
        except Exception as e:
            logger.error(f"Error handling block header: {e}")
    
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
                timestamp = self.substrate.query(
                    module='Timestamp',
                    storage_function='Now',
                    block_hash=block_hash
                ).value
                block_time = datetime.fromtimestamp(timestamp / 1000, timezone.utc)
            except Exception as e:
                logger.warning(f"Error getting block timestamp: {e}")
                block_time = datetime.now(timezone.utc)
            
            # Get extrinsics
            extrinsics = self._get_extrinsics(block_hash)
            
            # Process extrinsics
            self._process_extrinsics(block_num, extrinsics)
            
            # Get events
            try:
                events_decoded = self.substrate.query(
                    module="System",
                    storage_function="Events",
                    block_hash=block_hash
                )
                
                # Process events
                self._process_events(block_num, events_decoded)
            except Exception as e:
                logger.error(f"Error querying events for block {block_hash}: {e}")
            
            # Purge old calls
            self._purge_old_calls(block_num)
            
            # Update last processed block
            self.last_block = block_num
        except Exception as e:
            logger.error(f"Error processing block {block_num}: {e}")
    
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
        """
        Process extrinsics in a block to record pending stake-related calls.
        
        Args:
            block_num: Block number
            extrinsics: List of extrinsics
        """
        for ext in extrinsics:
            try:
                ext_dict = ext.value if hasattr(ext, 'value') else ext
                if self.verbose:
                    logger.info(f"Raw extrinsic data: {json.dumps(ext_dict, indent=2, default=str)}")
                
                # Extract the coldkey from the "address" field.
                coldkey = ext_dict.get('address')
                call_info = ext_dict.get('call', {})
                call_module = call_info.get('call_module')
                call_function = call_info.get('call_function')
                
                if call_module != 'SubtensorModule':
                    continue
                
                # Prepare a pending call record.
                pending = {
                    'block_number': block_num,
                    'call_function': call_function,
                    'coldkey': coldkey,
                    'hotkey': None,
                    'origin_netuid': None,    # For calls like move, transfer, swap.
                    'netuid': None,           # For standard stake calls.
                    'destination_coldkey': None,  # For transfer_stake.
                    'destination_hotkey': None,   # For move_stake.
                    'destination_netuid': None,   # For transfer_stake/move.
                    'call_amount': None,
                    'final_amount': None,
                    'validated': False,
                    'raw': call_info,
                    'timestamp': datetime.now(timezone.utc),
                    'flow_type': None,  # Will be determined when event is matched
                    'fee': None,
                    'tx_hash': None
                }
                
                params = call_info.get('call_args', [])
                if call_function in ['add_stake', 'remove_stake', 'add_stake_limit', 'remove_stake_limit']:
                    for param in params:
                        pname = param.get('name')
                        if pname == 'hotkey':
                            pending['hotkey'] = param.get('value')
                        elif pname == 'netuid':
                            try:
                                pending['netuid'] = int(param.get('value', -1))
                            except Exception as conv_e:
                                logger.error(f"Error converting netuid: {conv_e}")
                        elif pname in ['amount', 'amount_staked', 'amount_unstaked']:
                            try:
                                pending['call_amount'] = float(param.get('value', 0)) / 1e9
                            except Exception as conv_e:
                                logger.error(f"Error converting call amount: {conv_e}")
                    pending['origin_netuid'] = pending['netuid']
                    
                    # Set initial flow type based on call function
                    if call_function.startswith('add_stake'):
                        pending['flow_type'] = INFLOW
                    elif call_function.startswith('remove_stake'):
                        pending['flow_type'] = OUTFLOW
                    
                elif call_function == 'move_stake':
                    # Expected parameters: origin_hotkey, destination_hotkey, origin_netuid, destination_netuid, alpha_amount
                    for param in params:
                        pname = param.get('name')
                        if pname == 'origin_hotkey':
                            pending['hotkey'] = param.get('value')
                        elif pname == 'destination_hotkey':
                            pending['destination_hotkey'] = param.get('value')
                        elif pname == 'origin_netuid':
                            try:
                                pending['origin_netuid'] = int(param.get('value', -1))
                            except Exception as conv_e:
                                logger.error(f"Error converting origin_netuid: {conv_e}")
                        elif pname == 'destination_netuid':
                            try:
                                pending['destination_netuid'] = int(param.get('value', -1))
                            except Exception as conv_e:
                                logger.error(f"Error converting destination_netuid: {conv_e}")
                        elif pname == 'alpha_amount':
                            try:
                                pending['call_amount'] = float(param.get('value', 0)) / 1e9
                            except Exception as conv_e:
                                logger.error(f"Error converting alpha_amount: {conv_e}")
                    
                    # From the perspective of the origin hotkey, this is an outflow
                    pending['flow_type'] = OUTFLOW
                    
                elif call_function == 'transfer_stake':
                    # Expected: destination_coldkey, hotkey, origin_netuid, destination_netuid, alpha_amount
                    for param in params:
                        pname = param.get('name')
                        if pname == 'destination_coldkey':
                            pending['destination_coldkey'] = param.get('value')
                        elif pname == 'hotkey':
                            pending['hotkey'] = param.get('value')
                        elif pname == 'origin_netuid':
                            try:
                                pending['origin_netuid'] = int(param.get('value', -1))
                            except Exception as conv_e:
                                logger.error(f"Error converting origin_netuid: {conv_e}")
                        elif pname == 'destination_netuid':
                            try:
                                pending['destination_netuid'] = int(param.get('value', -1))
                            except Exception as conv_e:
                                logger.error(f"Error converting destination_netuid: {conv_e}")
                        elif pname == 'alpha_amount':
                            try:
                                pending['call_amount'] = float(param.get('value', 0)) / 1e9
                            except Exception as conv_e:
                                logger.error(f"Error converting alpha_amount: {conv_e}")
                    
                    # From the perspective of the origin hotkey/netuid, this is an outflow
                    pending['flow_type'] = OUTFLOW
                    
                elif call_function == 'swap_stake':
                    # Expected: hotkey, origin_netuid, destination_netuid, alpha_amount
                    for param in params:
                        pname = param.get('name')
                        if pname == 'hotkey':
                            pending['hotkey'] = param.get('value')
                        elif pname == 'origin_netuid':
                            try:
                                pending['origin_netuid'] = int(param.get('value', -1))
                            except Exception as conv_e:
                                logger.error(f"Error converting origin_netuid: {conv_e}")
                        elif pname == 'destination_netuid':
                            try:
                                pending['destination_netuid'] = int(param.get('value', -1))
                            except Exception as conv_e:
                                logger.error(f"Error converting destination_netuid: {conv_e}")
                        elif pname == 'alpha_amount':
                            try:
                                pending['call_amount'] = float(param.get('value', 0)) / 1e9
                            except Exception as conv_e:
                                logger.error(f"Error converting alpha_amount: {conv_e}")
                    
                    # From the perspective of the origin netuid, this is an outflow
                    # From the perspective of the destination netuid, this is an inflow
                    # We'll mark it as outflow for now and handle the destination separately
                    pending['flow_type'] = OUTFLOW
                else:
                    continue
                
                # Skip if not related to our target subnet
                origin_netuid = pending.get('origin_netuid')
                dest_netuid = pending.get('destination_netuid')
                net_netuid = pending.get('netuid')
                
                # Check if this transaction involves our subnet
                subnet_involved = False
                if net_netuid == self.subnet_id:
                    subnet_involved = True
                elif origin_netuid == self.subnet_id:
                    subnet_involved = True
                elif dest_netuid == self.subnet_id:
                    subnet_involved = True
                
                # Deep check for subnet ID in any parameter
                # This ensures we don't miss any transactions that might involve our subnet
                if not subnet_involved and self.subnet_id is not None:
                    # Search for subnet ID in all call parameters
                    for param in params:
                        param_value = param.get('value')
                        if isinstance(param_value, (int, str)) and str(param_value) == str(self.subnet_id):
                            logger.info(f"Found subnet {self.subnet_id} in parameter {param.get('name')}")
                            subnet_involved = True
                            break
                    
                    # Also check the raw call data for any mentions of our subnet
                    if not subnet_involved:
                        raw_str = str(call_info)
                        if f"netuid': {self.subnet_id}" in raw_str or f"netuid': '{self.subnet_id}'" in raw_str:
                            logger.info(f"Found subnet {self.subnet_id} in raw call data")
                            subnet_involved = True
                
                if not subnet_involved:
                    if self.verbose:
                        logger.debug(f"Skipping call not involving our subnet {self.subnet_id}")
                    continue
                
                # Store pending call
                call_id = self.call_counter
                self.call_counter += 1
                self.pending_calls[call_id] = pending
                
                logger.info(
                    f"Recorded pending call {call_id} in block {block_num}: {pending['call_function']} - "
                    f"Coldkey: {pending.get('coldkey')}, Hotkey: {pending.get('hotkey')}, "
                    f"Call Amount: {pending.get('call_amount') if pending.get('call_amount') is not None else 'None'}, "
                    f"Flow Type: {pending.get('flow_type')}, "
                    f"Origin Netuid: {pending.get('origin_netuid') if pending.get('origin_netuid') is not None else pending.get('netuid')}, "
                    f"Destination Netuid: {pending.get('destination_netuid') if pending.get('destination_netuid') is not None else 'N/A'}"
                )
            except Exception as e:
                logger.error(f"Error processing extrinsic in block {block_num}: {e}")
    
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
    
    def _store_transaction(self, transaction_data):
        """
        Store a validated transaction in the database.
        
        Args:
            transaction_data: Transaction data to store
        """
        try:
            # Collect necessary data
            db_data = {
                'block_number': transaction_data['block_number'],
                'timestamp': int(transaction_data['timestamp'].timestamp()),
                'transaction_type': transaction_data['call_function'],
                'flow_type': transaction_data['flow_type'],
                'hotkey': transaction_data['hotkey'],
                'coldkey': transaction_data['coldkey'],
                'call_amount': transaction_data['call_amount'],
                'final_amount': transaction_data['final_amount'],
                'fee': transaction_data['fee'],
                'origin_netuid': transaction_data.get('origin_netuid') or transaction_data.get('netuid'),
                'destination_netuid': transaction_data.get('destination_netuid'),
                'destination_coldkey': transaction_data.get('destination_coldkey'),
                'destination_hotkey': transaction_data.get('destination_hotkey'),
                'validated': 1  # True
            }
            
            # Queue insertion in database
            query = """
                INSERT INTO stake_transactions (
                    block_number, timestamp, transaction_type, flow_type, hotkey, coldkey,
                    call_amount, final_amount, fee, origin_netuid, destination_netuid,
                    destination_coldkey, destination_hotkey, validated
                ) VALUES (
                    :block_number, :timestamp, :transaction_type, :flow_type, :hotkey, :coldkey,
                    :call_amount, :final_amount, :fee, :origin_netuid, :destination_netuid,
                    :destination_coldkey, :destination_hotkey, :validated
                )
            """
            self.db_manager.queue_query(query, db_data)
            
            logger.info(f"Queued transaction for DB: {transaction_data['call_function']} - {transaction_data['flow_type']} for {transaction_data['hotkey']}")
        except Exception as e:
            logger.error(f"Error storing transaction: {e}")
    
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