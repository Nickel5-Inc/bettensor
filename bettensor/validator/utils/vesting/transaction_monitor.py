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

from bettensor.validator.utils.database.database_manager import DatabaseManager

logger = logging.getLogger(__name__)

# Transaction flow categories
INFLOW = "inflow"         # Stake added to the hotkey
OUTFLOW = "outflow"       # Stake removed from the hotkey
NEUTRAL = "neutral"       # No net change to the hotkey's stake
EMISSION = "emission"     # Stake added via network emissions/rewards

class TransactionMonitor:
    """
    Monitors and tracks detailed stake transactions on the Bittensor blockchain.
    
    This class provides precise tracking of:
    1. Add stake transactions
    2. Remove stake transactions
    3. Move stake operations
    4. Transfer stake operations
    5. Swap stake operations
    
    It matches extrinsics (transaction requests) with their corresponding 
    events (confirmed transactions) and tracks detailed information including
    amounts, fees, and involved parties.
    
    Transactions are categorized as:
    - INFLOW: Stake added to the hotkey (AddStake, MoveStake/destination, SwapStake/destination)
    - OUTFLOW: Stake removed from the hotkey (RemoveStake, MoveStake/origin, TransferStake, SwapStake/origin)
    - NEUTRAL: No net change to the hotkey's stake (certain TransferStake scenarios)
    - EMISSION: Stake added via network emissions/rewards
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
            subtensor: Initialized subtensor instance
            subnet_id: The subnet ID to monitor
            db_manager: Database manager for persistent storage
            verbose: Whether to log verbose details of extrinsics and events
        """
        self.subtensor = subtensor
        self.subnet_id = subnet_id
        self.db_manager = db_manager
        self.verbose = verbose
        
        # Get substrate from subtensor
        self.substrate = subtensor.substrate
        
        # Control variables
        self.should_exit = False
        self.subscription = None
        self._is_running = False
        
        # Transaction tracking variables
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
            
            # Load last processed block from database
            await self._load_last_processed_block()
            
            # Setup WebSocket event processing
            if self.monitor_events:
                self._init_event_processing()
            
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
        Start monitoring blockchain transactions.
        
        This starts a subscription to block headers and processes
        extrinsics and events for each block.
        
        Returns:
            bool: True if monitoring was started successfully
        """
        if self._is_running:
            logger.warning("Transaction monitor is already running")
            return False
        
        try:
            logger.info(f"Starting transaction monitoring on {self.subtensor.network}")
            
            # Subscribe to block headers
            self.subscription = self.substrate.subscribe_block_headers(
                self._block_header_handler
            )
            
            self._is_running = True
            return True
        except Exception as e:
            logger.error(f"Error starting transaction monitoring: {e}")
            return False
    
    def stop_monitoring(self):
        """
        Stop monitoring blockchain transactions.
        
        Returns:
            bool: True if monitoring was stopped successfully
        """
        if not self._is_running:
            logger.warning("Transaction monitor is not running")
            return False
        
        try:
            logger.info("Stopping transaction monitoring")
            
            # Signal to exit and unsubscribe
            self.should_exit = True
            
            if self.subscription:
                self.subscription.unsubscribe()
                self.subscription = None
            
            self._is_running = False
            return True
        except Exception as e:
            logger.error(f"Error stopping transaction monitoring: {e}")
            return False
    
    def _block_header_handler(self, block_header, update_nr, subscription_id):
        """
        Handle incoming block headers.
        
        Args:
            block_header: Block header data
            update_nr: Update number
            subscription_id: Subscription ID
        """
        try:
            # Extract block number
            block_num_val = block_header.get("header", {}).get("number")
            if isinstance(block_num_val, str) and block_num_val.startswith("0x"):
                block_num = int(block_num_val, 16)
            else:
                block_num = int(block_num_val) if block_num_val else None
            
            if not block_num:
                logger.error("Could not determine block number")
                return
            
            if self.verbose:
                logger.info(f"Processing block {block_num}")
            
            # Get block hash
            block_hash = self.substrate.get_block_hash(block_num)
            if not block_hash:
                logger.error(f"Could not compute block hash for block {block_num}")
                return
            
            # Get and process extrinsics
            extrinsics = self._get_extrinsics(block_hash)
            self._process_extrinsics(block_num, extrinsics)
            
            # Get and process events
            try:
                events_decoded = self.substrate.query("System", "Events", block_hash=block_hash)
                self._process_events(block_num, events_decoded)
            except Exception as e:
                logger.error(f"Error querying events for block {block_hash}: {e}")
            
            # Purge old pending calls
            self._purge_old_calls(block_num)
            
        except Exception as e:
            logger.error(f"Error processing block header: {e}")
    
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
                    'block_num': block_num,
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
                    'flow_type': None  # Will be determined when event is matched
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
                    
                    # This is an outflow from the origin hotkey's perspective
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
                    
                    # Set initial flow type based on the netuid direction
                    if pending.get('origin_netuid') == self.subnet_id:
                        # Swapping out of our subnet
                        pending['flow_type'] = OUTFLOW
                    elif pending.get('destination_netuid') == self.subnet_id:
                        # Swapping into our subnet
                        pending['flow_type'] = INFLOW
                    else:
                        # Neither origin nor destination is our subnet
                        pending['flow_type'] = NEUTRAL
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
                
                logger.info(
                    f"Event {idx} in block {block_num}: {event_id} - "
                    f"Coldkey: {event_coldkey}, Hotkey: {event_hotkey}, "
                    f"Event Amount: {event_amount if event_amount is not None else 'None'}, "
                    f"Origin Netuid: {event_origin_netuid}, "
                    f"Destination Netuid: {event_destination_netuid if event_destination_netuid is not None else 'N/A'}"
                )
                
                # Try to match with pending calls
                mapping = {
                    'add_stake': 'StakeAdded',
                    'add_stake_limit': 'StakeAdded',
                    'remove_stake': 'StakeRemoved',
                    'remove_stake_limit': 'StakeRemoved',
                    'move_stake': 'StakeMoved',
                    'transfer_stake': 'StakeTransferred',
                    'swap_stake': 'StakeSwapped'
                }
                
                for call_id, call_data in list(self.pending_calls.items()):
                    expected_event_id = mapping.get(call_data['call_function'])
                    if expected_event_id != event_id:
                        continue
                    
                    # Match based on hotkey and netuid
                    if call_data['hotkey'] != event_hotkey:
                        continue
                    
                    # For call types with origin_netuid, match that
                    if call_data['call_function'] in ['move_stake', 'transfer_stake', 'swap_stake']:
                        if call_data.get('origin_netuid') is None or int(call_data.get('origin_netuid')) != event_origin_netuid:
                            continue
                    else:
                        if call_data.get('netuid') is None or int(call_data.get('netuid')) != event_origin_netuid:
                            continue
                    
                    # Calculate final amount and fee
                    if call_data['call_function'] in ['add_stake', 'add_stake_limit', 'move_stake', 'transfer_stake', 'swap_stake']:
                        final_amount = event_amount
                        fee = None
                        if call_data['call_amount'] is not None and event_amount is not None:
                            fee = call_data['call_amount'] - event_amount
                    else:
                        final_amount = call_data['call_amount']
                        fee = 0.0
                    
                    call_data['final_amount'] = final_amount
                    call_data['fee'] = fee
                    call_data['validated'] = True
                    
                    # Update destination information from event if available
                    if event_destination_hotkey:
                        call_data['destination_hotkey'] = event_destination_hotkey
                    if event_destination_netuid is not None:
                        call_data['destination_netuid'] = event_destination_netuid
                    if event_destination_coldkey:
                        call_data['destination_coldkey'] = event_destination_coldkey
                    
                    # For events that have origin and destination, determine flow type based on subnet_id
                    if event_id in ['StakeMoved', 'StakeSwapped', 'StakeTransferred']:
                        # Refine flow type based on the event information
                        if event_id == 'StakeMoved':
                            # For MoveStake, it's an outflow for the origin hotkey
                            # If we're monitoring for the origin hotkey, it's an outflow
                            # If we ever need to track for destination hotkey, we'd need to create a new record
                            call_data['flow_type'] = OUTFLOW
                            
                        elif event_id == 'StakeTransferred':
                            # For TransferStake:
                            if event_origin_netuid == self.subnet_id and event_destination_netuid != self.subnet_id:
                                # Moving out of our subnet - outflow
                                call_data['flow_type'] = OUTFLOW
                            elif event_origin_netuid != self.subnet_id and event_destination_netuid == self.subnet_id:
                                # Moving into our subnet - inflow
                                call_data['flow_type'] = INFLOW
                            else:
                                # Could be within same subnet but different coldkey
                                # This is the case where a validator stake replaces miner stake
                                # Considered an outflow for the hotkey
                                call_data['flow_type'] = OUTFLOW
                                
                        elif event_id == 'StakeSwapped':
                            if event_origin_netuid == self.subnet_id:
                                # Swapping out of our subnet
                                call_data['flow_type'] = OUTFLOW
                            elif event_destination_netuid == self.subnet_id:
                                # Swapping into our subnet
                                call_data['flow_type'] = INFLOW
                    
                    logger.info(
                        f"Validated pending call {call_id} (from block {call_data['block_num']}) with event {event_id} in block {block_num}: "
                        f"Coldkey: {call_data['coldkey']}, Hotkey: {event_hotkey}, Final Amount: {final_amount:.9f}"
                        f"{', Fee: ' + str(fee) if fee is not None else ''}, Flow Type: {call_data['flow_type']}"
                    )
                    
                    # Store the matched transaction in the database
                    self._store_transaction(call_data)
                    
                    # Mark for removal
                    matched_calls.append(call_id)
                
                # Remove matched calls
                for call_id in matched_calls:
                    del self.pending_calls[call_id]
                
            except Exception as e:
                logger.error(f"Error processing event {idx} in block {block_num}: {e}")
    
    def _store_transaction(self, transaction_data):
        """
        Store a validated transaction in the database.
        
        Args:
            transaction_data: Transaction data to store
        """
        try:
            # Convert transaction type to a standard format
            transaction_type = transaction_data['call_function']
            if transaction_type.startswith('add_stake'):
                transaction_type = 'add_stake'
            elif transaction_type.startswith('remove_stake'):
                transaction_type = 'remove_stake'
            
            # Create a dictionary with all the transaction data
            db_data = {
                'block_number': transaction_data['block_num'],
                'timestamp': transaction_data['timestamp'],
                'transaction_type': transaction_type,
                'flow_type': transaction_data['flow_type'],
                'coldkey': transaction_data['coldkey'],
                'hotkey': transaction_data['hotkey'],
                'call_amount': transaction_data['call_amount'],
                'final_amount': transaction_data['final_amount'],
                'fee': transaction_data.get('fee'),
                'origin_netuid': transaction_data.get('origin_netuid') or transaction_data.get('netuid'),
                'destination_netuid': transaction_data.get('destination_netuid'),
                'destination_coldkey': transaction_data.get('destination_coldkey'),
                'destination_hotkey': transaction_data.get('destination_hotkey'),
                'tx_hash': None,  # tx_hash not available in current implementation
                'validated': transaction_data['validated']
            }
            
            # Queue the transaction for insertion
            # The database manager will handle this asynchronously
            self.db_manager.queue_query(
                """
                INSERT INTO detailed_stake_transactions (
                    block_number,
                    timestamp,
                    transaction_type,
                    flow_type,
                    coldkey,
                    hotkey,
                    call_amount,
                    final_amount,
                    fee,
                    origin_netuid,
                    destination_netuid,
                    destination_coldkey,
                    destination_hotkey,
                    tx_hash,
                    validated
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, 
                (
                    db_data['block_number'],
                    db_data['timestamp'],
                    db_data['transaction_type'],
                    db_data['flow_type'],
                    db_data['coldkey'],
                    db_data['hotkey'],
                    db_data['call_amount'],
                    db_data['final_amount'],
                    db_data['fee'],
                    db_data['origin_netuid'],
                    db_data['destination_netuid'],
                    db_data['destination_coldkey'],
                    db_data['destination_hotkey'],
                    db_data['tx_hash'],
                    db_data['validated']
                )
            )
            
            logger.info(f"Queued transaction for database: {transaction_type} ({transaction_data['flow_type']}) for hotkey {transaction_data['hotkey']}")
        except Exception as e:
            logger.error(f"Error storing transaction in database: {e}")
    
    def _purge_old_calls(self, current_block):
        """
        Purge pending calls older than a threshold.
        
        Args:
            current_block: Current block number
        """
        threshold = 20  # 20 blocks is a reasonable threshold
        to_delete = []
        
        for call_id, call_data in self.pending_calls.items():
            if current_block - call_data['block_num'] >= threshold:
                logger.info(f"Discarding pending call {call_id} from block {call_data['block_num']} (older than {threshold} blocks)")
                to_delete.append(call_id)
        
        for call_id in to_delete:
            del self.pending_calls[call_id]
    
    @property
    def is_running(self):
        """
        Check if the transaction monitor is running.
        
        Returns:
            bool: True if running, False otherwise
        """
        return self._is_running 