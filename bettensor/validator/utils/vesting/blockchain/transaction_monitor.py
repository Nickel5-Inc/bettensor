"""
Transaction monitoring for the vesting system.

This module provides functionality to monitor the Bittensor blockchain for
stake-related transactions and events, distinguishing between manually added
stake and earned rewards.
"""

import logging
import asyncio
import threading
import queue
from datetime import datetime
from typing import Dict, List, Tuple, Optional, Any, Callable, Set

import bittensor as bt
from substrateinterface import SubstrateInterface
from bettensor.validator.utils.database import DatabaseManager
from bettensor.validator.utils.vesting.database.models import StakeTransaction

logger = logging.getLogger(__name__)


class TransactionMonitor:
    """
    Monitors the Bittensor blockchain for stake-related transactions.
    
    This class tracks add_stake and remove_stake events on the blockchain,
    allowing the vesting system to distinguish between manually added stake
    and earned rewards.
    """
    
    def __init__(
        self,
        subnet_id: int,
        network: str = "finney",
        callback: Optional[Callable[[Dict[str, Any]], None]] = None,
        event_queue_size: int = 1000,
        dedicated_thread: bool = False
    ):
        """
        Initialize the transaction monitor.
        
        Args:
            subnet_id: The subnet ID to track
            network: The Bittensor network to connect to (default: finney)
            callback: Optional callback function for transactions
            event_queue_size: Maximum size of the event queue for thread mode
            dedicated_thread: Whether to run in a dedicated thread
        """
        self.subnet_id = subnet_id
        self.network = network
        self.callback = callback
        self.event_queue_size = event_queue_size
        self.dedicated_thread = dedicated_thread
        
        # State variables
        self.substrate = None
        self.is_connected = False
        self.is_running = False
        self.should_exit = False
        self.monitor_task = None
        self.thread = None
        
        # Event queue for thread mode
        self.event_queue = queue.Queue(maxsize=event_queue_size) if dedicated_thread else None
        
        # Transaction tracking
        self.pending_calls = {}
        self.call_counter = 0
        self.tracked_hotkeys: Set[str] = set()
    
    async def connect(self) -> bool:
        """
        Connect to the Bittensor blockchain.
        
        Returns:
            True if connected successfully, False otherwise
        """
        try:
            # Create substrate interface
            self.substrate = SubstrateInterface(
                url=f"wss://entrypoint-{self.network}.opentensor.ai:443"
            )
            
            # Test connection
            block_hash = self.substrate.get_chain_head()
            if not block_hash:
                logger.error("Failed to get chain head")
                self.is_connected = False
                return False
                
            logger.info(f"Connected to Bittensor {self.network} network")
            self.is_connected = True
            return True
            
        except Exception as e:
            logger.error(f"Failed to connect to Bittensor: {e}")
            self.is_connected = False
            return False
    
    def add_hotkey_to_track(self, hotkey: str):
        """
        Add a hotkey to the tracking list.
        
        Args:
            hotkey: The hotkey to track
        """
        self.tracked_hotkeys.add(hotkey)
        logger.debug(f"Added hotkey to tracking: {hotkey}")
    
    def remove_hotkey_from_tracking(self, hotkey: str):
        """
        Remove a hotkey from the tracking list.
        
        Args:
            hotkey: The hotkey to remove from tracking
        """
        if hotkey in self.tracked_hotkeys:
            self.tracked_hotkeys.remove(hotkey)
            logger.debug(f"Removed hotkey from tracking: {hotkey}")
    
    async def start(self):
        """Start the transaction monitoring."""
        if self.is_running:
            logger.warning("Transaction monitor already running")
            return
            
        # Check if we should use dedicated thread
        if self.dedicated_thread:
            self._start_dedicated_thread()
            return
            
        # Connect to blockchain if needed
        if not self.is_connected:
            connected = await self.connect()
            if not connected:
                logger.error("Failed to connect to blockchain. Transaction monitoring not started.")
                return
        
        # Start monitoring
        self.is_running = True
        self.should_exit = False
        self.monitor_task = asyncio.create_task(self._monitor_loop())
        
        logger.info("Transaction monitoring started in current thread")
    
    def _start_dedicated_thread(self):
        """Start monitoring in a dedicated thread."""
        # Create and start thread
        self.thread = threading.Thread(
            target=self._thread_entry_point,
            daemon=True,
            name="TransactionMonitor"
        )
        self.thread.start()
        
        # Start event processor in current thread
        if self.callback is not None:
            asyncio.create_task(self._process_events_from_queue())
            
        logger.info("Transaction monitoring started in dedicated thread")
        self.is_running = True
        
    def _thread_entry_point(self):
        """Entry point for the dedicated thread."""
        try:
            # Create event loop for this thread
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            
            # Connect to blockchain 
            connect_result = loop.run_until_complete(self.connect())
            if not connect_result:
                logger.error("Failed to connect to blockchain in dedicated thread")
                return
                
            # Start monitor loop in this thread's event loop
            self.should_exit = False
            loop.run_until_complete(self._monitor_loop_thread_safe())
            
        except Exception as e:
            logger.error(f"Error in transaction monitor thread: {e}")
        finally:
            logger.info("Transaction monitor thread exiting")
            self.is_running = False
    
    async def _monitor_loop_thread_safe(self):
        """Monitoring loop that is safe to run in a dedicated thread."""
        try:
            last_block = None
            
            while not self.should_exit:
                try:
                    current_block = self.substrate.get_block_number(self.substrate.get_chain_head())
                    
                    # Skip if we've already processed this block
                    if last_block == current_block:
                        await asyncio.sleep(0.5)  # Wait for next block
                        continue
                        
                    # Get block hash
                    block_hash = self.substrate.get_block_hash(current_block)
                    if not block_hash:
                        logger.error(f"Failed to get block hash for block {current_block}")
                        await asyncio.sleep(0.5)
                        continue
                    
                    # Process extrinsics
                    block = self.substrate.get_block(block_hash=block_hash)
                    extrinsics = block.get('extrinsics', [])
                    
                    for ext in extrinsics:
                        await self._process_extrinsic_thread_safe(current_block, ext)
                        
                    # Process events
                    events_decoded = self.substrate.query("System", "Events", block_hash=block_hash)
                    await self._process_events_thread_safe(current_block, events_decoded)
                    
                    # Update last processed block
                    last_block = current_block
                    
                    # Purge old pending calls
                    self._purge_old_calls(current_block)
                    
                    # Sleep to avoid excessive CPU usage
                    await asyncio.sleep(0.5)
                    
                except Exception as e:
                    logger.error(f"Error in thread monitor loop: {e}")
                    await asyncio.sleep(1)  # Wait before retrying
                    
        except asyncio.CancelledError:
            logger.info("Thread monitor task cancelled")
        except Exception as e:
            logger.error(f"Thread monitoring failed: {e}")
    
    async def _process_extrinsic_thread_safe(self, block_num: int, extrinsic):
        """
        Process an extrinsic in thread-safe manner.
        
        Args:
            block_num: The block number
            extrinsic: The extrinsic to process
        """
        # Skip if no tracked hotkeys - optimization
        if not self.tracked_hotkeys:
            return
            
        try:
            # Use ext.value if available; otherwise assume ext is a dict
            ext_dict = extrinsic.value if hasattr(extrinsic, 'value') else extrinsic
            
            # Extract the coldkey from the extrinsic's "address" field
            coldkey = ext_dict.get('address')
            
            call_info = ext_dict.get('call', {})
            call_module = call_info.get('call_module')
            call_function = call_info.get('call_function')
            
            # Only process stake-related calls from SubtensorModule
            if call_module == 'SubtensorModule' and call_function in [
                'add_stake', 'remove_stake', 'add_stake_limit', 'remove_stake_limit'
            ]:
                params = call_info.get('call_args', [])
                hotkey = None
                call_amount = None
                netuid_param = None
                
                for param in params:
                    pname = param.get('name')
                    if pname == 'hotkey':
                        hotkey = param.get('value')
                    elif pname in ['amount', 'amount_staked', 'amount_unstaked']:
                        try:
                            call_amount = float(param.get('value', 0)) / 1e9
                        except Exception as conv_e:
                            logger.error(f"Error converting call amount from {param.get('value')}: {conv_e}")
                    elif pname == 'netuid':
                        try:
                            netuid_param = int(param.get('value', -1))
                        except Exception as conv_e:
                            logger.error(f"Error converting netuid: {conv_e}")
                
                # Only track if this is a hotkey we care about or if we're tracking all hotkeys
                if not self.tracked_hotkeys or hotkey in self.tracked_hotkeys:
                    # Record pending call
                    call_id = self.call_counter
                    self.call_counter += 1
                    self.pending_calls[call_id] = {
                        'block_num': block_num,
                        'call_function': call_function,
                        'coldkey': coldkey,
                        'hotkey': hotkey,
                        'netuid': netuid_param,
                        'call_amount': call_amount,
                        'final_amount': None,
                        'validated': False,
                        'timestamp': datetime.utcnow()
                    }
                    
                    logger.debug(
                        f"Recorded pending call {call_id} in block {block_num}: {call_function} - "
                        f"Coldkey: {coldkey}, Hotkey: {hotkey}, Call Amount: {call_amount}, "
                        f"Netuid: {netuid_param}"
                    )
                    
        except Exception as e:
            logger.error(f"Error processing extrinsic in block {block_num}: {e}")
    
    async def _process_events_thread_safe(self, block_num: int, events_decoded):
        """
        Process events in a thread-safe manner.
        
        Args:
            block_num: The block number
            events_decoded: The decoded events
        """
        # Skip if no tracked hotkeys - optimization
        if not self.tracked_hotkeys and not self.pending_calls:
            return
            
        try:
            for idx, event_record in enumerate(events_decoded.value):
                event = event_record.get('event', {})
                module_id = event.get('module_id')
                event_id = event.get('event_id')
                attributes = event.get('attributes')
                
                # We care about stake events from SubtensorModule
                if module_id == 'SubtensorModule' and event_id in ['StakeAdded', 'StakeRemoved']:
                    event_hotkey = None
                    event_netuid = None
                    event_amount = None
                    
                    # Assume attributes is a list/tuple with at least 5 items:
                    # [caller, hotkey, amount, new_total, netuid]
                    if isinstance(attributes, (list, tuple)) and len(attributes) >= 5:
                        event_hotkey = attributes[1]
                        try:
                            event_amount = float(attributes[2]) / 1e9
                        except Exception as conv_e:
                            logger.error(f"Error converting event amount: {conv_e}")
                        try:
                            event_netuid = int(attributes[4])
                        except Exception as conv_e:
                            logger.error(f"Error converting event netuid: {conv_e}")
                    
                    # Skip if this is not a hotkey we're tracking
                    if self.tracked_hotkeys and event_hotkey not in self.tracked_hotkeys:
                        continue
                    
                    logger.debug(
                        f"Event {idx} in block {block_num}: {event_id} - Hotkey: {event_hotkey}, "
                        f"Event Amount: {event_amount}, Netuid: {event_netuid}"
                    )
                    
                    # Match with pending calls
                    mapping = {
                        'add_stake': 'StakeAdded',
                        'add_stake_limit': 'StakeAdded',
                        'remove_stake': 'StakeRemoved',
                        'remove_stake_limit': 'StakeRemoved'
                    }
                    
                    matched = False
                    for call_id, call_data in list(self.pending_calls.items()):
                        expected_event_id = mapping.get(call_data['call_function'])
                        if expected_event_id != event_id:
                            continue
                        if call_data['hotkey'] != event_hotkey or call_data['netuid'] != event_netuid:
                            continue
                            
                        # We found a match!
                        matched = True
                        
                        # For stake orders, record final amount from the event; for unstake orders, use the call amount
                        if call_data['call_function'] in ['add_stake', 'add_stake_limit']:
                            final_amount = event_amount
                        else:
                            final_amount = call_data['call_amount']
                            
                        call_data['final_amount'] = final_amount
                        call_data['validated'] = True
                        
                        fee = None
                        if call_data['call_function'] in ['add_stake', 'add_stake_limit'] and call_data['call_amount'] is not None and event_amount is not None:
                            fee = call_data['call_amount'] - event_amount
                            
                        logger.info(
                            f"Validated call in block {block_num}: {event_id} - Coldkey: {call_data['coldkey']}, "
                            f"Hotkey: {event_hotkey}, Final Amount: {final_amount:.9f}, "
                            f"Fee: {fee:.9f if fee is not None else 'N/A'}, Netuid: {event_netuid}"
                        )
                        
                        # Create a transaction object
                        transaction = {
                            'block_num': block_num,
                            'transaction_type': call_data['call_function'],
                            'coldkey': call_data['coldkey'],
                            'hotkey': event_hotkey,
                            'netuid': event_netuid,
                            'amount': final_amount,
                            'fee': fee,
                            'timestamp': call_data['timestamp'],
                            'extrinsic_hash': f"block-{block_num}-{idx}"  # Use block number and event index as hash
                        }
                        
                        # Process the transaction
                        if self.dedicated_thread:
                            # Put in queue for processing in main thread
                            try:
                                self.event_queue.put(transaction, block=False)
                            except queue.Full:
                                logger.warning("Event queue full, dropping transaction")
                        elif self.callback:
                            # Process directly
                            self.callback(transaction)
                            
                        # Remove the pending call
                        del self.pending_calls[call_id]
                        
                    # If no match was found, this could be a result of epoch rewards or other automated process
                    if not matched and (not self.tracked_hotkeys or event_hotkey in self.tracked_hotkeys):
                        # This is a stake change that wasn't initiated by a manual extrinsic call
                        # These are typically from network rewards
                        transaction = {
                            'block_num': block_num,
                            'transaction_type': 'epoch_rewards' if event_id == 'StakeAdded' else 'epoch_penalty',
                            'coldkey': None,  # No coldkey for automated events
                            'hotkey': event_hotkey,
                            'netuid': event_netuid,
                            'amount': event_amount,
                            'fee': None,
                            'timestamp': datetime.utcnow(),
                            'extrinsic_hash': f"auto-{block_num}-{idx}"  # Use block number and event index as hash
                        }
                        
                        logger.info(
                            f"Detected automated stake change in block {block_num}: {event_id} - "
                            f"Hotkey: {event_hotkey}, Amount: {event_amount:.9f}, Netuid: {event_netuid}"
                        )
                        
                        # Process the transaction
                        if self.dedicated_thread:
                            # Put in queue for processing in main thread
                            try:
                                self.event_queue.put(transaction, block=False)
                            except queue.Full:
                                logger.warning("Event queue full, dropping transaction")
                        elif self.callback:
                            # Process directly
                            self.callback(transaction)
                            
        except Exception as e:
            logger.error(f"Error processing events in block {block_num}: {e}")
            
    async def _process_events_from_queue(self):
        """Process events from queue in the main thread."""
        while self.is_running:
            try:
                # Check if there are items in the queue
                if self.event_queue.empty():
                    await asyncio.sleep(0.1)
                    continue
                    
                # Get item from queue
                try:
                    transaction = self.event_queue.get_nowait()
                except queue.Empty:
                    continue
                
                # Process item if callback exists
                if self.callback:
                    await asyncio.coroutine(self.callback)(transaction)
                
                # Mark as done
                self.event_queue.task_done()
                
            except Exception as e:
                logger.error(f"Error processing event from queue: {e}")
                await asyncio.sleep(0.1)
    
    async def stop(self):
        """Stop the transaction monitoring."""
        if not self.is_running:
            logger.warning("Transaction monitor not running")
            return
            
        # Stop monitoring
        self.should_exit = True
        
        if self.dedicated_thread:
            # In thread mode, just set the flag and let the thread exit
            logger.info("Signaled thread to stop")
            # Wait for thread to finish (with timeout)
            if self.thread and self.thread.is_alive():
                self.thread.join(timeout=2.0)
                if self.thread.is_alive():
                    logger.warning("Thread did not exit within timeout")
                    
        else:
            # Stop the task in async mode
            if self.monitor_task:
                self.monitor_task.cancel()
                try:
                    await self.monitor_task
                except asyncio.CancelledError:
                    pass
                self.monitor_task = None
            
        self.is_running = False
        logger.info("Transaction monitoring stopped")
    
    async def _monitor_loop(self):
        """Background loop for monitoring transactions."""
        try:
            last_block = None
            
            # Initialize subscription
            while self.is_running and not self.should_exit:
                try:
                    current_block = self.substrate.get_block_number(self.substrate.get_chain_head())
                    
                    # Skip if we've already processed this block
                    if last_block == current_block:
                        await asyncio.sleep(0.5)  # Wait for next block
                        continue
                        
                    # Get block hash
                    block_hash = self.substrate.get_block_hash(current_block)
                    if not block_hash:
                        logger.error(f"Failed to get block hash for block {current_block}")
                        await asyncio.sleep(0.5)
                        continue
                    
                    # Process block
                    await self._process_block(current_block, block_hash)
                    
                    # Update last processed block
                    last_block = current_block
                    
                    # Purge old pending calls
                    self._purge_old_calls(current_block)
                    
                    # Sleep to avoid excessive CPU usage
                    await asyncio.sleep(0.5)
                    
                except Exception as e:
                    logger.error(f"Error in monitor loop: {e}")
                    await asyncio.sleep(1)  # Wait before retrying
                    
        except asyncio.CancelledError:
            logger.info("Transaction monitoring task cancelled")
        except Exception as e:
            logger.error(f"Transaction monitoring failed: {e}")
    
    async def _process_block(self, block_num: int, block_hash: str):
        """
        Process a block for extrinsics and events.
        
        Args:
            block_num: The block number
            block_hash: The block hash
        """
        try:
            # Get extrinsics
            block = self.substrate.get_block(block_hash=block_hash)
            extrinsics = block.get('extrinsics', [])
            
            # Process extrinsics to find stake-related calls
            for ext in extrinsics:
                await self._process_extrinsic(block_num, ext)
                
            # Process events to match with extrinsics
            events_decoded = self.substrate.query("System", "Events", block_hash=block_hash)
            await self._process_events(block_num, events_decoded)
            
        except Exception as e:
            logger.error(f"Error processing block {block_num}: {e}")
    
    async def _process_extrinsic(self, block_num: int, extrinsic):
        """
        Process an extrinsic to detect stake-related calls.
        
        Args:
            block_num: The block number
            extrinsic: The extrinsic to process
        """
        try:
            # Use ext.value if available; otherwise assume ext is a dict
            ext_dict = extrinsic.value if hasattr(extrinsic, 'value') else extrinsic
            
            # Extract the coldkey from the extrinsic's "address" field
            coldkey = ext_dict.get('address')
            
            call_info = ext_dict.get('call', {})
            call_module = call_info.get('call_module')
            call_function = call_info.get('call_function')
            
            # Only process stake-related calls from SubtensorModule
            if call_module == 'SubtensorModule' and call_function in [
                'add_stake', 'remove_stake', 'add_stake_limit', 'remove_stake_limit'
            ]:
                params = call_info.get('call_args', [])
                hotkey = None
                call_amount = None
                netuid_param = None
                
                for param in params:
                    pname = param.get('name')
                    if pname == 'hotkey':
                        hotkey = param.get('value')
                    elif pname in ['amount', 'amount_staked', 'amount_unstaked']:
                        try:
                            call_amount = float(param.get('value', 0)) / 1e9
                        except Exception as conv_e:
                            logger.error(f"Error converting call amount from {param.get('value')}: {conv_e}")
                    elif pname == 'netuid':
                        try:
                            netuid_param = int(param.get('value', -1))
                        except Exception as conv_e:
                            logger.error(f"Error converting netuid: {conv_e}")
                
                # Only track if this is a hotkey we care about or if we're tracking all hotkeys
                if not self.tracked_hotkeys or hotkey in self.tracked_hotkeys:
                    # Record pending call
                    call_id = self.call_counter
                    self.call_counter += 1
                    self.pending_calls[call_id] = {
                        'block_num': block_num,
                        'call_function': call_function,
                        'coldkey': coldkey,
                        'hotkey': hotkey,
                        'netuid': netuid_param,
                        'call_amount': call_amount,
                        'final_amount': None,
                        'validated': False,
                        'timestamp': datetime.utcnow()
                    }
                    
                    logger.debug(
                        f"Recorded pending call {call_id} in block {block_num}: {call_function} - "
                        f"Coldkey: {coldkey}, Hotkey: {hotkey}, Call Amount: {call_amount}, "
                        f"Netuid: {netuid_param}"
                    )
                    
        except Exception as e:
            logger.error(f"Error processing extrinsic in block {block_num}: {e}")
    
    async def _process_events(self, block_num: int, events_decoded):
        """
        Process events in a block and match them with pending extrinsic calls.
        
        Args:
            block_num: The block number
            events_decoded: The decoded events
        """
        try:
            for idx, event_record in enumerate(events_decoded.value):
                event = event_record.get('event', {})
                module_id = event.get('module_id')
                event_id = event.get('event_id')
                attributes = event.get('attributes')
                
                # We care about stake events from SubtensorModule
                if module_id == 'SubtensorModule' and event_id in ['StakeAdded', 'StakeRemoved']:
                    event_hotkey = None
                    event_netuid = None
                    event_amount = None
                    
                    # Assume attributes is a list/tuple with at least 5 items:
                    # [caller, hotkey, amount, new_total, netuid]
                    if isinstance(attributes, (list, tuple)) and len(attributes) >= 5:
                        event_hotkey = attributes[1]
                        try:
                            event_amount = float(attributes[2]) / 1e9
                        except Exception as conv_e:
                            logger.error(f"Error converting event amount: {conv_e}")
                        try:
                            event_netuid = int(attributes[4])
                        except Exception as conv_e:
                            logger.error(f"Error converting event netuid: {conv_e}")
                    
                    # Skip if this is not a hotkey we're tracking
                    if self.tracked_hotkeys and event_hotkey not in self.tracked_hotkeys:
                        continue
                    
                    logger.debug(
                        f"Event {idx} in block {block_num}: {event_id} - Hotkey: {event_hotkey}, "
                        f"Event Amount: {event_amount}, Netuid: {event_netuid}"
                    )
                    
                    # Match with pending calls
                    mapping = {
                        'add_stake': 'StakeAdded',
                        'add_stake_limit': 'StakeAdded',
                        'remove_stake': 'StakeRemoved',
                        'remove_stake_limit': 'StakeRemoved'
                    }
                    
                    matched = False
                    for call_id, call_data in list(self.pending_calls.items()):
                        expected_event_id = mapping.get(call_data['call_function'])
                        if expected_event_id != event_id:
                            continue
                        if call_data['hotkey'] != event_hotkey or call_data['netuid'] != event_netuid:
                            continue
                            
                        # We found a match!
                        matched = True
                        
                        # For stake orders, record final amount from the event; for unstake orders, use the call amount
                        if call_data['call_function'] in ['add_stake', 'add_stake_limit']:
                            final_amount = event_amount
                        else:
                            final_amount = call_data['call_amount']
                            
                        call_data['final_amount'] = final_amount
                        call_data['validated'] = True
                        
                        fee = None
                        if call_data['call_function'] in ['add_stake', 'add_stake_limit'] and call_data['call_amount'] is not None and event_amount is not None:
                            fee = call_data['call_amount'] - event_amount
                            
                        logger.info(
                            f"Validated call in block {block_num}: {event_id} - Coldkey: {call_data['coldkey']}, "
                            f"Hotkey: {event_hotkey}, Final Amount: {final_amount:.9f}, "
                            f"Fee: {fee:.9f if fee is not None else 'N/A'}, Netuid: {event_netuid}"
                        )
                        
                        # Create a transaction object
                        transaction = {
                            'block_num': block_num,
                            'transaction_type': call_data['call_function'],
                            'coldkey': call_data['coldkey'],
                            'hotkey': event_hotkey,
                            'netuid': event_netuid,
                            'amount': final_amount,
                            'fee': fee,
                            'timestamp': call_data['timestamp'],
                            'extrinsic_hash': f"block-{block_num}-{idx}"  # Use block number and event index as hash
                        }
                        
                        # Call the callback if it exists
                        if self.callback:
                            self.callback(transaction)
                            
                        # Remove the pending call
                        del self.pending_calls[call_id]
                        
                    # If no match was found, this could be a result of epoch rewards or other automated process
                    if not matched and (not self.tracked_hotkeys or event_hotkey in self.tracked_hotkeys):
                        # This is a stake change that wasn't initiated by a manual extrinsic call
                        # These are typically from network rewards
                        transaction = {
                            'block_num': block_num,
                            'transaction_type': 'epoch_rewards' if event_id == 'StakeAdded' else 'epoch_penalty',
                            'coldkey': None,  # No coldkey for automated events
                            'hotkey': event_hotkey,
                            'netuid': event_netuid,
                            'amount': event_amount,
                            'fee': None,
                            'timestamp': datetime.utcnow(),
                            'extrinsic_hash': f"auto-{block_num}-{idx}"  # Use block number and event index as hash
                        }
                        
                        logger.info(
                            f"Detected automated stake change in block {block_num}: {event_id} - "
                            f"Hotkey: {event_hotkey}, Amount: {event_amount:.9f}, Netuid: {event_netuid}"
                        )
                        
                        # Call the callback if it exists
                        if self.callback:
                            self.callback(transaction)
                            
        except Exception as e:
            logger.error(f"Error processing events in block {block_num}: {e}")
    
    def _purge_old_calls(self, current_block: int):
        """
        Purge pending extrinsic calls older than a threshold.
        
        Args:
            current_block: The current block number
        """
        to_delete = []
        for call_id, call_data in self.pending_calls.items():
            # Use different thresholds for different call types
            if call_data['call_function'] in ['add_stake_limit', 'remove_stake_limit']:
                threshold = 2160  # About 6 hours assuming 10s block time
            else:
                threshold = 20  # About 3 minutes
                
            if current_block - call_data['block_num'] >= threshold:
                logger.debug(f"Discarding pending call {call_id} from block {call_data['block_num']} (older than {threshold} blocks)")
                to_delete.append(call_id)
                
        for call_id in to_delete:
            del self.pending_calls[call_id]


class StakeChangeProcessor:
    """
    Processes stake change transactions and updates the database.
    
    This class handles the processing of stake change events,
    distinguishing between manually added stake and earned rewards,
    and updating the database accordingly.
    """
    
    def __init__(
        self, 
        db_manager: DatabaseManager,
        subnet_id: int,
        network: str = "finney",
        dedicated_thread: bool = True
    ):
        """
        Initialize the stake change processor.
        
        Args:
            db_manager: Database manager for persistent storage
            subnet_id: The subnet ID to track
            network: The Bittensor network to connect to (default: finney)
            dedicated_thread: Whether to run blockchain monitoring in a dedicated thread
        """
        self.db_manager = db_manager
        self.subnet_id = subnet_id
        self.network = network
        self.dedicated_thread = dedicated_thread
        
        # Create transaction monitor
        self.transaction_monitor = TransactionMonitor(
            subnet_id=subnet_id,
            network=network,
            callback=self.process_transaction,
            dedicated_thread=dedicated_thread
        )
    
    async def start(self):
        """Start the stake change processor."""
        await self.transaction_monitor.start()
    
    async def stop(self):
        """Stop the stake change processor."""
        await self.transaction_monitor.stop()
    
    def add_hotkey_to_track(self, hotkey: str):
        """
        Add a hotkey to the tracking list.
        
        Args:
            hotkey: The hotkey to track
        """
        self.transaction_monitor.add_hotkey_to_track(hotkey)
    
    def remove_hotkey_from_tracking(self, hotkey: str):
        """
        Remove a hotkey from the tracking list.
        
        Args:
            hotkey: The hotkey to remove from tracking
        """
        self.transaction_monitor.remove_hotkey_from_tracking(hotkey)
    
    async def process_transaction(self, transaction: Dict[str, Any]):
        """
        Process a stake change transaction.
        
        This method is called by the transaction monitor when a new
        transaction is detected. It records the transaction in the database
        and distinguishes between manual stake changes and earned rewards.
        
        Args:
            transaction: The transaction data
        """
        try:
            # Convert transaction type to standardized types
            transaction_type = transaction['transaction_type']
            if transaction_type in ['add_stake', 'add_stake_limit']:
                standardized_type = 'add_stake'
            elif transaction_type in ['remove_stake', 'remove_stake_limit']:
                standardized_type = 'remove_stake'
            elif transaction_type == 'epoch_rewards':
                standardized_type = 'reward'
            elif transaction_type == 'epoch_penalty':
                standardized_type = 'penalty'
            else:
                standardized_type = transaction_type
            
            # Create database record
            async with self.db_manager.async_session() as session:
                # Check if transaction already exists
                existing = await session.execute(
                    session.query(StakeTransaction)
                    .filter(StakeTransaction.extrinsic_hash == transaction['extrinsic_hash'])
                )
                if existing.first():
                    logger.debug(f"Transaction {transaction['extrinsic_hash']} already recorded")
                    return
                
                # Create new transaction record
                db_transaction = StakeTransaction(
                    extrinsic_hash=transaction['extrinsic_hash'],
                    block_number=transaction['block_num'],
                    block_timestamp=transaction['timestamp'],
                    hotkey=transaction['hotkey'],
                    coldkey=transaction['coldkey'] or '',  # Handle None values
                    transaction_type=standardized_type,
                    amount=transaction['amount'] or 0.0,  # Handle None values
                    source_hotkey=None  # Only used for move_stake
                )
                
                session.add(db_transaction)
                await session.commit()
                
                logger.info(
                    f"Recorded {standardized_type} transaction: "
                    f"Hotkey: {transaction['hotkey']}, "
                    f"Amount: {transaction['amount']:.9f if transaction['amount'] is not None else 'N/A'}, "
                    f"Block: {transaction['block_num']}"
                )
                
        except Exception as e:
            logger.error(f"Error processing transaction: {e}") 