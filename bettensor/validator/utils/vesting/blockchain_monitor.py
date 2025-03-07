"""
Blockchain monitoring module for the vesting system.

This module provides functionality to monitor the blockchain for stake changes,
track transactions, and detect epoch boundaries.
"""

import logging
import asyncio
import threading
import time
import pickle
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple, Any, Set, Union

import bittensor as bt
from bittensor.utils.balance import Balance
import numpy as np
from bettensor.validator.utils.database.database_manager import DatabaseManager
from substrateinterface import SubstrateInterface
from substrateinterface.exceptions import SubstrateRequestException

logger = logging.getLogger(__name__)

class BlockchainMonitor:
    """
    Monitor for blockchain stake changes and transactions.
    
    This class tracks:
    1. Manual stake transactions (add/remove)
    2. Epoch-based balance changes (rewards/emissions)
    3. Epoch boundaries for scheduling updates
    
    It uses caching to minimize RPC calls and provides methods to
    retrieve stake history for miners.
    
    The monitor can run in a background thread for improved performance.
    """
    
    def __init__(
        self,
        subtensor: 'bt.subtensor',
        subnet_id: int,
        db_manager: 'DatabaseManager',
        query_interval_seconds: int = 300,
        auto_start_thread: bool = False
    ):
        """
        Initialize the blockchain monitor.
        
        Args:
            subtensor: Initialized subtensor instance
            subnet_id: The subnet ID to monitor
            db_manager: Database manager for persistent storage
            query_interval_seconds: Interval between queries in seconds
            auto_start_thread: Whether to automatically start the background thread
        """
        self.subtensor = subtensor
        self.subnet_id = subnet_id
        self.db_manager = db_manager
        self.query_interval = query_interval_seconds
        
        # State variables
        self._prev_stake_dict = {}  # hotkey -> {coldkey -> stake}
        self._last_check_block = 0
        self._last_epoch_block = 0
        self._is_connected = False
        
        # Cache for dynamic info
        self._dynamic_info = None
        self._dynamic_info_last_updated = None
        self._dynamic_info_ttl = 300  # 5 minutes
        
        # Threading variables
        self._thread = None
        self._stop_event = threading.Event()
        self._is_running = False
        self._current_epoch = 0
        
        # Epoch boundary detection variables
        self._last_blocks_since_step = None
        self._pre_epoch_stakes = {}
        self._blocks_per_epoch = None
        self._substrate = None
        
        # Auto-start thread if requested
        if auto_start_thread:
            self.start_background_thread()
    
    async def initialize(self):
        """
        Initialize the blockchain monitor.
        
        This method initializes the blockchain monitor but no longer creates 
        database tables as this is now handled centrally by the DatabaseManager
        using schema definitions from database_schema.py.
        
        Returns:
            bool: True if initialization was successful
        """
        try:
            logger.info("Initializing blockchain monitor")
            
            # Load last processed blocks from database
            await self._load_last_processed_blocks()
            
            # Initialize epoch boundaries tracking
            await self._initialize_epoch_tracking()
            
            return True
        except Exception as e:
            logger.error(f"Failed to initialize blockchain monitor: {e}")
            return False
    
    def start_background_thread(self):
        """
        Start the blockchain monitor in a background thread.
        
        This method starts a background thread that periodically:
        1. Tracks manual transactions
        2. Tracks balance changes
        3. Checks for epoch boundaries
        
        Returns:
            bool: True if thread was started, False otherwise
        """
        if self._is_running:
            logger.warning("Blockchain monitor thread is already running")
            return False
        
        # Reset stop event
        self._stop_event.clear()
        
        # Create and start thread
        self._thread = threading.Thread(
            target=self._background_thread_loop,
            daemon=True
        )
        self._thread.start()
        
        self._is_running = True
        logger.info("Started blockchain monitor background thread")
        return True
    
    def stop_background_thread(self, timeout=10):
        """
        Stop the blockchain monitor background thread.
        
        Args:
            timeout: Maximum time to wait for thread to stop (seconds)
            
        Returns:
            bool: True if thread was stopped, False otherwise
        """
        if not self._is_running:
            logger.warning("Blockchain monitor thread is not running")
            return False
        
        # Signal thread to stop
        self._stop_event.set()
        
        # Wait for thread to stop
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=timeout)
            if self._thread.is_alive():
                logger.warning(f"Blockchain monitor thread did not stop within {timeout} seconds")
                return False
        
        self._is_running = False
        logger.info("Stopped blockchain monitor background thread")
        return True
    
    def _background_thread_loop(self):
        """
        Background thread loop for the blockchain monitor.
        
        This method runs in a separate thread and periodically:
        1. Tracks manual transactions
        2. Tracks balance changes
        3. Checks for epoch boundaries
        """
        logger.info("Blockchain monitor background thread started")
        
        # Create event loop for the thread
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        try:
            # Initialize if not already initialized
            if not self._is_connected:
                loop.run_until_complete(self.initialize())
            
            # Main loop
            start_time = time.time()
            last_check_time = 0
            check_interval = 5  # Check more frequently for epoch boundaries
            
            while not self._stop_event.is_set():
                try:
                    current_time = time.time()
                    
                    # Check for epoch boundary more frequently
                    if current_time - last_check_time >= check_interval:
                        # Get current block
                        if self._substrate:
                            block_hash = self._substrate.get_chain_head()
                            block_num = self._substrate.get_block_number(block_hash)
                        else:
                            block_num = self.subtensor.get_current_block()
                        
                        # Check for epoch boundary
                        epoch_changed = loop.run_until_complete(self._check_for_epoch_boundary(block_num))
                        if epoch_changed:
                            self._current_epoch += 1
                            logger.info(f"Epoch boundary detected, new epoch: {self._current_epoch}")
                            
                            # Track balance changes for the new epoch
                            loop.run_until_complete(self.track_balance_changes(self._current_epoch))
                        
                        # Track manual transactions periodically
                        if current_time - last_check_time >= self.query_interval:
                            loop.run_until_complete(self.track_manual_transactions())
                            
                        last_check_time = current_time
                    
                    # Log status periodically
                    elapsed = time.time() - start_time
                    if elapsed % 300 < check_interval:
                        hours = elapsed / 3600
                        logger.info(f"Monitor running for {hours:.1f} hours, detected {self._current_epoch} epochs")
                    
                    # Sleep briefly
                    time.sleep(1)
                        
                except Exception as e:
                    logger.error(f"Error in blockchain monitor background thread: {e}")
                    # Sleep for a shorter interval on error
                    time.sleep(10)
        
        finally:
            # Clean up
            loop.close()
            logger.info("Blockchain monitor background thread stopped")
    
    async def _check_for_epoch_boundary(self, block_num: int) -> bool:
        """
        Check if an epoch boundary has occurred by monitoring blocksSinceLastStep.
        
        This method uses the substrate interface to directly query the blockchain
        for the BlocksSinceLastStep value, which is more reliable for detecting
        epoch boundaries than checking the last_step value.
        
        Args:
            block_num: Current block number
            
        Returns:
            bool: True if epoch boundary detected, False otherwise
        """
        try:
            # If substrate is not available, fall back to subtensor
            if not self._substrate:
                subnet_info = self.subtensor.subnet(self.subnet_id)
                current_epoch_block = subnet_info.last_step
                
                # Check if epoch boundary crossed
                if current_epoch_block > self._last_epoch_block:
                    logger.info(f"Epoch boundary crossed: {self._last_epoch_block} -> {current_epoch_block}")
                    self._last_epoch_block = current_epoch_block
                    await self._save_last_processed_blocks()
                    return True
                
                return False
            
            # Use substrate to query BlocksSinceLastStep
            blocks_since_step = self._substrate.query(
                module='SubtensorModule',
                storage_function='BlocksSinceLastStep',
                params=[self.subnet_id]
            )
            
            if blocks_since_step is None:
                logger.warning(f"Failed to get BlocksSinceLastStep for subnet {self.subnet_id}")
                return False
                
            blocks_since_step = blocks_since_step.value
            
            if not isinstance(blocks_since_step, int):
                try:
                    blocks_since_step = int(blocks_since_step)
                except:
                    logger.error(f"Invalid BlocksSinceLastStep value: {blocks_since_step}")
                    return False
        
            logger.debug(f"Block {block_num}: BlocksSinceLastStep={blocks_since_step}/{self._blocks_per_epoch}")
                
            # Capture pre-epoch stakes when approaching epoch boundary
            if blocks_since_step == self._blocks_per_epoch - 1 or blocks_since_step == self._blocks_per_epoch:
                logger.info(f"At final block of epoch (blocks_since_step={blocks_since_step}), capturing pre-epoch stakes")
                self._pre_epoch_stakes = await self._get_stake_balances()
                await self._record_stake_snapshot(block_num, self._pre_epoch_stakes, is_pre_epoch=True)
                
            # Detect epoch boundary
            epoch_detected = False
            if self._last_blocks_since_step is not None:
                if blocks_since_step == 0 and self._last_blocks_since_step >= self._blocks_per_epoch - 5:
                    logger.info(f"EPOCH BOUNDARY DETECTED at block {block_num}: "
                               f"BlocksSinceLastStep reset from {self._last_blocks_since_step} to {blocks_since_step}")
                    
                    # Record epoch boundary
                    await self._record_epoch_boundary(block_num)
                    
                    # Get post-epoch stakes
                    logger.info(f"Capturing post-epoch stakes at block {block_num}")
                    post_epoch_stakes = await self._get_stake_balances()
                    await self._record_stake_snapshot(block_num, post_epoch_stakes, is_pre_epoch=False, epoch_block=block_num)
                    
                    # Calculate emissions
                    if self._pre_epoch_stakes:
                        logger.info(f"Calculating emissions between pre-epoch and post-epoch stakes")
                        await self._record_emissions(block_num, self._pre_epoch_stakes, post_epoch_stakes)
                    else:
                        logger.warning(f"No pre-epoch stakes available for emission calculation")
                    
                    # Update last epoch block
                    self._last_epoch_block = block_num
                    await self._save_last_processed_blocks()
                    
                    # Reset pre-epoch stakes
                    self._pre_epoch_stakes = {}
                    
                    epoch_detected = True
            
            self._last_blocks_since_step = blocks_since_step
            return epoch_detected
            
        except Exception as e:
            logger.error(f"Error checking for epoch boundary: {e}")
            return False
    
    async def _get_stake_balances(self) -> Dict[int, Dict[str, Any]]:
        """
        Get current stake balances for all neurons in the subnet.
        
        Returns:
            Dict[int, Dict[str, Any]]: Dictionary mapping UIDs to stake info
        """
        try:
            metagraph = self.subtensor.metagraph(self.subnet_id)
            balances = {}
            
            stakes = metagraph.S
            
            n_neurons = min(256, len(stakes))
            logger.debug(f"Processing stakes for {n_neurons} neurons")
            
            for uid in range(n_neurons):
                stake_value = float(stakes[uid])
                hotkey = metagraph.hotkeys[uid] if uid < len(metagraph.hotkeys) else None
                coldkey = metagraph.coldkeys[uid] if uid < len(metagraph.coldkeys) else None
                
                balances[uid] = {
                    'stake': stake_value,
                    'hotkey': hotkey,
                    'coldkey': coldkey
                }
            
            logger.info(f"Captured stake balances for {len(balances)} UIDs")
            return balances
        except Exception as e:
            logger.error(f"Error getting stake balances: {e}")
            return {}
    
    async def _record_stake_snapshot(self, block_num: int, stakes: Dict[int, Dict[str, Any]], 
                                    is_pre_epoch: bool = True, epoch_block: Optional[int] = None) -> bool:
        """
        Record a snapshot of stake balances to the database.
        
        Args:
            block_num: Current block number
            stakes: Dictionary of stake balances
            is_pre_epoch: Whether this is a pre-epoch snapshot
            epoch_block: Block number of the epoch boundary
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            timestamp = int(time.time())
            serialized_data = pickle.dumps(stakes)
            
            await self.db_manager.execute_query("""
                CREATE TABLE IF NOT EXISTS stake_snapshots (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    block_num INTEGER NOT NULL,
                    netuid INTEGER NOT NULL,
                    timestamp INTEGER NOT NULL,
                    is_pre_epoch BOOLEAN NOT NULL,
                    epoch_block INTEGER,
                    data BLOB NOT NULL
                )
            """)
            
            await self.db_manager.execute_query("""
                INSERT INTO stake_snapshots 
                (block_num, netuid, timestamp, is_pre_epoch, epoch_block, data) 
                VALUES (?, ?, ?, ?, ?, ?)
            """, (block_num, self.subnet_id, timestamp, is_pre_epoch, epoch_block, serialized_data))
            
            status = "pre-epoch" if is_pre_epoch else "post-epoch"
            logger.info(f"Recorded {status} stake snapshot at block {block_num}")
            return True
        except Exception as e:
            logger.error(f"Error recording stake snapshot: {e}")
            return False
    
    async def _record_epoch_boundary(self, block_num: int) -> bool:
        """
        Record an epoch boundary to the database.
        
        Args:
            block_num: Block number where the epoch boundary occurred
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            timestamp = int(time.time())
            
            await self.db_manager.execute_query("""
                CREATE TABLE IF NOT EXISTS epoch_boundaries (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    block_num INTEGER NOT NULL,
                    netuid INTEGER NOT NULL,
                    timestamp INTEGER NOT NULL
                )
            """)
            
            await self.db_manager.execute_query("""
                INSERT INTO epoch_boundaries (block_num, netuid, timestamp) 
                VALUES (?, ?, ?)
            """, (block_num, self.subnet_id, timestamp))
            
            logger.info(f"Recorded epoch boundary at block {block_num} to database")
            return True
        except Exception as e:
            logger.error(f"Error recording epoch boundary: {e}")
            return False
    
    async def _record_emissions(self, block_num: int, pre_stakes: Dict[int, Dict[str, Any]], 
                               post_stakes: Dict[int, Dict[str, Any]]) -> bool:
        """
        Calculate and record emissions by comparing pre and post epoch stakes.
        
        Args:
            block_num: Current block number
            pre_stakes: Pre-epoch stake balances
            post_stakes: Post-epoch stake balances
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            timestamp = int(time.time())
            
            await self.db_manager.execute_query("""
                CREATE TABLE IF NOT EXISTS stake_changes (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    block_num INTEGER NOT NULL,
                    uid INTEGER NOT NULL,
                    hotkey TEXT,
                    netuid INTEGER NOT NULL,
                    stake_before REAL,
                    stake_after REAL,
                    manual_add_stake REAL DEFAULT 0,
                    manual_remove_stake REAL DEFAULT 0,
                    true_emission REAL,
                    timestamp INTEGER NOT NULL
                )
            """)
            
            changes_count = 0
            total_emission = 0.0
            
            # Process all UIDs that exist in either pre or post epoch stakes
            all_uids = set(pre_stakes.keys()) | set(post_stakes.keys())
            logger.info(f"Processing stake changes for {len(all_uids)} UIDs")
            
            for uid in all_uids:
                pre_stake_data = pre_stakes.get(uid, {'stake': 0.0, 'hotkey': None})
                post_stake_data = post_stakes.get(uid, {'stake': 0.0, 'hotkey': None})
                
                stake_before = pre_stake_data['stake']
                stake_after = post_stake_data['stake']
                hotkey = pre_stake_data['hotkey'] or post_stake_data['hotkey']
                
                # Calculate stake change
                stake_change = stake_after - stake_before
                
                # For now, assume all changes are emissions
                # In a more complete implementation, we would subtract manual operations
                true_emission = stake_change
                
                if abs(stake_change) > 0.000001:
                    await self.db_manager.execute_query("""
                        INSERT INTO stake_changes
                        (block_num, uid, hotkey, netuid, stake_before, stake_after, 
                        manual_add_stake, manual_remove_stake, true_emission, timestamp)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (block_num, uid, hotkey, self.subnet_id, stake_before, stake_after, 
                         0.0, 0.0, true_emission, timestamp))
                    
                    changes_count += 1
                    total_emission += true_emission
                    
                    if true_emission > 0.1:
                        logger.info(f"UID {uid}: Emission of {true_emission:.6f}τ detected")
            
            if changes_count > 0:
                logger.info(f"Recorded {changes_count} stake changes, total emission: {total_emission:.6f}τ")
            return True
        except Exception as e:
            logger.error(f"Error recording emissions: {e}")
            return False
    
    @property
    def is_running(self):
        """
        Check if the blockchain monitor thread is running.
        
        Returns:
            bool: True if thread is running, False otherwise
        """
        return self._is_running
    
    @property
    def current_epoch(self):
        """
        Get the current epoch number.
        
        Returns:
            int: Current epoch number
        """
        return self._current_epoch
    
    async def _ensure_tables_exist(self):
        """
        DEPRECATED: Table creation is now handled centrally by DatabaseManager.
        
        This method is maintained for backward compatibility only and does nothing.
        Table schemas are defined in database_schema.py and created by DatabaseManager.
        """
        logger.debug("_ensure_tables_exist() is deprecated - tables are created centrally")
        return
    
    async def _load_last_processed_blocks(self):
        """
        Load the last processed blocks from the database.
        """
        try:
            # Load last check block
            result = await self.db_manager.fetch_one("""
                SELECT value FROM blockchain_state 
                WHERE key = 'last_check_block'
            """)
            if result:
                self._last_check_block = int(result['value'])
            
            # Load last epoch block
            result = await self.db_manager.fetch_one("""
                SELECT value FROM blockchain_state 
                WHERE key = 'last_epoch_block'
            """)
            if result:
                self._last_epoch_block = int(result['value'])
                
        except Exception as e:
            logger.error(f"Error loading last processed blocks: {e}")
    
    async def _save_last_processed_blocks(self):
        """
        Save the last processed blocks to the database.
        """
        try:
            current_time = datetime.now(timezone.utc)
            
            # Save last check block
            await self.db_manager.execute_query("""
                INSERT OR REPLACE INTO blockchain_state (key, value, updated_at)
                VALUES (?, ?, ?)
            """, ('last_check_block', str(self._last_check_block), current_time))
            
            # Save last epoch block
            await self.db_manager.execute_query("""
                INSERT OR REPLACE INTO blockchain_state (key, value, updated_at)
                VALUES (?, ?, ?)
            """, ('last_epoch_block', str(self._last_epoch_block), current_time))
                
        except Exception as e:
            logger.error(f"Error saving last processed blocks: {e}")
    
    async def track_manual_transactions(self, start_block: Optional[int] = None) -> int:
        """
        Track manual stake transactions between the last checked block and the current block.
        
        This function queries the blockchain for stake-related transactions and
        records them in the database. It ensures we capture all transactions involving
        our subnet of interest (subnet_id), looking for any occurrence of the subnet ID
        in transaction data.
        
        Args:
            start_block: Optional starting block. If None, uses last checked block.
            
        Returns:
            int: Number of transactions found
        """
        try:
            # Get start_block
            if start_block is None:
                start_block = self._last_check_block
            
            # Get current block
            current_block = self.subtensor.get_current_block()
            
            # Limit block range to avoid huge queries
            max_blocks = 100
            end_block = min(current_block, start_block + max_blocks)
            
            # Skip if no new blocks
            if end_block <= start_block:
                logger.debug(f"No new blocks to check (start={start_block}, end={end_block})")
                return 0
            
            logger.info(f"Checking for manual transactions from block {start_block} to {end_block}")
            
            # Query transactions
            transactions = await self._query_transactions(start_block, end_block)
            
            # Additional deep check for our subnet
            subnet_transactions = []
            for tx in transactions:
                if 'netuid' in tx and tx['netuid'] == self.subnet_id:
                    subnet_transactions.append(tx)
                elif 'origin_netuid' in tx and tx['origin_netuid'] == self.subnet_id:
                    subnet_transactions.append(tx)
                elif 'destination_netuid' in tx and tx['destination_netuid'] == self.subnet_id:
                    subnet_transactions.append(tx)
                else:
                    # Deep check for subnet ID in any field
                    tx_str = str(tx)
                    if f"'netuid': {self.subnet_id}" in tx_str or f"'netuid': '{self.subnet_id}'" in tx_str:
                        logger.info(f"Found subnet {self.subnet_id} in transaction data")
                        subnet_transactions.append(tx)
            
            # Store transactions
            count = 0
            if subnet_transactions:
                for tx in subnet_transactions:
                    # Insert into database
                    await self.db_manager.execute_query("""
                        INSERT INTO stake_transactions (
                            block_number, timestamp, transaction_type, hotkey, coldkey, amount, tx_hash
                        ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """, (
                        tx['block_number'],
                        tx['timestamp'],
                        tx['transaction_type'],
                        tx['hotkey'],
                        tx.get('coldkey', 'unknown'),
                        tx['amount'],
                        tx.get('tx_hash', None)
                    ))
                    count += 1
                    
                logger.info(f"Recorded {count} manual transactions in blocks {start_block}-{end_block}")
            
            # Update last_check_block
            self._last_check_block = end_block
            await self._save_last_processed_blocks()
            
            return count
        except Exception as e:
            logger.error(f"Error tracking manual transactions: {e}")
            return 0
    
    async def _query_transactions(self, start_block: int, end_block: int) -> List[Dict[str, Any]]:
        """
        Query for stake transactions in a block range.
        
        This method performs a comprehensive search for all transactions that might
        involve our subnet_id, checking multiple attributes and fields for the subnet ID.
        
        Args:
            start_block: Starting block number
            end_block: Ending block number
            
        Returns:
            List[Dict[str, Any]]: List of transaction dictionaries
        """
        transactions = []
        
        try:
            logger.info(f"Querying blocks {start_block} to {end_block} for subnet {self.subnet_id} stake transactions")
            
            # Use direct substrate queries if available
            if self._substrate:
                try:
                    # Query batch of blocks
                    for block_num in range(start_block, end_block + 1):
                        # Get block hash
                        block_hash = self._substrate.get_block_hash(block_num)
                        
                        # Get block with timestamp
                        block = self._substrate.get_block(block_hash)
                        block_timestamp = self._substrate.query(
                            module='Timestamp',
                            storage_function='Now',
                            block_hash=block_hash
                        ).value
                        
                        # Get extrinsics (transactions)
                        extrinsics = block.get('extrinsics', [])
                        
                        # Get events
                        events = self._substrate.get_events(block_hash)
                        
                        # Process extrinsics and events
                        for ext in extrinsics:
                            # Only process SubtensorModule calls
                            if ext.get('call', {}).get('module', {}).get('name') != 'SubtensorModule':
                                continue
                                
                            call = ext.get('call', {})
                            call_function = call.get('call_function', {}).get('name')
                            
                            # Initialize with default values
                            tx = {
                                'block_number': block_num,
                                'timestamp': datetime.fromtimestamp(block_timestamp / 1000, timezone.utc),
                                'transaction_type': 'unknown',
                                'hotkey': None,
                                'coldkey': ext.get('address'),
                                'amount': 0,
                                'tx_hash': None,
                                'netuid': None,
                                'origin_netuid': None,
                                'destination_netuid': None
                            }
                            
                            # Check for stake-related calls
                            if call_function in ['add_stake', 'remove_stake']:
                                # Extract parameters
                                params = {p.get('name'): p.get('value') for p in call.get('call_args', [])}
                                
                                tx['transaction_type'] = call_function
                                tx['hotkey'] = params.get('hotkey')
                                
                                # Check for netuid in different possible fields
                                netuid_found = False
                                for netuid_field in ['netuid', 'origin_netuid', 'destination_netuid']:
                                    if netuid_field in params:
                                        try:
                                            tx[netuid_field] = int(params[netuid_field])
                                            if tx[netuid_field] == self.subnet_id:
                                                netuid_found = True
                                        except (ValueError, TypeError):
                                            logger.warning(f"Could not convert {netuid_field} to int: {params[netuid_field]}")
                                
                                # Deep check all parameters for subnet ID
                                if not netuid_found:
                                    for param_name, param_value in params.items():
                                        if str(param_value) == str(self.subnet_id):
                                            logger.info(f"Found subnet {self.subnet_id} in parameter {param_name}")
                                            netuid_found = True
                                            tx['netuid'] = self.subnet_id
                                            break
                                
                                # Skip if no involvement with our subnet
                                if not netuid_found and not self._check_raw_data_for_subnet(call):
                                    continue
                                
                                # Extract amount
                                if 'amount' in params:
                                    try:
                                        tx['amount'] = float(params['amount']) / 1e9
                                    except (ValueError, TypeError):
                                        logger.warning(f"Could not convert amount to float: {params['amount']}")
                                
                                # Add to transactions
                                transactions.append(tx)
                            
                            # Check for move_stake, transfer_stake, etc.
                            elif call_function in ['move_stake', 'transfer_stake', 'swap_stake']:
                                # Extract parameters
                                params = {p.get('name'): p.get('value') for p in call.get('call_args', [])}
                                
                                tx['transaction_type'] = call_function
                                
                                # Get relevant fields based on call type
                                if call_function == 'move_stake':
                                    tx['hotkey'] = params.get('origin_hotkey')
                                    tx['destination_hotkey'] = params.get('destination_hotkey')
                                elif call_function == 'transfer_stake':
                                    tx['hotkey'] = params.get('hotkey')
                                    tx['destination_coldkey'] = params.get('destination_coldkey')
                                elif call_function == 'swap_stake':
                                    tx['hotkey'] = params.get('hotkey')
                                
                                # Check for netuid in different possible fields
                                netuid_found = False
                                for netuid_field in ['netuid', 'origin_netuid', 'destination_netuid']:
                                    if netuid_field in params:
                                        try:
                                            tx[netuid_field] = int(params[netuid_field])
                                            if tx[netuid_field] == self.subnet_id:
                                                netuid_found = True
                                        except (ValueError, TypeError):
                                            logger.warning(f"Could not convert {netuid_field} to int: {params[netuid_field]}")
                                
                                # Deep check all parameters for subnet ID
                                if not netuid_found:
                                    for param_name, param_value in params.items():
                                        if str(param_value) == str(self.subnet_id):
                                            logger.info(f"Found subnet {self.subnet_id} in parameter {param_name}")
                                            netuid_found = True
                                            # Set a default field if we found subnet elsewhere
                                            if 'netuid' not in tx or tx['netuid'] is None:
                                                tx['netuid'] = self.subnet_id
                                            break
                                
                                # Skip if no involvement with our subnet
                                if not netuid_found and not self._check_raw_data_for_subnet(call):
                                    continue
                                
                                # Extract amount
                                amount_field = next((f for f in ['amount', 'alpha_amount'] if f in params), None)
                                if amount_field:
                                    try:
                                        tx['amount'] = float(params[amount_field]) / 1e9
                                    except (ValueError, TypeError):
                                        logger.warning(f"Could not convert {amount_field} to float: {params[amount_field]}")
                                
                                # Add to transactions
                                transactions.append(tx)
                except Exception as e:
                    logger.error(f"Error processing blocks with substrate API: {e}")
                    # Fall back to subtensor
            
            # If we couldn't query directly or there was an error, use subtensor
            if not transactions:
                logger.warning("Using subtensor fallback for transaction query")
                # Add fallback implementation here
        
        except Exception as e:
            logger.error(f"Error querying transactions: {e}")
        
        return transactions
        
    def _check_raw_data_for_subnet(self, data: Dict) -> bool:
        """
        Performs a deep check in raw data for the subnet ID.
        
        Args:
            data: Dictionary of data to check
            
        Returns:
            bool: True if subnet ID is found, False otherwise
        """
        # Convert to string for simple text search
        data_str = str(data)
        subnet_patterns = [
            f"'netuid': {self.subnet_id}",
            f"'netuid': '{self.subnet_id}'",
            f"'origin_netuid': {self.subnet_id}",
            f"'origin_netuid': '{self.subnet_id}'",
            f"'destination_netuid': {self.subnet_id}",
            f"'destination_netuid': '{self.subnet_id}'"
        ]
        
        for pattern in subnet_patterns:
            if pattern in data_str:
                logger.info(f"Found subnet {self.subnet_id} in raw transaction data")
                return True
                
        return False
    
    async def track_balance_changes(self, epoch: int) -> Dict[str, float]:
        """
        Track balance changes between epochs.
        
        This method compares the current stake dictionary with the previous one
        and records any changes in the database.
        
        Args:
            epoch: Current epoch number
            
        Returns:
            Dict[str, float]: Dictionary of hotkeys and their stake changes
        """
        try:
            # Get current block
            current_block = self.subtensor.get_current_block()
            
            # Get current stake dictionary
            current_stakes = await self._get_current_stakes()
            
            # Compare with previous stake dictionary
            changes = {}
            timestamp = datetime.now(timezone.utc)
            
            for hotkey, stake_info in current_stakes.items():
                coldkey = stake_info.get('coldkey', 'unknown')
                current_stake = stake_info.get('stake', 0.0)
                
                # Get previous stake
                prev_stake = 0.0
                if hotkey in self._prev_stake_dict:
                    prev_stake = self._prev_stake_dict[hotkey].get('stake', 0.0)
                
                # Calculate change
                change = current_stake - prev_stake
                
                # Record significant changes (filter out tiny fluctuations)
                if abs(change) > 0.0001:
                    changes[hotkey] = change
                    
                    # Record in database
                    await self.db_manager.execute_query("""
                        INSERT INTO stake_balance_changes 
                        (epoch, block_number, timestamp, hotkey, coldkey, previous_stake, current_stake, change)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        epoch,
                        current_block,
                        timestamp,
                        hotkey,
                        coldkey,
                        prev_stake,
                        current_stake,
                        change
                    ))
            
            # Update previous stake dictionary
            self._prev_stake_dict = current_stakes
            
            # Update last check block
            self._last_check_block = current_block
            await self._save_last_processed_blocks()
            
            logger.info(f"Tracked {len(changes)} stake balance changes for epoch {epoch}")
            return changes
            
        except Exception as e:
            logger.error(f"Error tracking balance changes: {e}")
            return {}
    
    async def get_epoch_progress(self) -> Tuple[int, int]:
        """
        Get the current epoch progress.
        
        Returns:
            Tuple[int, int]: (blocks_since_last_epoch, blocks_per_epoch)
        """
        try:
            # Get subnet info
            subnet_info = self.subtensor.subnet(self.subnet_id)
            current_epoch_block = subnet_info.last_step
            blocks_per_epoch = subnet_info.tempo
            
            # Get current block
            current_block = self.subtensor.get_current_block()
            
            # Calculate blocks since last epoch
            blocks_since_last_epoch = current_block - current_epoch_block
            
            return blocks_since_last_epoch, blocks_per_epoch
            
        except Exception as e:
            logger.error(f"Error getting epoch progress: {e}")
            return 0, 0
    
    async def _get_current_stakes(self) -> Dict[str, Dict[str, Any]]:
        """
        Get the current stakes for all neurons in the subnet.
        
        Returns:
            Dict[str, Dict[str, Any]]: Dictionary of hotkeys and their stake info
        """
        try:
            # Get metagraph
            metagraph = self.subtensor.metagraph(self.subnet_id)
            
            # Build stake dictionary
            stake_dict = {}
            for uid, hotkey in enumerate(metagraph.hotkeys):
                if hotkey:
                    coldkey = metagraph.coldkeys[uid] if uid < len(metagraph.coldkeys) else "unknown"
                    stake = float(metagraph.stake[uid]) if uid < len(metagraph.stake) else 0.0
                    
                    stake_dict[hotkey] = {
                        'coldkey': coldkey,
                        'stake': stake,
                        'uid': uid
                    }
            
            return stake_dict
            
        except Exception as e:
            logger.error(f"Error getting current stakes: {e}")
            return {}
    
    async def _get_previous_stakes(self) -> Dict[str, Dict[str, Any]]:
        """
        Get the previous stakes from the database.
        
        Returns:
            Dict[str, Dict[str, Any]]: Dictionary of hotkeys and their stake info
        """
        return self._prev_stake_dict
    
    async def _get_coldkey_for_hotkey(self, hotkey: str) -> str:
        """
        Get the coldkey associated with a hotkey.
        
        Args:
            hotkey: Hotkey to look up
            
        Returns:
            str: Associated coldkey or "unknown"
        """
        try:
            # Check cache first
            if hotkey in self._prev_stake_dict:
                return self._prev_stake_dict[hotkey].get('coldkey', 'unknown')
            
            # Query metagraph
            metagraph = self.subtensor.metagraph(self.subnet_id)
            
            for uid, hk in enumerate(metagraph.hotkeys):
                if hk == hotkey and uid < len(metagraph.coldkeys):
                    return metagraph.coldkeys[uid]
            
            return "unknown"
            
        except Exception as e:
            logger.error(f"Error getting coldkey for hotkey {hotkey}: {e}")
            return "unknown"
    
    async def get_stake_history(
        self, 
        hotkey: str, 
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None
    ) -> List[Dict]:
        """
        Get the stake history for a hotkey.
        
        Args:
            hotkey: Hotkey to get history for
            start_time: Optional start time filter
            end_time: Optional end time filter
            
        Returns:
            List[Dict]: List of stake history records
        """
        try:
            # Build query
            query = """
                SELECT * FROM (
                    SELECT 
                        block_number, 
                        timestamp, 
                        'transaction' as record_type,
                        transaction_type as change_type, 
                        amount as change
                    FROM stake_transactions 
                    WHERE hotkey = ?
                    
                    UNION
                    
                    SELECT 
                        block_number, 
                        timestamp, 
                        'balance_change' as record_type,
                        CASE 
                            WHEN change > 0 THEN 'reward' 
                            ELSE 'penalty' 
                        END as change_type,
                        change
                    FROM stake_balance_changes 
                    WHERE hotkey = ?
                )
                ORDER BY timestamp
            """
            params = [hotkey, hotkey]
            
            # Add time filters if provided
            if start_time or end_time:
                query = query.replace("ORDER BY timestamp", "WHERE 1=1")
                
                if start_time:
                    query += " AND timestamp >= ?"
                    params.append(start_time)
                
                if end_time:
                    query += " AND timestamp <= ?"
                    params.append(end_time)
                
                query += " ORDER BY timestamp"
            
            # Execute query
            results = await self.db_manager.fetch_all(query, params)
            
            # Convert to list of dicts
            history = []
            for row in results:
                history.append(dict(row))
            
            return history
            
        except Exception as e:
            logger.error(f"Error getting stake history for {hotkey}: {e}")
            return []
            
    def get_dynamic_info(self) -> 'bt.DynamicInfo':
        """
        Get the dynamic info for the network with caching.
        
        Returns:
            bt.DynamicInfo: Dynamic info for the network
        """
        current_time = datetime.now(timezone.utc)
        
        # Check if cache is valid
        if (self._dynamic_info is not None and 
            self._dynamic_info_last_updated is not None and
            (current_time - self._dynamic_info_last_updated).total_seconds() < self._dynamic_info_ttl):
            return self._dynamic_info
        
        # Update cache
        try:
            self._dynamic_info = self.subtensor.get_dynamic_info()
            self._dynamic_info_last_updated = current_time
            return self._dynamic_info
        except Exception as e:
            logger.error(f"Error getting dynamic info: {e}")
            if self._dynamic_info is not None:
                return self._dynamic_info
            else:
                raise 