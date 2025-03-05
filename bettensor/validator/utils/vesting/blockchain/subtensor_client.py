"""
Subtensor blockchain client for the vesting system.

This module provides a unified interface for interacting with the Subtensor
blockchain for stake-related operations and monitoring.
"""

import logging
from datetime import datetime, timedelta
from typing import Dict, List, Set, Tuple, Optional, Any, Union
import json

import bittensor as bt
from bittensor.utils.balance import Balance
from substrateinterface import SubstrateInterface
from substrateinterface.exceptions import SubstrateRequestException

logger = logging.getLogger(__name__)


class SubtensorClient:
    """
    Unified client for Subtensor blockchain interactions.
    
    This class provides methods for:
    1. Connecting to the Bittensor blockchain
    2. Querying stake information
    3. Monitoring transactions
    4. Tracking epoch boundaries
    """
    
    def __init__(
        self,
        subnet_id: int,
        network: str = "finney",
        query_interval_seconds: int = 300,
    ):
        """
        Initialize the Subtensor client.
        
        Args:
            subnet_id: The subnet ID to track
            network: The Bittensor network to connect to (default: finney)
            query_interval_seconds: Interval between queries in seconds
        """
        self.subnet_id = subnet_id
        self.network = network
        self.query_interval = query_interval_seconds
        
        # State variables
        self.subtensor = None
        self._prev_stake_dict = {}  # hotkey -> {coldkey -> stake}
        self._last_check_block = 0
        self._last_epoch_block = 0
        self._is_connected = False
        
        # Connect to the network
        self.subtensor = bt.subtensor(network=self.network)
        
        # Cache for dynamic info
        self._dynamic_info = None
        self._dynamic_info_last_updated = None
        self._dynamic_info_ttl = 300  # 5 minutes
    
    async def connect(self) -> bool:
        """
        Connect to the Bittensor blockchain.
        
        Returns:
            bool: True if connection successful, False otherwise
        """
        try:
            # Initialize Subtensor connection
            self.subtensor = bt.subtensor(network=self.network)
            
            # Get current block as starting point
            current_block = self.subtensor.get_current_block()
            self._last_check_block = current_block
            
            # Get current epoch block
            subnet_info = self.subtensor.subnet(self.subnet_id)
            self._last_epoch_block = subnet_info.last_step
            
            logger.info(f"Connected to Bittensor network: {self.network}")
            logger.info(f"Current block: {current_block}")
            logger.info(f"Last epoch block: {self._last_epoch_block}")
            
            self._is_connected = True
            return True
            
        except Exception as e:
            logger.error(f"Failed to connect to Bittensor: {e}")
            self._is_connected = False
            return False
    
    async def get_stake_dict(self) -> Dict[str, Dict[str, float]]:
        """
        Get current stake dictionary for all neurons in the subnet.
        
        Returns:
            Dict mapping hotkeys to Dict of coldkeys and their stake amounts
        """
        if not self._is_connected:
            await self.connect()
            
        try:
            # Get metagraph
            metagraph = self.subtensor.metagraph(self.subnet_id)
            metagraph.sync()
            
            # Build stake dictionary
            stake_dict = {}
            for neuron in metagraph.neurons:
                if neuron.hotkey:
                    hotkey = neuron.hotkey
                    stake_dict[hotkey] = {}
                    
                    for coldkey, stake in neuron.stake_dict.items():
                        stake_dict[hotkey][coldkey] = float(stake)
            
            return stake_dict
            
        except Exception as e:
            logger.error(f"Error getting stake dict: {e}")
            return {}
    
    async def detect_stake_changes(self) -> Dict[str, List[Dict[str, Any]]]:
        """
        Detect changes in stake balances since the last check.
        
        Returns:
            Dict mapping event types to lists of events
            {
                "add_stake": [...],
                "remove_stake": [...],
                "move_stake": [...]
            }
        """
        events = {
            "add_stake": [],
            "remove_stake": [],
            "move_stake": []
        }
        
        try:
            # Get current stake dict
            current_stake_dict = await self.get_stake_dict()
            
            # First run, just store the current state
            if not self._prev_stake_dict:
                self._prev_stake_dict = current_stake_dict
                return events
            
            # Detect stake changes by comparing current and previous state
            for hotkey, coldkeys in current_stake_dict.items():
                # New hotkey appeared
                if hotkey not in self._prev_stake_dict:
                    for coldkey, stake in coldkeys.items():
                        if stake > 0:
                            events["add_stake"].append({
                                "hotkey": hotkey,
                                "coldkey": coldkey,
                                "amount": stake,
                                "timestamp": datetime.utcnow()
                            })
                    continue
                
                # Check for changes in existing hotkey's stake
                for coldkey, stake in coldkeys.items():
                    prev_stake = self._prev_stake_dict[hotkey].get(coldkey, 0)
                    
                    # Stake increased
                    if stake > prev_stake:
                        events["add_stake"].append({
                            "hotkey": hotkey,
                            "coldkey": coldkey,
                            "amount": stake - prev_stake,
                            "timestamp": datetime.utcnow()
                        })
                    
                    # Stake decreased
                    elif stake < prev_stake:
                        events["remove_stake"].append({
                            "hotkey": hotkey,
                            "coldkey": coldkey,
                            "amount": prev_stake - stake,
                            "timestamp": datetime.utcnow()
                        })
            
            # Check for disappeared stake (potential move stake or full unstake)
            for hotkey, coldkeys in self._prev_stake_dict.items():
                if hotkey not in current_stake_dict:
                    # Hotkey disappeared completely
                    for coldkey, stake in coldkeys.items():
                        events["remove_stake"].append({
                            "hotkey": hotkey,
                            "coldkey": coldkey,
                            "amount": stake,
                            "timestamp": datetime.utcnow()
                        })
                else:
                    # Check for coldkeys that disappeared
                    for coldkey, stake in coldkeys.items():
                        if coldkey not in current_stake_dict[hotkey]:
                            events["remove_stake"].append({
                                "hotkey": hotkey,
                                "coldkey": coldkey,
                                "amount": stake,
                                "timestamp": datetime.utcnow()
                            })
            
            # Update previous stake dict
            self._prev_stake_dict = current_stake_dict
            
            return events
            
        except Exception as e:
            logger.error(f"Error detecting stake changes: {e}")
            return events
    
    async def query_transactions(self, force_update: bool = False) -> List[Dict[str, Any]]:
        """
        Query recent stake transactions from the blockchain.
        
        Args:
            force_update: Force update from the beginning if True
            
        Returns:
            List of transaction dictionaries
        """
        if not self._is_connected:
            await self.connect()
            
        try:
            transactions = []
            
            # Get current block
            current_block = self.subtensor.get_current_block()
            
            # If forcing update, reset last check block
            if force_update:
                self._last_check_block = max(0, current_block - 10000)  # Limit to 10000 blocks back
            
            # If no blocks to check, return empty list
            if self._last_check_block >= current_block:
                return transactions
            
            # Query recent blocks
            start_block = self._last_check_block + 1
            end_block = current_block
            
            logger.info(f"Querying blocks {start_block} to {end_block}")
            
            # TODO: Implement the actual transaction querying logic using substrate interface
            # This would involve parsing add_stake, remove_stake, and move_stake extrinsics
            
            # Update last check block
            self._last_check_block = current_block
            
            return transactions
            
        except Exception as e:
            logger.error(f"Error querying transactions: {e}")
            return []
    
    async def check_epoch_boundary(self) -> bool:
        """
        Check if a new epoch has started since the last check.
        
        Returns:
            True if new epoch, False otherwise
        """
        if not self._is_connected:
            await self.connect()
            
        try:
            # Get current epoch block
            subnet_info = self.subtensor.subnet(self.subnet_id)
            current_epoch_block = subnet_info.last_step
            
            # Check if epoch changed
            if current_epoch_block > self._last_epoch_block:
                logger.info(f"New epoch detected: {self._last_epoch_block} -> {current_epoch_block}")
                self._last_epoch_block = current_epoch_block
                return True
            
            return False
            
        except Exception as e:
            logger.error(f"Error checking epoch boundary: {e}")
            return False
    
    async def get_epoch_progress(self) -> Tuple[int, int]:
        """
        Get current epoch progress.
        
        Returns:
            Tuple of (blocks_since_epoch, epoch_length)
        """
        if not self._is_connected:
            await self.connect()
            
        try:
            # Get current block
            current_block = self.subtensor.get_current_block()
            
            # Get subnet info
            subnet_info = self.subtensor.subnet(self.subnet_id)
            last_epoch_block = subnet_info.last_step
            blocks_per_epoch = subnet_info.blocks_per_epoch
            
            # Calculate blocks since epoch
            blocks_since_epoch = current_block - last_epoch_block
            
            return blocks_since_epoch, blocks_per_epoch
            
        except Exception as e:
            logger.error(f"Error getting epoch progress: {e}")
            return 0, 0
    
    def get_dynamic_info(self) -> 'bt.DynamicInfo':
        """
        Get the DynamicInfo for the subnet.
        
        Returns:
            DynamicInfo object for the current subnet
        """
        current_time = datetime.utcnow()
        
        # Check if we need to refresh the cached info
        if (self._dynamic_info is None or 
            self._dynamic_info_last_updated is None or
            (current_time - self._dynamic_info_last_updated).total_seconds() > self._dynamic_info_ttl):
            
            try:
                # Get fresh dynamic info from the subtensor
                self._dynamic_info = self.subtensor.get_dynamic_info(self.subnet_id)
                self._dynamic_info_last_updated = current_time
                
                logger.info(f"Updated dynamic info for subnet {self.subnet_id}: "
                           f"symbol={self._dynamic_info.symbol}, "
                           f"price={self._dynamic_info.price}")
            except Exception as e:
                logger.error(f"Error getting dynamic info: {e}")
                
                # If we have no cached info, create a default one
                if self._dynamic_info is None:
                    # Create a default dynamic info with a 1:1 conversion rate
                    # This will be updated on the next successful fetch
                    logger.warning("Using default 1:1 tao to alpha conversion rate")
                    self._dynamic_info = bt.DynamicInfo()
                    self._dynamic_info.netuid = self.subnet_id
                    self._dynamic_info.price = bt.Balance.from_tao(1.0)
        
        return self._dynamic_info 