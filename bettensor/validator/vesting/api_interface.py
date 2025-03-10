"""
Vesting System API Interface.

This module provides an interface for exposing vesting system data to external web services.
It handles formatting, serialization, and transmission of vesting metrics, stake data,
transaction history, and other related information.
"""

import logging
import asyncio
import json
import time
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Any, Optional, Union, Tuple
import aiohttp
import numpy as np

import bittensor as bt
from bettensor.validator.database.database_manager import DatabaseManager
from bettensor.validator.vesting.system import VestingSystem

logger = logging.getLogger(__name__)

class VestingAPIInterface:
    """
    Interface for exporting vesting system data to web APIs.
    
    This class provides methods to collect, format, and push vesting-related data
    to external web services. It supports both regular polling and webhook-style 
    push notifications.
    """
    
    def __init__(
        self,
        vesting_system: VestingSystem,
        api_base_url: str,
        api_key: Optional[str] = None,
        poll_interval_seconds: int = 300,
        push_enabled: bool = False,
        export_detailed_transactions: bool = True,
        max_batch_size: int = 1000
    ):
        """
        Initialize the vesting API interface.
        
        Args:
            vesting_system: The vesting system instance
            api_base_url: Base URL for the web API
            api_key: Optional API key for authentication
            poll_interval_seconds: How often to poll for updates (if using background thread)
            push_enabled: Whether to automatically push updates
            export_detailed_transactions: Whether to include detailed transaction data
            max_batch_size: Maximum number of records to send in a single batch
        """
        self.vesting_system = vesting_system
        self.api_base_url = api_base_url.rstrip('/')
        self.api_key = api_key
        self.poll_interval = poll_interval_seconds
        self.push_enabled = push_enabled
        self.export_detailed_transactions = export_detailed_transactions
        self.max_batch_size = max_batch_size
        
        # State tracking
        self.is_running = False
        self.task = None
        self.session = None
        self.last_sync_time = None
        
        # Cache of last processed IDs to avoid duplicate sends
        self._last_processed = {
            'transactions': 0,
            'stake_changes': 0,
            'epochs': 0
        }
    
    async def initialize(self):
        """
        Initialize the API interface.
        
        This method:
        1. Creates an HTTP client session
        2. Tests connectivity to the API
        3. Initializes state tracking
        
        Returns:
            bool: True if initialization was successful
        """
        try:
            logger.info(f"Initializing vesting API interface with endpoint {self.api_base_url}")
            
            # Create HTTP session
            self.session = aiohttp.ClientSession(
                headers=self._get_auth_headers()
            )
            
            # Test API connectivity
            connected = await self._test_connection()
            if not connected:
                logger.error("Failed to connect to vesting API endpoint")
                return False
            
            # Initialize state tracking
            await self._load_last_processed_ids()
            
            logger.info("Vesting API interface initialized successfully")
            return True
        except Exception as e:
            logger.error(f"Failed to initialize vesting API interface: {e}")
            return False
    
    async def start(self):
        """
        Start background polling if enabled.
        
        Returns:
            bool: True if started successfully
        """
        if not self.push_enabled:
            logger.info("Push updates not enabled, skipping background task")
            return True
        
        if self.is_running:
            logger.warning("API interface is already running")
            return True
        
        try:
            self.is_running = True
            self.task = asyncio.create_task(self._background_poll_loop())
            logger.info("Started vesting API background polling")
            return True
        except Exception as e:
            logger.error(f"Failed to start vesting API interface: {e}")
            self.is_running = False
            return False
    
    async def stop(self):
        """
        Stop background polling.
        
        Returns:
            bool: True if stopped successfully
        """
        if not self.is_running:
            return True
        
        try:
            self.is_running = False
            if self.task:
                self.task.cancel()
                try:
                    await self.task
                except asyncio.CancelledError:
                    pass
                self.task = None
            
            # Close HTTP session
            if self.session:
                await self.session.close()
                self.session = None
            
            logger.info("Stopped vesting API interface")
            return True
        except Exception as e:
            logger.error(f"Error stopping vesting API interface: {e}")
            return False
    
    async def push_all_data(self):
        """
        Push all vesting system data to the API.
        
        This comprehensive data push includes:
        1. System overview and configuration
        2. Miner stake metrics
        3. Coldkey metrics
        4. Recent transactions
        5. Vesting stats and multipliers
        
        Returns:
            bool: True if all data was pushed successfully
        """
        try:
            # Push system overview
            await self.push_system_overview()
            
            # Push stake metrics for all miners
            await self.push_all_stake_metrics()
            
            # Push coldkey metrics
            await self.push_all_coldkey_metrics()
            
            # Push recent transactions
            if self.export_detailed_transactions:
                await self.push_recent_transactions()
            
            # Push vesting multipliers
            await self.push_vesting_multipliers()
            
            self.last_sync_time = datetime.now(timezone.utc)
            logger.info(f"Pushed all vesting system data to API at {self.last_sync_time}")
            return True
        except Exception as e:
            logger.error(f"Failed to push all vesting system data: {e}")
            return False
    
    async def push_system_overview(self):
        """
        Push system overview data to the API.
        
        This includes configuration settings, global statistics, and system health.
        
        Returns:
            bool: True if data was pushed successfully
        """
        try:
            # Get vesting system stats
            stats = await self.vesting_system.get_vesting_stats()
            
            # Format system overview data
            overview = {
                'timestamp': datetime.now(timezone.utc).isoformat(),
                'config': {
                    'minimum_stake': self.vesting_system.minimum_stake,
                    'retention_window_days': self.vesting_system.retention_window_days,
                    'retention_target': self.vesting_system.retention_target,
                    'max_multiplier': self.vesting_system.max_multiplier
                },
                'stats': stats,
                'status': {
                    'blockchain_monitor_running': self.vesting_system.blockchain_monitor.is_running,
                    'transaction_monitor_running': (
                        self.vesting_system.transaction_monitor.is_running 
                        if self.vesting_system.transaction_monitor else False
                    ),
                    'last_epoch': self.vesting_system.blockchain_monitor.current_epoch
                }
            }
            
            # Push to API
            response = await self._post_data('/vesting/overview', overview)
            return response
        except Exception as e:
            logger.error(f"Failed to push system overview: {e}")
            return False
    
    async def push_all_stake_metrics(self, batch_size: Optional[int] = None):
        """
        Push stake metrics for all miners to the API.
        
        Args:
            batch_size: Optional batch size for sending data
        
        Returns:
            bool: True if all data was pushed successfully
        """
        try:
            # Use class default if batch size not specified
            batch_size = batch_size or self.max_batch_size
            
            # Get all stake metrics from database
            query = """
                SELECT * FROM stake_metrics
            """
            results = await self.vesting_system.db_manager.fetch_all(query)
            
            if not results:
                logger.warning("No stake metrics found to push")
                return True
            
            # Process metrics
            metrics = []
            for row in results:
                # Get holdings metrics
                holding_percentage, holding_duration = await self.vesting_system.stake_tracker.calculate_holding_metrics(
                    row['hotkey'], 
                    window_days=self.vesting_system.retention_window_days
                )
                
                # Calculate multiplier
                multiplier = self.vesting_system._calculate_multiplier(
                    holding_percentage, 
                    holding_duration
                )
                
                # Format metric data
                metric = {
                    'hotkey': row['hotkey'],
                    'coldkey': row['coldkey'],
                    'total_stake': float(row['total_stake']),
                    'manual_stake': float(row['manual_stake']),
                    'earned_stake': float(row['earned_stake']),
                    'first_stake_timestamp': row.get('first_stake_timestamp'),
                    'last_update': row['last_update'],
                    'holding_percentage': holding_percentage,
                    'holding_duration_days': holding_duration,
                    'vesting_multiplier': multiplier
                }
                metrics.append(metric)
            
            # Send metrics in batches
            success = True
            for i in range(0, len(metrics), batch_size):
                batch = metrics[i:i+batch_size]
                batch_response = await self._post_data('/vesting/stake_metrics', {
                    'timestamp': datetime.now(timezone.utc).isoformat(),
                    'metrics': batch
                })
                
                if not batch_response:
                    success = False
                    logger.error(f"Failed to push stake metrics batch {i//batch_size + 1}")
            
            logger.info(f"Pushed {len(metrics)} stake metrics to API")
            return success
        except Exception as e:
            logger.error(f"Failed to push stake metrics: {e}")
            return False
    
    async def push_all_coldkey_metrics(self, batch_size: Optional[int] = None):
        """
        Push coldkey metrics to the API.
        
        Args:
            batch_size: Optional batch size for sending data
        
        Returns:
            bool: True if all data was pushed successfully
        """
        try:
            # Use class default if batch size not specified
            batch_size = batch_size or self.max_batch_size
            
            # Get all coldkey metrics from database
            query = """
                SELECT * FROM coldkey_metrics
            """
            results = await self.vesting_system.db_manager.fetch_all(query)
            
            if not results:
                logger.warning("No coldkey metrics found to push")
                return True
            
            # Process metrics
            metrics = []
            for row in results:
                # Get all hotkeys for this coldkey
                hotkeys = await self.vesting_system.stake_tracker.get_all_hotkeys_for_coldkey(row['coldkey'])
                
                # Format metric data
                metric = {
                    'coldkey': row['coldkey'],
                    'total_stake': float(row['total_stake']),
                    'manual_stake': float(row['manual_stake']),
                    'earned_stake': float(row['earned_stake']),
                    'hotkey_count': row['hotkey_count'],
                    'hotkeys': hotkeys,
                    'last_update': row['last_update']
                }
                metrics.append(metric)
            
            # Send metrics in batches
            success = True
            for i in range(0, len(metrics), batch_size):
                batch = metrics[i:i+batch_size]
                batch_response = await self._post_data('/vesting/coldkey_metrics', {
                    'timestamp': datetime.now(timezone.utc).isoformat(),
                    'metrics': batch
                })
                
                if not batch_response:
                    success = False
                    logger.error(f"Failed to push coldkey metrics batch {i//batch_size + 1}")
            
            logger.info(f"Pushed {len(metrics)} coldkey metrics to API")
            return success
        except Exception as e:
            logger.error(f"Failed to push coldkey metrics: {e}")
            return False
    
    async def push_recent_transactions(self, days: int = 7, batch_size: Optional[int] = None):
        """
        Push recent transaction data to the API.
        
        Args:
            days: Number of days of transaction history to push
            batch_size: Optional batch size for sending data
        
        Returns:
            bool: True if all data was pushed successfully
        """
        if not self.export_detailed_transactions or not self.vesting_system.transaction_monitor:
            logger.info("Detailed transaction export disabled, skipping")
            return True
            
        try:
            # Use class default if batch size not specified
            batch_size = batch_size or self.max_batch_size
            
            # Calculate cutoff time
            cutoff_time = int((datetime.now(timezone.utc) - timedelta(days=days)).timestamp())
            
            # Get recent transactions
            query = f"""
                SELECT * FROM stake_transactions
                WHERE timestamp > ? AND id > ?
                ORDER BY timestamp DESC
                LIMIT {batch_size * 2}
            """
            results = await self.vesting_system.db_manager.fetch_all(
                query, 
                (cutoff_time, self._last_processed['transactions'])
            )
            
            if not results:
                logger.info("No new transactions to push")
                return True
            
            # Format transactions
            transactions = []
            max_id = self._last_processed['transactions']
            
            for row in results:
                transaction = {
                    'id': row['id'],
                    'block_number': row['block_number'],
                    'timestamp': row['timestamp'],
                    'transaction_type': row['transaction_type'],
                    'flow_type': row['flow_type'],
                    'hotkey': row['hotkey'],
                    'coldkey': row['coldkey'],
                    'call_amount': float(row['call_amount']) if row['call_amount'] is not None else None,
                    'final_amount': float(row['final_amount']) if row['final_amount'] is not None else None,
                    'fee': float(row['fee']) if row['fee'] is not None else None,
                    'origin_netuid': row.get('origin_netuid'),
                    'destination_netuid': row.get('destination_netuid'),
                    'destination_coldkey': row.get('destination_coldkey'),
                    'destination_hotkey': row.get('destination_hotkey'),
                    'tx_hash': row.get('tx_hash')
                }
                transactions.append(transaction)
                max_id = max(max_id, row['id'])
            
            # Send transactions in batches
            success = True
            for i in range(0, len(transactions), batch_size):
                batch = transactions[i:i+batch_size]
                batch_response = await self._post_data('/vesting/transactions', {
                    'timestamp': datetime.now(timezone.utc).isoformat(),
                    'transactions': batch
                })
                
                if not batch_response:
                    success = False
                    logger.error(f"Failed to push transactions batch {i//batch_size + 1}")
            
            # Update last processed ID
            if success and max_id > self._last_processed['transactions']:
                self._last_processed['transactions'] = max_id
                await self._save_last_processed_ids()
            
            logger.info(f"Pushed {len(transactions)} transactions to API")
            return success
        except Exception as e:
            logger.error(f"Failed to push recent transactions: {e}")
            return False
    
    async def push_stake_change_history(self, days: int = 30, batch_size: Optional[int] = None):
        """
        Push stake change history to the API.
        
        Args:
            days: Number of days of history to push
            batch_size: Optional batch size for sending data
        
        Returns:
            bool: True if all data was pushed successfully
        """
        try:
            # Use class default if batch size not specified
            batch_size = batch_size or self.max_batch_size
            
            # Calculate cutoff time
            cutoff_time = int((datetime.now(timezone.utc) - timedelta(days=days)).timestamp())
            
            # Get stake change history
            query = f"""
                SELECT * FROM stake_change_history
                WHERE timestamp > ? AND id > ?
                ORDER BY timestamp DESC
                LIMIT {batch_size * 2}
            """
            results = await self.vesting_system.db_manager.fetch_all(
                query, 
                (cutoff_time, self._last_processed['stake_changes'])
            )
            
            if not results:
                logger.info("No new stake changes to push")
                return True
            
            # Format history records
            history = []
            max_id = self._last_processed['stake_changes']
            
            for row in results:
                record = {
                    'id': row['id'],
                    'timestamp': row['timestamp'],
                    'hotkey': row['hotkey'],
                    'coldkey': row['coldkey'],
                    'change_type': row['change_type'],
                    'flow_type': row['flow_type'],
                    'amount': float(row['amount']),
                    'total_stake_after': float(row['total_stake_after']),
                    'manual_stake_after': float(row['manual_stake_after']),
                    'earned_stake_after': float(row['earned_stake_after'])
                }
                history.append(record)
                max_id = max(max_id, row['id'])
            
            # Send history in batches
            success = True
            for i in range(0, len(history), batch_size):
                batch = history[i:i+batch_size]
                batch_response = await self._post_data('/vesting/stake_history', {
                    'timestamp': datetime.now(timezone.utc).isoformat(),
                    'history': batch
                })
                
                if not batch_response:
                    success = False
                    logger.error(f"Failed to push stake history batch {i//batch_size + 1}")
            
            # Update last processed ID
            if success and max_id > self._last_processed['stake_changes']:
                self._last_processed['stake_changes'] = max_id
                await self._save_last_processed_ids()
            
            logger.info(f"Pushed {len(history)} stake change records to API")
            return success
        except Exception as e:
            logger.error(f"Failed to push stake change history: {e}")
            return False
    
    async def push_vesting_multipliers(self):
        """
        Push current vesting multipliers for all miners.
        
        Returns:
            bool: True if data was pushed successfully
        """
        try:
            # Get metagraph
            metagraph = self.vesting_system.subtensor.metagraph(self.vesting_system.subnet_id)
            
            # Calculate multipliers for all miners
            multipliers = []
            
            for uid in range(min(256, len(metagraph.hotkeys))):
                hotkey = metagraph.hotkeys[uid]
                
                # Get stake metrics
                metrics = await self.vesting_system.stake_tracker.get_stake_metrics(hotkey)
                if not metrics:
                    continue
                
                # Get holding metrics
                holding_percentage, holding_duration = await self.vesting_system.stake_tracker.calculate_holding_metrics(
                    hotkey,
                    window_days=self.vesting_system.retention_window_days
                )
                
                # Calculate multiplier
                multiplier = self.vesting_system._calculate_multiplier(holding_percentage, holding_duration)
                
                multipliers.append({
                    'uid': uid,
                    'hotkey': hotkey,
                    'coldkey': metagraph.coldkeys[uid] if uid < len(metagraph.coldkeys) else None,
                    'stake': float(metagraph.S[uid]),
                    'holding_percentage': holding_percentage,
                    'holding_duration_days': holding_duration,
                    'multiplier': multiplier
                })
            
            # Push to API
            response = await self._post_data('/vesting/multipliers', {
                'timestamp': datetime.now(timezone.utc).isoformat(),
                'subnet_id': self.vesting_system.subnet_id,
                'multipliers': multipliers
            })
            
            logger.info(f"Pushed {len(multipliers)} vesting multipliers to API")
            return response
        except Exception as e:
            logger.error(f"Failed to push vesting multipliers: {e}")
            return False
    
    async def push_tranche_data(self, hotkey: Optional[str] = None, batch_size: Optional[int] = None):
        """
        Push stake tranche data to the API.
        
        Args:
            hotkey: Optional hotkey to only push tranches for a specific miner
            batch_size: Optional batch size for sending data
            
        Returns:
            bool: True if data was pushed successfully
        """
        try:
            # Get tranche data
            tranches = []
            
            if hotkey:
                # Get tranches for specific hotkey
                miner_tranches = await self.vesting_system.stake_tracker.get_tranche_details(
                    hotkey, 
                    include_exits=True
                )
                tranches.append({
                    'hotkey': hotkey,
                    'tranches': miner_tranches
                })
            else:
                # Get tranches for all miners
                query = "SELECT hotkey FROM stake_metrics"
                results = await self.vesting_system.db_manager.fetch_all(query)
                
                for row in results:
                    miner_hotkey = row['hotkey']
                    miner_tranches = await self.vesting_system.stake_tracker.get_tranche_details(
                        miner_hotkey,
                        include_exits=True
                    )
                    
                    if miner_tranches:
                        tranches.append({
                            'hotkey': miner_hotkey,
                            'tranches': miner_tranches
                        })
            
            # Use class default if batch size not specified
            batch_size = batch_size or self.max_batch_size
            
            # Send tranche data in batches
            success = True
            for i in range(0, len(tranches), batch_size):
                batch = tranches[i:i+batch_size]
                batch_response = await self._post_data('/vesting/tranches', {
                    'timestamp': datetime.now(timezone.utc).isoformat(),
                    'miner_tranches': batch
                })
                
                if not batch_response:
                    success = False
                    logger.error(f"Failed to push tranche data batch {i//batch_size + 1}")
            
            logger.info(f"Pushed tranche data for {len(tranches)} miners to API")
            return success
        except Exception as e:
            logger.error(f"Failed to push tranche data: {e}")
            return False
    
    async def request_miner_details(self, hotkey: str):
        """
        Request detailed data for a specific miner.
        
        This method collects and returns comprehensive data for a specific miner,
        including stake metrics, transaction history, and tranche data.
        
        Args:
            hotkey: The miner's hotkey
            
        Returns:
            Dict: Miner details or None if failed
        """
        try:
            # Get stake metrics
            metrics = await self.vesting_system.stake_tracker.get_stake_metrics(hotkey)
            if not metrics:
                logger.warning(f"No stake metrics found for hotkey {hotkey}")
                return None
            
            # Get coldkey
            coldkey = metrics['coldkey']
            
            # Get holding metrics
            holding_percentage, holding_duration = await self.vesting_system.stake_tracker.calculate_holding_metrics(
                hotkey,
                window_days=self.vesting_system.retention_window_days
            )
            
            # Calculate multiplier
            multiplier = self.vesting_system._calculate_multiplier(holding_percentage, holding_duration)
            
            # Get transaction history (last 30 days)
            cutoff_time = int((datetime.now(timezone.utc) - timedelta(days=30)).timestamp())
            query = """
                SELECT * FROM stake_transactions
                WHERE timestamp > ? AND hotkey = ?
                ORDER BY timestamp DESC
            """
            transactions = await self.vesting_system.db_manager.fetch_all(query, (cutoff_time, hotkey))
            
            # Get stake change history
            query = """
                SELECT * FROM stake_change_history
                WHERE timestamp > ? AND hotkey = ?
                ORDER BY timestamp DESC
            """
            stake_changes = await self.vesting_system.db_manager.fetch_all(query, (cutoff_time, hotkey))
            
            # Get tranche data
            tranches = await self.vesting_system.stake_tracker.get_tranche_details(hotkey, include_exits=True)
            
            # Assemble complete miner details
            details = {
                'hotkey': hotkey,
                'coldkey': coldkey,
                'metrics': {
                    'total_stake': float(metrics['total_stake']),
                    'manual_stake': float(metrics['manual_stake']),
                    'earned_stake': float(metrics['earned_stake']),
                    'first_stake_timestamp': metrics.get('first_stake_timestamp'),
                    'last_update': metrics['last_update'],
                    'holding_percentage': holding_percentage,
                    'holding_duration_days': holding_duration,
                    'vesting_multiplier': multiplier
                },
                'transactions': [
                    {
                        'id': tx['id'],
                        'block_number': tx['block_number'],
                        'timestamp': tx['timestamp'],
                        'transaction_type': tx['transaction_type'],
                        'flow_type': tx['flow_type'],
                        'call_amount': float(tx['call_amount']) if tx['call_amount'] is not None else None,
                        'final_amount': float(tx['final_amount']) if tx['final_amount'] is not None else None,
                        'fee': float(tx['fee']) if tx['fee'] is not None else None,
                        'origin_netuid': tx.get('origin_netuid'),
                        'destination_netuid': tx.get('destination_netuid'),
                        'tx_hash': tx.get('tx_hash')
                    }
                    for tx in transactions
                ],
                'stake_changes': [
                    {
                        'id': ch['id'],
                        'timestamp': ch['timestamp'],
                        'change_type': ch['change_type'],
                        'flow_type': ch['flow_type'],
                        'amount': float(ch['amount']),
                        'total_stake_after': float(ch['total_stake_after']),
                        'manual_stake_after': float(ch['manual_stake_after']),
                        'earned_stake_after': float(ch['earned_stake_after'])
                    }
                    for ch in stake_changes
                ],
                'tranches': tranches
            }
            
            return details
        except Exception as e:
            logger.error(f"Failed to get miner details for {hotkey}: {e}")
            return None
    
    async def _background_poll_loop(self):
        """
        Background task for polling and pushing data to the API.
        """
        logger.info("Starting vesting API background polling loop")
        try:
            while self.is_running:
                try:
                    logger.debug("Running vesting API data push cycle")
                    
                    # Push incremental transaction updates
                    await self.push_recent_transactions()
                    
                    # Push incremental stake change history
                    await self.push_stake_change_history()
                    
                    # Every hour, push complete metrics update
                    if (not self.last_sync_time or 
                        (datetime.now(timezone.utc) - self.last_sync_time) > timedelta(hours=1)):
                        await self.push_system_overview()
                        await self.push_all_stake_metrics()
                        await self.push_all_coldkey_metrics()
                        await self.push_vesting_multipliers()
                        self.last_sync_time = datetime.now(timezone.utc)
                        
                except Exception as e:
                    logger.error(f"Error in vesting API polling loop: {e}")
                
                # Wait for next poll interval
                await asyncio.sleep(self.poll_interval)
        except asyncio.CancelledError:
            logger.info("Vesting API background task cancelled")
        except Exception as e:
            logger.error(f"Unexpected error in vesting API background task: {e}")
    
    async def _test_connection(self):
        """
        Test API connectivity.
        
        Returns:
            bool: True if connection successful
        """
        try:
            if not self.session:
                self.session = aiohttp.ClientSession(
                    headers=self._get_auth_headers()
                )
            
            url = f"{self.api_base_url}/health"
            async with self.session.get(url, timeout=10) as response:
                if response.status == 200:
                    logger.info("Successfully connected to vesting API")
                    return True
                else:
                    text = await response.text()
                    logger.error(f"API connection test failed with status {response.status}: {text}")
                    return False
        except Exception as e:
            logger.error(f"API connection test failed: {e}")
            return False
    
    async def _post_data(self, endpoint: str, data: Dict):
        """
        Post data to the API.
        
        Args:
            endpoint: API endpoint
            data: Data to post
            
        Returns:
            bool: True if successful
        """
        if not self.session:
            logger.error("No active session, initialize first")
            return False
        
        try:
            url = f"{self.api_base_url}{endpoint}"
            
            # Convert any numpy types to Python native types
            serializable_data = self._make_serializable(data)
            
            async with self.session.post(
                url,
                json=serializable_data,
                headers=self._get_auth_headers(),
                timeout=30
            ) as response:
                if response.status in (200, 201, 202):
                    return True
                else:
                    text = await response.text()
                    logger.error(f"API request to {endpoint} failed with status {response.status}: {text}")
                    return False
        except Exception as e:
            logger.error(f"Error posting data to {endpoint}: {e}")
            return False
    
    def _get_auth_headers(self):
        """Get authentication headers for API requests."""
        headers = {'Content-Type': 'application/json'}
        if self.api_key:
            headers['Authorization'] = f"Bearer {self.api_key}"
        return headers
    
    async def _load_last_processed_ids(self):
        """Load the last processed IDs from the database."""
        try:
            # Table to store API interface state
            create_table_query = """
                CREATE TABLE IF NOT EXISTS vesting_api_state (
                    key TEXT PRIMARY KEY,
                    value INTEGER NOT NULL,
                    updated_at INTEGER NOT NULL
                )
            """
            await self.vesting_system.db_manager.execute_query(create_table_query)
            
            # Load values
            for key in self._last_processed.keys():
                query = "SELECT value FROM vesting_api_state WHERE key = ?"
                result = await self.vesting_system.db_manager.fetch_one(query, (f"last_{key}_id",))
                if result:
                    self._last_processed[key] = result['value']
            
            logger.debug(f"Loaded last processed IDs: {self._last_processed}")
        except Exception as e:
            logger.error(f"Error loading last processed IDs: {e}")
    
    async def _save_last_processed_ids(self):
        """Save the last processed IDs to the database."""
        try:
            timestamp = int(datetime.now(timezone.utc).timestamp())
            
            for key, value in self._last_processed.items():
                query = """
                    INSERT INTO vesting_api_state (key, value, updated_at)
                    VALUES (?, ?, ?)
                    ON CONFLICT(key) DO UPDATE SET
                    value = excluded.value,
                    updated_at = excluded.updated_at
                """
                await self.vesting_system.db_manager.execute_query(
                    query, 
                    (f"last_{key}_id", value, timestamp)
                )
            
            logger.debug(f"Saved last processed IDs: {self._last_processed}")
        except Exception as e:
            logger.error(f"Error saving last processed IDs: {e}")
    
    def _make_serializable(self, obj):
        """
        Make an object JSON serializable by converting numpy types.
        
        Args:
            obj: Input object
            
        Returns:
            Serializable object
        """
        if isinstance(obj, dict):
            return {k: self._make_serializable(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self._make_serializable(v) for v in obj]
        elif isinstance(obj, (np.int32, np.int64)):
            return int(obj)
        elif isinstance(obj, (np.float32, np.float64)):
            return float(obj)
        elif isinstance(obj, np.bool_):
            return bool(obj)
        elif isinstance(obj, np.ndarray):
            return self._make_serializable(obj.tolist())
        else:
            return obj 