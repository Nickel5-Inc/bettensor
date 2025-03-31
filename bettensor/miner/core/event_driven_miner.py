"""
Event-Driven Miner implementation for Bettensor.

This module provides the main entry point for the event-driven miner architecture,
coordinating various services and handling lifecycle operations.
"""

import argparse
import asyncio
import os
import signal
import sys
from typing import Dict, List, Optional

import bittensor as bt

from bettensor.base.neuron import BaseNeuron
from bettensor.miner.core.event_manager import EventManager
from bettensor.miner.core.task_manager import TaskManager
from bettensor.miner.core.communication_service import CommunicationService
from bettensor.miner.core.database_manager import DatabaseManager
from bettensor.miner.core.game_service import GameService
from bettensor.miner.core.prediction_service import PredictionService


class EventDrivenMiner(BaseNeuron):
    """
    Event-driven implementation of the Bettensor Miner.
    
    This class orchestrates the various services that make up the miner,
    providing a clean, event-driven architecture for improved maintainability
    and extensibility.
    """
    
    @classmethod
    def add_args(cls, parser: argparse.ArgumentParser):
        """Add miner-specific arguments to the parser."""
        # First add BaseNeuron's arguments
        super().add_args(parser)
        
        # Database configuration
        parser.add_argument(
            "--db_path",
            type=str,
            default="./bettensor_miner.db",
            help="Path to the SQLite database file"
        )
        
        # Redis configuration (for GUI interfaces)
        parser.add_argument(
            "--redis_host",
            type=str,
            default="localhost",
            help="Redis host"
        )
        parser.add_argument(
            "--redis_port",
            type=int,
            default=6379,
            help="Redis port"
        )
        
        # WebSocket configuration
        parser.add_argument(
            "--websocket_enabled",
            type=str,
            default="True",
            help="Enable WebSocket connections (True/False)"
        )
        parser.add_argument(
            "--websocket_heartbeat_interval",
            type=int,
            default=30,
            help="Interval in seconds between WebSocket heartbeats"
        )
        parser.add_argument(
            "--websocket_connection_timeout",
            type=int,
            default=5,
            help="Timeout in seconds for WebSocket connection attempts"
        )
        
        # Task scheduling configuration
        parser.add_argument(
            "--task_metagraph_sync_interval",
            type=int,
            default=60,
            help="Interval in seconds for metagraph synchronization"
        )
        parser.add_argument(
            "--task_stats_update_interval",
            type=int,
            default=300,  # 5 minutes
            help="Interval in seconds for statistics updates"
        )
        parser.add_argument(
            "--task_prediction_interval",
            type=int,
            default=3600,  # 1 hour
            help="Interval in seconds for prediction generation"
        )
        parser.add_argument(
            "--task_game_update_interval",
            type=int,
            default=3600,  # 1 hour
            help="Interval in seconds for game data updates"
        )
        
        # Game data configuration
        parser.add_argument(
            "--minimum_wager_amount",
            type=float,
            default=0.01,
            help="Minimum wager amount for predictions"
        )
        parser.add_argument(
            "--validator_min_stake",
            type=float,
            default=100.0,
            help="Minimum stake required for validators"
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
        """Initialize the event-driven miner."""
        super().__init__(config=config)
        
        # Initialize bittensor components
        self.wallet = None
        self.subtensor = None
        self.metagraph = None
        self.dendrite = None
        self.axon = None
        self.miner_uid = None
        
        # Initialize services
        self.event_manager = EventManager()
        self.task_manager = TaskManager(self.event_manager)
        self.database_manager = DatabaseManager(db_path=getattr(self.config, "db_path", "./bettensor_miner.db"))
        
        # Will be initialized during initialize()
        self.communication_service = None
        self.game_service = None
        self.prediction_service = None
        
        # Initialize other components
        self.is_running = False
        self.is_initialized = False
        self.last_updated_block = 0
        self.step = 0
        
        bt.logging.info("EventDrivenMiner initialized")
    
    async def initialize(self):
        """Initialize the miner and its components."""
        if self.is_initialized:
            bt.logging.warning("Miner is already initialized")
            return
        
        try:
            bt.logging.info("Initializing event-driven miner")
            
            # Set up bittensor objects (wallet, subtensor, dendrite, metagraph)
            try:
                self.wallet = bt.wallet(config=self.config)
                self.subtensor = bt.subtensor(config=self.config)
                self.dendrite = bt.dendrite(wallet=self.wallet)
                self.metagraph = self.subtensor.metagraph(self.config.netuid)
                
                # Check if wallet is registered
                if self.wallet.hotkey.ss58_address not in self.metagraph.hotkeys:
                    bt.logging.error(f"Wallet {self.wallet.hotkey.ss58_address} is not registered")
                    return
                
                # Get miner UID
                self.miner_uid = self.metagraph.hotkeys.index(self.wallet.hotkey.ss58_address)
                bt.logging.info(f"Miner initialized with UID: {self.miner_uid}")
                
            except Exception as e:
                bt.logging.error(f"Failed to initialize bittensor objects: {e}")
                raise
            
            # Initialize database
            await self.database_manager.initialize()
            
            # Initialize communication service
            self.communication_service = CommunicationService(
                event_manager=self.event_manager,
                metagraph=self.metagraph,
                wallet=self.wallet,
                dendrite=self.dendrite
            )
            
            # Initialize game service
            self.game_service = GameService(
                event_manager=self.event_manager,
                db_manager=self.database_manager
            )
            
            # Initialize prediction service
            self.prediction_service = PredictionService(
                event_manager=self.event_manager,
                communication_service=self.communication_service,
                game_service=self.game_service
            )
            
            # Initialize axon
            self.axon = bt.axon(wallet=self.wallet, config=self.config)
            
            # Set up signal handlers
            signal.signal(signal.SIGINT, self._signal_handler)
            signal.signal(signal.SIGTERM, self._signal_handler)
            
            self.is_initialized = True
            bt.logging.info("Event-driven miner initialization complete")
            
        except Exception as e:
            bt.logging.error(f"Error initializing miner: {e}")
            raise
    
    async def start(self):
        """Start the miner and its components."""
        if not self.is_initialized:
            bt.logging.warning("Miner is not initialized, initializing now")
            await self.initialize()
        
        if self.is_running:
            bt.logging.warning("Miner is already running")
            return
        
        try:
            bt.logging.info("Starting event-driven miner")
            
            # Start the task manager
            await self.task_manager.start()
            
            # Start the database manager (no actual start needed)
            
            # Start the game service
            await self.game_service.start()
            
            # Start the prediction service
            await self.prediction_service.start()
            
            # Start the communication service
            await self.communication_service.start()
            
            # Schedule periodic tasks
            self._schedule_periodic_tasks()
            
            # Start axon
            self.axon.start()
            bt.logging.info(f"Axon started on port {self.config.axon.port}")
            
            # Mark miner as running
            self.is_running = True
            bt.logging.info("Event-driven miner started")
            
        except Exception as e:
            bt.logging.error(f"Error starting miner: {e}")
            # Try to stop anything that might have started
            await self.stop()
            raise
    
    async def stop(self):
        """Stop the miner and its components."""
        if not self.is_running:
            bt.logging.warning("Miner is not running")
            return
        
        try:
            bt.logging.info("Stopping event-driven miner")
            
            # Stop all services
            if self.task_manager:
                await self.task_manager.stop()
            
            if self.prediction_service:
                await self.prediction_service.stop()
            
            if self.game_service:
                await self.game_service.stop()
            
            if self.communication_service:
                await self.communication_service.stop()
            
            # Close database connection
            if self.database_manager:
                await self.database_manager.close()
            
            # Stop axon
            if self.axon:
                self.axon.stop()
            
            # Mark miner as not running
            self.is_running = False
            bt.logging.info("Event-driven miner stopped")
            
        except Exception as e:
            bt.logging.error(f"Error stopping miner: {e}")
            raise
    
    def _schedule_periodic_tasks(self):
        """Schedule periodic tasks for the miner."""
        bt.logging.info("Scheduling periodic tasks")
        
        # Schedule metagraph sync
        self.task_manager.add_task(
            name="metagraph_sync",
            coro_func=self._sync_metagraph,
            interval=getattr(self.config, "task_metagraph_sync_interval", 60),
            jitter=0.1
        )
        
        # Schedule statistics update
        self.task_manager.add_task(
            name="stats_update",
            coro_func=self._update_stats,
            interval=getattr(self.config, "task_stats_update_interval", 300),
            jitter=0.1
        )
        
        # Schedule game data update
        self.task_manager.add_task(
            name="game_update",
            coro_func=self._update_game_data,
            interval=getattr(self.config, "task_game_update_interval", 3600),
            jitter=0.2
        )
        
        # Schedule prediction task
        self.task_manager.add_task(
            name="prediction_generation",
            coro_func=self._generate_predictions,
            interval=getattr(self.config, "task_prediction_interval", 3600),
            jitter=0.2
        )
        
        bt.logging.info("Periodic tasks scheduled")
    
    async def _sync_metagraph(self):
        """Synchronize the metagraph."""
        try:
            bt.logging.info("Syncing metagraph")
            
            # Sync metagraph
            current_block = self.subtensor.block
            self.metagraph.sync(subtensor=self.subtensor)
            
            # Update miner UID if it's changed
            if self.wallet.hotkey.ss58_address in self.metagraph.hotkeys:
                new_uid = self.metagraph.hotkeys.index(self.wallet.hotkey.ss58_address)
                if new_uid != self.miner_uid:
                    bt.logging.info(f"Miner UID changed from {self.miner_uid} to {new_uid}")
                    self.miner_uid = new_uid
            else:
                bt.logging.warning("Miner hotkey not found in metagraph")
            
            # Sync validators in communication service
            if self.communication_service:
                self.communication_service.sync_validators()
            
            self.last_updated_block = current_block
            
            # Log current state
            if self.miner_uid < len(self.metagraph.S):
                stake = self.metagraph.S[self.miner_uid].item()
                rank = self.metagraph.R[self.miner_uid].item()
                trust = self.metagraph.T[self.miner_uid].item()
                consensus = self.metagraph.C[self.miner_uid].item()
                incentive = self.metagraph.I[self.miner_uid].item()
                
                bt.logging.info(
                    f"Miner state: UID={self.miner_uid}, Block={current_block}, "
                    f"Stake={stake:.2f}, Rank={rank:.2f}, Trust={trust:.2f}, "
                    f"Consensus={consensus:.2f}, Incentive={incentive:.6f}"
                )
            
            # Publish metagraph updated event
            await self.event_manager.publish("metagraph_updated", {
                "block": current_block,
                "miner_uid": self.miner_uid
            })
            
        except Exception as e:
            bt.logging.error(f"Error syncing metagraph: {e}")
            # No need to re-raise, the task manager will handle retries
    
    async def _update_stats(self):
        """Update and log statistics."""
        try:
            # Gather statistics from all components
            stats = self.get_stats()
            
            # Log summary
            bt.logging.info(f"Miner statistics: Block={stats['block']}, Step={stats['step']}")
            
            if self.communication_service:
                comm_stats = stats.get("communication_service", {})
                connected = comm_stats.get("connected_validators", 0)
                total = comm_stats.get("total_validators", 0)
                ws_sent = comm_stats.get("predictions_sent_ws", 0)
                http_sent = comm_stats.get("predictions_sent_http", 0)
                confirmed = comm_stats.get("predictions_confirmed", 0)
                
                bt.logging.info(
                    f"Communication: Connected={connected}/{total} validators, "
                    f"Predictions: WS={ws_sent}, HTTP={http_sent}, Confirmed={confirmed}"
                )
            
            # Publish stats updated event
            await self.event_manager.publish("stats_updated", stats)
            
            # Save stats to database
            if self.database_manager:
                await self.database_manager.save_statistic("miner_stats", 1.0, stats)
            
        except Exception as e:
            bt.logging.error(f"Error updating stats: {e}")
    
    async def _update_game_data(self):
        """Update game data."""
        try:
            bt.logging.info("Starting game data update task")
            
            if self.game_service:
                # Update data for each supported sport
                for sport in ["soccer", "football"]:
                    games = await self.game_service.get_games_by_sport(sport)
                    bt.logging.info(f"Updated {len(games)} {sport} games")
            
        except Exception as e:
            bt.logging.error(f"Error updating game data: {e}")
    
    async def _generate_predictions(self):
        """Generate and send predictions."""
        try:
            bt.logging.info("Starting prediction generation task")
            
            if self.prediction_service:
                # Generate predictions for all sports
                results = await self.prediction_service.generate_predictions()
                
                # Log summary
                total_predictions = sum(len(preds) for preds in results.values())
                bt.logging.info(f"Generated {total_predictions} predictions across {len(results)} sports")
            
        except Exception as e:
            bt.logging.error(f"Error generating predictions: {e}")
    
    def get_stats(self) -> Dict:
        """
        Get statistics about the miner.
        
        Returns:
            A dictionary of statistics
        """
        stats = {
            "miner_uid": self.miner_uid,
            "step": self.step,
            "block": self.last_updated_block,
            "initialized": self.is_initialized,
            "running": self.is_running,
            "event_manager": self.event_manager.get_stats(),
            "task_manager": self.task_manager.get_stats(),
        }
        
        # Add service stats
        if self.communication_service:
            stats["communication_service"] = self.communication_service.get_stats()
        
        if self.game_service:
            stats["game_service"] = self.game_service.get_stats()
        
        if self.prediction_service:
            stats["prediction_service"] = self.prediction_service.get_stats()
        
        if self.database_manager:
            stats["database_manager"] = self.database_manager.get_stats()
        
        return stats
    
    def _signal_handler(self, signum, frame):
        """Handle process signals gracefully."""
        signal_names = {
            signal.SIGINT: "SIGINT",
            signal.SIGTERM: "SIGTERM"
        }
        
        signal_name = signal_names.get(signum, f"Signal {signum}")
        bt.logging.info(f"Received {signal_name}. Starting graceful shutdown...")
        
        # Create event loop for cleanup
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        try:
            # Run cleanup
            loop.run_until_complete(self.stop())
            bt.logging.info("Graceful shutdown completed")
        except Exception as e:
            bt.logging.error(f"Error during shutdown: {e}")
        finally:
            loop.close()
            sys.exit(0)
    
    @classmethod
    async def create(cls):
        """Create and initialize a new miner instance."""
        # Create instance
        miner = cls(config=cls.config())
        
        # Initialize
        await miner.initialize()
        
        return miner 