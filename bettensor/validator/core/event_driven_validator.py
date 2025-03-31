"""
Event-driven validator for Bettensor.

This module provides the main EventDrivenValidator class that integrates
all the core components to create an event-driven architecture for the validator.
"""

import asyncio
import time
import traceback
from typing import Dict, Any, Optional, List
from datetime import datetime, timezone
import bittensor as bt
import argparse

from bettensor.base.neuron import BaseNeuron
from bettensor.validator.io.miner_data import MinerDataMixin
from bettensor.validator.core.event_manager import EventManager
from bettensor.validator.core.task_manager import TaskManager
from bettensor.validator.core.prediction_service import PredictionService
from bettensor.validator.core.scoring_service import ScoringService
from bettensor.validator.core.weight_service import WeightService
from bettensor.validator.core.communication_manager import CommunicationManager

class EventDrivenValidator(BaseNeuron, MinerDataMixin):
    """
    Event-driven validator for Bettensor.
    
    This validator uses an event-driven architecture to handle operations
    efficiently. Components communicate through a central event bus,
    allowing for loosely coupled systems and more flexible operation.
    
    Features:
    - Real-time processing of predictions via WebSocket and HTTP
    - Event-based communication between components
    - Scheduled periodic tasks for scoring and weight setting
    - Smart fallback to HTTP when WebSocket is unavailable
    - Detailed statistics tracking
    
    This architecture provides better performance, cleaner code structure,
    and more reliable operation compared to the previous polling-based approach.
    """
    
    @classmethod
    def config(cls):
        """Get config from the argument parser."""
        parser = argparse.ArgumentParser()
        bt.wallet.add_args(parser)
        bt.subtensor.add_args(parser)
        bt.logging.add_args(parser)
        bt.axon.add_args(parser)
        cls.add_args(parser)
        return cls(bt.config(parser))
    
    @classmethod
    def add_args(cls, parser):
        """Add validator specific arguments to the parser."""
        parser.add_argument(
            "--db",
            type=str,
            default="./bettensor/validator/state/validator.db",
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
        
        # WebSocket configuration parameters
        parser.add_argument(
            "--websocket.enabled",
            type=str,
            default="True",
            help="Enable WebSocket connections for real-time communications (True/False)"
        )
        parser.add_argument(
            "--websocket.heartbeat_interval",
            type=int,
            default=60,
            help="Interval in seconds between heartbeat messages"
        )
        parser.add_argument(
            "--websocket.connection_timeout",
            type=int,
            default=300,
            help="Time in seconds after which a connection is considered stale"
        )
        
        # Event-driven specific parameters
        parser.add_argument(
            "--event_driven.scoring_interval",
            type=int,
            default=3600,
            help="Interval in seconds between scoring runs"
        )
        parser.add_argument(
            "--event_driven.weights_interval",
            type=int,
            default=7200,
            help="Interval in seconds between setting weights"
        )
        parser.add_argument(
            "--event_driven.game_data_interval",
            type=int,
            default=600,
            help="Interval in seconds between game data updates"
        )
        parser.add_argument(
            "--event_driven.http_fallback",
            type=str,
            default="True",
            help="Enable HTTP fallback for disconnected miners (True/False)"
        )
    
    def __init__(self, config=None):
        """
        Initialize the event-driven validator.
        
        Args:
            config: Optional configuration parameters
        """
        # Initialize base classes
        super().__init__(config)
        
        # Create core services
        self.event_manager = EventManager()
        self.task_manager = TaskManager()
        
        # These will be initialized during startup
        self.prediction_service = None
        self.scoring_service = None
        self.weight_service = None
        self.communication_manager = None
        
        # Flags for operation
        self.is_running = False
        self.is_initialized = False
        
    async def initialize(self):
        """Initialize the validator and all its components."""
        try:
            bt.logging.info("Initializing event-driven validator")
            
            # First initialize the base validator
            await self.initialize_neuron()
            
            # Start event manager
            await self.event_manager.start()
            
            # Create and initialize services
            self.prediction_service = PredictionService(self, self.event_manager)
            self.scoring_service = ScoringService(self, self.event_manager)
            self.weight_service = WeightService(self, self.event_manager)
            self.communication_manager = CommunicationManager(self, self.event_manager)
            
            # Set cross-references between services
            if hasattr(self, 'websocket_manager') and self.websocket_manager is not None:
                self.prediction_service.set_websocket_manager(self.websocket_manager)
                self.communication_manager.set_websocket_manager(self.websocket_manager)
            
            # Start task manager
            await self.task_manager.start()
            
            # Start communication manager
            await self.communication_manager.start()
            
            # Flag as initialized
            self.is_initialized = True
            bt.logging.info("Event-driven validator initialized successfully")
            
            return True
            
        except Exception as e:
            bt.logging.error(f"Error initializing event-driven validator: {str(e)}")
            bt.logging.debug(traceback.format_exc())
            return False
            
    async def start(self):
        """Start the validator and schedule periodic tasks."""
        if not self.is_initialized:
            bt.logging.error("Cannot start: validator not initialized")
            return False
            
        try:
            bt.logging.info("Starting event-driven validator")
            
            # Schedule periodic tasks
            self._schedule_periodic_tasks()
            
            # Serve Axon if not already serving
            if not hasattr(self, 'axon') or not self.axon:
                self.serve_axon()
                
            # Start WebSocket manager if enabled
            if hasattr(self, 'websocket_manager') and self.websocket_manager is not None:
                if not self.websocket_manager.is_running():
                    await self.websocket_manager.start()
            
            # Flag as running
            self.is_running = True
            bt.logging.info("Event-driven validator started successfully")
            
            # Publish start event
            await self.event_manager.publish(
                "validator_started",
                {
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "hotkey": self.wallet.hotkey.ss58_address
                },
                source="validator"
            )
            
            return True
            
        except Exception as e:
            bt.logging.error(f"Error starting event-driven validator: {str(e)}")
            bt.logging.debug(traceback.format_exc())
            return False
            
    async def stop(self):
        """Stop the validator and all its components."""
        if not self.is_running:
            return
            
        try:
            bt.logging.info("Stopping event-driven validator")
            
            # Flag as not running
            self.is_running = False
            
            # Save state before shutting down
            await self.save_state()
            
            # Stop task manager
            await self.task_manager.stop()
            
            # Stop WebSocket manager if enabled
            if hasattr(self, 'websocket_manager') and self.websocket_manager is not None:
                await self.websocket_manager.stop()
                
            # Stop communication manager
            await self.communication_manager.stop()
            
            # Stop event manager (do this last)
            await self.event_manager.stop()
            
            bt.logging.info("Event-driven validator stopped successfully")
            
        except Exception as e:
            bt.logging.error(f"Error stopping event-driven validator: {str(e)}")
            bt.logging.debug(traceback.format_exc())
            
    def _schedule_periodic_tasks(self):
        """Schedule all periodic tasks."""
        # Schedule metagraph sync
        self.task_manager.create_periodic_task(
            self._sync_metagraph,
            interval_seconds=300,  # 5 minutes
            name="sync_metagraph",
            jitter=0.1  # 10% jitter
        )
        
        # Schedule game data updates
        self.task_manager.create_periodic_task(
            self._update_game_data,
            interval_seconds=600,  # 10 minutes
            name="update_game_data",
            jitter=0.1  # 10% jitter
        )
        
        # Schedule scoring run
        self.task_manager.create_periodic_task(
            self._run_scoring,
            interval_seconds=3600,  # 1 hour
            name="run_scoring",
            jitter=0.05  # 5% jitter
        )
        
        # Schedule weight setting
        self.task_manager.create_periodic_task(
            self._set_weights,
            interval_seconds=7200,  # 2 hours
            name="set_weights",
            jitter=0.05  # 5% jitter
        )
        
        # Schedule status logging
        self.task_manager.create_periodic_task(
            self._log_status,
            interval_seconds=60,  # 1 minute
            name="log_status"
        )
        
        # Schedule periodic state saving
        self.task_manager.create_periodic_task(
            self.save_state,
            interval_seconds=900,  # 15 minutes
            name="save_state"
        )
        
        bt.logging.info("Scheduled all periodic tasks")
        
    async def _sync_metagraph(self):
        """Sync the metagraph and check for changes."""
        bt.logging.info("Syncing metagraph")
        
        # Sync metagraph in a thread to avoid blocking
        self.metagraph = await asyncio.to_thread(
            self.sync_metagraph
        )
        
        # Check for hotkey changes
        await self.check_hotkeys()
        
        # Publish event
        await self.event_manager.publish(
            "metagraph_updated",
            {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "num_hotkeys": len(self.metagraph.hotkeys),
                "block": self.subtensor.block
            },
            source="validator"
        )
        
    async def _update_game_data(self):
        """Update game data from the API."""
        bt.logging.info("Updating game data")
        
        try:
            # Get current time
            current_time = datetime.now(timezone.utc)
            
            # Update games using the sports data service
            all_games = await self.sports_data.fetch_and_update_game_data(self.last_api_call)
            
            # Update last API call time
            self.last_api_call = current_time
            
            # Update game data event
            if all_games and isinstance(all_games, list) and len(all_games) > 0:
                await self.event_manager.publish(
                    "game_updated",
                    all_games,
                    source="validator"
                )
                
                # Broadcast game updates to miners via communication manager
                await self.communication_manager.broadcast_game_updates(all_games)
            
        except Exception as e:
            bt.logging.error(f"Error updating game data: {str(e)}")
            bt.logging.debug(traceback.format_exc())
            
    async def _run_scoring(self):
        """Run the scoring calculation."""
        # Check if we should run scoring
        should_run = await self.scoring_service.should_run_scoring()
        
        if should_run:
            bt.logging.info("Running scoring calculation")
            
            # Run scoring
            success = await self.scoring_service.run_scoring()
            
            if success:
                # Update block number
                self.last_scoring_block = self.subtensor.block
                
                # Save state after scoring
                await self.save_state()
        else:
            bt.logging.debug("Skipping scoring run (not due yet)")
            
    async def _set_weights(self):
        """Set weights on the network."""
        # Check if we should set weights
        should_set = await self.weight_service.should_set_weights()
        
        if should_set:
            bt.logging.info("Setting weights on the network")
            
            # Set weights
            success = await self.weight_service.set_weights()
            
            if success:
                # Update block number
                self.last_set_weights_block = self.subtensor.block
                
                # Save state after setting weights
                await self.save_state()
        else:
            bt.logging.debug("Skipping weights setting (not due yet)")
            
    async def _log_status(self):
        """Log the current status of the validator."""
        if not self.is_running:
            return
            
        try:
            # Get current time and block
            current_time = datetime.now(timezone.utc)
            current_block = self.subtensor.block if self.subtensor else None
            
            # Get connection stats
            connected_miners = len(self.communication_manager.get_connected_miners())
            total_miners = len(self.metagraph.hotkeys) if self.metagraph else 0
            connection_percentage = (connected_miners / total_miners * 100) if total_miners > 0 else 0
            
            # Get task stats
            task_stats = self.task_manager.get_task_stats()
            event_stats = self.event_manager.get_stats()
            
            # Log basic status
            bt.logging.info(
                f"\n================================ VALIDATOR STATUS ================================\n"
                f"Current time: {current_time}\n"
                f"Current block: {current_block}\n"
                f"Connected miners: {connected_miners}/{total_miners} ({connection_percentage:.1f}%)\n"
                f"Events processed: {sum(event_stats['processed_events'].values())}\n"
                f"Tasks running: {len(self.task_manager.tasks)}\n"
                f"Event subscribers: {sum(event_stats['subscriber_count'].values())}\n"
                f"================================================================================\n"
            )
            
        except Exception as e:
            bt.logging.error(f"Error logging status: {str(e)}")
            
    def get_stats(self) -> Dict[str, Any]:
        """Get comprehensive statistics from all components."""
        stats = {
            "validator": {
                "is_running": self.is_running,
                "is_initialized": self.is_initialized,
                "current_block": self.subtensor.block if hasattr(self, 'subtensor') and self.subtensor else None,
                "hotkey": self.wallet.hotkey.ss58_address if hasattr(self, 'wallet') and self.wallet else None
            }
        }
        
        # Get stats from all components
        if self.task_manager:
            stats["tasks"] = self.task_manager.get_task_stats()
            
        if self.event_manager:
            stats["events"] = self.event_manager.get_stats()
            
        if self.prediction_service:
            stats["predictions"] = self.prediction_service.get_stats()
            
        if self.scoring_service:
            stats["scoring"] = self.scoring_service.get_stats()
            
        if self.weight_service:
            stats["weights"] = self.weight_service.get_stats()
            
        if self.communication_manager:
            stats["communication"] = self.communication_manager.get_stats()
            
        return stats
        
    # HTTP and WebSocket handlers
    
    async def handle_websocket_prediction(self, miner_hotkey, prediction):
        """Handle a prediction received via WebSocket."""
        if self.prediction_service:
            await self.prediction_service.handle_websocket_prediction(miner_hotkey, prediction)
            
    async def handle_http_prediction(self, synapse, miner_hotkey):
        """Handle a prediction received via HTTP."""
        if self.prediction_service:
            return await self.prediction_service.handle_http_prediction(synapse, miner_hotkey)
        return synapse 