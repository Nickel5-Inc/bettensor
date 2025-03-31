"""
WebSocket Manager for Bettensor Miners

This module implements a WebSocket client that establishes and maintains 
persistent connections to validators for real-time prediction submission 
and confirmations.
"""

import asyncio
import json
import time
import traceback
from datetime import datetime, timezone
from typing import Dict, List, Optional, Set, Tuple

import bittensor as bt
import websockets
from websockets.exceptions import ConnectionClosed

from bettensor.protocol import (
    ConfirmationMessage,
    GameUpdateMessage,
    HeartbeatMessage,
    PredictionMessage,
    TeamGamePrediction,
    WebSocketConnection,
    WebSocketMessage
)

class MinerWebSocketManager:
    """
    Manages WebSocket connections to validators for real-time communication.
    
    This class handles establishing connections to validators, sending predictions,
    and processing confirmation messages.
    """
    
    def __init__(self, miner_hotkey: str, metagraph: bt.metagraph, fallback_http_timeout: int = 20):
        """
        Initialize the WebSocket manager.
        
        Args:
            miner_hotkey: The miner's hotkey address
            metagraph: The bittensor metagraph
            fallback_http_timeout: Timeout for fallback HTTP requests in seconds
        """
        self.miner_hotkey = miner_hotkey
        self.metagraph = metagraph
        self.fallback_http_timeout = fallback_http_timeout
        
        # Connection management
        self.connections: Dict[str, WebSocketConnection] = {}  # validator_hotkey -> connection
        self.connection_locks: Dict[str, asyncio.Lock] = {}    # validator_hotkey -> lock
        self.connection_tasks: Dict[str, asyncio.Task] = {}    # validator_hotkey -> task
        
        # Event handlers for incoming messages
        self.confirmation_handlers: List[callable] = []
        self.game_update_handlers: List[callable] = []
        
        # Connection states
        self.is_running = False
        self.validator_endpoints: Dict[str, str] = {}  # validator_hotkey -> ws_endpoint
        self.reconnect_intervals: Dict[str, int] = {}  # validator_hotkey -> seconds
        self.max_reconnect_interval = 300            # 5 minutes maximum backoff
        self.base_reconnect_interval = 5             # 5 seconds initial backoff
        
        # Heartbeat settings
        self.heartbeat_interval = 60                 # 1 minute
        self.heartbeat_task = None
        
        # Processing queue
        self.queue = asyncio.Queue()
        self.queue_processor_task = None
    
    async def start(self):
        """Start the WebSocket manager and establish connections to validators."""
        if self.is_running:
            return
        
        bt.logging.info("Starting WebSocket manager")
        self.is_running = True
        
        # Start background tasks
        self.queue_processor_task = asyncio.create_task(self._process_queue())
        self.heartbeat_task = asyncio.create_task(self._send_heartbeats())
        
        # Discover validator endpoints
        await self.discover_validator_endpoints()
        
        # Connect to all validators
        for validator_hotkey, ws_endpoint in self.validator_endpoints.items():
            self.connection_locks[validator_hotkey] = asyncio.Lock()
            self._start_connection(validator_hotkey, ws_endpoint)
    
    def _start_connection(self, validator_hotkey: str, ws_endpoint: str):
        """Start a connection task for a validator."""
        if validator_hotkey in self.connection_tasks and not self.connection_tasks[validator_hotkey].done():
            # Already connecting/connected
            return
        
        # Start new connection
        self.reconnect_intervals.setdefault(validator_hotkey, self.base_reconnect_interval)
        self.connection_tasks[validator_hotkey] = asyncio.create_task(
            self._maintain_connection(validator_hotkey, ws_endpoint)
        )
    
    async def discover_validator_endpoints(self):
        """Discover WebSocket endpoints for all validators in the metagraph."""
        bt.logging.info("Discovering validator WebSocket endpoints")
        
        for uid, axon in enumerate(self.metagraph.axons):
            # Skip own axon
            if axon.hotkey == self.miner_hotkey:
                continue
            
            # Construct WebSocket URL (default to port 8000 for WebSockets)
            ip = axon.ip if axon.ip != "0.0.0.0" else "127.0.0.1"
            port = axon.port
            
            # WebSocket connection uses the HTTP port with /ws path
            ws_endpoint = f"ws://{ip}:{port}/ws"
            self.validator_endpoints[axon.hotkey] = ws_endpoint
            
            bt.logging.debug(f"Discovered validator {axon.hotkey} at {ws_endpoint}")
    
    async def _maintain_connection(self, validator_hotkey: str, ws_endpoint: str):
        """
        Maintain a persistent connection to a validator.
        
        This coroutine runs in a loop, trying to reconnect if the connection fails.
        """
        while self.is_running:
            try:
                # Try to connect
                bt.logging.debug(f"Connecting to validator {validator_hotkey} at {ws_endpoint}")
                
                async with websockets.connect(ws_endpoint) as websocket:
                    # Update connection state
                    async with self.connection_locks[validator_hotkey]:
                        self.connections[validator_hotkey] = WebSocketConnection(
                            hotkey=validator_hotkey,
                            role="validator",
                            websocket=websocket
                        )
                    
                    # Reset reconnect interval on successful connection
                    self.reconnect_intervals[validator_hotkey] = self.base_reconnect_interval
                    
                    # Initial authentication handshake
                    await self._authenticate(validator_hotkey, websocket)
                    
                    # Process messages
                    try:
                        while self.is_running:
                            message_text = await websocket.recv()
                            await self._handle_message(validator_hotkey, message_text)
                    except ConnectionClosed:
                        bt.logging.warning(f"Connection to validator {validator_hotkey} closed")
                    finally:
                        # Clean up connection
                        async with self.connection_locks[validator_hotkey]:
                            if validator_hotkey in self.connections:
                                self.connections[validator_hotkey].close()
                                self.connections.pop(validator_hotkey, None)
            
            except Exception as e:
                bt.logging.error(f"Error connecting to validator {validator_hotkey}: {str(e)}")
                bt.logging.debug(traceback.format_exc())
            
            # Back off before reconnecting
            current_interval = self.reconnect_intervals[validator_hotkey]
            bt.logging.debug(f"Reconnecting to {validator_hotkey} in {current_interval} seconds")
            await asyncio.sleep(current_interval)
            
            # Exponential backoff
            self.reconnect_intervals[validator_hotkey] = min(
                current_interval * 2,
                self.max_reconnect_interval
            )
    
    async def _authenticate(self, validator_hotkey: str, websocket):
        """
        Authenticate with the validator using the miner's hotkey.
        
        Args:
            validator_hotkey: The validator's hotkey
            websocket: The WebSocket connection
        """
        # Create handshake message
        auth_message = WebSocketMessage.create(
            sender_hotkey=self.miner_hotkey,
            message_type="handshake"
        )
        
        # Send authentication message
        await websocket.send(auth_message.json())
        
        # Wait for authentication response
        response_text = await websocket.recv()
        response = json.loads(response_text)
        
        if response.get("message_type") == "handshake_response" and response.get("success"):
            bt.logging.info(f"Successfully authenticated with validator {validator_hotkey}")
            return True
        else:
            bt.logging.error(f"Authentication failed with validator {validator_hotkey}")
            raise ValueError("Authentication failed")
    
    async def _handle_message(self, validator_hotkey: str, message_text: str):
        """
        Handle an incoming WebSocket message.
        
        Args:
            validator_hotkey: The validator's hotkey
            message_text: The raw message text
        """
        try:
            # Parse message
            message_data = json.loads(message_text)
            message_type = message_data.get("message_type")
            
            # Process based on message type
            if message_type == "confirmation":
                # Handle confirmation message
                confirmation = ConfirmationMessage(**message_data)
                
                # Call all registered confirmation handlers
                for handler in self.confirmation_handlers:
                    asyncio.create_task(handler(validator_hotkey, confirmation))
            
            elif message_type == "game_update":
                # Handle game update message
                game_update = GameUpdateMessage(**message_data)
                
                # Call all registered game update handlers
                for handler in self.game_update_handlers:
                    asyncio.create_task(handler(validator_hotkey, game_update))
            
            elif message_type == "heartbeat":
                # Update connection heartbeat timestamp
                if validator_hotkey in self.connections:
                    self.connections[validator_hotkey].update_heartbeat()
            
            else:
                bt.logging.warning(f"Received unknown message type: {message_type}")
        
        except Exception as e:
            bt.logging.error(f"Error handling message from {validator_hotkey}: {str(e)}")
            bt.logging.debug(f"Message: {message_text}")
            bt.logging.debug(traceback.format_exc())
    
    async def _send_heartbeats(self):
        """Periodically send heartbeat messages to all connected validators."""
        while self.is_running:
            try:
                # Wait for the heartbeat interval
                await asyncio.sleep(self.heartbeat_interval)
                
                # Send heartbeats to all connections
                for validator_hotkey, connection in list(self.connections.items()):
                    if connection.active:
                        try:
                            heartbeat = HeartbeatMessage.create(self.miner_hotkey)
                            await connection.websocket.send(heartbeat.json())
                            connection.update_heartbeat()
                            bt.logging.debug(f"Sent heartbeat to {validator_hotkey}")
                        except Exception as e:
                            bt.logging.warning(f"Failed to send heartbeat to {validator_hotkey}: {str(e)}")
            
            except asyncio.CancelledError:
                break
            except Exception as e:
                bt.logging.error(f"Error in heartbeat task: {str(e)}")
                bt.logging.debug(traceback.format_exc())
                await asyncio.sleep(10)  # Wait a bit before retrying
    
    async def _process_queue(self):
        """Process the outgoing message queue."""
        while self.is_running:
            try:
                # Get next message from queue
                validator_hotkey, message = await self.queue.get()
                
                # Send message
                await self._send_message(validator_hotkey, message)
                
                # Mark task as done
                self.queue.task_done()
            
            except asyncio.CancelledError:
                break
            except Exception as e:
                bt.logging.error(f"Error processing message queue: {str(e)}")
                bt.logging.debug(traceback.format_exc())
                await asyncio.sleep(1)  # Wait a bit before retrying
    
    async def _send_message(self, validator_hotkey: str, message: WebSocketMessage):
        """
        Send a message to a validator.
        
        Args:
            validator_hotkey: The validator's hotkey
            message: The message to send
        """
        # Check if we have an active connection
        if (validator_hotkey in self.connections and 
            self.connections[validator_hotkey].active and 
            self.connections[validator_hotkey].websocket):
            
            try:
                # Send via WebSocket
                await self.connections[validator_hotkey].websocket.send(message.json())
                bt.logging.debug(f"Sent {message.message_type} message to {validator_hotkey}")
                return True
            
            except Exception as e:
                bt.logging.warning(f"Failed to send message to {validator_hotkey} via WebSocket: {str(e)}")
                # Fall through to HTTP fallback
        
        # WebSocket failed or not available, use HTTP fallback
        bt.logging.debug(f"Using HTTP fallback for {validator_hotkey}")
        # Implement HTTP fallback if needed
        return False
    
    async def send_prediction(self, prediction: TeamGamePrediction, validator_hotkeys: Optional[List[str]] = None):
        """
        Send a prediction to validators.
        
        Args:
            prediction: The prediction to send
            validator_hotkeys: Optional list of validator hotkeys to send to (default: all connected)
        """
        # Create prediction message
        prediction_message = PredictionMessage.create(
            sender_hotkey=self.miner_hotkey,
            prediction=prediction
        )
        
        # Determine target validators
        if validator_hotkeys is None:
            validator_hotkeys = list(self.validator_endpoints.keys())
        
        # Queue message for each validator
        for validator_hotkey in validator_hotkeys:
            await self.queue.put((validator_hotkey, prediction_message))
    
    def register_confirmation_handler(self, handler: callable):
        """
        Register a handler for confirmation messages.
        
        Args:
            handler: A coroutine function that takes (validator_hotkey, confirmation)
        """
        self.confirmation_handlers.append(handler)
    
    def register_game_update_handler(self, handler: callable):
        """
        Register a handler for game update messages.
        
        Args:
            handler: A coroutine function that takes (validator_hotkey, game_update)
        """
        self.game_update_handlers.append(handler)
    
    async def stop(self):
        """Stop the WebSocket manager and close all connections."""
        if not self.is_running:
            return
        
        bt.logging.info("Stopping WebSocket manager")
        self.is_running = False
        
        # Cancel background tasks
        if self.queue_processor_task:
            self.queue_processor_task.cancel()
        
        if self.heartbeat_task:
            self.heartbeat_task.cancel()
        
        # Close all connections
        for validator_hotkey, connection in list(self.connections.items()):
            connection.close()
            if validator_hotkey in self.connection_tasks:
                self.connection_tasks[validator_hotkey].cancel()
        
        # Wait for all tasks to complete
        pending_tasks = [
            task for task in [self.queue_processor_task, self.heartbeat_task] + list(self.connection_tasks.values())
            if task is not None and not task.done()
        ]
        
        if pending_tasks:
            await asyncio.gather(*pending_tasks, return_exceptions=True)
        
        # Clear connection data
        self.connections.clear()
        self.connection_tasks.clear()
        self.connection_locks.clear()
        
        bt.logging.info("WebSocket manager stopped") 