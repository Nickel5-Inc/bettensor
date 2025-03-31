"""
WebSocket Handler for Bettensor Validators

This module provides the WebSocket server functionality for validators to accept
and manage persistent connections from miners for real-time communication.
"""

import asyncio
import json
import time
import traceback
from collections import defaultdict
from datetime import datetime, timezone
from typing import Dict, List, Any, Callable, Optional, Set

import bittensor as bt
import websockets
from fastapi import Depends, FastAPI, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.responses import JSONResponse
from websockets.exceptions import ConnectionClosedError, ConnectionClosedOK
from pydantic import BaseModel
from contextlib import asynccontextmanager
from bittensor_wallet import Wallet


from bettensor.protocol import (
    ConfirmationMessage, 
    GameUpdateMessage, 
    HeartbeatMessage, 
    PredictionMessage, 
    TeamGamePrediction, 
    WebSocketConnection, 
    WebSocketMessage
)

class WebSocketMessage(BaseModel):
    """Base class for WebSocket messages"""
    type: str
    sender: str
    data: Dict[str, Any]
    timestamp: Optional[str] = None
    signature: Optional[str] = None
    nonce: Optional[str] = None
    
    def __init__(self, **data):
        # Set timestamp if not provided
        if 'timestamp' not in data:
            data['timestamp'] = datetime.now(timezone.utc).isoformat()
        super().__init__(**data)
    
    @classmethod
    def create(cls, wallet: "Wallet", data: Dict[str, Any], message_type: str):
        """
        Create a signed WebSocket message
        
        Args:
            wallet: The wallet to sign with
            data: The message data
            message_type: The type of message
            
        Returns:
            A signed WebSocket message
        """
        # Create the message without signature
        message = {
            "type": message_type,
            "sender": wallet.hotkey.ss58_address,
            "data": data,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "nonce": str(time.time() * 1000)  # Simple nonce based on current time in milliseconds
        }
        
        # Create a serialized message for signing (excluding signature field)
        message_to_sign = json.dumps(message, sort_keys=True)
        
        # Sign the message
        signature = wallet.hotkey.sign(message_to_sign)
        message["signature"] = signature.hex()
        
        return cls(**message)
    
    def verify_signature(self) -> bool:
        """
        Verify the message signature
        
        Returns:
            True if signature is valid, False otherwise
        """
        if not self.signature:
            return False
            
        # Create a copy of the message without signature
        message_to_verify = self.dict()
        message_to_verify.pop("signature", None)
        
        # Serialize for verification (must match signing process)
        message_str = json.dumps(message_to_verify, sort_keys=True)
        
        try:
            # Verify using Bittensor's verification mechanism
            return Wallet.verify_signature(
                ss58_address=self.sender,
                message=message_str,
                signature=bytes.fromhex(self.signature)
            )
        except Exception as e:
            bt.logging.error(f"Error verifying signature: {str(e)}")
            return False

class PredictionMessage(WebSocketMessage):
    """WebSocket message for sending predictions from miners to validators"""
    pass

class ConfirmationMessage(WebSocketMessage):
    """WebSocket message for sending prediction confirmations from validators to miners"""
    pass

class GameUpdateMessage(WebSocketMessage):
    """WebSocket message for sending game updates from validators to miners"""
    pass

class HeartbeatMessage(WebSocketMessage):
    """WebSocket message for keeping connections alive"""
    pass

class WebSocketClient:
    """Represents a connected WebSocket client (miner)"""
    def __init__(self, websocket: WebSocket, hotkey: str, uid: Optional[int] = None):
        self.websocket = websocket
        self.hotkey = hotkey
        self.uid = uid
        self.connected_at = datetime.now(timezone.utc)
        self.last_activity = datetime.now(timezone.utc)
        self.messages_received = 0
        self.messages_sent = 0
        self.is_active = True
        self.pending_messages = asyncio.Queue()
        self.pending_priorities = asyncio.PriorityQueue()  # Priority queue for important messages
        self.sender_task = None
        self.priority_sender_task = None
        self.last_received_nonce = set()  # Track received nonces to prevent replay attacks
        
    def update_activity(self):
        """Update the last activity timestamp"""
        self.last_activity = datetime.now(timezone.utc)
        
    async def send_message(self, message: dict):
        """Queue a message to be sent to the client"""
        await self.pending_messages.put(message)
        
    async def send_with_priority(self, message: dict, priority: int = 1):
        """Queue a message with priority (lower number = higher priority)"""
        await self.pending_priorities.put((priority, message))
        
    async def sender_loop(self):
        """Background task that sends queued messages to the client"""
        try:
            while True:
                try:
                    # First check priority queue
                    if not self.pending_priorities.empty():
                        _, message = await self.pending_priorities.get()
                    else:
                        # If no priority messages, get from regular queue
                        message = await self.pending_messages.get()
                    
                    # Send the message
                    await self.websocket.send_json(message)
                    self.messages_sent += 1
                    self.update_activity()
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    bt.logging.error(f"Error sending message to {self.hotkey}: {str(e)}")
                finally:
                    # Mark as done regardless of success/failure
                    if not self.pending_priorities.empty():
                        self.pending_priorities.task_done()
                    else:
                        self.pending_messages.task_done()
        except asyncio.CancelledError:
            bt.logging.debug(f"Sender task for {self.hotkey} cancelled")
        except Exception as e:
            bt.logging.error(f"Error in sender loop for {self.hotkey}: {str(e)}")

class ValidatorWebSocketManager:
    """
    Manages WebSocket connections for the validator
    
    This class handles:
    1. Connection management
    2. Message routing
    3. Authentication
    4. Heartbeats
    """
    def __init__(self, validator):
        """
        Initialize the WebSocket manager
        
        Args:
            validator: The validator instance this manager belongs to
        """
        self.validator = validator
        self.wallet = validator.wallet
        self.validator_hotkey = validator.wallet.hotkey.ss58_address
        self.clients: Dict[str, WebSocketClient] = {}  # Maps hotkey -> WebSocketClient
        self.handlers = {}  # Maps message type -> handler function
        self.heartbeat_interval = getattr(validator.config, "websocket.heartbeat_interval", 60)
        self.connection_timeout = getattr(validator.config, "websocket.connection_timeout", 300)
        self.is_running_flag = False
        self.cleanup_task = None
        self.heartbeat_task = None
        self.start_time = None
        self.nonce_expiration = 300  # Nonces expire after 5 minutes (prevents replay)
        self.recent_nonces = {}  # Maps hotkey -> {nonce -> timestamp}
        
        # Create FastAPI app
        self.app = FastAPI()
        self._setup_routes()
    
    def _setup_routes(self):
        """Set up the WebSocket routes for the FastAPI app."""
        @self.app.websocket("/ws")
        async def websocket_endpoint(websocket: WebSocket):
            await self.handle_connection(websocket)
    
    async def handle_connection(self, websocket: WebSocket):
        """
        Handle a new WebSocket connection
        
        Args:
            websocket: The WebSocket connection
        """
        await websocket.accept()
        client_hotkey = None
        
        try:
            # Receive authentication message
            auth_message = await websocket.receive_json()
            
            # Validate authentication message
            if not self.validate_auth_message(auth_message):
                await websocket.send_json({
                    "status": "error",
                    "message": "Invalid authentication"
                })
                await websocket.close(code=1008, reason="Invalid authentication")
                return
                
            client_hotkey = auth_message['hotkey']
            
            # Validate the hotkey is registered in the metagraph
            if client_hotkey not in self.validator.metagraph.hotkeys:
                await websocket.send_json({
                    "status": "error", 
                    "message": "Hotkey not registered in metagraph"
                })
                await websocket.close(code=1008, reason="Unregistered hotkey")
                return
                
            # Get the UID for this hotkey
            try:
                client_uid = self.validator.metagraph.hotkeys.index(client_hotkey)
            except ValueError:
                client_uid = None
                
            # Check if this hotkey is already connected
            if client_hotkey in self.clients:
                # Close the old connection
                old_client = self.clients[client_hotkey]
                if old_client.sender_task:
                    old_client.sender_task.cancel()
                await old_client.websocket.close(code=1000, reason="New connection established")
                bt.logging.info(f"Replaced existing connection for {client_hotkey}")
                
            # Create new client
            client = WebSocketClient(websocket, client_hotkey, client_uid)
            
            # Start the sender task
            client.sender_task = asyncio.create_task(client.sender_loop())
            
            # Store the client
            self.clients[client_hotkey] = client
            
            # Send welcome message
            # Create a signed welcome message
            welcome_message = WebSocketMessage.create(
                wallet=self.wallet,
                data={
                    "message": "Connection established",
                    "status": "success"
                },
                message_type="welcome"
            )
            
            await client.send_message(welcome_message.dict())
            
            bt.logging.info(f"WebSocket connection established with miner {client_hotkey} (UID: {client_uid})")
            
            # Handle messages from this client
            while True:
                message_raw = await websocket.receive_text()
                client.update_activity()
                client.messages_received += 1
                
                try:
                    message_data = json.loads(message_raw)
                    
                    # Validate message signature and structure
                    if not self.validate_message(message_data, client_hotkey):
                        await client.send_message({
                            "type": "error",
                            "message": "Invalid message signature or structure",
                            "timestamp": datetime.now(timezone.utc).isoformat()
                        })
                        continue
                    
                    # Handle different message types
                    message_type = message_data.get('type')
                    
                    if message_type == 'heartbeat':
                        # For heartbeat messages, just acknowledge receipt
                        heartbeat_ack = WebSocketMessage.create(
                            wallet=self.wallet,
                            data={},
                            message_type="heartbeat_ack"
                        )
                        await client.send_message(heartbeat_ack.dict())
                    elif message_type == 'prediction':
                        # Handle prediction message with the registered handler
                        if 'prediction' in self.handlers:
                            prediction_data = message_data.get('data', {})
                            # Call the prediction handler
                            asyncio.create_task(
                                self.handlers['prediction'](client_hotkey, prediction_data)
                            )
                        else:
                            bt.logging.warning(f"No handler registered for prediction messages")
                            error_message = WebSocketMessage.create(
                                wallet=self.wallet,
                                data={"message": "No handler registered for predictions"},
                                message_type="error"
                            )
                            await client.send_message(error_message.dict())
                    else:
                        # For other message types, look for a registered handler
                        if message_type in self.handlers:
                            # Create task to handle the message asynchronously
                            asyncio.create_task(
                                self.handlers[message_type](websocket, message_data)
                            )
                        else:
                            bt.logging.warning(f"Received unknown message type: {message_type}")
                            error_message = WebSocketMessage.create(
                                wallet=self.wallet,
                                data={"message": f"Unknown message type: {message_type}"},
                                message_type="error"
                            )
                            await client.send_message(error_message.dict())
                            
                except json.JSONDecodeError:
                    bt.logging.warning(f"Received invalid JSON from {client_hotkey}")
                    error_message = WebSocketMessage.create(
                        wallet=self.wallet,
                        data={"message": "Invalid JSON"},
                        message_type="error"
                    )
                    await client.send_message(error_message.dict())
                except (ConnectionClosedError, ConnectionClosedOK, WebSocketDisconnect):
                    # Connection closed, break out of the loop
                    break
                except Exception as e:
                    bt.logging.error(f"Error handling message from {client_hotkey}: {str(e)}")
                    bt.logging.error(traceback.format_exc())
                    
        except (ConnectionClosedError, ConnectionClosedOK, WebSocketDisconnect):
            # Normal disconnection
            pass
        except Exception as e:
            bt.logging.error(f"WebSocket error: {str(e)}")
            bt.logging.error(traceback.format_exc())
        finally:
            # Clean up client resources
            if client_hotkey and client_hotkey in self.clients:
                client = self.clients[client_hotkey]
                if client.sender_task:
                    client.sender_task.cancel()
                self.clients.pop(client_hotkey)
                
            bt.logging.info(f"WebSocket connection closed for {client_hotkey or 'unknown'}")
    
    async def start(self):
        """Start the WebSocket manager"""
        bt.logging.info("Starting WebSocket manager")
        self.is_running_flag = True
        self.start_time = time.time()
        self.cleanup_task = asyncio.create_task(self.cleanup_stale_connections())
        self.heartbeat_task = asyncio.create_task(self.send_heartbeats())
        self.nonce_cleanup_task = asyncio.create_task(self.cleanup_nonces())
        bt.logging.info(f"WebSocket manager started with heartbeat interval: {self.heartbeat_interval}s")
        
    async def stop(self):
        """Stop the WebSocket manager and disconnect all clients"""
        bt.logging.info("Stopping WebSocket manager")
        self.is_running_flag = False
        
        # Cancel background tasks
        for task_name, task in [
            ("cleanup", self.cleanup_task),
            ("heartbeat", self.heartbeat_task),
            ("nonce_cleanup", getattr(self, "nonce_cleanup_task", None))
        ]:
            if task:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
                except Exception as e:
                    bt.logging.error(f"Error cancelling {task_name} task: {str(e)}")
            
        # Close all client connections
        for hotkey, client in list(self.clients.items()):
            try:
                if client.sender_task:
                    client.sender_task.cancel()
                await client.websocket.close(code=1000, reason="Server shutting down")
                bt.logging.info(f"Closed connection for {hotkey}")
            except Exception as e:
                bt.logging.error(f"Error closing connection for {hotkey}: {str(e)}")
                
        self.clients.clear()
        bt.logging.info("WebSocket manager stopped")
    
    async def restart(self):
        """Restart the WebSocket manager"""
        bt.logging.info("Restarting WebSocket manager")
        await self.stop()
        await asyncio.sleep(1)  # Brief pause before restart
        await self.start()
        bt.logging.info("WebSocket manager restarted")
        
    def is_running(self) -> bool:
        """Check if the WebSocket manager is running"""
        return self.is_running_flag
    
    def get_uptime_seconds(self) -> int:
        """Get the uptime of the WebSocket manager in seconds"""
        if not self.start_time:
            return 0
        return int(time.time() - self.start_time)
    
    def get_active_connection_count(self) -> int:
        """Get the number of active WebSocket connections"""
        return sum(1 for client in self.clients.values() if client.is_active)
    
    def get_connected_hotkeys(self) -> List[str]:
        """Get a list of all connected miner hotkeys"""
        return list(self.clients.keys())
    
    def check_health(self) -> bool:
        """Check if the WebSocket manager is healthy"""
        # Check if the manager is running
        if not self.is_running():
            return False
            
        # Check if background tasks are running
        if not self.cleanup_task or self.cleanup_task.done():
            return False
            
        if not self.heartbeat_task or self.heartbeat_task.done():
            return False
            
        # Consider the manager healthy if running and tasks are active
        return True
    
    def validate_auth_message(self, auth_message: dict) -> bool:
        """
        Validate an authentication message
        
        Args:
            auth_message: The authentication message to validate
            
        Returns:
            True if valid, False otherwise
        """
        # Check required fields
        if not isinstance(auth_message, dict):
            return False
            
        required_fields = ['hotkey', 'signature', 'timestamp', 'nonce']
        if not all(field in auth_message for field in required_fields):
            return False
            
        # Check timestamp is not too old (within 5 minutes)
        try:
            auth_time = datetime.fromisoformat(auth_message['timestamp'])
            now = datetime.now(timezone.utc)
            if abs((now - auth_time).total_seconds()) > 300:  # 5 minutes
                bt.logging.warning(f"Authentication timestamp too old: {auth_message['timestamp']}")
                return False
        except (ValueError, TypeError):
            return False
            
        # Create message for verification (excluding signature)
        message_to_verify = {k: v for k, v in auth_message.items() if k != 'signature'}
        message_str = json.dumps(message_to_verify, sort_keys=True)
        
        # Verify signature
        try:
            is_valid = Wallet.verify_signature(
                ss58_address=auth_message['hotkey'],
                message=message_str,
                signature=bytes.fromhex(auth_message['signature'])
            )
            if not is_valid:
                bt.logging.warning(f"Invalid signature for hotkey: {auth_message['hotkey']}")
            return is_valid
        except Exception as e:
            bt.logging.error(f"Error verifying auth signature: {str(e)}")
            return False
    
    def validate_message(self, message: dict, client_hotkey: str) -> bool:
        """
        Validate a WebSocket message
        
        Args:
            message: The message to validate
            client_hotkey: The expected sender hotkey
            
        Returns:
            True if valid, False otherwise
        """
        # Check required fields
        required_fields = ['type', 'sender', 'data', 'timestamp', 'signature', 'nonce']
        if not all(field in message for field in required_fields):
            bt.logging.warning(f"Message missing required fields from {client_hotkey}")
            return False
            
        # Verify sender matches expected client
        if message['sender'] != client_hotkey:
            bt.logging.warning(f"Message sender mismatch: expected {client_hotkey}, got {message['sender']}")
            return False
            
        # Check for replay attacks
        nonce = message.get('nonce')
        if nonce in self.recent_nonces.get(client_hotkey, set()):
            bt.logging.warning(f"Duplicate nonce detected from {client_hotkey}: {nonce}")
            return False
            
        # Verify signature
        message_obj = WebSocketMessage(**message)
        if not message_obj.verify_signature():
            bt.logging.warning(f"Invalid message signature from {client_hotkey}")
            return False
            
        # Store nonce to prevent replays
        if client_hotkey not in self.recent_nonces:
            self.recent_nonces[client_hotkey] = set()
        self.recent_nonces[client_hotkey].add(nonce)
        
        return True
    
    async def cleanup_nonces(self):
        """Periodically clean up expired nonces to prevent memory leaks"""
        try:
            while self.is_running():
                # Get current time
                now = time.time()
                
                # Check each client's nonces
                for hotkey in list(self.recent_nonces.keys()):
                    # Clean up if client is no longer connected
                    if hotkey not in self.clients:
                        self.recent_nonces.pop(hotkey, None)
                        continue
                    
                # Sleep before next check
                await asyncio.sleep(60)  # Check every minute
                
        except asyncio.CancelledError:
            bt.logging.debug("Nonce cleanup task cancelled")
        except Exception as e:
            bt.logging.error(f"Error in nonce cleanup task: {str(e)}")
    
    async def cleanup_stale_connections(self):
        """Periodically check and close inactive connections"""
        try:
            while self.is_running():
                now = datetime.now(timezone.utc)
                stale_clients = []
                
                # Find stale connections
                for hotkey, client in self.clients.items():
                    inactive_seconds = (now - client.last_activity).total_seconds()
                    if inactive_seconds > self.connection_timeout:
                        stale_clients.append((hotkey, client))
                        
                # Close stale connections
                for hotkey, client in stale_clients:
                    try:
                        bt.logging.info(f"Closing stale connection for {hotkey} (inactive for {int(inactive_seconds)}s)")
                        if client.sender_task:
                            client.sender_task.cancel()
                        await client.websocket.close(code=1000, reason="Connection timeout")
                        self.clients.pop(hotkey, None)
                    except Exception as e:
                        bt.logging.error(f"Error closing stale connection for {hotkey}: {str(e)}")
                        
                # Sleep before next check
                await asyncio.sleep(60)  # Check every minute
                
        except asyncio.CancelledError:
            bt.logging.debug("Cleanup task cancelled")
        except Exception as e:
            bt.logging.error(f"Error in cleanup task: {str(e)}")
            bt.logging.error(traceback.format_exc())
    
    async def send_heartbeats(self):
        """Periodically send heartbeat messages to all clients"""
        try:
            while self.is_running():
                # Create heartbeat message
                heartbeat = WebSocketMessage.create(
                    wallet=self.wallet,
                    data={},
                    message_type="heartbeat"
                )
                
                # Send to all clients
                for hotkey, client in list(self.clients.items()):
                    try:
                        await client.send_message(heartbeat.dict())
                    except Exception as e:
                        bt.logging.error(f"Error sending heartbeat to {hotkey}: {str(e)}")
                        
                # Sleep until next heartbeat
                await asyncio.sleep(self.heartbeat_interval)
                
        except asyncio.CancelledError:
            bt.logging.debug("Heartbeat task cancelled")
        except Exception as e:
            bt.logging.error(f"Error in heartbeat task: {str(e)}")
            bt.logging.error(traceback.format_exc())
    
    async def broadcast(self, message_data: dict, message_type: str = None):
        """
        Broadcast a message to all connected clients
        
        Args:
            message_data: The message data to broadcast
            message_type: Optional message type override
        """
        if not self.is_running():
            bt.logging.warning("Attempted to broadcast while WebSocket manager is not running")
            return
        
        # Create signed message if message_type is provided
        if message_type:
            message = WebSocketMessage.create(
                wallet=self.wallet,
                data=message_data,
                message_type=message_type
            ).dict()
        else:
            # Assume message_data is already a complete message
            message = message_data
            
        # Send to all clients
        for hotkey, client in list(self.clients.items()):
            try:
                await client.send_message(message)
            except Exception as e:
                bt.logging.error(f"Error broadcasting to {hotkey}: {str(e)}")
    
    async def send_confirmation(self, miner_hotkey: str, prediction_id: str, success: bool, message: str = "", miner_stats: dict = None):
        """
        Send a confirmation message to a specific miner
        
        Args:
            miner_hotkey: The miner's hotkey
            prediction_id: The ID of the prediction being confirmed
            success: Whether the prediction was successful
            message: Error message (if any)
            miner_stats: Optional miner statistics to include
        """
        if not self.is_running():
            bt.logging.warning("Attempted to send confirmation while WebSocket manager is not running")
            return False
            
        # Check if the miner is connected
        if miner_hotkey not in self.clients:
            bt.logging.warning(f"Attempted to send confirmation to disconnected miner: {miner_hotkey}")
            return False
            
        try:
            # Create confirmation data
            confirmation_data = {
                "prediction_id": prediction_id,
                "status": "success" if success else "error",
                "message": message,
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
            
            # Add miner stats if provided
            if miner_stats:
                confirmation_data["miner_stats"] = miner_stats
                
            # Create signed confirmation message
            confirmation = WebSocketMessage.create(
                wallet=self.wallet,
                data=confirmation_data,
                message_type="confirmation"
            )
                
            # Send confirmation with high priority (1)
            await self.clients[miner_hotkey].send_with_priority(confirmation.dict(), priority=1)
            bt.logging.info(f"Sent confirmation for prediction {prediction_id} to {miner_hotkey} via WebSocket")
            return True
            
        except Exception as e:
            bt.logging.error(f"Error sending confirmation to {miner_hotkey}: {str(e)}")
            return False
    
    async def broadcast_game_updates(self, games: List[dict]):
        """
        Broadcast game updates to all connected miners
        
        Args:
            games: List of game data to broadcast
        """
        if not self.is_running():
            bt.logging.warning("Attempted to broadcast game updates while WebSocket manager is not running")
            return False
            
        if not self.clients:
            bt.logging.info("No connected miners to receive game updates")
            return False
            
        try:
            # Generate hash of current game data to avoid redundant broadcasts
            import hashlib
            game_hash = hashlib.md5(json.dumps(games, sort_keys=True).encode()).hexdigest()
            
            # Check if this is the same as the last broadcast
            if hasattr(self, 'last_game_hash') and self.last_game_hash == game_hash:
                bt.logging.info("Skipping game update broadcast - data unchanged")
                return True
                
            # Create game update message
            game_update = WebSocketMessage.create(
                wallet=self.wallet,
                data={
                    "games": games,
                    "timestamp": datetime.now(timezone.utc).isoformat()
                },
                message_type="game_update"
            )
            
            # Broadcast to all clients with medium priority (2)
            for hotkey, client in list(self.clients.items()):
                try:
                    await client.send_with_priority(game_update.dict(), priority=2)
                except Exception as e:
                    bt.logging.error(f"Error sending game update to {hotkey}: {str(e)}")
            
            # Save hash for comparison
            self.last_game_hash = game_hash
            
            bt.logging.info(f"Broadcast game updates for {len(games)} games to {len(self.clients)} miners")
            return True
            
        except Exception as e:
            bt.logging.error(f"Error broadcasting game updates: {str(e)}")
            return False
    
    async def integrate_with_axon(self, app: FastAPI):
        """
        Integrate WebSocket routes with the validator's Axon FastAPI app
        
        Args:
            app: The FastAPI app from the validator's Axon
        """
        # Mount WebSocket endpoints to the Axon's FastAPI app
        app.add_websocket_route("/ws", self.handle_connection)
        
        # Add health check endpoint
        @app.get("/ws/health")
        async def websocket_health():
            return JSONResponse({
                "status": "online" if self.is_running() else "offline",
                "connections": len(self.clients),
                "active_connections": self.get_active_connection_count(),
                "uptime": self.get_uptime_seconds()
            }) 