import asyncio
import json
import time
import traceback
from typing import Dict, List, Any, Callable, Optional, Union
from datetime import datetime, timezone
import websockets
from websockets.exceptions import ConnectionClosedError, ConnectionClosedOK
import bittensor as bt
from bittensor.wallet import Wallet
import uuid

class WebSocketMessage:
    """Base class for WebSocket messages"""
    def __init__(self, type: str, sender: str, data: Dict[str, Any], timestamp: Optional[str] = None, 
                 signature: Optional[str] = None, nonce: Optional[str] = None):
        self.type = type
        self.sender = sender
        self.data = data
        self.timestamp = timestamp or datetime.now(timezone.utc).isoformat()
        self.signature = signature
        self.nonce = nonce or str(time.time() * 1000)  # Simple nonce based on current time in milliseconds
        
    def to_dict(self) -> Dict[str, Any]:
        """Convert message to dictionary"""
        return {
            "type": self.type,
            "sender": self.sender,
            "data": self.data,
            "timestamp": self.timestamp,
            "nonce": self.nonce,
            "signature": self.signature
        }
    
    @classmethod
    def from_dict(cls, message_dict: Dict[str, Any]) -> 'WebSocketMessage':
        """Create message from dictionary"""
        return cls(
            type=message_dict["type"],
            sender=message_dict["sender"],
            data=message_dict["data"],
            timestamp=message_dict.get("timestamp"),
            signature=message_dict.get("signature"),
            nonce=message_dict.get("nonce")
        )
    
    @classmethod
    def create_signed(cls, wallet: Wallet, message_type: str, data: Dict[str, Any]) -> 'WebSocketMessage':
        """Create a signed message"""
        # Create message without signature
        message = cls(
            type=message_type,
            sender=wallet.hotkey.ss58_address,
            data=data
        )
        
        # Create a message for signing (excluding signature field)
        message_to_sign = {
            "type": message.type,
            "sender": message.sender,
            "data": message.data,
            "timestamp": message.timestamp,
            "nonce": message.nonce
        }
        
        # Sort keys for consistent serialization
        message_str = json.dumps(message_to_sign, sort_keys=True)
        
        # Sign the message
        signature = wallet.hotkey.sign(message_str)
        message.signature = signature.hex()
        
        return message
    
    def verify(self) -> bool:
        """Verify the message signature"""
        if not self.signature:
            return False
            
        # Create a copy of the message without signature
        message_to_verify = {
            "type": self.type,
            "sender": self.sender,
            "data": self.data,
            "timestamp": self.timestamp,
            "nonce": self.nonce
        }
        
        # Sort keys for consistent serialization
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
    @classmethod
    def create(cls, wallet: Wallet, prediction_data: Dict[str, Any]) -> 'PredictionMessage':
        return WebSocketMessage.create_signed(wallet, "prediction", prediction_data)

class MinerWebSocketManager:
    """
    Manages WebSocket connections from a miner to validators
    
    This class handles:
    1. Connecting to validators
    2. Authentication
    3. Sending predictions
    4. Receiving confirmations and game updates
    5. Automatic reconnection
    """
    def __init__(self, miner_hotkey: str, metagraph, timeout: int = 30):
        """
        Initialize the WebSocket manager
        
        Args:
            miner_hotkey: The miner's hotkey
            metagraph: The metagraph
            timeout: Connection timeout in seconds
        """
        self.miner_hotkey = miner_hotkey
        self.metagraph = metagraph
        self.timeout = timeout
        self.connections: Dict[str, websockets.WebSocketClientProtocol] = {}
        self.connection_tasks: Dict[str, asyncio.Task] = {}
        self.handlers: Dict[str, Callable] = {}
        self.wallet = None  # Will be set later
        self.is_running = False
        self.validators_info: Dict[str, Dict[str, Any]] = {}  # Maps validator hotkey -> validator info
        
        # WebSocket configuration
        self.heartbeat_interval = 60  # 60 seconds
        self.reconnect_base_interval = 1  # 1 second
        self.max_reconnect_interval = 60  # 60 seconds
        self.heartbeat_tasks: Dict[str, asyncio.Task] = {}
        
        # Message tracking
        self.sent_predictions: Dict[str, Dict[str, Any]] = {}  # Maps prediction_id -> prediction
        self.connection_queue = asyncio.Queue()  # Queue for connecting to validators
        
    def set_wallet(self, wallet: Wallet):
        """Set the wallet for signing messages"""
        self.wallet = wallet
        
    def set_config(self, 
                  heartbeat_interval: int = None, 
                  reconnect_base_interval: int = None,
                  max_reconnect_interval: int = None):
        """Set configuration parameters"""
        if heartbeat_interval is not None:
            self.heartbeat_interval = heartbeat_interval
        if reconnect_base_interval is not None:
            self.reconnect_base_interval = reconnect_base_interval
        if max_reconnect_interval is not None:
            self.max_reconnect_interval = max_reconnect_interval
            
    def register_handler(self, message_type: str, handler: Callable):
        """
        Register a handler function for a specific message type
        
        Args:
            message_type: The type of message to handle
            handler: The handler function to call
        """
        self.handlers[message_type] = handler
        bt.logging.info(f"Registered handler for message type: {message_type}")
        
    def register_confirmation_handler(self, handler: Callable):
        """Register a handler for confirmation messages"""
        self.register_handler("confirmation", handler)
        
    def register_game_update_handler(self, handler: Callable):
        """Register a handler for game update messages"""
        self.register_handler("game_update", handler)
        
    async def start(self):
        """Start the WebSocket manager"""
        if self.is_running:
            bt.logging.warning("WebSocket manager is already running")
            return
            
        if not self.wallet:
            bt.logging.error("Wallet not set, cannot start WebSocket manager")
            return
            
        bt.logging.info("Starting WebSocket manager")
        self.is_running = True
        
        # Start the connection manager task
        asyncio.create_task(self.connection_manager())
        
        # Queue validators for connection
        await self.refresh_validators()
        
        bt.logging.info(f"WebSocket manager started with heartbeat interval: {self.heartbeat_interval}s")
        
    async def stop(self):
        """Stop the WebSocket manager and disconnect from all validators"""
        bt.logging.info("Stopping WebSocket manager")
        self.is_running = False
        
        # Cancel all connection tasks
        for validator_hotkey, task in list(self.connection_tasks.items()):
            if task and not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
                except Exception as e:
                    bt.logging.error(f"Error cancelling connection task for {validator_hotkey}: {str(e)}")
                    
        # Cancel all heartbeat tasks
        for validator_hotkey, task in list(self.heartbeat_tasks.items()):
            if task and not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
                except Exception as e:
                    bt.logging.error(f"Error cancelling heartbeat task for {validator_hotkey}: {str(e)}")
            
        # Close all connections
        for validator_hotkey, connection in list(self.connections.items()):
            try:
                await connection.close()
                bt.logging.info(f"Closed connection to validator {validator_hotkey}")
            except Exception as e:
                bt.logging.error(f"Error closing connection to validator {validator_hotkey}: {str(e)}")
                
        self.connections.clear()
        self.connection_tasks.clear()
        self.heartbeat_tasks.clear()
        bt.logging.info("WebSocket manager stopped")
        
    async def refresh_validators(self):
        """Refresh the list of validators and queue them for connection"""
        try:
            # Reset validators info
            self.validators_info = {}
            
            # Get all validator axons from metagraph
            for uid, hotkey in enumerate(self.metagraph.hotkeys):
                # Skip non-validators
                if not self.metagraph.validator_permit[uid]:
                    continue
                    
                # Get axon info
                axon_info = self.metagraph.axons[uid]
                ip = axon_info.ip
                port = axon_info.port
                
                # Skip validators with invalid IP
                if ip == "0.0.0.0":
                    continue
                    
                # Store validator info
                self.validators_info[hotkey] = {
                    "uid": uid,
                    "ip": ip,
                    "port": port,
                    "last_attempted_connection": 0,
                    "connection_failures": 0
                }
                
                # Queue this validator for connection
                await self.connection_queue.put(hotkey)
                
            bt.logging.info(f"Refreshed validators list, found {len(self.validators_info)} active validators")
        except Exception as e:
            bt.logging.error(f"Error refreshing validators: {str(e)}")
            bt.logging.error(traceback.format_exc())
            
    async def connection_manager(self):
        """Task that manages connections to validators"""
        try:
            while self.is_running:
                try:
                    # Get the next validator to connect to
                    if self.connection_queue.empty():
                        # If queue is empty, refresh validators and wait
                        await self.refresh_validators()
                        await asyncio.sleep(5)
                        continue
                        
                    validator_hotkey = await self.connection_queue.get()
                    
                    # Skip if we're already connected
                    if validator_hotkey in self.connections and not self.connections[validator_hotkey].closed:
                        continue
                        
                    # Skip if we don't have info for this validator
                    if validator_hotkey not in self.validators_info:
                        continue
                        
                    validator_info = self.validators_info[validator_hotkey]
                    now = time.time()
                    
                    # Check if we should attempt reconnection based on backoff
                    last_attempt = validator_info.get("last_attempted_connection", 0)
                    failures = validator_info.get("connection_failures", 0)
                    backoff = min(self.reconnect_base_interval * (2 ** failures), self.max_reconnect_interval)
                    
                    if now - last_attempt < backoff:
                        # Not time to reconnect yet, put back in queue with delay
                        await asyncio.sleep(1)
                        await self.connection_queue.put(validator_hotkey)
                        continue
                        
                    # Update last attempt time
                    validator_info["last_attempted_connection"] = now
                    
                    # Start connection task
                    task = asyncio.create_task(self.connect_to_validator(validator_hotkey))
                    self.connection_tasks[validator_hotkey] = task
                    
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    bt.logging.error(f"Error in connection manager: {str(e)}")
                    bt.logging.error(traceback.format_exc())
                    await asyncio.sleep(5)  # Wait before retrying
                    
        except asyncio.CancelledError:
            bt.logging.info("Connection manager task cancelled")
        except Exception as e:
            bt.logging.error(f"Fatal error in connection manager: {str(e)}")
            bt.logging.error(traceback.format_exc())
            
    async def connect_to_validator(self, validator_hotkey: str):
        """Connect to a validator"""
        try:
            # Get validator info
            validator_info = self.validators_info.get(validator_hotkey)
            if not validator_info:
                bt.logging.error(f"No info found for validator {validator_hotkey}")
                return
                
            ip = validator_info["ip"]
            port = validator_info["port"]
            
            # Construct WebSocket URI
            uri = f"ws://{ip}:{port}/ws"
            
            bt.logging.info(f"Connecting to validator {validator_hotkey} at {uri}")
            
            try:
                # Connect to the validator
                connection = await websockets.connect(uri, open_timeout=self.timeout)
                
                # Create authentication message
                auth_message = {
                    "hotkey": self.miner_hotkey,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "nonce": str(time.time() * 1000)
                }
                
                # Sign the message
                message_str = json.dumps(auth_message, sort_keys=True)
                signature = self.wallet.hotkey.sign(message_str)
                auth_message["signature"] = signature.hex()
                
                # Send authentication message
                await connection.send(json.dumps(auth_message))
                
                # Wait for authentication response
                response = await connection.recv()
                response_data = json.loads(response)
                
                if response_data.get("status") == "error":
                    error_message = response_data.get("message", "Unknown error")
                    bt.logging.error(f"Authentication failed with validator {validator_hotkey}: {error_message}")
                    
                    # Update failure count
                    validator_info["connection_failures"] = validator_info.get("connection_failures", 0) + 1
                    
                    # Close connection
                    await connection.close()
                    
                    # Requeue with backoff
                    await self.connection_queue.put(validator_hotkey)
                    return
                
                # Reset failure count on successful connection
                validator_info["connection_failures"] = 0
                
                # Store the connection
                self.connections[validator_hotkey] = connection
                
                # Start heartbeat task
                self.heartbeat_tasks[validator_hotkey] = asyncio.create_task(self.heartbeat_loop(validator_hotkey))
                
                bt.logging.info(f"Successfully connected to validator {validator_hotkey}")
                
                # Start listening for messages
                await self.message_loop(validator_hotkey, connection)
                
            except asyncio.CancelledError:
                raise
            except Exception as e:
                bt.logging.error(f"Error connecting to validator {validator_hotkey}: {str(e)}")
                
                # Update failure count
                validator_info["connection_failures"] = validator_info.get("connection_failures", 0) + 1
                
                # Requeue with backoff
                await self.connection_queue.put(validator_hotkey)
                
        except asyncio.CancelledError:
            bt.logging.debug(f"Connection task for validator {validator_hotkey} cancelled")
        except Exception as e:
            bt.logging.error(f"Fatal error in connect_to_validator for {validator_hotkey}: {str(e)}")
            bt.logging.error(traceback.format_exc())
        finally:
            # Clean up resources
            if validator_hotkey in self.connection_tasks:
                self.connection_tasks.pop(validator_hotkey)
                
            if validator_hotkey in self.heartbeat_tasks:
                task = self.heartbeat_tasks.pop(validator_hotkey)
                if task and not task.done():
                    task.cancel()
                    
            if validator_hotkey in self.connections:
                connection = self.connections.pop(validator_hotkey)
                try:
                    await connection.close()
                except:
                    pass
            
            # Requeue after some delay
            await asyncio.sleep(1)
            await self.connection_queue.put(validator_hotkey)
            
    async def message_loop(self, validator_hotkey: str, connection: websockets.WebSocketClientProtocol):
        """Listen for messages from a validator"""
        try:
            async for message_raw in connection:
                try:
                    # Parse message
                    message_data = json.loads(message_raw)
                    message_type = message_data.get("type")
                    
                    # Verify message signature if present
                    if "signature" in message_data:
                        message = WebSocketMessage.from_dict(message_data)
                        if not message.verify():
                            bt.logging.warning(f"Received message with invalid signature from validator {validator_hotkey}")
                            continue
                    
                    # Handle different message types
                    if message_type == "heartbeat":
                        # For heartbeat messages, send a response
                        await self.send_heartbeat_response(validator_hotkey)
                    elif message_type == "heartbeat_ack":
                        # Heartbeat acknowledgement, just log
                        bt.logging.debug(f"Received heartbeat ack from validator {validator_hotkey}")
                    elif message_type == "confirmation":
                        # Handle confirmation message
                        if "confirmation" in self.handlers:
                            confirmation_data = message_data.get("data", {})
                            await self.handlers["confirmation"](validator_hotkey, confirmation_data)
                        else:
                            bt.logging.debug(f"Received confirmation from {validator_hotkey}, but no handler registered")
                    elif message_type == "game_update":
                        # Handle game update message
                        if "game_update" in self.handlers:
                            game_data = message_data.get("data", {})
                            await self.handlers["game_update"](validator_hotkey, game_data)
                        else:
                            bt.logging.debug(f"Received game update from {validator_hotkey}, but no handler registered")
                    elif message_type == "error":
                        # Handle error message
                        error_message = message_data.get("data", {}).get("message", "Unknown error")
                        bt.logging.warning(f"Received error from validator {validator_hotkey}: {error_message}")
                    else:
                        # Unknown message type
                        bt.logging.warning(f"Received unknown message type from validator {validator_hotkey}: {message_type}")
                        
                except json.JSONDecodeError:
                    bt.logging.warning(f"Received invalid JSON from validator {validator_hotkey}")
                except Exception as e:
                    bt.logging.error(f"Error handling message from validator {validator_hotkey}: {str(e)}")
                    bt.logging.error(traceback.format_exc())
                    
        except (ConnectionClosedError, ConnectionClosedOK):
            bt.logging.info(f"Connection to validator {validator_hotkey} closed")
        except asyncio.CancelledError:
            bt.logging.debug(f"Message loop for validator {validator_hotkey} cancelled")
        except Exception as e:
            bt.logging.error(f"Fatal error in message_loop for validator {validator_hotkey}: {str(e)}")
            bt.logging.error(traceback.format_exc())
            
    async def heartbeat_loop(self, validator_hotkey: str):
        """Send periodic heartbeats to keep the connection alive"""
        try:
            while self.is_running:
                # Sleep first to avoid immediate heartbeat after connection
                await asyncio.sleep(self.heartbeat_interval)
                
                # Check if we're still connected
                if validator_hotkey not in self.connections:
                    break
                
                connection = self.connections[validator_hotkey]
                if connection.closed:
                    break
                    
                try:
                    # Create heartbeat message
                    heartbeat = WebSocketMessage.create_signed(
                        wallet=self.wallet,
                        message_type="heartbeat",
                        data={}
                    )
                    
                    # Send heartbeat
                    await connection.send(json.dumps(heartbeat.to_dict()))
                    bt.logging.debug(f"Sent heartbeat to validator {validator_hotkey}")
                    
                except Exception as e:
                    bt.logging.error(f"Error sending heartbeat to validator {validator_hotkey}: {str(e)}")
                    break
                    
        except asyncio.CancelledError:
            bt.logging.debug(f"Heartbeat loop for validator {validator_hotkey} cancelled")
        except Exception as e:
            bt.logging.error(f"Fatal error in heartbeat_loop for validator {validator_hotkey}: {str(e)}")
            bt.logging.error(traceback.format_exc())
            
    async def send_heartbeat_response(self, validator_hotkey: str):
        """Send heartbeat response to a validator"""
        if validator_hotkey not in self.connections:
            return
            
        connection = self.connections[validator_hotkey]
        if connection.closed:
            return
            
        try:
            # Create heartbeat response
            heartbeat_resp = WebSocketMessage.create_signed(
                wallet=self.wallet,
                message_type="heartbeat_response",
                data={}
            )
            
            # Send response
            await connection.send(json.dumps(heartbeat_resp.to_dict()))
            bt.logging.debug(f"Sent heartbeat response to validator {validator_hotkey}")
            
        except Exception as e:
            bt.logging.error(f"Error sending heartbeat response to validator {validator_hotkey}: {str(e)}")
            
    async def send_prediction(self, prediction_data: Dict[str, Any], validator_hotkeys: List[str] = None):
        """
        Send a prediction to validators
        
        Args:
            prediction_data: The prediction data
            validator_hotkeys: Optional list of validator hotkeys to send to (default: all connected)
        
        Returns:
            List of validator hotkeys the prediction was sent to
        """
        if not self.is_running:
            bt.logging.warning("Attempted to send prediction while WebSocket manager is not running")
            return []
            
        # If no validator_hotkeys specified, send to all connected validators
        if validator_hotkeys is None:
            validator_hotkeys = list(self.connections.keys())
            
        # Create prediction message
        prediction_message = PredictionMessage.create(
            wallet=self.wallet,
            prediction_data=prediction_data
        )
        
        # Store prediction
        prediction_id = prediction_data.get("prediction_id")
        if prediction_id:
            self.sent_predictions[prediction_id] = prediction_data
            
        # Send to each validator
        sent_to = []
        for validator_hotkey in validator_hotkeys:
            if validator_hotkey not in self.connections:
                continue
                
            connection = self.connections[validator_hotkey]
            if connection.closed:
                continue
                
            try:
                await connection.send(json.dumps(prediction_message.to_dict()))
                sent_to.append(validator_hotkey)
                bt.logging.info(f"Sent prediction {prediction_id} to validator {validator_hotkey} via WebSocket")
                
            except Exception as e:
                bt.logging.error(f"Error sending prediction to validator {validator_hotkey}: {str(e)}")
                
        return sent_to
    
    async def send_to_all_validators(self, message_type: str, data: Dict[str, Any]):
        """
        Send a message to all connected validators
        
        Args:
            message_type: The type of message to send
            data: The message data
            
        Returns:
            List of validator hotkeys the message was sent to
        """
        if not self.is_running:
            bt.logging.warning(f"Attempted to send {message_type} while WebSocket manager is not running")
            return []
            
        # Create message
        message = WebSocketMessage.create_signed(
            wallet=self.wallet,
            message_type=message_type,
            data=data
        )
        
        # Send to all validators
        sent_to = []
        for validator_hotkey, connection in list(self.connections.items()):
            if connection.closed:
                continue
                
            try:
                await connection.send(json.dumps(message.to_dict()))
                sent_to.append(validator_hotkey)
                bt.logging.debug(f"Sent {message_type} to validator {validator_hotkey}")
                
            except Exception as e:
                bt.logging.error(f"Error sending {message_type} to validator {validator_hotkey}: {str(e)}")
                
        return sent_to 