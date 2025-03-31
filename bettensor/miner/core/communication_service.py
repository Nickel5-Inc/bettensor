"""
Communication Service for the Bettensor Miner.

This module handles communication with validators using both WebSocket and HTTP,
providing reliable message delivery with automatic fallback mechanisms.
"""

import asyncio
import json
import random
import time
import uuid
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Union

import bittensor as bt
import websockets
from websockets.exceptions import ConnectionClosed, WebSocketException

from bettensor.protocol import TeamGamePrediction


class ValidatorConnection:
    """Represents a connection to a validator."""
    
    def __init__(self, validator_info):
        """
        Initialize a connection to a validator.
        
        Args:
            validator_info: Information about the validator (hotkey, ip, port, etc.)
        """
        self.hotkey = validator_info.get('hotkey')
        self.ip = validator_info.get('ip')
        self.port = validator_info.get('port')
        self.ws_port = validator_info.get('ws_port', self.port)  # Default to regular port if WS port not specified
        self.protocol = validator_info.get('protocol', 'wss')  # Default to secure WebSocket
        
        # Current connection status
        self.ws = None
        self.is_connected = False
        self.last_connected = None
        self.last_heartbeat = None
        self.connection_attempts = 0
        self.max_reconnect_attempts = 10
        self.reconnect_delay = 1.0  # Initial reconnect delay in seconds
        self.max_reconnect_delay = 60.0  # Maximum reconnect delay in seconds
        
        # Connection stats
        self.messages_sent = 0
        self.messages_received = 0
        self.errors = 0
        self.connection_time = None
        
        # Message queue for this validator
        self.message_queue = asyncio.Queue()
        self.sender_task = None
    
    @property
    def ws_uri(self) -> str:
        """Get the WebSocket URI for this validator."""
        return f"{self.protocol}://{self.ip}:{self.ws_port}/ws"
    
    def get_stats(self) -> Dict[str, Any]:
        """
        Get statistics about this connection.
        
        Returns:
            A dictionary of connection statistics
        """
        uptime = 0
        if self.connection_time:
            uptime = (datetime.now() - self.connection_time).total_seconds()
        
        return {
            "hotkey": self.hotkey,
            "uri": self.ws_uri,
            "is_connected": self.is_connected,
            "last_connected": self.last_connected,
            "last_heartbeat": self.last_heartbeat,
            "connection_attempts": self.connection_attempts,
            "messages_sent": self.messages_sent,
            "messages_received": self.messages_received,
            "errors": self.errors,
            "uptime": uptime,
            "queue_size": self.message_queue.qsize(),
        }


class CommunicationService:
    """
    Manages communication with validators using WebSocket and HTTP.
    
    This service provides reliable message delivery with automatic fallback
    to HTTP when WebSocket connections are unavailable or unreliable.
    """
    
    def __init__(self, event_manager, metagraph, wallet, dendrite):
        """
        Initialize the communication service.
        
        Args:
            event_manager: Event manager for publishing events
            metagraph: Bittensor metagraph for validator discovery
            wallet: Bittensor wallet for authentication
            dendrite: Bittensor dendrite for HTTP communication
        """
        self.event_manager = event_manager
        self.metagraph = metagraph
        self.wallet = wallet
        self.dendrite = dendrite
        
        # WebSocket connections to validators
        self.validators: Dict[str, ValidatorConnection] = {}
        
        # Statistics
        self.stats = {
            "predictions_sent_ws": 0,
            "predictions_sent_http": 0,
            "predictions_confirmed": 0,
            "failed_predictions": 0,
            "total_messages_sent": 0,
            "total_messages_received": 0,
            "connected_validators": 0,
            "total_validators": 0,
            "errors": 0,
            "last_error": None,
            "http_latency_ms": [],
            "ws_latency_ms": [],
            "connection_success_rate": 1.0,
        }
        
        # Connection settings
        self.heartbeat_interval = 30  # Seconds between heartbeats
        self.connection_timeout = 5  # Seconds to wait for connection
        self.message_timeout = 10  # Seconds to wait for message to be sent
        
        # Service state
        self.running = False
        self.tasks = []
        
        # Message handlers
        self.message_handlers = {}
        
        # Register default message handlers
        self.register_message_handler("heartbeat", self._handle_heartbeat)
        self.register_message_handler("game_update", self._handle_game_update)
        self.register_message_handler("prediction_confirmation", self._handle_prediction_confirmation)
        
        bt.logging.info("CommunicationService initialized")
    
    def register_message_handler(self, message_type: str, handler: Callable) -> None:
        """
        Register a handler for a message type.
        
        Args:
            message_type: Type of message to handle
            handler: Function to call when a message of this type is received
        """
        self.message_handlers[message_type] = handler
        bt.logging.info(f"Registered handler for message type: {message_type}")
    
    def sync_validators(self) -> None:
        """Synchronize validator list with the metagraph."""
        updated_validators = {}
        
        # Get all validators from the metagraph
        validator_uids = [
            i for i, stake in enumerate(self.metagraph.S) 
            if stake > 0.0 and self.metagraph.hotkeys[i] != self.wallet.hotkey.ss58_address
        ]
        
        # Update validator connections
        for uid in validator_uids:
            hotkey = self.metagraph.hotkeys[uid]
            ip = self.metagraph.neurons[uid].axon_info.ip
            port = self.metagraph.neurons[uid].axon_info.port
            
            # Skip validators with invalid IP
            if ip == "0.0.0.0":
                continue
            
            # Update existing connection or create new one
            if hotkey in self.validators:
                validator = self.validators[hotkey]
                validator.ip = ip
                validator.port = port
                updated_validators[hotkey] = validator
            else:
                validator_info = {
                    "hotkey": hotkey,
                    "ip": ip,
                    "port": port,
                }
                updated_validators[hotkey] = ValidatorConnection(validator_info)
        
        # Update stats
        self.stats["total_validators"] = len(updated_validators)
        
        # Replace the validators dict
        self.validators = updated_validators
        
        bt.logging.info(f"Synchronized {len(self.validators)} validators from metagraph")
    
    async def start(self) -> None:
        """Start the communication service."""
        if self.running:
            bt.logging.warning("CommunicationService is already running")
            return
        
        self.running = True
        
        # Sync validators from metagraph
        self.sync_validators()
        
        # Start tasks
        self.tasks = [
            asyncio.create_task(self._connection_manager()),
            asyncio.create_task(self._heartbeat_sender()),
        ]
        
        bt.logging.info("CommunicationService started")
    
    async def stop(self) -> None:
        """Stop the communication service."""
        if not self.running:
            bt.logging.warning("CommunicationService is not running")
            return
        
        self.running = False
        
        # Cancel all tasks
        for task in self.tasks:
            task.cancel()
        
        # Wait for tasks to terminate
        await asyncio.gather(*self.tasks, return_exceptions=True)
        
        # Close all WebSocket connections
        for validator in self.validators.values():
            if validator.ws:
                await validator.ws.close()
                validator.is_connected = False
        
        bt.logging.info("CommunicationService stopped")
    
    async def _connection_manager(self) -> None:
        """Manage connections to validators."""
        try:
            while self.running:
                connected_count = 0
                
                # Check each validator connection
                for hotkey, validator in list(self.validators.items()):
                    # Skip if already connected
                    if validator.is_connected and validator.ws and not validator.ws.closed:
                        connected_count += 1
                        continue
                    
                    # Try to connect if not connected
                    try:
                        # Calculate exponential backoff if needed
                        if validator.connection_attempts > 0:
                            # Calculate delay with exponential backoff and jitter
                            delay = min(
                                validator.max_reconnect_delay,
                                validator.reconnect_delay * (2 ** (validator.connection_attempts - 1))
                            )
                            delay += delay * 0.1 * random.random()  # Add up to 10% jitter
                            
                            # Check if we should try again based on last attempt time
                            last_attempt = validator.last_connected or time.time() - delay - 1
                            if time.time() - last_attempt < delay:
                                continue
                        
                        # Increment attempt counter
                        validator.connection_attempts += 1
                        
                        # Try to connect with timeout
                        ws = await asyncio.wait_for(
                            websockets.connect(validator.ws_uri),
                            timeout=self.connection_timeout
                        )
                        
                        # Update connection state
                        validator.ws = ws
                        validator.is_connected = True
                        validator.last_connected = time.time()
                        validator.connection_time = datetime.now()
                        connected_count += 1
                        
                        # Reset reconnection delay on successful connection
                        validator.reconnect_delay = 1.0
                        
                        # Authentication message (first message after connection)
                        auth_message = {
                            "type": "authentication",
                            "hotkey": self.wallet.hotkey.ss58_address,
                            "signature": self.wallet.hotkey.sign(self.wallet.hotkey.ss58_address).hex(),
                            "timestamp": time.time(),
                        }
                        
                        await ws.send(json.dumps(auth_message))
                        
                        # Start message handler for this connection
                        asyncio.create_task(self._message_handler(validator))
                        
                        # Start sender task for this connection
                        if validator.sender_task is None or validator.sender_task.done():
                            validator.sender_task = asyncio.create_task(
                                self._message_sender(validator)
                            )
                        
                        bt.logging.info(f"Connected to validator {hotkey[:10]}... at {validator.ws_uri}")
                        
                        # Publish connection event
                        if self.event_manager:
                            await self.event_manager.publish(
                                "validator_connected",
                                {"hotkey": hotkey, "uri": validator.ws_uri}
                            )
                    
                    except asyncio.TimeoutError:
                        bt.logging.debug(f"Connection to validator {hotkey[:10]}... timed out")
                        validator.is_connected = False
                        validator.errors += 1
                        self.stats["errors"] += 1
                        
                    except (ConnectionRefusedError, OSError, WebSocketException) as e:
                        bt.logging.debug(f"Connection to validator {hotkey[:10]}... failed: {str(e)}")
                        validator.is_connected = False
                        validator.errors += 1
                        self.stats["errors"] += 1
                
                # Update connection stats
                self.stats["connected_validators"] = connected_count
                success_rate = connected_count / max(1, len(self.validators))
                self.stats["connection_success_rate"] = success_rate
                
                # Sleep before next connection check
                await asyncio.sleep(5)
                
        except asyncio.CancelledError:
            bt.logging.info("Connection manager task cancelled")
            raise
        
        except Exception as e:
            bt.logging.error(f"Error in connection manager: {str(e)}")
            if self.running:
                # Restart the task
                self.tasks.append(asyncio.create_task(self._connection_manager()))
    
    async def _message_handler(self, validator: ValidatorConnection) -> None:
        """
        Handle messages from a validator.
        
        Args:
            validator: The validator connection
        """
        try:
            while self.running and validator.is_connected and validator.ws and not validator.ws.closed:
                try:
                    # Receive message with timeout
                    message = await asyncio.wait_for(
                        validator.ws.recv(),
                        timeout=self.connection_timeout * 2
                    )
                    
                    # Parse message
                    try:
                        data = json.loads(message)
                        message_type = data.get("type")
                        
                        # Update connection stats
                        validator.messages_received += 1
                        validator.last_heartbeat = time.time()
                        self.stats["total_messages_received"] += 1
                        
                        # Handle message based on type
                        if message_type in self.message_handlers:
                            handler = self.message_handlers[message_type]
                            await handler(validator.hotkey, data)
                        else:
                            bt.logging.warning(f"Received unknown message type: {message_type}")
                            
                    except json.JSONDecodeError:
                        bt.logging.warning(f"Received invalid JSON from validator {validator.hotkey[:10]}...")
                        validator.errors += 1
                        self.stats["errors"] += 1
                
                except (asyncio.TimeoutError, ConnectionClosed, WebSocketException) as e:
                    bt.logging.debug(f"WebSocket error with validator {validator.hotkey[:10]}...: {str(e)}")
                    break
            
            # Connection closed, update state
            validator.is_connected = False
            
            # Publish disconnection event
            if self.event_manager:
                await self.event_manager.publish(
                    "validator_disconnected",
                    {"hotkey": validator.hotkey}
                )
            
            bt.logging.info(f"Disconnected from validator {validator.hotkey[:10]}...")
            
        except asyncio.CancelledError:
            bt.logging.debug(f"Message handler for validator {validator.hotkey[:10]}... cancelled")
            raise
        
        except Exception as e:
            bt.logging.error(f"Error in message handler for validator {validator.hotkey[:10]}...: {str(e)}")
            validator.is_connected = False
            validator.errors += 1
            self.stats["errors"] += 1
            self.stats["last_error"] = str(e)
    
    async def _message_sender(self, validator: ValidatorConnection) -> None:
        """
        Send queued messages to a validator.
        
        Args:
            validator: The validator connection
        """
        try:
            while self.running and validator.is_connected and validator.ws and not validator.ws.closed:
                try:
                    # Get message from queue with timeout
                    message = await asyncio.wait_for(
                        validator.message_queue.get(),
                        timeout=30  # Check connection every 30 seconds
                    )
                    
                    # Send message
                    start_time = time.time()
                    await validator.ws.send(json.dumps(message))
                    latency_ms = (time.time() - start_time) * 1000
                    
                    # Update stats
                    validator.messages_sent += 1
                    self.stats["total_messages_sent"] += 1
                    self.stats["ws_latency_ms"].append(latency_ms)
                    
                    # Keep only the last 100 latency measurements
                    if len(self.stats["ws_latency_ms"]) > 100:
                        self.stats["ws_latency_ms"] = self.stats["ws_latency_ms"][-100:]
                    
                    # Mark task as done
                    validator.message_queue.task_done()
                    
                except asyncio.TimeoutError:
                    # No messages to send, continue waiting
                    continue
                
                except (ConnectionClosed, WebSocketException) as e:
                    bt.logging.debug(f"Send error to validator {validator.hotkey[:10]}...: {str(e)}")
                    validator.is_connected = False
                    validator.errors += 1
                    self.stats["errors"] += 1
                    break
            
            bt.logging.debug(f"Message sender for validator {validator.hotkey[:10]}... stopped")
            
        except asyncio.CancelledError:
            bt.logging.debug(f"Message sender for validator {validator.hotkey[:10]}... cancelled")
            raise
        
        except Exception as e:
            bt.logging.error(f"Error in message sender for validator {validator.hotkey[:10]}...: {str(e)}")
            validator.is_connected = False
            validator.errors += 1
            self.stats["errors"] += 1
            self.stats["last_error"] = str(e)
    
    async def _heartbeat_sender(self) -> None:
        """Send periodic heartbeats to all connected validators."""
        try:
            while self.running:
                # Sleep first to allow initial connections
                await asyncio.sleep(self.heartbeat_interval)
                
                # Send heartbeat to all connected validators
                for hotkey, validator in self.validators.items():
                    if validator.is_connected:
                        heartbeat_message = {
                            "type": "heartbeat",
                            "timestamp": time.time(),
                            "hotkey": self.wallet.hotkey.ss58_address,
                        }
                        
                        # Queue the message for sending
                        await validator.message_queue.put(heartbeat_message)
                
                bt.logging.debug(f"Sent heartbeats to {self.stats['connected_validators']} validators")
            
        except asyncio.CancelledError:
            bt.logging.info("Heartbeat sender task cancelled")
            raise
        
        except Exception as e:
            bt.logging.error(f"Error in heartbeat sender: {str(e)}")
            if self.running:
                # Restart the task
                self.tasks.append(asyncio.create_task(self._heartbeat_sender()))
    
    async def _handle_heartbeat(self, validator_hotkey: str, message: Dict) -> None:
        """
        Handle heartbeat messages from validators.
        
        Args:
            validator_hotkey: The validator's hotkey
            message: The heartbeat message
        """
        if validator_hotkey in self.validators:
            # Update last heartbeat time
            self.validators[validator_hotkey].last_heartbeat = time.time()
            
            # Publish heartbeat event
            if self.event_manager:
                await self.event_manager.publish(
                    "validator_heartbeat",
                    {"hotkey": validator_hotkey, "timestamp": message.get("timestamp")}
                )
    
    async def _handle_game_update(self, validator_hotkey: str, message: Dict) -> None:
        """
        Handle game update messages from validators.
        
        Args:
            validator_hotkey: The validator's hotkey
            message: The game update message
        """
        # Publish game update event
        if self.event_manager:
            await self.event_manager.publish("game_update", message.get("game_data"))
    
    async def _handle_prediction_confirmation(self, validator_hotkey: str, message: Dict) -> None:
        """
        Handle prediction confirmation messages from validators.
        
        Args:
            validator_hotkey: The validator's hotkey
            message: The prediction confirmation message
        """
        # Update stats
        self.stats["predictions_confirmed"] += 1
        
        # Check if confirmation was successful
        success = message.get("success", False)
        if not success:
            self.stats["failed_predictions"] += 1
        
        # Publish prediction confirmation event
        if self.event_manager:
            await self.event_manager.publish("prediction_confirmation", message)
    
    async def send_prediction(self, prediction: TeamGamePrediction, validator_hotkeys: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        Send a prediction to validators.
        
        Args:
            prediction: The prediction to send
            validator_hotkeys: Optional list of validator hotkeys to send to,
                              if None, send to all connected validators
        
        Returns:
            A dictionary with results for each validator
        """
        results = {}
        
        # Convert prediction to dictionary
        prediction_dict = prediction.dict()
        
        # Create message
        message = {
            "type": "prediction",
            "prediction": prediction_dict,
            "timestamp": time.time(),
            "hotkey": self.wallet.hotkey.ss58_address,
        }
        
        # Get list of validators to send to
        if validator_hotkeys is None:
            target_validators = list(self.validators.keys())
        else:
            target_validators = [
                hotkey for hotkey in validator_hotkeys 
                if hotkey in self.validators
            ]
        
        # Send to each validator
        for hotkey in target_validators:
            validator = self.validators[hotkey]
            
            # Try WebSocket first if connected
            if validator.is_connected:
                try:
                    # Queue the message for sending
                    await validator.message_queue.put(message)
                    
                    # Update stats
                    self.stats["predictions_sent_ws"] += 1
                    
                    # Add to results
                    results[hotkey] = {
                        "success": True,
                        "method": "websocket",
                        "message": "Prediction queued for delivery via WebSocket",
                    }
                    
                    bt.logging.debug(f"Queued prediction {prediction.prediction_id} for validator {hotkey[:10]}... via WebSocket")
                    
                    continue  # Skip HTTP fallback
                    
                except Exception as e:
                    bt.logging.warning(f"WebSocket send failed for validator {hotkey[:10]}...: {str(e)}")
            
            # Fallback to HTTP if WebSocket failed or not connected
            try:
                # Get validator neuron
                axon_info = None
                for uid, hotkey_check in enumerate(self.metagraph.hotkeys):
                    if hotkey_check == hotkey:
                        axon_info = self.metagraph.neurons[uid].axon_info
                        break
                
                if not axon_info:
                    results[hotkey] = {
                        "success": False,
                        "method": "http",
                        "message": "Validator not found in metagraph",
                    }
                    continue
                
                # Build synapse
                synapse = TeamGamePrediction()
                for key, value in prediction_dict.items():
                    setattr(synapse, key, value)
                
                # Send via dendrite
                start_time = time.time()
                response = await self.dendrite.forward(
                    axons=[axon_info],
                    synapse=synapse,
                    timeout=self.message_timeout
                )
                latency_ms = (time.time() - start_time) * 1000
                
                # Update stats
                self.stats["predictions_sent_http"] += 1
                self.stats["http_latency_ms"].append(latency_ms)
                
                # Keep only the last 100 latency measurements
                if len(self.stats["http_latency_ms"]) > 100:
                    self.stats["http_latency_ms"] = self.stats["http_latency_ms"][-100:]
                
                # Check response
                if response and getattr(response[0], "success", False):
                    self.stats["predictions_confirmed"] += 1
                    
                    results[hotkey] = {
                        "success": True,
                        "method": "http",
                        "message": getattr(response[0], "message", "Prediction accepted"),
                    }
                    
                    bt.logging.debug(f"Sent prediction {prediction.prediction_id} to validator {hotkey[:10]}... via HTTP")
                else:
                    error_msg = getattr(response[0], "message", "Unknown error") if response else "No response"
                    self.stats["failed_predictions"] += 1
                    
                    results[hotkey] = {
                        "success": False,
                        "method": "http",
                        "message": error_msg,
                    }
                    
                    bt.logging.warning(f"Failed to send prediction {prediction.prediction_id} to validator {hotkey[:10]} via HTTP: {error_msg}")
                
            except Exception as e:
                self.stats["failed_predictions"] += 1
                self.stats["errors"] += 1
                self.stats["last_error"] = str(e)
                
                results[hotkey] = {
                    "success": False,
                    "method": "http",
                    "message": str(e),
                }
                
                bt.logging.error(f"Error sending prediction to validator {hotkey[:10]}...: {str(e)}")
        
        # Publish prediction sent event
        if self.event_manager:
            await self.event_manager.publish(
                "prediction_sent",
                {
                    "prediction_id": prediction.prediction_id,
                    "results": results,
                }
            )
        
        return results
    
    def get_stats(self) -> Dict[str, Any]:
        """
        Get statistics about the communication service.
        
        Returns:
            A dictionary of statistics
        """
        # Calculate average latencies
        avg_ws_latency_ms = 0
        if self.stats["ws_latency_ms"]:
            avg_ws_latency_ms = sum(self.stats["ws_latency_ms"]) / len(self.stats["ws_latency_ms"])
            
        avg_http_latency_ms = 0
        if self.stats["http_latency_ms"]:
            avg_http_latency_ms = sum(self.stats["http_latency_ms"]) / len(self.stats["http_latency_ms"])
        
        # Base stats
        stats = {
            "running": self.running,
            "connected_validators": self.stats["connected_validators"],
            "total_validators": self.stats["total_validators"],
            "connection_success_rate": self.stats["connection_success_rate"],
            "predictions_sent_ws": self.stats["predictions_sent_ws"],
            "predictions_sent_http": self.stats["predictions_sent_http"],
            "predictions_confirmed": self.stats["predictions_confirmed"],
            "failed_predictions": self.stats["failed_predictions"],
            "total_messages_sent": self.stats["total_messages_sent"],
            "total_messages_received": self.stats["total_messages_received"],
            "errors": self.stats["errors"],
            "last_error": self.stats["last_error"],
            "avg_ws_latency_ms": avg_ws_latency_ms,
            "avg_http_latency_ms": avg_http_latency_ms,
        }
        
        # Add validator connection stats
        stats["validators"] = {
            hotkey: validator.get_stats() 
            for hotkey, validator in self.validators.items()
        }
        
        return stats 