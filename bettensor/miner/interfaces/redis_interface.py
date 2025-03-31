import asyncio
import json
from typing import Dict, Any, List, Optional, Set, Union
import traceback
import time

import bittensor as bt
import redis
import aioredis

class RedisInterface:
    """Interface for Redis message queue communication"""
    def __init__(self, 
                miner,
                redis_host: str = "localhost", 
                redis_port: int = 6379,
                redis_db: int = 0,
                redis_password: Optional[str] = None):
        """Initialize the Redis interface"""
        self.miner = miner
        self.redis_host = redis_host
        self.redis_port = redis_port
        self.redis_db = redis_db
        self.redis_password = redis_password
        
        # Redis clients
        self.redis_client = None  # Synchronous client
        self.redis_async_client = None  # Asynchronous client
        
        # Channels for PubSub
        self.prediction_channel = "bettensor:predictions"
        self.game_update_channel = "bettensor:game_updates"
        self.command_channel = "bettensor:commands"
        
        # Store active subscriptions
        self.subscriptions = []
        self.listener_task = None
        self.is_listening = False
        
    async def connect(self) -> bool:
        """
        Connect to Redis server
        
        Returns:
            True if connected successfully, False otherwise
        """
        try:
            # Create synchronous client
            self.redis_client = redis.Redis(
                host=self.redis_host,
                port=self.redis_port,
                db=self.redis_db,
                password=self.redis_password,
                decode_responses=True
            )
            
            # Test connection
            self.redis_client.ping()
            
            # Create asynchronous client
            self.redis_async_client = await aioredis.from_url(
                f"redis://{self.redis_host}:{self.redis_port}/{self.redis_db}",
                password=self.redis_password,
                decode_responses=True
            )
            
            bt.logging.info(f"Connected to Redis at {self.redis_host}:{self.redis_port}")
            return True
            
        except Exception as e:
            bt.logging.error(f"Failed to connect to Redis: {str(e)}")
            return False
            
    async def disconnect(self):
        """Disconnect from Redis server and clean up resources"""
        try:
            # Stop listener if running
            if self.listener_task and not self.listener_task.done():
                self.is_listening = False
                self.listener_task.cancel()
                try:
                    await self.listener_task
                except asyncio.CancelledError:
                    pass
                
            # Close Redis connections
            if self.redis_client:
                self.redis_client.close()
                
            if self.redis_async_client:
                await self.redis_async_client.close()
                
            bt.logging.info("Disconnected from Redis")
            
        except Exception as e:
            bt.logging.error(f"Error disconnecting from Redis: {str(e)}")
            
    async def start_listening(self):
        """Start listening for Redis messages"""
        if self.is_listening or self.listener_task and not self.listener_task.done():
            bt.logging.warning("Redis listener is already running")
            return
            
        self.is_listening = True
        self.listener_task = asyncio.create_task(self._listen_messages())
        bt.logging.info("Started Redis message listener")
        
    async def _listen_messages(self):
        """Background task to listen for Redis messages"""
        try:
            # Create PubSub
            pubsub = self.redis_async_client.pubsub()
            
            # Subscribe to channels
            await pubsub.subscribe(
                self.prediction_channel,
                self.game_update_channel,
                self.command_channel
            )
            
            bt.logging.info(f"Subscribed to Redis channels")
            
            # Listen for messages
            while self.is_listening:
                message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
                if message:
                    try:
                        await self._process_message(message)
                    except Exception as e:
                        bt.logging.error(f"Error processing Redis message: {str(e)}")
                        bt.logging.error(traceback.format_exc())
                
                # Brief yield to avoid blocking the event loop
                await asyncio.sleep(0.01)
                
            # Unsubscribe when done
            await pubsub.unsubscribe()
            bt.logging.info("Unsubscribed from Redis channels")
            
        except asyncio.CancelledError:
            bt.logging.info("Redis listener task cancelled")
            raise
        except Exception as e:
            bt.logging.error(f"Error in Redis listener: {str(e)}")
            bt.logging.error(traceback.format_exc())
            
    async def _process_message(self, message: Dict[str, Any]):
        """
        Process a Redis message
        
        Args:
            message: The Redis message to process
        """
        try:
            # Extract channel and data
            channel = message.get('channel')
            data = message.get('data')
            
            if not data:
                return
                
            # Parse message data
            try:
                data_dict = json.loads(data) if isinstance(data, str) else data
            except json.JSONDecodeError:
                bt.logging.warning(f"Received invalid JSON from Redis channel {channel}")
                return
                
            # Process based on channel
            if channel == self.prediction_channel:
                await self._handle_prediction_message(data_dict)
            elif channel == self.game_update_channel:
                await self._handle_game_update_message(data_dict)
            elif channel == self.command_channel:
                await self._handle_command_message(data_dict)
                
        except Exception as e:
            bt.logging.error(f"Error processing Redis message: {str(e)}")
            bt.logging.error(traceback.format_exc())
            
    async def _handle_prediction_message(self, data: Dict[str, Any]):
        """
        Handle a prediction message from Redis
        
        Args:
            data: The prediction message data
        """
        game_id = data.get('game_id')
        if not game_id:
            bt.logging.warning("Received prediction message without game_id")
            return
            
        bt.logging.info(f"Received prediction request for game {game_id} via Redis")
        
        # Check if we have a prediction manager
        if hasattr(self.miner, 'prediction_manager'):
            # Submit the prediction
            success = await self.miner.prediction_manager.submit_prediction(game_id)
            
            if success:
                bt.logging.info(f"Successfully submitted prediction for game {game_id} from Redis message")
            else:
                bt.logging.warning(f"Failed to submit prediction for game {game_id} from Redis message")
                
    async def _handle_game_update_message(self, data: Dict[str, Any]):
        """
        Handle a game update message from Redis
        
        Args:
            data: The game update message data
        """
        games = data.get('games', [])
        if not games:
            bt.logging.warning("Received game update message without games data")
            return
            
        bt.logging.info(f"Received game updates for {len(games)} games via Redis")
        
        # Check if we have a prediction manager
        if hasattr(self.miner, 'prediction_manager'):
            # Update active games
            await self.miner.prediction_manager.update_active_games(games)
            
    async def _handle_command_message(self, data: Dict[str, Any]):
        """
        Handle a command message from Redis
        
        Args:
            data: The command message data
        """
        command = data.get('command')
        if not command:
            bt.logging.warning("Received command message without command field")
            return
            
        bt.logging.info(f"Received command via Redis: {command}")
        
        # Process commands
        if command == "submit_prediction" and "game_id" in data:
            game_id = data.get('game_id')
            if hasattr(self.miner, 'prediction_manager'):
                await self.miner.prediction_manager.submit_prediction(game_id)
        elif command == "refresh_metagraph":
            await self.miner.resync_metagraph()
        elif command == "ping":
            await self.publish_message(
                channel=self.command_channel,
                message={
                    "response": "pong",
                    "miner_hotkey": self.miner.wallet.hotkey.ss58_address,
                    "timestamp": time.time()
                }
            )
            
    async def publish_message(self, channel: str, message: Dict[str, Any]) -> bool:
        """
        Publish a message to a Redis channel
        
        Args:
            channel: The channel to publish to
            message: The message to publish
            
        Returns:
            True if published successfully, False otherwise
        """
        try:
            if not self.redis_async_client:
                bt.logging.error("Cannot publish message: Redis client not connected")
                return False
                
            # Serialize message
            message_json = json.dumps(message)
            
            # Publish
            await self.redis_async_client.publish(channel, message_json)
            bt.logging.debug(f"Published message to channel {channel}")
            return True
            
        except Exception as e:
            bt.logging.error(f"Error publishing message to Redis: {str(e)}")
            return False
            
    def publish_sync(self, channel: str, message: Dict[str, Any]) -> bool:
        """
        Publish a message to a Redis channel (synchronous version)
        
        Args:
            channel: The channel to publish to
            message: The message to publish
            
        Returns:
            True if published successfully, False otherwise
        """
        try:
            if not self.redis_client:
                bt.logging.error("Cannot publish message: Redis client not connected")
                return False
                
            # Serialize message
            message_json = json.dumps(message)
            
            # Publish
            self.redis_client.publish(channel, message_json)
            bt.logging.debug(f"Published message to channel {channel} (sync)")
            return True
            
        except Exception as e:
            bt.logging.error(f"Error publishing message to Redis: {str(e)}")
            return False
