#!/usr/bin/env python3
# examples/websocket_signed_demo.py

import asyncio
import argparse
import json
import logging
import sys
import time
import uuid
from typing import Dict, Any, List
import bittensor as bt

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("WebSocketDemo")

# Import WebSocket classes
try:
    from bettensor.miner.io.websocket_client import WebSocketMessage, MinerWebSocketManager
    from bettensor.validator.io.websocket_handler import ValidatorWebSocketManager
except ImportError:
    logger.error("Failed to import WebSocket classes. Make sure the bettensor package is installed.")
    sys.exit(1)

class MockMiner:
    """A mock miner for demonstration purposes"""
    
    def __init__(self, hotkey: str = None):
        self.wallet = self._create_wallet(hotkey)
        self.hotkey = self.wallet.hotkey.ss58_address
        self.metagraph = MockMetagraph()
        self.loop = asyncio.get_event_loop()
        self.websocket_manager = None
        
    def _create_wallet(self, hotkey: str = None):
        """Create a wallet for the miner"""
        wallet = bt.wallet(name="demo_miner", hotkey=hotkey or str(uuid.uuid4())[:8])
        return wallet
        
    async def start_websocket(self, validator_endpoint: str):
        """Start WebSocket client to connect to a validator"""
        logger.info(f"Starting WebSocket client with hotkey {self.hotkey}")
        
        # Create WebSocket manager
        self.websocket_manager = MinerWebSocketManager(
            miner_hotkey=self.hotkey,
            metagraph=self.metagraph,
            timeout=10
        )
        
        # Set wallet for signing messages
        self.websocket_manager.set_wallet(self.wallet)
        
        # Apply configuration
        self.websocket_manager.set_config(
            heartbeat_interval=10,  # Short interval for demo
            reconnect_base_interval=1,
            max_reconnect_interval=10
        )
        
        # Register handlers
        self.websocket_manager.register_confirmation_handler(self.handle_confirmation)
        self.websocket_manager.register_game_update_handler(self.handle_game_update)
        
        # Add validator info
        host, port = validator_endpoint.split(':')
        self.websocket_manager.validators_info[validator_endpoint] = {
            "uid": 0,
            "ip": host,
            "port": port,
            "last_attempted_connection": 0,
            "connection_failures": 0
        }
        
        # Queue validator for connection
        await self.websocket_manager.connection_queue.put(validator_endpoint)
        
        # Start WebSocket manager
        await self.websocket_manager.start()
        
        logger.info("WebSocket client started")
        
    async def send_prediction(self):
        """Send a sample prediction via WebSocket"""
        # Wait for connection
        await asyncio.sleep(2)
        
        if not self.websocket_manager.connections:
            logger.error("No validator connections available")
            return
            
        logger.info("Sending prediction via WebSocket")
        
        # Create prediction data
        prediction_id = str(uuid.uuid4())
        prediction_data = {
            "prediction_id": prediction_id,
            "game_id": "game_123",
            "market": "moneyline",
            "value": "home",
            "stake": 1.0,
            "timestamp": time.time()
        }
        
        # Send prediction
        sent_to = await self.websocket_manager.send_prediction(
            prediction_data=prediction_data
        )
        
        logger.info(f"Prediction sent to validators: {sent_to}")
        return prediction_id
        
    async def handle_confirmation(self, validator_hotkey: str, confirmation_data: Dict[str, Any]):
        """Handle confirmation message from validator"""
        logger.info(f"Received confirmation from {validator_hotkey}: {confirmation_data}")
        
    async def handle_game_update(self, validator_hotkey: str, game_data: Dict[str, Any]):
        """Handle game update message from validator"""
        logger.info(f"Received game update from {validator_hotkey} with {len(game_data.get('games', []))} games")
        
    async def stop(self):
        """Stop the miner"""
        if self.websocket_manager:
            await self.websocket_manager.stop()
            logger.info("WebSocket manager stopped")
            
class MockValidator:
    """A mock validator for demonstration purposes"""
    
    def __init__(self, port: int = 8080, hotkey: str = None):
        self.wallet = self._create_wallet(hotkey)
        self.hotkey = self.wallet.hotkey.ss58_address
        self.metagraph = MockMetagraph()
        self.port = port
        self.loop = asyncio.get_event_loop()
        self.websocket_manager = None
        self.predictions = {}
        
    def _create_wallet(self, hotkey: str = None):
        """Create a wallet for the validator"""
        wallet = bt.wallet(name="demo_validator", hotkey=hotkey or str(uuid.uuid4())[:8])
        return wallet
        
    async def start_websocket(self):
        """Start WebSocket server"""
        import websockets
        from fastapi import FastAPI
        import uvicorn
        from uvicorn.config import Config
        
        logger.info(f"Starting WebSocket server with hotkey {self.hotkey}")
        
        # Create FastAPI app
        app = FastAPI()
        
        # Create WebSocket manager
        self.websocket_manager = ValidatorWebSocketManager(self)
        
        # Integrate with FastAPI
        await self.websocket_manager.integrate_with_axon(app)
        
        # Register prediction handler
        self.websocket_manager.register_prediction_handler(self.handle_prediction)
        
        # Start WebSocket manager
        await self.websocket_manager.start()
        
        # Start uvicorn server
        config = Config(app=app, host="0.0.0.0", port=self.port, log_level="info")
        server = uvicorn.Server(config)
        self.server = server
        
        # Start server in a separate task
        self.server_task = asyncio.create_task(server.serve())
        
        logger.info(f"WebSocket server started on port {self.port}")
        
    async def handle_prediction(self, miner_hotkey: str, prediction_data: Dict[str, Any]):
        """Handle prediction message from miner"""
        logger.info(f"Received prediction from {miner_hotkey}: {prediction_data}")
        
        # Store prediction
        prediction_id = prediction_data.get("prediction_id")
        self.predictions[prediction_id] = prediction_data
        
        # Process prediction (validation would happen here in a real implementation)
        success = True
        message = "Prediction processed successfully"
        
        # Get miner UID
        try:
            miner_uid = self.metagraph.hotkeys.index(miner_hotkey)
        except ValueError:
            logger.warning(f"Miner {miner_hotkey} not found in metagraph")
            miner_uid = 0
            
        # Send confirmation
        await self.send_confirmation(miner_hotkey, prediction_id, success, message)
        
        # After some time, send a game update
        asyncio.create_task(self.send_game_update_after_delay(2))
        
    async def send_confirmation(self, miner_hotkey: str, prediction_id: str, success: bool, message: str):
        """Send confirmation to a miner"""
        if not self.websocket_manager:
            logger.error("WebSocket manager not initialized")
            return
            
        # Send confirmation
        await self.websocket_manager.send_confirmation(
            miner_hotkey=miner_hotkey,
            prediction_id=prediction_id,
            success=success,
            message=message,
            miner_stats={"score": 0.75, "rank": 10}
        )
        
        logger.info(f"Sent confirmation for prediction {prediction_id} to {miner_hotkey}")
        
    async def send_game_update_after_delay(self, delay_seconds: int):
        """Send a game update after a delay"""
        await asyncio.sleep(delay_seconds)
        
        if not self.websocket_manager:
            logger.error("WebSocket manager not initialized")
            return
            
        # Create sample game data
        games = [
            {
                "game_id": "game_123",
                "home_team": "Team A",
                "away_team": "Team B",
                "status": "inprogress",
                "home_score": 2,
                "away_score": 1,
                "start_time": time.time() - 3600,  # Started an hour ago
                "updated_at": time.time()
            }
        ]
        
        # Broadcast game update
        await self.websocket_manager.broadcast_game_updates(games)
        
        logger.info(f"Broadcast game update with {len(games)} games")
        
    async def stop(self):
        """Stop the validator"""
        if self.websocket_manager:
            await self.websocket_manager.stop()
            logger.info("WebSocket manager stopped")
            
        if hasattr(self, 'server_task') and self.server_task:
            self.server_task.cancel()
            try:
                await self.server_task
            except asyncio.CancelledError:
                pass
            logger.info("Server stopped")

class MockMetagraph:
    """A mock metagraph for demonstration purposes"""
    
    def __init__(self):
        self.hotkeys = []
        self.validator_permit = []
        self.S = []
        self.axons = []
        
    def add_peer(self, hotkey: str, is_validator: bool = True, stake: float = 1.0, ip: str = "127.0.0.1", port: int = 8080):
        """Add a peer to the metagraph"""
        self.hotkeys.append(hotkey)
        self.validator_permit.append(is_validator)
        self.S.append(stake)
        self.axons.append(MockAxon(hotkey, ip, port))
        
class MockAxon:
    """A mock axon for demonstration purposes"""
    
    def __init__(self, hotkey: str, ip: str, port: int):
        self.hotkey = hotkey
        self.ip = ip
        self.port = port

async def run_validator_demo(port: int):
    """Run a validator demo"""
    validator = MockValidator(port=port)
    
    try:
        # Start WebSocket server
        await validator.start_websocket()
        
        # Wait for connections
        logger.info(f"Validator running on port {port}. Press Ctrl+C to stop.")
        
        # Keep running
        while True:
            await asyncio.sleep(1)
            
    except KeyboardInterrupt:
        logger.info("Stopping validator...")
    finally:
        await validator.stop()

async def run_miner_demo(validator_endpoint: str):
    """Run a miner demo"""
    miner = MockMiner()
    
    try:
        # Start WebSocket client
        await miner.start_websocket(validator_endpoint)
        
        # Send a prediction
        prediction_id = await miner.send_prediction()
        logger.info(f"Sent prediction with ID: {prediction_id}")
        
        # Keep running to receive confirmations and updates
        logger.info("Waiting for confirmations and updates. Press Ctrl+C to stop.")
        
        # Keep running
        while True:
            await asyncio.sleep(1)
            
    except KeyboardInterrupt:
        logger.info("Stopping miner...")
    finally:
        await miner.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="WebSocket Demo")
    parser.add_argument("--mode", choices=["validator", "miner"], required=True, help="Run as validator or miner")
    parser.add_argument("--port", type=int, default=8080, help="Port for validator (default: 8080)")
    parser.add_argument("--endpoint", default="127.0.0.1:8080", help="Validator endpoint for miner (default: 127.0.0.1:8080)")
    
    args = parser.parse_args()
    
    if args.mode == "validator":
        asyncio.run(run_validator_demo(args.port))
    else:
        asyncio.run(run_miner_demo(args.endpoint)) 