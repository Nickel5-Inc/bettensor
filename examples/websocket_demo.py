#!/usr/bin/env python3
# WebSocket Communication Demo for Bettensor

import asyncio
import argparse
import logging
import sys
import json
import time
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("websocket_demo")

try:
    # Try to import Bettensor modules
    # These imports will work when the script is run in the Bettensor environment
    from bettensor.protocol import WebSocketMessage
    from bettensor.validator.websocket.manager import ValidatorWebSocketManager
    from bettensor.miner.websocket.manager import MinerWebSocketManager
except ImportError:
    # For demonstration purposes, create mock classes
    logger.warning("Bettensor modules not found, using mock implementations")

    class WebSocketMessage:
        def __init__(self, sender, data, message_type=None):
            self.sender = sender
            self.data = data
            self.message_type = message_type or "generic"

        @classmethod
        def from_json(cls, json_data):
            data = json.loads(json_data) if isinstance(json_data, str) else json_data
            return cls(
                sender=data.get("sender", "unknown"),
                data=data.get("data", {}),
                message_type=data.get("type", "generic")
            )

        def to_json(self):
            return json.dumps({
                "type": self.message_type,
                "sender": self.sender,
                "data": self.data,
                "timestamp": datetime.now().isoformat()
            })

    class PredictionMessage(WebSocketMessage):
        def __init__(self, sender, data):
            super().__init__(sender, data, "prediction")

    class ConfirmationMessage(WebSocketMessage):
        def __init__(self, sender, data):
            super().__init__(sender, data, "confirmation")

    class GameUpdateMessage(WebSocketMessage):
        def __init__(self, sender, data):
            super().__init__(sender, data, "game_update")

    class MockWebSocketManager:
        def __init__(self, hotkey="demo_hotkey"):
            self.hotkey = hotkey
            self.handlers = {}
            self.connected = False
            self.clients = []
            
        def register_handler(self, message_type, handler):
            self.handlers[message_type] = handler
            logger.info(f"Registered handler for message type: {message_type}")
            
        async def start(self):
            self.connected = True
            logger.info("WebSocket manager started")
            
        async def stop(self):
            self.connected = False
            logger.info("WebSocket manager stopped")
            
        def is_connected(self):
            return self.connected

    class ValidatorWebSocketManager(MockWebSocketManager):
        async def broadcast(self, message):
            logger.info(f"Broadcasting message: {message.message_type}")
            for client in self.clients:
                logger.info(f"  - Sent to client: {client}")
                
        def add_client(self, client_id):
            self.clients.append(client_id)
            logger.info(f"Added client: {client_id}")
            
    class MinerWebSocketManager(MockWebSocketManager):
        async def send_message(self, message):
            logger.info(f"Sent message: {message.message_type}")
            return True

# Sample handler functions
async def handle_prediction(websocket, message):
    """Handler for prediction messages (validator-side)"""
    logger.info(f"Received prediction from {message.sender}")
    logger.info(f"Prediction data: {message.data}")
    
    # Process the prediction
    prediction_id = message.data.get("id", "unknown")
    game_id = message.data.get("game_id", "unknown")
    
    # Send confirmation
    confirmation = ConfirmationMessage(
        sender="validator_hotkey",
        data={
            "prediction_id": prediction_id,
            "game_id": game_id,
            "status": "success",
            "timestamp": datetime.now().isoformat()
        }
    )
    
    if hasattr(websocket, "send_message"):
        await websocket.send_message(confirmation)
    else:
        logger.info(f"Would send confirmation: {confirmation.to_json()}")

async def handle_confirmation(websocket, message):
    """Handler for confirmation messages (miner-side)"""
    logger.info(f"Received confirmation from {message.sender}")
    logger.info(f"Confirmation data: {message.data}")
    
    prediction_id = message.data.get("prediction_id", "unknown")
    status = message.data.get("status", "unknown")
    
    logger.info(f"Prediction {prediction_id} status: {status}")

async def handle_game_update(websocket, message):
    """Handler for game update messages (miner-side)"""
    logger.info(f"Received game update from {message.sender}")
    
    games = message.data.get("games", [])
    logger.info(f"Received updates for {len(games)} games")
    
    for game in games:
        game_id = game.get("id", "unknown")
        status = game.get("status", "unknown")
        score = game.get("score", {})
        logger.info(f"Game {game_id} status: {status}, score: {score}")

async def run_validator_demo():
    """Run a demonstration of validator WebSocket functionality"""
    logger.info("=== Starting Validator WebSocket Demo ===")
    
    # Create WebSocket manager
    manager = ValidatorWebSocketManager(hotkey="validator_demo_key")
    
    # Register handlers
    manager.register_handler("prediction", handle_prediction)
    
    # Start the manager
    await manager.start()
    
    # Simulate clients connecting
    manager.add_client("miner1_hotkey")
    manager.add_client("miner2_hotkey")
    
    # Simulate broadcasting game updates
    for i in range(3):
        game_update = GameUpdateMessage(
            sender="validator_demo_key",
            data={
                "games": [
                    {
                        "id": f"game_{i+1}",
                        "status": "in_progress" if i < 2 else "completed",
                        "score": {"home": i*2, "away": i},
                        "teams": {"home": "Team A", "away": "Team B"},
                        "timestamp": datetime.now().isoformat()
                    }
                ]
            }
        )
        
        await manager.broadcast(game_update)
        await asyncio.sleep(1)
    
    logger.info("=== Validator WebSocket Demo Completed ===")

async def run_miner_demo():
    """Run a demonstration of miner WebSocket functionality"""
    logger.info("=== Starting Miner WebSocket Demo ===")
    
    # Create WebSocket manager
    manager = MinerWebSocketManager(hotkey="miner_demo_key")
    
    # Register handlers
    manager.register_handler("confirmation", handle_confirmation)
    manager.register_handler("game_update", handle_game_update)
    
    # Start the manager
    await manager.start()
    
    # Simulate sending predictions
    for i in range(3):
        prediction = PredictionMessage(
            sender="miner_demo_key",
            data={
                "id": f"pred_{i+1}",
                "game_id": f"game_{i+1}",
                "prediction": {
                    "home_win_probability": 0.5 + (i * 0.1),
                    "over_under": 45.5
                },
                "timestamp": datetime.now().isoformat()
            }
        )
        
        success = await manager.send_message(prediction)
        if success:
            logger.info(f"Successfully sent prediction {i+1}")
        else:
            logger.error(f"Failed to send prediction {i+1}")
        
        await asyncio.sleep(1)
    
    logger.info("=== Miner WebSocket Demo Completed ===")

async def run_demo(mode):
    """Run the appropriate demo based on mode"""
    if mode == "validator":
        await run_validator_demo()
    elif mode == "miner":
        await run_miner_demo()
    elif mode == "both":
        # Run both demos one after the other
        await run_validator_demo()
        logger.info("\n")
        await run_miner_demo()

def main():
    """Main entry point for the demo script"""
    parser = argparse.ArgumentParser(description="WebSocket Demo for Bettensor")
    parser.add_argument(
        "--mode", 
        type=str,
        choices=["validator", "miner", "both"],
        default="both",
        help="Which demo to run: validator, miner, or both"
    )
    
    args = parser.parse_args()
    
    try:
        asyncio.run(run_demo(args.mode))
    except KeyboardInterrupt:
        logger.info("Demo stopped by user")
    except Exception as e:
        logger.error(f"Error running demo: {str(e)}")

if __name__ == "__main__":
    main() 