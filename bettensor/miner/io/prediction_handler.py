import asyncio
import json
import time
from typing import Dict, Any, List, Optional
import bittensor as bt

# Remove custom logger
# import logging
# logger = logging.getLogger(__name__)

class PredictionHandler:
    """Handler for predictions, manages the queue and confirmation of predictions"""
    def __init__(self, miner):
        """Initialize the prediction handler with the miner instance"""
        self.miner = miner
        self.pending_predictions = {}  # Dict to track predictions waiting for confirmation
        self.prediction_timeout = 60  # Time in seconds after which a prediction is considered failed
        
    async def handle_prediction(self, game_id: str, prediction_data: Dict[str, Any]) -> bool:
        """
        Handle a prediction request, first trying WebSocket then falling back to HTTP
        Returns True if the prediction was successfully sent and confirmed
        """
        # Create a unique ID for this prediction
        prediction_id = f"{game_id}_{int(time.time())}"
        
        # First try WebSocket if available
        websocket_manager = getattr(self.miner, 'websocket_manager', None)
        if websocket_manager and websocket_manager.is_running and websocket_manager.has_connections():
            bt.logging.info(f"Sending prediction {prediction_id} via WebSocket")
            
            try:
                # Track this prediction for confirmation
                self.pending_predictions[prediction_id] = {
                    "timestamp": time.time(),
                    "game_id": game_id,
                    "prediction_data": prediction_data,
                    "confirmed": False,
                    "event": asyncio.Event()
                }
                
                # Add prediction ID to the data
                prediction_data_with_id = prediction_data.copy()
                prediction_data_with_id["prediction_id"] = prediction_id
                
                # Send the prediction to all connected validators
                sent_to = await websocket_manager.send_prediction(prediction_data_with_id)
                
                if not sent_to:
                    bt.logging.warning(f"Failed to send prediction {prediction_id} via WebSocket, falling back to HTTP")
                    # Clean up and fall back to HTTP
                    del self.pending_predictions[prediction_id]
                    return await self._send_via_http(game_id, prediction_data)
                
                # Wait for confirmation with timeout
                try:
                    # Wait for the confirmation event to be set
                    await asyncio.wait_for(self.pending_predictions[prediction_id]["event"].wait(), 
                                         timeout=self.prediction_timeout)
                    
                    # Check if it was confirmed
                    if self.pending_predictions[prediction_id]["confirmed"]:
                        bt.logging.info(f"Prediction {prediction_id} confirmed via WebSocket")
                        return True
                    else:
                        bt.logging.warning(f"Prediction {prediction_id} was not confirmed within timeout period")
                        # Fall back to HTTP
                        return await self._send_via_http(game_id, prediction_data)
                    
                except asyncio.TimeoutError:
                    bt.logging.warning(f"Timed out waiting for confirmation of prediction {prediction_id}, falling back to HTTP")
                    # Fall back to HTTP
                    return await self._send_via_http(game_id, prediction_data)
                finally:
                    # Clean up
                    if prediction_id in self.pending_predictions:
                        del self.pending_predictions[prediction_id]
                
            except Exception as e:
                bt.logging.error(f"Error sending prediction via WebSocket: {str(e)}")
                # Fall back to HTTP
                if prediction_id in self.pending_predictions:
                    del self.pending_predictions[prediction_id]
                return await self._send_via_http(game_id, prediction_data)
        else:
            bt.logging.info(f"WebSocket not available for prediction {prediction_id}, using HTTP")
            return await self._send_via_http(game_id, prediction_data)
    
    async def _send_via_http(self, game_id: str, prediction_data: Dict[str, Any]) -> bool:
        """Send the prediction via HTTP as a fallback"""
        bt.logging.info(f"Sending prediction for game {game_id} via HTTP")
        try:
            # Call the miner's HTTP method to send prediction
            result = await self.miner.send_prediction_http(game_id, prediction_data)
            bt.logging.info(f"Prediction for game {game_id} sent via HTTP with result: {result}")
            return result
        except Exception as e:
            bt.logging.error(f"Error sending prediction via HTTP: {str(e)}")
            return False
    
    async def handle_confirmation(self, validator_hotkey: str, confirmation_data: Dict[str, Any]):
        """Handle confirmation received from a validator via WebSocket"""
        prediction_id = confirmation_data.get("prediction_id")
        status = confirmation_data.get("status")
        
        if not prediction_id or prediction_id not in self.pending_predictions:
            bt.logging.warning(f"Received confirmation for unknown prediction ID: {prediction_id}")
            return
        
        bt.logging.info(f"Received confirmation for prediction {prediction_id} from validator {validator_hotkey} with status: {status}")
        
        # Mark prediction as confirmed if status is success
        if status == "success":
            self.pending_predictions[prediction_id]["confirmed"] = True
            
        # Set the event to signal waiting code
        self.pending_predictions[prediction_id]["event"].set()
    
    def cleanup_expired_predictions(self):
        """Clean up expired predictions that haven't received confirmations"""
        current_time = time.time()
        expired_ids = []
        
        for prediction_id, data in self.pending_predictions.items():
            if current_time - data["timestamp"] > self.prediction_timeout:
                expired_ids.append(prediction_id)
                # Set the event to unblock any waiting code
                data["event"].set()
        
        for prediction_id in expired_ids:
            bt.logging.warning(f"Prediction {prediction_id} expired without confirmation")
            del self.pending_predictions[prediction_id]
    
    async def prediction_cleanup_loop(self):
        """Background task to periodically clean up expired predictions"""
        while True:
            try:
                self.cleanup_expired_predictions()
            except Exception as e:
                bt.logging.error(f"Error in prediction cleanup loop: {str(e)}")
            
            await asyncio.sleep(10)  # Check every 10 seconds 