import asyncio
import json
import time
from typing import Dict, Any, List, Optional
import bittensor as bt

class PredictionHandler:
    """Handler for predictions, manages confirmations and WebSocket communication"""
    def __init__(self, validator):
        """Initialize with the validator instance"""
        self.validator = validator
        
    async def handle_prediction(self, miner_hotkey: str, prediction_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Handle a prediction from a miner
        
        Args:
            miner_hotkey: The miner's hotkey
            prediction_data: The prediction data
            
        Returns:
            Dict with status and optional error message
        """
        try:
            # Extract prediction details
            game_id = prediction_data.get("game_id")
            prediction_id = prediction_data.get("prediction_id")
            
            if not game_id:
                bt.logging.warning(f"Received prediction from {miner_hotkey} without game_id")
                return {
                    "status": "error",
                    "message": "Missing game_id in prediction"
                }
                
            bt.logging.info(f"Handling prediction for game {game_id} from miner {miner_hotkey}")
            
            # Validate the prediction
            if not self._validate_prediction(prediction_data):
                bt.logging.warning(f"Invalid prediction format from {miner_hotkey}")
                return {
                    "status": "error",
                    "message": "Invalid prediction format"
                }
                
            # Process the prediction
            result = await self._process_prediction(miner_hotkey, prediction_data)
            
            # Send confirmation via WebSocket if prediction_id is provided
            if prediction_id and hasattr(self.validator, 'websocket_manager'):
                websocket_manager = self.validator.websocket_manager
                
                if websocket_manager and websocket_manager.is_running():
                    # Include miner stats in the confirmation
                    miner_stats = self._get_miner_stats(miner_hotkey)
                    
                    # Send confirmation
                    asyncio.create_task(
                        websocket_manager.send_confirmation(
                            miner_hotkey=miner_hotkey,
                            prediction_id=prediction_id,
                            success=(result.get("status") == "success"),
                            message=result.get("message", ""),
                            miner_stats=miner_stats
                        )
                    )
                    
            return result
            
        except Exception as e:
            bt.logging.error(f"Error handling prediction from {miner_hotkey}: {str(e)}")
            import traceback
            bt.logging.error(traceback.format_exc())
            return {
                "status": "error",
                "message": f"Internal error: {str(e)}"
            }
    
    def _validate_prediction(self, prediction_data: Dict[str, Any]) -> bool:
        """
        Validate prediction data structure
        
        Args:
            prediction_data: The prediction data to validate
            
        Returns:
            True if valid, False otherwise
        """
        # Check required fields
        required_fields = ["game_id", "home_score", "away_score"]
        if not all(field in prediction_data for field in required_fields):
            bt.logging.warning(f"Prediction missing required fields: {required_fields}")
            return False
            
        # Check score values are valid numbers
        try:
            home_score = float(prediction_data["home_score"])
            away_score = float(prediction_data["away_score"])
            
            # Scores should be non-negative
            if home_score < 0 or away_score < 0:
                bt.logging.warning(f"Prediction contains negative scores: {home_score}, {away_score}")
                return False
                
        except (ValueError, TypeError):
            bt.logging.warning(f"Prediction contains invalid score values: {prediction_data.get('home_score')}, {prediction_data.get('away_score')}")
            return False
            
        return True
    
    async def _process_prediction(self, miner_hotkey: str, prediction_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process a validated prediction
        
        Args:
            miner_hotkey: The miner's hotkey
            prediction_data: The prediction data
            
        Returns:
            Result dict with status and optional message
        """
        try:
            # Add timestamp if not present
            if "timestamp" not in prediction_data:
                prediction_data["timestamp"] = time.time()
                
            # Add miner hotkey to the prediction
            prediction_data["miner_hotkey"] = miner_hotkey
            
            # Check if the game exists and is active
            game_id = prediction_data["game_id"]
            game = await self.validator.game_db.get_game(game_id)
            
            if not game:
                bt.logging.warning(f"Prediction for non-existent game {game_id} from {miner_hotkey}")
                return {
                    "status": "error",
                    "message": f"Game {game_id} not found"
                }
                
            if game.get("status") != "active":
                bt.logging.warning(f"Prediction for non-active game {game_id} from {miner_hotkey}")
                return {
                    "status": "error",
                    "message": f"Game {game_id} is not active"
                }
                
            # Store the prediction
            await self.validator.prediction_db.store_prediction(prediction_data)
            
            bt.logging.info(f"Stored prediction for game {game_id} from miner {miner_hotkey}")
            
            # Return success
            return {
                "status": "success",
                "message": f"Prediction for game {game_id} accepted"
            }
            
        except Exception as e:
            bt.logging.error(f"Error processing prediction: {str(e)}")
            import traceback
            bt.logging.error(traceback.format_exc())
            return {
                "status": "error",
                "message": f"Error processing prediction: {str(e)}"
            }
    
    def _get_miner_stats(self, miner_hotkey: str) -> Dict[str, Any]:
        """
        Get stats for a miner to include in confirmation messages
        
        Args:
            miner_hotkey: The miner's hotkey
            
        Returns:
            Dict with miner statistics
        """
        try:
            stats = {}
            
            # Add basic stats from metagraph if available
            if hasattr(self.validator, 'metagraph'):
                try:
                    uid = self.validator.metagraph.hotkeys.index(miner_hotkey)
                    stats["stake"] = float(self.validator.metagraph.S[uid])
                    stats["rank"] = float(self.validator.metagraph.R[uid])
                    stats["trust"] = float(self.validator.metagraph.T[uid])
                    stats["consensus"] = float(self.validator.metagraph.C[uid])
                    stats["incentive"] = float(self.validator.metagraph.I[uid])
                    stats["emission"] = float(self.validator.metagraph.E[uid])
                except (ValueError, IndexError):
                    # Hotkey not found in metagraph
                    pass
            
            # Add performance stats if available
            if hasattr(self.validator, 'scores_db'):
                # This would be implementation-specific
                pass
                
            return stats
            
        except Exception as e:
            bt.logging.error(f"Error getting miner stats for {miner_hotkey}: {str(e)}")
            return {} 