"""
Prediction service for processing and validating predictions.

This service handles the validation, storage, and notification
of predictions in the validator.
"""

import asyncio
import uuid
import time
import traceback
from typing import Dict, Any, Optional, List, Tuple, Union
from datetime import datetime, timezone
import bittensor as bt

from bettensor.protocol import TeamGamePrediction
from bettensor.validator.core.event_manager import EventManager, Event

class PredictionService:
    """
    Service for handling prediction validation, storage and notifications.
    
    This service:
    - Validates incoming predictions
    - Stores valid predictions in the database
    - Sends confirmations back to miners
    - Emits events for successful predictions
    
    It handles predictions from both WebSocket and HTTP sources.
    """
    
    def __init__(self, validator, event_manager: EventManager):
        """
        Initialize the prediction service.
        
        Args:
            validator: The validator instance
            event_manager: The event manager for publishing events
        """
        self.validator = validator
        self.event_manager = event_manager
        self.db_manager = validator.db_manager
        self.websocket_manager = None  # Will be set after initialization
        
        # Register event handlers
        self.event_manager.subscribe("prediction_received", self.handle_prediction_event)
        self.event_manager.subscribe("game_updated", self.handle_game_update_event)
        
        # Statistics
        self.stats = {
            "predictions_received": 0,
            "predictions_validated": 0,
            "predictions_rejected": 0,
            "predictions_stored": 0,
            "confirmations_sent": 0,
            "confirmations_failed": 0
        }
        
    def set_websocket_manager(self, websocket_manager):
        """Set the WebSocket manager after it's initialized."""
        self.websocket_manager = websocket_manager
        
    async def handle_prediction_event(self, event: Event):
        """
        Handle a prediction received event.
        
        Args:
            event: The event containing the prediction data
        """
        prediction_data = event.data
        source = event.source
        miner_hotkey = prediction_data.get("miner_hotkey")
        prediction = prediction_data.get("prediction")
        
        if not miner_hotkey or not prediction:
            bt.logging.error(f"Invalid prediction event data: missing miner_hotkey or prediction")
            return
            
        bt.logging.info(f"Processing prediction from {miner_hotkey} via {source}")
        self.stats["predictions_received"] += 1
        
        # Find miner UID from hotkey
        try:
            miner_uid = self.validator.metagraph.hotkeys.index(miner_hotkey)
        except ValueError:
            bt.logging.error(f"Unknown miner hotkey: {miner_hotkey}")
            await self._send_confirmation(
                miner_hotkey=miner_hotkey,
                prediction_id=prediction.prediction_id,
                success=False,
                message="Unknown miner hotkey"
            )
            self.stats["predictions_rejected"] += 1
            return
            
        # Validate prediction
        is_valid, message = await self.validate_prediction(miner_uid, prediction)
        
        if is_valid:
            # Store prediction
            success = await self.store_prediction(miner_uid, prediction)
            if success:
                self.stats["predictions_stored"] += 1
                
                # Get miner stats for the confirmation
                miner_stats = await self._get_miner_stats(miner_uid)
                
                # Send confirmation
                await self._send_confirmation(
                    miner_hotkey=miner_hotkey,
                    prediction_id=prediction.prediction_id,
                    success=True,
                    message="Prediction accepted",
                    miner_stats=miner_stats
                )
                
                # Publish event for successful prediction
                await self.event_manager.publish(
                    "prediction_accepted",
                    {
                        "miner_uid": miner_uid,
                        "miner_hotkey": miner_hotkey,
                        "prediction": prediction
                    },
                    source="prediction_service"
                )
            else:
                await self._send_confirmation(
                    miner_hotkey=miner_hotkey,
                    prediction_id=prediction.prediction_id,
                    success=False,
                    message="Error storing prediction"
                )
                self.stats["predictions_rejected"] += 1
        else:
            await self._send_confirmation(
                miner_hotkey=miner_hotkey,
                prediction_id=prediction.prediction_id,
                success=False,
                message=message
            )
            self.stats["predictions_rejected"] += 1
            
    async def handle_game_update_event(self, event: Event):
        """
        Handle a game update event.
        
        Args:
            event: The event containing the game update data
        """
        # When games are updated, we may need to reevaluate predictions
        game_updates = event.data
        
        if not game_updates:
            return
            
        # Check if any game outcomes have changed
        games_with_outcome_change = [
            game for game in game_updates 
            if "outcome" in game and game["outcome"] != 3  # 3 = Unfinished
        ]
        
        if games_with_outcome_change:
            bt.logging.info(f"Processing {len(games_with_outcome_change)} games with outcome changes")
            
            # Process game outcomes and update predictions
            # This could trigger rewards or other updates
            for game in games_with_outcome_change:
                game_id = game.get("external_id")
                if game_id:
                    await self._process_game_outcome(game_id, game["outcome"])
                    
    async def handle_http_prediction(self, synapse, miner_hotkey: str):
        """
        Handle a prediction received via HTTP.
        
        Args:
            synapse: The synapse containing the prediction
            miner_hotkey: The miner's hotkey
        """
        if not hasattr(synapse, "prediction_dict") or not synapse.prediction_dict:
            bt.logging.warning(f"Empty prediction received from {miner_hotkey}")
            return synapse
            
        # Process each prediction in the dict
        for prediction_id, prediction in synapse.prediction_dict.items():
            # Create event payload
            prediction_data = {
                "miner_hotkey": miner_hotkey,
                "prediction": prediction,
                "source": "http"
            }
            
            # Publish event
            await self.event_manager.publish(
                "prediction_received", 
                prediction_data,
                source="http_handler"
            )
            
        return synapse
        
    async def handle_websocket_prediction(self, miner_hotkey: str, prediction):
        """
        Handle a prediction received via WebSocket.
        
        Args:
            miner_hotkey: The miner's hotkey
            prediction: The prediction object
        """
        # Create event payload
        prediction_data = {
            "miner_hotkey": miner_hotkey,
            "prediction": prediction,
            "source": "websocket"
        }
        
        # Publish event
        await self.event_manager.publish(
            "prediction_received", 
            prediction_data,
            source="websocket_handler"
        )
        
    async def validate_prediction(self, 
                                miner_uid: int, 
                                prediction) -> Tuple[bool, str]:
        """
        Validate a prediction.
        
        Args:
            miner_uid: The miner's UID
            prediction: The prediction to validate
            
        Returns:
            Tuple of (is_valid, message)
        """
        self.stats["predictions_validated"] += 1
        
        try:
            # Check if prediction ID already exists
            existing_predictions = await self._get_existing_predictions([prediction.prediction_id])
            
            # Use existing validation logic from the validator
            is_valid, message, error_code, severity = await self.validator.validate_prediction(
                miner_uid=miner_uid,
                prediction_id=prediction.prediction_id,
                prediction_data=prediction.dict(),
                existing_predictions=existing_predictions
            )
            
            return is_valid, message
            
        except Exception as e:
            bt.logging.error(f"Error validating prediction: {str(e)}")
            bt.logging.debug(traceback.format_exc())
            return False, f"Validation error: {str(e)}"
            
    async def store_prediction(self, miner_uid: int, prediction) -> bool:
        """
        Store a valid prediction in the database.
        
        Args:
            miner_uid: The miner's UID
            prediction: The prediction to store
            
        Returns:
            True if successfully stored, False otherwise
        """
        try:
            # Use existing insertion logic
            predictions_dict = {prediction.prediction_id: prediction}
            await self.validator.insert_predictions([miner_uid], predictions_dict)
            return True
        except Exception as e:
            bt.logging.error(f"Error storing prediction: {str(e)}")
            bt.logging.debug(traceback.format_exc())
            return False
            
    async def _send_confirmation(self,
                              miner_hotkey: str,
                              prediction_id: str,
                              success: bool,
                              message: str,
                              miner_stats: Dict = None) -> bool:
        """
        Send confirmation to a miner.
        
        Args:
            miner_hotkey: The miner's hotkey
            prediction_id: The prediction ID
            success: Whether the prediction was successful
            message: Message to send to the miner
            miner_stats: Optional miner statistics to include
            
        Returns:
            True if confirmation was sent successfully, False otherwise
        """
        try:
            # Attempt to send via WebSocket first
            if self.websocket_manager:
                sent = await self.websocket_manager.send_confirmation(
                    miner_hotkey=miner_hotkey,
                    prediction_id=prediction_id,
                    success=success,
                    message=message,
                    miner_stats=miner_stats
                )
                
                if sent:
                    self.stats["confirmations_sent"] += 1
                    return True
                    
            # Fall back to HTTP if WebSocket failed or not available
            bt.logging.debug(f"Using HTTP fallback for confirmation to {miner_hotkey}")
            
            # Create confirmation synapse (using existing validator method)
            success_str = "true" if success else "false"
            confirmation = {
                prediction_id: {
                    "success": success_str,
                    "message": message
                }
            }
            
            if miner_stats:
                confirmation[prediction_id]["miner_stats"] = miner_stats
                
            # Send confirmation via HTTP
            await self.validator.send_confirmation_synapse(
                miner_hotkey=miner_hotkey,
                confirmation=confirmation
            )
            
            self.stats["confirmations_sent"] += 1
            return True
            
        except Exception as e:
            bt.logging.error(f"Error sending confirmation to {miner_hotkey}: {str(e)}")
            bt.logging.debug(traceback.format_exc())
            self.stats["confirmations_failed"] += 1
            return False
            
    async def _get_miner_stats(self, miner_uid: int) -> Dict:
        """
        Get miner statistics from the database.
        
        Args:
            miner_uid: The miner's UID
            
        Returns:
            Dictionary of miner statistics
        """
        try:
            miner_stats = await self.db_manager.fetch_one(
                "SELECT * FROM miner_stats WHERE miner_uid = ?", (miner_uid,)
            )
            
            # Convert to dictionary if not None
            return dict(miner_stats) if miner_stats else {}
            
        except Exception as e:
            bt.logging.error(f"Error getting miner stats: {str(e)}")
            return {}
            
    async def _get_existing_predictions(self, prediction_ids: List[str]) -> Dict[str, Any]:
        """
        Check if predictions already exist in the database.
        
        Args:
            prediction_ids: List of prediction IDs to check
            
        Returns:
            Dictionary of existing predictions
        """
        try:
            placeholders = ','.join(['?'] * len(prediction_ids))
            query = f"SELECT prediction_id FROM predictions WHERE prediction_id IN ({placeholders})"
            
            existing = await self.db_manager.fetch_all(query, prediction_ids)
            return {row['prediction_id']: True for row in existing}
            
        except Exception as e:
            bt.logging.error(f"Error checking existing predictions: {str(e)}")
            return {}
            
    async def _process_game_outcome(self, game_id: str, outcome: int) -> None:
        """
        Process a game outcome update.
        
        Args:
            game_id: The game ID
            outcome: The game outcome (0=TeamA, 1=TeamB, 2=Draw, 3=Unfinished)
        """
        bt.logging.info(f"Processing outcome for game {game_id}: {outcome}")
        
        # Get predictions for this game
        query = "SELECT * FROM predictions WHERE game_id = ? AND outcome = 'Pending'"
        predictions = await self.db_manager.fetch_all(query, (game_id,))
        
        if not predictions:
            bt.logging.debug(f"No pending predictions found for game {game_id}")
            return
            
        bt.logging.info(f"Found {len(predictions)} pending predictions for game {game_id}")
        
        # Process each prediction
        for prediction in predictions:
            miner_uid = prediction['miner_uid']
            prediction_id = prediction['prediction_id']
            
            # Determine if prediction was correct
            predicted_outcome = prediction['predicted_outcome']
            wager = float(prediction['wager'])
            predicted_odds = float(prediction['predicted_odds'])
            
            # Map outcome to team
            game_query = "SELECT team_a, team_b FROM game_data WHERE external_id = ?"
            game = await self.db_manager.fetch_one(game_query, (game_id,))
            
            if not game:
                bt.logging.warning(f"Game {game_id} not found in database")
                continue
                
            actual_winner = ""
            if outcome == 0:
                actual_winner = game['team_a']
            elif outcome == 1:
                actual_winner = game['team_b']
            elif outcome == 2:
                actual_winner = "Tie"
                
            # Calculate payout
            is_correct = predicted_outcome == actual_winner
            payout = wager * predicted_odds if is_correct else 0.0
            
            # Update prediction
            await self.db_manager.execute(
                """
                UPDATE predictions 
                SET outcome = ?, payout = ? 
                WHERE prediction_id = ?
                """,
                (actual_winner, payout, prediction_id)
            )
            
            # Update miner stats
            if is_correct:
                await self.db_manager.execute(
                    """
                    UPDATE miner_stats 
                    SET miner_cash = miner_cash + ?, 
                        miner_lifetime_earnings = miner_lifetime_earnings + ?,
                        miner_lifetime_wins = miner_lifetime_wins + 1
                    WHERE miner_uid = ?
                    """,
                    (payout, payout, miner_uid)
                )
            else:
                await self.db_manager.execute(
                    """
                    UPDATE miner_stats 
                    SET miner_lifetime_losses = miner_lifetime_losses + 1
                    WHERE miner_uid = ?
                    """,
                    (miner_uid,)
                )
                
            # Publish event for prediction outcome
            await self.event_manager.publish(
                "prediction_settled",
                {
                    "prediction_id": prediction_id,
                    "miner_uid": miner_uid,
                    "game_id": game_id,
                    "is_correct": is_correct,
                    "payout": payout,
                    "predicted_outcome": predicted_outcome,
                    "actual_outcome": actual_winner
                },
                source="prediction_service"
            )
            
    def get_stats(self) -> Dict[str, Any]:
        """Get statistics about prediction processing."""
        return self.stats.copy() 