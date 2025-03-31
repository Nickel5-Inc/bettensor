import asyncio
import json
import time
import traceback
from typing import Dict, Any, List, Optional, Set, Union
import bittensor as bt

from bettensor.protocol import SportsOddsSynapse
from bettensor.protocol import TeamGamePrediction

class PredictionManager:
    """Manages predictions for the miner"""
    def __init__(self, miner):
        """Initialize the prediction manager"""
        self.miner = miner
        self.active_games: Set[str] = set()
        self.pending_predictions: Dict[str, Dict] = {}
        
    async def update_active_games(self, games: List[Dict[str, Any]]):
        """
        Update the list of active games
        
        Args:
            games: List of game data dictionaries
        """
        if not games:
            bt.logging.warning("Received empty games list in update_active_games")
            return
            
        # Extract game IDs and filter for active games
        new_active_games = set()
        for game in games:
            game_id = game.get('game_id')
            status = game.get('status')
            if game_id and status == 'active':
                new_active_games.add(game_id)
                
        old_count = len(self.active_games)
        added = new_active_games - self.active_games
        removed = self.active_games - new_active_games
        
        # Update the active games set
        self.active_games = new_active_games
        
        bt.logging.info(f"Updated active games: {len(self.active_games)} total, {len(added)} added, {len(removed)} removed")
        
        # Log the game IDs at debug level
        if added:
            bt.logging.debug(f"New active games: {added}")
        if removed:
            bt.logging.debug(f"Games no longer active: {removed}")
            
    async def process_game_updates(self, validator_hotkey: str, game_data: Dict[str, Any]):
        """
        Process game updates received from a validator
        
        Args:
            validator_hotkey: The validator's hotkey
            game_data: Dictionary containing game updates
        """
        games = game_data.get('games', [])
        if not games:
            bt.logging.warning(f"Received empty game updates from validator {validator_hotkey}")
            return
            
        bt.logging.info(f"Processing {len(games)} game updates from validator {validator_hotkey}")
        
        # Update active games
        await self.update_active_games(games)
        
        # Process each game update
        for game in games:
            # You could add game-specific processing here
            pass
            
    async def generate_prediction(self, game_id: str) -> Optional[Dict[str, Any]]:
        """
        Generate a prediction for a game
        
        Args:
            game_id: The game ID to generate a prediction for
            
        Returns:
            Prediction data dictionary or None if prediction couldn't be generated
        """
        try:
            bt.logging.info(f"Generating prediction for game {game_id}")
            
            # Here you would implement your prediction model logic
            # For now we'll just create a simple dummy prediction
            
            # Example prediction data
            prediction = {
                "game_id": game_id,
                "home_score": 3,  # Example predicted home team score
                "away_score": 1,  # Example predicted away team score
                "confidence": 0.75,  # Optional confidence score
                "timestamp": time.time()
            }
            
            bt.logging.info(f"Generated prediction for game {game_id}: home={prediction['home_score']}, away={prediction['away_score']}")
            return prediction
            
        except Exception as e:
            bt.logging.error(f"Error generating prediction for game {game_id}: {str(e)}")
            bt.logging.error(traceback.format_exc())
            return None
            
    async def submit_prediction(self, game_id: str) -> bool:
        """
        Generate and submit a prediction for a game
        
        Args:
            game_id: The game ID to submit a prediction for
            
        Returns:
            True if prediction was successfully submitted, False otherwise
        """
        if game_id not in self.active_games:
            bt.logging.warning(f"Attempted to submit prediction for inactive game {game_id}")
            return False
            
        try:
            # Generate the prediction
            prediction_data = await self.generate_prediction(game_id)
            if not prediction_data:
                bt.logging.warning(f"Failed to generate prediction for game {game_id}")
                return False
                
            # Check if we have a prediction handler
            prediction_handler = getattr(self.miner, 'prediction_handler', None)
            if prediction_handler:
                # Use the prediction handler to submit (will try WebSocket first)
                bt.logging.info(f"Submitting prediction for game {game_id} via prediction handler")
                success = await prediction_handler.handle_prediction(game_id, prediction_data)
                return success
            
            # Fallback: use direct HTTP submission
            bt.logging.info(f"Submitting prediction for game {game_id} via HTTP")
            success = await self.submit_prediction_http(game_id, prediction_data)
            return success
            
        except Exception as e:
            bt.logging.error(f"Error submitting prediction for game {game_id}: {str(e)}")
            bt.logging.error(traceback.format_exc())
            return False
            
    async def submit_prediction_http(self, game_id: str, prediction_data: Dict[str, Any]) -> bool:
        """
        Submit a prediction via HTTP
        
        Args:
            game_id: The game ID to submit a prediction for
            prediction_data: The prediction data to submit
            
        Returns:
            True if prediction was successfully submitted, False otherwise
        """
        try:
            # Create a synapse
            synapse = TeamGamePrediction(
                game_id=game_id,
                home_score=prediction_data.get('home_score'),
                away_score=prediction_data.get('away_score'),
                prediction_id=f"{game_id}_{int(time.time())}",
                timestamp=prediction_data.get('timestamp', time.time())
            )
            
            # Get validator UIDs for top stake validators
            uids = self.miner.get_top_validators(k=3)  # Submit to top 3 validators
            if not uids:
                bt.logging.warning("No validators found, cannot submit prediction")
                return False
                
            # Submit the prediction to validators
            successful_submissions = 0
            for uid in uids:
                try:
                    # Create a copy of the synapse for each validator
                    validator_synapse = synapse.copy()
                    
                    # Submit the prediction
                    validator_hotkey = self.miner.metagraph.hotkeys[uid]
                    bt.logging.info(f"Submitting prediction for game {game_id} to validator {validator_hotkey} (UID {uid})")
                    
                    # Query the validator with the synapse
                    result = await self.miner.dendrite.forward(
                        axons=[self.miner.metagraph.axons[uid]],
                        synapse=validator_synapse,
                        timeout=30  # 30 second timeout
                    )
                    
                    # Check the response
                    if result and result.success:
                        bt.logging.info(f"Successfully submitted prediction for game {game_id} to validator {validator_hotkey}")
                        successful_submissions += 1
                    else:
                        error_msg = result.message if result else "No response"
                        bt.logging.warning(f"Failed to submit prediction to validator {validator_hotkey}: {error_msg}")
                        
                except Exception as e:
                    bt.logging.error(f"Error submitting prediction to validator UID {uid}: {str(e)}")
                    
            # Return success if at least one submission was successful
            return successful_submissions > 0
            
        except Exception as e:
            bt.logging.error(f"Error in submit_prediction_http for game {game_id}: {str(e)}")
            bt.logging.error(traceback.format_exc())
            return False 