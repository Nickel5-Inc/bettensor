"""
Scoring service for calculating miner scores.

This service handles the periodic calculation of miner scores
based on prediction performance and other metrics.
"""

import asyncio
import time
import traceback
from typing import Dict, List, Set, Any, Optional, Tuple
from datetime import datetime, timezone
import bittensor as bt
import torch

from bettensor.validator.core.event_manager import EventManager, Event

class ScoringService:
    """
    Service for calculating and managing miner scores.
    
    This service:
    - Periodically recalculates miner scores
    - Listens for events that affect scoring
    - Publishes score update events
    - Manages the scoring system state
    
    The scoring calculations are performed on a schedule to avoid
    recalculating scores with every new prediction, which would be inefficient.
    """
    
    def __init__(self, validator, event_manager: EventManager):
        """
        Initialize the scoring service.
        
        Args:
            validator: The validator instance
            event_manager: The event manager for publishing events
        """
        self.validator = validator
        self.event_manager = event_manager
        self.scoring_system = validator.scoring_system
        self.alpha = getattr(validator.config, 'alpha', 0.9)
        
        # Last scoring run
        self.last_scoring_time = 0
        self.last_weights_set_time = 0
        
        # Register event handlers
        self.event_manager.subscribe("prediction_settled", self.handle_prediction_settled_event)
        self.event_manager.subscribe("miner_updated", self.handle_miner_updated_event)
        
        # Statistics
        self.stats = {
            "scoring_runs": 0,
            "weights_set": 0,
            "scores_skipped": 0,
            "prediction_settlements": 0,
            "errors": 0
        }
        
    async def handle_prediction_settled_event(self, event: Event):
        """
        Handle a prediction settled event.
        
        Args:
            event: The event containing the settled prediction data
        """
        self.stats["prediction_settlements"] += 1
        # We don't need to recalculate scores immediately
        # Just track that we got a settlement for stats
        
    async def handle_miner_updated_event(self, event: Event):
        """
        Handle a miner updated event.
        
        Args:
            event: The event containing the miner update data
        """
        # Miner metadata (stakes, etc.) was updated
        # This might trigger a score recalculation depending on the update
        update_type = event.data.get("update_type")
        if update_type == "stake_changed":
            # Just note that a stake change happened
            # Actual recalculation happens on schedule
            bt.logging.debug(f"Stake change detected for {event.data.get('miner_hotkey')}")
        
    async def run_scoring(self, current_time: Optional[datetime] = None) -> bool:
        """
        Run the scoring calculation.
        
        Args:
            current_time: Optional current time for testing
            
        Returns:
            True if scoring was run, False if skipped
        """
        # Get current time if not provided
        if current_time is None:
            current_time = datetime.now(timezone.utc)
            
        # Update statistics
        self.stats["scoring_runs"] += 1
        self.last_scoring_time = time.time()
        
        try:
            bt.logging.info("Running scoring calculations")
            
            # Get UIDs to query and invalid UIDs
            (
                _,
                list_of_uids,
                blacklisted_uids,
                uids_not_to_query,
            ) = self.validator.get_uids_to_query(self.validator.metagraph.axons)

            valid_uids = set(list_of_uids)
            # Combine blacklisted_uids and uids_not_to_query
            invalid_uids = set(blacklisted_uids + uids_not_to_query)
            bt.logging.info(f"Invalid UIDs: {invalid_uids}")
            
            # Run scoring system
            new_scores = await self.scoring_system.scoring_run(
                current_time, invalid_uids, valid_uids
            )
            
            if new_scores is None:
                bt.logging.error("Scoring system returned None scores")
                self.stats["errors"] += 1
                return False
                
            # Apply penalty to blacklisted miners
            for uid in blacklisted_uids:
                if uid is not None and uid < len(new_scores):
                    bt.logging.debug(
                        f"Setting score for blacklisted UID: {uid}. Old score: {new_scores[uid]}"
                    )
                    new_scores[uid] = (
                        self.alpha * new_scores[uid]
                        + (1 - self.alpha) * 0.0
                    )
                    bt.logging.debug(
                        f"Set score for blacklisted UID: {uid}. New score: {new_scores[uid]}"
                    )

            # Apply penalty to miners not queried
            for uid in uids_not_to_query:
                if uid is not None and uid < len(new_scores):
                    bt.logging.debug(
                        f"Setting score for not queried UID: {uid}. Old score: {new_scores[uid]}"
                    )
                    new_scores[uid] = (
                        self.alpha * new_scores[uid]
                        + (1 - self.alpha) * 0.0
                    )
                    bt.logging.debug(
                        f"Set score for not queried UID: {uid}. New score: {new_scores[uid]}"
                    )
                    
            # Update validator scores
            self.validator.scores = new_scores
            
            # Save state
            await self.validator.save_state()
            
            # Publish event for scoring complete
            await self.event_manager.publish(
                "scoring_complete",
                {
                    "scores": new_scores.tolist(),
                    "timestamp": current_time.isoformat()
                },
                source="scoring_service"
            )
            
            return True
            
        except Exception as e:
            bt.logging.error(f"Error in scoring run: {str(e)}")
            bt.logging.debug(traceback.format_exc())
            self.stats["errors"] += 1
            return False
            
    async def set_weights(self, weights_to_set: Optional[torch.Tensor] = None) -> bool:
        """
        Set weights based on calculated scores.
        
        Args:
            weights_to_set: Optional weights to set (uses validator.scores if not provided)
            
        Returns:
            True if weights were set successfully, False otherwise
        """
        # Use validator scores if not provided
        if weights_to_set is None:
            if self.validator.scores is None:
                bt.logging.error("No scores available for setting weights")
                return False
            weights_to_set = self.validator.scores
            
        try:
            bt.logging.info("Setting weights")
            
            # Update statistics
            self.stats["weights_set"] += 1
            self.last_weights_set_time = time.time()
            
            # Run the weight setter in a thread to not block the event loop
            result = await asyncio.to_thread(
                self.validator.weight_setter.set_weights,
                weights_to_set
            )
            
            bt.logging.info(f"Set weights result: {result}")
            
            if result is True:
                # Update last set weights block
                self.validator.last_set_weights_block = self.validator.subtensor.block
                
                # Publish event for weights set
                await self.event_manager.publish(
                    "weights_set",
                    {
                        "result": result,
                        "timestamp": datetime.now(timezone.utc).isoformat()
                    },
                    source="scoring_service"
                )
                
                return True
            else:
                # Set last set weights block to trigger retry soon
                if hasattr(self.validator, 'last_set_weights_block'):
                    self.validator.last_set_weights_block = self.validator.subtensor.block - 250
                    
                return False
                
        except Exception as e:
            bt.logging.error(f"Error setting weights: {str(e)}")
            bt.logging.debug(traceback.format_exc())
            self.stats["errors"] += 1
            
            # Set last set weights block to trigger retry soon
            if hasattr(self.validator, 'last_set_weights_block'):
                self.validator.last_set_weights_block = self.validator.subtensor.block - 250
                
            return False
            
    async def should_run_scoring(self) -> bool:
        """
        Determine if scoring should be run based on current state.
        
        Returns:
            True if scoring should be run, False otherwise
        """
        # Check if we have the subtensor block
        if not hasattr(self.validator, 'subtensor') or not self.validator.subtensor:
            return False
            
        try:
            current_block = self.validator.subtensor.block
            
            # Check if we're past the scoring interval
            if not hasattr(self.validator, 'last_scoring_block') or not hasattr(self.validator, 'scoring_interval'):
                return True
                
            blocks_since_scoring = current_block - self.validator.last_scoring_block
            return blocks_since_scoring > self.validator.scoring_interval
            
        except Exception as e:
            bt.logging.error(f"Error checking if scoring should run: {str(e)}")
            return False
            
    async def should_set_weights(self) -> bool:
        """
        Determine if weights should be set based on current state.
        
        Returns:
            True if weights should be set, False otherwise
        """
        # Check if we have the subtensor block
        if not hasattr(self.validator, 'subtensor') or not self.validator.subtensor:
            return False
            
        try:
            current_block = self.validator.subtensor.block
            
            # Check if we're past the set weights interval
            if not hasattr(self.validator, 'last_set_weights_block') or not hasattr(self.validator, 'set_weights_interval'):
                return True
                
            blocks_since_weights = current_block - self.validator.last_set_weights_block
            return blocks_since_weights > self.validator.set_weights_interval
            
        except Exception as e:
            bt.logging.error(f"Error checking if weights should be set: {str(e)}")
            return False
            
    async def rebuild_historical_scores(self) -> bool:
        """
        Rebuild historical scoring data.
        
        Returns:
            True if rebuild was successful, False otherwise
        """
        try:
            bt.logging.info("Rebuilding historical scores")
            await self.scoring_system.rebuild_historical_scores()
            bt.logging.info("Historical scores rebuilt successfully")
            return True
        except Exception as e:
            bt.logging.error(f"Error rebuilding historical scores: {str(e)}")
            bt.logging.debug(traceback.format_exc())
            return False
            
    def get_stats(self) -> Dict[str, Any]:
        """Get statistics about scoring processing."""
        stats = self.stats.copy()
        
        # Add timing information
        stats["last_scoring_time"] = self.last_scoring_time
        stats["last_weights_set_time"] = self.last_weights_set_time
        
        # Add blocks since last operations if available
        if hasattr(self.validator, 'subtensor') and self.validator.subtensor:
            current_block = self.validator.subtensor.block
            
            if hasattr(self.validator, 'last_scoring_block'):
                stats["blocks_since_scoring"] = current_block - self.validator.last_scoring_block
                
            if hasattr(self.validator, 'last_set_weights_block'):
                stats["blocks_since_weights"] = current_block - self.validator.last_set_weights_block
                
        return stats 