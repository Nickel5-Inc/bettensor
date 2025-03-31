"""
Weight service for setting weights on the network.

This service is responsible for setting the calculated scores as weights
on the Bittensor network.
"""

import asyncio
import time
import traceback
from typing import Dict, Any, Optional
from datetime import datetime, timezone
import bittensor as bt
import torch

from bettensor.validator.core.event_manager import EventManager, Event

class WeightService:
    """
    Service for setting weights on the network.
    
    This service:
    - Sets weights based on calculated scores
    - Handles weight setting errors and retries
    - Tracks weight setting history
    
    Weight setting is a critical operation that commits the validator's
    evaluation of miners to the blockchain.
    """
    
    def __init__(self, validator, event_manager: EventManager):
        """
        Initialize the weight service.
        
        Args:
            validator: The validator instance
            event_manager: The event manager for publishing events
        """
        self.validator = validator
        self.event_manager = event_manager
        self.weight_setter = validator.weight_setter
        
        # Last weight setting
        self.last_weights_set_time = 0
        self.last_success_time = 0
        self.weights_set_history = []
        self.MAX_HISTORY_ITEMS = 10
        
        # Statistics
        self.stats = {
            "weight_sets_attempted": 0,
            "weight_sets_succeeded": 0,
            "weight_sets_failed": 0,
            "errors": {}  # Error type -> count
        }
        
    async def set_weights(self, scores: Optional[torch.Tensor] = None) -> bool:
        """
        Set weights on the network.
        
        Args:
            scores: Optional scores to set as weights (uses validator.scores if not provided)
            
        Returns:
            True if weights were set successfully, False otherwise
        """
        # Use validator scores if not provided
        if scores is None:
            if not hasattr(self.validator, 'scores') or self.validator.scores is None:
                bt.logging.error("No scores available for setting weights")
                return False
            scores = self.validator.scores
            
        try:
            bt.logging.info("Setting weights on the network")
            
            # Update statistics
            self.stats["weight_sets_attempted"] += 1
            self.last_weights_set_time = time.time()
            
            # Set weights using existing weight_setter
            result = await asyncio.to_thread(
                self.weight_setter.set_weights,
                scores
            )
            
            bt.logging.info(f"Set weights result: {result}")
            
            # Update weight setting metrics
            current_time = time.time()
            weight_setting = {
                "timestamp": current_time,
                "success": bool(result),
                "score_sum": float(torch.sum(scores).item()) if isinstance(scores, torch.Tensor) else None,
                "score_min": float(torch.min(scores).item()) if isinstance(scores, torch.Tensor) else None,
                "score_max": float(torch.max(scores).item()) if isinstance(scores, torch.Tensor) else None
            }
            
            # Add to history
            self.weights_set_history.append(weight_setting)
            if len(self.weights_set_history) > self.MAX_HISTORY_ITEMS:
                self.weights_set_history.pop(0)
                
            if result is True:
                # Update success metrics
                self.stats["weight_sets_succeeded"] += 1
                self.last_success_time = current_time
                
                # Update block tracking
                if hasattr(self.validator, 'last_set_weights_block'):
                    self.validator.last_set_weights_block = self.validator.subtensor.block
                    
                # Publish event for weights set successfully
                await self.event_manager.publish(
                    "weights_set",
                    {
                        "success": True,
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                        "score_stats": {
                            "sum": weight_setting["score_sum"],
                            "min": weight_setting["score_min"],
                            "max": weight_setting["score_max"]
                        }
                    },
                    source="weight_service"
                )
                
                return True
            else:
                # Update failure metrics
                self.stats["weight_sets_failed"] += 1
                
                # Update block tracking to retry sooner
                if hasattr(self.validator, 'last_set_weights_block'):
                    self.validator.last_set_weights_block = self.validator.subtensor.block - 250
                    
                # Publish event for weights set failure
                await self.event_manager.publish(
                    "weights_set",
                    {
                        "success": False,
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                        "error": "Unknown error"
                    },
                    source="weight_service"
                )
                
                return False
                
        except Exception as e:
            bt.logging.error(f"Error setting weights: {str(e)}")
            bt.logging.debug(traceback.format_exc())
            
            # Update error statistics
            error_type = type(e).__name__
            self.stats["errors"][error_type] = self.stats["errors"].get(error_type, 0) + 1
            self.stats["weight_sets_failed"] += 1
            
            # Update block tracking to retry sooner
            if hasattr(self.validator, 'last_set_weights_block'):
                self.validator.last_set_weights_block = self.validator.subtensor.block - 250
                
            # Publish event for weights set error
            await self.event_manager.publish(
                "weights_set",
                {
                    "success": False,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "error": str(e),
                    "error_type": error_type
                },
                source="weight_service"
            )
            
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
            
    def get_stats(self) -> Dict[str, Any]:
        """Get statistics about weight setting."""
        stats = self.stats.copy()
        
        # Add timing information
        stats["last_weights_set_time"] = self.last_weights_set_time
        stats["last_success_time"] = self.last_success_time
        stats["seconds_since_last_set"] = time.time() - self.last_weights_set_time if self.last_weights_set_time > 0 else None
        stats["seconds_since_last_success"] = time.time() - self.last_success_time if self.last_success_time > 0 else None
        
        # Add blocks since last operations if available
        if hasattr(self.validator, 'subtensor') and self.validator.subtensor and hasattr(self.validator, 'last_set_weights_block'):
            current_block = self.validator.subtensor.block
            stats["blocks_since_weights"] = current_block - self.validator.last_set_weights_block
            
        # Add recent history summary
        if self.weights_set_history:
            recent_success_rate = sum(1 for item in self.weights_set_history if item["success"]) / len(self.weights_set_history)
            stats["recent_success_rate"] = recent_success_rate
            
        return stats
        
    def get_weight_history(self) -> list:
        """Get the history of weight setting operations."""
        return self.weights_set_history.copy() 