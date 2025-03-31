"""
Game Service for the Bettensor Miner.

This module handles retrieval, caching, and management of game data
for various sports used in the prediction models.
"""

import asyncio
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Set

import bittensor as bt


class GameService:
    """
    Manages game data for the miner.
    
    Responsibilities:
    1. Retrieve game data from various sources
    2. Cache game data to reduce API calls
    3. Track active games for prediction generation
    4. Update game statuses based on validator updates
    """
    
    def __init__(self, event_manager, db_manager):
        """
        Initialize the game service.
        
        Args:
            event_manager: Event manager for publishing game events
            db_manager: Database manager for storing and retrieving game data
        """
        self.event_manager = event_manager
        self.db_manager = db_manager
        
        # Game cache by sport type
        self.game_cache = {
            "soccer": {},
            "football": {},
            # Add more sports as needed
        }
        
        # Last update timestamp by sport
        self.last_update = {
            "soccer": 0,
            "football": 0,
            # Add more sports as needed
        }
        
        # Update frequency in seconds by sport
        self.update_frequency = {
            "soccer": 3600,  # 1 hour
            "football": 3600,  # 1 hour
        }
        
        # Service state
        self.running = False
        
        # Subscribe to relevant events
        if self.event_manager:
            self.event_manager.subscribe("game_update", self._handle_game_update)
        
        bt.logging.info("GameService initialized")
    
    async def start(self):
        """Start the game service."""
        if self.running:
            bt.logging.warning("GameService is already running")
            return
        
        self.running = True
        bt.logging.info("GameService started")
    
    async def stop(self):
        """Stop the game service."""
        if not self.running:
            bt.logging.warning("GameService is not running")
            return
        
        self.running = False
        bt.logging.info("GameService stopped")
    
    async def get_games_by_sport(self, sport: str) -> List[Dict[str, Any]]:
        """
        Get active games for a specific sport.
        
        Args:
            sport: The sport type (e.g., "soccer", "football")
        
        Returns:
            A list of game data dictionaries
        """
        # Check if we need to update the cache
        current_time = time.time()
        if current_time - self.last_update.get(sport, 0) > self.update_frequency.get(sport, 3600):
            await self._update_games(sport)
        
        # Return games from cache
        return list(self.game_cache.get(sport, {}).values())
    
    async def _update_games(self, sport: str):
        """
        Update game data for a specific sport.
        
        Args:
            sport: The sport type to update
        """
        try:
            # In a real implementation, this would:
            # 1. Query the database for recent games
            # 2. If needed, call external APIs to get updates
            # 3. Store the updated data in the database
            # 4. Update the cache
            
            # For now, just update the timestamp
            self.last_update[sport] = time.time()
            
            # Publish event
            if self.event_manager:
                await self.event_manager.publish("games_updated", {
                    "sport": sport,
                    "count": len(self.game_cache.get(sport, {})),
                    "timestamp": self.last_update[sport]
                })
            
            bt.logging.info(f"Updated {len(self.game_cache.get(sport, {}))} {sport} games")
            
        except Exception as e:
            bt.logging.error(f"Error updating {sport} games: {str(e)}")
    
    async def _handle_game_update(self, event_type: str, data: Any):
        """
        Handle game update events from validators.
        
        Args:
            event_type: The type of event
            data: The event data
        """
        if not data or not isinstance(data, dict):
            return
        
        game_data = data.get("game_data")
        if not game_data:
            return
        
        # Extract game info and update cache
        game_id = game_data.get("game_id")
        sport = game_data.get("sport")
        
        if game_id and sport and sport in self.game_cache:
            self.game_cache[sport][game_id] = game_data
            
            # Publish event
            if self.event_manager:
                await self.event_manager.publish("game_state_changed", {
                    "game_id": game_id,
                    "sport": sport,
                    "state": game_data.get("state"),
                    "timestamp": time.time()
                })
    
    def get_stats(self) -> Dict[str, Any]:
        """
        Get statistics about the game service.
        
        Returns:
            A dictionary of statistics
        """
        stats = {
            "running": self.running,
            "games_by_sport": {
                sport: len(games) for sport, games in self.game_cache.items()
            },
            "last_update": {
                sport: datetime.fromtimestamp(timestamp).isoformat() 
                for sport, timestamp in self.last_update.items() 
                if timestamp > 0
            }
        }
        
        return stats 