"""
Communication manager for handling WebSocket and HTTP communications.

This module provides a unified interface for communicating with miners
via both WebSocket and HTTP methods, with WebSocket being the preferred
method when available.
"""

import asyncio
import time
import traceback
from typing import Dict, List, Set, Any, Optional, Tuple
import bittensor as bt

from bettensor.protocol import GameData
from bettensor.validator.core.event_manager import EventManager, Event

class CommunicationManager:
    """
    Manages all communication channels between validator and miners.
    
    This class:
    - Manages WebSocket connections to miners
    - Provides HTTP fallback for miners without WebSocket
    - Handles broadcasting game updates to miners
    - Manages confirmations for predictions
    - Tracks connection statistics
    
    Communication is prioritized through WebSocket when available,
    falling back to HTTP only when necessary.
    """
    
    def __init__(self, validator, event_manager: EventManager):
        """
        Initialize the communication manager.
        
        Args:
            validator: The validator instance
            event_manager: The event manager for publishing events
        """
        self.validator = validator
        self.event_manager = event_manager
        self.websocket_manager = None  # Will be set after initialization
        
        # Connection tracking
        self.connected_miners: Set[str] = set()
        self.connection_stats: Dict[str, Dict[str, Any]] = {}
        
        # Register event handlers
        self.event_manager.subscribe("game_updated", self.handle_game_update_event)
        self.event_manager.subscribe("prediction_accepted", self.handle_prediction_accepted_event)
        self.event_manager.subscribe("prediction_rejected", self.handle_prediction_rejected_event)
        self.event_manager.subscribe("miner_connected", self.handle_miner_connected_event)
        self.event_manager.subscribe("miner_disconnected", self.handle_miner_disconnected_event)
        
        # Statistics
        self.stats = {
            "websocket_broadcasts": 0,
            "http_fallbacks": 0,
            "game_updates_sent": 0,
            "confirmations_sent": 0,
            "connection_events": 0
        }
        
    def set_websocket_manager(self, websocket_manager):
        """Set the WebSocket manager after it's initialized."""
        self.websocket_manager = websocket_manager
        
    async def start(self):
        """Start the communication manager."""
        bt.logging.info("Communication manager started")
        
    async def stop(self):
        """Stop the communication manager and clean up resources."""
        bt.logging.info("Communication manager stopped")
        
    async def handle_game_update_event(self, event: Event):
        """
        Handle a game update event.
        
        Args:
            event: The event containing the game update data
        """
        game_updates = event.data
        
        if not game_updates:
            return
            
        bt.logging.info(f"Broadcasting {len(game_updates)} game updates to miners")
        await self.broadcast_game_updates(game_updates)
        
    async def handle_prediction_accepted_event(self, event: Event):
        """
        Handle a prediction accepted event.
        
        Args:
            event: The event containing the prediction data
        """
        prediction_data = event.data
        miner_hotkey = prediction_data.get("miner_hotkey")
        prediction = prediction_data.get("prediction")
        
        if not miner_hotkey or not prediction:
            return
            
        # Process confirmation in a separate task to avoid blocking
        asyncio.create_task(
            self.send_confirmation(
                miner_hotkey=miner_hotkey,
                prediction_id=prediction.prediction_id,
                success=True,
                message="Prediction accepted",
                miner_stats=prediction_data.get("miner_stats")
            )
        )
        
    async def handle_prediction_rejected_event(self, event: Event):
        """
        Handle a prediction rejected event.
        
        Args:
            event: The event containing the prediction data
        """
        prediction_data = event.data
        miner_hotkey = prediction_data.get("miner_hotkey")
        prediction_id = prediction_data.get("prediction_id")
        message = prediction_data.get("message", "Prediction rejected")
        
        if not miner_hotkey or not prediction_id:
            return
            
        # Process confirmation in a separate task to avoid blocking
        asyncio.create_task(
            self.send_confirmation(
                miner_hotkey=miner_hotkey,
                prediction_id=prediction_id,
                success=False,
                message=message
            )
        )
        
    async def handle_miner_connected_event(self, event: Event):
        """
        Handle a miner connected event.
        
        Args:
            event: The event containing the connection data
        """
        miner_hotkey = event.data.get("miner_hotkey")
        
        if not miner_hotkey:
            return
            
        self.connected_miners.add(miner_hotkey)
        self.stats["connection_events"] += 1
        
        # Track connection stats
        self.connection_stats[miner_hotkey] = {
            "connected_at": time.time(),
            "last_activity": time.time(),
            "is_connected": True,
            "connection_count": self.connection_stats.get(miner_hotkey, {}).get("connection_count", 0) + 1
        }
        
        bt.logging.info(f"Miner {miner_hotkey} connected, total connected: {len(self.connected_miners)}")
        
    async def handle_miner_disconnected_event(self, event: Event):
        """
        Handle a miner disconnected event.
        
        Args:
            event: The event containing the disconnection data
        """
        miner_hotkey = event.data.get("miner_hotkey")
        
        if not miner_hotkey:
            return
            
        if miner_hotkey in self.connected_miners:
            self.connected_miners.remove(miner_hotkey)
            
        self.stats["connection_events"] += 1
        
        # Update connection stats
        if miner_hotkey in self.connection_stats:
            self.connection_stats[miner_hotkey]["is_connected"] = False
            self.connection_stats[miner_hotkey]["disconnected_at"] = time.time()
            
        bt.logging.info(f"Miner {miner_hotkey} disconnected, total connected: {len(self.connected_miners)}")
        
    async def broadcast_game_updates(self, games: List[Dict[str, Any]]) -> Tuple[int, int]:
        """
        Broadcast game updates to all miners.
        
        Args:
            games: List of game data to broadcast
            
        Returns:
            Tuple of (websocket_sent, http_sent) counts
        """
        websocket_sent = 0
        http_sent = 0
        
        try:
            # First try WebSocket for all connected miners
            if self.websocket_manager and self.connected_miners:
                result = await self.websocket_manager.broadcast_game_updates(games)
                websocket_sent = result.get("sent", 0)
                self.stats["websocket_broadcasts"] += 1
                
            # Get important miners that are not connected via WebSocket
            if self.validator.config.get("enable_http_fallback", True):
                disconnected_miners = self.get_important_disconnected_miners()
                
                if disconnected_miners:
                    bt.logging.info(f"Sending game updates via HTTP to {len(disconnected_miners)} important disconnected miners")
                    
                    # Convert games to a format suitable for HTTP
                    gamedata_dict = {}
                    for game in games:
                        # Convert the game data to TeamGame format
                        game_id = game.get("game_id") or game.get("external_id")
                        if game_id:
                            gamedata_dict[game_id] = self._convert_game_for_synapse(game)
                            
                    # Create synapse
                    synapse = GameData.create(
                        db_path=self.validator.db_path,
                        wallet=self.validator.wallet,
                        subnet_version=self.validator.subnet_version,
                        neuron_uid=self.validator.uid,
                        synapse_type="game_data",
                        gamedata_dict=gamedata_dict,
                    )
                    
                    # Send to each disconnected miner
                    for miner_hotkey in disconnected_miners:
                        # Find the axon for this miner
                        try:
                            uid = self.validator.metagraph.hotkeys.index(miner_hotkey)
                            axon = self.validator.metagraph.axons[uid]
                            
                            # Send via HTTP
                            await self.validator.dendrite.forward(
                                axons=[axon],
                                synapse=synapse,
                                timeout=self.validator.timeout,
                                deserialize=False,
                            )
                            
                            http_sent += 1
                            self.stats["http_fallbacks"] += 1
                            
                        except (ValueError, IndexError) as e:
                            bt.logging.warning(f"Error sending HTTP update to {miner_hotkey}: {str(e)}")
                    
            # Update stats
            self.stats["game_updates_sent"] += (websocket_sent + http_sent)
            
            bt.logging.info(f"Game updates sent: {websocket_sent} via WebSocket, {http_sent} via HTTP")
            return websocket_sent, http_sent
            
        except Exception as e:
            bt.logging.error(f"Error broadcasting game updates: {str(e)}")
            bt.logging.debug(traceback.format_exc())
            return websocket_sent, http_sent
            
    async def send_confirmation(self,
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
            websocket_sent = False
            if self.websocket_manager and miner_hotkey in self.connected_miners:
                websocket_sent = await self.websocket_manager.send_confirmation(
                    miner_hotkey=miner_hotkey,
                    prediction_id=prediction_id,
                    success=success,
                    message=message,
                    miner_stats=miner_stats
                )
                
            # Fall back to HTTP if WebSocket failed or not available
            if not websocket_sent:
                # Create confirmation
                success_str = "true" if success else "false"
                confirmation = {
                    prediction_id: {
                        "success": success_str,
                        "message": message
                    }
                }
                
                if miner_stats:
                    confirmation[prediction_id]["miner_stats"] = miner_stats
                    
                # Send via HTTP
                await self.validator.send_confirmation_synapse(
                    miner_hotkey=miner_hotkey,
                    confirmation=confirmation
                )
                
                self.stats["http_fallbacks"] += 1
                
            self.stats["confirmations_sent"] += 1
            return True
            
        except Exception as e:
            bt.logging.error(f"Error sending confirmation to {miner_hotkey}: {str(e)}")
            bt.logging.debug(traceback.format_exc())
            return False
            
    def get_important_disconnected_miners(self) -> List[str]:
        """
        Get a list of important miners that are not connected via WebSocket.
        
        Returns:
            List of miner hotkeys that should receive HTTP fallback
        """
        if not hasattr(self.validator, 'metagraph') or not self.validator.metagraph:
            return []
            
        # Get disconnected miners (all miners - connected miners)
        all_miners = set(self.validator.metagraph.hotkeys)
        disconnected_miners = all_miners - self.connected_miners
        
        # Filter to important miners (e.g., high-stake miners)
        important_miners = []
        try:
            # Get top 25% of miners by stake
            if hasattr(self.validator.metagraph, 'total_stake'):
                uids = range(len(self.validator.metagraph.hotkeys))
                hotkeys = self.validator.metagraph.hotkeys
                stakes = self.validator.metagraph.total_stake
                
                # Sort miners by stake
                miners_by_stake = sorted(
                    [(hotkeys[uid], stakes[uid].item()) for uid in uids],
                    key=lambda x: x[1],
                    reverse=True
                )
                
                # Take top 25%
                top_miners = miners_by_stake[:max(1, len(miners_by_stake) // 4)]
                top_miner_hotkeys = {miner[0] for miner in top_miners}
                
                # Filter disconnected miners to only include important ones
                important_miners = [
                    hotkey for hotkey in disconnected_miners 
                    if hotkey in top_miner_hotkeys
                ]
                
        except Exception as e:
            bt.logging.error(f"Error determining important miners: {str(e)}")
            bt.logging.debug(traceback.format_exc())
            
        return important_miners
        
    def get_connected_miners(self) -> Set[str]:
        """Get the set of miners currently connected via WebSocket."""
        return self.connected_miners.copy()
        
    def get_connection_stats(self) -> Dict[str, Dict[str, Any]]:
        """Get connection statistics."""
        return self.connection_stats.copy()
        
    def get_stats(self) -> Dict[str, Any]:
        """Get communication statistics."""
        stats = self.stats.copy()
        stats["connected_miners"] = len(self.connected_miners)
        return stats
        
    def _convert_game_for_synapse(self, game: Dict[str, Any]) -> Any:
        """
        Convert a game dictionary to a format suitable for GameData synapse.
        
        Args:
            game: Game data dictionary
            
        Returns:
            TeamGame object or compatible dictionary
        """
        # Import here to avoid circular imports
        from bettensor.protocol import TeamGame
        
        try:
            # Extract fields, with appropriate type conversions
            game_id = game.get("game_id") or game.get("external_id", "")
            team_a = game.get("team_a", "")
            team_b = game.get("team_b", "")
            sport = game.get("sport", "")
            league = game.get("league", "")
            create_date = game.get("create_date", "")
            last_update_date = game.get("last_update_date", "")
            event_start_date = game.get("event_start_date", "")
            active = bool(game.get("active", 1))
            outcome = str(game.get("outcome", "3"))
            team_a_odds = float(game.get("team_a_odds", 0.0))
            team_b_odds = float(game.get("team_b_odds", 0.0))
            tie_odds = float(game.get("tie_odds", 0.0))
            can_tie = bool(game.get("can_tie", 0))
            
            # Create TeamGame object
            return TeamGame(
                game_id=game_id,
                team_a=team_a,
                team_b=team_b,
                sport=sport,
                league=league,
                create_date=create_date,
                last_update_date=last_update_date,
                event_start_date=event_start_date,
                active=active,
                outcome=outcome,
                team_a_odds=team_a_odds,
                team_b_odds=team_b_odds,
                tie_odds=tie_odds,
                can_tie=can_tie
            )
        except Exception as e:
            bt.logging.error(f"Error converting game for synapse: {str(e)}")
            bt.logging.debug(traceback.format_exc())
            return None 