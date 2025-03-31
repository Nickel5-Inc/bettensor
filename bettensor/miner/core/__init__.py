"""
Core functionality for the BettensorMiner.

This module provides the core services for the miner implementation,
including event handling, prediction generation, and communication.
"""

# Import services as they are implemented
from bettensor.miner.core.event_manager import EventManager
from bettensor.miner.core.task_manager import TaskManager
from bettensor.miner.core.communication_service import CommunicationService, ValidatorConnection
from bettensor.miner.core.event_driven_miner import EventDrivenMiner
from bettensor.miner.core.prediction_service import PredictionService
from bettensor.miner.core.game_service import GameService
from bettensor.miner.core.database_manager import DatabaseManager

__all__ = [
    "EventManager",
    "TaskManager",
    "CommunicationService",
    "ValidatorConnection",
    "EventDrivenMiner",
    "PredictionService",
    "GameService",
    "DatabaseManager",
    # Future services will be added here as they are implemented:
    # "StatisticsService",
] 