"""
Vesting rewards system for Bettensor.

This module implements a vesting rewards system that tracks miners who hold 
their rewards rather than immediately selling them, and provides multipliers
to their scores based on the amount and duration of holding.
"""

# Core components
from bettensor.validator.utils.vesting.core import (
    calculate_multiplier,
    get_tier_thresholds,
    configure_tiers,
    VestingScheduler,
    StakeTracker
)

# Blockchain components
from bettensor.validator.utils.vesting.blockchain import SubtensorClient

# Main system
from bettensor.validator.utils.vesting.system import VestingSystem

__all__ = [
    # Core components
    "calculate_multiplier",
    "get_tier_thresholds",
    "configure_tiers",
    "VestingScheduler",
    "StakeTracker",
    
    # Blockchain components
    "SubtensorClient",
    
    # Main system
    "VestingSystem"
] 