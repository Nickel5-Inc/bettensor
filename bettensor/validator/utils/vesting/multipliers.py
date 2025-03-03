"""
Multiplier calculation for the vesting rewards system.
"""

import logging
from typing import Dict, Tuple

logger = logging.getLogger(__name__)

# Define multiplier tiers
# Format: (threshold_value, multiplier_value)
AMOUNT_HELD_TIERS = [
    (0.3, 1.1),  # >30% held: 1.1x multiplier
    (0.5, 1.2),  # >50% held: 1.2x multiplier
    (0.7, 1.3),  # >70% held: 1.3x multiplier
]

TIME_HELD_TIERS = [
    (14, 1.1),   # >2 weeks: 1.1x multiplier
    (30, 1.2),   # >1 month: 1.2x multiplier
    (60, 1.3),   # >2 months: 1.3x multiplier
]

# Maximum multiplier (1.5x)
MAX_MULTIPLIER = 1.5


def calculate_multiplier(holding_percentage: float, holding_duration_days: int) -> float:
    """
    Calculate the score multiplier based on holding percentage and duration.
    
    The multiplier is calculated from both the holding percentage and duration,
    with a maximum combined value of MAX_MULTIPLIER.
    
    Args:
        holding_percentage: The percentage of rewards being held (0.0-1.0)
        holding_duration_days: The duration rewards have been held in days
        
    Returns:
        The score multiplier (1.0-1.5)
    """
    # Calculate amount held multiplier
    amount_multiplier = 1.0
    for threshold, multiplier in AMOUNT_HELD_TIERS:
        if holding_percentage >= threshold:
            amount_multiplier = multiplier
    
    # Calculate time held multiplier
    time_multiplier = 1.0
    for threshold, multiplier in TIME_HELD_TIERS:
        if holding_duration_days >= threshold:
            time_multiplier = multiplier
    
    # Combine multipliers: We take the average and then apply a bonus
    # This ensures a balanced approach between amount and time
    # The formula is designed so that max values (1.3 for both) result in MAX_MULTIPLIER (1.5)
    combined_multiplier = (amount_multiplier + time_multiplier) / 2
    
    # Scale to ensure max combined value is MAX_MULTIPLIER
    if combined_multiplier > 1.0:
        # Scale the bonus part (everything above 1.0)
        bonus = combined_multiplier - 1.0
        scaled_bonus = bonus * (MAX_MULTIPLIER - 1.0) / 0.3  # 0.3 is max possible bonus from avg
        combined_multiplier = 1.0 + scaled_bonus
    
    # Ensure we don't exceed MAX_MULTIPLIER
    combined_multiplier = min(combined_multiplier, MAX_MULTIPLIER)
    
    logger.debug(
        f"Multiplier calculation: holding_pct={holding_percentage:.2f}, "
        f"duration={holding_duration_days} days, "
        f"amount_mult={amount_multiplier:.2f}, time_mult={time_multiplier:.2f}, "
        f"final={combined_multiplier:.2f}"
    )
    
    return combined_multiplier


def get_tier_thresholds() -> Dict[str, Tuple]:
    """
    Get the current tier thresholds for documentation and display purposes.
    
    Returns:
        Dictionary containing the current tier thresholds and their multipliers
    """
    return {
        "amount_held_tiers": AMOUNT_HELD_TIERS,
        "time_held_tiers": TIME_HELD_TIERS,
        "max_multiplier": MAX_MULTIPLIER
    }


def update_tier_thresholds(
    amount_held_tiers=None,
    time_held_tiers=None,
    max_multiplier=None
):
    """
    Update the tier thresholds (admin function).
    
    Args:
        amount_held_tiers: New thresholds for amount held
        time_held_tiers: New thresholds for time held
        max_multiplier: New maximum multiplier
    """
    global AMOUNT_HELD_TIERS, TIME_HELD_TIERS, MAX_MULTIPLIER
    
    if amount_held_tiers is not None:
        AMOUNT_HELD_TIERS = amount_held_tiers
        
    if time_held_tiers is not None:
        TIME_HELD_TIERS = time_held_tiers
        
    if max_multiplier is not None:
        MAX_MULTIPLIER = max_multiplier
        
    logger.info("Updated vesting multiplier thresholds") 