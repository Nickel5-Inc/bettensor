#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Test script for the vesting rewards system.
"""

import argparse
import asyncio
import logging
import os
import sys
from datetime import datetime, timezone
import numpy as np
import bittensor as bt

from bettensor.validator.utils.database import DatabaseManager
from bettensor.validator.utils.scoring.scoring import ScoringSystem
from bettensor.validator.utils.vesting.tracker import VestingTracker
from bettensor.validator.utils.vesting.multipliers import calculate_multiplier, get_tier_thresholds
from bettensor.validator.utils.vesting.integration import VestingSystemIntegration

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)

logger = logging.getLogger(__name__)


class MockMetagraph:
    """Mock metagraph for testing without real blockchain access."""
    
    def __init__(self, num_miners=256):
        self.n = num_miners
        self.hotkeys = [f"hotkey_{i}" for i in range(num_miners)]
        self.coldkeys = [f"coldkey_{i//4}" for i in range(num_miners)]  # 4 hotkeys per coldkey
        self.stake = np.random.rand(num_miners) * 100000  # Random stake amounts


async def test_multiplier_calculation():
    """Test the multiplier calculation."""
    logger.info("Testing multiplier calculation...")
    
    tiers = get_tier_thresholds()
    logger.info(f"Multiplier tiers: {tiers}")
    
    test_cases = [
        (0.0, 0, 1.0),
        (0.2, 0, 1.0),
        (0.3, 0, 1.1),
        (0.5, 0, 1.2),
        (0.7, 0, 1.3),
        (0.0, 14, 1.1),
        (0.0, 30, 1.2),
        (0.0, 60, 1.3),
        (0.3, 14, 1.1),
        (0.5, 30, 1.2),
        (0.7, 60, 1.4),
        (0.8, 70, 1.5),
    ]
    
    for holding_pct, holding_days, expected in test_cases:
        multiplier = calculate_multiplier(holding_pct, holding_days)
        logger.info(f"Holding {holding_pct*100:3.0f}% for {holding_days:3d} days: multiplier = {multiplier:.2f}")
        assert abs(multiplier - expected) < 0.1, f"Expected ~{expected}, got {multiplier}"
    
    logger.info("Multiplier calculation tests passed!")


async def test_vesting_tracker(db_path=":memory:"):
    """Test the vesting tracker."""
    logger.info("Testing vesting tracker...")
    
    # Create in-memory database for testing
    db_manager = DatabaseManager(db_path)
    await db_manager.initialize()
    
    # Create mock metagraph
    metagraph = MockMetagraph(num_miners=10)
    
    # Create vesting tracker
    tracker = VestingTracker(db_manager, metagraph, subnet_id=1)
    
    # Test key association tracking
    await tracker.update_key_associations()
    logger.info(f"Tracked {len(tracker._hotkey_to_coldkey)} hotkey-coldkey associations")
    
    # Test stake history tracking
    await tracker.update_stake_history()
    logger.info(f"Tracked stake history for {len(tracker._stake_history)} coldkeys")
    
    # Test emission recording
    for i in range(5):
        hotkey = metagraph.hotkeys[i]
        await tracker.record_emissions(hotkey, 100.0 * (i + 1))
    
    # Test holding metric calculation
    for i in range(5):
        hotkey = metagraph.hotkeys[i]
        holding_pct, holding_days = await tracker.calculate_holding_metrics(hotkey)
        logger.info(f"Hotkey {hotkey}: holding {holding_pct*100:.1f}%, {holding_days} days")
    
    logger.info("Vesting tracker tests passed!")


async def test_integration(db_path=":memory:"):
    """Test the integration with scoring system."""
    logger.info("Testing vesting system integration...")
    
    # Create in-memory database for testing
    db_manager = DatabaseManager(db_path)
    await db_manager.initialize()
    
    # Create mock metagraph
    metagraph = MockMetagraph(num_miners=256)
    
    # Create scoring system
    scoring_system = ScoringSystem(
        db_manager=db_manager,
        num_miners=256,
        max_days=45,
        reference_date=datetime.now(timezone.utc)
    )
    
    # Create vesting tracker
    tracker = VestingTracker(db_manager, metagraph, subnet_id=1)
    
    # Create and install integration
    integration = VestingSystemIntegration(scoring_system, tracker)
    integration.install()
    
    # Simulate some weights
    weights = np.zeros(256)
    weights[10:50] = np.random.rand(40) / 40  # Random weights for 40 miners
    
    # Simulate original calculate_weights
    original_calculate_weights = scoring_system.calculate_weights
    scoring_system.calculate_weights = lambda *args: weights
    
    # Call patched method
    modified_weights = await integration._patched_calculate_weights()
    
    # Restore original method
    scoring_system.calculate_weights = original_calculate_weights
    
    # Check results
    logger.info(f"Original weights sum: {weights.sum():.6f}")
    logger.info(f"Modified weights sum: {modified_weights.sum():.6f}")
    logger.info(f"Number of miners with modified weights: {np.sum(modified_weights > 0)}")
    
    # Verify that sum of weights is preserved
    assert abs(weights.sum() - modified_weights.sum()) < 1e-6
    
    logger.info("Vesting system integration tests passed!")


async def main():
    """Main test function."""
    parser = argparse.ArgumentParser(description="Test the vesting rewards system")
    parser.add_argument(
        "--db-path", 
        type=str, 
        default=":memory:",
        help="Path to SQLite database file (default: in-memory)"
    )
    parser.add_argument(
        "--test", 
        type=str, 
        choices=["all", "multiplier", "tracker", "integration"],
        default="all",
        help="Which test to run (default: all)"
    )
    
    args = parser.parse_args()
    
    try:
        if args.test in ["all", "multiplier"]:
            await test_multiplier_calculation()
        
        if args.test in ["all", "tracker"]:
            await test_vesting_tracker(args.db_path)
        
        if args.test in ["all", "integration"]:
            await test_integration(args.db_path)
            
        logger.info("All tests completed successfully!")
        
    except Exception as e:
        logger.error(f"Error during testing: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main()) 