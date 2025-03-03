#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Utility script to enable vesting rewards in a validator node.
This script adds the vesting rewards multiplier to the scoring system.
"""

import argparse
import asyncio
import logging
import os
import sys
from datetime import datetime, timezone
import bittensor as bt

from bettensor.validator.utils.database import DatabaseManager
from bettensor.validator.utils.vesting import VestingTracker, VestingSystemIntegration


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)

logger = logging.getLogger(__name__)


async def enable_vesting_rewards(subnet_id, db_path, scoring_system=None):
    """
    Enable vesting rewards in the validator.
    
    Args:
        subnet_id: The subnet ID to track vesting rewards for
        db_path: Path to the SQLite database file
        scoring_system: Optional scoring system instance (for testing)
    
    Returns:
        The VestingSystemIntegration instance if successful
    """
    logger.info(f"Enabling vesting rewards for subnet {subnet_id}")
    
    # Initialize database connection
    db_manager = DatabaseManager(db_path)
    await db_manager.initialize()
    
    # Get subnet metagraph
    try:
        if not bt.subtensor().is_subnet_registered(netuid=subnet_id):
            logger.error(f"Subnet {subnet_id} is not registered")
            return None
        
        metagraph = bt.metagraph(netuid=subnet_id)
        metagraph.sync(subtensor=bt.subtensor())
        logger.info(f"Synced metagraph for subnet {subnet_id} with {metagraph.n} miners")
    except Exception as e:
        logger.error(f"Failed to sync metagraph: {e}")
        return None
    
    # Create vesting tracker
    tracker = VestingTracker(db_manager, metagraph, subnet_id=subnet_id)
    
    # Initialize tracker data
    logger.info("Initializing vesting tracker data")
    await tracker.update_key_associations()
    await tracker.update_stake_history()
    
    # If no scoring system provided, attempt to find and load it
    if scoring_system is None:
        from bettensor.validator.utils.scoring.scoring import get_active_scoring_system
        
        scoring_system = get_active_scoring_system()
        if scoring_system is None:
            logger.error("No active scoring system found. Please ensure validator is running.")
            return None
    
    # Create and install integration
    integration = VestingSystemIntegration(scoring_system, tracker)
    integration.install()
    
    logger.info("Vesting rewards system successfully enabled")
    return integration


async def disable_vesting_rewards(integration=None):
    """
    Disable vesting rewards in the validator.
    
    Args:
        integration: Optional VestingSystemIntegration instance
    """
    logger.info("Disabling vesting rewards")
    
    # If no integration provided, attempt to find it
    if integration is None:
        from bettensor.validator.utils.scoring.scoring import get_active_scoring_system
        
        scoring_system = get_active_scoring_system()
        if scoring_system is None:
            logger.error("No active scoring system found. Cannot disable vesting rewards.")
            return False
        
        # Check if vesting is already installed by inspecting scoring system methods
        original_method = getattr(scoring_system, "_original_calculate_weights", None)
        if original_method is None:
            logger.info("Vesting rewards system not found (not enabled)")
            return True
        
        # Create dummy integration object for uninstallation
        integration = VestingSystemIntegration(scoring_system, None)
    
    # Uninstall integration
    integration.uninstall()
    logger.info("Vesting rewards system successfully disabled")
    return True


async def main():
    """Main function."""
    parser = argparse.ArgumentParser(description="Enable or disable vesting rewards in a validator node")
    parser.add_argument(
        "--subnet-id", 
        type=int, 
        default=1,
        help="Subnet ID to track vesting rewards for (default: 1)"
    )
    parser.add_argument(
        "--db-path", 
        type=str, 
        default="~/.bettensor/database.db",
        help="Path to SQLite database file (default: ~/.bettensor/database.db)"
    )
    parser.add_argument(
        "--disable", 
        action="store_true",
        help="Disable vesting rewards instead of enabling"
    )
    
    args = parser.parse_args()
    
    # Expand user path if necessary
    db_path = os.path.expanduser(args.db_path)
    
    try:
        if args.disable:
            success = await disable_vesting_rewards()
            if not success:
                sys.exit(1)
        else:
            integration = await enable_vesting_rewards(args.subnet_id, db_path)
            if integration is None:
                sys.exit(1)
            
        logger.info("Operation completed successfully")
        
    except Exception as e:
        logger.error(f"Error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main()) 