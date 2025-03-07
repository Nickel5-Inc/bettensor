#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Script to configure and control the vesting system integration.

This script allows enabling or disabling the vesting system and
configuring its redistribution approach.
"""

import os
import sys
import argparse
import asyncio
import logging
from pathlib import Path
import json

# Add bettensor to Python path
ROOT_DIR = str(Path(__file__).parent.parent.absolute())
sys.path.insert(0, ROOT_DIR)

import bittensor as bt
from bettensor.validator.utils.vesting.integration import VestingSystemIntegration
from bettensor.validator.utils.vesting.system import VestingSystem
from bettensor.validator.utils.scoring.scoring import get_active_scoring_system

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


async def enable_vesting(
    use_enhanced_redistribution: bool = False,
    threshold_factor: float = 0.5,
    absolute_threshold: float = 0.01,
    subnet_id: int = 1
):
    """
    Enable the vesting system integration.
    
    Args:
        use_enhanced_redistribution: Whether to use enhanced redistribution
            that zeroes out miners below threshold.
        threshold_factor: Relative threshold as a factor of original weight (0.0-1.0).
        absolute_threshold: Minimum absolute weight to maintain.
        subnet_id: The subnet ID to enable vesting for.
    """
    try:
        config_path = Path(CONFIG_PATH)
        config_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Check if config exists
        if config_path.exists():
            with open(config_path, 'r') as f:
                config = json.load(f)
        else:
            config = {'enabled': False, 'subnet_id': subnet_id}
        
        # Update config
        config['enabled'] = True
        config['subnet_id'] = subnet_id
        config['use_enhanced_redistribution'] = use_enhanced_redistribution
        config['threshold_factor'] = threshold_factor
        config['absolute_threshold'] = absolute_threshold
        
        # Save config
        with open(config_path, 'w') as f:
            json.dump(config, f, indent=2)
        
        logging.info(f"Vesting system enabled for subnet {subnet_id}")
        if use_enhanced_redistribution:
            logging.info(f"Using enhanced redistribution with:")
            logging.info(f"  - Threshold factor: {threshold_factor}")
            logging.info(f"  - Absolute threshold: {absolute_threshold}")
            logging.info(f"Miners whose adjusted weight falls below these thresholds will be zeroed out")
        else:
            logging.info(f"Using standard redistribution (implicit through renormalization)")
        
        return True
    except Exception as e:
        logging.error(f"Failed to enable vesting system: {e}")
        return False


async def disable_vesting():
    """
    Disable the vesting system.
    
    Returns:
        True if successful, False otherwise
    """
    logger.info("Disabling vesting system...")
    
    # Get active scoring system
    scoring_system = get_active_scoring_system()
    if scoring_system is None:
        logger.error("No active scoring system found.")
        return False
    
    # Check if vesting is installed
    if not hasattr(scoring_system, "_original_calculate_weights"):
        logger.info("Vesting system is not enabled.")
        return True
    
    try:
        # Get the integration object if it exists
        integration = getattr(scoring_system, "_vesting_integration", None)
        
        if integration:
            # Uninstall properly
            success = integration.uninstall()
            
            # Stop vesting system
            if hasattr(integration, "vesting_system") and hasattr(integration.vesting_system, "stop"):
                await integration.vesting_system.stop()
                
            # Remove reference
            delattr(scoring_system, "_vesting_integration")
            
            if success:
                logger.info("Vesting system disabled successfully.")
                return True
            else:
                logger.error("Failed to uninstall vesting integration.")
                return False
        else:
            # Fallback: manually restore original method
            scoring_system.calculate_weights = scoring_system._original_calculate_weights
            delattr(scoring_system, "_original_calculate_weights")
            logger.info("Vesting system disabled (using fallback method).")
            return True
            
    except Exception as e:
        logger.error(f"Error disabling vesting system: {e}")
        return False


async def show_status():
    """
    Show the current status of the vesting system.
    
    Returns:
        True if successful, False otherwise
    """
    logger.info("Checking vesting system status...")
    
    # Get active scoring system
    scoring_system = get_active_scoring_system()
    if scoring_system is None:
        logger.error("No active scoring system found.")
        return False
    
    # Check if vesting is installed
    if not hasattr(scoring_system, "_original_calculate_weights"):
        logger.info("Vesting system is NOT enabled.")
        return True
    
    logger.info("Vesting system is enabled.")
    
    # Get the integration object if it exists
    integration = getattr(scoring_system, "_vesting_integration", None)
    
    if integration:
        logger.info(f"Redistribution type: {'Enhanced' if integration.use_enhanced_redistribution else 'Standard'}")
        
        if integration.use_enhanced_redistribution:
            logger.info(f"Threshold factor: {integration.threshold_factor}")
            logger.info(f"Absolute threshold: {integration.absolute_threshold}")
    else:
        logger.info("Vesting integration object not found, cannot retrieve detailed settings.")
    
    return True


async def main():
    """Main entry point for the script."""
    parser = argparse.ArgumentParser(description="Configure and control the vesting system.")
    
    # Create a subparsers object to handle subcommands
    subparsers = parser.add_subparsers(dest="command", help="Command to execute")
    
    # Enable command
    enable_parser = subparsers.add_parser("enable", help="Enable the vesting system")
    enable_parser.add_argument("--enhanced", action="store_true", 
                             help="Use enhanced redistribution with explicit thresholds")
    enable_parser.add_argument("--threshold-factor", type=float, default=0.5, 
                             help="Relative threshold as factor of original weight (0.0-1.0)")
    enable_parser.add_argument("--absolute-threshold", type=float, default=0.01, 
                             help="Minimum absolute weight to maintain")
    enable_parser.add_argument("--subnet-id", type=int, default=1, 
                             help="Subnet ID to track")
    
    # Disable command
    disable_parser = subparsers.add_parser("disable", help="Disable the vesting system")
    
    # Status command
    status_parser = subparsers.add_parser("status", help="Show vesting system status")
    
    args = parser.parse_args()
    
    if args.command == "enable":
        success = await enable_vesting(
            use_enhanced_redistribution=args.enhanced,
            threshold_factor=args.threshold_factor,
            absolute_threshold=args.absolute_threshold,
            subnet_id=args.subnet_id
        )
    elif args.command == "disable":
        success = await disable_vesting()
    elif args.command == "status":
        success = await show_status()
    else:
        parser.print_help()
        success = False
    
    return 0 if success else 1


if __name__ == "__main__":
    exitcode = asyncio.run(main())
    sys.exit(exitcode) 