#!/usr/bin/env python3
# The MIT License (MIT)
# Copyright © 2024 Bettensor Contributors

"""Example of using the Vesting API Interface to push data to a web service."""

import os
import sys
import asyncio
import argparse
from datetime import datetime, timezone

# Add the parent directory to the path so we can import bettensor
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import bittensor as bt
from bettensor.validator.utils.database.database_manager import DatabaseManager
from bettensor.validator.utils.vesting.system import VestingSystem
from bettensor.validator.utils.vesting.api_interface import VestingAPIInterface

async def main(config):
    """Main function demonstrating the use of the VestingAPIInterface."""
    try:
        # Initialize logger
        bt.logging(config=config, logging_dir=config.logging.logging_dir)
        bt.logging.info("Starting Vesting API Example")
        
        # Create subtensor connection
        subtensor = bt.subtensor(config=config)
        bt.logging.info(f"Connected to subtensor: {subtensor.network}")
        
        # Create database manager
        db_path = config.database.db_path
        bt.logging.info(f"Using database at {db_path}")
        db_manager = DatabaseManager(db_path)
        await db_manager.initialize()
        
        # Initialize vesting system
        bt.logging.info(f"Initializing vesting system for subnet {config.netuid}")
        vesting_system = VestingSystem(
            subtensor=subtensor,
            subnet_id=config.netuid,
            db_manager=db_manager,
            minimum_stake=config.vesting.minimum_stake,
            retention_window_days=config.vesting.retention_window,
            retention_target=config.vesting.retention_target,
            max_multiplier=config.vesting.max_multiplier,
            detailed_transaction_tracking=True
        )
        
        # Initialize vesting system
        bt.logging.info("Initializing vesting system")
        await vesting_system.initialize(auto_init_metagraph=True)
        
        # Create API interface
        bt.logging.info(f"Creating API interface with endpoint {config.api.url}")
        api_interface = VestingAPIInterface(
            vesting_system=vesting_system,
            api_base_url=config.api.url,
            api_key=config.api.key,
            poll_interval_seconds=config.api.poll_interval,
            push_enabled=config.api.push_enabled,
            export_detailed_transactions=True
        )
        
        # Initialize API interface
        bt.logging.info("Initializing API interface")
        success = await api_interface.initialize()
        if not success:
            bt.logging.error("Failed to initialize API interface, exiting")
            return
        
        # Start background polling if enabled
        if config.api.push_enabled:
            bt.logging.info("Starting background polling")
            await api_interface.start()
        
        # Push data based on specified action
        if config.action == "all":
            bt.logging.info("Pushing all data to API")
            await api_interface.push_all_data()
        elif config.action == "overview":
            bt.logging.info("Pushing system overview to API")
            await api_interface.push_system_overview()
        elif config.action == "metrics":
            bt.logging.info("Pushing stake metrics to API")
            await api_interface.push_all_stake_metrics()
        elif config.action == "coldkeys":
            bt.logging.info("Pushing coldkey metrics to API")
            await api_interface.push_all_coldkey_metrics()
        elif config.action == "transactions":
            bt.logging.info("Pushing recent transactions to API")
            await api_interface.push_recent_transactions(days=config.api.history_days)
        elif config.action == "multipliers":
            bt.logging.info("Pushing vesting multipliers to API")
            await api_interface.push_vesting_multipliers()
        elif config.action == "tranches":
            bt.logging.info("Pushing tranche data to API")
            await api_interface.push_tranche_data()
        elif config.action == "miner":
            bt.logging.info(f"Requesting details for miner {config.miner_hotkey}")
            details = await api_interface.request_miner_details(config.miner_hotkey)
            if details:
                bt.logging.info(f"Retrieved details for miner {config.miner_hotkey}")
                # Print selected details
                bt.logging.info(f"Total stake: {details['metrics']['total_stake']} τ")
                bt.logging.info(f"Manual stake: {details['metrics']['manual_stake']} τ")
                bt.logging.info(f"Earned stake: {details['metrics']['earned_stake']} τ")
                bt.logging.info(f"Holding percentage: {details['metrics']['holding_percentage']:.2%}")
                bt.logging.info(f"Holding duration: {details['metrics']['holding_duration_days']} days")
                bt.logging.info(f"Vesting multiplier: {details['metrics']['vesting_multiplier']:.4f}")
                bt.logging.info(f"Recent transactions: {len(details['transactions'])}")
                bt.logging.info(f"Stake changes: {len(details['stake_changes'])}")
                bt.logging.info(f"Tranches: {len(details['tranches'])}")
            else:
                bt.logging.error(f"Failed to get details for miner {config.miner_hotkey}")
        elif config.action == "daemon":
            # Run as a background daemon
            bt.logging.info("Running as a background daemon")
            
            if not config.api.push_enabled:
                bt.logging.warning("Push updates not enabled, daemon mode will not push data")
                await api_interface.push_all_data()  # Do a single push
            
            # Keep running until interrupted
            try:
                while True:
                    bt.logging.info(f"Daemon running, next update in {config.api.poll_interval} seconds")
                    await asyncio.sleep(config.api.poll_interval)
            except KeyboardInterrupt:
                bt.logging.info("Daemon interrupted")
        
        # Stop the API interface
        bt.logging.info("Stopping API interface")
        await api_interface.stop()
        
        # Cleanup
        bt.logging.info("Shutting down vesting system")
        await vesting_system.shutdown()
        
        # Close database connection
        bt.logging.info("Closing database connection")
        await db_manager.close()
        
        bt.logging.info("Vesting API Example completed successfully")
        
    except Exception as e:
        bt.logging.error(f"Error in main function: {e}")
        raise

def get_config():
    """Parse command line arguments and return config."""
    parser = argparse.ArgumentParser(description="Vesting API Example")
    
    # Network parameters
    parser.add_argument("--netuid", type=int, default=30, help="Subnet UID to use")
    parser.add_argument("--subtensor.network", default="finney", help="Subtensor network")
    parser.add_argument("--subtensor.chain_endpoint", default=None, help="Subtensor chain endpoint")
    
    # Database parameters
    parser.add_argument("--database.db_path", type=str, default="./validator.db", help="Path to SQLite database")
    
    # Vesting parameters
    parser.add_argument("--vesting.minimum_stake", type=float, default=0.3, help="Minimum stake required for multiplier")
    parser.add_argument("--vesting.retention_window", type=int, default=30, help="Retention window in days")
    parser.add_argument("--vesting.retention_target", type=float, default=0.9, help="Target retention percentage")
    parser.add_argument("--vesting.max_multiplier", type=float, default=1.5, help="Maximum multiplier")
    
    # API parameters
    parser.add_argument("--api.url", type=str, required=True, help="API base URL")
    parser.add_argument("--api.key", type=str, default=None, help="API key for authentication")
    parser.add_argument("--api.poll_interval", type=int, default=300, help="Polling interval in seconds")
    parser.add_argument("--api.push_enabled", action="store_true", help="Enable push updates")
    parser.add_argument("--api.history_days", type=int, default=7, help="Days of history to push")
    parser.add_argument("--api.batch_size", type=int, default=1000, help="Batch size for data uploads")
    
    # Action to perform
    parser.add_argument(
        "--action", 
        type=str, 
        default="all",
        choices=["all", "overview", "metrics", "coldkeys", "transactions", "multipliers", "tranches", "miner", "daemon"],
        help="Action to perform"
    )
    
    # Miner hotkey for miner details
    parser.add_argument("--miner_hotkey", type=str, default=None, help="Miner hotkey for details")
    
    # Logging
    parser.add_argument("--logging.debug", action="store_true", help="Enable debug logging")
    parser.add_argument("--logging.trace", action="store_true", help="Enable trace logging")
    parser.add_argument("--logging.logging_dir", type=str, default="logs", help="Logging directory")
    
    # Parse and convert to Bittensor config
    config = bt.config(parser)
    
    # Validate config
    if config.action == "miner" and not config.miner_hotkey:
        parser.error("--miner_hotkey is required for --action=miner")
    
    return config

if __name__ == "__main__":
    # Get config
    config = get_config()
    
    # Run main function
    asyncio.run(main(config)) 