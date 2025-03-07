#!/usr/bin/env python3
"""
Vesting Integration Example

This script demonstrates how to initialize and use the vesting system
with the scoring system in a Bettensor validator.
"""

import asyncio
import argparse
import logging
import sys
import bittensor as bt
from datetime import datetime, timezone, timedelta
from pathlib import Path
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

from bettensor.validator.utils.database.database_manager import DatabaseManager
from bettensor.validator.utils.scoring.scoring import ScoringSystem
from bettensor.validator.utils.vesting import (
    BlockchainMonitor,
    StakeTracker,
    TransactionMonitor,
    VestingSystem,
    VestingIntegration
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)

logger = logging.getLogger(__name__)

async def simulate_stake_changes(stake_tracker, hotkey, coldkey):
    """
    Simulate a series of stake changes to demonstrate tranche-based FILO accounting.
    
    Args:
        stake_tracker: Initialized StakeTracker
        hotkey: Hotkey to use for simulation
        coldkey: Coldkey to use for simulation
    """
    logger.info(f"Simulating stake changes for {hotkey}")
    
    # Current time
    now = datetime.now(timezone.utc)
    
    # Simulate initial stake (manual)
    await stake_tracker.record_stake_change(
        hotkey=hotkey,
        coldkey=coldkey,
        amount=10.0,
        change_type="add_stake",
        flow_type="inflow",
        timestamp=now - timedelta(days=60)
    )
    logger.info(f"Added initial manual stake of 10.0 TAO (60 days ago)")
    
    # Simulate emissions over time
    emission_days = [50, 40, 30, 20, 10]
    emission_amounts = [1.0, 1.5, 2.0, 2.5, 3.0]
    
    for days, amount in zip(emission_days, emission_amounts):
        await stake_tracker.record_stake_change(
            hotkey=hotkey,
            coldkey=coldkey,
            amount=amount,
            change_type="emission",
            flow_type="emission",
            timestamp=now - timedelta(days=days)
        )
        logger.info(f"Added emission of {amount} TAO ({days} days ago)")
    
    # Simulate a partial withdrawal (should consume newest tranches first with FILO)
    await stake_tracker.record_stake_change(
        hotkey=hotkey,
        coldkey=coldkey,
        amount=-5.0,
        change_type="remove_stake",
        flow_type="outflow",
        timestamp=now - timedelta(days=25)
    )
    logger.info(f"Removed 5.0 TAO (25 days ago) - With FILO, this should consume from newest tranches")
    
    # Simulate another manual stake addition
    await stake_tracker.record_stake_change(
        hotkey=hotkey,
        coldkey=coldkey,
        amount=5.0,
        change_type="add_stake",
        flow_type="inflow",
        timestamp=now - timedelta(days=15)
    )
    logger.info(f"Added manual stake of 5.0 TAO (15 days ago)")
    
    # Simulate another withdrawal
    await stake_tracker.record_stake_change(
        hotkey=hotkey,
        coldkey=coldkey,
        amount=-3.0,
        change_type="remove_stake",
        flow_type="outflow",
        timestamp=now - timedelta(days=5)
    )
    logger.info(f"Removed 3.0 TAO (5 days ago) - With FILO, this should consume from newest tranches")
    
    # Final emission
    await stake_tracker.record_stake_change(
        hotkey=hotkey,
        coldkey=coldkey,
        amount=1.0,
        change_type="emission",
        flow_type="emission",
        timestamp=now - timedelta(days=1)
    )
    logger.info(f"Added emission of 1.0 TAO (1 day ago)")

async def visualize_tranches(stake_tracker, hotkey):
    """
    Visualize the tranches for a hotkey, highlighting the benefits of FILO accounting.
    
    Args:
        stake_tracker: Initialized StakeTracker
        hotkey: Hotkey to visualize tranches for
    """
    # Get tranche details
    tranches = await stake_tracker.get_tranche_details(hotkey, include_exits=True)
    
    if not tranches:
        logger.warning(f"No tranches found for {hotkey}")
        return
    
    # Convert to pandas DataFrame for easier manipulation
    df = pd.DataFrame(tranches)
    
    # Calculate age in days
    now = datetime.now(timezone.utc)
    df['age_days'] = df['entry_timestamp'].apply(lambda ts: (now - ts).days)
    
    # Sort by age (oldest first)
    df = df.sort_values('age_days', ascending=False)
    
    # Create a figure with two subplots
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 10))
    
    # Plot 1: Tranche composition
    emission_mask = df['is_emission'] == 1
    
    # Plot emission tranches
    ax1.bar(df.loc[emission_mask, 'age_days'], df.loc[emission_mask, 'remaining_amount'], 
            color='green', alpha=0.7, label='Emission Stake')
    
    # Plot manual tranches
    ax1.bar(df.loc[~emission_mask, 'age_days'], df.loc[~emission_mask, 'remaining_amount'], 
            color='blue', alpha=0.7, label='Manual Stake')
    
    # Add consumed portions (exits)
    for idx, row in df.iterrows():
        if 'exits' in row and row['exits']:
            for exit in row['exits']:
                exit_age = (now - exit['exit_timestamp']).days
                ax1.bar(row['age_days'], exit['exit_amount'], 
                        color='red', alpha=0.3, bottom=row['remaining_amount'])
    
    ax1.set_xlabel('Age (days)')
    ax1.set_ylabel('Amount (TAO)')
    ax1.set_title(f'Stake Tranches for {hotkey} (FILO Accounting)')
    ax1.legend()
    ax1.grid(True, alpha=0.3)
    
    # Plot 2: Percentage remaining by tranche
    ax2.bar(df['age_days'], df['percent_remaining'] * 100, 
            color=df['is_emission'].map({1: 'green', 0: 'blue'}), alpha=0.7)
    
    ax2.set_xlabel('Age (days)')
    ax2.set_ylabel('Percentage Remaining (%)')
    ax2.set_title('Percentage of Original Tranche Remaining (Notice older tranches preserved with FILO)')
    ax2.grid(True, alpha=0.3)
    
    plt.tight_layout()
    plt.savefig('tranche_visualization.png')
    logger.info("Saved tranche visualization to tranche_visualization.png")

async def demonstrate_metagraph_sync(vesting_system):
    """
    Demonstrate initializing from the metagraph and handling deregistrations.
    
    Args:
        vesting_system: Initialized VestingSystem
    """
    logger.info("Demonstrating metagraph synchronization features")
    
    # Get current metagraph state
    metagraph = vesting_system.subtensor.metagraph(vesting_system.subnet_id)
    metagraph.sync(subtensor=vesting_system.subtensor)
    
    # Count miners in metagraph
    num_miners = len(metagraph.hotkeys)
    logger.info(f"Metagraph has {num_miners} miners")
    
    # Initialize from metagraph
    logger.info("Initializing from metagraph (force update=True)")
    await vesting_system.initialize_from_metagraph(metagraph=metagraph, force_update=True)
    
    # Get current stake metrics
    results = await vesting_system.db_manager.fetch_all("""
        SELECT COUNT(*) as count FROM stake_metrics
    """)
    num_entries = results[0]['count'] if results else 0
    logger.info(f"After initialization: {num_entries} stake metric entries")
    
    # Simulate deregistration by selecting a few random hotkeys
    import random
    if num_miners > 0:
        num_to_deregister = min(3, num_miners)  # Deregister up to 3 miners
        hotkeys_to_deregister = random.sample(metagraph.hotkeys, num_to_deregister)
        
        logger.info(f"Simulating deregistration for {num_to_deregister} miners")
        for hotkey in hotkeys_to_deregister:
            logger.info(f"Deregistering {hotkey}")
        
        # Handle deregistrations
        num_cleaned = await vesting_system.handle_deregistered_keys(deregistered_hotkeys=hotkeys_to_deregister)
        logger.info(f"Cleaned up data for {num_cleaned} deregistered miners")
        
        # Check stake metrics after deregistration
        results = await vesting_system.db_manager.fetch_all("""
            SELECT COUNT(*) as count FROM stake_metrics
        """)
        num_entries_after = results[0]['count'] if results else 0
        logger.info(f"After deregistration: {num_entries_after} stake metric entries")
    else:
        logger.warning("No miners in metagraph to simulate deregistration")

async def main():
    # Parse arguments
    parser = argparse.ArgumentParser(description="Vesting integration example")
    parser.add_argument("--netuid", type=int, default=19, help="Subnet UID to monitor")
    parser.add_argument("--db-path", type=str, default="./validator.db", help="Path to SQLite database")
    parser.add_argument("--min-stake", type=float, default=0.3, help="Minimum stake required for vesting bonus")
    parser.add_argument("--retention-days", type=int, default=30, help="Days for retention window")
    parser.add_argument("--max-multiplier", type=float, default=1.5, help="Maximum vesting multiplier")
    parser.add_argument("--simulate", action="store_true", help="Simulate stake changes")
    parser.add_argument("--demo-metagraph", action="store_true", help="Demonstrate metagraph syncing")
    
    # Add bittensor arguments
    bt.subtensor.add_args(parser)
    bt.logging.add_args(parser)
    bt.wallet.add_args(parser)
    
    # Parse args
    args = parser.parse_args()
    config = bt.config(parser)
    
    # Create DB directory if it doesn't exist
    db_dir = Path(args.db_path).parent
    db_dir.mkdir(parents=True, exist_ok=True)
    
    # Initialize bittensor objects
    logger.info(f"Initializing subtensor connection to {config.subtensor.network}")
    subtensor = bt.subtensor(config=config)
    wallet = bt.wallet(config=config)
    
    # Initialize database manager
    logger.info(f"Initializing database at {args.db_path}")
    db_manager = DatabaseManager(args.db_path)
    await db_manager.initialize()
    
    # Initialize vesting system
    logger.info("Initializing vesting system")
    vesting_system = VestingSystem(
        subtensor=subtensor,
        subnet_id=args.netuid,
        db_manager=db_manager,
        minimum_stake=args.min_stake,
        retention_window_days=args.retention_days,
        retention_target=0.9,
        max_multiplier=args.max_multiplier,
        use_background_thread=True,
        detailed_transaction_tracking=True
    )
    await vesting_system.initialize(auto_init_metagraph=False)  # We'll manually control initialization
    
    # Initialize stake tracker
    stake_tracker = vesting_system.stake_tracker
    
    # Simulate stake changes if requested
    if args.simulate:
        test_hotkey = "5FHneW46xGXgs5mUiveU4sbTyGBzmstUspZC92UhjJM694ty"
        test_coldkey = "5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY"
        
        await simulate_stake_changes(stake_tracker, test_hotkey, test_coldkey)
        
        # Visualize tranches
        await visualize_tranches(stake_tracker, test_hotkey)
        
        # Get holding metrics
        holding_percentage, holding_duration = await stake_tracker.calculate_holding_metrics(
            hotkey=test_hotkey,
            window_days=args.retention_days
        )
        
        logger.info(f"Holding metrics for {test_hotkey}:")
        logger.info(f"  Holding percentage: {holding_percentage:.2%}")
        logger.info(f"  Holding duration: {holding_duration} days")
        
        # Get detailed tranche information
        tranche_details = await stake_tracker.get_tranche_details(
            hotkey=test_hotkey,
            include_exits=True
        )
        
        logger.info(f"Tranche details for {test_hotkey}:")
        for i, tranche in enumerate(tranche_details):
            logger.info(f"  Tranche {i+1}:")
            logger.info(f"    Type: {tranche['type']}")
            logger.info(f"    Initial amount: {tranche['initial_amount']:.6f} TAO")
            logger.info(f"    Remaining amount: {tranche['remaining_amount']:.6f} TAO")
            logger.info(f"    Age: {tranche['holding_days']} days")
            logger.info(f"    Percent remaining: {tranche['percent_remaining']:.2%}")
            
            if 'exits' in tranche and tranche['exits']:
                logger.info(f"    Exit history:")
                for j, exit in enumerate(tranche['exits']):
                    exit_age = (datetime.now(timezone.utc) - exit['exit_timestamp']).days
                    logger.info(f"      Exit {j+1}: {exit['exit_amount']:.6f} TAO ({exit_age} days ago)")
    
    # Demonstrate metagraph syncing if requested
    if args.demo_metagraph:
        await demonstrate_metagraph_sync(vesting_system)
    
    # Initialize scoring system
    logger.info("Initializing scoring system")
    scoring_system = ScoringSystem(
        db_manager=db_manager,
        num_miners=256,
        max_days=45,
        reference_date=datetime(year=2024, month=9, day=30, tzinfo=timezone.utc)
    )
    await scoring_system.initialize()
    
    # Initialize vesting integration
    logger.info("Initializing vesting integration")
    vesting_integration = VestingIntegration(
        scoring_system=scoring_system,
        vesting_system=vesting_system
    )
    vesting_integration.install()
    logger.info("Vesting integration installed successfully")
    
    # Get vesting stats
    logger.info("Getting vesting system statistics")
    stats = await vesting_system.get_vesting_stats()
    logger.info(f"Vesting stats: {stats}")
    
    # Cleanup and shutdown
    logger.info("Cleaning up")
    await vesting_system.shutdown()
    await db_manager.close()
    
    logger.info("Example completed successfully")

if __name__ == "__main__":
    asyncio.run(main()) 