# Bettensor Vesting System

This module provides a comprehensive stake tracking and vesting system for Bittensor validators. It monitors stake balances, detects stake-related transactions, calculates rewards, and manages vesting schedules for validator rewards.

## Overview

The vesting system consists of the following components:

1. **Stake Tracking**: Monitors and records changes in stake balances for validators and miners
2. **Transaction Detection**: Detects and classifies stake-related transactions (add, remove, move)
3. **Reward Calculation**: Distinguishes between manual stake adjustments and earned rewards
4. **Vesting Schedules**: Creates and manages vesting schedules for earned rewards
5. **Vesting Payments**: Processes scheduled vesting payments to miners

## Components

### SubtensorTransactionTracker

The `SubtensorTransactionTracker` class connects to the Bittensor blockchain using the Subtensor API to track stake transactions:

- Monitors stake changes for neurons in a subnet
- Detects add_stake, remove_stake, and move_stake events
- Tracks epoch boundaries for synchronizing with the network

### StakeTrackerService

The `StakeTrackerService` provides high-level stake tracking functionality:

- Tracks stake balance changes for each miner over time
- Records stake transactions and maintains a history
- Distinguishes between manual stake changes and earned rewards
- Creates vesting schedules for earned rewards
- Processes vesting payments according to configured schedules

## Database Models

The system uses the following database models to store data:

1. `MinerStakeHistory`: Records stake history and changes over time
2. `StakeTransaction`: Tracks individual stake transactions on the blockchain
3. `HotkeyColdkeyAssociation`: Maintains associations between hotkeys and coldkeys
4. `VestingSchedule`: Defines vesting schedules for earned rewards
5. `VestingPayment`: Tracks individual payments from vesting schedules

## Usage Example

Here's a simple example of how to use the stake tracking service:

```python
import asyncio
import bittensor as bt
from bettensor.validator.utils.database import DatabaseManager
from bettensor.validator.utils.vesting.stake_tracker import StakeTrackerService

async def main():
    # Initialize database manager
    db_manager = DatabaseManager("vesting.db")
    await db_manager.connect()
    
    # Load metagraph
    metagraph = bt.metagraph(netuid=1)  # Replace with your subnet ID
    
    # Create stake tracker service
    service = StakeTrackerService(
        db_manager=db_manager,
        metagraph=metagraph,
        subnet_id=1,  # Replace with your subnet ID
        vesting_duration_days=30,  # Configure vesting duration
        vesting_interval_days=1,   # Configure payment interval
        min_vesting_amount=0.1,    # Minimum reward amount for vesting
        network="finney"           # Bittensor network
    )
    
    # Start the service
    await service.start()
    
    try:
        # Run for some time
        await asyncio.sleep(3600)  # Run for an hour
    finally:
        # Stop the service
        await service.stop()
        
        # Close database connection
        await db_manager.disconnect()

if __name__ == "__main__":
    asyncio.run(main())
```

## Advanced Features

### Epoch Boundary Detection

The system detects epoch boundaries to synchronize with the Bittensor network:

- Adjusts sleep intervals based on proximity to epoch boundaries
- Processes data more frequently near epoch boundaries
- Recalculates rewards at epoch boundaries

### Transaction Classification

The system classifies stake transactions into several types:

- **AddStake**: Adding stake to a hotkey
- **RemoveStake**: Removing stake from a hotkey
- **MoveStake**: Moving stake between hotkeys (detected as related add/remove pairs)

### Stake vs. Reward Attribution

The system distinguishes between manual stake adjustments and earned rewards:

- Manual adjustments are detected via blockchain transactions
- Earned rewards are calculated by subtracting manual adjustments from total stake changes
- Only actual rewards are eligible for vesting

## Configuration

The vesting system can be configured with several parameters:

- **vesting_duration_days**: Duration for vesting schedules (default: 30 days)
- **vesting_interval_days**: Interval between vesting payments (default: 1 day)
- **min_vesting_amount**: Minimum reward amount for creating a vesting schedule (default: 0.1 TAO)
- **network**: Bittensor network to connect to (default: "finney")

## Integration

To integrate the vesting system into your validator:

1. Initialize the `StakeTrackerService` with your subnet settings
2. Start the service at validator startup
3. Stop the service gracefully at validator shutdown
4. Use the database models to query stake history, transactions, and vesting schedules
5. Implement reward distribution logic based on vesting schedules 