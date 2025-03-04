# Bettensor Vesting System

This module provides a comprehensive stake tracking and vesting system for Bittensor validators. It monitors stake balances, detects stake-related transactions, calculates rewards, and manages vesting schedules for validator rewards.

## Overview

The vesting system rewards miners who hold their stake rather than immediately selling it. Miners who maintain or increase their stake over time receive multipliers to their scores, enhancing their rewards.

The vesting system consists of the following components:

1. **Stake Tracking**: Monitors and records changes in stake balances for validators and miners
2. **Transaction Detection**: Detects and classifies stake-related transactions (add, remove, move)
3. **Reward Calculation**: Distinguishes between manual stake adjustments and earned rewards
4. **Vesting Schedules**: Creates and manages vesting schedules for earned rewards
5. **Vesting Payments**: Processes scheduled vesting payments to miners

## Directory Structure

```
bettensor/validator/utils/vesting/
├── __init__.py                 # Public exports
├── system.py                   # Main VestingSystem class
├── core/                       # Core components
│   ├── __init__.py
│   ├── multipliers.py          # Vesting multiplier calculations
│   ├── stake_tracker.py        # Stake tracking and history
│   └── vesting_scheduler.py    # Vesting schedule management
├── blockchain/                 # Blockchain interaction
│   ├── __init__.py
│   └── subtensor_client.py     # Subtensor API client
└── database/                   # Database models
    ├── __init__.py
    └── models.py               # Database model definitions
```

## Components

### VestingSystem

The `VestingSystem` class is the main entry point for using the vesting system. It integrates all components and provides a unified API for:

- Tracking stake balances and history
- Monitoring blockchain transactions
- Creating and managing vesting schedules
- Calculating vesting multipliers
- Integrating with scoring systems

### Core Components

#### StakeTracker

The `StakeTracker` tracks stake balances and calculates holding metrics:

- Monitors stake balances for each neuron
- Records stake history over time
- Maintains hotkey-coldkey associations
- Calculates holding percentages and durations

#### VestingScheduler

The `VestingScheduler` manages vesting schedules and payments:

- Creates vesting schedules for earned rewards
- Processes scheduled vesting payments
- Tracks vesting status and progress

#### Multipliers

The multiplier functions calculate score multipliers based on:

- Holding percentage (what percentage of rewards are held)
- Holding duration (how long rewards have been held)

### Blockchain Components

#### SubtensorClient

The `SubtensorClient` provides a unified interface for blockchain interactions:

- Connects to the Bittensor blockchain
- Monitors stake changes and transactions
- Detects epoch boundaries
- Queries transaction history

### Database Models

The system uses these database models to store data:

1. `HotkeyColdkeyAssociation`: Maintains associations between hotkeys and coldkeys
2. `StakeHistory`: Records stake history and changes over time
3. `StakeTransaction`: Tracks individual stake transactions on the blockchain
4. `VestingSchedule`: Defines vesting schedules for earned rewards
5. `VestingPayment`: Tracks individual payments from vesting schedules

## Usage Example

Here's a simple example of how to use the vesting system:

```python
import asyncio
import bittensor as bt
from bettensor.validator.utils.database import DatabaseManager
from bettensor.validator.utils.vesting import VestingSystem

async def main():
    # Initialize components
    db_manager = DatabaseManager("sqlite:///validator.db")
    metagraph = bt.metagraph(netuid=1)
    subnet_id = 1
    
    # Create vesting system
    vesting_system = VestingSystem(
        db_manager=db_manager,
        metagraph=metagraph,
        subnet_id=subnet_id,
        vesting_duration_days=30,
        vesting_interval_days=1,
        min_vesting_amount=0.1
    )
    
    # Start the system
    await vesting_system.start()
    
    # Integrate with scoring system (optional)
    vesting_system.integrate_with_scoring(your_scoring_system)
    
    # Get holding metrics for a hotkey
    metrics = await vesting_system.get_holding_metrics("example_hotkey")
    print(f"Holding metrics: {metrics}")
    
    # Get full vesting summary
    summary = await vesting_system.get_vesting_summary("example_hotkey")
    print(f"Vesting summary: {summary}")
    
    # Create a vesting schedule
    schedule_id = await vesting_system.create_vesting_schedule(
        hotkey="example_hotkey",
        amount=10.0
    )
    
    # ... your validator code ...
    
    # Stop the system
    await vesting_system.stop()

if __name__ == "__main__":
    asyncio.run(main())
```

## Multiplier Calculation

The vesting system calculates score multipliers based on two factors:

1. **Holding Percentage**: What percentage of rewards are being held
   - >30% held: 1.1x multiplier
   - >50% held: 1.2x multiplier
   - >70% held: 1.3x multiplier

2. **Holding Duration**: How long rewards have been held
   - >2 weeks: 1.1x multiplier
   - >1 month: 1.2x multiplier
   - >2 months: 1.3x multiplier

These factors are combined to produce a final multiplier, with a maximum value of 1.5x.

## Integration with Validators

The vesting system can be integrated with your validator's scoring system to automatically apply multipliers to scores:

```python
# Create vesting system
vesting_system = VestingSystem(...)

# Start the system
await vesting_system.start()

# Integrate with scoring system
vesting_system.integrate_with_scoring(your_scoring_system)
```

This will patch the scoring system's `calculate_weights` method to apply vesting multipliers. 