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
6. **Minimum Stake Requirements**: Enforces minimum stake requirements for receiving rewards
7. **Emissions Retention**: Tracks and incentivizes the retention of earned rewards

## Directory Structure

```
bettensor/validator/utils/vesting/
├── __init__.py                 # Public exports
├── system.py                   # Main VestingSystem class
├── core/                       # Core components
│   ├── __init__.py
│   ├── multipliers.py          # Vesting multiplier calculations
│   ├── stake_tracker.py        # Stake tracking and history
│   ├── vesting_scheduler.py    # Vesting schedule management
│   └── stake_requirements.py   # Minimum stake & emissions retention
├── blockchain/                 # Blockchain interaction
│   ├── __init__.py
│   ├── subtensor_client.py     # Subtensor API client
│   └── transaction_monitor.py  # Blockchain transaction monitoring
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
- Distinguishing between earned rewards and manually added stake
- Enforcing minimum stake requirements
- Tracking emissions retention rates

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

#### StakeRequirements

The `StakeRequirements` enforces minimum stake requirements and tracks emissions retention:

- Checks if miners meet minimum stake requirements
- Tracks emissions by epoch
- Calculates emissions retention rates
- Computes retention-based multipliers
- Filters and redistributes weights based on requirements

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

#### TransactionMonitor

The `TransactionMonitor` provides real-time monitoring of blockchain transactions:

- Detects stake-related transactions on the blockchain
- Distinguishes between manually added stake and earned rewards
- Tracks add_stake, remove_stake, and network reward events
- Allows for accurate tracking of a miner's earned rewards vs. manual stake contributions

### Database Models

The system uses these database models to store data:

1. `HotkeyColdkeyAssociation`: Maintains associations between hotkeys and coldkeys
2. `StakeHistory`: Records stake history and changes over time
3. `StakeTransaction`: Tracks individual stake transactions on the blockchain
4. `VestingSchedule`: Defines vesting schedules for earned rewards
5. `VestingPayment`: Tracks individual payments from vesting schedules
6. `EpochEmissions`: Records emissions earned by miners per epoch
7. `StakeMinimumRequirement`: Tracks minimum stake requirements for hotkeys

## Minimum Stake Requirements

The vesting system enforces minimum stake requirements for miners to receive rewards:

- Each hotkey must have at least 0.3 TAO equivalent staked to receive rewards
- For coldkeys with multiple hotkeys, the total stake must meet the sum of minimums for all hotkeys
- Miners not meeting the minimum requirement receive zero weight (and thus zero rewards)
- Weights from ineligible miners are redistributed among eligible miners

This feature ensures that miners have "skin in the game" before receiving rewards.

## Emissions Retention Incentives

The system incentivizes miners to retain (not sell) their earned rewards:

- The system tracks emissions (rewards) earned during each epoch
- It monitors what percentage of those emissions are retained over time (not sold/unstaked)
- A linear retention multiplier is applied to scores:
  - If a miner retains at least 90% of emissions, they get a full 1.0 multiplier
  - Otherwise, the multiplier scales linearly (e.g., retaining 45% = 0.5 multiplier)
- This retention multiplier is applied before the existing vesting bonuses
- The retention window is 30 days, meaning emissions are tracked for this period

This feature encourages miners to hold their rewards, reducing selling pressure and supporting token value.

## Earned Rewards vs. Purchased Stake

The vesting system distinguishes between:

1. **Earned Rewards**: Stake gained through participation in the subnet (automatic rewards)
2. **Purchased Stake**: Stake manually added or removed by the miner's coldkey

This distinction is important because:

- Only earned rewards should be considered for vesting schedules
- Manually added stake should not be rewarded with vesting multipliers
- Manually removed stake should be tracked separately from earned rewards
- Holding percentage calculations should focus on rewards retention

The system monitors the blockchain for all stake change events and classifies them as either:

- `add_stake`: Manual stake addition by a miner's coldkey
- `remove_stake`: Manual stake removal by a miner's coldkey
- `reward`: Automatic stake addition from subnet rewards
- `penalty`: Automatic stake reduction from subnet penalties

## Multiplier Stacking

The system applies multiple multipliers that stack with each other:

1. **Minimum Stake Filter**: Sets weight to zero if minimum requirement not met
2. **Emissions Retention Multiplier**: Based on percentage of emissions retained (0.0-1.0)
3. **Vesting Multiplier**: Based on holding percentage and duration (1.0-1.5)

The effective multiplier is the product of these components, encouraging both minimum stake levels and long-term holding.

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