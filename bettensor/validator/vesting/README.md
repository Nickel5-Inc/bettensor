# Bettensor Vesting System

The Bettensor Vesting System is designed to incentivize miners to hold their earned rewards rather than immediately selling them. This promotes long-term commitment and alignment with the network's interests.

## Overview

The vesting system tracks stake changes on the blockchain and applies multipliers to miners' scores based on:

1. **Amount Held**: The percentage of rewards a miner has retained vs. sold
2. **Duration Held**: How long the miner has maintained their stake

Miners who maintain a higher stake for longer periods receive higher multipliers, leading to increased rewards.

## Enhanced Tranche-Based Accounting with FILO

The vesting system uses a sophisticated tranche-based accounting system with FILO (First-In-Last-Out) to accurately track stake holding duration:

### What are Tranches?

In financial terms, a "tranche" is a portion of something, especially money. In our system, each stake addition (whether manual or from emissions/rewards) creates a new "tranche" with:
- The amount added
- A timestamp of when it was added
- Whether it's from emissions or manual stake

### FILO Accounting for Stake Withdrawals

When stake is withdrawn, we use First-In-Last-Out (FILO) accounting:
1. Newest tranches are consumed first
2. Oldest tranches are preserved as long as possible
3. Partial tranche consumption is tracked precisely
4. Each withdrawal creates "exit" records for affected tranches

This approach allows miners to:
- Build up long-term holdings while still having operational liquidity
- Withdraw newer rewards without resetting their holding duration
- Gradually increase their average holding duration over time
- Achieve meaningful multipliers even with periodic withdrawals

### Economic Benefits of FILO Accounting

1. **Realistic Operational Model**: Miners can use newer rewards for operational expenses while maintaining long-term holdings
2. **Gradual Multiplier Building**: Holding duration can increase over time even with periodic withdrawals
3. **Fairer Incentives**: Smaller miners with liquidity needs can still achieve meaningful multipliers
4. **Network Stability**: Prevents "cliff selling" where miners hold until hitting a target then sell everything
5. **Long-term Alignment**: Still rewards long-term holding but with more flexible and realistic expectations

### Implementation Details

- Each stake addition creates a new tranche record
- Withdrawals consume tranches using FILO accounting (newest first)
- Partial tranche consumption is tracked with exit records
- Numpy is used for efficient vector calculations
- Metrics are cached for performance optimization

## Automatic Metagraph Syncing and Deregistration Handling

The vesting system includes advanced features to manage the lifecycle of miners in the network:

### Metagraph Initialization

On system startup, the vesting system can automatically initialize stake data from the metagraph:

- **Automatic Detection**: The system checks if stake data exists and initializes from metagraph if needed
- **Initial Stake Setup**: Creates initial stake tranches for all miners in the metagraph
- **Update Mode**: Can update existing entries to match current metagraph state
- **Force Mode**: Option to force reinitialize all miners regardless of existing data

### Deregistration Handling

The system can automatically detect and handle deregistered miners:

- **Auto Detection**: Compares metagraph with database to find deregistered miners
- **Data Cleanup**: Removes all vesting data associated with deregistered miners
- **Cascading Updates**: Properly updates coldkey metrics when hotkeys are deregistered
- **Manual Mode**: Can also handle specific deregistrations passed from the validator

These features ensure that the vesting system stays in sync with the network state, properly handling the entire lifecycle of miners from registration to deregistration.

## Components

The system consists of four main components:

### 1. BlockchainMonitor

Tracks stake changes on the blockchain by:
- Monitoring manual stake transactions (add/remove)
- Tracking epoch-based balance changes (rewards/emissions)
- Differentiating between manual stake and earned rewards

```python
from bettensor.validator.utils.vesting import BlockchainMonitor

monitor = BlockchainMonitor(
    subtensor=subtensor,
    subnet_id=19,
    db_manager=db_manager
)
await monitor.initialize()
```

### 2. TransactionMonitor

Provides detailed, granular tracking of stake-related transactions on the blockchain:
- Tracks various transaction types: add_stake, remove_stake, move_stake, transfer_stake, swap_stake
- Categorizes transactions as inflows, outflows, neutral, or emissions
- Matches extrinsics (transaction requests) with events (confirmed transactions)

```python
from bettensor.validator.utils.vesting import TransactionMonitor

transaction_monitor = TransactionMonitor(
    subtensor=subtensor,
    subnet_id=19,
    db_manager=db_manager
)
await transaction_monitor.initialize()
transaction_monitor.start_monitoring()
```

### 3. StakeTracker

Maintains stake metrics and differentiates between manual and earned stake:
- Tracks current stake amounts for each miner
- Differentiates between manually added stake and earned rewards
- Calculates holding metrics (percentage and duration)
- Manages many-to-one hotkey-coldkey relationships
- Implements tranche-based FILO accounting for accurate duration tracking

```python
from bettensor.validator.utils.vesting import StakeTracker

stake_tracker = StakeTracker(db_manager=db_manager)
await stake_tracker.initialize()
```

### 4. VestingSystem

Main system that integrates all components and applies multipliers to miner weights:
- Connects blockchain monitoring with the scoring system
- Calculates vesting multipliers based on holding metrics
- Provides statistics and metrics for the vesting system
- Automatically initializes from metagraph and handles deregistrations

```python
from bettensor.validator.utils.vesting import VestingSystem

vesting_system = VestingSystem(
    subtensor=subtensor,
    subnet_id=19,
    db_manager=db_manager,
    minimum_stake=0.3,  # Minimum stake required for multiplier
    retention_window_days=30,  # Window for calculating retention
    retention_target=0.9,  # Target retention percentage for max multiplier
    max_multiplier=1.5  # Maximum multiplier to apply
)
await vesting_system.initialize(auto_init_metagraph=True)  # Auto-initialize from metagraph
```

## Integration with Scoring System

The vesting system integrates with the scoring system via the `VestingIntegration` class, which patches the scoring system's `calculate_weights` method to apply vesting multipliers:

```python
from bettensor.validator.utils.vesting import VestingIntegration

# Initialize integration
integration = VestingIntegration(
    scoring_system=scoring_system,
    vesting_system=vesting_system
)

# Install integration (patches scoring_system.calculate_weights)
integration.install()

# Later, when you're done
integration.uninstall()  # Restores original method
```

After installation, whenever `scoring_system.calculate_weights()` is called, it will automatically apply vesting multipliers to the weights.

## Example Usage with Metagraph Syncing and Deregistration

```python
import asyncio
import bittensor as bt
from bettensor.validator.utils.database.database_manager import DatabaseManager
from bettensor.validator.utils.scoring.scoring import ScoringSystem
from bettensor.validator.utils.vesting import VestingSystem, VestingIntegration

async def main():
    # Initialize components
    subtensor = bt.subtensor()
    db_manager = DatabaseManager("./validator.db")
    await db_manager.initialize()
    
    # Get metagraph
    metagraph = subtensor.metagraph(netuid=19)
    metagraph.sync(subtensor=subtensor)
    
    # Initialize vesting system with auto-init from metagraph
    vesting_system = VestingSystem(
        subtensor=subtensor,
        subnet_id=19,
        db_manager=db_manager,
        minimum_stake=0.3,
        retention_window_days=30,
        retention_target=0.9,
        max_multiplier=1.5
    )
    await vesting_system.initialize(auto_init_metagraph=True)
    
    # Force reinitialize from metagraph if needed
    # await vesting_system.initialize_from_metagraph(metagraph=metagraph, force_update=True)
    
    # Handle deregistered miners if any
    num_deregistered = await vesting_system.handle_deregistered_keys()
    print(f"Cleaned up data for {num_deregistered} deregistered miners")
    
    # Setup scoring system and integration
    scoring_system = ScoringSystem(db_manager=db_manager)
    await scoring_system.initialize()
    
    integration = VestingIntegration(
        scoring_system=scoring_system,
        vesting_system=vesting_system
    )
    integration.install()
    
    # Calculate weights (will automatically apply vesting multipliers)
    weights = scoring_system.calculate_weights()
    
    # Cleanup
    await vesting_system.shutdown()
    await db_manager.close()

if __name__ == "__main__":
    asyncio.run(main())
```

## How Multipliers Are Calculated

Multipliers are calculated based on two factors:

1. **Holding Percentage**: The percentage of total stake that is still held (vs. sold)
   - Formula: `percentage_factor = min(1.0, holding_percentage / retention_target)`
   
2. **Holding Duration**: How long the stake has been held, relative to the retention window
   - Formula: `duration_factor = min(1.0, holding_duration / retention_window_days)`

The final multiplier combines these factors using a geometric mean:
   - `combined_factor = (percentage_factor * duration_factor) ** 0.5`
   - `multiplier = 1.0 + combined_factor * (max_multiplier - 1.0)`

## Flow Categorization

The system categorizes stake transactions into four flow types:

1. **INFLOW**: Stake added to the hotkey (AddStake, destination of MoveStake/SwapStake)
2. **OUTFLOW**: Stake removed from the hotkey (RemoveStake, origin of MoveStake/TransferStake/SwapStake)
3. **NEUTRAL**: No net change to the hotkey's stake (certain TransferStake scenarios)
4. **EMISSION**: Stake added via network emissions/rewards

This categorization helps maintain accurate accounting of manual vs. earned stake.

## Example Usage

Complete example of initializing and using the vesting system:

```python
import asyncio
import bittensor as bt
from bettensor.validator.utils.database.database_manager import DatabaseManager
from bettensor.validator.utils.scoring.scoring import ScoringSystem
from bettensor.validator.utils.vesting import VestingSystem, VestingIntegration

async def main():
    # Initialize components
    subtensor = bt.subtensor()
    db_manager = DatabaseManager("./validator.db")
    await db_manager.initialize()
    
    # Initialize scoring system
    scoring_system = ScoringSystem(db_manager=db_manager)
    await scoring_system.initialize()
    
    # Initialize vesting system
    vesting_system = VestingSystem(
        subtensor=subtensor,
        subnet_id=19,
        db_manager=db_manager,
        minimum_stake=0.3,
        retention_window_days=30,
        retention_target=0.9,
        max_multiplier=1.5
    )
    await vesting_system.initialize()
    
    # Install integration
    integration = VestingIntegration(
        scoring_system=scoring_system,
        vesting_system=vesting_system
    )
    integration.install()
    
    # Calculate weights (will automatically apply vesting multipliers)
    weights = scoring_system.calculate_weights()
    
    # Get vesting stats
    stats = await vesting_system.get_vesting_stats()
    print(f"Vesting stats: {stats}")
    
    # Get detailed tranche information for a specific hotkey
    tranche_details = await vesting_system.stake_tracker.get_tranche_details(
        hotkey="your_hotkey_here",
        include_exits=True
    )
    
    # Cleanup
    await vesting_system.shutdown()
    await db_manager.close()

if __name__ == "__main__":
    asyncio.run(main())
```

See the example script at `examples/vesting_integration_example.py` for a complete example.

## Database Management

All database tables for the vesting system are now defined in `database_schema.py` and created centrally by the `DatabaseManager`. This improves organization and ensures consistent schema management across the entire system.

### Integration with DatabaseManager

The vesting system integrates with the centralized `DatabaseManager` using the following hooks:

1. The `database_schema.py` file defines all table schemas and indices
2. The `integration.py` file provides the `initialize_vesting_tables()` function for the `DatabaseManager` to call
3. Each component (`BlockchainMonitor`, `StakeTracker`, etc.) now relies on tables being created centrally

### Integrating with Validator

The vesting system integrates with the validator in several key ways:

1. **Deregistration handling**: When a miner is deregistered, the validator can call `handle_deregistered_miner()` from the `integration.py` module to clean up all vesting data for that miner.

2. **Weight application**: The vesting system provides a function `apply_vesting_multipliers()` in the `integration.py` module that the validator can call during weight setting to apply vesting-based multipliers to the base weights.

3. **Epoch boundary monitoring**: The vesting system monitors epoch boundaries and processes stake changes, which the validator can use to track emissions versus manual stake changes.

## Transaction Categorization

The system categorizes transactions based on their flow direction:

- **Inflow**: Transactions that increase stake (AddStake)
- **Outflow**: Transactions that decrease stake (RemoveStake, Burn)
- **Neutral**: Transactions that move stake (MoveStake, TransferOwnership)
- **Emission**: Stake increases from emissions/rewards

## Troubleshooting

Common issues and solutions:

- **No vesting multipliers being applied**: Ensure the vesting system is initialized and the integration is installed
- **Incorrect stake tracking**: Check that the blockchain monitor is running and tracking transactions
- **Database errors**: Verify that the database manager is initialized correctly
- **Incorrect minimum stake requirement**: Adjust the `minimum_stake` parameter in the VestingSystem constructor

## Performance Considerations

The vesting system is designed to be efficient, but can be tuned for performance:

- **Background Thread**: Enable `use_background_thread=True` to run blockchain monitoring in a background thread
- **Query Interval**: Adjust `query_interval_seconds` to control how often the blockchain is queried
- **Detailed Transaction Tracking**: Disable `detailed_transaction_tracking=False` for lighter-weight tracking
- **Metric Caching**: Aggregated tranche metrics are cached for one hour to reduce computation
- **Numpy Optimizations**: Vector operations use numpy for efficient calculation

## Web API Integration

The vesting system can now expose all its data to external web services through the `VestingAPIInterface`. This interface provides methods to collect, format, and push vesting-related data to a web API endpoint. This is useful for:

1. **Dashboards**: Create web dashboards showing vesting stats and multipliers
2. **Analytics**: Perform advanced analytics on stake data
3. **Monitoring**: Real-time monitoring of vesting system health
4. **User Interfaces**: Build user interfaces for miners to check their vesting status

### API Interface Features

- **System Overview**: Push system configuration, stats, and health information
- **Miner Metrics**: Push stake metrics for all miners, including holding percentages and multipliers
- **Coldkey Data**: Push aggregated metrics for coldkeys and their associated hotkeys
- **Transaction History**: Push detailed transaction history with flow categorization
- **Tranche Data**: Push tranche-level data for detailed retention analysis
- **Multiplier Data**: Push calculated multipliers for all miners
- **Incremental Updates**: Support for incremental data updates to minimize bandwidth
- **Batched Processing**: Support for processing data in batches for large datasets
- **Background Polling**: Automatically push data updates on a configurable schedule

### Using the API Interface

```python
from bettensor.validator.utils.vesting.api_interface import VestingAPIInterface

# Create API interface
api_interface = VestingAPIInterface(
    vesting_system=vesting_system,
    api_base_url="https://api.example.com",
    api_key="your_api_key",
    poll_interval_seconds=300,
    push_enabled=True
)

# Initialize API interface
await api_interface.initialize()

# Start background polling (if push_enabled=True)
await api_interface.start()

# Push all data
await api_interface.push_all_data()

# Or push specific data
await api_interface.push_system_overview()
await api_interface.push_all_stake_metrics()
await api_interface.push_all_coldkey_metrics()
await api_interface.push_recent_transactions()
await api_interface.push_vesting_multipliers()
await api_interface.push_tranche_data()

# Get details for a specific miner
details = await api_interface.request_miner_details("miner_hotkey")

# Stop the API interface
await api_interface.stop()
```

See the example script at `examples/vesting_api_example.py` for a complete example.

### Running as a Daemon

The API interface can be run as a background daemon to continuously push data updates:

```bash
python examples/vesting_api_example.py \
    --netuid 30 \
    --api.url https://api.example.com \
    --api.key your_api_key \
    --api.push_enabled \
    --action daemon
```

This will start the interface in daemon mode, continuously pushing updates to the API endpoint. 