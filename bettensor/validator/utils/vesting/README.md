# Bettensor Vesting System

The Bettensor Vesting System is designed to incentivize miners to hold their rewards rather than immediately selling them. This creates a more stable ecosystem and rewards long-term participants.

## System Components

The vesting system consists of three main components:

1. **BlockchainMonitor**: Tracks stake changes and transactions on the blockchain
2. **StakeTracker**: Manages stake metrics and calculations
3. **VestingSystem**: Integrates with the scoring system to apply multipliers

### BlockchainMonitor

The `BlockchainMonitor` is responsible for:
- Tracking manual stake transactions (add/remove)
- Tracking balance changes between epochs (rewards/emissions)
- Detecting epoch boundaries with high precision
- Capturing stake snapshots before and after epoch boundaries
- Calculating true emissions by comparing pre and post epoch stakes
- Providing stake history for miners

The monitor can run in a background thread for improved performance, which prevents blockchain queries from blocking the main execution thread.

### StakeTracker

The `StakeTracker` is responsible for:
- Tracking current stake amounts
- Differentiating between manual and earned stake
- Calculating holding metrics (percentage and duration)
- Managing many-to-one hotkey-coldkey relationships

### VestingSystem

The `VestingSystem` is responsible for:
- Integrating the blockchain monitor and stake tracker
- Applying vesting multipliers to scoring weights
- Enforcing minimum stake requirements
- Providing vesting statistics
- Managing the background thread for blockchain monitoring

## Integration with Scoring System

The vesting system integrates with the scoring system by applying multipliers to the weights calculated by the scoring system. The multipliers are based on:

1. The percentage of stake that is held (vs. sold)
2. The duration of holding

Miners who hold a higher percentage of their rewards for a longer period will receive a higher multiplier, up to a configured maximum.

## Configuration

The vesting system can be configured with the following parameters:

- `minimum_stake`: Minimum stake required to receive a multiplier
- `retention_window_days`: Window for calculating retention metrics
- `retention_target`: Target retention percentage for max multiplier
- `max_multiplier`: Maximum multiplier to apply
- `use_background_thread`: Whether to run blockchain monitoring in a background thread
- `query_interval_seconds`: Interval between blockchain queries in seconds

## Usage

```python
# Initialize components
subtensor = bt.subtensor(network="finney")
db_manager = DatabaseManager("path/to/database.db")

# Create vesting system
vesting_system = VestingSystem(
    subtensor=subtensor,
    subnet_id=1,  # Replace with your subnet ID
    db_manager=db_manager,
    minimum_stake=0.3,
    retention_window_days=30,
    retention_target=0.9,
    max_multiplier=1.5,
    use_background_thread=True,
    query_interval_seconds=300
)

# Initialize
await vesting_system.initialize()

# The background thread will automatically update stake metrics
# No need to call update() manually

# Apply multipliers to weights
adjusted_weights = await vesting_system.apply_vesting_multipliers(
    weights=original_weights,
    uids=uids,
    hotkeys=hotkeys
)

# Get vesting statistics
stats = await vesting_system.get_vesting_stats()

# Shutdown when done
await vesting_system.shutdown()
```

## Background Thread

The blockchain monitor can run in a background thread, which provides several benefits:

1. **Non-blocking operation**: Blockchain queries don't block the main execution thread
2. **Automatic updates**: Stake changes are tracked automatically without manual intervention
3. **Real-time monitoring**: Epoch boundaries are detected as they occur
4. **Improved responsiveness**: The main application remains responsive during blockchain queries

The background thread periodically:
1. Checks for epoch boundaries using direct substrate queries
2. Tracks manual stake transactions
3. Captures stake snapshots before and after epoch boundaries
4. Calculates and records emissions when epoch boundaries are detected

## Epoch Boundary Detection

The system uses a sophisticated approach to detect epoch boundaries:

1. **Direct Substrate Queries**: Uses the substrate interface to query `BlocksSinceLastStep` directly
2. **Pre-Epoch Snapshots**: Captures stake balances just before an epoch boundary
3. **Post-Epoch Snapshots**: Captures stake balances immediately after an epoch boundary
4. **Emission Calculation**: Compares pre and post epoch stakes to calculate true emissions
5. **Fallback Mechanism**: Falls back to subtensor if direct substrate queries fail

This approach provides more accurate and reliable epoch boundary detection than simply checking the `last_step` value.

## Database Schema

The vesting system uses the following database tables:

- `stake_transactions`: Records of manual stake transactions
- `stake_balance_changes`: Records of balance changes between epochs
- `blockchain_state`: State information for the blockchain monitor
- `epoch_boundaries`: Records of epoch boundaries
- `stake_snapshots`: Snapshots of stake balances before and after epoch boundaries
- `stake_changes`: Records of stake changes between epochs, including true emissions

## Performance Considerations

The vesting system is designed to be efficient and scalable:

- Uses background threading to prevent blocking the main thread
- Uses caching to minimize RPC calls
- Batches database operations where possible
- Implements efficient querying patterns
- Handles many-to-one hotkey-coldkey relationships 