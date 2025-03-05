# Bittensor Balance Integration

This document describes the integration of the `bittensor.Balance` class into the vesting system to improve precision and type safety when handling token amounts.

## Overview

The Bittensor network uses a custom `Balance` class to represent token amounts, providing better precision and consistency when dealing with TAO tokens and their subunits (RAO). Previously, our vesting system was using raw float values to represent token amounts, which could lead to precision issues and inconsistent handling.

This integration replaces all raw float values representing token amounts with the `bittensor.Balance` class, which provides:

- Precise representation of token amounts with proper decimal handling
- Consistent formatting and display of token values
- Mathematical operations that maintain precision
- Clear distinction between TAO and Alpha token amounts

## Implementation Details

### BalanceType for SQLAlchemy

We've created a custom SQLAlchemy type called `BalanceType` that handles the conversion between `bittensor.Balance` objects and database float values:

```python
class BalanceType(TypeDecorator):
    """SQLAlchemy type for bittensor.Balance objects"""
    impl = Float
    
    def process_bind_param(self, value, dialect):
        """Convert Balance to float when storing in database"""
        if value is None:
            return None
        if isinstance(value, bt.Balance):
            return value.tao
        return float(value)
        
    def process_result_value(self, value, dialect):
        """Convert float to Balance when loading from database"""
        if value is None:
            return None
        return bt.Balance.from_tao(value)
```

This type automatically converts between `Balance` objects and float values when reading from or writing to the database.

### Model Updates

All database models that store token amounts have been updated to use the `BalanceType`:

- `StakeTransaction.amount`
- `StakeHistory.stake`
- `VestingSchedule.initial_amount` and `remaining_amount`
- `VestingPayment.amount`
- `EpochEmissions.emission_amount` and `retained_amount`
- `StakeMinimumRequirement.minimum_stake`

### Stake Tracker Updates

The `StakeTracker` has been updated to handle `Balance` objects when recording transactions:

```python
# If it's not already a Balance object, convert it
if not isinstance(tao_amount, bt.Balance):
    tao_amount = bt.Balance.from_tao(tao_amount)

# Convert from tao to alpha
alpha_amount = self.dynamic_info.tao_to_alpha(tao_amount)
```

## Migration

A migration script (`scripts/migrate_to_balance_type.py`) has been provided to help transition existing databases to use the new `BalanceType`. The script performs the following steps:

1. Creates a backup of the existing database
2. Updates the database schema to use the new `BalanceType`

Since `BalanceType` handles the conversion automatically, no data migration is necessary - the existing float values will be converted to `Balance` objects when they are read from the database.

## Testing

A comprehensive test suite has been created in `tests/validator/test_balance_type.py` to verify that the `Balance` integration works correctly, including:

- Storing and retrieving `Balance` objects directly using SQLAlchemy
- Recording transactions with both `Balance` objects and raw floats using the `StakeTracker`
- Converting between TAO and Alpha amounts

## Usage Examples

### Creating a Balance Object

```python
import bittensor as bt

# From TAO amount
balance = bt.Balance.from_tao(10.5)

# From float (same as from_tao)
balance = bt.Balance.from_float(10.5)

# Access the TAO value
tao_value = balance.tao  # 10.5
```

### Using Balance with StakeTracker

```python
# With a Balance object
await stake_tracker.record_stake_transaction(
    transaction_type="add_stake",
    hotkey="0xhotkey",
    coldkey="0xcoldkey",
    amount=bt.Balance.from_tao(10.5),
    extrinsic_hash="0xhash",
    block_number=100,
    block_timestamp=datetime.utcnow()
)

# With a float (automatically converted to Balance)
await stake_tracker.record_stake_transaction(
    transaction_type="add_stake",
    hotkey="0xhotkey",
    coldkey="0xcoldkey",
    amount=10.5,  # Will be converted to Balance
    extrinsic_hash="0xhash",
    block_number=100,
    block_timestamp=datetime.utcnow()
)
```

## Benefits of Balance over Float

- **Type Safety**: Clear indication that values represent token amounts
- **Precision**: Consistent handling of decimal places
- **Operations**: Mathematical operations maintain proper precision
- **Formatting**: Consistent string representation with currency symbol (τ) 