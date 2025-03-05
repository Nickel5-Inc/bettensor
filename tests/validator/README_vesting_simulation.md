# Vesting System Simulation Tests

This directory contains simulation tests for the vesting system, which model real-world validator behavior over time.

## Overview

The simulation tests are designed to verify that the vesting system correctly handles complex scenarios that occur in the real world, such as:

1. **Different Types of Stake Transactions**: Distinguishing between manually added stake (purchases), earned stake (rewards), and removed stake (selling)
2. **Stake Changes Over Time**: Tracking stake changes across multiple epochs
3. **Multiple Hotkeys Per Coldkey**: Managing stake across multiple validators owned by the same miner
4. **Minimum Stake Requirements**: Enforcing minimum stake thresholds
5. **Retention Multipliers**: Correctly calculating retention multipliers based on holding metrics

## Test Cases

### 1. Stake Transaction Accounting

This test verifies that the vesting system correctly accounts for different types of stake transactions:

- **Manually Added Stake**: Stake that is purchased/added by miners should not affect retention metrics
- **Earned Rewards**: Stake earned as rewards should be tracked for vesting purposes
- **Removed Stake**: When stake is removed (sold), it should be counted first against manually added stake. Only if more stake is removed than was manually added should it affect retention metrics.

**Example Scenario**:
- A miner adds 10 TAO manually to a validator
- The validator earns 5 TAO in rewards
- The miner removes 12 TAO
- The system should count the first 10 TAO against manually added stake (no penalty), and only the remaining 2 TAO should affect retention metrics

### 2. Coldkey Total Stake Tracking

This test verifies that the vesting system correctly tracks the total stake across multiple hotkeys owned by the same coldkey:

- One coldkey can own multiple hotkeys (validators)
- Stake changes on different hotkeys should be correctly reflected in the total
- The system should enforce minimum stake requirements based on the total stake

### 3. Minimum Stake Enforcement

This test verifies that the minimum stake requirement is properly enforced:

- Validators with stake below the minimum threshold should have their weights zeroed out
- Validators meeting the minimum stake requirement should receive normal weights
- Weight redistribution should occur properly when some validators are filtered out

### 4. Holding Metrics Impact

This test verifies that holding metrics (percentage and duration) correctly impact validator weights:

- Stronger holders (higher percentage held for longer duration) should receive higher weights
- Weaker holders should receive lower weights
- The weights should be proportional to the strength of holding

### 5. Stake Changes Simulation

This test models a 90-day simulation with various stake changes:

- Initial stake distribution
- Stake removals and additions at different points in time
- Verification that the weight calculations correctly account for these changes

## Mocking Strategy

Since these tests need to verify complex behaviors without actually connecting to a blockchain, they use sophisticated mocking techniques:

- **Controlled Time**: The tests simulate the passage of time to model stake holding duration
- **Transaction Recording**: The tests track all stake transactions to verify proper accounting
- **Stake History**: The tests maintain a history of stake changes to verify holding metrics
- **Override Methods**: Key methods in the vesting system are temporarily overridden to inject specific test behaviors

## Running the Tests

To run all simulation tests:

```bash
python -m pytest tests/validator/test_vesting_simulation.py
```

To run a specific simulation test:

```bash
python -m pytest tests/validator/test_vesting_simulation.py::TestVestingSimulation::test_stake_transaction_accounting
```

## Adding New Simulation Tests

When adding new simulation tests, follow these guidelines:

1. **Model Real-World Behavior**: Focus on scenarios that model real-world validator behavior
2. **Include Clear Assertions**: Verify expected behavior with specific assertions
3. **Clean Up Mocks**: Always restore original methods in the `finally` block
4. **Document Expectations**: Include clear documentation of what the test is verifying

## Relation to Unit Tests

The simulation tests complement the unit tests in `test_vesting_system.py`:

- **Unit Tests**: Verify that individual components work correctly in isolation
- **Simulation Tests**: Verify that the components work correctly together in complex scenarios

Together, these tests provide comprehensive coverage of the vesting system's functionality. 