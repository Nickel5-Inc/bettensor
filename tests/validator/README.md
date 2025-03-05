# Vesting System Test Suite

This directory contains comprehensive tests for the vesting system implementation.

## Overview

The test suite covers all major components of the vesting system:

1. **StakeTracker** - Tests for tracking stake changes, holding metrics, and transaction recording
2. **StakeRequirements** - Tests for minimum stake requirements and emissions retention
3. **Error Recovery** - Tests for the retry mechanism and transaction management
4. **VestingSystem** - Integration tests for the complete system
5. **Performance** - Benchmarks for critical operations

## Running the Tests

### Using the Test Runner

The easiest way to run tests is using the included test runner script:

```bash
# Run all tests
python tests/validator/run_vesting_tests.py

# Run specific component tests
python tests/validator/run_vesting_tests.py --component tracker
python tests/validator/run_vesting_tests.py --component requirements
python tests/validator/run_vesting_tests.py --component retry
python tests/validator/run_vesting_tests.py --component system

# Run performance benchmarks
python tests/validator/run_vesting_tests.py --component performance

# Generate coverage report
python tests/validator/run_vesting_tests.py --coverage

# Increase verbosity
python tests/validator/run_vesting_tests.py -v
python tests/validator/run_vesting_tests.py -vv

# Run with multiple options
python tests/validator/run_vesting_tests.py --component system --coverage -vv
```

### Using pytest Directly

Alternatively, you can run tests directly with pytest:

```bash
# Run all tests
python -m pytest tests/validator/test_vesting_system.py -v

# Run specific test cases
python -m pytest tests/validator/test_vesting_system.py::TestStakeTracker -v
python -m pytest tests/validator/test_vesting_system.py::TestStakeRequirements -v

# Run performance tests
python -m pytest tests/validator/test_vesting_system.py::TestVestingSystem::test_performance -v

# Generate coverage report
python -m pytest tests/validator/test_vesting_system.py --cov=bettensor.validator.utils.vesting
```

## Test Database

The tests use an in-memory SQLite database that is created fresh for each test. This ensures that tests are isolated and don't interfere with each other or with production data.

## Mock Objects

The tests use mock objects to simulate blockchain interactions:

- `MockMetagraph` - Simulates a network of miners with random stake amounts
- `MockSubtensorClient` - Simulates blockchain API calls

## Test Structure

Each test class focuses on a specific component:

- `TestStakeTracker` - Tests for the StakeTracker class
- `TestStakeRequirements` - Tests for the StakeRequirements class
- `TestRetryMechanism` - Tests for retry and transaction management
- `TestVestingSystem` - Integration and performance tests

## Adding New Tests

When adding new features to the vesting system, please add corresponding tests to ensure functionality and prevent regressions.

1. Add unit tests for new methods
2. Add performance tests for operations that might impact system efficiency
3. Add integration tests for features that interact with multiple components

## Continuous Integration

These tests are automatically run as part of the CI pipeline to ensure code quality and prevent regressions.

## Additional Resources

- [pytest Documentation](https://docs.pytest.org/)
- [SQLAlchemy Testing](https://docs.sqlalchemy.org/en/20/orm/session_transaction.html#session-transaction-tests)
- [Async Testing in Python](https://pytest-asyncio.readthedocs.io/en/latest/) 