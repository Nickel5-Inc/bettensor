# Vesting System Integration Guide

## Overview

The vesting system provides incentives for validators to maintain their stake over time. It calculates multipliers based on stake holding percentage and duration, and applies these multipliers to miner weights during the reward distribution process.

## Components

1. **StakeTracker**: Monitors stake balances and calculates holding metrics
2. **StakeRequirements**: Enforces minimum stake requirements and retention policies
3. **VestingSystem**: Main class that integrates with the scoring system
4. **VestingSystemIntegration**: Provides a clean way to hook into the scoring system

## Weight Adjustment Process

The vesting system modifies miner weights through the following process:

1. Original weights are calculated by the validator's scoring system
2. Minimum stake requirements are applied, zeroing out miners whose validator doesn't meet the threshold
3. Vesting multipliers are calculated based on holding metrics
4. Multipliers are applied to weights, and weights are renormalized to sum to 1.0

## Enhanced Weight Redistribution

The vesting system now supports two approaches for redistributing weights from penalized miners:

### Standard Approach (Default)

In the standard approach, weight redistribution is implicit through renormalization:

1. Miners whose validators don't meet minimum stake requirements have their weights set to zero
2. Miners with validators who have poor retention metrics have their weights reduced by multipliers
3. Weights are then renormalized to sum to 1.0, implicitly redistributing the "lost" weight proportionally to all remaining miners

### Enhanced Approach

The enhanced approach provides more explicit control over weight redistribution:

1. First, standard vesting adjustments are applied (minimum stake requirements and multipliers)
2. Then, miners whose adjusted weight falls below specified thresholds are identified:
   - **Relative threshold**: A percentage of the miner's original weight (e.g., 50%)
   - **Absolute threshold**: A minimum absolute weight value (e.g., 0.01)
3. Miners below threshold have their weights completely zeroed out
4. The freed-up weight is explicitly redistributed to remaining miners proportionally to their current weights

This approach allows for more aggressive incentive structures where miners associated with validators having poor metrics don't just receive reduced rewards, but can be excluded entirely from the reward distribution if they fall below acceptable thresholds.

## Configuration

You can configure and control the vesting system using the `scripts/configure_vesting.py` script:

```bash
# Enable vesting with standard redistribution
python scripts/configure_vesting.py enable

# Enable vesting with enhanced redistribution
python scripts/configure_vesting.py enable --enhanced

# Configure enhanced redistribution with custom thresholds
python scripts/configure_vesting.py enable --enhanced --threshold-factor 0.4 --absolute-threshold 0.02

# Check current status
python scripts/configure_vesting.py status

# Disable vesting
python scripts/configure_vesting.py disable
```

## Implementation Details

### Weight Thresholds

The enhanced redistribution approach uses two types of thresholds:

1. **Relative threshold**: A percentage of the miner's original weight
   - Example: With a factor of 0.5, a miner with an original weight of 0.2 would be zeroed out if its adjusted weight falls below 0.1
   
2. **Absolute threshold**: A minimum absolute weight value
   - Example: With a threshold of 0.01, any miner with a weight below 0.01 would be zeroed out

The system uses the maximum of these two thresholds to determine if a miner's weight should be zeroed out.

### Safety Mechanisms

The enhanced approach includes safety mechanisms to prevent extreme scenarios:

1. If all miners would fall below threshold, none are zeroed out
2. The total weight is always maintained at 1.0 through normalization
3. The redistribution is done proportionally to current weights, ensuring fair distribution

## Use Cases

The enhanced redistribution approach is particularly useful for:

1. Creating stronger incentives for validators to maintain stake above minimum requirements
2. Encouraging long-term stake holding by setting clear thresholds for participation
3. Ensuring validators that significantly reduce their stake don't continue to have their associated miners receive rewards, but are excluded until they increase their stake

## Comparison

| Feature | Standard Approach | Enhanced Approach |
|---------|------------------|------------------|
| Implementation | Implicit through renormalization | Explicit with configurable thresholds |
| Incentive Strength | Moderate - Reduced weights | Strong - Complete exclusion possible |
| Configuration | Simple - On/Off | Detailed - Multiple threshold parameters |
| Weight Distribution | Proportional to all miners | Proportional to miners above threshold |
| Use Case | General incentives | Stronger stake holding enforcement | 