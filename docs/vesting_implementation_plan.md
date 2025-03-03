# Vesting Rewards System Implementation Plan

## Core Requirements

1. **Track Stake Holdings**: Monitor how much stake miners are holding and for how long.
2. **Apply Multipliers**: Automatically calculate and apply stake multipliers to miners' rewards based on holding percentage and duration.
3. **Record Emissions**: Keep track of emissions for each miner to calculate holding percentages.
4. **Integration**: Seamlessly integrate with the existing scoring and rewards system.

## Technical Components

1. ✅ **Vesting Tracker**:
   - ✅ Track hotkey to coldkey associations
   - ✅ Record emissions per miner
   - ✅ Track stake history
   - ✅ Calculate holding metrics (percentage and duration)

2. ✅ **Multiplier Calculator**:
   - ✅ Define stake and duration tiers
   - ✅ Implement multiplier calculation logic
   - ✅ Apply multipliers to scores/weights

3. ✅ **Database Schema**:
   - ✅ Tables for stake history
   - ✅ Tables for emissions recording
   - ✅ Indices for efficient querying

4. ✅ **Integration Module**:
   - ✅ Hook into the scoring system
   - ✅ Apply vesting multipliers to final weights
   - ✅ Method to enable/disable the vesting system

5. ✅ **CLI & Scripts**:
   - ✅ Script to enable/disable vesting rewards
   - ✅ Test script for verification

## Implementation Status

### Phase 1: Core Infrastructure (✅ COMPLETED)

- ✅ Design database schema for stake tracking and emissions recording
- ✅ Implement VestingTracker class for monitoring stake and calculating metrics
- ✅ Create multiplier calculation function based on holding percentage and duration
- ✅ Build integration with the scoring system

### Phase 2: Application & Testing (✅ COMPLETED)

- ✅ Implement script to enable/disable vesting rewards
- ✅ Create comprehensive test script
- ✅ Integration with the scoring system's calculate_weights method

### Phase 3: Deployment & Monitoring (⏳ IN PROGRESS)

- ⏳ Deploy to production
- ⏳ Monitor system performance
- ⏳ Gather feedback and adjust parameters if necessary

## Files Created

1. **Database Module**:
   - Leveraged existing `DatabaseManager` class

2. **Vesting Module**:
   - ✅ `bettensor/validator/utils/vesting/tracker.py`: VestingTracker class
   - ✅ `bettensor/validator/utils/vesting/multipliers.py`: Multiplier calculation functions
   - ✅ `bettensor/validator/utils/vesting/integration.py`: Integration with scoring system
   - ✅ `bettensor/validator/utils/vesting/__init__.py`: Module exports

3. **Scripts & Tests**:
   - ✅ `scripts/apply_vesting.py`: CLI utility for enabling/disabling vesting rewards
   - ✅ `tests/vesting_test.py`: Comprehensive test script

4. **Scoring System Extensions**:
   - ✅ Added `get_active_scoring_system()` function to access the current instance

## Risk Management

| Risk | Mitigation |
|------|------------|
| Multipliers could skew reward distribution too dramatically | Implemented conservative multiplier tiers with gradual increases |
| Performance impact on weight calculation | Optimized database queries and caching of stake history |
| Incorrect tracking of stake | Unit tests to verify accuracy |

## Next Steps

1. Deploy the vesting rewards system to production
2. Monitor system performance and effectiveness
3. Collect feedback from validators and miners
4. Adjust multiplier parameters if necessary based on real-world data 