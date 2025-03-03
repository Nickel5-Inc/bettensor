# Bettensor System Overhaul: Design Document

## Priority Notice
**IMPORTANT**: The vesting rewards system (Goal 3) has been identified as the highest priority feature and will be implemented first. Other goals will be addressed after the vesting system is complete.

## Overview
This document outlines the comprehensive plan for overhauling the Bettensor application to support new bet types, improve miner reward mechanisms, implement vesting rewards, enhance CLI functionality, optimize system performance, and revamp the entropy scoring system.

## Current Architecture

The Bettensor system currently:
- Scores miners based on moneyline betting predictions
- Uses a tiered structure for miner classification
- Calculates scores using CLV (closing line value), ROI, risk assessment, and entropy
- Manages daily scoring, tier promotions/demotions
- Stores state in a database
- Operates in a decentralized network where miners submit predictions

## Goal 1: Support for Additional Bet Types

### Requirements
- Add support for point spread, over/under, and totals betting
- Maintain compatibility with existing moneyline bet scoring
- Implement fair scoring across all bet types

### Files to Modify
- `bettensor/validator/utils/scoring/scoring.py`
  - Update `_update_raw_scores()` to handle multiple bet types
  - Modify `_calculate_clv_scores()`, `_calculate_roi_scores()`, and `_calculate_risk_scores()`
- Database schema modifications to support new bet structures
- Prediction validation and processing modules

### New Files to Create
- `bettensor/validator/utils/scoring/bet_types.py` 
  - Define base `BetType` class and subclasses for each type
- `bettensor/validator/utils/scoring/spread_scoring.py`
  - Point spread-specific scoring algorithms
- `bettensor/validator/utils/scoring/totals_scoring.py` 
  - Over/under and totals scoring logic

### Technical Design
```python
class BetType:
    def validate(self, prediction):
        """Base validation method"""
        
    def calculate_clv(self, prediction, closing_odds):
        """Base CLV calculation"""
        
    def calculate_roi(self, prediction, result):
        """Base ROI calculation"""
        
    def calculate_risk(self, prediction, result):
        """Base risk calculation"""

class MoneylineBet(BetType):
    # Current implementation moved here

class PointSpreadBet(BetType):
    # Implementation for point spread bets
    
class OverUnderBet(BetType):
    # Implementation for over/under bets
    
class TotalsBet(BetType):
    # Implementation for totals bets
```

### Risk Assessment
- **Data Structure Changes**: Will require database migrations
- **Backward Compatibility**: Must ensure existing moneyline bet scoring remains unchanged
- **Performance Impact**: More complex calculations may increase processing time

### Mitigation Strategy
- Comprehensive test suite for each bet type
- Phased rollout starting with one bet type
- Performance benchmarking before/after

### Clarification Responses

#### Database Schema Changes
We will need a new schema for games and predictions. Currently, we hold the data in the games and predictions tables. The new design must:
- Store multiple bet types associated with each game
- Handle cases where not all games have all bet types
- Store multiple sets of odds for each bet type at various "levels"
- Record miner submissions for each bet type
- Support evaluation and reward calculation for each bet type

#### Historical Data Migration
We will migrate the historical moneyline performance of miners so they don't lose their current position in the system. This ensures continuity in scoring and tier status.

#### Scoring Approach
All bet types will be scored evenly using the same metrics currently applied to moneyline bets (CLV, ROI, risk assessment, etc.).

#### Multiple Submissions
The system will accept all submissions from miners as long as they don't exceed their daily wager balance, regardless of which bet types they choose to engage with.

#### Validation Rules
We will only accept bets that match the odds and levels we have provided. Any submission that doesn't align with our offered bet types, odds, or levels will be rejected.

## Goal 2: ROI-Based Tier Requirements

### Requirements
- Only reward miners with positive ROI over 15+ days in tier 3 and up
- Demote miners with negative ROI to lower tiers
- Maintain system stability during transitions

### Files to Modify
- `bettensor/validator/utils/scoring/scoring.py`
  - Update `_meets_tier_requirements()` to check ROI
  - Modify `manage_tiers()` for ROI-based demotion
  - Adjust `_promote_and_swap()` to handle new criteria
  - Create tracking mechanism for 15-day ROI windows

### Technical Design
```python
def _meets_tier_requirements(self, miner, tier):
    # Existing code...
    
    # For tier 3+, add ROI requirement
    if tier >= 3:
        roi_window = 15  # days
        avg_roi = self._calculate_window_roi(miner, roi_window)
        if avg_roi <= 0:
            return False
    
    return meets_wager and meets_history
```

### Risk Assessment
- **User Impact**: Miners may face unexpected demotion
- **Network Stability**: Potential for significant tier reorganization

### Mitigation Strategy
- Implement warning system before demotion
- Gradual rollout with transition period
- Simulate impact on current miners before deployment

### Clarification Responses

#### ROI Threshold
A 0% ROI threshold is sufficient for tier 3+ miners. Any non-negative ROI value will satisfy the requirement.

#### Tiered Requirements
We will not implement additional ROI requirements for different tiers beyond the base 0% threshold.

#### Insufficient History
The existing minimum wager requirements should prevent miners from reaching tier 3 without sufficient history, so this should not be a concern.

#### Grace Period
No grace period or warning system will be implemented for ROI-based demotions.

#### Calculation Frequency
ROI will be calculated during the main scoring run, maintaining the existing timing structure.

#### ROI Calculation Method
We will use the current ROI calculation method without additional considerations for volatility.

## Goal 3: Vesting Rewards System

### Requirements
- Track miners who hold rather than immediately sell rewards
- Implement multiplier tiers based on:
  - Amount held (>30%, >50%, >70%)
  - Time held (2 weeks, 1 month, 2 months)
- Integrate with metagraph to track hotkey/coldkey associations
- Monitor daily emissions and token movement

### Files to Modify
- `bettensor/validator/utils/scoring/scoring.py`
  - Add reward multiplier to score calculations
- Database schema for tracking reward holding data

### New Files to Create
- `bettensor/validator/utils/vesting/tracker.py`
  - Implementation for tracking stake and rewards
- `bettensor/validator/utils/vesting/multipliers.py`
  - Multiplier calculation logic

### Technical Design
```python
class VestingTracker:
    def __init__(self, metagraph):
        self.metagraph = metagraph
        # Initialize tracking data structures
        
    def update_emissions(self, uid, amount):
        """Record new emissions for a miner"""
        
    def track_transfers(self, blockchain_events):
        """Process token transfers"""
        
    def calculate_holding_percentage(self, uid):
        """Calculate what percentage of rewards are being held"""
        
    def calculate_holding_time(self, uid):
        """Calculate how long rewards have been held"""
        
    def get_multiplier(self, uid):
        """Calculate the final multiplier based on amount and time"""
```

### Risk Assessment
- **Technical Complexity**: Requires blockchain monitoring
- **Data Reliability**: Depends on accurate transaction tracking
- **Privacy Concerns**: Monitors wallet activity

### Mitigation Strategy
- Use only on-chain data with transparent methodology
- Consider opt-in mechanisms for privacy concerns
- Implement robust security review

### Clarification Responses

#### Token Tracking Mechanism
We will track how much a miner has staked on the subnet via their coldkey. Miners earn rewards as staked Alpha and must unstake those rewards to sell them. Movements to other validators within the subnet will not be considered sells. Implementation details for metagraph access will be determined during development using the Bittensor Core Library.

#### Multiplier Values
The maximum bonus multiplier will be 1.5X (150%). The tier structure should be designed accordingly, with graduated increases based on both amount held and time held criteria.

#### Metagraph Integration
We will use the Bittensor Core Library for metagraph access to track stake and rewards. Specific API functions will be determined during implementation.

#### Multiplier Application
Multipliers will only be applied going forward after implementation, not retroactively. They will affect the final score before normalization and weight submission.

#### Edge Case Handling
- Token movements will be considered "sells" unless they are to validators on the subnet
- We will track emissions for each hotkey individually
- For coldkeys with multiple hotkeys, the total stake will be divided evenly across keys (or by individual key if feasible)
- We will prioritize simplicity in implementation while maintaining accurate tracking

## Goal 4: Miner CLI Support for New Bet Types

### Requirements
- Update CLI to support submission of new bet types
- Provide clear documentation and examples
- Ensure backward compatibility

### Files to Modify
- CLI command handlers
- Prediction submission modules

### New Files to Create
- CLI modules for each bet type
- Documentation and example files

### Technical Design
```python
# Example CLI command structure
def submit_point_spread_bet(event_id, team, spread, odds, stake):
    """Submit a point spread bet"""
    # Implementation
    
def submit_over_under_bet(event_id, total, is_over, odds, stake):
    """Submit an over/under bet"""
    # Implementation
```

### Risk Assessment
- **User Experience**: More complex bet types may confuse users
- **Error Rates**: Potential increase in invalid submissions

### Mitigation Strategy
- Comprehensive documentation
- Robust input validation with clear error messages
- Interactive examples

## Goal 5: Organizational Improvements and Optimizations

### Requirements
- Identify and eliminate performance bottlenecks
- Improve code organization and maintainability
- Enhance error handling and logging

### Files to Modify
- `bettensor/validator/utils/scoring/scoring.py`
- Database interaction code
- Core processing loops

### Technical Design
- Profile current system to identify bottlenecks
- Optimize database queries:
  - Add appropriate indices
  - Batch operations where possible
  - Use prepared statements
- Refactor large methods into smaller components
- Implement caching for expensive operations
- Consider parallelization for independent tasks

### Risk Assessment
- **Regression Risk**: Optimizations may introduce bugs
- **Complexity**: Could make the codebase harder to understand

### Mitigation Strategy
- Comprehensive test coverage
- Benchmark performance before/after
- Clear documentation of optimization strategies

## Goal 6: Entropy System Overhaul

### Requirements
- Prevent copy trading and "incentive farming"
- Detect miners running multiple hotkeys with the same strategy
- Encourage diverse prediction strategies

### Files to Modify
- `bettensor/validator/utils/scoring/scoring.py`
  - Update entropy calculations
- Database schema for strategy fingerprinting

### New Files to Create
- `bettensor/validator/utils/entropy/strategy_detection.py`
  - Algorithms to identify similar strategies
- `bettensor/validator/utils/entropy/clustering.py`
  - Group miners with similar prediction patterns

### Technical Design
```python
class StrategyDetector:
    def __init__(self):
        # Initialize detection algorithms
        
    def generate_fingerprint(self, predictions):
        """Generate a fingerprint for a set of predictions"""
        
    def calculate_similarity(self, fingerprint1, fingerprint2):
        """Calculate similarity between two fingerprints"""
        
    def cluster_strategies(self, all_fingerprints):
        """Group similar strategies together"""
        
    def detect_copying(self, clusters):
        """Identify potential copy trading"""
```

### Risk Assessment
- **False Positives**: May penalize legitimately similar strategies
- **Computational Cost**: Advanced detection may be resource-intensive
- **Gaming**: Sophisticated miners may find ways to evade detection

### Mitigation Strategy
- Implement appeals process for miners
- Use tiered detection approach (simple checks first)
- Regular reviews of detection effectiveness

## Implementation Roadmap

### Revised Priorities
1. Vesting Rewards System (Goal 3) - Immediate Focus
2. Other goals will be addressed after the vesting system is implemented

### Phase 1: Vesting System Foundation (Weeks 1-2)
- Design database schema for tracking rewards and holdings
- Create metagraph integration for tracking stake/rewards
- Develop core VestingTracker functionality

### Phase 2: Vesting System Implementation (Weeks 3-4)
- Implement multiplier calculation logic
- Integrate with scoring system
- Test with real blockchain data

### Phase 3: Vesting System Refinement (Weeks 5-6)
- Performance optimization
- Edge case handling
- Documentation and user guides

### Future Phases (Timeline TBD)
- Implementation of remaining goals (1, 2, 4, 5, 6)
- Integration testing across all systems
- Full system rollout

## Open Questions

### Bet Types
- Do we need historical data migration for existing bets?
- What specific formats will the point spread and totals use?

### ROI Requirements
- Should there be different thresholds for different tiers?
- How should we handle miners with insufficient history?

### Vesting Rewards
- How will we track token sales across different exchanges?
- Should multipliers be applied daily or at reward distribution time?

### Entropy System
- What specific similarity metrics should we prioritize?
- Is there historical data on known copying behavior we can use?

## Next Steps
- Finalize requirements for each goal
- Prioritize implementation order
- Establish testing methodology
- Create detailed technical specifications for each component 