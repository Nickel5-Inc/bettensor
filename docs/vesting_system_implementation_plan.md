# Vesting Rewards System: Detailed Implementation Plan

## Overview
This document provides a detailed implementation plan for the vesting rewards system in Bettensor. The system will reward miners who hold their earned tokens rather than immediately selling them, encouraging long-term participation in the network.

## Core Requirements
- Track how much miners have staked on the subnet via their coldkeys
- Monitor when miners unstake their rewards (considered "selling")
- Implement score multipliers based on:
  - Amount held (>30%, >50%, >70% of earned rewards)
  - Time held (2 weeks, 1 month, 2 months)
- Maximum multiplier of 1.5X (150%)
- Apply multipliers to final scores before normalization and weight submission

## Technical Components

### 1. Stake Tracking
- Use Bittensor Core Library to access metagraph data
- Track stake amounts for each coldkey
- Associate hotkeys with their corresponding coldkeys
- Maintain historical record of stake changes

### 2. Emissions Monitoring
- Record daily emissions for each miner
- Calculate cumulative rewards over time
- Store emissions history in database

### 3. Holding Analysis
- Calculate percentage of rewards being held
- Track holding duration
- Update metrics daily during scoring runs

### 4. Multiplier System
- Define multiplier tiers:
  - Amount held tiers: >30%, >50%, >70%
  - Time held tiers: 2 weeks, 1 month, 2 months
- Implement graduated multiplier calculation
- Maximum multiplier: 1.5X

### 5. Integration with Scoring
- Apply multipliers to final scores
- Update weight submission process
- Ensure proper normalization after applying multipliers

## Implementation Phases

### Phase 1: Database Design and Preparation (Week 1)
- Design database schema for tracking:
  - Daily emissions by miner
  - Stake amounts over time
  - Hotkey/coldkey associations
  - Holding percentages and durations
- Implement database migrations
- Create data storage and retrieval functions

#### Key Deliverables:
- Database schema design document
- Migration scripts
- Basic data access layer

### Phase 2: Metagraph Integration (Week 2)
- Research Bittensor Core Library API functions for stake tracking
- Implement metagraph data polling
- Create hotkey/coldkey association tracker
- Build stake change detection system
- Develop emission record keeping

#### Key Deliverables:
- Metagraph integration module
- Stake tracking functionality
- Emission recording system

### Phase 3: Multiplier Logic (Week 3)
- Implement holding percentage calculation
- Develop holding duration tracking
- Create multiplier tier system
- Build multiplier calculation logic
- Design score application mechanism

#### Key Deliverables:
- Multiplier calculation module
- Holding metrics implementation
- Integration with existing score calculation

### Phase 4: Integration and Testing (Week 4)
- Integrate with scoring system
- Implement weight adjustment mechanism
- Develop monitoring dashboard for operators
- Test with historical data
- Create simulation tools

#### Key Deliverables:
- Fully integrated system
- Test suite
- Monitoring tools

### Phase 5: Refinement and Deployment (Weeks 5-6)
- Performance optimization
- Edge case handling
- Documentation
- User guides
- Deployment preparation

#### Key Deliverables:
- Optimized system
- Comprehensive documentation
- Deployment package

## Testing Strategy
- Unit tests for all components
- Integration tests for system interaction
- Simulation testing with historical data
- Performance benchmarking
- Security review

## Risk Assessment and Mitigation

### Data Accuracy Risks
- **Risk**: Inaccurate stake data from metagraph
- **Mitigation**: Multiple data sources, validation checks, error detection

### Performance Risks
- **Risk**: Processing overhead affecting scoring performance
- **Mitigation**: Optimization, caching, asynchronous processing

### Security Risks
- **Risk**: Gaming the system through transaction manipulation
- **Mitigation**: Multiple verification steps, anomaly detection

### Edge Case Risks
- **Risk**: Unusual staking patterns causing incorrect multipliers
- **Mitigation**: Extensive testing with unusual patterns, manual override capability

## Key Integration Points
- Metagraph API for stake data
- Scoring system for multiplier application
- Database for historical tracking
- Weight submission system for final reward calculation

## Open Questions
1. What specific Bittensor Core Library functions should we use for stake tracking?
2. How frequently should we poll the blockchain for stake changes?
3. How should we handle network disruptions or missing data?
4. What are the exact multiplier values for each tier combination?

## Next Steps
1. Finalize database schema design
2. Research Bittensor Core Library API specifics
3. Create initial stake tracking implementation
4. Begin development of emission recording system 