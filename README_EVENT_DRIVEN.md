# Bettensor Event-Driven Architecture

This document provides an overview of the event-driven architecture design for the Bettensor project. The architecture has been implemented for both the validator and miner components, aiming to improve performance, reliability, and maintainability.

## Overview

The event-driven architecture is a software design pattern that facilitates communication between components through events rather than direct method calls. This approach provides several benefits:

1. **Loose coupling** - Components interact through events, reducing direct dependencies
2. **Improved testability** - Each component can be tested in isolation
3. **Better maintainability** - Components can be updated or replaced without affecting the entire system
4. **Scalability** - The system can handle more load by adding more processing capacity
5. **Fault tolerance** - Failures in one component do not necessarily bring down the entire system

## Core Components

### Common Components

#### EventManager
- Manages event publishing and subscription
- Facilitates communication between components
- Tracks event statistics and history

#### TaskManager
- Handles scheduling and execution of periodic tasks
- Manages task error handling and retries
- Tracks task execution statistics

### Validator Components

#### EventDrivenValidator
- Main orchestrator for the validator
- Initializes and manages all validator services
- Handles lifecycle events (start, stop)
- Schedules periodic tasks

#### PredictionService
- Receives and processes predictions from miners
- Maintains history of predictions
- Provides prediction statistics

#### ScoringService
- Evaluates miner predictions
- Calculates scores based on prediction accuracy
- Updates scoring history

#### WeightService
- Sets weights on the Bittensor network
- Determines when weights should be updated
- Tracks weight-setting history and statistics

#### CommunicationManager
- Handles WebSocket connections with miners
- Manages HTTP fallback endpoints
- Processes incoming predictions

### Miner Components

#### EventDrivenMiner
- Main orchestrator for the miner
- Initializes and manages all miner services
- Handles lifecycle events (start, stop)
- Schedules periodic tasks

#### PredictionService
- Generates predictions for active games
- Tracks submitted predictions and their statuses
- Manages prediction models

#### GameService
- Retrieves and caches game data
- Tracks active games for prediction generation
- Updates game statuses

#### CommunicationService
- Establishes connections to validators
- Submits predictions via WebSocket or HTTP
- Handles reconnection and failover logic

#### DatabaseManager
- Manages persistent storage for the miner
- Provides methods for storing and retrieving data
- Handles database migrations and backups

## Event Flow

### Validator Event Flow

1. **Miner Prediction Submission**:
   - Prediction received via WebSocket or HTTP
   - CommunicationManager publishes a "prediction_received" event
   - PredictionService handles the event and stores the prediction

2. **Periodic Scoring**:
   - TaskManager triggers a scoring task
   - ScoringService evaluates predictions and calculates scores
   - ScoringService publishes a "scoring_complete" event

3. **Weight Setting**:
   - WeightService subscribes to "scoring_complete" events
   - When scores change significantly, WeightService sets new weights on the network
   - WeightService publishes a "weights_set" event

### Miner Event Flow

1. **Game Data Updates**:
   - TaskManager triggers a game data update task
   - GameService retrieves latest game data
   - GameService publishes a "games_updated" event

2. **Prediction Generation**:
   - PredictionService subscribes to "games_updated" events
   - PredictionService generates predictions for new games
   - PredictionService publishes a "predictions_generated" event

3. **Prediction Submission**:
   - CommunicationService subscribes to "predictions_generated" events
   - CommunicationService submits predictions to validators
   - CommunicationService publishes "prediction_submitted" events

## Configuration Options

Both the EventDrivenValidator and EventDrivenMiner include comprehensive configuration options that can be set via command-line arguments, including:

- Database settings
- WebSocket parameters
- Task scheduling intervals
- Logging levels
- Network configurations

## Implementation Details

### Error Handling

The architecture includes robust error handling at multiple levels:

1. **Component-level error handling** - Each component catches and logs errors
2. **Task retries** - TaskManager retries failed tasks with exponential backoff
3. **Graceful degradation** - System can operate with reduced functionality if some components fail

### Statistics and Monitoring

All components provide detailed statistics through their `get_stats()` methods, including:

- Event counts and processing times
- Task success and failure rates
- Connection states
- Prediction accuracy metrics

### Persistence

The system uses SQLite for persistence with the DatabaseManager, providing:

- Game data storage
- Prediction history
- Performance statistics
- Automatic backups

## Migration Guide

To adopt the event-driven architecture:

1. Set the appropriate environment variable:
   - For validator: `USE_EVENT_DRIVEN=True`
   - For miner: `USE_EVENT_DRIVEN=True`

2. Run with standard command, the system will automatically use the event-driven architecture:
   ```
   python neurons/validator.py --netuid 1 --subtensor.network <network>
   ```
   or
   ```
   python neurons/miner.py --netuid 1 --subtensor.network <network>
   ```

## Architecture Diagrams

### Validator Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                       EventDrivenValidator                               │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  ┌─────────────┐   ┌─────────────┐   ┌─────────────┐   ┌─────────────┐  │
│  │             │   │             │   │             │   │             │  │
│  │ EventManager│◄──┤TaskManager  │   │PredictionSvc│◄──┤ScoringService│  │
│  │             │   │             │   │             │   │             │  │
│  └─────┬───────┘   └─────────────┘   └──────┬──────┘   └──────┬──────┘  │
│        │                 ▲                  │                 │         │
│        │                 │                  │                 │         │
│        │                 │                  │                 │         │
│        │            ┌────┴──────┐          │                 │         │
│        │            │           │          │                 │         │
│        └──────────► │WeightService│◄─────────┘                 │         │
│                     │           │                            │         │
│                     └───────────┘                            │         │
│                          ▲                                   │         │
│                          │                                   │         │
│                          │                                   │         │
│                     ┌────┴──────┐                            │         │
│                     │           │                            │         │
│                     │  CommMgr  │◄───────────────────────────┘         │
│                     │           │                                      │
│                     └───────────┘                                      │
│                          ▲                                             │
└───────────────────────────────────────────────────────────────────────┘
                           │
                           │
                           ▼
                   ┌───────────────┐
                   │     Miners    │
                   └───────────────┘
```

### Miner Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                          EventDrivenMiner                                │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  ┌─────────────┐   ┌─────────────┐   ┌─────────────┐   ┌─────────────┐  │
│  │             │   │             │   │             │   │             │  │
│  │ EventManager│◄──┤TaskManager  │   │ GameService │◄──┤PredictionSvc│  │
│  │             │   │             │   │             │   │             │  │
│  └─────┬───────┘   └─────────────┘   └──────┬──────┘   └──────┬──────┘  │
│        │                 ▲                  │                 │         │
│        │                 │                  │                 │         │
│        │                 │                  │                 │         │
│        │            ┌────┴──────┐          │                 │         │
│        │            │           │          │                 │         │
│        └──────────► │  CommSvc  │◄─────────┴─────────────────┘         │
│                     │           │                                      │
│                     └───────────┘                                      │
│                          │                                             │
│                          │                                             │
│                          │                                             │
│                     ┌────┴──────┐                                      │
│                     │           │                                      │
│                     │   DB Mgr  │                                      │
│                     │           │                                      │
│                     └───────────┘                                      │
└───────────────────────────────────────────────────────────────────────┘
                           │
                           │
                           ▼
                   ┌───────────────┐
                   │   Validators  │
                   └───────────────┘
```

## Conclusion

The event-driven architecture significantly improves the Bettensor system in several ways:

1. **Performance** - Asynchronous processing improves throughput
2. **Reliability** - Better error handling and fault tolerance
3. **Maintainability** - Cleaner separation of concerns
4. **Extensibility** - Easier to add new features

This architecture establishes a foundation for future enhancements and features while maintaining backward compatibility with the existing codebase. 