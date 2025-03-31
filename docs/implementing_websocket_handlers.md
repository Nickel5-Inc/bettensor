# Implementing WebSocket Handlers in Bettensor

This document provides detailed instructions for implementing custom handlers for WebSocket events in Bettensor.

## Overview

The WebSocket-based communication system in Bettensor uses an event-driven architecture where different types of messages trigger corresponding handlers. This guide explains how to implement custom handlers for both miners and validators.

## Prerequisites

Before implementing WebSocket handlers, ensure:

1. You have the WebSocket manager set up and configured.
2. You understand the message structure and protocols.
3. You have access to the validator's or miner's context (depending on which side you're implementing).

## Message Types

The following message types can be handled:

1. **PredictionMessage**: Sent by miners with prediction data
2. **ConfirmationMessage**: Sent by validators to confirm prediction receipt
3. **GameUpdateMessage**: Sent by validators with updated game data
4. **HeartbeatMessage**: Used for keeping connections alive

## Implementing Validator-Side Handlers

### Prediction Handler

The prediction handler processes incoming predictions from miners. Here's how to implement it:

```python
async def handle_prediction(websocket, message):
    """
    Handle incoming prediction messages from miners.
    
    Args:
        websocket: The active WebSocket connection
        message: The PredictionMessage containing prediction data
    """
    try:
        # 1. Extract and validate prediction data
        prediction_data = message.data
        miner_hotkey = message.sender
        
        # 2. Process the prediction (similar to HTTP endpoint logic)
        valid, reason = validate_prediction(prediction_data)
        if not valid:
            # Send error response
            await websocket.send_json({
                "type": "confirmation",
                "status": "error",
                "message": reason
            })
            return
        
        # 3. Store the prediction
        store_prediction(prediction_data)
        
        # 4. Send confirmation
        await websocket.send_json({
            "type": "confirmation",
            "status": "success",
            "prediction_id": prediction_data.get("id")
        })
        
        # 5. Log the successful prediction
        logging.info(f"Received prediction via WebSocket from {miner_hotkey}")
        
    except Exception as e:
        logging.error(f"Error handling prediction: {str(e)}")
        # Send error response
        await websocket.send_json({
            "type": "confirmation",
            "status": "error",
            "message": "Internal server error"
        })
```

### Register the Prediction Handler

Register your handler with the WebSocket manager:

```python
# In your validator setup code
websocket_manager = ValidatorWebSocketManager(...)
websocket_manager.register_handler("prediction", handle_prediction)
```

## Implementing Miner-Side Handlers

### Confirmation Handler

The confirmation handler processes confirmation messages from validators:

```python
async def handle_confirmation(websocket, message):
    """
    Handle confirmation messages from validators.
    
    Args:
        websocket: The active WebSocket connection
        message: The ConfirmationMessage containing confirmation data
    """
    try:
        # 1. Extract confirmation data
        confirmation_data = message.data
        validator_hotkey = message.sender
        prediction_id = confirmation_data.get("prediction_id")
        status = confirmation_data.get("status")
        
        # 2. Update local prediction status
        if status == "success":
            update_prediction_status(prediction_id, "confirmed")
            logging.info(f"Prediction {prediction_id} confirmed by validator {validator_hotkey}")
        else:
            error_message = confirmation_data.get("message", "Unknown error")
            update_prediction_status(prediction_id, "rejected", error_message)
            logging.warning(f"Prediction {prediction_id} rejected by validator {validator_hotkey}: {error_message}")
        
    except Exception as e:
        logging.error(f"Error handling confirmation: {str(e)}")
```

### Game Update Handler

The game update handler processes game updates from validators:

```python
async def handle_game_updates(websocket, message):
    """
    Handle game update messages from validators.
    
    Args:
        websocket: The active WebSocket connection
        message: The GameUpdateMessage containing game data
    """
    try:
        # 1. Extract game data
        games = message.data.get("games", [])
        validator_hotkey = message.sender
        
        # 2. Update local game database
        for game in games:
            update_game(game)
        
        # 3. Trigger predictions if needed
        if games:
            trigger_predictions_for_updated_games(games)
        
        # 4. Log the update
        logging.info(f"Received updates for {len(games)} games from validator {validator_hotkey}")
        
    except Exception as e:
        logging.error(f"Error handling game updates: {str(e)}")
```

### Register Miner-Side Handlers

Register your handlers with the WebSocket client:

```python
# In your miner setup code
websocket_manager = MinerWebSocketManager(...)
websocket_manager.register_handler("confirmation", handle_confirmation)
websocket_manager.register_handler("game_update", handle_game_updates)
```

## Custom Message Types

You can implement custom message types by extending the base WebSocketMessage class:

```python
class CustomMessage(WebSocketMessage):
    """Custom message type for specific needs."""
    
    MESSAGE_TYPE = "custom"
    
    def __init__(self, sender, data):
        super().__init__(sender, data)
        # Additional custom fields
        self.priority = data.get("priority", "normal")
```

Then implement a handler for your custom message type:

```python
async def handle_custom_message(websocket, message):
    """Handle custom message type."""
    # Custom implementation
    priority = message.priority
    # Process accordingly
```

And register it:

```python
websocket_manager.register_handler("custom", handle_custom_message)
```

## Error Handling Best Practices

When implementing WebSocket handlers:

1. **Always use try/except blocks** to prevent crashes from bad messages
2. **Validate message data** before processing
3. **Log errors and successes** for debugging
4. **Send appropriate responses** when handling errors
5. **Consider message idempotency** (handling duplicates gracefully)

## Performance Considerations

1. **Keep handlers lightweight**: Avoid blocking operations
2. **Use async/await** for I/O-bound operations
3. **Offload heavy processing** to background tasks
4. **Limit message sizes** to prevent abuse
5. **Implement rate limiting** for frequent message types

## Testing Handlers

Test your handlers with:

1. **Unit tests**: Test handler logic independently
2. **Integration tests**: Test with mock WebSocket connections
3. **Load tests**: Verify handling of many simultaneous messages

Example unit test:

```python
async def test_confirmation_handler():
    # Create mock websocket
    mock_websocket = MockWebSocket()
    
    # Create mock message
    message = ConfirmationMessage(
        sender="validator_hotkey",
        data={
            "prediction_id": "test_id",
            "status": "success"
        }
    )
    
    # Call handler
    await handle_confirmation(mock_websocket, message)
    
    # Verify prediction status was updated
    prediction = get_prediction("test_id")
    assert prediction.status == "confirmed"
```

## Debugging Tips

When handlers aren't working as expected:

1. Enable debug-level logging for WebSocket operations
2. Use a WebSocket client tool to send test messages
3. Implement message echo handlers for connection testing
4. Check for message format inconsistencies
5. Verify the handler is correctly registered

## Conclusion

Implementing WebSocket handlers in Bettensor enables real-time, event-driven communication between miners and validators. Following these best practices helps ensure robust, maintainable, and efficient handling of WebSocket events. 