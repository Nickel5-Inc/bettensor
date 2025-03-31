# WebSocket-Based Communication in Bettensor

This document provides details about the WebSocket-based communication system implemented in Bettensor for real-time predictions and game updates.

## Overview

The Bettensor network has been enhanced with a real-time communication system that leverages persistent WebSocket connections for improved efficiency and responsiveness. This system provides the following benefits:

- **Real-time Prediction Submission**: Miners can immediately push predictions to validators without waiting for a request.
- **Instant Confirmations**: Validators send immediate confirmations back to miners.
- **Game State Updates**: Validators broadcast game updates to connected miners.
- **Reduced Network Overhead**: Persistent connections reduce the overhead of establishing new connections for each communication.
- **Improved Responsiveness**: WebSocket's bidirectional nature enables push notifications rather than poll-based updates.

The system maintains backward compatibility with the existing HTTP-based communication model, which serves as a fallback mechanism.

## Architecture

The WebSocket-based communication architecture consists of the following components:

1. **Protocol Classes**: Standardized message formats for different types of communications.
2. **Validator WebSocket Server**: Accepts and manages connections from miners.
3. **Miner WebSocket Client**: Establishes and maintains connections to validators.
4. **Event Handlers**: Process different types of messages.
5. **Fallback Mechanism**: Uses the existing HTTP-based communication as a fallback.

## Configuration

### Validator Configuration

WebSocket functionality can be configured using the following command-line arguments:

```bash
# Enable/disable WebSocket (default: True)
--websocket.enabled=True

# Heartbeat interval in seconds (default: 60)
--websocket.heartbeat_interval=60

# Connection timeout in seconds (default: 300)
--websocket.connection_timeout=300
```

### Miner Configuration

```bash
# Enable/disable WebSocket (default: True)
--websocket_enabled=True

# Heartbeat interval in seconds (default: 60)
--websocket_heartbeat_interval=60

# Base reconnect interval in seconds (default: 5)
--websocket_reconnect_base_interval=5

# Maximum reconnect interval in seconds (default: 300)
--websocket_max_reconnect_interval=300
```

## How It Works

### Connection Establishment

1. The validator starts a WebSocket server as part of its Axon server.
2. Miners establish WebSocket connections to validators.
3. The connection process includes:
   - Initial handshake with authentication using the miner's hotkey
   - Validation of the miner's registration in the metagraph
   - Verification that the miner is not blacklisted

### Prediction Submission

1. When a miner generates a new prediction, it:
   - Saves the prediction locally
   - Immediately pushes the prediction to connected validators via WebSocket
   - Falls back to HTTP if WebSocket is not available

2. The validator:
   - Receives the prediction in real-time
   - Validates it using the same logic as the HTTP-based approach
   - Sends an immediate confirmation back to the miner via the same WebSocket connection

### Game Updates

1. When validators receive new game data:
   - They broadcast updates to all connected miners
   - Miners process these updates in real-time

2. Miners can immediately act on updated game information without waiting for periodic polling.

### Heartbeats and Connection Management

1. Both miners and validators send periodic heartbeats to keep connections alive.
2. Stale connections are automatically cleaned up.
3. Miners implement exponential backoff for reconnection attempts to avoid overwhelming validators.

## Implementation Details

### Message Types

All WebSocket messages are standardized and extend from the base `WebSocketMessage` class:

- **PredictionMessage**: Sent by miners to validators with prediction data
- **ConfirmationMessage**: Sent by validators to miners to confirm prediction receipt
- **GameUpdateMessage**: Sent by validators to miners with updated game data
- **HeartbeatMessage**: Sent by both parties to keep connections alive

### Security

The WebSocket communication is secured by:

1. **Authentication**: Verifying miner hotkeys during connection establishment
2. **Signature Verification**: Using the same Bittensor security mechanisms as the HTTP protocol
3. **Miner Validation**: Checking if miners are registered and not blacklisted

## Fallback Mechanism

The system is designed to gracefully fall back to the HTTP-based communication when:

1. WebSocket connections cannot be established
2. WebSocket connections are unexpectedly terminated
3. WebSocket functionality is disabled via configuration

## Monitoring and Debugging

### Validator Endpoints

A health check endpoint is available at `/ws/health` to monitor WebSocket server status. It returns:

```json
{
  "status": "online",
  "connections": 10,
  "active_connections": 8
}
```

### Logging

Both miners and validators include detailed logging for WebSocket operations. Look for log entries with:

- "WebSocket connection established with miner..."
- "Sent confirmation for prediction ... via WebSocket"
- "Pushed prediction ... via WebSocket"
- "Broadcasting game updates to connected miners"

## Best Practices

1. **Enable WebSocket for Production**: The WebSocket-based communication is more efficient and responsive.
2. **Monitor Connection Status**: Regularly check that WebSocket connections are healthy.
3. **Configure Timeouts Appropriately**: Adjust heartbeat and connection timeouts based on your network conditions.

## Limitations and Future Improvements

1. **No Encryption**: The current implementation does not include WebSocket-specific encryption. It relies on the security mechanisms provided by Bittensor.
2. **Connection Scaling**: For networks with many miners per validator, additional optimization may be needed.
3. **Future Plans**: 
   - Secure WebSocket (WSS) support
   - Compression for large game update messages
   - Connection pooling and load balancing

## Troubleshooting

### Common Issues

1. **Connection Failures**: 
   - Check firewall settings
   - Verify validator IP and port accessibility
   - Ensure miner is properly registered

2. **Stale Connections**:
   - Increase heartbeat frequency
   - Check for network interruptions
   - Monitor system resource usage

3. **Message Processing Errors**:
   - Check logs for detailed error messages
   - Verify message format and content

## Conclusion

The WebSocket-based communication system significantly improves the real-time capabilities of the Bettensor network. By enabling miners to push predictions immediately and receive instant confirmations, it enhances the user experience and system efficiency while maintaining backward compatibility with the existing HTTP-based approach. 