# Bettensor Secure WebSocket Implementation

This document provides an overview of the secure WebSocket implementation in Bettensor, which enables real-time communication between miners and validators with cryptographic verification using Bittensor's wallet system.

## Overview

The WebSocket implementation in Bettensor provides a real-time, bidirectional communication channel between miners and validators, enhancing the system's responsiveness and efficiency compared to the traditional HTTP-based communication.

Key features:
- Cryptographic message signing and verification using Bittensor wallets
- Automatic connection management with reconnection capabilities
- Heartbeat mechanism to maintain connection health
- Message queuing with priority handling
- Fallback to HTTP when WebSocket connections fail

## Architecture

The implementation consists of two main components:

1. **Validator WebSocket Manager**: A server-side component that manages WebSocket connections from miners, verifies message signatures, and routes messages to appropriate handlers.

2. **Miner WebSocket Manager**: A client-side component that maintains connections to validators, signs outgoing messages, and handles incoming messages.

### Security Features

All WebSocket messages are secured using the following mechanisms:

- **Message Signing**: Every message is signed with the sender's private key.
- **Signature Verification**: Recipients verify the signature before processing messages.
- **Nonce Protection**: Each message includes a unique nonce to prevent replay attacks.
- **Timestamp Validation**: Messages with timestamps too far from the current time are rejected.
- **Sender Verification**: The sender field is validated against the authenticated connection.

## Usage

### Enabling WebSocket in Validators

To enable WebSocket support in validators, use the following command-line arguments:

```bash
python -m bettensor.validator.main \
  --websocket_enabled \
  --websocket_heartbeat_interval 60 \
  # Other validator arguments...
```

### Enabling WebSocket in Miners

To enable WebSocket support in miners, use the following command-line arguments:

```bash
python -m bettensor.miner.main \
  --websocket_enabled \
  --websocket_heartbeat_interval 60 \
  --websocket_reconnect_base_interval 1 \
  --websocket_max_reconnect_interval 60 \
  # Other miner arguments...
```

### Message Types

The WebSocket implementation supports the following message types:

1. **Prediction Messages**: Sent from miners to validators with prediction data.
2. **Confirmation Messages**: Sent from validators to miners to confirm received predictions.
3. **Game Update Messages**: Broadcast from validators to miners with the latest game data.
4. **Heartbeat Messages**: Used to keep connections alive and check health.

### WebSocket vs HTTP Fallback

The system is designed to use WebSockets as the primary communication method but falls back to HTTP when necessary:

- Miners attempt to send predictions via WebSocket first, falling back to HTTP if WebSocket is unavailable.
- Validators send confirmations via WebSocket if the miner is connected, falling back to HTTP otherwise.
- Game updates are broadcast to all connected miners via WebSocket only.

## Implementing Custom Handlers

### Validator-side Handlers

To implement a custom handler for predictions on the validator side:

```python
from bettensor.validator.io.prediction_handler import PredictionHandler

# Create prediction handler
prediction_handler = PredictionHandler(validator)

# Register with WebSocket manager
prediction_handler.register_with_websocket_manager(websocket_manager)
```

### Miner-side Handlers

To implement custom handlers for confirmations and game updates on the miner side:

```python
# Register confirmation handler
websocket_manager.register_confirmation_handler(handle_confirmation)

# Register game update handler
websocket_manager.register_game_update_handler(handle_game_update)

async def handle_confirmation(validator_hotkey, confirmation_data):
    # Process confirmation
    prediction_id = confirmation_data.get("prediction_id")
    status = confirmation_data.get("status")
    # ...

async def handle_game_update(validator_hotkey, game_data):
    # Process game update
    games = game_data.get("games", [])
    # ...
```

## Demo Script

A demonstration script is provided to showcase the secure WebSocket functionality:

```bash
# Run as validator
python examples/websocket_signed_demo.py --mode validator --port 8080

# Run as miner (in a separate terminal)
python examples/websocket_signed_demo.py --mode miner --endpoint 127.0.0.1:8080
```

## Benefits Over HTTP

The WebSocket implementation offers several advantages over the traditional HTTP-based approach:

1. **Lower Latency**: Real-time communication without the overhead of establishing new connections.
2. **Reduced Bandwidth**: Less header overhead and connection establishment.
3. **Bidirectional Communication**: Validators can push updates to miners without polling.
4. **Connection Awareness**: System knows when miners are connected and can avoid unnecessary HTTP requests.
5. **Prioritized Messages**: Critical messages can be prioritized over less important ones.

## Best Practices

1. **Always Enable WSS in Production**: Use WSS (WebSocket Secure) in production environments for encrypted communication.
2. **Monitor Connection Health**: Implement health checks and alerts for WebSocket connection issues.
3. **Tune Heartbeat Intervals**: Adjust heartbeat intervals based on network conditions and expected traffic.
4. **Implement Rate Limiting**: Protect against DoS attacks by implementing rate limiting for connections and messages.
5. **Handle Graceful Degradation**: Ensure your application can gracefully fall back to HTTP when WebSocket connections fail.

## Troubleshooting

Common issues and solutions:

1. **Connection Failures**: Check network connectivity, firewall settings, and server logs.
2. **Authentication Errors**: Verify wallet configuration and signature verification settings.
3. **Message Processing Errors**: Check for proper message format and handler implementation.
4. **Performance Issues**: Monitor connection counts, message queue sizes, and system resources.

For more detailed troubleshooting, see the [WebSocket Troubleshooting Guide](websocket_troubleshooting.md). 