# Migration Guide: HTTP to WebSocket Communication

This document provides a comprehensive guide for transitioning from HTTP-based to WebSocket-based communication in the Bettensor system.

## Why Migrate?

Transitioning from HTTP to WebSocket offers several significant advantages:

1. **Real-time Updates**: WebSocket enables push-based updates, eliminating polling overhead.
2. **Reduced Latency**: Persistent connections remove handshake overhead for each message.
3. **Lower Bandwidth Usage**: WebSocket has a lighter protocol overhead compared to HTTP.
4. **Bidirectional Communication**: Enables true two-way communication between miners and validators.
5. **Enhanced User Experience**: Faster response times and more timely updates.

## Migration Strategy

The Bettensor WebSocket implementation follows a gradual migration strategy:

1. **Dual Protocol Support**: Both HTTP and WebSocket protocols are supported simultaneously.
2. **WebSocket First**: The system attempts WebSocket communication first, with HTTP as fallback.
3. **Configuration Options**: WebSocket can be enabled/disabled and fine-tuned via configuration.
4. **Graceful Degradation**: Systems transparently fall back to HTTP when WebSocket isn't available.

## Step-by-Step Migration Guide

### 1. Update Dependencies

Ensure your environment has the updated dependencies:

```bash
pip install --upgrade bettensor
# Or if installing from source
pip install -e .
```

### 2. Configure WebSocket for Validators

Update your validator configuration to enable WebSocket:

**Command Line:**
```bash
python -m bettensor.validator.main \
  --websocket.enabled=True \
  --websocket.heartbeat_interval=60 \
  --websocket.connection_timeout=300
```

**Configuration File:**
```yaml
websocket:
  enabled: true
  heartbeat_interval: 60
  connection_timeout: 300
```

### 3. Configure WebSocket for Miners

Update your miner configuration to enable WebSocket:

**Command Line:**
```bash
python -m bettensor.miner.main \
  --websocket_enabled=True \
  --websocket_heartbeat_interval=60 \
  --websocket_reconnect_base_interval=5 \
  --websocket_max_reconnect_interval=300
```

**Configuration File:**
```yaml
websocket:
  enabled: true
  heartbeat_interval: 60
  reconnect_base_interval: 5
  max_reconnect_interval: 300
```

### 4. Update Validator Code

#### Initialize WebSocket Manager

In your validator's initialization:

```python
from bettensor.validator.websocket.manager import ValidatorWebSocketManager

class BettensorValidator:
    def __init__(self, config):
        # ... existing initialization ...
        
        # Initialize WebSocket Manager
        if config.websocket.enabled:
            self.websocket_manager = ValidatorWebSocketManager(
                metagraph=self.metagraph,
                hotkey=self.wallet.hotkey.ss58_address,
                timeout=config.websocket.connection_timeout
            )
            
            # Configure WebSocket
            self.websocket_manager.set_heartbeat_interval(config.websocket.heartbeat_interval)
            
            # Register handlers
            self.websocket_manager.register_handler("prediction", self.handle_prediction)
            
            # Start WebSocket manager
            self.websocket_manager.start()
            
            bt.logging.info(
                f"WebSocket server started with:\n"
                f"  - Heartbeat interval: {config.websocket.heartbeat_interval}s\n"
                f"  - Connection timeout: {config.websocket.connection_timeout}s"
            )
        else:
            self.websocket_manager = None
            bt.logging.info("WebSocket server disabled")
```

#### Handle Predictions via WebSocket

Implement a WebSocket prediction handler:

```python
async def handle_prediction(self, websocket, message):
    """
    Handle incoming prediction messages via WebSocket.
    """
    try:
        miner_hotkey = message.sender
        prediction_data = message.data
        
        # Validate miner
        uid = self.metagraph.hotkeys.index(miner_hotkey) if miner_hotkey in self.metagraph.hotkeys else -1
        if uid < 0:
            # Send error response
            await websocket.send_json({
                "type": "confirmation",
                "status": "error",
                "message": "Miner not found in metagraph"
            })
            return
        
        # Process prediction (similar to HTTP endpoint)
        success, result = self.process_prediction(uid, prediction_data)
        
        # Send confirmation
        confirmation_data = {
            "prediction_id": prediction_data.get("id"),
            "status": "success" if success else "error",
            "message": result if not success else "",
            "timestamp": datetime.now().isoformat()
        }
        
        await websocket.send_json({
            "type": "confirmation",
            "sender": self.wallet.hotkey.ss58_address,
            "data": confirmation_data
        })
        
        bt.logging.info(f"Processed prediction via WebSocket from {miner_hotkey}")
        
    except Exception as e:
        bt.logging.error(f"Error handling prediction via WebSocket: {str(e)}")
        try:
            await websocket.send_json({
                "type": "confirmation",
                "status": "error",
                "message": "Internal server error"
            })
        except:
            pass
```

#### Modify Game Update Logic

Update the game update logic to broadcast via WebSocket:

```python
def broadcast_game_updates(self, games):
    """
    Broadcast game updates to all connected miners via WebSocket.
    Falls back to HTTP if WebSocket is not available.
    """
    # Check if WebSocket is available
    if self.websocket_manager and self.websocket_manager.is_running():
        try:
            # Prepare game update data
            game_data = {
                "games": games,
                "timestamp": datetime.now().isoformat()
            }
            
            # Create message
            message = GameUpdateMessage(
                sender=self.wallet.hotkey.ss58_address,
                data=game_data
            )
            
            # Broadcast to all connected miners
            asyncio.create_task(self.websocket_manager.broadcast(message))
            
            bt.logging.info(f"Broadcasting game updates to {len(self.websocket_manager.clients)} miners via WebSocket")
            return True
        except Exception as e:
            bt.logging.error(f"Error broadcasting game updates via WebSocket: {str(e)}")
    
    # Fall back to HTTP (if implemented)
    bt.logging.info("WebSocket broadcasting not available, using HTTP fallback")
    return self.broadcast_game_updates_http(games)
```

#### Update Confirmation Sending

Modify confirmation sending to use WebSocket:

```python
def send_confirmation(self, uid, prediction_id, success, message=""):
    """
    Send confirmation to a miner, preferring WebSocket if available.
    """
    try:
        # Get miner hotkey
        if uid < 0 or uid >= len(self.metagraph.hotkeys):
            bt.logging.error(f"Invalid UID: {uid}")
            return False
            
        miner_hotkey = self.metagraph.hotkeys[uid]
        
        # First try WebSocket
        if self.websocket_manager and self.websocket_manager.is_running():
            client = self.websocket_manager.get_client_by_hotkey(miner_hotkey)
            if client:
                # Prepare confirmation data
                confirmation_data = {
                    "prediction_id": prediction_id,
                    "status": "success" if success else "error",
                    "message": message if not success else "",
                    "timestamp": datetime.now().isoformat()
                }
                
                # Create and send confirmation message
                message = ConfirmationMessage(
                    sender=self.wallet.hotkey.ss58_address,
                    data=confirmation_data
                )
                
                asyncio.create_task(self.websocket_manager.send_to_client(client, message))
                bt.logging.info(f"Sent confirmation for prediction {prediction_id} via WebSocket")
                return True
        
        # Fall back to HTTP
        bt.logging.info(f"WebSocket unavailable for miner {miner_hotkey}, using HTTP fallback")
        return self.send_confirmation_http(uid, prediction_id, success, message)
        
    except Exception as e:
        bt.logging.error(f"Error sending confirmation: {str(e)}")
        return False
```

### 5. Update Miner Code

#### Initialize WebSocket Client

In your miner's initialization:

```python
from bettensor.miner.websocket.manager import MinerWebSocketManager

class BettensorMiner:
    def __init__(self, config):
        # ... existing initialization ...
        
        # Initialize WebSocket Client
        if config.websocket_enabled:
            self.websocket_manager = MinerWebSocketManager(
                metagraph=self.metagraph,
                hotkey=self.wallet.hotkey.ss58_address
            )
            
            # Configure WebSocket
            self.websocket_manager.set_heartbeat_interval(config.websocket_heartbeat_interval)
            self.websocket_manager.set_reconnect_params(
                base_interval=config.websocket_reconnect_base_interval,
                max_interval=config.websocket_max_reconnect_interval
            )
            
            # Register handlers
            self.websocket_manager.register_handler("confirmation", self.handle_confirmation)
            self.websocket_manager.register_handler("game_update", self.handle_game_update)
            
            # Start WebSocket manager
            self.websocket_manager.start()
            
            bt.logging.info(
                f"WebSocket client started with:\n"
                f"  - Heartbeat interval: {config.websocket_heartbeat_interval}s\n"
                f"  - Reconnect interval: {config.websocket_reconnect_base_interval}s - {config.websocket_max_reconnect_interval}s"
            )
        else:
            self.websocket_manager = None
            bt.logging.info("WebSocket client disabled")
```

#### Implement Confirmation Handler

```python
async def handle_confirmation(self, websocket, message):
    """
    Handle confirmation messages from validators.
    """
    try:
        validator_hotkey = message.sender
        confirmation_data = message.data
        
        prediction_id = confirmation_data.get("prediction_id")
        status = confirmation_data.get("status")
        error_message = confirmation_data.get("message", "")
        
        if status == "success":
            bt.logging.info(f"Prediction {prediction_id} confirmed by validator {validator_hotkey}")
            # Update prediction status in your local database
            self.update_prediction_status(prediction_id, "confirmed")
        else:
            bt.logging.warning(f"Prediction {prediction_id} rejected by validator {validator_hotkey}: {error_message}")
            # Update prediction status with error
            self.update_prediction_status(prediction_id, "rejected", error_message)
            
    except Exception as e:
        bt.logging.error(f"Error handling confirmation: {str(e)}")
```

#### Implement Game Update Handler

```python
async def handle_game_update(self, websocket, message):
    """
    Handle game update messages from validators.
    """
    try:
        validator_hotkey = message.sender
        games = message.data.get("games", [])
        
        bt.logging.info(f"Received updates for {len(games)} games from validator {validator_hotkey}")
        
        # Update local game database
        for game in games:
            self.update_game(game)
        
        # Process updated games if needed
        if games:
            self.process_updated_games(games)
            
    except Exception as e:
        bt.logging.error(f"Error handling game updates: {str(e)}")
```

#### Update Prediction Sending

Modify your prediction sending method to use WebSocket:

```python
def send_prediction(self, validator_url, prediction):
    """
    Send a prediction to a validator, preferring WebSocket if available.
    """
    # Get validator hotkey from URL
    validator_hotkey = self.get_validator_hotkey_from_url(validator_url)
    
    # Try WebSocket first
    if self.websocket_manager and self.websocket_manager.is_running():
        # Find connection for validator
        connection = self.websocket_manager.get_connection_by_validator(validator_hotkey)
        
        if connection and connection.is_connected():
            try:
                # Create prediction message
                message = PredictionMessage(
                    sender=self.wallet.hotkey.ss58_address,
                    data=prediction
                )
                
                # Send via WebSocket
                asyncio.create_task(connection.send_message(message))
                bt.logging.info(f"Sent prediction to {validator_hotkey} via WebSocket")
                return True
            except Exception as e:
                bt.logging.error(f"Error sending prediction via WebSocket: {str(e)}")
    
    # Fall back to HTTP
    bt.logging.info(f"WebSocket unavailable for validator {validator_hotkey}, using HTTP fallback")
    return self.send_prediction_http(validator_url, prediction)
```

### 6. Testing the Migration

Follow these steps to test your WebSocket integration:

1. **Start in Fallback Mode**: Begin with WebSocket disabled to verify HTTP fallback works.
2. **Enable WebSocket**: Enable WebSocket and verify connections are established.
3. **Monitor Logs**: Check logs for WebSocket connection and message indicators.
4. **Connection Status**: Use the health endpoint to monitor connection status.
5. **Stress Test**: Send multiple predictions rapidly to test WebSocket performance.

### 7. Common Migration Issues

#### Connection Problems

**Problem**: WebSocket connections fail to establish.
**Solution**: 
- Check network connectivity and firewall settings
- Verify validator URLs are correct
- Confirm validators have WebSocket enabled
- Check for SSL/TLS configuration issues

#### Message Handling Errors

**Problem**: Messages aren't being processed correctly.
**Solution**:
- Verify message format matches expected structure
- Check handler registration for correct message types
- Add more detailed logging to trace message flow

#### Race Conditions

**Problem**: WebSocket manager starts before connections are ready.
**Solution**:
- Ensure proper initialization sequence
- Implement retry logic for early connection attempts
- Add status checks before sending messages

#### Memory Leaks

**Problem**: Long-running WebSocket connections cause memory growth.
**Solution**:
- Implement proper connection cleanup
- Add monitoring for connection lifetimes
- Periodically restart connections to prevent accumulated state

## Monitoring the WebSocket System

### Performance Metrics

Track the following metrics to monitor WebSocket performance:

1. **Connection Count**: Number of active WebSocket connections
2. **Message Throughput**: Messages sent/received per second
3. **Latency**: Time between message send and receipt
4. **Fallback Rate**: Percentage of communications using HTTP fallback
5. **Error Rate**: WebSocket errors per minute

### Health Checks

Implement the following health checks:

```python
@app.route('/ws/health')
async def websocket_health():
    """WebSocket health check endpoint."""
    if not websocket_manager:
        return {"status": "disabled"}
        
    return {
        "status": "online" if websocket_manager.is_running() else "offline",
        "connections": websocket_manager.connection_count(),
        "active_connections": websocket_manager.active_connection_count(),
        "uptime": websocket_manager.uptime_seconds()
    }
```

## Conclusion

Migrating from HTTP to WebSocket communication enhances the real-time capabilities of your Bettensor system while maintaining compatibility with existing components. The gradual migration approach ensures that your system remains functional throughout the transition process.

By following this guide, you should be able to successfully implement WebSocket communication in your Bettensor deployment, improving performance and user experience while reducing network overhead. 