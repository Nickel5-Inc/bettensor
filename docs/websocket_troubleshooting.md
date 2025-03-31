# WebSocket Troubleshooting Guide

This document provides solutions for common WebSocket-related issues that miners and validators might encounter in the Bettensor system.

## Connection Issues

### Symptoms: Unable to Establish WebSocket Connection

If your miner or validator cannot establish WebSocket connections, check the following:

#### 1. Network Configuration

**Issue**: Firewall blocking WebSocket traffic.

**Diagnostic Commands**:
```bash
# Test if the port is accessible
nc -zv <validator_ip> <websocket_port>

# Check local firewall rules
sudo iptables -L | grep <websocket_port>
```

**Solution**:
- Ensure the WebSocket port (usually 10401) is open in your firewall.
- For validators, add a rule to allow inbound traffic on the WebSocket port.
- For miners, ensure outbound connections to the WebSocket port are allowed.

#### 2. Validator Configuration

**Issue**: WebSocket server not enabled or misconfigured on validator.

**Diagnostic Commands**:
```bash
# Check if WebSocket port is listening
netstat -tlnp | grep <websocket_port>

# Check validator logs
tail -f /var/log/bettensor/validator.log | grep -i websocket
```

**Solution**:
- Ensure validator has `--websocket.enabled=True` in its configuration.
- Verify WebSocket port is set correctly.
- Restart validator service after configuration changes.

#### 3. Connection Timeouts

**Issue**: WebSocket connection attempts timeout.

**Diagnostic Commands**:
```bash
# Check network latency
ping <validator_ip>

# Trace route
traceroute <validator_ip>
```

**Solution**:
- Increase connection timeout parameters.
- Check if there are network issues between miner and validator.
- Verify validator is not overloaded (check CPU/memory usage).

### Symptoms: Connections Drop Frequently

If WebSocket connections are established but drop frequently:

#### 1. Heartbeat Issues

**Issue**: Heartbeat messages failing or not configured properly.

**Diagnostic Commands**:
```bash
# Check logs for heartbeat messages
grep -i "heartbeat" /var/log/bettensor/miner.log
```

**Solution**:
- Decrease heartbeat interval (e.g., from 60s to 30s).
- Ensure heartbeat messages are being sent and received.
- Check for network instability that might interrupt heartbeats.

#### 2. Resource Constraints

**Issue**: System running out of resources.

**Diagnostic Commands**:
```bash
# Check memory usage
free -m

# Check CPU usage
top -bn1

# Check open file descriptors
lsof | wc -l
```

**Solution**:
- Increase system resources (memory, CPU).
- Review and increase system limits (e.g., max open files).
- Add configuration to the `/etc/security/limits.conf` file:
  ```
  * soft nofile 65535
  * hard nofile 65535
  ```

#### 3. Reconnection Loop

**Issue**: Miner repeatedly reconnecting to validator.

**Diagnostic Commands**:
```bash
# Look for reconnection patterns
grep -i "reconnect" /var/log/bettensor/miner.log
```

**Solution**:
- Adjust reconnection parameters (base interval, max interval).
- Implement exponential backoff if not already done.
- Ensure reconnection attempts are properly limited.

## Message Handling Issues

### Symptoms: Messages Not Being Processed

If messages are sent but not processed by the recipient:

#### 1. Handler Registration

**Issue**: Message handlers not properly registered.

**Diagnostic Commands**:
```bash
# Check if handlers are registered at startup
grep -i "register_handler" /var/log/bettensor/miner.log
```

**Solution**:
- Verify handler registration code is executed at startup.
- Ensure handler registration is called with correct parameters.
- Check for any errors during handler registration.

#### 2. Message Format

**Issue**: Messages not formatted according to protocol.

**Diagnostic Commands**:
```bash
# Enable debug logging for message parsing
# Add to configuration file:
# logging.level.bettensor.websocket=DEBUG

# Check logs for message parsing errors
grep -i "parse error" /var/log/bettensor/validator.log
```

**Solution**:
- Ensure messages follow the format specified in the protocol.
- Add validation for message structure before sending.
- Check if protocol versions match between miner and validator.

#### 3. Error Handling

**Issue**: Exceptions in message handlers causing messages to be dropped.

**Diagnostic Commands**:
```bash
# Look for exceptions in logs
grep -i "exception" /var/log/bettensor/validator.log
```

**Solution**:
- Add try/except blocks in message handlers.
- Log all exceptions in handlers for better diagnosis.
- Ensure exceptions don't terminate the WebSocket connection.

### Symptoms: Delayed Message Processing

If messages are being processed but with significant delay:

#### 1. Handler Blocking

**Issue**: Message handlers contain blocking operations.

**Diagnostic Solution**:
- Review handler implementations for blocking calls.
- Move intensive operations to background tasks.
- Ensure handlers return quickly and delegate work.

#### 2. Queue Overflow

**Issue**: Message queue backed up due to high volume.

**Diagnostic Commands**:
```bash
# Check system load
uptime

# Monitor message processing rate through logs
# Add appropriate logging and then analyze
```

**Solution**:
- Implement message prioritization.
- Scale up resources to handle higher throughput.
- Consider adding worker threads for processing.

## Broadcast Issues

### Symptoms: Game Updates Not Reaching Miners

If game updates are not being broadcast successfully:

#### 1. Broadcast Method

**Issue**: Broadcast method not implemented correctly.

**Diagnostic Commands**:
```bash
# Check if broadcast is being called
grep -i "broadcast" /var/log/bettensor/validator.log

# Check if updates are being sent
grep -i "game update" /var/log/bettensor/validator.log
```

**Solution**:
- Verify broadcast method is called when game updates occur.
- Ensure broadcast code iterates through all connected clients.
- Add logging to track broadcast attempts and successes.

#### 2. Connection List

**Issue**: List of connections not maintained properly.

**Diagnostic Solution**:
- Add regular logging of connected client count.
- Check for proper client addition and removal.
- Implement periodic connection validity checks.

## Performance Issues

### Symptoms: High CPU or Memory Usage

If WebSocket communication is causing high resource usage:

#### 1. Connection Leaks

**Issue**: Connections not being closed properly.

**Diagnostic Commands**:
```bash
# Check number of connections
netstat -an | grep <websocket_port> | wc -l
```

**Solution**:
- Ensure proper cleanup of closed connections.
- Implement periodic stale connection cleanup.
- Set appropriate timeouts for inactive connections.

#### 2. Message Size

**Issue**: Large messages consuming excessive bandwidth.

**Diagnostic Solution**:
- Monitor message sizes in logs.
- Consider implementing message compression.
- Break large updates into smaller chunks.

#### 3. Connection Scaling

**Issue**: Too many connections for system capacity.

**Diagnostic Commands**:
```bash
# Check current connection count
netstat -an | grep <websocket_port> | wc -l
```

**Solution**:
- Implement connection pooling.
- Consider validator load balancing for large deployments.
- Optimize WebSocket library configuration for high connection counts.

## Debugging Techniques

### Enable Verbose WebSocket Logging

Add the following to your configuration to enable detailed WebSocket logs:

**For Validators**:
```bash
--log.level=DEBUG --websocket.log_level=TRACE
```

**For Miners**:
```bash
--log_level=DEBUG --websocket_log_level=TRACE
```

### Use WebSocket Test Tools

For testing WebSocket connections independently of the Bettensor system:

1. **WebSocket Client Tool**:
   ```bash
   # Install wscat
   npm install -g wscat
   
   # Connect to validator WebSocket
   wscat -c ws://<validator_ip>:<websocket_port>
   ```

2. **Monitor WebSocket Traffic**:
   ```bash
   # Capture WebSocket traffic with tcpdump
   sudo tcpdump -i any -s 0 -w websocket.pcap port <websocket_port>
   
   # Analyze with Wireshark
   wireshark websocket.pcap
   ```

### Test Connection Script

Save this script as `test_websocket.py` to test basic WebSocket connectivity:

```python
#!/usr/bin/env python3
import asyncio
import websockets
import sys
import json
import time

async def test_connection(url, hotkey):
    """Test WebSocket connection to a validator."""
    try:
        print(f"Connecting to {url}...")
        async with websockets.connect(url) as websocket:
            print("Connected! Sending heartbeat...")
            
            # Send a heartbeat message
            await websocket.send(json.dumps({
                "type": "heartbeat",
                "sender": hotkey,
                "data": {"timestamp": time.time()}
            }))
            
            # Wait for response
            print("Waiting for response...")
            response = await asyncio.wait_for(websocket.recv(), timeout=5)
            print(f"Received: {response}")
            
            print("Connection test successful!")
            return True
    except Exception as e:
        print(f"Connection failed: {str(e)}")
        return False

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: test_websocket.py <validator_url> <miner_hotkey>")
        sys.exit(1)
        
    validator_url = sys.argv[1]
    miner_hotkey = sys.argv[2]
    
    # Ensure URL has ws:// prefix
    if not validator_url.startswith("ws://") and not validator_url.startswith("wss://"):
        validator_url = f"ws://{validator_url}"
    
    asyncio.run(test_connection(validator_url, miner_hotkey))
```

Execute the script:
```bash
python test_websocket.py ws://validator_ip:10401 your_miner_hotkey
```

## Common Error Messages and Solutions

| Error Message | Possible Cause | Solution |
|---------------|----------------|----------|
| `Connection refused` | WebSocket server not running | Verify validator is running with WebSocket enabled |
| `Connection timeout` | Network latency or firewall issues | Check network connectivity and firewall rules |
| `Not authorized` | Invalid hotkey or authentication failure | Verify miner is registered and has correct hotkey |
| `Max connections reached` | Validator connection limit reached | Increase max connections or distribute miners |
| `Invalid message format` | Message doesn't follow protocol | Fix message structure to comply with protocol |
| `Heartbeat timeout` | No heartbeat received in time | Check network stability, decrease heartbeat interval |
| `Client already connected` | Multiple connections from same miner | Implement proper connection management in miner |
| `Server closing connection` | Server-side issue or maintenance | Check validator logs for shutdown reason |

## System-Specific Issues

### Docker Environments

**Issue**: WebSocket connections not working in Docker containers.

**Solution**:
- Ensure proper port mapping in Docker configuration.
- For miners in containers, use host networking if possible.
- Check Docker network configuration for isolation issues.

### Proxy and Load Balancer Setups

**Issue**: WebSocket connections not working through proxy.

**Solution**:
- Configure proxy to support WebSocket protocol.
- Ensure proxy timeouts are longer than WebSocket timeouts.
- For NGINX, use the following configuration:
  ```
  location /ws/ {
      proxy_pass http://backend_server;
      proxy_http_version 1.1;
      proxy_set_header Upgrade $http_upgrade;
      proxy_set_header Connection "upgrade";
      proxy_read_timeout 600s;
  }
  ```

### Cloud Environments

**Issue**: WebSocket connections dropping in cloud environments.

**Solution**:
- Check cloud provider's load balancer settings.
- Increase idle timeout settings for network components.
- Consider using dedicated instances instead of shared resources.

## Proactive Monitoring

Implement the following monitoring to detect issues before they affect users:

1. **Connection Count Alerts**:
   - Set up alerts for sudden drops in connection count.
   - Monitor connection establishment rate.

2. **Message Rate Monitoring**:
   - Track messages sent/received per second.
   - Alert on significant deviations from baseline.

3. **Latency Tracking**:
   - Measure round-trip time for messages.
   - Set thresholds for acceptable latency.

4. **Error Rate Monitoring**:
   - Track WebSocket errors as a percentage of total operations.
   - Alert when error rate exceeds threshold.

## Conclusion

Most WebSocket issues in Bettensor can be resolved by checking:

1. **Connectivity**: Ensure network connections are possible and stable.
2. **Configuration**: Verify WebSocket settings on both miners and validators.
3. **Resource Allocation**: Provide sufficient system resources for expected load.
4. **Implementation**: Ensure handlers are properly registered and error-resistant.
5. **Monitoring**: Implement proactive monitoring to catch issues early.

If you encounter issues not covered in this guide, check the latest logs, refer to the documentation, or reach out to the development team for assistance. 