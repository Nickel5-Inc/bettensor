# Bettensor WebSocket Documentation

## Overview

The Bettensor WebSocket implementation provides real-time communication capabilities for miners and validators, enabling instant predictions, confirmations, and game updates. This documentation covers all aspects of the WebSocket system, from basic usage to advanced troubleshooting.

## Table of Contents

### General Documentation

1. [**WebSocket-Based Communication in Bettensor**](websocket_communication.md)  
   Overview of the WebSocket implementation, its benefits, architecture, and configuration options.

2. [**WebSocket vs HTTP Performance**](websocket_vs_http_performance.md)  
   Detailed performance comparisons between WebSocket and HTTP communication methods, including latency, bandwidth, and throughput metrics.

### Developer Guides

3. [**Migration Guide: HTTP to WebSocket Communication**](migration_guide_http_to_websocket.md)  
   Step-by-step instructions for migrating existing HTTP-based communication to WebSocket, with code examples and best practices.

4. [**Implementing WebSocket Handlers**](implementing_websocket_handlers.md)  
   Guide to implementing custom WebSocket event handlers for both miners and validators, with examples for prediction, confirmation, and game update handlers.

### Examples and Tutorials

5. [**WebSocket Demo**](../examples/websocket_demo.py)  
   Demonstration script showing WebSocket usage in both miner and validator modes.

### Troubleshooting

6. [**WebSocket Troubleshooting Guide**](websocket_troubleshooting.md)  
   Comprehensive guide to diagnosing and resolving common WebSocket issues, including connection problems, message handling, and performance optimization.

## Quick Start

1. **Enable WebSocket in Validator**:
   ```bash
   python -m bettensor.validator.main \
     --websocket.enabled=True \
     --websocket.heartbeat_interval=60 \
     --websocket.connection_timeout=300
   ```

2. **Enable WebSocket in Miner**:
   ```bash
   python -m bettensor.miner.main \
     --websocket_enabled=True \
     --websocket_heartbeat_interval=60 \
     --websocket_reconnect_base_interval=5 \
     --websocket_max_reconnect_interval=300
   ```

3. **Test WebSocket Connection**:
   ```bash
   python examples/websocket_demo.py --mode=both
   ```

## System Requirements

- **Python**: 3.8 or higher
- **Dependencies**: 
  - websockets>=10.0
  - asyncio>=3.4.3
  - bittensor>=3.5.0
- **Network**: Open TCP port for WebSocket (default: 10401)
- **Hardware**: At least 2GB RAM recommended for validators with many connections

## Support and Community

For questions, issues, or suggestions regarding the WebSocket implementation:

- **GitHub Issues**: [Bettensor GitHub Repository](https://github.com/opentensor/bettensor/issues)
- **Discord**: [Bittensor Discord](https://discord.gg/bittensor)
- **Forum**: [Bittensor Forum](https://forum.bittensor.com)

## Future Development

Planned improvements for the WebSocket system include:

- Secure WebSocket (WSS) support
- Message compression for large payloads
- Connection pooling and load balancing
- Extended monitoring and analytics

## Contributing

Contributions to the WebSocket implementation are welcome! See the [contribution guidelines](../CONTRIBUTING.md) for more information.

## License

The Bettensor WebSocket implementation is available under the same license as the rest of the Bettensor project. 