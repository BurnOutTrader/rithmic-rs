# Migration Guide: 0.5.x → 0.6.0

This guide will help you migrate your code from rithmic-rs 0.5.x to 0.6.0.

## Overview of Changes

Version 0.6.0 removes the optional heartbeat response handling API that was introduced in 0.5.2. Connection health monitoring is now handled exclusively via WebSocket ping/pong timeouts.

## Breaking Changes

### Removed API: `return_heartbeat_response()`

The `return_heartbeat_response()` method has been removed from all plant handles:
- `RithmicTickerPlantHandle`
- `RithmicOrderPlantHandle`
- `RithmicPnlPlantHandle`
- `RithmicHistoryPlantHandle`

### Migration Steps

#### Before (0.5.x)
```rust
let handle = ticker_plant.get_handle();

// Enable heartbeat monitoring
handle.return_heartbeat_response(true).await;

loop {
    match handle.subscription_receiver.recv().await {
        Ok(update) => {
            // Handle heartbeat responses
            if matches!(update.message, RithmicMessage::ResponseHeartbeat(_)) {
                if let Some(error) = &update.error {
                    eprintln!("Heartbeat error - connection degraded");
                    break;
                }
            }
            // ... other message handling
        }
        Err(e) => break,
    }
}
```

#### After (0.6.0)
```rust
let handle = ticker_plant.get_handle();

loop {
    match handle.subscription_receiver.recv().await {
        Ok(update) => {
            // Check for errors on all messages
            if let Some(error) = &update.error {
                eprintln!("Error: {}", error);
            }

            // Handle connection health issues
            match update.message {
                RithmicMessage::HeartbeatTimeout => {
                    eprintln!("Connection timeout - reconnection needed");
                    break;
                }
                RithmicMessage::ForcedLogout(_) => {
                    eprintln!("Forced logout - reconnection needed");
                    break;
                }
                RithmicMessage::ConnectionError => {
                    eprintln!("Connection error - reconnection needed");
                    break;
                }
                _ => {}
            }

            // Process market data
            // ...
        }
        Err(e) => {
            eprintln!("Channel closed: {}", e);
            break;
        }
    }
}
```

## Connection Health Monitoring

Connection health is monitored automatically:

### WebSocket Ping/Pong (Primary)
- Automatically sent every 60 seconds
- Timeout detected after 50 seconds without response
- Triggers `HeartbeatTimeout` message on failure

### Rithmic Protocol Heartbeats
- Sent automatically for protocol compliance
- Successful responses are silently dropped
- Errors from server trigger `HeartbeatTimeout` message

### What You Need to Handle

Monitor the subscription channel for these events:

1. **`HeartbeatTimeout`**: Connection is dead or server is unresponsive
   - Triggered by WebSocket ping timeout or heartbeat error
   - Requires reconnection

2. **`ForcedLogout`**: Server disconnected you
   - Requires reconnection and login

3. **`ConnectionError`**: WebSocket connection failed
   - Requires reconnection

## Complete Migration Example

### Before (0.5.x)
```rust
use rithmic_rs::{RithmicConfig, RithmicEnv, ConnectStrategy, RithmicTickerPlant};
use rithmic_rs::rti::messages::RithmicMessage;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = RithmicConfig::from_env(RithmicEnv::Demo)?;
    let ticker_plant = RithmicTickerPlant::connect(&config, ConnectStrategy::Retry).await?;
    let handle = ticker_plant.get_handle();

    handle.login().await?;
    handle.subscribe("ESM1", "CME").await?;

    // Enable heartbeat monitoring
    handle.return_heartbeat_response(true).await;

    loop {
        match handle.subscription_receiver.recv().await {
            Ok(update) => {
                // Check heartbeat responses
                if matches!(update.message, RithmicMessage::ResponseHeartbeat(_)) {
                    if let Some(error) = &update.error {
                        eprintln!("Heartbeat error");
                        break;
                    }
                    continue; // Skip successful heartbeats
                }

                // Handle other messages
                match update.message {
                    RithmicMessage::LastTrade(trade) => {
                        println!("Trade: {:?}", trade);
                    }
                    _ => {}
                }
            }
            Err(e) => break,
        }
    }

    Ok(())
}
```

### After (0.6.0)
```rust
use rithmic_rs::{RithmicConfig, RithmicEnv, ConnectStrategy, RithmicTickerPlant};
use rithmic_rs::rti::messages::RithmicMessage;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = RithmicConfig::from_env(RithmicEnv::Demo)?;
    let ticker_plant = RithmicTickerPlant::connect(&config, ConnectStrategy::Retry).await?;
    let handle = ticker_plant.get_handle();

    handle.login().await?;
    handle.subscribe("ESM1", "CME").await?;

    loop {
        match handle.subscription_receiver.recv().await {
            Ok(update) => {
                // Check for errors
                if let Some(error) = &update.error {
                    eprintln!("Error: {}", error);
                }

                // Handle connection health issues
                match update.message {
                    RithmicMessage::HeartbeatTimeout => {
                        eprintln!("Connection timeout - reconnection needed");
                        break;
                    }
                    RithmicMessage::ForcedLogout(_) => {
                        eprintln!("Forced logout - reconnection needed");
                        break;
                    }
                    RithmicMessage::ConnectionError => {
                        eprintln!("Connection error - reconnection needed");
                        break;
                    }
                    RithmicMessage::LastTrade(trade) => {
                        println!("Trade: {:?}", trade);
                    }
                    _ => {}
                }
            }
            Err(e) => {
                eprintln!("Channel closed: {}", e);
                break;
            }
        }
    }

    Ok(())
}
```

## Key Differences

| Aspect | 0.5.x | 0.6.0 |
|--------|-------|-------|
| Heartbeat monitoring | Optional via `return_heartbeat_response()` | Automatic via ping/pong |
| API calls needed | `handle.return_heartbeat_response(true)` | None |
| ResponseHeartbeat messages | Delivered to subscription channel | Never delivered |
| Heartbeat errors | Delivered as `ResponseHeartbeat` with error | Delivered as `HeartbeatTimeout` |
| Ping/pong monitoring | Active | Active (primary mechanism) |

## FAQ

### Why was `return_heartbeat_response()` removed?

WebSocket ping/pong provides more reliable connection health monitoring:
- Works at the transport layer
- Detects network issues faster
- Simpler API with fewer concepts to understand

### Will I miss heartbeat errors?

No. Heartbeat errors are still reported as `HeartbeatTimeout` messages, the same as ping timeout events.

### Do I need to send heartbeats manually?

No. Heartbeats are sent automatically for Rithmic protocol compliance.

### How do I detect dead connections?

Monitor for `HeartbeatTimeout` messages in the subscription channel. These are triggered by:
- WebSocket ping timeout (50s)
- Rithmic heartbeat errors from server

## Summary

The 0.6.0 migration simplifies connection health monitoring:
1. ✅ Remove `return_heartbeat_response()` calls
2. ✅ Monitor subscription channel for `HeartbeatTimeout`, `ForcedLogout`, and `ConnectionError`
3. ✅ Connection health is now automatic via WebSocket ping/pong

This change reduces API surface while maintaining robust connection health detection.
