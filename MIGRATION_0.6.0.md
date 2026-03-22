# Migration Guide: 0.4.x → 0.6.0

This guide helps you migrate from rithmic-rs 0.4.x to 0.6.0. If you're already on 0.5.x, skip to the [0.5.x → 0.6.0 section](#migrating-from-05x).

## Quick Summary

**Major Changes:**
- New unified `RithmicConfig` API (replaces `AccountInfo`)
- Explicit connection strategies (replaces `new()` with `connect()`)
- Automatic connection health monitoring via WebSocket ping/pong
- Removed deprecated `connection_info` module
- Updated to modern `dotenvy` crate

## Migrating from 0.4.x

### 1. Update Dependencies

```toml
[dependencies]
rithmic-rs = "0.6.0"
```

### 2. Replace Configuration API

**Before (0.4.x):**
```rust
use rithmic_rs::connection_info::{AccountInfo, RithmicConnectionSystem};

let account_info = AccountInfo {
    account_id: "your_account".to_string(),
    env: RithmicConnectionSystem::Demo,
    fcm_id: "your_fcm".to_string(),
    ib_id: "your_ib".to_string(),
};
```

**After (0.6.0):**
```rust
use rithmic_rs::{RithmicConfig, RithmicEnv};

// From environment variables (recommended)
let config = RithmicConfig::from_env(RithmicEnv::Demo)?;

// Or using builder pattern
let config = RithmicConfig::builder(RithmicEnv::Demo)
    .account_id("your_account")
    .fcm_id("your_fcm")
    .ib_id("your_ib")
    .user("your_user")
    .password("your_password")
    .app_name("your_app_name")
    .app_version("1")
    .build()?;
```

**Required Environment Variables:**
```bash
RITHMIC_APP_NAME=your_app_name
RITHMIC_APP_VERSION=1

# For Demo
RITHMIC_DEMO_ACCOUNT_ID=your_account
RITHMIC_DEMO_FCM_ID=your_fcm
RITHMIC_DEMO_IB_ID=your_ib
RITHMIC_DEMO_USER=your_username
RITHMIC_DEMO_PW=your_password
RITHMIC_DEMO_URL=<provided_by_rithmic>
RITHMIC_DEMO_ALT_URL=<provided_by_rithmic>

# For Live
RITHMIC_LIVE_ACCOUNT_ID=your_account
RITHMIC_LIVE_FCM_ID=your_fcm
RITHMIC_LIVE_IB_ID=your_ib
RITHMIC_LIVE_USER=your_username
RITHMIC_LIVE_PW=your_password
RITHMIC_LIVE_URL=<provided_by_rithmic>
RITHMIC_LIVE_ALT_URL=<provided_by_rithmic>
```

See `examples/.env.blank` for connection URLs provided by Rithmic.

### 3. Update Plant Initialization

**Before (0.4.x):**
```rust
let ticker_plant = RithmicTickerPlant::new(&account_info).await;
```

**After (0.6.0):**
```rust
use rithmic_rs::ConnectStrategy;

let ticker_plant = RithmicTickerPlant::connect(
    &config,
    ConnectStrategy::Simple  // or Retry, or AlternateWithRetry
).await?;
```

**Connection Strategies:**
- `Simple`: Single attempt, fast-fail (recommended for development)
- `Retry`: Infinite retries with exponential backoff, max 60s (recommended for production)
- `AlternateWithRetry`: Alternates between primary/beta URLs with retries

### 4. Update Event Handling

**Before (0.4.x):**
```rust
loop {
    match handle.subscription_receiver.recv().await {
        Ok(update) => {
            if let RithmicMessage::LastTrade(trade) = update.message {
                println!("Trade: {:?}", trade);
            }
        }
        Err(_) => break,
    }
}
```

**After (0.6.0):**
```rust
loop {
    match handle.subscription_receiver.recv().await {
        Ok(update) => {
            // Check for errors on all messages
            if let Some(error) = &update.error {
                eprintln!("Error: {}", error);
            }

            // Handle connection health events
            match update.message {
                RithmicMessage::HeartbeatTimeout => {
                    eprintln!("Connection timeout - reconnect needed");
                    break;
                }
                RithmicMessage::ForcedLogout(_) => {
                    eprintln!("Forced logout - reconnect needed");
                    break;
                }
                RithmicMessage::ConnectionError => {
                    eprintln!("Connection error - reconnect needed");
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
```

### Complete Example

**Before (0.4.x):**
```rust
use rithmic_rs::connection_info::{AccountInfo, RithmicConnectionSystem};
use rithmic_rs::plants::ticker_plant::RithmicTickerPlant;
use rithmic_rs::rti::messages::RithmicMessage;

#[tokio::main]
async fn main() {
    let account_info = AccountInfo {
        account_id: "account".to_string(),
        env: RithmicConnectionSystem::Demo,
        fcm_id: "fcm".to_string(),
        ib_id: "ib".to_string(),
    };

    let ticker_plant = RithmicTickerPlant::new(&account_info).await;
    let handle = ticker_plant.get_handle();

    handle.login().await.unwrap();
    handle.subscribe("ESM1", "CME").await.unwrap();

    loop {
        match handle.subscription_receiver.recv().await {
            Ok(update) => {
                if let RithmicMessage::LastTrade(trade) = update.message {
                    println!("Trade: {:?}", trade);
                }
            }
            Err(_) => break,
        }
    }
}
```

**After (0.6.0):**
```rust
use rithmic_rs::{RithmicConfig, RithmicEnv, ConnectStrategy, RithmicTickerPlant};
use rithmic_rs::rti::messages::RithmicMessage;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Load config from environment
    let config = RithmicConfig::from_env(RithmicEnv::Demo)?;

    // Connect with retry strategy
    let ticker_plant = RithmicTickerPlant::connect(
        &config,
        ConnectStrategy::Retry
    ).await?;
    let handle = ticker_plant.get_handle();

    handle.login().await?;
    handle.subscribe("ESM1", "CME").await?;

    loop {
        match handle.subscription_receiver.recv().await {
            Ok(update) => {
                if let Some(error) = &update.error {
                    eprintln!("Error: {}", error);
                }

                match update.message {
                    RithmicMessage::HeartbeatTimeout => {
                        eprintln!("Connection timeout");
                        break;
                    }
                    RithmicMessage::ForcedLogout(_) => {
                        eprintln!("Forced logout");
                        break;
                    }
                    RithmicMessage::ConnectionError => {
                        eprintln!("Connection error");
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

## Migrating from 0.5.x

If you're already on 0.5.x, you only need to make these small changes:

### 1. Remove Heartbeat Response Handling

**Before (0.5.x):**
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
                    eprintln!("Heartbeat error");
                    break;
                }
                continue;
            }
            // ... other handling
        }
        Err(e) => break,
    }
}
```

**After (0.6.0):**
```rust
let handle = ticker_plant.get_handle();

loop {
    match handle.subscription_receiver.recv().await {
        Ok(update) => {
            match update.message {
                RithmicMessage::HeartbeatTimeout => {
                    eprintln!("Connection timeout");
                    break;
                }
                // ... other handling
            }
        }
        Err(e) => break,
    }
}
```

### 2. Update Environment Variable Loading (if using .env files)

If your code directly imports `dotenv`:

**Before (0.5.x):**
```rust
use dotenv::dotenv;

dotenv().ok();
let config = RithmicConfig::from_env(RithmicEnv::Demo)?;
```

**After (0.6.0):**
```rust
use dotenvy;

dotenvy::dotenv().ok();
let config = RithmicConfig::from_env(RithmicEnv::Demo)?;
```

Or add to your dependencies:
```toml
[dependencies]
dotenvy = "0.15.7"
```

## Connection Health Monitoring

Connection health is now automatic via WebSocket ping/pong:

- **WebSocket ping** sent every 60 seconds
- **Timeout** detected after 50 seconds without response
- **Events** delivered through subscription channel:
  - `HeartbeatTimeout`: Connection dead or server unresponsive
  - `ForcedLogout`: Server disconnected you
  - `ConnectionError`: WebSocket connection failed

**No manual heartbeat handling needed** - just monitor for these events and reconnect when they occur.

## Troubleshooting

### "connection_info module not found"
The deprecated `connection_info` module has been removed. Use `RithmicConfig` instead.

### "return_heartbeat_response method not found"
This method was removed in 0.6.0. Connection health is now automatic - just monitor for `HeartbeatTimeout` events.

### "Missing environment variable"
Ensure all required variables are set for your environment:
```bash
RITHMIC_DEMO_ACCOUNT_ID=...
RITHMIC_DEMO_FCM_ID=...
RITHMIC_DEMO_IB_ID=...
RITHMIC_DEMO_USER=...
RITHMIC_DEMO_PW=...
RITHMIC_DEMO_URL=...
RITHMIC_DEMO_ALT_URL=...
```

### Connection fails immediately
Try using `ConnectStrategy::Retry` for automatic reconnection:
```rust
RithmicTickerPlant::connect(&config, ConnectStrategy::Retry).await?
```

## Summary

Key improvements in 0.6.0:
- ✅ Cleaner, more ergonomic configuration API
- ✅ Explicit connection strategies for better control
- ✅ Automatic connection health monitoring
- ✅ Proper error handling (no more panics)
- ✅ Removed deprecated APIs

These changes make your trading systems more reliable and easier to maintain.
