# Migration Guide: 0.4.x → 0.5.0

This guide will help you migrate your code from rithmic-rs 0.4.x to 0.5.0.

## Overview of Changes

Version 0.5.0 introduces breaking changes focused on three main areas:

1. **Plant Connection API**: Constructor pattern changed from `new()` to `connect()` with explicit strategies
2. **Configuration API**: New unified `RithmicConfig` replacing separate `AccountInfo` types
3. **Error Handling**: Forced logout and connection error events now require explicit handling (heartbeat monitoring is optional)

## Migration Checklist

- [ ] Update `Cargo.toml` to version 0.5.0
- [ ] Replace `AccountInfo` with `RithmicConfig`
- [ ] Update plant initialization from `new()` to `connect()`
- [ ] Choose appropriate `ConnectStrategy` for your use case
- [ ] Add error handling for forced logout and connection error events (required)
- [ ] Optionally enable heartbeat monitoring with `return_heartbeat_response(true)` if needed
- [ ] Test reconnection behavior
- [ ] Update deprecated type references

## Step-by-Step Migration

### 1. Update Cargo.toml

```toml
[dependencies]
# Before
rithmic-rs = "0.4.2"

# After
rithmic-rs = "0.5.0"
```

### 2. Update Configuration

#### Before (0.4.x)
```rust
use rithmic_rs::connection_info::{AccountInfo, RithmicConnectionSystem, get_config};

let account_info = AccountInfo {
    account_id: "your_account".to_string(),
    env: RithmicConnectionSystem::Demo,
    fcm_id: "your_fcm".to_string(),
    ib_id: "your_ib".to_string(),
};

let conn_info = get_config(&account_info.env);
```

#### After (0.5.0)
```rust
use rithmic_rs::{RithmicConfig, RithmicEnv};

// Option 1: Load from environment variables (recommended)
let config = RithmicConfig::from_env(RithmicEnv::Demo)?;

// Option 2: Use builder pattern
let config = RithmicConfig::builder()
    .user("your_user".to_string())
    .password("your_password".to_string())
    .system_name("Rithmic Paper Trading".to_string())
    .fcm_id("your_fcm".to_string())
    .ib_id("your_ib".to_string())
    .account_id("your_account".to_string())
    .env(RithmicEnv::Demo)
    .build()?;
```

**Key Changes:**
- `RithmicConnectionSystem` → `RithmicEnv`
- `AccountInfo` → `RithmicConfig`
- `get_config()` deprecated → use `RithmicConfig::from_env()` or builder
- Proper error handling with `Result` instead of `.unwrap()`

### 3. Update Plant Initialization

#### Before (0.4.x)
```rust
use rithmic_rs::plants::ticker_plant::RithmicTickerPlant;

// This could panic on connection failure
let ticker_plant = RithmicTickerPlant::new(&account_info).await;
```

#### After (0.5.0)
```rust
use rithmic_rs::{RithmicTickerPlant, ConnectStrategy};

// Choose your connection strategy
let ticker_plant = RithmicTickerPlant::connect(
    &config,
    ConnectStrategy::Simple  // or Retry, or AlternateWithRetry
).await?;
```

**Key Changes:**
- `new()` → `connect()`
- Returns `Result` for proper error handling
- Requires explicit `ConnectStrategy` parameter

### 4. Choose Connection Strategy

Select the appropriate strategy for your use case:

#### Simple (Recommended Default)
```rust
// Fast-fail: Single connection attempt
// Best for: Applications with external retry logic, development/testing
let plant = RithmicTickerPlant::connect(&config, ConnectStrategy::Simple).await?;
```

#### Retry
```rust
// Indefinite retries with exponential backoff (capped at 60s)
// Best for: Production systems requiring high availability
let plant = RithmicTickerPlant::connect(&config, ConnectStrategy::Retry).await?;
```

#### AlternateWithRetry
```rust
// Alternates between primary and beta URLs with retries
// Best for: Maximum reliability during server maintenance
let plant = RithmicTickerPlant::connect(&config, ConnectStrategy::AlternateWithRetry).await?;
```

### 5. Add Connection Health Monitoring

#### Before (0.4.x)
```rust
// Heartbeats were invisible - no error handling possible
loop {
    match handle.subscription_receiver.recv().await {
        Ok(update) => {
            match update.message {
                RithmicMessage::LastTrade(trade) => { /* ... */ }
                _ => {}
            }
        }
        Err(e) => break,
    }
}
```

#### After (0.5.0+)
```rust
// Must handle forced logouts and connection errors
// Heartbeat monitoring is OPTIONAL - see below
loop {
    match handle.subscription_receiver.recv().await {
        Ok(update) => {
            // Check for errors on all messages
            if let Some(error) = &update.error {
                eprintln!("Error from {}: {}", update.source, error);
            }

            // Handle forced logout events
            if matches!(update.message, RithmicMessage::ForcedLogout(_)) {
                eprintln!("Forced logout - must reconnect");
                break;
            }

            // Handle connection errors (added in 0.5.1)
            if matches!(update.message, RithmicMessage::ConnectionError) {
                eprintln!("Connection error - must reconnect");
                break;
            }

            // Process normal messages
            match update.message {
                RithmicMessage::LastTrade(trade) => { /* ... */ }
                RithmicMessage::BestBidOffer(bbo) => { /* ... */ }
                _ => {}
            }
        }
        Err(e) => {
            eprintln!("Channel error: {}", e);
            break;
        }
    }
}
```

#### Optional: Enable Heartbeat Monitoring

By default, heartbeats use request/response pattern and are NOT delivered through the subscription channel. If you need to monitor heartbeat health:

```rust
// Enable heartbeat responses in subscription channel
handle.return_heartbeat_response(true).await;

loop {
    match handle.subscription_receiver.recv().await {
        Ok(update) => {
            // Now heartbeat errors will appear here
            if matches!(update.message, RithmicMessage::ResponseHeartbeat(_)) {
                if let Some(error) = &update.error {
                    eprintln!("Heartbeat error - connection may be degraded");
                    // Implement reconnection logic
                    break;
                }
            }
            // ... rest of message handling
        }
        Err(e) => break,
    }
}
```

**Key Changes:**
- Forced logout events now delivered through subscription channel (REQUIRED)
- Connection errors now delivered through subscription channel (REQUIRED, added in 0.5.1)
- Heartbeat monitoring is OPTIONAL - must be explicitly enabled with `return_heartbeat_response(true)`
- Default: heartbeats use request/response pattern, not sent through channel
- Applications must implement reconnection logic for forced logouts and connection errors

## Complete Example Migration

### Before (0.4.x)
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

### After (0.5.0)
```rust
use rithmic_rs::{RithmicConfig, RithmicEnv, ConnectStrategy};
use rithmic_rs::{RithmicTickerPlant};
use rithmic_rs::rti::messages::RithmicMessage;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Load configuration from environment
    let config = RithmicConfig::from_env(RithmicEnv::Demo)?;

    // Connect with retry strategy
    let ticker_plant = RithmicTickerPlant::connect(
        &config,
        ConnectStrategy::Retry
    ).await?;
    let handle = ticker_plant.get_handle();

    // Login and subscribe
    handle.login().await?;
    handle.subscribe("ESM1", "CME").await?;

    // Process updates with connection health monitoring
    loop {
        match handle.subscription_receiver.recv().await {
            Ok(update) => {
                // Check for errors
                if let Some(error) = &update.error {
                    eprintln!("Error from {}: {}", update.source, error);
                }

                // Handle forced logout (REQUIRED)
                if matches!(update.message, RithmicMessage::ForcedLogout(_)) {
                    eprintln!("Forced logout - must reconnect");
                    break;
                }

                // Handle connection errors (REQUIRED, added in 0.5.1)
                if matches!(update.message, RithmicMessage::ConnectionError) {
                    eprintln!("Connection error - must reconnect");
                    break;
                }

                // Process market data
                if let RithmicMessage::LastTrade(trade) = update.message {
                    println!("Trade: {:?}", trade);
                }
            }
            Err(e) => {
                eprintln!("Channel error: {}", e);
                break;
            }
        }
    }

    Ok(())
}
```

## Common Migration Patterns

### Pattern 1: Simple Migration (Minimal Changes)

If you just want to get your code working with minimal changes:

```rust
// Quick migration: Use deprecated compatibility layer
use rithmic_rs::connection_info::AccountInfo;
use rithmic_rs::config::RithmicConfig;

let account_info = AccountInfo { /* ... */ };
let config: RithmicConfig = account_info.try_into()?;  // Automatic conversion

// Then use new API
let plant = RithmicTickerPlant::connect(&config, ConnectStrategy::Simple).await?;
```

### Pattern 2: Production Migration (Recommended)

For production systems, migrate completely to the new API:

```rust
// 1. Replace AccountInfo with from_env()
let config = RithmicConfig::from_env(RithmicEnv::Demo)?;

// 2. Use Retry strategy for reliability
let plant = RithmicTickerPlant::connect(&config, ConnectStrategy::Retry).await?;

// 3. Implement proper health monitoring
// See "Add Connection Health Monitoring" section above
```

### Pattern 3: Custom Configuration

For programmatic configuration without environment variables:

```rust
let config = RithmicConfig::builder()
    .user(get_username_from_vault()?)
    .password(get_password_from_vault()?)
    .system_name("Rithmic Paper Trading".to_string())
    .env(RithmicEnv::Demo)
    .build()?;
```

## Troubleshooting

### Issue: Connection immediately fails

**Problem:** Using `Simple` strategy in unstable network conditions.

**Solution:** Switch to `Retry` or `AlternateWithRetry` strategy:
```rust
let plant = RithmicTickerPlant::connect(&config, ConnectStrategy::Retry).await?;
```

### Issue: Not detecting heartbeat errors

**Problem:** Heartbeat monitoring is disabled by default.

**Solution:** Enable heartbeat responses if you need connection health monitoring:
```rust
// Enable heartbeat responses to be delivered through subscription channel
handle.return_heartbeat_response(true).await;

// Then monitor for heartbeat errors
if matches!(update.message, RithmicMessage::ResponseHeartbeat(_)) {
    if let Some(error) = &update.error {
        // Handle heartbeat error - connection may be degraded
    }
}
```

### Issue: Application doesn't reconnect after forced logout

**Problem:** Not handling `ForcedLogout` events.

**Solution:** Check for forced logout and implement reconnection:
```rust
if matches!(update.message, RithmicMessage::ForcedLogout(_)) {
    eprintln!("Forced logout - must reconnect");
    // Implement reconnection logic
    break;
}
```

### Issue: Compilation errors with deprecated types

**Problem:** Using deprecated types that will be removed in 0.6.0.

**Solution:** Migrate to new types now:
- `RithmicConnectionSystem` → `RithmicEnv`
- `AccountInfo` → `RithmicConfig`
- `RithmicConnectionInfo` → `RithmicConfig`

### Issue: Environment variables not found

**Problem:** `from_env()` expects specific environment variable names.

**Solution:** Ensure your `.env` file has the correct format:
```bash
# For Demo environment
RITHMIC_DEMO_USER=your_username
RITHMIC_DEMO_PW=your_password

# For Live environment
RITHMIC_LIVE_USER=your_username
RITHMIC_LIVE_PW=your_password

# For Test environment
RITHMIC_TEST_USER=your_username
RITHMIC_TEST_PW=your_password
```

## Testing Your Migration

1. **Test connection establishment:**
   ```rust
   let plant = RithmicTickerPlant::connect(&config, ConnectStrategy::Simple).await?;
   assert!(plant.get_handle().login().await.is_ok());
   ```

2. **Test error handling:**
   - Simulate network disruption
   - Verify forced logout and connection errors are caught
   - If using heartbeat monitoring, verify heartbeat errors are caught
   - Verify reconnection logic works

3. **Test all plants:**
   - Migrate and test each plant type (ticker, order, pnl, history)
   - Verify subscription channels work correctly
   - Ensure no panics on error conditions

## Getting Help

- **Documentation:** https://docs.rs/rithmic-rs
- **Examples:** Check the `examples/` directory in the repository
- **Issues:** https://github.com/pbeets/rithmic-rs/issues
- **Changelog:** See [CHANGELOG.md](CHANGELOG.md) for complete list of changes

## Summary

The 0.5.0 migration focuses on:
1. ✅ More ergonomic configuration with `RithmicConfig`
2. ✅ Explicit connection strategies for better control
3. ✅ Proper error handling instead of panics
4. ✅ Connection health monitoring for robust applications

These changes improve reliability and make your trading systems more resilient to network issues and server events.
