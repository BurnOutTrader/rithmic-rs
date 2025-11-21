# Rust Rithmic R | Protocol API client

Unofficial rust client for connecting to Rithmic's R | Protocol API.

[Documentation](https://docs.rs/rithmic-rs/latest/rithmic_rs/) | [Rithmic APIs](https://www.rithmic.com/apis)

_rithmic protocol version: 0.84.0.0_

Not all functionality has been implemented, but this is currently being used to trade live capital through Rithmic.

Only `order_plant`, `ticker_plant`, `pnl_plant`, `history_plant` are provided. Each plant uses the actor pattern so you'll want to start a plant, and communicate / call commands with it using it's handle. The crate is setup to be used with tokio channels.

The `history_plant` supports loading both historical tick data and time bar data (1-second, 1-minute, 5-minute, daily, and weekly bars).

## Installation

You can install it from crates.io

```
$ cargo add rithmic-rs
```

Or manually add it to your `Cargo.toml` file.


```
[dependencies]
rithmic-rs = "0.5.1"
```

## Breaking Changes in 0.5.0

Version 0.5.0 introduces breaking changes for improved stability and error handling:
- Plant constructors changed from `new()` to `connect()` with explicit connection strategies
- New unified `RithmicConfig` API replaces separate `AccountInfo` types
- Heartbeat errors and forced logout events now delivered through subscription channel

**📖 See [MIGRATION_0.5.0.md](MIGRATION_0.5.0.md) for step-by-step migration guide with code examples.**

Also see [CHANGELOG.md](CHANGELOG.md) for complete list of changes.

## Usage

### Configuration

Rithmic supports three types of account environments: `RithmicEnv::Demo` for paper trading, `RithmicEnv::Live` for funded accounts, and `RithmicEnv::Test` for the test environment before app approval.

Configure your connection using the builder pattern:

```rust
use rithmic_rs::{RithmicConfig, RithmicEnv};

let config = RithmicConfig::builder()
    .user("your_username".to_string())
    .password("your_password".to_string())
    .system_name("Rithmic Paper Trading".to_string())
    .env(RithmicEnv::Demo)
    .build()?;
```

Alternatively, you can load from environment variables using `from_env()`:

```rust
let config = RithmicConfig::from_env(RithmicEnv::Demo)?;
```

Required environment variables for `from_env()`:
```sh
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

> **Note:** The `dotenv` dependency will become optional in version 0.6.0. The builder pattern is recommended for new projects.

### Connection Strategies

The library provides three connection strategies:
- **`Simple`**: Single connection attempt (recommended default, fast-fail)
- **`Retry`**: Indefinite retries with exponential backoff capped at 60 seconds
- **`AlternateWithRetry`**: Alternates between primary and beta URLs with retries

### Quick Start

To use this crate, create a configuration and connect to a plant with your chosen strategy. Each plant uses the actor pattern and spawns a task that listens to commands via a handle. Plants like the ticker plant also include a broadcast channel for real-time updates.

```rust
use rithmic_rs::{RithmicConfig, RithmicEnv, ConnectStrategy, RithmicTickerPlant};

async fn stream_live_ticks() -> Result<(), Box<dyn std::error::Error>> {
    // Load configuration from environment variables
    let config = RithmicConfig::from_env(RithmicEnv::Demo)?;

    // Connect with retry strategy (indefinite retries with 60s max backoff)
    let ticker_plant = RithmicTickerPlant::connect(&config, ConnectStrategy::Retry).await?;
    let handle = ticker_plant.get_handle();

    // Login and subscribe
    handle.login().await?;
    handle.subscribe("ESU5", "CME").await?;

    // Process real-time updates
    loop {
        match handle.subscription_receiver.recv().await {
            Ok(update) => {
                // IMPORTANT: Check for connection health issues
                if let Some(error) = &update.error {
                    eprintln!("Error from {}: {}", update.source, error);
                }

                // Handle forced logout events
                if matches!(update.message, RithmicMessage::ForcedLogout(_)) {
                    eprintln!("Forced logout - must reconnect");
                    break;
                }

                // Handle connection errors (new in 0.5.1)
                if matches!(update.message, RithmicMessage::ConnectionError) {
                    eprintln!("Connection error from {}: {}",
                        update.source,
                        update.error.unwrap_or_else(|| "Unknown error".to_string())
                    );
                    // Plant has stopped, channel will close
                    // Implement reconnection logic here
                    break;
                }

                // Process market data
                match update.message {
                    RithmicMessage::LastTrade(trade) => {
                        println!("Trade: {} @ {}", trade.trade_size.unwrap(), trade.trade_price.unwrap());
                    }
                    RithmicMessage::BestBidOffer(bbo) => {
                        println!("BBO: {}@{} / {}@{}",
                            bbo.bid_size.unwrap(), bbo.bid_price.unwrap(),
                            bbo.ask_price.unwrap(), bbo.ask_size.unwrap());
                    }
                    _ => {}
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

### Heartbeat Monitoring (Optional)

By default, heartbeats are sent automatically but timeout monitoring is disabled. Enable connection health monitoring to detect if the server stops responding:

```rust
// Enable connection health monitoring
handle.return_heartbeat_response(true).await;

// Monitor for heartbeat timeouts (only failures are reported)
loop {
    match handle.subscription_receiver.recv().await {
        Ok(update) => {
            // Check for heartbeat timeouts (automatic detection)
            if matches!(update.message, RithmicMessage::HeartbeatTimeout) {
                eprintln!("Heartbeat timeout - no response after 30s: {}",
                    update.error.unwrap_or_default());
                // Connection is unhealthy - implement reconnection logic
                break;
            }
            // ... handle other messages
        }
        Err(e) => break,
    }
}
```

#### Connection Health Verification

When enabled with `return_heartbeat_response(true)`, the library tracks pending heartbeats and reports ONLY failures:
- If heartbeat response arrives within 30 seconds: Silent (connection is healthy)
- If no response after 30 seconds: `HeartbeatTimeout` message sent to subscription channel

This "silent success, noisy failure" approach reduces channel noise while ensuring you're alerted to connection issues.

**Note:** Enable during critical trading periods to verify the connection is alive. Disable during off-hours to avoid false alarms when the server may not respond.

## Examples

The repository includes several examples to help you get started:

**Environment Variables**

Before running examples, copy `.env.blank` to `.env` and fill in your credentials:
```bash
cp examples/.env.blank .env
# Edit .env with your Rithmic credentials
```

### Basic Connection
```bash
cargo run --example connect
```

### Historical Data
```bash
# Load historical tick data
cargo run --example load_historical_ticks

# Load historical time bars (1-minute, 5-minute, daily)
cargo run --example load_historical_bars
```

## Contribution

Contributions encouraged and welcomed!

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you, as defined in the Apache-2.0 license, shall be
dual licensed as above, without any additional terms or conditions.

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or
[MIT license](LICENSE-MIT) at your option.

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you, as defined in the Apache-2.0 license, shall
be dual licensed as above, without any additional terms or conditions.
