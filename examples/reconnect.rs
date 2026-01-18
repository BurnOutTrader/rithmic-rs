//! Example: Persistent connection with automatic reconnection and re-subscription
//!
//! Run with: cargo run --example reconnect
//!
//! This example runs indefinitely. Press Ctrl+C to exit.

use std::{collections::HashSet, env};
use tracing::{error, info, warn};

use rithmic_rs::{
    ConnectStrategy, RithmicConfig, RithmicEnv, RithmicTickerPlant, rti::messages::RithmicMessage,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenvy::dotenv().ok();
    tracing_subscriber::fmt().init();

    let config = RithmicConfig::from_env(RithmicEnv::Demo)?;

    // Track subscriptions to restore after reconnect
    let mut subscriptions: HashSet<(String, String)> = HashSet::new();
    let symbol = env::var("SYMBOL").unwrap_or_else(|_| "ESH5".to_string());
    let exchange = env::var("EXCHANGE").unwrap_or_else(|_| "CME".to_string());
    subscriptions.insert((symbol, exchange));

    // Outer loop: reconnection
    loop {
        let plant = match RithmicTickerPlant::connect(&config, ConnectStrategy::Simple).await {
            Ok(p) => p,
            Err(e) => {
                error!("Connect failed: {}", e);
                tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                continue;
            }
        };

        let mut handle = plant.get_handle();

        if handle.login().await.is_err() {
            tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
            continue;
        }

        // Re-subscribe to all tracked symbols
        for (symbol, exchange) in &subscriptions {
            let _ = handle.subscribe(symbol, exchange).await;
            info!("Subscribed to {} on {}", symbol, exchange);
        }

        // Process until disconnect
        while let Ok(update) = handle.subscription_receiver.recv().await {
            match &update.message {
                RithmicMessage::HeartbeatTimeout
                | RithmicMessage::ForcedLogout(_)
                | RithmicMessage::ConnectionError => {
                    warn!("Disconnected, reconnecting...");
                    break;
                }
                RithmicMessage::LastTrade(t) => {
                    info!(
                        "Trade: {} @ {}",
                        t.trade_size.unwrap_or(0),
                        t.trade_price.unwrap_or(0.0)
                    );
                }
                _ => {}
            }
        }

        tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;
    }
}
