//! Example: Subscribe to real-time market data (LastTrade and BBO)
use std::env;
use tracing::{Level, event, info};

use rithmic_rs::{
    ConnectStrategy, RithmicConfig, RithmicEnv, RithmicTickerPlant, rti::messages::RithmicMessage,
    ws::RithmicStream,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Load environment variables from .env file
    dotenvy::dotenv().ok();

    // Create configuration from environment variables
    let config = RithmicConfig::from_env(RithmicEnv::Demo)?;

    tracing_subscriber::fmt().init();

    // Symbol and exchange can be customized here
    let symbol = env::var("SYMBOL").unwrap_or_else(|_| "ZNU5".to_string());
    let exchange = env::var("EXCHANGE").unwrap_or_else(|_| "CBOT".to_string());

    let ticker_plant = RithmicTickerPlant::connect(&config, ConnectStrategy::Simple).await?;
    let mut handle = ticker_plant.get_handle();

    handle.login().await?;

    info!("Subscribing to market data for {} on {}", symbol, exchange);

    let _ = handle.subscribe(&symbol, &exchange).await?;

    // Process a handful of updates, then exit
    let mut processed = 0usize;
    let max_updates = 100usize;

    while processed < max_updates {
        match handle.subscription_receiver.recv().await {
            Ok(update) => match update.message {
                RithmicMessage::LastTrade(t) => {
                    let price = t.trade_price.unwrap_or(0.0);
                    let size = t.trade_size.unwrap_or(0);

                    info!("LastTrade {}: {} @ {}", symbol, size, price);

                    processed += 1;
                }
                RithmicMessage::BestBidOffer(b) => {
                    let bid = b.bid_price.unwrap_or(0.0);
                    let bid_sz = b.bid_size.unwrap_or(0);
                    let ask = b.ask_price.unwrap_or(0.0);
                    let ask_sz = b.ask_size.unwrap_or(0);

                    info!(
                        "BBO {}: bid {} x {}, ask {} x {}",
                        symbol, bid, bid_sz, ask, ask_sz
                    );

                    processed += 1;
                }
                _ => {}
            },
            Err(e) => {
                event!(Level::ERROR, "Error receiving update: {}", e);
                break;
            }
        }
    }

    handle.disconnect().await?;

    info!("Disconnected from Rithmic");

    Ok(())
}
