//! # rithmic-rs
//!
//! `rithmic-rs` is a Rust client library for the Rithmic R | Protocol API.
//! This crate provides tools to build algorithmic trading systems that
//! interact with the Rithmic trading platform.
//!
//! ## Features
//!
//! - Connect to Rithmic's WebSocket API with configurable connection strategies
//! - Stream real-time market data (trades, quotes, order book depth)
//! - Submit and manage orders (bracket orders, modifications, cancellations)
//! - Access historical market data (ticks and time bars)
//! - Track positions and P&L
//! - Connection health monitoring with heartbeat and forced logout handling
//!
//! ## Quick Start
//!
//! ```no_run
//! use rithmic_rs::{RithmicConfig, RithmicEnv, ConnectStrategy, RithmicTickerPlant};
//! use rithmic_rs::rti::messages::RithmicMessage;
//! use rithmic_rs::ws::RithmicStream;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     // Load configuration from environment variables
//!     let config = RithmicConfig::from_env(RithmicEnv::Demo)?;
//!
//!     // Connect with Simple strategy (recommended default)
//!     let ticker_plant = RithmicTickerPlant::connect(&config, ConnectStrategy::Simple).await?;
//!     let mut handle = ticker_plant.get_handle();
//!
//!     // Login and subscribe to market data
//!     handle.login().await?;
//!     handle.subscribe("ESM1", "CME").await?;
//!
//!     // Process real-time updates
//!     loop {
//!         match handle.subscription_receiver.recv().await {
//!             Ok(update) => {
//!                 // Check for connection health issues
//!                 if let Some(error) = &update.error {
//!                     eprintln!("Error: {}", error);
//!                     break;
//!                 }
//!
//!                 // Process market data
//!                 match update.message {
//!                     RithmicMessage::LastTrade(trade) => {
//!                         println!("Trade: {:?}", trade);
//!                     }
//!                     RithmicMessage::BestBidOffer(bbo) => {
//!                         println!("BBO: {:?}", bbo);
//!                     }
//!                     _ => {}
//!                 }
//!             }
//!             Err(e) => {
//!                 eprintln!("Channel error: {}", e);
//!                 break;
//!             }
//!         }
//!     }
//!
//!     Ok(())
//! }
//! ```
//!
//! ## Connection Strategies
//!
//! The library provides three connection strategies:
//!
//! - [`ConnectStrategy::Simple`]: Single connection attempt (recommended default, fast-fail)
//! - [`ConnectStrategy::Retry`]: Indefinite retries with exponential backoff capped at 60s
//! - [`ConnectStrategy::AlternateWithRetry`]: Alternates between primary and beta URLs
//!
//! ## Configuration
//!
//! Use [`RithmicConfig`] for modern, ergonomic configuration:
//!
//! ```no_run
//! use rithmic_rs::{RithmicConfig, RithmicEnv};
//!
//! fn example() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
//!     // From environment variables
//!     let config = RithmicConfig::from_env(RithmicEnv::Demo)?;
//!
//!     // Or using builder pattern
//!     let config = RithmicConfig::builder(RithmicEnv::Demo)
//!         .user("your_user".to_string())
//!         .password("your_password".to_string())
//!         .system_name("Rithmic Paper Trading".to_string())
//!         .build()?;
//!     Ok(())
//! }
//! ```
//!
//! ## Module Organization
//!
//! - [`plants`]: Specialized clients for different data types (ticker, order, P&L, history)
//! - [`config`]: Configuration API for connecting to Rithmic
//! - [`api`]: Low-level API interfaces for sending and receiving messages
//! - [`rti`]: Protocol message definitions
//! - [`ws`]: WebSocket connectivity and connection strategies

pub mod api;
/// Configuration API for connecting to Rithmic
pub mod config;
mod ping_manager;
/// Plants for handling different types of market data and order interactions
pub mod plants;
mod request_handler;
/// Definitions for RTI protocol messages
pub mod rti;
/// WebSocket connectivity layer
pub mod ws;

// Re-export plant types for easier access
pub use plants::history_plant::{RithmicHistoryPlant, RithmicHistoryPlantHandle};
pub use plants::order_plant::{RithmicOrderPlant, RithmicOrderPlantHandle};
pub use plants::pnl_plant::{RithmicPnlPlant, RithmicPnlPlantHandle};
pub use plants::ticker_plant::{RithmicTickerPlant, RithmicTickerPlantHandle};

// Re-export modern configuration types for convenience
pub use config::{ConfigError, RithmicConfig, RithmicEnv};

// Re-export connection strategy
pub use ws::ConnectStrategy;
