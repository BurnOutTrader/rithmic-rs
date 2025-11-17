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
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     // Load configuration from environment variables
//!     let config = RithmicConfig::from_env(RithmicEnv::Demo)?;
//!
//!     // Connect with Simple strategy (recommended default)
//!     let ticker_plant = RithmicTickerPlant::connect(&config, ConnectStrategy::Simple).await?;
//!     let handle = ticker_plant.get_handle();
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
//! // From environment variables
//! let config = RithmicConfig::from_env(RithmicEnv::Demo)?;
//!
//! // Or using builder pattern
//! let config = RithmicConfig::builder()
//!     .user("your_user".to_string())
//!     .password("your_password".to_string())
//!     .system_name("Rithmic Paper Trading".to_string())
//!     .env(RithmicEnv::Demo)
//!     .build()?;
//! # Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
//! ```
//!
//! ## Module Organization
//!
//! - [`plants`]: Specialized clients for different data types (ticker, order, P&L, history)
//! - [`config`]: Modern configuration API (recommended)
//! - [`connection_info`]: Deprecated configuration types (use `config` instead)
//! - [`api`]: Low-level API interfaces for sending and receiving messages
//! - [`rti`]: Protocol message definitions
//! - [`ws`]: WebSocket connectivity and connection strategies

pub mod api;
/// Modern, streamlined configuration API (recommended)
pub mod config;
/// Deprecated connection information types (use `config` module instead)
pub mod connection_info;
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
