//! # rithmic-rs
//!
//! `rithmic-rs` is a Rust client library for the Rithmic R | Protocol API.
//! This crate provides tools to build algorithmic trading systems that
//! interact with the Rithmic trading platform.
//!
//! ## Features
//!
//! - Connect to Rithmic's WebSocket API
//! - Stream market data (ticker, depth)
//! - Submit and manage orders
//! - Access historical market data
//! - Track positions and P&L
//!
//! ## Structure
//!
//! The library is organized into several modules:
//! - `plants`: Contains specialized clients for different data types (ticker, order, P&L, history)
//! - `api`: Contains the API interfaces for sending and receiving messages
//! - `rti`: Contains definitions for RTI protocol messages
//! - `ws`: WebSocket connectivity layer

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
