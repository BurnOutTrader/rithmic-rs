//! Utility types and helpers for working with Rithmic data.
//!
//! This module provides convenience types that help translate Rithmic's raw
//! protocol data into more ergonomic forms for downstream consumers.
//!
//! - [`time`]: Timestamp conversion utilities
//! - [`order_status`]: Order status types and parsing
//! - [`instrument`]: Instrument information types

pub mod instrument;
pub mod order_status;
pub mod time;

// Re-export commonly used types at the util level
pub use instrument::InstrumentInfo;
pub use order_status::OrderStatus;
pub use time::{rithmic_to_unix_nanos, rithmic_to_unix_nanos_precise};
