//! Order status types and utilities.
//!
//! This module provides a typed representation of order status values
//! returned by Rithmic, along with helper methods for common status checks.

use std::convert::Infallible;
use std::fmt;
use std::str::FromStr;

/// Status string for an open order.
pub const OPEN: &str = "open";

/// Status string for a completed/filled order.
pub const COMPLETE: &str = "complete";

/// Status string for a cancelled order.
pub const CANCELLED: &str = "cancelled";

/// Status string for a pending order.
pub const PENDING: &str = "pending";

/// Status string for a rejected order.
pub const REJECTED: &str = "rejected";

/// Status string for a partially filled order.
pub const PARTIAL: &str = "partial";

/// Typed representation of order status.
///
/// This enum provides a type-safe way to work with order statuses,
/// with helper methods for common status checks.
///
/// # Example
/// ```
/// use rithmic_rs::OrderStatus;
///
/// let status: OrderStatus = "complete".parse().unwrap();
/// assert_eq!(status, OrderStatus::Complete);
/// assert!(status.is_terminal());
/// assert!(!status.is_active());
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum OrderStatus {
    /// Order is open and working on the exchange.
    Open,
    /// Order has been completely filled.
    Complete,
    /// Order has been cancelled.
    Cancelled,
    /// Order is pending (not yet acknowledged by exchange).
    Pending,
    /// Order was rejected by the exchange or broker.
    Rejected,
    /// Order is partially filled.
    Partial,
    /// Unknown or unrecognized status.
    #[default]
    Unknown,
}

impl OrderStatus {
    /// Returns true if this is a terminal status (order is no longer active).
    ///
    /// Terminal statuses are: Complete, Cancelled, Rejected.
    ///
    /// # Example
    /// ```
    /// use rithmic_rs::OrderStatus;
    ///
    /// assert!(OrderStatus::Complete.is_terminal());
    /// assert!(OrderStatus::Cancelled.is_terminal());
    /// assert!(OrderStatus::Rejected.is_terminal());
    /// assert!(!OrderStatus::Open.is_terminal());
    /// ```
    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Complete | Self::Cancelled | Self::Rejected)
    }

    /// Returns true if this is an active status (order may still fill).
    ///
    /// Active statuses are: Open, Pending, Partial.
    ///
    /// # Example
    /// ```
    /// use rithmic_rs::OrderStatus;
    ///
    /// assert!(OrderStatus::Open.is_active());
    /// assert!(OrderStatus::Pending.is_active());
    /// assert!(OrderStatus::Partial.is_active());
    /// assert!(!OrderStatus::Complete.is_active());
    /// ```
    pub fn is_active(&self) -> bool {
        matches!(self, Self::Open | Self::Pending | Self::Partial)
    }
}

impl FromStr for OrderStatus {
    type Err = Infallible;

    /// Parse a status string into an OrderStatus enum.
    ///
    /// Matching is case-insensitive and handles common variations:
    /// - "open" -> Open
    /// - "complete", "filled" -> Complete
    /// - "cancelled", "canceled" -> Cancelled
    /// - "pending" -> Pending
    /// - "rejected" -> Rejected
    /// - "partial", "partially_filled" -> Partial
    ///
    /// Unrecognized values return `OrderStatus::Unknown`.
    ///
    /// # Example
    /// ```
    /// use rithmic_rs::OrderStatus;
    ///
    /// let status: OrderStatus = "Complete".parse().unwrap();
    /// assert_eq!(status, OrderStatus::Complete);
    ///
    /// let status: OrderStatus = "FILLED".parse().unwrap();
    /// assert_eq!(status, OrderStatus::Complete);
    ///
    /// let status: OrderStatus = "canceled".parse().unwrap();
    /// assert_eq!(status, OrderStatus::Cancelled);
    /// ```
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let status = match s.to_lowercase().as_str() {
            OPEN => Self::Open,
            COMPLETE | "filled" => Self::Complete,
            CANCELLED | "canceled" => Self::Cancelled,
            PENDING => Self::Pending,
            REJECTED => Self::Rejected,
            PARTIAL | "partially_filled" | "partially filled" => Self::Partial,
            _ => Self::Unknown,
        };
        Ok(status)
    }
}

impl fmt::Display for OrderStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            Self::Open => OPEN,
            Self::Complete => COMPLETE,
            Self::Cancelled => CANCELLED,
            Self::Pending => PENDING,
            Self::Rejected => REJECTED,
            Self::Partial => PARTIAL,
            Self::Unknown => "unknown",
        };
        write!(f, "{}", s)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_case_insensitive() {
        assert_eq!("open".parse::<OrderStatus>().unwrap(), OrderStatus::Open);
        assert_eq!("OPEN".parse::<OrderStatus>().unwrap(), OrderStatus::Open);
        assert_eq!("Open".parse::<OrderStatus>().unwrap(), OrderStatus::Open);
    }

    #[test]
    fn test_parse_variations() {
        // Complete variations
        assert_eq!(
            "complete".parse::<OrderStatus>().unwrap(),
            OrderStatus::Complete
        );
        assert_eq!(
            "filled".parse::<OrderStatus>().unwrap(),
            OrderStatus::Complete
        );

        // Cancelled variations
        assert_eq!(
            "cancelled".parse::<OrderStatus>().unwrap(),
            OrderStatus::Cancelled
        );
        assert_eq!(
            "canceled".parse::<OrderStatus>().unwrap(),
            OrderStatus::Cancelled
        );

        // Partial variations
        assert_eq!(
            "partial".parse::<OrderStatus>().unwrap(),
            OrderStatus::Partial
        );
        assert_eq!(
            "partially_filled".parse::<OrderStatus>().unwrap(),
            OrderStatus::Partial
        );
    }

    #[test]
    fn test_is_terminal() {
        assert!(OrderStatus::Complete.is_terminal());
        assert!(OrderStatus::Cancelled.is_terminal());
        assert!(OrderStatus::Rejected.is_terminal());
        assert!(!OrderStatus::Open.is_terminal());
        assert!(!OrderStatus::Pending.is_terminal());
        assert!(!OrderStatus::Partial.is_terminal());
        assert!(!OrderStatus::Unknown.is_terminal());
    }

    #[test]
    fn test_is_active() {
        assert!(OrderStatus::Open.is_active());
        assert!(OrderStatus::Pending.is_active());
        assert!(OrderStatus::Partial.is_active());
        assert!(!OrderStatus::Complete.is_active());
        assert!(!OrderStatus::Cancelled.is_active());
        assert!(!OrderStatus::Rejected.is_active());
        assert!(!OrderStatus::Unknown.is_active());
    }

    #[test]
    fn test_unknown_status() {
        assert_eq!("".parse::<OrderStatus>().unwrap(), OrderStatus::Unknown);
        assert_eq!(
            "foobar".parse::<OrderStatus>().unwrap(),
            OrderStatus::Unknown
        );
    }

    #[test]
    fn test_roundtrip() {
        // Verify that Display output can be parsed back to the same variant
        for status in [
            OrderStatus::Open,
            OrderStatus::Complete,
            OrderStatus::Cancelled,
            OrderStatus::Pending,
            OrderStatus::Rejected,
            OrderStatus::Partial,
        ] {
            let s = status.to_string();
            let parsed: OrderStatus = s.parse().unwrap();
            assert_eq!(parsed, status);
        }
    }
}
