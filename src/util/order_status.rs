//! Order status types and utilities.

use std::convert::Infallible;
use std::fmt;
use std::str::FromStr;

pub const OPEN: &str = "open";
pub const COMPLETE: &str = "complete";
pub const CANCELLED: &str = "cancelled";
pub const PENDING: &str = "pending";
pub const REJECTED: &str = "rejected";
pub const PARTIAL: &str = "partial";

/// Order status with helpers for checking terminal/active states.
///
/// Parses case-insensitively and handles common variations like "filled" for Complete
/// and "canceled" (US spelling) for Cancelled.
///
/// # Example
/// ```
/// use rithmic_rs::OrderStatus;
///
/// let status: OrderStatus = "filled".parse().unwrap();
/// assert_eq!(status, OrderStatus::Complete);
/// assert!(status.is_terminal());
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum OrderStatus {
    Open,
    Complete,
    Cancelled,
    Pending,
    Rejected,
    Partial,
    #[default]
    Unknown,
}

impl OrderStatus {
    /// Returns true if this is a terminal status (Complete, Cancelled, Rejected).
    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Complete | Self::Cancelled | Self::Rejected)
    }

    /// Returns true if this is an active status (Open, Pending, Partial).
    pub fn is_active(&self) -> bool {
        matches!(self, Self::Open | Self::Pending | Self::Partial)
    }
}

impl FromStr for OrderStatus {
    type Err = Infallible;

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
        assert_eq!(
            "complete".parse::<OrderStatus>().unwrap(),
            OrderStatus::Complete
        );
        assert_eq!(
            "filled".parse::<OrderStatus>().unwrap(),
            OrderStatus::Complete
        );

        assert_eq!(
            "cancelled".parse::<OrderStatus>().unwrap(),
            OrderStatus::Cancelled
        );
        assert_eq!(
            "canceled".parse::<OrderStatus>().unwrap(),
            OrderStatus::Cancelled
        );

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
