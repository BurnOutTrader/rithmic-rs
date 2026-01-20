//! Timestamp conversion utilities for Rithmic's two-part timestamp format.
//!
//! Rithmic uses `ssboe` (seconds since Unix epoch) and `usecs` (microseconds).
//! Some messages also include `nsecs` for nanosecond precision from the exchange.

/// Convert Rithmic timestamp (ssboe + usecs) to Unix nanoseconds.
///
/// # Example
/// ```
/// use rithmic_rs::rithmic_to_unix_nanos;
///
/// let nanos = rithmic_to_unix_nanos(1_704_067_200, 500_000);
/// assert_eq!(nanos, 1_704_067_200_500_000_000);
/// ```
pub fn rithmic_to_unix_nanos(ssboe: i32, usecs: i32) -> u64 {
    debug_assert!(ssboe >= 0, "ssboe must be non-negative, got {}", ssboe);
    debug_assert!(usecs >= 0, "usecs must be non-negative, got {}", usecs);
    (ssboe as u64 * 1_000_000_000) + (usecs as u64 * 1_000)
}

/// Convert Rithmic timestamp to Unix nanoseconds with optional nanosecond precision.
///
/// Use this variant for messages that include exchange-level nanosecond timestamps.
///
/// # Example
/// ```
/// use rithmic_rs::rithmic_to_unix_nanos_precise;
///
/// // With nanoseconds from exchange
/// let nanos = rithmic_to_unix_nanos_precise(1_704_067_200, 500_000, Some(123));
/// assert_eq!(nanos, 1_704_067_200_500_000_123);
/// ```
pub fn rithmic_to_unix_nanos_precise(ssboe: i32, usecs: i32, nsecs: Option<i32>) -> u64 {
    debug_assert!(ssboe >= 0, "ssboe must be non-negative, got {}", ssboe);
    debug_assert!(usecs >= 0, "usecs must be non-negative, got {}", usecs);
    if let Some(ns) = nsecs {
        debug_assert!(ns >= 0, "nsecs must be non-negative, got {}", ns);
    }
    let base = (ssboe as u64 * 1_000_000_000) + (usecs as u64 * 1_000);
    match nsecs {
        Some(ns) => base + (ns as u64),
        None => base,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rithmic_to_unix_nanos() {
        assert_eq!(rithmic_to_unix_nanos(1, 0), 1_000_000_000);
        assert_eq!(rithmic_to_unix_nanos(1, 1), 1_000_001_000);
        assert_eq!(rithmic_to_unix_nanos(1, 999999), 1_999_999_000);
        assert_eq!(
            rithmic_to_unix_nanos(1_704_067_200, 500_000),
            1_704_067_200_500_000_000
        );
    }

    #[test]
    fn test_rithmic_to_unix_nanos_precise() {
        assert_eq!(rithmic_to_unix_nanos_precise(1, 0, None), 1_000_000_000);
        assert_eq!(
            rithmic_to_unix_nanos_precise(1, 0, Some(123)),
            1_000_000_123
        );
        assert_eq!(
            rithmic_to_unix_nanos_precise(1, 500000, Some(456)),
            1_500_000_456
        );
    }
}
