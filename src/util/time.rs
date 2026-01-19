//! Timestamp conversion utilities for Rithmic timestamps.
//!
//! Rithmic uses a two-part timestamp format:
//! - `ssboe`: Seconds since the beginning of the Unix epoch (January 1, 1970 UTC)
//! - `usecs`: Microseconds within that second (0-999999)
//!
//! Some messages also include `nsecs` for nanosecond precision from the exchange.

/// Convert Rithmic timestamp to Unix nanoseconds.
///
/// Rithmic timestamps consist of:
/// - `ssboe`: Seconds since Unix epoch (January 1, 1970 UTC)
/// - `usecs`: Microseconds within that second (0-999999)
///
/// # Arguments
/// * `ssboe` - Seconds since beginning of epoch (must be non-negative)
/// * `usecs` - Microseconds within the second (must be non-negative, typically 0-999999)
///
/// # Returns
/// Unix timestamp in nanoseconds
///
/// # Panics
/// Debug builds will panic if either argument is negative.
///
/// # Note
/// Both arguments are expected to be non-negative values from the Rithmic protocol.
/// Negative values indicate malformed data and will produce incorrect results
/// in release builds (due to unsigned wraparound).
///
/// # Example
/// ```
/// use rithmic_rs::rithmic_to_unix_nanos;
///
/// // Convert timestamp: 1704067200 seconds + 500000 microseconds
/// let nanos = rithmic_to_unix_nanos(1704067200, 500000);
/// assert_eq!(nanos, 1704067200_500_000_000);
/// ```
pub fn rithmic_to_unix_nanos(ssboe: i32, usecs: i32) -> u64 {
    debug_assert!(ssboe >= 0, "ssboe must be non-negative, got {}", ssboe);
    debug_assert!(usecs >= 0, "usecs must be non-negative, got {}", usecs);
    (ssboe as u64 * 1_000_000_000) + (usecs as u64 * 1_000)
}

/// Convert Rithmic timestamp to Unix nanoseconds with optional nanosecond precision.
///
/// This variant supports the additional nanosecond field provided by some messages
/// (like `LastTrade` and `DepthByOrder`) that include exchange-level nanosecond precision.
///
/// # Arguments
/// * `ssboe` - Seconds since beginning of epoch (must be non-negative)
/// * `usecs` - Microseconds within the second (must be non-negative, typically 0-999999)
/// * `nsecs` - Optional nanoseconds to add (must be non-negative if provided, typically 0-999)
///
/// # Returns
/// Unix timestamp in nanoseconds
///
/// # Panics
/// Debug builds will panic if any argument is negative.
///
/// # Note
/// All arguments are expected to be non-negative values from the Rithmic protocol.
/// Negative values indicate malformed data and will produce incorrect results
/// in release builds (due to unsigned wraparound).
///
/// # Example
/// ```
/// use rithmic_rs::rithmic_to_unix_nanos_precise;
///
/// // Without nanoseconds (same as rithmic_to_unix_nanos)
/// let nanos = rithmic_to_unix_nanos_precise(1704067200, 500000, None);
/// assert_eq!(nanos, 1704067200_500_000_000);
///
/// // With nanoseconds from exchange
/// let nanos = rithmic_to_unix_nanos_precise(1704067200, 500000, Some(123));
/// assert_eq!(nanos, 1704067200_500_000_123);
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
        // Test basic conversion
        assert_eq!(rithmic_to_unix_nanos(1, 0), 1_000_000_000);
        assert_eq!(rithmic_to_unix_nanos(1, 1), 1_000_001_000);
        assert_eq!(rithmic_to_unix_nanos(1, 999999), 1_999_999_000);

        // Test realistic timestamp
        let nanos = rithmic_to_unix_nanos(1704067200, 500000);
        assert_eq!(nanos, 1704067200_500_000_000);
    }

    #[test]
    fn test_rithmic_to_unix_nanos_precise() {
        // Without nsecs
        assert_eq!(rithmic_to_unix_nanos_precise(1, 0, None), 1_000_000_000);

        // With nsecs
        assert_eq!(rithmic_to_unix_nanos_precise(1, 0, Some(123)), 1_000_000_123);
        assert_eq!(
            rithmic_to_unix_nanos_precise(1, 500000, Some(456)),
            1_500_000_456
        );
    }
}
