use std::time::Duration;
use tokio::time::Instant;
use tracing::warn;

/// Manages heartbeat timeout detection for plant actors.
///
/// This manager tracks pending heartbeat requests and detects when responses
/// do not arrive within the configured timeout period. It is used internally
/// by plant actors to monitor connection health when heartbeat responses are
/// enabled via `return_heartbeat_response(true)`.
///
/// ## Behavior
///
/// - Tracks at most one pending heartbeat at a time
/// - If a new heartbeat is sent before the previous one times out or responds,
///   the previous pending heartbeat is replaced (a warning is logged)
/// - Timeout detection is performed in the plant's tokio select! loop
/// - When a timeout occurs, a `HeartbeatTimeout` error is sent to subscribers
///
/// ## Timeout Configuration
///
/// The timeout duration is set to 30 seconds by default (`HEARTBEAT_TIMEOUT_SECS`),
/// which is half the heartbeat interval (60 seconds). This provides a reasonable
/// balance between detecting connection issues and avoiding false positives from
/// transient network delays.
#[derive(Debug)]
pub struct HeartbeatManager {
    /// Pending heartbeat waiting for response
    pending: Option<PendingHeartbeat>,
    /// Timeout duration in seconds
    timeout_secs: u64,
}

#[derive(Debug)]
struct PendingHeartbeat {
    sent_at: Instant,
    request_id: String,
}

impl HeartbeatManager {
    /// Create a new manager with the given timeout in seconds
    pub fn new(timeout_secs: u64) -> Self {
        Self {
            pending: None,
            timeout_secs,
        }
    }

    /// Register that we sent a heartbeat expecting a response.
    ///
    /// If there is already a pending heartbeat waiting for a response,
    /// it will be replaced and a warning will be logged. This can occur
    /// in high-latency scenarios where responses are delayed beyond the
    /// heartbeat interval.
    pub fn sent(&mut self, request_id: String) {
        if let Some(old) = self.pending.replace(PendingHeartbeat {
            sent_at: Instant::now(),
            request_id: request_id.clone(),
        }) {
            warn!(
                "Replacing pending heartbeat {} with {}",
                old.request_id, request_id
            );
        }
    }

    /// Mark that we received a response for a heartbeat.
    ///
    /// Returns `true` if the response matches the pending heartbeat's request ID
    /// and clears the pending state. Returns `false` if there is no pending
    /// heartbeat or the IDs don't match.
    pub fn received(&mut self, response_id: &str) -> bool {
        if let Some(pending) = &self.pending {
            if pending.request_id == response_id {
                self.pending = None;
                return true;
            }
        }
        false
    }

    /// Check if the pending heartbeat has timed out.
    ///
    /// Returns `Some(request_id)` if the pending heartbeat has exceeded the
    /// timeout duration, and clears the pending state. Returns `None` if
    /// there is no pending heartbeat or it has not yet timed out.
    ///
    /// This method should be called when the timeout instant returned by
    /// `next_timeout_at()` is reached in the select! loop.
    pub fn check_timeout(&mut self) -> Option<String> {
        if let Some(pending) = &self.pending {
            if pending.sent_at.elapsed() > Duration::from_secs(self.timeout_secs) {
                return self.pending.take().map(|p| p.request_id);
            }
        }
        None
    }

    /// Get the next timeout instant for use in tokio select! loop.
    ///
    /// Returns `Some(Instant)` if there is a pending heartbeat, representing
    /// when the timeout will occur. Returns `None` if there is no pending
    /// heartbeat, which should be combined with `std::future::pending()` in
    /// the select! branch to avoid busy-waiting.
    pub fn next_timeout_at(&self) -> Option<Instant> {
        self.pending
            .as_ref()
            .map(|p| p.sent_at + Duration::from_secs(self.timeout_secs))
    }
}
