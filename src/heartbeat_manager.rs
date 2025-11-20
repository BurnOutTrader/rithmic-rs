use std::time::Duration;
use tokio::time::Instant;
use tracing::warn;

/// Simple manager to track heartbeat timeouts
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

    /// Register that we sent a heartbeat expecting a response
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

    /// Mark that we received a response
    pub fn received(&mut self, response_id: &str) -> bool {
        if let Some(pending) = &self.pending {
            if pending.request_id == response_id {
                self.pending = None;
                return true;
            }
        }
        false
    }

    /// Check if the pending heartbeat has timed out
    /// Returns the request_id if timeout occurred
    pub fn check_timeout(&mut self) -> Option<String> {
        if let Some(pending) = &self.pending {
            if pending.sent_at.elapsed() > Duration::from_secs(self.timeout_secs) {
                return self.pending.take().map(|p| p.request_id);
            }
        }
        None
    }

    /// Get the next timeout instant, or None if no pending heartbeat
    pub fn next_timeout_at(&self) -> Option<Instant> {
        self.pending
            .as_ref()
            .map(|p| p.sent_at + Duration::from_secs(self.timeout_secs))
    }
}
