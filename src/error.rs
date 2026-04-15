use std::fmt;

/// A non-transport request rejection reported by Rithmic via `rp_code`.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct RithmicRequestError {
    /// The raw `rp_code` payload exactly as received from Rithmic.
    pub rp_code: Vec<String>,
    /// The first `rp_code` element, when present.
    pub code: Option<String>,
    /// The second `rp_code` element when present, otherwise the first element.
    pub message: String,
}

impl fmt::Display for RithmicRequestError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.code.as_deref() {
            Some(code) if !code.is_empty() => write!(f, "[{code}] {}", self.message),
            _ => write!(f, "{}", self.message),
        }
    }
}

/// Typed errors returned by all plant handle methods.
///
/// ```ignore
/// match handle.subscribe("ESH6", "CME").await {
///     Ok(resp) => { /* success */ }
///     Err(RithmicError::ConnectionClosed | RithmicError::SendFailed) => {
///         handle.abort();
///         // reconnect — see examples/reconnect.rs
///     }
///     Err(RithmicError::RequestRejected(err)) => {
///         eprintln!("request rejected {}: {}", err.code.as_deref().unwrap_or("?"), err.message);
///     }
///     Err(RithmicError::ProtocolError(msg)) => eprintln!("protocol error: {msg}"),
///     Err(RithmicError::InvalidArgument(msg)) => eprintln!("bad local input: {msg}"),
///     Err(e) => eprintln!("{e}"),
/// }
/// ```
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum RithmicError {
    /// WebSocket connection could not be established.
    ConnectionFailed(String),
    /// The plant's WebSocket connection is gone; pending requests will never complete.
    ConnectionClosed,
    /// WebSocket send failed or timed out after the request was registered.
    ///
    /// Treat this as a connection-health failure for the current request path and
    /// trigger your reconnection logic if the request is required for continued
    /// trading. This error alone does not prove that the actor has already shut
    /// down; keep-alive failure detection can still emit a synthetic
    /// [`crate::rti::messages::RithmicMessage::HeartbeatTimeout`] or
    /// [`crate::rti::messages::RithmicMessage::ConnectionError`] update if the
    /// connection is actually dead. A successful `disconnect()` on a plant
    /// handle does not emit those synthetic health events.
    SendFailed,
    /// Server returned an empty response where at least one was expected.
    EmptyResponse,
    /// Non-transport request rejection from Rithmic.
    ///
    /// This preserves the server rejection text and the raw `rp_code` when one
    /// is supplied. It is a request/business outcome, not a transport failure.
    /// Do not treat this alone as evidence that the plant disconnected or
    /// should be reconnected.
    RequestRejected(RithmicRequestError),
    /// Non-transport protocol/receiver failure without an `rp_code` rejection.
    ///
    /// This covers malformed or unexpected responses that should fail the
    /// current request path but do not indicate a dead connection.
    ProtocolError(String),
    /// Invalid request input.
    ///
    /// This is reserved for local argument validation before a request is sent.
    InvalidArgument(String),
}

impl fmt::Display for RithmicError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            RithmicError::ConnectionFailed(msg) => write!(f, "connection failed: {msg}"),
            RithmicError::ConnectionClosed => write!(f, "connection closed"),
            RithmicError::SendFailed => write!(f, "WebSocket send failed or timed out"),
            RithmicError::EmptyResponse => write!(f, "empty response"),
            RithmicError::RequestRejected(err) => write!(f, "request rejected: {err}"),
            RithmicError::ProtocolError(msg) => write!(f, "protocol error: {msg}"),
            RithmicError::InvalidArgument(msg) => write!(f, "invalid argument: {msg}"),
        }
    }
}

impl std::error::Error for RithmicError {}
