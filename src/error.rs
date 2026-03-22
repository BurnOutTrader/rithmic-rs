use std::fmt;

/// Error type returned by plant handle methods.
///
/// Enables consumers to programmatically distinguish error kinds without
/// parsing strings.
///
/// # Example
///
/// ```ignore
/// match handle.subscribe("ESH6", "CME").await {
///     Ok(resp) => { /* success */ }
///     Err(RithmicError::ConnectionClosed) => { handle.abort(); /* reconnect */ }
///     Err(RithmicError::SendFailed) => { handle.abort(); /* reconnect */ }
///     Err(RithmicError::ServerError(msg)) => { eprintln!("Server: {}", msg); }
///     Err(e) => { eprintln!("Error: {}", e); }
/// }
/// ```
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum RithmicError {
    /// The plant actor has shut down; the response channel was dropped.
    ConnectionClosed,
    /// The WebSocket send failed after the request was registered.
    SendFailed,
    /// The server returned an empty response vector.
    EmptyResponse,
    /// The Rithmic server returned a protocol-level error (rp_code text).
    ServerError(String),
}

impl fmt::Display for RithmicError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            RithmicError::ConnectionClosed => write!(f, "Connection closed"),
            RithmicError::SendFailed => write!(f, "WebSocket send failed"),
            RithmicError::EmptyResponse => write!(f, "Empty response"),
            RithmicError::ServerError(msg) => write!(f, "Server error: {}", msg),
        }
    }
}

impl std::error::Error for RithmicError {}
