//! Caller-owned, one-use permission to hand a mutation to the native writer.

use std::fmt;

/// A synchronous, one-use claim for one mutation's native handoff.
///
/// The caller supplies an atomic claim of its original state, rather than a
/// readiness read. Returning `true` transfers the mutation to the native writer;
/// `false` returns [`crate::RithmicError::MutationHandoffRefused`] without starting
/// that mutation's native write. The callback runs after underlying sink
/// readiness and immediately before native `start_send`. It must not block.
///
/// A successful claim does not establish delivery or venue acceptance. A later
/// transport failure leaves the outcome unknown; the library does not retry the
/// mutation. Dropping a queued caller does not revoke its handoff. Revocation,
/// when wanted, must race the claim in the caller's original shared state.
///
/// Existing unguarded mutation methods remain available, including cancellation
/// and exit operations. No policy is imposed on them by this type.
#[must_use]
pub struct MutationHandoff {
    claim: Box<dyn FnOnce() -> bool + Send + 'static>,
}

impl MutationHandoff {
    /// Capture an atomic, nonblocking, one-use claim for this mutation.
    pub fn new(claim: impl FnOnce() -> bool + Send + 'static) -> Self {
        Self {
            claim: Box::new(claim),
        }
    }

    pub(crate) fn claim(self) -> bool {
        (self.claim)()
    }
}

impl fmt::Debug for MutationHandoff {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MutationHandoff").finish_non_exhaustive()
    }
}
