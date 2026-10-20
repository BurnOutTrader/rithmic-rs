// Copyright (c) 2026 Kevin Monaghan
// Licensed under MIT OR Apache-2.0, the same terms as this crate.

//! Redacted credential owners and login frames erased when their last owner drops.

use std::{
    fmt,
    hash::{Hash, Hasher},
    ops::Deref,
};
use zeroize::{Zeroize, Zeroizing};

/// A password whose owned allocation is erased on replacement and drop.
#[derive(Clone, Default)]
pub struct Password {
    value: Zeroizing<String>,
    #[cfg(test)]
    erased: Option<std::sync::Arc<std::sync::atomic::AtomicUsize>>,
}

impl Password {
    /// Takes ownership of a password without retaining an unprotected copy.
    pub fn new(value: impl Into<String>) -> Self {
        Self {
            value: Zeroizing::new(value.into()),
            #[cfg(test)]
            erased: None,
        }
    }

    /// Borrows the plaintext only for authentication or explicit inspection.
    pub fn expose(&self) -> &str {
        &self.value
    }

    /// Borrows the protected string in the representation required by the protobuf encoder.
    pub(crate) fn as_string(&self) -> &String {
        &self.value
    }

    #[cfg(test)]
    pub(crate) fn observed(
        value: &str,
        erased: &std::sync::Arc<std::sync::atomic::AtomicUsize>,
    ) -> Self {
        Self {
            value: Zeroizing::new(value.to_owned()),
            erased: Some(erased.clone()),
        }
    }
}

impl PartialEq for Password {
    fn eq(&self, other: &Self) -> bool {
        self.value == other.value
    }
}

impl Eq for Password {}

impl fmt::Debug for Password {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("[REDACTED]")
    }
}

impl Hash for Password {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.value.hash(state);
    }
}

impl Zeroize for Password {
    fn zeroize(&mut self) {
        self.value.zeroize();
    }
}

impl Drop for Password {
    fn drop(&mut self) {
        self.zeroize();
        #[cfg(test)]
        if let Some(erased) = &self.erased {
            if self.expose().is_empty() {
                erased.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }
        }
    }
}

/// A serialized login allocation erased after its final shared byte owner drops.
///
/// Transport and TLS implementations may make additional copies outside this owner.
pub struct LoginFrame {
    value: Zeroizing<Vec<u8>>,
    #[cfg(test)]
    erased: Option<std::sync::Arc<std::sync::atomic::AtomicUsize>>,
}

impl LoginFrame {
    pub(crate) fn new(value: Zeroizing<Vec<u8>>) -> Self {
        Self {
            value,
            #[cfg(test)]
            erased: None,
        }
    }

    #[cfg(test)]
    fn observed(value: &[u8], erased: &std::sync::Arc<std::sync::atomic::AtomicUsize>) -> Self {
        Self {
            value: Zeroizing::new(value.to_vec()),
            erased: Some(erased.clone()),
        }
    }

    /// Transfers the protected allocation to shared transport bytes without copying it.
    ///
    /// Cloned byte views retain this owner until their final drop.
    pub fn into_bytes(self) -> bytes::Bytes {
        bytes::Bytes::from_owner(self)
    }
}

impl AsRef<[u8]> for LoginFrame {
    fn as_ref(&self) -> &[u8] {
        &self.value
    }
}

impl Deref for LoginFrame {
    type Target = [u8];
    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

impl Drop for LoginFrame {
    fn drop(&mut self) {
        self.value.zeroize();
        #[cfg(test)]
        if let Some(erased) = &self.erased {
            if self.value.is_empty() {
                erased.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }
        }
    }
}

impl fmt::Debug for LoginFrame {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("LoginFrame([REDACTED])")
    }
}

/// An outbound frame whose contents never appear in plant diagnostics.
pub(crate) struct OutboundFrame(bytes::Bytes);

impl OutboundFrame {
    /// Transfers the frame while retaining any protected allocation owner.
    pub(crate) fn into_bytes(self) -> bytes::Bytes {
        self.0
    }
}

impl From<Vec<u8>> for OutboundFrame {
    fn from(value: Vec<u8>) -> Self {
        Self(value.into())
    }
}

impl From<LoginFrame> for OutboundFrame {
    fn from(value: LoginFrame) -> Self {
        Self(value.into_bytes())
    }
}

impl Deref for OutboundFrame {
    type Target = [u8];
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl fmt::Debug for OutboundFrame {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("OutboundFrame([REDACTED])")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    #[test]
    fn password_debug_is_redacted_and_each_clone_erases_before_drop() {
        let erased = Arc::new(AtomicUsize::new(0));
        let password = Password::observed("sdk-secret", &erased);
        let copy = password.clone();
        assert_eq!(format!("{password:?}"), "[REDACTED]");
        assert_eq!(copy.expose(), "sdk-secret");
        drop(password);
        assert_eq!(erased.load(Ordering::SeqCst), 1);
        assert_eq!(copy.expose(), "sdk-secret");
        drop(copy);
        assert_eq!(erased.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn explicit_password_erasure_preserves_a_valid_empty_owner() {
        let mut password = Password::new("sdk-secret");
        password.zeroize();
        assert_eq!(password.expose(), "");
        assert_eq!(format!("{password:?}"), "[REDACTED]");
    }

    #[test]
    fn login_bytes_erase_only_when_the_last_shared_view_drops() {
        let erased = Arc::new(AtomicUsize::new(0));
        let frame = LoginFrame::observed(b"sdk-secret", &erased);
        assert_eq!(format!("{frame:?}"), "LoginFrame([REDACTED])");
        let bytes = frame.into_bytes();
        let original_address = bytes.as_ptr();
        let copy = bytes.clone();
        assert_eq!(copy.as_ptr(), original_address);
        assert_eq!(copy.as_ref(), b"sdk-secret");
        drop(bytes);
        assert_eq!(erased.load(Ordering::SeqCst), 0);
        drop(copy);
        assert_eq!(erased.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn cancelling_a_queued_login_write_erases_its_frame_owner() {
        let erased = Arc::new(AtomicUsize::new(0));
        let frame = LoginFrame::observed(b"sdk-secret", &erased);
        let queued_write = async move {
            let frame = frame.into_bytes();
            std::future::pending::<()>().await;
            drop(frame);
        };
        drop(queued_write);
        assert_eq!(erased.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn outbound_diagnostics_never_format_login_bytes() {
        let frame = LoginFrame::new(Zeroizing::new(b"sdk-secret".to_vec()));
        let frame = OutboundFrame::from(frame);
        assert_eq!(format!("{frame:?}"), "OutboundFrame([REDACTED])");
        assert_eq!(&frame[..], b"sdk-secret");
    }
}
