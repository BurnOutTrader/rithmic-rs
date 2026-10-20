// Copyright (c) 2026 Kevin Monaghan
// Licensed under MIT OR Apache-2.0, the same terms as this crate.

//! Login wire fields whose password remains a protected owner through decoding.

use crate::secret::Password;
use prost::{
    Message,
    bytes::{Buf, BufMut},
    encoding::{self, DecodeContext, WireType},
};

/// Rithmic login fields, preserving the native protobuf tags and redacting credentials.
#[non_exhaustive]
#[derive(Clone, Default, PartialEq, Eq, Hash)]
pub struct RequestLogin {
    /// Native `template_id` login field.
    pub template_id: i32,
    /// Native `template_version` login field.
    pub template_version: Option<String>,
    /// Native `user_msg` login field.
    pub user_msg: Vec<String>,
    /// Native `user` login field.
    pub user: Option<String>,
    /// Native `password` login field.
    pub password: Option<Password>,
    /// Native `app_name` login field.
    pub app_name: Option<String>,
    /// Native `app_version` login field.
    pub app_version: Option<String>,
    /// Native `system_name` login field.
    pub system_name: Option<String>,
    /// Native `infra_type` login field.
    pub infra_type: Option<i32>,
    /// Native `mac_addr` login field.
    pub mac_addr: Vec<String>,
    /// Native `os_version` login field.
    pub os_version: Option<String>,
    /// Native `os_platform` login field.
    pub os_platform: Option<String>,
    /// Native `aggregated_quotes` login field.
    pub aggregated_quotes: Option<bool>,
}

impl std::fmt::Debug for RequestLogin {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RequestLogin")
            .field("template_id", &self.template_id)
            .field("password", &"[REDACTED]")
            .finish_non_exhaustive()
    }
}

impl Message for RequestLogin {
    fn encode_raw(&self, buf: &mut impl BufMut) {
        encoding::int32::encode(154467, &self.template_id, buf);
        if let Some(value) = &self.template_version {
            encoding::string::encode(153634, value, buf);
        }
        encoding::string::encode_repeated(132760, &self.user_msg, buf);
        if let Some(value) = &self.user {
            encoding::string::encode(131003, value, buf);
        }
        if let Some(value) = &self.password {
            encoding::string::encode(130004, value.as_string(), buf);
        }
        if let Some(value) = &self.app_name {
            encoding::string::encode(130002, value, buf);
        }
        if let Some(value) = &self.app_version {
            encoding::string::encode(131803, value, buf);
        }
        if let Some(value) = &self.system_name {
            encoding::string::encode(153628, value, buf);
        }
        if let Some(value) = &self.infra_type {
            encoding::int32::encode(153621, value, buf);
        }
        encoding::string::encode_repeated(144108, &self.mac_addr, buf);
        if let Some(value) = &self.os_version {
            encoding::string::encode(144021, value, buf);
        }
        if let Some(value) = &self.os_platform {
            encoding::string::encode(144020, value, buf);
        }
        if let Some(value) = &self.aggregated_quotes {
            encoding::bool::encode(153644, value, buf);
        }
    }

    fn merge_field(
        &mut self,
        tag: u32,
        wire_type: WireType,
        buf: &mut impl Buf,
        ctx: DecodeContext,
    ) -> Result<(), prost::DecodeError> {
        match tag {
            154467 => encoding::int32::merge(wire_type, &mut self.template_id, buf, ctx),
            153634 => encoding::string::merge(
                wire_type,
                self.template_version.get_or_insert_with(Default::default),
                buf,
                ctx,
            ),
            132760 => encoding::string::merge_repeated(wire_type, &mut self.user_msg, buf, ctx),
            131003 => encoding::string::merge(
                wire_type,
                self.user.get_or_insert_with(Default::default),
                buf,
                ctx,
            ),
            130004 => {
                let mut value = zeroize::Zeroizing::new(String::new());
                encoding::string::merge(wire_type, &mut value, buf, ctx)?;
                self.password = Some(Password::new(std::mem::take(&mut *value)));
                Ok(())
            }
            130002 => encoding::string::merge(
                wire_type,
                self.app_name.get_or_insert_with(Default::default),
                buf,
                ctx,
            ),
            131803 => encoding::string::merge(
                wire_type,
                self.app_version.get_or_insert_with(Default::default),
                buf,
                ctx,
            ),
            153628 => encoding::string::merge(
                wire_type,
                self.system_name.get_or_insert_with(Default::default),
                buf,
                ctx,
            ),
            153621 => encoding::int32::merge(
                wire_type,
                self.infra_type.get_or_insert_with(Default::default),
                buf,
                ctx,
            ),
            144108 => encoding::string::merge_repeated(wire_type, &mut self.mac_addr, buf, ctx),
            144021 => encoding::string::merge(
                wire_type,
                self.os_version.get_or_insert_with(Default::default),
                buf,
                ctx,
            ),
            144020 => encoding::string::merge(
                wire_type,
                self.os_platform.get_or_insert_with(Default::default),
                buf,
                ctx,
            ),
            153644 => encoding::bool::merge(
                wire_type,
                self.aggregated_quotes.get_or_insert_with(Default::default),
                buf,
                ctx,
            ),
            _ => encoding::skip_field(wire_type, tag, buf, ctx),
        }
    }

    fn encoded_len(&self) -> usize {
        encoding::int32::encoded_len(154467, &self.template_id)
            + self
                .template_version
                .as_ref()
                .map_or(0, |value| encoding::string::encoded_len(153634, value))
            + encoding::string::encoded_len_repeated(132760, &self.user_msg)
            + self
                .user
                .as_ref()
                .map_or(0, |value| encoding::string::encoded_len(131003, value))
            + self.password.as_ref().map_or(0, |value| {
                encoding::string::encoded_len(130004, value.as_string())
            })
            + self
                .app_name
                .as_ref()
                .map_or(0, |value| encoding::string::encoded_len(130002, value))
            + self
                .app_version
                .as_ref()
                .map_or(0, |value| encoding::string::encoded_len(131803, value))
            + self
                .system_name
                .as_ref()
                .map_or(0, |value| encoding::string::encoded_len(153628, value))
            + self
                .infra_type
                .as_ref()
                .map_or(0, |value| encoding::int32::encoded_len(153621, value))
            + encoding::string::encoded_len_repeated(144108, &self.mac_addr)
            + self
                .os_version
                .as_ref()
                .map_or(0, |value| encoding::string::encoded_len(144021, value))
            + self
                .os_platform
                .as_ref()
                .map_or(0, |value| encoding::string::encoded_len(144020, value))
            + self
                .aggregated_quotes
                .as_ref()
                .map_or(0, |value| encoding::bool::encoded_len(153644, value))
    }

    fn clear(&mut self) {
        *self = Self::default();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    const LOGIN: &[u8] = &[
        152, 182, 75, 10, 162, 189, 63, 10, b's', b'd', b'k', b'-', b's', b'e', b'c', b'r', b'e',
        b't',
    ];

    #[test]
    fn native_login_password_tag_decodes_and_reencodes_unchanged_with_redacted_debug() {
        let login = RequestLogin::decode(LOGIN).unwrap();
        assert_eq!(login.template_id, 10);
        assert_eq!(login.password.as_ref().unwrap().expose(), "sdk-secret");
        assert_eq!(
            format!("{login:?}"),
            "RequestLogin { template_id: 10, password: \"[REDACTED]\", .. }"
        );
        assert_eq!(login.encode_to_vec(), LOGIN);
    }

    #[test]
    fn protobuf_clear_and_repeated_password_fields_erase_the_replaced_owner() {
        let erased = Arc::new(AtomicUsize::new(0));
        let mut login = RequestLogin {
            template_id: 10,
            password: Some(Password::observed("old-secret", &erased)),
            ..Default::default()
        };
        login.merge(LOGIN).unwrap();
        assert_eq!(erased.load(Ordering::SeqCst), 1);
        assert_eq!(login.password.as_ref().unwrap().expose(), "sdk-secret");
        login.password = Some(Password::observed("old-secret", &erased));
        login.clear();
        assert_eq!(erased.load(Ordering::SeqCst), 2);
        assert_eq!(login.password, None);
        assert_eq!(login.template_id, 0);
    }

    #[test]
    fn a_truncated_password_refuses_decode_and_preserves_the_prior_protected_owner() {
        let erased = Arc::new(AtomicUsize::new(0));
        let mut login = RequestLogin {
            password: Some(Password::observed("old-secret", &erased)),
            ..Default::default()
        };
        assert!(login.merge(&[162, 189, 63, 10, b'x'][..]).is_err());
        assert_eq!(login.password.as_ref().unwrap().expose(), "old-secret");
        assert_eq!(erased.load(Ordering::SeqCst), 0);
        drop(login);
        assert_eq!(erased.load(Ordering::SeqCst), 1);
    }
}
