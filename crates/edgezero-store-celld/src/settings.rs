//! Pure structural validation. This module never reads the environment.

#![expect(
    clippy::arbitrary_source_item_ordering,
    reason = "keep each public settings shape beside its validator and redacted Debug implementation"
)]

use serde::Deserialize;
use std::collections::BTreeMap;
use std::fmt;
use url::{Host, Url};

use crate::PreparationError;

/// Explicit listener access policy, not a credential value.
#[derive(Clone, Deserialize, Eq, Hash, PartialEq)]
#[serde(tag = "mode", rename_all = "kebab-case", deny_unknown_fields)]
pub enum CelldAccess {
    TrustedNetwork {
        #[serde(default)]
        allow_remote_http: bool,
    },
    Authenticated {
        token_env: String,
    },
}

impl CelldAccess {
    /// Selected bootstrap variable, without looking it up.
    #[inline]
    #[must_use]
    pub fn token_env(&self) -> Option<&str> {
        match self {
            Self::Authenticated { token_env } => Some(token_env),
            Self::TrustedNetwork { .. } => None,
        }
    }
}

impl fmt::Debug for CelldAccess {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CelldAccess").finish_non_exhaustive()
    }
}

/// One complete remote document selected by a service-side store alias.
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CelldDocument {
    pub store: String,
    pub key: String,
}

impl fmt::Debug for CelldDocument {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CelldDocument").finish_non_exhaustive()
    }
}

/// Explicit local lookup mappings to complete UTF-8 documents.
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CelldConfigSettings {
    pub endpoint: String,
    pub access: CelldAccess,
    pub default_key: String,
    pub documents: BTreeMap<String, CelldDocument>,
}

impl CelldConfigSettings {
    /// Validates every mapping without credentials, runtime or network access.
    ///
    /// # Errors
    /// Rejects invalid endpoint, access policy, default, lookup key or target.
    #[inline]
    pub fn validate(&self) -> Result<(), PreparationError> {
        endpoint(&self.endpoint, &self.access)?;
        if self.documents.is_empty() || !self.documents.contains_key(&self.default_key) {
            return Err(PreparationError::InvalidBinding);
        }
        for (key, document) in &self.documents {
            if !valid_key(key, false) || key.trim().is_empty() {
                return Err(PreparationError::InvalidBinding);
            }
            if !valid_alias(&document.store) || !valid_key(&document.key, false) {
                return Err(PreparationError::InvalidResource);
            }
        }
        Ok(())
    }
}

impl fmt::Debug for CelldConfigSettings {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CelldConfigSettings")
            .finish_non_exhaustive()
    }
}

/// One live scoped store alias. No physical namespace is accepted.
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CelldKvSettings {
    pub endpoint: String,
    pub access: CelldAccess,
    pub store: String,
}

impl CelldKvSettings {
    /// Validates the binding without probing remote permissions.
    ///
    /// # Errors
    /// Rejects invalid endpoint, access policy or store alias.
    #[inline]
    pub fn validate(&self) -> Result<(), PreparationError> {
        endpoint(&self.endpoint, &self.access)?;
        if !valid_alias(&self.store) {
            return Err(PreparationError::InvalidResource);
        }
        Ok(())
    }
}

impl fmt::Debug for CelldKvSettings {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CelldKvSettings").finish_non_exhaustive()
    }
}

/// Captured application bearer token. Debug reveals neither value nor length.
#[derive(Clone)]
pub struct CelldAccessToken(String);

impl CelldAccessToken {
    /// Validates explicit bytes against the embedded service's token contract.
    ///
    /// # Errors
    /// Rejects tokens outside 32..=512 ASCII graphic bytes.
    #[inline]
    pub fn new(value: String) -> Result<Self, PreparationError> {
        if !(32..=512).contains(&value.len()) || !value.bytes().all(|byte| byte.is_ascii_graphic())
        {
            return Err(PreparationError::Credentials);
        }
        Ok(Self(value))
    }

    /// Exposes bytes only where explicitly needed for transport.
    #[inline]
    #[must_use]
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for CelldAccessToken {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("CelldAccessToken([redacted])")
    }
}

/// Aggregate preparation and independent live-operation bounds.
#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct CelldPreparationLimits {
    pub max_remote_values: u64,
    pub max_config_document_bytes: u64,
    pub max_snapshot_bytes: u64,
    pub max_metadata_bytes: u64,
    pub max_value_bytes: u64,
    pub max_page_bytes: u64,
    pub preparation_timeout_ms: u64,
    pub operation_timeout_ms: u64,
    pub connect_timeout_ms: u64,
}

impl Default for CelldPreparationLimits {
    #[inline]
    fn default() -> Self {
        Self {
            max_remote_values: 1_024,
            max_config_document_bytes: 0x0010_0000,
            max_snapshot_bytes: 0x0080_0000,
            max_metadata_bytes: 0x0010_0000,
            max_value_bytes: 0x0010_0000,
            max_page_bytes: 0x0020_0000,
            preparation_timeout_ms: 30_000,
            operation_timeout_ms: 5_000,
            connect_timeout_ms: 1_000,
        }
    }
}

impl CelldPreparationLimits {
    /// Checks positive, representable quotas and ordered finite deadlines.
    ///
    /// # Errors
    /// Returns a fixed category for invalid bounds or service-ceiling violations.
    #[inline]
    pub fn validate(&self) -> Result<(), PreparationError> {
        let quotas = [
            self.max_remote_values,
            self.max_config_document_bytes,
            self.max_snapshot_bytes,
            self.max_metadata_bytes,
            self.max_value_bytes,
            self.max_page_bytes,
        ];
        if quotas.iter().any(|value| *value == 0 || usize::try_from(*value).is_err())
            || self.max_value_bytes > 0x0010_0000
            || self.max_config_document_bytes > 0x0010_0000
            || self.max_config_document_bytes > self.max_snapshot_bytes
            || self.max_page_bytes > 0x0020_0000
            || self.connect_timeout_ms == 0
            || self.connect_timeout_ms > self.operation_timeout_ms
            || self.operation_timeout_ms > self.preparation_timeout_ms
            // Match the portable clock's finite seven-day horizon.
            || self.preparation_timeout_ms > 604_800_000
        {
            return Err(PreparationError::InvalidLimits);
        }
        Ok(())
    }
}

pub(crate) fn endpoint(value: &str, access: &CelldAccess) -> Result<Url, PreparationError> {
    if value.len() > 4_096
        || value
            .bytes()
            .any(|byte| byte.is_ascii_control() || byte.is_ascii_whitespace())
        || value.contains(['\\', '?', '#', '@'])
    {
        return Err(PreparationError::InvalidEndpoint);
    }
    let origin_suffix = value
        .strip_prefix("http://")
        .or_else(|| value.strip_prefix("https://"))
        .ok_or(PreparationError::InvalidEndpoint)?;
    let (authority, path) = origin_suffix.split_once('/').unwrap_or((origin_suffix, ""));
    if !path.is_empty() || authority.contains('%') || authority.ends_with(':') {
        return Err(PreparationError::InvalidEndpoint);
    }
    let parsed = Url::parse(value).map_err(|_error| PreparationError::InvalidEndpoint)?;
    if parsed.query().is_some()
        || parsed.fragment().is_some()
        || !parsed.username().is_empty()
        || parsed.password().is_some()
        || parsed.host_str().is_none()
        || parsed.port() == Some(0)
        || parsed.path() != "/"
    {
        return Err(PreparationError::InvalidEndpoint);
    }
    let raw_host = if authority.starts_with('[') {
        authority
            .split_once(']')
            .map(|(host, _port)| host)
            .and_then(|host| host.strip_prefix('['))
    } else {
        authority.split(':').next()
    }
    .ok_or(PreparationError::InvalidEndpoint)?;
    let loopback = match parsed.host() {
        Some(Host::Ipv4(ip)) => {
            if raw_host != ip.to_string() {
                return Err(PreparationError::InvalidEndpoint);
            }
            ip.is_loopback()
        }
        Some(Host::Ipv6(ip)) => {
            if raw_host != ip.to_string() {
                return Err(PreparationError::InvalidEndpoint);
            }
            ip.is_loopback()
        }
        Some(Host::Domain(host)) => {
            if host.ends_with('.') || host.is_empty() {
                return Err(PreparationError::InvalidEndpoint);
            }
            false
        }
        None => return Err(PreparationError::InvalidEndpoint),
    };
    match access {
        CelldAccess::Authenticated { token_env } => {
            if !valid_variable(token_env) {
                return Err(PreparationError::InvalidBinding);
            }
            if parsed.scheme() != "https" {
                return Err(PreparationError::InvalidEndpoint);
            }
        }
        CelldAccess::TrustedNetwork { allow_remote_http } => {
            if parsed.scheme() == "http" && !loopback && !allow_remote_http {
                return Err(PreparationError::InvalidEndpoint);
            }
        }
    }
    Ok(parsed)
}

pub(crate) fn valid_alias(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
}

pub(crate) fn valid_key(value: &str, empty: bool) -> bool {
    (empty || !value.is_empty()) && value.len() <= 512 && !value.chars().any(char::is_control)
}

fn valid_variable(value: &str) -> bool {
    let mut bytes = value.bytes();
    value.len() <= 128
        && bytes
            .next()
            .is_some_and(|byte| byte.is_ascii_alphabetic() || byte == b'_')
        && bytes.all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
}

#[cfg(test)]
mod tests {
    use super::{CelldAccess, CelldAccessToken, CelldKvSettings, CelldPreparationLimits, endpoint};
    use crate::PreparationError;

    #[test]
    fn endpoints_are_explicit_origins_not_normalized_paths_or_obfuscated_ips() {
        let trusted = CelldAccess::TrustedNetwork {
            allow_remote_http: false,
        };
        for good in [
            "http://127.0.0.1:7780",
            "http://[::1]:7780/",
            "https://stores.example",
        ] {
            endpoint(good, &trusted).expect("explicit loopback or TLS origin must be accepted");
        }
        for bad in [
            "http://localhost",
            "http://127.1",
            "http://2130706433",
            "http://127.0.0.1/a/..",
            "http://127.0.0.1//",
            "http://127.0.0.1?",
            "http://127.0.0.1#",
            "http://user@127.0.0.1",
            "http://127.0.0.1\\x",
            " http://127.0.0.1",
            "http://private.example",
            "http://127.0.0.1:0",
            "http://127.0.0.1:",
            "https://stores.example.",
        ] {
            endpoint(bad, &trusted).expect_err("invalid endpoint origin must be rejected");
        }
        let remote = CelldAccess::TrustedNetwork {
            allow_remote_http: true,
        };
        endpoint("http://private.example", &remote)
            .expect("remote HTTP origin with explicit opt-in must be accepted");
        let authenticated = CelldAccess::Authenticated {
            token_env: "TOKEN".to_owned(),
        };
        endpoint("http://127.0.0.1", &authenticated)
            .expect_err("authenticated access must reject plaintext HTTP");
        endpoint("https://stores.example", &authenticated)
            .expect("authenticated TLS origin must be accepted");
    }

    #[test]
    fn config_and_kv_value_caps_are_independent() {
        let limits = CelldPreparationLimits {
            max_value_bytes: 1,
            ..CelldPreparationLimits::default()
        };
        limits
            .validate()
            .expect("lower live KV cap does not constrain config snapshots");
        assert_eq!(
            CelldPreparationLimits {
                max_config_document_bytes: 0x0010_0001,
                ..limits
            }
            .validate()
            .expect_err("config retains its independent service ceiling"),
            PreparationError::InvalidLimits
        );
    }

    #[test]
    fn tokens_match_service_bounds_and_debug_is_fully_redacted() {
        for size in [0, 1, 31, 513] {
            CelldAccessToken::new("x".repeat(size))
                .expect_err("token length outside service bounds must be rejected");
        }
        for value in [" ".repeat(32), "\n".repeat(32), "\u{e9}".repeat(32)] {
            CelldAccessToken::new(value)
                .expect_err("token with non-graphic ASCII bytes must be rejected");
        }
        let token = CelldAccessToken::new("s".repeat(32)).unwrap();
        assert_eq!(format!("{token:?}"), "CelldAccessToken([redacted])");
        assert_eq!(token.clone().expose().len(), 32);
        CelldAccessToken::new("x".repeat(512))
            .expect("maximum-length graphic ASCII token must be accepted");
    }

    #[test]
    fn settings_and_limits_reject_unknown_or_mixed_fields() {
        for access in [
            r#"{"mode":"trusted-network","token_env":"TOKEN"}"#,
            r#"{"mode":"authenticated","token_env":"TOKEN","allow_remote_http":true}"#,
            "{}",
        ] {
            serde_json::from_str::<CelldAccess>(access)
                .expect_err("mixed or missing access mode fields must be rejected");
        }
        let settings: CelldKvSettings = serde_json::from_str(r#"{"endpoint":"http://127.0.0.1","store":"Case_Sensitive-1","access":{"mode":"trusted-network"}}"#).unwrap();
        settings.validate().unwrap();
        assert_eq!(settings.access.token_env(), None);
        serde_json::from_str::<CelldPreparationLimits>(r#"{"unknown":1}"#)
            .expect_err("unknown preparation limit field must be rejected");
        CelldPreparationLimits::default().validate().unwrap();
        for limits in [
            CelldPreparationLimits {
                max_remote_values: 0,
                ..CelldPreparationLimits::default()
            },
            CelldPreparationLimits {
                max_value_bytes: 0x0010_0001,
                ..CelldPreparationLimits::default()
            },
            CelldPreparationLimits {
                max_snapshot_bytes: 1,
                ..CelldPreparationLimits::default()
            },
            CelldPreparationLimits {
                max_metadata_bytes: 0,
                ..CelldPreparationLimits::default()
            },
            CelldPreparationLimits {
                connect_timeout_ms: 5_001,
                ..CelldPreparationLimits::default()
            },
            CelldPreparationLimits {
                preparation_timeout_ms: u64::MAX,
                ..CelldPreparationLimits::default()
            },
        ] {
            limits
                .validate()
                .expect_err("invalid preparation quotas or deadlines must be rejected");
        }
    }
}
