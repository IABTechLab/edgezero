//! The strict `version = 1` store bindings document, shared by every adapter.
//!
//! This module only parses and validates settings. It performs no I/O, reads no
//! secret and constructs no provider. Axum reads the document from the file named by
//! `EDGEZERO__STORE_BINDINGS_FILE`; Fastly reads it from the Config Store
//! `edgezero_store_bindings`, entry `EDGEZERO__STORE_BINDINGS`.

use std::collections::BTreeMap;
use std::fmt;

use edgezero_store_aws::settings::{
    AppConfigAgentSettings, AwsPreparationLimits, DynamoDbKvLimits, DynamoDbKvSettings,
    DynamoDbRecordsSettings, SecretsManagerSettings,
};
use edgezero_store_celld::settings::{
    CelldKvSettings, CelldPreparationLimits, CelldRecordsSettings,
};
use serde::Deserialize;

/// Config Store name that holds a portable target's bindings document.
pub const PORTABLE_BINDINGS_STORE: &str = "edgezero_store_bindings";
/// Config Store entry that holds a portable target's bindings document.
pub const PORTABLE_BINDINGS_ENTRY: &str = "EDGEZERO__STORE_BINDINGS";

/// Closed document failures. Never carries document text or settings.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum StoreBindingsError {
    #[error("invalid store bindings")]
    Invalid,
    #[error("malformed store bindings")]
    Malformed,
    #[error("portable binding has no credentials reference")]
    MissingCredentials,
    #[error("portable binding has no pinned identity")]
    MissingPin,
    #[error("provider not compiled")]
    NotCompiled,
    #[error("store bindings size limit")]
    SizeLimit,
    #[error("undeclared binding")]
    Undeclared,
    #[error("provider unsupported on this target")]
    Unsupported,
}

/// Strict KV provider dispatch. The physical namespace is independent of the declared ID.
#[derive(Clone, Debug, Deserialize)]
#[serde(tag = "provider", deny_unknown_fields)]
pub enum KvBinding {
    #[serde(rename = "celld-kv")]
    Celld(CelldKvSettings),
    #[serde(rename = "celld-records")]
    CelldRecords(CelldRecordsSettings),
    #[serde(rename = "aws-dynamodb")]
    DynamoDb(DynamoDbKvSettings),
    #[serde(rename = "aws-dynamodb-records")]
    DynamoDbRecords(DynamoDbRecordsSettings),
}

/// Strict configuration dispatch. Provider-specific fields are validated by AWS settings.
#[derive(Clone, Debug, Deserialize)]
#[serde(tag = "provider", deny_unknown_fields)]
pub enum ConfigBinding {
    #[serde(rename = "aws-appconfig-agent")]
    AppConfigAgent(AppConfigAgentSettings),
}

/// Strict secret dispatch. No arbitrary remote names or per-request discovery.
#[derive(Clone, Debug, Deserialize)]
#[serde(tag = "provider", deny_unknown_fields)]
pub enum SecretBinding {
    #[serde(rename = "aws-secrets-manager")]
    SecretsManager(SecretsManagerSettings),
}

/// Document-level native preparation limits.
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BindingLimits {
    pub aws: Option<AwsPreparationLimits>,
    pub celld: Option<CelldPreparationLimits>,
}

/// One parsed `version = 1` document. Logical IDs and lookup keys are case-sensitive.
#[derive(Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoreBindingsDocument {
    #[serde(default)]
    pub config: BTreeMap<String, ConfigBinding>,
    #[serde(default)]
    pub kv: BTreeMap<String, KvBinding>,
    pub limits: Option<BindingLimits>,
    #[serde(default)]
    pub secrets: BTreeMap<String, SecretBinding>,
    pub version: u32,
}

impl fmt::Debug for StoreBindingsDocument {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StoreBindingsDocument")
            .finish_non_exhaustive()
    }
}

/// Portable record providers compiled into a target binary.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct PortableRecordProviders {
    pub celld: bool,
    pub dynamodb: bool,
}

/// Declared logical IDs a portable document may reference.
#[derive(Clone, Copy, Debug)]
pub struct PortableDeclarations<'ids> {
    pub kv: &'ids [&'ids str],
    pub secrets: &'ids [&'ids str],
}

impl StoreBindingsDocument {
    /// Parses strict TOML under a trusted byte cap, before any validation.
    ///
    /// # Errors
    /// Returns [`StoreBindingsError::SizeLimit`] above `max_bytes` and
    /// [`StoreBindingsError::Malformed`] for any TOML or schema error.
    #[inline]
    pub fn parse(input: &str, max_bytes: usize) -> Result<Self, StoreBindingsError> {
        if input.len() > max_bytes {
            return Err(StoreBindingsError::SizeLimit);
        }
        toml::from_str(input).map_err(|_error| StoreBindingsError::Malformed)
    }

    /// Offline checks for a portable target such as Fastly. No secret reads and no I/O.
    ///
    /// Only `aws-dynamodb-records` and `celld-records` KV bindings are accepted. Each needs
    /// its pinned identity and a declared logical secrets reference. KV IDs must be declared
    /// and match `[a-z0-9_]`. Native-only sections and providers fail; nothing falls back.
    ///
    /// # Errors
    /// Returns the first closed category that applies.
    #[inline]
    pub fn validate_portable(
        &self,
        declarations: &PortableDeclarations<'_>,
        compiled: PortableRecordProviders,
    ) -> Result<(), StoreBindingsError> {
        if self.version != 1 {
            return Err(StoreBindingsError::Invalid);
        }
        if !self.config.is_empty() || !self.secrets.is_empty() || self.limits.is_some() {
            return Err(StoreBindingsError::Unsupported);
        }
        for (id, binding) in &self.kv {
            if !declarations.kv.contains(&id.as_str()) {
                return Err(StoreBindingsError::Undeclared);
            }
            if !valid_portable_id(id) {
                return Err(StoreBindingsError::Invalid);
            }
            let secrets = match binding {
                KvBinding::DynamoDb(_) | KvBinding::Celld(_) => {
                    return Err(StoreBindingsError::Unsupported);
                }
                KvBinding::DynamoDbRecords(settings) => {
                    if !compiled.dynamodb {
                        return Err(StoreBindingsError::NotCompiled);
                    }
                    settings
                        .validate(DynamoDbKvLimits::PORTABLE)
                        .and_then(|()| settings.portable_endpoint())
                        .map_err(|_category| StoreBindingsError::Invalid)?;
                    if settings.pinned.is_none() {
                        return Err(StoreBindingsError::MissingPin);
                    }
                    settings
                        .credentials
                        .as_ref()
                        .map(|reference| reference.secrets.as_str())
                }
                KvBinding::CelldRecords(settings) => {
                    if !compiled.celld {
                        return Err(StoreBindingsError::NotCompiled);
                    }
                    settings
                        .portable_endpoint()
                        .map_err(|_category| StoreBindingsError::Invalid)?;
                    if settings.pinned.is_none() {
                        return Err(StoreBindingsError::MissingPin);
                    }
                    settings.token_secret()
                }
            };
            let secrets_id = secrets.ok_or(StoreBindingsError::MissingCredentials)?;
            if !declarations.secrets.contains(&secrets_id) {
                return Err(StoreBindingsError::Undeclared);
            }
        }
        Ok(())
    }
}

/// Lowercase ASCII, digits and underscore, so uppercased secret key names cannot collide.
#[inline]
#[must_use]
pub fn valid_portable_id(id: &str) -> bool {
    !id.is_empty()
        && id
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
}

#[cfg(test)]
mod tests {
    use super::*;

    const DECLARED: PortableDeclarations<'static> = PortableDeclarations {
        kv: &["identity", "sessions", "Upper"],
        secrets: &["stores"],
    };
    const BOTH: PortableRecordProviders = PortableRecordProviders {
        celld: true,
        dynamodb: true,
    };

    const DOCUMENT: &str = r#"
version = 1

[kv.identity]
provider = "aws-dynamodb-records"
region = "us-east-1"
table = "ts-identity"
namespace = "ts-prod/identity"
pinned = { table_arn = "arn:aws:dynamodb:us-east-1:111122223333:table/ts-identity", table_id = "id" }
credentials = { secrets = "stores" }

[kv.sessions]
provider = "celld-records"
endpoint = "https://celld.example.net"
store = "sessions"
pinned = { instance = "AAAAAAAAAAAAAAAAAAAAAA" }
access = { mode = "authenticated", token_secret = { secrets = "stores" } }
"#;

    fn document(input: &str) -> StoreBindingsDocument {
        StoreBindingsDocument::parse(input, 8_000).unwrap()
    }

    #[test]
    fn the_specified_document_validates_for_a_portable_target() {
        document(DOCUMENT)
            .validate_portable(&DECLARED, BOTH)
            .unwrap();
    }

    #[test]
    fn parsing_is_strict_and_capped() {
        assert_eq!(
            StoreBindingsDocument::parse(DOCUMENT, 10).unwrap_err(),
            StoreBindingsError::SizeLimit
        );
        for malformed in [
            "version = 1\nunknown = true",
            "version = 1\n[kv.x]\nprovider = \"aws-dynamodb-records\"\nregion = \"us-east-1\"\ntable = \"ttt\"\nnamespace = \"n\"\nextra = 1",
            "version = 1\n[kv.x]\nprovider = \"other\"",
        ] {
            assert_eq!(
                StoreBindingsDocument::parse(malformed, 8_000).unwrap_err(),
                StoreBindingsError::Malformed
            );
        }
    }

    #[test]
    fn portable_validation_fails_closed_without_fallback() {
        let cases = [
            (DOCUMENT.replace("version = 1", "version = 2"), StoreBindingsError::Invalid),
            (format!("{DOCUMENT}\n[limits.celld]\noperation_timeout_ms = 10"), StoreBindingsError::Unsupported),
            (DOCUMENT.replace("[kv.sessions]", "[kv.undeclared]"), StoreBindingsError::Undeclared),
            (DOCUMENT.replace("[kv.sessions]", "[kv.Upper]"), StoreBindingsError::Invalid),
            (DOCUMENT.replace("pinned = { instance = \"AAAAAAAAAAAAAAAAAAAAAA\" }\n", ""), StoreBindingsError::MissingPin),
            (DOCUMENT.replace("credentials = { secrets = \"stores\" }\n", ""), StoreBindingsError::MissingCredentials),
            (DOCUMENT.replace("credentials = { secrets = \"stores\" }", "credentials = { secrets = \"other\" }"), StoreBindingsError::Undeclared),
            (DOCUMENT.replace("https://celld.example.net", "http://celld.example.net"), StoreBindingsError::Invalid),
            (
                DOCUMENT.replace(
                    "access = { mode = \"authenticated\", token_secret = { secrets = \"stores\" } }",
                    "access = { mode = \"trusted-network\" }",
                ),
                StoreBindingsError::Invalid,
            ),
            (
                DOCUMENT.replace("token_secret = { secrets = \"stores\" }", "token_env = \"TOKEN\""),
                StoreBindingsError::Invalid,
            ),
            (
                DOCUMENT.replace("provider = \"celld-records\"", "provider = \"celld-kv\"")
                    .replace("pinned = { instance = \"AAAAAAAAAAAAAAAAAAAAAA\" }\n", "")
                    .replace("token_secret = { secrets = \"stores\" }", "token_env = \"TOKEN\""),
                StoreBindingsError::Unsupported,
            ),
        ];
        for (input, expected) in cases {
            assert_eq!(
                document(&input).validate_portable(&DECLARED, BOTH),
                Err(expected),
                "{input}"
            );
        }
        let dynamodb_only = PortableRecordProviders {
            celld: false,
            dynamodb: true,
        };
        assert_eq!(
            document(DOCUMENT).validate_portable(&DECLARED, dynamodb_only),
            Err(StoreBindingsError::NotCompiled)
        );
    }

    #[test]
    fn portable_ids_cannot_collide_after_uppercasing() {
        assert!(valid_portable_id("identity_2"));
        for id in ["", "Identity", "id-entity", "id.entity", "id\u{e9}"] {
            assert!(!valid_portable_id(id));
        }
    }
}
