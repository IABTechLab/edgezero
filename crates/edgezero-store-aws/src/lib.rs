//! Native AWS store preparation, independent of the HTTP adapter.
//!
//! Settings remain available without either service feature. Service failures
//! use closed categories; no SDK, HTTP, settings or payload source chain survives.
//!
//! The default-off `dynamodb-records` feature adds
//! `AwsPreparation::prepare_dynamodb_records` on native targets. Its explicit
//! partitioned format does not read or migrate legacy ordinary KV rows. Inserts
//! allocate a retained per-key generation with `UpdateItem`, then `PutItem` the value.
//! Allocators must survive deletion and must not have TTL. Value TTL uses native
//! `ttl_epoch_s` cleanup, without a hard expiry filter. Body and UTF-8 metadata
//! share the configured `max_value_bytes` payload budget. Partition pages use
//! strong base-table Query; ordinary global listing
//! uses unordered Scan, which reads broadly before namespace/prefix filtering.
//! Record preparation binds cursors to the resolved endpoint and `DescribeTable`
//! ARN, plus the table ID when supplied. It does not perform an IAM/write probe.

#![cfg_attr(
    all(
        not(target_arch = "wasm32"),
        any(
            feature = "appconfig-agent",
            feature = "secrets-manager",
            feature = "dynamodb-kv"
        )
    ),
    expect(
        clippy::pub_use,
        reason = "keep the provider's supported preparation API at the crate root"
    )
)]

pub mod settings;

#[cfg(test)]
#[cfg(not(target_arch = "wasm32"))]
#[cfg(feature = "appconfig-agent")]
mod appconfig_tests;

#[cfg(all(not(target_arch = "wasm32"), feature = "dynamodb-kv"))]
mod dynamodb_kv;
#[cfg(feature = "dynamodb-records-portable")]
pub mod portable_dynamodb;
#[cfg(all(
    not(target_arch = "wasm32"),
    any(
        feature = "appconfig-agent",
        feature = "secrets-manager",
        feature = "dynamodb-kv"
    )
))]
mod preparation;
#[cfg(any(
    all(not(target_arch = "wasm32"), feature = "dynamodb-records"),
    feature = "dynamodb-records-portable"
))]
mod records_core;
#[cfg(all(not(target_arch = "wasm32"), feature = "secrets-manager"))]
mod secrets_manager;
#[cfg(test)]
#[cfg(all(not(target_arch = "wasm32"), feature = "secrets-manager"))]
mod snapshot_tests;
#[cfg(all(
    not(target_arch = "wasm32"),
    any(
        feature = "appconfig-agent",
        feature = "secrets-manager",
        feature = "dynamodb-kv"
    )
))]
pub use preparation::{AwsPreparation, PreparationStats};

/// Sanitized preparation failures. Never attach provider response details.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum PreparationError {
    #[error("access denied")]
    AccessDenied,
    #[error("credential failure")]
    Credentials,
    #[error("preparation deadline")]
    Deadline,
    #[error("decryption failure")]
    Decryption,
    #[error("invalid binding")]
    InvalidBinding,
    #[error("invalid agent endpoint")]
    InvalidEndpoint,
    #[error("invalid preparation limits")]
    InvalidLimits,
    #[error("invalid resource reference")]
    InvalidResource,
    #[error("invalid version selector")]
    InvalidSelector,
    #[error("malformed value")]
    Malformed,
    #[error("missing resource or version")]
    Missing,
    #[error("pinned identity mismatch")]
    PinnedIdentityMismatch,
    #[error("size limit")]
    SizeLimit,
    #[error("throttled")]
    Throttled,
    #[error("provider unavailable")]
    Unavailable,
    #[error("provider not compiled")]
    UnsupportedProvider,
}
