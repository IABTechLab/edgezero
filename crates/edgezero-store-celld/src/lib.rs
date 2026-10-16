//! Scoped native celld stores. Settings are available without runtime features.
//!
//! Config is an immutable startup snapshot; KV is live. Owned errors retain no
//! endpoint, credential, resource, remote body or underlying transport error.

#![cfg_attr(
    all(not(target_arch = "wasm32"), feature = "_transport"),
    expect(
        clippy::pub_use,
        reason = "the supported preparation API is re-exported at the crate root; this lint is module-scoped"
    )
)]

pub mod settings;

#[cfg(all(not(target_arch = "wasm32"), feature = "_transport"))]
mod client;
#[cfg(all(not(target_arch = "wasm32"), feature = "config"))]
mod config;
#[cfg(all(not(target_arch = "wasm32"), feature = "kv"))]
mod kv;
#[cfg(all(not(target_arch = "wasm32"), feature = "_transport"))]
mod preparation;
#[cfg(test)]
#[cfg(all(not(target_arch = "wasm32"), feature = "_transport"))]
mod test_support;
#[cfg(all(not(target_arch = "wasm32"), feature = "_transport"))]
pub use preparation::{CelldPreparation, PreparationStats};

/// Closed failure categories, never transport error chains or server text.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[expect(
    clippy::arbitrary_source_item_ordering,
    reason = "keep binding validation, service failures and transport failures grouped in the agreed closed API"
)]
pub enum PreparationError {
    #[error("invalid binding")]
    InvalidBinding,
    #[error("invalid endpoint")]
    InvalidEndpoint,
    #[error("invalid preparation limits")]
    InvalidLimits,
    #[error("invalid resource")]
    InvalidResource,
    #[error("credential failure")]
    Credentials,
    #[error("unauthenticated")]
    Unauthenticated,
    #[error("forbidden")]
    Forbidden,
    #[error("missing resource")]
    Missing,
    #[error("size limit")]
    SizeLimit,
    #[error("malformed response")]
    Malformed,
    #[error("unsupported version")]
    UnsupportedVersion,
    #[error("provider unavailable")]
    Unavailable,
    #[error("deadline exceeded")]
    Deadline,
}
