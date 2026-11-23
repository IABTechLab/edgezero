//! Fastly client-identity binding.
//!
//! The certificate chain and hosts list live in the identity's config store,
//! which has no per-request read limit. Only the private key lives in its
//! Secret Store, opened as a handle and never read as plaintext. The config half
//! is resolved at most once per inbound request; the key is opened only when a
//! backend has to be registered.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard};

use edgezero_core::error::EdgeError;
use edgezero_core::outbound::{
    ClientCertificateChain, ClientIdentityError, ClientIdentityHosts, ClientIdentityMaterial,
    ClientIdentityMetadata, ClientIdentityPolicy,
};

#[cfg(feature = "fastly")]
use crate::chunked_config::{SyncHostCallError, run_sync_host_call};
#[cfg(feature = "fastly")]
use crate::config_store::FastlyConfigStore;
#[cfg(feature = "fastly")]
use edgezero_core::config_store::ConfigStoreError;
#[cfg(feature = "fastly")]
use edgezero_core::{Deadline, MonotonicClock};
#[cfg(feature = "fastly")]
use fastly::secret_store::{Secret, SecretStore};

/// Longest accepted certificate entry, Fastly's config value limit.
pub(super) const MAX_CERTIFICATE_BYTES: u64 = 8_000;
/// Longest accepted hosts entry.
pub(super) const MAX_HOSTS_BYTES: u64 = 4_000;

/// The config-store half of a binding, shared by every dispatch in one inbound request.
#[derive(Clone, Debug)]
pub(super) struct ConfigBinding {
    pub chain: ClientCertificateChain,
    pub hosts: ClientIdentityHosts,
}

/// Why one config entry could not be read.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ConfigReadError {
    DeadlineExceeded,
    StoreMissing,
    TooLarge,
    Unavailable,
}

/// Why a binding could not be resolved.
#[derive(Debug)]
pub(super) enum BindingFailure {
    /// The read deadline expired; never cached.
    DeadlineExceeded,
    /// Determined by the stored data; cached for the rest of the inbound request.
    Material(ClientIdentityError),
    /// The store could not be reached; never cached.
    Unavailable,
}

/// Reads one config entry of at most `limit` bytes from a logical config store.
pub(super) trait ConfigEntryReader {
    fn read(&self, store: &str, key: &str, limit: u64) -> Result<Option<String>, ConfigReadError>;
}

type BindingEntries = HashMap<String, Result<Arc<ConfigBinding>, ClientIdentityError>>;

/// Bindings resolved during one inbound request, keyed by identity name.
#[derive(Debug, Default)]
pub(super) struct BindingCache {
    entries: Mutex<BindingEntries>,
}

impl BindingCache {
    fn lock(&self) -> Result<MutexGuard<'_, BindingEntries>, BindingFailure> {
        self.entries
            .lock()
            .map_err(|_poisoned| BindingFailure::Unavailable)
    }

    /// Returns the identity's config binding, reading it on first use in this request.
    pub(super) fn resolve(
        &self,
        metadata: &ClientIdentityMetadata,
        policy: &ClientIdentityPolicy,
        reader: &dyn ConfigEntryReader,
    ) -> Result<Arc<ConfigBinding>, BindingFailure> {
        let key = metadata.name().as_str();
        if let Some(cached) = self.lock()?.get(key) {
            return cached.clone().map_err(BindingFailure::Material);
        }
        let loaded = load_config_binding(metadata, policy, reader);
        let cacheable = match &loaded {
            Ok(binding) => Some(Ok(Arc::clone(binding))),
            Err(BindingFailure::Material(reason)) => Some(Err(reason.clone())),
            Err(BindingFailure::DeadlineExceeded | BindingFailure::Unavailable) => None,
        };
        if let Some(entry) = cacheable {
            self.lock()?.insert(key.to_owned(), entry);
        }
        loaded
    }
}

/// Config key of the identity's certificate chain.
pub(super) fn certificate_key(metadata: &ClientIdentityMetadata) -> String {
    format!("{}.certificate", metadata.name())
}

/// Config key of the identity's hosts list.
pub(super) fn hosts_key(metadata: &ClientIdentityMetadata) -> String {
    format!("{}.hosts", metadata.name())
}

fn load_config_binding(
    metadata: &ClientIdentityMetadata,
    policy: &ClientIdentityPolicy,
    reader: &dyn ConfigEntryReader,
) -> Result<Arc<ConfigBinding>, BindingFailure> {
    let store = metadata.config_store().ok_or(BindingFailure::Material(
        ClientIdentityError::MaterialMissing {
            which: ClientIdentityMaterial::Store,
        },
    ))?;
    let hosts_text = read_entry(
        reader,
        store,
        &hosts_key(metadata),
        MAX_HOSTS_BYTES,
        ClientIdentityMaterial::Hosts,
    )?;
    let pem = read_entry(
        reader,
        store,
        &certificate_key(metadata),
        MAX_CERTIFICATE_BYTES,
        ClientIdentityMaterial::Certificate,
    )?;
    let hosts =
        ClientIdentityHosts::parse(&hosts_text, policy).map_err(BindingFailure::Material)?;
    let chain = ClientCertificateChain::parse(&pem).map_err(BindingFailure::Material)?;
    Ok(Arc::new(ConfigBinding { chain, hosts }))
}

/// Opens the identity's private key as a Secret Store handle, without plaintext.
///
/// Called only when a backend must be registered, so a request reusing a
/// registered backend spends no Secret Store read.
#[cfg(feature = "fastly")]
pub(super) fn open_private_key(
    metadata: &ClientIdentityMetadata,
    clock: &MonotonicClock,
    deadline: Deadline,
) -> Result<Secret, BindingFailure> {
    let missing = |which| BindingFailure::Material(ClientIdentityError::MaterialMissing { which });
    let alias = metadata
        .secret_store()
        .ok_or_else(|| missing(ClientIdentityMaterial::Store))?;
    let store = run_sync_host_call(clock, deadline, || {
        SecretStore::open(alias).map_err(|_error| missing(ClientIdentityMaterial::Store))
    })
    .map_err(sync_failure)?;
    let key = format!("{}.private-key", metadata.name());
    let secret = run_sync_host_call(clock, deadline, || {
        store
            .try_get(&key)
            .map_err(|_error| BindingFailure::Unavailable)
    })
    .map_err(sync_failure)?;
    secret.ok_or_else(|| missing(ClientIdentityMaterial::Key))
}

/// Reads binding entries from Fastly config stores under the dispatch deadline.
#[cfg(feature = "fastly")]
pub(super) struct FastlyConfigEntries<'clock> {
    pub clock: &'clock MonotonicClock,
    pub deadline: Deadline,
}

#[cfg(feature = "fastly")]
impl ConfigEntryReader for FastlyConfigEntries<'_> {
    fn read(&self, store: &str, key: &str, limit: u64) -> Result<Option<String>, ConfigReadError> {
        let opened = run_sync_host_call(self.clock, self.deadline, || {
            FastlyConfigStore::try_open_optional(store)
        })
        .map_err(|error| config_failure(&error))?
        .ok_or(ConfigReadError::StoreMissing)?;
        run_sync_host_call(self.clock, self.deadline, || opened.read_entry(key, limit))
            .map_err(|error| config_failure(&error))
    }
}

#[cfg(feature = "fastly")]
fn config_failure(error: &SyncHostCallError<ConfigStoreError>) -> ConfigReadError {
    match error {
        SyncHostCallError::DeadlineExceeded
        | SyncHostCallError::Backend(ConfigStoreError::DeadlineExceeded) => {
            ConfigReadError::DeadlineExceeded
        }
        SyncHostCallError::Backend(ConfigStoreError::ValueTooLarge) => ConfigReadError::TooLarge,
        SyncHostCallError::Backend(
            ConfigStoreError::Internal { .. }
            | ConfigStoreError::InvalidKey { .. }
            | ConfigStoreError::Unavailable { .. }
            | _,
        ) => ConfigReadError::Unavailable,
    }
}

#[cfg(feature = "fastly")]
fn sync_failure(error: SyncHostCallError<BindingFailure>) -> BindingFailure {
    match error {
        SyncHostCallError::Backend(failure) => failure,
        SyncHostCallError::DeadlineExceeded => BindingFailure::DeadlineExceeded,
    }
}

fn read_entry(
    reader: &dyn ConfigEntryReader,
    store: &str,
    key: &str,
    limit: u64,
    which: ClientIdentityMaterial,
) -> Result<String, BindingFailure> {
    match reader.read(store, key, limit) {
        Ok(Some(value)) => Ok(value),
        Ok(None) => Err(BindingFailure::Material(
            ClientIdentityError::MaterialMissing { which },
        )),
        Err(ConfigReadError::TooLarge) => Err(BindingFailure::Material(
            ClientIdentityError::MaterialInvalid { which },
        )),
        Err(ConfigReadError::StoreMissing) => Err(BindingFailure::Material(
            ClientIdentityError::MaterialMissing {
                which: ClientIdentityMaterial::Store,
            },
        )),
        Err(ConfigReadError::DeadlineExceeded) => Err(BindingFailure::DeadlineExceeded),
        Err(ConfigReadError::Unavailable) => Err(BindingFailure::Unavailable),
    }
}

/// Maps a binding failure for `metadata` to the request's error.
pub(super) fn binding_error(
    metadata: &ClientIdentityMetadata,
    failure: BindingFailure,
    timeout: impl FnOnce() -> EdgeError,
) -> EdgeError {
    match failure {
        BindingFailure::Material(reason) => EdgeError::client_identity(metadata.name(), reason),
        BindingFailure::DeadlineExceeded => timeout(),
        BindingFailure::Unavailable => EdgeError::client_identity(
            metadata.name(),
            ClientIdentityError::MaterialMissing {
                which: ClientIdentityMaterial::Store,
            },
        ),
    }
}

#[cfg(test)]
mod tests;
