//! Spin KV store adapter.
//!
//! Wraps `spin_sdk::key_value::Store` to implement the
//! `edgezero_core::key_value_store::KvStore` trait.
//!
//! # Limitations
//!
//! - **TTL**: The Spin KV API has no TTL support. Calls to
//!   `put_bytes_with_ttl` return [`KvError::Unsupported`] without writing.
//! - **Listing**: `spin_sdk::key_value::Store::get_keys()` returns all keys
//!   with no prefix, cursor, or limit support. `list_keys_page` materialises
//!   the full key list, filters by prefix, sorts, and pages client-side via
//!   [`crate::kv_pagination::paginate_keys`]. A `max_list_keys` cap guards
//!   against runaway materialisation; exceeding it yields
//!   [`KvError::LimitExceeded`].
//!
//! # Note
//!
//! This module is only compiled when the `spin` feature is enabled and the
//! target is `wasm32`.

use async_trait::async_trait;
use bytes::Bytes;
use edgezero_core::key_value_store::{KvError, KvPage, KvStore, KvStoreOpenFailure};
use edgezero_core::logging::BOOT_LOG_TARGET;
use futures::lock::Mutex as AsyncMutex;
use spin_sdk::key_value::{Error as SpinKvError, Store as SpinSdkStore};
use std::collections::HashSet;
use std::sync::{Arc, Mutex, OnceLock, PoisonError};
use std::time::Duration;

use crate::kv_pagination::paginate_keys;

/// Default `max_list_keys` cap. Matches the Cloudflare KV page size ceiling
/// (`KvHandle::MAX_LIST_PAGE_SIZE`) and stays well below the soft per-isolate
/// memory budgets a Spin component is given. Overridable via
/// `EDGEZERO__STORES__KV__<ID>__MAX_LIST_KEYS`.
pub const DEFAULT_MAX_LIST_KEYS: usize = 1_000;

/// KV store backed by the Spin KV API.
///
/// Opens compiled declarations lazily per request, or wraps an explicitly opened
/// `spin_sdk::key_value::Store`, with a `max_list_keys` paging cap.
pub struct SpinKvStore {
    max_list_keys: usize,
    source: SpinKvStoreSource,
}

enum SpinKvStoreSource {
    Declared {
        id: &'static str,
        label: String,
        opened: AsyncMutex<Option<Result<Arc<SpinSdkStore>, KvStoreOpenFailure>>>,
    },
    Opened(Arc<SpinSdkStore>),
}

impl SpinKvStore {
    pub(crate) fn declared(id: &'static str, label: String, max_list_keys: usize) -> Self {
        Self {
            max_list_keys,
            source: SpinKvStoreSource::Declared {
                id,
                label,
                opened: AsyncMutex::new(None),
            },
        }
    }

    /// Open a Spin KV store by label, using the default `max_list_keys` cap.
    ///
    /// The `label` must match a `key_value_stores` entry in `spin.toml`.
    ///
    /// # Errors
    /// Returns [`KvError::OpenFailed`] with a payload-free open reason.
    #[inline]
    pub async fn open(label: &str) -> Result<Self, KvError> {
        Self::open_with_max_list_keys(label, DEFAULT_MAX_LIST_KEYS).await
    }

    /// Open a Spin KV store by label with a custom `max_list_keys` cap.
    /// Pass `0` to disable the cap (not recommended in production).
    ///
    /// # Errors
    /// Returns [`KvError::OpenFailed`] with a payload-free open reason.
    #[inline]
    pub async fn open_with_max_list_keys(
        label: &str,
        max_list_keys: usize,
    ) -> Result<Self, KvError> {
        let store = SpinSdkStore::open(label)
            .await
            .map_err(|error| KvError::OpenFailed {
                reason: open_failure(error),
            })?;
        Ok(Self {
            max_list_keys,
            source: SpinKvStoreSource::Opened(Arc::new(store)),
        })
    }

    async fn store(&self) -> Result<Arc<SpinSdkStore>, KvError> {
        match &self.source {
            SpinKvStoreSource::Opened(store) => Ok(Arc::clone(store)),
            SpinKvStoreSource::Declared { id, label, opened } => {
                let mut slot = opened.lock().await;
                if slot.is_none() {
                    // Cancellation leaves the cell empty; only settled opens are retained.
                    let result = SpinSdkStore::open(label)
                        .await
                        .map(Arc::new)
                        .map_err(open_failure)
                        .inspect_err(|reason| warn_declared_open_once(id, *reason));
                    *slot = Some(result);
                }
                match slot.as_ref() {
                    Some(Ok(store)) => Ok(Arc::clone(store)),
                    Some(Err(reason)) => Err(KvError::OpenFailed { reason: *reason }),
                    None => Err(KvError::Internal(anyhow::anyhow!(
                        "KV initialization did not settle"
                    ))),
                }
            }
        }
    }
}

#[async_trait(?Send)]
impl KvStore for SpinKvStore {
    #[inline]
    async fn delete(&self, key: &str) -> Result<(), KvError> {
        self.store()
            .await?
            .delete(key)
            .await
            .map_err(|err| KvError::Internal(anyhow::anyhow!("delete failed: {err}")))
    }

    #[inline]
    async fn exists(&self, key: &str) -> Result<bool, KvError> {
        self.store()
            .await?
            .exists(key)
            .await
            .map_err(|err| KvError::Internal(anyhow::anyhow!("exists failed: {err}")))
    }

    #[inline]
    async fn get_bytes(&self, key: &str) -> Result<Option<Bytes>, KvError> {
        self.store()
            .await?
            .get(key)
            .await
            .map(|opt| opt.map(Bytes::from))
            .map_err(|err| KvError::Internal(anyhow::anyhow!("get failed: {err}")))
    }

    #[inline]
    async fn list_keys_page(
        &self,
        prefix: &str,
        cursor: Option<&str>,
        limit: usize,
    ) -> Result<KvPage, KvError> {
        let all_keys = self
            .store()
            .await?
            .get_keys()
            .await
            .collect()
            .await
            .map_err(|err| KvError::Internal(anyhow::anyhow!("get_keys failed: {err}")))?;
        paginate_keys(all_keys, prefix, cursor, limit, self.max_list_keys)
    }

    #[inline]
    async fn put_bytes(&self, key: &str, value: Bytes) -> Result<(), KvError> {
        self.store()
            .await?
            .set(key, value.as_ref())
            .await
            .map_err(|err| KvError::Internal(anyhow::anyhow!("set failed: {err}")))
    }

    #[inline]
    async fn put_bytes_with_ttl(
        &self,
        _key: &str,
        _value: Bytes,
        _ttl: Duration,
    ) -> Result<(), KvError> {
        let _store = self.store().await?;
        Err(KvError::Unsupported {
            operation: "put_bytes_with_ttl".to_owned(),
        })
    }
}

fn open_failure(error: SpinKvError) -> KvStoreOpenFailure {
    match error {
        SpinKvError::NoSuchStore => KvStoreOpenFailure::MissingBinding,
        SpinKvError::AccessDenied => KvStoreOpenFailure::InvalidConfiguration,
        SpinKvError::StoreTableFull => KvStoreOpenFailure::Unavailable,
        SpinKvError::Other(message) => {
            drop(message);
            KvStoreOpenFailure::Unavailable
        }
    }
}

fn warn_declared_open_once(id: &'static str, reason: KvStoreOpenFailure) {
    // Membership is bounded by compiled metadata IDs, never runtime-selected labels.
    static WARNED: OnceLock<Mutex<HashSet<&'static str>>> = OnceLock::new();
    let first = WARNED
        .get_or_init(|| Mutex::new(HashSet::new()))
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .insert(id);
    if first {
        log::warn!(target: BOOT_LOG_TARGET, "declared Spin KV store '{id}' failed to open: {reason}");
    }
}
