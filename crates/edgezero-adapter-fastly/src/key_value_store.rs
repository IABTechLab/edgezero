//! Fastly KV Store adapter.
//!
//! Wraps `fastly::kv_store::KVStore` to implement the `edgezero_core::key_value_store::KvStore` trait.
//!
//! # Note
//!
//! This module is only compiled when the `fastly` feature is enabled.

use async_trait::async_trait;
use bytes::Bytes;
use edgezero_core::key_value_store::{KvError, KvPage, KvStore, KvStoreOpenFailure};
use edgezero_core::logging::BOOT_LOG_TARGET;
use fastly::kv_store::{KVStore, KVStoreError};
use fastly_shared::FastlyStatus;
use std::collections::HashSet;
use std::sync::{Mutex, OnceLock, PoisonError};
use std::time::Duration;

/// KV store backed by Fastly's KV Store API.
///
/// Opens compiled declarations lazily per request, or wraps an explicitly opened handle.
pub struct FastlyKvStore {
    source: FastlyKvStoreSource,
}

enum FastlyKvStoreSource {
    Declared {
        name: &'static str,
        opened: OnceLock<Result<KVStore, KvStoreOpenFailure>>,
    },
    Opened(KVStore),
}

impl FastlyKvStore {
    pub(crate) fn declared(name: &'static str) -> Self {
        Self {
            source: FastlyKvStoreSource::Declared {
                name,
                opened: OnceLock::new(),
            },
        }
    }

    /// Open a Fastly KV Store by name.
    ///
    /// # Errors
    /// Returns [`KvError::OpenFailed`] with a payload-free reason when opening fails.
    #[inline]
    pub fn open(name: &str) -> Result<Self, KvError> {
        let store =
            open_result(KVStore::open(name)).map_err(|reason| KvError::OpenFailed { reason })?;
        Ok(Self {
            source: FastlyKvStoreSource::Opened(store),
        })
    }

    fn with_store<Value>(
        &self,
        operation: impl FnOnce(&KVStore) -> Result<Value, KvError>,
    ) -> Result<Value, KvError> {
        match &self.source {
            FastlyKvStoreSource::Opened(store) => operation(store),
            FastlyKvStoreSource::Declared { name, opened } => use_declared_store(
                opened,
                || {
                    open_result(KVStore::open(name))
                        .inspect_err(|reason| warn_declared_open_once(name, *reason))
                },
                operation,
            ),
        }
    }
}

#[async_trait(?Send)]
impl KvStore for FastlyKvStore {
    #[inline]
    async fn delete(&self, key: &str) -> Result<(), KvError> {
        self.with_store(|store| {
            store
                .delete(key)
                .map_err(|err| KvError::Internal(anyhow::anyhow!("delete failed: {err}")))
        })
    }

    #[inline]
    async fn exists(&self, key: &str) -> Result<bool, KvError> {
        Ok(self.get_bytes(key).await?.is_some())
    }

    #[inline]
    async fn get_bytes(&self, key: &str) -> Result<Option<Bytes>, KvError> {
        self.with_store(|store| match store.lookup(key) {
            Ok(mut response) => {
                let bytes = response.take_body_bytes();
                Ok(Some(Bytes::from(bytes)))
            }
            Err(KVStoreError::ItemNotFound) => Ok(None),
            Err(err) => Err(KvError::Internal(anyhow::anyhow!("lookup failed: {err}"))),
        })
    }

    #[inline]
    async fn list_keys_page(
        &self,
        prefix: &str,
        cursor: Option<&str>,
        limit: usize,
    ) -> Result<KvPage, KvError> {
        let limit_u32 = u32::try_from(limit)
            .map_err(|_e| KvError::Validation("list limit exceeds u32".to_owned()))?;

        self.with_store(|store| {
            let mut request = store.build_list().limit(limit_u32);

            if !prefix.is_empty() {
                request = request.prefix(prefix);
            }
            if let Some(token) = cursor.filter(|token| !token.is_empty()) {
                request = request.cursor(token);
            }

            let page = request
                .execute()
                .map_err(|err| KvError::Internal(anyhow::anyhow!("list failed: {err}")))?;
            let next_cursor = page.next_cursor().filter(|token| !token.is_empty());

            Ok(KvPage {
                cursor: next_cursor,
                keys: page.into_keys(),
            })
        })
    }

    #[inline]
    async fn put_bytes(&self, key: &str, value: Bytes) -> Result<(), KvError> {
        self.with_store(|store| {
            store
                .insert(key, value.as_ref())
                .map_err(|err| KvError::Internal(anyhow::anyhow!("insert failed: {err}")))
        })
    }

    #[inline]
    async fn put_bytes_with_ttl(
        &self,
        key: &str,
        value: Bytes,
        ttl: Duration,
    ) -> Result<(), KvError> {
        self.with_store(|store| {
            store
                .build_insert()
                .time_to_live(ttl)
                .execute(key, value.as_ref())
                .map_err(|err| KvError::Internal(anyhow::anyhow!("insert with ttl failed: {err}")))
        })
    }
}

fn open_result<Store>(
    result: Result<Option<Store>, KVStoreError>,
) -> Result<Store, KvStoreOpenFailure> {
    result
        .map_err(|error| match error {
            KVStoreError::StoreNotFound(_) => KvStoreOpenFailure::MissingBinding,
            KVStoreError::InvalidKey
            | KVStoreError::InvalidStoreHandle
            | KVStoreError::InvalidStoreOptions
            | KVStoreError::ItemBadRequest
            | KVStoreError::Unexpected(
                FastlyStatus::INVAL
                | FastlyStatus::BADF
                | FastlyStatus::BUFLEN
                | FastlyStatus::UNSUPPORTED
                | FastlyStatus::BADALIGN,
            ) => KvStoreOpenFailure::InvalidConfiguration,
            KVStoreError::ItemNotFound
            | KVStoreError::ItemPreconditionFailed
            | KVStoreError::ItemPayloadTooLarge
            | KVStoreError::TooManyRequests
            | KVStoreError::Unexpected(_)
            | _ => KvStoreOpenFailure::Unavailable,
        })?
        .ok_or(KvStoreOpenFailure::MissingBinding)
}

fn mark_warning(warned: &Mutex<HashSet<&'static str>>, name: &'static str) -> bool {
    warned
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .insert(name)
}

fn warn_declared_open_once(name: &'static str, reason: KvStoreOpenFailure) {
    // Only compiled metadata IDs enter this set; never retain availability or provider payloads.
    static WARNED: OnceLock<Mutex<HashSet<&'static str>>> = OnceLock::new();
    if mark_warning(WARNED.get_or_init(|| Mutex::new(HashSet::new())), name) {
        log::warn!(target: BOOT_LOG_TARGET, "declared Fastly KV store '{name}' failed to open: {reason}");
    }
}

fn use_declared_store<Store, Value>(
    opened: &OnceLock<Result<Store, KvStoreOpenFailure>>,
    open: impl FnOnce() -> Result<Store, KvStoreOpenFailure>,
    operation: impl FnOnce(&Store) -> Result<Value, KvError>,
) -> Result<Value, KvError> {
    let store = opened
        .get_or_init(open)
        .as_ref()
        .map_err(|reason| KvError::OpenFailed { reason: *reason })?;
    operation(store)
}

#[cfg(test)]
mod tests {
    use super::*;
    use edgezero_core::EdgeError;
    use edgezero_core::http::StatusCode;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn open_failure_classification_drops_provider_payloads() {
        assert_eq!(open_result(Ok(Some(7_u8))), Ok(7));
        assert_eq!(
            open_result::<()>(Ok(None)),
            Err(KvStoreOpenFailure::MissingBinding)
        );
        for (error, expected) in [
            (
                KVStoreError::StoreNotFound("credential-bearing-store-token".to_owned()),
                KvStoreOpenFailure::MissingBinding,
            ),
            (
                KVStoreError::InvalidKey,
                KvStoreOpenFailure::InvalidConfiguration,
            ),
            (
                KVStoreError::InvalidStoreHandle,
                KvStoreOpenFailure::InvalidConfiguration,
            ),
            (
                KVStoreError::InvalidStoreOptions,
                KvStoreOpenFailure::InvalidConfiguration,
            ),
            (
                KVStoreError::ItemBadRequest,
                KvStoreOpenFailure::InvalidConfiguration,
            ),
            (
                KVStoreError::Unexpected(FastlyStatus::INVAL),
                KvStoreOpenFailure::InvalidConfiguration,
            ),
            (
                KVStoreError::Unexpected(FastlyStatus::BADF),
                KvStoreOpenFailure::InvalidConfiguration,
            ),
            (
                KVStoreError::Unexpected(FastlyStatus::BUFLEN),
                KvStoreOpenFailure::InvalidConfiguration,
            ),
            (
                KVStoreError::Unexpected(FastlyStatus::UNSUPPORTED),
                KvStoreOpenFailure::InvalidConfiguration,
            ),
            (
                KVStoreError::Unexpected(FastlyStatus::BADALIGN),
                KvStoreOpenFailure::InvalidConfiguration,
            ),
            (
                KVStoreError::TooManyRequests,
                KvStoreOpenFailure::Unavailable,
            ),
            (
                KVStoreError::Unexpected(FastlyStatus::ERROR),
                KvStoreOpenFailure::Unavailable,
            ),
            (
                KVStoreError::Unexpected(FastlyStatus::AGAIN),
                KvStoreOpenFailure::Unavailable,
            ),
            (
                KVStoreError::Unexpected(FastlyStatus::LIMITEXCEEDED),
                KvStoreOpenFailure::Unavailable,
            ),
            (
                KVStoreError::Unexpected(FastlyStatus { code: i32::MAX }),
                KvStoreOpenFailure::Unavailable,
            ),
        ] {
            let reason = open_result::<()>(Err(error)).expect_err("open failure");
            assert_eq!(reason, expected);
            let mapped = KvError::OpenFailed { reason };
            assert!(!format!("{mapped:?} {mapped}").contains("credential-bearing-store-token"));
            assert_eq!(
                EdgeError::from(mapped).status(),
                if reason == KvStoreOpenFailure::Unavailable {
                    StatusCode::SERVICE_UNAVAILABLE
                } else {
                    StatusCode::INTERNAL_SERVER_ERROR
                }
            );
        }
    }

    #[test]
    fn declared_open_is_once_per_request_and_next_request_recovers() {
        let opens = AtomicUsize::new(0);
        let operations = AtomicUsize::new(0);
        let first = OnceLock::new();
        assert_eq!(opens.load(Ordering::SeqCst), 0);
        for _ in 0_u8..6 {
            let result = use_declared_store(
                &first,
                || {
                    opens.fetch_add(1, Ordering::SeqCst);
                    Err::<u8, _>(KvStoreOpenFailure::Unavailable)
                },
                |_| {
                    operations.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                },
            );
            assert!(matches!(
                result,
                Err(KvError::OpenFailed {
                    reason: KvStoreOpenFailure::Unavailable
                })
            ));
        }
        assert_eq!(opens.load(Ordering::SeqCst), 1);
        assert_eq!(operations.load(Ordering::SeqCst), 0);
        let next_request = OnceLock::new();
        for _ in 0_u8..6 {
            assert_eq!(
                use_declared_store(
                    &next_request,
                    || {
                        opens.fetch_add(1, Ordering::SeqCst);
                        Ok(42_u8)
                    },
                    |store| {
                        operations.fetch_add(1, Ordering::SeqCst);
                        Ok(*store)
                    }
                )
                .unwrap(),
                42
            );
        }
        assert_eq!(opens.load(Ordering::SeqCst), 2);
        assert_eq!(operations.load(Ordering::SeqCst), 6);
    }

    #[test]
    fn declared_warning_ids_are_never_evicted_or_availability_cached() {
        const IDS: &str = concat!(
            "s00 s01 s02 s03 s04 s05 s06 s07 s08 s09 s10 s11 s12 s13 s14 s15 s16 ",
            "s17 s18 s19 s20 s21 s22 s23 s24 s25 s26 s27 s28 s29 s30 s31 s32 s33 ",
            "s34 s35 s36 s37 s38 s39 s40 s41 s42 s43 s44 s45 s46 s47 s48 s49 s50 ",
            "s51 s52 s53 s54 s55 s56 s57 s58 s59 s60 s61 s62 s63 s64 s65 s66 s67"
        );
        let warned = Mutex::new(HashSet::new());
        for name in IDS.split_ascii_whitespace() {
            assert!(mark_warning(&warned, name));
        }
        for name in IDS.split_ascii_whitespace() {
            assert!(!mark_warning(&warned, name));
        }
        assert_eq!(warned.lock().unwrap().len(), 68);
    }
}
