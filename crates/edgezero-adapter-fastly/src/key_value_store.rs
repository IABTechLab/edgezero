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
use edgezero_core::record_store::{
    Record, RecordBinding, RecordGeneration, RecordLayout, RecordPage, RecordPartition,
    RecordWrite, RecordWriteMode, RecordWriteOutcome,
};
use fastly::kv_store::{InsertMode, KVStore, KVStoreError, ListMode};
use fastly_shared::FastlyStatus;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::str::from_utf8;
use std::sync::{Mutex, OnceLock, PoisonError};
use std::time::Duration;

/// KV store backed by Fastly's KV Store API.
///
/// Opens compiled declarations lazily per request, or wraps an explicitly opened handle.
pub struct FastlyKvStore {
    records: Option<RecordBinding>,
    source: FastlyKvStoreSource,
}

enum FastlyKvStoreSource {
    Declared {
        name: &'static str,
        opened: OnceLock<Result<KVStore, KvStoreOpenFailure>>,
    },
    Opened(KVStore),
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct OrdinaryCursor {
    format: String,
    layout: String,
    native: String,
    prefix: String,
    scope: String,
    version: u32,
}

impl FastlyKvStore {
    pub(crate) fn declared(name: &'static str) -> Self {
        Self {
            source: FastlyKvStoreSource::Declared {
                name,
                opened: OnceLock::new(),
            },
            records: None,
        }
    }

    pub(crate) fn declared_records(name: &'static str, layout: RecordLayout) -> Self {
        Self {
            records: Some(record_binding(name, layout)),
            ..Self::declared(name)
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
            records: None,
        })
    }

    /// Open a record-capable Fastly store with its immutable application layout.
    ///
    /// # Errors
    /// Returns [`KvError::OpenFailed`] when the native binding cannot be opened.
    #[inline]
    pub fn open_records(name: &str, layout: RecordLayout) -> Result<Self, KvError> {
        Ok(Self {
            records: Some(record_binding(name, layout)),
            ..Self::open(name)?
        })
    }

    fn validate_partition(&self, key: &str, partition: &RecordPartition) -> Result<(), KvError> {
        let binding = self.records.as_ref().ok_or_else(unsupported_records)?;
        if binding.layout.resolve(key)? != *partition {
            return Err(KvError::Validation("record partition mismatch".to_owned()));
        }
        Ok(())
    }

    fn validate_point(&self, key: &str) -> Result<(), KvError> {
        if let Some(binding) = &self.records {
            binding.layout.resolve(key)?;
        }
        Ok(())
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
        self.validate_point(key)?;
        self.with_store(|store| {
            if self.records.is_some() {
                return delete_record(store, key);
            }
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
        self.validate_point(key)?;
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
        if let Some(binding) = &self.records {
            let native_cursor = decode_ordinary_cursor(binding, prefix, cursor)?;
            let page = self.with_store(|store| {
                strong_list_page(store, prefix, native_cursor.as_deref(), limit)
            })?;
            for key in &page.keys {
                binding.layout.resolve(key)?;
            }
            return Ok(KvPage {
                keys: page.keys,
                cursor: encode_ordinary_cursor(binding, prefix, page.cursor)?,
            });
        }
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
        self.validate_point(key)?;
        self.with_store(|store| {
            if self.records.is_some() {
                return insert_record(
                    store,
                    key,
                    RecordWrite {
                        body: value.as_ref(),
                        metadata: None,
                        ttl: None,
                        mode: RecordWriteMode::Overwrite,
                    },
                )
                .map(|_| ());
            }
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
        self.validate_point(key)?;
        self.with_store(|store| {
            if self.records.is_some() {
                return insert_record(
                    store,
                    key,
                    RecordWrite {
                        body: value.as_ref(),
                        metadata: None,
                        ttl: Some(ttl),
                        mode: RecordWriteMode::Overwrite,
                    },
                )
                .map(|_| ());
            }
            store
                .build_insert()
                .time_to_live(ttl)
                .execute(key, value.as_ref())
                .map_err(|err| KvError::Internal(anyhow::anyhow!("insert with ttl failed: {err}")))
        })
    }

    #[inline]
    fn record_binding(&self) -> Option<RecordBinding> {
        self.records.clone()
    }

    #[inline]
    async fn record_delete(&self, key: &str, partition: &RecordPartition) -> Result<(), KvError> {
        self.validate_partition(key, partition)?;
        self.with_store(|store| delete_record(store, key))
    }

    #[inline]
    async fn record_exists_strong(
        &self,
        key: &str,
        partition: &RecordPartition,
    ) -> Result<bool, KvError> {
        self.validate_partition(key, partition)?;
        self.with_store(|store| {
            exact_exists(key, |cursor| strong_list_page(store, key, cursor, 1000))
        })
    }

    #[inline]
    async fn record_insert(
        &self,
        key: &str,
        partition: &RecordPartition,
        write: RecordWrite<'_>,
    ) -> Result<RecordWriteOutcome, KvError> {
        self.validate_partition(key, partition)?;
        self.with_store(|store| insert_record(store, key, write))
    }

    #[inline]
    async fn record_list_partition(
        &self,
        partition: &RecordPartition,
        cursor: Option<&str>,
        limit: usize,
    ) -> Result<RecordPage, KvError> {
        let binding = self.records.as_ref().ok_or_else(unsupported_records)?;
        if binding.layout.partition(partition.prefix())? != *partition {
            return Err(KvError::Validation("record partition mismatch".to_owned()));
        }
        let page =
            self.with_store(|store| strong_list_page(store, partition.prefix(), cursor, limit))?;
        for key in &page.keys {
            self.validate_partition(key, partition)?;
        }
        Ok(RecordPage {
            keys: page.keys,
            cursor: page.cursor,
        })
    }

    #[inline]
    async fn record_lookup(
        &self,
        key: &str,
        partition: &RecordPartition,
    ) -> Result<Option<Record>, KvError> {
        self.validate_partition(key, partition)?;
        self.with_store(|store| match store.lookup(key) {
            Ok(mut response) => Ok(Some(Record {
                generation: RecordGeneration::from_u64(response.current_generation()),
                metadata: decode_metadata(response.metadata().as_deref())?,
                body: Bytes::from(response.take_body_bytes()),
            })),
            Err(KVStoreError::ItemNotFound) => Ok(None),
            Err(_) => Err(KvError::Unavailable),
        })
    }
}

fn record_binding(name: &str, layout: RecordLayout) -> RecordBinding {
    RecordBinding {
        layout,
        scope: name.to_owned(),
        format: "fastly-native-v1".to_owned(),
        max_metadata_bytes: 2000,
    }
}

fn unsupported_records() -> KvError {
    KvError::Unsupported {
        operation: "records".to_owned(),
    }
}

fn decode_metadata(metadata: Option<&[u8]>) -> Result<Option<String>, KvError> {
    metadata
        .filter(|bytes| !bytes.is_empty())
        .map(|bytes| {
            from_utf8(bytes).map(str::to_owned).map_err(|_error| {
                KvError::Internal(anyhow::anyhow!("invalid UTF-8 record metadata"))
            })
        })
        .transpose()
}

fn insert_record(
    store: &KVStore,
    key: &str,
    write: RecordWrite<'_>,
) -> Result<RecordWriteOutcome, KvError> {
    let conditional = !matches!(write.mode, RecordWriteMode::Overwrite);
    let mut request = store.build_insert();
    request = match write.mode {
        RecordWriteMode::Add => request.mode(InsertMode::Add),
        RecordWriteMode::Overwrite => request.mode(InsertMode::Overwrite),
        RecordWriteMode::IfGenerationMatch(generation) => {
            request.if_generation_match(generation.as_u64())
        }
    };
    if let Some(metadata) = write.metadata.filter(|text| !text.is_empty()) {
        if metadata.len() > 2000 {
            return Err(KvError::Validation(
                "Fastly metadata exceeds 2000 bytes".to_owned(),
            ));
        }
        request = request.metadata(metadata);
    }
    if let Some(ttl) = write.ttl {
        request = request.time_to_live(ttl);
    }
    insert_outcome(request.execute(key, write.body), conditional)
}

fn insert_outcome(
    result: Result<(), KVStoreError>,
    conditional: bool,
) -> Result<RecordWriteOutcome, KvError> {
    match result {
        Ok(()) => Ok(RecordWriteOutcome::Written),
        Err(KVStoreError::ItemPreconditionFailed) if conditional => {
            Ok(RecordWriteOutcome::PreconditionFailed)
        }
        Err(error) => Err(mutation_error(&error)),
    }
}

fn mutation_error(error: &KVStoreError) -> KvError {
    match error {
        KVStoreError::InvalidKey
        | KVStoreError::InvalidStoreOptions
        | KVStoreError::ItemBadRequest
        | KVStoreError::ItemPayloadTooLarge => {
            KvError::Validation("Fastly rejected record mutation".to_owned())
        }
        KVStoreError::ItemPreconditionFailed | KVStoreError::ItemNotFound => {
            KvError::Internal(anyhow::anyhow!("unexpected record mutation rejection"))
        }
        KVStoreError::TooManyRequests => KvError::Unavailable,
        KVStoreError::InvalidStoreHandle
        | KVStoreError::StoreNotFound(_)
        | KVStoreError::Unexpected(_)
        | _ => KvError::MutationUncertain,
    }
}

fn delete_record(store: &KVStore, key: &str) -> Result<(), KvError> {
    match store.delete(key) {
        Ok(()) | Err(KVStoreError::ItemNotFound) => Ok(()),
        Err(error) => Err(mutation_error(&error)),
    }
}

fn strong_list_page(
    store: &KVStore,
    prefix: &str,
    cursor: Option<&str>,
    limit: usize,
) -> Result<KvPage, KvError> {
    let limit_u32 = u32::try_from(limit)
        .map_err(|_error| KvError::Validation("list limit exceeds u32".to_owned()))?;
    let pending = store
        .as_handle()
        .list(
            ListMode::Strong,
            cursor.map(str::to_owned),
            Some(limit_u32),
            Some(prefix.to_owned()),
        )
        .map_err(|_error| KvError::Unavailable)?;
    let page = store
        .pending_list_wait(pending)
        .map_err(|_error| KvError::Unavailable)?;
    if !matches!(page.mode(), ListMode::Strong)
        || page.keys().iter().any(|key| !key.starts_with(prefix))
    {
        return Err(KvError::Internal(anyhow::anyhow!(
            "invalid strong list response"
        )));
    }
    let next = page.next_cursor().filter(|token| !token.is_empty());
    if next.as_deref().is_some_and(|token| Some(token) == cursor) {
        return Err(KvError::Internal(anyhow::anyhow!(
            "nonadvancing list cursor"
        )));
    }
    Ok(KvPage {
        keys: page.into_keys(),
        cursor: next,
    })
}

fn exact_exists(
    key: &str,
    mut list: impl FnMut(Option<&str>) -> Result<KvPage, KvError>,
) -> Result<bool, KvError> {
    let mut cursor = None;
    let mut seen = HashSet::new();
    loop {
        let page = list(cursor.as_deref())?;
        if page.keys.iter().any(|candidate| candidate == key) {
            return Ok(true);
        }
        let Some(next) = page.cursor else {
            return Ok(false);
        };
        if next.is_empty() || !seen.insert(next.clone()) {
            return Err(KvError::Internal(anyhow::anyhow!(
                "nonadvancing existence cursor"
            )));
        }
        cursor = Some(next);
    }
}

fn decode_ordinary_cursor(
    binding: &RecordBinding,
    prefix: &str,
    cursor: Option<&str>,
) -> Result<Option<String>, KvError> {
    let Some(token) = cursor else {
        return Ok(None);
    };
    let parsed: OrdinaryCursor = serde_json::from_str(token)
        .map_err(|_error| KvError::Validation("invalid ordinary list cursor".to_owned()))?;
    if parsed.scope != binding.scope
        || parsed.format != binding.format
        || parsed.layout != binding.layout.id()
        || parsed.version != binding.layout.version()
        || parsed.prefix != prefix
        || parsed.native.is_empty()
    {
        return Err(KvError::Validation(
            "ordinary list cursor scope mismatch".to_owned(),
        ));
    }
    Ok(Some(parsed.native))
}

fn encode_ordinary_cursor(
    binding: &RecordBinding,
    prefix: &str,
    cursor: Option<String>,
) -> Result<Option<String>, KvError> {
    cursor
        .map(|native| {
            serde_json::to_string(&OrdinaryCursor {
                scope: binding.scope.clone(),
                format: binding.format.clone(),
                layout: binding.layout.id().to_owned(),
                version: binding.layout.version(),
                prefix: prefix.to_owned(),
                native,
            })
            .map_err(KvError::from)
        })
        .transpose()
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
    use edgezero_core::record_store::RecordKeyLayout;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct DotLayout;

    impl RecordKeyLayout for DotLayout {
        fn id(&self) -> &'static str {
            "fastly-test"
        }
        fn partition_prefix<'key>(&self, key: &'key str) -> Result<&'key str, KvError> {
            let end = key
                .find('.')
                .ok_or_else(|| KvError::Validation("missing partition".to_owned()))?;
            key.get(..=end)
                .ok_or_else(|| KvError::Validation("invalid partition".to_owned()))
        }
        fn validate(&self) -> Result<(), KvError> {
            Ok(())
        }
        fn validate_partition(&self, prefix: &str) -> Result<(), KvError> {
            if prefix.ends_with('.') && prefix.matches('.').count() == 1 {
                Ok(())
            } else {
                Err(KvError::Validation("invalid partition".to_owned()))
            }
        }
        fn version(&self) -> u32 {
            1
        }
    }

    #[test]
    fn record_binding_is_immutable_and_acquisition_needs_no_native_io() {
        let layout = RecordLayout::new(DotLayout).unwrap();
        let store = FastlyKvStore::declared_records("test-store", layout.clone());
        let binding = store.record_binding().unwrap();
        assert_eq!(binding.layout, layout);
        assert_eq!(binding.max_metadata_bytes, 2000);
        store.validate_point("group.key").unwrap();
        store.validate_point("invalid").unwrap_err();
        store
            .validate_partition("group.key", &layout.partition("other.").unwrap())
            .unwrap_err();
        assert!(
            FastlyKvStore::declared("ordinary")
                .record_binding()
                .is_none()
        );
        FastlyKvStore::declared("ordinary")
            .validate_point("invalid")
            .unwrap();
    }

    #[test]
    fn metadata_is_text_and_preserves_nonempty_bytes() {
        assert_eq!(decode_metadata(None).unwrap(), None);
        assert_eq!(decode_metadata(Some(b"")).unwrap(), None);
        assert_eq!(
            decode_metadata(Some("a\u{96ea}\n".as_bytes()))
                .unwrap()
                .as_deref(),
            Some("a\u{96ea}\n")
        );
        decode_metadata(Some(&[0xff, 0xfe])).unwrap_err();
    }

    #[test]
    fn only_conditional_precondition_rejection_is_contention() {
        for conditional in [true, false] {
            assert!(matches!(
                insert_outcome(Ok(()), conditional),
                Ok(RecordWriteOutcome::Written)
            ));
            assert!(matches!(
                insert_outcome(
                    Err(KVStoreError::Unexpected(FastlyStatus::ERROR)),
                    conditional
                ),
                Err(KvError::MutationUncertain)
            ));
            assert!(matches!(
                insert_outcome(Err(KVStoreError::TooManyRequests), conditional),
                Err(KvError::Unavailable)
            ));
        }
        assert!(matches!(
            insert_outcome(Err(KVStoreError::ItemPreconditionFailed), true),
            Ok(RecordWriteOutcome::PreconditionFailed)
        ));
        assert!(matches!(
            insert_outcome(Err(KVStoreError::ItemPreconditionFailed), false),
            Err(KvError::Internal(_))
        ));
        let error = mutation_error(&KVStoreError::StoreNotFound(
            "secret-provider-payload".to_owned(),
        ));
        assert!(matches!(error, KvError::MutationUncertain));
        assert!(!format!("{error:?} {error}").contains("secret-provider-payload"));
    }

    #[test]
    fn exact_existence_follows_empty_and_neighbour_pages_until_exact_match() {
        let mut calls = 0_usize;
        assert!(
            exact_exists("group.key", |cursor| {
                calls += 1;
                match calls {
                    1 => {
                        assert_eq!(cursor, None);
                        Ok(KvPage {
                            keys: vec!["group.key.child".to_owned()],
                            cursor: Some("one".to_owned()),
                        })
                    }
                    2 => {
                        assert_eq!(cursor, Some("one"));
                        Ok(KvPage {
                            keys: vec![],
                            cursor: Some("two".to_owned()),
                        })
                    }
                    3 => {
                        assert_eq!(cursor, Some("two"));
                        Ok(KvPage {
                            keys: vec!["group.key".to_owned()],
                            cursor: None,
                        })
                    }
                    _ => panic!("unexpected extra page"),
                }
            })
            .unwrap()
        );
        assert_eq!(calls, 3);
    }

    #[test]
    fn exact_existence_does_not_infer_absence_before_end_or_error() {
        let mut calls = 0_usize;
        assert!(
            !exact_exists("group.key", |_| {
                calls += 1;
                Ok(KvPage {
                    keys: vec!["group.key.child".to_owned()],
                    cursor: (calls == 1).then(|| "next".to_owned()),
                })
            })
            .unwrap()
        );
        assert_eq!(calls, 2);
        assert!(matches!(
            exact_exists("group.key", |_| Err(KvError::Unavailable)),
            Err(KvError::Unavailable)
        ));
        exact_exists("group.key", |_| {
            Ok(KvPage {
                keys: vec![],
                cursor: Some("stuck".to_owned()),
            })
        })
        .unwrap_err();
    }

    #[test]
    fn ordinary_cursor_preserves_native_position_and_rejects_other_scopes() {
        let layout = RecordLayout::new(DotLayout).unwrap();
        let binding = record_binding("first", layout.clone());
        let cursor = encode_ordinary_cursor(&binding, "", Some("native-position".to_owned()))
            .unwrap()
            .unwrap();
        assert_eq!(
            decode_ordinary_cursor(&binding, "", Some(&cursor))
                .unwrap()
                .as_deref(),
            Some("native-position")
        );
        decode_ordinary_cursor(&binding, "group.", Some(&cursor)).unwrap_err();
        decode_ordinary_cursor(&record_binding("other", layout), "", Some(&cursor)).unwrap_err();
        decode_ordinary_cursor(&binding, "", Some("native-position")).unwrap_err();
        decode_ordinary_cursor(
            &binding,
            "",
            Some(r#"{"scope":"first","native":"record-cursor"}"#),
        )
        .unwrap_err();
    }

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
