//! Portable conditional records over the same value rows as ordinary KV.
//!
//! Record capability is established during provider preparation, without a write
//! probe. Lookup consistency and TTL visibility remain provider-specific. Exact
//! existence and every partition-list page are authoritative. A traversal is live,
//! not a snapshot, and ends only when its continuation is absent.

/// Reusable assertions over the public handle for real provider fixtures.
///
/// These do not establish native race, restart, expiry timing or fault-injection
/// behavior. Those require independent provider-specific runtime checks.
#[cfg(any(test, feature = "test-utils"))]
pub mod contract {
    #![expect(
        clippy::expect_used,
        clippy::missing_assert_message,
        clippy::unwrap_used,
        reason = "shared test assertions intentionally panic at the failed provider operation or assertion"
    )]

    use std::collections::BTreeSet;

    use bytes::Bytes;

    use super::{RecordGeneration, RecordHandle, RecordWrite, RecordWriteMode, RecordWriteOutcome};
    use crate::key_value_store::KvHandle;

    async fn assert_coherence(kv: &KvHandle, records: &RecordHandle, partition_prefix: &str) {
        let key = format!("{partition_prefix}contract-main");
        assert_conditionals(kv, records, &key).await;
        assert_replacements(kv, records, &key).await;
        assert_deletion(kv, records, &key).await;
    }

    async fn assert_conditionals(kv: &KvHandle, records: &RecordHandle, key: &str) {
        let body = b"\x00\xffrecord\x00";
        assert!(!records.exists_strong(key).await.unwrap());
        let mut add = write(body, RecordWriteMode::Add);
        add.metadata = Some("\u{00e9}\u{6587}\u{5b57}");
        assert_eq!(
            records.insert(key, add).await.unwrap(),
            RecordWriteOutcome::Written
        );
        assert_eq!(
            records.insert(key, add).await.unwrap(),
            RecordWriteOutcome::PreconditionFailed
        );
        let first = records.lookup(key).await.unwrap().unwrap();
        assert_eq!(first.body.as_ref(), body);
        assert_eq!(first.metadata.as_deref(), add.metadata);
        assert_eq!(kv.get_bytes(key).await.unwrap().unwrap().as_ref(), body);
        assert!(records.exists_strong(key).await.unwrap());
        assert!(
            !records
                .exists_strong(&format!("{key}-missing"))
                .await
                .unwrap()
        );

        let mut cas = write(b"cas", RecordWriteMode::IfGenerationMatch(first.generation));
        cas.metadata = add.metadata;
        assert_eq!(
            records.insert(key, cas).await.unwrap(),
            RecordWriteOutcome::Written
        );
        assert_eq!(
            records.insert(key, cas).await.unwrap(),
            RecordWriteOutcome::PreconditionFailed
        );
    }

    async fn assert_deletion(kv: &KvHandle, records: &RecordHandle, key: &str) {
        let before_delete = records.lookup(key).await.unwrap().unwrap().generation;
        records.delete(key).await.unwrap();
        assert_eq!(kv.get_bytes(key).await.unwrap(), None);
        assert!(!records.exists_strong(key).await.unwrap());
        records
            .insert(key, write(b"recreated", RecordWriteMode::Add))
            .await
            .unwrap();
        assert_stale(records, key, before_delete).await;
        let mut empty_metadata = write(b"\x00\xffrecord\x00", RecordWriteMode::Overwrite);
        empty_metadata.metadata = Some("");
        records.insert(key, empty_metadata).await.unwrap();
        assert_eq!(records.lookup(key).await.unwrap().unwrap().metadata, None);
        kv.delete(key).await.unwrap();
        assert!(!records.exists_strong(key).await.unwrap());
    }

    async fn assert_pagination(kv: &KvHandle, records: &RecordHandle, partition_prefix: &str) {
        let partition = records
            .partition(partition_prefix)
            .expect("complete partition");
        let expected: BTreeSet<String> = (0_usize..4_usize)
            .map(|index| format!("{partition_prefix}contract-page-{index}"))
            .collect();
        for key in &expected {
            records
                .insert(key, write(b"\x00\xffrecord\x00", RecordWriteMode::Add))
                .await
                .unwrap();
        }
        let mut cursor = None;
        let mut seen_cursors = BTreeSet::new();
        let mut actual = BTreeSet::new();
        loop {
            let page = records
                .list_partition(&partition, cursor.as_deref(), 2)
                .await
                .unwrap();
            assert!(page.keys.len() <= 2);
            for key in page.keys {
                assert!(actual.insert(key), "duplicate partition key");
            }
            cursor = page.cursor;
            let Some(next) = &cursor else { break };
            assert!(
                seen_cursors.insert(next.clone()),
                "repeated partition continuation"
            );
        }
        assert_eq!(actual, expected);

        // Comparing sets does not require provider ordering. Empty-prefix ordinary
        // listing is independent of complete-partition validation.
        let mut ordinary_cursor = None;
        let mut ordinary_seen_cursors = BTreeSet::new();
        let mut ordinary_keys = BTreeSet::new();
        loop {
            let page = kv
                .list_keys_page("", ordinary_cursor.as_deref(), 2)
                .await
                .unwrap();
            assert!(page.keys.len() <= 2);
            for key in page.keys {
                assert!(ordinary_keys.insert(key), "duplicate ordinary key");
            }
            ordinary_cursor = page.cursor;
            let Some(next) = &ordinary_cursor else { break };
            assert!(
                ordinary_seen_cursors.insert(next.clone()),
                "repeated ordinary continuation"
            );
        }
        assert_eq!(ordinary_keys, expected);
        for key in expected {
            records.delete(&key).await.unwrap();
        }
    }

    /// Exercise coherence, atomic precondition outcomes, generation invalidation,
    /// text/body fidelity and complete unordered partition/global pagination.
    ///
    /// The fixture must provide an isolated empty store and accept keys of the
    /// form of the supplied prefix followed by `contract-*`. Run in a quiescent fixture
    /// where acknowledged writes are observable by subsequent lookups. This is a
    /// fixture condition, not a portable strong-lookup promise. No mutation is retried.
    ///
    /// # Panics
    /// Panics on a failed contract assertion or provider error.
    #[inline]
    pub async fn assert_record_contract(kv: &KvHandle, partition_prefix: &str) {
        let records = kv.require_records().expect("prepared record binding");
        assert_coherence(kv, &records, partition_prefix).await;
        assert_pagination(kv, &records, partition_prefix).await;
    }

    async fn assert_replacements(kv: &KvHandle, records: &RecordHandle, key: &str) {
        let before_ordinary = records.lookup(key).await.unwrap().unwrap().generation;
        kv.put_bytes(key, Bytes::from_static(b"ordinary"))
            .await
            .unwrap();
        let ordinary = records.lookup(key).await.unwrap().unwrap();
        assert_eq!(ordinary.body.as_ref(), b"ordinary");
        assert_eq!(ordinary.metadata, None);
        assert_ne!(ordinary.generation, before_ordinary);
        assert_stale(records, key, before_ordinary).await;

        let mut with_metadata = write(b"metadata", RecordWriteMode::Overwrite);
        with_metadata.metadata = Some("\u{00e9}\u{6587}\u{5b57}");
        records.insert(key, with_metadata).await.unwrap();
        let before_ttl = records.lookup(key).await.unwrap().unwrap().generation;
        kv.put_bytes_with_ttl(key, Bytes::from_static(b"ttl"), KvHandle::MIN_TTL)
            .await
            .unwrap();
        assert_stale(records, key, before_ttl).await;
        let ttl_record = records.lookup(key).await.unwrap().unwrap();
        assert_eq!(ttl_record.body.as_ref(), b"ttl");
        assert_eq!(ttl_record.metadata, None);
        kv.put_bytes(key, Bytes::from_static(b"no-expiry"))
            .await
            .unwrap();
        assert_stale(records, key, ttl_record.generation).await;
    }

    async fn assert_stale(records: &RecordHandle, key: &str, generation: RecordGeneration) {
        assert_eq!(
            records
                .insert(
                    key,
                    write(
                        b"must-not-commit",
                        RecordWriteMode::IfGenerationMatch(generation)
                    )
                )
                .await
                .unwrap(),
            RecordWriteOutcome::PreconditionFailed,
        );
    }

    fn write(body: &[u8], mode: RecordWriteMode) -> RecordWrite<'_> {
        RecordWrite {
            body,
            metadata: None,
            mode,
            ttl: None,
        }
    }
}

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::key_value_store::{KvError, KvHandle, KvStore};

/// An equality token scoped to one key and binding, not an ordered counter.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct RecordGeneration(u64);

impl RecordGeneration {
    /// Encode a token for a provider. The number has no ordering guarantee.
    #[must_use]
    #[inline]
    pub const fn as_u64(self) -> u64 {
        self.0
    }

    /// Construct a token from its provider representation.
    #[must_use]
    #[inline]
    pub const fn from_u64(value: u64) -> Self {
        Self(value)
    }
}

/// A record's exact body, optional UTF-8 metadata and generation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Record {
    /// Exact value bytes, without an envelope.
    pub body: Bytes,
    /// Equality token for this key and binding.
    pub generation: RecordGeneration,
    /// Nonempty UTF-8 text. Empty metadata is normalized to absence.
    pub metadata: Option<String>,
}

/// A replacement whose precondition is evaluated atomically by the provider.
#[derive(Clone, Copy, Debug)]
pub struct RecordWrite<'value> {
    /// Replacement value bytes.
    pub body: &'value [u8],
    /// Optional UTF-8 text, normalized before provider I/O.
    pub metadata: Option<&'value str>,
    /// Provider-atomic precondition.
    pub mode: RecordWriteMode,
    /// Native provider TTL, or no expiry.
    pub ttl: Option<Duration>,
}

/// Atomic replacement preconditions. Overwrite has no generation precondition.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RecordWriteMode {
    /// Write only when the key does not exist under native expiry rules.
    Add,
    /// Write only while this key still has the supplied generation.
    IfGenerationMatch(RecordGeneration),
    /// Replace without a generation precondition.
    Overwrite,
}

/// An acknowledged mutation or a definitive Add/CAS precondition rejection.
///
/// Acknowledgement does not return a new generation. A later lookup may observe
/// another writer. Ambiguous completion is [`KvError::MutationUncertain`], not an
/// outcome, and must not cause automatic replay.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RecordWriteOutcome {
    /// Definitive Add/CAS rejection. The replacement did not occur.
    PreconditionFailed,
    /// The provider acknowledged the write, without returning its generation.
    Written,
}

/// Full logical keys from one strong partition page, without ordering promises.
/// Empty and short pages may have continuation. Only `cursor == None` ends a traversal.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RecordPage {
    /// Opaque continuation. Only absence establishes completion.
    pub cursor: Option<String>,
    /// Full logical keys, without an ordering guarantee.
    pub keys: Vec<String>,
}

/// Application-owned rules for stable, disjoint, enumerable key prefixes.
///
/// Implementations validate their rules in [`Self::validate`]. Equal id/version
/// pairs must describe identical rules for the lifetime of the stored format.
/// Partition prefixes must be disjoint and must preserve the original key bytes.
/// Changing placement requires an explicit format/cutover choice, not new rules
/// under the same identity. Core does not parse application key formats.
pub trait RecordKeyLayout: Send + Sync {
    /// Stable application format identity.
    fn id(&self) -> &str;

    /// Resolve a full key to a borrowed, complete partition prefix of that key.
    ///
    /// # Errors
    /// Rejects keys outside the declared format or with ambiguous membership.
    fn partition_prefix<'key>(&self, key: &'key str) -> Result<&'key str, KvError>;

    /// Validate the rule declaration, including stable, non-overlapping partitions.
    ///
    /// # Errors
    /// Returns a validation error when the declaration cannot satisfy the contract.
    fn validate(&self) -> Result<(), KvError>;

    /// Accept only complete enumerable partition prefixes, never partial groups.
    ///
    /// # Errors
    /// Rejects a prefix that does not identify exactly one declared partition.
    fn validate_partition(&self, prefix: &str) -> Result<(), KvError>;

    /// Version of the stored key layout, captured when the declaration is validated.
    fn version(&self) -> u32;
}

/// Validated application rules with a captured immutable identity.
#[derive(Clone)]
pub struct RecordLayout {
    id: String,
    rules: Arc<dyn RecordKeyLayout>,
    version: u32,
}

impl fmt::Debug for RecordLayout {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RecordLayout").finish_non_exhaustive()
    }
}

#[expect(
    clippy::missing_trait_methods,
    reason = "retain default ne as the negation of captured-identity equality"
)]
impl PartialEq for RecordLayout {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id && self.version == other.version
    }
}

#[expect(
    clippy::missing_trait_methods,
    reason = "Eq marker assertion methods are unstable compiler machinery"
)]
impl Eq for RecordLayout {}

impl RecordLayout {
    /// Captured stable layout identity.
    #[must_use]
    #[inline]
    pub fn id(&self) -> &str {
        &self.id
    }

    /// Validate a declaration and capture its stable identity once.
    ///
    /// # Errors
    /// Returns declaration errors or rejects an empty layout identity.
    #[inline]
    pub fn new<KeyLayout>(layout: KeyLayout) -> Result<Self, KvError>
    where
        KeyLayout: RecordKeyLayout + 'static,
    {
        layout.validate()?;
        let id = layout.id().to_owned();
        if id.is_empty() {
            return Err(KvError::Validation(
                "record layout identity is empty".to_owned(),
            ));
        }
        Ok(Self {
            version: layout.version(),
            id,
            rules: Arc::new(layout),
        })
    }

    /// Validate a complete enumerable prefix. Ordinary listing does not use this check.
    ///
    /// # Errors
    /// Rejects invalid prefixes or incomplete/undeclared partitions.
    #[inline]
    pub fn partition(&self, prefix: &str) -> Result<RecordPartition, KvError> {
        KvHandle::validate_prefix(prefix)?;
        self.rules.validate_partition(prefix)?;
        Ok(RecordPartition {
            layout_id: self.id.clone(),
            layout_version: self.version,
            prefix: prefix.to_owned(),
        })
    }

    /// Resolve a full logical key and validate its actual prefix membership.
    ///
    /// # Errors
    /// Rejects invalid keys, undeclared keys or an inconsistent layout result.
    #[inline]
    pub fn resolve(&self, key: &str) -> Result<RecordPartition, KvError> {
        KvHandle::validate_key(key)?;
        let prefix = self.rules.partition_prefix(key)?;
        if !key.starts_with(prefix) {
            return Err(KvError::Validation(
                "record layout resolved a prefix outside the key".to_owned(),
            ));
        }
        self.partition(prefix)
    }

    /// Captured layout version.
    #[must_use]
    #[inline]
    pub const fn version(&self) -> u32 {
        self.version
    }
}

/// A complete partition validated against an immutable application layout.
#[derive(Clone, Eq, PartialEq)]
pub struct RecordPartition {
    layout_id: String,
    layout_version: u32,
    prefix: String,
}

impl fmt::Debug for RecordPartition {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RecordPartition").finish_non_exhaustive()
    }
}

impl RecordPartition {
    /// Full logical prefix identifying this complete partition.
    #[must_use]
    #[inline]
    pub fn prefix(&self) -> &str {
        &self.prefix
    }
}

/// Immutable per-logical-ID requirements installed during application configuration.
pub type RecordStoreRequirements = BTreeMap<String, RecordLayout>;

/// Prepared record capability for one provider and backing scope.
///
/// `scope` must be stable, non-secret and identify the bound backing namespace.
/// `format` identifies its explicit stored record format. These identities bind
/// cursors; they are not permissions or availability guarantees. Providers must
/// route ordinary operations to the same value rows, invalidate generations on
/// every replacement, and retain generation history across deletion and expiry.
#[derive(Clone)]
pub struct RecordBinding {
    /// Explicit provider record-format identity.
    pub format: String,
    /// Immutable, validated application layout.
    pub layout: RecordLayout,
    /// Actual provider limit in UTF-8 bytes, excluding absent/empty metadata.
    pub max_metadata_bytes: usize,
    /// Stable, non-secret backing namespace identity. Omitted from Debug output.
    pub scope: String,
}

impl fmt::Debug for RecordBinding {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RecordBinding").finish_non_exhaustive()
    }
}

#[derive(Deserialize, Serialize)]
struct RecordCursor {
    cursor: String,
    format: String,
    layout_id: String,
    layout_version: u32,
    operation: String,
    partition: String,
    scope: String,
}

/// A validated record view over the exact provider used by its ordinary KV handle.
#[derive(Clone)]
pub struct RecordHandle {
    binding: RecordBinding,
    store: Arc<dyn KvStore>,
}

impl fmt::Debug for RecordHandle {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RecordHandle").finish_non_exhaustive()
    }
}

impl RecordHandle {
    /// Immutable capability captured from the prepared provider.
    #[must_use]
    #[inline]
    pub fn binding(&self) -> &RecordBinding {
        &self.binding
    }

    fn decode_cursor(&self, partition: &RecordPartition, encoded: &str) -> Result<String, KvError> {
        let cursor: RecordCursor = serde_json::from_str(encoded)
            .map_err(|_error| KvError::Validation("record cursor is invalid".to_owned()))?;
        if cursor.scope != self.binding.scope
            || cursor.format != self.binding.format
            || cursor.layout_id != self.binding.layout.id
            || cursor.layout_version != self.binding.layout.version
            || cursor.partition != partition.prefix
            || cursor.operation != "partition"
            || cursor.cursor.is_empty()
        {
            return Err(KvError::Validation(
                "record cursor scope mismatch".to_owned(),
            ));
        }
        Ok(cursor.cursor)
    }

    /// Delete a record without replay. Deleting a missing value succeeds.
    ///
    /// # Errors
    /// Returns validation/provider errors, including possible mutation completion.
    #[inline]
    pub async fn delete(&self, key: &str) -> Result<(), KvError> {
        let partition = self.binding.layout.resolve(key)?;
        self.store.record_delete(key, &partition).await
    }

    /// Check exact-key existence against authoritative provider state.
    ///
    /// # Errors
    /// Returns validation/provider errors when authority cannot be established.
    #[inline]
    pub async fn exists_strong(&self, key: &str) -> Result<bool, KvError> {
        let partition = self.binding.layout.resolve(key)?;
        self.store.record_exists_strong(key, &partition).await
    }

    /// Atomically replace a record. This method never retries a mutation.
    ///
    /// # Errors
    /// Returns input/provider failures, including possible completion as
    /// [`KvError::MutationUncertain`]. An unconditional rejection is not contention.
    #[inline]
    pub async fn insert(
        &self,
        key: &str,
        mut write: RecordWrite<'_>,
    ) -> Result<RecordWriteOutcome, KvError> {
        let partition = self.binding.layout.resolve(key)?;
        KvHandle::validate_value(write.body)?;
        if let Some(ttl) = write.ttl {
            KvHandle::validate_ttl(ttl)?;
        }
        write.metadata = write.metadata.filter(|text| !text.is_empty());
        if write
            .metadata
            .is_some_and(|text| text.len() > self.binding.max_metadata_bytes)
        {
            return Err(KvError::Validation(
                "record metadata exceeds the provider byte limit".to_owned(),
            ));
        }
        let mode = write.mode;
        let outcome = self.store.record_insert(key, &partition, write).await?;
        if mode == RecordWriteMode::Overwrite && outcome == RecordWriteOutcome::PreconditionFailed {
            return Err(KvError::Internal(anyhow::anyhow!(
                "record provider rejected an unconditional write as contention"
            )));
        }
        Ok(outcome)
    }

    /// Read one strongly consistent page from a complete partition.
    ///
    /// The opaque continuation is bound to backing scope, format, layout and
    /// partition/query mode. Providers receive their native continuation only.
    /// The traversal is live, unordered and complete only when continuation is absent.
    ///
    /// # Errors
    /// Rejects foreign/malformed/nonadvancing cursors, invalid partitions/limits,
    /// invalid provider keys or provider failures.
    #[inline]
    pub async fn list_partition(
        &self,
        partition: &RecordPartition,
        cursor: Option<&str>,
        limit: usize,
    ) -> Result<RecordPage, KvError> {
        KvHandle::validate_list_limit(limit)?;
        if self.binding.layout.partition(partition.prefix())? != *partition {
            return Err(KvError::Validation(
                "record partition layout mismatch".to_owned(),
            ));
        }
        let decoded = cursor
            .map(|encoded| self.decode_cursor(partition, encoded))
            .transpose()?;
        let mut page = self
            .store
            .record_list_partition(partition, decoded.as_deref(), limit)
            .await?;
        if page.keys.len() > limit
            || page.keys.iter().any(|key| {
                !self
                    .binding
                    .layout
                    .resolve(key)
                    .is_ok_and(|resolved| resolved == *partition)
            })
        {
            return Err(KvError::Internal(anyhow::anyhow!(
                "record provider returned an invalid partition page"
            )));
        }
        if page
            .cursor
            .as_deref()
            .is_some_and(|next| next.is_empty() || Some(next) == decoded.as_deref())
        {
            return Err(KvError::Internal(anyhow::anyhow!(
                "record provider returned a nonadvancing continuation"
            )));
        }
        page.cursor = page
            .cursor
            .map(|native| {
                serde_json::to_string(&RecordCursor {
                    scope: self.binding.scope.clone(),
                    format: self.binding.format.clone(),
                    layout_id: self.binding.layout.id.clone(),
                    layout_version: self.binding.layout.version,
                    partition: partition.prefix.clone(),
                    operation: "partition".to_owned(),
                    cursor: native,
                })
                .map_err(KvError::from)
            })
            .transpose()?;
        Ok(page)
    }

    /// Read a generation-bearing record. A miss need not prove authoritative absence.
    ///
    /// # Errors
    /// Returns key/layout validation errors or provider read/decoding failures.
    #[inline]
    pub async fn lookup(&self, key: &str) -> Result<Option<Record>, KvError> {
        let partition = self.binding.layout.resolve(key)?;
        let mut result = self.store.record_lookup(key, &partition).await?;
        if let Some(record) = &mut result
            && record.metadata.as_deref() == Some("")
        {
            record.metadata = None;
        }
        Ok(result)
    }

    #[inline]
    pub(crate) fn new(store: Arc<dyn KvStore>) -> Result<Self, KvError> {
        let binding = store.record_binding().ok_or_else(|| KvError::Unsupported {
            operation: "records".to_owned(),
        })?;
        if binding.scope.is_empty() || binding.format.is_empty() {
            return Err(KvError::Validation(
                "record binding scope and format must be declared".to_owned(),
            ));
        }
        Ok(Self { binding, store })
    }

    /// Validate a complete partition without I/O or routing mutation.
    ///
    /// # Errors
    /// Rejects invalid or incomplete partition prefixes.
    #[inline]
    pub fn partition(&self, prefix: &str) -> Result<RecordPartition, KvError> {
        self.binding.layout.partition(prefix)
    }
}
