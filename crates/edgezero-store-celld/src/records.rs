//! Live conditional records, separate from ordinary KV and its generic interface.
//!
//! The server owns atomic preconditions, durable generations and expiry. This
//! client never emulates a conditional write with a read followed by a raw put.

#![expect(
    clippy::arbitrary_source_item_ordering,
    reason = "keep the public record contract before its transport and wire validators"
)]

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use bytes::Bytes;
use edgezero_core::time::Deadline;
use reqwest::Method;
use serde::{Deserialize, Deserializer, Serialize};
use url::Url;

use crate::PreparationError;
use crate::client::{ResponseKind, ScopedClient};
use crate::settings::{CelldPreparationLimits, valid_key};

/// Maximum opaque metadata bytes accepted by the conditional-record protocol.
pub const MAX_RECORD_METADATA_BYTES: usize = 1_024;

/// A live row and the generation required for a conditional update.
#[derive(Clone)]
pub struct CelldRecord {
    /// Opaque stored body bytes.
    pub body: Vec<u8>,
    /// Absent metadata is distinct from present, empty metadata.
    pub metadata: Option<Vec<u8>>,
    /// Positive server-assigned generation, never a client-side counter.
    pub generation: u64,
}

impl fmt::Debug for CelldRecord {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CelldRecord").finish_non_exhaustive()
    }
}

/// Server-enforced write precondition.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RecordWriteMode {
    /// Create only when no live record exists.
    Add,
    /// Replace a record without a generation precondition.
    Overwrite,
    /// Replace only a live record with this positive generation.
    IfGenerationMatch(u64),
}

/// One atomic write. The server starts the relative TTL inside its transaction.
pub struct RecordWrite<'record> {
    /// Opaque record body, at most the configured value limit.
    pub body: &'record [u8],
    /// Opaque optional metadata, at most [`MAX_RECORD_METADATA_BYTES`].
    pub metadata: Option<&'record [u8]>,
    /// Positive lifetime, rounded upward to whole milliseconds on the wire.
    pub ttl: Duration,
    /// Condition evaluated atomically with the generation allocation and write.
    pub mode: RecordWriteMode,
}

/// A rejected precondition is normal contention, not a transport failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RecordWriteOutcome {
    /// The server acknowledged the write.
    Written,
    /// The key was live for Add, or the expected generation did not match.
    PreconditionFailed,
}

/// Cloneable live conditional-record client with no row cache or mutation replay.
///
/// Construct through [`crate::CelldPreparation::prepare_records`]. Preparation
/// proves the record protocol and listing permission, not write authorization or
/// fleet durability. A failed mutation may have applied; errors remain errors.
#[derive(Clone)]
pub struct CelldRecordStore {
    client: Arc<ScopedClient>,
    store: String,
    limits: CelldPreparationLimits,
}

impl fmt::Debug for CelldRecordStore {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CelldRecordStore").finish_non_exhaustive()
    }
}

impl CelldRecordStore {
    pub(crate) const fn new(
        client: Arc<ScopedClient>,
        store: String,
        limits: CelldPreparationLimits,
    ) -> Self {
        Self {
            client,
            store,
            limits,
        }
    }

    /// Reads a live record. Logical expiry is a miss, not stale data.
    ///
    /// # Errors
    /// Rejects invalid keys, malformed records and any service/transport failure.
    #[inline]
    pub async fn lookup(&self, key: &str) -> Result<Option<CelldRecord>, PreparationError> {
        let url = self.record_url(key)?;
        let Some(bytes) = self
            .client
            .execute(
                Method::GET,
                url,
                None,
                self.record_cap()?,
                self.client.deadline()?,
                ResponseKind::Record,
            )
            .await?
        else {
            return Ok(None);
        };
        let wire: RecordBody =
            serde_json::from_slice(&bytes).map_err(|_error| PreparationError::Malformed)?;
        Ok(Some(CelldRecord {
            body: decode(
                &wire.body,
                usize::try_from(self.limits.max_value_bytes)
                    .map_err(|_error| PreparationError::InvalidLimits)?,
            )?,
            metadata: wire
                .metadata
                .map(|value| decode(&value, MAX_RECORD_METADATA_BYTES))
                .transpose()?,
            generation: generation(&wire.generation)?,
        }))
    }

    /// Checks exact physical existence, including an expired row not yet swept.
    ///
    /// This is not ordinary KV HEAD and never substitutes prefix matching or a
    /// cached/logically filtered miss for the server's authoritative check.
    ///
    /// # Errors
    /// Inconclusive checks and all service/transport failures are errors.
    #[inline]
    pub async fn key_exists(&self, key: &str) -> Result<bool, PreparationError> {
        self.client
            .execute(
                Method::HEAD,
                self.record_url(key)?,
                None,
                0,
                self.client.deadline()?,
                ResponseKind::RecordExists,
            )
            .await
            .map(|body| body.is_some())
    }

    /// Writes under the server's atomic precondition without automatic replay.
    ///
    /// # Errors
    /// Rejects invalid input, protocol failures and ambiguous mutation failures.
    #[inline]
    pub async fn insert(
        &self,
        key: &str,
        write: RecordWrite<'_>,
    ) -> Result<RecordWriteOutcome, PreparationError> {
        let url = self.record_url(key)?;
        if u64::try_from(write.body.len()).map_err(|_error| PreparationError::SizeLimit)?
            > self.limits.max_value_bytes
            || write
                .metadata
                .is_some_and(|value| value.len() > MAX_RECORD_METADATA_BYTES)
        {
            return Err(PreparationError::SizeLimit);
        }
        let ttl = u64::try_from(write.ttl.as_millis())
            .ok()
            .and_then(|millis| {
                millis.checked_add(u64::from(
                    !write.ttl.subsec_nanos().is_multiple_of(1_000_000),
                ))
            })
            .filter(|millis| *millis > 0)
            .ok_or(PreparationError::InvalidResource)?;
        let (mode, expected) = match write.mode {
            RecordWriteMode::Add => ("add", None),
            RecordWriteMode::Overwrite => ("overwrite", None),
            RecordWriteMode::IfGenerationMatch(0) => return Err(PreparationError::InvalidResource),
            RecordWriteMode::IfGenerationMatch(value) => {
                ("if-generation-match", Some(value.to_string()))
            }
        };
        let bytes = serde_json::to_vec(&WriteBody {
            body: STANDARD.encode(write.body),
            metadata: write.metadata.map(|value| STANDARD.encode(value)),
            ttl_ms: ttl.to_string(),
            mode,
            generation: expected,
        })
        .map_err(|_error| PreparationError::InvalidResource)?;
        match self
            .client
            .execute(
                Method::PUT,
                url,
                Some(Bytes::from(bytes)),
                0,
                self.client.deadline()?,
                ResponseKind::RecordMutation,
            )
            .await
        {
            Ok(_) => Ok(RecordWriteOutcome::Written),
            Err(PreparationError::PreconditionFailed)
                if write.mode != RecordWriteMode::Overwrite =>
            {
                Ok(RecordWriteOutcome::PreconditionFailed)
            }
            Err(PreparationError::PreconditionFailed) => Err(PreparationError::Malformed),
            Err(error) => Err(error),
        }
    }

    /// Returns one authoritative prefix page, never a complete-scan promise.
    ///
    /// A full page can be truncated. Callers needing completeness must treat it
    /// conservatively. Zero returns an empty page without a network call.
    ///
    /// # Errors
    /// Rejects invalid prefixes, limits above 1000, malformed pages and I/O errors.
    #[inline]
    pub async fn list_keys_with_prefix(
        &self,
        prefix: &str,
        limit: u32,
    ) -> Result<Vec<String>, PreparationError> {
        self.list_with_deadline(prefix, limit, self.client.deadline()?)
            .await
    }

    /// Hard-deletes a record, leaving the server's generation allocator intact.
    ///
    /// # Errors
    /// Invalid keys and ambiguous/service failures are errors, never a false ack.
    #[inline]
    pub async fn delete(&self, key: &str) -> Result<(), PreparationError> {
        self.client
            .execute(
                Method::DELETE,
                self.record_url(key)?,
                None,
                0,
                self.client.deadline()?,
                ResponseKind::RecordMutation,
            )
            .await
            .map(|_| ())
    }

    pub(crate) async fn probe(&self, deadline: Deadline) -> Result<(), PreparationError> {
        let probe_deadline =
            Deadline::at_instant(deadline.instant().min(self.client.deadline()?.instant()));
        self.list_with_deadline("", 1, probe_deadline)
            .await
            .map(|_| ())
    }

    fn record_url(&self, key: &str) -> Result<Url, PreparationError> {
        if !valid_key(key, false) {
            return Err(PreparationError::InvalidResource);
        }
        let mut url = self.client.url(&self.store, "record")?;
        url.query_pairs_mut().append_pair("key", key);
        Ok(url)
    }

    fn record_cap(&self) -> Result<usize, PreparationError> {
        let value = usize::try_from(self.limits.max_value_bytes)
            .map_err(|_error| PreparationError::InvalidLimits)?;
        // Base64 value + base64 metadata + fixed JSON fields and u64 strings.
        encoded_len(value)?
            .checked_add(encoded_len(MAX_RECORD_METADATA_BYTES)?)
            .and_then(|bytes| bytes.checked_add(256))
            .ok_or(PreparationError::SizeLimit)
    }

    async fn list_with_deadline(
        &self,
        prefix: &str,
        limit: u32,
        deadline: Deadline,
    ) -> Result<Vec<String>, PreparationError> {
        if !valid_key(prefix, true) || limit > 1_000 {
            return Err(PreparationError::InvalidResource);
        }
        if limit == 0 {
            return Ok(Vec::new());
        }
        let mut url = self.client.url(&self.store, "record-keys")?;
        url.query_pairs_mut()
            .append_pair("prefix", prefix)
            .append_pair("limit", &limit.to_string());
        let bytes = self
            .client
            .execute(
                Method::GET,
                url,
                None,
                usize::try_from(self.limits.max_page_bytes)
                    .map_err(|_error| PreparationError::InvalidLimits)?,
                deadline,
                ResponseKind::RecordPage,
            )
            .await?
            .ok_or(PreparationError::Malformed)?;
        let page: KeyPage =
            serde_json::from_slice(&bytes).map_err(|_error| PreparationError::Malformed)?;
        if page.keys.len()
            > usize::try_from(limit).map_err(|_error| PreparationError::InvalidLimits)?
            || page
                .keys
                .iter()
                .any(|key| !valid_key(key, false) || !key.starts_with(prefix))
            || page
                .keys
                .iter()
                .zip(page.keys.iter().skip(1))
                .any(|(previous, next)| previous >= next)
        {
            return Err(PreparationError::Malformed);
        }
        Ok(page.keys)
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RecordBody {
    body: String,
    #[serde(deserialize_with = "required_metadata")]
    metadata: Option<String>,
    generation: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct KeyPage {
    keys: Vec<String>,
}

#[derive(Serialize)]
struct WriteBody<'mode> {
    body: String,
    metadata: Option<String>,
    ttl_ms: String,
    mode: &'mode str,
    #[serde(skip_serializing_if = "Option::is_none")]
    generation: Option<String>,
}

fn required_metadata<'de, D>(deserializer: D) -> Result<Option<String>, D::Error>
where
    D: Deserializer<'de>,
{
    Option::deserialize(deserializer)
}

fn generation(value: &str) -> Result<u64, PreparationError> {
    if value.starts_with('0') || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(PreparationError::Malformed);
    }
    value
        .parse::<u64>()
        .ok()
        .filter(|number| *number > 0)
        .ok_or(PreparationError::Malformed)
}

fn encoded_len(bytes: usize) -> Result<usize, PreparationError> {
    bytes
        .div_ceil(3)
        .checked_mul(4)
        .ok_or(PreparationError::SizeLimit)
}

fn decode(value: &str, cap: usize) -> Result<Vec<u8>, PreparationError> {
    if value.len() > encoded_len(cap)? {
        return Err(PreparationError::SizeLimit);
    }
    let bytes = STANDARD
        .decode(value)
        .map_err(|_error| PreparationError::Malformed)?;
    if bytes.len() > cap {
        return Err(PreparationError::SizeLimit);
    }
    if STANDARD.encode(&bytes) != value {
        return Err(PreparationError::Malformed);
    }
    Ok(bytes)
}

#[cfg(test)]
#[path = "records_tests.rs"]
mod tests;
