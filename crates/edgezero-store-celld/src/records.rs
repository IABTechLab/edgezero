//! Prepared records over the EdgeZero-owned service on official stock celld.
//! Ordinary and record operations share its rows and generation history.
//!
//! The record logic is transport-independent. Native preparation reads the service
//! capabilities; portable bindings use the instance pinned by offline preflight.

#![expect(
    clippy::arbitrary_source_item_ordering,
    reason = "keep the prepared store and record operations before wire models and validation"
)]

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use base64::Engine as _;
use base64::engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD};
use bytes::Bytes;
use edgezero_core::http::Method;
use edgezero_core::key_value_store::{KvError, KvHandle, KvPage, KvStore};
use edgezero_core::record_store::{
    Record, RecordBinding, RecordGeneration, RecordLayout, RecordPage, RecordPartition,
    RecordWrite, RecordWriteMode, RecordWriteOutcome,
};
#[cfg(all(not(target_arch = "wasm32"), feature = "records"))]
use edgezero_core::time::Deadline;
use serde::{Deserialize, Deserializer, Serialize};
use sha2::{Digest as _, Sha256};
use url::Url;

use crate::PreparationError;
#[cfg(all(not(target_arch = "wasm32"), feature = "records"))]
use crate::client::ScopedClient;
use crate::settings::{CelldPreparationLimits, valid_key};
use crate::wire::{Exchanger, ResponseKind};

pub(crate) const MAX_METADATA_BYTES: usize = 1_024;
const MAX_PROVIDER_CURSOR: usize = 8_192;
const MAX_SERVICE_CURSOR: usize = 2_048;

pub(crate) struct CelldRecordKvStore<E> {
    binding: RecordBinding,
    client: Arc<E>,
    instance: [u8; 22],
    limits: CelldPreparationLimits,
    store: String,
}

/// The binding every transport reports for one store instance.
pub(crate) fn record_binding(
    endpoint: &Url,
    store: &str,
    instance: &str,
    layout: &RecordLayout,
) -> Result<RecordBinding, PreparationError> {
    let scope = fingerprint(&[
        "edgezero-celld-store",
        "rows-v1",
        "records-v1",
        endpoint.as_str(),
        store,
        instance,
    ])?;
    Ok(RecordBinding {
        layout: layout.clone(),
        scope,
        format: "celld-store-records-v1".to_owned(),
        max_metadata_bytes: MAX_METADATA_BYTES,
    })
}

/// A 22-byte URL-safe store instance, as the service reports it.
pub(crate) fn instance_bytes(instance: &str) -> Result<[u8; 22], PreparationError> {
    let bytes: [u8; 22] = instance
        .as_bytes()
        .try_into()
        .map_err(|_error| PreparationError::Malformed)?;
    if !bytes
        .iter()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    {
        return Err(PreparationError::Malformed);
    }
    Ok(bytes)
}

#[cfg(all(not(target_arch = "wasm32"), feature = "records"))]
impl CelldRecordKvStore<ScopedClient> {
    pub(crate) async fn prepare(
        client: Arc<ScopedClient>,
        endpoint: &Url,
        store: String,
        mut limits: CelldPreparationLimits,
        layout: &RecordLayout,
        preparation_deadline: Deadline,
    ) -> Result<Self, PreparationError> {
        let capabilities =
            read_capabilities(client.as_ref(), &store, limits, preparation_deadline).await?;
        limits.max_value_bytes = limits.max_value_bytes.min(capabilities.max_value_bytes);
        let instance = instance_bytes(&capabilities.instance)?;
        Ok(Self {
            binding: record_binding(endpoint, &store, &capabilities.instance, layout)?,
            client,
            instance,
            limits,
            store,
        })
    }

    /// Whether preparation observed exactly this pinned instance.
    pub(crate) fn has_instance(&self, instance: &str) -> bool {
        self.instance.as_slice() == instance.as_bytes()
    }
}

/// Native read-only capability exchange. It never writes a probe record.
#[cfg(all(not(target_arch = "wasm32"), feature = "records"))]
pub(crate) async fn read_capabilities(
    client: &ScopedClient,
    store: &str,
    limits: CelldPreparationLimits,
    preparation_deadline: Deadline,
) -> Result<ServiceCapabilities, PreparationError> {
    let deadline = Deadline::at_instant(
        preparation_deadline
            .instant()
            .min(client.deadline()?.instant()),
    );
    let exchange = client
        .execute(
            Method::GET,
            client.url(store, "capabilities")?,
            None,
            usize::try_from(limits.max_page_bytes)
                .map_err(|_error| PreparationError::InvalidLimits)?,
            deadline,
            ResponseKind::RecordCapabilities,
        )
        .await?;
    let capabilities: Capabilities =
        serde_json::from_slice(&exchange.body.ok_or(PreparationError::Malformed)?)
            .map_err(|_error| PreparationError::Malformed)?;
    if capabilities.service != "edgezero-celld-store"
        || capabilities.protocol != 1
        || capabilities.format != "rows-v1"
        || capabilities.pagination != "keyset"
        || !capabilities.no_expiry
        || !capabilities.mutation_ledger
        || !capabilities.records
        || capabilities.record_protocol != 1
        || capabilities.max_metadata_bytes != MAX_METADATA_BYTES
        || capabilities.max_value_bytes == 0
    {
        return Err(PreparationError::UnsupportedVersion);
    }
    if exchange.instance.as_deref() != Some(capabilities.instance.as_str()) {
        return Err(PreparationError::Malformed);
    }
    instance_bytes(&capabilities.instance)?;
    client.check(deadline)?;
    Ok(ServiceCapabilities {
        instance: capabilities.instance,
        max_value_bytes: capabilities.max_value_bytes,
    })
}

/// The capability facts preflight and native preparation keep.
#[cfg(all(not(target_arch = "wasm32"), feature = "records"))]
pub(crate) struct ServiceCapabilities {
    pub instance: String,
    pub max_value_bytes: u64,
}

impl<E: Exchanger> CelldRecordKvStore<E> {
    /// Binds a pinned instance without any I/O. Every request still carries the
    /// expected instance, so a reset store fails until the binding is re-pinned.
    #[cfg_attr(
        not(feature = "portable-records"),
        expect(
            dead_code,
            reason = "only portable bindings construct from a pinned instance"
        )
    )]
    pub(crate) fn pinned(
        client: Arc<E>,
        binding: RecordBinding,
        instance: [u8; 22],
        limits: CelldPreparationLimits,
        store: String,
    ) -> Self {
        Self {
            binding,
            client,
            instance,
            limits,
            store,
        }
    }

    fn point_url(&self, resource: &str, key: &str) -> Result<Url, KvError> {
        self.binding.layout.resolve(key)?;
        let mut url = self.client.url(&self.store, resource).map_err(kv_error)?;
        url.query_pairs_mut().append_pair("key", key);
        Ok(url)
    }

    fn check_partition(&self, key: &str, partition: &RecordPartition) -> Result<(), KvError> {
        if self.binding.layout.resolve(key)? != *partition {
            return Err(invalid("record partition mismatch"));
        }
        Ok(())
    }

    fn record_cap(&self) -> Result<usize, KvError> {
        let value =
            usize::try_from(self.limits.max_value_bytes).map_err(|_error| KvError::Unavailable)?;
        encoded_len(value)
            .and_then(|size| size.checked_add(encoded_len(MAX_METADATA_BYTES)?))
            .and_then(|size| size.checked_add(256))
            .ok_or(KvError::Unavailable)
    }

    async fn lookup(&self, key: &str) -> Result<Option<Record>, KvError> {
        let deadline = self.client.deadline().map_err(kv_error)?;
        let Some(bytes) = self
            .client
            .execute(
                Method::GET,
                self.point_url("record", key)?,
                None,
                self.record_cap()?,
                deadline,
                ResponseKind::RecordValue(self.instance),
            )
            .await
            .map(|exchange| exchange.body)
            .map_err(kv_error)?
        else {
            return Ok(None);
        };
        let wire: WireRecord =
            serde_json::from_slice(&bytes).map_err(|_error| KvError::Unavailable)?;
        let body = decode(
            &wire.body,
            usize::try_from(self.limits.max_value_bytes).map_err(|_error| KvError::Unavailable)?,
        )?;
        let metadata = wire
            .metadata
            .map(|value| {
                let metadata_bytes = decode(&value, MAX_METADATA_BYTES)?;
                String::from_utf8(metadata_bytes).map_err(|_error| KvError::Unavailable)
            })
            .transpose()?
            .filter(|text| !text.is_empty());
        let generation = wire
            .generation
            .parse::<u64>()
            .ok()
            .filter(|value| *value > 0 && value.to_string() == wire.generation)
            .ok_or(KvError::Unavailable)?;
        self.client.check(deadline).map_err(kv_error)?;
        Ok(Some(Record {
            body: Bytes::from(body),
            metadata,
            generation: RecordGeneration::from_u64(generation),
        }))
    }

    async fn insert(
        &self,
        key: &str,
        write: RecordWrite<'_>,
    ) -> Result<RecordWriteOutcome, KvError> {
        let url = self.point_url("record-put", key)?;
        if u64::try_from(write.body.len())
            .map_err(|_error| invalid("record value exceeds limit"))?
            > self.limits.max_value_bytes
            || write
                .metadata
                .is_some_and(|text| text.len() > MAX_METADATA_BYTES)
        {
            return Err(invalid("record value or metadata exceeds provider limit"));
        }
        let ttl_ms = write.ttl.map(ttl_ms).transpose()?;
        let (mode, generation) = match write.mode {
            RecordWriteMode::Add => ("add", None),
            RecordWriteMode::Overwrite => ("overwrite", None),
            RecordWriteMode::IfGenerationMatch(generation) if generation.as_u64() != 0 => {
                ("if-generation-match", Some(generation.as_u64().to_string()))
            }
            RecordWriteMode::IfGenerationMatch(_) => {
                return Err(invalid("invalid record generation"));
            }
        };
        let body = serde_json::to_vec(&WireWrite {
            body: STANDARD.encode(write.body),
            metadata: write
                .metadata
                .filter(|text| !text.is_empty())
                .map(|text| STANDARD.encode(text.as_bytes())),
            ttl_ms,
            mode,
            generation,
        })
        .map_err(|_error| invalid("invalid record write"))?;
        let deadline = self.client.deadline().map_err(kv_error)?;
        match self
            .client
            .execute(
                Method::POST,
                url,
                Some(Bytes::from(body)),
                0,
                deadline,
                ResponseKind::RecordMutation(self.instance),
            )
            .await
            .map(|exchange| exchange.body)
        {
            Ok(Some(_)) => Ok(RecordWriteOutcome::Written),
            Err(PreparationError::PreconditionFailed)
                if write.mode != RecordWriteMode::Overwrite =>
            {
                Ok(RecordWriteOutcome::PreconditionFailed)
            }
            Ok(None) | Err(PreparationError::PreconditionFailed) => Err(KvError::Unavailable),
            Err(error) => Err(kv_error(error)),
        }
    }

    async fn exists_value(&self, key: &str) -> Result<bool, KvError> {
        self.client
            .execute(
                Method::HEAD,
                self.point_url("record", key)?,
                None,
                0,
                self.client.deadline().map_err(kv_error)?,
                ResponseKind::RecordExists(self.instance),
            )
            .await
            .map(|exchange| exchange.body.is_some())
            .map_err(kv_error)
    }

    async fn delete_value(&self, key: &str) -> Result<(), KvError> {
        self.client
            .execute(
                Method::POST,
                self.point_url("record-delete", key)?,
                None,
                0,
                self.client.deadline().map_err(kv_error)?,
                ResponseKind::RecordMutation(self.instance),
            )
            .await
            .map(|_| ())
            .map_err(kv_error)
    }

    async fn page(
        &self,
        prefix: &str,
        cursor: Option<&str>,
        limit: usize,
        operation: &str,
    ) -> Result<RecordPage, KvError> {
        if !valid_key(prefix, true) || !(1..=1_000).contains(&limit) {
            return Err(invalid("invalid record page input"));
        }
        let identity = fingerprint(&[
            &self.binding.scope,
            &self.binding.format,
            self.binding.layout.id(),
            &self.binding.layout.version().to_string(),
            prefix,
            operation,
        ])
        .map_err(kv_error)?;
        let prior = cursor
            .map(|encoded| decode_cursor(encoded, &identity, prefix))
            .transpose()?;
        let mut url = self
            .client
            .url(&self.store, "record-keys")
            .map_err(kv_error)?;
        {
            let mut query = url.query_pairs_mut();
            query
                .append_pair("prefix", prefix)
                .append_pair("limit", &limit.to_string())
                .append_pair("mode", operation);
            if let Some(previous) = &prior {
                query.append_pair("cursor", &previous.service);
            }
        }
        let deadline = self.client.deadline().map_err(kv_error)?;
        let bytes = self
            .client
            .execute(
                Method::GET,
                url,
                None,
                usize::try_from(self.limits.max_page_bytes)
                    .map_err(|_error| KvError::Unavailable)?,
                deadline,
                ResponseKind::RecordPage(self.instance),
            )
            .await
            .map(|exchange| exchange.body)
            .map_err(kv_error)?
            .ok_or(KvError::Unavailable)?;
        let page: WirePage =
            serde_json::from_slice(&bytes).map_err(|_error| KvError::Unavailable)?;
        if page.keys.len() > limit {
            return Err(KvError::Unavailable);
        }
        let mut last = prior.as_ref().and_then(|previous| previous.last.as_deref());
        for key in &page.keys {
            if !valid_key(key, false)
                || !key.starts_with(prefix)
                || last.is_some_and(|previous_key| key.as_str() <= previous_key)
            {
                return Err(KvError::Unavailable);
            }
            last = Some(key);
        }
        let next_cursor = page
            .cursor
            .map(|service| {
                if service.is_empty()
                    || service.len() > MAX_SERVICE_CURSOR
                    || prior
                        .as_ref()
                        .is_some_and(|previous| previous.service == service)
                {
                    return Err(KvError::Unavailable);
                }
                let envelope = serde_json::to_vec(&PageCursor {
                    version: 1,
                    identity,
                    last: last.map(str::to_owned),
                    service,
                })
                .map_err(|_error| KvError::Unavailable)?;
                let encoded_cursor = URL_SAFE_NO_PAD.encode(envelope);
                if encoded_cursor.len() > MAX_PROVIDER_CURSOR {
                    return Err(KvError::Unavailable);
                }
                Ok(encoded_cursor)
            })
            .transpose()?;
        self.client.check(deadline).map_err(kv_error)?;
        Ok(RecordPage {
            keys: page.keys,
            cursor: next_cursor,
        })
    }
}

#[async_trait(?Send)]
impl<E: Exchanger> KvStore for CelldRecordKvStore<E> {
    async fn delete(&self, key: &str) -> Result<(), KvError> {
        self.delete_value(key).await
    }
    async fn exists(&self, key: &str) -> Result<bool, KvError> {
        self.exists_value(key).await
    }
    async fn get_bytes(&self, key: &str) -> Result<Option<Bytes>, KvError> {
        self.lookup(key)
            .await
            .map(|record| record.map(|value| value.body))
    }
    async fn list_keys_page(
        &self,
        prefix: &str,
        cursor: Option<&str>,
        limit: usize,
    ) -> Result<KvPage, KvError> {
        self.page(prefix, cursor, limit, "ordinary")
            .await
            .map(|page| KvPage {
                keys: page.keys,
                cursor: page.cursor,
            })
    }
    async fn put_bytes(&self, key: &str, value: Bytes) -> Result<(), KvError> {
        self.insert(
            key,
            RecordWrite {
                body: &value,
                metadata: None,
                ttl: None,
                mode: RecordWriteMode::Overwrite,
            },
        )
        .await
        .map(|_| ())
    }
    async fn put_bytes_with_ttl(
        &self,
        key: &str,
        value: Bytes,
        ttl: Duration,
    ) -> Result<(), KvError> {
        self.insert(
            key,
            RecordWrite {
                body: &value,
                metadata: None,
                ttl: Some(ttl),
                mode: RecordWriteMode::Overwrite,
            },
        )
        .await
        .map(|_| ())
    }
    fn record_binding(&self) -> Option<RecordBinding> {
        Some(self.binding.clone())
    }
    async fn record_lookup(
        &self,
        key: &str,
        partition: &RecordPartition,
    ) -> Result<Option<Record>, KvError> {
        self.check_partition(key, partition)?;
        self.lookup(key).await
    }
    async fn record_insert(
        &self,
        key: &str,
        partition: &RecordPartition,
        write: RecordWrite<'_>,
    ) -> Result<RecordWriteOutcome, KvError> {
        self.check_partition(key, partition)?;
        self.insert(key, write).await
    }
    async fn record_exists_strong(
        &self,
        key: &str,
        partition: &RecordPartition,
    ) -> Result<bool, KvError> {
        self.check_partition(key, partition)?;
        self.exists_value(key).await
    }
    async fn record_delete(&self, key: &str, partition: &RecordPartition) -> Result<(), KvError> {
        self.check_partition(key, partition)?;
        self.delete_value(key).await
    }
    async fn record_list_partition(
        &self,
        partition: &RecordPartition,
        cursor: Option<&str>,
        limit: usize,
    ) -> Result<RecordPage, KvError> {
        if self.binding.layout.partition(partition.prefix())? != *partition {
            return Err(invalid("record partition mismatch"));
        }
        self.page(partition.prefix(), cursor, limit, "partition")
            .await
    }
}

#[cfg(all(not(target_arch = "wasm32"), feature = "records"))]
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Capabilities {
    service: String,
    protocol: u8,
    format: String,
    pagination: String,
    no_expiry: bool,
    mutation_ledger: bool,
    records: bool,
    record_protocol: u8,
    max_value_bytes: u64,
    max_metadata_bytes: usize,
    instance: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WireRecord {
    body: String,
    #[serde(deserialize_with = "nullable_text")]
    metadata: Option<String>,
    generation: String,
}

#[derive(Serialize)]
struct WireWrite<'mode> {
    body: String,
    metadata: Option<String>,
    ttl_ms: Option<u64>,
    mode: &'mode str,
    #[serde(skip_serializing_if = "Option::is_none")]
    generation: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WirePage {
    keys: Vec<String>,
    #[serde(deserialize_with = "nullable_text")]
    cursor: Option<String>,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct PageCursor {
    version: u8,
    identity: String,
    last: Option<String>,
    service: String,
}

fn nullable_text<'de, D>(deserializer: D) -> Result<Option<String>, D::Error>
where
    D: Deserializer<'de>,
{
    Option::deserialize(deserializer)
}

fn encoded_len(bytes: usize) -> Option<usize> {
    bytes.div_ceil(3).checked_mul(4)
}

fn decode(text: &str, cap: usize) -> Result<Vec<u8>, KvError> {
    if text.len() > encoded_len(cap).ok_or(KvError::Unavailable)? {
        return Err(KvError::Unavailable);
    }
    let bytes = STANDARD
        .decode(text)
        .map_err(|_error| KvError::Unavailable)?;
    if bytes.len() > cap || STANDARD.encode(&bytes) != text {
        return Err(KvError::Unavailable);
    }
    Ok(bytes)
}

fn decode_cursor(text: &str, identity: &str, prefix: &str) -> Result<PageCursor, KvError> {
    if text.is_empty() || text.len() > MAX_PROVIDER_CURSOR {
        return Err(invalid("invalid record cursor"));
    }
    let bytes = URL_SAFE_NO_PAD
        .decode(text)
        .map_err(|_error| invalid("invalid record cursor"))?;
    let cursor: PageCursor =
        serde_json::from_slice(&bytes).map_err(|_error| invalid("invalid record cursor"))?;
    if cursor.version != 1
        || cursor.identity != identity
        || cursor.service.is_empty()
        || cursor.service.len() > MAX_SERVICE_CURSOR
        || cursor
            .last
            .as_deref()
            .is_some_and(|key| !valid_key(key, false) || !key.starts_with(prefix))
    {
        return Err(invalid("foreign record cursor"));
    }
    Ok(cursor)
}

#[expect(
    clippy::big_endian_bytes,
    reason = "length-frame native backing identity fields without delimiter collisions"
)]
fn fingerprint(parts: &[&str]) -> Result<String, PreparationError> {
    let mut hash = Sha256::new();
    for part in parts {
        hash.update(
            u64::try_from(part.len())
                .map_err(|_error| PreparationError::SizeLimit)?
                .to_be_bytes(),
        );
        hash.update(part.as_bytes());
    }
    Ok(URL_SAFE_NO_PAD.encode(hash.finalize()))
}

fn ttl_ms(ttl: Duration) -> Result<u64, KvError> {
    if ttl.is_zero() || ttl > KvHandle::MAX_TTL {
        return Err(invalid("record TTL outside supported range"));
    }
    let millis = u64::try_from(ttl.as_millis())
        .ok()
        .and_then(|millis| {
            millis.checked_add(u64::from(!ttl.subsec_nanos().is_multiple_of(1_000_000)))
        })
        .ok_or_else(|| invalid("invalid record TTL"))?;
    Ok(millis)
}

fn invalid(message: &str) -> KvError {
    KvError::Validation(message.to_owned())
}

fn kv_error(error: PreparationError) -> KvError {
    match error {
        PreparationError::MutationUncertain => KvError::MutationUncertain,
        PreparationError::InvalidBinding
        | PreparationError::InvalidEndpoint
        | PreparationError::InvalidLimits
        | PreparationError::InvalidResource => invalid("invalid celld record input"),
        PreparationError::SizeLimit => KvError::LimitExceeded {
            message: "celld record size limit".to_owned(),
        },
        PreparationError::Credentials
        | PreparationError::Unauthenticated
        | PreparationError::Forbidden
        | PreparationError::Missing
        | PreparationError::PreconditionFailed
        | PreparationError::Malformed
        | PreparationError::UnsupportedVersion
        | PreparationError::InstanceChanged
        | PreparationError::PinnedIdentityMismatch
        | PreparationError::Unavailable
        | PreparationError::Deadline => KvError::Unavailable,
    }
}

#[cfg(test)]
#[cfg(all(not(target_arch = "wasm32"), feature = "records"))]
#[path = "records_tests.rs"]
mod tests;
