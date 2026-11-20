//! Transport-independent `DynamoDB` record format shared by native and portable transports.
//!
//! The core owns partition and sort keys, retained allocator items, cursor encoding, size and
//! value-kind checks, deadlines and the error classifier. A transport only performs one
//! single-attempt operation and reports where a failure happened.

#![expect(
    clippy::arbitrary_source_item_ordering,
    reason = "keep the wire model, transport boundary, record core and validators grouped"
)]

use std::collections::BTreeMap;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use edgezero_core::key_value_store::{KvError, KvPage, KvStore};
use edgezero_core::record_store::{
    Record, RecordBinding, RecordGeneration, RecordLayout, RecordPage, RecordPartition,
    RecordWrite, RecordWriteMode, RecordWriteOutcome,
};
use edgezero_core::time::{Deadline, MonotonicClock};
use serde::{Deserialize, Serialize};
use web_time::{SystemTime, UNIX_EPOCH};

use crate::PreparationError;
use crate::settings::{DynamoDbKvLimits, DynamoDbKvSettings};

pub(crate) const FORMAT: &str = "edgezero-dynamodb-records-v1";
pub(crate) const MAX_CURSOR_BYTES: usize = 0x4000;
pub(crate) const MAX_ITEM_BYTES: usize = 409_600;
pub(crate) const MAX_KEY_BYTES: usize = 512;
pub(crate) const MAX_PAGE_KEYS: usize = 1_000;
const MAX_PARTITION_BYTES: usize = 2_048;

/// The scalar attribute shapes used by the record format. Other shapes are opaque.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum Attr {
    Binary(Vec<u8>),
    Number(String),
    /// Any other `DynamoDB` attribute type. It never matches an expected value.
    Other,
    Text(String),
}

impl Attr {
    pub(crate) fn as_n(&self) -> Result<&str, KvError> {
        match self {
            Self::Number(number) => Ok(number),
            Self::Binary(_) | Self::Other | Self::Text(_) => Err(KvError::Unavailable),
        }
    }

    pub(crate) fn as_s(&self) -> Result<&str, KvError> {
        match self {
            Self::Text(text) => Ok(text),
            Self::Binary(_) | Self::Number(_) | Self::Other => Err(KvError::Unavailable),
        }
    }
}

pub(crate) type Item = BTreeMap<String, Attr>;

/// The six operations the record format uses.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Operation {
    DeleteItem,
    GetItem,
    PutItem,
    Query,
    Scan,
    UpdateItem,
}

#[cfg(feature = "dynamodb-records-portable")]
impl Operation {
    pub(crate) const fn target(self) -> &'static str {
        match self {
            Self::DeleteItem => "DeleteItem",
            Self::GetItem => "GetItem",
            Self::PutItem => "PutItem",
            Self::Query => "Query",
            Self::Scan => "Scan",
            Self::UpdateItem => "UpdateItem",
        }
    }
}

/// One operation's inputs. Empty maps and `None` fields are omitted from the wire request.
#[derive(Clone, Debug, Default)]
pub(crate) struct Request {
    pub condition_expression: Option<&'static str>,
    pub consistent_read: Option<bool>,
    pub exclusive_start_key: Option<Item>,
    pub filter_expression: Option<&'static str>,
    pub item: Option<Item>,
    pub key: Option<Item>,
    pub key_condition_expression: Option<&'static str>,
    pub limit: Option<i32>,
    pub names: BTreeMap<String, String>,
    pub projection_expression: Option<&'static str>,
    pub return_updated_new: bool,
    pub scan_index_forward: Option<bool>,
    pub update_expression: Option<&'static str>,
    pub values: Item,
}

/// One operation's outputs. Absent fields are `None` or empty.
#[derive(Debug, Default)]
pub(crate) struct Response {
    pub attributes: Option<Item>,
    pub item: Option<Item>,
    pub items: Vec<Item>,
    pub last_evaluated_key: Option<Item>,
}

/// Where a single-attempt operation failed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TransportError {
    /// The request was never handed to the network: construction, signing, credentials,
    /// backend creation or an exhausted budget.
    NotDispatched,
    /// The service returned a typed error. `definitive` is a 4xx status.
    Service { conditional: bool, definitive: bool },
    /// The request may have reached the service: timeout, reset, 5xx-like or unreadable reply.
    Uncertain,
}

/// One single-attempt `DynamoDB` operation, bounded by `deadline`. No retries.
#[async_trait(?Send)]
pub(crate) trait RecordTransport: Send + Sync {
    async fn call(
        &self,
        operation: Operation,
        request: Request,
        deadline: Deadline,
    ) -> Result<Response, TransportError>;
}

/// Physical identity bound into every record binding scope and cursor.
#[derive(Clone, Copy, Debug)]
pub(crate) struct RecordScope<'identity> {
    pub endpoint: &'identity str,
    pub namespace: &'identity str,
    pub region: &'identity str,
    pub table: &'identity str,
    pub table_arn: &'identity str,
    pub table_id: Option<&'identity str>,
}

impl RecordScope<'_> {
    /// Encodes the same tuple native preparation has always bound into cursors.
    pub(crate) fn encode(&self) -> Result<String, PreparationError> {
        if self.table_arn.is_empty() {
            return Err(PreparationError::Malformed);
        }
        serde_json::to_string(&(
            self.endpoint,
            self.table_arn,
            self.table_id,
            self.region,
            self.table,
            self.namespace,
        ))
        .map_err(|_error| PreparationError::Malformed)
    }
}

/// Builds the record binding a provider reports through `record_binding()`.
pub(crate) fn record_binding(
    limits: DynamoDbKvLimits,
    layout: RecordLayout,
    scope: &RecordScope<'_>,
) -> Result<RecordBinding, PreparationError> {
    Ok(RecordBinding {
        layout,
        scope: scope.encode()?,
        format: FORMAT.to_owned(),
        // Metadata may use the full configured payload budget when the
        // body is empty. Inserts also enforce their combined byte size.
        max_metadata_bytes: limits.max_value_bytes,
    })
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct RecordCursor {
    format: String,
    layout_id: String,
    layout_version: u32,
    mode: String,
    pk: String,
    prefix: String,
    scope: String,
    sk: String,
}

/// The record format over one transport. All fields are fixed for the provider's lifetime.
pub(crate) struct RecordCore<T> {
    pub binding: RecordBinding,
    pub clock: MonotonicClock,
    pub limits: DynamoDbKvLimits,
    pub partition_base: String,
    pub transport: T,
}

#[expect(
    clippy::arbitrary_source_item_ordering,
    reason = "keep keys, point operations and pagination helpers grouped"
)]
impl<T: RecordTransport> RecordCore<T> {
    pub(crate) fn runtime_deadline(&self) -> Result<Deadline, KvError> {
        self.clock
            .now()
            .checked_add(Duration::from_millis(self.limits.operation_timeout_ms))
            .map(Deadline::at_instant)
            .ok_or(KvError::Unavailable)
    }

    fn require_remaining(&self, deadline: Deadline) -> Result<(), KvError> {
        deadline
            .remaining_at(self.clock.now())
            .filter(|remaining| !remaining.is_zero())
            .map(|_remaining| ())
            .ok_or(KvError::Unavailable)
    }

    /// A read or allocation. Every failure, including a late reply, is `Unavailable`.
    async fn bounded(
        &self,
        deadline: Deadline,
        operation: Operation,
        request: Request,
    ) -> Result<Response, KvError> {
        self.require_remaining(deadline)?;
        let result = self.transport.call(operation, request, deadline).await;
        if deadline.is_expired_at(self.clock.now()) {
            return Err(KvError::Unavailable);
        }
        result.map_err(|_error| KvError::Unavailable)
    }

    /// A value mutation. A late reply is `MutationUncertain`; the caller classifies errors.
    async fn mutate(
        &self,
        deadline: Deadline,
        operation: Operation,
        request: Request,
    ) -> Result<Result<Response, TransportError>, KvError> {
        self.require_remaining(deadline)?;
        let result = self.transport.call(operation, request, deadline).await;
        if deadline.is_expired_at(self.clock.now()) {
            return Err(KvError::MutationUncertain);
        }
        Ok(result)
    }

    pub(crate) fn partition_key(&self, partition: &RecordPartition) -> Result<String, KvError> {
        if self.binding.layout.partition(partition.prefix())? != *partition {
            return Err(invalid_input());
        }
        let pk = format!("{}{}", self.partition_base, partition.prefix());
        if pk.len() > MAX_PARTITION_BYTES {
            return Err(invalid_input());
        }
        Ok(pk)
    }

    pub(crate) fn key(
        &self,
        key: &str,
        partition: &RecordPartition,
        kind: &str,
    ) -> Result<Item, KvError> {
        if self.binding.layout.resolve(key)? != *partition {
            return Err(invalid_input());
        }
        Ok(physical_key(
            self.partition_key(partition)?,
            format!("{kind}#{key}"),
        ))
    }

    async fn allocate(
        &self,
        key: &str,
        partition: &RecordPartition,
        deadline: Deadline,
    ) -> Result<RecordGeneration, KvError> {
        // Failure here cannot have changed a value row. An unobserved allocation
        // is merely a gap, and is never represented as a successful value write.
        let request = Request {
            key: Some(self.key(key, partition, "g")?),
            update_expression: Some("SET #generation = if_not_exists(#generation, :zero) + :one"),
            condition_expression: Some(
                "attribute_not_exists(#generation) OR #generation < :ceiling",
            ),
            names: names(&[("#generation", "generation")]),
            values: Item::from([
                (":zero".to_owned(), Attr::Number("0".to_owned())),
                (":one".to_owned(), Attr::Number("1".to_owned())),
                (":ceiling".to_owned(), Attr::Number(u64::MAX.to_string())),
            ]),
            return_updated_new: true,
            ..Request::default()
        };
        let output = self
            .bounded(deadline, Operation::UpdateItem, request)
            .await?;
        let generation = output
            .attributes
            .as_ref()
            .and_then(|attributes| attributes.get("generation"))
            .ok_or(KvError::Unavailable)
            .and_then(Attr::as_n)
            .and_then(read_unsigned)?;
        if generation == 0 {
            return Err(KvError::Unavailable);
        }
        Ok(RecordGeneration::from_u64(generation))
    }

    pub(crate) async fn insert(
        &self,
        key: &str,
        partition: &RecordPartition,
        write: RecordWrite<'_>,
    ) -> Result<RecordWriteOutcome, KvError> {
        let deadline = self.runtime_deadline()?;
        let metadata = write.metadata.filter(|text| !text.is_empty());
        if write
            .body
            .len()
            .checked_add(metadata.map_or(0, str::len))
            .is_none_or(|bytes| bytes > self.limits.max_value_bytes)
        {
            return Err(invalid_input());
        }
        let expiry = write
            .ttl
            .map(|ttl| {
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map_err(|_error| KvError::Unavailable)?;
                // Native TTL is in seconds. Rounding up never shortens the requested TTL.
                Ok::<u64, KvError>(expiry_from_duration(now, ttl)?.div_ceil(1_000))
            })
            .transpose()?;
        let mut item = self.key(key, partition, "r")?;
        item.insert("v".to_owned(), Attr::Binary(write.body.to_vec()));
        item.insert("format".to_owned(), Attr::Text(FORMAT.to_owned()));
        item.insert("kind".to_owned(), Attr::Text("r".to_owned()));
        if let Some(text) = metadata {
            item.insert("metadata".to_owned(), Attr::Text(text.to_owned()));
        }
        if let Some(expires) = expiry {
            item.insert("ttl_epoch_s".to_owned(), Attr::Number(expires.to_string()));
        }
        // Include the maximum generation before allocation so oversized values
        // never consume an allocator request.
        item.insert("generation".to_owned(), Attr::Number(u64::MAX.to_string()));
        validate_record_size(&item)?;
        let generation = self.allocate(key, partition, deadline).await?;
        item.insert(
            "generation".to_owned(),
            Attr::Number(generation.as_u64().to_string()),
        );
        let mut request = Request {
            item: Some(item),
            ..Request::default()
        };
        match write.mode {
            RecordWriteMode::Overwrite => {}
            RecordWriteMode::Add => {
                request.condition_expression = Some("attribute_not_exists(#pk)");
                request.names = names(&[("#pk", "pk")]);
            }
            RecordWriteMode::IfGenerationMatch(expected) => {
                request.condition_expression = Some("#generation = :expected");
                request.names = names(&[("#generation", "generation")]);
                request.values = Item::from([(
                    ":expected".to_owned(),
                    Attr::Number(expected.as_u64().to_string()),
                )]);
            }
        }
        // Deadline exhaustion after allocation alone remains Unavailable.
        match self.mutate(deadline, Operation::PutItem, request).await? {
            Ok(_) => Ok(RecordWriteOutcome::Written),
            Err(TransportError::Service {
                conditional: true, ..
            }) if write.mode != RecordWriteMode::Overwrite => {
                Ok(RecordWriteOutcome::PreconditionFailed)
            }
            Err(error) => Err(mutation_error(error)),
        }
    }

    pub(crate) async fn lookup(
        &self,
        key: &str,
        partition: &RecordPartition,
    ) -> Result<Option<Record>, KvError> {
        let deadline = self.runtime_deadline()?;
        let expected = self.key(key, partition, "r")?;
        let request = Request {
            key: Some(expected.clone()),
            consistent_read: Some(true),
            ..Request::default()
        };
        self.bounded(deadline, Operation::GetItem, request)
            .await?
            .item
            .map(|item| self.record(&expected, item))
            .transpose()
    }

    fn record(&self, expected: &Item, mut item: Item) -> Result<Record, KvError> {
        if expected
            .iter()
            .any(|(name, value)| item.get(name) != Some(value))
        {
            return Err(KvError::Unavailable);
        }
        validate_value_kind(&item)?;
        let generation = match item.remove("generation") {
            Some(Attr::Number(value)) => RecordGeneration::from_u64(read_unsigned(&value)?),
            _ => return Err(KvError::Unavailable),
        };
        let metadata = match item.remove("metadata") {
            None => None,
            Some(Attr::Text(text)) if text.len() <= self.limits.max_value_bytes => {
                (!text.is_empty()).then_some(text)
            }
            _ => return Err(KvError::Unavailable),
        };
        let body = match item.remove("v") {
            Some(Attr::Binary(body)) if body.len() <= self.limits.max_value_bytes => {
                Bytes::from(body)
            }
            _ => return Err(KvError::Unavailable),
        };
        if body
            .len()
            .checked_add(metadata.as_deref().map_or(0, str::len))
            .is_none_or(|bytes| bytes > self.limits.max_value_bytes)
        {
            return Err(KvError::Unavailable);
        }
        // Check native TTL's shape but never impose a hard expiry filter.
        if let Some(ttl) = item.get("ttl_epoch_s") {
            read_unsigned(ttl.as_n()?)?;
        }
        Ok(Record {
            body,
            metadata,
            generation,
        })
    }

    pub(crate) async fn delete_value(
        &self,
        key: &str,
        partition: &RecordPartition,
    ) -> Result<(), KvError> {
        let deadline = self.runtime_deadline()?;
        let request = Request {
            key: Some(self.key(key, partition, "r")?),
            ..Request::default()
        };
        self.mutate(deadline, Operation::DeleteItem, request)
            .await?
            .map(|_response| ())
            .map_err(mutation_error)
    }

    pub(crate) fn decode_cursor(
        &self,
        mode: &str,
        prefix: &str,
        encoded: &str,
        partition: Option<&RecordPartition>,
    ) -> Result<Item, KvError> {
        if encoded.len() > MAX_CURSOR_BYTES {
            return Err(invalid_input());
        }
        let cursor: RecordCursor =
            serde_json::from_str(encoded).map_err(|_error| invalid_input())?;
        if cursor.format != FORMAT
            || cursor.layout_id != self.binding.layout.id()
            || cursor.layout_version != self.binding.layout.version()
            || cursor.scope != self.binding.scope
            || cursor.mode != mode
            || cursor.prefix != prefix
        {
            return Err(invalid_input());
        }
        let key = physical_key(cursor.pk, cursor.sk);
        validate_position(&key).map_err(|_error| invalid_input())?;
        if let Some(expected_partition) = partition
            && key.get("pk") != Some(&Attr::Text(self.partition_key(expected_partition)?))
        {
            return Err(invalid_input());
        }
        Ok(key)
    }

    pub(crate) fn next_cursor(
        &self,
        mode: &str,
        prefix: &str,
        after: Option<&Item>,
        next_position: Option<Item>,
        partition: Option<&RecordPartition>,
    ) -> Result<Option<String>, KvError> {
        let Some(position) = next_position.filter(|candidate| !candidate.is_empty()) else {
            return Ok(None);
        };
        let (pk, sk) = validate_position(&position)?;
        if after == Some(&position) {
            return Err(KvError::Unavailable);
        }
        if let Some(expected_partition) = partition
            && (pk != self.partition_key(expected_partition)?
                || after
                    .and_then(|key| key.get("sk"))
                    .and_then(|value| value.as_s().ok())
                    .is_some_and(|previous| sk <= previous))
        {
            return Err(KvError::Unavailable);
        }
        let encoded = serde_json::to_string(&RecordCursor {
            format: FORMAT.to_owned(),
            layout_id: self.binding.layout.id().to_owned(),
            layout_version: self.binding.layout.version(),
            mode: mode.to_owned(),
            pk: pk.to_owned(),
            prefix: prefix.to_owned(),
            scope: self.binding.scope.clone(),
            sk: sk.to_owned(),
        })
        .map_err(|_error| KvError::Unavailable)?;
        if encoded.len() > MAX_CURSOR_BYTES {
            return Err(KvError::Unavailable);
        }
        Ok(Some(encoded))
    }

    fn listed_keys(
        &self,
        items: Vec<Item>,
        prefix: &str,
        limit: usize,
        partition: Option<&RecordPartition>,
    ) -> Result<Vec<String>, KvError> {
        if items.len() > limit {
            return Err(KvError::Unavailable);
        }
        items
            .into_iter()
            .map(|item| {
                validate_value_kind(&item)?;
                let (pk, sk) = validate_position_item(&item)?;
                let key = sk
                    .strip_prefix("r#")
                    .filter(|key| key.starts_with(prefix))
                    .ok_or(KvError::Unavailable)?;
                let resolved = self
                    .binding
                    .layout
                    .resolve(key)
                    .map_err(|_error| KvError::Unavailable)?;
                if pk != self.partition_key(&resolved)?
                    || partition.is_some_and(|expected| expected != &resolved)
                {
                    return Err(KvError::Unavailable);
                }
                Ok(key.to_owned())
            })
            .collect()
    }

    fn page_request(
        &self,
        prefix: &str,
        partition: Option<&RecordPartition>,
        after: Option<Item>,
        limit: usize,
    ) -> Result<(Operation, Request), KvError> {
        let native_limit = i32::try_from(limit).map_err(|_error| invalid_input())?;
        let mut request = Request {
            exclusive_start_key: after,
            limit: Some(native_limit),
            names: names(&[
                ("#pk", "pk"),
                ("#sk", "sk"),
                ("#format", "format"),
                ("#kind", "kind"),
            ]),
            projection_expression: Some("#pk, #sk, #format, #kind"),
            values: Item::from([
                (":format".to_owned(), Attr::Text(FORMAT.to_owned())),
                (":kind".to_owned(), Attr::Text("r".to_owned())),
            ]),
            ..Request::default()
        };
        if let Some(query_partition) = partition {
            // Query cannot filter a key attribute. Full membership is checked
            // after the reply, while the partition PK already bounds keys.
            request.consistent_read = Some(true);
            request.scan_index_forward = Some(true);
            request.key_condition_expression = Some("#pk = :pk");
            request.filter_expression = Some("#format = :format AND #kind = :kind");
            request.values.insert(
                ":pk".to_owned(),
                Attr::Text(self.partition_key(query_partition)?),
            );
            return Ok((Operation::Query, request));
        }
        request.filter_expression = Some(
            "begins_with(#pk, :binding) AND #format = :format AND #kind = :kind AND begins_with(#sk, :prefix)",
        );
        request.values.insert(
            ":binding".to_owned(),
            Attr::Text(self.partition_base.clone()),
        );
        request
            .values
            .insert(":prefix".to_owned(), Attr::Text(format!("r#{prefix}")));
        Ok((Operation::Scan, request))
    }

    pub(crate) async fn page(
        &self,
        prefix: &str,
        partition: Option<&RecordPartition>,
        cursor: Option<&str>,
        limit: usize,
    ) -> Result<KvPage, KvError> {
        let deadline = self.runtime_deadline()?;
        if prefix.len() > MAX_KEY_BYTES || prefix.chars().any(char::is_control) {
            return Err(invalid_input());
        }
        if limit == 0 || limit > MAX_PAGE_KEYS {
            return Err(invalid_input());
        }
        let mode = if partition.is_some() { "query" } else { "scan" };
        let after = cursor
            .map(|encoded| self.decode_cursor(mode, prefix, encoded, partition))
            .transpose()?;
        let (operation, request) = self.page_request(prefix, partition, after.clone(), limit)?;
        let output = self.bounded(deadline, operation, request).await?;
        if partition.is_some() {
            validate_query_positions(
                &output.items,
                after.as_ref(),
                output.last_evaluated_key.as_ref(),
            )?;
        }
        let keys = self.listed_keys(output.items, prefix, limit, partition)?;
        let next = self.next_cursor(
            mode,
            prefix,
            after.as_ref(),
            output.last_evaluated_key,
            partition,
        )?;
        Ok(KvPage { keys, cursor: next })
    }
}

#[expect(
    clippy::arbitrary_source_item_ordering,
    reason = "keep prepared capability and ordinary methods ahead of the record boundary"
)]
#[expect(
    clippy::missing_trait_methods,
    reason = "ordinary exists deliberately delegates to the same strong get-bytes path"
)]
#[async_trait(?Send)]
impl<T: RecordTransport> KvStore for RecordCore<T> {
    fn record_binding(&self) -> Option<RecordBinding> {
        Some(self.binding.clone())
    }

    async fn get_bytes(&self, key: &str) -> Result<Option<Bytes>, KvError> {
        let partition = self.binding.layout.resolve(key)?;
        Ok(self
            .lookup(key, &partition)
            .await?
            .map(|record| record.body))
    }

    async fn put_bytes(&self, key: &str, value: Bytes) -> Result<(), KvError> {
        let partition = self.binding.layout.resolve(key)?;
        self.insert(
            key,
            &partition,
            RecordWrite {
                body: &value,
                metadata: None,
                ttl: None,
                mode: RecordWriteMode::Overwrite,
            },
        )
        .await?;
        Ok(())
    }

    async fn put_bytes_with_ttl(
        &self,
        key: &str,
        value: Bytes,
        ttl: Duration,
    ) -> Result<(), KvError> {
        let partition = self.binding.layout.resolve(key)?;
        self.insert(
            key,
            &partition,
            RecordWrite {
                body: &value,
                metadata: None,
                ttl: Some(ttl),
                mode: RecordWriteMode::Overwrite,
            },
        )
        .await?;
        Ok(())
    }

    async fn delete(&self, key: &str) -> Result<(), KvError> {
        self.delete_value(key, &self.binding.layout.resolve(key)?)
            .await
    }

    async fn list_keys_page(
        &self,
        prefix: &str,
        cursor: Option<&str>,
        limit: usize,
    ) -> Result<KvPage, KvError> {
        self.page(prefix, None, cursor, limit).await
    }

    async fn record_lookup(
        &self,
        key: &str,
        partition: &RecordPartition,
    ) -> Result<Option<Record>, KvError> {
        self.lookup(key, partition).await
    }

    async fn record_insert(
        &self,
        key: &str,
        partition: &RecordPartition,
        write: RecordWrite<'_>,
    ) -> Result<RecordWriteOutcome, KvError> {
        self.insert(key, partition, write).await
    }

    async fn record_exists_strong(
        &self,
        key: &str,
        partition: &RecordPartition,
    ) -> Result<bool, KvError> {
        Ok(self.lookup(key, partition).await?.is_some())
    }

    async fn record_delete(&self, key: &str, partition: &RecordPartition) -> Result<(), KvError> {
        self.delete_value(key, partition).await
    }

    async fn record_list_partition(
        &self,
        partition: &RecordPartition,
        cursor: Option<&str>,
        limit: usize,
    ) -> Result<RecordPage, KvError> {
        self.partition_key(partition)?;
        let page = self
            .page(partition.prefix(), Some(partition), cursor, limit)
            .await?;
        Ok(RecordPage {
            keys: page.keys,
            cursor: page.cursor,
        })
    }
}

/// One classifier for value mutations on every transport.
pub(crate) const fn mutation_error(error: TransportError) -> KvError {
    match error {
        TransportError::NotDispatched
        | TransportError::Service {
            definitive: true, ..
        } => KvError::Unavailable,
        TransportError::Service {
            definitive: false, ..
        }
        | TransportError::Uncertain => KvError::MutationUncertain,
    }
}

fn names(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
    pairs
        .iter()
        .map(|(alias, name)| ((*alias).to_owned(), (*name).to_owned()))
        .collect()
}

pub(crate) fn physical_key(pk: String, sk: String) -> Item {
    Item::from([
        ("pk".to_owned(), Attr::Text(pk)),
        ("sk".to_owned(), Attr::Text(sk)),
    ])
}

pub(crate) fn record_partition_base(
    settings: &DynamoDbKvSettings,
    layout: &RecordLayout,
) -> Result<String, PreparationError> {
    // Byte lengths delimit arbitrary namespace/layout identities without escaping
    // or collisions. The final remainder is the unchanged declared partition.
    settings.validate()?;
    let base = format!(
        "{FORMAT}#{}:{}{}:{}{}:",
        settings.namespace.len(),
        settings.namespace,
        layout.id().len(),
        layout.id(),
        layout.version()
    );
    if base
        .len()
        .checked_add(MAX_KEY_BYTES)
        .is_none_or(|size| size > MAX_PARTITION_BYTES)
    {
        return Err(PreparationError::InvalidBinding);
    }
    Ok(base)
}

fn validate_position(key: &Item) -> Result<(&str, &str), KvError> {
    if key.len() != 2 {
        return Err(KvError::Unavailable);
    }
    validate_position_item(key)
}

fn validate_position_item(item: &Item) -> Result<(&str, &str), KvError> {
    match (item.get("pk"), item.get("sk")) {
        (Some(Attr::Text(pk)), Some(Attr::Text(sk)))
            if !pk.is_empty()
                && pk.len() <= MAX_PARTITION_BYTES
                && !sk.is_empty()
                && sk.len() <= 1_024 =>
        {
            Ok((pk, sk))
        }
        _ => Err(KvError::Unavailable),
    }
}

fn validate_query_positions(
    items: &[Item],
    after: Option<&Item>,
    next_position: Option<&Item>,
) -> Result<(), KvError> {
    let mut previous = after.map(validate_position).transpose()?.map(|(_, sk)| sk);
    for item in items {
        let (_, sk) = validate_position_item(item)?;
        if previous.is_some_and(|last| sk <= last) {
            return Err(KvError::Unavailable);
        }
        previous = Some(sk);
    }
    if let Some(position) = next_position.filter(|candidate| !candidate.is_empty()) {
        let (_, sk) = validate_position(position)?;
        if previous.is_some_and(|last| sk < last) {
            return Err(KvError::Unavailable);
        }
    }
    Ok(())
}

fn validate_value_kind(item: &Item) -> Result<(), KvError> {
    if item.get("format") != Some(&Attr::Text(FORMAT.to_owned()))
        || item.get("kind") != Some(&Attr::Text("r".to_owned()))
    {
        return Err(KvError::Unavailable);
    }
    Ok(())
}

fn validate_record_size(item: &Item) -> Result<(), KvError> {
    let mut size = 0_usize;
    for (name, value) in item {
        let bytes = match value {
            Attr::Text(text) | Attr::Number(text) => text.len(),
            Attr::Binary(blob) => blob.len(),
            Attr::Other => return Err(invalid_input()),
        };
        size = size
            .checked_add(name.len())
            .and_then(|with_name| with_name.checked_add(bytes))
            .ok_or_else(invalid_input)?;
    }
    if size > MAX_ITEM_BYTES {
        return Err(invalid_input());
    }
    Ok(())
}

pub(crate) fn expiry_from_duration(now: Duration, ttl: Duration) -> Result<u64, KvError> {
    if ttl.is_zero() {
        return Err(invalid_input());
    }
    let expires = now.checked_add(ttl).ok_or_else(invalid_input)?;
    let rounded = expires
        .as_millis()
        .checked_add(u128::from(
            !expires.subsec_nanos().is_multiple_of(1_000_000),
        ))
        .ok_or_else(invalid_input)?;
    u64::try_from(rounded).map_err(|_error| invalid_input())
}

pub(crate) fn invalid_input() -> KvError {
    KvError::Validation("invalid DynamoDB KV input".to_owned())
}

pub(crate) fn read_unsigned(value: &str) -> Result<u64, KvError> {
    if value.is_empty() || value.len() > 20 || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(KvError::Unavailable);
    }
    let number = value
        .parse::<u64>()
        .map_err(|_error| KvError::Unavailable)?;
    if number.to_string() != value {
        return Err(KvError::Unavailable);
    }
    Ok(number)
}
