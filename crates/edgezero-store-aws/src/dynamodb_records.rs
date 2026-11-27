//! Explicit partitioned record format. Allocators survive value deletion and TTL.

use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use aws_sdk_dynamodb::error::{ProvideErrorMetadata, SdkError};
use aws_sdk_dynamodb::primitives::Blob;
use aws_sdk_dynamodb::types::{AttributeValue, ReturnValue, TableDescription};
use bytes::Bytes;
use edgezero_core::key_value_store::{KvError, KvHandle, KvPage, KvStore};
use edgezero_core::time::Deadline;
use serde::{Deserialize, Serialize};
use tokio::time::timeout;

use super::{
    DynamoDbStore, MAX_CURSOR_BYTES, MAX_ITEM_BYTES, MAX_KEY_BYTES, MAX_PAGE_KEYS,
    expiry_from_duration, invalid_input, read_unsigned,
};
use crate::PreparationError;
use crate::preparation::AwsPreparation;
use crate::settings::DynamoDbKvSettings;
use edgezero_core::record_store::{
    Record, RecordBinding, RecordGeneration, RecordLayout, RecordPage, RecordPartition,
    RecordWrite, RecordWriteMode, RecordWriteOutcome,
};

const FORMAT: &str = "edgezero-dynamodb-records-v1";
const MAX_PARTITION_BYTES: usize = 2_048;

struct DynamoDbRecordStore {
    binding: RecordBinding,
    inner: DynamoDbStore,
    partition_base: String,
}

pub(super) struct PreparedRecordIdentity {
    endpoint: String,
    table_arn: Option<String>,
    table_id: Option<String>,
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

#[expect(
    clippy::multiple_inherent_impl,
    reason = "record preparation shares the ordinary provider's cached client and table preflight"
)]
impl AwsPreparation {
    /// Prepare a fresh partitioned record binding, never the legacy ordinary format.
    ///
    /// Uses `GetItem`, `UpdateItem`, `PutItem`, `DeleteItem`, strong base-table Query and
    /// unordered Scan. Scan evaluates rows across the table before filtering.
    /// Allocators have no TTL and must not be deleted with value rows. Native TTL
    /// uses `ttl_epoch_s`; expiry is asynchronous and not filtered on reads.
    ///
    /// # Errors
    /// Returns a sanitized preparation failure without writing a probe record.
    #[inline]
    pub async fn prepare_dynamodb_records(
        &mut self,
        settings: &DynamoDbKvSettings,
        layout: &RecordLayout,
    ) -> Result<KvHandle, PreparationError> {
        let partition_base = record_partition_base(settings, layout)?;
        let inner = self.prepare_dynamodb_store(settings).await?;
        Ok(KvHandle::new(Arc::new(DynamoDbRecordStore::new(
            inner,
            layout.clone(),
            partition_base,
        )?)))
    }
}

#[expect(
    clippy::arbitrary_source_item_ordering,
    reason = "keep prepared identity, point operations and native pagination helpers grouped"
)]
impl DynamoDbRecordStore {
    fn new(
        inner: DynamoDbStore,
        layout: RecordLayout,
        partition_base: String,
    ) -> Result<Self, PreparationError> {
        let identity = inner
            .record_identity
            .as_ref()
            .ok_or(PreparationError::Malformed)?;
        let table_arn = identity
            .table_arn
            .as_deref()
            .filter(|arn| !arn.is_empty())
            .ok_or(PreparationError::Malformed)?;
        let scope = serde_json::to_string(&(
            &identity.endpoint,
            table_arn,
            &identity.table_id,
            &inner.region,
            &inner.table,
            &inner.namespace,
        ))
        .map_err(|_error| PreparationError::Malformed)?;
        Ok(Self {
            binding: RecordBinding {
                layout,
                scope,
                format: FORMAT.to_owned(),
                // Metadata may use the full configured payload budget when the
                // body is empty. Inserts also enforce their combined byte size.
                max_metadata_bytes: inner.limits.max_value_bytes,
            },
            inner,
            partition_base,
        })
    }

    fn partition_key(&self, partition: &RecordPartition) -> Result<String, KvError> {
        if self.binding.layout.partition(partition.prefix())? != *partition {
            return Err(invalid_input());
        }
        let pk = format!("{}{}", self.partition_base, partition.prefix());
        if pk.len() > MAX_PARTITION_BYTES {
            return Err(invalid_input());
        }
        Ok(pk)
    }

    fn key(
        &self,
        key: &str,
        partition: &RecordPartition,
        kind: &str,
    ) -> Result<HashMap<String, AttributeValue>, KvError> {
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
        self.inner
            .bounded(deadline, async {
                let output = self
                    .inner
                    .client
                    .update_item()
                    .table_name(&self.inner.table)
                    .set_key(Some(self.key(key, partition, "g")?))
                    .update_expression("SET #generation = if_not_exists(#generation, :zero) + :one")
                    .condition_expression(
                        "attribute_not_exists(#generation) OR #generation < :ceiling",
                    )
                    .expression_attribute_names("#generation", "generation")
                    .expression_attribute_values(":zero", AttributeValue::N("0".to_owned()))
                    .expression_attribute_values(":one", AttributeValue::N("1".to_owned()))
                    .expression_attribute_values(
                        ":ceiling",
                        AttributeValue::N(u64::MAX.to_string()),
                    )
                    .return_values(ReturnValue::UpdatedNew)
                    .customize()
                    .config_override(self.inner.operation_policy(deadline)?)
                    .send()
                    .await
                    .map_err(|_error| KvError::Unavailable)?;
                let generation = output
                    .attributes
                    .as_ref()
                    .and_then(|attributes| attributes.get("generation"))
                    .and_then(|value| value.as_n().ok())
                    .ok_or(KvError::Unavailable)
                    .and_then(|value| read_unsigned(value))?;
                if generation == 0 {
                    return Err(KvError::Unavailable);
                }
                Ok(RecordGeneration::from_u64(generation))
            })
            .await
    }

    async fn mutate<T>(
        &self,
        deadline: Deadline,
        operation: impl Future<Output = Result<T, KvError>>,
    ) -> Result<T, KvError> {
        let remaining = deadline
            .remaining_at(self.inner.clock.now())
            .filter(|remaining| !remaining.is_zero())
            .ok_or(KvError::Unavailable)?;
        let result = timeout(remaining, operation)
            .await
            .map_err(|_elapsed| KvError::MutationUncertain)?;
        if deadline.is_expired_at(self.inner.clock.now()) {
            return Err(KvError::MutationUncertain);
        }
        result
    }

    async fn insert(
        &self,
        key: &str,
        partition: &RecordPartition,
        write: RecordWrite<'_>,
    ) -> Result<RecordWriteOutcome, KvError> {
        let deadline = self.inner.runtime_deadline()?;
        let metadata = write.metadata.filter(|text| !text.is_empty());
        if write
            .body
            .len()
            .checked_add(metadata.map_or(0, str::len))
            .is_none_or(|bytes| bytes > self.inner.limits.max_value_bytes)
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
        item.insert("v".to_owned(), AttributeValue::B(Blob::new(write.body)));
        item.insert("format".to_owned(), AttributeValue::S(FORMAT.to_owned()));
        item.insert("kind".to_owned(), AttributeValue::S("r".to_owned()));
        if let Some(text) = metadata {
            item.insert("metadata".to_owned(), AttributeValue::S(text.to_owned()));
        }
        if let Some(expires) = expiry {
            item.insert(
                "ttl_epoch_s".to_owned(),
                AttributeValue::N(expires.to_string()),
            );
        }
        // Include the maximum generation before allocation so oversized values
        // never consume an allocator request.
        item.insert(
            "generation".to_owned(),
            AttributeValue::N(u64::MAX.to_string()),
        );
        validate_record_size(&item)?;
        let generation = self.allocate(key, partition, deadline).await?;
        item.insert(
            "generation".to_owned(),
            AttributeValue::N(generation.as_u64().to_string()),
        );
        // Resolve the remaining policy before constructing the mutation future.
        // Deadline exhaustion after allocation alone remains Unavailable.
        let policy = self.inner.operation_policy(deadline)?;
        let mut request = self
            .inner
            .client
            .put_item()
            .table_name(&self.inner.table)
            .set_item(Some(item));
        match write.mode {
            RecordWriteMode::Overwrite => {}
            RecordWriteMode::Add => {
                request = request
                    .condition_expression("attribute_not_exists(#pk)")
                    .expression_attribute_names("#pk", "pk");
            }
            RecordWriteMode::IfGenerationMatch(expected) => {
                request = request
                    .condition_expression("#generation = :expected")
                    .expression_attribute_names("#generation", "generation")
                    .expression_attribute_values(
                        ":expected",
                        AttributeValue::N(expected.as_u64().to_string()),
                    );
            }
        }
        self.mutate(deadline, async {
            match request.customize().config_override(policy).send().await {
                Ok(_) => Ok(RecordWriteOutcome::Written),
                Err(error)
                    if write.mode != RecordWriteMode::Overwrite
                        && error.as_service_error().is_some_and(|service| {
                            service.is_conditional_check_failed_exception()
                        }) =>
                {
                    Ok(RecordWriteOutcome::PreconditionFailed)
                }
                Err(error) => Err(mutation_error(&error)),
            }
        })
        .await
    }

    async fn lookup(
        &self,
        key: &str,
        partition: &RecordPartition,
    ) -> Result<Option<Record>, KvError> {
        let deadline = self.inner.runtime_deadline()?;
        let expected = self.key(key, partition, "r")?;
        self.inner
            .bounded(deadline, async {
                let output = self
                    .inner
                    .client
                    .get_item()
                    .table_name(&self.inner.table)
                    .set_key(Some(expected.clone()))
                    .consistent_read(true)
                    .customize()
                    .config_override(self.inner.operation_policy(deadline)?)
                    .send()
                    .await
                    .map_err(|_error| KvError::Unavailable)?;
                output
                    .item
                    .map(|mut item| {
                        if expected
                            .iter()
                            .any(|(name, value)| item.get(name) != Some(value))
                        {
                            return Err(KvError::Unavailable);
                        }
                        validate_value_kind(&item)?;
                        let generation = match item.remove("generation") {
                            Some(AttributeValue::N(value)) => {
                                RecordGeneration::from_u64(read_unsigned(&value)?)
                            }
                            _ => return Err(KvError::Unavailable),
                        };
                        let metadata = match item.remove("metadata") {
                            None => None,
                            Some(AttributeValue::S(text))
                                if text.len() <= self.inner.limits.max_value_bytes =>
                            {
                                (!text.is_empty()).then_some(text)
                            }
                            _ => return Err(KvError::Unavailable),
                        };
                        let body = match item.remove("v") {
                            Some(AttributeValue::B(body))
                                if body.as_ref().len() <= self.inner.limits.max_value_bytes =>
                            {
                                Bytes::from(body.into_inner())
                            }
                            _ => return Err(KvError::Unavailable),
                        };
                        if body
                            .len()
                            .checked_add(metadata.as_deref().map_or(0, str::len))
                            .is_none_or(|bytes| bytes > self.inner.limits.max_value_bytes)
                        {
                            return Err(KvError::Unavailable);
                        }
                        // Check native TTL's shape but never impose a hard expiry filter.
                        if let Some(ttl) = item.get("ttl_epoch_s") {
                            let number = ttl.as_n().map_err(|_error| KvError::Unavailable)?;
                            read_unsigned(number)?;
                        }
                        Ok(Record {
                            body,
                            metadata,
                            generation,
                        })
                    })
                    .transpose()
            })
            .await
    }

    async fn delete_value(&self, key: &str, partition: &RecordPartition) -> Result<(), KvError> {
        let deadline = self.inner.runtime_deadline()?;
        let attributes = self.key(key, partition, "r")?;
        let policy = self.inner.operation_policy(deadline)?;
        self.mutate(deadline, async {
            self.inner
                .client
                .delete_item()
                .table_name(&self.inner.table)
                .set_key(Some(attributes))
                .customize()
                .config_override(policy)
                .send()
                .await
                .map_err(|error| mutation_error(&error))?;
            Ok(())
        })
        .await
    }

    fn decode_cursor(
        &self,
        mode: &str,
        prefix: &str,
        encoded: &str,
        partition: Option<&RecordPartition>,
    ) -> Result<HashMap<String, AttributeValue>, KvError> {
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
            && key.get("pk") != Some(&AttributeValue::S(self.partition_key(expected_partition)?))
        {
            return Err(invalid_input());
        }
        Ok(key)
    }

    fn next_cursor(
        &self,
        mode: &str,
        prefix: &str,
        after: Option<&HashMap<String, AttributeValue>>,
        next_position: Option<HashMap<String, AttributeValue>>,
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
                    .is_some_and(|previous| sk <= previous.as_str()))
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
        items: Vec<HashMap<String, AttributeValue>>,
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

    async fn page(
        &self,
        prefix: &str,
        partition: Option<&RecordPartition>,
        cursor: Option<&str>,
        limit: usize,
    ) -> Result<KvPage, KvError> {
        let deadline = self.inner.runtime_deadline()?;
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
        let native_limit = i32::try_from(limit).map_err(|_error| invalid_input())?;
        let names = HashMap::from([
            ("#pk".to_owned(), "pk".to_owned()),
            ("#sk".to_owned(), "sk".to_owned()),
            ("#format".to_owned(), "format".to_owned()),
            ("#kind".to_owned(), "kind".to_owned()),
        ]);
        let base_values = HashMap::from([
            (":format".to_owned(), AttributeValue::S(FORMAT.to_owned())),
            (":kind".to_owned(), AttributeValue::S("r".to_owned())),
            (
                ":prefix".to_owned(),
                AttributeValue::S(format!("r#{prefix}")),
            ),
        ]);
        self.inner.bounded(deadline, async {
            let (items, position) = if let Some(query_partition) = partition {
                let mut values = base_values;
                values.insert(":pk".to_owned(), AttributeValue::S(self.partition_key(query_partition)?));
                let output = self.inner.client.query().table_name(&self.inner.table).consistent_read(true)
                    .scan_index_forward(true).limit(native_limit)
                    .key_condition_expression("#pk = :pk")
                    .filter_expression("#format = :format AND #kind = :kind")
                    .projection_expression("#pk, #sk, #format, #kind")
                    .set_expression_attribute_names(Some(names))
                    // Query cannot filter a key attribute. Full membership is
                    // checked below, while the partition PK already bounds keys.
                    .set_expression_attribute_values(Some(values.into_iter().filter(|(name, _)| name != ":prefix").collect()))
                    .set_exclusive_start_key(after.clone())
                    .customize().config_override(self.inner.operation_policy(deadline)?)
                    .send().await.map_err(|_error| KvError::Unavailable)?;
                (output.items.unwrap_or_default(), output.last_evaluated_key)
            } else {
                let mut values = base_values;
                values.insert(":binding".to_owned(), AttributeValue::S(self.partition_base.clone()));
                let output = self.inner.client.scan().table_name(&self.inner.table).limit(native_limit)
                    .filter_expression("begins_with(#pk, :binding) AND #format = :format AND #kind = :kind AND begins_with(#sk, :prefix)")
                    .projection_expression("#pk, #sk, #format, #kind")
                    .set_expression_attribute_names(Some(names)).set_expression_attribute_values(Some(values))
                    .set_exclusive_start_key(after.clone())
                    .customize().config_override(self.inner.operation_policy(deadline)?)
                    .send().await.map_err(|_error| KvError::Unavailable)?;
                (output.items.unwrap_or_default(), output.last_evaluated_key)
            };
            if partition.is_some() {
                validate_query_positions(&items, after.as_ref(), position.as_ref())?;
            }
            let keys = self.listed_keys(items, prefix, limit, partition)?;
            let next = self.next_cursor(mode, prefix, after.as_ref(), position, partition)?;
            Ok(KvPage { keys, cursor: next })
        }).await
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
impl KvStore for DynamoDbRecordStore {
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

pub(super) fn endpoint_identity(uri: &str) -> Option<String> {
    let mut endpoint = url::Url::parse(uri).ok()?;
    endpoint.host_str()?;
    endpoint.set_username("").ok()?;
    endpoint.set_password(None).ok()?;
    endpoint.set_query(None);
    endpoint.set_fragment(None);
    Some(endpoint.into())
}

pub(super) fn prepared_identity(
    endpoint: Option<&str>,
    table: &TableDescription,
) -> Option<PreparedRecordIdentity> {
    Some(PreparedRecordIdentity {
        endpoint: endpoint?.to_owned(),
        table_arn: table.table_arn.clone(),
        table_id: table.table_id.clone(),
    })
}

fn mutation_error<E>(error: &SdkError<E>) -> KvError
where
    E: ProvideErrorMetadata,
{
    if matches!(error, SdkError::ConstructionFailure(_))
        || error.as_service_error().is_some_and(|_service| {
            error
                .raw_response()
                .is_some_and(|response| (400..500).contains(&response.status().as_u16()))
        })
    {
        KvError::Unavailable
    } else {
        KvError::MutationUncertain
    }
}

fn physical_key(pk: String, sk: String) -> HashMap<String, AttributeValue> {
    HashMap::from([
        ("pk".to_owned(), AttributeValue::S(pk)),
        ("sk".to_owned(), AttributeValue::S(sk)),
    ])
}

fn record_partition_base(
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

fn validate_position(key: &HashMap<String, AttributeValue>) -> Result<(&str, &str), KvError> {
    if key.len() != 2 {
        return Err(KvError::Unavailable);
    }
    validate_position_item(key)
}

fn validate_position_item(item: &HashMap<String, AttributeValue>) -> Result<(&str, &str), KvError> {
    match (item.get("pk"), item.get("sk")) {
        (Some(AttributeValue::S(pk)), Some(AttributeValue::S(sk)))
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
    items: &[HashMap<String, AttributeValue>],
    after: Option<&HashMap<String, AttributeValue>>,
    next_position: Option<&HashMap<String, AttributeValue>>,
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

fn validate_value_kind(item: &HashMap<String, AttributeValue>) -> Result<(), KvError> {
    if item.get("format") != Some(&AttributeValue::S(FORMAT.to_owned()))
        || item.get("kind") != Some(&AttributeValue::S("r".to_owned()))
    {
        return Err(KvError::Unavailable);
    }
    Ok(())
}

#[expect(
    clippy::iter_over_hash_type,
    reason = "checked item-byte summation is independent of attribute order"
)]
#[expect(
    clippy::wildcard_enum_match_arm,
    reason = "all non-scalar attribute types are rejected uniformly in the record format"
)]
fn validate_record_size(item: &HashMap<String, AttributeValue>) -> Result<(), KvError> {
    let mut size = 0_usize;
    for (name, value) in item {
        let bytes = match value {
            AttributeValue::S(text) | AttributeValue::N(text) => text.len(),
            AttributeValue::B(blob) => blob.as_ref().len(),
            _ => return Err(invalid_input()),
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

#[cfg(test)]
#[path = "dynamodb_records_tests.rs"]
mod tests;
