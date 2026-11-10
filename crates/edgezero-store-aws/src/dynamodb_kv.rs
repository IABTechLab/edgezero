//! Live native `DynamoDB` KV operations. SDK models never cross the portable boundary.

use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use aws_config::BehaviorVersion;
use aws_sdk_dynamodb::Client;
use aws_sdk_dynamodb::config::timeout::TimeoutConfig;
use aws_sdk_dynamodb::config::{Builder, Region, retry::RetryConfig};
use aws_sdk_dynamodb::error::SdkError;
use aws_sdk_dynamodb::primitives::Blob;
use aws_sdk_dynamodb::types::{AttributeValue, TableDescription};
use bytes::Bytes;
use edgezero_core::key_value_store::{KvError, KvHandle, KvPage, KvStore};
use edgezero_core::time::{Deadline, MonotonicClock};
use serde::{Deserialize, Serialize};
use tokio::time::timeout;

use crate::PreparationError;
use crate::preparation::AwsPreparation;
use crate::settings::{DynamoDbKvLimits, DynamoDbKvSettings};

const MAX_CURSOR_BYTES: usize = 0x4000;
const MAX_ITEM_BYTES: usize = 409_600;
const MAX_KEY_BYTES: usize = 512;
const MAX_PAGE_KEYS: usize = 1_000;

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Cursor {
    key: String,
    namespace: String,
    prefix: String,
    region: String,
    table: String,
    version: u8,
}

struct DynamoDbStore {
    client: Client,
    clock: MonotonicClock,
    limits: DynamoDbKvLimits,
    namespace: String,
    region: String,
    table: String,
}

#[expect(
    clippy::multiple_inherent_impl,
    reason = "keep feature-gated DynamoDB transport separate from other AWS services"
)]
#[expect(
    clippy::arbitrary_source_item_ordering,
    reason = "keep the supported preparation entry point ahead of its private shared setup path"
)]
impl AwsPreparation {
    /// Prepare a live namespace-bound KV handle after bounded table validation.
    ///
    /// No automatic retries are made. A failed write may already have committed.
    ///
    /// # Errors
    /// Returns a closed error for invalid bindings, deadline expiry, or provider failure.
    #[inline]
    pub async fn prepare_dynamodb(
        &mut self,
        settings: &DynamoDbKvSettings,
    ) -> Result<KvHandle, PreparationError> {
        self.validate_dynamodb_mapping(settings)?;
        let key = (settings.region.clone(), settings.limits);
        let client = if let Some(client) = self.dynamodb_clients.get(&key) {
            client.clone()
        } else {
            let budget = self.dynamodb_budget(settings)?;
            let result = timeout(budget, async {
                let shared = aws_config::defaults(BehaviorVersion::v2026_01_12())
                    .region(Region::new(settings.region.clone()))
                    .load()
                    .await;
                self.check_deadline()?;
                Ok::<Client, PreparationError>(Client::from_conf(
                    aws_sdk_dynamodb::Config::from(&shared)
                        .to_builder()
                        .region(Region::new(settings.region.clone()))
                        .retry_config(RetryConfig::standard().with_max_attempts(1))
                        .timeout_config(timeout_policy(
                            settings.limits,
                            Duration::from_millis(settings.limits.operation_timeout_ms),
                        ))
                        .build(),
                ))
            })
            .await;
            self.check_deadline()?;
            let client = result.map_err(|_elapsed| PreparationError::Deadline)??;
            self.dynamodb_clients.insert(key, client.clone());
            client
        };
        self.finish_dynamodb_preparation(settings, client).await
    }

    fn dynamodb_budget(&self, settings: &DynamoDbKvSettings) -> Result<Duration, PreparationError> {
        Ok(self
            .operation_budget()?
            .min(Duration::from_millis(settings.limits.operation_timeout_ms)))
    }

    async fn finish_dynamodb_preparation(
        &self,
        settings: &DynamoDbKvSettings,
        client: Client,
    ) -> Result<KvHandle, PreparationError> {
        let budget = self.dynamodb_budget(settings)?;
        let result = timeout(
            budget,
            client
                .describe_table()
                .table_name(&settings.table)
                .customize()
                .config_override(
                    aws_sdk_dynamodb::Config::builder()
                        .retry_config(RetryConfig::standard().with_max_attempts(1))
                        .timeout_config(timeout_policy(settings.limits, budget)),
                )
                .send(),
        )
        .await;
        self.check_deadline()?;
        let output = result
            .map_err(|_elapsed| PreparationError::Deadline)?
            .map_err(|error| {
                if matches!(&error, SdkError::TimeoutError(_)) {
                    return PreparationError::Deadline;
                }
                classify_preparation_error(
                    error
                        .as_service_error()
                        .and_then(|service| service.meta().code()),
                    error
                        .raw_response()
                        .map(|response| response.status().as_u16()),
                )
            })?;
        let validation = output
            .table
            .as_ref()
            .ok_or(PreparationError::Malformed)
            .and_then(|table| validate_table(table, &settings.table));
        self.check_deadline()?;
        validation?;
        let handle = KvHandle::new(Arc::new(DynamoDbStore::new(
            settings,
            client,
            self.clock.clone(),
        )));
        self.check_deadline()?;
        Ok(handle)
    }

    #[cfg(test)]
    async fn prepare_dynamodb_with_client(
        &mut self,
        settings: &DynamoDbKvSettings,
        client: Client,
    ) -> Result<KvHandle, PreparationError> {
        self.validate_dynamodb_mapping(settings)?;
        self.finish_dynamodb_preparation(settings, client).await
    }

    fn validate_dynamodb_mapping(
        &mut self,
        settings: &DynamoDbKvSettings,
    ) -> Result<(), PreparationError> {
        settings.validate()?;
        self.check_deadline()?;
        let metadata = settings
            .region
            .len()
            .checked_add(settings.table.len())
            .ok_or(PreparationError::SizeLimit)?;
        self.charge_mapping(0, settings.namespace.len(), metadata)
    }
}

impl DynamoDbStore {
    async fn bounded<T, Operation>(
        &self,
        deadline: Deadline,
        operation: Operation,
    ) -> Result<T, KvError>
    where
        Operation: Future<Output = Result<T, KvError>>,
    {
        let remaining = deadline
            .remaining_at(self.clock.now())
            .filter(|remaining| !remaining.is_zero())
            .ok_or(KvError::Unavailable)?;
        let result = timeout(remaining, operation).await;
        self.check_runtime_deadline(deadline)?;
        result.map_err(|_elapsed| KvError::Unavailable)?
    }

    fn check_runtime_deadline(&self, deadline: Deadline) -> Result<(), KvError> {
        if deadline.is_expired_at(self.clock.now()) {
            return Err(KvError::Unavailable);
        }
        Ok(())
    }

    fn decode_cursor(&self, prefix: &str, encoded: &str) -> Result<String, KvError> {
        decode_cursor(encoded, prefix, &self.region, &self.table, &self.namespace)
    }

    fn encode_cursor(&self, prefix: &str, key: String) -> Result<String, KvError> {
        let cursor = Cursor {
            key,
            namespace: self.namespace.clone(),
            prefix: prefix.to_owned(),
            region: self.region.clone(),
            table: self.table.clone(),
            version: 1,
        };
        let encoded = serde_json::to_string(&cursor).map_err(|_error| KvError::Unavailable)?;
        if encoded.len() > MAX_CURSOR_BYTES {
            return Err(KvError::Unavailable);
        }
        Ok(encoded)
    }

    fn key_attributes(&self, key: &str) -> HashMap<String, AttributeValue> {
        HashMap::from([
            ("pk".to_owned(), AttributeValue::S(self.namespace.clone())),
            ("sk".to_owned(), AttributeValue::S(key.to_owned())),
        ])
    }

    fn new(settings: &DynamoDbKvSettings, client: Client, clock: MonotonicClock) -> Self {
        Self {
            client,
            clock,
            limits: settings.limits,
            namespace: settings.namespace.clone(),
            region: settings.region.clone(),
            table: settings.table.clone(),
        }
    }

    fn operation_policy(&self, deadline: Deadline) -> Result<Builder, KvError> {
        let budget = deadline
            .remaining_at(self.clock.now())
            .filter(|remaining| !remaining.is_zero())
            .ok_or(KvError::Unavailable)?;
        Ok(aws_sdk_dynamodb::Config::builder()
            .retry_config(RetryConfig::standard().with_max_attempts(1))
            .timeout_config(timeout_policy(self.limits, budget)))
    }

    async fn put(&self, key: &str, value: Bytes, ttl: Option<Duration>) -> Result<(), KvError> {
        let deadline = self.runtime_deadline()?;
        if !valid_key(key) || value.len() > self.limits.max_value_bytes {
            return Err(invalid_input());
        }
        let expiry = ttl
            .map(|duration| {
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map_err(|_error| KvError::Unavailable)?;
                expiry_from_duration(now, duration)
            })
            .transpose()?;
        validate_item_size(self.namespace.len(), key.len(), value.len(), expiry)?;
        let mut item = self.key_attributes(key);
        // The payload cap and complete item accounting are checked before this copy.
        item.insert("v".to_owned(), AttributeValue::B(Blob::new(value.to_vec())));
        if let Some(expires_at) = expiry {
            item.insert(
                "expires_at_ms".to_owned(),
                AttributeValue::N(expires_at.to_string()),
            );
            item.insert(
                "ttl_epoch_s".to_owned(),
                AttributeValue::N(expires_at.div_ceil(1_000).to_string()),
            );
        }
        self.bounded(deadline, async {
            self.client
                .put_item()
                .table_name(&self.table)
                .set_item(Some(item))
                .customize()
                .config_override(self.operation_policy(deadline)?)
                .send()
                .await
                .map_err(|_error| KvError::Unavailable)?;
            Ok(())
        })
        .await
    }

    fn runtime_deadline(&self) -> Result<Deadline, KvError> {
        self.clock
            .now()
            .checked_add(Duration::from_millis(self.limits.operation_timeout_ms))
            .map(Deadline::at_instant)
            .ok_or(KvError::Unavailable)
    }
}

#[async_trait(?Send)]
impl KvStore for DynamoDbStore {
    #[inline]
    async fn delete(&self, key: &str) -> Result<(), KvError> {
        let deadline = self.runtime_deadline()?;
        if !valid_key(key) {
            return Err(invalid_input());
        }
        self.bounded(deadline, async {
            self.client
                .delete_item()
                .table_name(&self.table)
                .set_key(Some(self.key_attributes(key)))
                .customize()
                .config_override(self.operation_policy(deadline)?)
                .send()
                .await
                .map_err(|_error| KvError::Unavailable)?;
            Ok(())
        })
        .await
    }

    #[inline]
    async fn exists(&self, key: &str) -> Result<bool, KvError> {
        Ok(self.get_bytes(key).await?.is_some())
    }

    #[inline]
    async fn get_bytes(&self, key: &str) -> Result<Option<Bytes>, KvError> {
        let deadline = self.runtime_deadline()?;
        if !valid_key(key) {
            return Err(invalid_input());
        }
        let result = self
            .bounded(deadline, async {
                let output = self
                    .client
                    .get_item()
                    .table_name(&self.table)
                    .set_key(Some(self.key_attributes(key)))
                    .consistent_read(true)
                    .customize()
                    .config_override(self.operation_policy(deadline)?)
                    .send()
                    .await
                    .map_err(|_error| KvError::Unavailable)?;
                let Some(mut item) = output.item else {
                    return Ok(None);
                };
                if row_key(&item, &self.namespace)? != key {
                    return Err(KvError::Unavailable);
                }
                let expiry = read_expiry(&item)?;
                let value = match item.remove("v") {
                    Some(AttributeValue::B(value))
                        if value.as_ref().len() <= self.limits.max_value_bytes =>
                    {
                        Bytes::from(value.into_inner())
                    }
                    _ => return Err(KvError::Unavailable),
                };
                Ok(Some((value, expiry)))
            })
            .await?;
        let now = epoch_ms()?;
        let value = result.and_then(|(value, expiry)| (!is_expired(expiry, now)).then_some(value));
        self.check_runtime_deadline(deadline)?;
        Ok(value)
    }

    #[inline]
    async fn list_keys_page(
        &self,
        prefix: &str,
        cursor: Option<&str>,
        limit: usize,
    ) -> Result<KvPage, KvError> {
        let deadline = self.runtime_deadline()?;
        if prefix.len() > MAX_KEY_BYTES
            || prefix.chars().any(char::is_control)
            || limit == 0
            || limit > MAX_PAGE_KEYS
        {
            return Err(invalid_input());
        }
        let after = cursor
            .map(|encoded| self.decode_cursor(prefix, encoded))
            .transpose()?;
        let query_limit = i32::try_from(limit).map_err(|_error| invalid_input())?;
        self.bounded(deadline, async {
            let mut request = self
                .client
                .query()
                .table_name(&self.table)
                .consistent_read(true)
                .scan_index_forward(true)
                .limit(query_limit)
                .key_condition_expression(if prefix.is_empty() {
                    "#pk = :pk"
                } else {
                    "#pk = :pk AND begins_with(#sk, :prefix)"
                })
                .projection_expression("#pk, #sk, #expires, #ttl")
                .expression_attribute_names("#pk", "pk")
                .expression_attribute_names("#sk", "sk")
                .expression_attribute_names("#expires", "expires_at_ms")
                .expression_attribute_names("#ttl", "ttl_epoch_s")
                .expression_attribute_values(":pk", AttributeValue::S(self.namespace.clone()));
            if !prefix.is_empty() {
                request = request
                    .expression_attribute_values(":prefix", AttributeValue::S(prefix.to_owned()));
            }
            if let Some(key) = &after {
                request = request.set_exclusive_start_key(Some(self.key_attributes(key)));
            }
            let output = request
                .customize()
                .config_override(self.operation_policy(deadline)?)
                .send()
                .await
                .map_err(|_error| KvError::Unavailable)?;
            let items = output.items.unwrap_or_default();
            if items.len() > limit {
                return Err(KvError::Unavailable);
            }
            let mut rows = Vec::with_capacity(items.len());
            let mut previous = after.clone();
            for item in &items {
                let key = row_key(item, &self.namespace)?;
                if !key.starts_with(prefix) || previous.as_deref().is_some_and(|last| key <= last) {
                    return Err(KvError::Unavailable);
                }
                previous = Some(key.to_owned());
                rows.push((key.to_owned(), read_expiry(item)?));
            }
            let next = match output.last_evaluated_key {
                None => None,
                Some(key) if key.is_empty() => None,
                Some(key) => {
                    if key.len() != 2 {
                        return Err(KvError::Unavailable);
                    }
                    let next_key = row_key(&key, &self.namespace)?;
                    if !next_key.starts_with(prefix)
                        || after.as_deref().is_some_and(|last| next_key <= last)
                        || previous.as_deref().is_some_and(|last| next_key < last)
                    {
                        return Err(KvError::Unavailable);
                    }
                    Some(self.encode_cursor(prefix, next_key.to_owned())?)
                }
            };
            // Expiry is checked after validation and cursor encoding, not at request start.
            let completed = epoch_ms()?;
            let keys = rows
                .into_iter()
                .filter_map(|(key, expiry)| (!is_expired(expiry, completed)).then_some(key))
                .collect();
            Ok(KvPage { cursor: next, keys })
        })
        .await
    }

    #[inline]
    async fn put_bytes(&self, key: &str, value: Bytes) -> Result<(), KvError> {
        self.put(key, value, None).await
    }

    #[inline]
    async fn put_bytes_with_ttl(
        &self,
        key: &str,
        value: Bytes,
        ttl: Duration,
    ) -> Result<(), KvError> {
        self.put(key, value, Some(ttl)).await
    }
}

fn classify_preparation_error(code: Option<&str>, status: Option<u16>) -> PreparationError {
    match code {
        Some("AccessDeniedException" | "AccessDenied") => PreparationError::AccessDenied,
        Some("ExpiredTokenException" | "InvalidClientTokenId" | "UnrecognizedClientException") => {
            PreparationError::Credentials
        }
        Some("ResourceNotFoundException") => PreparationError::Missing,
        Some(
            "ThrottlingException"
            | "ProvisionedThroughputExceededException"
            | "RequestLimitExceeded",
        ) => PreparationError::Throttled,
        Some("ValidationException") => PreparationError::InvalidResource,
        _ => match status {
            Some(401 | 403) => PreparationError::AccessDenied,
            Some(429) => PreparationError::Throttled,
            _ => PreparationError::Unavailable,
        },
    }
}

fn decode_cursor(
    encoded: &str,
    prefix: &str,
    region: &str,
    table: &str,
    namespace: &str,
) -> Result<String, KvError> {
    if encoded.len() > MAX_CURSOR_BYTES {
        return Err(invalid_input());
    }
    let cursor: Cursor = serde_json::from_str(encoded).map_err(|_error| invalid_input())?;
    if cursor.version != 1
        || cursor.region != region
        || cursor.table != table
        || cursor.namespace != namespace
        || cursor.prefix != prefix
        || !valid_key(&cursor.key)
        || !cursor.key.starts_with(prefix)
    {
        return Err(invalid_input());
    }
    Ok(cursor.key)
}

fn epoch_ms() -> Result<u64, KvError> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_error| KvError::Unavailable)?;
    u64::try_from(now.as_millis()).map_err(|_error| KvError::Unavailable)
}

fn expiry_from_duration(now: Duration, ttl: Duration) -> Result<u64, KvError> {
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

fn invalid_input() -> KvError {
    KvError::Validation("invalid DynamoDB KV input".to_owned())
}

fn is_expired(expiry: Option<u64>, now: u64) -> bool {
    expiry.is_some_and(|expires| now >= expires)
}

fn read_expiry(item: &HashMap<String, AttributeValue>) -> Result<Option<u64>, KvError> {
    match (item.get("expires_at_ms"), item.get("ttl_epoch_s")) {
        (None, None) => Ok(None),
        (Some(AttributeValue::N(expiry)), Some(AttributeValue::N(ttl))) => {
            let expires = read_unsigned(expiry)?;
            if read_unsigned(ttl)? != expires.div_ceil(1_000) {
                return Err(KvError::Unavailable);
            }
            Ok(Some(expires))
        }
        _ => Err(KvError::Unavailable),
    }
}

fn read_unsigned(value: &str) -> Result<u64, KvError> {
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

fn row_key<'row>(
    item: &'row HashMap<String, AttributeValue>,
    namespace: &str,
) -> Result<&'row str, KvError> {
    match (item.get("pk"), item.get("sk")) {
        (Some(AttributeValue::S(partition)), Some(AttributeValue::S(key)))
            if partition == namespace && valid_key(key) =>
        {
            Ok(key)
        }
        _ => Err(KvError::Unavailable),
    }
}

fn timeout_policy(limits: DynamoDbKvLimits, budget: Duration) -> TimeoutConfig {
    TimeoutConfig::builder()
        .connect_timeout(budget.min(Duration::from_millis(limits.connect_timeout_ms)))
        .operation_attempt_timeout(budget.min(Duration::from_millis(limits.attempt_timeout_ms)))
        .operation_timeout(budget)
        .build()
}

fn valid_key(key: &str) -> bool {
    !key.is_empty()
        && key.len() <= MAX_KEY_BYTES
        && key != "."
        && key != ".."
        && !key.chars().any(char::is_control)
}

fn validate_item_size(
    namespace: usize,
    key: usize,
    value: usize,
    expiry: Option<u64>,
) -> Result<(), KvError> {
    let expiry_bytes = expiry
        .map(|expires| {
            "expires_at_ms"
                .len()
                .checked_add(expires.to_string().len())
                .and_then(|size| size.checked_add("ttl_epoch_s".len()))
                .and_then(|size| size.checked_add(expires.div_ceil(1_000).to_string().len()))
                .ok_or_else(invalid_input)
        })
        .transpose()?
        .unwrap_or_default();
    // Decimal text overestimates DynamoDB numeric storage and keeps accounting conservative.
    let size = namespace
        .checked_add(key)
        .and_then(|size| size.checked_add(value))
        .and_then(|size| size.checked_add("pk".len()))
        .and_then(|size| size.checked_add("sk".len()))
        .and_then(|size| size.checked_add("v".len()))
        .and_then(|size| size.checked_add(expiry_bytes))
        .ok_or_else(invalid_input)?;
    if namespace > MAX_KEY_BYTES || key > MAX_KEY_BYTES || size > MAX_ITEM_BYTES {
        return Err(invalid_input());
    }
    Ok(())
}

fn validate_table(table: &TableDescription, expected_name: &str) -> Result<(), PreparationError> {
    if table.table_name.as_deref() != Some(expected_name) {
        return Err(PreparationError::InvalidResource);
    }
    if table
        .table_status
        .as_ref()
        .is_none_or(|status| status.as_str() != "ACTIVE")
        || !table.global_secondary_indexes().is_empty()
        || !table.local_secondary_indexes().is_empty()
        || table
            .vector_indexes
            .as_ref()
            .is_some_and(|indexes| !indexes.is_empty())
        || !table.replicas().is_empty()
        || table.global_table_version.is_some()
    {
        return Err(PreparationError::InvalidResource);
    }
    let keys = table.key_schema();
    let attributes = table.attribute_definitions();
    if keys.len() != 2
        || attributes.len() != 2
        || !keys
            .iter()
            .any(|key| key.attribute_name() == "pk" && key.key_type().as_str() == "HASH")
        || !keys
            .iter()
            .any(|key| key.attribute_name() == "sk" && key.key_type().as_str() == "RANGE")
        || !attributes.iter().any(|attribute| {
            attribute.attribute_name() == "pk" && attribute.attribute_type().as_str() == "S"
        })
        || !attributes.iter().any(|attribute| {
            attribute.attribute_name() == "sk" && attribute.attribute_type().as_str() == "S"
        })
    {
        return Err(PreparationError::InvalidResource);
    }
    Ok(())
}

#[cfg(test)]
#[path = "dynamodb_tests.rs"]
mod sdk_tests;

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::time::Duration;

    use aws_sdk_dynamodb::types::{
        AttributeDefinition, AttributeValue, KeySchemaElement, KeyType, ScalarAttributeType,
        TableDescription, TableStatus,
    };
    use edgezero_core::key_value_store::KvError;

    use crate::PreparationError;

    use super::{
        Cursor, MAX_CURSOR_BYTES, decode_cursor, expiry_from_duration, is_expired, read_expiry,
        read_unsigned, row_key, valid_key, validate_item_size, validate_table,
    };

    fn active_table_description() -> TableDescription {
        TableDescription::builder()
            .table_name("expected-table")
            .table_status(TableStatus::Active)
            .key_schema(
                KeySchemaElement::builder()
                    .attribute_name("pk")
                    .key_type(KeyType::Hash)
                    .build()
                    .unwrap(),
            )
            .key_schema(
                KeySchemaElement::builder()
                    .attribute_name("sk")
                    .key_type(KeyType::Range)
                    .build()
                    .unwrap(),
            )
            .attribute_definitions(
                AttributeDefinition::builder()
                    .attribute_name("pk")
                    .attribute_type(ScalarAttributeType::S)
                    .build()
                    .unwrap(),
            )
            .attribute_definitions(
                AttributeDefinition::builder()
                    .attribute_name("sk")
                    .attribute_type(ScalarAttributeType::S)
                    .build()
                    .unwrap(),
            )
            .build()
    }

    #[test]
    fn cursor_codec_rejects_extra_fields_and_versions_are_explicit() {
        let cursor = Cursor {
            key: "a/b".to_owned(),
            namespace: "ns".to_owned(),
            prefix: "a/".to_owned(),
            region: "us-east-1".to_owned(),
            table: "table".to_owned(),
            version: 1,
        };
        let encoded = serde_json::to_string(&cursor).unwrap();
        let decoded: Cursor = serde_json::from_str(&encoded).unwrap();
        assert_eq!(decoded.key, "a/b");
        assert_eq!(decoded.version, 1);
        let mut object = serde_json::to_value(&cursor).unwrap();
        object
            .as_object_mut()
            .unwrap()
            .insert("unknown".to_owned(), serde_json::json!(1_i32));
        assert!(serde_json::from_value::<Cursor>(object).is_err());
    }

    #[test]
    fn cursor_validation_rejects_every_scope_and_invalid_continuations() {
        let cursor = Cursor {
            key: "a/b".to_owned(),
            namespace: "ns".to_owned(),
            prefix: "a/".to_owned(),
            region: "us-east-1".to_owned(),
            table: "expected-table".to_owned(),
            version: 1,
        };
        let valid_encoded = serde_json::to_string(&cursor).unwrap();
        assert_eq!(
            decode_cursor(&valid_encoded, "a/", "us-east-1", "expected-table", "ns").unwrap(),
            "a/b"
        );
        for field in ["namespace", "prefix", "region", "table"] {
            let mut object = serde_json::to_value(&cursor).unwrap();
            object
                .as_object_mut()
                .unwrap()
                .insert(field.to_owned(), serde_json::json!("other"));
            let scope_encoded = serde_json::to_string(&object).unwrap();
            assert!(matches!(
                decode_cursor(&scope_encoded, "a/", "us-east-1", "expected-table", "ns")
                    .unwrap_err(),
                KvError::Validation(_)
            ));
        }
        for key in ["", ".", "..", "other/key", "a/\n"] {
            let mut object = serde_json::to_value(&cursor).unwrap();
            object
                .as_object_mut()
                .unwrap()
                .insert("key".to_owned(), serde_json::json!(key));
            let key_encoded = serde_json::to_string(&object).unwrap();
            assert!(matches!(
                decode_cursor(&key_encoded, "a/", "us-east-1", "expected-table", "ns").unwrap_err(),
                KvError::Validation(_)
            ));
        }
        let mut object = serde_json::to_value(&cursor).unwrap();
        object
            .as_object_mut()
            .unwrap()
            .insert("version".to_owned(), serde_json::json!(2_u8));
        let version_encoded = serde_json::to_string(&object).unwrap();
        assert!(matches!(
            decode_cursor(&version_encoded, "a/", "us-east-1", "expected-table", "ns").unwrap_err(),
            KvError::Validation(_)
        ));
        object.as_object_mut().unwrap().remove("namespace");
        let missing_scope_encoded = serde_json::to_string(&object).unwrap();
        decode_cursor(
            &missing_scope_encoded,
            "a/",
            "us-east-1",
            "expected-table",
            "ns",
        )
        .unwrap_err();
        let oversized = "x".repeat(MAX_CURSOR_BYTES.checked_add(1).unwrap());
        decode_cursor(&oversized, "a/", "us-east-1", "expected-table", "ns").unwrap_err();
    }

    #[test]
    fn expiry_metadata_rejects_wrong_types_and_unbounded_numbers() {
        let valid = HashMap::from([
            (
                "expires_at_ms".to_owned(),
                AttributeValue::N("1001".to_owned()),
            ),
            ("ttl_epoch_s".to_owned(), AttributeValue::N("2".to_owned())),
        ]);
        for attribute in ["expires_at_ms", "ttl_epoch_s"] {
            for invalid in [
                AttributeValue::S("2".to_owned()),
                AttributeValue::Bool(false),
                AttributeValue::Null(true),
                AttributeValue::N("-1".to_owned()),
                AttributeValue::N("NaN".to_owned()),
                AttributeValue::N("100000000000000000000".to_owned()),
            ] {
                let mut row = valid.clone();
                row.insert(attribute.to_owned(), invalid);
                assert!(matches!(
                    read_expiry(&row).unwrap_err(),
                    KvError::Unavailable
                ));
            }
            let mut row = valid.clone();
            row.remove(attribute);
            assert!(matches!(
                read_expiry(&row).unwrap_err(),
                KvError::Unavailable
            ));
        }
        let mut inconsistent = valid;
        inconsistent.insert("ttl_epoch_s".to_owned(), AttributeValue::N("1".to_owned()));
        assert!(matches!(
            read_expiry(&inconsistent).unwrap_err(),
            KvError::Unavailable
        ));
        let maximum = HashMap::from([
            (
                "expires_at_ms".to_owned(),
                AttributeValue::N(u64::MAX.to_string()),
            ),
            (
                "ttl_epoch_s".to_owned(),
                AttributeValue::N(u64::MAX.div_ceil(1_000).to_string()),
            ),
        ]);
        assert_eq!(read_expiry(&maximum).unwrap(), Some(u64::MAX));
    }

    #[test]
    fn expiry_pairs_are_checked_and_deadline_is_inclusive() {
        let mut row = HashMap::from([
            (
                "expires_at_ms".to_owned(),
                AttributeValue::N("1001".to_owned()),
            ),
            ("ttl_epoch_s".to_owned(), AttributeValue::N("2".to_owned())),
        ]);
        assert_eq!(read_expiry(&row).unwrap(), Some(1_001));
        assert!(!is_expired(Some(1_001), 1_000));
        assert!(is_expired(Some(1_001), 1_001));
        row.remove("ttl_epoch_s");
        assert!(matches!(read_expiry(&row), Err(KvError::Unavailable)));
        row.clear();
        assert_eq!(read_expiry(&row).unwrap(), None);
    }

    #[test]
    fn expiry_rounding_never_shortens_ttl() {
        assert_eq!(
            expiry_from_duration(Duration::from_nanos(1), Duration::from_millis(1)).unwrap(),
            2
        );
        assert_eq!(
            expiry_from_duration(Duration::from_secs(1), Duration::from_millis(1)).unwrap(),
            1_001
        );
        expiry_from_duration(Duration::ZERO, Duration::ZERO).unwrap_err();
        expiry_from_duration(Duration::MAX, Duration::from_millis(1)).unwrap_err();
    }

    #[test]
    fn integer_metadata_is_bounded_and_canonical() {
        for value in ["", "-1", "+1", "01", "1.0", "1e3", "18446744073709551616"] {
            read_unsigned(value).unwrap_err();
        }
        assert_eq!(read_unsigned("18446744073709551615").unwrap(), u64::MAX);
    }

    #[test]
    fn item_size_accounting_handles_boundaries_and_overflow() {
        validate_item_size(512, 512, 393_216, Some(u64::MAX)).unwrap();
        validate_item_size(512, 512, 409_600, None).unwrap_err();
        validate_item_size(512, 512, usize::MAX, None).unwrap_err();
    }

    #[test]
    fn keys_preserve_unicode_and_reject_invalid_rows() {
        let composed = "\u{00e9}";
        let decomposed = "e\u{0301}";
        assert_ne!(composed, decomposed);
        assert!(valid_key(composed));
        assert!(valid_key(decomposed));
        assert!(valid_key(&"x".repeat(512)));
        assert!(!valid_key(&"x".repeat(513)));
        for key in ["", ".", "..", "a\n"] {
            assert!(!valid_key(key));
        }
        let row = HashMap::from([
            ("pk".to_owned(), AttributeValue::S("ns".to_owned())),
            ("sk".to_owned(), AttributeValue::S(decomposed.to_owned())),
        ]);
        assert_eq!(row_key(&row, "ns").unwrap(), decomposed);
        row_key(&row, "other").unwrap_err();
    }

    #[test]
    fn table_name_must_be_present_and_match_the_selected_resource() {
        let mut table = active_table_description();
        validate_table(&table, "expected-table").unwrap();
        assert_eq!(
            validate_table(&table, "wrong-table").unwrap_err(),
            PreparationError::InvalidResource
        );
        table.table_name = None;
        assert_eq!(
            validate_table(&table, "expected-table").unwrap_err(),
            PreparationError::InvalidResource
        );
    }
}
