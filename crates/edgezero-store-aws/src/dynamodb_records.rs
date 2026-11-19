//! Native SDK transport for the explicit partitioned record format.
//! Allocators survive value deletion and TTL. The format lives in `records_core`.

#![expect(
    clippy::arbitrary_source_item_ordering,
    reason = "keep preparation, the SDK transport and its conversions grouped"
)]

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use aws_sdk_dynamodb::Client;
use aws_sdk_dynamodb::config::Builder;
use aws_sdk_dynamodb::config::retry::RetryConfig;
use aws_sdk_dynamodb::error::{ProvideErrorMetadata, SdkError};
use aws_sdk_dynamodb::primitives::Blob;
use aws_sdk_dynamodb::types::{AttributeValue, ReturnValue, TableDescription};
#[cfg(test)]
use edgezero_core::key_value_store::KvError;
use edgezero_core::key_value_store::KvHandle;
use edgezero_core::record_store::RecordLayout;
use edgezero_core::time::{Deadline, MonotonicClock};
use tokio::time::timeout;

use super::{DynamoDbStore, timeout_policy};
use crate::PreparationError;
use crate::preparation::AwsPreparation;
use crate::records_core::{
    Attr, Item, Operation, RecordCore, RecordScope, RecordTransport, Request, Response,
    TransportError, record_binding, record_partition_base,
};
#[cfg(test)]
use crate::records_core::{FORMAT, invalid_input, physical_key};
use crate::settings::{
    DynamoDbKvLimits, DynamoDbKvSettings, DynamoDbPinnedIdentity, DynamoDbRecordIdentity,
    DynamoDbRecordsSettings,
};

pub(super) use crate::settings::endpoint_identity;

type DynamoDbRecordStore = RecordCore<NativeTransport>;

/// The SDK client and fixed operation policy for one table.
pub(crate) struct NativeTransport {
    client: Client,
    clock: MonotonicClock,
    limits: DynamoDbKvLimits,
    table: String,
}

/// Native preparation output bound into record scopes.
pub(crate) struct PreparedRecordIdentity {
    pub endpoint: String,
    pub table_arn: Option<String>,
    pub table_id: Option<String>,
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
        self.prepare_dynamodb_records_pinned(settings, layout, None)
            .await
    }

    /// Prepares like [`Self::prepare_dynamodb_records`], then requires the observed
    /// table identity to equal `pinned` when one is supplied.
    ///
    /// # Errors
    /// Returns [`PreparationError::PinnedIdentityMismatch`] when the table ARN or table
    /// ID differs, and any preparation failure otherwise.
    #[inline]
    pub async fn prepare_dynamodb_records_pinned(
        &mut self,
        settings: &DynamoDbKvSettings,
        layout: &RecordLayout,
        pinned: Option<&DynamoDbPinnedIdentity>,
    ) -> Result<KvHandle, PreparationError> {
        let partition_base = record_partition_base(settings, layout)?;
        let inner = self.prepare_dynamodb_store(settings, None).await?;
        if let Some(expected) = pinned {
            let identity = observed_identity(&inner)?;
            if identity.pinned != *expected {
                return Err(PreparationError::PinnedIdentityMismatch);
            }
        }
        Ok(KvHandle::new(Arc::new(DynamoDbRecordStore::new(
            inner,
            layout.clone(),
            partition_base,
        )?)))
    }

    /// Offline preflight for a portable binding. Describes the table through the
    /// binding's own endpoint, with the SDK credential chain, and returns the identity
    /// to pin. It rejects every table native record preparation rejects and writes nothing.
    ///
    /// # Errors
    /// Returns [`PreparationError::InvalidEndpoint`] when the endpoint the SDK used
    /// differs from the one a portable transport would use, and any preparation failure.
    #[inline]
    pub async fn preflight_dynamodb_records(
        &mut self,
        settings: &DynamoDbRecordsSettings,
    ) -> Result<DynamoDbRecordIdentity, PreparationError> {
        let endpoint = settings.portable_endpoint()?;
        let address = settings.kv_settings(DynamoDbKvLimits::default());
        let inner = self
            .prepare_dynamodb_store(&address, Some(&endpoint))
            .await?;
        let identity = observed_identity(&inner)?;
        if identity.endpoint != endpoint {
            return Err(PreparationError::InvalidEndpoint);
        }
        Ok(identity)
    }
}

fn observed_identity(inner: &DynamoDbStore) -> Result<DynamoDbRecordIdentity, PreparationError> {
    let identity = inner
        .record_identity
        .as_ref()
        .ok_or(PreparationError::Malformed)?;
    let table_arn = identity
        .table_arn
        .clone()
        .filter(|arn| !arn.is_empty())
        .ok_or(PreparationError::Malformed)?;
    Ok(DynamoDbRecordIdentity {
        endpoint: identity.endpoint.clone(),
        pinned: DynamoDbPinnedIdentity {
            table_arn,
            table_id: identity.table_id.clone(),
        },
    })
}

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
        let binding = record_binding(
            inner.limits,
            layout,
            &RecordScope {
                endpoint: &identity.endpoint,
                namespace: &inner.namespace,
                region: &inner.region,
                table: &inner.table,
                table_arn,
                table_id: identity.table_id.as_deref(),
            },
        )?;
        Ok(Self {
            binding,
            clock: inner.clock.clone(),
            limits: inner.limits,
            partition_base,
            transport: NativeTransport {
                client: inner.client,
                clock: inner.clock,
                limits: inner.limits,
                table: inner.table,
            },
        })
    }
}

impl NativeTransport {
    fn operation_policy(&self, deadline: Deadline) -> Result<Builder, TransportError> {
        let budget = deadline
            .remaining_at(self.clock.now())
            .filter(|remaining| !remaining.is_zero())
            .ok_or(TransportError::NotDispatched)?;
        Ok(aws_sdk_dynamodb::Config::builder()
            .retry_config(RetryConfig::standard().with_max_attempts(1))
            .timeout_config(timeout_policy(self.limits, budget)))
    }

    async fn send(
        &self,
        operation: Operation,
        request: Request,
        policy: Builder,
    ) -> Result<Response, TransportError> {
        match operation {
            Operation::Query | Operation::Scan => self.send_page(operation, request, policy).await,
            Operation::DeleteItem | Operation::GetItem | Operation::PutItem => {
                self.send_point(operation, request, policy).await
            }
            Operation::UpdateItem => {
                let (names, values) = expression_maps(&request);
                let output = self
                    .client
                    .update_item()
                    .table_name(&self.table)
                    .set_key(request.key.map(to_sdk_item))
                    .set_update_expression(request.update_expression.map(str::to_owned))
                    .set_condition_expression(request.condition_expression.map(str::to_owned))
                    .set_expression_attribute_names(names)
                    .set_expression_attribute_values(values)
                    .set_return_values(
                        request
                            .return_updated_new
                            .then_some(ReturnValue::UpdatedNew),
                    )
                    .customize()
                    .config_override(policy)
                    .send()
                    .await
                    .map_err(|error| transport_error(&error))?;
                Ok(Response {
                    attributes: output.attributes.map(from_sdk_item),
                    ..Response::default()
                })
            }
        }
    }

    async fn send_point(
        &self,
        operation: Operation,
        request: Request,
        policy: Builder,
    ) -> Result<Response, TransportError> {
        let (names, values) = expression_maps(&request);
        if operation == Operation::GetItem {
            let output = self
                .client
                .get_item()
                .table_name(&self.table)
                .set_key(request.key.map(to_sdk_item))
                .set_consistent_read(request.consistent_read)
                .customize()
                .config_override(policy)
                .send()
                .await
                .map_err(|error| transport_error(&error))?;
            return Ok(Response {
                item: output.item.map(from_sdk_item),
                ..Response::default()
            });
        }
        if operation == Operation::PutItem {
            self.client
                .put_item()
                .table_name(&self.table)
                .set_item(request.item.map(to_sdk_item))
                .set_condition_expression(request.condition_expression.map(str::to_owned))
                .set_expression_attribute_names(names)
                .set_expression_attribute_values(values)
                .customize()
                .config_override(policy)
                .send()
                .await
                .map_err(|error| transport_error(&error))?;
            return Ok(Response::default());
        }
        self.client
            .delete_item()
            .table_name(&self.table)
            .set_key(request.key.map(to_sdk_item))
            .customize()
            .config_override(policy)
            .send()
            .await
            .map_err(|error| transport_error(&error))?;
        Ok(Response::default())
    }

    async fn send_page(
        &self,
        operation: Operation,
        request: Request,
        policy: Builder,
    ) -> Result<Response, TransportError> {
        let (names, values) = expression_maps(&request);
        if operation == Operation::Query {
            let output = self
                .client
                .query()
                .table_name(&self.table)
                .set_consistent_read(request.consistent_read)
                .set_scan_index_forward(request.scan_index_forward)
                .set_limit(request.limit)
                .set_key_condition_expression(request.key_condition_expression.map(str::to_owned))
                .set_filter_expression(request.filter_expression.map(str::to_owned))
                .set_projection_expression(request.projection_expression.map(str::to_owned))
                .set_expression_attribute_names(names)
                .set_expression_attribute_values(values)
                .set_exclusive_start_key(request.exclusive_start_key.map(to_sdk_item))
                .customize()
                .config_override(policy)
                .send()
                .await
                .map_err(|error| transport_error(&error))?;
            return Ok(page_response(output.items, output.last_evaluated_key));
        }
        let output = self
            .client
            .scan()
            .table_name(&self.table)
            .set_limit(request.limit)
            .set_filter_expression(request.filter_expression.map(str::to_owned))
            .set_projection_expression(request.projection_expression.map(str::to_owned))
            .set_expression_attribute_names(names)
            .set_expression_attribute_values(values)
            .set_exclusive_start_key(request.exclusive_start_key.map(to_sdk_item))
            .customize()
            .config_override(policy)
            .send()
            .await
            .map_err(|error| transport_error(&error))?;
        Ok(page_response(output.items, output.last_evaluated_key))
    }
}

#[async_trait(?Send)]
impl RecordTransport for NativeTransport {
    async fn call(
        &self,
        operation: Operation,
        request: Request,
        deadline: Deadline,
    ) -> Result<Response, TransportError> {
        // Resolve the remaining policy before constructing the request future.
        let policy = self.operation_policy(deadline)?;
        let remaining = deadline
            .remaining_at(self.clock.now())
            .filter(|budget| !budget.is_zero())
            .ok_or(TransportError::NotDispatched)?;
        timeout(remaining, self.send(operation, request, policy))
            .await
            .map_err(|_elapsed| TransportError::Uncertain)?
    }
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

type SdkNames = Option<HashMap<String, String>>;
type SdkValues = Option<HashMap<String, AttributeValue>>;

/// The SDK omits an expression map when it is empty; so does the JSON transport.
fn expression_maps(request: &Request) -> (SdkNames, SdkValues) {
    let names = (!request.names.is_empty()).then(|| {
        request
            .names
            .iter()
            .map(|(alias, name)| (alias.clone(), name.clone()))
            .collect()
    });
    let values = (!request.values.is_empty()).then(|| to_sdk_item(request.values.clone()));
    (names, values)
}

fn page_response(
    items: Option<Vec<HashMap<String, AttributeValue>>>,
    last_evaluated_key: Option<HashMap<String, AttributeValue>>,
) -> Response {
    Response {
        items: items
            .unwrap_or_default()
            .into_iter()
            .map(from_sdk_item)
            .collect(),
        last_evaluated_key: last_evaluated_key.map(from_sdk_item),
        ..Response::default()
    }
}

/// The SDK's view of one attempt. Only a typed service error is a service reply.
fn transport_error<E>(error: &SdkError<E>) -> TransportError
where
    E: ProvideErrorMetadata,
{
    if matches!(error, SdkError::ConstructionFailure(_)) {
        return TransportError::NotDispatched;
    }
    error
        .as_service_error()
        .map_or(TransportError::Uncertain, |service| {
            TransportError::Service {
                conditional: service.code() == Some("ConditionalCheckFailedException"),
                definitive: error
                    .raw_response()
                    .is_some_and(|response| (400..500).contains(&response.status().as_u16())),
            }
        })
}

fn to_sdk_item(item: Item) -> HashMap<String, AttributeValue> {
    item.into_iter()
        .map(|(name, value)| (name, to_sdk_value(value)))
        .collect()
}

fn to_sdk_value(value: Attr) -> AttributeValue {
    match value {
        Attr::Binary(bytes) => AttributeValue::B(Blob::new(bytes)),
        Attr::Number(number) => AttributeValue::N(number),
        Attr::Text(text) => AttributeValue::S(text),
        // The core never sends an opaque value. NULL is the inert placeholder if it did.
        Attr::Other => AttributeValue::Null(true),
    }
}

#[expect(
    clippy::wildcard_enum_match_arm,
    reason = "every non-scalar SDK attribute type is opaque to the record format"
)]
fn from_sdk_item(item: HashMap<String, AttributeValue>) -> Item {
    item.into_iter()
        .map(|(name, value)| {
            let converted = match value {
                AttributeValue::B(blob) => Attr::Binary(blob.into_inner()),
                AttributeValue::N(number) => Attr::Number(number),
                AttributeValue::S(text) => Attr::Text(text),
                _ => Attr::Other,
            };
            (name, converted)
        })
        .collect()
}

#[cfg(test)]
#[path = "dynamodb_records_tests.rs"]
mod tests;
