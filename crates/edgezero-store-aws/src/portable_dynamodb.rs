//! Portable `DynamoDB` record provider over `EdgeZero` outbound HTTP.
//!
//! Requests use the `DynamoDB` JSON 1.0 protocol and are signed with `SigV4` from
//! credentials in a logical secrets store. There is no AWS SDK, credential chain,
//! endpoint discovery or retry. Rows, cursors and outcomes are the native record format.

#![expect(
    clippy::arbitrary_source_item_ordering,
    reason = "keep the prepared state, its request-local transport and the wire codec in lifecycle order"
)]

use std::fmt;
use std::str;
use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH as StdEpoch};

use async_trait::async_trait;
use aws_credential_types::Credentials;
use aws_sigv4::http_request::{SignableBody, SignableRequest, SigningSettings, sign};
use aws_sigv4::sign::v4;
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use edgezero_core::error::EdgeError;
use edgezero_core::error_detail::ProviderFailurePhase;
use edgezero_core::http::header::{ACCEPT_ENCODING, CONTENT_TYPE};
use edgezero_core::http::{HeaderName, HeaderValue, Method, StatusCode, Uri};
use edgezero_core::key_value_store::KvStore;
use edgezero_core::outbound::{HttpClient, OutboundRequest, ResponseDecoding};
use edgezero_core::record_store::{RecordBinding, RecordLayout};
use edgezero_core::store_registry::BoundSecretStore;
use edgezero_core::time::Deadline;
use futures_util::lock::Mutex;
use serde_json::{Map, Value};
use web_time::{SystemTime, UNIX_EPOCH};

use crate::PreparationError;
use crate::records_core::{
    Attr, Item, Operation, RecordCore, RecordScope, RecordTransport, Request, Response,
    TransportError, record_binding, record_partition_base,
};
use crate::settings::{DynamoDbKvLimits, DynamoDbRecordsSettings};

const CONTENT: &str = "application/x-amz-json-1.0";
/// A Query or Scan page reads at most 1 MB; JSON and base64 roughly double it.
const MAX_RESPONSE_BYTES: u64 = 0x0040_0000;
const MAX_ERROR_BYTES: u64 = 0x1_0000;

/// The secret key names a `DynamoDB` binding reads for KV ID `id`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DynamoDbCredentialKeys {
    pub access_key_id: String,
    pub secret_access_key: String,
    pub session_token: String,
}

impl DynamoDbCredentialKeys {
    /// `KV__<ID>__AWS_ACCESS_KEY_ID`, `..._AWS_SECRET_ACCESS_KEY` and optional
    /// `..._AWS_SESSION_TOKEN`. Bindings cannot name other keys.
    #[inline]
    #[must_use]
    pub fn for_kv_id(id: &str) -> Self {
        let prefix = format!("KV__{}__", id.to_ascii_uppercase());
        Self {
            access_key_id: format!("{prefix}AWS_ACCESS_KEY_ID"),
            secret_access_key: format!("{prefix}AWS_SECRET_ACCESS_KEY"),
            session_token: format!("{prefix}AWS_SESSION_TOKEN"),
        }
    }
}

struct Shared {
    endpoint: Uri,
    keys: DynamoDbCredentialKeys,
    limits: DynamoDbKvLimits,
    region: String,
    secrets: BoundSecretStore,
    table: String,
}

/// Validated settings, pinned identity and secrets reference for one KV ID.
///
/// Building it performs no I/O and reads no secret. It is `Send + Sync` and is
/// kept for the life of the application; [`Self::bind`] is cheap and per request.
pub struct PreparedDynamoDbRecords {
    binding: RecordBinding,
    partition_base: String,
    shared: Arc<Shared>,
}

impl fmt::Debug for PreparedDynamoDbRecords {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PreparedDynamoDbRecords")
            .finish_non_exhaustive()
    }
}

impl PreparedDynamoDbRecords {
    /// Builds the record binding from configuration alone.
    ///
    /// `secrets` must be the bound logical store named by the binding's `credentials`.
    ///
    /// # Errors
    /// Rejects a binding without `pinned` or `credentials`, invalid settings or
    /// endpoint, and a KV ID outside `[a-z0-9_]`.
    #[inline]
    pub fn new(
        kv_id: &str,
        settings: &DynamoDbRecordsSettings,
        layout: &RecordLayout,
        secrets: BoundSecretStore,
    ) -> Result<Self, PreparationError> {
        if !valid_portable_id(kv_id) {
            return Err(PreparationError::InvalidBinding);
        }
        settings.validate(DynamoDbKvLimits::PORTABLE)?;
        let pinned = settings
            .pinned
            .as_ref()
            .ok_or(PreparationError::InvalidBinding)?;
        if settings.credentials.is_none() {
            return Err(PreparationError::InvalidBinding);
        }
        let address = settings.kv_settings(DynamoDbKvLimits::PORTABLE);
        let endpoint = settings.portable_endpoint()?;
        let binding = record_binding(
            address.limits,
            layout.clone(),
            &RecordScope {
                endpoint: &endpoint,
                namespace: &address.namespace,
                region: &address.region,
                table: &address.table,
                table_arn: &pinned.table_arn,
                table_id: pinned.table_id.as_deref(),
            },
        )?;
        Ok(Self {
            binding,
            partition_base: record_partition_base(&address, layout)?,
            shared: Arc::new(Shared {
                endpoint: endpoint
                    .parse()
                    .map_err(|_error| PreparationError::InvalidEndpoint)?,
                keys: DynamoDbCredentialKeys::for_kv_id(kv_id),
                limits: address.limits,
                region: address.region,
                secrets,
                table: address.table,
            }),
        })
    }

    /// The configured record binding. No I/O.
    #[inline]
    #[must_use]
    pub const fn record_binding(&self) -> &RecordBinding {
        &self.binding
    }

    /// A request-bound store over `client`. Credentials are read on its first
    /// operation and reused only by this store.
    #[inline]
    #[must_use]
    pub fn bind(&self, client: HttpClient) -> impl KvStore + use<> {
        let clock = client.time().clock().clone();
        RecordCore {
            binding: self.binding.clone(),
            clock,
            limits: self.shared.limits,
            partition_base: self.partition_base.clone(),
            transport: PortableTransport {
                client,
                credentials: Mutex::new(None),
                shared: Arc::clone(&self.shared),
            },
        }
    }
}

/// One request's transport. The credential cache lives and dies with it.
struct PortableTransport {
    client: HttpClient,
    credentials: Mutex<Option<Credentials>>,
    shared: Arc<Shared>,
}

impl PortableTransport {
    async fn credentials(&self) -> Result<Credentials, TransportError> {
        let mut cached = self.credentials.lock().await;
        if let Some(credentials) = cached.as_ref() {
            return Ok(credentials.clone());
        }
        let keys = &self.shared.keys;
        let access = self.secret(&keys.access_key_id).await?;
        let secret = self.secret(&keys.secret_access_key).await?;
        let session = self
            .shared
            .secrets
            .get_bytes(&keys.session_token)
            .await
            .map_err(|_error| TransportError::NotDispatched)?
            .map(|bytes| secret_text(&bytes))
            .transpose()?;
        let credentials = Credentials::new(access, secret, session, None, "edgezero-secrets");
        *cached = Some(credentials.clone());
        Ok(credentials)
    }

    async fn secret(&self, key: &str) -> Result<String, TransportError> {
        let bytes = self
            .shared
            .secrets
            .require_bytes(key)
            .await
            .map_err(|_error| TransportError::NotDispatched)?;
        secret_text(&bytes)
    }

    fn signed_request(
        &self,
        operation: Operation,
        body: Vec<u8>,
        credentials: Credentials,
        deadline: Deadline,
    ) -> Result<OutboundRequest, TransportError> {
        let target = format!("DynamoDB_20120810.{}", operation.target());
        let uri = self.shared.endpoint.to_string();
        let identity = credentials.into();
        let since_epoch = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_error| TransportError::NotDispatched)?;
        let params = v4::SigningParams::builder()
            .identity(&identity)
            .region(&self.shared.region)
            .name("dynamodb")
            .time(
                StdEpoch
                    .checked_add(since_epoch)
                    .ok_or(TransportError::NotDispatched)?,
            )
            .settings(SigningSettings::default())
            .build()
            .map_err(|_error| TransportError::NotDispatched)?
            .into();
        let signable = SignableRequest::new(
            "POST",
            &uri,
            [("content-type", CONTENT), ("x-amz-target", target.as_str())].into_iter(),
            SignableBody::Bytes(&body),
        )
        .map_err(|_error| TransportError::NotDispatched)?;
        let (instructions, _signature) = sign(signable, &params)
            .map_err(|_error| TransportError::NotDispatched)?
            .into_parts();
        let mut request = OutboundRequest::new(Method::POST, self.shared.endpoint.clone())
            .map_err(|_error| TransportError::NotDispatched)?
            .body(body)
            .deadline(deadline)
            .connect_timeout(Duration::from_millis(self.shared.limits.connect_timeout_ms))
            .response_decoding(ResponseDecoding::Original)
            .max_response_bytes(MAX_RESPONSE_BYTES);
        let headers = request.headers_mut();
        headers.insert(CONTENT_TYPE, HeaderValue::from_static(CONTENT));
        headers.insert(ACCEPT_ENCODING, HeaderValue::from_static("identity"));
        headers.insert(
            HeaderName::from_static("x-amz-target"),
            HeaderValue::from_str(&target).map_err(|_error| TransportError::NotDispatched)?,
        );
        for (name, value) in instructions.headers() {
            let mut header =
                HeaderValue::from_str(value).map_err(|_error| TransportError::NotDispatched)?;
            header.set_sensitive(true);
            headers.insert(
                HeaderName::from_bytes(name.as_bytes())
                    .map_err(|_error| TransportError::NotDispatched)?,
                header,
            );
        }
        Ok(request)
    }
}

#[async_trait(?Send)]
impl RecordTransport for PortableTransport {
    async fn call(
        &self,
        operation: Operation,
        request: Request,
        deadline: Deadline,
    ) -> Result<Response, TransportError> {
        let credentials = self.credentials().await?;
        let body = serde_json::to_vec(&encode_request(&self.shared.table, request))
            .map_err(|_error| TransportError::NotDispatched)?;
        let outbound = self.signed_request(operation, body, credentials, deadline)?;
        let response = self
            .client
            .send(outbound)
            .await
            .map_err(|error| send_error(&error))?;
        let status = response.status();
        if status == StatusCode::OK {
            let bytes = response
                .into_bytes_bounded_until(MAX_RESPONSE_BYTES, deadline)
                .await
                .map_err(|_error| TransportError::Uncertain)?;
            return decode_response(&bytes).ok_or(TransportError::Uncertain);
        }
        let definitive = status.is_client_error();
        let conditional = response
            .into_bytes_bounded_until(MAX_ERROR_BYTES, deadline)
            .await
            .is_ok_and(|bytes| is_conditional_failure(&bytes));
        Err(TransportError::Service {
            conditional,
            definitive,
        })
    }
}

/// Facade validation and pre-dispatch adapter phases never reached the network.
fn send_error(error: &EdgeError) -> TransportError {
    if matches!(
        error,
        EdgeError::Validation { .. } | EdgeError::BadRequest { .. }
    ) || error.provider_failure_detail().is_some_and(|detail| {
        matches!(
            detail.phase(),
            ProviderFailurePhase::BackendRegistration | ProviderFailurePhase::RequestPreparation
        )
    }) {
        TransportError::NotDispatched
    } else {
        TransportError::Uncertain
    }
}

fn secret_text(bytes: &[u8]) -> Result<String, TransportError> {
    str::from_utf8(bytes)
        .ok()
        .map(str::trim)
        .filter(|text| !text.is_empty() && text.bytes().all(|byte| byte.is_ascii_graphic()))
        .map(str::to_owned)
        .ok_or(TransportError::NotDispatched)
}

/// Lowercase ASCII, digits and underscore, so uppercased key names cannot collide.
pub(crate) fn valid_portable_id(id: &str) -> bool {
    !id.is_empty()
        && id
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
}

fn is_conditional_failure(bytes: &[u8]) -> bool {
    serde_json::from_slice::<Value>(bytes)
        .ok()
        .and_then(|value| value.get("__type")?.as_str().map(str::to_owned))
        .is_some_and(|kind| kind.rsplit('#').next() == Some("ConditionalCheckFailedException"))
}

fn encode_request(table: &str, request: Request) -> Value {
    let mut body = Map::new();
    body.insert("TableName".to_owned(), Value::from(table));
    let mut text = |name: &str, value: Option<&'static str>| {
        if let Some(expression) = value {
            body.insert(name.to_owned(), Value::from(expression));
        }
    };
    text("UpdateExpression", request.update_expression);
    text("ConditionExpression", request.condition_expression);
    text("KeyConditionExpression", request.key_condition_expression);
    text("FilterExpression", request.filter_expression);
    text("ProjectionExpression", request.projection_expression);
    if let Some(key) = request.key {
        body.insert("Key".to_owned(), encode_item(key));
    }
    if let Some(item) = request.item {
        body.insert("Item".to_owned(), encode_item(item));
    }
    if let Some(start) = request.exclusive_start_key {
        body.insert("ExclusiveStartKey".to_owned(), encode_item(start));
    }
    if let Some(consistent) = request.consistent_read {
        body.insert("ConsistentRead".to_owned(), Value::from(consistent));
    }
    if let Some(forward) = request.scan_index_forward {
        body.insert("ScanIndexForward".to_owned(), Value::from(forward));
    }
    if let Some(limit) = request.limit {
        body.insert("Limit".to_owned(), Value::from(limit));
    }
    if request.return_updated_new {
        body.insert("ReturnValues".to_owned(), Value::from("UPDATED_NEW"));
    }
    if !request.names.is_empty() {
        body.insert(
            "ExpressionAttributeNames".to_owned(),
            Value::Object(
                request
                    .names
                    .into_iter()
                    .map(|(alias, name)| (alias, Value::from(name)))
                    .collect(),
            ),
        );
    }
    if !request.values.is_empty() {
        body.insert(
            "ExpressionAttributeValues".to_owned(),
            encode_item(request.values),
        );
    }
    Value::Object(body)
}

fn encode_item(item: Item) -> Value {
    Value::Object(
        item.into_iter()
            .map(|(name, value)| (name, encode_value(value)))
            .collect(),
    )
}

fn encode_value(value: Attr) -> Value {
    let (kind, encoded) = match value {
        Attr::Binary(bytes) => ("B", STANDARD.encode(bytes)),
        Attr::Number(number) => ("N", number),
        Attr::Text(text) => ("S", text),
        // The core never sends an opaque value. NULL is the inert placeholder if it did.
        Attr::Other => return serde_json::json!({ "NULL": true }),
    };
    let mut object = Map::new();
    object.insert(kind.to_owned(), Value::from(encoded));
    Value::Object(object)
}

fn decode_response(bytes: &[u8]) -> Option<Response> {
    let Value::Object(mut body) = serde_json::from_slice::<Value>(bytes).ok()? else {
        return None;
    };
    // `None` is malformed; `Some(None)` is an absent field.
    let item = |fields: &mut Map<String, Value>, name: &str| match fields.remove(name) {
        None => Some(None),
        Some(value) => decode_item(value).map(Some),
    };
    let items = match body.remove("Items") {
        None => Vec::new(),
        Some(Value::Array(values)) => values
            .into_iter()
            .map(decode_item)
            .collect::<Option<Vec<_>>>()?,
        Some(_) => return None,
    };
    Some(Response {
        attributes: item(&mut body, "Attributes")?,
        item: item(&mut body, "Item")?,
        items,
        last_evaluated_key: item(&mut body, "LastEvaluatedKey")?,
    })
}

fn decode_item(value: Value) -> Option<Item> {
    let Value::Object(object) = value else {
        return None;
    };
    object
        .into_iter()
        .map(|(name, attribute)| Some((name, decode_value(attribute)?)))
        .collect()
}

fn decode_value(value: Value) -> Option<Attr> {
    let Value::Object(object) = value else {
        return None;
    };
    let mut entries = object.into_iter();
    let (kind, encoded) = entries.next()?;
    if entries.next().is_some() {
        return None;
    }
    Some(match (kind.as_str(), encoded) {
        ("B", Value::String(text)) => Attr::Binary(STANDARD.decode(text).ok()?),
        ("N", Value::String(text)) => Attr::Number(text),
        ("S", Value::String(text)) => Attr::Text(text),
        ("B" | "N" | "S", _) => return None,
        _ => Attr::Other,
    })
}

#[cfg(test)]
#[path = "portable_dynamodb_tests.rs"]
mod tests;
