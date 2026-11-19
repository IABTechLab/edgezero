//! Portable celld record provider over `EdgeZero` outbound HTTP.
//!
//! Only authenticated HTTPS access is supported. The bearer token comes from a
//! logical secrets store, the store instance from offline preflight. Request and
//! response policy is the native client's, in [`crate::wire`]; no request is retried.

#![expect(
    clippy::arbitrary_source_item_ordering,
    reason = "keep the prepared state, its request-local exchanger and error mapping in lifecycle order"
)]

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use edgezero_core::error::EdgeError;
use edgezero_core::error_detail::ProviderFailurePhase;
use edgezero_core::http::{HeaderValue, Method, Uri};
use edgezero_core::key_value_store::KvStore;
use edgezero_core::outbound::{HttpClient, OutboundRequest, ResponseDecoding};
use edgezero_core::record_store::{RecordBinding, RecordLayout};
use edgezero_core::store_registry::BoundSecretStore;
use edgezero_core::time::{Deadline, MonotonicClock};
use futures_util::lock::Mutex;
use url::Url;

use crate::PreparationError;
use crate::records::{CelldRecordKvStore, instance_bytes, record_binding};
use crate::settings::{
    CelldAccessToken, CelldPreparationLimits, CelldRecordsSettings, celld_token_key,
};
use crate::wire::{self, ERROR_CAP, Exchange, Exchanger, Head, ResponseKind};

struct Shared {
    endpoint: Url,
    instance: [u8; 22],
    limits: CelldPreparationLimits,
    secrets: BoundSecretStore,
    store: String,
    token_key: String,
}

/// Validated settings, pinned instance and secrets reference for one KV ID.
///
/// Building it performs no I/O and reads no secret. It is `Send + Sync` and is
/// kept for the life of the application; [`Self::bind`] is cheap and per request.
pub struct PreparedCelldRecords {
    binding: RecordBinding,
    shared: Arc<Shared>,
}

impl fmt::Debug for PreparedCelldRecords {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PreparedCelldRecords")
            .finish_non_exhaustive()
    }
}

impl PreparedCelldRecords {
    /// Builds the record binding from configuration alone.
    ///
    /// `secrets` must be the bound logical store named by the binding's `token_secret`.
    ///
    /// # Errors
    /// Rejects anything but authenticated HTTPS with a `token_secret`, a missing or
    /// malformed pinned instance, and a KV ID outside `[a-z0-9_]`.
    #[inline]
    pub fn new(
        kv_id: &str,
        settings: &CelldRecordsSettings,
        layout: &RecordLayout,
        secrets: BoundSecretStore,
    ) -> Result<Self, PreparationError> {
        if !valid_portable_id(kv_id) {
            return Err(PreparationError::InvalidBinding);
        }
        let endpoint = settings.portable_endpoint()?;
        let pinned = settings
            .pinned
            .as_ref()
            .ok_or(PreparationError::InvalidBinding)?;
        let instance = instance_bytes(&pinned.instance)?;
        Ok(Self {
            binding: record_binding(&endpoint, &settings.store, &pinned.instance, layout)?,
            shared: Arc::new(Shared {
                endpoint,
                instance,
                limits: CelldPreparationLimits::PORTABLE,
                secrets,
                store: settings.store.clone(),
                token_key: celld_token_key(kv_id),
            }),
        })
    }

    /// The configured record binding. No I/O.
    #[inline]
    #[must_use]
    pub const fn record_binding(&self) -> &RecordBinding {
        &self.binding
    }

    /// A request-bound store over `client`. The token is read on its first
    /// operation and reused only by this store.
    #[inline]
    #[must_use]
    pub fn bind(&self, client: HttpClient) -> impl KvStore + use<> {
        let clock = client.time().clock().clone();
        CelldRecordKvStore::pinned(
            Arc::new(PortableExchanger {
                authorization: Mutex::new(None),
                client,
                clock,
                shared: Arc::clone(&self.shared),
            }),
            self.binding.clone(),
            self.shared.instance,
            self.shared.limits,
            self.shared.store.clone(),
        )
    }
}

/// One request's exchanger. The bearer header cache lives and dies with it.
struct PortableExchanger {
    authorization: Mutex<Option<HeaderValue>>,
    client: HttpClient,
    clock: MonotonicClock,
    shared: Arc<Shared>,
}

impl PortableExchanger {
    async fn authorization(&self) -> Result<HeaderValue, PreparationError> {
        let mut cached = self.authorization.lock().await;
        if let Some(header) = cached.as_ref() {
            return Ok(header.clone());
        }
        let bytes = self
            .shared
            .secrets
            .require_bytes(&self.shared.token_key)
            .await
            .map_err(|_error| PreparationError::Credentials)?;
        let text =
            String::from_utf8(bytes.to_vec()).map_err(|_error| PreparationError::Credentials)?;
        let header = wire::authorization(&CelldAccessToken::new(text.trim().to_owned())?)?;
        *cached = Some(header.clone());
        Ok(header)
    }

    fn request(
        &self,
        method: Method,
        url: &Url,
        body: Option<Bytes>,
        cap: usize,
        deadline: Deadline,
    ) -> Result<OutboundRequest, PreparationError> {
        let uri: Uri = url
            .as_str()
            .parse()
            .map_err(|_error| PreparationError::InvalidEndpoint)?;
        let response_cap =
            u64::try_from(cap.max(ERROR_CAP)).map_err(|_error| PreparationError::SizeLimit)?;
        let mut request = OutboundRequest::new(method, uri)
            .map_err(|_error| PreparationError::InvalidEndpoint)?
            .deadline(deadline)
            .connect_timeout(Duration::from_millis(self.shared.limits.connect_timeout_ms))
            .response_decoding(ResponseDecoding::Original)
            .max_response_bytes(response_cap);
        if let Some(payload) = body {
            request = request.body(payload);
        }
        Ok(request)
    }
}

#[expect(
    clippy::arbitrary_source_item_ordering,
    reason = "keep deadlines ahead of the request lifecycle, as in the native client"
)]
#[async_trait(?Send)]
impl Exchanger for PortableExchanger {
    fn deadline(&self) -> Result<Deadline, PreparationError> {
        self.clock
            .now()
            .checked_add(Duration::from_millis(
                self.shared.limits.operation_timeout_ms,
            ))
            .map(Deadline::at_instant)
            .ok_or(PreparationError::Deadline)
    }

    fn check(&self, deadline: Deadline) -> Result<Duration, PreparationError> {
        deadline
            .remaining_at(self.clock.now())
            .ok_or(PreparationError::Deadline)
    }

    fn url(&self, store: &str, resource: &str) -> Result<Url, PreparationError> {
        wire::store_url(&self.shared.endpoint, store, resource)
    }

    async fn execute(
        &self,
        method: Method,
        url: Url,
        body: Option<Bytes>,
        cap: usize,
        deadline: Deadline,
        kind: ResponseKind,
    ) -> Result<Exchange, PreparationError> {
        // Every failure before the request is handed to the client is definitive.
        self.check(deadline)?;
        let authorization = self.authorization().await?;
        let headers = wire::request_headers(kind, Some(&authorization), body.is_some())?;
        let mut request = self.request(method.clone(), &url, body, cap, deadline)?;
        request.headers_mut().extend(headers);
        let response = match self.client.send(request).await {
            Ok(response) => response,
            Err(error) if not_dispatched(&error) => return Err(PreparationError::Unavailable),
            Err(error) => return wire::finish(kind, Err(transport_error(&error)), false),
        };
        let status = response.status();
        let mut definitive_rejection = false;
        let result = match wire::head(kind, &method, status, response.headers(), cap) {
            Ok(Head::Complete(exchange)) => Ok(exchange),
            Ok(Head::Body {
                cap: body_cap,
                instance,
                success,
            }) => match u64::try_from(body_cap) {
                Ok(limit) => match response.into_bytes_bounded_until(limit, deadline).await {
                    Ok(bytes) => wire::body(
                        kind,
                        status,
                        instance,
                        success,
                        bytes.to_vec(),
                        &mut definitive_rejection,
                    ),
                    Err(error) => Err(transport_error(&error)),
                },
                Err(_overflow) => Err(PreparationError::SizeLimit),
            },
            Err(error) => Err(error),
        };
        let checked = self.check(deadline).and_then(|_remaining| result);
        wire::finish(kind, checked, definitive_rejection)
    }
}

/// Facade validation and pre-dispatch adapter phases never reached the network.
fn not_dispatched(error: &EdgeError) -> bool {
    matches!(
        error,
        EdgeError::Validation { .. } | EdgeError::BadRequest { .. }
    ) || error.provider_failure_detail().is_some_and(|detail| {
        matches!(
            detail.phase(),
            ProviderFailurePhase::BackendRegistration | ProviderFailurePhase::RequestPreparation
        )
    })
}

fn transport_error(error: &EdgeError) -> PreparationError {
    if matches!(error, EdgeError::GatewayTimeout { .. }) {
        PreparationError::Deadline
    } else if matches!(error, EdgeError::ResponseTooLarge { .. }) {
        PreparationError::SizeLimit
    } else {
        PreparationError::Unavailable
    }
}

/// Lowercase ASCII, digits and underscore, so uppercased key names cannot collide.
fn valid_portable_id(id: &str) -> bool {
    !id.is_empty()
        && id
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
}

#[cfg(test)]
#[cfg(all(not(target_arch = "wasm32"), feature = "records"))]
#[path = "portable_tests.rs"]
mod tests;
