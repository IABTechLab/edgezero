#![expect(
    clippy::self_named_module_files,
    reason = "the workspace denies both contradictory module-file conventions; retain its self-named-file convention"
)]

mod batch;
mod dispatch;
mod exchange;
mod head;
mod image_optimizer;
mod progress;
mod redirect;
#[cfg(test)]
mod redirect_tests;
mod scope;
#[cfg(test)]
mod scope_tests;
mod task;

use std::net::IpAddr;
use std::num::NonZeroU64;
use std::rc::Rc;
use std::str;
use std::sync::Arc;
use std::time::Duration;

use async_stream::stream;
use bytes::Bytes;
use futures_util::future::LocalBoxFuture;
use futures_util::{Stream, StreamExt as _};
use serde::Serialize;
use serde::de::DeserializeOwned;
use url::Url;

use crate::body::{Body, BodyPayload, BodyStream};
use crate::compression::{
    ContentEncoding, classify_content_encoding, decode_brotli_stream, decode_deflate_stream,
    decode_gzip_stream,
};
use crate::error::{
    BadGatewayDecodeReason, BadGatewayReason, EdgeError, RequestBodyOverflowOrigin,
    ResponseLimitReason,
};
use crate::http::header::{
    CONNECTION, CONTENT_ENCODING, CONTENT_LENGTH, CONTENT_TYPE, HOST, PROXY_AUTHENTICATE,
    PROXY_AUTHORIZATION, TE, TRAILER, TRANSFER_ENCODING, UPGRADE,
};
use crate::http::{
    Authority, HeaderMap, HeaderName, HeaderValue, Method, Request, Response, StatusCode, Uri,
    response_builder,
};
use crate::time::{
    DEADLINE_FAR_FUTURE, Deadline, DispatchBudget, MonotonicClock, MonotonicInstant,
};

pub use batch::{
    OutboundBatch, OutboundBatchAccumulator, OutboundBatchCheck, OutboundBatchContext,
    OutboundBatchDriverEvent, OutboundBatchFailure, OutboundBatchInsertFailure, OutboundBatchItem,
    OutboundBatchMergeFailure, OutboundBatchNext, OutboundBatchResults, OutboundBatchTermination,
    OutboundCollectedSlot, OutboundPreparedBatch,
};
pub(crate) use dispatch::ForwardedReadDeadline;
pub use redirect::{
    OutboundFollowFailure, OutboundFollowResult, OutboundRedirectAction, OutboundRedirectContext,
    OutboundRedirectLimit, OutboundRedirectStopReason,
};
pub(crate) use scope::Revocable;
pub use scope::{
    IndependentOutboundProgress, OutboundProgressRuntime, OutboundScope, OutboundScopeKind,
    OutboundScopeOwner,
};
pub use task::OutboundTask;

pub const DEFAULT_MAX_DECODER_BYTES: u64 = 32 * 1024 * 1024;
pub const DEFAULT_MAX_RESPONSE_BYTES: u64 = 1024 * 1024;
pub const DEFAULT_OUTBOUND_REQUEST_BODY_BYTES: u64 = 8 * 1024 * 1024;
/// Response header identifying the adapter that completed an outbound request.
pub const PROXY_HEADER: &str = "x-edgezero-proxy";

/// An owned local outbound operation; neither its future nor its result needs to be `Send`.
pub type OutboundFuture<T> = LocalBoxFuture<'static, Result<T, EdgeError>>;

type OutboundWaitFactory =
    dyn Fn(Deadline, MonotonicClock) -> OutboundFuture<()> + Send + Sync + 'static;

/// An application clock paired with an adapter-owned deadline wakeup factory.
///
/// Clones share the same clock source and factory. There is deliberately no default or inert
/// timer: providers must supply wakeups that work in the application's monotonic time domain.
#[derive(Clone)]
pub struct OutboundTime {
    clock: MonotonicClock,
    wait: Arc<OutboundWaitFactory>,
}

/// Local, exactly-once teardown ownership for an outbound exchange or task group.
///
/// The callback may own `Rc` values and SDK handles. It must perform bounded, nonblocking
/// cleanup. Do not assign the same SDK resource two independent teardown callbacks.
/// When used as pending cancellation, destroying the callback's captures without invoking it
/// must be safe after a successful response handoff; do not capture an armed abort-on-drop guard.
#[must_use]
pub struct OutboundExchangeOwner {
    release: Option<Box<dyn FnOnce()>>,
}

/// A completion receiver and cancellation owner for work that is already scheduled.
///
/// This handle does not schedule transport work. Its producer must issue or schedule the
/// exchange before construction, provide a nonblocking receiver, and complete transport/body
/// ownership transfer before publishing success. A successful streamed response owns its own
/// transport cleanup; pending cancellation only cancels/unregisters scheduled work.
#[must_use]
pub struct OutboundPending {
    ready: bool,
    state: progress::PendingStorage,
}

/// The facade operation whose effective policies were captured before dispatch.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OutboundSendMode {
    Batch,
    Begin,
    Follow,
    Send,
}

/// One request and the immutable policies captured by the shared facade at invocation.
/// Providers inspect or consume this value, never construct or reset it.
#[derive(Debug)]
pub struct OutboundDispatchRequest {
    context: OutboundResponseContext,
    request: OutboundRequest,
}

/// Validated, owned policy and time inputs for one response.
///
/// The facade supplies this context through prepared dispatch. There is no public constructor;
/// fields cannot be reset, cloned or decomposed.
///
/// ```compile_fail
/// use edgezero_core::OutboundResponseContext;
/// fn duplicate(context: OutboundResponseContext) { let _ = context.clone(); }
/// ```
/// ```compile_fail
/// use edgezero_core::OutboundResponseContext;
/// fn reset(context: &mut OutboundResponseContext) { context.entered_at = context.clock().now(); }
/// ```
/// ```compile_fail
/// use edgezero_core::OutboundResponseContext;
/// let _ = OutboundResponseContext::default();
/// ```
#[derive(Debug)]
pub struct OutboundResponseContext {
    body_policy: ResponseBodyPolicy,
    budget: DispatchBudget,
    entered_at: MonotonicInstant,
    max_response_header_bytes: Option<u64>,
    max_response_header_count: Option<u64>,
    mode: OutboundSendMode,
    original_chain_deadline: Option<Deadline>,
    proxy_marker: OutboundProxyMarker,
    request_method: Method,
    scope: OutboundScope,
    time: OutboundTime,
}

/// Whether original field boundaries are available at the adapter boundary.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OutboundHeaderFidelity {
    AdapterVisibleFields,
    ProviderFieldBoundariesUnknown,
}

/// One adapter-visible header map, consumed before normalization and payload processing.
pub struct OutboundHeaderInput {
    adapter: &'static str,
    fidelity: OutboundHeaderFidelity,
    headers: HeaderMap,
}

/// Immutable original Location facts; no raw header value is copied into this evidence.
#[derive(Debug)]
pub struct OutboundLocationEvidence {
    fidelity: OutboundHeaderFidelity,
    has_non_text: bool,
    missing_or_empty: bool,
    observed_count: usize,
    retained_singleton: bool,
}

/// An owned response head whose payload has not been processed.
///
/// Construction validates header syntax and caps, not declared payload limits or compression.
/// Discard never polls the body. Finalization applies ordinary body policy and transfers cleanup
/// to a live streamed response without starting another transport exchange.
#[must_use]
pub struct OutboundHead {
    context: OutboundResponseContext,
    encoded: BodyStream,
    headers: HeaderMap,
    location_evidence: OutboundLocationEvidence,
    normalization: ResponseHeadNormalization,
    owner: OutboundExchangeOwner,
    status: StatusCode,
}

/// Explicit image output format; no format is selected by default.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OutboundImageFormat {
    Auto,
    Avif,
    Gif,
    Jpeg,
    JpegXl,
    Mp4,
    Png,
    WebP,
}

/// Resampling filter for an explicitly configured image transformation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OutboundResizeFilter {
    Bicubic,
    Bilinear,
    Lanczos2,
    Lanczos3,
    Nearest,
}

/// Crop focus with paired percentage coordinates, validated by the options builder.
///
/// ```compile_fail
/// use edgezero_core::OutboundCropFocus;
/// let _ = OutboundCropFocus::Percent { x: 25 };
/// ```
/// ```compile_fail
/// use edgezero_core::OutboundCropFocus;
/// let _ = OutboundCropFocus::Smart { x: 25, y: 75 };
/// ```
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OutboundCropFocus {
    Default,
    Percent { x: u8, y: u8 },
    Smart,
}

/// An Image Optimizer region from the pinned SDK's portable subset.
///
/// ```
/// use edgezero_core::OutboundImageOptimizerRegion;
/// let region = OutboundImageOptimizerRegion::UsWest;
/// assert_eq!(region.as_str(), "us_west");
/// ```
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OutboundImageOptimizerRegion {
    Asia,
    Australia,
    EuCentral,
    EuWest,
    UsCentral,
    UsEast,
    UsWest,
}

/// Validated native transformation settings with no implicit selectors.
///
/// Attach with [`OutboundRequest::image_optimizer`]. Only Fastly Send/Follow hops support these
/// settings; Begin/Batch reject them. The synchronous provider call cannot be interrupted. Query preservation
/// defaults to false. Transformation and performance qualification require hosted evidence.
///
/// ```compile_fail
/// use edgezero_core::OutboundImageOptimizerOptions;
/// let mut options = OutboundImageOptimizerOptions::new();
/// options.quality = Some(255);
/// ```
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct OutboundImageOptimizerOptions {
    crop: Option<(u32, u32, OutboundCropFocus)>,
    format: Option<OutboundImageFormat>,
    height: Option<u32>,
    preserve_origin_query: bool,
    quality: Option<u8>,
    region: Option<OutboundImageOptimizerRegion>,
    resize_filter: Option<OutboundResizeFilter>,
    width: Option<u32>,
}

#[derive(Clone, Copy)]
pub(crate) struct BudgetInputs {
    pub deadline: Option<Deadline>,
    pub timeout: Option<Duration>,
}

#[derive(Debug)]
pub struct OutboundRequest {
    body: Body,
    cache_policy: OutboundCachePolicy,
    connect_timeout: Option<Duration>,
    deadline: Option<Deadline>,
    headers: HeaderMap,
    host_authority_override: Option<Authority>,
    image_optimizer: Option<OutboundImageOptimizerOptions>,
    inbound_read_deadline: Option<Deadline>,
    max_brotli_window_bits: u8,
    max_chunk_bytes: Option<NonZeroU64>,
    max_decoded_response_bytes: Option<u64>,
    max_decoder_bytes: u64,
    max_encoded_response_bytes: Option<u64>,
    max_request_body_bytes: u64,
    max_response_header_bytes: Option<u64>,
    max_response_header_count: Option<u64>,
    method: Method,
    proxy_marker: OutboundProxyMarker,
    response_decoding: ResponseDecoding,
    response_mode: ResponseMode,
    timeout: Option<Duration>,
    uri: Uri,
}

#[derive(Debug)]
#[non_exhaustive]
#[expect(
    clippy::partial_pub_fields,
    reason = "forwarded deadline provenance is immutable across public option round-trips"
)]
pub struct OutboundRequestParts {
    pub body: Body,
    pub cache_policy: OutboundCachePolicy,
    pub connect_timeout: Option<Duration>,
    pub deadline: Option<Deadline>,
    pub headers: HeaderMap,
    pub host_authority_override: Option<Authority>,
    pub image_optimizer: Option<OutboundImageOptimizerOptions>,
    inbound_read_deadline: Option<Deadline>,
    pub max_brotli_window_bits: u8,
    pub max_chunk_bytes: Option<NonZeroU64>,
    pub max_decoded_response_bytes: Option<u64>,
    pub max_decoder_bytes: u64,
    pub max_encoded_response_bytes: Option<u64>,
    pub max_request_body_bytes: u64,
    pub max_response_header_bytes: Option<u64>,
    pub max_response_header_count: Option<u64>,
    pub method: Method,
    pub proxy_marker: OutboundProxyMarker,
    pub response_decoding: ResponseDecoding,
    pub response_mode: ResponseMode,
    pub timeout: Option<Duration>,
    pub uri: Uri,
}

/// Whether recognized response content codings are decoded or delivered unchanged.
///
/// Partial (206) responses always retain their original coding. Identity bodies remain subject
/// to decoded-byte limits under either choice; encoded passthrough skips only that limit.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ResponseDecoding {
    Decode,
    Original,
}

/// Per-request override of the client's synthetic adapter-marker policy.
/// The facade resolves Inherit before handing a prepared request to its implementation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OutboundProxyMarker {
    Inherit,
    Off,
    On,
}

#[derive(Clone, Copy)]
enum PayloadDelivery {
    Decoded,
    Identity,
    Original,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ResponseMode {
    Buffered { max_bytes: u64 },
    Streamed,
}

/// Shared response-body limits and decoding policy applied by the head finalizer.
/// Adapters retain provider reads and transport ownership; core shapes and collects the payload.
#[derive(Clone, Copy, Debug)]
pub struct ResponseBodyPolicy {
    pub max_brotli_window_bits: u8,
    pub max_chunk_bytes: Option<NonZeroU64>,
    pub max_decoded_response_bytes: Option<u64>,
    pub max_decoder_bytes: u64,
    pub max_encoded_response_bytes: Option<u64>,
    pub response_decoding: ResponseDecoding,
    pub response_mode: ResponseMode,
    pub status: StatusCode,
}

/// Controls use of an adapter-managed intermediary cache for one request.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum OutboundCachePolicy {
    Bypass,
    #[default]
    PlatformDefault,
}

#[derive(Clone)]
pub struct HttpClient {
    inner: Arc<dyn OutboundHttpClient>,
    proxy_marker_enabled: bool,
}

/// Provider I/O for requests prepared by the shared facade.
/// Implementations must schedule eager work before returning pending/batch handles.
pub trait OutboundHttpClient: Send + Sync {
    /// Schedules the complete exchange and returns its completion receiver.
    ///
    /// # Errors
    /// Returns setup errors before I/O when the required scheduler is unavailable.
    fn begin(&self, request: OutboundDispatchRequest) -> Result<OutboundPending, EdgeError>;
    fn dispatch_head(&self, request: OutboundDispatchRequest) -> OutboundFuture<OutboundHead>;
    /// Captures provider-local progress without I/O, timers or body polling.
    fn progress(&self) -> Rc<dyn OutboundProgressRuntime>;
    fn start_batch_until(&self, batch: OutboundPreparedBatch) -> OutboundBatch;
    fn time(&self) -> OutboundTime;
    /// Checks provider support without I/O, tasks, timers or body polling.
    ///
    /// # Errors
    /// Returns a typed client error for unsupported options or operation modes.
    fn validate_supported_options(
        &self,
        request: &OutboundRequest,
        mode: OutboundSendMode,
    ) -> Result<(), EdgeError>;
}

#[derive(Debug)]
pub struct OutboundResponse {
    body: Body,
    content_type_provenance: ContentTypeProvenance,
    headers: HeaderMap,
    headers_mutated: bool,
    monotonic_clock: MonotonicClock,
    request_method: Method,
    status: StatusCode,
}

#[derive(Debug)]
#[non_exhaustive]
/// One terminal result produced by an outbound batch slot.
pub struct OutboundSlotResult {
    /// Time from the batch's shared method-entry snapshot until this slot became terminal.
    pub elapsed: Duration,
    /// This slot's terminal response or typed failure.
    pub outcome: Result<OutboundResponse, EdgeError>,
}

impl OutboundSlotResult {
    #[must_use]
    #[inline]
    pub fn new(elapsed: Duration, outcome: Result<OutboundResponse, EdgeError>) -> Self {
        Self { elapsed, outcome }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ResponseBodyDisposition {
    FramingBodyless,
    Payload,
    ResetContent { declared_body: bool },
}

/// Why the original Content-Type cannot certify a retained textual singleton.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ContentTypeIneligibility {
    Missing,
    Modified,
    MultipleValues,
    NonText,
    Removed,
}

/// Original Content-Type evidence, captured before `EdgeZero` filters response headers.
///
/// This does not validate MIME syntax, split commas, trim, or redact the value. A textual
/// singleton can still be invalid MIME. `Unknown` is required when the provider hides original
/// bytes or field boundaries. No raw header value is copied into this metadata.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ContentTypeProvenance {
    Ineligible(ContentTypeIneligibility),
    SingleTextUnchanged,
    Unknown,
}

/// Results of one upstream-header normalization pass, including pre-filter evidence.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ResponseHeadNormalization {
    pub body_disposition: ResponseBodyDisposition,
    pub content_type_provenance: ContentTypeProvenance,
}

pub struct ResponseHeaderLimiter {
    max_bytes: Option<u64>,
    max_count: Option<u64>,
    observed_bytes: u64,
    observed_count: u64,
}

impl ResponseHeaderLimiter {
    #[must_use]
    #[inline]
    pub fn new(max_bytes: Option<u64>, max_count: Option<u64>) -> Self {
        Self {
            max_bytes,
            max_count,
            observed_bytes: 0,
            observed_count: 0,
        }
    }

    /// Adds one adapter-visible upstream field section to the cumulative response-header
    /// budget before normalization.
    ///
    /// # Errors
    /// Returns a typed response limit error on count, byte, or accounting overflow.
    #[inline]
    pub fn observe(&mut self, headers: &HeaderMap) -> Result<(), EdgeError> {
        for (name, value) in headers {
            self.observe_field(name, value)?;
        }
        Ok(())
    }

    fn observe_field(&mut self, name: &HeaderName, value: &HeaderValue) -> Result<(), EdgeError> {
        let next_count = self
            .observed_count
            .checked_add(1)
            .ok_or_else(|| response_limit_error(ResponseLimitReason::HeaderCount))?;
        self.observed_count = next_count;
        if self.max_count.is_some_and(|max| next_count > max) {
            return Err(response_limit_error(ResponseLimitReason::HeaderCount));
        }

        let name_bytes = u64::try_from(name.as_str().len()).unwrap_or(u64::MAX);
        let value_bytes = u64::try_from(value.as_bytes().len()).unwrap_or(u64::MAX);
        let next_bytes = self
            .observed_bytes
            .checked_add(name_bytes)
            .and_then(|total| total.checked_add(value_bytes))
            .ok_or_else(|| response_limit_error(ResponseLimitReason::HeaderBytes))?;
        self.observed_bytes = next_bytes;
        if self.max_bytes.is_some_and(|max| next_bytes > max) {
            return Err(response_limit_error(ResponseLimitReason::HeaderBytes));
        }
        Ok(())
    }
}

impl HttpClient {
    /// Settles permitted GET/HEAD streams, then schedules the complete exchange.
    ///
    /// # Errors
    /// Returns preparation, settlement or provider scheduling errors unchanged.
    #[inline]
    pub fn begin(&self, request: OutboundRequest) -> OutboundFuture<OutboundPending> {
        let (scope, time, entered_at, prepared) =
            match self.prepare(request, OutboundSendMode::Begin, None) {
                Ok(captured) => captured,
                Err(error) => return Box::pin(async move { Err(error) }),
            };
        let client = Arc::clone(&self.inner);
        let pending_scope = scope.clone();
        let pending_time = time.clone();
        scope::bind_future(
            scope,
            time,
            entered_at,
            Box::pin(async move {
                let settled = dispatch::settle(prepared).await?;
                client
                    .begin(settled)?
                    .bind(&pending_scope, pending_time, entered_at)
            }),
        )
    }

    /// Inspects effective marker policy without validation or I/O.
    #[must_use]
    #[inline]
    pub fn effective_proxy_marker_enabled(&self, request: &OutboundRequest) -> bool {
        match request.proxy_marker_setting() {
            OutboundProxyMarker::Inherit => self.proxy_marker_enabled,
            OutboundProxyMarker::On => true,
            OutboundProxyMarker::Off => false,
        }
    }

    /// Follows only with an explicit synchronous target policy and validated hop limit.
    ///
    /// Every hop uses this client's ordinary prepared dispatch and head finalizer. Followed
    /// bodies are dropped unread. Per-call timeouts restart per hop, but the initial absolute
    /// deadline remains binding through policy execution and final streamed body consumption.
    /// Plain send, begin and batches never follow. Custom credentials are policy-owned: only
    /// Authorization, Cookie and authority overrides are stripped on prepared cross-origin hops.
    ///
    /// # Errors
    /// Returns an unchanged hop error with its zero-based index, or an unchanged policy value
    /// with its one-based redirect number. The policy and its error need not be Send or Clone.
    #[inline]
    pub fn follow<P, E>(
        &self,
        request: OutboundRequest,
        limit: OutboundRedirectLimit,
        policy: P,
    ) -> LocalBoxFuture<'static, Result<OutboundFollowResult, OutboundFollowFailure<E>>>
    where
        P: for<'head> FnMut(
                &mut OutboundRedirectContext<'head>,
            ) -> Result<OutboundRedirectAction, E>
            + 'static,
        E: 'static,
    {
        redirect::follow(self, request, limit, policy)
    }

    #[inline]
    pub fn new(client: Arc<dyn OutboundHttpClient>) -> Self {
        Self {
            inner: client,
            proxy_marker_enabled: true,
        }
    }

    fn prepare(
        &self,
        request: OutboundRequest,
        mode: OutboundSendMode,
        cutoff: Option<Deadline>,
    ) -> Result<
        (
            OutboundScope,
            OutboundTime,
            MonotonicInstant,
            OutboundDispatchRequest,
        ),
        EdgeError,
    > {
        let scope = OutboundScope::capture(self.inner.progress());
        scope.check()?;
        let time = self.inner.time();
        let entered_at = time.clock().now();
        let prepared = dispatch::prepare(
            self,
            request,
            mode,
            time.clone(),
            entered_at,
            dispatch::DispatchBounds {
                observation_cutoff: cutoff,
                original_chain_deadline: None,
            },
            scope.clone(),
        )?;
        Ok((scope, time, entered_at, prepared))
    }

    #[must_use]
    #[inline]
    pub fn proxy_marker_enabled(&self) -> bool {
        self.proxy_marker_enabled
    }

    fn resolve_proxy_marker(&self, request: OutboundRequest) -> OutboundRequest {
        let setting = if self.effective_proxy_marker_enabled(&request) {
            OutboundProxyMarker::On
        } else {
            OutboundProxyMarker::Off
        };
        request.proxy_marker(setting)
    }

    /// Sends through shared preparation, provider head acquisition and finalization.
    ///
    /// # Errors
    /// Returns typed preparation, transport or response-policy failures unchanged.
    #[inline]
    pub fn send(&self, request: OutboundRequest) -> OutboundFuture<OutboundResponse> {
        let (scope, time, entered_at, prepared) =
            match self.prepare(request, OutboundSendMode::Send, None) {
                Ok(captured) => captured,
                Err(error) => return Box::pin(async move { Err(error) }),
            };
        let client = Arc::clone(&self.inner);
        scope::bind_future(
            scope,
            time,
            entered_at,
            Box::pin(async move {
                let settled = dispatch::settle(prepared).await?;
                client.dispatch_head(settled).await?.finish().await
            }),
        )
    }

    /// Starts eagerly and collects in original input order.
    /// The observation cutoff can intentionally truncate still-live request budgets.
    #[must_use]
    #[inline]
    pub fn send_all_until(
        &self,
        requests: Vec<OutboundRequest>,
        observation_cutoff: Deadline,
    ) -> LocalBoxFuture<'static, Result<OutboundBatchResults, OutboundBatchFailure>> {
        let batch = self.start_batch_until(requests, observation_cutoff);
        Box::pin(batch.collect())
    }

    /// Prepares every input and schedules eligible work before returning.
    #[must_use]
    #[inline]
    pub fn start_batch_until(
        &self,
        requests: Vec<OutboundRequest>,
        observation_cutoff: Deadline,
    ) -> OutboundBatch {
        let scope = OutboundScope::capture(self.inner.progress());
        let time = self.inner.time();
        let started_at = time.clock().now();
        if scope.check().is_err() {
            let count = requests.len();
            drop(requests);
            return OutboundBatch::cutoff(OutboundBatchContext::new(
                started_at,
                time,
                observation_cutoff,
                vec![None; count],
                Vec::new(),
                scope,
            ));
        }
        let mut slots = Vec::new();
        let mut budgets = Vec::with_capacity(requests.len());
        let mut preflight = Vec::new();
        for (index, request) in requests.into_iter().enumerate() {
            match dispatch::prepare(
                self,
                request,
                OutboundSendMode::Batch,
                time.clone(),
                started_at,
                dispatch::DispatchBounds {
                    observation_cutoff: Some(observation_cutoff),
                    original_chain_deadline: None,
                },
                scope.clone(),
            ) {
                Ok(prepared) => {
                    budgets.push(Some(prepared.budget()));
                    slots.push((index, prepared));
                }
                Err(error) => {
                    budgets.push(None);
                    preflight.push((index, time.clock().now(), Err(error)));
                }
            }
        }
        let context = OutboundBatchContext::new(
            started_at,
            time,
            observation_cutoff,
            budgets,
            preflight,
            scope,
        );
        if context.scope().check().is_err() {
            return OutboundBatch::cutoff(context);
        }
        self.inner
            .start_batch_until(OutboundPreparedBatch::new(context, slots))
    }

    #[inline]
    pub fn with_client<Client>(client: Client) -> Self
    where
        Client: OutboundHttpClient + 'static,
    {
        Self::new(Arc::new(client))
    }

    /// Sets the marker default for this handle; explicit per-request settings win.
    #[must_use]
    #[inline]
    pub fn with_proxy_marker(mut self, enabled: bool) -> Self {
        self.proxy_marker_enabled = enabled;
        self
    }
}

impl OutboundResponse {
    #[must_use]
    #[inline]
    pub fn body(&self) -> &Body {
        &self.body
    }

    /// Returns original-header evidence, never inferred from the filtered header map.
    ///
    /// Mutable header access invalidates an originally eligible result conservatively, even
    /// when only an unrelated field changes. Original ineligible/unknown evidence cannot be
    /// upgraded by application edits. MIME validation and redaction remain application-owned.
    #[must_use]
    #[inline]
    pub fn content_type_provenance(&self) -> ContentTypeProvenance {
        if self.headers_mutated
            && self.content_type_provenance == ContentTypeProvenance::SingleTextUnchanged
        {
            ContentTypeProvenance::Ineligible(ContentTypeIneligibility::Modified)
        } else {
            self.content_type_provenance
        }
    }

    #[must_use]
    #[inline]
    pub fn headers(&self) -> &HeaderMap {
        &self.headers
    }

    #[must_use]
    /// Returns adapter-facing mutable headers without normalizing them.
    ///
    /// Callers that mutate framing or hop-by-hop metadata must finish through
    /// [`Self::into_response`], which reapplies the defensive normalization pass.
    #[inline]
    pub fn headers_mut(&mut self) -> &mut HeaderMap {
        self.headers_mutated = true;
        &mut self.headers
    }

    #[must_use]
    #[inline]
    pub fn into_body(self) -> Body {
        self.body
    }

    /// Collects the body under a final response-buffer limit.
    ///
    /// # Errors
    /// Returns a typed source error or `BufferedBody` response-limit error.
    #[inline]
    pub async fn into_bytes_bounded(self, max: u64) -> Result<Bytes, EdgeError> {
        match self.body.into_stream() {
            Err(body) => validate_buffered_response_bytes(
                body.into_bytes().ok_or_else(|| {
                    EdgeError::internal(anyhow::anyhow!("body conversion lost buffered payload"))
                })?,
                max,
            ),
            Ok(stream) => collect_response_stream(stream, max).await,
        }
    }

    /// Collects the body under a final response-buffer limit and cooperative deadline.
    ///
    /// # Errors
    /// Returns 504 when the deadline is observed expired, otherwise a typed source or
    /// `BufferedBody` response-limit error.
    #[inline]
    pub async fn into_bytes_bounded_until(
        self,
        max: u64,
        deadline: Deadline,
    ) -> Result<Bytes, EdgeError> {
        let clock = self.monotonic_clock.clone();
        ensure_deadline_live(deadline, &clock)?;
        match self.body.into_stream() {
            Err(body) => {
                let bytes = body.into_bytes().ok_or_else(|| {
                    EdgeError::internal(anyhow::anyhow!("body conversion lost buffered payload"))
                })?;
                let outcome = validate_buffered_response_bytes(bytes, max);
                ensure_deadline_live(deadline, &clock)?;
                outcome
            }
            Ok(stream) => {
                collect_response_stream_until_with_clock(stream, max, deadline, &clock).await
            }
        }
    }

    #[must_use]
    /// Destructures the adapter-facing response representation without re-running response-header
    /// normalization. Call [`Self::into_response`] at an application boundary that requires the
    /// final defensive framing pass.
    #[inline]
    pub fn into_parts(self) -> (Method, StatusCode, HeaderMap, Body, ContentTypeProvenance) {
        let provenance = self.content_type_provenance();
        (
            self.request_method,
            self.status,
            self.headers,
            self.body,
            provenance,
        )
    }

    /// Converts the outbound value after one final defensive metadata pass.
    ///
    /// # Errors
    /// Returns a typed protocol error for malformed framing, or an internal error if the
    /// already-validated response cannot be assembled.
    #[inline]
    pub fn into_response(mut self) -> Result<Response, EdgeError> {
        let provenance = self.content_type_provenance();
        let normalized =
            normalize_response_headers(&self.request_method, self.status, &mut self.headers)?;
        if normalized.body_disposition != ResponseBodyDisposition::Payload {
            let owner = self.body.take_egress_owner();
            self.body = Body::from_parts(BodyPayload::Once(Bytes::new()), owner);
        }
        let effective_provenance = if provenance == ContentTypeProvenance::SingleTextUnchanged {
            normalized.content_type_provenance
        } else {
            provenance
        };
        let mut builder = response_builder()
            .status(self.status)
            .extension(effective_provenance);
        for (name, value) in &self.headers {
            builder = builder.header(name, value);
        }
        builder.body(self.body).map_err(EdgeError::internal)
    }

    #[must_use]
    #[inline]
    pub fn is_success(&self) -> bool {
        self.status.is_success()
    }

    /// Deserializes an already-buffered upstream body as JSON.
    ///
    /// # Errors
    /// Returns protocol 502 for a streamed body and decode 502 for malformed JSON.
    #[inline]
    pub fn json<Value>(&self) -> Result<Value, EdgeError>
    where
        Value: DeserializeOwned,
    {
        let Some(bytes) = self.body.as_bytes() else {
            return Err(EdgeError::bad_gateway_with_reason(
                "response body not buffered; use json_bounded(max) or json_bounded_until(max, deadline)",
                BadGatewayReason::Protocol,
            ));
        };
        decode_json(bytes)
    }

    /// Collects and deserializes an upstream body as JSON.
    ///
    /// # Errors
    /// Returns typed source/limit failures or decode 502 for malformed JSON.
    #[inline]
    pub async fn json_bounded<Value>(self, max: u64) -> Result<Value, EdgeError>
    where
        Value: DeserializeOwned,
    {
        let bytes = self.into_bytes_bounded(max).await?;
        decode_json(&bytes)
    }

    /// Collects and deserializes an upstream body under a cooperative deadline.
    ///
    /// # Errors
    /// Returns 504 on observed expiry, typed source/limit failures, or decode 502 for malformed
    /// JSON.
    #[inline]
    pub async fn json_bounded_until<Value>(
        self,
        max: u64,
        deadline: Deadline,
    ) -> Result<Value, EdgeError>
    where
        Value: DeserializeOwned,
    {
        let clock = self.monotonic_clock.clone();
        let bytes = self.into_bytes_bounded_until(max, deadline).await?;
        ensure_deadline_live(deadline, &clock)?;
        decode_json(&bytes)
    }

    #[must_use]
    /// Constructs an adapter response paired with its application clock.
    ///
    /// `headers` must already have passed [`OutboundHeaderInput::normalize`], including bodyless
    /// disposition handling, before the response is exposed through app-facing accessors.
    /// `content_type_provenance` must come from that pre-filter pass, not a second inspection of
    /// normalized headers. Use `Unknown` if original field boundaries or bytes are unavailable.
    /// The clock must be the same source used for the request and outbound client. This keeps
    /// deferred response-body deadlines in the application's monotonic clock domain.
    #[inline]
    pub fn new(
        request_method: Method,
        status: StatusCode,
        headers: HeaderMap,
        body: Body,
        monotonic_clock: MonotonicClock,
        content_type_provenance: ContentTypeProvenance,
    ) -> Self {
        Self {
            body,
            content_type_provenance,
            headers,
            headers_mutated: false,
            monotonic_clock,
            request_method,
            status,
        }
    }

    #[must_use]
    #[inline]
    pub fn status(&self) -> StatusCode {
        self.status
    }
}

impl OutboundRequest {
    #[must_use]
    #[inline]
    pub fn backend_target(&self) -> String {
        let host = bracket_ipv6(self.host_name());
        format!("{host}:{}", self.resolved_port())
    }

    #[must_use]
    #[inline]
    pub fn body<BodyValue>(mut self, body: BodyValue) -> Self
    where
        BodyValue: Into<Body>,
    {
        self.body = body.into();
        self
    }

    pub(crate) fn budget_inputs(&self) -> BudgetInputs {
        BudgetInputs {
            deadline: self.deadline,
            timeout: self.timeout,
        }
    }

    #[must_use]
    #[inline]
    pub fn cache_policy(mut self, policy: OutboundCachePolicy) -> Self {
        self.cache_policy = policy;
        self
    }

    #[must_use]
    #[inline]
    pub fn cert_host(&self) -> Option<&str> {
        (self.uri.scheme_str() == Some("https")).then(|| self.host_name())
    }

    /// Sets an independent connect-phase ceiling; zero is rejected at preflight.
    #[must_use]
    #[inline]
    pub fn connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = Some(timeout.min(DEADLINE_FAR_FUTURE));
        self
    }

    #[must_use]
    #[inline]
    pub fn connect_timeout_setting(&self) -> Option<Duration> {
        self.connect_timeout
    }

    #[must_use]
    #[inline]
    pub fn deadline(mut self, deadline: Deadline) -> Self {
        self.deadline = Some(deadline);
        self
    }

    /// Reassembles a request and revalidates its target.
    ///
    /// # Errors
    /// Returns [`EdgeError::BadRequest`] when `parts.uri` is not a canonicalizable HTTP(S)
    /// target.
    #[inline]
    pub fn from_parts(parts: OutboundRequestParts) -> Result<Self, EdgeError> {
        let canonical_uri = canonicalize_typed_uri(&parts.uri)?;
        Ok(Self {
            body: parts.body,
            cache_policy: parts.cache_policy,
            connect_timeout: parts
                .connect_timeout
                .map(|timeout| timeout.min(DEADLINE_FAR_FUTURE)),
            deadline: parts.deadline,
            headers: parts.headers,
            host_authority_override: parts
                .host_authority_override
                .map(validate_host_authority_override)
                .transpose()?,
            image_optimizer: parts.image_optimizer,
            inbound_read_deadline: parts.inbound_read_deadline,
            max_decoder_bytes: parts.max_decoder_bytes,
            max_brotli_window_bits: parts.max_brotli_window_bits,
            max_chunk_bytes: parts.max_chunk_bytes,
            max_decoded_response_bytes: parts.max_decoded_response_bytes,
            max_encoded_response_bytes: parts.max_encoded_response_bytes,
            max_request_body_bytes: parts.max_request_body_bytes,
            max_response_header_bytes: parts.max_response_header_bytes,
            max_response_header_count: parts.max_response_header_count,
            method: parts.method,
            proxy_marker: parts.proxy_marker,
            response_decoding: parts.response_decoding,
            response_mode: parts.response_mode,
            timeout: parts.timeout,
            uri: canonical_uri,
        })
    }

    fn from_raw(method: Method, raw: &str) -> Result<Self, EdgeError> {
        reject_ambiguous_raw_target(raw)?;
        let uri = canonicalize_url(raw)?;
        Ok(Self::with_canonical_uri(method, uri))
    }

    /// Creates an outbound request from an inbound request while removing framing and
    /// hop-by-hop metadata.
    ///
    /// # Errors
    /// Returns [`EdgeError::BadRequest`] when the target is invalid or a `connection`
    /// nomination is malformed.
    #[inline]
    pub fn from_request(request: Request, target: Uri) -> Result<Self, EdgeError> {
        let (parts, body) = request.into_parts();
        let mut outbound = Self::new(parts.method, target)?;
        outbound.body = body;
        outbound.inbound_read_deadline = parts
            .extensions
            .get::<ForwardedReadDeadline>()
            .map(ForwardedReadDeadline::deadline);
        outbound.headers = parts.headers;
        normalize_request_headers(&mut outbound.headers)?;
        Ok(outbound)
    }

    /// Creates a canonical GET request from a raw URL.
    ///
    /// # Errors
    /// Returns [`EdgeError::BadRequest`] when the URL is not an absolute HTTP(S) target.
    #[inline]
    pub fn get<Target>(uri: Target) -> Result<Self, EdgeError>
    where
        Target: AsRef<str>,
    {
        Self::from_raw(Method::GET, uri.as_ref())
    }

    #[must_use]
    #[inline]
    pub fn has_host_authority_override(&self) -> bool {
        self.host_authority_override.is_some()
    }

    /// Appends one validated header value.
    ///
    /// # Errors
    /// Returns [`EdgeError::BadRequest`] when the name is invalid, the value is not UTF-8,
    /// or the value contains bytes forbidden by HTTP header syntax.
    #[inline]
    pub fn header<N, V>(mut self, name: N, value: V) -> Result<Self, EdgeError>
    where
        N: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        let header_name = HeaderName::from_bytes(name.as_ref())
            .map_err(|_error| EdgeError::bad_request("invalid outbound header name"))?;
        if str::from_utf8(value.as_ref()).is_err() {
            return Err(EdgeError::bad_request(format!(
                "header value is not valid UTF-8: {header_name}"
            )));
        }
        let header_value = HeaderValue::from_bytes(value.as_ref()).map_err(|_error| {
            EdgeError::bad_request(format!(
                "header value contains forbidden bytes: {header_name}"
            ))
        })?;
        self.headers.append(header_name, header_value);
        Ok(self)
    }

    #[must_use]
    #[inline]
    pub fn headers(&self) -> &HeaderMap {
        &self.headers
    }

    #[must_use]
    #[inline]
    pub fn headers_mut(&mut self) -> &mut HeaderMap {
        &mut self.headers
    }

    #[must_use]
    #[inline]
    pub fn host_authority(&self) -> &str {
        self.host_authority_override.as_ref().map_or_else(
            || self.uri.authority().map_or("", Authority::as_str),
            Authority::as_str,
        )
    }

    /// Overrides only the outgoing HTTP authority while preserving the URI connection identity.
    ///
    /// # Errors
    /// Returns [`EdgeError::BadRequest`] unless `authority` is a host or host-and-port value with
    /// bracketed IPv6 and no user information, scheme, path, query, fragment, or controls.
    #[inline]
    pub fn host_authority_override(mut self, authority: &str) -> Result<Self, EdgeError> {
        let parsed = authority
            .parse::<Authority>()
            .map_err(|_error| invalid_host_authority_override())?;
        self.host_authority_override = Some(validate_host_authority_override(parsed)?);
        Ok(self)
    }

    #[must_use]
    #[inline]
    pub fn host_name(&self) -> &str {
        let host = self.uri.host().unwrap_or_default();
        host.strip_prefix('[')
            .and_then(|unbracketed| unbracketed.strip_suffix(']'))
            .unwrap_or(host)
    }

    /// Attaches native optimizer settings without changing the target or headers.
    ///
    /// Fastly supports these only for bodiless GET/HEAD Send/Follow hops with default caching.
    /// Its synchronous optimizer call has no established blocking-time bound; late results
    /// are rejected against the original deadline when control returns.
    #[must_use]
    #[inline]
    pub fn image_optimizer(mut self, options: OutboundImageOptimizerOptions) -> Self {
        self.image_optimizer = Some(options);
        self
    }

    #[must_use]
    #[inline]
    pub fn image_optimizer_options(&self) -> Option<&OutboundImageOptimizerOptions> {
        self.image_optimizer.as_ref()
    }

    #[must_use]
    #[inline]
    pub fn into_parts(self) -> OutboundRequestParts {
        OutboundRequestParts {
            body: self.body,
            cache_policy: self.cache_policy,
            connect_timeout: self.connect_timeout,
            deadline: self.deadline,
            headers: self.headers,
            host_authority_override: self.host_authority_override,
            image_optimizer: self.image_optimizer,
            inbound_read_deadline: self.inbound_read_deadline,
            max_decoder_bytes: self.max_decoder_bytes,
            max_brotli_window_bits: self.max_brotli_window_bits,
            max_chunk_bytes: self.max_chunk_bytes,
            max_decoded_response_bytes: self.max_decoded_response_bytes,
            max_encoded_response_bytes: self.max_encoded_response_bytes,
            max_request_body_bytes: self.max_request_body_bytes,
            max_response_header_bytes: self.max_response_header_bytes,
            max_response_header_count: self.max_response_header_count,
            method: self.method,
            proxy_marker: self.proxy_marker,
            response_decoding: self.response_decoding,
            response_mode: self.response_mode,
            timeout: self.timeout,
            uri: self.uri,
        }
    }

    #[must_use]
    #[inline]
    pub fn is_stream_body(&self) -> bool {
        self.body.is_stream()
    }

    #[must_use]
    #[inline]
    pub fn is_stream_response(&self) -> bool {
        self.response_mode == ResponseMode::Streamed
    }

    /// Serializes a request body as JSON.
    ///
    /// # Errors
    /// Returns [`EdgeError::Internal`] when serialization fails.
    #[inline]
    pub fn json<T>(mut self, value: &T) -> Result<Self, EdgeError>
    where
        T: Serialize,
    {
        self.body = Body::json(value).map_err(EdgeError::internal)?;
        if !self.headers.contains_key(CONTENT_TYPE) {
            self.headers
                .insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        }
        Ok(self)
    }

    #[must_use]
    #[inline]
    pub fn max_brotli_window_bits(mut self, bits: u8) -> Self {
        self.max_brotli_window_bits = bits;
        self
    }

    #[must_use]
    #[inline]
    pub fn max_chunk_bytes(mut self, bytes: NonZeroU64) -> Self {
        self.max_chunk_bytes = Some(bytes);
        self
    }

    #[must_use]
    #[inline]
    pub fn max_decoded_response_bytes(mut self, bytes: u64) -> Self {
        self.max_decoded_response_bytes = Some(bytes);
        self
    }

    #[must_use]
    #[inline]
    pub fn max_decoder_bytes(mut self, bytes: u64) -> Self {
        self.max_decoder_bytes = bytes;
        self
    }

    #[must_use]
    #[inline]
    pub fn max_encoded_response_bytes(mut self, bytes: u64) -> Self {
        self.max_encoded_response_bytes = Some(bytes);
        self
    }

    #[must_use]
    #[inline]
    pub fn max_request_body_bytes(mut self, bytes: u64) -> Self {
        self.max_request_body_bytes = bytes;
        self
    }

    #[must_use]
    /// Selects buffered response mode and sets its final collection limit.
    ///
    /// This is a last-write-wins mode setter: a later [`Self::stream_response`] call selects
    /// streamed mode instead.
    #[inline]
    pub fn max_response_bytes(mut self, bytes: u64) -> Self {
        self.response_mode = ResponseMode::Buffered { max_bytes: bytes };
        self
    }

    #[must_use]
    #[inline]
    pub fn max_response_header_bytes(mut self, bytes: u64) -> Self {
        self.max_response_header_bytes = Some(bytes);
        self
    }

    #[must_use]
    #[inline]
    pub fn max_response_header_count(mut self, count: u64) -> Self {
        self.max_response_header_count = Some(count);
        self
    }

    #[must_use]
    #[inline]
    pub fn method(&self) -> &Method {
        &self.method
    }

    /// Creates an outbound request from a typed URI.
    ///
    /// # Errors
    /// Returns [`EdgeError::BadRequest`] when the URI is not an absolute HTTP(S) target.
    #[expect(
        clippy::needless_pass_by_value,
        reason = "the public request constructor takes ownership consistently with http::Request"
    )]
    #[inline]
    pub fn new(method: Method, uri: Uri) -> Result<Self, EdgeError> {
        let canonical_uri = canonicalize_typed_uri(&uri)?;
        Ok(Self::with_canonical_uri(method, canonical_uri))
    }

    /// Creates a canonical POST request from a raw URL.
    ///
    /// # Errors
    /// Returns [`EdgeError::BadRequest`] when the URL is not an absolute HTTP(S) target.
    #[inline]
    pub fn post<Target>(uri: Target) -> Result<Self, EdgeError>
    where
        Target: AsRef<str>,
    {
        Self::from_raw(Method::POST, uri.as_ref())
    }

    #[must_use]
    #[inline]
    pub fn proxy_marker(mut self, setting: OutboundProxyMarker) -> Self {
        self.proxy_marker = setting;
        self
    }

    #[must_use]
    #[inline]
    pub fn proxy_marker_setting(&self) -> OutboundProxyMarker {
        self.proxy_marker
    }

    fn resolved_port(&self) -> u16 {
        self.uri.port_u16().unwrap_or_else(|| {
            if self.uri.scheme_str() == Some("https") {
                443
            } else {
                80
            }
        })
    }

    /// Selects response decoding without changing `Accept-Encoding` or any response limit.
    #[must_use]
    #[inline]
    pub fn response_decoding(mut self, choice: ResponseDecoding) -> Self {
        self.response_decoding = choice;
        self
    }

    #[must_use]
    #[inline]
    pub fn response_decoding_setting(&self) -> ResponseDecoding {
        self.response_decoding
    }

    #[must_use]
    #[inline]
    pub fn sni_hostname(&self) -> Option<&str> {
        let host = self.host_name();
        (self.uri.scheme_str() == Some("https") && host.parse::<IpAddr>().is_err()).then_some(host)
    }

    #[must_use]
    /// Selects streamed response mode.
    ///
    /// This is a last-write-wins mode setter: a later [`Self::max_response_bytes`] call selects
    /// buffered mode instead.
    #[inline]
    pub fn stream_response(mut self) -> Self {
        self.response_mode = ResponseMode::Streamed;
        self
    }

    #[must_use]
    #[inline]
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    #[must_use]
    #[inline]
    pub fn uri(&self) -> &Uri {
        &self.uri
    }

    fn with_canonical_uri(method: Method, uri: Uri) -> Self {
        Self {
            body: Body::empty(),
            cache_policy: OutboundCachePolicy::PlatformDefault,
            connect_timeout: None,
            deadline: None,
            headers: HeaderMap::new(),
            host_authority_override: None,
            image_optimizer: None,
            inbound_read_deadline: None,
            max_decoder_bytes: DEFAULT_MAX_DECODER_BYTES,
            max_brotli_window_bits: 24,
            max_chunk_bytes: None,
            max_decoded_response_bytes: None,
            max_encoded_response_bytes: None,
            max_request_body_bytes: DEFAULT_OUTBOUND_REQUEST_BODY_BYTES,
            max_response_header_bytes: None,
            max_response_header_count: None,
            method,
            proxy_marker: OutboundProxyMarker::Inherit,
            response_decoding: ResponseDecoding::Decode,
            response_mode: ResponseMode::Buffered {
                max_bytes: DEFAULT_MAX_RESPONSE_BYTES,
            },
            timeout: None,
            uri,
        }
    }
}

/// Applies the shared payload policy without polling the native body.
///
/// Declared lengths are checked first, then streaming encoded limits, recognized decoding,
/// decoded limits, and rechunking. Original coding and all 206 payloads keep their surviving
/// encoding/length headers and bypass decoding, not normalization or transport/final caps.
/// Identity still obeys the decoded cap. Adapters must handle bodyless responses first.
///
/// # Errors
/// Returns malformed content-length or declared payload-limit errors before rewriting headers.
#[inline]
pub fn apply_response_body_policy(
    native: BodyStream,
    headers: &mut HeaderMap,
    policy: ResponseBodyPolicy,
) -> Result<BodyStream, EdgeError> {
    let encoding = classify_content_encoding(headers);
    let delivery = payload_delivery(
        &encoding,
        policy.response_decoding == ResponseDecoding::Decode
            && policy.status != StatusCode::PARTIAL_CONTENT,
    );
    let max_buffered = match policy.response_mode {
        ResponseMode::Buffered { max_bytes } => Some(max_bytes),
        ResponseMode::Streamed => None,
    };
    enforce_declared_payload_length(
        headers,
        delivery,
        max_buffered,
        policy.max_decoded_response_bytes,
        policy.max_encoded_response_bytes,
    )?;
    let encoded = limit_encoded_stream(native, policy.max_encoded_response_bytes);
    if matches!(delivery, PayloadDelivery::Original) {
        return Ok(rechunk_stream(encoded, policy.max_chunk_bytes));
    }
    let decoded = match &encoding {
        ContentEncoding::Brotli => decode_brotli_stream(
            encoded,
            policy.max_brotli_window_bits,
            policy.max_decoder_bytes,
        ),
        ContentEncoding::Deflate => decode_deflate_stream(encoded, policy.max_decoder_bytes),
        ContentEncoding::Gzip => decode_gzip_stream(encoded, policy.max_decoder_bytes),
        ContentEncoding::Identity | ContentEncoding::Passthrough(_) => encoded,
    };
    if matches!(
        &encoding,
        ContentEncoding::Brotli | ContentEncoding::Deflate | ContentEncoding::Gzip
    ) {
        headers.remove(CONTENT_ENCODING);
        headers.remove(CONTENT_LENGTH);
    }
    let output = match &encoding {
        ContentEncoding::Brotli
        | ContentEncoding::Deflate
        | ContentEncoding::Gzip
        | ContentEncoding::Identity => {
            limit_decoded_stream(decoded, policy.max_decoded_response_bytes)
        }
        ContentEncoding::Passthrough(_) => decoded,
    };
    Ok(rechunk_stream(output, policy.max_chunk_bytes))
}

fn invalid_host_authority_override() -> EdgeError {
    EdgeError::bad_request("invalid outbound host authority override")
}

fn validate_host_authority_override(authority: Authority) -> Result<Authority, EdgeError> {
    let raw = authority.as_str();
    let host = authority.host();
    let invalid_port = match raw.strip_prefix(host) {
        Some("") => false,
        Some(suffix) if suffix.starts_with(':') => authority.port_u16().is_none(),
        _ => true,
    };
    let unbracketed_ipv6 = raw.bytes().filter(|byte| *byte == b':').count() > 1
        && !(raw.starts_with('[') && raw.contains(']'));
    if raw.is_empty()
        || host.is_empty()
        || raw.contains('@')
        || invalid_port
        || unbracketed_ipv6
        || HeaderValue::from_str(raw).is_err()
    {
        return Err(invalid_host_authority_override());
    }
    Ok(authority)
}

/// Collects a response stream under the final buffered-body cap.
///
/// # Errors
/// Returns a typed source error unchanged or a `BufferedBody` response-limit error.
#[inline]
pub async fn collect_response_stream(stream: BodyStream, max: u64) -> Result<Bytes, EdgeError> {
    collect_response_stream_inner(stream, max, None).await
}

/// Applies sound pre-poll checks to a normalized payload `content-length`.
/// This low-level check assumes ordinary decoding of a full payload; original/206 delivery uses
/// [`apply_response_body_policy`] so declared limits match the bytes actually delivered.
///
/// # Errors
/// Returns a protocol 502 for malformed length metadata or a typed response-limit 502 when
/// the declared wire/final size exceeds a comparable configured cap.
#[inline]
pub fn enforce_payload_content_length(
    headers: &HeaderMap,
    encoding: &ContentEncoding,
    max_buffered_bytes: Option<u64>,
    max_decoded_bytes: Option<u64>,
    max_encoded_bytes: Option<u64>,
) -> Result<(), EdgeError> {
    enforce_declared_payload_length(
        headers,
        payload_delivery(encoding, true),
        max_buffered_bytes,
        max_decoded_bytes,
        max_encoded_bytes,
    )
}

fn payload_delivery(encoding: &ContentEncoding, decode: bool) -> PayloadDelivery {
    match encoding {
        ContentEncoding::Identity => PayloadDelivery::Identity,
        ContentEncoding::Passthrough(_) => PayloadDelivery::Original,
        ContentEncoding::Brotli | ContentEncoding::Deflate | ContentEncoding::Gzip if decode => {
            PayloadDelivery::Decoded
        }
        ContentEncoding::Brotli | ContentEncoding::Deflate | ContentEncoding::Gzip => {
            PayloadDelivery::Original
        }
    }
}

fn enforce_declared_payload_length(
    headers: &HeaderMap,
    delivery: PayloadDelivery,
    max_buffered_bytes: Option<u64>,
    max_decoded_bytes: Option<u64>,
    max_encoded_bytes: Option<u64>,
) -> Result<(), EdgeError> {
    let Some(length) = parse_content_length(headers)? else {
        return Ok(());
    };
    if max_encoded_bytes.is_some_and(|max| length > max) {
        return Err(response_limit_error(ResponseLimitReason::EncodedBody));
    }
    match delivery {
        PayloadDelivery::Identity => {
            if max_decoded_bytes.is_some_and(|max| length > max) {
                return Err(response_limit_error(ResponseLimitReason::DecodedBody));
            }
            if max_buffered_bytes.is_some_and(|max| length > max) {
                return Err(response_limit_error(ResponseLimitReason::BufferedBody));
            }
        }
        PayloadDelivery::Original => {
            if max_buffered_bytes.is_some_and(|max| length > max) {
                return Err(response_limit_error(ResponseLimitReason::BufferedBody));
            }
        }
        PayloadDelivery::Decoded => {}
    }
    Ok(())
}

/// Applies the synthetic adapter-marker policy after ordinary header normalization.
/// Off neither charges nor edits headers. Inherit uses the direct adapter default, on;
/// the client facade resolves its own default before handing requests to an implementation.
///
/// # Errors
/// Returns a typed response-header limit error before insertion when the marker would exceed the
/// configured cumulative byte or field-count cap.
#[inline]
fn insert_proxy_header(
    headers: &mut HeaderMap,
    limiter: &mut ResponseHeaderLimiter,
    setting: OutboundProxyMarker,
    value: HeaderValue,
) -> Result<(), EdgeError> {
    if setting == OutboundProxyMarker::Off {
        return Ok(());
    }
    let name = HeaderName::from_static(PROXY_HEADER);
    limiter.observe_field(&name, &value)?;
    headers.insert(name, value);
    Ok(())
}

#[must_use]
#[inline]
pub fn limit_decoded_stream(stream: BodyStream, max: Option<u64>) -> BodyStream {
    limit_response_stream(stream, max, ResponseLimitReason::DecodedBody)
}

#[must_use]
#[inline]
pub fn limit_encoded_stream(stream: BodyStream, max: Option<u64>) -> BodyStream {
    limit_response_stream(stream, max, ResponseLimitReason::EncodedBody)
}

/// Reapplies request header normalization immediately before platform conversion.
///
/// # Errors
/// Returns [`EdgeError::BadRequest`] when a `connection` nomination is malformed.
#[inline]
pub fn normalize_for_dispatch(request: &mut OutboundRequest) -> Result<(), EdgeError> {
    normalize_request_headers(&mut request.headers)
}

/// Normalizes raw upstream metadata before response-body policy is selected.
///
/// # Errors
/// Returns a protocol-classified 502 when `connection` or payload framing is malformed.
#[inline]
fn normalize_response_headers(
    request_method: &Method,
    status: StatusCode,
    headers: &mut HeaderMap,
) -> Result<ResponseHeadNormalization, EdgeError> {
    let original_provenance = original_content_type_provenance(headers);
    let nominated = connection_nominations(headers, true)?;
    retain_utf8_header_values(headers, true);
    strip_hop_by_hop(headers, nominated, false);

    let content_type_provenance = if original_provenance
        == ContentTypeProvenance::SingleTextUnchanged
        && !headers.contains_key(CONTENT_TYPE)
    {
        ContentTypeProvenance::Ineligible(ContentTypeIneligibility::Removed)
    } else {
        original_provenance
    };

    let body_disposition = if status.is_informational() || status == StatusCode::NO_CONTENT {
        headers.remove(CONTENT_LENGTH);
        ResponseBodyDisposition::FramingBodyless
    } else if request_method == Method::HEAD || status == StatusCode::NOT_MODIFIED {
        parse_content_length(headers)?;
        ResponseBodyDisposition::FramingBodyless
    } else {
        let content_length = parse_content_length(headers)?;
        if status == StatusCode::RESET_CONTENT {
            headers.insert(CONTENT_LENGTH, HeaderValue::from_static("0"));
            ResponseBodyDisposition::ResetContent {
                declared_body: content_length.is_some_and(|length| length > 0),
            }
        } else {
            ResponseBodyDisposition::Payload
        }
    };
    Ok(ResponseHeadNormalization {
        body_disposition,
        content_type_provenance,
    })
}

fn original_content_type_provenance(headers: &HeaderMap) -> ContentTypeProvenance {
    let mut values = headers.get_all(CONTENT_TYPE).iter();
    let Some(value) = values.next() else {
        return ContentTypeProvenance::Ineligible(ContentTypeIneligibility::Missing);
    };
    if values.next().is_some() {
        ContentTypeProvenance::Ineligible(ContentTypeIneligibility::MultipleValues)
    } else if str::from_utf8(value.as_bytes()).is_err() {
        ContentTypeProvenance::Ineligible(ContentTypeIneligibility::NonText)
    } else {
        ContentTypeProvenance::SingleTextUnchanged
    }
}

#[must_use]
#[inline]
pub fn rechunk_stream(mut source: BodyStream, optional_maximum: Option<NonZeroU64>) -> BodyStream {
    let Some(maximum) = optional_maximum else {
        return source;
    };
    let maximum_chunk = usize::try_from(maximum.get()).unwrap_or(usize::MAX);
    stream! {
        while let Some(item) = source.next().await {
            match item {
                Ok(mut bytes) => {
                    if bytes.is_empty() {
                        yield Ok(Bytes::new());
                        continue;
                    }
                    while bytes.len() > maximum_chunk {
                        yield Ok(bytes.split_to(maximum_chunk));
                    }
                    if !bytes.is_empty() {
                        yield Ok(bytes);
                    }
                }
                Err(error) => {
                    yield Err(error);
                    return;
                }
            }
        }
    }
    .boxed_local()
}

/// Validates the adapter-independent outbound method, body, and response-policy contract.
/// Adapter-specific supported-option checks must run first. Begin requires buffered
/// uploads; Batch requires buffered uploads and responses. Validation never polls a body.
///
/// # Errors
/// Returns [`EdgeError::PayloadTooLarge`] for a buffered upload over its byte cap,
/// or [`EdgeError::BadRequest`] when a common contract invariant is invalid. Individual
/// adapters may reject additional target-specific constraints during request preparation.
#[inline]
pub fn validate_for_dispatch(
    request: &OutboundRequest,
    mode: OutboundSendMode,
) -> Result<(), EdgeError> {
    if request.body.has_egress_owner() {
        return Err(EdgeError::bad_request(
            "response-scoped body cannot be used as an outbound request body",
        ));
    }
    if request.image_optimizer_options().is_some()
        && matches!(mode, OutboundSendMode::Begin | OutboundSendMode::Batch)
    {
        return Err(EdgeError::bad_request(
            "image_optimizer is not supported for eager outbound requests",
        ));
    }
    image_optimizer::validate_options(request)?;
    if !matches!(
        request.method,
        Method::GET
            | Method::HEAD
            | Method::POST
            | Method::PUT
            | Method::PATCH
            | Method::DELETE
            | Method::OPTIONS
    ) {
        return Err(EdgeError::bad_request(format!(
            "method {} is not portable; supported: GET, HEAD, POST, PUT, PATCH, DELETE, OPTIONS",
            request.method
        )));
    }

    if matches!(request.method, Method::GET | Method::HEAD) {
        match request.body.as_bytes() {
            Some([]) => {}
            Some(_) => {
                return Err(EdgeError::bad_request(
                    "GET/HEAD request must not carry a body",
                ));
            }
            None if mode == OutboundSendMode::Batch => {
                return Err(EdgeError::bad_request(
                    "outbound batches require buffered request bodies",
                ));
            }
            None => {}
        }
    }

    if !(10..=30).contains(&request.max_brotli_window_bits) {
        return Err(EdgeError::bad_request(
            "max_brotli_window_bits must be between 10 and 30 inclusive",
        ));
    }
    if let Some(bytes) = request.body.as_bytes()
        && u64::try_from(bytes.len()).map_or(true, |length| length > request.max_request_body_bytes)
    {
        return Err(EdgeError::payload_too_large(
            RequestBodyOverflowOrigin::OutboundUpload,
        ));
    }
    if request
        .connect_timeout
        .is_some_and(|timeout| timeout.is_zero())
    {
        return Err(EdgeError::bad_request("connect timeout must be positive"));
    }
    if mode == OutboundSendMode::Begin
        && request.is_stream_body()
        && !matches!(request.method, Method::GET | Method::HEAD)
    {
        return Err(EdgeError::bad_request(
            "begin requires a buffered request body; use send for a streamed upload",
        ));
    }
    if mode == OutboundSendMode::Batch {
        if request.is_stream_body() {
            return Err(EdgeError::bad_request(
                "outbound batches require buffered request bodies; use send for a streamed upload",
            ));
        }
        if request.is_stream_response() {
            return Err(EdgeError::bad_request(
                "outbound batches require buffered responses; use send for a streamed response",
            ));
        }
    }
    Ok(())
}

fn bracket_ipv6(host: &str) -> String {
    if host.contains(':') {
        format!("[{host}]")
    } else {
        host.to_owned()
    }
}

fn canonicalize_typed_uri(uri: &Uri) -> Result<Uri, EdgeError> {
    let raw = uri.to_string();
    reject_ambiguous_raw_target(&raw)?;
    canonicalize_url(&raw)
}

fn canonicalize_url(raw: &str) -> Result<Uri, EdgeError> {
    let mut parsed = Url::parse(raw)
        .map_err(|_error| EdgeError::bad_request("outbound URI must be absolute with authority"))?;
    if !matches!(parsed.scheme(), "http" | "https") {
        return Err(EdgeError::bad_request(
            "outbound URI scheme must be http or https",
        ));
    }
    if parsed.host_str().is_none() {
        return Err(EdgeError::bad_request(
            "outbound URI must be absolute with authority",
        ));
    }
    if !parsed.username().is_empty() || parsed.password().is_some() {
        return Err(EdgeError::bad_request(
            "outbound URI must not contain userinfo; pass credentials via the `authorization` header",
        ));
    }
    if parsed.fragment().is_some() {
        return Err(EdgeError::bad_request(
            "outbound URI must not contain a fragment",
        ));
    }
    let default_port = if parsed.scheme() == "http" { 80 } else { 443 };
    if parsed.port() == Some(default_port) {
        parsed
            .set_port(None)
            .map_err(|()| EdgeError::bad_request("outbound URI contains an invalid port"))?;
    }
    parsed
        .as_str()
        .parse()
        .map_err(|_error| EdgeError::bad_request("outbound URI cannot be represented as HTTP URI"))
}

async fn collect_response_stream_inner<S>(
    mut source: S,
    max: u64,
    optional_deadline: Option<(Deadline, &MonotonicClock)>,
) -> Result<Bytes, EdgeError>
where
    S: Stream<Item = Result<Bytes, EdgeError>> + Unpin,
{
    let mut collected = Vec::new();
    let mut total = 0_u64;
    loop {
        if let Some((active_deadline, clock)) = optional_deadline {
            ensure_deadline_live(active_deadline, clock)?;
        }
        let next_item = source.next().await;
        if let Some((active_deadline, clock)) = optional_deadline {
            ensure_deadline_live(active_deadline, clock).map_err(|mut selected| {
                if let EdgeError::GatewayTimeout {
                    provider_detail, ..
                } = &mut selected
                {
                    *provider_detail = next_item
                        .as_ref()
                        .and_then(|item| item.as_ref().err())
                        .and_then(EdgeError::provider_failure_detail)
                        .copied();
                }
                selected
            })?;
        }
        let Some(item) = next_item else {
            return Ok(Bytes::from(collected));
        };
        let bytes = item?;
        let chunk_len = u64::try_from(bytes.len())
            .map_err(|_error| response_limit_error(ResponseLimitReason::BufferedBody))?;
        let next_total = total
            .checked_add(chunk_len)
            .ok_or_else(|| response_limit_error(ResponseLimitReason::BufferedBody))?;
        if next_total > max {
            if let Some((active_deadline, clock)) = optional_deadline {
                ensure_deadline_live(active_deadline, clock)?;
            }
            return Err(response_limit_error(ResponseLimitReason::BufferedBody));
        }
        collected.extend_from_slice(&bytes);
        total = next_total;
    }
}

/// Collects a response stream under an absolute deadline evaluated by `clock`.
///
/// # Errors
/// Returns a typed buffered-body overflow or gateway timeout while preserving source errors.
#[doc(hidden)]
#[inline]
pub async fn collect_response_stream_until_with_clock(
    stream: BodyStream,
    max: u64,
    deadline: Deadline,
    clock: &MonotonicClock,
) -> Result<Bytes, EdgeError> {
    collect_response_stream_inner(stream, max, Some((deadline, clock))).await
}

fn decode_json<Value>(bytes: &[u8]) -> Result<Value, EdgeError>
where
    Value: DeserializeOwned,
{
    serde_json::from_slice(bytes).map_err(|_error| {
        EdgeError::bad_gateway_with_reason(
            "failed to decode upstream JSON",
            BadGatewayReason::Decode(BadGatewayDecodeReason::Json),
        )
    })
}

fn ensure_deadline_live(deadline: Deadline, clock: &MonotonicClock) -> Result<(), EdgeError> {
    if deadline.is_expired_at(clock.now()) {
        Err(EdgeError::gateway_timeout("response body deadline expired"))
    } else {
        Ok(())
    }
}

fn normalize_request_headers(headers: &mut HeaderMap) -> Result<(), EdgeError> {
    let nominated = connection_nominations(headers, false)?;
    retain_utf8_header_values(headers, false);
    strip_hop_by_hop(headers, nominated, true);
    Ok(())
}

fn connection_nominations(
    headers: &HeaderMap,
    response: bool,
) -> Result<Vec<HeaderName>, EdgeError> {
    let mut nominated = Vec::new();
    for header_value in headers.get_all(CONNECTION) {
        let raw_value = str::from_utf8(header_value.as_bytes()).map_err(|_error| {
            malformed_connection(response, "connection header value is not valid UTF-8")
        })?;
        for raw_token in raw_value.split(',') {
            let token = raw_token.trim_matches([' ', '\t']);
            if token.is_empty() {
                return Err(malformed_connection(
                    response,
                    "connection header contains an empty nomination",
                ));
            }
            let name = HeaderName::from_bytes(token.as_bytes()).map_err(|_error| {
                malformed_connection(response, "connection header contains an invalid nomination")
            })?;
            nominated.push(name);
        }
    }
    Ok(nominated)
}

fn malformed_connection(response: bool, message: &'static str) -> EdgeError {
    if response {
        EdgeError::bad_gateway_with_reason(message, BadGatewayReason::Protocol)
    } else {
        EdgeError::bad_request(message)
    }
}

fn limit_response_stream(
    mut source: BodyStream,
    optional_maximum: Option<u64>,
    reason: ResponseLimitReason,
) -> BodyStream {
    let Some(maximum) = optional_maximum else {
        return source;
    };
    stream! {
        let mut total = 0_u64;
        while let Some(item) = source.next().await {
            match item {
                Ok(bytes) => {
                    let Ok(chunk_len) = u64::try_from(bytes.len()) else {
                        let error = response_limit_error(reason);
                        drop(source);
                        yield Err(error);
                        return;
                    };
                    let Some(next_total) = total.checked_add(chunk_len) else {
                        let error = response_limit_error(reason);
                        drop(source);
                        yield Err(error);
                        return;
                    };
                    if next_total > maximum {
                        let error = response_limit_error(reason);
                        drop(source);
                        yield Err(error);
                        return;
                    }
                    total = next_total;
                    yield Ok(bytes);
                }
                Err(error) => {
                    drop(source);
                    yield Err(error);
                    return;
                }
            }
        }
    }
    .boxed_local()
}

fn parse_content_length(headers: &HeaderMap) -> Result<Option<u64>, EdgeError> {
    let mut parsed = None;
    for value in headers.get_all(CONTENT_LENGTH) {
        let raw = str::from_utf8(value.as_bytes()).map_err(|_error| protocol_content_length())?;
        if raw.contains(',') {
            return Err(protocol_content_length());
        }
        let trimmed = raw.trim_matches([' ', '\t']);
        if trimmed.is_empty() || !trimmed.bytes().all(|byte| byte.is_ascii_digit()) {
            return Err(protocol_content_length());
        }
        let length = trimmed
            .parse::<u64>()
            .map_err(|_error| protocol_content_length())?;
        if parsed.is_some_and(|previous| previous != length) {
            return Err(protocol_content_length());
        }
        parsed = Some(length);
    }
    Ok(parsed)
}

fn protocol_content_length() -> EdgeError {
    EdgeError::bad_gateway_with_reason(
        "upstream response has malformed or conflicting content-length",
        BadGatewayReason::Protocol,
    )
}

fn response_limit_error(reason: ResponseLimitReason) -> EdgeError {
    EdgeError::response_too_large_with_reason("upstream response exceeded configured limit", reason)
}

fn retain_utf8_header_values(headers: &mut HeaderMap, response: bool) {
    let mut retained = HeaderMap::with_capacity(headers.len());
    for (name, value) in headers.iter() {
        if name != CONNECTION
            && !(response && (name == CONTENT_ENCODING || name == CONTENT_LENGTH))
            && str::from_utf8(value.as_bytes()).is_err()
        {
            if response {
                log::warn!("dropping non-UTF-8 outbound response header: {name}");
            } else {
                log::warn!("dropping non-UTF-8 outbound request header: {name}");
            }
            continue;
        }
        retained.append(name.clone(), value.clone());
    }
    *headers = retained;
}

fn strip_hop_by_hop(
    headers: &mut HeaderMap,
    nominated: Vec<HeaderName>,
    strip_request_framing: bool,
) {
    for name in nominated {
        headers.remove(name);
    }
    for name in [
        CONNECTION,
        HeaderName::from_static("keep-alive"),
        HeaderName::from_static("proxy-connection"),
        PROXY_AUTHENTICATE,
        PROXY_AUTHORIZATION,
        TE,
        TRAILER,
        TRANSFER_ENCODING,
        UPGRADE,
    ] {
        headers.remove(name);
    }
    if strip_request_framing {
        headers.remove(HOST);
        headers.remove(CONTENT_LENGTH);
    }
}

fn validate_buffered_response_bytes(bytes: Bytes, max: u64) -> Result<Bytes, EdgeError> {
    let length = u64::try_from(bytes.len())
        .map_err(|_error| response_limit_error(ResponseLimitReason::BufferedBody))?;
    if length > max {
        Err(response_limit_error(ResponseLimitReason::BufferedBody))
    } else {
        Ok(bytes)
    }
}

fn reject_ambiguous_raw_target(raw: &str) -> Result<(), EdgeError> {
    if raw.as_bytes().contains(&b'#') {
        return Err(EdgeError::bad_request(
            "outbound URI must not contain a fragment",
        ));
    }
    if raw.as_bytes().contains(&b'\\') {
        return Err(EdgeError::bad_request(
            "outbound URI must not contain a backslash",
        ));
    }
    let Some((_scheme, authority_tail)) = raw.split_once("://") else {
        return Err(EdgeError::bad_request(
            "outbound URI must be absolute with authority",
        ));
    };
    let authority = authority_tail
        .split(['/', '?', '#'])
        .next()
        .unwrap_or_default();
    if authority.is_empty() {
        return Err(EdgeError::bad_request(
            "outbound URI must be absolute with authority",
        ));
    }
    if authority.as_bytes().contains(&b'@') {
        return Err(EdgeError::bad_request(
            "outbound URI must not contain userinfo; pass credentials via the `authorization` header",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests;
