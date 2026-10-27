#![cfg_attr(
    any(feature = "fastly", test),
    expect(
        clippy::self_named_module_files,
        reason = "retain the repository module-file convention despite contradictory restriction lints"
    )
)]
#![cfg_attr(
    feature = "fastly",
    expect(
        clippy::arbitrary_source_item_ordering,
        reason = "the target-gated implementation module is kept before shared test seams"
    )
)]
#![cfg_attr(
    feature = "fastly",
    expect(
        clippy::pub_use,
        reason = "the target-gated implementation keeps Fastly imports out of native builds"
    )
)]

#[cfg(feature = "fastly")]
mod image_optimizer;

#[cfg(any(feature = "fastly", test))]
mod progress;

#[cfg(any(feature = "fastly", test))]
pub(crate) mod scope;

#[cfg(test)]
use std::time::Duration;

use edgezero_core::error::EdgeError;
#[cfg(any(feature = "fastly", test))]
use edgezero_core::error::{BadGatewayReason, BudgetSource};
#[cfg(any(feature = "fastly", test))]
use edgezero_core::outbound::{
    OutboundBatchDriverEvent, OutboundBatchItem, OutboundResponse, finish_batch_item,
};
#[cfg(any(feature = "fastly", feature = "test-utils"))]
use edgezero_core::outbound::{OutboundRequest, OutboundSendMode, validate_for_dispatch};
#[cfg(any(feature = "fastly", test))]
use edgezero_core::time::Deadline;
#[cfg(any(feature = "fastly", test))]
use edgezero_core::time::{DispatchBudget, MonotonicInstant};

#[cfg(any(feature = "fastly", test))]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SendFailure {
    BudgetedTimeout,
    ImageOptimizerUnavailable,
    LocalInvariant,
    PlatformInternal,
    ProviderTimeout,
    Transport,
    Unknown,
    Unreachable,
    UpstreamProtocol,
}

#[cfg(any(feature = "fastly", test))]
enum FastlyBatchObservation {
    Continue(OutboundBatchItem),
    Cutoff,
}

#[cfg(any(feature = "fastly", test))]
fn classify_send_failure(
    failure: SendFailure,
    budget: DispatchBudget,
    observed_at: MonotonicInstant,
) -> EdgeError {
    if budget.deadline.is_expired_at(observed_at) {
        return timeout_error(budget.cause);
    }
    match failure {
        SendFailure::BudgetedTimeout => timeout_error(budget.cause),
        SendFailure::ImageOptimizerUnavailable => EdgeError::bad_gateway_with_reason(
            "Fastly outbound request failed",
            BadGatewayReason::ImageOptimizerUnavailable,
        ),
        SendFailure::ProviderTimeout => timeout_error(BudgetSource::Unspecified),
        SendFailure::Unreachable => EdgeError::bad_gateway_with_reason(
            "outbound destination could not be reached",
            BadGatewayReason::Unreachable,
        ),
        SendFailure::Transport => EdgeError::bad_gateway_with_reason(
            "outbound connection failed after dispatch",
            BadGatewayReason::Transport,
        ),
        SendFailure::UpstreamProtocol => EdgeError::bad_gateway_with_reason(
            "upstream response violated the HTTP protocol",
            BadGatewayReason::Protocol,
        ),
        SendFailure::Unknown => EdgeError::bad_gateway_with_reason(
            "Fastly outbound request failed",
            BadGatewayReason::Unspecified,
        ),
        SendFailure::LocalInvariant | SendFailure::PlatformInternal => EdgeError::internal(
            anyhow::anyhow!("Fastly rejected an adapter-owned outbound operation"),
        ),
    }
}

#[cfg(test)]
fn test_budget(started_at: MonotonicInstant, cause: BudgetSource) -> DispatchBudget {
    let duration = Duration::from_secs(1);
    DispatchBudget {
        cause,
        deadline: Deadline::at_instant(started_at.checked_add(duration).expect("deadline instant")),
        duration,
    }
}

#[cfg(any(feature = "fastly", test))]
fn synchronous_send_until<Output, Send>(
    budget: DispatchBudget,
    clock: &edgezero_core::MonotonicClock,
    send: Send,
) -> Result<Output, EdgeError>
where
    Send: FnOnce() -> Result<Output, EdgeError>,
{
    let entered = clock.now();
    if budget.deadline.is_expired_at(entered) {
        return Err(timeout_error(budget.cause));
    }
    let outcome = send();
    let returned = clock.now();
    if returned < entered {
        return Err(EdgeError::internal(anyhow::anyhow!(
            "outbound clock moved backwards during native send"
        )));
    }
    if budget.deadline.is_expired_at(returned) {
        let mut selected = timeout_error(budget.cause);
        if let EdgeError::GatewayTimeout {
            provider_detail, ..
        } = &mut selected
        {
            *provider_detail = outcome
                .as_ref()
                .err()
                .and_then(EdgeError::provider_failure_detail)
                .copied();
        }
        return Err(selected);
    }
    outcome
}

#[cfg(test)]
fn assert_send_failure_error(failure: SendFailure, error: &EdgeError, selected: BudgetSource) {
    match failure {
        SendFailure::BudgetedTimeout => {
            if let EdgeError::GatewayTimeout { cause, .. } = error {
                assert_eq!(*cause, selected);
            } else {
                panic!("expected selected timeout");
            }
        }
        SendFailure::ProviderTimeout => assert!(matches!(
            error,
            EdgeError::GatewayTimeout {
                cause: BudgetSource::Unspecified,
                ..
            }
        )),
        SendFailure::LocalInvariant | SendFailure::PlatformInternal => {
            assert!(matches!(error, EdgeError::Internal { .. }));
        }
        SendFailure::Transport => assert!(matches!(
            error,
            EdgeError::BadGateway {
                reason: BadGatewayReason::Transport,
                ..
            }
        )),
        SendFailure::ImageOptimizerUnavailable => assert!(matches!(
            error,
            EdgeError::BadGateway {
                reason: BadGatewayReason::ImageOptimizerUnavailable,
                ..
            }
        )),
        SendFailure::Unknown => assert!(matches!(
            error,
            EdgeError::BadGateway {
                reason: BadGatewayReason::Unspecified,
                ..
            }
        )),
        SendFailure::Unreachable => assert!(matches!(
            error,
            EdgeError::BadGateway {
                reason: BadGatewayReason::Unreachable,
                ..
            }
        )),
        SendFailure::UpstreamProtocol => assert!(matches!(
            error,
            EdgeError::BadGateway {
                reason: BadGatewayReason::Protocol,
                ..
            }
        )),
    }
}

#[cfg(any(feature = "fastly", test))]
fn finish_fastly_batch_observation(
    index: usize,
    started_at: MonotonicInstant,
    completed_at: MonotonicInstant,
    observation_cutoff: Deadline,
    outcome: Result<OutboundResponse, EdgeError>,
) -> FastlyBatchObservation {
    match finish_batch_item(index, started_at, completed_at, observation_cutoff, outcome) {
        Some(item) => FastlyBatchObservation::Continue(item),
        None => FastlyBatchObservation::Cutoff,
    }
}

#[cfg(feature = "fastly")]
mod fastly_impl {
    use core::convert::identity;
    use core::future::Future;
    use core::task::{Context, Poll, Waker};
    #[cfg(feature = "test-utils")]
    use std::cell::Cell;
    use std::collections::HashMap;
    use std::io::{Read as _, Result as IoResult, Write};
    use std::net::IpAddr;
    use std::num::NonZeroU64;
    use std::sync::{Mutex, OnceLock};
    use std::time::Duration;

    use async_stream::stream;
    use async_trait::async_trait;
    use base16ct::lower::encode_string;
    use bytes::Bytes;
    use edgezero_core::body::{Body, BodyStream};
    use edgezero_core::error::{BadGatewayReason, EdgeError};
    use edgezero_core::error_detail::{
        FastlyFailureCause, ProviderFailureDetail, ProviderFailurePhase,
    };
    use edgezero_core::http::header::ACCEPT_ENCODING;
    use edgezero_core::http::{HeaderMap, HeaderValue, Method, StatusCode, Uri};
    use edgezero_core::outbound::{
        OutboundBatch, OutboundBatchDriverEvent, OutboundCachePolicy, OutboundFuture,
        OutboundHeaderFidelity, OutboundHeaderInput, OutboundHttpClient, OutboundProxyMarker,
        OutboundRequest, OutboundRequestParts, OutboundResponse, OutboundTime,
        ResponseBodyDisposition, ResponseBodyPolicy, ResponseDecoding, ResponseHeaderLimiter,
        ResponseMode, apply_response_body_policy, collect_response_stream, normalize_for_dispatch,
        validate_for_dispatch,
    };
    use edgezero_core::time::{
        BATCH_DISPATCH_SLACK_MAX, DEADLINE_FAR_FUTURE, Deadline, DispatchBudget, MonotonicClock,
        MonotonicInstant, dispatch_budget,
    };
    #[cfg(test)]
    use fastly::Body as FastlyBody;
    use fastly::backend::BackendCreationError;
    use fastly::handle::{BodyHandle, PendingRequestHandle, PollHandleResult};
    use fastly::http::body::StreamingBody;
    use fastly::http::request::{PendingRequest, PollResult, SendError, SendErrorCause};
    use fastly::{Backend, Request as FastlyRequest, Response as FastlyResponse};
    use fastly_sys::fastly_async_io;
    use futures_util::future::poll_fn;
    use futures_util::stream::FuturesUnordered;
    use futures_util::{FutureExt as _, StreamExt as _};
    use sha2::{Digest as _, Sha256};

    use super::progress::{HeadPoll, Progress, Transport};
    use super::{
        FastlyBatchObservation, driver_selection_failure, finish_fastly_batch_observation,
        timeout_error, validate_batch_request,
    };

    type NativeProgress = Progress<NativeTransport>;

    pub(crate) fn native_time(clock: MonotonicClock) -> OutboundTime {
        OutboundTime::new(clock, super::progress::wait_current)
    }

    enum PendingHead {
        Direct(PendingRequestHandle),
        Sdk(Box<PendingRequest>),
    }

    struct NativeTransport;

    impl Transport for NativeTransport {
        type Body = BodyHandle;
        type Head = FastlyResponse;
        type Pending = PendingHead;

        #[expect(
            unsafe_code,
            reason = "readiness borrows the live SDK-owned body handle without transferring ownership"
        )]
        #[cfg_attr(
            not(target_arch = "wasm32"),
            expect(
                deprecated,
                reason = "the pinned SDK exposes WASI handle identity only"
            )
        )]
        fn body_handle(body: &BodyHandle) -> u32 {
            // SAFETY: this observes identity only; the slot retains the owning SDK handle.
            unsafe { body.as_u32() }
        }

        fn head_handle(pending: &PendingHead) -> Option<u32> {
            match pending {
                PendingHead::Direct(handle) => Some(pending_handle_id(handle)),
                PendingHead::Sdk(_) => None,
            }
        }

        fn poll_head(
            &mut self,
            pending: PendingHead,
            budget: DispatchBudget,
            clock: &MonotonicClock,
        ) -> HeadPoll<PendingHead, FastlyResponse> {
            match pending {
                PendingHead::Direct(handle) => match handle.poll() {
                    PollHandleResult::Pending(waiting) => {
                        HeadPoll::Pending(PendingHead::Direct(waiting))
                    }
                    PollHandleResult::Done(result) => HeadPoll::Ready(
                        result
                            .map(|(head, body)| FastlyResponse::from_handles(head, body))
                            .map_err(|cause| map_send_cause(&cause, budget, clock)),
                    ),
                },
                PendingHead::Sdk(mut sdk) => match (*sdk).poll() {
                    PollResult::Pending(waiting) => {
                        *sdk = waiting;
                        HeadPoll::Pending(PendingHead::Sdk(sdk))
                    }
                    PollResult::Done(result) => HeadPoll::Ready(
                        result.map_err(|error| map_send_error(&error, budget, clock)),
                    ),
                },
            }
        }

        fn read_body(
            &mut self,
            body: &mut BodyHandle,
            maximum: usize,
            budget: DispatchBudget,
            clock: &MonotonicClock,
        ) -> Result<Bytes, EdgeError> {
            let mut bytes = vec![0; maximum];
            let count = body.read(&mut bytes).map_err(|error| {
                let classified = if budget.deadline.is_expired_at(clock.now()) {
                    timeout_error(budget.cause)
                } else {
                    EdgeError::bad_gateway_with_reason(
                        "Fastly upstream response body failed",
                        BadGatewayReason::Transport,
                    )
                };
                with_fastly_detail(
                    classified,
                    ProviderFailurePhase::ResponseBodyRead,
                    FastlyFailureCause::IoError { kind: error.kind() },
                )
            })?;
            if count > maximum {
                return Err(EdgeError::internal(anyhow::anyhow!(
                    "Fastly body read exceeded the supplied buffer"
                )));
            }
            bytes.truncate(count);
            Ok(Bytes::from(bytes))
        }

        #[expect(
            unsafe_code,
            reason = "the timed readiness ABI borrows stable IDs from owning SDK handles"
        )]
        fn select(&mut self, handles: &[u32], timeout_ms: u32) -> Result<Option<usize>, EdgeError> {
            let mut selected = u32::MAX;
            // SAFETY: the slice remains live through the call; zero means indefinite, so callers use a positive timeout.
            let status = unsafe {
                fastly_async_io::select(
                    handles.as_ptr(),
                    handles.len(),
                    timeout_ms,
                    &raw mut selected,
                )
            };
            if !status.is_ok() {
                return Err(with_fastly_detail(
                    EdgeError::internal(anyhow::anyhow!("Fastly timed readiness failed")),
                    ProviderFailurePhase::Send,
                    FastlyFailureCause::InternalError {
                        status: Some(status.code),
                    },
                ));
            }
            if selected == u32::MAX {
                return Ok(None);
            }
            usize::try_from(selected).map(Some).map_err(|_error| {
                EdgeError::internal(anyhow::anyhow!("invalid Fastly readiness index"))
            })
        }
    }

    pub const DYNAMIC_BACKENDS_DISABLED_MESSAGE: &str = "Fastly dynamic backends are not enabled on this service; enable them in the service configuration";
    const DISPATCH_SLACK_MESSAGE: &str = "Fastly batch adapter overhead between batch_now and SDK arming (preflight + dynamic-backend lookup/creation + SDK setup) exceeded BATCH_DISPATCH_SLACK_MAX; refusing to arm SDK timers with stale duration";

    static BACKENDS: OnceLock<Mutex<HashMap<String, (BackendIdentity, Backend)>>> = OnceLock::new();

    #[cfg(feature = "test-utils")]
    std::thread_local! {
        static DISPATCH_SLACK_INJECTION: Cell<Option<Duration>> = const { Cell::new(None) };
    }

    #[cfg(feature = "test-utils")]
    struct DispatchSlackInjection {
        previous: Option<Duration>,
    }

    #[cfg(feature = "test-utils")]
    #[expect(
        clippy::missing_trait_methods,
        reason = "`Drop::pin_drop` is unstable (`pin_ergonomics`) and conflicts with `drop`"
    )]
    impl Drop for DispatchSlackInjection {
        fn drop(&mut self) {
            DISPATCH_SLACK_INJECTION.set(self.previous);
        }
    }

    /// Overrides the dispatch-guard clock offset for one target-runtime contract test.
    #[cfg(feature = "test-utils")]
    #[inline]
    #[must_use]
    pub fn inject_dispatch_slack_for_test(delay: Duration) -> impl Drop {
        let previous = DISPATCH_SLACK_INJECTION.replace(Some(delay));
        DispatchSlackInjection { previous }
    }

    #[derive(Clone, Debug, Eq, Hash, PartialEq)]
    struct BackendIdentity {
        host: String,
        phase_ms: u64,
        port: u16,
        scheme: String,
        tls: bool,
        wire_authority: String,
    }

    struct PreparedRequest {
        backend: Backend,
        budget: DispatchBudget,
        parts: OutboundRequestParts,
        started_at: MonotonicInstant,
    }

    struct PendingSlot {
        metadata: PendingSlotMetadata,
        pending: PendingRequestHandle,
    }

    struct PendingSlotMetadata {
        index: usize,
        budget: DispatchBudget,
        max_decoder_bytes: u64,
        max_brotli_window_bits: u8,
        max_chunk_bytes: Option<NonZeroU64>,
        max_decoded_response_bytes: Option<u64>,
        max_encoded_response_bytes: Option<u64>,
        max_response_header_bytes: Option<u64>,
        max_response_header_count: Option<u64>,
        request_method: Method,
        proxy_marker: OutboundProxyMarker,
        response_decoding: ResponseDecoding,
        response_mode: ResponseMode,
    }

    /// Native outbound HTTP implementation for Fastly Compute.
    pub struct FastlyOutboundClient {
        time: OutboundTime,
    }

    impl FastlyOutboundClient {
        #[must_use]
        #[inline]
        pub fn new() -> Self {
            Self::with_time(native_time(MonotonicClock::default()))
        }

        /// Builds a client that evaluates every outbound lifetime against `clock`.
        #[must_use]
        #[inline]
        pub fn with_time(time: OutboundTime) -> Self {
            Self { time }
        }

        fn ensure_backend(
            &self,
            request: OutboundRequest,
            budget: DispatchBudget,
            batch_started_at: Option<MonotonicInstant>,
        ) -> Result<(Backend, OutboundRequestParts), EdgeError> {
            let (identity, parts) = backend_identity(request, budget)?;
            let registry = BACKENDS.get_or_init(|| Mutex::new(HashMap::new()));
            let backend = cached_backend(
                registry,
                identity,
                budget,
                self.time.clock(),
                |name, settings| {
                    let target =
                        format!("{}:{}", parts.uri.host().unwrap_or_default(), settings.port);
                    let timers = backend_timers(settings.phase_ms);
                    let mut builder = Backend::builder(name, target)
                        .override_host(&settings.wire_authority)
                        .connect_timeout(timers.connect)
                        .first_byte_timeout(timers.first_byte)
                        .between_bytes_timeout(timers.between_bytes);
                    if settings.tls {
                        builder = builder.enable_ssl().check_certificate(&settings.host);
                        if settings.host.parse::<IpAddr>().is_err() {
                            builder = builder.sni_hostname(&settings.host);
                        }
                    }
                    register_backend_with_batch_guard(
                        batch_started_at,
                        budget,
                        self.time.clock(),
                        || builder.finish(),
                    )
                },
            )?;
            Ok((backend, parts))
        }

        fn prepare(
            &self,
            request: OutboundRequest,
            started_at: MonotonicInstant,
        ) -> Result<PreparedRequest, EdgeError> {
            validate_for_dispatch(&request, super::OutboundSendMode::Send)?;
            self.prepare_validated(request, started_at, None, false)
        }

        fn prepare_batch(
            &self,
            request: OutboundRequest,
            started_at: MonotonicInstant,
            observation_cutoff: Deadline,
        ) -> Result<PreparedRequest, EdgeError> {
            validate_batch_request(&request)?;
            self.prepare_validated(request, started_at, Some(observation_cutoff), true)
        }

        fn prepare_validated(
            &self,
            mut request: OutboundRequest,
            started_at: MonotonicInstant,
            observation_cutoff: Option<Deadline>,
            enforce_batch_slack: bool,
        ) -> Result<PreparedRequest, EdgeError> {
            let budget = dispatch_budget(&request, started_at, observation_cutoff)?;
            normalize_fastly_request(&mut request)?;
            budget_remaining(budget, self.time.clock())?;
            let (backend, parts) =
                self.ensure_backend(request, budget, enforce_batch_slack.then_some(started_at))?;
            budget_remaining(budget, self.time.clock())?;
            Ok(PreparedRequest {
                backend,
                budget,
                parts,
                started_at,
            })
        }

        async fn execute(&self, prepared: PreparedRequest) -> Result<OutboundResponse, EdgeError> {
            let budget = prepared.budget;
            let progress = NativeProgress::new(
                NativeTransport,
                self.time.clock().clone(),
                prepared.started_at,
                Some(budget.deadline),
            );
            progress
                .drive(race_deadline(
                    self.execute_with_progress(prepared, &progress)
                        .boxed_local(),
                    budget,
                    &self.time,
                ))
                .await?
        }

        async fn execute_with_progress(
            &self,
            prepared: PreparedRequest,
            progress: &NativeProgress,
        ) -> Result<OutboundResponse, EdgeError> {
            let PreparedRequest {
                backend,
                budget,
                parts,
                started_at,
            } = prepared;
            let OutboundRequestParts {
                body,
                cache_policy,
                headers,
                image_optimizer,
                max_decoder_bytes,
                max_brotli_window_bits,
                max_chunk_bytes,
                max_decoded_response_bytes,
                max_encoded_response_bytes,
                max_request_body_bytes,
                max_response_header_bytes,
                max_response_header_count,
                method,
                proxy_marker,
                response_decoding,
                response_mode,
                uri,
                ..
            } = parts;
            let fastly_request = build_fastly_request(&method, &uri, &headers, cache_policy);
            let response = match body {
                Body::Once(bytes) => {
                    validate_request_body_length(&bytes, max_request_body_bytes)?;
                    let mut buffered_request = fastly_request;
                    if let Some(options) = image_optimizer {
                        buffered_request
                            .set_image_optimizer(super::image_optimizer::native_options(&options)?);
                        dispatch_guard(started_at, budget, self.time.clock())?;
                        super::synchronous_send_until(budget, self.time.clock(), || {
                            buffered_request
                                .send(backend)
                                .map_err(|error| map_send_error(&error, budget, self.time.clock()))
                        })?
                    } else {
                        buffered_request.set_body(bytes.to_vec());
                        dispatch_guard(started_at, budget, self.time.clock())?;
                        let pending = buffered_request
                            .send_async(backend)
                            .map_err(|error| map_send_error(&error, budget, self.time.clock()))?;
                        progress
                            .head(PendingHead::Sdk(Box::new(pending)), budget)
                            .receive()
                            .await?
                    }
                }
                Body::Stream(source) => {
                    send_streamed(
                        fastly_request,
                        backend,
                        source,
                        max_request_body_bytes,
                        budget,
                        started_at,
                        self.time.clock().clone(),
                        progress,
                    )
                    .await?
                }
            };
            process_response(
                response,
                method,
                proxy_marker,
                response_decoding,
                response_mode,
                budget,
                max_decoder_bytes,
                max_brotli_window_bits,
                max_chunk_bytes,
                max_decoded_response_bytes,
                max_encoded_response_bytes,
                max_response_header_bytes,
                max_response_header_count,
                self.time.clone(),
                progress,
            )
            .await
        }

        fn dispatch_batch_slot(
            &self,
            index: usize,
            prepared: PreparedRequest,
        ) -> Result<PendingSlot, EdgeError> {
            let PreparedRequest {
                backend,
                budget,
                parts,
                started_at,
            } = prepared;
            let OutboundRequestParts {
                body,
                cache_policy,
                headers,
                max_decoder_bytes,
                max_brotli_window_bits,
                max_chunk_bytes,
                max_decoded_response_bytes,
                max_encoded_response_bytes,
                max_request_body_bytes,
                max_response_header_bytes,
                max_response_header_count,
                method,
                proxy_marker,
                response_decoding,
                response_mode,
                uri,
                ..
            } = parts;
            let Body::Once(bytes) = body else {
                return Err(EdgeError::internal(anyhow::anyhow!(
                    "Fastly batch preflight admitted a streamed upload"
                )));
            };
            validate_request_body_length(&bytes, max_request_body_bytes)?;
            let mut request = build_fastly_request(&method, &uri, &headers, cache_policy);
            request.set_body(bytes.to_vec());
            dispatch_guard(started_at, budget, self.time.clock())?;
            let (request_handle, optional_body_handle) = request.into_handles();
            let body_handle = optional_body_handle.ok_or_else(|| {
                EdgeError::internal(anyhow::anyhow!(
                    "Fastly buffered batch request did not produce a body handle"
                ))
            })?;
            let pending = request_handle
                .send_async_to_backend(body_handle, &backend)
                .map_err(|cause| map_send_cause(&cause, budget, self.time.clock()))?;
            Ok(PendingSlot {
                metadata: PendingSlotMetadata {
                    index,
                    budget,
                    max_decoder_bytes,
                    max_brotli_window_bits,
                    max_chunk_bytes,
                    max_decoded_response_bytes,
                    max_encoded_response_bytes,
                    max_response_header_bytes,
                    max_response_header_count,
                    request_method: method,
                    proxy_marker,
                    response_decoding,
                    response_mode,
                },
                pending,
            })
        }
    }

    impl Default for FastlyOutboundClient {
        #[inline]
        fn default() -> Self {
            Self::new()
        }
    }

    #[async_trait(?Send)]
    impl OutboundHttpClient for FastlyOutboundClient {
        #[inline]
        async fn send(&self, request: OutboundRequest) -> Result<OutboundResponse, EdgeError> {
            super::scope::ensure_open()?;
            let started_at = self.time.clock().now();
            let prepared = self.prepare(request, started_at)?;
            self.execute(prepared).await
        }

        #[inline]
        fn start_batch_until(
            &self,
            requests: Vec<OutboundRequest>,
            observation_cutoff: Deadline,
        ) -> OutboundBatch {
            if let Err(error) = super::scope::ensure_open() {
                let failed = stream! { yield OutboundBatchDriverEvent::Failed(error); };
                return OutboundBatch::from_driver(requests.len(), failed);
            }
            let batch_started_at = self.time.clock().now();
            let slot_count = requests.len();
            if observation_cutoff.is_expired_at(batch_started_at) {
                return OutboundBatch::cutoff(slot_count);
            }

            let progress = NativeProgress::new(
                NativeTransport,
                self.time.clock().clone(),
                batch_started_at,
                Some(observation_cutoff),
            );

            let mut completed = Vec::new();
            let mut pending = Vec::new();
            let mut observation_cutoff_reached = false;
            for (index, request) in requests.into_iter().enumerate() {
                let prepared = self
                    .prepare_batch(request, batch_started_at, observation_cutoff)
                    .and_then(|prepared| {
                        let mut timer = self.time.wait_until(prepared.budget.deadline);
                        let ready = progress.with_current(|| {
                            timer.as_mut().poll(&mut Context::from_waker(Waker::noop()))
                        });
                        if let Poll::Ready(result) = ready {
                            return Err(result
                                .err()
                                .unwrap_or_else(|| timeout_error(prepared.budget.cause)));
                        }
                        self.dispatch_batch_slot(index, prepared)
                            .map(|slot| (slot, timer))
                    });
                match prepared {
                    Ok(slot) => pending.push(slot),
                    Err(error) => {
                        let completed_at = self.time.clock().now();
                        match finish_fastly_batch_observation(
                            index,
                            batch_started_at,
                            completed_at,
                            observation_cutoff,
                            Err(error),
                        ) {
                            FastlyBatchObservation::Continue(item) => completed.push(item),
                            FastlyBatchObservation::Cutoff => {
                                observation_cutoff_reached = true;
                                break;
                            }
                        }
                    }
                }
            }

            let clock = self.time.clock().clone();
            let mut exchanges = pending
                .into_iter()
                .map(|(slot, timer)| {
                    finish_timed_slot(slot, timer, self.time.clone(), progress.clone())
                        .boxed_local()
                })
                .collect::<FuturesUnordered<_>>();
            let completions = stream! {
                for item in completed {
                    yield OutboundBatchDriverEvent::Item(item);
                }

                if observation_cutoff_reached {
                    yield OutboundBatchDriverEvent::Cutoff;
                    return;
                }

                while !exchanges.is_empty() && !observation_cutoff.is_expired_at(clock.now()) {
                    let selected = match progress.drive(exchanges.next()).await {
                        Ok(selected) => selected,
                        Err(error) => {
                            yield driver_selection_failure(error);
                            return;
                        }
                    };
                    let Some((index, completed_at, outcome)) = selected else { break; };
                    match finish_fastly_batch_observation(
                        index,
                        batch_started_at,
                        completed_at,
                        observation_cutoff,
                        outcome,
                    ) {
                        FastlyBatchObservation::Continue(item) => {
                            yield OutboundBatchDriverEvent::Item(item);
                        }
                        FastlyBatchObservation::Cutoff => {
                            yield OutboundBatchDriverEvent::Cutoff;
                            return;
                        }
                    }
                }

                if !exchanges.is_empty() {
                    yield OutboundBatchDriverEvent::Cutoff;
                }
            };
            OutboundBatch::from_driver(slot_count, super::scope::bind_batch(completions))
        }
    }

    #[derive(Clone, Copy)]
    struct BackendTimers {
        between_bytes: Duration,
        connect: Duration,
        first_byte: Duration,
    }

    fn backend_identity(
        request: OutboundRequest,
        budget: DispatchBudget,
    ) -> Result<(BackendIdentity, OutboundRequestParts), EdgeError> {
        let scheme_str = request
            .uri()
            .scheme_str()
            .ok_or_else(|| EdgeError::bad_request("outbound request URI has no scheme"))?;
        let port = request
            .uri()
            .port_u16()
            .unwrap_or(if scheme_str == "https" { 443 } else { 80 });
        let host = request.host_name().to_owned();
        let wire_authority = request.host_authority().to_owned();
        let tls = scheme_str == "https";
        let scheme = scheme_str.to_owned();
        let parts = request.into_parts();
        let identity = BackendIdentity {
            host,
            phase_ms: backend_timer_ms(parts.timeout.unwrap_or(budget.duration)),
            port,
            scheme,
            tls,
            wire_authority,
        };
        Ok((identity, parts))
    }

    #[expect(
        clippy::big_endian_bytes,
        reason = "versioned backend identity uses a fixed byte order independent of the host architecture"
    )]
    fn backend_name(identity: &BackendIdentity) -> String {
        let mut hasher = Sha256::new();
        for field in [&identity.scheme, &identity.host, &identity.wire_authority] {
            hasher.update(field.as_bytes());
            hasher.update([0_u8]);
        }
        hasher.update(identity.port.to_be_bytes());
        hasher.update([u8::from(identity.tls)]);
        hasher.update(identity.phase_ms.to_be_bytes());
        format!("ez_b31v1_{}", encode_string(&hasher.finalize()))
    }

    fn backend_timer_ms(anchor: Duration) -> u64 {
        let maximum = ceil_millis(DEADLINE_FAR_FUTURE);
        ceil_millis(anchor.min(DEADLINE_FAR_FUTURE))
            .checked_next_power_of_two()
            .unwrap_or(maximum)
            .min(maximum)
    }

    fn backend_timers(phase_ms: u64) -> BackendTimers {
        let phase = Duration::from_millis(phase_ms);
        BackendTimers {
            between_bytes: phase,
            connect: phase,
            first_byte: phase,
        }
    }

    fn cached_backend<Value, Register>(
        registry: &Mutex<HashMap<String, (BackendIdentity, Value)>>,
        identity: BackendIdentity,
        budget: DispatchBudget,
        clock: &MonotonicClock,
        register: Register,
    ) -> Result<Value, EdgeError>
    where
        Value: Clone,
        Register: FnOnce(
            &str,
            &BackendIdentity,
        ) -> Result<Result<Value, BackendCreationError>, EdgeError>,
    {
        let name = backend_name(&identity);
        // Serialize registration and insertion; NameInUse alone cannot certify a configuration.
        let mut cache = registry.lock().map_err(|_poisoned| {
            EdgeError::internal(anyhow::anyhow!("Fastly backend cache was poisoned"))
        })?;
        budget_remaining(budget, clock)?;
        if let Some((registered_identity, backend)) = cache.get(&name) {
            if registered_identity != &identity {
                return Err(EdgeError::internal(anyhow::anyhow!(
                    "dynamic backend name collision; refusing to reuse"
                )));
            }
            return Ok(backend.clone());
        }
        let result = register(&name, &identity)?;
        finish_backend_creation(result, budget, clock, |backend| {
            cache.insert(name, (identity, backend.clone()));
            Ok(backend)
        })
    }

    fn build_fastly_request(
        method: &Method,
        uri: &Uri,
        headers: &HeaderMap,
        cache_policy: OutboundCachePolicy,
    ) -> FastlyRequest {
        let mut request = FastlyRequest::new(method.clone(), uri.to_string());
        request.set_method(method.clone());
        for (name, value) in headers {
            request.append_header(name.as_str(), value.as_bytes());
        }
        if cache_policy == OutboundCachePolicy::Bypass {
            request.set_pass(true);
        }
        request
    }

    fn normalize_fastly_request(request: &mut OutboundRequest) -> Result<(), EdgeError> {
        normalize_for_dispatch(request)?;
        if !request.headers().contains_key(ACCEPT_ENCODING) {
            request
                .headers_mut()
                .insert(ACCEPT_ENCODING, HeaderValue::from_static("identity"));
        }
        Ok(())
    }

    fn ceil_millis(duration: Duration) -> u64 {
        let rounded = duration.as_nanos().div_ceil(1_000_000);
        u64::try_from(rounded).unwrap_or(u64::MAX).max(1)
    }

    fn register_backend_with_batch_guard<BackendValue, CreationError, Register>(
        batch_started_at: Option<MonotonicInstant>,
        budget: DispatchBudget,
        clock: &MonotonicClock,
        register: Register,
    ) -> Result<Result<BackendValue, CreationError>, EdgeError>
    where
        Register: FnOnce() -> Result<BackendValue, CreationError>,
    {
        if let Some(started_at) = batch_started_at {
            dispatch_guard(started_at, budget, clock)?;
        }
        Ok(register())
    }

    #[cfg_attr(
        not(target_env = "p1"),
        expect(
            deprecated,
            reason = "Fastly exposes only the raw handle identity needed to restore select order"
        )
    )]
    fn pending_handle_id(handle: &PendingRequestHandle) -> u32 {
        handle.as_u32()
    }

    async fn finish_selected(
        response: Result<FastlyResponse, EdgeError>,
        metadata: PendingSlotMetadata,
        time: &OutboundTime,
        progress: &NativeProgress,
    ) -> Result<OutboundResponse, EdgeError> {
        let PendingSlotMetadata {
            budget,
            max_decoder_bytes,
            max_brotli_window_bits,
            max_chunk_bytes,
            max_decoded_response_bytes,
            max_encoded_response_bytes,
            max_response_header_bytes,
            max_response_header_count,
            request_method,
            proxy_marker,
            response_decoding,
            response_mode,
            ..
        } = metadata;
        process_response(
            response?,
            request_method,
            proxy_marker,
            response_decoding,
            response_mode,
            budget,
            max_decoder_bytes,
            max_brotli_window_bits,
            max_chunk_bytes,
            max_decoded_response_bytes,
            max_encoded_response_bytes,
            max_response_header_bytes,
            max_response_header_count,
            time.clone(),
            progress,
        )
        .await
    }

    fn map_backend_creation_error(error: &BackendCreationError) -> EdgeError {
        let classified = match error {
            BackendCreationError::Disallowed => EdgeError::bad_gateway_with_reason(
                DYNAMIC_BACKENDS_DISABLED_MESSAGE,
                BadGatewayReason::Unspecified,
            ),
            BackendCreationError::NameInUse => EdgeError::internal(anyhow::anyhow!(
                "dynamic backend name collision; refusing to reuse"
            )),
            BackendCreationError::BetweenBytesTimeoutTooLarge(_)
            | BackendCreationError::ConnectTimeoutTooLarge(_)
            | BackendCreationError::EncodingError(_)
            | BackendCreationError::FirstByteTimeoutTooLarge(_)
            | BackendCreationError::InvalidHealthcheckValue(_)
            | BackendCreationError::NameTooLong(_) => EdgeError::internal(anyhow::anyhow!(
                "Fastly rejected deterministic dynamic backend configuration"
            )),
            BackendCreationError::HostError(_) => EdgeError::bad_gateway_with_reason(
                "Fastly failed to register a dynamic backend",
                BadGatewayReason::Unspecified,
            ),
        };
        with_fastly_detail(
            classified,
            ProviderFailurePhase::BackendRegistration,
            backend_failure_cause(error),
        )
    }

    fn with_fastly_detail(
        mut error: EdgeError,
        phase: ProviderFailurePhase,
        cause: FastlyFailureCause,
    ) -> EdgeError {
        if let EdgeError::BadGateway {
            provider_detail, ..
        }
        | EdgeError::GatewayTimeout {
            provider_detail, ..
        }
        | EdgeError::Internal {
            provider_detail, ..
        } = &mut error
        {
            *provider_detail = Some(ProviderFailureDetail::Fastly { phase, cause });
        }
        error
    }

    fn backend_failure_cause(error: &BackendCreationError) -> FastlyFailureCause {
        match error {
            BackendCreationError::BetweenBytesTimeoutTooLarge(_) => {
                FastlyFailureCause::BackendBetweenBytesTimeoutTooLarge
            }
            BackendCreationError::ConnectTimeoutTooLarge(_) => {
                FastlyFailureCause::BackendConnectTimeoutTooLarge
            }
            BackendCreationError::FirstByteTimeoutTooLarge(_) => {
                FastlyFailureCause::BackendFirstByteTimeoutTooLarge
            }
            BackendCreationError::Disallowed => FastlyFailureCause::BackendDisallowed,
            BackendCreationError::EncodingError(_) => FastlyFailureCause::BackendEncodingError,
            BackendCreationError::InvalidHealthcheckValue(_) => {
                FastlyFailureCause::BackendInvalidHealthcheckValue
            }
            BackendCreationError::NameInUse => FastlyFailureCause::BackendNameInUse,
            BackendCreationError::NameTooLong(_) => FastlyFailureCause::BackendNameTooLong,
            BackendCreationError::HostError(status) => FastlyFailureCause::BackendHostError {
                status: status.code,
            },
        }
    }

    fn send_failure_cause(error: &SendErrorCause) -> FastlyFailureCause {
        match error {
            SendErrorCause::DnsTimeout => FastlyFailureCause::DnsTimeout,
            SendErrorCause::DnsError { rcode, info_code } => FastlyFailureCause::DnsError {
                rcode: *rcode,
                info_code: *info_code,
            },
            SendErrorCause::DestinationNotFound => FastlyFailureCause::DestinationNotFound,
            SendErrorCause::DestinationUnavailable => FastlyFailureCause::DestinationUnavailable,
            SendErrorCause::DestinationIpUnroutable => FastlyFailureCause::DestinationIpUnroutable,
            SendErrorCause::ConnectionRefused => FastlyFailureCause::ConnectionRefused,
            SendErrorCause::ConnectionTerminated => FastlyFailureCause::ConnectionTerminated,
            SendErrorCause::ConnectionTimeout => FastlyFailureCause::ConnectionTimeout,
            SendErrorCause::ConnectionLimitReached => FastlyFailureCause::ConnectionLimitReached,
            SendErrorCause::TlsProtocolError => FastlyFailureCause::TlsProtocolError,
            SendErrorCause::TlsCertificateError => FastlyFailureCause::TlsCertificateError,
            SendErrorCause::TlsConfigurationError => FastlyFailureCause::TlsConfigurationError,
            SendErrorCause::TlsAlertReceived { alert_id } => FastlyFailureCause::TlsAlertReceived {
                alert_id: *alert_id,
            },
            SendErrorCause::HttpIncompleteResponse => FastlyFailureCause::HttpIncompleteResponse,
            SendErrorCause::HttpProtocolError => FastlyFailureCause::HttpProtocolError,
            SendErrorCause::HttpResponseHeaderSectionTooLarge => {
                FastlyFailureCause::HttpResponseHeaderSectionTooLarge
            }
            SendErrorCause::HttpResponseBodyTooLarge => {
                FastlyFailureCause::HttpResponseBodyTooLarge
            }
            SendErrorCause::HttpResponseTimeout => FastlyFailureCause::HttpResponseTimeout,
            SendErrorCause::HttpResponseStatusInvalid => {
                FastlyFailureCause::HttpResponseStatusInvalid
            }
            SendErrorCause::HttpUpgradeFailed => FastlyFailureCause::HttpUpgradeFailed,
            SendErrorCause::HttpRequestCacheKeyInvalid => {
                FastlyFailureCause::HttpRequestCacheKeyInvalid
            }
            SendErrorCause::HttpRequestUriInvalid => FastlyFailureCause::HttpRequestUriInvalid,
            SendErrorCause::HttpCacheLimitExceeded => FastlyFailureCause::HttpCacheLimitExceeded,
            SendErrorCause::HttpCacheApiUnsupported => FastlyFailureCause::HttpCacheApiUnsupported,
            SendErrorCause::IoError(source) => FastlyFailureCause::IoError {
                kind: source.kind(),
            },
            SendErrorCause::FanoutNotEnabled => FastlyFailureCause::FanoutNotEnabled,
            SendErrorCause::ImageOptimizerUnsupported => {
                FastlyFailureCause::ImageOptimizerUnsupported
            }
            SendErrorCause::InternalError(status) => FastlyFailureCause::InternalError {
                status: status.as_ref().map(|value| value.code),
            },
            SendErrorCause::Http2StreamError {
                frame_type,
                error_code,
            } => FastlyFailureCause::Http2StreamError {
                frame_type: *frame_type,
                error_code: *error_code,
            },
            SendErrorCause::RequestCollapse => FastlyFailureCause::RequestCollapse,
            SendErrorCause::Custom(_) => FastlyFailureCause::Custom,
            _ => FastlyFailureCause::Unknown,
        }
    }

    fn finish_backend_creation<BackendValue, RetainSuccess>(
        result: Result<BackendValue, BackendCreationError>,
        budget: DispatchBudget,
        clock: &MonotonicClock,
        retain_success: RetainSuccess,
    ) -> Result<BackendValue, EdgeError>
    where
        RetainSuccess: FnOnce(BackendValue) -> Result<BackendValue, EdgeError>,
    {
        match result {
            Ok(backend) => {
                let retained = retain_success(backend);
                if budget.deadline.is_expired_at(clock.now()) {
                    Err(timeout_error(budget.cause))
                } else {
                    retained
                }
            }
            Err(error) => {
                if budget.deadline.is_expired_at(clock.now()) {
                    Err(with_fastly_detail(
                        timeout_error(budget.cause),
                        ProviderFailurePhase::BackendRegistration,
                        backend_failure_cause(&error),
                    ))
                } else {
                    Err(map_backend_creation_error(&error))
                }
            }
        }
    }

    fn cause_to_failure(cause: &SendErrorCause) -> super::SendFailure {
        match cause {
            SendErrorCause::ConnectionTimeout | SendErrorCause::HttpResponseTimeout => {
                super::SendFailure::BudgetedTimeout
            }
            SendErrorCause::DnsTimeout => super::SendFailure::ProviderTimeout,
            SendErrorCause::ConnectionLimitReached
            | SendErrorCause::ConnectionRefused
            | SendErrorCause::DestinationIpUnroutable
            | SendErrorCause::DestinationNotFound
            | SendErrorCause::DestinationUnavailable
            | SendErrorCause::DnsError { .. }
            | SendErrorCause::TlsAlertReceived { .. }
            | SendErrorCause::TlsCertificateError
            | SendErrorCause::TlsConfigurationError
            | SendErrorCause::TlsProtocolError => super::SendFailure::Unreachable,
            SendErrorCause::ConnectionTerminated | SendErrorCause::IoError(_) => {
                super::SendFailure::Transport
            }
            SendErrorCause::Http2StreamError { .. }
            | SendErrorCause::HttpIncompleteResponse
            | SendErrorCause::HttpProtocolError
            | SendErrorCause::HttpResponseBodyTooLarge
            | SendErrorCause::HttpResponseHeaderSectionTooLarge
            | SendErrorCause::HttpResponseStatusInvalid
            | SendErrorCause::HttpUpgradeFailed => super::SendFailure::UpstreamProtocol,
            SendErrorCause::HttpCacheApiUnsupported
            | SendErrorCause::HttpCacheLimitExceeded
            | SendErrorCause::HttpRequestCacheKeyInvalid
            | SendErrorCause::HttpRequestUriInvalid => super::SendFailure::LocalInvariant,
            SendErrorCause::InternalError(_) => super::SendFailure::PlatformInternal,
            SendErrorCause::ImageOptimizerUnsupported => {
                super::SendFailure::ImageOptimizerUnavailable
            }
            SendErrorCause::FanoutNotEnabled
            | SendErrorCause::RequestCollapse
            | SendErrorCause::Custom(_)
            | _ => super::SendFailure::Unknown,
        }
    }

    fn map_send_error(
        error: &SendError,
        budget: DispatchBudget,
        clock: &MonotonicClock,
    ) -> EdgeError {
        map_send_cause(error.root_cause(), budget, clock)
    }

    fn map_send_cause(
        cause: &SendErrorCause,
        budget: DispatchBudget,
        clock: &MonotonicClock,
    ) -> EdgeError {
        with_fastly_detail(
            super::classify_send_failure(cause_to_failure(cause), budget, clock.now()),
            ProviderFailurePhase::Send,
            send_failure_cause(cause),
        )
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "the adapter consumes independent response-policy fields"
    )]
    async fn process_response(
        mut response: FastlyResponse,
        request_method: Method,
        proxy_marker: OutboundProxyMarker,
        response_decoding: ResponseDecoding,
        response_mode: ResponseMode,
        budget: DispatchBudget,
        max_decoder_bytes: u64,
        max_brotli_window_bits: u8,
        max_chunk_bytes: Option<NonZeroU64>,
        max_decoded_response_bytes: Option<u64>,
        max_encoded_response_bytes: Option<u64>,
        max_response_header_bytes: Option<u64>,
        max_response_header_count: Option<u64>,
        time: OutboundTime,
        progress: &NativeProgress,
    ) -> Result<OutboundResponse, EdgeError> {
        let clock = time.clock().clone();
        budget_remaining(budget, &clock)?;
        let response_clock = clock.clone();
        let status = StatusCode::from_u16(response.get_status().as_u16()).map_err(|_error| {
            EdgeError::bad_gateway_with_reason(
                "Fastly returned an invalid upstream status code",
                BadGatewayReason::Protocol,
            )
        })?;
        let (mut headers, normalization, _) = OutboundHeaderInput::new(
            "fastly",
            response_headers(&response),
            OutboundHeaderFidelity::AdapterVisibleFields,
        )
        .normalize(
            ResponseHeaderLimiter::new(max_response_header_bytes, max_response_header_count),
            &request_method,
            status,
            proxy_marker,
        )?;
        let disposition = normalization.body_disposition;
        let body = if disposition == ResponseBodyDisposition::FramingBodyless {
            Body::empty()
        } else {
            let declared_reset_body = matches!(
                disposition,
                ResponseBodyDisposition::ResetContent {
                    declared_body: true
                }
            );
            let native = progress.body(response.take_body().into_handle(), budget);
            if matches!(disposition, ResponseBodyDisposition::ResetContent { .. }) {
                if !declared_reset_body {
                    let mut reset_stream = native;
                    if let Some(item) = reset_stream.next().await {
                        item?;
                    }
                }
                Body::empty()
            } else {
                let shaped = apply_response_body_policy(
                    native,
                    &mut headers,
                    ResponseBodyPolicy {
                        max_brotli_window_bits,
                        max_chunk_bytes,
                        max_decoded_response_bytes,
                        max_decoder_bytes,
                        max_encoded_response_bytes,
                        response_decoding,
                        response_mode,
                        status,
                    },
                )?;
                match response_mode {
                    ResponseMode::Buffered { max_bytes } => {
                        Body::from(collect_response_stream(shaped, max_bytes).await?)
                    }
                    ResponseMode::Streamed => {
                        Body::from_stream(deadline_stream(shaped, budget, time, progress.clone()))
                    }
                }
            }
        };
        budget_remaining(budget, &response_clock)?;
        Ok(OutboundResponse::new(
            request_method,
            status,
            headers,
            body,
            response_clock,
            normalization.content_type_provenance,
        ))
    }

    fn response_headers(response: &FastlyResponse) -> HeaderMap {
        let mut headers = HeaderMap::new();
        for name in response.get_header_names() {
            for value in response.get_header_all(name) {
                headers.append(name.clone(), value.clone());
            }
        }
        headers
    }

    trait StreamedUploadWriter {
        fn finish(self) -> IoResult<()>;
        fn flush(&mut self) -> IoResult<()>;
        fn write_all(&mut self, bytes: &[u8]) -> IoResult<()>;
    }

    impl StreamedUploadWriter for StreamingBody {
        fn finish(self) -> IoResult<()> {
            StreamingBody::finish(self)
        }

        fn flush(&mut self) -> IoResult<()> {
            Write::flush(self)
        }

        fn write_all(&mut self, bytes: &[u8]) -> IoResult<()> {
            Write::write_all(self, bytes)
        }
    }

    async fn complete_streamed_exchange<Writer, Wait, Output>(
        mut writer: Writer,
        mut source: BodyStream,
        maximum: u64,
        budget: DispatchBudget,
        clock: &MonotonicClock,
        wait: Wait,
    ) -> Result<Output, EdgeError>
    where
        Writer: StreamedUploadWriter,
        Wait: FnOnce() -> Result<Output, EdgeError>,
    {
        let mut total = 0_u64;
        loop {
            budget_remaining(budget, clock)?;
            let next_item = source.next().await;
            budget_remaining(budget, clock)?;
            let Some(item) = next_item else {
                break;
            };
            let bytes = item?;
            let length = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
            let Some(next_total) = total.checked_add(length) else {
                return Err(EdgeError::payload_too_large(
                    edgezero_core::RequestBodyOverflowOrigin::OutboundUpload,
                ));
            };
            if next_total > maximum {
                return Err(EdgeError::payload_too_large(
                    edgezero_core::RequestBodyOverflowOrigin::OutboundUpload,
                ));
            }
            resolve_stream_io_result(
                writer.write_all(&bytes),
                budget,
                clock.now(),
                "Fastly outbound request body write failed",
            )?;
            resolve_stream_io_result(
                writer.flush(),
                budget,
                clock.now(),
                "Fastly outbound request body flush failed",
            )?;
            total = next_total;
        }
        resolve_stream_io_result(
            writer.finish(),
            budget,
            clock.now(),
            "Fastly outbound request completion failed",
        )?;
        wait()
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "upload ownership and original dispatch budget stay explicit at the provider boundary"
    )]
    async fn send_streamed(
        request: FastlyRequest,
        backend: Backend,
        source: BodyStream,
        maximum: u64,
        budget: DispatchBudget,
        started_at: MonotonicInstant,
        clock: MonotonicClock,
        progress: &NativeProgress,
    ) -> Result<FastlyResponse, EdgeError> {
        dispatch_guard(started_at, budget, &clock)?;
        let (writer, pending) = request
            .send_async_streaming(backend)
            .map_err(|error| map_send_error(&error, budget, &clock))?;
        let uploaded =
            complete_streamed_exchange(writer, source, maximum, budget, &clock, || Ok(pending))
                .await?;
        progress
            .head(PendingHead::Sdk(Box::new(uploaded)), budget)
            .receive()
            .await
    }

    fn resolve_stream_io_result(
        result: IoResult<()>,
        budget: DispatchBudget,
        observed_at: MonotonicInstant,
        failure_message: &'static str,
    ) -> Result<(), EdgeError> {
        if budget.deadline.is_expired_at(observed_at) {
            let selected = timeout_error(budget.cause);
            return Err(match result {
                Err(source) => with_fastly_detail(
                    selected,
                    ProviderFailurePhase::RequestBodyUpload,
                    FastlyFailureCause::IoError {
                        kind: source.kind(),
                    },
                ),
                Ok(()) => selected,
            });
        }
        result.map_err(|error| {
            EdgeError::bad_gateway_with_detail(
                failure_message,
                BadGatewayReason::Transport,
                ProviderFailureDetail::Fastly {
                    phase: ProviderFailurePhase::RequestBodyUpload,
                    cause: FastlyFailureCause::IoError { kind: error.kind() },
                },
            )
        })
    }

    #[cfg(test)]
    fn fastly_body_stream(
        body: FastlyBody,
        budget: DispatchBudget,
        clock: MonotonicClock,
    ) -> BodyStream {
        let entered = budget
            .deadline
            .instant()
            .checked_sub(budget.duration)
            .unwrap_or(budget.deadline.instant());
        NativeProgress::new(NativeTransport, clock, entered, Some(budget.deadline))
            .body(body.into_handle(), budget)
    }

    fn deadline_stream(
        mut source: BodyStream,
        budget: DispatchBudget,
        time: OutboundTime,
        progress: NativeProgress,
    ) -> BodyStream {
        stream! {
            let mut timer = time.wait_until(budget.deadline);
            let mut previous = time.clock().now();
            loop {
                let read = race_with_timer(source.next(), budget, &time, &mut timer, &mut previous, |item| {
                    item.as_ref().and_then(|outcome| outcome.as_ref().err()).and_then(EdgeError::provider_failure_detail).copied()
                });
                let next_item = match progress.drive(read).await.and_then(identity) {
                    Ok(item) => item,
                    Err(error) => {
                        drop(source); drop(timer); drop(time); drop(progress);
                        yield Err(error);
                        return;
                    }
                };
                match next_item {
                    Some(Ok(bytes)) => yield Ok(bytes),
                    Some(Err(error)) => {
                        drop(source); drop(timer); drop(time); drop(progress);
                        yield Err(error);
                        return;
                    }
                    None => return,
                }
            }
        }
        .boxed_local()
    }

    async fn finish_timed_slot(
        slot: PendingSlot,
        mut timer: OutboundFuture<()>,
        time: OutboundTime,
        progress: NativeProgress,
    ) -> (usize, MonotonicInstant, Result<OutboundResponse, EdgeError>) {
        let PendingSlot {
            metadata,
            pending: handle,
        } = slot;
        let index = metadata.index;
        let budget = metadata.budget;
        let lease = progress.head(PendingHead::Direct(handle), budget);
        let exchange = async {
            let response = lease.receive().await;
            finish_selected(response, metadata, &time, &progress).await
        };
        let mut previous = time.clock().now();
        let outcome = race_with_timer(
            exchange,
            budget,
            &time,
            &mut timer,
            &mut previous,
            |result| {
                result
                    .as_ref()
                    .err()
                    .and_then(EdgeError::provider_failure_detail)
                    .copied()
            },
        )
        .await
        .and_then(identity);
        (index, time.clock().now(), outcome)
    }

    async fn race_deadline<Output>(
        future: impl Future<Output = Result<Output, EdgeError>>,
        budget: DispatchBudget,
        time: &OutboundTime,
    ) -> Result<Output, EdgeError> {
        let mut timer = time.wait_until(budget.deadline);
        let mut previous = time.clock().now();
        race_with_timer(future, budget, time, &mut timer, &mut previous, |result| {
            result
                .as_ref()
                .err()
                .and_then(EdgeError::provider_failure_detail)
                .copied()
        })
        .await?
    }

    fn checked_now(
        time: &OutboundTime,
        previous: &mut MonotonicInstant,
    ) -> Result<MonotonicInstant, EdgeError> {
        let observed = time.clock().now();
        if observed < *previous {
            return Err(EdgeError::internal(anyhow::anyhow!(
                "outbound clock moved backwards during I/O"
            )));
        }
        *previous = observed;
        Ok(observed)
    }

    async fn race_with_timer<Output>(
        future: impl Future<Output = Output>,
        budget: DispatchBudget,
        time: &OutboundTime,
        timer: &mut OutboundFuture<()>,
        previous: &mut MonotonicInstant,
        detail: impl FnOnce(&Output) -> Option<edgezero_core::ProviderFailureDetail>,
    ) -> Result<Output, EdgeError> {
        let mut extract_detail = Some(detail);
        futures_util::pin_mut!(future);
        poll_fn(|cx| {
            checked_now(time, previous)?;
            if let Poll::Ready(result) = timer.as_mut().poll(cx) {
                return Poll::Ready(Err(result
                    .err()
                    .unwrap_or_else(|| timeout_error(budget.cause))));
            }
            if budget.deadline.is_expired_at(checked_now(time, previous)?) {
                return Poll::Ready(Err(timeout_error(budget.cause)));
            }
            let output = future.as_mut().poll(cx);
            if budget.deadline.is_expired_at(checked_now(time, previous)?) {
                let mut error = timeout_error(budget.cause);
                if let EdgeError::GatewayTimeout {
                    provider_detail, ..
                } = &mut error
                    && let Poll::Ready(value) = &output
                {
                    *provider_detail = extract_detail.take().and_then(|extract| extract(value));
                }
                return Poll::Ready(Err(error));
            }
            output.map(Ok)
        })
        .await
    }

    fn validate_request_body_length(bytes: &Bytes, maximum: u64) -> Result<(), EdgeError> {
        let length = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
        if length > maximum {
            return Err(EdgeError::payload_too_large(
                edgezero_core::RequestBodyOverflowOrigin::OutboundUpload,
            ));
        }
        Ok(())
    }

    fn budget_remaining(
        budget: DispatchBudget,
        clock: &MonotonicClock,
    ) -> Result<Duration, EdgeError> {
        budget
            .deadline
            .remaining_at(clock.now())
            .map(|remaining| remaining.min(budget.duration))
            .ok_or_else(|| timeout_error(budget.cause))
    }

    fn dispatch_guard(
        started_at: MonotonicInstant,
        budget: DispatchBudget,
        clock: &MonotonicClock,
    ) -> Result<(), EdgeError> {
        dispatch_guard_at(started_at, budget, dispatch_observed_at(started_at, clock)?)
    }

    #[cfg(feature = "test-utils")]
    fn dispatch_observed_at(
        started_at: MonotonicInstant,
        clock: &MonotonicClock,
    ) -> Result<MonotonicInstant, EdgeError> {
        if let Some(delay) = DISPATCH_SLACK_INJECTION.get() {
            return started_at.checked_add(delay).ok_or_else(|| {
                EdgeError::internal(anyhow::anyhow!(
                    "Fastly dispatch test clock exceeded the monotonic instant range"
                ))
            });
        }
        Ok(clock.now())
    }

    #[cfg(not(feature = "test-utils"))]
    #[expect(
        clippy::unnecessary_wraps,
        reason = "the test-utils build can reject an injected monotonic overflow"
    )]
    fn dispatch_observed_at(
        _started_at: MonotonicInstant,
        clock: &MonotonicClock,
    ) -> Result<MonotonicInstant, EdgeError> {
        Ok(clock.now())
    }

    fn dispatch_guard_at(
        started_at: MonotonicInstant,
        budget: DispatchBudget,
        observed_at: MonotonicInstant,
    ) -> Result<(), EdgeError> {
        if budget.deadline.is_expired_at(observed_at) {
            return Err(timeout_error(budget.cause));
        }
        let elapsed = observed_at
            .checked_duration_since(started_at)
            .ok_or_else(|| {
                EdgeError::internal(anyhow::anyhow!(
                    "monotonic clock moved backwards before Fastly SDK dispatch"
                ))
            })?;
        if elapsed > BATCH_DISPATCH_SLACK_MAX {
            return Err(EdgeError::internal(anyhow::anyhow!(DISPATCH_SLACK_MESSAGE)));
        }
        Ok(())
    }

    #[cfg(test)]
    mod tests {
        use std::cell::Cell;
        use std::collections::{BTreeSet, VecDeque};
        use std::io::{Error, ErrorKind};
        use std::mem::discriminant;
        use std::num::NonZeroU64;
        use std::sync::{Arc, Mutex};
        use std::thread;

        use edgezero_core::error::{BudgetSource, ResponseLimitReason};
        use edgezero_core::http::header::{
            CONNECTION, CONTENT_ENCODING, CONTENT_LENGTH, CONTENT_TYPE,
        };
        use edgezero_core::outbound::{ContentTypeIneligibility, ContentTypeProvenance};
        use edgezero_core::time::Deadline;
        use flate2::{Compression, write::GzEncoder};
        use futures::executor::block_on;
        use futures_util::stream;

        use super::*;

        fn scripted_clock(script: Vec<MonotonicInstant>) -> MonotonicClock {
            let observations = Arc::new(Mutex::new(VecDeque::from(script)));
            MonotonicClock::new(move || {
                observations
                    .lock()
                    .expect("clock observations")
                    .pop_front()
                    .expect("clock observation")
            })
        }

        fn constant_clock(now: MonotonicInstant) -> MonotonicClock {
            MonotonicClock::new(move || now)
        }

        fn clock_budget(start: MonotonicInstant, duration: Duration) -> DispatchBudget {
            DispatchBudget {
                cause: BudgetSource::PerCallTimeout,
                deadline: Deadline::at_instant(start.checked_add(duration).expect("deadline")),
                duration,
            }
        }

        #[test]
        fn image_optimizer_response_processing_rejects_late_handoff() {
            for (method, status) in [
                (Method::HEAD, StatusCode::OK),
                (Method::GET, StatusCode::NO_CONTENT),
                (Method::GET, StatusCode::OK),
            ] {
                let start = MonotonicInstant::now();
                let budget = clock_budget(start, Duration::from_secs(1));
                let clock = scripted_clock(vec![start, budget.deadline.instant()]);
                let response = FastlyResponse::from_status(status);
                let result = block_on(process_response(
                    response,
                    method,
                    OutboundProxyMarker::Off,
                    ResponseDecoding::Decode,
                    ResponseMode::Streamed,
                    budget,
                    0x0010_0000,
                    24,
                    None,
                    Some(16),
                    Some(16),
                    Some(1024),
                    Some(10),
                    native_time(clock.clone()),
                    &NativeProgress::new(NativeTransport, clock, start, Some(budget.deadline)),
                ));
                assert!(
                    matches!(
                        result,
                        Err(EdgeError::GatewayTimeout {
                            cause: BudgetSource::PerCallTimeout,
                            ..
                        })
                    ),
                    "late finalization must not hand off a successful response"
                );
            }
        }

        #[test]
        fn image_optimizer_response_uses_shared_limits_and_lazy_stream() {
            use std::sync::atomic::{AtomicUsize, Ordering};
            for (mode, encoded, decoded, expected) in [
                (ResponseMode::Streamed, 8, 8, None),
                (
                    ResponseMode::Streamed,
                    7,
                    8,
                    Some(ResponseLimitReason::EncodedBody),
                ),
                (
                    ResponseMode::Streamed,
                    8,
                    7,
                    Some(ResponseLimitReason::DecodedBody),
                ),
                (
                    ResponseMode::Buffered { max_bytes: 7 },
                    8,
                    8,
                    Some(ResponseLimitReason::BufferedBody),
                ),
            ] {
                let start = MonotonicInstant::now();
                let budget = clock_budget(start, Duration::from_secs(1));
                let samples = Arc::new(AtomicUsize::new(0));
                let observed = Arc::clone(&samples);
                let clock = MonotonicClock::new(move || {
                    observed.fetch_add(1, Ordering::SeqCst);
                    start
                });
                let native = FastlyResponse::from_body("12345678");
                let processed = block_on(process_response(
                    native,
                    Method::GET,
                    OutboundProxyMarker::Off,
                    ResponseDecoding::Decode,
                    mode,
                    budget,
                    0x0010_0000,
                    24,
                    None,
                    Some(decoded),
                    Some(encoded),
                    Some(1024),
                    Some(10),
                    native_time(clock.clone()),
                    &NativeProgress::new(NativeTransport, clock, start, Some(budget.deadline)),
                ));
                let outcome = match processed {
                    Ok(response) => {
                        assert!(response.body().is_stream());
                        assert_eq!(
                            samples.load(Ordering::SeqCst),
                            2,
                            "no native body read before handoff"
                        );
                        block_on(response.into_bytes_bounded(8))
                    }
                    Err(error) => Err(error),
                };
                if let Some(expected_reason) = expected {
                    assert!(
                        matches!(outcome, Err(EdgeError::ResponseTooLarge { reason, .. }) if reason == expected_reason)
                    );
                } else {
                    assert_eq!(outcome.unwrap(), Bytes::from_static(b"12345678"));
                }
            }
        }

        #[test]
        fn method_entry_and_preflight_elapsed_use_the_injected_clock() {
            let start = MonotonicInstant::now();
            let completed = start
                .checked_add(Duration::from_millis(9))
                .expect("completed instant");
            let client = FastlyOutboundClient::with_time(native_time(scripted_clock(vec![
                start, completed,
            ])));
            let request = OutboundRequest::get("https://example.com/")
                .expect("request")
                .stream_response();

            let results = block_on(
                client
                    .start_batch_until(
                        vec![request],
                        Deadline::at_instant(
                            start
                                .checked_add(Duration::from_secs(1))
                                .expect("batch cutoff"),
                        ),
                    )
                    .collect(),
            )
            .expect("valid batch driver");
            let slot = results.slots[0].as_ref().expect("resolved slot");

            assert_eq!(slot.elapsed, Duration::from_millis(9));
            assert!(matches!(slot.outcome, Err(EdgeError::BadRequest { .. })));
        }

        #[test]
        fn batch_reports_per_slot_elapsed() {
            let start = MonotonicInstant::now();
            let first = start
                .checked_add(Duration::from_millis(3))
                .expect("first completion");
            let second = start
                .checked_add(Duration::from_millis(8))
                .expect("second completion");
            let client = FastlyOutboundClient::with_time(native_time(scripted_clock(vec![
                start, first, second,
            ])));
            let requests = vec![
                OutboundRequest::get("https://example.com/")
                    .expect("request")
                    .body("invalid GET body"),
                OutboundRequest::get("https://example.com/")
                    .expect("request")
                    .stream_response(),
            ];

            let results = block_on(
                client
                    .start_batch_until(
                        requests,
                        Deadline::at_instant(
                            start
                                .checked_add(Duration::from_secs(1))
                                .expect("batch cutoff"),
                        ),
                    )
                    .collect(),
            )
            .expect("valid batch driver");
            let first_slot = results.slots[0].as_ref().expect("first resolved slot");
            let second_slot = results.slots[1].as_ref().expect("second resolved slot");

            assert_eq!(first_slot.elapsed, Duration::from_millis(3));
            assert_eq!(second_slot.elapsed, Duration::from_millis(8));
            assert!(
                results
                    .slots
                    .iter()
                    .flatten()
                    .all(|slot| matches!(slot.outcome, Err(EdgeError::BadRequest { .. })))
            );
        }

        #[test]
        fn one_slot_batch_matches_send() {
            let now = MonotonicInstant::now();
            let invalid = || {
                OutboundRequest::get("https://example.com/")
                    .expect("request")
                    .body("invalid GET body")
            };
            let single_outcome = block_on(
                FastlyOutboundClient::with_time(native_time(constant_clock(now))).send(invalid()),
            );
            let batch = block_on(
                FastlyOutboundClient::with_time(native_time(constant_clock(now)))
                    .start_batch_until(
                        vec![invalid()],
                        Deadline::at_instant(
                            now.checked_add(Duration::from_secs(1))
                                .expect("batch cutoff"),
                        ),
                    )
                    .collect(),
            )
            .expect("valid batch driver");
            let batched_outcome = batch
                .slots
                .into_iter()
                .next()
                .flatten()
                .expect("slot")
                .outcome;

            let (Err(single_error), Err(batched_error)) = (single_outcome, batched_outcome) else {
                panic!("single and batch paths must return errors");
            };
            assert_eq!(discriminant(&single_error), discriminant(&batched_error));
            assert_eq!(single_error.status(), batched_error.status());
            assert_eq!(single_error.message(), batched_error.message());
        }

        #[test]
        fn backwards_clock_fails_slot_without_invalid_elapsed() {
            let start = MonotonicInstant::now();
            let earlier = start
                .checked_sub(Duration::from_millis(1))
                .expect("earlier instant");
            let client =
                FastlyOutboundClient::with_time(native_time(scripted_clock(vec![start, earlier])));
            let request = OutboundRequest::get("https://example.com/")
                .expect("request")
                .stream_response();

            let results = block_on(
                client
                    .start_batch_until(
                        vec![request],
                        Deadline::at_instant(
                            start
                                .checked_add(Duration::from_secs(1))
                                .expect("batch cutoff"),
                        ),
                    )
                    .collect(),
            )
            .expect("valid batch driver");
            let slot = results.slots[0].as_ref().expect("resolved slot");

            assert_eq!(slot.elapsed, Duration::ZERO);
            assert!(matches!(slot.outcome, Err(EdgeError::Internal { .. })));
        }

        #[test]
        fn backwards_clock_cannot_expand_the_selected_budget() {
            let start = MonotonicInstant::now();
            let budget = clock_budget(start, Duration::from_millis(10));
            let earlier = start
                .checked_sub(Duration::from_millis(5))
                .expect("earlier instant");
            let clock = scripted_clock(vec![earlier]);

            assert_eq!(
                budget_remaining(budget, &clock).expect("remaining budget"),
                budget.duration
            );
        }

        #[test]
        fn backend_identity_uses_the_method_entry_budget_snapshot() {
            let start = MonotonicInstant::now();
            let budget = clock_budget(start, Duration::from_millis(10));
            let request = OutboundRequest::get("https://example.com/").expect("request");

            let (identity, _) = backend_identity(request, budget).expect("backend identity");

            assert_eq!(identity.phase_ms, 16);
        }

        #[test]
        fn b31_identity_ignores_shorter_deadlines_with_equal_timeouts() {
            let start = MonotonicInstant::now();
            let mut identities = Vec::new();
            for milliseconds in [511_u64, 513, 640, 655] {
                let request = OutboundRequest::get("https://example.com/path")
                    .expect("request")
                    .timeout(Duration::from_millis(800))
                    .deadline(Deadline::at_instant(
                        start
                            .checked_add(Duration::from_millis(milliseconds))
                            .expect("deadline"),
                    ));
                let budget = dispatch_budget(&request, start, None).expect("selected budget");
                assert_eq!(budget.duration, Duration::from_millis(milliseconds));
                assert_eq!(budget.cause, BudgetSource::RequestDeadline);
                identities.push(backend_identity(request, budget).expect("identity").0);
            }
            let first = identities.first().expect("first identity");
            for identity in &identities {
                assert_eq!(identity, first);
                assert_eq!(identity.phase_ms, 1024);
            }
        }

        #[test]
        fn b31_timeout_anchor_is_clamped_and_independent_of_cutoff() {
            let start = MonotonicInstant::now();
            for timeout in [
                Duration::from_nanos(1),
                Duration::from_millis(800),
                Duration::from_secs(1),
                Duration::from_secs(15),
                DEADLINE_FAR_FUTURE,
                Duration::MAX,
            ] {
                let anchor = timeout.min(DEADLINE_FAR_FUTURE);
                let mut names = BTreeSet::new();
                for selected in BTreeSet::from([
                    Duration::from_nanos(1),
                    anchor,
                    anchor
                        .checked_div(2)
                        .unwrap_or(anchor)
                        .max(Duration::from_nanos(1)),
                ]) {
                    let request = OutboundRequest::get("https://example.com/")
                        .expect("request")
                        .timeout(timeout);
                    let observation_cutoff =
                        Deadline::at_instant(start.checked_add(selected).expect("cutoff"));
                    let budget =
                        dispatch_budget(&request, start, Some(observation_cutoff)).expect("budget");
                    assert_eq!(budget.duration, selected);
                    assert_eq!(budget.deadline.instant(), observation_cutoff.instant());
                    assert_eq!(budget.cause, BudgetSource::BatchCutoff);
                    let (identity, parts) = backend_identity(request, budget).expect("identity");
                    assert_eq!(parts.timeout, Some(timeout));
                    assert!(Duration::from_millis(identity.phase_ms) >= anchor);
                    assert!(Duration::from_millis(identity.phase_ms) <= DEADLINE_FAR_FUTURE);
                    names.insert(backend_name(&identity));
                }
                assert_eq!(names.len(), 1);
            }
        }

        #[test]
        fn b31_identity_omits_nontransport_policies_and_preserves_owned_parts() {
            let start = MonotonicInstant::now();
            let budget = clock_budget(start, Duration::from_millis(800));
            let first_request = OutboundRequest::get("https://example.com/first?key=one")
                .expect("request")
                .timeout(Duration::from_millis(800));
            let second_request = OutboundRequest::post("https://example.com/second?key=two")
                .expect("request")
                .timeout(Duration::from_millis(800))
                .body("payload")
                .max_response_bytes(64)
                .proxy_marker(OutboundProxyMarker::Off)
                .response_decoding(ResponseDecoding::Original)
                .cache_policy(OutboundCachePolicy::Bypass);
            let (first, _) = backend_identity(first_request, budget).expect("identity");
            let (second, parts) = backend_identity(second_request, budget).expect("identity");
            assert_eq!(first, second);
            assert_eq!(backend_name(&first), backend_name(&second));
            assert_eq!(parts.body.as_bytes(), Some(&b"payload"[..]));
            assert_eq!(parts.method, Method::POST);
            assert_eq!(parts.proxy_marker, OutboundProxyMarker::Off);
            assert_eq!(parts.response_decoding, ResponseDecoding::Original);
            assert_eq!(
                parts.response_mode,
                ResponseMode::Buffered { max_bytes: 64 }
            );
            assert_eq!(parts.cache_policy, OutboundCachePolicy::Bypass);
        }

        #[test]
        fn b31_deadline_only_boundaries_have_thirty_one_configurations() {
            let maximum = Duration::from_hours(168);
            let ceilings = (0_u32..30)
                .map(|shift| Duration::from_millis(1_u64 << shift))
                .chain([maximum])
                .collect::<Vec<_>>();
            let mut samples = BTreeSet::from([Duration::from_nanos(1), maximum]);
            for ceiling in &ceilings {
                for sample in [
                    ceiling.checked_sub(Duration::from_nanos(1)),
                    Some(*ceiling),
                    ceiling.checked_add(Duration::from_nanos(1)),
                ]
                .into_iter()
                .flatten()
                {
                    if !sample.is_zero() && sample <= maximum {
                        samples.insert(sample);
                    }
                }
            }
            assert_eq!(samples.len(), 93);
            let start = MonotonicInstant::now();
            let mut names = BTreeSet::new();
            for sample in samples {
                let request = OutboundRequest::get("https://example.com/").expect("request");
                let budget = clock_budget(start, sample);
                let (identity, _) = backend_identity(request, budget).expect("identity");
                let expected = ceilings
                    .iter()
                    .find(|ceiling| **ceiling >= sample)
                    .expect("ceiling");
                assert_eq!(Duration::from_millis(identity.phase_ms), *expected);
                names.insert(backend_name(&identity));
            }
            assert_eq!(names.len(), 31);
        }

        #[test]
        fn b31_phase_timers_are_coupled_without_a_rigid_split() {
            for milliseconds in [1_u64, 2, 4, 128, 1024, 604_800_000] {
                let timers = backend_timers(milliseconds);
                let expected = Duration::from_millis(milliseconds);
                assert_eq!(timers.connect, expected);
                assert_eq!(timers.first_byte, expected);
                assert_eq!(timers.between_bytes, expected);
            }
        }

        #[cfg(target_env = "p1")]
        #[test]
        fn b31_independent_clients_share_verified_backend() {
            let start = MonotonicInstant::now();
            let first = FastlyOutboundClient::with_time(native_time(constant_clock(start)));
            let second = FastlyOutboundClient::with_time(native_time(constant_clock(start)));
            let first_request = OutboundRequest::get("http://127.0.0.1:9/b31-client-reuse")
                .expect("request")
                .timeout(Duration::from_millis(800))
                .deadline(Deadline::at_instant(
                    start
                        .checked_add(Duration::from_millis(511))
                        .expect("deadline"),
                ));
            let second_request = OutboundRequest::get("http://127.0.0.1:9/another-path")
                .expect("request")
                .timeout(Duration::from_millis(800))
                .deadline(Deadline::at_instant(
                    start
                        .checked_add(Duration::from_millis(513))
                        .expect("deadline"),
                ));
            let first_prepared = first.prepare(first_request, start).expect("first backend");
            let second_prepared = second
                .prepare(second_request, start)
                .expect("second backend");
            assert_eq!(
                first_prepared.backend.name(),
                second_prepared.backend.name()
            );
            assert_eq!(first_prepared.budget.duration, Duration::from_millis(511));
            assert_eq!(second_prepared.budget.duration, Duration::from_millis(513));
            assert_eq!(first_prepared.budget.cause, BudgetSource::RequestDeadline);
            assert_eq!(second_prepared.budget.cause, BudgetSource::RequestDeadline);
        }

        #[cfg(target_env = "p1")]
        #[test]
        fn b31_registered_transport_preserves_origin_and_wire_authority() {
            let start = MonotonicInstant::now();
            let client = FastlyOutboundClient::with_time(native_time(constant_clock(start)));
            for (target, override_authority, host, authority, port, tls) in [
                (
                    "https://api.example:8443/",
                    Some("tenant.example:9443"),
                    "api.example",
                    "tenant.example:9443",
                    8443,
                    true,
                ),
                (
                    "http://[::1]:8443/",
                    None,
                    "[::1]",
                    "[::1]:8443",
                    8443,
                    false,
                ),
                (
                    "https://127.0.0.1:8443/",
                    None,
                    "127.0.0.1",
                    "127.0.0.1:8443",
                    8443,
                    true,
                ),
            ] {
                let mut request = OutboundRequest::get(target).expect("request");
                if let Some(value) = override_authority {
                    request = request.host_authority_override(value).expect("authority");
                }
                let prepared = client.prepare(request, start).expect("registered backend");
                assert_eq!(prepared.backend.get_host(), host);
                assert_eq!(
                    prepared.backend.get_host_override(),
                    Some(HeaderValue::from_str(authority).expect("header"))
                );
                assert_eq!(prepared.backend.get_port(), port);
                assert_eq!(prepared.backend.is_ssl(), tls);
            }
        }

        #[test]
        fn b31_registry_reuses_matching_identity_and_refuses_a_collision() {
            let start = MonotonicInstant::now();
            let budget = clock_budget(start, Duration::from_secs(1));
            let clock = constant_clock(start);
            let request = OutboundRequest::get("https://example.com/").expect("request");
            let (identity, _) = backend_identity(request, budget).expect("identity");
            let registry = Mutex::new(HashMap::new());
            let registered = Cell::new(0_usize);
            for _ in 0_u8..2 {
                let backend = cached_backend(
                    &registry,
                    identity.clone(),
                    budget,
                    &clock,
                    |name, settings| {
                        assert_eq!(name, backend_name(settings));
                        registered.set(registered.get() + 1);
                        Ok(Ok(7_u8))
                    },
                )
                .expect("verified backend");
                assert_eq!(backend, 7);
            }
            assert_eq!(registered.get(), 1);
            let mut collision = identity.clone();
            collision.wire_authority = "other.example".to_owned();
            registry
                .lock()
                .expect("registry")
                .insert(backend_name(&identity), (collision, 9));
            let error = cached_backend(&registry, identity, budget, &clock, |_, _| {
                panic!("collision must not register or reuse")
            })
            .expect_err("identity collision");
            assert!(matches!(error, EdgeError::Internal { .. }));
        }

        #[test]
        fn b31_registry_does_not_trust_failed_registrations() {
            let start = MonotonicInstant::now();
            let budget = clock_budget(start, Duration::from_secs(1));
            let clock = constant_clock(start);
            let request = OutboundRequest::get("https://example.com/").expect("request");
            let (identity, _) = backend_identity(request, budget).expect("identity");
            for failure in [
                BackendCreationError::NameInUse,
                BackendCreationError::Disallowed,
            ] {
                let registry = Mutex::new(HashMap::new());
                cached_backend::<u8, _>(&registry, identity.clone(), budget, &clock, |_, _| {
                    Ok(Err(failure))
                })
                .expect_err("unverified registration must fail");
                assert!(registry.lock().expect("registry").is_empty());
                let backend =
                    cached_backend(&registry, identity.clone(), budget, &clock, |_, _| {
                        Ok(Ok(7))
                    })
                    .expect("retry may register");
                assert_eq!(backend, 7);
            }
        }

        #[test]
        fn b31_registry_retains_late_success_and_rechecks_cached_deadlines() {
            let start = MonotonicInstant::now();
            let budget = clock_budget(start, Duration::from_secs(1));
            let request = OutboundRequest::get("https://example.com/").expect("request");
            let (identity, _) = backend_identity(request, budget).expect("identity");
            let registry = Mutex::new(HashMap::new());
            let clock = scripted_clock(vec![start, budget.deadline.instant()]);
            let error = cached_backend(&registry, identity.clone(), budget, &clock, |_, _| {
                Ok(Ok(7_u8))
            })
            .expect_err("late registration");
            assert!(matches!(
                error,
                EdgeError::GatewayTimeout {
                    cause: BudgetSource::PerCallTimeout,
                    ..
                }
            ));
            assert_eq!(registry.lock().expect("registry").len(), 1);
            let expired_clock = constant_clock(budget.deadline.instant());
            cached_backend(
                &registry,
                identity.clone(),
                budget,
                &expired_clock,
                |_, _| panic!("expired cache hit must not register"),
            )
            .expect_err("cached backend does not renew an expired budget");
            let next_budget = clock_budget(budget.deadline.instant(), Duration::from_secs(1));
            let backend =
                cached_backend(&registry, identity, next_budget, &expired_clock, |_, _| {
                    panic!("successful late registration must already be retained")
                })
                .expect("reuse under a new operation's budget");
            assert_eq!(backend, 7);
        }

        #[test]
        fn adapter_final_dispatch_reapplies_request_normalization() {
            let mut request = OutboundRequest::get("https://example.com/").expect("request");
            request
                .headers_mut()
                .insert(CONNECTION, HeaderValue::from_static("accept-encoding"));
            request
                .headers_mut()
                .insert(ACCEPT_ENCODING, HeaderValue::from_static("gzip"));

            normalize_fastly_request(&mut request).expect("normalized request");

            assert!(!request.headers().contains_key(CONNECTION));
            assert_eq!(
                request.headers().get(ACCEPT_ENCODING),
                Some(&HeaderValue::from_static("identity"))
            );
        }

        #[test]
        fn canonical_uri_wire_serialization_table() {
            for raw in [
                "https://example.com",
                "https://example.com/a/../b?x=%2F",
                "http://127.0.0.1:8080/path?empty=",
                "https://[::1]:8443/",
                "https://xn--bcher-kva.example/catalog",
            ] {
                let request = OutboundRequest::get(raw).expect("canonical request");
                let native = build_fastly_request(
                    request.method(),
                    request.uri(),
                    request.headers(),
                    OutboundCachePolicy::PlatformDefault,
                );

                assert_eq!(native.get_url_str(), request.uri().to_string(), "{raw}");
            }
        }

        #[test]
        fn backend_builder_keeps_pooling_for_identical_identity_and_settings() {
            let start = MonotonicInstant::now();
            let budget = clock_budget(start, Duration::from_millis(100));
            let first = OutboundRequest::get("https://example.com/path").expect("first request");
            let second = OutboundRequest::get("https://example.com/other").expect("second request");
            let (first_identity, _) = backend_identity(first, budget).expect("first identity");
            let (second_identity, _) = backend_identity(second, budget).expect("second identity");

            assert_eq!(first_identity, second_identity);
            assert_eq!(
                backend_name(&first_identity),
                backend_name(&second_identity)
            );
            let first_timers = backend_timers(first_identity.phase_ms);
            let second_timers = backend_timers(second_identity.phase_ms);
            assert_eq!(first_timers.connect, second_timers.connect);
            assert_eq!(first_timers.first_byte, second_timers.first_byte);
            assert_eq!(first_timers.between_bytes, second_timers.between_bytes);
        }

        #[cfg(target_env = "p1")]
        #[test]
        fn proposed_b31_timer_vectors_register() {
            let register = |name: &str, connect, first_byte, between_bytes| {
                Backend::builder(name, "127.0.0.1:9")
                    .connect_timeout(connect)
                    .first_byte_timeout(first_byte)
                    .between_bytes_timeout(between_bytes)
                    .finish()
            };
            let ceilings = (0_u32..30).map(|shift| 1_u64 << shift).chain([604_800_000]);
            let mut registered = 0_usize;
            for milliseconds in ceilings {
                let phase = Duration::from_millis(milliseconds);
                for (scope, connect) in
                    [("coupled", phase), ("explicit", Duration::from_millis(200))]
                {
                    let name = format!("ez-b31-probe-{scope}-{milliseconds}");
                    let backend = register(&name, connect, phase, phase)
                        .expect("proposed timer vector must register through the pinned SDK");
                    assert_eq!(backend.name(), name);
                    assert!(backend.exists(), "successful registration must be visible");
                    registered += 1;
                }
            }
            assert_eq!(registered, 62);

            let valid = Duration::from_millis(200);
            let oversized = Duration::from_millis(u64::from(u32::MAX) + 1);
            assert!(matches!(
                register("ez-b31-probe-overflow-connect", oversized, valid, valid),
                Err(BackendCreationError::ConnectTimeoutTooLarge(value)) if value == oversized
            ));
            assert!(matches!(
                register("ez-b31-probe-overflow-first", valid, oversized, valid),
                Err(BackendCreationError::FirstByteTimeoutTooLarge(value)) if value == oversized
            ));
            assert!(matches!(
                register("ez-b31-probe-overflow-between", valid, valid, oversized),
                Err(BackendCreationError::BetweenBytesTimeoutTooLarge(value)) if value == oversized
            ));
        }

        #[test]
        fn authority_override_has_a_distinct_backend_identity() {
            let start = MonotonicInstant::now();
            let budget = clock_budget(start, Duration::from_millis(100));
            let original = OutboundRequest::get("https://origin.example/path").expect("request");
            let overridden = OutboundRequest::get("https://origin.example/path")
                .expect("request")
                .host_authority_override("tenant.example:8443")
                .expect("authority override");

            let (original_identity, _) = backend_identity(original, budget).expect("identity");
            let (overridden_identity, _) = backend_identity(overridden, budget).expect("identity");

            assert_eq!(overridden_identity.host, "origin.example");
            assert_eq!(overridden_identity.port, 443);
            assert_ne!(original_identity, overridden_identity);
            assert_ne!(
                backend_name(&original_identity),
                backend_name(&overridden_identity)
            );
        }

        #[test]
        fn ceil_millis_rounds_up_and_saturates_without_wrapping() {
            assert_eq!(ceil_millis(Duration::ZERO), 1);
            assert_eq!(ceil_millis(Duration::from_nanos(1)), 1);
            assert_eq!(ceil_millis(Duration::from_micros(999)), 1);
            assert_eq!(ceil_millis(Duration::from_millis(1)), 1);
            assert_eq!(ceil_millis(Duration::from_nanos(1_000_001)), 2);
            assert_eq!(ceil_millis(Duration::from_micros(1_001)), 2);
            assert_eq!(ceil_millis(Duration::MAX), u64::MAX);
        }

        #[test]
        fn heterogeneous_hosts_and_timer_buckets_have_distinct_backend_identities() {
            let started_at = MonotonicInstant::now();
            let first_budget = clock_budget(started_at, Duration::from_millis(1));
            let second_budget = clock_budget(started_at, Duration::from_nanos(1_000_001));
            let first_request =
                OutboundRequest::get("https://first.example/path").expect("first request");
            let second_request =
                OutboundRequest::get("https://second.example/path").expect("second request");

            let (first, _) = backend_identity(first_request, first_budget).expect("first identity");
            let (second, _) =
                backend_identity(second_request, second_budget).expect("second identity");

            assert_eq!(first.phase_ms, 1);
            assert_eq!(second.phase_ms, 2);
            assert_ne!(first, second);
            assert_ne!(backend_name(&first), backend_name(&second));
        }

        #[test]
        fn constant_clock_can_be_sampled_without_exhaustion() {
            let now = MonotonicInstant::now();
            let clock = constant_clock(now);

            assert_eq!(clock.now(), now);
            assert_eq!(clock.now(), now);
            assert_eq!(clock.now(), now);
        }

        #[test]
        fn native_body_read_rejects_expiry_before_payload_read() {
            let start = MonotonicInstant::now();
            let budget = clock_budget(start, Duration::from_millis(10));
            let clock = constant_clock(budget.deadline.instant());
            let mut body = fastly_body_stream(FastlyBody::from("body"), budget, clock);

            let error = block_on(body.next())
                .expect("terminal item")
                .expect_err("post-ready expiry");

            assert!(matches!(error, EdgeError::GatewayTimeout { .. }));
        }

        #[test]
        fn deadline_stream_rejects_clock_regression_between_chunks_before_polling_source() {
            use futures_util::stream::poll_fn as stream_poll_fn;
            use std::sync::atomic::{AtomicUsize, Ordering};
            let start = MonotonicInstant::now();
            let budget = clock_budget(start, Duration::from_secs(1));
            let observed = Arc::new(Mutex::new(start));
            let sampled = Arc::clone(&observed);
            let clock = MonotonicClock::new(move || *sampled.lock().expect("clock"));
            let source_now = Arc::clone(&observed);
            let polls = Arc::new(AtomicUsize::new(0));
            let source_polls = Arc::clone(&polls);
            let source = stream_poll_fn(move |_context| {
                source_polls.fetch_add(1, Ordering::SeqCst);
                *source_now.lock().expect("clock") =
                    start.checked_add(Duration::from_millis(20)).expect("later");
                Poll::Ready(Some(Ok(Bytes::from_static(b"chunk"))))
            })
            .boxed_local();
            let progress =
                NativeProgress::new(NativeTransport, clock.clone(), start, Some(budget.deadline));
            let mut guarded = deadline_stream(source, budget, native_time(clock), progress);
            assert_eq!(
                block_on(guarded.next())
                    .expect("first item")
                    .expect("first chunk"),
                Bytes::from_static(b"chunk")
            );
            *observed.lock().expect("clock") = start
                .checked_add(Duration::from_millis(15))
                .expect("regression");
            assert!(matches!(
                block_on(guarded.next()),
                Some(Err(EdgeError::Internal { .. }))
            ));
            assert_eq!(polls.load(Ordering::SeqCst), 1);
        }

        #[test]
        fn deadline_stream_checks_clock_after_ready_bytes_and_eof() {
            use futures_util::stream::poll_fn as stream_poll_fn;
            use std::sync::atomic::{AtomicUsize, Ordering};
            for eof in [false, true] {
                let start = MonotonicInstant::now();
                let budget = clock_budget(start, Duration::from_secs(1));
                let observed = Arc::new(Mutex::new(start));
                let sampled = Arc::clone(&observed);
                let clock = MonotonicClock::new(move || *sampled.lock().expect("clock"));
                let reads = Arc::new(AtomicUsize::new(0));
                let source_reads = Arc::clone(&reads);
                let source = stream_poll_fn(move |_context| {
                    source_reads.fetch_add(1, Ordering::SeqCst);
                    *observed.lock().expect("clock") = budget.deadline.instant();
                    Poll::Ready((!eof).then_some(Ok(Bytes::from_static(b"late"))))
                })
                .boxed_local();
                let progress = NativeProgress::new(
                    NativeTransport,
                    clock.clone(),
                    start,
                    Some(budget.deadline),
                );
                let mut guarded = deadline_stream(source, budget, native_time(clock), progress);
                assert!(matches!(
                    block_on(guarded.next()),
                    Some(Err(EdgeError::GatewayTimeout {
                        cause: BudgetSource::PerCallTimeout,
                        ..
                    }))
                ));
                assert_eq!(
                    reads.load(Ordering::SeqCst),
                    1,
                    "must sample after source completion"
                );
            }
        }

        #[test]
        fn deadline_stream_preserves_source_detail_and_releases_before_terminal_yield() {
            for expired_before_poll in [false, true] {
                let start = MonotonicInstant::now();
                let budget = clock_budget(start, Duration::from_millis(10));
                let observed = Arc::new(Mutex::new(if expired_before_poll {
                    budget.deadline.instant()
                } else {
                    start
                }));
                let source_now = Arc::clone(&observed);
                let clock_now = Arc::clone(&observed);
                let clock = MonotonicClock::new(move || *clock_now.lock().expect("clock"));
                let retained = Arc::new(());
                let source_resource = Arc::clone(&retained);
                let detail = ProviderFailureDetail::Fastly {
                    phase: ProviderFailurePhase::ResponseBodyRead,
                    cause: FastlyFailureCause::IoError {
                        kind: ErrorKind::UnexpectedEof,
                    },
                };
                let source = stream! {
                    let owned_until_source_drop = source_resource;
                    *source_now.lock().expect("clock") = budget.deadline.instant();
                    yield Err(EdgeError::bad_gateway_with_detail("fixture source failed", BadGatewayReason::Transport, detail));
                    drop(owned_until_source_drop);
                }.boxed_local();
                let progress = NativeProgress::new(
                    NativeTransport,
                    clock.clone(),
                    start,
                    Some(budget.deadline),
                );
                let mut guarded = deadline_stream(source, budget, native_time(clock), progress);
                let error = block_on(guarded.next())
                    .expect("terminal item")
                    .expect_err("deadline");
                assert!(matches!(
                    error,
                    EdgeError::GatewayTimeout {
                        cause: BudgetSource::PerCallTimeout,
                        ..
                    }
                ));
                assert_eq!(
                    error.provider_failure_detail(),
                    (!expired_before_poll).then_some(&detail)
                );
                assert_eq!(
                    Arc::strong_count(&retained),
                    1,
                    "terminal stream must not retain source ownership"
                );
                assert!(block_on(guarded.next()).is_none());
                assert_eq!(Arc::strong_count(&retained), 1);
            }
        }

        #[test]
        fn converted_response_retains_injected_clock() {
            let start = MonotonicInstant::now();
            let budget = clock_budget(start, Duration::from_secs(1));
            let observed = Arc::new(Mutex::new(start));
            let clock_observed = Arc::clone(&observed);
            let clock =
                MonotonicClock::new(move || *clock_observed.lock().expect("clock observation"));
            let native = FastlyResponse::from_status(StatusCode::NO_CONTENT.as_u16());
            let response = block_on(process_response(
                native,
                Method::GET,
                OutboundProxyMarker::Inherit,
                ResponseDecoding::Decode,
                ResponseMode::Streamed,
                budget,
                32 * 1024 * 1024,
                24,
                None,
                None,
                None,
                None,
                None,
                native_time(clock.clone()),
                &NativeProgress::new(NativeTransport, clock, start, Some(budget.deadline)),
            ))
            .expect("converted response");
            *observed.lock().expect("clock observation") = budget.deadline.instant();

            let error = block_on(response.into_bytes_bounded_until(1, budget.deadline))
                .expect_err("retained clock reached deadline");

            assert!(matches!(error, EdgeError::GatewayTimeout { .. }));
        }

        type ContentTypeCase = (&'static [&'static [u8]], bool, ContentTypeProvenance, usize);

        fn content_type_cases() -> &'static [ContentTypeCase] {
            use ContentTypeIneligibility::{Missing, MultipleValues, NonText, Removed};
            use ContentTypeProvenance::{Ineligible, SingleTextUnchanged};

            &[
                (&[], false, Ineligible(Missing), 0),
                (&[b"text/plain"], false, SingleTextUnchanged, 1),
                (
                    &[b"text/plain; label=caf\xc3\xa9"],
                    false,
                    SingleTextUnchanged,
                    1,
                ),
                (
                    &[b"text/plain", b"text/plain"],
                    false,
                    Ineligible(MultipleValues),
                    2,
                ),
                (
                    &[b"text/plain", b"application/json"],
                    false,
                    Ineligible(MultipleValues),
                    2,
                ),
                (
                    &[b"text/plain", b"\xff"],
                    false,
                    Ineligible(MultipleValues),
                    1,
                ),
                (&[b"\xff"], false, Ineligible(NonText), 0),
                (
                    &[b"text/plain, application/json"],
                    false,
                    SingleTextUnchanged,
                    1,
                ),
                (&[b"not a MIME type"], false, SingleTextUnchanged, 1),
                (&[b"text/plain"], true, Ineligible(Removed), 0),
            ]
        }

        #[test]
        fn converted_response_preserves_original_content_type_provenance() {
            use ContentTypeIneligibility::Modified;
            use ContentTypeProvenance::{Ineligible, SingleTextUnchanged};

            for &(values, nominated, expected, retained_count) in content_type_cases() {
                for status in [
                    StatusCode::OK,
                    StatusCode::NO_CONTENT,
                    StatusCode::RESET_CONTENT,
                ] {
                    for mode in [
                        ResponseMode::Streamed,
                        ResponseMode::Buffered { max_bytes: 64 },
                    ] {
                        let start = MonotonicInstant::now();
                        let budget = clock_budget(start, Duration::from_secs(1));
                        let mut native = FastlyResponse::from_status(status.as_u16());
                        for value in values {
                            native.append_header(CONTENT_TYPE, *value);
                        }
                        if nominated {
                            native.set_header(CONNECTION, "content-type");
                        }
                        let mut response = block_on(process_response(
                            native,
                            Method::GET,
                            OutboundProxyMarker::Inherit,
                            ResponseDecoding::Decode,
                            mode,
                            budget,
                            1024,
                            24,
                            None,
                            None,
                            None,
                            None,
                            None,
                            native_time(constant_clock(start)),
                            &NativeProgress::new(
                                NativeTransport,
                                constant_clock(start),
                                start,
                                Some(budget.deadline),
                            ),
                        ))
                        .expect("converted response");

                        assert_eq!(response.content_type_provenance(), expected);
                        assert_eq!(
                            response.headers().get_all(CONTENT_TYPE).iter().count(),
                            retained_count
                        );
                        if expected == SingleTextUnchanged {
                            assert_eq!(
                                response
                                    .headers()
                                    .get(CONTENT_TYPE)
                                    .map(HeaderValue::as_bytes),
                                values.first().copied()
                            );
                        }
                        response
                            .headers_mut()
                            .insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
                        let after_mutation = if expected == SingleTextUnchanged {
                            Ineligible(Modified)
                        } else {
                            expected
                        };
                        assert_eq!(response.content_type_provenance(), after_mutation);
                    }
                }
            }
        }

        #[test]
        fn expired_native_body_read_does_not_accept_eof() {
            let start = MonotonicInstant::now();
            let budget = clock_budget(start, Duration::from_millis(10));
            let clock = constant_clock(budget.deadline.instant());
            let mut body = fastly_body_stream(FastlyBody::new(), budget, clock);

            assert!(matches!(
                block_on(body.next()),
                Some(Err(EdgeError::GatewayTimeout {
                    cause: BudgetSource::PerCallTimeout,
                    ..
                }))
            ));
        }

        #[test]
        fn decoder_stalls_timeout_at_all_completion_boundaries() {
            let started_at = MonotonicInstant::now();
            let budget = clock_budget(started_at, Duration::from_millis(100));
            let mut gzip_encoder = GzEncoder::new(Vec::new(), Compression::default());
            gzip_encoder.write_all(b"ab").expect("gzip input");
            let gzip_bytes = gzip_encoder.finish().expect("gzip output");
            let mut native = FastlyResponse::from_status(StatusCode::OK.as_u16());
            native.set_header(CONTENT_ENCODING, "gzip");
            native.set_body(gzip_bytes);

            let response = block_on(process_response(
                native,
                Method::GET,
                OutboundProxyMarker::Inherit,
                ResponseDecoding::Decode,
                ResponseMode::Streamed,
                budget,
                32 * 1024 * 1024,
                24,
                NonZeroU64::new(1),
                None,
                None,
                None,
                None,
                native_time(MonotonicClock::default()),
                &NativeProgress::new(
                    NativeTransport,
                    MonotonicClock::default(),
                    started_at,
                    Some(budget.deadline),
                ),
            ))
            .expect("response head");
            let mut body = response.into_body().into_stream().expect("streamed body");

            assert_eq!(
                block_on(body.next())
                    .expect("first item")
                    .expect("first byte"),
                Bytes::from_static(b"a")
            );
            thread::sleep(Duration::from_millis(125));
            assert!(matches!(
                block_on(body.next()),
                Some(Err(EdgeError::GatewayTimeout {
                    cause: BudgetSource::PerCallTimeout,
                    ..
                }))
            ));
        }

        #[test]
        fn response_content_length_rejects_before_body_poll() {
            let start = MonotonicInstant::now();
            let budget = clock_budget(start, Duration::from_secs(1));
            let mut native = FastlyResponse::from_status(StatusCode::OK.as_u16());
            native.set_header(CONTENT_LENGTH, "2");
            native.set_body("xx");

            let error = block_on(process_response(
                native,
                Method::GET,
                OutboundProxyMarker::Inherit,
                ResponseDecoding::Decode,
                ResponseMode::Buffered { max_bytes: 1 },
                budget,
                32 * 1024 * 1024,
                24,
                None,
                None,
                None,
                None,
                None,
                native_time(constant_clock(start)),
                &NativeProgress::new(
                    NativeTransport,
                    constant_clock(start),
                    start,
                    Some(budget.deadline),
                ),
            ))
            .expect_err("content length exceeds final buffer cap");

            assert!(matches!(
                error,
                EdgeError::ResponseTooLarge {
                    reason: ResponseLimitReason::BufferedBody,
                    ..
                }
            ));
        }

        #[test]
        fn response_content_length_explicit_identity_rejects_before_body_poll() {
            let start = MonotonicInstant::now();
            let budget = clock_budget(start, Duration::from_secs(1));
            let mut native = FastlyResponse::from_status(StatusCode::OK.as_u16());
            native.set_header(CONTENT_ENCODING, "identity");
            native.set_header(CONTENT_LENGTH, "2");
            native.set_body("xx");

            let error = block_on(process_response(
                native,
                Method::GET,
                OutboundProxyMarker::Inherit,
                ResponseDecoding::Decode,
                ResponseMode::Buffered { max_bytes: 16 },
                budget,
                32 * 1024 * 1024,
                24,
                None,
                Some(1),
                None,
                None,
                None,
                native_time(constant_clock(start)),
                &NativeProgress::new(
                    NativeTransport,
                    constant_clock(start),
                    start,
                    Some(budget.deadline),
                ),
            ))
            .expect_err("content length exceeds decoded cap");

            assert!(matches!(
                error,
                EdgeError::ResponseTooLarge {
                    reason: ResponseLimitReason::DecodedBody,
                    ..
                }
            ));
        }

        #[derive(Default)]
        struct UploadEvents {
            finishes: usize,
            flushes: usize,
            waits: usize,
            writes: Vec<Bytes>,
        }

        struct TestUploadWriter {
            events: Arc<Mutex<UploadEvents>>,
        }

        impl StreamedUploadWriter for TestUploadWriter {
            fn finish(self) -> IoResult<()> {
                let mut events = self
                    .events
                    .lock()
                    .map_err(|_poisoned| Error::other("upload events lock poisoned"))?;
                events.finishes = events.finishes.saturating_add(1);
                Ok(())
            }

            fn flush(&mut self) -> IoResult<()> {
                let mut events = self
                    .events
                    .lock()
                    .map_err(|_poisoned| Error::other("upload events lock poisoned"))?;
                events.flushes = events.flushes.saturating_add(1);
                Ok(())
            }

            fn write_all(&mut self, bytes: &[u8]) -> IoResult<()> {
                self.events
                    .lock()
                    .map_err(|_poisoned| Error::other("upload events lock poisoned"))?
                    .writes
                    .push(Bytes::copy_from_slice(bytes));
                Ok(())
            }
        }

        #[test]
        fn streamed_upload_overflow_never_finishes_or_waits() {
            let start = MonotonicInstant::now();
            let budget = clock_budget(start, Duration::from_secs(1));
            let clock = constant_clock(start);
            for len in [7_usize, 8, 9] {
                let events = Arc::new(Mutex::new(UploadEvents::default()));
                let writer = TestUploadWriter {
                    events: Arc::clone(&events),
                };
                let source = stream::iter([
                    Ok(Bytes::from_static(b"xxxx")),
                    Ok(Bytes::from(vec![b'x'; len - 4])),
                ])
                .boxed_local();
                let wait_events = Arc::clone(&events);
                let result = block_on(complete_streamed_exchange(
                    writer,
                    source,
                    8,
                    budget,
                    &clock,
                    move || {
                        wait_events.lock().expect("events").waits += 1;
                        Ok(7_u8)
                    },
                ));
                let observed = events.lock().expect("events");
                if len > 8 {
                    let error = result.expect_err("upload overflow");
                    assert_eq!(error.status(), StatusCode::PAYLOAD_TOO_LARGE);
                    assert_eq!(
                        error.request_body_overflow_origin(),
                        Some(edgezero_core::RequestBodyOverflowOrigin::OutboundUpload)
                    );
                    assert_eq!(observed.finishes, 0);
                    assert_eq!(observed.waits, 0);
                    assert_eq!(observed.writes, [Bytes::from_static(b"xxxx")]);
                } else {
                    result.expect("within cap");
                    assert_eq!(observed.finishes, 1);
                    assert_eq!(observed.waits, 1);
                }
            }
        }

        #[test]
        fn streamed_upload_finishes_exactly_once() {
            let started_at = MonotonicInstant::now();
            let budget = clock_budget(started_at, Duration::from_secs(1));
            let clock = constant_clock(started_at);
            let events = Arc::new(Mutex::new(UploadEvents::default()));
            let writer = TestUploadWriter {
                events: Arc::clone(&events),
            };
            let source = stream::iter([
                Ok(Bytes::from_static(b"first")),
                Ok(Bytes::from_static(b"second")),
            ])
            .boxed_local();
            let wait_events = Arc::clone(&events);

            let outcome = block_on(complete_streamed_exchange(
                writer,
                source,
                32,
                budget,
                &clock,
                move || {
                    wait_events.lock().expect("upload events").waits += 1;
                    Ok(7_u8)
                },
            ));
            let observed = events.lock().expect("upload events");

            assert_eq!(outcome.expect("exchange"), 7);
            assert_eq!(
                observed.writes,
                [Bytes::from_static(b"first"), Bytes::from_static(b"second")]
            );
            assert_eq!(observed.flushes, 2);
            assert_eq!(observed.finishes, 1);
            assert_eq!(observed.waits, 1);
        }

        #[test]
        fn streamed_upload_failure_never_waits() {
            let started_at = MonotonicInstant::now();
            let budget = clock_budget(started_at, Duration::from_secs(1));
            let clock = constant_clock(started_at);
            let events = Arc::new(Mutex::new(UploadEvents::default()));
            let writer = TestUploadWriter {
                events: Arc::clone(&events),
            };
            let source = stream::iter([Err(EdgeError::bad_gateway_with_reason(
                "source failed",
                BadGatewayReason::Transport,
            ))])
            .boxed_local();
            let wait_events = Arc::clone(&events);

            let outcome = block_on(complete_streamed_exchange(
                writer,
                source,
                32,
                budget,
                &clock,
                move || {
                    wait_events.lock().expect("upload events").waits += 1;
                    Ok(7_u8)
                },
            ));
            let observed = events.lock().expect("upload events");

            assert!(matches!(
                outcome,
                Err(EdgeError::BadGateway {
                    reason: BadGatewayReason::Transport,
                    ..
                })
            ));
            assert!(observed.writes.is_empty());
            assert_eq!(observed.finishes, 0);
            assert_eq!(observed.waits, 0);
        }

        #[test]
        fn backend_creation_error_table_is_exhaustive() {
            use fastly_shared::FastlyStatus;

            let mapped = map_backend_creation_error(&BackendCreationError::Disallowed);
            assert_eq!(mapped.status(), StatusCode::BAD_GATEWAY);
            assert!(matches!(
                mapped,
                EdgeError::BadGateway {
                    message,
                    reason: BadGatewayReason::Unspecified,
                    ..
                } if message == DYNAMIC_BACKENDS_DISABLED_MESSAGE
            ));

            for local_invariant in [
                BackendCreationError::BetweenBytesTimeoutTooLarge(Duration::from_secs(1)),
                BackendCreationError::ConnectTimeoutTooLarge(Duration::from_secs(1)),
                BackendCreationError::EncodingError(
                    String::from_utf8(vec![0xff]).expect_err("invalid UTF-8"),
                ),
                BackendCreationError::FirstByteTimeoutTooLarge(Duration::from_secs(1)),
                BackendCreationError::InvalidHealthcheckValue("invalid".to_owned()),
                BackendCreationError::NameTooLong("x".to_owned()),
                BackendCreationError::NameInUse,
            ] {
                assert!(matches!(
                    map_backend_creation_error(&local_invariant),
                    EdgeError::Internal { .. }
                ));
            }

            assert!(matches!(
                map_backend_creation_error(&BackendCreationError::HostError(FastlyStatus::ERROR)),
                EdgeError::BadGateway {
                    reason: BadGatewayReason::Unspecified,
                    ..
                }
            ));
        }

        #[test]
        #[expect(
            clippy::too_many_lines,
            reason = "one exhaustive pinned SDK table checks all causes and unchanged wire behavior"
        )]
        fn provider_diagnostics_cover_send_causes_without_changing_public_errors() {
            use edgezero_core::error_detail::{
                FastlyFailureCause, ProviderFailureDetail, ProviderFailurePhase,
            };
            use edgezero_core::response::IntoResponse as _;
            use fastly_shared::FastlyStatus;

            macro_rules! unit_causes {
                ($($variant:ident),* $(,)?) => { vec![$((SendErrorCause::$variant, FastlyFailureCause::$variant)),*] };
            }
            let mut cases = unit_causes![
                DnsTimeout,
                DestinationNotFound,
                DestinationUnavailable,
                DestinationIpUnroutable,
                ConnectionRefused,
                ConnectionTerminated,
                ConnectionTimeout,
                ConnectionLimitReached,
                TlsProtocolError,
                TlsCertificateError,
                TlsConfigurationError,
                HttpIncompleteResponse,
                HttpResponseHeaderSectionTooLarge,
                HttpResponseBodyTooLarge,
                HttpResponseTimeout,
                HttpResponseStatusInvalid,
                HttpUpgradeFailed,
                HttpProtocolError,
                HttpRequestCacheKeyInvalid,
                HttpRequestUriInvalid,
                HttpCacheLimitExceeded,
                HttpCacheApiUnsupported,
                FanoutNotEnabled,
                ImageOptimizerUnsupported,
                RequestCollapse,
            ];
            cases.extend([
                (
                    SendErrorCause::DnsError {
                        rcode: Some(u16::MAX),
                        info_code: Some(u16::MAX),
                    },
                    FastlyFailureCause::DnsError {
                        rcode: Some(u16::MAX),
                        info_code: Some(u16::MAX),
                    },
                ),
                (
                    SendErrorCause::DnsError {
                        rcode: None,
                        info_code: None,
                    },
                    FastlyFailureCause::DnsError {
                        rcode: None,
                        info_code: None,
                    },
                ),
                (
                    SendErrorCause::TlsAlertReceived {
                        alert_id: Some(u8::MAX),
                    },
                    FastlyFailureCause::TlsAlertReceived {
                        alert_id: Some(u8::MAX),
                    },
                ),
                (
                    SendErrorCause::TlsAlertReceived { alert_id: None },
                    FastlyFailureCause::TlsAlertReceived { alert_id: None },
                ),
                (
                    SendErrorCause::Http2StreamError {
                        frame_type: u8::MAX,
                        error_code: u32::MAX,
                    },
                    FastlyFailureCause::Http2StreamError {
                        frame_type: u8::MAX,
                        error_code: u32::MAX,
                    },
                ),
                (
                    SendErrorCause::InternalError(Some(FastlyStatus::ERROR)),
                    FastlyFailureCause::InternalError {
                        status: Some(FastlyStatus::ERROR.code),
                    },
                ),
                (
                    SendErrorCause::InternalError(None),
                    FastlyFailureCause::InternalError { status: None },
                ),
                (
                    SendErrorCause::IoError(Error::new(
                        ErrorKind::UnexpectedEof,
                        "provider-secret-token",
                    )),
                    FastlyFailureCause::IoError {
                        kind: ErrorKind::UnexpectedEof,
                    },
                ),
                (
                    SendErrorCause::Custom(anyhow::anyhow!(
                        "https://example.com/?token=provider-secret-token"
                    )),
                    FastlyFailureCause::Custom,
                ),
            ]);
            let now = MonotonicInstant::now();
            let clock = constant_clock(now);
            for (source, expected) in cases {
                for expired in [false, true] {
                    let mut budget = clock_budget(now, Duration::from_secs(1));
                    if expired {
                        budget.deadline = Deadline::at_instant(now);
                    }
                    let plain =
                        super::super::classify_send_failure(cause_to_failure(&source), budget, now);
                    let mapped = map_send_cause(&source, budget, &clock);
                    assert_eq!(
                        mapped.provider_failure_detail(),
                        Some(&ProviderFailureDetail::Fastly {
                            phase: ProviderFailurePhase::Send,
                            cause: expected,
                        })
                    );
                    assert_eq!(mapped.message(), plain.message());
                    assert_eq!(mapped.to_string(), plain.to_string());
                    assert_eq!(mapped.status(), plain.status());
                    assert_eq!(mapped.kind(), plain.kind());
                    assert!(
                        !format!("{:?}", mapped.provider_failure_detail())
                            .contains("provider-secret-token")
                    );
                    let before = plain.into_response().expect("plain wire");
                    let after = mapped.into_response().expect("mapped wire");
                    assert_eq!(before.status(), after.status());
                    assert_eq!(before.headers(), after.headers());
                    assert_eq!(before.body().as_bytes(), after.body().as_bytes());
                }
            }
        }

        #[test]
        fn provider_diagnostics_keep_backend_registration_and_upload_io() {
            use edgezero_core::error_detail::{
                FastlyFailureCause, ProviderFailureDetail, ProviderFailurePhase,
            };
            use fastly_shared::FastlyStatus;

            let backend =
                map_backend_creation_error(&BackendCreationError::HostError(FastlyStatus::ERROR));
            assert_eq!(
                backend.provider_failure_detail(),
                Some(&ProviderFailureDetail::Fastly {
                    phase: ProviderFailurePhase::BackendRegistration,
                    cause: FastlyFailureCause::BackendHostError {
                        status: FastlyStatus::ERROR.code
                    },
                })
            );
            let now = MonotonicInstant::now();
            let error = resolve_stream_io_result(
                Err(Error::new(ErrorKind::BrokenPipe, "provider-secret-token")),
                clock_budget(now, Duration::from_secs(1)),
                now,
                "Fastly outbound request body write failed",
            )
            .expect_err("upload failure");
            assert_eq!(
                error.provider_failure_detail(),
                Some(&ProviderFailureDetail::Fastly {
                    phase: ProviderFailurePhase::RequestBodyUpload,
                    cause: FastlyFailureCause::IoError {
                        kind: ErrorKind::BrokenPipe
                    },
                })
            );
            assert_eq!(error.message(), "Fastly outbound request body write failed");
        }

        #[test]
        fn backend_creation_failure_observed_at_deadline_is_a_timeout() {
            let started_at = MonotonicInstant::now();
            let budget = super::super::test_budget(started_at, BudgetSource::BatchCutoff);
            let retained = Cell::new(false);
            let clock = scripted_clock(vec![budget.deadline.instant()]);

            let error = finish_backend_creation::<(), _>(
                Err(BackendCreationError::Disallowed),
                budget,
                &clock,
                |_backend| {
                    retained.set(true);
                    Ok(())
                },
            )
            .expect_err("deadline must outrank backend failure");

            assert!(!retained.get());
            assert_eq!(
                error.provider_failure_detail(),
                Some(&ProviderFailureDetail::Fastly {
                    phase: ProviderFailurePhase::BackendRegistration,
                    cause: FastlyFailureCause::BackendDisallowed,
                })
            );
            assert!(matches!(
                error,
                EdgeError::GatewayTimeout {
                    cause: BudgetSource::BatchCutoff,
                    ..
                }
            ));
        }

        #[test]
        fn successful_backend_creation_is_retained_before_late_timeout() {
            let started_at = MonotonicInstant::now();
            let budget = super::super::test_budget(started_at, BudgetSource::BatchCutoff);
            let retained = Cell::new(false);
            let observed_at = Arc::new(Mutex::new(started_at));
            let clock_observation = Arc::clone(&observed_at);
            let clock =
                MonotonicClock::new(move || *clock_observation.lock().expect("clock observation"));

            let error = finish_backend_creation(Ok(7_u8), budget, &clock, |backend| {
                retained.set(true);
                *observed_at.lock().expect("clock observation") = budget.deadline.instant();
                Ok(backend)
            })
            .expect_err("late success must still return the attributed timeout");

            assert!(retained.get());
            assert!(matches!(
                error,
                EdgeError::GatewayTimeout {
                    cause: BudgetSource::BatchCutoff,
                    ..
                }
            ));
        }

        #[test]
        fn late_timeout_outranks_retention_error_after_success_is_retained() {
            let started_at = MonotonicInstant::now();
            let budget = super::super::test_budget(started_at, BudgetSource::BatchCutoff);
            let retained = Cell::new(false);
            let observed_at = Arc::new(Mutex::new(started_at));
            let clock_observation = Arc::clone(&observed_at);
            let clock =
                MonotonicClock::new(move || *clock_observation.lock().expect("clock observation"));

            let error = finish_backend_creation(Ok(7_u8), budget, &clock, |_backend| {
                retained.set(true);
                *observed_at.lock().expect("clock observation") = budget.deadline.instant();
                Err(EdgeError::internal(anyhow::anyhow!("retention failed")))
            })
            .expect_err("late timeout must outrank retention failure");

            assert!(retained.get());
            assert!(matches!(
                error,
                EdgeError::GatewayTimeout {
                    cause: BudgetSource::BatchCutoff,
                    ..
                }
            ));
        }

        #[test]
        fn streamed_write_failure_observed_at_expiry_is_a_timeout() {
            let started_at = MonotonicInstant::now();
            let budget = super::super::test_budget(started_at, BudgetSource::PerCallTimeout);

            let error = resolve_stream_io_result(
                Err(Error::other("write failed")),
                budget,
                budget.deadline.instant(),
                "Fastly outbound request body write failed",
            )
            .expect_err("deadline must outrank transport failure");

            assert_eq!(
                error.provider_failure_detail(),
                Some(&ProviderFailureDetail::Fastly {
                    phase: ProviderFailurePhase::RequestBodyUpload,
                    cause: FastlyFailureCause::IoError {
                        kind: ErrorKind::Other
                    },
                })
            );

            assert!(matches!(
                error,
                EdgeError::GatewayTimeout {
                    cause: BudgetSource::PerCallTimeout,
                    ..
                }
            ));
        }

        #[test]
        fn backend_identity_uses_every_canonical_property() {
            let first = BackendIdentity {
                phase_ms: 128,
                host: "example.com".to_owned(),
                port: 443,
                scheme: "https".to_owned(),
                tls: true,
                wire_authority: "example.com".to_owned(),
            };
            assert_eq!(
                backend_name(&first),
                "ez_b31v1_a165932b532b7ff063ee219444cb3cb093847ee427d4f76b54a68909b2e279fc"
            );
            let mut second = first.clone();
            second.phase_ms = 256;
            assert_ne!(backend_name(&first), backend_name(&second));
            second = first.clone();
            second.port = 8443;
            assert_ne!(backend_name(&first), backend_name(&second));
            second = first.clone();
            second.host = "other.example".to_owned();
            assert_ne!(backend_name(&first), backend_name(&second));
            second = first.clone();
            second.scheme = "http".to_owned();
            assert_ne!(backend_name(&first), backend_name(&second));
            second = first.clone();
            second.tls = false;
            assert_ne!(backend_name(&first), backend_name(&second));
            second = first.clone();
            second.wire_authority = "tenant.example:8443".to_owned();
            assert_ne!(backend_name(&first), backend_name(&second));
        }

        #[test]
        fn backend_authority_preserves_explicit_port() {
            let mut request =
                OutboundRequest::get("https://api.example.com:8443/resource").expect("request");
            normalize_for_dispatch(&mut request).expect("normalize");

            let (identity, _) = backend_identity(
                request,
                clock_budget(MonotonicInstant::now(), Duration::from_secs(1)),
            )
            .expect("identity");
            assert_eq!(identity.wire_authority, "api.example.com:8443");
        }

        #[test]
        fn backend_authority_preserves_bracketed_ipv6() {
            let mut request = OutboundRequest::get("http://[::1]:8443/resource").expect("request");
            normalize_for_dispatch(&mut request).expect("normalize");

            let (identity, _) = backend_identity(
                request,
                clock_budget(MonotonicInstant::now(), Duration::from_secs(1)),
            )
            .expect("identity");
            assert_eq!(identity.wire_authority, "[::1]:8443");
        }

        #[test]
        fn dispatch_guard_checks_expiry_before_slack() {
            use edgezero_core::time::{BATCH_DISPATCH_SLACK_MAX, Deadline};

            let started_at = MonotonicInstant::now();
            let duration = Duration::from_secs(1);
            let budget = DispatchBudget {
                cause: BudgetSource::PerCallTimeout,
                deadline: Deadline::at_instant(
                    started_at.checked_add(duration).expect("deadline instant"),
                ),
                duration,
            };
            let at_limit = started_at
                .checked_add(BATCH_DISPATCH_SLACK_MAX)
                .expect("slack boundary");
            dispatch_guard_at(started_at, budget, at_limit).expect("dispatch at slack boundary");

            let over_limit = at_limit
                .checked_add(Duration::from_nanos(1))
                .expect("past slack boundary");
            assert!(matches!(
                dispatch_guard_at(started_at, budget, over_limit),
                Err(EdgeError::Internal { source, .. })
                    if source.to_string() == DISPATCH_SLACK_MESSAGE
            ));

            let expired_budget = DispatchBudget {
                deadline: Deadline::at_instant(over_limit),
                ..budget
            };
            assert!(matches!(
                dispatch_guard_at(started_at, expired_budget, over_limit),
                Err(EdgeError::GatewayTimeout {
                    cause: BudgetSource::PerCallTimeout,
                    ..
                })
            ));
        }

        #[test]
        fn cold_backend_registration_is_refused_before_more_synchronous_work() {
            use edgezero_core::time::{BATCH_DISPATCH_SLACK_MAX, Deadline};

            let started_at = MonotonicInstant::now();
            let over_limit = started_at
                .checked_add(BATCH_DISPATCH_SLACK_MAX + Duration::from_nanos(1))
                .expect("past slack boundary");
            let budget = DispatchBudget {
                cause: BudgetSource::PerCallTimeout,
                deadline: Deadline::at_instant(
                    started_at
                        .checked_add(Duration::from_secs(1))
                        .expect("deadline instant"),
                ),
                duration: Duration::from_secs(1),
            };
            let clock = scripted_clock(vec![started_at, over_limit]);
            let registrations = Cell::new(0_u8);
            let register = || {
                registrations.set(registrations.get().saturating_add(1));
                Ok::<_, ()>(())
            };

            let first =
                register_backend_with_batch_guard(Some(started_at), budget, &clock, register);
            assert!(matches!(first, Ok(Ok(()))));
            let second =
                register_backend_with_batch_guard(Some(started_at), budget, &clock, register);
            assert!(matches!(
                second,
                Err(EdgeError::Internal { source, .. })
                    if source.to_string() == DISPATCH_SLACK_MESSAGE
            ));
            assert_eq!(registrations.get(), 1);
        }

        #[test]
        #[expect(
            clippy::too_many_lines,
            reason = "the pinned SDK cause table intentionally constructs every known variant"
        )]
        fn send_error_cause_table_preserves_timeout_source() {
            let cases = [
                (
                    SendErrorCause::DnsTimeout,
                    super::super::SendFailure::ProviderTimeout,
                ),
                (
                    SendErrorCause::DnsError {
                        rcode: None,
                        info_code: None,
                    },
                    super::super::SendFailure::Unreachable,
                ),
                (
                    SendErrorCause::DestinationNotFound,
                    super::super::SendFailure::Unreachable,
                ),
                (
                    SendErrorCause::DestinationUnavailable,
                    super::super::SendFailure::Unreachable,
                ),
                (
                    SendErrorCause::DestinationIpUnroutable,
                    super::super::SendFailure::Unreachable,
                ),
                (
                    SendErrorCause::ConnectionRefused,
                    super::super::SendFailure::Unreachable,
                ),
                (
                    SendErrorCause::ConnectionTerminated,
                    super::super::SendFailure::Transport,
                ),
                (
                    SendErrorCause::ConnectionTimeout,
                    super::super::SendFailure::BudgetedTimeout,
                ),
                (
                    SendErrorCause::ConnectionLimitReached,
                    super::super::SendFailure::Unreachable,
                ),
                (
                    SendErrorCause::TlsProtocolError,
                    super::super::SendFailure::Unreachable,
                ),
                (
                    SendErrorCause::TlsCertificateError,
                    super::super::SendFailure::Unreachable,
                ),
                (
                    SendErrorCause::TlsAlertReceived { alert_id: None },
                    super::super::SendFailure::Unreachable,
                ),
                (
                    SendErrorCause::TlsConfigurationError,
                    super::super::SendFailure::Unreachable,
                ),
                (
                    SendErrorCause::HttpIncompleteResponse,
                    super::super::SendFailure::UpstreamProtocol,
                ),
                (
                    SendErrorCause::HttpResponseHeaderSectionTooLarge,
                    super::super::SendFailure::UpstreamProtocol,
                ),
                (
                    SendErrorCause::HttpResponseBodyTooLarge,
                    super::super::SendFailure::UpstreamProtocol,
                ),
                (
                    SendErrorCause::HttpResponseTimeout,
                    super::super::SendFailure::BudgetedTimeout,
                ),
                (
                    SendErrorCause::HttpResponseStatusInvalid,
                    super::super::SendFailure::UpstreamProtocol,
                ),
                (
                    SendErrorCause::HttpUpgradeFailed,
                    super::super::SendFailure::UpstreamProtocol,
                ),
                (
                    SendErrorCause::Http2StreamError {
                        frame_type: 0,
                        error_code: 0,
                    },
                    super::super::SendFailure::UpstreamProtocol,
                ),
                (
                    SendErrorCause::HttpProtocolError,
                    super::super::SendFailure::UpstreamProtocol,
                ),
                (
                    SendErrorCause::HttpRequestCacheKeyInvalid,
                    super::super::SendFailure::LocalInvariant,
                ),
                (
                    SendErrorCause::HttpRequestUriInvalid,
                    super::super::SendFailure::LocalInvariant,
                ),
                (
                    SendErrorCause::HttpCacheLimitExceeded,
                    super::super::SendFailure::LocalInvariant,
                ),
                (
                    SendErrorCause::HttpCacheApiUnsupported,
                    super::super::SendFailure::LocalInvariant,
                ),
                (
                    SendErrorCause::IoError(Error::other("test")),
                    super::super::SendFailure::Transport,
                ),
                (
                    SendErrorCause::FanoutNotEnabled,
                    super::super::SendFailure::Unknown,
                ),
                (
                    SendErrorCause::ImageOptimizerUnsupported,
                    super::super::SendFailure::ImageOptimizerUnavailable,
                ),
                (
                    SendErrorCause::InternalError(None),
                    super::super::SendFailure::PlatformInternal,
                ),
                (
                    SendErrorCause::RequestCollapse,
                    super::super::SendFailure::Unknown,
                ),
                (
                    SendErrorCause::Custom(anyhow::anyhow!("test")),
                    super::super::SendFailure::Unknown,
                ),
            ];

            let observed_at = MonotonicInstant::now();
            let selected = BudgetSource::PerCallTimeout;
            let budget = super::super::test_budget(observed_at, selected);
            for (cause, expected) in cases {
                let failure = cause_to_failure(&cause);
                assert_eq!(failure, expected, "{cause:?}");
                super::super::assert_send_failure_error(
                    failure,
                    &super::super::classify_send_failure(failure, budget, observed_at),
                    selected,
                );
            }
        }
    }
}

#[cfg(all(feature = "fastly", feature = "test-utils"))]
pub use fastly_impl::inject_dispatch_slack_for_test;
#[cfg(feature = "fastly")]
pub(crate) use fastly_impl::native_time;
#[cfg(feature = "fastly")]
pub use fastly_impl::{DYNAMIC_BACKENDS_DISABLED_MESSAGE, FastlyOutboundClient};

#[cfg(any(feature = "fastly", feature = "test-utils"))]
fn validate_batch_request(request: &OutboundRequest) -> Result<(), EdgeError> {
    if request.image_optimizer_options().is_some() {
        return Err(EdgeError::bad_request(
            "image_optimizer is not supported by fastly batches",
        ));
    }
    validate_for_dispatch(request, OutboundSendMode::Batch)
}

#[cfg(any(feature = "fastly", test))]
fn driver_selection_failure(error: EdgeError) -> OutboundBatchDriverEvent {
    OutboundBatchDriverEvent::Failed(error)
}

/// Runs the target-neutral Fastly batch preflight contract in native tests.
///
/// # Errors
/// Returns the same portable validation or batch-shape error as production dispatch.
#[cfg(feature = "test-utils")]
#[inline]
pub fn validate_batch_request_for_test(request: &OutboundRequest) -> Result<(), EdgeError> {
    validate_batch_request(request)
}

#[cfg(any(feature = "fastly", test))]
fn timeout_error(cause: BudgetSource) -> EdgeError {
    EdgeError::gateway_timeout_caused("outbound request deadline expired", cause)
}

#[cfg(test)]
mod send_failure_policy_tests {
    use std::sync::atomic::{AtomicU64, Ordering};

    use edgezero_core::outbound::{
        OutboundBatch, OutboundBatchDriverEvent, OutboundBatchItem, OutboundBatchTermination,
        OutboundSlotResult,
    };
    use futures::executor::block_on;
    use futures_util::stream;

    use super::*;

    #[test]
    fn image_optimizer_sync_call_preserves_original_deadline_and_releases_late_response() {
        use edgezero_core::MonotonicClock;
        use std::sync::{Arc, Mutex};

        struct Release(Arc<AtomicU64>);
        #[expect(
            clippy::missing_trait_methods,
            reason = "Drop::pin_drop is unstable and conflicts with drop"
        )]
        impl Drop for Release {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }
        for cause in [
            BudgetSource::Default,
            BudgetSource::PerCallTimeout,
            BudgetSource::RequestDeadline,
            BudgetSource::BatchCutoff,
        ] {
            for delta in [999_999_999, 1_000_000_000, 1_000_000_001] {
                let start = MonotonicInstant::now();
                let now = Arc::new(Mutex::new(start));
                let samples = Arc::clone(&now);
                let clock = MonotonicClock::new(move || *samples.lock().unwrap());
                let drops = Arc::new(AtomicU64::new(0));
                let released = Arc::clone(&drops);
                let result = synchronous_send_until(test_budget(start, cause), &clock, || {
                    *now.lock().unwrap() = start.checked_add(Duration::from_nanos(delta)).unwrap();
                    Ok(Release(released))
                });
                if delta < 1_000_000_000 {
                    assert!(result.is_ok());
                    assert_eq!(drops.load(Ordering::SeqCst), 0);
                    drop(result);
                } else {
                    assert!(
                        matches!(result, Err(EdgeError::GatewayTimeout { cause: selected, .. }) if selected == cause)
                    );
                }
                assert_eq!(drops.load(Ordering::SeqCst), 1);
            }
        }
    }

    #[test]
    fn image_optimizer_sync_call_preserves_late_error_detail_and_skips_expired_dispatch() {
        use edgezero_core::{
            FastlyFailureCause, MonotonicClock, ProviderFailureDetail, ProviderFailurePhase,
        };
        use std::sync::{Arc, Mutex};

        let start = MonotonicInstant::now();
        let budget = test_budget(start, BudgetSource::RequestDeadline);
        let now = Arc::new(Mutex::new(start));
        let samples = Arc::clone(&now);
        let clock = MonotonicClock::new(move || *samples.lock().unwrap());
        let detail = ProviderFailureDetail::Fastly {
            phase: ProviderFailurePhase::Send,
            cause: FastlyFailureCause::ImageOptimizerUnsupported,
        };
        let error = synchronous_send_until::<(), _>(budget, &clock, || {
            *now.lock().unwrap() = budget.deadline.instant();
            Err(EdgeError::bad_gateway_with_detail(
                "Fastly outbound request failed",
                BadGatewayReason::ImageOptimizerUnavailable,
                detail,
            ))
        })
        .expect_err("late provider failure");
        assert!(matches!(
            error,
            EdgeError::GatewayTimeout {
                cause: BudgetSource::RequestDeadline,
                ..
            }
        ));
        assert_eq!(error.provider_failure_detail(), Some(&detail));
        synchronous_send_until::<(), _>(budget, &clock, || {
            panic!("expired request must not dispatch")
        })
        .expect_err("already expired");
        let unavailable =
            classify_send_failure(SendFailure::ImageOptimizerUnavailable, budget, start);
        assert_eq!(unavailable.message(), "Fastly outbound request failed");
        assert_eq!(unavailable.to_string(), "Fastly outbound request failed");
    }

    #[test]
    fn selection_failure_preserves_its_diagnostic_and_previously_collected_slots() {
        let event = driver_selection_failure(EdgeError::internal(anyhow::anyhow!(
            "Fastly readiness returned an invalid handle index"
        )));
        let OutboundBatchDriverEvent::Failed(_) = &event else {
            panic!("selection failure must become a driver failure event");
        };
        let completed = OutboundBatchItem::new(
            0,
            OutboundSlotResult::new(
                Duration::from_millis(1),
                Err(EdgeError::bad_gateway("completed slot failure")),
            ),
        );
        let batch = OutboundBatch::from_driver(
            2,
            stream::iter([OutboundBatchDriverEvent::Item(completed), event]),
        );
        let failure =
            block_on(batch.collect()).expect_err("selection failure must fail collection");

        assert_eq!(
            failure.error.to_string(),
            "internal error: Fastly readiness returned an invalid handle index"
        );
        assert!(matches!(failure.slots.first(), Some(Some(_))));
        assert!(matches!(failure.slots.get(1), Some(None)));
    }

    #[test]
    fn early_observation_cutoff_attributed_timeout_preserves_a_later_sibling() {
        let started_at = MonotonicInstant::now();
        let observation_cutoff = Deadline::at_instant(
            started_at
                .checked_add(Duration::from_millis(10))
                .expect("cutoff instant"),
        );
        let early_timeout = finish_fastly_batch_observation(
            0,
            started_at,
            started_at
                .checked_add(Duration::from_millis(1))
                .expect("early completion"),
            observation_cutoff,
            Err(timeout_error(BudgetSource::BatchCutoff)),
        );
        let sibling = finish_fastly_batch_observation(
            1,
            started_at,
            started_at
                .checked_add(Duration::from_millis(2))
                .expect("sibling completion"),
            observation_cutoff,
            Err(EdgeError::bad_gateway("sibling completed")),
        );
        let events = [early_timeout, sibling].map(|observation| match observation {
            FastlyBatchObservation::Continue(item) => OutboundBatchDriverEvent::Item(item),
            FastlyBatchObservation::Cutoff => OutboundBatchDriverEvent::Cutoff,
        });
        let batch = OutboundBatch::from_driver(2, stream::iter(events));

        let results = block_on(batch.collect()).expect("valid Fastly batch driver");

        assert_eq!(results.termination, OutboundBatchTermination::Completed);
        assert!(matches!(
            results.slots.first(),
            Some(Some(OutboundSlotResult {
                outcome: Err(EdgeError::GatewayTimeout {
                    cause: BudgetSource::BatchCutoff,
                    ..
                }),
                ..
            }))
        ));
        assert!(matches!(
            results.slots.get(1),
            Some(Some(OutboundSlotResult {
                outcome: Err(EdgeError::BadGateway { .. }),
                ..
            }))
        ));
    }

    fn failures() -> [SendFailure; 9] {
        [
            SendFailure::BudgetedTimeout,
            SendFailure::ImageOptimizerUnavailable,
            SendFailure::LocalInvariant,
            SendFailure::PlatformInternal,
            SendFailure::ProviderTimeout,
            SendFailure::Transport,
            SendFailure::Unknown,
            SendFailure::Unreachable,
            SendFailure::UpstreamProtocol,
        ]
    }

    #[test]
    fn send_failure_policy_maps_every_known_category() {
        let observed_at = MonotonicInstant::now();
        for selected in [
            BudgetSource::PerCallTimeout,
            BudgetSource::BatchCutoff,
            BudgetSource::RequestDeadline,
            BudgetSource::Default,
        ] {
            let budget = test_budget(observed_at, selected);
            for failure in failures() {
                assert_send_failure_error(
                    failure,
                    &classify_send_failure(failure, budget, observed_at),
                    selected,
                );
            }
        }
    }

    #[test]
    fn absolute_deadline_wins_every_send_failure() {
        let started_at = MonotonicInstant::now();
        for selected in [
            BudgetSource::PerCallTimeout,
            BudgetSource::BatchCutoff,
            BudgetSource::RequestDeadline,
            BudgetSource::Default,
        ] {
            let budget = test_budget(started_at, selected);
            let after_deadline = budget
                .deadline
                .instant()
                .checked_add(Duration::from_nanos(1))
                .expect("after deadline");
            for observed_at in [budget.deadline.instant(), after_deadline] {
                for failure in failures() {
                    let error = classify_send_failure(failure, budget, observed_at);
                    if let EdgeError::GatewayTimeout { cause, .. } = error {
                        assert_eq!(cause, selected);
                    } else {
                        panic!("deadline did not win {failure:?} for {selected:?}");
                    }
                }
            }
        }
    }
}
