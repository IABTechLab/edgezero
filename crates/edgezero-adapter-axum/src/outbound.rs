use async_stream::stream;
use async_trait::async_trait;
use bytes::Bytes;
use core::num::NonZeroU64;
use core::time::Duration;
use edgezero_core::body::{Body, BodyStream};
use edgezero_core::error::{BadGatewayReason, BudgetSource, EdgeError};
use edgezero_core::error_detail::{
    AxumFailureCause, AxumFailureKind, ProviderFailureDetail, ProviderFailurePhase,
};
use edgezero_core::http::header::{ACCEPT_ENCODING, HOST};
use edgezero_core::http::{HeaderMap, HeaderValue, Method, StatusCode};
use edgezero_core::outbound::{
    OutboundBatch, OutboundHeaderFidelity, OutboundHeaderInput, OutboundHttpClient,
    OutboundProxyMarker, OutboundRequest, OutboundRequestParts, OutboundResponse,
    ResponseBodyDisposition, ResponseBodyPolicy, ResponseDecoding, ResponseHeaderLimiter,
    ResponseMode, apply_response_body_policy, collect_response_stream, normalize_for_dispatch,
    validate_for_dispatch,
};
use edgezero_core::time::{
    Deadline, DispatchBudget, MonotonicClock, MonotonicInstant, dispatch_budget,
};
use futures_util::StreamExt as _;
use futures_util::future::{FutureExt as _, LocalBoxFuture};
use reqwest::header::HeaderMap as ReqwestHeaderMap;
use reqwest::redirect::Policy;
use std::error::Error as _;
use std::io::Error as IoError;
use std::sync::Arc;
use tokio::time::timeout;

/// Native outbound HTTP implementation used by the Axum adapter.
#[derive(Clone)]
pub struct AxumOutboundClient {
    client: Arc<reqwest::Client>,
    clock: MonotonicClock,
}

struct PreparedRequest {
    budget: DispatchBudget,
    parts: OutboundRequestParts,
}

impl AxumOutboundClient {
    async fn execute(&self, prepared: PreparedRequest) -> Result<OutboundResponse, EdgeError> {
        let PreparedRequest { budget, parts } = prepared;
        let OutboundRequestParts {
            body,
            mut headers,
            host_authority_override,
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

        if !headers.contains_key(ACCEPT_ENCODING) {
            headers.insert(ACCEPT_ENCODING, HeaderValue::from_static("identity"));
        }
        if let Some(authority) = host_authority_override {
            let value = HeaderValue::from_str(authority.as_str()).map_err(EdgeError::internal)?;
            headers.insert(HOST, value);
        }
        let request_body =
            collect_request_body(body, max_request_body_bytes, budget, &self.clock).await?;
        let request_builder = self
            .client
            .request(reqwest_method(&method)?, uri.to_string())
            .headers(headers)
            .body(request_body);
        let request = apply_dispatch_timeout(request_builder, budget, &self.clock)?;
        let response = request
            .send()
            .await
            .map_err(|error| classify_send_error(&error, budget, &self.clock))?;

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
            self.clock.clone(),
        )
        .await
    }

    fn prepare(
        request: OutboundRequest,
        started_at: MonotonicInstant,
    ) -> Result<PreparedRequest, EdgeError> {
        Self::validate_options(&request)?;
        validate_for_dispatch(&request)?;
        Self::prepare_validated(request, started_at, None)
    }

    fn prepare_batch(
        request: OutboundRequest,
        started_at: MonotonicInstant,
        observation_cutoff: Deadline,
    ) -> Result<PreparedRequest, EdgeError> {
        Self::validate_options(&request)?;
        validate_for_dispatch(&request)?;
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
        Self::prepare_validated(request, started_at, Some(observation_cutoff))
    }

    fn prepare_validated(
        mut request: OutboundRequest,
        started_at: MonotonicInstant,
        observation_cutoff: Option<Deadline>,
    ) -> Result<PreparedRequest, EdgeError> {
        let budget = dispatch_budget(&request, started_at, observation_cutoff)?;
        normalize_for_dispatch(&mut request)?;
        Ok(PreparedRequest {
            budget,
            parts: request.into_parts(),
        })
    }

    /// Builds a client with redirects and automatic content decoding disabled.
    ///
    /// # Errors
    /// Returns the underlying client-construction error when TLS initialization fails.
    #[inline]
    pub fn try_new() -> Result<Self, reqwest::Error> {
        Self::try_with_clock(MonotonicClock::default())
    }

    pub(crate) fn try_transport() -> Result<Arc<reqwest::Client>, reqwest::Error> {
        let client = reqwest::Client::builder()
            .redirect(Policy::none())
            .no_brotli()
            .no_deflate()
            .no_gzip()
            .no_zstd()
            .build()?;
        Ok(Arc::new(client))
    }

    /// Builds a client that evaluates every outbound lifetime against `clock`.
    ///
    /// # Errors
    /// Returns the underlying client-construction error when TLS initialization fails.
    #[inline]
    pub fn try_with_clock(clock: MonotonicClock) -> Result<Self, reqwest::Error> {
        Self::try_transport().map(|client| Self { client, clock })
    }

    fn validate_options(request: &OutboundRequest) -> Result<(), EdgeError> {
        if request.image_optimizer_options().is_some() {
            return Err(EdgeError::bad_request(
                "image_optimizer is not supported by axum",
            ));
        }
        Ok(())
    }
}

#[async_trait(?Send)]
impl OutboundHttpClient for AxumOutboundClient {
    #[inline]
    async fn send(&self, request: OutboundRequest) -> Result<OutboundResponse, EdgeError> {
        let started_at = self.clock.now();
        let prepared = Self::prepare(request, started_at)?;
        self.execute(prepared).await
    }

    #[inline]
    fn start_batch_until(
        &self,
        requests: Vec<OutboundRequest>,
        observation_cutoff: Deadline,
    ) -> OutboundBatch {
        let batch_started_at = self.clock.now();
        let slot_count = requests.len();
        if observation_cutoff.is_expired_at(batch_started_at) {
            return OutboundBatch::cutoff(slot_count);
        }

        let mut completed = Vec::new();
        let mut pending_slots = Vec::<LocalBoxFuture<'static, _>>::new();
        for (index, request) in requests.into_iter().enumerate() {
            match Self::prepare_batch(request, batch_started_at, observation_cutoff) {
                Ok(prepared) => {
                    let client = self.clone();
                    pending_slots.push(
                        async move {
                            let outcome = client.execute(prepared).await;
                            (index, client.clock.now(), outcome)
                        }
                        .boxed_local(),
                    );
                }
                Err(error) => completed.push((index, self.clock.now(), Err(error))),
            }
        }

        OutboundBatch::from_futures(
            slot_count,
            batch_started_at,
            observation_cutoff,
            completed,
            pending_slots,
        )
    }
}

async fn collect_request_body(
    body: Body,
    maximum: u64,
    budget: DispatchBudget,
    clock: &MonotonicClock,
) -> Result<Bytes, EdgeError> {
    match body {
        Body::Once(bytes) => {
            let length = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
            if length > maximum {
                return Err(EdgeError::payload_too_large(
                    edgezero_core::RequestBodyOverflowOrigin::OutboundUpload,
                ));
            }
            budget_remaining(budget, clock)?;
            Ok(bytes)
        }
        Body::Stream(mut source) => {
            let mut collected = Vec::new();
            let mut total = 0_u64;
            loop {
                let remaining = budget_remaining(budget, clock)?;
                let next_item = timeout(remaining, source.next())
                    .await
                    .map_err(|_elapsed| timeout_error(budget.cause))?;
                budget_remaining(budget, clock).map_err(|selected| {
                    let detail = next_item
                        .as_ref()
                        .and_then(|item| item.as_ref().err())
                        .and_then(EdgeError::provider_failure_detail)
                        .copied();
                    preserve_timeout_detail(selected, detail)
                })?;
                let Some(item) = next_item else {
                    return Ok(Bytes::from(collected));
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
                collected.extend_from_slice(&bytes);
                total = next_total;
            }
        }
    }
}

fn deadline_stream(
    mut source: BodyStream,
    budget: DispatchBudget,
    clock: MonotonicClock,
) -> BodyStream {
    stream! {
        loop {
            let remaining = match budget_remaining(budget, &clock) {
                Ok(remaining) => remaining,
                Err(error) => {
                    yield Err(error);
                    return;
                }
            };
            let next_item = match timeout(remaining, source.next()).await {
                Ok(item) => item,
                Err(_elapsed) => {
                    yield Err(timeout_error(budget.cause));
                    return;
                }
            };
            if budget_remaining(budget, &clock).is_err() {
                let detail = next_item.as_ref().and_then(|item| item.as_ref().err()).and_then(EdgeError::provider_failure_detail).copied();
                yield Err(preserve_timeout_detail(timeout_error(budget.cause), detail));
                return;
            }
            match next_item {
                Some(Ok(bytes)) => yield Ok(bytes),
                Some(Err(error)) => {
                    yield Err(error);
                    return;
                }
                None => return,
            }
        }
    }
    .boxed_local()
}

#[expect(
    clippy::too_many_arguments,
    reason = "the adapter consumes the independent request policy fields without hiding them"
)]
async fn process_response(
    response: reqwest::Response,
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
    clock: MonotonicClock,
) -> Result<OutboundResponse, EdgeError> {
    budget_remaining(budget, &clock)?;
    let response_clock = clock.clone();
    let status = StatusCode::from_u16(response.status().as_u16()).map_err(EdgeError::internal)?;
    let (mut headers, normalization, _) = OutboundHeaderInput::new(
        "axum",
        copy_headers(response.headers()),
        OutboundHeaderFidelity::AdapterVisibleFields,
    )
    .normalize(
        ResponseHeaderLimiter::new(max_response_header_bytes, max_response_header_count),
        &request_method,
        status,
        proxy_marker,
    )?;
    let disposition = normalization.body_disposition;

    if disposition == ResponseBodyDisposition::FramingBodyless {
        return Ok(OutboundResponse::new(
            request_method,
            status,
            headers,
            Body::empty(),
            response_clock,
            normalization.content_type_provenance,
        ));
    }

    let declared_reset_body = matches!(
        disposition,
        ResponseBodyDisposition::ResetContent {
            declared_body: true
        }
    );
    let native = response_stream(response, budget, clock.clone());
    if matches!(disposition, ResponseBodyDisposition::ResetContent { .. }) {
        if !declared_reset_body {
            let mut reset_stream = deadline_stream(native, budget, clock);
            if let Some(item) = reset_stream.next().await {
                item?;
            }
        }
        return Ok(OutboundResponse::new(
            request_method,
            status,
            headers,
            Body::empty(),
            response_clock,
            normalization.content_type_provenance,
        ));
    }

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
    let deadline_bound = deadline_stream(shaped, budget, clock);
    let body = match response_mode {
        ResponseMode::Buffered { max_bytes } => {
            Body::from(collect_response_stream(deadline_bound, max_bytes).await?)
        }
        ResponseMode::Streamed => Body::from_stream(deadline_bound),
    };
    Ok(OutboundResponse::new(
        request_method,
        status,
        headers,
        body,
        response_clock,
        normalization.content_type_provenance,
    ))
}

fn budget_remaining(budget: DispatchBudget, clock: &MonotonicClock) -> Result<Duration, EdgeError> {
    budget
        .deadline
        .remaining_at(clock.now())
        .map(|remaining| remaining.min(budget.duration))
        .ok_or_else(|| timeout_error(budget.cause))
}

fn classify_send_error(
    error: &reqwest::Error,
    budget: DispatchBudget,
    clock: &MonotonicClock,
) -> EdgeError {
    let classified = if budget.deadline.is_expired_at(clock.now()) || error.is_timeout() {
        timeout_error(budget.cause)
    } else {
        let reason = if error.is_connect() {
            BadGatewayReason::Unreachable
        } else if error.is_builder() {
            BadGatewayReason::Protocol
        } else {
            BadGatewayReason::Transport
        };
        EdgeError::bad_gateway_with_reason("upstream request failed", reason)
    };
    diagnose_reqwest_error(classified, error, ProviderFailurePhase::Send)
}

fn diagnose_reqwest_error(
    mut classified: EdgeError,
    source: &reqwest::Error,
    phase: ProviderFailurePhase,
) -> EdgeError {
    if let EdgeError::BadGateway {
        provider_detail, ..
    }
    | EdgeError::GatewayTimeout {
        provider_detail, ..
    } = &mut classified
    {
        *provider_detail = Some(reqwest_failure_detail(source, phase));
    }
    classified
}

fn reqwest_failure_detail(
    error: &reqwest::Error,
    phase: ProviderFailurePhase,
) -> ProviderFailureDetail {
    let kind = if error.is_builder() {
        AxumFailureKind::Builder
    } else if error.is_redirect() {
        AxumFailureKind::Redirect
    } else if error.is_status() {
        AxumFailureKind::Status
    } else if error.is_request() {
        AxumFailureKind::Request
    } else if error.is_body() {
        AxumFailureKind::Body
    } else if error.is_decode() {
        AxumFailureKind::Decode
    } else if error.is_upgrade() {
        AxumFailureKind::Upgrade
    } else {
        AxumFailureKind::Unknown
    };
    let mut io_kind = None;
    let mut source = error.source();
    for _ in 0_u8..32 {
        let Some(current) = source else {
            break;
        };
        if let Some(io) = current.downcast_ref::<IoError>() {
            io_kind = Some(io.kind());
            break;
        }
        source = current.source();
    }
    ProviderFailureDetail::Axum {
        phase,
        cause: AxumFailureCause {
            io_kind,
            is_connect: error.is_connect(),
            is_timeout: error.is_timeout(),
            kind,
        },
    }
}

fn copy_headers(headers: &ReqwestHeaderMap) -> HeaderMap {
    let mut copied = HeaderMap::with_capacity(headers.len());
    for (name, value) in headers {
        copied.append(name.clone(), value.clone());
    }
    copied
}

fn reqwest_method(method: &Method) -> Result<reqwest::Method, EdgeError> {
    reqwest::Method::from_bytes(method.as_str().as_bytes()).map_err(EdgeError::internal)
}

fn response_stream(
    mut response: reqwest::Response,
    budget: DispatchBudget,
    clock: MonotonicClock,
) -> BodyStream {
    stream! {
        loop {
            match response.chunk().await {
                Ok(Some(bytes)) => yield Ok(bytes),
                Ok(None) => return,
                Err(error) => {
                    let classified = if budget.deadline.is_expired_at(clock.now()) || error.is_timeout() {
                        timeout_error(budget.cause)
                    } else {
                        EdgeError::bad_gateway_with_reason(
                            "upstream response body failed",
                            BadGatewayReason::Transport,
                        )
                    };
                    yield Err(diagnose_reqwest_error(classified, &error, ProviderFailurePhase::ResponseBodyRead));
                    return;
                }
            }
        }
    }
    .boxed_local()
}

fn timeout_error(cause: BudgetSource) -> EdgeError {
    EdgeError::gateway_timeout_caused("outbound request deadline expired", cause)
}

fn preserve_timeout_detail(
    mut selected: EdgeError,
    detail: Option<ProviderFailureDetail>,
) -> EdgeError {
    if let EdgeError::GatewayTimeout {
        provider_detail, ..
    } = &mut selected
    {
        *provider_detail = detail;
    }
    selected
}

fn apply_dispatch_timeout(
    request: reqwest::RequestBuilder,
    budget: DispatchBudget,
    clock: &MonotonicClock,
) -> Result<reqwest::RequestBuilder, EdgeError> {
    Ok(request.timeout(budget_remaining(budget, clock)?))
}

#[cfg(test)]
mod clock_tests {
    use std::collections::VecDeque;
    use std::io::{Error as IoError, ErrorKind};
    use std::pin::Pin;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::task::{Context, Poll};

    use edgezero_core::http::response_builder;
    use edgezero_core::response::IntoResponse as _;
    use edgezero_core::time::Deadline;
    use edgezero_core::{
        AxumFailureCause, AxumFailureKind, ProviderFailureDetail, ProviderFailurePhase,
    };
    use futures_util::stream;
    use http_body::Frame;
    use tokio::net::TcpListener;

    use super::*;

    struct NeverReadyBody(Arc<AtomicUsize>);

    struct FailingBody(Option<IoError>);

    #[expect(
        clippy::missing_trait_methods,
        reason = "default hints describe this failing test body"
    )]
    impl http_body::Body for FailingBody {
        type Data = Bytes;
        type Error = IoError;

        fn poll_frame(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
            Poll::Ready(self.get_mut().0.take().map(Err))
        }
    }

    #[expect(
        clippy::missing_trait_methods,
        reason = "default size and EOF hints describe this never-ready test source"
    )]
    impl http_body::Body for NeverReadyBody {
        type Data = Bytes;
        type Error = IoError;

        fn poll_frame(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
            self.0.fetch_add(1, Ordering::Relaxed);
            Poll::Pending
        }
    }

    #[test]
    fn image_optimizer_rejection_precedes_pending_body_and_expired_budget() {
        use edgezero_core::OutboundImageOptimizerOptions;
        use futures::executor::block_on;
        use std::cell::Cell;
        use std::rc::Rc;

        let polls = Rc::new(Cell::new(0_usize));
        let counted = Rc::clone(&polls);
        let body = Body::from_stream(stream::poll_fn(move |_| {
            counted.set(counted.get().saturating_add(1));
            Poll::Pending
        }));
        let request = OutboundRequest::get("http://127.0.0.1:9/image")
            .unwrap()
            .image_optimizer(OutboundImageOptimizerOptions::new())
            .body(body)
            .timeout(Duration::ZERO);
        let client = AxumOutboundClient::try_new().unwrap();
        let error = block_on(client.send(request)).expect_err("unsupported");
        assert!(matches!(error, EdgeError::BadRequest { .. }));
        assert!(error.message().contains("image_optimizer"));
        assert!(error.message().contains("axum"));
        assert_eq!(polls.get(), 0);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn image_optimizer_batch_rejection_keeps_live_plain_sibling() {
        use edgezero_core::OutboundImageOptimizerOptions;
        use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let target = format!("http://{}/plain", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let (mut socket, _) = timeout(Duration::from_secs(2), listener.accept())
                .await
                .unwrap()
                .unwrap();
            let mut head = Vec::new();
            while !head.ends_with(b"\r\n\r\n") {
                assert!(head.len() < 1024, "bounded fixture request head");
                head.push(socket.read_u8().await.unwrap());
            }
            assert!(head.starts_with(b"GET /plain HTTP/1.1\r\n"));
            socket
                .write_all(b"HTTP/1.1 204 No Content\r\nConnection: close\r\n\r\n")
                .await
                .unwrap();
        });
        let client = AxumOutboundClient::try_new().unwrap();
        let image = OutboundRequest::get(&target)
            .unwrap()
            .image_optimizer(OutboundImageOptimizerOptions::new())
            .body(Body::from_stream(stream::pending()))
            .timeout(Duration::ZERO);
        let plain = OutboundRequest::get(&target).unwrap();
        let results = client
            .start_batch_until(vec![image, plain], Deadline::after(Duration::from_secs(2)))
            .collect()
            .await
            .unwrap();
        assert_eq!(
            results.termination,
            edgezero_core::OutboundBatchTermination::Completed
        );
        assert!(matches!(
            results.slots.first(),
            Some(Some(edgezero_core::OutboundSlotResult {
                outcome: Err(EdgeError::BadRequest { .. }),
                ..
            }))
        ));
        assert!(
            matches!(results.slots.get(1), Some(Some(edgezero_core::OutboundSlotResult {
            outcome: Ok(response), ..
        })) if response.status() == StatusCode::NO_CONTENT)
        );
        server.await.unwrap();
    }

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

    fn test_budget(start: MonotonicInstant, duration: Duration) -> DispatchBudget {
        DispatchBudget {
            cause: BudgetSource::PerCallTimeout,
            deadline: Deadline::at_instant(start.checked_add(duration).expect("deadline")),
            duration,
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn provider_diagnostics_survive_post_ready_upload_and_response_expiry() {
        let detail = ProviderFailureDetail::Axum {
            phase: ProviderFailurePhase::ResponseBodyRead,
            cause: AxumFailureCause {
                kind: AxumFailureKind::Decode,
                is_connect: false,
                is_timeout: false,
                io_kind: Some(ErrorKind::UnexpectedEof),
            },
        };
        for upload in [false, true] {
            let start = MonotonicInstant::now();
            let budget = test_budget(start, Duration::from_secs(1));
            let source = stream::iter([Err(EdgeError::bad_gateway_with_detail(
                "source failed",
                BadGatewayReason::Transport,
                detail,
            ))])
            .boxed_local();
            let clock = scripted_clock(vec![start, budget.deadline.instant()]);
            let error = if upload {
                collect_request_body(Body::from_stream(source), 32, budget, &clock)
                    .await
                    .expect_err("late upload error")
            } else {
                deadline_stream(source, budget, clock)
                    .next()
                    .await
                    .expect("terminal")
                    .expect_err("late response error")
            };
            assert!(matches!(
                error,
                EdgeError::GatewayTimeout {
                    cause: BudgetSource::PerCallTimeout,
                    ..
                }
            ));
            assert_eq!(error.provider_failure_detail(), Some(&detail));
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn provider_diagnostics_preserve_refused_connection_without_url_text() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("listener");
        let address = listener.local_addr().expect("address");
        drop(listener);
        let error = reqwest::Client::builder()
            .no_proxy()
            .build()
            .expect("client")
            .get(format!("http://{address}/?token=provider-secret-token"))
            .timeout(Duration::from_secs(2))
            .send()
            .await
            .expect_err("closed local port");
        assert!(error.to_string().contains("provider-secret-token"));
        assert!(error.is_connect());
        let start = MonotonicInstant::now();
        for expired in [false, true] {
            let clock = MonotonicClock::new(move || start);
            let mut budget = test_budget(start, Duration::from_secs(1));
            if expired {
                budget.deadline = Deadline::at_instant(start);
            }
            let mapped = classify_send_error(&error, budget, &clock);
            assert_eq!(
                mapped.provider_failure_detail(),
                Some(&ProviderFailureDetail::Axum {
                    phase: ProviderFailurePhase::Send,
                    cause: AxumFailureCause {
                        kind: AxumFailureKind::Request,
                        is_connect: true,
                        is_timeout: false,
                        io_kind: Some(ErrorKind::ConnectionRefused),
                    },
                })
            );
            let plain = if expired {
                timeout_error(budget.cause)
            } else {
                EdgeError::bad_gateway_with_reason(
                    "upstream request failed",
                    BadGatewayReason::Unreachable,
                )
            };
            assert_same_public_error(plain, mapped);
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn provider_diagnostics_preserve_reqwest_body_source_kind() {
        let source = FailingBody(Some(IoError::new(
            ErrorKind::UnexpectedEof,
            "provider-secret-token",
        )));
        let response = reqwest::Response::from(
            response_builder()
                .body(reqwest::Body::wrap(source))
                .expect("response"),
        );
        let start = MonotonicInstant::now();
        let budget = test_budget(start, Duration::from_secs(1));
        let mut stream = response_stream(response, budget, MonotonicClock::new(move || start));
        let mapped = stream
            .next()
            .await
            .expect("source failure")
            .expect_err("body failure");
        assert_eq!(
            mapped.provider_failure_detail(),
            Some(&ProviderFailureDetail::Axum {
                phase: ProviderFailurePhase::ResponseBodyRead,
                cause: AxumFailureCause {
                    kind: AxumFailureKind::Decode,
                    is_connect: false,
                    is_timeout: false,
                    io_kind: Some(ErrorKind::UnexpectedEof),
                },
            })
        );
        assert!(stream.next().await.is_none());
        assert_same_public_error(
            EdgeError::bad_gateway_with_reason(
                "upstream response body failed",
                BadGatewayReason::Transport,
            ),
            mapped,
        );
    }

    fn assert_same_public_error(plain: EdgeError, diagnosed: EdgeError) {
        assert_eq!(plain.to_string(), diagnosed.to_string());
        assert_eq!(plain.message(), diagnosed.message());
        assert_eq!(plain.kind(), diagnosed.kind());
        assert_eq!(plain.status(), diagnosed.status());
        assert!(
            !format!("{:?}", diagnosed.provider_failure_detail()).contains("provider-secret-token")
        );
        let before = plain.into_response().expect("plain wire");
        let after = diagnosed.into_response().expect("diagnosed wire");
        assert_eq!(before.status(), after.status());
        assert_eq!(before.headers(), after.headers());
        assert_eq!(before.body().as_bytes(), after.body().as_bytes());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn upload_cap_boundaries_preserve_typed_origins() {
        let start = MonotonicInstant::now();
        let clock = MonotonicClock::new(move || start);
        let budget = test_budget(start, Duration::from_secs(1));
        for len in [7_usize, 8, 9] {
            for streamed in [false, true] {
                let bytes = Bytes::from(vec![b'x'; len]);
                let body = if streamed {
                    Body::stream(stream::iter([bytes]))
                } else {
                    Body::from(bytes)
                };
                let result = collect_request_body(body, 8, budget, &clock).await;
                if len > 8 {
                    let error = result.expect_err("upload overflow");
                    assert_eq!(error.status(), StatusCode::PAYLOAD_TOO_LARGE);
                    assert_eq!(
                        error.request_body_overflow_origin(),
                        Some(edgezero_core::RequestBodyOverflowOrigin::OutboundUpload)
                    );
                } else {
                    assert_eq!(result.expect("body").len(), len);
                }
            }
        }
        let source_error =
            EdgeError::payload_too_large(edgezero_core::RequestBodyOverflowOrigin::Inbound);
        let body = Body::from_stream(stream::iter([Err(source_error)]));
        let error = collect_request_body(body, 8, budget, &clock)
            .await
            .expect_err("source overflow");
        assert_eq!(
            error.request_body_overflow_origin(),
            Some(edgezero_core::RequestBodyOverflowOrigin::Inbound)
        );
    }

    #[tokio::test]
    async fn marker_off_preserves_exact_upstream_header_budgets() {
        let bytes = u64::try_from(edgezero_core::PROXY_HEADER.len() + "upstream".len() + 2)
            .expect("header bytes");
        for (max_bytes, max_count, reason) in [
            (
                Some(bytes),
                None,
                edgezero_core::ResponseLimitReason::HeaderBytes,
            ),
            (
                None,
                Some(2),
                edgezero_core::ResponseLimitReason::HeaderCount,
            ),
        ] {
            for setting in [
                OutboundProxyMarker::Off,
                OutboundProxyMarker::On,
                OutboundProxyMarker::Inherit,
            ] {
                let native = http::Response::builder()
                    .status(StatusCode::NO_CONTENT)
                    .header(edgezero_core::PROXY_HEADER, "upstream")
                    .header("x", "y")
                    .body(reqwest::Body::from(Bytes::new()))
                    .expect("native response");
                let start = MonotonicInstant::now();
                let result = process_response(
                    reqwest::Response::from(native),
                    Method::GET,
                    setting,
                    ResponseDecoding::Decode,
                    ResponseMode::Buffered { max_bytes: 0 },
                    test_budget(start, Duration::from_secs(1)),
                    0,
                    24,
                    None,
                    Some(0),
                    Some(0),
                    max_bytes,
                    max_count,
                    MonotonicClock::new(move || start),
                )
                .await;
                if setting == OutboundProxyMarker::Off {
                    assert_eq!(
                        result
                            .expect("exact upstream caps")
                            .headers()
                            .get(edgezero_core::PROXY_HEADER)
                            .expect("upstream marker"),
                        "upstream"
                    );
                } else {
                    assert!(
                        matches!(result, Err(EdgeError::ResponseTooLarge { reason: actual, .. }) if actual == reason)
                    );
                }
            }
        }
    }

    #[tokio::test]
    async fn original_coding_bodyless_disposition_precedes_caps_and_polling() {
        for (method, status) in [
            (Method::HEAD, StatusCode::OK),
            (Method::GET, StatusCode::NOT_MODIFIED),
            (Method::GET, StatusCode::NO_CONTENT),
            (Method::GET, StatusCode::RESET_CONTENT),
        ] {
            for choice in [ResponseDecoding::Decode, ResponseDecoding::Original] {
                for mode in [
                    ResponseMode::Buffered { max_bytes: 1 },
                    ResponseMode::Streamed,
                ] {
                    let polls = Arc::new(AtomicUsize::new(0));
                    let native = http::Response::builder()
                        .status(status)
                        .header("content-encoding", "gzip")
                        .header("content-length", "1073741824")
                        .body(reqwest::Body::wrap(NeverReadyBody(Arc::clone(&polls))))
                        .expect("native response");
                    let start = MonotonicInstant::now();
                    let response = process_response(
                        reqwest::Response::from(native),
                        method.clone(),
                        OutboundProxyMarker::Inherit,
                        choice,
                        mode,
                        test_budget(start, Duration::from_secs(1)),
                        0,
                        24,
                        None,
                        Some(1),
                        Some(1),
                        None,
                        None,
                        MonotonicClock::new(move || start),
                    )
                    .await
                    .expect("bodyless response");
                    assert_eq!(polls.load(Ordering::Relaxed), 0);
                    assert_eq!(response.body().as_bytes(), Some(b"".as_slice()));
                    assert_eq!(
                        response
                            .headers()
                            .get("content-encoding")
                            .expect("baseline coding metadata"),
                        "gzip"
                    );
                    if method == Method::HEAD || status == StatusCode::NOT_MODIFIED {
                        assert_eq!(
                            response
                                .headers()
                                .get("content-length")
                                .expect("representation length"),
                            "1073741824"
                        );
                    } else if status == StatusCode::RESET_CONTENT {
                        assert_eq!(
                            response
                                .headers()
                                .get("content-length")
                                .expect("reset length"),
                            "0"
                        );
                    } else {
                        assert!(!response.headers().contains_key("content-length"));
                    }
                }
            }
        }
    }

    #[tokio::test]
    async fn method_entry_and_preflight_elapsed_use_the_injected_clock() {
        let start = MonotonicInstant::now();
        let completed = start
            .checked_add(Duration::from_millis(9))
            .expect("completed instant");
        let client = AxumOutboundClient::try_with_clock(scripted_clock(vec![start, completed]))
            .expect("client");
        let request = OutboundRequest::get("https://example.com/")
            .expect("request")
            .stream_response();

        let results = client
            .start_batch_until(vec![request], Deadline::after(Duration::from_secs(1)))
            .collect()
            .await
            .expect("valid batch driver");

        let slot = results.slots[0].as_ref().expect("resolved slot");
        assert_eq!(slot.elapsed, Duration::from_millis(9));
        assert!(matches!(slot.outcome, Err(EdgeError::BadRequest { .. })));
    }

    #[tokio::test]
    async fn backwards_clock_fails_slot_without_invalid_elapsed() {
        let start = MonotonicInstant::now();
        let earlier = start
            .checked_sub(Duration::from_millis(1))
            .expect("earlier instant");
        let client = AxumOutboundClient::try_with_clock(scripted_clock(vec![start, earlier]))
            .expect("client");
        let request = OutboundRequest::get("https://example.com/")
            .expect("request")
            .stream_response();

        let results = client
            .start_batch_until(vec![request], Deadline::after(Duration::from_secs(1)))
            .collect()
            .await
            .expect("valid batch driver");

        let slot = results.slots[0].as_ref().expect("resolved slot");
        assert_eq!(slot.elapsed, Duration::ZERO);
        assert!(matches!(slot.outcome, Err(EdgeError::Internal { .. })));
    }

    #[test]
    fn backwards_clock_cannot_expand_the_selected_budget() {
        let start = MonotonicInstant::now();
        let budget = test_budget(start, Duration::from_millis(10));
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
    fn request_timer_arms_from_final_remaining_budget() {
        let start = MonotonicInstant::now();
        let budget = test_budget(start, Duration::from_millis(10));
        let clock = scripted_clock(vec![budget.deadline.instant()]);
        let request = reqwest::Client::new().get("https://example.com/");

        let error = apply_dispatch_timeout(request, budget, &clock)
            .expect_err("final dispatch sample must reject equality expiry");

        assert!(matches!(error, EdgeError::GatewayTimeout { .. }));
    }

    #[tokio::test]
    async fn buffered_request_preparation_reduces_the_remaining_injected_budget() {
        let start = MonotonicInstant::now();
        let budget = test_budget(start, Duration::from_millis(10));
        let observed = start
            .checked_add(Duration::from_millis(3))
            .expect("observed instant");
        let clock = scripted_clock(vec![observed, observed]);

        let body = collect_request_body(Body::from("body"), 16, budget, &clock)
            .await
            .expect("prepared body");
        let remaining = budget_remaining(budget, &clock).expect("remaining budget");

        assert_eq!(body, Bytes::from_static(b"body"));
        assert_eq!(remaining, Duration::from_millis(7));
    }

    #[tokio::test]
    async fn streamed_upload_checks_injected_clock_after_ready_item() {
        let start = MonotonicInstant::now();
        let budget = test_budget(start, Duration::from_millis(10));
        let clock = scripted_clock(vec![start, budget.deadline.instant()]);
        let body = Body::from_stream(stream::once(async { Ok(Bytes::from_static(b"body")) }));

        let error = collect_request_body(body, 16, budget, &clock)
            .await
            .expect_err("post-ready expiry");

        assert!(matches!(error, EdgeError::GatewayTimeout { .. }));
    }

    #[tokio::test]
    async fn converted_response_retains_injected_clock() {
        let start = MonotonicInstant::now();
        let budget = test_budget(start, Duration::from_secs(1));
        let observed = Arc::new(Mutex::new(start));
        let clock_observed = Arc::clone(&observed);
        let clock = MonotonicClock::new(move || *clock_observed.lock().expect("clock observation"));
        let native = reqwest::Response::from(
            http::Response::builder()
                .status(StatusCode::NO_CONTENT)
                .body(reqwest::Body::from(Bytes::new()))
                .expect("native response"),
        );
        let response = process_response(
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
            clock,
        )
        .await
        .expect("converted response");
        *observed.lock().expect("clock observation") = budget.deadline.instant();

        let error = response
            .into_bytes_bounded_until(1, budget.deadline)
            .await
            .expect_err("retained clock reached deadline");

        assert!(matches!(error, EdgeError::GatewayTimeout { .. }));
    }

    #[tokio::test]
    async fn response_content_type_provenance_observes_raw_tcp_duplicates() {
        use edgezero_core::{ContentTypeIneligibility, ContentTypeProvenance};
        use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
        use tokio::net::TcpListener;

        for opaque_sibling in [false, true] {
            let listener = TcpListener::bind("127.0.0.1:0")
                .await
                .expect("loopback listener");
            let address = listener.local_addr().expect("local address");
            let origin = tokio::spawn(async move {
                let (mut socket, _) = listener.accept().await.expect("accepted connection");
                let mut request = [0_u8; 4096];
                let read = timeout(Duration::from_secs(5), socket.read(&mut request))
                    .await
                    .expect("request deadline")
                    .expect("request head");
                assert!(read > 0);
                let mut head =
                    b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Type: ".to_vec();
                head.extend_from_slice(if opaque_sibling {
                    b"\xff"
                } else {
                    b"application/json"
                });
                head.extend_from_slice(b"\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}");
                socket.write_all(&head).await.expect("upstream response");
            });
            let client = AxumOutboundClient::try_new().expect("outbound client");
            let request = OutboundRequest::get(format!("http://{address}/"))
                .expect("request")
                .timeout(Duration::from_secs(5));
            let response = client.send(request).await.expect("outbound response");
            assert_eq!(
                response.content_type_provenance(),
                ContentTypeProvenance::Ineligible(ContentTypeIneligibility::MultipleValues)
            );
            assert_eq!(
                response.headers().get_all("content-type").iter().count(),
                if opaque_sibling { 1 } else { 2 }
            );
            assert_eq!(response.body().as_bytes(), Some(b"{}".as_slice()));
            origin.await.expect("origin task");
        }
    }

    #[tokio::test]
    async fn response_content_type_provenance_survives_native_conversion() {
        use ContentTypeIneligibility::{Missing, MultipleValues, NonText, Removed};
        use ContentTypeProvenance::{Ineligible, SingleTextUnchanged};
        use edgezero_core::{ContentTypeIneligibility, ContentTypeProvenance};

        let cases: &[(&[&[u8]], bool, ContentTypeProvenance)] = &[
            (&[], false, Ineligible(Missing)),
            (&[b"application/json"], false, SingleTextUnchanged),
            (&[b"caf\xc3\xa9"], false, SingleTextUnchanged),
            (
                &[b"application/json, text/plain"],
                false,
                SingleTextUnchanged,
            ),
            (
                &[b"application/json", b"application/json"],
                false,
                Ineligible(MultipleValues),
            ),
            (
                &[b"application/json", b"text/plain"],
                false,
                Ineligible(MultipleValues),
            ),
            (
                &[b"application/json", b"\xff"],
                false,
                Ineligible(MultipleValues),
            ),
            (&[b"\xff"], false, Ineligible(NonText)),
            (&[b"application/json"], true, Ineligible(Removed)),
        ];
        let buffered = ResponseMode::Buffered { max_bytes: 32 };
        for (method, status, response_mode) in [
            (Method::GET, StatusCode::OK, buffered),
            (Method::GET, StatusCode::OK, ResponseMode::Streamed),
            (Method::HEAD, StatusCode::OK, buffered),
            (Method::GET, StatusCode::NO_CONTENT, buffered),
            (Method::GET, StatusCode::RESET_CONTENT, buffered),
            (Method::GET, StatusCode::NOT_MODIFIED, buffered),
        ] {
            for (values, nominated, expected) in cases {
                let mut native_headers = HeaderMap::new();
                for value in *values {
                    native_headers.append(
                        "content-type",
                        HeaderValue::from_bytes(value).expect("header bytes"),
                    );
                }
                if *nominated {
                    native_headers.insert("connection", HeaderValue::from_static("content-type"));
                }
                let mut native = http::Response::builder()
                    .status(status)
                    .body(reqwest::Body::from(Bytes::new()))
                    .expect("native response");
                *native.headers_mut() = native_headers;
                let start = MonotonicInstant::now();
                let response = process_response(
                    reqwest::Response::from(native),
                    method.clone(),
                    OutboundProxyMarker::Inherit,
                    ResponseDecoding::Decode,
                    response_mode,
                    test_budget(start, Duration::from_secs(1)),
                    32 * 1024 * 1024,
                    24,
                    None,
                    None,
                    None,
                    None,
                    None,
                    MonotonicClock::new(move || start),
                )
                .await
                .expect("converted response");
                assert_eq!(response.content_type_provenance(), *expected);
                let converted = response.into_response().expect("application response");
                assert_eq!(
                    converted.extensions().get::<ContentTypeProvenance>(),
                    Some(expected)
                );
            }
        }
    }

    #[tokio::test]
    async fn response_stream_retains_clock_for_post_ready_expiry() {
        let start = MonotonicInstant::now();
        let budget = test_budget(start, Duration::from_millis(10));
        let clock = scripted_clock(vec![start, budget.deadline.instant()]);
        let source = stream::once(async { Ok(Bytes::from_static(b"body")) }).boxed_local();
        let mut body = deadline_stream(source, budget, clock);

        let error = body
            .next()
            .await
            .expect("terminal item")
            .expect_err("post-ready expiry");

        assert!(matches!(error, EdgeError::GatewayTimeout { .. }));
    }
}
