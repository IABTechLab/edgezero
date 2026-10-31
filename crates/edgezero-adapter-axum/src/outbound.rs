#![cfg_attr(
    test,
    expect(
        clippy::self_named_module_files,
        reason = "the established outbound module keeps private response fixtures in a submodule"
    )
)]

use async_stream::stream;
use bytes::Bytes;
use core::time::Duration;
use edgezero_core::body::{Body, BodyPayload, BodyStream};
use edgezero_core::error::{BadGatewayReason, BudgetSource, EdgeError};
use edgezero_core::error_detail::{
    AxumFailureCause, AxumFailureKind, ProviderFailureDetail, ProviderFailurePhase,
};
use edgezero_core::http::header::{ACCEPT_ENCODING, HOST};
use edgezero_core::http::{HeaderMap, HeaderValue, Method, StatusCode};
use edgezero_core::outbound::{
    OutboundBatch, OutboundBatchDriverEvent, OutboundDispatchRequest, OutboundExchangeOwner,
    OutboundFuture, OutboundHead, OutboundHeaderFidelity, OutboundHeaderInput, OutboundHttpClient,
    OutboundPending, OutboundPreparedBatch, OutboundRequest, OutboundRequestParts,
    OutboundResponse, OutboundResponseContext, OutboundSendMode, OutboundTask, OutboundTime,
};
use edgezero_core::time::{DispatchBudget, MonotonicClock, MonotonicInstant};
use edgezero_core::{IndependentOutboundProgress, OutboundProgressRuntime};
use futures::channel::{mpsc, oneshot};
use futures_util::StreamExt as _;
use futures_util::future::{FutureExt as _, LocalBoxFuture, poll_fn};
use futures_util::stream::{FuturesUnordered, once};
use reqwest::header::HeaderMap as ReqwestHeaderMap;
use reqwest::redirect::Policy;
use std::error::Error as _;
use std::future::Future;
use std::io::Error as IoError;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::rc::Rc;
use std::sync::Arc;
use std::task::Poll;
use tokio::runtime::Handle;
use tokio::task::spawn_local;
use tokio::time::sleep;
#[cfg(test)]
use tokio::time::timeout;

#[cfg(test)]
mod testing;

/// Native outbound HTTP implementation used by the Axum adapter.
#[derive(Clone)]
pub struct AxumOutboundClient {
    client: Arc<reqwest::Client>,
    time: OutboundTime,
}

impl AxumOutboundClient {
    async fn execute(&self, prepared: OutboundDispatchRequest) -> Result<OutboundHead, EdgeError> {
        let (parts, context) = prepared.into_parts();
        let budget = context.budget();
        let time = context.time().clone();
        let OutboundRequestParts {
            body,
            connect_timeout,
            mut headers,
            host_authority_override,
            max_request_body_bytes,
            method,
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
            collect_request_body(body, max_request_body_bytes, budget, &time).await?;
        let request_builder = self
            .client
            .request(reqwest_method(&method)?, uri.to_string())
            .headers(headers)
            .body(request_body);
        let mut previous = time.clock().now();
        let mut timer = time.wait_until(budget.deadline);
        let response = race_with_timer(
            request_builder.send(),
            budget,
            &time,
            &mut timer,
            &mut previous,
            None,
            |result| {
                result
                    .as_ref()
                    .err()
                    .map(|error| reqwest_failure_detail(error, ProviderFailurePhase::Send))
            },
        )
        .await?
        .map_err(|error| {
            classify_send_error(&error, budget, time.clock(), connect_timeout.is_some())
        })?;

        response_head(response, context)
    }

    /// Builds a client with redirects and automatic content decoding disabled.
    ///
    /// # Errors
    /// Returns the underlying client-construction error when TLS initialization fails.
    #[inline]
    pub fn try_new() -> Result<Self, reqwest::Error> {
        Self::try_with_time(native_time(MonotonicClock::default()))
    }

    #[expect(
        clippy::absolute_paths,
        reason = "keep the no-retry policy explicit at the shared transport boundary"
    )]
    pub(crate) fn try_transport() -> Result<Arc<reqwest::Client>, reqwest::Error> {
        reqwest::Client::builder()
            .redirect(Policy::none())
            .retry(reqwest::retry::never())
            .no_brotli()
            .no_deflate()
            .no_gzip()
            .no_zstd()
            .build()
            .map(Arc::new)
    }

    /// Builds a client using the supplied monotonic clock and cancellable timer factory.
    ///
    /// # Errors
    /// Returns the underlying client-construction error when TLS initialization fails.
    #[inline]
    pub fn try_with_time(time: OutboundTime) -> Result<Self, reqwest::Error> {
        Self::try_transport().map(|client| Self::with_transport_and_time(client, time))
    }

    fn validate_options(request: &OutboundRequest) -> Result<(), EdgeError> {
        if request.image_optimizer_options().is_some() {
            return Err(EdgeError::bad_request(
                "image_optimizer is not supported by axum",
            ));
        }
        Ok(())
    }

    pub(crate) fn with_transport_and_time(
        client: Arc<reqwest::Client>,
        time: OutboundTime,
    ) -> Self {
        Self { client, time }
    }
}

impl OutboundHttpClient for AxumOutboundClient {
    #[inline]
    fn begin(&self, request: OutboundDispatchRequest) -> Result<OutboundPending, EdgeError> {
        let scope = request.scope().clone();
        let time = request.time().clone();
        let entered_at = request.entered_at();
        let head = self.dispatch_head(request);
        schedule_response(scope.run(
            time,
            entered_at,
            Box::pin(async move { head.await?.finish().await }),
        ))
    }

    #[inline]
    fn dispatch_head(&self, request: OutboundDispatchRequest) -> OutboundFuture<OutboundHead> {
        let client = self.clone();
        let scope = request.scope().clone();
        scope.run(
            request.time().clone(),
            request.entered_at(),
            Box::pin(async move { client.execute(request).await }),
        )
    }

    #[inline]
    fn progress(&self) -> Rc<dyn OutboundProgressRuntime> {
        Rc::new(IndependentOutboundProgress)
    }

    #[inline]
    fn start_batch_until(&self, batch: OutboundPreparedBatch) -> OutboundBatch {
        schedule_batch(self, batch)
    }

    #[inline]
    fn time(&self) -> OutboundTime {
        self.time.clone()
    }

    #[inline]
    fn validate_supported_options(
        &self,
        request: &OutboundRequest,
        _mode: OutboundSendMode,
    ) -> Result<(), EdgeError> {
        Self::validate_options(request)
    }
}

fn schedule_batch(client: &dyn OutboundHttpClient, batch: OutboundPreparedBatch) -> OutboundBatch {
    let (context, slots) = batch.into_slots();
    if slots.is_empty() {
        return OutboundBatch::cutoff(context);
    }
    let cutoff = context.observation_cutoff();
    let (mut sender, receiver) = mpsc::channel(slots.len().saturating_add(1));
    let mut work = FuturesUnordered::new();
    for (index, request) in slots {
        let budget = request.budget();
        let clock = request.clock().clone();
        let scope = request.scope().clone();
        let time = request.time().clone();
        let entered_at = request.entered_at();
        let head = client.dispatch_head(request);
        let pipeline = scope.run(
            time,
            entered_at,
            Box::pin(async move { head.await?.finish().await }),
        );
        work.push(
            async move {
                let outcome = pipeline.await;
                (index, clock.now(), budget, outcome)
            }
            .boxed_local(),
        );
    }
    let task = Box::pin(async move {
        let mut cutoff_unresolved = false;
        while let Some((index, completed_at, budget, outcome)) = work.next().await {
            cutoff_unresolved |= cutoff.is_expired_at(completed_at)
                && !budget.own_deadline_expired_before(cutoff, completed_at);
            if sender
                .try_send(OutboundBatchDriverEvent::Item {
                    index,
                    completed_at,
                    outcome,
                })
                .is_err()
            {
                return;
            }
        }
        if cutoff_unresolved {
            let _closed = sender.try_send(OutboundBatchDriverEvent::Cutoff);
        }
    });
    match spawn_owned(task) {
        Ok(owner) => OutboundBatch::from_driver(context, receiver, owner),
        Err(error) => OutboundBatch::from_driver(
            context,
            once(async move { OutboundBatchDriverEvent::Failed(error) }),
            OutboundExchangeOwner::new(|| {}),
        ),
    }
}

fn spawn_owned(work: LocalBoxFuture<'static, ()>) -> Result<OutboundExchangeOwner, EdgeError> {
    Handle::try_current().map_err(|_missing| {
        EdgeError::internal(anyhow::anyhow!(
            "outbound work requires a Tokio local scheduler"
        ))
    })?;
    let (runner, owner) = OutboundTask::new(work)?;
    let task = catch_unwind(AssertUnwindSafe(|| spawn_local(runner))).map_err(|_missing| {
        EdgeError::internal(anyhow::anyhow!("outbound work requires a Tokio LocalSet"))
    })?;
    let abort = task.abort_handle();
    Ok(OutboundExchangeOwner::new(move || {
        drop(owner);
        abort.abort();
    }))
}

fn schedule_response(work: OutboundFuture<OutboundResponse>) -> Result<OutboundPending, EdgeError> {
    let (sender, receiver) = oneshot::channel();
    let owner = spawn_owned(Box::pin(async move {
        let _closed = sender.send(work.await);
    }))?;
    Ok(OutboundPending::new(
        Box::pin(async move {
            receiver.await.map_err(|_closed| {
                EdgeError::internal(anyhow::anyhow!("outbound completion task stopped"))
            })?
        }),
        owner,
    ))
}

async fn collect_request_body(
    body: Body,
    maximum: u64,
    budget: DispatchBudget,
    time: &OutboundTime,
) -> Result<Bytes, EdgeError> {
    let clock = time.clock();
    let (payload, _scope) = body.into_parts();
    match payload {
        BodyPayload::Once(bytes) => {
            let length = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
            if length > maximum {
                return Err(EdgeError::payload_too_large(
                    edgezero_core::RequestBodyOverflowOrigin::OutboundUpload,
                ));
            }
            budget_remaining(budget, clock)?;
            Ok(bytes)
        }
        BodyPayload::Stream(mut source) => {
            let mut collected = Vec::new();
            let mut total = 0_u64;
            let mut previous = clock.now();
            let mut timer = time.wait_until(budget.deadline);
            loop {
                let next_item = race_with_timer(
                    source.next(),
                    budget,
                    time,
                    &mut timer,
                    &mut previous,
                    None,
                    |item| {
                        item.as_ref()
                            .and_then(|outcome| outcome.as_ref().err())
                            .and_then(EdgeError::provider_failure_detail)
                            .copied()
                    },
                )
                .await?;
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

fn response_head(
    response: reqwest::Response,
    context: OutboundResponseContext,
) -> Result<OutboundHead, EdgeError> {
    let budget = context.budget();
    let clock = context.clock().clone();
    budget_remaining(budget, &clock)?;
    let status = StatusCode::from_u16(response.status().as_u16()).map_err(EdgeError::internal)?;
    let headers = OutboundHeaderInput::new(
        "axum",
        copy_headers(response.headers()),
        OutboundHeaderFidelity::AdapterVisibleFields,
    );
    let encoded = response_stream(response, budget, clock);
    OutboundHead::new(
        status,
        headers,
        encoded,
        context,
        OutboundExchangeOwner::new(|| {}),
    )
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
    connect_timeout_configured: bool,
) -> EdgeError {
    let classified = if budget.deadline.is_expired_at(clock.now()) {
        timeout_error(budget.cause)
    } else if error.is_timeout() && error.is_connect() && connect_timeout_configured {
        timeout_error(BudgetSource::ConnectTimeout)
    } else if error.is_timeout() {
        timeout_error(BudgetSource::Unspecified)
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

pub(crate) fn native_time(clock: MonotonicClock) -> OutboundTime {
    OutboundTime::new(clock, |deadline, paired_clock| {
        Box::pin(async move {
            if Handle::try_current().is_err() {
                return Err(EdgeError::internal(anyhow::anyhow!(
                    "outbound timer requires a Tokio runtime"
                )));
            }
            let Some(remaining) = deadline.remaining_at(paired_clock.now()) else {
                return Ok(());
            };
            AssertUnwindSafe(async move {
                sleep(remaining).await;
            })
            .catch_unwind()
            .await
            .map_err(|_panic| EdgeError::internal(anyhow::anyhow!("outbound timer is unavailable")))
        })
    })
}

fn checked_now(
    time: &OutboundTime,
    previous: &mut MonotonicInstant,
) -> Result<MonotonicInstant, EdgeError> {
    let now = time.clock().now();
    if now < *previous {
        return Err(EdgeError::internal(anyhow::anyhow!(
            "outbound clock moved backwards during I/O"
        )));
    }
    *previous = now;
    Ok(now)
}

async fn race_with_timer<Output>(
    future: impl Future<Output = Output>,
    budget: DispatchBudget,
    time: &OutboundTime,
    timer: &mut OutboundFuture<()>,
    previous: &mut MonotonicInstant,
    expiry_detail: Option<ProviderFailureDetail>,
    detail: impl FnOnce(&Output) -> Option<ProviderFailureDetail>,
) -> Result<Output, EdgeError> {
    let mut extract_detail = Some(detail);
    futures_util::pin_mut!(future);
    poll_fn(|cx| {
        checked_now(time, previous)?;
        if let Poll::Ready(result) = timer.as_mut().poll(cx) {
            result?;
            checked_now(time, previous)?;
            return Poll::Ready(Err(preserve_timeout_detail(
                timeout_error(budget.cause),
                expiry_detail,
            )));
        }
        if budget.deadline.is_expired_at(checked_now(time, previous)?) {
            return Poll::Ready(Err(preserve_timeout_detail(
                timeout_error(budget.cause),
                expiry_detail,
            )));
        }
        let output = future.as_mut().poll(cx);
        if budget.deadline.is_expired_at(checked_now(time, previous)?) {
            let observed_detail = if let Poll::Ready(value) = &output {
                extract_detail.take().and_then(|extract| extract(value))
            } else {
                None
            };
            return Poll::Ready(Err(preserve_timeout_detail(
                timeout_error(budget.cause),
                observed_detail.or(expiry_detail),
            )));
        }
        output.map(Ok)
    })
    .await
}

#[cfg(test)]
mod clock_tests {

    use super::testing::{finish_fixture_response, finish_fixture_stream};
    use edgezero_core::outbound::{
        HttpClient, OutboundBatchTermination, OutboundCollectedSlot, OutboundProxyMarker,
        ResponseDecoding, ResponseMode,
    };
    use std::cell::{Cell, RefCell};
    use std::collections::VecDeque;
    use std::io::{Error as IoError, ErrorKind};
    use std::iter::repeat_with;
    use std::pin::Pin;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::task::{Context, Poll};

    use edgezero_core::http::response_builder;
    use edgezero_core::response::IntoResponse as _;
    use edgezero_core::time::DEFAULT_NO_DEADLINE_BUDGET;
    use edgezero_core::time::Deadline;
    use edgezero_core::{
        AxumFailureCause, AxumFailureKind, OutboundScopeOwner, ProviderFailureDetail,
        ProviderFailurePhase, ResponseLimitReason,
    };
    use futures::executor::block_on;
    use futures_util::future::pending;
    use futures_util::stream;
    use http_body::Frame;
    use tokio::net::TcpListener;
    use tokio::task::{LocalSet, yield_now};

    use super::*;

    struct NeverReadyBody(Arc<AtomicUsize>);

    struct FailingBody(Option<IoError>);

    struct CountedBody {
        bytes: Option<Bytes>,
        polls: Arc<AtomicUsize>,
    }

    #[expect(
        clippy::missing_trait_methods,
        reason = "default hints keep counted source reads observable"
    )]
    impl http_body::Body for CountedBody {
        type Data = Bytes;
        type Error = IoError;

        fn poll_frame(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
            let body = self.get_mut();
            body.polls.fetch_add(1, Ordering::SeqCst);
            Poll::Ready(body.bytes.take().map(|bytes| Ok(Frame::data(bytes))))
        }
    }

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
    fn transport_explicitly_selects_never_retry_policy() {
        // Policy selection, not an HTTP/1 closed-POST retry reproduction.
        let source = include_str!("outbound.rs");
        let transport = source
            .split_once("pub(crate) fn try_transport()")
            .expect("existing transport builder")
            .1
            .split_once(".build()")
            .expect("transport construction")
            .0;
        assert!(
            transport.contains(".retry(reqwest::retry::never())"),
            "the shared transport must explicitly disable reqwest retries"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn streamed_send_keeps_original_scope_under_foreign_request_polling() {
        LocalSet::new()
            .run_until(async {
                let original = OutboundScopeOwner::callback();
                let foreign = OutboundScopeOwner::callback();
                let client =
                    HttpClient::with_client(AxumOutboundClient::try_new().expect("client"));
                let nested_client = client.clone();
                let nested = Rc::new(RefCell::new(None::<OutboundFuture<OutboundResponse>>));
                let retained = Rc::clone(&nested);
                let upload = Rc::new(());
                let released_upload = Rc::downgrade(&upload);
                let child = Rc::new(());
                let released_child = Rc::downgrade(&child);
                let mut pending_child = Some(child);
                let body = Body::from_stream(stream::poll_fn(move |_| {
                    assert_eq!(Rc::strong_count(&upload), 1);
                    if let Some(owner) = pending_child.take() {
                        let source = Body::from_stream(stream::poll_fn(move |_| {
                            assert_eq!(Rc::strong_count(&owner), 1);
                            Poll::<Option<Result<Bytes, EdgeError>>>::Pending
                        }));
                        let send = nested_client.send(
                            OutboundRequest::post("http://127.0.0.1:9/")
                                .expect("child request")
                                .body(source),
                        );
                        *retained.borrow_mut() = Some(send);
                    }
                    Poll::<Option<Result<Bytes, EdgeError>>>::Pending
                }));
                let mut send = original.with(|| {
                    client.send(
                        OutboundRequest::post("http://127.0.0.1:9/")
                            .expect("request")
                            .body(body),
                    )
                });
                foreign
                    .run(poll_fn(|cx| {
                        assert!(send.as_mut().poll(cx).is_pending());
                        Poll::Ready(())
                    }))
                    .await;
                assert!(nested.borrow().is_some(), "upload poll creates child send");
                drop(foreign);
                assert!(
                    released_child.upgrade().is_some(),
                    "foreign scope cannot cancel child"
                );
                drop(original);
                assert!(
                    released_upload.upgrade().is_none(),
                    "send upload is revoked"
                );
                assert!(
                    released_child.upgrade().is_none(),
                    "child belongs to original scope"
                );
                assert!(nested.borrow().is_some(), "child future is still retained");
                drop(send);
                drop(nested.borrow_mut().take());
            })
            .await;
    }

    #[tokio::test]
    async fn missing_local_scheduler_never_polls_owned_upload() {
        let polls = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&polls);
        let source = stream::poll_fn(move |_| {
            observed.fetch_add(1, Ordering::SeqCst);
            Poll::Pending
        })
        .boxed_local();
        let pair = native_time(MonotonicClock::default());
        let budget = test_budget(pair.clock().now(), Duration::from_secs(1));
        let pending = schedule_response(Box::pin(async move {
            collect_request_body(Body::from_stream(source), 8, budget, &pair).await?;
            Err(EdgeError::internal(anyhow::anyhow!(
                "fixture upload unexpectedly completed"
            )))
        }));
        assert!(matches!(pending, Err(EdgeError::Internal { .. })));
        assert_eq!(polls.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn supplied_timer_setup_error_precedes_upload_polling() {
        let reads = Arc::new(AtomicUsize::new(0));
        let captured = Arc::clone(&reads);
        let source = stream::poll_fn(move |_context| {
            captured.fetch_add(1, Ordering::SeqCst);
            Poll::Pending
        });
        let pair = OutboundTime::new(MonotonicClock::default(), |_deadline, _clock| {
            Box::pin(async {
                Err(EdgeError::internal(anyhow::anyhow!(
                    "fixture timer setup failed"
                )))
            })
        });
        let client = HttpClient::with_client(AxumOutboundClient::try_with_time(pair).unwrap());
        let request = OutboundRequest::post("http://127.0.0.1:9/upload")
            .unwrap()
            .body(Body::from_stream(source));
        let error = block_on(client.send(request)).expect_err("timer setup");
        assert!(matches!(error, EdgeError::Internal { .. }));
        assert_eq!(reads.load(Ordering::SeqCst), 0);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn closed_scope_rejects_scheduling_before_work_is_polled() {
        LocalSet::new()
            .run_until(async {
                let callback = OutboundScopeOwner::callback();
                callback.close();
                let polls = Rc::new(Cell::new(0_usize));
                let observed = Rc::clone(&polls);
                let resource = Rc::new(());
                let released = Rc::downgrade(&resource);
                let refused = callback.with(|| {
                    schedule_response(Box::pin(poll_fn(move |_| {
                        assert_eq!(Rc::strong_count(&resource), 1);
                        observed.set(observed.get().saturating_add(1));
                        Poll::Pending
                    })))
                });
                assert!(matches!(refused, Err(EdgeError::Internal { .. })));
                assert_eq!(polls.get(), 0);
                assert!(released.upgrade().is_none(), "refused work is released");
            })
            .await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn local_set_drop_releases_work_and_fails_retained_manual_pending() {
        let local = LocalSet::new();
        let resource = Rc::new(());
        let released = Rc::downgrade(&resource);
        let polls = Rc::new(Cell::new(0_usize));
        let observed = Rc::clone(&polls);
        let mut pending = local
            .run_until(async move {
                let work = poll_fn(move |_| {
                    assert_eq!(Rc::strong_count(&resource), 1);
                    observed.set(observed.get().saturating_add(1));
                    Poll::Pending
                });
                let retained = schedule_response(Box::pin(work)).expect("manual pending");
                yield_now().await;
                retained
            })
            .await;
        assert_eq!(polls.get(), 1, "scheduled work entered pending");
        assert!(released.upgrade().is_some());
        drop(local);
        assert!(
            released.upgrade().is_none(),
            "executor drop releases owned work"
        );
        assert!(pending.is_ready(), "destroyed producer closes its receiver");
        let error = timeout(Duration::from_secs(1), pending.collect())
            .await
            .expect("completion failure is bounded")
            .expect_err("stopped task cannot complete successfully");
        assert!(matches!(error, EdgeError::Internal { .. }));
        assert!(
            error
                .to_string()
                .contains("outbound completion task stopped")
        );
        assert_eq!(polls.get(), 1, "executor destruction does not repoll work");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn scheduled_work_allows_reentrant_cancellation_while_polling() {
        LocalSet::new()
            .run_until(async {
                let owner = Rc::new(RefCell::new(None::<OutboundExchangeOwner>));
                let cancelled = Rc::clone(&owner);
                let panicked = Rc::new(Cell::new(false));
                let observed = Rc::clone(&panicked);
                let polls = Rc::new(Cell::new(0_usize));
                let counted = Rc::clone(&polls);
                let resource = Rc::new(());
                let released = Rc::downgrade(&resource);
                let work = poll_fn(move |_| {
                    assert_eq!(Rc::strong_count(&resource), 1);
                    counted.set(counted.get().saturating_add(1));
                    let cancel_owner = cancelled.borrow_mut().take();
                    observed.set(catch_unwind(AssertUnwindSafe(|| drop(cancel_owner))).is_err());
                    Poll::Pending
                })
                .boxed_local();
                *owner.borrow_mut() = Some(spawn_owned(work).expect("local scheduler"));
                yield_now().await;
                assert_eq!(polls.get(), 1, "cancelled work must not be polled again");
                assert!(
                    !panicked.get(),
                    "cancellation must not borrow the future being polled"
                );
                assert!(
                    released.upgrade().is_none(),
                    "cancelled pipeline releases its source"
                );
            })
            .await;
    }

    #[test]
    fn native_timer_without_runtime_returns_internal_error() {
        let pair = native_time(MonotonicClock::default());
        let deadline = pair.clock().deadline_after(Duration::from_secs(1));
        let error = block_on(pair.wait_until(deadline)).expect_err("no runtime");
        assert!(matches!(error, EdgeError::Internal { .. }));
        assert!(error.to_string().contains("Tokio runtime"));
    }

    #[test]
    fn native_timer_without_timer_driver_returns_internal_error() {
        use tokio::runtime::Builder;

        let runtime = Builder::new_current_thread().build().expect("runtime");
        let pair = native_time(MonotonicClock::default());
        let deadline = pair.clock().deadline_after(Duration::from_secs(1));
        let error = runtime
            .block_on(pair.wait_until(deadline))
            .expect_err("no timer driver");
        assert!(matches!(error, EdgeError::Internal { .. }));
        assert!(error.to_string().contains("timer is unavailable"));
    }

    #[tokio::test]
    async fn review_cross_chunk_regression_releases_retained_source() {
        use std::future::pending;

        let start = MonotonicInstant::now();
        let budget = test_budget(start, Duration::from_secs(10));
        let advanced = start.checked_add(Duration::from_millis(2)).unwrap();
        let regressed = start.checked_add(Duration::from_millis(1)).unwrap();
        let observed = Arc::new(Mutex::new(start));
        let sampled = Arc::clone(&observed);
        let pair = native_time(MonotonicClock::new(move || *sampled.lock().unwrap()));
        let owner = Arc::new(());
        let released = Arc::downgrade(&owner);
        let source_clock = Arc::clone(&observed);
        let source = stream! {
            *source_clock.lock().unwrap() = advanced;
            yield Ok(Bytes::from_static(b"first"));
            yield Ok(Bytes::from_static(b"second"));
            pending::<()>().await;
            drop(owner);
        }
        .boxed_local();
        let mut guarded = finish_fixture_stream(source, budget, pair)
            .await
            .expect("shared response stream");
        assert_eq!(
            guarded.next().await.unwrap().unwrap(),
            Bytes::from_static(b"first")
        );
        *observed.lock().unwrap() = regressed;
        let error = guarded
            .next()
            .await
            .unwrap()
            .expect_err("cross-chunk clock regression");
        assert!(matches!(error, EdgeError::Internal { .. }));
        assert!(
            released.upgrade().is_none(),
            "release before terminal wrapper drop"
        );
    }

    #[test]
    fn unsupported_optimizer_precedes_expiry_without_transport() {
        use edgezero_core::outbound::test_support::check_unsupported_image_optimizer_preflight;
        use futures::executor::block_on;

        block_on(check_unsupported_image_optimizer_preflight(
            |time| AxumOutboundClient::try_with_time(time).expect("client"),
            "axum",
            &[
                OutboundSendMode::Send,
                OutboundSendMode::Follow,
                OutboundSendMode::Begin,
                OutboundSendMode::Batch,
            ],
        ))
        .expect("unsupported-mode conformance");
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
        let client = HttpClient::with_client(AxumOutboundClient::try_new().unwrap());
        let error = block_on(client.send(request)).expect_err("unsupported");
        assert!(matches!(error, EdgeError::BadRequest { .. }));
        assert!(error.message().contains("image_optimizer"));
        assert!(error.message().contains("axum"));
        assert_eq!(polls.get(), 0);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn image_optimizer_batch_rejection_keeps_live_plain_sibling() {
        LocalSet::new().run_until(async {
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
        let client = HttpClient::with_client(AxumOutboundClient::try_new().unwrap());
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
            results.termination(),
            edgezero_core::OutboundBatchTermination::Completed
        );
        assert!(matches!(
            results.slots().first(),
            Some(OutboundCollectedSlot::Resolved(edgezero_core::OutboundSlotResult {
                outcome: Err(EdgeError::BadRequest { .. }),
                ..
            }))
        ));
        assert!(
            matches!(results.slots().get(1), Some(OutboundCollectedSlot::Resolved(edgezero_core::OutboundSlotResult {
            outcome: Ok(response), ..
        })) if response.status() == StatusCode::NO_CONTENT)
        );
        server.await.unwrap();

        }).await;
    }

    fn scripted_clock(script: Vec<MonotonicInstant>) -> MonotonicClock {
        let observations = Arc::new(Mutex::new(VecDeque::from(script)));
        MonotonicClock::new(move || {
            let mut samples = observations.lock().expect("clock observations");
            let now = *samples.front().expect("clock observation");
            if samples.len() > 1 {
                samples.pop_front();
            }
            now
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
            let observed = Arc::new(Mutex::new(start));
            let sampled = Arc::clone(&observed);
            let clock = MonotonicClock::new(move || *sampled.lock().unwrap());
            let source = stream::once(async move {
                *observed.lock().unwrap() = budget.deadline.instant();
                Err(EdgeError::bad_gateway_with_detail(
                    "source failed",
                    BadGatewayReason::Transport,
                    detail,
                ))
            })
            .boxed_local();
            let error = if upload {
                collect_request_body(Body::from_stream(source), 32, budget, &native_time(clock))
                    .await
                    .expect_err("late upload error")
            } else {
                finish_fixture_stream(source, budget, native_time(clock))
                    .await
                    .expect("shared response stream")
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
    async fn reported_connect_timeout_requires_setting_and_preserves_governing_deadline() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("origin");
        let target = format!("https://{}/", listener.local_addr().expect("address"));
        let server = tokio::spawn(async move {
            let (_socket, _) = listener.accept().await.expect("connection");
            pending::<()>().await;
        });
        let error = reqwest::Client::builder()
            .no_proxy()
            .connect_timeout(Duration::from_millis(20))
            .build()
            .expect("native client")
            .get(target)
            .send()
            .await
            .expect_err("stalled TLS");
        server.abort();
        let _cancelled = server.await;
        assert!(error.is_timeout() && error.is_connect());
        let start = MonotonicInstant::now();
        for configured in [false, true] {
            for cause in [
                BudgetSource::PerCallTimeout,
                BudgetSource::RequestDeadline,
                BudgetSource::BatchCutoff,
                BudgetSource::Default,
            ] {
                let mut budget = test_budget(start, Duration::from_secs(1));
                budget.cause = cause;
                for offset in [999_u64, 1000, 1001] {
                    let observed = start
                        .checked_add(Duration::from_millis(offset))
                        .expect("observation");
                    let clock = MonotonicClock::new(move || observed);
                    let mapped = classify_send_error(&error, budget, &clock, configured);
                    let expected = if offset >= 1000 {
                        cause
                    } else if configured {
                        BudgetSource::ConnectTimeout
                    } else {
                        BudgetSource::Unspecified
                    };
                    assert!(
                        matches!(mapped, EdgeError::GatewayTimeout { cause: actual, .. } if actual == expected),
                        "configured={configured}, offset={offset}, {mapped:?}"
                    );
                    assert_eq!(
                        mapped.provider_failure_detail(),
                        Some(&reqwest_failure_detail(&error, ProviderFailurePhase::Send))
                    );
                }
            }
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn clamped_batch_cutoff_uses_actual_observation_cutoff() {
        LocalSet::new()
            .run_until(async {
                for (elapsed_hours, unresolved) in [(168_u64, false), (192, true)] {
                    let start = MonotonicInstant::now();
                    let now = Arc::new(Mutex::new(start));
                    let sampled = Arc::clone(&now);
                    let clock = MonotonicClock::new(move || *sampled.lock().expect("clock"));
                    let client = HttpClient::with_client(
                        AxumOutboundClient::try_with_time(native_time(clock)).expect("client"),
                    );
                    let cutoff = Deadline::at_instant(
                        start
                            .checked_add(Duration::from_hours(192))
                            .expect("eight-day cutoff"),
                    );
                    let requests = repeat_with(|| {
                        OutboundRequest::get("http://127.0.0.1:9/")
                            .expect("request")
                            .timeout(Duration::from_hours(192))
                    })
                    .take(2)
                    .collect();
                    let batch = client.start_batch_until(requests, cutoff);
                    *now.lock().expect("clock") = start
                        .checked_add(Duration::from_hours(elapsed_hours))
                        .expect("terminal observation");
                    let results = batch
                        .collect()
                        .await
                        .expect("clamped observation remains a valid driver");
                    let expected = if unresolved {
                        OutboundBatchTermination::Cutoff
                    } else {
                        OutboundBatchTermination::Completed
                    };
                    assert_eq!(results.termination(), expected);
                    assert_eq!(results.slots().len(), 2);
                    for slot in results.slots() {
                        if unresolved {
                            assert!(matches!(slot, OutboundCollectedSlot::Unresolved));
                        } else {
                            assert!(matches!(slot, OutboundCollectedSlot::Resolved(result) if matches!(result.outcome, Err(EdgeError::GatewayTimeout { cause: BudgetSource::BatchCutoff, .. }))));
                        }
                    }
                }
            })
            .await;
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
            let mapped = classify_send_error(&error, budget, &clock, false);
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
                let result =
                    collect_request_body(body, 8, budget, &native_time(clock.clone())).await;
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
        let error = collect_request_body(body, 8, budget, &native_time(clock))
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
                let result = finish_fixture_response(
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
                    native_time(MonotonicClock::new(move || start)),
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
    async fn response_conversion_checks_deadline_before_handoff() {
        for (method, status) in [
            (Method::HEAD, StatusCode::OK),
            (Method::GET, StatusCode::NOT_MODIFIED),
            (Method::GET, StatusCode::NO_CONTENT),
            (Method::GET, StatusCode::RESET_CONTENT),
            (Method::GET, StatusCode::OK),
        ] {
            let modes: &[ResponseMode] = if method == Method::GET && status == StatusCode::OK {
                &[ResponseMode::Streamed]
            } else {
                &[
                    ResponseMode::Buffered { max_bytes: 1 },
                    ResponseMode::Streamed,
                ]
            };
            for &response_mode in modes {
                for cause in [
                    BudgetSource::PerCallTimeout,
                    BudgetSource::RequestDeadline,
                    BudgetSource::BatchCutoff,
                    BudgetSource::Default,
                ] {
                    for offset in [9_u64, 10, 11] {
                        let start = MonotonicInstant::now();
                        let duration = if cause == BudgetSource::Default {
                            DEFAULT_NO_DEADLINE_BUDGET
                        } else {
                            Duration::from_millis(10)
                        };
                        let mut budget = test_budget(start, duration);
                        budget.cause = cause;
                        let offset_duration = duration
                            .checked_sub(Duration::from_millis(10))
                            .and_then(|lead| lead.checked_add(Duration::from_millis(offset)))
                            .expect("fixture duration bound");
                        let observed = start.checked_add(offset_duration).expect("handoff time");
                        let polls = Arc::new(AtomicUsize::new(0));
                        let native = response_builder()
                            .status(status)
                            .header("content-length", "1")
                            .body(reqwest::Body::wrap(NeverReadyBody(Arc::clone(&polls))))
                            .expect("native response");
                        let result = finish_fixture_response(
                            reqwest::Response::from(native),
                            method.clone(),
                            OutboundProxyMarker::Inherit,
                            ResponseDecoding::Decode,
                            response_mode,
                            budget,
                            1024,
                            24,
                            None,
                            None,
                            None,
                            None,
                            None,
                            native_time(scripted_clock(vec![start, observed])),
                        )
                        .await;
                        assert_eq!(
                            polls.load(Ordering::Relaxed),
                            0,
                            "handoff never reads payload"
                        );
                        if cause == BudgetSource::BatchCutoff
                            && response_mode == ResponseMode::Streamed
                        {
                            assert!(
                                matches!(result, Err(EdgeError::BadRequest { .. })),
                                "streamed batches reject before response processing"
                            );
                        } else if offset < 10 {
                            drop(result.expect("on-time handoff"));
                        } else {
                            assert!(
                                matches!(result, Err(EdgeError::GatewayTimeout {cause: actual, ..}) if actual == cause),
                                "expired handoff must retain its selected budget"
                            );
                        }
                        assert_eq!(
                            Arc::strong_count(&polls),
                            1,
                            "source released on return or response drop"
                        );
                    }
                }
            }
        }
    }

    #[tokio::test]
    async fn original_coding_bodyless_disposition_precedes_caps_and_polling() {
        for encoded_cap in [1, 64 * 1024] {
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
                        let response = finish_fixture_response(
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
                            Some(encoded_cap),
                            None,
                            None,
                            native_time(MonotonicClock::new(move || start)),
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
    }

    #[tokio::test]
    #[expect(
        clippy::too_many_lines,
        reason = "the literal native normalization matrix keeps input and policy assertions together"
    )]
    async fn native_response_normalization_preserves_policy_precedence() {
        use flate2::{Compression, write::GzEncoder};
        use std::io::Write as _;

        let mut encoder = GzEncoder::new(Vec::new(), Compression::fast());
        encoder.write_all(b"body").expect("gzip input");
        let gzip = Bytes::from(encoder.finish().expect("gzip fixture"));
        for choice in [ResponseDecoding::Decode, ResponseDecoding::Original] {
            for mode in [
                ResponseMode::Buffered { max_bytes: 256 },
                ResponseMode::Streamed,
            ] {
                for (case, status) in [
                    ("nominated-over", StatusCode::OK),
                    ("nominated-exact", StatusCode::OK),
                    ("missing-over", StatusCode::OK),
                    ("opaque", StatusCode::OK),
                    ("etag", StatusCode::OK),
                    ("malformed", StatusCode::OK),
                    ("representation", StatusCode::OK),
                    ("representation", StatusCode::PARTIAL_CONTENT),
                ] {
                    let mut headers = HeaderMap::new();
                    let mut payload = Bytes::from_static(b"ab");
                    let mut decoded = 1;
                    match case {
                        "nominated-over" | "nominated-exact" => {
                            headers.insert(
                                "connection",
                                HeaderValue::from_static("content-encoding, content-length, etag"),
                            );
                            headers.insert("content-encoding", HeaderValue::from_static("gzip"));
                            headers.insert("content-length", HeaderValue::from_static("malformed"));
                            headers.insert("etag", HeaderValue::from_static("\"removed\""));
                            if case == "nominated-exact" {
                                decoded = 2;
                            }
                        }
                        "opaque" | "etag" => {
                            headers.insert(
                                "etag",
                                HeaderValue::from_bytes(&[0xff]).expect("opaque ETag"),
                            );
                            headers.insert("content-length", HeaderValue::from_static("2"));
                            if case == "opaque" {
                                headers.insert(
                                    "content-encoding",
                                    HeaderValue::from_bytes(&[0xff]).expect("opaque coding"),
                                );
                            } else {
                                decoded = 2;
                            }
                        }
                        "malformed" => {
                            headers.insert("content-length", HeaderValue::from_static("malformed"));
                        }
                        "representation" => {
                            payload = gzip.clone();
                            decoded = 4;
                            headers.insert("content-encoding", HeaderValue::from_static("gzip"));
                            headers.insert(
                                "content-length",
                                HeaderValue::from_str(&gzip.len().to_string()).expect("length"),
                            );
                            headers.insert(
                                "content-range",
                                HeaderValue::from_str(&format!(
                                    "bytes 0-{}/{}",
                                    gzip.len() - 1,
                                    gzip.len() * 2
                                ))
                                .expect("range"),
                            );
                            for (name, value) in [
                                ("etag", "\"strong\""),
                                ("last-modified", "Wed, 21 Oct 2015 07:28:00 GMT"),
                                ("vary", "Accept-Encoding"),
                                ("accept-ranges", "bytes"),
                                ("cache-control", "public, max-age=60"),
                                (
                                    "content-digest",
                                    "sha-256=:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=:",
                                ),
                            ] {
                                headers.insert(name, HeaderValue::from_static(value));
                            }
                        }
                        _ => {}
                    }
                    let original = headers.clone();
                    let polls = Arc::new(AtomicUsize::new(0));
                    let mut native = response_builder()
                        .status(status)
                        .body(reqwest::Body::wrap(CountedBody {
                            bytes: Some(payload),
                            polls: Arc::clone(&polls),
                        }))
                        .expect("native response");
                    *native.headers_mut() = headers;
                    let start = MonotonicInstant::now();
                    let processed = finish_fixture_response(
                        reqwest::Response::from(native),
                        Method::GET,
                        OutboundProxyMarker::Off,
                        choice,
                        mode,
                        test_budget(start, Duration::from_secs(1)),
                        edgezero_core::DEFAULT_MAX_DECODER_BYTES,
                        24,
                        None,
                        Some(decoded),
                        Some(256),
                        None,
                        None,
                        native_time(MonotonicClock::new(move || start)),
                    )
                    .await;
                    let result = async {
                        let response = processed?;
                        match case {
                            "nominated-exact" => {
                                for name in
                                    ["connection", "content-encoding", "content-length", "etag"]
                                {
                                    assert!(
                                        !response.headers().contains_key(name),
                                        "{name} stays removed"
                                    );
                                }
                            }
                            "opaque" | "etag" => {
                                assert!(!response.headers().contains_key("etag"));
                                assert_eq!(response.headers()["content-length"], "2");
                                if case == "opaque" {
                                    assert_eq!(
                                        response.headers()["content-encoding"].as_bytes(),
                                        &[0xff]
                                    );
                                } else {
                                    assert!(!response.headers().contains_key("content-encoding"));
                                }
                            }
                            "representation" => {
                                if choice == ResponseDecoding::Original
                                    || status == StatusCode::PARTIAL_CONTENT
                                {
                                    assert_eq!(response.headers(), &original);
                                } else {
                                    assert!(!response.headers().contains_key("content-encoding"));
                                    assert!(!response.headers().contains_key("content-length"));
                                }
                            }
                            _ => {}
                        }
                        response.into_bytes_bounded(256).await
                    }
                    .await;
                    if case == "malformed" {
                        assert!(matches!(
                            result,
                            Err(EdgeError::BadGateway {
                                reason: BadGatewayReason::Protocol,
                                ..
                            })
                        ));
                        assert_eq!(polls.load(Ordering::SeqCst), 0);
                    } else if matches!(case, "nominated-over" | "missing-over") {
                        assert!(matches!(
                            result,
                            Err(EdgeError::ResponseTooLarge {
                                reason: ResponseLimitReason::DecodedBody,
                                ..
                            })
                        ));
                        assert!(polls.load(Ordering::SeqCst) > 0);
                    } else {
                        let expected = if case == "representation" {
                            if choice == ResponseDecoding::Original
                                || status == StatusCode::PARTIAL_CONTENT
                            {
                                gzip.as_ref()
                            } else {
                                b"body"
                            }
                        } else {
                            b"ab"
                        };
                        assert_eq!(result.expect(case).as_ref(), expected);
                    }
                    assert_eq!(
                        Arc::strong_count(&polls),
                        1,
                        "native source released: {case}"
                    );
                }
            }
        }
    }

    #[tokio::test]
    async fn native_identity_limits_cover_declared_observed_and_exact_boundaries() {
        for choice in [ResponseDecoding::Decode, ResponseDecoding::Original] {
            for mode in [
                ResponseMode::Buffered { max_bytes: 20 },
                ResponseMode::Streamed,
            ] {
                for (length, declared) in [(11, true), (11, false), (10, true), (10, false)] {
                    let polls = Arc::new(AtomicUsize::new(0));
                    let mut native = response_builder()
                        .status(StatusCode::OK)
                        .body(reqwest::Body::wrap(CountedBody {
                            bytes: Some(Bytes::from(vec![b'a'; length])),
                            polls: Arc::clone(&polls),
                        }))
                        .expect("native identity response");
                    if declared {
                        native.headers_mut().insert(
                            "content-length",
                            HeaderValue::from_str(&length.to_string()).expect("length"),
                        );
                    }
                    let start = MonotonicInstant::now();
                    let result = async {
                        let response = finish_fixture_response(
                            reqwest::Response::from(native),
                            Method::GET,
                            OutboundProxyMarker::Off,
                            choice,
                            mode,
                            test_budget(start, Duration::from_secs(1)),
                            0,
                            24,
                            None,
                            Some(10),
                            Some(20),
                            None,
                            None,
                            native_time(MonotonicClock::new(move || start)),
                        )
                        .await?;
                        response.into_bytes_bounded(20).await
                    }
                    .await;
                    if length == 11 {
                        assert!(matches!(
                            result,
                            Err(EdgeError::ResponseTooLarge {
                                reason: ResponseLimitReason::DecodedBody,
                                ..
                            })
                        ));
                        assert_eq!(polls.load(Ordering::SeqCst) == 0, declared);
                    } else {
                        assert_eq!(result.expect("exact identity cap"), vec![b'a'; 10]);
                        assert!(polls.load(Ordering::SeqCst) > 0);
                    }
                    assert_eq!(Arc::strong_count(&polls), 1);
                }
            }
        }
    }

    #[tokio::test]
    async fn native_original_gzip_exempts_decoded_and_decoder_limits() {
        use flate2::{Compression, write::GzEncoder};
        use std::io::Write as _;

        let mut encoder = GzEncoder::new(Vec::new(), Compression::fast());
        encoder.write_all(b"body").expect("gzip input");
        let gzip = Bytes::from(encoder.finish().expect("gzip fixture"));
        let size = u64::try_from(gzip.len()).expect("fixture length");
        for declared in [false, true] {
            for streamed in [false, true] {
                for (encoded_cap, final_cap, expected) in [
                    (size, size, None),
                    (size - 1, size, Some(ResponseLimitReason::EncodedBody)),
                    (size, size - 1, Some(ResponseLimitReason::BufferedBody)),
                ] {
                    if streamed && expected == Some(ResponseLimitReason::BufferedBody) {
                        continue;
                    }
                    let polls = Arc::new(AtomicUsize::new(0));
                    let mut native = response_builder()
                        .status(StatusCode::OK)
                        .header("content-encoding", "gzip")
                        .body(reqwest::Body::wrap(CountedBody {
                            bytes: Some(gzip.clone()),
                            polls: Arc::clone(&polls),
                        }))
                        .expect("native gzip response");
                    if declared {
                        native.headers_mut().insert(
                            "content-length",
                            HeaderValue::from_str(&size.to_string()).expect("length"),
                        );
                    }
                    let start = MonotonicInstant::now();
                    let result = async {
                        let response = finish_fixture_response(
                            reqwest::Response::from(native),
                            Method::GET,
                            OutboundProxyMarker::Off,
                            ResponseDecoding::Original,
                            if streamed {
                                ResponseMode::Streamed
                            } else {
                                ResponseMode::Buffered {
                                    max_bytes: final_cap,
                                }
                            },
                            test_budget(start, Duration::from_secs(1)),
                            0,
                            24,
                            None,
                            Some(1),
                            Some(encoded_cap),
                            None,
                            None,
                            native_time(MonotonicClock::new(move || start)),
                        )
                        .await?;
                        assert_eq!(response.headers()["content-encoding"], "gzip");
                        response.into_bytes_bounded(256).await
                    }
                    .await;
                    if let Some(expected_reason) = expected {
                        assert!(
                            matches!(result, Err(EdgeError::ResponseTooLarge { reason, .. }) if reason == expected_reason)
                        );
                        assert_eq!(polls.load(Ordering::SeqCst) == 0, declared);
                    } else {
                        assert_eq!(result.expect("original gzip without a decoder"), gzip);
                        assert!(polls.load(Ordering::SeqCst) > 0);
                    }
                    assert_eq!(Arc::strong_count(&polls), 1, "native source released");
                }
            }
        }
    }

    #[tokio::test]
    async fn native_reset_content_preserves_original_disposition() {
        for choice in [ResponseDecoding::Decode, ResponseDecoding::Original] {
            for mode in [
                ResponseMode::Buffered { max_bytes: 0 },
                ResponseMode::Streamed,
            ] {
                for length in [None, Some("0"), Some("9")] {
                    let polls = Arc::new(AtomicUsize::new(0));
                    let mut native = response_builder()
                        .status(StatusCode::RESET_CONTENT)
                        .header("content-encoding", "gzip")
                        .body(reqwest::Body::wrap(CountedBody {
                            bytes: Some(Bytes::from_static(b"not decoded")),
                            polls: Arc::clone(&polls),
                        }))
                        .expect("native reset response");
                    if let Some(value) = length {
                        native
                            .headers_mut()
                            .insert("content-length", HeaderValue::from_static(value));
                    }
                    let start = MonotonicInstant::now();
                    let response = finish_fixture_response(
                        reqwest::Response::from(native),
                        Method::GET,
                        OutboundProxyMarker::Off,
                        choice,
                        mode,
                        test_budget(start, Duration::from_secs(1)),
                        0,
                        24,
                        None,
                        Some(0),
                        Some(0),
                        None,
                        None,
                        native_time(MonotonicClock::new(move || start)),
                    )
                    .await
                    .expect("bodyless before caps");
                    assert_eq!(response.headers()["content-length"], "0");
                    assert_eq!(response.headers()["content-encoding"], "gzip");
                    assert_eq!(
                        polls.load(Ordering::SeqCst),
                        usize::from(length != Some("9"))
                    );
                    assert_eq!(
                        Arc::strong_count(&polls),
                        1,
                        "source released before collection"
                    );
                    assert!(
                        response
                            .into_bytes_bounded(0)
                            .await
                            .expect("empty reset")
                            .is_empty()
                    );
                }
            }
        }
    }

    #[tokio::test]
    async fn method_entry_and_preflight_elapsed_use_the_injected_clock() {
        LocalSet::new()
            .run_until(async {
                let start = MonotonicInstant::now();
                let completed = start
                    .checked_add(Duration::from_millis(9))
                    .expect("completed instant");
                let client = HttpClient::with_client(
                    AxumOutboundClient::try_with_time(native_time(scripted_clock(vec![
                        start, completed,
                    ])))
                    .expect("client"),
                );
                let request = OutboundRequest::get("https://example.com/")
                    .expect("request")
                    .stream_response();

                let results = client
                    .start_batch_until(vec![request], Deadline::after(Duration::from_secs(1)))
                    .collect()
                    .await
                    .expect("valid batch driver");

                let slot = resolved_slot(&results.slots()[0]);
                assert_eq!(slot.elapsed, Duration::from_millis(9));
                assert!(matches!(slot.outcome, Err(EdgeError::BadRequest { .. })));
            })
            .await;
    }

    #[tokio::test]
    async fn backwards_clock_fails_slot_without_invalid_elapsed() {
        LocalSet::new()
            .run_until(async {
                let start = MonotonicInstant::now();
                let earlier = start
                    .checked_sub(Duration::from_millis(1))
                    .expect("earlier instant");
                let client = HttpClient::with_client(
                    AxumOutboundClient::try_with_time(native_time(scripted_clock(vec![
                        start, earlier,
                    ])))
                    .expect("client"),
                );
                let request = OutboundRequest::get("https://example.com/")
                    .expect("request")
                    .stream_response();

                let results = client
                    .start_batch_until(vec![request], Deadline::after(Duration::from_secs(1)))
                    .collect()
                    .await
                    .expect("valid batch driver");

                let slot = resolved_slot(&results.slots()[0]);
                assert_eq!(slot.elapsed, Duration::ZERO);
                assert!(matches!(slot.outcome, Err(EdgeError::Internal { .. })));
            })
            .await;
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
    fn paired_timer_rejects_final_expiry_before_transport_poll() {
        let start = MonotonicInstant::now();
        let budget = test_budget(start, Duration::from_millis(10));
        let clock = MonotonicClock::new(move || budget.deadline.instant());
        let time = native_time(clock);
        let mut previous = start;
        let mut timer = time.wait_until(budget.deadline);
        let error = block_on(race_with_timer(
            async {},
            budget,
            &time,
            &mut timer,
            &mut previous,
            None,
            |()| None,
        ))
        .expect_err("dispatch sample must reject equality expiry without a runtime");

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

        let body =
            collect_request_body(Body::from("body"), 16, budget, &native_time(clock.clone()))
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
        let observed = Arc::new(Mutex::new(start));
        let sampled = Arc::clone(&observed);
        let clock = MonotonicClock::new(move || *sampled.lock().unwrap());
        let body = Body::from_stream(stream::once(async move {
            *observed.lock().unwrap() = budget.deadline.instant();
            Ok(Bytes::from_static(b"body"))
        }));

        let error = collect_request_body(body, 16, budget, &native_time(clock))
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
        let response = finish_fixture_response(
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
            native_time(clock),
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
            let client =
                HttpClient::with_client(AxumOutboundClient::try_new().expect("outbound client"));
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
                let response = finish_fixture_response(
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
                    native_time(MonotonicClock::new(move || start)),
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
        let observed = Arc::new(Mutex::new(start));
        let sampled = Arc::clone(&observed);
        let clock = MonotonicClock::new(move || *sampled.lock().unwrap());
        let source = stream::once(async move {
            *observed.lock().unwrap() = budget.deadline.instant();
            Ok(Bytes::from_static(b"body"))
        })
        .boxed_local();
        let mut body = finish_fixture_stream(source, budget, native_time(clock))
            .await
            .expect("shared response stream");

        let error = body
            .next()
            .await
            .expect("terminal item")
            .expect_err("post-ready expiry");

        assert!(matches!(error, EdgeError::GatewayTimeout { .. }));
    }

    fn resolved_slot(slot: &OutboundCollectedSlot) -> &edgezero_core::OutboundSlotResult {
        match slot {
            OutboundCollectedSlot::Resolved(result) => result,
            other @ (OutboundCollectedSlot::AlreadyDelivered
            | OutboundCollectedSlot::Unresolved) => panic!("expected resolved slot, got {other:?}"),
        }
    }
}
