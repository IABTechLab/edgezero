#![cfg_attr(
    all(
        feature = "cloudflare",
        target_arch = "wasm32",
        any(test, feature = "test-utils")
    ),
    expect(
        clippy::self_named_module_files,
        reason = "the established outbound module keeps private Workers response fixtures in a submodule"
    )
)]
#![cfg_attr(
    any(test, all(feature = "cloudflare", target_arch = "wasm32")),
    expect(
        clippy::arbitrary_source_item_ordering,
        reason = "target and test helper modules stay adjacent to the imports that define their boundaries"
    )
)]
#![cfg_attr(
    all(feature = "cloudflare", target_arch = "wasm32"),
    expect(
        clippy::pub_use,
        reason = "the target-gated implementation keeps Workers imports out of native builds"
    )
)]

use edgezero_core::error::EdgeError;
#[cfg(any(
    all(feature = "cloudflare", target_arch = "wasm32"),
    feature = "test-utils"
))]
use edgezero_core::error::{BadGatewayReason, BudgetSource};
#[cfg(any(
    all(feature = "cloudflare", target_arch = "wasm32"),
    feature = "test-utils"
))]
use edgezero_core::outbound::OutboundRequest;
#[cfg(feature = "test-utils")]
use edgezero_core::outbound::{OutboundSendMode, validate_for_dispatch};

#[cfg(any(all(feature = "cloudflare", target_arch = "wasm32"), test))]
mod header_bridge {
    use std::str;

    use edgezero_core::error::EdgeError;
    use edgezero_core::http::{HeaderMap, HeaderName};

    pub(crate) trait HeaderSink {
        fn append(&mut self, name: &str, value: &str) -> Result<(), EdgeError>;
    }

    pub(crate) fn copy_header_values<Sink, InvalidValue>(
        source: &HeaderMap,
        sink: &mut Sink,
        invalid_value: InvalidValue,
    ) -> Result<(), EdgeError>
    where
        Sink: HeaderSink,
        InvalidValue: Fn(&HeaderName) -> EdgeError,
    {
        for (name, value) in source {
            let header_value =
                str::from_utf8(value.as_bytes()).map_err(|_encoding_error| invalid_value(name))?;
            sink.append(name.as_str(), header_value)?;
        }
        Ok(())
    }
}

#[cfg(any(all(feature = "cloudflare", target_arch = "wasm32"), test))]
use header_bridge::HeaderSink;
#[cfg(any(all(feature = "cloudflare", target_arch = "wasm32"), test))]
pub(crate) use header_bridge::copy_header_values;

#[cfg(all(feature = "cloudflare", target_arch = "wasm32"))]
impl HeaderSink for worker::Headers {
    fn append(&mut self, name: &str, value: &str) -> Result<(), EdgeError> {
        worker::Headers::append(self, name, value).map_err(EdgeError::internal)
    }
}

#[cfg(all(feature = "cloudflare", target_arch = "wasm32"))]
mod worker_impl {
    #[cfg(any(test, feature = "test-utils"))]
    mod testing;
    #[cfg(any(test, feature = "test-utils"))]
    use edgezero_core::outbound::{
        OutboundProxyMarker, OutboundRequest, ResponseDecoding, ResponseMode,
    };
    use std::cell::{Cell, RefCell};
    use std::future::Future;
    use std::pin::Pin;
    use std::rc::Rc;
    use std::task::{Context, Poll, Waker};
    use std::time::Duration;
    #[cfg(any(test, feature = "test-utils"))]
    use testing::{finish_fixture_response, finish_fixture_stream};

    use async_stream::stream;
    use bytes::Bytes;
    use edgezero_core::body::{Body, BodyPayload, BodyStream};
    #[cfg(any(test, feature = "test-utils"))]
    use edgezero_core::error::BudgetSource;
    use edgezero_core::error::{BadGatewayReason, EdgeError};
    use edgezero_core::error_detail::{
        CloudflareFailureCause, ProviderFailureDetail, ProviderFailurePhase,
    };
    use edgezero_core::http::header::{ACCEPT_ENCODING, HOST};
    use edgezero_core::http::{HeaderMap, HeaderName, HeaderValue, Method, StatusCode};
    use edgezero_core::outbound::{
        OutboundBatch, OutboundBatchDriverEvent, OutboundCachePolicy, OutboundDispatchRequest,
        OutboundExchangeOwner, OutboundFuture, OutboundHead, OutboundHeaderFidelity,
        OutboundHeaderInput, OutboundHttpClient, OutboundPending, OutboundPreparedBatch,
        OutboundRequestParts, OutboundResponse, OutboundResponseContext, OutboundSendMode,
        OutboundTask, OutboundTime,
    };
    #[cfg(feature = "test-utils")]
    use edgezero_core::time::{Deadline, dispatch_budget};
    use edgezero_core::time::{DispatchBudget, MonotonicClock, MonotonicInstant};
    use edgezero_core::{IndependentOutboundProgress, OutboundProgressRuntime};
    use futures::channel::{mpsc, oneshot};
    use futures_util::StreamExt as _;
    use futures_util::future::{FutureExt as _, LocalBoxFuture, poll_fn};
    use futures_util::stream::{FuturesUnordered, once};
    use worker::js_sys::{Function, Reflect, Uint8Array, global};
    use worker::wasm_bindgen::closure::Closure;
    use worker::wasm_bindgen::{JsCast as _, JsValue};
    use worker::wasm_bindgen_futures::{JsFuture, spawn_local};
    use worker::web_sys;
    use worker::{
        CacheMode, Headers, Method as WorkerMethod, Request as WorkerRequest, RequestInit,
        RequestRedirect, Response as WorkerResponse, ResponseBody as WorkerResponseBody,
    };

    use super::timeout_error;

    const READY_ITEM_YIELD_QUOTA: u32 = 64;

    struct HostEventYield {
        awoken: Rc<Cell<bool>>,
        callback: Option<Closure<dyn FnMut()>>,
        cancellation: Option<(Function, JsValue)>,
        duration: Duration,
        timeout_handle: Option<JsValue>,
        waker: Rc<RefCell<Option<Waker>>>,
    }

    impl HostEventYield {
        fn new() -> Self {
            Self::after(Duration::ZERO)
        }

        fn after(duration: Duration) -> Self {
            Self {
                awoken: Rc::new(Cell::new(false)),
                callback: None,
                cancellation: None,
                duration,
                timeout_handle: None,
                waker: Rc::new(RefCell::new(None)),
            }
        }
    }

    impl Future for HostEventYield {
        type Output = Result<(), EdgeError>;

        fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            let this = self.get_mut();
            if this.awoken.get() {
                drop(this.callback.take());
                drop(this.cancellation.take());
                drop(this.timeout_handle.take());
                let previous = this.waker.borrow_mut().take();
                drop(previous);
                return Poll::Ready(Ok(()));
            }
            let next_waker = cx.waker().clone();
            let previous = this.waker.borrow_mut().replace(next_waker);
            drop(previous);
            if this.callback.is_some() {
                return Poll::Pending;
            }

            let awoken = Rc::clone(&this.awoken);
            let waker = Rc::clone(&this.waker);
            let callback = Closure::<dyn FnMut()>::new(move || {
                awoken.set(true);
                let registered = waker.borrow_mut().take();
                if let Some(waiting) = registered {
                    waiting.wake();
                }
            });
            let global_scope = global();
            let Some(set_timeout) = Reflect::get(&global_scope, &JsValue::from_str("setTimeout"))
                .ok()
                .and_then(|value| value.dyn_into::<Function>().ok())
            else {
                return Poll::Ready(Err(host_event_schedule_error()));
            };
            let Some(clear_timeout) =
                Reflect::get(&global_scope, &JsValue::from_str("clearTimeout"))
                    .ok()
                    .and_then(|value| value.dyn_into::<Function>().ok())
            else {
                return Poll::Ready(Err(host_event_schedule_error()));
            };
            let milliseconds =
                i32::try_from(this.duration.as_nanos().div_ceil(1_000_000)).unwrap_or(i32::MAX);
            let timeout_handle = match set_timeout.call2(
                &global_scope,
                callback.as_ref(),
                &JsValue::from_f64(f64::from(milliseconds)),
            ) {
                Ok(handle) => handle,
                Err(_error) => return Poll::Ready(Err(host_event_schedule_error())),
            };
            this.callback = Some(callback);
            this.timeout_handle = Some(timeout_handle);
            this.cancellation = Some((clear_timeout, global_scope.into()));
            Poll::Pending
        }
    }

    #[expect(
        clippy::missing_trait_methods,
        reason = "`Drop::pin_drop` is unstable (`pin_ergonomics`) and conflicts with `drop`"
    )]
    impl Drop for HostEventYield {
        fn drop(&mut self) {
            if self.awoken.get() {
                return;
            }
            let Some(timeout_handle) = self.timeout_handle.take() else {
                return;
            };
            if let Some((clear, receiver)) = self.cancellation.take() {
                let _ignored = clear.call1(&receiver, &timeout_handle);
            }
        }
    }

    fn host_event_schedule_error() -> EdgeError {
        EdgeError::internal(anyhow::anyhow!("Cloudflare host-event scheduling failed"))
    }

    /// Native outbound HTTP implementation for Cloudflare Workers.
    #[derive(Clone)]
    pub struct CloudflareOutboundClient {
        time: OutboundTime,
    }

    struct AbortGuard {
        controller: Option<web_sys::AbortController>,
    }

    impl AbortGuard {
        fn disarm(&mut self) {
            self.controller = None;
        }

        fn new(controller: web_sys::AbortController) -> Self {
            Self {
                controller: Some(controller),
            }
        }
    }

    impl CloudflareOutboundClient {
        async fn execute(
            &self,
            prepared: OutboundDispatchRequest,
        ) -> Result<OutboundHead, EdgeError> {
            let (parts, context) = prepared.into_parts();
            let budget = context.budget();
            let time = context.time().clone();
            let OutboundRequestParts {
                body,
                cache_policy,
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
                let value =
                    HeaderValue::from_str(authority.as_str()).map_err(EdgeError::internal)?;
                headers.insert(HOST, value);
            }
            let worker_body =
                request_body(body, max_request_body_bytes, budget, time.clone()).await?;
            let url = uri.to_string();
            let request = build_worker_request(&method, &url, &headers, worker_body, cache_policy)?;
            let (response, abort_guard) = raw_fetch(request, budget, &time).await?;
            response_head(response, abort_guard, context)
        }

        /// Builds a client using the process-default monotonic clock.
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
    }

    impl Default for CloudflareOutboundClient {
        #[inline]
        fn default() -> Self {
            Self::new()
        }
    }

    #[expect(
        clippy::missing_trait_methods,
        reason = "`Drop::pin_drop` is unstable (`pin_ergonomics`) and conflicts with `drop`"
    )]
    impl Drop for AbortGuard {
        fn drop(&mut self) {
            if let Some(controller) = self.controller.take() {
                controller.abort();
            }
        }
    }

    impl OutboundHttpClient for CloudflareOutboundClient {
        #[inline]
        fn progress(&self) -> Rc<dyn OutboundProgressRuntime> {
            Rc::new(IndependentOutboundProgress)
        }

        #[inline]
        fn time(&self) -> OutboundTime {
            self.time.clone()
        }

        #[inline]
        fn validate_supported_options(
            &self,
            request: &super::OutboundRequest,
            _mode: OutboundSendMode,
        ) -> Result<(), EdgeError> {
            super::validate_supported_options(request)
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
        fn start_batch_until(&self, batch: OutboundPreparedBatch) -> OutboundBatch {
            schedule_batch(self, batch)
        }
    }

    fn schedule_batch(
        client: &dyn OutboundHttpClient,
        batch: OutboundPreparedBatch,
    ) -> OutboundBatch {
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
        let (runner, owner) = OutboundTask::new(work)?;
        spawn_local(runner);
        Ok(owner)
    }

    fn schedule_response(
        work: OutboundFuture<OutboundResponse>,
    ) -> Result<OutboundPending, EdgeError> {
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

    fn build_headers(headers: &HeaderMap) -> Result<Headers, EdgeError> {
        let mut worker_headers = Headers::new();
        super::copy_header_values(headers, &mut worker_headers, |name| {
            EdgeError::bad_request(format!("header value is not valid UTF-8: {name}"))
        })
        .map_err(diagnose_worker_preparation_error)?;
        Ok(worker_headers)
    }

    fn build_worker_request(
        method: &Method,
        url: &str,
        headers: &HeaderMap,
        request_body: Option<JsValue>,
        cache_policy: OutboundCachePolicy,
    ) -> Result<WorkerRequest, EdgeError> {
        let mut init = RequestInit::new();
        init.with_headers(build_headers(headers)?)
            .with_method(worker_method(method))
            .with_redirect(RequestRedirect::Manual);
        if cache_policy == OutboundCachePolicy::Bypass {
            init.with_cache(CacheMode::NoStore);
        }
        if let Some(worker_body) = request_body {
            init.with_body(Some(worker_body));
        }
        WorkerRequest::new_with_init(url, &init)
            .map_err(|error| diagnose_worker_preparation_error(EdgeError::internal(error)))
    }

    fn guarded_response_stream(input: BodyStream, guard: AbortGuard) -> BodyStream {
        stream! {
            let mut source = Some(input);
            let mut abort_guard = Some(guard);
            let mut ready_items = 0_u32;
            loop {
                let next_item = source.as_mut().ok_or_else(|| EdgeError::internal(anyhow::anyhow!("response source already released")))?.next().await;
                match next_item {
                    Some(Ok(bytes)) => yield Ok(bytes),
                    Some(Err(error)) => {
                        drop(source.take());
                        drop(abort_guard.take());
                        yield Err(error);
                        return;
                    }
                    None => {
                        if let Some(completed) = &mut abort_guard { completed.disarm(); }
                        drop(source.take());
                        drop(abort_guard.take());
                        if let Err(error) = HostEventYield::new().await { yield Err(error); }
                        return;
                    }
                }
                ready_items = ready_items.saturating_add(1);
                if ready_items >= READY_ITEM_YIELD_QUOTA {
                    let checkpoint = HostEventYield::new().await;
                    ready_items = 0;
                    if let Err(error) = checkpoint {
                        drop(source.take());
                        drop(abort_guard.take());
                        yield Err(error);
                        return;
                    }
                }
            }
        }
        .boxed_local()
    }

    fn response_head(
        mut response: WorkerResponse,
        abort_guard: AbortGuard,
        context: OutboundResponseContext,
    ) -> Result<OutboundHead, EdgeError> {
        let budget = context.budget();
        let clock = context.clock().clone();
        budget_remaining(budget, &clock)?;
        let status = StatusCode::from_u16(response.status_code()).map_err(EdgeError::internal)?;
        let headers = OutboundHeaderInput::new(
            "cloudflare",
            response_headers(&response)?,
            OutboundHeaderFidelity::ProviderFieldBoundariesUnknown,
        );
        let native = stream! {
            if matches!(response.body(), WorkerResponseBody::Empty) { return; }
            let mut source = response_stream(&mut response)?;
            while let Some(item) = source.next().await { yield item; }
        }
        .boxed_local();
        let encoded = guarded_response_stream(native, abort_guard);
        OutboundHead::new(
            status,
            headers,
            encoded,
            context,
            OutboundExchangeOwner::new(|| {}),
        )
    }

    async fn raw_fetch(
        request: WorkerRequest,
        budget: DispatchBudget,
        time: &OutboundTime,
    ) -> Result<(WorkerResponse, AbortGuard), EdgeError> {
        let clock = time.clock();
        let mut previous = clock.now();
        let mut timer = time.wait_until(budget.deadline);
        // Register a checked wakeup before the eager fetch promise can start network work.
        poll_fn(|cx| {
            Poll::Ready(match timer.as_mut().poll(cx) {
                Poll::Ready(result) => {
                    Err(result.err().unwrap_or_else(|| timeout_error(budget.cause)))
                }
                Poll::Pending => Ok(()),
            })
        })
        .await?;
        checked_now(time, &mut previous)?;
        let controller = web_sys::AbortController::new().map_err(|error| {
            js_preparation_error(&error, "failed to construct abort controller")
        })?;
        let signal = controller.signal();
        let abort_guard = AbortGuard::new(controller);
        let fetch_init = web_sys::RequestInit::new();
        fetch_init.set_signal(Some(&signal));
        let set = Reflect::set(
            fetch_init.as_ref(),
            &JsValue::from_str("encodeResponseBody"),
            &JsValue::from_str("manual"),
        )
        .map_err(|error| js_preparation_error(&error, "failed to set manual response encoding"))?;
        if !set {
            return Err(EdgeError::internal(anyhow::anyhow!(
                "runtime refused manual response encoding"
            )));
        }

        let global: web_sys::WorkerGlobalScope = global().unchecked_into();
        let (promise, _remaining) = begin_fetch(budget, clock, || {
            global.fetch_with_request_and_init(request.inner(), &fetch_init)
        })?;
        let fetch = JsFuture::from(promise);
        let result = race_with_timer(
            fetch,
            budget,
            time,
            &mut timer,
            &mut previous,
            None,
            |result| {
                let error = result.as_ref().err()?;
                map_fetch_failure(error).provider_failure_detail().copied()
            },
        )
        .await?;
        let value = finish_fetch_result(result, budget_remaining(budget, clock))?;
        let web_response: web_sys::Response = value.dyn_into().map_err(|_non_response| {
            EdgeError::internal(anyhow::anyhow!("fetch returned a non-response value"))
        })?;
        Ok((WorkerResponse::from(web_response), abort_guard))
    }

    fn begin_fetch<Output>(
        budget: DispatchBudget,
        clock: &MonotonicClock,
        dispatch: impl FnOnce() -> Output,
    ) -> Result<(Output, Duration), EdgeError> {
        let remaining = budget_remaining(budget, clock)?;
        Ok((dispatch(), remaining))
    }

    async fn request_body(
        body: Body,
        maximum: u64,
        budget: DispatchBudget,
        time: OutboundTime,
    ) -> Result<Option<JsValue>, EdgeError> {
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
                if bytes.is_empty() {
                    Ok(None)
                } else {
                    Ok(Some(Uint8Array::from(bytes.as_ref()).into()))
                }
            }
            BodyPayload::Stream(source) => {
                let mut bounded = upload_stream(source, maximum, budget, time);
                let mut collected = Vec::new();
                while let Some(item) = bounded.next().await {
                    collected.extend_from_slice(&item?);
                }
                if collected.is_empty() {
                    Ok(None)
                } else {
                    Ok(Some(Uint8Array::from(collected.as_slice()).into()))
                }
            }
        }
    }

    #[cfg(feature = "test-utils")]
    #[doc(hidden)]
    #[inline]
    pub async fn response_abort_lifecycle_holds_for_test() -> bool {
        async fn run_case(
            status: StatusCode,
            content_length: Option<&str>,
            start: MonotonicInstant,
        ) -> Option<bool> {
            let headers = Headers::new();
            if let Some(value) = content_length {
                headers.set("content-length", value).ok()?;
            }
            let response = WorkerResponse::empty()
                .ok()?
                .with_status(status.as_u16())
                .with_headers(headers);
            let controller = web_sys::AbortController::new().ok()?;
            let signal = controller.signal();
            let deadline = start.checked_add(Duration::from_secs(1))?;
            let clock = MonotonicClock::new(move || start);
            finish_fixture_response(
                response,
                AbortGuard::new(controller),
                Method::GET,
                OutboundProxyMarker::Inherit,
                ResponseDecoding::Decode,
                ResponseMode::Buffered { max_bytes: 64 },
                DispatchBudget {
                    cause: BudgetSource::PerCallTimeout,
                    deadline: Deadline::at_instant(deadline),
                    duration: Duration::from_secs(1),
                },
                1024,
                24,
                None,
                None,
                None,
                None,
                None,
                native_time(clock),
            )
            .await
            .ok()?;
            Some(signal.aborted())
        }

        let start = MonotonicInstant::now();
        matches!(
            run_case(StatusCode::RESET_CONTENT, None, start).await,
            Some(false)
        ) && matches!(
            run_case(StatusCode::RESET_CONTENT, Some("7"), start).await,
            Some(true)
        ) && matches!(
            run_case(StatusCode::NO_CONTENT, None, start).await,
            Some(true)
        )
    }

    fn response_headers(response: &WorkerResponse) -> Result<HeaderMap, EdgeError> {
        let mut headers = HeaderMap::new();
        for (raw_name, raw_value) in response.headers().entries() {
            let parsed_name = HeaderName::from_bytes(raw_name.as_bytes()).map_err(|_error| {
                EdgeError::bad_gateway_with_reason(
                    "upstream response contains an invalid header name",
                    BadGatewayReason::Protocol,
                )
            })?;
            let parsed_value = HeaderValue::from_bytes(raw_value.as_bytes()).map_err(|_error| {
                EdgeError::bad_gateway_with_reason(
                    "upstream response contains an invalid header value",
                    BadGatewayReason::Protocol,
                )
            })?;
            headers.append(parsed_name, parsed_value);
        }
        Ok(headers)
    }

    fn js_failure_cause(value: &JsValue) -> CloudflareFailureCause {
        let Ok(name) = Reflect::get(value, &JsValue::from_str("name")) else {
            return CloudflareFailureCause::Unknown;
        };
        if name == JsValue::from_str("AbortError") {
            CloudflareFailureCause::AbortError
        } else if name == JsValue::from_str("TypeError") {
            CloudflareFailureCause::TypeError
        } else {
            CloudflareFailureCause::Unknown
        }
    }

    fn worker_failure_cause(error: &worker::Error) -> CloudflareFailureCause {
        if let worker::Error::Internal(value) = error {
            return js_failure_cause(value);
        }
        if let worker::Error::UnknownJsError { name, .. } = error {
            return match name.as_deref() {
                Some("AbortError") => CloudflareFailureCause::AbortError,
                Some("TypeError") => CloudflareFailureCause::TypeError,
                _ => CloudflareFailureCause::Unknown,
            };
        }
        CloudflareFailureCause::Unknown
    }

    fn diagnose_worker_preparation_error(mut error: EdgeError) -> EdgeError {
        if let EdgeError::Internal {
            source,
            provider_detail,
        } = &mut error
            && let Some(worker_error) = source.downcast_ref::<worker::Error>()
        {
            *provider_detail = Some(ProviderFailureDetail::Cloudflare {
                phase: ProviderFailurePhase::RequestPreparation,
                cause: worker_failure_cause(worker_error),
            });
        }
        error
    }

    fn js_preparation_error(source: &JsValue, message: &'static str) -> EdgeError {
        EdgeError::internal_with_detail(
            anyhow::anyhow!(message),
            ProviderFailureDetail::Cloudflare {
                phase: ProviderFailurePhase::RequestPreparation,
                cause: js_failure_cause(source),
            },
        )
    }

    fn worker_body_failure(error: &worker::Error, message: &'static str) -> EdgeError {
        EdgeError::bad_gateway_with_detail(
            message,
            BadGatewayReason::Transport,
            ProviderFailureDetail::Cloudflare {
                phase: ProviderFailurePhase::ResponseBodyRead,
                cause: worker_failure_cause(error),
            },
        )
    }

    fn map_fetch_failure(error: &JsValue) -> EdgeError {
        let mut mapped = super::generic_fetch_failure();
        if let EdgeError::BadGateway {
            provider_detail, ..
        } = &mut mapped
        {
            *provider_detail = Some(ProviderFailureDetail::Cloudflare {
                phase: ProviderFailurePhase::Send,
                cause: js_failure_cause(error),
            });
        }
        mapped
    }

    fn finish_fetch_result(
        result: Result<JsValue, JsValue>,
        remaining: Result<Duration, EdgeError>,
    ) -> Result<JsValue, EdgeError> {
        if let Err(selected) = remaining {
            let detail = result
                .as_ref()
                .err()
                .map(|source| ProviderFailureDetail::Cloudflare {
                    phase: ProviderFailurePhase::Send,
                    cause: js_failure_cause(source),
                });
            return Err(preserve_timeout_detail(selected, detail));
        }
        result.map_err(|error| map_fetch_failure(&error))
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

    fn stream_error_detail(
        item: Option<&Result<Bytes, EdgeError>>,
    ) -> Option<ProviderFailureDetail> {
        item.and_then(|result| result.as_ref().err())
            .and_then(EdgeError::provider_failure_detail)
            .copied()
    }

    fn response_stream(response: &mut WorkerResponse) -> Result<BodyStream, EdgeError> {
        let source = response.stream().map_err(|error| {
            worker_body_failure(&error, "upstream response body is unavailable")
        })?;
        Ok(source
            .map(|result| {
                result
                    .map(Bytes::from)
                    .map_err(|error| worker_body_failure(&error, "upstream response body failed"))
            })
            .boxed_local())
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

    async fn yield_host_event(
        budget: DispatchBudget,
        time: &OutboundTime,
        previous: &mut MonotonicInstant,
        expiry_detail: Option<ProviderFailureDetail>,
    ) -> Result<(), EdgeError> {
        let mut timer = time.wait_until(budget.deadline);
        race_with_timer(
            HostEventYield::new(),
            budget,
            time,
            &mut timer,
            previous,
            expiry_detail,
            |_result| None,
        )
        .await?
    }

    async fn terminal_error_after_host_event(
        error: EdgeError,
        budget: DispatchBudget,
        time: &OutboundTime,
        previous: &mut MonotonicInstant,
    ) -> EdgeError {
        let detail = error.provider_failure_detail().copied();
        match yield_host_event(budget, time, previous, detail).await {
            Ok(()) => error,
            Err(selected) => selected,
        }
    }

    fn upload_stream(
        mut source: BodyStream,
        maximum: u64,
        budget: DispatchBudget,
        time: OutboundTime,
    ) -> BodyStream {
        stream! {
            let mut ready_items = 0_u32;
            let mut total = 0_u64;
            let mut previous = time.clock().now();
            let mut timer = time.wait_until(budget.deadline);
            loop {
                let next_item = match race_with_timer(source.next(), budget, &time, &mut timer, &mut previous, None, |item| stream_error_detail(item.as_ref())).await {
                    Ok(item) => item,
                    Err(error) => {
                        drop(source);
                        drop(timer);
                        drop(time);
                        yield Err(error);
                        return;
                    }
                };
                let Some(item) = next_item else {
                    if let Err(error) = yield_host_event(budget, &time, &mut previous, None).await {
                        drop(source);
                        drop(timer);
                        drop(time);
                        yield Err(error);
                    }
                    return;
                };
                let bytes = match item {
                    Ok(bytes) => bytes,
                    Err(error) => {
                        let selected = terminal_error_after_host_event(error, budget, &time, &mut previous).await;
                        drop(source);
                        drop(timer);
                        drop(time);
                        yield Err(selected);
                        return;
                    }
                };
                let length = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
                let Some(next_total) = total.checked_add(length) else {
                    let error = EdgeError::payload_too_large(
                        edgezero_core::RequestBodyOverflowOrigin::OutboundUpload,
                    );
                    let selected = terminal_error_after_host_event(error, budget, &time, &mut previous).await;
                    drop(source); drop(timer); drop(time);
                    yield Err(selected);
                    return;
                };
                if next_total > maximum {
                    let error = EdgeError::payload_too_large(
                        edgezero_core::RequestBodyOverflowOrigin::OutboundUpload,
                    );
                    let selected = terminal_error_after_host_event(error, budget, &time, &mut previous).await;
                    drop(source); drop(timer); drop(time);
                    yield Err(selected);
                    return;
                }
                total = next_total;
                yield Ok(bytes);
                ready_items = ready_items.saturating_add(1);
                if ready_items >= READY_ITEM_YIELD_QUOTA {
                    let checkpoint = yield_host_event(budget, &time, &mut previous, None).await;
                    ready_items = 0;
                    if let Err(error) = checkpoint {
                        drop(source); drop(timer); drop(time);
                        yield Err(error);
                        return;
                    }
                }
            }
        }
        .boxed_local()
    }

    pub(crate) fn native_time(clock: MonotonicClock) -> OutboundTime {
        OutboundTime::new(clock, |deadline, paired_clock| {
            Box::pin(async move {
                let Some(remaining) = deadline.remaining_at(paired_clock.now()) else {
                    return Ok(());
                };
                HostEventYield::after(remaining).await
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

    fn worker_method(method: &Method) -> WorkerMethod {
        match *method {
            Method::DELETE => WorkerMethod::Delete,
            Method::HEAD => WorkerMethod::Head,
            Method::OPTIONS => WorkerMethod::Options,
            Method::PATCH => WorkerMethod::Patch,
            Method::POST => WorkerMethod::Post,
            Method::PUT => WorkerMethod::Put,
            _ => WorkerMethod::Get,
        }
    }

    /// Runs deferred upload and response stream clock checks in the hosted contract binary.
    #[cfg(feature = "test-utils")]
    #[doc(hidden)]
    #[inline]
    pub async fn deferred_clock_paths_hold_for_test() -> bool {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::{Arc, Mutex, PoisonError};

        use futures_util::stream::once;

        let start = MonotonicInstant::now();
        let Ok(request) = OutboundRequest::get("https://example.com/") else {
            return false;
        };
        let Ok(budget) = dispatch_budget(&request.timeout(Duration::from_millis(10)), start, None)
        else {
            return false;
        };
        let observed = Arc::new(Mutex::new(start));
        let sampled = Arc::clone(&observed);
        let clock =
            MonotonicClock::new(move || *sampled.lock().unwrap_or_else(PoisonError::into_inner));
        let polls = Arc::new(AtomicUsize::new(0));
        let upload_clock = Arc::clone(&observed);
        let upload_polls = Arc::clone(&polls);
        let upload_source = once(async move {
            upload_polls.fetch_add(1, Ordering::SeqCst);
            *upload_clock.lock().unwrap_or_else(PoisonError::into_inner) =
                budget.deadline.instant();
            Ok(Bytes::from_static(b"body"))
        })
        .boxed_local();
        let mut upload = upload_stream(upload_source, 16, budget, native_time(clock.clone()));
        let upload_holds = matches!(
            upload.next().await,
            Some(Err(EdgeError::GatewayTimeout { .. }))
        );

        let Ok(controller) = web_sys::AbortController::new() else {
            return false;
        };
        *observed.lock().unwrap_or_else(PoisonError::into_inner) = start;
        let response_polls = Arc::clone(&polls);
        let response_source = once(async move {
            response_polls.fetch_add(1, Ordering::SeqCst);
            *observed.lock().unwrap_or_else(PoisonError::into_inner) = budget.deadline.instant();
            Ok(Bytes::from_static(b"body"))
        })
        .boxed_local();
        let Ok(mut response) = finish_fixture_stream(
            response_source,
            AbortGuard::new(controller),
            budget,
            native_time(clock),
        )
        .await
        else {
            return false;
        };
        let response_holds = matches!(
            response.next().await,
            Some(Err(EdgeError::GatewayTimeout { .. }))
        );

        upload_holds && response_holds && polls.load(Ordering::SeqCst) == 2
    }

    #[cfg(test)]
    mod redirect_tests;

    #[cfg(test)]
    mod clock_tests {
        use edgezero_core::outbound::{
            HttpClient, OutboundBatchTermination, OutboundCollectedSlot,
        };
        fn resolved_slot(slot: &OutboundCollectedSlot) -> &edgezero_core::OutboundSlotResult {
            match slot {
                OutboundCollectedSlot::Resolved(result) => result,
                other @ (OutboundCollectedSlot::AlreadyDelivered
                | OutboundCollectedSlot::Unresolved) => {
                    panic!("expected resolved slot, got {other:?}")
                }
            }
        }
        use std::collections::VecDeque;
        use std::iter::repeat_with;
        use std::ptr;
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::{Arc, Mutex};
        use std::task::{Poll, Wake};

        use crate::test_runtime::NumericTimeoutGuard;
        use edgezero_core::error::{BadGatewayDecodeReason, BudgetSource, ResponseLimitReason};
        use edgezero_core::http::header::CONTENT_TYPE;
        use edgezero_core::outbound::ContentTypeProvenance;
        use edgezero_core::response::IntoResponse as _;
        use edgezero_core::time::DEFAULT_NO_DEADLINE_BUDGET;
        use edgezero_core::time::Deadline;
        use edgezero_core::{
            CloudflareFailureCause, OutboundScopeOwner, ProviderFailureDetail, ProviderFailurePhase,
        };
        use futures_util::future::pending;
        use futures_util::stream;
        use wasm_bindgen_test::wasm_bindgen_test;

        use super::*;

        pub(super) struct RestoreGlobal(Function);

        impl RestoreGlobal {
            pub(super) fn new(restore: Function) -> Self {
                Self(restore)
            }
        }

        #[wasm_bindgen_test]
        async fn streamed_send_keeps_original_scope_under_foreign_request_polling() {
            let _timers = NumericTimeoutGuard::install();
            let original = OutboundScopeOwner::callback();
            let foreign = OutboundScopeOwner::callback();
            let client = HttpClient::with_client(CloudflareOutboundClient::new());
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
                        OutboundRequest::post("https://fixture.invalid/")
                            .expect("child request")
                            .body(source),
                    );
                    *retained.borrow_mut() = Some(send);
                }
                Poll::<Option<Result<Bytes, EdgeError>>>::Pending
            }));
            let mut send = original.with(|| {
                client.send(
                    OutboundRequest::post("https://fixture.invalid/")
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
        }

        #[wasm_bindgen_test]
        async fn scope_exit_aborts_retained_begin_and_batch_fetches_exactly_once() {
            let _timers = NumericTimeoutGuard::install();
            let (probe, _restore) = fetch_probe(true);
            let callback = OutboundScopeOwner::callback();
            let client = HttpClient::with_client(CloudflareOutboundClient::new());
            let (pending, batch) = callback
                .run(async {
                    let pending = client
                        .begin(
                            OutboundRequest::get("https://fixture.invalid/begin").expect("request"),
                        )
                        .await
                        .expect("begin");
                    let batch = client.start_batch_until(
                        vec![
                            OutboundRequest::get("https://fixture.invalid/first").expect("first"),
                            OutboundRequest::get("https://fixture.invalid/second").expect("second"),
                        ],
                        Deadline::after(Duration::from_secs(10)),
                    );
                    HostEventYield::after(Duration::from_millis(10))
                        .await
                        .expect("scheduled fetches");
                    (pending, batch)
                })
                .await;
            assert_eq!(
                Reflect::get(&probe, &JsValue::from_str("calls"))
                    .expect("calls")
                    .as_f64(),
                Some(3.0_f64)
            );
            assert_eq!(
                Reflect::get(&probe, &JsValue::from_str("aborts"))
                    .expect("aborts")
                    .as_f64(),
                Some(0.0_f64)
            );
            drop(callback);
            assert_eq!(
                Reflect::get(&probe, &JsValue::from_str("aborts"))
                    .expect("aborts")
                    .as_f64(),
                Some(3.0_f64)
            );
            HostEventYield::after(Duration::from_millis(10))
                .await
                .expect("cancelled tasks retire");
            assert!(matches!(
                pending.collect().await,
                Err(EdgeError::Internal { .. })
            ));
            batch.collect().await.expect_err("scope revoked driver");
            assert_eq!(
                Reflect::get(&probe, &JsValue::from_str("aborts"))
                    .expect("aborts")
                    .as_f64(),
                Some(3.0_f64)
            );
        }

        #[wasm_bindgen_test]
        async fn closed_scope_rejects_scheduling_before_work_is_polled() {
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
        }

        #[wasm_bindgen_test]
        async fn scheduled_work_allows_reentrant_cancellation_while_polling() {
            let _timers = NumericTimeoutGuard::install();
            let owner = Rc::new(RefCell::new(None::<OutboundExchangeOwner>));
            let cancelled = Rc::clone(&owner);
            let polls = Rc::new(Cell::new(0_usize));
            let counted = Rc::clone(&polls);
            let work = poll_fn(move |_| {
                counted.set(counted.get().saturating_add(1));
                let cancel_owner = cancelled.borrow_mut().take();
                drop(cancel_owner);
                Poll::Pending
            })
            .boxed_local();
            *owner.borrow_mut() = Some(spawn_owned(work).expect("local scheduler"));
            HostEventYield::after(Duration::from_millis(10))
                .await
                .expect("unrelated host wait");
            assert_eq!(polls.get(), 1, "cancelled work must not be polled again");
        }

        #[wasm_bindgen_test]
        async fn cancelling_idle_work_retires_the_wasm_task_and_source() {
            let _timers = NumericTimeoutGuard::install();
            let source = Rc::new(());
            let released = Rc::downgrade(&source);
            let entered = Rc::new(Cell::new(false));
            let observed = Rc::clone(&entered);
            let (runner, owner) = OutboundTask::new(Box::pin(async move {
                observed.set(true);
                pending::<()>().await;
                drop(source);
            }))
            .expect("task setup");
            let running = Rc::new(());
            let retired = Rc::downgrade(&running);
            spawn_local(async move {
                runner.await;
                drop(running);
            });
            HostEventYield::after(Duration::from_millis(10))
                .await
                .expect("work enters pending");
            assert!(entered.get(), "work entered pending");
            drop(owner);
            assert!(
                released.upgrade().is_none(),
                "cancellation immediately releases the source"
            );
            HostEventYield::after(Duration::from_millis(10))
                .await
                .expect("cancelled task retires");
            assert!(
                retired.upgrade().is_none(),
                "no retained task/waker/state cycle"
            );
        }

        fn fetch_probe(pending: bool) -> (JsValue, RestoreGlobal) {
            let probe = Function::new_with_args("pending", "const original = globalThis.fetch; const probe = {calls:0, signal:null, aborts:0}; globalThis.fetch = (request, init) => { probe.calls++; probe.signal = init.signal; init.signal.addEventListener('abort', () => {probe.aborts++;}, {once:true}); if (init.encodeResponseBody !== 'manual' || request.redirect !== 'manual') throw new Error('manual fetch policy lost'); return pending ? new Promise(() => {}) : Promise.resolve(new Response('ready')); }; probe.restore = () => {globalThis.fetch = original;}; return probe;")
                .call1(&JsValue::UNDEFINED, &JsValue::from_bool(pending)).expect("fetch probe");
            let restore = Reflect::get(&probe, &JsValue::from_str("restore"))
                .expect("restore")
                .dyn_into::<Function>()
                .expect("restore function");
            (probe, RestoreGlobal::new(restore))
        }

        #[wasm_bindgen_test]
        async fn clamped_batch_cutoff_uses_actual_observation_cutoff() {
            for (elapsed_hours, unresolved) in [(168_u64, false), (192, true)] {
                let start = MonotonicInstant::now();
                let now = Arc::new(Mutex::new(start));
                let clock = controlled_clock(Arc::clone(&now));
                let client = HttpClient::with_client(CloudflareOutboundClient::with_time(
                    native_time(clock),
                ));
                let cutoff = Deadline::at_instant(
                    start
                        .checked_add(Duration::from_hours(192))
                        .expect("eight-day cutoff"),
                );
                let requests = repeat_with(|| {
                    OutboundRequest::get("https://fixture.invalid/")
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
                        assert!(
                            matches!(slot, OutboundCollectedSlot::Resolved(result) if matches!(result.outcome, Err(EdgeError::GatewayTimeout { cause: BudgetSource::BatchCutoff, .. })))
                        );
                    }
                }
            }
        }

        #[wasm_bindgen_test]
        async fn unpolled_batch_dispatches_all_slots() {
            let _timers = NumericTimeoutGuard::install();
            let (probe, _restore) = fetch_probe(false);
            let client = HttpClient::with_client(CloudflareOutboundClient::new());
            let batch = client.start_batch_until(
                vec![
                    OutboundRequest::get("https://fixture.invalid/first").expect("first"),
                    OutboundRequest::get("https://fixture.invalid/second").expect("second"),
                ],
                Deadline::after(Duration::from_secs(1)),
            );
            HostEventYield::after(Duration::from_millis(10))
                .await
                .expect("unrelated host wait");
            assert_eq!(
                Reflect::get(&probe, &JsValue::from_str("calls"))
                    .expect("calls")
                    .as_f64(),
                Some(2.0_f64)
            );
            let results = batch.collect().await.expect("batch");
            assert!(results.slots().iter().all(|slot| matches!(slot, OutboundCollectedSlot::Resolved(result) if result.outcome.is_ok())));
        }

        #[wasm_bindgen_test]
        async fn begin_dispatches_without_collection_and_drop_aborts() {
            let _timers = NumericTimeoutGuard::install();
            let (probe, _restore) = fetch_probe(true);
            let client = HttpClient::with_client(CloudflareOutboundClient::new());
            let pending = client
                .begin(OutboundRequest::get("https://fixture.invalid/").expect("request"))
                .await
                .expect("begin");
            HostEventYield::after(Duration::from_millis(10))
                .await
                .expect("unrelated host wait");
            assert_eq!(
                Reflect::get(&probe, &JsValue::from_str("calls"))
                    .expect("calls")
                    .as_f64(),
                Some(1.0_f64)
            );
            let signal = Reflect::get(&probe, &JsValue::from_str("signal"))
                .expect("signal")
                .dyn_into::<web_sys::AbortSignal>()
                .expect("abort signal");
            assert!(!signal.aborted());
            drop(pending);
            assert!(
                signal.aborted(),
                "cancellation releases fetch without another guest poll"
            );
        }

        #[wasm_bindgen_test]
        async fn collected_begin_stream_stays_live_and_disarms_clean_eof() {
            let _timers = NumericTimeoutGuard::install();
            let (probe, _restore) = fetch_probe(false);
            let client = HttpClient::with_client(CloudflareOutboundClient::new());
            let pending = client
                .begin(
                    OutboundRequest::get("https://fixture.invalid/")
                        .expect("request")
                        .stream_response(),
                )
                .await
                .expect("begin");
            let response = pending.collect().await.expect("live response");
            let signal = Reflect::get(&probe, &JsValue::from_str("signal"))
                .expect("signal")
                .dyn_into::<web_sys::AbortSignal>()
                .expect("abort signal");
            assert!(
                !signal.aborted(),
                "collect disarms pending cancellation without aborting the body"
            );
            assert_eq!(
                response
                    .into_bytes_bounded(5)
                    .await
                    .expect("complete live stream"),
                "ready"
            );
            assert!(
                !signal.aborted(),
                "clean EOF disarms the one transport guard"
            );
        }

        #[wasm_bindgen_test]
        async fn settlement_timer_wakes_without_body_data() {
            let _timers = NumericTimeoutGuard::install();
            let (probe, _restore) = fetch_probe(false);
            let reads = Rc::new(Cell::new(0_usize));
            let observed = Rc::clone(&reads);
            let body = Body::from_stream(stream::poll_fn(move |_| {
                observed.set(observed.get().saturating_add(1));
                Poll::Pending
            }));
            let client = HttpClient::with_client(CloudflareOutboundClient::new());
            let error = client
                .send(
                    OutboundRequest::get("https://fixture.invalid/")
                        .expect("request")
                        .body(body)
                        .timeout(Duration::from_millis(20)),
                )
                .await
                .expect_err("settlement timeout");
            assert!(matches!(
                error,
                EdgeError::GatewayTimeout {
                    cause: BudgetSource::PerCallTimeout,
                    ..
                }
            ));
            assert!(reads.get() > 0);
            assert_eq!(
                Reflect::get(&probe, &JsValue::from_str("calls"))
                    .expect("calls")
                    .as_f64(),
                Some(0.0_f64)
            );
        }

        #[wasm_bindgen_test]
        async fn review_cross_chunk_regression_releases_retained_source() {
            use std::future::pending;

            let _timers = NumericTimeoutGuard::install();
            let start = MonotonicInstant::now();
            let budget = test_budget(start, Duration::from_secs(10));
            let advanced = start.checked_add(Duration::from_millis(2)).unwrap();
            let regressed = start.checked_add(Duration::from_millis(1)).unwrap();
            let observed = Arc::new(Mutex::new(start));
            let pair = native_time(controlled_clock(Arc::clone(&observed)));
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
            let controller = web_sys::AbortController::new().unwrap();
            let signal = controller.signal();
            let mut guarded =
                finish_fixture_stream(source, AbortGuard::new(controller), budget, pair)
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
            assert!(signal.aborted());
        }

        #[wasm_bindgen_test]
        async fn review_host_yield_preserves_supplied_timer_failure_detail() {
            let start = MonotonicInstant::now();
            let budget = test_budget(start, Duration::from_secs(10));
            let timer_detail = ProviderFailureDetail::Cloudflare {
                phase: ProviderFailurePhase::Send,
                cause: CloudflareFailureCause::AbortError,
            };
            let source_detail = ProviderFailureDetail::Cloudflare {
                phase: ProviderFailurePhase::ResponseBodyRead,
                cause: CloudflareFailureCause::TypeError,
            };
            let pair = OutboundTime::new(constant_clock(start), move |_, _| {
                Box::pin(async move {
                    Err(preserve_timeout_detail(
                        EdgeError::gateway_timeout_caused(
                            "supplied timer failure",
                            BudgetSource::Unspecified,
                        ),
                        Some(timer_detail),
                    ))
                })
            });
            let original = EdgeError::bad_gateway_with_detail(
                "source failed",
                BadGatewayReason::Transport,
                source_detail,
            );
            let mut previous = start;
            let selected =
                terminal_error_after_host_event(original, budget, &pair, &mut previous).await;
            assert_eq!(selected.message(), "supplied timer failure");
            assert_eq!(selected.provider_failure_detail(), Some(&timer_detail));
        }

        #[wasm_bindgen_test]
        async fn review_host_yield_keeps_prior_source_high_water() {
            let start = MonotonicInstant::now();
            let budget = test_budget(start, Duration::from_secs(10));
            let mut previous = start.checked_add(Duration::from_millis(2)).unwrap();
            let regressed = start.checked_add(Duration::from_millis(1)).unwrap();
            let pair = native_time(constant_clock(regressed));
            let error = yield_host_event(budget, &pair, &mut previous, None)
                .await
                .expect_err("host yield must retain the preceding source observation");
            assert!(matches!(error, EdgeError::Internal { .. }));
        }

        #[derive(Default)]
        struct WakeCount(AtomicUsize);

        impl Wake for WakeCount {
            fn wake(self: Arc<Self>) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
            fn wake_by_ref(self: &Arc<Self>) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }

        #[wasm_bindgen_test]
        fn timer_replaces_waker_and_cancels_only_pending_registration() {
            let saved = Function::new_no_args("const set = globalThis.setTimeout, clear = globalThis.clearTimeout; globalThis.__edgezeroTimer = {arms:0, clears:0}; globalThis.setTimeout = (callback, milliseconds) => { const t = globalThis.__edgezeroTimer; t.arms++; t.callback=callback; t.milliseconds=milliseconds; return 123; }; globalThis.clearTimeout = () => {globalThis.__edgezeroTimer.clears++;}; return () => {globalThis.setTimeout=set; globalThis.clearTimeout=clear; delete globalThis.__edgezeroTimer;};")
                .call0(&JsValue::UNDEFINED).unwrap().dyn_into::<Function>().unwrap();
            let guard = RestoreGlobal(saved);
            let first = Arc::new(WakeCount::default());
            let second = Arc::new(WakeCount::default());
            let first_waker = Waker::from(Arc::clone(&first));
            let second_waker = Waker::from(Arc::clone(&second));
            let mut timer = Box::pin(HostEventYield::after(Duration::from_nanos(1)));
            assert!(
                timer
                    .as_mut()
                    .poll(&mut Context::from_waker(&first_waker))
                    .is_pending()
            );
            assert!(
                timer
                    .as_mut()
                    .poll(&mut Context::from_waker(&second_waker))
                    .is_pending()
            );
            let state = Reflect::get(&global(), &JsValue::from_str("__edgezeroTimer")).unwrap();
            assert_eq!(
                Reflect::get(&state, &JsValue::from_str("arms"))
                    .unwrap()
                    .as_f64(),
                Some(1.0_f64)
            );
            assert_eq!(
                Reflect::get(&state, &JsValue::from_str("milliseconds"))
                    .unwrap()
                    .as_f64(),
                Some(1.0_f64)
            );
            let callback = Reflect::get(&state, &JsValue::from_str("callback"))
                .unwrap()
                .dyn_into::<Function>()
                .unwrap();
            callback.call0(&JsValue::UNDEFINED).unwrap();
            assert_eq!(first.0.load(Ordering::SeqCst), 0);
            assert_eq!(second.0.load(Ordering::SeqCst), 1);
            assert!(matches!(
                timer.as_mut().poll(&mut Context::from_waker(&second_waker)),
                Poll::Ready(Ok(()))
            ));
            assert!(timer.callback.is_none());
            assert!(timer.waker.borrow().is_none());
            drop(timer);
            assert_eq!(
                Reflect::get(&state, &JsValue::from_str("clears"))
                    .unwrap()
                    .as_f64(),
                Some(0.0_f64)
            );
            let mut pending = Box::pin(HostEventYield::new());
            assert!(
                pending
                    .as_mut()
                    .poll(&mut Context::from_waker(&first_waker))
                    .is_pending()
            );
            drop(pending);
            assert_eq!(
                Reflect::get(&state, &JsValue::from_str("clears"))
                    .unwrap()
                    .as_f64(),
                Some(1.0_f64)
            );
            drop(guard);
        }

        #[wasm_bindgen_test]
        fn timer_validates_cancellation_before_registering() {
            let restore = Function::new_no_args("const set = globalThis.setTimeout, clear = globalThis.clearTimeout; globalThis.__edgezeroTimerRegistrations = 0; globalThis.setTimeout = () => { globalThis.__edgezeroTimerRegistrations++; return 123; }; globalThis.clearTimeout = undefined; return () => { globalThis.setTimeout = set; globalThis.clearTimeout = clear; delete globalThis.__edgezeroTimerRegistrations; };")
                .call0(&JsValue::UNDEFINED).unwrap().dyn_into::<Function>().unwrap();
            let guard = RestoreGlobal(restore);
            let mut timer = Box::pin(HostEventYield::new());
            let outcome = timer.as_mut().poll(&mut Context::from_waker(Waker::noop()));
            let registrations = Reflect::get(
                &global(),
                &JsValue::from_str("__edgezeroTimerRegistrations"),
            )
            .unwrap();
            drop(timer);
            drop(guard);
            assert!(matches!(
                outcome,
                Poll::Ready(Err(EdgeError::Internal { .. }))
            ));
            assert_eq!(registrations.as_f64(), Some(0.0_f64));
        }

        #[expect(
            clippy::missing_trait_methods,
            reason = "`Drop::pin_drop` is unstable (`pin_ergonomics`) and conflicts with `drop`"
        )]
        impl Drop for RestoreGlobal {
            fn drop(&mut self) {
                let _restored = self.0.call0(&JsValue::UNDEFINED);
            }
        }

        fn assert_same_public_error(plain: EdgeError, diagnosed: EdgeError) {
            assert_eq!(plain.to_string(), diagnosed.to_string());
            assert_eq!(plain.message(), diagnosed.message());
            assert_eq!(plain.kind(), diagnosed.kind());
            assert_eq!(plain.status(), diagnosed.status());
            assert!(
                !format!("{:?}", diagnosed.provider_failure_detail())
                    .contains("provider-secret-token")
            );
            let before = plain.into_response().expect("plain wire");
            let after = diagnosed.into_response().expect("diagnosed wire");
            assert_eq!(before.status(), after.status());
            assert_eq!(before.headers(), after.headers());
            assert_eq!(before.body().as_bytes(), after.body().as_bytes());
        }

        #[wasm_bindgen_test]
        fn provider_diagnostics_preserve_preparation_errors_without_changing_sources() {
            let mut headers = HeaderMap::new();
            headers.insert(
                "x-test",
                HeaderValue::from_bytes("\u{100}provider-secret-token".as_bytes())
                    .expect("valid raw header"),
            );
            let header_error = build_headers(&headers).expect_err("Workers header rejection");
            let detail = header_error
                .provider_failure_detail()
                .expect("header detail");
            assert_eq!(
                *detail,
                ProviderFailureDetail::Cloudflare {
                    phase: ProviderFailurePhase::RequestPreparation,
                    cause: CloudflareFailureCause::TypeError,
                }
            );
            assert!(header_error.inner().expect("source").is::<worker::Error>());
            let header_source = header_error.inner().expect("source").to_string();
            assert_same_public_error(
                EdgeError::internal(anyhow::anyhow!(header_source)),
                header_error,
            );

            let request_error = build_worker_request(
                &Method::GET,
                "https://example.com/?token=provider-secret-token",
                &HeaderMap::new(),
                Some(JsValue::from_str("body")),
                OutboundCachePolicy::PlatformDefault,
            )
            .expect_err("GET body rejection");
            assert_eq!(
                request_error.provider_failure_detail(),
                Some(&ProviderFailureDetail::Cloudflare {
                    phase: ProviderFailurePhase::RequestPreparation,
                    cause: CloudflareFailureCause::Unknown,
                })
            );
            assert!(request_error.inner().expect("source").is::<worker::Error>());
            let request_source = request_error.inner().expect("source").to_string();
            assert_same_public_error(
                EdgeError::internal(anyhow::anyhow!(request_source)),
                request_error,
            );

            headers.insert(
                "x-test",
                HeaderValue::from_bytes(b"\xff").expect("raw header"),
            );
            let invalid = build_headers(&headers).expect_err("non-UTF8 rejection");
            assert!(invalid.provider_failure_detail().is_none());
            assert_eq!(invalid.status(), StatusCode::BAD_REQUEST);

            let mut response_headers = Headers::new();
            headers.insert(
                "x-test",
                HeaderValue::from_bytes("\u{100}".as_bytes()).expect("raw header"),
            );
            let response_error =
                super::super::copy_header_values(&headers, &mut response_headers, |_name| {
                    EdgeError::internal(anyhow::anyhow!("invalid response header"))
                })
                .expect_err("shared sink rejection");
            assert!(response_error.provider_failure_detail().is_none());
        }

        #[wasm_bindgen_test]
        fn provider_diagnostics_keep_original_worker_source_identity() {
            let original =
                EdgeError::internal(worker::Error::RustError("provider-secret-token".to_owned()));
            let pointer = ptr::from_ref(
                original
                    .inner()
                    .expect("source")
                    .downcast_ref::<worker::Error>()
                    .expect("worker source"),
            );
            let mapped = diagnose_worker_preparation_error(original);
            let retained = mapped
                .inner()
                .expect("source")
                .downcast_ref::<worker::Error>()
                .expect("retained worker source");
            assert!(ptr::eq(pointer, retained));
            assert!(
                matches!(retained, worker::Error::RustError(text) if text == "provider-secret-token")
            );
            assert_eq!(
                mapped.provider_failure_detail(),
                Some(&ProviderFailureDetail::Cloudflare {
                    phase: ProviderFailurePhase::RequestPreparation,
                    cause: CloudflareFailureCause::Unknown,
                })
            );
            assert_same_public_error(
                EdgeError::internal(worker::Error::RustError("provider-secret-token".to_owned())),
                mapped,
            );
        }

        #[wasm_bindgen_test]
        async fn provider_diagnostics_preserve_preparation_setup_exceptions() {
            for (setup, message, expected) in [
                (
                    "const old = globalThis.AbortController; globalThis.AbortController = class { constructor() { throw new TypeError('provider-secret-token'); } }; return () => { globalThis.AbortController = old; };",
                    "failed to construct abort controller",
                    Some(CloudflareFailureCause::TypeError),
                ),
                (
                    "const old = Reflect.set; Reflect.set = () => { throw new DOMException('provider-secret-token', 'AbortError'); }; return () => { Reflect.set = old; };",
                    "failed to set manual response encoding",
                    Some(CloudflareFailureCause::AbortError),
                ),
                (
                    "const old = Reflect.set; Reflect.set = () => false; return () => { Reflect.set = old; };",
                    "runtime refused manual response encoding",
                    None,
                ),
            ] {
                let request = build_worker_request(
                    &Method::GET,
                    "https://example.com/?token=provider-secret-token",
                    &HeaderMap::new(),
                    None,
                    OutboundCachePolicy::PlatformDefault,
                )
                .expect("valid request");
                let start = MonotonicInstant::now();
                let budget = DispatchBudget {
                    cause: BudgetSource::PerCallTimeout,
                    deadline: Deadline::at_instant(
                        start.checked_add(Duration::from_secs(1)).expect("deadline"),
                    ),
                    duration: Duration::from_secs(1),
                };
                let outcome = {
                    let restore: Function = Function::new_no_args(setup)
                        .call0(&JsValue::UNDEFINED)
                        .expect("patch setup")
                        .dyn_into()
                        .expect("restore function");
                    let _restore = RestoreGlobal::new(restore);
                    raw_fetch(
                        request,
                        budget,
                        &native_time(MonotonicClock::new(move || start)),
                    )
                    .await
                };
                let error = outcome.err().expect("setup refusal");
                assert_eq!(
                    error.provider_failure_detail(),
                    expected
                        .map(|cause| ProviderFailureDetail::Cloudflare {
                            phase: ProviderFailurePhase::RequestPreparation,
                            cause,
                        })
                        .as_ref()
                );
                assert_same_public_error(EdgeError::internal(anyhow::anyhow!(message)), error);
            }
        }

        #[wasm_bindgen_test]
        fn provider_diagnostics_fetch_settlement_keeps_only_observed_failures() {
            let value = Function::new_no_args("return new TypeError('provider-secret-token');")
                .call0(&JsValue::UNDEFINED)
                .expect("exception");
            let failed = finish_fetch_result(
                Err(value),
                Err(timeout_error(BudgetSource::RequestDeadline)),
            )
            .expect_err("deadline wins");
            assert_eq!(
                failed.provider_failure_detail(),
                Some(&ProviderFailureDetail::Cloudflare {
                    phase: ProviderFailurePhase::Send,
                    cause: CloudflareFailureCause::TypeError,
                })
            );
            assert!(matches!(
                failed,
                EdgeError::GatewayTimeout {
                    cause: BudgetSource::RequestDeadline,
                    ..
                }
            ));
            let late_ok = finish_fetch_result(
                Ok(JsValue::UNDEFINED),
                Err(timeout_error(BudgetSource::RequestDeadline)),
            )
            .expect_err("late success");
            assert!(late_ok.provider_failure_detail().is_none());
        }

        #[wasm_bindgen_test]
        async fn provider_diagnostics_survive_reset_content_deadline_checks() {
            let _timer_guard = NumericTimeoutGuard::install();
            for during_yield in [false, true] {
                let start = MonotonicInstant::now();
                let budget = test_budget(start, Duration::from_secs(1));
                let observed = Arc::new(Mutex::new(start));
                let pair = native_time(controlled_clock(Arc::clone(&observed)));
                let late = Arc::clone(&observed);
                let classify = Closure::<dyn FnMut()>::new(move || {
                    *late.lock().expect("clock") = budget.deadline.instant();
                });
                let raw = Function::new_with_args("classify, delayed", "const error = new Error('provider-secret-token'); Object.defineProperty(error, 'name', {get() {classify(); return 'TypeError';}}); return new Response(new ReadableStream({start(c) {if (delayed) setTimeout(() => c.error(error), 0); else c.error(error);}}));")
                    .call2(&JsValue::UNDEFINED, classify.as_ref(), &JsValue::from_bool(during_yield)).expect("error response")
                    .dyn_into::<web_sys::Response>().expect("native response");
                let native = WorkerResponse::from(raw).with_status(205);
                let result = finish_fixture_response(
                    native,
                    AbortGuard::new(web_sys::AbortController::new().expect("controller")),
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
                    pair,
                )
                .await;
                let error = result.expect_err("expiry overrides body failure");
                assert!(matches!(
                    error,
                    EdgeError::GatewayTimeout {
                        cause: BudgetSource::PerCallTimeout,
                        ..
                    }
                ));
                assert_eq!(
                    error.provider_failure_detail(),
                    Some(&ProviderFailureDetail::Cloudflare {
                        phase: ProviderFailurePhase::ResponseBodyRead,
                        cause: CloudflareFailureCause::TypeError,
                    })
                );
            }
        }

        #[wasm_bindgen_test]
        async fn provider_diagnostics_survive_post_ready_expiry_and_host_yield() {
            let _timer_guard = NumericTimeoutGuard::install();
            let detail = ProviderFailureDetail::Cloudflare {
                phase: ProviderFailurePhase::ResponseBodyRead,
                cause: CloudflareFailureCause::TypeError,
            };
            for upload in [false, true] {
                let start = MonotonicInstant::now();
                let budget = test_budget(start, Duration::from_secs(1));
                let observed = Arc::new(Mutex::new(start));
                let clock = controlled_clock(Arc::clone(&observed));
                let source = stream::once(async move {
                    *observed.lock().expect("clock") = budget.deadline.instant();
                    Err(EdgeError::bad_gateway_with_detail(
                        "source failed",
                        BadGatewayReason::Transport,
                        detail,
                    ))
                })
                .boxed_local();
                let mut wrapped = if upload {
                    upload_stream(source, 32, budget, native_time(clock))
                } else {
                    finish_fixture_stream(
                        source,
                        AbortGuard::new(web_sys::AbortController::new().expect("controller")),
                        budget,
                        native_time(clock),
                    )
                    .await
                    .expect("shared response stream")
                };
                let error = wrapped.next().await.expect("terminal").expect_err("expiry");
                assert!(matches!(error, EdgeError::GatewayTimeout { .. }));
                assert_eq!(error.provider_failure_detail(), Some(&detail));
            }
            let start = MonotonicInstant::now();
            let budget = test_budget(start, Duration::from_secs(1));
            let clock = constant_clock(budget.deadline.instant());
            let mut previous = start;
            let error = terminal_error_after_host_event(
                EdgeError::bad_gateway_with_detail(
                    "source failed",
                    BadGatewayReason::Transport,
                    detail,
                ),
                budget,
                &native_time(clock),
                &mut previous,
            )
            .await;
            assert!(matches!(error, EdgeError::GatewayTimeout { .. }));
            assert_eq!(error.provider_failure_detail(), Some(&detail));
        }

        #[wasm_bindgen_test]
        fn provider_diagnostics_fetch_names_are_closed_and_never_infer_timeouts() {
            for (script, expected) in [
                (
                    "return new TypeError('provider-secret-token');",
                    CloudflareFailureCause::TypeError,
                ),
                (
                    "return Object.assign(new Error('provider-secret-token'), {name:'AbortError'});",
                    CloudflareFailureCause::AbortError,
                ),
                (
                    "return Object.assign(new Error('provider-secret-token'), {name:'secret-name'});",
                    CloudflareFailureCause::Unknown,
                ),
                (
                    "return Object.assign(new Error('provider-secret-token'), {name:17});",
                    CloudflareFailureCause::Unknown,
                ),
                (
                    "return Object.defineProperty({}, 'name', {get() {throw new Error('getter-secret')}});",
                    CloudflareFailureCause::Unknown,
                ),
                (
                    "return 'TypeError: provider-secret-token';",
                    CloudflareFailureCause::Unknown,
                ),
            ] {
                let value = Function::new_no_args(script)
                    .call0(&JsValue::UNDEFINED)
                    .expect("fixture value");
                let mapped = map_fetch_failure(&value);
                assert_eq!(
                    mapped.provider_failure_detail(),
                    Some(&ProviderFailureDetail::Cloudflare {
                        phase: ProviderFailurePhase::Send,
                        cause: expected,
                    })
                );
                assert!(matches!(
                    mapped,
                    EdgeError::BadGateway {
                        reason: BadGatewayReason::Unspecified,
                        ..
                    }
                ));
                assert_eq!(mapped.message(), "outbound fetch failed");
                assert_eq!(mapped.to_string(), "outbound fetch failed");
                assert!(!format!("{mapped:?}").contains("secret"));
                assert_eq!(
                    worker_failure_cause(&worker::Error::Internal(value)),
                    expected
                );
            }
            for (name, expected) in [
                ("TypeError", CloudflareFailureCause::TypeError),
                ("AbortError", CloudflareFailureCause::AbortError),
            ] {
                let value = Function::new_with_args(
                    "name",
                    "return Object.assign(new Error('provider-secret-token'), {name});",
                )
                .call1(&JsValue::UNDEFINED, &JsValue::from_str(name))
                .expect("actual Error");
                assert_eq!(worker_failure_cause(&worker::Error::from(value)), expected);
            }
            for error in [
                worker::Error::JsError("TypeError: provider-secret-token".to_owned()),
                worker::Error::RustError("AbortError: provider-secret-token".to_owned()),
            ] {
                assert_eq!(
                    worker_failure_cause(&error),
                    CloudflareFailureCause::Unknown
                );
            }
        }

        #[wasm_bindgen_test]
        async fn provider_diagnostics_preserve_response_stream_exception_without_text() {
            for (name, expected) in [
                ("TypeError", CloudflareFailureCause::TypeError),
                ("AbortError", CloudflareFailureCause::AbortError),
                ("secret-name", CloudflareFailureCause::Unknown),
            ] {
                let factory = Function::new_with_args(
                    "name",
                    "return new Response(new ReadableStream({ start(controller) { const error = new Error('provider-secret-token'); error.name = name; controller.error(error); } }));",
                );
                let native = factory
                    .call1(&JsValue::UNDEFINED, &JsValue::from_str(name))
                    .expect("raw response")
                    .dyn_into::<web_sys::Response>()
                    .expect("response type");
                let mut response = WorkerResponse::from(native);
                let mut source = response_stream(&mut response).expect("stream acquisition");
                let mapped = source
                    .next()
                    .await
                    .expect("terminal error")
                    .expect_err("upstream failure");
                assert_eq!(
                    mapped.provider_failure_detail(),
                    Some(&ProviderFailureDetail::Cloudflare {
                        phase: ProviderFailurePhase::ResponseBodyRead,
                        cause: expected,
                    })
                );
                let plain = EdgeError::bad_gateway_with_reason(
                    "upstream response body failed",
                    BadGatewayReason::Transport,
                );
                assert_eq!(plain.to_string(), mapped.to_string());
                assert_eq!(plain.message(), mapped.message());
                assert_eq!(plain.status(), mapped.status());
                assert_eq!(plain.kind(), mapped.kind());
                assert!(!format!("{:?}", mapped.provider_failure_detail()).contains("secret"));
                let before = plain.into_response().expect("plain wire");
                let after = mapped.into_response().expect("diagnosed wire");
                assert_eq!(before.status(), after.status());
                assert_eq!(before.headers(), after.headers());
                assert_eq!(before.body().as_bytes(), after.body().as_bytes());
            }
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

        fn constant_clock(now: MonotonicInstant) -> MonotonicClock {
            MonotonicClock::new(move || now)
        }

        fn controlled_clock(observed: Arc<Mutex<MonotonicInstant>>) -> MonotonicClock {
            MonotonicClock::new(move || *observed.lock().expect("clock"))
        }

        fn expire_at_host_yield(
            budget: DispatchBudget,
            observed: Arc<Mutex<MonotonicInstant>>,
        ) -> OutboundTime {
            use std::sync::atomic::{AtomicUsize, Ordering};
            let arms = Arc::new(AtomicUsize::new(0));
            OutboundTime::new(
                controlled_clock(Arc::clone(&observed)),
                move |_deadline, _clock| {
                    let during_yield = arms.fetch_add(1, Ordering::SeqCst) > 0;
                    let late = Arc::clone(&observed);
                    Box::pin(async move {
                        if during_yield {
                            HostEventYield::new().await?;
                            *late.lock().expect("clock") = budget.deadline.instant();
                        } else {
                            HostEventYield::after(budget.duration).await?;
                        }
                        Ok(())
                    })
                },
            )
        }

        fn empty_worker_response(
            status: StatusCode,
            content_length: Option<&str>,
        ) -> WorkerResponse {
            let headers = Headers::new();
            if let Some(value) = content_length {
                headers
                    .set("content-length", value)
                    .expect("content-length");
            }
            WorkerResponse::empty()
                .expect("empty response")
                .with_status(status.as_u16())
                .with_headers(headers)
        }

        fn test_budget(start: MonotonicInstant, duration: Duration) -> DispatchBudget {
            DispatchBudget {
                cause: BudgetSource::PerCallTimeout,
                deadline: Deadline::at_instant(start.checked_add(duration).expect("deadline")),
                duration,
            }
        }

        #[wasm_bindgen_test]
        async fn upload_cap_boundaries_preserve_typed_origins() {
            let start = MonotonicInstant::now();
            let budget = test_budget(start, Duration::from_secs(1));
            for len in [7_usize, 8, 9] {
                let clock = MonotonicClock::new(move || start);
                let source = stream::iter([Ok(Bytes::from(vec![b'x'; len]))]).boxed_local();
                let mut bounded = upload_stream(source, 8, budget, native_time(clock));
                let result = bounded.next().await.expect("upload item");
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
            let source_error =
                EdgeError::payload_too_large(edgezero_core::RequestBodyOverflowOrigin::Inbound);
            let mut source = upload_stream(
                stream::iter([Err(source_error)]).boxed_local(),
                8,
                budget,
                native_time(MonotonicClock::new(move || start)),
            );
            let error = source
                .next()
                .await
                .expect("item")
                .expect_err("source overflow");
            assert_eq!(
                error.request_body_overflow_origin(),
                Some(edgezero_core::RequestBodyOverflowOrigin::Inbound)
            );
        }

        #[wasm_bindgen_test]
        fn raw_fetch_timer_arms_from_final_remaining_budget() {
            let start = MonotonicInstant::now();
            let observed = start
                .checked_add(Duration::from_millis(3))
                .expect("observed instant");
            let budget = test_budget(start, Duration::from_millis(10));
            let sampled = Arc::new(Mutex::new(false));
            let sampled_by_clock = Arc::clone(&sampled);
            let clock = MonotonicClock::new(move || {
                *sampled_by_clock.lock().expect("sampled lock") = true;
                observed
            });

            let (value, remaining) = begin_fetch(budget, &clock, || {
                assert!(
                    *sampled.lock().expect("sampled lock"),
                    "budget must be sampled before dispatch"
                );
                42_u8
            })
            .expect("live budget");

            assert_eq!(value, 42);
            assert_eq!(remaining, Duration::from_millis(7));
        }

        #[wasm_bindgen_test]
        async fn method_entry_and_preflight_elapsed_use_the_injected_clock() {
            let start = MonotonicInstant::now();
            let completed = start
                .checked_add(Duration::from_millis(9))
                .expect("completed instant");
            let client = HttpClient::with_client(CloudflareOutboundClient::with_time(native_time(
                scripted_clock(vec![start, completed]),
            )));
            let request = OutboundRequest::get("https://example.com/")
                .expect("request")
                .stream_response();

            let results = client
                .start_batch_until(
                    vec![request],
                    Deadline::at_instant(
                        start
                            .checked_add(Duration::from_secs(1))
                            .expect("cutoff instant"),
                    ),
                )
                .collect()
                .await
                .expect("valid batch driver");

            let slot = resolved_slot(&results.slots()[0]);
            assert_eq!(slot.elapsed, Duration::from_millis(9));
            assert!(matches!(slot.outcome, Err(EdgeError::BadRequest { .. })));
        }

        #[wasm_bindgen_test]
        async fn backwards_clock_fails_slot_without_invalid_elapsed() {
            let start = MonotonicInstant::now();
            let earlier = start
                .checked_sub(Duration::from_millis(1))
                .expect("earlier instant");
            let client = HttpClient::with_client(CloudflareOutboundClient::with_time(native_time(
                scripted_clock(vec![start, earlier]),
            )));
            let request = OutboundRequest::get("https://example.com/")
                .expect("request")
                .stream_response();

            let results = client
                .start_batch_until(
                    vec![request],
                    Deadline::at_instant(
                        start
                            .checked_add(Duration::from_secs(1))
                            .expect("cutoff instant"),
                    ),
                )
                .collect()
                .await
                .expect("valid batch driver");

            let slot = resolved_slot(&results.slots()[0]);
            assert_eq!(slot.elapsed, Duration::ZERO);
            assert!(matches!(slot.outcome, Err(EdgeError::Internal { .. })));
        }

        #[wasm_bindgen_test]
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

        #[wasm_bindgen_test]
        async fn buffered_request_preparation_reduces_the_remaining_injected_budget() {
            let start = MonotonicInstant::now();
            let budget = test_budget(start, Duration::from_millis(10));
            let observed = start
                .checked_add(Duration::from_millis(3))
                .expect("observed instant");
            let clock = scripted_clock(vec![observed, observed]);
            let body = request_body(Body::from("body"), 16, budget, native_time(clock.clone()))
                .await
                .expect("prepared body");
            let remaining = budget_remaining(budget, &clock).expect("remaining budget");

            assert!(body.is_some());
            assert_eq!(remaining, Duration::from_millis(7));
        }

        #[wasm_bindgen_test]
        async fn streamed_request_body_is_drained_before_fetch_construction() {
            use std::sync::atomic::{AtomicUsize, Ordering};

            let start = MonotonicInstant::now();
            let budget = test_budget(start, Duration::from_millis(10));
            let polls = Arc::new(AtomicUsize::new(0));
            let observed_polls = Arc::clone(&polls);
            let source = stream::poll_fn(move |_context| {
                let poll = observed_polls.fetch_add(1, Ordering::SeqCst);
                if poll == 0 {
                    Poll::Ready(Some(Bytes::from_static(b"body")))
                } else {
                    Poll::Ready(None)
                }
            })
            .boxed_local();

            let body = request_body(
                Body::stream(source),
                16,
                budget,
                native_time(constant_clock(start)),
            )
            .await
            .expect("prepared body");

            assert!(body.is_some());
            assert_eq!(polls.load(Ordering::SeqCst), 2);
        }

        #[wasm_bindgen_test]
        async fn streamed_upload_checks_injected_clock_after_ready_item() {
            let start = MonotonicInstant::now();
            let budget = test_budget(start, Duration::from_millis(10));
            let observed = Arc::new(Mutex::new(start));
            let clock = controlled_clock(Arc::clone(&observed));
            let source = stream::once(async move {
                *observed.lock().expect("clock") = budget.deadline.instant();
                Ok(Bytes::from_static(b"body"))
            })
            .boxed_local();
            let mut body = upload_stream(source, 16, budget, native_time(clock));

            let error = body
                .next()
                .await
                .expect("terminal item")
                .expect_err("post-ready expiry");

            assert!(matches!(error, EdgeError::GatewayTimeout { .. }));
        }

        #[wasm_bindgen_test]
        async fn streamed_upload_cap_error_yields_before_final_precedence() {
            let start = MonotonicInstant::now();
            let budget = test_budget(start, Duration::from_millis(10));
            let pair = expire_at_host_yield(budget, Arc::new(Mutex::new(start)));
            let source = stream::once(async { Ok(Bytes::from_static(b"too large")) }).boxed_local();
            let mut body = upload_stream(source, 1, budget, pair);

            let error = body
                .next()
                .await
                .expect("terminal item")
                .expect_err("host-event deadline wins over cap error");

            assert!(matches!(error, EdgeError::GatewayTimeout { .. }));
        }

        #[wasm_bindgen_test]
        async fn streamed_upload_eof_yields_before_final_precedence() {
            let start = MonotonicInstant::now();
            let budget = test_budget(start, Duration::from_millis(10));
            let pair = expire_at_host_yield(budget, Arc::new(Mutex::new(start)));
            let mut body = upload_stream(stream::empty().boxed_local(), 1, budget, pair);

            let error = body
                .next()
                .await
                .expect("deadline after host-event yield")
                .expect_err("host-event deadline wins over EOF");

            assert!(matches!(error, EdgeError::GatewayTimeout { .. }));
        }

        #[wasm_bindgen_test]
        fn streamed_upload_fairness_quota_is_at_most_sixty_four() {
            const { assert!(READY_ITEM_YIELD_QUOTA <= 64) };
        }

        #[wasm_bindgen_test]
        async fn streamed_upload_source_error_yields_before_final_precedence() {
            let start = MonotonicInstant::now();
            let budget = test_budget(start, Duration::from_millis(10));
            let pair = expire_at_host_yield(budget, Arc::new(Mutex::new(start)));
            let source =
                stream::once(async { Err(EdgeError::bad_gateway("source failed")) }).boxed_local();
            let mut body = upload_stream(source, 1, budget, pair);

            let error = body
                .next()
                .await
                .expect("terminal item")
                .expect_err("host-event deadline wins over source error");

            assert!(matches!(error, EdgeError::GatewayTimeout { .. }));
        }

        #[wasm_bindgen_test]
        async fn native_response_terminal_paths_preserve_core_errors_and_abort_ownership() {
            let _timers = NumericTimeoutGuard::install();
            for (script, mode, decoded_cap, encoded_cap, reason, limit, aborted) in [
                (
                    "return new Response('ok');",
                    ResponseMode::Buffered { max_bytes: 8 },
                    None,
                    None,
                    None,
                    None,
                    false,
                ),
                (
                    "return new Response('ok');",
                    ResponseMode::Streamed,
                    None,
                    None,
                    None,
                    None,
                    true,
                ),
                (
                    "return new Response(new ReadableStream({pull(c) {c.error(new TypeError('provider-secret-token'));}}));",
                    ResponseMode::Buffered { max_bytes: 8 },
                    None,
                    None,
                    Some(BadGatewayReason::Transport),
                    None,
                    true,
                ),
                (
                    "return new Response(new Uint8Array([31, 139]), {headers: {'content-encoding': 'gzip'}});",
                    ResponseMode::Buffered { max_bytes: 8 },
                    None,
                    None,
                    Some(BadGatewayReason::Decode(BadGatewayDecodeReason::Gzip)),
                    None,
                    false,
                ),
                (
                    "return new Response('ok');",
                    ResponseMode::Buffered { max_bytes: 8 },
                    None,
                    Some(1),
                    None,
                    Some(ResponseLimitReason::EncodedBody),
                    true,
                ),
                (
                    "return new Response('ok');",
                    ResponseMode::Buffered { max_bytes: 8 },
                    Some(1),
                    None,
                    None,
                    Some(ResponseLimitReason::DecodedBody),
                    true,
                ),
                (
                    "return new Response('ok');",
                    ResponseMode::Buffered { max_bytes: 1 },
                    None,
                    None,
                    None,
                    Some(ResponseLimitReason::BufferedBody),
                    true,
                ),
            ] {
                assert_native_response_terminal_path(
                    script,
                    mode,
                    decoded_cap,
                    encoded_cap,
                    reason,
                    limit,
                    aborted,
                )
                .await;
            }
        }

        async fn assert_native_response_terminal_path(
            script: &str,
            mode: ResponseMode,
            decoded_cap: Option<u64>,
            encoded_cap: Option<u64>,
            reason: Option<BadGatewayReason>,
            limit: Option<ResponseLimitReason>,
            aborted: bool,
        ) {
            let start = MonotonicInstant::now();
            let raw = Function::new_no_args(script)
                .call0(&JsValue::UNDEFINED)
                .expect("native response")
                .dyn_into::<web_sys::Response>()
                .expect("response type");
            let controller = web_sys::AbortController::new().expect("controller");
            let signal = controller.signal();
            let aborts = Rc::new(Cell::new(0_usize));
            let observed = Rc::clone(&aborts);
            let listener = Closure::<dyn FnMut()>::new(move || {
                observed.set(observed.get().saturating_add(1));
            });
            signal
                .add_event_listener_with_callback("abort", listener.as_ref().unchecked_ref())
                .expect("abort listener");
            let result = finish_fixture_response(
                WorkerResponse::from(raw),
                AbortGuard::new(controller),
                Method::GET,
                OutboundProxyMarker::Inherit,
                ResponseDecoding::Decode,
                mode,
                test_budget(start, Duration::from_secs(1)),
                32 * 1024 * 1024,
                24,
                None,
                decoded_cap,
                encoded_cap,
                None,
                None,
                native_time(constant_clock(start)),
            )
            .await;
            if let Some(expected) = reason {
                let error = result.expect_err("shared finalizer rejects native body");
                assert!(
                    matches!(error, EdgeError::BadGateway {reason: actual, ..} if actual == expected),
                    "{error:?}"
                );
                if expected == BadGatewayReason::Transport {
                    assert_eq!(
                        error.provider_failure_detail(),
                        Some(&ProviderFailureDetail::Cloudflare {
                            phase: ProviderFailurePhase::ResponseBodyRead,
                            cause: CloudflareFailureCause::TypeError,
                        })
                    );
                } else {
                    assert!(error.provider_failure_detail().is_none());
                }
            } else if let Some(expected) = limit {
                let error = result.expect_err("shared finalizer bounds native body");
                assert!(
                    matches!(error, EdgeError::ResponseTooLarge { reason: actual, .. } if actual == expected),
                    "{error:?}"
                );
                assert!(error.provider_failure_detail().is_none());
            } else {
                let response = result.expect("response");
                if mode == ResponseMode::Streamed {
                    assert!(
                        !signal.aborted(),
                        "live body is not cancelled at head handoff"
                    );
                } else {
                    assert_eq!(response.body().as_bytes(), Some(b"ok".as_slice()));
                }
                drop(response);
            }
            assert_eq!(signal.aborted(), aborted);
            assert_eq!(aborts.get(), usize::from(aborted));
            signal
                .remove_event_listener_with_callback("abort", listener.as_ref().unchecked_ref())
                .expect("remove listener");
        }

        #[wasm_bindgen_test]
        async fn reset_content_inspects_only_undeclared_native_streams() {
            let _timers = NumericTimeoutGuard::install();
            for declared_body in [false, true] {
                for native_body_is_empty in [false, true] {
                    let start = MonotonicInstant::now();
                    let probe = Function::new_no_args("const probe = {pulls: 0}; probe.response = new Response(new ReadableStream({pull(c) {probe.pulls++; c.close();}}, {highWaterMark: 0})); return probe;")
                        .call0(&JsValue::UNDEFINED).expect("source probe");
                    let native = if native_body_is_empty {
                        empty_worker_response(StatusCode::RESET_CONTENT, None)
                    } else {
                        let raw = Reflect::get(&probe, &JsValue::from_str("response"))
                            .expect("response")
                            .dyn_into::<web_sys::Response>()
                            .expect("response type");
                        WorkerResponse::from(raw).with_status(205)
                    };
                    let headers = Headers::new();
                    if declared_body {
                        headers.set("content-length", "7").expect("declared body");
                    }
                    let controller = web_sys::AbortController::new().expect("controller");
                    let signal = controller.signal();
                    let response = finish_fixture_response(
                        native.with_headers(headers),
                        AbortGuard::new(controller),
                        Method::GET,
                        OutboundProxyMarker::Inherit,
                        ResponseDecoding::Decode,
                        ResponseMode::Buffered { max_bytes: 8 },
                        test_budget(start, Duration::from_secs(1)),
                        1024,
                        24,
                        None,
                        None,
                        None,
                        None,
                        None,
                        native_time(constant_clock(start)),
                    )
                    .await
                    .expect("reset response");
                    assert_eq!(response.status(), StatusCode::RESET_CONTENT);
                    assert_eq!(response.body().as_bytes(), Some(b"".as_slice()));
                    assert_eq!(signal.aborted(), declared_body);
                    assert_eq!(
                        Reflect::get(&probe, &JsValue::from_str("pulls"))
                            .expect("pulls")
                            .as_f64(),
                        Some(f64::from(!declared_body && !native_body_is_empty))
                    );
                }
            }
        }

        #[wasm_bindgen_test]
        async fn response_stream_uses_only_the_shared_core_deadline_timer() {
            let _timers = NumericTimeoutGuard::install();
            let start = MonotonicInstant::now();
            let budget = test_budget(start, Duration::from_secs(1));
            let timers = Arc::new(AtomicUsize::new(0));
            let registered = Arc::clone(&timers);
            let pair = OutboundTime::new(constant_clock(start), move |_, _| {
                registered.fetch_add(1, Ordering::SeqCst);
                Box::pin(pending())
            });
            let controller = web_sys::AbortController::new().expect("abort controller");
            let signal = controller.signal();
            let raw = Function::new_no_args("return new Response('live');")
                .call0(&JsValue::UNDEFINED)
                .expect("native response")
                .dyn_into::<web_sys::Response>()
                .expect("response type");
            let native = WorkerResponse::from(raw);
            let response = finish_fixture_response(
                native,
                AbortGuard::new(controller),
                Method::GET,
                OutboundProxyMarker::Inherit,
                ResponseDecoding::Decode,
                ResponseMode::Streamed,
                budget,
                1024,
                24,
                None,
                None,
                None,
                None,
                None,
                pair,
            )
            .await
            .expect("streamed response");
            assert_eq!(response.into_bytes_bounded(4).await.expect("body"), "live");
            assert_eq!(
                timers.load(Ordering::SeqCst),
                1,
                "only core owns the response deadline wait"
            );
            assert!(!signal.aborted(), "clean EOF disarms the SDK resource");
        }

        #[wasm_bindgen_test]
        async fn response_stream_retains_clock_for_post_ready_expiry() {
            let start = MonotonicInstant::now();
            let budget = test_budget(start, Duration::from_millis(10));
            let observed = Arc::new(Mutex::new(start));
            let clock = controlled_clock(Arc::clone(&observed));
            let source = stream::once(async move {
                *observed.lock().expect("clock") = budget.deadline.instant();
                Ok(Bytes::from_static(b"body"))
            })
            .boxed_local();
            let controller = web_sys::AbortController::new().expect("abort controller");
            let mut body = finish_fixture_stream(
                source,
                AbortGuard::new(controller),
                budget,
                native_time(clock),
            )
            .await
            .expect("shared response stream");

            let error = body
                .next()
                .await
                .expect("terminal item")
                .expect_err("post-ready expiry");

            assert!(matches!(error, EdgeError::GatewayTimeout { .. }));
        }

        #[wasm_bindgen_test]
        async fn converted_response_retains_injected_clock() {
            let start = MonotonicInstant::now();
            let budget = test_budget(start, Duration::from_secs(1));
            let observed = Arc::new(Mutex::new(start));
            let clock_observed = Arc::clone(&observed);
            let clock =
                MonotonicClock::new(move || *clock_observed.lock().expect("clock observation"));
            let controller = web_sys::AbortController::new().expect("abort controller");
            let response = finish_fixture_response(
                empty_worker_response(StatusCode::NO_CONTENT, None),
                AbortGuard::new(controller),
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

        #[wasm_bindgen_test]
        async fn converted_response_content_type_provenance_is_always_unknown() {
            let _timer_guard = NumericTimeoutGuard::install();
            let cases: &[&[&str]] = &[
                &[],
                &["text/plain"],
                &["text/plain", "text/plain"],
                &["text/plain", "application/json"],
                &["text/plain, application/json"],
            ];
            for values in cases {
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
                        let headers = Headers::new();
                        for value in *values {
                            headers
                                .append(CONTENT_TYPE.as_str(), value)
                                .expect("content-type field");
                        }
                        let native = WorkerResponse::from_stream(stream::empty::<
                            Result<Vec<u8>, worker::Error>,
                        >())
                        .expect("streaming response")
                        .with_status(status.as_u16())
                        .with_headers(headers);
                        let bridged = response_headers(&native).expect("bridged headers");
                        assert_eq!(
                            bridged.get_all(CONTENT_TYPE).iter().count(),
                            usize::from(!values.is_empty())
                        );
                        if values.len() > 1 {
                            assert_eq!(
                                bridged.get(CONTENT_TYPE).map(HeaderValue::as_bytes),
                                Some(values.join(", ").as_bytes())
                            );
                        }
                        let controller = web_sys::AbortController::new().expect("abort controller");
                        let mut response = finish_fixture_response(
                            native,
                            AbortGuard::new(controller),
                            Method::GET,
                            OutboundProxyMarker::Inherit,
                            ResponseDecoding::Decode,
                            mode,
                            test_budget(start, Duration::from_secs(1)),
                            1024,
                            24,
                            None,
                            None,
                            None,
                            None,
                            None,
                            native_time(constant_clock(start)),
                        )
                        .await
                        .expect("converted response");

                        assert_eq!(
                            response.content_type_provenance(),
                            ContentTypeProvenance::Unknown
                        );
                        response
                            .headers_mut()
                            .insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
                        assert_eq!(
                            response.content_type_provenance(),
                            ContentTypeProvenance::Unknown
                        );
                    }
                }
            }
        }

        #[wasm_bindgen_test]
        async fn null_reset_content_disarms_only_without_a_declared_body() {
            let start = MonotonicInstant::now();
            let budget = test_budget(start, Duration::from_secs(1));
            for (content_length, expected_aborted) in
                [(None, false), (Some("0"), false), (Some("7"), true)]
            {
                for observed in [
                    start,
                    budget.deadline.instant(),
                    budget
                        .deadline
                        .instant()
                        .checked_add(Duration::from_nanos(1))
                        .expect("after deadline"),
                ] {
                    let controller = web_sys::AbortController::new().expect("abort controller");
                    let signal = controller.signal();
                    let result = finish_fixture_response(
                        empty_worker_response(StatusCode::RESET_CONTENT, content_length),
                        AbortGuard::new(controller),
                        Method::GET,
                        OutboundProxyMarker::Inherit,
                        ResponseDecoding::Decode,
                        ResponseMode::Buffered { max_bytes: 64 },
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

                    if observed < budget.deadline.instant() {
                        let response = result.expect("empty reset response");
                        assert_eq!(response.status(), StatusCode::RESET_CONTENT);
                        assert_eq!(response.body().as_bytes(), Some(&[][..]));
                    } else {
                        assert!(
                            matches!(
                                result,
                                Err(EdgeError::GatewayTimeout {
                                    cause: BudgetSource::PerCallTimeout,
                                    ..
                                })
                            ),
                            "empty reset handoff must retain its deadline"
                        );
                    }
                    let expired_before_head = observed >= budget.deadline.instant();
                    assert_eq!(
                        signal.aborted(),
                        expected_aborted || expired_before_head,
                        "preparation expiry releases an unhanded native response; length={content_length:?}, observed={observed:?}"
                    );
                }
            }
        }

        #[wasm_bindgen_test]
        async fn null_reset_content_keeps_clean_disarm_when_final_handoff_expires() {
            struct ExpireOnDrop {
                observed: Arc<Mutex<MonotonicInstant>>,
                deadline: MonotonicInstant,
            }
            impl Future for ExpireOnDrop {
                type Output = Result<(), EdgeError>;
                fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
                    Poll::Pending
                }
            }
            #[expect(
                clippy::missing_trait_methods,
                reason = "Drop::pin_drop is unstable and conflicts with drop"
            )]
            impl Drop for ExpireOnDrop {
                fn drop(&mut self) {
                    *self.observed.lock().expect("clock") = self.deadline;
                }
            }
            let _timers = NumericTimeoutGuard::install();
            for content_length in [None, Some("0")] {
                let start = MonotonicInstant::now();
                let budget = test_budget(start, Duration::from_secs(1));
                let observed = Arc::new(Mutex::new(start));
                let expiry = Arc::clone(&observed);
                let pair = OutboundTime::new(controlled_clock(observed), move |_, _| {
                    Box::pin(ExpireOnDrop {
                        observed: Arc::clone(&expiry),
                        deadline: budget.deadline.instant(),
                    })
                });
                let controller = web_sys::AbortController::new().expect("controller");
                let signal = controller.signal();
                let result = finish_fixture_response(
                    empty_worker_response(StatusCode::RESET_CONTENT, content_length),
                    AbortGuard::new(controller),
                    Method::GET,
                    OutboundProxyMarker::Inherit,
                    ResponseDecoding::Decode,
                    ResponseMode::Buffered { max_bytes: 64 },
                    budget,
                    1024,
                    24,
                    None,
                    None,
                    None,
                    None,
                    None,
                    pair,
                )
                .await;
                assert!(matches!(
                    result,
                    Err(EdgeError::GatewayTimeout {
                        cause: BudgetSource::PerCallTimeout,
                        ..
                    })
                ));
                assert!(
                    !signal.aborted(),
                    "clean transport stays disarmed through a later handoff timeout"
                );
            }
        }

        #[wasm_bindgen_test]
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
                            let observed =
                                start.checked_add(offset_duration).expect("handoff time");
                            let controller = web_sys::AbortController::new().expect("controller");
                            let signal = controller.signal();
                            let probe = Function::new_no_args("const probe = {pulls: 0}; probe.response = new Response(new ReadableStream({pull() {probe.pulls++;}}, {highWaterMark: 0}), {headers: {'content-length': '1'}}); return probe;")
                            .call0(&JsValue::UNDEFINED).expect("pending native source");
                            let native = if status == StatusCode::OK {
                                let native = Reflect::get(&probe, &JsValue::from_str("response"))
                                    .expect("native response")
                                    .dyn_into::<web_sys::Response>()
                                    .expect("response type");
                                WorkerResponse::from(native)
                            } else {
                                empty_worker_response(status, Some("1"))
                            };
                            let result = finish_fixture_response(
                                native,
                                AbortGuard::new(controller),
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
                            assert!(
                                signal.aborted(),
                                "terminal handoff or response drop aborts the owned exchange"
                            );
                            assert_eq!(
                                Reflect::get(&probe, &JsValue::from_str("pulls"))
                                    .expect("source pulls")
                                    .as_f64(),
                                Some(0.0_f64),
                                "handoff never reads payload"
                            );
                        }
                    }
                }
            }
        }

        #[wasm_bindgen_test]
        async fn framing_bodyless_response_aborts_its_unread_native_body() {
            let start = MonotonicInstant::now();
            let budget = test_budget(start, Duration::from_secs(1));
            let controller = web_sys::AbortController::new().expect("abort controller");
            let signal = controller.signal();

            let response = finish_fixture_response(
                empty_worker_response(StatusCode::NO_CONTENT, None),
                AbortGuard::new(controller),
                Method::GET,
                OutboundProxyMarker::Inherit,
                ResponseDecoding::Decode,
                ResponseMode::Buffered { max_bytes: 64 },
                budget,
                1024,
                24,
                None,
                None,
                None,
                None,
                None,
                native_time(constant_clock(start)),
            )
            .await
            .expect("bodyless response");

            assert_eq!(response.status(), StatusCode::NO_CONTENT);
            assert!(signal.aborted());
        }
    }
}

#[cfg(all(feature = "cloudflare", target_arch = "wasm32"))]
pub use worker_impl::CloudflareOutboundClient;
#[cfg(all(feature = "cloudflare", target_arch = "wasm32", feature = "test-utils"))]
pub use worker_impl::deferred_clock_paths_hold_for_test;
#[cfg(all(feature = "cloudflare", target_arch = "wasm32"))]
pub(crate) use worker_impl::native_time;
#[cfg(all(feature = "cloudflare", target_arch = "wasm32", feature = "test-utils"))]
pub use worker_impl::response_abort_lifecycle_holds_for_test;

#[cfg(any(
    all(feature = "cloudflare", target_arch = "wasm32"),
    feature = "test-utils"
))]
fn generic_fetch_failure() -> EdgeError {
    // Workers exposes a rejected Fetch promise as an opaque JS exception, without a stable phase
    // or connection-failure discriminator. Preserve that limitation as `Unspecified` instead of
    // guessing that every rejection happened before a response was reachable.
    EdgeError::bad_gateway_with_reason("outbound fetch failed", BadGatewayReason::Unspecified)
}

#[cfg(any(
    all(feature = "cloudflare", target_arch = "wasm32"),
    feature = "test-utils"
))]
fn timeout_error(cause: BudgetSource) -> EdgeError {
    EdgeError::gateway_timeout_caused("outbound request deadline expired", cause)
}

#[cfg(any(
    all(feature = "cloudflare", target_arch = "wasm32"),
    feature = "test-utils"
))]
fn validate_supported_options(request: &OutboundRequest) -> Result<(), EdgeError> {
    if request.image_optimizer_options().is_some() {
        return Err(EdgeError::bad_request(
            "image_optimizer is not supported by cloudflare",
        ));
    }
    Ok(())
}

/// Runs the target-neutral Cloudflare batch preflight contract in native tests.
///
/// # Errors
/// Returns the same portable validation or batch-shape error as production dispatch.
#[cfg(feature = "test-utils")]
#[inline]
pub fn validate_batch_request_for_test(request: &OutboundRequest) -> Result<(), EdgeError> {
    validate_supported_options(request)?;
    validate_for_dispatch(request, OutboundSendMode::Batch)
}

/// Returns the target-neutral classification for an opaque Workers fetch rejection.
#[cfg(feature = "test-utils")]
#[must_use]
#[inline]
pub fn generic_fetch_failure_for_test() -> EdgeError {
    generic_fetch_failure()
}

/// Returns the same attributed timeout emitted by Workers budget timers.
#[cfg(feature = "test-utils")]
#[must_use]
#[inline]
pub fn timeout_error_for_test(cause: BudgetSource) -> EdgeError {
    timeout_error(cause)
}

#[cfg(test)]
mod header_bridge_tests {
    use std::collections::BTreeMap;

    use edgezero_core::http::{HeaderMap, HeaderValue};

    use super::*;

    #[derive(Default)]
    struct AppendSink(BTreeMap<String, Vec<String>>);

    impl HeaderSink for AppendSink {
        fn append(&mut self, name: &str, value: &str) -> Result<(), EdgeError> {
            self.0
                .entry(name.to_owned())
                .or_default()
                .push(value.to_owned());
            Ok(())
        }
    }

    #[test]
    fn response_header_copy_preserves_duplicate_set_cookie_values() {
        let mut source = HeaderMap::new();
        source.append("set-cookie", HeaderValue::from_static("first=1"));
        source.append("set-cookie", HeaderValue::from_static("second=2"));
        let mut sink = AppendSink::default();

        copy_header_values(&source, &mut sink, |_name| {
            EdgeError::internal(anyhow::anyhow!("invalid response header"))
        })
        .expect("copy response headers");

        assert_eq!(
            sink.0.get("set-cookie"),
            Some(&vec!["first=1".to_owned(), "second=2".to_owned()])
        );
    }

    #[test]
    fn response_header_copy_accepts_non_ascii_utf8() {
        let mut source = HeaderMap::new();
        source.append(
            "x-label",
            HeaderValue::from_bytes("caf\u{e9}".as_bytes()).expect("valid header bytes"),
        );
        let mut sink = AppendSink::default();

        copy_header_values(&source, &mut sink, |_name| {
            EdgeError::internal(anyhow::anyhow!("invalid response header"))
        })
        .expect("copy UTF-8 response header");

        assert_eq!(sink.0.get("x-label"), Some(&vec!["caf\u{e9}".to_owned()]));
    }
}
