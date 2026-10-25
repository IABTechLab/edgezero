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
use edgezero_core::outbound::{OutboundRequest, OutboundSendMode, validate_for_dispatch};

#[cfg(any(all(feature = "cloudflare", target_arch = "wasm32"), test))]
mod header_bridge {
    use std::str;

    use edgezero_core::error::EdgeError;
    use edgezero_core::http::{HeaderMap, HeaderName};

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    pub(super) enum ResetContentAction {
        Abort,
        Disarm,
        InspectStream,
    }

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

    pub(super) fn reset_content_action(
        declared_body: bool,
        native_body_is_empty: bool,
    ) -> ResetContentAction {
        if declared_body {
            ResetContentAction::Abort
        } else if native_body_is_empty {
            ResetContentAction::Disarm
        } else {
            ResetContentAction::InspectStream
        }
    }
}

#[cfg(any(all(feature = "cloudflare", target_arch = "wasm32"), test))]
pub(crate) use header_bridge::copy_header_values;
#[cfg(any(all(feature = "cloudflare", target_arch = "wasm32"), test))]
use header_bridge::{HeaderSink, ResetContentAction, reset_content_action};

#[cfg(all(feature = "cloudflare", target_arch = "wasm32"))]
impl HeaderSink for worker::Headers {
    fn append(&mut self, name: &str, value: &str) -> Result<(), EdgeError> {
        worker::Headers::append(self, name, value).map_err(EdgeError::internal)
    }
}

#[cfg(all(feature = "cloudflare", target_arch = "wasm32"))]
mod worker_impl {
    use std::cell::Cell;
    use std::future::Future;
    use std::num::NonZeroU64;
    use std::pin::Pin;
    use std::rc::Rc;
    use std::task::{Context, Poll};
    use std::time::Duration;

    use async_stream::stream;
    use async_trait::async_trait;
    use bytes::Bytes;
    use edgezero_core::body::{Body, BodyStream};
    #[cfg(feature = "test-utils")]
    use edgezero_core::error::BudgetSource;
    use edgezero_core::error::{BadGatewayReason, EdgeError};
    use edgezero_core::error_detail::{
        CloudflareFailureCause, ProviderFailureDetail, ProviderFailurePhase,
    };
    use edgezero_core::http::header::{ACCEPT_ENCODING, HOST};
    use edgezero_core::http::{HeaderMap, HeaderName, HeaderValue, Method, StatusCode};
    use edgezero_core::outbound::{
        OutboundBatch, OutboundCachePolicy, OutboundHeaderFidelity, OutboundHeaderInput,
        OutboundHttpClient, OutboundProxyMarker, OutboundRequest, OutboundRequestParts,
        OutboundResponse, ResponseBodyDisposition, ResponseBodyPolicy, ResponseDecoding,
        ResponseHeaderLimiter, ResponseMode, apply_response_body_policy, collect_response_stream,
        normalize_for_dispatch,
    };
    use edgezero_core::time::{
        Deadline, DispatchBudget, MonotonicClock, MonotonicInstant, dispatch_budget,
    };
    use futures_util::StreamExt as _;
    use futures_util::future::{Either, FutureExt as _, LocalBoxFuture, select};
    use worker::js_sys::{Function, Reflect, Uint8Array, global};
    use worker::wasm_bindgen::closure::Closure;
    use worker::wasm_bindgen::{JsCast as _, JsValue};
    use worker::wasm_bindgen_futures::JsFuture;
    use worker::web_sys;
    use worker::{
        CacheMode, Delay, Headers, Method as WorkerMethod, Request as WorkerRequest, RequestInit,
        RequestRedirect, Response as WorkerResponse, ResponseBody as WorkerResponseBody,
    };

    use super::{ResetContentAction, reset_content_action, timeout_error};

    const READY_ITEM_YIELD_QUOTA: u32 = 64;

    struct HostEventYield {
        awoken: Rc<Cell<bool>>,
        callback: Option<Closure<dyn FnMut()>>,
        timeout_handle: Option<JsValue>,
    }

    impl HostEventYield {
        fn new() -> Self {
            Self {
                awoken: Rc::new(Cell::new(false)),
                callback: None,
                timeout_handle: None,
            }
        }
    }

    impl Future for HostEventYield {
        type Output = Result<(), EdgeError>;

        fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            let this = self.get_mut();
            if this.awoken.get() {
                return Poll::Ready(Ok(()));
            }
            if this.callback.is_some() {
                return Poll::Pending;
            }

            let awoken = Rc::clone(&this.awoken);
            let waker = cx.waker().clone();
            let callback = Closure::<dyn FnMut()>::new(move || {
                awoken.set(true);
                waker.wake_by_ref();
            });
            let global_scope = global();
            let Some(set_timeout) = Reflect::get(&global_scope, &JsValue::from_str("setTimeout"))
                .ok()
                .and_then(|value| value.dyn_into::<Function>().ok())
            else {
                return Poll::Ready(Err(host_event_schedule_error()));
            };
            let timeout_handle = match set_timeout.call2(
                &global_scope,
                callback.as_ref(),
                &JsValue::from_f64(0.0),
            ) {
                Ok(handle) => handle,
                Err(_error) => return Poll::Ready(Err(host_event_schedule_error())),
            };
            this.callback = Some(callback);
            this.timeout_handle = Some(timeout_handle);
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
            let global_scope = global();
            let Ok(clear_timeout) = Reflect::get(&global_scope, &JsValue::from_str("clearTimeout"))
            else {
                return;
            };
            let Some(clear_timeout_function) = clear_timeout.dyn_ref::<Function>() else {
                return;
            };
            let _ignored = clear_timeout_function.call1(&global_scope, &timeout_handle);
        }
    }

    fn host_event_schedule_error() -> EdgeError {
        EdgeError::internal(anyhow::anyhow!("Cloudflare host-event scheduling failed"))
    }

    /// Native outbound HTTP implementation for Cloudflare Workers.
    #[derive(Clone)]
    pub struct CloudflareOutboundClient {
        clock: MonotonicClock,
    }

    struct AbortGuard {
        controller: Option<web_sys::AbortController>,
    }

    struct PreparedRequest {
        budget: DispatchBudget,
        parts: OutboundRequestParts,
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
        async fn execute(&self, prepared: PreparedRequest) -> Result<OutboundResponse, EdgeError> {
            let PreparedRequest { budget, parts } = prepared;
            let OutboundRequestParts {
                body,
                cache_policy,
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
                let value =
                    HeaderValue::from_str(authority.as_str()).map_err(EdgeError::internal)?;
                headers.insert(HOST, value);
            }
            let worker_body =
                request_body(body, max_request_body_bytes, budget, self.clock.clone()).await?;
            let url = uri.to_string();
            let request = build_worker_request(&method, &url, &headers, worker_body, cache_policy)?;
            let (response, abort_guard) = raw_fetch(request, budget, &self.clock).await?;

            process_response(
                response,
                abort_guard,
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
            super::validate_request(&request, super::OutboundSendMode::Send)?;
            Self::prepare_validated(request, started_at, None)
        }

        fn prepare_batch(
            request: OutboundRequest,
            started_at: MonotonicInstant,
            observation_cutoff: Deadline,
        ) -> Result<PreparedRequest, EdgeError> {
            super::validate_batch_request(&request)?;
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

        /// Builds a client using the process-default monotonic clock.
        #[must_use]
        #[inline]
        pub fn new() -> Self {
            Self::with_clock(MonotonicClock::default())
        }

        /// Builds a client that evaluates every outbound lifetime against `clock`.
        #[must_use]
        #[inline]
        pub fn with_clock(clock: MonotonicClock) -> Self {
            Self { clock }
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

    #[async_trait(?Send)]
    impl OutboundHttpClient for CloudflareOutboundClient {
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

    fn deadline_stream(
        mut source: BodyStream,
        budget: DispatchBudget,
        clock: MonotonicClock,
        mut abort_guard: AbortGuard,
    ) -> BodyStream {
        stream! {
            let mut ready_items = 0_u32;
            loop {
                let remaining = match budget_remaining(budget, &clock) {
                    Ok(remaining) => remaining,
                    Err(error) => {
                        yield Err(error);
                        return;
                    }
                };
                let next = source.next();
                let timer = Delay::from(remaining);
                futures_util::pin_mut!(next, timer);
                let next_item = match select(next, timer).await {
                    Either::Left((item, _timer)) => item,
                    Either::Right(((), _next)) => {
                        yield Err(timeout_error(budget.cause));
                        return;
                    }
                };
                if budget_remaining(budget, &clock).is_err() {
                    let detail = stream_error_detail(next_item.as_ref());
                    yield Err(preserve_timeout_detail(timeout_error(budget.cause), detail));
                    return;
                }
                match next_item {
                    Some(Ok(bytes)) => yield Ok(bytes),
                    Some(Err(error)) => {
                        yield Err(terminal_error_after_host_event(error, budget, &clock).await);
                        return;
                    }
                    None => {
                        if let Err(error) = yield_host_event(budget, &clock).await {
                            yield Err(error);
                            return;
                        }
                        abort_guard.disarm();
                        return;
                    }
                }
                ready_items = ready_items.saturating_add(1);
                if ready_items >= READY_ITEM_YIELD_QUOTA {
                    let checkpoint = yield_host_event(budget, &clock).await;
                    ready_items = 0;
                    if let Err(error) = checkpoint {
                        yield Err(error);
                        return;
                    }
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
        mut response: WorkerResponse,
        mut abort_guard: AbortGuard,
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
        let status = StatusCode::from_u16(response.status_code()).map_err(EdgeError::internal)?;
        let (mut headers, normalization, _) = OutboundHeaderInput::new(
            "cloudflare",
            response_headers(&response)?,
            OutboundHeaderFidelity::ProviderFieldBoundariesUnknown,
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
        } else if let ResponseBodyDisposition::ResetContent { declared_body } = disposition {
            match reset_content_action(
                declared_body,
                matches!(response.body(), WorkerResponseBody::Empty),
            ) {
                ResetContentAction::Abort => {}
                ResetContentAction::Disarm => abort_guard.disarm(),
                ResetContentAction::InspectStream => {
                    let native = response_stream(&mut response)?;
                    let remaining = budget_remaining(budget, &clock)?;
                    let next = native.into_future();
                    let timer = Delay::from(remaining);
                    futures_util::pin_mut!(next, timer);
                    let ready = match select(next, timer).await {
                        Either::Left((ready, _timer)) => ready,
                        Either::Right(((), _next)) => return Err(timeout_error(budget.cause)),
                    };
                    let detail = stream_error_detail(ready.0.as_ref());
                    budget_remaining(budget, &clock)
                        .map_err(|selected| preserve_timeout_detail(selected, detail))?;
                    yield_host_event(budget, &clock)
                        .await
                        .map_err(|selected| preserve_timeout_detail(selected, detail))?;
                    match ready {
                        (Some(item), _rest) => {
                            item?;
                        }
                        (None, _rest) => abort_guard.disarm(),
                    }
                }
            }
            Body::empty()
        } else {
            let native = response_stream(&mut response)?;
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
            let deadline_bound = deadline_stream(shaped, budget, clock, abort_guard);
            match response_mode {
                ResponseMode::Buffered { max_bytes } => {
                    Body::from(collect_response_stream(deadline_bound, max_bytes).await?)
                }
                ResponseMode::Streamed => Body::from_stream(deadline_bound),
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

    async fn raw_fetch(
        request: WorkerRequest,
        budget: DispatchBudget,
        clock: &MonotonicClock,
    ) -> Result<(WorkerResponse, AbortGuard), EdgeError> {
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
        let (promise, remaining) = begin_fetch(budget, clock, || {
            global.fetch_with_request_and_init(request.inner(), &fetch_init)
        })?;
        let fetch = JsFuture::from(promise);
        let timer = Delay::from(remaining);
        futures_util::pin_mut!(fetch, timer);
        let result = match select(fetch, timer).await {
            Either::Left((result, _timer)) => result,
            Either::Right(((), _fetch)) => return Err(timeout_error(budget.cause)),
        };
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
        clock: MonotonicClock,
    ) -> Result<Option<JsValue>, EdgeError> {
        match body {
            Body::Once(bytes) => {
                let length = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
                if length > maximum {
                    return Err(EdgeError::payload_too_large(
                        edgezero_core::RequestBodyOverflowOrigin::OutboundUpload,
                    ));
                }
                budget_remaining(budget, &clock)?;
                if bytes.is_empty() {
                    Ok(None)
                } else {
                    Ok(Some(Uint8Array::from(bytes.as_ref()).into()))
                }
            }
            Body::Stream(source) => {
                let mut bounded = upload_stream(source, maximum, budget, clock);
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
            process_response(
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
                clock,
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
        clock: &MonotonicClock,
    ) -> Result<(), EdgeError> {
        HostEventYield::new().await?;
        budget_remaining(budget, clock).map(drop)
    }

    async fn terminal_error_after_host_event(
        error: EdgeError,
        budget: DispatchBudget,
        clock: &MonotonicClock,
    ) -> EdgeError {
        let detail = error.provider_failure_detail().copied();
        match yield_host_event(budget, clock).await {
            Ok(()) => error,
            Err(selected) => preserve_timeout_detail(selected, detail),
        }
    }

    fn upload_stream(
        mut source: BodyStream,
        maximum: u64,
        budget: DispatchBudget,
        clock: MonotonicClock,
    ) -> BodyStream {
        stream! {
            let mut ready_items = 0_u32;
            let mut total = 0_u64;
            loop {
                let remaining = match budget_remaining(budget, &clock) {
                    Ok(remaining) => remaining,
                    Err(error) => {
                        yield Err(error);
                        return;
                    }
                };
                let next = source.next();
                let timer = Delay::from(remaining);
                futures_util::pin_mut!(next, timer);
                let next_item = match select(next, timer).await {
                    Either::Left((item, _timer)) => item,
                    Either::Right(((), _next)) => {
                        yield Err(timeout_error(budget.cause));
                        return;
                    }
                };
                if budget_remaining(budget, &clock).is_err() {
                    let detail = stream_error_detail(next_item.as_ref());
                    yield Err(preserve_timeout_detail(timeout_error(budget.cause), detail));
                    return;
                }
                let Some(item) = next_item else {
                    if let Err(error) = yield_host_event(budget, &clock).await {
                        yield Err(error);
                    }
                    return;
                };
                let bytes = match item {
                    Ok(bytes) => bytes,
                    Err(error) => {
                        yield Err(terminal_error_after_host_event(error, budget, &clock).await);
                        return;
                    }
                };
                let length = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
                let Some(next_total) = total.checked_add(length) else {
                    let error = EdgeError::payload_too_large(
                        edgezero_core::RequestBodyOverflowOrigin::OutboundUpload,
                    );
                    yield Err(terminal_error_after_host_event(error, budget, &clock).await);
                    return;
                };
                if next_total > maximum {
                    let error = EdgeError::payload_too_large(
                        edgezero_core::RequestBodyOverflowOrigin::OutboundUpload,
                    );
                    yield Err(terminal_error_after_host_event(error, budget, &clock).await);
                    return;
                }
                total = next_total;
                yield Ok(bytes);
                ready_items = ready_items.saturating_add(1);
                if ready_items >= READY_ITEM_YIELD_QUOTA {
                    let checkpoint = yield_host_event(budget, &clock).await;
                    ready_items = 0;
                    if let Err(error) = checkpoint {
                        yield Err(error);
                        return;
                    }
                }
            }
        }
        .boxed_local()
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
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};

        use futures_util::stream::once;

        fn expiring_clock(start: MonotonicInstant, deadline: MonotonicInstant) -> MonotonicClock {
            let observations = Arc::new(AtomicUsize::new(0));
            MonotonicClock::new(move || {
                if observations.fetch_add(1, Ordering::SeqCst) == 0 {
                    start
                } else {
                    deadline
                }
            })
        }

        let start = MonotonicInstant::now();
        let Ok(request) = OutboundRequest::get("https://example.com/") else {
            return false;
        };
        let Ok(budget) = dispatch_budget(&request.timeout(Duration::from_millis(10)), start, None)
        else {
            return false;
        };
        let upload_source = once(async { Ok(Bytes::from_static(b"body")) }).boxed_local();
        let mut upload = upload_stream(
            upload_source,
            16,
            budget,
            expiring_clock(start, budget.deadline.instant()),
        );
        let upload_holds = matches!(
            upload.next().await,
            Some(Err(EdgeError::GatewayTimeout { .. }))
        );

        let Ok(controller) = web_sys::AbortController::new() else {
            return false;
        };
        let response_source = once(async { Ok(Bytes::from_static(b"body")) }).boxed_local();
        let mut response = deadline_stream(
            response_source,
            budget,
            expiring_clock(start, budget.deadline.instant()),
            AbortGuard::new(controller),
        );
        let response_holds = matches!(
            response.next().await,
            Some(Err(EdgeError::GatewayTimeout { .. }))
        );

        upload_holds && response_holds
    }

    #[cfg(test)]
    mod clock_tests {
        use std::collections::VecDeque;
        use std::ptr;
        use std::sync::{Arc, Mutex};
        use std::task::Poll;

        use crate::test_runtime::NumericTimeoutGuard;
        use edgezero_core::error::BudgetSource;
        use edgezero_core::http::header::CONTENT_TYPE;
        use edgezero_core::outbound::ContentTypeProvenance;
        use edgezero_core::response::IntoResponse as _;
        use edgezero_core::time::Deadline;
        use edgezero_core::{CloudflareFailureCause, ProviderFailureDetail, ProviderFailurePhase};
        use futures_util::stream;
        use wasm_bindgen_test::wasm_bindgen_test;

        use super::*;

        struct RestoreGlobal(Function);

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
                    let _restore = RestoreGlobal(restore);
                    raw_fetch(request, budget, &MonotonicClock::new(move || start)).await
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
                let mut observations = vec![start, start];
                if during_yield {
                    observations.push(start);
                }
                observations.push(budget.deadline.instant());
                let native = WorkerResponse::from_stream(stream::iter([Err::<Vec<u8>, _>(
                    worker::Error::RustError("provider-secret-token".to_owned()),
                )]))
                .expect("stream response")
                .with_status(205);
                let result = process_response(
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
                    scripted_clock(observations),
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
                        cause: CloudflareFailureCause::Unknown,
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
                let source = stream::iter([Err(EdgeError::bad_gateway_with_detail(
                    "source failed",
                    BadGatewayReason::Transport,
                    detail,
                ))])
                .boxed_local();
                let clock = scripted_clock(vec![start, budget.deadline.instant()]);
                let mut wrapped = if upload {
                    upload_stream(source, 32, budget, clock)
                } else {
                    deadline_stream(
                        source,
                        budget,
                        clock,
                        AbortGuard::new(web_sys::AbortController::new().expect("controller")),
                    )
                };
                let error = wrapped.next().await.expect("terminal").expect_err("expiry");
                assert!(matches!(error, EdgeError::GatewayTimeout { .. }));
                assert_eq!(error.provider_failure_detail(), Some(&detail));
            }
            let start = MonotonicInstant::now();
            let budget = test_budget(start, Duration::from_secs(1));
            let clock = constant_clock(budget.deadline.instant());
            let error = terminal_error_after_host_event(
                EdgeError::bad_gateway_with_detail(
                    "source failed",
                    BadGatewayReason::Transport,
                    detail,
                ),
                budget,
                &clock,
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
                let mut bounded = upload_stream(source, 8, budget, clock);
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
                MonotonicClock::new(move || start),
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
            let client =
                CloudflareOutboundClient::with_clock(scripted_clock(vec![start, completed]));
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

            let slot = results.slots[0].as_ref().expect("resolved slot");
            assert_eq!(slot.elapsed, Duration::from_millis(9));
            assert!(matches!(slot.outcome, Err(EdgeError::BadRequest { .. })));
        }

        #[wasm_bindgen_test]
        async fn backwards_clock_fails_slot_without_invalid_elapsed() {
            let start = MonotonicInstant::now();
            let earlier = start
                .checked_sub(Duration::from_millis(1))
                .expect("earlier instant");
            let client = CloudflareOutboundClient::with_clock(scripted_clock(vec![start, earlier]));
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

            let slot = results.slots[0].as_ref().expect("resolved slot");
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
            let body = request_body(Body::from("body"), 16, budget, clock.clone())
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

            let body = request_body(Body::stream(source), 16, budget, constant_clock(start))
                .await
                .expect("prepared body");

            assert!(body.is_some());
            assert_eq!(polls.load(Ordering::SeqCst), 2);
        }

        #[wasm_bindgen_test]
        async fn streamed_upload_checks_injected_clock_after_ready_item() {
            let start = MonotonicInstant::now();
            let budget = test_budget(start, Duration::from_millis(10));
            let clock = scripted_clock(vec![start, budget.deadline.instant()]);
            let source = stream::once(async { Ok(Bytes::from_static(b"body")) }).boxed_local();
            let mut body = upload_stream(source, 16, budget, clock);

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
            let clock = scripted_clock(vec![start, start, budget.deadline.instant()]);
            let source = stream::once(async { Ok(Bytes::from_static(b"too large")) }).boxed_local();
            let mut body = upload_stream(source, 1, budget, clock);

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
            let clock = scripted_clock(vec![start, start, budget.deadline.instant()]);
            let mut body = upload_stream(stream::empty().boxed_local(), 1, budget, clock);

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
            let clock = scripted_clock(vec![start, start, budget.deadline.instant()]);
            let source =
                stream::once(async { Err(EdgeError::bad_gateway("source failed")) }).boxed_local();
            let mut body = upload_stream(source, 1, budget, clock);

            let error = body
                .next()
                .await
                .expect("terminal item")
                .expect_err("host-event deadline wins over source error");

            assert!(matches!(error, EdgeError::GatewayTimeout { .. }));
        }

        #[wasm_bindgen_test]
        async fn response_stream_retains_clock_for_post_ready_expiry() {
            let start = MonotonicInstant::now();
            let budget = test_budget(start, Duration::from_millis(10));
            let clock = scripted_clock(vec![start, budget.deadline.instant()]);
            let source = stream::once(async { Ok(Bytes::from_static(b"body")) }).boxed_local();
            let controller = web_sys::AbortController::new().expect("abort controller");
            let mut body = deadline_stream(source, budget, clock, AbortGuard::new(controller));

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
            let response = process_response(
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
                        let mut response = process_response(
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
                            constant_clock(start),
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
                    let result = process_response(
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
                        scripted_clock(vec![start, observed]),
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
                    assert_eq!(signal.aborted(), expected_aborted);
                }
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
                            let mut budget = test_budget(start, Duration::from_millis(10));
                            budget.cause = cause;
                            let observed = start
                                .checked_add(Duration::from_millis(offset))
                                .expect("handoff time");
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
                            let result = process_response(
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
                                scripted_clock(vec![start, observed]),
                            )
                            .await;
                            if offset < 10 {
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

            let response = process_response(
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
                constant_clock(start),
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
fn validate_batch_request(request: &OutboundRequest) -> Result<(), EdgeError> {
    validate_request(request, OutboundSendMode::Batch)
}

#[cfg(any(
    all(feature = "cloudflare", target_arch = "wasm32"),
    feature = "test-utils"
))]
fn validate_request(request: &OutboundRequest, mode: OutboundSendMode) -> Result<(), EdgeError> {
    if request.image_optimizer_options().is_some() {
        return Err(EdgeError::bad_request(
            "image_optimizer is not supported by cloudflare",
        ));
    }
    validate_for_dispatch(request, mode)
}

/// Runs the target-neutral Cloudflare batch preflight contract in native tests.
///
/// # Errors
/// Returns the same portable validation or batch-shape error as production dispatch.
#[cfg(feature = "test-utils")]
#[inline]
pub fn validate_batch_request_for_test(request: &OutboundRequest) -> Result<(), EdgeError> {
    validate_batch_request(request)
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

    #[test]
    fn reset_content_null_body_selects_abort_or_disarm_without_streaming() {
        assert_eq!(
            reset_content_action(false, true),
            ResetContentAction::Disarm
        );
        assert_eq!(reset_content_action(true, true), ResetContentAction::Abort);
        assert_eq!(
            reset_content_action(false, false),
            ResetContentAction::InspectStream
        );
        assert_eq!(reset_content_action(true, false), ResetContentAction::Abort);
    }
}
