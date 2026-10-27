#![cfg_attr(
    any(test, all(feature = "spin", target_arch = "wasm32")),
    expect(
        clippy::self_named_module_files,
        reason = "retain the repository module-file convention despite contradictory restriction lints"
    )
)]
#![cfg_attr(
    any(test, all(feature = "spin", target_arch = "wasm32")),
    expect(
        clippy::arbitrary_source_item_ordering,
        reason = "target and test helper modules stay adjacent to the imports that define their boundaries"
    )
)]
#![cfg_attr(
    all(feature = "spin", target_arch = "wasm32"),
    expect(
        clippy::pub_use,
        reason = "the target-gated implementation keeps Spin SDK imports out of native builds"
    )
)]

#[cfg(all(feature = "spin", any(feature = "test-utils", target_arch = "wasm32")))]
use std::time::Duration;

use edgezero_core::error::EdgeError;

#[cfg(any(test, all(feature = "spin", target_arch = "wasm32")))]
mod scheduling;
#[cfg(any(feature = "test-utils", all(feature = "spin", target_arch = "wasm32")))]
use edgezero_core::outbound::{OutboundRequest, OutboundSendMode, validate_for_dispatch};

#[cfg(all(feature = "spin", any(feature = "test-utils", target_arch = "wasm32")))]
use edgezero_core::error::{BadGatewayReason, BudgetSource};
#[cfg(all(feature = "spin", any(feature = "test-utils", target_arch = "wasm32")))]
use edgezero_core::error_detail::{
    ProviderFailureDetail, ProviderFailurePhase, SpinDnsRcode, SpinFailureCause, SpinFieldSize,
};
#[cfg(all(feature = "spin", any(feature = "test-utils", target_arch = "wasm32")))]
use edgezero_core::time::Deadline;
#[cfg(all(feature = "spin", any(feature = "test-utils", target_arch = "wasm32")))]
use spin_sdk::wasip3::http::types::{ErrorCode, FieldSizePayload};

#[cfg(any(test, all(feature = "spin", target_arch = "wasm32")))]
mod exchange {
    use std::future::{Future, IntoFuture};
    use std::task::Poll;
    use std::time::Duration;

    use async_stream::stream;
    use edgezero_core::body::BodyStream;
    use edgezero_core::error::EdgeError;
    use edgezero_core::error_detail::{ProviderFailureDetail, ProviderFailurePhase};
    use edgezero_core::{DispatchBudget, MonotonicInstant, OutboundTime};
    use futures_util::{StreamExt as _, future::poll_fn};

    pub(super) const READY_ITEM_YIELD_QUOTA: usize = 64;

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    pub(super) enum UploadCompletion {
        Complete,
        ReaderGone,
    }

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    pub(super) struct RequestTimeouts {
        between_bytes: u64,
        connect: u64,
        first_byte: u64,
    }

    impl RequestTimeouts {
        pub(super) fn between_bytes(self) -> u64 {
            self.between_bytes
        }

        pub(super) fn connect(self) -> u64 {
            self.connect
        }

        pub(super) fn first_byte(self) -> u64 {
            self.first_byte
        }
    }

    pub(super) fn request_timeouts(
        remaining: Duration,
        connect: Option<Duration>,
    ) -> RequestTimeouts {
        let full_remaining = duration_nanos(remaining);
        RequestTimeouts {
            between_bytes: full_remaining,
            connect: duration_nanos(
                connect.map_or(remaining, |configured| configured.min(remaining)),
            ),
            first_byte: full_remaining,
        }
    }

    pub(super) fn should_emit_response_chunk(length: usize) -> bool {
        length != 0
    }

    pub(super) fn duration_nanos(duration: Duration) -> u64 {
        u64::try_from(duration.as_nanos())
            .unwrap_or(u64::MAX)
            .max(1)
    }

    pub(super) async fn cooperative_yield_once() {
        let mut yielded = false;
        poll_fn(move |context| {
            if yielded {
                Poll::Ready(())
            } else {
                yielded = true;
                context.waker().wake_by_ref();
                Poll::Pending
            }
        })
        .await;
    }

    pub(super) async fn cooperate_after_response_read(ready_reads: &mut usize) {
        *ready_reads = ready_reads.saturating_add(1);
        if *ready_reads >= READY_ITEM_YIELD_QUOTA {
            *ready_reads = 0;
            cooperative_yield_once().await;
        }
    }

    pub(super) fn cooperative_stream(mut source: BodyStream) -> BodyStream {
        stream! {
            let mut ready_items = 0_usize;
            while let Some(item) = source.next().await {
                let terminal = item.is_err();
                yield item;
                if terminal {
                    return;
                }
                ready_items = ready_items.saturating_add(1);
                if ready_items >= READY_ITEM_YIELD_QUOTA {
                    cooperative_yield_once().await;
                    ready_items = 0;
                }
            }
        }
        .boxed_local()
    }

    #[cfg(test)]
    pub(super) async fn settle_reset_content(
        mut source: BodyStream,
        declared_body: bool,
    ) -> Result<(), EdgeError> {
        if !declared_body && let Some(item) = source.next().await {
            item?;
        }
        Ok(())
    }

    pub(super) fn checked_now(
        time: &OutboundTime,
        previous: &mut MonotonicInstant,
    ) -> Result<MonotonicInstant, EdgeError> {
        let now = time.clock().now();
        if now < *previous {
            return Err(EdgeError::internal(anyhow::anyhow!(
                "outbound clock moved backwards during Spin exchange"
            )));
        }
        *previous = now;
        Ok(now)
    }

    pub(super) async fn race_deadline<Output>(
        future: impl Future<Output = Output>,
        budget: DispatchBudget,
        time: &OutboundTime,
        previous: &mut MonotonicInstant,
        failure_detail: impl FnOnce(&Output) -> Option<ProviderFailureDetail>,
    ) -> Result<Output, EdgeError> {
        let timer = time.wait_until(budget.deadline);
        futures_util::pin_mut!(future, timer);
        let output = poll_fn(|context| {
            checked_now(time, previous)?;
            if let Poll::Ready(result) = timer.as_mut().poll(context) {
                result?;
                return Poll::Ready(Err(super::timeout_error(budget.cause)));
            }
            if budget.deadline.is_expired_at(checked_now(time, previous)?) {
                return Poll::Ready(Err(super::timeout_error(budget.cause)));
            }
            match future.as_mut().poll(context) {
                Poll::Ready(output) => Poll::Ready(Ok(output)),
                Poll::Pending => {
                    if budget.deadline.is_expired_at(checked_now(time, previous)?) {
                        return Poll::Ready(Err(super::timeout_error(budget.cause)));
                    }
                    Poll::Pending
                }
            }
        })
        .await?;
        if budget.deadline.is_expired_at(checked_now(time, previous)?) {
            let mut selected = super::timeout_error(budget.cause);
            if let EdgeError::GatewayTimeout {
                provider_detail, ..
            } = &mut selected
            {
                *provider_detail = failure_detail(&output);
            }
            return Err(selected);
        }
        Ok(output)
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(super) fn deadline_stream(
        mut source: BodyStream,
        budget: DispatchBudget,
        time: OutboundTime,
    ) -> BodyStream {
        stream! {
            let mut previous = time.clock().now();
            loop {
                let next_item = match race_deadline(source.next(), budget, &time, &mut previous, |item| {
                    item.as_ref().and_then(|result| result.as_ref().err()).and_then(EdgeError::provider_failure_detail).copied()
                }).await {
                    Ok(item) => item,
                    Err(error) => {
                        drop(source);
                        drop(time);
                        yield Err(error);
                        return;
                    }
                };
                match next_item {
                    Some(Ok(bytes)) => yield Ok(bytes),
                    Some(Err(error)) => {
                        drop(source);
                        drop(time);
                        yield Err(error);
                        return;
                    }
                    None => return,
                }
            }
        }
        .boxed_local()
    }

    pub(super) async fn run_exchange<Send, Pump, RequestDone, Output, HostError, MapError>(
        send_future: Send,
        pump_future: Pump,
        request_done_reader: RequestDone,
        map_error: MapError,
    ) -> Result<Output, EdgeError>
    where
        Send: Future<Output = Result<Output, HostError>>,
        Pump: Future<Output = Result<UploadCompletion, EdgeError>>,
        RequestDone: IntoFuture<Output = Result<(), HostError>>,
        MapError: Fn(&HostError, ProviderFailurePhase) -> EdgeError,
    {
        #[derive(Clone, Copy)]
        enum State {
            AwaitingRequestDone,
            ReaderGone,
            RequestComplete,
            Uploading,
        }

        enum Outcome<Output, HostError> {
            PumpError(EdgeError),
            RequestDoneError(HostError),
            Send(Result<Output, HostError>),
        }

        let mut state = State::Uploading;
        let mut retained_send = None;
        let mut send = Box::pin(send_future);
        let mut pump = Box::pin(pump_future);
        let mut request_done = Box::pin(request_done_reader.into_future());

        let outcome = poll_fn(|context| {
            loop {
                match state {
                    State::Uploading => match pump.as_mut().poll(context) {
                        Poll::Ready(Err(error)) => return Poll::Ready(Outcome::PumpError(error)),
                        Poll::Ready(Ok(UploadCompletion::Complete)) => {
                            state = State::AwaitingRequestDone;
                        }
                        Poll::Ready(Ok(UploadCompletion::ReaderGone)) => {
                            state = State::ReaderGone;
                        }
                        Poll::Pending => match send.as_mut().poll(context) {
                            Poll::Ready(result) => return Poll::Ready(Outcome::Send(result)),
                            Poll::Pending => return Poll::Pending,
                        },
                    },
                    State::AwaitingRequestDone => match request_done.as_mut().poll(context) {
                        Poll::Ready(Err(error)) => {
                            return Poll::Ready(Outcome::RequestDoneError(error));
                        }
                        Poll::Ready(Ok(())) => state = State::RequestComplete,
                        Poll::Pending => {
                            if retained_send.is_none()
                                && let Poll::Ready(result) = send.as_mut().poll(context)
                            {
                                retained_send = Some(result);
                            }
                            return Poll::Pending;
                        }
                    },
                    State::ReaderGone | State::RequestComplete => {
                        if let Some(result) = retained_send.take() {
                            return Poll::Ready(Outcome::Send(result));
                        }
                        match send.as_mut().poll(context) {
                            Poll::Ready(result) => return Poll::Ready(Outcome::Send(result)),
                            Poll::Pending => return Poll::Pending,
                        }
                    }
                }
            }
        })
        .await;

        drop(request_done);
        drop(pump);
        drop(send);
        match outcome {
            Outcome::PumpError(error) => Err(error),
            Outcome::RequestDoneError(error) => {
                Err(map_error(&error, ProviderFailurePhase::RequestBodyUpload))
            }
            Outcome::Send(Err(error)) => Err(map_error(&error, ProviderFailurePhase::Send)),
            Outcome::Send(Ok(output)) => Ok(output),
        }
    }
}

#[cfg(any(
    test,
    all(feature = "spin", target_arch = "wasm32", feature = "test-utils")
))]
use exchange::READY_ITEM_YIELD_QUOTA;
#[cfg(any(
    test,
    all(feature = "spin", target_arch = "wasm32", feature = "test-utils")
))]
use exchange::deadline_stream;
#[cfg(any(test, all(feature = "spin", target_arch = "wasm32")))]
use exchange::{
    UploadCompletion, cooperate_after_response_read, cooperative_stream, cooperative_yield_once,
    race_deadline, request_timeouts, run_exchange, should_emit_response_chunk,
};
#[cfg(test)]
use exchange::{duration_nanos, settle_reset_content};

#[cfg(all(feature = "spin", target_arch = "wasm32"))]
mod spin_impl {
    #[cfg(feature = "test-utils")]
    mod scope_tests;

    use core::ffi::c_void;
    use core::ptr::null_mut;
    #[cfg(any(test, feature = "test-utils"))]
    use std::cell::RefCell;
    #[cfg(any(test, feature = "test-utils"))]
    use std::num::NonZeroU64;
    use std::rc::Rc;
    use std::time::Duration;

    use async_stream::stream;
    use bytes::Bytes;
    use edgezero_core::body::{Body, BodyPayload, BodyStream};
    #[cfg(feature = "test-utils")]
    use edgezero_core::error::BudgetSource;
    use edgezero_core::error::{BadGatewayReason, EdgeError};
    use edgezero_core::error_detail::{
        ProviderFailureDetail, ProviderFailurePhase, SpinFailureCause, SpinTimeoutOption,
    };
    use edgezero_core::http::header::ACCEPT_ENCODING;
    use edgezero_core::http::{HeaderMap, HeaderName, HeaderValue, Method, StatusCode, Uri};
    use edgezero_core::outbound::{
        IndependentOutboundProgress, OutboundBatch, OutboundBatchDriverEvent,
        OutboundDispatchRequest, OutboundExchangeOwner, OutboundFuture, OutboundHead,
        OutboundHeaderFidelity, OutboundHeaderInput, OutboundHttpClient, OutboundPending,
        OutboundPreparedBatch, OutboundProgressRuntime, OutboundRequest, OutboundRequestParts,
        OutboundResponseContext, OutboundSendMode, OutboundTime,
    };
    #[cfg(any(test, feature = "test-utils"))]
    use edgezero_core::time::{Deadline, MonotonicInstant};
    use edgezero_core::time::{DispatchBudget, MonotonicClock};
    use futures_util::StreamExt as _;
    use futures_util::future::FutureExt as _;
    use futures_util::stream::{iter, once};
    use spin_sdk::time::sleep;
    use spin_sdk::wasip3::http::client;
    use spin_sdk::wasip3::http::types::{
        ErrorCode, Fields, HeaderError, Method as WasiMethod, Request, RequestOptions,
        RequestOptionsError, Response, Scheme,
    };
    use spin_sdk::wasip3::http_compat::BodyWriter;
    use spin_sdk::wasip3::spawn;
    use spin_sdk::wasip3::wit_bindgen::{FutureReader, FutureWriter, StreamReader, StreamResult};
    use spin_sdk::wasip3::wit_future;

    #[cfg(any(test, feature = "test-utils"))]
    use super::deadline_stream;
    use super::{
        UploadCompletion, cooperate_after_response_read, cooperative_yield_once,
        map_spin_dispatch_error, map_spin_host_error, race_deadline, run_exchange,
        should_emit_response_chunk, timeout_error,
    };

    const RESPONSE_READ_BYTES: usize = 16 * 1024;

    #[expect(
        unsafe_code,
        reason = "query and immediately restore the pinned wit-bindgen task-pointer ABI without dereferencing it"
    )]
    fn ensure_p3_context() -> Result<(), EdgeError> {
        unsafe extern "C" {
            fn wasip3_task_set(task: *mut c_void) -> *mut c_void;
        }
        // SAFETY: the pinned ABI permits this getter sequence; no callback or suspension intervenes.
        let prior = unsafe { wasip3_task_set(null_mut()) };
        // SAFETY: restore the identical opaque pointer immediately, without dereferencing it.
        let _restored = unsafe { wasip3_task_set(prior) };
        let active = !prior.is_null();
        if active {
            Ok(())
        } else {
            Err(EdgeError::internal(anyhow::anyhow!(
                "Spin outbound scheduling requires an active WASIp3 task"
            )))
        }
    }

    #[cfg(any(test, feature = "test-utils"))]
    use edgezero_core::time::dispatch_budget;
    #[cfg(any(test, feature = "test-utils"))]
    use edgezero_core::{OutboundProxyMarker, OutboundResponse, ResponseDecoding, ResponseMode};

    pub(crate) fn native_time(clock: MonotonicClock) -> OutboundTime {
        OutboundTime::new(clock, |deadline, paired_clock| {
            Box::pin(async move {
                ensure_p3_context()?;
                if let Some(remaining) = deadline.remaining_at(paired_clock.now()) {
                    sleep(remaining).await;
                }
                Ok(())
            })
        })
    }

    /// Native outbound HTTP implementation for Spin's WASI HTTP 0.3 host.
    #[derive(Clone)]
    pub struct SpinOutboundClient {
        time: OutboundTime,
    }

    #[derive(Clone, Copy)]
    enum RequestTargetPart {
        Authority,
        Method,
        Path,
        Scheme,
    }

    struct PreparedRequestBody {
        writer: Option<BodyWriter>,
        contents: Option<StreamReader<u8>>,
        trailers: FutureReader<Result<Option<Fields>, ErrorCode>>,
    }

    const EMPTY_REQUEST_TRAILERS: fn() -> Result<Option<Fields>, ErrorCode> = || Ok(None);

    fn prepare_request_body(method: &Method, body: &Body) -> PreparedRequestBody {
        if matches!(*method, Method::GET | Method::HEAD)
            && body.as_bytes().is_some_and(<[u8]>::is_empty)
        {
            // Publish clean trailers, not BodyWriter's error-on-drop default.
            let (complete, trailers) = wit_future::new(EMPTY_REQUEST_TRAILERS);
            drop(complete);
            return PreparedRequestBody {
                writer: None,
                contents: None,
                trailers,
            };
        }
        let (writer, contents, trailers) = BodyWriter::new();
        PreparedRequestBody {
            writer: Some(writer),
            contents: Some(contents),
            trailers,
        }
    }

    #[cfg(feature = "test-utils")]
    fn bodyless_request_resources_hold() -> bool {
        if !matches!(EMPTY_REQUEST_TRAILERS(), Ok(None)) {
            return false;
        }
        for method in [Method::GET, Method::HEAD] {
            let body = prepare_request_body(&method, &Body::empty());
            if body.writer.is_some() || body.contents.is_some() {
                drop((body.contents, body.trailers, body.writer));
                return false;
            }
            let (request, request_done) =
                Request::new(Fields::new(), body.contents, body.trailers, None);
            let (complete, completed) = wit_future::new(|| Ok(()));
            let (mut contents, trailers) = Request::consume_body(request, completed);
            let closed_empty = matches!(
                contents.read(Vec::with_capacity(1)).now_or_never(),
                Some((StreamResult::Dropped, bytes)) if bytes.is_empty()
            );
            // The local wire fixture checks the non-numeric trailer/completion values
            // across components; the pinned host cannot loop them back within one.
            drop((contents, trailers, request_done, complete));
            if !closed_empty {
                return false;
            }
        }
        for (method, source) in [
            (Method::POST, Body::empty()),
            (Method::GET, Body::from("payload")),
        ] {
            let PreparedRequestBody {
                writer,
                contents,
                trailers,
            } = prepare_request_body(&method, &source);
            let upload_retained = writer.is_some() && contents.is_some();
            drop((contents, trailers, writer));
            if !upload_retained {
                return false;
            }
        }
        true
    }

    impl SpinOutboundClient {
        /// Builds a client using the process-default monotonic clock.
        #[must_use]
        #[inline]
        pub fn new() -> Self {
            Self::with_time(native_time(MonotonicClock::default()))
        }

        /// Builds a client using one paired clock and deadline wakeup factory.
        #[must_use]
        #[inline]
        pub fn with_time(time: OutboundTime) -> Self {
            Self { time }
        }

        async fn execute(prepared: OutboundDispatchRequest) -> Result<OutboundHead, EdgeError> {
            ensure_p3_context()?;
            let (parts, context) = prepared.into_parts();
            let time = context.time().clone();
            let budget = context.budget();
            let mut previous = context.entered_at();
            let observed = super::exchange::checked_now(&time, &mut previous)?;
            if budget.deadline.is_expired_at(observed) {
                return Err(timeout_error(budget.cause));
            }
            let OutboundRequestParts {
                body,
                connect_timeout,
                mut headers,
                max_request_body_bytes,
                method,
                uri,
                ..
            } = parts;

            if !headers.contains_key(ACCEPT_ENCODING) {
                headers.insert(ACCEPT_ENCODING, HeaderValue::from_static("identity"));
            }

            let fields = request_fields(&headers)?;
            let options = request_options(budget, &time, connect_timeout)?;
            let PreparedRequestBody {
                writer,
                contents,
                trailers,
            } = prepare_request_body(&method, &body);
            let (request, request_done) = Request::new(fields, contents, trailers, Some(options));
            set_request_target(&request, &method, &uri)?;
            let exchange_time = time.clone();
            let exchange = async move {
                let upload = async {
                    match writer {
                        Some(upload_writer) => {
                            pump_request_body(
                                body,
                                max_request_body_bytes,
                                upload_writer,
                                budget,
                                exchange_time.clone(),
                            )
                            .await
                        }
                        None => Ok(UploadCompletion::Complete),
                    }
                };
                let send = client::send(request);
                run_exchange(send, upload, request_done, |error, phase| {
                    map_spin_dispatch_error(
                        error,
                        budget.deadline,
                        budget.cause,
                        exchange_time.clock().now(),
                        phase,
                        connect_timeout,
                    )
                })
                .await
            };
            let response = race_deadline(exchange, budget, &time, &mut previous, |result| {
                result
                    .as_ref()
                    .err()
                    .and_then(EdgeError::provider_failure_detail)
                    .copied()
            })
            .await??;

            response_head(response, context)
        }
    }

    impl Default for SpinOutboundClient {
        #[inline]
        fn default() -> Self {
            Self::new()
        }
    }

    impl OutboundHttpClient for SpinOutboundClient {
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
            request: &OutboundRequest,
            mode: OutboundSendMode,
        ) -> Result<(), EdgeError> {
            super::validate_request(request, mode)
        }

        #[inline]
        fn dispatch_head(&self, request: OutboundDispatchRequest) -> OutboundFuture<OutboundHead> {
            let scope = request.scope().clone();
            let time = request.time().clone();
            let entered_at = request.entered_at();
            scope.run(time, entered_at, Self::execute(request).boxed_local())
        }

        #[inline]
        fn begin(&self, request: OutboundDispatchRequest) -> Result<OutboundPending, EdgeError> {
            ensure_p3_context()?;
            let future = self.dispatch_head(request);
            super::scheduling::schedule_exchange(
                async move { future.await?.finish().await }.boxed_local(),
                spawn,
            )
        }

        #[inline]
        fn start_batch_until(&self, batch: OutboundPreparedBatch) -> OutboundBatch {
            let (context, slots) = batch.into_slots();
            if slots.is_empty() {
                return OutboundBatch::cutoff(context);
            }
            let completed_at = context.clock().now();
            if context.observation_cutoff().is_expired_at(completed_at) {
                let mut events = Vec::new();
                let mut unresolved = false;
                for (index, request) in slots {
                    let budget = request.budget();
                    if budget
                        .own_deadline_expired_before(context.observation_cutoff(), completed_at)
                    {
                        events.push(OutboundBatchDriverEvent::Item {
                            index,
                            completed_at,
                            outcome: Err(timeout_error(budget.cause)),
                        });
                    } else {
                        unresolved = true;
                    }
                }
                if unresolved {
                    events.push(OutboundBatchDriverEvent::Cutoff);
                }
                return OutboundBatch::from_driver(
                    context,
                    iter(events),
                    OutboundExchangeOwner::new(|| {}),
                );
            }
            if let Err(error) = ensure_p3_context() {
                return OutboundBatch::from_driver(
                    context,
                    once(async move { edgezero_core::OutboundBatchDriverEvent::Failed(error) }),
                    OutboundExchangeOwner::new(|| {}),
                );
            }
            let exchanges = slots
                .into_iter()
                .map(|(index, request)| {
                    let budget = request.budget();
                    let head = self.dispatch_head(request);
                    (
                        index,
                        budget,
                        async move { head.await?.finish().await }.boxed_local(),
                    )
                })
                .collect();
            super::scheduling::schedule_prepared_batch(context, exchanges, spawn)
        }
    }

    fn request_fields(headers: &HeaderMap) -> Result<Fields, EdgeError> {
        let fields = Fields::new();
        for (name, value) in headers {
            fields
                .append(name.as_str(), value.as_bytes())
                .map_err(|error| map_spin_preparation_header(&error))?;
        }
        Ok(fields)
    }

    fn request_options(
        budget: DispatchBudget,
        time: &OutboundTime,
        connect_timeout: Option<Duration>,
    ) -> Result<RequestOptions, EdgeError> {
        let options = RequestOptions::new();
        let remaining = budget_remaining(budget, time)?;
        let timeouts = super::request_timeouts(remaining, connect_timeout);
        apply_timeout_settings(timeouts, |option, value| match option {
            SpinTimeoutOption::Connect => options.set_connect_timeout(Some(value)),
            SpinTimeoutOption::FirstByte => options.set_first_byte_timeout(Some(value)),
            SpinTimeoutOption::BetweenBytes => options.set_between_bytes_timeout(Some(value)),
            _ => Err(RequestOptionsError::Other(None)),
        })?;
        Ok(options)
    }

    fn apply_timeout_settings(
        timeouts: super::exchange::RequestTimeouts,
        mut setter: impl FnMut(SpinTimeoutOption, u64) -> Result<(), RequestOptionsError>,
    ) -> Result<(), EdgeError> {
        for (option, value) in [
            (SpinTimeoutOption::Connect, timeouts.connect()),
            (SpinTimeoutOption::FirstByte, timeouts.first_byte()),
            (SpinTimeoutOption::BetweenBytes, timeouts.between_bytes()),
        ] {
            set_timeout_option(option, &setter(option, value))?;
        }
        Ok(())
    }

    #[cfg(feature = "test-utils")]
    #[doc(hidden)]
    #[inline]
    pub fn request_timeout_settings_for_test<Setter>(
        remaining: Duration,
        configured_connect: Option<Duration>,
        setter: Setter,
    ) -> Result<(), EdgeError>
    where
        Setter: FnMut(SpinTimeoutOption, u64) -> Result<(), RequestOptionsError>,
    {
        apply_timeout_settings(
            super::request_timeouts(remaining, configured_connect),
            setter,
        )
    }

    fn set_timeout_option(
        option: SpinTimeoutOption,
        outcome: &Result<(), RequestOptionsError>,
    ) -> Result<(), EdgeError> {
        let name = match option {
            SpinTimeoutOption::BetweenBytes => "between-bytes",
            SpinTimeoutOption::Connect => "connect",
            SpinTimeoutOption::FirstByte => "first-byte",
            _ => "unknown",
        };
        match outcome {
            Ok(()) => Ok(()),
            Err(RequestOptionsError::NotSupported) => {
                log::debug!("Spin host does not support the outbound {name} timeout option");
                Ok(())
            }
            Err(RequestOptionsError::Immutable | RequestOptionsError::Other(_))
                if option == SpinTimeoutOption::Connect =>
            {
                log::debug!(
                    "Spin host rejected the outbound connect timeout option; retaining the aggregate deadline race"
                );
                Ok(())
            }
            Err(RequestOptionsError::Immutable) => Err(EdgeError::internal_with_detail(
                anyhow::anyhow!("Spin outbound {name} timeout option is immutable"),
                preparation_detail(SpinFailureCause::RequestOptionsImmutable(option)),
            )),
            Err(RequestOptionsError::Other(_)) => Err(EdgeError::internal_with_detail(
                anyhow::anyhow!("Spin host rejected the outbound {name} timeout option"),
                preparation_detail(SpinFailureCause::RequestOptionsOther(option)),
            )),
        }
    }

    fn map_spin_preparation_header(error: &HeaderError) -> EdgeError {
        let cause = match error {
            HeaderError::Forbidden => SpinFailureCause::HeaderForbidden,
            HeaderError::Immutable => SpinFailureCause::HeaderImmutable,
            HeaderError::InvalidSyntax => SpinFailureCause::HeaderInvalidSyntax,
            HeaderError::Other(_) => SpinFailureCause::HeaderOther,
            HeaderError::SizeExceeded => SpinFailureCause::HeaderSizeExceeded,
        };
        EdgeError::bad_request_with_detail(
            "Spin rejected an outbound request header",
            preparation_detail(cause),
        )
    }

    fn preparation_detail(cause: SpinFailureCause) -> ProviderFailureDetail {
        ProviderFailureDetail::Spin {
            phase: ProviderFailurePhase::RequestPreparation,
            cause,
        }
    }

    #[cfg(feature = "test-utils")]
    #[doc(hidden)]
    #[must_use]
    #[inline]
    pub fn map_spin_preparation_header_for_test(error: &HeaderError) -> EdgeError {
        map_spin_preparation_header(error)
    }

    #[cfg(feature = "test-utils")]
    #[doc(hidden)]
    #[inline]
    pub fn request_fields_for_test(headers: &HeaderMap) -> Result<(), EdgeError> {
        request_fields(headers).map(|_fields| ())
    }

    #[cfg(feature = "test-utils")]
    #[doc(hidden)]
    #[inline]
    pub fn set_timeout_option_for_test(
        option: SpinTimeoutOption,
        outcome: &Result<(), RequestOptionsError>,
    ) -> Result<(), EdgeError> {
        set_timeout_option(option, outcome)
    }

    fn request_target_error(part: RequestTargetPart) -> EdgeError {
        let (message, cause) = match part {
            RequestTargetPart::Authority => (
                "Spin rejected the outbound request authority",
                SpinFailureCause::RequestAuthorityRejected,
            ),
            RequestTargetPart::Method => (
                "Spin rejected the outbound request method",
                SpinFailureCause::RequestMethodRejected,
            ),
            RequestTargetPart::Path => (
                "Spin rejected the outbound request path",
                SpinFailureCause::RequestPathRejected,
            ),
            RequestTargetPart::Scheme => (
                "Spin rejected the outbound request scheme",
                SpinFailureCause::RequestSchemeRejected,
            ),
        };
        EdgeError::bad_request_with_detail(message, preparation_detail(cause))
    }

    #[cfg(feature = "test-utils")]
    #[doc(hidden)]
    #[must_use]
    #[inline]
    pub fn request_target_errors_for_test() -> [EdgeError; 4] {
        [
            RequestTargetPart::Authority,
            RequestTargetPart::Method,
            RequestTargetPart::Path,
            RequestTargetPart::Scheme,
        ]
        .map(request_target_error)
    }

    fn set_request_target(request: &Request, method: &Method, uri: &Uri) -> Result<(), EdgeError> {
        request
            .set_method(&wasi_method(method))
            .map_err(|()| request_target_error(RequestTargetPart::Method))?;
        let scheme = match uri.scheme_str() {
            Some("http") => Scheme::Http,
            Some("https") => Scheme::Https,
            Some(other) => Scheme::Other(other.to_owned()),
            None => return Err(EdgeError::bad_request("outbound request URI has no scheme")),
        };
        request
            .set_scheme(Some(&scheme))
            .map_err(|()| request_target_error(RequestTargetPart::Scheme))?;
        let authority = uri
            .authority()
            .ok_or_else(|| EdgeError::bad_request("outbound request URI has no authority"))?;
        request
            .set_authority(Some(authority.as_str()))
            .map_err(|()| request_target_error(RequestTargetPart::Authority))?;
        request
            .set_path_with_query(Some(
                uri.path_and_query()
                    .map_or("/", |path_and_query| path_and_query.as_str()),
            ))
            .map_err(|()| request_target_error(RequestTargetPart::Path))?;
        Ok(())
    }

    fn wasi_method(method: &Method) -> WasiMethod {
        match *method {
            Method::GET => WasiMethod::Get,
            Method::HEAD => WasiMethod::Head,
            Method::POST => WasiMethod::Post,
            Method::PUT => WasiMethod::Put,
            Method::DELETE => WasiMethod::Delete,
            Method::CONNECT => WasiMethod::Connect,
            Method::OPTIONS => WasiMethod::Options,
            Method::TRACE => WasiMethod::Trace,
            Method::PATCH => WasiMethod::Patch,
            _ => WasiMethod::Other(method.as_str().to_owned()),
        }
    }

    async fn pump_request_body(
        body: Body,
        maximum: u64,
        writer: BodyWriter,
        budget: DispatchBudget,
        time: OutboundTime,
    ) -> Result<UploadCompletion, EdgeError> {
        let BodyWriter {
            mut stream_writer,
            result_writer,
            ..
        } = writer;
        let mut total = 0_u64;
        let (payload, _scope) = body.into_parts();
        let mut source = match payload {
            BodyPayload::Once(bytes) => once(async move { Ok(bytes) }).boxed_local(),
            BodyPayload::Stream(source) => source,
        };

        // `BodyWriter` configures `result_writer` to publish a typed host error on drop.
        // Deadline returns can therefore signal failure without awaiting after budget expiry.
        while let Some(item) = source.next().await {
            budget_remaining(budget, &time)?;
            let bytes = match item {
                Ok(bytes) => bytes,
                Err(error) => {
                    drop(stream_writer);
                    signal_upload_failure(result_writer, ErrorCode::InternalError(None)).await;
                    return Err(error);
                }
            };
            let length = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
            let Some(next_total) = total.checked_add(length) else {
                drop(stream_writer);
                signal_upload_failure(result_writer, ErrorCode::HttpRequestBodySize(None)).await;
                return Err(EdgeError::payload_too_large(
                    edgezero_core::RequestBodyOverflowOrigin::OutboundUpload,
                ));
            };
            if next_total > maximum {
                drop(stream_writer);
                signal_upload_failure(
                    result_writer,
                    ErrorCode::HttpRequestBodySize(Some(next_total)),
                )
                .await;
                return Err(EdgeError::payload_too_large(
                    edgezero_core::RequestBodyOverflowOrigin::OutboundUpload,
                ));
            }
            let unwritten = stream_writer.write_all(bytes.to_vec()).await;
            budget_remaining(budget, &time)?;
            if !unwritten.is_empty() {
                drop(stream_writer);
                drop(result_writer.write(Ok(None)).await);
                return Ok(UploadCompletion::ReaderGone);
            }
            total = next_total;
            cooperative_yield_once().await;
        }

        drop(stream_writer);
        if result_writer.write(Ok(None)).await.is_err() {
            return Ok(UploadCompletion::ReaderGone);
        }
        Ok(UploadCompletion::Complete)
    }

    async fn signal_upload_failure(
        writer: FutureWriter<Result<Option<Fields>, ErrorCode>>,
        error: ErrorCode,
    ) {
        match writer.write(Err(error)).await {
            Ok(()) | Err(_) => {}
        }
    }

    fn response_head(
        response: Response,
        context: OutboundResponseContext,
    ) -> Result<OutboundHead, EdgeError> {
        let time = context.time().clone();
        let budget = context.budget();
        budget_remaining(budget, &time)?;
        let status = StatusCode::from_u16(response.get_status_code()).map_err(|_error| {
            EdgeError::bad_gateway_with_reason(
                "Spin returned an invalid upstream status code",
                BadGatewayReason::Protocol,
            )
        })?;
        let headers = OutboundHeaderInput::new(
            "spin",
            response_headers(&response)?,
            OutboundHeaderFidelity::AdapterVisibleFields,
        );
        // The encoded stream alone owns the WASI reader, trailers and completion writer.
        OutboundHead::new(
            status,
            headers,
            super::cooperative_stream(response_stream(response, budget, time)),
            context,
            OutboundExchangeOwner::new(|| {}),
        )
    }

    #[cfg(any(test, feature = "test-utils"))]
    std::thread_local! {
        static FIXTURE_RESPONSE: RefCell<Option<Response>> = const { RefCell::new(None) };
    }

    #[cfg(any(test, feature = "test-utils"))]
    struct FixtureClient(OutboundTime);

    #[cfg(any(test, feature = "test-utils"))]
    impl OutboundHttpClient for FixtureClient {
        fn progress(&self) -> Rc<dyn OutboundProgressRuntime> {
            Rc::new(IndependentOutboundProgress)
        }

        fn time(&self) -> OutboundTime {
            self.0.clone()
        }

        fn validate_supported_options(
            &self,
            request: &OutboundRequest,
            mode: OutboundSendMode,
        ) -> Result<(), EdgeError> {
            super::validate_request(request, mode)
        }

        fn dispatch_head(&self, request: OutboundDispatchRequest) -> OutboundFuture<OutboundHead> {
            let optional_response = FIXTURE_RESPONSE.with(|slot| slot.borrow_mut().take());
            Box::pin(async move {
                let response = optional_response.ok_or_else(|| {
                    EdgeError::internal(anyhow::anyhow!("missing Spin fixture response"))
                })?;
                response_head(response, request.into_parts().1)
            })
        }

        fn begin(&self, request: OutboundDispatchRequest) -> Result<OutboundPending, EdgeError> {
            let head = self.dispatch_head(request);
            super::scheduling::schedule_exchange(
                async move { head.await?.finish().await }.boxed_local(),
                spawn,
            )
        }

        fn start_batch_until(&self, batch: OutboundPreparedBatch) -> OutboundBatch {
            let (context, requests) = batch.into_slots();
            if requests.is_empty()
                || context
                    .observation_cutoff()
                    .is_expired_at(context.clock().now())
            {
                return OutboundBatch::cutoff(context);
            }
            let slots = requests
                .into_iter()
                .map(|(index, request)| {
                    let budget = request.budget();
                    let head = self.dispatch_head(request);
                    (
                        index,
                        budget,
                        async move { head.await?.finish().await }.boxed_local(),
                    )
                })
                .collect();
            super::scheduling::schedule_prepared_batch(context, slots, spawn)
        }
    }

    #[cfg(any(test, feature = "test-utils"))]
    #[expect(
        clippy::too_many_arguments,
        reason = "existing response fixtures retain each independent policy assertion"
    )]
    async fn process_response(
        response: Response,
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
    ) -> Result<OutboundResponse, EdgeError> {
        use edgezero_core::{BudgetSource, HttpClient};

        struct Reset;
        #[expect(clippy::missing_trait_methods, reason = "Drop::pin_drop is unstable")]
        impl Drop for Reset {
            fn drop(&mut self) {
                FIXTURE_RESPONSE.with(|slot| drop(slot.borrow_mut().take()));
            }
        }

        let mut parts = OutboundRequest::get("https://example.com/")?.into_parts();
        parts.method = request_method;
        parts.proxy_marker = proxy_marker;
        parts.response_decoding = response_decoding;
        parts.response_mode = response_mode;
        parts.max_decoder_bytes = max_decoder_bytes;
        parts.max_brotli_window_bits = max_brotli_window_bits;
        parts.max_chunk_bytes = max_chunk_bytes;
        parts.max_decoded_response_bytes = max_decoded_response_bytes;
        parts.max_encoded_response_bytes = max_encoded_response_bytes;
        parts.max_response_header_bytes = max_response_header_bytes;
        parts.max_response_header_count = max_response_header_count;
        match budget.cause {
            BudgetSource::PerCallTimeout => parts.timeout = Some(budget.duration),
            BudgetSource::RequestDeadline => parts.deadline = Some(budget.deadline),
            BudgetSource::Default => {}
            BudgetSource::BatchCutoff
            | BudgetSource::ConnectTimeout
            | BudgetSource::IdleTimeout
            | BudgetSource::Unspecified
            | _ => {
                return Err(EdgeError::internal(anyhow::anyhow!(
                    "fixture requires a genuine single-send budget"
                )));
            }
        }
        FIXTURE_RESPONSE.with(|slot| *slot.borrow_mut() = Some(response));
        let _reset = Reset;
        HttpClient::with_client(FixtureClient(time))
            .send(OutboundRequest::from_parts(parts)?)
            .await
    }

    #[cfg(feature = "test-utils")]
    async fn check_eager_begin() -> Result<(), EdgeError> {
        use edgezero_core::HttpClient;

        ensure_p3_context()?;
        let (mut writer, contents, trailers) = BodyWriter::new();
        let (native, response_done) = Response::new(Fields::new(), Some(contents), trailers);
        FIXTURE_RESPONSE.with(|slot| *slot.borrow_mut() = Some(native));
        let client = HttpClient::with_client(FixtureClient(native_time(MonotonicClock::default())));
        let mut pending = client
            .begin(OutboundRequest::get("https://example.com/")?.stream_response())
            .await?;
        sleep(Duration::from_millis(1)).await;
        if !pending.is_ready() {
            return Err(EdgeError::internal(anyhow::anyhow!(
                "SDK begin did not progress during an unrelated wait"
            )));
        }
        let response = pending.collect().await?;
        if writer
            .stream_writer
            .write_all(vec![1])
            .now_or_never()
            .is_some()
        {
            return Err(EdgeError::internal(anyhow::anyhow!(
                "begun stream consumed or dropped its payload before application polling"
            )));
        }
        drop(response);
        if writer.stream_writer.write_all(vec![1]).now_or_never() != Some(vec![1]) {
            return Err(EdgeError::internal(anyhow::anyhow!(
                "collected stream failed to release its SDK reader"
            )));
        }
        drop((writer, response_done));
        Ok(())
    }

    /// Exercises canonical dispatch and assembly scope cleanup under a Spin HTTP host.
    #[cfg(feature = "test-utils")]
    #[doc(hidden)]
    #[inline]
    pub async fn canonical_scopes_hold_for_test() -> Result<(), EdgeError> {
        scope_tests::canonical_entrypoints_release_retained_handles().await
    }

    /// Runs in a real asynchronous WASIp3 export, unlike the synchronous SDK-resource binary.
    #[cfg(feature = "test-utils")]
    #[doc(hidden)]
    #[inline]
    pub async fn eager_outbound_paths_hold_for_test() -> Result<(), EdgeError> {
        use std::sync::{Arc, Mutex};

        use edgezero_core::{HttpClient, OutboundBatchTermination, OutboundCollectedSlot};

        ensure_p3_context()?;
        scope_tests::foreign_poll_cannot_reparent_prepared_dispatch().await?;
        scope_tests::canonical_callback_releases_retained_handles().await?;
        if !bodyless_request_resources_hold() {
            return Err(EdgeError::internal(anyhow::anyhow!(
                "certified empty GET/HEAD must omit WASI contents and close cleanly; other methods retain upload resources"
            )));
        }
        check_eager_begin().await?;

        let start = MonotonicInstant::now();
        let end = start
            .checked_add(Duration::from_secs(1))
            .ok_or_else(|| EdgeError::internal(anyhow::anyhow!("fixture cutoff overflow")))?;
        let observed = Arc::new(Mutex::new(start));
        let clock_now = Arc::clone(&observed);
        let time = native_time(MonotonicClock::new(move || {
            clock_now.lock().map_or(start, |current| *current)
        }));
        let (mut writer, contents, trailers) = BodyWriter::new();
        let (native, response_done) = Response::new(Fields::new(), Some(contents), trailers);
        native
            .set_status_code(StatusCode::NO_CONTENT.as_u16())
            .map_err(|()| EdgeError::internal(anyhow::anyhow!("fixture status rejected")))?;
        FIXTURE_RESPONSE.with(|slot| *slot.borrow_mut() = Some(native));
        let client = HttpClient::with_client(FixtureClient(time));
        let batch = client.start_batch_until(
            vec![
                OutboundRequest::get("https://example.com/rejected")?.stream_response(),
                OutboundRequest::get("https://example.com/ready")?,
            ],
            Deadline::at_instant(end),
        );
        sleep(Duration::from_millis(1)).await;
        *observed.lock().map_err(|_poisoned| {
            EdgeError::internal(anyhow::anyhow!("fixture clock poisoned"))
        })? = end;
        let results = batch
            .collect()
            .await
            .map_err(|failure| failure.into_parts().0)?;
        if results.termination() != OutboundBatchTermination::Completed
            || !matches!(results.slots(), [OutboundCollectedSlot::Resolved(rejected), OutboundCollectedSlot::Resolved(ready)]
                if matches!(&rejected.outcome, Err(EdgeError::BadRequest { .. })) && ready.outcome.is_ok())
        {
            return Err(EdgeError::internal(anyhow::anyhow!(
                "eager batch lost an original index, preflight outcome or on-time arrival"
            )));
        }
        drop(results);
        if writer.stream_writer.write_all(vec![1]).now_or_never() != Some(vec![1]) {
            return Err(EdgeError::internal(anyhow::anyhow!(
                "buffered batch retained its SDK consumer"
            )));
        }
        drop((writer, response_done));
        handoff_tests::check_batch_handoff().await
    }

    fn response_headers(response: &Response) -> Result<HeaderMap, EdgeError> {
        let fields = response.get_headers();
        let mut headers = HeaderMap::new();
        for (raw_name, raw_value) in fields.copy_all() {
            let header_name = HeaderName::from_bytes(raw_name.as_bytes()).map_err(|_error| {
                EdgeError::bad_gateway_with_reason(
                    "Spin returned an invalid upstream header name",
                    BadGatewayReason::Protocol,
                )
            })?;
            let header_value = HeaderValue::from_bytes(&raw_value).map_err(|_error| {
                EdgeError::bad_gateway_with_reason(
                    "Spin returned an invalid upstream header value",
                    BadGatewayReason::Protocol,
                )
            })?;
            headers.append(header_name, header_value);
        }
        Ok(headers)
    }

    fn default_response_result() -> Result<(), ErrorCode> {
        Err(ErrorCode::InternalError(Some(
            "response body consumer dropped before completion".to_owned(),
        )))
    }

    fn response_stream(
        response: Response,
        budget: DispatchBudget,
        time: OutboundTime,
    ) -> BodyStream {
        let (completion_writer, result_reader) = wit_future::new(default_response_result);
        let (mut body_reader, trailer_reader) = Response::consume_body(response, result_reader);
        stream! {
            let mut ready_reads = 0_usize;
            let mut previous = time.clock().now();
            loop {
                let read = body_reader.read(Vec::with_capacity(RESPONSE_READ_BYTES));
                let (result, chunk) = match race_deadline(read, budget, &time, &mut previous, |_result| None).await {
                    Ok(value) => value,
                    Err(error) => {
                        yield Err(error);
                        return;
                    }
                };
                match result {
                    StreamResult::Complete(length) => {
                        if should_emit_response_chunk(length) {
                            yield Ok(Bytes::from(chunk));
                        }
                        cooperate_after_response_read(&mut ready_reads).await;
                    }
                    StreamResult::Dropped => {
                        let trailer_result = race_deadline(
                            async move { trailer_reader.await },
                            budget,
                            &time,
                            &mut previous,
                            |trailers| trailers.as_ref().err().map(|source| super::ProviderFailureDetail::Spin {
                                phase: ProviderFailurePhase::ResponseBodyRead, cause: super::spin_failure_cause(source),
                            }),
                        )
                        .await;
                        match trailer_result {
                            Ok(Ok(_trailers)) => {}
                            Ok(Err(error)) => {
                                yield Err(map_spin_host_error(
                                    &error,
                                    budget.deadline,
                                    budget.cause,
                                    time.clock().now(),
                                    ProviderFailurePhase::ResponseBodyRead,
                                ));
                                return;
                            }
                            Err(error) => {
                                yield Err(error);
                                return;
                            }
                        }
                        let completion = completion_writer.write(Ok(()));
                        match race_deadline(completion, budget, &time, &mut previous, |_result| None).await {
                            Ok(Ok(())) => return,
                            Ok(Err(_closed)) => {
                                yield Err(EdgeError::bad_gateway_with_reason(
                                    "Spin response completion reader closed",
                                    BadGatewayReason::Protocol,
                                ));
                                return;
                            }
                            Err(error) => {
                                yield Err(error);
                                return;
                            }
                        }
                    }
                    StreamResult::Cancelled => {
                        yield Err(EdgeError::bad_gateway_with_reason(
                            "Spin cancelled the upstream response body read",
                            BadGatewayReason::Transport,
                        ));
                        return;
                    }
                }
            }
        }
        .boxed_local()
    }

    fn budget_remaining(
        budget: DispatchBudget,
        time: &OutboundTime,
    ) -> Result<Duration, EdgeError> {
        budget
            .deadline
            .remaining_at(time.clock().now())
            .map(|remaining| remaining.min(budget.duration))
            .ok_or_else(|| timeout_error(budget.cause))
    }

    /// Exercises real timer wakeups; requires an asynchronous WASI service export.
    #[cfg(feature = "test-utils")]
    #[doc(hidden)]
    #[inline]
    pub async fn connect_timeout_fallback_holds_for_test() -> bool {
        use std::cell::Cell;
        use std::rc::Rc;
        use std::sync::{Arc, Mutex};

        use futures_util::future::pending;

        for outcome in [
            Ok(()),
            Err(RequestOptionsError::NotSupported),
            Err(RequestOptionsError::Immutable),
            Err(RequestOptionsError::Other(None)),
            Err(RequestOptionsError::Other(Some(
                "provider-secret-token".to_owned(),
            ))),
        ] {
            if set_timeout_option(SpinTimeoutOption::Connect, &outcome).is_err() {
                return false;
            }
            for cause in [
                BudgetSource::PerCallTimeout,
                BudgetSource::RequestDeadline,
                BudgetSource::BatchCutoff,
                BudgetSource::Default,
            ] {
                for expired in [false, true] {
                    let start = MonotonicInstant::now();
                    let duration = Duration::from_millis(2);
                    let Some(end) = start.checked_add(duration) else {
                        return false;
                    };
                    let budget = DispatchBudget {
                        cause,
                        deadline: Deadline::at_instant(end),
                        duration,
                    };
                    let now = Arc::new(Mutex::new(if expired { end } else { start }));
                    let observed = Arc::clone(&now);
                    let time = OutboundTime::new(
                        MonotonicClock::new(move || {
                            observed.lock().map_or(start, |current| *current)
                        }),
                        move |deadline, clock| {
                            let advanced = Arc::clone(&now);
                            Box::pin(async move {
                                if let Some(remaining) = deadline.remaining_at(clock.now()) {
                                    sleep(remaining).await;
                                }
                                *advanced.lock().map_err(|_poisoned| {
                                    EdgeError::internal(anyhow::anyhow!("fixture clock poisoned"))
                                })? = deadline.instant();
                                Ok(())
                            })
                        },
                    );
                    let owner = Rc::new(());
                    let released = Rc::downgrade(&owner);
                    let polls = Rc::new(Cell::new(0_usize));
                    let operation_polls = Rc::clone(&polls);
                    let operation = async move {
                        operation_polls.set(operation_polls.get().saturating_add(1));
                        pending::<()>().await;
                        drop(owner);
                    };
                    let mut previous = time.clock().now();
                    let result =
                        race_deadline(operation, budget, &time, &mut previous, |()| None).await;
                    if !matches!(result, Err(EdgeError::GatewayTimeout { cause: actual, provider_detail: None, .. }) if actual == cause)
                        || polls.get() != usize::from(!expired)
                        || released.upgrade().is_some()
                    {
                        return false;
                    }
                }
                let start = MonotonicInstant::now();
                let duration = Duration::from_secs(1);
                let Some(end) = start.checked_add(duration) else {
                    return false;
                };
                let budget = DispatchBudget {
                    cause,
                    deadline: Deadline::at_instant(end),
                    duration,
                };
                let time = native_time(MonotonicClock::new(move || start));
                let mut previous = start;
                if !matches!(
                    race_deadline(async { 7_u8 }, budget, &time, &mut previous, |_output| None)
                        .await,
                    Ok(7)
                ) {
                    return false;
                }
            }
        }
        true
    }

    /// Runs request-option, response-clock, and fairness checks in the hosted contract binary.
    #[cfg(feature = "test-utils")]
    #[doc(hidden)]
    #[inline]
    pub async fn deferred_clock_paths_hold_for_test() -> bool {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};

        let start = MonotonicInstant::now();
        let Ok(request) = OutboundRequest::get("https://example.com/") else {
            return false;
        };
        let Ok(budget) = dispatch_budget(&request.timeout(Duration::from_millis(10)), start, None)
        else {
            return false;
        };
        let observed = start.checked_add(Duration::from_millis(3)).unwrap_or(start);
        let option_observations = Arc::new(AtomicUsize::new(0));
        let observed_options = Arc::clone(&option_observations);
        let option_clock = MonotonicClock::new(move || {
            observed_options.fetch_add(1, Ordering::SeqCst);
            observed
        });
        if request_options(budget, &native_time(option_clock), None).is_err()
            || option_observations.load(Ordering::SeqCst) != 1
        {
            return false;
        }

        let mut ready_reads = 0_usize;
        for _ in 0..super::READY_ITEM_YIELD_QUOTA {
            cooperate_after_response_read(&mut ready_reads).await;
        }
        if ready_reads != 0 {
            return false;
        }

        let stream_observations = Arc::new(AtomicUsize::new(0));
        let observed_stream = Arc::clone(&stream_observations);
        let deadline = budget.deadline.instant();
        let stream_clock = MonotonicClock::new(move || {
            if observed_stream.fetch_add(1, Ordering::SeqCst) == 0 {
                start
            } else {
                deadline
            }
        });
        let source = once(async { Ok(Bytes::from_static(b"body")) }).boxed_local();
        let mut body = deadline_stream(source, budget, native_time(stream_clock));

        matches!(
            body.next().await,
            Some(Err(EdgeError::GatewayTimeout { .. }))
        ) && handoff_tests::check_response_handoff().await.is_ok()
    }

    /// Checks ready-error diagnostic retention through paired deadline races.
    #[cfg(feature = "test-utils")]
    #[doc(hidden)]
    #[inline]
    pub async fn provider_deadline_diagnostics_hold_for_test() -> bool {
        use std::sync::{Arc, Mutex};
        use std::task::{Poll, Waker};

        use futures_util::future::poll_fn;

        let detail = super::ProviderFailureDetail::Spin {
            phase: ProviderFailurePhase::Send,
            cause: super::SpinFailureCause::ConnectionRefused,
        };
        for body in [false, true] {
            let start = MonotonicInstant::now();
            let Some(end) = start.checked_add(Duration::from_secs(1)) else {
                return false;
            };
            let budget = DispatchBudget {
                deadline: Deadline::at_instant(end),
                duration: Duration::from_secs(1),
                cause: edgezero_core::BudgetSource::PerCallTimeout,
            };
            let observations = Arc::new(Mutex::new((start, None::<Waker>)));
            let observed = Arc::clone(&observations);
            let timer_clock = Arc::clone(&observations);
            let time = OutboundTime::new(
                MonotonicClock::new(move || observed.lock().map_or(start, |current| current.0)),
                move |deadline, _clock| {
                    let pending_clock = Arc::clone(&timer_clock);
                    Box::pin(poll_fn(move |context| {
                        let mut pending = pending_clock.lock().map_err(|_poisoned| {
                            EdgeError::internal(anyhow::anyhow!("fixture clock poisoned"))
                        })?;
                        if deadline.is_expired_at(pending.0) {
                            Poll::Ready(Ok(()))
                        } else {
                            pending.1 = Some(context.waker().clone());
                            Poll::Pending
                        }
                    }))
                },
            );
            let source_error = EdgeError::bad_gateway_with_detail(
                "host failed",
                BadGatewayReason::Unreachable,
                detail,
            );
            let operation = async move {
                let waiting = {
                    let mut advanced = observations.lock().map_err(|_poisoned| {
                        EdgeError::internal(anyhow::anyhow!("fixture clock poisoned"))
                    })?;
                    advanced.0 = end;
                    advanced.1.take()
                };
                if let Some(registered_waker) = waiting {
                    registered_waker.wake();
                }
                Err::<(), _>(source_error)
            };
            let error = if body {
                let source = once(async move {
                    operation.await?;
                    Ok(Bytes::new())
                })
                .boxed_local();
                let Some(Err(error)) = deadline_stream(source, budget, time).next().await else {
                    return false;
                };
                error
            } else {
                let mut previous = time.clock().now();
                let Err(error) = race_deadline(operation, budget, &time, &mut previous, |result| {
                    result
                        .as_ref()
                        .err()
                        .and_then(EdgeError::provider_failure_detail)
                        .copied()
                })
                .await
                else {
                    return false;
                };
                error
            };
            if error.provider_failure_detail() != Some(&detail)
                || !matches!(error, EdgeError::GatewayTimeout { .. })
            {
                return false;
            }
        }
        true
    }

    /// Exercises original Content-Type evidence through hosted response conversion.
    #[cfg(feature = "test-utils")]
    #[doc(hidden)]
    #[inline]
    pub async fn converted_response_content_type_provenance_holds_for_test() -> bool {
        content_type_tests::check_converted_response_content_type_provenance()
            .await
            .is_ok()
    }

    /// Proves response conversion retains the client clock in the hosted contract binary.
    #[cfg(feature = "test-utils")]
    #[doc(hidden)]
    #[inline]
    pub async fn converted_response_retains_injected_clock_for_test() -> bool {
        use std::sync::{Arc, Mutex};

        fn set_clock(clock: &Mutex<MonotonicInstant>, observed_at: MonotonicInstant) -> bool {
            let Ok(mut current) = clock.lock() else {
                return false;
            };
            *current = observed_at;
            true
        }

        let start = MonotonicInstant::now();
        let duration = Duration::from_secs(1);
        let budget = DispatchBudget {
            cause: BudgetSource::PerCallTimeout,
            deadline: Deadline::at_instant(start.checked_add(duration).unwrap_or(start)),
            duration,
        };
        let clock_now = Arc::new(Mutex::new(start));
        let clock_observed = Arc::clone(&clock_now);
        let clock =
            MonotonicClock::new(move || clock_observed.lock().map_or(start, |current| *current));
        let (writer, contents, trailers) = BodyWriter::new();
        let (native, response_done) = Response::new(Fields::new(), Some(contents), trailers);
        if native
            .set_status_code(StatusCode::NO_CONTENT.as_u16())
            .is_err()
        {
            return false;
        }
        let Ok(response) = process_response(
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
        else {
            return false;
        };
        drop((writer, response_done));
        if !set_clock(&clock_now, budget.deadline.instant()) {
            return false;
        }

        matches!(
            response.into_bytes_bounded_until(1, budget.deadline).await,
            Err(EdgeError::GatewayTimeout { .. })
        )
    }

    #[cfg(test)]
    mod clock_tests {
        use std::collections::VecDeque;
        use std::sync::{Arc, Mutex};

        use edgezero_core::error::BudgetSource;
        use edgezero_core::time::Deadline;
        use futures_util::stream;
        use spin_sdk::wasip3::wit_bindgen::block_on;

        use super::*;

        fn scripted_clock(script: Vec<MonotonicInstant>) -> MonotonicClock {
            let last = *script.last().expect("clock observations");
            let observations = Arc::new(Mutex::new(VecDeque::from(script)));
            MonotonicClock::new(move || {
                observations
                    .lock()
                    .expect("clock observations")
                    .pop_front()
                    .unwrap_or(last)
            })
        }

        fn test_budget(start: MonotonicInstant, duration: Duration) -> DispatchBudget {
            DispatchBudget {
                cause: BudgetSource::PerCallTimeout,
                deadline: Deadline::at_instant(start.checked_add(duration).expect("deadline")),
                duration,
            }
        }

        #[test]
        fn paired_constructors_do_not_sample_the_clock() {
            use std::sync::atomic::{AtomicUsize, Ordering};

            let observations = Arc::new(AtomicUsize::new(0));
            let observed = Arc::clone(&observations);
            let start = MonotonicInstant::now();
            let clock = MonotonicClock::new(move || {
                observed.fetch_add(1, Ordering::SeqCst);
                start
            });
            let _client = SpinOutboundClient::with_time(native_time(clock));
            assert_eq!(observations.load(Ordering::SeqCst), 0);
        }

        #[test]
        fn method_entry_and_preflight_elapsed_use_the_injected_clock() {
            let start = MonotonicInstant::now();
            let completed = start
                .checked_add(Duration::from_millis(9))
                .expect("completed instant");
            let client = edgezero_core::HttpClient::with_client(SpinOutboundClient::with_time(
                native_time(scripted_clock(vec![start, completed])),
            ));
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
                                .expect("cutoff instant"),
                        ),
                    )
                    .collect(),
            )
            .expect("valid batch driver");

            let edgezero_core::OutboundCollectedSlot::Resolved(slot) = &results.slots()[0] else {
                panic!("resolved slot");
            };
            assert_eq!(slot.elapsed, Duration::from_millis(9));
            assert!(matches!(slot.outcome, Err(EdgeError::BadRequest { .. })));
        }

        #[test]
        fn backwards_clock_fails_slot_without_invalid_elapsed() {
            let start = MonotonicInstant::now();
            let earlier = start
                .checked_sub(Duration::from_millis(1))
                .expect("earlier instant");
            let client = edgezero_core::HttpClient::with_client(SpinOutboundClient::with_time(
                native_time(scripted_clock(vec![start, earlier])),
            ));
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
                                .expect("cutoff instant"),
                        ),
                    )
                    .collect(),
            )
            .expect("valid batch driver");

            let edgezero_core::OutboundCollectedSlot::Resolved(slot) = &results.slots()[0] else {
                panic!("resolved slot");
            };
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
                budget_remaining(budget, &native_time(clock)).expect("remaining budget"),
                budget.duration
            );
        }

        #[test]
        fn request_option_preparation_consumes_injected_budget() {
            let start = MonotonicInstant::now();
            let budget = test_budget(start, Duration::from_millis(10));
            let observed = start
                .checked_add(Duration::from_millis(3))
                .expect("observed instant");
            let observations = Arc::new(Mutex::new(VecDeque::from([observed])));
            let clock_observations = Arc::clone(&observations);
            let clock = MonotonicClock::new(move || {
                clock_observations
                    .lock()
                    .expect("clock observations")
                    .pop_front()
                    .expect("clock observation")
            });

            let _options =
                request_options(budget, &native_time(clock), None).expect("request options");

            assert!(observations.lock().expect("clock observations").is_empty());
        }

        #[test]
        fn response_conversion_checks_deadline_before_handoff() {
            block_on(super::handoff_tests::check_response_handoff()).expect("handoff deadlines");
        }

        #[test]
        fn response_stream_retains_clock_for_post_ready_expiry() {
            let start = MonotonicInstant::now();
            let budget = test_budget(start, Duration::from_millis(10));
            let observed = Arc::new(Mutex::new(start));
            let clock_now = Arc::clone(&observed);
            let time = native_time(MonotonicClock::new(move || {
                *clock_now.lock().expect("clock")
            }));
            let source = stream::once(async move {
                *observed.lock().expect("clock") = budget.deadline.instant();
                Ok(Bytes::from_static(b"body"))
            })
            .boxed_local();
            let mut body = deadline_stream(source, budget, time);

            let error = block_on(body.next())
                .expect("terminal item")
                .expect_err("post-ready expiry");

            assert!(matches!(error, EdgeError::GatewayTimeout { .. }));
        }

        #[cfg(feature = "test-utils")]
        #[test]
        fn converted_response_retains_injected_clock() {
            assert!(block_on(
                converted_response_retains_injected_clock_for_test()
            ));
        }

        #[test]
        fn converted_response_preserves_original_content_type_provenance() {
            block_on(super::content_type_tests::check_converted_response_content_type_provenance())
                .expect("converted content-type provenance");
        }
    }

    #[cfg(any(test, feature = "test-utils"))]
    mod handoff_tests {
        use std::sync::atomic::{AtomicUsize, Ordering};

        use edgezero_core::error::BudgetSource;
        use edgezero_core::time::DEFAULT_NO_DEADLINE_BUDGET;

        use super::*;

        #[expect(
            clippy::too_many_lines,
            reason = "preserve the full method, status, response-mode and deadline boundary matrix"
        )]
        pub(super) async fn check_response_handoff() -> Result<(), EdgeError> {
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
                        BudgetSource::Default,
                    ] {
                        for offset in [9_u64, 10, 11] {
                            let start = MonotonicInstant::now();
                            let duration = if cause == BudgetSource::Default {
                                DEFAULT_NO_DEADLINE_BUDGET
                            } else {
                                Duration::from_millis(10)
                            };
                            let deadline = start.checked_add(duration).ok_or_else(|| {
                                EdgeError::internal(anyhow::anyhow!("fixture deadline overflow"))
                            })?;
                            let observed = start
                                .checked_add(
                                    duration
                                        .saturating_sub(Duration::from_millis(10))
                                        .saturating_add(Duration::from_millis(offset)),
                                )
                                .ok_or_else(|| {
                                    EdgeError::internal(anyhow::anyhow!(
                                        "fixture observation overflow"
                                    ))
                                })?;
                            let budget = DispatchBudget {
                                cause,
                                deadline: Deadline::at_instant(deadline),
                                duration,
                            };
                            let observed_samples = AtomicUsize::new(0);
                            let clock = MonotonicClock::new(move || {
                                if observed_samples.fetch_add(1, Ordering::SeqCst) == 0 {
                                    start
                                } else {
                                    observed
                                }
                            });
                            let fields = Fields::new();
                            fields
                                .append("content-length", b"1")
                                .map_err(EdgeError::internal)?;
                            let (mut writer, contents, trailers) = BodyWriter::new();
                            let (native, _response_done) =
                                Response::new(fields, Some(contents), trailers);
                            native.set_status_code(status.as_u16()).map_err(|()| {
                                EdgeError::internal(anyhow::anyhow!("fixture status rejected"))
                            })?;
                            let result = process_response(
                                native,
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
                                native_time(clock),
                            )
                            .await;
                            let expected = if offset < 10 {
                                drop(result?);
                                true
                            } else {
                                matches!(result, Err(EdgeError::GatewayTimeout {cause: actual, ..}) if actual == cause)
                            };
                            if !expected {
                                return Err(EdgeError::internal(anyhow::anyhow!(
                                    "expired handoff lost its selected budget"
                                )));
                            }
                            if writer.stream_writer.write_all(vec![1]).now_or_never()
                                != Some(vec![1])
                            {
                                return Err(EdgeError::internal(anyhow::anyhow!(
                                    "handoff retained its native body consumer"
                                )));
                            }
                        }
                    }
                }
            }
            Ok(())
        }

        pub(super) async fn check_batch_handoff() -> Result<(), EdgeError> {
            use edgezero_core::{HttpClient, OutboundBatchTermination, OutboundCollectedSlot};

            for (method, status) in [
                (Method::HEAD, StatusCode::OK),
                (Method::GET, StatusCode::NOT_MODIFIED),
                (Method::GET, StatusCode::NO_CONTENT),
                (Method::GET, StatusCode::RESET_CONTENT),
            ] {
                for offset in [9_u64, 10, 11] {
                    let start = MonotonicInstant::now();
                    let end = start
                        .checked_add(Duration::from_millis(10))
                        .ok_or_else(|| {
                            EdgeError::internal(anyhow::anyhow!("fixture cutoff overflow"))
                        })?;
                    let observed = start
                        .checked_add(Duration::from_millis(offset))
                        .ok_or_else(|| {
                            EdgeError::internal(anyhow::anyhow!("fixture observation overflow"))
                        })?;
                    let samples = AtomicUsize::new(0);
                    let clock = MonotonicClock::new(move || {
                        if samples.fetch_add(1, Ordering::SeqCst) == 0 {
                            start
                        } else {
                            observed
                        }
                    });
                    let fields = Fields::new();
                    fields
                        .append("content-length", b"1")
                        .map_err(EdgeError::internal)?;
                    let (mut writer, contents, trailers) = BodyWriter::new();
                    let (native, response_done) = Response::new(fields, Some(contents), trailers);
                    native.set_status_code(status.as_u16()).map_err(|()| {
                        EdgeError::internal(anyhow::anyhow!("fixture status rejected"))
                    })?;
                    FIXTURE_RESPONSE.with(|slot| *slot.borrow_mut() = Some(native));
                    let request = OutboundRequest::new(
                        method.clone(),
                        Uri::from_static("https://example.com/"),
                    )?
                    .max_response_bytes(1);
                    let result = HttpClient::with_client(FixtureClient(native_time(clock)))
                        .start_batch_until(vec![request], Deadline::at_instant(end))
                        .collect()
                        .await;
                    FIXTURE_RESPONSE.with(|slot| drop(slot.borrow_mut().take()));
                    let results = result.map_err(|failure| failure.into_parts().0)?;
                    let correct = if offset < 10 {
                        results.termination() == OutboundBatchTermination::Completed
                            && matches!(results.slots(), [OutboundCollectedSlot::Resolved(slot)] if slot.outcome.is_ok())
                    } else {
                        results.termination() == OutboundBatchTermination::Cutoff
                            && matches!(results.slots(), [OutboundCollectedSlot::Unresolved])
                    };
                    drop(results);
                    drop(response_done);
                    if !correct
                        || writer.stream_writer.write_all(vec![1]).now_or_never() != Some(vec![1])
                    {
                        return Err(EdgeError::internal(anyhow::anyhow!(
                            "batch handoff lost cutoff state or retained its SDK consumer"
                        )));
                    }
                }
            }
            Ok(())
        }
    }

    #[cfg(any(test, feature = "test-utils"))]
    mod content_type_tests {
        use edgezero_core::error::BudgetSource;
        use edgezero_core::http::header::{CONNECTION, CONTENT_LENGTH, CONTENT_TYPE};
        use edgezero_core::outbound::{ContentTypeIneligibility, ContentTypeProvenance};
        use spin_sdk::wasip3::http::types::HeaderError;

        use super::*;

        type ContentTypeCase = (&'static [&'static [u8]], ContentTypeProvenance, usize);

        fn content_type_cases() -> &'static [ContentTypeCase] {
            use ContentTypeIneligibility::{Missing, MultipleValues, NonText};
            use ContentTypeProvenance::{Ineligible, SingleTextUnchanged};

            &[
                (&[], Ineligible(Missing), 0),
                (&[b"text/plain"], SingleTextUnchanged, 1),
                (&[b"text/plain; label=caf\xc3\xa9"], SingleTextUnchanged, 1),
                (
                    &[b"text/plain", b"text/plain"],
                    Ineligible(MultipleValues),
                    2,
                ),
                (
                    &[b"text/plain", b"application/json"],
                    Ineligible(MultipleValues),
                    2,
                ),
                (&[b"text/plain", b"\xff"], Ineligible(MultipleValues), 1),
                (&[b"\xff"], Ineligible(NonText), 0),
                (&[b"text/plain, application/json"], SingleTextUnchanged, 1),
                (&[b"not a MIME type"], SingleTextUnchanged, 1),
            ]
        }

        async fn convert_response(
            values: &[&[u8]],
            status: StatusCode,
            mode: ResponseMode,
            expected: ContentTypeProvenance,
            retained_count: usize,
        ) -> Result<(), EdgeError> {
            let start = MonotonicInstant::now();
            let fields = Fields::new();
            for value in values {
                fields
                    .append(CONTENT_TYPE.as_str(), value)
                    .map_err(EdgeError::internal)?;
            }
            if status == StatusCode::RESET_CONTENT {
                fields
                    .append(CONTENT_LENGTH.as_str(), b"1")
                    .map_err(EdgeError::internal)?;
            }
            let (writer, contents, trailers) = BodyWriter::new();
            let (native, response_done) = Response::new(fields, Some(contents), trailers);
            native
                .set_status_code(status.as_u16())
                .map_err(|()| EdgeError::bad_gateway("invalid test response status"))?;
            let deadline = start
                .checked_add(Duration::from_secs(1))
                .ok_or_else(|| EdgeError::bad_gateway("test deadline overflow"))?;
            let result = process_response(
                native,
                Method::GET,
                OutboundProxyMarker::Inherit,
                ResponseDecoding::Decode,
                mode,
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
                native_time(MonotonicClock::new(move || start)),
            )
            .await;
            let checked = result
                .and_then(|response| check_response(response, values, expected, retained_count));
            drop((writer, response_done));
            checked
        }

        fn check_response(
            mut response: OutboundResponse,
            values: &[&[u8]],
            expected: ContentTypeProvenance,
            retained_count: usize,
        ) -> Result<(), EdgeError> {
            use ContentTypeIneligibility::Modified;
            use ContentTypeProvenance::{Ineligible, SingleTextUnchanged};

            if response.content_type_provenance() != expected
                || response.headers().get_all(CONTENT_TYPE).iter().count() != retained_count
            {
                return Err(EdgeError::bad_gateway(
                    "converted test response lost original content-type provenance",
                ));
            }
            if expected == SingleTextUnchanged
                && response
                    .headers()
                    .get(CONTENT_TYPE)
                    .map(HeaderValue::as_bytes)
                    != values.first().copied()
            {
                return Err(EdgeError::bad_gateway(
                    "converted test response changed content-type bytes",
                ));
            }
            response
                .headers_mut()
                .insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
            let after_mutation = if expected == SingleTextUnchanged {
                Ineligible(Modified)
            } else {
                expected
            };
            if response.content_type_provenance() != after_mutation {
                return Err(EdgeError::bad_gateway(
                    "test response mutation upgraded original provenance",
                ));
            }
            Ok(())
        }

        pub(super) async fn check_converted_response_content_type_provenance()
        -> Result<(), EdgeError> {
            if !matches!(
                Fields::new().append(CONNECTION.as_str(), b"content-type"),
                Err(HeaderError::Forbidden)
            ) {
                return Err(EdgeError::bad_gateway(
                    "test host must forbid connection fields",
                ));
            }
            for &(values, expected, retained_count) in content_type_cases() {
                for (status, mode) in [
                    (StatusCode::OK, ResponseMode::Streamed),
                    (StatusCode::NO_CONTENT, ResponseMode::Streamed),
                    (
                        StatusCode::NO_CONTENT,
                        ResponseMode::Buffered { max_bytes: 64 },
                    ),
                    (StatusCode::RESET_CONTENT, ResponseMode::Streamed),
                    (
                        StatusCode::RESET_CONTENT,
                        ResponseMode::Buffered { max_bytes: 64 },
                    ),
                ] {
                    convert_response(values, status, mode, expected, retained_count).await?;
                }
            }
            Ok(())
        }
    }
}

#[cfg(all(feature = "spin", target_arch = "wasm32"))]
pub use spin_impl::SpinOutboundClient;
#[cfg(all(feature = "spin", target_arch = "wasm32"))]
pub(crate) use spin_impl::native_time;
#[cfg(all(feature = "spin", target_arch = "wasm32", feature = "test-utils"))]
pub use spin_impl::{
    canonical_scopes_hold_for_test, connect_timeout_fallback_holds_for_test,
    converted_response_content_type_provenance_holds_for_test,
    converted_response_retains_injected_clock_for_test, deferred_clock_paths_hold_for_test,
    eager_outbound_paths_hold_for_test, map_spin_preparation_header_for_test,
    provider_deadline_diagnostics_hold_for_test, request_fields_for_test,
    request_target_errors_for_test, request_timeout_settings_for_test, set_timeout_option_for_test,
};

#[cfg(all(feature = "spin", any(feature = "test-utils", target_arch = "wasm32")))]
fn map_spin_dispatch_error(
    error: &ErrorCode,
    deadline: Deadline,
    cause: BudgetSource,
    observed_at: edgezero_core::MonotonicInstant,
    phase: ProviderFailurePhase,
    connect_timeout: Option<Duration>,
) -> EdgeError {
    if connect_timeout.is_some()
        && phase == ProviderFailurePhase::Send
        && matches!(error, ErrorCode::ConnectionTimeout)
        && !deadline.is_expired_at(observed_at)
    {
        return with_spin_detail(timeout_error(BudgetSource::ConnectTimeout), error, phase);
    }
    map_spin_host_error(error, deadline, cause, observed_at, phase)
}

#[cfg(all(feature = "spin", any(feature = "test-utils", target_arch = "wasm32")))]
fn map_spin_host_error(
    error: &ErrorCode,
    deadline: Deadline,
    cause: BudgetSource,
    observed_at: edgezero_core::MonotonicInstant,
    phase: ProviderFailurePhase,
) -> EdgeError {
    if deadline.is_expired_at(observed_at) {
        return with_spin_detail(timeout_error(cause), error, phase);
    }

    let classified = match error {
        ErrorCode::DnsTimeout
        | ErrorCode::ConnectionTimeout
        | ErrorCode::ConnectionReadTimeout
        | ErrorCode::ConnectionWriteTimeout
        | ErrorCode::HttpResponseTimeout => timeout_error(BudgetSource::Unspecified),
        ErrorCode::HttpRequestDenied
        | ErrorCode::HttpRequestBodySize(_)
        | ErrorCode::HttpRequestUriTooLong
        | ErrorCode::HttpRequestHeaderSectionSize(_)
        | ErrorCode::HttpRequestHeaderSize(_) => {
            EdgeError::bad_request("outbound request was rejected by the Spin HTTP host")
        }
        ErrorCode::HttpRequestLengthRequired
        | ErrorCode::HttpRequestMethodInvalid
        | ErrorCode::HttpRequestUriInvalid
        | ErrorCode::HttpRequestTrailerSectionSize(_)
        | ErrorCode::HttpRequestTrailerSize(_)
        | ErrorCode::ConfigurationError => EdgeError::internal(anyhow::anyhow!(
            "Spin rejected an adapter-owned outbound request invariant"
        )),
        ErrorCode::DnsError(_)
        | ErrorCode::DestinationNotFound
        | ErrorCode::DestinationUnavailable
        | ErrorCode::DestinationIpProhibited
        | ErrorCode::DestinationIpUnroutable
        | ErrorCode::ConnectionRefused
        | ErrorCode::ConnectionLimitReached
        | ErrorCode::TlsProtocolError
        | ErrorCode::TlsCertificateError
        | ErrorCode::TlsAlertReceived(_) => EdgeError::bad_gateway_with_reason(
            "outbound destination could not be reached",
            BadGatewayReason::Unreachable,
        ),
        ErrorCode::ConnectionTerminated => EdgeError::bad_gateway_with_reason(
            "outbound connection failed after dispatch",
            BadGatewayReason::Transport,
        ),
        ErrorCode::HttpResponseIncomplete
        | ErrorCode::HttpResponseHeaderSectionSize(_)
        | ErrorCode::HttpResponseHeaderSize(_)
        | ErrorCode::HttpResponseBodySize(_)
        | ErrorCode::HttpResponseTrailerSectionSize(_)
        | ErrorCode::HttpResponseTrailerSize(_)
        | ErrorCode::HttpResponseTransferCoding(_)
        | ErrorCode::HttpResponseContentCoding(_)
        | ErrorCode::HttpUpgradeFailed
        | ErrorCode::HttpProtocolError
        | ErrorCode::LoopDetected => EdgeError::bad_gateway_with_reason(
            "upstream response violated the HTTP protocol",
            BadGatewayReason::Protocol,
        ),
        ErrorCode::InternalError(_) => EdgeError::bad_gateway_with_reason(
            "Spin outbound HTTP host failed",
            BadGatewayReason::Unspecified,
        ),
    };
    with_spin_detail(classified, error, phase)
}

#[cfg(all(feature = "spin", any(feature = "test-utils", target_arch = "wasm32")))]
fn with_spin_detail(
    mut error: EdgeError,
    source: &ErrorCode,
    phase: ProviderFailurePhase,
) -> EdgeError {
    if let EdgeError::BadGateway {
        provider_detail, ..
    }
    | EdgeError::BadRequest {
        provider_detail, ..
    }
    | EdgeError::GatewayTimeout {
        provider_detail, ..
    }
    | EdgeError::Internal {
        provider_detail, ..
    } = &mut error
    {
        *provider_detail = Some(ProviderFailureDetail::Spin {
            phase,
            cause: spin_failure_cause(source),
        });
    }
    error
}

#[cfg(all(feature = "spin", any(feature = "test-utils", target_arch = "wasm32")))]
fn spin_failure_cause(error: &ErrorCode) -> SpinFailureCause {
    let field = |payload: &FieldSizePayload| SpinFieldSize {
        field_size: payload.field_size,
    };
    match error {
        ErrorCode::DnsTimeout => SpinFailureCause::DnsTimeout,
        ErrorCode::DnsError(payload) => SpinFailureCause::DnsError {
            rcode: payload
                .rcode
                .as_deref()
                .map(SpinDnsRcode::from_provider_text),
            info_code: payload.info_code,
        },
        ErrorCode::DestinationNotFound => SpinFailureCause::DestinationNotFound,
        ErrorCode::DestinationUnavailable => SpinFailureCause::DestinationUnavailable,
        ErrorCode::DestinationIpProhibited => SpinFailureCause::DestinationIpProhibited,
        ErrorCode::DestinationIpUnroutable => SpinFailureCause::DestinationIpUnroutable,
        ErrorCode::ConnectionRefused => SpinFailureCause::ConnectionRefused,
        ErrorCode::ConnectionTerminated => SpinFailureCause::ConnectionTerminated,
        ErrorCode::ConnectionTimeout => SpinFailureCause::ConnectionTimeout,
        ErrorCode::ConnectionReadTimeout => SpinFailureCause::ConnectionReadTimeout,
        ErrorCode::ConnectionWriteTimeout => SpinFailureCause::ConnectionWriteTimeout,
        ErrorCode::ConnectionLimitReached => SpinFailureCause::ConnectionLimitReached,
        ErrorCode::TlsProtocolError => SpinFailureCause::TlsProtocolError,
        ErrorCode::TlsCertificateError => SpinFailureCause::TlsCertificateError,
        ErrorCode::TlsAlertReceived(payload) => SpinFailureCause::TlsAlertReceived {
            alert_id: payload.alert_id,
        },
        ErrorCode::HttpRequestDenied => SpinFailureCause::HttpRequestDenied,
        ErrorCode::HttpRequestLengthRequired => SpinFailureCause::HttpRequestLengthRequired,
        ErrorCode::HttpRequestBodySize(size) => SpinFailureCause::HttpRequestBodySize(*size),
        ErrorCode::HttpRequestMethodInvalid => SpinFailureCause::HttpRequestMethodInvalid,
        ErrorCode::HttpRequestUriInvalid => SpinFailureCause::HttpRequestUriInvalid,
        ErrorCode::HttpRequestUriTooLong => SpinFailureCause::HttpRequestUriTooLong,
        ErrorCode::HttpRequestHeaderSectionSize(size) => {
            SpinFailureCause::HttpRequestHeaderSectionSize(*size)
        }
        ErrorCode::HttpRequestHeaderSize(payload) => {
            SpinFailureCause::HttpRequestHeaderSize(payload.as_ref().map(field))
        }
        ErrorCode::HttpRequestTrailerSectionSize(size) => {
            SpinFailureCause::HttpRequestTrailerSectionSize(*size)
        }
        ErrorCode::HttpRequestTrailerSize(payload) => {
            SpinFailureCause::HttpRequestTrailerSize(field(payload))
        }
        ErrorCode::HttpResponseIncomplete => SpinFailureCause::HttpResponseIncomplete,
        ErrorCode::HttpResponseHeaderSectionSize(size) => {
            SpinFailureCause::HttpResponseHeaderSectionSize(*size)
        }
        ErrorCode::HttpResponseHeaderSize(payload) => {
            SpinFailureCause::HttpResponseHeaderSize(field(payload))
        }
        ErrorCode::HttpResponseBodySize(size) => SpinFailureCause::HttpResponseBodySize(*size),
        ErrorCode::HttpResponseTrailerSectionSize(size) => {
            SpinFailureCause::HttpResponseTrailerSectionSize(*size)
        }
        ErrorCode::HttpResponseTrailerSize(payload) => {
            SpinFailureCause::HttpResponseTrailerSize(field(payload))
        }
        ErrorCode::HttpResponseTransferCoding(_) => SpinFailureCause::HttpResponseTransferCoding,
        ErrorCode::HttpResponseContentCoding(_) => SpinFailureCause::HttpResponseContentCoding,
        ErrorCode::HttpResponseTimeout => SpinFailureCause::HttpResponseTimeout,
        ErrorCode::HttpUpgradeFailed => SpinFailureCause::HttpUpgradeFailed,
        ErrorCode::HttpProtocolError => SpinFailureCause::HttpProtocolError,
        ErrorCode::LoopDetected => SpinFailureCause::LoopDetected,
        ErrorCode::ConfigurationError => SpinFailureCause::ConfigurationError,
        ErrorCode::InternalError(_) => SpinFailureCause::InternalError,
    }
}

#[cfg(any(
    test,
    all(feature = "spin", any(feature = "test-utils", target_arch = "wasm32"))
))]
fn timeout_error(cause: edgezero_core::BudgetSource) -> EdgeError {
    EdgeError::gateway_timeout_caused("outbound request deadline expired", cause)
}

#[cfg(feature = "test-utils")]
fn validate_batch_request(request: &OutboundRequest) -> Result<(), EdgeError> {
    validate_request(request, OutboundSendMode::Batch)
}

#[cfg(any(feature = "test-utils", all(feature = "spin", target_arch = "wasm32")))]
fn validate_request(request: &OutboundRequest, mode: OutboundSendMode) -> Result<(), EdgeError> {
    if request.image_optimizer_options().is_some() {
        return Err(EdgeError::bad_request(
            "image_optimizer is not supported by spin",
        ));
    }
    if request.has_host_authority_override() {
        return Err(EdgeError::bad_request(
            "Spin cannot separate the outbound Host authority from the connection target",
        ));
    }
    validate_for_dispatch(request, mode)
}

/// Runs the target-neutral Spin batch preflight contract in native tests.
///
/// # Errors
/// Returns the same portable validation or batch-shape error as production dispatch.
#[cfg(feature = "test-utils")]
#[inline]
pub fn validate_batch_request_for_test(request: &OutboundRequest) -> Result<(), EdgeError> {
    validate_batch_request(request)
}

/// Runs the target-neutral Spin request policy before any provider dispatch.
///
/// # Errors
/// Returns the same portable validation or unsupported-authority error as production dispatch.
#[cfg(feature = "test-utils")]
#[inline]
pub fn validate_request_for_test(request: &OutboundRequest) -> Result<(), EdgeError> {
    validate_request(request, OutboundSendMode::Send)
}

/// Exposes the real pinned SDK classifier to WASI resource contract tests.
#[cfg(all(feature = "spin", feature = "test-utils"))]
#[must_use]
#[inline]
pub fn map_spin_host_error_for_test(
    error: &ErrorCode,
    deadline: Deadline,
    cause: BudgetSource,
    observed_at: edgezero_core::MonotonicInstant,
    phase: ProviderFailurePhase,
) -> EdgeError {
    map_spin_host_error(error, deadline, cause, observed_at, phase)
}

/// Exercises the dispatch-boundary classifier with the retained per-request connect setting.
#[cfg(all(feature = "spin", feature = "test-utils"))]
#[must_use]
#[inline]
pub fn map_spin_dispatch_error_for_test(
    error: &ErrorCode,
    deadline: Deadline,
    cause: BudgetSource,
    observed_at: edgezero_core::MonotonicInstant,
    phase: ProviderFailurePhase,
    connect_timeout: Option<Duration>,
) -> EdgeError {
    map_spin_dispatch_error(error, deadline, cause, observed_at, phase, connect_timeout)
}

#[cfg(test)]
mod exchange_tests {
    use std::cell::Cell;
    use std::collections::VecDeque;
    use std::future::Future;
    use std::pin::Pin;
    use std::rc::Rc;
    use std::sync::{Arc, Mutex};
    use std::task::{Context, Poll, Waker};
    use std::time::Duration;

    use edgezero_core::{
        BudgetSource, Deadline, DispatchBudget, MonotonicClock, MonotonicInstant, OutboundTime,
    };
    use futures::executor::block_on;
    use futures_util::future::poll_fn;
    use futures_util::task::noop_waker_ref;

    use super::{
        EdgeError, READY_ITEM_YIELD_QUOTA, UploadCompletion, cooperate_after_response_read,
        cooperative_stream, cooperative_yield_once, deadline_stream, duration_nanos, race_deadline,
        request_timeouts, run_exchange, settle_reset_content, should_emit_response_chunk,
    };

    struct ScriptedFuture<Output> {
        drops: Rc<Cell<u32>>,
        polls: Rc<Cell<u32>>,
        steps: VecDeque<Poll<Output>>,
    }

    impl<Output> Unpin for ScriptedFuture<Output> {}

    impl<Output> ScriptedFuture<Output> {
        fn new(
            steps: impl IntoIterator<Item = Poll<Output>>,
            polls: Rc<Cell<u32>>,
            drops: Rc<Cell<u32>>,
        ) -> Self {
            Self {
                drops,
                polls,
                steps: steps.into_iter().collect(),
            }
        }
    }

    impl<Output> Future for ScriptedFuture<Output> {
        type Output = Output;

        fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            self.polls.set(self.polls.get().saturating_add(1));
            let step = self.steps.pop_front().expect("scripted future exhausted");
            if step.is_pending() {
                cx.waker().wake_by_ref();
            }
            step
        }
    }

    #[expect(
        clippy::missing_trait_methods,
        reason = "`Drop::pin_drop` is unstable (`pin_ergonomics`) and conflicts with `drop`"
    )]
    impl<Output> Drop for ScriptedFuture<Output> {
        fn drop(&mut self) {
            self.drops.set(self.drops.get().saturating_add(1));
        }
    }

    fn counters() -> (Rc<Cell<u32>>, Rc<Cell<u32>>) {
        (Rc::new(Cell::new(0)), Rc::new(Cell::new(0)))
    }

    fn map_host_error(
        _error: &&'static str,
        _phase: edgezero_core::ProviderFailurePhase,
    ) -> EdgeError {
        EdgeError::bad_gateway("scripted host failure")
    }

    struct TimerState {
        creations: usize,
        drops: usize,
        now: MonotonicInstant,
        waiter: Option<Waker>,
    }

    struct TimerRegistration(Arc<Mutex<TimerState>>);

    #[expect(clippy::missing_trait_methods, reason = "Drop::pin_drop is unstable")]
    impl Drop for TimerRegistration {
        fn drop(&mut self) {
            let mut state = self.0.lock().expect("timer registration");
            state.drops = state.drops.saturating_add(1);
            state.waiter = None;
        }
    }

    fn paired_time() -> (OutboundTime, Arc<Mutex<TimerState>>, DispatchBudget) {
        let start = MonotonicInstant::now()
            .checked_add(Duration::from_hours(1))
            .expect("synthetic clock");
        let state = Arc::new(Mutex::new(TimerState {
            creations: 0,
            drops: 0,
            now: start,
            waiter: None,
        }));
        let observed = Arc::clone(&state);
        let clock = MonotonicClock::new(move || observed.lock().expect("clock").now);
        let shared = Arc::clone(&state);
        let time = OutboundTime::new(clock, move |deadline, factory_clock| {
            assert_eq!(
                factory_clock.now(),
                shared.lock().expect("paired clock").now
            );
            let registration = TimerRegistration(Arc::clone(&shared));
            let mut factory_state = shared.lock().expect("timer factory");
            factory_state.creations = factory_state.creations.saturating_add(1);
            drop(factory_state);
            Box::pin(poll_fn(move |context| {
                let mut polled = registration.0.lock().expect("timer poll");
                if deadline.is_expired_at(polled.now) {
                    Poll::Ready(Ok(()))
                } else {
                    polled.waiter = Some(context.waker().clone());
                    Poll::Pending
                }
            }))
        });
        let budget = DispatchBudget {
            cause: BudgetSource::PerCallTimeout,
            deadline: Deadline::at_instant(
                start
                    .checked_add(Duration::from_millis(10))
                    .expect("deadline"),
            ),
            duration: Duration::from_millis(10),
        };
        (time, state, budget)
    }

    #[test]
    fn paired_timer_failure_precedes_ready_source_and_releases_it() {
        let clock = MonotonicClock::default();
        let budget = DispatchBudget {
            cause: BudgetSource::PerCallTimeout,
            deadline: clock.deadline_after(Duration::from_secs(1)),
            duration: Duration::from_secs(1),
        };
        let time = OutboundTime::new(clock, |_, _| {
            Box::pin(async { Err(EdgeError::bad_gateway("paired timer failed")) })
        });
        let (polls, drops) = counters();
        let source = ScriptedFuture::new([Poll::Ready(7_u8)], Rc::clone(&polls), Rc::clone(&drops));
        let mut previous = time.clock().now();
        let mut guarded = Box::pin(race_deadline(source, budget, &time, &mut previous, |_| {
            None
        }));
        let error = block_on(&mut guarded).expect_err("timer setup must win");
        assert_eq!(error.message(), "paired timer failed");
        assert_eq!(polls.get(), 0);
        assert_eq!(drops.get(), 1, "release before completed future is dropped");
    }

    #[test]
    fn paired_expiry_wakes_and_releases_owned_pending_source_at_equality() {
        for cause in [
            BudgetSource::PerCallTimeout,
            BudgetSource::RequestDeadline,
            BudgetSource::BatchCutoff,
            BudgetSource::Default,
        ] {
            let (time, state, mut budget) = paired_time();
            budget.cause = cause;
            let (polls, drops) = counters();
            let source =
                ScriptedFuture::<()>::new([Poll::Pending], Rc::clone(&polls), Rc::clone(&drops));
            let mut previous = time.clock().now();
            let mut guarded = Box::pin(race_deadline(source, budget, &time, &mut previous, |()| {
                None
            }));
            let mut context = Context::from_waker(noop_waker_ref());
            assert!(guarded.as_mut().poll(&mut context).is_pending());
            let wake = {
                let mut advanced = state.lock().expect("advance clock");
                assert_eq!(advanced.creations, 1);
                advanced.now = budget.deadline.instant();
                advanced.waiter.take().expect("registered timer waker")
            };
            wake.wake();
            assert!(
                matches!(guarded.as_mut().poll(&mut context), Poll::Ready(Err(EdgeError::GatewayTimeout { cause: actual, .. })) if actual == cause)
            );
            assert_eq!(polls.get(), 1, "timer must win before repolling source");
            assert_eq!(drops.get(), 1);
            let released = state.lock().expect("released timer");
            assert_eq!(released.drops, 1);
            assert!(released.waiter.is_none());
        }
    }

    #[test]
    fn dropping_paired_guard_cancels_pending_timer_and_source() {
        let (time, state, budget) = paired_time();
        let (polls, drops) = counters();
        let source = ScriptedFuture::<()>::new([Poll::Pending], polls, Rc::clone(&drops));
        let mut previous = time.clock().now();
        let mut guarded = Box::pin(race_deadline(source, budget, &time, &mut previous, |()| {
            None
        }));
        assert!(
            guarded
                .as_mut()
                .poll(&mut Context::from_waker(noop_waker_ref()))
                .is_pending()
        );
        drop(guarded);
        assert_eq!(drops.get(), 1);
        let released = state.lock().expect("released timer");
        assert_eq!(released.drops, 1);
        assert!(released.waiter.is_none());
    }

    #[test]
    fn paired_guard_rechecks_ready_source_for_expiry_and_backwards_time() {
        use edgezero_core::{
            BadGatewayReason, ProviderFailureDetail, ProviderFailurePhase, SpinFailureCause,
        };

        for backwards in [false, true] {
            let (time, state, budget) = paired_time();
            let detail = ProviderFailureDetail::Spin {
                phase: ProviderFailurePhase::ResponseBodyRead,
                cause: SpinFailureCause::HttpResponseIncomplete,
            };
            let observed = Arc::clone(&state);
            let source = async move {
                let mut source_clock = observed.lock().expect("source clock");
                source_clock.now = if backwards {
                    source_clock
                        .now
                        .checked_sub(Duration::from_nanos(1))
                        .expect("earlier time")
                } else {
                    budget.deadline.instant()
                };
                Err::<(), _>(EdgeError::bad_gateway_with_detail(
                    "truncated response",
                    BadGatewayReason::Protocol,
                    detail,
                ))
            };
            let mut previous = time.clock().now();
            let error = block_on(race_deadline(
                source,
                budget,
                &time,
                &mut previous,
                |result| {
                    result
                        .as_ref()
                        .err()
                        .and_then(EdgeError::provider_failure_detail)
                        .copied()
                },
            ))
            .expect_err("post-ready clock check");
            if backwards {
                assert!(matches!(error, EdgeError::Internal { .. }));
            } else {
                assert!(matches!(
                    error,
                    EdgeError::GatewayTimeout {
                        cause: BudgetSource::PerCallTimeout,
                        ..
                    }
                ));
                assert_eq!(error.provider_failure_detail(), Some(&detail));
            }
            assert_eq!(state.lock().expect("released timer").drops, 1);
        }
    }

    #[test]
    fn already_expired_pair_drops_source_without_registering_timer() {
        let (time, state, budget) = paired_time();
        state.lock().expect("expired clock").now = budget.deadline.instant();
        let (polls, drops) = counters();
        let source = ScriptedFuture::new([Poll::Ready(())], Rc::clone(&polls), Rc::clone(&drops));
        let mut previous = time.clock().now();
        assert!(matches!(
            block_on(race_deadline(source, budget, &time, &mut previous, |()| {
                None
            })),
            Err(EdgeError::GatewayTimeout { .. })
        ));
        assert_eq!(polls.get(), 0);
        assert_eq!(drops.get(), 1);
        assert_eq!(state.lock().expect("no timer").creations, 0);
    }

    #[test]
    fn review_cross_chunk_regression_is_rejected_between_races() {
        let (time, state, budget) = paired_time();
        let start = state.lock().unwrap().now;
        let mut previous = start;
        let advanced = start.checked_add(Duration::from_millis(2)).unwrap();
        let regressed = start.checked_add(Duration::from_millis(1)).unwrap();
        let first_clock = Arc::clone(&state);
        let first = async move {
            first_clock.lock().unwrap().now = advanced;
            1_u8
        };
        assert_eq!(
            block_on(race_deadline(first, budget, &time, &mut previous, |_| None)).unwrap(),
            1
        );
        state.lock().unwrap().now = regressed;
        let error = block_on(race_deadline(
            async { 2_u8 },
            budget,
            &time,
            &mut previous,
            |_| None,
        ))
        .expect_err("cross-chunk clock regression");
        assert!(matches!(error, EdgeError::Internal { .. }));
    }

    #[test]
    fn review_cross_chunk_regression_releases_retained_source() {
        use std::future::pending;

        use bytes::Bytes;
        use futures_util::StreamExt as _;

        let (time, state, budget) = paired_time();
        let start = state.lock().unwrap().now;
        let advanced = start.checked_add(Duration::from_millis(2)).unwrap();
        let regressed = start.checked_add(Duration::from_millis(1)).unwrap();
        let owner = Arc::new(());
        let released = Arc::downgrade(&owner);
        let source_clock = Arc::clone(&state);
        let source = async_stream::stream! {
            source_clock.lock().unwrap().now = advanced;
            yield Ok(Bytes::from_static(b"first"));
            yield Ok(Bytes::from_static(b"second"));
            pending::<()>().await;
            drop(owner);
        }
        .boxed_local();
        let mut guarded = deadline_stream(source, budget, time);
        assert_eq!(
            block_on(guarded.next()).unwrap().unwrap(),
            Bytes::from_static(b"first")
        );
        state.lock().unwrap().now = regressed;
        let error = block_on(guarded.next())
            .unwrap()
            .expect_err("cross-chunk clock regression");
        assert!(matches!(error, EdgeError::Internal { .. }));
        assert!(
            released.upgrade().is_none(),
            "release before terminal wrapper drop"
        );
    }

    #[test]
    fn provider_diagnostics_distinguish_send_and_request_completion_phases() {
        for send_fails in [false, true] {
            let (send_polls, send_drops) = counters();
            let (pump_polls, pump_drops) = counters();
            let (done_polls, done_drops) = counters();
            let send = ScriptedFuture::new(
                [Poll::Ready(Err::<u8, _>("host failed"))],
                send_polls,
                send_drops,
            );
            let pump = ScriptedFuture::new(
                [if send_fails {
                    Poll::Pending
                } else {
                    Poll::Ready(Ok(UploadCompletion::Complete))
                }],
                pump_polls,
                pump_drops,
            );
            let done =
                ScriptedFuture::new([Poll::Ready(Err("host failed"))], done_polls, done_drops);
            let expected = if send_fails {
                edgezero_core::ProviderFailurePhase::Send
            } else {
                edgezero_core::ProviderFailurePhase::RequestBodyUpload
            };
            let outcome = block_on(run_exchange(send, pump, done, |_error, phase| {
                assert_eq!(phase, expected);
                EdgeError::bad_gateway("host failed")
            }));
            assert_eq!(outcome.expect_err("host error").message(), "host failed");
        }
    }

    #[test]
    fn continuously_ready_chunks_yield_after_each_item() {
        use futures_util::StreamExt as _;
        use futures_util::stream;
        use futures_util::task::noop_waker;

        let processed = Rc::new(Cell::new(0_u32));
        let observed = Rc::clone(&processed);
        let mut source = stream::iter([(), (), ()]);
        let mut pump = Box::pin(async move {
            while source.next().await.is_some() {
                observed.set(observed.get().saturating_add(1));
                cooperative_yield_once().await;
            }
        });
        let waker = noop_waker();
        let mut context = Context::from_waker(&waker);

        assert!(pump.as_mut().poll(&mut context).is_pending());
        assert_eq!(processed.get(), 1);
        assert!(pump.as_mut().poll(&mut context).is_pending());
        assert_eq!(processed.get(), 2);
        assert!(pump.as_mut().poll(&mut context).is_pending());
        assert_eq!(processed.get(), 3);
        assert!(pump.as_mut().poll(&mut context).is_ready());
    }

    #[test]
    fn request_timeouts_use_full_remaining_budget_for_every_phase() {
        let timeouts = request_timeouts(Duration::from_millis(40), None);
        assert_eq!(timeouts.connect(), 40_000_000);
        assert_eq!(timeouts.first_byte(), 40_000_000);
        assert_eq!(timeouts.between_bytes(), 40_000_000);
    }

    #[test]
    fn configured_connect_timeout_is_independent_of_response_timeouts() {
        for (configured, expected) in [(7, 7_000_000), (80, 40_000_000)] {
            let timeouts = request_timeouts(
                Duration::from_millis(40),
                Some(Duration::from_millis(configured)),
            );
            assert_eq!(timeouts.connect(), expected);
            assert_eq!(timeouts.first_byte(), 40_000_000);
            assert_eq!(timeouts.between_bytes(), 40_000_000);
        }
    }

    #[test]
    fn duration_nanos_floors_and_saturates_without_wrapping() {
        assert_eq!(duration_nanos(Duration::ZERO), 1);
        assert_eq!(duration_nanos(Duration::from_nanos(1)), 1);
        assert_eq!(duration_nanos(Duration::from_nanos(999)), 999);
        assert_eq!(duration_nanos(Duration::MAX), u64::MAX);
    }

    #[test]
    fn cooperative_stream_stops_after_the_first_error() {
        use bytes::Bytes;
        use futures_util::StreamExt as _;
        use futures_util::stream;

        let source = stream::iter([
            Err(EdgeError::bad_gateway("terminal")),
            Ok(Bytes::from_static(b"must not escape")),
        ])
        .boxed_local();
        let mut cooperative = cooperative_stream(source);

        assert!(matches!(
            block_on(cooperative.next()),
            Some(Err(EdgeError::BadGateway { .. }))
        ));
        assert!(block_on(cooperative.next()).is_none());
    }

    #[test]
    fn zero_length_complete_does_not_emit_an_empty_response_chunk() {
        assert!(!should_emit_response_chunk(0));
        assert!(should_emit_response_chunk(1));
    }

    #[test]
    fn reset_content_settlement_is_bounded_and_releases_source() {
        use bytes::Bytes;
        use futures_util::StreamExt as _;
        use futures_util::stream::poll_fn;

        struct DropSignal(Rc<Cell<u32>>);

        #[expect(
            clippy::missing_trait_methods,
            reason = "`Drop::pin_drop` is unstable (`pin_ergonomics`) and conflicts with `drop`"
        )]
        impl Drop for DropSignal {
            fn drop(&mut self) {
                self.0.set(self.0.get().saturating_add(1));
            }
        }

        let polls = Rc::new(Cell::new(0_u32));
        let drops = Rc::new(Cell::new(0_u32));
        let observed_polls = Rc::clone(&polls);
        let signal = DropSignal(Rc::clone(&drops));
        let source = poll_fn(move |_context| {
            let _keep_signal_alive = &signal;
            observed_polls.set(observed_polls.get().saturating_add(1));
            Poll::Ready(Some(Ok(Bytes::from_static(b"illegal"))))
        })
        .boxed_local();

        block_on(settle_reset_content(source, false)).expect("205 settlement");

        assert_eq!(polls.get(), 1);
        assert_eq!(drops.get(), 1);

        let declared_polls = Rc::new(Cell::new(0_u32));
        let declared_drops = Rc::new(Cell::new(0_u32));
        let observed_declared_polls = Rc::clone(&declared_polls);
        let declared_signal = DropSignal(Rc::clone(&declared_drops));
        let declared_source = poll_fn(move |_context| {
            let _keep_signal_alive = &declared_signal;
            observed_declared_polls.set(observed_declared_polls.get().saturating_add(1));
            Poll::Pending::<Option<Result<Bytes, EdgeError>>>
        })
        .boxed_local();

        block_on(settle_reset_content(declared_source, true)).expect("declared 205 settlement");

        assert_eq!(declared_polls.get(), 0);
        assert_eq!(declared_drops.get(), 1);

        let error_drops = Rc::new(Cell::new(0_u32));
        let error_signal = DropSignal(Rc::clone(&error_drops));
        let error_source = poll_fn(move |_context| {
            let _keep_signal_alive = &error_signal;
            Poll::Ready(Some(Err(EdgeError::bad_gateway("205 source failed"))))
        })
        .boxed_local();

        block_on(settle_reset_content(error_source, false)).expect_err("205 source error");

        assert_eq!(error_drops.get(), 1);
    }

    #[test]
    fn zero_length_response_reads_force_a_pending_boundary_at_the_quota() {
        use futures_util::task::noop_waker;

        let processed = Rc::new(Cell::new(0_usize));
        let observed = Rc::clone(&processed);
        let mut task = Box::pin(async move {
            let mut ready_reads = 0_usize;
            for _ in 0..READY_ITEM_YIELD_QUOTA.saturating_add(1) {
                observed.set(observed.get().saturating_add(1));
                cooperate_after_response_read(&mut ready_reads).await;
            }
        });
        let waker = noop_waker();
        let mut context = Context::from_waker(&waker);

        assert!(task.as_mut().poll(&mut context).is_pending());
        assert_eq!(processed.get(), READY_ITEM_YIELD_QUOTA);
        assert!(task.as_mut().poll(&mut context).is_ready());
        assert_eq!(processed.get(), READY_ITEM_YIELD_QUOTA.saturating_add(1));
    }

    #[test]
    fn cooperative_stream_forces_a_pending_boundary_at_its_quota() {
        use std::iter::repeat_with;

        use bytes::Bytes;
        use futures_util::StreamExt as _;
        use futures_util::stream;
        use futures_util::task::noop_waker;

        let mut source = cooperative_stream(
            stream::iter(
                repeat_with(|| Ok::<Bytes, EdgeError>(Bytes::new()))
                    .take(READY_ITEM_YIELD_QUOTA.saturating_add(1)),
            )
            .boxed_local(),
        );
        let waker = noop_waker();
        let mut context = Context::from_waker(&waker);

        for _ in 0..READY_ITEM_YIELD_QUOTA {
            assert!(matches!(
                source.as_mut().poll_next(&mut context),
                Poll::Ready(Some(Ok(_)))
            ));
        }
        assert!(source.as_mut().poll_next(&mut context).is_pending());
        assert!(matches!(
            source.as_mut().poll_next(&mut context),
            Poll::Ready(Some(Ok(_)))
        ));
    }

    #[test]
    fn upload_failure_wins_over_simultaneously_ready_send() {
        let (send_polls, send_drops) = counters();
        let (pump_polls, pump_drops) = counters();
        let (done_polls, done_drops) = counters();
        let send = ScriptedFuture::new(
            [Poll::Ready(Ok(7_u8))],
            Rc::clone(&send_polls),
            Rc::clone(&send_drops),
        );
        let pump = ScriptedFuture::new(
            [Poll::Ready(Err(EdgeError::payload_too_large(
                edgezero_core::RequestBodyOverflowOrigin::OutboundUpload,
            )))],
            Rc::clone(&pump_polls),
            Rc::clone(&pump_drops),
        );
        let done = ScriptedFuture::new(
            [Poll::Ready(Ok(()))],
            Rc::clone(&done_polls),
            Rc::clone(&done_drops),
        );

        let outcome = block_on(run_exchange(send, pump, done, map_host_error));

        assert!(matches!(
            outcome,
            Err(EdgeError::PayloadTooLarge {
                origin: edgezero_core::RequestBodyOverflowOrigin::OutboundUpload
            })
        ));
        assert_eq!(pump_polls.get(), 1);
        assert_eq!(send_polls.get(), 0);
        assert_eq!(done_polls.get(), 0);
        assert_eq!(send_drops.get(), 1);
        assert_eq!(pump_drops.get(), 1);
        assert_eq!(done_drops.get(), 1);
    }

    #[test]
    fn ready_send_after_pending_upload_is_authoritative() {
        let (send_polls, send_drops) = counters();
        let (pump_polls, pump_drops) = counters();
        let (done_polls, done_drops) = counters();
        let send = ScriptedFuture::new(
            [Poll::Ready(Ok(7_u8))],
            Rc::clone(&send_polls),
            Rc::clone(&send_drops),
        );
        let pump = ScriptedFuture::new(
            [
                Poll::Pending,
                Poll::Ready(Err(EdgeError::bad_request("late upload failure"))),
            ],
            Rc::clone(&pump_polls),
            Rc::clone(&pump_drops),
        );
        let done = ScriptedFuture::new(
            [Poll::Ready(Ok(()))],
            Rc::clone(&done_polls),
            Rc::clone(&done_drops),
        );

        let outcome = block_on(run_exchange(send, pump, done, map_host_error));

        assert_eq!(outcome.expect("send result"), 7);
        assert_eq!(pump_polls.get(), 1);
        assert_eq!(send_polls.get(), 1);
        assert_eq!(done_polls.get(), 0);
        assert_eq!(send_drops.get(), 1);
        assert_eq!(pump_drops.get(), 1);
        assert_eq!(done_drops.get(), 1);
    }

    #[test]
    fn request_done_error_wins_over_retained_send() {
        let (send_polls, send_drops) = counters();
        let (pump_polls, pump_drops) = counters();
        let (done_polls, done_drops) = counters();
        let send = ScriptedFuture::new(
            [Poll::Ready(Ok(7_u8))],
            Rc::clone(&send_polls),
            Rc::clone(&send_drops),
        );
        let pump = ScriptedFuture::new(
            [Poll::Ready(Ok(UploadCompletion::Complete))],
            pump_polls,
            Rc::clone(&pump_drops),
        );
        let done = ScriptedFuture::new(
            [Poll::Pending, Poll::Ready(Err("request completion failed"))],
            Rc::clone(&done_polls),
            Rc::clone(&done_drops),
        );

        let observed_send_drops = Rc::clone(&send_drops);
        let observed_pump_drops = Rc::clone(&pump_drops);
        let observed_done_drops = Rc::clone(&done_drops);
        let outcome = block_on(run_exchange(send, pump, done, move |_error, phase| {
            assert_eq!(
                phase,
                edgezero_core::ProviderFailurePhase::RequestBodyUpload
            );
            assert_eq!(observed_send_drops.get(), 1);
            assert_eq!(observed_pump_drops.get(), 1);
            assert_eq!(observed_done_drops.get(), 1);
            EdgeError::bad_gateway("scripted host failure")
        }));

        assert!(matches!(outcome, Err(EdgeError::BadGateway { .. })));
        assert_eq!(send_polls.get(), 1);
        assert_eq!(done_polls.get(), 2);
    }

    #[test]
    fn request_done_success_releases_retained_send_without_repolling_it() {
        let (send_polls, send_drops) = counters();
        let (pump_polls, pump_drops) = counters();
        let (done_polls, done_drops) = counters();
        let send = ScriptedFuture::new([Poll::Ready(Ok(7_u8))], Rc::clone(&send_polls), send_drops);
        let pump = ScriptedFuture::new(
            [Poll::Ready(Ok(UploadCompletion::Complete))],
            pump_polls,
            pump_drops,
        );
        let done = ScriptedFuture::new(
            [Poll::Pending, Poll::Ready(Ok(()))],
            Rc::clone(&done_polls),
            done_drops,
        );

        let outcome = block_on(run_exchange(send, pump, done, map_host_error));

        assert_eq!(outcome.expect("retained send result"), 7);
        assert_eq!(send_polls.get(), 1);
        assert_eq!(done_polls.get(), 2);
    }

    #[test]
    fn reader_gone_never_polls_request_done() {
        let (send_polls, send_drops) = counters();
        let (pump_polls, pump_drops) = counters();
        let (done_polls, done_drops) = counters();
        let send = ScriptedFuture::new(
            [Poll::Pending, Poll::Ready(Ok(7_u8))],
            Rc::clone(&send_polls),
            send_drops,
        );
        let pump = ScriptedFuture::new(
            [Poll::Ready(Ok(UploadCompletion::ReaderGone))],
            pump_polls,
            pump_drops,
        );
        let done = ScriptedFuture::new([Poll::Ready(Ok(()))], Rc::clone(&done_polls), done_drops);

        let outcome = block_on(run_exchange(send, pump, done, map_host_error));

        assert_eq!(outcome.expect("early response"), 7);
        assert_eq!(send_polls.get(), 2);
        assert_eq!(done_polls.get(), 0);
    }
}
