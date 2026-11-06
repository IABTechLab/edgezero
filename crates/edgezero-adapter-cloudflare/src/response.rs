#![cfg_attr(
    all(feature = "cloudflare", target_arch = "wasm32"),
    expect(
        clippy::arbitrary_source_item_ordering,
        reason = "the platform module follows runtime, writer, race, and entrypoint lifecycle order"
    )
)]

use std::panic::{AssertUnwindSafe, catch_unwind};
use std::time::Duration;

use bytes::Bytes;
use edgezero_core::body::{Body, BodyPayload, BodyStream};
use edgezero_core::error::EdgeError;
use edgezero_core::http::header::CONTENT_ENCODING;
use edgezero_core::http::{HeaderMap, Method, Response, StatusCode};
use edgezero_core::response_egress::{
    RESPONSE_EGRESS_FALLBACK_SAFETY_BUDGET, ResponseEgressAttempt,
    ResponseEgressFallbackDisposition, ResponseEgressOutcome, ResponseEgressPolicy,
};
use edgezero_core::response_egress_framing::ResponseEgressFramingMode;
use edgezero_core::response_egress_framing::{
    PreparedResponseEgress, response_egress_source_error_category,
};
use edgezero_core::time::{Deadline, MonotonicClock, MonotonicInstant};
use futures_util::StreamExt as _;
use futures_util::future::{FutureExt as _, LocalBoxFuture};
use futures_util::stream;

const INTERNAL_FALLBACK_BODY: &[u8] = b"internal server error";
const TIMEOUT_FALLBACK_BODY: &[u8] = b"response write deadline exceeded";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DeliveryError {
    Cancelled,
    Conversion,
    Deadline,
    Source,
    Transport,
}

impl DeliveryError {
    const fn from_outcome(outcome: ResponseEgressOutcome) -> Self {
        match outcome {
            ResponseEgressOutcome::ConversionError => Self::Conversion,
            ResponseEgressOutcome::DeadlineExceeded => Self::Deadline,
            ResponseEgressOutcome::RequestCancelled => Self::Cancelled,
            ResponseEgressOutcome::SourceError => Self::Source,
            ResponseEgressOutcome::ClientDisconnected
            | ResponseEgressOutcome::Completed
            | ResponseEgressOutcome::HostHandoff
            | ResponseEgressOutcome::TransportError
            | ResponseEgressOutcome::Unspecified
            | _ => Self::Transport,
        }
    }

    const fn outcome(self) -> ResponseEgressOutcome {
        match self {
            Self::Cancelled => ResponseEgressOutcome::RequestCancelled,
            Self::Conversion => ResponseEgressOutcome::ConversionError,
            Self::Deadline => ResponseEgressOutcome::DeadlineExceeded,
            Self::Source => ResponseEgressOutcome::SourceError,
            Self::Transport => ResponseEgressOutcome::TransportError,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum EgressKind {
    Application,
    Fallback,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum IoFailureKind {
    Cancelled,
    Deadline,
    Transport,
}

impl IoFailureKind {
    const fn delivery_error(self) -> DeliveryError {
        match self {
            Self::Cancelled => DeliveryError::Cancelled,
            Self::Deadline => DeliveryError::Deadline,
            Self::Transport => DeliveryError::Transport,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct IoFailure {
    accepted: usize,
    kind: IoFailureKind,
}

enum StartOutcome<PlatformResponse> {
    Precommit {
        attempt: Box<ResponseEgressAttempt>,
        cause: ResponseEgressOutcome,
    },
    Started(PlatformResponse),
}

trait CloudflareEgressCommitter {
    type Io: CloudflareEgressIo + 'static;
    type Response;

    fn prepare(
        &mut self,
        status: StatusCode,
        headers: &HeaderMap,
        declared_length: Option<u64>,
        transmits_body: bool,
    ) -> Result<(Self::Response, Self::Io), DeliveryError>;

    fn spawn(&mut self, task: LocalBoxFuture<'static, ()>);
}

#[async_trait::async_trait(?Send)]
trait CloudflareEgressIo {
    fn abort(&mut self);

    async fn finish(
        &mut self,
        deadline: Deadline,
        clock: &MonotonicClock,
    ) -> Result<(), IoFailureKind>;

    async fn next_body(
        &mut self,
        source: &mut BodyStream,
        deadline: Deadline,
        clock: &MonotonicClock,
        attempt: &mut ResponseEgressAttempt,
    ) -> Result<Option<Result<Bytes, EdgeError>>, IoFailureKind>;

    fn preflight(&self, deadline: Deadline, clock: &MonotonicClock) -> Result<(), IoFailureKind>;

    async fn write(
        &mut self,
        bytes: Vec<u8>,
        deadline: Deadline,
        clock: &MonotonicClock,
    ) -> Result<usize, IoFailure>;
}

async fn transmit_committed<Io>(
    body: Body,
    policy: ResponseEgressPolicy,
    mut attempt: ResponseEgressAttempt,
    clock: MonotonicClock,
    kind: EgressKind,
    mut io: Io,
) -> Result<(), DeliveryError>
where
    Io: CloudflareEgressIo,
{
    let result = pump_body(body, policy.write_deadline, &mut attempt, &clock, &mut io).await;
    match result {
        Ok(()) => {
            settle_success(kind, &mut attempt, clock.now());
            Ok(())
        }
        Err(error) => {
            abort(&mut io);
            let observed_at = terminal_observed_at(error, policy.write_deadline, &clock);
            settle_failure(kind, &mut attempt, error.outcome(), observed_at);
            Err(error)
        }
    }
}

async fn pump_body<Io>(
    body: Body,
    deadline: Deadline,
    attempt: &mut ResponseEgressAttempt,
    clock: &MonotonicClock,
    io: &mut Io,
) -> Result<(), DeliveryError>
where
    Io: CloudflareEgressIo,
{
    ensure_deadline(deadline, clock)?;
    let mut source = body_source(body);
    loop {
        let observed = io.next_body(&mut source, deadline, clock, attempt).await;
        if let Ok(Some(Err(error))) = &observed {
            attempt.record_source_error(error);
        }
        let next = observed.map_err(IoFailureKind::delivery_error)?;
        ensure_deadline(deadline, clock)?;
        let Some(item) = next else {
            break;
        };
        let chunk = item.map_err(|error| {
            log::warn!(
                "response-egress Cloudflare body source failed after commit: {}",
                response_egress_source_error_category(&error)
            );
            DeliveryError::Source
        })?;
        if chunk.is_empty() {
            continue;
        }
        let requested = chunk.len();
        match io.write(chunk.to_vec(), deadline, clock).await {
            Ok(accepted) => {
                account_prefix(accepted, requested, attempt, clock.now())?;
                if accepted != requested {
                    return Err(DeliveryError::Transport);
                }
            }
            Err(failure) => {
                account_prefix(failure.accepted, requested, attempt, clock.now())?;
                return Err(failure.kind.delivery_error());
            }
        }
        ensure_deadline(deadline, clock)?;
    }
    io.finish(deadline, clock)
        .await
        .map_err(IoFailureKind::delivery_error)?;
    ensure_deadline(deadline, clock)
}

fn account_prefix(
    accepted: usize,
    requested: usize,
    attempt: &mut ResponseEgressAttempt,
    observed_at: MonotonicInstant,
) -> Result<(), DeliveryError> {
    if accepted > requested {
        return Err(DeliveryError::Transport);
    }
    let accepted_u64 = u64::try_from(accepted).map_err(|_error| DeliveryError::Transport)?;
    if !attempt.account_bytes(accepted_u64, observed_at) {
        return Err(DeliveryError::Transport);
    }
    Ok(())
}

fn abort<Io>(io: &mut Io)
where
    Io: CloudflareEgressIo,
{
    if catch_unwind(AssertUnwindSafe(|| io.abort())).is_err() {
        log::error!("response-egress Cloudflare abort panicked");
    }
}

fn body_source(body: Body) -> BodyStream {
    let (payload, _scope) = body.into_parts();
    match payload {
        BodyPayload::Once(bytes) => stream::once(async move { Ok(bytes) }).boxed_local(),
        BodyPayload::Stream(source) => source,
    }
}

fn ensure_deadline(deadline: Deadline, clock: &MonotonicClock) -> Result<(), DeliveryError> {
    if deadline.is_expired_at(clock.now()) {
        Err(DeliveryError::Deadline)
    } else {
        Ok(())
    }
}

fn terminal_observed_at(
    error: DeliveryError,
    deadline: Deadline,
    clock: &MonotonicClock,
) -> MonotonicInstant {
    let observed_at = clock.now();
    if error == DeliveryError::Deadline {
        observed_at.max(deadline.instant())
    } else {
        observed_at
    }
}

fn settle_failure(
    kind: EgressKind,
    attempt: &mut ResponseEgressAttempt,
    outcome: ResponseEgressOutcome,
    observed_at: MonotonicInstant,
) {
    match kind {
        EgressKind::Application => {
            attempt.terminate(outcome, observed_at);
        }
        EgressKind::Fallback => {
            attempt.finish_fallback(ResponseEgressFallbackDisposition::Aborted, observed_at);
        }
    }
}

fn settle_success(
    kind: EgressKind,
    attempt: &mut ResponseEgressAttempt,
    observed_at: MonotonicInstant,
) {
    match kind {
        EgressKind::Application => {
            attempt.terminate(ResponseEgressOutcome::HostHandoff, observed_at);
        }
        EgressKind::Fallback => {
            attempt.finish_fallback(ResponseEgressFallbackDisposition::Completed, observed_at);
        }
    }
}

fn start_prepared<Committer>(
    prepared: PreparedResponseEgress,
    policy: ResponseEgressPolicy,
    mut attempt: ResponseEgressAttempt,
    clock: MonotonicClock,
    kind: EgressKind,
    committer: &mut Committer,
) -> StartOutcome<Committer::Response>
where
    Committer: CloudflareEgressCommitter,
{
    if policy.write_deadline.is_expired_at(clock.now()) {
        return StartOutcome::Precommit {
            attempt: Box::new(attempt),
            cause: ResponseEgressOutcome::DeadlineExceeded,
        };
    }
    let declared_length = prepared.declared_length();
    let transmits_body = prepared.transmits_body();
    let response = prepared.into_response();
    let (parts, body) = response.into_parts();
    let (platform_response, mut io) = match committer.prepare(
        parts.status,
        &parts.headers,
        declared_length,
        transmits_body,
    ) {
        Ok(pair) => pair,
        Err(error) => {
            return StartOutcome::Precommit {
                attempt: Box::new(attempt),
                cause: error.outcome(),
            };
        }
    };
    if let Err(error) = io.preflight(policy.write_deadline, &clock) {
        abort(&mut io);
        return StartOutcome::Precommit {
            attempt: Box::new(attempt),
            cause: error.delivery_error().outcome(),
        };
    }
    if !attempt.begin_writing() {
        abort(&mut io);
        return StartOutcome::Precommit {
            attempt: Box::new(attempt),
            cause: ResponseEgressOutcome::ConversionError,
        };
    }
    if !transmits_body {
        settle_success(kind, &mut attempt, clock.now());
        return StartOutcome::Started(platform_response);
    }
    let task = AssertUnwindSafe(transmit_committed(body, policy, attempt, clock, kind, io))
        .catch_unwind()
        .map(|result| {
            if result.is_err() {
                log::error!("response-egress Cloudflare coordinator panicked");
            }
        })
        .boxed_local();
    committer.spawn(task);
    StartOutcome::Started(platform_response)
}

fn start_fallback<Committer>(
    request_method: &Method,
    original_cause: ResponseEgressOutcome,
    mut attempt: ResponseEgressAttempt,
    clock: &MonotonicClock,
    committer: &mut Committer,
) -> Result<Committer::Response, DeliveryError>
where
    Committer: CloudflareEgressCommitter,
{
    if !attempt.begin_fallback(original_cause) {
        attempt.terminate(original_cause, clock.now());
        return Err(DeliveryError::from_outcome(original_cause));
    }
    let started_at = clock.now();
    let fallback_deadline = started_at
        .checked_add(RESPONSE_EGRESS_FALLBACK_SAFETY_BUDGET)
        .unwrap_or(started_at);
    let prepared = attempt
        .prepare_response(
            ResponseEgressFramingMode::ExplicitBufferedLength,
            request_method,
            fallback_response(original_cause),
        )
        .map_err(|_error| {
            attempt.finish_fallback(ResponseEgressFallbackDisposition::Aborted, clock.now());
            DeliveryError::from_outcome(original_cause)
        })?;
    match start_prepared(
        prepared,
        ResponseEgressPolicy {
            write_deadline: Deadline::at_instant(fallback_deadline),
        },
        attempt,
        clock.clone(),
        EgressKind::Fallback,
        committer,
    ) {
        StartOutcome::Started(response) => Ok(response),
        StartOutcome::Precommit {
            attempt: mut failed_attempt,
            cause: _fallback_cause,
        } => {
            failed_attempt.finish_fallback(ResponseEgressFallbackDisposition::Aborted, clock.now());
            Err(DeliveryError::from_outcome(original_cause))
        }
    }
}

fn fallback_response(cause: ResponseEgressOutcome) -> Response {
    let (status, body) = if cause == ResponseEgressOutcome::DeadlineExceeded {
        (StatusCode::GATEWAY_TIMEOUT, TIMEOUT_FALLBACK_BODY)
    } else {
        (StatusCode::INTERNAL_SERVER_ERROR, INTERNAL_FALLBACK_BODY)
    };
    let mut response = Response::new(Body::from(body));
    *response.status_mut() = status;
    response
}

fn manual_response_encoding(headers: &HeaderMap, transmits_body: bool) -> bool {
    transmits_body && headers.contains_key(CONTENT_ENCODING)
}

#[cfg(all(feature = "cloudflare", target_arch = "wasm32"))]
mod platform {
    use super::{
        BodyStream, Bytes, CloudflareEgressCommitter, CloudflareEgressIo, Deadline, DeliveryError,
        Duration, EdgeError, EgressKind, HeaderMap, IoFailure, IoFailureKind, LocalBoxFuture,
        MonotonicClock, ResponseEgressFramingMode, StartOutcome, StatusCode, ensure_deadline,
        manual_response_encoding, start_fallback, start_prepared,
    };
    use std::cell::RefCell;
    use std::future::Future;
    use std::pin::Pin;
    use std::rc::Rc;
    use std::sync::Arc;
    use std::task::{Context as TaskContext, Poll, Waker};

    use edgezero_core::response_egress::{ResponseEgressAttempt, ResponseEgressEnvelope};
    use edgezero_core::time::MonotonicInstant;
    use futures_util::FutureExt as _;
    use futures_util::StreamExt as _;
    use futures_util::future::poll_fn;
    use worker::js_sys::{BigInt, Uint8Array};
    use worker::wasm_bindgen::JsCast as _;
    use worker::wasm_bindgen::closure::Closure;
    use worker::wasm_bindgen_futures::JsFuture;
    use worker::worker_sys::FixedLengthStream;
    use worker::{AbortSignal, Context, Delay, EncodeBody, Response as CfResponse};

    use crate::outbound::{copy_header_values, native_time};

    const fn public_message(error: DeliveryError) -> &'static str {
        match error {
            DeliveryError::Cancelled => "request cancelled during response delivery",
            DeliveryError::Conversion => "response conversion failed",
            DeliveryError::Deadline => "response write deadline exceeded",
            DeliveryError::Source => "response body source failed",
            DeliveryError::Transport => "response transport failed",
        }
    }

    pub(crate) struct CloudflareEgressRuntime {
        context: Arc<Context>,
        signal: AbortSignal,
    }

    impl CloudflareEgressRuntime {
        pub(crate) fn new(signal: web_sys::AbortSignal, context: Arc<Context>) -> Self {
            Self {
                context,
                signal: signal.into(),
            }
        }
    }

    struct WorkersCommitter {
        context: Arc<Context>,
        signal: AbortSignal,
    }

    impl CloudflareEgressCommitter for WorkersCommitter {
        type Io = WorkersEgressIo;
        type Response = CfResponse;

        fn prepare(
            &mut self,
            status: StatusCode,
            headers: &HeaderMap,
            declared_length: Option<u64>,
            transmits_body: bool,
        ) -> Result<(Self::Response, Self::Io), DeliveryError> {
            if !(200..=599).contains(&status.as_u16()) {
                return Err(DeliveryError::Conversion);
            }
            let mut worker_headers = worker::Headers::new();
            copy_header_values(headers, &mut worker_headers, |_name| {
                EdgeError::internal(anyhow::anyhow!(
                    "response header cannot be represented by Workers"
                ))
            })
            .map_err(|_error| DeliveryError::Conversion)?;
            let mut builder = CfResponse::builder()
                .with_status(status.as_u16())
                .with_headers(worker_headers);
            if manual_response_encoding(headers, transmits_body) {
                builder = builder.with_encode_body(EncodeBody::Manual);
            }
            if !transmits_body {
                return Ok((
                    builder.empty(),
                    WorkersEgressIo {
                        signal: self.signal.clone(),
                        writer: None,
                        closed: None,
                    },
                ));
            }
            let (readable, writable) = if let Some(length) = declared_length {
                let fixed = if let Ok(length_u32) = u32::try_from(length) {
                    FixedLengthStream::new(length_u32)
                } else {
                    FixedLengthStream::new_big_int(BigInt::from(length))
                }
                .map_err(|_error| DeliveryError::Conversion)?;
                (fixed.readable(), fixed.writable())
            } else {
                let transform =
                    web_sys::TransformStream::new().map_err(|_error| DeliveryError::Conversion)?;
                (transform.readable(), transform.writable())
            };
            let writer = writable
                .get_writer()
                .map_err(|_error| DeliveryError::Conversion)?;
            Ok((
                builder.stream(readable),
                WorkersEgressIo {
                    signal: self.signal.clone(),
                    closed: Some(WriterClosed::Watching(JsFuture::from(writer.closed()))),
                    writer: Some(writer),
                },
            ))
        }

        fn spawn(&mut self, task: LocalBoxFuture<'static, ()>) {
            self.context.wait_until(task);
        }
    }

    struct WorkersEgressIo {
        signal: AbortSignal,
        writer: Option<web_sys::WritableStreamDefaultWriter>,
        closed: Option<WriterClosed>,
    }

    enum WriterClosed {
        Watching(JsFuture),
        Resolved,
        Failed,
    }

    impl WriterClosed {
        fn poll_failed(&mut self, context: &mut TaskContext<'_>) -> bool {
            if let Self::Watching(future) = self
                && let Poll::Ready(result) = Pin::new(future).poll(context)
            {
                *self = if result.is_err() {
                    Self::Failed
                } else {
                    Self::Resolved
                };
            }
            matches!(self, Self::Failed)
        }
    }

    #[expect(
        clippy::missing_trait_methods,
        reason = "`Drop::pin_drop` is unstable (`pin_ergonomics`) and conflicts with `drop`"
    )]
    impl Drop for WorkersEgressIo {
        fn drop(&mut self) {
            self.abort();
        }
    }

    enum RaceEvent<Output> {
        Cancelled,
        Deadline,
        TransportFailed,
        Operation {
            output: Output,
            terminal: Option<IoFailureKind>,
        },
    }

    #[derive(Clone, Copy, Eq, PartialEq)]
    enum EgressOperationKind {
        BodySource,
        NativePromise,
    }

    fn terminal_operation<Operation>(
        kind: EgressOperationKind,
        cause: IoFailureKind,
        operation: Pin<&mut Operation>,
    ) -> Poll<RaceEvent<Operation::Output>>
    where
        Operation: Future,
    {
        // An existing native promise can report bytes already accepted by the writer.
        // Polling a body producer instead could start new work after termination.
        if kind == EgressOperationKind::NativePromise
            && let Poll::Ready(output) = operation.poll(&mut TaskContext::from_waker(Waker::noop()))
        {
            return Poll::Ready(RaceEvent::Operation {
                output,
                terminal: Some(cause),
            });
        }
        Poll::Ready(match cause {
            IoFailureKind::Cancelled => RaceEvent::Cancelled,
            IoFailureKind::Deadline => RaceEvent::Deadline,
            IoFailureKind::Transport => RaceEvent::TransportFailed,
        })
    }

    struct AbortWait {
        signal: AbortSignal,
        callback: Closure<dyn FnMut()>,
        waker: Rc<RefCell<Option<Waker>>>,
    }

    impl AbortWait {
        fn register(
            signal: &AbortSignal,
            deadline: Deadline,
            clock: &MonotonicClock,
        ) -> Result<(Self, MonotonicInstant), IoFailureKind> {
            if signal.aborted() {
                return Err(IoFailureKind::Cancelled);
            }
            let previous = clock.now();
            if deadline.is_expired_at(previous) {
                return Err(IoFailureKind::Deadline);
            }
            let waker = Rc::new(RefCell::new(None::<Waker>));
            let notified = Rc::clone(&waker);
            let callback = Closure::new(move || {
                let waiting = notified.borrow_mut().take();
                if let Some(task_waker) = waiting {
                    task_waker.wake();
                }
            });
            let wait = Self {
                signal: signal.clone(),
                callback,
                waker,
            };
            let registered = wait
                .signal
                .add_event_listener_with_callback("abort", wait.callback.as_ref().unchecked_ref());
            if signal.aborted() {
                return Err(IoFailureKind::Cancelled);
            }
            let registered_at = clock.now();
            if registered_at < previous {
                return Err(IoFailureKind::Transport);
            }
            if deadline.is_expired_at(registered_at) {
                return Err(IoFailureKind::Deadline);
            }
            registered.map_err(|_error| IoFailureKind::Transport)?;
            Ok((wait, registered_at))
        }
    }

    impl Future for AbortWait {
        type Output = ();

        fn poll(self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<Self::Output> {
            let previous = self.waker.borrow_mut().replace(cx.waker().clone());
            drop(previous);
            if self.signal.aborted() {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        }
    }

    #[expect(
        clippy::missing_trait_methods,
        reason = "`Drop::pin_drop` is unstable (`pin_ergonomics`) and conflicts with `drop`"
    )]
    impl Drop for AbortWait {
        fn drop(&mut self) {
            let waiting = self.waker.borrow_mut().take();
            drop(waiting);
            drop(self.signal.remove_event_listener_with_callback(
                "abort",
                self.callback.as_ref().unchecked_ref(),
            ));
        }
    }

    async fn race_operation<OperationFactory, Operation>(
        operation: OperationFactory,
        kind: EgressOperationKind,
        deadline: Deadline,
        clock: &MonotonicClock,
        signal: &AbortSignal,
        mut closed: Option<&mut WriterClosed>,
    ) -> RaceEvent<Operation::Output>
    where
        OperationFactory: FnOnce() -> Operation,
        Operation: Future,
    {
        let (cancellation, mut previous) = match AbortWait::register(signal, deadline, clock) {
            Ok(registered) => registered,
            Err(IoFailureKind::Cancelled) => return RaceEvent::Cancelled,
            Err(IoFailureKind::Deadline) => return RaceEvent::Deadline,
            Err(IoFailureKind::Transport) => return RaceEvent::TransportFailed,
        };
        let timer = native_time(clock.clone()).wait_until(deadline);
        let operation_future = operation();
        futures_util::pin_mut!(cancellation, timer, operation_future);
        poll_fn(|context| {
            if signal.aborted() || cancellation.as_mut().poll(context).is_ready() {
                return terminal_operation(
                    kind,
                    IoFailureKind::Cancelled,
                    operation_future.as_mut(),
                );
            }
            let timer_result = timer.as_mut().poll(context);
            if signal.aborted() {
                return terminal_operation(
                    kind,
                    IoFailureKind::Cancelled,
                    operation_future.as_mut(),
                );
            }
            if let Poll::Ready(result) = timer_result {
                let cause = match result {
                    Ok(()) => IoFailureKind::Deadline,
                    Err(_error) => IoFailureKind::Transport,
                };
                return terminal_operation(kind, cause, operation_future.as_mut());
            }
            let now = clock.now();
            if now < previous {
                return terminal_operation(
                    kind,
                    IoFailureKind::Transport,
                    operation_future.as_mut(),
                );
            }
            previous = now;
            if deadline.is_expired_at(now) {
                return terminal_operation(
                    kind,
                    IoFailureKind::Deadline,
                    operation_future.as_mut(),
                );
            }
            if closed
                .as_deref_mut()
                .is_some_and(|watch| watch.poll_failed(context))
            {
                let cause = if signal.aborted() {
                    IoFailureKind::Cancelled
                } else if deadline.is_expired_at(clock.now()) {
                    IoFailureKind::Deadline
                } else {
                    IoFailureKind::Transport
                };
                return terminal_operation(kind, cause, operation_future.as_mut());
            }
            let observed = operation_future.as_mut().poll(context);
            let observed_at = clock.now();
            let terminal = if signal.aborted() {
                Some(IoFailureKind::Cancelled)
            } else if observed_at < previous {
                Some(IoFailureKind::Transport)
            } else if deadline.is_expired_at(observed_at) {
                Some(IoFailureKind::Deadline)
            } else if closed
                .as_deref_mut()
                .is_some_and(|watch| watch.poll_failed(context))
            {
                Some(IoFailureKind::Transport)
            } else {
                None
            };
            previous = observed_at;
            match observed {
                Poll::Ready(output) => Poll::Ready(RaceEvent::Operation { output, terminal }),
                Poll::Pending => match terminal {
                    Some(IoFailureKind::Cancelled) => Poll::Ready(RaceEvent::Cancelled),
                    Some(IoFailureKind::Deadline) => Poll::Ready(RaceEvent::Deadline),
                    Some(IoFailureKind::Transport) => Poll::Ready(RaceEvent::TransportFailed),
                    None => Poll::Pending,
                },
            }
        })
        .await
    }

    #[async_trait::async_trait(?Send)]
    impl CloudflareEgressIo for WorkersEgressIo {
        fn abort(&mut self) {
            let settled = matches!(
                self.closed.take(),
                Some(WriterClosed::Resolved | WriterClosed::Failed)
            );
            if let Some(writer) = self.writer.take() {
                if !settled {
                    drop(JsFuture::from(writer.abort()));
                }
                writer.release_lock();
            }
        }

        async fn finish(
            &mut self,
            deadline: Deadline,
            clock: &MonotonicClock,
        ) -> Result<(), IoFailureKind> {
            let Some(writer) = self.writer.as_ref() else {
                return ensure_deadline(deadline, clock).map_err(|_error| IoFailureKind::Deadline);
            };
            let event = race_operation(
                || JsFuture::from(writer.close()),
                EgressOperationKind::NativePromise,
                deadline,
                clock,
                &self.signal,
                None,
            )
            .await;
            let result = match event {
                RaceEvent::Cancelled => Err(IoFailureKind::Cancelled),
                RaceEvent::Deadline => Err(IoFailureKind::Deadline),
                RaceEvent::TransportFailed => Err(IoFailureKind::Transport),
                RaceEvent::Operation {
                    terminal: Some(kind),
                    ..
                } => Err(kind),
                RaceEvent::Operation {
                    output: Ok(_value),
                    terminal: None,
                } => Ok(()),
                RaceEvent::Operation {
                    output: Err(_error),
                    terminal: None,
                } => Err(IoFailureKind::Transport),
            };
            if result.is_ok() {
                self.closed = None;
                if let Some(finished_writer) = self.writer.take() {
                    finished_writer.release_lock();
                }
            }
            result
        }

        async fn next_body(
            &mut self,
            source: &mut BodyStream,
            deadline: Deadline,
            clock: &MonotonicClock,
            attempt: &mut ResponseEgressAttempt,
        ) -> Result<Option<Result<Bytes, EdgeError>>, IoFailureKind> {
            Delay::from(Duration::ZERO).await;
            match race_operation(
                || {
                    source.next().inspect(move |item| {
                        if let Some(Err(error)) = item {
                            attempt.record_source_error(error);
                        }
                    })
                },
                EgressOperationKind::BodySource,
                deadline,
                clock,
                &self.signal,
                self.closed.as_mut(),
            )
            .await
            {
                RaceEvent::Cancelled => Err(IoFailureKind::Cancelled),
                RaceEvent::Deadline => Err(IoFailureKind::Deadline),
                RaceEvent::TransportFailed => Err(IoFailureKind::Transport),
                RaceEvent::Operation {
                    terminal: Some(kind),
                    ..
                } => Err(kind),
                RaceEvent::Operation {
                    output: item,
                    terminal: None,
                } => Ok(item),
            }
        }

        fn preflight(
            &self,
            deadline: Deadline,
            clock: &MonotonicClock,
        ) -> Result<(), IoFailureKind> {
            if self.signal.aborted() {
                Err(IoFailureKind::Cancelled)
            } else {
                ensure_deadline(deadline, clock).map_err(|_error| IoFailureKind::Deadline)
            }
        }

        async fn write(
            &mut self,
            bytes: Vec<u8>,
            deadline: Deadline,
            clock: &MonotonicClock,
        ) -> Result<usize, IoFailure> {
            let requested = bytes.len();
            let Some(writer) = self.writer.as_ref() else {
                return Err(IoFailure {
                    accepted: 0,
                    kind: IoFailureKind::Transport,
                });
            };
            let chunk = Uint8Array::from(bytes.as_slice());
            let event = race_operation(
                || JsFuture::from(writer.write_with_chunk(&chunk.into())),
                EgressOperationKind::NativePromise,
                deadline,
                clock,
                &self.signal,
                None,
            )
            .await;
            match event {
                RaceEvent::Cancelled => Err(IoFailure {
                    accepted: 0,
                    kind: IoFailureKind::Cancelled,
                }),
                RaceEvent::Deadline => Err(IoFailure {
                    accepted: 0,
                    kind: IoFailureKind::Deadline,
                }),
                RaceEvent::TransportFailed => Err(IoFailure {
                    accepted: 0,
                    kind: IoFailureKind::Transport,
                }),
                RaceEvent::Operation {
                    output: Ok(_value),
                    terminal: Some(kind),
                } => Err(IoFailure {
                    accepted: requested,
                    kind,
                }),
                RaceEvent::Operation {
                    output: Ok(_value),
                    terminal: None,
                } => Ok(requested),
                RaceEvent::Operation {
                    output: Err(_error),
                    terminal,
                } => Err(IoFailure {
                    accepted: 0,
                    kind: terminal.unwrap_or(IoFailureKind::Transport),
                }),
            }
        }
    }

    /// Converts an owned egress envelope into a Workers response backed by one adapter-owned writer.
    ///
    /// # Errors
    /// Returns a category-only error when both the application response and bounded precommit fallback
    /// fail before the Workers response is returned.
    pub(crate) fn from_egress_response(
        egress: ResponseEgressEnvelope,
        runtime: CloudflareEgressRuntime,
    ) -> Result<CfResponse, EdgeError> {
        let request_method = egress.request_method().clone();
        let mut committer = WorkersCommitter {
            context: runtime.context,
            signal: runtime.signal,
        };
        let result = match egress.begin(ResponseEgressFramingMode::ExplicitBufferedLength) {
            Ok((prepared, policy, attempt, clock)) => {
                match start_prepared(
                    prepared,
                    policy,
                    attempt,
                    clock.clone(),
                    EgressKind::Application,
                    &mut committer,
                ) {
                    StartOutcome::Started(response) => Ok(response),
                    StartOutcome::Precommit {
                        attempt: failed_attempt,
                        cause,
                    } => start_fallback(
                        &request_method,
                        cause,
                        *failed_attempt,
                        &clock,
                        &mut committer,
                    ),
                }
            }
            Err(failure) => {
                let (attempt, clock, cause) = failure.into_parts();
                start_fallback(&request_method, cause, attempt, &clock, &mut committer)
            }
        };
        result.map_err(|error| EdgeError::internal(anyhow::anyhow!(public_message(error))))
    }

    #[cfg(test)]
    mod race_tests {
        use std::cell::Cell;
        use std::rc::Rc;
        use std::sync::Mutex;
        use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
        use std::task::Waker;

        use edgezero_core::body::Body;
        use edgezero_core::error::BadGatewayReason;
        use edgezero_core::error_detail::{
            CloudflareFailureCause, ProviderFailureDetail, ProviderFailurePhase,
        };
        use edgezero_core::http::{Method, Response, Version, response_builder};
        use edgezero_core::outbound::OutboundScopeOwner;
        use edgezero_core::response_egress::{
            ResponseEgressBodyKind, ResponseEgressCompletion, ResponseEgressFallbackDisposition,
            ResponseEgressHead, ResponseEgressObserver as _, ResponseEgressObserverHandle,
            ResponseEgressOutcome, ResponseEgressPolicy, ResponseEgressReport,
        };
        use edgezero_core::response_egress_framing::PreparedResponseEgress;
        use edgezero_core::time::MonotonicInstant;
        use futures::channel::oneshot;
        use futures::task::{ArcWake, waker as task_waker};
        use futures_util::{future, stream};
        use wasm_bindgen_test::wasm_bindgen_test;
        use worker::js_sys::{Array, Function, Promise, Reflect, global};
        use worker::wasm_bindgen::JsValue;
        use worker::wasm_bindgen::closure::Closure;

        use super::super::INTERNAL_FALLBACK_BODY;
        use super::super::tests::RecordingObserver;
        use super::*;
        use crate::test_runtime::NumericTimeoutGuard;

        struct StatusFixture {
            attempt: ResponseEgressAttempt,
            clock: MonotonicClock,
            completed: RecordingObserver,
            observed: RecordingObserver,
            policy: ResponseEgressPolicy,
            prepared: PreparedResponseEgress,
        }

        fn status_fixture(response: Response, method: &Method) -> StatusFixture {
            let clock = MonotonicClock::default();
            let now = clock.now();
            let deadline = now.checked_add(Duration::from_secs(10)).expect("deadline");
            let completed = RecordingObserver::default();
            let observed = RecordingObserver::default();
            let head = ResponseEgressHead::new(
                response.status(),
                response.version(),
                response.headers(),
                None,
                now,
                None,
            );
            let completion = completed.clone();
            let mut attempt = ResponseEgressAttempt::new(
                &head,
                now,
                ResponseEgressCompletion::new(move |report| completion.complete(report)),
                ResponseEgressObserverHandle::new(observed.clone()),
                clock.clone(),
            );
            let prepared = attempt
                .prepare_response(
                    ResponseEgressFramingMode::ExplicitBufferedLength,
                    method,
                    response,
                )
                .expect("core preserves provider-specific status validation");
            StatusFixture {
                attempt,
                clock,
                completed,
                observed,
                policy: ResponseEgressPolicy {
                    write_deadline: Deadline::at_instant(deadline),
                },
                prepared,
            }
        }

        fn sdk_committer(signal: web_sys::AbortSignal) -> (WorkersCommitter, Array) {
            let pending = Array::new();
            let context = Function::new_with_args(
                "tasks",
                "return { waitUntil(promise) { tasks.push(promise); } };",
            )
            .call1(&JsValue::UNDEFINED, &pending)
            .expect("guest waitUntil");
            (
                WorkersCommitter {
                    context: Arc::new(Context::new(context.unchecked_into())),
                    signal: signal.into(),
                },
                pending,
            )
        }

        fn sdk_io(signal: web_sys::AbortSignal) -> (WorkersEgressIo, wasm_streams::ReadableStream) {
            let (mut committer, _) = sdk_committer(signal);
            let (response, io) = committer
                .prepare(StatusCode::OK, &HeaderMap::new(), None, true)
                .expect("SDK response");
            let Ok(raw) = worker::IntoResponse::into_raw(response) else {
                panic!("SDK response");
            };
            (
                io,
                wasm_streams::ReadableStream::from_raw(raw.body().expect("body")),
            )
        }

        async fn assert_status_handoff(status: u16, streamed: bool, method: &Method) {
            let _timers = NumericTimeoutGuard::install();
            Function::new_no_args(include_str!("../tests/worker_runtime.js"))
                .call0(&JsValue::UNDEFINED)
                .expect("guest Workers stream fixture");
            let controller = web_sys::AbortController::new().expect("controller");
            let (mut committer, pending) = sdk_committer(controller.signal());
            let polls = Rc::new(Cell::new(0_usize));
            let counted = Rc::clone(&polls);
            let source = Rc::new(());
            let retained = Rc::downgrade(&source);
            let body = if streamed {
                Body::from_stream(stream::once(async move {
                    counted.set(counted.get().checked_add(1).expect("source poll count"));
                    drop(source);
                    Ok(Bytes::from_static(b"application body"))
                }))
            } else {
                drop(source);
                Body::from("application body")
            };
            let fixture = status_fixture(
                response_builder()
                    .status(status)
                    .body(body)
                    .expect("response"),
                method,
            );
            let response = match start_prepared(
                fixture.prepared,
                fixture.policy,
                fixture.attempt,
                fixture.clock.clone(),
                EgressKind::Application,
                &mut committer,
            ) {
                StartOutcome::Started(response) => response,
                StartOutcome::Precommit { attempt, cause } => {
                    assert_eq!(cause, ResponseEgressOutcome::ConversionError);
                    assert_eq!(polls.get(), 0);
                    assert!(retained.upgrade().is_none());
                    assert!(fixture.completed.reports().is_empty());
                    assert!(fixture.observed.reports().is_empty());
                    start_fallback(method, cause, *attempt, &fixture.clock, &mut committer)
                        .expect("status conversion fallback")
                }
            };
            let Ok(raw) = worker::IntoResponse::into_raw(response) else {
                panic!("SDK native response conversion must succeed");
            };
            let bytes = Uint8Array::new(
                &JsFuture::from(raw.array_buffer().expect("JS response body"))
                    .await
                    .expect("consume JS response"),
            )
            .to_vec();
            JsFuture::from(Promise::all(&pending))
                .await
                .expect("response writer tasks");
            let rejected = status != 599;
            assert_eq!(raw.status(), if rejected { 500 } else { status });
            let expected_body = if *method == Method::HEAD {
                b"".as_slice()
            } else if rejected {
                INTERNAL_FALLBACK_BODY
            } else {
                b"application body".as_slice()
            };
            assert_eq!(bytes, expected_body);
            assert_eq!(
                polls.get(),
                usize::from(streamed && !rejected && *method != Method::HEAD)
            );
            assert!(retained.upgrade().is_none());
            assert_status_report(
                &fixture.completed,
                &fixture.observed,
                rejected,
                expected_body.len(),
            );
        }

        fn assert_status_report(
            completed: &RecordingObserver,
            observed: &RecordingObserver,
            rejected: bool,
            expected_bytes: usize,
        ) {
            let reports = observed.reports();
            assert_eq!(reports.len(), 1);
            assert_eq!(completed.reports(), reports);
            assert_eq!(
                reports[0].outcome,
                if rejected {
                    ResponseEgressOutcome::ConversionError
                } else {
                    ResponseEgressOutcome::HostHandoff
                }
            );
            assert_eq!(
                reports[0].fallback_disposition,
                rejected.then_some(ResponseEgressFallbackDisposition::Completed)
            );
            assert_eq!(
                reports[0].body_kind,
                if rejected {
                    ResponseEgressBodyKind::Fallback
                } else {
                    ResponseEgressBodyKind::Application
                }
            );
            assert_eq!(
                reports[0].bytes_written,
                u64::try_from(expected_bytes).expect("byte count")
            );
            assert_eq!(reports[0].framing_failures.application(), None);
            assert_eq!(reports[0].framing_failures.fallback(), None);
        }

        #[wasm_bindgen_test]
        async fn writer_closed_sdk_controls_reject_readable_cancellation_without_request_abort() {
            Function::new_no_args(include_str!("../tests/worker_runtime.js"))
                .call0(&JsValue::UNDEFINED)
                .expect("guest-only FixedLengthStream shim");
            for declared_length in [None, Some(7)] {
                let (raw_readable, writable) = if let Some(length) = declared_length {
                    let fixed = FixedLengthStream::new(length).expect("fixed stream");
                    (fixed.readable(), fixed.writable())
                } else {
                    let transform = web_sys::TransformStream::new().expect("transform stream");
                    (transform.readable(), transform.writable())
                };
                let controller = web_sys::AbortController::new().expect("controller");
                let writer = writable.get_writer().expect("writer");
                let closed = JsFuture::from(writer.closed());
                let prefix = Uint8Array::from(b"prefix".as_slice());
                let written = JsFuture::from(writer.write_with_chunk(&prefix.into()));
                let mut readable = wasm_streams::ReadableStream::from_raw(raw_readable);
                let mut reader = readable.get_reader();
                let value = reader.read().await.expect("prefix read").expect("prefix");
                assert_eq!(Uint8Array::new(&value).to_vec(), b"prefix");
                written.await.expect("accepted prefix");
                assert!(!controller.signal().aborted());
                reader.cancel().await.expect("cancel readable");
                assert!(
                    closed.await.is_err(),
                    "readable cancellation rejects closed"
                );
                assert!(!controller.signal().aborted());
                writer.release_lock();
            }
        }

        struct DropCounter(Rc<Cell<usize>>);

        #[expect(clippy::missing_trait_methods, reason = "Drop::pin_drop is unstable")]
        impl Drop for DropCounter {
            fn drop(&mut self) {
                self.0.set(self.0.get().checked_add(1).expect("drop count"));
            }
        }

        #[wasm_bindgen_test]
        async fn writer_closed_releases_pending_source_and_owner_before_original_deadline() {
            let _timers = NumericTimeoutGuard::install();
            Function::new_no_args(include_str!("../tests/worker_runtime.js"))
                .call0(&JsValue::UNDEFINED)
                .expect("guest-only FixedLengthStream shim");
            for length in [None, Some(7)] {
                assert_pending_source_cancelled(length).await;
            }
        }

        async fn assert_pending_source_cancelled(length: Option<u32>) {
            let source_drops = Rc::new(Cell::new(0));
            let lease = DropCounter(Rc::clone(&source_drops));
            let (notify_pending, entered_pending) = oneshot::channel();
            let source = stream::once(future::ready(Ok(Bytes::from_static(b"prefix")))).chain(
                stream::once(async move {
                    let _lease = lease;
                    let mut notification = Some(notify_pending);
                    future::poll_fn(move |_| {
                        if let Some(sender) = notification.take() {
                            sender.send(()).expect("pending observer");
                        }
                        Poll::<Result<Bytes, EdgeError>>::Pending
                    })
                    .await
                }),
            );
            let owner = OutboundScopeOwner::response_body();
            let scope = owner.scope();
            let owner_drops = Rc::new(Cell::new(0));
            let resource = scope
                .provider_state(|| DropCounter(Rc::clone(&owner_drops)))
                .expect("owned resource");
            let retained = Rc::downgrade(&resource);
            drop(resource);
            let mut application = response_builder()
                .body(Body::from_stream(source))
                .expect("response");
            if let Some(declared_length) = length {
                application.headers_mut().insert(
                    "content-length",
                    declared_length.to_string().parse().expect("length header"),
                );
            }
            let owned_response = owner.attach(application).expect("attached owner");
            let mut fixture = status_fixture(owned_response, &Method::GET);
            fixture.policy.write_deadline =
                fixture.clock.deadline_after(Duration::from_millis(500));
            let deadline = fixture.policy.write_deadline;
            let controller = web_sys::AbortController::new().expect("controller");
            let (mut committer, tasks) = sdk_committer(controller.signal());
            let native_response = match start_prepared(
                fixture.prepared,
                fixture.policy,
                fixture.attempt,
                fixture.clock.clone(),
                EgressKind::Application,
                &mut committer,
            ) {
                StartOutcome::Started(delivering) => delivering,
                StartOutcome::Precommit { .. } => panic!("no fallback before commit"),
            };
            let Ok(raw) = worker::IntoResponse::into_raw(native_response) else {
                panic!("SDK response");
            };
            let mut readable = wasm_streams::ReadableStream::from_raw(raw.body().expect("body"));
            let mut reader = readable.get_reader();
            let value = reader.read().await.expect("prefix read").expect("prefix");
            assert_eq!(Uint8Array::new(&value).to_vec(), b"prefix");
            entered_pending
                .await
                .expect("source actually returned Pending");
            assert!(fixture.observed.reports().is_empty());
            assert_eq!(source_drops.get(), 0);
            assert_eq!(owner_drops.get(), 0);
            assert!(!controller.signal().aborted());
            reader.cancel().await.expect("cancel readable");
            JsFuture::from(Promise::all(&tasks))
                .await
                .expect("egress task");
            let reports = fixture.observed.reports();
            assert_eq!(reports.len(), 1);
            assert_eq!(fixture.completed.reports(), reports);
            assert_eq!(reports[0].outcome, ResponseEgressOutcome::TransportError);
            assert!(!deadline.is_expired_at(fixture.clock.now()));
            assert_eq!(reports[0].bytes_written, 6);
            assert_eq!(reports[0].body_kind, ResponseEgressBodyKind::Application);
            assert_eq!(reports[0].fallback_disposition, None);
            assert_eq!(source_drops.get(), 1);
            assert_eq!(owner_drops.get(), 1);
            assert!(retained.upgrade().is_none());
            assert!(scope.check().is_err());
            assert!(!controller.signal().aborted());
            drop(reader);
            drop(raw);
            assert_eq!(fixture.observed.reports(), reports);
        }

        #[wasm_bindgen_test]
        async fn writer_closed_already_rejected_preserves_abort_and_deadline_precedence() {
            let _timers = NumericTimeoutGuard::install();
            for expected in [
                IoFailureKind::Transport,
                IoFailureKind::Cancelled,
                IoFailureKind::Deadline,
            ] {
                let controller = web_sys::AbortController::new().expect("controller");
                let (mut io, mut readable) = sdk_io(controller.signal());
                readable.cancel().await.expect("cancel readable");
                let mut fixture = status_fixture(
                    response_builder().body(Body::empty()).expect("response"),
                    &Method::GET,
                );
                let deadline = if expected == IoFailureKind::Deadline {
                    Deadline::at_instant(fixture.clock.now())
                } else {
                    fixture.clock.deadline_after(Duration::from_millis(500))
                };
                if expected == IoFailureKind::Cancelled {
                    controller.abort();
                }
                let polls = Rc::new(Cell::new(0_usize));
                let counted = Rc::clone(&polls);
                let mut source = stream::poll_fn(move |_| {
                    counted.set(counted.get().checked_add(1).expect("poll count"));
                    Poll::Ready(None)
                })
                .boxed_local();
                let error = io
                    .next_body(&mut source, deadline, &fixture.clock, &mut fixture.attempt)
                    .await
                    .expect_err("guard precedes ready source");
                assert_eq!(error, expected);
                assert_eq!(polls.get(), 0);
                if expected == IoFailureKind::Transport {
                    assert!(matches!(io.closed, Some(WriterClosed::Failed)));
                    assert_eq!(
                        io.next_body(&mut source, deadline, &fixture.clock, &mut fixture.attempt)
                            .await
                            .expect_err("latched rejection"),
                        IoFailureKind::Transport
                    );
                    assert_eq!(polls.get(), 0);
                }
                io.abort();
                io.abort();
                assert!(io.closed.is_none());
                assert!(io.writer.is_none());
                Delay::from(Duration::ZERO).await;
            }
        }

        #[wasm_bindgen_test]
        async fn writer_closed_success_is_not_eof_and_is_not_repolled() {
            let _timers = NumericTimeoutGuard::install();
            let controller = web_sys::AbortController::new().expect("controller");
            let (mut io, mut readable) = sdk_io(controller.signal());
            let close = JsFuture::from(io.writer.as_ref().expect("writer").close());
            let mut reader = readable.get_reader();
            assert!(reader.read().await.expect("closed readable").is_none());
            close.await.expect("successful SDK close");
            let mut fixture = status_fixture(
                response_builder().body(Body::empty()).expect("response"),
                &Method::GET,
            );
            let mut source = stream::iter([
                Ok(Bytes::from_static(b"first")),
                Ok(Bytes::from_static(b"second")),
            ])
            .boxed_local();
            for expected in [b"first".as_slice(), b"second".as_slice()] {
                let item = io
                    .next_body(
                        &mut source,
                        fixture.policy.write_deadline,
                        &fixture.clock,
                        &mut fixture.attempt,
                    )
                    .await
                    .expect("source read")
                    .expect("source item")
                    .expect("source bytes");
                assert_eq!(item.as_ref(), expected);
                assert!(matches!(io.closed, Some(WriterClosed::Resolved)));
            }
            assert!(
                io.next_body(
                    &mut source,
                    fixture.policy.write_deadline,
                    &fixture.clock,
                    &mut fixture.attempt,
                )
                .await
                .expect("source EOF")
                .is_none()
            );
            io.abort();
            io.abort();
            assert!(io.closed.is_none());
            assert!(io.writer.is_none());
        }

        #[wasm_bindgen_test]
        async fn writer_closed_finish_clears_watch_and_writer_before_idempotent_abort() {
            let _timers = NumericTimeoutGuard::install();
            let controller = web_sys::AbortController::new().expect("controller");
            let (mut io, mut readable) = sdk_io(controller.signal());
            let clock = MonotonicClock::default();
            let deadline = clock.deadline_after(Duration::from_millis(500));
            let mut reader = readable.get_reader();
            let (finished, read) = future::join(io.finish(deadline, &clock), reader.read()).await;
            finished.expect("successful finish");
            assert!(read.expect("finished readable").is_none());
            assert!(io.closed.is_none());
            assert!(io.writer.is_none());
            io.abort();
            io.abort();
            Delay::from(Duration::ZERO).await;
        }

        #[wasm_bindgen_test]
        async fn status_600_buffered_get_uses_conversion_fallback() {
            assert_status_handoff(600, false, &Method::GET).await;
        }

        #[wasm_bindgen_test]
        async fn status_600_streamed_get_uses_conversion_fallback() {
            assert_status_handoff(600, true, &Method::GET).await;
        }

        #[wasm_bindgen_test]
        async fn status_600_buffered_head_uses_conversion_fallback() {
            assert_status_handoff(600, false, &Method::HEAD).await;
        }

        #[wasm_bindgen_test]
        async fn status_600_streamed_head_uses_conversion_fallback() {
            assert_status_handoff(600, true, &Method::HEAD).await;
        }

        #[wasm_bindgen_test]
        async fn status_999_buffered_get_uses_conversion_fallback() {
            assert_status_handoff(999, false, &Method::GET).await;
        }

        #[wasm_bindgen_test]
        async fn status_999_streamed_get_uses_conversion_fallback() {
            assert_status_handoff(999, true, &Method::GET).await;
        }

        #[wasm_bindgen_test]
        async fn status_999_buffered_head_uses_conversion_fallback() {
            assert_status_handoff(999, false, &Method::HEAD).await;
        }

        #[wasm_bindgen_test]
        async fn status_999_streamed_head_uses_conversion_fallback() {
            assert_status_handoff(999, true, &Method::HEAD).await;
        }

        #[wasm_bindgen_test]
        async fn status_599_is_accepted_for_buffered_and_streamed_get_and_head() {
            for method in [Method::GET, Method::HEAD] {
                for streamed in [false, true] {
                    assert_status_handoff(599, streamed, &method).await;
                }
            }
        }

        #[derive(Clone, Copy, Eq, PartialEq)]
        enum SourceCase {
            AlreadyAborted,
            AlreadyExpired,
            Eof,
            Ready,
            ReadyAborted,
            ReadyExpired,
        }

        async fn observed_source(
            case: SourceCase,
            with_detail: bool,
        ) -> (ResponseEgressReport, usize) {
            let controller = web_sys::AbortController::new().expect("controller");
            if case == SourceCase::AlreadyAborted {
                controller.abort();
            }
            let now = MonotonicInstant::now();
            let deadline = now.checked_add(Duration::from_secs(10)).expect("deadline");
            let expired = Arc::new(AtomicBool::new(case == SourceCase::AlreadyExpired));
            let clock_expired = Arc::clone(&expired);
            let clock = MonotonicClock::new(move || {
                if clock_expired.load(Ordering::SeqCst) {
                    deadline
                } else {
                    now
                }
            });
            let polls = Arc::new(AtomicUsize::new(0));
            let source_polls = Arc::clone(&polls);
            let body_controller = controller.clone();
            let mut source = if case == SourceCase::Eof {
                stream::empty().boxed_local()
            } else {
                stream::once(async move {
                    source_polls.fetch_add(1, Ordering::SeqCst);
                    expired.store(case == SourceCase::ReadyExpired, Ordering::SeqCst);
                    if case == SourceCase::ReadyAborted {
                        body_controller.abort();
                    }
                    let detail = ProviderFailureDetail::Cloudflare {
                        phase: ProviderFailurePhase::ResponseBodyRead,
                        cause: CloudflareFailureCause::TypeError,
                    };
                    Err(if with_detail {
                        EdgeError::bad_gateway_with_detail(
                            "provider-secret-token",
                            BadGatewayReason::Transport,
                            detail,
                        )
                    } else {
                        EdgeError::bad_gateway_with_reason(
                            "provider-secret-token",
                            BadGatewayReason::Transport,
                        )
                    })
                })
                .boxed_local()
            };
            let reports = Arc::new(Mutex::new(Vec::new()));
            let completed = Arc::clone(&reports);
            let headers = HeaderMap::new();
            let head = ResponseEgressHead::new(
                StatusCode::OK,
                Version::HTTP_11,
                &headers,
                None,
                now,
                None,
            );
            let mut attempt = ResponseEgressAttempt::new(
                &head,
                now,
                ResponseEgressCompletion::new(move |report| {
                    completed.lock().expect("reports").push(report.clone());
                }),
                ResponseEgressObserverHandle::default(),
                clock.clone(),
            );
            assert!(attempt.begin_writing());
            let mut io = WorkersEgressIo {
                signal: controller.signal().into(),
                writer: None,
                closed: None,
            };
            let timers = NumericTimeoutGuard::install();
            let result = io
                .next_body(
                    &mut source,
                    Deadline::at_instant(deadline),
                    &clock,
                    &mut attempt,
                )
                .await;
            drop(timers);
            let outcome = match result {
                Ok(Some(Err(_))) => ResponseEgressOutcome::SourceError,
                Ok(None) => ResponseEgressOutcome::Completed,
                Err(IoFailureKind::Deadline) => ResponseEgressOutcome::DeadlineExceeded,
                Err(IoFailureKind::Cancelled) => ResponseEgressOutcome::RequestCancelled,
                _ => panic!("unexpected source result"),
            };
            assert!(attempt.terminate(outcome, clock.now()));
            drop(attempt);
            let retained = reports.lock().expect("reports");
            assert_eq!(retained.len(), 1);
            (retained[0].clone(), polls.load(Ordering::SeqCst))
        }

        #[wasm_bindgen_test]
        async fn provider_diagnostics_survive_real_source_race_preemption() {
            for case in [
                SourceCase::AlreadyAborted,
                SourceCase::AlreadyExpired,
                SourceCase::Eof,
                SourceCase::Ready,
                SourceCase::ReadyAborted,
                SourceCase::ReadyExpired,
            ] {
                for with_detail in [false, true] {
                    let (report, polls) = observed_source(case, with_detail).await;
                    let observed = matches!(
                        case,
                        SourceCase::Ready | SourceCase::ReadyAborted | SourceCase::ReadyExpired
                    );
                    assert_eq!(polls, usize::from(observed));
                    assert_eq!(
                        report.outcome,
                        match case {
                            SourceCase::AlreadyAborted | SourceCase::ReadyAborted =>
                                ResponseEgressOutcome::RequestCancelled,
                            SourceCase::AlreadyExpired | SourceCase::ReadyExpired =>
                                ResponseEgressOutcome::DeadlineExceeded,
                            SourceCase::Eof => ResponseEgressOutcome::Completed,
                            SourceCase::Ready => ResponseEgressOutcome::SourceError,
                        }
                    );
                    assert_eq!(report.source_failure.is_some(), observed);
                    if let Some(failure) = report.source_failure {
                        assert_eq!(failure.category(), "bad_gateway_transport");
                        assert_eq!(failure.body_kind(), ResponseEgressBodyKind::Application);
                        let expected = ProviderFailureDetail::Cloudflare {
                            phase: ProviderFailurePhase::ResponseBodyRead,
                            cause: CloudflareFailureCause::TypeError,
                        };
                        assert_eq!(
                            failure.provider_failure_detail(),
                            with_detail.then_some(&expected)
                        );
                        assert!(!format!("{failure:?}").contains("secret"));
                    }
                }
            }
        }

        #[wasm_bindgen_test]
        fn response_egress_deadline_includes_operation_factory_work() {
            assert_pending_operation_expiry(true);
        }

        #[wasm_bindgen_test]
        fn response_egress_deadline_includes_pending_operation_work() {
            assert_pending_operation_expiry(false);
        }

        fn assert_pending_operation_expiry(expire_in_factory: bool) {
            let _timers = NumericTimeoutGuard::install();
            let controller = web_sys::AbortController::new().expect("abort controller");
            let signal: AbortSignal = controller.signal().into();
            let start = MonotonicInstant::now();
            let until = start
                .checked_add(Duration::from_millis(10))
                .expect("deadline");
            let now = Arc::new(Mutex::new(start));
            let sample = Arc::clone(&now);
            let clock = MonotonicClock::new(move || *sample.lock().expect("clock"));
            let polls = Rc::new(Cell::new(0_usize));
            let observed = Rc::clone(&polls);
            let resource = Rc::new(());
            let released = Rc::downgrade(&resource);
            let mut operation = Box::pin(race_operation(
                move || {
                    if expire_in_factory {
                        *now.lock().expect("factory clock") = until;
                    }
                    poll_fn(move |_context| {
                        assert_eq!(Rc::strong_count(&resource), 1);
                        observed.set(observed.get().checked_add(1).expect("poll count"));
                        *now.lock().expect("operation clock") = until;
                        Poll::<()>::Pending
                    })
                },
                EgressOperationKind::BodySource,
                Deadline::at_instant(until),
                &clock,
                &signal,
                None,
            ));
            let outcome = operation
                .as_mut()
                .poll(&mut TaskContext::from_waker(Waker::noop()));
            let released_at_terminal = released.upgrade().is_none();
            drop(operation);
            assert!(matches!(outcome, Poll::Ready(RaceEvent::Deadline)));
            assert_eq!(polls.get(), usize::from(!expire_in_factory));
            assert!(released_at_terminal);
        }

        #[wasm_bindgen_test]
        fn response_egress_rechecks_deadline_without_timer_notification() {
            assert_response_egress_repoll(false);
        }

        #[wasm_bindgen_test]
        fn response_egress_cancellation_wins_expiry_without_timer_notification() {
            assert_response_egress_repoll(true);
        }

        fn assert_response_egress_repoll(cancel: bool) {
            let _timers = NumericTimeoutGuard::install();
            let controller = web_sys::AbortController::new().expect("abort controller");
            let signal: AbortSignal = controller.signal().into();
            let start = MonotonicInstant::now();
            let until = start
                .checked_add(Duration::from_millis(10))
                .expect("deadline");
            let now = Arc::new(Mutex::new(start));
            let sample = Arc::clone(&now);
            let clock = MonotonicClock::new(move || *sample.lock().expect("clock"));
            let polls = Rc::new(Cell::new(0_usize));
            let observed = Rc::clone(&polls);
            let mut operation = Box::pin(race_operation(
                move || {
                    poll_fn(move |_context| {
                        observed.set(observed.get().checked_add(1).expect("poll count"));
                        Poll::<()>::Pending
                    })
                },
                EgressOperationKind::BodySource,
                Deadline::at_instant(until),
                &clock,
                &signal,
                None,
            ));
            let mut context = TaskContext::from_waker(Waker::noop());
            assert!(operation.as_mut().poll(&mut context).is_pending());
            *now.lock().expect("advance clock") = until;
            if cancel {
                controller.abort();
            }
            let outcome = operation.as_mut().poll(&mut context);
            drop(operation);
            assert!(matches!(
                (cancel, outcome),
                (true, Poll::Ready(RaceEvent::Cancelled))
                    | (false, Poll::Ready(RaceEvent::Deadline))
            ));
            assert_eq!(polls.get(), 1);
        }

        #[wasm_bindgen_test]
        fn response_egress_cancellation_during_pending_poll_is_terminal() {
            let _timers = NumericTimeoutGuard::install();
            let controller = web_sys::AbortController::new().expect("abort controller");
            let signal: AbortSignal = controller.signal().into();
            let clock = MonotonicClock::default();
            let mut operation = Box::pin(race_operation(
                move || {
                    poll_fn(move |_context| {
                        controller.abort();
                        Poll::<()>::Pending
                    })
                },
                EgressOperationKind::BodySource,
                clock.deadline_after(Duration::from_secs(1)),
                &clock,
                &signal,
                None,
            ));
            let outcome = operation
                .as_mut()
                .poll(&mut TaskContext::from_waker(Waker::noop()));
            drop(operation);
            assert!(matches!(outcome, Poll::Ready(RaceEvent::Cancelled)));
        }

        #[wasm_bindgen_test]
        fn response_egress_regression_during_pending_poll_is_transport_failure() {
            let _timers = NumericTimeoutGuard::install();
            let controller = web_sys::AbortController::new().expect("abort controller");
            let signal: AbortSignal = controller.signal().into();
            let start = MonotonicInstant::now();
            let now = Arc::new(Mutex::new(start));
            let sample = Arc::clone(&now);
            let clock = MonotonicClock::new(move || *sample.lock().expect("clock"));
            let mut operation = Box::pin(race_operation(
                move || {
                    poll_fn(move |_context| {
                        *now.lock().expect("regress clock") = start
                            .checked_sub(Duration::from_millis(1))
                            .expect("earlier clock");
                        Poll::<()>::Pending
                    })
                },
                EgressOperationKind::BodySource,
                clock.deadline_after(Duration::from_secs(1)),
                &clock,
                &signal,
                None,
            ));
            let outcome = operation
                .as_mut()
                .poll(&mut TaskContext::from_waker(Waker::noop()));
            drop(operation);
            assert!(matches!(outcome, Poll::Ready(RaceEvent::TransportFailed)));
        }

        #[wasm_bindgen_test]
        fn response_egress_deadline_timer_failure_is_transport_failure() {
            assert_response_egress_timer_failure(false);
        }

        #[wasm_bindgen_test]
        fn response_egress_cancellation_during_timer_failure_keeps_precedence() {
            assert_response_egress_timer_failure(true);
        }

        fn assert_response_egress_timer_failure(cancel: bool) {
            let _timers = NumericTimeoutGuard::install();
            let controller = web_sys::AbortController::new().expect("abort controller");
            let signal: AbortSignal = controller.signal().into();
            let on_timer = Closure::<dyn FnMut()>::new(move || {
                if cancel {
                    controller.abort();
                }
            });
            let scope = global();
            let original =
                Reflect::get(&scope, &JsValue::from_str("setTimeout")).expect("timeout function");
            let failing = Function::new_with_args(
                "original, onTimer",
                "return (...args) => { if (args[1] > 1) { onTimer(); throw new Error('fixture timer failure'); } return original.apply(globalThis, args); };",
            )
            .call2(&JsValue::UNDEFINED, &original, on_timer.as_ref())
            .expect("failing deadline timer");
            assert!(
                Reflect::set(&scope, &JsValue::from_str("setTimeout"), &failing,)
                    .expect("install timer failure")
            );
            let start = MonotonicInstant::now();
            let clock = MonotonicClock::new(move || start);
            let polls = Rc::new(Cell::new(0_usize));
            let observed = Rc::clone(&polls);
            let resource = Rc::new(());
            let released = Rc::downgrade(&resource);
            let mut operation = Box::pin(race_operation(
                move || {
                    poll_fn(move |_context| {
                        assert_eq!(Rc::strong_count(&resource), 1);
                        observed.set(observed.get().checked_add(1).expect("poll count"));
                        Poll::<()>::Pending
                    })
                },
                EgressOperationKind::BodySource,
                clock.deadline_after(Duration::from_millis(10)),
                &clock,
                &signal,
                None,
            ));
            let outcome = operation
                .as_mut()
                .poll(&mut TaskContext::from_waker(Waker::noop()));
            let released_at_terminal = released.upgrade().is_none();
            drop(operation);
            assert!(matches!(
                (cancel, outcome),
                (true, Poll::Ready(RaceEvent::Cancelled))
                    | (false, Poll::Ready(RaceEvent::TransportFailed))
            ));
            assert_eq!(polls.get(), 0);
            assert!(released_at_terminal);
        }

        #[wasm_bindgen_test]
        async fn response_egress_terminal_observes_only_ready_native_promises() {
            let _timers = NumericTimeoutGuard::install();
            for expected in [
                IoFailureKind::Cancelled,
                IoFailureKind::Deadline,
                IoFailureKind::Transport,
            ] {
                for state in ["fulfilled", "pending", "rejected"] {
                    let controller = web_sys::AbortController::new().expect("abort controller");
                    let signal: AbortSignal = controller.signal().into();
                    let start = MonotonicInstant::now();
                    let now = Arc::new(Mutex::new(start));
                    let sample = Arc::clone(&now);
                    let clock = MonotonicClock::new(move || *sample.lock().expect("clock"));
                    let deadline = clock.deadline_after(Duration::from_secs(1));
                    let promise = match state {
                        "fulfilled" => Promise::resolve(&JsValue::TRUE),
                        "pending" => Promise::new(&mut |_resolve, _reject| {}),
                        "rejected" => Promise::reject(&JsValue::FALSE),
                        _ => panic!("unknown fixture promise state"),
                    };
                    let calls = Rc::new(Cell::new(0_usize));
                    let constructed = Rc::clone(&calls);
                    let mut operation = Box::pin(race_operation(
                        move || {
                            constructed.set(constructed.get().checked_add(1).expect("native call"));
                            JsFuture::from(promise)
                        },
                        EgressOperationKind::NativePromise,
                        deadline,
                        &clock,
                        &signal,
                        None,
                    ));
                    let mut context = TaskContext::from_waker(Waker::noop());
                    assert!(operation.as_mut().poll(&mut context).is_pending());
                    Delay::from(Duration::ZERO).await;
                    *now.lock().expect("write clock") = if expected == IoFailureKind::Transport {
                        start
                            .checked_sub(Duration::from_millis(1))
                            .expect("regressed clock")
                    } else {
                        deadline.instant()
                    };
                    if expected == IoFailureKind::Cancelled {
                        controller.abort();
                    }
                    let outcome = operation.as_mut().poll(&mut context);
                    drop(operation);
                    match (state, outcome) {
                        (
                            "fulfilled",
                            Poll::Ready(RaceEvent::Operation {
                                output: Ok(value),
                                terminal,
                            }),
                        ) => {
                            assert_eq!(value, JsValue::TRUE);
                            assert_eq!(terminal, Some(expected));
                        }
                        (
                            "rejected",
                            Poll::Ready(RaceEvent::Operation {
                                output: Err(value),
                                terminal,
                            }),
                        ) => {
                            assert_eq!(value, JsValue::FALSE);
                            assert_eq!(terminal, Some(expected));
                        }
                        ("pending", Poll::Ready(RaceEvent::Cancelled)) => {
                            assert_eq!(expected, IoFailureKind::Cancelled);
                        }
                        ("pending", Poll::Ready(RaceEvent::Deadline)) => {
                            assert_eq!(expected, IoFailureKind::Deadline);
                        }
                        ("pending", Poll::Ready(RaceEvent::TransportFailed)) => {
                            assert_eq!(expected, IoFailureKind::Transport);
                        }
                        _ => panic!("terminal observation changed native promise state"),
                    }
                    assert_eq!(
                        calls.get(),
                        1,
                        "terminal observation must not retry native I/O"
                    );
                }
            }
        }

        #[wasm_bindgen_test]
        fn response_egress_arms_only_budget_left_after_operation_factory() {
            let _timers = NumericTimeoutGuard::install();
            let scheduled = Array::new();
            let scope = global();
            let original =
                Reflect::get(&scope, &JsValue::from_str("setTimeout")).expect("timeout function");
            let intercept = Function::new_with_args(
                "original, scheduled",
                "return (...args) => { scheduled.push(args[1]); return original.apply(globalThis, args); };",
            )
            .call2(&JsValue::UNDEFINED, &original, &scheduled)
            .expect("record timeout durations");
            assert!(
                Reflect::set(&scope, &JsValue::from_str("setTimeout"), &intercept,)
                    .expect("install timeout observation")
            );
            let controller = web_sys::AbortController::new().expect("abort controller");
            let signal: AbortSignal = controller.signal().into();
            let start = MonotonicInstant::now();
            let until = start
                .checked_add(Duration::from_millis(10))
                .expect("deadline");
            let now = Arc::new(Mutex::new(start));
            let sample = Arc::clone(&now);
            let clock = MonotonicClock::new(move || *sample.lock().expect("clock"));
            let mut operation = Box::pin(race_operation(
                move || {
                    *now.lock().expect("factory clock") = start
                        .checked_add(Duration::from_millis(7))
                        .expect("factory cost");
                    future::pending::<()>()
                },
                EgressOperationKind::BodySource,
                Deadline::at_instant(until),
                &clock,
                &signal,
                None,
            ));
            assert!(
                operation
                    .as_mut()
                    .poll(&mut TaskContext::from_waker(Waker::noop()))
                    .is_pending()
            );
            drop(operation);
            assert_eq!(scheduled.to_vec(), vec![JsValue::from_f64(3.0)]);
        }

        fn observe_abort_listeners(signal: &AbortSignal) -> (Array, Array) {
            let added = Array::new();
            let removed = Array::new();
            Function::new_with_args(
                "signal, added, removed",
                "const add = signal.addEventListener, remove = signal.removeEventListener;
                 signal.addEventListener = function(type, callback, ...options) {
                     if (type === 'abort') added.push(callback);
                     return add.call(this, type, callback, ...options);
                 };
                 signal.removeEventListener = function(type, callback, ...options) {
                     if (type === 'abort') removed.push(callback);
                     return remove.call(this, type, callback, ...options);
                 };",
            )
            .call3(&JsValue::UNDEFINED, signal, &added, &removed)
            .expect("observe native listener ownership");
            (added, removed)
        }

        struct AbortWakeCounter(AtomicUsize);

        impl ArcWake for AbortWakeCounter {
            fn wake(self: Arc<Self>) {
                Self::wake_by_ref(&self);
            }

            fn wake_by_ref(arc_self: &Arc<Self>) {
                arc_self.0.fetch_add(1, Ordering::SeqCst);
            }
        }

        #[wasm_bindgen_test]
        fn response_egress_abort_wakes_pending_operation_without_timer_delivery() {
            let _timers = NumericTimeoutGuard::install();
            let controller = web_sys::AbortController::new().expect("controller");
            let signal: AbortSignal = controller.signal().into();
            let (added, removed) = observe_abort_listeners(&signal);
            let clock = MonotonicClock::default();
            let deadline = clock.deadline_after(Duration::from_secs(10));
            let mut operation = Box::pin(race_operation(
                future::pending::<()>,
                EgressOperationKind::BodySource,
                deadline,
                &clock,
                &signal,
                None,
            ));
            let first = Arc::new(AbortWakeCounter(AtomicUsize::new(0)));
            let second = Arc::new(AbortWakeCounter(AtomicUsize::new(0)));
            for counter in [&first, &second] {
                let waker = task_waker(Arc::clone(counter));
                assert!(
                    operation
                        .as_mut()
                        .poll(&mut TaskContext::from_waker(&waker))
                        .is_pending()
                );
            }
            assert_eq!(added.length(), 1);
            assert_eq!(removed.length(), 0);
            // A synthetic event is not cancellation and must not consume the listener.
            Function::new_with_args("signal", "signal.dispatchEvent(new Event('abort'));")
                .call1(&JsValue::UNDEFINED, &signal)
                .expect("synthetic abort event");
            let waker = task_waker(Arc::clone(&second));
            assert!(
                operation
                    .as_mut()
                    .poll(&mut TaskContext::from_waker(&waker))
                    .is_pending()
            );
            let before = second.0.load(Ordering::SeqCst);
            controller.abort();
            assert_eq!(first.0.load(Ordering::SeqCst), 0);
            assert_eq!(
                second.0.load(Ordering::SeqCst),
                before.checked_add(1).expect("abort wake")
            );
            assert!(matches!(
                operation
                    .as_mut()
                    .poll(&mut TaskContext::from_waker(&waker)),
                Poll::Ready(RaceEvent::Cancelled)
            ));
            assert_eq!(removed.to_vec(), added.to_vec());
            drop(operation);
            assert_eq!(removed.length(), 1);
        }

        #[wasm_bindgen_test]
        fn response_egress_abort_listener_releases_on_success_error_and_drop() {
            let _timers = NumericTimeoutGuard::install();
            for result in [Some(Ok(())), Some(Err(())), None] {
                let controller = web_sys::AbortController::new().expect("controller");
                let signal: AbortSignal = controller.signal().into();
                let (added, removed) = observe_abort_listeners(&signal);
                let clock = MonotonicClock::default();
                let deadline = clock.deadline_after(Duration::from_secs(10));
                let counter = Arc::new(AbortWakeCounter(AtomicUsize::new(0)));
                let retained = Arc::downgrade(&counter);
                let waker = task_waker(counter);
                let mut operation = Box::pin(race_operation(
                    || poll_fn(move |_| result.map_or(Poll::Pending, Poll::Ready)),
                    EgressOperationKind::BodySource,
                    deadline,
                    &clock,
                    &signal,
                    None,
                ));
                let observed = operation
                    .as_mut()
                    .poll(&mut TaskContext::from_waker(&waker));
                assert_eq!(observed.is_ready(), result.is_some());
                assert_eq!(added.length(), 1);
                if result.is_some() {
                    assert_eq!(removed.to_vec(), added.to_vec());
                }
                drop(operation);
                drop(waker);
                assert_eq!(removed.to_vec(), added.to_vec());
                assert!(retained.upgrade().is_none());
                controller.abort();
                assert_eq!(removed.length(), 1);
            }
        }

        #[wasm_bindgen_test]
        fn response_egress_abort_listener_failure_does_not_start_operation() {
            for expected in [
                IoFailureKind::Transport,
                IoFailureKind::Cancelled,
                IoFailureKind::Deadline,
            ] {
                let controller = web_sys::AbortController::new().expect("controller");
                let signal: AbortSignal = controller.signal().into();
                let (added, removed) = observe_abort_listeners(&signal);
                let start = MonotonicInstant::now();
                let until = start.checked_add(Duration::from_secs(1)).expect("deadline");
                let expired = Arc::new(AtomicBool::new(false));
                let sample = Arc::clone(&expired);
                let clock = MonotonicClock::new(move || {
                    if sample.load(Ordering::SeqCst) {
                        until
                    } else {
                        start
                    }
                });
                let on_add = Closure::<dyn FnMut()>::new(move || {
                    if expected == IoFailureKind::Cancelled {
                        controller.abort();
                    }
                    expired.store(expected == IoFailureKind::Deadline, Ordering::SeqCst);
                });
                Function::new_with_args(
                    "signal, notify",
                    "const add = signal.addEventListener;
                     signal.addEventListener = function(...args) {
                         add.apply(this, args); notify(); throw new Error('listener unavailable');
                     };",
                )
                .call2(&JsValue::UNDEFINED, &signal, on_add.as_ref())
                .expect("listener registration failure");
                let constructed = Rc::new(Cell::new(false));
                let observed = Rc::clone(&constructed);
                let mut operation = Box::pin(race_operation(
                    move || {
                        observed.set(true);
                        future::pending::<()>()
                    },
                    EgressOperationKind::BodySource,
                    Deadline::at_instant(until),
                    &clock,
                    &signal,
                    None,
                ));
                let event = operation
                    .as_mut()
                    .poll(&mut TaskContext::from_waker(Waker::noop()));
                assert!(matches!(
                    (expected, event),
                    (
                        IoFailureKind::Transport,
                        Poll::Ready(RaceEvent::TransportFailed)
                    ) | (IoFailureKind::Cancelled, Poll::Ready(RaceEvent::Cancelled))
                        | (IoFailureKind::Deadline, Poll::Ready(RaceEvent::Deadline))
                ));
                assert!(!constructed.get());
                assert_eq!(added.length(), 1);
                assert_eq!(removed.to_vec(), added.to_vec());
            }
        }

        #[wasm_bindgen_test]
        async fn response_egress_preflight_precedes_writer_operation() {
            let controller = web_sys::AbortController::new().expect("abort controller");
            controller.abort();
            let signal: AbortSignal = controller.signal().into();
            let now = MonotonicInstant::now();
            let deadline =
                Deadline::at_instant(now.checked_add(Duration::from_secs(1)).expect("deadline"));
            let clock = MonotonicClock::new(move || now);
            let constructed = Rc::new(Cell::new(false));
            let observed = Rc::clone(&constructed);

            let event = race_operation(
                move || {
                    observed.set(true);
                    future::ready(())
                },
                EgressOperationKind::BodySource,
                deadline,
                &clock,
                &signal,
                None,
            )
            .await;

            assert!(matches!(event, RaceEvent::Cancelled));
            assert!(!constructed.get());
        }
    }
}

#[cfg(all(feature = "cloudflare", target_arch = "wasm32"))]
pub(crate) use platform::{CloudflareEgressRuntime, from_egress_response};

#[cfg(test)]
mod tests {
    use edgezero_core::response_egress_framing::prepare_response_egress;
    use std::cell::{Cell, RefCell};
    use std::collections::VecDeque;
    use std::mem;
    use std::rc::Rc;
    use std::sync::{Arc, Mutex};
    use std::task::Poll;

    use edgezero_core::http::header::CONTENT_ENCODING;
    use edgezero_core::http::{Version, response_builder};
    use edgezero_core::response_egress::{
        ResponseEgressBodyKind, ResponseEgressCompletion, ResponseEgressHead,
        ResponseEgressObserver, ResponseEgressObserverHandle, ResponseEgressReport,
    };
    use futures::executor::block_on;
    use futures_util::stream::poll_fn;

    use super::*;

    #[derive(Clone, Default)]
    pub(super) struct RecordingObserver(Arc<Mutex<Vec<ResponseEgressReport>>>);

    impl RecordingObserver {
        pub(super) fn reports(&self) -> Vec<ResponseEgressReport> {
            self.0.lock().expect("reports lock").clone()
        }
    }

    impl ResponseEgressObserver for RecordingObserver {
        fn complete(&self, report: &ResponseEgressReport) {
            self.0.lock().expect("reports lock").push(report.clone());
        }
    }

    #[derive(Clone, Copy)]
    enum MockWrite {
        Accept,
        Fail { accepted: usize },
    }

    #[derive(Default)]
    struct MockState {
        aborts: usize,
        closes: usize,
        writes: Vec<Vec<u8>>,
    }

    struct MockIo {
        close_error: Option<IoFailureKind>,
        preflight_error: Option<IoFailureKind>,
        state: Rc<RefCell<MockState>>,
        writes: VecDeque<MockWrite>,
    }

    #[async_trait::async_trait(?Send)]
    impl CloudflareEgressIo for MockIo {
        fn abort(&mut self) {
            let mut state = self.state.borrow_mut();
            state.aborts = state.aborts.saturating_add(1);
        }

        async fn finish(
            &mut self,
            _deadline: Deadline,
            _clock: &MonotonicClock,
        ) -> Result<(), IoFailureKind> {
            self.state.borrow_mut().closes += 1;
            self.close_error.map_or(Ok(()), Err)
        }

        async fn next_body(
            &mut self,
            source: &mut BodyStream,
            _deadline: Deadline,
            _clock: &MonotonicClock,
            _attempt: &mut ResponseEgressAttempt,
        ) -> Result<Option<Result<Bytes, EdgeError>>, IoFailureKind> {
            Ok(source.next().await)
        }

        fn preflight(
            &self,
            _deadline: Deadline,
            _clock: &MonotonicClock,
        ) -> Result<(), IoFailureKind> {
            self.preflight_error.map_or(Ok(()), Err)
        }

        async fn write(
            &mut self,
            bytes: Vec<u8>,
            _deadline: Deadline,
            _clock: &MonotonicClock,
        ) -> Result<usize, IoFailure> {
            self.state.borrow_mut().writes.push(bytes.clone());
            match self.writes.pop_front().unwrap_or(MockWrite::Accept) {
                MockWrite::Accept => Ok(bytes.len()),
                MockWrite::Fail { accepted } => Err(IoFailure {
                    accepted,
                    kind: IoFailureKind::Transport,
                }),
            }
        }
    }

    #[derive(Debug)]
    struct MockResponse {
        declared_length: Option<u64>,
        headers: HeaderMap,
        status: StatusCode,
        transmits_body: bool,
    }

    struct MockCommitter {
        close_error: Option<IoFailureKind>,
        preflight_error: Option<IoFailureKind>,
        prepare_error: Option<DeliveryError>,
        state: Rc<RefCell<MockState>>,
        writes: VecDeque<MockWrite>,
    }

    impl MockCommitter {
        fn new() -> (Self, Rc<RefCell<MockState>>) {
            let state = Rc::new(RefCell::new(MockState::default()));
            (
                Self {
                    close_error: None,
                    preflight_error: None,
                    prepare_error: None,
                    state: Rc::clone(&state),
                    writes: VecDeque::new(),
                },
                state,
            )
        }
    }

    impl CloudflareEgressCommitter for MockCommitter {
        type Io = MockIo;
        type Response = MockResponse;

        fn prepare(
            &mut self,
            status: StatusCode,
            headers: &HeaderMap,
            declared_length: Option<u64>,
            transmits_body: bool,
        ) -> Result<(Self::Response, Self::Io), DeliveryError> {
            if let Some(error) = self.prepare_error.take() {
                return Err(error);
            }
            Ok((
                MockResponse {
                    declared_length,
                    headers: headers.clone(),
                    status,
                    transmits_body,
                },
                MockIo {
                    close_error: self.close_error,
                    preflight_error: self.preflight_error,
                    state: Rc::clone(&self.state),
                    writes: mem::take(&mut self.writes),
                },
            ))
        }

        fn spawn(&mut self, task: LocalBoxFuture<'static, ()>) {
            block_on(task);
        }
    }

    fn attempt(
        observer: &RecordingObserver,
        started_at: MonotonicInstant,
    ) -> ResponseEgressAttempt {
        let headers = HeaderMap::new();
        let head = ResponseEgressHead::new(
            StatusCode::OK,
            Version::HTTP_11,
            &headers,
            None,
            started_at,
            None,
        );
        ResponseEgressAttempt::new(
            &head,
            started_at,
            ResponseEgressCompletion::empty(),
            ResponseEgressObserverHandle::new(observer.clone()),
            MonotonicClock::default(),
        )
    }

    fn prepared(method: &Method, body: Body) -> PreparedResponseEgress {
        prepare_response_egress(
            ResponseEgressFramingMode::ExplicitBufferedLength,
            method,
            response_builder()
                .status(StatusCode::OK)
                .body(body)
                .expect("response"),
        )
        .expect("prepared response")
    }

    fn policy(deadline: MonotonicInstant) -> ResponseEgressPolicy {
        ResponseEgressPolicy {
            write_deadline: Deadline::at_instant(deadline),
        }
    }

    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test::wasm_bindgen_test)]
    #[cfg_attr(not(target_arch = "wasm32"), test)]
    fn framing_observer_preserves_each_typed_reason() {
        use edgezero_core::response_egress_framing::test_support::{
            FRAMING_RULES, framing_fixture, has_full_synthesized_fixture_headers,
        };
        for reason in FRAMING_RULES {
            for detached in [false, true] {
                let fixture = block_on(framing_fixture(reason, detached)).expect("fixture");
                let method = fixture.envelope.request_method().clone();
                let Err(failure) = fixture
                    .envelope
                    .begin(ResponseEgressFramingMode::ExplicitBufferedLength)
                else {
                    panic!("framing rejection")
                };
                let (attempt, clock, cause) = failure.into_parts();
                let (mut committer, state) = MockCommitter::new();
                let response = start_fallback(&method, cause, attempt, &clock, &mut committer)
                    .expect("fallback");
                assert_eq!(response.status, StatusCode::INTERNAL_SERVER_ERROR);
                assert_eq!(response.headers["x-synthesized-fixture"], "configured");
                assert!(has_full_synthesized_fixture_headers(&response.headers));
                assert_eq!(state.borrow().writes.concat(), INTERNAL_FALLBACK_BODY);
                let completed = fixture.completed.lock().expect("completion");
                assert_eq!(completed.len(), 1);
                assert_eq!(completed[0].framing_failures.application(), Some(reason));
                assert_eq!(completed[0].framing_failures.fallback(), None);
                assert_eq!(completed[0].outcome, ResponseEgressOutcome::ConversionError);
                let observed = fixture.observed.lock().expect("observer");
                if detached {
                    assert!(observed.is_empty());
                } else {
                    assert_eq!(*observed, *completed);
                }
            }
        }
    }

    #[cfg(all(not(target_arch = "wasm32"), panic = "unwind"))]
    #[test]
    fn policy_panic_keeps_framing_reasons_empty() {
        use edgezero_core::response_egress_framing::test_support::policy_panic_fixture;

        for method in [Method::GET, Method::HEAD] {
            let head = method == Method::HEAD;
            let fixture = policy_panic_fixture(method.clone());
            let Err(failure) = fixture
                .envelope
                .begin(ResponseEgressFramingMode::ExplicitBufferedLength)
            else {
                panic!("policy rejection");
            };
            let (attempt, clock, cause) = failure.into_parts();
            assert_eq!(cause, ResponseEgressOutcome::ConversionError);
            assert!(fixture.completed.lock().expect("completion").is_empty());
            assert!(fixture.observed.lock().expect("observer").is_empty());
            let (mut committer, state) = MockCommitter::new();
            let response =
                start_fallback(&method, cause, attempt, &clock, &mut committer).expect("fallback");
            assert_eq!(response.status, StatusCode::INTERNAL_SERVER_ERROR);
            assert_eq!(
                state.borrow().writes.concat(),
                if head {
                    b"".as_slice()
                } else {
                    INTERNAL_FALLBACK_BODY
                }
            );
            assert_eq!(state.borrow().aborts, 0);
            let completed = fixture.completed.lock().expect("completed").clone();
            assert_eq!(completed.len(), 1);
            assert_eq!(*fixture.observed.lock().expect("observed"), completed);
            let report = &completed[0];
            assert_eq!(report.outcome, ResponseEgressOutcome::ConversionError);
            assert_eq!(report.body_kind, ResponseEgressBodyKind::Fallback);
            assert_eq!(
                report.fallback_disposition,
                Some(ResponseEgressFallbackDisposition::Completed)
            );
            assert_eq!(report.bytes_written, if head { 0 } else { 21 });
            assert_eq!(report.framing_failures.application(), None);
            assert_eq!(report.framing_failures.fallback(), None);
            assert!(report.source_failure.is_none());
        }
    }

    #[test]
    fn provider_diagnostics_reach_response_observer_before_deadline_arbitration() {
        use edgezero_core::error::BadGatewayReason;
        use edgezero_core::error_detail::{
            CloudflareFailureCause, ProviderFailureDetail, ProviderFailurePhase,
        };
        use std::sync::atomic::{AtomicBool, Ordering};

        let detail = ProviderFailureDetail::Cloudflare {
            phase: ProviderFailurePhase::ResponseBodyRead,
            cause: CloudflareFailureCause::TypeError,
        };
        for with_detail in [false, true] {
            for late in [false, true] {
                let now = MonotonicInstant::now();
                let deadline = now.checked_add(Duration::from_secs(10)).expect("deadline");
                let expired = Arc::new(AtomicBool::new(false));
                let clock_expired = Arc::clone(&expired);
                let clock = MonotonicClock::new(move || {
                    if clock_expired.load(Ordering::SeqCst) {
                        deadline
                    } else {
                        now
                    }
                });
                let error = if with_detail {
                    EdgeError::bad_gateway_with_detail(
                        "provider-secret-token",
                        BadGatewayReason::Transport,
                        detail,
                    )
                } else {
                    EdgeError::bad_gateway_with_reason(
                        "provider-secret-token",
                        BadGatewayReason::Transport,
                    )
                };
                let body = Body::from_stream(stream::once(async move {
                    expired.store(late, Ordering::SeqCst);
                    Err(error)
                }));
                let observer = RecordingObserver::default();
                let mut attempt = attempt(&observer, now);
                assert!(attempt.begin_writing());
                let (committer, state) = MockCommitter::new();
                let io = MockIo {
                    close_error: None,
                    preflight_error: None,
                    state: Rc::clone(&state),
                    writes: committer.writes,
                };
                let result = block_on(transmit_committed(
                    body,
                    policy(deadline),
                    attempt,
                    clock,
                    EgressKind::Application,
                    io,
                ));
                assert!(result.is_err());
                let reports = observer.reports();
                assert_eq!(reports.len(), 1);
                assert_eq!(
                    reports[0].outcome,
                    if late {
                        ResponseEgressOutcome::DeadlineExceeded
                    } else {
                        ResponseEgressOutcome::SourceError
                    }
                );
                let failure = reports[0]
                    .source_failure
                    .as_ref()
                    .expect("source observation");
                assert_eq!(reports[0].framing_failures.application(), None);
                assert_eq!(reports[0].framing_failures.fallback(), None);
                assert_eq!(failure.category(), "bad_gateway_transport");
                assert_eq!(
                    failure.provider_failure_detail(),
                    with_detail.then_some(&detail)
                );
                assert!(!format!("{failure:?}").contains("secret"));
                assert_eq!(state.borrow().aborts, 1);
            }
        }
    }

    #[test]
    fn once_and_multi_chunk_bodies_account_only_accepted_writes() {
        for (body, declared_length) in [
            (Body::from(Bytes::from_static(b"once")), Some(4)),
            (Body::empty(), Some(0)),
            (
                Body::stream(stream::iter([
                    Bytes::from_static(b"one"),
                    Bytes::from_static(b"two"),
                ])),
                None,
            ),
        ] {
            let now = MonotonicInstant::now();
            let deadline = now.checked_add(Duration::from_secs(10)).expect("deadline");
            let clock = MonotonicClock::new(move || now);
            let observer = RecordingObserver::default();
            let (mut committer, state) = MockCommitter::new();
            let started = start_prepared(
                prepared(&Method::GET, body),
                policy(deadline),
                attempt(&observer, now),
                clock,
                EgressKind::Application,
                &mut committer,
            );
            let StartOutcome::Started(response) = started else {
                panic!("response must start");
            };
            assert!(response.transmits_body);
            assert_eq!(response.declared_length, declared_length);
            assert_eq!(state.borrow().closes, 1);
            let expected = state.borrow().writes.iter().map(Vec::len).sum::<usize>();
            let reports = observer.reports();
            assert_eq!(reports.len(), 1);
            assert_eq!(
                reports[0].bytes_written,
                u64::try_from(expected).expect("byte count")
            );
            assert_eq!(reports[0].outcome, ResponseEgressOutcome::HostHandoff);
        }
    }

    #[test]
    fn partial_rejection_accounts_prefix_and_aborts_once() {
        let now = MonotonicInstant::now();
        let deadline = now.checked_add(Duration::from_secs(10)).expect("deadline");
        let clock = MonotonicClock::new(move || now);
        let observer = RecordingObserver::default();
        let (mut committer, state) = MockCommitter::new();
        committer.writes.push_back(MockWrite::Fail { accepted: 2 });
        let started = start_prepared(
            prepared(&Method::GET, Body::from(Bytes::from_static(b"body"))),
            policy(deadline),
            attempt(&observer, now),
            clock.clone(),
            EgressKind::Application,
            &mut committer,
        );
        assert!(matches!(started, StartOutcome::Started(_)));
        assert_eq!(state.borrow().aborts, 1);
        let reports = observer.reports();
        assert_eq!(reports.len(), 1);
        assert_eq!(reports[0].bytes_written, 2);
        assert_eq!(reports[0].outcome, ResponseEgressOutcome::TransportError);
    }

    #[test]
    fn head_suppresses_source_without_polling() {
        let now = MonotonicInstant::now();
        let deadline = now.checked_add(Duration::from_secs(10)).expect("deadline");
        let clock = MonotonicClock::new(move || now);
        let observer = RecordingObserver::default();
        let polled = Rc::new(Cell::new(false));
        let observed_source = Rc::clone(&polled);
        let source = poll_fn(move |_context| {
            observed_source.set(true);
            Poll::Ready(Some(Bytes::from_static(b"hidden")))
        });
        let (mut committer, state) = MockCommitter::new();
        let started = start_prepared(
            prepared(&Method::HEAD, Body::stream(source)),
            policy(deadline),
            attempt(&observer, now),
            clock,
            EgressKind::Application,
            &mut committer,
        );
        let StartOutcome::Started(response) = started else {
            panic!("HEAD response must start");
        };
        assert!(!response.transmits_body);
        assert!(!polled.get());
        assert!(state.borrow().writes.is_empty());
        assert_eq!(observer.reports()[0].bytes_written, 0);
    }

    #[test]
    fn bodyless_response_checks_cancel_and_deadline_before_handoff() {
        let now = MonotonicInstant::now();
        let deadline = now.checked_add(Duration::from_secs(10)).expect("deadline");
        let clock = MonotonicClock::new(move || now);
        let observer = RecordingObserver::default();
        let (mut committer, state) = MockCommitter::new();
        committer.preflight_error = Some(IoFailureKind::Cancelled);

        let StartOutcome::Precommit { attempt, cause } = start_prepared(
            prepared(&Method::HEAD, Body::empty()),
            policy(deadline),
            attempt(&observer, now),
            clock.clone(),
            EgressKind::Application,
            &mut committer,
        ) else {
            panic!("cancelled request must not be handed off");
        };

        assert_eq!(cause, ResponseEgressOutcome::RequestCancelled);
        assert_eq!(
            start_fallback(&Method::HEAD, cause, *attempt, &clock, &mut committer)
                .expect_err("cancelled request cannot start fallback delivery"),
            DeliveryError::Cancelled
        );
        assert_eq!(state.borrow().aborts, 1);
        assert_eq!(observer.reports()[0].outcome, cause);
    }

    #[test]
    fn generated_overflow_retry_after_precedes_synthesized_headers() {
        use edgezero_core::RequestBodyOverflowOrigin;
        use edgezero_core::response_egress_framing::test_support::synthesized_error_fixture;
        for (error, retry) in [
            (
                EdgeError::payload_too_large(RequestBodyOverflowOrigin::Inbound),
                None,
            ),
            (
                EdgeError::payload_too_large(RequestBodyOverflowOrigin::OutboundUpload),
                None,
            ),
            (
                EdgeError::config_out_of_date("private", "field"),
                Some("60"),
            ),
        ] {
            let envelope = synthesized_error_fixture(error).expect("fixture");
            let Ok((prepared, policy, attempt, clock)) =
                envelope.begin(ResponseEgressFramingMode::ExplicitBufferedLength)
            else {
                panic!("prepared error");
            };
            let (mut committer, _) = MockCommitter::new();
            let StartOutcome::Started(response) = start_prepared(
                prepared,
                policy,
                attempt,
                clock,
                EgressKind::Application,
                &mut committer,
            ) else {
                panic!("started error");
            };
            assert_eq!(response.headers["x-synthesized-fixture"], "configured");
            assert_eq!(
                response
                    .headers
                    .get("retry-after")
                    .map(|value| value.to_str().expect("text")),
                retry
            );
        }
    }

    #[test]
    fn precommit_deadline_uses_fallback_and_preserves_original_cause() {
        let now = MonotonicInstant::now();
        let clock = MonotonicClock::new(move || now);
        let observer = RecordingObserver::default();
        let (mut committer, _state) = MockCommitter::new();
        let StartOutcome::Precommit { attempt, cause } = start_prepared(
            prepared(&Method::GET, Body::from(Bytes::from_static(b"late"))),
            policy(now),
            attempt(&observer, now),
            clock.clone(),
            EgressKind::Application,
            &mut committer,
        ) else {
            panic!("deadline must fail before commit");
        };
        assert_eq!(cause, ResponseEgressOutcome::DeadlineExceeded);
        let response = start_fallback(&Method::GET, cause, *attempt, &clock, &mut committer)
            .expect("fallback response");
        assert_eq!(response.status, StatusCode::GATEWAY_TIMEOUT);
        let reports = observer.reports();
        assert_eq!(reports.len(), 1);
        assert_eq!(reports[0].body_kind, ResponseEgressBodyKind::Fallback);
        assert_eq!(reports[0].outcome, ResponseEgressOutcome::DeadlineExceeded);
        assert_eq!(
            reports[0].fallback_disposition,
            Some(ResponseEgressFallbackDisposition::Completed)
        );
    }

    #[test]
    fn fallback_failure_preserves_original_conversion_cause() {
        let now = MonotonicInstant::now();
        let clock = MonotonicClock::new(move || now);
        let observer = RecordingObserver::default();
        let (mut committer, _state) = MockCommitter::new();
        committer.prepare_error = Some(DeliveryError::Conversion);
        let result = start_fallback(
            &Method::GET,
            ResponseEgressOutcome::ConversionError,
            attempt(&observer, now),
            &clock,
            &mut committer,
        );
        assert_eq!(
            result.expect_err("fallback must fail"),
            DeliveryError::Conversion
        );
        let reports = observer.reports();
        assert_eq!(reports.len(), 1);
        assert_eq!(reports[0].outcome, ResponseEgressOutcome::ConversionError);
        assert_eq!(
            reports[0].fallback_disposition,
            Some(ResponseEgressFallbackDisposition::Aborted)
        );
        assert_eq!(reports[0].framing_failures.application(), None);
        assert_eq!(reports[0].framing_failures.fallback(), None);
    }

    #[test]
    fn source_and_close_failures_have_distinct_terminal_outcomes() {
        let cases = [
            (
                Body::from_stream(stream::once(async {
                    Err(EdgeError::internal(anyhow::anyhow!("source")))
                })),
                None,
                ResponseEgressOutcome::SourceError,
            ),
            (
                Body::from(Bytes::new()),
                Some(IoFailureKind::Transport),
                ResponseEgressOutcome::TransportError,
            ),
            (
                Body::from(Bytes::new()),
                Some(IoFailureKind::Deadline),
                ResponseEgressOutcome::DeadlineExceeded,
            ),
            (
                Body::from(Bytes::new()),
                Some(IoFailureKind::Cancelled),
                ResponseEgressOutcome::RequestCancelled,
            ),
        ];
        for (body, close_error, expected) in cases {
            let now = MonotonicInstant::now();
            let deadline = now.checked_add(Duration::from_secs(10)).expect("deadline");
            let clock = MonotonicClock::new(move || now);
            let observer = RecordingObserver::default();
            let (mut committer, _state) = MockCommitter::new();
            committer.close_error = close_error;
            let started = start_prepared(
                prepared(&Method::GET, body),
                policy(deadline),
                attempt(&observer, now),
                clock,
                EgressKind::Application,
                &mut committer,
            );
            assert!(matches!(started, StartOutcome::Started(_)));
            let reports = observer.reports();
            assert_eq!(reports[0].outcome, expected);
            if expected == ResponseEgressOutcome::DeadlineExceeded {
                assert_eq!(reports[0].elapsed, Duration::from_secs(10));
            }
        }
    }

    #[test]
    fn preencoded_payload_uses_manual_worker_encoding() {
        let mut headers = HeaderMap::new();
        assert!(!manual_response_encoding(&headers, true));
        headers.insert(CONTENT_ENCODING, "zstd".parse().expect("encoding"));
        assert!(manual_response_encoding(&headers, true));
        assert!(!manual_response_encoding(&headers, false));
    }
}
