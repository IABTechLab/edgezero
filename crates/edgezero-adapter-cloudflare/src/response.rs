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
    use std::future::Future;
    use std::sync::Arc;

    use edgezero_core::response_egress::{ResponseEgressAttempt, ResponseEgressEnvelope};
    use futures_util::FutureExt as _;
    use futures_util::StreamExt as _;
    use futures_util::future::{Either, select};
    use worker::js_sys::{BigInt, Uint8Array};
    use worker::wasm_bindgen_futures::JsFuture;
    use worker::worker_sys::FixedLengthStream;
    use worker::{AbortSignal, Context, Delay, EncodeBody, Response as CfResponse};

    use crate::outbound::copy_header_values;

    const ABORT_POLL_INTERVAL: Duration = Duration::from_millis(1);

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
        Operation {
            output: Output,
            terminal: Option<IoFailureKind>,
        },
    }

    async fn wait_for_abort(signal: AbortSignal) {
        while !signal.aborted() {
            Delay::from(ABORT_POLL_INTERVAL).await;
        }
    }

    async fn race_operation<OperationFactory, Operation>(
        operation: OperationFactory,
        deadline: Deadline,
        clock: &MonotonicClock,
        signal: &AbortSignal,
    ) -> RaceEvent<Operation::Output>
    where
        OperationFactory: FnOnce() -> Operation,
        Operation: Future,
    {
        if signal.aborted() {
            return RaceEvent::Cancelled;
        }
        let Some(remaining) = deadline.remaining_at(clock.now()) else {
            return RaceEvent::Deadline;
        };
        let cancellation = wait_for_abort(signal.clone());
        let timer = Delay::from(remaining);
        let guard = async move {
            futures_util::pin_mut!(cancellation, timer);
            match select(cancellation, timer).await {
                Either::Left(((), _timer)) => RaceEvent::Cancelled,
                Either::Right(((), _cancellation)) => RaceEvent::Deadline,
            }
        };
        let operation_future = operation();
        futures_util::pin_mut!(guard, operation_future);
        match select(guard, operation_future).await {
            Either::Left((event, _operation)) => event,
            Either::Right((output, _guard)) => {
                let terminal = if signal.aborted() {
                    Some(IoFailureKind::Cancelled)
                } else if deadline.is_expired_at(clock.now()) {
                    Some(IoFailureKind::Deadline)
                } else {
                    None
                };
                RaceEvent::Operation { output, terminal }
            }
        }
    }

    #[async_trait::async_trait(?Send)]
    impl CloudflareEgressIo for WorkersEgressIo {
        fn abort(&mut self) {
            if let Some(writer) = self.writer.take() {
                drop(writer.abort());
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
                deadline,
                clock,
                &self.signal,
            )
            .await;
            let result = match event {
                RaceEvent::Cancelled => Err(IoFailureKind::Cancelled),
                RaceEvent::Deadline => Err(IoFailureKind::Deadline),
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
            if result.is_ok()
                && let Some(finished_writer) = self.writer.take()
            {
                finished_writer.release_lock();
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
                deadline,
                clock,
                &self.signal,
            )
            .await
            {
                RaceEvent::Cancelled => Err(IoFailureKind::Cancelled),
                RaceEvent::Deadline => Err(IoFailureKind::Deadline),
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
                deadline,
                clock,
                &self.signal,
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

        use edgezero_core::error::BadGatewayReason;
        use edgezero_core::error_detail::{
            CloudflareFailureCause, ProviderFailureDetail, ProviderFailurePhase,
        };
        use edgezero_core::http::Version;
        use edgezero_core::response_egress::{
            ResponseEgressBodyKind, ResponseEgressCompletion, ResponseEgressHead,
            ResponseEgressObserverHandle, ResponseEgressOutcome, ResponseEgressReport,
        };
        use edgezero_core::time::MonotonicInstant;
        use futures_util::{future, stream};
        use wasm_bindgen_test::wasm_bindgen_test;

        use super::*;
        use crate::test_runtime::NumericTimeoutGuard;

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
                deadline,
                &clock,
                &signal,
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
    struct RecordingObserver(Arc<Mutex<Vec<ResponseEgressReport>>>);

    impl RecordingObserver {
        fn reports(&self) -> Vec<ResponseEgressReport> {
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
