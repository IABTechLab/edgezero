#![cfg_attr(
    feature = "fastly",
    expect(
        clippy::arbitrary_source_item_ordering,
        reason = "portable protocol traits precede the gated Fastly ABI lifecycle implementation"
    )
)]

#[cfg(feature = "fastly")]
#[path = "fastly_response_abi.rs"]
mod fastly_abi;

#[cfg(feature = "fastly")]
use core::convert::identity;
use std::task::Poll;

use bytes::Bytes;
use edgezero_core::body::{Body, BodyPayload, BodyStream};
use edgezero_core::error::EdgeError;
use edgezero_core::http::{HeaderMap, Method, Response, StatusCode, Uri};
use edgezero_core::outbound::{OutboundFuture, OutboundTime};
use edgezero_core::response_egress::{
    RESPONSE_EGRESS_FALLBACK_SAFETY_BUDGET, ResponseEgressAttempt, ResponseEgressEnvelope,
    ResponseEgressFallbackDisposition, ResponseEgressOutcome, ResponseEgressPolicy,
};
use edgezero_core::response_egress_framing::ResponseEgressFramingMode;
use edgezero_core::response_egress_framing::{
    PreparedResponseEgress, response_egress_source_error_category,
};
use edgezero_core::time::{Deadline, MonotonicClock, MonotonicInstant};
use futures::executor;
use futures_util::StreamExt as _;
use futures_util::future::poll_fn;

use crate::delivery::{DeliveryError, FastlyResponseCommitState, FastlyResponseDelivery};
#[cfg(feature = "fastly")]
use crate::outbound::{drive_callback, native_time};

const INTERNAL_FALLBACK_BODY: &[u8] = b"internal server error";
const TIMEOUT_FALLBACK_BODY: &[u8] = b"response write deadline exceeded";

struct BufferedBody<'body> {
    bytes: &'body [u8],
    clock: &'body MonotonicClock,
    deadline: Deadline,
}

impl BufferedBody<'_> {
    fn stage<Error>(
        &self,
        mut write: impl FnMut(&[u8]) -> Result<usize, Error>,
    ) -> Result<(), DeliveryError> {
        let mut remaining = self.bytes;
        while !remaining.is_empty() {
            ensure_deadline(self.deadline, self.clock)?;
            let result = write(remaining);
            ensure_deadline(self.deadline, self.clock)?;
            let written = result.map_err(|_error| DeliveryError::Transport)?;
            if written == 0 || written > remaining.len() {
                return Err(DeliveryError::Transport);
            }
            remaining = remaining.get(written..).ok_or(DeliveryError::Transport)?;
        }
        Ok(())
    }
}

#[derive(Debug)]
enum StreamRead {
    Source(Option<Result<Bytes, EdgeError>>),
    WriteDeadline,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TransmitFailure {
    Postcommit(DeliveryError),
    Precommit {
        cause: ResponseEgressOutcome,
        error: DeliveryError,
    },
    Uncommitted(DeliveryError),
    Unknown(DeliveryError),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TransmitKind {
    Application,
    Fallback,
}

trait EgressSink {
    type Error;

    fn abandon(&mut self) -> Result<(), Self::Error>;
    fn finish(&mut self) -> Result<(), Self::Error>;
    fn write(&mut self, bytes: &[u8]) -> Result<usize, Self::Error>;
}

trait EgressCommitter {
    type Error;
    type Prepared;
    type Sink: EgressSink;

    fn commit(&mut self, prepared: Self::Prepared) -> Result<Self::Sink, Self::Error>;
    fn prepare(
        &mut self,
        status: StatusCode,
        headers: &HeaderMap,
        transmits_body: bool,
        buffered_body: Option<BufferedBody<'_>>,
    ) -> Result<Self::Prepared, DeliveryError>;
}

#[cfg(feature = "fastly")]
mod platform {
    use super::{
        BufferedBody, DeliveryError, EgressCommitter, EgressSink, FastlyResponseDelivery,
        HeaderMap, ResponseEgressEnvelope, StatusCode, manual_stream_framing, send_egress_with,
    };
    use std::mem;

    use fastly_shared::{INVALID_BODY_HANDLE, INVALID_RESPONSE_HANDLE};
    use fastly_sys::{BodyHandle as FastlyBodyHandle, ResponseHandle as FastlyResponseHandle};

    use super::fastly_abi::{self, FastlyAbiError};

    pub(super) struct FastlyPrepared {
        body: FastlyBodyHandle,
        response: FastlyResponseHandle,
        streaming: bool,
    }

    impl FastlyPrepared {
        fn commit(mut self) -> Result<FastlyBodySink, FastlyAbiError> {
            let response = mem::replace(&mut self.response, INVALID_RESPONSE_HANDLE);
            let body = mem::replace(&mut self.body, INVALID_BODY_HANDLE);
            if self.streaming {
                if fastly_abi::send_streaming(response, body).is_err() {
                    let _result = fastly_abi::abandon_body(body);
                    return Err(FastlyAbiError);
                }
                Ok(FastlyBodySink(body))
            } else {
                fastly_abi::send_buffered(response, body)?;
                Ok(FastlyBodySink(INVALID_BODY_HANDLE))
            }
        }

        fn new(
            status: StatusCode,
            headers: &HeaderMap,
            transmits_body: bool,
            buffered_body: Option<BufferedBody<'_>>,
        ) -> Result<Self, DeliveryError> {
            let response =
                fastly_abi::new_response().map_err(|_error| DeliveryError::Conversion)?;
            let mut prepared = Self {
                body: INVALID_BODY_HANDLE,
                response,
                streaming: buffered_body.is_none(),
            };
            prepared.body = fastly_abi::new_body().map_err(|_error| DeliveryError::Conversion)?;
            fastly_abi::set_status(prepared.response, status.as_u16())
                .map_err(|_error| DeliveryError::Conversion)?;
            for (name, value) in headers {
                fastly_abi::append_header(
                    prepared.response,
                    name.as_str().as_bytes(),
                    value.as_bytes(),
                )
                .map_err(|_error| DeliveryError::Conversion)?;
            }
            if manual_stream_framing(headers, transmits_body, prepared.streaming) {
                fastly_abi::set_manual_framing(prepared.response)
                    .map_err(|_error| DeliveryError::Conversion)?;
            }
            if let Some(buffered) = buffered_body {
                buffered.stage(|remaining| fastly_abi::write_body(prepared.body, remaining))?;
            }
            Ok(prepared)
        }
    }

    #[expect(
        clippy::missing_trait_methods,
        reason = "`Drop::pin_drop` is unstable (`pin_ergonomics`) and conflicts with `drop`"
    )]
    impl Drop for FastlyPrepared {
        fn drop(&mut self) {
            if self.body != INVALID_BODY_HANDLE {
                let _result = fastly_abi::close_body(self.body);
                self.body = INVALID_BODY_HANDLE;
            }
            if self.response != INVALID_RESPONSE_HANDLE {
                let _result = fastly_abi::close_response(self.response);
                self.response = INVALID_RESPONSE_HANDLE;
            }
        }
    }

    pub(super) struct FastlyBodySink(FastlyBodyHandle);

    #[expect(
        clippy::missing_trait_methods,
        reason = "`Drop::pin_drop` is unstable (`pin_ergonomics`) and conflicts with `drop`"
    )]
    impl Drop for FastlyBodySink {
        fn drop(&mut self) {
            if self.0 != INVALID_BODY_HANDLE {
                let _result = fastly_abi::abandon_body(self.0);
                self.0 = INVALID_BODY_HANDLE;
            }
        }
    }

    impl EgressSink for FastlyBodySink {
        type Error = FastlyAbiError;

        fn abandon(&mut self) -> Result<(), Self::Error> {
            if self.0 == INVALID_BODY_HANDLE {
                return Ok(());
            }
            let handle = mem::replace(&mut self.0, INVALID_BODY_HANDLE);
            fastly_abi::abandon_body(handle)
        }

        fn finish(&mut self) -> Result<(), Self::Error> {
            if self.0 == INVALID_BODY_HANDLE {
                return Err(FastlyAbiError);
            }
            let handle = mem::replace(&mut self.0, INVALID_BODY_HANDLE);
            fastly_abi::close_body(handle)
        }

        fn write(&mut self, bytes: &[u8]) -> Result<usize, Self::Error> {
            if self.0 == INVALID_BODY_HANDLE {
                return Err(FastlyAbiError);
            }
            fastly_abi::write_body(self.0, bytes)
        }
    }

    pub(super) struct FastlyCommitter;

    impl EgressCommitter for FastlyCommitter {
        type Error = FastlyAbiError;
        type Prepared = FastlyPrepared;
        type Sink = FastlyBodySink;

        fn commit(&mut self, prepared: Self::Prepared) -> Result<Self::Sink, Self::Error> {
            prepared.commit()
        }

        fn prepare(
            &mut self,
            status: StatusCode,
            headers: &HeaderMap,
            transmits_body: bool,
            buffered_body: Option<BufferedBody<'_>>,
        ) -> Result<Self::Prepared, DeliveryError> {
            FastlyPrepared::new(status, headers, transmits_body, buffered_body)
        }
    }

    /// Sends buffers atomically and streams through an owned downstream body handle.
    ///
    /// Returns its exact terminal report and last-body commitment. Precommit conversion and
    /// deadline failures first attempt the bounded fallback on the same response-egress attempt.
    pub(crate) fn send_egress_response(egress: ResponseEgressEnvelope) -> FastlyResponseDelivery {
        send_egress_with(egress, &mut FastlyCommitter)
    }
}

#[cfg(feature = "fastly")]
pub(crate) use platform::send_egress_response;

fn send_egress_with<Committer>(
    egress: ResponseEgressEnvelope,
    committer: &mut Committer,
) -> FastlyResponseDelivery
where
    Committer: EgressCommitter,
{
    let request_method = egress.request_method().clone();
    let (attempt, transmit_result) = match egress.begin(ResponseEgressFramingMode::HostAutomatic) {
        Ok((prepared, policy, mut attempt, clock)) => {
            let result = match transmit_prepared(
                prepared,
                policy,
                &mut attempt,
                &clock,
                TransmitKind::Application,
                committer,
            ) {
                Err(TransmitFailure::Precommit { cause, .. }) => {
                    send_fallback(&request_method, cause, &mut attempt, &clock, committer)
                }
                result => result,
            };
            (attempt, result)
        }
        Err(failure) => {
            let (mut attempt, clock, cause) = failure.into_parts();
            let result = send_fallback(&request_method, cause, &mut attempt, &clock, committer);
            (attempt, result)
        }
    };
    let (commitment, delivery_result) = match transmit_result {
        Ok(()) => (FastlyResponseCommitState::Committed, Ok(())),
        Err(TransmitFailure::Postcommit(error)) => {
            (FastlyResponseCommitState::Committed, Err(error))
        }
        Err(TransmitFailure::Unknown(error)) => (FastlyResponseCommitState::Unknown, Err(error)),
        Err(TransmitFailure::Uncommitted(error) | TransmitFailure::Precommit { error, .. }) => {
            (FastlyResponseCommitState::NotCommitted, Err(error))
        }
    };
    FastlyResponseDelivery::new(commitment, attempt.into_terminal_report(), delivery_result)
}

fn send_fallback<Committer>(
    request_method: &Method,
    original_cause: ResponseEgressOutcome,
    attempt: &mut ResponseEgressAttempt,
    clock: &MonotonicClock,
    committer: &mut Committer,
) -> Result<(), TransmitFailure>
where
    Committer: EgressCommitter,
{
    if !attempt.begin_fallback(original_cause) {
        log::error!("response-egress fallback state transition failed");
        attempt.terminate(ResponseEgressOutcome::ConversionError, clock.now());
        return Err(TransmitFailure::Uncommitted(DeliveryError::Conversion));
    }

    let fallback_started_at = clock.now();
    let fallback_deadline = fallback_started_at
        .checked_add(RESPONSE_EGRESS_FALLBACK_SAFETY_BUDGET)
        .unwrap_or(fallback_started_at);
    let fallback_response = fallback_response(original_cause);
    let prepared = match attempt.prepare_response(
        ResponseEgressFramingMode::HostAutomatic,
        request_method,
        fallback_response,
    ) {
        Ok(prepared) => prepared,
        Err(_error) => {
            attempt.finish_fallback(ResponseEgressFallbackDisposition::Aborted, clock.now());
            return Err(TransmitFailure::Uncommitted(DeliveryError::Conversion));
        }
    };
    let policy = ResponseEgressPolicy {
        write_deadline: Deadline::at_instant(fallback_deadline),
    };
    transmit_prepared(
        prepared,
        policy,
        attempt,
        clock,
        TransmitKind::Fallback,
        committer,
    )
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

fn transmit_prepared<Committer>(
    prepared: PreparedResponseEgress,
    policy: ResponseEgressPolicy,
    attempt: &mut ResponseEgressAttempt,
    clock: &MonotonicClock,
    kind: TransmitKind,
    committer: &mut Committer,
) -> Result<(), TransmitFailure>
where
    Committer: EgressCommitter,
{
    if policy.write_deadline.is_expired_at(clock.now()) {
        return precommit_failure(
            kind,
            attempt,
            clock,
            ResponseEgressOutcome::DeadlineExceeded,
        );
    }

    let transmits_body = prepared.transmits_body();
    let (parts, body) = prepared.into_response().into_parts();
    // Suppressed payload size is not representation size. An empty bulk body would
    // let HTTP/2 synthesize a false Content-Length: 0 on HEAD/304.
    let buffered_bytes = if transmits_body {
        body.as_bytes()
    } else {
        None
    };
    let buffered_length = buffered_bytes.map(<[u8]>::len);
    let buffered_body = buffered_bytes.map(|bytes| BufferedBody {
        bytes,
        clock,
        deadline: policy.write_deadline,
    });
    let prepared_head =
        match committer.prepare(parts.status, &parts.headers, transmits_body, buffered_body) {
            Ok(prepared_head) => prepared_head,
            Err(error) => {
                log::error!("response-egress Fastly head preparation failed");
                let cause = if error == DeliveryError::Deadline
                    || policy.write_deadline.is_expired_at(clock.now())
                {
                    ResponseEgressOutcome::DeadlineExceeded
                } else {
                    ResponseEgressOutcome::ConversionError
                };
                return precommit_failure(kind, attempt, clock, cause);
            }
        };

    let commit_started_at = clock.now();
    if policy.write_deadline.is_expired_at(commit_started_at) {
        return precommit_failure(
            kind,
            attempt,
            clock,
            ResponseEgressOutcome::DeadlineExceeded,
        );
    }
    if !attempt.begin_writing() {
        log::error!("response-egress writing state transition failed");
        settle_failure(
            kind,
            attempt,
            ResponseEgressOutcome::TransportError,
            clock.now(),
        );
        return Err(TransmitFailure::Uncommitted(DeliveryError::Transport));
    }

    let sink = match committer.commit(prepared_head) {
        Ok(sink) => sink,
        Err(_error) => {
            log::error!("response-egress Fastly downstream commit failed");
            settle_failure(
                kind,
                attempt,
                ResponseEgressOutcome::TransportError,
                clock.now(),
            );
            return Err(TransmitFailure::Unknown(DeliveryError::Transport));
        }
    };

    let committed_at = clock.now();
    if let Some(length) = buffered_length {
        // A successful bulk send consumes the complete native body. Never truncate it by
        // entering streaming cleanup, even if the clock expires during that hostcall.
        let accepted =
            u64::try_from(length).is_ok_and(|bytes| attempt.account_bytes(bytes, committed_at));
        let terminal_error = if policy.write_deadline.is_expired_at(committed_at) {
            Some(DeliveryError::Deadline)
        } else if !accepted {
            Some(DeliveryError::Transport)
        } else {
            None
        };
        if let Some(error) = terminal_error {
            settle_failure(kind, attempt, error.outcome(), committed_at);
            return Err(TransmitFailure::Postcommit(error));
        }
        settle_success(kind, attempt, committed_at);
        return Ok(());
    }
    transmit_stream(body, sink, policy, attempt, clock, kind, committed_at)
}

fn transmit_stream<Sink>(
    body: Body,
    mut sink: Sink,
    policy: ResponseEgressPolicy,
    attempt: &mut ResponseEgressAttempt,
    clock: &MonotonicClock,
    kind: TransmitKind,
    committed_at: MonotonicInstant,
) -> Result<(), TransmitFailure>
where
    Sink: EgressSink,
{
    if policy.write_deadline.is_expired_at(committed_at) {
        abandon(&mut sink);
        settle_failure(
            kind,
            attempt,
            ResponseEgressOutcome::DeadlineExceeded,
            committed_at,
        );
        return Err(TransmitFailure::Postcommit(DeliveryError::Deadline));
    }

    let pump_result = pump_body(body, &mut sink, policy.write_deadline, attempt, clock);
    if let Err(error) = pump_result {
        abandon(&mut sink);
        settle_failure(kind, attempt, error.outcome(), clock.now());
        return Err(TransmitFailure::Postcommit(error));
    }

    if policy.write_deadline.is_expired_at(clock.now()) {
        abandon(&mut sink);
        settle_failure(
            kind,
            attempt,
            ResponseEgressOutcome::DeadlineExceeded,
            clock.now(),
        );
        return Err(TransmitFailure::Postcommit(DeliveryError::Deadline));
    }
    // Fastly consumes the native handle before reporting a close failure, so no
    // second abandon operation is available after this boundary.
    let finish_result = sink.finish();
    let observed_at = clock.now();
    let finish_failed = finish_result.is_err();
    let finish_error = if policy.write_deadline.is_expired_at(observed_at) {
        Some(DeliveryError::Deadline)
    } else if finish_failed {
        Some(DeliveryError::Transport)
    } else {
        None
    };
    if let Some(error) = finish_error {
        if finish_failed {
            log::error!("response-egress Fastly body finish failed");
        }
        settle_failure(kind, attempt, error.outcome(), observed_at);
        return Err(TransmitFailure::Postcommit(error));
    }

    settle_success(kind, attempt, observed_at);
    Ok(())
}

fn pump_body<Sink>(
    body: Body,
    sink: &mut Sink,
    deadline: Deadline,
    attempt: &mut ResponseEgressAttempt,
    clock: &MonotonicClock,
) -> Result<(), DeliveryError>
where
    Sink: EgressSink,
{
    let (payload, _scope) = body.into_parts();
    match payload {
        BodyPayload::Once(bytes) => write_chunk(bytes.as_ref(), sink, deadline, attempt, clock),
        BodyPayload::Stream(mut source) => {
            #[cfg(feature = "fastly")]
            let time = native_time(clock.clone());
            #[cfg(feature = "fastly")]
            let mut write_timer = None;
            loop {
                let observed_at = clock.now();
                if deadline.is_expired_at(observed_at) {
                    return Err(DeliveryError::Deadline);
                }
                #[cfg(feature = "fastly")]
                let read = executor::block_on(drive_callback(
                    read_stream_until(&mut source, &time, deadline, &mut write_timer),
                    clock.clone(),
                    observed_at,
                ))
                .and_then(identity)
                .map_err(|error| {
                    attempt.record_source_error(&error);
                    DeliveryError::Source
                })?;
                #[cfg(feature = "fastly")]
                let next = match read {
                    StreamRead::WriteDeadline => return Err(DeliveryError::Deadline),
                    StreamRead::Source(next) => next,
                };
                #[cfg(not(feature = "fastly"))]
                let next = executor::block_on(source.next());
                if let Some(Err(error)) = &next {
                    attempt.record_source_error(error);
                }
                ensure_deadline(deadline, clock)?;
                match next {
                    Some(Ok(bytes)) => {
                        write_chunk(bytes.as_ref(), sink, deadline, attempt, clock)?;
                    }
                    Some(Err(error)) => {
                        log::warn!(
                            "response-egress Fastly body source failed after commit: {}",
                            response_egress_source_error_category(&error)
                        );
                        return Err(DeliveryError::Source);
                    }
                    None => return Ok(()),
                }
            }
        }
    }
}

async fn read_stream_until(
    source: &mut BodyStream,
    time: &OutboundTime,
    deadline: Deadline,
    write_timer: &mut Option<OutboundFuture<()>>,
) -> Result<StreamRead, EdgeError> {
    poll_fn(|cx| {
        let timer = write_timer.get_or_insert_with(|| time.wait_until(deadline));
        if let Poll::Ready(result) = timer.as_mut().poll(cx) {
            drop(write_timer.take());
            return Poll::Ready(result.map(|()| StreamRead::WriteDeadline));
        }
        source
            .poll_next_unpin(cx)
            .map(|next| Ok(StreamRead::Source(next)))
    })
    .await
}

fn write_chunk<Sink>(
    bytes: &[u8],
    sink: &mut Sink,
    deadline: Deadline,
    attempt: &mut ResponseEgressAttempt,
    clock: &MonotonicClock,
) -> Result<(), DeliveryError>
where
    Sink: EgressSink,
{
    let mut offset = 0;
    while offset < bytes.len() {
        ensure_deadline(deadline, clock)?;
        let remaining = bytes.get(offset..).ok_or(DeliveryError::Transport)?;
        let write_result = sink.write(remaining);
        let observed_at = clock.now();
        if deadline.is_expired_at(observed_at) {
            if let Ok(accepted_size) = write_result
                && accepted_size <= remaining.len()
            {
                let accepted_u64 =
                    u64::try_from(accepted_size).map_err(|_error| DeliveryError::Transport)?;
                if !attempt.account_bytes(accepted_u64, observed_at) {
                    return Err(DeliveryError::Transport);
                }
            }
            return Err(DeliveryError::Deadline);
        }
        let accepted_size = match write_result {
            Ok(0) => return Err(DeliveryError::Transport),
            Ok(accepted) if accepted <= remaining.len() => accepted,
            Ok(_) | Err(_) => return Err(DeliveryError::Transport),
        };
        let accepted_u64 =
            u64::try_from(accepted_size).map_err(|_error| DeliveryError::Transport)?;
        if !attempt.account_bytes(accepted_u64, observed_at) {
            return Err(DeliveryError::Transport);
        }
        offset = offset
            .checked_add(accepted_size)
            .ok_or(DeliveryError::Transport)?;
    }
    Ok(())
}

fn ensure_deadline(deadline: Deadline, clock: &MonotonicClock) -> Result<(), DeliveryError> {
    if deadline.is_expired_at(clock.now()) {
        Err(DeliveryError::Deadline)
    } else {
        Ok(())
    }
}

fn abandon<Sink>(sink: &mut Sink)
where
    Sink: EgressSink,
{
    if sink.abandon().is_err() {
        log::error!("response-egress Fastly body abandon failed");
    }
}

fn precommit_failure(
    kind: TransmitKind,
    attempt: &mut ResponseEgressAttempt,
    clock: &MonotonicClock,
    cause: ResponseEgressOutcome,
) -> Result<(), TransmitFailure> {
    let error = DeliveryError::from_outcome(cause);
    if kind == TransmitKind::Fallback {
        attempt.finish_fallback(ResponseEgressFallbackDisposition::Aborted, clock.now());
    }
    Err(TransmitFailure::Precommit { cause, error })
}

fn settle_failure(
    kind: TransmitKind,
    attempt: &mut ResponseEgressAttempt,
    outcome: ResponseEgressOutcome,
    observed_at: MonotonicInstant,
) {
    match kind {
        TransmitKind::Application => {
            attempt.terminate(outcome, observed_at);
        }
        TransmitKind::Fallback => {
            attempt.finish_fallback(ResponseEgressFallbackDisposition::Aborted, observed_at);
        }
    }
}

fn settle_success(
    kind: TransmitKind,
    attempt: &mut ResponseEgressAttempt,
    observed_at: MonotonicInstant,
) {
    match kind {
        TransmitKind::Application => {
            attempt.complete(observed_at);
        }
        TransmitKind::Fallback => {
            attempt.finish_fallback(ResponseEgressFallbackDisposition::Completed, observed_at);
        }
    }
}

fn manual_stream_framing(headers: &HeaderMap, transmits_body: bool, streaming: bool) -> bool {
    streaming
        && transmits_body
        && headers.contains_key("content-length")
        && !headers.contains_key("x-compress-hint")
}

pub(crate) fn parse_uri(uri: &str) -> Result<Uri, EdgeError> {
    uri.parse::<Uri>()
        .map_err(|err| EdgeError::bad_request(format!("invalid request URI: {err}")))
}

#[cfg(test)]
mod tests {
    use crate::outbound::testing;
    use edgezero_core::response_egress_framing::prepare_response_egress;
    use std::cell::{Cell, RefCell};
    use std::collections::VecDeque;
    use std::rc::Rc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::task::Poll;
    use std::time::Duration;

    use bytes::Bytes;
    use edgezero_core::app::App;
    use edgezero_core::context::RequestContext;
    use edgezero_core::http::{Version, request_builder, response_builder};
    use edgezero_core::ingress::{IngressDispatchOutcome, IngressFraming, IngressHeadAccounting};
    use edgezero_core::response_egress::{
        ResponseEgressBodyKind, ResponseEgressCompletion, ResponseEgressHead,
        ResponseEgressObserver, ResponseEgressObserverHandle, ResponseEgressReport,
    };
    use edgezero_core::router::RouterService;
    use edgezero_core::time::MonotonicInstant;
    use futures_util::future::{pending, poll_fn as poll_future};
    use futures_util::stream::{self, poll_fn};

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

    #[derive(Default)]
    struct SinkState {
        abandon: MockTerminal,
        accepted: Vec<u8>,
        advance_on_finish: Option<(Arc<Mutex<MonotonicInstant>>, MonotonicInstant)>,
        advance_on_write: Option<(Arc<Mutex<MonotonicInstant>>, MonotonicInstant)>,
        commits: Vec<(StatusCode, HeaderMap, bool)>,
        events: Vec<&'static str>,
        finish: MockTerminal,
        staged: Vec<Vec<u8>>,
        write_plan: VecDeque<Result<usize, ()>>,
    }

    struct MockSink(Rc<RefCell<SinkState>>);

    #[derive(Clone, Copy, Default)]
    enum MockTerminal {
        Error,
        #[default]
        Success,
    }

    impl EgressSink for MockSink {
        type Error = ();

        fn abandon(&mut self) -> Result<(), Self::Error> {
            let mut state = self.0.borrow_mut();
            state.events.push("abandon");
            match state.abandon {
                MockTerminal::Error => Err(()),
                MockTerminal::Success => Ok(()),
            }
        }

        fn finish(&mut self) -> Result<(), Self::Error> {
            let mut state = self.0.borrow_mut();
            state.events.push("finish");
            if let Some((clock, observed)) = &state.advance_on_finish {
                *clock.lock().map_err(|_poisoned| ())? = *observed;
            }
            match state.finish {
                MockTerminal::Error => Err(()),
                MockTerminal::Success => Ok(()),
            }
        }

        fn write(&mut self, bytes: &[u8]) -> Result<usize, Self::Error> {
            let mut state = self.0.borrow_mut();
            state.events.push("write");
            let result = state.write_plan.pop_front().unwrap_or(Ok(bytes.len()));
            if let Ok(accepted) = result
                && accepted <= bytes.len()
            {
                state.accepted.extend_from_slice(&bytes[..accepted]);
            }
            if let Some((clock, observed)) = &state.advance_on_write {
                *clock.lock().map_err(|_poisoned| ())? = *observed;
            }
            result
        }
    }

    struct MockCommitter {
        commit_plan: VecDeque<MockCommit>,
        prepare_plan: VecDeque<Result<(), ()>>,
        state: Rc<RefCell<SinkState>>,
    }

    enum MockCommit {
        Accept,
        Error,
    }

    struct MockPrepared {
        buffered_body: Option<Vec<u8>>,
        head: (StatusCode, HeaderMap, bool),
    }

    impl MockCommitter {
        fn new(state: Rc<RefCell<SinkState>>) -> Self {
            Self {
                commit_plan: VecDeque::new(),
                prepare_plan: VecDeque::new(),
                state,
            }
        }
    }

    impl EgressCommitter for MockCommitter {
        type Error = ();
        type Prepared = MockPrepared;
        type Sink = MockSink;

        fn commit(&mut self, prepared: Self::Prepared) -> Result<Self::Sink, Self::Error> {
            let transmits_body = prepared.head.2;
            self.state.borrow_mut().commits.push(prepared.head);
            match self.commit_plan.pop_front().unwrap_or(MockCommit::Accept) {
                MockCommit::Accept => {
                    if transmits_body && let Some(bytes) = prepared.buffered_body {
                        self.state.borrow_mut().accepted.extend(bytes);
                    }
                    Ok(MockSink(Rc::clone(&self.state)))
                }
                MockCommit::Error => Err(()),
            }
        }

        fn prepare(
            &mut self,
            status: StatusCode,
            headers: &HeaderMap,
            transmits_body: bool,
            buffered_body: Option<BufferedBody<'_>>,
        ) -> Result<Self::Prepared, DeliveryError> {
            self.prepare_plan
                .pop_front()
                .unwrap_or(Ok(()))
                .map_err(|_error| DeliveryError::Conversion)?;
            let staged_body = if let Some(buffered) = buffered_body {
                let mut staged = Vec::new();
                buffered.stage(|remaining| {
                    let accepted = self
                        .state
                        .borrow_mut()
                        .write_plan
                        .pop_front()
                        .unwrap_or(Ok(remaining.len()))?;
                    if accepted <= remaining.len() {
                        staged.extend_from_slice(&remaining[..accepted]);
                    }
                    Ok::<usize, ()>(accepted)
                })?;
                self.state.borrow_mut().staged.push(staged.clone());
                Some(staged)
            } else {
                None
            };
            Ok(MockPrepared {
                buffered_body: staged_body,
                head: (status, headers.clone(), transmits_body),
            })
        }
    }

    fn response_egress(outcome: IngressDispatchOutcome) -> ResponseEgressEnvelope {
        let IngressDispatchOutcome::Response(envelope) = outcome else {
            panic!("expected response egress");
        };
        *envelope
    }

    fn stable_clock(now: MonotonicInstant) -> MonotonicClock {
        MonotonicClock::new(move || now)
    }

    fn attempt(observer: &RecordingObserver, now: MonotonicInstant) -> ResponseEgressAttempt {
        let headers = HeaderMap::new();
        let head =
            ResponseEgressHead::new(StatusCode::OK, Version::HTTP_11, &headers, None, now, None);
        ResponseEgressAttempt::new(
            &head,
            now,
            ResponseEgressCompletion::empty(),
            ResponseEgressObserverHandle::new(observer.clone()),
            MonotonicClock::default(),
        )
    }

    fn future_policy(now: MonotonicInstant) -> ResponseEgressPolicy {
        ResponseEgressPolicy {
            write_deadline: Deadline::at_instant(
                now.checked_add(Duration::from_secs(10)).expect("deadline"),
            ),
        }
    }

    fn prepared(body: Body) -> PreparedResponseEgress {
        prepare_response_egress(
            ResponseEgressFramingMode::HostAutomatic,
            &Method::GET,
            response_builder()
                .status(StatusCode::OK)
                .body(body)
                .expect("response"),
        )
        .expect("prepared response")
    }

    fn transmit(
        body: Body,
        state: Rc<RefCell<SinkState>>,
    ) -> (Result<(), TransmitFailure>, Vec<ResponseEgressReport>) {
        let now = MonotonicInstant::now();
        let clock = stable_clock(now);
        let observer = RecordingObserver::default();
        let mut attempt = attempt(&observer, now);
        let mut committer = MockCommitter::new(state);
        let prepared = attempt
            .prepare_response(
                ResponseEgressFramingMode::HostAutomatic,
                &Method::GET,
                Response::new(body),
            )
            .expect("prepared scoped response");
        let result = transmit_prepared(
            prepared,
            future_policy(now),
            &mut attempt,
            &clock,
            TransmitKind::Application,
            &mut committer,
        );
        drop(attempt);
        (result, observer.reports())
    }

    #[test]
    fn body_created_batches_end_at_egress_without_closing_callback_work() {
        use crate::outbound::scope::bind_batch;
        use edgezero_core::{OutboundBatch, OutboundBatchDriverEvent, OutboundScopeOwner};
        use futures_util::future::poll_fn as poll_future;
        use std::future::Future as _;

        struct Release(Rc<Cell<usize>>);

        #[expect(clippy::missing_trait_methods, reason = "Drop::pin_drop is unstable")]
        impl Drop for Release {
            fn drop(&mut self) {
                self.0.set(self.0.get().saturating_add(1));
            }
        }

        fn batch(released: Rc<Cell<usize>>) -> OutboundBatch {
            let owner = Release(released);
            let source = poll_fn(move |_cx| {
                let _keep_owner = &owner;
                Poll::<Option<OutboundBatchDriverEvent>>::Pending
            });
            let context = testing::batch_context(1);
            let receiver = bind_batch(context.scope(), source);
            OutboundBatch::from_driver(context, receiver, testing::owner())
        }

        for failure in 0_u8..4 {
            let callback = OutboundScopeOwner::callback();
            let callback_released = Rc::new(Cell::new(0));
            let callback_batch = Rc::new(RefCell::new(
                callback.with(|| batch(Rc::clone(&callback_released))),
            ));
            let polled_callback = Rc::clone(&callback_batch);
            let response_released = Rc::new(Cell::new(0));
            let captured_release = Rc::clone(&response_released);
            let saved = Rc::new(RefCell::new(None));
            let captured_batch = Rc::clone(&saved);
            let body = Body::from_stream(async_stream::stream! {
                poll_future(|cx| {
                    let mut borrowed = polled_callback.borrow_mut();
                    let mut next = Box::pin(borrowed.next());
                    assert!(next.as_mut().poll(cx).is_pending());
                    Poll::Ready(())
                }).await;
                *captured_batch.borrow_mut() = Some(batch(captured_release));
                if failure == 1 {
                    yield Err(EdgeError::bad_gateway("fixture source failed"));
                } else {
                    yield Ok(Bytes::from_static(b"response"));
                }
            });
            let state = Rc::new(RefCell::new(SinkState::default()));
            if failure == 2 {
                state.borrow_mut().write_plan.push_back(Err(()));
            }
            if failure == 3 {
                state.borrow_mut().finish = MockTerminal::Error;
            }
            let (result, reports) = callback.with(|| transmit(body, state));
            assert_eq!(result.is_err(), failure != 0);
            assert_eq!(reports.len(), 1);
            assert_eq!(response_released.get(), 1, "egress owns body-created work");
            assert_eq!(
                callback_released.get(),
                0,
                "callback remains active after egress"
            );
            let closed_batch = saved.borrow_mut().take().expect("body created a batch");
            assert!(matches!(
                executor::block_on(closed_batch.collect())
                    .unwrap_err()
                    .error(),
                EdgeError::Internal { .. }
            ));
            let later_released = Rc::new(Cell::new(0));
            let later_batch = callback.with(|| batch(Rc::clone(&later_released)));
            assert_eq!(later_released.get(), 0);
            drop(callback);
            assert_eq!(callback_released.get(), 1);
            assert_eq!(later_released.get(), 1);
            drop(callback_batch);
            drop(later_batch);
            assert_eq!(response_released.get(), 1);
        }
    }

    #[test]
    fn buffered_handoff_does_not_reenter_the_stream_writer() {
        for bytes in ["abcdef", ""] {
            let state = Rc::new(RefCell::new(SinkState::default()));
            let (result, reports) = transmit(Body::from(bytes), Rc::clone(&state));
            assert_eq!(result, Ok(()));
            assert_eq!(state.borrow().accepted, bytes.as_bytes());
            assert!(
                state.borrow().events.is_empty(),
                "buffered handoff must not write or finish a consumed streaming handle"
            );
            assert_eq!(reports.len(), 1);
            assert_eq!(
                reports[0].bytes_written,
                u64::try_from(bytes.len()).expect("length")
            );
        }
    }

    #[test]
    fn manual_framing_is_only_for_transmitted_unhinted_declared_streams() {
        let mut headers = HeaderMap::new();
        assert!(!manual_stream_framing(&headers, true, true));
        headers.insert("content-length", "6".parse().expect("length"));
        assert!(manual_stream_framing(&headers, true, true));
        assert!(!manual_stream_framing(&headers, false, true));
        assert!(!manual_stream_framing(&headers, true, false));
        for hint in ["on", "gzip", "br", "off", "ON", " on ", "", "unrecognized"] {
            headers.insert("x-compress-hint", hint.parse().expect("hint"));
            assert!(!manual_stream_framing(&headers, true, true));
        }
        headers.append("x-compress-hint", "on".parse().expect("repeated hint"));
        assert!(!manual_stream_framing(&headers, true, true));
    }

    #[test]
    fn buffered_staging_stops_at_deadline_before_another_write() {
        use std::sync::atomic::AtomicBool;

        let start = MonotonicInstant::now();
        let deadline = start.checked_add(Duration::from_secs(1)).expect("deadline");
        let expired = Arc::new(AtomicBool::new(false));
        let clock_expired = Arc::clone(&expired);
        let clock = MonotonicClock::new(move || {
            if clock_expired.load(Ordering::SeqCst) {
                deadline
            } else {
                start
            }
        });
        let staged = BufferedBody {
            bytes: b"abcdef",
            clock: &clock,
            deadline: Deadline::at_instant(deadline),
        };
        let mut writes = 0_usize;
        assert_eq!(
            staged.stage(|_| {
                writes += 1;
                expired.store(true, Ordering::SeqCst);
                Ok::<usize, ()>(2)
            }),
            Err(DeliveryError::Deadline)
        );
        assert_eq!(writes, 1);
    }

    #[test]
    fn observed_staging_deadline_survives_a_later_backwards_clock_sample() {
        struct DeadlinePreparer;
        impl EgressCommitter for DeadlinePreparer {
            type Error = DeliveryError;
            type Prepared = ();
            type Sink = MockSink;

            fn commit(&mut self, _prepared: ()) -> Result<MockSink, DeliveryError> {
                Err(DeliveryError::Transport)
            }

            fn prepare(
                &mut self,
                _status: StatusCode,
                _headers: &HeaderMap,
                _transmits_body: bool,
                _buffered_body: Option<BufferedBody<'_>>,
            ) -> Result<(), DeliveryError> {
                Err(DeliveryError::Deadline)
            }
        }
        let now = MonotonicInstant::now();
        let observer = RecordingObserver::default();
        let mut attempt = attempt(&observer, now);
        assert_eq!(
            transmit_prepared(
                prepared(Body::from("abcdef")),
                future_policy(now),
                &mut attempt,
                &stable_clock(now),
                TransmitKind::Application,
                &mut DeadlinePreparer
            ),
            Err(TransmitFailure::Precommit {
                cause: ResponseEgressOutcome::DeadlineExceeded,
                error: DeliveryError::Deadline
            })
        );
        assert!(observer.reports().is_empty());
    }

    #[test]
    fn bulk_commit_expiry_preserves_the_complete_handed_off_buffer() {
        let start = MonotonicInstant::now();
        let deadline = start.checked_add(Duration::from_secs(1)).expect("deadline");
        let reads = Arc::new(AtomicUsize::new(0));
        let clock_reads = Arc::clone(&reads);
        let clock = MonotonicClock::new(move || {
            if clock_reads.fetch_add(1, Ordering::SeqCst) < 4 {
                start
            } else {
                deadline
            }
        });
        let observer = RecordingObserver::default();
        let mut attempt = attempt(&observer, start);
        let state = Rc::new(RefCell::new(SinkState::default()));
        let mut committer = MockCommitter::new(Rc::clone(&state));
        let result = transmit_prepared(
            prepared(Body::from("abcdef")),
            ResponseEgressPolicy {
                write_deadline: Deadline::at_instant(deadline),
            },
            &mut attempt,
            &clock,
            TransmitKind::Application,
            &mut committer,
        );
        assert_eq!(
            result,
            Err(TransmitFailure::Postcommit(DeliveryError::Deadline))
        );
        assert_eq!(state.borrow().accepted, b"abcdef");
        assert_eq!(state.borrow().commits.len(), 1);
        assert!(state.borrow().events.is_empty());
        assert_eq!(observer.reports().len(), 1);
        assert_eq!(observer.reports()[0].bytes_written, 6);
    }

    #[test]
    fn buffered_head_never_restores_suppressed_payload() {
        for status in [
            StatusCode::OK,
            StatusCode::NO_CONTENT,
            StatusCode::RESET_CONTENT,
            StatusCode::NOT_MODIFIED,
        ] {
            let response = response_builder()
                .status(status)
                .body(Body::from("abcdef"))
                .expect("response");
            let prepared = prepare_response_egress(
                ResponseEgressFramingMode::HostAutomatic,
                &Method::HEAD,
                response,
            )
            .expect("prepared");
            let now = MonotonicInstant::now();
            let observer = RecordingObserver::default();
            let mut attempt = attempt(&observer, now);
            let state = Rc::new(RefCell::new(SinkState::default()));
            assert_eq!(
                transmit_prepared(
                    prepared,
                    future_policy(now),
                    &mut attempt,
                    &stable_clock(now),
                    TransmitKind::Application,
                    &mut MockCommitter::new(Rc::clone(&state))
                ),
                Ok(())
            );
            assert!(state.borrow().staged.is_empty());
            assert!(state.borrow().accepted.is_empty());
            assert_eq!(observer.reports()[0].bytes_written, 0);
        }
    }

    async fn invalid_length_response(_context: RequestContext) -> Result<Response, EdgeError> {
        response_builder()
            .status(StatusCode::OK)
            .header("content-length", "2")
            .body(Body::from("x"))
            .map_err(EdgeError::internal)
    }

    async fn ordinary_response(_context: RequestContext) -> Result<Response, EdgeError> {
        response_builder()
            .status(StatusCode::OK)
            .body(Body::from("application"))
            .map_err(EdgeError::internal)
    }

    #[test]
    fn parse_valid_uri() {
        let uri = parse_uri("https://example.com/foo").expect("uri");
        assert_eq!(uri.to_string(), "https://example.com/foo");
    }

    #[test]
    fn parse_invalid_uri() {
        let err = parse_uri("::invalid uri::").expect_err("should fail");
        assert_eq!(err.status().as_u16(), 400);
    }

    #[test]
    #[cfg(all(feature = "fastly", target_arch = "wasm32"))]
    fn raw_abi_commits_repeated_set_cookie_and_streams_body() {
        let mut headers = HeaderMap::new();
        headers.append("set-cookie", "a=1".parse().expect("header"));
        headers.append("set-cookie", "b=2".parse().expect("header"));
        let mut committer = platform::FastlyCommitter;
        let prepared = committer
            .prepare(StatusCode::OK, &headers, true, None)
            .expect("prepare raw response");
        let mut sink = committer.commit(prepared).expect("commit raw response");
        assert_eq!(sink.write(b"raw-egress"), Ok(10));
        assert_eq!(sink.finish(), Ok(()));
    }

    #[test]
    fn streams_empty_once_and_multiple_chunks_with_short_writes() {
        for (body, plan, expected) in [
            (
                Body::stream(stream::empty()),
                VecDeque::new(),
                b"".as_slice(),
            ),
            (
                Body::stream(stream::iter([Bytes::from_static(b"hello")])),
                VecDeque::from([Ok(2), Ok(3)]),
                b"hello".as_slice(),
            ),
            (
                Body::stream(stream::iter([
                    Bytes::from_static(b"hello "),
                    Bytes::from_static(b"world"),
                ])),
                VecDeque::new(),
                b"hello world".as_slice(),
            ),
        ] {
            let state = Rc::new(RefCell::new(SinkState {
                write_plan: plan,
                ..SinkState::default()
            }));
            let (result, reports) = transmit(body, Rc::clone(&state));
            assert_eq!(result, Ok(()));
            assert_eq!(state.borrow().accepted, expected);
            assert_eq!(state.borrow().events.last(), Some(&"finish"));
            assert_eq!(reports.len(), 1);
            assert_eq!(reports[0].outcome, ResponseEgressOutcome::Completed);
            assert_eq!(
                reports[0].bytes_written,
                u64::try_from(expected.len()).expect("body length")
            );
        }
    }

    #[test]
    fn fastly_delivery_report_tracks_the_last_fallback_commitment() {
        use crate::delivery::{DispatchOutcome, FastlyRequestDelivery, finish_request};
        use edgezero_core::response_egress_framing::{
            ResponseEgressFramingError, test_support::framing_fixture,
        };
        for (prepare, commit, expected) in [
            (
                Err(()),
                MockCommit::Accept,
                FastlyResponseCommitState::NotCommitted,
            ),
            (
                Ok(()),
                MockCommit::Error,
                FastlyResponseCommitState::Unknown,
            ),
            (
                Ok(()),
                MockCommit::Accept,
                FastlyResponseCommitState::Committed,
            ),
        ] {
            let fixture = executor::block_on(framing_fixture(
                ResponseEgressFramingError::UnsupportedTrailers,
                false,
            ))
            .unwrap();
            let state = Rc::new(RefCell::new(SinkState::default()));
            let mut committer = MockCommitter {
                prepare_plan: VecDeque::from([prepare]),
                commit_plan: VecDeque::from([commit]),
                state,
            };
            let result = finish_request(
                Ok(DispatchOutcome::Routed(fixture.envelope)),
                |_| 42_u8,
                |envelope| send_egress_with(envelope, &mut committer),
            );
            assert_eq!(
                result.may_send_native_response(),
                expected == FastlyResponseCommitState::NotCommitted
            );
            let FastlyRequestDelivery::Routed {
                state: hook_state,
                delivery,
            } = result
            else {
                panic!("routed result");
            };
            assert_eq!(hook_state, 42);
            assert_eq!(delivery.commitment(), expected);
            let completed = fixture.completed.lock().unwrap();
            let observed = fixture.observed.lock().unwrap();
            assert_eq!(completed.len(), 1);
            assert_eq!(completed.first(), Some(delivery.report()));
            assert_eq!(*completed, *observed);
            assert_eq!(
                delivery.report().body_kind,
                ResponseEgressBodyKind::Fallback
            );
            assert_eq!(
                delivery.report().fallback_disposition,
                Some(if expected == FastlyResponseCommitState::Committed {
                    ResponseEgressFallbackDisposition::Completed
                } else {
                    ResponseEgressFallbackDisposition::Aborted
                })
            );
        }
    }

    #[test]
    fn framing_observer_preserves_each_typed_reason() {
        use crate::delivery::{DispatchOutcome, FastlyRequestDelivery, finish_request};
        use edgezero_core::response_egress_framing::test_support::{
            FRAMING_RULES, framing_fixture, has_full_synthesized_fixture_headers,
        };
        for reason in FRAMING_RULES {
            for detached in [false, true] {
                let fixture =
                    executor::block_on(framing_fixture(reason, detached)).expect("fixture");
                let state = Rc::new(RefCell::new(SinkState::default()));
                let mut committer = MockCommitter::new(Rc::clone(&state));
                let dispatched = if detached {
                    DispatchOutcome::Detached(fixture.envelope)
                } else {
                    DispatchOutcome::Routed(fixture.envelope)
                };
                let result = finish_request(
                    Ok(dispatched),
                    |_| {
                        assert!(!detached);
                        42_u8
                    },
                    |envelope| send_egress_with(envelope, &mut committer),
                );
                let delivery = match &result {
                    FastlyRequestDelivery::Routed {
                        state: hook_state,
                        delivery,
                    } => {
                        assert_eq!(*hook_state, 42);
                        delivery
                    }
                    FastlyRequestDelivery::Detached(delivery) => delivery,
                    FastlyRequestDelivery::Aborted | FastlyRequestDelivery::NoAttempt(_) => {
                        panic!("lost attempted delivery")
                    }
                };
                assert_eq!(delivery.error(), None);
                assert_eq!(
                    state.borrow().commits[0].1["x-synthesized-fixture"],
                    "configured"
                );
                assert!(has_full_synthesized_fixture_headers(
                    &state.borrow().commits[0].1
                ));
                assert_eq!(delivery.commitment(), FastlyResponseCommitState::Committed);
                let snapshot = state.borrow();
                assert_eq!(snapshot.commits.len(), 1);
                assert_eq!(snapshot.commits[0].0, StatusCode::INTERNAL_SERVER_ERROR);
                assert_eq!(snapshot.accepted, INTERNAL_FALLBACK_BODY);
                let completed = fixture.completed.lock().expect("completion");
                assert_eq!(completed.len(), 1);
                assert_eq!(delivery.report(), &completed[0]);
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
            FastlyFailureCause, ProviderFailureDetail, ProviderFailurePhase,
        };
        use std::sync::atomic::{AtomicBool, Ordering};

        let detail = ProviderFailureDetail::Fastly {
            phase: ProviderFailurePhase::ResponseBodyRead,
            cause: FastlyFailureCause::ConnectionTerminated,
        };
        for with_detail in [false, true] {
            for late in [false, true] {
                let now = MonotonicInstant::now();
                let policy = future_policy(now);
                let deadline = policy.write_deadline.instant();
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
                let state = Rc::new(RefCell::new(SinkState::default()));
                let mut committer = MockCommitter::new(Rc::clone(&state));
                let result = transmit_prepared(
                    prepared(body),
                    policy,
                    &mut attempt,
                    &clock,
                    TransmitKind::Application,
                    &mut committer,
                );
                assert!(result.is_err());
                drop(attempt);
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
                assert_eq!(
                    state
                        .borrow()
                        .events
                        .iter()
                        .filter(|event| **event == "abandon")
                        .count(),
                    1
                );
            }
        }
    }

    #[test]
    fn zero_write_write_error_source_error_and_finish_error_abort_once() {
        let cases = [
            (
                Body::stream(stream::iter([Bytes::from_static(b"x")])),
                VecDeque::from([Ok(0)]),
                false,
                ResponseEgressOutcome::TransportError,
            ),
            (
                Body::stream(stream::iter([Bytes::from_static(b"x")])),
                VecDeque::from([Err(())]),
                false,
                ResponseEgressOutcome::TransportError,
            ),
            (
                Body::from_stream(stream::iter([Err(EdgeError::internal(anyhow::anyhow!(
                    "private source detail"
                )))])),
                VecDeque::new(),
                false,
                ResponseEgressOutcome::SourceError,
            ),
            (
                Body::stream(stream::empty()),
                VecDeque::new(),
                true,
                ResponseEgressOutcome::TransportError,
            ),
        ];
        for (body, write_plan, finish_error, expected) in cases {
            let state = Rc::new(RefCell::new(SinkState {
                abandon: MockTerminal::Error,
                finish: if finish_error {
                    MockTerminal::Error
                } else {
                    MockTerminal::Success
                },
                write_plan,
                ..SinkState::default()
            }));
            let (result, reports) = transmit(body, Rc::clone(&state));
            assert!(matches!(result, Err(TransmitFailure::Postcommit(_))));
            assert_eq!(reports.len(), 1);
            assert_eq!(reports[0].outcome, expected);
            assert_eq!(
                reports[0].source_failure.is_some(),
                expected == ResponseEgressOutcome::SourceError
            );
            let expected_terminal = if finish_error { "finish" } else { "abandon" };
            assert_eq!(state.borrow().events.last(), Some(&expected_terminal));
        }
    }

    #[cfg(feature = "fastly")]
    use edgezero_core::OutboundBatchDriverEvent;
    #[cfg(feature = "fastly")]
    use futures_util::stream::LocalBoxStream;

    #[cfg(feature = "fastly")]
    struct TimedWorkOwner(Rc<Cell<usize>>);

    #[cfg(feature = "fastly")]
    #[expect(clippy::missing_trait_methods, reason = "Drop::pin_drop is unstable")]
    impl Drop for TimedWorkOwner {
        fn drop(&mut self) {
            self.0.set(self.0.get().saturating_add(1));
        }
    }

    #[cfg(feature = "fastly")]
    fn timed_scope_work(
        released: Rc<Cell<usize>>,
        time: OutboundTime,
        observation_cutoff: Deadline,
    ) -> LocalBoxStream<'static, OutboundBatchDriverEvent> {
        let owner = TimedWorkOwner(released);
        async_stream::stream! {
        let _owner = owner;
            yield match time.wait_until(observation_cutoff).await {
            Ok(()) => OutboundBatchDriverEvent::Cutoff,
            Err(error) => OutboundBatchDriverEvent::Failed(error),
        };
        }
        .boxed_local()
    }

    #[cfg(feature = "fastly")]
    fn assert_unavailable_sdk_origin<Output>(outcome: &Result<Output, EdgeError>) {
        use edgezero_core::{FastlyFailureCause, ProviderFailureDetail, ProviderFailurePhase};
        let Err(error) = outcome else {
            panic!("the controlled unavailable SDK origin cannot succeed");
        };
        assert!(
            matches!(
                error.provider_failure_detail(),
                Some(ProviderFailureDetail::Fastly {
                    phase: ProviderFailurePhase::Send,
                    cause: FastlyFailureCause::ConnectionRefused
                        | FastlyFailureCause::InternalError {
                            status: Some(1_i32)
                        },
                })
            ),
            "unavailable origin must retain its actual SDK cause: {error:?}"
        );
    }

    #[cfg(feature = "fastly")]
    async fn callback_origin_until(
        client: &edgezero_core::HttpClient,
        time: &OutboundTime,
        origin_bound: Deadline,
    ) -> Result<(), EdgeError> {
        use edgezero_core::OutboundRequest;
        let origin = client
            .send(
                OutboundRequest::get("http://127.0.0.1:9/callback-origin")
                    .unwrap()
                    .timeout(Duration::from_secs(1)),
            )
            .await;
        assert_unavailable_sdk_origin(&origin);
        time.wait_until(origin_bound).await
    }

    #[cfg(feature = "fastly")]
    fn early_batch_timeline_body(
        mut batch: edgezero_core::OutboundBatch,
        mut delayed: edgezero_core::OutboundBatch,
        time: OutboundTime,
        entered: MonotonicInstant,
        handoff_elapsed: Duration,
        ready_elapsed: Duration,
        checks: Rc<RefCell<Vec<u64>>>,
    ) -> Body {
        use edgezero_core::{OutboundBatchCheck, OutboundBatchTermination};
        Body::from_stream(async_stream::stream! {
            for millis in [260_u64, 320, 400] {
                time.wait_until(Deadline::at_instant(entered.checked_add(Duration::from_millis(millis)).unwrap())).await?;
                let observed = batch.check()?;
                if millis == 260 {
                    let OutboundBatchCheck::Item(item) = observed else { panic!("body-first SDK pipeline must already be recorded at first check"); };
                    assert_eq!(item.index(), 0);
                    assert!(item.result().elapsed < handoff_elapsed, "callback origin work must progress the idle body-owned pipeline before handoff");
                    assert_unavailable_sdk_origin(&item.result().outcome);
                } else {
                    assert!(matches!(observed, OutboundBatchCheck::Finished(OutboundBatchTermination::Completed)));
                }
                let delayed_result = delayed.check()?;
                match millis {
                    260 => assert!(matches!(delayed_result, OutboundBatchCheck::Pending)),
                    320 => {
                        let OutboundBatchCheck::Item(item) = delayed_result else { panic!("body-owned native timer must keep progressing after callback exit"); };
                        assert!(item.result().elapsed >= ready_elapsed);
                        assert!(matches!(item.result().outcome, Err(EdgeError::BadGateway { .. })));
                    }
                    _ => assert!(matches!(delayed_result, OutboundBatchCheck::Finished(OutboundBatchTermination::Completed))),
                }
                checks.borrow_mut().push(millis);
                yield Ok(Bytes::from_static(b"tick"));
            }
        })
    }

    #[cfg(feature = "fastly")]
    fn delayed_body_scope_batch(
        time: OutboundTime,
        ready_at: Deadline,
        observation_cutoff: Deadline,
    ) -> (edgezero_core::OutboundBatch, Duration) {
        use crate::outbound::scope::bind_batch;
        let context = testing::batch_context_using(1, time.clone(), observation_cutoff);
        let ready_elapsed = ready_at
            .instant()
            .saturating_duration_since(context.started_at());
        let source = async_stream::stream! {
            if let Err(error) = time.wait_until(ready_at).await {
                yield OutboundBatchDriverEvent::Failed(error);
                return;
            }
            yield OutboundBatchDriverEvent::Item {
                index: 0, completed_at: time.clock().now(),
                outcome: Err(EdgeError::bad_gateway("delayed pipeline fixture")),
            };
        };
        let receiver = bind_batch(context.scope(), source);
        (
            edgezero_core::OutboundBatch::from_driver(context, receiver, testing::owner()),
            ready_elapsed,
        )
    }

    #[test]
    #[cfg(feature = "fastly")]
    fn precreated_body_batch_progresses_with_callback_origin_before_attachment() {
        use crate::outbound::FastlyOutboundClient;
        use crate::outbound::scope;
        use edgezero_core::{HttpClient, OutboundRequest, OutboundScopeOwner};

        let callback = OutboundScopeOwner::callback();
        let body_owner = callback.with(OutboundScopeOwner::response_body);
        let body_scope = body_owner.with(scope::capture);
        let clock = MonotonicClock::default();
        let entered = clock.now();
        let time = native_time(clock.clone());
        let client = HttpClient::with_client(FastlyOutboundClient::with_time(time.clone()));
        let batch = body_owner.with(|| {
            client.start_batch_until(
                vec![
                    OutboundRequest::post("http://127.0.0.1:9/body-first")
                        .unwrap()
                        .body("body-owned")
                        .timeout(Duration::from_secs(1)),
                ],
                Deadline::at_instant(entered.checked_add(Duration::from_millis(500)).unwrap()),
            )
        });
        let (delayed, ready_elapsed) = body_owner.with(|| {
            delayed_body_scope_batch(
                time.clone(),
                Deadline::at_instant(entered.checked_add(Duration::from_millis(300)).unwrap()),
                Deadline::at_instant(entered.checked_add(Duration::from_millis(500)).unwrap()),
            )
        });
        executor::block_on(body_scope.run(time.clone(), entered, Box::pin(async { Ok(()) })))
            .unwrap();
        let origin_bound =
            Deadline::at_instant(entered.checked_add(Duration::from_millis(120)).unwrap());
        callback
            .with(|| {
                executor::block_on(drive_callback(
                    callback_origin_until(&client, &time, origin_bound),
                    clock.clone(),
                    entered,
                ))
            })
            .unwrap()
            .unwrap();
        assert!(origin_bound.is_expired_at(clock.now()));
        let handoff_elapsed = clock.now().saturating_duration_since(entered);
        let checks = Rc::new(RefCell::new(Vec::new()));
        let response = body_owner
            .attach(Response::new(early_batch_timeline_body(
                batch,
                delayed,
                time.clone(),
                entered,
                handoff_elapsed,
                ready_elapsed,
                Rc::clone(&checks),
            )))
            .unwrap();
        assert_eq!(checks.borrow().len(), 0, "attachment cannot poll the body");
        assert!(response.body().as_bytes().is_none());
        let observer = RecordingObserver::default();
        let mut attempt = attempt(&observer, entered);
        let prepared = attempt
            .prepare_response(
                ResponseEgressFramingMode::HostAutomatic,
                &Method::GET,
                response,
            )
            .unwrap();
        body_scope
            .check()
            .expect("framing adopts the exact precreated body owner");
        callback.close();
        body_scope
            .check()
            .expect("handler return cannot close precreated body work");
        assert!(attempt.begin_writing());
        let delivered = body_scope.run(
            time,
            entered,
            Box::pin(async move {
                prepared
                    .into_response()
                    .into_body()
                    .into_bytes_bounded(64)
                    .await
            }),
        );
        assert_eq!(
            executor::block_on(delivered).unwrap(),
            Bytes::from_static(b"tickticktick")
        );
        assert_eq!(*checks.borrow(), [260, 320, 400]);
        body_scope
            .check()
            .expect("source EOF is not transmission completion");
        assert!(attempt.complete(clock.now()));
        assert!(matches!(
            body_scope.check(),
            Err(EdgeError::Internal { .. })
        ));
    }

    #[cfg(feature = "fastly")]
    fn timed_scope_batch(
        released: Rc<Cell<usize>>,
        time: OutboundTime,
        observation_cutoff: Deadline,
    ) -> edgezero_core::OutboundBatch {
        use crate::outbound::scope::bind_batch;
        let context = testing::batch_context_using(1, time.clone(), observation_cutoff);
        let receiver = bind_batch(
            context.scope(),
            timed_scope_work(released, time, observation_cutoff),
        );
        edgezero_core::OutboundBatch::from_driver(context, receiver, testing::owner())
    }

    #[cfg(feature = "fastly")]
    fn pending_scoped_body(
        time: OutboundTime,
        observation_cutoff: Deadline,
        task: Rc<RefCell<Option<edgezero_core::OutboundBatch>>>,
        task_released: Rc<Cell<usize>>,
        source_released: Rc<Cell<usize>>,
    ) -> Body {
        use edgezero_core::BudgetSource;
        let owner = TimedWorkOwner(source_released);
        let clock = time.clock().clone();
        Body::from_stream(poll_fn(move |_cx| {
            let _owner = &owner;
            if task.borrow().is_none() {
                task.borrow_mut().replace(timed_scope_batch(
                    Rc::clone(&task_released),
                    time.clone(),
                    observation_cutoff,
                ));
            }
            // Bound the old failure path without changing the egress or outbound deadline.
            if observation_cutoff.is_expired_at(clock.now()) {
                Poll::Ready(Some(Err(EdgeError::gateway_timeout_caused(
                    "outbound fixture safety fence",
                    BudgetSource::BatchCutoff,
                ))))
            } else {
                Poll::Pending
            }
        }))
    }

    #[test]
    fn write_timer_failure_precedes_source_and_releases_once() {
        #[derive(Debug, thiserror::Error)]
        #[error("write timer fixture failed")]
        struct TimerFailure;

        struct TimerOwner(Arc<AtomicUsize>);

        #[expect(clippy::missing_trait_methods, reason = "Drop::pin_drop is unstable")]
        impl Drop for TimerOwner {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }

        for delayed in [false, true] {
            let entered = MonotonicInstant::now();
            let deadline =
                Deadline::at_instant(entered.checked_add(Duration::from_secs(1)).unwrap());
            let released = Arc::new(AtomicUsize::new(0));
            let timer_released = Arc::clone(&released);
            let armed = Arc::new(AtomicUsize::new(0));
            let timer_armed = Arc::clone(&armed);
            let time = OutboundTime::new(stable_clock(entered), move |_, _| {
                timer_armed.fetch_add(1, Ordering::SeqCst);
                let owner = TimerOwner(Arc::clone(&timer_released));
                let mut first = true;
                Box::pin(poll_future(move |cx| {
                    let _owner = &owner;
                    if delayed && first {
                        first = false;
                        cx.waker().wake_by_ref();
                        Poll::Pending
                    } else {
                        Poll::Ready(Err(EdgeError::internal(TimerFailure)))
                    }
                }))
            });
            let reads = Rc::new(Cell::new(0_usize));
            let source_reads = Rc::clone(&reads);
            let mut source = poll_fn(move |_cx| {
                assert_eq!(
                    armed.load(Ordering::SeqCst),
                    1,
                    "timer setup precedes payload reads"
                );
                source_reads.set(source_reads.get().saturating_add(1));
                Poll::Pending
            })
            .boxed_local();
            let mut timer = None;
            let error =
                executor::block_on(read_stream_until(&mut source, &time, deadline, &mut timer))
                    .expect_err("timer failure is terminal");
            let EdgeError::Internal {
                source: failure, ..
            } = error
            else {
                panic!("typed timer failure");
            };
            assert!(failure.is::<TimerFailure>());
            assert_eq!(reads.get(), usize::from(delayed));
            assert!(timer.is_none());
            assert_eq!(released.load(Ordering::SeqCst), 1);
            drop(timer);
            assert_eq!(released.load(Ordering::SeqCst), 1);
        }
    }

    #[test]
    fn ready_body_chunks_keep_one_absolute_write_timer() {
        let entered = MonotonicInstant::now();
        let deadline = Deadline::at_instant(entered.checked_add(Duration::from_secs(1)).unwrap());
        let registered = Arc::new(Mutex::new(Vec::new()));
        let timer_registered = Arc::clone(&registered);
        let time = OutboundTime::new(stable_clock(entered), move |bound, _| {
            timer_registered.lock().unwrap().push(bound.instant());
            Box::pin(pending())
        });
        let mut source = stream::iter([
            Ok(Bytes::from_static(b"first")),
            Ok(Bytes::from_static(b"second")),
        ])
        .boxed_local();
        let mut timer = None;
        for expected in [
            Some(Bytes::from_static(b"first")),
            Some(Bytes::from_static(b"second")),
            None,
        ] {
            let next =
                executor::block_on(read_stream_until(&mut source, &time, deadline, &mut timer))
                    .unwrap();
            let StreamRead::Source(outcome) = next else {
                panic!("write deadline is still live");
            };
            assert_eq!(outcome.transpose().unwrap(), expected);
        }
        assert_eq!(*registered.lock().unwrap(), [deadline.instant()]);
    }

    #[test]
    #[cfg(feature = "fastly")]
    fn pending_body_write_deadline_releases_only_response_work() {
        use edgezero_core::{OutboundBatchNext, OutboundBatchTermination, OutboundScopeOwner};

        let entered = MonotonicInstant::now();
        let clock = MonotonicClock::default();
        let write_deadline =
            Deadline::at_instant(entered.checked_add(Duration::from_millis(50)).unwrap());
        let outbound_cutoff =
            Deadline::at_instant(entered.checked_add(Duration::from_millis(500)).unwrap());
        let callback = OutboundScopeOwner::callback();
        callback
            .with(|| executor::block_on(drive_callback(async {}, clock.clone(), entered)))
            .unwrap();
        let time = native_time(clock.clone());
        let callback_released = Rc::new(Cell::new(0));
        let mut sibling = callback.with(|| {
            timed_scope_batch(Rc::clone(&callback_released), time.clone(), outbound_cutoff)
        });
        let response_released = Rc::new(Cell::new(0));
        let body_task = Rc::new(RefCell::new(None));
        let source_released = Rc::new(Cell::new(0));
        let body = pending_scoped_body(
            time,
            outbound_cutoff,
            Rc::clone(&body_task),
            Rc::clone(&response_released),
            Rc::clone(&source_released),
        );
        let observer = RecordingObserver::default();
        let mut attempt = callback.with(|| attempt(&observer, entered));
        let state = Rc::new(RefCell::new(SinkState::default()));
        let mut committer = MockCommitter::new(Rc::clone(&state));
        let prepared = callback
            .with(|| {
                attempt.prepare_response(
                    ResponseEgressFramingMode::HostAutomatic,
                    &Method::GET,
                    Response::new(body),
                )
            })
            .unwrap();
        let result = callback.with(|| {
            transmit_prepared(
                prepared,
                ResponseEgressPolicy { write_deadline },
                &mut attempt,
                &clock,
                TransmitKind::Application,
                &mut committer,
            )
        });
        assert_eq!(
            result,
            Err(TransmitFailure::Postcommit(DeliveryError::Deadline))
        );
        assert!(
            !outbound_cutoff.is_expired_at(clock.now()),
            "egress must not wait for the outbound cutoff"
        );
        let reports = observer.reports();
        assert_eq!(reports.len(), 1);
        assert_eq!(reports[0].outcome, ResponseEgressOutcome::DeadlineExceeded);
        assert!(
            reports[0].source_failure.is_none(),
            "write expiry is not a source timeout"
        );
        assert_eq!(source_released.get(), 1);
        assert_eq!(response_released.get(), 1);
        assert_eq!(callback_released.get(), 0);
        assert_eq!(state.borrow().events, ["abandon"]);
        let mut closed = body_task
            .borrow_mut()
            .take()
            .expect("response-created work");
        assert!(matches!(
            executor::block_on(closed.next()),
            Err(EdgeError::Internal { .. })
        ));
        let callback_result = callback
            .with(|| executor::block_on(drive_callback(sibling.next(), clock.clone(), clock.now())))
            .unwrap();
        assert!(matches!(
            callback_result,
            Ok(OutboundBatchNext::Finished(
                OutboundBatchTermination::Cutoff
            ))
        ));
        assert!(
            outbound_cutoff.is_expired_at(clock.now()),
            "callback deadline is unchanged"
        );
        assert_eq!(callback_released.get(), 1);
        drop(callback);
        assert_eq!(source_released.get(), 1);
        assert_eq!(response_released.get(), 1);
    }

    #[test]
    fn accepted_prefix_at_deadline_is_accounted_before_deadline_wins() {
        let start = MonotonicInstant::now();
        let deadline = start.checked_add(Duration::from_secs(1)).expect("deadline");
        let now = Arc::new(Mutex::new(start));
        let sampled = Arc::clone(&now);
        let clock = MonotonicClock::new(move || *sampled.lock().expect("clock"));
        let report_observer = RecordingObserver::default();
        let mut attempt = attempt(&report_observer, start);
        let state = Rc::new(RefCell::new(SinkState {
            advance_on_write: Some((Arc::clone(&now), deadline)),
            ..SinkState::default()
        }));
        let mut committer = MockCommitter::new(Rc::clone(&state));
        let result = transmit_prepared(
            prepared(Body::stream(stream::iter([Bytes::from_static(b"abc")]))),
            ResponseEgressPolicy {
                write_deadline: Deadline::at_instant(deadline),
            },
            &mut attempt,
            &clock,
            TransmitKind::Application,
            &mut committer,
        );
        assert_eq!(
            result,
            Err(TransmitFailure::Postcommit(DeliveryError::Deadline))
        );
        let reports = report_observer.reports();
        assert_eq!(reports.len(), 1);
        assert_eq!(reports[0].outcome, ResponseEgressOutcome::DeadlineExceeded);
        assert_eq!(reports[0].bytes_written, 3);
    }

    #[test]
    fn deadline_at_commit_abandons_without_polling_or_writing() {
        let start = MonotonicInstant::now();
        let deadline = start.checked_add(Duration::from_secs(1)).expect("deadline");
        let reads = Arc::new(AtomicUsize::new(0));
        let clock_reads = Arc::clone(&reads);
        let clock = MonotonicClock::new(move || {
            if clock_reads.fetch_add(1, Ordering::SeqCst) < 2 {
                start
            } else {
                deadline
            }
        });
        let polls = Rc::new(Cell::new(0_usize));
        let observed_polls = Rc::clone(&polls);
        let body = Body::from_stream(poll_fn(move |_| {
            observed_polls.set(observed_polls.get() + 1);
            Poll::Pending
        }));
        let report_observer = RecordingObserver::default();
        let mut attempt = attempt(&report_observer, start);
        let state = Rc::new(RefCell::new(SinkState::default()));
        let mut committer = MockCommitter::new(Rc::clone(&state));
        let result = transmit_prepared(
            prepared(body),
            ResponseEgressPolicy {
                write_deadline: Deadline::at_instant(deadline),
            },
            &mut attempt,
            &clock,
            TransmitKind::Application,
            &mut committer,
        );

        assert_eq!(
            result,
            Err(TransmitFailure::Postcommit(DeliveryError::Deadline))
        );
        assert_eq!(polls.get(), 0);
        assert!(state.borrow().accepted.is_empty());
        assert_eq!(state.borrow().events, ["abandon"]);
        assert_eq!(
            report_observer.reports()[0].outcome,
            ResponseEgressOutcome::DeadlineExceeded
        );
    }

    #[test]
    fn deadline_after_head_preparation_prevents_irreversible_commit() {
        let start = MonotonicInstant::now();
        let deadline = start.checked_add(Duration::from_secs(1)).expect("deadline");
        let reads = Arc::new(AtomicUsize::new(0));
        let clock_reads = Arc::clone(&reads);
        let clock = MonotonicClock::new(move || {
            if clock_reads.fetch_add(1, Ordering::SeqCst) == 0 {
                start
            } else {
                deadline
            }
        });
        let report_observer = RecordingObserver::default();
        let mut attempt = attempt(&report_observer, start);
        let state = Rc::new(RefCell::new(SinkState::default()));
        let mut committer = MockCommitter::new(Rc::clone(&state));

        let result = transmit_prepared(
            prepared(Body::from("uncommitted")),
            ResponseEgressPolicy {
                write_deadline: Deadline::at_instant(deadline),
            },
            &mut attempt,
            &clock,
            TransmitKind::Application,
            &mut committer,
        );

        assert_eq!(
            result,
            Err(TransmitFailure::Precommit {
                cause: ResponseEgressOutcome::DeadlineExceeded,
                error: DeliveryError::Deadline,
            })
        );
        assert!(state.borrow().commits.is_empty());
        assert!(report_observer.reports().is_empty());
    }

    #[test]
    fn deadline_at_source_poll_abandons_before_writing_the_chunk() {
        let start = MonotonicInstant::now();
        let deadline = start.checked_add(Duration::from_secs(1)).expect("deadline");
        let now = Arc::new(Mutex::new(start));
        let sampled = Arc::clone(&now);
        let clock = MonotonicClock::new(move || *sampled.lock().expect("clock"));
        let polls = Rc::new(Cell::new(0_usize));
        let observed_polls = Rc::clone(&polls);
        let source_now = Arc::clone(&now);
        let body = Body::from_stream(poll_fn(move |_| {
            *source_now.lock().expect("source clock") = deadline;
            observed_polls.set(observed_polls.get() + 1);
            Poll::Ready(Some(Ok(Bytes::from_static(b"late"))))
        }));
        let report_observer = RecordingObserver::default();
        let mut attempt = attempt(&report_observer, start);
        let state = Rc::new(RefCell::new(SinkState::default()));
        let mut committer = MockCommitter::new(Rc::clone(&state));

        let result = transmit_prepared(
            prepared(body),
            ResponseEgressPolicy {
                write_deadline: Deadline::at_instant(deadline),
            },
            &mut attempt,
            &clock,
            TransmitKind::Application,
            &mut committer,
        );

        assert_eq!(
            result,
            Err(TransmitFailure::Postcommit(DeliveryError::Deadline))
        );
        assert_eq!(polls.get(), 1);
        assert!(state.borrow().accepted.is_empty());
        assert_eq!(state.borrow().events, ["abandon"]);
        let reports = report_observer.reports();
        assert_eq!(reports.len(), 1);
        assert_eq!(reports[0].outcome, ResponseEgressOutcome::DeadlineExceeded);
        assert_eq!(reports[0].bytes_written, 0);
    }

    #[test]
    fn deadline_at_finish_wins_after_the_handle_is_consumed() {
        let start = MonotonicInstant::now();
        let deadline = start.checked_add(Duration::from_secs(1)).expect("deadline");
        let now = Arc::new(Mutex::new(start));
        let sampled = Arc::clone(&now);
        let clock = MonotonicClock::new(move || *sampled.lock().expect("clock"));
        let report_observer = RecordingObserver::default();
        let mut attempt = attempt(&report_observer, start);
        let state = Rc::new(RefCell::new(SinkState {
            advance_on_finish: Some((Arc::clone(&now), deadline)),
            ..SinkState::default()
        }));
        let mut committer = MockCommitter::new(Rc::clone(&state));

        let result = transmit_prepared(
            prepared(Body::stream(stream::empty())),
            ResponseEgressPolicy {
                write_deadline: Deadline::at_instant(deadline),
            },
            &mut attempt,
            &clock,
            TransmitKind::Application,
            &mut committer,
        );

        assert_eq!(
            result,
            Err(TransmitFailure::Postcommit(DeliveryError::Deadline))
        );
        assert_eq!(state.borrow().events, ["finish"]);
        let reports = report_observer.reports();
        assert_eq!(reports.len(), 1);
        assert_eq!(reports[0].outcome, ResponseEgressOutcome::DeadlineExceeded);
    }

    #[test]
    fn head_suppression_releases_source_without_polling() {
        struct DropSignal(Rc<Cell<usize>>);
        #[expect(
            clippy::missing_trait_methods,
            reason = "`Drop::pin_drop` is unstable (`pin_ergonomics`) and conflicts with `drop`"
        )]
        impl Drop for DropSignal {
            fn drop(&mut self) {
                self.0.set(self.0.get() + 1);
            }
        }

        let polls = Rc::new(Cell::new(0_usize));
        let drops = Rc::new(Cell::new(0_usize));
        let observed_polls = Rc::clone(&polls);
        let signal = DropSignal(Rc::clone(&drops));
        let body = Body::from_stream(poll_fn(move |_| {
            let _keep_alive = &signal;
            observed_polls.set(observed_polls.get() + 1);
            Poll::Pending
        }));
        let response = response_builder()
            .status(StatusCode::OK)
            .header("content-length", "99")
            .body(body)
            .expect("response");
        let prepared = prepare_response_egress(
            ResponseEgressFramingMode::HostAutomatic,
            &Method::HEAD,
            response,
        )
        .expect("prepared");
        let start = MonotonicInstant::now();
        let clock = stable_clock(start);
        let observer = RecordingObserver::default();
        let mut attempt = attempt(&observer, start);
        let state = Rc::new(RefCell::new(SinkState::default()));
        let mut committer = MockCommitter::new(Rc::clone(&state));
        let result = transmit_prepared(
            prepared,
            future_policy(start),
            &mut attempt,
            &clock,
            TransmitKind::Application,
            &mut committer,
        );
        assert_eq!(result, Ok(()));
        assert_eq!(polls.get(), 0);
        assert_eq!(drops.get(), 1);
        assert!(state.borrow().accepted.is_empty());
        assert_eq!(observer.reports()[0].bytes_written, 0);
    }

    #[test]
    fn body_suppressed_response_disables_manual_framing() {
        for (method, status) in [
            (Method::HEAD, StatusCode::OK),
            (Method::GET, StatusCode::NOT_MODIFIED),
        ] {
            let response = response_builder()
                .status(status)
                .header("content-length", "99")
                .body(Body::from("suppressed"))
                .expect("response");
            let prepared = prepare_response_egress(
                ResponseEgressFramingMode::HostAutomatic,
                &method,
                response,
            )
            .expect("prepared");
            let now = MonotonicInstant::now();
            let clock = stable_clock(now);
            let observer = RecordingObserver::default();
            let mut attempt = attempt(&observer, now);
            let state = Rc::new(RefCell::new(SinkState::default()));
            let mut committer = MockCommitter::new(Rc::clone(&state));

            let result = transmit_prepared(
                prepared,
                future_policy(now),
                &mut attempt,
                &clock,
                TransmitKind::Application,
                &mut committer,
            );

            assert_eq!(result, Ok(()));
            let observed_state = state.borrow();
            let (committed_status, headers, transmits_body) = &observed_state.commits[0];
            assert_eq!(*committed_status, status);
            assert_eq!(headers.get("content-length").expect("length"), "99");
            assert!(!transmits_body);
            assert!(observed_state.accepted.is_empty());
        }
    }

    #[test]
    fn payload_response_allows_manual_framing() {
        let response = response_builder()
            .status(StatusCode::OK)
            .header("content-length", "3")
            .body(Body::from("abc"))
            .expect("response");
        let prepared = prepare_response_egress(
            ResponseEgressFramingMode::HostAutomatic,
            &Method::GET,
            response,
        )
        .expect("prepared");
        let now = MonotonicInstant::now();
        let clock = stable_clock(now);
        let observer = RecordingObserver::default();
        let mut attempt = attempt(&observer, now);
        let state = Rc::new(RefCell::new(SinkState::default()));
        let mut committer = MockCommitter::new(Rc::clone(&state));

        let result = transmit_prepared(
            prepared,
            future_policy(now),
            &mut attempt,
            &clock,
            TransmitKind::Application,
            &mut committer,
        );

        assert_eq!(result, Ok(()));
        let observed_state = state.borrow();
        let (_, headers, transmits_body) = &observed_state.commits[0];
        assert_eq!(headers.get("content-length").expect("length"), "3");
        assert!(*transmits_body);
        assert_eq!(observed_state.accepted, b"abc");
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
            let fixture_envelope = synthesized_error_fixture(error).expect("fixture");
            let mut replacement = HeaderMap::new();
            replacement.insert("x-synthesized-fixture", "request".parse().expect("field"));
            replacement.insert("retry-after", "360".parse().expect("retry"));
            let envelope = fixture_envelope.with_synthesized_response_headers(
                edgezero_core::SynthesizedResponseHeaders::new(replacement)
                    .expect("request policy"),
            );
            let state = Rc::new(RefCell::new(SinkState::default()));
            let mut committer = MockCommitter::new(Rc::clone(&state));
            assert_eq!(send_egress_with(envelope, &mut committer).error(), None);
            let snapshot = state.borrow();
            let headers = &snapshot.commits[0].1;
            assert_eq!(headers["x-synthesized-fixture"], "request");
            assert_eq!(
                headers
                    .get("retry-after")
                    .map(|value| value.to_str().expect("text")),
                retry
            );
        }
    }

    #[test]
    fn framing_failure_sends_bounded_fallback_with_original_cause() {
        let mut app = App::new(
            RouterService::builder()
                .get("/", invalid_length_response)
                .build(),
        );
        let observer = RecordingObserver::default();
        app.set_response_egress_observer(observer.clone());
        let request_start = app.monotonic_now();
        let request = request_builder()
            .method(Method::GET)
            .uri("/")
            .body(Body::empty())
            .expect("request");
        let egress = response_egress(
            executor::block_on(app.dispatch_ingress(
                request,
                request_start,
                IngressHeadAccounting::HostManaged,
                IngressFraming::HostManaged,
            ))
            .expect("egress"),
        );
        let state = Rc::new(RefCell::new(SinkState::default()));
        let mut committer = MockCommitter::new(Rc::clone(&state));
        assert_eq!(send_egress_with(egress, &mut committer).error(), None);

        let snapshot = state.borrow();
        assert_eq!(snapshot.commits.len(), 1);
        assert_eq!(snapshot.commits[0].0, StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(snapshot.accepted, INTERNAL_FALLBACK_BODY);
        let reports = observer.reports();
        assert_eq!(reports.len(), 1);
        assert_eq!(reports[0].outcome, ResponseEgressOutcome::ConversionError);
        assert_eq!(reports[0].body_kind, ResponseEgressBodyKind::Fallback);
        assert_eq!(
            reports[0].fallback_disposition,
            Some(ResponseEgressFallbackDisposition::Completed)
        );
    }

    #[test]
    fn expired_application_deadline_uses_independent_fallback_budget() {
        let mut app = App::new(RouterService::builder().get("/", ordinary_response).build());
        let now = MonotonicInstant::now();
        app.set_monotonic_clock(stable_clock(now));
        app.set_response_egress_policy(move |_, _| ResponseEgressPolicy {
            write_deadline: Deadline::at_instant(now),
        });
        let observer = RecordingObserver::default();
        app.set_response_egress_observer(observer.clone());
        let request = request_builder()
            .method(Method::GET)
            .uri("/")
            .body(Body::empty())
            .expect("request");
        let egress = response_egress(
            executor::block_on(app.dispatch_ingress(
                request,
                now,
                IngressHeadAccounting::HostManaged,
                IngressFraming::HostManaged,
            ))
            .expect("egress"),
        );
        let state = Rc::new(RefCell::new(SinkState::default()));
        let mut committer = MockCommitter::new(Rc::clone(&state));
        assert_eq!(send_egress_with(egress, &mut committer).error(), None);
        assert_eq!(state.borrow().commits[0].0, StatusCode::GATEWAY_TIMEOUT);
        assert_eq!(state.borrow().accepted, TIMEOUT_FALLBACK_BODY);
        let reports = observer.reports();
        assert_eq!(reports.len(), 1);
        assert_eq!(reports[0].outcome, ResponseEgressOutcome::DeadlineExceeded);
        assert_eq!(reports[0].body_kind, ResponseEgressBodyKind::Fallback);
    }

    #[test]
    #[cfg(not(target_arch = "wasm32"))]
    fn head_fallback_suppresses_payload_and_policy_panic_stays_one_attempt() {
        let mut app = App::new(
            RouterService::builder()
                .route("/", Method::HEAD, ordinary_response)
                .build(),
        );
        app.set_response_egress_policy(|_, _| panic!("private policy payload"));
        let observer = RecordingObserver::default();
        app.set_response_egress_observer(observer.clone());
        let request_start = app.monotonic_now();
        let request = request_builder()
            .method(Method::HEAD)
            .uri("/")
            .body(Body::empty())
            .expect("request");
        let egress = response_egress(
            executor::block_on(app.dispatch_ingress(
                request,
                request_start,
                IngressHeadAccounting::HostManaged,
                IngressFraming::HostManaged,
            ))
            .expect("egress"),
        );
        let state = Rc::new(RefCell::new(SinkState::default()));
        let mut committer = MockCommitter::new(Rc::clone(&state));
        assert_eq!(send_egress_with(egress, &mut committer).error(), None);
        assert!(state.borrow().accepted.is_empty());
        let reports = observer.reports();
        assert_eq!(reports.len(), 1);
        assert_eq!(reports[0].body_kind, ResponseEgressBodyKind::Fallback);
        assert_eq!(reports[0].bytes_written, 0);
        assert_eq!(reports[0].outcome, ResponseEgressOutcome::ConversionError);
        assert_eq!(
            reports[0].fallback_disposition,
            Some(ResponseEgressFallbackDisposition::Completed)
        );
        assert_eq!(reports[0].framing_failures.application(), None);
        assert_eq!(reports[0].framing_failures.fallback(), None);
    }

    #[test]
    fn fallback_safety_deadline_aborts_and_preserves_original_cause() {
        let mut app = App::new(
            RouterService::builder()
                .get("/", invalid_length_response)
                .build(),
        );
        let start = MonotonicInstant::now();
        let safety_deadline = start
            .checked_add(RESPONSE_EGRESS_FALLBACK_SAFETY_BUDGET)
            .expect("safety deadline");
        let reads = Arc::new(AtomicUsize::new(0));
        let clock_reads = Arc::clone(&reads);
        app.set_monotonic_clock(MonotonicClock::new(move || {
            if clock_reads.fetch_add(1, Ordering::SeqCst) < 3 {
                start
            } else {
                safety_deadline
            }
        }));
        let observer = RecordingObserver::default();
        app.set_response_egress_observer(observer.clone());
        let request = request_builder()
            .method(Method::GET)
            .uri("/")
            .body(Body::empty())
            .expect("request");
        let egress = response_egress(
            executor::block_on(app.dispatch_ingress(
                request,
                start,
                IngressHeadAccounting::HostManaged,
                IngressFraming::HostManaged,
            ))
            .expect("egress"),
        );
        let state = Rc::new(RefCell::new(SinkState::default()));
        let mut committer = MockCommitter::new(Rc::clone(&state));
        assert_eq!(
            send_egress_with(egress, &mut committer).error(),
            Some(DeliveryError::Deadline)
        );
        assert!(state.borrow().accepted.is_empty());
        assert!(state.borrow().commits.is_empty());
        assert!(state.borrow().events.is_empty());
        let reports = observer.reports();
        assert_eq!(reports.len(), 1);
        assert_eq!(reports[0].outcome, ResponseEgressOutcome::ConversionError);
        assert_eq!(reports[0].body_kind, ResponseEgressBodyKind::Fallback);
        assert_eq!(
            reports[0].fallback_disposition,
            Some(ResponseEgressFallbackDisposition::Aborted)
        );
    }

    #[test]
    fn fallback_staging_and_bulk_commit_failure_preserve_conversion_cause() {
        for commit_error in [false, true] {
            let mut app = App::new(
                RouterService::builder()
                    .get("/", invalid_length_response)
                    .build(),
            );
            let observer = RecordingObserver::default();
            app.set_response_egress_observer(observer.clone());
            let request_start = app.monotonic_now();
            let request = request_builder()
                .method(Method::GET)
                .uri("/")
                .body(Body::empty())
                .expect("request");
            let egress = response_egress(
                executor::block_on(app.dispatch_ingress(
                    request,
                    request_start,
                    IngressHeadAccounting::HostManaged,
                    IngressFraming::HostManaged,
                ))
                .expect("egress"),
            );
            let state = Rc::new(RefCell::new(SinkState {
                write_plan: if commit_error {
                    VecDeque::new()
                } else {
                    VecDeque::from([Ok(3), Err(())])
                },
                ..SinkState::default()
            }));
            let mut committer = MockCommitter::new(Rc::clone(&state));
            if commit_error {
                committer.commit_plan = VecDeque::from([MockCommit::Error]);
            }
            assert!(send_egress_with(egress, &mut committer).error().is_some());
            let reports = observer.reports();
            assert_eq!(reports.len(), 1);
            assert_eq!(reports[0].outcome, ResponseEgressOutcome::ConversionError);
            assert_eq!(reports[0].body_kind, ResponseEgressBodyKind::Fallback);
            assert_eq!(
                reports[0].fallback_disposition,
                Some(ResponseEgressFallbackDisposition::Aborted)
            );
            assert_eq!(reports[0].bytes_written, 0);
            assert!(state.borrow().accepted.is_empty());
            assert_eq!(state.borrow().commits.len(), usize::from(commit_error));
        }
    }

    #[test]
    fn head_preparation_error_uses_one_successful_fallback_attempt() {
        let mut app = App::new(RouterService::builder().get("/", ordinary_response).build());
        let observer = RecordingObserver::default();
        app.set_response_egress_observer(observer.clone());
        let request_start = app.monotonic_now();
        let request = request_builder()
            .method(Method::GET)
            .uri("/")
            .body(Body::empty())
            .expect("request");
        let egress = response_egress(
            executor::block_on(app.dispatch_ingress(
                request,
                request_start,
                IngressHeadAccounting::HostManaged,
                IngressFraming::HostManaged,
            ))
            .expect("egress"),
        );
        let state = Rc::new(RefCell::new(SinkState::default()));
        let mut committer = MockCommitter {
            commit_plan: VecDeque::new(),
            prepare_plan: VecDeque::from([Err(()), Ok(())]),
            state: Rc::clone(&state),
        };
        assert_eq!(send_egress_with(egress, &mut committer).error(), None);
        assert_eq!(state.borrow().commits.len(), 1);
        assert_eq!(
            state.borrow().commits[0].0,
            StatusCode::INTERNAL_SERVER_ERROR
        );
        let reports = observer.reports();
        assert_eq!(reports.len(), 1);
        assert_eq!(reports[0].outcome, ResponseEgressOutcome::ConversionError);
        assert_eq!(reports[0].body_kind, ResponseEgressBodyKind::Fallback);
        assert_eq!(
            reports[0].fallback_disposition,
            Some(ResponseEgressFallbackDisposition::Completed)
        );
        assert_eq!(reports[0].framing_failures.application(), None);
        assert_eq!(reports[0].framing_failures.fallback(), None);
    }

    #[test]
    fn downstream_commit_error_is_unknown_and_does_not_send_fallback() {
        let now = MonotonicInstant::now();
        let clock = stable_clock(now);
        let observer = RecordingObserver::default();
        let mut attempt = attempt(&observer, now);
        let state = Rc::new(RefCell::new(SinkState::default()));
        let mut committer = MockCommitter {
            commit_plan: VecDeque::from([MockCommit::Error]),
            prepare_plan: VecDeque::new(),
            state: Rc::clone(&state),
        };

        let result = transmit_prepared(
            prepared(Body::from("application")),
            future_policy(now),
            &mut attempt,
            &clock,
            TransmitKind::Application,
            &mut committer,
        );

        assert_eq!(
            result,
            Err(TransmitFailure::Unknown(DeliveryError::Transport))
        );
        assert_eq!(state.borrow().commits.len(), 1);
        assert_eq!(observer.reports().len(), 1);
        assert_eq!(
            observer.reports()[0].outcome,
            ResponseEgressOutcome::TransportError
        );
    }
}
