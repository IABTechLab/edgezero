#![allow(
    dead_code,
    reason = "shared scenarios are selected by distinct synchronous, CLI and HTTP test owners"
)]
#![expect(
    clippy::arbitrary_source_item_ordering,
    reason = "preserve the resource, clock and policy scenario grouping during the ownership move"
)]

#[path = "../scopes.rs"]
pub(crate) mod scope_tests;

use std::cell::RefCell;
use std::num::NonZeroU64;
use std::rc::Rc;
use std::time::Duration;

use bytes::Bytes;
use edgezero_adapter_spin::outbound::test_support::{
    READY_ITEM_YIELD_QUOTA, cooperate_after_response_read, deadline_stream, ensure_p3_context,
    native_time, race_deadline, request_body_resources, request_options, response_head,
    set_timeout_option,
};
use edgezero_adapter_spin::outbound::{SpinOutboundClient, test_support};
use edgezero_core::body::Body;
use edgezero_core::error::{BadGatewayReason, BudgetSource, EdgeError};
use edgezero_core::error_detail::{
    ProviderFailureDetail, ProviderFailurePhase, SpinFailureCause, SpinTimeoutOption,
};
use edgezero_core::http::{HeaderValue, Method, StatusCode, Uri};
use edgezero_core::outbound::{
    IndependentOutboundProgress, OutboundBatch, OutboundDispatchRequest, OutboundFuture,
    OutboundHead, OutboundHttpClient, OutboundPending, OutboundPreparedBatch,
    OutboundProgressRuntime, OutboundRequest, OutboundSendMode, OutboundTime,
};
use edgezero_core::time::{
    Deadline, DispatchBudget, MonotonicClock, MonotonicInstant, dispatch_budget,
};
use edgezero_core::{OutboundProxyMarker, OutboundResponse, ResponseDecoding, ResponseMode};
use futures_util::stream::once;
use futures_util::{FutureExt as _, StreamExt as _};
use spin_sdk::time::sleep;
use spin_sdk::wasip3::http::types::{Fields, Request, RequestOptionsError, Response};
use spin_sdk::wasip3::http_compat::BodyWriter;
use spin_sdk::wasip3::wit_bindgen::StreamResult;
use spin_sdk::wasip3::wit_future;

fn bodyless_request_resources_hold() -> bool {
    if !matches!(test_support::empty_request_trailers(), Ok(None)) {
        return false;
    }
    for method in [Method::GET, Method::HEAD] {
        let body = request_body_resources(&method, &Body::empty());
        if body.0.is_some() || body.1.is_some() {
            drop((body.1, body.2, body.0));
            return false;
        }
        let (request, request_done) = Request::new(Fields::new(), body.1, body.2, None);
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
        let (writer, contents, trailers) = request_body_resources(&method, &source);
        let upload_retained = writer.is_some() && contents.is_some();
        drop((contents, trailers, writer));
        if !upload_retained {
            return false;
        }
    }
    true
}

std::thread_local! {
    pub(crate) static FIXTURE_RESPONSE: RefCell<Option<Response>> = const { RefCell::new(None) };
}

struct FixtureClient(OutboundTime);

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
        test_support::validate_options(request, mode)
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
        test_support::schedule_exchange(async move { head.await?.finish().await }.boxed_local())
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
        test_support::schedule_prepared_batch(context, slots)
    }
}

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

/// Runs in a real asynchronous WASIp3 export, unlike the synchronous SDK-resource binary.
#[doc(hidden)]
#[inline]
pub async fn eager_outbound_paths_hold() -> Result<(), EdgeError> {
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
    *observed
        .lock()
        .map_err(|_poisoned| EdgeError::internal(anyhow::anyhow!("fixture clock poisoned")))? = end;
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

/// Exercises real timer wakeups; requires an asynchronous WASI service export.
#[doc(hidden)]
#[inline]
pub async fn connect_timeout_fallback_holds() -> bool {
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
                    MonotonicClock::new(move || observed.lock().map_or(start, |current| *current)),
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
                race_deadline(async { 7_u8 }, budget, &time, &mut previous, |_output| None).await,
                Ok(7)
            ) {
                return false;
            }
        }
    }
    true
}

/// Runs request-option, response-clock, and fairness checks in the hosted contract binary.
#[doc(hidden)]
#[inline]
pub async fn deferred_clock_paths_hold() -> bool {
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
    for _ in 0..READY_ITEM_YIELD_QUOTA {
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
#[doc(hidden)]
#[inline]
pub async fn provider_deadline_diagnostics_hold() -> bool {
    use std::sync::{Arc, Mutex};
    use std::task::{Poll, Waker};

    use futures_util::future::poll_fn;

    let detail = ProviderFailureDetail::Spin {
        phase: ProviderFailurePhase::Send,
        cause: SpinFailureCause::ConnectionRefused,
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
#[doc(hidden)]
#[inline]
pub async fn converted_response_content_type_provenance_holds() -> bool {
    content_type_tests::check_converted_response_content_type_provenance()
        .await
        .is_ok()
}

/// Proves response conversion retains the client clock in the hosted contract binary.
#[doc(hidden)]
#[inline]
pub async fn converted_response_retains_injected_clock() -> bool {
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

mod native_policy_tests {
    use super::*;
    use edgezero_core::error::BudgetSource;

    pub(crate) async fn check_native_bodyless_precedes_caps_and_polling() -> Result<(), EdgeError> {
        for encoded_cap in [1, 0x0001_0000] {
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
                        let fields = Fields::new();
                        fields
                            .append("content-encoding", b"gzip")
                            .map_err(EdgeError::internal)?;
                        fields
                            .append("content-length", b"1073741824")
                            .map_err(EdgeError::internal)?;
                        let (mut writer, contents, trailers) = BodyWriter::new();
                        let (native, response_done) =
                            Response::new(fields, Some(contents), trailers);
                        native.set_status_code(status.as_u16()).map_err(|()| {
                            EdgeError::internal(anyhow::anyhow!("Spin fixture status rejected"))
                        })?;
                        let start = MonotonicInstant::now();
                        let duration = Duration::from_secs(5);
                        let end = start.checked_add(duration).ok_or_else(|| {
                            EdgeError::internal(anyhow::anyhow!("Spin fixture deadline overflow"))
                        })?;
                        let (outcome, polls, released) =
                            test_support::observe_response_head(process_response(
                                native,
                                method.clone(),
                                OutboundProxyMarker::Off,
                                choice,
                                mode,
                                DispatchBudget {
                                    cause: BudgetSource::PerCallTimeout,
                                    deadline: Deadline::at_instant(end),
                                    duration,
                                },
                                0,
                                24,
                                None,
                                Some(1),
                                Some(encoded_cap),
                                None,
                                None,
                                native_time(MonotonicClock::new(move || start)),
                            ))
                            .await?;
                        let response = outcome?;
                        let expected_length =
                            if method == Method::HEAD || status == StatusCode::NOT_MODIFIED {
                                Some("1073741824")
                            } else if status == StatusCode::RESET_CONTENT {
                                Some("0")
                            } else {
                                None
                            };
                        let correct = response.headers().get("content-encoding")
                            == Some(&HeaderValue::from_static("gzip"))
                            && response
                                .headers()
                                .get("content-length")
                                .and_then(|value| value.to_str().ok())
                                == expected_length
                            && response.body().as_bytes() == Some(b"".as_slice())
                            && polls == 0
                            && released
                            && writer.stream_writer.write_all(vec![1]).now_or_never()
                                == Some(vec![1]);
                        if !correct {
                            return Err(EdgeError::internal(anyhow::anyhow!(
                                "bodyless 1 GiB representation must preserve metadata and release its unread native source"
                            )));
                        }
                        if !response.into_bytes_bounded(0).await?.is_empty() {
                            return Err(EdgeError::internal(anyhow::anyhow!(
                                "bodyless representation exposed payload bytes"
                            )));
                        }
                        drop((writer, response_done));
                    }
                }
            }
        }
        Ok(())
    }
}

pub(crate) mod handoff_tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use edgezero_core::error::BudgetSource;
    use edgezero_core::time::DEFAULT_NO_DEADLINE_BUDGET;

    use super::*;

    #[expect(
        clippy::too_many_lines,
        reason = "preserve the full method, status, response-mode and deadline boundary matrix"
    )]
    pub(crate) async fn check_response_handoff() -> Result<(), EdgeError> {
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
                                EdgeError::internal(anyhow::anyhow!("fixture observation overflow"))
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
                        if writer.stream_writer.write_all(vec![1]).now_or_never() != Some(vec![1]) {
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

    #[expect(
        clippy::print_stdout,
        reason = "the existing eager component reports each acceptance criterion after its checks complete"
    )]
    pub(crate) async fn check_batch_handoff() -> Result<(), EdgeError> {
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
                let request =
                    OutboundRequest::new(method.clone(), Uri::from_static("https://example.com/"))?
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
        super::native_policy_tests::check_native_bodyless_precedes_caps_and_polling().await?;
        println!("ez_5_acceptance_09_spin: passed");
        Ok(())
    }
}

pub(crate) mod content_type_tests {
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
        let checked =
            result.and_then(|response| check_response(response, values, expected, retained_count));
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

    pub(crate) async fn check_converted_response_content_type_provenance() -> Result<(), EdgeError>
    {
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
