#![allow(
    clippy::shadow_reuse,
    clippy::shadow_unrelated,
    reason = "table-driven tests intentionally reuse conventional request, response, and budget names"
)]

use std::cell::Cell;
use std::future::Future as _;
use std::io::{Error as IoError, ErrorKind, Write as _};
use std::mem::take;
use std::num::NonZeroU64;
use std::rc::Rc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use bytes::Bytes;
use flate2::write::{GzEncoder, ZlibEncoder};
use futures::FutureExt as _;
use futures::channel::oneshot;
use futures::executor::block_on;
use futures::future::{poll_fn, ready};
use futures::pin_mut;
use futures::task::noop_waker_ref;
use futures_util::{StreamExt as _, stream};

use crate::body::Body;
use crate::compression::{ContentEncoding, PassthroughReason, classify_content_encoding};
use crate::error::{
    BadGatewayDecodeReason, BadGatewayReason, BudgetSource, EdgeError, RequestBodyOverflowOrigin,
    ResponseLimitReason,
};
use crate::http::{Authority, HeaderMap, HeaderValue, Method, StatusCode, Uri, request_builder};
use crate::time::{
    DEADLINE_FAR_FUTURE, DEFAULT_NO_DEADLINE_BUDGET, Deadline, MonotonicClock, MonotonicInstant,
    dispatch_budget,
};

use super::{
    HttpClient, OutboundBatch, OutboundBatchContext, OutboundBatchDriverEvent, OutboundBatchNext,
    OutboundBatchTermination, OutboundCachePolicy, OutboundCollectedSlot, OutboundDispatchRequest,
    OutboundExchangeOwner, OutboundFuture, OutboundHead, OutboundHeaderFidelity,
    OutboundHeaderInput, OutboundHttpClient, OutboundPending, OutboundPreparedBatch,
    OutboundProxyMarker, OutboundRequest, OutboundResponse, OutboundSendMode, OutboundTime,
    PROXY_HEADER, ResponseBodyDisposition, ResponseBodyPolicy, ResponseDecoding,
    ResponseHeaderLimiter, ResponseMode, apply_response_body_policy, collect_response_stream,
    enforce_payload_content_length, insert_proxy_header, limit_decoded_stream,
    limit_encoded_stream, normalize_for_dispatch, normalize_response_headers, rechunk_stream,
    validate_for_dispatch,
};

struct MockClient {
    batch_calls: AtomicUsize,
    send_calls: AtomicUsize,
    time: TestTime,
}

struct MarkerClient;

struct BodyDropSignal(Rc<Cell<usize>>);

#[derive(Clone)]
struct TestTime {
    now: Arc<Mutex<MonotonicInstant>>,
    waiters: Arc<Mutex<Vec<Waker>>>,
}

struct BatchFixture {
    cutoff: Deadline,
    started_at: MonotonicInstant,
    time: TestTime,
}

impl OutboundHttpClient for MarkerClient {
    fn begin(&self, request: OutboundDispatchRequest) -> Result<OutboundPending, EdgeError> {
        let outcome = block_on(marker_head(request)?.finish());
        Ok(OutboundPending::new(Box::pin(ready(outcome)), no_owner()))
    }

    fn dispatch_head(&self, request: OutboundDispatchRequest) -> OutboundFuture<OutboundHead> {
        Box::pin(ready(marker_head(request)))
    }

    fn progress(&self) -> Rc<dyn super::OutboundProgressRuntime> {
        Rc::new(super::IndependentOutboundProgress)
    }

    fn start_batch_until(&self, batch: OutboundPreparedBatch) -> OutboundBatch {
        let (context, slots) = batch.into_slots();
        let items = slots
            .into_iter()
            .map(|(index, request)| {
                let outcome = marker_head(request).and_then(|head| block_on(head.finish()));
                OutboundBatchDriverEvent::Item {
                    index,
                    completed_at: context.clock().now(),
                    outcome,
                }
            })
            .collect::<Vec<_>>();
        OutboundBatch::from_driver(context, stream::iter(items), no_owner())
    }

    fn time(&self) -> OutboundTime {
        TestTime::new(MonotonicInstant::now()).pair()
    }

    fn validate_supported_options(
        &self,
        _request: &OutboundRequest,
        _mode: OutboundSendMode,
    ) -> Result<(), EdgeError> {
        Ok(())
    }
}

#[expect(
    clippy::missing_trait_methods,
    reason = "`Drop::pin_drop` is unstable (`pin_ergonomics`) and conflicts with `drop`"
)]
impl Drop for BodyDropSignal {
    fn drop(&mut self) {
        self.0.set(self.0.get().saturating_add(1));
    }
}

impl OutboundHttpClient for MockClient {
    fn begin(&self, request: OutboundDispatchRequest) -> Result<OutboundPending, EdgeError> {
        let outcome =
            block_on(self.dispatch_head(request)).and_then(|head| block_on(head.finish()));
        Ok(OutboundPending::new(Box::pin(ready(outcome)), no_owner()))
    }

    fn dispatch_head(&self, request: OutboundDispatchRequest) -> OutboundFuture<OutboundHead> {
        self.send_calls.fetch_add(1, Ordering::Relaxed);
        Box::pin(ready(test_head(
            request,
            StatusCode::CREATED,
            Body::from("single"),
        )))
    }

    fn progress(&self) -> Rc<dyn super::OutboundProgressRuntime> {
        Rc::new(super::IndependentOutboundProgress)
    }

    fn start_batch_until(&self, batch: OutboundPreparedBatch) -> OutboundBatch {
        self.batch_calls.fetch_add(1, Ordering::Relaxed);
        let (context, slots) = batch.into_slots();
        let items = slots
            .into_iter()
            .rev()
            .map(|(index, request)| {
                self.time.advance_to(
                    self.time
                        .now()
                        .checked_add(Duration::from_millis(1))
                        .expect("completion instant"),
                );
                let outcome = if request.request().method() == Method::DELETE {
                    Err(EdgeError::bad_gateway("mock failure"))
                } else {
                    test_head(request, StatusCode::OK, Body::empty())
                        .and_then(|head| block_on(head.finish()))
                };
                OutboundBatchDriverEvent::Item {
                    index,
                    completed_at: context.clock().now(),
                    outcome,
                }
            })
            .collect::<Vec<_>>();
        OutboundBatch::from_driver(context, stream::iter(items), no_owner())
    }

    fn time(&self) -> OutboundTime {
        self.time.pair()
    }

    fn validate_supported_options(
        &self,
        _request: &OutboundRequest,
        _mode: OutboundSendMode,
    ) -> Result<(), EdgeError> {
        Ok(())
    }
}

impl TestTime {
    fn advance_to(&self, now: MonotonicInstant) {
        *self.now.lock().expect("fake clock") = now;
        for waker in take(&mut *self.waiters.lock().expect("fake waiters")) {
            waker.wake();
        }
    }

    fn new(now: MonotonicInstant) -> Self {
        Self {
            now: Arc::new(Mutex::new(now)),
            waiters: Arc::default(),
        }
    }

    fn now(&self) -> MonotonicInstant {
        *self.now.lock().expect("fake clock")
    }

    fn pair(&self) -> OutboundTime {
        let now = Arc::clone(&self.now);
        let waiters = Arc::clone(&self.waiters);
        OutboundTime::new(
            MonotonicClock::new(move || *now.lock().expect("fake clock")),
            move |deadline, clock| {
                let waiters = Arc::clone(&waiters);
                Box::pin(poll_fn(move |cx| {
                    if deadline.is_expired_at(clock.now()) {
                        Poll::Ready(Ok(()))
                    } else {
                        waiters
                            .lock()
                            .expect("fake waiters")
                            .push(cx.waker().clone());
                        Poll::Pending
                    }
                }))
            },
        )
    }
}

impl BatchFixture {
    fn context(&self, count: usize) -> OutboundBatchContext {
        let request = OutboundRequest::get("https://example.com").expect("request");
        let budget = dispatch_budget(&request, self.started_at, Some(self.cutoff)).expect("budget");
        OutboundBatchContext::new(
            self.started_at,
            self.time.pair(),
            self.cutoff,
            vec![Some(budget); count],
            Vec::new(),
            super::OutboundScope::capture(Rc::new(super::IndependentOutboundProgress)),
        )
    }

    fn new() -> Self {
        let started_at = MonotonicInstant::now();
        Self {
            started_at,
            cutoff: Deadline::at_instant(
                started_at
                    .checked_add(Duration::from_secs(1))
                    .expect("cutoff instant"),
            ),
            time: TestTime::new(started_at),
        }
    }

    fn observation(&self, index: usize) -> OutboundBatchDriverEvent {
        OutboundBatchDriverEvent::Item {
            index,
            completed_at: self.time.now(),
            outcome: Ok(response_with_body(Body::empty())),
        }
    }
}

fn marker_head(request: OutboundDispatchRequest) -> Result<OutboundHead, EdgeError> {
    if request.request().proxy_marker_setting() == OutboundProxyMarker::Inherit {
        return Err(EdgeError::internal(anyhow::anyhow!(
            "client failed to resolve marker policy"
        )));
    }
    test_head(request, StatusCode::OK, Body::empty())
}

fn test_head(
    request: OutboundDispatchRequest,
    status: StatusCode,
    body: Body,
) -> Result<OutboundHead, EdgeError> {
    let (_parts, context) = request.into_parts();
    OutboundHead::new(
        status,
        OutboundHeaderInput::new(
            "test",
            HeaderMap::new(),
            OutboundHeaderFidelity::AdapterVisibleFields,
        ),
        match body.into_stream() {
            Err(body) => Box::pin(stream::once(ready(Ok(body
                .into_bytes()
                .expect("buffered body"))))),
            Ok(source) => source,
        },
        context,
        no_owner(),
    )
}

fn no_owner() -> OutboundExchangeOwner {
    OutboundExchangeOwner::new(|| {})
}

#[test]
fn proxy_marker_overwrite_and_normalization_precedence_are_unchanged() {
    for setting in [
        OutboundProxyMarker::Inherit,
        OutboundProxyMarker::On,
        OutboundProxyMarker::Off,
    ] {
        for nominated in [false, true] {
            let mut headers = HeaderMap::new();
            headers.append(PROXY_HEADER, HeaderValue::from_static("upstream"));
            headers.append(PROXY_HEADER, HeaderValue::from_static("other"));
            if nominated {
                headers.insert("connection", HeaderValue::from_static(PROXY_HEADER));
            }
            let mut limiter = ResponseHeaderLimiter::new(None, None);
            limiter.observe(&headers).expect("upstream accounting");
            normalize_response_headers(&Method::GET, StatusCode::OK, &mut headers)
                .expect("normalize");
            insert_proxy_header(
                &mut headers,
                &mut limiter,
                setting,
                HeaderValue::from_static("test"),
            )
            .expect("marker policy");
            let values = headers
                .get_all(PROXY_HEADER)
                .iter()
                .map(HeaderValue::as_bytes)
                .collect::<Vec<_>>();
            let expected = if setting != OutboundProxyMarker::Off {
                vec![b"test".as_slice()]
            } else if nominated {
                vec![]
            } else {
                vec![b"upstream".as_slice(), b"other".as_slice()]
            };
            assert_eq!(values, expected);
        }
    }
}

#[test]
fn proxy_marker_request_setting_round_trips() {
    let request = OutboundRequest::get("https://example.com").expect("request");
    assert_eq!(request.proxy_marker_setting(), OutboundProxyMarker::Inherit);
    let setting: crate::OutboundProxyMarker = OutboundProxyMarker::Off;
    let parts = request.proxy_marker(setting).into_parts();
    assert_eq!(parts.proxy_marker, setting);
    let request = OutboundRequest::from_parts(parts).expect("round trip");
    assert_eq!(request.proxy_marker_setting(), setting);
    assert_eq!(
        request
            .proxy_marker(OutboundProxyMarker::On)
            .proxy_marker_setting(),
        OutboundProxyMarker::On
    );
}

#[test]
fn proxy_marker_client_resolves_overrides_for_supplied_send_and_batch() {
    let default_client = HttpClient::with_client(MarkerClient);
    let default_arc = HttpClient::new(Arc::new(MarkerClient));
    assert!(default_client.proxy_marker_enabled());
    assert!(default_arc.proxy_marker_enabled());
    let off_client = default_client.clone().with_proxy_marker(false);
    assert!(
        default_client.proxy_marker_enabled(),
        "configuring a clone must not mutate the original handle"
    );
    assert!(!off_client.proxy_marker_enabled());
    for client in [default_client, off_client] {
        for setting in [
            OutboundProxyMarker::Inherit,
            OutboundProxyMarker::On,
            OutboundProxyMarker::Off,
        ] {
            let expected = match setting {
                OutboundProxyMarker::Inherit => client.proxy_marker_enabled(),
                OutboundProxyMarker::On => true,
                OutboundProxyMarker::Off => false,
            };
            let request = OutboundRequest::get("https://example.com")
                .expect("request")
                .proxy_marker(setting);
            assert_eq!(client.effective_proxy_marker_enabled(&request), expected);
            assert_eq!(
                request.proxy_marker_setting(),
                setting,
                "inspection is immutable"
            );
            let response = block_on(client.send(request)).expect("supplied response");
            assert_eq!(response.headers().contains_key(PROXY_HEADER), expected);
        }
        let requests = [
            OutboundProxyMarker::Inherit,
            OutboundProxyMarker::On,
            OutboundProxyMarker::Off,
        ]
        .into_iter()
        .map(|setting| {
            OutboundRequest::get("https://example.com")
                .expect("request")
                .proxy_marker(setting)
        })
        .collect();
        let results =
            block_on(client.send_all_until(requests, Deadline::after(Duration::from_secs(1))))
                .expect("batch");
        for (slot, expected) in
            results
                .into_parts()
                .0
                .into_iter()
                .zip([client.proxy_marker_enabled(), true, false])
        {
            let OutboundCollectedSlot::Resolved(result) = slot else {
                panic!("terminal slot");
            };
            let response = result.outcome.expect("response");
            assert_eq!(response.headers().contains_key(PROXY_HEADER), expected);
        }
    }
}

#[test]
fn proxy_marker_inspector_neither_validates_nor_polls_body() {
    let polls = Rc::new(Cell::new(0_usize));
    let counted = Rc::clone(&polls);
    let body = Body::from_stream(stream::poll_fn(move |_context| {
        counted.set(counted.get() + 1);
        Poll::Pending
    }));
    let request = OutboundRequest::get("https://example.com")
        .expect("request")
        .body(body)
        .max_brotli_window_bits(0);
    let client = HttpClient::with_client(MarkerClient).with_proxy_marker(false);
    assert!(!client.effective_proxy_marker_enabled(&request));
    assert_eq!(request.proxy_marker_setting(), OutboundProxyMarker::Inherit);
    assert_eq!(polls.get(), 0);
}

#[test]
fn proxy_marker_off_preserves_upstream_and_consumes_no_header_budget() {
    let mut upstream = HeaderMap::new();
    upstream.append(PROXY_HEADER, HeaderValue::from_static("upstream"));
    upstream.append(PROXY_HEADER, HeaderValue::from_static("other"));
    upstream.insert("x", HeaderValue::from_static("y"));
    let bytes = upstream
        .iter()
        .map(|(name, value)| {
            u64::try_from(name.as_str().len() + value.as_bytes().len()).expect("header size")
        })
        .sum::<u64>();
    for adapter in ["axum", "cloudflare", "fastly", "spin"] {
        for (byte_cap, count_cap, reason) in [
            (Some(bytes), None, ResponseLimitReason::HeaderBytes),
            (None, Some(3), ResponseLimitReason::HeaderCount),
        ] {
            for setting in [
                OutboundProxyMarker::Off,
                OutboundProxyMarker::On,
                OutboundProxyMarker::Inherit,
            ] {
                let mut headers = upstream.clone();
                let mut limiter = ResponseHeaderLimiter::new(byte_cap, count_cap);
                limiter.observe(&headers).expect("exact upstream limit");
                let result = insert_proxy_header(
                    &mut headers,
                    &mut limiter,
                    setting,
                    HeaderValue::from_static(adapter),
                );
                if setting == OutboundProxyMarker::Off {
                    result.expect("off adds no synthetic charge");
                } else {
                    assert!(
                        matches!(result, Err(EdgeError::ResponseTooLarge { reason: actual, .. }) if actual == reason)
                    );
                }
                assert_eq!(
                    headers, upstream,
                    "off and failed insertions must leave all upstream values intact"
                );
            }
        }
    }
}

#[test]
fn ready_driver_retains_terminal_timestamps_before_delivering_preflight_failures() {
    let fixture = BatchFixture::new();
    fixture
        .time
        .advance_to(fixture.started_at + Duration::from_millis(2));
    let context = OutboundBatchContext::new(
        fixture.started_at,
        fixture.time.pair(),
        fixture.cutoff,
        vec![
            None,
            Some(
                dispatch_budget(
                    &OutboundRequest::get("https://example.com").expect("request"),
                    fixture.started_at,
                    Some(fixture.cutoff),
                )
                .expect("budget"),
            ),
        ],
        vec![(
            0,
            fixture.time.now(),
            Err(EdgeError::bad_request("preflight")),
        )],
        super::OutboundScope::capture(Rc::new(super::IndependentOutboundProgress)),
    );
    fixture
        .time
        .advance_to(fixture.started_at + Duration::from_millis(5));
    let event = OutboundBatchDriverEvent::Item {
        index: 1,
        completed_at: fixture.time.now(),
        outcome: Err(EdgeError::bad_request("ready slot")),
    };
    let mut batch = OutboundBatch::from_driver(context, stream::iter([event]), no_owner());
    let OutboundBatchNext::Item(first) = block_on(batch.next()).expect("preflight") else {
        panic!("expected preflight item");
    };
    assert_eq!(first.index(), 0);
    assert_eq!(first.result().elapsed, Duration::from_millis(2));
    fixture
        .time
        .advance_to(fixture.started_at + Duration::from_secs(2));
    let OutboundBatchNext::Item(second) = block_on(batch.next()).expect("retained result") else {
        panic!("expected retained result");
    };
    assert_eq!(second.index(), 1);
    assert_eq!(second.result().elapsed, Duration::from_millis(5));
    assert_eq!(
        second
            .result()
            .outcome
            .as_ref()
            .expect_err("retained failure")
            .message(),
        "ready slot"
    );
    assert!(matches!(
        block_on(batch.next()).expect("EOF"),
        OutboundBatchNext::Finished(OutboundBatchTermination::Completed)
    ));
}

#[test]
fn ready_driver_cutoff_depends_on_observation_not_timeout_attribution() {
    let fixture = BatchFixture::new();
    fixture
        .time
        .advance_to(fixture.started_at + Duration::from_millis(5));
    let early = OutboundBatchDriverEvent::Item {
        index: 1,
        completed_at: fixture.time.now(),
        outcome: Err(EdgeError::gateway_timeout_caused(
            "phase timeout",
            BudgetSource::BatchCutoff,
        )),
    };
    fixture.time.advance_to(fixture.cutoff.instant());
    let at_cutoff = OutboundBatchDriverEvent::Item {
        index: 0,
        completed_at: fixture.time.now(),
        outcome: Err(EdgeError::bad_request("late")),
    };
    let batch = OutboundBatch::from_driver(
        fixture.context(2),
        stream::iter([early, at_cutoff, OutboundBatchDriverEvent::Cutoff]),
        no_owner(),
    );
    let result = block_on(batch.collect()).expect("observation_cutoff");
    assert_eq!(result.termination(), OutboundBatchTermination::Cutoff);
    assert!(matches!(
        result.slots().first(),
        Some(OutboundCollectedSlot::Unresolved)
    ));
    let OutboundCollectedSlot::Resolved(early_result) = &result.slots()[1] else {
        panic!("early slot retained");
    };
    assert!(matches!(
        early_result.outcome,
        Err(EdgeError::GatewayTimeout {
            cause: BudgetSource::BatchCutoff,
            ..
        })
    ));
}

#[test]
fn ready_driver_orders_controlled_completions_and_drops_cancelled_work() {
    let fixture = BatchFixture::new();
    let drops = Rc::new(Cell::new(0));
    let signal = BodyDropSignal(Rc::clone(&drops));
    let (send_first, receive_first) = oneshot::channel::<()>();
    let (send_second, receive_second) = oneshot::channel::<()>();
    let first_time = fixture.time.clone();
    let first = async move {
        let _signal = signal;
        receive_first.await.expect("first completion");
        OutboundBatchDriverEvent::Item {
            index: 0,
            completed_at: first_time.now(),
            outcome: Err(EdgeError::bad_request("first")),
        }
    }
    .boxed_local();
    let second_time = fixture.time.clone();
    let second = async move {
        receive_second.await.expect("second completion");
        OutboundBatchDriverEvent::Item {
            index: 1,
            completed_at: second_time.now(),
            outcome: Err(EdgeError::bad_request("second")),
        }
    }
    .boxed_local();
    let mut batch = OutboundBatch::from_driver(
        fixture.context(2),
        stream::FuturesUnordered::from_iter([first, second]),
        no_owner(),
    );
    fixture
        .time
        .advance_to(fixture.started_at + Duration::from_millis(3));
    send_second.send(()).expect("second ready");
    let OutboundBatchNext::Item(item) = block_on(batch.next()).expect("second completes first")
    else {
        panic!("expected completion");
    };
    assert_eq!(item.index(), 1);
    assert_eq!(item.result().elapsed, Duration::from_millis(3));
    assert_eq!(batch.cancel(), vec![0]);
    assert_eq!(drops.get(), 1);
    assert!(send_first.send(()).is_err());
}

#[test]
fn ready_driver_validates_empty_and_premature_drivers() {
    let fixture = BatchFixture::new();
    let completed = block_on(
        OutboundBatch::from_driver(fixture.context(0), stream::empty(), no_owner()).collect(),
    )
    .expect("empty completion");
    assert_eq!(completed.termination(), OutboundBatchTermination::Completed);
    let budget = dispatch_budget(
        &OutboundRequest::get("https://example.com").expect("request"),
        fixture.started_at,
        Some(fixture.cutoff),
    )
    .expect("budget");
    let context = OutboundBatchContext::new(
        fixture.started_at,
        fixture.time.pair(),
        fixture.cutoff,
        vec![Some(budget), None],
        vec![(
            1,
            fixture.time.now(),
            Err(EdgeError::bad_request("preflight")),
        )],
        super::OutboundScope::capture(Rc::new(super::IndependentOutboundProgress)),
    );
    let failure =
        block_on(OutboundBatch::from_driver(context, stream::empty(), no_owner()).collect())
            .expect_err("missing future");
    assert!(matches!(failure.error(), EdgeError::Internal { .. }));
    assert!(matches!(
        failure.slots().get(1),
        Some(OutboundCollectedSlot::Resolved(_))
    ));
}

#[test]
fn ready_driver_cutoff_releases_pending_work_before_reporting_finished() {
    let fixture = BatchFixture::new();
    let drops = Rc::new(Cell::new(0));
    let signal = BodyDropSignal(Rc::clone(&drops));
    let (cancelled, pending_work) = oneshot::channel::<()>();
    fixture.time.advance_to(fixture.cutoff.instant());
    let late = OutboundBatchDriverEvent::Item {
        index: 0,
        completed_at: fixture.time.now(),
        outcome: Err(EdgeError::bad_request("late completion")),
    };
    let mut batch = OutboundBatch::from_driver(
        fixture.context(2),
        stream::iter([late, OutboundBatchDriverEvent::Cutoff]),
        OutboundExchangeOwner::new(move || {
            drop(signal);
            drop(pending_work);
        }),
    );
    assert!(matches!(
        block_on(batch.next()).expect("cutoff"),
        OutboundBatchNext::Finished(OutboundBatchTermination::Cutoff)
    ));
    assert_eq!(
        drops.get(),
        1,
        "finished cutoff must release pending work even while batch remains owned"
    );
    assert_eq!(batch.cancel(), vec![0, 1]);
    assert_eq!(drops.get(), 1);
    assert!(
        cancelled.send(()).is_err(),
        "pending work released at cutoff"
    );
}

#[test]
fn batch_terminal_and_poisoned_states_release_the_driver_while_retained() {
    let fixture = BatchFixture::new();
    for (count, events) in [
        (1, vec![OutboundBatchDriverEvent::Cutoff]),
        (
            1,
            vec![OutboundBatchDriverEvent::Failed(EdgeError::internal(
                anyhow::anyhow!("fixture failure"),
            ))],
        ),
        (1, vec![fixture.observation(1)]),
        (1, vec![fixture.observation(0), fixture.observation(0)]),
        (1, vec![]),
        (0, vec![]),
        (0, vec![OutboundBatchDriverEvent::Cutoff]),
    ] {
        let drops = Rc::new(Cell::new(0));
        let signal = BodyDropSignal(Rc::clone(&drops));
        let mut remaining = events.into_iter();
        let driver = stream::poll_fn(move |_context| {
            let _keep_driver_owned = &signal;
            Poll::Ready(remaining.next())
        });
        fixture.time.advance_to(fixture.cutoff.instant());
        let mut batch = OutboundBatch::from_driver(fixture.context(count), driver, no_owner());
        while matches!(block_on(batch.next()), Ok(OutboundBatchNext::Item(_))) {}
        assert_eq!(
            drops.get(),
            1,
            "a retained terminal batch must not retain its driver"
        );
        let _terminal_again = block_on(batch.next());
        drop(batch);
        assert_eq!(drops.get(), 1, "driver must release exactly once");
    }
}

fn body_policy() -> ResponseBodyPolicy {
    ResponseBodyPolicy {
        max_brotli_window_bits: 24,
        max_chunk_bytes: None,
        max_decoded_response_bytes: Some(32),
        max_decoder_bytes: super::DEFAULT_MAX_DECODER_BYTES,
        max_encoded_response_bytes: Some(256),
        response_decoding: ResponseDecoding::Decode,
        response_mode: ResponseMode::Buffered { max_bytes: 32 },
        status: StatusCode::OK,
    }
}

#[test]
fn response_decoding_request_option_round_trips() {
    let request = OutboundRequest::get("https://example.com").expect("request");
    assert_eq!(
        request.response_decoding_setting(),
        ResponseDecoding::Decode
    );
    let parts = request
        .response_decoding(ResponseDecoding::Original)
        .into_parts();
    assert_eq!(parts.response_decoding, ResponseDecoding::Original);
    let request = OutboundRequest::from_parts(parts).expect("round trip");
    assert_eq!(
        request.response_decoding_setting(),
        ResponseDecoding::Original
    );
    assert_eq!(
        request
            .response_decoding(ResponseDecoding::Decode)
            .response_decoding_setting(),
        ResponseDecoding::Decode
    );
    let choice: crate::ResponseDecoding = ResponseDecoding::Original;
    assert_eq!(choice, ResponseDecoding::Original);
}

#[test]
fn original_coding_skips_decoder_but_keeps_transport_caps() {
    for coding in ["gzip", "deflate", "br", "unknown", "gzip, br", "gzip;"] {
        for response_mode in [
            ResponseMode::Buffered { max_bytes: 4 },
            ResponseMode::Streamed,
        ] {
            for (choice, status) in [
                (ResponseDecoding::Original, StatusCode::OK),
                (ResponseDecoding::Original, StatusCode::PARTIAL_CONTENT),
                (ResponseDecoding::Decode, StatusCode::PARTIAL_CONTENT),
            ] {
                let mut headers = HeaderMap::new();
                headers.insert("content-encoding", HeaderValue::from_static(coding));
                headers.insert("content-length", HeaderValue::from_static("4"));
                headers.insert("content-range", HeaderValue::from_static("bytes 0-3/8"));
                let original = headers.clone();
                let source = stream::iter([Ok(Bytes::from_static(b"raw!"))]).boxed_local();
                let policy = ResponseBodyPolicy {
                    max_decoded_response_bytes: Some(1),
                    max_decoder_bytes: 0,
                    max_encoded_response_bytes: Some(4),
                    max_chunk_bytes: NonZeroU64::new(2),
                    response_decoding: choice,
                    response_mode,
                    status,
                    ..body_policy()
                };
                let output =
                    apply_response_body_policy(source, &mut headers, policy).expect("raw policy");
                assert_eq!(headers, original, "{coding} {choice:?} {status}");
                let chunks = block_on(output.collect::<Vec<_>>());
                let mut bytes = Vec::new();
                for chunk in chunks {
                    let chunk = chunk.expect("no decoder and no decoded cap");
                    assert!(chunk.len() <= 2);
                    bytes.extend_from_slice(&chunk);
                }
                assert_eq!(bytes, b"raw!");
            }
        }
    }
}

#[test]
fn original_coding_declared_caps_fail_before_polling() {
    for (encoded_cap, response_mode, reason) in [
        (
            3,
            ResponseMode::Buffered { max_bytes: 4 },
            ResponseLimitReason::EncodedBody,
        ),
        (
            4,
            ResponseMode::Buffered { max_bytes: 3 },
            ResponseLimitReason::BufferedBody,
        ),
        (3, ResponseMode::Streamed, ResponseLimitReason::EncodedBody),
    ] {
        for (choice, status) in [
            (ResponseDecoding::Original, StatusCode::OK),
            (ResponseDecoding::Original, StatusCode::PARTIAL_CONTENT),
            (ResponseDecoding::Decode, StatusCode::PARTIAL_CONTENT),
        ] {
            let polls = Rc::new(Cell::new(0_usize));
            let counted = Rc::clone(&polls);
            let source = stream::poll_fn(move |_context| {
                counted.set(counted.get() + 1);
                Poll::Ready(None)
            })
            .boxed_local();
            let mut headers = HeaderMap::new();
            headers.insert("content-encoding", HeaderValue::from_static("gzip"));
            headers.insert("content-length", HeaderValue::from_static("4"));
            let original = headers.clone();
            let policy = ResponseBodyPolicy {
                max_encoded_response_bytes: Some(encoded_cap),
                response_decoding: choice,
                response_mode,
                status,
                ..body_policy()
            };
            let error = apply_response_body_policy(source, &mut headers, policy)
                .err()
                .expect("declared cap");
            assert!(
                matches!(error, EdgeError::ResponseTooLarge { reason: actual, .. } if actual == reason)
            );
            assert_eq!(polls.get(), 0);
            assert_eq!(headers, original);
        }
    }
}

#[test]
fn original_coding_observed_caps_and_source_errors_are_preserved() {
    for response_mode in [
        ResponseMode::Buffered { max_bytes: 4 },
        ResponseMode::Streamed,
    ] {
        let policy = ResponseBodyPolicy {
            response_decoding: ResponseDecoding::Original,
            response_mode,
            max_encoded_response_bytes: Some(3),
            ..body_policy()
        };
        let mut headers = HeaderMap::new();
        headers.insert("content-encoding", HeaderValue::from_static("gzip"));
        let source = stream::iter([Ok(Bytes::from_static(b"raw!"))]).boxed_local();
        let output = apply_response_body_policy(source, &mut headers, policy).expect("lazy policy");
        assert!(matches!(
            block_on(collect_response_stream(output, 4)),
            Err(EdgeError::ResponseTooLarge {
                reason: ResponseLimitReason::EncodedBody,
                ..
            })
        ));

        let source = stream::iter([Err(EdgeError::gateway_timeout_caused(
            "source timeout",
            BudgetSource::RequestDeadline,
        ))])
        .boxed_local();
        let output = apply_response_body_policy(source, &mut headers, policy).expect("lazy policy");
        assert!(matches!(
            block_on(collect_response_stream(output, 4)),
            Err(EdgeError::GatewayTimeout {
                cause: BudgetSource::RequestDeadline,
                ..
            })
        ));
    }
    let mut headers = HeaderMap::new();
    headers.insert("content-encoding", HeaderValue::from_static("br"));
    let source = stream::iter([Ok(Bytes::from_static(b"raw!"))]).boxed_local();
    let policy = ResponseBodyPolicy {
        response_decoding: ResponseDecoding::Original,
        ..body_policy()
    };
    let output = apply_response_body_policy(source, &mut headers, policy).expect("raw policy");
    assert!(matches!(
        block_on(collect_response_stream(output, 3)),
        Err(EdgeError::ResponseTooLarge {
            reason: ResponseLimitReason::BufferedBody,
            ..
        })
    ));
}

#[test]
fn original_identity_retains_declared_and_observed_decoded_caps() {
    for choice in [ResponseDecoding::Decode, ResponseDecoding::Original] {
        for status in [StatusCode::OK, StatusCode::PARTIAL_CONTENT] {
            for response_mode in [
                ResponseMode::Buffered { max_bytes: 20 },
                ResponseMode::Streamed,
            ] {
                for declared in [false, true] {
                    for (bytes, exceeds) in [
                        (b"0123456789".as_slice(), false),
                        (b"0123456789x".as_slice(), true),
                    ] {
                        let polls = Rc::new(Cell::new(0_usize));
                        let counted = Rc::clone(&polls);
                        let mut chunk = Some(Bytes::copy_from_slice(bytes));
                        let source = stream::poll_fn(move |_context| {
                            counted.set(counted.get() + 1);
                            Poll::Ready(chunk.take().map(Ok))
                        })
                        .boxed_local();
                        let mut headers = HeaderMap::new();
                        if declared {
                            headers.insert(
                                "content-length",
                                HeaderValue::from_str(&bytes.len().to_string()).expect("length"),
                            );
                        }
                        let policy = ResponseBodyPolicy {
                            response_decoding: choice,
                            status,
                            response_mode,
                            max_decoded_response_bytes: Some(10),
                            max_encoded_response_bytes: Some(20),
                            ..body_policy()
                        };
                        let result = apply_response_body_policy(source, &mut headers, policy)
                            .and_then(|output| block_on(collect_response_stream(output, 20)));
                        if exceeds {
                            assert!(matches!(
                                result,
                                Err(EdgeError::ResponseTooLarge {
                                    reason: ResponseLimitReason::DecodedBody,
                                    ..
                                })
                            ));
                        } else {
                            assert_eq!(result.expect("exact cap"), bytes);
                        }
                        if declared && exceeds {
                            assert_eq!(polls.get(), 0);
                        }
                    }
                }
            }
        }
    }
}

#[test]
fn body_policy_rejects_declared_limits_without_polling_or_rewriting_headers() {
    let polls = Rc::new(Cell::new(0_usize));
    let counted = Rc::clone(&polls);
    let source = stream::poll_fn(move |_context| {
        counted.set(counted.get() + 1);
        Poll::Ready(None)
    })
    .boxed_local();
    let mut headers = HeaderMap::new();
    headers.insert("content-encoding", HeaderValue::from_static("gzip"));
    headers.insert("content-length", HeaderValue::from_static("257"));
    let error = apply_response_body_policy(source, &mut headers, body_policy())
        .err()
        .expect("encoded preflight");
    assert!(matches!(
        error,
        EdgeError::ResponseTooLarge {
            reason: ResponseLimitReason::EncodedBody,
            ..
        }
    ));
    assert_eq!(polls.get(), 0);
    assert!(headers.contains_key("content-encoding"));
    assert!(headers.contains_key("content-length"));
}

#[test]
fn original_coding_preserves_normalization_precedence() {
    for choice in [ResponseDecoding::Decode, ResponseDecoding::Original] {
        for status in [StatusCode::OK, StatusCode::PARTIAL_CONTENT] {
            for response_mode in [
                ResponseMode::Streamed,
                ResponseMode::Buffered { max_bytes: 20 },
            ] {
                let policy = ResponseBodyPolicy {
                    response_decoding: choice,
                    status,
                    response_mode,
                    max_decoded_response_bytes: Some(1),
                    ..body_policy()
                };
                let mut headers = HeaderMap::new();
                headers.insert(
                    "connection",
                    HeaderValue::from_static("content-encoding, content-length, etag"),
                );
                headers.insert("content-encoding", HeaderValue::from_static("gzip"));
                headers.insert("content-length", HeaderValue::from_static("malformed"));
                headers.insert("etag", HeaderValue::from_static("\"removed\""));
                headers.insert(
                    "last-modified",
                    HeaderValue::from_static("Wed, 21 Oct 2015 07:28:00 GMT"),
                );
                normalize_response_headers(&Method::GET, status, &mut headers)
                    .expect("nominations before length validation");
                let normalized = headers.clone();
                let source = stream::iter([Ok(Bytes::from_static(b"ab"))]).boxed_local();
                let output = apply_response_body_policy(source, &mut headers, policy)
                    .expect("identity policy");
                assert_eq!(headers, normalized);
                assert!(!headers.contains_key("etag"));
                assert!(!headers.contains_key("content-encoding"));
                assert!(!headers.contains_key("content-length"));
                assert!(matches!(
                    block_on(collect_response_stream(output, 20)),
                    Err(EdgeError::ResponseTooLarge {
                        reason: ResponseLimitReason::DecodedBody,
                        ..
                    })
                ));

                let mut headers = HeaderMap::new();
                headers.insert(
                    "content-encoding",
                    HeaderValue::from_bytes(&[0xff]).expect("opaque coding"),
                );
                headers.insert(
                    "etag",
                    HeaderValue::from_bytes(&[0xff]).expect("opaque etag"),
                );
                normalize_response_headers(&Method::GET, status, &mut headers)
                    .expect("normalization");
                assert!(!headers.contains_key("etag"));
                assert_eq!(
                    classify_content_encoding(&headers),
                    ContentEncoding::Passthrough(PassthroughReason::Malformed)
                );
                let normalized = headers.clone();
                let source = stream::iter([Ok(Bytes::from_static(b"ab"))]).boxed_local();
                let output = apply_response_body_policy(source, &mut headers, policy)
                    .expect("malformed coding passthrough");
                assert_eq!(headers, normalized);
                assert_eq!(
                    block_on(collect_response_stream(output, 20)).expect("raw body"),
                    b"ab".as_slice()
                );

                headers.insert("content-length", HeaderValue::from_static("malformed"));
                assert!(matches!(
                    normalize_response_headers(&Method::GET, status, &mut headers),
                    Err(EdgeError::BadGateway {
                        reason: BadGatewayReason::Protocol,
                        ..
                    })
                ));
            }
        }
    }
}

#[test]
fn original_coding_retains_valid_compressed_bytes_and_representation_fields() {
    let plain = b"decoded body";
    let mut gzip = GzEncoder::new(Vec::new(), flate2::Compression::default());
    gzip.write_all(plain).expect("gzip");
    let mut deflate = ZlibEncoder::new(Vec::new(), flate2::Compression::default());
    deflate.write_all(plain).expect("deflate");
    let mut brotli = Vec::new();
    let mut compressor = brotli::CompressorWriter::new(&mut brotli, 4096, 5, 21);
    compressor.write_all(plain).expect("brotli");
    drop(compressor);
    for (coding, encoded) in [
        ("gzip", gzip.finish().expect("gzip finish")),
        ("deflate", deflate.finish().expect("deflate finish")),
        ("br", brotli),
    ] {
        for response_mode in [
            ResponseMode::Buffered { max_bytes: 256 },
            ResponseMode::Streamed,
        ] {
            for (response_decoding, status) in [
                (ResponseDecoding::Original, StatusCode::OK),
                (ResponseDecoding::Original, StatusCode::PARTIAL_CONTENT),
                (ResponseDecoding::Decode, StatusCode::PARTIAL_CONTENT),
            ] {
                let mut headers = HeaderMap::new();
                for (name, value) in [
                    ("etag", "\"strong\""),
                    ("last-modified", "Wed, 21 Oct 2015 07:28:00 GMT"),
                    ("vary", "Accept-Encoding"),
                    ("content-range", "bytes 0-3/8"),
                    ("accept-ranges", "bytes"),
                    ("cache-control", "public, max-age=60"),
                    ("content-digest", "sha-256=:c2hhMjU2:"),
                ] {
                    headers.insert(name, HeaderValue::from_static(value));
                }
                headers.insert("content-encoding", HeaderValue::from_static(coding));
                headers.insert(
                    "content-length",
                    HeaderValue::from_str(&encoded.len().to_string()).expect("length"),
                );
                normalize_response_headers(&Method::GET, status, &mut headers)
                    .expect("normalization");
                let normalized = headers.clone();
                let policy = ResponseBodyPolicy {
                    response_decoding,
                    status,
                    response_mode,
                    max_decoder_bytes: 0,
                    max_decoded_response_bytes: Some(1),
                    ..body_policy()
                };
                let source = stream::iter([Ok(Bytes::copy_from_slice(&encoded))]).boxed_local();
                let output =
                    apply_response_body_policy(source, &mut headers, policy).expect("raw policy");
                assert_eq!(headers, normalized);
                assert_eq!(
                    block_on(collect_response_stream(output, 256)).expect("original bytes"),
                    encoded
                );
            }
        }
    }
}

#[test]
fn body_policy_decodes_recognized_codings_before_rechunking() {
    let plain = b"decoded body";
    let mut gzip = GzEncoder::new(Vec::new(), flate2::Compression::default());
    gzip.write_all(plain).expect("gzip");
    let mut deflate = ZlibEncoder::new(Vec::new(), flate2::Compression::default());
    deflate.write_all(plain).expect("deflate");
    let mut brotli = Vec::new();
    let mut compressor = brotli::CompressorWriter::new(&mut brotli, 4096, 5, 21);
    compressor.write_all(plain).expect("brotli");
    drop(compressor);
    for (coding, encoded) in [
        ("gzip", gzip.finish().expect("gzip finish")),
        ("deflate", deflate.finish().expect("deflate finish")),
        ("br", brotli),
    ] {
        let mut headers = HeaderMap::new();
        headers.insert("content-encoding", HeaderValue::from_static(coding));
        headers.insert(
            "content-length",
            HeaderValue::from_str(&encoded.len().to_string()).expect("length"),
        );
        let source = stream::iter([Ok(Bytes::from(encoded))]).boxed_local();
        let mut policy = body_policy();
        policy.max_chunk_bytes = NonZeroU64::new(2);
        let output = apply_response_body_policy(source, &mut headers, policy).expect("policy");
        assert!(!headers.contains_key("content-encoding"));
        assert!(!headers.contains_key("content-length"));
        let chunks = block_on(output.collect::<Vec<_>>());
        let mut collected = Vec::new();
        for chunk in chunks {
            let bytes = chunk.expect("decoded chunk");
            assert!(bytes.len() <= 2);
            collected.extend_from_slice(&bytes);
        }
        assert_eq!(collected, plain);
    }
}

#[test]
fn provider_diagnostics_survive_response_body_policy_and_collection() {
    use crate::error_detail::{FastlyFailureCause, ProviderFailureDetail, ProviderFailurePhase};

    let detail = ProviderFailureDetail::Fastly {
        phase: ProviderFailurePhase::ResponseBodyRead,
        cause: FastlyFailureCause::IoError {
            kind: ErrorKind::UnexpectedEof,
        },
    };
    let plain = b"diagnostic body";
    let mut gzip = GzEncoder::new(Vec::new(), flate2::Compression::default());
    gzip.write_all(plain).expect("gzip");
    let mut deflate = ZlibEncoder::new(Vec::new(), flate2::Compression::default());
    deflate.write_all(plain).expect("deflate");
    let mut brotli = Vec::new();
    let mut compressor = brotli::CompressorWriter::new(&mut brotli, 4096, 5, 21);
    compressor.write_all(plain).expect("brotli");
    drop(compressor);
    for (coding, encoded) in [
        ("identity", plain.to_vec()),
        ("gzip", gzip.finish().expect("gzip finish")),
        ("deflate", deflate.finish().expect("deflate finish")),
        ("br", brotli),
    ] {
        for response_decoding in [ResponseDecoding::Decode, ResponseDecoding::Original] {
            for status in [StatusCode::OK, StatusCode::PARTIAL_CONTENT] {
                for error in [
                    EdgeError::bad_request_with_detail("source rejected", detail),
                    EdgeError::bad_gateway_with_detail(
                        "source failed",
                        BadGatewayReason::Transport,
                        detail,
                    ),
                    EdgeError::gateway_timeout_with_detail(
                        "source timed out",
                        BudgetSource::RequestDeadline,
                        detail,
                    ),
                    EdgeError::internal_with_detail(IoError::from_raw_os_error(42), detail),
                ] {
                    let expected_display = error.to_string();
                    let expected_message = error.message();
                    let expected_kind = error.kind();
                    let expected_status = error.status();
                    let source = stream::iter([Ok(Bytes::copy_from_slice(&encoded)), Err(error)])
                        .boxed_local();
                    let mut headers = HeaderMap::new();
                    headers.insert("content-encoding", HeaderValue::from_static(coding));
                    let mut policy = body_policy();
                    policy.max_chunk_bytes = NonZeroU64::new(2);
                    policy.max_encoded_response_bytes = Some(512);
                    policy.max_decoded_response_bytes = Some(512);
                    policy.response_mode = ResponseMode::Buffered { max_bytes: 512 };
                    policy.response_decoding = response_decoding;
                    policy.status = status;
                    let output =
                        apply_response_body_policy(source, &mut headers, policy).expect("policy");
                    let retained = block_on(
                        response_with_body(Body::from_stream(output)).into_bytes_bounded(512),
                    )
                    .expect_err("original source failure");
                    assert_eq!(retained.provider_failure_detail(), Some(&detail));
                    assert_eq!(retained.to_string(), expected_display);
                    assert_eq!(retained.message(), expected_message);
                    assert_eq!(retained.kind(), expected_kind);
                    assert_eq!(retained.status(), expected_status);
                    if let EdgeError::BadGateway { reason, .. } = &retained {
                        assert_eq!(*reason, BadGatewayReason::Transport);
                    }
                    if let EdgeError::GatewayTimeout { cause, .. } = &retained {
                        assert_eq!(*cause, BudgetSource::RequestDeadline);
                    }
                    if let Some(inner) = retained.inner() {
                        assert_eq!(
                            inner
                                .downcast_ref::<IoError>()
                                .expect("concrete source")
                                .raw_os_error(),
                            Some(42_i32)
                        );
                    }
                }
            }
        }
    }
}

#[test]
fn provider_diagnostics_survive_bounded_collection_deadline() {
    use crate::error_detail::{FastlyFailureCause, ProviderFailureDetail, ProviderFailurePhase};

    let detail = ProviderFailureDetail::Fastly {
        phase: ProviderFailurePhase::ResponseBodyRead,
        cause: FastlyFailureCause::ConnectionTerminated,
    };
    for via_response in [false, true] {
        for (item, expected) in [
            (
                Some(Err(EdgeError::bad_gateway_with_detail(
                    "source failed",
                    BadGatewayReason::Transport,
                    detail,
                ))),
                Some(&detail),
            ),
            (Some(Err(EdgeError::bad_gateway("source failed"))), None),
            (Some(Ok(Bytes::from_static(b"ready"))), None),
            (None, None),
        ] {
            let start = MonotonicInstant::now();
            let end = start.checked_add(Duration::from_secs(1)).expect("deadline");
            let deadline = Deadline::at_instant(end);
            let samples = AtomicUsize::new(0);
            let live_samples = if via_response { 2_usize } else { 1_usize };
            let clock = MonotonicClock::new(move || {
                if samples.fetch_add(1, Ordering::SeqCst) < live_samples {
                    start
                } else {
                    end
                }
            });
            let source = stream::iter(item).boxed_local();
            let result = if via_response {
                block_on(
                    OutboundResponse::new(
                        Method::GET,
                        StatusCode::OK,
                        HeaderMap::new(),
                        Body::from_stream(source),
                        clock,
                        super::ContentTypeProvenance::Unknown,
                    )
                    .into_bytes_bounded_until(32, deadline),
                )
            } else {
                block_on(super::collect_response_stream_until_with_clock(
                    source, 32, deadline, &clock,
                ))
            };
            let error = result.expect_err("post-ready deadline wins");
            assert_eq!(error.provider_failure_detail(), expected);
            assert_eq!(error.message(), "response body deadline expired");
            assert!(matches!(
                error,
                EdgeError::GatewayTimeout {
                    cause: BudgetSource::Unspecified,
                    ..
                }
            ));
        }
    }
}

#[test]
fn body_policy_passthrough_preserves_headers_and_bypasses_decoded_limit_only() {
    for coding in ["unknown", "gzip, br", "gzip;"] {
        let mut headers = HeaderMap::new();
        headers.insert("content-encoding", HeaderValue::from_static(coding));
        headers.insert("content-length", HeaderValue::from_static("4"));
        let source = stream::iter([Ok(Bytes::from_static(b"abcd"))]).boxed_local();
        let mut policy = body_policy();
        policy.max_decoded_response_bytes = Some(1);
        let output =
            apply_response_body_policy(source, &mut headers, policy).expect("passthrough policy");
        assert_eq!(
            headers.get("content-encoding"),
            Some(&HeaderValue::from_static(coding))
        );
        assert!(headers.contains_key("content-length"));
        assert_eq!(
            block_on(collect_response_stream(output, 4)).expect("passthrough"),
            Bytes::from_static(b"abcd")
        );
    }
    let mut headers = HeaderMap::new();
    let mut policy = body_policy();
    policy.max_decoded_response_bytes = Some(1);
    policy.max_chunk_bytes = NonZeroU64::new(1);
    let source = stream::iter([Ok(Bytes::from_static(b"ab"))]).boxed_local();
    let mut output = apply_response_body_policy(source, &mut headers, policy).expect("identity");
    assert!(matches!(
        block_on(output.next()),
        Some(Err(EdgeError::ResponseTooLarge {
            reason: ResponseLimitReason::DecodedBody,
            ..
        }))
    ));
}

#[test]
fn body_policy_encoded_limit_precedes_decoder_and_preserves_source_errors() {
    let mut headers = HeaderMap::new();
    headers.insert("content-encoding", HeaderValue::from_static("gzip"));
    let mut policy = body_policy();
    policy.max_encoded_response_bytes = Some(1);
    let source = stream::iter([Ok(Bytes::from_static(b"invalid gzip"))]).boxed_local();
    let output = apply_response_body_policy(source, &mut headers, policy).expect("policy");
    assert!(matches!(
        block_on(collect_response_stream(output, 32)),
        Err(EdgeError::ResponseTooLarge {
            reason: ResponseLimitReason::EncodedBody,
            ..
        })
    ));

    for coding in ["identity", "gzip", "deflate", "br", "unknown"] {
        let mut headers = HeaderMap::new();
        headers.insert("content-encoding", HeaderValue::from_static(coding));
        let source = stream::iter([Err(EdgeError::gateway_timeout_caused(
            "source timeout",
            BudgetSource::RequestDeadline,
        ))])
        .boxed_local();
        let output =
            apply_response_body_policy(source, &mut headers, body_policy()).expect("policy");
        assert!(matches!(
            block_on(collect_response_stream(output, 32)),
            Err(EdgeError::GatewayTimeout {
                cause: BudgetSource::RequestDeadline,
                ..
            })
        ));
    }
}

#[test]
fn batch_ledger_preserves_an_on_time_terminal_outcome() {
    let mut fixture = BatchFixture::new();
    fixture.cutoff = Deadline::at_instant(fixture.started_at + Duration::from_millis(10));
    fixture
        .time
        .advance_to(fixture.started_at + Duration::from_millis(7));
    let event = fixture.observation(3);
    let mut batch =
        OutboundBatch::from_driver(fixture.context(4), stream::iter([event]), no_owner());
    fixture
        .time
        .advance_to(fixture.started_at + Duration::from_millis(20));
    let OutboundBatchNext::Item(item) = block_on(batch.next()).expect("on-time outcome") else {
        panic!("expected on-time terminal outcome");
    };
    assert_eq!(item.index(), 3);
    assert_eq!(item.result().elapsed, Duration::from_millis(7));
    let _response = item
        .into_parts()
        .1
        .outcome
        .expect("successful on-time outcome");
}

#[test]
fn batch_ledger_rejects_an_outcome_observed_at_observation_cutoff() {
    let mut fixture = BatchFixture::new();
    fixture.cutoff = Deadline::at_instant(fixture.started_at + Duration::from_millis(10));
    fixture.time.advance_to(fixture.cutoff.instant());
    let batch = OutboundBatch::from_driver(
        fixture.context(1),
        stream::iter([
            OutboundBatchDriverEvent::Item {
                index: 0,
                completed_at: fixture.time.now(),
                outcome: Err(EdgeError::bad_gateway("late")),
            },
            OutboundBatchDriverEvent::Cutoff,
        ]),
        no_owner(),
    );
    let result = block_on(batch.collect()).expect("cutoff");
    assert_eq!(result.termination(), OutboundBatchTermination::Cutoff);
    assert!(matches!(
        result.slots(),
        [OutboundCollectedSlot::Unresolved]
    ));
}

#[test]
fn batch_ledger_preserves_an_early_observation_cutoff_attributed_timeout() {
    let mut fixture = BatchFixture::new();
    fixture.cutoff = Deadline::at_instant(fixture.started_at + Duration::from_millis(10));
    fixture
        .time
        .advance_to(fixture.started_at + Duration::from_millis(1));
    let event = OutboundBatchDriverEvent::Item {
        index: 0,
        completed_at: fixture.time.now(),
        outcome: Err(EdgeError::gateway_timeout_caused(
            "batch cutoff",
            BudgetSource::BatchCutoff,
        )),
    };
    let mut batch =
        OutboundBatch::from_driver(fixture.context(1), stream::iter([event]), no_owner());
    let OutboundBatchNext::Item(item) = block_on(batch.next()).expect("early phase timeout") else {
        panic!("expected early phase timeout");
    };
    assert_eq!(item.result().elapsed, Duration::from_millis(1));
    assert!(matches!(
        item.result().outcome,
        Err(EdgeError::GatewayTimeout {
            cause: BudgetSource::BatchCutoff,
            ..
        })
    ));
}

#[test]
fn batch_ledger_maps_a_backward_terminal_sample_to_internal() {
    let fixture = BatchFixture::new();
    let context = fixture.context(5);
    let completed_at = fixture
        .started_at
        .checked_sub(Duration::from_millis(1))
        .expect("earlier instant");
    fixture.time.advance_to(completed_at);
    let mut batch =
        OutboundBatch::from_driver(context, stream::iter([fixture.observation(4)]), no_owner());
    let OutboundBatchNext::Item(item) =
        block_on(batch.next()).expect("clock fault remains terminal")
    else {
        panic!("expected clock fault outcome");
    };
    assert_eq!(item.index(), 4);
    assert_eq!(item.result().elapsed, Duration::ZERO);
    let error = item
        .into_parts()
        .1
        .outcome
        .expect_err("clock fault outcome");
    assert_eq!(
        error.to_string(),
        "internal error: monotonic clock moved backwards during outbound dispatch"
    );
}

#[expect(
    clippy::wildcard_enum_match_arm,
    reason = "EdgeError is non-exhaustive and this assertion helper must reject future variants"
)]
fn bad_request_message(error: EdgeError) -> String {
    match error {
        EdgeError::BadRequest { message, .. } => message,
        other => panic!("expected bad request, got {other:?}"),
    }
}

fn response_with_body(body: Body) -> OutboundResponse {
    OutboundResponse::new(
        Method::GET,
        StatusCode::OK,
        HeaderMap::new(),
        body,
        MonotonicClock::default(),
        super::ContentTypeProvenance::Unknown,
    )
}

#[test]
fn buffered_collection_limit_is_independent() {
    let source = Body::from_stream(stream::iter([
        Ok(Bytes::from_static(b"ab")),
        Ok(Bytes::from_static(b"cd")),
    ]))
    .into_stream()
    .expect("stream");
    let bytes = block_on(collect_response_stream(source, 4)).expect("exact cap");
    assert_eq!(bytes, Bytes::from_static(b"abcd"));

    let error = block_on(response_with_body(Body::from("raw-passthrough")).into_bytes_bounded(3))
        .expect_err("final cap");
    assert!(matches!(
        error,
        EdgeError::ResponseTooLarge {
            reason: ResponseLimitReason::BufferedBody,
            ..
        }
    ));
}

#[test]
fn dispatch_budget_rejects_expired_or_zero() {
    let now = MonotonicInstant::now();
    let expired = OutboundRequest::get("https://example.com")
        .expect("request")
        .deadline(Deadline::at_instant(now));
    let error = dispatch_budget(&expired, now, None).expect_err("expired");
    assert!(matches!(
        error,
        EdgeError::GatewayTimeout {
            cause: BudgetSource::RequestDeadline,
            ..
        }
    ));

    let tied = OutboundRequest::get("https://example.com")
        .expect("request")
        .timeout(Duration::ZERO)
        .deadline(Deadline::at_instant(now));
    let error = dispatch_budget(&tied, now, None).expect_err("zero tie");
    assert!(matches!(
        error,
        EdgeError::GatewayTimeout {
            cause: BudgetSource::PerCallTimeout,
            ..
        }
    ));
}

#[test]
fn dispatch_budget_selects_and_attributes_minimum() {
    let now = MonotonicInstant::now();

    let default = OutboundRequest::get("https://example.com").expect("request");
    let budget = dispatch_budget(&default, now, None).expect("default budget");
    assert_eq!(budget.cause, BudgetSource::Default);
    assert_eq!(budget.duration, DEFAULT_NO_DEADLINE_BUDGET);
    assert_eq!(
        budget.deadline.instant(),
        now.checked_add(DEFAULT_NO_DEADLINE_BUDGET)
            .expect("no overflow")
    );

    let timeout = OutboundRequest::get("https://example.com")
        .expect("request")
        .timeout(Duration::from_secs(3));
    let budget = dispatch_budget(&timeout, now, None).expect("timeout budget");
    assert_eq!(budget.cause, BudgetSource::PerCallTimeout);
    assert_eq!(budget.duration, Duration::from_secs(3));

    let deadline = Deadline::at_instant(
        now.checked_add(Duration::from_secs(4))
            .expect("no overflow"),
    );
    let request = OutboundRequest::get("https://example.com")
        .expect("request")
        .deadline(deadline);
    let budget = dispatch_budget(&request, now, None).expect("deadline budget");
    assert_eq!(budget.cause, BudgetSource::RequestDeadline);
    assert_eq!(budget.duration, Duration::from_secs(4));
    assert_eq!(budget.deadline.instant(), deadline.instant());

    let timeout_wins = OutboundRequest::get("https://example.com")
        .expect("request")
        .timeout(Duration::from_secs(2))
        .deadline(deadline);
    let budget = dispatch_budget(&timeout_wins, now, None).expect("timeout wins");
    assert_eq!(budget.cause, BudgetSource::PerCallTimeout);
    assert_eq!(budget.duration, Duration::from_secs(2));

    let deadline_wins = OutboundRequest::get("https://example.com")
        .expect("request")
        .timeout(Duration::from_secs(5))
        .deadline(deadline);
    let budget = dispatch_budget(&deadline_wins, now, None).expect("deadline wins");
    assert_eq!(budget.cause, BudgetSource::RequestDeadline);
    assert_eq!(budget.duration, Duration::from_secs(4));

    let tied_deadline = Deadline::at_instant(
        now.checked_add(Duration::from_secs(5))
            .expect("no overflow"),
    );
    let tied = OutboundRequest::get("https://example.com")
        .expect("request")
        .timeout(Duration::from_secs(5))
        .deadline(tied_deadline);
    let budget = dispatch_budget(&tied, now, None).expect("equal budget");
    assert_eq!(budget.cause, BudgetSource::PerCallTimeout);

    let huge_timeout = OutboundRequest::get("https://example.com")
        .expect("request")
        .timeout(Duration::MAX);
    let budget = dispatch_budget(&huge_timeout, now, None).expect("clamped timeout");
    assert_eq!(budget.duration, DEADLINE_FAR_FUTURE);

    let far_deadline = Deadline::at_instant(
        now.checked_add(Duration::from_hours(8_760))
            .expect("no overflow"),
    );
    let far = OutboundRequest::get("https://example.com")
        .expect("request")
        .deadline(far_deadline);
    let budget = dispatch_budget(&far, now, None).expect("clamped deadline");
    assert_eq!(budget.duration, DEADLINE_FAR_FUTURE);
    assert_eq!(budget.cause, BudgetSource::RequestDeadline);

    let observation_cutoff = Deadline::at_instant(
        now.checked_add(Duration::from_secs(1))
            .expect("cutoff instant"),
    );
    let budget = dispatch_budget(&default, now, Some(observation_cutoff)).expect("batch cutoff");
    assert_eq!(budget.cause, BudgetSource::BatchCutoff);
    assert_eq!(budget.deadline.instant(), observation_cutoff.instant());

    let observation_cutoff_tie = OutboundRequest::get("https://example.com")
        .expect("request")
        .timeout(Duration::from_secs(1))
        .deadline(observation_cutoff);
    let budget = dispatch_budget(&observation_cutoff_tie, now, Some(observation_cutoff))
        .expect("cutoff tie");
    assert_eq!(budget.cause, BudgetSource::BatchCutoff);

    let error = dispatch_budget(&default, now, Some(Deadline::at_instant(now)))
        .expect_err("cutoff equality is expired");
    assert!(matches!(
        error,
        EdgeError::GatewayTimeout {
            cause: BudgetSource::BatchCutoff,
            ..
        }
    ));
}

#[test]
fn encoded_limit_stops_before_decoder() {
    let polls = Rc::new(Cell::new(0_usize));
    let observed = Rc::clone(&polls);
    let source = Body::from_stream(
        stream::iter([
            Ok(Bytes::from_static(b"four")),
            Ok(Bytes::from_static(b"later")),
        ])
        .inspect(move |_| observed.set(observed.get().saturating_add(1))),
    )
    .into_stream()
    .expect("stream");
    let mut limited = limit_encoded_stream(source, Some(3));
    let error = block_on(limited.next())
        .expect("limit result")
        .expect_err("over limit");
    assert!(matches!(
        error,
        EdgeError::ResponseTooLarge {
            reason: ResponseLimitReason::EncodedBody,
            ..
        }
    ));
    assert_eq!(polls.get(), 1);
    assert!(block_on(limited.next()).is_none());
}

#[test]
fn terminal_stream_error_releases_source_before_yield() {
    struct DropSignal(Rc<Cell<usize>>);

    #[expect(
        clippy::missing_trait_methods,
        reason = "`Drop::pin_drop` is unstable (`pin_ergonomics`) and conflicts with `drop`"
    )]
    impl Drop for DropSignal {
        fn drop(&mut self) {
            self.0.set(self.0.get().saturating_add(1));
        }
    }

    let limit_drops = Rc::new(Cell::new(0_usize));
    let limit_signal = DropSignal(Rc::clone(&limit_drops));
    let limit_source = stream::poll_fn(move |_context| {
        let _keep_signal_alive = &limit_signal;
        Poll::Ready(Some(Ok(Bytes::from_static(b"over"))))
    })
    .boxed_local();
    let mut limited = limit_encoded_stream(limit_source, Some(3));
    block_on(limited.next())
        .expect("limit result")
        .expect_err("over limit");
    assert_eq!(limit_drops.get(), 1_usize);

    let source_drops = Rc::new(Cell::new(0_usize));
    let source_signal = DropSignal(Rc::clone(&source_drops));
    let source_error = stream::poll_fn(move |_context| {
        let _keep_signal_alive = &source_signal;
        Poll::Ready(Some(Err(EdgeError::bad_gateway("source failed"))))
    })
    .boxed_local();
    let mut limited = limit_decoded_stream(source_error, Some(3));
    block_on(limited.next())
        .expect("source result")
        .expect_err("source error");
    assert_eq!(source_drops.get(), 1_usize);
}

#[test]
fn http_client_delegates_send_and_empty_batch_collection() {
    let client = HttpClient::with_client(MockClient {
        batch_calls: AtomicUsize::new(0),
        send_calls: AtomicUsize::new(0),
        time: TestTime::new(MonotonicInstant::now()),
    });

    let response = block_on(
        client.send(
            OutboundRequest::post("https://example.com")
                .expect("request")
                .body("request"),
        ),
    )
    .expect("response");
    assert_eq!(response.status(), StatusCode::CREATED);
    assert_eq!(response.body().as_bytes(), Some(b"single".as_slice()));

    let empty =
        block_on(client.send_all_until(Vec::new(), Deadline::after(Duration::from_secs(1))))
            .expect("valid empty batch");
    assert!(empty.slots().is_empty());
    assert_eq!(empty.termination(), OutboundBatchTermination::Completed);
}

#[test]
fn batch_next_yields_completion_order_and_collection_restores_input_order() {
    let client = HttpClient::with_client(MockClient {
        batch_calls: AtomicUsize::new(0),
        send_calls: AtomicUsize::new(0),
        time: TestTime::new(MonotonicInstant::now()),
    });
    let requests = vec![
        OutboundRequest::get("https://example.com/one").expect("one"),
        OutboundRequest::new(Method::DELETE, Uri::from_static("https://example.com/two"))
            .expect("two"),
        OutboundRequest::post("https://example.com/three").expect("three"),
    ];

    let observation_cutoff = Deadline::after(Duration::from_secs(1));
    let mut batch = client.start_batch_until(requests, observation_cutoff);
    let OutboundBatchNext::Item(first) = block_on(batch.next()).expect("valid batch driver") else {
        panic!("expected first completion");
    };
    assert_eq!(first.index(), 2);
    assert_eq!(first.result().elapsed, Duration::from_millis(1));
    let OutboundBatchNext::Item(second) = block_on(batch.next()).expect("valid batch driver")
    else {
        panic!("expected second completion");
    };
    assert_eq!(second.index(), 1);
    assert!(matches!(
        second.result().outcome,
        Err(EdgeError::BadGateway { .. })
    ));
    let OutboundBatchNext::Item(third) = block_on(batch.next()).expect("valid batch driver") else {
        panic!("expected third completion");
    };
    assert_eq!(third.index(), 0);
    assert!(matches!(
        block_on(batch.next()).expect("valid batch driver"),
        OutboundBatchNext::Finished(OutboundBatchTermination::Completed)
    ));

    let requests = vec![
        OutboundRequest::get("https://example.com/one").expect("one"),
        OutboundRequest::new(Method::DELETE, Uri::from_static("https://example.com/two"))
            .expect("two"),
        OutboundRequest::post("https://example.com/three").expect("three"),
    ];
    let results =
        block_on(client.send_all_until(requests, observation_cutoff)).expect("valid batch driver");
    assert_eq!(results.termination(), OutboundBatchTermination::Completed);
    assert_eq!(results.slots().len(), 3);
    let [
        OutboundCollectedSlot::Resolved(first),
        OutboundCollectedSlot::Resolved(second),
        OutboundCollectedSlot::Resolved(third),
    ] = results.slots()
    else {
        panic!("all input slots must resolve");
    };
    assert_eq!(first.elapsed, Duration::from_millis(3));
    assert_eq!(second.elapsed, Duration::from_millis(2));
    assert_eq!(third.elapsed, Duration::from_millis(1));
    first.outcome.as_ref().expect("first slot success");
    assert!(matches!(second.outcome, Err(EdgeError::BadGateway { .. })));
    third.outcome.as_ref().expect("third slot success");
}

#[test]
fn batch_next_is_cancellation_safe_and_cancel_reports_unresolved_indices() {
    fn poll_next_once(batch: &mut OutboundBatch) -> Poll<Result<OutboundBatchNext, EdgeError>> {
        let next = batch.next();
        pin_mut!(next);
        let waker = noop_waker_ref();
        let mut context = Context::from_waker(waker);
        next.as_mut().poll(&mut context)
    }

    let fixture = BatchFixture::new();
    let context = fixture.context(3);
    let time = fixture.time.clone();
    let poll_count = Rc::new(Cell::new(0_u8));
    let observed_poll_count = Rc::clone(&poll_count);
    let source = stream::poll_fn(move |_context| {
        let current = observed_poll_count.get();
        observed_poll_count.set(current.saturating_add(1));
        if current == 1 {
            Poll::Ready(Some(OutboundBatchDriverEvent::Item {
                index: 1,
                completed_at: time.now(),
                outcome: Err(EdgeError::bad_gateway("second")),
            }))
        } else {
            Poll::Pending
        }
    });
    let mut batch = OutboundBatch::from_driver(context, source, no_owner());

    assert!(poll_next_once(&mut batch).is_pending());
    fixture
        .time
        .advance_to(fixture.started_at + Duration::from_millis(2));

    let OutboundBatchNext::Item(item) =
        block_on(batch.next()).expect("item survives cancelled next future")
    else {
        panic!("expected terminal item");
    };
    assert_eq!(item.index(), 1);
    assert_eq!(item.result().elapsed, Duration::from_millis(2));
    assert_eq!(batch.cancel(), vec![0, 2]);
}

#[test]
fn batch_collection_reports_cutoff_and_preserves_unresolved_slots() {
    let fixture = BatchFixture::new();
    let event = fixture.observation(2);
    fixture.time.advance_to(fixture.cutoff.instant());
    let batch = OutboundBatch::from_driver(
        fixture.context(3),
        stream::iter([event, OutboundBatchDriverEvent::Cutoff]),
        no_owner(),
    );

    let results = block_on(batch.collect()).expect("valid cutoff");
    assert_eq!(results.termination(), OutboundBatchTermination::Cutoff);
    assert!(matches!(
        results.slots().first(),
        Some(OutboundCollectedSlot::Unresolved)
    ));
    assert!(matches!(
        results.slots()[1],
        OutboundCollectedSlot::Unresolved
    ));
    assert!(matches!(
        results.slots()[2],
        OutboundCollectedSlot::Resolved(_)
    ));
}

#[test]
fn batch_cutoff_constructor_reports_every_slot_unresolved() {
    let fixture = BatchFixture::new();
    fixture.time.advance_to(fixture.cutoff.instant());
    let results =
        block_on(OutboundBatch::cutoff(fixture.context(3)).collect()).expect("valid cutoff");

    assert_eq!(results.termination(), OutboundBatchTermination::Cutoff);
    assert_eq!(results.slots().len(), 3);
    assert!(
        results
            .slots()
            .iter()
            .all(|slot| matches!(slot, OutboundCollectedSlot::Unresolved))
    );
}

#[test]
fn batch_collection_rejects_premature_driver_end() {
    let fixture = BatchFixture::new();
    let batch = OutboundBatch::from_driver(
        fixture.context(3),
        stream::iter([fixture.observation(2)]),
        no_owner(),
    );

    let failure = block_on(batch.collect()).expect_err("premature EOF must fail");
    assert_eq!(
        failure.error().to_string(),
        "internal error: outbound batch driver ended before every slot resolved"
    );
    assert!(matches!(
        failure.slots().first(),
        Some(OutboundCollectedSlot::Unresolved)
    ));
    assert!(matches!(
        failure.slots().get(1),
        Some(OutboundCollectedSlot::Unresolved)
    ));
    assert!(matches!(
        failure.slots().get(2),
        Some(OutboundCollectedSlot::Resolved(_))
    ));
}

#[test]
fn batch_collection_preserves_driver_failure_and_completed_slots() {
    use crate::error_detail::{FastlyFailureCause, ProviderFailureDetail, ProviderFailurePhase};

    let detail = ProviderFailureDetail::Fastly {
        phase: ProviderFailurePhase::ResponseHeaders,
        cause: FastlyFailureCause::InternalError {
            status: Some(-1_i32),
        },
    };
    let fixture = BatchFixture::new();
    let completed = OutboundBatchDriverEvent::Item {
        index: 1,
        completed_at: fixture.time.now(),
        outcome: Err(EdgeError::bad_gateway_with_detail(
            "slot failed",
            BadGatewayReason::Transport,
            detail,
        )),
    };
    let batch = OutboundBatch::from_driver(
        fixture.context(3),
        stream::iter([
            completed,
            OutboundBatchDriverEvent::Failed(EdgeError::internal_with_detail(
                anyhow::anyhow!("Fastly omitted a pending handle from selection"),
                detail,
            )),
        ]),
        no_owner(),
    );

    let failure = block_on(batch.collect()).expect_err("driver failure must surface");
    assert_eq!(
        failure.error().to_string(),
        "internal error: Fastly omitted a pending handle from selection"
    );
    assert!(matches!(
        failure.slots().first(),
        Some(OutboundCollectedSlot::Unresolved)
    ));
    assert!(matches!(
        failure.slots().get(1),
        Some(OutboundCollectedSlot::Resolved(_))
    ));
    assert!(matches!(
        failure.slots().get(2),
        Some(OutboundCollectedSlot::Unresolved)
    ));
    assert_eq!(failure.error().provider_failure_detail(), Some(&detail));
    let OutboundCollectedSlot::Resolved(completed) = &failure.slots()[1] else {
        panic!("completed slot");
    };
    let retained = completed
        .outcome
        .as_ref()
        .expect_err("original slot failure");
    assert_eq!(retained.provider_failure_detail(), Some(&detail));
    assert_eq!(retained.message(), "slot failed");
}

#[test]
fn batch_collection_validates_events_after_the_final_slot() {
    let fixture = BatchFixture::new();
    for (event, diagnostic) in [
        (
            OutboundBatchDriverEvent::Failed(EdgeError::internal(anyhow::anyhow!(
                "driver finalization failed"
            ))),
            "internal error: driver finalization failed",
        ),
        (
            fixture.observation(0),
            "internal error: outbound batch driver emitted a duplicate slot index",
        ),
        (
            fixture.observation(1),
            "internal error: outbound batch driver emitted an out-of-range slot index",
        ),
        (
            OutboundBatchDriverEvent::Cutoff,
            "internal error: outbound batch driver emitted cutoff after every slot resolved",
        ),
    ] {
        let batch = OutboundBatch::from_driver(
            fixture.context(1),
            stream::iter([fixture.observation(0), event]),
            no_owner(),
        );

        let failure = block_on(batch.collect()).expect_err("trailing event must be checked");
        assert_eq!(failure.error().to_string(), diagnostic);
        assert_eq!(failure.slots().len(), 1);
        assert!(matches!(
            failure.slots().first(),
            Some(OutboundCollectedSlot::Resolved(_))
        ));
    }
}

#[test]
fn batch_zero_slot_driver_is_validated_before_completion() {
    let fixture = BatchFixture::new();
    let completed = block_on(
        OutboundBatch::from_driver(fixture.context(0), stream::empty(), no_owner()).collect(),
    )
    .expect("empty driver completes");
    assert_eq!(completed.termination(), OutboundBatchTermination::Completed);
    let completed =
        block_on(OutboundBatch::cutoff(fixture.context(0)).collect()).expect("no work to cut off");
    assert_eq!(completed.termination(), OutboundBatchTermination::Completed);

    for (event, diagnostic) in [
        (
            OutboundBatchDriverEvent::Failed(EdgeError::internal(anyhow::anyhow!(
                "empty driver failed"
            ))),
            "internal error: empty driver failed",
        ),
        (
            fixture.observation(0),
            "internal error: outbound batch driver emitted an out-of-range slot index",
        ),
        (
            OutboundBatchDriverEvent::Cutoff,
            "internal error: outbound batch driver emitted cutoff after every slot resolved",
        ),
    ] {
        let failure = block_on(
            OutboundBatch::from_driver(fixture.context(0), stream::iter([event]), no_owner())
                .collect(),
        )
        .expect_err("zero slots do not hide malformed driver events");
        assert_eq!(failure.error().to_string(), diagnostic);
        assert!(failure.slots().is_empty());
    }
}

#[test]
fn batch_finalization_remains_pending_and_cancellation_safe_until_eof() {
    let fixture = BatchFixture::new();
    let context = fixture.context(1);
    let mut event = Some(fixture.observation(0));
    let finish = Rc::new(Cell::new(false));
    let driver_finish = Rc::clone(&finish);
    let polls = Rc::new(Cell::new(0_usize));
    let source_polls = Rc::clone(&polls);
    let source = stream::poll_fn(move |_context| {
        let index = source_polls.get();
        source_polls.set(index + 1);
        if let Some(event) = event.take() {
            Poll::Ready(Some(event))
        } else if driver_finish.get() {
            Poll::Ready(None)
        } else {
            Poll::Pending
        }
    });
    let mut batch = OutboundBatch::from_driver(context, source, no_owner());
    assert!(matches!(
        block_on(batch.next()).expect("terminal item"),
        OutboundBatchNext::Item(_)
    ));
    assert!(batch.next().now_or_never().is_none());
    finish.set(true);
    assert!(matches!(
        block_on(batch.next()).expect("validated EOF"),
        OutboundBatchNext::Finished(OutboundBatchTermination::Completed)
    ));
    assert_eq!(polls.get(), 4);
    assert!(matches!(
        block_on(batch.next()).expect("cached termination"),
        OutboundBatchNext::Finished(OutboundBatchTermination::Completed)
    ));
    assert_eq!(polls.get(), 4);
    assert!(batch.cancel().is_empty());
}

#[test]
fn batch_cancel_releases_pending_finalization_after_every_slot_resolves() {
    struct DropProbe(Rc<Cell<usize>>);
    #[expect(
        clippy::missing_trait_methods,
        reason = "`Drop::pin_drop` is unstable (`pin_ergonomics`) and conflicts with `drop`"
    )]
    impl Drop for DropProbe {
        fn drop(&mut self) {
            self.0.set(self.0.get() + 1);
        }
    }
    let fixture = BatchFixture::new();
    let context = fixture.context(1);
    let mut event = Some(fixture.observation(0));
    let drops = Rc::new(Cell::new(0));
    let drop_probe = DropProbe(Rc::clone(&drops));
    let source = stream::poll_fn(move |_context| {
        let _keep_driver_alive = &drop_probe;
        if let Some(event) = event.take() {
            Poll::Ready(Some(event))
        } else {
            Poll::Pending
        }
    });
    let mut batch = OutboundBatch::from_driver(context, source, no_owner());
    assert!(matches!(
        block_on(batch.next()).expect("slot"),
        OutboundBatchNext::Item(_)
    ));
    assert!(batch.next().now_or_never().is_none());
    assert_eq!(drops.get(), 0);
    assert!(batch.cancel().is_empty());
    assert_eq!(drops.get(), 1);
}

#[test]
fn batch_collection_rejects_duplicate_driver_indices() {
    let fixture = BatchFixture::new();
    let batch = OutboundBatch::from_driver(
        fixture.context(2),
        stream::iter([fixture.observation(0), fixture.observation(0)]),
        no_owner(),
    );

    let failure = block_on(batch.collect()).expect_err("duplicate index must fail");
    assert_eq!(
        failure.error().to_string(),
        "internal error: outbound batch driver emitted a duplicate slot index"
    );
    assert!(matches!(
        failure.slots().first(),
        Some(OutboundCollectedSlot::Resolved(_))
    ));
    assert!(matches!(
        failure.slots().get(1),
        Some(OutboundCollectedSlot::Unresolved)
    ));
}

#[test]
fn batch_collection_rejects_out_of_range_driver_indices() {
    let fixture = BatchFixture::new();
    let batch = OutboundBatch::from_driver(
        fixture.context(2),
        stream::iter([fixture.observation(2)]),
        no_owner(),
    );

    let failure = block_on(batch.collect()).expect_err("out-of-range index must fail");
    assert_eq!(
        failure.error().to_string(),
        "internal error: outbound batch driver emitted an out-of-range slot index"
    );
    assert!(
        failure
            .slots()
            .iter()
            .all(|slot| matches!(slot, OutboundCollectedSlot::Unresolved))
    );
}

#[test]
fn batch_poisoned_repoll_reports_the_guard_diagnostic() {
    let fixture = BatchFixture::new();
    let mut batch = OutboundBatch::from_driver(
        fixture.context(1),
        stream::iter([fixture.observation(1)]),
        no_owner(),
    );

    let first = block_on(batch.next()).expect_err("out-of-range index must poison driver");
    assert_eq!(
        first.to_string(),
        "internal error: outbound batch driver emitted an out-of-range slot index"
    );
    let second = block_on(batch.next()).expect_err("poisoned driver must reject re-poll");
    assert_eq!(
        second.to_string(),
        "internal error: outbound batch driver was polled after an invariant failure"
    );
}

#[test]
fn outbound_response_parts_preserve_method_headers_and_body() {
    let mut headers = HeaderMap::new();
    headers.append("set-cookie", HeaderValue::from_static("a=1"));
    headers.append("set-cookie", HeaderValue::from_static("b=2"));
    let mut response = OutboundResponse::new(
        Method::HEAD,
        StatusCode::CREATED,
        headers,
        Body::from("payload"),
        MonotonicClock::default(),
        super::ContentTypeProvenance::Unknown,
    );
    assert_eq!(response.status(), StatusCode::CREATED);
    assert!(response.is_success());
    assert_eq!(response.body().as_bytes(), Some(b"payload".as_slice()));
    assert_eq!(response.headers().get_all("set-cookie").iter().count(), 2);
    response
        .headers_mut()
        .append("x-adapter", HeaderValue::from_static("yes"));

    let (method, status, headers, body, provenance) = response.into_parts();
    assert_eq!(provenance, super::ContentTypeProvenance::Unknown);
    assert_eq!(method, Method::HEAD);
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(
        headers.get("x-adapter"),
        Some(&HeaderValue::from_static("yes"))
    );
    assert_eq!(body.as_bytes(), Some(b"payload".as_slice()));

    let body = OutboundResponse::new(
        Method::GET,
        StatusCode::OK,
        HeaderMap::new(),
        Body::from("owned"),
        MonotonicClock::default(),
        super::ContentTypeProvenance::Unknown,
    )
    .into_body();
    assert_eq!(body.as_bytes(), Some(b"owned".as_slice()));
}

#[test]
fn payload_content_length_explicit_identity_rejects_before_body_poll() {
    let mut headers = HeaderMap::new();
    headers.append("content-encoding", HeaderValue::from_static("identity"));
    headers.append("content-length", HeaderValue::from_static("11"));
    let error = enforce_payload_content_length(
        &headers,
        &ContentEncoding::Identity,
        Some(20),
        Some(10),
        Some(20),
    )
    .expect_err("decoded cap");
    assert!(matches!(
        error,
        EdgeError::ResponseTooLarge {
            reason: ResponseLimitReason::DecodedBody,
            ..
        }
    ));
}

#[test]
fn payload_content_length_passthrough_uses_buffered_not_decoded_cap() {
    let mut headers = HeaderMap::new();
    headers.append("content-length", HeaderValue::from_static("15"));
    enforce_payload_content_length(
        &headers,
        &ContentEncoding::Passthrough(PassthroughReason::Unsupported(String::from("zstd"))),
        Some(20),
        Some(10),
        Some(20),
    )
    .expect("decoded cap bypassed");

    let error = enforce_payload_content_length(
        &headers,
        &ContentEncoding::Passthrough(PassthroughReason::Unsupported(String::from("zstd"))),
        Some(14),
        Some(100),
        Some(20),
    )
    .expect_err("buffered cap");
    assert!(matches!(
        error,
        EdgeError::ResponseTooLarge {
            reason: ResponseLimitReason::BufferedBody,
            ..
        }
    ));
}

#[test]
fn payload_content_length_rejects_before_body_poll() {
    let mut headers = HeaderMap::new();
    headers.append("content-length", HeaderValue::from_static("21"));
    let error = enforce_payload_content_length(
        &headers,
        &ContentEncoding::Identity,
        Some(30),
        Some(30),
        Some(20),
    )
    .expect_err("encoded cap");
    assert!(matches!(
        error,
        EdgeError::ResponseTooLarge {
            reason: ResponseLimitReason::EncodedBody,
            ..
        }
    ));

    for value in ["", "+1", "-1", "bad", "1, 1"] {
        let mut headers = HeaderMap::new();
        headers.append(
            "content-length",
            HeaderValue::from_bytes(value.as_bytes()).expect("header"),
        );
        enforce_payload_content_length(&headers, &ContentEncoding::Identity, None, None, None)
            .expect_err("malformed content-length");
    }

    let mut headers = HeaderMap::new();
    headers.append(
        "content-length",
        HeaderValue::from_bytes(b"\xc2\xa05").expect("opaque whitespace"),
    );
    enforce_payload_content_length(&headers, &ContentEncoding::Identity, None, None, None)
        .expect_err("non-HTTP whitespace must be rejected");
}

#[test]
fn payload_content_length_skips_output_caps_for_compressed_body() {
    let mut headers = HeaderMap::new();
    headers.append("content-length", HeaderValue::from_static("100"));
    for encoding in [ContentEncoding::Brotli, ContentEncoding::Gzip] {
        enforce_payload_content_length(&headers, &encoding, Some(1), Some(1), Some(100))
            .expect("wire length is not output length");
    }
}

#[test]
fn rechunk_stream_is_lazy_and_ordered() {
    let source_polls = Rc::new(Cell::new(0_u8));
    let observed_polls = Rc::clone(&source_polls);
    let source = Body::from_stream(stream::poll_fn(move |_context| {
        let poll = source_polls.get();
        source_polls.set(poll.saturating_add(1));
        Poll::Ready(match poll {
            0 => Some(Ok(Bytes::from_static(b"abcde"))),
            1 => Some(Ok(Bytes::new())),
            2 => Some(Err(EdgeError::bad_gateway("source"))),
            _ => None,
        })
    }))
    .into_stream()
    .expect("stream");
    let mut chunks = rechunk_stream(source, NonZeroU64::new(2));
    assert_eq!(observed_polls.get(), 0);

    assert_eq!(
        block_on(chunks.next()).expect("first").expect("bytes"),
        "ab"
    );
    assert_eq!(observed_polls.get(), 1);
    assert_eq!(
        block_on(chunks.next()).expect("second").expect("bytes"),
        "cd"
    );
    assert_eq!(observed_polls.get(), 1);
    assert_eq!(block_on(chunks.next()).expect("third").expect("bytes"), "e");
    assert_eq!(observed_polls.get(), 1);
    assert_eq!(
        block_on(chunks.next()).expect("empty").expect("bytes"),
        Bytes::new()
    );
    assert_eq!(observed_polls.get(), 2);
    assert!(matches!(
        block_on(chunks.next()).expect("error"),
        Err(EdgeError::BadGateway { .. })
    ));
    assert_eq!(observed_polls.get(), 3);
}

#[test]
fn response_header_limiter_accumulates_field_sections() {
    let mut limiter = ResponseHeaderLimiter::new(Some(12), Some(3));
    let mut first = HeaderMap::new();
    first.append("a", HeaderValue::from_static("123"));
    limiter.observe(&first).expect("first section");

    let mut second = HeaderMap::new();
    second.append("b", HeaderValue::from_static("456"));
    limiter.observe(&second).expect("second section");

    let mut third = HeaderMap::new();
    third.append("c", HeaderValue::from_static("789"));
    limiter.observe(&third).expect("third section");

    let mut fourth = HeaderMap::new();
    fourth.append("d", HeaderValue::from_static("0"));
    let error = limiter.observe(&fourth).expect_err("cumulative count");
    assert!(matches!(
        error,
        EdgeError::ResponseTooLarge {
            reason: ResponseLimitReason::HeaderCount,
            ..
        }
    ));

    let mut byte_limiter = ResponseHeaderLimiter::new(Some(11), Some(4));
    byte_limiter.observe(&first).expect("first byte section");
    byte_limiter.observe(&second).expect("second byte section");
    let error = byte_limiter
        .observe(&third)
        .expect_err("cumulative byte limit");
    assert!(matches!(
        error,
        EdgeError::ResponseTooLarge {
            reason: ResponseLimitReason::HeaderBytes,
            ..
        }
    ));
}

#[test]
fn proxy_header_is_included_in_exact_byte_and_count_limits() {
    let cases = [
        ("axum", 20_u64),
        ("cloudflare", 26_u64),
        ("fastly", 22_u64),
        ("spin", 20_u64),
    ];

    for (adapter, exact_bytes) in cases {
        let value = HeaderValue::from_str(adapter).expect("adapter marker");
        let mut exact_headers = HeaderMap::new();
        let mut exact = ResponseHeaderLimiter::new(Some(exact_bytes), Some(1));
        insert_proxy_header(
            &mut exact_headers,
            &mut exact,
            OutboundProxyMarker::On,
            value.clone(),
        )
        .expect("exact proxy header budget");
        assert_eq!(exact_headers.get(PROXY_HEADER), Some(&value));

        let mut byte_headers = HeaderMap::new();
        let mut byte_limited =
            ResponseHeaderLimiter::new(Some(exact_bytes.saturating_sub(1)), None);
        let byte_error = insert_proxy_header(
            &mut byte_headers,
            &mut byte_limited,
            OutboundProxyMarker::On,
            value.clone(),
        )
        .expect_err("proxy header exceeds byte cap");
        assert!(matches!(
            byte_error,
            EdgeError::ResponseTooLarge {
                reason: ResponseLimitReason::HeaderBytes,
                ..
            }
        ));

        let mut count_headers = HeaderMap::new();
        let mut count_limited = ResponseHeaderLimiter::new(None, Some(0));
        let count_error = insert_proxy_header(
            &mut count_headers,
            &mut count_limited,
            OutboundProxyMarker::On,
            value,
        )
        .expect_err("proxy header exceeds count cap");
        assert!(matches!(
            count_error,
            EdgeError::ResponseTooLarge {
                reason: ResponseLimitReason::HeaderCount,
                ..
            }
        ));
    }
}

#[test]
fn response_resource_header_limit_count_wins_tie() {
    let mut limiter = ResponseHeaderLimiter::new(Some(0), Some(0));
    let mut headers = HeaderMap::new();
    headers.append("x", HeaderValue::from_static("y"));
    let error = limiter.observe(&headers).expect_err("both limits");
    assert!(matches!(
        error,
        EdgeError::ResponseTooLarge {
            reason: ResponseLimitReason::HeaderCount,
            ..
        }
    ));
}

#[test]
fn decoded_limit_bypasses_raw_passthrough() {
    let source = Body::from_stream(stream::iter([
        Ok(Bytes::from_static(b"ab")),
        Ok(Bytes::from_static(b"cd")),
    ]))
    .into_stream()
    .expect("stream");
    let error = block_on(async {
        let mut limited = limit_decoded_stream(source, Some(3));
        assert_eq!(limited.next().await.expect("first").expect("bytes"), "ab");
        limited.next().await.expect("second").expect_err("over cap")
    });
    assert!(matches!(
        error,
        EdgeError::ResponseTooLarge {
            reason: ResponseLimitReason::DecodedBody,
            ..
        }
    ));

    let raw = Body::from_stream(stream::iter([Ok(Bytes::from_static(b"raw-body"))]))
        .into_stream()
        .expect("raw passthrough");
    let bytes = block_on(async { raw.collect::<Vec<_>>().await });
    assert_eq!(bytes[0].as_ref().expect("raw").as_ref(), b"raw-body");
}

#[test]
fn outbound_response_into_response_reapplies_normalization() {
    let mut headers = HeaderMap::new();
    headers.append("connection", HeaderValue::from_static("x-private"));
    headers.append("x-private", HeaderValue::from_static("secret"));
    headers.append("set-cookie", HeaderValue::from_static("a=1"));
    headers.append("set-cookie", HeaderValue::from_static("b=2"));
    let body = Body::stream(stream::iter([Bytes::from_static(b"lazy")]));
    let response = OutboundResponse::new(
        Method::GET,
        StatusCode::OK,
        headers,
        body,
        MonotonicClock::default(),
        super::ContentTypeProvenance::Unknown,
    )
    .into_response()
    .expect("response");

    assert!(response.headers().get("connection").is_none());
    assert!(response.headers().get("x-private").is_none());
    assert_eq!(response.headers().get_all("set-cookie").iter().count(), 2);
    assert!(response.into_body().is_stream());
}

#[test]
fn outbound_response_into_response_drops_bodyless_status_stream_without_polling() {
    for (method, status, expected_content_length) in [
        (Method::GET, StatusCode::EARLY_HINTS, None),
        (Method::GET, StatusCode::NO_CONTENT, None),
        (Method::GET, StatusCode::RESET_CONTENT, Some("0")),
        (Method::GET, StatusCode::NOT_MODIFIED, None),
        (Method::HEAD, StatusCode::OK, None),
    ] {
        let polls = Rc::new(Cell::new(0_usize));
        let drops = Rc::new(Cell::new(0_usize));
        let observed_polls = Rc::clone(&polls);
        let observed_drops = Rc::clone(&drops);
        let drop_signal = BodyDropSignal(observed_drops);
        let body = Body::from_stream(stream::poll_fn(move |_| {
            let _keep_signal_alive = &drop_signal;
            observed_polls.set(observed_polls.get().saturating_add(1));
            Poll::Ready(Some(Ok(Bytes::from_static(b"must-not-escape"))))
        }));

        let response = OutboundResponse::new(
            method,
            status,
            HeaderMap::new(),
            body,
            MonotonicClock::default(),
            super::ContentTypeProvenance::Unknown,
        )
        .into_response()
        .expect("bodyless response");

        assert_eq!(polls.get(), 0, "{status}");
        assert_eq!(drops.get(), 1, "{status}");
        assert_eq!(response.body().as_bytes(), Some([].as_slice()), "{status}");
        assert_eq!(
            response
                .headers()
                .get("content-length")
                .and_then(|value| value.to_str().ok()),
            expected_content_length,
            "{status}"
        );
    }
}

#[test]
fn outbound_response_json_error_classification() {
    let response = response_with_body(Body::from(serde_json::json!({"ok": true}).to_string()));
    let value: serde_json::Value = response.json().expect("json");
    assert_eq!(value["ok"], true);

    let malformed = response_with_body(Body::from("not-json"));
    let error = malformed
        .json::<serde_json::Value>()
        .expect_err("malformed JSON");
    assert!(matches!(
        error,
        EdgeError::BadGateway {
            reason: BadGatewayReason::Decode(BadGatewayDecodeReason::Json),
            ..
        }
    ));

    let streamed = response_with_body(Body::stream(stream::iter([Bytes::from(
        serde_json::json!({}).to_string(),
    )])))
    .json::<serde_json::Value>()
    .expect_err("stream requires bounded helper");
    assert!(matches!(
        streamed,
        EdgeError::BadGateway {
            reason: BadGatewayReason::Protocol,
            ..
        }
    ));

    let streamed = response_with_body(Body::stream(stream::iter([Bytes::from(
        serde_json::json!({}).to_string(),
    )])));
    let value: serde_json::Value = block_on(streamed.json_bounded(2)).expect("bounded json");
    assert_eq!(value, serde_json::json!({}));
}

#[test]
fn outbound_response_until_deadline_wins_ready_result() {
    let deadline = Deadline::at_instant(MonotonicInstant::now());
    let response = response_with_body(Body::from_stream(stream::iter([Err(
        EdgeError::bad_gateway("ready source error"),
    )])));
    let error =
        block_on(response.into_bytes_bounded_until(100, deadline)).expect_err("expired deadline");
    assert!(matches!(
        error,
        EdgeError::GatewayTimeout {
            cause: BudgetSource::Unspecified,
            ..
        }
    ));
}

#[test]
fn outbound_response_deadline_uses_its_injected_clock() {
    let start = MonotonicInstant::now();
    let observed = Arc::new(Mutex::new(start));
    let clock_now = Arc::clone(&observed);
    let clock = MonotonicClock::new(move || *clock_now.lock().expect("clock lock"));
    let deadline = start.checked_add(Duration::from_secs(1)).expect("deadline");
    let response = OutboundResponse::new(
        Method::GET,
        StatusCode::OK,
        HeaderMap::new(),
        Body::from("ready"),
        clock,
        super::ContentTypeProvenance::Unknown,
    );
    *observed.lock().expect("clock lock") = deadline;

    let error = block_on(response.into_bytes_bounded_until(100, Deadline::at_instant(deadline)))
        .expect_err("injected clock reached deadline");

    assert!(matches!(error, EdgeError::GatewayTimeout { .. }));
}

#[test]
fn response_content_type_provenance_precedes_filtering() {
    use super::{ContentTypeIneligibility, ContentTypeProvenance};

    let cases: &[(&[&[u8]], ContentTypeProvenance)] = &[
        (
            &[],
            ContentTypeProvenance::Ineligible(ContentTypeIneligibility::Missing),
        ),
        (
            &[b"application/json"],
            ContentTypeProvenance::SingleTextUnchanged,
        ),
        (
            &[b"caf\xc3\xa9"],
            ContentTypeProvenance::SingleTextUnchanged,
        ),
        (
            &[b"not a MIME type"],
            ContentTypeProvenance::SingleTextUnchanged,
        ),
        (
            &[b"application/json, text/plain"],
            ContentTypeProvenance::SingleTextUnchanged,
        ),
        (
            &[b"application/json; note=\"x,y\""],
            ContentTypeProvenance::SingleTextUnchanged,
        ),
        (
            &[b"application/json", b"application/json"],
            ContentTypeProvenance::Ineligible(ContentTypeIneligibility::MultipleValues),
        ),
        (
            &[b"application/json", b"text/plain"],
            ContentTypeProvenance::Ineligible(ContentTypeIneligibility::MultipleValues),
        ),
        (
            &[b"application/json", b"\xff"],
            ContentTypeProvenance::Ineligible(ContentTypeIneligibility::MultipleValues),
        ),
        (
            &[b"\xff"],
            ContentTypeProvenance::Ineligible(ContentTypeIneligibility::NonText),
        ),
    ];
    for (values, expected) in cases {
        let mut headers = HeaderMap::new();
        for value in *values {
            headers.append(
                "content-type",
                HeaderValue::from_bytes(value).expect("header bytes"),
            );
        }
        let normalized = normalize_response_headers(&Method::GET, StatusCode::OK, &mut headers)
            .expect("normalized head");
        assert_eq!(normalized.content_type_provenance, *expected);
        if *expected == ContentTypeProvenance::SingleTextUnchanged {
            assert_eq!(
                headers.get("content-type").expect("retained").as_bytes(),
                values[0]
            );
        }
        assert!(
            headers
                .get_all("content-type")
                .iter()
                .all(|value| str::from_utf8(value.as_bytes()).is_ok())
        );
    }

    let mut headers = HeaderMap::new();
    headers.insert("content-type", HeaderValue::from_static("application/json"));
    headers.insert("connection", HeaderValue::from_static("content-type"));
    let normalized = normalize_response_headers(&Method::HEAD, StatusCode::OK, &mut headers)
        .expect("bodyless normalized head");
    assert_eq!(
        normalized.body_disposition,
        ResponseBodyDisposition::FramingBodyless
    );
    assert_eq!(
        normalized.content_type_provenance,
        ContentTypeProvenance::Ineligible(ContentTypeIneligibility::Removed)
    );
    assert!(!headers.contains_key("content-type"));
}

#[test]
fn response_content_type_provenance_survives_ownership_without_upgrades() {
    use super::{ContentTypeIneligibility, ContentTypeProvenance};

    for initial in [
        ContentTypeProvenance::SingleTextUnchanged,
        ContentTypeProvenance::Ineligible(ContentTypeIneligibility::MultipleValues),
        ContentTypeProvenance::Unknown,
    ] {
        let mut headers = HeaderMap::new();
        headers.insert("content-type", HeaderValue::from_static("application/json"));
        let mut response = OutboundResponse::new(
            Method::GET,
            StatusCode::OK,
            headers,
            Body::empty(),
            MonotonicClock::default(),
            initial,
        );
        assert_eq!(response.content_type_provenance(), initial);
        response
            .headers_mut()
            .insert("content-type", HeaderValue::from_static("text/plain"));
        let expected = if initial == ContentTypeProvenance::SingleTextUnchanged {
            ContentTypeProvenance::Ineligible(ContentTypeIneligibility::Modified)
        } else {
            initial
        };
        assert_eq!(response.content_type_provenance(), expected);
        let response = response.into_response().expect("converted response");
        assert_eq!(
            response.extensions().get::<ContentTypeProvenance>(),
            Some(&expected)
        );
    }
    let response = OutboundResponse::new(
        Method::GET,
        StatusCode::OK,
        HeaderMap::new(),
        Body::empty(),
        MonotonicClock::default(),
        ContentTypeProvenance::Unknown,
    );
    let (_, _, _, _, provenance) = response.into_parts();
    assert_eq!(provenance, ContentTypeProvenance::Unknown);
}

#[test]
fn response_normalization_precedence_table() {
    let mut nominated = HeaderMap::new();
    nominated.append(
        "connection",
        HeaderValue::from_static("content-encoding, content-length"),
    );
    nominated.append("content-encoding", HeaderValue::from_static("gzip"));
    nominated.append("content-length", HeaderValue::from_static("malformed"));
    nominated.append("keep-alive", HeaderValue::from_static("timeout=5"));
    assert_eq!(
        normalize_response_headers(&Method::GET, StatusCode::OK, &mut nominated)
            .expect("nominations precede semantics")
            .body_disposition,
        ResponseBodyDisposition::Payload
    );
    assert!(nominated.get("content-encoding").is_none());
    assert!(nominated.get("content-length").is_none());
    assert!(nominated.get("keep-alive").is_none());

    let mut ambiguous_encoding = HeaderMap::new();
    ambiguous_encoding.append("content-encoding", HeaderValue::from_static("gzip"));
    ambiguous_encoding.append(
        "content-encoding",
        HeaderValue::from_bytes(&[0xff]).expect("opaque header value"),
    );
    normalize_response_headers(&Method::GET, StatusCode::OK, &mut ambiguous_encoding)
        .expect("invalid encoding sibling must remain visible to classification");
    assert_eq!(
        classify_content_encoding(&ambiguous_encoding),
        ContentEncoding::Passthrough(PassthroughReason::Stacked)
    );
    assert_eq!(
        ambiguous_encoding
            .get_all("content-encoding")
            .iter()
            .count(),
        2
    );

    for malformed in ["x-good,,x-other", "bad name", ",x-good", "x-good,"] {
        let mut headers = HeaderMap::new();
        headers.append(
            "connection",
            HeaderValue::from_bytes(malformed.as_bytes()).expect("header"),
        );
        let error = normalize_response_headers(&Method::GET, StatusCode::OK, &mut headers)
            .expect_err("malformed connection");
        assert!(matches!(
            error,
            EdgeError::BadGateway {
                reason: BadGatewayReason::Protocol,
                ..
            }
        ));
    }

    for status in [
        StatusCode::CONTINUE,
        StatusCode::NO_CONTENT,
        StatusCode::NOT_MODIFIED,
    ] {
        let mut headers = HeaderMap::new();
        headers.append("content-length", HeaderValue::from_static("7"));
        assert_eq!(
            normalize_response_headers(&Method::GET, status, &mut headers)
                .expect("bodyless")
                .body_disposition,
            ResponseBodyDisposition::FramingBodyless
        );
        if status == StatusCode::NOT_MODIFIED {
            assert_eq!(headers.get("content-length").expect("metadata"), "7");
        } else {
            assert!(headers.get("content-length").is_none());
        }
    }

    for (value, declared_body) in [(None, false), (Some("0"), false), (Some("7"), true)] {
        let mut headers = HeaderMap::new();
        if let Some(value) = value {
            headers.append(
                "content-length",
                HeaderValue::from_bytes(value.as_bytes()).expect("length"),
            );
        }
        assert_eq!(
            normalize_response_headers(&Method::GET, StatusCode::RESET_CONTENT, &mut headers)
                .expect("205")
                .body_disposition,
            ResponseBodyDisposition::ResetContent { declared_body }
        );
        assert_eq!(headers.get("content-length").expect("normalized"), "0");
    }

    let mut conflicting = HeaderMap::new();
    conflicting.append("content-length", HeaderValue::from_static("7"));
    conflicting.append("content-length", HeaderValue::from_static("8"));
    normalize_response_headers(&Method::GET, StatusCode::OK, &mut conflicting)
        .expect_err("conflicting content-length");

    let mut idempotent = HeaderMap::new();
    idempotent.append("x-keep", HeaderValue::from_static("yes"));
    normalize_response_headers(&Method::GET, StatusCode::OK, &mut idempotent).expect("first");
    let once = idempotent.clone();
    normalize_response_headers(&Method::GET, StatusCode::OK, &mut idempotent).expect("second");
    assert_eq!(idempotent, once);
}

#[test]
fn response_normalization_rejects_non_utf8_content_length() {
    let mut headers = HeaderMap::new();
    headers.append(
        "content-length",
        HeaderValue::from_bytes(&[0xff]).expect("opaque content-length"),
    );

    let error = normalize_response_headers(&Method::GET, StatusCode::OK, &mut headers)
        .expect_err("non-UTF-8 content-length must fail closed");

    assert!(matches!(
        error,
        EdgeError::BadGateway {
            reason: BadGatewayReason::Protocol,
            ..
        }
    ));
}

#[test]
fn head_and_not_modified_validate_and_retain_representation_length() {
    for (method, status) in [
        (Method::HEAD, StatusCode::OK),
        (Method::GET, StatusCode::NOT_MODIFIED),
    ] {
        for invalid in ["not-a-number", "18446744073709551616"] {
            let mut headers = HeaderMap::new();
            headers.append(
                "content-length",
                HeaderValue::from_bytes(invalid.as_bytes()).expect("header value"),
            );
            let error = normalize_response_headers(&method, status, &mut headers)
                .expect_err("invalid bodyless content-length");
            assert!(matches!(
                error,
                EdgeError::BadGateway {
                    reason: BadGatewayReason::Protocol,
                    ..
                }
            ));
        }

        let mut conflicting = HeaderMap::new();
        conflicting.append("content-length", HeaderValue::from_static("7"));
        conflicting.append("content-length", HeaderValue::from_static("8"));
        let error = normalize_response_headers(&method, status, &mut conflicting)
            .expect_err("conflicting bodyless content-length");
        assert!(matches!(
            error,
            EdgeError::BadGateway {
                reason: BadGatewayReason::Protocol,
                ..
            }
        ));

        let mut valid = HeaderMap::new();
        valid.append("content-length", HeaderValue::from_static("4294967296"));
        assert_eq!(
            normalize_response_headers(&method, status, &mut valid)
                .expect("valid bodyless metadata")
                .body_disposition,
            ResponseBodyDisposition::FramingBodyless
        );
        assert_eq!(valid.get("content-length").expect("length"), "4294967296");
    }
}

#[test]
fn portable_dispatch_modes_share_batch_shape_validation_without_polling() {
    for mode in [
        OutboundSendMode::Send,
        OutboundSendMode::Follow,
        OutboundSendMode::Begin,
        OutboundSendMode::Batch,
    ] {
        for streamed_upload in [false, true] {
            let polls = Arc::new(AtomicUsize::new(0));
            let source_polls = Arc::clone(&polls);
            let mut request = OutboundRequest::post("https://example.com/")
                .expect("request")
                .stream_response()
                .timeout(Duration::ZERO);
            if streamed_upload {
                request = request.body(Body::from_stream(stream::poll_fn(move |_cx| {
                    source_polls.fetch_add(1, Ordering::SeqCst);
                    Poll::<Option<Result<Bytes, EdgeError>>>::Pending
                })));
            }
            let result = validate_for_dispatch(&request, mode);
            if mode == OutboundSendMode::Batch
                || (mode == OutboundSendMode::Begin && streamed_upload)
            {
                assert_eq!(
                    bad_request_message(result.expect_err("batch shape")),
                    if mode == OutboundSendMode::Begin {
                        "begin requires a buffered request body; use send for a streamed upload"
                    } else if streamed_upload {
                        "outbound batches require buffered request bodies; use send for a streamed upload"
                    } else {
                        "outbound batches require buffered responses; use send for a streamed response"
                    }
                );
            } else {
                result.expect("single exchange accepts streams before budget selection");
            }
            assert_eq!(polls.load(Ordering::SeqCst), 0);
        }
    }
}

#[test]
fn portable_dispatch_modes_keep_method_and_upload_cap_precedence() {
    for mode in [
        OutboundSendMode::Send,
        OutboundSendMode::Follow,
        OutboundSendMode::Begin,
        OutboundSendMode::Batch,
    ] {
        for method in [Method::GET, Method::HEAD] {
            let polls = Rc::new(Cell::new(0_usize));
            let counted = Rc::clone(&polls);
            let request = OutboundRequest::new(method, Uri::from_static("https://example.com/"))
                .expect("request")
                .body(Body::from_stream(stream::poll_fn(move |_cx| {
                    counted.set(counted.get() + 1);
                    Poll::Pending
                })))
                .stream_response();
            let result = validate_for_dispatch(&request, mode);
            if mode == OutboundSendMode::Batch {
                assert_eq!(
                    bad_request_message(result.expect_err("batch GET/HEAD stream")),
                    "outbound batches require buffered request bodies"
                );
            } else {
                result.expect("GET/HEAD stream is settled by the single-exchange facade");
            }
            assert_eq!(
                polls.get(),
                0,
                "validation must not settle or poll the body"
            );
        }
        let request = OutboundRequest::post("https://example.com/")
            .expect("request")
            .body("too large")
            .max_request_body_bytes(1)
            .stream_response();
        assert_eq!(
            validate_for_dispatch(&request, mode)
                .expect_err("upload cap")
                .request_body_overflow_origin(),
            Some(RequestBodyOverflowOrigin::OutboundUpload)
        );
    }
}

#[test]
fn image_optimizer_mode_rejection_precedes_ordinary_validation() {
    for mode in [OutboundSendMode::Begin, OutboundSendMode::Batch] {
        let request = OutboundRequest::post("https://example.com/")
            .expect("request")
            .image_optimizer(super::OutboundImageOptimizerOptions::new())
            .body(Body::from_stream(stream::pending()))
            .max_brotli_window_bits(9)
            .timeout(Duration::ZERO);
        assert_eq!(
            bad_request_message(
                validate_for_dispatch(&request, mode).expect_err("unsupported mode")
            ),
            "image_optimizer is not supported for eager outbound requests"
        );
    }
    for mode in [OutboundSendMode::Send, OutboundSendMode::Follow] {
        let request = OutboundRequest::get("https://example.com/")
            .expect("request")
            .image_optimizer(
                super::OutboundImageOptimizerOptions::new()
                    .region(super::OutboundImageOptimizerRegion::Asia),
            );
        validate_for_dispatch(&request, mode).expect("single-send optimizer combination");
    }
}

#[test]
fn buffered_upload_overflow_is_rejected_during_common_preflight() {
    for len in [7_usize, 8, 9] {
        let request = OutboundRequest::post("https://example.com/upload")
            .expect("request")
            .body(vec![b'x'; len])
            .max_request_body_bytes(8);
        let result = validate_for_dispatch(&request, OutboundSendMode::Send);
        if len > 8 {
            let error = result.expect_err("buffered overflow before dispatch");
            assert_eq!(error.status(), StatusCode::PAYLOAD_TOO_LARGE);
            assert_eq!(error.kind(), "payload_too_large");
        } else {
            result.expect("within cap");
        }
    }
}

#[test]
fn dispatch_validation_precedence_table() {
    for method in [
        Method::GET,
        Method::HEAD,
        Method::POST,
        Method::PUT,
        Method::PATCH,
        Method::DELETE,
        Method::OPTIONS,
    ] {
        let request =
            OutboundRequest::new(method, Uri::from_static("https://example.com/resource"))
                .expect("portable request");
        validate_for_dispatch(&request, OutboundSendMode::Send).expect("portable method");
    }

    let request = OutboundRequest::new(
        Method::CONNECT,
        Uri::from_static("https://example.com/resource"),
    )
    .expect("request");
    assert_eq!(
        bad_request_message(
            validate_for_dispatch(&request, OutboundSendMode::Send).expect_err("custom method")
        ),
        "method CONNECT is not portable; supported: GET, HEAD, POST, PUT, PATCH, DELETE, OPTIONS"
    );

    let request = OutboundRequest::get("https://example.com")
        .expect("request")
        .body("not empty");
    assert_eq!(
        bad_request_message(
            validate_for_dispatch(&request, OutboundSendMode::Send).expect_err("GET body")
        ),
        "GET/HEAD request must not carry a body"
    );

    let request = OutboundRequest::get("https://example.com")
        .expect("request")
        .body(Body::stream(stream::iter([Bytes::new()])));
    assert!(request.is_stream_body());
    validate_for_dispatch(&request, OutboundSendMode::Send).expect("GET stream awaits settlement");
    let response = block_on(HttpClient::with_client(MarkerClient).send(request))
        .expect("GET stream with only empty chunks settles to an empty request body");
    assert_eq!(response.status(), StatusCode::OK);
    let nonempty = OutboundRequest::get("https://example.com")
        .expect("request")
        .body(Body::stream(stream::iter([Bytes::from_static(b"data")])));
    assert_eq!(
        bad_request_message(
            block_on(HttpClient::with_client(MarkerClient).send(nonempty))
                .expect_err("GET payload rejected during settlement")
        ),
        "GET/HEAD request must not carry a body"
    );

    let request = OutboundRequest::post("https://example.com")
        .expect("request")
        .body(Body::stream(stream::iter([Bytes::from_static(b"data")])))
        .stream_response();
    assert!(request.is_stream_body());
    assert!(request.is_stream_response());
    validate_for_dispatch(&request, OutboundSendMode::Send).expect("POST stream");

    for bits in [9, 31] {
        let request = OutboundRequest::get("https://example.com")
            .expect("request")
            .max_brotli_window_bits(bits);
        assert!(
            validate_for_dispatch(&request, OutboundSendMode::Send).is_err(),
            "accepted {bits}"
        );
    }
    for bits in [10, 30] {
        let request = OutboundRequest::get("https://example.com")
            .expect("request")
            .max_brotli_window_bits(bits);
        validate_for_dispatch(&request, OutboundSendMode::Send).expect("valid Brotli window");
    }
}

#[test]
fn request_normalization_is_idempotent() {
    let mut request = OutboundRequest::post("https://example.com")
        .expect("request")
        .header("connection", "x-first, x-second")
        .expect("connection")
        .header("x-first", "one")
        .expect("first")
        .header("x-second", "two")
        .expect("second")
        .header("x-keep", "yes")
        .expect("keep");

    normalize_for_dispatch(&mut request).expect("first pass");
    let once = request.headers().clone();
    normalize_for_dispatch(&mut request).expect("second pass");
    assert_eq!(request.headers(), &once);
    assert_eq!(
        request.headers().get("x-keep"),
        Some(&HeaderValue::from_static("yes"))
    );
}

#[test]
fn request_normalization_strips_connection_nominations() {
    let mut request = OutboundRequest::post("https://example.com")
        .expect("request")
        .header("connection", "x-first")
        .expect("connection")
        .header("connection", "x-second, X-Third")
        .expect("connection")
        .header("x-first", "one")
        .expect("first")
        .header("x-second", "two")
        .expect("second")
        .header("x-third", "three")
        .expect("third")
        .header("keep-alive", "timeout=5")
        .expect("keep-alive")
        .header("proxy-authenticate", "challenge")
        .expect("proxy-authenticate")
        .header("proxy-authorization", "secret")
        .expect("proxy-authorization")
        .header("te", "trailers")
        .expect("te")
        .header("trailer", "x-checksum")
        .expect("trailer")
        .header("transfer-encoding", "chunked")
        .expect("transfer-encoding")
        .header("upgrade", "websocket")
        .expect("upgrade")
        .header("host", "wrong.example")
        .expect("host")
        .header("content-length", "100")
        .expect("content-length")
        .header("x-keep", "yes")
        .expect("keep");

    normalize_for_dispatch(&mut request).expect("normalize");
    for name in [
        "connection",
        "x-first",
        "x-second",
        "x-third",
        "keep-alive",
        "proxy-authenticate",
        "proxy-authorization",
        "te",
        "trailer",
        "transfer-encoding",
        "upgrade",
        "host",
        "content-length",
    ] {
        assert!(request.headers().get(name).is_none(), "retained {name}");
    }
    assert_eq!(
        request.headers().get("x-keep"),
        Some(&HeaderValue::from_static("yes"))
    );
}

#[test]
fn request_normalization_rejects_malformed_connection() {
    for value in ["x-valid,,x-other", "bad name", ",x-valid", "x-valid,"] {
        let mut request = OutboundRequest::get("https://example.com").expect("request");
        request.headers_mut().append(
            "connection",
            HeaderValue::from_str(value).expect("header value"),
        );
        assert!(
            normalize_for_dispatch(&mut request).is_err(),
            "accepted {value}"
        );
    }

    let mut request = OutboundRequest::get("https://example.com").expect("request");
    request.headers_mut().append(
        "connection",
        HeaderValue::from_bytes(b"x-private,\xff").expect("opaque header"),
    );
    normalize_for_dispatch(&mut request).expect_err("opaque connection nomination");

    let mut request = OutboundRequest::get("https://example.com").expect("request");
    request.headers_mut().append(
        "connection",
        HeaderValue::from_bytes(b"\xc2\xa0x-private").expect("UTF-8 header value"),
    );
    normalize_for_dispatch(&mut request).expect_err("non-HTTP whitespace nomination");

    let mut headers = HeaderMap::new();
    headers.append(
        "connection",
        HeaderValue::from_bytes(b"\xc2\xa0x-private").expect("UTF-8 header value"),
    );
    normalize_response_headers(&Method::GET, StatusCode::OK, &mut headers)
        .expect_err("non-HTTP whitespace response nomination");
}

#[test]
fn request_normalization_drops_non_utf8_content_encoding() {
    let mut request = OutboundRequest::post("https://example.com").expect("request");
    request.headers_mut().append(
        "content-encoding",
        HeaderValue::from_bytes(&[0xff]).expect("opaque header value"),
    );

    normalize_for_dispatch(&mut request).expect("normalize");

    assert!(request.headers().get("content-encoding").is_none());
}

#[test]
#[expect(
    clippy::cognitive_complexity,
    reason = "the round-trip contract intentionally verifies every independent outbound field"
)]
fn outbound_request_defaults_and_parts_round_trip() {
    let deadline = Deadline::after(Duration::from_secs(9));
    let request = OutboundRequest::post("https://example.com/items")
        .expect("request")
        .body("payload")
        .cache_policy(OutboundCachePolicy::Bypass)
        .deadline(deadline)
        .header("x-test", "one")
        .expect("header")
        .host_authority_override("[2001:db8::1]:8443")
        .expect("authority override")
        .max_decoder_bytes(40 * 1024 * 1024)
        .max_brotli_window_bits(23)
        .max_chunk_bytes(NonZeroU64::new(4096).expect("nonzero"))
        .max_decoded_response_bytes(2_000_000)
        .max_encoded_response_bytes(1_000_000)
        .max_request_body_bytes(3_000_000)
        .max_response_bytes(4_000_000)
        .max_response_header_bytes(32_000)
        .max_response_header_count(80)
        .timeout(Duration::from_secs(8));

    let parts = request.into_parts();
    assert_eq!(parts.method, Method::POST);
    assert_eq!(parts.uri, Uri::from_static("https://example.com/items"));
    assert_eq!(parts.cache_policy, OutboundCachePolicy::Bypass);
    assert_eq!(
        parts
            .host_authority_override
            .as_ref()
            .map(Authority::as_str),
        Some("[2001:db8::1]:8443")
    );
    assert_eq!(parts.body.as_bytes(), Some(b"payload".as_slice()));
    assert_eq!(
        parts.deadline.expect("deadline").instant(),
        deadline.instant()
    );
    assert_eq!(
        parts.headers.get("x-test"),
        Some(&HeaderValue::from_static("one"))
    );
    assert_eq!(parts.max_decoder_bytes, 40 * 1024 * 1024);
    assert_eq!(parts.max_brotli_window_bits, 23);
    assert_eq!(parts.max_chunk_bytes.map(NonZeroU64::get), Some(4096));
    assert_eq!(parts.max_decoded_response_bytes, Some(2_000_000));
    assert_eq!(parts.max_encoded_response_bytes, Some(1_000_000));
    assert_eq!(parts.max_request_body_bytes, 3_000_000);
    assert_eq!(parts.max_response_header_bytes, Some(32_000));
    assert_eq!(parts.max_response_header_count, Some(80));
    assert_eq!(
        parts.response_mode,
        ResponseMode::Buffered {
            max_bytes: 4_000_000
        }
    );
    assert_eq!(parts.timeout, Some(Duration::from_secs(8)));

    let request = OutboundRequest::from_parts(parts).expect("round trip");
    assert_eq!(request.method(), &Method::POST);
    assert_eq!(request.host_authority(), "[2001:db8::1]:8443");
    assert_eq!(request.backend_target(), "example.com:443");
    assert_eq!(request.sni_hostname(), Some("example.com"));
    assert_eq!(request.cert_host(), Some("example.com"));
    assert_eq!(
        request.uri(),
        &Uri::from_static("https://example.com/items")
    );

    let defaults = OutboundRequest::get("https://example.com")
        .expect("defaults")
        .into_parts();
    assert_eq!(defaults.max_decoder_bytes, 0x0200_0000);
    assert_eq!(defaults.max_brotli_window_bits, 24);
    assert_eq!(defaults.max_request_body_bytes, 0x0080_0000);
    assert_eq!(defaults.cache_policy, OutboundCachePolicy::PlatformDefault);
    assert!(defaults.host_authority_override.is_none());
    assert_eq!(
        defaults.response_mode,
        ResponseMode::Buffered {
            max_bytes: 0x0010_0000
        }
    );
    assert!(defaults.deadline.is_none());
    assert!(defaults.max_decoded_response_bytes.is_none());
    assert!(defaults.max_encoded_response_bytes.is_none());
    assert!(defaults.max_response_header_bytes.is_none());
    assert!(defaults.max_response_header_count.is_none());
    assert!(defaults.timeout.is_none());
}

#[test]
fn outbound_request_policy_accepts_only_http_authorities() {
    for authority in [
        "api.example.com",
        "api.example.com:8443",
        "api.example.com:65535",
        "192.0.2.1:8080",
        "[2001:db8::1]",
        "[2001:db8::1]:8443",
    ] {
        let request = OutboundRequest::get("https://origin.example/resource")
            .expect("request")
            .host_authority_override(authority)
            .expect("valid authority");
        assert!(request.has_host_authority_override());
        assert_eq!(request.host_authority(), authority);
        assert_eq!(request.backend_target(), "origin.example:443");
        assert_eq!(request.sni_hostname(), Some("origin.example"));
        assert_eq!(request.cert_host(), Some("origin.example"));
    }

    for invalid in [
        "",
        "user@api.example.com",
        "https://api.example.com",
        "api.example.com/path",
        "api example.com",
        "api.example.com\r\nx-added: value",
        "2001:db8::1",
        "api.example.com:",
        "api.example.com:abc",
        "api.example.com:65536",
        "[2001:db8::1]:abc",
        "[2001:db8::1]:65536",
    ] {
        let error = OutboundRequest::get("https://origin.example")
            .expect("request")
            .host_authority_override(invalid)
            .expect_err("invalid authority");
        assert_eq!(
            bad_request_message(error),
            "invalid outbound host authority override"
        );
    }
}

#[test]
fn outbound_request_response_mode_setters_are_last_write_wins() {
    let streamed = OutboundRequest::get("https://example.com")
        .expect("streamed request")
        .max_response_bytes(7)
        .stream_response()
        .into_parts();
    assert_eq!(streamed.response_mode, ResponseMode::Streamed);

    let buffered = OutboundRequest::get("https://example.com")
        .expect("buffered request")
        .stream_response()
        .max_response_bytes(7)
        .into_parts();
    assert_eq!(
        buffered.response_mode,
        ResponseMode::Buffered { max_bytes: 7 }
    );
}

#[test]
fn outbound_request_canonicalizes_url_table() {
    let cases = [
        ("HTTPS://EXAMPLE.com:443/a/../b", "https://example.com/b"),
        ("http://example.com:80", "http://example.com/"),
        ("https://example.com:8443", "https://example.com:8443/"),
        ("https://127.0.0.1", "https://127.0.0.1/"),
        ("https://[::1]:443/a", "https://[::1]/a"),
        ("https://caf%C3%A9.example/", "https://xn--caf-dma.example/"),
    ];

    for (input, expected) in cases {
        let request = OutboundRequest::get(input).expect(input);
        assert_eq!(request.uri().to_string(), expected, "{input}");
    }

    let dns = OutboundRequest::get("https://example.com").expect("dns");
    assert_eq!(dns.backend_target(), "example.com:443");
    assert_eq!(dns.cert_host(), Some("example.com"));
    assert_eq!(dns.host_authority(), "example.com");
    assert_eq!(dns.host_name(), "example.com");
    assert_eq!(dns.sni_hostname(), Some("example.com"));

    let ip = OutboundRequest::get("https://[::1]:8443").expect("ip");
    assert_eq!(ip.backend_target(), "[::1]:8443");
    assert_eq!(ip.cert_host(), Some("::1"));
    assert_eq!(ip.host_authority(), "[::1]:8443");
    assert_eq!(ip.host_name(), "::1");
    assert_eq!(ip.sni_hostname(), None);
}

#[test]
fn outbound_request_rejects_invalid_target_table() {
    for target in [
        "ftp://example.com",
        "/relative",
        "https:///missing",
        "https://user:pass@example.com",
        "https://example.com/path#fragment",
    ] {
        assert!(OutboundRequest::get(target).is_err(), "accepted {target}");
    }
}

#[test]
fn outbound_request_rejects_empty_userinfo() {
    OutboundRequest::get("https://@example.com/").expect_err("empty userinfo");
}

#[test]
fn typed_outbound_request_rejects_empty_userinfo() {
    let typed = "https://@example.com/"
        .parse::<Uri>()
        .expect("typed URI retains empty userinfo syntax");
    OutboundRequest::new(Method::GET, typed.clone()).expect_err("typed empty userinfo");

    let mut parts = OutboundRequest::get("https://example.com/")
        .expect("baseline request")
        .into_parts();
    parts.uri = typed;
    OutboundRequest::from_parts(parts).expect_err("parts empty userinfo");
}

#[test]
fn outbound_request_rejects_backslash_authority_forms() {
    for target in [
        r"https:\\@example.com/",
        r"https:/\@example.com/",
        r"https:\/\@example.com/",
        r"https://example.com\path",
    ] {
        assert!(OutboundRequest::get(target).is_err(), "accepted {target}");
    }
}

#[test]
fn outbound_request_preserves_percent_encoded_hash() {
    let request =
        OutboundRequest::get("https://example.com/a%23b?q=%40user").expect("encoded delimiters");
    assert_eq!(
        request.uri().to_string(),
        "https://example.com/a%23b?q=%40user"
    );
}

#[test]
fn outbound_request_from_request_normalizes_immediately() {
    let request = request_builder()
        .method(Method::POST)
        .uri("/local")
        .header("connection", "x-remove")
        .header("x-remove", "gone")
        .header("host", "local.invalid")
        .header("content-length", "7")
        .header("transfer-encoding", "chunked")
        .header("x-keep", "yes")
        .body(Body::from("payload"))
        .expect("inbound request");

    let outbound =
        OutboundRequest::from_request(request, Uri::from_static("https://example.com/target"))
            .expect("outbound request");
    assert_eq!(outbound.method(), &Method::POST);
    assert_eq!(
        outbound.headers().get("x-keep"),
        Some(&HeaderValue::from_static("yes"))
    );
    for removed in [
        "connection",
        "x-remove",
        "host",
        "content-length",
        "transfer-encoding",
    ] {
        assert!(
            !outbound.headers().contains_key(removed),
            "retained {removed}"
        );
    }
    assert_eq!(
        outbound.into_parts().body.as_bytes(),
        Some(b"payload".as_slice())
    );

    let request = request_builder()
        .method(Method::GET)
        .uri("/local")
        .header("x-keep", "yes")
        .body(Body::empty())
        .expect("inbound request");
    let outbound =
        OutboundRequest::from_request(request, Uri::from_static("https://example.com/target"))
            .expect("outbound request");
    assert_eq!(
        outbound.headers().get("x-keep"),
        Some(&HeaderValue::from_static("yes"))
    );
}

#[test]
fn outbound_normalization_strips_proxy_connection() {
    let mut request = OutboundRequest::get("https://example.com/").expect("request");
    request
        .headers_mut()
        .insert("proxy-connection", HeaderValue::from_static("keep-alive"));
    normalize_for_dispatch(&mut request).expect("request normalization");
    assert!(!request.headers().contains_key("proxy-connection"));

    let mut response_headers = HeaderMap::new();
    response_headers.insert("proxy-connection", HeaderValue::from_static("keep-alive"));
    normalize_response_headers(&Method::GET, StatusCode::OK, &mut response_headers)
        .expect("response normalization");
    assert!(!response_headers.contains_key("proxy-connection"));
}

#[test]
fn outbound_request_rejects_invalid_header_bytes() {
    let request = OutboundRequest::get("https://example.com").expect("request");
    request
        .header(b"bad name", b"value")
        .expect_err("invalid header name");

    let request = OutboundRequest::get("https://example.com").expect("request");
    request
        .header(b"x-test", [0xff])
        .expect_err("invalid header value");

    let request = OutboundRequest::get("https://example.com").expect("request");
    request
        .header(b"x-test", b"line\nbreak")
        .expect_err("header value line break");
}

#[test]
fn forwarding_parts_preserve_inbound_deadline_metadata() {
    use super::ForwardedReadDeadline;

    let deadline = Deadline::after(Duration::from_millis(100));
    let mut inbound = request_builder()
        .method("GET")
        .uri("/")
        .body(Body::from_stream(stream::pending()))
        .expect("inbound");
    inbound
        .extensions_mut()
        .insert(ForwardedReadDeadline::new(deadline));
    let request =
        OutboundRequest::from_request(inbound, "https://example.com/".parse().expect("target"))
            .expect("forward");
    let rebuilt = OutboundRequest::from_parts(request.into_parts()).expect("rebuild");
    assert_eq!(
        rebuilt.inbound_read_deadline.map(|bound| bound.instant()),
        Some(deadline.instant())
    );
}

#[test]
fn callback_exit_revokes_the_entire_unpolled_send_future() {
    let owner = super::OutboundScopeOwner::callback();
    let drops = Rc::new(Cell::new(0));
    let captured = BodyDropSignal(Rc::clone(&drops));
    let body = Body::from_stream(stream::poll_fn(move |_cx| {
        let _keep_alive = &captured;
        Poll::Pending
    }));
    let client = HttpClient::with_client(MarkerClient);
    let request = OutboundRequest::post("https://example.com/unpolled")
        .expect("request")
        .body(body);
    let future = owner.with(|| client.send(request));
    assert_eq!(drops.get(), 0);
    drop(owner);
    assert_eq!(
        drops.get(),
        1,
        "retaining the future must not retain its request"
    );
    assert!(matches!(block_on(future), Err(EdgeError::Internal { .. })));
    assert_eq!(drops.get(), 1);
}

#[test]
fn callback_exit_revokes_a_begin_future_waiting_in_settlement() {
    let owner = super::OutboundScopeOwner::callback();
    let drops = Rc::new(Cell::new(0));
    let polls = Rc::new(Cell::new(0_usize));
    let captured = BodyDropSignal(Rc::clone(&drops));
    let observed = Rc::clone(&polls);
    let body = Body::from_stream(stream::poll_fn(move |_cx| {
        let _keep_alive = &captured;
        observed.set(observed.get().saturating_add(1));
        Poll::Pending
    }));
    let provider = Arc::new(MockClient {
        batch_calls: AtomicUsize::new(0),
        send_calls: AtomicUsize::new(0),
        time: TestTime::new(MonotonicInstant::now()),
    });
    let shared: Arc<dyn OutboundHttpClient> = Arc::<MockClient>::clone(&provider);
    let client = HttpClient::new(shared);
    let request = OutboundRequest::get("https://example.com/settling")
        .expect("request")
        .body(body);
    let mut future = owner.with(|| client.begin(request));
    assert!(
        future
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    assert_eq!(polls.get(), 1);
    assert_eq!(provider.send_calls.load(Ordering::Relaxed), 0);
    drop(owner);
    assert_eq!(drops.get(), 1);
    assert!(matches!(block_on(future), Err(EdgeError::Internal { .. })));
    assert_eq!(polls.get(), 1);
    assert_eq!(provider.send_calls.load(Ordering::Relaxed), 0);
}

#[test]
fn facade_begin_keeps_a_ready_pending_registered_after_begin_returns() {
    let owner = super::OutboundScopeOwner::callback();
    let client = HttpClient::with_client(MarkerClient);
    let request = OutboundRequest::get("https://example.com/ready").expect("request");
    let mut pending = owner
        .with(|| client.begin(request))
        .now_or_never()
        .expect("ready fixture")
        .expect("begun exchange");
    assert!(pending.is_ready());
    owner.close();
    assert!(
        pending.is_ready(),
        "scope closure is a ready terminal error"
    );
    let error =
        block_on(pending.collect()).expect_err("retained pending belongs to closed callback");
    assert!(matches!(error, EdgeError::Internal { .. }));
}

#[test]
fn facade_future_polled_in_another_scope_keeps_its_invocation_scope() {
    let initiating = super::OutboundScopeOwner::callback();
    let foreign = super::OutboundScopeOwner::callback();
    let original_scope = initiating
        .with(|| super::OutboundScope::capture(Rc::new(super::IndependentOutboundProgress)));
    let expected = original_scope
        .provider_state(|| 31_u8)
        .expect("original provider state");
    let foreign_scope =
        foreign.with(|| super::OutboundScope::capture(Rc::new(super::IndependentOutboundProgress)));
    assert_eq!(
        *foreign_scope
            .provider_state(|| 99_u8)
            .expect("foreign provider state"),
        99
    );
    let observed = Rc::new(Cell::new(false));
    let did_poll = Rc::clone(&observed);
    let source = stream::poll_fn(move |_cx| {
        let active = super::OutboundScope::capture(Rc::new(super::IndependentOutboundProgress));
        let current = active
            .provider_state(|| 7_u8)
            .expect("active provider state");
        assert!(Rc::ptr_eq(&expected, &current));
        did_poll.set(true);
        Poll::Ready(None)
    });
    let client = HttpClient::with_client(MarkerClient);
    let request = OutboundRequest::get("https://example.com/foreign")
        .expect("request")
        .body(Body::from_stream(source));
    let future = initiating.with(|| client.send(request));
    block_on(foreign.run(future)).expect("original callback is still open");
    assert!(observed.get());
    assert_eq!(
        *foreign_scope
            .provider_state(|| 0_u8)
            .expect("foreign state unchanged"),
        99
    );
}

#[test]
fn batch_scope_exit_releases_only_producer_and_retains_recorded_partials() {
    let owner = super::OutboundScopeOwner::callback();
    let fixture = BatchFixture::new();
    let stream_drops = Rc::new(Cell::new(0));
    let owner_drops = Rc::new(Cell::new(0_usize));
    let release = BodyDropSignal(Rc::clone(&stream_drops));
    let waiting = stream::poll_fn(move |_cx| {
        let _keep_alive = &release;
        Poll::Pending
    });
    let released_owner = Rc::clone(&owner_drops);
    let mut batch = owner.with(|| {
        OutboundBatch::from_driver(
            fixture.context(3),
            stream::iter([fixture.observation(0), fixture.observation(1)]).chain(waiting),
            OutboundExchangeOwner::new(move || {
                released_owner.set(released_owner.get().saturating_add(1));
            }),
        )
    });
    let super::OutboundBatchCheck::Item(first) = batch.check().expect("record ready arrivals")
    else {
        panic!("first ready receipt");
    };
    assert_eq!(first.index(), 0);
    owner.close();
    assert_eq!(
        stream_drops.get(),
        1,
        "producer stream released before another poll"
    );
    assert_eq!(
        owner_drops.get(),
        1,
        "producer cancellation released immediately"
    );
    let failure = block_on(batch.collect()).expect_err("precise scope closure");
    assert!(matches!(failure.error(), EdgeError::Internal { .. }));
    assert!(
        failure
            .error()
            .inner()
            .expect("scope error")
            .to_string()
            .contains("scope")
    );
    assert!(matches!(
        failure.slots(),
        [
            OutboundCollectedSlot::AlreadyDelivered,
            OutboundCollectedSlot::Resolved(_),
            OutboundCollectedSlot::Unresolved
        ]
    ));
    first
        .result()
        .outcome
        .as_ref()
        .expect("delivered receipt remains valid");
}
