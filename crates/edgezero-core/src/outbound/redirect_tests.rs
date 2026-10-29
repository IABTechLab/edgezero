#![allow(
    clippy::shadow_reuse,
    clippy::shadow_unrelated,
    reason = "table-driven cases reuse request, result and fixture bindings"
)]

use std::cell::{Cell, RefCell};
use std::collections::VecDeque;
use std::future::pending;
use std::iter::repeat_with;
use std::mem::take;
use std::num::NonZeroU64;
use std::rc::Rc;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use bytes::Bytes;
use futures::executor::block_on;
use futures_util::future::poll_fn;
use futures_util::stream::{self, StreamExt as _};

use crate::body::{Body, BodyPayload, BodyStream};
use crate::error::{BadGatewayDecodeReason, BadGatewayReason, BudgetSource, EdgeError};
use crate::http::{HeaderMap, HeaderValue, Method, StatusCode, Uri};
use crate::time::{Deadline, DispatchBudget, MonotonicClock, MonotonicInstant};

use super::redirect::{
    OutboundFollowFailure, OutboundFollowResult, OutboundRedirectAction, OutboundRedirectContext,
    OutboundRedirectLimit, OutboundRedirectStopReason,
};

use super::{
    HttpClient, IndependentOutboundProgress, OutboundBatch, OutboundCachePolicy,
    OutboundDispatchRequest, OutboundExchangeOwner, OutboundFuture, OutboundHead,
    OutboundHeaderFidelity, OutboundHeaderInput, OutboundHttpClient, OutboundImageOptimizerOptions,
    OutboundImageOptimizerRegion, OutboundPending, OutboundPreparedBatch, OutboundProgressRuntime,
    OutboundProxyMarker, OutboundRequest, OutboundRequestParts, OutboundScopeOwner,
    OutboundSendMode, OutboundTime, ResponseDecoding, ResponseMode,
};

static REDIRECT_LOGGER: RedirectLogger = RedirectLogger;

#[derive(Clone)]
struct ManualTime {
    now: Arc<Mutex<MonotonicInstant>>,
    start: MonotonicInstant,
    waiters: Arc<Mutex<Vec<Waker>>>,
}

struct RecordingClient(ManualTime);

struct Recorded {
    body: Option<Bytes>,
    budget: DispatchBudget,
    entered_at: MonotonicInstant,
    mode: OutboundSendMode,
    parts: OutboundRequestParts,
}

struct Reply {
    headers: HeaderMap,
    source: BodyStream,
    status: StatusCode,
}

enum Step {
    Error(EdgeError),
    Head(Reply),
    Pending(Rc<Cell<usize>>),
}

#[derive(Default)]
struct Recording {
    records: Vec<Recorded>,
    steps: VecDeque<Step>,
    validations: Vec<(Uri, OutboundSendMode)>,
}

struct Fixture {
    client: HttpClient,
    state: Rc<RefCell<Recording>>,
    time: ManualTime,
}

struct DropCount(Rc<Cell<usize>>);

struct ScopeCloseOnDrop {
    drops: Rc<Cell<usize>>,
    owner: Option<Rc<OutboundScopeOwner>>,
}

#[derive(Clone, Default)]
struct SourceProbe {
    drops: Rc<Cell<usize>>,
    polls: Rc<Cell<usize>>,
}

#[derive(Clone, Copy)]
enum UnreadBody {
    DeclaredHuge,
    EmptyBrotli,
    Failed,
    Pending,
}

struct PolicyRejection {
    identity: Rc<Cell<usize>>,
}

struct RedirectLogger;

std::thread_local! {
    static RECORDING: RefCell<Option<Rc<RefCell<Recording>>>> = const { RefCell::new(None) };
    static REDIRECT_LOGS: RefCell<Vec<String>> = const { RefCell::new(Vec::new()) };
}

impl ManualTime {
    fn at(&self, millis: u64) -> MonotonicInstant {
        self.start
            .checked_add(Duration::from_millis(millis))
            .expect("bounded test clock offset")
    }

    fn new() -> Self {
        let start = MonotonicInstant::now();
        Self {
            now: Arc::new(Mutex::new(start)),
            start,
            waiters: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn paired(&self) -> OutboundTime {
        let now = Arc::clone(&self.now);
        let waiters = Arc::clone(&self.waiters);
        OutboundTime::new(
            MonotonicClock::new(move || *now.lock().expect("clock")),
            move |deadline, clock| {
                let registrations = Arc::clone(&waiters);
                Box::pin(poll_fn(move |cx| {
                    if deadline.is_expired_at(clock.now()) {
                        Poll::Ready(Ok(()))
                    } else {
                        registrations
                            .lock()
                            .expect("waiters")
                            .push(cx.waker().clone());
                        Poll::Pending
                    }
                }))
            },
        )
    }

    fn set(&self, millis: u64) {
        *self.now.lock().expect("clock") = self.at(millis);
        let waiters = take(&mut *self.waiters.lock().expect("waiters"));
        for waiter in waiters {
            waiter.wake();
        }
    }
}

#[expect(clippy::missing_trait_methods, reason = "Drop::pin_drop is unstable")]
impl Drop for DropCount {
    fn drop(&mut self) {
        self.0.set(self.0.get().saturating_add(1));
    }
}

#[expect(clippy::missing_trait_methods, reason = "Drop::pin_drop is unstable")]
impl Drop for ScopeCloseOnDrop {
    fn drop(&mut self) {
        self.drops.set(self.drops.get().saturating_add(1));
        if let Some(owner) = &self.owner {
            owner.close();
        }
    }
}

impl OutboundHttpClient for RecordingClient {
    fn begin(&self, _request: OutboundDispatchRequest) -> Result<OutboundPending, EdgeError> {
        Err(EdgeError::internal(anyhow::anyhow!(
            "redirect fixture does not begin"
        )))
    }

    fn dispatch_head(&self, request: OutboundDispatchRequest) -> OutboundFuture<OutboundHead> {
        let budget = request.budget();
        let entered_at = request.entered_at();
        let (mut parts, context) = request.into_parts();
        let (payload, owner) = take(&mut parts.body).into_parts();
        assert!(
            owner.is_none(),
            "request preflight rejects response body ownership"
        );
        let body = match payload {
            BodyPayload::Once(bytes) => {
                parts.body = Body::from(bytes.clone());
                Body::from(bytes)
            }
            BodyPayload::Stream(source) => Body::from_stream(source),
        };
        let recorded = Recorded {
            body: body.as_bytes().map(Bytes::copy_from_slice),
            budget,
            entered_at,
            mode: context.mode(),
            parts,
        };
        let step = RECORDING.with(|slot| {
            let active = slot.borrow();
            let mut state = active.as_ref().expect("installed fixture").borrow_mut();
            state.records.push(recorded);
            state
                .steps
                .pop_front()
                .expect("one scripted head per dispatch")
        });
        Box::pin(async move {
            let _upload = body;
            match step {
                Step::Head(reply) => OutboundHead::new(
                    reply.status,
                    OutboundHeaderInput::new(
                        "fixture",
                        reply.headers,
                        OutboundHeaderFidelity::AdapterVisibleFields,
                    ),
                    reply.source,
                    context,
                    OutboundExchangeOwner::new(|| {}),
                ),
                Step::Error(error) => Err(error),
                Step::Pending(drops) => {
                    let _release = DropCount(drops);
                    pending().await
                }
            }
        })
    }

    fn progress(&self) -> Rc<dyn OutboundProgressRuntime> {
        Rc::new(IndependentOutboundProgress)
    }

    fn start_batch_until(&self, batch: OutboundPreparedBatch) -> OutboundBatch {
        let (context, _) = batch.into_slots();
        OutboundBatch::cutoff(context)
    }

    fn time(&self) -> OutboundTime {
        self.0.paired()
    }

    fn validate_supported_options(
        &self,
        request: &OutboundRequest,
        mode: OutboundSendMode,
    ) -> Result<(), EdgeError> {
        RECORDING.with(|slot| {
            slot.borrow()
                .as_ref()
                .expect("fixture")
                .borrow_mut()
                .validations
                .push((request.uri().clone(), mode));
        });
        if request.headers().contains_key("x-fixture-unsupported") {
            Err(EdgeError::bad_request("unsupported fixture option"))
        } else {
            Ok(())
        }
    }
}

impl Fixture {
    fn new(steps: impl IntoIterator<Item = Step>) -> Self {
        let state = Rc::new(RefCell::new(Recording {
            steps: steps.into_iter().collect(),
            ..Recording::default()
        }));
        RECORDING.with(|slot| *slot.borrow_mut() = Some(Rc::clone(&state)));
        let time = ManualTime::new();
        Self {
            client: HttpClient::with_client(RecordingClient(time.clone())),
            state,
            time,
        }
    }

    fn sent(&self) -> usize {
        self.state.borrow().records.len()
    }
}

impl Reply {
    fn bytes(status: StatusCode, bytes: &'static [u8]) -> Self {
        Self {
            headers: HeaderMap::new(),
            source: stream::once(async move { Ok(Bytes::from_static(bytes)) }).boxed_local(),
            status,
        }
    }

    fn location(mut self, target: &str) -> Self {
        self.headers
            .insert("location", target.parse().expect("Location"));
        self
    }

    fn step(self) -> Step {
        Step::Head(self)
    }
}

impl SourceProbe {
    fn source(&self, kind: UnreadBody) -> BodyStream {
        let release = DropCount(Rc::clone(&self.drops));
        let polls = Rc::clone(&self.polls);
        stream::poll_fn(move |_| {
            let _retained = &release;
            polls.set(polls.get().saturating_add(1));
            match kind {
                UnreadBody::Failed => Poll::Ready(Some(Err(EdgeError::bad_gateway_with_reason(
                    "scripted body failure",
                    BadGatewayReason::Unreachable,
                )))),
                UnreadBody::EmptyBrotli | UnreadBody::DeclaredHuge => Poll::Ready(None),
                UnreadBody::Pending => Poll::Pending,
            }
        })
        .boxed_local()
    }
}

impl log::Log for RedirectLogger {
    fn enabled(&self, metadata: &log::Metadata<'_>) -> bool {
        metadata.target() == "edgezero::outbound::redirect"
    }

    fn flush(&self) {}

    fn log(&self, record: &log::Record<'_>) {
        if self.enabled(record.metadata()) {
            REDIRECT_LOGS.with(|messages| messages.borrow_mut().push(record.args().to_string()));
        }
    }
}

fn limit(value: usize) -> OutboundRedirectLimit {
    OutboundRedirectLimit::new(value).expect("valid redirect limit")
}

fn success<E>(
    result: Result<OutboundFollowResult, OutboundFollowFailure<E>>,
) -> OutboundFollowResult {
    match result {
        Ok(result) => result,
        Err(OutboundFollowFailure::Hop { index, error }) => {
            panic!("unexpected hop {index} failure: {error}")
        }
        Err(OutboundFollowFailure::Policy {
            redirect_number, ..
        }) => {
            panic!("unexpected policy rejection at redirect {redirect_number}")
        }
    }
}

fn request(method: Method) -> OutboundRequest {
    OutboundRequest::new(method, Uri::from_static("https://a.example/start")).expect("request")
}

#[expect(
    clippy::unnecessary_wraps,
    reason = "redirect policies require a Result-returning signature"
)]
fn allow_follow(_: &mut OutboundRedirectContext<'_>) -> Result<OutboundRedirectAction, ()> {
    Ok(OutboundRedirectAction::FollowPrepared)
}

fn hop_error<E>(
    result: Result<OutboundFollowResult, OutboundFollowFailure<E>>,
    expected: usize,
) -> EdgeError {
    match result {
        Err(OutboundFollowFailure::Hop { index, error }) => {
            assert_eq!(index, expected);
            error
        }
        Err(OutboundFollowFailure::Policy {
            redirect_number, ..
        }) => {
            panic!("expected hop failure, got policy rejection at redirect {redirect_number}")
        }
        Ok(_) => panic!("expected indexed hop failure"),
    }
}

fn assert_protocol(error: EdgeError) {
    let EdgeError::BadGateway {
        reason, message, ..
    } = error
    else {
        panic!("expected protocol-classified gateway failure");
    };
    assert_eq!(reason, BadGatewayReason::Protocol, "{message}");
}

fn assert_deadline(error: EdgeError) {
    let EdgeError::GatewayTimeout { cause, message, .. } = error else {
        panic!("expected request-deadline timeout");
    };
    assert_eq!(cause, BudgetSource::RequestDeadline, "{message}");
}

fn assert_no_body_headers(headers: &HeaderMap) {
    for name in [
        "content-type",
        "content-length",
        "content-encoding",
        "content-language",
        "content-location",
        "expect",
    ] {
        assert!(!headers.contains_key(name), "dropped body retained {name}");
    }
}

fn content_request(method: Method) -> OutboundRequest {
    let bytes: &'static [u8] = if matches!(method, Method::GET | Method::HEAD) {
        b""
    } else {
        b"12345"
    };
    request(method)
        .body(bytes)
        .header("content-type", "text/plain")
        .expect("header")
        .header("content-language", "en")
        .expect("header")
        .header("content-location", "/original")
        .expect("header")
        .header("content-encoding", "identity")
        .expect("header")
}

fn unread_reply(kind: UnreadBody, probe: &SourceProbe) -> Reply {
    let mut reply = Reply {
        headers: HeaderMap::new(),
        source: probe.source(kind),
        status: StatusCode::FOUND,
    }
    .location("/next");
    match kind {
        UnreadBody::EmptyBrotli => {
            reply
                .headers
                .insert("content-encoding", HeaderValue::from_static("br"));
        }
        UnreadBody::DeclaredHuge => {
            reply
                .headers
                .insert("content-length", HeaderValue::from_static("10485760"));
        }
        UnreadBody::Failed | UnreadBody::Pending => {}
    }
    reply
}

#[test]
fn redirect_chain_uses_supplied_head_client_and_original_deadline() {
    let fixture = Fixture::new([
        Reply::bytes(StatusCode::FOUND, b"discarded")
            .location("/final")
            .step(),
        Reply::bytes(StatusCode::OK, b"final").step(),
    ]);
    let original = Deadline::at_instant(fixture.time.at(50));
    let policy_time = fixture.time.clone();
    let request = OutboundRequest::get("https://a.example/start")
        .expect("request")
        .timeout(Duration::from_millis(30))
        .deadline(original);
    let future = fixture.client.follow(request, limit(4), move |context| {
        assert_eq!(context.redirect_number(), 1);
        assert_eq!(context.status(), StatusCode::FOUND);
        assert_eq!(
            context.request_uri(),
            &Uri::from_static("https://a.example/start")
        );
        assert_eq!(context.resolved_target(), "https://a.example/final");
        policy_time.set(10);
        Ok::<_, ()>(OutboundRedirectAction::FollowPrepared)
    });
    assert_eq!(fixture.sent(), 0, "follow owns its input without eager I/O");
    fixture.time.set(5);
    let result = success(block_on(future));
    assert_eq!(
        result.final_uri(),
        &Uri::from_static("https://a.example/final")
    );
    assert_eq!(result.redirects_followed(), 1);
    assert_eq!(result.stop_reason(), None);
    let state = fixture.state.borrow();
    assert_eq!(state.records.len(), 2);
    assert!(
        state
            .records
            .iter()
            .all(|record| record.mode == OutboundSendMode::Follow)
    );
    assert_eq!(state.records[0].entered_at, fixture.time.start);
    assert_eq!(state.records[1].entered_at, fixture.time.at(10));
    assert_eq!(
        state.records[0].budget.deadline.instant(),
        fixture.time.at(30)
    );
    assert_eq!(
        state.records[1].budget.deadline.instant(),
        fixture.time.at(40)
    );
    assert_eq!(
        state.records[1]
            .parts
            .deadline
            .as_ref()
            .map(Deadline::instant),
        Some(original.instant())
    );
    drop(state);
    assert_eq!(
        block_on(result.into_parts().0.into_bytes_bounded(16)).expect("final body"),
        "final"
    );
}

#[test]
fn redirect_limit_accepts_only_one_through_twenty() {
    for value in [0, 21, usize::MAX] {
        assert!(matches!(
            OutboundRedirectLimit::new(value),
            Err(EdgeError::BadRequest { .. })
        ));
    }
    for value in 1..=20 {
        assert_eq!(limit(value).get(), value);
    }
}

#[test]
fn every_status_and_portable_method_obeys_the_redirect_table() {
    for status in 300..=308 {
        for method in [
            Method::GET,
            Method::HEAD,
            Method::POST,
            Method::PUT,
            Method::PATCH,
            Method::DELETE,
            Method::OPTIONS,
        ] {
            let fixture = Fixture::new([
                Reply::bytes(StatusCode::from_u16(status).expect("status"), b"")
                    .location("/next")
                    .step(),
                Reply::bytes(StatusCode::OK, b"").step(),
            ]);
            let calls = Rc::new(Cell::new(0));
            let observed = Rc::clone(&calls);
            let result = success(block_on(fixture.client.follow(
                content_request(method.clone()),
                limit(4),
                move |_| {
                    observed.set(observed.get() + 1);
                    Ok::<_, ()>(OutboundRedirectAction::FollowPrepared)
                },
            )));
            let next = expected_redirect_method(status, &method);
            let state = fixture.state.borrow();
            assert_eq!(
                state.records.len(),
                1 + usize::from(next.is_some()),
                "{status} {method}"
            );
            assert_eq!(calls.get(), usize::from(next.is_some()));
            if let Some(next_method) = next {
                assert_eq!(state.records[1].parts.method, next_method);
                assert_redirect_body(&state.records[1], status, &method);
                assert_eq!(result.response().status(), StatusCode::OK);
                assert_eq!(result.redirects_followed(), 1);
                assert_eq!(result.stop_reason(), None);
            } else if matches!(status, 301 | 302) {
                assert_eq!(
                    result.stop_reason(),
                    Some(OutboundRedirectStopReason::MethodNotRewritable)
                );
            } else {
                assert_eq!(result.stop_reason(), None);
            }
        }
    }
}

fn expected_redirect_method(status: u16, method: &Method) -> Option<Method> {
    match status {
        301 | 302 if *method == Method::POST => Some(Method::GET),
        301 | 302 if matches!(*method, Method::GET | Method::HEAD) => Some(method.clone()),
        303 if *method == Method::HEAD => Some(Method::HEAD),
        303 => Some(Method::GET),
        307 | 308 => Some(method.clone()),
        _ => None,
    }
}

fn assert_redirect_body(record: &Recorded, status: u16, method: &Method) {
    let replayed = matches!(status, 307 | 308);
    let expected = if replayed && !matches!(*method, Method::GET | Method::HEAD) {
        &b"12345"[..]
    } else {
        &b""[..]
    };
    assert_eq!(record.body.as_deref(), Some(expected));
    if replayed {
        assert_eq!(record.parts.headers["content-type"], "text/plain");
        assert_eq!(record.parts.headers["content-language"], "en");
        assert_eq!(record.parts.headers["content-encoding"], "identity");
        assert_eq!(record.parts.headers["content-location"], "/original");
    } else {
        assert_no_body_headers(&record.parts.headers);
    }
}

#[test]
fn rewritten_post_drops_content_metadata_and_expectation_for_later_replay() {
    let fixture = Fixture::new([
        Reply::bytes(StatusCode::FOUND, b"")
            .location("/second")
            .step(),
        Reply::bytes(StatusCode::TEMPORARY_REDIRECT, b"")
            .location("/third")
            .step(),
        Reply::bytes(StatusCode::OK, b"").step(),
    ]);
    let initial = content_request(Method::POST)
        .header("expect", "100-continue")
        .expect("expectation");
    success(block_on(fixture.client.follow(
        initial,
        limit(4),
        allow_follow,
    )));
    let state = fixture.state.borrow();
    assert_eq!(state.records.len(), 3);
    assert_eq!(state.records[0].parts.method, Method::POST);
    assert_eq!(state.records[0].body.as_deref(), Some(&b"12345"[..]));
    assert_eq!(state.records[0].parts.headers["expect"], "100-continue");
    for record in &state.records[1..] {
        assert_eq!(record.parts.method, Method::GET);
        assert_eq!(record.body.as_deref(), Some(&b""[..]));
        assert_no_body_headers(&record.parts.headers);
    }
}

#[test]
fn replaying_empty_post_preserves_content_type() {
    let fixture = Fixture::new([
        Reply::bytes(StatusCode::TEMPORARY_REDIRECT, b"")
            .location("/next")
            .step(),
        Reply::bytes(StatusCode::OK, b"").step(),
    ]);
    let initial = request(Method::POST)
        .header("content-type", "text/plain")
        .expect("header");
    success(block_on(fixture.client.follow(
        initial,
        limit(4),
        allow_follow,
    )));
    let state = fixture.state.borrow();
    for record in &state.records {
        assert_eq!(record.parts.method, Method::POST);
        assert_eq!(record.body.as_deref(), Some(&b""[..]));
        assert_eq!(record.parts.headers["content-type"], "text/plain");
    }
    assert_eq!(state.records.len(), 2);
}

#[test]
fn buffered_redirect_replay_shares_the_original_nonempty_allocation() {
    let fixture = Fixture::new([
        Reply::bytes(StatusCode::TEMPORARY_REDIRECT, b"")
            .location("/next")
            .step(),
        Reply::bytes(StatusCode::OK, b"").step(),
    ]);
    let original = Bytes::from(vec![42_u8; 1024]);
    let allocation = original.as_ptr();
    let initial = request(Method::POST).body(original.clone());
    success(block_on(fixture.client.follow(
        initial,
        limit(4),
        allow_follow,
    )));
    let state = fixture.state.borrow();
    assert_eq!(state.records.len(), 2);
    for record in &state.records {
        let bytes = record.parts.body.as_bytes().expect("buffered replay");
        assert_eq!(bytes.len(), 1024);
        assert_eq!(
            bytes.as_ptr(),
            allocation,
            "replay must clone Bytes, not copy its slice"
        );
        assert_eq!(bytes, original.as_ref());
    }
}

#[test]
fn streamed_upload_cannot_replay_and_never_consults_policy() {
    for status in [
        StatusCode::TEMPORARY_REDIRECT,
        StatusCode::PERMANENT_REDIRECT,
    ] {
        let fixture = Fixture::new([Reply::bytes(status, b"").location("/next").step()]);
        let body = Body::from_stream(stream::once(async { Ok(Bytes::from_static(b"upload")) }));
        let result = success(block_on(fixture.client.follow(
            request(Method::POST).body(body),
            limit(4),
            |_| -> Result<OutboundRedirectAction, ()> {
                panic!("non-replayable stream must stop before policy")
            },
        )));
        assert_eq!(fixture.sent(), 1);
        assert_eq!(
            result.stop_reason(),
            Some(OutboundRedirectStopReason::BodyNotReplayable)
        );
    }
}

#[test]
fn inbound_empty_get_stream_is_settled_once_before_all_replayed_hops() {
    let fixture = Fixture::new([
        Reply::bytes(StatusCode::TEMPORARY_REDIRECT, b"")
            .location("/next")
            .step(),
        Reply::bytes(StatusCode::PERMANENT_REDIRECT, b"")
            .location("/last")
            .step(),
        Reply::bytes(StatusCode::OK, b"").step(),
    ]);
    let polls = Rc::new(Cell::new(0_usize));
    let counted = Rc::clone(&polls);
    let body = Body::from_stream(stream::poll_fn(move |_| {
        counted.set(counted.get() + 1);
        Poll::Ready(None)
    }));
    let result = success(block_on(fixture.client.follow(
        request(Method::GET).body(body),
        limit(4),
        allow_follow,
    )));
    assert_eq!(polls.get(), 1);
    assert_eq!(result.redirects_followed(), 2);
    assert_eq!(result.stop_reason(), None);
    let state = fixture.state.borrow();
    assert_eq!(state.records.len(), 3);
    assert!(
        state
            .records
            .iter()
            .all(|record| record.body.as_deref() == Some(&b""[..]))
    );
}

#[test]
fn followed_bodies_are_dropped_without_polling_or_finalization() {
    for kind in [
        UnreadBody::Failed,
        UnreadBody::EmptyBrotli,
        UnreadBody::DeclaredHuge,
        UnreadBody::Pending,
    ] {
        let probe = SourceProbe::default();
        let fixture = Fixture::new([
            unread_reply(kind, &probe).step(),
            Reply::bytes(StatusCode::OK, b"final").step(),
        ]);
        let policy_probe = probe.clone();
        let result = success(block_on(fixture.client.follow(
            request(Method::GET),
            limit(4),
            move |_| {
                assert_eq!(
                    policy_probe.polls.get(),
                    0,
                    "policy must precede payload processing"
                );
                Ok::<_, ()>(OutboundRedirectAction::FollowPrepared)
            },
        )));
        assert_eq!(fixture.sent(), 2);
        assert_eq!(probe.polls.get(), 0);
        assert_eq!(probe.drops.get(), 1);
        assert_eq!(
            block_on(result.into_parts().0.into_bytes_bounded(16)).expect("final body"),
            "final"
        );
    }
}

#[test]
fn stopped_and_limit_reached_heads_keep_ordinary_final_body_errors() {
    for by_limit in [false, true] {
        for kind in [
            UnreadBody::Failed,
            UnreadBody::EmptyBrotli,
            UnreadBody::DeclaredHuge,
        ] {
            let probe = SourceProbe::default();
            let mut steps = Vec::new();
            if by_limit {
                steps.push(
                    Reply::bytes(StatusCode::FOUND, b"")
                        .location("/first")
                        .step(),
                );
            }
            steps.push(unread_reply(kind, &probe).step());
            let fixture = Fixture::new(steps);
            let error = hop_error(
                block_on(
                    fixture
                        .client
                        .follow(request(Method::GET), limit(1), move |_| {
                            Ok::<_, ()>(if by_limit {
                                OutboundRedirectAction::FollowPrepared
                            } else {
                                OutboundRedirectAction::Stop
                            })
                        }),
                ),
                usize::from(by_limit),
            );
            match kind {
                UnreadBody::Failed => assert!(matches!(
                    error,
                    EdgeError::BadGateway {
                        reason: BadGatewayReason::Unreachable,
                        ..
                    }
                )),
                UnreadBody::EmptyBrotli => assert!(matches!(
                    error,
                    EdgeError::BadGateway {
                        reason: BadGatewayReason::Decode(BadGatewayDecodeReason::Brotli),
                        ..
                    }
                )),
                UnreadBody::DeclaredHuge => {
                    assert!(matches!(error, EdgeError::ResponseTooLarge { .. }));
                }
                UnreadBody::Pending => panic!("pending final bodies are tested separately"),
            }
            assert_eq!(fixture.sent(), 1 + usize::from(by_limit));
            assert_eq!(probe.drops.get(), 1);
            if matches!(kind, UnreadBody::DeclaredHuge) {
                assert_eq!(probe.polls.get(), 0);
            }
        }
    }
}

#[test]
fn stopped_pending_final_body_waits_only_until_the_original_deadline() {
    let probe = SourceProbe::default();
    let fixture = Fixture::new([unread_reply(UnreadBody::Pending, &probe).step()]);
    let initial = request(Method::GET).deadline(Deadline::at_instant(fixture.time.at(20)));
    let mut future = fixture.client.follow(initial, limit(4), |_| {
        Ok::<_, ()>(OutboundRedirectAction::Stop)
    });
    assert!(
        future
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    assert!(probe.polls.get() > 0);
    assert_eq!(probe.drops.get(), 0);
    fixture.time.set(20);
    assert_deadline(hop_error(block_on(future), 0));
    assert_eq!(probe.drops.get(), 1);
    assert_eq!(fixture.sent(), 1);
}

#[test]
fn original_chain_deadline_does_not_replace_a_fresh_requests_default_timeout() {
    let fixture = Fixture::new([
        Reply::bytes(StatusCode::FOUND, b"")
            .location("/next")
            .step(),
        Reply::bytes(StatusCode::OK, b"").step(),
    ]);
    let clock = fixture.time.clone();
    let initial = request(Method::GET).deadline(Deadline::at_instant(fixture.time.at(60_000)));
    success(block_on(fixture.client.follow(
        initial,
        limit(4),
        move |_| {
            clock.set(1_000);
            Ok::<_, ()>(OutboundRedirectAction::Follow(request(Method::GET)))
        },
    )));
    let state = fixture.state.borrow();
    assert_eq!(state.records.len(), 2);
    assert_eq!(state.records[0].budget.cause, BudgetSource::RequestDeadline);
    assert_eq!(
        state.records[0].budget.deadline.instant(),
        fixture.time.at(60_000)
    );
    assert_eq!(state.records[1].budget.cause, BudgetSource::Default);
    assert_eq!(
        state.records[1].budget.deadline.instant(),
        fixture.time.at(31_000)
    );
    assert_eq!(state.records[1].parts.timeout, None);
    assert!(state.records[1].parts.deadline.is_none());
}

#[test]
fn limit_four_sends_five_requests_and_skips_a_fifth_policy_call() {
    let fixture = Fixture::new(
        repeat_with(|| {
            Reply::bytes(StatusCode::FOUND, b"last")
                .location("/next")
                .step()
        })
        .take(5),
    );
    let calls = Rc::new(Cell::new(0));
    let observed = Rc::clone(&calls);
    let result = success(block_on(fixture.client.follow(
        request(Method::GET),
        limit(4),
        move |context| {
            observed.set(observed.get() + 1);
            assert_eq!(context.redirect_number(), observed.get());
            Ok::<_, ()>(OutboundRedirectAction::FollowPrepared)
        },
    )));
    assert_eq!(fixture.sent(), 5);
    assert_eq!(calls.get(), 4);
    assert_eq!(result.response().status(), StatusCode::FOUND);
    assert_eq!(result.redirects_followed(), 4);
    assert_eq!(
        result.stop_reason(),
        Some(OutboundRedirectStopReason::LimitReached)
    );
    assert_eq!(
        block_on(result.into_parts().0.into_bytes_bounded(16)).expect("final body"),
        "last"
    );
}

#[test]
fn location_presence_and_method_rules_precede_limit_and_location_resolution() {
    for (target, expected) in [
        (None, OutboundRedirectStopReason::NoLocation),
        (Some(""), OutboundRedirectStopReason::NoLocation),
        (
            Some("http://["),
            OutboundRedirectStopReason::MethodNotRewritable,
        ),
    ] {
        let mut last = Reply::bytes(StatusCode::MOVED_PERMANENTLY, b"");
        if let Some(target) = target {
            last = last.location(target);
        }
        let fixture = Fixture::new([
            Reply::bytes(StatusCode::TEMPORARY_REDIRECT, b"")
                .location("/second")
                .step(),
            last.step(),
        ]);
        let result = success(block_on(fixture.client.follow(
            content_request(Method::PUT),
            limit(1),
            |context| {
                assert_eq!(
                    context.redirect_number(),
                    1,
                    "last head must stop before policy"
                );
                Ok::<_, ()>(OutboundRedirectAction::FollowPrepared)
            },
        )));
        assert_eq!(fixture.sent(), 2);
        assert_eq!(result.stop_reason(), Some(expected));
    }
}

#[test]
fn replay_rule_precedes_limit_and_invalid_location() {
    let fixture = Fixture::new([
        Reply::bytes(StatusCode::TEMPORARY_REDIRECT, b"")
            .location("/second")
            .step(),
        Reply::bytes(StatusCode::PERMANENT_REDIRECT, b"")
            .location("http://[")
            .step(),
    ]);
    let result = success(block_on(fixture.client.follow(
        content_request(Method::POST),
        limit(1),
        |context| {
            assert_eq!(context.redirect_number(), 1);
            let replacement = request(Method::POST).body(Body::from_stream(stream::empty()));
            Ok::<_, ()>(OutboundRedirectAction::Follow(replacement))
        },
    )));
    assert_eq!(fixture.sent(), 2);
    assert_eq!(
        result.stop_reason(),
        Some(OutboundRedirectStopReason::BodyNotReplayable)
    );
}

#[test]
fn limit_precedes_invalid_location_and_policy() {
    let fixture = Fixture::new([
        Reply::bytes(StatusCode::FOUND, b"")
            .location("/second")
            .step(),
        Reply::bytes(StatusCode::FOUND, b"")
            .location("http://[")
            .step(),
    ]);
    let result = success(block_on(fixture.client.follow(
        request(Method::GET),
        limit(1),
        |context| {
            assert_eq!(context.redirect_number(), 1);
            Ok::<_, ()>(OutboundRedirectAction::FollowPrepared)
        },
    )));
    assert_eq!(fixture.sent(), 2);
    assert_eq!(
        result.stop_reason(),
        Some(OutboundRedirectStopReason::LimitReached)
    );
}

#[test]
fn relative_and_absolute_locations_resolve_without_sending_fragments() {
    for (location, target) in [
        ("child", "https://a.example/dir/child"),
        ("../x", "https://a.example/x"),
        ("//b.example/x", "https://b.example/x"),
        ("http://b.example/x#private", "http://b.example/x"),
        ("/next?q=1#private", "https://a.example/next?q=1"),
        ("#private", "https://a.example/dir/start"),
    ] {
        let fixture = Fixture::new([
            Reply::bytes(StatusCode::FOUND, b"")
                .location(location)
                .step(),
            Reply::bytes(StatusCode::OK, b"").step(),
        ]);
        let initial = OutboundRequest::get("https://a.example/dir/start").expect("request");
        let result = success(block_on(fixture.client.follow(
            initial,
            limit(4),
            move |context| {
                assert_eq!(context.resolved_target(), target);
                Ok::<_, ()>(OutboundRedirectAction::FollowPrepared)
            },
        )));
        assert_eq!(
            fixture.state.borrow().records[1].parts.uri.to_string(),
            target
        );
        assert_eq!(result.final_uri().to_string(), target);
    }
}

#[test]
fn unusable_locations_fail_at_the_answering_hop_before_policy() {
    for target in [
        "http://[",
        "http://user:secret@b.example/x",
        "https://",
        "http://b.example:99999/x",
    ] {
        let fixture = Fixture::new([Reply::bytes(StatusCode::FOUND, b"").location(target).step()]);
        let outcome = block_on(fixture.client.follow(
            request(Method::GET),
            limit(4),
            |_| -> Result<OutboundRedirectAction, ()> {
                panic!("invalid target must fail before policy")
            },
        ));
        assert_protocol(hop_error(outcome, 0));
        assert_eq!(fixture.sent(), 1);
    }
}

fn invalid_location_reply(duplicate: bool) -> Reply {
    let mut reply = Reply::bytes(StatusCode::FOUND, b"");
    if duplicate {
        reply = reply.location("/next");
    }
    reply.headers.append(
        "location",
        HeaderValue::from_bytes(b"\xff").expect("non-text header value"),
    );
    reply
}

#[test]
fn original_nontext_and_duplicate_location_evidence_survives_normalization() {
    for duplicate in [false, true] {
        let fixture = Fixture::new([invalid_location_reply(duplicate).step()]);
        let outcome = block_on(fixture.client.follow(
            request(Method::GET),
            limit(4),
            |_| -> Result<OutboundRedirectAction, ()> {
                panic!("original invalid Location must not become an eligible singleton")
            },
        ));
        assert_protocol(hop_error(outcome, 0));
        assert_eq!(fixture.sent(), 1);
    }
    let mut duplicate = Reply::bytes(StatusCode::FOUND, b"").location("/one");
    duplicate
        .headers
        .append("location", HeaderValue::from_static("/two"));
    let fixture = Fixture::new([duplicate.step()]);
    assert_protocol(hop_error(
        block_on(
            fixture
                .client
                .follow(request(Method::GET), limit(4), allow_follow),
        ),
        0,
    ));
}

#[test]
fn earlier_stop_rules_do_not_bypass_normal_header_filtering() {
    for duplicate in [false, true] {
        let mut reply = invalid_location_reply(duplicate);
        reply.status = StatusCode::MOVED_PERMANENTLY;
        let fixture = Fixture::new([reply.step()]);
        let result = success(block_on(fixture.client.follow(
            content_request(Method::PUT),
            limit(4),
            |_| -> Result<OutboundRedirectAction, ()> {
                panic!("method rule precedes original Location evidence validation")
            },
        )));
        assert_eq!(
            result.stop_reason(),
            Some(OutboundRedirectStopReason::MethodNotRewritable)
        );
        assert_eq!(
            result
                .response()
                .headers()
                .get_all("location")
                .iter()
                .count(),
            usize::from(duplicate)
        );
        assert!(
            result
                .response()
                .headers()
                .get_all("location")
                .iter()
                .all(|value| value.to_str().is_ok())
        );
    }
    let fixture = Fixture::new([
        Reply::bytes(StatusCode::FOUND, b"")
            .location("/first")
            .step(),
        invalid_location_reply(true).step(),
    ]);
    let result = success(block_on(fixture.client.follow(
        request(Method::GET),
        limit(1),
        allow_follow,
    )));
    assert_eq!(
        result.stop_reason(),
        Some(OutboundRedirectStopReason::LimitReached)
    );
    assert_eq!(
        result
            .response()
            .headers()
            .get_all("location")
            .iter()
            .count(),
        1
    );
}

#[test]
fn nonredirect_status_precedes_all_location_rules() {
    let mut reply = invalid_location_reply(true);
    reply.status = StatusCode::OK;
    let fixture = Fixture::new([reply.step()]);
    let result = success(block_on(fixture.client.follow(
        request(Method::GET),
        limit(4),
        |_| -> Result<OutboundRedirectAction, ()> {
            panic!("nonredirect head never invokes policy")
        },
    )));
    assert_eq!(result.response().status(), StatusCode::OK);
    assert_eq!(result.stop_reason(), None);
    assert_eq!(result.redirects_followed(), 0);
}

#[test]
fn non_http_target_can_be_stopped_rejected_or_replaced_but_not_followed_prepared() {
    for follow_prepared in [false, true] {
        let fixture = Fixture::new([Reply::bytes(StatusCode::FOUND, b"")
            .location("javascript:alert(1)")
            .step()]);
        let outcome = block_on(fixture.client.follow(
            request(Method::GET),
            limit(4),
            move |context| {
                assert_eq!(context.resolved_target(), "javascript:alert(1)");
                assert!(context.prepared_request().is_none());
                Ok::<_, ()>(if follow_prepared {
                    OutboundRedirectAction::FollowPrepared
                } else {
                    OutboundRedirectAction::Stop
                })
            },
        ));
        if follow_prepared {
            assert_protocol(hop_error(outcome, 0));
        } else {
            assert_eq!(
                success(outcome).stop_reason(),
                Some(OutboundRedirectStopReason::StoppedByPolicy)
            );
        }
        assert_eq!(fixture.sent(), 1);
    }
    let fixture = Fixture::new([
        Reply::bytes(StatusCode::FOUND, b"")
            .location("javascript:alert(1)")
            .step(),
        Reply::bytes(StatusCode::OK, b"").step(),
    ]);
    success(block_on(fixture.client.follow(
        request(Method::GET),
        limit(4),
        |context| {
            assert!(context.prepared_request().is_none());
            Ok::<_, ()>(OutboundRedirectAction::Follow(request(Method::GET)))
        },
    )));
    assert_eq!(fixture.sent(), 2);
}

#[test]
fn taking_prepared_request_then_following_prepared_is_a_protocol_failure() {
    let fixture = Fixture::new([Reply::bytes(StatusCode::FOUND, b"")
        .location("/next")
        .step()]);
    let outcome = block_on(
        fixture
            .client
            .follow(request(Method::GET), limit(4), |context| {
                assert!(context.take_prepared_request().is_some());
                assert!(
                    context.take_prepared_request().is_none(),
                    "request transfers only once"
                );
                assert!(context.prepared_request().is_none());
                Ok::<_, ()>(OutboundRedirectAction::FollowPrepared)
            }),
    );
    assert_protocol(hop_error(outcome, 0));
    assert_eq!(fixture.sent(), 1);
}

#[test]
fn policy_observes_normalized_head_and_next_request_before_any_next_dispatch() {
    let mut reply = Reply::bytes(StatusCode::SEE_OTHER, b"").location("/next");
    reply
        .headers
        .insert("x-answer", HeaderValue::from_static("visible"));
    let fixture = Fixture::new([reply.step(), Reply::bytes(StatusCode::OK, b"").step()]);
    let recorded = Rc::clone(&fixture.state);
    success(block_on(fixture.client.follow(
        content_request(Method::POST),
        limit(4),
        move |context| {
            assert_eq!(recorded.borrow().records.len(), 1);
            assert_eq!(context.redirect_number(), 1);
            assert_eq!(context.status(), StatusCode::SEE_OTHER);
            assert_eq!(
                context.request_uri(),
                &Uri::from_static("https://a.example/start")
            );
            assert_eq!(context.resolved_target(), "https://a.example/next");
            assert_eq!(context.headers()["x-answer"], "visible");
            assert!(!context.is_cross_origin());
            let prepared = context.prepared_request().expect("prepared next request");
            assert_eq!(prepared.method(), Method::GET);
            assert_no_body_headers(prepared.headers());
            Ok::<_, ()>(OutboundRedirectAction::FollowPrepared)
        },
    )));
    assert_eq!(fixture.sent(), 2);
    assert!(
        fixture
            .state
            .borrow()
            .validations
            .iter()
            .all(|(_, mode)| *mode == OutboundSendMode::Follow)
    );
}

fn credential_request() -> OutboundRequest {
    request(Method::GET)
        .header("authorization", "original credential")
        .expect("header")
        .header("cookie", "session=original")
        .expect("header")
        .header("x-api-key", "application-managed")
        .expect("header")
        .header("x-amz-date", "20261027T000000Z")
        .expect("header")
        .host_authority_override("virtual.example")
        .expect("authority")
}

#[test]
fn origin_changes_strip_only_automatic_credentials_before_policy() {
    for (target, cross_origin) in [
        ("https://a.example/next", false),
        ("https://A.EXAMPLE:443/next", false),
        ("https://b.example/next", true),
        ("https://a.example:444/next", true),
        ("http://a.example/next", true),
    ] {
        let fixture = Fixture::new([
            Reply::bytes(StatusCode::FOUND, b"").location(target).step(),
            Reply::bytes(StatusCode::OK, b"").step(),
        ]);
        success(block_on(fixture.client.follow(
            credential_request(),
            limit(4),
            move |context| {
                assert_eq!(context.is_cross_origin(), cross_origin);
                let prepared = context.prepared_request().expect("HTTP target");
                assert_eq!(
                    prepared.headers().contains_key("authorization"),
                    !cross_origin
                );
                assert_eq!(prepared.headers().contains_key("cookie"), !cross_origin);
                assert_eq!(prepared.has_host_authority_override(), !cross_origin);
                assert_eq!(prepared.headers()["x-api-key"], "application-managed");
                assert_eq!(prepared.headers()["x-amz-date"], "20261027T000000Z");
                Ok::<_, ()>(OutboundRedirectAction::FollowPrepared)
            },
        )));
        let state = fixture.state.borrow();
        assert_eq!(state.records.len(), 2);
        let next = &state.records[1].parts;
        assert_eq!(next.headers.contains_key("authorization"), !cross_origin);
        assert_eq!(next.headers.contains_key("cookie"), !cross_origin);
        assert_eq!(next.host_authority_override.is_some(), !cross_origin);
        assert_eq!(next.headers["x-api-key"], "application-managed");
        assert_eq!(next.headers["x-amz-date"], "20261027T000000Z");
        assert!(!next.headers.contains_key("referer"));
        assert!(!next.headers.contains_key("origin"));
    }
}

#[test]
fn policy_authorized_cross_origin_credentials_are_not_stripped_again() {
    let fixture = Fixture::new([
        Reply::bytes(StatusCode::FOUND, b"")
            .location("https://b.example/next")
            .step(),
        Reply::bytes(StatusCode::OK, b"").step(),
    ]);
    success(block_on(fixture.client.follow(
        credential_request(),
        limit(4),
        |context| {
            let prepared = context.take_prepared_request().expect("prepared request");
            assert!(!prepared.headers().contains_key("authorization"));
            let authorized = prepared
                .header("authorization", "new signature")
                .expect("header")
                .header("cookie", "session=new")
                .expect("header")
                .host_authority_override("signed.example")
                .expect("authority");
            Ok::<_, ()>(OutboundRedirectAction::Follow(authorized))
        },
    )));
    let state = fixture.state.borrow();
    let sent = &state.records[1].parts;
    assert_eq!(sent.headers["authorization"], "new signature");
    assert_eq!(sent.headers["cookie"], "session=new");
    assert_eq!(
        sent.host_authority_override
            .as_ref()
            .expect("authority")
            .as_str(),
        "signed.example"
    );
}

fn configured_request(time: &ManualTime) -> OutboundRequest {
    request(Method::GET)
        .timeout(Duration::from_millis(120))
        .deadline(Deadline::at_instant(time.at(200)))
        .connect_timeout(Duration::from_millis(30))
        .cache_policy(OutboundCachePolicy::Bypass)
        .proxy_marker(OutboundProxyMarker::Off)
        .response_decoding(ResponseDecoding::Original)
        .max_request_body_bytes(11)
        .max_response_bytes(13)
        .max_encoded_response_bytes(17)
        .max_decoded_response_bytes(19)
        .max_decoder_bytes(0x0001_0000)
        .max_brotli_window_bits(20)
        .max_chunk_bytes(NonZeroU64::new(3).expect("chunk"))
        .max_response_header_bytes(4_096)
        .max_response_header_count(16)
}

fn assert_same_options(actual: &OutboundRequestParts, expected: &OutboundRequestParts) {
    assert_eq!(actual.timeout, expected.timeout);
    assert_eq!(
        actual.deadline.as_ref().map(Deadline::instant),
        expected.deadline.as_ref().map(Deadline::instant)
    );
    assert_eq!(actual.connect_timeout, expected.connect_timeout);
    assert_eq!(actual.cache_policy, expected.cache_policy);
    assert_eq!(actual.proxy_marker, expected.proxy_marker);
    assert_eq!(actual.response_decoding, expected.response_decoding);
    assert_eq!(actual.response_mode, expected.response_mode);
    assert_eq!(
        actual.max_request_body_bytes,
        expected.max_request_body_bytes
    );
    assert_eq!(
        actual.max_response_header_bytes,
        expected.max_response_header_bytes
    );
    assert_eq!(
        actual.max_response_header_count,
        expected.max_response_header_count
    );
    assert_eq!(
        actual.max_encoded_response_bytes,
        expected.max_encoded_response_bytes
    );
    assert_eq!(
        actual.max_decoded_response_bytes,
        expected.max_decoded_response_bytes
    );
    assert_eq!(actual.max_decoder_bytes, expected.max_decoder_bytes);
    assert_eq!(
        actual.max_brotli_window_bits,
        expected.max_brotli_window_bits
    );
    assert_eq!(actual.max_chunk_bytes, expected.max_chunk_bytes);
    assert_eq!(actual.image_optimizer, expected.image_optimizer);
}

#[test]
fn prepared_requests_retain_all_unchanged_caps_and_options() {
    let fixture = Fixture::new([
        Reply::bytes(StatusCode::FOUND, b"")
            .location("/next")
            .step(),
        Reply::bytes(StatusCode::OK, b"").step(),
    ]);
    let expected = configured_request(&fixture.time).into_parts();
    success(block_on(fixture.client.follow(
        configured_request(&fixture.time),
        limit(4),
        |context| {
            let prepared = context.take_prepared_request().expect("prepared request");
            let modified = prepared.header("x-policy", "modified").expect("header");
            Ok::<_, ()>(OutboundRedirectAction::Follow(modified))
        },
    )));
    let state = fixture.state.borrow();
    assert_same_options(&state.records[0].parts, &expected);
    assert_same_options(&state.records[1].parts, &expected);
    assert_eq!(state.records[1].parts.headers["x-policy"], "modified");
}

#[test]
fn connect_timeout_round_trip_and_follow_preserve_the_aggregate_budget() {
    for following in [false, true] {
        let mut steps = vec![Reply::bytes(StatusCode::OK, b"").step()];
        if following {
            steps.insert(
                0,
                Reply::bytes(StatusCode::FOUND, b"")
                    .location("/next")
                    .step(),
            );
        }
        let fixture = Fixture::new(steps);
        let deadline = Deadline::at_instant(fixture.time.at(20000));
        let original = request(Method::GET)
            .connect_timeout(Duration::from_secs(1))
            .timeout(Duration::from_secs(15))
            .deadline(deadline);
        let parts = original.into_parts();
        assert_eq!(parts.connect_timeout, Some(Duration::from_secs(1)));
        let rebuilt = OutboundRequest::from_parts(parts).expect("round trip");
        assert_eq!(
            rebuilt.connect_timeout_setting(),
            Some(Duration::from_secs(1))
        );
        if following {
            let clock = fixture.time.clone();
            success(block_on(fixture.client.follow(
                rebuilt,
                limit(2),
                move |context| {
                    assert_eq!(
                        context
                            .prepared_request()
                            .expect("prepared hop")
                            .connect_timeout_setting(),
                        Some(Duration::from_secs(1))
                    );
                    clock.set(500);
                    Ok::<_, ()>(OutboundRedirectAction::FollowPrepared)
                },
            )));
        } else {
            block_on(fixture.client.send(rebuilt)).expect("plain send");
        }
        let recording = fixture.state.borrow();
        assert_eq!(recording.records.len(), if following { 2 } else { 1 });
        for (record, entered_ms) in recording.records.iter().zip([0, 500]) {
            assert_eq!(
                record.mode,
                if following {
                    OutboundSendMode::Follow
                } else {
                    OutboundSendMode::Send
                }
            );
            assert_eq!(record.entered_at, fixture.time.at(entered_ms));
            assert_eq!(record.parts.connect_timeout, Some(Duration::from_secs(1)));
            assert_eq!(record.parts.timeout, Some(Duration::from_secs(15)));
            assert_eq!(
                record.parts.deadline.expect("original deadline").instant(),
                deadline.instant()
            );
            assert_eq!(record.budget.duration, Duration::from_secs(15));
            assert_eq!(record.budget.cause, BudgetSource::PerCallTimeout);
            assert_eq!(
                record.budget.deadline.instant(),
                record.entered_at + Duration::from_secs(15)
            );
        }
    }
}

#[test]
fn image_optimizer_options_survive_every_prepared_follow_hop() {
    let fixture = Fixture::new([
        Reply::bytes(StatusCode::FOUND, b"")
            .location("/next")
            .step(),
        Reply::bytes(StatusCode::OK, b"").step(),
    ]);
    let options = OutboundImageOptimizerOptions::new()
        .region(OutboundImageOptimizerRegion::Asia)
        .width(320)
        .expect("width")
        .preserve_origin_query(true);
    let expected = options.clone();
    let result = fixture.client.follow(
        request(Method::GET).image_optimizer(options),
        limit(4),
        move |context| {
            assert_eq!(
                context
                    .prepared_request()
                    .expect("prepared request")
                    .image_optimizer_options(),
                Some(&expected)
            );
            Ok::<_, ()>(OutboundRedirectAction::FollowPrepared)
        },
    );
    success(block_on(result));
    let state = fixture.state.borrow();
    assert_eq!(state.records[0].mode, OutboundSendMode::Follow);
    assert_eq!(state.records[1].mode, OutboundSendMode::Follow);
    assert_eq!(
        state.records[0].parts.image_optimizer,
        state.records[1].parts.image_optimizer
    );
}

#[test]
fn freshly_built_policy_request_uses_its_own_default_caps_and_options() {
    let fixture = Fixture::new([
        Reply::bytes(StatusCode::FOUND, b"")
            .location("/next")
            .step(),
        Reply::bytes(StatusCode::OK, b"larger than old cap").step(),
    ]);
    let initial = configured_request(&fixture.time).max_response_bytes(1);
    let expected = request(Method::GET)
        .proxy_marker(OutboundProxyMarker::On)
        .into_parts();
    let result = success(block_on(fixture.client.follow(initial, limit(4), |_| {
        Ok::<_, ()>(OutboundRedirectAction::Follow(request(Method::GET)))
    })));
    assert_same_options(&fixture.state.borrow().records[1].parts, &expected);
    assert_eq!(
        block_on(result.into_parts().0.into_bytes_bounded(64)).expect("fresh default cap"),
        "larger than old cap"
    );
}

#[test]
fn later_prepared_hops_inherit_policy_overrides_not_the_original_options() {
    let fixture = Fixture::new([
        Reply::bytes(StatusCode::FOUND, b"")
            .location("/second")
            .step(),
        Reply::bytes(StatusCode::FOUND, b"")
            .location("/third")
            .step(),
        Reply::bytes(StatusCode::OK, b"data").step(),
    ]);
    success(block_on(fixture.client.follow(
        request(Method::GET).max_response_bytes(1),
        limit(4),
        |context| {
            let prepared = context.take_prepared_request().expect("prepared request");
            if context.redirect_number() == 1 {
                Ok::<_, ()>(OutboundRedirectAction::Follow(
                    prepared
                        .max_response_bytes(8)
                        .timeout(Duration::from_millis(40)),
                ))
            } else {
                let parts = prepared.into_parts();
                assert_eq!(parts.response_mode, ResponseMode::Buffered { max_bytes: 8 });
                assert_eq!(parts.timeout, Some(Duration::from_millis(40)));
                Ok(OutboundRedirectAction::Follow(
                    OutboundRequest::from_parts(parts).expect("request"),
                ))
            }
        },
    )));
    let state = fixture.state.borrow();
    for record in &state.records[1..] {
        assert_eq!(
            record.parts.response_mode,
            ResponseMode::Buffered { max_bytes: 8 }
        );
        assert_eq!(record.parts.timeout, Some(Duration::from_millis(40)));
    }
}

#[test]
fn policy_owns_local_state_and_returns_nonclone_nonsend_nondebug_error_unchanged() {
    for target in ["/next", "javascript:alert(1)"] {
        let fixture = Fixture::new([Reply::bytes(StatusCode::FOUND, b"").location(target).step()]);
        let identity = Rc::new(Cell::new(7));
        let local = Rc::clone(&identity);
        let outcome = block_on(
            fixture
                .client
                .follow(request(Method::GET), limit(4), move |_| {
                    local.set(local.get() + 1);
                    Err::<OutboundRedirectAction, _>(PolicyRejection {
                        identity: Rc::clone(&local),
                    })
                }),
        );
        match outcome {
            Err(OutboundFollowFailure::Policy {
                redirect_number,
                error,
            }) => {
                assert_eq!(redirect_number, 1);
                assert!(Rc::ptr_eq(&identity, &error.identity));
                assert_eq!(identity.get(), 8);
            }
            _ => panic!("application policy value must be returned unchanged"),
        }
        assert_eq!(fixture.sent(), 1);
    }
}

#[test]
fn policy_stop_returns_the_answering_head_as_final() {
    let fixture = Fixture::new([Reply::bytes(StatusCode::FOUND, b"stopped")
        .location("/next")
        .step()]);
    let result = success(block_on(fixture.client.follow(
        request(Method::GET),
        limit(4),
        |_| Ok::<_, ()>(OutboundRedirectAction::Stop),
    )));
    let (response, uri, redirects, reason) = result.into_parts();
    assert_eq!(uri, Uri::from_static("https://a.example/start"));
    assert_eq!(redirects, 0);
    assert_eq!(reason, Some(OutboundRedirectStopReason::StoppedByPolicy));
    assert_eq!(response.status(), StatusCode::FOUND);
    assert_eq!(
        block_on(response.into_bytes_bounded(16)).expect("final response"),
        "stopped"
    );
    assert_eq!(fixture.sent(), 1);
}

#[test]
fn policy_returned_invalid_request_remains_a_client_error_at_the_selected_hop() {
    let fixture = Fixture::new([Reply::bytes(StatusCode::FOUND, b"")
        .location("/next")
        .step()]);
    let error = hop_error(
        block_on(fixture.client.follow(request(Method::GET), limit(4), |_| {
            Ok::<_, ()>(OutboundRedirectAction::Follow(
                request(Method::GET).body("invalid GET body"),
            ))
        })),
        1,
    );
    assert!(matches!(error, EdgeError::BadRequest { .. }));
    assert_eq!(fixture.sent(), 1);
}

#[test]
fn transport_errors_keep_their_original_category_and_failed_hop_index() {
    for index in [0, 1] {
        let mut steps = Vec::new();
        if index == 1 {
            steps.push(
                Reply::bytes(StatusCode::FOUND, b"")
                    .location("/next")
                    .step(),
            );
        }
        steps.push(Step::Error(EdgeError::bad_gateway_with_reason(
            "original transport error",
            BadGatewayReason::Unreachable,
        )));
        let fixture = Fixture::new(steps);
        let error = hop_error(
            block_on(
                fixture
                    .client
                    .follow(request(Method::GET), limit(4), allow_follow),
            ),
            index,
        );
        let EdgeError::BadGateway {
            reason, message, ..
        } = error
        else {
            panic!("transport failure must not be reclassified");
        };
        assert_eq!(reason, BadGatewayReason::Unreachable);
        assert_eq!(message, "original transport error");
        assert_eq!(fixture.sent(), index + 1);
    }
}

#[test]
fn policy_time_is_charged_to_original_deadline_before_follow_or_finalization() {
    for stop in [false, true] {
        let fixture = Fixture::new([Reply::bytes(StatusCode::FOUND, b"")
            .location("/next")
            .step()]);
        let clock = fixture.time.clone();
        let initial = request(Method::GET).deadline(Deadline::at_instant(fixture.time.at(10)));
        let outcome = block_on(fixture.client.follow(initial, limit(4), move |_| {
            clock.set(10);
            Ok::<_, ()>(if stop {
                OutboundRedirectAction::Stop
            } else {
                OutboundRedirectAction::FollowPrepared
            })
        }));
        assert_deadline(hop_error(outcome, usize::from(!stop)));
        assert_eq!(fixture.sent(), 1);
    }
}

#[test]
fn expired_policy_rejection_is_not_replaced_with_a_deadline_error() {
    let fixture = Fixture::new([Reply::bytes(StatusCode::FOUND, b"")
        .location("/next")
        .step()]);
    let clock = fixture.time.clone();
    let identity = Rc::new(Cell::new(19));
    let local = Rc::clone(&identity);
    let initial = request(Method::GET).deadline(Deadline::at_instant(fixture.time.at(10)));
    let outcome = block_on(fixture.client.follow(initial, limit(4), move |_| {
        clock.set(11);
        Err::<OutboundRedirectAction, _>(PolicyRejection {
            identity: Rc::clone(&local),
        })
    }));
    match outcome {
        Err(OutboundFollowFailure::Policy {
            redirect_number: 1,
            error,
        }) => assert!(Rc::ptr_eq(&error.identity, &identity)),
        _ => panic!("late synchronous policy rejection is still the original application error"),
    }
    assert_eq!(fixture.sent(), 1);
}

#[test]
fn unsupported_next_options_win_before_the_expired_chain_budget() {
    let fixture = Fixture::new([Reply::bytes(StatusCode::FOUND, b"")
        .location("/next")
        .step()]);
    let clock = fixture.time.clone();
    let initial = request(Method::GET).deadline(Deadline::at_instant(fixture.time.at(10)));
    let outcome = block_on(fixture.client.follow(initial, limit(4), move |_| {
        clock.set(10);
        let next = request(Method::GET)
            .header("x-fixture-unsupported", "yes")
            .expect("header");
        Ok::<_, ()>(OutboundRedirectAction::Follow(next))
    }));
    let error = hop_error(outcome, 1);
    assert!(
        matches!(error, EdgeError::BadRequest { message, .. } if message == "unsupported fixture option")
    );
    assert_eq!(fixture.sent(), 1);
    assert_eq!(fixture.state.borrow().validations.len(), 2);
}

#[test]
fn replacement_deadlines_cannot_extend_the_original_final_stream_bound() {
    for replacement_deadline in [None, Some(100), Some(20)] {
        let probe = SourceProbe::default();
        let final_reply = Reply {
            status: StatusCode::OK,
            headers: HeaderMap::new(),
            source: probe.source(UnreadBody::Pending),
        };
        let fixture = Fixture::new([
            Reply::bytes(StatusCode::FOUND, b"")
                .location("/next")
                .step(),
            final_reply.step(),
        ]);
        let clock = fixture.time.clone();
        let initial = request(Method::GET).deadline(Deadline::at_instant(fixture.time.at(50)));
        let result = success(block_on(fixture.client.follow(
            initial,
            limit(4),
            move |_| {
                clock.set(10);
                let mut replacement = request(Method::GET).stream_response();
                if let Some(millis) = replacement_deadline {
                    replacement = replacement.deadline(Deadline::at_instant(clock.at(millis)));
                }
                Ok::<_, ()>(OutboundRedirectAction::Follow(replacement))
            },
        )));
        assert!(result.response().body().is_stream());
        let expires = replacement_deadline.unwrap_or(50).min(50);
        let state = fixture.state.borrow();
        assert_eq!(
            state.records[1].budget.deadline.instant(),
            fixture.time.at(expires)
        );
        assert_eq!(state.records[1].budget.cause, BudgetSource::RequestDeadline);
        drop(state);
        assert_eq!(probe.polls.get(), 0);
        fixture.time.set(expires);
        assert_deadline(
            block_on(result.into_parts().0.into_bytes_bounded(1))
                .expect_err("original bound survives final handoff"),
        );
        assert_eq!(probe.drops.get(), 1);
        assert_eq!(fixture.sent(), 2);
    }
}

#[test]
fn first_pending_hop_drop_prevents_a_second_dispatch() {
    let dropped = Rc::new(Cell::new(0));
    let fixture = Fixture::new([
        Step::Pending(Rc::clone(&dropped)),
        Reply::bytes(StatusCode::OK, b"must not send").step(),
    ]);
    let mut future = fixture
        .client
        .follow(request(Method::GET), limit(4), allow_follow);
    assert!(
        future
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    assert_eq!(fixture.sent(), 1);
    drop(future);
    assert_eq!(dropped.get(), 1);
    assert_eq!(fixture.sent(), 1);
    assert_eq!(fixture.state.borrow().steps.len(), 1);
}

#[test]
fn dropping_inflight_second_hop_releases_it_and_prevents_a_third_dispatch() {
    let dropped = Rc::new(Cell::new(0));
    let probe = SourceProbe::default();
    let fixture = Fixture::new([
        unread_reply(UnreadBody::Pending, &probe).step(),
        Step::Pending(Rc::clone(&dropped)),
        Reply::bytes(StatusCode::OK, b"must not send").step(),
    ]);
    let mut future = fixture
        .client
        .follow(request(Method::GET), limit(4), allow_follow);
    assert!(
        future
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    assert_eq!(fixture.sent(), 2);
    assert_eq!(probe.polls.get(), 0);
    assert_eq!(probe.drops.get(), 1);
    assert_eq!(dropped.get(), 0);
    drop(future);
    assert_eq!(dropped.get(), 1);
    assert_eq!(fixture.sent(), 2);
    assert_eq!(fixture.state.borrow().steps.len(), 1);
}

#[test]
fn dropping_unpolled_follow_releases_the_owned_request_without_transport_work() {
    let fixture = Fixture::new([]);
    let probe = SourceProbe::default();
    let initial = request(Method::POST).body(Body::from_stream(probe.source(UnreadBody::Pending)));
    let future = fixture.client.follow(initial, limit(4), allow_follow);
    assert_eq!(fixture.sent(), 0);
    drop(future);
    assert_eq!(probe.polls.get(), 0);
    assert_eq!(probe.drops.get(), 1);
    assert_eq!(fixture.sent(), 0);
}

#[test]
fn plain_send_does_not_follow_a_scripted_redirect() {
    let fixture = Fixture::new([
        Reply::bytes(StatusCode::FOUND, b"redirect body")
            .location("/next")
            .step(),
        Reply::bytes(StatusCode::OK, b"not sent").step(),
    ]);
    let response = block_on(fixture.client.send(request(Method::GET))).expect("plain send");
    assert_eq!(response.status(), StatusCode::FOUND);
    assert_eq!(
        block_on(response.into_bytes_bounded(32)).expect("redirect body"),
        "redirect body"
    );
    assert_eq!(fixture.sent(), 1);
    assert_eq!(
        fixture.state.borrow().records[0].mode,
        OutboundSendMode::Send
    );
}

#[test]
fn followed_hop_logs_omit_query_names_values_and_fragments() {
    log::set_logger(&REDIRECT_LOGGER).expect("one logger for the core unit-test binary");
    log::set_max_level(log::LevelFilter::Trace);
    REDIRECT_LOGS.with(|messages| messages.borrow_mut().clear());
    let fixture = Fixture::new([
        Reply::bytes(StatusCode::FOUND, b"")
            .location("/middle?private-query=private-value#private-fragment")
            .step(),
        Reply::bytes(StatusCode::TEMPORARY_REDIRECT, b"")
            .location("https://b.example/final?private-query=private-value")
            .step(),
        Reply::bytes(StatusCode::OK, b"").step(),
    ]);
    let initial = OutboundRequest::get("https://a.example/start?private-query=private-value")
        .expect("request");
    success(block_on(fixture.client.follow(
        initial,
        limit(4),
        allow_follow,
    )));
    let messages = REDIRECT_LOGS.with(|messages| take(&mut *messages.borrow_mut()));
    assert!(
        !messages.is_empty(),
        "query-safety proof must actually capture redirect logs"
    );
    assert!(messages.iter().any(|message| message.contains("b.example")));
    for message in &messages {
        assert!(!message.contains("private-query"));
        assert!(!message.contains("private-value"));
        assert!(!message.contains("private-fragment"));
    }
    assert_eq!(fixture.sent(), 3);

    let literal = Fixture::new([
        Reply::bytes(StatusCode::FOUND, b"")
            .location("https://b.example/final?ts-ec=abc")
            .step(),
        Reply::bytes(StatusCode::OK, b"").step(),
    ]);
    let initial = OutboundRequest::get("https://a.example/start?ts-ec=abc").expect("request");
    success(block_on(literal.client.follow(
        initial,
        limit(4),
        allow_follow,
    )));
    let messages = REDIRECT_LOGS.with(|messages| take(&mut *messages.borrow_mut()));
    assert!(
        !messages.is_empty(),
        "literal two-hop proof must capture logs"
    );
    for message in &messages {
        assert!(!message.contains("ts-ec"));
        assert!(!message.contains("abc"));
    }
    assert_eq!(literal.sent(), 2);
}

#[test]
fn closing_callback_during_policy_prevents_the_next_dispatch() {
    let fixture = Fixture::new([
        Reply::bytes(StatusCode::FOUND, b"")
            .location("/next")
            .step(),
        Reply::bytes(StatusCode::OK, b"must not send").step(),
    ]);
    let owner = Rc::new(OutboundScopeOwner::callback());
    let cancel = Rc::clone(&owner);
    let future = owner.with(|| {
        fixture
            .client
            .follow(request(Method::GET), limit(4), move |_| {
                cancel.close();
                Ok::<_, ()>(OutboundRedirectAction::FollowPrepared)
            })
    });
    let outcome = block_on(owner.run(future));
    assert_eq!(
        fixture.sent(),
        1,
        "policy cancellation must prevent provider dispatch, not just replace its result"
    );
    assert!(matches!(
        outcome,
        Err(OutboundFollowFailure::Hop {
            error: EdgeError::Internal { .. },
            ..
        })
    ));
    assert!(owner.scope().check().is_err());
}

#[test]
fn closing_callback_from_followed_source_drop_prevents_the_next_dispatch() {
    let owner = Rc::new(OutboundScopeOwner::callback());
    let probe = SourceProbe::default();
    let on_drop = ScopeCloseOnDrop {
        drops: Rc::clone(&probe.drops),
        owner: Some(Rc::clone(&owner)),
    };
    let polls = Rc::clone(&probe.polls);
    let source = stream::poll_fn(move |_| {
        let _retained = &on_drop;
        polls.set(polls.get().saturating_add(1));
        Poll::<Option<Result<Bytes, EdgeError>>>::Pending
    })
    .boxed_local();
    let first = Reply {
        status: StatusCode::FOUND,
        headers: HeaderMap::new(),
        source,
    }
    .location("/next");
    let fixture = Fixture::new([
        first.step(),
        Reply::bytes(StatusCode::OK, b"must not send").step(),
    ]);
    let future = owner.with(|| {
        fixture
            .client
            .follow(request(Method::GET), limit(4), allow_follow)
    });
    let outcome = block_on(owner.run(future));
    assert_eq!(probe.polls.get(), 0);
    assert_eq!(probe.drops.get(), 1);
    assert_eq!(
        fixture.sent(),
        1,
        "discard cleanup must be checked before the next provider dispatch"
    );
    assert!(matches!(
        outcome,
        Err(OutboundFollowFailure::Hop {
            error: EdgeError::Internal { .. },
            ..
        })
    ));
}

fn assert_empty_get_scope_cancellation(follow: bool, close_on_drop: bool) {
    let owner = Rc::new(OutboundScopeOwner::callback());
    let probe = SourceProbe::default();
    let on_drop = ScopeCloseOnDrop {
        drops: Rc::clone(&probe.drops),
        owner: close_on_drop.then(|| Rc::clone(&owner)),
    };
    let on_eof = (!close_on_drop).then(|| Rc::clone(&owner));
    let polls = Rc::clone(&probe.polls);
    let body = Body::from_stream(stream::poll_fn(move |_| {
        let _retained = &on_drop;
        polls.set(polls.get().saturating_add(1));
        if let Some(cancel) = &on_eof {
            cancel.close();
        }
        Poll::Ready(None)
    }));
    let fixture = Fixture::new([Reply::bytes(StatusCode::OK, b"must not send").step()]);
    let initial = request(Method::GET).body(body);
    let error = if follow {
        let future = owner.with(|| fixture.client.follow(initial, limit(4), allow_follow));
        match block_on(owner.run(future)) {
            Err(OutboundFollowFailure::Hop { error, .. }) => error,
            Err(OutboundFollowFailure::Policy { .. }) | Ok(_) => {
                panic!("settlement cancellation must fail before dispatch")
            }
        }
    } else {
        let future = owner.with(|| fixture.client.send(initial));
        block_on(owner.run(future)).expect_err("settlement cancellation")
    };
    assert_eq!(probe.polls.get(), 1);
    assert_eq!(probe.drops.get(), 1);
    assert_eq!(
        fixture.sent(),
        0,
        "empty-body settlement cleanup must prevent any provider dispatch"
    );
    assert!(matches!(error, EdgeError::Internal { .. }));
    assert!(owner.scope().check().is_err());
}

#[test]
fn send_empty_get_scope_closed_from_eof_never_dispatches() {
    assert_empty_get_scope_cancellation(false, false);
}

#[test]
fn send_empty_get_scope_closed_from_source_drop_never_dispatches() {
    assert_empty_get_scope_cancellation(false, true);
}

#[test]
fn follow_empty_get_scope_closed_from_eof_never_dispatches() {
    assert_empty_get_scope_cancellation(true, false);
}

#[test]
fn follow_empty_get_scope_closed_from_source_drop_never_dispatches() {
    assert_empty_get_scope_cancellation(true, true);
}

#[test]
fn policy_stop_clock_rewind_after_observed_head_is_internal() {
    let fixture = Fixture::new([Reply::bytes(StatusCode::FOUND, b"stopped")
        .location("/next")
        .step()]);
    let clock = fixture.time.clone();
    let future = fixture
        .client
        .follow(request(Method::GET), limit(4), move |_| {
            clock.set(5);
            Ok::<_, ()>(OutboundRedirectAction::Stop)
        });
    fixture.time.set(10);
    let outcome = block_on(future);
    assert_eq!(fixture.sent(), 1);
    assert!(
        matches!(
            outcome,
            Err(OutboundFollowFailure::Hop {
                index: 0,
                error: EdgeError::Internal { .. }
            })
        ),
        "finalization must not reset the observed head time from 10 to entry time 0"
    );
}

#[test]
fn policy_error_survives_a_clock_rewind_after_observed_head() {
    let fixture = Fixture::new([Reply::bytes(StatusCode::FOUND, b"")
        .location("/next")
        .step()]);
    let clock = fixture.time.clone();
    let identity = Rc::new(Cell::new(7_usize));
    let local = Rc::clone(&identity);
    let future = fixture
        .client
        .follow(request(Method::GET), limit(4), move |_| {
            clock.set(5);
            Err::<OutboundRedirectAction, _>(PolicyRejection {
                identity: Rc::clone(&local),
            })
        });
    fixture.time.set(10);
    match block_on(future) {
        Err(OutboundFollowFailure::Policy {
            redirect_number: 1,
            error,
        }) => assert!(Rc::ptr_eq(&identity, &error.identity)),
        Err(OutboundFollowFailure::Policy { .. } | OutboundFollowFailure::Hop { .. }) | Ok(_) => {
            panic!("policy rejection must remain the original application value")
        }
    }
    assert_eq!(fixture.sent(), 1);
}
