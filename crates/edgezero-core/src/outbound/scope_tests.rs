use std::cell::RefCell;
use std::collections::VecDeque;
use std::iter;
use std::mem::take;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::task::{Context, Poll, Wake, Waker};
use std::time::Duration;

use bytes::Bytes;
use futures::executor::block_on;
use futures_util::future::{pending, poll_fn, ready};
use futures_util::{FutureExt as _, StreamExt as _, stream};

use super::{
    ContentTypeProvenance, HttpClient, IndependentOutboundProgress, OutboundBatch,
    OutboundBatchCheck, OutboundBatchContext, OutboundBatchDriverEvent, OutboundBatchItem,
    OutboundBatchTermination, OutboundCollectedSlot, OutboundDispatchRequest,
    OutboundExchangeOwner, OutboundFuture, OutboundHead, OutboundHeaderFidelity,
    OutboundHeaderInput, OutboundHttpClient, OutboundPending, OutboundPreparedBatch,
    OutboundProgressRuntime, OutboundRequest, OutboundResponse, OutboundScope, OutboundScopeKind,
    OutboundScopeOwner, OutboundSendMode, OutboundTime, validate_for_dispatch,
};
use crate::body::{Body, BodyPayload};
use crate::error::{BudgetSource, EdgeError};
use crate::http::{HeaderMap, Method, Response, StatusCode};
use crate::response_egress::{ResponseEgressCompletion, ResponseEgressEnvelope};
use crate::response_egress_framing::ResponseEgressFramingMode;
use crate::time::{Deadline, DispatchBudget, MonotonicClock, MonotonicInstant};

#[derive(Default)]
struct Observations {
    aborts: AtomicUsize,
    cleanup_closed: AtomicBool,
    entered: Mutex<Vec<MonotonicInstant>>,
    polls: Mutex<Vec<Option<OutboundScopeKind>>>,
    provider_allocations: AtomicUsize,
    releases: AtomicUsize,
}

struct ProbeClient {
    batch: Option<Arc<TimelineBatch>>,
    observed: Arc<Observations>,
    time: OutboundTime,
}

struct ProbeProgress(Arc<Observations>);

struct ProbeProviderState;

#[derive(Default)]
struct TimelineBatch {
    completions: Mutex<VecDeque<(usize, MonotonicInstant)>>,
    releases: AtomicUsize,
    started: AtomicUsize,
}

struct TimelineFixture {
    client: HttpClient,
    clock: MonotonicClock,
    entered: MonotonicInstant,
    now: Arc<Mutex<MonotonicInstant>>,
    observed: Arc<Observations>,
    producer: Arc<TimelineBatch>,
    waiters: Arc<Mutex<Vec<Weak<TimerRegistration>>>>,
}

struct TimerRegistration(Mutex<Option<Waker>>);

#[derive(Default)]
struct WakeCount(AtomicUsize);

impl OutboundHttpClient for ProbeClient {
    fn begin(&self, request: OutboundDispatchRequest) -> Result<OutboundPending, EdgeError> {
        let head = self.dispatch_head(request).now_or_never().ok_or_else(|| {
            EdgeError::internal(anyhow::anyhow!("probe head unexpectedly pending"))
        })??;
        let completion = head.finish();
        let observed = Arc::clone(&self.observed);
        Ok(OutboundPending::new(
            completion,
            OutboundExchangeOwner::new(move || {
                observed.aborts.fetch_add(1, Ordering::SeqCst);
            }),
        ))
    }

    fn dispatch_head(&self, request: OutboundDispatchRequest) -> OutboundFuture<OutboundHead> {
        self.observed
            .entered
            .lock()
            .expect("entries")
            .push(request.entered_at());
        let (parts, captured) = request.into_parts();
        let observed = Arc::clone(&self.observed);
        Box::pin(async move {
            if parts.uri.path() == "/origin" {
                let head_at = Deadline::at_instant(
                    captured
                        .entered_at()
                        .checked_add(Duration::from_millis(118))
                        .expect("origin head instant"),
                );
                captured.time().wait_until(head_at).await?;
            }
            if parts.uri.path() == "/mediator" {
                captured
                    .time()
                    .wait_until(captured.budget().deadline)
                    .await?;
                return Err(EdgeError::gateway_timeout_caused(
                    "mediator deadline",
                    captured.budget().cause,
                ));
            }
            OutboundHead::new(
                StatusCode::OK,
                OutboundHeaderInput::new(
                    "runtime-probe",
                    HeaderMap::new(),
                    OutboundHeaderFidelity::AdapterVisibleFields,
                ),
                stream::once(ready(Ok(Bytes::from_static(b"live")))).boxed_local(),
                captured,
                OutboundExchangeOwner::new(move || {
                    let scope = OutboundScope::capture(Rc::new(IndependentOutboundProgress));
                    observed.cleanup_closed.store(
                        scope.kind() == Some(OutboundScopeKind::ResponseBody)
                            && scope.check().is_err(),
                        Ordering::SeqCst,
                    );
                    observed.releases.fetch_add(1, Ordering::SeqCst);
                }),
            )
        })
    }

    fn progress(&self) -> Rc<dyn OutboundProgressRuntime> {
        Rc::new(ProbeProgress(Arc::clone(&self.observed)))
    }

    fn start_batch_until(&self, batch: OutboundPreparedBatch) -> OutboundBatch {
        let Some(timeline) = &self.batch else {
            return OutboundBatch::cutoff(batch.into_slots().0);
        };
        let (context, slots) = batch.into_slots();
        let count = slots.len();
        timeline.started.store(count, Ordering::SeqCst);
        drop(slots);
        let completed = Arc::clone(timeline);
        let released = Arc::clone(timeline);
        let clock = context.clock().clone();
        let mut delivered = 0_usize;
        OutboundBatch::from_driver(
            context,
            stream::poll_fn(move |_| {
                let next = completed
                    .completions
                    .lock()
                    .expect("completed slots")
                    .pop_front();
                if let Some((index, completed_at)) = next {
                    delivered = delivered.saturating_add(1);
                    Poll::Ready(Some(OutboundBatchDriverEvent::Item {
                        index,
                        completed_at,
                        outcome: Ok(OutboundResponse::new(
                            Method::GET,
                            StatusCode::OK,
                            HeaderMap::new(),
                            Body::empty(),
                            clock.clone(),
                            ContentTypeProvenance::Unknown,
                        )),
                    }))
                } else if delivered == count {
                    Poll::Ready(None)
                } else {
                    Poll::Pending
                }
            }),
            OutboundExchangeOwner::new(move || {
                released.releases.fetch_add(1, Ordering::SeqCst);
            }),
        )
    }

    fn time(&self) -> OutboundTime {
        self.time.clone()
    }

    fn validate_supported_options(
        &self,
        request: &OutboundRequest,
        mode: OutboundSendMode,
    ) -> Result<(), EdgeError> {
        validate_for_dispatch(request, mode)
    }
}

impl OutboundProgressRuntime for ProbeProgress {
    fn poll(
        &self,
        scope: &OutboundScope,
        _time: &OutboundTime,
        _entered_at: MonotonicInstant,
        cx: &mut Context<'_>,
        inner: &mut dyn FnMut(&mut Context<'_>) -> Poll<()>,
    ) -> Poll<Result<(), EdgeError>> {
        if let Err(error) = scope.provider_state(|| {
            self.0.provider_allocations.fetch_add(1, Ordering::SeqCst);
            ProbeProviderState
        }) {
            return Poll::Ready(Err(error));
        }
        self.0
            .polls
            .lock()
            .expect("runtime polls")
            .push(scope.kind());
        inner(cx).map(Ok)
    }
}

impl TimelineFixture {
    fn advance(&self, millis: u64) {
        *self.now.lock().expect("advance paired clock") = self.at(millis);
        let waiting = take(&mut *self.waiters.lock().expect("timer registry"));
        for weak in waiting {
            if let Some(registration) = weak.upgrade() {
                let wake = registration.0.lock().expect("timer waker").take();
                if let Some(waker) = wake {
                    waker.wake();
                }
            }
        }
    }

    fn at(&self, millis: u64) -> MonotonicInstant {
        self.entered
            .checked_add(Duration::from_millis(millis))
            .expect("fixture time")
    }

    fn complete_batch(&self, batch: &RefCell<OutboundBatch>) -> Vec<OutboundBatchItem> {
        let mut receipts = Vec::new();
        for (index, completed_ms, observed_ms, elapsed_ms) in
            [(0, 180, 260, 146), (1, 320, 320, 286), (2, 400, 400, 366)]
        {
            self.advance(completed_ms);
            self.producer
                .completions
                .lock()
                .expect("queue completion")
                .push_back((index, self.at(completed_ms)));
            self.advance(observed_ms);
            let OutboundBatchCheck::Item(item) =
                batch.borrow_mut().check().expect("batch observation")
            else {
                panic!("body-owned completion must survive callback exit");
            };
            assert_eq!(item.index(), index);
            assert_eq!(item.result().elapsed, Duration::from_millis(elapsed_ms));
            item.result()
                .outcome
                .as_ref()
                .expect("successful body-owned slot");
            receipts.push(item);
        }
        assert!(matches!(
            batch.borrow_mut().check(),
            Ok(OutboundBatchCheck::Finished(
                OutboundBatchTermination::Completed
            ))
        ));
        assert_eq!(self.producer.releases.load(Ordering::SeqCst), 1);
        receipts
    }

    fn new() -> Self {
        let entered = MonotonicInstant::now();
        let now = Arc::new(Mutex::new(entered));
        let clock_now = Arc::clone(&now);
        let clock = MonotonicClock::new(move || *clock_now.lock().expect("clock"));
        let waiters = Arc::new(Mutex::new(Vec::new()));
        let registrations = Arc::clone(&waiters);
        let time = OutboundTime::new(clock.clone(), move |deadline, paired_clock| {
            let registry = Arc::clone(&registrations);
            let registration = Arc::new(TimerRegistration(Mutex::new(None)));
            Box::pin(poll_fn(move |cx| {
                if deadline.is_expired_at(paired_clock.now()) {
                    drop(registration.0.lock().expect("expired timer").take());
                    Poll::Ready(Ok(()))
                } else {
                    *registration.0.lock().expect("registered waker") = Some(cx.waker().clone());
                    registry
                        .lock()
                        .expect("timer registration")
                        .push(Arc::downgrade(&registration));
                    Poll::Pending
                }
            }))
        });
        let observed = Arc::new(Observations::default());
        let producer = Arc::new(TimelineBatch::default());
        let client = HttpClient::with_client(ProbeClient {
            batch: Some(Arc::clone(&producer)),
            observed: Arc::clone(&observed),
            time,
        });
        Self {
            client,
            clock,
            entered,
            now,
            observed,
            producer,
            waiters,
        }
    }

    fn ready_pending(&self, owner: &OutboundScopeOwner) -> OutboundPending {
        let mut pending_response = block_on(owner.with(|| {
            self.client
                .begin(OutboundRequest::get("https://example.com/").expect("request"))
        }))
        .expect("begun pending");
        assert!(
            !pending_response.is_ready(),
            "buffered finalizer yields between ready reads"
        );
        assert!(
            pending_response.is_ready(),
            "completion is cached before callback exit"
        );
        pending_response
    }

    fn start_origin(
        &self,
        owner: &OutboundScopeOwner,
    ) -> (OutboundFuture<OutboundResponse>, Arc<WakeCount>) {
        self.advance(2);
        let mut origin = owner.with(|| {
            self.client.send(
                OutboundRequest::get("https://example.com/origin")
                    .expect("origin")
                    .stream_response(),
            )
        });
        let notifications = Arc::new(WakeCount::default());
        let waker = Waker::from(Arc::clone(&notifications));
        assert!(
            origin
                .poll_unpin(&mut Context::from_waker(&waker))
                .is_pending()
        );
        assert_eq!(
            self.observed.entered.lock().expect("entries").first(),
            Some(&self.at(2))
        );
        (origin, notifications)
    }
}

impl Wake for WakeCount {
    fn wake(self: Arc<Self>) {
        Self::wake_by_ref(&self);
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

fn client() -> (HttpClient, Arc<Observations>) {
    let observed = Arc::new(Observations::default());
    let now = MonotonicInstant::now();
    let time = OutboundTime::new(MonotonicClock::new(move || now), |_, _| Box::pin(pending()));
    (
        HttpClient::with_client(ProbeClient {
            batch: None,
            observed: Arc::clone(&observed),
            time,
        }),
        observed,
    )
}

#[test]
fn body_created_before_batch_keeps_original_timeline_until_egress_terminal() {
    let fixture = TimelineFixture::new();
    let callback = OutboundScopeOwner::callback();
    let body_owner = callback.with(OutboundScopeOwner::response_body);
    let body_scope = body_owner.scope();
    let (origin, wakes) = fixture.start_origin(&body_owner);
    fixture.advance(34);
    let requests =
        iter::repeat_with(|| OutboundRequest::get("https://example.com/").expect("request"))
            .take(3)
            .collect();
    let batch = Rc::new(RefCell::new(body_owner.with(|| {
        fixture
            .client
            .start_batch_until(requests, Deadline::at_instant(fixture.at(1000)))
    })));
    assert_eq!(fixture.producer.started.load(Ordering::SeqCst), 3);
    let body_pending = fixture.ready_pending(&body_owner);
    let callback_pending = fixture.ready_pending(&callback);
    fixture.advance(120);
    assert!(
        wakes.0.load(Ordering::SeqCst) > 0,
        "paired origin timer must wake"
    );
    let origin_response = origin
        .now_or_never()
        .expect("origin head at 120")
        .expect("origin head");
    let retained_batch = Rc::clone(&batch);
    let response = body_owner
        .attach(Response::new(Body::from_stream(stream::once(async move {
            let _retain_group = retained_batch;
            origin_response.into_body().into_bytes_bounded(4).await
        }))))
        .expect("attach exact body lifetime");
    let envelope = ResponseEgressEnvelope::detached(
        response,
        fixture.entered,
        Method::GET,
        ResponseEgressCompletion::empty(),
        fixture.clock.clone(),
    );
    let Ok((prepared, _, mut attempt, _)) =
        callback.with(|| envelope.begin(ResponseEgressFramingMode::ExplicitBufferedLength))
    else {
        panic!("body scope must be accepted before callback return");
    };
    assert!(attempt.begin_writing());
    callback.close();
    assert_eq!(
        fixture.observed.aborts.load(Ordering::SeqCst),
        1,
        "callback cleanup does not cancel body-owned pending"
    );
    assert!(matches!(
        block_on(callback_pending.collect()),
        Err(EdgeError::Internal { .. })
    ));
    body_scope
        .check()
        .expect("accepted body work survives callback exit");

    let receipts = fixture.complete_batch(&batch);
    fixture.advance(410);
    assert_eq!(
        block_on(prepared.into_response().into_body().into_bytes_bounded(6)).expect("origin body"),
        "live"
    );
    body_scope
        .check()
        .expect("source EOF is not transport completion");
    assert_eq!(fixture.observed.aborts.load(Ordering::SeqCst), 1);
    fixture.advance(420);
    assert!(attempt.complete(fixture.at(420)));
    assert!(body_scope.check().is_err());
    assert_eq!(
        fixture.observed.aborts.load(Ordering::SeqCst),
        2,
        "terminal releases retained body pending before it is collected"
    );
    assert!(matches!(
        block_on(body_pending.collect()),
        Err(EdgeError::Internal { .. })
    ));
    assert_eq!(
        receipts.len(),
        3,
        "already delivered receipts remain owned after scope cleanup"
    );
    for item in receipts {
        item.result()
            .outcome
            .as_ref()
            .expect("receipt remains valid");
    }
}

#[test]
fn body_scoped_mediator_keeps_134ms_request_deadline_after_120ms_callback_exit() {
    let fixture = TimelineFixture::new();
    let callback = OutboundScopeOwner::callback();
    let owner = callback.with(OutboundScopeOwner::response_body);
    fixture.advance(34);
    let mut mediator = owner.with(|| {
        fixture.client.send(
            OutboundRequest::get("https://example.com/mediator")
                .expect("mediator")
                .deadline(Deadline::at_instant(fixture.at(134))),
        )
    });
    let notifications = Arc::new(WakeCount::default());
    let waker = Waker::from(Arc::clone(&notifications));
    assert!(
        mediator
            .poll_unpin(&mut Context::from_waker(&waker))
            .is_pending()
    );
    let response = owner
        .attach(Response::new(Body::empty()))
        .expect("attach before callback ends");
    fixture.advance(120);
    let envelope = ResponseEgressEnvelope::detached(
        response,
        fixture.entered,
        Method::GET,
        ResponseEgressCompletion::empty(),
        fixture.clock.clone(),
    );
    let Ok((_prepared, _, mut attempt, _)) =
        callback.with(|| envelope.begin(ResponseEgressFramingMode::ExplicitBufferedLength))
    else {
        panic!("accept mediator body owner");
    };
    callback.close();
    assert!(
        mediator
            .poll_unpin(&mut Context::from_waker(&waker))
            .is_pending()
    );
    let before_expiry = notifications.0.load(Ordering::SeqCst);
    fixture.advance(134);
    assert!(
        notifications.0.load(Ordering::SeqCst) > before_expiry,
        "the paired deadline timer must wake"
    );
    fixture.advance(260);
    let error = mediator
        .now_or_never()
        .expect("expired mediator must be ready")
        .expect_err("134ms own deadline cannot restart at handoff");
    assert!(matches!(
        error,
        EdgeError::GatewayTimeout {
            cause: BudgetSource::RequestDeadline,
            ..
        }
    ));
    assert_eq!(
        fixture
            .observed
            .entered
            .lock()
            .expect("mediator entry")
            .as_slice(),
        &[fixture.at(34)]
    );
    assert!(attempt.begin_writing());
    assert!(attempt.complete(fixture.at(260)));
}

#[test]
fn nonblocking_observers_activate_scope_without_driving_progress() {
    let callback = OutboundScopeOwner::callback();
    let observed = Arc::new(Observations::default());
    let scope =
        callback.with(|| OutboundScope::capture(Rc::new(ProbeProgress(Arc::clone(&observed)))));
    let entered = MonotonicInstant::now();
    let time = OutboundTime::new(MonotonicClock::new(move || entered), |_, _| {
        panic!("observation cannot create a timer")
    });
    let receiver = poll_fn(|_cx| {
        assert_eq!(
            OutboundScope::capture(Rc::new(IndependentOutboundProgress)).kind(),
            Some(OutboundScopeKind::Callback)
        );
        Poll::Pending
    });
    let mut pending_response =
        OutboundPending::new(Box::pin(receiver), OutboundExchangeOwner::new(|| {}))
            .bind(&scope, time.clone(), entered)
            .expect("bind receiver");
    let context = OutboundBatchContext::new(
        entered,
        time,
        Deadline::after(Duration::from_secs(1)),
        vec![Some(DispatchBudget {
            cause: BudgetSource::BatchCutoff,
            deadline: Deadline::after(Duration::from_secs(1)),
            duration: Duration::from_secs(1),
        })],
        Vec::new(),
        scope,
    );
    let mut batch = OutboundBatch::from_driver(
        context,
        stream::poll_fn(|_cx| {
            assert_eq!(
                OutboundScope::capture(Rc::new(IndependentOutboundProgress)).kind(),
                Some(OutboundScopeKind::Callback)
            );
            Poll::Pending
        }),
        OutboundExchangeOwner::new(|| {}),
    );
    let foreign = callback.with(OutboundScopeOwner::response_body);
    assert!(!foreign.with(|| pending_response.is_ready()));
    assert!(matches!(
        foreign.with(|| batch.check()),
        Ok(OutboundBatchCheck::Pending)
    ));
    assert!(
        observed.polls.lock().expect("progress calls").is_empty(),
        "neither nonblocking observer may drive the provider"
    );
    let mut collect = pending_response.collect();
    assert!(
        collect
            .poll_unpin(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    let mut next = Box::pin(batch.next());
    assert!(
        next.poll_unpin(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    assert_eq!(
        observed.polls.lock().expect("progress calls").as_slice(),
        &[
            Some(OutboundScopeKind::Callback),
            Some(OutboundScopeKind::Callback)
        ]
    );
}

#[test]
fn captured_runtime_wraps_settlement_and_head_finalization() {
    let callback = OutboundScopeOwner::callback();
    let (client, observed) = client();
    let request = OutboundRequest::get("https://example.com/")
        .expect("request")
        .timeout(Duration::from_secs(1))
        .body(crate::Body::from_stream(stream::empty()));
    let future = callback.with(|| client.send(request));
    let response = block_on(callback.run(future)).expect("send");
    assert_eq!(response.body().as_bytes(), Some(b"live".as_slice()));
    assert_eq!(observed.releases.load(Ordering::SeqCst), 1);
    assert_eq!(
        observed.provider_allocations.load(Ordering::SeqCst),
        1,
        "one captured provider state in the callback pipeline"
    );
    let polls = observed.polls.lock().expect("runtime polls");
    assert!(
        polls.len() >= 2,
        "the facade and head finalizer both run captured progress"
    );
    assert!(
        polls
            .iter()
            .all(|kind| *kind == Some(OutboundScopeKind::Callback))
    );
}

#[test]
fn response_owned_body_is_rejected_before_any_outbound_transport() {
    for mode in [
        OutboundSendMode::Send,
        OutboundSendMode::Begin,
        OutboundSendMode::Batch,
    ] {
        let (client, observed) = client();
        let owner = OutboundScopeOwner::response_body();
        let scope = owner.scope();
        let response = owner
            .attach(Response::new(Body::from("payload")))
            .expect("attach response lifetime");
        let request = OutboundRequest::post("https://example.com/")
            .expect("request")
            .body(response.into_body());
        let error = match mode {
            OutboundSendMode::Send => block_on(client.send(request))
                .expect_err("response ownership is not upload ownership"),
            OutboundSendMode::Begin => block_on(client.begin(request))
                .expect_err("response ownership is not upload ownership"),
            OutboundSendMode::Batch => {
                let batch = client
                    .start_batch_until(vec![request], Deadline::after(Duration::from_secs(1)));
                let (mut results, _) = block_on(batch.collect())
                    .expect("preflight receipts")
                    .into_parts();
                let OutboundCollectedSlot::Resolved(result) = results.pop().expect("slot") else {
                    panic!("preflight result must be resolved");
                };
                result
                    .outcome
                    .expect_err("response ownership is not upload ownership")
            }
            OutboundSendMode::Follow => panic!("only direct facade modes are tested"),
        };
        assert!(matches!(error, EdgeError::BadRequest { .. }));
        assert!(
            observed
                .entered
                .lock()
                .expect("provider entries")
                .is_empty()
        );
        assert!(
            scope.check().is_err(),
            "rejected upload releases response work"
        );
    }
}

#[test]
fn outbound_response_normalization_preserves_attached_owner_and_buffered_length() {
    for status in [StatusCode::OK, StatusCode::NO_CONTENT] {
        let owner = OutboundScopeOwner::response_body();
        let scope = owner.scope();
        let bytes = Bytes::from(vec![b'x'; 16]);
        let pointer = bytes.as_ptr();
        let response = owner
            .attach(Response::new(Body::from(bytes)))
            .expect("attach");
        let normalized_response = OutboundResponse::new(
            Method::GET,
            status,
            HeaderMap::new(),
            response.into_body(),
            MonotonicClock::default(),
            ContentTypeProvenance::Unknown,
        )
        .into_response()
        .expect("normalized response");
        let retained_bytes = normalized_response
            .body()
            .as_bytes()
            .expect("Once remains Once");
        if status == StatusCode::OK {
            assert_eq!(retained_bytes.as_ptr(), pointer);
            assert_eq!(retained_bytes.len(), 16);
        } else {
            assert!(retained_bytes.is_empty());
        }
        scope
            .check()
            .expect("normalization keeps the exact response owner");
        drop(normalized_response);
        assert!(scope.check().is_err());
    }
}

#[test]
fn reused_body_scope_releases_retained_outbound_transport_at_egress_terminal() {
    let callback = OutboundScopeOwner::callback();
    let owner = callback.with(OutboundScopeOwner::response_body);
    let (client, observed) = client();
    let retained = block_on(owner.with(|| {
        client.send(
            OutboundRequest::get("https://example.com/")
                .expect("request")
                .stream_response(),
        )
    }))
    .expect("retained origin response");
    let response = owner
        .attach(Response::new(Body::empty()))
        .expect("attach precreated owner");
    let now = MonotonicInstant::now();
    let envelope = ResponseEgressEnvelope::detached(
        response,
        now,
        Method::GET,
        ResponseEgressCompletion::empty(),
        MonotonicClock::new(move || now),
    );
    let Ok((_prepared, _, mut attempt, _)) =
        callback.with(|| envelope.begin(ResponseEgressFramingMode::ExplicitBufferedLength))
    else {
        panic!("accept body owner");
    };
    callback.close();
    assert_eq!(observed.releases.load(Ordering::SeqCst), 0);
    assert!(attempt.begin_writing());
    assert!(attempt.complete(now));
    assert_eq!(
        observed.releases.load(Ordering::SeqCst),
        1,
        "terminal must drop SDK resources before retained handles are polled or dropped"
    );
    assert!(
        observed.cleanup_closed.load(Ordering::SeqCst),
        "transport cleanup keeps its closed captured body identity"
    );
    assert!(matches!(
        block_on(retained.into_bytes_bounded(4)),
        Err(EdgeError::Internal { .. })
    ));
    assert_eq!(observed.releases.load(Ordering::SeqCst), 1);
}

#[test]
fn precreated_body_owner_attaches_origin_without_creating_another_owner() {
    let callback = OutboundScopeOwner::callback();
    let owner = callback.with(OutboundScopeOwner::response_body);
    let (client, observed) = client();
    let origin = block_on(owner.with(|| {
        client.send(
            OutboundRequest::get("https://example.com/")
                .expect("request")
                .stream_response(),
        )
    }))
    .expect("origin");
    let response = owner
        .attach(origin.into_response().expect("origin response"))
        .expect("precreated owner is the sole body owner");
    drop(response);
    assert_eq!(observed.releases.load(Ordering::SeqCst), 1);
    callback
        .scope()
        .check()
        .expect("body cleanup does not close callback siblings");
}

#[test]
fn standalone_origin_rejects_duplicate_body_owner_attachment() {
    let callback = OutboundScopeOwner::callback();
    let (client, observed) = client();
    let origin = block_on(callback.with(|| {
        client.send(
            OutboundRequest::get("https://example.com/")
                .expect("request")
                .stream_response(),
        )
    }))
    .expect("origin");
    assert!(matches!(
        OutboundScopeOwner::response_body()
            .attach(origin.into_response().expect("origin response")),
        Err(EdgeError::Internal { .. })
    ));
    assert_eq!(observed.releases.load(Ordering::SeqCst), 1);
}

#[test]
fn collected_stream_transfers_exact_body_owner_and_eof_does_not_close_it() {
    let callback = OutboundScopeOwner::callback();
    let (client, observed) = client();
    let response = block_on(callback.with(|| {
        client.send(
            OutboundRequest::get("https://example.com/")
                .expect("request")
                .stream_response(),
        )
    }))
    .expect("stream handoff");
    let (payload, retained_owner) = response.into_body().into_parts();
    let owner = retained_owner.expect("the accepted stream lifetime must travel in Body metadata");
    let scope = owner.scope();
    let BodyPayload::Stream(mut source) = payload else {
        panic!("stream remains streamed");
    };
    callback.close();
    assert_eq!(
        block_on(source.next()).expect("chunk").expect("bytes"),
        "live"
    );
    assert!(block_on(source.next()).is_none());
    assert_eq!(observed.releases.load(Ordering::SeqCst), 1);
    scope
        .check()
        .expect("upstream EOF is not downstream egress terminal");
    drop(owner);
    assert!(scope.check().is_err());
}

#[test]
fn collected_stream_uses_captured_runtime_after_callback_exit() {
    let callback = OutboundScopeOwner::callback();
    let (client, observed) = client();
    let request = OutboundRequest::get("https://example.com/")
        .expect("request")
        .stream_response();
    let future = callback.with(|| client.send(request));
    let response = block_on(callback.run(future)).expect("head handoff");
    callback.close();
    assert_eq!(observed.releases.load(Ordering::SeqCst), 0);
    assert_eq!(
        block_on(response.into_bytes_bounded(4)).expect("independent body"),
        Bytes::from_static(b"live")
    );
    assert_eq!(observed.releases.load(Ordering::SeqCst), 1);
    assert_eq!(
        observed.provider_allocations.load(Ordering::SeqCst),
        1,
        "body handoff retains the captured provider state before its first read"
    );
    assert!(
        observed
            .polls
            .lock()
            .expect("runtime polls")
            .contains(&Some(OutboundScopeKind::ResponseBody)),
        "stream reads must activate retained provider progress under a body owner"
    );
}

#[test]
fn facade_begin_stream_handoff_unregisters_callback_cancellation() {
    let callback = OutboundScopeOwner::callback();
    let (client, observed) = client();
    let request = OutboundRequest::get("https://example.com/")
        .expect("request")
        .stream_response();
    let future = callback.with(|| client.begin(request));
    let pending = block_on(future).expect("begun stream");
    let response = block_on(pending.collect()).expect("stream transfer");
    callback.close();
    assert_eq!(observed.aborts.load(Ordering::SeqCst), 0);
    assert_eq!(observed.releases.load(Ordering::SeqCst), 0);
    assert_eq!(
        block_on(response.into_bytes_bounded(4)).expect("body after callback"),
        Bytes::from_static(b"live")
    );
    assert_eq!(observed.aborts.load(Ordering::SeqCst), 0);
    assert_eq!(observed.releases.load(Ordering::SeqCst), 1);
    assert_eq!(observed.provider_allocations.load(Ordering::SeqCst), 1);
}

#[test]
fn facade_begin_uncollected_ready_stream_is_released_at_callback_exit() {
    let callback = OutboundScopeOwner::callback();
    let (client, observed) = client();
    let request = OutboundRequest::get("https://example.com/")
        .expect("request")
        .stream_response();
    let future = callback.with(|| client.begin(request));
    let mut retained = block_on(future).expect("begun stream");
    assert!(retained.is_ready());
    callback.close();
    assert_eq!(observed.aborts.load(Ordering::SeqCst), 1);
    assert_eq!(observed.releases.load(Ordering::SeqCst), 1);
    assert!(matches!(
        block_on(retained.collect()),
        Err(EdgeError::Internal { .. })
    ));
    assert!(
        !observed
            .polls
            .lock()
            .expect("runtime polls")
            .contains(&Some(OutboundScopeKind::ResponseBody)),
        "uncollected bodies are released without reading them"
    );
}

#[test]
fn streamed_body_drop_closes_ownership_before_sdk_cleanup_callbacks() {
    let callback = OutboundScopeOwner::callback();
    let (client, observed) = client();
    let request = OutboundRequest::get("https://example.com/")
        .expect("request")
        .stream_response();
    let future = callback.with(|| client.send(request));
    let response = block_on(future).expect("stream handoff");
    drop(response);
    assert_eq!(observed.releases.load(Ordering::SeqCst), 1);
    assert!(
        observed.cleanup_closed.load(Ordering::SeqCst),
        "SDK cleanup runs inside a closed body scope, not a fresh manual scope"
    );
    callback
        .scope()
        .check()
        .expect("callback siblings stay live");
}
