use std::iter::repeat_with;
use std::sync::Weak;
use std::sync::atomic::{AtomicUsize, Ordering};

use edgezero_core::action;
use edgezero_core::app::App;
use edgezero_core::context::RequestContext;
use edgezero_core::http::{Request, Uri};
use edgezero_core::ingress::{
    AdmissionDecision, IngressDispatchOutcome, IngressFraming, IngressGrant, IngressHeadAccounting,
};
use edgezero_core::outbound::{
    HttpClient, IndependentOutboundProgress, OutboundBatch, OutboundBatchDriverEvent,
    OutboundCollectedSlot, OutboundDispatchRequest, OutboundExchangeOwner, OutboundFuture,
    OutboundHead, OutboundHttpClient, OutboundPending, OutboundPreparedBatch,
    OutboundProgressRuntime, OutboundRequest, OutboundResponse, OutboundScope, OutboundScopeOwner,
    OutboundSendMode, OutboundTime,
};
use edgezero_core::response_egress::ResponseEgressBeginFailure;
use edgezero_core::router::RouterService;

use super::*;

type BegunEgress = Result<
    (
        PreparedResponseEgress,
        ResponseEgressPolicy,
        ResponseEgressAttempt,
        MonotonicClock,
    ),
    Box<ResponseEgressBeginFailure>,
>;

struct PendingClient(OutboundTime, Arc<ProgressResources>);

#[derive(Default)]
struct ProgressResources {
    batch_cancelled: AtomicUsize,
    begin_cancelled: AtomicUsize,
    dispatches: AtomicUsize,
    polls: AtomicUsize,
    released: AtomicUsize,
}

struct ProgressResource(Arc<ProgressResources>);
#[expect(clippy::missing_trait_methods, reason = "Drop::pin_drop is unstable")]
impl Drop for ProgressResource {
    fn drop(&mut self) {
        self.0.released.fetch_add(1, Ordering::SeqCst);
    }
}

struct Handoff {
    batch: OutboundBatch,
    begun: Option<BegunEgress>,
    callback: Option<OutboundScopeOwner>,
    completed: Arc<AtomicUsize>,
    observer: RecordingObserver,
    origin: OutboundPending,
    resources: Arc<ProgressResources>,
    retained: OutboundFuture<OutboundResponse>,
    scope: OutboundScope,
    upload: Weak<[u8]>,
}

std::thread_local! {
    static RESPONSE: RefCell<Option<Response>> = const { RefCell::new(None) };
}

impl OutboundHttpClient for PendingClient {
    fn begin(&self, request: OutboundDispatchRequest) -> Result<OutboundPending, EdgeError> {
        let resource = ProgressResource(Arc::clone(&self.1));
        let cancelled = Arc::clone(&self.1);
        Ok(OutboundPending::new(
            Box::pin(async move {
                let _resource = resource;
                let _request = request;
                pending().await
            }),
            OutboundExchangeOwner::new(move || {
                cancelled.begin_cancelled.fetch_add(1, Ordering::SeqCst);
            }),
        ))
    }

    fn dispatch_head(&self, _request: OutboundDispatchRequest) -> OutboundFuture<OutboundHead> {
        self.1.dispatches.fetch_add(1, Ordering::SeqCst);
        panic!("fixture Send must remain unpolled")
    }

    fn progress(&self) -> Rc<dyn OutboundProgressRuntime> {
        Rc::new(IndependentOutboundProgress)
    }

    fn start_batch_until(&self, batch: OutboundPreparedBatch) -> OutboundBatch {
        let (context, slots) = batch.into_slots();
        assert_eq!(context.slot_count(), 3);
        assert_eq!(slots.len(), 3);
        let resources = repeat_with(|| ProgressResource(Arc::clone(&self.1)))
            .take(3)
            .collect::<Vec<_>>();
        let polled = Arc::clone(&self.1);
        let cancelled = Arc::clone(&self.1);
        OutboundBatch::from_driver(
            context,
            stream::poll_fn(move |_| {
                let _resources = &resources;
                let _slots = &slots;
                polled.polls.fetch_add(1, Ordering::SeqCst);
                Poll::<Option<OutboundBatchDriverEvent>>::Pending
            }),
            OutboundExchangeOwner::new(move || {
                cancelled.batch_cancelled.fetch_add(1, Ordering::SeqCst);
            }),
        )
    }

    fn time(&self) -> OutboundTime {
        self.0.clone()
    }

    fn validate_supported_options(
        &self,
        _request: &OutboundRequest,
        _mode: OutboundSendMode,
    ) -> Result<(), EdgeError> {
        Ok(())
    }
}

#[action]
async fn attached_response(_context: RequestContext) -> Result<Response, EdgeError> {
    RESPONSE
        .with(|slot| slot.borrow_mut().take())
        .ok_or_else(|| EdgeError::internal(anyhow::anyhow!("attached response missing")))
}

impl Handoff {
    fn assert_closed(self, outcome: ResponseEgressOutcome) {
        self.assert_revoked();
        let polls = self.resources.polls.load(Ordering::SeqCst);
        assert!(
            self.upload.upgrade().is_none(),
            "retained handle cannot retain its request body"
        );
        assert!(matches!(
            block_on(self.retained),
            Err(EdgeError::Internal { .. })
        ));
        assert!(matches!(
            block_on(self.origin.collect()),
            Err(EdgeError::Internal { .. })
        ));
        let failure = block_on(self.batch.collect()).expect_err("closed original batch");
        assert!(matches!(failure.error(), EdgeError::Internal { .. }));
        assert_eq!(failure.slots().len(), 3);
        assert!(
            failure
                .slots()
                .iter()
                .all(|slot| matches!(slot, OutboundCollectedSlot::Unresolved))
        );
        assert_eq!(self.resources.released.load(Ordering::SeqCst), 4);
        assert_eq!(self.resources.begin_cancelled.load(Ordering::SeqCst), 1);
        assert_eq!(self.resources.batch_cancelled.load(Ordering::SeqCst), 1);
        assert_eq!(self.resources.dispatches.load(Ordering::SeqCst), 0);
        assert_eq!(
            self.resources.polls.load(Ordering::SeqCst),
            polls,
            "terminal handles cannot poll producers"
        );
        assert_eq!(self.completed.load(Ordering::SeqCst), 1);
        let reports = self.observer.reports();
        assert_eq!(reports.len(), 1);
        assert_eq!(reports[0].outcome, outcome);
    }

    fn assert_live(&mut self) {
        self.scope.check().expect("accepted jobs remain live");
        assert!(self.upload.upgrade().is_some());
        assert_eq!(self.resources.released.load(Ordering::SeqCst), 0);
        assert_eq!(self.resources.begin_cancelled.load(Ordering::SeqCst), 0);
        assert_eq!(self.resources.batch_cancelled.load(Ordering::SeqCst), 0);
        assert_eq!(self.resources.dispatches.load(Ordering::SeqCst), 0);
        assert!(!self.origin.is_ready());
        assert!(matches!(
            self.batch.check().expect("original batch"),
            edgezero_core::OutboundBatchCheck::Pending
        ));
    }

    fn assert_revoked(&self) {
        assert!(self.scope.check().is_err());
        assert_eq!(
            self.resources.released.load(Ordering::SeqCst),
            4,
            "original resources released before consuming handles"
        );
        assert_eq!(self.resources.begin_cancelled.load(Ordering::SeqCst), 1);
        assert_eq!(self.resources.batch_cancelled.load(Ordering::SeqCst), 1);
        assert_eq!(self.resources.dispatches.load(Ordering::SeqCst), 0);
        assert!(self.upload.upgrade().is_none());
    }

    fn new(response: Response, method: Method) -> Self {
        let now = MonotonicInstant::now();
        let callback = OutboundScopeOwner::callback();
        let owner = callback.with(OutboundScopeOwner::response_body);
        let scope = owner.scope();
        let bytes = Arc::<[u8]>::from(&b"owned before callback return"[..]);
        let upload = Arc::downgrade(&bytes);
        let resources = Arc::new(ProgressResources::default());
        let client = HttpClient::with_client(PendingClient(
            OutboundTime::new(stable_clock(now), |_, _| Box::pin(pending())),
            Arc::clone(&resources),
        ));
        let retained = owner.with(|| {
            client.send(
                OutboundRequest::post("https://fixture.invalid/retained")
                    .expect("fixture request")
                    .body(Bytes::from_owner(bytes)),
            )
        });
        let (origin, batch) = pending_jobs(&owner, &client, now);
        assert_eq!(resources.released.load(Ordering::SeqCst), 0);
        assert!(resources.polls.load(Ordering::SeqCst) > 0);
        let attached = owner
            .attach(response)
            .expect("unique body owner attachment");
        let previous = RESPONSE.with(|slot| slot.replace(Some(attached)));
        assert!(previous.is_none());

        let mut app = App::new(
            RouterService::builder()
                .route("/", method.clone(), attached_response)
                .build(),
        );
        app.set_monotonic_clock(stable_clock(now));
        let observer = RecordingObserver::default();
        app.set_response_egress_observer(observer.clone());
        let completed = Arc::new(AtomicUsize::new(0));
        let calls = Arc::clone(&completed);
        let completion_upload = Weak::clone(&upload);
        let completion_resources = Arc::clone(&resources);
        app.set_ingress_admission_policy(move |head| {
            let call_count = Arc::clone(&calls);
            let upload_probe = Weak::clone(&completion_upload);
            let tracked = Arc::clone(&completion_resources);
            AdmissionDecision::Admit {
                completion: ResponseEgressCompletion::new(move |_report| {
                    assert!(
                        upload_probe.upgrade().is_none(),
                        "scope closes before completion"
                    );
                    assert_eq!(
                        tracked.released.load(Ordering::SeqCst),
                        4,
                        "scope releases original jobs before completion callback"
                    );
                    assert_eq!(tracked.begin_cancelled.load(Ordering::SeqCst), 1);
                    assert_eq!(tracked.batch_cancelled.load(Ordering::SeqCst), 1);
                    assert_eq!(tracked.dispatches.load(Ordering::SeqCst), 0);
                    call_count.fetch_add(1, Ordering::SeqCst);
                }),
                grant: IngressGrant::empty(),
                read_deadline: head.read_deadline_after(Duration::from_secs(1)),
            }
        });
        let mut request = Request::new(Body::empty());
        *request.method_mut() = method;
        *request.uri_mut() = Uri::from_static("/");
        let outcome = block_on(callback.run(app.dispatch_ingress(
            request,
            now,
            IngressHeadAccounting::HostManaged,
            IngressFraming::HostManaged,
        )))
        .expect("admitted response");
        let IngressDispatchOutcome::Response(envelope) = outcome else {
            panic!("response envelope");
        };
        let begun =
            callback.with(|| envelope.begin(ResponseEgressFramingMode::ExplicitBufferedLength));
        let mut handoff = Self {
            begun: Some(begun),
            callback: Some(callback),
            completed,
            observer,
            retained,
            scope,
            upload,
            origin,
            batch,
            resources,
        };
        handoff.assert_live();
        handoff
    }

    fn start(&mut self, committer: &mut MockCommitter) -> MockResponse {
        match self.begun.take().expect("one egress attempt") {
            Ok((prepared, policy, attempt, clock)) => {
                let StartOutcome::Started(response) = start_prepared(
                    prepared,
                    policy,
                    attempt,
                    clock,
                    EgressKind::Application,
                    committer,
                ) else {
                    panic!("started response");
                };
                response
            }
            Err(failure) => {
                let (attempt, clock, cause) = failure.into_parts();
                start_fallback(&Method::GET, cause, attempt, &clock, committer)
                    .expect("fallback response")
            }
        }
    }
}

fn pending_jobs(
    owner: &OutboundScopeOwner,
    client: &HttpClient,
    now: MonotonicInstant,
) -> (OutboundPending, OutboundBatch) {
    let mut origin = owner
        .with(|| {
            block_on(
                client
                    .begin(OutboundRequest::get("https://fixture.invalid/begun").expect("origin")),
            )
        })
        .expect("original begun request");
    let mut batch = owner.with(|| {
        client.start_batch_until(
            repeat_with(|| OutboundRequest::get("https://fixture.invalid/bidder").expect("bidder"))
                .take(3)
                .collect(),
            stable_clock(now).deadline_after(Duration::from_secs(3)),
        )
    });
    assert!(!origin.is_ready());
    assert!(
        batch.next().now_or_never().is_none(),
        "original batch pending before egress"
    );
    (origin, batch)
}

#[test]
fn attached_owner_survives_callback_return_and_eof_until_sdk_completion() {
    let mut handoff = Handoff::new(Response::new(Body::stream(stream::empty())), Method::GET);
    let state = Rc::new(RefCell::new(MockState {
        pending: Some(PendingAt::Finish),
        ..MockState::default()
    }));
    let mut committer = MockCommitter::new(Rc::clone(&state));
    handoff.start(&mut committer);
    drop(handoff.callback.take());
    handoff
        .scope
        .check()
        .expect("accepted owner outlives callback");
    let mut task = committer.tasks.pop_front().expect("coordinator");
    assert!(task.as_mut().now_or_never().is_none());
    assert!(
        state.borrow().events.contains(&"finish"),
        "source reached EOF"
    );
    handoff.scope.check().expect("EOF is not SDK completion");
    handoff.assert_live();
    assert!(handoff.upload.upgrade().is_some());
    assert!(handoff.observer.reports().is_empty());
    state.borrow_mut().pending = None;
    block_on(task);
    handoff.assert_closed(ResponseEgressOutcome::HostHandoff);
}

#[test]
fn attached_owner_closes_on_coordinator_cancellation_at_every_await() {
    for pending_at in [PendingAt::Source, PendingAt::Write, PendingAt::Finish] {
        let mut handoff = Handoff::new(Response::new(Body::from("payload")), Method::GET);
        let state = Rc::new(RefCell::new(MockState {
            pending: Some(pending_at),
            ..MockState::default()
        }));
        let mut committer = MockCommitter::new(state);
        handoff.start(&mut committer);
        drop(handoff.callback.take());
        let mut task = committer.tasks.pop_front().expect("coordinator");
        assert!(task.as_mut().now_or_never().is_none());
        handoff.scope.check().expect("coordinator still owns scope");
        handoff.assert_live();
        drop(task);
        handoff.assert_closed(ResponseEgressOutcome::TransportError);
    }
}

#[test]
fn attached_owner_closes_on_source_deadline_write_and_finish_failures() {
    for (body, state, expected) in [
        (
            Body::from_stream(stream::iter([Err(EdgeError::bad_gateway(
                "fixture source",
            ))])),
            MockState::default(),
            ResponseEgressOutcome::SourceError,
        ),
        (
            Body::from("payload"),
            MockState {
                next_body: Some(IoFailureKind::Deadline),
                ..MockState::default()
            },
            ResponseEgressOutcome::DeadlineExceeded,
        ),
        (
            Body::from("payload"),
            MockState {
                writes: VecDeque::from([MockWrite::Fail(IoFailureKind::Transport, 0)]),
                ..MockState::default()
            },
            ResponseEgressOutcome::TransportError,
        ),
        (
            Body::empty(),
            MockState {
                finish: Some(IoFailureKind::Transport),
                ..MockState::default()
            },
            ResponseEgressOutcome::TransportError,
        ),
    ] {
        let mut handoff = Handoff::new(Response::new(body), Method::GET);
        let mut committer = MockCommitter::new(Rc::new(RefCell::new(state)));
        handoff.start(&mut committer);
        drop(handoff.callback.take());
        handoff
            .scope
            .check()
            .expect("accepted work remains live before its egress failure");
        handoff.assert_live();
        committer.run_task();
        handoff.assert_closed(expected);
    }
}

#[test]
fn head_suppression_keeps_attached_owner_until_sdk_terminal() {
    let response = Response::new(Body::from_stream(stream::poll_fn(|_| {
        panic!("HEAD must not read source")
    })));
    let mut handoff = Handoff::new(response, Method::HEAD);
    handoff
        .scope
        .check()
        .expect("owner live after envelope begin");
    let state = Rc::new(RefCell::new(MockState {
        pending: Some(PendingAt::Finish),
        ..MockState::default()
    }));
    let mut committer = MockCommitter::new(Rc::clone(&state));
    let delivered = handoff.start(&mut committer);
    assert!(!delivered.transmits_body);
    drop(handoff.callback.take());
    let mut task = committer.tasks.pop_front().expect("coordinator");
    assert!(task.as_mut().now_or_never().is_none());
    assert!(state.borrow().events.contains(&"finish"));
    handoff.scope.check().expect("HEAD retains accepted scope");
    handoff.assert_live();
    assert!(handoff.upload.upgrade().is_some());
    assert_eq!(handoff.completed.load(Ordering::SeqCst), 0);
    assert!(handoff.observer.reports().is_empty());
    state.borrow_mut().pending = None;
    block_on(task);
    handoff.assert_closed(ResponseEgressOutcome::HostHandoff);
}

#[test]
fn fallback_revokes_application_work_before_sdk_terminal_without_completing_egress() {
    let mut invalid = Response::new(Body::empty());
    invalid
        .headers_mut()
        .insert("content-length", "invalid".parse().expect("header"));
    let mut handoff = Handoff::new(invalid, Method::GET);
    handoff
        .scope
        .check()
        .expect("failed application owner retained until fallback selection");
    assert!(handoff.upload.upgrade().is_some());
    let state = Rc::new(RefCell::new(MockState {
        pending: Some(PendingAt::Finish),
        ..MockState::default()
    }));
    let mut committer = MockCommitter::new(Rc::clone(&state));
    let delivered = handoff.start(&mut committer);
    assert_eq!(delivered.status, StatusCode::INTERNAL_SERVER_ERROR);
    assert!(delivered.transmits_body);
    assert!(handoff.scope.check().is_err());
    handoff.assert_revoked();
    assert!(handoff.upload.upgrade().is_none());
    handoff
        .callback
        .as_ref()
        .expect("callback")
        .scope()
        .check()
        .expect("fallback must not close callback siblings");
    assert_eq!(handoff.completed.load(Ordering::SeqCst), 0);
    assert!(handoff.observer.reports().is_empty());
    drop(handoff.callback.take());
    let mut task = committer.tasks.pop_front().expect("fallback coordinator");
    assert!(task.as_mut().now_or_never().is_none());
    assert!(state.borrow().events.contains(&"finish"));
    assert_eq!(state.borrow().accepted, INTERNAL_FALLBACK_BODY);
    assert_eq!(handoff.completed.load(Ordering::SeqCst), 0);
    assert!(handoff.observer.reports().is_empty());
    assert!(task.as_mut().now_or_never().is_none());
    assert_eq!(handoff.completed.load(Ordering::SeqCst), 0);
    assert!(handoff.observer.reports().is_empty());
    state.borrow_mut().pending = None;
    block_on(task);
    assert_eq!(state.borrow().aborts, 0);
    assert_eq!(state.borrow().drops, 1);
    let reports = handoff.observer.reports();
    assert_eq!(reports.len(), 1);
    assert_eq!(reports[0].body_kind, ResponseEgressBodyKind::Fallback);
    assert_eq!(
        reports[0].fallback_disposition,
        Some(ResponseEgressFallbackDisposition::Completed)
    );
    handoff.assert_closed(ResponseEgressOutcome::ConversionError);
}

#[test]
fn externally_retained_attachment_is_revoked_when_callback_returns_error() {
    let now = MonotonicInstant::now();
    let client = HttpClient::with_client(PendingClient(
        OutboundTime::new(stable_clock(now), |_, _| {
            panic!("unpolled Send must not register timers")
        }),
        Arc::new(ProgressResources::default()),
    ));
    let bytes = Arc::<[u8]>::from(&b"unaccepted response upload"[..]);
    let upload = Arc::downgrade(&bytes);
    let mut escaped_response = None;
    let mut escaped_work = None;
    let mut captured = None;
    let result = block_on(crate::in_outbound_callback(async {
        let owner = OutboundScopeOwner::response_body();
        captured = Some(owner.scope());
        escaped_work = Some(owner.with(|| {
            client.send(
                OutboundRequest::post("https://fixture.invalid/escaped")
                    .expect("fixture request")
                    .body(Bytes::from_owner(bytes)),
            )
        }));
        escaped_response = Some(
            owner
                .attach(Response::new(Body::empty()))
                .expect("attachment"),
        );
        Err::<(), _>("callback failed before returning its response")
    }));
    assert!(result.is_err());
    assert!(
        escaped_response.is_some(),
        "public response remains retained"
    );
    assert!(captured.expect("body scope").check().is_err());
    assert!(
        upload.upgrade().is_none(),
        "attachment alone cannot promote work"
    );
    assert!(matches!(
        block_on(escaped_work.expect("retained Send")),
        Err(EdgeError::Internal { .. })
    ));
    drop(escaped_response);
}
