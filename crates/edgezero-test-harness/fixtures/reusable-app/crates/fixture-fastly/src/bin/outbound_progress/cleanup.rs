use edgezero_adapter_fastly::{
    FASTLY_PLATFORM,
    delivery::{DeliveryError, FastlyRequestDelivery, FastlyResponseCommitState},
    lifecycle::run_custom,
    outbound::FastlyOutboundClient,
    request::FastlyService,
};
use edgezero_core::outbound::test_support::{
    NativeBodySnapshot, NativeBodyWitness, NativeOperationPending,
};
use edgezero_core::{
    Body, Deadline, EdgeError, HttpClient, IndependentOutboundProgress, MonotonicClock,
    MonotonicInstant, OutboundBatchCheck, OutboundCollectedSlot, OutboundHttpClient,
    OutboundRequest, OutboundScope, OutboundScopeOwner, OutboundTime, ResponseEgressBodyKind,
    ResponseEgressDeadline, ResponseEgressOutcome, action,
    app::App,
    http::{Response, Uri},
    response_egress::{ResponseEgressObserver, ResponseEgressReport},
    router::RouterService,
};
use futures::{StreamExt as _, executor::block_on, future, stream};
use serde_json::{Value, json};
use std::{
    cell::{Cell, RefCell},
    rc::Rc,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

const RECORD_PREFIX: &str = "edgezero-lifetime-cleanup ";
const SDK_ROLES: [&str; 6] = ["origin", "a", "b", "c", "callback", "post"];
const SDK_COLUMNS: [&str; 13] = [
    "attachments",
    "preparations",
    "operation_acquisitions",
    "operation_polls",
    "operation_releases",
    "ready_operation_polls",
    "heads",
    "source_acquisitions",
    "source_releases",
    "reads",
    "ready_reads",
    "pending_kind",
    "invalid",
];

#[derive(Default)]
struct NativeWitnesses {
    operations: [NativeBodyWitness; 6],
    continued: RefCell<Option<Vec<NativeBodySnapshot>>>,
}

impl NativeWitnesses {
    fn snapshots(&self, count: usize) -> Vec<NativeBodySnapshot> {
        self.operations[..count]
            .iter()
            .map(NativeBodyWitness::snapshot)
            .collect()
    }
}

fn native_rows(snapshots: &[NativeBodySnapshot]) -> Value {
    json!(
        snapshots
            .iter()
            .map(|s| json!([
                s.attachments,
                s.preparations,
                s.operation_acquisitions,
                s.operation_polls,
                s.operation_releases,
                s.ready_operation_polls,
                s.heads,
                s.source_acquisitions,
                s.source_releases,
                s.reads,
                s.ready_reads,
                s.pending_operation().map(NativeOperationPending::kind),
                s.invalid
            ]))
            .collect::<Vec<_>>()
    )
}

fn native_evidence(probe: &Probe, count: usize, phases: &[(&str, &[NativeBodySnapshot])]) -> Value {
    let mut value = json!({"base_uri":probe.base.to_string(),"roles":&SDK_ROLES[..count],"columns":SDK_COLUMNS});
    for (phase, snapshots) in phases {
        value[*phase] = native_rows(snapshots);
    }
    value
}

fn check_native_request(
    probe: &Probe,
    index: usize,
    snapshot: &NativeBodySnapshot,
) -> Result<(), EdgeError> {
    use edgezero_core::OutboundSendMode;
    let prepared = snapshot.preparation.as_ref().ok_or_else(failed)?;
    let acquired = snapshot.first_operation_acquisition.ok_or_else(failed)?;
    let (started, observed) = if let Some(poll) = snapshot.first_operation_poll {
        (poll.started_at, poll.observed_at)
    } else {
        let wait = snapshot.first_head_wait_timeout.ok_or_else(failed)?;
        (wait.started_at, wait.observed_at)
    };
    let mode = match index {
        0 | 4 => OutboundSendMode::Begin,
        1..=3 => OutboundSendMode::Batch,
        _ => OutboundSendMode::Send,
    };
    check(
        !snapshot.invalid
            && snapshot.attachments == 1
            && snapshot.preparations == 1
            && snapshot.operation_acquisitions == 1
            && snapshot.first_operation_poll.is_some() == (snapshot.operation_polls > 0)
            && prepared.target.to_string() == format!("{}/{}", probe.base, SDK_ROLES[index])
            && prepared.mode == mode
            && prepared.entered_at <= acquired
            && acquired <= started
            && started <= observed
            && snapshot.operation_polls >= snapshot.ready_operation_polls
            && snapshot.reads >= snapshot.ready_reads,
    )
}

fn check_pending(snapshot: &NativeBodySnapshot, closed: bool) -> Result<(), EdgeError> {
    let (started, observed) = match snapshot.pending_operation().ok_or_else(failed)? {
        NativeOperationPending::SdkPoll(poll) => {
            check(snapshot.operation_polls > 0)?;
            (poll.started_at, poll.observed_at)
        }
        NativeOperationPending::HeadReadinessTimeout(wait) => {
            check(snapshot.operation_polls == 0)?;
            (wait.started_at, wait.observed_at)
        }
    };
    check(
        snapshot
            .first_operation_acquisition
            .is_some_and(|at| at <= started)
            && started <= observed
            && snapshot.ready_operation_polls == 0
            && snapshot.operation_releases == u64::from(closed)
            && snapshot.heads == 0
            && snapshot.source_acquisitions == 0
            && snapshot.source_releases == 0
            && snapshot.reads == 0
            && snapshot.waits == 0
            && match snapshot.first_operation_release {
                Some(at) => closed && at >= observed,
                None => !closed,
            },
    )
}

fn check_completed(snapshot: &NativeBodySnapshot) -> Result<(), EdgeError> {
    let first = snapshot.first_operation_poll.ok_or_else(failed)?;
    check(
        snapshot.operation_releases == 1
            && snapshot.ready_operation_polls == 1
            && snapshot.heads == 1
            && snapshot.source_acquisitions == 1
            && snapshot.source_releases == 1
            && snapshot.reads > 0
            && snapshot.ready_reads > 0
            && snapshot
                .first_operation_release
                .is_some_and(|at| at >= first.observed_at)
            && snapshot.first_release.is_some(),
    )
}

fn check_unchanged(
    before: &[NativeBodySnapshot],
    after: &[NativeBodySnapshot],
) -> Result<(), EdgeError> {
    check(
        before.len() == after.len()
            && native_rows(before) == native_rows(after)
            && before.iter().zip(after).all(|(a, b)| {
                a.first_operation_release == b.first_operation_release
                    && a.first_operation_acquisition == b.first_operation_acquisition
                    && a.first_release == b.first_release
                    && a.first_head_wait_timeout
                        .map(|wait| (wait.started_at, wait.observed_at, wait.outcome))
                        == b.first_head_wait_timeout
                            .map(|wait| (wait.started_at, wait.observed_at, wait.outcome))
            }),
    )
}

fn wait_native_pending(probe: &Probe, observation_cutoff: Deadline) -> Result<(), EdgeError> {
    let scope =
        OutboundScope::capture(FastlyOutboundClient::with_time(probe.time.clone()).progress());
    loop {
        let snapshots = probe.native.snapshots(5);
        if snapshots
            .iter()
            .all(|snapshot| snapshot.pending_operation().is_some())
        {
            return Ok(());
        }
        let now = probe.clock.now();
        let mut remaining = observation_cutoff.remaining_at(now).ok_or_else(failed)?;
        for snapshot in snapshots {
            check(
                !snapshot.invalid
                    && snapshot.operation_acquisitions == 1
                    && snapshot.operation_releases == 0
                    && snapshot.heads == 0,
            )?;
            let prepared = snapshot.preparation.ok_or_else(failed)?;
            remaining = remaining.min(
                prepared
                    .budget
                    .deadline
                    .remaining_at(now)
                    .ok_or_else(failed)?,
            );
        }
        let deadline = Deadline::at_instant(now + remaining.min(Duration::from_millis(1)));
        let time = probe.time.clone();
        block_on(scope.run(
            time.clone(),
            now,
            Box::pin(async move { time.wait_until(deadline).await }),
        ))?;
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Mode {
    Complete,
    SourceError,
    WriteDeadline,
}

impl Mode {
    fn parse(value: &str) -> Option<Self> {
        match value {
            "complete" => Some(Self::Complete),
            "source-error" => Some(Self::SourceError),
            "write-deadline" => Some(Self::WriteDeadline),
            _ => None,
        }
    }

    fn name(self) -> &'static str {
        match self {
            Self::Complete => "response-cleanup",
            Self::SourceError => "response-cleanup-source-error",
            Self::WriteDeadline => "response-cleanup-write-deadline",
        }
    }

    fn outcome(self) -> ResponseEgressOutcome {
        match self {
            Self::Complete => ResponseEgressOutcome::Completed,
            Self::SourceError => ResponseEgressOutcome::SourceError,
            Self::WriteDeadline => ResponseEgressOutcome::DeadlineExceeded,
        }
    }

    fn error(self) -> Option<DeliveryError> {
        match self {
            Self::Complete => None,
            Self::SourceError => Some(DeliveryError::Source),
            Self::WriteDeadline => Some(DeliveryError::Deadline),
        }
    }
}

#[derive(Clone, Default)]
struct TailWitness {
    source_error: Rc<Cell<bool>>,
    pending_poll: Rc<Cell<bool>>,
}

struct Reports(Arc<AtomicUsize>);

impl ResponseEgressObserver for Reports {
    fn complete(&self, _: &ResponseEgressReport) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[derive(Clone)]
struct Probe {
    token: String,
    base: Uri,
    clock: MonotonicClock,
    time: OutboundTime,
    start: MonotonicInstant,
    mode: Mode,
    native: Rc<NativeWitnesses>,
}

impl Probe {
    fn parse(request: &fastly::Request, time: OutboundTime) -> Result<Self, EdgeError> {
        let clock = time.clock().clone();
        let token = request
            .get_header_str("x-request-token")
            .ok_or_else(failed)?;
        let base: Uri = request
            .get_header_str("x-fixture-backend")
            .ok_or_else(failed)?
            .parse()
            .map_err(|_| failed())?;
        let mode = request
            .get_header_str("x-fixture-cleanup-mode")
            .and_then(Mode::parse)
            .ok_or_else(failed)?;
        check(
            !token.is_empty()
                && token.len() <= 64
                && token
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_'),
        )?;
        check(
            fixture_core::is_loopback_http(&base)
                && base.port_u16().is_some_and(|port| port != 0)
                && base
                    .authority()
                    .is_some_and(|host| !host.as_str().contains('@'))
                && base.query().is_none()
                && base.path() == format!("/lifetimes/{token}/response-cleanup"),
        )?;
        let start = clock.now();
        Ok(Self {
            token: token.into(),
            base,
            clock,
            time,
            start,
            mode,
            native: Rc::new(NativeWitnesses::default()),
        })
    }

    fn request(&self, role: &str) -> Result<OutboundRequest, EdgeError> {
        let at = self
            .clock
            .now()
            .saturating_duration_since(self.start)
            .as_micros();
        let mut request = OutboundRequest::get(format!("{}/{role}", self.base))
            .map(|request| {
                request
                    .timeout(Duration::from_secs(3))
                    .max_response_bytes(64)
            })?
            .header("x-fixture-at-us", at.to_string())?;
        if let Some(index) = SDK_ROLES.iter().position(|candidate| *candidate == role) {
            self.native.operations[index].attach(&mut request)?;
        }
        Ok(request)
    }

    fn record(&self, stage: &str, evidence: Value) -> Result<(), EdgeError> {
        let line = serde_json::to_string(&json!({"schema":1,"event":"response-cleanup",
            "case":self.mode.name(),"token":self.token,"stage":stage,"evidence":evidence,"qualification":false}))
            .map_err(EdgeError::internal)?;
        check(line.len() <= 2048)?;
        eprintln!("{RECORD_PREFIX}{line}");
        Ok(())
    }
}

fn failed() -> EdgeError {
    EdgeError::internal(std::io::Error::other("response cleanup fixture failed"))
}

fn check(valid: bool) -> Result<(), EdgeError> {
    if valid { Ok(()) } else { Err(failed()) }
}

#[action]
async fn placeholder() -> Result<Response, EdgeError> {
    Ok(Response::new(Body::empty()))
}

pub(super) fn run(request: fastly::Request) -> Result<(), fastly::Error> {
    let provider = FastlyOutboundClient::default();
    let time = provider.time();
    let client = HttpClient::with_client(provider);
    let probe = Probe::parse(&request, time)?;
    let reports = Arc::new(AtomicUsize::new(0));
    let mut callback_scope = None;
    let result = run_custom::<fixture_core::FixtureApp, (), _, _>(request, |request, _| {
        let scope = OutboundScope::capture(Rc::new(IndependentOutboundProgress));
        callback_scope = Some(scope.clone());
        deliver(request, &probe, &client, &scope, Arc::clone(&reports)).map_err(fastly::Error::from)
    });
    let returned = probe.native.snapshots(6);
    if result.is_ok() {
        let continued = probe.native.continued.borrow();
        check_unchanged(continued.as_ref().ok_or_else(failed)?, &returned)?;
        for (index, snapshot) in returned.iter().enumerate() {
            check_native_request(&probe, index, snapshot)?;
            if index < 4 {
                check_pending(snapshot, true)?;
            } else {
                check_completed(snapshot)?;
            }
        }
    }
    probe.record(
        "returned",
        json!({"result":if result.is_ok(){"Ok"}else{"Err"},
        "callback_scope_closed":callback_scope.as_ref().is_some_and(|scope| scope.check().is_err()),
        "terminal_reports":reports.load(Ordering::SeqCst),"native_sdk":native_evidence(&probe,6,&[("returned",&returned)])}),
    )?;
    result
}

fn deliver(
    request: fastly::Request,
    probe: &Probe,
    client: &HttpClient,
    callback_scope: &OutboundScope,
    reports: Arc<AtomicUsize>,
) -> Result<(), EdgeError> {
    let mut callback = block_on(client.begin(probe.request("callback")?))?;
    let owner = OutboundScopeOwner::response_body();
    let body_scope = owner.scope();
    let origin_request = probe.request("origin")?.stream_response();
    // Capture ownership when the original public future is created, before awaiting it.
    let mut origin = block_on(owner.with(|| client.begin(origin_request)))?;
    let requests = ["a", "b", "c"]
        .into_iter()
        .map(|role| probe.request(role))
        .collect::<Result<Vec<_>, _>>()?;
    let observation_cutoff = probe.clock.deadline_after(Duration::from_secs(3));
    let batch =
        Rc::new(RefCell::new(Some(owner.with(|| {
            client.start_batch_until(requests, observation_cutoff)
        }))));
    check(
        !callback.is_ready()
            && !origin.is_ready()
            && matches!(
                batch.borrow_mut().as_mut().ok_or_else(failed)?.check()?,
                OutboundBatchCheck::Pending
            ),
    )?;
    let ready = block_on(client.send(probe.request("ready")?))?;
    check(ready.status().as_u16() == 204 && block_on(ready.into_bytes_bounded(64))?.is_empty())?;
    check(
        !callback.is_ready()
            && !origin.is_ready()
            && matches!(
                batch.borrow_mut().as_mut().ok_or_else(failed)?.check()?,
                OutboundBatchCheck::Pending
            ),
    )?;
    wait_native_pending(probe, observation_cutoff)?;
    let pending = probe.native.snapshots(5);
    for (index, snapshot) in pending.iter().enumerate() {
        check_native_request(probe, index, snapshot)?;
        check_pending(snapshot, false)?;
    }
    let mut bytes = serde_json::to_vec(
        &json!({"schema":1,"case":probe.mode.name(),"token":probe.token,
        "origin_pending":true,"batch_pending":true,"callback_pending":true,"qualification":false}),
    )
    .map_err(EdgeError::internal)?;
    bytes.push(b'\n');
    check(bytes.len() <= 512)?;
    let prefix_len = u64::try_from(bytes.len()).map_err(EdgeError::internal)?;
    let witness = TailWitness::default();
    let tail_witness = witness.clone();
    let tail_probe = probe.clone();
    let tail_client = client.clone();
    let tail_batch = Rc::clone(&batch);
    let tail = stream::once(async move {
        if tail_probe.mode == Mode::Complete {
            return Ok(());
        }
        let ack = tail_client.send(tail_probe.request("ack")?).await?;
        check(ack.status().as_u16() == 204 && ack.into_bytes_bounded(64).await?.is_empty())?;
        if tail_probe.mode == Mode::SourceError {
            tail_witness.source_error.set(true);
            return Err(failed());
        }
        std::future::poll_fn(move |cx| {
            let mut saved = tail_batch.borrow_mut();
            let Some(batch) = saved.as_mut() else {
                return std::task::Poll::Ready(Err(failed()));
            };
            // `next` is cancellation-safe; keep no RefCell borrow across a pending poll.
            let mut next = std::pin::pin!(batch.next());
            let observed = std::future::Future::poll(next.as_mut(), cx);
            if observed.is_pending() {
                tail_witness.pending_poll.set(true);
            }
            observed
        })
        .await?;
        Err(failed())
    })
    .filter_map(|result: Result<(), EdgeError>| future::ready(result.err().map(Err)));
    let source = stream::iter([Ok(bytes.into())]).chain(tail);
    let mut response = Response::new(Body::from_stream(source));
    if probe.mode == Mode::WriteDeadline {
        response
            .extensions_mut()
            .insert(ResponseEgressDeadline::after(Duration::from_secs(1)));
    }
    response.headers_mut().insert(
        "content-type",
        "application/json".parse().map_err(EdgeError::internal)?,
    );
    let response = owner.attach(response)?;
    let mut app = App::with_name_and_platform(
        RouterService::builder()
            .get("/progress/lifetimes/response-cleanup", placeholder)
            .build(),
        "response-cleanup-probe",
        FASTLY_PLATFORM,
    );
    app.set_monotonic_clock(probe.clock.clone());
    app.set_response_egress_observer(Reports(Arc::clone(&reports)));
    let delivery = FastlyService::new(&app).send_request_with_hooks(
        request,
        |_, extensions| {
            extensions.insert(client.clone());
        },
        |output| {
            *output = response;
        },
    );
    check(matches!(&delivery, FastlyRequestDelivery::Routed { .. }))?;
    let report = delivery.delivery().ok_or_else(failed)?;
    check(
        report.error() == probe.mode.error()
            && report.commitment() == FastlyResponseCommitState::Committed
            && report.report().outcome == probe.mode.outcome()
            && report.report().body_kind == ResponseEgressBodyKind::Application
            && report.report().fallback_disposition.is_none()
            && report.report().framing_failures.application().is_none()
            && report.report().framing_failures.fallback().is_none()
            && report.report().bytes_written == prefix_len
            && !delivery.may_send_native_response()
            && reports.load(Ordering::SeqCst) == 1,
    )?;
    match probe.mode {
        Mode::SourceError => check(
            witness.source_error.get()
                && report.report().source_failure.is_some_and(|source| {
                    source.category() == "internal"
                        && source.body_kind() == ResponseEgressBodyKind::Application
                        && source.provider_failure_detail().is_none()
                }),
        )?,
        Mode::WriteDeadline => check(
            witness.pending_poll.get()
                && report.report().source_failure.is_none()
                && report.report().elapsed >= Duration::from_secs(1)
                && !observation_cutoff.is_expired_at(probe.clock.now()),
        )?,
        Mode::Complete => check(report.report().source_failure.is_none())?,
    }
    check(body_scope.check().is_err() && callback_scope.check().is_ok() && !callback.is_ready())?;
    // Snapshot while the original public origin and batch handles remain retained.
    let closed = probe.native.snapshots(5);
    for (index, snapshot) in closed.iter().enumerate() {
        check_native_request(probe, index, snapshot)?;
        check_pending(snapshot, index < 4)?;
    }
    check(matches!(
        block_on(origin.collect()),
        Err(EdgeError::Internal { .. })
    ))?;
    let batch = batch.borrow_mut().take().ok_or_else(failed)?;
    let failure = block_on(batch.collect()).err().ok_or_else(failed)?;
    check(
        matches!(failure.error(), EdgeError::Internal { .. })
            && failure.slots().len() == 3
            && failure
                .slots()
                .iter()
                .all(|slot| matches!(slot, OutboundCollectedSlot::Unresolved)),
    )?;
    let collected = probe.native.snapshots(5);
    check_unchanged(&closed[..4], &collected[..4])?;
    check_native_request(probe, 4, &collected[4])?;
    check_pending(&collected[4], false)?;
    let mut cleaned = json!({"outcome":format!("{:?}",probe.mode.outcome()),"terminal_reports":1,"body_scope_closed":true,
        "origin_error":"Internal","batch_error":"Internal","slots":["Unresolved","Unresolved","Unresolved"],
        "callback_scope_live":true,"callback_pending":true,
        "native_sdk":native_evidence(probe,5,&[("pending",&pending),("closed",&closed),("collected",&collected)])});
    if probe.mode != Mode::Complete {
        cleaned["delivery_error"] = json!(format!("{:?}", probe.mode.error().ok_or_else(failed)?));
        cleaned["source_error_reached"] = json!(witness.source_error.get());
        cleaned["pending_poll"] = json!(witness.pending_poll.get());
        cleaned["source_category"] = json!(
            report
                .report()
                .source_failure
                .map(|failure| failure.category())
        );
        cleaned["accepted_prefix"] = json!(true);
    }
    probe.record("cleaned", cleaned)?;
    let callback = block_on(callback.collect())?;
    check(
        callback.status().as_u16() == 200
            && block_on(callback.into_bytes_bounded(64))?.as_ref() == b"K",
    )?;
    let post = block_on(client.send(probe.request("post")?))?;
    check(
        post.status().as_u16() == 200 && block_on(post.into_bytes_bounded(64))?.as_ref() == b"P",
    )?;
    check(callback_scope.check().is_ok() && reports.load(Ordering::SeqCst) == 1)?;
    let completed = probe.native.snapshots(6);
    check_unchanged(&collected[..4], &completed[..4])?;
    for (index, snapshot) in completed.iter().enumerate() {
        check_native_request(probe, index, snapshot)?;
        if index < 4 {
            check_pending(snapshot, true)?;
        } else {
            check_completed(snapshot)?;
        }
    }
    *probe.native.continued.borrow_mut() = Some(completed.clone());
    probe.record(
        "continued",
        json!({"callback_bytes":[75],"post_bytes":[80],"callback_scope_live":true,
        "terminal_reports":1,"same_client":true,"native_sdk":native_evidence(probe,6,&[("completed",&completed)])}),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use edgezero_core::outbound::test_support::{NativeBodyPreparation, NativeOperationPoll};

    fn probe(token: &str) -> Probe {
        let clock = MonotonicClock::default();
        Probe {
            token: token.into(),
            base: format!("http://127.0.0.1:1234/lifetimes/{token}/response-cleanup")
                .parse()
                .unwrap(),
            start: clock.now(),
            time: OutboundTime::new(clock.clone(), |_, _| Box::pin(future::pending())),
            clock,
            mode: Mode::Complete,
            native: Rc::new(NativeWitnesses::default()),
        }
    }

    fn snapshot(probe: &Probe, index: usize, closed: bool) -> NativeBodySnapshot {
        use edgezero_core::{BudgetSource, Deadline, DispatchBudget, OutboundSendMode};
        let at = probe.start;
        NativeBodySnapshot {
            attachments: 1,
            preparations: 1,
            operation_acquisitions: 1,
            operation_polls: 3,
            operation_releases: u64::from(closed),
            first_operation_acquisition: Some(at),
            first_operation_poll: Some(NativeOperationPoll {
                started_at: at,
                observed_at: at,
                pending: true,
            }),
            first_operation_release: closed.then_some(at),
            preparation: Some(NativeBodyPreparation {
                entered_at: at,
                budget: DispatchBudget {
                    cause: BudgetSource::PerCallTimeout,
                    deadline: Deadline::at_instant(at + Duration::from_secs(3)),
                    duration: Duration::from_secs(3),
                },
                mode: match index {
                    0 | 4 => OutboundSendMode::Begin,
                    1..=3 => OutboundSendMode::Batch,
                    _ => OutboundSendMode::Send,
                },
                target: format!("{}/{}", probe.base, SDK_ROLES[index])
                    .parse()
                    .unwrap(),
            }),
            ..NativeBodySnapshot::default()
        }
    }

    #[test]
    fn cleanup_head_timeout_qualifies_without_a_fabricated_sdk_poll() {
        use edgezero_core::outbound::test_support::{NativeBodyWait, NativeBodyWaitOutcome};
        let probe = probe("cleanup");
        let mut live = snapshot(&probe, 1, false);
        live.first_operation_poll = None;
        live.operation_polls = 0;
        live.first_head_wait_timeout = Some(NativeBodyWait {
            started_at: probe.start,
            observed_at: probe.start,
            outcome: NativeBodyWaitOutcome::TimedOut,
        });
        check_native_request(&probe, 1, &live).expect("owned native operation");
        check_pending(&live, false).expect("native timeout");
        live.operation_releases = 1;
        live.first_operation_release = Some(probe.start);
        check_pending(&live, true).expect("released once");
        live.first_head_wait_timeout.as_mut().unwrap().outcome = NativeBodyWaitOutcome::OtherReady;
        assert!(check_pending(&live, true).is_err());
    }

    #[test]
    fn cleanup_original_requests_attach_distinct_sdk_witnesses_without_dispatch() {
        let probe = probe("cleanup");
        let mut requests = SDK_ROLES.map(|role| probe.request(role).unwrap());
        for witness in &probe.native.operations {
            let observed = witness.snapshot();
            assert_eq!(observed.attachments, 1);
            assert_eq!(observed.preparations, 0);
            assert!(!observed.invalid);
        }
        for (role, request) in SDK_ROLES.iter().zip(&mut requests) {
            assert!(
                edgezero_core::outbound::test_support::NativeBodyWitness::new()
                    .attach(request)
                    .is_err(),
                "original SDK request {role} has no witness"
            );
        }
    }

    #[test]
    fn cleanup_sdk_checks_require_actual_pending_ownership_and_release_before_retained_collection()
    {
        let probe = probe("cleanup");
        let live = snapshot(&probe, 0, false);
        check_native_request(&probe, 0, &live).unwrap();
        check_pending(&live, false).unwrap();
        let closed = snapshot(&probe, 0, true);
        check_native_request(&probe, 0, &closed).unwrap();
        check_pending(&closed, true).unwrap();
        assert!(check_pending(&live, true).is_err());
        assert!(check_pending(&closed, false).is_err());
        for mutation in 0..10 {
            let mut invalid = live.clone();
            match mutation {
                0 => invalid.operation_acquisitions = 0,
                1 => invalid.operation_polls = 0,
                2 => invalid.operation_releases = 1,
                3 => invalid.ready_operation_polls = 1,
                4 => invalid.heads = 1,
                5 => invalid.source_acquisitions = 1,
                6 => invalid.invalid = true,
                7 => invalid.first_operation_poll = None,
                8 => {
                    invalid.preparation.as_mut().unwrap().target =
                        "http://127.0.0.1:9999/foreign".parse().unwrap()
                }
                _ => invalid.first_operation_poll.as_mut().unwrap().pending = false,
            }
            assert!(
                check_native_request(&probe, 0, &invalid)
                    .and_then(|()| check_pending(&invalid, false))
                    .is_err(),
                "mutation {mutation}"
            );
        }
        let mut polled = closed.clone();
        polled.operation_polls += 1;
        assert!(check_unchanged(&[closed], &[polled]).is_err());
    }

    #[test]
    fn cleanup_continuation_requires_native_callback_and_post_head_and_body_release() {
        let probe = probe("cleanup");
        for index in [4, 5] {
            let mut completed = snapshot(&probe, index, true);
            completed.ready_operation_polls = 1;
            completed.heads = 1;
            completed.source_acquisitions = 1;
            completed.source_releases = 1;
            completed.reads = 2;
            completed.ready_reads = 2;
            completed.first_release = Some(probe.start);
            check_native_request(&probe, index, &completed).unwrap();
            check_completed(&completed).unwrap();
            for mutation in 0..5 {
                let mut missing = completed.clone();
                match mutation {
                    0 => missing.operation_releases = 0,
                    1 => missing.source_releases = 0,
                    2 => missing.ready_operation_polls = 0,
                    3 => missing.first_release = None,
                    _ => missing.first_operation_release = None,
                }
                assert!(check_completed(&missing).is_err());
            }
        }
    }

    #[test]
    fn cleanup_native_matrices_fit_existing_record_bound_at_maximum_token_and_poll_counts() {
        let mut probe = probe(&"x".repeat(64));
        let pending: Vec<_> = (0..5)
            .map(|index| {
                let mut s = snapshot(&probe, index, false);
                s.operation_polls = u64::MAX;
                s
            })
            .collect();
        let mut closed = pending.clone();
        for s in &mut closed[..4] {
            s.operation_releases = 1;
            s.first_operation_release = Some(probe.start);
        }
        let native = native_evidence(
            &probe,
            5,
            &[
                ("pending", &pending),
                ("closed", &closed),
                ("collected", &closed),
            ],
        );
        for mode in [Mode::Complete, Mode::SourceError, Mode::WriteDeadline] {
            probe.mode = mode;
            let mut evidence = json!({"outcome":format!("{:?}",mode.outcome()),"terminal_reports":1,"body_scope_closed":true,
                "origin_error":"Internal","batch_error":"Internal","slots":["Unresolved","Unresolved","Unresolved"],
                "callback_scope_live":true,"callback_pending":true,"native_sdk":native});
            if mode != Mode::Complete {
                evidence["delivery_error"] = json!(format!("{:?}", mode.error().unwrap()));
                evidence["source_error_reached"] = json!(mode == Mode::SourceError);
                evidence["pending_poll"] = json!(mode == Mode::WriteDeadline);
                evidence["source_category"] = if mode == Mode::SourceError {
                    json!("internal")
                } else {
                    Value::Null
                };
                evidence["accepted_prefix"] = json!(true);
            }
            let line = json!({"schema":1,"event":"response-cleanup","case":mode.name(),"token":probe.token,"stage":"cleaned","evidence":evidence,"qualification":false});
            assert!(serde_json::to_vec(&line).unwrap().len() <= 2048);
        }
    }

    #[test]
    fn cleanup_mode_requires_one_exact_known_token() {
        for (token, mode) in [
            ("complete", Mode::Complete),
            ("source-error", Mode::SourceError),
            ("write-deadline", Mode::WriteDeadline),
        ] {
            assert_eq!(Mode::parse(token), Some(mode));
        }
        for token in [
            "",
            " Complete",
            "source",
            "deadline",
            "source-error,complete",
            "write-deadline\n",
        ] {
            assert_eq!(Mode::parse(token), None);
        }
    }
}
