use super::*;
use edgezero_core::body::BodyStream;
use edgezero_core::outbound::test_support::{NativeBodySnapshot, NativeBodyWitness};
use edgezero_core::{IngressGrant, ResponseEgressCompletion, ResponseEgressDeadline};
use futures::{FutureExt as _, future::poll_fn, stream};
use std::sync::{
    Mutex,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use std::task::Poll;

pub(super) const PREFIX: &str = "edgezero-response-owned-cleanup ";
pub(super) const CASES: [&str; 5] = [
    "response-owned-cleanup-complete",
    "response-owned-cleanup-disconnect",
    "response-owned-cleanup-write-deadline",
    "response-owned-cleanup-source-error",
    "response-owned-cleanup-cancel",
];

struct Witness {
    case: String,
    token: String,
    isolated: bool,
    native: Vec<NativeBodyWitness>,
    start: OnceLock<MonotonicInstant>,
    clock: OnceLock<MonotonicClock>,
    source_drops: AtomicUsize,
    source_polls: AtomicUsize,
    write_chunks: AtomicUsize,
    cleanup: Mutex<Option<Value>>,
    terminal: Mutex<Option<Value>>,
    emitted: AtomicBool,
    prefix_produced: AtomicBool,
    pending_observed: AtomicBool,
}

struct Retained {
    scope: OutboundScope,
    witness: Arc<Witness>,
    origin: RefCell<Option<edgezero_core::OutboundPending>>,
    body: RefCell<Option<BodyStream>>,
    batch: RefCell<Option<OutboundBatch>>,
}

struct SourceLease(Arc<Witness>);

struct SourceState {
    _lease: SourceLease,
    client: HttpClient,
    probe: Probe,
    retained: Rc<Retained>,
    phase: usize,
    isolated: bool,
    timeline: Timeline,
}

fn encoded_record(witness: &Witness, stage: &str, evidence: Value) -> Result<String, EdgeError> {
    let value = json!({"schema":1,"event":"response-owned-cleanup","case":witness.case,
        "token":witness.token,"stage":stage,"evidence":evidence,"qualification":false});
    let bytes = serde_json::to_string(&value).map_err(EdgeError::internal)?;
    if bytes.len() > 2048 {
        return Err(invalid());
    }
    Ok(bytes)
}

fn record(witness: &Witness, stage: &str, evidence: Value) {
    if let Ok(bytes) = encoded_record(witness, stage, evidence) {
        let line = format!("{PREFIX}{bytes}");
        if let Some(sink) = crate::TELEMETRY.get() {
            sink(&line);
        } else {
            eprintln!("{line}");
        }
    }
}

impl Witness {
    fn closed_evidence(&self, cleanup: Value, terminal: Value) -> Value {
        json!({"cleanup":cleanup,"terminal":terminal,
            "source_drops":self.source_drops.load(Ordering::SeqCst),
            "source_polls":self.source_polls.load(Ordering::SeqCst),
            "write_chunks":self.write_chunks.load(Ordering::SeqCst)})
    }
    fn snapshots(&self) -> Vec<NativeBodySnapshot> {
        self.native
            .iter()
            .map(NativeBodyWitness::snapshot)
            .collect()
    }

    fn native_json(&self, snapshots: &[NativeBodySnapshot]) -> Value {
        let at = |instant: MonotonicInstant| {
            self.start
                .get()
                .and_then(|start| instant.checked_duration_since(*start))
                .map(micros)
        };
        json!(snapshots.iter().zip(["origin", "a", "b", "c"]).map(|(snapshot,role)| json!({
            "role":role,"attachments":snapshot.attachments,"preparations":snapshot.preparations,"invalid":snapshot.invalid,
            "acquisitions":snapshot.operation_acquisitions,"polls":snapshot.operation_polls,
            "ready_polls":snapshot.ready_operation_polls,"releases":snapshot.operation_releases,
            "heads":snapshot.heads,"body_acquisitions":snapshot.source_acquisitions,"body_releases":snapshot.source_releases,
            "reads":snapshot.reads,"ready_reads":snapshot.ready_reads,
            "first_pending":snapshot.first_operation_poll.map(|poll|poll.pending),
            "read_pending":snapshot.first_read.map(|read|read.pending),
            "released_at_us":snapshot.first_operation_release.and_then(at),
            "body_released_at_us":snapshot.first_release.and_then(at),
            "entered_at_us":snapshot.preparation.as_ref().and_then(|prepared|at(prepared.entered_at)),
            "budget_cause":snapshot.preparation.as_ref().map(|prepared|format!("{:?}",prepared.budget.cause)),
            "deadline_at_us":snapshot.preparation.as_ref().and_then(|prepared|at(prepared.budget.deadline.instant()))
        })).collect::<Vec<_>>())
    }

    fn emit_closed(&self) {
        let cleanup = self.cleanup.lock().ok().and_then(|value| value.clone());
        let terminal = self.terminal.lock().ok().and_then(|value| value.clone());
        let (Some(cleanup), Some(terminal)) = (cleanup, terminal) else {
            return;
        };
        if self.source_drops.load(Ordering::SeqCst) != 1
            || self.emitted.swap(true, Ordering::SeqCst)
        {
            return;
        }
        record(self, "closed", self.closed_evidence(cleanup, terminal));
    }

    fn record_native(&self, stage: &str, snapshots: &[NativeBodySnapshot]) {
        let native = self.native_json(snapshots);
        for (index, observation) in native.as_array().into_iter().flatten().enumerate() {
            record(
                self,
                &format!("{stage}-native-{index}"),
                observation.clone(),
            );
        }
    }
}

impl Drop for SourceLease {
    fn drop(&mut self) {
        self.0.source_drops.fetch_add(1, Ordering::SeqCst);
        self.0.emit_closed();
    }
}

fn collection_evidence(
    scope_closed: bool,
    origin_closed: &str,
    batch_closed: Option<bool>,
    stable: bool,
    after: &[NativeBodySnapshot],
) -> Value {
    json!({"scope_closed":scope_closed,"origin_closed":origin_closed,
        "batch_closed":batch_closed,"handles_retained":true,"polls_stable":stable,
        "after":after.iter().map(|snapshot|json!({"acquisitions":snapshot.operation_acquisitions,
            "polls":snapshot.operation_polls,"releases":snapshot.operation_releases,"reads":snapshot.reads,
            "body_acquisitions":snapshot.source_acquisitions,"body_releases":snapshot.source_releases,
            "invalid":snapshot.invalid})).collect::<Vec<_>>() })
}

impl Drop for Retained {
    fn drop(&mut self) {
        let before = self.witness.snapshots();
        let scope_closed = self.scope.check().is_err();
        self.witness.record_native("closed", &before);
        let mut origin = self.body.get_mut().take();
        let origin_closed = match origin.as_mut() {
            Some(body) => match body.next().now_or_never() {
                Some(Some(Err(EdgeError::Internal { .. }))) => "Internal",
                Some(None) => "Eof",
                _ => "Invalid",
            },
            None => "Missing",
        };
        let batch_closed = self.batch.get_mut().take().map(|batch| {
            batch
                .collect()
                .now_or_never()
                .is_some_and(|result| match result {
                    Err(failure) => {
                        matches!(failure.error(), EdgeError::Internal { .. })
                            && failure.slots().len() == 3
                            && failure
                                .slots()
                                .iter()
                                .all(|slot| matches!(slot, OutboundCollectedSlot::Unresolved))
                    }
                    Ok(_) => false,
                })
        });
        let after = self.witness.snapshots();
        let stable = before.iter().zip(&after).all(|(first, last)| {
            !last.invalid
                && first.operation_acquisitions == last.operation_acquisitions
                && first.operation_polls == last.operation_polls
                && first.reads == last.reads
                && first.operation_releases == last.operation_releases
                && first.source_acquisitions == last.source_acquisitions
                && first.source_releases == last.source_releases
                && first.heads == last.heads
        });
        if let Ok(mut cleanup) = self.witness.cleanup.lock() {
            *cleanup = Some(collection_evidence(
                scope_closed,
                origin_closed,
                batch_closed,
                stable,
                &after,
            ));
        }
        let evidence = self
            .witness
            .cleanup
            .lock()
            .ok()
            .and_then(|value| value.clone());
        if let Some(evidence) = evidence {
            record(&self.witness, "collected", evidence);
        }
        self.witness.emit_closed();
    }
}

pub(super) fn admission(
    path: &str,
    headers: &HeaderMap,
) -> Option<(ResponseEgressCompletion, IngressGrant)> {
    let case = path.strip_prefix("/progress/lifetimes/")?;
    if !CASES.contains(&case) {
        return None;
    }
    let token = field(headers, "x-request-token").ok()?.to_owned();
    if token.is_empty()
        || token.len() > 64
        || !token
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    {
        return None;
    }
    let isolated = match headers.get("x-fixture-cleanup-isolation") {
        None => false,
        Some(value) if value == "1" && case == CASES[0] => true,
        _ => return None,
    };
    let witness = Arc::new(Witness {
        case: case.to_owned(),
        token,
        isolated,
        native: (0..if isolated { 1 } else { 4 })
            .map(|_| NativeBodyWitness::new())
            .collect(),
        start: OnceLock::new(),
        clock: OnceLock::new(),
        source_drops: AtomicUsize::new(0),
        source_polls: AtomicUsize::new(0),
        write_chunks: AtomicUsize::new(0),
        cleanup: Mutex::new(None),
        terminal: Mutex::new(None),
        emitted: AtomicBool::new(false),
        prefix_produced: AtomicBool::new(false),
        pending_observed: AtomicBool::new(false),
    });
    let observed = Arc::clone(&witness);
    let completion = ResponseEgressCompletion::new(move |report| {
        if let Ok(mut terminal) = observed.terminal.lock() {
            *terminal = Some(
                json!({"outcome":format!("{:?}",report.outcome),"bytes_written":report.bytes_written,
                "body_kind":format!("{:?}",report.body_kind),"elapsed_us":micros(report.elapsed),
                "same_request_start":observed.start.get()==Some(&report.request_start),
                "terminal_at_us":observed.clock.get().zip(observed.start.get())
                    .and_then(|(clock,start)|clock.now().checked_duration_since(*start)).map(micros),
                "source_failure":report.source_failure.map(|failure|failure.category())}),
            );
        }
        observed.emit_closed();
    });
    Some((completion, IngressGrant::new(witness)))
}

async fn begin_source(state: &SourceState) -> Result<Bytes, EdgeError> {
    let pending = state
        .retained
        .origin
        .borrow_mut()
        .take()
        .ok_or_else(invalid)?;
    let response = pending.collect().await?;
    let mut body = response.into_body().into_stream().map_err(|_| invalid())?;
    poll_fn(|cx| {
        Poll::Ready(if body.as_mut().poll_next(cx).is_pending() {
            Ok(())
        } else {
            Err(invalid())
        })
    })
    .await?;
    *state.retained.body.borrow_mut() = Some(body);
    let native = state.retained.witness.snapshots();
    if !native
        .iter()
        .enumerate()
        .all(|(index, snapshot)| validate_native(snapshot, index == 0, false))
    {
        return Err(invalid());
    }
    state.retained.witness.record_native("live", &native);
    record(
        &state.retained.witness,
        "live",
        json!({"isolation":state.isolated,"source_drops":0}),
    );
    line(
        &json!({"schema":1,"case":state.probe.case,"token":state.probe.token,
        "phase":"pending","isolation":state.isolated,"qualification":false}),
    )
}

async fn healthy_work(state: &SourceState) -> Result<(), EdgeError> {
    let response = state
        .client
        .send(state.probe.request("post", &state.timeline)?)
        .await?;
    let status = response.status().as_u16();
    let bytes = response.into_bytes_bounded(64).await?;
    let native = state.retained.witness.snapshots();
    state.retained.witness.record_native("isolated", &native);
    record(
        &state.retained.witness,
        "isolated",
        json!({"scope_live":state.retained.scope.check().is_ok(),
            "origin_retained":state.retained.body.borrow().is_some(),
            "healthy":{"status":status,"bytes":bytes.to_vec()}}),
    );
    Ok(())
}

async fn after_prefix(state: &SourceState) -> Result<Bytes, EdgeError> {
    if state.probe.case != CASES[1] || state.isolated {
        let request = state.probe.request("ack", &state.timeline)?;
        if state.client.send(request).await?.status().as_u16() != 204 {
            return Err(invalid());
        }
    }
    if state.probe.case == CASES[3] {
        return Err(EdgeError::internal(std::io::Error::other(
            "controlled response cleanup source failure",
        )));
    }
    if state.isolated {
        healthy_work(state).await?;
    }
    if state.probe.case == CASES[1] {
        state
            .retained
            .witness
            .write_chunks
            .fetch_add(1, Ordering::SeqCst);
    }
    Ok(Bytes::from_static(b"done\n"))
}

async fn next_source(mut state: SourceState) -> Option<(Result<Bytes, EdgeError>, SourceState)> {
    let result = match state.phase {
        0 => begin_source(&state).await,
        1 if matches!(
            state.probe.case.as_str(),
            "response-owned-cleanup-write-deadline" | "response-owned-cleanup-cancel"
        ) =>
        {
            futures::future::pending::<()>().await;
            return None;
        }
        1 => after_prefix(&state).await,
        2..=65 if state.probe.case == CASES[1] => {
            state
                .retained
                .witness
                .write_chunks
                .fetch_add(1, Ordering::SeqCst);
            Ok(Bytes::from(vec![b'x'; 16 * 1024]))
        }
        _ => return None,
    };
    state.phase += 1;
    Some((result, state))
}

fn native_request(
    probe: &Probe,
    timeline: &Timeline,
    witness: &Witness,
    index: usize,
) -> Result<OutboundRequest, EdgeError> {
    let role = if witness.isolated {
        "isolated-origin"
    } else {
        *["origin", "a", "b", "c"].get(index).ok_or_else(invalid)?
    };
    let mut request = probe.request(role, timeline)?;
    if witness.isolated {
        request = request
            .timeout(Duration::from_secs(15))
            .stream_response()
            .max_decoded_response_bytes(64);
    }
    witness
        .native
        .get(index)
        .ok_or_else(invalid)?
        .attach(&mut request)?;
    Ok(request)
}

async fn prepare_source(state: &SourceState, ctx: &RequestContext) -> Result<(), EdgeError> {
    let request = native_request(&state.probe, &state.timeline, &state.retained.witness, 0)?;
    let origin = state.client.begin(request).await?;
    *state.retained.origin.borrow_mut() = Some(origin);
    if !state.isolated {
        state.timeline.wait_until(ctx, 34).await?;
        let requests = (1..4)
            .map(|index| {
                native_request(
                    &state.probe,
                    &state.timeline,
                    &state.retained.witness,
                    index,
                )
            })
            .collect::<Result<Vec<_>, EdgeError>>()?;
        *state.retained.batch.borrow_mut() = Some(
            state
                .client
                .start_batch_until(requests, state.timeline.deadline_at(534)),
        );
    }
    let ready = if state.isolated {
        "isolated-ready"
    } else {
        "ready"
    };
    if state
        .client
        .send(state.probe.request(ready, &state.timeline)?)
        .await?
        .status()
        .as_u16()
        != 204
    {
        return Err(invalid());
    }
    Ok(())
}

pub(super) async fn handle(ctx: RequestContext) -> Result<Response, EdgeError> {
    let case = ctx.path_params().get("case").ok_or_else(invalid)?;
    if !CASES.contains(&case) {
        return Err(invalid());
    }
    let probe = Probe::parse(case, ctx.headers())?;
    let client = ctx.http_client().ok_or_else(invalid)?;
    let witness = ctx
        .take_ingress_grant()
        .and_then(|grant| grant.downcast::<Arc<Witness>>().ok())
        .ok_or_else(invalid)?;
    witness
        .start
        .set(ctx.request_start())
        .map_err(|_| invalid())?;
    witness
        .clock
        .set(ctx.monotonic_clock())
        .map_err(|_| invalid())?;
    let isolated = witness.isolated;
    let timeline = Timeline::new(ctx.monotonic_clock(), ctx.request_start());
    let owner = OutboundScopeOwner::response_body();
    let scope = owner.scope();
    let retained = scope.provider_state(|| Retained {
        scope: scope.clone(),
        witness: Arc::clone(&witness),
        origin: RefCell::new(None),
        body: RefCell::new(None),
        batch: RefCell::new(None),
    })?;
    let state = SourceState {
        _lease: SourceLease(Arc::clone(&witness)),
        client,
        probe,
        retained,
        phase: 0,
        isolated,
        timeline,
    };
    owner.run(prepare_source(&state, &ctx)).await?;
    let deadline = state.probe.case == CASES[2];
    let source = stream::unfold(state, next_source);
    let mut source = source.boxed_local();
    let observed = Arc::clone(&witness);
    let source = stream::poll_fn(move |cx| {
        observed.source_polls.fetch_add(1, Ordering::SeqCst);
        let result = source.as_mut().poll_next(cx);
        if matches!(&result, Poll::Ready(Some(Ok(_)))) {
            observed.prefix_produced.store(true, Ordering::SeqCst);
        }
        if result.is_pending()
            && observed.prefix_produced.load(Ordering::SeqCst)
            && matches!(
                observed.case.as_str(),
                "response-owned-cleanup-cancel" | "response-owned-cleanup-write-deadline"
            )
            && !observed.pending_observed.swap(true, Ordering::SeqCst)
        {
            record(
                &observed,
                "source-pending",
                json!({"pending":true,"after_prefix":true}),
            );
        }
        result
    });
    let mut response = response_builder()
        .header("content-type", "application/x-ndjson")
        .body(Body::from_stream(source))
        .map_err(EdgeError::internal)?;
    if deadline {
        response
            .extensions_mut()
            .insert(ResponseEgressDeadline::after(Duration::from_millis(200)));
    }
    owner.attach(response)
}

fn validate_native(snapshot: &NativeBodySnapshot, origin: bool, closed: bool) -> bool {
    let Some(acquired) = snapshot.first_operation_acquisition else {
        return false;
    };
    let Some(first) = snapshot.first_operation_poll else {
        return false;
    };
    !snapshot.invalid
        && snapshot.attachments == 1
        && snapshot.preparations == 1
        && snapshot.operation_acquisitions == 1
        && snapshot.operation_polls > 0
        && snapshot.ready_operation_polls == u64::from(origin)
        && snapshot.operation_releases == u64::from(origin || closed)
        && first.started_at >= acquired
        && first.observed_at >= first.started_at
        && match snapshot.first_operation_release {
            Some(at) => (origin || closed) && at >= first.observed_at,
            None => !origin && !closed,
        }
        && snapshot.heads == u64::from(origin)
        && snapshot.source_acquisitions == u64::from(origin)
        && snapshot.source_releases == u64::from(origin && closed)
        && snapshot.ready_reads == 0
        && if origin {
            snapshot.reads > 0
                && snapshot
                    .first_read
                    .is_some_and(|read| read.pending && read.observed_at >= read.started_at)
                && if closed {
                    snapshot.first_release.is_some()
                } else {
                    snapshot.first_release.is_none()
                }
        } else {
            first.pending
                && snapshot.reads == 0
                && snapshot.first_read.is_none()
                && snapshot.first_release.is_none()
        }
}

#[cfg(test)]
mod tests {
    use super::*;
    use edgezero_core::outbound::test_support::{NativeBodyRead, NativeOperationPoll};

    #[test]
    fn isolated_request_prepares_only_one_independent_native_owner() {
        for isolated in [false, true] {
            let mut headers = HeaderMap::new();
            headers.insert(
                "x-request-token",
                "request_owned_resources".parse().expect("token"),
            );
            if isolated {
                headers.insert(
                    "x-fixture-cleanup-isolation",
                    "1".parse().expect("isolated"),
                );
            }
            let (_, grant) = admission(
                "/progress/lifetimes/response-owned-cleanup-complete",
                &headers,
            )
            .expect("admitted cleanup case");
            let witness = grant.downcast::<Arc<Witness>>().expect("typed witness");
            assert_eq!(witness.native.len(), if isolated { 1 } else { 4 });
            headers.insert("x-fixture-backend", "http://127.0.0.1:3/lifetimes/request_owned_resources/response-owned-cleanup-complete".parse().expect("backend"));
            let probe = Probe::parse(CASES[0], &headers).expect("probe");
            let at = MonotonicInstant::now();
            let timeline = Timeline::new(MonotonicClock::new(move || at), at);
            for index in 0..witness.native.len() {
                let request =
                    native_request(&probe, &timeline, &witness, index).expect("prepared request");
                assert_eq!(request.is_stream_response(), index == 0);
                assert_eq!(
                    request.into_parts().timeout,
                    Some(Duration::from_secs(if index == 0 { 15 } else { 1 }))
                );
            }
        }
    }

    fn native(origin: bool, closed: bool) -> NativeBodySnapshot {
        let at = MonotonicInstant::now();
        NativeBodySnapshot {
            attachments: 1,
            preparations: 1,
            operation_acquisitions: 1,
            operation_polls: 1,
            operation_releases: u64::from(origin || closed),
            ready_operation_polls: u64::from(origin),
            heads: u64::from(origin),
            source_acquisitions: u64::from(origin),
            source_releases: u64::from(origin && closed),
            reads: u64::from(origin),
            first_operation_acquisition: Some(at),
            first_operation_release: (origin || closed).then_some(at),
            first_operation_poll: Some(NativeOperationPoll {
                started_at: at,
                observed_at: at,
                pending: !origin,
            }),
            first_read: origin.then_some(NativeBodyRead {
                started_at: at,
                observed_at: at,
                pending: true,
            }),
            first_release: (origin && closed).then_some(at),
            ..NativeBodySnapshot::default()
        }
    }

    #[test]
    fn cleanup_requires_actual_sdk_pending_operation_and_original_body_source() {
        for origin in [false, true] {
            let live = native(origin, false);
            let closed = native(origin, true);
            assert!(validate_native(&live, origin, false));
            assert!(validate_native(&closed, origin, true));
            assert!(
                !validate_native(&live, origin, true),
                "missing native release"
            );
            for mutation in 0..6 {
                let mut invalid = closed.clone();
                match mutation {
                    0 => invalid.operation_acquisitions = 0,
                    1 => invalid.operation_polls = 0,
                    2 => invalid.invalid = true,
                    3 => invalid.operation_releases = 2,
                    4 if origin => invalid.first_read = None,
                    4 => invalid.heads = 1,
                    _ if origin => invalid.first_read.as_mut().expect("read").pending = false,
                    _ => invalid.first_operation_poll.as_mut().expect("poll").pending = false,
                }
                assert!(
                    !validate_native(&invalid, origin, true),
                    "mutation {mutation}"
                );
            }
        }
    }

    #[test]
    fn full_encoded_native_and_closed_records_keep_the_existing_two_kib_bound() {
        let mut headers = HeaderMap::new();
        headers.insert("x-request-token", "t".repeat(64).parse().expect("token"));
        let (_, grant) = admission(
            "/progress/lifetimes/response-owned-cleanup-write-deadline",
            &headers,
        )
        .expect("admission");
        let witness = grant
            .downcast::<Arc<Witness>>()
            .expect("request-owned witness");
        witness.start.set(MonotonicInstant::now()).expect("start");
        witness.source_polls.store(usize::MAX, Ordering::SeqCst);
        let snapshot = NativeBodySnapshot {
            attachments: u64::MAX,
            preparations: u64::MAX,
            operation_acquisitions: u64::MAX,
            operation_polls: u64::MAX,
            operation_releases: u64::MAX,
            ready_operation_polls: u64::MAX,
            source_acquisitions: u64::MAX,
            source_releases: u64::MAX,
            reads: u64::MAX,
            ready_reads: u64::MAX,
            heads: u64::MAX,
            ..native(true, true)
        };
        let snapshots: [NativeBodySnapshot; 4] = std::array::from_fn(|_| snapshot.clone());
        let native = witness.native_json(&snapshots);
        for (index, value) in native
            .as_array()
            .expect("four SDK owners")
            .iter()
            .enumerate()
        {
            encoded_record(&witness, &format!("closed-native-{index}"), value.clone())
                .expect("bounded SDK record");
        }
        let cleanup = collection_evidence(true, "Internal", Some(true), true, &snapshots);
        let terminal = json!({"outcome":"DeadlineExceeded","bytes_written":u64::MAX,"body_kind":"Application",
            "elapsed_us":u64::MAX,"same_request_start":true,"terminal_at_us":u64::MAX,"source_failure":null});
        let full = witness.closed_evidence(cleanup.clone(), terminal);
        encoded_record(&witness, "collected", cleanup).expect("bounded collection record");
        encoded_record(&witness, "closed", full).expect("bounded actual closed envelope");
        assert!(encoded_record(&witness, "closed", json!({"oversize":"x".repeat(2049)})).is_err());
    }
}
