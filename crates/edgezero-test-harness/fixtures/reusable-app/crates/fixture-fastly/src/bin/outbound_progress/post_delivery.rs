use edgezero_adapter_fastly::{
    FASTLY_PLATFORM,
    delivery::{FastlyRequestDelivery, FastlyResponseCommitState},
    lifecycle::run_custom,
    outbound::FastlyOutboundClient,
    request::FastlyService,
};
use edgezero_core::{
    Body, Deadline, EdgeError, HttpClient, MonotonicInstant, OutboundBatchTermination,
    OutboundCollectedSlot, OutboundHttpClient, OutboundRequest, OutboundScope, OutboundTime,
    ResponseEgressOutcome, action,
    app::App,
    http::{Response, Uri},
    outbound::test_support::NativeBodyWitness,
    response_egress::{ResponseEgressObserver, ResponseEgressReport},
    router::RouterService,
};
use fixture_core::qualification::progress::probe_failure;
use futures::executor::block_on;
use serde_json::{Value, json};
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

struct Reports(Arc<AtomicUsize>);

impl ResponseEgressObserver for Reports {
    fn complete(&self, _: &ResponseEgressReport) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

fn check(valid: bool) -> Result<(), EdgeError> {
    if valid { Ok(()) } else { Err(probe_failure()) }
}

fn at(start: MonotonicInstant, instant: MonotonicInstant) -> Result<u64, EdgeError> {
    let elapsed = instant
        .checked_duration_since(start)
        .ok_or_else(probe_failure)?;
    u64::try_from(elapsed.as_micros()).map_err(EdgeError::internal)
}

fn deadline(start: MonotonicInstant, millis: u64) -> Result<Deadline, EdgeError> {
    start
        .checked_add(Duration::from_millis(millis))
        .map(Deadline::at_instant)
        .ok_or_else(probe_failure)
}

#[action]
async fn placeholder() -> Result<Response, EdgeError> {
    Ok(Response::new(Body::empty()))
}

pub(super) fn run(request: fastly::Request) -> Result<(), fastly::Error> {
    let token = request
        .get_header_str("x-request-token")
        .ok_or_else(probe_failure)?
        .to_owned();
    let base: Uri = request
        .get_header_str("x-fixture-backend")
        .ok_or_else(probe_failure)?
        .parse()
        .map_err(|_| probe_failure())?;
    check(
        !token.is_empty()
            && token.len() <= 64
            && token
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_')),
    )?;
    check(
        fixture_core::is_loopback_http(&base)
            && base.port_u16().is_some_and(|port| port != 0)
            && base
                .authority()
                .is_some_and(|host| !host.as_str().contains('@'))
            && base.query().is_none()
            && base.path() == format!("/lifetimes/{token}/batch-progress"),
    )?;
    let provider = Arc::new(FastlyOutboundClient::default());
    let time = provider.time();
    let client = HttpClient::new(provider.clone());
    let reports = Arc::new(AtomicUsize::new(0));
    let witnesses = std::array::from_fn::<_, 3, _>(|_| NativeBodyWitness::new());
    let mut callback = None;
    let mut evidence = Value::Null;
    let result = run_custom::<(), _, _>(request, |request, _| {
        let scope = OutboundScope::capture(provider.progress());
        callback = Some(scope.clone());
        let mut app = App::with_name_and_platform(
            RouterService::builder()
                .get("/progress/lifetimes/post-delivery-batch", placeholder)
                .build(),
            "post-delivery-batch-probe",
            FASTLY_PLATFORM,
        );
        app.set_monotonic_clock(time.clock().clone());
        app.set_response_egress_observer(Reports(Arc::clone(&reports)));
        let mut bytes = serde_json::to_vec(&json!({"schema":1,"case":"post-delivery-batch",
            "token":token,"qualification":false}))
        .map_err(EdgeError::internal)?;
        bytes.push(b'\n');
        check(bytes.len() <= 512)?;
        let length = bytes.len() as u64;
        let delivery = FastlyService::new(&app).send_request_with_hooks(
            request,
            |_, extensions| {
                extensions.insert(client.clone());
            },
            |output| {
                *output = Response::new(Body::from(bytes));
                output.headers_mut().insert(
                    "content-type",
                    edgezero_core::http::HeaderValue::from_static("application/json"),
                );
            },
        );
        check(
            matches!(delivery, FastlyRequestDelivery::Routed { .. })
                && !delivery.may_send_native_response(),
        )?;
        let report = delivery.delivery().ok_or_else(probe_failure)?;
        check(
            report.error().is_none()
                && report.commitment() == FastlyResponseCommitState::Committed
                && report.report().outcome == ResponseEgressOutcome::Completed
                && report.report().fallback_disposition.is_none()
                && report.report().source_failure.is_none()
                && report.report().framing_failures.application().is_none()
                && report.report().framing_failures.fallback().is_none()
                && report.report().bytes_written == length
                && reports.load(Ordering::SeqCst) == 1,
        )?;
        let start = report.report().request_start;
        let handoff = at(start, time.clock().now())?;
        let requests = witnesses
            .iter()
            .enumerate()
            .map(|(index, witness)| {
                let mut request =
                    OutboundRequest::get(format!("{base}/{}", ["a", "b", "c"][index]))?
                        .timeout(Duration::from_secs(1))
                        .max_response_bytes(64);
                witness.attach(&mut request)?;
                Ok(request)
            })
            .collect::<Result<Vec<_>, EdgeError>>()?;
        wait(&scope, &time, start, 34)?;
        let requests = requests
            .into_iter()
            .map(|request| {
                request.header(
                    "x-fixture-at-us",
                    at(start, time.clock().now())?.to_string(),
                )
            })
            .collect::<Result<Vec<_>, EdgeError>>()?;
        let batch = client.start_batch_until(requests, deadline(start, 534)?);
        wait(&scope, &time, start, 450)?;
        let collecting = at(start, time.clock().now())?;
        // Snapshot while the original batch and all of its buffered outcomes are still retained.
        let snapshots = witnesses
            .iter()
            .map(NativeBodyWitness::snapshot)
            .collect::<Vec<_>>();
        let batch_entered = snapshots
            .first()
            .and_then(|snapshot| snapshot.preparation.as_ref())
            .ok_or_else(probe_failure)?
            .entered_at;
        let batch_at = at(start, batch_entered)?;
        let collected = block_on(batch.collect()).map_err(|failure| failure.into_parts().0)?;
        let collected_at = at(start, time.clock().now())?;
        let (slots, termination) = collected.into_parts();
        check(termination == OutboundBatchTermination::Completed && slots.len() == 3)?;
        let mut results = Vec::with_capacity(3);
        for (index, (slot, snapshot)) in slots.into_iter().zip(snapshots).enumerate() {
            check(
                !snapshot.invalid
                    && snapshot.attachments == 1
                    && snapshot.preparations == 1
                    && snapshot.heads == 1
                    && snapshot.source_acquisitions == 1
                    && snapshot.source_releases == 1,
            )?;
            let prepared = snapshot.preparation.as_ref().ok_or_else(probe_failure)?;
            check(
                prepared.entered_at == batch_entered
                    && prepared.budget.deadline.instant() == deadline(start, 534)?.instant()
                    && prepared.budget.cause == edgezero_core::BudgetSource::BatchCutoff,
            )?;
            let released = at(start, snapshot.first_release.ok_or_else(probe_failure)?)?;
            check(released < collecting)?;
            let OutboundCollectedSlot::Resolved(slot) = slot else {
                return Err(probe_failure().into());
            };
            let elapsed = u64::try_from(slot.elapsed.as_micros()).map_err(EdgeError::internal)?;
            let response = slot.outcome?;
            check(response.status().as_u16() == 200)?;
            let bytes = block_on(response.into_bytes_bounded(64))?;
            check(bytes.as_ref() == [b'A' + index as u8])?;
            results.push(
                json!({"index":index,"elapsed_us":elapsed,"bytes":bytes.as_ref(),
                "heads":snapshot.heads,"acquired":snapshot.source_acquisitions,
                "released":snapshot.source_releases,"release_at_us":released}),
            );
        }
        check(scope.check().is_ok() && reports.load(Ordering::SeqCst) == 1)?;
        evidence = json!({"handoff_at_us":handoff,"batch_at_us":batch_at,"cutoff_at_us":534000,
            "collect_started_us":collecting,"collected_at_us":collected_at,"termination":"Completed",
            "callback_scope_live":true,"same_client":true,"slots":results});
        Ok(())
    });
    let released_after_return = witnesses
        .iter()
        .map(|witness| witness.snapshot().source_releases)
        .collect::<Vec<_>>();
    let record = serde_json::to_string(&json!({"schema":1,"event":"post-delivery-batch","token":token,
        "qualification":false,"callback_scope_closed":callback.is_some_and(|scope| scope.check().is_err()),
        "released_after_return":released_after_return,
        "terminal_reports":reports.load(Ordering::SeqCst),"result":if result.is_ok(){"Ok"}else{"Err"},
        "evidence":evidence})).map_err(EdgeError::internal)?;
    check(record.len() <= 2048)?;
    eprintln!("edgezero-lifetime-cleanup {record}");
    result
}

fn wait(
    scope: &OutboundScope,
    time: &OutboundTime,
    start: MonotonicInstant,
    millis: u64,
) -> Result<(), EdgeError> {
    block_on(scope.run(
        time.clone(),
        start,
        time.wait_until(deadline(start, millis)?),
    ))
}
