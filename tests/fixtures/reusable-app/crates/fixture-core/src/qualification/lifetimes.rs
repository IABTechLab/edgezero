use edgezero_core::context::RequestContext;
use edgezero_core::http::{HeaderMap, Method, Response, Uri, response_builder};
use edgezero_core::{
    Body, Deadline, EdgeError, HttpClient, MonotonicClock, MonotonicInstant, OutboundBatch,
    OutboundBatchCheck, OutboundBatchNext, OutboundCollectedSlot, OutboundRequest,
    OutboundResponse, OutboundSlotResult, action,
};
use serde_json::{Value, json};
use std::time::Duration;

const MAX_CPU_ITERATIONS: u64 = 10_000_000;

struct Probe {
    case: String,
    token: String,
    base: Uri,
}

struct Timeline {
    clock: MonotonicClock,
    start: MonotonicInstant,
    events: Vec<Value>,
}

#[derive(Default)]
struct Report {
    items: Vec<Value>,
    slots: Vec<&'static str>,
    termination: Option<String>,
    error: Option<Value>,
    origin: Option<Value>,
    mediator: Option<Value>,
}

impl Probe {
    fn parse(case: &str, headers: &HeaderMap) -> Result<Self, EdgeError> {
        if !matches!(
            case,
            "slow-origin"
                | "timeout-before-cutoff"
                | "fast-origin"
                | "begin-progress"
                | "batch-progress"
                | "pending-head-cutoff"
                | "pending-body-cutoff"
                | "cpu-stretch"
        ) {
            return Err(invalid());
        }
        let token = field(headers, "x-request-token")?;
        if token.is_empty()
            || token.len() > 64
            || !token
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
        {
            return Err(invalid());
        }
        let target = field(headers, "x-fixture-backend")?;
        if target.len() > 2048 || target.contains('#') {
            return Err(invalid());
        }
        let base: Uri = target.parse().map_err(|_| invalid())?;
        if !crate::is_loopback_http(&base)
            || !base.port_u16().is_some_and(|port| port != 0)
            || base
                .authority()
                .is_some_and(|authority| authority.as_str().contains('@'))
            || base.query().is_some()
            || base.path() != format!("/lifetimes/{token}/{case}")
        {
            return Err(invalid());
        }
        Ok(Self {
            case: case.to_owned(),
            token: token.to_owned(),
            base,
        })
    }

    fn request(&self, role: &str, timeline: &Timeline) -> Result<OutboundRequest, EdgeError> {
        let uri = format!("{}/{role}", self.base)
            .parse()
            .map_err(|_| invalid())?;
        let timeout = match role {
            "origin" => Duration::from_secs(15),
            "d" if self.case == "timeout-before-cutoff" => Duration::from_millis(300),
            _ => Duration::from_secs(1),
        };
        let mut request = OutboundRequest::new(Method::GET, uri)?
            .timeout(timeout)
            .max_response_bytes(64)
            .header(
                "x-fixture-at-us",
                timeline.elapsed_us(timeline.clock.now()).to_string(),
            )?;
        if role == "origin" {
            request = request.stream_response().max_decoded_response_bytes(64);
        }
        Ok(request)
    }

    fn calibration_request(
        &self,
        timeline: &Timeline,
        head_observed: MonotonicInstant,
    ) -> Result<OutboundRequest, EdgeError> {
        self.request("calibration", timeline)?
            .header(
                "x-fixture-origin-head-us",
                timeline.elapsed_us(head_observed).to_string(),
            )
            .map(|request| request.deadline(timeline.deadline_at(520)))
    }
}

fn invalid() -> EdgeError {
    EdgeError::bad_request("invalid lifetime probe")
}

fn field<'a>(headers: &'a HeaderMap, name: &str) -> Result<&'a str, EdgeError> {
    let mut values = headers.get_all(name).iter();
    let value = values.next().ok_or_else(invalid)?;
    if values.next().is_some() {
        return Err(invalid());
    }
    value.to_str().map_err(|_| invalid())
}

impl Timeline {
    fn new(clock: MonotonicClock, start: MonotonicInstant) -> Self {
        Self {
            clock,
            start,
            events: Vec::new(),
        }
    }

    fn elapsed_us(&self, at: MonotonicInstant) -> u64 {
        micros(at.saturating_duration_since(self.start))
    }

    fn remaining(&self, millis: u64) -> Duration {
        (self.start + Duration::from_millis(millis)).saturating_duration_since(self.clock.now())
    }

    fn deadline_at(&self, millis: u64) -> Deadline {
        Deadline::at_instant(self.start + Duration::from_millis(millis))
    }

    fn event(&mut self, op: &str, at: MonotonicInstant, ended: MonotonicInstant, result: Value) {
        let started_us = self.elapsed_us(at);
        self.events.push(json!({"op":op,"at_us":started_us,
            "duration_us":self.elapsed_us(ended).saturating_sub(started_us),"result":result}));
    }

    async fn wait_until(&self, ctx: &RequestContext, millis: u64) -> Result<(), EdgeError> {
        let remaining = self.remaining(millis);
        if !remaining.is_zero() {
            super::progress::pause(ctx, remaining)?.await;
        }
        if !self.remaining(millis).is_zero() {
            return Err(EdgeError::internal(std::io::Error::other(
                "lifetime timer returned before its target",
            )));
        }
        Ok(())
    }
}

fn micros(duration: Duration) -> u64 {
    u64::try_from(duration.as_micros()).unwrap_or(u64::MAX)
}

impl Report {
    fn json(&self, probe: &Probe, timeline: &Timeline) -> Value {
        json!({"schema":1,"case":probe.case,"token":probe.token,"qualification":false,
            "events":timeline.events,"items":self.items,"slots":self.slots,
            "termination":self.termination,"error":self.error,"origin":self.origin,"mediator":self.mediator})
    }
}

fn error_report(error: &EdgeError) -> Value {
    let reason = match error {
        EdgeError::GatewayTimeout { cause, .. } => Some(format!("{cause:?}")),
        EdgeError::BadGateway { reason, .. } => Some(format!("{reason:?}")),
        EdgeError::ResponseTooLarge { reason, .. } => Some(format!("{reason:?}")),
        _ => None,
    };
    json!({"kind":error.kind(),"status":error.status().as_u16(),"reason":reason})
}

async fn response_report(response: Result<OutboundResponse, EdgeError>) -> Value {
    let outcome = match response {
        Ok(response) => {
            let status = response.status().as_u16();
            response
                .into_bytes_bounded(64)
                .await
                .map(|bytes| json!({"status":status,"bytes":bytes.to_vec()}))
        }
        Err(error) => Err(error),
    };
    outcome.unwrap_or_else(|error| json!({"error":error_report(&error)}))
}

async fn record_item(report: &mut Report, index: usize, slot: OutboundSlotResult, via: &str) {
    report.items.push(
        json!({"index":index,"via":via,"elapsed_us":micros(slot.elapsed),
        "outcome":response_report(slot.outcome).await}),
    );
}

async fn record_slots(report: &mut Report, slots: Vec<OutboundCollectedSlot>) {
    for (index, slot) in slots.into_iter().enumerate() {
        let state = match slot {
            OutboundCollectedSlot::AlreadyDelivered => "AlreadyDelivered",
            OutboundCollectedSlot::Unresolved => "Unresolved",
            OutboundCollectedSlot::Resolved(slot) => {
                record_item(report, index, slot, "collect").await;
                "Resolved"
            }
        };
        report.slots.push(state);
    }
}

async fn collect(batch: OutboundBatch, report: &mut Report, timeline: &mut Timeline) {
    let at = timeline.clock.now();
    let result = batch.collect().await;
    let ended = timeline.clock.now();
    timeline.event(
        "collect",
        at,
        ended,
        json!(if result.is_ok() { "ok" } else { "error" }),
    );
    let slots = match result {
        Ok(results) => {
            let (slots, termination) = results.into_parts();
            report.termination = Some(format!("{termination:?}"));
            slots
        }
        Err(failure) => {
            let (error, slots) = failure.into_parts();
            report.error = Some(error_report(&error));
            slots
        }
    };
    record_slots(report, slots).await;
}

async fn check(
    batch: &mut OutboundBatch,
    report: &mut Report,
    timeline: &mut Timeline,
) -> Result<bool, EdgeError> {
    let at = timeline.clock.now();
    let result = batch.check();
    let ended = timeline.clock.now();
    timeline.event(
        "check",
        at,
        ended,
        json!(match &result {
            Ok(OutboundBatchCheck::Item(_)) => "Item",
            Ok(OutboundBatchCheck::Finished(_)) => "Finished",
            Ok(OutboundBatchCheck::Pending) => "Pending",
            Err(_) => "error",
        }),
    );
    match result? {
        OutboundBatchCheck::Item(item) => {
            let (index, slot) = item.into_parts();
            record_item(report, index, slot, "check").await;
            Ok(true)
        }
        OutboundBatchCheck::Finished(_) | OutboundBatchCheck::Pending => Ok(false),
    }
}

async fn next(batch: &mut OutboundBatch, report: &mut Report, timeline: &mut Timeline) {
    let at = timeline.clock.now();
    let result = batch.next().await;
    let ended = timeline.clock.now();
    timeline.event(
        "next",
        at,
        ended,
        json!(match &result {
            Ok(OutboundBatchNext::Item(_)) => "Item",
            Ok(OutboundBatchNext::Finished(_)) => "Finished",
            Err(_) => "error",
        }),
    );
    match result {
        Ok(OutboundBatchNext::Item(item)) => {
            let (index, slot) = item.into_parts();
            record_item(report, index, slot, "next").await;
        }
        Ok(OutboundBatchNext::Finished(_)) => {}
        Err(error) => report.error = Some(error_report(&error)),
    }
}

fn busy_until(
    clock: &MonotonicClock,
    deadline: MonotonicInstant,
    max_iterations: u64,
) -> Result<u64, EdgeError> {
    let mut previous = clock.now();
    if previous >= deadline {
        return Err(EdgeError::internal(std::io::Error::other(
            "CPU probe window already expired",
        )));
    }
    let mut work = 0_u64;
    for iterations in 0..max_iterations {
        let now = clock.now();
        if now < previous {
            return Err(EdgeError::internal(std::io::Error::other(
                "CPU probe clock moved backwards",
            )));
        }
        if now >= deadline {
            return Ok(iterations);
        }
        previous = now;
        work = std::hint::black_box(work.wrapping_mul(1_664_525).wrapping_add(1_013_904_223));
    }
    Err(EdgeError::internal(std::io::Error::other(
        "CPU probe exhausted its finite work bound",
    )))
}

async fn run(
    ctx: &RequestContext,
    probe: &Probe,
    client: &HttpClient,
    report: &mut Report,
    timeline: &mut Timeline,
) -> Result<(), EdgeError> {
    if matches!(
        probe.case.as_str(),
        "pending-head-cutoff" | "pending-body-cutoff"
    ) {
        return pending_cutoff(ctx, probe, client, report, timeline).await;
    }
    let origin = if probe.case == "batch-progress" {
        None
    } else {
        timeline.wait_until(ctx, 2).await?;
        let request = probe.request("origin", timeline)?;
        let at = timeline.clock.now();
        let result = client.begin(request).await;
        let ended = timeline.clock.now();
        timeline.event(
            "begin",
            at,
            ended,
            json!(if result.is_ok() { "scheduled" } else { "error" }),
        );
        Some(result?)
    };
    if probe.case == "begin-progress" {
        let at = timeline.clock.now();
        timeline.event("quiet-start", at, at, Value::Null);
        super::progress::pause(ctx, Duration::from_millis(100))?.await;
        let at = timeline.clock.now();
        timeline.event("quiet-end", at, at, Value::Null);
        let at = timeline.clock.now();
        let result = origin.ok_or_else(invalid)?.collect().await;
        let ended = timeline.clock.now();
        timeline.event(
            "origin-head",
            at,
            ended,
            json!(if result.is_ok() { "ok" } else { "error" }),
        );
        report.origin = Some(response_report(result).await);
        return Ok(());
    }

    timeline.wait_until(ctx, 34).await?;
    let requests = ["a", "b", "c"]
        .into_iter()
        .chain((probe.case == "timeout-before-cutoff").then_some("d"))
        .map(|role| probe.request(role, timeline))
        .collect::<Result<Vec<_>, _>>()?;
    let max_checks = requests.len() + 1;
    let observation_cutoff = timeline.deadline_at(534);
    let at = timeline.clock.now();
    let mut batch = client.start_batch_until(requests, observation_cutoff);
    let ended = timeline.clock.now();
    timeline.event(
        "batch-start",
        at,
        ended,
        if probe.case == "cpu-stretch" {
            json!({"cutoff_at_us":timeline.elapsed_us(observation_cutoff.instant())})
        } else {
            Value::Null
        },
    );

    if probe.case == "batch-progress" {
        let request = probe.request("quiet", timeline)?;
        let at = timeline.clock.now();
        timeline.event("quiet-start", at, at, Value::Null);
        let result = client.send(request).await;
        let at = timeline.clock.now();
        timeline.event("quiet-end", at, at, Value::Null);
        let quiet = response_report(result).await;
        if let Some(error) = quiet.get("error") {
            report.error = Some(error.clone());
        }
        collect(batch, report, timeline).await;
        return Ok(());
    }

    let at = timeline.clock.now();
    let result = origin.ok_or_else(invalid)?.collect().await;
    let ended = timeline.clock.now();
    timeline.event(
        "origin-head",
        at,
        ended,
        json!(if result.is_ok() { "ok" } else { "error" }),
    );
    let response = result?;
    if probe.case == "cpu-stretch" {
        let at = timeline.clock.now();
        report.origin = Some(response_report(Ok(response)).await);
        timeline.event("origin-body", at, timeline.clock.now(), Value::Null);
        let request = probe.calibration_request(timeline, ended)?;
        let head_observed_us = timeline.elapsed_us(ended);
        let at = timeline.clock.now();
        let result = client.send(request).await;
        let ended = timeline.clock.now();
        let outcome = response_report(result).await;
        let failure = outcome.get("error").cloned();
        timeline.event(
            "calibration-send",
            at,
            ended,
            json!({"origin_head_observed_us":head_observed_us,"outcome":outcome}),
        );
        if let Some(error) = failure {
            report.error = Some(error);
            return Ok(());
        }
        timeline.wait_until(ctx, 500).await?;
        next(&mut batch, report, timeline).await;
        timeline.wait_until(ctx, 520).await?;
        let at = timeline.clock.now();
        let result = busy_until(
            &timeline.clock,
            timeline.deadline_at(560).instant(),
            MAX_CPU_ITERATIONS,
        );
        let ended = timeline.clock.now();
        timeline.event(
            "cpu-stretch",
            at,
            ended,
            json!({"iterations":result.as_ref().ok(),"max_iterations":MAX_CPU_ITERATIONS}),
        );
        result?;
        collect(batch, report, timeline).await;
        return Ok(());
    }
    if matches!(probe.case.as_str(), "slow-origin" | "timeout-before-cutoff") {
        timeline.wait_until(ctx, 701).await?;
        // One check per slot plus terminal observation; never spin on Pending.
        for _ in 0..max_checks {
            match check(&mut batch, report, timeline).await {
                Ok(true) => {}
                Ok(false) => break,
                Err(error) => {
                    report.error = Some(error_report(&error));
                    break;
                }
            }
        }
        collect(batch, report, timeline).await;
        let at = timeline.clock.now();
        report.origin = Some(response_report(Ok(response)).await);
        let ended = timeline.clock.now();
        timeline.event("origin-body", at, ended, Value::Null);
        return Ok(());
    }

    let at = timeline.clock.now();
    report.origin = Some(response_report(Ok(response)).await);
    let ended = timeline.clock.now();
    timeline.event("origin-body", at, ended, Value::Null);
    if let Err(error) = check(&mut batch, report, timeline).await {
        report.error = Some(error_report(&error));
    }
    next(&mut batch, report, timeline).await;
    collect(batch, report, timeline).await;

    let request = probe
        .request("mediator", timeline)?
        .deadline(observation_cutoff);
    let at = timeline.clock.now();
    let remaining = observation_cutoff.instant().saturating_duration_since(at);
    let result = client.send(request).await;
    let ended = timeline.clock.now();
    timeline.event(
        "mediator-send",
        at,
        ended,
        json!(if result.is_ok() { "ok" } else { "error" }),
    );
    let mut mediator =
        json!({"sent_at_us":timeline.elapsed_us(at),"remaining_us":micros(remaining),"error":null});
    match result {
        Ok(response) => mediator["outcome"] = response_report(Ok(response)).await,
        Err(error) => mediator["error"] = error_report(&error),
    }
    report.mediator = Some(mediator);
    Ok(())
}

async fn pending_cutoff(
    ctx: &RequestContext,
    probe: &Probe,
    client: &HttpClient,
    report: &mut Report,
    timeline: &mut Timeline,
) -> Result<(), EdgeError> {
    use std::future::Future;

    timeline.wait_until(ctx, 34).await?;
    let requests = ["a", "b", "c"]
        .into_iter()
        .map(|role| probe.request(role, timeline))
        .collect::<Result<Vec<_>, _>>()?;
    let observation_cutoff = timeline.deadline_at(200);
    let at = timeline.clock.now();
    let mut batch = client.start_batch_until(requests, observation_cutoff);
    let ended = timeline.clock.now();
    timeline.event(
        "batch-start",
        at,
        ended,
        json!({"cutoff_at_us":timeline.elapsed_us(observation_cutoff.instant())}),
    );
    let at = timeline.clock.now();
    let mut first_poll = None;
    let mut pending_polls = 0_u64;
    let (result, ended) = {
        let mut next = std::pin::pin!(batch.next());
        let result = futures::future::poll_fn(|context| {
            first_poll.get_or_insert_with(|| timeline.elapsed_us(timeline.clock.now()));
            let outcome = next.as_mut().poll(context);
            if outcome.is_pending() {
                pending_polls = pending_polls.saturating_add(1);
            }
            outcome
        })
        .await;
        (result, timeline.clock.now())
    };
    let state = match &result {
        Ok(OutboundBatchNext::Finished(termination)) => format!("{termination:?}"),
        Ok(OutboundBatchNext::Item(_)) => "Item".to_owned(),
        Err(_) => "error".to_owned(),
    };
    timeline.event(
        "next",
        at,
        ended,
        json!({"state":state,"first_poll_us":first_poll,
        "pending_polls":pending_polls}),
    );
    match result {
        Ok(OutboundBatchNext::Item(item)) => {
            let (index, slot) = item.into_parts();
            record_item(report, index, slot, "next").await;
        }
        Ok(OutboundBatchNext::Finished(_)) => {}
        Err(error) => report.error = Some(error_report(&error)),
    }
    collect(batch, report, timeline).await;
    Ok(())
}

#[action]
pub(super) async fn probe(ctx: RequestContext) -> Result<Response, EdgeError> {
    let case = ctx.path_params().get("case").ok_or_else(invalid)?;
    let probe = Probe::parse(case, ctx.headers())?;
    let client = ctx.http_client().ok_or_else(|| {
        EdgeError::internal(std::io::Error::other(
            "lifetime probe has no injected client",
        ))
    })?;
    let mut timeline = Timeline::new(ctx.monotonic_clock(), ctx.request_start());
    let mut report = Report::default();
    if let Err(error) = run(&ctx, &probe, &client, &mut report, &mut timeline).await
        && report.error.is_none()
    {
        report.error = Some(error_report(&error));
    }
    response_builder()
        .header("content-type", "application/json")
        .body(Body::json(&report.json(&probe, &timeline)).map_err(EdgeError::internal)?)
        .map_err(EdgeError::internal)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::outbound_test_support::TestTime;
    use bytes::Bytes;
    use edgezero_core::http::{Method, StatusCode, request_builder};
    use edgezero_core::{Body, BudgetSource, ContentTypeProvenance, ResponseMode};
    use futures::{StreamExt, executor::block_on};
    use std::sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    };
    use std::{cell::Cell, rc::Rc};

    #[test]
    fn cpu_work_stops_at_the_original_target_without_yielding() {
        let start = TestTime::default().clock().now();
        let calls = Arc::new(AtomicU64::new(0));
        let observed = Arc::clone(&calls);
        let clock = MonotonicClock::new(move || {
            start + Duration::from_millis(520 + observed.fetch_add(1, Ordering::SeqCst))
        });
        assert_eq!(
            busy_until(&clock, start + Duration::from_millis(560), 1000).unwrap(),
            39
        );
        assert_eq!(calls.load(Ordering::SeqCst), 41);
    }

    #[test]
    fn cpu_work_is_finite_when_time_is_frozen_or_backwards() {
        for backwards in [false, true] {
            let start = TestTime::default().clock().now();
            let calls = Arc::new(AtomicU64::new(0));
            let observed = Arc::clone(&calls);
            let clock = MonotonicClock::new(move || {
                let count = observed.fetch_add(1, Ordering::SeqCst);
                start + Duration::from_millis(520 + u64::from(backwards && count == 1))
            });
            let error = busy_until(&clock, start + Duration::from_millis(560), 8).unwrap_err();
            assert_eq!(error.status(), StatusCode::INTERNAL_SERVER_ERROR);
            assert_eq!(calls.load(Ordering::SeqCst), if backwards { 3 } else { 9 });
        }
    }

    #[test]
    fn cpu_work_rejects_an_already_expired_window() {
        let time = TestTime::default();
        let clock = time.clock();
        let target = clock.now();
        assert_eq!(
            busy_until(&clock, target, 8).unwrap_err().status(),
            StatusCode::INTERNAL_SERVER_ERROR
        );
    }

    fn headers(case: &str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert("x-request-token", "Run_1-a".parse().unwrap());
        headers.insert(
            "x-fixture-backend",
            format!("http://127.0.0.1:1234/lifetimes/Run_1-a/{case}")
                .parse()
                .unwrap(),
        );
        headers
    }

    fn response(body: Body) -> OutboundResponse {
        OutboundResponse::new(
            Method::GET,
            StatusCode::OK,
            HeaderMap::new(),
            body,
            TestTime::default().clock(),
            ContentTypeProvenance::Unknown,
        )
    }

    #[test]
    fn cpu_stretch_is_a_fixed_supported_probe() {
        assert_eq!(
            Probe::parse("cpu-stretch", &headers("cpu-stretch"))
                .unwrap()
                .case,
            "cpu-stretch"
        );
    }

    #[test]
    fn timeout_before_cutoff_only_shortens_the_fourth_bidder_per_call_timeout() {
        let time = TestTime::default();
        let start = time.clock().now();
        let timeline = Timeline::new(time.clock(), start);
        let probe =
            Probe::parse("timeout-before-cutoff", &headers("timeout-before-cutoff")).unwrap();
        time.advance_to(start + Duration::from_millis(34));
        for role in ["origin", "a", "b", "c", "d"] {
            let parts = probe.request(role, &timeline).unwrap().into_parts();
            let millis = match role {
                "origin" => 15_000,
                "d" => 300,
                _ => 1_000,
            };
            assert_eq!(parts.timeout, Some(Duration::from_millis(millis)));
            assert!(parts.deadline.is_none());
            assert_eq!(parts.headers["x-fixture-at-us"], "34000");
        }
        assert_eq!(timeline.remaining(534), Duration::from_millis(500));
        let ordinary = Probe::parse("slow-origin", &headers("slow-origin")).unwrap();
        assert_eq!(
            ordinary
                .request("d", &timeline)
                .unwrap()
                .into_parts()
                .timeout,
            Some(Duration::from_secs(1))
        );
    }

    #[test]
    fn cpu_calibration_uses_the_same_bounded_request_and_absolute_safety_deadline() {
        let time = TestTime::default();
        let start = time.clock().now();
        let timeline = Timeline::new(time.clock(), start);
        let probe = Probe::parse("cpu-stretch", &headers("cpu-stretch")).unwrap();
        time.advance_to(start + Duration::from_micros(105123));
        let parts = probe
            .calibration_request(&timeline, start + Duration::from_micros(100456))
            .unwrap()
            .into_parts();
        assert_eq!(parts.uri.to_string(), format!("{}/calibration", probe.base));
        assert_eq!(parts.headers["x-fixture-at-us"], "105123");
        assert_eq!(parts.headers["x-fixture-origin-head-us"], "100456");
        assert_eq!(parts.headers.get_all("x-fixture-at-us").iter().count(), 1);
        assert_eq!(parts.timeout, Some(Duration::from_secs(1)));
        assert_eq!(
            parts.deadline.unwrap().instant(),
            timeline.deadline_at(520).instant()
        );
        assert!(matches!(
            parts.response_mode,
            ResponseMode::Buffered { max_bytes: 64 }
        ));
        time.advance_to(start + Duration::from_millis(510));
        assert_eq!(
            timeline.deadline_at(534).instant(),
            start + Duration::from_millis(534)
        );
    }

    #[test]
    fn requests_use_actual_guest_elapsed_and_original_limits() {
        let time = TestTime::default();
        let start = time.clock().now();
        let timeline = Timeline::new(time.clock(), start);
        for case in [
            "slow-origin",
            "fast-origin",
            "begin-progress",
            "batch-progress",
            "pending-head-cutoff",
            "pending-body-cutoff",
        ] {
            let probe = Probe::parse(case, &headers(case)).unwrap();
            assert_eq!(probe.case, case);
            assert_eq!(probe.token, "Run_1-a");
            time.advance_to(start + Duration::from_micros(37_123));
            for role in ["origin", "a", "b", "c", "quiet", "mediator"] {
                let parts = probe.request(role, &timeline).unwrap().into_parts();
                assert_eq!(parts.method, Method::GET);
                assert_eq!(parts.uri.to_string(), format!("{}/{role}", probe.base));
                assert_eq!(parts.headers["x-fixture-at-us"], "37123");
                assert_eq!(
                    parts.timeout,
                    Some(Duration::from_secs(if role == "origin" { 15 } else { 1 }))
                );
                if role == "origin" {
                    assert!(matches!(parts.response_mode, ResponseMode::Streamed));
                    assert_eq!(parts.max_decoded_response_bytes, Some(64));
                } else {
                    assert!(matches!(
                        parts.response_mode,
                        ResponseMode::Buffered { max_bytes: 64 }
                    ));
                }
            }
        }
    }

    #[test]
    fn input_rejects_uncontrolled_ambiguous_or_unimplemented_cases() {
        for case in ["", "other"] {
            assert!(Probe::parse(case, &headers(case)).is_err());
        }
        for target in [
            "https://127.0.0.1:1234/lifetimes/Run_1-a/slow-origin",
            "http://example.com:1234/lifetimes/Run_1-a/slow-origin",
            "http://user@127.0.0.1:1234/lifetimes/Run_1-a/slow-origin",
            "http://127.0.0.1/lifetimes/Run_1-a/slow-origin",
            "http://127.0.0.1:0/lifetimes/Run_1-a/slow-origin",
            "http://127.0.0.1:1234/lifetimes/other/slow-origin",
            "http://127.0.0.1:1234/lifetimes/Run_1-a/fast-origin",
            "http://127.0.0.1:1234/lifetimes/Run_1-a/slow-origin?x=1",
            "http://127.0.0.1:1234/lifetimes/Run_1-a/slow-origin#x",
        ] {
            let mut headers = headers("slow-origin");
            headers.insert("x-fixture-backend", target.parse().unwrap());
            assert!(Probe::parse("slow-origin", &headers).is_err(), "{target}");
        }
        for name in ["x-request-token", "x-fixture-backend"] {
            let mut duplicate = headers("slow-origin");
            duplicate.append(name, duplicate[name].clone());
            assert!(Probe::parse("slow-origin", &duplicate).is_err());
            duplicate.remove(name);
            assert!(Probe::parse("slow-origin", &duplicate).is_err());
        }
        for token in [
            "".to_owned(),
            "a".repeat(65),
            "a/b".to_owned(),
            "a%2fb".to_owned(),
        ] {
            let mut headers = headers("slow-origin");
            headers.insert("x-request-token", token.parse().unwrap());
            assert!(Probe::parse("slow-origin", &headers).is_err());
        }
    }

    #[test]
    fn timeline_excludes_reporting_work_after_the_operation_returns() {
        for ended_us in [200_000, 200_002, 204_999, 205_001] {
            let time = TestTime::default();
            let start = time.clock().now();
            let mut timeline = Timeline::new(time.clock(), start);
            let at = start + Duration::from_nanos(34_000_999);
            time.advance_to(start + Duration::from_micros(ended_us));
            let returned = time.clock().now();
            timeline.event("next", at, returned, {
                time.advance_to(start + Duration::from_millis(500));
                json!({"state":"Cutoff"})
            });
            assert_eq!(timeline.elapsed_us(returned), ended_us);
            let observed_end = timeline.events[0]["at_us"].as_u64().unwrap()
                + timeline.events[0]["duration_us"].as_u64().unwrap();
            assert_eq!(observed_end, ended_us);
            assert_eq!(
                (200_000..=205_000).contains(&observed_end),
                ended_us <= 205_000
            );
        }
    }

    #[test]
    fn timeline_keeps_two_thirty_four_and_fixed_five_thirty_four_without_clock_loops() {
        let time = TestTime::default();
        let start = time.clock().now();
        let mut timeline = Timeline::new(time.clock(), start);
        assert_eq!(timeline.remaining(2), Duration::from_millis(2));
        time.advance_to(start + Duration::from_millis(2));
        assert_eq!(timeline.remaining(34), Duration::from_millis(32));
        time.advance_to(start + Duration::from_millis(34));
        let at = time.clock().now();
        time.advance_to(start + Duration::from_micros(34_017));
        timeline.event("batch-start", at, time.clock().now(), Value::Null);
        assert_eq!(
            timeline.events,
            [json!({"op":"batch-start","at_us":34000,"duration_us":17,"result":null})]
        );
        time.advance_to(start + Duration::from_millis(400));
        assert_eq!(
            timeline.deadline_at(534).instant(),
            start + Duration::from_millis(534)
        );
        assert_eq!(timeline.remaining(534), Duration::from_millis(134));
        time.advance_to(start + Duration::from_millis(710));
        assert_eq!(timeline.remaining(701), Duration::ZERO);
        assert_eq!(timeline.elapsed_us(time.clock().now()), 710_000);
        assert_eq!(
            timeline.deadline_at(200).instant(),
            start + Duration::from_millis(200)
        );
        assert!(timeline.deadline_at(200).is_expired_at(time.clock().now()));
    }

    #[test]
    fn collection_report_preserves_receipts_and_delivered_versus_unresolved_slots() {
        let time = TestTime::default();
        let timeline = Timeline::new(time.clock(), time.clock().now());
        let probe = Probe::parse("slow-origin", &headers("slow-origin")).unwrap();
        let mut report = Report::default();
        block_on(record_item(
            &mut report,
            0,
            OutboundSlotResult::new(
                Duration::from_micros(146_123),
                Ok(response(Body::from("A"))),
            ),
            "check",
        ));
        block_on(record_slots(
            &mut report,
            vec![
                OutboundCollectedSlot::AlreadyDelivered,
                OutboundCollectedSlot::Resolved(OutboundSlotResult::new(
                    Duration::from_micros(286_456),
                    Err(EdgeError::gateway_timeout_caused(
                        "private message",
                        BudgetSource::PerCallTimeout,
                    )),
                )),
                OutboundCollectedSlot::Unresolved,
            ],
        ));
        let json = report.json(&probe, &timeline);
        assert_eq!(
            json["items"],
            json!([
                {"index":0,"via":"check","elapsed_us":146123,"outcome":{"status":200,"bytes":[65]}},
                {"index":1,"via":"collect","elapsed_us":286456,"outcome":{"error":{"kind":"gateway_timeout","status":504,"reason":"PerCallTimeout"}}}
            ])
        );
        assert_eq!(
            json["slots"],
            json!(["AlreadyDelivered", "Resolved", "Unresolved"])
        );
        for field in ["termination", "error", "origin", "mediator"] {
            assert_eq!(json.get(field), Some(&Value::Null), "{field}");
        }
        assert_eq!(json["qualification"], false);
        assert_eq!(json["schema"], 1);
        assert!(!json.to_string().contains("private message"));
    }

    #[test]
    fn report_body_consumption_is_bounded_and_errors_keep_actual_reasons() {
        let polls = Rc::new(Cell::new(0));
        let count = Rc::clone(&polls);
        let source = futures::stream::repeat(Bytes::from_static(&[b'x'; 64])).map(move |bytes| {
            count.set(count.get() + 1);
            Ok(bytes)
        });
        let report = block_on(response_report(Ok(response(Body::from_stream(source)))));
        assert_eq!(
            report,
            json!({"error":{"kind":"response_too_large","status":502,"reason":"BufferedBody"}})
        );
        assert!(polls.get() <= 2, "unbounded report body polling");
        assert_eq!(
            error_report(&EdgeError::gateway_timeout_caused(
                "private",
                BudgetSource::RequestDeadline
            )),
            json!({"kind":"gateway_timeout","status":504,"reason":"RequestDeadline"})
        );
        assert_eq!(
            error_report(&EdgeError::bad_request("private")),
            json!({"kind":"bad_request","status":400,"reason":null})
        );
    }

    #[test]
    fn route_requires_an_injected_context_client() {
        let router = edgezero_core::router::RouterService::builder()
            .get("/progress/lifetimes/{case}", probe)
            .build();
        let mut request = request_builder()
            .uri("/progress/lifetimes/slow-origin")
            .body(Body::empty())
            .unwrap();
        *request.headers_mut() = headers("slow-origin");
        assert_eq!(
            block_on(router.oneshot(request)).unwrap().status(),
            StatusCode::INTERNAL_SERVER_ERROR
        );
    }
}
