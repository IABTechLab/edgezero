use bytes::Bytes;
use edgezero_core::context::RequestContext;
use edgezero_core::http::{HeaderMap, Method, Response, Uri, response_builder};
use edgezero_core::{
    Body, Deadline, EdgeError, HttpClient, IndependentOutboundProgress, MonotonicClock,
    MonotonicInstant, OutboundBatch, OutboundBatchCheck, OutboundBatchNext, OutboundCollectedSlot,
    OutboundRequest, OutboundResponse, OutboundScope, OutboundScopeOwner, OutboundSlotResult,
    action,
};
use futures::StreamExt as _;
use serde_json::{Value, json};
use std::{
    rc::Rc,
    sync::{Arc, OnceLock},
    time::Duration,
};

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
    handoff: Option<Value>,
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
                | "caps-while-waiting"
                | "response-handoff"
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
        if self.case == "caps-while-waiting" && role == "a" {
            request = request
                .max_response_bytes(64 * 1024)
                .max_decoded_response_bytes(1024 * 1024)
                .max_encoded_response_bytes(256 * 1024);
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
            "termination":self.termination,"error":self.error,"origin":self.origin,"mediator":self.mediator,"handoff":self.handoff})
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
    let slot_error = slot.outcome.is_err();
    report.items.push(
        json!({"index":index,"via":via,"elapsed_us":micros(slot.elapsed),
        "slot_error":slot_error,"outcome":response_report(slot.outcome).await}),
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
    let origin = if matches!(probe.case.as_str(), "batch-progress" | "caps-while-waiting") {
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
        if matches!(probe.case.as_str(), "cpu-stretch" | "caps-while-waiting") {
            json!({"cutoff_at_us":timeline.elapsed_us(observation_cutoff.instant())})
        } else {
            Value::Null
        },
    );

    if probe.case == "caps-while-waiting" {
        collect(batch, report, timeline).await;
        return Ok(());
    }

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

fn handoff_prelude(token: &str, cutoff_at_us: u64, dispatch_returned: Option<bool>) -> Value {
    json!({"schema":1,"case":"response-handoff","token":token,"phase":"pending-handoff",
        "origin_pending":true,"batch_pending":true,"slots":3,"cutoff_at_us":cutoff_at_us,"qualification":false,
        "callback_scope_closed":true,"body_scope_live":true,"dispatch_returned":dispatch_returned})
}

async fn response_handoff(
    ctx: &RequestContext,
    probe: Probe,
    client: HttpClient,
    mut timeline: Timeline,
) -> Result<Response, EdgeError> {
    let callback = OutboundScope::capture(Rc::new(IndependentOutboundProgress));
    let owner = OutboundScopeOwner::response_body();
    let body_scope = owner.scope();
    let dispatched = super::progress::dispatch_returned(ctx);
    timeline.wait_until(ctx, 2).await?;
    let request = probe.request("origin", &timeline)?;
    let at = timeline.clock.now();
    let mut origin = owner.with(|| client.begin(request)).await?;
    timeline.event("begin", at, timeline.clock.now(), json!("scheduled"));
    timeline.wait_until(ctx, 34).await?;
    let requests = ["a", "b", "c"]
        .into_iter()
        .map(|role| probe.request(role, &timeline))
        .collect::<Result<Vec<_>, _>>()?;
    let observation_cutoff = timeline.deadline_at(534);
    let at = timeline.clock.now();
    let mut batch = owner.with(|| client.start_batch_until(requests, observation_cutoff));
    timeline.event(
        "batch-start",
        at,
        timeline.clock.now(),
        json!({"cutoff_at_us":timeline.elapsed_us(observation_cutoff.instant())}),
    );
    let at = timeline.clock.now();
    if origin.is_ready() || !matches!(batch.check()?, OutboundBatchCheck::Pending) {
        return Err(EdgeError::internal(std::io::Error::other(
            "handoff work was already terminal",
        )));
    }
    timeline.event(
        "handoff-pending",
        at,
        timeline.clock.now(),
        json!({"origin":"Pending","batch":"Pending"}),
    );
    let prelude = Arc::new(OnceLock::new());
    let prefix_snapshot = Arc::clone(&prelude);
    let prefix_callback = callback.clone();
    let prefix_owner = body_scope.clone();
    let prefix_dispatch = dispatched.clone();
    let prefix_token = probe.token.clone();
    let cutoff_at_us = timeline.elapsed_us(observation_cutoff.instant());
    let prefix = futures::stream::once(async move {
        if let Some(signal) = &prefix_dispatch {
            signal.wait().await;
        }
        if prefix_callback.check().is_ok() || prefix_owner.check().is_err() {
            return Err(EdgeError::internal(std::io::Error::other(
                "handoff prelude preceded accepted dispatch return",
            )));
        }
        let value = handoff_prelude(
            &prefix_token,
            cutoff_at_us,
            prefix_dispatch.as_ref().map(|signal| signal.is_returned()),
        );
        let mut bytes = serde_json::to_vec(&value).map_err(EdgeError::internal)?;
        bytes.push(b'\n');
        if bytes.len() > 512 {
            return Err(invalid());
        }
        prefix_snapshot.set(value).map_err(|_| invalid())?;
        Ok(Bytes::from(bytes))
    });
    let source = prefix.chain(
        futures::stream::once(async move {
            let mut report = Report::default();
            let at = timeline.clock.now();
            let result = origin.collect().await;
            timeline.event("origin-head", at, timeline.clock.now(), json!(if result.is_ok(){"ok"}else{"error"}));
            let mut origin_streamed = false;
            match result {
                Ok(response) => {
                    let status = response.status().as_u16();
                    let body = response.into_body();
                    origin_streamed = body.is_stream();
                    let at = timeline.clock.now();
                    report.origin = Some(match body.into_bytes_bounded(64).await {
                        Ok(bytes) => json!({"status":status,"bytes":bytes.to_vec()}),
                        Err(error) => json!({"error":error_report(&error)}),
                    });
                    timeline.event("origin-body", at, timeline.clock.now(), Value::Null);
                }
                Err(error) => {
                    report.error = Some(error_report(&error));
                    report.origin = Some(json!({"error":error_report(&error)}));
                }
            }
            collect(batch, &mut report, &mut timeline).await;
            let snapshot = prelude.get().ok_or_else(invalid)?;
            report.handoff = Some(json!({"prelude":snapshot,"origin_streamed":origin_streamed,
                "callback_scope_closed":callback.check().is_err(),"body_scope_live":body_scope.check().is_ok(),
                "dispatch_returned":dispatched.as_ref().map(|flag| flag.is_returned())}));
            let mut bytes = serde_json::to_vec(&report.json(&probe, &timeline)).map_err(EdgeError::internal)?;
            bytes.push(b'\n');
            if bytes.len() > 16 * 1024 { return Err(invalid()); }
            Ok(Bytes::from(bytes))
        }),
    );
    owner.attach(
        response_builder()
            .header("content-type", "application/x-ndjson")
            .body(Body::from_stream(source))
            .map_err(EdgeError::internal)?,
    )
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
    if probe.case == "response-handoff" {
        return response_handoff(&ctx, probe, client, timeline).await;
    }
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
    use edgezero_core::{
        Body, BudgetSource, ContentTypeProvenance, ResponseLimitReason, ResponseMode,
    };
    use futures::{StreamExt, executor::block_on};
    use std::sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    };
    use std::{cell::Cell, rc::Rc};

    #[derive(Clone)]
    struct HandoffClient {
        time: TestTime,
        jobs: Arc<
            std::sync::Mutex<
                std::collections::BTreeMap<String, futures::channel::oneshot::Sender<()>>,
            >,
        >,
        resources: Arc<std::sync::atomic::AtomicUsize>,
        cancelled: Arc<std::sync::atomic::AtomicUsize>,
        family: Arc<std::sync::atomic::AtomicUsize>,
        groups: Arc<std::sync::atomic::AtomicUsize>,
        calls: Arc<std::sync::atomic::AtomicUsize>,
        polls: Arc<std::sync::atomic::AtomicUsize>,
    }

    struct FamilyDrop(Arc<std::sync::atomic::AtomicUsize>);

    #[derive(Clone, Default)]
    struct TerminalReports(Arc<std::sync::Mutex<Vec<edgezero_core::ResponseEgressOutcome>>>);

    impl edgezero_core::response_egress::ResponseEgressObserver for TerminalReports {
        fn complete(&self, report: &edgezero_core::ResponseEgressReport) {
            self.0.lock().unwrap().push(report.outcome);
        }
    }

    enum HandoffEnd {
        Collect,
        Terminal(edgezero_core::ResponseEgressOutcome),
        Drop,
    }
    impl Drop for FamilyDrop {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    impl HandoffClient {
        fn queue(
            &self,
            request: edgezero_core::OutboundDispatchRequest,
        ) -> edgezero_core::OutboundFuture<OutboundResponse> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            drop(
                request
                    .scope()
                    .provider_state(|| FamilyDrop(Arc::clone(&self.family)))
                    .unwrap(),
            );
            let (parts, context) = request.into_parts();
            let role = parts.uri.path().rsplit('/').next().unwrap().to_owned();
            let payload = match role.as_str() {
                "origin" => b'O',
                "a" => b'A',
                "b" => b'B',
                "c" => b'C',
                _ => panic!("unexpected handoff job"),
            };
            let (send, mut receive) = futures::channel::oneshot::channel();
            assert!(self.jobs.lock().unwrap().insert(role, send).is_none());
            let resources = Arc::clone(&self.resources);
            let owner = edgezero_core::OutboundExchangeOwner::new(move || {
                resources.fetch_add(1, Ordering::SeqCst);
            });
            let polls = Arc::clone(&self.polls);
            Box::pin(async move {
                std::future::poll_fn(move |cx| {
                    polls.fetch_add(1, Ordering::SeqCst);
                    futures::FutureExt::poll_unpin(&mut receive, cx)
                })
                .await
                .map_err(|_| invalid())?;
                let source = futures::stream::once(async move { Ok(Bytes::from(vec![payload])) })
                    .boxed_local();
                edgezero_core::OutboundHead::new(
                    StatusCode::OK,
                    edgezero_core::OutboundHeaderInput::new(
                        "fixture",
                        HeaderMap::new(),
                        edgezero_core::OutboundHeaderFidelity::AdapterVisibleFields,
                    ),
                    source,
                    context,
                    owner,
                )?
                .finish()
                .await
            })
        }
    }

    impl edgezero_core::OutboundHttpClient for HandoffClient {
        fn progress(&self) -> Rc<dyn edgezero_core::OutboundProgressRuntime> {
            Rc::new(IndependentOutboundProgress)
        }
        fn time(&self) -> edgezero_core::OutboundTime {
            self.time.pair()
        }
        fn validate_supported_options(
            &self,
            _: &OutboundRequest,
            _: edgezero_core::OutboundSendMode,
        ) -> Result<(), EdgeError> {
            Ok(())
        }
        fn dispatch_head(
            &self,
            _: edgezero_core::OutboundDispatchRequest,
        ) -> edgezero_core::OutboundFuture<edgezero_core::OutboundHead> {
            Box::pin(async { Err(invalid()) })
        }
        fn begin(
            &self,
            request: edgezero_core::OutboundDispatchRequest,
        ) -> Result<edgezero_core::OutboundPending, EdgeError> {
            let future = self.queue(request);
            let cancelled = Arc::clone(&self.cancelled);
            let jobs = Arc::clone(&self.jobs);
            Ok(edgezero_core::OutboundPending::new(
                future,
                edgezero_core::OutboundExchangeOwner::new(move || {
                    jobs.lock().unwrap().remove("origin");
                    cancelled.fetch_add(1, Ordering::SeqCst);
                }),
            ))
        }
        fn start_batch_until(&self, batch: edgezero_core::OutboundPreparedBatch) -> OutboundBatch {
            let (context, requests) = batch.into_slots();
            let time = self.time.clock();
            let futures = requests
                .into_iter()
                .map(|(index, request)| {
                    let response = self.queue(request);
                    let clock = time.clone();
                    async move {
                        let outcome = response.await;
                        edgezero_core::OutboundBatchDriverEvent::Item {
                            index,
                            completed_at: clock.now(),
                            outcome,
                        }
                    }
                })
                .collect::<futures::stream::FuturesUnordered<_>>();
            let jobs = Arc::clone(&self.jobs);
            let groups = Arc::clone(&self.groups);
            OutboundBatch::from_driver(
                context,
                futures,
                edgezero_core::OutboundExchangeOwner::new(move || {
                    let mut jobs = jobs.lock().unwrap();
                    for role in ["a", "b", "c"] {
                        jobs.remove(role);
                    }
                    groups.fetch_add(1, Ordering::SeqCst);
                }),
            )
        }
    }

    struct HandoffRuntime(TestTime);
    impl super::super::progress::ProbeRuntime for HandoffRuntime {
        fn adapter(&self) -> &'static str {
            "unit"
        }
        fn perform(&self, _: String) -> super::super::progress::Job {
            Box::pin(async { Err(invalid()) })
        }
        fn start(&self, _: String) -> Result<super::super::progress::Job, EdgeError> {
            Err(invalid())
        }
        fn pause(&self, duration: Duration) -> futures::future::LocalBoxFuture<'static, ()> {
            let time = self.0.clone();
            Box::pin(async move {
                time.advance_to(time.clock().now() + duration);
            })
        }
    }

    #[test]
    fn handoff_route_keeps_original_jobs_past_dispatch_and_releases_at_terminal() {
        for has_return_signal in [false, true] {
            handoff_route(has_return_signal);
        }
    }

    #[test]
    fn terminal_cleanup_revokes_pending_jobs_before_retained_body_drop() {
        use edgezero_core::ResponseEgressOutcome;
        for outcome in [
            ResponseEgressOutcome::Completed,
            ResponseEgressOutcome::ClientDisconnected,
            ResponseEgressOutcome::DeadlineExceeded,
            ResponseEgressOutcome::SourceError,
            ResponseEgressOutcome::RequestCancelled,
        ] {
            pending_terminal_cleanup(Some(outcome));
        }
        pending_terminal_cleanup(None);
    }

    fn pending_terminal_cleanup(outcome: Option<edgezero_core::ResponseEgressOutcome>) {
        handoff_route_until(true, outcome.map_or(HandoffEnd::Drop, HandoffEnd::Terminal));
    }

    fn handoff_route(has_return_signal: bool) {
        handoff_route_until(has_return_signal, HandoffEnd::Collect);
    }

    fn handoff_route_until(has_return_signal: bool, end: HandoffEnd) {
        use edgezero_core::response_egress_framing::ResponseEgressFramingMode;
        use edgezero_core::{
            IngressDispatchOutcome, IngressFraming, IngressHeadAccounting, ResponseEgressOutcome,
            app::App,
        };
        use futures::FutureExt as _;
        use std::{
            collections::BTreeMap,
            sync::{Mutex, atomic::AtomicUsize},
        };
        let time = TestTime::default();
        let start = time.clock().now();
        let client = HandoffClient {
            time: time.clone(),
            jobs: Arc::new(Mutex::new(BTreeMap::new())),
            resources: Arc::new(AtomicUsize::new(0)),
            cancelled: Arc::new(AtomicUsize::new(0)),
            family: Arc::new(AtomicUsize::new(0)),
            groups: Arc::new(AtomicUsize::new(0)),
            calls: Arc::new(AtomicUsize::new(0)),
            polls: Arc::new(AtomicUsize::new(0)),
        };
        let returned = Arc::new(super::super::progress::DispatchReturn::default());
        let router = super::super::progress::router(
            Arc::new(HandoffRuntime(time.clone())),
            Value::Null,
            has_return_signal.then(|| Arc::clone(&returned)),
        );
        let mut app = App::new(router);
        app.set_monotonic_clock(time.clock());
        let reports = TerminalReports::default();
        app.set_response_egress_observer(reports.clone());
        let mut request = request_builder()
            .uri("/progress/lifetimes/response-handoff")
            .body(Body::empty())
            .unwrap();
        *request.headers_mut() = headers("response-handoff");
        request
            .extensions_mut()
            .insert(HttpClient::with_client(client.clone()));
        let callback = OutboundScopeOwner::callback();
        let outcome = block_on(callback.run(app.dispatch_ingress(
            request,
            start,
            IngressHeadAccounting::HostManaged,
            IngressFraming::HostManaged,
        )))
        .unwrap();
        let IngressDispatchOutcome::Response(envelope) = outcome else {
            panic!("handoff response")
        };
        assert_eq!(
            client
                .jobs
                .lock()
                .unwrap()
                .keys()
                .map(String::as_str)
                .collect::<Vec<_>>(),
            ["a", "b", "c", "origin"]
        );
        let (prepared, _, mut attempt, clock) = callback
            .with(|| envelope.begin(ResponseEgressFramingMode::ExplicitBufferedLength))
            .unwrap_or_else(|failure| {
                let (_, _, cause) = failure.into_parts();
                panic!("handoff preparation failed: {cause:?}");
            });
        assert!(attempt.begin_writing());
        callback.close();
        drop(callback);
        assert_eq!(client.cancelled.load(Ordering::SeqCst), 0);
        assert_eq!(client.calls.load(Ordering::SeqCst), 4);
        assert_eq!(client.jobs.lock().unwrap().len(), 4);
        let mut body = prepared.into_response().into_body().into_stream().unwrap();
        if has_return_signal {
            assert!(
                body.next().now_or_never().is_none(),
                "no prefix before dispatch-return notification"
            );
        }
        assert_eq!(client.resources.load(Ordering::SeqCst), 0);
        assert_eq!(client.family.load(Ordering::SeqCst), 0);
        returned.mark_returned();
        let prefix = block_on(body.next()).unwrap().unwrap();
        let prelude: Value = serde_json::from_slice(&prefix).unwrap();
        assert_eq!(
            prelude,
            handoff_prelude("Run_1-a", 534000, has_return_signal.then_some(true))
        );
        assert!(
            body.next().now_or_never().is_none(),
            "original origin remains gated after the prefix"
        );
        assert_eq!(client.jobs.lock().unwrap().len(), 4);
        if !matches!(end, HandoffEnd::Collect) {
            let jobs = std::mem::take(&mut *client.jobs.lock().unwrap());
            assert!(jobs.values().all(|sender| !sender.is_canceled()));
            let polls = client.polls.load(Ordering::SeqCst);
            let mut retained_attempt = Some(attempt);
            let expected = match end {
                HandoffEnd::Terminal(outcome) => {
                    let attempt = retained_attempt.as_mut().unwrap();
                    assert!(attempt.terminate(outcome, clock.now()));
                    assert!(!attempt.terminate(ResponseEgressOutcome::Unspecified, clock.now()));
                    outcome
                }
                HandoffEnd::Drop => {
                    drop(retained_attempt.take());
                    ResponseEgressOutcome::TransportError
                }
                HandoffEnd::Collect => unreachable!(),
            };
            let counts = || {
                [
                    client.resources.load(Ordering::SeqCst),
                    client.cancelled.load(Ordering::SeqCst),
                    client.groups.load(Ordering::SeqCst),
                    client.family.load(Ordering::SeqCst),
                ]
            };
            assert_eq!(
                counts(),
                [4, 1, 1, 1],
                "terminal revokes all work before retained body drop"
            );
            assert_eq!(*reports.0.lock().unwrap(), [expected]);
            assert!(jobs.values().all(|sender| sender.is_canceled()));
            assert!(matches!(
                body.next().now_or_never(),
                Some(None) | Some(Some(Err(EdgeError::Internal { .. })))
            ));
            assert_eq!(
                client.polls.load(Ordering::SeqCst),
                polls,
                "terminal prevents further producer polls"
            );
            drop(retained_attempt);
            assert_eq!(
                counts(),
                [4, 1, 1, 1],
                "attempt drop cannot defer or repeat terminal cleanup"
            );
            drop(body);
            drop(jobs);
            assert_eq!(
                counts(),
                [4, 1, 1, 1],
                "later wrapper/handle drops cannot release twice"
            );
            assert_eq!(*reports.0.lock().unwrap(), [expected]);
            assert_eq!(client.calls.load(Ordering::SeqCst), 4);
            return;
        }
        time.advance_to(start + Duration::from_millis(50));
        for (_, sender) in std::mem::take(&mut *client.jobs.lock().unwrap()) {
            sender.send(()).unwrap();
        }
        let bytes = block_on(body.next()).unwrap().unwrap();
        let report: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(report["origin"], json!({"status":200,"bytes":[79]}));
        assert_eq!(report["termination"], "Completed");
        assert_eq!(report["slots"], json!(["Resolved", "Resolved", "Resolved"]));
        assert_eq!(report["handoff"]["prelude"], prelude);
        assert_eq!(report["error"], Value::Null);
        for (index, item) in report["items"].as_array().unwrap().iter().enumerate() {
            assert_eq!(item["index"], index);
            assert_eq!(item["outcome"], json!({"status":200,"bytes":[65+index]}));
            assert_eq!(item["slot_error"], false);
            assert_eq!(item["elapsed_us"], 16000);
        }
        for field in [
            "callback_scope_closed",
            "body_scope_live",
            "origin_streamed",
        ] {
            assert_eq!(report["handoff"][field], true, "{field}");
        }
        assert_eq!(
            report["handoff"]["dispatch_returned"],
            json!(has_return_signal.then_some(true))
        );
        assert!(block_on(body.next()).is_none());
        assert_eq!(client.cancelled.load(Ordering::SeqCst), 0);
        assert_eq!(client.resources.load(Ordering::SeqCst), 4);
        assert_eq!(client.groups.load(Ordering::SeqCst), 1);
        assert_eq!(client.calls.load(Ordering::SeqCst), 4);
        assert_eq!(
            client.family.load(Ordering::SeqCst),
            0,
            "EOF is not the egress terminal"
        );
        assert!(attempt.account_bytes(
            u64::try_from(prefix.len() + bytes.len()).unwrap(),
            clock.now()
        ));
        assert!(attempt.complete(clock.now()));
        assert!(!attempt.complete(clock.now()));
        assert_eq!(
            attempt.into_terminal_report().outcome,
            ResponseEgressOutcome::Completed
        );
        assert_eq!(client.family.load(Ordering::SeqCst), 1);
    }

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

    #[test]
    fn response_handoff_preserves_existing_request_budgets_and_caps() {
        let time = TestTime::default();
        let timeline = Timeline::new(time.clock(), time.clock().now());
        let probe = Probe::parse("response-handoff", &headers("response-handoff")).unwrap();
        for role in ["origin", "a", "b", "c"] {
            let parts = probe.request(role, &timeline).unwrap().into_parts();
            assert_eq!(
                parts.timeout,
                Some(Duration::from_secs(if role == "origin" { 15 } else { 1 }))
            );
            assert!(parts.deadline.is_none());
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
    fn caps_while_waiting_keeps_independent_limits_and_the_original_budget() {
        let time = TestTime::default();
        let start = time.clock().now();
        let timeline = Timeline::new(time.clock(), start);
        let probe = Probe::parse("caps-while-waiting", &headers("caps-while-waiting")).unwrap();
        for role in ["a", "b", "c"] {
            let parts = probe.request(role, &timeline).unwrap().into_parts();
            assert_eq!(parts.timeout, Some(Duration::from_secs(1)));
            assert!(parts.deadline.is_none());
            let max_bytes = if role == "a" { 64 * 1024 } else { 64 };
            assert!(
                matches!(parts.response_mode, ResponseMode::Buffered { max_bytes: actual } if actual == max_bytes)
            );
            if role == "a" {
                assert_eq!(parts.max_decoded_response_bytes, Some(1024 * 1024));
                assert_eq!(parts.max_encoded_response_bytes, Some(256 * 1024));
            }
        }
        assert_eq!(
            timeline.deadline_at(534).instant(),
            start + Duration::from_millis(534)
        );
    }

    #[test]
    fn slot_error_provenance_excludes_the_report_readers_own_limit() {
        let mut report = Report::default();
        block_on(record_item(
            &mut report,
            0,
            OutboundSlotResult::new(Duration::ZERO, Ok(response(Body::from(vec![b'x'; 65])))),
            "collect",
        ));
        assert_eq!(
            report.items[0]["outcome"]["error"]["reason"],
            "BufferedBody"
        );
        assert_eq!(report.items[0]["slot_error"], false);
        block_on(record_item(
            &mut report,
            1,
            OutboundSlotResult::new(
                Duration::ZERO,
                Err(EdgeError::response_too_large_with_reason(
                    "bounded",
                    ResponseLimitReason::BufferedBody,
                )),
            ),
            "collect",
        ));
        assert_eq!(report.items[1]["slot_error"], true);
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
                {"index":0,"via":"check","elapsed_us":146123,"slot_error":false,"outcome":{"status":200,"bytes":[65]}},
                {"index":1,"via":"collect","elapsed_us":286456,"slot_error":true,"outcome":{"error":{"kind":"gateway_timeout","status":504,"reason":"PerCallTimeout"}}}
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
