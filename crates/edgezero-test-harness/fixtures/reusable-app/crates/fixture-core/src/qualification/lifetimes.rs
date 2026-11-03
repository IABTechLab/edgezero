use bytes::Bytes;
use edgezero_core::context::RequestContext;
use edgezero_core::http::{HeaderMap, Method, Response, Uri, response_builder};
#[cfg(feature = "native-body-witness")]
use edgezero_core::outbound::test_support::{
    NativeBodySnapshot, NativeBodyWait, NativeBodyWitness,
};
use edgezero_core::{
    Body, Deadline, EdgeError, HttpClient, IndependentOutboundProgress, MonotonicClock,
    MonotonicInstant, OutboundBatch, OutboundBatchCheck, OutboundBatchNext, OutboundCollectedSlot,
    OutboundRequest, OutboundResponse, OutboundScope, OutboundScopeOwner, OutboundSlotResult,
    action,
};
use futures::{StreamExt as _, TryStreamExt as _};
use serde_json::{Value, json};
use std::{
    cell::RefCell,
    rc::Rc,
    sync::{Arc, OnceLock},
    time::Duration,
};

#[cfg(feature = "native-body-witness")]
mod response_cleanup;

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
    native_body: Option<Value>,
    batch_state: Option<Value>,
}

impl Probe {
    fn parse(case: &str, headers: &HeaderMap) -> Result<Self, EdgeError> {
        if !matches!(
            case,
            "slow-origin"
                | "timeout-before-cutoff"
                | "fast-origin"
                | "body-read-arrivals"
                | "trickling-slot"
                | "send-during-batch"
                | "begin-progress"
                | "batch-progress"
                | "pending-head-cutoff"
                | "pending-body-cutoff"
                | "cpu-stretch"
                | "caps-while-waiting"
                | "response-handoff"
                | "batch-write-deadline"
                | "batch-cancel"
                | "body-check"
                | "begin-upload-rules"
                | "response-owned-cleanup-complete"
                | "response-owned-cleanup-disconnect"
                | "response-owned-cleanup-write-deadline"
                | "response-owned-cleanup-source-error"
                | "response-owned-cleanup-cancel"
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
            "termination":self.termination,"error":self.error,"origin":self.origin,"mediator":self.mediator,"handoff":self.handoff,
            "native_body":self.native_body,"batch_state":self.batch_state})
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

async fn read_trickle(
    response: OutboundResponse,
    timeline: &Timeline,
    observations: &Rc<RefCell<Vec<Value>>>,
) -> Value {
    let status = response.status().as_u16();
    let Ok(mut source) = response.into_body().into_stream() else {
        return json!({"error":error_report(&invalid())});
    };
    let mut bytes = Vec::new();
    for _ in 0..64 {
        let at = timeline.clock.now();
        let result = source.next().await;
        let ended = timeline.clock.now();
        let detail = match &result {
            Some(Ok(chunk)) if bytes.len().saturating_add(chunk.len()) <= 64 => {
                json!(chunk.as_ref())
            }
            Some(Ok(_)) => json!({"error":error_report(&invalid())}),
            Some(Err(error)) => json!({"error":error_report(error)}),
            None => Value::Null,
        };
        observations.borrow_mut().push(json!({"op":"read","started_at_us":timeline.elapsed_us(at),"at_us":timeline.elapsed_us(ended),"result":detail}));
        match result {
            Some(Ok(chunk)) if bytes.len().saturating_add(chunk.len()) <= 64 => {
                bytes.extend_from_slice(&chunk)
            }
            Some(Ok(_)) => return json!({"error":error_report(&invalid())}),
            Some(Err(error)) => return json!({"error":error_report(&error)}),
            None => return json!({"status":status,"bytes":bytes}),
        }
    }
    json!({"error":error_report(&invalid())})
}

async fn trickling_slot(
    response: OutboundResponse,
    mut batch: OutboundBatch,
    report: &mut Report,
    timeline: &mut Timeline,
) {
    let observations = Rc::new(RefCell::new(Vec::new()));
    let at = timeline.clock.now();
    let returned = async {
        let mut results = Vec::new();
        for _ in 0..4 {
            let started = timeline.clock.now();
            let result = batch.next().await;
            let ended = timeline.clock.now();
            let mut record = json!({"op":"next","started_at_us":timeline.elapsed_us(started),"at_us":timeline.elapsed_us(ended)});
            match &result {
                Ok(OutboundBatchNext::Item(item)) => {
                    record["result"] = json!("Item");
                    record["index"] = json!(item.index());
                }
                Ok(OutboundBatchNext::Finished(termination)) => {
                    record["result"] = json!("Finished");
                    record["termination"] = json!(format!("{termination:?}"));
                }
                Err(error) => {
                    record["result"] = json!("error");
                    record["error"] = error_report(error);
                }
            }
            observations.borrow_mut().push(record);
            let finished = !matches!(result, Ok(OutboundBatchNext::Item(_)));
            results.push(result);
            if finished {
                break;
            }
        }
        results
    };
    let (origin, results) =
        futures::future::join(read_trickle(response, timeline, &observations), returned).await;
    let ended = timeline.clock.now();
    let log = std::mem::take(&mut *observations.borrow_mut());
    timeline.event("trickle", at, ended, json!({"observations":log}));
    report.origin = Some(origin);
    for result in results {
        match result {
            Ok(OutboundBatchNext::Item(item)) => {
                let (index, slot) = item.into_parts();
                record_item(report, index, slot, "next").await;
            }
            Ok(OutboundBatchNext::Finished(_)) => {}
            Err(error) => report.error = Some(error_report(&error)),
        }
    }
    collect(batch, report, timeline).await;
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
    let origin = if matches!(
        probe.case.as_str(),
        "batch-progress" | "caps-while-waiting" | "send-during-batch"
    ) {
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
        if matches!(
            probe.case.as_str(),
            "cpu-stretch"
                | "caps-while-waiting"
                | "body-read-arrivals"
                | "trickling-slot"
                | "send-during-batch"
        ) {
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
    let result = if probe.case == "send-during-batch" {
        client.send(probe.request("origin", timeline)?).await
    } else {
        origin.ok_or_else(invalid)?.collect().await
    };
    let ended = timeline.clock.now();
    timeline.event(
        if probe.case == "send-during-batch" {
            "origin-send"
        } else {
            "origin-head"
        },
        at,
        ended,
        json!(if result.is_ok() { "ok" } else { "error" }),
    );
    let response = result?;
    if probe.case == "trickling-slot" {
        trickling_slot(response, batch, report, timeline).await;
        return Ok(());
    }
    if probe.case == "body-read-arrivals" {
        if !response.body().is_stream() {
            return Err(invalid());
        }
        let at = timeline.clock.now();
        report.origin = Some(response_report(Ok(response)).await);
        timeline.event("origin-body", at, timeline.clock.now(), json!("streamed"));
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
        return Ok(());
    }
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
    if matches!(
        probe.case.as_str(),
        "slow-origin" | "timeout-before-cutoff" | "send-during-batch"
    ) {
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
    #[cfg(feature = "native-body-witness")]
    let (requests, witnesses) = {
        let mut requests = requests;
        let witnesses = [
            NativeBodyWitness::new(),
            NativeBodyWitness::new(),
            NativeBodyWitness::new(),
        ];
        for (request, witness) in requests.iter_mut().zip(&witnesses) {
            witness.attach(request)?;
        }
        (requests, witnesses)
    };
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
    // Snapshot before collect can release any additional work.
    #[cfg(feature = "native-body-witness")]
    {
        report.native_body = Some(json!(
            witnesses
                .iter()
                .map(|witness| { native_body_report(witness.snapshot(), timeline) })
                .collect::<Vec<_>>()
        ));
    }
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

#[cfg(feature = "native-body-witness")]
fn native_body_report(snapshot: NativeBodySnapshot, timeline: &Timeline) -> Value {
    let at = |instant: MonotonicInstant| {
        instant
            .checked_duration_since(timeline.start)
            .and_then(|duration| u64::try_from(duration.as_micros()).ok())
    };
    let preparation = snapshot.preparation.map(|prepared| json!({
        "mode":format!("{:?}", prepared.mode), "cause":format!("{:?}", prepared.budget.cause),
        "entered_at_us":at(prepared.entered_at), "deadline_at_us":at(prepared.budget.deadline.instant()),
        "target_uri":prepared.target.to_string(),
    }));
    let wait = |observation: NativeBodyWait| {
        json!({
            "kind":"BodyReadiness","outcome":format!("{:?}",observation.outcome),
            "started_at_us":at(observation.started_at),"observed_at_us":at(observation.observed_at),
        })
    };
    json!({"attachments":snapshot.attachments,"preparations":snapshot.preparations,
        "preparation":preparation,"invalid":snapshot.invalid,
        "heads":snapshot.heads,"head_at_us":snapshot.first_head.and_then(at),
        "first_read":snapshot.first_read.map(|read| json!({
            "started_at_us":at(read.started_at),"observed_at_us":at(read.observed_at),"pending":read.pending})),
        "reads":snapshot.reads,"ready_reads":snapshot.ready_reads,
        "waits":snapshot.waits,"ready_waits":snapshot.ready_waits,"failed_waits":snapshot.failed_waits,
        "first_wait":snapshot.first_wait.map(wait),
        "first_wait_timeout":snapshot.first_wait_timeout.map(wait),
        "last_wait_timeout":snapshot.last_wait_timeout.map(wait),
        "source_acquisitions":snapshot.source_acquisitions,"source_releases":snapshot.source_releases,
        "operation_acquisitions":snapshot.operation_acquisitions,"operation_polls":snapshot.operation_polls,
        "ready_operation_polls":snapshot.ready_operation_polls,"operation_releases":snapshot.operation_releases,
        "operation_acquired_at_us":snapshot.first_operation_acquisition.and_then(at),
        "operation_released_at_us":snapshot.first_operation_release.and_then(at),
        "first_operation_poll":snapshot.first_operation_poll.map(|poll|json!({"started_at_us":at(poll.started_at),
            "observed_at_us":at(poll.observed_at),"pending":poll.pending})),
        "released_at_us":snapshot.first_release.and_then(at)})
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

const BATCH_TERMINAL_PREFIX: &str = "edgezero-lifetime-batch ";

fn batch_prelude(probe: &Probe) -> Value {
    let mut value = json!({"schema":1,"case":probe.case,"token":probe.token,"phase":"batch-pending",
        "slots":3,"qualification":false});
    if probe.case == "batch-write-deadline" {
        value["cutoff_after_us"] = json!(3_000_000);
    } else {
        value["cutoff_at_us"] = json!(534_000);
        value["delivered"] = json!([0]);
        value["in_flight"] = json!([1, 2]);
    }
    value
}

fn line(value: &Value) -> Result<Bytes, EdgeError> {
    let mut bytes = serde_json::to_vec(value).map_err(EdgeError::internal)?;
    if bytes.len() >= 16 * 1024 {
        return Err(invalid());
    }
    bytes.push(b'\n');
    Ok(Bytes::from(bytes))
}

struct BatchSourceDrop(Arc<std::sync::atomic::AtomicUsize>);

impl Drop for BatchSourceDrop {
    fn drop(&mut self) {
        self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }
}

#[derive(Default)]
struct BatchWitness {
    dropped: Arc<std::sync::atomic::AtomicUsize>,
    pending: Arc<std::sync::atomic::AtomicBool>,
    prefix_bytes: std::sync::atomic::AtomicUsize,
    cutoff_at_us: std::sync::atomic::AtomicU64,
    #[cfg(feature = "native-body-witness")]
    native: std::sync::Mutex<Option<BatchNative>>,
}

#[cfg(feature = "native-body-witness")]
struct BatchNative {
    start: MonotonicInstant,
    witnesses: [NativeBodyWitness; 3],
}

#[cfg(feature = "native-body-witness")]
fn buffered_a_complete(snapshot: &NativeBodySnapshot) -> bool {
    !snapshot.invalid
        && snapshot.attachments == 1
        && snapshot.preparations == 1
        && snapshot.operation_acquisitions == 1
        && snapshot.operation_releases == 1
        && snapshot.operation_polls > 0
        && snapshot.ready_operation_polls == 1
        && snapshot.heads == 1
        && snapshot.source_acquisitions == 1
        && snapshot.source_releases == 1
        && snapshot.reads > 0
        && snapshot.ready_reads > 0
}

#[cfg(feature = "native-body-witness")]
fn batch_native_complete(snapshots: &[NativeBodySnapshot; 3], cancel: bool) -> bool {
    buffered_a_complete(&snapshots[0])
        && snapshots[1].pending_operation().is_some()
        && (cancel || snapshots[2].pending_operation().is_some())
}

#[cfg(feature = "native-body-witness")]
async fn wait_for_batch_native(
    scope: &OutboundScope,
    time: &edgezero_core::OutboundTime,
    cutoff: Deadline,
    turns: &mut usize,
    mut observe: impl FnMut() -> Result<bool, EdgeError>,
) -> Result<(), EdgeError> {
    let entered = time.clock().now();
    let mut previous = entered;
    loop {
        scope.check()?;
        let now = time.clock().now();
        if now < previous || cutoff.is_expired_at(now) {
            return Err(invalid());
        }
        let complete = observe()?;
        let observed = time.clock().now();
        if observed < now || cutoff.is_expired_at(observed) {
            return Err(invalid());
        }
        if complete {
            scope.check()?;
            return Ok(());
        }
        if *turns >= 128 {
            return Err(invalid());
        }
        *turns += 1;
        previous = observed;
        let next = observed
            .checked_add(Duration::from_millis(1))
            .ok_or_else(invalid)?
            .min(cutoff.instant());
        let mut timer = time.wait_until(Deadline::at_instant(next));
        std::future::poll_fn(|cx| scope.poll(time, entered, cx, timer.as_mut())).await??;
    }
}

#[cfg(feature = "native-body-witness")]
impl BatchNative {
    fn operation_observation(snapshot: &NativeBodySnapshot, start: MonotonicInstant) -> Value {
        use edgezero_core::outbound::test_support::NativeOperationPending;
        let at = |instant: MonotonicInstant| micros(instant.saturating_duration_since(start));
        match snapshot.pending_operation().or_else(|| {
            snapshot
                .first_operation_poll
                .map(NativeOperationPending::SdkPoll)
        }) {
            Some(NativeOperationPending::SdkPoll(poll)) => json!({"kind":"SdkPoll",
                "pending":poll.pending,"at_us":at(poll.started_at),"observed_at_us":at(poll.observed_at)}),
            Some(NativeOperationPending::HeadReadinessTimeout(wait)) => {
                json!({"kind":"HeadReadinessTimeout",
                "at_us":at(wait.started_at),"observed_at_us":at(wait.observed_at)})
            }
            None => Value::Null,
        }
    }

    fn json(&self) -> Value {
        json!(self.witnesses.iter().enumerate().map(|(index, witness)| {
            let snapshot = witness.snapshot();
            let at = |instant: MonotonicInstant| micros(instant.saturating_duration_since(self.start));
            json!({"index":index,"attachments":snapshot.attachments,"preparations":snapshot.preparations,
                "target_uri":snapshot.preparation.as_ref().map(|prepared|prepared.target.to_string()),"invalid":snapshot.invalid,
                "budget_cause":snapshot.preparation.as_ref().map(|prepared|format!("{:?}",prepared.budget.cause)),
                "deadline_at_us":snapshot.preparation.as_ref().map(|prepared|at(prepared.budget.deadline.instant())),
                "acquisitions":snapshot.operation_acquisitions,"polls":snapshot.operation_polls,
                "ready_polls":snapshot.ready_operation_polls,"releases":snapshot.operation_releases,
                "heads":snapshot.heads,"body_acquisitions":snapshot.source_acquisitions,"body_releases":snapshot.source_releases,
                "reads":snapshot.reads,"ready_reads":snapshot.ready_reads,
                "operation_observation":Self::operation_observation(&snapshot, self.start),
                "acquired_at_us":snapshot.first_operation_acquisition.map(at),
                "released_at_us":snapshot.first_operation_release.map(at)})
        }).collect::<Vec<_>>())
    }
}

impl BatchWitness {
    fn native_json(&self) -> Value {
        #[cfg(feature = "native-body-witness")]
        {
            self.native
                .lock()
                .unwrap()
                .as_ref()
                .map_or(Value::Null, BatchNative::json)
        }
        #[cfg(not(feature = "native-body-witness"))]
        {
            Value::Null
        }
    }
}

pub(super) fn configure_admission(app: &mut edgezero_core::app::App) {
    if option_env!("EDGEZERO_QUALIFICATION_SUITE") != Some("outbound-lifetimes") {
        return;
    }
    install_batch_admission(app);
}

fn install_batch_admission(app: &mut edgezero_core::app::App) {
    use edgezero_core::{AdmissionDecision, IngressGrant, ResponseEgressCompletion};
    use std::sync::atomic::Ordering;
    app.set_ingress_admission_policy(|head| {
        #[cfg(feature = "native-body-witness")]
        if let Some((completion,grant)) = response_cleanup::admission(head.target().path(),head.headers()) {
            return AdmissionDecision::Admit {completion,grant,
                read_deadline:head.read_deadline_after(edgezero_core::DEFAULT_INBOUND_READ_BUDGET)};
        }
        let mut completion = ResponseEgressCompletion::empty();
        let mut grant = IngressGrant::empty();
        if head.target().path() == "/progress/lifetimes/batch-write-deadline" {
            let witness = Arc::new(BatchWitness::default());
            let observed = Arc::clone(&witness);
            let token = field(head.headers(), "x-request-token").ok().filter(|token| token.len() <= 64).map(str::to_owned);
            grant = IngressGrant::new(witness);
            completion = ResponseEgressCompletion::new(move |report| {
                let record = json!({"schema":1,"event":"batch-terminal","case":"batch-write-deadline",
                    "token":token,"qualification":false,"outcome":format!("{:?}",report.outcome),
                    "body_kind":format!("{:?}",report.body_kind),"bytes_written":report.bytes_written,
                    "prefix_bytes":observed.prefix_bytes.load(Ordering::SeqCst),"elapsed_us":micros(report.elapsed),
                    "cutoff_at_us":observed.cutoff_at_us.load(Ordering::SeqCst),
                    "pending_poll":observed.pending.load(Ordering::SeqCst),"source_drops":observed.dropped.load(Ordering::SeqCst),
                    "source_failure":report.source_failure.map(|source|source.category()),
                    "native_operations":observed.native_json()});
                let line = format!("{BATCH_TERMINAL_PREFIX}{record}");
                if let Some(sink) = crate::TELEMETRY.get() { sink(&line); } else { eprintln!("{line}"); }
            });
        }
        AdmissionDecision::Admit { completion, grant,
            read_deadline: head.read_deadline_after(edgezero_core::DEFAULT_INBOUND_READ_BUDGET) }
    });
}

async fn prepare_runtime_batch(
    scope: &OutboundScope,
    probe: &Probe,
    client: &HttpClient,
    timeline: &mut Timeline,
    witness: &BatchWitness,
    time: &edgezero_core::OutboundTime,
) -> Result<(OutboundBatch, Report), EdgeError> {
    #[cfg(not(feature = "native-body-witness"))]
    let _ = time;
    use std::sync::atomic::Ordering;
    let deadline = probe.case == "batch-write-deadline";
    if !timeline.remaining(34).is_zero() {
        return Err(invalid());
    }
    let mut requests = ["a", "b", "c"]
        .into_iter()
        .map(|role| probe.request(role, timeline))
        .collect::<Result<Vec<_>, _>>()?;
    let cancel = probe.case == "batch-cancel";
    if cancel {
        // Deliver C's preflight result before A can complete. A remains an actual SDK job.
        requests[2] = probe
            .request("c", timeline)?
            .body(Body::from("invalid GET upload"));
    }
    #[cfg(feature = "native-body-witness")]
    {
        let native = BatchNative {
            start: timeline.start,
            witnesses: std::array::from_fn(|_| NativeBodyWitness::new()),
        };
        for (request, witness) in requests.iter_mut().zip(&native.witnesses) {
            witness.attach(request)?;
        }
        *witness.native.lock().unwrap() = Some(native);
    }
    let observation_cutoff = if deadline {
        timeline.clock.deadline_after(Duration::from_secs(3))
    } else {
        timeline.deadline_at(534)
    };
    witness.cutoff_at_us.store(
        timeline.elapsed_us(observation_cutoff.instant()),
        Ordering::SeqCst,
    );
    let at = timeline.clock.now();
    let mut batch = scope.with(|| client.start_batch_until(requests, observation_cutoff))?;
    timeline.event(
        "batch-start",
        at,
        timeline.clock.now(),
        json!({"cutoff_at_us":timeline.elapsed_us(observation_cutoff.instant()),"slots":3,"per_call_timeout_us":1_000_000}),
    );
    let mut report = Report::default();
    if cancel {
        check(&mut batch, &mut report, timeline).await?;
        if report.items.len() != 1
            || report.items[0]["index"] != 2
            || report.items[0]["outcome"]["error"]["kind"] != "bad_request"
        {
            return Err(invalid());
        }
    }
    #[cfg(feature = "native-body-witness")]
    let mut turns = 0;
    #[cfg(feature = "native-body-witness")]
    if cancel || probe.case == "body-check" {
        wait_for_batch_native(scope, time, observation_cutoff, &mut turns, || {
            let native = witness.native.lock().unwrap();
            let native = native.as_ref().ok_or_else(invalid)?;
            Ok(native.witnesses[1..if cancel { 2 } else { 3 }]
                .iter()
                .all(|witness| witness.snapshot().pending_operation().is_some()))
        })
        .await?;
    }
    let ready_request = probe.request("ready", timeline)?;
    let ready = scope.with(|| client.send(ready_request))?.await?;
    if ready.status().as_u16() != 204 || !ready.into_bytes_bounded(64).await?.is_empty() {
        return Err(invalid());
    }
    let at = timeline.clock.now();
    timeline.event("barrier-ready", at, at, json!(true));
    #[cfg(feature = "native-body-witness")]
    if cancel || probe.case == "body-check" {
        wait_for_batch_native(scope, time, observation_cutoff, &mut turns, || {
            let native = witness.native.lock().unwrap();
            let native = native.as_ref().ok_or_else(invalid)?;
            let snapshots = native.witnesses.each_ref().map(NativeBodyWitness::snapshot);
            Ok(batch_native_complete(&snapshots, cancel))
        })
        .await?;
    }
    if probe.case == "body-check" {
        next(&mut batch, &mut report, timeline).await;
        if report.error.is_some()
            || report.items.len() != 1
            || report.items[0]["index"] != 0
            || report.items[0]["outcome"] != json!({"status":200,"bytes":b"A"})
        {
            return Err(invalid());
        }
    }
    Ok((batch, report))
}

fn batch_body(
    probe: Probe,
    batch: OutboundBatch,
    report: Report,
    timeline: Timeline,
    witness: Arc<BatchWitness>,
) -> Result<Body, EdgeError> {
    use std::sync::atomic::Ordering;
    let deadline = probe.case == "batch-write-deadline";
    let prefix = line(&batch_prelude(&probe))?;
    witness.prefix_bytes.store(prefix.len(), Ordering::SeqCst);
    let source_drop = BatchSourceDrop(Arc::clone(&witness.dropped));
    let pending_poll = Arc::clone(&witness.pending);
    let source = futures::stream::unfold(
        (0, batch, report, timeline, probe, source_drop),
        move |(phase, mut batch, mut report, mut timeline, probe, source_drop)| {
            let prefix = prefix.clone();
            let pending = Arc::clone(&pending_poll);
            let witness = Arc::clone(&witness);
            async move {
                let chunk = match phase {
                    0 => Ok(prefix),
                    1 if deadline => {
                        let result = std::future::poll_fn(|cx| {
                            let mut next = std::pin::pin!(batch.next());
                            let result = std::future::Future::poll(next.as_mut(), cx);
                            if result.is_pending() {
                                pending.store(true, Ordering::SeqCst);
                            }
                            result
                        })
                        .await;
                        return Some((
                            Err(result.err().unwrap_or_else(invalid)),
                            (4, batch, report, timeline, probe, source_drop),
                        ));
                    }
                    1 | 2 => {
                        if check(&mut batch, &mut report, &mut timeline).await.is_err() {
                            return Some((
                                Err(invalid()),
                                (4, batch, report, timeline, probe, source_drop),
                            ));
                        }
                        line(timeline.events.last()?)
                    }
                    3 => {
                        report.batch_state = Some(json!({"delivered":[0],"in_flight":[1,2]}));
                        report.native_body = Some(witness.native_json());
                        line(&report.json(&probe, &timeline))
                    }
                    _ => return None,
                };
                Some((
                    chunk,
                    (phase + 1, batch, report, timeline, probe, source_drop),
                ))
            }
        },
    );
    Ok(Body::from_stream(source))
}

async fn batch_runtime(
    ctx: &RequestContext,
    probe: Probe,
    mut timeline: Timeline,
) -> Result<Response, EdgeError> {
    let native = super::progress::outbound_client(ctx)?;
    let time = native.time();
    let client = HttpClient::new(Arc::clone(&native));
    let deadline = probe.case == "batch-write-deadline";
    let witness = if deadline {
        ctx.take_ingress_grant()
            .and_then(|grant| grant.downcast::<Arc<BatchWitness>>().ok())
            .ok_or_else(|| {
                EdgeError::internal(std::io::Error::other(
                    "batch lifetime admission witness is absent",
                ))
            })?
    } else {
        Arc::new(BatchWitness::default())
    };
    let owner = OutboundScopeOwner::response_body();
    let scope = owner.with(|| OutboundScope::capture(native.progress()));
    let body = if deadline {
        // Egress owns the one-second clock before the 34 ms batch dispatch. The
        // unchanged per-call timers therefore expire after the response deadline.
        let delay = super::progress::pause(ctx, timeline.remaining(34))?;
        Body::from_stream(
            futures::stream::once(async move {
                delay.await;
                let (batch, report) =
                    prepare_runtime_batch(&scope, &probe, &client, &mut timeline, &witness, &time)
                        .await?;
                batch_body(probe, batch, report, timeline, witness)?
                    .into_stream()
                    .map_err(|_| invalid())
            })
            .try_flatten(),
        )
    } else {
        timeline.wait_until(ctx, 34).await?;
        let (batch, mut report) =
            prepare_runtime_batch(&scope, &probe, &client, &mut timeline, &witness, &time).await?;
        if probe.case == "batch-cancel" {
            let recorded = witness.native_json();
            let at = timeline.clock.now();
            let indices = batch.cancel();
            timeline.event("cancel", at, timeline.clock.now(), json!(indices));
            report.batch_state = Some(
                json!({"recorded_unobserved":0,"recorded_kind":"sdk-buffered-response",
                "delivered":[2],"in_flight":[1],"cancelled":indices,"before_cancel":recorded}),
            );
            report.native_body = Some(witness.native_json());
            return response_builder()
                .header("content-type", "application/json")
                .body(Body::json(&report.json(&probe, &timeline)).map_err(EdgeError::internal)?)
                .map_err(EdgeError::internal);
        }
        batch_body(probe, batch, report, timeline, witness)?
    };
    let mut response = response_builder()
        .header("content-type", "application/x-ndjson")
        .body(body)
        .map_err(EdgeError::internal)?;
    if deadline {
        response
            .extensions_mut()
            .insert(edgezero_core::ResponseEgressDeadline::after(
                Duration::from_secs(1),
            ));
    }
    owner.attach(response)
}

#[action]
pub(super) async fn probe(ctx: RequestContext) -> Result<Response, EdgeError> {
    let case = ctx.path_params().get("case").ok_or_else(invalid)?;
    #[cfg(feature = "native-body-witness")]
    if response_cleanup::CASES.contains(&case) {
        return response_cleanup::handle(ctx).await;
    }
    let probe = Probe::parse(case, ctx.headers())?;
    let mut timeline = Timeline::new(ctx.monotonic_clock(), ctx.request_start());
    if matches!(
        probe.case.as_str(),
        "batch-write-deadline" | "batch-cancel" | "body-check"
    ) {
        return batch_runtime(&ctx, probe, timeline).await;
    }
    let client = ctx.http_client().ok_or_else(|| {
        EdgeError::internal(std::io::Error::other(
            "lifetime probe has no injected client",
        ))
    })?;
    if probe.case == "begin-upload-rules" {
        return upload_rules(ctx, probe, client).await;
    }
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

async fn upload_rules(
    ctx: RequestContext,
    probe: Probe,
    client: HttpClient,
) -> Result<Response, EdgeError> {
    use std::sync::atomic::{AtomicUsize, Ordering};
    let target = Uri::builder()
        .scheme("http")
        .authority(probe.base.authority().cloned().ok_or_else(invalid)?)
        .path_and_query(format!("/forward/send/{}", probe.token))
        .build()
        .map_err(EdgeError::internal)?;
    let streamed_post = |polls: Arc<AtomicUsize>| -> Result<OutboundRequest, EdgeError> {
        OutboundRequest::new(Method::POST, target.clone())?
            .header("x-request-token", &probe.token)
            .map(|request| {
                request
                    .timeout(Duration::from_secs(5))
                    .max_response_bytes(64)
                    .body(Body::from_stream(futures::stream::once(async move {
                        polls.fetch_add(1, Ordering::SeqCst);
                        Ok(Bytes::from_static(b"POST"))
                    })))
            })
    };
    let rejected_polls = Arc::new(AtomicUsize::new(0));
    let rejected = match client
        .begin(streamed_post(Arc::clone(&rejected_polls))?)
        .await
    {
        Err(error) => error_report(&error),
        Ok(pending) => {
            pending.cancel();
            return Err(invalid());
        }
    };
    let incoming = ctx.into_request()?;
    if incoming.method() != Method::GET {
        return Err(invalid());
    }
    let get = OutboundRequest::from_request(incoming, target.clone())?
        .timeout(Duration::from_secs(5))
        .max_response_bytes(64);
    let begun = response_report(client.begin(get).await?.collect().await).await;
    let sent_polls = Arc::new(AtomicUsize::new(0));
    let sent = response_report(client.send(streamed_post(Arc::clone(&sent_polls))?).await).await;
    response_builder().header("content-type","application/json")
        .body(Body::json(&json!({"schema":1,"case":probe.case,"token":probe.token,"qualification":false,
            "begin_post":rejected,"begin_post_polls":rejected_polls.load(Ordering::SeqCst),
            "begin_get":begun,"send_post":sent,"send_post_polls":sent_polls.load(Ordering::SeqCst)}))
            .map_err(EdgeError::internal)?).map_err(EdgeError::internal)
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

    #[derive(Default)]
    struct HandoffJobs(
        RefCell<futures::stream::FuturesUnordered<futures::future::LocalBoxFuture<'static, ()>>>,
    );

    struct HandoffProgress(TestTime);

    impl edgezero_core::OutboundProgressRuntime for HandoffProgress {
        fn poll(
            &self,
            scope: &OutboundScope,
            _: &edgezero_core::OutboundTime,
            _: MonotonicInstant,
            cx: &mut std::task::Context<'_>,
            inner: &mut dyn FnMut(&mut std::task::Context<'_>) -> std::task::Poll<()>,
        ) -> std::task::Poll<Result<(), EdgeError>> {
            let jobs = scope.provider_state(HandoffJobs::default)?;
            if let Ok(mut tasks) = jobs.0.try_borrow_mut() {
                while let std::task::Poll::Ready(Some(())) = tasks.poll_next_unpin(cx) {}
            }
            let result = inner(cx);
            if result.is_pending() && jobs.0.try_borrow().is_ok_and(|tasks| !tasks.is_empty()) {
                self.0
                    .advance_to(self.0.clock().now() + Duration::from_millis(1));
                cx.waker().wake_by_ref();
            }
            result.map(Ok)
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
            #[cfg(feature = "native-body-witness")]
            let recorder = edgezero_core::outbound::test_support::native_body_recorder(&context);
            let role = parts.uri.path().rsplit('/').next().unwrap().to_owned();
            let payload = match role.as_str() {
                "origin" => b'O',
                "a" => b'A',
                "b" => b'B',
                "c" => b'C',
                _ => panic!("unexpected handoff job"),
            };
            let (send, receive) = futures::channel::oneshot::channel();
            #[cfg(feature = "native-body-witness")]
            let mut receive =
                edgezero_core::outbound::test_support::own_sdk_operation(receive, recorder.clone());
            #[cfg(not(feature = "native-body-witness"))]
            let mut receive = receive;
            assert!(self.jobs.lock().unwrap().insert(role, send).is_none());
            let resources = Arc::clone(&self.resources);
            let owner = edgezero_core::OutboundExchangeOwner::new(move || {
                resources.fetch_add(1, Ordering::SeqCst);
            });
            let polls = Arc::clone(&self.polls);
            Box::pin(async move {
                std::future::poll_fn(|cx| {
                    polls.fetch_add(1, Ordering::SeqCst);
                    futures::FutureExt::poll_unpin(&mut receive, cx)
                })
                .await
                .map_err(|_| invalid())?;
                drop(receive);
                #[cfg(feature = "native-body-witness")]
                if let Some(recorder) = &recorder {
                    recorder.head_received();
                }
                let source = futures::stream::once(async move { Ok(Bytes::from(vec![payload])) })
                    .boxed_local();
                #[cfg(feature = "native-body-witness")]
                let source =
                    edgezero_core::outbound::test_support::observe_sdk_stream(source, recorder);
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
            Rc::new(HandoffProgress(self.time.clone()))
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
            request: edgezero_core::OutboundDispatchRequest,
        ) -> edgezero_core::OutboundFuture<edgezero_core::OutboundHead> {
            if request.request().uri().path().ends_with("/ready")
                && !request
                    .request()
                    .uri()
                    .path()
                    .contains("batch-write-deadline")
            {
                let expected = if request.request().uri().path().contains("batch-cancel") {
                    2
                } else {
                    3
                };
                assert!(
                    self.polls.load(Ordering::SeqCst) >= expected,
                    "pending operations must be witnessed before the ready request releases A"
                );
            }
            if !request
                .request()
                .uri()
                .path()
                .contains("batch-write-deadline")
                && request.request().uri().path().ends_with("/ready")
                && let Some(send) = self.jobs.lock().unwrap().remove("a")
            {
                send.send(()).unwrap();
            }
            let (parts, context) = request.into_parts();
            let calls = Arc::clone(&self.calls);
            Box::pin(async move {
                let forward = parts.uri.path().starts_with("/forward/send/");
                if !forward && !parts.uri.path().ends_with("/ready") {
                    return Err(invalid());
                }
                let bytes = parts.body.into_bytes_bounded(8).await?;
                if forward {
                    calls.fetch_add(1, Ordering::SeqCst);
                    if !((parts.method == Method::GET && bytes.is_empty())
                        || (parts.method == Method::POST && bytes == b"POST".as_slice()))
                    {
                        return Err(invalid());
                    }
                }
                edgezero_core::OutboundHead::new(
                    if forward {
                        StatusCode::CREATED
                    } else {
                        StatusCode::NO_CONTENT
                    },
                    edgezero_core::OutboundHeaderInput::new(
                        "unit",
                        HeaderMap::new(),
                        edgezero_core::OutboundHeaderFidelity::AdapterVisibleFields,
                    ),
                    futures::stream::iter(forward.then(|| Ok(Bytes::from_static(b"end"))))
                        .boxed_local(),
                    context,
                    edgezero_core::OutboundExchangeOwner::new(|| {}),
                )
            })
        }
        fn begin(
            &self,
            request: edgezero_core::OutboundDispatchRequest,
        ) -> Result<edgezero_core::OutboundPending, EdgeError> {
            if request.request().uri().path().starts_with("/forward/send/") {
                let head = edgezero_core::OutboundHttpClient::dispatch_head(self, request);
                return Ok(edgezero_core::OutboundPending::new(
                    Box::pin(async move { head.await?.finish().await }),
                    edgezero_core::OutboundExchangeOwner::new(|| {}),
                ));
            }
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
            let background = context
                .scope()
                .provider_state(HandoffJobs::default)
                .unwrap();
            let (send, receive) = futures::channel::mpsc::unbounded();
            for (index, request) in requests {
                let response = self.queue(request);
                let clock = time.clone();
                let send = send.clone();
                background.0.borrow().push(Box::pin(async move {
                    let outcome = response.await;
                    let _ = send.unbounded_send(edgezero_core::OutboundBatchDriverEvent::Item {
                        index,
                        completed_at: clock.now(),
                        outcome,
                    });
                }));
            }
            drop(send);
            let jobs = Arc::clone(&self.jobs);
            let groups = Arc::clone(&self.groups);
            OutboundBatch::from_driver(
                context,
                receive,
                edgezero_core::OutboundExchangeOwner::new(move || {
                    *background.0.borrow_mut() = futures::stream::FuturesUnordered::new();
                    let mut jobs = jobs.lock().unwrap();
                    for role in ["a", "b", "c"] {
                        jobs.remove(role);
                    }
                    groups.fetch_add(1, Ordering::SeqCst);
                }),
            )
        }
    }

    struct HandoffRuntime(
        TestTime,
        Option<HandoffClient>,
        Arc<std::sync::atomic::AtomicUsize>,
    );
    impl super::super::progress::ProbeRuntime for HandoffRuntime {
        fn adapter(&self) -> &'static str {
            "unit"
        }
        fn outbound_client(&self) -> Result<Arc<dyn edgezero_core::OutboundHttpClient>, EdgeError> {
            assert_eq!(
                self.2.fetch_add(1, Ordering::SeqCst),
                0,
                "one native factory call"
            );
            self.1
                .clone()
                .map(|client| Arc::new(client) as Arc<dyn edgezero_core::OutboundHttpClient>)
                .ok_or_else(invalid)
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
            Arc::new(HandoffRuntime(time.clone(), None, Arc::default())),
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
    fn runtime_batch_probes_have_fixed_cases_and_independent_budgets() {
        let time = TestTime::default();
        let timeline = Timeline::new(time.clock(), time.clock().now());
        for case in ["batch-write-deadline", "batch-cancel", "body-check"] {
            let probe = Probe::parse(case, &headers(case)).expect("supported runtime case");
            for role in ["a", "b", "c", "ready"] {
                let parts = probe.request(role, &timeline).unwrap().into_parts();
                assert_eq!(parts.timeout, Some(Duration::from_secs(1)));
                assert!(parts.deadline.is_none());
                assert!(matches!(
                    parts.response_mode,
                    ResponseMode::Buffered { max_bytes: 64 }
                ));
            }
        }
    }

    #[cfg(feature = "native-body-witness")]
    fn buffered_a_snapshot(body_releases: u64) -> NativeBodySnapshot {
        NativeBodySnapshot {
            attachments: 1,
            preparations: 1,
            operation_acquisitions: 1,
            operation_polls: 1,
            ready_operation_polls: 1,
            operation_releases: 1,
            heads: 1,
            source_acquisitions: 1,
            source_releases: body_releases,
            reads: 2,
            ready_reads: 1,
            ..NativeBodySnapshot::default()
        }
    }

    #[test]
    #[cfg(feature = "native-body-witness")]
    fn batch_operation_observation_keeps_actual_sdk_poll_and_ready_fallback() {
        use edgezero_core::outbound::test_support::NativeOperationPoll;
        let time = TestTime::default();
        let start = time.clock().now();
        for pending in [true, false] {
            let snapshot = NativeBodySnapshot {
                first_operation_poll: Some(NativeOperationPoll {
                    started_at: start + Duration::from_micros(34_000),
                    observed_at: start + Duration::from_micros(34_010),
                    pending,
                }),
                ..NativeBodySnapshot::default()
            };
            assert_eq!(
                BatchNative::operation_observation(&snapshot, start),
                json!({"kind":"SdkPoll","pending":pending,"at_us":34_000,"observed_at_us":34_010})
            );
        }
    }

    #[test]
    #[cfg(feature = "native-body-witness")]
    fn batch_operation_observation_prioritizes_historical_head_wait_without_fake_poll() {
        use edgezero_core::outbound::test_support::{NativeBodyWaitOutcome, NativeOperationPoll};
        let time = TestTime::default();
        let start = time.clock().now();
        let mut snapshot = NativeBodySnapshot {
            first_head_wait_timeout: Some(NativeBodyWait {
                started_at: start + Duration::from_micros(34_000),
                observed_at: start + Duration::from_micros(35_000),
                outcome: NativeBodyWaitOutcome::TimedOut,
            }),
            ..NativeBodySnapshot::default()
        };
        let expected =
            json!({"kind":"HeadReadinessTimeout","at_us":34_000,"observed_at_us":35_000});
        assert_eq!(
            BatchNative::operation_observation(&snapshot, start),
            expected
        );
        assert_eq!(snapshot.operation_polls, 0);
        snapshot.first_operation_poll = Some(NativeOperationPoll {
            started_at: start + Duration::from_micros(36_000),
            observed_at: start + Duration::from_micros(36_010),
            pending: false,
        });
        assert_eq!(
            BatchNative::operation_observation(&snapshot, start),
            expected
        );
    }

    #[test]
    #[cfg(feature = "native-body-witness")]
    fn batch_operation_observation_is_null_for_unprepared_c_and_has_no_old_alias() {
        let time = TestTime::default();
        let native = BatchNative {
            start: time.clock().now(),
            witnesses: std::array::from_fn(|_| NativeBodyWitness::new()),
        };
        for slot in native.json().as_array().unwrap() {
            assert!(slot.get("first_poll").is_none());
            assert!(slot.get("pending_operation").is_none());
            assert_eq!(slot.get("operation_observation"), Some(&Value::Null));
        }
    }

    #[test]
    #[cfg(feature = "native-body-witness")]
    fn cancel_barrier_requires_body_release_not_only_a_ready_head() {
        assert!(!buffered_a_complete(&buffered_a_snapshot(0)));
        assert!(buffered_a_complete(&buffered_a_snapshot(1)));
        let mut invalid = buffered_a_snapshot(1);
        invalid.ready_reads = 0;
        assert!(!buffered_a_complete(&invalid));
        invalid = buffered_a_snapshot(1);
        invalid.invalid = true;
        assert!(!buffered_a_complete(&invalid));
        invalid = buffered_a_snapshot(1);
        invalid.ready_operation_polls = 2;
        assert!(!buffered_a_complete(&invalid));
    }

    #[test]
    #[cfg(feature = "native-body-witness")]
    fn cancel_barrier_waits_on_paired_timer_until_delayed_eof() {
        use std::task::{Context, Waker};
        let time = TestTime::default();
        let pair = time.pair();
        let start = time.clock().now();
        time.advance_to(start + Duration::from_millis(37));
        let owner = OutboundScopeOwner::response_body();
        let scope = owner.scope();
        let released = Rc::new(Cell::new(false));
        let observed = Rc::clone(&released);
        let observations = Rc::new(Cell::new(0));
        let samples = Rc::clone(&observations);
        let mut turns = 0;
        let mut wait = Box::pin(wait_for_batch_native(
            &scope,
            &pair,
            Deadline::at_instant(start + Duration::from_millis(534)),
            &mut turns,
            move || {
                samples.set(samples.get() + 1);
                Ok(buffered_a_complete(&buffered_a_snapshot(u64::from(
                    observed.get(),
                ))))
            },
        ));
        let mut cx = Context::from_waker(Waker::noop());
        assert!(std::future::Future::poll(wait.as_mut(), &mut cx).is_pending());
        assert_eq!(observations.get(), 1);
        released.set(true);
        time.advance_to(start + Duration::from_millis(38));
        assert!(matches!(
            std::future::Future::poll(wait.as_mut(), &mut cx),
            std::task::Poll::Ready(Ok(()))
        ));
        assert_eq!(observations.get(), 2);
        assert_eq!(pair.clock().now(), start + Duration::from_millis(38));
    }

    #[test]
    #[cfg(feature = "native-body-witness")]
    fn cancel_barrier_missing_eof_fails_at_original_cutoff() {
        use std::task::{Context, Waker};
        let time = TestTime::default();
        let pair = time.pair();
        let start = time.clock().now();
        time.advance_to(start + Duration::from_micros(533_500));
        let owner = OutboundScopeOwner::response_body();
        let scope = owner.scope();
        let mut turns = 0;
        let mut wait = Box::pin(wait_for_batch_native(
            &scope,
            &pair,
            Deadline::at_instant(start + Duration::from_millis(534)),
            &mut turns,
            || Ok(buffered_a_complete(&buffered_a_snapshot(0))),
        ));
        let mut cx = Context::from_waker(Waker::noop());
        assert!(std::future::Future::poll(wait.as_mut(), &mut cx).is_pending());
        time.advance_to(start + Duration::from_millis(534));
        assert!(matches!(
            std::future::Future::poll(wait.as_mut(), &mut cx),
            std::task::Poll::Ready(Err(_))
        ));
        assert_eq!(pair.clock().now(), start + Duration::from_millis(534));
    }

    #[test]
    #[cfg(feature = "native-body-witness")]
    fn cancel_barrier_has_at_most_128_timer_turns() {
        let time = TestTime::default();
        let start = time.clock().now();
        time.advance_to(start + Duration::from_millis(34));
        let timers = Arc::new(AtomicU64::new(0));
        let count = Arc::clone(&timers);
        let advanced = time.clone();
        let pair = edgezero_core::OutboundTime::new(time.clock(), move |deadline, _| {
            count.fetch_add(1, Ordering::SeqCst);
            advanced.advance_to(deadline.instant());
            Box::pin(async { Ok(()) })
        });
        let owner = OutboundScopeOwner::response_body();
        assert!(
            block_on(wait_for_batch_native(
                &owner.scope(),
                &pair,
                Deadline::at_instant(start + Duration::from_millis(534)),
                &mut 0,
                || Ok(buffered_a_complete(&buffered_a_snapshot(0)))
            ))
            .is_err()
        );
        assert_eq!(timers.load(Ordering::SeqCst), 128);
        assert_eq!(pair.clock().now(), start + Duration::from_millis(162));
    }

    #[test]
    #[cfg(feature = "native-body-witness")]
    fn batch_native_barrier_requires_pending_witnesses_for_only_the_active_slots() {
        use edgezero_core::outbound::test_support::{NativeBodyWaitOutcome, NativeOperationPoll};
        let time = TestTime::default();
        let at = time.clock().now();
        let mut snapshots = [
            buffered_a_snapshot(1),
            NativeBodySnapshot::default(),
            NativeBodySnapshot::default(),
        ];
        assert!(!batch_native_complete(&snapshots, true));
        snapshots[1].first_operation_poll = Some(NativeOperationPoll {
            pending: true,
            started_at: at,
            observed_at: at,
        });
        assert!(batch_native_complete(&snapshots, true));
        assert!(!batch_native_complete(&snapshots, false));
        snapshots[2].first_head_wait_timeout = Some(NativeBodyWait {
            outcome: NativeBodyWaitOutcome::TimedOut,
            started_at: at,
            observed_at: at + Duration::from_millis(1),
        });
        assert!(batch_native_complete(&snapshots, false));
        snapshots[0].source_releases = 0;
        assert!(!batch_native_complete(&snapshots, true));
        snapshots[0].source_releases = 1;
        snapshots[1].invalid = true;
        assert!(!batch_native_complete(&snapshots, true));
    }

    #[test]
    #[cfg(feature = "native-body-witness")]
    fn batch_native_barrier_phases_share_one_timer_turn_limit() {
        let time = TestTime::default();
        let start = time.clock().now();
        let advanced = time.clone();
        let timers = Arc::new(AtomicU64::new(0));
        let counted = Arc::clone(&timers);
        let pair = edgezero_core::OutboundTime::new(time.clock(), move |deadline, _| {
            counted.fetch_add(1, Ordering::SeqCst);
            advanced.advance_to(deadline.instant());
            Box::pin(async { Ok(()) })
        });
        let owner = OutboundScopeOwner::response_body();
        let scope = owner.scope();
        let cutoff = Deadline::at_instant(start + Duration::from_millis(534));
        let mut turns = 127;
        let mut observations = 0;
        block_on(wait_for_batch_native(
            &scope,
            &pair,
            cutoff,
            &mut turns,
            || {
                observations += 1;
                Ok(observations == 2)
            },
        ))
        .unwrap();
        assert_eq!(turns, 128);
        assert!(
            block_on(wait_for_batch_native(
                &scope,
                &pair,
                cutoff,
                &mut turns,
                || Ok(false)
            ))
            .is_err()
        );
        assert_eq!(turns, 128);
        assert_eq!(timers.load(Ordering::SeqCst), 1);
        assert_eq!(pair.clock().now(), start + Duration::from_millis(1));
    }

    #[test]
    #[cfg(feature = "native-body-witness")]
    fn batch_native_barrier_rejects_backwards_time_during_observation() {
        let time = TestTime::default();
        let pair = time.pair();
        let entered = time.clock().now();
        let owner = OutboundScopeOwner::response_body();
        time.advance_to(entered + Duration::from_millis(1));
        assert!(
            block_on(wait_for_batch_native(
                &owner.scope(),
                &pair,
                Deadline::at_instant(entered + Duration::from_millis(534)),
                &mut 0,
                || {
                    time.advance_to(entered);
                    Ok(true)
                },
            ))
            .is_err()
        );
    }

    #[test]
    #[cfg(feature = "native-body-witness")]
    fn cancel_barrier_rejects_closed_scope_missing_witness_and_cutoff_equality() {
        let time = TestTime::default();
        let pair = time.pair();
        let cutoff = pair.clock().deadline_after(Duration::from_millis(534));
        let owner = OutboundScopeOwner::response_body();
        let scope = owner.scope();
        assert!(
            block_on(wait_for_batch_native(
                &scope,
                &pair,
                cutoff,
                &mut 0,
                || Err(invalid())
            ))
            .is_err()
        );
        time.advance_to(cutoff.instant());
        assert!(
            block_on(wait_for_batch_native(&scope, &pair, cutoff, &mut 0, || Ok(
                buffered_a_complete(&buffered_a_snapshot(1))
            )))
            .is_err()
        );
        let later = pair.clock().deadline_after(Duration::from_millis(1));
        owner.close();
        let error = block_on(wait_for_batch_native(&scope, &pair, later, &mut 0, || {
            panic!("closed scope sampled witness")
        }))
        .unwrap_err();
        assert!(matches!(error, EdgeError::Internal { .. }));
    }

    #[test]
    #[cfg(feature = "native-body-witness")]
    fn cancel_barrier_preserves_paired_timer_failure() {
        let time = TestTime::default();
        let pair = edgezero_core::OutboundTime::new(time.clock(), |_, _| {
            Box::pin(async { Err(EdgeError::bad_gateway("timer failure")) })
        });
        let owner = OutboundScopeOwner::response_body();
        let error = block_on(wait_for_batch_native(
            &owner.scope(),
            &pair,
            pair.clock().deadline_after(Duration::from_millis(534)),
            &mut 0,
            || Ok(buffered_a_complete(&buffered_a_snapshot(0))),
        ))
        .unwrap_err();
        assert!(matches!(error, EdgeError::BadGateway { .. }));
        assert_eq!(error.message(), "timer failure");
    }

    #[test]
    #[cfg(feature = "native-body-witness")]
    fn shared_snapshot_preserves_sdk_operation_lifetime_samples() {
        let time = TestTime::default();
        let start = time.clock().now();
        let timeline = Timeline::new(time.clock(), start);
        let acquired = start + Duration::from_millis(34);
        let polled = start + Duration::from_millis(35);
        let released = start + Duration::from_millis(36);
        let snapshot = NativeBodySnapshot {
            operation_acquisitions: 1,
            operation_polls: 3,
            operation_releases: 1,
            first_operation_acquisition: Some(acquired),
            first_operation_release: Some(released),
            first_operation_poll: Some(
                edgezero_core::outbound::test_support::NativeOperationPoll {
                    started_at: polled,
                    observed_at: polled,
                    pending: true,
                },
            ),
            ..NativeBodySnapshot::default()
        };
        let report = native_body_report(snapshot, &timeline);
        assert_eq!(report["operation_acquisitions"], 1);
        assert_eq!(report["operation_polls"], 3);
        assert_eq!(report["operation_releases"], 1);
        assert_eq!(report["operation_acquired_at_us"], 34000);
        assert_eq!(report["operation_released_at_us"], 36000);
        assert_eq!(
            report["first_operation_poll"],
            json!({"started_at_us":35000,"observed_at_us":35000,"pending":true})
        );
    }

    #[test]
    fn runtime_batch_routes_preserve_delivered_and_unobserved_slots_through_body_work() {
        for case in [
            "batch-cancel",
            "body-check",
            "batch-write-deadline",
            "begin-upload-rules",
        ] {
            runtime_batch_route(case);
        }
    }

    fn runtime_batch_route(case: &str) {
        use edgezero_core::response_egress_framing::ResponseEgressFramingMode;
        use edgezero_core::{
            IngressDispatchOutcome, IngressFraming, IngressHeadAccounting, ResponseEgressOutcome,
            app::App,
        };
        use futures::FutureExt as _;
        use std::sync::{Mutex, atomic::AtomicUsize};
        let time = TestTime::default();
        let start = time.clock().now();
        let client = HandoffClient {
            time: time.clone(),
            jobs: Arc::new(Mutex::new(std::collections::BTreeMap::new())),
            resources: Arc::new(AtomicUsize::new(0)),
            cancelled: Arc::new(AtomicUsize::new(0)),
            family: Arc::new(AtomicUsize::new(0)),
            groups: Arc::new(AtomicUsize::new(0)),
            calls: Arc::new(AtomicUsize::new(0)),
            polls: Arc::new(AtomicUsize::new(0)),
        };
        let factories = Arc::new(AtomicUsize::new(0));
        let mut app = App::new(super::super::progress::router(
            Arc::new(HandoffRuntime(
                time.clone(),
                Some(client.clone()),
                Arc::clone(&factories),
            )),
            Value::Null,
            None,
        ));
        app.set_monotonic_clock(time.clock());
        install_batch_admission(&mut app);
        let reports = TerminalReports::default();
        app.set_response_egress_observer(reports.clone());
        let mut request = request_builder()
            .uri(format!("/progress/lifetimes/{case}"))
            .body(Body::empty())
            .unwrap();
        *request.headers_mut() = headers(case);
        if case == "begin-upload-rules" {
            request
                .extensions_mut()
                .insert(HttpClient::with_client(client.clone()));
        }
        let callback = OutboundScopeOwner::callback();
        let outcome = block_on(callback.run(app.dispatch_ingress(
            request,
            start,
            IngressHeadAccounting::HostManaged,
            IngressFraming::HostManaged,
        )))
        .unwrap();
        let IngressDispatchOutcome::Response(envelope) = outcome else {
            panic!("batch response")
        };
        let (prepared, policy, mut attempt, _) = envelope
            .begin(ResponseEgressFramingMode::ExplicitBufferedLength)
            .unwrap_or_else(|_| panic!("batch envelope"));
        assert!(attempt.begin_writing());
        callback.close();
        let response = prepared.into_response();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            factories.load(Ordering::SeqCst),
            usize::from(case != "begin-upload-rules")
        );
        if case == "begin-upload-rules" {
            let bytes = block_on(response.into_body().into_bytes_bounded(16384)).unwrap();
            let report: Value = serde_json::from_slice(&bytes).unwrap();
            assert_eq!(
                report["begin_post"],
                json!({"kind":"bad_request","status":400,"reason":null})
            );
            assert_eq!(report["begin_post_polls"], 0);
            assert_eq!(report["begin_get"], json!({"status":201,"bytes":b"end"}));
            assert_eq!(report["send_post"], json!({"status":201,"bytes":b"end"}));
            assert_eq!(report["send_post_polls"], 1);
            assert_eq!(client.calls.load(Ordering::SeqCst), 2);
            assert_eq!(client.groups.load(Ordering::SeqCst), 0);
            return;
        }
        if case == "batch-cancel" {
            assert_eq!(
                client.calls.load(Ordering::SeqCst),
                2,
                "A and B reach transport; delivered preflight C does not"
            );
            assert_eq!(client.groups.load(Ordering::SeqCst), 1);
            let bytes = block_on(response.into_body().into_bytes_bounded(16384)).unwrap();
            let report: Value = serde_json::from_slice(&bytes).unwrap();
            assert_eq!(
                report["batch_state"],
                json!({"recorded_unobserved":0,"recorded_kind":"sdk-buffered-response","delivered":[2],"in_flight":[1],"cancelled":[0,1],"before_cancel":report["batch_state"]["before_cancel"]})
            );
            assert_eq!(report["items"].as_array().unwrap().len(), 1);
            assert_eq!(report["items"][0]["index"], 2);
            assert_eq!(report["events"][1]["op"], "check");
            assert_eq!(report["events"][2]["op"], "barrier-ready");
            assert_eq!(report["events"][3]["result"], json!([0, 1]));
            assert_eq!(report["events"].as_array().unwrap().len(), 4);
            #[cfg(feature = "native-body-witness")]
            {
                let recorded = &report["batch_state"]["before_cancel"];
                assert_eq!(recorded[0]["heads"], 1);
                assert_eq!(recorded[0]["acquisitions"], 1);
                assert_eq!(recorded[0]["releases"], 1);
                assert_eq!(recorded[0]["ready_polls"], 1);
                assert_eq!(recorded[0]["body_acquisitions"], 1);
                assert_eq!(recorded[0]["body_releases"], 1);
                assert!(recorded[0]["ready_reads"].as_u64().unwrap() > 0);
                assert_eq!(recorded[1]["operation_observation"]["kind"], "SdkPoll");
                assert_eq!(recorded[1]["operation_observation"]["pending"], true);
                assert_eq!(recorded[2]["operation_observation"], Value::Null);
            }
            return;
        }
        assert_eq!(
            client.calls.load(Ordering::SeqCst),
            if case == "batch-write-deadline" { 0 } else { 3 }
        );
        assert_eq!(client.groups.load(Ordering::SeqCst), 0);
        // Canonical adapters extract the egress owner before consuming the source.
        // Do not use Body::into_stream here: its manual-consumer cleanup is different.
        let (payload, owner) = response.into_body().into_parts();
        assert!(owner.is_none(), "attempt already owns response egress");
        let edgezero_core::BodyPayload::Stream(mut source) = payload else {
            panic!("streamed batch body")
        };
        let prefix = block_on(source.next()).unwrap().unwrap();
        assert_eq!(client.calls.load(Ordering::SeqCst), 3);
        assert_eq!(
            serde_json::from_slice::<Value>(&prefix).unwrap()["phase"],
            "batch-pending"
        );
        if case == "batch-write-deadline" {
            assert_eq!(
                policy.write_deadline.instant(),
                start + Duration::from_secs(1)
            );
            assert!(source.next().now_or_never().is_none());
            assert_eq!(client.groups.load(Ordering::SeqCst), 0);
            time.advance_to(start + Duration::from_secs(1));
            drop(source);
            assert_eq!(
                client.groups.load(Ordering::SeqCst),
                1,
                "canonical source release drops the live batch"
            );
            assert!(attempt.terminate(ResponseEgressOutcome::DeadlineExceeded, time.clock().now()));
            assert_eq!(
                *reports.0.lock().unwrap(),
                [ResponseEgressOutcome::DeadlineExceeded]
            );
        } else {
            for _ in 0..2 {
                let bytes = block_on(source.next()).unwrap().unwrap();
                let check: Value = serde_json::from_slice(&bytes).unwrap();
                assert_eq!(check["op"], "check");
                assert_eq!(check["result"], "Pending");
                assert!(check["duration_us"].as_u64().unwrap() < 5000);
                assert_eq!(client.groups.load(Ordering::SeqCst), 0);
            }
            let bytes = block_on(source.next()).unwrap().unwrap();
            let report: Value = serde_json::from_slice(&bytes).unwrap();
            assert_eq!(
                report["batch_state"],
                json!({"delivered":[0],"in_flight":[1,2]})
            );
            assert_eq!(report["items"].as_array().unwrap().len(), 1);
            assert_eq!(report["items"][0]["index"], 0);
            assert_eq!(report["events"][0]["at_us"], 34000);
            assert_eq!(report["events"][0]["result"]["cutoff_at_us"], 534000);
            assert!(block_on(source.next()).is_none());
            assert_eq!(client.groups.load(Ordering::SeqCst), 1);
        }
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
    fn trickle_reader_records_real_receipts_and_eof_in_the_original_clock() {
        let time = TestTime::default();
        let start = time.clock().now();
        let timeline = Timeline::new(time.clock(), start);
        time.advance_to(start + Duration::from_millis(100));
        let source_time = time.clone();
        let source = futures::stream::iter(0..11).map(move |index| {
            source_time.advance_to(start + Duration::from_millis(100 + index * 50));
            Ok(Bytes::from(vec![48 + index as u8]))
        });
        let observations = Rc::new(std::cell::RefCell::new(Vec::new()));
        let outcome = block_on(read_trickle(
            response(Body::from_stream(source)),
            &timeline,
            &observations,
        ));
        assert_eq!(outcome, json!({"status":200,"bytes":b"0123456789:"}));
        let log = observations.borrow();
        assert_eq!(log.len(), 12);
        for (index, entry) in log[..11].iter().enumerate() {
            assert_eq!(entry["op"], "read");
            assert_eq!(entry["at_us"], 100000 + index as u64 * 50000);
            assert_eq!(entry["result"], json!([48 + index]));
        }
        assert_eq!(log[11]["at_us"], 600000);
        assert!(log[11]["result"].is_null());
    }

    #[test]
    fn trickle_reader_bounds_empty_reads_and_payload_before_copying() {
        for bytes in [Bytes::new(), Bytes::from(vec![0; 65])] {
            let time = TestTime::default();
            let timeline = Timeline::new(time.clock(), time.clock().now());
            let polls = Rc::new(Cell::new(0));
            let observed = Rc::clone(&polls);
            let oversized = !bytes.is_empty();
            let source = futures::stream::repeat_with(move || {
                observed.set(observed.get() + 1);
                Ok(bytes.clone())
            });
            let log = Rc::new(std::cell::RefCell::new(Vec::new()));
            let outcome = block_on(read_trickle(
                response(Body::from_stream(source)),
                &timeline,
                &log,
            ));
            assert!(outcome.get("error").is_some());
            assert_eq!(polls.get(), if oversized { 1 } else { 64 });
            assert_eq!(log.borrow().len(), polls.get());
            if oversized {
                assert!(log.borrow()[0]["result"].get("error").is_some());
            }
        }
    }

    #[test]
    fn requests_use_actual_guest_elapsed_and_original_limits() {
        let time = TestTime::default();
        let start = time.clock().now();
        let timeline = Timeline::new(time.clock(), start);
        for case in [
            "slow-origin",
            "fast-origin",
            "body-read-arrivals",
            "trickling-slot",
            "send-during-batch",
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
