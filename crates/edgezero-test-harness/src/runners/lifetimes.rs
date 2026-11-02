use super::*;
use crate::net::WireOrigins;
mod callback;
mod cleanup;
mod protocol;

#[derive(Clone, Copy)]
struct AdapterBoundary<'a> {
    name: &'a str,
    origin: &'a str,
}

struct CapsClockBounds {
    epoch: [i128; 2],
    host_epoch: [i128; 2],
    offset_upper: i128,
    terminal: [i128; 2],
    collection_upper: i128,
    window_end: i128,
    native_return: i128,
}

impl CapsClockBounds {
    fn evidence(&self) -> Value {
        json!({"basis":"causal-receipts-epoch-and-diagnostic-return",
            "epoch_lower_us":self.epoch[0],"epoch_upper_us":self.epoch[1],
            "host_epoch_lower_us":self.host_epoch[0],"host_epoch_upper_us":self.host_epoch[1],
            "offset_upper_us":self.offset_upper,"terminal_lower_us":self.terminal[0],
            "terminal_upper_us":self.terminal[1],"collection_upper_us":self.collection_upper,
            "window_end_us":self.window_end,"native_http_response_us":self.native_return,
            "rounding_guard_us":1000,"same_rate_assumed":true,"clock_sync_proven":false})
    }
}

const CASES: [&str; 9] = [
    "slow-origin",
    "timeout-before-cutoff",
    "fast-origin",
    "begin-progress",
    "batch-progress",
    "pending-head-cutoff",
    "pending-body-cutoff",
    "cpu-stretch",
    "caps-while-waiting",
];

pub(super) fn run(name: &str, run: &Path, records: &mut Records) -> Result<()> {
    let log = run.join(format!("{name}-outbound-lifetimes.log"));
    let mut cursor = protocol::LogCursor::default();
    let outcome = wire::progress_suite(
        name,
        "outbound-lifetimes",
        run,
        records,
        |port, origins, records| checks(name, port, origins, &log, &mut cursor, records),
    );
    // The shared runner has now stopped and joined the runtime, including on case failure.
    let tail = if name == "fastly" && log.exists() {
        let result = cleanup::finish(&log, &cursor);
        records.push(json!({"event":"cleanup-log-tail","adapter":name,"status":if result.is_ok(){"pass"}else{"fail"},
            "error":result.as_ref().err().map(ToString::to_string),"qualification":false}))?;
        result
    } else {
        Ok(())
    };
    let callbacks = if name == "fastly" {
        callback::run(run, records)
    } else {
        Ok(0)
    };
    let count = combined_count(outcome, tail, callbacks)?;
    if let Some(count) = count {
        records.push(json!({"event":"lifetime-inventory","adapter":name,
            "status":"pass","cases":count,"qualification":false}))?;
    }
    Ok(())
}

fn combined_count(
    outcome: Result<Option<usize>>,
    tail: Result<()>,
    callbacks: Result<usize>,
) -> Result<Option<usize>> {
    let mut failures = Vec::new();
    let count = match outcome {
        Ok(count) => count,
        Err(error) => {
            failures.push(format!("lifetime cases: {error}"));
            None
        }
    };
    if let Err(error) = tail {
        failures.push(format!("cleanup tail: {error}"));
    }
    let additional = match callbacks {
        Ok(count) => count,
        Err(error) => {
            failures.push(format!("callback wrappers: {error}"));
            0
        }
    };
    require(failures.is_empty(), failures.join("; "))?;
    Ok(count.map(|count| count + additional))
}

fn checks(
    adapter: &str,
    port: u16,
    origins: &WireOrigins,
    log: &Path,
    cursor: &mut protocol::LogCursor,
    records: &mut Records,
) -> Result<usize> {
    let mut failures = Vec::new();
    let handoff = matches!(adapter, "axum" | "cloudflare" | "spin");
    if !handoff {
        records.push(
            json!({"event":"lifetime-case-inapplicable","case":"response-handoff","adapter":adapter,
            "reason":"EZ-6 criterion 20 targets Axum, workerd and Spin","qualification":false}),
        )?;
    }
    let mut executed = 0;
    for case in CASES
        .into_iter()
        .chain((adapter == "fastly").then_some("body-read-arrivals"))
        .chain((adapter == "fastly").then_some("trickling-slot"))
        .chain((adapter == "fastly").then_some("send-during-batch"))
        .chain(handoff.then_some("response-handoff"))
    {
        executed += 1;
        let token = token();
        origins.register_lifetime(&token, case)?;
        let backend_origin = format!("http://127.0.0.1:{}", origins.port(0));
        let backend = format!("{backend_origin}/lifetimes/{token}/{case}");
        let source_anchor = origins.mark_lifetime_start(&token)?;
        let prefix_bytes = if case == "response-handoff" {
            serde_json::to_vec(&handoff_prelude(
                &token,
                if adapter == "axum" { None } else { Some(true) },
            ))?
            .len()
                + 1
        } else {
            0
        };
        let notify = |status, headers: &[Value], bytes: &[u8]| {
            observe_handoff_prefix(origins, adapter, &token, status, headers, bytes)
        };
        let reply = net::header_request(
            port,
            &format!("/progress/lifetimes/{case}"),
            &token,
            &[("x-fixture-backend", &backend)],
            (case == "response-handoff").then_some(net::PrefixObserver {
                bytes: prefix_bytes,
                notify: &notify,
            }),
        );
        let response_returned = Instant::now();
        let released = if pending_cutoff(case) {
            origins.release_lifetime(&token, response_returned)
        } else {
            Ok(())
        };
        let source = origins.take_lifetime_events(&token);
        let source_error = source.as_ref().err().map(ToString::to_string);
        let mut origin = source.unwrap_or(Value::Null);
        if origin.is_array() && matches!(case, "caps-while-waiting" | "response-handoff") {
            let returned = response_returned
                .checked_duration_since(source_anchor)
                .ok_or("invalid response-return clock")?;
            origin
                .as_array_mut()
                .ok_or("missing source events")?
                .push(json!({
                "op":"response-returned","role":"control","token":token,"case":case,
                "at_us":u64::try_from(returned.as_micros())?,"observer":"harness"}));
        }
        let mut report = Value::Null;
        let result = (|| -> Result<()> {
            let reply = reply.as_ref().map_err(|error| error.to_string())?;
            released.as_ref().map_err(|error| error.to_string())?;
            require(
                source_error.is_none(),
                source_error.clone().unwrap_or_default(),
            )?;
            require(
                reply["status"] == 200,
                "lifetime guest returned an error response",
            )?;
            let bytes: Vec<u8> = serde_json::from_value(reply["body_bytes"].clone())?;
            report = decode_report(adapter, case, &token, &bytes)?;
            validate(
                AdapterBoundary {
                    name: adapter,
                    origin: &backend_origin,
                },
                case,
                &token,
                &report,
                &origin,
            )?;
            if case == "response-handoff" {
                validate_handoff_dispatch(adapter, &report)?;
            }
            Ok(())
        })();
        let clock_correlation = if case == "cpu-stretch" {
            report["events"]
                .as_array()
                .and_then(|events| cpu_clock_bounds(events, &origin).ok())
                .map(|[lower, upper]| {
                    json!({"basis":"causal-calibration-and-head-bounds",
                    "lower_us":lower,"upper_us":upper,"rounding_guard_us":1000,
                    "same_rate_assumed":true,"clock_sync_proven":false})
                })
        } else if case == "caps-while-waiting" {
            caps_clock_bounds(&report, &origin)
                .ok()
                .map(|bounds| bounds.evidence())
        } else {
            None
        };
        let timeout_contract = (case == "timeout-before-cutoff").then(|| {
            let result = validate_timeout_before_cutoff(&report, &origin);
            json!({"status":if result.is_ok(){"pass"}else{"fail"},
                "error":result.err().map(|error|error.to_string())})
        });
        records.push(json!({"event":"lifetime-case","case":case,"token":token,
            "status":if result.is_ok(){"pass"}else{"fail"},
            "error":result.as_ref().err().map(ToString::to_string),"report":report,
            "origin":origin,"source_error":source_error,"client_error":reply.as_ref().err().map(ToString::to_string),
            "partial_client":reply.as_ref().err().and_then(|error|error.downcast_ref::<net::InterruptedResponse>())
                .map(|partial|json!({"status":partial.status,"body_bytes":partial.body,"reason":partial.reason})),
            "clock_correlation":clock_correlation,"timeout_contract":timeout_contract,
            "timing_diagnostics":timing_diagnostics(case,&report,&origin),"body_phase_evidence":if case == "pending-body-cutoff" && result.is_ok() {
                Some(if adapter == "fastly" { "sdk-readiness-wait" } else { "sdk-read-pending" })
            } else { None },"client":reply.as_ref().ok().map(|reply| json!({
                "status":reply["status"],"complete_ns":reply["complete_ns"],
                "header_ns":reply["header_ns"],"headers":reply["headers"]})),"qualification":false}))?;
        if let Err(error) = result {
            failures.push(format!("{case}: {error}"));
        }
    }
    if adapter == "fastly" {
        for mode in cleanup::CASES {
            executed += 1;
            if let Err(error) = cleanup::check(mode, port, origins, log, cursor, records) {
                failures.push(format!("{}: {error}", mode.name()));
            }
        }
    }
    require(failures.is_empty(), failures.join("; "))?;
    Ok(executed)
}

fn handoff_prelude(token: &str, dispatch_returned: Option<bool>) -> Value {
    json!({"schema":1,"case":"response-handoff","token":token,"phase":"pending-handoff",
        "origin_pending":true,"batch_pending":true,"slots":3,"cutoff_at_us":534000,"qualification":false,
        "callback_scope_closed":true,"body_scope_live":true,"dispatch_returned":dispatch_returned})
}

fn validate_handoff_prefix(
    adapter: &str,
    token: &str,
    status: u16,
    headers: &[Value],
    bytes: &[u8],
) -> Result<()> {
    let types: Vec<_> = headers
        .iter()
        .filter(|header| {
            header[0]
                .as_str()
                .is_some_and(|name| name.eq_ignore_ascii_case("content-type"))
        })
        .collect();
    require(
        status == 200 && types.len() == 1 && types[0][1] == "application/x-ndjson",
        "handoff prefix has an invalid response head",
    )?;
    validate_handoff_prelude(adapter, token, bytes)
}

fn observe_handoff_prefix(
    origins: &WireOrigins,
    adapter: &str,
    token: &str,
    status: u16,
    headers: &[Value],
    bytes: &[u8],
) -> Result<()> {
    let observed = Instant::now();
    validate_handoff_prefix(adapter, token, status, headers, bytes)?;
    origins.release_lifetime(token, observed)
}

fn validate_handoff_prelude(adapter: &str, token: &str, bytes: &[u8]) -> Result<()> {
    let returned = match adapter {
        "axum" => None,
        "cloudflare" | "spin" => Some(true),
        _ => return Err("handoff case is inapplicable to this adapter".into()),
    };
    let mut expected = serde_json::to_vec(&handoff_prelude(token, returned))?;
    expected.push(b'\n');
    require(
        bytes.len() <= 512 && bytes == expected,
        "invalid canonical handoff prefix or dispatch witness",
    )
}

fn decode_report(adapter: &str, case: &str, token: &str, bytes: &[u8]) -> Result<Value> {
    if case != "response-handoff" {
        return Ok(serde_json::from_slice(bytes)?);
    }
    let mut lines = bytes.split(|byte| *byte == b'\n');
    let prefix = lines.next().ok_or("missing handoff prefix")?;
    let report = lines.next().ok_or("missing handoff report")?;
    require(
        prefix.len() < 512
            && report.len() < 16 * 1024
            && lines.next() == Some([].as_slice())
            && lines.next().is_none(),
        "invalid handoff report framing or bound",
    )?;
    validate_handoff_prelude(adapter, token, &bytes[..prefix.len() + 1])?;
    Ok(serde_json::from_slice(report)?)
}

fn validate_handoff_dispatch(adapter: &str, report: &Value) -> Result<()> {
    let expected = match adapter {
        "axum" => Value::Null,
        "cloudflare" | "spin" => json!(true),
        _ => return Err("handoff case is inapplicable to this adapter".into()),
    };
    require(
        report["handoff"].get("dispatch_returned") == Some(&expected),
        "handoff lacks the adapter dispatch-return witness",
    )
}

fn pending_cutoff(case: &str) -> bool {
    matches!(case, "pending-head-cutoff" | "pending-body-cutoff")
}

fn validate(
    boundary: AdapterBoundary<'_>,
    case: &str,
    token: &str,
    report: &Value,
    origin: &Value,
) -> Result<()> {
    require(
        report["schema"] == 1
            && report["case"] == case
            && report["token"] == token
            && report["qualification"] == false
            && report.get("error") == Some(&Value::Null),
        "lifetime report identity or error mismatch",
    )?;
    let events = report["events"]
        .as_array()
        .ok_or("missing guest lifetime events")?;
    for entry in origin.as_array().ok_or("missing host lifetime events")? {
        require(
            entry["token"] == token && entry["case"] == case,
            "foreign host lifetime event",
        )?;
    }
    for check in events.iter().filter(|entry| entry["op"] == "check") {
        require(
            number(check, "duration_us")? < 5_000,
            "nonblocking batch check stalled",
        )?;
    }
    for pair in events.windows(2) {
        require(
            number(&pair[0], "at_us")? <= number(&pair[1], "at_us")?,
            "guest lifetime events are reordered",
        )?;
    }
    let receipts: Vec<_> = origin
        .as_array()
        .ok_or("missing host lifetime events")?
        .iter()
        .filter(|event| event["op"] == "request")
        .collect();
    let mut roles: Vec<_> = receipts
        .iter()
        .map(|entry| entry["role"].as_str().ok_or("missing role"))
        .collect::<std::result::Result<_, _>>()?;
    roles.sort_unstable();
    let expected: &[&str] = match case {
        "slow-origin" | "body-read-arrivals" | "trickling-slot" | "send-during-batch" => {
            &["a", "b", "c", "origin"]
        }
        "timeout-before-cutoff" => &["a", "b", "c", "d", "origin"],
        "cpu-stretch" => &["a", "b", "c", "calibration", "origin"],
        "fast-origin" => &["a", "b", "c", "mediator", "origin"],
        "begin-progress" => &["origin"],
        "batch-progress" => &["a", "b", "c", "quiet"],
        "pending-head-cutoff" | "pending-body-cutoff" => &["a", "b", "c"],
        "caps-while-waiting" => &["a", "b", "c"],
        "response-handoff" => &["a", "b", "c", "origin"],
        _ => return Err("unknown lifetime case".into()),
    };
    require(
        roles == expected,
        "missing, duplicate or foreign origin receipt",
    )?;
    if matches!(case, "trickling-slot" | "send-during-batch") {
        require(boundary.name == "fastly", "live-batch case requires Fastly")?;
        return if case == "trickling-slot" {
            validate_trickling_slot(report, events, origin)
        } else {
            validate_send_during_batch(report, events, origin)
        };
    }
    if case == "body-read-arrivals" {
        require(
            boundary.name == "fastly",
            "body-read arrival case requires Fastly",
        )?;
        return validate_body_read_arrivals(report, events, origin);
    }
    if case == "response-handoff" {
        return validate_handoff(report, events, origin);
    }
    if pending_cutoff(case) {
        validate_native_pending(boundary, case, token, report, events)?;
        return validate_pending_cutoff(case, report, events, origin);
    }
    if case == "caps-while-waiting" {
        return validate_caps(report, events, origin);
    }
    if case == "cpu-stretch" {
        return validate_cpu_stretch(report, events, origin);
    }
    if case == "begin-progress" || case == "batch-progress" {
        let start = number(event(events, "quiet-start")?, "at_us")?;
        let end = number(event(events, "quiet-end")?, "at_us")?;
        let budget = if case == "begin-progress" {
            100_000
        } else {
            300_000
        };
        require(
            end.saturating_sub(start) >= budget,
            "unobserved quiet window was too short",
        )?;
        let lower = clock_offset_lower(case, events, origin)?;
        for receipt in &receipts {
            require(
                i128::from(number(receipt, "at_us")?) <= lower + i128::from(end),
                "request did not reach origin before unrelated operation completed",
            )?;
        }
        for observation in events
            .iter()
            .filter(|event| matches!(event["op"].as_str(), Some("check" | "next" | "collect")))
        {
            require(
                number(observation, "at_us")? >= end,
                "batch was observed during quiet window",
            )?;
        }
        if case == "begin-progress" {
            let begin = event(events, "begin")?;
            require(
                number(begin, "at_us")? + number(begin, "duration_us")? <= start,
                "begin was not awaited before quiet window",
            )?;
            require(
                report["origin"] == json!({"status":200,"bytes":[79]}),
                "begun response changed",
            )?;
            require(
                report["items"] == json!([])
                    && report["slots"] == json!([])
                    && report["termination"].is_null(),
                "begin unexpectedly observed a batch",
            )?;
            return Ok(());
        }
        let batch = event(events, "batch-start")?;
        require(
            number(batch, "at_us")? + number(batch, "duration_us")? <= start,
            "batch was not started before quiet window",
        )?;
        validate_items(
            report,
            &[],
            &["collect", "collect", "collect"],
            &["Resolved", "Resolved", "Resolved"],
            "Completed",
        )?;
        let batch_start = number(batch, "at_us")?;
        for (index, item) in report["items"].as_array().unwrap().iter().enumerate() {
            let role = ["a", "b", "c"][index];
            let chunk = origin
                .as_array()
                .unwrap()
                .iter()
                .find(|entry| entry["op"] == "chunk" && entry["role"] == role)
                .ok_or("missing bidder write receipt")?;
            let upper =
                i128::from(number(chunk, "at_us")?) - lower - i128::from(batch_start) + 5_000;
            require(
                i128::from(number(item, "elapsed_us")?) <= upper
                    && number(item, "elapsed_us")? < end - start,
                "batch elapsed was stamped at collection instead of arrival",
            )?;
        }
        return Ok(());
    }
    within(
        number(event(events, "begin")?, "at_us")?,
        2_000,
        "origin begin missed 2 ms",
    )?;
    within(
        number(event(events, "batch-start")?, "at_us")?,
        34_000,
        "batch start missed 34 ms",
    )?;
    validate_timeline(case, events, origin)?;
    if matches!(case, "slow-origin" | "timeout-before-cutoff") {
        let check = events
            .iter()
            .find(|entry| entry["op"] == "check")
            .ok_or("missing first batch check")?;
        within(
            number(check, "at_us")?,
            701_000,
            "first batch check missed 701 ms",
        )?;
        require(
            report["origin"] == json!({"status":200,"bytes":[79,111,33]}),
            "slow origin body changed",
        )?;
        if case == "timeout-before-cutoff" {
            return validate_timeout_before_cutoff(report, origin);
        }
        validate_items(
            report,
            &[146_000, 286_000],
            &["check", "check"],
            &["AlreadyDelivered", "AlreadyDelivered", "Unresolved"],
            "Cutoff",
        )
    } else {
        within(
            number(event(events, "check")?, "at_us")?,
            260_000,
            "batch check missed origin EOF",
        )?;
        for (op, expected) in [("next", 320_000), ("collect", 400_000)] {
            let observed = event(events, op)?;
            within(
                number(observed, "at_us")? + number(observed, "duration_us")?,
                expected,
                "batch observation missed scenario completion",
            )?;
        }
        require(
            report["origin"] == json!({"status":200,"bytes":[79]}),
            "fast origin body changed",
        )?;
        validate_items(
            report,
            &[146_000, 286_000, 366_000],
            &["check", "next", "collect"],
            &["AlreadyDelivered", "AlreadyDelivered", "Resolved"],
            "Completed",
        )?;
        let mediator = &report["mediator"];
        within(
            number(mediator, "sent_at_us")?,
            400_000,
            "mediator did not start at 400 ms",
        )?;
        within(
            number(mediator, "remaining_us")?,
            134_000,
            "mediator did not retain original cutoff",
        )?;
        require(
            mediator["error"]
                == json!({"kind":"gateway_timeout","status":504,"reason":"RequestDeadline"}),
            "mediator lost typed request-deadline cause",
        )?;
        let send = event(events, "mediator-send")?;
        within(
            number(send, "at_us")? + number(send, "duration_us")?,
            534_000,
            "mediator deadline was extended",
        )
    }
}

fn validate_handoff(report: &Value, events: &[Value], origin: &Value) -> Result<()> {
    require(
        report["termination"] == "Completed"
            && report["slots"] == json!(["Resolved", "Resolved", "Resolved"])
            && report["origin"] == json!({"status":200,"bytes":[79]})
            && report["mediator"].is_null()
            && report["handoff"]["prelude"]
                == handoff_prelude(
                    report["token"].as_str().ok_or("missing handoff token")?,
                    report["handoff"]["dispatch_returned"].as_bool(),
                )
            && report["handoff"]["callback_scope_closed"] == true
            && report["handoff"]["body_scope_live"] == true
            && report["handoff"]["origin_streamed"] == true
            && report["handoff"]
                .get("dispatch_returned")
                .is_some_and(|value| matches!(value, Value::Null | Value::Bool(true))),
        "handoff lost pending response ownership or successful results",
    )?;
    let ops: Vec<_> = events.iter().map(|entry| entry["op"].as_str()).collect();
    require(
        ops == [
            "begin",
            "batch-start",
            "handoff-pending",
            "origin-head",
            "origin-body",
            "collect",
        ]
        .map(Some),
        "handoff did not collect inside the response body",
    )?;
    let begin = event(events, "begin")?;
    let batch = event(events, "batch-start")?;
    let pending = event(events, "handoff-pending")?;
    let head = event(events, "origin-head")?;
    let body = event(events, "origin-body")?;
    let collected = event(events, "collect")?;
    within(number(begin, "at_us")?, 2000, "handoff begin missed 2 ms")?;
    within(number(batch, "at_us")?, 34000, "handoff batch missed 34 ms")?;
    for pair in events.windows(2) {
        require(
            number(&pair[1], "at_us")?
                >= number(&pair[0], "at_us")? + number(&pair[0], "duration_us")?,
            "handoff operations overlap or were backdated",
        )?;
    }
    require(
        begin["result"] == "scheduled"
            && batch["result"]["cutoff_at_us"] == 534000
            && pending["result"] == json!({"origin":"Pending","batch":"Pending"})
            && number(pending, "duration_us")? < 5000
            && head["result"] == "ok"
            && body["result"].is_null()
            && collected["result"] == "ok",
        "handoff pending check or original budget changed",
    )?;
    let returned_at = number(collected, "at_us")? + number(collected, "duration_us")?;
    let items = report["items"].as_array().ok_or("missing handoff slots")?;
    require(items.len() == 3, "missing handoff outcome")?;
    let mut earliest_epoch = number(batch, "at_us")?;
    let mut latest_epoch = number(batch, "at_us")? + number(batch, "duration_us")?;
    for (index, item) in items.iter().enumerate() {
        require(
            item["index"] == index
                && item["via"] == "collect"
                && item["slot_error"] == false
                && item["outcome"] == json!({"status":200,"bytes":[65+index]})
                && number(item, "elapsed_us")? < 500000,
            "handoff replayed, lost or failed a slot",
        )?;
        let elapsed = number(item, "elapsed_us")?;
        earliest_epoch = earliest_epoch.max(
            (number(pending, "at_us")? + number(pending, "duration_us")?).saturating_sub(elapsed),
        );
        latest_epoch = latest_epoch.min(
            returned_at
                .min(533999)
                .checked_sub(elapsed)
                .ok_or("handoff arrival exceeds collection")?,
        );
    }
    require(
        earliest_epoch <= latest_epoch,
        "handoff arrivals cannot share an on-time epoch after the pending check",
    )?;
    let origin = origin.as_array().ok_or("missing handoff source")?;
    require(
        origin.len() == 23
            && origin.iter().all(|entry| {
                entry.get("error").is_none()
                    && matches!(
                        entry["role"].as_str(),
                        Some("origin" | "a" | "b" | "c" | "control")
                    )
                    && matches!(
                        entry["op"].as_str(),
                        Some(
                            "request"
                                | "waiting"
                                | "head"
                                | "chunk"
                                | "closed"
                                | "client-prefix-observed"
                                | "release"
                                | "response-returned"
                        )
                    )
            }),
        "unexpected handoff source evidence",
    )?;
    let prefix = role_event(origin, "control", "client-prefix-observed")?;
    let release = role_event(origin, "control", "release")?;
    let returned = role_event(origin, "control", "response-returned")?;
    let position = |role: &str, op: &str| {
        origin
            .iter()
            .position(|entry| entry["role"] == role && entry["op"] == op)
            .ok_or("missing handoff phase")
    };
    let release_index = position("control", "release")?;
    let ended = number(returned, "at_us")?;
    require(
        origin
            .iter()
            .filter(|entry| entry["role"] == "control")
            .count()
            == 3
            && position("control", "client-prefix-observed")? < release_index
            && release_index < position("control", "response-returned")?
            && release["prefix_observed_us"] == prefix["at_us"]
            && release.get("response_returned_us").is_none()
            && number(prefix, "at_us")? <= number(release, "at_us")?
            && number(release, "at_us")? <= ended
            && returned["observer"] == "harness",
        "handoff gate lacks a causal client-prefix witness",
    )?;
    for (index, role) in ["origin", "a", "b", "c"].into_iter().enumerate() {
        let request = role_event(origin, role, "request")?;
        let waiting = role_event(origin, role, "waiting")?;
        let head = role_event(origin, role, "head")?;
        let chunk = role_event(origin, role, "chunk")?;
        let closed = role_event(origin, role, "closed")?;
        require(
            position(role, "request")? < position(role, "waiting")?
                && position(role, "waiting")? < position(role, "head")?
                && release_index < position(role, "head")?
                && position(role, "head")? < position(role, "chunk")?
                && position(role, "chunk")? < position(role, "closed")?
                && number(request, "at_us")? <= number(waiting, "at_us")?
                && number(waiting, "at_us")? <= number(head, "write_started_us")?
                && number(head, "write_started_us")? >= number(release, "at_us")?
                && number(head, "write_started_us")? <= number(head, "at_us")?
                && number(chunk, "write_started_us")? >= number(head, "at_us")?
                && number(chunk, "write_started_us")? <= number(chunk, "at_us")?
                && number(chunk, "write_started_us")? <= ended
                && number(chunk, "at_us")? <= number(closed, "at_us")?
                && chunk["bytes"] == json!([if index == 0 { 79 } else { 64 + index }])
                && closed["reason"] == "complete",
            "handoff source answered before release or lost its exact payload",
        )?;
    }
    Ok(())
}

fn caps_clock_bounds(report: &Value, origin: &Value) -> Result<CapsClockBounds> {
    let events = report["events"].as_array().ok_or("missing caps events")?;
    let origin = origin.as_array().ok_or("missing caps source events")?;
    let items = report["items"].as_array().ok_or("missing caps items")?;
    require(
        items.len() == 3,
        "caps timing requires three original slots",
    )?;
    let batch = event(events, "batch-start")?;
    let collected = event(events, "collect")?;
    let entered = i128::from(number(batch, "at_us")?);
    let returned = entered + i128::from(number(batch, "duration_us")?);
    let collection =
        i128::from(number(collected, "at_us")?) + i128::from(number(collected, "duration_us")?);
    require(
        i128::from(number(collected, "at_us")?) >= returned,
        "caps collection precedes the original batch invocation",
    )?;
    let native_return = i128::from(number(
        role_event(origin, "control", "response-returned")?,
        "at_us",
    )?);
    let mut epoch = [(entered - 1).max(0), returned + 1];
    let mut offset_upper = i128::MAX;
    let mut elapsed = [0_i128; 3];
    let mut payload = [0_i128; 3];
    for (index, role) in ["a", "b", "c"].into_iter().enumerate() {
        require(
            items[index]["index"] == index,
            "caps timing lost an original slot",
        )?;
        let request = role_event(origin, role, "request")?;
        let stamp = i128::from(number(request, "guest_at_us")?);
        require(stamp <= entered, "caps request stamp follows batch entry")?;
        // Sending precedes receipt: transit delay can only loosen this upper bound.
        offset_upper = offset_upper.min(i128::from(number(request, "at_us")?) - stamp);
        elapsed[index] = i128::from(number(&items[index], "elapsed_us")?);
        epoch[1] = epoch[1].min(collection + 1 - elapsed[index]);
        let piece = if index == 0 {
            origin
                .iter()
                .find(|entry| {
                    entry["role"] == "a"
                        && entry["op"] == "chunk"
                        && entry["decoded_prefix_bytes"]
                            .as_u64()
                            .is_some_and(|prefix| prefix > 65536)
                        && entry["written_bytes"]
                            .as_u64()
                            .is_some_and(|written| written > 0)
                })
                .ok_or("caps timing lacks a confirmed over-cap prefix")?
        } else {
            role_event(origin, role, "chunk")?
        };
        payload[index] = i128::from(number(piece, "write_started_us")?);
    }
    require(
        epoch[0] <= epoch[1],
        "caps arrivals have no common invocation epoch",
    )?;
    offset_upper = (offset_upper + 1000).min(native_return - collection + 1000);
    // All slots share one host epoch. Payload writes must precede terminal observations;
    // every observation must precede collection and the diagnostic response.
    let host_lower = (0..3)
        .map(|index| payload[index] - elapsed[index] - 1000)
        .max()
        .unwrap();
    let host_upper = (epoch[1] + offset_upper)
        .min(native_return + 1000 - elapsed.iter().copied().max().unwrap());
    require(
        host_lower <= host_upper,
        "caps payloads and arrivals have contradictory shared epochs",
    )?;
    let start = i128::from(number(role_event(origin, "a", "head")?, "body_start_us")?);
    Ok(CapsClockBounds {
        epoch,
        host_epoch: [host_lower, host_upper],
        offset_upper,
        terminal: [host_lower + elapsed[0], host_upper + elapsed[0]],
        collection_upper: collection + 1 + offset_upper,
        window_end: start + 300000,
        native_return,
    })
}

fn validate_caps(report: &Value, events: &[Value], origin: &Value) -> Result<()> {
    let timing = caps_clock_bounds(report, origin)?;
    require(
        report["slots"] == json!(["Resolved", "Resolved", "Resolved"])
            && report["termination"] == "Completed"
            && report["origin"].is_null()
            && report["mediator"].is_null()
            && events.len() == 2,
        "caps collection lost its completed slots",
    )?;
    let batch = event(events, "batch-start")?;
    let collected = event(events, "collect")?;
    within(number(batch, "at_us")?, 34000, "caps batch missed 34 ms")?;
    require(
        batch["result"]["cutoff_at_us"] == 534000
            && collected["result"] == "ok"
            && number(collected, "at_us")?
                >= number(batch, "at_us")? + number(batch, "duration_us")?,
        "caps batch changed its original cutoff or collection",
    )?;
    let items = report["items"].as_array().ok_or("missing caps items")?;
    require(items.len() == 3, "missing or extra caps items")?;
    let collected_at = number(collected, "at_us")? + number(collected, "duration_us")?;
    let mut latest_epoch = number(batch, "at_us")? + number(batch, "duration_us")?;
    for (index, item) in items.iter().enumerate() {
        let expected = if index == 0 {
            json!({"error":{"kind":"response_too_large","status":502,"reason":"BufferedBody"}})
        } else {
            json!({"status":200,"bytes":[b'A'+index as u8]})
        };
        require(
            item["index"] == index
                && item["via"] == "collect"
                && item["slot_error"] == (index == 0)
                && item["outcome"] == expected
                && (1..500000).contains(&number(item, "elapsed_us")?),
            "caps error was not an original buffered-limit slot failure",
        )?;
        latest_epoch = latest_epoch.min(
            collected_at
                .checked_sub(number(item, "elapsed_us")?)
                .ok_or("caps arrival elapsed exceeds collection time")?,
        );
    }
    require(
        number(batch, "at_us")? <= latest_epoch,
        "caps arrival occurred after collection",
    )?;
    let origin = origin.as_array().ok_or("missing caps source events")?;
    require(
        origin.iter().all(|entry| {
            matches!(
                entry["op"].as_str(),
                Some("request" | "head" | "chunk" | "closed" | "response-returned")
            ) && entry.get("error").is_none()
                && matches!(entry["role"].as_str(), Some("a" | "b" | "c" | "control"))
        }),
        "unexpected caps source event or failure",
    )?;
    let returned = role_event(origin, "control", "response-returned")?;
    require(
        returned["observer"] == "harness"
            && origin
                .iter()
                .filter(|entry| entry["role"] == "control")
                .count()
                == 1,
        "invalid response-return observer",
    )?;
    let returned = number(returned, "at_us")?;
    let request = role_event(origin, "a", "request")?;
    let head = role_event(origin, "a", "head")?;
    let closed = role_event(origin, "a", "closed")?;
    let start = number(head, "body_start_us")?;
    let encoded = number(head, "content_length")?;
    require(
        head["content_encoding"] == "gzip"
            && head["decoded_bytes"] == 131072
            && (131072..=129 * 1024).contains(&encoded)
            && number(request, "at_us")? <= number(head, "write_started_us")?
            && number(head, "write_started_us")? <= number(head, "at_us")?
            && number(head, "at_us")? == start,
        "invalid caps response head or body-start evidence",
    )?;
    require(
        timing.terminal[1] < timing.window_end,
        "caps slot terminal upper bound reaches the source's full trickle window",
    )?;
    let ends = head["piece_ends"]
        .as_array()
        .ok_or("missing caps piece boundaries")?;
    require(
        ends.len() == 8 && ends.last().and_then(Value::as_u64) == Some(encoded),
        "invalid caps encoding boundaries",
    )?;
    let mut previous_end = 0;
    for end in ends {
        let end = end.as_u64().ok_or("invalid caps piece boundary")?;
        require(
            end >= previous_end + 16384 && end <= encoded,
            "caps piece cannot contain its decoded payload",
        )?;
        previous_end = end;
    }
    let chunks: Vec<_> = origin
        .iter()
        .filter(|entry| entry["role"] == "a" && entry["op"] == "chunk")
        .collect();
    require(
        (5..=8).contains(&chunks.len()),
        "missing caps crossing prefix",
    )?;
    let mut written_total = 0;
    let mut decoded = 0;
    let mut prefix_before_return = false;
    let mut peer_closed = false;
    let mut previous_at = start;
    for (index, chunk) in chunks.iter().enumerate() {
        let scheduled = start + index as u64 * 300000 / 7;
        let end = ends[index].as_u64().ok_or("invalid caps end")?;
        let piece_start = if index == 0 {
            0
        } else {
            ends[index - 1].as_u64().ok_or("invalid caps start")?
        };
        let attempted = end - piece_start;
        let written = number(chunk, "written_bytes")?;
        let prefix = number(chunk, "decoded_prefix_bytes")?;
        let io = chunk.get("io_error").ok_or("missing caps I/O outcome")?;
        require(
            !peer_closed
                && chunk["index"] == index
                && chunk["schedule_basis"] == "body-start"
                && number(chunk, "scheduled_us")? == scheduled
                && number(chunk, "write_started_us")? >= scheduled.max(previous_at)
                && number(chunk, "write_started_us")? <= number(chunk, "at_us")?
                && number(chunk, "at_us")? <= scheduled + 5000
                && number(chunk, "attempted_bytes")? == attempted
                && written <= attempted
                && number(chunk, "encoded_end")? == written_total + written
                && prefix >= decoded
                && prefix <= 65537
                && prefix <= (index as u64 * 16384 + written).min(65537),
            "inconsistent caps write progress",
        )?;
        if io.is_null() {
            require(
                written == attempted && prefix == ((index as u64 + 1) * 16384).min(65537),
                "incomplete successful caps piece",
            )?;
        } else {
            require(
                index + 1 == chunks.len()
                    && matches!(
                        io.as_str(),
                        Some("BrokenPipe" | "ConnectionReset" | "ConnectionAborted")
                    )
                    && prefix > 65536,
                "unrelated or premature caps source failure",
            )?;
            peer_closed = true;
        }
        written_total += written;
        decoded = prefix;
        previous_at = number(chunk, "at_us")?;
        prefix_before_return |= prefix > 65536 && previous_at <= returned;
    }
    require(
        prefix_before_return && number(closed, "at_us")? >= previous_at,
        "caps failure preceded no confirmed over-cap prefix",
    )?;
    if peer_closed {
        require(
            closed["reason"] == "peer_closed_after_cap_prefix",
            "caps peer-close classification changed",
        )?;
    } else {
        require(
            chunks.len() == 8
                && written_total == encoded
                && closed["reason"] == "complete"
                && number(closed, "at_us")? >= start + 300000,
            "caps source completed early",
        )?;
    }
    for (index, role) in [(1, "b"), (2, "c")] {
        let request = role_event(origin, role, "request")?;
        let head = role_event(origin, role, "head")?;
        let chunk = role_event(origin, role, "chunk")?;
        let closed = role_event(origin, role, "closed")?;
        let scheduled = number(request, "at_us")? + index * 20000;
        require(
            number(head, "scheduled_us")? == scheduled
                && number(head, "write_started_us")? >= scheduled
                && number(head, "at_us")? >= number(head, "write_started_us")?
                && number(head, "at_us")? <= scheduled + 5000
                && number(chunk, "scheduled_us")? == scheduled
                && number(chunk, "write_started_us")? >= number(head, "at_us")?
                && number(chunk, "at_us")? >= number(chunk, "write_started_us")?
                && number(chunk, "at_us")? <= scheduled + 5000
                && chunk["bytes"] == json!([65 + index])
                && number(chunk, "at_us")? <= returned
                && closed["reason"] == "complete"
                && number(closed, "at_us")? >= number(chunk, "at_us")?,
            "caps healthy sibling changed",
        )?;
    }
    require(
        origin.len() == 12 + chunks.len(),
        "missing or extra caps source evidence",
    )
}

fn validate_timeout_before_cutoff(report: &Value, origin: &Value) -> Result<()> {
    require(
        report["slots"]
            == json!([
                "AlreadyDelivered",
                "AlreadyDelivered",
                "Unresolved",
                "AlreadyDelivered"
            ])
            && report["termination"] == "Cutoff"
            && report["mediator"].is_null(),
        "per-call timeout lost siblings or batch cutoff",
    )?;
    let items = report["items"].as_array().ok_or("missing lifetime items")?;
    require(items.len() == 3, "missing or extra timeout-case items")?;
    let events = report["events"]
        .as_array()
        .ok_or("missing timeout-case events")?;
    let checks: Vec<_> = events
        .iter()
        .filter(|entry| entry["op"] == "check")
        .collect();
    require(
        checks.len() == 4
            && checks.iter().take(3).all(|check| check["result"] == "Item")
            && checks[3]["result"] == "Finished",
        "timeout-case cutoff was not observed after all three items",
    )?;
    for (item, index, elapsed, outcome) in [
        (&items[0], 0, 146000, json!({"status":200,"bytes":[65]})),
        (&items[1], 1, 286000, json!({"status":200,"bytes":[66]})),
        (
            &items[2],
            3,
            300000,
            json!({"error":{"kind":"gateway_timeout","status":504,"reason":"PerCallTimeout"}}),
        ),
    ] {
        require(
            item["index"] == index && item["via"] == "check" && item["outcome"] == outcome,
            "wrong timeout-case item identity, observation or outcome",
        )?;
        within(
            number(item, "elapsed_us")?,
            elapsed,
            "timeout-case arrival elapsed changed",
        )?;
    }
    let origin = origin.as_array().unwrap();
    let phases: Vec<_> = origin
        .iter()
        .filter(|entry| entry["role"] == "d")
        .map(|entry| entry["op"].as_str().unwrap_or(""))
        .collect();
    let request = role_event(origin, "d", "request")?;
    let closed = role_event(origin, "d", "closed")?;
    let aligned = timeline_epoch(origin, "timeout-before-cutoff")?;
    let scheduled = number(closed, "scheduled_us")?;
    require(
        phases == ["request", "closed"]
            && closed["reason"] == "silent"
            && closed["scenario_scheduled_us"] == 1500000
            && closed["schedule_basis"] == "initial-receipts-minimum-upper-bound"
            && number(request, "at_us")? <= scheduled
            && i128::from(scheduled) - aligned == 1500000
            && number(closed, "at_us")? >= scheduled,
        "fourth bidder was not silent past the cutoff",
    )
}

fn role_event<'a>(origin: &'a [Value], role: &str, op: &str) -> Result<&'a Value> {
    let mut entries = origin
        .iter()
        .filter(|entry| entry["role"] == role && entry["op"] == op);
    let entry = entries.next().ok_or("missing origin phase")?;
    require(entries.next().is_none(), "duplicate origin phase")?;
    Ok(entry)
}

fn timeline_epoch(origin: &[Value], case: &str) -> Result<i128> {
    let roles: &[&str] = if case == "timeout-before-cutoff" {
        &["origin", "a", "b", "c", "d"]
    } else {
        &["origin", "a", "b", "c"]
    };
    let seal = role_event(origin, "control", "alignment")?;
    let sealed = number(seal, "at_us")?;
    let mut candidates = Vec::new();
    for role in roles {
        let receipt = role_event(origin, role, "request")?;
        require(
            number(receipt, "at_us")? <= sealed,
            "timeline sealed before an initial receipt",
        )?;
        candidates.push((
            *role,
            i128::from(number(receipt, "at_us")?) - i128::from(number(receipt, "guest_at_us")?),
        ));
    }
    let (selected, minimum) = candidates
        .iter()
        .min_by_key(|(_, offset)| offset)
        .ok_or("missing timeline candidates")?;
    let aligned = seal["scenario_anchor_host_offset_us"]
        .as_i64()
        .ok_or("missing sealed epoch")?;
    require(
        seal["basis"] == "initial-receipts-minimum-upper-bound"
            && seal["receipt_roles"] == json!(roles)
            && seal["selected_role"] == *selected
            && seal["rounding_guard_us"] == 1000
            && i128::from(aligned) == minimum + 1000
            && seal["clock_sync_proven"] == false
            && seal["same_rate_assumed"] == true
            && seal.get("transit_uncertainty_us") == Some(&Value::Null)
            && i128::from(sealed)
                <= i128::from(aligned)
                    + match case {
                        "fast-origin" => 120000,
                        "body-read-arrivals" | "trickling-slot" => 100000,
                        _ => 180000,
                    },
        "invalid or late sealed timeline epoch",
    )?;
    for write in origin
        .iter()
        .filter(|entry| matches!(entry["op"].as_str(), Some("head" | "chunk")))
    {
        require(
            number(write, "write_started_us")? >= sealed,
            "source wrote before sealing its timeline",
        )?;
    }
    Ok(i128::from(aligned))
}

fn cpu_clock_bounds(events: &[Value], origin: &Value) -> Result<[i128; 2]> {
    let origin = origin.as_array().ok_or("missing CPU origin events")?;
    let request = role_event(origin, "origin", "request")?;
    let head = role_event(origin, "origin", "head")?;
    let observed = event(events, "origin-head")?;
    let body = event(events, "origin-body")?;
    let calibration = event(events, "calibration-send")?;
    let calibrated = role_event(origin, "calibration", "request")?;
    let payload = role_event(origin, "origin", "chunk")?;
    let observed_us = number(observed, "at_us")? + number(observed, "duration_us")?;
    require(
        calibration["result"]["origin_head_observed_us"] == observed_us
            && calibrated["origin_head_observed_us"] == observed_us
            && number(body, "at_us")? >= observed_us
            && number(calibrated, "guest_at_us")?
                >= number(body, "at_us")? + number(body, "duration_us")?
            && number(calibrated, "guest_at_us")? <= number(calibration, "at_us")?,
        "calibration does not follow the observed origin body",
    )?;
    require(
        number(calibrated, "at_us")? >= number(payload, "write_started_us")?,
        "calibration reached the host before the origin payload write",
    )?;
    // With offset = host - guest, sending precedes receipt in each direction.
    let lower = i128::from(number(head, "write_started_us")?) - i128::from(observed_us) - 1000;
    let upper = (i128::from(number(request, "at_us")?)
        - i128::from(number(request, "guest_at_us")?))
    .min(i128::from(number(calibrated, "at_us")?) - i128::from(number(calibrated, "guest_at_us")?))
        + 1000;
    require(lower <= upper, "contradictory CPU clock bounds")?;
    Ok([lower, upper])
}

fn validate_cpu_stretch(report: &Value, events: &[Value], origin: &Value) -> Result<()> {
    require(
        events
            .iter()
            .map(|entry| entry["op"].as_str().unwrap_or(""))
            .collect::<Vec<_>>()
            == [
                "begin",
                "batch-start",
                "origin-head",
                "origin-body",
                "calibration-send",
                "next",
                "cpu-stretch",
                "collect",
            ],
        "CPU probe performed an unexpected operation",
    )?;
    require(
        report["origin"] == json!({"status":200,"bytes":[79]}) && report["mediator"].is_null(),
        "CPU origin outcome changed",
    )?;
    within(
        number(event(events, "begin")?, "at_us")?,
        2000,
        "CPU origin begin missed 2 ms",
    )?;
    let batch = event(events, "batch-start")?;
    let batch_started = number(batch, "at_us")?;
    let batch_ended = batch_started + number(batch, "duration_us")?;
    require(
        (34000..=39000).contains(&batch_started),
        "CPU batch start missed 34 ms",
    )?;
    require(
        batch["result"]["cutoff_at_us"] == 534000,
        "CPU probe restarted the observation cutoff",
    )?;
    let next = event(events, "next")?;
    let next_ended = number(next, "at_us")? + number(next, "duration_us")?;
    within(
        number(next, "at_us")?,
        500000,
        "pre-CPU observation missed 500 ms",
    )?;
    let busy = event(events, "cpu-stretch")?;
    let start = number(busy, "at_us")?;
    let end = start + number(busy, "duration_us")?;
    let collected = event(events, "collect")?;
    require(
        (520000..=525000).contains(&start)
            && (560000..=565000).contains(&end)
            && next["result"] == "Item"
            && next_ended <= start
            && busy["result"]["max_iterations"] == 10_000_000
            && busy["result"]["iterations"]
                .as_u64()
                .is_some_and(|iterations| (1..=10_000_000).contains(&iterations))
            && number(collected, "at_us")? >= end
            && number(collected, "duration_us")? < 5000
            && collected["result"] == "ok",
        "CPU work did not span the fixed 520..560 ms interval",
    )?;
    within(
        number(busy, "duration_us")?,
        40000,
        "CPU work duration changed",
    )?;
    let [lower, upper] = cpu_clock_bounds(events, origin)?;
    let origin = origin.as_array().unwrap();
    require(
        origin.len() <= 19
            && origin.iter().all(|entry| {
                matches!(
                    entry["role"].as_str(),
                    Some("origin" | "a" | "b" | "c" | "calibration")
                ) && matches!(
                    entry["op"].as_str(),
                    Some("request" | "head" | "chunk" | "closed")
                )
            }),
        "unexpected CPU origin evidence",
    )?;
    let calibration = event(events, "calibration-send")?;
    let calibrated_end = number(calibration, "at_us")? + number(calibration, "duration_us")?;
    require(
        calibrated_end <= 500000,
        "calibration missed the pre-CPU wait",
    )?;
    let phases: Vec<_> = origin
        .iter()
        .filter(|entry| entry["role"] == "calibration")
        .map(|entry| entry["op"].as_str().unwrap())
        .collect();
    let request = role_event(origin, "calibration", "request")?;
    let head = role_event(origin, "calibration", "head")?;
    let closed = role_event(origin, "calibration", "closed")?;
    require(
        calibration["result"]["outcome"] == json!({"status":204,"bytes":[]})
            && phases == ["request", "head", "closed"]
            && closed["reason"] == "complete"
            && number(request, "at_us")? <= number(head, "write_started_us")?
            && number(head, "write_started_us")? <= number(head, "at_us")?
            && number(head, "at_us")? <= number(closed, "at_us")?
            && i128::from(number(head, "write_started_us")?) - upper >= 450000
            && i128::from(number(head, "write_started_us")?) - upper
                <= i128::from(calibrated_end) + 1000
            && calibrated_end <= number(next, "at_us")?,
        "calibration response did not preserve its bounded wait",
    )?;
    let c_closed = role_event(origin, "c", "closed")?;
    let c_complete = c_closed["reason"] == "complete";
    require(
        matches!(
            c_closed["reason"].as_str(),
            Some("complete" | "peer_closed_after_cutoff")
        ) && i128::from(number(c_closed, "at_us")?) - upper >= 534000,
        "C did not remain outside the observation cutoff",
    )?;
    if !c_complete {
        let phases: Vec<_> = origin
            .iter()
            .filter(|entry| entry["role"] == "c")
            .map(|entry| entry["op"].as_str().unwrap())
            .collect();
        require(
            phases == ["request", "closed"] || phases == ["request", "head", "closed"],
            "unexpected C peer-close evidence",
        )?;
    }
    for (role, byte) in [("origin", b'O'), ("a", b'A'), ("b", b'B'), ("c", b'C')]
        .into_iter()
        .filter(|(role, _)| *role != "c" || c_complete)
    {
        let phases: Vec<_> = origin
            .iter()
            .filter(|entry| entry["role"] == role)
            .map(|entry| entry["op"].as_str().unwrap())
            .collect();
        require(
            phases == ["request", "head", "chunk", "closed"],
            "CPU origin phase ordering changed",
        )?;
        let request = role_event(origin, role, "request")?;
        let head = role_event(origin, role, "head")?;
        let chunk = role_event(origin, role, "chunk")?;
        let closed = role_event(origin, role, "closed")?;
        require(
            number(request, "at_us")? <= number(head, "write_started_us")?
                && number(head, "write_started_us")? <= number(head, "at_us")?
                && number(head, "at_us")? <= number(chunk, "write_started_us")?
                && number(chunk, "write_started_us")? <= number(chunk, "at_us")?
                && number(chunk, "at_us")? <= number(closed, "at_us")?
                && chunk["bytes"] == json!([byte])
                && closed["reason"] == "complete",
            "CPU response evidence changed",
        )?;
    }
    if let Some(head) = origin
        .iter()
        .find(|entry| entry["role"] == "c" && entry["op"] == "head")
    {
        let request = role_event(origin, "c", "request")?;
        require(
            number(head, "write_started_us")? >= number(request, "at_us")?
                && number(head, "at_us")? >= number(head, "write_started_us")?
                && number(c_closed, "at_us")? >= number(head, "at_us")?
                && i128::from(number(head, "write_started_us")?) - upper >= 534000,
            "C response was written before the observation cutoff",
        )?;
    }
    let head = role_event(origin, "b", "head")?;
    let chunk = role_event(origin, "b", "chunk")?;
    let write_start = i128::from(number(head, "write_started_us")?);
    let payload_start = i128::from(number(chunk, "write_started_us")?);
    let write_end = i128::from(number(chunk, "at_us")?);
    require(
        i128::from(start) + upper <= write_start && write_end <= i128::from(end) + lower,
        "B response write was not proven inside CPU work",
    )?;
    require(
        write_start - upper >= 525000 && write_end - lower <= 535000,
        format!(
            "B response write missed 530 ms: guest interval {}..{} us",
            write_start - upper,
            write_end - lower
        ),
    )?;
    let delivered = report["slots"][1] == "Resolved";
    if delivered {
        validate_items(
            report,
            &[146000],
            &["next", "collect"],
            &["AlreadyDelivered", "Resolved", "Unresolved"],
            "Cutoff",
        )?;
        let bidder = &report["items"][1];
        within(
            number(bidder, "elapsed_us")?,
            496000,
            "B arrival elapsed changed during CPU work",
        )?;
    } else {
        validate_items(
            report,
            &[146000],
            &["next"],
            &["AlreadyDelivered", "Unresolved", "Unresolved"],
            "Cutoff",
        )?;
    }
    let a_payload_start = i128::from(number(
        role_event(origin, "a", "chunk")?,
        "write_started_us",
    )?);
    require(
        report["items"]
            .as_array()
            .unwrap()
            .iter()
            .enumerate()
            .all(|(index, item)| {
                number(item, "elapsed_us").is_ok_and(|elapsed| {
                    // Core samples its epoch inside the measured invocation.
                    let earliest_arrival = batch_started + elapsed;
                    let latest_arrival = batch_ended + elapsed;
                    let earliest = if index == 0 {
                        a_payload_start - upper
                    } else {
                        payload_start - upper
                    };
                    elapsed < 500000
                        && i128::from(earliest_arrival) >= earliest
                        && latest_arrival < 534000
                        && if index == 0 {
                            earliest_arrival >= number(calibration, "at_us").unwrap_or(u64::MAX)
                                && latest_arrival <= calibrated_end
                                && latest_arrival <= next_ended
                        } else {
                            earliest_arrival >= start && latest_arrival <= end
                        }
                })
            }),
        "a post-cutoff observation was delivered as an on-time CPU slot",
    )
}

fn validate_native_pending(
    boundary: AdapterBoundary<'_>,
    case: &str,
    token: &str,
    report: &Value,
    events: &[Value],
) -> Result<()> {
    let snapshots = report["native_body"]
        .as_array()
        .ok_or("missing native SDK body evidence")?;
    require(snapshots.len() == 3, "missing or extra native SDK witness")?;
    let batch = event(events, "batch-start")?;
    let next = event(events, "next")?;
    let returned = number(next, "at_us")? + number(next, "duration_us")?;
    for (snapshot, role) in snapshots.iter().zip(["a", "b", "c"]) {
        let prepared = &snapshot["preparation"];
        let entered = number(prepared, "entered_at_us")?;
        require(
            snapshot["invalid"] == false
                && snapshot["attachments"] == 1
                && snapshot["preparations"] == 1
                && prepared["mode"] == "Batch"
                && prepared["cause"] == "BatchCutoff"
                && prepared["deadline_at_us"] == 200_000
                && prepared["target_uri"]
                    == format!("{}/lifetimes/{token}/{case}/{role}", boundary.origin)
                && entered >= number(batch, "at_us")?
                && entered <= number(next, "at_us")?
                && entered < 200_000,
            "native SDK witness is reused, foreign or has different dispatch policies",
        )?;
        if case == "pending-body-cutoff" {
            if boundary.name == "fastly" {
                validate_native_body_wait(snapshot, entered, returned)?;
                continue;
            }
            let read = &snapshot["first_read"];
            let head = number(snapshot, "head_at_us")?;
            let started = number(read, "started_at_us")?;
            let observed = number(read, "observed_at_us")?;
            let released = number(snapshot, "released_at_us")?;
            require(
                snapshot["heads"] == 1
                    && read["pending"] == true
                    && snapshot["reads"].as_u64().is_some_and(|count| count > 0)
                    && snapshot["ready_reads"] == 0
                    && entered <= head
                    && head <= started
                    && started <= observed
                    && observed < 200_000
                    && observed <= returned
                    && snapshot["source_acquisitions"] == 1
                    && snapshot["source_releases"] == 1
                    && (200_000..=returned).contains(&released),
                "native SDK body read was not pending before cutoff or its owner was not released",
            )?;
        } else {
            require(
                snapshot["heads"] == 0
                    && snapshot["head_at_us"].is_null()
                    && snapshot["first_read"].is_null()
                    && snapshot["reads"] == 0
                    && snapshot["ready_reads"] == 0
                    && snapshot["source_acquisitions"] == 0
                    && snapshot["source_releases"] == 0
                    && snapshot["released_at_us"].is_null()
                    && snapshot["waits"] == 0
                    && snapshot["ready_waits"] == 0
                    && snapshot["failed_waits"] == 0
                    && snapshot["first_wait"].is_null()
                    && snapshot["first_wait_timeout"].is_null()
                    && snapshot["last_wait_timeout"].is_null(),
                "head-only cutoff unexpectedly reached the native body",
            )?;
        }
    }
    Ok(())
}

fn validate_native_body_wait(snapshot: &Value, entered: u64, returned: u64) -> Result<()> {
    let first = &snapshot["first_wait"];
    let first_timeout = &snapshot["first_wait_timeout"];
    let last_timeout = &snapshot["last_wait_timeout"];
    let head = number(snapshot, "head_at_us")?;
    let started = number(first, "started_at_us")?;
    let observed = number(first, "observed_at_us")?;
    let initial_start = number(first_timeout, "started_at_us")?;
    let initial_end = number(first_timeout, "observed_at_us")?;
    let last_start = number(last_timeout, "started_at_us")?;
    let last_end = number(last_timeout, "observed_at_us")?;
    let released = number(snapshot, "released_at_us")?;
    let waits = number(snapshot, "waits")?;
    let distinct_timeouts = initial_start != last_start || initial_end != last_end;
    let minimum_waits =
        1 + u64::from(first["outcome"] != "TimedOut") + u64::from(distinct_timeouts);
    let first_call_matches = if first["outcome"] == "TimedOut" {
        started == initial_start && observed == initial_end
    } else {
        observed <= initial_start
    };
    let timeout_calls_match = !distinct_timeouts || initial_end <= last_start;
    require(
        snapshot["heads"] == 1
            && snapshot["first_read"].is_null()
            && snapshot["reads"] == 0
            && snapshot["ready_reads"] == 0
            && waits >= minimum_waits
            && first_call_matches
            && timeout_calls_match
            && snapshot["ready_waits"] == 0
            && snapshot["failed_waits"] == 0
            && first["kind"] == "BodyReadiness"
            && matches!(first["outcome"].as_str(), Some("TimedOut" | "OtherReady"))
            && first_timeout["kind"] == "BodyReadiness"
            && first_timeout["outcome"] == "TimedOut"
            && last_timeout["kind"] == "BodyReadiness"
            && last_timeout["outcome"] == "TimedOut"
            && entered <= head
            && head <= started
            && started <= observed
            && started <= initial_start
            && initial_start <= initial_end
            && observed <= initial_end
            && initial_start <= last_start
            && initial_end <= last_end
            && last_start < 200_000
            && (200_000..=returned).contains(&last_end)
            && snapshot["source_acquisitions"] == 1
            && snapshot["source_releases"] == 1
            && (last_end..=returned).contains(&released),
        "native body readiness did not span cutoff or its SDK owner was not released",
    )
}

fn validate_pending_cutoff(
    case: &str,
    report: &Value,
    events: &[Value],
    origin: &Value,
) -> Result<()> {
    validate_items(report, &[], &[], &["Unresolved"; 3], "Cutoff")?;
    require(
        report.get("origin") == Some(&Value::Null)
            && report.get("mediator") == Some(&Value::Null)
            && events.len() == 3,
        "unexpected operation in pending cutoff probe",
    )?;
    let batch = event(events, "batch-start")?;
    let next = event(events, "next")?;
    let collected = event(events, "collect")?;
    within(number(batch, "at_us")?, 34_000, "batch start missed 34 ms")?;
    require(
        batch["result"]["cutoff_at_us"] == 200_000,
        "pending cutoff was restarted",
    )?;
    let first = number(&next["result"], "first_poll_us")?;
    let returned = number(next, "at_us")? + number(next, "duration_us")?;
    require(
        next["result"]["state"] == "Cutoff"
            && next["result"]["pending_polls"]
                .as_u64()
                .is_some_and(|count| count > 0)
            && first >= number(next, "at_us")?
            && first < 200_000
            && number(next, "at_us")? >= number(batch, "at_us")? + number(batch, "duration_us")?
            && (200_000..=205_000).contains(&returned)
            && number(collected, "at_us")? >= returned
            && number(collected, "duration_us")? < 5_000
            && collected["result"] == "ok",
        "next did not wait for the original cutoff within 205 ms",
    )?;
    let origin = origin.as_array().unwrap();
    require(
        origin.iter().all(|entry| {
            matches!(
                entry["op"].as_str(),
                Some("request" | "head" | "waiting" | "release" | "closed")
            )
        }),
        "unexpected body data or origin event during cutoff wait",
    )?;
    let release = event(origin, "release")?;
    let response_returned = number(release, "response_returned_us")?;
    require(
        response_returned <= number(release, "at_us")?,
        "fixture released before the response returned",
    )?;
    let release_index = origin
        .iter()
        .position(|entry| entry["op"] == "release")
        .unwrap();
    require(release["role"] == "control", "invalid fixture release")?;
    for role in ["a", "b", "c"] {
        let position = |op: &str| {
            origin
                .iter()
                .position(|entry| entry["role"] == role && entry["op"] == op)
                .ok_or("missing origin phase")
        };
        let entries: Vec<_> = origin
            .iter()
            .filter(|entry| entry["role"] == role)
            .cloned()
            .collect();
        let request = event(&entries, "request")?;
        let waiting = event(&entries, "waiting")?;
        let closed = event(&entries, "closed")?;
        require(
            position("request")? < position("waiting")?
                && position("waiting")? < release_index
                && release_index < position("closed")?
                && number(request, "at_us")? <= number(waiting, "at_us")?
                && number(waiting, "at_us")? <= number(release, "at_us")?
                && number(waiting, "at_us")? <= response_returned
                && number(closed, "at_us")? >= number(release, "at_us")?
                && closed["reason"] == "fixture_released",
            "origin was not held until explicit fixture release",
        )?;
        let heads: Vec<_> = entries
            .iter()
            .filter(|entry| entry["op"] == "head")
            .collect();
        if case == "pending-body-cutoff" {
            require(
                heads.len() == 1,
                "pending-body probe lacks one written head",
            )?;
            let head = heads[0];
            require(
                head["content_length"] == 1
                    && position("request")? < position("head")?
                    && position("head")? < position("waiting")?
                    && number(head, "write_started_us")? >= number(request, "at_us")?
                    && number(head, "write_started_us")? <= number(head, "at_us")?
                    && number(head, "at_us")? <= number(waiting, "at_us")?,
                "pending-body head was not written before waiting",
            )?;
        } else {
            require(
                heads.is_empty(),
                "pending-head probe unexpectedly sent headers",
            )?;
        }
    }
    require(
        origin.len()
            == if case == "pending-body-cutoff" {
                13
            } else {
                10
            },
        "missing or extra pending origin evidence",
    )
}

fn number(value: &Value, field: &str) -> Result<u64> {
    value[field]
        .as_u64()
        .filter(|value| *value <= 30_000_000)
        .ok_or_else(|| format!("missing measured {field}").into())
}

fn clock_offset_lower(case: &str, events: &[Value], origin: &Value) -> Result<i128> {
    let (role, observed) = if case == "begin-progress" {
        let head = event(events, "origin-head")?;
        (
            "origin",
            number(head, "at_us")? + number(head, "duration_us")?,
        )
    } else {
        ("quiet", number(event(events, "quiet-end")?, "at_us")?)
    };
    let heads: Vec<_> = origin
        .as_array()
        .unwrap()
        .iter()
        .filter(|entry| entry["op"] == "head" && entry["role"] == role)
        .collect();
    require(
        heads.len() == 1,
        "missing or duplicate quiet clock boundary",
    )?;
    let started = number(heads[0], "write_started_us")?;
    require(
        started <= number(heads[0], "at_us")?,
        "head write started after completion",
    )?;
    // Sending precedes guest observation. This lower bound survives cold-entry offsets;
    // the measurement guard makes receipt-within-window checks more conservative.
    Ok(i128::from(started) - i128::from(observed) - 5_000)
}

fn event<'a>(events: &'a [Value], op: &str) -> Result<&'a Value> {
    let matching: Vec<_> = events.iter().filter(|event| event["op"] == op).collect();
    require(
        matching.len() == 1,
        format!("missing or duplicate {op} event"),
    )?;
    Ok(matching[0])
}

fn within(actual: u64, expected: u64, message: &str) -> Result<()> {
    require(
        actual.abs_diff(expected) <= 5_000,
        format!("{message}: measured {actual} us, expected {expected} +/- 5000 us"),
    )
}

fn validate_items(
    report: &Value,
    elapsed: &[u64],
    via: &[&str],
    slots: &[&str],
    termination: &str,
) -> Result<()> {
    require(
        report["slots"] == json!(slots) && report["termination"] == termination,
        "wrong lifetime collection states or termination",
    )?;
    let items = report["items"].as_array().ok_or("missing lifetime items")?;
    require(items.len() == via.len(), "missing or extra lifetime items")?;
    for (index, item) in items.iter().enumerate() {
        require(
            item["index"] == index
                && item["via"] == via[index]
                && item["outcome"] == json!({"status":200,"bytes":[b'A' + index as u8]}),
            "wrong lifetime item identity, observation or outcome",
        )?;
        if let Some(expected) = elapsed.get(index) {
            within(
                number(item, "elapsed_us")?,
                *expected,
                "slot elapsed did not record arrival",
            )?;
        }
    }
    Ok(())
}

fn validate_late_c(origin: &[Value], aligned: i128) -> Result<()> {
    let phases: Vec<_> = origin
        .iter()
        .filter(|entry| entry["role"] == "c")
        .map(|entry| entry["op"].as_str().unwrap_or(""))
        .collect();
    let request = role_event(origin, "c", "request")?;
    let closed = role_event(origin, "c", "closed")?;
    let scheduled = u64::try_from(aligned + 900000)?;
    require(
        number(request, "at_us")? <= number(closed, "at_us")?
            && number(closed, "at_us")? >= scheduled,
        "C source closed before its late response schedule",
    )?;
    match closed["reason"].as_str() {
        Some("complete") => require(
            phases == ["request", "head", "chunk", "closed"],
            "missing or extra late C response evidence",
        )?,
        Some("peer_closed_after_cutoff") => require(
            phases == ["request", "closed"] || phases == ["request", "head", "closed"],
            "unexpected late C peer-close evidence",
        )?,
        _ => return Err("C source did not complete or observe a late peer close".into()),
    }
    for entry in origin.iter().filter(|entry| {
        entry["role"] == "c" && matches!(entry["op"].as_str(), Some("head" | "chunk"))
    }) {
        let actual = number(entry, "at_us")?;
        let started = number(entry, "write_started_us")?;
        require(
            entry["scenario_scheduled_us"] == 900000
                && number(entry, "scheduled_us")? == scheduled
                && entry["schedule_basis"] == "initial-receipts-minimum-upper-bound"
                && number(entry, "wait_returned_us")? >= scheduled
                && started >= number(entry, "wait_returned_us")?
                && started <= actual
                && actual <= number(closed, "at_us")?,
            "late C source schedule or phase order changed",
        )?;
        within(actual, scheduled, "late C origin scheduling target missed")?;
        if entry["op"] == "head" {
            require(
                started >= number(request, "at_us")?,
                "C response head began before its late schedule",
            )?;
        } else {
            require(
                entry["bytes"] == json!([67])
                    && started >= number(role_event(origin, "c", "head")?, "at_us")?,
                "late C payload changed or preceded its head",
            )?;
        }
    }
    Ok(())
}

fn event_end(entry: &Value) -> Result<u64> {
    number(entry, "at_us")?
        .checked_add(number(entry, "duration_us")?)
        .ok_or_else(|| "invalid event duration".into())
}

fn sequential_events(events: &[Value], expected: &[&str]) -> Result<()> {
    require(
        events
            .iter()
            .map(|entry| entry["op"].as_str().unwrap_or(""))
            .collect::<Vec<_>>()
            == expected,
        "lifetime probe performed an unexpected operation",
    )?;
    for pair in events.windows(2) {
        require(
            event_end(&pair[0])? <= number(&pair[1], "at_us")?,
            "lifetime probe operations overlap",
        )?;
    }
    Ok(())
}

fn validate_send_during_batch(report: &Value, events: &[Value], origin: &Value) -> Result<()> {
    sequential_events(
        events,
        &[
            "batch-start",
            "origin-send",
            "check",
            "check",
            "check",
            "collect",
            "origin-body",
        ],
    )?;
    let batch = event(events, "batch-start")?;
    let send = event(events, "origin-send")?;
    within(number(batch, "at_us")?, 34000, "batch start missed 34 ms")?;
    require(
        batch["result"]["cutoff_at_us"] == 534000
            && send["result"] == "ok"
            && report["mediator"].is_null(),
        "send changed the original live batch",
    )?;
    let request = role_event(
        origin.as_array().ok_or("missing origin")?,
        "origin",
        "request",
    )?;
    let prepared = number(request, "guest_at_us")?;
    require(
        prepared >= number(send, "at_us")? && prepared <= event_end(send)? && prepared < 534000,
        "origin send was not prepared during the live batch",
    )?;
    validate_timeline("send-during-batch", events, origin)?;
    let first_check = events
        .iter()
        .find(|entry| entry["op"] == "check")
        .ok_or("missing live-batch check")?;
    within(
        number(first_check, "at_us")?,
        701000,
        "first batch check missed 701 ms",
    )?;
    require(
        report["origin"] == json!({"status":200,"bytes":[79,111,33]}),
        "sent origin body changed",
    )?;
    for (entry, result) in events
        .iter()
        .filter(|entry| entry["op"] == "check")
        .zip(["Item", "Item", "Finished"])
    {
        require(
            entry["result"] == result,
            "live-batch check sequence changed",
        )?;
    }
    require(
        event(events, "collect")?["result"] == "ok",
        "live-batch collection failed",
    )?;
    validate_items(
        report,
        &[146000, 286000],
        &["check", "check"],
        &["AlreadyDelivered", "AlreadyDelivered", "Unresolved"],
        "Cutoff",
    )
}

fn validate_trickling_slot(report: &Value, events: &[Value], origin: &Value) -> Result<()> {
    sequential_events(
        events,
        &["begin", "batch-start", "origin-head", "trickle", "collect"],
    )?;
    let begin = event(events, "begin")?;
    let batch = event(events, "batch-start")?;
    let head = event(events, "origin-head")?;
    let trickle = event(events, "trickle")?;
    within(number(begin, "at_us")?, 2000, "origin begin missed 2 ms")?;
    within(number(batch, "at_us")?, 34000, "batch start missed 34 ms")?;
    require(
        begin["result"] == "scheduled"
            && head["result"] == "ok"
            && batch["result"]["cutoff_at_us"] == 534000
            && report["origin"] == json!({"status":200,"bytes":b"0123456789:"})
            && report["mediator"].is_null(),
        "trickling probe changed its original exchanges",
    )?;
    let log = trickle["result"]["observations"]
        .as_array()
        .ok_or("missing trickle observations")?;
    require(log.len() <= 68, "trickle observation limit exceeded")?;
    let next = log
        .iter()
        .filter(|entry| entry["op"] == "next")
        .collect::<Vec<_>>();
    require(
        next.len() == 2
            && next[0]["result"] == "Item"
            && next[0]["index"] == 0
            && next[1]["result"] == "Finished"
            && next[1]["termination"] == "Cutoff",
        "trickling slot was delivered or cutoff was not observed",
    )?;
    let observation_returned_at = number(next[1], "at_us")?;
    require(
        (534000..=539000).contains(&observation_returned_at)
            && number(next[1], "started_at_us")? < 534000,
        "trickling slot delayed the waiting cutoff",
    )?;
    let mut previous = number(trickle, "at_us")?;
    let mut read_end = previous;
    let mut next_end = previous;
    let mut reads = 0;
    let mut bytes = 0;
    let mut eof = false;
    for entry in log {
        let started = number(entry, "started_at_us")?;
        let observed = number(entry, "at_us")?;
        require(
            started >= number(trickle, "at_us")?
                && started <= observed
                && observed >= previous
                && observed <= event_end(trickle)?,
            "trickle observations are inverted or reordered",
        )?;
        previous = observed;
        match entry["op"].as_str() {
            Some("next") => {
                require(started >= next_end, "batch observations overlap")?;
                next_end = observed;
            }
            Some("read") => {
                require(
                    !eof && started >= read_end,
                    "origin reads overlap or follow EOF",
                )?;
                read_end = observed;
                reads += 1;
                let result = entry.get("result").ok_or("missing origin read result")?;
                if result.is_null() {
                    eof = true;
                    within(observed, 600000, "trickling origin EOF missed 600 ms")?;
                } else {
                    for byte in result.as_array().ok_or("trickling origin read failed")? {
                        require(
                            bytes < 11 && byte.as_u64() == Some(48 + bytes),
                            "trickling origin bytes changed",
                        )?;
                        within(
                            observed,
                            100000 + bytes * 50000,
                            "origin chunk stalled behind a bidder body",
                        )?;
                        if bytes == 1 {
                            require(
                                number(next[0], "started_at_us")? < observed,
                                "batch wait began only after origin progress",
                            )?;
                        }
                        if (2..=8).contains(&bytes) {
                            require(
                                number(next[1], "started_at_us")? <= observed
                                    && observed < observation_returned_at,
                                "origin progress did not overlap the waiting cutoff",
                            )?;
                        }
                        if bytes >= 9 {
                            require(
                                observed > observation_returned_at,
                                "origin stopped when the batch reached cutoff",
                            )?;
                        }
                        bytes += 1;
                    }
                }
            }
            _ => return Err("unexpected trickle operation".into()),
        }
    }
    require(
        reads <= 64 && bytes == 11 && eof,
        "trickling origin lacked bounded complete progress",
    )?;
    require(
        event(events, "collect")?["result"] == "ok"
            && number(event(events, "collect")?, "duration_us")? < 5000,
        "trickle collection blocked or failed",
    )?;
    validate_timeline("trickling-slot", events, origin)?;
    validate_items(
        report,
        &[146000],
        &["next"],
        &["AlreadyDelivered", "Unresolved", "Unresolved"],
        "Cutoff",
    )
}

fn validate_body_read_arrivals(report: &Value, events: &[Value], origin: &Value) -> Result<()> {
    sequential_events(
        events,
        &[
            "begin",
            "batch-start",
            "origin-head",
            "origin-body",
            "check",
            "check",
            "check",
            "collect",
        ],
    )?;
    let begin = event(events, "begin")?;
    require(begin["result"] == "scheduled", "origin begin failed")?;
    within(number(begin, "at_us")?, 2000, "origin begin missed 2 ms")?;
    let batch = event(events, "batch-start")?;
    within(number(batch, "at_us")?, 34000, "batch start missed 34 ms")?;
    let head = event(events, "origin-head")?;
    let body = event(events, "origin-body")?;
    let body_start = number(body, "at_us")?;
    let body_end = event_end(body)?;
    within(
        body_start,
        100000,
        "origin body read did not start at its head",
    )?;
    require(
        batch["result"]["cutoff_at_us"] == 534000
            && event_end(batch)? <= number(head, "at_us")?
            && head["result"] == "ok"
            && event_end(head)? <= body_start
            && body["result"] == "streamed"
            && report["origin"] == json!({"status":200,"bytes":[79,111]})
            && report["mediator"].is_null(),
        "body-read arrival probe changed its original exchange, cutoff or source bytes",
    )?;
    for (entry, result) in events
        .iter()
        .filter(|entry| entry["op"] == "check")
        .zip(["Item", "Item", "Finished"])
    {
        require(
            number(entry, "at_us")? >= body_end && entry["result"] == result,
            "batch check preceded origin EOF or changed terminal order",
        )?;
    }
    let collected = event(events, "collect")?;
    require(
        number(collected, "at_us")? >= body_end
            && number(collected, "duration_us")? < 5000
            && collected["result"] == "ok",
        "late body-read collection blocked or failed",
    )?;
    validate_timeline("body-read-arrivals", events, origin)?;
    validate_items(
        report,
        &[146000, 286000],
        &["check", "check"],
        &["AlreadyDelivered", "AlreadyDelivered", "Unresolved"],
        "Cutoff",
    )
}

fn validate_timeline(case: &str, events: &[Value], origin: &Value) -> Result<()> {
    validate_guest_timeline(case, events)?;
    validate_source_timeline(case, origin)
}

fn validate_guest_timeline(case: &str, events: &[Value]) -> Result<()> {
    let slow = matches!(
        case,
        "slow-origin" | "timeout-before-cutoff" | "send-during-batch"
    );
    let body_reads = case == "body-read-arrivals";
    let trickle = case == "trickling-slot";
    let head = event(
        events,
        if case == "send-during-batch" {
            "origin-send"
        } else {
            "origin-head"
        },
    )?;
    within(
        event_end(head)?,
        if slow {
            700_000
        } else if body_reads || trickle {
            100_000
        } else {
            120_000
        },
        "origin head missed scenario time",
    )?;
    let body = event(events, if trickle { "trickle" } else { "origin-body" })?;
    within(
        event_end(body)?,
        if slow {
            1_300_000
        } else if body_reads {
            800_000
        } else if trickle {
            600_000
        } else {
            260_000
        },
        "origin EOF missed scenario time",
    )
}

fn validate_source_timeline(case: &str, origin: &Value) -> Result<()> {
    let slow = matches!(
        case,
        "slow-origin" | "timeout-before-cutoff" | "send-during-batch"
    );
    let body_reads = case == "body-read-arrivals";
    let trickle = case == "trickling-slot";
    let origin = origin.as_array().ok_or("missing source timeline")?;
    let request = origin
        .iter()
        .find(|entry| entry["op"] == "request" && entry["role"] == "origin")
        .ok_or("missing origin receipt")?;
    require(
        request["alignment"]["basis"] == "origin-receipt-minus-guest-at-us"
            && request["alignment"]["clock_sync_proven"] == false
            && request["alignment"].get("transit_uncertainty_us") == Some(&Value::Null),
        "scenario alignment must disclose unproven clock synchronization",
    )?;
    let aligned = request["alignment"]["scenario_anchor_host_offset_us"]
        .as_i64()
        .filter(|value| value.unsigned_abs() <= 30_000_000)
        .ok_or("missing measured scenario alignment")?;
    let aligned = i128::from(aligned);
    require(
        (i128::from(number(request, "at_us")?)
            - i128::from(number(request, "guest_at_us")?)
            - aligned)
            .abs()
            <= 1,
        "scenario alignment differs from original origin receipt",
    )?;
    let aligned = timeline_epoch(origin, case)?;
    for (role, head_at, chunk_times, bytes) in [
        (
            "origin",
            if slow {
                700_000
            } else if body_reads || trickle {
                100_000
            } else {
                120_000
            },
            if slow {
                vec![700_000, 900_000, 1_300_000]
            } else if body_reads {
                vec![100_000, 800_000]
            } else if trickle {
                (0..11).map(|index| 100000 + index * 50000).collect()
            } else {
                vec![260_000]
            },
            if slow {
                b"Oo!".as_slice()
            } else if body_reads {
                b"Oo".as_slice()
            } else if trickle {
                b"0123456789:".as_slice()
            } else {
                b"O"
            },
        ),
        ("a", 180_000, vec![180_000], b"A"),
        (
            "b",
            if trickle { 100_000 } else { 320_000 },
            vec![if trickle { 600_000 } else { 320_000 }],
            b"B",
        ),
    ]
    .into_iter()
    .chain((!slow && !body_reads && !trickle).then_some((
        "c",
        400_000,
        vec![400_000],
        b"C".as_slice(),
    ))) {
        let heads: Vec<_> = origin
            .iter()
            .filter(|entry| entry["op"] == "head" && entry["role"] == role)
            .collect();
        let chunks: Vec<_> = origin
            .iter()
            .filter(|entry| entry["op"] == "chunk" && entry["role"] == role)
            .collect();
        require(
            heads.len() == 1
                && (chunks.len() == chunk_times.len()
                    || (trickle && role == "b" && chunks.is_empty())),
            "missing or extra origin writes",
        )?;
        if trickle && role == "b" {
            let closed = role_event(origin, "b", "closed")?;
            let phases = origin
                .iter()
                .filter(|entry| entry["role"] == "b")
                .map(|entry| entry["op"].as_str().unwrap_or(""))
                .collect::<Vec<_>>();
            require(
                heads[0]["body_scheduled_us"] == u64::try_from(aligned + 600000)?
                    && number(closed, "at_us")? >= u64::try_from(aligned + 600000)?
                    && number(heads[0], "write_started_us")?
                        >= number(role_event(origin, "b", "request")?, "at_us")?
                    && number(heads[0], "at_us")? <= number(closed, "at_us")?,
                "trickling bidder body schedule changed",
            )?;
            match closed["reason"].as_str() {
                Some("complete") => require(
                    phases == ["request", "head", "chunk", "closed"],
                    "trickling bidder phases changed",
                )?,
                Some("peer_closed_after_cutoff") => require(
                    phases == ["request", "head", "closed"],
                    "unexpected trickling bidder close",
                )?,
                _ => {
                    return Err(
                        "trickling bidder did not complete or observe a late peer close".into(),
                    );
                }
            }
        }
        for (entry, scheduled) in std::iter::once((heads[0], head_at)).chain(
            chunks
                .iter()
                .zip(chunk_times)
                .map(|(entry, time)| (*entry, time)),
        ) {
            require(
                number(entry, "scenario_scheduled_us")? == scheduled
                    && entry["schedule_basis"] == "initial-receipts-minimum-upper-bound",
                "wrong origin scenario schedule",
            )?;
            let actual = number(entry, "at_us")?;
            let scheduled = number(entry, "scheduled_us")?;
            let started = number(entry, "write_started_us")?;
            require(
                (i128::from(scheduled)
                    - aligned
                    - i128::from(number(entry, "scenario_scheduled_us")?))
                .abs()
                    <= 1,
                "host schedule was reanchored after registration",
            )?;
            require(
                number(entry, "wait_returned_us")? >= scheduled
                    && started >= number(entry, "wait_returned_us")?
                    && started <= actual,
                "origin write began before its schedule or after its completion",
            )?;
            within(actual, scheduled, "origin scheduling target missed")?;
        }
        for (entry, byte) in chunks.iter().zip(bytes) {
            require(
                entry["bytes"] == json!([byte])
                    && number(entry, "write_started_us")? >= number(heads[0], "at_us")?,
                "origin chunk bytes differ or precede the response head",
            )?;
        }
        if role == "origin" {
            let last = chunks.last().ok_or("missing final origin chunk")?;
            let terminal = &last["terminal_frame"];
            let closed = role_event(origin, "origin", "closed")?;
            require(
                terminal["status"] == "pass"
                    && terminal.get("error") == Some(&Value::Null)
                    && number(last, "at_us")? <= number(terminal, "write_started_us")?
                    && number(terminal, "write_started_us")? <= number(terminal, "completed_us")?
                    && number(terminal, "completed_us")? <= number(closed, "at_us")?
                    && closed["reason"] == "complete",
                "missing, failed or inverted origin terminal framing",
            )?;
        }
    }
    if slow || body_reads || trickle {
        validate_late_c(origin, aligned)?;
    }
    Ok(())
}

fn timing_diagnostics(case: &str, report: &Value, origin: &Value) -> Option<Value> {
    if !matches!(
        case,
        "slow-origin"
            | "timeout-before-cutoff"
            | "fast-origin"
            | "body-read-arrivals"
            | "trickling-slot"
            | "send-during-batch"
    ) {
        return None;
    }
    let guest = report["events"]
        .as_array()
        .ok_or_else(|| "missing guest timeline".into())
        .and_then(|events| validate_guest_timeline(case, events));
    let source = validate_source_timeline(case, origin);
    let metrics = (|| -> Result<Vec<Value>> {
        let entries = origin.as_array().ok_or("missing source timeline")?;
        require(entries.len() <= 32, "source phase count exceeds its bound")?;
        entries.iter().filter(|entry|entry.get("scheduled_us").is_some() && matches!(entry["op"].as_str(),Some("head"|"chunk")))
            .map(|entry| {
                let scheduled=number(entry,"scheduled_us")?;
                let waited=number(entry,"wait_returned_us")?;
                let started=number(entry,"write_started_us")?;
                let completed=number(entry,"at_us")?;
                Ok(json!({"role":entry["role"],"op":entry["op"],
                    "wait_lateness_us":waited.checked_sub(scheduled).ok_or("source wait returned early")?,
                    "post_wait_us":started.checked_sub(waited).ok_or("source write preceded wait return")?,
                    "write_duration_us":completed.checked_sub(started).ok_or("source write completed before it started")?}))
            }).collect()
    })();
    let verdict = |result: Result<()>| json!({"status":if result.is_ok(){"pass"}else{"fail"},"error":result.err().map(|error|error.to_string())});
    Some(
        json!({"scope":"sealed-origin-timeline-only","source_timeline":verdict(source),"guest_head_body":verdict(guest),
        "source_phases":metrics.as_ref().ok(),"metrics_error":metrics.as_ref().err().map(ToString::to_string)}),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    const AXUM: AdapterBoundary<'static> = AdapterBoundary {
        name: "axum",
        origin: "http://127.0.0.1:8080",
    };

    #[test]
    fn suite_failures_preserve_original_timing_tail_and_both_wrapper_diagnostics() {
        let error = combined_count(
            Err("original timing failure".into()),
            Err("late protocol failure".into()),
            Err("run_custom and serve_custom failed".into()),
        )
        .unwrap_err()
        .to_string();
        for expected in [
            "original timing failure",
            "late protocol failure",
            "run_custom and serve_custom failed",
        ] {
            assert!(error.contains(expected), "missing {expected}: {error}");
        }
        assert_eq!(
            combined_count(Ok(Some(12)), Ok(()), Ok(8)).unwrap(),
            Some(20)
        );
        assert_eq!(combined_count(Ok(None), Ok(()), Ok(0)).unwrap(), None);
    }

    fn handoff_sample() -> (Value, Value) {
        let prelude = json!({"schema":1,"case":"response-handoff","token":"handoff","phase":"pending-handoff",
            "origin_pending":true,"batch_pending":true,"slots":3,"cutoff_at_us":534000,"qualification":false,
            "callback_scope_closed":true,"body_scope_live":true,"dispatch_returned":null});
        let report = json!({"schema":1,"case":"response-handoff","token":"handoff","qualification":false,
        "error":null,"origin":{"status":200,"bytes":[79]},"mediator":null,
        "slots":["Resolved","Resolved","Resolved"],"termination":"Completed",
        "handoff":{"prelude":prelude,"callback_scope_closed":true,"body_scope_live":true,"origin_streamed":true,"dispatch_returned":null},
        "items":[
            {"index":0,"via":"collect","elapsed_us":12000,"slot_error":false,"outcome":{"status":200,"bytes":[65]}},
            {"index":1,"via":"collect","elapsed_us":14000,"slot_error":false,"outcome":{"status":200,"bytes":[66]}},
            {"index":2,"via":"collect","elapsed_us":16000,"slot_error":false,"outcome":{"status":200,"bytes":[67]}}
        ],"events":[
            {"op":"begin","at_us":2000,"duration_us":50,"result":"scheduled"},
            {"op":"batch-start","at_us":34000,"duration_us":100,"result":{"cutoff_at_us":534000}},
            {"op":"handoff-pending","at_us":34100,"duration_us":10,"result":{"origin":"Pending","batch":"Pending"}},
            {"op":"origin-head","at_us":34300,"duration_us":10000,"result":"ok"},
            {"op":"origin-body","at_us":44300,"duration_us":2000,"result":null},
            {"op":"collect","at_us":46300,"duration_us":10000,"result":"ok"}
        ]});
        let mut origin = Vec::new();
        for (index, role) in ["origin", "a", "b", "c"].into_iter().enumerate() {
            for (op, at) in [
                ("request", 5000 + index * 1000),
                ("waiting", 6000 + index * 1000),
            ] {
                origin.push(json!({"op":op,"role":role,"at_us":at}));
            }
        }
        origin.push(json!({"op":"client-prefix-observed","role":"control","at_us":40000}));
        origin.push(
            json!({"op":"release","role":"control","at_us":40100,"prefix_observed_us":40000}),
        );
        for (index, role) in ["origin", "a", "b", "c"].into_iter().enumerate() {
            origin.push(json!({"op":"head","role":role,"at_us":41100+index*100,"write_started_us":41000+index*100}));
            origin.push(json!({"op":"chunk","role":role,"at_us":42100+index*100,"write_started_us":42000+index*100,"bytes":[if index==0{79}else{64+index}]}));
            origin.push(
                json!({"op":"closed","role":role,"at_us":43000+index*100,"reason":"complete"}),
            );
        }
        origin.push(
            json!({"op":"response-returned","role":"control","at_us":60000,"observer":"harness"}),
        );
        for entry in &mut origin {
            entry["token"] = json!("handoff");
            entry["case"] = json!("response-handoff");
        }
        (report, json!(origin))
    }

    #[test]
    fn response_handoff_requires_pending_original_work_and_post_prefix_success() {
        let (report, origin) = handoff_sample();
        validate(AXUM, "response-handoff", "handoff", &report, &origin).unwrap();
        for path in [
            "callback_scope_closed",
            "body_scope_live",
            "origin_streamed",
        ] {
            let mut changed = report.clone();
            changed["handoff"][path] = json!(false);
            assert!(
                validate(AXUM, "response-handoff", "handoff", &changed, &origin).is_err(),
                "{path}"
            );
        }
        for slot in 0..3 {
            let mut changed = report.clone();
            changed["slots"][slot] = json!("Unresolved");
            assert!(validate(AXUM, "response-handoff", "handoff", &changed, &origin).is_err());
            let mut changed = report.clone();
            changed["items"][slot]["slot_error"] = json!(true);
            assert!(validate(AXUM, "response-handoff", "handoff", &changed, &origin).is_err());
        }
    }

    #[test]
    fn handoff_requires_causal_on_time_arrivals_but_not_on_time_collection() {
        let (report, origin) = handoff_sample();
        let mut changed = report.clone();
        for item in changed["items"].as_array_mut().unwrap() {
            item["elapsed_us"] = json!(0);
        }
        assert!(validate(AXUM, "response-handoff", "handoff", &changed, &origin).is_err());
        let mut late = report.clone();
        late["events"][5]["at_us"] = json!(800000);
        validate(AXUM, "response-handoff", "handoff", &late, &origin).unwrap();
        late["items"][2]["elapsed_us"] = json!(499999);
        validate(AXUM, "response-handoff", "handoff", &late, &origin).unwrap();
        late["events"][1]["at_us"] = json!(34001);
        late["events"][2]["at_us"] = json!(34101);
        assert!(validate(AXUM, "response-handoff", "handoff", &late, &origin).is_err());
    }

    #[test]
    fn handoff_prefix_errors_never_open_the_gate_and_dispatch_order_is_not_inferred_late() {
        let origins = WireOrigins::start().unwrap();
        origins
            .register_lifetime("handoff", "response-handoff")
            .unwrap();
        origins.mark_lifetime_start("handoff").unwrap();
        let fields = vec![json!(["Content-Type", "application/x-ndjson"])];
        let mut valid = serde_json::to_vec(&handoff_prelude("handoff", Some(true))).unwrap();
        valid.push(b'\n');
        for field in [
            "origin_pending",
            "batch_pending",
            "callback_scope_closed",
            "body_scope_live",
            "dispatch_returned",
        ] {
            let mut altered = handoff_prelude("handoff", Some(true));
            altered[field] = json!(false);
            let mut bytes = serde_json::to_vec(&altered).unwrap();
            bytes.push(b'\n');
            assert!(
                observe_handoff_prefix(&origins, "cloudflare", "handoff", 200, &fields, &bytes)
                    .is_err(),
                "{field}"
            );
        }
        for (key, value) in [
            ("token", json!("foreign")),
            ("case", json!("slow-origin")),
            ("schema", json!(2)),
            ("slots", json!(2)),
            ("cutoff_at_us", json!(535000)),
            ("unexpected", json!(true)),
        ] {
            let mut altered = handoff_prelude("handoff", Some(true));
            altered[key] = value;
            let mut bytes = serde_json::to_vec(&altered).unwrap();
            bytes.push(b'\n');
            assert!(
                observe_handoff_prefix(&origins, "cloudflare", "handoff", 200, &fields, &bytes)
                    .is_err()
            );
        }
        // Duplicate keys intentionally cannot be expressed by json!.
        let duplicated = format!(
            "{{\"schema\":1,{}",
            std::str::from_utf8(&valid[1..]).unwrap()
        );
        for bytes in [
            &valid[..valid.len() - 1],
            duplicated.as_bytes(),
            b"{}\n",
            b"not json\n",
        ] {
            assert!(
                observe_handoff_prefix(&origins, "cloudflare", "handoff", 200, &fields, bytes)
                    .is_err()
            );
        }
        for headers in [
            vec![],
            vec![json!(["content-type", "application/json"])],
            vec![fields[0].clone(), fields[0].clone()],
        ] {
            assert!(
                observe_handoff_prefix(&origins, "cloudflare", "handoff", 200, &headers, &valid)
                    .is_err()
            );
        }
        assert!(
            observe_handoff_prefix(&origins, "cloudflare", "handoff", 503, &fields, &valid)
                .is_err()
        );
        assert_eq!(origins.take_lifetime_events("handoff").unwrap(), json!([]));
        observe_handoff_prefix(&origins, "cloudflare", "handoff", 200, &fields, &valid).unwrap();
        assert_eq!(
            origins
                .take_lifetime_events("handoff")
                .unwrap()
                .as_array()
                .unwrap()
                .len(),
            2
        );
        assert!(
            observe_handoff_prefix(&origins, "cloudflare", "handoff", 200, &fields, &valid)
                .is_err()
        );
        origins.finish().unwrap();
        let (mut report, _) = handoff_sample();
        assert!(validate_handoff_dispatch("cloudflare", &report).is_err());
        report["handoff"]["dispatch_returned"] = json!(true);
        validate_handoff_dispatch("cloudflare", &report).unwrap();
        validate_handoff_dispatch("spin", &report).unwrap();
        assert!(validate_handoff_dispatch("axum", &report).is_err());
        report["handoff"]
            .as_object_mut()
            .unwrap()
            .remove("dispatch_returned");
        assert!(validate_handoff_dispatch("axum", &report).is_err());
        assert!(validate_handoff_dispatch("spin", &report).is_err());
    }

    #[test]
    fn handoff_source_witness_rejects_early_reordered_missing_or_failed_phases() {
        let (report, origin) = handoff_sample();
        for index in 0..origin.as_array().unwrap().len() {
            let mut changed = origin.clone();
            changed.as_array_mut().unwrap().remove(index);
            assert!(
                validate(AXUM, "response-handoff", "handoff", &report, &changed).is_err(),
                "missing {index}"
            );
        }
        for role in ["origin", "a", "b", "c"] {
            for op in ["head", "chunk"] {
                let mut changed = origin.clone();
                let entry = changed
                    .as_array_mut()
                    .unwrap()
                    .iter_mut()
                    .find(|entry| entry["role"] == role && entry["op"] == op)
                    .unwrap();
                entry["write_started_us"] = json!(40099);
                assert!(
                    validate(AXUM, "response-handoff", "handoff", &report, &changed).is_err(),
                    "early {role} {op}"
                );
            }
            let mut changed = origin.clone();
            let entry = changed
                .as_array_mut()
                .unwrap()
                .iter_mut()
                .find(|entry| entry["role"] == role && entry["op"] == "closed")
                .unwrap();
            entry["reason"] = json!("peer_closed_after_cutoff");
            assert!(validate(AXUM, "response-handoff", "handoff", &report, &changed).is_err());
        }
        let mut tied = origin.clone();
        for entry in tied
            .as_array_mut()
            .unwrap()
            .iter_mut()
            .filter(|entry| entry["op"] != "request" && entry["op"] != "waiting")
        {
            entry["at_us"] = json!(40000);
            if entry.get("write_started_us").is_some() {
                entry["write_started_us"] = json!(40000);
            }
        }
        validate(AXUM, "response-handoff", "handoff", &report, &tied).unwrap();
        tied.as_array_mut().unwrap().swap(8, 9);
        assert!(validate(AXUM, "response-handoff", "handoff", &report, &tied).is_err());
        let mut late = origin.clone();
        for entry in late
            .as_array_mut()
            .unwrap()
            .iter_mut()
            .filter(|entry| matches!(entry["op"].as_str(), Some("request" | "waiting")))
        {
            entry["at_us"] = json!(40500);
        }
        validate(AXUM, "response-handoff", "handoff", &report, &late).unwrap();
        let mut cleanup = origin.clone();
        for entry in cleanup
            .as_array_mut()
            .unwrap()
            .iter_mut()
            .filter(|entry| entry["op"] == "closed")
        {
            entry["at_us"] = json!(61000);
        }
        validate(AXUM, "response-handoff", "handoff", &report, &cleanup).unwrap();
    }

    #[test]
    fn handoff_ndjson_report_has_two_bounded_records_and_no_silent_truncation() {
        let (mut report, _) = handoff_sample();
        let mut prefix = serde_json::to_vec(&handoff_prelude("handoff", None)).unwrap();
        prefix.push(b'\n');
        let encode = |report: &Value| {
            let mut bytes = prefix.clone();
            bytes.extend(serde_json::to_vec(report).unwrap());
            bytes.push(b'\n');
            bytes
        };
        assert_eq!(
            decode_report("axum", "response-handoff", "handoff", &encode(&report)).unwrap(),
            report
        );
        assert!(decode_report("spin", "response-handoff", "handoff", &encode(&report)).is_err());
        let normal = encode(&report);
        for bytes in [&normal[..normal.len() - 1], &prefix, b"{}\n"] {
            assert!(decode_report("axum", "response-handoff", "handoff", bytes).is_err());
        }
        let mut extra = normal.clone();
        extra.extend(b"{}\n");
        assert!(decode_report("axum", "response-handoff", "handoff", &extra).is_err());
        report["padding"] = json!("");
        let base = serde_json::to_vec(&report).unwrap().len();
        report["padding"] = json!("x".repeat(16 * 1024 - 1 - base));
        decode_report("axum", "response-handoff", "handoff", &encode(&report)).unwrap();
        report["padding"] = json!("x".repeat(16 * 1024 - base));
        assert!(decode_report("axum", "response-handoff", "handoff", &encode(&report)).is_err());
    }

    fn caps_sample() -> (Value, Value) {
        let report = json!({"schema":1,"case":"caps-while-waiting","token":"sample","qualification":false,
            "events":[{"op":"batch-start","at_us":34000,"duration_us":100,"result":{"cutoff_at_us":534000}},
            {"op":"collect","at_us":34100,"duration_us":175900,"result":"ok"}],
            "slots":["Resolved","Resolved","Resolved"],"termination":"Completed","error":null,"origin":null,"mediator":null,
            "items":[{"index":0,"via":"collect","elapsed_us":174000,"slot_error":true,"outcome":{"error":{"kind":"response_too_large","status":502,"reason":"BufferedBody"}}},
            {"index":1,"via":"collect","elapsed_us":20000,"slot_error":false,"outcome":{"status":200,"bytes":[66]}},
            {"index":2,"via":"collect","elapsed_us":40000,"slot_error":false,"outcome":{"status":200,"bytes":[67]}}]});
        let mut origin = vec![
            json!({"op":"request","role":"a","at_us":1000,"guest_at_us":34000}),
            json!({"op":"head","role":"a","at_us":2000,"write_started_us":1990,"body_start_us":2000,
            "content_encoding":"gzip","content_length":131200,"decoded_bytes":131072,
            "piece_ends":[16400,32800,49200,65600,82000,98400,114800,131200]}),
        ];
        for index in 0..6 {
            let scheduled = 2000 + index * 300000 / 7;
            let written = if index == 5 { 0 } else { 16400 };
            origin.push(json!({"op":"chunk","role":"a","index":index,"scheduled_us":scheduled,
                "write_started_us":scheduled,"at_us":scheduled+1,"schedule_basis":"body-start","attempted_bytes":16400,"written_bytes":written,
                "encoded_end":if index==5 {82000}else{(index+1)*16400},
                "decoded_prefix_bytes":((index+1)*16384).min(65537),
                "io_error":if index==5 {Some("BrokenPipe")}else{None}}));
        }
        origin.push(json!({"op":"closed","role":"a","at_us":216287,"reason":"peer_closed_after_cap_prefix"}));
        for (index, role) in [(1, "b"), (2, "c")] {
            let request = 1000 + index * 10;
            let at = request + index * 20000;
            origin.extend([json!({"op":"request","role":role,"at_us":request,"guest_at_us":34000}),
                json!({"op":"head","role":role,"at_us":at,"write_started_us":at,"scheduled_us":at}),
                json!({"op":"chunk","role":role,"at_us":at+1,"write_started_us":at+1,"scheduled_us":at,"bytes":[65+index]}),
                json!({"op":"closed","role":role,"at_us":at+2,"reason":"complete"})]);
        }
        origin.push(
            json!({"op":"response-returned","role":"control","at_us":200000,"observer":"harness"}),
        );
        for entry in &mut origin {
            entry["token"] = json!("sample");
            entry["case"] = json!("caps-while-waiting");
        }
        (report, json!(origin))
    }

    #[test]
    fn caps_evidence_requires_the_raw_slot_error_before_source_completion() {
        let (report, origin) = caps_sample();
        validate(AXUM, "caps-while-waiting", "sample", &report, &origin).unwrap();
        for (field, value) in [
            ("slot_error", json!(false)),
            ("slot_error", Value::Null),
            ("via", json!("next")),
        ] {
            let mut changed = report.clone();
            changed["items"][0][field] = value;
            assert!(validate(AXUM, "caps-while-waiting", "sample", &changed, &origin).is_err());
        }
        for reason in ["DecodedBody", "EncodedBody", "Unspecified"] {
            let mut changed = report.clone();
            changed["items"][0]["outcome"]["error"]["reason"] = json!(reason);
            assert!(validate(AXUM, "caps-while-waiting", "sample", &changed, &origin).is_err());
        }
    }

    #[test]
    fn caps_siblings_must_write_before_response_return_but_may_close_later() {
        let (report, origin) = caps_sample();
        for role in ["b", "c"] {
            let mut impossible = origin.clone();
            for entry in impossible
                .as_array_mut()
                .unwrap()
                .iter_mut()
                .filter(|entry| entry["role"] == role)
            {
                for field in ["at_us", "write_started_us", "scheduled_us"] {
                    if let Some(at) = entry[field].as_u64() {
                        entry[field] = json!(at + 250000);
                    }
                }
            }
            assert!(
                validate(AXUM, "caps-while-waiting", "sample", &report, &impossible).is_err(),
                "{role}"
            );
            let mut late_close = origin.clone();
            let closed = late_close
                .as_array_mut()
                .unwrap()
                .iter_mut()
                .find(|entry| entry["role"] == role && entry["op"] == "closed")
                .unwrap();
            closed["at_us"] = json!(300000);
            validate(AXUM, "caps-while-waiting", "sample", &report, &late_close).unwrap();
        }
    }

    #[test]
    fn caps_evidence_rejects_bunched_source_writes() {
        let (report, origin) = caps_sample();
        let mut bunched = origin.clone();
        for entry in bunched.as_array_mut().unwrap().iter_mut().filter(|entry| {
            entry["role"] == "a" && entry["op"] == "chunk" && entry["index"].as_u64().unwrap() < 4
        }) {
            let at = 150000 + entry["index"].as_u64().unwrap();
            entry["write_started_us"] = json!(at);
            entry["at_us"] = json!(at + 1);
        }
        assert!(validate(AXUM, "caps-while-waiting", "sample", &report, &bunched).is_err());
        let mut late = origin.clone();
        for entry in late
            .as_array_mut()
            .unwrap()
            .iter_mut()
            .filter(|entry| entry["role"] == "b" && entry["op"] != "request")
        {
            for field in ["at_us", "write_started_us"] {
                if let Some(at) = entry[field].as_u64() {
                    entry[field] = json!(at + 10000);
                }
            }
        }
        assert!(validate(AXUM, "caps-while-waiting", "sample", &report, &late).is_err());
    }

    #[test]
    fn caps_evidence_rejects_impossible_arrival_times() {
        let (report, origin) = caps_sample();
        let mut impossible = report.clone();
        impossible["items"][0]["elapsed_us"] = json!(499999);
        assert!(validate(AXUM, "caps-while-waiting", "sample", &impossible, &origin).is_err());
        let mut possible = report.clone();
        possible["items"][0]["elapsed_us"] = json!(175950);
        validate(AXUM, "caps-while-waiting", "sample", &possible, &origin).unwrap();
    }

    #[test]
    fn caps_evidence_rejects_wrong_results_and_unrelated_partial_write_errors() {
        let (report, origin) = caps_sample();
        for (path, value) in [
            ("/items/0/outcome/error/status", json!(504)),
            ("/items/0/outcome/error/kind", json!("gateway_timeout")),
            ("/items/0/index", json!(1)),
            ("/slots/0", json!("Unresolved")),
            ("/termination", json!("Cutoff")),
        ] {
            let mut changed = report.clone();
            *changed.pointer_mut(path).unwrap() = value;
            assert!(
                validate(AXUM, "caps-while-waiting", "sample", &changed, &origin).is_err(),
                "{path}"
            );
        }
        for (field, value) in [
            ("io_error", json!("TimedOut")),
            ("io_error", Value::Null),
            ("written_bytes", json!(16401)),
            ("encoded_end", json!(1)),
            ("schedule_basis", json!("request-receipt")),
            ("decoded_prefix_bytes", json!(65536)),
        ] {
            let mut changed = origin.clone();
            let piece = changed
                .as_array_mut()
                .unwrap()
                .iter_mut()
                .find(|entry| entry["role"] == "a" && entry["index"] == 5)
                .unwrap();
            piece[field] = value;
            assert!(
                validate(AXUM, "caps-while-waiting", "sample", &report, &changed).is_err(),
                "{field}"
            );
        }
        let mut unproven = origin.clone();
        let returned = unproven
            .as_array_mut()
            .unwrap()
            .iter_mut()
            .find(|entry| entry["op"] == "response-returned")
            .unwrap();
        returned["at_us"] = json!(173428);
        let error = validate(AXUM, "caps-while-waiting", "sample", &report, &unproven).unwrap_err();
        assert!(error.to_string().contains("contradictory shared epochs"));
        let mut early = origin.clone();
        let head = early
            .as_array_mut()
            .unwrap()
            .iter_mut()
            .find(|entry| entry["role"] == "a" && entry["op"] == "head")
            .unwrap();
        head["content_encoding"] = json!("identity");
        assert!(validate(AXUM, "caps-while-waiting", "sample", &report, &early).is_err());
    }

    #[test]
    fn caps_evidence_rejects_missing_foreign_and_unproven_source_boundaries() {
        let (report, origin) = caps_sample();
        for field in ["observer", "token", "case"] {
            let mut changed = origin.clone();
            changed.as_array_mut().unwrap().last_mut().unwrap()[field] = json!("foreign");
            assert!(validate(AXUM, "caps-while-waiting", "sample", &report, &changed).is_err());
        }
        let mut missing = origin.clone();
        missing.as_array_mut().unwrap().pop();
        assert!(validate(AXUM, "caps-while-waiting", "sample", &report, &missing).is_err());
        let mut duplicate = origin.clone();
        duplicate
            .as_array_mut()
            .unwrap()
            .push(origin.as_array().unwrap().last().unwrap().clone());
        assert!(validate(AXUM, "caps-while-waiting", "sample", &report, &duplicate).is_err());
        let mut premature = origin.clone();
        for entry in premature
            .as_array_mut()
            .unwrap()
            .iter_mut()
            .filter(|entry| entry["op"] == "chunk" && entry["role"] == "a")
        {
            entry["decoded_prefix_bytes"] = json!(65536);
        }
        assert!(validate(AXUM, "caps-while-waiting", "sample", &report, &premature).is_err());
    }

    #[test]
    fn caps_slot_time_is_not_the_native_diagnostic_response_time() {
        let (report, origin) = caps_sample();
        for at in [302000, 400000] {
            let mut changed = origin.clone();
            changed.as_array_mut().unwrap().last_mut().unwrap()["at_us"] = json!(at);
            validate(AXUM, "caps-while-waiting", "sample", &report, &changed).unwrap();
        }
    }

    #[test]
    fn caps_timing_rejects_early_errors_and_incompatible_sibling_epochs() {
        let (report, origin) = caps_sample();
        for (index, elapsed) in [(0, 50000), (1, 5000)] {
            let mut changed = report.clone();
            changed["items"][index]["elapsed_us"] = json!(elapsed);
            assert!(validate(AXUM, "caps-while-waiting", "sample", &changed, &origin).is_err());
        }
    }

    #[test]
    fn caps_timing_requires_each_original_request_stamp() {
        let (report, origin) = caps_sample();
        for role in ["a", "b", "c"] {
            for stamp in [Value::Null, json!(35000), json!(u64::MAX)] {
                let mut changed = origin.clone();
                changed
                    .as_array_mut()
                    .unwrap()
                    .iter_mut()
                    .find(|entry| entry["role"] == role && entry["op"] == "request")
                    .unwrap()["guest_at_us"] = stamp;
                assert!(validate(AXUM, "caps-while-waiting", "sample", &report, &changed).is_err());
            }
        }
    }

    #[test]
    fn caps_timing_rejects_equality_overlap_and_late_slot_observation() {
        let (report, mut origin) = caps_sample();
        origin.as_array_mut().unwrap().last_mut().unwrap()["at_us"] = json!(500000);
        for (elapsed, passes) in [
            (299898, true),
            (299899, false),
            (299900, false),
            (340000, false),
        ] {
            let mut changed = report.clone();
            changed["events"][1]["duration_us"] = json!(400000);
            changed["items"][0]["elapsed_us"] = json!(elapsed);
            assert_eq!(
                validate(AXUM, "caps-while-waiting", "sample", &changed, &origin).is_ok(),
                passes,
                "elapsed={elapsed}"
            );
        }
    }

    #[test]
    fn caps_timing_is_independent_of_the_native_clock_origin() {
        let (report, mut origin) = caps_sample();
        for entry in origin.as_array_mut().unwrap() {
            for field in ["at_us", "write_started_us", "body_start_us", "scheduled_us"] {
                if let Some(at) = entry[field].as_u64() {
                    entry[field] = json!(at + 500000);
                }
            }
        }
        validate(AXUM, "caps-while-waiting", "sample", &report, &origin).unwrap();
    }

    #[test]
    fn caps_timing_uses_the_tightest_original_receipt_across_all_slots() {
        let (report, origin) = caps_sample();
        for (controlling, expected) in [("a", -32000), ("b", -31990), ("c", -31980)] {
            let mut changed = origin.clone();
            for entry in changed
                .as_array_mut()
                .unwrap()
                .iter_mut()
                .filter(|entry| entry["op"] == "request" && entry["role"] != controlling)
            {
                entry["guest_at_us"] = json!(33000);
            }
            let proof = caps_clock_bounds(&report, &changed).unwrap();
            assert_eq!(proof.offset_upper, expected);
            validate(AXUM, "caps-while-waiting", "sample", &report, &changed).unwrap();
        }
    }

    #[test]
    fn caps_timing_requires_collection_to_precede_the_diagnostic_response() {
        let (mut report, mut origin) = caps_sample();
        report["events"][1]["duration_us"] = json!(400000);
        assert!(validate(AXUM, "caps-while-waiting", "sample", &report, &origin).is_err());
        origin.as_array_mut().unwrap().last_mut().unwrap()["at_us"] = json!(500000);
        validate(AXUM, "caps-while-waiting", "sample", &report, &origin).unwrap();
    }

    #[test]
    fn caps_timing_extremes_fail_without_arithmetic_panics() {
        let (report, origin) = caps_sample();
        for index in [0, 1] {
            let mut changed = report.clone();
            changed["events"][index]["duration_us"] = json!(u64::MAX);
            let rejected = std::panic::catch_unwind(|| {
                validate(AXUM, "caps-while-waiting", "sample", &changed, &origin)
            });
            assert!(rejected.expect("malformed timing must not panic").is_err());
        }
    }

    fn cpu_sample(delivered: bool) -> (Value, Value) {
        let mut report = json!({"schema":1,"case":"cpu-stretch","token":"sample","qualification":false,
            "events":[
                {"op":"begin","at_us":2000,"duration_us":100,"result":"scheduled"},
                {"op":"batch-start","at_us":34000,"duration_us":100,"result":{"cutoff_at_us":534000}},
                {"op":"origin-head","at_us":34100,"duration_us":66400,"result":"ok"},
                {"op":"origin-body","at_us":100500,"duration_us":100,"result":null},
                {"op":"calibration-send","at_us":100600,"duration_us":352400,
                    "result":{"origin_head_observed_us":100500,"outcome":{"status":204,"bytes":[]}}},
                {"op":"next","at_us":500000,"duration_us":100,"result":"Item"},
                {"op":"cpu-stretch","at_us":520000,"duration_us":40000,
                    "result":{"iterations":4000,"max_iterations":10000000}},
                {"op":"collect","at_us":560000,"duration_us":100,"result":"ok"}
            ],"items":[{"index":0,"via":"next","elapsed_us":146000,"outcome":{"status":200,"bytes":[65]}}],
            "slots":["AlreadyDelivered","Unresolved","Unresolved"],"termination":"Cutoff",
            "error":null,"origin":{"status":200,"bytes":[79]},"mediator":null});
        if delivered {
            report["items"]
                .as_array_mut()
                .unwrap()
                .push(json!({"index":1,"via":"collect","elapsed_us":496000,
                "outcome":{"status":200,"bytes":[66]}}));
            report["slots"][1] = json!("Resolved");
        }
        let mut origin = json!([
            {"op":"request","role":"origin","at_us":3000,"guest_at_us":2000},
            {"op":"request","role":"a","at_us":35000,"guest_at_us":34000},
            {"op":"request","role":"b","at_us":35010,"guest_at_us":34000},
            {"op":"request","role":"c","at_us":35020,"guest_at_us":34000},
            {"op":"request","role":"calibration","at_us":101600,"guest_at_us":100600,"origin_head_observed_us":100500},
            {"op":"head","role":"origin","at_us":100100,"write_started_us":100000},
            {"op":"chunk","role":"origin","at_us":100200,"write_started_us":100150,"bytes":[79]},
            {"op":"head","role":"a","at_us":180100,"write_started_us":180000},
            {"op":"chunk","role":"a","at_us":180200,"write_started_us":180150,"bytes":[65]},
            {"op":"head","role":"calibration","at_us":452100,"write_started_us":452000},
            {"op":"closed","role":"calibration","at_us":452300,"reason":"complete"},
            {"op":"head","role":"b","at_us":530100,"write_started_us":530000},
            {"op":"chunk","role":"b","at_us":530200,"write_started_us":530150,"bytes":[66]},
            {"op":"closed","role":"origin","at_us":100300,"reason":"complete"},
            {"op":"closed","role":"a","at_us":180300,"reason":"complete"},
            {"op":"closed","role":"b","at_us":530300,"reason":"complete"},
            {"op":"closed","role":"c","at_us":900300,"reason":"peer_closed_after_cutoff"}
        ]);
        for entry in origin.as_array_mut().unwrap() {
            entry["case"] = json!("cpu-stretch");
            entry["token"] = json!("sample");
        }
        (report, origin)
    }

    #[test]
    fn cpu_stretch_rejects_arrivals_before_original_payload_production() {
        for (role, payload_start, payload_end, closed_at) in
            [("a", 183400, 183500, 183600), ("b", 533400, 533500, 533600)]
        {
            let (report, mut origin) = cpu_sample(true);
            for entry in origin
                .as_array_mut()
                .unwrap()
                .iter_mut()
                .filter(|e| e["role"] == role)
            {
                if entry["op"] == "chunk" {
                    entry["write_started_us"] = json!(payload_start);
                    entry["at_us"] = json!(payload_end);
                } else if entry["op"] == "closed" {
                    entry["at_us"] = json!(closed_at);
                }
            }
            assert!(
                validate(AXUM, "cpu-stretch", "sample", &report, &origin).is_err(),
                "{role} cannot arrive before its bytes exist under any allowed offset"
            );
        }
    }

    #[test]
    fn cpu_stretch_rejects_payload_starts_outside_their_write_phase() {
        for start in [None, Some(530050), Some(530201)] {
            let (report, mut origin) = cpu_sample(true);
            let chunk = origin
                .as_array_mut()
                .unwrap()
                .iter_mut()
                .find(|e| e["role"] == "b" && e["op"] == "chunk")
                .unwrap();
            if let Some(start) = start {
                chunk["write_started_us"] = json!(start);
            } else {
                chunk.as_object_mut().unwrap().remove("write_started_us");
            }
            assert!(
                validate(AXUM, "cpu-stretch", "sample", &report, &origin).is_err(),
                "payload must start after head completion and before its completion"
            );
        }
    }

    #[test]
    fn cpu_stretch_accepts_on_time_or_unresolved_but_never_collection_stamped_b() {
        for delivered in [false, true] {
            let (report, origin) = cpu_sample(delivered);
            validate(AXUM, "cpu-stretch", "sample", &report, &origin).unwrap();
        }
    }

    #[test]
    fn cpu_stretch_rejects_late_or_foreign_slot_results() {
        for (path, value) in [
            ("/items/0/elapsed_us", json!(0)),
            ("/items/0/elapsed_us", json!(466000)),
            ("/items/1/elapsed_us", json!(500000)),
            ("/items/1/elapsed_us", json!(501000)),
            ("/items/1/elapsed_us", json!(526000)),
            ("/items/1/index", json!(2)),
            ("/slots/2", json!("Resolved")),
            ("/termination", json!("Completed")),
            ("/events/1/result/cutoff_at_us", json!(1060000)),
            ("/events/6/result/iterations", json!(0)),
            ("/events/6/at_us", json!(519999)),
            ("/events/6/duration_us", json!(39999)),
        ] {
            let (mut report, origin) = cpu_sample(true);
            *report.pointer_mut(path).unwrap() = value;
            assert!(
                validate(AXUM, "cpu-stretch", "sample", &report, &origin).is_err(),
                "{path}"
            );
        }
    }

    #[test]
    fn cpu_stretch_rejects_unproven_actual_write_overlap_and_clock_bounds() {
        for (op, field, at) in [
            ("head", "write_started_us", 510000),
            ("head", "write_started_us", 550000),
            ("chunk", "at_us", 570000),
            ("chunk", "at_us", 539000),
        ] {
            let (report, mut origin) = cpu_sample(false);
            let entry = origin
                .as_array_mut()
                .unwrap()
                .iter_mut()
                .find(|entry| entry["role"] == "b" && entry["op"] == op)
                .unwrap();
            entry[field] = json!(at);
            assert!(validate(AXUM, "cpu-stretch", "sample", &report, &origin).is_err());
        }
        let (mut report, origin) = cpu_sample(false);
        report["events"][2]["duration_us"] = json!(100000);
        assert!(validate(AXUM, "cpu-stretch", "sample", &report, &origin).is_err());
    }

    #[test]
    fn cpu_stretch_uses_causal_intervals_not_a_selected_clock_alignment() {
        for delivered in [false, true] {
            let (report, mut origin) = cpu_sample(delivered);
            for entry in origin.as_array_mut().unwrap() {
                for field in ["at_us", "write_started_us"] {
                    if let Some(at) = entry[field].as_u64() {
                        entry[field] = json!(at + 1_000_000);
                    }
                }
            }
            validate(AXUM, "cpu-stretch", "sample", &report, &origin).unwrap();
        }
        for field in ["guest_at_us", "write_started_us"] {
            let (report, mut origin) = cpu_sample(false);
            let op = if field == "guest_at_us" {
                "request"
            } else {
                "head"
            };
            let entry = origin
                .as_array_mut()
                .unwrap()
                .iter_mut()
                .find(|entry| entry["role"] == "origin" && entry["op"] == op)
                .unwrap();
            entry.as_object_mut().unwrap().remove(field);
            assert!(validate(AXUM, "cpu-stretch", "sample", &report, &origin).is_err());
        }
        let (mut report, origin) = cpu_sample(false);
        report["events"][2]["duration_us"] = json!(0);
        assert!(validate(AXUM, "cpu-stretch", "sample", &report, &origin).is_err());
    }

    #[test]
    fn cpu_stretch_rejects_absolute_arrivals_after_cutoff_or_prior_delivery() {
        let (mut report, origin) = cpu_sample(true);
        report["events"][1]["at_us"] = json!(39000);
        report["events"][2]["at_us"] = json!(39100);
        report["events"][2]["duration_us"] = json!(61400);
        assert!(validate(AXUM, "cpu-stretch", "sample", &report, &origin).is_err());
        for delivered in [false, true] {
            let (mut report, origin) = cpu_sample(delivered);
            report["items"][0]["elapsed_us"] = json!(499000);
            assert!(validate(AXUM, "cpu-stretch", "sample", &report, &origin).is_err());
        }
    }

    #[test]
    fn cpu_stretch_bounds_receipt_epoch_by_the_entire_batch_call() {
        for (duration, accepted) in [(3999, true), (4000, false)] {
            let (mut report, origin) = cpu_sample(true);
            report["events"][1]["duration_us"] = json!(duration);
            report["events"][2]["at_us"] = json!(34000 + duration);
            report["events"][2]["duration_us"] = json!(66500 - duration);
            assert_eq!(
                validate(AXUM, "cpu-stretch", "sample", &report, &origin).is_ok(),
                accepted,
                "batch invocation duration {duration}"
            );
        }
    }

    #[test]
    fn cpu_stretch_rejects_early_c_writes_hidden_by_a_late_close() {
        for (reason, chunk) in [
            ("complete", true),
            ("peer_closed_after_cutoff", false),
            ("peer_closed_after_cutoff", true),
        ] {
            let (report, mut origin) = cpu_sample(false);
            let entries = origin.as_array_mut().unwrap();
            let index = entries
                .iter()
                .position(|entry| entry["role"] == "c" && entry["op"] == "closed")
                .unwrap();
            entries[index]["reason"] = json!(reason);
            if chunk {
                entries.insert(index, json!({"token":"sample","case":"cpu-stretch","role":"c","op":"chunk","at_us":400200,"write_started_us":400150,"bytes":[67]}));
            }
            entries.insert(index, json!({"token":"sample","case":"cpu-stretch","role":"c","op":"head","at_us":400100,"write_started_us":400000}));
            assert!(validate(AXUM, "cpu-stretch", "sample", &report, &origin).is_err());
        }
    }

    #[test]
    fn cpu_stretch_accepts_late_complete_or_partially_written_c() {
        for complete in [false, true] {
            let (report, mut origin) = cpu_sample(false);
            let entries = origin.as_array_mut().unwrap();
            let index = entries
                .iter()
                .position(|entry| entry["role"] == "c" && entry["op"] == "closed")
                .unwrap();
            if complete {
                entries[index]["reason"] = json!("complete");
                entries.insert(index, json!({"token":"sample","case":"cpu-stretch","role":"c","op":"chunk","at_us":900200,"write_started_us":900150,"bytes":[67]}));
            }
            entries.insert(index, json!({"token":"sample","case":"cpu-stretch","role":"c","op":"head","at_us":900100,"write_started_us":900000}));
            validate(AXUM, "cpu-stretch", "sample", &report, &origin).unwrap();
        }
    }

    #[test]
    fn cpu_stretch_requires_every_offset_not_a_convenient_midpoint() {
        let (report, mut origin) = cpu_sample(false);
        for entry in origin
            .as_array_mut()
            .unwrap()
            .iter_mut()
            .filter(|entry| entry["role"] == "b")
        {
            match entry["op"].as_str().unwrap() {
                "head" => {
                    entry["write_started_us"] = json!(527000);
                    entry["at_us"] = json!(527100);
                }
                "chunk" => entry["at_us"] = json!(533500),
                "closed" => entry["at_us"] = json!(533600),
                _ => {}
            }
        }
        validate(AXUM, "cpu-stretch", "sample", &report, &origin).unwrap();
        for (op, field, at) in [
            ("chunk", "at_us", 533501),
            ("head", "write_started_us", 526999),
        ] {
            let mut changed = origin.clone();
            let entry = changed
                .as_array_mut()
                .unwrap()
                .iter_mut()
                .find(|entry| entry["role"] == "b" && entry["op"] == op)
                .unwrap();
            entry[field] = json!(at);
            assert!(validate(AXUM, "cpu-stretch", "sample", &report, &changed).is_err());
        }
    }

    #[test]
    fn cpu_stretch_accepts_tolerated_write_uncertainty_but_not_a_late_receipt() {
        let (mut report, mut origin) = cpu_sample(false);
        report["events"][2]["duration_us"] = json!(63900);
        report["events"][4]["result"]["origin_head_observed_us"] = json!(98000);
        origin
            .as_array_mut()
            .unwrap()
            .iter_mut()
            .find(|entry| entry["role"] == "calibration" && entry["op"] == "request")
            .unwrap()["origin_head_observed_us"] = json!(98000);
        for entry in origin
            .as_array_mut()
            .unwrap()
            .iter_mut()
            .filter(|entry| entry["role"] == "b")
        {
            match entry["op"].as_str().unwrap() {
                "head" => {
                    entry["write_started_us"] = json!(534000);
                    entry["at_us"] = json!(534100);
                }
                "chunk" => {
                    entry["write_started_us"] = json!(534150);
                    entry["at_us"] = json!(535500);
                }
                "closed" => entry["at_us"] = json!(535600),
                _ => {}
            }
        }
        validate(AXUM, "cpu-stretch", "sample", &report, &origin).unwrap();
        report["items"]
            .as_array_mut()
            .unwrap()
            .push(json!({"index":1,"via":"collect","elapsed_us":500000,
            "outcome":{"status":200,"bytes":[66]}}));
        report["slots"][1] = json!("Resolved");
        assert!(validate(AXUM, "cpu-stretch", "sample", &report, &origin).is_err());
    }

    #[test]
    fn cpu_calibration_rejects_missing_duplicate_mismatched_or_early_evidence() {
        for mutation in 0..9 {
            let (mut report, mut origin) = cpu_sample(false);
            let entries = origin.as_array_mut().unwrap();
            let index = entries
                .iter()
                .position(|entry| entry["role"] == "calibration" && entry["op"] == "request")
                .unwrap();
            match mutation {
                0 => { entries.remove(index); }
                1 => entries.push(entries[index].clone()),
                2 => entries[index]["origin_head_observed_us"] = json!(100499),
                3 => entries[index]["guest_at_us"] = json!(100599),
                4 => entries[index]["at_us"] = json!(97000),
                5 => report["events"][4]["result"]["origin_head_observed_us"] = json!(100499),
                6 => report["events"][4]["result"]["outcome"]["bytes"] = json!([75]),
                7 => {
                    entries.iter_mut().find(|entry| entry["role"] == "calibration" && entry["op"] == "head").unwrap()["write_started_us"] = json!(451999);
                }
                8 => entries.push(json!({"op":"chunk","role":"calibration","at_us":452200,"token":"sample","case":"cpu-stretch","bytes":[]})),
                _ => unreachable!(),
            }
            assert!(
                validate(AXUM, "cpu-stretch", "sample", &report, &origin).is_err(),
                "mutation {mutation}"
            );
        }
    }

    #[test]
    fn cpu_calibration_tightens_cold_bounds_without_using_a_point_estimate() {
        let (report, mut origin) = cpu_sample(false);
        origin
            .as_array_mut()
            .unwrap()
            .iter_mut()
            .find(|entry| entry["role"] == "origin" && entry["op"] == "request")
            .unwrap()["at_us"] = json!(10000);
        assert_eq!(
            cpu_clock_bounds(report["events"].as_array().unwrap(), &origin).unwrap(),
            [-1500, 2000]
        );
        validate(AXUM, "cpu-stretch", "sample", &report, &origin).unwrap();
        origin
            .as_array_mut()
            .unwrap()
            .iter_mut()
            .find(|entry| entry["role"] == "calibration" && entry["op"] == "request")
            .unwrap()["at_us"] = json!(109600);
        assert_eq!(
            cpu_clock_bounds(report["events"].as_array().unwrap(), &origin).unwrap(),
            [-1500, 9000]
        );
        assert!(validate(AXUM, "cpu-stretch", "sample", &report, &origin).is_err());
    }

    #[test]
    fn cpu_calibration_finishes_before_the_original_observation_target() {
        let (mut report, mut origin) = cpu_sample(false);
        validate(AXUM, "cpu-stretch", "sample", &report, &origin).unwrap();
        report["events"][5]["at_us"] = json!(453000);
        assert!(validate(AXUM, "cpu-stretch", "sample", &report, &origin).is_err());
        report["events"][5]["at_us"] = json!(500000);
        report["events"][4]["at_us"] = json!(200000);
        report["events"][4]["duration_us"] = json!(253000);
        let request = origin
            .as_array_mut()
            .unwrap()
            .iter_mut()
            .find(|entry| entry["role"] == "calibration" && entry["op"] == "request")
            .unwrap();
        request["guest_at_us"] = json!(200000);
        request["at_us"] = json!(201000);
        assert!(validate(AXUM, "cpu-stretch", "sample", &report, &origin).is_err());
    }

    #[test]
    fn cpu_calibration_cannot_reach_the_host_before_the_origin_payload() {
        let (report, mut origin) = cpu_sample(false);
        origin
            .as_array_mut()
            .unwrap()
            .iter_mut()
            .find(|entry| entry["role"] == "calibration" && entry["op"] == "request")
            .unwrap()["at_us"] = json!(100100);
        assert!(validate(AXUM, "cpu-stretch", "sample", &report, &origin).is_err());
    }

    fn pending_cutoff_sample(case: &str) -> (Value, Value) {
        let report = json!({"schema":1,"case":case,"token":"sample","qualification":false,
            "events":[
                {"op":"batch-start","at_us":34000,"duration_us":100,
                    "result":{"cutoff_at_us":200000}},
                {"op":"next","at_us":34100,"duration_us":166100,
                    "result":{"state":"Cutoff","first_poll_us":34110,"pending_polls":1}},
                {"op":"collect","at_us":200200,"duration_us":0,"result":"ok"}
            ],"items":[],"slots":["Unresolved","Unresolved","Unresolved"],"termination":"Cutoff",
            "error":null,"origin":null,"mediator":null,"native_body":native_pending_sample(case)});
        let mut origin = Vec::new();
        for role in ["a", "b", "c"] {
            origin.push(json!({"op":"request","role":role,"at_us":35000,"guest_at_us":34000}));
            if case == "pending-body-cutoff" {
                origin.push(json!({"op":"head","role":role,"at_us":36000,
                    "write_started_us":35900,"content_length":1}));
            }
            origin.push(json!({"op":"waiting","role":role,"at_us":37000}));
        }
        origin.push(json!({"op":"release","role":"control","at_us":210000}));
        origin.last_mut().unwrap()["response_returned_us"] = json!(209000);
        for role in ["a", "b", "c"] {
            origin.push(json!({"op":"closed","role":role,"at_us":211000,
                "reason":"fixture_released"}));
        }
        for event in &mut origin {
            event["token"] = json!("sample");
            event["case"] = json!(case);
        }
        (report, json!(origin))
    }

    fn native_pending_sample(case: &str) -> Value {
        json!(["a", "b", "c"].map(|role| {
            let body = case == "pending-body-cutoff";
            json!({"attachments":1,"preparations":1,"invalid":false,
                "preparation":{"mode":"Batch","cause":"BatchCutoff","deadline_at_us":200000,
                    "entered_at_us":34010,"target_uri":format!("http://127.0.0.1:8080/lifetimes/sample/{case}/{role}")},
                "heads":u64::from(body),"head_at_us":body.then_some(36000),
                "first_read":body.then(||json!({"started_at_us":36100,"observed_at_us":36101,"pending":true})),
                "reads":u64::from(body),"ready_reads":0,
                "source_acquisitions":u64::from(body),"source_releases":u64::from(body),
                "waits":0,"ready_waits":0,"failed_waits":0,
                "first_wait":null,"first_wait_timeout":null,"last_wait_timeout":null,
                "released_at_us":body.then_some(200100)})
        }))
    }

    #[test]
    fn pending_cutoff_cannot_use_host_heads_as_native_sdk_read_evidence() {
        let (mut report, origin) = pending_cutoff_sample("pending-body-cutoff");
        report.as_object_mut().unwrap().remove("native_body");
        for adapter in ["axum", "cloudflare", "spin", "fastly"] {
            assert!(
                validate(
                    AdapterBoundary {
                        name: adapter,
                        ..AXUM
                    },
                    "pending-body-cutoff",
                    "sample",
                    &report,
                    &origin
                )
                .is_err(),
                "{adapter} must require actual SDK read evidence"
            );
        }
    }

    fn fastly_native_wait_sample(case: &str) -> Value {
        let body = case == "pending-body-cutoff";
        let mut snapshots = native_pending_sample(case);
        for snapshot in snapshots.as_array_mut().unwrap() {
            snapshot["first_read"] = Value::Null;
            snapshot["reads"] = json!(0);
            snapshot["waits"] = json!(u64::from(body));
            snapshot["ready_waits"] = json!(0);
            snapshot["failed_waits"] = json!(0);
            let wait = if body {
                json!({"kind":"BodyReadiness","outcome":"TimedOut","started_at_us":37000,"observed_at_us":200050})
            } else {
                Value::Null
            };
            for field in ["first_wait", "first_wait_timeout", "last_wait_timeout"] {
                snapshot[field] = wait.clone();
            }
        }
        snapshots
    }

    #[test]
    fn fastly_native_wait_requires_same_body_timeout_across_original_cutoff() {
        let (mut report, origin) = pending_cutoff_sample("pending-body-cutoff");
        report["native_body"] = fastly_native_wait_sample("pending-body-cutoff");
        let boundary = AdapterBoundary {
            name: "fastly",
            ..AXUM
        };
        validate(boundary, "pending-body-cutoff", "sample", &report, &origin).unwrap();
        let mut mixed = report.clone();
        for snapshot in mixed["native_body"].as_array_mut().unwrap() {
            snapshot["waits"] = json!(2);
            snapshot["first_wait"] = json!({"kind":"BodyReadiness","outcome":"OtherReady",
                "started_at_us":36100,"observed_at_us":36200});
        }
        validate(boundary, "pending-body-cutoff", "sample", &mixed, &origin).unwrap();
        let mut repeated = mixed.clone();
        for snapshot in repeated["native_body"].as_array_mut().unwrap() {
            snapshot["waits"] = json!(3);
            snapshot["first_wait_timeout"]["observed_at_us"] = json!(190000);
            snapshot["last_wait_timeout"]["started_at_us"] = json!(199900);
        }
        validate(
            boundary,
            "pending-body-cutoff",
            "sample",
            &repeated,
            &origin,
        )
        .unwrap();
        repeated["native_body"][0]["waits"] = json!(2);
        assert!(
            validate(
                boundary,
                "pending-body-cutoff",
                "sample",
                &repeated,
                &origin
            )
            .is_err()
        );
        for (field, value) in [
            ("/native_body", Value::Null),
            ("/native_body/0/attachments", json!(0)),
            ("/native_body/0/preparations", json!(2)),
            ("/native_body/0/invalid", json!(true)),
            ("/native_body/0/preparation/mode", json!("Send")),
            ("/native_body/0/preparation/cause", json!("RequestDeadline")),
            ("/native_body/0/preparation/deadline_at_us", json!(201000)),
            ("/native_body/0/heads", json!(0)),
            ("/native_body/0/head_at_us", Value::Null),
            ("/native_body/0/first_wait", Value::Null),
            ("/native_body/0/first_wait/outcome", json!("OtherReady")),
            ("/native_body/0/first_wait/observed_at_us", json!(199999)),
            ("/native_body/0/last_wait_timeout", Value::Null),
            (
                "/native_body/0/first_wait_timeout/kind",
                json!("HeadReadiness"),
            ),
            ("/native_body/0/last_wait_timeout/outcome", json!("Ready")),
            (
                "/native_body/0/last_wait_timeout/outcome",
                json!("OtherReady"),
            ),
            ("/native_body/0/last_wait_timeout/outcome", json!("Failed")),
            (
                "/native_body/0/last_wait_timeout/observed_at_us",
                json!(199999),
            ),
            (
                "/native_body/0/last_wait_timeout/started_at_us",
                json!(200000),
            ),
            (
                "/native_body/0/last_wait_timeout/observed_at_us",
                json!(201000),
            ),
            (
                "/native_body/0/first_wait_timeout/observed_at_us",
                json!(200060),
            ),
            (
                "/native_body/0/last_wait_timeout/started_at_us",
                json!(38000),
            ),
            ("/native_body/0/first_wait/started_at_us", json!(35000)),
            ("/native_body/0/waits", json!(0)),
            ("/native_body/0/ready_waits", json!(1)),
            ("/native_body/0/failed_waits", json!(1)),
            ("/native_body/0/reads", json!(1)),
            ("/native_body/0/first_read", json!({"pending":true})),
            ("/native_body/0/released_at_us", json!(199999)),
            ("/native_body/0/source_acquisitions", json!(2)),
            ("/native_body/0/source_releases", json!(0)),
            ("/native_body/0/source_releases", json!(2)),
            (
                "/native_body/0/preparation/target_uri",
                json!("http://127.0.0.1:8081/lifetimes/sample/pending-body-cutoff/a"),
            ),
        ] {
            let mut changed = report.clone();
            *changed.pointer_mut(field).unwrap() = value;
            assert!(
                validate(boundary, "pending-body-cutoff", "sample", &changed, &origin).is_err(),
                "{field}"
            );
        }
    }

    #[test]
    fn native_body_cutoff_rejects_missing_reused_ready_late_and_unreleased_witnesses() {
        let (report, origin) = pending_cutoff_sample("pending-body-cutoff");
        for adapter in ["axum", "cloudflare", "spin"] {
            validate(
                AdapterBoundary {
                    name: adapter,
                    ..AXUM
                },
                "pending-body-cutoff",
                "sample",
                &report,
                &origin,
            )
            .unwrap();
            for (path, value) in [
                ("/native_body", Value::Null),
                ("/native_body/0/attachments", json!(0)),
                ("/native_body/1/preparations", json!(2)),
                ("/native_body/1/invalid", json!(true)),
                ("/native_body/1/preparation/mode", json!("Send")),
                ("/native_body/1/preparation/cause", json!("RequestDeadline")),
                ("/native_body/1/preparation/deadline_at_us", json!(201000)),
                ("/native_body/1/preparation/entered_at_us", json!(33999)),
                (
                    "/native_body/1/preparation/target_uri",
                    json!("http://127.0.0.1:8080/lifetimes/foreign/pending-body-cutoff/b"),
                ),
                ("/native_body/1/heads", json!(0)),
                ("/native_body/1/head_at_us", Value::Null),
                ("/native_body/1/first_read", Value::Null),
                ("/native_body/1/first_read/pending", json!(false)),
                ("/native_body/1/first_read/started_at_us", json!(200000)),
                ("/native_body/1/first_read/observed_at_us", json!(36099)),
                ("/native_body/1/first_read/observed_at_us", json!(200000)),
                ("/native_body/1/reads", json!(0)),
                ("/native_body/1/ready_reads", json!(1)),
                ("/native_body/1/source_acquisitions", json!(2)),
                ("/native_body/1/source_releases", json!(0)),
                ("/native_body/1/source_releases", json!(2)),
            ] {
                let mut changed = report.clone();
                *changed.pointer_mut(path).unwrap() = value;
                assert!(
                    validate(
                        AdapterBoundary {
                            name: adapter,
                            ..AXUM
                        },
                        "pending-body-cutoff",
                        "sample",
                        &changed,
                        &origin
                    )
                    .is_err(),
                    "{adapter}: {path}"
                );
            }
            for duplicate in [false, true] {
                let mut changed = report.clone();
                if duplicate {
                    changed["native_body"][1] = changed["native_body"][0].clone();
                } else {
                    changed["native_body"].as_array_mut().unwrap().pop();
                }
                assert!(
                    validate(
                        AdapterBoundary {
                            name: adapter,
                            ..AXUM
                        },
                        "pending-body-cutoff",
                        "sample",
                        &changed,
                        &origin
                    )
                    .is_err()
                );
            }
        }
    }

    #[test]
    fn native_head_cutoff_is_a_zero_head_and_zero_read_control() {
        let (report, origin) = pending_cutoff_sample("pending-head-cutoff");
        for adapter in ["axum", "cloudflare", "spin", "fastly"] {
            validate(
                AdapterBoundary {
                    name: adapter,
                    ..AXUM
                },
                "pending-head-cutoff",
                "sample",
                &report,
                &origin,
            )
            .unwrap();
            for field in [
                "heads",
                "reads",
                "ready_reads",
                "source_acquisitions",
                "source_releases",
                "waits",
                "ready_waits",
                "failed_waits",
            ] {
                let mut changed = report.clone();
                changed["native_body"][0][field] = json!(1);
                assert!(
                    validate(
                        AdapterBoundary {
                            name: adapter,
                            ..AXUM
                        },
                        "pending-head-cutoff",
                        "sample",
                        &changed,
                        &origin
                    )
                    .is_err(),
                    "{field}"
                );
            }
        }
    }

    #[test]
    fn native_body_evidence_rejects_early_release_and_same_path_at_another_origin() {
        let (report, origin) = pending_cutoff_sample("pending-body-cutoff");
        for adapter in ["axum", "cloudflare", "spin"] {
            for (field, value) in [
                ("/native_body/0/released_at_us", json!(40000)),
                (
                    "/native_body/0/preparation/target_uri",
                    json!("http://127.0.0.1:8081/lifetimes/sample/pending-body-cutoff/a"),
                ),
            ] {
                let mut changed = report.clone();
                *changed.pointer_mut(field).unwrap() = value;
                assert!(
                    validate(
                        AdapterBoundary {
                            name: adapter,
                            ..AXUM
                        },
                        "pending-body-cutoff",
                        "sample",
                        &changed,
                        &origin
                    )
                    .is_err(),
                    "{adapter}: {field}"
                );
            }
        }
    }

    #[test]
    fn pending_head_and_body_cutoff_evidence_requires_actual_blocked_exchange() {
        for case in ["pending-head-cutoff", "pending-body-cutoff"] {
            let (report, origin) = pending_cutoff_sample(case);
            validate(AXUM, case, "sample", &report, &origin).unwrap();
        }
    }

    #[test]
    fn pending_cutoff_rejects_early_late_unpolled_and_resolved_outcomes() {
        for case in ["pending-head-cutoff", "pending-body-cutoff"] {
            let (report, origin) = pending_cutoff_sample(case);
            for (path, value) in [
                ("/events/0/result/cutoff_at_us", json!(234000)),
                ("/events/1/duration_us", json!(165899)),
                ("/events/1/duration_us", json!(170901)),
                ("/events/1/result/state", json!("error")),
                ("/events/1/result/pending_polls", json!(0)),
                ("/events/1/result/first_poll_us", json!(200000)),
                ("/events/2/at_us", json!(199999)),
                ("/events/2/duration_us", json!(5000)),
                ("/slots/0", json!("Resolved")),
                ("/termination", json!("Completed")),
                ("/error", json!({"status":504})),
                ("/items", json!([{"index":0}])),
            ] {
                let mut changed = report.clone();
                *changed.pointer_mut(path).unwrap() = value;
                assert!(
                    validate(AXUM, case, "sample", &changed, &origin).is_err(),
                    "{case}: {path}"
                );
            }
        }
    }

    #[test]
    fn pending_cutoff_rejects_missing_heads_body_data_and_premature_release() {
        for case in ["pending-head-cutoff", "pending-body-cutoff"] {
            let (report, origin) = pending_cutoff_sample(case);
            for op in ["request", "waiting", "release", "closed"] {
                let mut changed = origin.clone();
                let values = changed.as_array_mut().unwrap();
                let index = values.iter().position(|entry| entry["op"] == op).unwrap();
                values.remove(index);
                assert!(
                    validate(AXUM, case, "sample", &report, &changed).is_err(),
                    "{case}: {op}"
                );
            }
            let mut changed = origin.clone();
            let release = changed
                .as_array_mut()
                .unwrap()
                .iter_mut()
                .find(|entry| entry["op"] == "release")
                .unwrap();
            release["at_us"] = json!(36000);
            assert!(validate(AXUM, case, "sample", &report, &changed).is_err());
            let mut changed = origin.clone();
            changed
                .as_array_mut()
                .unwrap()
                .push(json!({"op":"chunk","role":"a",
                "token":"sample","case":case,"at_us":50000,"bytes":[65]}));
            assert!(validate(AXUM, case, "sample", &report, &changed).is_err());
            let mut changed = origin.clone();
            if case == "pending-body-cutoff" {
                changed
                    .as_array_mut()
                    .unwrap()
                    .retain(|entry| entry["op"] != "head");
            } else {
                changed
                    .as_array_mut()
                    .unwrap()
                    .push(json!({"op":"head","role":"a",
                    "token":"sample","case":case,"at_us":36000}));
            }
            assert!(validate(AXUM, case, "sample", &report, &changed).is_err());
        }
    }

    #[test]
    fn pending_cutoff_rejects_release_before_wait_even_when_timestamps_tie() {
        let (report, mut origin) = pending_cutoff_sample("pending-body-cutoff");
        let entries = origin.as_array_mut().unwrap();
        for entry in entries.iter_mut() {
            if entry["op"] == "waiting" {
                entry["at_us"] = json!(210000);
            } else if entry["op"] == "release" {
                entry["response_returned_us"] = json!(210000);
            }
        }
        validate(AXUM, "pending-body-cutoff", "sample", &report, &origin).unwrap();
        let entries = origin.as_array_mut().unwrap();
        let index = entries
            .iter()
            .position(|entry| entry["op"] == "release")
            .unwrap();
        let release = entries.remove(index);
        let index = entries
            .iter()
            .position(|entry| entry["op"] == "waiting")
            .unwrap();
        entries.insert(index, release);
        assert!(validate(AXUM, "pending-body-cutoff", "sample", &report, &origin).is_err());
    }

    #[test]
    fn pending_cutoff_rejects_origin_waiting_only_after_guest_response_returned() {
        for case in ["pending-head-cutoff", "pending-body-cutoff"] {
            for role in ["a", "b", "c"] {
                for move_whole_phase in [false, true] {
                    let (report, mut origin) = pending_cutoff_sample(case);
                    for entry in origin
                        .as_array_mut()
                        .unwrap()
                        .iter_mut()
                        .filter(|entry| entry["role"] == role)
                    {
                        match entry["op"].as_str().unwrap() {
                            "waiting" => entry["at_us"] = json!(209500),
                            "request" if move_whole_phase => entry["at_us"] = json!(209300),
                            "head" if move_whole_phase => {
                                entry["write_started_us"] = json!(209350);
                                entry["at_us"] = json!(209400);
                            }
                            _ => {}
                        }
                    }
                    assert!(validate(AXUM, case, "sample", &report, &origin).is_err());
                }
            }
        }
    }

    #[test]
    fn pending_cutoff_requires_a_valid_response_boundary_before_release() {
        for case in ["pending-head-cutoff", "pending-body-cutoff"] {
            for boundary in [
                Value::Null,
                json!(-1),
                json!("209000"),
                json!(0),
                json!(210001),
            ] {
                let (report, mut origin) = pending_cutoff_sample(case);
                let release = origin
                    .as_array()
                    .unwrap()
                    .iter()
                    .position(|entry| entry["op"] == "release")
                    .unwrap();
                origin[release]["response_returned_us"] = boundary;
                assert!(validate(AXUM, case, "sample", &report, &origin).is_err());
                origin[release]
                    .as_object_mut()
                    .unwrap()
                    .remove("response_returned_us");
                assert!(validate(AXUM, case, "sample", &report, &origin).is_err());
            }
        }
    }

    fn sample(case: &str) -> (Value, Value) {
        let mut report = json!({"schema":1,"case":case,"token":"sample","qualification":false,
            "events":[],"items":[],"slots":[],"termination":null,"error":null,
            "origin":null,"mediator":null});
        let mut origin = json!([]);
        if case == "begin-progress" {
            report["events"] = json!([
                {"op":"begin","at_us":2000,"duration_us":100},
                {"op":"quiet-start","at_us":2100,"duration_us":0},
                {"op":"quiet-end","at_us":102100,"duration_us":0},
                {"op":"origin-head","at_us":102100,"duration_us":47900}
            ]);
            report["origin"] = json!({"status":200,"bytes":[79]});
            origin = json!([
                {"op":"request","role":"origin","at_us":3000,"guest_at_us":2000},
                {"op":"head","role":"origin","at_us":151000,"write_started_us":151000}
            ]);
        } else if case == "batch-progress" {
            report["events"] = json!([
                {"op":"batch-start","at_us":34000,"duration_us":100},
                {"op":"quiet-start","at_us":34100,"duration_us":0},
                {"op":"quiet-end","at_us":335000,"duration_us":0}
            ]);
            report["slots"] = json!(["Resolved", "Resolved", "Resolved"]);
            report["termination"] = json!("Completed");
            report["items"] = json!([
                {"index":0,"via":"collect","elapsed_us":20000,"outcome":{"status":200,"bytes":[65]}},
                {"index":1,"via":"collect","elapsed_us":40000,"outcome":{"status":200,"bytes":[66]}},
                {"index":2,"via":"collect","elapsed_us":60000,"outcome":{"status":200,"bytes":[67]}}
            ]);
            origin = json!([
                {"op":"request","role":"a","at_us":35000,"guest_at_us":34000},
                {"op":"request","role":"b","at_us":35010,"guest_at_us":34000},
                {"op":"request","role":"c","at_us":35020,"guest_at_us":34000},
                {"op":"request","role":"quiet","at_us":35100,"guest_at_us":34100},
                {"op":"head","role":"quiet","at_us":336000,"write_started_us":336000},
                {"op":"chunk","role":"a","at_us":55000},
                {"op":"chunk","role":"b","at_us":75000},
                {"op":"chunk","role":"c","at_us":95000}
            ]);
        } else {
            let slow = case == "slow-origin";
            report["events"] = json!([
                {"op":"begin","at_us":2000,"duration_us":100},
                {"op":"batch-start","at_us":34000,"duration_us":100},
                {"op":"check","at_us":if slow {701000}else{260000},"duration_us":10}
            ]);
            report["items"] = json!([
                {"index":0,"via":"check","elapsed_us":146000,"outcome":{"status":200,"bytes":[65]}},
                {"index":1,"via":if slow{"check"}else{"next"},"elapsed_us":286000,"outcome":{"status":200,"bytes":[66]}}
            ]);
            report["slots"] = json!([
                "AlreadyDelivered",
                "AlreadyDelivered",
                if slow { "Unresolved" } else { "Resolved" }
            ]);
            report["termination"] = json!(if slow { "Cutoff" } else { "Completed" });
            report["origin"] =
                json!({"status":200,"bytes":if slow {vec![79,111,33]} else {vec![79]}});
            origin = json!([
                {"op":"request","role":"origin","at_us":3000,"guest_at_us":2000},
                {"op":"request","role":"a","at_us":34000,"guest_at_us":34000},
                {"op":"request","role":"b","at_us":34010,"guest_at_us":34000},
                {"op":"request","role":"c","at_us":34020,"guest_at_us":34000}
            ]);
            if !slow {
                report["items"].as_array_mut().unwrap().push(json!(
                    {"index":2,"via":"collect","elapsed_us":366000,"outcome":{"status":200,"bytes":[67]}}));
                report["events"].as_array_mut().unwrap().push(json!(
                    {"op":"mediator-send","at_us":400000,"duration_us":134000}));
                report["mediator"] = json!({"sent_at_us":400000,"remaining_us":134000,
                    "error":{"kind":"gateway_timeout","status":504,"reason":"RequestDeadline"}});
                origin.as_array_mut().unwrap().push(json!(
                    {"op":"request","role":"mediator","at_us":401000,"guest_at_us":400000}));
            }
            report["events"].as_array_mut().unwrap().extend(if slow {
                vec![json!({"op":"collect","at_us":701010,"duration_us":1})]
            } else {
                vec![
                    json!({"op":"next","at_us":260010,"duration_us":59990}),
                    json!({"op":"collect","at_us":320000,"duration_us":80000}),
                ]
            });
            report["events"].as_array_mut().unwrap().extend([
                json!({"op":"origin-head","at_us":34000,"duration_us":if slow{666000}else{86000}}),
                json!({"op":"origin-body","at_us":if slow{701000}else{120000},
                    "duration_us":if slow{599000}else{140000}}),
            ]);
            origin[0]["alignment"] = json!({"basis":"origin-receipt-minus-guest-at-us",
                "clock_sync_proven":false,"transit_uncertainty_us":null,"scenario_anchor_host_offset_us":1000});
            origin.as_array_mut().unwrap().push(json!({"op":"alignment","role":"control","at_us":36000,
                "basis":"initial-receipts-minimum-upper-bound","receipt_roles":["origin","a","b","c"],
                "selected_role":"a","scenario_anchor_host_offset_us":1000,"rounding_guard_us":1000,
                "transit_uncertainty_us":null,"clock_sync_proven":false,"same_rate_assumed":true}));
            let scripts = vec![
                (
                    "origin",
                    if slow { 700000 } else { 120000 },
                    if slow {
                        vec![(700000, 79), (900000, 111), (1300000, 33)]
                    } else {
                        vec![(260000, 79)]
                    },
                ),
                ("a", 180000, vec![(180000, 65)]),
                ("b", 320000, vec![(320000, 66)]),
            ];
            for (role, head, chunks) in scripts.into_iter().chain(std::iter::once((
                "c",
                if slow { 900000 } else { 400000 },
                vec![(if slow { 900000 } else { 400000 }, 67)],
            ))) {
                origin
                    .as_array_mut()
                    .unwrap()
                    .push(json!({"op":"head","role":role,
                    "at_us":head+1000,"write_started_us":head+1000,"wait_returned_us":head+1000,"scheduled_us":head+1000,"scenario_scheduled_us":head,
                    "schedule_basis":"initial-receipts-minimum-upper-bound"}));
                for (time, byte) in chunks {
                    let mut chunk = json!({"op":"chunk","role":role,"bytes":[byte],
                        "at_us":time+1000,"write_started_us":time+1000,"wait_returned_us":time+1000,"scheduled_us":time+1000,"scenario_scheduled_us":time,
                        "schedule_basis":"initial-receipts-minimum-upper-bound"});
                    if role == "origin" && time == if slow { 1300000 } else { 260000 } {
                        chunk["terminal_frame"] = json!({"write_started_us":time+1000,"completed_us":time+1000,"status":"pass","error":null});
                    }
                    origin.as_array_mut().unwrap().push(chunk);
                }
                if role == "c" || role == "origin" {
                    let end = if role == "origin" {
                        if slow { 1300000 } else { 260000 }
                    } else {
                        head
                    };
                    origin
                        .as_array_mut()
                        .unwrap()
                        .push(json!({"op":"closed","role":role,
                        "at_us":end+1001,"reason":"complete"}));
                }
            }
        }
        report["events"]
            .as_array_mut()
            .unwrap()
            .sort_by_key(|entry| number(entry, "at_us").unwrap());
        for entry in origin.as_array_mut().unwrap() {
            entry["token"] = json!("sample");
            entry["case"] = json!(case);
        }
        (report, origin)
    }

    fn body_read_arrivals_sample() -> (Value, Value) {
        let (mut report, mut origin) = sample("slow-origin");
        report["case"] = json!("body-read-arrivals");
        report["origin"] = json!({"status":200,"bytes":[79,111]});
        report["events"] = json!([
            {"op":"begin","at_us":2000,"duration_us":100,"result":"scheduled"},
            {"op":"batch-start","at_us":34000,"duration_us":100,"result":{"cutoff_at_us":534000}},
            {"op":"origin-head","at_us":34100,"duration_us":65900,"result":"ok"},
            {"op":"origin-body","at_us":100000,"duration_us":700000,"result":"streamed"},
            {"op":"check","at_us":800000,"duration_us":10,"result":"Item"},
            {"op":"check","at_us":800010,"duration_us":10,"result":"Item"},
            {"op":"check","at_us":800020,"duration_us":10,"result":"Finished"},
            {"op":"collect","at_us":800030,"duration_us":10,"result":"ok"}
        ]);
        origin.as_array_mut().unwrap().retain(|entry| {
            !(entry["role"] == "origin"
                && entry["op"] == "chunk"
                && entry["scenario_scheduled_us"] == 1300000)
        });
        for entry in origin.as_array_mut().unwrap() {
            entry["case"] = json!("body-read-arrivals");
            if entry["role"] != "origin" || entry["op"] == "request" {
                continue;
            }
            if entry["op"] == "closed" {
                entry["at_us"] = json!(801001);
                continue;
            }
            let target = if entry["scenario_scheduled_us"] == 700000 {
                100000
            } else {
                800000
            };
            entry["scenario_scheduled_us"] = json!(target);
            for field in [
                "at_us",
                "write_started_us",
                "wait_returned_us",
                "scheduled_us",
            ] {
                entry[field] = json!(target + 1000);
            }
            if target == 800000 {
                entry["terminal_frame"] = json!({"write_started_us":801000,"completed_us":801000,"status":"pass","error":null});
            }
        }
        (report, origin)
    }

    #[test]
    fn body_read_arrivals_require_original_elapsed_without_batch_observation_during_reads() {
        let boundary = AdapterBoundary {
            name: "fastly",
            ..AXUM
        };
        let (report, origin) = body_read_arrivals_sample();
        validate(boundary, "body-read-arrivals", "sample", &report, &origin).unwrap();
        for (path, value) in [
            ("/events/0/result", json!("error")),
            ("/events/0/duration_us", json!(40000)),
            ("/events/4/duration_us", json!(4000)),
            ("/items/1/elapsed_us", json!(766000)),
            ("/items/1/elapsed_us", json!(500000)),
            ("/items/1/index", json!(2)),
            ("/slots/2", json!("Resolved")),
            ("/termination", json!("Completed")),
            ("/origin/bytes", json!([79])),
            ("/events/1/result/cutoff_at_us", json!(1034000)),
            ("/events/3/result", json!("buffered")),
            ("/events/3/at_us", json!(400000)),
            ("/events/3/duration_us", json!(100000)),
            ("/events/4/at_us", json!(320000)),
            ("/events/4/op", json!("next")),
        ] {
            let mut changed = report.clone();
            *changed.pointer_mut(path).unwrap() = value;
            assert!(
                validate(boundary, "body-read-arrivals", "sample", &changed, &origin).is_err(),
                "{path}"
            );
        }
        let mut missing = report.clone();
        missing["items"].as_array_mut().unwrap().pop();
        assert!(validate(boundary, "body-read-arrivals", "sample", &missing, &origin).is_err());
        let mut polled = report.clone();
        polled["events"].as_array_mut().unwrap().insert(
            3,
            json!({"op":"check","at_us":100000,"duration_us":0,"result":"Pending"}),
        );
        assert!(validate(boundary, "body-read-arrivals", "sample", &polled, &origin).is_err());
        for (field, value) in [
            ("scenario_scheduled_us", json!(600000)),
            ("bytes", json!([0])),
            ("terminal_frame", Value::Null),
        ] {
            let mut changed = origin.clone();
            let entry = changed
                .as_array_mut()
                .unwrap()
                .iter_mut()
                .find(|entry| {
                    entry["role"] == "origin"
                        && entry["op"] == "chunk"
                        && entry["scenario_scheduled_us"] == 800000
                })
                .unwrap();
            entry[field] = value;
            assert!(
                validate(boundary, "body-read-arrivals", "sample", &report, &changed).is_err(),
                "{field}"
            );
        }
    }

    fn fastly_progress_sample(case: &str) -> (Value, Value) {
        let (mut report, mut origin) = sample("slow-origin");
        let trickle = case == "trickling-slot";
        report["case"] = json!(case);
        origin
            .as_array_mut()
            .unwrap()
            .retain(|entry| matches!(entry["op"].as_str(), Some("request" | "alignment")));
        if !trickle {
            origin[0]["at_us"] = json!(35100);
            origin[0]["guest_at_us"] = json!(34100);
        }
        for (role, head, chunks) in [
            (
                "origin",
                if trickle { 100000 } else { 700000 },
                if trickle {
                    (0..11)
                        .map(|index| (100000 + index * 50000, 48 + index))
                        .collect::<Vec<_>>()
                } else {
                    vec![(700000, 79), (900000, 111), (1300000, 33)]
                },
            ),
            ("a", 180000, vec![(180000, 65)]),
            (
                "b",
                if trickle { 100000 } else { 320000 },
                vec![(if trickle { 600000 } else { 320000 }, 66)],
            ),
            ("c", 900000, vec![(900000, 67)]),
        ] {
            origin.as_array_mut().unwrap().push(json!({"op":"head","role":role,"at_us":head+1000,"write_started_us":head+1000,
                "wait_returned_us":head+1000,"scheduled_us":head+1000,"scenario_scheduled_us":head,"schedule_basis":"initial-receipts-minimum-upper-bound"}));
            if trickle && role == "b" {
                origin.as_array_mut().unwrap().last_mut().unwrap()["body_scheduled_us"] =
                    json!(601000);
            }
            let end = chunks.last().unwrap().0;
            for (at, byte) in chunks {
                let mut entry = json!({"op":"chunk","role":role,"at_us":at+1000,"write_started_us":at+1000,"wait_returned_us":at+1000,
                    "scheduled_us":at+1000,"scenario_scheduled_us":at,"schedule_basis":"initial-receipts-minimum-upper-bound","bytes":[byte]});
                if role == "origin" && at == end {
                    entry["terminal_frame"] = json!({"status":"pass","error":null,"write_started_us":at+1000,"completed_us":at+1000});
                }
                origin.as_array_mut().unwrap().push(entry);
            }
            origin
                .as_array_mut()
                .unwrap()
                .push(json!({"op":"closed","role":role,"at_us":end+1001,"reason":"complete"}));
        }
        for entry in origin.as_array_mut().unwrap() {
            entry["token"] = json!("sample");
            entry["case"] = json!(case);
        }
        if trickle {
            let mut observations = Vec::new();
            for index in 0..11 {
                let at = 100000 + index * 50000;
                observations.push(json!({"op":"read","started_at_us":if index==0 {100000}else{at-50000},"at_us":at,"result":[48+index]}));
                if index == 1 {
                    observations.push(json!({"op":"next","started_at_us":100000,"at_us":180000,"result":"Item","index":0}));
                }
                if index == 8 {
                    observations.push(json!({"op":"next","started_at_us":180001,"at_us":534000,"result":"Finished","termination":"Cutoff"}));
                }
            }
            observations
                .push(json!({"op":"read","started_at_us":600000,"at_us":600001,"result":null}));
            report["events"] = json!([
                {"op":"begin","at_us":2000,"duration_us":100,"result":"scheduled"},
                {"op":"batch-start","at_us":34000,"duration_us":100,"result":{"cutoff_at_us":534000}},
                {"op":"origin-head","at_us":34100,"duration_us":65900,"result":"ok"},
                {"op":"trickle","at_us":100000,"duration_us":500001,"result":{"observations":observations}},
                {"op":"collect","at_us":600001,"duration_us":10,"result":"ok"}
            ]);
            report["items"].as_array_mut().unwrap().truncate(1);
            report["items"][0]["via"] = json!("next");
            report["slots"] = json!(["AlreadyDelivered", "Unresolved", "Unresolved"]);
            report["origin"] = json!({"status":200,"bytes":b"0123456789:"});
        } else {
            report["events"] = json!([
                {"op":"batch-start","at_us":34000,"duration_us":100,"result":{"cutoff_at_us":534000}},
                {"op":"origin-send","at_us":34100,"duration_us":665900,"result":"ok"},
                {"op":"check","at_us":701000,"duration_us":10,"result":"Item"},
                {"op":"check","at_us":701010,"duration_us":10,"result":"Item"},
                {"op":"check","at_us":701020,"duration_us":10,"result":"Finished"},
                {"op":"collect","at_us":701030,"duration_us":10,"result":"ok"},
                {"op":"origin-body","at_us":701040,"duration_us":598960,"result":null}
            ]);
        }
        (report, origin)
    }

    #[test]
    fn trickling_slot_requires_app_receipts_and_cutoff_before_the_bidder_body() {
        let boundary = AdapterBoundary {
            name: "fastly",
            ..AXUM
        };
        let (report, origin) = fastly_progress_sample("trickling-slot");
        validate(boundary, "trickling-slot", "sample", &report, &origin).unwrap();
        for (path, value) in [
            ("/events/1/result/cutoff_at_us", json!(600000)),
            ("/events/3/result/observations/1/at_us", json!(600000)),
            ("/events/3/result/observations/10/at_us", json!(600000)),
            (
                "/events/3/result/observations/10/termination",
                json!("Completed"),
            ),
            (
                "/events/3/result/observations/10/started_at_us",
                json!(600000),
            ),
            ("/events/3/result/observations/0/result", json!([0])),
            ("/events/3/result/observations/13/result", json!([])),
            ("/items/0/elapsed_us", json!(500000)),
            ("/slots/1", json!("Resolved")),
        ] {
            let mut changed = report.clone();
            *changed.pointer_mut(path).unwrap() = value;
            assert!(
                validate(boundary, "trickling-slot", "sample", &changed, &origin).is_err(),
                "{path}"
            );
        }
        let mut empty = report.clone();
        empty["events"][3]["result"]["observations"]
            .as_array_mut()
            .unwrap()
            .insert(
                1,
                json!({"op":"read","started_at_us":100000,"at_us":125000,"result":[]}),
            );
        empty["events"][3]["result"]["observations"][2]["started_at_us"] = json!(125000);
        validate(boundary, "trickling-slot", "sample", &empty, &origin).unwrap();
        let mut missing = report.clone();
        missing["events"][3]["result"]["observations"]
            .as_array_mut()
            .unwrap()
            .pop();
        assert!(validate(boundary, "trickling-slot", "sample", &missing, &origin).is_err());
        let mut missing_result = report.clone();
        missing_result["events"][3]["result"]["observations"]
            .as_array_mut()
            .unwrap()
            .last_mut()
            .unwrap()
            .as_object_mut()
            .unwrap()
            .remove("result");
        assert!(
            validate(
                boundary,
                "trickling-slot",
                "sample",
                &missing_result,
                &origin
            )
            .is_err()
        );
        let mut many = report.clone();
        many["events"][3]["result"]["observations"]
            .as_array_mut()
            .unwrap()
            .splice(
                1..1,
                (0..53).map(
                    |_| json!({"op":"read","started_at_us":100000,"at_us":100000,"result":[]}),
                ),
            );
        assert!(validate(boundary, "trickling-slot", "sample", &many, &origin).is_err());
        let mut closed = origin.clone();
        closed
            .as_array_mut()
            .unwrap()
            .retain(|entry| !(entry["role"] == "b" && entry["op"] == "chunk"));
        let end = closed
            .as_array_mut()
            .unwrap()
            .iter_mut()
            .find(|entry| entry["role"] == "b" && entry["op"] == "closed")
            .unwrap();
        end["reason"] = json!("peer_closed_after_cutoff");
        validate(boundary, "trickling-slot", "sample", &report, &closed).unwrap();
        for (op, field, value) in [
            ("head", "body_scheduled_us", json!(534000)),
            ("head", "scenario_scheduled_us", json!(600000)),
            ("closed", "at_us", json!(599000)),
        ] {
            let mut changed = closed.clone();
            changed
                .as_array_mut()
                .unwrap()
                .iter_mut()
                .find(|entry| entry["role"] == "b" && entry["op"] == op)
                .unwrap()[field] = value;
            assert!(
                validate(boundary, "trickling-slot", "sample", &report, &changed).is_err(),
                "{field}"
            );
        }
    }

    #[test]
    fn trickling_slot_requires_read_progress_inside_the_terminal_wait() {
        let boundary = AdapterBoundary {
            name: "fastly",
            ..AXUM
        };
        let (mut report, origin) = fastly_progress_sample("trickling-slot");
        report["events"][3]["result"]["observations"][10]["started_at_us"] = json!(533999);
        assert!(validate(boundary, "trickling-slot", "sample", &report, &origin).is_err());
    }

    #[test]
    fn send_during_batch_requires_send_after_start_and_preserves_arrival_results() {
        let boundary = AdapterBoundary {
            name: "fastly",
            ..AXUM
        };
        let (report, origin) = fastly_progress_sample("send-during-batch");
        validate(boundary, "send-during-batch", "sample", &report, &origin).unwrap();
        for (path, value) in [
            ("/events/1/op", json!("begin")),
            ("/events/1/at_us", json!(34000)),
            ("/events/0/result/cutoff_at_us", json!(1034000)),
            ("/events/2/at_us", json!(400000)),
            ("/items/1/elapsed_us", json!(666000)),
            ("/slots/2", json!("Resolved")),
        ] {
            let mut changed = report.clone();
            *changed.pointer_mut(path).unwrap() = value;
            assert!(
                validate(boundary, "send-during-batch", "sample", &changed, &origin).is_err(),
                "{path}"
            );
        }
        let mut early = origin.clone();
        early[0]["guest_at_us"] = json!(2000);
        assert!(validate(boundary, "send-during-batch", "sample", &report, &early).is_err());
    }

    #[test]
    fn progress_evidence_requires_actual_receipts_inside_unobserved_windows() {
        for case in ["begin-progress", "batch-progress"] {
            let (report, origin) = sample(case);
            validate(AXUM, case, "sample", &report, &origin).unwrap();
            let mut late = origin.clone();
            late[0]["at_us"] = json!(999000);
            assert!(validate(AXUM, case, "sample", &report, &late).is_err());
            assert!(validate(AXUM, case, "sample", &report, &json!([])).is_err());
            let mut duplicate = origin.clone();
            duplicate.as_array_mut().unwrap().push(origin[0].clone());
            assert!(validate(AXUM, case, "sample", &report, &duplicate).is_err());
        }
    }

    fn timeout_sample() -> (Value, Value) {
        let (mut report, mut origin) = sample("slow-origin");
        report["case"] = json!("timeout-before-cutoff");
        report["items"]
            .as_array_mut()
            .unwrap()
            .push(json!({"index":3,"via":"check",
            "elapsed_us":300000,"outcome":{"error":{"kind":"gateway_timeout",
                "status":504,"reason":"PerCallTimeout"}}}));
        report["slots"]
            .as_array_mut()
            .unwrap()
            .push(json!("AlreadyDelivered"));
        report["events"]
            .as_array_mut()
            .unwrap()
            .iter_mut()
            .find(|entry| entry["op"] == "check")
            .unwrap()["result"] = json!("Item");
        report["events"].as_array_mut().unwrap().extend([
            json!({"op":"check","at_us":701001,"duration_us":0,"result":"Item"}),
            json!({"op":"check","at_us":701002,"duration_us":0,"result":"Item"}),
            json!({"op":"check","at_us":701003,"duration_us":0,"result":"Finished"}),
        ]);
        report["events"]
            .as_array_mut()
            .unwrap()
            .sort_by_key(|entry| number(entry, "at_us").unwrap());
        origin.as_array_mut().unwrap().extend([
            json!({"op":"request","role":"d","at_us":35030,"guest_at_us":34000}),
            json!({"op":"closed","role":"d","at_us":1501000,"scheduled_us":1501000,
                "scenario_scheduled_us":1500000,"reason":"silent",
                "schedule_basis":"initial-receipts-minimum-upper-bound"}),
        ]);
        origin
            .as_array_mut()
            .unwrap()
            .iter_mut()
            .find(|entry| entry["op"] == "alignment")
            .unwrap()["receipt_roles"] = json!(["origin", "a", "b", "c", "d"]);
        for entry in origin.as_array_mut().unwrap() {
            entry["token"] = json!("sample");
            entry["case"] = json!("timeout-before-cutoff");
        }
        (report, origin)
    }

    #[test]
    fn timeout_before_cutoff_preserves_siblings_and_per_call_timeout_provenance() {
        let (report, origin) = timeout_sample();
        validate(AXUM, "timeout-before-cutoff", "sample", &report, &origin).unwrap();
        for (field, value) in [
            ("index", json!(2)),
            ("via", json!("collect")),
            ("elapsed_us", json!(500000)),
            (
                "outcome",
                json!({"error":{"kind":"gateway_timeout","status":504,"reason":"BatchCutoff"}}),
            ),
            (
                "outcome",
                json!({"error":{"kind":"gateway_timeout","status":504,"reason":"RequestDeadline"}}),
            ),
            ("outcome", json!({"status":200,"bytes":[68]})),
        ] {
            let mut invalid = report.clone();
            invalid["items"][2][field] = value;
            assert!(
                validate(AXUM, "timeout-before-cutoff", "sample", &invalid, &origin).is_err(),
                "{field}"
            );
        }
        for (field, value) in [
            (
                "slots",
                json!([
                    "AlreadyDelivered",
                    "AlreadyDelivered",
                    "Unresolved",
                    "Unresolved"
                ]),
            ),
            ("termination", json!("Completed")),
            ("items", json!([])),
        ] {
            let mut invalid = report.clone();
            invalid[field] = value;
            assert!(validate(AXUM, "timeout-before-cutoff", "sample", &invalid, &origin).is_err());
        }
        let mut invalid = origin.clone();
        invalid
            .as_array_mut()
            .unwrap()
            .push(json!({"op":"head","role":"d","at_us":200000,
            "token":"sample","case":"timeout-before-cutoff"}));
        assert!(validate(AXUM, "timeout-before-cutoff", "sample", &report, &invalid).is_err());
        let mut invalid = origin.clone();
        invalid
            .as_array_mut()
            .unwrap()
            .retain(|entry| entry["role"] != "d");
        assert!(validate(AXUM, "timeout-before-cutoff", "sample", &report, &invalid).is_err());
    }

    #[test]
    fn timeout_before_cutoff_diagnostics_do_not_waive_failed_source_timing() {
        let (mut report, origin) = timeout_sample();
        report["events"]
            .as_array_mut()
            .unwrap()
            .iter_mut()
            .find(|entry| entry["op"] == "origin-head")
            .unwrap()["duration_us"] = json!(676000);
        validate_timeout_before_cutoff(&report, &origin).unwrap();
        assert!(validate(AXUM, "timeout-before-cutoff", "sample", &report, &origin).is_err());
        let (mut report, origin) = timeout_sample();
        let check = report["events"]
            .as_array_mut()
            .unwrap()
            .iter_mut()
            .find(|entry| entry["op"] == "check")
            .unwrap();
        check["result"] = json!("Finished");
        assert!(validate_timeout_before_cutoff(&report, &origin).is_err());
    }

    #[test]
    fn slow_origin_requires_late_c_evidence_even_when_c_is_unresolved() {
        for case in ["slow-origin", "timeout-before-cutoff"] {
            let (report, origin) = if case == "slow-origin" {
                sample(case)
            } else {
                timeout_sample()
            };
            validate(AXUM, case, "sample", &report, &origin).unwrap();
            for mutation in 0..4 {
                let mut changed = origin.clone();
                let entries = changed.as_array_mut().unwrap();
                match mutation {
                    0 => entries.retain(|entry| entry["role"] != "c" || entry["op"] != "closed"),
                    1 => {
                        entries
                            .iter_mut()
                            .find(|entry| entry["role"] == "c" && entry["op"] == "chunk")
                            .unwrap()["at_us"] = json!(500000);
                    }
                    2 => {
                        let chunk = entries
                            .iter()
                            .find(|entry| entry["role"] == "c" && entry["op"] == "chunk")
                            .unwrap()
                            .clone();
                        entries.push(chunk);
                    }
                    3 => entries.retain(|entry| entry["role"] != "c" || entry["op"] == "request"),
                    _ => unreachable!(),
                }
                assert!(
                    validate(AXUM, case, "sample", &report, &changed).is_err(),
                    "{case} mutation {mutation}"
                );
            }
        }
    }

    #[test]
    fn slow_origin_accepts_only_late_c_peer_closes_without_early_payload() {
        for keep_head in [false, true] {
            let (report, mut origin) = timeout_sample();
            let entries = origin.as_array_mut().unwrap();
            entries.retain(|entry| {
                entry["role"] != "c"
                    || entry["op"] != "chunk" && (keep_head || entry["op"] != "head")
            });
            let closed = entries
                .iter_mut()
                .find(|entry| entry["role"] == "c" && entry["op"] == "closed")
                .unwrap();
            closed["reason"] = json!("peer_closed_after_cutoff");
            validate(AXUM, "timeout-before-cutoff", "sample", &report, &origin).unwrap();
            if keep_head {
                let mut early = origin.clone();
                early
                    .as_array_mut()
                    .unwrap()
                    .iter_mut()
                    .find(|entry| entry["role"] == "c" && entry["op"] == "head")
                    .unwrap()["write_started_us"] = json!(800000);
                assert!(
                    validate(AXUM, "timeout-before-cutoff", "sample", &report, &early).is_err()
                );
            }
            let entries = origin.as_array_mut().unwrap();
            entries
                .iter_mut()
                .find(|entry| entry["role"] == "c" && entry["op"] == "closed")
                .unwrap()["at_us"] = json!(800000);
            assert!(validate(AXUM, "timeout-before-cutoff", "sample", &report, &origin).is_err());
        }
    }

    #[test]
    fn report_identity_and_terminal_states_cannot_be_guessed() {
        for case in ["begin-progress", "batch-progress"] {
            let (report, origin) = sample(case);
            for (field, value) in [
                ("schema", json!(2)),
                ("case", json!("other")),
                ("token", json!("foreign")),
                ("qualification", json!(true)),
                ("error", json!({"kind":"internal"})),
                ("events", json!([])),
            ] {
                let mut invalid = report.clone();
                invalid[field] = value;
                assert!(
                    validate(AXUM, case, "sample", &invalid, &origin).is_err(),
                    "{case} {field}"
                );
            }
        }
        let (report, origin) = sample("batch-progress");
        for (field, value) in [
            ("termination", json!("Cutoff")),
            ("slots", json!(["Resolved", "Unresolved", "Resolved"])),
            ("items", json!([])),
        ] {
            let mut invalid = report.clone();
            invalid[field] = value;
            assert!(validate(AXUM, "batch-progress", "sample", &invalid, &origin).is_err());
        }
    }

    #[test]
    fn slow_and_fast_origin_reject_collection_time_elapsed_and_renewed_deadlines() {
        for case in ["slow-origin", "fast-origin"] {
            let (report, origin) = sample(case);
            validate(AXUM, case, "sample", &report, &origin).unwrap();
            for index in 0..2 {
                let mut late = report.clone();
                late["items"][index]["elapsed_us"] = json!(667000);
                assert!(validate(AXUM, case, "sample", &late, &origin).is_err());
                let mut wrong = report.clone();
                wrong["items"][index]["index"] = json!(2);
                assert!(validate(AXUM, case, "sample", &wrong, &origin).is_err());
            }
            let mut states = report.clone();
            states["slots"][0] = json!("Unresolved");
            assert!(validate(AXUM, case, "sample", &states, &origin).is_err());
        }
        let (report, origin) = sample("fast-origin");
        for (field, value) in [
            ("remaining_us", json!(1000000)),
            ("sent_at_us", json!(534000)),
            (
                "error",
                json!({"kind":"gateway_timeout","status":504,"reason":"PerCallTimeout"}),
            ),
        ] {
            let mut invalid = report.clone();
            invalid["mediator"][field] = value;
            assert!(validate(AXUM, "fast-origin", "sample", &invalid, &origin).is_err());
        }
        let mut extended = report;
        extended["events"]
            .as_array_mut()
            .unwrap()
            .iter_mut()
            .find(|entry| entry["op"] == "mediator-send")
            .unwrap()["duration_us"] = json!(1000000);
        assert!(validate(AXUM, "fast-origin", "sample", &extended, &origin).is_err());
    }

    #[test]
    fn source_and_guest_timing_failures_are_reported_independently() {
        for case in ["slow-origin", "fast-origin"] {
            let (report, origin) = sample(case);
            let valid = timing_diagnostics(case, &report, &origin).unwrap();
            assert_eq!(valid["source_timeline"]["status"], "pass");
            assert_eq!(valid["guest_head_body"]["status"], "pass");
            assert!(valid["metrics_error"].is_null());
            let mut late_guest = report.clone();
            let head = late_guest["events"]
                .as_array_mut()
                .unwrap()
                .iter_mut()
                .find(|e| e["op"] == "origin-head")
                .unwrap();
            head["duration_us"] = json!(number(head, "duration_us").unwrap() + 6000);
            let verdict = timing_diagnostics(case, &late_guest, &origin).unwrap();
            assert_eq!(verdict["source_timeline"]["status"], "pass");
            assert_eq!(verdict["guest_head_body"]["status"], "fail");
            assert!(validate(AXUM, case, "sample", &late_guest, &origin).is_err());
            for (stage, expected) in [
                ("wait", [6000, 0, 0]),
                ("post-wait", [0, 6000, 0]),
                ("write", [0, 0, 6000]),
            ] {
                let mut delayed = origin.clone();
                let chunk = delayed
                    .as_array_mut()
                    .unwrap()
                    .iter_mut()
                    .find(|e| e["role"] == "a" && e["op"] == "chunk")
                    .unwrap();
                if stage == "wait" {
                    chunk["wait_returned_us"] =
                        json!(number(chunk, "wait_returned_us").unwrap() + 6000);
                }
                if stage != "write" {
                    chunk["write_started_us"] =
                        json!(number(chunk, "write_started_us").unwrap() + 6000);
                }
                chunk["at_us"] = json!(number(chunk, "at_us").unwrap() + 6000);
                let verdict = timing_diagnostics(case, &report, &delayed).unwrap();
                assert_eq!(verdict["source_timeline"]["status"], "fail");
                assert_eq!(verdict["guest_head_body"]["status"], "pass");
                let phases = verdict["source_phases"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .find(|e| e["role"] == "a" && e["op"] == "chunk")
                    .unwrap();
                assert_eq!(
                    [
                        number(phases, "wait_lateness_us").unwrap(),
                        number(phases, "post_wait_us").unwrap(),
                        number(phases, "write_duration_us").unwrap()
                    ],
                    expected
                );
                assert!(validate(AXUM, case, "sample", &report, &delayed).is_err());
            }
        }
        assert!(timing_diagnostics("caps-while-waiting", &Value::Null, &Value::Null).is_none());
    }

    #[test]
    fn timeline_requires_original_wait_return_between_schedule_and_write() {
        for case in ["slow-origin", "fast-origin"] {
            let (report, origin) = sample(case);
            validate(AXUM, case, "sample", &report, &origin).unwrap();
            for role in ["origin", "a", "b", "c"] {
                for op in ["head", "chunk"] {
                    for mutation in ["missing", "before-schedule", "after-write"] {
                        let mut changed = origin.clone();
                        let write = changed
                            .as_array_mut()
                            .unwrap()
                            .iter_mut()
                            .find(|e| e["role"] == role && e["op"] == op)
                            .unwrap();
                        match mutation {
                            "missing" => {
                                write.as_object_mut().unwrap().remove("wait_returned_us");
                            }
                            "before-schedule" => {
                                write["wait_returned_us"] =
                                    json!(number(write, "scheduled_us").unwrap() - 1)
                            }
                            _ => {
                                write["wait_returned_us"] =
                                    json!(number(write, "write_started_us").unwrap() + 1)
                            }
                        }
                        assert!(
                            validate(AXUM, case, "sample", &report, &changed).is_err(),
                            "{case}: {role}: {op}: {mutation}"
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn timeline_requires_successful_ordered_terminal_frame_evidence() {
        for case in ["slow-origin", "fast-origin"] {
            let (report, origin) = sample(case);
            validate(AXUM, case, "sample", &report, &origin).unwrap();
            for mutation in [
                "missing",
                "failed",
                "error",
                "early",
                "inverted",
                "after-close",
            ] {
                let mut changed = origin.clone();
                let last = changed
                    .as_array_mut()
                    .unwrap()
                    .iter_mut()
                    .rev()
                    .find(|e| e["role"] == "origin" && e["op"] == "chunk")
                    .unwrap();
                match mutation {
                    "missing" => {
                        last.as_object_mut().unwrap().remove("terminal_frame");
                    }
                    "failed" => last["terminal_frame"]["status"] = json!("fail"),
                    "error" => last["terminal_frame"]["error"] = json!("write failed"),
                    "early" => {
                        last["terminal_frame"]["write_started_us"] =
                            json!(number(last, "at_us").unwrap() - 1)
                    }
                    "inverted" => {
                        last["terminal_frame"]["completed_us"] =
                            json!(number(last, "at_us").unwrap() - 1)
                    }
                    _ => {
                        last["terminal_frame"]["completed_us"] =
                            json!(number(last, "at_us").unwrap() + 2)
                    }
                }
                assert!(
                    validate(AXUM, case, "sample", &report, &changed).is_err(),
                    "{case}: {mutation}"
                );
            }
        }
    }

    #[test]
    fn timeline_seal_rejects_missing_recomputed_or_post_write_alignment() {
        for case in ["slow-origin", "fast-origin"] {
            let (report, origin) = sample(case);
            validate(AXUM, case, "sample", &report, &origin).unwrap();
            for (field, value) in [
                ("scenario_anchor_host_offset_us", json!(1001)),
                ("selected_role", json!("origin")),
                ("receipt_roles", json!(["origin", "a", "b"])),
                ("rounding_guard_us", json!(0)),
                ("at_us", json!(2000)),
                ("at_us", json!(200000)),
                ("clock_sync_proven", json!(true)),
                ("same_rate_assumed", json!(false)),
                ("transit_uncertainty_us", json!(0)),
            ] {
                let mut changed = origin.clone();
                changed
                    .as_array_mut()
                    .unwrap()
                    .iter_mut()
                    .find(|e| e["op"] == "alignment")
                    .unwrap()[field] = value;
                assert!(
                    validate(AXUM, case, "sample", &report, &changed).is_err(),
                    "{case}: {field}"
                );
            }
            let mut missing = origin.clone();
            missing
                .as_array_mut()
                .unwrap()
                .retain(|e| e["op"] != "alignment");
            assert!(validate(AXUM, case, "sample", &report, &missing).is_err());
            let mut duplicate = origin.clone();
            let seal = duplicate
                .as_array()
                .unwrap()
                .iter()
                .find(|e| e["op"] == "alignment")
                .unwrap()
                .clone();
            duplicate.as_array_mut().unwrap().push(seal);
            assert!(validate(AXUM, case, "sample", &report, &duplicate).is_err());
            let mut delayed_eof = report.clone();
            let body = delayed_eof["events"]
                .as_array_mut()
                .unwrap()
                .iter_mut()
                .find(|e| e["op"] == "origin-body")
                .unwrap();
            body["duration_us"] = json!(number(body, "duration_us").unwrap() + 5001);
            assert!(
                validate(AXUM, case, "sample", &delayed_eof, &origin).is_err(),
                "delayed EOF must not be hidden by timely payload writes"
            );
        }
    }

    #[test]
    fn timeline_rejects_early_missing_or_inverted_source_starts() {
        for case in ["slow-origin", "fast-origin"] {
            let (report, origin) = sample(case);
            validate(AXUM, case, "sample", &report, &origin).unwrap();
            for role in ["origin", "a", "b", "c"] {
                for op in ["head", "chunk"] {
                    for mutation in ["early", "missing", "inverted"] {
                        let mut changed = origin.clone();
                        let write = changed
                            .as_array_mut()
                            .unwrap()
                            .iter_mut()
                            .find(|e| e["role"] == role && e["op"] == op)
                            .unwrap();
                        match mutation {
                            "early" => {
                                write["write_started_us"] =
                                    json!(number(write, "scheduled_us").unwrap() - 1)
                            }
                            "inverted" => {
                                write["write_started_us"] =
                                    json!(number(write, "at_us").unwrap() + 1)
                            }
                            _ => {
                                write.as_object_mut().unwrap().remove("write_started_us");
                            }
                        }
                        assert!(
                            validate(AXUM, case, "sample", &report, &changed).is_err(),
                            "{case}: {role}: {op}: {mutation}"
                        );
                    }
                }
            }
            for role in ["a", "b", "c"] {
                let mut changed = origin.clone();
                for entry in changed
                    .as_array_mut()
                    .unwrap()
                    .iter_mut()
                    .filter(|e| e["role"] == role)
                {
                    if entry["op"] == "head" {
                        entry["at_us"] = json!(number(entry, "at_us").unwrap() + 1);
                    }
                    if entry["op"] == "chunk" {
                        entry["at_us"] = json!(number(entry, "at_us").unwrap() + 2);
                    }
                    if entry["op"] == "closed" {
                        entry["at_us"] = json!(number(entry, "at_us").unwrap() + 3);
                    }
                }
                assert!(
                    validate(AXUM, case, "sample", &report, &changed).is_err(),
                    "{case}: {role}: payload started before head completion"
                );
            }
            for role in ["a", "c"] {
                for (delay, accepted) in [(5000, true), (5001, false)] {
                    let mut changed = origin.clone();
                    let chunk = changed
                        .as_array_mut()
                        .unwrap()
                        .iter_mut()
                        .find(|e| e["role"] == role && e["op"] == "chunk")
                        .unwrap();
                    let completed = number(chunk, "scheduled_us").unwrap() + delay;
                    chunk["at_us"] = json!(completed);
                    if let Some(closed) = changed
                        .as_array_mut()
                        .unwrap()
                        .iter_mut()
                        .find(|e| e["role"] == role && e["op"] == "closed")
                    {
                        closed["at_us"] = json!(completed + 1);
                    }
                    assert_eq!(
                        validate_timeline(case, report["events"].as_array().unwrap(), &changed)
                            .is_ok(),
                        accepted,
                        "{case}: {role}: original {delay} us completion boundary"
                    );
                }
            }
        }
    }

    #[test]
    fn timeline_evidence_cannot_omit_heads_or_replace_actual_times_with_schedules() {
        for case in ["slow-origin", "fast-origin"] {
            let (report, origin) = sample(case);
            validate(AXUM, case, "sample", &report, &origin).unwrap();
            for op in ["head", "chunk"] {
                let mut missing = origin.clone();
                missing
                    .as_array_mut()
                    .unwrap()
                    .retain(|entry| entry["op"] != op);
                assert!(
                    validate(AXUM, case, "sample", &report, &missing).is_err(),
                    "missing {op}"
                );
            }
            let mut late = origin.clone();
            let head = late
                .as_array_mut()
                .unwrap()
                .iter_mut()
                .find(|entry| entry["op"] == "head")
                .unwrap();
            head["at_us"] = json!(number(head, "scheduled_us").unwrap() + 6000);
            assert!(validate(AXUM, case, "sample", &report, &late).is_err());
            let mut reanchored = origin.clone();
            for entry in reanchored.as_array_mut().unwrap() {
                if entry.get("scheduled_us").is_some() {
                    entry["at_us"] = json!(number(entry, "at_us").unwrap() + 10_000);
                    entry["scheduled_us"] = json!(number(entry, "scheduled_us").unwrap() + 10_000);
                }
            }
            assert!(validate(AXUM, case, "sample", &report, &reanchored).is_err());
        }
    }

    #[test]
    fn quiet_window_proof_survives_cold_entry_clock_offsets() {
        for case in ["begin-progress", "batch-progress"] {
            let (report, origin) = sample(case);
            validate(AXUM, case, "sample", &report, &origin).unwrap();
            for offset in [100_000, 1_000_000] {
                let mut shifted = origin.clone();
                for entry in shifted.as_array_mut().unwrap() {
                    for field in ["at_us", "write_started_us"] {
                        if let Some(value) = entry[field].as_u64() {
                            entry[field] = json!(value + offset);
                        }
                    }
                }
                validate(AXUM, case, "sample", &report, &shifted).unwrap();
            }
        }
    }

    #[test]
    fn quiet_proof_rejects_late_receipts_and_collection_stamps_even_after_cold_entry() {
        for case in ["begin-progress", "batch-progress"] {
            let (report, origin) = sample(case);
            for shift in [0, 1_000_000] {
                let mut shifted = origin.clone();
                for entry in shifted.as_array_mut().unwrap() {
                    for field in ["at_us", "write_started_us"] {
                        if let Some(value) = entry[field].as_u64() {
                            entry[field] = json!(value + shift);
                        }
                    }
                }
                let events = report["events"].as_array().unwrap();
                let lower = clock_offset_lower(case, events, &shifted).unwrap();
                let end = number(event(events, "quiet-end").unwrap(), "at_us").unwrap();
                let mut threshold = shifted.clone();
                threshold[0]["at_us"] = json!(u64::try_from(lower + i128::from(end)).unwrap());
                validate(AXUM, case, "sample", &report, &threshold).unwrap();
                threshold[0]["at_us"] = json!(number(&threshold[0], "at_us").unwrap() + 1);
                assert!(validate(AXUM, case, "sample", &report, &threshold).is_err());
                let mut invalid = shifted.clone();
                invalid[0]["token"] = json!("foreign");
                assert!(validate(AXUM, case, "sample", &report, &invalid).is_err());
                if case == "batch-progress" {
                    let mut observed = report.clone();
                    observed["items"][0]["elapsed_us"] = json!(300_000);
                    assert!(validate(AXUM, case, "sample", &observed, &shifted).is_err());
                }
                let mut recorded_late = shifted;
                let head = recorded_late
                    .as_array_mut()
                    .unwrap()
                    .iter_mut()
                    .find(|entry| entry["op"] == "head")
                    .unwrap();
                head["at_us"] = json!(number(head, "at_us").unwrap() + 50_000);
                validate(AXUM, case, "sample", &report, &recorded_late).unwrap();
            }
        }
    }
}
