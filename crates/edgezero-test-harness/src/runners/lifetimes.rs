use super::*;
use crate::net::WireOrigins;

const CASES: [&str; 6] = [
    "slow-origin",
    "fast-origin",
    "begin-progress",
    "batch-progress",
    "pending-head-cutoff",
    "pending-body-cutoff",
];

pub(super) fn run(name: &str, run: &Path, records: &mut Records) -> Result<()> {
    let count = wire::progress_suite(name, "outbound-lifetimes", run, records, checks)?;
    if let Some(count) = count {
        records.push(json!({"event":"lifetime-inventory","adapter":name,
            "status":"pass","cases":count,"qualification":false}))?;
    }
    Ok(())
}

fn checks(port: u16, origins: &WireOrigins, records: &mut Records) -> Result<usize> {
    let mut failures = Vec::new();
    for case in CASES {
        let token = token();
        origins.register_lifetime(&token, case)?;
        let backend = format!(
            "http://127.0.0.1:{}/lifetimes/{token}/{case}",
            origins.port(0)
        );
        origins.mark_lifetime_start(&token)?;
        let reply = net::header_request(
            port,
            &format!("/progress/lifetimes/{case}"),
            &token,
            &[("x-fixture-backend", &backend)],
        );
        let response_returned = Instant::now();
        let released = if pending_cutoff(case) {
            origins.release_lifetime(&token, response_returned)
        } else {
            Ok(())
        };
        let origin = origins.take_lifetime_events(&token)?;
        let mut report = Value::Null;
        let result = (|| -> Result<()> {
            let reply = reply.as_ref().map_err(|error| error.to_string())?;
            released.as_ref().map_err(|error| error.to_string())?;
            require(
                reply["status"] == 200,
                "lifetime guest returned an error response",
            )?;
            let bytes: Vec<u8> = serde_json::from_value(reply["body_bytes"].clone())?;
            report = serde_json::from_slice(&bytes)?;
            validate(case, &token, &report, &origin)
        })();
        records.push(json!({"event":"lifetime-case","case":case,"token":token,
            "status":if result.is_ok(){"pass"}else{"fail"},
            "error":result.as_ref().err().map(ToString::to_string),"report":report,
            "origin":origin,"body_phase_evidence":if case == "pending-body-cutoff" {
                Some("host-head-written-only")
            } else { None },"client":reply.as_ref().ok().map(|reply| json!({
                "status":reply["status"],"complete_ns":reply["complete_ns"],
                "header_ns":reply["header_ns"],"headers":reply["headers"]})),"qualification":false}))?;
        if let Err(error) = result {
            failures.push(format!("{case}: {error}"));
        }
    }
    require(failures.is_empty(), failures.join("; "))?;
    Ok(CASES.len())
}

fn pending_cutoff(case: &str) -> bool {
    matches!(case, "pending-head-cutoff" | "pending-body-cutoff")
}

fn validate(case: &str, token: &str, report: &Value, origin: &Value) -> Result<()> {
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
        "slow-origin" => &["a", "b", "c", "origin"],
        "fast-origin" => &["a", "b", "c", "mediator", "origin"],
        "begin-progress" => &["origin"],
        "batch-progress" => &["a", "b", "c", "quiet"],
        "pending-head-cutoff" | "pending-body-cutoff" => &["a", "b", "c"],
        _ => return Err("unknown lifetime case".into()),
    };
    require(
        roles == expected,
        "missing, duplicate or foreign origin receipt",
    )?;
    if pending_cutoff(case) {
        return validate_pending_cutoff(case, report, events, origin);
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
    if case == "slow-origin" {
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

fn validate_timeline(case: &str, events: &[Value], origin: &Value) -> Result<()> {
    let slow = case == "slow-origin";
    let head = event(events, "origin-head")?;
    within(
        number(head, "at_us")? + number(head, "duration_us")?,
        if slow { 700_000 } else { 120_000 },
        "origin head missed scenario time",
    )?;
    let body = event(events, "origin-body")?;
    within(
        number(body, "at_us")? + number(body, "duration_us")?,
        if slow { 1_300_000 } else { 260_000 },
        "origin EOF missed scenario time",
    )?;
    let origin = origin.as_array().unwrap();
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
    for (role, head_at, chunk_times, bytes) in [
        (
            "origin",
            if slow { 700_000 } else { 120_000 },
            if slow {
                vec![700_000, 900_000, 1_300_000]
            } else {
                vec![260_000]
            },
            if slow { b"Oo!".as_slice() } else { b"O" },
        ),
        ("a", 180_000, vec![180_000], b"A"),
        ("b", 320_000, vec![320_000], b"B"),
    ]
    .into_iter()
    .chain((!slow).then_some(("c", 400_000, vec![400_000], b"C".as_slice())))
    {
        let heads: Vec<_> = origin
            .iter()
            .filter(|entry| entry["op"] == "head" && entry["role"] == role)
            .collect();
        let chunks: Vec<_> = origin
            .iter()
            .filter(|entry| entry["op"] == "chunk" && entry["role"] == role)
            .collect();
        require(
            heads.len() == 1 && chunks.len() == chunk_times.len(),
            "missing or extra origin writes",
        )?;
        for (entry, scheduled) in std::iter::once((heads[0], head_at)).chain(
            chunks
                .iter()
                .zip(chunk_times)
                .map(|(entry, time)| (*entry, time)),
        ) {
            require(
                number(entry, "scenario_scheduled_us")? == scheduled
                    && entry["schedule_basis"] == "origin-receipt-minus-guest-at-us",
                "wrong origin scenario schedule",
            )?;
            let actual = number(entry, "at_us")?;
            let scheduled = number(entry, "scheduled_us")?;
            require(
                (i128::from(scheduled)
                    - aligned
                    - i128::from(number(entry, "scenario_scheduled_us")?))
                .abs()
                    <= 1,
                "host schedule was reanchored after registration",
            )?;
            require(actual >= scheduled, "origin wrote before scheduled time")?;
            within(actual, scheduled, "origin scheduling target missed")?;
        }
        for (entry, byte) in chunks.iter().zip(bytes) {
            require(entry["bytes"] == json!([byte]), "origin chunk bytes differ")?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pending_cutoff_sample(case: &str) -> (Value, Value) {
        let report = json!({"schema":1,"case":case,"token":"sample","qualification":false,
            "events":[
                {"op":"batch-start","at_us":34000,"duration_us":100,
                    "result":{"cutoff_at_us":200000}},
                {"op":"next","at_us":34100,"duration_us":166100,
                    "result":{"state":"Cutoff","first_poll_us":34110,"pending_polls":1}},
                {"op":"collect","at_us":200200,"duration_us":0,"result":"ok"}
            ],"items":[],"slots":["Unresolved","Unresolved","Unresolved"],"termination":"Cutoff",
            "error":null,"origin":null,"mediator":null});
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

    #[test]
    fn pending_head_and_body_cutoff_evidence_requires_actual_blocked_exchange() {
        for case in ["pending-head-cutoff", "pending-body-cutoff"] {
            let (report, origin) = pending_cutoff_sample(case);
            validate(case, "sample", &report, &origin).unwrap();
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
                    validate(case, "sample", &changed, &origin).is_err(),
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
                    validate(case, "sample", &report, &changed).is_err(),
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
            assert!(validate(case, "sample", &report, &changed).is_err());
            let mut changed = origin.clone();
            changed
                .as_array_mut()
                .unwrap()
                .push(json!({"op":"chunk","role":"a",
                "token":"sample","case":case,"at_us":50000,"bytes":[65]}));
            assert!(validate(case, "sample", &report, &changed).is_err());
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
            assert!(validate(case, "sample", &report, &changed).is_err());
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
        validate("pending-body-cutoff", "sample", &report, &origin).unwrap();
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
        assert!(validate("pending-body-cutoff", "sample", &report, &origin).is_err());
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
                    assert!(validate(case, "sample", &report, &origin).is_err());
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
                assert!(validate(case, "sample", &report, &origin).is_err());
                origin[release]
                    .as_object_mut()
                    .unwrap()
                    .remove("response_returned_us");
                assert!(validate(case, "sample", &report, &origin).is_err());
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
                {"op":"request","role":"a","at_us":35000,"guest_at_us":34000},
                {"op":"request","role":"b","at_us":35010,"guest_at_us":34000},
                {"op":"request","role":"c","at_us":35020,"guest_at_us":34000}
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
            for (role, head, chunks) in
                scripts
                    .into_iter()
                    .chain((!slow).then_some(("c", 400000, vec![(400000, 67)])))
            {
                origin
                    .as_array_mut()
                    .unwrap()
                    .push(json!({"op":"head","role":role,
                    "at_us":head+1000,"scheduled_us":head+1000,"scenario_scheduled_us":head,
                    "schedule_basis":"origin-receipt-minus-guest-at-us"}));
                for (time, byte) in chunks {
                    origin.as_array_mut().unwrap().push(
                        json!({"op":"chunk","role":role,"bytes":[byte],
                        "at_us":time+1000,"scheduled_us":time+1000,"scenario_scheduled_us":time,
                        "schedule_basis":"origin-receipt-minus-guest-at-us"}),
                    );
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

    #[test]
    fn progress_evidence_requires_actual_receipts_inside_unobserved_windows() {
        for case in ["begin-progress", "batch-progress"] {
            let (report, origin) = sample(case);
            validate(case, "sample", &report, &origin).unwrap();
            let mut late = origin.clone();
            late[0]["at_us"] = json!(999000);
            assert!(validate(case, "sample", &report, &late).is_err());
            assert!(validate(case, "sample", &report, &json!([])).is_err());
            let mut duplicate = origin.clone();
            duplicate.as_array_mut().unwrap().push(origin[0].clone());
            assert!(validate(case, "sample", &report, &duplicate).is_err());
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
                    validate(case, "sample", &invalid, &origin).is_err(),
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
            assert!(validate("batch-progress", "sample", &invalid, &origin).is_err());
        }
    }

    #[test]
    fn slow_and_fast_origin_reject_collection_time_elapsed_and_renewed_deadlines() {
        for case in ["slow-origin", "fast-origin"] {
            let (report, origin) = sample(case);
            validate(case, "sample", &report, &origin).unwrap();
            for index in 0..2 {
                let mut late = report.clone();
                late["items"][index]["elapsed_us"] = json!(667000);
                assert!(validate(case, "sample", &late, &origin).is_err());
                let mut wrong = report.clone();
                wrong["items"][index]["index"] = json!(2);
                assert!(validate(case, "sample", &wrong, &origin).is_err());
            }
            let mut states = report.clone();
            states["slots"][0] = json!("Unresolved");
            assert!(validate(case, "sample", &states, &origin).is_err());
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
            assert!(validate("fast-origin", "sample", &invalid, &origin).is_err());
        }
        let mut extended = report;
        extended["events"]
            .as_array_mut()
            .unwrap()
            .iter_mut()
            .find(|entry| entry["op"] == "mediator-send")
            .unwrap()["duration_us"] = json!(1000000);
        assert!(validate("fast-origin", "sample", &extended, &origin).is_err());
    }

    #[test]
    fn timeline_evidence_cannot_omit_heads_or_replace_actual_times_with_schedules() {
        for case in ["slow-origin", "fast-origin"] {
            let (report, origin) = sample(case);
            validate(case, "sample", &report, &origin).unwrap();
            for op in ["head", "chunk"] {
                let mut missing = origin.clone();
                missing
                    .as_array_mut()
                    .unwrap()
                    .retain(|entry| entry["op"] != op);
                assert!(
                    validate(case, "sample", &report, &missing).is_err(),
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
            assert!(validate(case, "sample", &report, &late).is_err());
            let mut reanchored = origin.clone();
            for entry in reanchored.as_array_mut().unwrap() {
                if entry.get("scheduled_us").is_some() {
                    entry["at_us"] = json!(number(entry, "at_us").unwrap() + 10_000);
                    entry["scheduled_us"] = json!(number(entry, "scheduled_us").unwrap() + 10_000);
                }
            }
            assert!(validate(case, "sample", &report, &reanchored).is_err());
        }
    }

    #[test]
    fn quiet_window_proof_survives_cold_entry_clock_offsets() {
        for case in ["begin-progress", "batch-progress"] {
            let (report, origin) = sample(case);
            validate(case, "sample", &report, &origin).unwrap();
            for offset in [100_000, 1_000_000] {
                let mut shifted = origin.clone();
                for entry in shifted.as_array_mut().unwrap() {
                    for field in ["at_us", "write_started_us"] {
                        if let Some(value) = entry[field].as_u64() {
                            entry[field] = json!(value + offset);
                        }
                    }
                }
                validate(case, "sample", &report, &shifted).unwrap();
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
                validate(case, "sample", &report, &threshold).unwrap();
                threshold[0]["at_us"] = json!(number(&threshold[0], "at_us").unwrap() + 1);
                assert!(validate(case, "sample", &report, &threshold).is_err());
                let mut invalid = shifted.clone();
                invalid[0]["token"] = json!("foreign");
                assert!(validate(case, "sample", &report, &invalid).is_err());
                if case == "batch-progress" {
                    let mut observed = report.clone();
                    observed["items"][0]["elapsed_us"] = json!(300_000);
                    assert!(validate(case, "sample", &observed, &shifted).is_err());
                }
                let mut recorded_late = shifted;
                let head = recorded_late
                    .as_array_mut()
                    .unwrap()
                    .iter_mut()
                    .find(|entry| entry["op"] == "head")
                    .unwrap();
                head["at_us"] = json!(number(head, "at_us").unwrap() + 50_000);
                validate(case, "sample", &report, &recorded_late).unwrap();
            }
        }
    }
}
