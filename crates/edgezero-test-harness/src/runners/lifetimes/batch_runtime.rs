use super::*;

pub(super) const CASES: [&str; 4] = [
    "batch-write-deadline",
    "batch-cancel",
    "body-check",
    "begin-upload-rules",
];
pub(super) const PREFIX: &[u8] = b"edgezero-lifetime-batch ";

fn prelude(case: &str, token: &str) -> Value {
    let mut value = json!({"schema":1,"case":case,"token":token,"phase":"batch-pending","slots":3,"qualification":false});
    if case == "batch-write-deadline" {
        value["cutoff_after_us"] = json!(3_000_000);
    } else {
        value["cutoff_at_us"] = json!(534_000);
        value["delivered"] = json!([0]);
        value["in_flight"] = json!([1, 2]);
    }
    value
}

fn decode(case: &str, token: &str, bytes: &[u8]) -> Result<Value> {
    require(bytes.len() <= 16 * 1024, "batch body exceeds bound")?;
    if case == "batch-cancel" {
        return Ok(serde_json::from_slice(bytes)?);
    }
    let mut lines = bytes.split(|byte| *byte == b'\n');
    let prefix = lines.next().ok_or("missing batch prefix")?;
    require(
        prefix.len() <= 512 && prefix == serde_json::to_vec(&prelude(case, token))?,
        "invalid batch prefix",
    )?;
    if case == "batch-write-deadline" {
        require(
            lines.next() == Some([].as_slice()) && lines.next().is_none(),
            "write deadline body was not pending",
        )?;
        return Ok(prelude(case, token));
    }
    let checks = [
        lines.next().ok_or("missing first body check")?,
        lines.next().ok_or("missing second body check")?,
    ];
    let report: Value = serde_json::from_slice(lines.next().ok_or("missing body report")?)?;
    require(
        lines.next() == Some([].as_slice()) && lines.next().is_none(),
        "extra body frames",
    )?;
    let events = report["events"].as_array().ok_or("missing body events")?;
    let observed: Vec<_> = events
        .iter()
        .filter(|event| event["op"] == "check")
        .collect();
    require(observed.len() == 2, "missing or extra body checks")?;
    for (bytes, event) in checks.into_iter().zip(observed) {
        require(
            bytes == serde_json::to_vec(event)?
                && event["result"] == "Pending"
                && number(event, "duration_us")? < 5_000,
            "body check blocked, delivered a slot, or changed on the wire",
        )?;
    }
    Ok(report)
}

fn validate_report(case: &str, token: &str, report: &Value) -> Result<()> {
    require(
        report["schema"] == 1
            && report["case"] == case
            && report["token"] == token
            && report["qualification"] == false
            && report.get("error") == Some(&Value::Null)
            && report["slots"] == json!([])
            && report.get("termination") == Some(&Value::Null),
        "invalid batch runtime report",
    )?;
    let events = report["events"]
        .as_array()
        .ok_or("missing batch runtime events")?;
    let expected = if case == "batch-cancel" {
        vec!["batch-start", "check", "barrier-ready", "cancel"]
    } else {
        vec!["batch-start", "barrier-ready", "next", "check", "check"]
    };
    require(
        events
            .iter()
            .map(|event| event["op"].as_str())
            .collect::<Vec<_>>()
            == expected.into_iter().map(Some).collect::<Vec<_>>(),
        "invalid batch observation sequence",
    )?;
    let start = &events[0];
    require(
        start["result"]["slots"] == 3
            && start["result"]["cutoff_at_us"] == 534000
            && start["result"]["per_call_timeout_us"] == 1000000
            && (34000..=39000).contains(&number(start, "at_us")?),
        "batch changed the three-slot 34/534/1000 ms defaults",
    )?;
    for pair in events.windows(2) {
        require(
            event_end(&pair[0])? <= number(&pair[1], "at_us")?,
            "batch events reordered",
        )?;
    }
    require(
        events
            .iter()
            .all(|entry| event_end(entry).is_ok_and(|at| at < 534000)),
        "observation lost to the original cutoff",
    )?;
    let items = report["items"]
        .as_array()
        .filter(|items| items.len() == 1)
        .ok_or("missing sole delivered slot")?;
    let item = &items[0];
    require(
        events[if case == "batch-cancel" { 2 } else { 1 }]["result"] == true,
        "missing pending source barrier",
    )?;
    if case == "batch-cancel" {
        let elapsed = number(item, "elapsed_us")?;
        let observed = event_end(&events[1])?
            .checked_sub(number(start, "at_us")?)
            .ok_or("preflight check ended before batch start")?;
        require(
            elapsed <= observed
                && events[1]["result"] == "Item"
                && number(&events[1], "duration_us")? < 5000
                && events[3]["result"] == json!([0, 1])
                && report["batch_state"]
                    == json!({
            "recorded_unobserved":0,"recorded_kind":"sdk-buffered-response","delivered":[2],"in_flight":[1],"cancelled":[0,1],"before_cancel":report["batch_state"]["before_cancel"]})
                && item
                    == &json!({"index":2,"via":"check","elapsed_us":elapsed,"slot_error":true,
                    "outcome":{"error":{"kind":"bad_request","status":400,"reason":null}}}),
            "cancel omitted the recorded or in-flight index",
        )?;
    } else {
        require(
            report["batch_state"] == json!({"delivered":[0],"in_flight":[1,2]})
                && events[2]["result"] == "Item"
                && item
                    == &json!({"index":0,"via":"next","elapsed_us":item["elapsed_us"],"slot_error":false,
                    "outcome":{"status":200,"bytes":b"A"}})
                && number(item, "elapsed_us")? < 500000,
            "body did not retain both slots",
        )?;
        for check in &events[3..] {
            require(
                check["result"] == "Pending" && number(check, "duration_us")? < 5_000,
                "body check blocked or delivered a slot",
            )?;
        }
    }
    Ok(())
}

fn validate_source(case: &str, token: &str, source: &Value) -> Result<()> {
    let events = source.as_array().ok_or("missing batch source events")?;
    require(
        events.len() <= 32
            && events
                .iter()
                .all(|event| event["token"] == token && event["case"] == case),
        "foreign or unbounded batch source events",
    )?;
    let roles: &[&str] = match case {
        "batch-cancel" => &["b"],
        "body-check" => &["b", "c"],
        _ => &["a", "b", "c"],
    };
    require(
        events.len() == roles.len() * 3 + 4 + if case == "batch-write-deadline" { 0 } else { 4 },
        "unaccounted batch source event",
    )?;
    let source_event = |role: &str, op: &str| -> Result<&Value> {
        let matches: Vec<_> = events
            .iter()
            .filter(|event| event["role"] == role && event["op"] == op)
            .collect();
        require(
            matches.len() == 1,
            "missing or duplicate batch source boundary",
        )?;
        Ok(matches[0])
    };
    let released = number(source_event("control", "release")?, "at_us")?;
    source_event("ready", "request")?;
    let ready = number(source_event("ready", "head")?, "at_us")?;
    let expected_requests = roles.len() + 1 + usize::from(case != "batch-write-deadline");
    require(
        events
            .iter()
            .filter(|event| event["op"] == "request")
            .count()
            == expected_requests,
        "unexpected batch source request (including rejected upload)",
    )?;
    source_event("ready", "closed")?;
    if case != "batch-write-deadline" {
        let request = source_event("a", "request")?;
        require(
            (34000..=39000).contains(&number(request, "guest_at_us")?),
            "completed slot did not use original batch start",
        )?;
        let head = number(source_event("a", "head")?, "at_us")?;
        let chunk = source_event("a", "chunk")?;
        let ended = source_event("a", "closed")?;
        require(
            head <= number(chunk, "at_us")?
                && number(chunk, "at_us")? <= number(ended, "at_us")?
                && number(ended, "at_us")? <= ready
                && ended["reason"] == "complete"
                && chunk["bytes"] == json!(b"A"),
            "A was not fully produced before the barrier",
        )?;
    }
    for role in roles {
        let request = source_event(role, "request")?;
        require(
            (34000..=39000).contains(&number(request, "guest_at_us")?),
            "slot did not use original batch start",
        )?;
        let waiting = number(source_event(role, "waiting")?, "at_us")?;
        let closed = source_event(role, "closed")?;
        require(
            waiting <= ready
                && ready <= released
                && released <= number(closed, "at_us")?
                && closed["reason"] == "fixture_released"
                && !events.iter().any(|event| {
                    event["role"] == *role && matches!(event["op"].as_str(), Some("head" | "chunk"))
                }),
            "batch slot was not held through the response",
        )?;
    }
    Ok(())
}

fn validate_terminal(adapter: &str, token: &str, record: &Value) -> Result<()> {
    let elapsed = number(record, "elapsed_us")?;
    require(
        number(record, "cutoff_at_us")? >= 3_000_000,
        "invalid batch cutoff measurement",
    )?;
    let outcome = record["outcome"]
        .as_str()
        .ok_or("missing terminal outcome")?;
    require(
        [
            "DeadlineExceeded",
            "SourceError",
            "TransportError",
            "RequestCancelled",
            "ClientDisconnected",
            "ConversionError",
            "Unspecified",
            "Completed",
            "HostHandoff",
        ]
        .contains(&outcome),
        "unknown terminal outcome",
    )?;
    let expected = json!({"schema":1,"event":"batch-terminal","case":"batch-write-deadline","token":token,
        "qualification":false,"outcome":outcome,"body_kind":"Application","bytes_written":record["prefix_bytes"],
        "prefix_bytes":record["prefix_bytes"],"elapsed_us":record["elapsed_us"],"pending_poll":true,"source_drops":1,
        "source_failure":record["source_failure"],"native_operations":record["native_operations"],"cutoff_at_us":record["cutoff_at_us"]});
    let prefix_bytes = serde_json::to_vec(&prelude("batch-write-deadline", token))?.len() + 1;
    require(
        record == &expected && record["prefix_bytes"] == prefix_bytes,
        "invalid terminal or source-release witness",
    )?;
    if adapter != "fastly" {
        require(
            outcome == "DeadlineExceeded"
                && record["source_failure"].is_null()
                && (1_000_000..=1_005_000).contains(&elapsed),
            "write deadline lost to batch cutoff",
        )?;
    }
    Ok(())
}

fn validate_native(
    adapter: &str,
    case: &str,
    backend: &str,
    native: &Value,
    cutoff: Option<u64>,
) -> Result<()> {
    if adapter == "fastly" && case == "batch-write-deadline" {
        return Ok(());
    }
    let slots = native
        .as_array()
        .filter(|slots| slots.len() == 3)
        .ok_or("missing per-target SDK observations")?;
    for (index, slot) in slots.iter().enumerate() {
        require(
            slot["index"] == index && slot["attachments"] == 1 && slot["invalid"] == false,
            "invalid per-target SDK identity",
        )?;
        if case.starts_with("batch-cancel") && index == 2 {
            require(
                slot["preparations"] == 0
                    && slot["acquisitions"] == 0
                    && slot["releases"] == 0
                    && slot["polls"] == 0
                    && slot["heads"] == 0
                    && slot["body_acquisitions"] == 0
                    && slot["body_releases"] == 0
                    && slot["target_uri"].is_null()
                    && slot.get("operation_observation") == Some(&Value::Null),
                "rejected slot reached the SDK",
            )?;
            continue;
        }
        let completed = case != "batch-write-deadline" && index == 0;
        let released = completed || case == "batch-cancel" || case == "batch-write-deadline";
        let polls = number(slot, "polls")?;
        let ready = number(slot, "ready_polls")?;
        let observation = &slot["operation_observation"];
        let valid_observation = match observation["kind"].as_str() {
            Some("SdkPoll") => {
                observation
                    .as_object()
                    .is_some_and(|fields| fields.len() == 4)
                    && polls > 0
                    && match observation["pending"].as_bool() {
                        Some(true) => polls > ready,
                        Some(false) => completed && polls == 1 && ready == 1,
                        None => false,
                    }
            }
            Some("HeadReadinessTimeout") => {
                adapter == "fastly"
                    && observation
                        .as_object()
                        .is_some_and(|fields| fields.len() == 3)
                    && number(observation, "at_us")? < number(observation, "observed_at_us")?
            }
            _ => false,
        };
        require(
            slot["preparations"] == 1
                && slot["target_uri"] == format!("{backend}/{}", ["a", "b", "c"][index])
                && slot["acquisitions"] == 1
                && ready == u64::from(completed)
                && polls >= ready
                && valid_observation
                && slot["releases"] == usize::from(released),
            "SDK operation was not pending or released exactly once",
        )?;
        if completed {
            require(
                number(slot, "ready_polls")? > 0
                    && slot["heads"] == 1
                    && slot["body_acquisitions"] == 1
                    && slot["body_releases"] == 1
                    && number(slot, "reads")? > 0
                    && number(slot, "ready_reads")? > 0,
                "A lacks SDK head, completed buffer and source release evidence",
            )?;
        } else {
            require(
                slot["heads"] == 0 && slot["body_acquisitions"] == 0 && slot["body_releases"] == 0,
                "pending slot reached a response head",
            )?;
        }
        if case != "batch-write-deadline" {
            require(
                slot["deadline_at_us"] == 534000 && slot["budget_cause"] == "BatchCutoff",
                "SDK preparation changed original cutoff or governing budget",
            )?;
        }
        let acquired = number(slot, "acquired_at_us")?;
        let polled = number(observation, "at_us")?;
        let observed = number(observation, "observed_at_us")?;
        require(
            acquired >= 34000
                && acquired <= polled
                && polled <= observed
                && cutoff.is_none_or(|cutoff| observed < cutoff),
            "SDK poll precedes acquisition",
        )?;
        if released {
            let ended = number(slot, "released_at_us")?;
            require(
                observed <= ended && cutoff.is_none_or(|cutoff| ended < cutoff),
                "SDK release lost to the cutoff",
            )?;
        } else {
            require(
                slot["released_at_us"].is_null(),
                "SDK slot released before body checks",
            )?;
        }
    }
    Ok(())
}

pub(super) fn check(
    adapter: &str,
    case: &str,
    port: u16,
    origins: &WireOrigins,
    log: &Path,
    cursor: &mut protocol::LogCursor,
    records: &mut Records,
) -> Result<()> {
    if case == "begin-upload-rules" {
        return check_upload(adapter, port, origins, records);
    }
    let token = token();
    origins.register_lifetime(&token, case)?;
    origins.mark_lifetime_start(&token)?;
    let backend = format!(
        "http://127.0.0.1:{}/lifetimes/{token}/{case}",
        origins.port(0)
    );
    let reply = net::header_request(
        port,
        &format!("/progress/lifetimes/{case}"),
        &token,
        &[("x-fixture-backend", &backend)],
        None,
    );
    let released = origins.release_lifetime(&token, Instant::now());
    let source = origins.take_lifetime_events(&token);
    let mut report = Value::Null;
    let mut terminal = Value::Null;
    let result = (|| -> Result<()> {
        released?;
        validate_source(case, &token, source.as_ref().map_err(ToString::to_string)?)?;
        let bytes = match &reply {
            Ok(reply) => {
                require(reply["status"] == 200, "batch runtime returned an error")?;
                serde_json::from_value::<Vec<u8>>(reply["body_bytes"].clone())?
            }
            Err(error) if case == "batch-write-deadline" => {
                let partial = error
                    .downcast_ref::<net::InterruptedResponse>()
                    .ok_or("missing deadline response head")?;
                require(
                    partial.status == 200
                        && matches!(
                            partial.cause,
                            net::ResponseInterruption::PrematureEof
                                | net::ResponseInterruption::PeerClosed
                        ),
                    "deadline response did not end at the peer",
                )?;
                partial.body.clone()
            }
            Err(error) => return Err(error.to_string().into()),
        };
        report = decode(case, &token, &bytes)?;
        if case == "batch-write-deadline" {
            let (observed, offset) = protocol::wait_validated(
                log,
                cursor,
                PREFIX,
                1,
                1,
                Instant::now() + Duration::from_secs(2),
                &|_, record| validate_terminal(adapter, &token, record),
            )?;
            terminal = observed[0].clone();
            cursor.offset = offset;
            validate_native(
                adapter,
                case,
                &backend,
                &terminal["native_operations"],
                Some(number(&terminal, "cutoff_at_us")?),
            )?;
        } else {
            validate_report(case, &token, &report)?;
            if case == "batch-cancel" {
                let before = &report["batch_state"]["before_cancel"];
                validate_native(
                    adapter,
                    "batch-cancel-recorded",
                    &backend,
                    before,
                    Some(534000),
                )?;
                let at = number(
                    event(
                        report["events"].as_array().ok_or("missing cancel events")?,
                        "cancel",
                    )?,
                    "at_us",
                )?;
                require(
                    before[0] == report["native_body"][0]
                        && before[2] == report["native_body"][2]
                        && before[1]["polls"] == report["native_body"][1]["polls"]
                        && number(&before[0], "released_at_us")? <= at
                        && number(&before[1]["operation_observation"], "observed_at_us")? <= at,
                    "cancel observed A or polled the pending SDK operation",
                )?;
            }
            validate_native(
                adapter,
                case,
                &backend,
                &report["native_body"],
                Some(534000),
            )?;
        }
        Ok(())
    })();
    records.push(json!({"event":"lifetime-case","case":case,"adapter":adapter,"token":token,
        "status":if result.is_err(){"fail"}else if case == "batch-write-deadline" && adapter == "fastly"{"recorded"}else{"pass"},
        "contract":if case == "batch-write-deadline" && adapter == "fastly"{"SHOULD"}else{"MUST"},
        "report":report,"terminal":terminal,"origin":source.as_ref().ok(),"source_error":source.as_ref().err().map(ToString::to_string),
        "error":result.as_ref().err().map(ToString::to_string),"qualification":false}))?;
    result
}

fn validate_upload(token: &str, report: &Value, receipts: &[Value]) -> Result<()> {
    require(
        report
            == &json!({"schema":1,"case":"begin-upload-rules","token":token,"qualification":false,
        "begin_post":{"kind":"bad_request","status":400,"reason":null},"begin_post_polls":0,
        "begin_get":{"status":201,"bytes":b"end"},"send_post":{"status":201,"bytes":b"end"},"send_post_polls":1}),
        "Begin/send upload rules did not hold independently",
    )?;
    require(receipts.len() == 2, "wrong upload receipt count")?;
    for (index, receipt) in receipts.iter().enumerate() {
        net::validate_forward_capture(receipt)?;
        require(
            receipt["operation"] == "send"
                && receipt["token"] == token
                && receipt["target"] == format!("/forward/send/{token}")
                && receipt["method"] == ["GET", "POST"][index]
                && receipt["payload"] == ["", "POST"][index],
            "rejected Begin dispatched or actual GET/POST changed",
        )?;
    }
    Ok(())
}

fn check_upload(
    adapter: &str,
    port: u16,
    origins: &WireOrigins,
    records: &mut Records,
) -> Result<()> {
    let token = token();
    let before = origins.accepted();
    let backend = format!(
        "http://127.0.0.1:{}/lifetimes/{token}/begin-upload-rules",
        origins.port(0)
    );
    let reply = net::header_request(
        port,
        "/progress/lifetimes/begin-upload-rules",
        &token,
        &[("x-fixture-backend", &backend)],
        None,
    );
    let mut report = Value::Null;
    let mut receipts = Vec::new();
    let result = (|| -> Result<()> {
        let reply = reply.as_ref().map_err(ToString::to_string)?;
        require(reply["status"] == 200, "upload rules route failed")?;
        let bytes: Vec<u8> = serde_json::from_value(reply["body_bytes"].clone())?;
        report = serde_json::from_slice(&bytes)?;
        for _ in 0..2 {
            receipts.push(origins.take_wire()?);
        }
        require(
            origins.accepted() == before + 2,
            "rejected Begin opened a transport",
        )?;
        validate_upload(&token, &report, &receipts)
    })();
    records.push(
        json!({"event":"lifetime-case","case":"begin-upload-rules","adapter":adapter,"token":token,
        "status":if result.is_ok(){"pass"}else{"fail"},"report":report,"origin":receipts,
        "error":result.as_ref().err().map(ToString::to_string),"qualification":false}),
    )?;
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normative_defaults_reject_two_slot_three_second_shortcuts() {
        for case in ["batch-cancel", "body-check"] {
            let valid = body_report(case);
            validate_report(case, "test", &valid).unwrap();
            for (field, value) in [
                ("slots", json!(2)),
                ("cutoff_at_us", json!(3000000)),
                ("per_call_timeout_us", json!(5000000)),
            ] {
                let mut bad = valid.clone();
                bad["events"][0]["result"][field] = value;
                assert!(validate_report(case, "test", &bad).is_err());
            }
            let mut early = valid;
            early["events"][0]["at_us"] = json!(0);
            assert!(validate_report(case, "test", &early).is_err());
        }
        assert_eq!(prelude("batch-write-deadline", "test")["slots"], 3);
        assert_eq!(prelude("body-check", "test")["cutoff_at_us"], 534000);
    }

    fn body_report(case: &str) -> Value {
        let cancel = case == "batch-cancel";
        let mut events = vec![
            json!({"op":"batch-start","at_us":34000,"duration_us":10,"result":{"cutoff_at_us":534000,"slots":3,"per_call_timeout_us":1000000}}),
        ];
        if cancel {
            events.push(json!({"op":"check","at_us":34010,"duration_us":1,"result":"Item"}));
            events.push(json!({"op":"barrier-ready","at_us":36000,"duration_us":0,"result":true}));
            events.push(json!({"op":"cancel","at_us":36010,"duration_us":0,"result":[0,1]}));
        } else {
            events.push(json!({"op":"barrier-ready","at_us":36000,"duration_us":0,"result":true}));
            events.push(json!({"op":"next","at_us":36000,"duration_us":1,"result":"Item"}));
            for at in [36010, 36020] {
                events.push(json!({"op":"check","at_us":at,"duration_us":1,"result":"Pending"}));
            }
        }
        let item = if cancel {
            json!({"index":2,"via":"check","elapsed_us":0,"slot_error":true,
            "outcome":{"error":{"kind":"bad_request","status":400,"reason":null}}})
        } else {
            json!({"index":0,"via":"next","elapsed_us":1000,"slot_error":false,"outcome":{"status":200,"bytes":b"A"}})
        };
        json!({"schema":1,"case":case,"token":"test","qualification":false,"events":events,"error":null,"items":[item],"slots":[],"termination":null,
            "batch_state":if cancel { json!({"recorded_unobserved":0,"recorded_kind":"sdk-buffered-response","delivered":[2],"in_flight":[1],"cancelled":[0,1],"before_cancel":[]}) }
            else {json!({"delivered":[0],"in_flight":[1,2]})}})
    }

    #[test]
    fn cancellation_requires_both_never_delivered_indices_without_observation() {
        let report = body_report("batch-cancel");
        validate_report("batch-cancel", "test", &report).unwrap();
        for indices in [json!([0]), json!([1]), json!([1, 0]), json!([0, 1, 1])] {
            let mut bad = report.clone();
            bad["batch_state"]["cancelled"] = indices;
            assert!(validate_report("batch-cancel", "test", &bad).is_err());
        }
        let mut bad = report;
        bad["events"][3]["op"] = json!("check");
        assert!(validate_report("batch-cancel", "test", &bad).is_err());
    }

    #[test]
    fn cancellation_preflight_elapsed_is_measured_within_the_original_check() {
        let mut report = body_report("batch-cancel");
        report["events"][0]["at_us"] = json!(35486);
        report["events"][0]["duration_us"] = json!(15);
        report["events"][1]["at_us"] = json!(35503);
        report["events"][1]["duration_us"] = json!(2);
        report["events"][2]["at_us"] = json!(36069);
        report["events"][3]["at_us"] = json!(36083);
        report["events"][3]["duration_us"] = json!(7);
        report["items"][0]["elapsed_us"] = json!(6);
        validate_report("batch-cancel", "test", &report).unwrap();

        for elapsed in [0, 6, 19] {
            let mut valid = report.clone();
            valid["items"][0]["elapsed_us"] = json!(elapsed);
            validate_report("batch-cancel", "test", &valid).unwrap();
        }
        for elapsed in [
            Value::Null,
            json!(20),
            json!(-1),
            json!("6"),
            json!(6.5),
            json!(u64::MAX),
        ] {
            let mut invalid = report.clone();
            invalid["items"][0]["elapsed_us"] = elapsed;
            assert!(validate_report("batch-cancel", "test", &invalid).is_err());
        }
        let mut missing = report;
        missing["items"][0]
            .as_object_mut()
            .unwrap()
            .remove("elapsed_us");
        assert!(validate_report("batch-cancel", "test", &missing).is_err());
        validate_report("batch-cancel", "test", &body_report("batch-cancel")).unwrap();
    }

    #[test]
    fn streamed_checks_require_two_pending_results_strictly_under_five_milliseconds() {
        let report = body_report("body-check");
        validate_report("body-check", "test", &report).unwrap();
        for (field, value) in [("result", json!("Item")), ("duration_us", json!(5000))] {
            let mut bad = report.clone();
            bad["events"][4][field] = value;
            assert!(validate_report("body-check", "test", &bad).is_err());
        }
        let encode = |report: &Value| {
            let lines = [
                prelude("body-check", "test"),
                report["events"][3].clone(),
                report["events"][4].clone(),
                report.clone(),
            ];
            lines
                .iter()
                .flat_map(|line| {
                    let mut bytes = serde_json::to_vec(line).unwrap();
                    bytes.push(b'\n');
                    bytes
                })
                .collect::<Vec<_>>()
        };
        let bytes = encode(&report);
        assert_eq!(decode("body-check", "test", &bytes).unwrap(), report);
        for bad in [
            &bytes[..bytes.len() - 1],
            &[bytes.clone(), b"{}\n".to_vec()].concat(),
            &vec![b'x'; 16385],
        ] {
            assert!(decode("body-check", "test", bad).is_err());
        }
    }

    #[test]
    fn deadline_requires_a_real_terminal_and_source_release_but_fastly_records_should() {
        let bytes = serde_json::to_vec(&prelude("batch-write-deadline", "test"))
            .unwrap()
            .len()
            + 1;
        let report = json!({"schema":1,"event":"batch-terminal","case":"batch-write-deadline","token":"test",
            "qualification":false,"outcome":"DeadlineExceeded","body_kind":"Application","bytes_written":bytes,"prefix_bytes":bytes,
            "elapsed_us":1000000,"pending_poll":true,"source_drops":1,"source_failure":null,"native_operations":null,"cutoff_at_us":3000000});
        for adapter in ["axum", "cloudflare", "spin", "fastly"] {
            validate_terminal(adapter, "test", &report).unwrap();
        }
        for (field, value) in [
            ("outcome", json!("SourceError")),
            ("elapsed_us", json!(3000000)),
            ("elapsed_us", json!(1005001)),
            ("elapsed_us", json!(999999)),
            ("pending_poll", json!(false)),
            ("source_drops", json!(0)),
            ("token", json!("foreign")),
        ] {
            let mut bad = report.clone();
            bad[field] = value;
            assert!(validate_terminal("axum", "test", &bad).is_err());
        }
        let mut should = report;
        should["outcome"] = json!("SourceError");
        should["source_failure"] = json!("internal");
        should["elapsed_us"] = json!(3000000);
        validate_terminal("fastly", "test", &should).unwrap();
    }

    #[test]
    fn pending_source_requires_exact_receipts_and_rejects_unaccounted_events() {
        let case = "body-check";
        let entry =
            |role, op, at| json!({"role":role,"op":op,"at_us":at,"case":case,"token":"test"});
        let mut entries = Vec::new();
        for role in ["b", "c"] {
            let mut request = entry(role, "request", 0);
            request["guest_at_us"] = json!(34000);
            entries.push(request);
            entries.push(entry(role, "waiting", 1));
            let mut closed = entry(role, "closed", 4);
            closed["reason"] = json!("fixture_released");
            entries.push(closed);
        }
        let mut request = entry("a", "request", 0);
        request["guest_at_us"] = json!(34000);
        entries.push(request);
        entries.push(entry("a", "head", 1));
        let mut chunk = entry("a", "chunk", 1);
        chunk["bytes"] = json!(b"A");
        entries.push(chunk);
        let mut closed = entry("a", "closed", 1);
        closed["reason"] = json!("complete");
        entries.push(closed);
        for (op, at) in [("request", 0), ("head", 2), ("closed", 2)] {
            entries.push(entry("ready", op, at));
        }
        entries.push(entry("control", "release", 3));
        let source = json!(entries);
        validate_source(case, "test", &source).unwrap();
        let mut extra = source.clone();
        extra
            .as_array_mut()
            .unwrap()
            .push(entry("foreign", "ignored", 0));
        assert!(validate_source(case, "test", &extra).is_err());
        let mut late = source.clone();
        late[1]["at_us"] = json!(5);
        assert!(validate_source(case, "test", &late).is_err());
        let mut missing = source;
        missing.as_array_mut().unwrap().remove(10);
        assert!(validate_source(case, "test", &missing).is_err());
    }

    #[test]
    fn native_head_wait_is_distinct_from_a_fabricated_sdk_poll() {
        let backend = "http://127.0.0.1:1234/lifetimes/test/body-check";
        let values = json!((0..3).map(|index| {
            let complete=index==0;
            json!({"index":index,"attachments":1,"preparations":1,"invalid":false,
                "target_uri":format!("{backend}/{}",["a","b","c"][index]),
                "budget_cause":"BatchCutoff","deadline_at_us":534000,
                "acquisitions":1,"polls":u64::from(complete),"ready_polls":u64::from(complete),
                "releases":u64::from(complete),"heads":u64::from(complete),
                "body_acquisitions":u64::from(complete),"body_releases":u64::from(complete),
                "reads":u64::from(complete),"ready_reads":u64::from(complete),
                "operation_observation":if complete {
                    json!({"kind":"SdkPoll","pending":false,"at_us":35000,"observed_at_us":35001})
                } else {json!({"kind":"HeadReadinessTimeout","at_us":35000,"observed_at_us":36000})},
                "acquired_at_us":34000,"released_at_us":if complete {Some(36001)}else{None}})
        }).collect::<Vec<_>>());
        validate_native("fastly", "body-check", backend, &values, Some(534000)).unwrap();
        for adapter in ["axum", "cloudflare", "spin"] {
            assert!(
                validate_native(adapter, "body-check", backend, &values, Some(534000)).is_err()
            );
        }
        for (field, value) in [
            ("kind", json!("SdkPoll")),
            ("kind", json!("BodyReadinessTimeout")),
            ("kind", json!("OtherReady")),
            ("at_us", json!(36001)),
            ("observed_at_us", json!(35000)),
            ("pending", json!(true)),
        ] {
            let mut invalid = values.clone();
            invalid[1]["operation_observation"][field] = value;
            assert!(
                validate_native("fastly", "body-check", backend, &invalid, Some(534000)).is_err(),
                "{field}"
            );
        }
        for field in ["operation_observation", "acquired_at_us"] {
            let mut missing = values.clone();
            missing[1].as_object_mut().unwrap().remove(field);
            assert!(
                validate_native("fastly", "body-check", backend, &missing, Some(534000)).is_err()
            );
        }
        let mut fake = values;
        fake[1]["operation_observation"] =
            json!({"kind":"SdkPoll","pending":true,"at_us":35000,"observed_at_us":36000});
        assert!(validate_native("fastly", "body-check", backend, &fake, Some(534000)).is_err());
    }

    #[test]
    fn native_operation_proof_binds_each_target_and_rejects_cutoff_equality() {
        let backend = "http://127.0.0.1:1234/lifetimes/test/batch-write-deadline";
        let slots = json!(
            (0..3)
                .map(
                    |index| json!({"index":index,"attachments":1,"preparations":1,
            "target_uri":format!("{backend}/{}",["a","b","c"][index]),"invalid":false,
            "acquisitions":1,"polls":2,"ready_polls":0,"releases":1,
            "heads":0,"body_acquisitions":0,"body_releases":0,"reads":0,"ready_reads":0,"budget_cause":"PerCallTimeout","deadline_at_us":1034000,
            "operation_observation":{"kind":"SdkPoll","pending":true,"at_us":34001,"observed_at_us":34002},
            "acquired_at_us":34000,"released_at_us":1000010})
                )
                .collect::<Vec<_>>()
        );
        validate_native(
            "axum",
            "batch-write-deadline",
            backend,
            &slots,
            Some(3000000),
        )
        .unwrap();
        for (field, value) in [
            ("target_uri", json!("http://127.0.0.1:1234/foreign")),
            ("released_at_us", json!(3000000)),
            ("releases", json!(2)),
            ("ready_polls", json!(1)),
            ("acquisitions", json!(0)),
        ] {
            let mut bad = slots.clone();
            bad[1][field] = value;
            assert!(
                validate_native("axum", "batch-write-deadline", backend, &bad, Some(3000000))
                    .is_err()
            );
        }
        let mut live = slots;
        for (index, slot) in live.as_array_mut().unwrap().iter_mut().enumerate() {
            slot["budget_cause"] = json!("BatchCutoff");
            slot["deadline_at_us"] = json!(534000);
            if index == 0 {
                slot["heads"] = json!(1);
                slot["body_acquisitions"] = json!(1);
                slot["body_releases"] = json!(1);
                slot["ready_polls"] = json!(1);
                slot["released_at_us"] = json!(36000);
                slot["reads"] = json!(2);
                slot["ready_reads"] = json!(2);
            } else {
                slot["releases"] = json!(0);
                slot["released_at_us"] = Value::Null;
            }
        }
        validate_native("spin", "body-check", backend, &live, None).unwrap();
        let mut recorded = live.clone();
        recorded[2] = json!({"index":2,"attachments":1,"preparations":0,"invalid":false,"acquisitions":0,"polls":0,
            "releases":0,"heads":0,"body_acquisitions":0,"body_releases":0,"target_uri":null,"operation_observation":null});
        validate_native(
            "axum",
            "batch-cancel-recorded",
            backend,
            &recorded,
            Some(534000),
        )
        .unwrap();
        for (field, value) in [
            ("preparations", json!(0)),
            ("body_releases", json!(0)),
            ("ready_reads", json!(0)),
        ] {
            let mut bad = recorded.clone();
            bad[0][field] = value;
            assert!(
                validate_native("axum", "batch-cancel-recorded", backend, &bad, Some(534000))
                    .is_err()
            );
        }
        live[1]["releases"] = json!(1);
        assert!(validate_native("cloudflare", "body-check", backend, &live, None).is_err());
    }

    #[test]
    fn terminal_protocol_is_canonical_bounded_and_rejects_duplicate_or_partial_tails() {
        let bytes = serde_json::to_vec(&prelude("batch-write-deadline", "test"))
            .unwrap()
            .len()
            + 1;
        let record = json!({"schema":1,"event":"batch-terminal","case":"batch-write-deadline","token":"test",
            "qualification":false,"outcome":"DeadlineExceeded","body_kind":"Application","bytes_written":bytes,"prefix_bytes":bytes,
            "elapsed_us":1000000,"pending_poll":true,"source_drops":1,"source_failure":null,"native_operations":null,"cutoff_at_us":3000000});
        let mut valid = PREFIX.to_vec();
        valid.extend(serde_json::to_vec(&record).unwrap());
        valid.push(b'\n');
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("terminal.log");
        let read = || {
            protocol::wait_validated(
                &path,
                &protocol::LogCursor::default(),
                PREFIX,
                1,
                1,
                Instant::now() + Duration::from_secs(2),
                &|_, record| validate_terminal("axum", "test", record),
            )
        };
        fs::write(&path, &valid).unwrap();
        assert_eq!(read().unwrap().0, [record]);
        for extra in [
            valid.clone(),
            b"edgezero-lifetime-batch {".to_vec(),
            b"edgezero-lifetime-ba".to_vec(),
        ] {
            fs::write(&path, [valid.clone(), extra].concat()).unwrap();
            assert!(read().is_err());
        }
        fs::write(&path, vec![b'x'; protocol::LOG_LIMIT + 1]).unwrap();
        assert!(read().is_err());
        fs::write(&path, [PREFIX, b"{\"noncanonical\": true}\n"].concat()).unwrap();
        assert!(read().is_err());

        let token = "a".repeat(64);
        let prefix_bytes = serde_json::to_vec(&prelude("batch-write-deadline", &token))
            .unwrap()
            .len()
            + 1;
        let mut maximum = json!({"schema":1,"event":"batch-terminal","case":"batch-write-deadline","token":token,
            "qualification":false,"outcome":"DeadlineExceeded","body_kind":"Application","bytes_written":prefix_bytes,
            "prefix_bytes":prefix_bytes,"elapsed_us":1005000,"pending_poll":true,"source_drops":1,
            "source_failure":null,"cutoff_at_us":3039000});
        maximum["native_operations"] = json!((0..3).map(|index|json!({
            "index":index,"attachments":1,"preparations":1,"invalid":false,
            "target_uri":format!("http://127.0.0.1:65535/lifetimes/{token}/batch-write-deadline/{}",["a","b","c"][index]),
            "budget_cause":"PerCallTimeout","deadline_at_us":1039000,"acquisitions":1,"polls":u64::MAX,"ready_polls":0,"releases":1,
            "heads":0,"body_acquisitions":0,"body_releases":0,"reads":0,"ready_reads":0,
            "operation_observation":{"kind":"SdkPoll","pending":true,"at_us":39000,"observed_at_us":39001},"acquired_at_us":39000,"released_at_us":1005000
        })).collect::<Vec<_>>());
        assert!(serde_json::to_vec(&maximum).unwrap().len() <= 2048);
        validate_terminal("axum", &token, &maximum).unwrap();
    }

    #[test]
    fn upload_rules_require_three_independent_api_results_and_two_actual_wire_receipts() {
        let report = json!({"schema":1,"case":"begin-upload-rules","token":"test","qualification":false,
            "begin_post":{"kind":"bad_request","status":400,"reason":null},"begin_post_polls":0,
            "begin_get":{"status":201,"bytes":b"end"},"send_post":{"status":201,"bytes":b"end"},"send_post_polls":1});
        let receipts: Vec<_> = ["GET","POST"].into_iter().map(|method| json!({"operation":"send","token":"test",
            "target":"/forward/send/test","method":method,"payload":if method == "GET" {""} else {"POST"},
            "content_length":if method == "GET" {json!([])} else {json!(["4"])},"transfer_encoding":[],
            "encoded_body_hex":if method == "GET" {""} else {"504f5354"}})).collect();
        validate_upload("test", &report, &receipts).unwrap();
        for field in ["begin_post_polls", "send_post_polls"] {
            let mut bad = report.clone();
            bad[field] = json!(2);
            assert!(validate_upload("test", &bad, &receipts).is_err());
        }
        let mut wrong = receipts.clone();
        wrong.reverse();
        assert!(validate_upload("test", &report, &wrong).is_err());
        assert!(validate_upload("test", &report, &receipts[..1]).is_err());
        let mut extra = receipts.clone();
        extra.push(receipts[1].clone());
        assert!(validate_upload("test", &report, &extra).is_err());
        let mut wrong = receipts;
        wrong[1]["payload"] = json!("");
        assert!(validate_upload("test", &report, &wrong).is_err());
    }
}
