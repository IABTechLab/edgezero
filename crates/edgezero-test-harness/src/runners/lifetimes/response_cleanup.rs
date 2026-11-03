use super::*;

pub(super) const PREFIX: &[u8] = b"edgezero-response-owned-cleanup ";
pub(super) const CASES: [&str; 5] = [
    "response-owned-cleanup-complete",
    "response-owned-cleanup-disconnect",
    "response-owned-cleanup-write-deadline",
    "response-owned-cleanup-source-error",
    "response-owned-cleanup-cancel",
];

fn validate_native(value: &Value, closed: bool) -> Result<()> {
    let observations = value
        .as_array()
        .filter(|values| matches!(values.len(), 1 | 4))
        .ok_or("missing original SDK owners")?;
    for (observation, role) in observations.iter().zip(["origin", "a", "b", "c"]) {
        let origin = role == "origin";
        require(
            observation["role"] == role
                && observation["invalid"] == false
                && number(observation, "attachments")? == 1
                && number(observation, "preparations")? == 1
                && number(observation, "acquisitions")? == 1
                && number(observation, "polls")? >= 1
                && number(observation, "ready_polls")? == u64::from(origin)
                && number(observation, "releases")? == u64::from(origin || closed)
                && number(observation, "heads")? == u64::from(origin)
                && number(observation, "body_acquisitions")? == u64::from(origin)
                && number(observation, "body_releases")? == u64::from(origin && closed)
                && number(observation, "ready_reads")? == 0,
            "invalid actual SDK operation/source ownership",
        )?;
        require(
            observation["first_pending"].is_boolean()
                && (origin || observation["first_pending"] == true)
                && if origin {
                    observation["read_pending"] == true && number(observation, "reads")? > 0
                } else {
                    observation.get("read_pending") == Some(&Value::Null)
                        && number(observation, "reads")? == 0
                },
            "pending source proof was substituted with a queued task or ready head",
        )?;
        let deadline = number(observation, "deadline_at_us")?;
        let entered = number(observation, "entered_at_us")?;
        require(
            if origin {
                observation["budget_cause"] == "PerCallTimeout"
                    && entered.checked_add(15_000_000) == Some(deadline)
            } else {
                observation["budget_cause"] == "BatchCutoff"
                    && deadline == 534_000
                    && (29_000..=39_000).contains(&entered)
            },
            "original response-owned setup budgets or batch entry were substituted",
        )?;
        for (field, released) in [
            ("released_at_us", origin || closed),
            ("body_released_at_us", origin && closed),
        ] {
            if released {
                let at = number(observation, field)?;
                require(
                    at >= entered && at < deadline,
                    "SDK release waited for its own timeout",
                )?;
            } else {
                require(
                    observation.get(field) == Some(&Value::Null),
                    "unreleased SDK resource has a release timestamp",
                )?;
            }
        }
    }
    Ok(())
}

fn prefix(case: &str, token: &str, isolation: bool) -> Result<Vec<u8>> {
    let mut bytes = serde_json::to_vec(&json!({"schema":1,"case":case,"token":token,
        "phase":"pending","isolation":isolation,"qualification":false}))?;
    bytes.push(b'\n');
    require(
        bytes.len() <= 512,
        "response cleanup prefix exceeds existing capture bound",
    )?;
    Ok(bytes)
}

fn pending_source(case: &str) -> bool {
    matches!(
        case,
        "response-owned-cleanup-cancel" | "response-owned-cleanup-write-deadline"
    )
}

fn coordination_remaining(deadline: Instant) -> Result<Duration> {
    deadline
        .checked_duration_since(Instant::now())
        .filter(|remaining| !remaining.is_zero())
        .ok_or_else(|| "cleanup coordination deadline expired".into())
}

fn specs(case: &str, victim: &str, survivor: &str) -> Vec<(String, String, String)> {
    let mut result = Vec::new();
    let mut append = |case: &str, token: &str, stage: String| {
        result.push((case.to_owned(), token.to_owned(), stage))
    };
    for (case, token, owners) in [(CASES[0], survivor, 1), (case, victim, 4)] {
        for index in 0..owners {
            append(case, token, format!("live-native-{index}"));
        }
        append(case, token, "live".into());
    }
    if pending_source(case) {
        append(case, victim, "source-pending".into());
    }
    for index in 0..4 {
        append(case, victim, format!("closed-native-{index}"));
    }
    append(case, victim, "collected".into());
    append(case, victim, "closed".into());
    for index in 0..1 {
        append(CASES[0], survivor, format!("isolated-native-{index}"));
    }
    append(CASES[0], survivor, "isolated".into());
    for index in 0..1 {
        append(CASES[0], survivor, format!("closed-native-{index}"));
    }
    append(CASES[0], survivor, "collected".into());
    append(CASES[0], survivor, "closed".into());
    result
}

fn validate_header(value: &Value, expected: &(String, String, String)) -> Result<()> {
    require(
        value.as_object().is_some_and(|object| object.len() == 7)
            && value["schema"] == 1
            && value["event"] == "response-owned-cleanup"
            && value["qualification"] == false
            && value["case"] == expected.0
            && value["token"] == expected.1
            && value["stage"] == expected.2
            && value["evidence"].is_object(),
        "missing, foreign, duplicate or reordered response cleanup stage",
    )
}

fn retain_stage(
    records: &mut Vec<Value>,
    index: usize,
    record: &Value,
    maximum: usize,
) -> Result<()> {
    require(index < maximum, "cleanup stage exceeds bound")?;
    if index == records.len() {
        records.push(record.clone());
    } else {
        require(
            records.get(index) == Some(record),
            "validated cleanup stage changed or skipped",
        )?;
    }
    Ok(())
}

fn native_group(records: &[Value], start: usize, closed: bool, owners: usize) -> Result<Value> {
    require(matches!(owners, 1 | 4), "invalid SDK owner count")?;
    let values = records
        .get(start..start + owners)
        .ok_or("missing SDK ownership stages")?
        .iter()
        .map(|record| record["evidence"].clone())
        .collect::<Vec<_>>();
    let value = json!(values);
    validate_native(&value, closed)?;
    Ok(value)
}

fn validate_progress(live: &Value, later: &Value) -> Result<()> {
    let live = live.as_array().ok_or("missing live SDK observations")?;
    let later = later.as_array().ok_or("missing later SDK observations")?;
    require(
        matches!(live.len(), 1 | 4) && later.len() == live.len(),
        "missing SDK owners",
    )?;
    for (first, last) in live.iter().zip(later) {
        require(
            first["role"] == last["role"]
                && first["deadline_at_us"] == last["deadline_at_us"]
                && first["entered_at_us"] == last["entered_at_us"]
                && first["budget_cause"] == last["budget_cause"]
                && first["first_pending"] == last["first_pending"]
                && first["read_pending"] == last["read_pending"],
            "original SDK ownership identity or deadline changed",
        )?;
        for field in ["polls", "reads"] {
            require(
                number(last, field)? >= number(first, field)?,
                "original SDK counters regressed",
            )?;
        }
        if first["role"] == "origin" {
            require(
                first["released_at_us"] == last["released_at_us"],
                "original SDK head release changed",
            )?;
        }
    }
    Ok(())
}

fn validate_closed(
    records: &[Value],
    live_start: usize,
    closed_start: usize,
    owners: usize,
    case: &str,
    prefix_bytes: usize,
) -> Result<()> {
    if pending_source(case) {
        require(
            records.get(closed_start - 1).is_some_and(|record| {
                record["evidence"] == json!({"pending":true,"after_prefix":true})
            }),
            "terminal case lacks an actual post-prefix Pending observation",
        )?;
    }
    let live = native_group(records, live_start, false, owners)?;
    let before = native_group(records, closed_start, true, owners)?;
    validate_progress(&live, &before)?;
    let collected = &records
        .get(closed_start + owners)
        .ok_or("missing retained-handle collection")?["evidence"];
    require(
        collected["scope_closed"] == true
            && collected["handles_retained"] == true
            && collected.get("batch_closed")
                == Some(&if owners == 4 {
                    json!(true)
                } else {
                    Value::Null
                })
            && collected["polls_stable"] == true
            && matches!(
                collected["origin_closed"].as_str(),
                Some("Internal" | "Eof")
            ),
        "original public handles did not remain retained through native cleanup",
    )?;
    let after = collected["after"]
        .as_array()
        .filter(|values| values.len() == owners)
        .ok_or("missing post-collection SDK observations")?;
    for (first, last) in before
        .as_array()
        .ok_or("missing SDK array")?
        .iter()
        .zip(after)
    {
        require(
            last["invalid"] == false,
            "post-terminal SDK recorder invalid",
        )?;
        for field in [
            "acquisitions",
            "polls",
            "releases",
            "reads",
            "body_acquisitions",
            "body_releases",
        ] {
            require(
                last.get(field) == first.get(field) && last.get(field).is_some(),
                "consuming a retained handle resumed or reacquired SDK work",
            )?;
        }
    }
    let closed = &records
        .get(closed_start + owners + 1)
        .ok_or("missing terminal response cleanup")?["evidence"];
    require(
        closed["cleanup"] == *collected
            && number(closed, "source_drops")? == 1
            && number(closed, "source_polls")? > 0
            && closed["terminal"]["body_kind"] == "Application"
            && closed["terminal"]["same_request_start"] == true,
        "invalid terminal cleanup or request clock",
    )?;
    let terminal = &closed["terminal"];
    let outcome = terminal["outcome"]
        .as_str()
        .ok_or("missing native egress outcome")?;
    let expected = match case {
        "response-owned-cleanup-complete" => outcome == "HostHandoff",
        "response-owned-cleanup-source-error" => {
            outcome == "SourceError" && terminal["source_failure"] == "internal"
        }
        "response-owned-cleanup-write-deadline" => {
            outcome == "DeadlineExceeded" && number(terminal, "elapsed_us")? >= 200_000
        }
        "response-owned-cleanup-disconnect" => matches!(
            outcome,
            "ClientDisconnected" | "TransportError" | "RequestCancelled"
        ),
        "response-owned-cleanup-cancel" => {
            matches!(
                outcome,
                "ClientDisconnected" | "RequestCancelled" | "TransportError"
            )
        }
        _ => false,
    };
    require(
        expected && number(terminal, "bytes_written")? >= prefix_bytes as u64,
        "wrong native terminal path or unaccepted prefix",
    )?;
    let terminal_at = number(terminal, "terminal_at_us")?;
    for observation in before.as_array().ok_or("missing SDK array")? {
        for field in ["released_at_us", "body_released_at_us"] {
            if observation.get(field) != Some(&Value::Null) {
                require(
                    number(observation, field)? <= terminal_at,
                    "SDK resource survived terminal egress",
                )?;
            }
        }
    }
    Ok(())
}

fn validate_survivor(records: &[Value], isolated: usize, prefix_bytes: usize) -> Result<()> {
    let live = native_group(records, 0, false, 1)?;
    let isolated_native = native_group(records, isolated, false, 1)?;
    validate_progress(&live, &isolated_native)?;
    let healthy = &records
        .get(isolated + 1)
        .ok_or("missing isolated healthy work")?["evidence"];
    require(
        healthy["scope_live"] == true
            && healthy["origin_retained"] == true
            && healthy["healthy"] == json!({"status":200,"bytes":b"P"}),
        "request isolation destroyed another live owner or its retained client",
    )?;
    validate_closed(records, isolated, isolated + 2, 1, CASES[0], prefix_bytes)
}

fn validate_prefix(status: u16, headers: &[Value], bytes: &[u8], expected: &[u8]) -> Result<()> {
    let types = headers
        .iter()
        .filter(|field| {
            field[0]
                .as_str()
                .is_some_and(|name| name.eq_ignore_ascii_case("content-type"))
        })
        .collect::<Vec<_>>();
    require(
        status == 200
            && types.len() == 1
            && types[0][1] == "application/x-ndjson"
            && bytes == expected,
        "invalid response-owned cleanup wire prefix",
    )
}

fn validate_wire(
    adapter: &str,
    case: &str,
    reply: &Result<Value>,
    expected: &[u8],
    prefix_seen: bool,
    abort_issued: bool,
) -> Result<()> {
    let explicit_abort = matches!(
        case,
        "response-owned-cleanup-disconnect" | "response-owned-cleanup-cancel"
    );
    require(
        prefix_seen && abort_issued == explicit_abort,
        "missing validated prefix or intentional abort",
    )?;
    let workers_error_prefix = adapter == "cloudflare"
        && matches!(
            case,
            "response-owned-cleanup-write-deadline" | "response-owned-cleanup-source-error"
        )
        && reply.is_ok();
    if case == CASES[0] || workers_error_prefix {
        let value = reply.as_ref().map_err(|error| error.to_string())?;
        let headers = value["headers"]
            .as_array()
            .ok_or("missing response headers")?;
        let status = u16::try_from(number(value, "status")?)?;
        validate_prefix(status, headers, expected, expected)?;
        let mut body = expected.to_vec();
        if !workers_error_prefix {
            body.extend_from_slice(b"done\n");
        }
        require(
            value["body_bytes"] == json!(body),
            "normal response cleanup did not finish its original stream",
        )
    } else {
        let partial = reply
            .as_ref()
            .err()
            .and_then(|error| error.downcast_ref::<net::InterruptedResponse>())
            .ok_or("missing original interrupted response")?;
        validate_prefix(partial.status, &partial.headers, &partial.body, expected)?;
        let expected_reason = if explicit_abort {
            matches!(partial.cause, net::ResponseInterruption::ObserverRejected)
        } else {
            matches!(
                partial.cause,
                net::ResponseInterruption::PrematureEof | net::ResponseInterruption::PeerClosed
            )
        };
        require(
            expected_reason && partial.body == expected,
            "wrong response cleanup interruption",
        )
    }
}

fn finish_native_proof(wire: Result<()>, proof: impl FnOnce() -> Result<()>) -> Result<()> {
    match (wire, proof()) {
        (Ok(()), result) | (result, Ok(())) => result,
        (Err(wire), Err(native)) => Err(format!("wire: {wire}; native: {native}").into()),
    }
}

fn wire_observation(reply: &Result<Value>, prefix_seen: bool) -> Value {
    match reply {
        Ok(value) => {
            let bytes = value["body_bytes"].as_array();
            json!({"complete":true,"status":value["status"],"prefix_seen":prefix_seen,
                "body_bytes":bytes.map(Vec::len),
                "prefix_bytes":bytes.map(|bytes|bytes.iter().take(512).cloned().collect::<Vec<_>>())})
        }
        Err(error) => match error.downcast_ref::<net::InterruptedResponse>() {
            Some(partial) => {
                json!({"complete":false,"status":partial.status,"prefix_seen":prefix_seen,
                "cause":format!("{:?}",partial.cause),"body_bytes":partial.body.len(),
                "prefix_bytes":partial.body.iter().take(512).copied().collect::<Vec<_>>()})
            }
            None => json!({"complete":false,"prefix_seen":prefix_seen,"cause":"BeforeBodyCapture"}),
        },
    }
}

fn validate_origin(value: &Value, token: &str, case: &str, isolation: bool) -> Result<()> {
    let events = value
        .as_array()
        .ok_or("missing response cleanup source evidence")?;
    require(
        events.len() <= 32
            && events
                .iter()
                .all(|event| event["token"] == token && event["case"] == case),
        "foreign or unbounded response cleanup source evidence",
    )?;
    let release_index = events
        .iter()
        .position(|event| event["role"] == "control" && event["op"] == "release")
        .ok_or("missing native source release")?;
    let release = &events[release_index];
    let released_at = number(release, "at_us")?;
    let returned_at = number(release, "response_returned_us")?;
    require(
        returned_at <= released_at,
        "source release preceded response return",
    )?;
    let prefix = events
        .iter()
        .enumerate()
        .find(|(_, event)| event["role"] == "control" && event["op"] == "prefix-acknowledged");
    if let Some((_, prefix)) = prefix {
        require(
            number(prefix, "prefix_observed_us")? <= number(prefix, "at_us")?
                && number(prefix, "at_us")? <= returned_at,
            "invalid native prefix acknowledgment boundary",
        )?;
    }
    let mut expected_count = 0;
    let origin = if isolation {
        "isolated-origin"
    } else {
        "origin"
    };
    let ready = if isolation { "isolated-ready" } else { "ready" };
    for role in [origin, "a", "b", "c", ready, "ack", "post", "control"] {
        let expected: &[&str] = match role {
            "origin" | "isolated-origin" => &["request", "head", "waiting", "closed"],
            "a" | "b" | "c" if !isolation => &["request", "waiting", "closed"],
            "ready" | "isolated-ready" => &["request", "head", "closed"],
            "ack" if !pending_source(case) && (case != CASES[1] || isolation) => {
                &["request", "head", "closed"]
            }
            "post" if isolation => &["request", "head", "chunk", "closed"],
            "control" if !pending_source(case) => &["prefix-acknowledged", "release"],
            "control" => &["release"],
            _ => &[],
        };
        expected_count += expected.len();
        let actual = events
            .iter()
            .filter(|event| event["role"] == role)
            .collect::<Vec<_>>();
        require(
            actual
                .iter()
                .map(|event| event["op"].as_str())
                .eq(expected.iter().copied().map(Some)),
            "missing, duplicate or reordered native source operations",
        )?;
        let mut previous = 0;
        for event in &actual {
            let observed = number(event, "at_us")?;
            require(observed >= previous, "native source time moved backwards")?;
            previous = observed;
            if event["op"] == "closed" {
                let reason = if matches!(role, "origin" | "isolated-origin" | "a" | "b" | "c") {
                    "fixture_released"
                } else {
                    "complete"
                };
                require(
                    event["reason"] == reason && event.get("error").is_none(),
                    "native source failed or closed before its control",
                )?;
            }
        }
        if role == origin {
            require(
                actual[1]["content_length"] == 1,
                "original SDK body source was not held",
            )?;
        }
        if matches!(role, "origin" | "isolated-origin" | "a" | "b" | "c") && !actual.is_empty() {
            let waiting = actual
                .iter()
                .find(|event| event["op"] == "waiting")
                .ok_or("missing held source wait")?;
            let closed = actual.last().ok_or("missing held source close")?;
            let waiting_index = events
                .iter()
                .position(|event| std::ptr::eq(event, *waiting))
                .ok_or("missing source wait position")?;
            let closed_index = events
                .iter()
                .position(|event| std::ptr::eq(event, *closed))
                .ok_or("missing source close position")?;
            require(
                waiting_index < release_index
                    && release_index < closed_index
                    && number(waiting, "at_us")? <= returned_at
                    && number(closed, "at_us")? >= released_at,
                "held source closed outside its release boundary",
            )?;
        }
        if role == "ack" && !actual.is_empty() {
            let (prefix_index, prefix) =
                prefix.ok_or("SDK acknowledgment lacks its controller boundary")?;
            let head_index = events
                .iter()
                .position(|event| std::ptr::eq(event, actual[1]))
                .ok_or("missing SDK acknowledgment position")?;
            require(
                prefix_index < head_index
                    && number(actual[1], "at_us")? >= number(prefix, "at_us")?,
                "SDK acknowledgment preceded its controller boundary",
            )?;
        }
    }
    require(
        events.len() == expected_count,
        "foreign native source exchange",
    )?;
    if isolation {
        require(
            events
                .iter()
                .filter(|event| {
                    event["role"] == "post"
                        && event["op"] == "chunk"
                        && event["bytes"] == json!(b"P")
                })
                .count()
                == 1,
            "surviving request did not complete healthy work on its retained client",
        )?;
    }
    Ok(())
}

pub(super) fn check(
    case: &str,
    adapter: &str,
    port: u16,
    origins: &WireOrigins,
    log: &Path,
    cursor: &mut protocol::LogCursor,
    records: &mut Records,
) -> Result<()> {
    use std::sync::mpsc::sync_channel;
    require(
        CASES.contains(&case) && matches!(adapter, "axum" | "cloudflare" | "spin"),
        "invalid response cleanup runtime",
    )?;
    let victim = token();
    let survivor = token();
    origins.register_lifetime(&victim, case)?;
    origins.register_lifetime(&survivor, CASES[0])?;
    origins.mark_lifetime_start(&victim)?;
    origins.mark_lifetime_start(&survivor)?;
    let backend = |token: &str, case: &str| {
        format!(
            "http://127.0.0.1:{}/lifetimes/{token}/{case}",
            origins.port(0)
        )
    };
    let victim_backend = backend(&victim, case);
    let survivor_backend = backend(&survivor, CASES[0]);
    let expected = specs(case, &victim, &survivor);
    let captured = std::cell::RefCell::new(Vec::new());
    let validate = |index: usize, record: &Value| {
        validate_header(record, &expected[index])?;
        retain_stage(&mut captured.borrow_mut(), index, record, expected.len())
    };
    let victim_prefix = prefix(case, &victim, false)?;
    let survivor_prefix = prefix(CASES[0], &survivor, true)?;
    let close_start = 7 + usize::from(pending_source(case));
    let victim_end = close_start + 6;
    let mut native_proof_complete = false;
    let mut victim_wire = Value::Null;
    let mut survivor_wire = Value::Null;
    let survivor_prefix_seen = std::sync::atomic::AtomicBool::new(false);
    let coordination_deadline = Instant::now() + Duration::from_secs(2);
    let result = thread::scope(|scope| -> Result<()> {
        let (started, ready) = sync_channel(1);
        let (release, released) = sync_channel(1);
        let live_token = &survivor;
        let live_backend = &survivor_backend;
        let live_prefix = &survivor_prefix;
        let live_prefix_seen = &survivor_prefix_seen;
        let survivor_request = scope.spawn(move || {
            let notify = |status, headers: &[Value], bytes: &[u8]| {
                validate_prefix(status, headers, bytes, live_prefix)?;
                live_prefix_seen.store(true, std::sync::atomic::Ordering::Relaxed);
                started
                    .send(())
                    .map_err(|_| "survivor readiness receiver stopped")?;
                released
                    .recv_timeout(coordination_remaining(coordination_deadline)?)
                    .map_err(|_| "survivor release was not acknowledged")?;
                origins.acknowledge_cleanup_prefix(live_token, Instant::now())
            };
            net::header_request(
                port,
                &format!("/progress/lifetimes/{}", CASES[0]),
                live_token,
                &[
                    ("x-fixture-backend", live_backend),
                    ("x-fixture-cleanup-isolation", "1"),
                ],
                Some(net::ResponseCapturePolicy {
                    exchange_deadline: Some(coordination_deadline),
                    bytes: live_prefix.len(),
                    notify: &notify,
                }),
            )
        });
        let checked = (|| -> Result<()> {
            ready
                .recv_timeout(coordination_remaining(coordination_deadline)?)
                .map_err(|_| "survivor did not enter its native pending setup")?;
            let prefix_seen = std::cell::Cell::new(false);
            let abort_issued = std::cell::Cell::new(false);
            let notify = |status, headers: &[Value], bytes: &[u8]| {
                validate_prefix(status, headers, bytes, &victim_prefix)?;
                prefix_seen.set(true);
                if case == "response-owned-cleanup-cancel" {
                    protocol::wait_validated(
                        log,
                        cursor,
                        PREFIX,
                        expected.len(),
                        8,
                        coordination_deadline,
                        &validate,
                    )?;
                    abort_issued.set(true);
                    return Err(
                        "intentional request cancellation after observed source Pending".into(),
                    );
                }
                if case == "response-owned-cleanup-disconnect" {
                    abort_issued.set(true);
                    return Err("intentional peer disconnect after prefix".into());
                }
                if !pending_source(case) {
                    origins.acknowledge_cleanup_prefix(&victim, Instant::now())?;
                }
                Ok(())
            };
            let reply = net::header_request(
                port,
                &format!("/progress/lifetimes/{case}"),
                &victim,
                &[("x-fixture-backend", &victim_backend)],
                Some(net::ResponseCapturePolicy {
                    exchange_deadline: Some(coordination_deadline),
                    bytes: victim_prefix.len(),
                    notify: &notify,
                }),
            );
            if case == "response-owned-cleanup-disconnect" && abort_issued.get() {
                origins.acknowledge_cleanup_prefix(&victim, Instant::now())?;
            }
            victim_wire = wire_observation(&reply, prefix_seen.get());
            victim_wire["controller_abort_issued"] = json!(abort_issued.get());
            finish_native_proof(
                validate_wire(
                    adapter,
                    case,
                    &reply,
                    &victim_prefix,
                    prefix_seen.get(),
                    abort_issued.get(),
                ),
                || {
                    let (initial, _) = protocol::wait_validated(
                        log,
                        cursor,
                        PREFIX,
                        expected.len(),
                        victim_end,
                        coordination_deadline,
                        &validate,
                    )?;
                    validate_closed(&initial, 2, close_start, 4, case, victim_prefix.len())?;
                    release
                        .send(())
                        .map_err(|_| "surviving request stopped during another owner's cleanup")?;
                    let survivor_reply = survivor_request
                        .join()
                        .map_err(|_| "survivor request panicked")?;
                    survivor_wire = wire_observation(
                        &survivor_reply,
                        survivor_prefix_seen.load(std::sync::atomic::Ordering::Relaxed),
                    );
                    let (all, end) = protocol::wait_validated(
                        log,
                        cursor,
                        PREFIX,
                        expected.len(),
                        expected.len(),
                        coordination_deadline,
                        &validate,
                    )?;
                    let isolated = victim_end;
                    validate_survivor(&all, isolated, survivor_prefix.len())?;
                    native_proof_complete = true;
                    cursor.offset = end;
                    validate_wire(
                        adapter,
                        CASES[0],
                        &survivor_reply,
                        &survivor_prefix,
                        survivor_prefix_seen.load(std::sync::atomic::Ordering::Relaxed),
                        false,
                    )
                },
            )
        })();
        let now = Instant::now();
        let first = origins.release_lifetime(&victim, now);
        let second = origins.release_lifetime(&survivor, now);
        checked.and(first).and(second)
    });
    let victim_source = origins.take_lifetime_events(&victim);
    let survivor_source = origins.take_lifetime_events(&survivor);
    let result = finish_native_proof(result, || {
        validate_origin(
            victim_source.as_ref().map_err(|error| error.to_string())?,
            &victim,
            case,
            false,
        )?;
        validate_origin(
            survivor_source
                .as_ref()
                .map_err(|error| error.to_string())?,
            &survivor,
            CASES[0],
            true,
        )
    });
    records.push(json!({"event":"response-cleanup-case","adapter":adapter,"case":case,"token":victim,
        "survivor_token":survivor,"status":if result.is_ok(){"pass"}else{"fail"},"error":result.as_ref().err().map(ToString::to_string),
        "protocol":captured.into_inner(),"native_proof_complete":native_proof_complete,
        "origin":victim_source.as_ref().ok(),"survivor_origin":survivor_source.as_ref().ok(),
        "wire":victim_wire,"survivor_wire":survivor_wire,
        "coverage":"native-response-owned-cleanup","host_cancellation_proven":false,"qualification":false}))?;
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn expired_coordination_cannot_accept_already_written_terminal_proof() {
        let directory = tempfile::tempdir().unwrap();
        let log = directory.path().join("cleanup.log");
        let record = json!({"stage":"closed"});
        let mut bytes = PREFIX.to_vec();
        bytes.extend(serde_json::to_vec(&record).unwrap());
        bytes.push(b'\n');
        fs::write(&log, bytes).unwrap();
        let cursor = protocol::LogCursor::default();
        let past = Instant::now() - Duration::from_millis(1);
        assert!(coordination_remaining(past).is_err());
        assert!(
            protocol::wait_validated(&log, &cursor, PREFIX, 1, 1, past, &|_, _| Ok(())).is_err()
        );
        assert_eq!(cursor.offset, 0);
    }

    #[test]
    fn partial_cleanup_capture_preserves_only_ordered_validated_stages() {
        let mut captured = Vec::new();
        let first = json!({"stage":"live"});
        let next = json!({"stage":"source-pending"});
        retain_stage(&mut captured, 0, &first, 2).unwrap();
        retain_stage(&mut captured, 0, &first, 2).unwrap();
        assert_eq!(captured, vec![first.clone()]);
        assert!(retain_stage(&mut captured, 0, &next, 2).is_err());
        assert!(retain_stage(&mut captured, 2, &next, 2).is_err());
        assert!(retain_stage(&mut captured, 2, &next, 3).is_err());
        retain_stage(&mut captured, 1, &next, 2).unwrap();
        assert_eq!(captured, vec![first, next]);
    }

    #[test]
    fn failed_wire_still_collects_native_proof() {
        let mut cursor = 0;
        let result = finish_native_proof(Err("wire failed".into()), || {
            cursor = 17;
            Ok(())
        });
        assert_eq!(cursor, 17);
        assert_eq!(result.unwrap_err().to_string(), "wire failed");
    }

    #[test]
    fn failed_native_proof_does_not_advance_cursor() {
        let mut cursor = 0;
        let result = finish_native_proof(Ok(()), || {
            require(false, "invalid native proof")?;
            cursor = 17;
            Ok(())
        });
        assert_eq!(cursor, 0);
        assert_eq!(result.unwrap_err().to_string(), "invalid native proof");
    }

    #[test]
    fn simultaneous_wire_and_native_failures_are_preserved() {
        let result = finish_native_proof(Err("wire failed".into()), || Err("native failed".into()));
        let error = result.unwrap_err().to_string();
        assert!(error.contains("wire failed"));
        assert!(error.contains("native failed"));
    }

    #[test]
    fn wire_failure_capture_remains_bounded_and_does_not_echo_errors() {
        let complete =
            wire_observation(&Ok(json!({"status":200,"body_bytes":vec![65;2048]})), true);
        assert_eq!(complete["body_bytes"], 2048);
        assert_eq!(complete["prefix_bytes"].as_array().unwrap().len(), 512);
        assert_eq!(complete["complete"], true);
        let failed = wire_observation(&Err("secret-provider-text".into()), false);
        assert_eq!(failed["cause"], "BeforeBodyCapture");
        assert!(!failed.to_string().contains("secret-provider-text"));
        let interrupted = Err(Box::new(net::InterruptedResponse {
            status: 200,
            headers: Vec::new(),
            body: vec![65; 2048],
            cause: net::ResponseInterruption::PeerClosed,
            reason: "secret-provider-text".into(),
        }) as Box<dyn std::error::Error + Send + Sync>);
        let partial = wire_observation(&interrupted, true);
        assert_eq!(partial["cause"], "PeerClosed");
        assert_eq!(partial["prefix_bytes"].as_array().unwrap().len(), 512);
        assert!(!partial.to_string().contains("secret-provider-text"));
    }

    #[test]
    fn cleanup_native_budget_rejects_extended_batch_cutoff() {
        for (role, field, value) in [
            (1, "deadline_at_us", 3_000_000),
            (0, "deadline_at_us", 3_000_000),
            (1, "entered_at_us", 40_000),
        ] {
            let mut invalid = sdk(false);
            invalid[role][field] = json!(value);
            assert!(
                validate_native(&invalid, false).is_err(),
                "substituted budget or late batch entry"
            );
        }
    }

    fn trace(case: &str) -> Vec<Value> {
        let live = sdk(false);
        let closed = sdk(true);
        let after = closed
            .as_array()
            .expect("SDK array")
            .iter()
            .map(|entry| {
                let mut value = json!({"invalid":false});
                for field in [
                    "acquisitions",
                    "polls",
                    "releases",
                    "reads",
                    "body_acquisitions",
                    "body_releases",
                ] {
                    value[field] = entry[field].clone();
                }
                value
            })
            .collect::<Vec<_>>();
        let cleanup = json!({"scope_closed":true,"origin_closed":"Internal","batch_closed":true,
            "handles_retained":true,"polls_stable":true,"after":after});
        let terminal = |name: &str, cleanup: &Value| {
            let outcome = match name {
                "response-owned-cleanup-complete" => "HostHandoff",
                "response-owned-cleanup-source-error" => "SourceError",
                "response-owned-cleanup-write-deadline" => "DeadlineExceeded",
                _ => "ClientDisconnected",
            };
            json!({"cleanup":cleanup,"source_drops":1,"source_polls":3,"write_chunks":0,
                "terminal":{"outcome":outcome,"bytes_written":200,"body_kind":"Application",
                "same_request_start":true,"terminal_at_us":234_000,"elapsed_us":234_000,
                "source_failure":if outcome=="SourceError"{Some("internal")}else{None}}})
        };
        specs(case, "victim", "survivor").into_iter().map(|(name, token, stage)| {
            let mut cleanup = cleanup.clone();
            if token == "survivor" {
                cleanup["batch_closed"] = Value::Null;
                cleanup["after"].as_array_mut().expect("SDK owners").truncate(1);
            }
            let evidence = if let Some(index) = stage.rsplit('-').next().and_then(|last|last.parse::<usize>().ok()) {
                if stage.starts_with("closed-") {closed[index].clone()} else {live[index].clone()}
            } else {
                match stage.as_str() {
                    "live" => json!({"isolation":token=="survivor","source_drops":0}),
                    "source-pending" => json!({"pending":true,"after_prefix":true}),
                    "collected" => cleanup.clone(),
                    "closed" => terminal(&name, &cleanup),
                    "isolated" => json!({"scope_live":true,"origin_retained":true,"healthy":{"status":200,"bytes":b"P"}}),
                    _ => panic!("unknown stage"),
                }
            };
            json!({"schema":1,"event":"response-owned-cleanup","case":name,"token":token,
                "stage":stage,"evidence":evidence,"qualification":false})
        }).collect()
    }

    #[test]
    fn cleanup_trace_rejects_regressing_original_sdk_counters() {
        for case in CASES {
            let mut records = trace(case);
            let closed_start = 7 + usize::from(pending_source(case));
            validate_closed(&records, 2, closed_start, 4, case, 200).expect("original trace");
            records[3]["evidence"]["polls"] = json!(999);
            assert!(
                validate_closed(&records, 2, closed_start, 4, case, 200).is_err(),
                "closed SDK counters cannot reset after the live observation"
            );
        }
    }

    #[test]
    fn cleanup_survivor_requires_original_counters_through_healthy_work_and_close() {
        for case in CASES {
            let valid = trace(case);
            let isolated = 13 + usize::from(pending_source(case));
            validate_survivor(&valid, isolated, 200).expect("original survivor trace");
            let mut invalid = valid;
            invalid[isolated]["evidence"]["reads"] = json!(999);
            assert!(
                validate_survivor(&invalid, isolated, 200).is_err(),
                "survivor SDK counters cannot reset after healthy work"
            );
        }
    }

    #[test]
    fn workers_error_prefix_is_only_wire_evidence_not_successful_completion() {
        let reply = Ok(
            json!({"status":200,"headers":[["content-type","application/x-ndjson"]],"body_bytes":b"prefix"}),
        );
        for case in [CASES[2], CASES[3]] {
            validate_wire("cloudflare", case, &reply, b"prefix", true, false).unwrap();
            for adapter in ["axum", "spin", "fastly"] {
                assert!(validate_wire(adapter, case, &reply, b"prefix", true, false).is_err());
            }
            for (field, value) in [
                ("status", json!(500)),
                (
                    "headers",
                    json!([
                        ["content-type", "application/x-ndjson"],
                        ["content-type", "application/x-ndjson"]
                    ]),
                ),
                ("body_bytes", json!(b"prefixdone\n")),
                ("body_bytes", json!(b"other")),
            ] {
                let mut changed = reply.as_ref().unwrap().clone();
                changed[field] = value;
                assert!(
                    validate_wire("cloudflare", case, &Ok(changed), b"prefix", true, false)
                        .is_err()
                );
            }
            assert!(validate_wire("cloudflare", case, &reply, b"prefix", false, false).is_err());
            assert!(validate_wire("cloudflare", case, &reply, b"prefix", true, true).is_err());
        }
        for case in [CASES[0], CASES[1], CASES[4]] {
            assert!(validate_wire("cloudflare", case, &reply, b"prefix", true, false).is_err());
        }
    }

    #[test]
    fn rejected_prefix_is_not_an_intentional_abort() {
        let reply = Err(Box::new(net::InterruptedResponse {
            status: 500,
            headers: vec![json!(["content-type", "application/x-ndjson"])],
            body: b"prefix".to_vec(),
            cause: net::ResponseInterruption::ObserverRejected,
            reason: "invalid response-owned cleanup wire prefix".into(),
        }) as Box<dyn std::error::Error + Send + Sync>);
        for case in [CASES[1], CASES[4]] {
            for (prefix_seen, abort_issued) in [(true, true), (false, true), (true, false)] {
                assert!(
                    validate_wire("axum", case, &reply, b"prefix", prefix_seen, abort_issued)
                        .is_err()
                );
            }
        }
        let valid = Err(Box::new(net::InterruptedResponse {
            status: 200,
            headers: vec![json!(["content-type", "application/x-ndjson"])],
            body: b"prefix".to_vec(),
            cause: net::ResponseInterruption::ObserverRejected,
            reason: "intentional abort".into(),
        }) as Box<dyn std::error::Error + Send + Sync>);
        for case in [CASES[1], CASES[4]] {
            validate_wire("axum", case, &valid, b"prefix", true, true).unwrap();
            for (prefix_seen, abort_issued) in [(false, true), (true, false), (false, false)] {
                assert!(
                    validate_wire("axum", case, &valid, b"prefix", prefix_seen, abort_issued)
                        .is_err()
                );
            }
        }
    }

    #[test]
    fn cleanup_explicit_abort_preserves_native_transport_outcome_without_losing_pending_control() {
        let case = CASES[4];
        let mut records = trace(case);
        let closed_start = 8;
        records[closed_start + 5]["evidence"]["terminal"]["outcome"] = json!("TransportError");
        validate_closed(&records, 2, closed_start, 4, case, 200)
            .expect("Axum and Spin expose coordinator cancellation as TransportError");
        records[closed_start - 1]["evidence"]["pending"] = json!(false);
        assert!(validate_closed(&records, 2, closed_start, 4, case, 200).is_err());
        assert!(
            validate_wire(
                "axum",
                case,
                &Ok(json!({"body_bytes":[]})),
                b"prefix",
                true,
                true
            )
            .is_err(),
            "a completed or spontaneously failed wire is not an explicit client abort"
        );
    }

    #[test]
    fn cleanup_trace_rejects_missing_stages_resumed_sdk_work_and_wrong_terminal() {
        for case in CASES {
            let valid = trace(case);
            let close_start = 7 + usize::from(pending_source(case));
            for mutation in 0..11 {
                let mut records = valid.clone();
                match mutation {
                    0 => {
                        records.remove(close_start);
                    }
                    1 => {
                        records.remove(close_start + 4);
                    }
                    2 => {
                        records.remove(close_start + 5);
                    }
                    3 => records[close_start + 4]["evidence"]["after"][0]["reads"] = json!(2),
                    4 => records[close_start + 4]["evidence"]["after"][1]["polls"] = json!(2),
                    5 => records[close_start + 5]["evidence"]["source_drops"] = json!(2),
                    6 => {
                        records[close_start + 5]["evidence"]["terminal"]["same_request_start"] =
                            json!(false)
                    }
                    7 => {
                        records[close_start + 5]["evidence"]["terminal"]["outcome"] =
                            json!("Refused")
                    }
                    8 => {
                        records[close_start + 5]["evidence"]["terminal"]["terminal_at_us"] =
                            json!(99)
                    }
                    9 => records[close_start + 1]["evidence"]["deadline_at_us"] = json!(4_000_000),
                    _ => records[close_start + 4]["evidence"]["origin_closed"] = json!("Pending"),
                }
                assert!(
                    validate_closed(&records, 2, close_start, 4, case, 200).is_err(),
                    "mutation {mutation}: {case}"
                );
            }
            if pending_source(case) {
                let mut records = valid;
                records[close_start - 1]["evidence"]["pending"] = json!(false);
                assert!(validate_closed(&records, 2, close_start, 4, case, 200).is_err());
            }
        }
    }

    #[test]
    fn cleanup_headers_reject_foreign_reordered_and_duplicate_records() {
        for case in CASES {
            let records = trace(case);
            let expected = specs(case, "victim", "survivor");
            assert_eq!(records.len(), if pending_source(case) { 19 } else { 18 });
            for (index, (record, specification)) in records.iter().zip(&expected).enumerate() {
                validate_header(record, specification).expect("canonical stage");
                assert!(validate_header(record, &expected[(index + 1) % expected.len()]).is_err());
                for field in ["case", "token", "stage"] {
                    let mut invalid = record.clone();
                    invalid[field] = json!("foreign");
                    assert!(validate_header(&invalid, specification).is_err());
                }
            }
        }
    }

    fn source(case: &str, isolation: bool) -> Value {
        let mut events = Vec::new();
        let mut held_closes = Vec::new();
        let mut at = 0_u64;
        let origin = if isolation {
            "isolated-origin"
        } else {
            "origin"
        };
        let ready = if isolation { "isolated-ready" } else { "ready" };
        for role in [origin, "a", "b", "c", ready, "ack", "post"] {
            if role == "ack" && !pending_source(case) {
                at += 1;
                events.push(json!({"token":"t","case":case,"role":"control","op":"prefix-acknowledged","at_us":at,"prefix_observed_us":at}));
            }
            if (isolation && matches!(role, "a" | "b" | "c"))
                || (role == "ack" && (pending_source(case) || (case == CASES[1] && !isolation)))
                || (role == "post" && !isolation)
            {
                continue;
            }
            let ops: &[&str] = match role {
                "origin" | "isolated-origin" => &["request", "head", "waiting", "closed"],
                "a" | "b" | "c" => &["request", "waiting", "closed"],
                "post" => &["request", "head", "chunk", "closed"],
                _ => &["request", "head", "closed"],
            };
            for op in ops {
                at += 1;
                let mut event = json!({"token":"t","case":case,"role":role,"op":op,"at_us":at});
                if *op == "closed" {
                    event["reason"] = json!(if matches!(
                        role,
                        "origin" | "isolated-origin" | "a" | "b" | "c"
                    ) {
                        "fixture_released"
                    } else {
                        "complete"
                    });
                }
                if role == origin && *op == "head" {
                    event["content_length"] = json!(1);
                }
                if *op == "chunk" {
                    event["bytes"] = json!(b"P");
                }
                if *op == "closed" && matches!(role, "origin" | "isolated-origin" | "a" | "b" | "c")
                {
                    held_closes.push(event);
                    continue;
                }
                events.push(event);
            }
        }
        at += 1;
        events.push(json!({"token":"t","case":case,"role":"control","op":"release","at_us":at,"response_returned_us":at}));
        for mut closed in held_closes {
            at += 1;
            closed["at_us"] = json!(at);
            events.push(closed);
        }
        json!(events)
    }

    #[test]
    fn cleanup_source_requires_one_final_release_and_valid_causal_edges() {
        for case in CASES {
            for isolation in [false, true] {
                let valid = source(case, isolation);
                let release = valid
                    .as_array()
                    .expect("ledger")
                    .iter()
                    .position(|event| event["op"] == "release")
                    .expect("release");
                let mut missing = valid.clone();
                missing.as_array_mut().expect("ledger").remove(release);
                assert!(
                    validate_origin(&missing, "t", case, isolation).is_err(),
                    "missing release"
                );
                validate_origin(&valid, "t", case, isolation).expect("actual release shape");
                for mutation in 0..8 {
                    let mut invalid = valid.clone();
                    match mutation {
                        0 => {
                            let extra = invalid[release].clone();
                            invalid.as_array_mut().expect("ledger").push(extra);
                        }
                        1 => {
                            let event = invalid.as_array_mut().expect("ledger").remove(release);
                            invalid.as_array_mut().expect("ledger").insert(0, event);
                        }
                        2 => invalid[release]["response_returned_us"] = json!(u64::MAX),
                        3 => {
                            invalid[release]
                                .as_object_mut()
                                .expect("release")
                                .remove("response_returned_us");
                        }
                        4 => invalid[release]["at_us"] = json!(0),
                        5 => invalid[release]["response_returned_us"] = json!(0),
                        6 => {
                            let event = invalid.as_array_mut().expect("ledger").remove(release + 1);
                            invalid
                                .as_array_mut()
                                .expect("ledger")
                                .insert(release, event);
                        }
                        _ => invalid[release + 1]["at_us"] = json!(0),
                    }
                    assert!(
                        validate_origin(&invalid, "t", case, isolation).is_err(),
                        "release mutation {mutation}: {case}"
                    );
                }
                let mut quantized = valid;
                let at = quantized[release]["at_us"].clone();
                for closed in &mut quantized.as_array_mut().expect("ledger")[release + 1..] {
                    closed["at_us"] = at.clone();
                }
                validate_origin(&quantized, "t", case, isolation)
                    .expect("strict event order survives equal encoded microseconds");
            }
        }
    }

    #[test]
    fn cleanup_source_replays_frozen_axum_cancel_ledger_without_sorting_or_dropping_release() {
        let actual = json!([
        {"at_us":3482,"case":"response-owned-cleanup-cancel","guest_at_us":13,"op":"request","role":"origin","token":"18e46240ba26b560-1433a-23"},
        {"at_us":5734,"case":"response-owned-cleanup-cancel","content_length":1,"op":"head","role":"origin","schedule_basis":"request-receipt","scheduled_us":5482,"token":"18e46240ba26b560-1433a-23"},
        {"at_us":5740,"case":"response-owned-cleanup-cancel","op":"waiting","role":"origin","token":"18e46240ba26b560-1433a-23"},
        {"at_us":40273,"case":"response-owned-cleanup-cancel","guest_at_us":36530,"op":"request","role":"b","token":"18e46240ba26b560-1433a-23"},
        {"at_us":40289,"case":"response-owned-cleanup-cancel","guest_at_us":36532,"op":"request","role":"c","token":"18e46240ba26b560-1433a-23"},
        {"at_us":40299,"case":"response-owned-cleanup-cancel","op":"waiting","role":"c","token":"18e46240ba26b560-1433a-23"},
        {"at_us":40290,"case":"response-owned-cleanup-cancel","op":"waiting","role":"b","token":"18e46240ba26b560-1433a-23"},
        {"at_us":40267,"case":"response-owned-cleanup-cancel","guest_at_us":36525,"op":"request","role":"a","token":"18e46240ba26b560-1433a-23"},
        {"at_us":40322,"case":"response-owned-cleanup-cancel","op":"waiting","role":"a","token":"18e46240ba26b560-1433a-23"},
        {"at_us":40260,"case":"response-owned-cleanup-cancel","guest_at_us":36547,"op":"request","role":"ready","token":"18e46240ba26b560-1433a-23"},
        {"at_us":40373,"case":"response-owned-cleanup-cancel","op":"head","role":"ready","token":"18e46240ba26b560-1433a-23"},
        {"at_us":40392,"case":"response-owned-cleanup-cancel","op":"closed","reason":"complete","role":"ready","token":"18e46240ba26b560-1433a-23"},
        {"at_us":50645,"case":"response-owned-cleanup-cancel","op":"release","response_returned_us":50645,"role":"control","token":"18e46240ba26b560-1433a-23"},
        {"at_us":50666,"case":"response-owned-cleanup-cancel","op":"closed","reason":"fixture_released","role":"origin","token":"18e46240ba26b560-1433a-23"},
        {"at_us":50659,"case":"response-owned-cleanup-cancel","op":"closed","reason":"fixture_released","role":"c","token":"18e46240ba26b560-1433a-23"},
        {"at_us":50699,"case":"response-owned-cleanup-cancel","op":"closed","reason":"fixture_released","role":"a","token":"18e46240ba26b560-1433a-23"},
        {"at_us":50703,"case":"response-owned-cleanup-cancel","op":"closed","reason":"fixture_released","role":"b","token":"18e46240ba26b560-1433a-23"}
        ]);
        validate_origin(&actual, "18e46240ba26b560-1433a-23", CASES[4], false)
            .expect("recorded native source shape");
    }

    #[test]
    fn cleanup_source_rejects_extra_roles_and_missing_acknowledgments() {
        for case in CASES {
            for isolation in [false, true] {
                let valid = source(case, isolation);
                validate_origin(&valid, "t", case, isolation).expect("original source ledger");
                let mut extra = valid.clone();
                extra.as_array_mut().expect("events").push(
                    json!({"token":"t","case":case,"role":"foreign","op":"request","at_us":0}),
                );
                assert!(
                    validate_origin(&extra, "t", case, isolation).is_err(),
                    "foreign native exchange must not be credited"
                );
                let mut missing = valid.clone();
                missing.as_array_mut().expect("events").retain(|event| {
                    event["role"] != if isolation { "isolated-ready" } else { "ready" }
                });
                assert!(
                    validate_origin(&missing, "t", case, isolation).is_err(),
                    "missing setup barrier"
                );
            }
        }
    }

    #[test]
    fn cleanup_ack_requires_prior_controller_record_at_equal_timestamps() {
        let mut valid = source(CASES[0], false);
        let rows = valid.as_array_mut().expect("ledger");
        let prefix = rows
            .iter()
            .position(|event| event["op"] == "prefix-acknowledged")
            .expect("prefix");
        let head = rows
            .iter()
            .position(|event| event["role"] == "ack" && event["op"] == "head")
            .expect("ack head");
        rows[prefix]["at_us"] = rows[head]["at_us"].clone();
        rows[prefix]["prefix_observed_us"] = rows[head]["at_us"].clone();
        validate_origin(&valid, "t", CASES[0], false)
            .expect("equal timestamps retain ledger order");
        let mut reordered = valid;
        let rows = reordered.as_array_mut().expect("ledger");
        let acknowledged = rows.remove(prefix);
        let head = rows
            .iter()
            .position(|event| event["role"] == "ack" && event["op"] == "head")
            .expect("ack head");
        rows.insert(head + 1, acknowledged);
        assert!(validate_origin(&reordered, "t", CASES[0], false).is_err());
    }

    #[test]
    fn cleanup_disconnect_without_sdk_ack_still_requires_controller_acknowledgment() {
        let valid = source(CASES[1], false);
        validate_origin(&valid, "t", CASES[1], false).expect("disconnect has no SDK ACK exchange");
        assert!(
            valid
                .as_array()
                .expect("source ledger")
                .iter()
                .all(|event| event["role"] != "ack")
        );
        let mut missing_control = valid;
        missing_control
            .as_array_mut()
            .expect("source ledger")
            .retain(|event| event["op"] != "prefix-acknowledged");
        assert!(
            validate_origin(&missing_control, "t", CASES[1], false).is_err(),
            "the post-reply controller acknowledgment remains mandatory"
        );
    }

    #[test]
    fn cleanup_disconnect_rejects_partial_and_complete_unexpected_sdk_ack() {
        for count in 1..=3 {
            let mut invalid = source(CASES[1], false);
            for (index, op) in ["request", "head", "closed"]
                .into_iter()
                .take(count)
                .enumerate()
            {
                let mut event =
                    json!({"token":"t","case":CASES[1],"role":"ack","op":op,"at_us":100+index});
                if op == "closed" {
                    event["reason"] = json!("complete");
                }
                invalid.as_array_mut().expect("source ledger").push(event);
            }
            assert!(
                validate_origin(&invalid, "t", CASES[1], false).is_err(),
                "unexpected SDK ACK prefix of length {count}"
            );
        }
    }

    #[test]
    fn cleanup_unaffected_paths_still_require_complete_sdk_ack() {
        for (case, isolation) in [
            (CASES[0], false),
            (CASES[0], true),
            (CASES[3], false),
            (CASES[1], true),
        ] {
            let valid = source(case, isolation);
            validate_origin(&valid, "t", case, isolation).expect("unchanged ACK path");
            for retained in 0..3 {
                let mut invalid = valid.clone();
                let mut seen = 0;
                invalid
                    .as_array_mut()
                    .expect("source ledger")
                    .retain(|event| {
                        if event["role"] != "ack" {
                            return true;
                        }
                        seen += 1;
                        seen <= retained
                    });
                assert!(
                    validate_origin(&invalid, "t", case, isolation).is_err(),
                    "missing SDK ACK completion: {case}, isolated={isolation}, retained={retained}"
                );
            }
        }
    }

    fn sdk(closed: bool) -> Value {
        json!(["origin", "a", "b", "c"].into_iter().map(|role| {
            let origin = role == "origin";
            json!({"role":role,"attachments":1,"preparations":1,"invalid":false,
                "acquisitions":1,"polls":1,"ready_polls":u64::from(origin),
                "releases":u64::from(origin || closed),"heads":u64::from(origin),
                "body_acquisitions":u64::from(origin),"body_releases":u64::from(origin && closed),
                "reads":u64::from(origin),"ready_reads":0,"first_pending":!origin,
                "read_pending":origin.then_some(true),"released_at_us":if origin{Some(2_000)}else{closed.then_some(234_000)},
                "body_released_at_us":(origin && closed).then_some(234_000),
                "entered_at_us":if origin{0}else{34_000},"budget_cause":if origin{"PerCallTimeout"}else{"BatchCutoff"},
                "deadline_at_us":if origin{15_000_000}else{534_000}})
        }).collect::<Vec<_>>())
    }

    #[test]
    fn native_cleanup_rejects_missing_owners_body_substitutions_and_premature_release() {
        for closed in [false, true] {
            validate_native(&sdk(closed), closed).expect("complete native ownership evidence");
            for mutation in 0..10 {
                let mut invalid = sdk(closed);
                match mutation {
                    0 => invalid[0]["body_acquisitions"] = json!(0),
                    1 => invalid[0]["read_pending"] = Value::Null,
                    2 => invalid[0]["reads"] = json!(0),
                    3 => invalid[1]["acquisitions"] = json!(0),
                    4 => invalid[1]["first_pending"] = json!(false),
                    5 => invalid[1]["heads"] = json!(1),
                    6 => invalid[1]["invalid"] = json!(true),
                    7 => invalid[1]["releases"] = json!(u64::from(!closed)),
                    8 => invalid.as_array_mut().expect("array").truncate(3),
                    _ => invalid[2] = invalid[1].clone(),
                }
                assert!(
                    validate_native(&invalid, closed).is_err(),
                    "mutation {mutation}"
                );
            }
        }
    }
}
