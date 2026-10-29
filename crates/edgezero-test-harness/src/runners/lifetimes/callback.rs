use super::*;

const PREFIX: &[u8] = b"edgezero-lifetime-callback ";
const CASES: [&str; 4] = ["after-ok", "after-error", "early-error", "setup-error"];

fn delivered(mode: &str) -> bool {
    matches!(mode, "after-ok" | "after-error")
}
fn body(wrapper: &str, mode: &str, token: &str) -> Value {
    json!({"schema":1,"case":mode,"wrapper":wrapper,"token":token,"originals_pending":true,"qualification":false})
}
fn record(wrapper: &str, mode: &str, token: &str, stage: &str) -> Value {
    let after = delivered(mode);
    let evidence = match stage {
        "pending" => {
            json!({"origin_pending":true,"batch_pending":true,"callback_scope_live":true,"unpolled_polls":0,"request_drops":0,"live_timers":true})
        }
        "returning" => {
            json!({"originals_pending":true,"callback_scope_live":true,"delivered":after,"post_pending":after,"terminal_reports":usize::from(after)})
        }
        "closed" => {
            json!({"callback_result":match mode{"after-ok"=>"Ok","setup-error"=>"setup-error",_=>"callback-error"},"callback_scope_closed":true,
            "send_error":"Internal","origin_error":"Internal","batch_error":"Internal","slots":["Unresolved","Unresolved","Unresolved"],
            "post_error":if after{Some("Internal")}else{None},"unpolled_polls":0,"request_drops":1,"all_timers_released":true,
            "release_before_handle_consumption":true,"release_stable":true,"terminal_reports":usize::from(after)})
        }
        _ => panic!("unknown callback protocol stage"),
    };
    json!({"schema":1,"event":"callback-exit","wrapper":wrapper,"case":mode,"token":token,"stage":stage,"evidence":evidence,"qualification":false})
}
fn expected(wrapper: &str, mode: &str, token: &str) -> [Value; 3] {
    ["pending", "returning", "closed"].map(|stage| record(wrapper, mode, token, stage))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum WireEvidence {
    ApplicationCompleted,
    PeerClosedWithoutHead,
    UnverifiedHttp500,
}

fn validate_wire(
    wrapper: &str,
    mode: &str,
    token: &str,
    reply: &Result<Value>,
) -> Result<WireEvidence> {
    if !delivered(mode) {
        return match reply {
            Ok(reply) if reply["status"]==500=>Ok(WireEvidence::UnverifiedHttp500),
            Err(error) if error.downcast_ref::<net::ResponseHeadAborted>().is_some()=>Ok(WireEvidence::PeerClosedWithoutHead),
            _=>Err("early callback exit produced an application response or an unproved reader failure".into()),
        };
    }
    let reply = reply.as_ref().map_err(ToString::to_string)?;
    let headers = reply["headers"]
        .as_array()
        .ok_or("missing callback response head")?;
    let types = headers
        .iter()
        .filter(|field| {
            field[0]
                .as_str()
                .is_some_and(|name| name.eq_ignore_ascii_case("content-type"))
        })
        .collect::<Vec<_>>();
    let mut bytes = serde_json::to_vec(&body(wrapper, mode, token))?;
    bytes.push(b'\n');
    require(
        reply["status"] == 200
            && types.len() == 1
            && types[0][1] == "application/json"
            && bytes.len() <= 512
            && reply["body_bytes"] == json!(bytes),
        "callback response was replaced or has invalid canonical pending witnesses",
    )?;
    Ok(WireEvidence::ApplicationCompleted)
}

fn validate_origin(mode: &str, token: &str, origin: &Value) -> Result<()> {
    let events = origin.as_array().ok_or("missing callback source events")?;
    require(
        events.len() == if delivered(mode) { 23 } else { 17 },
        "invalid callback source cardinality",
    )?;
    require(
        events.iter().all(|event| {
            event["case"] == "callback-exit"
                && event["token"] == token
                && event.get("error").is_none()
                && event.get("scheduled_us").is_none()
                && event["op"] != "chunk"
        }),
        "foreign or completed callback work",
    )?;
    let one = |role: &str, op: &str| -> Result<&Value> {
        let mut found = events
            .iter()
            .filter(|event| event["role"] == role && event["op"] == op);
        let value = found.next().ok_or("missing callback source phase")?;
        require(found.next().is_none(), "duplicate callback source phase")?;
        Ok(value)
    };
    let returned = one("control", "response-returned")?;
    let release = one("control", "release")?;
    let returned_at = number(returned, "at_us")?;
    let release_at = number(release, "at_us")?;
    require(
        returned_at <= number(returned, "closed_validated_us")?
            && number(returned, "closed_validated_us")? <= release_at
            && number(release, "response_returned_us")? == returned_at,
        "callback source released before canonical closed evidence",
    )?;
    let ready = one("ready", "head")?;
    let ready_at = number(ready, "write_started_us")?;
    for role in ["origin", "a", "b", "c"]
        .into_iter()
        .chain(delivered(mode).then_some("post"))
    {
        let request = number(one(role, "request")?, "at_us")?;
        let waiting = number(one(role, "waiting")?, "at_us")?;
        let gate = if role == "post" {
            number(one("post-ready", "head")?, "write_started_us")?
        } else {
            ready_at
        };
        require(
            request <= waiting && waiting <= gate,
            "callback work not pending before barrier",
        )?;
        let closed = one(role, "closed")?;
        require(
            closed["reason"] == "fixture_released" && number(closed, "at_us")? >= release_at,
            "callback source closed before fixture release",
        )?;
    }
    for role in ["ready"]
        .into_iter()
        .chain(delivered(mode).then_some("post-ready"))
    {
        let head = one(role, "head")?;
        require(
            number(one(role, "request")?, "at_us")? <= number(head, "write_started_us")?
                && number(head, "write_started_us")? <= number(head, "at_us")?
                && number(head, "at_us")? <= number(one(role, "closed")?, "at_us")?
                && one(role, "closed")?["reason"] == "complete"
                && number(head, "at_us")? <= number(returned, "closed_validated_us")?,
            "invalid callback readiness barrier",
        )?;
    }
    if delivered(mode) {
        require(
            number(one("post", "request")?, "at_us")? >= number(ready, "at_us")?,
            "post-delivery work preceded original readiness",
        )?;
    }
    Ok(())
}

fn check_case(
    wrapper: &str,
    mode: &str,
    port: u16,
    origins: &WireOrigins,
    log: &Path,
    cursor: &mut protocol::LogCursor,
    records: &mut Records,
) -> Result<()> {
    let token = token();
    origins.register_lifetime(&token, "callback-exit")?;
    let anchor = origins.mark_lifetime_start(&token)?;
    let backend = format!(
        "http://127.0.0.1:{}/lifetimes/{token}/callback-exit",
        origins.port(0)
    );
    let reply = net::header_request(
        port,
        "/progress/lifetimes/callback-exit",
        &token,
        &[
            ("x-fixture-backend", &backend),
            ("x-fixture-callback-mode", mode),
        ],
        None,
    );
    let returned = Instant::now();
    let mut evidence = Vec::new();
    let mut validated = None;
    let mut wire = None;
    let outcome = (|| -> Result<()> {
        wire = Some(validate_wire(wrapper, mode, &token, &reply)?);
        let (all, end) = protocol::wait(log, cursor, PREFIX, &expected(wrapper, mode, &token), 3)?;
        evidence = all;
        cursor.offset = end;
        validated = Some(Instant::now());
        origins.release_lifetime(&token, returned)?;
        Ok(())
    })();
    let source = origins.take_lifetime_events(&token);
    let source_error = source.as_ref().err().map(ToString::to_string);
    let mut origin = source.unwrap_or(Value::Null);
    if let Some(events) = origin.as_array_mut() {
        events.push(
            json!({"role":"control","op":"response-returned","case":"callback-exit","token":token,
            "at_us":returned.duration_since(anchor).as_micros() as u64,
            "closed_validated_us":validated.map(|at|at.duration_since(anchor).as_micros() as u64)}),
        );
    }
    let outcome = outcome.and_then(|()| {
        require(
            source_error.is_none(),
            source_error.clone().unwrap_or_default(),
        )?;
        validate_origin(mode, &token, &origin)
    });
    records.push(json!({"event":"callback-case","adapter":"fastly","wrapper":wrapper,"case":mode,"token":token,
        "status":if outcome.is_ok(){"pass"}else{"fail"},"error":outcome.as_ref().err().map(ToString::to_string),
        "source_error":source_error,"origin":origin,"protocol":evidence,"client":reply.as_ref().ok(),
        "client_error":reply.as_ref().err().map(ToString::to_string),"wire_evidence":wire.map(|kind|format!("{kind:?}")),
        "coverage":"guest-callback-cleanup","no_replacement_response_proven":wire==Some(WireEvidence::PeerClosedWithoutHead),"qualification":false}))?;
    outcome
}

pub(super) fn run(run: &Path, records: &mut Records) -> Result<usize> {
    let mut failures = Vec::new();
    let mut total = 0;
    for (suite, wrapper) in [
        ("outbound-callback-run", "run_custom"),
        ("outbound-callback-serve", "serve_custom"),
    ] {
        let directory = run.join(suite);
        fs::create_dir(&directory)?;
        let log = directory.join(format!("fastly-{suite}.log"));
        let mut cursor = protocol::LogCursor::default();
        let outcome = wire::progress_suite(
            "fastly",
            suite,
            &directory,
            records,
            |port, origins, records| {
                let mut failures = Vec::new();
                for mode in CASES {
                    if let Err(error) =
                        check_case(wrapper, mode, port, origins, &log, &mut cursor, records)
                    {
                        failures.push(format!("{mode}: {error}"));
                    }
                }
                require(failures.is_empty(), failures.join("; "))?;
                Ok(CASES.len())
            },
        );
        let tail = if log.exists() {
            protocol::finish(&log, &cursor, PREFIX)
        } else {
            Ok(())
        };
        records.push(json!({"event":"callback-log-tail","adapter":"fastly","wrapper":wrapper,
            "status":if tail.is_ok(){"pass"}else{"fail"},"error":tail.as_ref().err().map(ToString::to_string),"qualification":false}))?;
        match outcome {
            Ok(Some(count)) => total += count,
            Ok(None) => failures.push(format!("{wrapper}: runtime unavailable")),
            Err(error) => failures.push(format!("{wrapper}: {error}")),
        }
        if let Err(error) = tail {
            failures.push(format!("{wrapper} tail: {error}"));
        }
    }
    require(failures.is_empty(), failures.join("; "))?;
    records.push(json!({"event":"callback-inventory","adapter":"fastly","cases":total,"status":"pass","qualification":false}))?;
    Ok(total)
}

#[cfg(test)]
mod tests {
    use super::*;
    fn reply(wrapper: &str, mode: &str, token: &str) -> Result<Value> {
        let mut bytes = serde_json::to_vec(&body(wrapper, mode, token)).unwrap();
        bytes.push(b'\n');
        Ok(json!({"status":200,"headers":[["content-type","application/json"]],"body_bytes":bytes}))
    }
    #[test]
    fn callback_wire_requires_the_original_committed_body_or_a_bounded_pre_delivery_refusal() {
        for wrapper in ["run_custom", "serve_custom"] {
            for mode in CASES {
                if delivered(mode) {
                    let wire = reply(wrapper, mode, "case");
                    validate_wire(wrapper, mode, "case", &wire).unwrap();
                    assert!(validate_wire(wrapper, mode, "foreign", &wire).is_err());
                    assert!(
                        validate_wire(wrapper, mode, "case", &Ok(json!({"status":500}))).is_err()
                    );
                } else {
                    assert_eq!(
                        validate_wire(wrapper, mode, "case", &Ok(json!({"status":500}))).unwrap(),
                        WireEvidence::UnverifiedHttp500
                    );
                    assert!(
                        validate_wire(wrapper, mode, "case", &reply(wrapper, mode, "case"))
                            .is_err()
                    );
                }
                assert!(
                    validate_wire(
                        wrapper,
                        mode,
                        "case",
                        &Err(std::io::Error::from(std::io::ErrorKind::TimedOut).into())
                    )
                    .is_err()
                );
            }
        }
    }
    #[test]
    fn callback_protocol_rejects_other_wrappers_modes_and_early_release_claims() {
        let records = expected("run_custom", "after-error", "case");
        let bytes = records
            .iter()
            .flat_map(|record| {
                let mut line = PREFIX.to_vec();
                line.extend(serde_json::to_vec(record).unwrap());
                line.push(b'\n');
                line
            })
            .collect::<Vec<_>>();
        assert_eq!(protocol::parse(PREFIX, &bytes, &records).unwrap().len(), 3);
        assert!(
            protocol::parse(
                PREFIX,
                &bytes,
                &expected("serve_custom", "after-error", "case")
            )
            .is_err()
        );
        assert!(
            protocol::parse(PREFIX, &bytes, &expected("run_custom", "after-ok", "case")).is_err()
        );
        assert!(
            protocol::parse(
                PREFIX,
                &bytes,
                &expected("run_custom", "after-error", "foreign")
            )
            .is_err()
        );
    }
    fn source(mode: &str) -> Value {
        let mut events = Vec::new();
        for role in ["origin", "a", "b", "c"] {
            events.push(json!({"role":role,"op":"request","at_us":100}));
            events.push(json!({"role":role,"op":"waiting","at_us":200}));
        }
        for (role, start) in [("ready", 250)]
            .into_iter()
            .chain(delivered(mode).then_some(("post-ready", 370)))
        {
            if role == "post-ready" {
                events.push(json!({"role":"post","op":"request","at_us":350}));
                events.push(json!({"role":"post","op":"waiting","at_us":360}));
            }
            events.push(json!({"role":role,"op":"request","at_us":start}));
            events.push(
                json!({"role":role,"op":"head","at_us":start+60,"write_started_us":start+50}),
            );
            events.push(json!({"role":role,"op":"closed","at_us":start+70,"reason":"complete"}));
        }
        events.push(json!({"role":"control","op":"response-returned","at_us":450,"closed_validated_us":500}));
        events
            .push(json!({"role":"control","op":"release","at_us":600,"response_returned_us":450}));
        for role in ["origin", "a", "b", "c"]
            .into_iter()
            .chain(delivered(mode).then_some("post"))
        {
            events.push(json!({"role":role,"op":"closed","at_us":610,"reason":"fixture_released"}));
        }
        for event in &mut events {
            event["case"] = json!("callback-exit");
            event["token"] = json!("case");
        }
        json!(events)
    }
    #[test]
    fn callback_source_requires_all_originals_and_closure_evidence_before_release() {
        for mode in CASES {
            let events = source(mode);
            validate_origin(mode, "case", &events).unwrap();
            for index in 0..events.as_array().unwrap().len() {
                let mut changed = events.clone();
                changed.as_array_mut().unwrap().remove(index);
                assert!(
                    validate_origin(mode, "case", &changed).is_err(),
                    "{mode} missing {index}"
                );
            }
            let mut wrong = events.clone();
            wrong
                .as_array_mut()
                .unwrap()
                .iter_mut()
                .find(|event| event["op"] == "response-returned")
                .unwrap()["closed_validated_us"] = json!(601);
            assert!(validate_origin(mode, "case", &wrong).is_err());
        }
    }
}
