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

fn sdk_polls(row: &Value) -> Result<u64> {
    row["operation_polls"]
        .as_u64()
        .ok_or_else(|| "invalid callback SDK poll counter".into())
}

fn validate_record(
    wrapper: &str,
    mode: &str,
    token: &str,
    index: usize,
    value: &Value,
    previous: &[Value],
) -> Result<()> {
    require(
        index < 3 && previous.len() == index,
        "invalid callback record order",
    )?;
    let closed = index == 2;
    let count = if index > 0 && delivered(mode) { 5 } else { 4 };
    let rows = value["evidence"]["sdk_operations"]
        .as_array()
        .filter(|rows| rows.len() == count)
        .ok_or("missing callback SDK operation owners")?;
    for (role, row) in ["origin", "a", "b", "c", "post"].iter().zip(rows) {
        let polls = sdk_polls(row)?;
        let kind = row["pending_kind"].as_str();
        require(
            matches!(kind, Some("SdkPoll") if polls > 0)
                || matches!(kind, Some("HeadReadinessTimeout") if polls == 0),
            "missing typed callback pending evidence",
        )?;
        require(
            row == &json!({"role":role,"operation_acquisitions":1,
                    "operation_polls":polls,"operation_releases":u64::from(closed),
                    "ready_operation_polls":0,"pending_kind":kind,"heads":0,
                    "source_acquisitions":0,"source_releases":0}),
            "callback SDK operation not pending or not released exactly once",
        )?;
    }
    if let Some(before) = previous.last() {
        let before = before["evidence"]["sdk_operations"]
            .as_array()
            .ok_or("missing earlier callback SDK operations")?;
        for (before, after) in before.iter().zip(rows) {
            require(
                sdk_polls(before)? <= sdk_polls(after)?,
                "callback SDK operation poll count regressed",
            )?;
        }
    }
    let mut canonical = expected(wrapper, mode, token)[index].clone();
    canonical["evidence"]["sdk_operations"] = json!(rows);
    if closed {
        for (field, counter) in [
            ("sdk_polls_after_collect", "operation_polls"),
            ("sdk_releases_after_collect", "operation_releases"),
        ] {
            let counters = rows
                .iter()
                .map(|row| row[counter].clone())
                .collect::<Vec<_>>();
            require(
                value["evidence"][field] == json!(counters),
                "callback SDK operation changed after retained handle consumption",
            )?;
            canonical["evidence"][field] = json!(counters);
        }
    }
    require(
        value == &canonical,
        "invalid or reordered canonical callback cleanup record",
    )
}

fn wait_records(
    wrapper: &str,
    mode: &str,
    token: &str,
    log: &Path,
    cursor: &protocol::LogCursor,
    count: usize,
) -> Result<(Vec<Value>, u64)> {
    let seen = std::cell::RefCell::new(Vec::<Value>::new());
    protocol::wait_validated(
        log,
        cursor,
        PREFIX,
        3,
        count,
        Instant::now() + Duration::from_secs(2),
        &|index, value| {
            if index == 0 {
                seen.borrow_mut().clear();
            }
            validate_record(wrapper, mode, token, index, value, &seen.borrow())?;
            seen.borrow_mut().push(value.clone());
            Ok(())
        },
    )
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
        let (all, end) = wait_records(wrapper, mode, &token, log, cursor, 3)?;
        evidence = all;
        cursor.offset = end;
        validated = Some(Instant::now());
        origins.release_lifetime(&token, returned)?;
        Ok(())
    })();
    let outcome = if validated.is_none() {
        let released = origins.release_lifetime(&token, returned);
        outcome.and(released)
    } else {
        outcome
    };
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
pub(super) fn assert_failure_releases_origin(
    expected_error: &str,
    body: impl FnOnce(&str) -> Vec<u8> + Send,
    check: impl FnOnce(u16, &WireOrigins, &Path, &mut protocol::LogCursor, &mut Records) -> Result<()>,
) {
    use std::io::{BufRead as _, BufReader, Write as _};
    use std::net::TcpListener;

    let directory = tempfile::tempdir().unwrap();
    let log = directory.path().join("runtime.log");
    fs::write(&log, b"edgezero-life").unwrap();
    let mut records = Records {
        path: directory.path().join("events.jsonl"),
        values: Vec::new(),
    };
    let origins = WireOrigins::start().unwrap();
    let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
    let port = listener.local_addr().unwrap().port();
    let mut cursor = protocol::LogCursor::default();
    let result = thread::scope(|scope| {
        let server = scope.spawn(move || {
            let (socket, _) = listener.accept().unwrap();
            socket.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
            socket.set_write_timeout(Some(Duration::from_secs(5))).unwrap();
            let mut reader = BufReader::new(socket);
            let mut request_token = None;
            loop {
                let mut line = String::new();
                assert!(reader.read_line(&mut line).unwrap() > 0);
                if line == "\r\n" {
                    break;
                }
                if let Some((name, value)) = line.split_once(':')
                    && name.eq_ignore_ascii_case("x-request-token")
                {
                    request_token = Some(value.trim().to_owned());
                }
            }
            let bytes = body(&request_token.unwrap());
            let socket = reader.get_mut();
            write!(socket, "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n", bytes.len()).unwrap();
            socket.write_all(&bytes).unwrap();
            socket.flush().unwrap();
        });
        let result = check(port, &origins, &log, &mut cursor, &mut records);
        server.join().unwrap();
        result
    });
    assert_eq!(result.unwrap_err().to_string(), expected_error);
    assert_eq!(cursor.offset, 0);
    let record = records.values.last().unwrap();
    assert_eq!(record["status"], "fail");
    assert_eq!(record["source_error"], Value::Null);
    let events = record["origin"].as_array().unwrap();
    // Collection drains the ledger, so the release must precede collection.
    let releases = events
        .iter()
        .filter(|event| event["op"] == "release")
        .collect::<Vec<_>>();
    assert_eq!(
        releases.len(),
        1,
        "failed controller skipped origin release"
    );
    assert_eq!(releases[0]["token"], record["token"]);
    assert_eq!(releases[0]["role"], "control");
    assert_eq!(origins.finish_counted().unwrap(), 0);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn failure_releases_origin_before_collection_on_wire_validation_error() {
        assert_failure_releases_origin(
            "callback response was replaced or has invalid canonical pending witnesses",
            |_| b"invalid\n".to_vec(),
            |port, origins, log, cursor, records| {
                check_case(
                    "run_custom",
                    "after-ok",
                    port,
                    origins,
                    log,
                    cursor,
                    records,
                )
            },
        );
    }

    #[test]
    fn failure_releases_origin_before_collection_on_protocol_timeout() {
        assert_failure_releases_origin(
            "cleanup terminal evidence timed out",
            |token| {
                let mut bytes = serde_json::to_vec(&body("run_custom", "after-ok", token)).unwrap();
                bytes.push(b'\n');
                bytes
            },
            |port, origins, log, cursor, records| {
                check_case(
                    "run_custom",
                    "after-ok",
                    port,
                    origins,
                    log,
                    cursor,
                    records,
                )
            },
        );
    }

    fn sdk_records(wrapper: &str, mode: &str, token: &str) -> [Value; 3] {
        let mut records = expected(wrapper, mode, token);
        for (index, record) in records.iter_mut().enumerate() {
            let count = if index > 0 && delivered(mode) { 5 } else { 4 };
            let polls = 3 + index as u64 * 4;
            record["evidence"]["sdk_operations"] = json!(
                ["origin", "a", "b", "c", "post"][..count]
                    .iter()
                    .map(|role| json!({"role":role,"operation_acquisitions":1,
                        "operation_polls":polls,"operation_releases":u64::from(index==2),
                        "ready_operation_polls":0,"pending_kind":"SdkPoll","heads":0,
                        "source_acquisitions":0,"source_releases":0}))
                    .collect::<Vec<_>>()
            );
            if index == 2 {
                record["evidence"]["sdk_polls_after_collect"] = json!(vec![polls; count]);
                record["evidence"]["sdk_releases_after_collect"] = json!(vec![1; count]);
            }
        }
        records
    }

    fn encode(records: &[Value]) -> Vec<u8> {
        records
            .iter()
            .flat_map(|record| {
                let mut line = PREFIX.to_vec();
                line.extend(serde_json::to_vec(record).unwrap());
                line.push(b'\n');
                line
            })
            .collect()
    }

    fn parse_records(wrapper: &str, mode: &str, bytes: &[u8]) -> Result<Vec<Value>> {
        let directory = tempfile::tempdir()?;
        let log = directory.path().join("callback.log");
        fs::write(&log, bytes)?;
        wait_records(
            wrapper,
            mode,
            "case",
            &log,
            &protocol::LogCursor::default(),
            0,
        )
        .map(|(records, _)| records)
    }

    #[test]
    fn callback_sdk_protocol_accepts_typed_head_timeouts_with_zero_polls() {
        let mut records = sdk_records("run_custom", "after-ok", "case");
        for record in &mut records {
            let rows = record["evidence"]["sdk_operations"].as_array_mut().unwrap();
            for row in rows.iter_mut() {
                row.as_object_mut().unwrap().remove("first_pending");
                row["pending_kind"] = json!("HeadReadinessTimeout");
                row["operation_polls"] = json!(0);
            }
        }
        records[2]["evidence"]["sdk_polls_after_collect"] = json!(vec![0; 5]);
        parse_records("run_custom", "after-ok", &encode(&records))
            .expect("typed native head timeouts");
    }

    #[test]
    fn callback_sdk_protocol_accepts_actual_dynamic_counts_for_both_wrappers_and_all_modes() {
        for wrapper in ["run_custom", "serve_custom"] {
            for mode in CASES {
                let records = sdk_records(wrapper, mode, "case");
                assert_eq!(
                    parse_records(wrapper, mode, &encode(&records)).unwrap(),
                    records
                );
            }
        }
    }

    #[test]
    fn callback_sdk_protocol_preserves_maximum_counters_and_incremental_log_cursor_guards() {
        for wrapper in ["run_custom", "serve_custom"] {
            for mode in CASES {
                let token = "x".repeat(64);
                let mut records = sdk_records(wrapper, mode, &token);
                for record in &mut records {
                    for row in record["evidence"]["sdk_operations"].as_array_mut().unwrap() {
                        row["operation_polls"] = json!(u64::MAX);
                    }
                }
                let count = if delivered(mode) { 5 } else { 4 };
                records[2]["evidence"]["sdk_polls_after_collect"] = json!(vec![u64::MAX; count]);
                let directory = tempfile::tempdir().unwrap();
                let log = directory.path().join("callback.log");
                let cursor = protocol::LogCursor::default();
                fs::write(&log, encode(&records[..1])).unwrap();
                assert_eq!(
                    wait_records(wrapper, mode, &token, &log, &cursor, 0)
                        .unwrap()
                        .0,
                    records[..1]
                );
                let bytes = encode(&records);
                fs::write(&log, &bytes).unwrap();
                let (actual, end) = wait_records(wrapper, mode, &token, &log, &cursor, 3).unwrap();
                assert_eq!(actual, records);
                assert_eq!(end, bytes.len() as u64);
                fs::write(&log, encode(&records[..1])).unwrap();
                assert!(wait_records(wrapper, mode, &token, &log, &cursor, 0).is_err());
            }
        }
    }

    #[test]
    fn callback_sdk_protocol_rejects_scope_only_records_and_invalid_pending_or_released_owners() {
        for wrapper in ["run_custom", "serve_custom"] {
            for mode in CASES {
                assert!(
                    parse_records(wrapper, mode, &encode(&expected(wrapper, mode, "case")))
                        .is_err()
                );
                let records = sdk_records(wrapper, mode, "case");
                for stage in 0..3 {
                    let rows = records[stage]["evidence"]["sdk_operations"]
                        .as_array()
                        .unwrap();
                    for role in 0..rows.len() {
                        for (field, value) in [
                            ("role", json!("foreign")),
                            ("operation_acquisitions", json!(0)),
                            ("operation_polls", json!(0)),
                            ("operation_polls", json!(1.5)),
                            ("operation_polls", json!(-1)),
                            ("operation_polls", json!("3")),
                            ("operation_releases", json!(if stage == 2 { 0 } else { 1 })),
                            ("operation_releases", json!(2)),
                            ("ready_operation_polls", json!(1)),
                            ("pending_kind", json!(false)),
                            ("heads", json!(1)),
                            ("source_acquisitions", json!(1)),
                            ("source_releases", json!(1)),
                            ("extra", json!(true)),
                        ] {
                            let mut bad = records.clone();
                            bad[stage]["evidence"]["sdk_operations"][role][field] = value;
                            assert!(
                                parse_records(wrapper, mode, &encode(&bad)).is_err(),
                                "{wrapper}/{mode} stage {stage} role {role}: {field}"
                            );
                        }
                    }
                    let mut missing = records.clone();
                    missing[stage]["evidence"]["sdk_operations"]
                        .as_array_mut()
                        .unwrap()
                        .pop();
                    assert!(parse_records(wrapper, mode, &encode(&missing)).is_err());
                    let mut duplicate = records.clone();
                    duplicate[stage]["evidence"]["sdk_operations"][1] = rows[0].clone();
                    assert!(parse_records(wrapper, mode, &encode(&duplicate)).is_err());
                    let mut missing_counter = records.clone();
                    missing_counter[stage]["evidence"]["sdk_operations"][0]
                        .as_object_mut()
                        .unwrap()
                        .remove("pending_kind");
                    assert!(parse_records(wrapper, mode, &encode(&missing_counter)).is_err());
                }
            }
        }
    }

    #[test]
    fn callback_sdk_protocol_requires_monotonic_live_polls_and_stable_cleanup_before_consumption() {
        for wrapper in ["run_custom", "serve_custom"] {
            for mode in CASES {
                let records = sdk_records(wrapper, mode, "case");
                assert_eq!(
                    parse_records(wrapper, mode, &encode(&records)).unwrap(),
                    records
                );
                for stage in 1..3 {
                    let mut bad = records.clone();
                    bad[stage]["evidence"]["sdk_operations"][0]["operation_polls"] = json!(1);
                    assert!(parse_records(wrapper, mode, &encode(&bad)).is_err());
                }
                for field in ["sdk_polls_after_collect", "sdk_releases_after_collect"] {
                    let count = records[2]["evidence"][field].as_array().unwrap().len();
                    for role in 0..count {
                        let mut bad = records.clone();
                        bad[2]["evidence"][field][role] = json!(12);
                        assert!(parse_records(wrapper, mode, &encode(&bad)).is_err());
                    }
                    let mut missing = records.clone();
                    missing[2]["evidence"]
                        .as_object_mut()
                        .unwrap()
                        .remove(field);
                    assert!(parse_records(wrapper, mode, &encode(&missing)).is_err());
                    let mut short = records.clone();
                    short[2]["evidence"][field].as_array_mut().unwrap().pop();
                    assert!(parse_records(wrapper, mode, &encode(&short)).is_err());
                }
                let mut bad = records.clone();
                bad[2]["evidence"]["release_before_handle_consumption"] = json!(false);
                assert!(parse_records(wrapper, mode, &encode(&bad)).is_err());
                bad = records.clone();
                bad[2]["evidence"]["release_stable"] = json!(false);
                assert!(parse_records(wrapper, mode, &encode(&bad)).is_err());
            }
        }
    }

    #[test]
    fn callback_sdk_protocol_preserves_canonical_record_log_order_and_tail_bounds() {
        let wrapper = "run_custom";
        let mode = "after-ok";
        let records = sdk_records(wrapper, mode, "case");
        let bytes = encode(&records);
        assert_eq!(parse_records(wrapper, mode, &bytes).unwrap(), records);
        for foreign in [
            sdk_records("serve_custom", mode, "case"),
            sdk_records(wrapper, "after-error", "case"),
            sdk_records(wrapper, mode, "foreign"),
        ] {
            assert!(parse_records(wrapper, mode, &encode(&foreign)).is_err());
        }
        let mut reordered = records.clone();
        reordered.swap(0, 1);
        assert!(parse_records(wrapper, mode, &encode(&reordered)).is_err());
        for tail in [
            PREFIX.to_vec(),
            b"edgezero-life".to_vec(),
            encode(&records[..1]),
        ] {
            let mut extra = bytes.clone();
            extra.extend(tail);
            assert!(parse_records(wrapper, mode, &extra).is_err());
        }
        let mut oversized = records.clone();
        oversized[0]["extra"] = json!("x".repeat(2048));
        assert!(parse_records(wrapper, mode, &encode(&oversized)).is_err());
        assert!(parse_records(wrapper, mode, &vec![b'x'; protocol::LOG_LIMIT + 1]).is_err());
        let mut noncanonical = PREFIX.to_vec();
        noncanonical.extend(serde_json::to_vec_pretty(&records[0]).unwrap());
        noncanonical.push(b'\n');
        assert!(parse_records(wrapper, mode, &noncanonical).is_err());
    }

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
