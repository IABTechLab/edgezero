use super::*;

const PREFIX: &[u8] = b"edgezero-lifetime-cleanup ";
const LOG_LIMIT: usize = 1024 * 1024;

fn body(token: &str) -> Value {
    json!({"schema":1,"case":"response-cleanup","token":token,"origin_pending":true,
        "batch_pending":true,"callback_pending":true,"qualification":false})
}

fn record(token: &str, stage: &str) -> Value {
    let evidence = match stage {
        "cleaned" => json!({"outcome":"Completed","terminal_reports":1,"body_scope_closed":true,
            "origin_error":"Internal","batch_error":"Internal","slots":["Unresolved","Unresolved","Unresolved"],
            "callback_scope_live":true,"callback_pending":true}),
        "continued" => json!({"callback_bytes":[75],"post_bytes":[80],"callback_scope_live":true,
            "terminal_reports":1,"same_client":true}),
        "returned" => json!({"result":"Ok","callback_scope_closed":true,"terminal_reports":1}),
        _ => panic!("unknown cleanup record"),
    };
    json!({"schema":1,"event":"response-cleanup","case":"response-cleanup","token":token,
        "stage":stage,"evidence":evidence,"qualification":false})
}

fn parse_records(bytes: &[u8], token: &str) -> Result<Vec<Value>> {
    require(
        bytes.len() <= LOG_LIMIT,
        "cleanup runtime log exceeds bound",
    )?;
    let stages = ["cleaned", "continued", "returned"];
    let mut records = Vec::new();
    for line in bytes.split_inclusive(|byte| *byte == b'\n') {
        if !line.starts_with(PREFIX) {
            require(
                line.ends_with(b"\n") || records.len() < 3 || !PREFIX.starts_with(line),
                "incomplete extra cleanup protocol prefix",
            )?;
            continue;
        }
        require(
            line.len() <= PREFIX.len() + 2049,
            "cleanup protocol record exceeds bound",
        )?;
        if !line.ends_with(b"\n") {
            require(
                records.len() < 3,
                "incomplete extra cleanup protocol record",
            )?;
            break;
        }
        let stage = stages
            .get(records.len())
            .ok_or("extra cleanup protocol record")?;
        let expected = record(token, stage);
        let payload = &line[PREFIX.len()..line.len() - 1];
        require(
            payload == serde_json::to_vec(&expected)?,
            "invalid or reordered canonical cleanup record",
        )?;
        records.push(expected);
    }
    Ok(records)
}

fn validate_reply(reply: &Value, token: &str) -> Result<()> {
    let fields = reply["headers"]
        .as_array()
        .ok_or("cleanup response has no headers")?;
    let types: Vec<_> = fields
        .iter()
        .filter(|field| {
            field[0]
                .as_str()
                .is_some_and(|name| name.eq_ignore_ascii_case("content-type"))
        })
        .collect();
    require(
        reply["status"] == 200 && types.len() == 1 && types[0][1] == "application/json",
        "invalid cleanup response head",
    )?;
    let mut bytes = serde_json::to_vec(&body(token))?;
    bytes.push(b'\n');
    require(
        bytes.len() <= 512 && reply["body_bytes"] == json!(bytes),
        "invalid cleanup response body or pending witnesses",
    )
}

fn validate_origin(origin: &Value, token: &str) -> Result<()> {
    let events = origin.as_array().ok_or("missing cleanup source evidence")?;
    require(
        events.len() == 26
            && events.iter().all(|entry| {
                entry["token"] == token
                    && entry["case"] == "response-cleanup"
                    && entry.get("error").is_none()
                    && entry.get("scheduled_us").is_none()
            }),
        "invalid cleanup source identity or cardinality",
    )?;
    let one = |role: &str, op: &str| -> Result<&Value> {
        let mut found = events
            .iter()
            .filter(|entry| entry["role"] == role && entry["op"] == op);
        let entry = found.next().ok_or("missing cleanup source phase")?;
        require(found.next().is_none(), "duplicate cleanup source phase")?;
        Ok(entry)
    };
    let returned = one("control", "response-returned")?;
    let release = one("control", "release")?;
    let returned_at = number(returned, "at_us")?;
    let cleaned_at = number(returned, "cleanup_validated_us")?;
    let release_at = number(release, "at_us")?;
    require(
        returned_at <= cleaned_at
            && cleaned_at <= release_at
            && number(release, "response_returned_us")? == returned_at,
        "cleanup release preceded complete response or canonical terminal evidence",
    )?;
    let ready = one("ready", "head")?;
    let ready_started = number(ready, "write_started_us")?;
    require(
        number(one("ready", "request")?, "at_us")? <= ready_started
            && ready_started <= number(ready, "at_us")?
            && ready_started <= returned_at
            && one("ready", "closed")?["reason"] == "complete",
        "invalid cleanup readiness barrier",
    )?;
    for role in ["origin", "a", "b", "c", "callback"] {
        let requested = number(one(role, "request")?, "at_us")?;
        let waiting = number(one(role, "waiting")?, "at_us")?;
        require(
            requested <= waiting && waiting <= ready_started,
            "cleanup work was not pending before readiness/egress",
        )?;
        if role != "callback" {
            let closed = one(role, "closed")?;
            require(
                closed["reason"] == "fixture_released" && number(closed, "at_us")? >= release_at,
                "body-owned source completed or closed before fixture release",
            )?;
        }
    }
    for (role, byte) in [("callback", 75), ("post", 80)] {
        let head = one(role, "head")?;
        let chunk = one(role, "chunk")?;
        let closed = one(role, "closed")?;
        require(
            number(head, "write_started_us")? >= release_at
                && number(head, "at_us")? >= number(head, "write_started_us")?
                && number(chunk, "write_started_us")? >= number(head, "at_us")?
                && number(chunk, "at_us")? >= number(chunk, "write_started_us")?
                && chunk["bytes"] == json!([byte])
                && closed["reason"] == "complete"
                && number(closed, "at_us")? >= number(chunk, "at_us")?,
            "callback or post-delivery exchange failed or completed before cleanup",
        )?;
    }
    require(
        number(one("post", "request")?, "at_us")?
            >= number(one("callback", "chunk")?, "write_started_us")?,
        "post-delivery work started before successful sibling payload",
    )
}

fn wait_records(log: &Path, token: &str, count: usize) -> Result<Vec<Value>> {
    use std::io::Read as _;
    let started = Instant::now();
    loop {
        let mut bytes = Vec::new();
        fs::File::open(log)?
            .take((LOG_LIMIT + 1) as u64)
            .read_to_end(&mut bytes)?;
        let records = parse_records(&bytes, token)?;
        if records.len() >= count {
            return Ok(records);
        }
        require(
            started.elapsed() < Duration::from_secs(2),
            "cleanup terminal evidence timed out",
        )?;
        thread::sleep(Duration::from_millis(5));
    }
}

pub(super) fn check(
    port: u16,
    origins: &WireOrigins,
    log: &Path,
    records: &mut Records,
) -> Result<()> {
    let token = token();
    origins.register_lifetime(&token, "response-cleanup")?;
    let anchor = origins.mark_lifetime_start(&token)?;
    let backend = format!(
        "http://127.0.0.1:{}/lifetimes/{token}/response-cleanup",
        origins.port(0)
    );
    let reply = net::header_request(
        port,
        "/progress/lifetimes/response-cleanup",
        &token,
        &[("x-fixture-backend", &backend)],
        None,
    );
    let returned = Instant::now();
    let mut evidence = Vec::new();
    let mut cleaned = None;
    let outcome = (|| -> Result<()> {
        let reply = reply.as_ref().map_err(ToString::to_string)?;
        validate_reply(reply, &token)?;
        evidence = wait_records(log, &token, 1)?;
        cleaned = Some(Instant::now());
        origins.release_lifetime(&token, returned)?;
        evidence = wait_records(log, &token, 3)?;
        Ok(())
    })();
    let source = origins.take_lifetime_events(&token);
    let source_error = source.as_ref().err().map(ToString::to_string);
    let mut origin = source.unwrap_or(Value::Null);
    if let Some(events) = origin.as_array_mut() {
        events.push(json!({"role":"control","op":"response-returned","token":token,"case":"response-cleanup",
            "at_us":returned.duration_since(anchor).as_micros() as u64,
            "cleanup_validated_us":cleaned.map(|at|at.duration_since(anchor).as_micros() as u64)}));
    }
    let outcome = outcome.and_then(|()| {
        require(
            source_error.is_none(),
            source_error.clone().unwrap_or_default(),
        )?;
        validate_origin(&origin, &token)
    });
    records.push(json!({"event":"lifetime-case","adapter":"fastly","case":"response-cleanup","token":token,
        "status":if outcome.is_ok(){"pass"}else{"fail"},"error":outcome.as_ref().err().map(ToString::to_string),
        "source_error":source_error,"origin":origin,"protocol":evidence,"client":reply.as_ref().ok(),
        "client_error":reply.as_ref().err().map(ToString::to_string),"qualification":false}))?;
    outcome
}

#[cfg(test)]
mod tests {
    use super::*;

    fn line(stage: &str) -> Vec<u8> {
        let mut bytes = PREFIX.to_vec();
        bytes.extend(serde_json::to_vec(&record("cleanup", stage)).unwrap());
        bytes.push(b'\n');
        bytes
    }

    fn source() -> Value {
        let mut entries = Vec::new();
        for (index, role) in ["origin", "a", "b", "c", "callback"]
            .into_iter()
            .enumerate()
        {
            entries.push(json!({"role":role,"op":"request","at_us":100+index}));
            entries.push(json!({"role":role,"op":"waiting","at_us":200+index}));
        }
        entries.push(json!({"role":"ready","op":"request","at_us":150}));
        entries.push(json!({"role":"ready","op":"head","at_us":310,"write_started_us":300}));
        entries.push(json!({"role":"ready","op":"closed","at_us":320,"reason":"complete"}));
        entries
            .push(json!({"role":"control","op":"release","at_us":600,"response_returned_us":400}));
        for role in ["origin", "a", "b", "c"] {
            entries
                .push(json!({"role":role,"op":"closed","at_us":610,"reason":"fixture_released"}));
        }
        for (role, at, byte) in [("callback", 620, 75), ("post", 700, 80)] {
            if role == "post" {
                entries.push(json!({"role":role,"op":"request","at_us":at}));
            }
            entries.push(json!({"role":role,"op":"head","at_us":at+10,"write_started_us":at}));
            entries.push(json!({"role":role,"op":"chunk","at_us":at+20,"write_started_us":at+10,"bytes":[byte]}));
            entries.push(json!({"role":role,"op":"closed","at_us":at+30,"reason":"complete"}));
        }
        entries.push(json!({"role":"control","op":"response-returned","at_us":400,"cleanup_validated_us":500}));
        for entry in &mut entries {
            entry["token"] = json!("cleanup");
            entry["case"] = json!("response-cleanup");
        }
        json!(entries)
    }

    #[test]
    fn protocol_requires_exact_bounded_ordered_records_and_complete_lines() {
        let mut bytes = b"runtime diagnostic\n".to_vec();
        for (index, stage) in ["cleaned", "continued", "returned"].into_iter().enumerate() {
            bytes.extend(line(stage));
            assert_eq!(parse_records(&bytes, "cleanup").unwrap().len(), index + 1);
        }
        let partial = line("cleaned");
        assert!(
            parse_records(&partial[..partial.len() - 1], "cleanup")
                .unwrap()
                .is_empty()
        );
        for invalid in [
            line("continued"),
            [line("cleaned"), line("cleaned")].concat(),
            [bytes.clone(), line("returned")].concat(),
            vec![b'x'; LOG_LIMIT + 1],
        ] {
            assert!(parse_records(&invalid, "cleanup").is_err());
        }
        assert!(parse_records(&bytes, "foreign").is_err());
        for partial in [
            PREFIX,
            &PREFIX[..PREFIX.len() - 1],
            &line("cleaned")[..PREFIX.len() + 10],
        ] {
            assert!(parse_records(&[bytes.clone(), partial.to_vec()].concat(), "cleanup").is_err());
        }
        let third = line("returned");
        let partial_third = [
            line("cleaned"),
            line("continued"),
            third[..third.len() - 1].to_vec(),
        ]
        .concat();
        assert_eq!(parse_records(&partial_third, "cleanup").unwrap().len(), 2);
        let mut modified = record("cleanup", "cleaned");
        modified["evidence"]["body_scope_closed"] = json!(false);
        let mut wrong = PREFIX.to_vec();
        wrong.extend(serde_json::to_vec(&modified).unwrap());
        wrong.push(b'\n');
        assert!(parse_records(&wrong, "cleanup").is_err());
        // Duplicate fields are intentionally malformed protocol input, not a json! fixture.
        let duplicate = format!(
            "{}{}\n",
            std::str::from_utf8(PREFIX).unwrap(),
            serde_json::to_string(&record("cleanup", "cleaned"))
                .unwrap()
                .replacen('{', "{\"schema\":1,", 1)
        );
        assert!(parse_records(duplicate.as_bytes(), "cleanup").is_err());
    }

    #[test]
    fn response_requires_original_pending_jobs_before_opening_the_source_gate() {
        let mut bytes = serde_json::to_vec(&body("cleanup")).unwrap();
        bytes.push(b'\n');
        let reply = json!({"status":200,"headers":[["content-type","application/json"]],"body_bytes":bytes});
        validate_reply(&reply, "cleanup").unwrap();
        for (path, value) in [
            ("/status", json!(500)),
            ("/headers", json!([])),
            (
                "/headers",
                json!([
                    ["content-type", "application/json"],
                    ["content-type", "application/json"]
                ]),
            ),
            ("/body_bytes", json!(b"{}\n".to_vec())),
        ] {
            let mut changed = reply.clone();
            *changed.pointer_mut(path).unwrap() = value;
            assert!(validate_reply(&changed, "cleanup").is_err());
        }
    }

    #[test]
    fn source_requires_pending_barrier_terminal_release_and_fresh_successful_callback_work() {
        let events = source();
        validate_origin(&events, "cleanup").unwrap();
        for index in 0..events.as_array().unwrap().len() {
            let mut changed = events.clone();
            changed.as_array_mut().unwrap().remove(index);
            assert!(
                validate_origin(&changed, "cleanup").is_err(),
                "missing event {index}"
            );
        }
        for (role, op, field, value) in [
            ("origin", "waiting", "at_us", json!(301)),
            ("callback", "head", "write_started_us", json!(599)),
            ("post", "request", "at_us", json!(629)),
            ("callback", "chunk", "bytes", json!([80])),
            ("origin", "closed", "reason", json!("complete")),
            (
                "control",
                "response-returned",
                "cleanup_validated_us",
                json!(601),
            ),
        ] {
            let mut changed = events.clone();
            changed
                .as_array_mut()
                .unwrap()
                .iter_mut()
                .find(|e| e["role"] == role && e["op"] == op)
                .unwrap()[field] = value;
            assert!(
                validate_origin(&changed, "cleanup").is_err(),
                "{role} {op} {field}"
            );
        }
        let mut changed = events.clone();
        changed[0]["token"] = json!("foreign");
        assert!(validate_origin(&changed, "cleanup").is_err());
    }
}
