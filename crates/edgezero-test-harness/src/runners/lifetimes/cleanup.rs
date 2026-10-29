use super::*;

const PREFIX: &[u8] = b"edgezero-lifetime-cleanup ";
const LOG_LIMIT: usize = 1024 * 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Mode {
    Complete,
    SourceError,
    WriteDeadline,
}

pub(super) const CASES: [Mode; 3] = [Mode::Complete, Mode::SourceError, Mode::WriteDeadline];

impl Mode {
    pub(super) fn name(self) -> &'static str {
        match self {
            Self::Complete => "response-cleanup",
            Self::SourceError => "response-cleanup-source-error",
            Self::WriteDeadline => "response-cleanup-write-deadline",
        }
    }

    fn header(self) -> &'static str {
        match self {
            Self::Complete => "complete",
            Self::SourceError => "source-error",
            Self::WriteDeadline => "write-deadline",
        }
    }
}

fn validate_wire(mode: Mode, reply: &Result<Value>, token: &str) -> Result<&'static str> {
    match (mode, reply) {
        (Mode::Complete, Ok(reply)) => {
            validate_reply(mode, reply, token)?;
            Ok("complete")
        }
        (Mode::SourceError | Mode::WriteDeadline, Err(error)) => {
            let partial = error
                .downcast_ref::<net::InterruptedResponse>()
                .ok_or("cleanup failure has no complete response head")?;
            require(
                matches!(
                    partial.cause,
                    net::ResponseInterruption::PrematureEof | net::ResponseInterruption::PeerClosed
                ),
                "cleanup failure is not a confirmed peer end",
            )?;
            validate_payload(mode, token, partial.status, &partial.headers, &partial.body)?;
            Ok("interrupted")
        }
        _ => Err("cleanup wire termination does not match its mode".into()),
    }
}

fn body(mode: Mode, token: &str) -> Value {
    json!({"schema":1,"case":mode.name(),"token":token,"origin_pending":true,
        "batch_pending":true,"callback_pending":true,"qualification":false})
}

fn record(mode: Mode, token: &str, stage: &str) -> Value {
    let mut evidence = match stage {
        "cleaned" => json!({"outcome":"Completed","terminal_reports":1,"body_scope_closed":true,
            "origin_error":"Internal","batch_error":"Internal","slots":["Unresolved","Unresolved","Unresolved"],
            "callback_scope_live":true,"callback_pending":true}),
        "continued" => json!({"callback_bytes":[75],"post_bytes":[80],"callback_scope_live":true,
            "terminal_reports":1,"same_client":true}),
        "returned" => json!({"result":"Ok","callback_scope_closed":true,"terminal_reports":1}),
        _ => panic!("unknown cleanup record"),
    };
    if stage == "cleaned" && mode != Mode::Complete {
        let source = mode == Mode::SourceError;
        evidence["outcome"] = json!(if source {
            "SourceError"
        } else {
            "DeadlineExceeded"
        });
        evidence["delivery_error"] = json!(if source { "Source" } else { "Deadline" });
        evidence["source_error_reached"] = json!(source);
        evidence["pending_poll"] = json!(!source);
        evidence["source_category"] = json!(if source { Some("internal") } else { None });
        evidence["accepted_prefix"] = json!(true);
    }
    json!({"schema":1,"event":"response-cleanup","case":mode.name(),"token":token,
        "stage":stage,"evidence":evidence,"qualification":false})
}

fn parse_records(mode: Mode, bytes: &[u8], token: &str) -> Result<Vec<Value>> {
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
        let expected = record(mode, token, stage);
        let payload = &line[PREFIX.len()..line.len() - 1];
        require(
            payload == serde_json::to_vec(&expected)?,
            "invalid or reordered canonical cleanup record",
        )?;
        records.push(expected);
    }
    Ok(records)
}

fn expected_bytes(mode: Mode, token: &str) -> Result<Vec<u8>> {
    let mut bytes = serde_json::to_vec(&body(mode, token))?;
    bytes.push(b'\n');
    require(bytes.len() <= 512, "cleanup prefix exceeds limit")?;
    Ok(bytes)
}

fn validate_head(status: u16, fields: &[Value]) -> Result<()> {
    let types: Vec<_> = fields
        .iter()
        .filter(|field| {
            field[0]
                .as_str()
                .is_some_and(|name| name.eq_ignore_ascii_case("content-type"))
        })
        .collect();
    require(
        status == 200 && types.len() == 1 && types[0][1] == "application/json",
        "invalid cleanup response head",
    )
}

fn validate_payload(
    mode: Mode,
    token: &str,
    status: u16,
    fields: &[Value],
    bytes: &[u8],
) -> Result<()> {
    validate_head(status, fields)?;
    require(
        bytes == expected_bytes(mode, token)?,
        "invalid cleanup response prefix",
    )
}

fn validate_reply(mode: Mode, reply: &Value, token: &str) -> Result<()> {
    let fields = reply["headers"]
        .as_array()
        .ok_or("cleanup response has no headers")?;
    let status = u16::try_from(reply["status"].as_u64().ok_or("missing cleanup status")?)?;
    validate_head(status, fields)?;
    let bytes = expected_bytes(mode, token)?;
    require(
        bytes.len() <= 512 && reply["body_bytes"] == json!(bytes),
        "invalid cleanup response body or pending witnesses",
    )
}

fn validate_origin(mode: Mode, origin: &Value, token: &str) -> Result<()> {
    let events = origin.as_array().ok_or("missing cleanup source evidence")?;
    require(
        events.len() == (if mode == Mode::Complete { 26 } else { 30 })
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
    let prefix_at = number(returned, "prefix_observed_us")?;
    let ready = one("ready", "head")?;
    let ready_started = number(ready, "write_started_us")?;
    require(
        prefix_at <= returned_at
            && returned_at <= cleaned_at
            && cleaned_at <= release_at
            && number(release, "response_returned_us")? == returned_at,
        "cleanup release preceded complete response or canonical terminal evidence",
    )?;
    if mode != Mode::Complete {
        let acknowledged = one("control", "prefix-acknowledged")?;
        let ack = one("ack", "head")?;
        require(
            number(acknowledged, "prefix_observed_us")? == prefix_at
                && prefix_at <= number(acknowledged, "at_us")?
                && number(acknowledged, "at_us")? <= number(ack, "write_started_us")?
                && number(ack, "write_started_us")? <= number(ack, "at_us")?
                && number(ack, "write_started_us")? <= returned_at
                && number(one("ack", "request")?, "at_us")? >= ready_started
                && one("ack", "closed")?["reason"] == "complete",
            "cleanup fault was not acknowledged after its prefix",
        )?;
        let position = |op: &str| {
            events
                .iter()
                .position(|event| event["role"] == "control" && event["op"] == op)
                .ok_or("missing control phase")
        };
        require(
            position("prefix-acknowledged")? < position("release")?,
            "source release preceded fault acknowledgment",
        )?;
    }
    if mode == Mode::WriteDeadline {
        require(
            returned_at
                .checked_sub(number(returned, "request_started_us")?)
                .is_some_and(|elapsed| elapsed >= 1_000_000),
            "write deadline ended before one second",
        )?;
    }
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

#[derive(Default)]
pub(super) struct LogCursor {
    offset: u64,
    observed: std::cell::Cell<u64>,
}

pub(super) fn finish(log: &Path, cursor: &LogCursor) -> Result<()> {
    let bytes = read_bytes(log, cursor)?;
    require(
        bytes
            .split_inclusive(|byte| *byte == b'\n')
            .all(|line| !line.starts_with(PREFIX) && !PREFIX.starts_with(line)),
        "unexpected cleanup protocol in final runtime log tail",
    )
}

fn read_bytes(log: &Path, cursor: &LogCursor) -> Result<Vec<u8>> {
    use std::io::{Read as _, Seek as _, SeekFrom};
    let mut file = fs::File::open(log)?;
    let length = file.metadata()?.len();
    require(
        length <= LOG_LIMIT as u64 && length >= cursor.observed.get() && length >= cursor.offset,
        "cleanup log exceeded its total bound or shrank",
    )?;
    file.seek(SeekFrom::Start(cursor.offset))?;
    let remaining = LOG_LIMIT as u64 - cursor.offset;
    let mut bytes = Vec::new();
    file.take(remaining + 1).read_to_end(&mut bytes)?;
    require(
        bytes.len() as u64 <= remaining && cursor.offset + bytes.len() as u64 >= length,
        "cleanup runtime log exceeds total bound or shrank during read",
    )?;
    cursor.observed.set(cursor.offset + bytes.len() as u64);
    Ok(bytes)
}

fn read_records(
    mode: Mode,
    log: &Path,
    token: &str,
    cursor: &LogCursor,
) -> Result<(Vec<Value>, u64)> {
    let bytes = read_bytes(log, cursor)?;
    let records = parse_records(mode, &bytes, token)?;
    let complete = bytes
        .iter()
        .rposition(|byte| *byte == b'\n')
        .map_or(0, |index| index + 1);
    Ok((records, cursor.offset + complete as u64))
}

fn wait_records(
    mode: Mode,
    log: &Path,
    token: &str,
    count: usize,
    cursor: &LogCursor,
) -> Result<(Vec<Value>, u64)> {
    let started = Instant::now();
    loop {
        let (records, end) = read_records(mode, log, token, cursor)?;
        if records.len() >= count {
            return Ok((records, end));
        }
        require(
            started.elapsed() < Duration::from_secs(2),
            "cleanup terminal evidence timed out",
        )?;
        thread::sleep(Duration::from_millis(5));
    }
}

pub(super) fn check(
    mode: Mode,
    port: u16,
    origins: &WireOrigins,
    log: &Path,
    cursor: &mut LogCursor,
    records: &mut Records,
) -> Result<()> {
    let token = token();
    origins.register_lifetime(&token, "response-cleanup")?;
    let anchor = origins.mark_lifetime_start(&token)?;
    let backend = format!(
        "http://127.0.0.1:{}/lifetimes/{token}/response-cleanup",
        origins.port(0)
    );
    let prefix = std::cell::Cell::new(None);
    let notify = |status, headers: &[Value], bytes: &[u8]| {
        let observed = Instant::now();
        validate_payload(mode, &token, status, headers, bytes)?;
        require(
            prefix.get().is_none(),
            "duplicate cleanup prefix observation",
        )?;
        if mode != Mode::Complete {
            origins.acknowledge_cleanup_prefix(&token, observed)?;
        }
        prefix.set(Some(observed));
        Ok(())
    };
    let request_started = Instant::now();
    let reply = net::header_request(
        port,
        "/progress/lifetimes/response-cleanup",
        &token,
        &[
            ("x-fixture-backend", &backend),
            ("x-fixture-cleanup-mode", mode.header()),
        ],
        Some(net::PrefixObserver {
            bytes: expected_bytes(mode, &token)?.len(),
            notify: &notify,
        }),
    );
    let returned = Instant::now();
    let mut evidence = Vec::new();
    let mut cleaned = None;
    let mut wire_end = None;
    let outcome = (|| -> Result<()> {
        wire_end = Some(validate_wire(mode, &reply, &token)?);
        require(
            prefix.get().is_some(),
            "cleanup response has no acknowledged prefix",
        )?;
        if mode == Mode::WriteDeadline {
            require(
                returned.duration_since(request_started) >= Duration::from_secs(1),
                "cleanup wire ended before its write deadline",
            )?;
        }
        evidence = wait_records(mode, log, &token, 1, cursor)?.0;
        cleaned = Some(Instant::now());
        origins.release_lifetime(&token, returned)?;
        let (all, end) = wait_records(mode, log, &token, 3, cursor)?;
        evidence = all;
        cursor.offset = end;
        Ok(())
    })();
    let source = origins.take_lifetime_events(&token);
    let source_error = source.as_ref().err().map(ToString::to_string);
    let mut origin = source.unwrap_or(Value::Null);
    if let Some(events) = origin.as_array_mut() {
        events.push(json!({"role":"control","op":"response-returned","token":token,"case":"response-cleanup",
            "at_us":returned.duration_since(anchor).as_micros() as u64,
            "cleanup_validated_us":cleaned.map(|at|at.duration_since(anchor).as_micros() as u64),
            "prefix_observed_us":prefix.get().map(|at|at.duration_since(anchor).as_micros() as u64),
            "request_started_us":request_started.duration_since(anchor).as_micros() as u64}));
    }
    let outcome = outcome.and_then(|()| {
        require(
            source_error.is_none(),
            source_error.clone().unwrap_or_default(),
        )?;
        validate_origin(mode, &origin, &token)
    });
    records.push(json!({"event":"lifetime-case","adapter":"fastly","case":mode.name(),"token":token,
        "status":if outcome.is_ok(){"pass"}else{"fail"},"error":outcome.as_ref().err().map(ToString::to_string),
        "source_error":source_error,"origin":origin,"protocol":evidence,"client":reply.as_ref().ok(),
        "client_error":reply.as_ref().err().map(ToString::to_string),"wire_end":wire_end,
        "partial_client":reply.as_ref().err().and_then(|error|error.downcast_ref::<net::InterruptedResponse>())
            .map(|partial|json!({"status":partial.status,"headers":partial.headers,"body_bytes":partial.body,"cause":format!("{:?}",partial.cause)})),
        "qualification":false}))?;
    outcome
}

#[cfg(test)]
mod tests {
    use super::*;

    fn failure_wire(mode: Mode, cause: net::ResponseInterruption) -> Result<Value> {
        let payload = body(mode, "cleanup");
        let mut bytes = serde_json::to_vec(&payload).unwrap();
        bytes.push(b'\n');
        Err(Box::new(net::InterruptedResponse {
            status: 200,
            headers: vec![json!(["content-type", "application/json"])],
            body: bytes,
            reason: "fixture transport terminated".into(),
            cause,
        }))
    }

    #[test]
    fn failed_cleanup_requires_a_valid_prefix_and_typed_peer_end_not_a_reader_failure() {
        for mode in [Mode::SourceError, Mode::WriteDeadline] {
            let peer = failure_wire(mode, net::ResponseInterruption::PrematureEof);
            assert_eq!(
                validate_wire(mode, &peer, "cleanup").unwrap(),
                "interrupted"
            );
            let payload = body(mode, "cleanup");
            let mut bytes = serde_json::to_vec(&payload).unwrap();
            bytes.push(b'\n');
            let complete = Ok(
                json!({"status":200,"headers":[["content-type","application/json"]],"body_bytes":bytes}),
            );
            assert!(validate_wire(mode, &complete, "cleanup").is_err());
            for cause in [
                net::ResponseInterruption::InvalidFraming,
                net::ResponseInterruption::CaptureLimit,
                net::ResponseInterruption::ObserverRejected,
                net::ResponseInterruption::MissingPrefix,
                net::ResponseInterruption::ReadIo(std::io::ErrorKind::TimedOut),
                net::ResponseInterruption::ReadIo(std::io::ErrorKind::WouldBlock),
            ] {
                assert!(validate_wire(mode, &failure_wire(mode, cause), "cleanup").is_err());
            }
            assert!(validate_wire(Mode::Complete, &peer, "cleanup").is_err());
            assert!(validate_wire(mode, &peer, "foreign").is_err());
            assert!(validate_wire(mode, &Err("no response head".into()), "cleanup").is_err());
        }
    }

    fn line(stage: &str) -> Vec<u8> {
        mode_line(Mode::Complete, "cleanup", stage)
    }

    fn mode_line(mode: Mode, token: &str, stage: &str) -> Vec<u8> {
        let mut bytes = PREFIX.to_vec();
        bytes.extend(serde_json::to_vec(&record(mode, token, stage)).unwrap());
        bytes.push(b'\n');
        bytes
    }

    #[test]
    fn log_cursor_retains_partial_lines_and_rejects_late_records_shrink_and_total_overflow() {
        use std::io::Write as _;
        let path = std::env::temp_dir().join(format!("edgezero-cleanup-log-{}", token()));
        let complete = [line("cleaned"), line("continued"), line("returned")].concat();
        let mut bytes = complete.clone();
        bytes.extend(b"partial diagnostic");
        fs::write(&path, &bytes).unwrap();
        let mut cursor = LogCursor::default();
        let (records, end) = read_records(Mode::Complete, &path, "cleanup", &cursor).unwrap();
        assert_eq!(records.len(), 3);
        assert_eq!(end, complete.len() as u64);
        cursor.offset = end;
        let mut file = fs::OpenOptions::new().append(true).open(&path).unwrap();
        file.write_all(b" complete\n").unwrap();
        for stage in ["cleaned", "continued", "returned"] {
            file.write_all(&mode_line(Mode::SourceError, "second", stage))
                .unwrap();
        }
        let (records, end) = read_records(Mode::SourceError, &path, "second", &cursor).unwrap();
        assert_eq!(records.len(), 3);
        cursor.offset = end;
        // A late record from the previous case is not silently filtered by token.
        file.write_all(&mode_line(Mode::SourceError, "second", "returned"))
            .unwrap();
        assert!(read_records(Mode::WriteDeadline, &path, "third", &cursor).is_err());
        fs::write(&path, b"shrunk").unwrap();
        assert!(read_records(Mode::WriteDeadline, &path, "third", &cursor).is_err());
        fs::write(&path, vec![b'x'; LOG_LIMIT + 1]).unwrap();
        assert!(read_records(Mode::WriteDeadline, &path, "third", &cursor).is_err());
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn log_cursor_rejects_shrinking_an_observed_but_unconsumed_tail() {
        let path = std::env::temp_dir().join(format!("edgezero-cleanup-shrink-{}", token()));
        let complete = [line("cleaned"), line("continued"), line("returned")].concat();
        let mut bytes = complete.clone();
        bytes.extend(b"partial diagnostic");
        fs::write(&path, &bytes).unwrap();
        let mut cursor = LogCursor::default();
        cursor.offset = read_records(Mode::Complete, &path, "cleanup", &cursor)
            .unwrap()
            .1;
        fs::write(&path, &complete).unwrap();
        assert!(read_records(Mode::SourceError, &path, "next", &cursor).is_err());
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn final_log_tail_rejects_records_fragments_and_overflow_after_last_case() {
        let path = std::env::temp_dir().join(format!("edgezero-cleanup-tail-{}", token()));
        let complete = ["cleaned", "continued", "returned"]
            .into_iter()
            .flat_map(|stage| mode_line(Mode::WriteDeadline, "last", stage))
            .collect::<Vec<_>>();
        fs::write(&path, &complete).unwrap();
        let mut cursor = LogCursor::default();
        cursor.offset = read_records(Mode::WriteDeadline, &path, "last", &cursor)
            .unwrap()
            .1;
        finish(&path, &cursor).unwrap();
        for tail in [
            mode_line(Mode::WriteDeadline, "last", "returned"),
            PREFIX.to_vec(),
            PREFIX[..PREFIX.len() - 1].to_vec(),
            vec![b'x'; LOG_LIMIT],
        ] {
            let overflow = complete.len() + tail.len() > LOG_LIMIT;
            fs::write(&path, &complete).unwrap();
            let mut cursor = LogCursor::default();
            cursor.offset = read_records(Mode::WriteDeadline, &path, "last", &cursor)
                .unwrap()
                .1;
            fs::write(&path, [complete.clone(), tail].concat()).unwrap();
            let error = finish(&path, &cursor).unwrap_err();
            assert_eq!(
                error.to_string(),
                if overflow {
                    "cleanup log exceeded its total bound or shrank"
                } else {
                    "unexpected cleanup protocol in final runtime log tail"
                }
            );
        }
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn failure_records_require_the_matching_terminal_and_intentional_branch_witness() {
        for mode in [Mode::SourceError, Mode::WriteDeadline] {
            let bytes = ["cleaned", "continued", "returned"]
                .into_iter()
                .flat_map(|stage| mode_line(mode, "cleanup", stage))
                .collect::<Vec<_>>();
            assert_eq!(parse_records(mode, &bytes, "cleanup").unwrap().len(), 3);
            assert!(parse_records(Mode::Complete, &bytes, "cleanup").is_err());
            for field in ["source_error_reached", "pending_poll", "accepted_prefix"] {
                let mut wrong = record(mode, "cleanup", "cleaned");
                wrong["evidence"][field] = json!(!wrong["evidence"][field].as_bool().unwrap());
                let mut line = PREFIX.to_vec();
                line.extend(serde_json::to_vec(&wrong).unwrap());
                line.push(b'\n');
                assert!(
                    parse_records(mode, &line, "cleanup").is_err(),
                    "{mode:?} {field}"
                );
            }
        }
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
        entries.push(json!({"role":"control","op":"response-returned","at_us":400,"cleanup_validated_us":500,"prefix_observed_us":350,"request_started_us":0}));
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
            assert_eq!(
                parse_records(Mode::Complete, &bytes, "cleanup")
                    .unwrap()
                    .len(),
                index + 1
            );
        }
        let partial = line("cleaned");
        assert!(
            parse_records(Mode::Complete, &partial[..partial.len() - 1], "cleanup")
                .unwrap()
                .is_empty()
        );
        for invalid in [
            line("continued"),
            [line("cleaned"), line("cleaned")].concat(),
            [bytes.clone(), line("returned")].concat(),
            vec![b'x'; LOG_LIMIT + 1],
        ] {
            assert!(parse_records(Mode::Complete, &invalid, "cleanup").is_err());
        }
        assert!(parse_records(Mode::Complete, &bytes, "foreign").is_err());
        for partial in [
            PREFIX,
            &PREFIX[..PREFIX.len() - 1],
            &line("cleaned")[..PREFIX.len() + 10],
        ] {
            assert!(
                parse_records(
                    Mode::Complete,
                    &[bytes.clone(), partial.to_vec()].concat(),
                    "cleanup"
                )
                .is_err()
            );
        }
        let third = line("returned");
        let partial_third = [
            line("cleaned"),
            line("continued"),
            third[..third.len() - 1].to_vec(),
        ]
        .concat();
        assert_eq!(
            parse_records(Mode::Complete, &partial_third, "cleanup")
                .unwrap()
                .len(),
            2
        );
        let mut modified = record(Mode::Complete, "cleanup", "cleaned");
        modified["evidence"]["body_scope_closed"] = json!(false);
        let mut wrong = PREFIX.to_vec();
        wrong.extend(serde_json::to_vec(&modified).unwrap());
        wrong.push(b'\n');
        assert!(parse_records(Mode::Complete, &wrong, "cleanup").is_err());
        // Duplicate fields are intentionally malformed protocol input, not a json! fixture.
        let duplicate = format!(
            "{}{}\n",
            std::str::from_utf8(PREFIX).unwrap(),
            serde_json::to_string(&record(Mode::Complete, "cleanup", "cleaned"))
                .unwrap()
                .replacen('{', "{\"schema\":1,", 1)
        );
        assert!(parse_records(Mode::Complete, duplicate.as_bytes(), "cleanup").is_err());
    }

    #[test]
    fn response_requires_original_pending_jobs_before_opening_the_source_gate() {
        let mut bytes = serde_json::to_vec(&body(Mode::Complete, "cleanup")).unwrap();
        bytes.push(b'\n');
        let reply = json!({"status":200,"headers":[["content-type","application/json"]],"body_bytes":bytes});
        validate_reply(Mode::Complete, &reply, "cleanup").unwrap();
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
            assert!(validate_reply(Mode::Complete, &changed, "cleanup").is_err());
        }
    }

    #[test]
    fn source_requires_pending_barrier_terminal_release_and_fresh_successful_callback_work() {
        let events = source();
        validate_origin(Mode::Complete, &events, "cleanup").unwrap();
        for index in 0..events.as_array().unwrap().len() {
            let mut changed = events.clone();
            changed.as_array_mut().unwrap().remove(index);
            assert!(
                validate_origin(Mode::Complete, &changed, "cleanup").is_err(),
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
                validate_origin(Mode::Complete, &changed, "cleanup").is_err(),
                "{role} {op} {field}"
            );
        }
        let mut changed = events.clone();
        changed[0]["token"] = json!("foreign");
        assert!(validate_origin(Mode::Complete, &changed, "cleanup").is_err());
    }

    #[test]
    fn failure_source_requires_prefix_acknowledgment_before_release_and_a_real_deadline_interval() {
        for mode in [Mode::SourceError, Mode::WriteDeadline] {
            let mut source = source();
            let events = source.as_array_mut().unwrap();
            if mode == Mode::WriteDeadline {
                for event in events.iter_mut() {
                    for field in [
                        "at_us",
                        "write_started_us",
                        "response_returned_us",
                        "cleanup_validated_us",
                    ] {
                        if let Some(value) = event[field].as_u64().filter(|value| *value >= 400) {
                            event[field] = json!(value + 1_000_000);
                        }
                    }
                }
            }
            let release = events
                .iter()
                .position(|event| event["op"] == "release")
                .unwrap();
            let mut ack = vec![
                json!({"role":"ack","op":"request","at_us":360}),
                json!({"role":"control","op":"prefix-acknowledged","at_us":370,"prefix_observed_us":350}),
                json!({"role":"ack","op":"head","write_started_us":380,"at_us":385}),
                json!({"role":"ack","op":"closed","at_us":390,"reason":"complete"}),
            ];
            for event in &mut ack {
                event["token"] = json!("cleanup");
                event["case"] = json!("response-cleanup");
            }
            events.splice(release..release, ack);
            validate_origin(mode, &source, "cleanup").unwrap();
            for index in 0..source.as_array().unwrap().len() {
                let mut changed = source.clone();
                changed.as_array_mut().unwrap().remove(index);
                assert!(
                    validate_origin(mode, &changed, "cleanup").is_err(),
                    "{mode:?} missing {index}"
                );
            }
            let mut changed = source.clone();
            changed
                .as_array_mut()
                .unwrap()
                .iter_mut()
                .find(|event| event["op"] == "prefix-acknowledged")
                .unwrap()["prefix_observed_us"] = json!(351);
            assert!(validate_origin(mode, &changed, "cleanup").is_err());
            if mode == Mode::WriteDeadline {
                let mut changed = source.clone();
                changed
                    .as_array_mut()
                    .unwrap()
                    .iter_mut()
                    .find(|event| event["op"] == "response-returned")
                    .unwrap()["request_started_us"] = json!(1000);
                assert!(validate_origin(mode, &changed, "cleanup").is_err());
            }
        }
    }
}
