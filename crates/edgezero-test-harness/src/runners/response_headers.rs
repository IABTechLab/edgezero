use super::*;
use std::io::Read;
use std::net::TcpStream;

const MAX_HEAD: usize = 16 * 1024;
const MAX_BODY: usize = 4 * 1024;

struct Case {
    name: String,
    method: &'static str,
    path: String,
    status: u16,
    source: Option<&'static str>,
    mode: &'static str,
    large_headers: bool,
}

fn cases(adapter: &str) -> Vec<Case> {
    let mut cases = Vec::new();
    for method in ["GET", "HEAD"] {
        for (name, status, source) in [
            ("success", 200, None),
            ("handler-error", 400, None),
            ("refused", 503, None),
            ("missing/path", 404, Some("app")),
            ("fallback", 500, Some("app")),
        ] {
            cases.push(Case {
                name: format!("{name}-{method}"),
                method,
                path: format!("/progress/synthesized/{name}"),
                status,
                source,
                mode: "app",
                large_headers: false,
            });
        }
    }
    cases.push(Case {
        name: "wrong-method".into(),
        method: "POST",
        path: "/progress/synthesized/success".into(),
        status: 405,
        source: Some("app"),
        mode: "app",
        large_headers: false,
    });
    if adapter == "fastly" {
        cases.push(Case {
            name: "normalized-header-overflow".into(),
            method: "GET",
            path: "/progress/synthesized/success".into(),
            status: 431,
            source: Some("app"),
            mode: "app",
            large_headers: true,
        });
        for (mode, source) in [
            ("app", Some("app")),
            ("replace", Some("request")),
            ("invalid", Some("app")),
            ("empty", None),
        ] {
            cases.push(Case {
                name: format!("normalized-target-overflow-{mode}"),
                method: "GET",
                path: format!(
                    "http://localhost/progress/synthesized/{}",
                    "x".repeat(9 * 1024)
                ),
                status: 414,
                source,
                mode,
                large_headers: false,
            });
            if mode != "app" {
                for method in ["GET", "HEAD"] {
                    cases.push(Case {
                        name: format!("fallback-{mode}-{method}"),
                        method,
                        path: "/progress/synthesized/fallback".into(),
                        status: 500,
                        source,
                        mode,
                        large_headers: false,
                    });
                }
            }
        }
    }
    cases
}

pub(super) fn run(name: &str, parent: &Path, records: &mut Records) -> Result<()> {
    let run = parent.join(format!("synthesized-headers-{name}"));
    fs::create_dir_all(&run)?;
    let revision = Command::new("git")
        .args(["rev-parse", "HEAD"])
        .current_dir(fixture_root())
        .output()?;
    require(
        revision.status.success(),
        "missing header-probe source revision",
    )?;
    let revision = String::from_utf8(revision.stdout)?.trim().to_owned();
    let Some((mut command, port)) =
        progress::build(name, &run, records, &revision, "synthesized-headers")?
    else {
        return Ok(());
    };
    let log = run.join("runtime.log");
    let mut observed = Vec::new();
    let outcome = Process::start(&mut command, &log, port).and_then(|_process| {
        check_cases(
            name,
            records,
            |case, id| capture(port, case, id, &run, name == "cloudflare"),
            |case, id, response| {
                let bytes = response["body"].as_str().map_or(0, str::len);
                observed.push((id.to_owned(), case.name.clone(), bytes));
                if name == "fastly" {
                    await_evidence(|| validate_guest(&logs(&log), id, case, bytes))
                } else {
                    Ok(())
                }
            },
        )
    });
    // Recheck after runtime teardown, so a later duplicate cannot hide behind an earlier pass.
    let final_events = logs(&log);
    let final_result = if name == "fastly" {
        let result = require(
            observed.len() == cases(name).len(),
            "final guest evidence omitted attempted cases",
        )
        .and_then(|()| {
            observed.iter().try_for_each(|(id, case_name, bytes)| {
                let case = cases(name)
                    .into_iter()
                    .find(|case| case.name == *case_name)
                    .ok_or("unknown observed header case")?;
                validate_guest(&final_events, id, &case, *bytes)
            })
        });
        records.push(json!({"event":"synthesized-final-evidence","adapter":name,"checked":observed.len(),
            "status":if result.is_ok(){"pass"}else{"fail"},"error":result.as_ref().err().map(ToString::to_string),"qualification":false}))?;
        result
    } else {
        Ok(())
    };
    let outcome = outcome.and(final_result);
    if name == "spin" {
        for file in ["fixture_stdout.txt", "fixture_stderr.txt"] {
            for event in logs(&run.join(".spin/logs").join(file)) {
                records.push(annotate(
                    event,
                    json!({"adapter":name,"diagnostic_source":file}),
                ))?;
            }
        }
    }
    settle(
        records,
        &log,
        json!({"adapter":name,"suite":"synthesized-headers"}),
        json!({"phase":"synthesized-headers"}),
        outcome,
    )
}

fn check_cases(
    name: &str,
    records: &mut Records,
    mut capture: impl FnMut(&Case, &str) -> Result<Value>,
    mut guest: impl FnMut(&Case, &str, &Value) -> Result<()>,
) -> Result<()> {
    let (mut failures, mut attempted) = (0, 0);
    for case in cases(name) {
        attempted += 1;
        let id = token();
        let (response, result) = match capture(&case, &id) {
            Ok(response) => {
                let result =
                    validate_case(&response, &case).and_then(|()| guest(&case, &id, &response));
                (response, result)
            }
            Err(error) => (Value::Null, Err(error)),
        };
        failures += usize::from(result.is_err());
        records.push(json!({"event":"synthesized-header-case","adapter":name,"case":case.name,"token":id,
            "status":if result.is_ok(){"pass"}else{"fail"},"error":result.err().map(|error|error.to_string()),
            "client":response,"qualification":false}))?;
    }
    records.push(
        json!({"event":"synthesized-header-coverage","adapter":name,"attempted":attempted,
        "failed":failures,"protocol":"HTTP/1.1","qualification":false}),
    )?;
    require(
        failures == 0,
        "one or more synthesized-header wire cases failed",
    )
}

fn parse_wire(bytes: &[u8]) -> Result<Value> {
    let split = bytes
        .windows(4)
        .position(|part| part == b"\r\n\r\n")
        .ok_or("incomplete captured response head")?
        + 4;
    require(
        split <= MAX_HEAD && bytes.len() - split <= MAX_BODY,
        "response exceeded head/body capture bound",
    )?;
    let text = std::str::from_utf8(&bytes[..split])?;
    let mut lines = text.trim_end_matches("\r\n").split("\r\n");
    let mut status = lines
        .next()
        .ok_or("missing response status")?
        .split_whitespace();
    let protocol = status.next().ok_or("missing response version")?;
    let status: u16 = status.next().ok_or("missing response code")?.parse()?;
    let fields = lines
        .map(|line| {
            let (name, value) = line.split_once(':').ok_or("malformed response field")?;
            Ok(json!([name.to_ascii_lowercase(), value.trim()]))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(
        json!({"protocol":protocol,"status":status,"headers":fields,"body":std::str::from_utf8(&bytes[split..])?}),
    )
}

fn capture(port: u16, case: &Case, id: &str, run: &Path, half_close: bool) -> Result<Value> {
    let mut request = format!(
        "{} {} HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nConnection: close\r\nContent-Length: 0\r\nx-request-token: {id}\r\nx-fixture-synthesized-mode: {}\r\n",
        case.method, case.path, case.mode
    );
    if case.large_headers {
        for index in 0..60 {
            request.push_str(&format!("x-large-{index:02}: {}\r\n", "x".repeat(1230)));
        }
    }
    request.push_str("\r\n");
    let mut bytes = Vec::new();
    let started = Instant::now();
    let deadline = started + startup::remaining(port, Duration::from_secs(5))?;
    let outcome = (|| -> Result<()> {
        require(
            request.len() <= 96 * 1024,
            "request capture input exceeded its bound",
        )?;
        let mut stream = TcpStream::connect_timeout(
            &std::net::SocketAddr::from(([127, 0, 0, 1], port)),
            deadline.saturating_duration_since(Instant::now()),
        )?;
        stream.set_nonblocking(true)?;
        let mut written = 0;
        while written < request.len() {
            startup::check(port)?;
            let remaining = deadline.saturating_duration_since(Instant::now());
            require(
                !remaining.is_zero(),
                "absolute request-write deadline expired",
            )?;
            match stream.write(&request.as_bytes()[written..]) {
                Ok(0) => return Err("wire request writer closed".into()),
                Ok(bytes) => written += bytes,
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    thread::sleep(remaining.min(Duration::from_millis(2)))
                }
                Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
                Err(error) => return Err(format!("write captured request: {error}").into()),
            }
        }
        if half_close {
            stream.shutdown(std::net::Shutdown::Write)?;
        }
        loop {
            startup::check(port)?;
            let remaining = deadline.saturating_duration_since(Instant::now());
            require(
                !remaining.is_zero(),
                "absolute wire-capture deadline expired",
            )?;
            let mut buffer = [0; 1024];
            let room = (MAX_HEAD + MAX_BODY).saturating_sub(bytes.len());
            require(room > 0, "wire capture exceeded its allocation bound")?;
            let read = match stream.read(&mut buffer[..room.min(1024)]) {
                Ok(read) => read,
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    thread::sleep(remaining.min(Duration::from_millis(2)));
                    continue;
                }
                Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(error) => return Err(format!("read captured wire: {error}").into()),
            };
            if read == 0 {
                return Ok(());
            }
            bytes.extend_from_slice(&buffer[..read]);
            if let Some(head) = bytes
                .windows(4)
                .position(|part| part == b"\r\n\r\n")
                .map(|at| at + 4)
            {
                require(
                    head <= MAX_HEAD && bytes.len() - head <= MAX_BODY,
                    "wire head/body exceeded capture bound",
                )?;
            } else {
                require(bytes.len() <= MAX_HEAD, "wire head exceeded capture bound")?;
            }
        }
    })();
    let path = run.join(format!("{id}.http1"));
    fs::write(&path, &bytes)?;
    let mut response =
        parse_wire(&bytes).unwrap_or_else(|error| json!({"parse_error":error.to_string()}));
    response["eof"] = json!(outcome.is_ok());
    response["capture_error"] = json!(outcome.err().map(|error| error.to_string()));
    response["capture_artifact"] = artifact(&path)?;
    response["elapsed_ms"] = json!(started.elapsed().as_millis());
    response["request_target_bytes"] = json!(case.path.len());
    response["request_wire_bytes"] = json!(request.len());
    response["request_shutdown"] = json!(if half_close {
        "write-half-close"
    } else {
        "connection-close"
    });
    response["request_field_values"] = json!(
        request
            .split("\r\n")
            .skip(1)
            .take_while(|line| !line.is_empty())
            .count()
    );
    response["request_field_wire_bytes"] = json!(
        request
            .split_once("\r\n")
            .ok_or("missing request-line terminator")?
            .1
            .len()
    );
    if response["capture_error"].is_null() {
        startup::observe(
            port,
            &case.path,
            response["headers"].as_array().unwrap_or(&Vec::new()),
        )?;
        startup::annotate_response(port, &mut response);
    } else {
        startup::reject(port, "first prescribed wire capture did not complete");
        startup::check(port)?;
    }
    Ok(response)
}

pub(super) fn assembly_request(
    port: u16,
    path: &str,
    head: bool,
    run: &Path,
    half_close: bool,
) -> Result<Value> {
    let case = Case {
        name: "assembly".into(),
        method: if head { "HEAD" } else { "GET" },
        path: path.into(),
        status: 0,
        source: None,
        mode: "app",
        large_headers: false,
    };
    let id = token();
    let mut response = capture(port, &case, &id, run, half_close)?;
    response["token"] = json!(id);
    Ok(response)
}

fn validate_case(response: &Value, case: &Case) -> Result<()> {
    validate_wire(response, case.method == "HEAD", case.status, case.source)?;
    require(
        values(response, "transfer-encoding")?.is_empty(),
        "controlled buffered response gained transfer coding",
    )?;
    if case.method != "HEAD" {
        let body = response["body"].as_str().ok_or("missing captured body")?;
        require(
            values(response, "content-length")? == [body.len().to_string()],
            "buffered wire length mismatch",
        )?;
        match case.status {
            200 => require(body == "routed success", "routed body changed")?,
            500 => require(body == "internal server error", "fallback body changed")?,
            503 => require(body == "refused by probe", "refusal body changed")?,
            status => {
                let value: Value = serde_json::from_str(body)?;
                require(
                    value["error"]["status"] == status,
                    "canonical error body changed",
                )?;
            }
        }
    }
    if case.status == 405 {
        let allow = values(response, "allow")?;
        require(allow.len() == 1, "missing or duplicate Allow")?;
        let mut methods = allow[0].split(',').map(str::trim).collect::<Vec<_>>();
        methods.sort_unstable();
        require(methods == ["GET", "HEAD"], "canonical Allow changed")?;
    }
    Ok(())
}

fn validate_guest(events: &[Value], id: &str, case: &Case, bytes: usize) -> Result<()> {
    let matching = |event: &str| {
        events
            .iter()
            .filter(|value| value["event"] == event && value["token"] == id)
            .collect::<Vec<_>>()
    };
    let terminal = matching("synthesized-terminal");
    require(terminal.len() == 1, "missing/duplicate correlated terminal")?;
    let fallback = case.status == 500;
    require(
        terminal[0]["outcome"]
            == if fallback {
                "ConversionError"
            } else {
                "Completed"
            }
            && terminal[0]["body_kind"] == if fallback { "Fallback" } else { "Application" }
            && terminal[0]["fallback"]
                == if fallback {
                    json!("Completed")
                } else {
                    Value::Null
                }
            && terminal[0]["bytes_written"] == bytes,
        "terminal outcome/body-kind/disposition/bytes mismatch",
    )?;
    if case.status == 414 || case.status == 431 {
        let detached = matching("synthesized-detached");
        require(
            detached.len() == 1
                && detached[0]["status"] == case.status
                && matching("synthesized-admission").is_empty()
                && matching("synthesized-handler").is_empty()
                && matching("synthesized-finalize").is_empty(),
            "normalized rejection lost detached/zero-dispatch witness",
        )?;
    } else {
        require(
            matching("synthesized-admission").len() == 1,
            "missing/duplicate admission witness",
        )?;
        require(
            matching("synthesized-handler").len()
                == usize::from(matches!(case.status, 200 | 400 | 500))
                && matching("synthesized-finalize").len() == usize::from(case.status != 503),
            "handler/finalize count mismatch",
        )?;
    }
    if case.mode != "app" {
        let prepare = matching("synthesized-prepare");
        require(
            prepare.len() == 1
                && prepare[0]["mode"] == case.mode
                && prepare[0]["outcome"]
                    == if case.mode == "invalid" {
                        "TooManyBytes"
                    } else {
                        "installed"
                    },
            "missing typed per-request preparation result",
        )?;
    }
    Ok(())
}

fn values<'a>(response: &'a Value, name: &str) -> Result<Vec<&'a str>> {
    response["headers"]
        .as_array()
        .ok_or("missing captured fields")?
        .iter()
        .filter(|field| {
            field[0]
                .as_str()
                .is_some_and(|key| key.eq_ignore_ascii_case(name))
        })
        .map(|field| {
            field[1]
                .as_str()
                .ok_or_else(|| "non-text captured field".into())
        })
        .collect()
}

fn validate_wire(response: &Value, head: bool, status: u16, source: Option<&str>) -> Result<()> {
    require(
        response["protocol"] == "HTTP/1.1"
            && response["status"] == status
            && response["eof"] == true
            && response.get("capture_error") == Some(&Value::Null),
        "wire status/protocol or clean completion missing",
    )?;
    if head {
        require(response["body"] == "", "HEAD transmitted payload octets")?;
    }
    let marker = values(response, "x-synthesized-fixture")?;
    let cookies = values(response, "set-cookie")?;
    let Some(source) = source else {
        return require(
            marker.is_empty() && cookies.is_empty(),
            "ineligible response gained synthesized fields",
        );
    };
    require(
        marker == [source] && cookies.len() == 31,
        "missing, collapsed or extra synthesized values",
    )?;
    for (index, cookie) in cookies.iter().take(30).enumerate() {
        require(
            *cookie == format!("slot-{index:02}=value; Path=/"),
            "synthesized cookie value/order changed",
        )?;
    }
    let charge = 2
        + "x-synthesized-fixture".len()
        + source.len()
        + 4
        + cookies
            .iter()
            .map(|value| "set-cookie".len() + value.len() + 4)
            .sum::<usize>();
    require(
        charge == 4096
            && cookies[30]
                .strip_prefix("padding=")
                .is_some_and(|padding| padding.bytes().all(|byte| byte == b'a')),
        "maximum synthesized field-section charge/padding changed",
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn response(source: &str) -> Value {
        let mut fields = vec![json!(["x-synthesized-fixture", source])];
        for index in 0..30 {
            fields.push(json!([
                "set-cookie",
                format!("slot-{index:02}=value; Path=/")
            ]));
        }
        let used = 2
            + fields
                .iter()
                .map(|field| {
                    field[0].as_str().unwrap().len() + field[1].as_str().unwrap().len() + 4
                })
                .sum::<usize>()
            + "set-cookie".len()
            + 4;
        fields.push(json!([
            "set-cookie",
            format!("padding={}", "a".repeat(4096 - used - "padding=".len()))
        ]));
        json!({"protocol":"HTTP/1.1","status":500,"headers":fields,
            "body":"internal server error","eof":true,"capture_error":null})
    }

    #[test]
    fn synthesized_wire_requires_every_value_in_order_at_the_exact_byte_bound() {
        let valid = response("app");
        validate_wire(&valid, false, 500, Some("app")).unwrap();
        for mutation in [
            "missing",
            "reordered",
            "changed",
            "extra",
            "source",
            "status",
            "protocol",
            "eof",
            "error",
        ] {
            let mut changed = valid.clone();
            match mutation {
                "missing" => {
                    changed["headers"].as_array_mut().unwrap().pop();
                }
                "reordered" => changed["headers"].as_array_mut().unwrap().swap(1, 2),
                "changed" => changed["headers"][1][1] = json!("wrong"),
                "extra" => changed["headers"]
                    .as_array_mut()
                    .unwrap()
                    .push(json!(["set-cookie", "extra"])),
                "source" => changed["headers"][0][1] = json!("request"),
                "status" => changed["status"] = json!(200),
                "protocol" => changed["protocol"] = json!("HTTP/2"),
                "eof" => changed["eof"] = json!(false),
                "error" => changed["capture_error"] = json!("deadline"),
                _ => unreachable!(),
            }
            assert!(
                validate_wire(&changed, false, 500, Some("app")).is_err(),
                "{mutation}"
            );
        }
    }

    #[test]
    fn synthesized_wire_head_requires_clean_eof_and_zero_actual_payload() {
        let mut valid = response("app");
        valid["body"] = json!("");
        validate_wire(&valid, true, 500, Some("app")).unwrap();
        for mutation in ["payload", "timeout", "header-only"] {
            let mut changed = valid.clone();
            match mutation {
                "payload" => changed["body"] = json!("x"),
                "timeout" => changed["capture_error"] = json!("deadline"),
                "header-only" => changed["eof"] = Value::Null,
                _ => unreachable!(),
            }
            assert!(
                validate_wire(&changed, true, 500, Some("app")).is_err(),
                "{mutation}"
            );
        }
        assert!(validate_wire(&valid, true, 500, None).is_err());
        valid["headers"] = json!([]);
        validate_wire(&valid, true, 500, None).unwrap();
    }

    #[test]
    fn wire_capture_finishes_the_request_before_waiting_for_response_eof() {
        let listener = std::net::TcpListener::bind(("127.0.0.1", 0)).unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = thread::spawn(move || {
            let (mut socket, _) = listener.accept().unwrap();
            socket
                .set_read_timeout(Some(Duration::from_millis(200)))
                .unwrap();
            let mut request = Vec::new();
            if socket.read_to_end(&mut request).is_err() {
                return false;
            }
            socket
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 14\r\n\r\nrouted success")
                .unwrap();
            true
        });
        let run = std::env::temp_dir().join(format!("synthesized-capture-{}", token()));
        fs::create_dir(&run).unwrap();
        let case = cases("axum").remove(0);
        let response = capture(port, &case, "half-close-control", &run, true).unwrap();
        let accepted_eof = server.join().unwrap();
        fs::remove_dir_all(run).unwrap();
        assert!(
            accepted_eof,
            "capture did not terminate the request write side"
        );
        assert!(validate_case(&response, &case).is_ok(), "{response}");
    }

    #[test]
    fn wire_capture_rejects_head_payload_in_the_head_packet_or_a_later_write() {
        for delayed in [false, true] {
            let listener = std::net::TcpListener::bind(("127.0.0.1", 0)).unwrap();
            let port = listener.local_addr().unwrap().port();
            let server = thread::spawn(move || {
                let (mut socket, _) = listener.accept().unwrap();
                socket
                    .set_read_timeout(Some(Duration::from_secs(1)))
                    .unwrap();
                let mut request = Vec::new();
                socket.read_to_end(&mut request).unwrap();
                let head = b"HTTP/1.1 200 OK\r\nContent-Length: 14\r\n\r\n";
                if delayed {
                    socket.write_all(head).unwrap();
                    thread::sleep(Duration::from_millis(20));
                    socket.write_all(b"x").unwrap();
                } else {
                    socket.write_all(&[head.as_slice(), b"x"].concat()).unwrap();
                }
            });
            let run = std::env::temp_dir().join(format!("synthesized-head-{}", token()));
            fs::create_dir(&run).unwrap();
            let case = cases("axum")
                .into_iter()
                .find(|case| case.name == "success-HEAD")
                .unwrap();
            let response = capture(port, &case, "head-payload-control", &run, true).unwrap();
            server.join().unwrap();
            fs::remove_dir_all(run).unwrap();
            assert_eq!(response["eof"], true);
            assert_eq!(response["body"], "x");
            assert!(validate_case(&response, &case).is_err());
        }
    }

    #[test]
    fn guest_evidence_rejects_changed_terminal_fields_and_late_duplicates() {
        let case = cases("fastly")
            .into_iter()
            .find(|case| case.name == "fallback-GET")
            .unwrap();
        let valid = vec![
            json!({"event":"synthesized-terminal","token":"one","outcome":"ConversionError", "body_kind":"Fallback", "fallback":"Completed","bytes_written":21}),
            json!({"event":"synthesized-admission","token":"one"}),
            json!({"event":"synthesized-handler","token":"one"}),
            json!({"event":"synthesized-finalize","token":"one"}),
        ];
        validate_guest(&valid, "one", &case, 21).unwrap();
        for key in ["outcome", "body_kind", "fallback", "bytes_written", "token"] {
            let mut changed = valid.clone();
            changed[0][key] = json!("wrong");
            assert!(validate_guest(&changed, "one", &case, 21).is_err(), "{key}");
        }
        let mut late = valid.clone();
        late.push(valid[0].clone());
        assert!(validate_guest(&late, "one", &case, 21).is_err());
        let case = cases("fastly")
            .into_iter()
            .find(|case| case.status == 414)
            .unwrap();
        let valid = vec![
            json!({"event":"synthesized-terminal","token":"one","outcome":"Completed", "body_kind":"Application", "fallback":null,"bytes_written":21}),
            json!({"event":"synthesized-detached","token":"one","status":414}),
        ];
        validate_guest(&valid, "one", &case, 21).unwrap();
        for event in [
            "synthesized-admission",
            "synthesized-handler",
            "synthesized-finalize",
        ] {
            let mut late = valid.clone();
            late.push(json!({"event":event,"token":"one"}));
            assert!(validate_guest(&late, "one", &case, 21).is_err(), "{event}");
        }
    }

    #[test]
    fn capture_artifact_errors_do_not_skip_later_cases_or_claim_coverage() {
        let run = std::env::temp_dir().join(format!("synthesized-failure-{}", token()));
        fs::create_dir(&run).unwrap();
        let mut records = Records {
            path: run.join("events.jsonl"),
            values: Vec::new(),
        };
        let mut attempted = Vec::new();
        let result = check_cases(
            "axum",
            &mut records,
            |case, _| {
                attempted.push(case.name.clone());
                Err("controlled capture artifact failure".into())
            },
            |_, _, _| panic!("failed capture must not be validated"),
        );
        assert!(result.is_err());
        assert_eq!(
            attempted,
            cases("axum")
                .into_iter()
                .map(|case| case.name)
                .collect::<Vec<_>>()
        );
        assert_eq!(
            records
                .values
                .iter()
                .filter(|value| value["status"] == "fail")
                .count(),
            11
        );
        assert_eq!(records.values.last().unwrap()["attempted"], 11);
        assert_eq!(records.values.last().unwrap()["failed"], 11);
        fs::remove_dir_all(run).unwrap();
    }
}
