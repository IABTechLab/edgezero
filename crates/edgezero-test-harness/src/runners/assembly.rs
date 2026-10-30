use super::*;

fn header<'a>(reply: &'a Value, name: &str) -> Option<&'a str> {
    reply["headers"].as_array()?.iter().find_map(|field| {
        field[0]
            .as_str()?
            .eq_ignore_ascii_case(name)
            .then(|| field[1].as_str())
            .flatten()
    })
}

fn header_values<'a>(reply: &'a Value, name: &str) -> Result<Vec<&'a str>> {
    let fields = reply["headers"]
        .as_array()
        .ok_or("missing assembly headers")?;
    let mut values = Vec::new();
    for field in fields {
        let pair = field
            .as_array()
            .filter(|pair| pair.len() == 2)
            .ok_or("malformed assembly field pair")?;
        let key = pair[0].as_str().ok_or("non-text assembly field name")?;
        let value = pair[1].as_str().ok_or("non-text assembly field value")?;
        if key.eq_ignore_ascii_case(name) {
            values.push(value);
        }
    }
    Ok(values)
}

fn error_contract(case: &str) -> (u16, &'static str, &'static str) {
    let internal = case == "single-internal";
    let config = matches!(
        case,
        "single-config" | "single-hooks" | "single-config-explicit"
    );
    if internal {
        (500, "internal", "internal server error")
    } else if config {
        (503, "config_out_of_date", "configuration unavailable")
    } else {
        (
            503,
            "service_unavailable",
            if case == "custom-retained" {
                "configuration unavailable"
            } else {
                "state unavailable"
            },
        )
    }
}

fn validate_error_headers(reply: &Value, case: &str) -> Result<()> {
    let (status, kind, _) = error_contract(case);
    require(reply["status"] == status, "assembly failure status changed")?;
    require(
        header_values(reply, "content-type")? == ["application/json"],
        "assembly failure lost JSON content type",
    )?;
    require(
        header_values(reply, "x-assembly-configured")?.is_empty()
            && header_values(reply, "x-assembly-synthesized")?.is_empty()
            && header_values(reply, "set-cookie")?.is_empty(),
        "failed app headers escaped",
    )?;
    require(
        header_values(reply, "retry-after")?
            == if kind == "config_out_of_date" {
                vec!["60"]
            } else {
                vec![]
            },
        "assembly retry policy changed",
    )
}

fn validate_error(reply: &Value, case: &str) -> Result<()> {
    validate_error_headers(reply, case)?;
    let (status, kind, message) = error_contract(case);
    let body: Value = serde_json::from_str(reply["body"].as_str().ok_or("missing assembly body")?)?;
    require(
        body == json!({"error":{"kind":kind,"message":message,"status":status}}),
        "assembly default error body changed",
    )
}

fn validate_recovered_headers(reply: &Value) -> Result<()> {
    require(
        reply["status"] == 404
            && header_values(reply, "x-assembly-synthesized")? == ["retry-2"]
            && header_values(reply, "set-cookie")? == ["assembly=retry-2; Path=/"]
            && header_values(reply, "content-type")? == ["application/json"],
        "recovered app synthesized headers missing, duplicated or stale",
    )?;
    let body: Value = serde_json::from_str(
        reply["body"]
            .as_str()
            .ok_or("missing recovered error body")?,
    )?;
    require(
        body == json!({"error":{"kind":"not_found","message":"no route matched path: /assembly/retry/missing","status":404}}),
        "recovered app canonical 404 body changed",
    )
}

fn validate_installs(events: &[Value], expected: &[(&str, usize)]) -> Result<()> {
    let installed = events
        .iter()
        .filter(|event| event["event"] == "assembly-synthesized-installed")
        .collect::<Vec<_>>();
    require(
        installed.len() == expected.len(),
        "missing/duplicate partial-app header installation witness",
    )?;
    for (event, (owner, generation)) in installed.iter().zip(expected) {
        require(
            event["owner"] == *owner
                && event["generation"] == *generation
                && event["fields"] == 2
                && event["instance"]
                    .as_str()
                    .is_some_and(|instance| !instance.is_empty()),
            "wrong assembly header installation witness",
        )?;
    }
    Ok(())
}

fn wire_request(
    name: &str,
    case: &str,
    port: u16,
    path: &str,
    head: bool,
    run: &Path,
    records: &mut Records,
) -> Result<Value> {
    let reply = response_headers::assembly_request(port, path, head, run, name == "cloudflare")?;
    records.push(json!({"event":"assembly-wire-capture","adapter":name,"case":case,"method":if head {"HEAD"} else {"GET"},
        "client":reply,"qualification":false}))?;
    require(
        reply["protocol"] == "HTTP/1.1"
            && reply["eof"] == true
            && reply.get("capture_error") == Some(&Value::Null),
        "assembly capture did not finish cleanly",
    )?;
    let lengths = header_values(&reply, "content-length")?;
    require(
        lengths.len() <= 1
            && lengths
                .iter()
                .all(|value| !value.is_empty() && value.bytes().all(|byte| byte.is_ascii_digit()))
            && header_values(&reply, "transfer-encoding")?.is_empty(),
        "ambiguous assembly response framing",
    )?;
    if !head {
        let bytes = reply["body"]
            .as_str()
            .ok_or("missing captured assembly payload")?
            .len();
        require(
            lengths.len() == 1 && lengths[0].parse::<usize>()? == bytes,
            "assembly Content-Length disagrees with wire payload",
        )?;
    }
    if head {
        require(
            reply["body"] == "",
            "HEAD assembly response transmitted payload",
        )?;
    }
    Ok(reply)
}

fn validate_failed_drop(events: &[Value]) -> Result<()> {
    let attempts = events
        .iter()
        .filter(|event| event["event"] == "assembly-attempt")
        .collect::<Vec<_>>();
    require(
        attempts.len() == 1 && attempts[0]["attempt"] == 1,
        "recovery began before partial-state release was observed",
    )?;
    let drops = events
        .iter()
        .filter(|event| event["event"] == "assembly-state-drop" && event["generation"] == 1)
        .collect::<Vec<_>>();
    require(
        drops.len() == 1 && drops[0]["instance"] == attempts[0]["instance"],
        "failed app state was not released before recovery",
    )
}

fn validate_retry(
    replies: &[Value],
    events: &[Value],
    custom: bool,
    positive_token: Option<&str>,
) -> Result<()> {
    require(replies.len() == 3, "assembly retry needs three requests")?;
    validate_error(
        &replies[0],
        if custom {
            "custom-retained"
        } else {
            "standard-retained"
        },
    )?;
    let mut instance = None;
    for (index, reply) in replies[1..].iter().enumerate() {
        require(
            reply["status"] == 200,
            "failed assembly was cached or retry did not succeed",
        )?;
        require(
            header(reply, "x-assembly-configured") == Some("true"),
            "successful app configuration missing",
        )?;
        require(
            header_values(reply, "x-assembly-synthesized")?.is_empty()
                && header_values(reply, "set-cookie")?.is_empty(),
            "routed recovery response gained synthesized headers",
        )?;
        let value: Value =
            serde_json::from_str(reply["body"].as_str().ok_or("missing retry body")?)?;
        require(
            value["generation"] == 2 && value["attempts"] == 2,
            "retained app was rebuilt",
        )?;
        require(
            value["visits"] == index + 1,
            "successful registered state was not retained",
        )?;
        require(
            value["configurations"] == if custom { 2 } else { 1 },
            "configuration count changed",
        )?;
        let observed = value["instance"]
            .as_str()
            .filter(|id| !id.is_empty())
            .ok_or("missing assembly instance")?;
        if let Some(expected) = &instance {
            require(observed == expected, "recovery crossed instances")?;
        }
        instance = Some(observed.to_owned());
    }
    let attempts = events
        .iter()
        .filter(|e| e["event"] == "assembly-attempt")
        .collect::<Vec<_>>();
    require(
        attempts.len() == 2,
        "expected exactly two assembly attempts",
    )?;
    for (index, event) in attempts.iter().enumerate() {
        require(
            event["attempt"] == index + 1 && event["instance"].as_str() == instance.as_deref(),
            "assembly attempts crossed instances or ordinals",
        )?;
    }
    require(
        events
            .iter()
            .filter(|event| {
                event["event"] == "assembly-synthesized-installed" && event["owner"] == "retry"
            })
            .all(|event| event["instance"].as_str() == instance.as_deref()),
        "retry header installation crossed instances",
    )?;
    if custom {
        let drops = events
            .iter()
            .filter(|e| {
                e["event"] == "assembly-state-drop"
                    && e["generation"] == 1
                    && e["instance"].as_str() == instance.as_deref()
            })
            .count();
        require(
            drops == 1,
            "failed configuration did not release partial state exactly once",
        )?;
        let deliveries = events
            .iter()
            .filter(|e| e["event"] == "assembly-delivery")
            .collect::<Vec<_>>();
        require(
            deliveries.len() == 4,
            "missing owned assembly delivery report",
        )?;
        let mut tokens = replies
            .iter()
            .map(|reply| {
                reply["token"]
                    .as_str()
                    .ok_or("missing recovery request token")
            })
            .collect::<std::result::Result<Vec<_>, _>>()?;
        tokens.push(positive_token.ok_or("missing positive-control request token")?);
        require(
            tokens
                .iter()
                .enumerate()
                .all(|(index, token)| !token.is_empty() && !tokens[..index].contains(token)),
            "recovery request tokens are not unique",
        )?;
        for (index, delivery) in deliveries.iter().enumerate() {
            require(
                delivery["token"].as_str() == Some(tokens[index]),
                "missing, duplicate or foreign owned delivery report",
            )?;
            require(
                delivery["disposition"] == if index == 0 { "detached" } else { "routed" },
                "assembly delivery disposition changed",
            )?;
            require(
                delivery["commitment"] == "Committed"
                    && delivery["outcome"] == "Completed"
                    && delivery["fallback"] == false
                    && delivery["may_send_native"] == false,
                "assembly delivery failed or allowed a native resend",
            )?;
            require(
                delivery["attempts"] == if index == 0 { 1 } else { 2 },
                "retained build attempt count changed",
            )?;
            require(
                delivery["retained"] == (index != 0),
                "partial application was retained",
            )?;
        }
    }
    Ok(())
}

fn validate_boot(events: &[Value], expected: usize, expected_internal: usize) -> Result<()> {
    let logs = events
        .iter()
        .filter(|event| {
            event["event"] == "fixture_log"
                && event["target"] == "edgezero::boot"
                && event["message"]
                    .as_str()
                    .is_some_and(|m| m.starts_with("application assembly failed:"))
        })
        .collect::<Vec<_>>();
    require(
        logs.len() == expected,
        "assembly errors were not logged exactly once per failure",
    )?;
    require(
        logs.iter().all(|event| event["level"] == "ERROR"),
        "assembly boot diagnostic level changed",
    )?;
    if expected_internal > 0 {
        require(
            logs.iter()
                .filter(|event| {
                    event["message"]
                        .as_str()
                        .is_some_and(|m| m.contains("assembly-private-token=secret"))
                })
                .count()
                == expected_internal,
            "full internal error was not logged exactly once",
        )?;
    }
    Ok(())
}

pub(super) fn fastly(exe: &Path, run: &Path, records: &mut Records) -> Result<()> {
    checked(cargo_build()?.args([
        "build",
        "--locked",
        "--release",
        "-p",
        "fixture-fastly",
        "--bin",
        "fixture-fastly-assembly",
        "--target",
        "wasm32-wasip1",
    ]))?;
    let artifact_path = stage(
        &root().join("target/wasm32-wasip1/release/fixture-fastly-assembly.wasm"),
        run,
    )?;
    records.push(json!({"event":"build","adapter":"fastly","variant":"assembly","artifact":artifact(&artifact_path)?}))?;
    let mut failed = 0;
    for case in [
        "standard-retained",
        "custom-retained",
        "single-routes",
        "single-config",
        "single-internal",
        "single-hooks",
        "single-config-explicit",
    ] {
        let log = run.join(format!("assembly-{case}.log"));
        let config = run.join(format!("assembly-{case}.toml"));
        let mut contents = fs::read_to_string(root().join("crates/fixture-fastly/fastly.toml"))?;
        contents.push_str(&format!("\n[local_server.config_stores.assembly]\nformat = \"inline-toml\"\n[local_server.config_stores.assembly.contents]\ncase = {case:?}\n"));
        fs::write(&config, contents)?;
        let outcome = (|| -> Result<()> {
            let port = net::port()?;
            let process = Process::start(
                &mut fastly_command(exe, &config, run, "assembly", port)?,
                &log,
                port,
            )?;
            let retained = case.ends_with("retained");
            let installs: Vec<_> = match case {
                "standard-retained" => vec![("retry", 2)],
                "custom-retained" => vec![("retry", 1), ("retry", 2)],
                "single-config" | "single-hooks" | "single-config-explicit" => {
                    vec![("configuration", 1), ("configuration", 1)]
                }
                _ => vec![],
            };
            let mut replies = Vec::new();
            for index in 0..if retained { 3 } else { 1 } {
                replies.push(wire_request(
                    "fastly",
                    case,
                    port,
                    "/assembly/retry",
                    false,
                    run,
                    records,
                )?);
                if case == "custom-retained" && index == 0 {
                    await_evidence(|| validate_failed_drop(&logs(&log)))?;
                    records.push(json!({"assertion":"failed-state-released-before-retry","adapter":"fastly","token":replies[0]["token"],"status":"pass","qualification":false}))?;
                }
            }
            let mut positive_token = None;
            if retained {
                let positive = wire_request(
                    "fastly",
                    case,
                    port,
                    "/assembly/retry/missing",
                    false,
                    run,
                    records,
                )?;
                validate_recovered_headers(&positive)?;
                positive_token = positive["token"].as_str().map(str::to_owned);
                await_evidence(|| {
                    validate_retry(
                        &replies,
                        &logs(&log),
                        case == "custom-retained",
                        positive_token.as_deref(),
                    )
                })?;
            } else {
                validate_error(&replies[0], case)?;
                let head =
                    wire_request("fastly", case, port, "/assembly/retry", true, run, records)?;
                validate_error_headers(&head, case)?;
                require(
                    head["status"] == replies[0]["status"] && head["body"] == "",
                    "HEAD assembly failure transmitted bytes or changed status",
                )?;
                records.push(annotate(
                    head,
                    json!({"adapter":"fastly","case":case,"method":"HEAD"}),
                ))?;
            }
            await_evidence(|| {
                validate_installs(&logs(&log), &installs)?;
                validate_boot(
                    &logs(&log),
                    if retained { 1 } else { 2 },
                    if case == "single-internal" { 2 } else { 0 },
                )
            })?;
            drop(process);
            validate_installs(&logs(&log), &installs)?;
            if retained {
                validate_retry(
                    &replies,
                    &logs(&log),
                    case == "custom-retained",
                    positive_token.as_deref(),
                )?;
            }
            validate_boot(
                &logs(&log),
                if retained { 1 } else { 2 },
                if case == "single-internal" { 2 } else { 0 },
            )?;
            for reply in replies {
                records.push(annotate(reply, json!({"adapter":"fastly","case":case})))?;
            }
            records.push(json!({"assertion":"canonical-assembly-delivery","adapter":"fastly","case":case,"status":"pass","qualification":false}))
        })();
        let result = settle(
            records,
            &log,
            json!({"adapter":"fastly","case":case}),
            json!({"phase":"assembly"}),
            outcome,
        );
        failed += usize::from(result.is_err());
    }
    require(failed == 0, "one or more Fastly assembly cases failed")
}

pub(super) fn provider(name: &str, run: &Path, records: &mut Records) -> Result<()> {
    if name == "axum" {
        return axum_startup(run, records);
    }
    let package = root().join("crates").join(format!("fixture-{name}"));
    let Some((exe, staged)) = provider_build(name, &package, run, records)? else {
        return Ok(());
    };
    let mut failed = 0;
    for mode in ["per-request", "retained"] {
        let log = run.join(format!("assembly-{name}-{mode}.log"));
        let outcome = (|| -> Result<()> {
            let port = net::port()?;
            let process = Process::start(
                &mut provider_command(name, mode, &exe, &staged, &package, run, port)?,
                &log,
                port,
            )?;
            for (path, case) in [
                ("/assembly/configuration", "single-config"),
                ("/assembly/internal", "single-internal"),
            ] {
                let reply = wire_request(name, case, port, path, false, run, records)?;
                validate_error(&reply, case)?;
                records.push(annotate(
                    reply,
                    json!({"adapter":name,"mode":mode,"case":case}),
                ))?;
            }
            let head = wire_request(
                name,
                "single-config",
                port,
                "/assembly/configuration",
                true,
                run,
                records,
            )?;
            validate_error_headers(&head, "single-config")?;
            require(
                head["status"] == 503
                    && head["body"] == ""
                    && header(&head, "retry-after") == Some("60"),
                "HEAD config failure lost its status, suppression or retry policy",
            )?;
            records.push(annotate(
                head,
                json!({"adapter":name,"mode":mode,"method":"HEAD"}),
            ))?;
            let mut replies = vec![wire_request(
                name,
                "standard-retained",
                port,
                "/assembly/retry",
                false,
                run,
                records,
            )?];
            validate_error(&replies[0], "standard-retained")?;
            if mode == "retained" {
                replies.push(wire_request(
                    name,
                    "standard-retained",
                    port,
                    "/assembly/retry",
                    false,
                    run,
                    records,
                )?);
                replies.push(wire_request(
                    name,
                    "standard-retained",
                    port,
                    "/assembly/retry",
                    false,
                    run,
                    records,
                )?);
                await_evidence(|| {
                    require(
                        logs(&log)
                            .iter()
                            .filter(|e| e["event"] == "assembly-attempt")
                            .count()
                            >= 2,
                        "assembly attempts not observed",
                    )
                })?;
                let events = logs(&log);
                let attempts = events
                    .iter()
                    .filter(|e| e["event"] == "assembly-attempt")
                    .collect::<Vec<_>>();
                if replies[1]["status"] == 503
                    && attempts.len() == 3
                    && attempts.iter().all(|e| e["attempt"] == 1)
                    && attempts
                        .windows(2)
                        .all(|pair| pair[0]["instance"] != pair[1]["instance"])
                {
                    records.push(json!({"assertion":"same-instance-assembly-retry","adapter":name,"mode":mode,"status":"unverified","reason":"runtime supplied fresh instances; same-instance retry was not observable","qualification":false}))?;
                } else {
                    validate_retry(&replies, &events, false, None)?;
                    let positive = wire_request(
                        name,
                        "standard-retained",
                        port,
                        "/assembly/retry/missing",
                        false,
                        run,
                        records,
                    )?;
                    validate_recovered_headers(&positive)?;
                }
            }
            let mut installs = vec![("configuration", 1), ("configuration", 1)];
            if mode == "retained" && replies[1]["status"] == 200 {
                installs.push(("retry", 2));
            }
            let errors = 3 + replies.iter().filter(|r| r["status"] == 503).count();
            await_evidence(|| {
                validate_installs(&logs(&log), &installs)?;
                validate_boot(&logs(&log), errors, 1)
            })?;
            drop(process);
            validate_installs(&logs(&log), &installs)?;
            if mode == "retained" && replies[1]["status"] == 200 {
                validate_retry(&replies, &logs(&log), false, None)?;
            }
            validate_boot(&logs(&log), errors, 1)?;
            for reply in replies {
                records.push(annotate(
                    reply,
                    json!({"adapter":name,"mode":mode,"case":"retry"}),
                ))?;
            }
            records.push(json!({"assertion":"canonical-assembly-delivery","adapter":name,"mode":mode,"status":"pass","qualification":false}))
        })();
        let result = settle(
            records,
            &log,
            json!({"adapter":name,"mode":mode}),
            json!({"phase":"assembly"}),
            outcome,
        );
        failed += usize::from(result.is_err());
    }
    require(failed == 0, "one or more provider assembly cases failed")
}

fn axum_startup(run: &Path, records: &mut Records) -> Result<()> {
    let output = cargo_build()?
        .args([
            "build",
            "--offline",
            "--locked",
            "--release",
            "-p",
            "fixture-axum",
            "--bin",
            "fixture-axum-assembly",
            "--message-format=json-render-diagnostics",
        ])
        .stderr(Stdio::inherit())
        .output()?;
    require(
        output.status.success(),
        "native assembly fixture build failed",
    )?;
    let binary = stage(
        &built_executable(&output.stdout, "fixture-axum-assembly")?,
        run,
    )?;
    records.push(json!({"event":"build","adapter":"axum","variant":"assembly-startup","artifact":artifact(&binary)?}))?;
    let log = run.join("assembly-axum-startup.log");
    let outcome = (|| -> Result<()> {
        let occupied = std::net::TcpListener::bind(("127.0.0.1", 0))?;
        let addr = occupied.local_addr()?;
        let mut command = Command::new(&binary);
        command
            .current_dir(run)
            .env("EDGEZERO__ADAPTER__HOST", "127.0.0.1")
            .env("EDGEZERO__ADAPTER__PORT", addr.port().to_string());
        let process = Process::spawn(&mut command, &log)?;
        let deadline = Instant::now() + Duration::from_secs(5);
        let status = loop {
            if let Some(status) = process.0.try_wait()? {
                break status;
            }
            require(
                Instant::now() < deadline,
                "native assembly failure did not exit before deadline",
            )?;
            thread::sleep(Duration::from_millis(5));
        };
        drop(process);
        let events = logs(&log);
        validate_installs(&events, &[("configuration", 1)])?;
        validate_boot(&events, 1, 0)?;
        let text = fs::read_to_string(&log)?;
        require(
            !status.success()
                && text.contains("application configuration failed")
                && text.contains("configuration unavailable")
                && !text.contains("Address already in use")
                && !text.contains("panicked"),
            "native failure was not canonical pre-listener configuration rejection",
        )?;
        drop(occupied);
        let _recovered = std::net::TcpListener::bind(addr)?;
        records.push(
            json!({"assertion":"assembly-before-bind","adapter":"axum","status":"pass",
            "exit_code":status.code(),"occupied_port_guard":true,"http_response_attempted":false,
            "qualification":false}),
        )
    })();
    settle(
        records,
        &log,
        json!({"adapter":"axum","case":"assembly-startup"}),
        json!({"phase":"assembly-startup"}),
        outcome,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn head_capture_accepts_representation_length_but_rejects_transmitted_bytes() {
        use std::io::{BufRead, BufReader};
        for body in ["", "body"] {
            let listener = std::net::TcpListener::bind(("127.0.0.1", 0)).unwrap();
            let port = listener.local_addr().unwrap().port();
            let server = thread::spawn(move || {
                let (mut stream, _) = listener.accept().unwrap();
                stream
                    .set_read_timeout(Some(Duration::from_secs(1)))
                    .unwrap();
                let mut reader = BufReader::new(stream.try_clone().unwrap());
                let mut line = String::new();
                reader.read_line(&mut line).unwrap();
                assert!(line.starts_with("HEAD "));
                while line != "\r\n" {
                    line.clear();
                    assert!(reader.read_line(&mut line).unwrap() > 0);
                }
                write!(stream, "HTTP/1.1 503 Service Unavailable\r\nContent-Length: 4\r\nConnection: close\r\n\r\n{body}").unwrap();
            });
            let run = std::env::temp_dir().join(format!("assembly-head-{}", token()));
            fs::create_dir(&run).unwrap();
            let mut records = Records {
                path: run.join("events.jsonl"),
                values: Vec::new(),
            };
            let response = wire_request(
                "fastly",
                "head-control",
                port,
                "/assembly/configuration",
                true,
                &run,
                &mut records,
            );
            server.join().unwrap();
            assert_eq!(response.is_ok(), body.is_empty());
            fs::remove_dir_all(run).unwrap();
        }
    }

    #[test]
    fn assembly_capture_rejects_wrong_or_ambiguous_get_framing() {
        use std::io::{BufRead, BufReader};
        for fields in [
            "Content-Length: 0",
            "Content-Length: 14\r\nContent-Length: 14",
            "Transfer-Encoding: chunked",
        ] {
            let listener = std::net::TcpListener::bind(("127.0.0.1", 0)).unwrap();
            let port = listener.local_addr().unwrap().port();
            let server = thread::spawn(move || {
                let (mut stream, _) = listener.accept().unwrap();
                stream
                    .set_read_timeout(Some(Duration::from_secs(1)))
                    .unwrap();
                let mut reader = BufReader::new(stream.try_clone().unwrap());
                loop {
                    let mut line = String::new();
                    assert!(reader.read_line(&mut line).unwrap() > 0);
                    if line == "\r\n" {
                        break;
                    }
                }
                write!(
                    stream,
                    "HTTP/1.1 200 OK\r\n{fields}\r\nConnection: close\r\n\r\nrouted success"
                )
                .unwrap();
            });
            let run = std::env::temp_dir().join(format!("assembly-framing-{}", token()));
            fs::create_dir(&run).unwrap();
            let mut records = Records {
                path: run.join("events.jsonl"),
                values: Vec::new(),
            };
            let result = wire_request(
                "fastly",
                "framing-control",
                port,
                "/assembly/retry",
                false,
                &run,
                &mut records,
            );
            server.join().unwrap();
            fs::remove_dir_all(run).unwrap();
            assert!(result.is_err(), "{fields}");
        }
    }

    #[test]
    fn assembly_boot_evidence_counts_each_failed_request_including_head() {
        let event = json!({"event":"fixture_log","target":"edgezero::boot","level":"ERROR",
            "message":"application assembly failed: assembly-private-token=secret"});
        validate_boot(&[event.clone(), event], 2, 2).unwrap();
    }

    #[test]
    fn assembly_evidence_rejects_changed_status_body_and_required_headers() {
        let valid = json!({"status":503,"headers":[["content-type","application/json"],["retry-after","60"]],
            "body":json!({"error":{"kind":"config_out_of_date","message":"configuration unavailable","status":503}}).to_string()});
        validate_error(&valid, "single-config").unwrap();
        for (field, value) in [
            ("status", json!(500)),
            ("body", json!("private-configured-renderer")),
            ("headers", json!([["content-type", "application/json"]])),
        ] {
            let mut invalid = valid.clone();
            invalid[field] = value;
            assert!(validate_error(&invalid, "single-config").is_err());
        }
    }

    #[test]
    fn failed_assembly_evidence_rejects_synthesized_fields_from_a_partial_app() {
        let valid = json!({"status":503,"headers":[["content-type","application/json"],["retry-after","60"]],
            "body":json!({"error":{"kind":"config_out_of_date","message":"configuration unavailable","status":503}}).to_string()});
        validate_error(&valid, "single-config").unwrap();
        for name in ["X-Assembly-Synthesized", "Set-Cookie"] {
            let mut leaked = valid.clone();
            leaked["headers"]
                .as_array_mut()
                .unwrap()
                .push(json!([name, "private-configured-header"]));
            assert!(validate_error(&leaked, "single-config").is_err(), "{name}");
        }
    }

    #[test]
    fn assembly_header_evidence_rejects_malformed_and_conflicting_metadata() {
        let valid = json!({"status":503,"headers":[["content-type","application/json"],["retry-after","60"]]});
        validate_error_headers(&valid, "single-config").unwrap();
        for extra in [
            json!(["CONTENT-TYPE", "text/plain"]),
            json!(["Retry-After", "1"]),
            json!(["broken"]),
            json!([false, "value"]),
        ] {
            let mut invalid = valid.clone();
            invalid["headers"].as_array_mut().unwrap().push(extra);
            assert!(validate_error_headers(&invalid, "single-config").is_err());
        }
        for missing in [Value::Null, json!([]), json!("missing")] {
            let mut invalid = valid.clone();
            invalid["headers"] = missing;
            assert!(validate_error_headers(&invalid, "single-config").is_err());
        }
    }

    #[test]
    fn recovered_headers_require_the_successful_generation_and_canonical_body() {
        let valid = json!({"status":404,"headers":[["x-assembly-synthesized","retry-2"],["set-cookie","assembly=retry-2; Path=/"],["content-type","application/json"]],
            "body":json!({"error":{"kind":"not_found","message":"no route matched path: /assembly/retry/missing","status":404}}).to_string()});
        validate_recovered_headers(&valid).unwrap();
        for mutation in [
            "missing",
            "stale-header",
            "stale-cookie",
            "duplicate",
            "body",
            "status",
        ] {
            let mut invalid = valid.clone();
            match mutation {
                "missing" => {
                    invalid["headers"].as_array_mut().unwrap().remove(0);
                }
                "stale-header" => invalid["headers"][0][1] = json!("retry-1"),
                "stale-cookie" => invalid["headers"][1][1] = json!("assembly=retry-1; Path=/"),
                "duplicate" => invalid["headers"]
                    .as_array_mut()
                    .unwrap()
                    .push(json!(["X-Assembly-Synthesized", "retry-2"])),
                "body" => invalid["body"] = json!("private-configured-renderer"),
                "status" => invalid["status"] = json!(503),
                _ => unreachable!(),
            }
            assert!(validate_recovered_headers(&invalid).is_err(), "{mutation}");
        }
    }

    #[test]
    fn installation_witness_requires_exact_count_owner_and_generation() {
        let valid = vec![
            json!({"event":"assembly-synthesized-installed","instance":"one","owner":"retry","generation":1,"fields":2}),
            json!({"event":"assembly-synthesized-installed","instance":"one","owner":"retry","generation":2,"fields":2}),
        ];
        let expected = [("retry", 1), ("retry", 2)];
        validate_installs(&valid, &expected).unwrap();
        assert!(validate_installs(&valid[..1], &expected).is_err());
        let mut duplicate = valid.clone();
        duplicate.push(valid[1].clone());
        assert!(validate_installs(&duplicate, &expected).is_err());
        for field in ["instance", "owner", "generation", "fields"] {
            let mut invalid = valid.clone();
            invalid[1][field] = Value::Null;
            assert!(validate_installs(&invalid, &expected).is_err(), "{field}");
        }
    }

    #[test]
    fn owned_delivery_evidence_rejects_count_preserving_duplicate_requests() {
        let replies = vec![
            json!({"status":503,"token":"failed","headers":[["content-type","application/json"]],
            "body":json!({"error":{"status":503,"kind":"service_unavailable","message":"configuration unavailable"}}).to_string()}),
            json!({"status":200,"token":"first","headers":[["x-assembly-configured","true"]],"body":json!({"instance":"one","generation":2,"attempts":2,"visits":1,"configurations":2}).to_string()}),
            json!({"status":200,"token":"second","headers":[["x-assembly-configured","true"]],"body":json!({"instance":"one","generation":2,"attempts":2,"visits":2,"configurations":2}).to_string()}),
        ];
        let mut events = vec![
            json!({"event":"assembly-attempt","instance":"one","attempt":1}),
            json!({"event":"assembly-attempt","instance":"one","attempt":2}),
            json!({"event":"assembly-state-drop","instance":"one","generation":1}),
        ];
        for (index, token) in ["failed", "first", "second", "positive"]
            .into_iter()
            .enumerate()
        {
            events.push(json!({"event":"assembly-delivery","token":token,"disposition":if index==0 {"detached"} else {"routed"},
                "commitment":"Committed","outcome":"Completed","fallback":false,"may_send_native":false,
                "attempts":if index==0 {1} else {2},"retained":index!=0}));
        }
        validate_retry(&replies, &events, true, Some("positive")).unwrap();
        let mut duplicate = events.clone();
        duplicate[6] = events[4].clone();
        assert!(validate_retry(&replies, &duplicate, true, Some("positive")).is_err());
        assert!(validate_retry(&replies, &events, true, Some("first")).is_err());
        let mut foreign = events;
        foreign[6]["token"] = json!("other-request");
        assert!(validate_retry(&replies, &foreign, true, Some("positive")).is_err());
    }

    #[test]
    fn partial_state_release_requires_a_drop_before_recovery_starts() {
        let attempt = json!({"event":"assembly-attempt","instance":"one","attempt":1});
        let release = json!({"event":"assembly-state-drop","instance":"one","generation":1});
        validate_failed_drop(&[attempt.clone(), release.clone()]).unwrap();
        assert!(validate_failed_drop(std::slice::from_ref(&attempt)).is_err());
        assert!(
            validate_failed_drop(&[attempt.clone(), release.clone(), release.clone()]).is_err()
        );
        let mut foreign = release.clone();
        foreign["instance"] = json!("other");
        assert!(validate_failed_drop(&[attempt.clone(), foreign]).is_err());
        assert!(
            validate_failed_drop(&[
                attempt,
                release,
                json!({"event":"assembly-attempt","instance":"one","attempt":2})
            ])
            .is_err()
        );
    }
}
